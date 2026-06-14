"""mod:Position_Mirror - the symbol-keyed open-position store (rule:HR-PM-009).

Source: 0500000 dv1_241 sec 7 mod:Position_Mirror desc + sec 7 mod:WS_Manager
D1 PRIVATE-CHANNEL INBOUND WIRE FACTS (WS-EXE-009 exec_type dispatch / WS-EXE-011 /
WS-EXE-012) + D6 mod:Position_Mirror (the net-position state schema) + the AR-021 /
AR-023 / AR-024 / AR-047 / AR-054 / AR-056 / AR-057 / AR-061 executions rules.

Position_Mirror is the O(1) symbol-keyed dict mirror of every OPEN position. It is
a PURE state store (mirrors keepalive.py / silent_pair.py - no socket, no asyncio):
the executions exec_type dispatch and the snap_orders reconcile are unit-testable
without a network. Two architecture rules shape it:

  rule:HR-PM-009 SOLE WRITER - "WS Manager is the SOLE writer to Position Mirror.
    No other component may directly modify it." Every mutating method requires the
    writer identity WRITER_ID ("WS_Manager"); any other writer raises
    SoleWriterViolationError and emits POSITION_SOLE_WRITER_VIOLATION [CRITICAL]
    (defense-in-depth, rule:HR-PM-011 enforced in BOTH paper and live per PA-005).
    Reads are served through helper methods that return the frozen Position record
    (immutable), so a consumer can never mutate the store it reads.

  PA-004 div #4 - the WRITE SOURCE differs by mode (paper = local sim fills; live =
    container:Private_WS_v2 executions), but that divergence is absorbed UPSTREAM at
    the WS_Manager dispatch seam: this module sees one apply_execution() surface in
    both modes (byte-identical per decision:D-06_Paper_Live_Parity). The synthetic
    paper fill simulator (contract:Synthetic_Capital_Ledger, PA-004 div #3) is a
    LATER module; it feeds this same surface.

WS-EXE-009 exec_type dispatch (AR-023 / AR-057): the executions channel enumerates
EXACTLY 10 exec_type values, ALL handled explicitly; an unrecognised value is logged
UNKNOWN_EXEC_TYPE [WARNING] and never silently dropped (AR-023; complements
rule:HR-WM-006). Position-affecting handling:

  trade / filled  - a FILL. cum_qty (total filled across all fills) + avg_price are
                    the AUTHORITATIVE position fields (WS-EXE-012); last_qty/last_price
                    are fill-level analytics only (AR-061), NOT consumed here. Under
                    the marketable-IOC entry there is no partial/split/dust (AR-054):
                    an opening fill OPENS the symbol's position; a fill on the opposite
                    side CLOSES it; a same-side fill on the open order UPDATES it.
  restated        - engine-initiated maintenance amend, NOT a fill/cancel: it MUST
                    NOT mutate the mirror (AR-024). If a resting position exists for
                    the symbol an elevated ORDER_RESTATED_ALERT is surfaced.
  amended         - user-initiated amend. TothBot NEVER amends its own orders, so any
                    amended event is unexpected: UNEXPECTED_ORDER_AMENDED [CRITICAL],
                    operator alert, no auto-correct (AR-057).
  pending_new / new / canceled / expired / iceberg_refill / status
                  - acknowledged, NO mirror position change (the Pending Order Registry
                    of AR-053 is a separate per-module structure; positions exist only
                    from fills).

snap_orders reconcile (AR-056 / startup Step 6): on every reconnect (and at startup)
the executions subscription SNAPSHOT (snap_orders) is the open-order truth. The mirror
is reconciled by COMPARISON, not blind rebuild: a symbol open in the mirror but ABSENT
from snap_orders closed during the disconnect gap (its emergSL fired while we were
down) - it is removed and surfaced as POSITION_CLOSED_DURING_GAP so WS_Manager can fire
contract:CIATS_Trade_Outcome_Bus for it; symbols still present are retained verbatim
(preserving avg_entry_price, which snap_orders cannot reconstruct).
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from decimal import Decimal
from enum import Enum

# rule:HR-PM-009 - the ONLY identity permitted to write the mirror. Logged as
# writer_id on every state write; any other writer is a sole-writer violation.
WRITER_ID = "WS_Manager"

EventSink = Callable[[object], None]


class ExecType(Enum):
    """The 10 canonical Kraken executions-channel exec_type values (WS-EXE-009 /
    AR-023 / AR-057). ALL handled explicitly; an unknown wire value resolves to no
    member and is logged UNKNOWN_EXEC_TYPE, never dropped."""

    PENDING_NEW = "pending_new"      # order accepted by engine
    NEW = "new"                      # order confirmed active
    TRADE = "trade"                  # a fill event (WS-EXE-011: every fill)
    FILLED = "filled"                # order fully filled
    ICEBERG_REFILL = "iceberg_refill"  # iceberg refill (TothBot uses none)
    CANCELED = "canceled"            # order canceled
    EXPIRED = "expired"             # GTD order expired
    AMENDED = "amended"              # user-initiated amend (AR-057: unexpected)
    RESTATED = "restated"            # engine-maintenance amend (AR-024: no mirror update)
    STATUS = "status"                # status-only update


# The two exec_types that carry a fill and therefore move a position (WS-EXE-012).
_FILL_EXEC_TYPES: frozenset[ExecType] = frozenset({ExecType.TRADE, ExecType.FILLED})


def classify_exec_type(raw: object) -> ExecType | None:
    """Resolve a wire exec_type string to its ExecType (PURE). Returns None for an
    unrecognised value so the caller logs UNKNOWN_EXEC_TYPE and never drops it
    (AR-023 / complements rule:HR-WM-006)."""
    try:
        return ExecType(raw)
    except ValueError:
        return None


class PositionSide(Enum):
    """The direction of an open position, derived from the opening fill side
    (buy -> LONG, sell -> SHORT). A position is CLOSED by a fill on the opposite
    side (LONG closed by a sell; SHORT closed by a buy)."""

    LONG = "long"
    SHORT = "short"


def _side_from_fill(side: object) -> PositionSide | None:
    """Map a wire fill ``side`` (buy/sell) to the position side it OPENS."""
    if side == "buy":
        return PositionSide.LONG
    if side == "sell":
        return PositionSide.SHORT
    return None


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the mirror (AR-047)."""
    return Decimal(str(value))


@dataclass(frozen=True)
class Position:
    """One open position - the symbol-keyed record (frozen: a read consumer cannot
    mutate the store it reads, satisfying the rule:HR-PM-009 read contract).

    Fields reconcile the sec-7 visual box (symbol / entry / qty / cl_ord_id /
    emergSL id) with the D6 net-position schema ({qty, avg_entry_price,
    unrealized_pnl, fill_sequence_id, regime_at_entry, exit_layer_armed})."""

    symbol: str
    side: PositionSide
    qty: Decimal
    avg_entry_price: Decimal           # "entry" - WS-EXE-012 avg_price, authoritative
    cl_ord_id: str | None = None
    emergsl_id: str | None = None      # the ON-FILL batch_add emergSL leg (AR-054)
    fill_sequence_id: int | None = None  # contract:Executions_Channel_Sequence seq
    regime_at_entry: str | None = None
    exit_layer_armed: bool = False
    unrealized_pnl: Decimal = Decimal("0")


class PositionAction(Enum):
    """What an apply_execution() call did to the store (for the caller + tests)."""

    OPENED = "opened"
    CLOSED = "closed"
    UPDATED = "updated"
    IGNORED = "ignored"   # acknowledged, no position change (incl. AR-024 restated)
    ALERTED = "alerted"   # AR-057 amended - surfaced, no mutation
    UNKNOWN = "unknown"   # unrecognised exec_type (AR-023) - logged, never dropped


# --- canonical Logger events (routed to mod:Logger by the WS_Manager sole writer) -

@dataclass(frozen=True)
class PositionStateWrite:
    """POSITION_STATE_WRITE [INFO] {symbol, action, qty, avg_entry_price,
    fill_sequence_id, writer_id} - every mirror state write (the q5_logs line). The
    writer_id is always WRITER_ID (rule:HR-PM-009 enforcement marker)."""

    symbol: str
    action: PositionAction
    qty: Decimal
    avg_entry_price: Decimal
    fill_sequence_id: int | None = None
    writer_id: str = WRITER_ID
    code: str = field(default="POSITION_STATE_WRITE", init=False)


@dataclass(frozen=True)
class SoleWriterViolation:
    """POSITION_SOLE_WRITER_VIOLATION [CRITICAL] {symbol, attempted_writer} - a write
    was attempted by a writer other than WS_Manager (rule:HR-PM-009 defense-in-depth)."""

    attempted_writer: str
    method: str
    symbol: str | None = None
    code: str = field(default="POSITION_SOLE_WRITER_VIOLATION", init=False)


@dataclass(frozen=True)
class UnexpectedOrderAmended:
    """UNEXPECTED_ORDER_AMENDED [CRITICAL] {symbol, order_id} - an amended exec_type;
    TothBot never amends its own orders, so this is a system-integrity alert (AR-057).
    Cross-reference the mirror for emergSL-amend (crash-protection) risk; NO auto-correct."""

    symbol: str | None
    order_id: str | None
    code: str = field(default="UNEXPECTED_ORDER_AMENDED", init=False)


@dataclass(frozen=True)
class RestatedOrderAlert:
    """ORDER_RESTATED_ALERT [WARNING] {symbol, order_id} - a restated (engine-maintenance)
    exec_type arrived for a symbol that holds a resting position; surfaced as an elevated
    alert (AR-024). The mirror is NOT mutated."""

    symbol: str | None
    order_id: str | None
    code: str = field(default="ORDER_RESTATED_ALERT", init=False)


@dataclass(frozen=True)
class UnknownExecType:
    """UNKNOWN_EXEC_TYPE [WARNING] {raw_exec_type} - an exec_type outside the 10
    canonical values; logged, never silently dropped (AR-023)."""

    raw_exec_type: object
    code: str = field(default="UNKNOWN_EXEC_TYPE", init=False)


@dataclass(frozen=True)
class PositionClosedDuringGap:
    """POSITION_CLOSED_DURING_GAP [WARNING] {symbol} - a position open before a
    disconnect is absent from the reconnect snap_orders snapshot: it closed during the
    gap (emergSL fired while disconnected). WS_Manager fires the Trade Outcome Bus for
    it (AR-056)."""

    symbol: str
    position: Position
    code: str = field(default="POSITION_CLOSED_DURING_GAP", init=False)


@dataclass(frozen=True)
class ExecOutcome:
    """The result of one apply_execution() - the dispatched exec_type, the action
    taken, and the resulting position (None when closed / not position-affecting)."""

    exec_type: ExecType | None
    action: PositionAction
    position: Position | None = None


class SoleWriterViolationError(RuntimeError):
    """Raised when a mirror mutation is attempted by a writer other than WS_Manager
    (rule:HR-PM-009). The accompanying POSITION_SOLE_WRITER_VIOLATION event is emitted
    before the raise."""


class PositionMirror:
    """The O(1) symbol-keyed open-position store (rule:HR-PM-009 sole writer).

    Construct ONE per module (each parallel module mirrors its own positions; no
    cross-module contamination). WS_Manager - and only WS_Manager - drives the write
    surface (apply_execution / restore_from_snapshot, writer=WRITER_ID); every other
    module reads through the helper methods, which return the frozen Position record.
    """

    def __init__(self, *, on_event: EventSink | None = None) -> None:
        self._positions: dict[str, Position] = {}
        self._on_event = on_event

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    def _guard_writer(self, writer: str, method: str, symbol: str | None = None) -> None:
        """rule:HR-PM-009: only WS_Manager may write. Emit the CRITICAL violation
        event and raise on any other writer."""
        if writer != WRITER_ID:
            self._emit(SoleWriterViolation(attempted_writer=writer, method=method, symbol=symbol))
            raise SoleWriterViolationError(
                f"{method}: only {WRITER_ID!r} may write the Position Mirror (HR-PM-009); "
                f"got {writer!r}"
            )

    # --- read helpers (the sole-writer READ contract - rule:HR-PM-009) -----------
    def get(self, symbol: str) -> Position | None:
        """The open position for a symbol, or None. The record is frozen - a consumer
        cannot mutate the store through it."""
        return self._positions.get(symbol)

    def has_position(self, symbol: str) -> bool:
        return symbol in self._positions

    def __contains__(self, symbol: object) -> bool:
        return symbol in self._positions

    def __len__(self) -> int:
        return len(self._positions)

    def open_symbols(self) -> frozenset[str]:
        return frozenset(self._positions)

    def positions(self) -> dict[str, Position]:
        """A shallow COPY of the store (the Position records are frozen)."""
        return dict(self._positions)

    # --- write surface (WS_Manager only - rule:HR-PM-009) ------------------------
    def apply_execution(
        self,
        event: Mapping[str, object],
        *,
        writer: str,
        regime_at_entry: str | None = None,
        emergsl_id: str | None = None,
    ) -> ExecOutcome:
        """Dispatch one executions-channel frame through the WS-EXE-009 exec_type
        table and apply it to the store. regime_at_entry / emergsl_id are the
        TothBot-internal context the sole writer attaches when OPENING a position
        (they are not on the Kraken wire frame)."""
        self._guard_writer(writer, "apply_execution", _opt_str(event.get("symbol")))

        exec_type = classify_exec_type(event.get("exec_type"))
        if exec_type is None:
            self._emit(UnknownExecType(event.get("exec_type")))
            return ExecOutcome(None, PositionAction.UNKNOWN)

        if exec_type is ExecType.RESTATED:
            return self._handle_restated(event)
        if exec_type is ExecType.AMENDED:
            return self._handle_amended(event)
        if exec_type in _FILL_EXEC_TYPES:
            return self._handle_fill(exec_type, event, regime_at_entry, emergsl_id)
        # pending_new / new / canceled / expired / iceberg_refill / status:
        # acknowledged, no mirror position change (AR-053 registry is separate).
        return ExecOutcome(exec_type, PositionAction.IGNORED)

    def _handle_restated(self, event: Mapping[str, object]) -> ExecOutcome:
        """AR-024: a restated event MUST NOT mutate the mirror; surface an elevated
        alert only if a resting position exists for the symbol."""
        symbol = _opt_str(event.get("symbol"))
        if symbol is not None and symbol in self._positions:
            self._emit(RestatedOrderAlert(symbol, _opt_str(event.get("order_id"))))
        return ExecOutcome(ExecType.RESTATED, PositionAction.IGNORED)

    def _handle_amended(self, event: Mapping[str, object]) -> ExecOutcome:
        """AR-057: an amended event is unexpected (TothBot never amends its own
        orders) - CRITICAL alert, no mutation, no auto-correct."""
        self._emit(
            UnexpectedOrderAmended(_opt_str(event.get("symbol")), _opt_str(event.get("order_id")))
        )
        return ExecOutcome(ExecType.AMENDED, PositionAction.ALERTED)

    def _handle_fill(
        self,
        exec_type: ExecType,
        event: Mapping[str, object],
        regime_at_entry: str | None,
        emergsl_id: str | None,
    ) -> ExecOutcome:
        """A trade/filled fill: open, update, or close the symbol's position from the
        authoritative cum_qty + avg_price (WS-EXE-012)."""
        symbol = _opt_str(event.get("symbol"))
        fill_side = _side_from_fill(event.get("side"))
        if symbol is None or fill_side is None or event.get("cum_qty") is None:
            # A fill must carry symbol + side + cum_qty; a malformed frame is not a
            # position event but is never silently dropped.
            self._emit(UnknownExecType(event.get("exec_type")))
            return ExecOutcome(exec_type, PositionAction.UNKNOWN)

        cum_qty = _dec(event.get("cum_qty"))
        avg_price = _dec(event["avg_price"]) if event.get("avg_price") is not None else Decimal("0")
        seq = _opt_int(event.get("sequence"))
        existing = self._positions.get(symbol)

        if existing is None:
            return self._open(symbol, fill_side, cum_qty, avg_price, event, seq,
                              regime_at_entry, emergsl_id)
        if _closes(existing.side, fill_side):
            return self._close(existing, seq)
        # Same-side fill on an open position: cum_qty + avg_price are cumulative and
        # authoritative (AR-054 admits no separate adds; refresh defensively).
        return self._update(existing, cum_qty, avg_price, seq)

    def _open(
        self, symbol, side, qty, avg_price, event, seq, regime_at_entry, emergsl_id,
    ) -> ExecOutcome:
        position = Position(
            symbol=symbol,
            side=side,
            qty=qty,
            avg_entry_price=avg_price,
            cl_ord_id=_opt_str(event.get("cl_ord_id")),
            emergsl_id=emergsl_id,
            fill_sequence_id=seq,
            regime_at_entry=regime_at_entry,
        )
        self._positions[symbol] = position
        self._emit(PositionStateWrite(symbol, PositionAction.OPENED, qty, avg_price, seq))
        return ExecOutcome(_exec_of(event), PositionAction.OPENED, position)

    def _update(self, existing: Position, qty, avg_price, seq) -> ExecOutcome:
        position = replace(existing, qty=qty, avg_entry_price=avg_price, fill_sequence_id=seq)
        self._positions[existing.symbol] = position
        self._emit(PositionStateWrite(existing.symbol, PositionAction.UPDATED, qty, avg_price, seq))
        return ExecOutcome(ExecType.TRADE, PositionAction.UPDATED, position)

    def _close(self, existing: Position, seq) -> ExecOutcome:
        del self._positions[existing.symbol]
        self._emit(
            PositionStateWrite(
                existing.symbol, PositionAction.CLOSED, existing.qty, existing.avg_entry_price, seq
            )
        )
        return ExecOutcome(ExecType.FILLED, PositionAction.CLOSED, None)

    def restore_from_snapshot(
        self,
        snap_orders: Sequence[Mapping[str, object]],
        *,
        writer: str,
    ) -> list[PositionClosedDuringGap]:
        """AR-056 / startup Step 6: reconcile the mirror against the executions
        snapshot. Symbols open in the mirror but ABSENT from snap_orders closed during
        the disconnect gap (emergSL fired while down): they are removed and returned as
        POSITION_CLOSED_DURING_GAP for the Trade Outcome Bus. Symbols still present are
        retained verbatim (snap_orders cannot reconstruct avg_entry_price)."""
        self._guard_writer(writer, "restore_from_snapshot")
        snap_symbols = _snapshot_symbols(snap_orders)
        gap_closed: list[PositionClosedDuringGap] = []
        for symbol, position in list(self._positions.items()):
            if symbol not in snap_symbols:
                del self._positions[symbol]
                gap = PositionClosedDuringGap(symbol, position)
                gap_closed.append(gap)
                self._emit(gap)
        return gap_closed


def _closes(open_side: PositionSide, fill_side: PositionSide) -> bool:
    """A fill closes a position when it is on the opposite side (LONG closed by a sell
    -> SHORT-opening fill; SHORT closed by a buy -> LONG-opening fill)."""
    return open_side is not fill_side


def _snapshot_symbols(snap_orders: Sequence[Mapping[str, object]]) -> frozenset[str]:
    """The set of symbols carrying an open order in the executions snapshot."""
    return frozenset(
        s for s in (_opt_str(order.get("symbol")) for order in snap_orders) if s is not None
    )


def _exec_of(event: Mapping[str, object]) -> ExecType | None:
    return classify_exec_type(event.get("exec_type"))


def _opt_str(value: object) -> str | None:
    return None if value is None else str(value)


def _opt_int(value: object) -> int | None:
    return None if value is None else int(value)
