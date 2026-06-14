"""Paper fill simulator - the local synthetic-fill path (PA-004 div #3, paper side).

Source: 0500000 dv1_241 sec 12 Image7 PA-004(2) paper cell (_simulate_entry_fill /
_maybe_paper_fill) + PA-004(4) paper Position Mirror sourcing (rule:HR-WM-033) +
sec 12.4 (synthetic ledger) + sec 12.5 (paper exit routing) + the Execution_Engine
on-fill node (evt:PAPER_FILL_SIMULATED).

This is the body that plugs into the PaperDispatchSimulator boundary (outbound.py)
via the injected fill_simulator hook. In paper mode the dispatch seam transmits
NOTHING to Kraken (HR-WM-023); instead this simulator produces the synthetic fill
locally and feeds it to the SAME WSManager.record_execution() surface the live
executions stream feeds (decision:D-06_Paper_Live_Parity - the Position Mirror
schema and write path are byte-identical paper <-> live; only the SOURCE of the
fill differs, PA-004 div #4). The synthetic ledger debit/credit is applied through
the WSManager sole-writer methods (sec 12.4 single-owner spot_usd_balance).

What produces a fill (dispatched by OutboundOp):
  ADD_ORDER            the marketable-IOC entry. Under CR-03 it fills-or-kills
                       atomically at submission (AR-054 - no resting order, no
                       partial/split/dust), so the synthetic entry fills the whole
                       accepted qty at the submitted limit_price (the MPP-capped
                       marketable bound - the conservative paper fill price, since
                       paper has no real book). OPENS the symbol's position in the
                       mirror + applies the entry-fill DEBIT.
  DISPATCH_MARKET_SELL the L1a run-to-reversal / L2 MAE exit (PAIR_LIMIT_ONLY IOC at
                       best_bid). CLOSES the position in the mirror (opposite-side
                       fill) + applies the exit-fill CREDIT.
  others (BATCH_ADD / CANCEL_ORDER / AMEND_ORDER / BATCH_CANCEL)
                       produce NO immediate fill: the on-fill emergSL batch_add
                       places a resting stop that only fills later on a bbo touch
                       (the _maybe_paper_fill path, ticker-driven - a LATER slice),
                       and cancel/amend do not move a position. Clean no-op here.

The fill frame mirrors the Kraken WS v2 executions wire shape (the fields
mod:Position_Mirror reads, WS-EXE-009 / WS-EXE-012): exec_type, symbol, side,
cum_qty, avg_price, cl_ord_id, order_id (PAPER_FILL_ prefixed), sequence
(contract:Executions_Channel_Sequence monotone order). "live mimics the PAPER
simulator's order shape exactly" (Image7) - so the same field names serve both.

Async (the fill_simulator hook is awaited by the paper boundary); the work here is
synchronous state mutation, so it simply awaits nothing and returns.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum

from .seam import OutboundOp

# The WSManager sole-writer surfaces this simulator drives (injected as bound methods
# so paper_fill.py never imports ws_manager.py - no circular import). record_execution
# is the byte-identical Position Mirror write path (D-06); the ledger applies are the
# single-owner spot_usd_balance debit/credit (sec 12.4).
RecordExecution = Callable[[Mapping[str, object]], object]
ApplyEntryFill = Callable[..., object]
ApplyExitFill = Callable[..., object]
EventSink = Callable[[object], None]

# The OutboundOps that produce a synthetic fill in paper mode.
_ENTRY_OPS: frozenset[OutboundOp] = frozenset({OutboundOp.ADD_ORDER})
_EXIT_OPS: frozenset[OutboundOp] = frozenset({OutboundOp.DISPATCH_MARKET_SELL})


class PaperFillKind(Enum):
    ENTRY = "entry"
    EXIT = "exit"


@dataclass(frozen=True)
class PaperFillSimulated:
    """evt:PAPER_FILL_SIMULATED [INFO] - a synthetic fill was produced and written to
    the Position Mirror via the byte-identical record_execution surface. Payload mirrors
    the Execution_Engine on-fill log (symbol, side, fill_qty, fill_price, cl_ord_id)."""

    kind: PaperFillKind
    symbol: str
    side: str
    qty: object
    fill_price: object
    cl_ord_id: str | None
    sequence: int
    code: str = field(default="PAPER_FILL_SIMULATED", init=False)


@dataclass(frozen=True)
class PaperFillSkipped:
    """PAPER_FILL_SKIPPED [WARNING] - a fill-producing dispatch (entry/exit) arrived
    without the fields needed to simulate a fill (symbol / side / order_qty /
    limit_price). Under correct flow a well-formed marketable-IOC order always carries
    them, so a skip is a malformed-message defect - surfaced, never silently dropped."""

    op: OutboundOp
    reason: str
    code: str = field(default="PAPER_FILL_SKIPPED", init=False)


class PaperFillSimulator:
    """The injected fill_simulator hook: produce synthetic fills + drive the ledger.

    Constructed by WSManager in paper mode with its own sole-writer methods bound, so
    every mirror write and ledger mutation still flows through the single owner
    (rule:HR-PM-009 / rule:HR-WM-032). One per WSManager (i.e. per module wallet).
    """

    def __init__(
        self,
        *,
        record_execution: RecordExecution,
        apply_entry_fill: ApplyEntryFill,
        apply_exit_fill: ApplyExitFill,
        on_event: EventSink | None = None,
    ) -> None:
        self._record_execution = record_execution
        self._apply_entry_fill = apply_entry_fill
        self._apply_exit_fill = apply_exit_fill
        self._on_event = on_event
        # contract:Executions_Channel_Sequence - a monotone per-process fill sequence
        # so the synthetic frames carry ordering exactly as the live executions stream.
        self._sequence = 0

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    async def __call__(self, op: OutboundOp, message: dict) -> None:
        """The fill_simulator hook. Dispatch the paper order to a synthetic fill (entry
        or exit) or a clean no-op; transmits NOTHING to Kraken (the boundary already
        guarantees that - HR-WM-023)."""
        if op in _ENTRY_OPS:
            self._simulate_fill(op, message, PaperFillKind.ENTRY)
        elif op in _EXIT_OPS:
            self._simulate_fill(op, message, PaperFillKind.EXIT)
        # else: batch_add / cancel_order / amend_order / batch_cancel produce no
        # immediate fill (see module docstring) - clean no-op.

    def _simulate_fill(self, op: OutboundOp, message: dict, kind: PaperFillKind) -> None:
        params = _order_params(message)
        symbol = _opt_str(params.get("symbol"))
        side = _opt_str(params.get("side"))
        qty = params.get("order_qty")
        fill_price = params.get("limit_price")
        if symbol is None or side is None or qty is None or fill_price is None:
            self._emit(
                PaperFillSkipped(
                    op,
                    "missing one of symbol/side/order_qty/limit_price - cannot simulate a fill",
                )
            )
            return

        self._sequence += 1
        seq = self._sequence
        cl_ord_id = _opt_str(params.get("cl_ord_id"))
        # The synthetic executions frame - the SAME shape the live executions stream
        # pushes (WS-EXE-009 / WS-EXE-012). exec_type=filled: the marketable-IOC order
        # fully fills (entry) or the whole position is sold (exit); no partial (AR-054).
        frame: dict[str, object] = {
            "exec_type": "filled",
            "symbol": symbol,
            "side": side,
            "cum_qty": qty,
            "avg_price": fill_price,
            "cl_ord_id": cl_ord_id,
            "order_id": f"PAPER_FILL_{seq}",
            "sequence": seq,
        }
        # 1) Write the Position Mirror through the byte-identical sole-writer surface
        #    (D-06): entry OPENS, exit CLOSES (opposite-side fill).
        self._record_execution(frame)
        # 2) Apply the synthetic ledger debit/credit through the single owner (sec 12.4).
        if kind is PaperFillKind.ENTRY:
            self._apply_entry_fill(symbol, qty, fill_price)
        else:
            self._apply_exit_fill(
                symbol, qty, fill_price, exit_reason=_opt_str(params.get("exit_reason"))
            )
        self._emit(
            PaperFillSimulated(
                kind=kind,
                symbol=symbol,
                side=side,
                qty=qty,
                fill_price=fill_price,
                cl_ord_id=cl_ord_id,
                sequence=seq,
            )
        )


def _order_params(message: Mapping[str, object]) -> Mapping[str, object]:
    """The order parameters. Kraken WS v2 RPC wraps them in a "params" object
    ({"method": "add_order", "params": {...}}); accept a flat dict too (the field
    names are identical either way)."""
    params = message.get("params")
    if isinstance(params, Mapping):
        return params
    return message


def _opt_str(value: object) -> str | None:
    return None if value is None else str(value)
