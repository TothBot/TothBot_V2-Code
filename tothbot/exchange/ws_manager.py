"""mod:WS_Manager - the sole dispatch gatekeeper shell (PA-001).

Source: 0500000 dv1_240 sec 7 Image6 (mod:WS_Manager) + sec 2 Image1 + sec 12
Image7. WS_Manager is the SINGLE point through which all Kraken traffic flows:
every inbound push frame is routed by the O(1) dispatch table (dispatch.py),
every outbound order RPC passes through the paper/live seam (seam.py), and it is
the SOLE writer to mod:Position_Mirror (rule:HR-PM-009). That write authority is
wired here: WSManager holds the PositionMirror and is the ONLY caller that tags a
write with the WRITER_ID sentinel (position_mirror.py); every other module reads
the mirror through the helper methods below, never by direct dict access.

This S2b shell assembles the three pieces and binds the run mode ONCE at
construction (rule:HR-WM-021, immutable for the process lifetime):
  - one public WS connection (always);
  - one private WS connection ONLY in live mode - private WS is NEVER
    connected in paper (rule:HR-WM-022 / PA-004 divergence #1);
  - the inbound DispatchTable and the outbound DispatchSeam.

The outbound seam I/O bodies are WIRED (outbound.py): the live transmitter
(PrivateTransmitter / ws_private.send over the single private Transport) and the
paper boundary (PaperDispatchSimulator). The live transmitter's socket is
late-bound by the private_ws assembler once the private connection opens (startup
Step 5) and re-bound on each reconnect; WSManager exposes it as self.transmitter.

The PAPER capital path is now WIRED (PA-004 div #3 / #4, paper side). In paper mode
WSManager owns the synthetic spot_usd_balance (ledger.py, sec 12.4 single-owner) and
a PaperFillSimulator (paper_fill.py) is bound into the paper boundary's fill_simulator
hook: a paper dispatch produces a synthetic fill that writes the Position Mirror via
the byte-identical record_execution surface (D-06) and debits/credits the synthetic
ledger through the sole-writer methods below. WSManager is the SOLE writer of BOTH the
mirror (rule:HR-PM-009) and the ledger (rule:HR-WM-032).

DEFERRED: the _maybe_paper_fill bbo-touch emergSL fill (ticker-driven), the
Exit Controller TRADE_CLOSE net-P&L close path (sec 12.5), and the REST
contract:Reconciliation_REST fallback.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal

from .connection import ConnectionRole, WSConnection
from .dispatch import Channel, DispatchTable, Handler
from .ledger import LedgerUpdate, SyntheticCapitalLedger
from .outbound import PaperDispatchSimulator, PrivateTransmitter
from .paper_fill import PaperFillSimulator
from .position_mirror import (
    WRITER_ID,
    ExecOutcome,
    Position,
    PositionClosedDuringGap,
    PositionMirror,
)
from .seam import DispatchSeam, EventSink, LiveSender, PaperSimulator
from ..config import registry
from ..config.settings import Mode

# Default paper wallet seed (decision:D-05): $5,000 each for the Long + Short module
# wallets. Sourced from the registry (the two seeds are equal); the per-module assembler
# passes the wallet's own seed explicitly when it constructs each module's WSManager.
_DEFAULT_PAPER_STARTING_BALANCE: object = registry.value("paper_starting_balance_long_usd")


class WSManager:
    """The sole dispatch gatekeeper: inbound routing + outbound mode gate."""

    def __init__(
        self,
        mode: Mode,
        *,
        live_sender: LiveSender | None = None,
        paper_simulator: PaperSimulator | None = None,
        on_event: EventSink | None = None,
        paper_starting_balance: object | None = None,
    ) -> None:
        self._mode = mode  # frozen for process lifetime (rule:HR-WM-021)
        self._on_event = on_event

        # Public connection always exists; private exists ONLY in live mode
        # (rule:HR-WM-022 - private WS never connected in paper).
        self.public = WSConnection(ConnectionRole.PUBLIC)
        self.private: WSConnection | None = (
            None if mode is Mode.PAPER else WSConnection(ConnectionRole.PRIVATE)
        )

        # Inbound O(1) routing of the 7 channels.
        self.inbound = DispatchTable()

        # The live outbound transmitter (ws_private.send) - late-bound to the private
        # Transport by the private_ws assembler once the connection opens (startup
        # Step 5) and re-bound on each reconnect. Overridable by injection (tests).
        self.transmitter = PrivateTransmitter()

        # Sole-writer mirror of all open positions (rule:HR-PM-009). WSManager is the
        # only writer; the write source diverges by mode upstream (PA-004 div #4:
        # paper = local sim fills; live = executions) but is byte-identical here.
        self.positions = PositionMirror(on_event=on_event)

        # PAPER capital path (PA-004 div #3 / #4, paper side). In paper mode WSManager
        # owns the synthetic spot_usd_balance (sec 12.4 single-owner) and binds a
        # PaperFillSimulator into the paper boundary's fill_simulator hook so a paper
        # dispatch produces a synthetic fill -> record_execution (D-06) + ledger
        # debit/credit. In live mode there is no synthetic ledger (real Kraken balances
        # are authoritative) and the paper boundary is inert (the seam uses the
        # transmitter), so both stay None / a no-op boundary.
        self.ledger: SyntheticCapitalLedger | None = None
        self.paper_fill: PaperFillSimulator | None = None
        if mode is Mode.PAPER:
            seed = (
                paper_starting_balance
                if paper_starting_balance is not None
                else _DEFAULT_PAPER_STARTING_BALANCE
            )
            self.ledger = SyntheticCapitalLedger(seed, on_event=on_event)
            self.paper_fill = PaperFillSimulator(
                record_execution=self.record_execution,
                apply_entry_fill=self.apply_paper_entry_fill,
                apply_exit_fill=self.apply_paper_exit_fill,
                on_event=on_event,
            )
            self.paper_dispatch = PaperDispatchSimulator(fill_simulator=self.paper_fill)
        else:
            self.paper_dispatch = PaperDispatchSimulator()

        # Outbound order-dispatch mode gate (contract:WSManager_Dispatch_Seam).
        self.seam = DispatchSeam(
            mode,
            live_sender=live_sender or self.transmitter,
            paper_simulator=paper_simulator or self.paper_dispatch,
            on_event=on_event,
        )

    @property
    def mode(self) -> Mode:
        return self._mode

    @property
    def is_paper(self) -> bool:
        return self._mode is Mode.PAPER

    @property
    def is_live(self) -> bool:
        return self._mode is Mode.LIVE

    @property
    def has_private_connection(self) -> bool:
        """False in paper (rule:HR-WM-022); True in live."""
        return self.private is not None

    # --- inbound (read side) -------------------------------------------------
    def register_handler(self, channel: Channel, handler: Handler) -> None:
        """Bind the sole consumer for a channel's push frames."""
        self.inbound.register(channel, handler)

    def route_frame(self, name: str, frame: dict, interval: int | None = None) -> None:
        """Entry point for the WS read loop: resolve + dispatch one frame."""
        self.inbound.route(name, frame, interval)

    # --- Position Mirror: WSManager is the SOLE writer (rule:HR-PM-009) -------
    # These two methods are the ONLY callers that tag a mirror write with WRITER_ID,
    # so the sole-writer contract holds by construction: any other module that
    # reached for the mirror would have to forge the WS_Manager identity (and be
    # caught by the position_mirror sole-writer guard).
    def record_execution(
        self,
        event: Mapping[str, object],
        *,
        regime_at_entry: str | None = None,
        emergsl_id: str | None = None,
    ) -> ExecOutcome:
        """Apply one executions-channel frame to the mirror (WS-EXE-009 dispatch).
        The write source diverged by mode upstream (PA-004 div #4); the frame reaches
        here byte-identical in both paper and live."""
        return self.positions.apply_execution(
            event, writer=WRITER_ID, regime_at_entry=regime_at_entry, emergsl_id=emergsl_id
        )

    def restore_position_mirror(
        self, snap_orders: Sequence[Mapping[str, object]]
    ) -> list[PositionClosedDuringGap]:
        """Reconcile the mirror against the executions snapshot on reconnect/startup
        (AR-056 RESTORE_POSITION_MIRROR / Step 6); returns the gap-closed positions."""
        return self.positions.restore_from_snapshot(snap_orders, writer=WRITER_ID)

    # --- Synthetic Capital Ledger: WSManager is the SOLE writer (rule:HR-WM-032) --
    # The mirror image of the Position-Mirror sole-writer pattern: these are the ONLY
    # callers that tag a ledger write with WRITER_ID (sec 12.4 single-owner). Paper
    # mode only - in live the ledger is None (real Kraken balances are authoritative).
    def apply_paper_entry_fill(
        self, symbol: str, qty: object, entry_fill_price: object
    ) -> LedgerUpdate:
        """Debit the synthetic spot_usd_balance for a simulated entry fill (sec 12.4
        ENTRY-FILL DEBIT, FEE_TAKER_PCT). Paper mode only."""
        if self.ledger is None:
            raise RuntimeError(
                "apply_paper_entry_fill called with no synthetic ledger (live mode - "
                "real Kraken balances are authoritative; HR-WM-032 paper-only)"
            )
        return self.ledger.entry_fill_debit(symbol, qty, entry_fill_price, writer=WRITER_ID)

    def apply_paper_exit_fill(
        self, symbol: str, qty: object, exit_price: object, *, exit_reason: str | None = None
    ) -> LedgerUpdate:
        """Credit the synthetic spot_usd_balance for a simulated exit fill (sec 12.4
        EXIT-FILL CREDIT, FEE_TAKER_PCT). Paper mode only."""
        if self.ledger is None:
            raise RuntimeError(
                "apply_paper_exit_fill called with no synthetic ledger (live mode - "
                "real Kraken balances are authoritative; HR-WM-032 paper-only)"
            )
        return self.ledger.exit_fill_credit(
            symbol, qty, exit_price, writer=WRITER_ID, exit_reason=exit_reason
        )

    @property
    def spot_usd_balance(self) -> Decimal | None:
        """The synthetic spot_usd_balance (paper mode), or None in live mode where the
        real Kraken balance is authoritative (sec 12.4)."""
        return None if self.ledger is None else self.ledger.balance

    # --- Position Mirror read helpers (the rule:HR-PM-009 read contract) ------
    def position(self, symbol: str) -> Position | None:
        """The open position for a symbol (frozen record), or None."""
        return self.positions.get(symbol)

    def has_position(self, symbol: str) -> bool:
        return self.positions.has_position(symbol)

    def open_position_symbols(self) -> frozenset[str]:
        return self.positions.open_symbols()

    def open_positions(self) -> dict[str, Position]:
        """A copy of the open-position store (records are frozen)."""
        return self.positions.positions()
