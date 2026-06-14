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

The outbound seam I/O bodies are now WIRED (outbound.py): the live transmitter
(PrivateTransmitter / ws_private.send over the single private Transport) and the
paper boundary (PaperDispatchSimulator). The live transmitter's socket is
late-bound by the private_ws assembler once the private connection opens (startup
Step 5) and re-bound on each reconnect; WSManager exposes it as self.transmitter.

DEFERRED: the synthetic-capital fill simulator (contract:Synthetic_Capital_Ledger,
PA-004 div #3, Path B) that plugs into the paper boundary, and the REST
contract:Reconciliation_REST fallback.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from .connection import ConnectionRole, WSConnection
from .dispatch import Channel, DispatchTable, Handler
from .outbound import PaperDispatchSimulator, PrivateTransmitter
from .position_mirror import (
    WRITER_ID,
    ExecOutcome,
    Position,
    PositionClosedDuringGap,
    PositionMirror,
)
from .seam import DispatchSeam, EventSink, LiveSender, PaperSimulator
from ..config.settings import Mode


class WSManager:
    """The sole dispatch gatekeeper: inbound routing + outbound mode gate."""

    def __init__(
        self,
        mode: Mode,
        *,
        live_sender: LiveSender | None = None,
        paper_simulator: PaperSimulator | None = None,
        on_event: EventSink | None = None,
    ) -> None:
        self._mode = mode  # frozen for process lifetime (rule:HR-WM-021)

        # Public connection always exists; private exists ONLY in live mode
        # (rule:HR-WM-022 - private WS never connected in paper).
        self.public = WSConnection(ConnectionRole.PUBLIC)
        self.private: WSConnection | None = (
            None if mode is Mode.PAPER else WSConnection(ConnectionRole.PRIVATE)
        )

        # Inbound O(1) routing of the 7 channels.
        self.inbound = DispatchTable()

        # The outbound seam I/O bodies. The live transmitter (ws_private.send) is
        # late-bound to the private Transport by the private_ws assembler once the
        # connection opens (startup Step 5) and re-bound on each reconnect; the paper
        # boundary is the contract:Synthetic_Capital_Ledger plug point. Either may be
        # overridden by injection (tests / a custom fill simulator).
        self.transmitter = PrivateTransmitter()
        self.paper_dispatch = PaperDispatchSimulator()

        # Outbound order-dispatch mode gate (contract:WSManager_Dispatch_Seam).
        self.seam = DispatchSeam(
            mode,
            live_sender=live_sender or self.transmitter,
            paper_simulator=paper_simulator or self.paper_dispatch,
            on_event=on_event,
        )

        # Sole-writer mirror of all open positions (rule:HR-PM-009). WSManager is the
        # only writer; the write source diverges by mode upstream (PA-004 div #4:
        # paper = local sim fills; live = executions) but is byte-identical here.
        self._on_event = on_event
        self.positions = PositionMirror(on_event=on_event)

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
