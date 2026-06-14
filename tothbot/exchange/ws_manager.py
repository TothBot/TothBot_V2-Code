"""mod:WS_Manager - the sole dispatch gatekeeper shell (PA-001).

Source: 0500000 dv1_240 sec 7 Image6 (mod:WS_Manager) + sec 2 Image1 + sec 12
Image7. WS_Manager is the SINGLE point through which all Kraken traffic flows:
every inbound push frame is routed by the O(1) dispatch table (dispatch.py) and
every outbound order RPC passes through the paper/live seam (seam.py). It is
also the SOLE writer to mod:Position_Mirror (rule:HR-PM-009) - that write
authority is wired when Position_Mirror is built (later session).

This S2b shell assembles the three pieces and binds the run mode ONCE at
construction (rule:HR-WM-021, immutable for the process lifetime):
  - one public WS connection (always);
  - one private WS connection ONLY in live mode - private WS is NEVER
    connected in paper (rule:HR-WM-022 / PA-004 divergence #1);
  - the inbound DispatchTable and the outbound DispatchSeam.

DEFERRED to S2c (per TB00703): PATH-2 connection sharding, subscribe pacing,
reconnect orchestration, the silent-pair state machine, and the
contract:Reconciliation_REST fallback. The live transmitter and paper
simulator are injected here; their real implementations are built later.
"""

from __future__ import annotations

from .connection import ConnectionRole, WSConnection
from .dispatch import Channel, DispatchTable, Handler
from .seam import DispatchSeam, EventSink, LiveSender, OutboundOp, PaperSimulator
from ..config.settings import Mode


def _unwired_live_sender(op: OutboundOp, message: dict) -> None:
    raise NotImplementedError(
        f"live transmitter not wired (container:Private_WS_v2 send, S2c): {op.value}"
    )


def _unwired_paper_simulator(op: OutboundOp, message: dict) -> None:
    raise NotImplementedError(
        f"paper simulator not wired (contract:Synthetic_Capital_Ledger, S2c): {op.value}"
    )


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

        # Outbound order-dispatch mode gate (contract:WSManager_Dispatch_Seam).
        self.seam = DispatchSeam(
            mode,
            live_sender=live_sender or _unwired_live_sender,
            paper_simulator=paper_simulator or _unwired_paper_simulator,
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
