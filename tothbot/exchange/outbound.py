"""Outbound dispatch-seam I/O bodies - the live transmitter + the paper boundary.

Source: 0500000 dv1_241 sec 12 Image7 (contract:WSManager_Dispatch_Seam /
PA-004 divergence point #2) + sec 12.3 (the dispatch seam, GATE BEHAVIOR) + sec 7
container:Private_WS_v2 (OUTBOUND add_order/amend_order/batch_add/cancel).

The dispatch seam (seam.py) owns the mode GATE, the canary, and the events; it
delegates the actual I/O to two injected bodies. This module supplies the real
bodies the WSManager wires once (S2b shipped only the _unwired_* stubs):

  PrivateTransmitter   the LIVE body - WSManager._send_private(). It transmits the
                       already-built Kraken WS v2 order message over the SINGLE
                       private Transport (container:Private_WS_v2, ws-auth). The
                       socket is late-bound (bind/unbind): the private connection is
                       opened AFTER WSManager.__init__ (startup Step 5) and is
                       SWAPPED on every reconnect, so the transmitter holds the
                       current private Transport, not a frozen one. Transmitting
                       before the private connection exists is a defect (live must
                       have a private WS - PA-004 div #1), surfaced as
                       OutboundNotConnectedError, never a silent drop.

  PaperDispatchSimulator  the PAPER body - the boundary at which the local fill
                       simulator (contract:Synthetic_Capital_Ledger, PA-004 div #3)
                       plugs in. It transmits NOTHING to Kraken (paper places no
                       real orders; that is the whole point of the divergence) and
                       schedules the simulation via an INJECTED fill_simulator hook.
                       The full synthetic-capital fill simulator is a LATER slice
                       (Path B); until it is wired the hook is absent and the body
                       is a faithful no-op boundary that records the dispatched
                       (op, message) for audit - it does NOT raise (a paper order is
                       a valid, expected dispatch, not an error) and NEVER calls the
                       private transmitter (rule:HR-WM-023 - reaching the live branch
                       in paper is a session-halting defect, guarded by the seam
                       canary).

Both bodies are async: the live send awaits the async private Transport.send and
the paper hook may schedule an asyncio simulation task, so the seam dispatch
methods the callers await are themselves async (the rule:HR-EE-013 mode-opaque
caller awaits one coroutine regardless of mode).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, MutableSequence

from .seam import OutboundOp
from .transport import Transport


class OutboundNotConnectedError(RuntimeError):
    """The live transmitter was asked to send with no private Transport bound.

    In live mode the single private WS connection (PA-004 div #1) MUST be open
    before any order is dispatched (startup Step 5 precedes any pipeline fire,
    ar:AR-049). A send with no bound socket is a sequencing defect, surfaced here
    rather than silently dropped - an undispatched order leaves a sized-accepted
    candidate unexecuted.
    """


class PrivateTransmitter:
    """The live ``_send_private`` body: transmit an order message over the private WS.

    WSManager is the SOLE gatekeeper (sec 12.3): no other module may call this and
    no other module may hold the private Transport (NO DIRECT PRIVATE-WS ACCESS).
    The transmitter owns only the send edge - the message is already constructed by
    the seam method per the relevant AR rule (AR-006 add_order, AR-007 batch_add,
    AR-018 amend, etc.); here it goes on the wire (Image7 ``ws_private.send``).

    The private connection is assembled AFTER WSManager.__init__ and re-opened on
    every reconnect, so the socket is late-bound: the private_ws assembler calls
    bind() once the socket is live and re-binds the fresh Transport after each
    reconnect; unbind() clears it while disconnected.
    """

    def __init__(self) -> None:
        self._transport: Transport | None = None

    def bind(self, transport: Transport) -> None:
        """Bind (or re-bind on reconnect) the current live private Transport."""
        self._transport = transport

    def unbind(self) -> None:
        """Clear the bound socket (the private connection dropped / is reconnecting)."""
        self._transport = None

    @property
    def is_connected(self) -> bool:
        return self._transport is not None

    async def __call__(self, op: OutboundOp, message: dict) -> None:
        """Transmit one already-built order message over the private WS (live only).

        A TransportClosed from the send propagates: the private receive loop catches
        the same drop and drives the reconnect (rule:HR-WM-029); the caller sees the
        failed dispatch rather than a silently lost order."""
        if self._transport is None:
            raise OutboundNotConnectedError(
                f"no private WS bound for outbound {op.value} (live private connection "
                "not established - startup Step 5 / PA-004 div #1)"
            )
        await self._transport.send(message)


# The local-fill-simulation hook the paper boundary delegates to. The real body is
# contract:Synthetic_Capital_Ledger._simulate_entry_fill (PA-004 div #3), a LATER
# slice; until then no hook is wired and the boundary is a no-op.
FillSimulator = Callable[[OutboundOp, dict], Awaitable[None]]


class PaperDispatchSimulator:
    """The paper ``_simulate_*`` boundary: NO Kraken transmission; local sim hook.

    PA-004 divergence point #2 (order dispatch) - paper simulates orders locally and
    transmits nothing to Kraken (sec 12.3 PAPER MODE). This is the boundary at which
    the synthetic-capital fill simulator (PA-004 div #3, Path B) plugs in via the
    injected fill_simulator; the eventual fill it produces feeds the SAME
    WSManager.record_execution() surface the live executions stream feeds (D-06
    byte-identical Position Mirror sourcing).

    Until the fill simulator is wired this is a faithful no-op boundary: it captures
    the dispatched (op, message) (so the dispatch is observable / testable) and emits
    nothing on the wire. It NEVER reaches the live transmitter (rule:HR-WM-023; the
    seam canary is the defense-in-depth guard).
    """

    def __init__(
        self,
        *,
        fill_simulator: FillSimulator | None = None,
        log: MutableSequence[tuple[OutboundOp, dict]] | None = None,
    ) -> None:
        self._fill_simulator = fill_simulator
        # Append-only record of simulated dispatches (audit / test visibility); the
        # seam already emits evt:PAPER_ORDER_SIMULATED at the gate.
        self.simulated: MutableSequence[tuple[OutboundOp, dict]] = (
            log if log is not None else []
        )

    async def __call__(self, op: OutboundOp, message: dict) -> None:
        """Record the simulated dispatch and delegate to the fill simulator if wired.
        Transmits nothing to Kraken (paper places no real orders)."""
        self.simulated.append((op, message))
        if self._fill_simulator is not None:
            await self._fill_simulator(op, message)
