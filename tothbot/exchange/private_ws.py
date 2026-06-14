"""mod:WS_Manager private connection + executions ingest (live only; PA-004 div #1).

Source: 0500000 dv1_241 sec 7 container:Private_WS_v2 (executions/balances streams +
order-dispatch RPCs; order_status:true MANDATORY on every subscribe per rule:HR-WM-005;
snap_orders:true; ratecounter:true) + sec 2 Image1 AR-049 startup sequence steps 5/6
(connect private WS + subscribe executions/balances; snap_orders reconciliation ->
Position Mirror) + the ar:AR-056 / WS-REC-004 restore sequence (private subset).

This is the SINGLE private connection (NOT a public shard). It exists ONLY in live
mode (rule:HR-WM-022; paper keeps self._ws_private None for the whole session,
PA-004 divergence point #1). It closes the live fill -> mirror loop:

  inbound   the private receive loop routes each executions frame to the ingest,
            which feeds WSManager.record_execution() (the rule:HR-PM-009 sole-writer
            surface) per fill, and reconciles the snapshot (snap_orders) through
            WSManager.restore_position_mirror() (AR-056 / startup Step 6).
  outbound  on (re)connect it BINDS WSManager.transmitter to the fresh private
            Transport, so ws_private.send (the live order-dispatch body) transmits on
            the current socket (Image7 dispatch seam; sec 12.3 _send_private).
  reconnect its own ReconnectDriver runs the PRIVATE restore subset
            (build_private_restore_sequence): fresh token -> reconnect socket ->
            re-subscribe executions/balances -> reset rate ceiling -> resume keepalive
            -> RESTORE_POSITION_MIRROR from snap_orders.

All I/O is injected (the socket opener, the REST GetWebSocketsToken token acquire,
the snap_orders source, the per-step sleep, the clock) so the whole connect +
ingest + reconnect path is driven with stdlib asyncio.run over fakes - no network,
no real timers. The REST bodies (GetWebSocketsToken, GetOpenOrders) and the
maxratecount rate-ceiling unit are LATER slices; here they are injected edges.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field

from .channels import PrivateChannel
from .dispatch import DispatchTable, Handler
from .keepalive import ConnectionKeepalive
from .reconcile import ReconciliationTracker
from .reconnect import (
    DisconnectReason,
    RestoreStep,
    ShardReconnectCoordinator,
    build_private_restore_sequence,
)
from .reconnect_driver import ReconnectDriver
from .receive_loop import ShardReceiveLoop
from .transport import Transport

# The private connection is a single connection; it reuses the shard machinery with
# a fixed index so the one ShardReconnectCoordinator / ReconnectDriver drive it.
PRIVATE_SHARD_INDEX = 0

# Injected I/O edges.
OpenPrivateSocket = Callable[[], Awaitable[Transport]]   # open one private WS socket
AcquireToken = Callable[[], Awaitable[str]]              # REST GetWebSocketsToken (live)
# snap_orders source for the RESTORE_POSITION_MIRROR step (REST GetOpenOrders /
# executions snapshot). Returns the open-order snapshot to reconcile against.
FetchSnapOrders = Callable[[], Awaitable[Sequence[Mapping[str, object]]]]
BalancesHandler = Handler                                # balances frame consumer (ledger = Path B)
Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]
EventSink = Callable[[object], None]


# --- private subscribe frames (container:Private_WS_v2) ------------------------

def executions_subscribe(token: str) -> dict:
    """The Kraken WS v2 executions subscribe RPC. order_status:true is MANDATORY on
    EVERY subscribe incl reconnects (rule:HR-WM-005; without it amended+restated
    exec_types are silently dropped and Layer 3 monitoring is non-functional);
    snap_orders:true delivers the open-order snapshot for the AR-056 reconcile;
    ratecounter:true returns maxratecount (A-1 / AR-030)."""
    return {
        "method": "subscribe",
        "params": {
            "channel": "executions",
            "token": token,
            "snap_orders": True,
            "order_status": True,
            "ratecounter": True,
        },
    }


def balances_subscribe(token: str) -> dict:
    """The Kraken WS v2 balances subscribe RPC (real-time settlement; AR-050 per-module
    wallet sourcing). The synthetic/real capital ledger that consumes balances is a
    LATER slice (PA-004 div #3, Path B); this connection only subscribes the stream."""
    return {"method": "subscribe", "params": {"channel": "balances", "token": token}}


# --- canonical events ---------------------------------------------------------

@dataclass(frozen=True)
class PrivateConnected:
    """PRIVATE_WS_CONNECTED [INFO] {} - the single private connection opened + the
    executions/balances subscribes were issued (live only)."""

    code: str = field(default="PRIVATE_WS_CONNECTED", init=False)


@dataclass(frozen=True)
class PositionMirrorRestored:
    """POSITION_MIRROR_RESTORED [INFO] {gap_closed} - the RESTORE_POSITION_MIRROR step
    reconciled the mirror against snap_orders; gap_closed counts positions that closed
    during the disconnect (AR-056)."""

    gap_closed: int
    code: str = field(default="POSITION_MIRROR_RESTORED", init=False)


# --- executions ingest (the inbound fill -> mirror surface) -------------------

class ExecutionsIngest:
    """Routes private executions frames onto the WSManager mirror sole-writer surface.

    A Kraken executions channel frame is {channel:"executions", type:"snapshot"|"update",
    data:[...], sequence:N}. The data elements are the per-execution events.
      snapshot - the open-order snapshot (snap_orders): reconcile the mirror through
                 WSManager.restore_position_mirror() (AR-056 / startup Step 6). The
                 snapshot is also CAPTURED so the reconnect RESTORE_POSITION_MIRROR
                 step can reuse it when no REST GetOpenOrders source is injected.
      update   - one or more fill / cancel / amend events: each is fed to
                 WSManager.record_execution(), the WS-EXE-009 exec_type dispatch
                 (the mirror sole writer; rule:HR-PM-009).
    """

    def __init__(self, ws_manager, *, on_event: EventSink | None = None) -> None:
        self._wm = ws_manager
        self._on_event = on_event
        self.last_snap_orders: list[Mapping[str, object]] | None = None

    def __call__(self, message: dict) -> None:
        data = list(message.get("data") or [])
        if message.get("type") == "snapshot":
            self.last_snap_orders = data
            gap = self._wm.restore_position_mirror(data)
            self._emit(PositionMirrorRestored(len(gap)))
            return
        for event in data:
            self._wm.record_execution(event)

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)


def _noop_balances_handler(_frame: dict) -> None:
    """Default balances consumer: the seq-gap tracking happens in the receive loop;
    the wallet/ledger ingest (PA-004 div #3) is a LATER slice, so this is a no-op."""


# --- the assembled private connection ----------------------------------------

@dataclass
class PrivateConnection:
    """The wired, runnable private connection. run() drives the receive loop until
    stop(); the ReconnectDriver re-opens + re-subscribes + restores on a drop."""

    transport: Transport
    loop: ShardReceiveLoop
    ingest: ExecutionsIngest
    keepalive: ConnectionKeepalive
    coordinator: ShardReconnectCoordinator
    driver: ReconnectDriver

    async def run(self) -> None:
        await self.loop.run()

    def stop(self) -> None:
        self.loop.stop()


class PrivateConnectionAssembler:
    """Builds + runs the single private connection (live only; PA-004 div #1).

    Construct with the live WSManager (its mirror sole-writer surface + transmitter),
    the injected private-socket opener, the token acquire (REST GetWebSocketsToken),
    and optionally a snap_orders source (REST GetOpenOrders) + balances handler.
    build() acquires a token, opens the socket, binds the transmitter, issues the
    executions/balances subscribes, and wires the receive loop + the private
    ReconnectDriver; the returned PrivateConnection is ready to run().
    """

    def __init__(
        self,
        ws_manager,
        *,
        open_socket: OpenPrivateSocket,
        acquire_token: AcquireToken,
        fetch_snap_orders: FetchSnapOrders | None = None,
        balances_handler: BalancesHandler | None = None,
        coordinator: ShardReconnectCoordinator | None = None,
        on_event: EventSink | None = None,
        clock: Clock = time.monotonic,
        sleep: Sleep = asyncio.sleep,
        tick_interval: float = 1.0,
    ) -> None:
        if not ws_manager.is_live:
            # PA-004 divergence point #1 / rule:HR-WM-022: paper NEVER connects the
            # private WS. Building it in paper is a divergence-point violation.
            raise ValueError(
                "private WS connection is live-only (PA-004 div #1 / HR-WM-022); "
                "WSManager is in paper mode"
            )
        self._wm = ws_manager
        self._open_private_socket = open_socket
        self._acquire_token = acquire_token
        self._fetch_snap_orders = fetch_snap_orders
        self._balances_handler = balances_handler or _noop_balances_handler
        self._coordinator = coordinator or ShardReconnectCoordinator()
        self._on_event = on_event
        self._clock = clock
        self._sleep = sleep
        self._tick_interval = tick_interval

        self._ingest = ExecutionsIngest(ws_manager, on_event=on_event)
        self._keepalive = ConnectionKeepalive(clock=clock)
        self._token: str | None = None
        self._pending: Transport | None = None  # the freshest socket (build + reconnect)

        # The private connection's own driver runs the PRIVATE restore subset (token,
        # private re-subscribe, rate ceiling, RESTORE_POSITION_MIRROR) - never the
        # public-channel steps. paper_mode=False (this connection exists only in live).
        self._driver = ReconnectDriver(
            self._coordinator,
            paper_mode=False,
            open_socket=self._open_socket,
            run_step=self._run_step,
            sleep=sleep,
            on_event=on_event,
            restore_sequence=build_private_restore_sequence(),
        )

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    @property
    def driver(self) -> ReconnectDriver:
        return self._driver

    @property
    def ingest(self) -> ExecutionsIngest:
        return self._ingest

    # --- build (startup steps 5/6) -------------------------------------------
    async def build(self) -> PrivateConnection:
        """Token -> open socket + bind transmitter -> subscribe executions/balances ->
        wire the receive loop + private reconnect driver."""
        self._token = await self._acquire_token()
        transport = await self._open_socket(PRIVATE_SHARD_INDEX)
        await self._subscribe(transport, self._token)
        self._emit(PrivateConnected())

        dispatch = DispatchTable()
        dispatch.register(PrivateChannel.EXECUTIONS, self._ingest)
        dispatch.register(PrivateChannel.BALANCES, self._balances_handler)

        loop = ShardReceiveLoop(
            transport,
            dispatch,
            self._keepalive,
            recon=ReconciliationTracker(),  # executions/balances seq-gap (A-9/A-10)
            is_reconnecting=self._coordinator.any_reconnecting,  # rule:HR-WM-012
            initiate_reconnect=self._bind_reconnect(),
            on_event=self._on_event,
            clock=self._clock,
            tick_interval=self._tick_interval,
        )
        return PrivateConnection(
            transport=transport,
            loop=loop,
            ingest=self._ingest,
            keepalive=self._keepalive,
            coordinator=self._coordinator,
            driver=self._driver,
        )

    def _bind_reconnect(self):
        async def initiate(reason: DisconnectReason) -> Transport:
            return await self._driver.initiate(PRIVATE_SHARD_INDEX, reason)

        return initiate

    async def _subscribe(self, transport: Transport, token: str) -> None:
        """Issue the executions + balances subscribes on a socket (rule:HR-WM-005
        order_status:true). Only two RPCs - far under the AR-080 ceiling the public
        subscribe storm pacing defends - so they are sent directly."""
        await transport.send(executions_subscribe(token))
        await transport.send(balances_subscribe(token))

    # --- reconnect callbacks (the private restore subset) --------------------
    async def _open_socket(self, _shard_index: int) -> Transport:
        """RECONNECT_SOCKET (and the initial open): open the fresh private socket,
        bind the transmitter to it (so ws_private.send targets the live socket), and
        stash it for the in-restore re-subscribe."""
        transport = await self._open_private_socket()
        self._pending = transport
        self._wm.transmitter.bind(transport)
        return transport

    async def _run_step(self, _shard_index: int, step: RestoreStep) -> None:
        """Execute one private restore step (build_private_restore_sequence order)."""
        if step is RestoreStep.ACQUIRE_WS_TOKEN:
            # Fresh token every reconnect; never cached/reused (REST-WST-004 / WS-REC-004).
            self._token = await self._acquire_token()
        elif step is RestoreStep.RESUBSCRIBE_PRIVATE:
            assert self._pending is not None and self._token is not None
            await self._subscribe(self._pending, self._token)
        elif step is RestoreStep.RESET_RATE_CEILING:
            # AR-030: reset the maxratecount ceiling from the executions ACK. The
            # rate-counter unit is a LATER slice; no-op placeholder (carry-forward).
            pass
        elif step is RestoreStep.RESUME_KEEPALIVE:
            self._keepalive.reset()  # 30s ping + zombie timers (HR-WM-003/004)
        elif step is RestoreStep.RESTORE_POSITION_MIRROR:
            await self._restore_position_mirror()
        # No public-channel step is reachable here (private restore subset).

    async def _restore_position_mirror(self) -> None:
        """RESTORE_POSITION_MIRROR (AR-056 / startup Step 6): reconcile the mirror
        against snap_orders. Source = the injected REST GetOpenOrders edge if wired,
        else the last executions snapshot captured by the ingest. If neither exists
        the mirror is left untouched (never reconcile against an empty set - that
        would falsely gap-close every open position)."""
        if self._fetch_snap_orders is not None:
            snap = await self._fetch_snap_orders()
        else:
            snap = self._ingest.last_snap_orders
        if snap is None:
            return
        gap = self._wm.restore_position_mirror(snap)
        self._emit(PositionMirrorRestored(len(gap)))
