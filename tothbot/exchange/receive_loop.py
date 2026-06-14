"""mod:WS_Manager per-shard receive loop - the inbound socket I/O edge.

Source: 0500000 dv1_241 sec 2 Image1 ("per-shard receive loops"; "transient WS
errors caught LOCALLY -> _initiate_reconnect") + sec 7 mod:WS_Manager desc (the D1
KEEPALIVE / SEQ-GAP / PUBLIC-DATA / PRIVATE-CHANNEL wire facts) + the canonical
Logger event_registry (WS_CONNECTED / PING_SENT / PONG_RECEIVED / PING_TIMEOUT /
ZOMBIE_CONNECTION_DETECTED / SUBSCRIPTION_ACK / UNKNOWN_MESSAGE_TYPE).

Each shard owns ONE connection and ONE receive loop (rule:HR-WM-029 shard
independence). The loop is the thin async shell that drives the already-built PURE
policy units at the socket edge - it holds NO timing or sequence logic of its own:

  - classify(message) is a PURE function: it sorts each raw Kraken WS v2 frame into
    PONG / HEARTBEAT / STATUS / SUBSCRIBE_ACK / CHANNEL_DATA / UNKNOWN by envelope
    shape, so the routing decisions are unit-testable without a socket.
  - handle_message() and on_tick() are SYNC: they apply the verdicts of
    ConnectionKeepalive (mod:keepalive), the SilentPairMachine registry
    (mod:silent_pair), and the ReconciliationTracker (mod:reconcile), and route
    real data through the DispatchTable (mod:dispatch). All the loop adds is the
    glue + the canonical event emission.
  - run()/_step() is the ONLY async surface: asyncio.wait_for(recv) with the tick
    interval as timeout, so liveness timers fire even under a continuous data
    stream; a TransportClosed (the transient drop) is caught LOCALLY and turned
    into a reconnect for THIS shard only.

ZOMBIE / liveness contract (A-7/A-8): only actual market-data frames reset the
zombie timer (keepalive.mark_real_data) - heartbeat, pong, status, and ACK frames
do NOT (rule:HR-WM-004 / AR-042). A sent application ping with no pong within 10 s
is a dead connection (WS-PING-002); no real data for > 90 s is a zombie
(WS-ZOM-003); both force a reconnect.

rule:HR-WM-012 (pipeline-no-fire / candle-discard): while ANY shard is mid-reconnect
the system clock must not fire the pipeline on a partial universe, so an ohlc_5m
candle frame arriving on the clock shard during a reconnect is NOT routed onward
(the candle is discarded). The frame still resets the zombie timer and feeds the
silent-pair machine - only the pipeline routing is suppressed.

DisconnectReason selection (WS-REC-003): a drop is Scenario B (post-maintenance,
5 s floor) only if a Kraken status frame announced engine maintenance before the
drop; otherwise it is Scenario A (random). The async reconnect mechanics
(backoff + the WS-REC-004 restore sequence) live in reconnect_driver.py; this loop
only selects the reason and awaits the injected initiate_reconnect, which returns
the fresh Transport to read from next.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import Enum

from .channels import PrivateChannel, PublicChannel
from .dispatch import Channel, DispatchTable, UnknownChannelError, channel_from_wire
from .keepalive import PING_MESSAGE, ConnectionKeepalive, Liveness
from .reconcile import ReconciliationTracker
from .reconnect import DisconnectReason
from .silent_pair import SilentPairMachine
from .transport import Transport, TransportClosed

Clock = Callable[[], float]
# The reconnect mechanism (reconnect_driver.initiate): given the disconnect
# reason, perform backoff + WS-REC-004 restore and return the fresh Transport.
InitiateReconnect = Callable[[DisconnectReason], Awaitable[Transport]]
EventSink = Callable[[object], None]


class MessageClass(Enum):
    """The envelope category of one inbound Kraken WS v2 frame."""

    PONG = "pong"                    # {"method":"pong", ...}             -> keepalive.mark_pong
    SUBSCRIBE_ACK = "subscribe_ack"  # {"method":"subscribe","result":..} -> silent_pair.mark_subscribed
    HEARTBEAT = "heartbeat"          # {"channel":"heartbeat"}            -> no-op (does NOT reset zombie)
    STATUS = "status"                # {"channel":"status","data":[...]}  -> connection_id + engine state
    CHANNEL_DATA = "channel_data"    # {"channel":<data>,"data":[...]}    -> mark_real_data + route
    UNKNOWN = "unknown"              # anything else                      -> UNKNOWN_MESSAGE_TYPE, never dropped


def classify(message: dict) -> MessageClass:
    """Sort a raw Kraken WS v2 frame into its envelope category (PURE).

    Kraken WS v2 splits request/response frames (carrying ``method``) from channel
    push frames (carrying ``channel``). pong and subscribe ACKs are method frames;
    heartbeat, status, and the data channels are push frames. An envelope matching
    neither shape is UNKNOWN (logged WARN, never silently dropped)."""
    method = message.get("method")
    if method == "pong":
        return MessageClass.PONG
    if method in ("subscribe", "unsubscribe"):
        return MessageClass.SUBSCRIBE_ACK
    channel = message.get("channel")
    if channel == "heartbeat":
        return MessageClass.HEARTBEAT
    if channel == "status":
        return MessageClass.STATUS
    if channel is not None:
        return MessageClass.CHANNEL_DATA
    return MessageClass.UNKNOWN


# --- canonical Logger events emitted at the loop level (event_registry codes) ----

@dataclass(frozen=True)
class WsConnected:
    """WS_CONNECTED [INFO] {connection_id, url} - logged on every new connection
    (WS-STAT-006 / AR-064; the connection_id arrives on the status frame)."""

    connection_id: int
    url: str | None = None
    code: str = field(default="WS_CONNECTED", init=False)


@dataclass(frozen=True)
class PingSent:
    """PING_SENT [INFO] {connection_id} - an application ping was transmitted (A-7)."""

    connection_id: int | None
    code: str = field(default="PING_SENT", init=False)


@dataclass(frozen=True)
class PongReceived:
    """PONG_RECEIVED [INFO] {connection_id} - a pong cleared the outstanding ping."""

    connection_id: int | None
    code: str = field(default="PONG_RECEIVED", init=False)


@dataclass(frozen=True)
class PingTimeout:
    """PING_TIMEOUT [CRITICAL] {connection_id} - no pong within 10 s; dead
    connection -> reconnect (WS-PING-002). Canonical registry code (the figure's
    event_registry names PING_TIMEOUT)."""

    connection_id: int | None
    code: str = field(default="PING_TIMEOUT", init=False)


@dataclass(frozen=True)
class ZombieDetected:
    """ZOMBIE_CONNECTION_DETECTED [CRITICAL] {connection_id, elapsed_seconds} - no
    real market data for > 90 s -> reconnect (WS-ZOM-003)."""

    connection_id: int | None
    elapsed_seconds: float
    code: str = field(default="ZOMBIE_CONNECTION_DETECTED", init=False)


@dataclass(frozen=True)
class SubscriptionAck:
    """SUBSCRIPTION_ACK [INFO] {channel, warnings} - a subscribe ACK; non-empty
    warnings[] are surfaced for the rule:HR-WM-019 warnings audit (AR-031)."""

    channel: str | None
    symbol: str | None
    success: bool
    warnings: tuple[str, ...]
    code: str = field(default="SUBSCRIPTION_ACK", init=False)


@dataclass(frozen=True)
class UnknownMessage:
    """UNKNOWN_MESSAGE_TYPE [WARNING] {raw_channel} - an unclassifiable frame; the
    A-12 / rule:HR-WM-006 never-silently-drop guarantee at the loop level."""

    raw_channel: object
    code: str = field(default="UNKNOWN_MESSAGE_TYPE", init=False)


@dataclass(frozen=True)
class TickAction:
    """Outcome of one on_tick() poll: what the async shell must do this iteration.
    reconnect_reason takes priority over send_ping (a dead/zombie link is not pinged)."""

    send_ping: bool = False
    reconnect_reason: DisconnectReason | None = None


# Public per-pair data channels whose frames carry a per-pair ``symbol`` that
# drives the silent-pair first-data liveness machine (instrument/status are global).
_PER_PAIR_DATA: frozenset[Channel] = frozenset(
    {PublicChannel.OHLC_5M, PublicChannel.OHLC_60M, PublicChannel.TICKER}
)
# The status-channel engine state that arms Scenario B (WS-STAT-002 / WS-REC-003).
_MAINTENANCE_STATE = "maintenance"


class ShardReceiveLoop:
    """The inbound async receive loop for one shard's WS connection (HR-WM-029).

    Construct with the shard's Transport, its DispatchTable, a ConnectionKeepalive,
    the shared silent-pair registry, an optional ReconciliationTracker (private
    connection only), the is_reconnecting gate (the coordinator's any_reconnecting,
    for rule:HR-WM-012), and the async initiate_reconnect (which returns the fresh
    Transport after restore). handle_message()/on_tick() carry the logic and are
    sync; run()/_step() are the only async surface.
    """

    def __init__(
        self,
        transport: Transport,
        dispatch: DispatchTable,
        keepalive: ConnectionKeepalive,
        *,
        silent_pairs: Mapping[str, SilentPairMachine] | None = None,
        recon: ReconciliationTracker | None = None,
        is_reconnecting: Callable[[], bool] | None = None,
        initiate_reconnect: InitiateReconnect | None = None,
        on_event: EventSink | None = None,
        clock: Clock = time.monotonic,
        tick_interval: float = 1.0,
    ) -> None:
        self._transport = transport
        self._dispatch = dispatch
        self._keepalive = keepalive
        self._silent_pairs = silent_pairs if silent_pairs is not None else {}
        self._recon = recon
        self._is_reconnecting = is_reconnecting or (lambda: False)
        self._initiate_reconnect = initiate_reconnect
        self._on_event = on_event
        self._clock = clock
        self._tick_interval = tick_interval

        self.connection_id: int | None = None
        self._maintenance_announced = False
        self._running = False

    # --- emission helper -----------------------------------------------------
    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    @property
    def transport(self) -> Transport:
        return self._transport

    # --- synchronous message handling (the testable core) --------------------
    def handle_message(self, message: dict, now: float) -> None:
        """Apply one already-received frame to the policy units + route it."""
        kind = classify(message)
        if kind is MessageClass.PONG:
            self._keepalive.mark_pong(now)
            self._emit(PongReceived(self.connection_id))
        elif kind is MessageClass.HEARTBEAT:
            pass  # a heartbeat does NOT reset the zombie timer (AR-042)
        elif kind is MessageClass.STATUS:
            self._handle_status(message)
        elif kind is MessageClass.SUBSCRIBE_ACK:
            self._handle_subscribe_ack(message, now)
        elif kind is MessageClass.CHANNEL_DATA:
            self._handle_channel_data(message, now)
        else:  # UNKNOWN - never silently dropped (A-12 / HR-WM-006)
            self._emit(UnknownMessage(message.get("channel", message.get("method"))))

    def _handle_status(self, message: dict) -> None:
        """status channel (WS-STAT-002/005/006): adopt the connection_id, track the
        engine maintenance state (arms Scenario B), then route to its handler. Does
        NOT reset the zombie timer (status is not market data)."""
        for elem in message.get("data") or []:
            cid = elem.get("connection_id")
            if cid is not None and cid != self.connection_id:
                self.connection_id = cid
                self._emit(WsConnected(cid, self._endpoint_url()))
            system = elem.get("system")
            if system is not None:
                self._maintenance_announced = system == _MAINTENANCE_STATE
        # Route to the registered status consumer if one is bound (sole-owner).
        if PublicChannel.STATUS in self._dispatch.registered_channels:
            self._dispatch.dispatch(PublicChannel.STATUS, message)

    def _handle_subscribe_ack(self, message: dict, now: float) -> None:
        """A subscribe/unsubscribe ACK: surface warnings[] (HR-WM-019) and, for a
        per-pair channel, arm the silent-pair first-data timer (mark_subscribed)."""
        result = message.get("result") or {}
        channel = result.get("channel")
        symbol = result.get("symbol")
        warnings = result.get("warnings") or message.get("warnings") or []
        success = bool(message.get("success", True))
        self._emit(SubscriptionAck(channel, symbol, success, tuple(warnings)))
        if symbol and symbol in self._silent_pairs:
            self._silent_pairs[symbol].mark_subscribed(now)

    def _handle_channel_data(self, message: dict, now: float) -> None:
        """A data push frame: resolve the channel, reset liveness, drive silent-pair
        + seq-gap policy, then route - subject to the HR-WM-012 candle-discard gate."""
        name = message.get("channel")
        data = message.get("data") or []
        interval = data[0].get("interval") if (name == "ohlc" and data) else None
        try:
            channel = channel_from_wire(name, interval)
        except UnknownChannelError:
            self._emit(UnknownMessage(name))
            return

        # Real market data: reset the zombie timer (A-8 - only real data does this).
        self._keepalive.mark_real_data(now)

        # Per-pair first-data liveness (silent-pair machine) for per-pair channels.
        if channel in _PER_PAIR_DATA:
            for elem in data:
                sym = elem.get("symbol")
                machine = self._silent_pairs.get(sym) if sym else None
                if machine is not None:
                    event = machine.mark_data(now)
                    if event is not None:
                        self._emit(event)

        # Private-channel sequence-gap detection (executions/balances; A-9/A-10).
        if self._recon is not None and channel in (
            PrivateChannel.EXECUTIONS,
            PrivateChannel.BALANCES,
        ):
            seq = message.get("sequence")
            if seq is not None:
                gap = (
                    self._recon.observe_executions(seq)
                    if channel is PrivateChannel.EXECUTIONS
                    else self._recon.observe_balances(seq)
                )
                if gap is not None:
                    self._emit(gap)  # carries alert_key + REST recovery_endpoint

        # rule:HR-WM-012: the system clock must not fire the pipeline on a partial
        # universe - discard an ohlc_5m candle that arrives while ANY shard is
        # mid-reconnect (liveness + silent-pair already applied above).
        if channel is PublicChannel.OHLC_5M and self._is_reconnecting():
            return

        self._dispatch.route(name, message, interval)

    # --- synchronous scheduler tick (the testable core) ----------------------
    def on_tick(self, now: float) -> TickAction:
        """Poll the timers: silent-pair T_silent expiry, then the keepalive verdict
        (dead/zombie -> reconnect) and ping scheduling. Reconnect outranks ping."""
        for machine in self._silent_pairs.values():
            event = machine.evaluate(now)
            if event is not None:
                self._emit(event)

        verdict = self._keepalive.liveness(now)
        if verdict is Liveness.DEAD_NO_PONG:
            self._emit(PingTimeout(self.connection_id))
            return TickAction(reconnect_reason=DisconnectReason.RANDOM)
        if verdict is Liveness.ZOMBIE:
            self._emit(
                ZombieDetected(self.connection_id, self._keepalive.seconds_since_real_data(now))
            )
            return TickAction(reconnect_reason=DisconnectReason.RANDOM)

        if self._keepalive.due_for_ping(now):
            return TickAction(send_ping=True)
        return TickAction()

    # --- async shell (the only I/O surface) ----------------------------------
    async def run(self) -> None:
        """Drive the shard until stop(): recv -> handle -> tick, reconnecting on a
        local TransportClosed (rule:HR-WM-029)."""
        self._running = True
        while self._running:
            await self._step()

    def stop(self) -> None:
        self._running = False

    async def _step(self) -> None:
        """One receive/handle/tick iteration. wait_for(recv) with the tick interval
        as timeout guarantees the liveness timers fire even under a steady stream."""
        try:
            message = await asyncio.wait_for(self._transport.recv(), timeout=self._tick_interval)
        except asyncio.TimeoutError:
            message = None
        except TransportClosed:
            await self._reconnect(self._reason_for_drop())
            return

        now = self._clock()
        if message is not None:
            self.handle_message(message, now)

        action = self.on_tick(now)
        if action.reconnect_reason is not None:
            await self._reconnect(action.reconnect_reason)
            return
        if action.send_ping:
            await self._send_ping(now)

    async def _send_ping(self, now: float) -> None:
        """Transmit the application ping and arm the 10 s pong deadline (A-7).
        A send failure is itself a dropped connection -> reconnect."""
        try:
            await self._transport.send(dict(PING_MESSAGE))
        except TransportClosed:
            await self._reconnect(self._reason_for_drop())
            return
        self._keepalive.mark_ping_sent(now)
        self._emit(PingSent(self.connection_id))

    def _reason_for_drop(self) -> DisconnectReason:
        """A confirmed-maintenance announcement before the drop selects Scenario B
        (5 s floor); otherwise Scenario A (random) - WS-REC-003."""
        return (
            DisconnectReason.MAINTENANCE
            if self._maintenance_announced
            else DisconnectReason.RANDOM
        )

    async def _reconnect(self, reason: DisconnectReason) -> None:
        """Tear down the dead socket and run the injected reconnect (backoff +
        WS-REC-004 restore), then read from the fresh Transport it returns. The
        maintenance flag is consumed once the reconnect is initiated."""
        await self._transport.close()
        self._maintenance_announced = False
        if self._initiate_reconnect is None:
            self._running = False  # no reconnect wired (e.g. a bring-up test) - stop
            return
        self._transport = await self._initiate_reconnect(reason)

    def _endpoint_url(self) -> str | None:
        url = getattr(self._transport, "url", None)
        return url if isinstance(url, str) else None
