"""mod:WS_Manager PATH-2 startup assembler - builds + runs the public data layer.

Source: 0500000 dv1_241 sec 2 Image1 (the SHARD ASSIGNMENT + Subscribe Token Bucket
+ Per-Shard Reconnect Coordinator blocks) + sec 7 mod:WS_Manager q1_do/q4_triggers
("PATH 2 multi-shard owner: maintains N_conns shards ... per-shard subscribe
token-bucket ... silent-pair state machine ... per-shard reconnect coordinator ...
Triggered by startup (cold-start sequence - shard fan-out, channel subscribe
pacing)") + the AR-056 / WS-REC-004 restore sequence.

This is the ASSEMBLY edge that ties the already-built PURE units together into a
runnable data layer. It owns NO new policy - it only constructs and wires:

  build():  from a ShardPlan, opens N public sockets and constructs, per shard, a
            ConnectionKeepalive, the per-pair SilentPairMachine registry, a
            DispatchTable, and a ShardReceiveLoop bound to the SHARED
            ShardReconnectCoordinator (the one HR-WM-012 gate) and the SHARED
            ReconnectDriver. Then it runs the initial PACED subscribe over the
            process-singleton SubscribeTokenBucket (WM-PACE-001: one bucket for ALL
            shards - the ar:AR-080 ceiling is per-IP), so the startup subscribe storm
            is rate-bounded. This closes startup -> steady-state -> reconnect.

  reconnect: each shard's receive loop, on a local TransportClosed, awaits
            driver.initiate(shard_index, reason); the driver opens a fresh socket via
            this assembler's open_socket and runs the public restore steps via this
            assembler's run_step - RESUBSCRIBE_PUBLIC re-arms the shard's silent pairs
            (mark_shard_reconnect) and re-issues the paced subscribes on the fresh
            socket; RESUME_KEEPALIVE resets the keepalive timers.

PUBLIC-ONLY restore: a PATH-2 shard carries ONLY public channels (instrument/status
on shard 0; ohlc_5m + ticker per pair). The private executions/balances stream is a
SEPARATE single connection (live only; PA-004 div #1) assembled elsewhere, so the
private-side restore steps (token / private resub / rate ceiling / Position Mirror)
NEVER apply to a public shard. The shared driver is therefore built with the
public-only restore set regardless of run mode.

All I/O is injected (the socket opener, the per-step sleep, the monotonic clock) so
the whole startup + reconnect path is driven with stdlib asyncio.run over fakes - no
network, no real timers (the established test pattern).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from .channels import PublicChannel
from .dispatch import Channel, DispatchTable, Handler
from .keepalive import ConnectionKeepalive
from .pacing import SubscribeTokenBucket
from .reconnect import DisconnectReason, RestoreStep, ShardReconnectCoordinator
from .reconnect_driver import ReconnectDriver
from .receive_loop import ShardReceiveLoop
from .sharding import ShardAssignment, ShardPlan
from .silent_pair import SilentPairMachine
from .transport import Transport
from ..config.settings import Mode

# Open shard k's fresh PUBLIC socket (startup + every reconnect). Injected so the
# assembler is driven with a hand-built fake Transport in tests.
OpenShardSocket = Callable[[int], Awaitable[Transport]]
# Optionally bind the sole consumer for a (shard, channel) pair on that shard's
# DispatchTable. None -> no inbound handlers wired (a pure bring-up assembly).
HandlerProvider = Callable[[int, Channel], Handler]
Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]
EventSink = Callable[[object], None]

# Kraken WS v2 channel wire names + the ohlc interval discriminator (Image1).
_WIRE_NAME: dict[PublicChannel, str] = {
    PublicChannel.OHLC_5M: "ohlc",
    PublicChannel.OHLC_60M: "ohlc",
    PublicChannel.TICKER: "ticker",
    PublicChannel.INSTRUMENT: "instrument",
    PublicChannel.STATUS: "status",
}
_OHLC_INTERVAL: dict[PublicChannel, int] = {
    PublicChannel.OHLC_5M: 5,
    PublicChannel.OHLC_60M: 60,
}


@dataclass(frozen=True)
class SubscribeRequest:
    """One subscribe RPC: a channel and (for per-pair channels) its pair. Global
    channels (instrument, status) carry no symbol."""

    channel: PublicChannel
    symbol: str | None = None


def subscribe_requests(assignment: ShardAssignment) -> list[SubscribeRequest]:
    """The ordered subscribe RPCs for a shard: global channels first (shard 0 only),
    then one per (pair x per-pair channel). len() == assignment.subscribe_count -
    each RPC draws one subscribe token (rule:HR-WM-031 subscribe-count decoupling)."""
    reqs = [SubscribeRequest(ch) for ch in assignment.global_channels]
    for pair in assignment.pairs:
        for ch in assignment.per_pair_channels:
            reqs.append(SubscribeRequest(ch, pair))
    return reqs


def to_wire(req: SubscribeRequest) -> dict:
    """The Kraken WS v2 subscribe frame for a request. ohlc carries its interval
    (WS-OHLC-001); a fresh ticker subscribes event_trigger=trades (WS-TKR-002 default
    for a pair with no open position; flips to bbo on position open via WS-TKR-003)."""
    params: dict[str, object] = {"channel": _WIRE_NAME[req.channel]}
    if req.symbol is not None:
        params["symbol"] = [req.symbol]
    if req.channel in _OHLC_INTERVAL:
        params["interval"] = _OHLC_INTERVAL[req.channel]
    if req.channel is PublicChannel.TICKER:
        params["event_trigger"] = "trades"
    return {"method": "subscribe", "params": params}


# --- canonical pacing events (WM-PACE-005; routed to mod:Logger) ----------------

@dataclass(frozen=True)
class SubscribePaceWait:
    """SUBSCRIBE_PACE_WAIT [INFO] {shard_index, wait_seconds} - the subscribe loop
    blocked on the token bucket (evt:SUBSCRIBE_PACE_WAIT)."""

    shard_index: int
    wait_seconds: float
    code: str = field(default="SUBSCRIBE_PACE_WAIT", init=False)


@dataclass(frozen=True)
class SubscribePaceBatchComplete:
    """SUBSCRIBE_PACE_BATCH_COMPLETE [INFO] {shard_index, subscribe_count} - a shard's
    full subscribe batch finished (evt:SUBSCRIBE_PACE_BATCH_COMPLETE)."""

    shard_index: int
    subscribe_count: int
    code: str = field(default="SUBSCRIBE_PACE_BATCH_COMPLETE", init=False)


@dataclass
class ShardRuntime:
    """The wired objects for one shard (the receive loop drives the rest)."""

    shard_index: int
    assignment: ShardAssignment
    transport: Transport
    dispatch: DispatchTable
    keepalive: ConnectionKeepalive
    silent_pairs: dict[str, SilentPairMachine]
    loop: ShardReceiveLoop


class DataLayer:
    """The assembled, runnable public data layer: N shard runtimes over the shared
    coordinator + driver + subscribe token bucket. run() drives every shard loop
    concurrently; stop() halts them."""

    def __init__(
        self,
        shards: list[ShardRuntime],
        *,
        coordinator: ShardReconnectCoordinator,
        driver: ReconnectDriver,
        bucket: SubscribeTokenBucket,
        mode: Mode,
    ) -> None:
        self.shards = shards
        self.coordinator = coordinator
        self.driver = driver
        self.bucket = bucket
        self.mode = mode

    async def run(self) -> None:
        """Drive every shard's receive loop concurrently until stop()."""
        await asyncio.gather(*(s.loop.run() for s in self.shards))

    def stop(self) -> None:
        for shard in self.shards:
            shard.loop.stop()


class DataLayerAssembler:
    """Constructs + wires the PATH-2 public data layer from a ShardPlan.

    Construct with the plan, run mode, the injected public-socket opener, and the
    SHARED process-singleton SubscribeTokenBucket. build() opens the sockets, wires
    the shards, and runs the initial paced subscribe; the returned DataLayer is ready
    to run(). The single ReconnectDriver/coordinator are created here (or injected)
    and shared across every shard (rule:HR-WM-029 independence over one HR-WM-012 gate).
    """

    def __init__(
        self,
        plan: ShardPlan,
        *,
        mode: Mode,
        open_socket: OpenShardSocket,
        bucket: SubscribeTokenBucket,
        coordinator: ShardReconnectCoordinator | None = None,
        handler_provider: HandlerProvider | None = None,
        silent_pairs: dict[str, SilentPairMachine] | None = None,
        on_event: EventSink | None = None,
        clock: Clock = time.monotonic,
        sleep: Sleep = asyncio.sleep,
        tick_interval: float = 1.0,
    ) -> None:
        self._plan = plan
        self._mode = mode
        self._open_shard_socket = open_socket
        self._bucket = bucket
        self._coordinator = coordinator or ShardReconnectCoordinator()
        self._handler_provider = handler_provider
        # An optional SHARED silent-pair registry (pair -> machine). When the operational
        # assembly injects one, the same machines back the ws_state provider (the G1 gate
        # reads their subscription lifecycle); each shard takes its own pairs' machines from
        # it. When None, each shard owns a fresh per-shard registry (the standalone bring-up).
        self._silent_pairs_registry = silent_pairs
        self._on_event = on_event
        self._clock = clock
        self._sleep = sleep
        self._tick_interval = tick_interval

        # The single shared driver. A PATH-2 shard carries only public channels, so
        # the restore set is public-only (the private-side steps never apply); that is
        # exactly the build_restore_sequence(paper_mode=True) step set, regardless of
        # run mode. The private connection (live only) is assembled separately.
        self._driver = ReconnectDriver(
            self._coordinator,
            paper_mode=True,
            open_socket=self._open_socket,
            run_step=self._run_step,
            sleep=sleep,
            on_event=on_event,
        )
        # The latest socket opened per shard (startup + each reconnect): re-subscribes
        # during restore must target the FRESH socket, which the driver opens but holds
        # privately until restore completes.
        self._pending: dict[int, Transport] = {}
        self._shards: dict[int, ShardRuntime] = {}

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    @property
    def coordinator(self) -> ShardReconnectCoordinator:
        return self._coordinator

    @property
    def driver(self) -> ReconnectDriver:
        return self._driver

    # --- build (the startup cold-start sequence) -----------------------------
    async def build(self) -> DataLayer:
        """Open N public sockets, wire each shard, then run the initial paced
        subscribe across all shards over the shared token bucket."""
        runtimes: list[ShardRuntime] = []
        for assignment in self._plan.shards:
            runtimes.append(await self._build_shard(assignment))
        # Initial paced subscribe (shard 0 first - it carries the global channels).
        for runtime in runtimes:
            await self._paced_subscribe(
                runtime.shard_index,
                self._pending[runtime.shard_index],
                subscribe_requests(runtime.assignment),
            )
        return DataLayer(
            runtimes,
            coordinator=self._coordinator,
            driver=self._driver,
            bucket=self._bucket,
            mode=self._mode,
        )

    async def _build_shard(self, assignment: ShardAssignment) -> ShardRuntime:
        k = assignment.shard_index
        transport = await self._open_socket(k)

        keepalive = ConnectionKeepalive(clock=self._clock)
        if self._silent_pairs_registry is not None:
            silent_pairs = {
                pair: self._silent_pairs_registry.setdefault(
                    pair, SilentPairMachine(clock=self._clock)
                )
                for pair in assignment.pairs
            }
        else:
            silent_pairs = {
                pair: SilentPairMachine(clock=self._clock) for pair in assignment.pairs
            }
        dispatch = DispatchTable()
        if self._handler_provider is not None:
            for channel in (*assignment.global_channels, *assignment.per_pair_channels):
                dispatch.register(channel, self._handler_provider(k, channel))

        loop = ShardReceiveLoop(
            transport,
            dispatch,
            keepalive,
            silent_pairs=silent_pairs,
            is_reconnecting=self._coordinator.any_reconnecting,  # rule:HR-WM-012
            initiate_reconnect=self._bind_reconnect(k),
            on_event=self._on_event,
            clock=self._clock,
            tick_interval=self._tick_interval,
        )
        runtime = ShardRuntime(
            shard_index=k,
            assignment=assignment,
            transport=transport,
            dispatch=dispatch,
            keepalive=keepalive,
            silent_pairs=silent_pairs,
            loop=loop,
        )
        self._shards[k] = runtime
        return runtime

    def _bind_reconnect(self, shard_index: int):
        """Bind shard_index into the receive-loop initiate_reconnect callback (the loop
        passes only the reason; the driver needs the shard)."""
        async def initiate(reason: DisconnectReason) -> Transport:
            return await self._driver.initiate(shard_index, reason)

        return initiate

    # --- reconnect callbacks the shared driver invokes (per shard) -----------
    async def _open_socket(self, shard_index: int) -> Transport:
        """RECONNECT_SOCKET (and the initial open): open shard_index's fresh public
        socket and stash it so the in-restore re-subscribe targets it."""
        transport = await self._open_shard_socket(shard_index)
        self._pending[shard_index] = transport
        return transport

    async def _run_step(self, shard_index: int, step: RestoreStep) -> None:
        """Execute one public restore step for a shard (WS-REC-004 public subset)."""
        runtime = self._shards[shard_index]
        if step is RestoreStep.RESUBSCRIBE_PUBLIC:
            # The reconnecting shard's pairs re-arm their first-data timer
            # ((any) -> SUBSCRIBED) and re-subscribe on the fresh socket, paced.
            for machine in runtime.silent_pairs.values():
                machine.mark_shard_reconnect()
            await self._paced_subscribe(
                shard_index, self._pending[shard_index], subscribe_requests(runtime.assignment)
            )
        elif step is RestoreStep.RESUME_KEEPALIVE:
            runtime.keepalive.reset()  # resume 30s ping + zombie timers (HR-WM-003/004)
        elif step is RestoreStep.RESTORE_TICKER_TRIGGER:
            pass  # per-pair bbo/trades event_trigger restore is position-state driven (later)
        # Any private-side step is unreachable here (public-only restore set).

    # --- the paced subscribe path (WM-PACE-001 process-singleton bucket) ------
    async def _paced_subscribe(
        self, shard_index: int, transport: Transport, requests: list[SubscribeRequest]
    ) -> None:
        """Issue every subscribe RPC through the SHARED token bucket - NO bypass, NO
        priority lane (even reconnect re-subscribes are paced, since that is when the
        ar:AR-080 ceiling is most at risk)."""
        for req in requests:
            while not self._bucket.try_acquire():
                wait = self._bucket.time_until_next()
                self._emit(SubscribePaceWait(shard_index, wait))
                await self._sleep(wait)
            await transport.send(to_wire(req))
        self._emit(SubscribePaceBatchComplete(shard_index, len(requests)))
