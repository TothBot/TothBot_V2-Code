"""The capstone WS wiring - bind the data layer to the live organism + the AR-049 startup assembly.

Source: 0500000 dv1_250 ar:AR-049 (the cold-start startup sequence) + Image1 (the public WS channels)
+ Image6 (the Complete System) + contract:OHLC_5m_System_Clock. This is the TOP-LEVEL composition
edge: it ties the already-built PURE/edge units into a single runnable public-WS organism. It owns NO
new policy - only construction order + the channel->consumer binding.

TWO deliverables:

 make_public_handler_provider:  the DataLayerAssembler handler_provider (sec 7 the SOLE dispatch
   gatekeeper). It binds each public channel to its sole consumer:
       INSTRUMENT -> instrument_cache.ingest   (A-17 status/marginable/minimums snapshot+updates)
       TICKER     -> bbo_cache.ingest          (ar:AR-048 best bid/ask)
       OHLC_5M    -> driver.ohlc_5m_handler()  (the system clock: detect close -> step -> sweep)
       OHLC_60M   -> driver.ohlc_60m_handler() (the 1H HTF feed: advance EMAs -> EC-L1A-001)
       STATUS     -> status_handler            (the Kraken engine-state broadcast; default no-op)
   The same shared caches/driver back every shard, so the provider maps by channel (shard_index is
   accepted for the HandlerProvider contract but does not change the binding).

 assemble_operational:  the ar:AR-049 startup sequence end state. In PAPER it is the public data
   layer alone; in LIVE it ALSO builds the separate private executions/balances connection (PA-004
   div #1 / HR-WM-022 - paper never connects the private WS), so OperationalSystem.run() drives both.
   The phases:
     1. REST WARM-UP (warmup.py)        - GetOHLCData(5)/(60) seed the LiveIndicators + HtfCache.
     2. REST DAILY REGIME (scheduler.py) - GetOHLCData(1440) per pair + the BTC/USD anchor -> the
        RegimeCache (also drives EC-L1A-002 for any open position via wm.on_regime_classified).
     3. REST LIQUIDITY PROBE (LiquidityProbe) - GetTicker 24h USD volume -> the LiquidityCache.
     4. make_live_providers over the caches + the CIATS seed stores (OPS-1: the DEC-124
        expected_reward + DEC-128 mpp stores are SEEDED IN PHASES 1-2 from the 5m / daily series
        those phases already fetch - CIATS owns the values from the first tick) + the ws_state
        machine lifecycle.
     5. the LiveSweepDriver over the warmed pairs + the RegimeCache + the providers.
     6. build the DataLayerAssembler with the handler_provider bound + the SHARED silent-pair
        registry (so ws_state reads the same machines the receive loop drives), open the sockets, and
        run the initial paced subscribe. The returned DataLayer.run() drives the organism.

   The instrument/ticker SNAPSHOTS arrive over the WS after subscribe (during run); until a pair's
   caches are populated the providers raise ProviderNotReady and the sweep cleanly SKIPS that tick
   (no crash) - so building the WS layer last and running it is faithful to AR-049 (the snapshot is
   ready well before the first 5m close that reads it). rule:HR-WM-012 still gates the sweep during a
   reconnect (the receive loop discards OHLC_5M; the driver guard is belt-and-suspenders).

   The wiring cycle (ws_state needs the silent-pair machines; the assembler creates them) is resolved
   by pre-creating ONE shared machine registry for the universe and injecting it into BOTH the
   ws_state provider and the assembler. The coordinator is created first and injected so the driver's
   HR-WM-012 guard and the assembler share the one reconnect gate. All I/O is injected (the REST
   client, the socket opener, the sleeps, the clocks) - the whole assembly is driven under stdlib
   asyncio.run over fakes, no network, no real timers.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import datetime

from ..ciats.conductor import ApprovalInbox, CiatsConductor
from ..ciats.parameter_store import ParameterStore
from ..ciats.pool import CiatsPool
from ..ciats.regime_library import RegimeLibrary
from ..config.settings import Mode
from ..exchange.assembler import DataLayer, DataLayerAssembler
from ..exchange.bbo_cache import BboCache
from ..exchange.channels import PublicChannel
from ..exchange.dispatch import Channel, Handler
from ..exchange.instrument_cache import InstrumentCache
from ..exchange.liquidity_cache import LiquidityCache, LiquidityProbe
from ..exchange.pacing import SubscribeTokenBucket
from ..exchange.position_mirror import PositionSide
from ..exchange.private_ws import PrivateConnection, PrivateConnectionAssembler
from ..exchange.reconnect import ShardReconnectCoordinator
from ..exchange.sharding import ShardPlan
from ..exchange.silent_pair import SilentPairMachine
from ..exchange.transport import Transport
from ..exchange.warmup import WarmupOrchestrator
from ..regime.scheduler import DailyRegimeCompute, RegimeCache
from .live_driver import (
    LiveSweepDriver,
    make_approval_alert_sink,
    make_ciats_learning_sink,
)
from .providers import (
    make_cycle_parameters_provider,
    make_expected_reward_provider,
    make_live_providers,
    make_mpp_provider,
    make_ws_state_provider,
)
from .sweep import LiveProviders

EventSink = Callable[[object], None]
OpenShardSocket = Callable[[int], Awaitable[Transport]]
Sleep = Callable[[float], Awaitable[None]]
MonoClock = Callable[[], float]
WallClock = Callable[[], float]
UtcClock = Callable[[], datetime]


def _noop_handler(_frame: dict) -> None:
    """The default STATUS handler: the Kraken engine-state broadcast needs no cache write here
    (WS-STAT-002 maintenance arming is the receive loop's Scenario-B concern, not a channel sink)."""


def make_public_handler_provider(
    *,
    instrument_cache: InstrumentCache,
    bbo_cache: BboCache,
    driver: LiveSweepDriver,
    status_handler: Handler | None = None,
) -> Callable[[int, Channel], Handler]:
    """The DataLayerAssembler handler_provider binding each public channel to its sole consumer.

    instrument/ticker frames ingest into the shared caches; ohlc(5m)/ohlc(60m) drive the
    LiveSweepDriver. The same shared caches/driver serve every shard, so the binding is by channel
    (shard_index satisfies the HandlerProvider signature but does not vary the handler). A request
    for a channel with no public consumer raises (a wiring bug, never silently dropped)."""
    handlers: dict[Channel, Handler] = {
        PublicChannel.INSTRUMENT: instrument_cache.ingest,
        PublicChannel.TICKER: bbo_cache.ingest,
        PublicChannel.OHLC_5M: driver.ohlc_5m_handler(),
        PublicChannel.OHLC_60M: driver.ohlc_60m_handler(),
        PublicChannel.STATUS: status_handler or _noop_handler,
    }

    def handler_provider(shard_index: int, channel: Channel) -> Handler:
        try:
            return handlers[channel]
        except KeyError:
            raise ValueError(f"no public handler for channel {channel} on shard {shard_index}") from None

    return handler_provider


def assemble_ciats_modules(
    logger, *, on_event: EventSink | None = None, on_approval: "Callable | None" = None
) -> "tuple[dict[PositionSide, CiatsConductor], dict[PositionSide, Callable[[object], None]], dict[PositionSide, ApprovalInbox]]":
    """Construct the per-MODULE CIATS brain (one CiatsConductor + one TRADE_CLOSE learning sink + one
    operator ApprovalInbox per wallet, Long + Short - NO cross-module pooling, sec 7). Each conductor
    composes a fresh CiatsPool + RegimeLibrary + ParameterStore; `on_event` sinks its CIATS events to
    mod:Logger Stream-1.

    THE HR-CI-011 APPROVAL EDGE: `on_approval` is the approval surface - it defaults to the mod:Logger
    HR-LG-009 operator-alert seam (make_approval_alert_sink), so a staged evt:CIATS_APPROVAL_REQUESTED
    is routed to Bill (the SMTP operator surface). The approval RETURN lands in the per-module
    ApprovalInbox (the injected operator edge); the conductor polls it at each inter-trade boundary.

    THE LEARNING + BOUNDARY MEMBRANE: the learning sink (make_ciats_learning_sink) is the per-module
    membrane the exit path emits a TRADE_CLOSE through - it records to mod:Logger (Stream-1 + the
    module's Stream-2 corpus), drives conductor.ingest_close (the learning-loop close), AND - because a
    confirmed close is the HR-CI-003 inter-trade boundary - polls the inbox so an approved change
    reaches the Parameter Store at the right moment (never auto-applied). Returns (conductors, sinks,
    inboxes) keyed by side."""
    approval_edge = on_approval or make_approval_alert_sink(logger)
    conductors: dict[PositionSide, CiatsConductor] = {}
    sinks: dict[PositionSide, "Callable[[object], None]"] = {}
    inboxes: dict[PositionSide, ApprovalInbox] = {}
    for side in (PositionSide.LONG, PositionSide.SHORT):
        conductor = CiatsConductor(
            module=side.value,
            pool=CiatsPool(),
            regime_library=RegimeLibrary(),
            parameter_store=ParameterStore(),
            on_event=on_event,
            on_approval=approval_edge,
        )
        inbox = ApprovalInbox()
        conductors[side] = conductor
        inboxes[side] = inbox
        sinks[side] = make_ciats_learning_sink(logger, side.value, conductor, inbox=inbox)
    return conductors, sinks, inboxes


@dataclass
class OperationalSystem:
    """The assembled, runnable organism: call run() to drive the public data layer (and, in LIVE
    mode, the private executions/balances connection) concurrently; stop() halts both. The component
    handles are exposed for inspection. `conductors` + `ciats_sinks` are the per-module CIATS brain
    (the learning loop + the TRADE_CLOSE learning membrane, per side); `approval_inboxes` are the
    per-module operator HR-CI-011 inboxes (Bill submits a yes/no, the conductor applies it at the
    next inter-trade boundary); the providers' per-cycle Parameter_Store_Snapshot is backed by the
    conductors' stores. private_connection is None in paper (PA-004 div #1)."""

    data_layer: DataLayer
    driver: LiveSweepDriver
    providers: LiveProviders
    regime_cache: RegimeCache
    warmups: dict
    silent_pairs: dict[str, SilentPairMachine]
    instrument_cache: InstrumentCache
    bbo_cache: BboCache
    liquidity_cache: LiquidityCache
    conductors: dict[PositionSide, CiatsConductor]
    ciats_sinks: dict[PositionSide, Callable[[object], None]]
    approval_inboxes: dict[PositionSide, ApprovalInbox]
    private_connection: PrivateConnection | None = None

    async def run(self) -> None:
        """Drive the public data layer (and the live private connection, if present) concurrently."""
        runners = [self.data_layer.run()]
        if self.private_connection is not None:
            runners.append(self.private_connection.run())
        await asyncio.gather(*runners)

    def stop(self) -> None:
        self.data_layer.stop()
        if self.private_connection is not None:
            self.private_connection.stop()


async def assemble_operational(
    *,
    universe: Sequence[str],
    rest_client,
    open_socket: OpenShardSocket,
    bucket: SubscribeTokenBucket,
    wm,
    logger,
    mpp_store,
    reward_store,
    mode: Mode = Mode.PAPER,
    instrument_cache: InstrumentCache | None = None,
    bbo_cache: BboCache | None = None,
    liquidity_cache: LiquidityCache | None = None,
    on_event: EventSink | None = None,
    status_handler: Handler | None = None,
    mono_clock: MonoClock = time.monotonic,
    wall_clock: WallClock = time.time,
    now_utc: UtcClock | None = None,
    rest_sleep: Sleep = asyncio.sleep,
    pace_sleep: Sleep = asyncio.sleep,
    open_private_socket: "Callable | None" = None,
    acquire_token: "Callable | None" = None,
    fetch_snap_orders: "Callable | None" = None,
    balances_handler: Handler | None = None,
) -> OperationalSystem:
    """Run the ar:AR-049 cold-start sequence and return the runnable public organism (see module
    docstring). `on_event` (defaulting to logger.record) sinks the warm-up / regime / liquidity /
    pacing telemetry; `logger` is mod:Logger (the sweep's per-module Stream-2 sink). The CIATS seed
    stores are passed in EMPTY and seeded in-line during phases 1-2 from the historical bars those
    phases fetch (OPS-1) - no separate seeding pass, no duplicate REST."""
    universe = list(universe)
    instrument_cache = instrument_cache or InstrumentCache()
    bbo_cache = bbo_cache or BboCache()
    liquidity_cache = liquidity_cache or LiquidityCache()
    event_sink: EventSink = on_event or logger.record

    # The scheduler (EC-L1A-002) + the driver's 1H close (on_htf_ohlc_close) want a (bid, ask)
    # 2-tuple; BboCache.bbo returns None on a miss (no ticker yet at startup), so adapt None ->
    # (None, None) - the exit drivers treat a missing quote as no-quote (RegimeExitNoQuote), never
    # an unpack crash. (The sweep's LiveProviders.bbo keeps its own ProviderNotReady-on-miss path.)
    def bbo_pair(symbol: str) -> "tuple[object | None, object | None]":
        return bbo_cache.bbo(symbol) or (None, None)

    # ONE shared silent-pair registry for the universe (the ws_state machines == the receive-loop
    # machines) + ONE shared reconnect coordinator (the HR-WM-012 gate the driver + shards share).
    silent_pairs: dict[str, SilentPairMachine] = {
        pair: SilentPairMachine(clock=mono_clock) for pair in universe
    }
    coordinator = ShardReconnectCoordinator()

    # 1. REST WARM-UP (ar:AR-044): seed the per-pair LiveIndicators + the 1H HtfCache. OPS-1: the
    #    5m series also seeds the DEC-128 mpp cap store (no extra REST under the AR-036 stagger).
    warmups = await WarmupOrchestrator(
        rest_client, sleep=rest_sleep, on_event=event_sink, on_5m_bars=mpp_store.seed_from_bars
    ).warm_all(universe)
    # 2. REST DAILY REGIME (ar:AR-074 anchor): fill the RegimeCache (+ EC-L1A-002 for open positions).
    #    OPS-1: the daily series also seeds the DEC-124 expected_reward store (the run-to-reversal
    #    median per pair/regime) - again from the bars already fetched, no extra REST.
    regime_cache = await DailyRegimeCompute(
        rest_client, sleep=rest_sleep, on_event=event_sink, ws_manager=wm, bbo_provider=bbo_pair,
        on_daily_bars=reward_store.seed_from_bars,
    ).compute_all(universe)
    # 3. REST LIQUIDITY PROBE (Gate-2 vol_24h_usd): fill the LiquidityCache.
    await LiquidityProbe(
        rest_client, sleep=rest_sleep, on_event=event_sink, now=wall_clock
    ).refresh(universe, liquidity_cache)

    # 4. Build the per-module CIATS brain (one CiatsConductor + learning sink per side, sec 7) and
    #    assemble the LiveProviders over the caches + the CIATS seed stores + the ws_state lifecycle.
    #    The cycle_parameters provider is BACKED by the per-module Parameter Stores + the conductors'
    #    disallowed_regimes (CI-IF-003): a CIATS-tuned value + the protective block list now FLOW
    #    into the gates per cycle (no longer seed-only). The conductors' CIATS events sink to Stream-1.
    conductors, ciats_sinks, approval_inboxes = assemble_ciats_modules(logger, on_event=event_sink)
    providers = make_live_providers(
        instrument_cache=instrument_cache,
        bbo_cache=bbo_cache,
        liquidity_cache=liquidity_cache,
        expected_reward=make_expected_reward_provider(reward_store),
        mpp_abs_cap_pct=make_mpp_provider(mpp_store),
        ws_state=make_ws_state_provider(silent_pairs.get),
        now_utc=now_utc,
        cycle_parameters=make_cycle_parameters_provider(conductors),
    )

    # 5. The LiveSweepDriver over the warmed pairs (the HR-WM-012 guard reads the shared coordinator).
    driver = LiveSweepDriver(
        warmups=warmups,
        regime_cache=regime_cache,
        providers=providers,
        wm=wm,
        logger=logger,
        is_reconnecting=coordinator.any_reconnecting,
        now_monotonic=mono_clock,
        bbo_provider=bbo_pair,
    )

    # 6. Build + run the public data layer (handlers bound, the SHARED machines/coordinator injected).
    handler_provider = make_public_handler_provider(
        instrument_cache=instrument_cache,
        bbo_cache=bbo_cache,
        driver=driver,
        status_handler=status_handler,
    )
    assembler = DataLayerAssembler(
        ShardPlan(universe),
        mode=mode,
        open_socket=open_socket,
        bucket=bucket,
        coordinator=coordinator,
        handler_provider=handler_provider,
        silent_pairs=silent_pairs,
        on_event=event_sink,
        clock=mono_clock,
        sleep=pace_sleep,
    )
    data_layer = await assembler.build()

    # 7. LIVE only (PA-004 div #1 / HR-WM-022): the SEPARATE private executions/balances connection
    #    (AR-049 steps 5/6 - token -> private connect -> subscribe -> snap_orders mirror reconcile).
    #    Paper keeps it None (never connects the private WS). The live fill -> mirror loop is the
    #    already-built PrivateConnectionAssembler; this is the startup-sequencing tie-in.
    private_connection: PrivateConnection | None = None
    if mode is Mode.LIVE:
        if open_private_socket is None or acquire_token is None:
            raise ValueError(
                "live mode requires open_private_socket + acquire_token for the private WS connection"
            )
        private_connection = await PrivateConnectionAssembler(
            wm,
            open_socket=open_private_socket,
            acquire_token=acquire_token,
            fetch_snap_orders=fetch_snap_orders,
            balances_handler=balances_handler,
            on_event=event_sink,
            clock=mono_clock,
            sleep=pace_sleep,
        ).build()

    return OperationalSystem(
        data_layer=data_layer,
        driver=driver,
        providers=providers,
        regime_cache=regime_cache,
        warmups=warmups,
        silent_pairs=silent_pairs,
        instrument_cache=instrument_cache,
        bbo_cache=bbo_cache,
        liquidity_cache=liquidity_cache,
        conductors=conductors,
        ciats_sinks=ciats_sinks,
        approval_inboxes=approval_inboxes,
        private_connection=private_connection,
    )
