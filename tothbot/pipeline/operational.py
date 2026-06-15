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

 assemble_operational:  the ar:AR-049 startup sequence end state (the PUBLIC data layer; the private
   executions/balances connection is the SEPARATE live-only assembly, PA-004 div #1). The phases:
     1. REST WARM-UP (warmup.py)        - GetOHLCData(5)/(60) seed the LiveIndicators + HtfCache.
     2. REST DAILY REGIME (scheduler.py) - GetOHLCData(1440) per pair + the BTC/USD anchor -> the
        RegimeCache (also drives EC-L1A-002 for any open position via wm.on_regime_classified).
     3. REST LIQUIDITY PROBE (LiquidityProbe) - GetTicker 24h USD volume -> the LiquidityCache.
     4. make_live_providers over the caches + the CIATS seed stores (the DEC-124 expected_reward +
        DEC-128 mpp seeds, both CIATS-owned, seeded from historical OHLC at universe load) + the
        ws_state machine lifecycle.
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

from ..config.settings import Mode
from ..exchange.assembler import DataLayer, DataLayerAssembler
from ..exchange.bbo_cache import BboCache
from ..exchange.channels import PublicChannel
from ..exchange.dispatch import Channel, Handler
from ..exchange.instrument_cache import InstrumentCache
from ..exchange.liquidity_cache import LiquidityCache, LiquidityProbe
from ..exchange.pacing import SubscribeTokenBucket
from ..exchange.reconnect import ShardReconnectCoordinator
from ..exchange.sharding import ShardPlan
from ..exchange.silent_pair import SilentPairMachine
from ..exchange.transport import Transport
from ..exchange.warmup import WarmupOrchestrator
from ..regime.scheduler import DailyRegimeCompute, RegimeCache
from .live_driver import LiveSweepDriver
from .providers import (
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


@dataclass
class OperationalSystem:
    """The assembled, runnable public organism: call data_layer.run() to drive it. The component
    handles are exposed for inspection / a controlled shutdown (data_layer.stop())."""

    data_layer: DataLayer
    driver: LiveSweepDriver
    providers: LiveProviders
    regime_cache: RegimeCache
    warmups: dict
    silent_pairs: dict[str, SilentPairMachine]
    instrument_cache: InstrumentCache
    bbo_cache: BboCache
    liquidity_cache: LiquidityCache


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
) -> OperationalSystem:
    """Run the ar:AR-049 cold-start sequence and return the runnable public organism (see module
    docstring). `on_event` (defaulting to logger.record) sinks the warm-up / regime / liquidity /
    pacing telemetry; `logger` is mod:Logger (the sweep's per-module Stream-2 sink). The CIATS seed
    stores are injected pre-seeded (the universe-load historical probe owns the seeding)."""
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

    # 1. REST WARM-UP (ar:AR-044): seed the per-pair LiveIndicators + the 1H HtfCache.
    warmups = await WarmupOrchestrator(rest_client, sleep=rest_sleep, on_event=event_sink).warm_all(
        universe
    )
    # 2. REST DAILY REGIME (ar:AR-074 anchor): fill the RegimeCache (+ EC-L1A-002 for open positions).
    regime_cache = await DailyRegimeCompute(
        rest_client, sleep=rest_sleep, on_event=event_sink, ws_manager=wm, bbo_provider=bbo_pair
    ).compute_all(universe)
    # 3. REST LIQUIDITY PROBE (Gate-2 vol_24h_usd): fill the LiquidityCache.
    await LiquidityProbe(
        rest_client, sleep=rest_sleep, on_event=event_sink, now=wall_clock
    ).refresh(universe, liquidity_cache)

    # 4. Assemble the LiveProviders over the caches + the CIATS seed stores + the ws_state lifecycle.
    providers = make_live_providers(
        instrument_cache=instrument_cache,
        bbo_cache=bbo_cache,
        liquidity_cache=liquidity_cache,
        expected_reward=make_expected_reward_provider(reward_store),
        mpp_abs_cap_pct=make_mpp_provider(mpp_store),
        ws_state=make_ws_state_provider(silent_pairs.get),
        now_utc=now_utc,
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
    )
