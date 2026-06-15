"""Per-pair startup WARM-UP orchestrator - the REST edge that seeds the live caches (ar:AR-044).

Source: 0500000 dv1_250 ar:AR-044 (startup warm-up: ATR(14) + EMA via REST GetOHLCData before any
pipeline execution) + ar:AR-068 (the same GetOHLCData(interval=5) response seeds ALL FIVE per-pair
SSS indicators; WARM_UP -> READY requires all five seeded) + ar:AR-045 (the candle-close-detection
init: last_interval_begin / last_complete_candle per symbol, seeded from the last committed candle)
+ ar:AR-049 step 8 (REST warm-up GetOHLCData interval 5/60 per pair; interval 1440 is NOT part of
the per-pair warm-up - the daily regime is mod:Regime_Engine's own one-shot, scheduler.py).

PER PAIR (ar:AR-044):
  (a) GetOHLCData(interval=5)  -> seed LiveIndicators (the 5m ATR(14) trade-ATR + the five SSS
      running values) + init the AR-045 5m candle-close trackers.
  (b) GetOHLCData(interval=60) -> seed the 1H EMA(20)/EMA(50) HTF cache (G4 close_1h/ema20_1h +
      the L1a EC-L1A-001 1H reversal) + init the AR-045 1H candle-close trackers.
STAGGER 1.1s between the SAME pair's two calls (ar:AR-036 rate-limit wire fact, NOT a CIATS seed).
PARALLELISM: asyncio.gather() across DIFFERENT pairs (ar:AR-044 explicitly allows cross-pair
parallelism for warm-up, unlike the sequential daily compute). Per-pair failure isolation: a REST
or seed error -> evt:WARM_UP_FAIL, the pair is skipped (stays WARM_UP), the sweep continues.

WARM_UP -> READY (ar:AR-068 / AR-044): a pair is READY once its 5m indicators + 1H EMA are seeded
AND its daily regime is present in the RegimeCache (the three independent seed sources). The system
begins trading READY pairs immediately and does not wait for the whole universe; WARM_UP pairs are
skipped at the per-5m sweep pre-check.

All I/O is injected (the REST client, the inter-call sleep) so the orchestrator is driven under
stdlib asyncio.run over fakes - no network, no real timers. Decimal-only (ar:AR-047).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal

from ..regime.indicators import ema
from ..regime.live_indicators import IndicatorSeedError, LiveIndicators
from ..rest.client import KrakenRestError, RestOhlcBar

# ar:AR-044 warm-up candle intervals (minutes): 5m drives indicators + the clock; 60m the HTF cache.
INTERVAL_5_MIN = 5
INTERVAL_60_MIN = 60
# ar:AR-044(b): the 1H HTF EMAs are the 20/50 periods (the same period pair as the daily regime EMAs).
HTF_EMA_SHORT = 20
HTF_EMA_LONG = 50
# ar:AR-036 rate-limit wire fact: 1.1s between the same pair's two warm-up calls. NOT a CIATS seed.
WARMUP_STAGGER_SEC = 1.1

EventSink = Callable[[object], None]
Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class HtfCache:
    """The 1H HTF pre-computation cache for one pair (ar:AR-044(b)): the last committed 1H close +
    the 1H EMA(20)/EMA(50). G4 reads close_1h/ema20_1h; the L1a EC-L1A-001 reversal reads both EMAs."""

    close_1h: Decimal
    ema20_1h: Decimal
    ema50_1h: Decimal


@dataclass(frozen=True)
class WarmUpFailed:
    """evt:WARM_UP_FAIL [WARNING] {symbol, reason} - one pair's warm-up could not complete (REST
    error or too few committed candles to seed). The pair stays WARM_UP and is skipped; the sweep
    across the other pairs is unaffected (per-pair isolation, ar:AR-044)."""

    symbol: str
    reason: str
    code: str = field(default="WARM_UP_FAIL", init=False)


@dataclass(frozen=True)
class PairWarmedUp:
    """evt:WARM_UP_READY [INFO] {symbol} - one pair completed its 5m + 1H seed (indicators + HTF).
    Regime presence is checked separately at the READY transition (PairWarmup.is_ready)."""

    symbol: str
    code: str = field(default="WARM_UP_READY", init=False)


@dataclass
class PairWarmup:
    """One pair's seeded live state after warm-up: the 5m LiveIndicators, the 1H HtfCache, and the
    ar:AR-045 candle-close-detection init (the last committed 5m + 1H candle and its interval_begin).
    The per-5m sweep advances these on each WS ohlc message; here they hold the startup values."""

    symbol: str
    indicators: LiveIndicators
    htf: HtfCache
    # ar:AR-045 5m candle-close trackers (init from the last committed 5m candle = response[-2] raw).
    last_interval_begin: int
    last_complete_candle: RestOhlcBar
    # ar:AR-045 1H candle-close trackers (separate last_interval_begin_60).
    last_interval_begin_60: int
    last_complete_candle_60: RestOhlcBar

    def is_ready(self, regime_cache) -> bool:
        """ar:AR-068 / AR-044 WARM_UP -> READY: the 5m indicators + 1H EMA are seeded AND the daily
        regime is present. (indicators.seeded + htf are guaranteed once warm-up succeeded, so the
        live gate is regime presence - the three seed sources are independent.)"""
        return (
            self.indicators.seeded
            and self.htf is not None
            and regime_cache.get(self.symbol) is not None
        )


class WarmupOrchestrator:
    """The per-pair startup warm-up edge (ar:AR-044). warm_all(pairs) seeds every pair concurrently
    (cross-pair asyncio.gather) with a 1.1s stagger between each pair's own two REST calls, isolating
    per-pair failures. Returns {symbol: PairWarmup} for the pairs that seeded; failures are emitted
    as WARM_UP_FAIL and omitted (they stay WARM_UP)."""

    def __init__(
        self,
        rest_client,
        *,
        stagger_sec: float = WARMUP_STAGGER_SEC,
        sleep: Sleep = asyncio.sleep,
        on_event: EventSink | None = None,
        htf_ema_short: int = HTF_EMA_SHORT,
        htf_ema_long: int = HTF_EMA_LONG,
        on_5m_bars: Callable[[str, Sequence[RestOhlcBar]], None] | None = None,
    ) -> None:
        self._rest = rest_client
        self._stagger_sec = stagger_sec
        self._sleep = sleep
        self._on_event = on_event
        self._htf_ema_short = int(htf_ema_short)
        self._htf_ema_long = int(htf_ema_long)
        # OPS-1: an optional sink for the just-fetched 5m committed series (the DEC-128 mpp
        # estimator seeds its per-pair/side Q95 cap from it - no extra REST under the AR-036 stagger).
        self._on_5m_bars = on_5m_bars

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    async def warm_pair(self, symbol: str) -> PairWarmup:
        """Seed one pair: GetOHLCData(5) -> LiveIndicators + 5m trackers; stagger 1.1s; GetOHLCData
        (60) -> HtfCache + 1H trackers (ar:AR-044). Raises on REST / seed failure (caught upstream
        by the per-pair guard)."""
        resp5 = await self._rest.get_ohlc_data(symbol, INTERVAL_5_MIN)
        indicators = LiveIndicators(symbol)
        indicators.seed_from_bars(resp5.committed)  # ar:AR-068 seeds all five + the 5m ATR(14)
        if self._on_5m_bars is not None:
            self._on_5m_bars(symbol, resp5.committed)  # OPS-1: seed the DEC-128 mpp cap from this series

        await self._sleep(self._stagger_sec)  # ar:AR-036 same-pair inter-call stagger

        resp60 = await self._rest.get_ohlc_data(symbol, INTERVAL_60_MIN)
        closes60 = [b.close for b in resp60.committed]
        htf = HtfCache(
            close_1h=resp60.committed[-1].close,
            ema20_1h=ema(closes60, self._htf_ema_short),
            ema50_1h=ema(closes60, self._htf_ema_long),
        )

        last5 = resp5.committed[-1]    # raw response[-2] (parse_ohlc dropped the forming candle)
        last60 = resp60.committed[-1]
        return PairWarmup(
            symbol=symbol,
            indicators=indicators,
            htf=htf,
            last_interval_begin=last5.time,
            last_complete_candle=last5,
            last_interval_begin_60=last60.time,
            last_complete_candle_60=last60,
        )

    async def _warm_guarded(self, symbol: str) -> PairWarmup | None:
        """Warm one pair, converting any REST/seed failure into a WARM_UP_FAIL event + None (the
        per-pair isolation, ar:AR-044) so one bad pair never aborts the gather across the others."""
        try:
            warmup = await self.warm_pair(symbol)
        except (KrakenRestError, IndicatorSeedError, ValueError, IndexError) as exc:
            self._emit(WarmUpFailed(symbol, str(exc)))
            return None
        self._emit(PairWarmedUp(symbol))
        return warmup

    async def warm_all(self, pairs: Sequence[str]) -> dict[str, PairWarmup]:
        """Warm every pair CONCURRENTLY (ar:AR-044 cross-pair parallelism). Each pair runs its own
        two staggered calls; a per-pair failure is isolated (WARM_UP_FAIL, omitted from the result).
        Returns {symbol: PairWarmup} for the pairs that seeded - the WARM_UP/READY input set."""
        results = await asyncio.gather(*(self._warm_guarded(symbol) for symbol in pairs))
        return {symbol: warmup for symbol, warmup in zip(pairs, results) if warmup is not None}


def ready_pairs(
    warmups: dict[str, PairWarmup], regime_cache
) -> dict[str, PairWarmup]:
    """The subset of warmed pairs that are READY (ar:AR-068): 5m + 1H seeded AND regime present.
    The per-5m sweep iterates these; WARM_UP pairs (seeded but no regime yet) are skipped."""
    return {s: w for s, w in warmups.items() if w.is_ready(regime_cache)}
