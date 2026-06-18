"""The 24h DECISION cache: EMA(12)/EMA(26)/ATR(14) on the derived 24h decision series (TB00788).

The validated long-only strategy (TB00786) decides on the 24h candle: entry = EMA12/26 bullish
cross, exit = EMA12/26 bearish-cross reversal OR a wide ATR(14) disaster stop, no take-profit. This
module holds the per-pair daily indicator state and advances it on each Closed24H the OhlcAggregator
second fold stage (TB00787, fold_hour) emits - the daily analog of the 1H HtfCache one timeframe up.

SEED (no extra REST): from the authoritative GetOHLCData(interval=1440) daily series the
mod:Regime_Engine ALREADY fetches per pair for its daily classification (RE-008/RE-010). The daily
decision indicators are a SECOND consumer of that same series, NOT a third per-pair warm-up call -
so ar:AR-049 step 8 ("interval 1440 is NOT part of the per-pair warm-up") STANDS. The seed computes
EMA(12)/EMA(26) via the standard EMA and ATR(14) via the Wilder SMMA, exactly as the regime daily
compute does for its own EMA20/50 + ATR, and lands at the same 00:00-UTC boundary the live fold
emits its Closed24H on.

ADVANCE: each live Closed24H steps the cache incrementally - the standard EMA step
ema' = (close - ema) * alpha + ema (alpha = 2/(period+1), matching live_driver._step_htf) and the
Wilder ATR step atr' = (atr * (period-1) + TR) / period, TR = max(high-low, |high-prev_close|,
|low-prev_close|). The cache carries the prior 24h close so the TR needs no extra state. A
day-aligned gap (Htf24hGap) re-seeds from the regime 1440 series (the exact mirror of the 1H heal).

PURE: no I/O, no clock, Decimal-only (ar:AR-047). Every knob is a CIATS-owned seed
(param:decision_ema_fast 12, param:decision_ema_slow 26, param:decision_atr_period 14,
param:decision_bar_interval_min 1440); only the net 1:1.5 R:R floor is hardcoded.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from ..regime.indicators import atr_14_series, ema
from .candle_close import CommittedCandle

# The decision cadence + indicator periods - all CIATS-owned seeds (the validated long-only config,
# TB00786). param:decision_bar_interval_min keys the 24h decision bar; the EMA/ATR periods are the
# indicators computed on that derived series.
INTERVAL_1440_MIN = 1440
DECISION_BAR_INTERVAL_MIN = 1440
DECISION_EMA_FAST = 12
DECISION_EMA_SLOW = 26
DECISION_ATR_PERIOD = 14


class DailyDecisionSeedError(ValueError):
    """Too few daily (1440) bars to seed the decision indicators - the slow EMA needs at least
    `ema_slow` closes. The caller leaves the pair without a daily decision (no entry) until the
    regime 1440 series is long enough, exactly as a WARM_UP pair is skipped (ar:AR-068)."""


@dataclass(frozen=True)
class DailyDecisionCache:
    """One pair's 24h DECISION indicator state (TB00788): the last 24h close + EMA(fast)/EMA(slow)
    on the daily closes + ATR(14) on the daily bars (the wide-stop basis). Frozen - seed() and
    advance() return a fresh cache (the live_driver swaps it on each Closed24H, mirroring HtfCache).

    The forthcoming long-only entry reads the EMA alignment (bullish cross); the exit reads it for
    the reversal AND reads atr_14_24h for the wide disaster stop (param:atr_stop_mult x ATR(14))."""

    close_24h: Decimal
    ema_fast_24h: Decimal
    ema_slow_24h: Decimal
    atr_14_24h: Decimal

    @property
    def bullish(self) -> bool:
        """EMA(fast) strictly above EMA(slow) - the long-only entry/hold alignment. The bullish
        CROSS is this True on a decision bar whose predecessor cache was False (the entry consumer
        compares the pre- and post-advance caches); a bearish cross (True -> False) is the reversal
        exit. Equality is NOT bullish (conservative: a touch is not yet a cross)."""
        return self.ema_fast_24h > self.ema_slow_24h

    @classmethod
    def seed(
        cls,
        bars: Sequence[CommittedCandle],
        *,
        ema_fast: int = DECISION_EMA_FAST,
        ema_slow: int = DECISION_EMA_SLOW,
        atr_period: int = DECISION_ATR_PERIOD,
    ) -> "DailyDecisionCache":
        """Seed from the authoritative 1440 daily series (newest-last, each bar carrying
        .high/.low/.close) the Regime Engine already fetched - NO extra REST. Raises
        DailyDecisionSeedError when too few bars to seed the slow EMA."""
        closes = [b.close for b in bars]
        highs = [b.high for b in bars]
        lows = [b.low for b in bars]
        if len(closes) < ema_slow:
            raise DailyDecisionSeedError(
                f"EMA{ema_slow} needs at least {ema_slow} daily bars, got {len(closes)}"
            )
        return cls(
            close_24h=closes[-1],
            ema_fast_24h=ema(closes, ema_fast),
            ema_slow_24h=ema(closes, ema_slow),
            atr_14_24h=atr_14_series(highs, lows, closes, atr_period)[-1],
        )

    def advance(
        self,
        candle: CommittedCandle,
        *,
        ema_fast: int = DECISION_EMA_FAST,
        ema_slow: int = DECISION_EMA_SLOW,
        atr_period: int = DECISION_ATR_PERIOD,
    ) -> "DailyDecisionCache":
        """Incrementally step on a CLOSED 24h decision candle (the OhlcAggregator Closed24H): the
        standard EMA step on both EMAs + the Wilder ATR step (TR from the candle vs the prior 24h
        close this cache carries). Returns a fresh cache; the periods MUST match the seed."""
        alpha_fast = Decimal(2) / (ema_fast + 1)
        alpha_slow = Decimal(2) / (ema_slow + 1)
        true_range = max(
            candle.high - candle.low,
            abs(candle.high - self.close_24h),
            abs(candle.low - self.close_24h),
        )
        return DailyDecisionCache(
            close_24h=candle.close,
            ema_fast_24h=(candle.close - self.ema_fast_24h) * alpha_fast + self.ema_fast_24h,
            ema_slow_24h=(candle.close - self.ema_slow_24h) * alpha_slow + self.ema_slow_24h,
            atr_14_24h=(self.atr_14_24h * (atr_period - 1) + true_range) / atr_period,
        )


class DailyDecisionStore:
    """The per-pair DailyDecisionCache home + the live-wire maintenance seam (TB00789).

    A SECOND consumer of the per-pair GetOHLCData(interval=1440) daily series mod:Regime_Engine ALREADY
    fetches (the TB00788 ruling - NOT a third per-pair warm-up call, so ar:AR-049 step 8 STANDS):
    ``seed_from_bars`` is wired alongside the DEC-124 reward store on the DailyRegimeCompute
    ``on_daily_bars`` callback, so every pair's decision cache is seeded at the daily 00:00-UTC regime
    compute from bars already fetched - zero added REST.

    ``advance`` steps a pair's cache on each live Closed24H the OhlcAggregator second fold stage
    (TB00787 fold_hour) emits; the Htf24hGap self-heal re-seeds via ``seed_from_bars`` over one REST
    GetOHLCData(1440) (the exact mirror of the 1H heal one timeframe up, landing on the value the live
    cache would hold per the TB00788 incrementality invariant). A pair with too few daily bars to seed
    the slow EMA is simply ABSENT (no decision until the series is long enough, exactly as a WARM_UP
    pair is skipped, ar:AR-068). The forthcoming long-only entry/exit consumer reads ``get`` for the
    pre-advance cache and ``advance``'s return for the post-advance cache (the bullish-cross compare)."""

    def __init__(self) -> None:
        self._by_symbol: dict[str, DailyDecisionCache] = {}

    def get(self, symbol: str) -> "DailyDecisionCache | None":
        """The pair's current decision cache, or None if never seeded (too few daily bars / not yet
        computed) - the consumer treats None as no-decision (no entry), bounded."""
        return self._by_symbol.get(symbol)

    def seed_from_bars(self, symbol: str, bars: Sequence[object]) -> None:
        """Seed (or re-seed) one pair's decision cache from the authoritative 1440 daily series
        (newest-last bars carrying .high/.low/.close). Too few bars to seed the slow EMA leaves the
        pair UNSEEDED (popped) - bounded: no decision until the daily series grows / the next regime
        re-seed. The seam for BOTH the daily regime-compute seed AND the Htf24hGap REST re-seed."""
        try:
            self._by_symbol[symbol] = DailyDecisionCache.seed(bars)
        except DailyDecisionSeedError:
            self._by_symbol.pop(symbol, None)

    def advance(self, symbol: str, candle: CommittedCandle) -> "DailyDecisionCache | None":
        """Step a pair's cache on a live Closed24H decision candle; returns the new cache, or None if
        the pair was never seeded (the advance is skipped - bounded, the next daily regime compute or
        the Htf24hGap heal re-seeds)."""
        cache = self._by_symbol.get(symbol)
        if cache is None:
            return None
        cache = cache.advance(candle)
        self._by_symbol[symbol] = cache
        return cache
