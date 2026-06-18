"""Unit tests for the 24h DECISION cache (TB00788, daily_decision.py).

The cache holds EMA(12)/EMA(26)/ATR(14) on the derived 24h decision series (the validated long-only
strategy, TB00786) and advances on each Closed24H the OhlcAggregator second fold stage emits. Covers:
the seed from the 1440 daily series, the too-few-bars guard, the bullish-alignment property + the
cross transition, and the INCREMENTALITY INVARIANT - one live advance equals re-seeding over the
extended series exactly (Decimal-exact, so the live cache never drifts from a REST re-seed).
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.candle_close import CommittedCandle
from tothbot.exchange.daily_decision import (
    DECISION_EMA_FAST,
    DECISION_EMA_SLOW,
    DailyDecisionCache,
    DailyDecisionSeedError,
)
from tothbot.regime.indicators import atr_14_series, ema

DAY = 86400


def _c(begin: int, o, h, l, c, v=1) -> CommittedCandle:
    return CommittedCandle(
        symbol="BTC/USD", interval_begin=begin,
        open=Decimal(o), high=Decimal(h), low=Decimal(l), close=Decimal(c), volume=Decimal(v),
    )


def _daily_bars(n: int) -> list[CommittedCandle]:
    """n daily candles with a gently rising, wiggling close so EMA(12) sits above EMA(26)."""
    bars = []
    for i in range(n):
        # close oscillates around a rising trend; high/low straddle it by a few points.
        close = 100 + i + (3 if i % 2 else -2)
        bars.append(_c(i * DAY, close - 1, close + 4, close - 5, close))
    return bars


def test_seed_matches_the_batch_indicator_helpers():
    bars = _daily_bars(40)
    cache = DailyDecisionCache.seed(bars)
    closes = [b.close for b in bars]
    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    assert cache.close_24h == closes[-1]
    assert cache.ema_fast_24h == ema(closes, DECISION_EMA_FAST)
    assert cache.ema_slow_24h == ema(closes, DECISION_EMA_SLOW)
    assert cache.atr_14_24h == atr_14_series(highs, lows, closes)[-1]


def test_seed_raises_when_too_few_bars_for_the_slow_ema():
    bars = _daily_bars(DECISION_EMA_SLOW - 1)   # 25 bars, EMA(26) cannot seed
    try:
        DailyDecisionCache.seed(bars)
    except DailyDecisionSeedError:
        pass
    else:
        raise AssertionError("expected DailyDecisionSeedError on too few daily bars")


def test_one_advance_equals_reseeding_over_the_extended_series_exactly():
    """The incrementality invariant: a single live Closed24H advance is Decimal-IDENTICAL to a fresh
    seed over the same extended series - so the cache that advances live never drifts from a REST
    1440 re-seed (the Htf24hGap self-heal lands on the exact value the live cache would hold)."""
    bars = _daily_bars(40)
    stepped = DailyDecisionCache.seed(bars[:-1]).advance(bars[-1])
    reseeded = DailyDecisionCache.seed(bars)
    assert stepped.close_24h == reseeded.close_24h
    assert stepped.ema_fast_24h == reseeded.ema_fast_24h
    assert stepped.ema_slow_24h == reseeded.ema_slow_24h
    assert stepped.atr_14_24h == reseeded.atr_14_24h


def test_many_advances_track_a_full_reseed_exactly():
    bars = _daily_bars(50)
    cache = DailyDecisionCache.seed(bars[:30])
    for bar in bars[30:]:
        cache = cache.advance(bar)
    reseeded = DailyDecisionCache.seed(bars)
    assert cache.ema_fast_24h == reseeded.ema_fast_24h
    assert cache.ema_slow_24h == reseeded.ema_slow_24h
    assert cache.atr_14_24h == reseeded.atr_14_24h


def test_bullish_property_and_the_cross_transition():
    # fast above slow -> bullish (the long-only hold/entry alignment).
    bull = DailyDecisionCache(Decimal(100), Decimal(105), Decimal(102), Decimal(3))
    assert bull.bullish is True
    # fast below slow -> not bullish (a bearish cross from a prior bullish cache = the reversal exit).
    bear = DailyDecisionCache(Decimal(100), Decimal(101), Decimal(104), Decimal(3))
    assert bear.bullish is False
    # equality is NOT a cross (conservative - a touch is not yet bullish).
    touch = DailyDecisionCache(Decimal(100), Decimal(103), Decimal(103), Decimal(3))
    assert touch.bullish is False
    # the consumer detects the bullish CROSS as (not prev.bullish) and new.bullish.
    assert (not bear.bullish) and bull.bullish


def test_advance_atr_uses_the_prior_close_for_true_range():
    # A cache whose prior close is 100; a candle gapping up to high 130 makes TR = high - prev_close.
    cache = DailyDecisionCache(Decimal(100), Decimal(110), Decimal(108), Decimal(10))
    nxt = cache.advance(_c(DAY, 120, 130, 119, 125))
    # TR = max(130-119, |130-100|, |119-100|) = max(11, 30, 19) = 30; atr' = (10*13 + 30)/14.
    assert nxt.atr_14_24h == (Decimal(10) * 13 + Decimal(30)) / 14
    assert nxt.close_24h == Decimal(125)
