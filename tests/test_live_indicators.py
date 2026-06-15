"""LiveIndicators incremental-maintainer tests (regime/live_indicators.py; ar:AR-016/AR-075/AR-068).

THE DIVERGENCE GUARD: the design decision (TB00736 B2a) is option (i) - maintain exact incremental
running values per ar:AR-016/AR-075 - chosen because the Wilder/EMA/running-SMA recurrences are
mathematically identical to the batch units over the same committed history. These tests assert
that equality DIRECTLY and bit-exactly: seed_from_bars(series[:k]) + update over series[k:] yields
the same RSI/EMA9/EMA21/VolumeMA20/ATR(14) and the same SSS verdict as the batch
evaluate_sss / atr_14_series / rsi_14 / ema / volume_ma_20 over the full series. If a future change
makes the live path drift from the batch path, these fail. Decimal-only.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.regime.indicators import atr_14_series, ema
from tothbot.regime.live_indicators import IndicatorSeedError, LiveIndicators, OhlcCandle
from tothbot.regime.sss import (
    SignalSide,
    evaluate_sss,
    rsi_14,
    volume_ma_20,
)
from tothbot.rest.client import RestOhlcBar


# --------------------------------------------------------------------------- helpers
def _bars(n: int) -> list[RestOhlcBar]:
    """A deterministic non-monotonic OHLCV series (gains AND losses, EMA cross + volume vary),
    so RSI/EMA/ATR/volume all exercise their full recurrence (not a degenerate constant)."""
    bars: list[RestOhlcBar] = []
    price = Decimal("100")
    for i in range(n):
        delta = (Decimal((i * 7) % 11) - Decimal(5)) * Decimal("0.3")  # -1.5 .. +1.5
        close = price + delta
        high = close + Decimal("1.2")
        low = close - Decimal("1.1")
        volume = Decimal(1000 + (i * 37) % 500)
        bars.append(
            RestOhlcBar(time=1700000000 + i * 300, open=price, high=high, low=low,
                        close=close, volume=volume)
        )
        price = close
    return bars


def _seed_then_step(symbol: str, bars: list[RestOhlcBar], split: int) -> LiveIndicators:
    li = LiveIndicators(symbol)
    li.seed_from_bars(bars[:split])
    for b in bars[split:]:
        li.update(OhlcCandle(high=b.high, low=b.low, close=b.close, volume=b.volume))
    return li


# --------------------------------------------------------------------------- equivalence (the guard)
@pytest.mark.parametrize("split", [25, 30, 45, 60])  # incl. split==n (seed only, no updates)
def test_live_matches_batch_indicators(split):
    bars = _bars(60)
    closes = [b.close for b in bars]
    highs = [b.high for b in bars]
    lows = [b.low for b in bars]
    volumes = [b.volume for b in bars]
    li = _seed_then_step("BTC/USD", bars, split)

    assert li.atr_14 == atr_14_series(highs, lows, closes)[-1]
    assert li.rsi_14 == rsi_14(closes)
    assert li.ema9 == ema(closes, 9)
    assert li.ema21 == ema(closes, 21)
    assert li.volume_ma20 == volume_ma_20(volumes)
    assert li.current_volume == volumes[-1]


@pytest.mark.parametrize("side", [SignalSide.LONG, SignalSide.SHORT])
@pytest.mark.parametrize("split", [25, 40, 60])
def test_live_sss_verdict_matches_batch(side, split):
    bars = _bars(60)
    closes = [b.close for b in bars]
    volumes = [b.volume for b in bars]
    li = _seed_then_step("ETH/USD", bars, split)

    live = li.sss_verdict(side)
    batch = evaluate_sss("ETH/USD", closes, volumes, side=side)

    assert live.rsi_14 == batch.rsi_14
    assert live.ema9 == batch.ema9
    assert live.ema21 == batch.ema21
    assert live.volume == batch.volume
    assert live.volume_ma20 == batch.volume_ma20
    assert live.ema_cross == batch.ema_cross
    assert live.volume_vs_ma20 == batch.volume_vs_ma20
    assert live.sc_sss == batch.sc_sss
    assert live.passed == batch.passed


def test_step_by_step_stays_equal_to_growing_batch():
    """After EACH update the running values equal the batch over the series so far - the O(1) step
    never accumulates drift relative to the O(n) recompute."""
    bars = _bars(50)
    li = LiveIndicators("SOL/USD")
    li.seed_from_bars(bars[:25])
    for k in range(25, 50):
        b = bars[k]
        li.update(OhlcCandle(high=b.high, low=b.low, close=b.close, volume=b.volume))
        closes = [x.close for x in bars[: k + 1]]
        volumes = [x.volume for x in bars[: k + 1]]
        highs = [x.high for x in bars[: k + 1]]
        lows = [x.low for x in bars[: k + 1]]
        assert li.rsi_14 == rsi_14(closes)
        assert li.ema9 == ema(closes, 9)
        assert li.ema21 == ema(closes, 21)
        assert li.volume_ma20 == volume_ma_20(volumes)
        assert li.atr_14 == atr_14_series(highs, lows, closes)[-1]


# --------------------------------------------------------------------------- warm-up / READY state
def test_not_seeded_until_seed():
    li = LiveIndicators("BTC/USD")
    assert li.seeded is False
    assert li.atr_14 is None and li.rsi_14 is None
    li.seed_from_bars(_bars(30))
    assert li.seeded is True
    assert li.atr_14 is not None and li.rsi_14 is not None


def test_min_seed_closes_is_ema21_floor():
    li = LiveIndicators("BTC/USD")
    assert li.min_seed_closes == 21  # EMA21 binds (> rsi_period+1=15, atr_period+1=15)


def test_insufficient_seed_raises():
    li = LiveIndicators("BTC/USD")
    with pytest.raises(IndicatorSeedError):
        li.seed_from_bars(_bars(20))  # < 21 closes


def test_update_before_seed_raises():
    li = LiveIndicators("BTC/USD")
    b = _bars(1)[0]
    with pytest.raises(IndicatorSeedError):
        li.update(OhlcCandle(high=b.high, low=b.low, close=b.close, volume=b.volume))


def test_verdict_before_seed_raises():
    li = LiveIndicators("BTC/USD")
    with pytest.raises(IndicatorSeedError):
        li.sss_verdict(SignalSide.LONG)


def test_short_zone_uses_registry_mirror_by_default():
    # A flat-ish seed then a clean down-leg: just assert the verdict is coherent + side-correct.
    li = _seed_then_step("ADA/USD", _bars(40), 25)
    v = li.sss_verdict(SignalSide.SHORT)
    assert v.side is SignalSide.SHORT
    assert v.event_type in {"SIGNAL_PASS", "SIGNAL_REJECTED"}
    # SC-SSS-2 short state is EMA9 < EMA21:
    assert v.ema_cross == (li.ema9 < li.ema21)
