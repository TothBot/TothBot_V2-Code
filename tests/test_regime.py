"""mod:Regime_Engine pure-unit tests (0500000 dv1_242 sec 5 + Image4 R9).

Covers the RE-008/009/010/012 indicators (indicators.py), the six-regime 3x2 grid policy
(taxonomy.py), and the compute_regime classifier core incl. the ar:AR-017 response[-1]
exclusion + the 28-candle minimum (engine.py). Decimal-only throughout (HR-REGIME-006).

Analytic anchors: a strict linear arithmetic trend (high/low band constant, close +d/day) has
+DM = d, -DM = 0 (or the mirror), DX = 100 every day, so ADX = 100 exactly - a clean check on
the Wilder ADX. A constant series has EMA == the constant.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.regime import indicators, taxonomy
from tothbot.regime.engine import (
    DailyBar,
    RegimeComputeError,
    classify_from_indicators,
    compute_regime,
)
from tothbot.regime.taxonomy import DirectionalState, Regime, VolatilityState


# --------------------------------------------------------------------------- helpers
def _linear_bars(n: int, start=100, step=1, span=2):
    """n daily bars: close = start + step*i, high = close+span, low = close-span."""
    bars = []
    for i in range(n):
        c = Decimal(start) + Decimal(step) * i
        bars.append(DailyBar.of(c, c + span, c - span, c))
    return bars


def _closes(bars):
    return [b.close for b in bars]


# --------------------------------------------------------------------------- indicators
def test_true_ranges_length_and_value():
    bars = _linear_bars(5, span=2, step=1)
    tr = indicators.true_ranges(
        [b.high for b in bars], [b.low for b in bars], _closes(bars)
    )
    assert len(tr) == 4  # n-1
    # high-low = 4; high-prev_close = 3; low-prev_close = 1 -> TR = 4 every day.
    assert all(t == Decimal(4) for t in tr)


def test_adx_linear_uptrend_is_100():
    # Strict arithmetic uptrend: +DM = step every day, -DM = 0 -> DX = 100 -> ADX = 100.
    bars = _linear_bars(40, step=3, span=2)
    adx = indicators.adx_14([b.high for b in bars], [b.low for b in bars], _closes(bars))
    assert adx == Decimal(100)


def test_adx_linear_downtrend_is_100():
    bars = _linear_bars(40, start=500, step=-3, span=2)
    adx = indicators.adx_14([b.high for b in bars], [b.low for b in bars], _closes(bars))
    assert adx == Decimal(100)


def test_adx_requires_28_candles():
    bars = _linear_bars(27)
    with pytest.raises(ValueError):
        indicators.adx_14([b.high for b in bars], [b.low for b in bars], _closes(bars))


def test_ema_constant_series_equals_constant():
    assert indicators.ema([Decimal(7)] * 30, 20) == Decimal(7)


def test_ema20_above_ema50_in_uptrend():
    closes = _closes(_linear_bars(80, step=1))
    assert indicators.ema(closes, 20) > indicators.ema(closes, 50)


def test_ema_alpha_two_step():
    # period=1 -> alpha = 2/2 = 1 -> EMA tracks the latest value exactly.
    assert indicators.ema([Decimal(1), Decimal(5), Decimal(9)], 1) == Decimal(9)


def test_atr_percentile_rank_basic():
    # current 5 is strictly greater than 4 of the 5 buffer values -> 80th percentile.
    assert indicators.atr_percentile_rank([Decimal(x) for x in (1, 2, 3, 4, 5)]) == Decimal(80)


def test_atr_percentile_rank_window_tail():
    series = [Decimal(x) for x in (1, 2, 3, 4, 5, 6)]
    # window 3 -> buffer (4,5,6); current 6 strictly greater than 2 -> 2/3*100.
    assert indicators.atr_percentile_rank(series, window=3) == Decimal(2) / Decimal(3) * Decimal(100)


def test_atr_percentile_rank_all_equal_is_zero():
    assert indicators.atr_percentile_rank([Decimal(5)] * 10) == Decimal(0)


# --------------------------------------------------------------------------- taxonomy / cells
@pytest.mark.parametrize(
    "adx, ema20, ema50, pct, expected",
    [
        (30, 110, 100, 50, Regime.TRENDING_POS_NORMAL),
        (30, 110, 100, 80, Regime.TRENDING_POS_ELEVATED),
        (10, 110, 100, 50, Regime.NON_DIR_NORMAL),
        (10, 110, 100, 80, Regime.NON_DIR_ELEVATED),
        (30, 90, 100, 50, Regime.TRENDING_NEG_NORMAL),
        (30, 90, 100, 80, Regime.TRENDING_NEG_ELEVATED),
    ],
)
def test_classify_six_cells(adx, ema20, ema50, pct, expected):
    res = classify_from_indicators("BTC/USD", adx, ema20, ema50, Decimal(5), pct)
    assert res.regime is expected


def test_adx_threshold_is_inclusive_non_dir():
    # ADX == 25 (the threshold) is NON_DIRECTIONAL (ADX <= threshold).
    res = classify_from_indicators("X", 25, 110, 100, 5, 10)
    assert res.directional is DirectionalState.NON_DIRECTIONAL
    assert res.regime is Regime.NON_DIR_NORMAL


def test_atr_percentile_threshold_is_exclusive_normal():
    # percentile == 67 (the threshold) is NORMAL_VOL (rank > thresh required for ELEVATED).
    res = classify_from_indicators("X", 30, 110, 100, 5, 67)
    assert res.volatility is VolatilityState.NORMAL_VOL


def test_ema_tie_under_trend_resolves_negative():
    # Measure-zero EMA tie with ADX > threshold -> TRENDING_NEGATIVE (loss-min cascade).
    res = classify_from_indicators("X", 30, 100, 100, 5, 10)
    assert res.directional is DirectionalState.TRENDING_NEGATIVE
    assert res.regime is Regime.TRENDING_NEG_NORMAL


def test_profiles_match_d4_policy():
    p = taxonomy.profile
    assert p(Regime.TRENDING_POS_NORMAL).size_multiplier == Decimal("1.0")
    assert p(Regime.TRENDING_POS_NORMAL).long_entry_permitted is True
    assert p(Regime.NON_DIR_NORMAL).size_multiplier == Decimal("0.5")
    assert p(Regime.NON_DIR_NORMAL).long_entry_permitted is True
    # DEC-B SYMMETRIC (Image4 R10): NON_DIR_NORMAL admits SHORT too, both at half size.
    assert p(Regime.NON_DIR_NORMAL).short_entry_permitted is True
    assert p(Regime.NON_DIR_NORMAL).entry_permitted(is_long=False) is True
    assert p(Regime.NON_DIR_NORMAL).cascade is False
    # Regime 4: no entry either side (HR-REGIME-008).
    assert p(Regime.NON_DIR_ELEVATED).long_entry_permitted is False
    assert p(Regime.NON_DIR_ELEVATED).short_entry_permitted is False
    # Regimes 5/6: cascade - LONG blocked, SHORT permitted (HR-REGIME-007).
    for r in (Regime.TRENDING_NEG_NORMAL, Regime.TRENDING_NEG_ELEVATED):
        assert p(r).cascade is True
        assert p(r).long_entry_permitted is False
        assert p(r).short_entry_permitted is True
        assert p(r).entry_permitted(is_long=False) is True
        assert p(r).entry_permitted(is_long=True) is False


# --------------------------------------------------------------------------- engine end-to-end
def test_compute_regime_uptrend_is_trending_pos_normal():
    res = compute_regime("BTC/USD", _linear_bars(60, step=3, span=2))
    assert res.directional is DirectionalState.TRENDING_POSITIVE
    assert res.adx == Decimal(100)
    # constant ATR band -> percentile rank 0 -> NORMAL_VOL.
    assert res.volatility is VolatilityState.NORMAL_VOL
    assert res.regime is Regime.TRENDING_POS_NORMAL


def test_compute_regime_excludes_forming_candle_ar017():
    bars = _linear_bars(60, step=3, span=2)
    # A wild forming candle appended as response[-1] must NOT affect the classification.
    forming = DailyBar.of(99999, 99999, 0, 1)
    with_forming = compute_regime("X", bars + [forming], exclude_forming=True)
    committed = compute_regime("X", bars, exclude_forming=False)
    assert with_forming.regime is committed.regime
    assert with_forming.adx == committed.adx


def test_compute_regime_min_candles_after_exclusion():
    # The binding floor is EMA50 (htf_ema_long = 50 committed). 50 bars, drop the forming
    # one -> 49 committed < 50 -> RegimeComputeError.
    with pytest.raises(RegimeComputeError):
        compute_regime("X", _linear_bars(50), exclude_forming=True)
    # 51 bars -> 50 committed -> ok.
    assert compute_regime("X", _linear_bars(51), exclude_forming=True) is not None


def test_classified_event_carries_token_and_inputs():
    res = compute_regime("ETH/USD", _linear_bars(60, step=3))
    evt = res.classified_event
    assert evt.code == "REGIME_CLASSIFIED"
    assert evt.symbol == "ETH/USD"
    assert evt.regime == "TRENDING_POS_NORMAL"
    assert evt.adx == Decimal(100)
