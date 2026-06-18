"""CIATS expected_reward seed-estimator tests (ciats/expected_reward.py; DEC-124 run-to-reversal).

Covers the direction-symmetric excursion math, the rolling-classification precompute, the
hold-until-L1a-reversal replay (records on reversal, DISCARDS an entry whose reversal never fires
in-window), the per-regime median seed + store, and the make_expected_reward_provider wrapper
(ProviderNotReady on a missing seed). The two production detectors (regime_exit.py) are composed
exactly as the live organism runs them, fed from the rolling daily regime.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.ciats.expected_reward import (
    EXPECTED_REWARD_QUANTILE,
    ExpectedRewardStore,
    _excursion,
    compute_expected_reward,
    replay_excursions,
    rolling_classifications,
    rolling_ema,
)
from tothbot.exchange.position_mirror import PositionSide
from tothbot.pipeline.providers import make_expected_reward_provider
from tothbot.pipeline.sweep import ProviderNotReady
from tothbot.regime.engine import DailyBar
from tothbot.regime.indicators import ema
from tothbot.regime.taxonomy import Regime


def test_rolling_ema_is_none_before_the_seed_and_matches_ema_after_tb00790():
    # TB00790: the replay's 24h EMA(decision_ema_fast)/EMA(decision_ema_slow) reversal reads a rolling
    # EMA at each bar. rolling_ema[j] = EMA(period) over closes[:j+1]: None before the SMA seed at
    # index period-1, then identical to the one-shot ema() (the same SMA-seeded recurrence).
    closes = [Decimal(x) for x in (10, 11, 12, 13, 14, 15)]
    r = rolling_ema(closes, 3)
    assert r[0] is None and r[1] is None              # too few closes to seed EMA(3)
    assert r[2] == ema(closes[:3], 3)                 # the SMA seed at index period-1
    assert r[-1] == ema(closes, 3)                    # the full-series EMA at the last bar


def _bar(prev_close, close):
    o, c = Decimal(str(prev_close)), Decimal(str(close))
    return DailyBar.of(o, max(o, c) + Decimal("0.5"), min(o, c) - Decimal("0.5"), c, 1)


def _series(closes):
    out, prev = [], closes[0]
    for c in closes:
        out.append(_bar(prev, c))
        prev = c
    return out


# --------------------------------------------------------------------------- excursion (sign)
def test_long_excursion_positive_when_exit_above_entry():
    # A LONG running to a higher reversal price realizes a positive favorable excursion.
    assert _excursion(PositionSide.LONG, Decimal("100"), Decimal("110")) == Decimal("0.1")


def test_long_excursion_negative_when_exit_below_entry():
    # Sign-preserving: a reversal that exits below entry is a negative excursion (no clamp).
    assert _excursion(PositionSide.LONG, Decimal("100"), Decimal("90")) == Decimal("-0.1")


def test_short_excursion_positive_when_exit_below_entry():
    # The SHORT mirror: a downward run to reversal is the favorable (positive) excursion.
    assert _excursion(PositionSide.SHORT, Decimal("100"), Decimal("90")) == Decimal("0.1")


def test_short_excursion_negative_when_exit_above_entry():
    assert _excursion(PositionSide.SHORT, Decimal("100"), Decimal("110")) == Decimal("-0.1")


# --------------------------------------------------------------------------- median recipe
def test_quantile_level_is_the_median():
    # The DEC-124 recipe is the central-tendency MEDIAN = the 0.5 quantile.
    assert EXPECTED_REWARD_QUANTILE == Decimal("0.5")


# --------------------------------------------------------------------------- rolling classify
def test_rolling_classifications_none_until_floor():
    # Below the max(28, EMA50)=50 committed-candle floor compute_regime cannot classify -> None.
    bars = _series([100 + i for i in range(40)])
    classes = rolling_classifications("X", bars)
    assert len(classes) == 40
    assert all(c is None for c in classes)


def test_rolling_classifications_trending_positive_uptrend():
    # A long steady uptrend classifies TRENDING_POSITIVE once past the floor.
    bars = _series([100 + i * 2 for i in range(70)])
    classes = rolling_classifications("X", bars)
    assert classes[-1] is not None
    assert classes[-1].regime in (Regime.TRENDING_POS_NORMAL, Regime.TRENDING_POS_ELEVATED)


# --------------------------------------------------------------------------- replay
def test_monotonic_uptrend_never_reverses_is_discarded():
    # No reversal fires across a monotonic uptrend -> every entry is discarded (no realized run-to-
    # reversal to measure); the harness returns no samples rather than an artificial end-of-data exit.
    bars = _series([100 + i for i in range(80)])
    assert replay_excursions("X", bars) == {}
    assert compute_expected_reward("X", bars) == {}


def test_uptrend_then_downtrend_records_long_excursions():
    # An uptrend (TRENDING_POS LONG entries) that reverses into a downtrend fires the L1a exit and
    # records excursions under the ENTRY regime; the seed is the per-regime median (a Decimal).
    bars = _series([100 + i * 2 for i in range(55)] + [210 - i * 3 for i in range(1, 31)])
    excursions = replay_excursions("X", bars)
    seed = compute_expected_reward("X", bars)
    assert any(r in (Regime.TRENDING_POS_NORMAL, Regime.TRENDING_POS_ELEVATED) for r in excursions)
    assert set(seed) == set(excursions)
    for regime, value in seed.items():
        assert isinstance(value, Decimal)


def test_downtrend_then_uptrend_records_short_excursions():
    # The SHORT mirror: a downtrend (TRENDING_NEG SHORT entries) reversing up fires the L1a exit and
    # records SHORT excursions under the TRENDING_NEG entry regime.
    bars = _series([300 - i * 3 for i in range(55)] + [138 + i * 4 for i in range(1, 31)])
    excursions = replay_excursions("X", bars)
    assert any(
        r in (Regime.TRENDING_NEG_NORMAL, Regime.TRENDING_NEG_ELEVATED) for r in excursions
    )


# --------------------------------------------------------------------------- store + provider
def test_store_put_get():
    store = ExpectedRewardStore()
    store.put("BTC/USD", Regime.TRENDING_POS_NORMAL, "0.05")
    assert store.get("BTC/USD", Regime.TRENDING_POS_NORMAL) == Decimal("0.05")
    assert store.get("BTC/USD", Regime.NON_DIR_NORMAL) is None


def test_store_seed_from_bars_populates_observed_regimes():
    store = ExpectedRewardStore()
    bars = _series([100 + i * 2 for i in range(55)] + [210 - i * 3 for i in range(1, 31)])
    store.seed_from_bars("ETH/USD", bars)
    seed = compute_expected_reward("ETH/USD", bars)
    assert seed  # the reversing series observed at least one regime
    for regime, value in seed.items():
        assert store.get("ETH/USD", regime) == value


def test_provider_reads_store():
    store = ExpectedRewardStore()
    store.put("BTC/USD", Regime.TRENDING_POS_NORMAL, "0.07")
    provider = make_expected_reward_provider(store)
    assert provider("BTC/USD", Regime.TRENDING_POS_NORMAL) == Decimal("0.07")


def test_provider_missing_raises_not_ready():
    provider = make_expected_reward_provider(ExpectedRewardStore())
    with pytest.raises(ProviderNotReady):
        provider("BTC/USD", Regime.NON_DIR_NORMAL)
