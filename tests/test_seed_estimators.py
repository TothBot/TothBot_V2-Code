"""CIATS seed-estimator tests (ciats/seed_estimators.py; DEC-128 mpp_abs_cap_pct Q95).

Covers the adverse close-to-next-open gap fractions (long gap-up vs short gap-down, favorable=0),
the linear-interpolation quantile, the per-pair/side cap computation + store, and the
make_mpp_provider wrapper (ProviderNotReady on a missing seed).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.ciats.seed_estimators import (
    MppCapStore,
    adverse_gap_fractions,
    compute_mpp_cap,
    quantile,
)
from tothbot.exchange.position_mirror import PositionSide
from tothbot.pipeline.providers import make_mpp_provider
from tothbot.pipeline.sweep import ProviderNotReady
from tothbot.rest.client import RestOhlcBar


def _bar(open_, close):
    o, c = Decimal(str(open_)), Decimal(str(close))
    return RestOhlcBar(time=0, open=o, high=max(o, c), low=min(o, c), close=c, volume=Decimal("1"))


# --------------------------------------------------------------------------- adverse gaps
def test_long_adverse_is_gap_up():
    # close 100 -> next open 102 = +2% gap up = adverse for a LONG (pays more); favorable for short.
    bars = [_bar(100, 100), _bar(102, 102)]
    assert adverse_gap_fractions(bars, PositionSide.LONG) == [Decimal("0.02")]
    assert adverse_gap_fractions(bars, PositionSide.SHORT) == [Decimal("0")]


def test_short_adverse_is_gap_down():
    # close 100 -> next open 97 = -3% gap down = adverse for a SHORT (receives less).
    bars = [_bar(100, 100), _bar(97, 97)]
    assert adverse_gap_fractions(bars, PositionSide.SHORT) == [Decimal("0.03")]
    assert adverse_gap_fractions(bars, PositionSide.LONG) == [Decimal("0")]


# --------------------------------------------------------------------------- quantile
def test_quantile_interpolates():
    # 0.95 of [0..10] (11 values, ranks 0..10) -> rank 9.5 -> 9 + 0.5*(10-9) = 9.5
    assert quantile([Decimal(i) for i in range(11)], Decimal("0.95")) == Decimal("9.5")


def test_quantile_single_value():
    assert quantile([Decimal("3")]) == Decimal("3")


def test_quantile_empty_raises():
    with pytest.raises(ValueError):
        quantile([])


# --------------------------------------------------------------------------- compute + store
def test_compute_mpp_cap_q95_over_series():
    # Build closes flat at 100; opens gap up by 0..20 bps across 21 bars -> 20 gaps 0..0.0019.
    bars = [_bar(100, 100)]
    fractions = []
    for i in range(1, 21):
        gap_bps = Decimal(i) / Decimal("10000")        # i bps
        open_i = Decimal("100") * (Decimal(1) + gap_bps)
        bars.append(_bar(open_i, Decimal("100")))
        fractions.append(gap_bps)
    cap = compute_mpp_cap(bars, PositionSide.LONG)
    assert cap == quantile(fractions, Decimal("0.95"))


def test_compute_mpp_cap_needs_two_bars():
    with pytest.raises(ValueError):
        compute_mpp_cap([_bar(100, 100)], PositionSide.LONG)


def test_store_seeds_both_sides():
    store = MppCapStore()
    bars = [_bar(100, 100), _bar(101, 100), _bar(99, 100)]
    store.seed_from_bars("BTC/USD", bars)
    assert store.get("BTC/USD", PositionSide.LONG) is not None
    assert store.get("BTC/USD", PositionSide.SHORT) is not None


# --------------------------------------------------------------------------- provider
def test_mpp_provider_reads_store():
    store = MppCapStore()
    store.put("BTC/USD", PositionSide.LONG, "0.012")
    provider = make_mpp_provider(store)
    assert provider("BTC/USD", PositionSide.LONG) == Decimal("0.012")


def test_mpp_provider_missing_raises_not_ready():
    provider = make_mpp_provider(MppCapStore())
    with pytest.raises(ProviderNotReady):
        provider("BTC/USD", PositionSide.SHORT)
