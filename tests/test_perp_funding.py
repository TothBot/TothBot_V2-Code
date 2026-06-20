"""Tests: mod:Perp_Funding_Divergence_Monitor + the 8h funding model (tothbot/perp/funding.py).

Covers the funding sign mirror (0500000 sec 13.5), the exchange clamp (TB00799 item 3), and the
signals-only EwmaMonitor instance (0500000 sec 13.9; TB00806 battery B / W2): it FIRES on a
sustained pinned-adverse regime and stays SILENT on real (positive-funding) data, and it cannot
deadlock (it never mutates / pauses anything).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.ciats.ewma_monitor import EwmaMonitor
from tothbot.config import registry
from tothbot.exchange.position_mirror import PositionSide
from tothbot.perp.funding import (
    FUNDING_CLAMP,
    INTEREST_RATE,
    adverse_funding_per_period,
    funding_cost,
    funding_rate,
    make_funding_divergence_monitor,
)


# -- funding sign mirror (LONG pays positive, SHORT receives positive) --

def test_long_pays_positive_funding_short_receives():
    # Positive cumulative rate: LONG cost > 0 (pays), SHORT cost < 0 (receives a credit).
    assert funding_cost("0.001", PositionSide.LONG) == Decimal("0.001")
    assert funding_cost("0.001", PositionSide.SHORT) == Decimal("-0.001")


def test_negative_funding_flips_the_sign():
    assert funding_cost("-0.001", PositionSide.LONG) == Decimal("-0.001")
    assert funding_cost("-0.001", PositionSide.SHORT) == Decimal("0.001")  # short PAYS


# -- adverse-per-period (the monitor's observation) ---------------------

def test_short_adverse_only_when_it_pays():
    # Positive funding regime -> short receives -> adverse component is 0 (not adverse).
    assert adverse_funding_per_period("0.001", PositionSide.SHORT, 3) == Decimal("0")
    # Negative funding regime -> short pays -> adverse component > 0.
    adv = adverse_funding_per_period("-0.003", PositionSide.SHORT, 3)
    assert adv == Decimal("0.001")  # 0.003 cost / 3 periods


def test_adverse_per_period_rejects_nonpositive_periods():
    with pytest.raises(ValueError):
        adverse_funding_per_period("0.001", PositionSide.SHORT, 0)


# -- funding-rate clamp (exchange-set) ----------------------------------

def test_funding_rate_clamps_the_interest_premium_delta():
    # avg_premium far below IR: delta = IR - prem is clamped to +FUNDING_CLAMP.
    fr = funding_rate("-0.01")
    assert fr == Decimal("-0.01") + FUNDING_CLAMP
    # avg_premium far above IR: delta clamped to -FUNDING_CLAMP.
    fr2 = funding_rate("0.01")
    assert fr2 == Decimal("0.01") - FUNDING_CLAMP


def test_funding_rate_unclamped_in_band():
    # Small premium: delta within the clamp band passes through.
    prem = Decimal("0.0002")
    fr = funding_rate(prem)
    assert fr == prem + (INTEREST_RATE - prem)  # == INTEREST_RATE


# -- the signals-only monitor (mod:Perp_Funding_Divergence_Monitor) -----

def test_monitor_is_an_ewma_monitor_instance():
    m = make_funding_divergence_monitor()
    assert isinstance(m, EwmaMonitor)


def test_monitor_uses_registry_seed_and_sustained_count():
    m = make_funding_divergence_monitor()
    # Configured from the perp_funding_divergence_monitor threshold + fee_tier sustained count.
    thr = Decimal(str(registry.value("perp_funding_divergence_monitor")))
    n = int(registry.value("fee_tier_divergence_sustained_trades"))
    # Below threshold for n-1 obs: not yet sustained; one more crosses it.
    feed = Decimal(str(thr)) * Decimal("3")  # comfortably above threshold
    for _ in range(n - 1):
        m.update(feed)
    assert m.sustained is False
    m.update(feed)
    assert m.sustained is True


def test_monitor_silent_on_real_positive_funding_regime():
    # W2: fed the adverse component of a positive-funding regime (all 0), it NEVER fires.
    m = make_funding_divergence_monitor()
    for _ in range(500):
        adv = adverse_funding_per_period("0.0009", PositionSide.SHORT, 3)  # short receives -> 0
        m.update(adv)
    assert m.sustained is False
    assert m.diverging is False


def test_monitor_fires_on_sustained_pinned_adverse_funding():
    # W2 stress: a sustained pinned-adverse regime (short pays ~0.13%/day) fires after sustained_n.
    m = make_funding_divergence_monitor()
    n = int(registry.value("fee_tier_divergence_sustained_trades"))
    fired = 0
    for _ in range(n + 5):
        adv = adverse_funding_per_period("-0.0039", PositionSide.SHORT, 3)  # 0.0013/day adverse
        m.update(adv)
        if m.sustained:
            fired += 1
    assert fired > 0  # it eventually fires
