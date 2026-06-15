"""Tests: mod:CIATS_EWMA_Monitor (ciats/ewma_monitor.py).

Covers 0500000 dv1_250 sec 6/7 + CIATS-FEE-002/003: the lambda=0.2 EWMA recurrence and the
sustained-divergence fire condition (the fee-tier-change-detection shape). Decimal-only (AR-047).
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.ciats.ewma_monitor import EwmaMonitor


# -- the EWMA recurrence ------------------------------------------------

def test_first_observation_seeds_the_ewma():
    m = EwmaMonitor()
    assert m.value is None
    assert m.update("10") == Decimal("10")


def test_lambda_recurrence():
    m = EwmaMonitor(lambda_="0.2")
    m.update("10")                       # ewma_0 = 10
    # ewma_1 = 0.2*20 + 0.8*10 = 12.
    assert m.update("20") == Decimal("12")


def test_constant_series_equals_the_constant():
    m = EwmaMonitor(lambda_="0.2")
    for _ in range(10):
        m.update("7")
    assert m.value == Decimal("7")


# -- sustained divergence (the fire condition) --------------------------

def test_unconfigured_monitor_never_diverges():
    m = EwmaMonitor()
    m.update("999")
    assert m.diverging is False
    assert m.sustained is False


def test_within_threshold_does_not_diverge():
    # baseline 0.0026 (taker), threshold 0.0002 -> a value at 0.0026 stays inside.
    m = EwmaMonitor(baseline="0.0026", threshold="0.0002", sustained_n=3)
    for _ in range(5):
        m.update("0.0026")
    assert m.diverging is False
    assert m.sustained is False


def test_sustained_divergence_fires_after_n_observations():
    # a persistently higher entry fee (0.0040) pushes the EWMA past 0.0026 +/- 0.0002 and holds.
    m = EwmaMonitor(baseline="0.0026", threshold="0.0002", sustained_n=3)
    fired_at = None
    for i in range(1, 30):
        m.update("0.0040")
        if m.sustained and fired_at is None:
            fired_at = i
    assert m.diverging is True
    assert fired_at is not None and fired_at >= 3   # needed the EWMA to climb past the band first


def test_divergence_run_resets_when_it_returns_inside():
    m = EwmaMonitor(baseline="0.0026", threshold="0.0002", sustained_n=2)
    for _ in range(20):
        m.update("0.0040")            # diverged + sustained
    assert m.sustained is True
    for _ in range(20):
        m.update("0.0026")            # pull it back inside the band
    assert m.diverging is False
    assert m.consecutive_divergences == 0
    assert m.sustained is False


def test_no_float_enters_the_monitor():
    m = EwmaMonitor(lambda_=0.2)
    m.update(10.0)
    assert isinstance(m.update(20.0), Decimal)
