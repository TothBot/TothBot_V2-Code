"""Tests: param:perp_short_breaker_config (tothbot/perp/breaker.py).

Covers the recalibrated per-module hedge breaker (0500000 sec 13.9; TB00806e V2 winner):
PAUSE = rolling-window 10% (exogenous re-arm) / HALT = frozen-deposit 20% (ruin floor),
implemented ENTIRELY through evaluate_risk_guard's existing override params (no new gate). Also
covers the TB00804 DEADLOCK LAW (a re-armed pool always trades) and the spot LONG breaker being
UNAFFECTED (it keeps frozen 5%/10%).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.exchange.position_mirror import PositionSide
from tothbot.perp.breaker import (
    HedgeBreakerConfig,
    RollingPeakTracker,
    evaluate_hedge_breaker,
)
from tothbot.pipeline.risk_guard import RiskDisposition

_V2 = HedgeBreakerConfig.from_registry()  # rolling 10% / frozen 20%
_SPOT = HedgeBreakerConfig(  # the live spot-long default (frozen 5% / 10%), UNAFFECTED
    pause_pct=Decimal("0.05"), halt_pct=Decimal("0.10"), pause_baseline="frozen", window=Decimal("0")
)


def _short(wallet, *, rolling_peak=None, cfg=_V2, deposit=5000):
    return evaluate_hedge_breaker(
        PositionSide.SHORT, wallet_balance=wallet, deposit=deposit, config=cfg,
        rolling_peak=rolling_peak,
    )


# -- config from registry ------------------------------------------------

def test_v2_config_from_registry():
    assert _V2.pause_pct == Decimal("0.10")
    assert _V2.halt_pct == Decimal("0.20")
    assert _V2.pause_baseline == "rolling"
    assert _V2.window == Decimal("180") * Decimal("86400")  # 180 days in seconds


# -- the recalibrated short breaker (rolling pause / frozen halt) --------

def test_healthy_passes():
    out = _short(4800, rolling_peak=5000)  # 4% drawdown - below both thresholds
    assert out.disposition is RiskDisposition.PASS


def test_rolling_pause_fires_at_ten_percent_trailing():
    # 12% below the rolling high-water (5000 -> 4400) but only 12% below frozen deposit (< 20%).
    out = _short(4400, rolling_peak=5000)
    assert out.disposition is RiskDisposition.PAUSE
    assert out.baseline_used == Decimal("5000")


def test_frozen_halt_fires_at_twenty_percent_deposit():
    # 22% below the frozen deposit -> HALT (the ruin floor), short-circuiting the pause.
    out = _short(3900, rolling_peak=5000)
    assert out.disposition is RiskDisposition.HALT
    assert out.baseline_used == Decimal("5000")  # the frozen deposit


def test_halt_short_circuits_before_pause():
    # A wallet deep enough to trip BOTH: HALT wins (strict order, the frozen ruin floor).
    out = _short(3000, rolling_peak=5000)
    assert out.disposition is RiskDisposition.HALT


def test_natural_seven_point_five_percent_drawdown_does_not_halt():
    # The hedge's ~7.5% natural drawdown clears the 20% halt with wide margin (ruin-safe).
    out = _short(Decimal("5000") * Decimal("0.925"), rolling_peak=5000)
    assert out.disposition is not RiskDisposition.HALT


# -- deadlock safety (TB00804 law): a re-armed pool always trades --------

def test_rearmed_pool_passes_no_self_equity_deadlock():
    # A wallet 8% below deposit (above the 20% ruin floor) that was 11.5% below an OLD rolling
    # peak (5200) is PAUSED; once the peak re-arms down to the current wallet, drawdown is 0 and
    # it PASSES. This is the exogenous re-arm: a paused hedge is not trapped (TB00804 deadlock law,
    # it cannot self-equity-recover, so the resume must come from the time-based window, not equity).
    paused = _short(4600, rolling_peak=5200)
    assert paused.disposition is RiskDisposition.PAUSE
    rearmed = _short(4600, rolling_peak=4600)
    assert rearmed.disposition is RiskDisposition.PASS


def test_rolling_baseline_requires_a_peak():
    with pytest.raises(ValueError):
        _short(4400, rolling_peak=None)  # rolling config but no peak supplied


def test_rolling_peak_never_below_current_wallet():
    # If the supplied peak is below the wallet (shouldn't happen), it is floored at wallet -> PASS.
    out = _short(5200, rolling_peak=5000)
    assert out.disposition is RiskDisposition.PASS


# -- RollingPeakTracker: exogenous time-based re-arm ---------------------

def test_rolling_peak_tracks_window_high_water():
    t = RollingPeakTracker(window=100)
    t.observe(0, 5000)
    t.observe(50, 4400)
    # Within the window the old 5000 peak still governs.
    assert t.peak(50, 4400) == Decimal("5000")


def test_rolling_peak_rearms_after_window_passes():
    t = RollingPeakTracker(window=100)
    t.observe(0, 5000)
    t.observe(50, 4400)
    # Far past the window, the old peak has aged out -> baseline drops to current wallet (re-arm).
    assert t.peak(200, 4400) == Decimal("4400")


def test_rolling_peak_rejects_decreasing_times():
    t = RollingPeakTracker(window=100)
    t.observe(10, 5000)
    with pytest.raises(ValueError):
        t.observe(5, 5000)


# -- the spot LONG breaker is UNAFFECTED (frozen 5%/10%) ----------------

def test_spot_long_breaker_keeps_frozen_five_ten():
    # A 6% drawdown: the spot-long frozen 5% breaker PAUSES it...
    out_spot = evaluate_hedge_breaker(
        PositionSide.LONG, wallet_balance=4700, deposit=5000, config=_SPOT,
    )
    assert out_spot.disposition is RiskDisposition.PAUSE
    # ...but the recalibrated short (rolling 10%, freshly re-armed) does NOT pause the same 6%.
    out_short = _short(4700, rolling_peak=4700)
    assert out_short.disposition is RiskDisposition.PASS


def test_spot_long_frozen_halt_at_ten_percent():
    out = evaluate_hedge_breaker(
        PositionSide.LONG, wallet_balance=4400, deposit=5000, config=_SPOT,
    )
    assert out.disposition is RiskDisposition.HALT
