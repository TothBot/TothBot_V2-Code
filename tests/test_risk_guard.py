"""Tests: gate:G7_Risk_Guard (pipeline/risk_guard.py).

Covers 0500000 dv1_250 Image8 (Gate 7 Risk Guard Detail, R7): four checks in STRICT
order with first-failure short-circuit - per-wallet drawdown cascade (HALT/PAUSE),
capital-fraction concentration + exposure (inclusive <=, 100% seed NON-BINDING, no cap),
and the non-blocking per-module semaphore probe (SKIP). Pure compute, Decimal-only (AR-047).

Seeds: full_halt 10%, session_pause 5%, concentration_limit 100%, exposure_limit 100%.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.exchange.position_mirror import PositionSide
from tothbot.pipeline.risk_guard import (
    G7ConcentrationBreach,
    G7DrawdownHalt,
    G7ExposureBreach,
    G7SemaphoreBusy,
    RiskDisposition,
    evaluate_risk_guard,
)


def _healthy(**over):
    """A candidate that passes all 4 checks unless overridden."""
    kw = dict(
        wallet_balance="5000",
        portfolio_baseline="5000",
        candidate_committed_usd="1000",
        total_committed_usd="2000",
        semaphore_locked=False,
    )
    kw.update(over)
    return evaluate_risk_guard(PositionSide.LONG, **kw)


# -- all pass -----------------------------------------------------------

def test_all_four_checks_pass_proceeds_to_g8():
    out = _healthy()
    assert out.passed is True
    assert out.disposition is RiskDisposition.PASS
    assert out.event is None


# -- CHECK 1: drawdown cascade ------------------------------------------

def test_full_halt_drawdown_halts():
    # (5000 - 4400)/5000 = 0.12 >= 0.10 full_halt -> HALT.
    out = _healthy(wallet_balance="4400")
    assert out.disposition is RiskDisposition.HALT
    assert isinstance(out.event, G7DrawdownHalt)
    assert out.event.threshold_crossed == "full_halt"
    assert out.event.disposition == "HALT"
    assert out.event.drawdown_pct == Decimal("0.12")
    assert out.event.code == "G7_DRAWDOWN_HALT"


def test_session_pause_drawdown_pauses():
    # (5000 - 4650)/5000 = 0.07: >= 0.05 session_pause but < 0.10 full_halt -> PAUSE.
    out = _healthy(wallet_balance="4650")
    assert out.disposition is RiskDisposition.PAUSE
    assert out.event.threshold_crossed == "session_pause"
    assert out.event.disposition == "PAUSE"


def test_drawdown_threshold_is_inclusive_fail_at_session_pause():
    # exactly 5% (PASS requires drawdown STRICTLY below the threshold) -> PAUSE.
    out = _healthy(wallet_balance="4750")  # (5000-4750)/5000 = 0.05
    assert out.event.drawdown_pct == Decimal("0.05")
    assert out.disposition is RiskDisposition.PAUSE


def test_drawdown_just_under_session_pause_passes():
    # 4751 -> 0.0498 < 0.05 -> CHECK 1 passes (and the rest pass) -> PASS.
    out = _healthy(wallet_balance="4751")
    assert out.disposition is RiskDisposition.PASS


# -- CHECK 2: concentration (capital fraction) --------------------------

def test_concentration_breach_blocks():
    # override a binding 0.5 cap; 3000/5000 = 0.6 > 0.5 -> BLOCK.
    out = _healthy(candidate_committed_usd="3000", concentration_limit="0.5")
    assert out.disposition is RiskDisposition.BLOCK
    assert isinstance(out.event, G7ConcentrationBreach)
    assert out.event.concentration_ratio == Decimal("0.6")
    assert out.event.concentration_limit == Decimal("0.5")


def test_concentration_is_inclusive_at_the_limit():
    # 2500/5000 = 0.5 <= 0.5 -> passes CHECK 2 (up to the limit is allowed).
    out = _healthy(candidate_committed_usd="2500", concentration_limit="0.5")
    assert out.disposition is RiskDisposition.PASS


# -- CHECK 3: exposure (capital fraction, inclusive) --------------------

def test_exposure_breach_blocks():
    # override 0.5 cap; total 3000/5000 = 0.6 > 0.5 -> BLOCK (concentration passes first).
    out = _healthy(total_committed_usd="3000", exposure_limit_pct="0.5")
    assert out.disposition is RiskDisposition.BLOCK
    assert isinstance(out.event, G7ExposureBreach)
    assert out.event.exposure_ratio == Decimal("0.6")
    assert out.event.exposure_limit_pct == Decimal("0.5")


# -- CHECK 4: semaphore -------------------------------------------------

def test_semaphore_locked_skips_cycle():
    out = _healthy(semaphore_locked=True)
    assert out.disposition is RiskDisposition.SKIP
    assert isinstance(out.event, G7SemaphoreBusy)
    assert out.event.code == "G7_SEMAPHORE_BUSY"


# -- the 100% seed is NON-BINDING (no cap) ------------------------------

def test_full_wallet_commit_passes_under_non_binding_seed():
    # default limits (100%): committing the ENTIRE wallet across all positions passes -
    # the wallet is the sole sizing boundary, no cap (Bill ruling).
    out = _healthy(candidate_committed_usd="5000", total_committed_usd="5000")
    assert out.disposition is RiskDisposition.PASS  # 1.0 <= 1.0 both checks


# -- strict order: first failure wins -----------------------------------

def test_drawdown_short_circuits_before_semaphore_and_exposure():
    # drawdown HALT even though semaphore is also locked and exposure would breach.
    out = _healthy(
        wallet_balance="4000",            # 0.20 drawdown -> HALT
        total_committed_usd="9999",
        exposure_limit_pct="0.1",
        semaphore_locked=True,
    )
    assert out.disposition is RiskDisposition.HALT


def test_concentration_short_circuits_before_exposure():
    # both concentration and exposure would breach; concentration (CHECK 2) wins.
    out = _healthy(
        candidate_committed_usd="4000", concentration_limit="0.5",   # 0.8 > 0.5
        total_committed_usd="4000", exposure_limit_pct="0.5",        # would also breach
    )
    assert isinstance(out.event, G7ConcentrationBreach)


# -- direction symmetry -------------------------------------------------

def test_long_and_short_evaluate_identically():
    common = dict(
        wallet_balance="5000", portfolio_baseline="5000",
        candidate_committed_usd="1000", total_committed_usd="2000", semaphore_locked=False,
    )
    lng = evaluate_risk_guard(PositionSide.LONG, **common)
    sht = evaluate_risk_guard(PositionSide.SHORT, **common)
    assert lng.disposition is sht.disposition is RiskDisposition.PASS
    # the only side-dependence is the event label; a breach carries the side through.
    lng_b = evaluate_risk_guard(PositionSide.SHORT, **{**common, "semaphore_locked": True})
    assert lng_b.event.side is PositionSide.SHORT


# -- guards / AR-047 ----------------------------------------------------

def test_non_positive_baseline_is_a_loud_defect():
    with pytest.raises(ValueError):
        _healthy(portfolio_baseline="0")


def test_no_float_enters_the_guard():
    out = evaluate_risk_guard(
        PositionSide.LONG,
        wallet_balance=4400.0, portfolio_baseline=5000.0,
        candidate_committed_usd=1000.0, total_committed_usd=2000.0, semaphore_locked=False,
    )
    assert out.event.drawdown_pct == Decimal("0.12")
    assert isinstance(out.event.drawdown_pct, Decimal)
