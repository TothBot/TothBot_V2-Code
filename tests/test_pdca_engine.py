"""mod:CIATS_PDCA_Engine tests (ciats/pdca_engine.py).

Covers the CHECK-phase absolute gate (Mann-Whitney U one-sided alpha=0.01: a better cohort advances,
same/worse rejects) + the Sharpe/Spearman corroboration, and the PLAN->DO->CHECK->ACT state machine
with its HR-CI gates: the 200-trade floor (HR-CI-004), the sacred-R:R never-a-candidate rule, the
Bill-approval gate (HR-CI-011), the inter-trade-boundary gate (HR-CI-003), and the 50-trade interval
(HR-CI-005). DETECTION/PROPOSAL ONLY - the ACT emits an ApprovedChange, never writes a parameter.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from tothbot.ciats.pdca_engine import (
    MIN_TRADES_BETWEEN_CHANGES,
    ApprovedChange,
    CheckResult,
    PdcaEngine,
    PdcaPhase,
    PlanBlocked,
    check_phase,
)


def _seq(lo, hi):
    return [Decimal(x) for x in range(lo, hi)]


def _proposal(param="sc_body_threshold", value="0.6"):
    return SimpleNamespace(param_name=param, proposed_value=value)


# --------------------------------------------------------------------------- CHECK phase
def test_check_passes_for_a_clearly_better_cohort():
    r = check_phase(_seq(15, 35), _seq(0, 20))  # candidate shifted well above baseline
    assert isinstance(r, CheckResult)
    assert r.passed is True
    assert r.mw_z > r.mw_crit
    assert r.sharpe_improved is True


def test_check_rejects_an_identical_cohort():
    r = check_phase(_seq(0, 20), _seq(0, 20))
    assert r.passed is False
    assert r.mw_z == Decimal("0")


def test_check_rejects_a_worse_cohort():
    r = check_phase(_seq(-20, 0), _seq(0, 20))
    assert r.passed is False


def test_check_spearman_corroboration_when_paired_series_supplied():
    # A strong monotone (param level vs outcome) over n >= 200 -> |rho| > 0.3 AND p < 0.05.
    x = _seq(0, 220)
    y = _seq(0, 220)
    r = check_phase(_seq(15, 35), _seq(0, 20), spearman_xy=(x, y))
    assert r.spearman is not None
    assert r.spearman.rho == Decimal("1")
    assert r.spearman.significant is True


def test_check_spearman_absent_when_not_supplied():
    assert check_phase(_seq(15, 35), _seq(0, 20)).spearman is None


# --------------------------------------------------------------------------- PLAN gate
def test_plan_blocked_below_the_200_trade_floor():
    e = PdcaEngine()
    blocked = e.initiate_plan(_proposal(), trade_count=199)
    assert isinstance(blocked, PlanBlocked)
    assert e.phase is PdcaPhase.IDLE


def test_plan_blocks_the_sacred_rr_param():
    e = PdcaEngine()
    blocked = e.initiate_plan(_proposal(param="rr_floor"), trade_count=500)
    assert isinstance(blocked, PlanBlocked)
    assert "R:R" in blocked.reason or "Sacred" in blocked.reason


def test_plan_admits_a_valid_candidate_above_the_floor():
    e = PdcaEngine()
    p = _proposal()
    assert e.initiate_plan(p, trade_count=250) is p
    assert e.phase is PdcaPhase.PLAN


def test_out_of_cycle_plan_still_respects_the_floor():
    e = PdcaEngine()
    assert isinstance(e.initiate_plan(_proposal(), trade_count=150, out_of_cycle=True), PlanBlocked)


# --------------------------------------------------------------------------- full cycle / ACT gates
def _advance_to_awaiting_approval():
    e = PdcaEngine()
    e.initiate_plan(_proposal(), trade_count=250)
    e.run_do(_seq(15, 35), _seq(0, 20))
    assert e.phase is PdcaPhase.DO
    cr = e.run_check()
    assert cr.passed is True
    assert e.phase is PdcaPhase.AWAITING_APPROVAL
    return e


def test_check_failure_routes_to_rejected():
    e = PdcaEngine()
    e.initiate_plan(_proposal(), trade_count=250)
    e.run_do(_seq(0, 20), _seq(0, 20))  # identical -> fails the absolute gate
    e.run_check()
    assert e.phase is PdcaPhase.REJECTED


def test_act_requires_bill_approval():
    e = _advance_to_awaiting_approval()
    out = e.act(approved=False, at_inter_trade_boundary=True, trades_since_last_change=99)
    assert isinstance(out, PlanBlocked)
    assert e.phase is PdcaPhase.AWAITING_APPROVAL  # retryable, not consumed


def test_act_requires_inter_trade_boundary():
    e = _advance_to_awaiting_approval()
    out = e.act(approved=True, at_inter_trade_boundary=False, trades_since_last_change=99)
    assert isinstance(out, PlanBlocked)


def test_act_requires_50_trade_interval():
    e = _advance_to_awaiting_approval()
    out = e.act(approved=True, at_inter_trade_boundary=True,
                trades_since_last_change=MIN_TRADES_BETWEEN_CHANGES - 1)
    assert isinstance(out, PlanBlocked)


def test_act_emits_approved_change_when_all_gates_pass():
    e = _advance_to_awaiting_approval()
    out = e.act(approved=True, at_inter_trade_boundary=True,
                trades_since_last_change=MIN_TRADES_BETWEEN_CHANGES)
    assert isinstance(out, ApprovedChange)
    assert out.proposal.param_name == "sc_body_threshold"
    assert out.check.passed is True
    assert e.phase is PdcaPhase.ADVANCED


# --------------------------------------------------------------------------- phase guards
def test_run_check_requires_do_phase():
    e = PdcaEngine()
    e.initiate_plan(_proposal(), trade_count=250)  # in PLAN, not DO
    with pytest.raises(ValueError):
        e.run_check()


def test_act_requires_awaiting_approval_phase():
    e = PdcaEngine()
    with pytest.raises(ValueError):
        e.act(approved=True, at_inter_trade_boundary=True, trades_since_last_change=99)
