"""mod:CIATS_PDCA_Engine DO-phase real shadow replay tests (ciats/shadow_replay.py).

Covers build_shadow_evaluator: a GATING change (disallowed_regimes) excludes records of the blocked
asset_regime (INCLUDES/EXCLUDES); a SIZING change (per_trade_size_usd) scales every outcome by
proposed/current; an EXIT-stop change (mae_mult / emergency_sl_mult) scales the LOSSES and leaves the
GAINS unchanged; an unmodellable parameter replays as the realized outcome (seed-then-correct); and
the candidate cohort flows through shadow_cohorts the CHECK gate ranks. PURE, Decimal-only.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from tothbot.ciats.conductor import shadow_cohorts
from tothbot.ciats.shadow_replay import build_shadow_evaluator
from tothbot.regime.taxonomy import Regime


def _rec(net, *, regime=None):
    return SimpleNamespace(
        net_pl_usd=Decimal(net),
        asset_regime=(regime.value if isinstance(regime, Regime) else regime),
    )


def _proposal(param, proposed, *, current=None):
    return SimpleNamespace(param_name=param, current_value=current, proposed_value=proposed)


# --------------------------------------------------------------------------- gating (include/exclude)
def test_gating_excludes_the_blocked_regime():
    records = [_rec("2", regime=Regime.TRENDING_POS_NORMAL),
               _rec("-1", regime=Regime.NON_DIR_NORMAL),
               _rec("3", regime=Regime.TRENDING_POS_NORMAL)]
    ev = build_shadow_evaluator(_proposal("disallowed_regimes", Regime.NON_DIR_NORMAL))
    assert ev(records[0]) == Decimal("2")     # kept (allowed regime)
    assert ev(records[1]) is None             # excluded (the candidate blocks this regime)
    assert ev(records[2]) == Decimal("3")


def test_gating_accepts_a_token_or_iterable_and_ignores_unknowns():
    ev = build_shadow_evaluator(_proposal("disallowed_regimes", ["NON_DIR_NORMAL", "NOT_A_REGIME"]))
    assert ev(_rec("-1", regime=Regime.NON_DIR_NORMAL)) is None
    assert ev(_rec("5", regime=Regime.TRENDING_POS_NORMAL)) == Decimal("5")
    # a record with no asset_regime is never blocked (no regime to match)
    assert ev(_rec("4")) == Decimal("4")


# --------------------------------------------------------------------------- sizing (scale all)
def test_sizing_scales_every_outcome_by_the_size_ratio():
    ev = build_shadow_evaluator(_proposal("per_trade_size_usd", Decimal("200"), current=Decimal("100")))
    assert ev(_rec("3")) == Decimal("6")       # +3 * (200/100)
    assert ev(_rec("-2")) == Decimal("-4")     # losses scale too (qty scales the whole P/L)


def test_sizing_without_a_current_value_falls_back_to_baseline():
    ev = build_shadow_evaluator(_proposal("per_trade_size_usd", Decimal("200")))  # no current
    assert ev(_rec("3")) == Decimal("3")       # cannot form a ratio -> realized outcome


# --------------------------------------------------------------------------- exit-stop (scale losses)
def test_exit_stop_scales_losses_only():
    ev = build_shadow_evaluator(_proposal("mae_mult", Decimal("1.0"), current=Decimal("1.5")))
    # a tighter stop (1.0 vs 1.5) cuts the loss proportionally; the winner is untouched
    assert ev(_rec("-3")) == Decimal("-2")     # -3 * (1.0/1.5)
    assert ev(_rec("6")) == Decimal("6")       # gain unchanged (the stop did not bind a winner)


def test_emergency_sl_mult_is_an_exit_stop_param():
    ev = build_shadow_evaluator(_proposal("emergency_sl_mult", Decimal("6"), current=Decimal("3")))
    assert ev(_rec("-1")) == Decimal("-2")     # loss doubles with a looser emergency stop
    assert ev(_rec("4")) == Decimal("4")


# --------------------------------------------------------------------------- seed-then-correct
def test_unmodellable_param_replays_as_the_realized_outcome():
    ev = build_shadow_evaluator(_proposal("sc_body_threshold", Decimal("0.7"), current=Decimal("0.5")))
    assert ev(_rec("3")) == Decimal("3")       # no per-trade signal field -> baseline, never a crash
    assert ev(_rec("-2")) == Decimal("-2")


# --------------------------------------------------------------------------- through shadow_cohorts
def test_gating_cohort_drops_the_excluded_records():
    records = [_rec("2", regime=Regime.TRENDING_POS_NORMAL),
               _rec("-1", regime=Regime.NON_DIR_NORMAL),
               _rec("2", regime=Regime.TRENDING_POS_NORMAL)]
    ev = build_shadow_evaluator(_proposal("disallowed_regimes", Regime.NON_DIR_NORMAL))
    candidate, baseline = shadow_cohorts(records, ev)
    assert baseline == [Decimal("2"), Decimal("-1"), Decimal("2")]   # the realized cohort
    assert candidate == [Decimal("2"), Decimal("2")]                 # the loser was gated out
