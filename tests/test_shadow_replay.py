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


def _rec_sp(net, **levels):
    """A record carrying a signal_params dict (the entry re-simulation input) + net P/L."""
    return SimpleNamespace(
        net_pl_usd=Decimal(net),
        signal_params={k: Decimal(str(v)) for k, v in levels.items()},
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


# ------------------------------------------------------ entry-filter re-simulation (TB00751 c)
def test_volume_threshold_excludes_a_trade_below_the_raised_floor():
    # SC-SSS-3: pass iff volume_ratio > threshold. Raising the floor to 1.5 EXCLUDES the low-volume
    # trade (it would not have been entered) and keeps the high-volume one.
    ev = build_shadow_evaluator(_proposal("volume_sss_threshold", Decimal("1.5")))
    assert ev(_rec_sp("-2", volume_ratio="1.1")) is None      # 1.1 !> 1.5 -> excluded
    assert ev(_rec_sp("4", volume_ratio="2.0")) == Decimal("4")  # 2.0 > 1.5 -> kept


def test_rsi_low_bound_excludes_a_trade_below_the_raised_low():
    # SC-SSS-1 long: pass iff rsi_14 > low. Raising the low bound to 40 excludes the low-rsi trade.
    ev = build_shadow_evaluator(_proposal("rsi_long_low", Decimal("40")))
    assert ev(_rec_sp("-1", rsi_14="35")) is None             # 35 !> 40 -> excluded
    assert ev(_rec_sp("2", rsi_14="45")) == Decimal("2")      # 45 > 40 -> kept


def test_rsi_high_bound_excludes_a_trade_above_the_lowered_high():
    # SC-SSS-1 long: pass iff rsi_14 < high. Lowering the high bound to 45 excludes the high-rsi trade.
    ev = build_shadow_evaluator(_proposal("rsi_long_high", Decimal("45")))
    assert ev(_rec_sp("-1", rsi_14="48")) is None             # 48 !< 45 -> excluded
    assert ev(_rec_sp("2", rsi_14="42")) == Decimal("2")      # 42 < 45 -> kept


def test_entry_filter_without_the_level_replays_as_baseline():
    # a record carrying no signal_params / not the gated level cannot be re-decided (seed-then-correct).
    ev = build_shadow_evaluator(_proposal("volume_sss_threshold", Decimal("1.5")))
    assert ev(_rec("3")) == Decimal("3")                      # no signal_params -> realized outcome
    assert ev(_rec_sp("-2", rsi_14="35")) == Decimal("-2")    # has signal_params but not volume_ratio


# --------------------------------------------------------------------------- seed-then-correct
def test_unmodellable_param_replays_as_the_realized_outcome():
    ev = build_shadow_evaluator(_proposal("sc_body_threshold", Decimal("0.7"), current=Decimal("0.5")))
    assert ev(_rec("3")) == Decimal("3")       # no per-trade signal field -> baseline, never a crash
    assert ev(_rec("-2")) == Decimal("-2")


def test_ema_period_param_is_not_re_simulatable():
    # ema_9/ema_21 are LEVELS but sss_ema_short/long are PERIODS - a level cannot re-decide a period
    # change, so an ema-period proposal replays as the realized outcome (seed-then-correct).
    ev = build_shadow_evaluator(_proposal("sss_ema_short", Decimal("12"), current=Decimal("9")))
    assert ev(_rec_sp("3", ema_9="100", ema_21="98")) == Decimal("3")
    assert ev(_rec_sp("-1", ema_9="100", ema_21="98")) == Decimal("-1")


# --------------------------------------------------------------------------- through shadow_cohorts
def test_gating_cohort_drops_the_excluded_records():
    records = [_rec("2", regime=Regime.TRENDING_POS_NORMAL),
               _rec("-1", regime=Regime.NON_DIR_NORMAL),
               _rec("2", regime=Regime.TRENDING_POS_NORMAL)]
    ev = build_shadow_evaluator(_proposal("disallowed_regimes", Regime.NON_DIR_NORMAL))
    candidate, baseline = shadow_cohorts(records, ev)
    assert baseline == [Decimal("2"), Decimal("-1"), Decimal("2")]   # the realized cohort
    assert candidate == [Decimal("2"), Decimal("2")]                 # the loser was gated out
