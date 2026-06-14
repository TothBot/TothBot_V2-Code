"""S1 foundation tests: config package (settings, fees, parameter registry).

Verifies the seed state matches 0500000 dv1_240 / TB00000 v2_97 section 8.
"""

from __future__ import annotations

from tothbot.config import fees, registry, settings
from tothbot.config.registry import ParamClass, Scope


# -- fees ---------------------------------------------------------------

def test_fee_constants():
    assert fees.FEE_MAKER_PCT == 0.0016
    assert fees.FEE_TAKER_PCT == 0.0026
    # Taker (the leg the net R:R floor pays twice) exceeds maker.
    assert fees.FEE_TAKER_PCT > fees.FEE_MAKER_PCT


# -- settings: the single paper/live flag -------------------------------

def test_default_mode_is_paper():
    s = settings.Settings()
    assert s.mode is settings.Mode.PAPER
    assert s.is_paper and not s.is_live


def test_live_mode_flags():
    s = settings.Settings(mode=settings.Mode.LIVE)
    assert s.is_live and not s.is_paper


def test_settings_is_immutable():
    s = settings.Settings()
    try:
        s.mode = settings.Mode.LIVE  # type: ignore[misc]
    except Exception:
        return
    raise AssertionError("Settings must be frozen/immutable")


def test_exactly_four_divergence_points():
    # PA-004: paper and live differ at EXACTLY four physical-necessity points.
    assert len(settings.DIVERGENCE_POINTS) == 4
    assert len(set(settings.DIVERGENCE_POINTS)) == 4  # all distinct


# -- registry: the sacred constraint ------------------------------------

def test_sacred_rr_minimum_is_the_only_sacred_value():
    sacred = registry.by_class(ParamClass.SACRED)
    assert len(sacred) == 1
    rr = sacred[0]
    assert rr.name == "rr_minimum_net"
    assert rr.value == 1.5
    assert rr.scope is Scope.UNIVERSAL


# -- registry: integrity ------------------------------------------------

def test_registry_names_are_unique():
    names = [p.name for p in registry.REGISTRY]
    assert len(names) == len(set(names))


def test_classes_partition_the_registry():
    counts = {c: len(registry.by_class(c)) for c in ParamClass}
    assert counts[ParamClass.SACRED] == 1
    assert counts[ParamClass.OPERATOR] == 2
    assert sum(counts.values()) == len(registry.REGISTRY)


def test_recipe_seeds_carry_none_value():
    # Recipes are computed in their build session, not stored as scalars.
    recipes = {"per_trade_size_usd", "expected_reward_estimator_seed", "mpp_abs_cap_pct"}
    for name in recipes:
        assert registry.value(name) is None
    seeds = registry.scalar_seeds()
    assert recipes.isdisjoint(seeds)  # excluded from scalar seeds
    assert "rr_minimum_net" in seeds   # scalars included


def test_get_unknown_raises():
    try:
        registry.get("does_not_exist")
    except KeyError:
        return
    raise AssertionError("get() must raise KeyError on unknown name")


# -- registry: representative seed values (section 8) -------------------

def test_drawdown_halts():
    assert registry.value("session_pause_drawdown_pct") == 0.05
    assert registry.value("full_halt_drawdown_pct") == 0.10


def test_exit_controller_seeds():
    assert registry.value("mae_mult") == 1.5
    assert registry.value("emergency_sl_mult") == 3.0
    assert registry.value("cancel_timeout_window") == 5.0
    assert registry.value("mpp_retry_count") == 3


def test_gate7_limits_are_non_binding_full_wallet_seeds():
    assert registry.value("concentration_limit_per_module") == 1.0
    assert registry.value("exposure_limit_pct") == 1.0


def test_per_trade_size_recipe_components():
    assert registry.value("per_trade_size_floor_usd") == 50.0
    assert registry.value("per_trade_size_margin_mult") == 5.0


def test_paper_starting_balances():
    assert registry.value("paper_starting_balance_long_usd") == 5000.0
    assert registry.value("paper_starting_balance_short_usd") == 5000.0


def test_rsi_short_mirrors_long():
    # Short RSI bounds are the mirror of long (cornerstone short symmetry).
    assert registry.value("rsi_long_low") == 30
    assert registry.value("rsi_short_low") == 70
    assert registry.value("rsi_long_high") == registry.value("rsi_short_high") == 50


def test_regime_and_sss_seeds():
    assert registry.value("adx_threshold") == 25
    assert registry.value("atr_percentile_thresh") == 67
    assert registry.value("htf_ema_periods") == (20, 50)
    assert registry.value("min_volume_usd_daily") == 500_000.0
    assert registry.value("sss_ema_short") == 9
    assert registry.value("sss_ema_long") == 21


def test_short_side_leverage_cap():
    assert registry.value("leverage_cap_short") == 3


def test_acceptance_rule():
    assert registry.value("expected_rr_acceptance_rule") == "A1"
