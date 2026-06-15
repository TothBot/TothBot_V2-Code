"""contract:Parameter_Store_Snapshot tests (pipeline/parameter_snapshot.py + the pipeline read).

Covers the frozen per-cycle CIATS parameter view (CI-IF-003): a seed-only snapshot returns the
registry seeds; an owned store value overrides the seed; the sacred 1:1.5 R:R is never served
(raises); an unknown name raises; the snapshot is frozen against a mid-cycle store write; the
param:disallowed_regimes read; and - the point of the contract - a store-owned value FLOWS through
run_pipeline into the gates (a tuned mae_mult flips the G8 sacred-floor verdict; a disallowed regime
blocks at G3), while a seed-only / None snapshot is behavior-identical to the pre-CIATS pipeline.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from tothbot.config import registry
from tothbot.exchange.position_mirror import PositionSide
from tothbot.pipeline.parameter_snapshot import CycleParameters, build_cycle_parameters
from tothbot.pipeline.signal_pipeline import PipelineInputs, run_pipeline
from tothbot.ciats.parameter_store import ParameterStore
from tothbot.regime.taxonomy import Regime


# --------------------------------------------------------------------------- CycleParameters unit
def test_seed_only_snapshot_returns_the_registry_seed():
    params = build_cycle_parameters()
    assert params.get("mae_mult") == registry.value("mae_mult")
    assert params.get("sc_consecutive_limit") == registry.value("sc_consecutive_limit")


def test_owned_value_overrides_the_seed():
    params = CycleParameters(owned={"mae_mult": Decimal("2.0")})
    assert params.get("mae_mult") == Decimal("2.0")          # owned
    assert params.get("emergency_sl_mult") == registry.value("emergency_sl_mult")  # seed fallback


def test_sacred_rr_is_never_served():
    params = build_cycle_parameters()
    for sacred in ("rr_floor", "rr_minimum_net"):
        with pytest.raises(ValueError):
            params.get(sacred)


def test_unknown_name_raises():
    with pytest.raises(KeyError):
        build_cycle_parameters().get("not_a_real_param")


def test_regime_disallowed_read():
    params = CycleParameters(disallowed_regimes={Regime.NON_DIR_NORMAL})
    assert params.regime_disallowed(Regime.NON_DIR_NORMAL) is True
    assert params.regime_disallowed(Regime.TRENDING_POS_NORMAL) is False
    assert params.disallowed_regimes == frozenset({Regime.NON_DIR_NORMAL})


def test_snapshot_is_frozen_against_a_mid_cycle_store_write():
    store = ParameterStore()
    params = build_cycle_parameters(store)              # the frozen read for this cycle
    # a later store write must NOT perturb the already-taken snapshot (CI-IF-003 no-drift)
    change = SimpleNamespace(proposal=SimpleNamespace(param_name="mae_mult", proposed_value=Decimal("9")))
    store.apply(change, at_trade_count=300)
    assert params.get("mae_mult") == registry.value("mae_mult")  # the cycle's value, unchanged


def test_build_from_store_carries_the_owned_value():
    store = ParameterStore()
    change = SimpleNamespace(proposal=SimpleNamespace(param_name="mae_mult", proposed_value=Decimal("2.0")))
    store.apply(change, at_trade_count=300)
    assert build_cycle_parameters(store).get("mae_mult") == Decimal("2.0")


# --------------------------------------------------------------------------- the pipeline read
def _sss_pass(symbol, closes, volumes, *, side, **kw):
    return SimpleNamespace(passed=True, code="SIGNAL_PASS")


def _inputs(**over):
    """A LONG candidate in TRENDING_POS_NORMAL that passes every gate (mirrors the pipeline tests)."""
    kw = dict(
        instrument_status="online", marginable=True, ws_state="Subscribed", vol_24h_usd="600000",
        regime=Regime.TRENDING_POS_NORMAL,
        ema20_daily="105", ema50_daily="100", close_1h="106", ema20_1h="104",
        closes=[1] * 30, volumes=[1] * 30,
        candle_open="100", candle_high="110", candle_low="99", candle_close="108",
        seconds_since_last_exit="600", consecutive_loss_count=0, has_active_same_side_position=False,
        base_per_trade_size_usd="50", wallet_balance="5000", portfolio_baseline="5000",
        candidate_committed_usd="1000", total_committed_usd="2000", semaphore_locked=False,
        entry_fill_price="60000", atr_14="1000", expected_reward="0.05",
    )
    kw.update(over)
    return PipelineInputs(**kw)


def _run(params=None, **over):
    return run_pipeline("BTC/USD", PositionSide.LONG, _inputs(**over), sss_evaluator=_sss_pass, params=params)


def test_seed_only_pipeline_accepts_the_baseline():
    # seed mae_mult=1.5 -> mae_pct=0.025, net_loss~0.0302, expected_rr~1.66 >= 1.5 -> ACCEPT
    out = _run(params=None)
    assert out.accepted is True and out.reason == "G8_SIZED"


def test_owned_mae_mult_flows_into_g8_and_flips_the_sacred_floor():
    # A CIATS-owned mae_mult=3.0 doubles the risk leg -> expected_rr ~0.91 < 1.5 -> G8 A1 REJECT.
    out = _run(params=CycleParameters(owned={"mae_mult": Decimal("3.0")}))
    assert out.accepted is False
    assert out.stage == "G8" and out.reason == "G8_A1_REJECT"


def test_owned_mae_mult_changes_the_sized_order_risk_leg():
    seed = _run(params=None).sized
    tuned = _run(params=CycleParameters(owned={"mae_mult": Decimal("1.0")})).sized  # smaller risk leg
    assert tuned.mae_pct < seed.mae_pct                 # the owned value flowed into the sizer


def test_disallowed_regime_blocks_at_g3():
    out = _run(params=CycleParameters(disallowed_regimes={Regime.TRENDING_POS_NORMAL}))
    assert out.accepted is False
    assert out.stage == "G3" and out.reason == "REGIME_BLOCKED"


def test_owned_liquidity_floor_flows_into_g2():
    # raise the floor above the candidate's 600k vol -> G2 rejects (the owned value flowed)
    out = _run(params=CycleParameters(owned={"min_volume_usd_daily": Decimal("1000000")}))
    assert out.stage == "G2" and out.reason == "LIQUIDITY_REJECTED"
