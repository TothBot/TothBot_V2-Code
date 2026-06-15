"""Tests: mod:Signal_Pipeline orchestrator (pipeline/signal_pipeline.py).

Covers 0500000 dv1_250 Image2: the 8-gate chain order, first-failure short-circuit, the side
threading (a SHORT takes the short test everywhere), and ACCEPTED -> G8 sized order. The SSS
step is injected so the chaining is exercised in isolation. Decimal-only downstream (AR-047).
"""

from __future__ import annotations

from tothbot.exchange.position_mirror import PositionSide
from tothbot.regime.sss import SssComputeError
from tothbot.regime.taxonomy import Regime
from tothbot.pipeline.signal_pipeline import PipelineInputs, run_pipeline


_SIGNAL_PARAMS = {
    "rsi_14": 42, "ema_9": 101, "ema_21": 100, "volume_ratio": 1.3, "sss_pass": True, "side": "long",
}


class _Verdict:
    def __init__(self, passed):
        self.passed = passed
        self.code = "SIGNAL_PASS" if passed else "SIGNAL_REJECTED"
        self.signal_params = dict(_SIGNAL_PARAMS, sss_pass=passed)


def _sss_pass(symbol, closes, volumes, *, side, **kw):
    return _Verdict(True)


def _sss_fail(symbol, closes, volumes, *, side, **kw):
    return _Verdict(False)


def _sss_warmup(symbol, closes, volumes, *, side, **kw):
    raise SssComputeError("warmup")


def _inputs(**over):
    """A LONG candidate in TRENDING_POS_NORMAL that passes every gate."""
    kw = dict(
        instrument_status="online", marginable=True,
        ws_state="Subscribed",
        vol_24h_usd="600000",
        regime=Regime.TRENDING_POS_NORMAL,
        ema20_daily="105", ema50_daily="100", close_1h="106", ema20_1h="104",
        closes=[1] * 30, volumes=[1] * 30,
        candle_open="100", candle_high="110", candle_low="99", candle_close="108",
        seconds_since_last_exit="600", consecutive_loss_count=0,
        has_active_same_side_position=False,
        base_per_trade_size_usd="50",
        wallet_balance="5000", portfolio_baseline="5000",
        candidate_committed_usd="1000", total_committed_usd="2000", semaphore_locked=False,
        entry_fill_price="60000", atr_14="1000", expected_reward="0.05",
    )
    kw.update(over)
    return PipelineInputs(**kw)


def _run(side=PositionSide.LONG, sss=_sss_pass, **over):
    return run_pipeline("BTC/USD", side, _inputs(**over), sss_evaluator=sss)


# -- full pass ----------------------------------------------------------

def test_long_candidate_passes_all_gates_and_is_accepted():
    out = _run()
    assert out.accepted is True
    assert out.stage == "G8"
    assert out.reason == "G8_SIZED"
    assert out.sized is not None
    assert out.sized.code == "G8_SIZED"
    # the entry-time SSS levels ride the accept (the contract:TRADE_CLOSE field-19 producer input)
    assert out.signal_params == dict(_SIGNAL_PARAMS, sss_pass=True)


def test_rejected_outcome_carries_no_signal_params():
    out = _run(sss=_sss_fail)
    assert out.accepted is False
    assert out.signal_params is None   # only an ACCEPTED entry stashes the levels


def test_short_candidate_passes_with_short_tests_throughout():
    out = _run(
        side=PositionSide.SHORT,
        regime=Regime.TRENDING_NEG_NORMAL,          # G3 permits short
        ema20_daily="95", ema50_daily="100", close_1h="94", ema20_1h="96",  # G4 short alignment
    )
    assert out.accepted is True
    assert out.side is PositionSide.SHORT
    assert out.sized.side is PositionSide.SHORT
    assert out.sized.order_type == "margin_sell_to_open"


# -- short-circuit at each gate (strict order) --------------------------

def test_pre_gate_1_status_blocks():
    out = _run(instrument_status="maintenance")
    assert out.stage == "PRE_GATE_1"
    assert out.reason == "INSTRUMENT_STATUS_BLOCKED"
    assert out.accepted is False


def test_pre_gate_1_short_ineligible_on_non_marginable():
    out = _run(
        side=PositionSide.SHORT, marginable=False, regime=Regime.TRENDING_NEG_NORMAL,
        ema20_daily="95", ema50_daily="100", close_1h="94", ema20_1h="96",
    )
    assert out.stage == "PRE_GATE_1"
    assert out.reason == "SHORT_INELIGIBLE"


def test_gate_1_state_machine_waits():
    out = _run(ws_state="Idle")
    assert out.stage == "G1"


def test_gate_2_liquidity_rejects():
    out = _run(vol_24h_usd="100000")
    assert out.stage == "G2"
    assert out.reason == "LIQUIDITY_REJECTED"


def test_gate_3_regime_blocks_long_in_downtrend():
    # a LONG candidate in TRENDING_NEG is not permitted -> G3 REGIME_BLOCKED.
    out = _run(regime=Regime.TRENDING_NEG_NORMAL)
    assert out.stage == "G3"
    assert out.reason == "REGIME_BLOCKED"


def test_gate_4_htf_rejects_on_misalignment():
    out = _run(ema20_daily="99")  # daily not bullish -> HTF reject
    assert out.stage == "G4"
    assert out.reason == "HTF_GATE_REJECTED"


def test_sss_signal_rejected():
    out = _run(sss=_sss_fail)
    assert out.stage == "SSS"
    assert out.reason == "SIGNAL_REJECTED"


def test_sss_warmup_is_a_clean_skip():
    out = _run(sss=_sss_warmup)
    assert out.stage == "SSS"
    assert out.reason == "WARM_UP"


def test_gate_5_selection_rejects():
    out = _run(consecutive_loss_count=3)
    assert out.stage == "G5"
    assert out.reason == "SELECTION_REJECTED"


def test_gate_7_risk_guard_halts():
    out = _run(wallet_balance="4000")  # 20% drawdown -> HALT
    assert out.stage == "G7"
    assert out.reason == "HALT"


def test_gate_8_below_sacred_floor_rejects():
    out = _run(expected_reward="0.04")  # rr 1.32 < 1.5
    assert out.stage == "G8"
    assert out.reason == "G8_A1_REJECT"


# -- strict ordering: earliest failure wins -----------------------------

def test_earliest_failure_short_circuits():
    # bad status AND bad liquidity AND bad R:R -> stops at Pre-Gate-1 (the first).
    out = _run(instrument_status="delisted", vol_24h_usd="1", expected_reward="0.001")
    assert out.stage == "PRE_GATE_1"
