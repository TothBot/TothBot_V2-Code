"""Tests: the per-candidate conductor (pipeline/driver.py).

Covers the full tie 0500000 sec 3/7: run_pipeline -> mod:Logger -> on ACCEPT execute_entry into
the side's wallet. A passing candidate is dispatched + filled (a position opens in the wallet);
a rejected candidate is logged but never dispatched.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from tothbot.config.settings import Mode
from tothbot.exchange.position_mirror import PositionSide
from tothbot.exchange.ws_manager import WSManager
from tothbot.recorder.logger import Logger
from tothbot.regime.taxonomy import Regime
from tothbot.pipeline.driver import CandidateResult, ExecutionContext, process_candidate
from tothbot.pipeline.signal_pipeline import PipelineInputs


def _sss_pass(symbol, closes, volumes, *, side, **kw):
    return type("V", (), {"passed": True, "code": "SIGNAL_PASS"})()


def _inputs(**over):
    """A LONG candidate in TRENDING_POS_NORMAL that passes every gate."""
    kw = dict(
        instrument_status="online", marginable=True, ws_state="Subscribed", vol_24h_usd="600000",
        regime=Regime.TRENDING_POS_NORMAL,
        ema20_daily="105", ema50_daily="100", close_1h="106", ema20_1h="104",
        closes=[1] * 30, volumes=[1] * 30,
        candle_open="100", candle_high="110", candle_low="99", candle_close="108",
        seconds_since_last_exit="600", consecutive_loss_count=0, has_active_same_side_position=False,
        base_per_trade_size_usd="50",
        wallet_balance="5000", portfolio_baseline="5000",
        candidate_committed_usd="1000", total_committed_usd="2000", semaphore_locked=False,
        entry_fill_price="60000", atr_14="1000", expected_reward="0.05",
    )
    kw.update(over)
    return PipelineInputs(**kw)


def _ctx():
    return ExecutionContext(
        sized_usd="1000", best_bid="59990", best_ask="60000", mpp_abs_cap_pct="0.01",
        atr_14_entry="1000", regime_at_entry="TRENDING_POS_NORMAL",
        cl_ord_id="cl-1", deadline="2026-06-15T07:30:00Z",
    )


def _run(side, inputs, **over):
    wm = WSManager(Mode.PAPER)
    logger = Logger()
    res = asyncio.run(process_candidate(
        wm, logger, side, "BTC/USD", inputs, _ctx(), sss_evaluator=_sss_pass,
    ))
    return wm, logger, res


def test_accepted_candidate_is_dispatched_filled_and_logged():
    wm, logger, res = _run(PositionSide.LONG, _inputs())
    assert isinstance(res, CandidateResult)
    assert res.outcome.accepted is True
    assert res.dispatched is True
    assert res.filled is True
    # a position opened in the LONG wallet, with the entry-time emergSL snapshot.
    pos = wm.position("BTC/USD")
    assert pos is not None and pos.side is PositionSide.LONG
    assert pos.emergsl_price is not None
    assert wm.wallet_balance(PositionSide.LONG) < Decimal("5000.0")
    # the pipeline outcome was logged to Stream-1 under the long module tag.
    assert res.outcome in logger.operational


def test_short_accepted_candidate_opens_short_in_short_wallet():
    inputs = _inputs(
        regime=Regime.TRENDING_NEG_NORMAL,
        ema20_daily="95", ema50_daily="100", close_1h="94", ema20_1h="96",
    )
    wm = WSManager(Mode.PAPER)
    logger = Logger()
    res = asyncio.run(process_candidate(
        wm, logger, PositionSide.SHORT, "BTC/USD", inputs, _ctx(), sss_evaluator=_sss_pass,
    ))
    assert res.dispatched is True and res.filled is True
    assert wm.position("BTC/USD").side is PositionSide.SHORT
    assert wm.wallet_balance(PositionSide.SHORT) > Decimal("5000.0")
    assert wm.wallet_balance(PositionSide.LONG) == Decimal("5000.0")


def test_accepted_candidate_stashes_the_producer_fields_on_the_position():
    # the driver threads outcome.signal_params + ctx.market_regime/entry_timestamp_utc through
    # execute_entry -> dispatch_entry -> the opening fill, so the position carries the entry context.
    def _sss_with_params(symbol, closes, volumes, *, side, **kw):
        return type("V", (), {
            "passed": True, "code": "SIGNAL_PASS",
            "signal_params": {"rsi_14": 40, "sss_pass": True, "side": "long"},
        })()

    wm = WSManager(Mode.PAPER)
    logger = Logger()
    ctx = ExecutionContext(
        sized_usd="1000", best_bid="59990", best_ask="60000", mpp_abs_cap_pct="0.01",
        atr_14_entry="1000", regime_at_entry="TRENDING_POS_NORMAL",
        cl_ord_id="cl-1", deadline="2026-06-15T07:30:00Z",
        market_regime="NON_DIR_NORMAL", entry_timestamp_utc="2026-06-15T07:25:00+00:00",
    )
    res = asyncio.run(process_candidate(
        wm, logger, PositionSide.LONG, "BTC/USD", _inputs(), ctx, sss_evaluator=_sss_with_params,
    ))
    assert res.filled is True
    pos = wm.position("BTC/USD")
    assert pos.signal_params == {"rsi_14": 40, "sss_pass": True, "side": "long"}
    assert pos.market_regime == "NON_DIR_NORMAL"
    assert pos.entry_timestamp_utc == "2026-06-15T07:25:00+00:00"


def test_rejected_candidate_is_logged_but_not_dispatched():
    # a LONG in a downtrend is blocked at G3 -> logged, no execution, no position.
    wm, logger, res = _run(PositionSide.LONG, _inputs(regime=Regime.TRENDING_NEG_NORMAL))
    assert res.outcome.accepted is False
    assert res.outcome.stage == "G3"
    assert res.dispatched is False and res.filled is False
    assert wm.position("BTC/USD") is None
    assert wm.wallet_balance(PositionSide.LONG) == Decimal("5000.0")
    assert res.outcome in logger.operational     # still logged (Stream-1)
