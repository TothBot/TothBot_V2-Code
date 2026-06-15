"""Tests: mod:Execution_Engine connector (execution/execution_engine.py).

Covers 0500000 dv1_250 sec 7 + UT-EE-007: a gate:G8-accepted candidate is sized (MPP cap),
its emergSL is derived from the ACTUAL fill (not the G8 estimate), and the whole entry is
dispatched through WSManager into THIS side's wallet - end to end, both sides.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from tothbot.config.settings import Mode
from tothbot.exchange.position_mirror import PositionSide
from tothbot.exchange.ws_manager import WSManager
from tothbot.execution.execution_engine import execute_entry
from tothbot.pipeline.position_sizer import size_candidate

_DEADLINE = "2026-06-15T07:30:00Z"


def _g8(side):
    """A gate:G8-accepted order for the side (entry 60000, ATR 1000, expected_reward 0.05)."""
    out = size_candidate("BTC/USD", side, "60000", "1000", "0.05")
    assert out.accepted
    return out.event


def test_long_entry_executes_with_emergsl_from_actual_fill():
    wm = WSManager(Mode.PAPER)
    sized = _g8(PositionSide.LONG)
    filled = asyncio.run(execute_entry(
        wm, PositionSide.LONG, "BTC/USD", sized,
        sized_usd="1000", best_bid="59990", best_ask="60000", mpp_abs_cap_pct="0.01",
        atr_14_entry="1000", regime_at_entry="TRENDING_POS_NORMAL",
        cl_ord_id="cl-1", deadline=_DEADLINE,
    ))
    assert filled is True
    pos = wm.position("BTC/USD")
    assert pos.side is PositionSide.LONG
    # cap = min(0.01, 0.05-1.5*0.0302=0.0047)=0.0047 -> fill = 60000*1.0047 = 60282.
    fill = Decimal("60000") * Decimal("1.0047")
    assert pos.avg_entry_price == fill
    # emergSL from the ACTUAL fill: 60282 - (0.05*60000=3000) = 57282 (NOT the 57000 estimate).
    assert pos.emergsl_price == fill - Decimal("3000")
    assert pos.emergsl_price == Decimal("57282")
    assert pos.atr_14_entry == Decimal("1000")
    # the long wallet was debited; the short wallet is untouched.
    assert wm.wallet_balance(PositionSide.LONG) < Decimal("5000.0")
    assert wm.wallet_balance(PositionSide.SHORT) == Decimal("5000.0")


def test_short_entry_executes_margin_with_emergsl_above_fill():
    wm = WSManager(Mode.PAPER)
    sized = _g8(PositionSide.SHORT)
    filled = asyncio.run(execute_entry(
        wm, PositionSide.SHORT, "BTC/USD", sized,
        sized_usd="1000", best_bid="59990", best_ask="60000", mpp_abs_cap_pct="0.01",
        atr_14_entry="1000", regime_at_entry="TRENDING_NEG_NORMAL",
        cl_ord_id="cl-2", deadline=_DEADLINE,
    ))
    assert filled is True
    pos = wm.position("BTC/USD")
    assert pos.side is PositionSide.SHORT
    # cap = min(0.01, 0.05-1.5*0.0304=0.0044)=0.0044 -> fill = 59990*(1-0.0044).
    fill = Decimal("59990") * (Decimal("1") - Decimal("0.0044"))
    assert pos.avg_entry_price == fill
    # SHORT emergSL is ABOVE the fill (buy-to-cover): fill + 3000.
    assert pos.emergsl_price == fill + Decimal("3000")
    assert pos.emergsl_price > fill
    # the short wallet was credited (sell-to-open); the long wallet is untouched.
    assert wm.wallet_balance(PositionSide.SHORT) > Decimal("5000.0")
    assert wm.wallet_balance(PositionSide.LONG) == Decimal("5000.0")
