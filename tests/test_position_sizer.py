"""Tests: gate:G8_Position_Sizer (pipeline/position_sizer.py).

Covers 0500000 dv1_249 Image9 (Gate 8 Position Sizer Detail): the risk leg
(mae_pct -> net_loss with two taker legs + the SHORT margin borrow fee), the sacred
rule:Sacred_R_R_1_to_1_5 A1 acceptance floor (the ONLY hardcoded value), and the
direction-symmetric emergSL crash brake (LONG below entry / SHORT above entry). Pure
compute, Decimal-only (AR-047).

Anchors: entry=60000, ATR(14)=1000 -> mae_pct = 1000*1.5/60000 = 0.025; LONG net_loss
= 0.025 + 0.0026 + 0.0026 = 0.0302; SHORT net_loss = +0.0002 borrow = 0.0304;
emergSL_dist = 1000*3.0/60000 = 0.05 -> LONG stop 57000, SHORT stop 63000.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.config import registry
from tothbot.config.fees import FEE_TAKER_PCT
from tothbot.exchange.position_mirror import PositionSide
from tothbot.pipeline.position_sizer import (
    SACRED_RR_FLOOR,
    G8A1Reject,
    G8Sized,
    size_candidate,
)

_TAKER = Decimal(str(FEE_TAKER_PCT))
_MARGIN_OPEN = Decimal(str(registry.value("margin_open_fee_pct")))

_ENTRY = "60000"
_ATR = "1000"


# -- the sacred floor constant ------------------------------------------

def test_sacred_floor_is_the_hardcoded_one_point_five():
    # rule:Sacred_R_R_1_to_1_5 - the ONLY hardcoded value in TothBot V2.
    assert SACRED_RR_FLOOR == Decimal("1.5")


# -- LONG risk leg + acceptance -----------------------------------------

def test_long_accept_computes_net_loss_and_sizes():
    out = size_candidate("BTC/USD", PositionSide.LONG, _ENTRY, _ATR, "0.05")
    assert out.accepted is True
    e = out.event
    assert isinstance(e, G8Sized)
    assert e.code == "G8_SIZED"
    assert e.mae_pct == Decimal("0.025")                       # 1000*1.5/60000
    assert e.net_loss == Decimal("0.0302")                     # 0.025 + 2x taker
    assert e.expected_rr == Decimal("0.05") / Decimal("0.0302")
    assert e.order_type == "spot_buy_to_open"
    # emergSL: LONG stop BELOW entry = entry - ATR*3.0 = 57000 (NEVER in the R:R ratio).
    assert e.emergsl_dist == Decimal("0.05")
    assert e.emergsl_price == Decimal("57000.000")


def test_long_reject_below_sacred_floor():
    # expected_reward 0.04 / net_loss 0.0302 = 1.3245... < 1.5 -> REJECT.
    out = size_candidate("BTC/USD", PositionSide.LONG, _ENTRY, _ATR, "0.04")
    assert out.accepted is False
    e = out.event
    assert isinstance(e, G8A1Reject)
    assert e.code == "G8_A1_REJECT"
    assert e.expected_rr == Decimal("0.04") / Decimal("0.0302")
    assert e.expected_rr < SACRED_RR_FLOOR
    assert e.net_loss == Decimal("0.0302")


# -- SHORT risk leg: the full mirror + margin borrow fee ----------------

def test_short_net_loss_adds_margin_borrow_fee():
    out = size_candidate("BTC/USD", PositionSide.SHORT, _ENTRY, _ATR, "0.05")
    assert out.accepted is True
    e = out.event
    # SHORT net_loss = LONG net_loss + the at-entry margin OPEN fee (a spot long pays none).
    assert e.net_loss == Decimal("0.0302") + _MARGIN_OPEN     # 0.0304
    assert e.net_loss == Decimal("0.0304")
    assert e.mae_pct == Decimal("0.025")                       # magnitude identical to long
    assert e.order_type == "margin_sell_to_open"
    # emergSL: SHORT stop ABOVE entry = entry + ATR*3.0 = 63000 (buy-to-cover, reduce_only).
    assert e.emergsl_dist == Decimal("0.05")
    assert e.emergsl_price == Decimal("63000.000")


def test_short_margin_borrow_fee_override():
    # An explicit borrow override (e.g. open + estimated rollover) replaces the open-fee default.
    out = size_candidate("BTC/USD", PositionSide.SHORT, _ENTRY, _ATR, "0.06", margin_borrow_fee="0.0010")
    e = out.event
    assert e.net_loss == Decimal("0.0302") + Decimal("0.0010")  # 0.0312


def test_long_ignores_margin_borrow_fee():
    # A spot LONG pays no borrow - the override is ignored, net_loss unchanged.
    out = size_candidate("BTC/USD", PositionSide.LONG, _ENTRY, _ATR, "0.05", margin_borrow_fee="0.0010")
    assert out.event.net_loss == Decimal("0.0302")


# -- the floor is inclusive (admit iff >= 1.5) --------------------------

def test_floor_is_inclusive_at_exactly_one_point_five():
    # net_loss 0.0302, expected_reward 0.0453 -> expected_rr exactly 1.5 -> ACCEPT.
    out = size_candidate("BTC/USD", PositionSide.LONG, _ENTRY, _ATR, "0.0453")
    assert out.event.expected_rr == Decimal("1.5")
    assert out.accepted is True


# -- direction symmetry: emergSL is an equidistant mirror ----------------

def test_emergsl_is_equidistant_mirror_long_vs_short():
    lng = size_candidate("BTC/USD", PositionSide.LONG, _ENTRY, _ATR, "0.05").event
    sht = size_candidate("BTC/USD", PositionSide.SHORT, _ENTRY, _ATR, "0.05").event
    entry = Decimal(_ENTRY)
    # LONG stop sits below, SHORT stop above, the SAME distance (ATR*3.0 = 3000) from entry.
    assert entry - lng.emergsl_price == sht.emergsl_price - entry == Decimal("3000.000")


# -- guards / AR-047 -----------------------------------------------------

def test_non_positive_entry_is_a_loud_defect():
    with pytest.raises(ValueError):
        size_candidate("BTC/USD", PositionSide.LONG, "0", _ATR, "0.05")


def test_no_float_enters_the_sizer():
    # AR-047: float inputs are taken as Decimal(str(value)), not Decimal(float).
    out = size_candidate("BTC/USD", PositionSide.LONG, 60000.0, 1000.0, 0.05)
    assert out.event.entry_fill_price == Decimal("60000.0")
    assert isinstance(out.event.net_loss, Decimal)
