"""Tests: the entry order path (execution/entry_dispatch.py).

Covers 0500000 dv1_250 sec 7 outbound + ar:AR-007/AR-008/AR-009: the direction-symmetric
ENTRY add_order (long spot buy-to-open / short margin sell-to-open) and the ON-FILL emergSL
batch_add (long sell-stop below / short buy-to-cover reduce_only above). Pure message build.
"""

from __future__ import annotations

from tothbot.exchange.position_mirror import PositionSide
from tothbot.execution.entry_dispatch import build_emergsl_order, build_entry_order

_DEADLINE = "2026-06-15T07:30:00Z"


# -- entry add_order ----------------------------------------------------

def test_long_entry_is_a_spot_buy_to_open():
    msg = build_entry_order(
        "BTC/USD", PositionSide.LONG,
        order_qty="0.05", entry_limit_price="60050", cl_ord_id="cl-1", deadline=_DEADLINE,
    )
    assert msg["method"] == "add_order"
    p = msg["params"]
    assert p["side"] == "buy"
    assert p["order_type"] == "limit"
    assert p["time_in_force"] == "ioc"
    assert p["order_qty"] == "0.05"
    assert p["limit_price"] == "60050"
    assert p["stp_type"] == "cancel_newest"
    assert p["deadline"] == _DEADLINE
    assert "margin" not in p          # a spot long carries no margin flag


def test_short_entry_is_a_margin_sell_to_open():
    msg = build_entry_order(
        "BTC/USD", PositionSide.SHORT,
        order_qty="0.05", entry_limit_price="59950", cl_ord_id="cl-2", deadline=_DEADLINE,
    )
    p = msg["params"]
    assert p["side"] == "sell"        # sell-to-open (ar:AR-009)
    assert p["margin"] is True        # Kraken spot-margin
    assert p["order_type"] == "limit"
    assert p["time_in_force"] == "ioc"


def test_entry_stringifies_numeric_fields_no_float():
    msg = build_entry_order(
        "ETH/USD", PositionSide.LONG,
        order_qty=2.0, entry_limit_price=3000.5, cl_ord_id="cl-3", deadline=_DEADLINE,
    )
    p = msg["params"]
    assert isinstance(p["order_qty"], str)
    assert isinstance(p["limit_price"], str)


# -- on-fill emergSL batch_add ------------------------------------------

def test_long_emergsl_is_a_sell_stop_below_entry():
    msg = build_emergsl_order(
        "BTC/USD", PositionSide.LONG,
        order_qty="0.05", emergsl_price="57000", cl_ord_id="cl-sl-1", deadline=_DEADLINE,
    )
    assert msg["method"] == "batch_add"
    assert msg["params"]["symbol"] == "BTC/USD"
    leg = msg["params"]["orders"][0]
    assert leg["side"] == "sell"               # close a long by selling
    assert leg["order_type"] == "stop-loss"
    assert leg["triggers"] == {"reference": "last", "price": "57000"}  # UT-EE-004
    assert "reduce_only" not in leg            # spot long SL carries NO reduce_only (margin-only flag)
    assert leg["stp_type"] == "cancel_newest"


def test_short_emergsl_is_a_buy_to_cover_reduce_only_above_entry():
    msg = build_emergsl_order(
        "BTC/USD", PositionSide.SHORT,
        order_qty="0.05", emergsl_price="63000", cl_ord_id="cl-sl-2", deadline=_DEADLINE,
    )
    leg = msg["params"]["orders"][0]
    assert leg["side"] == "buy"                 # buy-to-cover (ar:AR-009)
    assert leg["reduce_only"] is True           # mandatory on the short margin cover
    assert leg["order_type"] == "stop-loss"
    assert leg["triggers"] == {"reference": "last", "price": "63000"}  # UT-EE-004


def test_emergsl_single_leg():
    msg = build_emergsl_order(
        "BTC/USD", PositionSide.SHORT,
        order_qty="0.05", emergsl_price="63000", cl_ord_id="x", deadline=_DEADLINE,
    )
    assert len(msg["params"]["orders"]) == 1    # emergSL is a single leg (sec 7)


# -- the entry / emergSL sides are opposite (the close-out invariant) ----

def test_entry_and_emergsl_sides_are_opposite_each_direction():
    for side in (PositionSide.LONG, PositionSide.SHORT):
        entry = build_entry_order(
            "BTC/USD", side, order_qty="1", entry_limit_price="100", cl_ord_id="a", deadline=_DEADLINE,
        )
        sl = build_emergsl_order(
            "BTC/USD", side, order_qty="1", emergsl_price="90", cl_ord_id="b", deadline=_DEADLINE,
        )
        assert entry["params"]["side"] != sl["params"]["orders"][0]["side"]
