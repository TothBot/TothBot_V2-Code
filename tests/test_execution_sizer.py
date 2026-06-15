"""Tests: the Execution-Engine order sizing (execution/execution_sizer.py).

Covers 0500000 dv1_250 sec 7 + ar:AR-069: the MPP slippage cap (min of the empirical cap and
the sacred-floor slack, clamped at 0), the direction-symmetric marketable bound (long off the
ask, short off the bid), and order_qty = sized_usd / limit. Decimal-only (AR-047).
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.position_mirror import PositionSide
from tothbot.execution.execution_sizer import compute_entry_order


def test_long_caps_slippage_by_rr_headroom_and_sizes_qty():
    out = compute_entry_order(
        PositionSide.LONG,
        sized_usd="1000", best_bid="59990", best_ask="60000",
        expected_reward="0.05", net_loss="0.0302", mpp_abs_cap_pct="0.01",
    )
    # rr_headroom = 0.05 - 1.5*0.0302 = 0.0047 < 0.01 abs cap -> cap = 0.0047.
    assert out.mpp_cap_pct == Decimal("0.05") - Decimal("1.5") * Decimal("0.0302")
    assert out.mpp_cap_pct == Decimal("0.0047")
    # LONG crosses UP from the ASK.
    assert out.entry_limit_price == Decimal("60000") * (Decimal("1") + out.mpp_cap_pct)
    assert out.entry_limit_price > Decimal("60000")
    assert out.order_qty == Decimal("1000") / out.entry_limit_price


def test_short_uses_the_bid_and_crosses_down():
    out = compute_entry_order(
        PositionSide.SHORT,
        sized_usd="1000", best_bid="59990", best_ask="60000",
        expected_reward="0.05", net_loss="0.0304", mpp_abs_cap_pct="0.01",
    )
    assert out.mpp_cap_pct == Decimal("0.05") - Decimal("1.5") * Decimal("0.0304")  # 0.0044
    # SHORT crosses DOWN from the BID (receive less, the conservative bound).
    assert out.entry_limit_price == Decimal("59990") * (Decimal("1") - out.mpp_cap_pct)
    assert out.entry_limit_price < Decimal("59990")
    assert out.order_qty == Decimal("1000") / out.entry_limit_price


def test_cap_clamped_to_abs_cap_when_headroom_is_large():
    out = compute_entry_order(
        PositionSide.LONG,
        sized_usd="1000", best_bid="59990", best_ask="60000",
        expected_reward="0.20", net_loss="0.0302", mpp_abs_cap_pct="0.01",
    )
    # rr_headroom = 0.20 - 0.0453 = 0.1547 >> 0.01 -> cap = abs cap 0.01.
    assert out.mpp_cap_pct == Decimal("0.01")
    assert out.entry_limit_price == Decimal("60000") * Decimal("1.01")


def test_no_slippage_at_the_sacred_floor():
    # expected_reward exactly 1.5*net_loss -> rr_headroom 0 -> cap 0 -> limit = the marketable price.
    out = compute_entry_order(
        PositionSide.LONG,
        sized_usd="1000", best_bid="59990", best_ask="60000",
        expected_reward="0.0453", net_loss="0.0302", mpp_abs_cap_pct="0.01",
    )
    assert out.mpp_cap_pct == Decimal("0")
    assert out.entry_limit_price == Decimal("60000")


def test_cap_never_negative_below_floor_defensive():
    # below the floor (should not reach execution) rr_headroom < 0 -> cap clamped to 0.
    out = compute_entry_order(
        PositionSide.LONG,
        sized_usd="1000", best_bid="59990", best_ask="60000",
        expected_reward="0.01", net_loss="0.0302", mpp_abs_cap_pct="0.01",
    )
    assert out.mpp_cap_pct == Decimal("0")


def test_no_float_enters_the_sizer():
    out = compute_entry_order(
        PositionSide.LONG,
        sized_usd=1000.0, best_bid=59990.0, best_ask=60000.0,
        expected_reward=0.05, net_loss=0.0302, mpp_abs_cap_pct=0.01,
    )
    assert isinstance(out.order_qty, Decimal)
    assert isinstance(out.entry_limit_price, Decimal)
