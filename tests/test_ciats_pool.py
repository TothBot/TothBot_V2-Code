"""Tests: mod:CIATS per-module pool (ciats/pool.py).

Covers 0500000 dv1_250 sec 6/7 + ar:AR-065: the trade-outcome accumulator, the 200-trade hard
floor gate, the realized win-rate / net-R:R, and the Half-Kelly fraction (net P/L, clamped).
Per-module independence (no cross-pooling). Decimal-only (AR-047).
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.ciats.pool import CIATS_TRADE_FLOOR, CiatsPool


def _fill(pool, wins, losses, gain="150", loss="100"):
    for _ in range(wins):
        pool.ingest_outcome(net_pl=gain, net_gain=gain, net_loss="0")
    for _ in range(losses):
        pool.ingest_outcome(net_pl="-" + loss, net_gain="0", net_loss=loss)


# -- the 200-trade floor ------------------------------------------------

def test_floor_is_200():
    assert CIATS_TRADE_FLOOR == 200


def test_below_floor_is_not_ready_and_half_kelly_none():
    p = CiatsPool()
    _fill(p, wins=100, losses=99)            # 199 < 200
    assert p.trade_count == 199
    assert p.ready is False
    assert p.half_kelly_fraction() is None    # seed sizing stands below the floor


def test_at_floor_is_ready():
    p = CiatsPool()
    _fill(p, wins=100, losses=100)            # exactly 200
    assert p.ready is True


# -- realized statistics ------------------------------------------------

def test_win_rate_and_net_reward_risk():
    p = CiatsPool()
    _fill(p, wins=100, losses=100, gain="150", loss="100")
    assert p.win_rate == Decimal("0.5")
    # R = avg(net_gain)/avg(net_loss) = 150/100 = 1.5.
    assert p.net_reward_risk == Decimal("1.5")


def test_net_reward_risk_none_without_both_legs():
    p = CiatsPool()
    _fill(p, wins=200, losses=0)             # all wins -> no loss leg
    assert p.net_reward_risk is None
    assert p.half_kelly_fraction() is None    # cannot size Kelly without R


# -- Half-Kelly (ar:AR-065, net P/L) ------------------------------------

def test_half_kelly_formula_w_half_r_one_point_five():
    p = CiatsPool()
    _fill(p, wins=100, losses=100, gain="150", loss="100")
    w, r = p.win_rate, p.net_reward_risk
    # f* = W - (1-W)/R = 0.5 - 0.5/1.5; half = f*/2.
    expected = (w - (Decimal("1") - w) / r) / Decimal("2")
    assert p.half_kelly_fraction() == expected
    assert p.half_kelly_fraction() > Decimal("0")     # a positive edge -> a positive size


def test_half_kelly_clamps_to_zero_on_negative_edge():
    # W = 0.2, R = 1.5 -> f* = 0.2 - 0.8/1.5 < 0 -> clamp to 0 (never a negative size).
    p = CiatsPool()
    _fill(p, wins=40, losses=160, gain="150", loss="100")
    assert p.win_rate == Decimal("0.2")
    assert p.half_kelly_fraction() == Decimal("0")


def test_half_kelly_never_exceeds_one():
    # An extreme edge (high W, huge R) clamps to 1 (the wallet is the sole hard bound).
    p = CiatsPool()
    _fill(p, wins=199, losses=1, gain="100000", loss="1")
    assert p.half_kelly_fraction() <= Decimal("1")


# -- per-module independence --------------------------------------------

def test_two_pools_are_independent():
    longp = CiatsPool()
    shortp = CiatsPool()
    _fill(longp, wins=100, losses=100)
    assert longp.ready is True
    assert shortp.trade_count == 0 and shortp.ready is False   # no cross-module pooling


# -- AR-047 -------------------------------------------------------------

def test_no_float_enters_the_pool():
    p = CiatsPool()
    p.ingest_outcome(net_pl=150.0, net_gain=150.0, net_loss=0.0)
    assert p.trade_count == 1
    assert p.win_rate == Decimal("1")
    assert isinstance(p.win_rate, Decimal)
