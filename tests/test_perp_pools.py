"""Tests: the three ring-fenced pools (tothbot/perp/pools.py).

Covers the three separately-funded pools (0500000 sec 13.7; Image10) and the RING-FENCE /
byte-isolation property (TB00806 battery C6): each pool holds its own isolated wallet, so
crashing one pool leaves the other two bit-for-bit unchanged.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.exchange.position_mirror import PositionSide
from tothbot.perp.margin import PerpContractSpec
from tothbot.perp.pools import (
    PerpPool,
    PoolEquityUpdated,
    PoolKind,
    ThreePoolWallet,
)


# -- pool identity (re-grounded sec 7 modules) --------------------------

def test_pool_kinds_carry_canonical_tokens_and_sides():
    assert PoolKind.LONG_SPOT.value == "mod:Long_Spot_Pool"
    assert PoolKind.LONG_PERP.value == "mod:Long_Perp_Pool"
    assert PoolKind.SHORT_PERP.value == "mod:Short_Perp_Pool"
    assert PerpPool(PoolKind.SHORT_PERP, deposit=5000).side is PositionSide.SHORT
    assert PerpPool(PoolKind.LONG_PERP, deposit=5000).side is PositionSide.LONG


def test_spot_pool_has_no_margin_spec():
    p = PerpPool(PoolKind.LONG_SPOT, deposit=5000)
    assert p.is_spot is True
    assert p.spec is None


def test_spot_pool_rejects_a_margin_spec():
    with pytest.raises(ValueError):
        PerpPool(PoolKind.LONG_SPOT, deposit=5000, spec=PerpContractSpec())


def test_perp_pool_defaults_to_centre_spec():
    p = PerpPool(PoolKind.SHORT_PERP, deposit=5000)
    assert p.spec is not None
    assert p.spec.leverage == Decimal("3")


# -- equity + drawdown ---------------------------------------------------

def test_init_seeds_equity_and_deposit():
    p = PerpPool(PoolKind.LONG_PERP, deposit=5000)
    assert p.equity == Decimal("5000")
    assert p.deposit == Decimal("5000")
    assert p.drawdown_from_deposit == Decimal("0")


def test_apply_realized_pnl_mutates_only_equity_not_deposit():
    p = PerpPool(PoolKind.SHORT_PERP, deposit=5000)
    p.apply_realized_pnl(Decimal("-16.67"))
    assert p.equity == Decimal("5000") - Decimal("16.67")
    assert p.deposit == Decimal("5000")  # frozen baseline UNCHANGED
    assert p.drawdown_from_deposit == Decimal("16.67") / Decimal("5000")


def test_apply_realized_pnl_emits_event():
    events: list = []
    p = PerpPool(PoolKind.SHORT_PERP, deposit=5000, on_event=events.append)
    p.apply_realized_pnl(Decimal("-16.67"), liquidated=True)
    assert len(events) == 1
    e = events[0]
    assert isinstance(e, PoolEquityUpdated)
    assert e.pool is PoolKind.SHORT_PERP
    assert e.liquidated is True
    assert e.code == "POOL_EQUITY_UPDATED"


def test_no_float_enters_pool_equity():
    p = PerpPool(PoolKind.LONG_PERP, deposit=5000.0)
    assert isinstance(p.equity, Decimal)
    p.apply_realized_pnl(12.5)
    assert p.equity == Decimal("5012.5")


# -- ThreePoolWallet + the RING-FENCE (battery C6) ----------------------

def test_three_pool_wallet_seeds_from_registry_defaults():
    w = ThreePoolWallet()
    assert w.long_spot.equity == Decimal("5000.0")
    assert w.long_perp.equity == Decimal("5000.0")
    assert w.short_perp.equity == Decimal("5000.0")
    assert w.total_equity == Decimal("15000.0")


def test_pools_tuple_is_canonical_order():
    w = ThreePoolWallet()
    assert [p.kind for p in w.pools] == [
        PoolKind.LONG_SPOT, PoolKind.LONG_PERP, PoolKind.SHORT_PERP,
    ]


def test_ring_fence_crashing_one_pool_leaves_others_byte_identical():
    # Battery C6: crash the Short-Perp pool (full margin loss) - the other two pools are
    # bit-for-bit unchanged (zero contagion, the structural ring-fence).
    w = ThreePoolWallet()
    before = w.snapshot()
    w.short_perp.apply_realized_pnl(Decimal("-5000"), liquidated=True)  # 100% crash
    after = w.snapshot()
    assert after[PoolKind.LONG_SPOT] == before[PoolKind.LONG_SPOT]
    assert after[PoolKind.LONG_PERP] == before[PoolKind.LONG_PERP]
    assert after[PoolKind.SHORT_PERP] != before[PoolKind.SHORT_PERP]


def test_ring_fence_crashing_long_perp_leaves_spot_and_short_identical():
    w = ThreePoolWallet()
    before = w.snapshot()
    w.long_perp.apply_realized_pnl(Decimal("-4950"), liquidated=True)  # 99% crash
    after = w.snapshot()
    assert after[PoolKind.LONG_SPOT] == before[PoolKind.LONG_SPOT]
    assert after[PoolKind.SHORT_PERP] == before[PoolKind.SHORT_PERP]


def test_independent_specs_per_perp_pool():
    w = ThreePoolWallet(
        long_perp_spec=PerpContractSpec(leverage=2),
        short_perp_spec=PerpContractSpec(leverage=3),
    )
    assert w.long_perp.spec.leverage == Decimal("2")
    assert w.short_perp.spec.leverage == Decimal("3")
