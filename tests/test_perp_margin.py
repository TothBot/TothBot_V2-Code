"""Tests: rule:Perp_Isolated_Margin_Loss_Cap (tothbot/perp/margin.py).

Covers the isolated-margin loss cap (0500000 sec 13.7; TB00806 battery C, 7/7): margin/liq
arithmetic, the STRUCTURAL loss cap (realized pool loss <= posted margin by construction, even
on a gap-through), whole-contract sizing, and the NON-PUBLIC swept-spec flag (STAY IN PAPER).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.exchange.position_mirror import PositionSide
from tothbot.perp.margin import (
    DEFAULT_LEVERAGE,
    MarginSourceStatus,
    PerpContractSpec,
    contracts_for_target,
    evaluate_loss_cap,
    liquidation_price,
    per_contract_notional,
    posted_margin,
    realized_notional,
)

# Centre spec = the TB00806 battery-C centre (lev=3, mmr=1%).
_CENTRE = PerpContractSpec()


# -- spec arithmetic ----------------------------------------------------

def test_centre_spec_margin_and_liq_frac():
    # margin_frac = 1/3; liq_frac = 1/3 - 0.01.
    assert _CENTRE.margin_frac == Decimal("1") / Decimal("3")
    assert _CENTRE.liq_frac == Decimal("1") / Decimal("3") - Decimal("0.01")
    # liq_frac is always BELOW margin_frac (the mmr buffer) -> liquidation before margin gone.
    assert _CENTRE.liq_frac < _CENTRE.margin_frac


def test_default_leverage_is_three_the_reused_cap():
    assert DEFAULT_LEVERAGE == Decimal("3")  # registry leverage_cap_short REUSED


def test_no_float_enters_spec():
    spec = PerpContractSpec(leverage=2.0, maint_margin_ratio=0.02, contract_multiplier=0.5)
    assert isinstance(spec.leverage, Decimal)
    assert isinstance(spec.maint_margin_ratio, Decimal)
    assert spec.leverage == Decimal("2")


def test_spec_defaults_to_swept_placeholder_not_pinned():
    # The maint-margin ratio + multiplier are NON-PUBLIC (section 13.1 item 3) - the default is
    # a SWEPT placeholder, STAY IN PAPER until pinned from the rulebook.
    assert _CENTRE.source is MarginSourceStatus.SWEPT_PLACEHOLDER
    assert _CENTRE.is_pinned is False
    pinned = PerpContractSpec(source=MarginSourceStatus.PINNED_FROM_RULEBOOK)
    assert pinned.is_pinned is True


def test_spec_rejects_nonpositive_leverage_and_multiplier():
    with pytest.raises(ValueError):
        PerpContractSpec(leverage=0)
    with pytest.raises(ValueError):
        PerpContractSpec(contract_multiplier=0)
    with pytest.raises(ValueError):
        PerpContractSpec(maint_margin_ratio=-0.01)


# -- posted margin + liquidation price ----------------------------------

def test_posted_margin_is_margin_frac_times_notional():
    # $50 notional at 3x -> $16.67 posted margin (the loss cap).
    assert posted_margin(50, _CENTRE) == Decimal("50") * (Decimal("1") / Decimal("3"))


def test_liq_price_long_below_short_above():
    entry = Decimal("60000")
    long_liq = liquidation_price(entry, PositionSide.LONG, _CENTRE)
    short_liq = liquidation_price(entry, PositionSide.SHORT, _CENTRE)
    assert long_liq == entry * (Decimal("1") - _CENTRE.liq_frac)
    assert short_liq == entry * (Decimal("1") + _CENTRE.liq_frac)
    assert long_liq < entry < short_liq


def test_liq_price_rejects_nonpositive_entry():
    with pytest.raises(ValueError):
        liquidation_price(0, PositionSide.LONG, _CENTRE)


# -- THE LOSS CAP: realized pool loss <= posted margin (battery C core) --

def test_small_adverse_move_not_liquidated_loss_is_actual():
    # A 10% adverse move (< liq_frac ~32%) does NOT liquidate; loss is the actual move.
    out = evaluate_loss_cap(
        entry_price=60000, worst_price=54000, notional=50, side=PositionSide.LONG, spec=_CENTRE
    )
    assert out.liquidated is False
    assert out.realized_pool_loss == Decimal("0.10") * Decimal("50")
    assert out.exchange_absorbed == Decimal("0")
    assert out.realized_pool_loss <= out.posted_margin


@pytest.mark.parametrize("gap_frac", ["0.05", "0.10", "0.20", "0.50", "0.84", "0.99"])
def test_loss_cap_holds_under_any_gap_long(gap_frac):
    # C1/C4/C5: on a gap-through of ANY size, the realized POOL loss is bounded by posted margin.
    entry = Decimal("60000")
    worst = entry * (Decimal("1") - Decimal(gap_frac))
    out = evaluate_loss_cap(
        entry_price=entry, worst_price=worst, notional=50, side=PositionSide.LONG, spec=_CENTRE
    )
    assert out.realized_pool_loss <= out.posted_margin  # the structural cap, BY CONSTRUCTION


@pytest.mark.parametrize("gap_frac", ["0.05", "0.10", "0.20", "0.50", "0.99"])
def test_loss_cap_holds_under_any_gap_short(gap_frac):
    entry = Decimal("60000")
    worst = entry * (Decimal("1") + Decimal(gap_frac))  # SHORT loses when price RISES
    out = evaluate_loss_cap(
        entry_price=entry, worst_price=worst, notional=50, side=PositionSide.SHORT, spec=_CENTRE
    )
    assert out.realized_pool_loss <= out.posted_margin


def test_gap_through_caps_loss_and_exchange_absorbs_overflow():
    # 50% gap on a LONG: pool loses the full $16.67 margin; the exchange eats the rest.
    out = evaluate_loss_cap(
        entry_price=60000, worst_price=30000, notional=50, side=PositionSide.LONG, spec=_CENTRE
    )
    assert out.liquidated is True
    assert out.realized_pool_loss == posted_margin(50, _CENTRE)
    # gross adverse = 50% * $50 = $25; pool pays $16.67, exchange absorbs $8.33.
    assert out.exchange_absorbed == Decimal("0.50") * Decimal("50") - posted_margin(50, _CENTRE)


def test_favourable_move_is_no_loss():
    # Price moved in the position's favour -> adverse_frac floored at 0, no loss, no liq.
    out = evaluate_loss_cap(
        entry_price=60000, worst_price=66000, notional=50, side=PositionSide.LONG, spec=_CENTRE
    )
    assert out.liquidated is False
    assert out.realized_pool_loss == Decimal("0")
    assert out.adverse_frac == Decimal("0")


@pytest.mark.parametrize("lev,mmr", [("2", "0.005"), ("3", "0.01"), ("5", "0.02"), ("20", "0.01")])
def test_loss_cap_holds_across_swept_specs(lev, mmr):
    # C5: the cap is structural across the full swept margin grid (NON-PUBLIC specs).
    spec = PerpContractSpec(leverage=lev, maint_margin_ratio=mmr)
    out = evaluate_loss_cap(
        entry_price=60000, worst_price=100, notional=50, side=PositionSide.LONG, spec=spec
    )
    assert out.realized_pool_loss <= out.posted_margin
    assert out.realized_pool_loss == posted_margin(50, spec)  # liquidated -> full margin


# -- whole-contract sizing (section 13.5) -------------------------------

def test_per_contract_notional_is_multiplier_times_mark():
    spec = PerpContractSpec(contract_multiplier="0.01")
    assert per_contract_notional(60000, spec) == Decimal("0.01") * Decimal("60000")  # $600


def test_contracts_for_target_floors_to_whole_contracts():
    spec = PerpContractSpec(contract_multiplier="0.01")  # $600/contract at $60k mark
    # target $50 -> 0 whole contracts (1-contract notional $600 exceeds target).
    assert contracts_for_target(50, 60000, spec) == 0
    # target $2000 -> floor(2000/600) = 3 contracts.
    assert contracts_for_target(2000, 60000, spec) == 3


def test_realized_notional_is_whole_contract_value():
    spec = PerpContractSpec(contract_multiplier="0.01")
    assert realized_notional(3, 60000, spec) == Decimal("3") * Decimal("600")


def test_contracts_for_target_rejects_bad_inputs():
    spec = PerpContractSpec()
    with pytest.raises(ValueError):
        contracts_for_target(-1, 60000, spec)
