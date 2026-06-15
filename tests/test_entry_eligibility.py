"""Tests: the pre-signal entry-eligibility gates (pipeline/entry_eligibility.py).

Covers 0500000 dv1_250 Image2 Pre-Gate-1 (per-pair status + per-side universe partition),
Gate-1 (WS state machine readiness), Gate-2 (24h USD-volume floor). Decimal-only (AR-047).
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.position_mirror import PositionSide
from tothbot.pipeline.entry_eligibility import (
    PreGate1Disposition,
    check_liquidity,
    check_pair_status,
    check_state_machine,
)


# -- Pre-Gate-1: status + per-side universe partition -------------------

def test_online_long_passes():
    d = check_pair_status(PositionSide.LONG, "online", marginable=False)
    assert d.passed is True
    assert d.disposition is PreGate1Disposition.PASS
    assert d.code == "PRE_GATE_1_DECISION"


def test_online_marginable_short_passes():
    d = check_pair_status(PositionSide.SHORT, "online", marginable=True)
    assert d.passed is True
    assert d.disposition is PreGate1Disposition.PASS


def test_online_non_marginable_short_is_short_ineligible():
    # LONG passes on the same pair; SHORT cannot (shorts trade Kraken margin, AR-009).
    assert check_pair_status(PositionSide.LONG, "online", marginable=False).passed is True
    d = check_pair_status(PositionSide.SHORT, "online", marginable=False)
    assert d.passed is False
    assert d.disposition is PreGate1Disposition.SHORT_INELIGIBLE


def test_non_online_status_blocks_both_sides():
    for status in ("post_only", "cancel_only", "maintenance", "limit_only", "delisted"):
        for side in (PositionSide.LONG, PositionSide.SHORT):
            d = check_pair_status(side, status, marginable=True)
            assert d.passed is False
            assert d.disposition is PreGate1Disposition.INSTRUMENT_STATUS_BLOCKED


# -- Gate-1: WS state machine -------------------------------------------

def test_subscribed_passes():
    d = check_state_machine("Subscribed")
    assert d.passed is True
    assert d.rejection_code is None


def test_non_subscribed_waits():
    for state in ("Idle", "Subscribing", "Resyncing", "Cooldown"):
        d = check_state_machine(state)
        assert d.passed is False
        assert d.rejection_code == "SYSTEM_STATE_BLOCKED"


# -- Gate-2: liquidity floor --------------------------------------------

def test_liquidity_above_floor_passes():
    d = check_liquidity("600000")  # > 500k seed
    assert d.passed is True
    assert d.floor_usd == Decimal("500000")


def test_liquidity_below_floor_rejected():
    d = check_liquidity("400000")
    assert d.passed is False


def test_liquidity_inclusive_at_floor():
    d = check_liquidity("500000")  # == floor -> PASS
    assert d.passed is True


def test_liquidity_floor_override():
    d = check_liquidity("600000", min_volume_usd_daily="1000000")  # 600k < 1M
    assert d.passed is False


def test_no_float_enters_the_liquidity_gate():
    d = check_liquidity(600000.0)
    assert d.vol_24h_usd == Decimal("600000.0")
    assert isinstance(d.vol_24h_usd, Decimal)
