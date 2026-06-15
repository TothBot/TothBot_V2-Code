"""Tests: gate:G4_HTF_Confirmation (pipeline/htf_confirmation.py).

Covers 0500000 dv1_250 Image2 G4 + rule:HR-SP-006: the long 1H HTF test (daily EMA20 >
EMA50 AND 1H close > 1H EMA20), its exact short mirror, the NON_DIR_NORMAL bypass, and the
HTF_GATE_REJECTED skip. Decimal-only (AR-047).
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.position_mirror import PositionSide
from tothbot.pipeline.htf_confirmation import G4HtfRejected, confirm_htf
from tothbot.regime.taxonomy import Regime


# -- LONG (TRENDING_POS) -------------------------------------------------

def test_long_passes_on_bullish_alignment():
    out = confirm_htf(
        PositionSide.LONG, Regime.TRENDING_POS_NORMAL,
        ema20_daily="105", ema50_daily="100", close_1h="106", ema20_1h="104",
    )
    assert out.passed is True
    assert out.bypassed is False
    assert out.event is None


def test_long_rejected_when_daily_not_aligned():
    # daily EMA20 < EMA50 -> not bullish -> SKIP even though 1H close > 1H EMA20.
    out = confirm_htf(
        PositionSide.LONG, Regime.TRENDING_POS_NORMAL,
        ema20_daily="99", ema50_daily="100", close_1h="106", ema20_1h="104",
    )
    assert out.passed is False
    assert isinstance(out.event, G4HtfRejected)
    assert out.event.code == "HTF_GATE_REJECTED"
    assert out.event.side is PositionSide.LONG


def test_long_rejected_when_1h_close_below_ema20():
    out = confirm_htf(
        PositionSide.LONG, Regime.TRENDING_POS_NORMAL,
        ema20_daily="105", ema50_daily="100", close_1h="103", ema20_1h="104",
    )
    assert out.passed is False
    assert isinstance(out.event, G4HtfRejected)


# -- SHORT (TRENDING_NEG) - the exact mirror -----------------------------

def test_short_passes_on_bearish_alignment():
    out = confirm_htf(
        PositionSide.SHORT, Regime.TRENDING_NEG_NORMAL,
        ema20_daily="95", ema50_daily="100", close_1h="94", ema20_1h="96",
    )
    assert out.passed is True
    assert out.bypassed is False


def test_short_rejected_when_daily_not_bearish():
    # daily EMA20 > EMA50 (bullish) is NOT a short setup -> SKIP.
    out = confirm_htf(
        PositionSide.SHORT, Regime.TRENDING_NEG_NORMAL,
        ema20_daily="105", ema50_daily="100", close_1h="94", ema20_1h="96",
    )
    assert out.passed is False
    assert out.event.side is PositionSide.SHORT


def test_short_rejected_when_1h_close_above_ema20():
    out = confirm_htf(
        PositionSide.SHORT, Regime.TRENDING_NEG_NORMAL,
        ema20_daily="95", ema50_daily="100", close_1h="97", ema20_1h="96",
    )
    assert out.passed is False


def test_long_and_short_are_exact_mirrors():
    # Mirror the inputs across `entry`: long bullish setup <-> short bearish setup, same verdict.
    lng = confirm_htf(
        PositionSide.LONG, Regime.TRENDING_POS_NORMAL,
        ema20_daily="105", ema50_daily="100", close_1h="106", ema20_1h="104",
    )
    sht = confirm_htf(
        PositionSide.SHORT, Regime.TRENDING_NEG_NORMAL,
        ema20_daily="95", ema50_daily="100", close_1h="94", ema20_1h="96",
    )
    assert lng.passed is sht.passed is True


# -- NON_DIR_NORMAL bypass ----------------------------------------------

def test_non_dir_normal_bypasses_gate4():
    # A non-directional regime has no 1H trend to confirm -> BYPASS (passed, no test).
    for side in (PositionSide.LONG, PositionSide.SHORT):
        out = confirm_htf(
            side, Regime.NON_DIR_NORMAL,
            ema20_daily="100", ema50_daily="100", close_1h="100", ema20_1h="100",
        )
        assert out.passed is True
        assert out.bypassed is True
        assert out.event is None


# -- AR-047 --------------------------------------------------------------

def test_no_float_enters_the_gate():
    out = confirm_htf(
        PositionSide.LONG, Regime.TRENDING_POS_NORMAL,
        ema20_daily=105.0, ema50_daily=100.0, close_1h=106.0, ema20_1h=104.0,
    )
    assert out.passed is True
    out2 = confirm_htf(
        PositionSide.LONG, Regime.TRENDING_POS_NORMAL,
        ema20_daily=99.0, ema50_daily=100.0, close_1h=106.0, ema20_1h=104.0,
    )
    assert isinstance(out2.event.ema20_daily, Decimal)
