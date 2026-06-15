"""Pure paper-exit detector tests (0500000 dv1_242 sec 12.5 step 1 + ar:AR-048 + sec 12.6).

detect_paper_exit is the ticker-bbo adverse-price check WS_Manager runs per open position:
ar:AR-048 L2 MAE (bid for longs / ask for shorts) with L2 priority, and the synthetic L3
emergSL touch (bid <= emergsl_price / ask >= emergsl_price) as the standalone backstop.
mae_mult = 1.5x (registry); entry 60000 -> L2 threshold = atr_14_entry * 1.5.
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.paper_exit import detect_paper_exit
from tothbot.exchange.position_mirror import Position, PositionSide


def _pos(side=PositionSide.LONG, entry="60000", atr="2000", emergsl=None):
    return Position(
        symbol="BTC/USD",
        side=side,
        qty=Decimal("0.05"),
        avg_entry_price=Decimal(entry),
        atr_14_entry=Decimal(atr) if atr is not None else None,
        emergsl_price=Decimal(emergsl) if emergsl is not None else None,
    )


def test_long_l2_mae_breach_uses_bid():
    # threshold = 2000*1.5 = 3000; bid 57000 -> mae 3000 >= 3000 -> breach at the bid.
    sig = detect_paper_exit(_pos(), bid="57000", ask="57100")
    assert sig is not None
    assert sig.exit_reason == "MAE_THRESHOLD_BREACH"
    assert sig.layer == "L2_MAE"
    assert sig.exit_price == Decimal("57000")
    assert sig.mae_pct == Decimal("3000") / Decimal("60000")


def test_long_no_breach_below_threshold():
    # bid 58000 -> mae 2000 < 3000, and no emergSL set -> no signal.
    assert detect_paper_exit(_pos(), bid="58000", ask="58100") is None


def test_long_uses_bid_not_ask_ar048():
    # ask is deep adverse but the long realizable price is the bid (no breach on bid).
    assert detect_paper_exit(_pos(), bid="58000", ask="40000") is None


def test_long_missing_bid_yields_no_signal():
    assert detect_paper_exit(_pos(), bid=None, ask="57000") is None


def test_long_l3_emergsl_touch_when_no_mae_context():
    # no atr snapshot -> L2 cannot evaluate; emergsl_price 54000, bid 53000 <= 54000 -> touch.
    sig = detect_paper_exit(_pos(atr=None, emergsl="54000"), bid="53000", ask="53100")
    assert sig is not None
    assert sig.exit_reason == "EMERGENCY_SL_FIRED"
    assert sig.layer == "L3_EMERGSL"
    assert sig.exit_price == Decimal("54000")


def test_long_l3_not_touched():
    assert detect_paper_exit(_pos(atr=None, emergsl="54000"), bid="55000", ask="55100") is None


def test_l2_takes_priority_over_l3():
    # both would fire (bid 53000: mae 7000 >= 3000 AND <= emergsl 54000); L2 wins (Image3:
    # emergSL never fires normally because L1a/L2 close first).
    sig = detect_paper_exit(_pos(atr="2000", emergsl="54000"), bid="53000", ask="53100")
    assert sig.exit_reason == "MAE_THRESHOLD_BREACH"
    assert sig.exit_price == Decimal("53000")


def test_short_l2_mae_breach_uses_ask():
    # short: mae = ask - entry; ask 63000 -> 3000 >= 3000 -> breach at the ask.
    sig = detect_paper_exit(_pos(side=PositionSide.SHORT), bid="62900", ask="63000")
    assert sig is not None
    assert sig.exit_reason == "MAE_THRESHOLD_BREACH"
    assert sig.exit_price == Decimal("63000")


def test_short_l3_emergsl_touch_above():
    # short emergSL sits above entry; ask 67000 >= emergsl 66000 -> touch.
    sig = detect_paper_exit(
        _pos(side=PositionSide.SHORT, atr=None, emergsl="66000"), bid="65900", ask="67000"
    )
    assert sig.exit_reason == "EMERGENCY_SL_FIRED"
    assert sig.exit_price == Decimal("66000")


def test_short_uses_ask_not_bid():
    assert detect_paper_exit(
        _pos(side=PositionSide.SHORT), bid="80000", ask="61000"
    ) is None  # ask 61000 -> mae 1000 < 3000
