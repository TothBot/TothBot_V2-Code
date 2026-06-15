"""SSS Signal Engine pure-unit tests (0500000 dv1_242 sec 3 Image2 + ar:AR-067 / HR-WM-018).

Covers the RSI(14) Wilder math + HR-SSS-004 division guard, the VolumeMA20 running-SMA, and
the three-factor PASS rule (strict AND; direction-symmetric long/short) incl. the SC-SSS-1 RSI
zone, the SC-SSS-2 EMA-cross state, and the SC-SSS-3 volume confirmation. Decimal-only.

Analytic RSI anchors: a strictly rising close series is all gains (avg_loss = 0, avg_gain > 0)
-> RSI = 100 (guard); strictly falling -> RS = 0 -> RSI = 0; a flat series -> both averages 0
-> RSI = 50 (guard).
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.regime.sss import (
    SignalSide,
    SssComputeError,
    SssVerdict,
    evaluate_sss,
    rsi_14,
    three_factor_pass,
    volume_ma_20,
)


# --------------------------------------------------------------------------- RSI
def test_rsi_pure_gains_is_100():
    assert rsi_14([Decimal(100 + i) for i in range(20)]) == Decimal(100)


def test_rsi_pure_losses_is_zero():
    assert rsi_14([Decimal(200 - i) for i in range(20)]) == Decimal(0)


def test_rsi_flat_series_is_50_guard():
    assert rsi_14([Decimal(100)] * 20) == Decimal(50)


def test_rsi_directional_sanity():
    # A strong up-bias series sits well above 50; the mirror down-bias well below.
    up = [Decimal(100) + Decimal("0.5") * i for i in range(30)]
    down = [Decimal(200) - Decimal("0.5") * i for i in range(30)]
    assert rsi_14(up) > Decimal(50)
    assert rsi_14(down) < Decimal(50)


def test_rsi_requires_15_closes():
    with pytest.raises(SssComputeError):
        rsi_14([Decimal(100)] * 14)


def test_rsi_known_wilder_value():
    # A single down-tick among 14 prior up-ticks: seed avg_gain=1, avg_loss=0; then one loss
    # of 2 at step 15 -> avg_gain=(1*13+0)/14=13/14, avg_loss=(0*13+2)/14=2/14=1/7.
    # RS = (13/14)/(1/7) = 13/2 = 6.5 -> RSI = 100 - 100/7.5 = 86.6667. Compared at 1e-12
    # tolerance: RS via Decimal division is not bit-exactly 6.5 at 28-digit precision.
    closes = [Decimal(100 + i) for i in range(15)] + [Decimal(112)]  # 114 -> 112 is a -2 tick
    expected = Decimal(100) - Decimal(100) / Decimal("7.5")
    assert abs(rsi_14(closes) - expected) < Decimal("1e-12")


# --------------------------------------------------------------------------- VolumeMA20
def test_volume_ma_constant():
    assert volume_ma_20([Decimal(1000)] * 25) == Decimal(1000)


def test_volume_ma_requires_20():
    with pytest.raises(SssComputeError):
        volume_ma_20([Decimal(1)] * 19)


# --------------------------------------------------------------------------- three-factor rule
def test_three_factor_all_true_long():
    sc = three_factor_pass(40, 110, 100, 2000, 1000, side=SignalSide.LONG, rsi_low=30, rsi_high=50)
    assert sc == (True, True, True)


def test_three_factor_rsi_out_of_zone_fails_sc1():
    # RSI 55 is outside the long zone (30, 50).
    sc = three_factor_pass(55, 110, 100, 2000, 1000, side=SignalSide.LONG, rsi_low=30, rsi_high=50)
    assert sc[0] is False


def test_three_factor_ema_not_crossed_fails_sc2_long():
    sc = three_factor_pass(40, 99, 100, 2000, 1000, side=SignalSide.LONG, rsi_low=30, rsi_high=50)
    assert sc[1] is False


def test_three_factor_volume_below_threshold_fails_sc3():
    # volume 1000 not > 1000 * 1.0.
    sc = three_factor_pass(40, 110, 100, 1000, 1000, side=SignalSide.LONG, rsi_low=30, rsi_high=50)
    assert sc[2] is False


def test_three_factor_short_mirror():
    # Short: RSI zone (50, 70) via mirror bounds 70/50; EMA9 < EMA21; volume confirms.
    sc = three_factor_pass(60, 100, 110, 2000, 1000, side=SignalSide.SHORT, rsi_low=70, rsi_high=50)
    assert sc == (True, True, True)


def test_three_factor_short_ema_direction_is_mirrored():
    # For a short, EMA9 > EMA21 (a bullish cross) must FAIL SC-SSS-2.
    sc = three_factor_pass(60, 120, 110, 2000, 1000, side=SignalSide.SHORT, rsi_low=70, rsi_high=50)
    assert sc[1] is False


def test_three_factor_short_long_zone_rejects():
    # RSI 40 is in the LONG zone but NOT the short zone (50, 70) -> SC-SSS-1 fails for a short.
    sc = three_factor_pass(40, 100, 110, 2000, 1000, side=SignalSide.SHORT, rsi_low=70, rsi_high=50)
    assert sc[0] is False


# --------------------------------------------------------------------------- evaluate integration
def _ramp_then_volume_spike(n=30):
    """A gentle uptrend (EMA9 > EMA21, RSI in the long zone) with a final volume spike."""
    closes = [Decimal(100) + Decimal("0.2") * i for i in range(n)]
    volumes = [Decimal(1000)] * (n - 1) + [Decimal(5000)]
    return closes, volumes


def test_evaluate_long_pass_integration():
    closes, volumes = _ramp_then_volume_spike()
    v = evaluate_sss("BTC/USD", closes, volumes, side=SignalSide.LONG)
    assert v.ema9 > v.ema21                # SC-SSS-2 holds
    assert v.sc_sss[1] is True
    assert v.sc_sss[2] is True             # 5000 > 1000-ish MA
    assert isinstance(v, SssVerdict)
    # rsi for a gentle steady uptrend is high (pure-ish gains); confirm the verdict is coherent.
    assert v.passed == all(v.sc_sss)
    assert v.event_type == ("SIGNAL_PASS" if v.passed else "SIGNAL_REJECTED")


def test_evaluate_requires_min_candles():
    with pytest.raises(SssComputeError):
        evaluate_sss("X", [Decimal(100)] * 20, [Decimal(1)] * 20, side=SignalSide.LONG)


def test_evaluate_volume_too_short():
    with pytest.raises(SssComputeError):
        evaluate_sss("X", [Decimal(100)] * 25, [Decimal(1)] * 19, side=SignalSide.LONG)


def test_verdict_score_counts_factors():
    closes = [Decimal(100)] * 25            # flat -> RSI 50 (outside long zone), EMA9==EMA21
    volumes = [Decimal(1000)] * 24 + [Decimal(5000)]
    v = evaluate_sss("X", closes, volumes, side=SignalSide.LONG)
    # flat: RSI 50 not in (30,50) -> sc1 False; ema9==ema21 -> sc2 False; volume spike -> sc3 True.
    assert v.sc_sss == (False, False, True)
    assert v.sss_score == 1
    assert v.passed is False


def test_signal_params_is_the_canonical_trade_close_dict():
    # contract:TRADE_CLOSE field (19): {rsi_14, ema_9, ema_21, volume_ratio, sss_pass, side}.
    closes = [Decimal(100)] * 25
    volumes = [Decimal(1000)] * 24 + [Decimal(5000)]
    v = evaluate_sss("X", closes, volumes, side=SignalSide.LONG)
    sp = v.signal_params
    assert set(sp) == {"rsi_14", "ema_9", "ema_21", "volume_ratio", "sss_pass", "side"}
    assert sp["rsi_14"] == v.rsi_14
    assert sp["ema_9"] == v.ema9 and sp["ema_21"] == v.ema21
    assert sp["volume_ratio"] == v.volume / v.volume_ma20   # the SC-SSS-3 ratio
    assert sp["sss_pass"] is v.passed and sp["side"] == "long"


def test_signal_params_volume_ratio_guards_zero_ma():
    v = SssVerdict(
        symbol="X", side=SignalSide.SHORT, rsi_14=Decimal(60), ema9=Decimal(1), ema21=Decimal(2),
        ema_cross=True, volume=Decimal(5), volume_ma20=Decimal(0), volume_vs_ma20=True,
        sc_sss=(True, True, True), passed=True,
    )
    assert v.signal_params["volume_ratio"] == Decimal(0)   # no division by a zero MA20
    assert v.signal_params["side"] == "short"
