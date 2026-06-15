"""Tests: gate:G6_Regime_Sizer (pipeline/regime_sizer.py).

Covers 0500000 dv1_250 Image2 G6 + ar:AR-074: the per-regime size multiplier applied to
the per-pair base order size, the clean Long/Short mirror (TRENDING_POS for long ==
TRENDING_NEG for short == 100%; NON_DIR_NORMAL == 50% each side), and the never-blocks
contract. Composes regime/taxonomy.py size_multiplier. Decimal-only (AR-047).
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.position_mirror import PositionSide
from tothbot.pipeline.regime_sizer import G6Sized, size_regime
from tothbot.regime.taxonomy import Regime

_BASE = "50"


def test_long_trending_pos_normal_full_size():
    out = size_regime("BTC/USD", PositionSide.LONG, Regime.TRENDING_POS_NORMAL, _BASE)
    assert isinstance(out, G6Sized)
    assert out.regime_multiplier == Decimal("1.0")
    assert out.sized_usd == Decimal("50.0")
    assert out.marker == "REGIME_100PCT"
    assert out.asset_regime == "TRENDING_POS_NORMAL"
    assert out.code == "G6_REGIME_SIZED"


def test_long_trending_pos_elevated_full_size():
    out = size_regime("BTC/USD", PositionSide.LONG, Regime.TRENDING_POS_ELEVATED, _BASE)
    assert out.regime_multiplier == Decimal("1.0")
    assert out.marker == "REGIME_100PCT"


def test_short_trending_neg_normal_full_size_mirror():
    # SHORT's TRENDING_NEG = the clean mirror of LONG's TRENDING_POS = 1.0.
    out = size_regime("BTC/USD", PositionSide.SHORT, Regime.TRENDING_NEG_NORMAL, _BASE)
    assert out.regime_multiplier == Decimal("1.0")
    assert out.sized_usd == Decimal("50.0")
    assert out.marker == "REGIME_100PCT"
    assert out.side is PositionSide.SHORT


def test_short_trending_neg_elevated_full_size():
    out = size_regime("BTC/USD", PositionSide.SHORT, Regime.TRENDING_NEG_ELEVATED, _BASE)
    assert out.regime_multiplier == Decimal("1.0")
    assert out.marker == "REGIME_100PCT"


def test_non_dir_normal_half_size_long():
    out = size_regime("BTC/USD", PositionSide.LONG, Regime.NON_DIR_NORMAL, _BASE)
    assert out.regime_multiplier == Decimal("0.5")
    assert out.sized_usd == Decimal("25.0")
    assert out.marker == "REGIME_50PCT"


def test_non_dir_normal_half_size_short_independently():
    # NON_DIR_NORMAL admits BOTH sides at 50% each, applied independently.
    out = size_regime("BTC/USD", PositionSide.SHORT, Regime.NON_DIR_NORMAL, _BASE)
    assert out.regime_multiplier == Decimal("0.5")
    assert out.sized_usd == Decimal("25.0")
    assert out.marker == "REGIME_50PCT"


def test_long_pos_and_short_neg_are_equal_mirrors():
    lng = size_regime("BTC/USD", PositionSide.LONG, Regime.TRENDING_POS_NORMAL, _BASE)
    sht = size_regime("BTC/USD", PositionSide.SHORT, Regime.TRENDING_NEG_NORMAL, _BASE)
    assert lng.regime_multiplier == sht.regime_multiplier
    assert lng.sized_usd == sht.sized_usd


def test_never_blocks_returns_sized_for_every_call():
    # A sizer, not a gate - every permitted candidate gets a G6Sized (no skip path).
    for side, regime in [
        (PositionSide.LONG, Regime.TRENDING_POS_NORMAL),
        (PositionSide.SHORT, Regime.TRENDING_NEG_ELEVATED),
        (PositionSide.LONG, Regime.NON_DIR_NORMAL),
    ]:
        assert isinstance(size_regime("X", side, regime, _BASE), G6Sized)


def test_no_float_enters_the_sizer():
    out = size_regime("BTC/USD", PositionSide.LONG, Regime.NON_DIR_NORMAL, 50.0)
    assert out.base_per_trade_size_usd == Decimal("50.0")
    assert isinstance(out.sized_usd, Decimal)
