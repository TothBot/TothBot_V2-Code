"""Six-regime taxonomy - the 3x2 grid tokens and their per-cell entry policy.

Source: 0500000 dv1_242 sec 5 + Image4 R9 (Regime Taxonomy, 6-regime 3x2 grid). The
directional axis (ADX + EMA20/EMA50) crosses the volatility axis (ATR percentile) to give
six regime: tokens; each cell carries the canonical gate:G3_Regime_Filter / gate:G6_Regime_
Sizer policy the Signal_Pipeline applies per side:

  REGIME 1  TRENDING_POS_NORMAL     LONG entry, full size (1.0)            G3 ALLOW
  REGIME 2  TRENDING_POS_ELEVATED   LONG entry, full size (1.0)            G3 ALLOW (ATR adapts)
  REGIME 3  NON_DIR_NORMAL          LONG entry, HALF size (0.5)            G6 reduce
  REGIME 4  NON_DIR_ELEVATED        NO entry (whipsaw)                     G3 BLOCK  (HR-REGIME-008)
  REGIME 5  TRENDING_NEG_NORMAL     LONG blocked, SHORT permitted          directional cascade
  REGIME 6  TRENDING_NEG_ELEVATED   LONG blocked, SHORT permitted          directional cascade

The directional cascade (regimes 5/6) is rule:HR-REGIME-007 (ar:AR-072 / ar:AR-073): a
confirmed downtrend blocks LONG (directionally incorrect) and routes SHORT via mod:Short_
Module. NON_DIR_ELEVATED blocks ALL entry unconditionally (rule:HR-REGIME-008). This module
is PURE policy data straight off D4; the Signal_Pipeline slice composes it with the SSS
signal at the gates.

DIRECTIONAL TIE NOTE: Image4 writes the directional split with strict inequalities
(POS = EMA20 > EMA50; NEG = EMA20 < EMA50) and defines NON_DIRECTIONAL solely by
ADX <= threshold. An exact EMA20 == EMA50 tie under ADX > threshold is a measure-zero
Decimal coincidence the figure's strict inequalities do not enumerate; classify() resolves
it to TRENDING_NEGATIVE - the loss-minimizing read (it blocks the LONG via the cascade
rather than admitting a long into a non-rising HTF). Carried as a cosmetic observation; not
a value invention (no new threshold), only a boundary completion.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum


class DirectionalState(Enum):
    """ADX(14) + EMA20/EMA50 directional axis (Image4 row headers)."""

    TRENDING_POSITIVE = "TRENDING_POSITIVE"   # ADX > thresh AND EMA20 > EMA50
    NON_DIRECTIONAL = "NON_DIRECTIONAL"       # ADX <= thresh (EMA relationship not evaluated)
    TRENDING_NEGATIVE = "TRENDING_NEGATIVE"   # ADX > thresh AND EMA20 < EMA50


class VolatilityState(Enum):
    """ATR(14) percentile volatility axis (Image4 column headers)."""

    NORMAL_VOL = "NORMAL_VOL"       # ATR percentile <= atr_percentile_thresh (67th)
    ELEVATED_VOL = "ELEVATED_VOL"   # ATR percentile > atr_percentile_thresh


class Regime(Enum):
    """The six regime: tokens (asset_regime / market_regime). Value == the canonical token."""

    TRENDING_POS_NORMAL = "TRENDING_POS_NORMAL"
    TRENDING_POS_ELEVATED = "TRENDING_POS_ELEVATED"
    NON_DIR_NORMAL = "NON_DIR_NORMAL"
    NON_DIR_ELEVATED = "NON_DIR_ELEVATED"
    TRENDING_NEG_NORMAL = "TRENDING_NEG_NORMAL"
    TRENDING_NEG_ELEVATED = "TRENDING_NEG_ELEVATED"


@dataclass(frozen=True)
class RegimeProfile:
    """The canonical D4 gate policy for one regime cell.

    long_entry_permitted / short_entry_permitted are the gate:G3_Regime_Filter decisions per
    side; size_multiplier is the gate:G6_Regime_Sizer factor applied to a permitted entry
    (1.0 = full, 0.5 = half). cascade is True for the HR-REGIME-007 LONG-block / SHORT-route
    downtrend cells. The Long and Short modules read their own side's permission.
    """

    regime: "Regime"
    directional: DirectionalState
    volatility: VolatilityState
    long_entry_permitted: bool
    short_entry_permitted: bool
    size_multiplier: Decimal
    cascade: bool = False

    def entry_permitted(self, *, is_long: bool) -> bool:
        """Whether gate:G3_Regime_Filter ALLOWs an entry for the given side in this regime."""
        return self.long_entry_permitted if is_long else self.short_entry_permitted


_FULL = Decimal("1.0")
_HALF = Decimal("0.5")

# The canonical per-cell policy table (Image4 regime cells 1-6).
PROFILES: dict[Regime, RegimeProfile] = {
    Regime.TRENDING_POS_NORMAL: RegimeProfile(
        Regime.TRENDING_POS_NORMAL, DirectionalState.TRENDING_POSITIVE,
        VolatilityState.NORMAL_VOL, True, False, _FULL,
    ),
    Regime.TRENDING_POS_ELEVATED: RegimeProfile(
        Regime.TRENDING_POS_ELEVATED, DirectionalState.TRENDING_POSITIVE,
        VolatilityState.ELEVATED_VOL, True, False, _FULL,
    ),
    Regime.NON_DIR_NORMAL: RegimeProfile(
        # Bill ruling DEC-B SYMMETRIC (0500000 Image4 R10): a non-directional NORMAL-vol
        # market admits BOTH sides at HALF size (the full mirror of Long) - long_entry +
        # short_entry both permitted, size_multiplier 0.5 each. No directional cascade.
        Regime.NON_DIR_NORMAL, DirectionalState.NON_DIRECTIONAL,
        VolatilityState.NORMAL_VOL, True, True, _HALF,
    ),
    Regime.NON_DIR_ELEVATED: RegimeProfile(
        Regime.NON_DIR_ELEVATED, DirectionalState.NON_DIRECTIONAL,
        VolatilityState.ELEVATED_VOL, False, False, _FULL,
    ),
    Regime.TRENDING_NEG_NORMAL: RegimeProfile(
        Regime.TRENDING_NEG_NORMAL, DirectionalState.TRENDING_NEGATIVE,
        VolatilityState.NORMAL_VOL, False, True, _FULL, cascade=True,
    ),
    Regime.TRENDING_NEG_ELEVATED: RegimeProfile(
        Regime.TRENDING_NEG_ELEVATED, DirectionalState.TRENDING_NEGATIVE,
        VolatilityState.ELEVATED_VOL, False, True, _FULL, cascade=True,
    ),
}

# (directional, volatility) -> Regime, the 3x2 grid lookup.
_GRID: dict[tuple[DirectionalState, VolatilityState], Regime] = {
    (p.directional, p.volatility): p.regime for p in PROFILES.values()
}


def classify_directional(
    adx: Decimal, ema20: Decimal, ema50: Decimal, adx_threshold: object
) -> DirectionalState:
    """The Image4 directional axis: ADX > threshold splits into POSITIVE (EMA20 > EMA50) or
    NEGATIVE; ADX <= threshold is NON_DIRECTIONAL. An EMA tie under trend resolves NEGATIVE
    (see the module DIRECTIONAL TIE NOTE)."""
    threshold = adx_threshold if isinstance(adx_threshold, Decimal) else Decimal(str(adx_threshold))
    if adx <= threshold:
        return DirectionalState.NON_DIRECTIONAL
    return (
        DirectionalState.TRENDING_POSITIVE
        if ema20 > ema50
        else DirectionalState.TRENDING_NEGATIVE
    )


def classify_volatility(atr_percentile: Decimal, atr_percentile_thresh: object) -> VolatilityState:
    """The Image4 volatility axis: percentile > threshold is ELEVATED_VOL else NORMAL_VOL."""
    threshold = (
        atr_percentile_thresh
        if isinstance(atr_percentile_thresh, Decimal)
        else Decimal(str(atr_percentile_thresh))
    )
    return VolatilityState.ELEVATED_VOL if atr_percentile > threshold else VolatilityState.NORMAL_VOL


def classify(directional: DirectionalState, volatility: VolatilityState) -> Regime:
    """Map a (directional, volatility) pair to its regime cell (the 3x2 grid)."""
    return _GRID[(directional, volatility)]


def profile(regime: Regime) -> RegimeProfile:
    """The canonical gate policy for a regime cell."""
    return PROFILES[regime]
