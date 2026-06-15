"""gate:G6_Regime_Sizer - the per-regime size multiplier gate (0500000 Image2 G6).

Source: 0500000 dv1_250 sec 3 Image2 gate:G6_Regime_Sizer + ar:AR-074 (regime-based
sizing) + the six-regime size_multiplier policy already encoded in regime/taxonomy.py
(the D4 per-cell RegimeProfile.size_multiplier).

Gate 6 maps the pair's daily regime tag to a per-side size multiplier and emits a SIZED
candidate carrying that multiplier into gate:G7_Risk_Guard. It NEVER blocks (a sizer, not
a gate) - gate:G3_Regime_Filter has already filtered the permitted side, so the regime
reaching Gate 6 always permits this candidate's side.

The multiplier is a property of the regime CELL (identical for the long and short reads of
their own permitted regimes - the clean mirror, ar:AR-074):
  LONG  TRENDING_POS_NORMAL / TRENDING_POS_ELEVATED  -> 100% (REGIME_100PCT)
  SHORT TRENDING_NEG_NORMAL / TRENDING_NEG_ELEVATED  -> 100% (the mirror of long's
                                                        TRENDING_POS = 1.0)
  BOTH  NON_DIR_NORMAL                                -> 50%  (REGIME_50PCT), applied to
                                                        EACH permitted side independently
These are exactly regime/taxonomy.py RegimeProfile.size_multiplier (NON_DIR_NORMAL = 0.5,
the trending cells = 1.0), so this gate simply reads that single source of truth and scales
the per-pair base order size.

base_per_trade_size_usd is the per-pair base (the CIATS recipe param:per_trade_size_usd =
max($50 floor, 5x the pair's real minimum); the $50/pair-minimum base CIATS scales) - an
INPUT computed off the hot path, not a fixed scalar. Gate 6 multiplies it by the regime
factor. The SHORT base is margin-sized downstream at gate:G8_Position_Sizer / by
leverage_cap_short; Gate 6 only applies the regime factor, identically for both sides.

PURE compute (Decimal-only, ar:AR-047). Never blocks; always returns a G6Sized.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from ..exchange.position_mirror import PositionSide
from ..regime.taxonomy import Regime, profile

_HALF = Decimal("0.5")


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the sizer (AR-047)."""
    return Decimal(str(value))


@dataclass(frozen=True)
class G6Sized:
    """evt:G6_REGIME_SIZED [INFO] (Image2 G6 q5_logs) - the regime-sized candidate passed to
    Gate 7. Carries the regime multiplier + the scaled order size. marker is REGIME_50PCT
    (NON_DIR_NORMAL half size) or REGIME_100PCT (full). Never a skip (the gate never blocks)."""

    symbol: str
    side: PositionSide
    asset_regime: str                  # the regime token (Regime.value)
    regime_multiplier: Decimal         # 1.0 full | 0.5 half
    base_per_trade_size_usd: Decimal
    sized_usd: Decimal                 # base * regime_multiplier
    marker: str                        # "REGIME_50PCT" | "REGIME_100PCT"
    code: str = field(default="G6_REGIME_SIZED", init=False)


def size_regime(
    symbol: str,
    side: PositionSide,
    regime: Regime,
    base_per_trade_size_usd: object,
) -> G6Sized:
    """Apply the regime size multiplier to the per-pair base order size (Image2 G6, AR-074).

    Reads the multiplier from the single source of truth (regime/taxonomy.py
    RegimeProfile.size_multiplier for the candidate's regime cell) and scales the base. PURE,
    never blocks - returns a G6Sized for every candidate (Gate 3 already filtered the side)."""
    multiplier = profile(regime).size_multiplier
    base = _dec(base_per_trade_size_usd)
    sized = base * multiplier
    marker = "REGIME_50PCT" if multiplier == _HALF else "REGIME_100PCT"
    return G6Sized(
        symbol=symbol,
        side=side,
        asset_regime=regime.value,
        regime_multiplier=multiplier,
        base_per_trade_size_usd=base,
        sized_usd=sized,
        marker=marker,
    )
