"""rule:Perp_Isolated_Margin_Loss_Cap - the crash-proof isolated-margin loss cap.

Source: 0500000 sec 13.1 ACCESS MODE + sec 13.5 MARGIN MODEL + sec 13.7 ISOLATED-MARGIN
LIQUIDATION (VALIDATED-structural by TB00806 battery C, 7/7) + Image10 rule:Perp_Isolated_
Margin_Loss_Cap. The exact margin/liquidation arithmetic mirrors the TB00806 oracle
(scripts/tb00806_perp_account_sim.py lines 72-176), the test oracle the live code must match.

Each perp pool runs in ISOLATED-margin mode: only the margin POSTED to a position is at risk,
and the EXCHANGE liquidates the position when that isolated margin is exhausted. The realized
POOL loss is therefore bounded by the posted margin BY CONSTRUCTION - even on a gap-through to
50%+, the overflow beyond the posted margin belongs to the EXCHANGE insurance fund, not to the
pool. This is the First-Principles property section 13.1 requires: a loss cap that is
EXCHANGE-resident, fires whether or not TothBot is alive, and sits UNDER the wide
layer:L2 = decision_atr_stop_mult x ATR stop as a TAIL backstop (battery C7 keeps leverage LOW
2-3x so liquidation never becomes the primary exit).

PURE compute, Decimal-only (ar:AR-047). The maintenance-margin ratio + the per-contract
multiplier are EXCHANGE-SET and NON-PUBLIC (section 13.1 confirm item 3); they are carried here
as a clearly-flagged PerpContractSpec the tests SWEEP across a plausible range. They MUST be
pinned from the Kraken/Bitnomial rulebook at code time, and the perps organism STAYS IN PAPER
until they are.

  margin_frac = 1 / leverage                 (fraction of notional posted as margin = the cap)
  liq_frac    = 1 / leverage - maint_margin_ratio   (adverse move that triggers liquidation)
  liq_price   = entry * (1 - liq_frac)  [LONG]  /  entry * (1 + liq_frac)  [SHORT]
  realized pool loss <= posted margin = margin_frac * notional  (BY CONSTRUCTION)
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from ..exchange.position_mirror import PositionSide

_ONE = Decimal("1")
_ZERO = Decimal("0")

# Centre defaults = the TB00806 battery-C centre (LEV0=3, MMR0=1%). FLAGGED + SWEPT: the real
# Kraken/Bitnomial maintenance-margin ratio + per-contract multiplier are NON-PUBLIC (section
# 13.1 item 3); these are placeholders the tests sweep. PIN from the rulebook at code time.
DEFAULT_LEVERAGE = Decimal("3")            # registry leverage_cap_short = 3 (REUSED; battery C 2-3x)
DEFAULT_MAINT_MARGIN_RATIO = Decimal("0.01")   # 1% centre (swept 0.5% / 1% / 2%) - PIN at code time
DEFAULT_CONTRACT_MULTIPLIER = Decimal("0.01")  # e.g. BTC perp = 0.01 BTC/contract (section 13.1 item 3)


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the margin model (ar:AR-047)."""
    return Decimal(str(value))


class MarginSourceStatus(Enum):
    """Provenance of the maintenance-margin ratio + contract multiplier (section 13.1 item 3)."""

    SWEPT_PLACEHOLDER = "swept_placeholder"  # NON-PUBLIC, swept by the tests; STAY IN PAPER
    PINNED_FROM_RULEBOOK = "pinned_from_rulebook"  # the real exchange specs (live-ready)


@dataclass(frozen=True)
class PerpContractSpec:
    """One perp product's margin/liquidation spec (section 13.1 item 3; section 13.5 MARGIN).

    leverage is bounded LOW (2-3x; registry leverage_cap_short=3 REUSED) so the liquidation
    price sits UNDER the wide layer:L2 ATR stop (battery C7). maint_margin_ratio +
    contract_multiplier are EXCHANGE-SET, NON-PUBLIC, and default to SWEPT placeholders -
    source must be PINNED_FROM_RULEBOOK before the perps organism leaves paper."""

    leverage: Decimal = DEFAULT_LEVERAGE
    maint_margin_ratio: Decimal = DEFAULT_MAINT_MARGIN_RATIO
    contract_multiplier: Decimal = DEFAULT_CONTRACT_MULTIPLIER
    source: MarginSourceStatus = MarginSourceStatus.SWEPT_PLACEHOLDER

    def __post_init__(self) -> None:
        object.__setattr__(self, "leverage", _dec(self.leverage))
        object.__setattr__(self, "maint_margin_ratio", _dec(self.maint_margin_ratio))
        object.__setattr__(self, "contract_multiplier", _dec(self.contract_multiplier))
        if self.leverage <= 0:
            raise ValueError(f"leverage must be > 0; got {self.leverage}")
        if self.maint_margin_ratio < 0:
            raise ValueError(f"maint_margin_ratio must be >= 0; got {self.maint_margin_ratio}")
        if self.contract_multiplier <= 0:
            raise ValueError(f"contract_multiplier must be > 0; got {self.contract_multiplier}")

    @property
    def margin_frac(self) -> Decimal:
        """Fraction of notional posted as isolated margin = 1 / leverage. This * notional is
        the POSTED MARGIN, which is the structural loss cap."""
        return _ONE / self.leverage

    @property
    def liq_frac(self) -> Decimal:
        """Adverse move (as a fraction of entry) that exhausts the isolated margin and triggers
        exchange liquidation = 1/leverage - maint_margin_ratio. Always < margin_frac (the mmr
        buffer the exchange keeps), so liquidation fires BEFORE the full margin is gone -
        realized loss is bounded by margin_frac regardless."""
        return _ONE / self.leverage - self.maint_margin_ratio

    @property
    def is_pinned(self) -> bool:
        """True iff the margin specs are the real rulebook values (live-ready, not a placeholder)."""
        return self.source is MarginSourceStatus.PINNED_FROM_RULEBOOK


@dataclass(frozen=True)
class PerpLiquidation:
    """The outcome of one isolated-margin loss-cap evaluation (rule:Perp_Isolated_Margin_Loss_Cap).

    realized_pool_loss is the $ the POOL loses - ALWAYS <= posted_margin BY CONSTRUCTION (the
    loss cap). exchange_absorbed is the gap-through overflow the EXCHANGE insurance fund eats,
    never the pool. liquidated names whether the isolated margin was exhausted."""

    liquidated: bool
    posted_margin: Decimal
    realized_pool_loss: Decimal
    exchange_absorbed: Decimal
    liquidation_price: Decimal
    adverse_frac: Decimal
    code: str = "PERP_ISOLATED_MARGIN_LOSS_CAP"


def posted_margin(notional: object, spec: PerpContractSpec) -> Decimal:
    """The isolated margin posted for a position of this notional = margin_frac * notional.
    This is the maximum the pool can lose on the position (the loss cap)."""
    return spec.margin_frac * _dec(notional)


def liquidation_price(entry_price: object, side: PositionSide, spec: PerpContractSpec) -> Decimal:
    """The isolated-margin liquidation price. LONG liquidates BELOW entry, SHORT ABOVE -
    entry * (1 -/+ liq_frac). Mirrors tb00806_perp_account_sim.liq_price."""
    entry = _dec(entry_price)
    if entry <= 0:
        raise ValueError(f"entry_price must be > 0; got {entry}")
    if side is PositionSide.SHORT:
        return entry * (_ONE + spec.liq_frac)
    return entry * (_ONE - spec.liq_frac)


def evaluate_loss_cap(
    *,
    entry_price: object,
    worst_price: object,
    notional: object,
    side: PositionSide,
    spec: PerpContractSpec,
) -> PerpLiquidation:
    """Evaluate the isolated-margin loss cap for one position taken to its worst adverse price.

    worst_price is the most-adverse mark the position saw (a gap-through low for a LONG / high
    for a SHORT). The realized POOL loss is CAPPED at the posted margin by construction:

      - if the adverse move reached liq_frac, the EXCHANGE liquidated the position and the pool
        loses exactly the posted margin (margin_frac * notional); any overflow beyond the posted
        margin (a gap-through past liquidation) is absorbed by the exchange insurance fund;
      - otherwise the position was NOT liquidated, and the loss is the actual adverse move
        (< liq_frac < margin_frac, so still under the cap).

    In EVERY case realized_pool_loss <= posted_margin. PURE; mirrors tb00806_perp_account_sim
    resolve_perp_trade's loss-cap branch (battery C, 7/7)."""
    entry = _dec(entry_price)
    if entry <= 0:
        raise ValueError(f"entry_price must be > 0; got {entry}")
    worst = _dec(worst_price)
    notion = _dec(notional)
    if notion < 0:
        raise ValueError(f"notional must be >= 0; got {notion}")

    # Adverse move as a positive fraction of entry (the direction depends on side).
    if side is PositionSide.SHORT:
        adverse_frac = (worst - entry) / entry  # SHORT loses when price RISES
    else:
        adverse_frac = (entry - worst) / entry  # LONG loses when price FALLS
    if adverse_frac < 0:
        adverse_frac = _ZERO  # a favourable move is no loss

    cap = posted_margin(notion, spec)  # margin_frac * notional
    liq_px = liquidation_price(entry, side, spec)
    liquidated = adverse_frac >= spec.liq_frac

    if liquidated:
        # Liquidated: the pool loses the full posted margin (capped); the exchange eats the rest.
        realized = cap
        gross_adverse = adverse_frac * notion
        exchange_absorbed = gross_adverse - cap
        if exchange_absorbed < 0:
            exchange_absorbed = _ZERO
    else:
        # Not liquidated: the realized loss is the actual adverse move, < liq_frac < margin_frac.
        realized = adverse_frac * notion
        exchange_absorbed = _ZERO

    # Defensive invariant: the loss cap is STRUCTURAL - realized can NEVER exceed posted margin.
    if realized > cap:
        raise AssertionError(
            f"loss-cap invariant breached: realized {realized} > posted_margin {cap} "
            "(rule:Perp_Isolated_Margin_Loss_Cap)"
        )

    return PerpLiquidation(
        liquidated=liquidated,
        posted_margin=cap,
        realized_pool_loss=realized,
        exchange_absorbed=exchange_absorbed,
        liquidation_price=liq_px,
        adverse_frac=adverse_frac,
    )


def per_contract_notional(mark_price: object, spec: PerpContractSpec) -> Decimal:
    """One contract's notional = contract_multiplier * mark_price (section 13.5 CONTRACT SIZING)."""
    return spec.contract_multiplier * _dec(mark_price)


def contracts_for_target(target_notional: object, mark_price: object, spec: PerpContractSpec) -> int:
    """WHOLE contracts for a fixed-notional target = floor(target / per_contract_notional)
    (section 13.5: fractional perps do not exist, so the realized notional is the nearest
    whole-contract value at or below the target). Returns an int count (>= 0)."""
    pcn = per_contract_notional(mark_price, spec)
    if pcn <= 0:
        raise ValueError(f"per_contract_notional must be > 0; got {pcn}")
    target = _dec(target_notional)
    if target < 0:
        raise ValueError(f"target_notional must be >= 0; got {target}")
    return int(target // pcn)


def realized_notional(contracts: int, mark_price: object, spec: PerpContractSpec) -> Decimal:
    """The realized whole-contract notional = contracts * per_contract_notional (section 13.5)."""
    if contracts < 0:
        raise ValueError(f"contracts must be >= 0; got {contracts}")
    return _dec(contracts) * per_contract_notional(mark_price, spec)
