"""mod:Perp_Funding_Divergence_Monitor + the 8h funding model (FCM Variation Margin).

Source: 0500000 sec 13.5 FUNDING MODEL + sec 13.8 battery B (funding stress) + sec 13.9
FUNDING MONITOR + Image10 mod:Perp_Funding_Divergence_Monitor + TB00000 v2_103 sec 8
perp_funding_divergence_monitor. Mirrors the TB00806 oracle (scripts/tb00806b_funding_stress.py
+ tb00806d_whole_organism.py W2, lines 256-294), the test oracle the live code must match.

Bitnomial perps settle funding every 8h, FOLDED into Variation Margin (NOT a separate spot
borrow). The funding RATE is EXCHANGE-SET (not a TothBot parameter; the 8h cadence is an
engineering constant). The realized funding COST folds into perp net_loss exactly as the
spot-short margin_borrow_fee does, so the sacred 1:1.5 net (after-fee) R:R floor absorbs it
AUTOMATICALLY and is NEVER lowered.

Long vs Short - the natural sign difference (equal detail): a LONG PAYS funding when the rate
is POSITIVE and RECEIVES it when negative; a SHORT is the mirror. Battery B found funding is
SECOND-ORDER + de-risked (the short COLLECTS funding on average), with ONE honest sensitivity:
funding pinned HARD-ADVERSE and SUSTAINED erases the thin short edge (break-even ~0.121%/day,
above typical ~4x but below the exchange clamp ceiling 0.225%/day). That sensitivity is covered
by the funding-divergence monitor below.

THE MONITOR is ONE new instance of the live ciats.EwmaMonitor (the exact fee_tier_divergence
machinery) - NOT a new module. It is SIGNALS-ONLY (TB00000 D5: monitors + alerts, never
self-adjusts; routes to the CIATS PDCA cycle + Bill HR-CI-011), so it CANNOT deadlock. It is
fed the per-trade ADVERSE funding cost (the >0 component, the side that PAYS) and fires
`.sustained` after sustained_n consecutive adverse observations.

PURE compute, Decimal-only (ar:AR-047).
"""

from __future__ import annotations

from decimal import Decimal

from ..ciats.ewma_monitor import EwmaMonitor
from ..config import registry
from ..exchange.position_mirror import PositionSide

_ZERO = Decimal("0")
_EWMA_LAMBDA = Decimal("0.2")  # the live fee_tier_divergence lambda (ciats.EwmaMonitor default)

# Funding-rate clamp reference (TB00799 item 3; section 13.6 NOT-SEEDS): the EXCHANGE-SET rate
# FR = avg(Premium) + clamp(InterestRate - avg(Premium), -clamp, +clamp), IR fixed 0.01%.
# These are exchange constants, NOT TothBot tunables - listed for the funding model, not seeds.
INTEREST_RATE = Decimal("0.0001")        # 0.01% fixed interest-rate component (per 8h)
FUNDING_CLAMP = Decimal("0.0005")        # +/- 0.05% clamp band on (IR - avg_premium)


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the funding model (ar:AR-047)."""
    return Decimal(str(value))


def funding_rate(avg_premium: object) -> Decimal:
    """The exchange-set 8h funding rate FR = avg(Premium) + clamp(IR - avg(Premium), +/-clamp).
    EXCHANGE-SET (not tunable); modelled here so the funding cost can fold into net P&L."""
    prem = _dec(avg_premium)
    delta = INTEREST_RATE - prem
    if delta > FUNDING_CLAMP:
        delta = FUNDING_CLAMP
    elif delta < -FUNDING_CLAMP:
        delta = -FUNDING_CLAMP
    return prem + delta


def funding_cost(cumulative_rate: object, side: PositionSide) -> Decimal:
    """The funding COST to a side over a hold, as a fraction of notional (positive = the side
    PAYS, negative = the side RECEIVES a credit). LONG cost = +cumulative_rate, SHORT is the
    mirror = -cumulative_rate. Mirrors tb00806b's signed funding (`fund = fr if LONG else -fr`,
    accounted as a cost). The realized $ cost = this * notional folds into perp net_loss."""
    rate = _dec(cumulative_rate)
    return rate if side is PositionSide.LONG else -rate


def adverse_funding_per_period(cumulative_rate: object, side: PositionSide, periods: object) -> Decimal:
    """The per-period ADVERSE funding cost (the >0 component the monitor watches): the side's
    funding cost spread over the hold's funding periods, floored at 0 (a credit is not adverse).
    Mirrors tb00806d W2's `max(0.0, -fr/days)` fed to the EwmaMonitor."""
    n = _dec(periods)
    if n <= 0:
        raise ValueError(f"periods must be > 0; got {n}")
    per_period = funding_cost(cumulative_rate, side) / n
    return per_period if per_period > _ZERO else _ZERO


def make_funding_divergence_monitor(
    *,
    threshold: object | None = None,
    sustained_n: int | None = None,
) -> EwmaMonitor:
    """Construct the perp funding-divergence monitor - ONE new instance of the live EwmaMonitor
    (mod:Perp_Funding_Divergence_Monitor, section 13.9). Baseline 0 (zero adverse = neutral),
    threshold = perp_funding_divergence_monitor seed (0.05%/day), sustained_n = the
    fee_tier_divergence sustained-count (50), lambda = 0.2 (the live fee_tier_divergence value).

    SIGNALS-ONLY: feed it adverse_funding_per_period each closed perp trade via .update(), read
    .sustained as the fire condition (route to CIATS PDCA + Bill HR-CI-011). It NEVER adjusts a
    parameter or pauses a pool, so it CANNOT deadlock (TB00000 D5)."""
    thr = registry.value("perp_funding_divergence_monitor") if threshold is None else threshold
    n = (
        int(registry.value("fee_tier_divergence_sustained_trades"))
        if sustained_n is None
        else int(sustained_n)
    )
    return EwmaMonitor(
        lambda_=_EWMA_LAMBDA,
        baseline=_ZERO,
        threshold=_dec(thr),
        sustained_n=n,
    )
