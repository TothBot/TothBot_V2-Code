"""Paper exit detection - the ticker-bbo adverse-price detector (sec 12.5 step 1).

Source: 0500000 dv1_242 sec 12.5 (WSManager detects the exit condition) + sec 3 Image3
(layer:L2_MAE_Threshold + layer:L3_Emergency_SL) + sec 12.6 (evt:PAPER_EMERG_SL_TRIGGERED,
bid <= emergsl_price) + ar:AR-048 (Exit Controller MAE detection MUST use bid for longs /
ask for shorts) + the D1 WS-TKR-002/003 ticker event_trigger:bbo facts.

This is the PURE detector mod:WS_Manager runs on every ticker bbo frame for each open
position (the realizable-exit-price adverse-move check). It decides WHICH exit condition
fired; the close itself routes through mod:Exit_Controller (exit_controller.py) per the
sec-12.5 close sequence. No socket, no asyncio, no state - one position + one bbo tick in,
one exit signal (or None) out.

ar:AR-048 MAE (direction-symmetric; bid/ask, never last):
    long_mae  = entry_fill_price - bid   (a long is sold at the bid; detects a DROP)
    short_mae = ask - entry_fill_price   (a short is covered at the ask; detects a RISE)
  layer:L2_MAE_Threshold fires when the MAE >= atr_14_entry * param:mae_mult (1.5x). The
  ATR(14) is the entry-time snapshot on the PositionRecord (D6, dv1_242), never live-
  recomputed.

layer:L3_Emergency_SL synthetic touch (paper): the off-book emergSL is a resting stop at
emergsl_price (entry-time snapshot, D6). In paper it has no real matching engine, so the
ticker bbo simulates the touch: a long fills when bid <= emergsl_price, a short when
ask >= emergsl_price (evt:PAPER_EMERG_SL_TRIGGERED, sec 12.6).

PRECEDENCE (faithful to Image3: "L3 SHOULD NEVER FIRE in normal operation because L1a/L2
close positions first"). The L2 MAE threshold (1.5x ATR) sits INSIDE the emergSL distance
(3.0x ATR), so whenever the emergSL would touch, the MAE has already breached - L2 is
checked first and wins. The emergSL branch is therefore the standalone BACKSTOP, reached
only when the position carries an emergsl_price but no atr_14_entry (no MAE context to
evaluate). Both branches route through the same close path; only the exit_reason differs.

NOTE: layer:L1a regime-reversal exits (HTF_REGIME_REVERSAL / DAILY_REGIME_DOWNGRADE) are
NOT detected here - they are driven by mod:Regime_Engine (daily classification, S3), not by
the ticker bbo. This detector covers the two bbo-price-driven paper exits (L2 + the L3
synthetic touch).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from ..config import registry
from .position_mirror import Position, PositionSide

# param:mae_mult (CIATS-owned, 1.5x starting; value home TB00000 sec 8) as Decimal once
# (ar:AR-047). The L2 MAE threshold = atr_14_entry * mae_mult.
_MAE_MULT = Decimal(str(registry.value("mae_mult")))


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the math (ar:AR-047)."""
    return Decimal(str(value))


@dataclass(frozen=True)
class PaperMaeDetected:
    """PAPER_MAE_DETECTED [HIGH] - the sec-12.5 step-3 PAPER_<EXIT_TYPE>_DETECTED event for
    a layer:L2_MAE_Threshold breach (the WSManager logs it before routing the close).
    Carries the symbol + the realizable exit bbo price + the adverse-excursion pct."""

    symbol: str
    exit_price: Decimal
    mae_pct: Decimal
    code: str = "PAPER_MAE_DETECTED"


@dataclass(frozen=True)
class PaperEmergSlTriggered:
    """evt:PAPER_EMERG_SL_TRIGGERED [CRITICAL] (sec 12.6) - the synthetic off-book emergSL
    touch fired (bid <= emergsl_price for a long; ask >= emergsl_price for a short). The
    sec-12.5 step-3 detection event for the layer:L3_Emergency_SL backstop."""

    symbol: str
    exit_price: Decimal
    mae_pct: Decimal
    code: str = "PAPER_EMERG_SL_TRIGGERED"


@dataclass(frozen=True)
class PaperExitSignal:
    """A detected paper exit condition for one open position. exit_reason is the
    mod:Exit_Controller / L3 reason string (matches execution.exit_controller.ExitReason
    values); exit_price is the realizable bbo price the exit fills at (bid/emergsl for a
    long, ask/emergsl for a short); mae_pct is the adverse excursion at this tick."""

    symbol: str
    exit_reason: str
    layer: str           # "L2_MAE" | "L3_EMERGSL"
    exit_price: Decimal
    mae_pct: Decimal


def detect_paper_exit(
    position: Position, bid: object | None, ask: object | None
) -> PaperExitSignal | None:
    """Evaluate one open position against one ticker bbo tick (ar:AR-048). Returns the
    fired exit signal, or None if neither the L2 MAE threshold nor the L3 emergSL touch
    is met. PURE - no mutation, no events; the caller (WSManager) acts on the signal and
    routes the close through mod:Exit_Controller."""
    entry = position.avg_entry_price
    is_long = position.side is PositionSide.LONG

    # The realizable exit price for this side (ar:AR-048: bid for a long, ask for a short).
    quote = bid if is_long else ask
    if quote is None:
        return None
    px = _dec(quote)

    # L2 MAE threshold (primary). Requires the entry-time ATR(14) snapshot.
    atr = position.atr_14_entry
    if atr is not None and entry != 0:
        mae = (entry - px) if is_long else (px - entry)
        if mae >= _dec(atr) * _MAE_MULT:
            return PaperExitSignal(
                symbol=position.symbol,
                exit_reason="MAE_THRESHOLD_BREACH",
                layer="L2_MAE",
                exit_price=px,
                mae_pct=mae / entry,
            )

    # L3 synthetic emergSL touch (backstop). long: bid <= emergsl_price;
    # short: ask >= emergsl_price (sec 12.6 evt:PAPER_EMERG_SL_TRIGGERED).
    esl = position.emergsl_price
    if esl is not None:
        esl_px = _dec(esl)
        touched = px <= esl_px if is_long else px >= esl_px
        if touched:
            adverse = (entry - esl_px) if is_long else (esl_px - entry)
            mae_pct = (adverse / entry) if entry != 0 and adverse > 0 else Decimal("0")
            return PaperExitSignal(
                symbol=position.symbol,
                exit_reason="EMERGENCY_SL_FIRED",
                layer="L3_EMERGSL",
                exit_price=esl_px,
                mae_pct=mae_pct,
            )

    return None
