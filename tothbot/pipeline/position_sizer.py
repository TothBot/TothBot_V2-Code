"""gate:G8_Position_Sizer - the entry-acceptance + sizing gate (Diagram 9 of 10).

Source: 0500000 dv1_249 sec 8 Image9 (0500000_Image9_T3_R7_Gate_8_Position_Sizer_Detail)
+ the sacred rule:Sacred_R_R_1_to_1_5 floor + the D1 FEE block (FEE_TAKER_PCT) + ar:AR-008
(actual entry_fill_price, never limit) + ar:AR-016 (ATR(14) incremental) + ar:AR-009 (a
SHORT is Kraken spot-margin).

Gate 8 is the LAST gate (entered only once all gate:G7_Risk_Guard checks PASS). It NEVER
blocks on size - it SIZES - but it DOES enforce the one sacred entry-acceptance floor
(rule:Expected_RR_A1_Acceptance): admit a candidate IFF

    expected_reward / net_loss >= 1.5      (rule:Sacred_R_R_1_to_1_5)

1.5 is the ONLY hardcoded value in TothBot V2 - never CIATS-owned, never lowered (a higher
expected R:R is fine; below the floor is a REJECT). The floor is an ADMISSION test only: it
NEVER constructs an exit price and NEVER closes a position (there is no fixed TP - the
take-profit is run-to-reversal). emergSL_dist is the layer:L3 crash brake and NEVER enters
the R:R ratio.

DIRECTION-SYMMETRIC (the full Long/Short mirror, Image9 - both sides equally described):
  - mae_pct and emergSL_dist MAGNITUDES are identical both sides; only the SIGN of the
    breach differs - a LONG breaches BELOW entry (on the bid), a SHORT ABOVE entry (on the
    ask, a buy-to-cover). So emergsl_price = entry * (1 - dist) for a LONG / (1 + dist) for
    a SHORT (i.e. entry -/+ ATR(14) * emergency_sl_mult).
  - net_loss carries TWO taker legs BOTH sides (entry fill + MAE exit, both FEE_TAKER_PCT):
    a LONG is buy-to-open + sell-exit, a SHORT is sell-to-open + buy-to-cover. A SHORT
    additionally carries the Kraken margin borrow fee (param:margin_borrow_fee) that a spot
    LONG never pays.
  - order_type: LONG spot buy-to-open / SHORT Kraken margin sell-to-open at
    param:leverage_cap_short.

PURE compute (mirrors taxonomy.py / paper_exit.py - no socket, no asyncio; Decimal-only per
ar:AR-047, every value taken as Decimal(str(value)) on receipt). expected_reward is an INPUT
- the CIATS-owned run-to-reversal estimate read from the tick-start snapshot
(contract:Parameter_Store_Snapshot / contract:Pre_Computation_Cache_Read); the estimator and
its post-200-trade paper tuning run OFF the hot path and are a separate concern. This module
READS the estimate and applies the sacred floor.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from ..config import registry
from ..config.fees import FEE_TAKER_PCT
from ..exchange.position_mirror import PositionSide

# The ONE hardcoded value in TothBot V2 (rule:Sacred_R_R_1_to_1_5, TB00000 sec 8). The
# entry-acceptance floor: admit IFF expected_reward / net_loss >= this. NEVER CIATS-owned,
# never lowered, never an exit price. Defined here as the sacred literal (not a registry
# seed) precisely because it is the single value CIATS does not own.
SACRED_RR_FLOOR = Decimal("1.5")

# Fee + multiplier seeds taken as Decimal exactly once (AR-047: never Decimal(float)).
_FEE_TAKER = Decimal(str(FEE_TAKER_PCT))
_MAE_MULT = Decimal(str(registry.value("mae_mult")))            # 1.5x ATR(14), CIATS-owned
_EMERG_SL_MULT = Decimal(str(registry.value("emergency_sl_mult")))  # 3.0x ATR(14)

# param:margin_borrow_fee (Image9 net_loss, SHORT only). The registry reconciles the 0500000
# token as margin_borrow_fee = margin_open_fee_pct + margin_rollover_fee_pct x held-4h-blocks.
# At ADMISSION the hold is zero blocks, so the deterministic at-entry borrow component is the
# OPEN fee; the per-4h rollover accrues over the hold and is charged at close (the ledger
# exit-fill debit, margin_rollover_usd). A spot LONG pays no borrow at all.
_MARGIN_OPEN_FEE = Decimal(str(registry.value("margin_open_fee_pct")))

# order_type tokens for the evt:G8_SIZED payload (Image9 sized output).
_LONG_ORDER = "spot_buy_to_open"        # LONG: Kraken SPOT buy-to-open
_SHORT_ORDER = "margin_sell_to_open"    # SHORT: Kraken MARGIN sell-to-open @ leverage_cap_short

_ONE = Decimal("1")


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the sizer (AR-047)."""
    return Decimal(str(value))


@dataclass(frozen=True)
class G8Sized:
    """evt:G8_SIZED [INFO] (Image9 sized output) - the canonical ACCEPT-side observable
    (the A1 floor mints no accept event itself; this is the accepted-outcome log record).
    Carries the full sized order for mod:Execution_Engine: side, order_type, entry_fill_price,
    the risk leg (net_loss, mae_pct), the CIATS estimate (expected_reward) and the realized
    ratio (expected_rr), and the layer:L3 crash brake (emergsl_dist + the resting stop price
    emergsl_price). emergsl_* NEVER participate in the R:R ratio."""

    symbol: str
    side: PositionSide
    order_type: str
    entry_fill_price: Decimal
    net_loss: Decimal
    expected_reward: Decimal
    expected_rr: Decimal
    mae_pct: Decimal
    emergsl_dist: Decimal
    emergsl_price: Decimal
    code: str = field(default="G8_SIZED", init=False)


@dataclass(frozen=True)
class G8A1Reject:
    """evt:G8_A1_REJECT [WARNING] (Image9 reject path) - the candidate's point-estimate
    expected R:R fell BELOW the sacred 1.5 floor. Carries expected_rr + its two inputs
    (expected_reward, net_loss) and the candidate identifier for CIATS reject telemetry.
    (Admitting a below-floor candidate would be a CRITICAL invariant breach - must never
    occur; this is the clean, logged reject.)"""

    symbol: str
    side: PositionSide
    expected_rr: Decimal
    expected_reward: Decimal
    net_loss: Decimal
    mae_pct: Decimal
    code: str = field(default="G8_A1_REJECT", init=False)


@dataclass(frozen=True)
class SizeOutcome:
    """The result of one Gate-8 evaluation. accepted=True carries a G8Sized event (the
    sized order); accepted=False carries a G8A1Reject (below the sacred floor)."""

    accepted: bool
    event: object  # G8Sized on accept, G8A1Reject on reject


def size_candidate(
    symbol: str,
    side: PositionSide,
    entry_fill_price: object,
    atr_14: object,
    expected_reward: object,
    *,
    margin_borrow_fee: object | None = None,
) -> SizeOutcome:
    """Evaluate one G7-passed candidate at gate:G8_Position_Sizer (Image9).

    Computes the risk leg (mae_pct -> net_loss), applies the sacred A1 acceptance floor
    (expected_reward / net_loss >= 1.5), and on ACCEPT computes the layer:L3 emergSL distance
    + resting stop price and returns a G8Sized order; on REJECT returns a G8A1Reject.

    expected_reward is the CIATS run-to-reversal estimate (a FRACTION of entry_fill_price,
    the same unit as net_loss so the ratio is dimensionless). margin_borrow_fee overrides the
    SHORT borrow component of net_loss (default: the at-entry margin OPEN fee); it is ignored
    for a LONG (a spot long pays no borrow). PURE - emits nothing; the caller logs the event."""
    is_short = side is PositionSide.SHORT
    entry = _dec(entry_fill_price)
    if entry <= 0:
        # ar:AR-008 uses the ACTUAL fill price, which is always > 0; a non-positive entry is
        # a malformed candidate (defect), never a normal flow - fail loud, never size on it.
        raise ValueError(f"entry_fill_price must be > 0 (ar:AR-008); got {entry}")
    atr = _dec(atr_14)
    exp_reward = _dec(expected_reward)

    # --- risk leg (Image9 g8d_mae -> g8d_net_loss), direction-symmetric magnitude ----------
    mae_pct = atr * _MAE_MULT / entry
    if is_short:
        borrow = _MARGIN_OPEN_FEE if margin_borrow_fee is None else _dec(margin_borrow_fee)
    else:
        borrow = Decimal("0")
    # Two taker legs BOTH sides (entry fill + MAE exit) + the margin borrow for a SHORT only.
    net_loss = mae_pct + _FEE_TAKER + _FEE_TAKER + borrow

    # --- A1 SACRED entry-acceptance floor (Image9 g8d_a1_acceptance) -----------------------
    expected_rr = exp_reward / net_loss
    if expected_rr < SACRED_RR_FLOOR:
        return SizeOutcome(
            accepted=False,
            event=G8A1Reject(
                symbol=symbol,
                side=side,
                expected_rr=expected_rr,
                expected_reward=exp_reward,
                net_loss=net_loss,
                mae_pct=mae_pct,
            ),
        )

    # --- ACCEPT: emergSL crash brake (Image9 g8d_emergsl; NEVER in the R:R ratio) ----------
    emergsl_dist = atr * _EMERG_SL_MULT / entry
    # LONG: SELL stop BELOW entry. SHORT: BUY-to-cover stop ABOVE entry (reduce_only, AR-009).
    emergsl_price = entry * (_ONE + emergsl_dist) if is_short else entry * (_ONE - emergsl_dist)
    order_type = _SHORT_ORDER if is_short else _LONG_ORDER
    return SizeOutcome(
        accepted=True,
        event=G8Sized(
            symbol=symbol,
            side=side,
            order_type=order_type,
            entry_fill_price=entry,
            net_loss=net_loss,
            expected_reward=exp_reward,
            expected_rr=expected_rr,
            mae_pct=mae_pct,
            emergsl_dist=emergsl_dist,
            emergsl_price=emergsl_price,
        ),
    )
