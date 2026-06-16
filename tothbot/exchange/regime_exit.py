"""layer:L1a regime-reversal exit detection - the daily-regime / HTF-EMA exit detector.

Source: 0500000 dv1_242 sec 3 Image3 R12 (layer:L1a_Regime_Exit + mod:Exit_Controller) +
sec 7 ar:AR-062 (the two L1a triggers) + rule:HR-EC-016 (the Step-1 pair-status precondition
+ the Layer-2 priority rule) + sec 12.5 / 12.6 (the PAPER_REGIME_EXIT_DETECTED close routing).

layer:L1a is THE take-profit (run to reversal; no fixed TP, no max-hold - DEC-124). It fires
on a DAILY regime transition or a 1H HTF EMA reversal AGAINST an open position's direction -
NOT on the ticker bbo (that is the layer:L2 MAE / layer:L3 emergSL detector, paper_exit.py).
Two triggers (ar:AR-062, "Two Layer 1a exit triggers"):

  EC-L1A-001 HTF Regime Reversal  - 1H EMA(20) crosses BELOW 1H EMA(50) while a LONG is open
    (Gate-4 symmetry). Checked on every 1H ohlc(60) close. exit_reason HTF_REGIME_REVERSAL.
  EC-L1A-002 Daily Regime Downgrade - the 00:00 UTC mod:Regime_Engine refresh reclassifies an
    open-position pair to TRENDING_NEGATIVE or NON_DIR + ELEVATED_VOL (Gate-3 symmetry).
    Checked immediately after compute_regime. exit_reason DAILY_REGIME_DOWNGRADE.

Both are direction-symmetric (the mod:Short_Module mirror inverts each inequality); the LONG
side is the primary/tested path (the Short_Module wiring is a later slice, TB00724 sec 7(3)).

PURE: a Position + the regime context in, one RegimeExitSignal (or None) out. No mutation, no
events, no socket - the caller (mod:WS_Manager) runs the rule:HR-EC-016 Step-1 pair-status
precondition and routes the close through mod:Exit_Controller (the sec-12.5 close path, the
same on_paper_close every paper exit uses; the L1a sell is the SAME close mechanism, not a
second one - no double-close).

rule:HR-EC-016(a) STEP-1 PRECONDITION: before ANY L1a cancel/sell action the pair status
(ar:AR-040 instrument status) MUST be checked. cancel_only / maintenance -> HOLD the position,
raise a CRITICAL alert, submit NO orders (the resting emergSL on the Kraken matching engine is
the only protection in a pair-disruption state). l1a_precondition_blocks() is that gate; the
WS_Manager checks it BEFORE the synthetic ledger credit so a HELD exit never moves the ledger.

DERIVATIONS (transparent; faithful to the figure, no value invented):
 (1) EC-L1A-002 fires on the figure's two enumerated downgrade TARGET states - directional
     TRENDING_NEGATIVE (either vol) OR the NON_DIR_ELEVATED whipsaw cell - which together are
     EXACTLY the long-blocking regimes (the cells where the D4 profile denies a long entry).
     The figure phrases it "TRENDING_POS -> ..."; we do NOT additionally gate on the ENTRY
     regime being TRENDING_POSITIVE, because a long held through NON_DIR_NORMAL that later
     downgrades into a long-blocking cell must still exit (gating on entry==POS would suppress
     a needed loss-min exit). Same target set as the figure; only the source-state guard is
     dropped, on loss-min grounds.
 (2) EC-L1A-001 is detected as the EMA cross-below STATE (EMA20 < EMA50), not the prev->cur
     EVENT. A long opens only under Gate-4 (1H EMA20 > EMA50 confirmed at entry), so the FIRST
     1H close where EMA20 < EMA50 IS the cross - identical to the SSS SC-SSS-2 state treatment
     (TB00724). An exact EMA20 == EMA50 tie is not "below" (the figure's word is strict), so it
     HOLDS; the daily downgrade path (which resolves an EMA tie under trend to TRENDING_NEGATIVE,
     taxonomy.py) is the conservative backstop.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from ..regime.engine import RegimeClassification
from ..regime.taxonomy import DirectionalState, Regime
from .position_mirror import Position, PositionSide


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the comparison (ar:AR-047)."""
    return value if isinstance(value, Decimal) else Decimal(str(value))


class PairStatus(Enum):
    """ar:AR-040 instrument-status states relevant to mod:Exit_Controller.

    cancel_only / maintenance are the two pair-disruption states that BLOCK an L1a/L2 sell (the
    rule:HR-EC-016(a) HOLD + CRITICAL alert); online is the normal proceed state. limit_only is its
    OWN active-exit path (NOT a blocking state): an open-position pair transitioning to limit_only
    triggers an ACTIVE exit via a single IOC limit order (LONG sell at best_bid / SHORT buy-to-cover
    at best_ask), exit_reason PAIR_LIMIT_ONLY_EXIT (the mod:Exit_Controller q4_triggers AR-040
    limit_only path, the 4th normal-operation exit reason)."""

    ONLINE = "online"
    CANCEL_ONLY = "cancel_only"
    MAINTENANCE = "maintenance"
    LIMIT_ONLY = "limit_only"


# The pair-disruption states that HOLD an L1a exit (rule:HR-EC-016(a)).
_L1A_BLOCKING: frozenset[PairStatus] = frozenset({PairStatus.CANCEL_ONLY, PairStatus.MAINTENANCE})

# WS-INST-008 wire trading-status -> the AR-040 PairStatus the Exit Controller acts on. The
# instrument channel enumerates 8 states; only the three exit-relevant ones map to a distinct
# PairStatus member (limit_only -> the AR-040 active single-IOC-limit exit; cancel_only /
# maintenance -> the L1a/L2 HOLD precondition). Every other wire status (online, post_only,
# reduce_only, work_in_progress, delisted) maps to ONLINE here - on_instrument_status only ACTS
# on limit_only, and the HOLD set is checked at the L1a/L2 dispatch precondition, not here.
_WIRE_PAIR_STATUS: dict[str, PairStatus] = {
    PairStatus.LIMIT_ONLY.value: PairStatus.LIMIT_ONLY,
    PairStatus.CANCEL_ONLY.value: PairStatus.CANCEL_ONLY,
    PairStatus.MAINTENANCE.value: PairStatus.MAINTENANCE,
    PairStatus.ONLINE.value: PairStatus.ONLINE,
}


def pair_status_from_wire(status: object) -> PairStatus:
    """Map a WS-INST-008 instrument-channel trading-status string to the AR-040 PairStatus (PURE).
    limit_only / cancel_only / maintenance map to their members; any other status (online, post_only,
    reduce_only, work_in_progress, delisted, or an unrecognised value) maps to ONLINE - the
    on_instrument_status handler only acts on LIMIT_ONLY and the HOLD set is enforced at the L1a/L2
    dispatch precondition, so a non-limit_only status is correctly inert at the instrument handler."""
    return _WIRE_PAIR_STATUS.get(str(status), PairStatus.ONLINE)


def l1a_precondition_blocks(pair_status: PairStatus) -> bool:
    """rule:HR-EC-016(a) Step 1: True when the pair status forbids the L1a cancel/sell sequence
    (cancel_only / maintenance) and the position must be HELD with a CRITICAL alert."""
    return pair_status in _L1A_BLOCKING


@dataclass(frozen=True)
class RegimeExitSignal:
    """A fired layer:L1a regime-reversal exit for one open position. exit_reason is the
    mod:Exit_Controller reason string (matches execution.exit_controller.ExitReason values);
    trigger is the canonical EC-L1A-00x id; layer distinguishes the daily vs HTF path. The
    realizable market-sell fill price is supplied by the caller (ar:AR-048 bid/ask), not here -
    regime detection is price-agnostic."""

    symbol: str
    exit_reason: str        # "HTF_REGIME_REVERSAL" | "DAILY_REGIME_DOWNGRADE"
    trigger: str            # "EC-L1A-001" | "EC-L1A-002"
    layer: str              # "L1a_HTF" | "L1a_DAILY"


@dataclass(frozen=True)
class PaperRegimeExitDetected:
    """PAPER_REGIME_EXIT_DETECTED [HIGH] (sec 12.6) - the sec-12.5 step-3 detection event for a
    fired layer:L1a regime-reversal close (the run-to-reversal take-profit). Logged by the
    WS_Manager before routing the close; carries the realizable exit price + the L1a reason."""

    symbol: str
    exit_price: Decimal
    exit_reason: str
    trigger: str
    code: str = "PAPER_REGIME_EXIT_DETECTED"


@dataclass(frozen=True)
class L1aExitHeld:
    """L1A_EXIT_HELD [CRITICAL] (rule:HR-EC-016(a)) - an L1a exit fired but the pair status is
    cancel_only / maintenance, so the position is HELD and NO order is submitted (the resting
    emergSL is the only protection in this pair-disruption state). Surfaced, never a silent
    drop - the exit re-detects on the next regime refresh / 1H close once the pair recovers."""

    symbol: str
    pair_status: str
    exit_reason: str
    trigger: str
    code: str = "L1A_EXIT_HELD"


@dataclass(frozen=True)
class RegimeExitNoQuote:
    """REGIME_EXIT_NO_QUOTE [WARNING] - an L1a exit fired and the pair-status precondition
    passed, but no realizable bbo quote (bid for a long / ask for a short, ar:AR-048) was
    available to price the simulated market sell, so the close is deferred (the position is
    retained and re-detected on the next event). Surfaced, never silently dropped."""

    symbol: str
    exit_reason: str
    trigger: str
    code: str = "REGIME_EXIT_NO_QUOTE"


def detect_daily_regime_downgrade(
    position: Position, classification: RegimeClassification
) -> RegimeExitSignal | None:
    """EC-L1A-002 (ar:AR-062): the 00:00 UTC daily regime refresh reclassified the pair into a
    downgrade state against the open position's direction. PURE - run immediately after
    compute_regime for an open-position pair. Returns the fired signal, or None.

    LONG downgrade target (Gate-3 symmetry): the new regime is TRENDING_NEGATIVE (either vol)
    OR NON_DIR_ELEVATED - the figure's two enumerated targets, == the long-blocking cells
    (derivation (1)). SHORT is the mirror (TRENDING_POSITIVE OR NON_DIR_ELEVATED)."""
    is_long = position.side is PositionSide.LONG
    directional = classification.directional
    blocking_trend = (
        DirectionalState.TRENDING_NEGATIVE if is_long else DirectionalState.TRENDING_POSITIVE
    )
    downgraded = (
        directional is blocking_trend or classification.regime is Regime.NON_DIR_ELEVATED
    )
    if not downgraded:
        return None
    return RegimeExitSignal(
        symbol=position.symbol,
        exit_reason="DAILY_REGIME_DOWNGRADE",
        trigger="EC-L1A-002",
        layer="L1a_DAILY",
    )


def detect_htf_regime_reversal(
    position: Position, htf_ema_short: object, htf_ema_long: object
) -> RegimeExitSignal | None:
    """EC-L1A-001 (ar:AR-062): the 1H EMA(20) crossed BELOW the 1H EMA(50) against the open
    position's direction. PURE - run on every 1H ohlc(60) close for an open-position pair.
    htf_ema_short / htf_ema_long are the current 1H EMA(20) / EMA(50) (indicators.ema on the 1H
    series, computed by the caller). Returns the fired signal, or None.

    LONG reversal (Gate-4 symmetry): EMA20 < EMA50 (the cross-below STATE; derivation (2)).
    SHORT is the mirror (EMA20 > EMA50). An exact tie is not "below" and HOLDS."""
    is_long = position.side is PositionSide.LONG
    ema_short = _dec(htf_ema_short)
    ema_long = _dec(htf_ema_long)
    reversed_ = ema_short < ema_long if is_long else ema_short > ema_long
    if not reversed_:
        return None
    return RegimeExitSignal(
        symbol=position.symbol,
        exit_reason="HTF_REGIME_REVERSAL",
        trigger="EC-L1A-001",
        layer="L1a_HTF",
    )
