"""mod:Signal_Pipeline - the 8-gate entry pipeline orchestrator (0500000 Image2).

Source: 0500000 dv1_250 sec 3 Image2 (the full gate chain) + the per-gate q4_triggers that
define the strict order. This module CHAINS the gates already built under pipeline/ +
regime/, threading the candidate `side` so a SHORT candidate takes the short test at every
directional gate (the full Long/Short mirror). It owns NO gate logic itself - it is the
deterministic conductor that runs a candidate through:

    Pre-Gate-1  per-pair status + per-side universe partition   (entry_eligibility)
       -> G1   WS state-machine readiness                        (entry_eligibility)
       -> G2   24h USD liquidity floor                           (entry_eligibility)
       -> G3   regime filter (permitted_side)                    (regime/taxonomy)
       -> G4   1H HTF directional confirmation                   (htf_confirmation)
       -> SSS  three-factor signal score                         (regime/sss)
       -> G5   selection-controller 4 quality sub-gates          (selection_controller)
       -> G6   regime size multiplier                            (regime_sizer)
       -> G7   risk guard (drawdown/concentration/exposure/sem)  (risk_guard)
       -> G8   position sizer + sacred 1:1.5 R:R floor           (position_sizer)
       -> ACCEPTED (the sized order dispatched to mod:Execution_Engine)

STRICT ORDER, first failure short-circuits (the cheap eligibility probes precede the
expensive regime/signal work). Each gate's labeled event is the terminal outcome on a stop.
G3 is the taxonomy permitted-side test; G6 never blocks (only sizes). PURE composition
(Decimal-only via the gates). The SSS evaluator is injected (defaults to regime.sss.
evaluate_sss) so the chaining is testable in isolation and a WARM_UP pair (too few committed
candles) is a clean SSS skip rather than an exception.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from ..exchange.position_mirror import PositionSide
from ..regime.sss import SignalSide, SssComputeError, evaluate_sss
from ..regime.taxonomy import Regime, profile
from .entry_eligibility import check_liquidity, check_pair_status, check_state_machine
from .htf_confirmation import confirm_htf
from .parameter_snapshot import CycleParameters, build_cycle_parameters
from .position_sizer import size_candidate
from .regime_sizer import size_regime
from .risk_guard import evaluate_risk_guard
from .selection_controller import evaluate_selection


@dataclass(frozen=True)
class PipelineInputs:
    """Everything the gate chain needs for one (pair, side) candidate at one tick. The fields
    are grouped by the gate that consumes them; all are read-only snapshots (the producers run
    off the hot path)."""

    # Pre-Gate-1
    instrument_status: str
    marginable: bool
    # Gate 1
    ws_state: str
    # Gate 2
    vol_24h_usd: object
    # Gate 3 / Gate 4 / Gate 6 (the daily regime tag + the HTF inputs)
    regime: Regime
    ema20_daily: object
    ema50_daily: object
    close_1h: object
    ema20_1h: object
    # SSS (committed 5m series)
    closes: object
    volumes: object
    # Gate 5 (the committed signal candle + the per-side SC state)
    candle_open: object
    candle_high: object
    candle_low: object
    candle_close: object
    seconds_since_last_exit: object | None
    consecutive_loss_count: int
    has_active_same_side_position: bool
    # Gate 6
    base_per_trade_size_usd: object
    # Gate 7 (the module wallet + commitments)
    wallet_balance: object
    portfolio_baseline: object
    candidate_committed_usd: object
    total_committed_usd: object
    semaphore_locked: bool
    # Gate 8
    entry_fill_price: object
    atr_14: object
    expected_reward: object


@dataclass(frozen=True)
class PipelineOutcome:
    """The terminal result of running one candidate through the pipeline. accepted=True carries
    the G8Sized order (the dispatchable entry); otherwise stage names the gate that stopped it,
    reason is that gate's labeled code/disposition, and event is the gate's event object."""

    accepted: bool
    side: PositionSide
    stage: str          # "PRE_GATE_1" | "G1" | ... | "G8"
    reason: str         # the terminal code/disposition (e.g. "SIGNAL_REJECTED", "G8_SIZED")
    event: object | None
    sized: object | None  # G8Sized on ACCEPTED, else None
    signal_params: dict | None = None  # (19) the entry-time SSS levels, carried on ACCEPTED only
    code: str = field(default="SIGNAL_PIPELINE_RESULT", init=False)


def _reject(side, stage, reason, event):
    return PipelineOutcome(accepted=False, side=side, stage=stage, reason=reason, event=event, sized=None)


def run_pipeline(
    symbol: str,
    side: PositionSide,
    inputs: PipelineInputs,
    *,
    sss_evaluator=evaluate_sss,
    params: CycleParameters | None = None,
) -> PipelineOutcome:
    """Run one (pair, side) candidate through the 8-gate chain (Image2), short-circuiting on the
    first failure. Returns the terminal PipelineOutcome - either ACCEPTED (the G8 sized order) or
    the first stop with its gate event. Threads `side` so a SHORT takes the short test everywhere.

    `params` is the FROZEN per-cycle Parameter_Store_Snapshot (contract:CI-IF-003): every CIATS-owned
    gate tunable is read ONCE from it at the start of the cycle, so a mid-cycle CIATS write never
    drifts an in-flight evaluation. None -> a seed-only snapshot (identical to the pre-CIATS behavior:
    each gate reads its registry seed). The sacred 1:1.5 R:R is never in the snapshot - G8 hardcodes it."""
    is_long = side is PositionSide.LONG
    # contract:Parameter_Store_Snapshot - take ONE frozen read for the whole cycle (CI-IF-003).
    params = params or build_cycle_parameters()

    # Pre-Gate-1: instrument status + per-side universe partition (SHORT = marginable subset).
    pre = check_pair_status(side, inputs.instrument_status, marginable=inputs.marginable)
    if not pre.passed:
        return _reject(side, "PRE_GATE_1", pre.disposition.value, pre)

    # Gate 1: WS state-machine readiness (side-shared).
    g1 = check_state_machine(inputs.ws_state)
    if not g1.passed:
        return _reject(side, "G1", g1.rejection_code, g1)

    # Gate 2: 24h USD liquidity floor (CIATS-owned min_volume_usd_daily from the snapshot).
    g2 = check_liquidity(inputs.vol_24h_usd, min_volume_usd_daily=params.get("min_volume_usd_daily"))
    if not g2.passed:
        return _reject(side, "G2", "LIQUIDITY_REJECTED", g2)

    # Gate 3: regime filter - the pair's daily regime must permit this side AND not be on the CIATS
    # Regime Library's param:disallowed_regimes block list (a proven non-positive-edge regime).
    if params.regime_disallowed(inputs.regime):
        return _reject(side, "G3", "REGIME_BLOCKED", None)
    if not profile(inputs.regime).entry_permitted(is_long=is_long):
        return _reject(side, "G3", "REGIME_BLOCKED", None)

    # Gate 4: 1H HTF directional confirmation (NON_DIR_NORMAL bypasses inside the gate).
    g4 = confirm_htf(
        side, inputs.regime,
        ema20_daily=inputs.ema20_daily, ema50_daily=inputs.ema50_daily,
        close_1h=inputs.close_1h, ema20_1h=inputs.ema20_1h,
    )
    if not g4.passed:
        return _reject(side, "G4", "HTF_GATE_REJECTED", g4.event)

    # SSS Signal Engine: three-factor score for this side. A WARM_UP pair (too few committed
    # candles) is a clean skip, not an exception (the diagram skips WARM_UP at the pre-check).
    signal_side = SignalSide.LONG if is_long else SignalSide.SHORT
    try:
        sss = sss_evaluator(symbol, inputs.closes, inputs.volumes, side=signal_side)
    except SssComputeError:
        return _reject(side, "SSS", "WARM_UP", None)
    if not sss.passed:
        return _reject(side, "SSS", "SIGNAL_REJECTED", sss)

    # Gate 5: selection-controller 4 quality sub-gates (per-side state).
    g5 = evaluate_selection(
        side,
        candle_open=inputs.candle_open, candle_high=inputs.candle_high,
        candle_low=inputs.candle_low, candle_close=inputs.candle_close,
        seconds_since_last_exit=inputs.seconds_since_last_exit,
        consecutive_loss_count=inputs.consecutive_loss_count,
        has_active_same_side_position=inputs.has_active_same_side_position,
        sc_body_threshold=params.get("sc_body_threshold"),
        sc_cooldown_seconds=params.get("sc_cooldown_seconds"),
        sc_consecutive_limit=params.get("sc_consecutive_limit"),
    )
    if not g5.passed:
        return _reject(side, "G5", "SELECTION_REJECTED", g5.event)

    # Gate 6: regime size multiplier (never blocks - only sizes).
    g6 = size_regime(symbol, side, inputs.regime, inputs.base_per_trade_size_usd)

    # Gate 7: risk guard (per-wallet drawdown / concentration / exposure / semaphore).
    g7 = evaluate_risk_guard(
        side,
        wallet_balance=inputs.wallet_balance, portfolio_baseline=inputs.portfolio_baseline,
        candidate_committed_usd=inputs.candidate_committed_usd,
        total_committed_usd=inputs.total_committed_usd,
        semaphore_locked=inputs.semaphore_locked,
        full_halt_drawdown_pct=params.get("full_halt_drawdown_pct"),
        session_pause_drawdown_pct=params.get("session_pause_drawdown_pct"),
        concentration_limit=params.get("concentration_limit_per_module"),
        exposure_limit_pct=params.get("exposure_limit_pct"),
    )
    if not g7.passed:
        return _reject(side, "G7", g7.disposition.value, g7.event)

    # Gate 8: position sizer + the SACRED 1:1.5 R:R acceptance floor (hardcoded, never the snapshot).
    g8 = size_candidate(
        symbol, side, inputs.entry_fill_price, inputs.atr_14, inputs.expected_reward,
        mae_mult=params.get("mae_mult"), emergency_sl_mult=params.get("emergency_sl_mult"),
    )
    if not g8.accepted:
        return _reject(side, "G8", "G8_A1_REJECT", g8.event)

    # ACCEPTED - the sized order (g8.event is the G8Sized) dispatches to mod:Execution_Engine.
    # g6 (the regime multiplier) is carried on the G8 path upstream; the accept observable is G8_SIZED.
    # signal_params (the entry-time SSS levels) rides the accept so the entry-side producer can stash
    # it on the position (the contract:TRADE_CLOSE field-19 per-trade level series, sec 7).
    return PipelineOutcome(
        accepted=True, side=side, stage="G8", reason="G8_SIZED", event=g8.event, sized=g8.event,
        signal_params=getattr(sss, "signal_params", None),
    )


# A convenience for callers/tests building a fully-passing baseline candidate.
_DECIMAL_ZERO = Decimal("0")
