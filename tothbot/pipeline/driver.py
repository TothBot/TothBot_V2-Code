"""The per-candidate conductor - run the pipeline, log it, execute on ACCEPT.

Source: 0500000 dv1_250 sec 3 (mod:Signal_Pipeline -> gate:G8 -> mod:Execution_Engine) + sec 7
(mod:Logger is the sole data sink on every pipeline tick). This is the thin connector that
turns one (pair, side) candidate into a trading action by composing the pieces already built:

    run_pipeline (the 8 gates, side-threaded)
      -> mod:Logger.record   (every outcome -> Stream-1; the module tag = the side)
      -> on ACCEPTED: execute_entry (size + MPP + emergSL-from-fill -> wm.dispatch_entry into
         THIS side's wallet)

The universe sweep is just this run once per (pair, permitted-side) per 5m candle close; the
CIATS learning side closes the loop OFF this hot path (a closed trade -> evt:TRADE_CLOSE ->
the Logger Stream-2 corpus -> the module's CiatsPool). PURE composition (async only because the
dispatch traverses the async seam); the SSS evaluator is injected for testability.
"""

from __future__ import annotations

from dataclasses import dataclass

from ..exchange.position_mirror import PositionSide
from ..execution.execution_engine import execute_entry
from ..regime.sss import evaluate_sss
from .signal_pipeline import PipelineInputs, PipelineOutcome, run_pipeline


@dataclass(frozen=True)
class ExecutionContext:
    """The execution-side inputs needed to dispatch an ACCEPTED candidate (the bbo + the
    regime-sized USD + the MPP cap + the entry-time snapshot + the order envelope). Assembled
    by the caller from the live data layer; bundled so the conductor stays thin."""

    sized_usd: object        # gate:G6_Regime_Sizer sized order value
    best_bid: object
    best_ask: object
    mpp_abs_cap_pct: object
    atr_14_entry: object
    regime_at_entry: str
    cl_ord_id: str
    deadline: str
    market_regime: str | None = None       # (18) BTC/USD anchor regime at entry (ar:AR-074)
    entry_timestamp_utc: str | None = None  # (8) ISO 8601 UTC of the entry-trigger 5m candle


@dataclass(frozen=True)
class CandidateResult:
    """The conductor's result for one candidate: the pipeline outcome + whether an entry was
    dispatched and filled."""

    outcome: PipelineOutcome
    dispatched: bool   # True iff an entry add_order was sent (PAPER: always on ACCEPT; LIVE: False if
    #                    the RL-MON-003 gate suppressed it - see PA-004 div #4 below)
    filled: bool       # PAPER: True iff the entry actually filled (the simulator opened the position).
    #                    LIVE: always False - the fill is async (confirmed later on the executions
    #                    channel via record_execution OPENED), never known synchronously here.


async def process_candidate(
    wm,
    logger,
    side: PositionSide,
    symbol: str,
    pipeline_inputs: PipelineInputs,
    exec_ctx: ExecutionContext,
    *,
    sss_evaluator=evaluate_sss,
    params=None,
) -> CandidateResult:
    """Run one (pair, side) candidate through the pipeline; log the outcome; on ACCEPTED, size +
    dispatch the entry into THIS side's wallet. Returns the pipeline outcome + dispatch/fill
    flags. `params` is the frozen per-cycle Parameter_Store_Snapshot (CI-IF-003; None -> seeds).
    The CIATS learning side closes OFF this path (via the Logger Stream-2 corpus)."""
    outcome = run_pipeline(symbol, side, pipeline_inputs, sss_evaluator=sss_evaluator, params=params)
    # mod:Logger sees every pipeline tick (Stream-1); the module tag is the side (sec 7).
    logger.record(outcome, module=side.value)

    if not outcome.accepted:
        return CandidateResult(outcome=outcome, dispatched=False, filled=False)

    entry = await execute_entry(
        wm,
        side,
        symbol,
        outcome.sized,
        sized_usd=exec_ctx.sized_usd,
        best_bid=exec_ctx.best_bid,
        best_ask=exec_ctx.best_ask,
        mpp_abs_cap_pct=exec_ctx.mpp_abs_cap_pct,
        atr_14_entry=exec_ctx.atr_14_entry,
        regime_at_entry=exec_ctx.regime_at_entry,
        cl_ord_id=exec_ctx.cl_ord_id,
        deadline=exec_ctx.deadline,
        # The entry-time D6 producer fields (contract:TRADE_CLOSE 8/18/19): signal_params rides the
        # accepted pipeline outcome (the entry SSS levels); market_regime + entry_timestamp_utc ride
        # the exec_ctx (assembled from the regime cache + the entry-trigger candle).
        signal_params=outcome.signal_params,
        market_regime=exec_ctx.market_regime,
        entry_timestamp_utc=exec_ctx.entry_timestamp_utc,
    )
    # PA-004 div #4 - the entry return diverges by mode. PAPER: `entry` is filled (the simulator
    # opened the position synchronously). LIVE: `entry` is dispatched (an add_order went out, False if
    # the RL-MON-003 gate suppressed it); the fill is async, so `filled` is False here - the open is
    # confirmed later on the executions channel (record_execution OPENED).
    if getattr(wm, "is_live", False):
        return CandidateResult(outcome=outcome, dispatched=entry, filled=False)
    return CandidateResult(outcome=outcome, dispatched=True, filled=entry)
