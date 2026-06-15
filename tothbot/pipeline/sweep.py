"""The per-5m-candle universe sweep - assemble live PipelineInputs + ExecutionContext, run the
conductor (0500000 Image2 + the contract:OHLC_5m_System_Clock tick).

Source: 0500000 dv1_250 sec 3 Image2 (the gate chain reads the Pre-Step-2 pre-computation cache) +
ar:AR-075 (the SSS verdict is served from the WS-Manager incremental running values, NOT recomputed)
+ ar:AR-016 (G8 reads the live ATR(14)) + ar:AR-053 (per-module capital commitment) + ar:AR-069
(entry_ref_price = the 5m close that triggered the pipeline) + ar:AR-044/AR-068 (only READY pairs
sweep). This is the live driver of pipeline/driver.process_candidate: for each READY pair, each
permitted side, it ASSEMBLES the snapshot inputs from the live caches and awaits the conductor.

THE LIVE SSS PATH (ar:AR-075): the sweep injects an sss_evaluator backed by the pair's
LiveIndicators - so the SSS verdict comes from the O(1) incremental running values (the cache read,
"without recomputation"), bit-identical to the batch evaluate_sss over the same committed history.
The committed closes/volumes series is therefore NOT the live verdict source (it is empty in the
assembled inputs); G8's atr_14 likewise comes from LiveIndicators (ar:AR-016).

THE INJECTED PLUG-POINTS (LiveProviders): five values originate OUTSIDE the warm-up/regime/live
caches - the CIATS-owned seeds (expected_reward DEC-124, mpp_abs_cap_pct DEC-128, the CR-06 base
size) + the instrument cache (status/marginable/24h-vol) + the ticker bbo. They are INJECTED as
providers (the live layer plugs the real REST/WS caches + the CIATS estimator in later); the sweep
READS them, it never invents a value. Everything else is sourced from the live state (the
RegimeCache, the PairWarmup's HtfCache + LiveIndicators, the candle, and the per-side WSManager /
TradingModule wallet + selection state).

AR-053 margin-sizing convention (verified here): sized_usd = the order NOTIONAL (base x regime
multiplier, gate:G6); a LONG commits the USD debit (notional x (1+taker)); a SHORT commits the
leverage-bounded margin/collateral (notional / leverage_cap_short x (1+taker)) - the upstream
bounding Gate-7 CHECK-3 expects (ar:AR-053). PURE composition; async only because the conductor
traverses the dispatch seam. Decimal-only (ar:AR-047).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal

from ..config import registry
from ..config.fees import FEE_TAKER_PCT
from ..exchange.position_mirror import PositionSide
from ..regime.sss import SignalSide
from ..regime.taxonomy import Regime, profile
from .driver import ExecutionContext, process_candidate
from .regime_sizer import size_regime
from .signal_pipeline import PipelineInputs

# Universal CIATS seeds taken as Decimal once (ar:AR-047).
_TAKER = Decimal(str(FEE_TAKER_PCT))                                  # 0.26% taker leg (config.fees)
_LEVERAGE_SHORT = Decimal(str(registry.value("leverage_cap_short")))  # 3x short collateral bound


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


class ProviderNotReady(Exception):
    """A live provider's cache does not hold this symbol yet (the instrument / bbo / liquidity
    snapshot has not arrived). The (pair, side) candidate is SKIPPED this tick - exactly like a
    WARM_UP pair - never a crash. Raised by the cache-backed providers (make_live_providers)."""

    def __init__(self, symbol: str, what: str) -> None:
        self.symbol = symbol
        self.what = what
        super().__init__(f"{symbol}: live provider not ready ({what})")


@dataclass(frozen=True)
class LiveProviders:
    """The injected live-layer plug-points (the values not held in the warm-up/regime/live caches).

    instrument(symbol)              -> (instrument_status, marginable, vol_24h_usd)
    bbo(symbol)                     -> (best_bid, best_ask)
    expected_reward(symbol, regime) -> the DEC-124 run-to-reversal estimate (fraction of entry)
    mpp_abs_cap_pct(symbol, side)   -> the DEC-128 Q95 adverse-gap slippage cap (fraction)
    base_per_trade_size(symbol, side, entry_ref_price) -> the CR-06 per-pair base order USD
    ws_state(symbol)                -> the G1 WS state-machine string ("Subscribed" passes)
    The defaulted callables are pure conveniences (a per-cl_ord_id generator, the now+5s deadline,
    the non-blocking semaphore probe - the G7 ACQUIRE side is a known carry-forward flag)."""

    instrument: Callable[[str], "tuple[str, bool, object]"]
    bbo: Callable[[str], "tuple[object, object]"]
    expected_reward: Callable[[str, Regime], object]
    mpp_abs_cap_pct: Callable[[str, PositionSide], object]
    base_per_trade_size: Callable[[str, PositionSide, object], object]
    ws_state: Callable[[str], str]
    new_cl_ord_id: Callable[[], str]
    new_deadline: Callable[[], str]
    semaphore_locked: Callable[[PositionSide], bool] = staticmethod(lambda _side: False)


def permitted_sides(regime: Regime) -> list[PositionSide]:
    """The sides the daily regime permits an entry on (gate:G3 mirror) - so the sweep assembles
    only the side(s) that can pass. NON_DIR_NORMAL permits BOTH (half-size each); a trending cell
    permits its one direction (ar:AR-074)."""
    prof = profile(regime)
    out: list[PositionSide] = []
    if prof.entry_permitted(is_long=True):
        out.append(PositionSide.LONG)
    if prof.entry_permitted(is_long=False):
        out.append(PositionSide.SHORT)
    return out


def candidate_committed_usd(
    side: PositionSide, sized_usd: object, *, leverage_short: object = _LEVERAGE_SHORT,
    taker: object = _TAKER,
) -> Decimal:
    """The candidate's committed capital for gate:G7 CHECK-2 (ar:AR-053). LONG = the USD debit
    estimate (notional x (1+taker)); SHORT = the leverage-bounded margin/collateral hold
    (notional / leverage_cap_short x (1+taker))."""
    notional = _dec(sized_usd)
    factor = Decimal(1) + _dec(taker)
    if side is PositionSide.SHORT:
        return notional / _dec(leverage_short) * factor
    return notional * factor


def total_committed_usd(
    wm, side: PositionSide, *, leverage_short: object = _LEVERAGE_SHORT
) -> Decimal:
    """The module's total committed capital across its open SAME-SIDE positions for gate:G7
    CHECK-3 (ar:AR-053). LONG = sum(qty x avg_entry_price); SHORT = the leverage-bounded
    margin requirement (sum(notional) / leverage_cap_short). Per-module isolation (TB00000 sec 7):
    a long position never counts against short collateral and vice versa."""
    lev = _dec(leverage_short)
    total = Decimal(0)
    for pos in wm.open_positions().values():
        if pos.side is not side:
            continue
        notional = _dec(pos.qty) * _dec(pos.avg_entry_price)
        total += notional / lev if side is PositionSide.SHORT else notional
    return total


def live_sss_evaluator(indicators):
    """An sss_evaluator (the evaluate_sss-shaped injection point) backed by the pair's
    LiveIndicators: it IGNORES the (empty) committed series and serves the verdict from the O(1)
    incremental running values (ar:AR-075 cache read). `indicators` MUST already be stepped with
    the just-closed candle before the verdict is taken (the WS-Manager maintains it on close)."""

    def _evaluate(symbol, closes, volumes, *, side: SignalSide, **_ignored):
        return indicators.sss_verdict(side)

    return _evaluate


def assemble_candidate(
    symbol: str,
    side: PositionSide,
    *,
    candle,
    warmup,
    regime_cache,
    providers: LiveProviders,
    wm,
    now_monotonic: Callable[[], float] = time.monotonic,
) -> "tuple[PipelineInputs, ExecutionContext]":
    """Build the (PipelineInputs, ExecutionContext) for one (pair, side) candidate from the live
    caches + the injected providers. `candle` is the just-closed 5m CommittedCandle (its close is
    the ar:AR-069 entry_ref_price); `warmup` is the pair's PairWarmup (LiveIndicators + HtfCache);
    `regime_cache` holds the daily classification (present for a READY pair)."""
    classification = regime_cache.get(symbol)
    regime = classification.regime
    htf = warmup.htf
    indicators = warmup.indicators
    entry_ref_price = candle.close  # ar:AR-069 base reference = the triggering 5m close

    base = providers.base_per_trade_size(symbol, side, entry_ref_price)
    sized_usd = size_regime(symbol, side, regime, base).sized_usd  # gate:G6 (single source)

    status, marginable, vol_24h = providers.instrument(symbol)
    best_bid, best_ask = providers.bbo(symbol)
    cooldown = wm.exit_cooldown_at(symbol, side)
    seconds_since_last_exit = None if cooldown is None else _dec(now_monotonic() - cooldown)
    pos = wm.position(symbol)
    has_same_side = pos is not None and pos.side == side

    inputs = PipelineInputs(
        instrument_status=status,
        marginable=marginable,
        ws_state=providers.ws_state(symbol),
        vol_24h_usd=vol_24h,
        regime=regime,
        ema20_daily=classification.ema20,
        ema50_daily=classification.ema50,
        close_1h=htf.close_1h,
        ema20_1h=htf.ema20_1h,
        closes=(),   # ar:AR-075: the live SSS verdict is served from LiveIndicators, not the series
        volumes=(),
        candle_open=candle.open,
        candle_high=candle.high,
        candle_low=candle.low,
        candle_close=candle.close,
        seconds_since_last_exit=seconds_since_last_exit,
        consecutive_loss_count=wm.consecutive_loss_count(symbol, side),
        has_active_same_side_position=has_same_side,
        base_per_trade_size_usd=base,
        wallet_balance=wm.wallet_balance(side),
        portfolio_baseline=wm.modules[side].portfolio_baseline,
        candidate_committed_usd=candidate_committed_usd(side, sized_usd),
        total_committed_usd=total_committed_usd(wm, side),
        semaphore_locked=providers.semaphore_locked(side),
        entry_fill_price=entry_ref_price,
        atr_14=indicators.atr_14,                          # ar:AR-016 live 5m ATR(14)
        expected_reward=providers.expected_reward(symbol, regime),
    )
    ctx = ExecutionContext(
        sized_usd=sized_usd,
        best_bid=best_bid,
        best_ask=best_ask,
        mpp_abs_cap_pct=providers.mpp_abs_cap_pct(symbol, side),
        atr_14_entry=indicators.atr_14,
        regime_at_entry=regime.value,
        cl_ord_id=providers.new_cl_ord_id(),
        deadline=providers.new_deadline(),
    )
    return inputs, ctx


async def sweep_pair(
    wm,
    logger,
    *,
    candle,
    warmup,
    regime_cache,
    providers: LiveProviders,
    now_monotonic: Callable[[], float] = time.monotonic,
):
    """Run the conductor for one just-closed 5m candle's pair across every permitted side.

    `warmup.indicators` MUST already be stepped with this closed candle (the WS-Manager maintains
    it on close, ar:AR-016/AR-075) so the SSS verdict + G8 ATR reflect the closing bar. Assembles
    the inputs per side and awaits process_candidate with the LiveIndicators-backed sss_evaluator.
    Returns the list of CandidateResult (one per permitted side that has a module wallet)."""
    classification = regime_cache.get(candle.symbol)
    if classification is None:  # not READY (no regime yet) - skip (ar:AR-044 WARM_UP pre-check)
        return []
    evaluator = live_sss_evaluator(warmup.indicators)
    results = []
    for side in permitted_sides(classification.regime):
        if wm.wallet_balance(side) is None:  # no module wired for this side
            continue
        try:
            inputs, ctx = assemble_candidate(
                candle.symbol, side, candle=candle, warmup=warmup, regime_cache=regime_cache,
                providers=providers, wm=wm, now_monotonic=now_monotonic,
            )
        except ProviderNotReady:
            continue  # the pair's live caches are not populated yet - skip this (pair, side) tick
        result = await process_candidate(
            wm, logger, side, candle.symbol, inputs, ctx, sss_evaluator=evaluator
        )
        results.append(result)
    return results
