"""The live driver - bind the WS ohlc stream to the per-5m sweep + the 1H HTF maintenance.

Source: 0500000 dv1_250 contract:OHLC_5m_System_Clock (the 5m candle close is the SOLE pipeline
tick) + ar:AR-045 (in-progress vs closed detection) + ar:AR-016/AR-075 (step the LiveIndicators on
each 5m close, before the gates read them) + ar:AR-044 (the 1H ohlc maintains the HTF EMA(20)/EMA(50)
cache) + EC-L1A-001 (a 1H close drives wm.on_htf_ohlc_close, the Layer-1a 1H reversal exit) +
rule:HR-WM-012 (the system clock must NOT fire the pipeline on a partial universe - skip the whole
sweep while any shard is reconnecting) + sec 7 mod:Logger as the sole CIATS data sink (a TRADE_CLOSE
closes the learning loop into the emitting module's CiatsPool).

THE WIRE: this is the consumer bound to the public OHLC channels (the DataLayerAssembler
handler_provider). on_ohlc_5m: detect each pair's candle close, STEP that pair's LiveIndicators
with the closed candle, then run sweep_pair (assemble live inputs -> await process_candidate per
permitted side). on_ohlc_60m: detect the 1H close, advance the HtfCache EMAs incrementally
(standard EMA step, ar:AR-044), and drive wm.on_htf_ohlc_close. The DispatchTable Handler is sync
while process_candidate is async, so ohlc_5m_handler() returns a sync adapter that schedules the
async sweep on the running loop; on_ohlc_5m itself is the testable async core.

make_ciats_sink wires mod:Logger as a per-MODULE on_event sink: every event -> Stream-1, and a
TRADE_CLOSE additionally -> that module's CiatsPool.ingest (the side is the emitting module's; the
record ALSO self-carries (25) side since dv1_253 for the durable per-module restore - per-module exit
controllers, one per wallet, sec 7). Decimal-only.
make_ciats_learning_sink is the same membrane wired to the whole CiatsConductor (the learning loop)
instead of a bare pool, so a closed trade drives conductor.ingest_close (pool + drift series + the
asset_regime bucket) per module - the operational assembly's TRADE_CLOSE -> CIATS learning seam.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal

from ..exchange.candle_close import (
    CandleCloseDetector,
    committed_candle_from_bar,
    committed_candle_from_frame,
)
from ..exchange.daily_decision import INTERVAL_1440_MIN, DailyDecisionStore
from ..exchange.ohlc_aggregate import (
    Closed1H,
    Closed24H,
    Htf1hGap,
    Htf1hHealed,
    Htf24hGap,
    Htf24hHealed,
    OhlcAggregator,
)
from ..exchange.position_mirror import PositionSide
from ..exchange.warmup import HTF_EMA_LONG, HTF_EMA_SHORT, INTERVAL_60_MIN, HtfCache
from ..regime.indicators import ema
from ..regime.live_indicators import OhlcCandle
from ..regime.taxonomy import Regime
from ..rest.client import KrakenRestError
from .sweep import LiveProviders, sweep_pair


def _is_trade_close(event: object) -> bool:
    return (
        getattr(event, "event", None) == "TRADE_CLOSE"
        or getattr(event, "code", None) == "TRADE_CLOSE"
    )


def _regime_of(record: object) -> Regime | None:
    """The Regime enum carried on a TRADE_CLOSE record's asset_regime token (sec 7 Image6), or
    None when the field is absent / not a known six-cell taxonomy value (the regime bucket is then
    skipped - the module pool + drift series still learn the close)."""
    token = getattr(record, "asset_regime", None)
    if token is None:
        return None
    try:
        return Regime(token)
    except ValueError:
        return None


def make_ciats_sink(logger, module: str, pool, *, downstream: Callable[[object], None] | None = None):
    """A per-module on_event sink: record every event to mod:Logger (Stream-1, tagged `module`),
    and route a TRADE_CLOSE into THIS module's CiatsPool (the learning loop, sec 7). `module` is
    the side name (the per-wallet exit path that emits the close knows it; the record ALSO self-carries
    (25) side since dv1_253 for the durable restore). An optional `downstream` sink chains (e.g.
    alerting)."""

    def sink(event: object) -> None:
        logger.record(event, module=module)
        if _is_trade_close(event):
            pool.ingest(event)
        if downstream is not None:
            downstream(event)

    return sink


def make_approval_alert_sink(logger) -> Callable[[object], None]:
    """The conductor's on_approval edge -> the mod:Logger HR-LG-009 operator-alert seam. A staged
    HR-CI-011 evt:CIATS_APPROVAL_REQUESTED is routed to the operator surface (the C1 IMMEDIATE / SMTP
    push) so Bill SEES the change awaiting his decision - even though the event is [HIGH], not
    [CRITICAL] (the level-driven auto-escalation would miss it). The conductor already records the
    request to Stream-1 via its on_event sink; this edge is the operator alert ONLY."""

    def on_approval(event: object) -> None:
        logger.alert(event)

    return on_approval


def make_ciats_learning_sink(
    logger,
    module: str,
    conductor,
    *,
    inbox=None,
    wallet_balance: Callable[[], object] | None = None,
    downstream: Callable[[object], None] | None = None,
):
    """The per-module CIATS LEARNING sink: record every event to mod:Logger (Stream-1, tagged
    `module`, and a schema-valid TRADE_CLOSE additionally into that module's Stream-2 corpus), and
    drive a TRADE_CLOSE into the module's CiatsConductor as the FULL per-close cadence (sec 7):
    conductor.on_close runs the learning-loop accumulate (pool + the net-P/L drift series + the
    asset_regime bucket), the Half-Kelly recompute at the cadence boundary (STAGES the
    per_trade_size_usd proposal to Bill - the HR-CI-011 alert seam fires through the conductor's
    on_approval edge), the HR-CI-007 net-P/L CUSUM out-of-cycle PLAN trigger (scan_drift DETECT/emit;
    the candidate-parameter PLAN awaits the per-trade param-level producer), and the HR-CI-003
    inter-trade boundary poll (APPLY any Bill-approved change at this confirmed close, never auto-
    applied). `module` is the side name (the partition IS the emitting wallet; the TRADE_CLOSE record
    also self-carries (25) side since dv1_253 for the durable per-module restore).

    `wallet_balance` is the side's current-balance read (a zero-arg thunk, e.g. wm.wallet_balance
    bound to this side) the Half-Kelly recompute needs; when None (live mode has no synthetic wallet)
    the sizing recompute is skipped and the seed sizing stands. `inbox` is the operator ApprovalInbox
    (the boundary poll is skipped when absent). An optional `downstream` sink chains (e.g. alerting)."""

    def sink(event: object) -> None:
        logger.record(event, module=module)
        if _is_trade_close(event):
            balance = wallet_balance() if wallet_balance is not None else None
            conductor.on_close(
                event, regime=_regime_of(event), wallet_balance=balance, inbox=inbox
            )
        if downstream is not None:
            downstream(event)

    return sink


class LiveSweepDriver:
    """Binds the public ohlc(5m)/ohlc(60m) stream to the per-5m universe sweep + the 1H HTF cache.

    Construct with the warmed pairs (PairWarmup per symbol - holds the LiveIndicators, the HtfCache,
    and the ar:AR-045 candle-close trackers), the RegimeCache, the LiveProviders, the WSManager, and
    mod:Logger. The 5m + 1H CandleCloseDetectors are seeded from each pair's warm-up trackers."""

    def __init__(
        self,
        *,
        warmups,
        regime_cache,
        providers: LiveProviders,
        wm,
        logger,
        is_reconnecting: Callable[[], bool] | None = None,
        now_monotonic: Callable[[], float] = time.monotonic,
        htf_ema_short: int = HTF_EMA_SHORT,
        htf_ema_long: int = HTF_EMA_LONG,
        bbo_provider: Callable[[str], "tuple[object, object]"] | None = None,
        on_clock_tick: Callable[[datetime], None] | None = None,
        htf_rest_client=None,
        on_committed_5m: Callable[[object], None] | None = None,
        decision_store: DailyDecisionStore | None = None,
    ) -> None:
        self._warmups = warmups
        self._regime_cache = regime_cache
        self._providers = providers
        self._wm = wm
        self._logger = logger
        self._is_reconnecting = is_reconnecting or (lambda: False)
        self._now = now_monotonic
        # The UTC-wall-clock tick for the periodic-report cadence (contract:Operator_Reporting_
        # Hierarchy). The OHLC_5m close is the SOLE pipeline clock, so each closed 5m candle's UTC
        # instant advances the PullCadenceScheduler (a calendar boundary fires the periodic pull).
        # Injected + optional - paper/unit assemblies that wire no reporting leave it None.
        self._on_clock_tick = on_clock_tick
        self._alpha_short = Decimal(2) / (int(htf_ema_short) + 1)   # 1H EMA(20) step alpha 2/21
        self._alpha_long = Decimal(2) / (int(htf_ema_long) + 1)     # 1H EMA(50) step alpha 2/51
        self._bbo = bbo_provider or providers.bbo
        # Seed the detectors with symbol-bearing CommittedCandles (the warm-up bars carry no symbol)
        # so the first ar:AR-045 fire hands the sweep a complete candle.
        self._det5 = {
            s: CandleCloseDetector(
                s, last_interval_begin=w.last_interval_begin,
                last_complete_candle=committed_candle_from_bar(s, w.last_complete_candle))
            for s, w in warmups.items()
        }
        self._det60 = {
            s: CandleCloseDetector(
                s, last_interval_begin=w.last_interval_begin_60,
                last_complete_candle=committed_candle_from_bar(s, w.last_complete_candle_60))
            for s, w in warmups.items()
        }
        # ar:AR-016/AR-075 step guards: the warm-up already seeded the indicators THROUGH the last
        # committed candle (committed[-1]). The first ar:AR-045 fire re-emits that candle - sweep on
        # it (AR-045), but do NOT re-step the indicators/HTF EMAs with it (it is already counted).
        # Step only for a candle strictly newer than the seed boundary.
        self._stepped5 = {s: w.last_interval_begin for s, w in warmups.items()}
        self._stepped60 = {s: w.last_interval_begin_60 for s, w in warmups.items()}
        # TB00768 Opt 5: Kraken WS v2 refuses a 2nd ohlc interval per symbol per connection, so the
        # 1H (ohlc_60m) feed is DERIVED by folding the 5m close stream (lossless: twelve 5m candles
        # partition the hour). on_ohlc_5m drives this; a complete hour advances the same HtfCache +
        # EC-L1A-001 path the (refused) WS 60m frame used to. The HtfCache no longer freezes.
        self._agg = OhlcAggregator()
        # TB00769: the REST handle the Htf1hGap self-heal refetches the 1H series with (one targeted
        # GetOHLCData(interval=60), reusing the warm-up seed math). Optional - a unit/bring-up assembly
        # that wires no REST client leaves it None and the gap is recorded but not auto-healed (the
        # cache still resumes on the next complete hour, bounded).
        self._htf_rest = htf_rest_client
        # TB00775 #1-B: the durable 5m-stream sink (PermanentOhlc5mSink). Called once per genuinely-new
        # closed 5m candle to persist the intraday corpus a DEEP realized backtest needs (Kraken history
        # is capped ~2.5 days). Optional - a unit/paper assembly with no records_dir leaves it None.
        self._on_committed_5m = on_committed_5m
        # TB00789: the per-pair DailyDecisionStore (the validated long-only 24h decision series). Each
        # Closed1H the OhlcAggregator emits is folded ONE TIMEFRAME UP (fold_hour -> the 24h decision
        # candle); a complete UTC day advances the pair's DailyDecisionCache on the same 00:00-UTC
        # boundary Kraken's native 1440 candle uses, and a day-aligned Htf24hGap self-heals from one
        # REST GetOHLCData(1440) re-seed (the mirror of _heal_htf). Seeded at the daily regime compute
        # (a SECOND consumer of the regime 1440 series, no third warm-up call). Optional - a unit
        # assembly that wires no store leaves it None and the second fold stage is not driven.
        self._decision = decision_store

    # --- the 5m system clock: detect -> step indicators -> sweep -------------------------------
    async def on_ohlc_5m(self, frame: dict):
        """Process one ohlc(5m) frame (the testable async core). Per element: detect the candle
        close (ar:AR-045); on a close, STEP the pair's LiveIndicators with the closed candle
        (ar:AR-016/AR-075, before the gates read it) and run sweep_pair. rule:HR-WM-012: the whole
        frame is skipped while any shard is reconnecting (no pipeline on a partial universe).
        Returns the flattened CandidateResults for the closes in this frame."""
        if self._is_reconnecting():
            return []
        results = []
        for elem in frame.get("data") or []:
            candle = committed_candle_from_frame(elem)
            detector = self._det5.get(candle.symbol)
            warmup = self._warmups.get(candle.symbol)
            if detector is None or warmup is None:   # not a warmed/known pair
                continue
            closed = detector.observe(candle)
            if closed is None:
                continue
            if closed.interval_begin > self._stepped5[candle.symbol]:  # not the already-seeded candle
                warmup.indicators.update(
                    OhlcCandle(high=closed.high, low=closed.low, close=closed.close, volume=closed.volume)
                )
                self._stepped5[candle.symbol] = closed.interval_begin
                # TB00775 #1-B: persist this genuinely-new closed 5m candle to the durable corpus (the
                # step guard dedups the re-emitted seed candle, so each real close is written once).
                if self._on_committed_5m is not None:
                    self._on_committed_5m(closed)
            # The OHLC_5m close is the system clock: advance the periodic-report cadence with this
            # candle's UTC instant (the interval_begin Unix-second key -> a UTC datetime). A calendar
            # boundary crossed since the last close fires the C2-C6 periodic pull (no manual tick).
            if self._on_clock_tick is not None:
                self._on_clock_tick(datetime.fromtimestamp(closed.interval_begin, tz=timezone.utc))
            # TB00768 Opt 5: fold this closed 5m candle into the DERIVED 1H feed. A complete hour
            # (its twelfth contiguous close) advances the HtfCache + drives the EC-L1A-001 1H reversal
            # on the same boundary the WS 60m frame used to - BEFORE this candle's sweep reads the
            # HtfCache (G4). An hour-aligned shortfall (a reconnect dropped 5m closes) surfaces as a
            # gap to self-heal; an expected mid-hour partial is discarded.
            folded = self._agg.fold(closed)
            if isinstance(folded, Closed1H):
                self._step_htf(folded.candle.symbol, folded.candle)
                # TB00789: fold this closed 1H ONE TIMEFRAME UP into the 24h DECISION series and
                # advance the pair's DailyDecisionCache on a complete UTC day (or self-heal a gap).
                await self._advance_decision(folded.candle)
            elif isinstance(folded, Htf1hGap):
                self._logger.record(folded, module="WS_Manager")
                await self._heal_htf(folded.symbol, folded.hour_begin)
            results.extend(await sweep_pair(
                self._wm, self._logger, candle=closed, warmup=warmup,
                regime_cache=self._regime_cache, providers=self._providers, now_monotonic=self._now,
            ))
        return results

    # --- the Htf1hGap self-heal: refetch the 1H series from REST and re-seed the HtfCache --------
    async def _heal_htf(self, symbol: str, hour_begin: int) -> None:
        """Self-heal a gapped pair's HtfCache from ONE targeted REST GetOHLCData(interval=60)
        (TB00769). A reconnect dropped 5m closes so the derived 1H fold was suppressed (Htf1hGap);
        the REST 1H series is authoritative (it already folds in the missed hour), so re-seed
        close_1h/EMA(20)/EMA(50) exactly as warm-up did (reuse ema()) and advance the 1H step guard
        past the refetched candle. Then drive the EC-L1A-001 1H reversal once on the fresh EMAs - a
        reversal the gap would have hidden still fires (the whole point: drive FN to zero). A REST
        failure leaves the cache untouched (it resumes on the next complete hour, bounded); no
        rest_client wired (a unit/bring-up assembly) -> the gap is recorded only."""
        if self._htf_rest is None:
            return
        warmup = self._warmups.get(symbol)
        if warmup is None:
            return
        try:
            resp60 = await self._htf_rest.get_ohlc_data(symbol, INTERVAL_60_MIN)
        except (KrakenRestError, ValueError, IndexError):
            return  # bounded miss: the cache resumes on the next complete hour (Htf1hGap already logged)
        committed = resp60.committed
        if not committed:
            return
        closes60 = [b.close for b in committed]
        last60 = committed[-1]
        warmup.htf = HtfCache(
            close_1h=last60.close,
            ema20_1h=ema(closes60, int(HTF_EMA_SHORT)),
            ema50_1h=ema(closes60, int(HTF_EMA_LONG)),
        )
        self._stepped60[symbol] = last60.time  # do not re-step the just-seeded boundary
        self._logger.record(Htf1hHealed(symbol, hour_begin), module="WS_Manager")
        bid, ask = self._bbo(symbol)
        # EC-L1A-001 1H reversal on the healed EMAs (a reversal hidden by the gap still fires).
        self._wm.on_htf_ohlc_close(symbol, warmup.htf.ema20_1h, warmup.htf.ema50_1h, bid=bid, ask=ask)

    # --- the 24h DECISION feed: fold each 1H one timeframe up -> advance the DailyDecisionCache ---
    async def _advance_decision(self, closed_1h) -> None:
        """TB00789: feed each CLOSED 1H candle into the OhlcAggregator SECOND fold stage (fold_hour,
        TB00787) and maintain the pair's DailyDecisionCache. A complete UTC day (its twenty-fourth
        contiguous 1H close) EAGER-emits a Closed24H, which advances the cache incrementally on the
        same 00:00-UTC boundary Kraken's native 1440 candle uses (the TB00788 advance). A day-aligned
        Htf24hGap (a 1H step the TB00769 heal could not recover) is recorded and self-healed from one
        REST GetOHLCData(1440) re-seed (the exact mirror of _heal_htf one timeframe up). MAINTENANCE
        only this slice - the long-only entry/exit consumer of the cache lands next; no store wired
        (a unit assembly) -> the second fold stage is not driven."""
        if self._decision is None:
            return
        folded = self._agg.fold_hour(closed_1h)
        if isinstance(folded, Closed24H):
            self._decision.advance(folded.candle.symbol, folded.candle)
        elif isinstance(folded, Htf24hGap):
            self._logger.record(folded, module="Regime_Engine")
            await self._heal_decision(folded.symbol, folded.day_begin)

    # --- the Htf24hGap self-heal: refetch the 1440 series from REST and re-seed the decision cache --
    async def _heal_decision(self, symbol: str, day_begin: int) -> None:
        """Self-heal a gapped pair's DailyDecisionCache from ONE targeted REST GetOHLCData(interval=
        1440) (TB00789, the exact mirror of _heal_htf one timeframe up). A 1H step the TB00769 heal
        could not recover left the 24h fold short of twenty-four, so the decision candle was suppressed
        (Htf24hGap); the REST 1440 daily series is authoritative (it already folds in the missed
        hours), so re-seed the cache to the exact value the live cache would hold (the TB00788
        incrementality invariant). A REST failure / no client / too-few-bars leaves the cache to
        resume on the next complete day or the next daily regime re-seed (bounded - the Htf24hGap
        already surfaced the miss), so HTF_24H_HEAL marks only a SUCCESSFUL re-seed."""
        if self._htf_rest is None or self._decision is None:
            return
        try:
            resp = await self._htf_rest.get_ohlc_data(symbol, INTERVAL_1440_MIN)
        except (KrakenRestError, ValueError, IndexError):
            return  # bounded miss: the cache resumes on the next complete day (Htf24hGap already logged)
        committed = resp.committed
        if not committed:
            return
        self._decision.seed_from_bars(symbol, committed)
        if self._decision.get(symbol) is not None:
            self._logger.record(Htf24hHealed(symbol, day_begin), module="Regime_Engine")

    # --- the 1H feed: advance the HTF EMAs -> drive the L1a 1H reversal ------------------------
    def _step_htf(self, symbol: str, closed) -> None:
        """Advance a pair's HtfCache EMA(20)/EMA(50) on a CLOSED 1H candle (standard EMA step,
        ar:AR-044, guarded against re-stepping the warm-up seed boundary), then drive
        wm.on_htf_ohlc_close (the EC-L1A-001 1H reversal exit for an open position). The closed 1H
        candle comes from the DERIVED feed (OhlcAggregator folding the 5m stream, TB00768 Opt 5);
        on_ohlc_60m feeds the same path from a WS 60m frame (retained for the unit contract)."""
        warmup = self._warmups.get(symbol)
        if warmup is None:
            return
        htf = warmup.htf
        if closed.interval_begin > self._stepped60[symbol]:  # not the already-seeded 1H candle
            close_1h = closed.close
            htf = HtfCache(
                close_1h=close_1h,
                ema20_1h=(close_1h - htf.ema20_1h) * self._alpha_short + htf.ema20_1h,
                ema50_1h=(close_1h - htf.ema50_1h) * self._alpha_long + htf.ema50_1h,
            )
            warmup.htf = htf
            self._stepped60[symbol] = closed.interval_begin
        bid, ask = self._bbo(symbol)
        # EC-L1A-001 1H reversal: drive the Layer-1a check with the current 1H EMA(20)/EMA(50).
        self._wm.on_htf_ohlc_close(symbol, htf.ema20_1h, htf.ema50_1h, bid=bid, ask=ask)

    def on_ohlc_60m(self, frame: dict) -> None:
        """Process one ohlc(60m) WS frame: detect the 1H close (ar:AR-045, separate tracker) and
        advance the HtfCache + EC-L1A-001 via _step_htf. NOTE (TB00768 Opt 5): Kraken WS v2 refuses
        a 2nd ohlc interval per symbol per connection, so the live 1H feed is now DERIVED from the 5m
        stream (on_ohlc_5m -> OhlcAggregator -> _step_htf) and this WS-frame entry is not wired to
        dispatch in production; it is retained as the equivalent frame-driven path (the unit
        contract + any single-interval connection). rule:HR-WM-012 skips while reconnecting."""
        if self._is_reconnecting():
            return
        for elem in frame.get("data") or []:
            candle = committed_candle_from_frame(elem)
            detector = self._det60.get(candle.symbol)
            warmup = self._warmups.get(candle.symbol)
            if detector is None or warmup is None:
                continue
            closed = detector.observe(candle)
            if closed is None:
                continue
            self._step_htf(candle.symbol, closed)

    # --- the sync DispatchTable adapters (Handler is sync; the 5m sweep is async) --------------
    def ohlc_5m_handler(self):
        """A sync DispatchTable Handler that schedules the async sweep on the running event loop
        (the receive loop is async). Bind via the DataLayerAssembler handler_provider for OHLC_5M."""

        def _handler(frame: dict) -> None:
            asyncio.ensure_future(self.on_ohlc_5m(frame))

        return _handler

    def ohlc_60m_handler(self):
        """The OHLC_60M Handler (already sync - the 1H feed does no async dispatch)."""
        return self.on_ohlc_60m
