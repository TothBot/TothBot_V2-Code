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
TRADE_CLOSE additionally -> that module's CiatsPool.ingest (the side is the emitting module's, the
record carries no side field - per-module exit controllers, one per wallet, sec 7). Decimal-only.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from decimal import Decimal

from ..exchange.candle_close import (
    CandleCloseDetector,
    committed_candle_from_bar,
    committed_candle_from_frame,
)
from ..exchange.position_mirror import PositionSide
from ..exchange.warmup import HTF_EMA_LONG, HTF_EMA_SHORT, HtfCache
from ..regime.live_indicators import OhlcCandle
from .sweep import LiveProviders, sweep_pair


def _is_trade_close(event: object) -> bool:
    return (
        getattr(event, "event", None) == "TRADE_CLOSE"
        or getattr(event, "code", None) == "TRADE_CLOSE"
    )


def make_ciats_sink(logger, module: str, pool, *, downstream: Callable[[object], None] | None = None):
    """A per-module on_event sink: record every event to mod:Logger (Stream-1, tagged `module`),
    and route a TRADE_CLOSE into THIS module's CiatsPool (the learning loop, sec 7). `module` is
    the side name (the TRADE_CLOSE record carries no side - it is known by the per-wallet exit path
    that emits it). An optional `downstream` sink chains (e.g. alerting)."""

    def sink(event: object) -> None:
        logger.record(event, module=module)
        if _is_trade_close(event):
            pool.ingest(event)
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
    ) -> None:
        self._warmups = warmups
        self._regime_cache = regime_cache
        self._providers = providers
        self._wm = wm
        self._logger = logger
        self._is_reconnecting = is_reconnecting or (lambda: False)
        self._now = now_monotonic
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
            results.extend(await sweep_pair(
                self._wm, self._logger, candle=closed, warmup=warmup,
                regime_cache=self._regime_cache, providers=self._providers, now_monotonic=self._now,
            ))
        return results

    # --- the 1H feed: detect -> advance the HTF EMAs -> drive the L1a 1H reversal --------------
    def on_ohlc_60m(self, frame: dict) -> None:
        """Process one ohlc(60m) frame: detect the 1H close (ar:AR-045, separate tracker), advance
        the pair's HtfCache EMA(20)/EMA(50) incrementally (standard EMA step, ar:AR-044), and drive
        wm.on_htf_ohlc_close (EC-L1A-001 1H reversal exit for an open position). rule:HR-WM-012
        skips while reconnecting. The 1H feed is DATA, not a pipeline clock tick (it never sweeps)."""
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
            htf = warmup.htf
            if closed.interval_begin > self._stepped60[candle.symbol]:  # not the already-seeded 1H candle
                close_1h = closed.close
                htf = HtfCache(
                    close_1h=close_1h,
                    ema20_1h=(close_1h - htf.ema20_1h) * self._alpha_short + htf.ema20_1h,
                    ema50_1h=(close_1h - htf.ema50_1h) * self._alpha_long + htf.ema50_1h,
                )
                warmup.htf = htf
                self._stepped60[candle.symbol] = closed.interval_begin
            bid, ask = self._bbo(candle.symbol)
            # EC-L1A-001 1H reversal: drive the Layer-1a check with the current 1H EMA(20)/EMA(50).
            self._wm.on_htf_ohlc_close(candle.symbol, htf.ema20_1h, htf.ema50_1h, bid=bid, ask=ask)

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
