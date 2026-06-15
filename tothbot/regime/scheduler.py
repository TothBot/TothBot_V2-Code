"""mod:Regime_Engine daily 00:00 UTC compute orchestrator - the REST I/O edge.

Source: 0500000 dv1_250 sec 5 + Image4 mod:Regime_Engine q1_do/q4_triggers: "Daily 00:00 UTC:
pull 720 daily OHLC candles via channel:kraken_rest_GetOHLCData per monitored pair + BTC/USD
anchor; exclude response[-1] per ar:AR-017; ... stagger calls at 1.1s per ar:AR-036; write to
pre-comp regime cache". Per ar:AR-049/OI-NEW-001 the daily regime classification is INDEPENDENT
of the per-pair warm-up (interval 5/60) - it is mod:Regime_Engine's own daily one-shot.

This is the edge that wraps the PURE engine.compute_regime: it drives the REST client (built in
rest/), maps each GetOHLCData response onto DailyBar, classifies, and fills the symbol-keyed
RegimeCache the Signal_Pipeline (Gate 3 + Gate 6) + Logger + CIATS read. The forming candle
(response[-1]) is already split off by rest.client.parse_ohlc per ar:AR-017, so compute_regime
runs with exclude_forming=False - the AR-017 exclusion lives in ONE place (the REST parser).

ar:AR-024/AR-036: Kraken public GetOHLCData is rate-limited by IP + pair at 1/second; calling
all monitored pairs at 00:00 UTC simultaneously trips the limit and corrupts classification, so
calls are issued SEQUENTIALLY with a 1.1s stagger between them (a rate-limit wire fact, not a
CIATS seed). The BTC/USD anchor (ar:AR-074) is always computed as market_regime. All I/O is
injected (the REST client, the inter-call sleep, the clock) so the whole orchestrator is driven
under stdlib asyncio.run over fakes - no network, no real timers.

ar:AR-062 / EC-L1A-002: when a live WSManager is wired, each fresh classification is fed to
wm.on_regime_classified immediately after compute (the daily-downgrade regime exit for an
open-position pair); bid/ask come from the optional bbo provider (the latest ticker).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field

from ..rest.client import KrakenRestError
from .engine import DailyBar, RegimeClassification, RegimeComputeError, compute_regime
from .taxonomy import Regime

# Daily candle interval (minutes) for the regime compute (interval=1440).
DAILY_INTERVAL_MIN = 1440
# ar:AR-024/AR-036 rate-limit wire fact: 1.1s between consecutive GetOHLCData calls (the
# Kraken 1/second IP+pair limit plus a 10% margin). NOT a CIATS-owned seed.
OHLC_STAGGER_SEC = 1.1
# ar:AR-074: BTC/USD is the market_regime anchor proxy.
MARKET_ANCHOR = "BTC/USD"

EventSink = Callable[[object], None]
# Optional latest-ticker bbo provider for the L1a regime-exit (bid, ask) per symbol.
BboProvider = Callable[[str], "tuple[object | None, object | None]"]
Sleep = Callable[[float], Awaitable[None]]


@dataclass(frozen=True)
class RegimeComputeFailed:
    """evt:REGIME_COMPUTE_FAIL [WARNING] {symbol, reason} - the daily compute for one pair could
    not classify (REST error or too few committed candles); the pair keeps its prior cache entry
    (stale) rather than being cleared, so a transient REST failure never silently blocks it."""

    symbol: str
    reason: str
    code: str = field(default="REGIME_COMPUTE_FAIL", init=False)


class RegimeCache:
    """The symbol-keyed pre-comp regime cache (Image4) downstream consumers read each 5m eval.

    Holds the latest RegimeClassification per pair + names the market anchor so market_regime
    (ar:AR-074, the BTC/USD daily regime) is a first-class lookup. A failed daily compute leaves
    the prior entry intact (see RegimeComputeFailed)."""

    def __init__(self, market_anchor: str = MARKET_ANCHOR) -> None:
        self._market_anchor = market_anchor
        self._by_symbol: dict[str, RegimeClassification] = {}

    def put(self, symbol: str, classification: RegimeClassification) -> None:
        self._by_symbol[symbol] = classification

    def get(self, symbol: str) -> RegimeClassification | None:
        return self._by_symbol.get(symbol)

    def regime(self, symbol: str) -> Regime | None:
        c = self._by_symbol.get(symbol)
        return c.regime if c is not None else None

    @property
    def market_anchor(self) -> str:
        return self._market_anchor

    @property
    def market_regime(self) -> Regime | None:
        """ar:AR-074: the BTC/USD anchor's daily regime (market_regime), or None if not yet
        computed for the day."""
        return self.regime(self._market_anchor)

    @property
    def symbols(self) -> frozenset[str]:
        return frozenset(self._by_symbol)


class DailyRegimeCompute:
    """The mod:Regime_Engine daily 00:00 UTC compute orchestrator (the REST edge over the pure
    classifier). compute_all() pulls + classifies every monitored pair (plus the BTC/USD anchor)
    with the AR-036 1.1s stagger, returning the filled RegimeCache."""

    def __init__(
        self,
        rest_client,
        *,
        market_anchor: str = MARKET_ANCHOR,
        stagger_sec: float = OHLC_STAGGER_SEC,
        sleep: Sleep = asyncio.sleep,
        on_event: EventSink | None = None,
        ws_manager=None,
        bbo_provider: BboProvider | None = None,
    ) -> None:
        self._rest = rest_client
        self._market_anchor = market_anchor
        self._stagger_sec = stagger_sec
        self._sleep = sleep
        self._on_event = on_event
        self._wm = ws_manager
        self._bbo = bbo_provider

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    async def compute_all(
        self, pairs: Sequence[str], *, cache: RegimeCache | None = None
    ) -> RegimeCache:
        """Classify every pair in `pairs` (plus the market anchor) for the day and fill the
        cache. Calls are issued SEQUENTIALLY with a 1.1s stagger between them (ar:AR-036). A
        per-pair failure is logged (REGIME_COMPUTE_FAIL) and skipped - it never aborts the sweep
        nor clears the pair's prior cache entry. Reuse `cache` to update an existing day's cache."""
        if cache is None:
            cache = RegimeCache(self._market_anchor)
        # The anchor is always computed; preserve caller order and de-dup.
        targets: list[str] = []
        for symbol in [*pairs, self._market_anchor]:
            if symbol not in targets:
                targets.append(symbol)

        for index, symbol in enumerate(targets):
            if index > 0:
                await self._sleep(self._stagger_sec)  # ar:AR-036 inter-call stagger
            await self._compute_one(symbol, cache)
        return cache

    async def _compute_one(self, symbol: str, cache: RegimeCache) -> None:
        try:
            resp = await self._rest.get_ohlc_data(symbol, DAILY_INTERVAL_MIN)
            # parse_ohlc already excluded response[-1] (ar:AR-017) -> exclude_forming=False here.
            bars = [
                DailyBar.of(b.open, b.high, b.low, b.close, b.volume)
                for b in resp.committed
            ]
            classification = compute_regime(symbol, bars, exclude_forming=False)
        except (KrakenRestError, RegimeComputeError) as exc:
            self._emit(RegimeComputeFailed(symbol, str(exc)))
            return

        cache.put(symbol, classification)
        self._emit(classification.classified_event)
        self._drive_regime_exit(symbol, classification)

    def _drive_regime_exit(self, symbol: str, classification: RegimeClassification) -> None:
        """ar:AR-062 / EC-L1A-002: feed the fresh classification to the WSManager so an
        open-position pair gets the daily-downgrade exit check (no-op if no WSManager wired or
        the pair has no open position; live is executions-driven and a no-op inside WSManager)."""
        if self._wm is None:
            return
        bid = ask = None
        if self._bbo is not None:
            bid, ask = self._bbo(symbol)
        self._wm.on_regime_classified(symbol, classification, bid=bid, ask=ask)
