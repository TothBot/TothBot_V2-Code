"""The 24h liquidity cache + its REST probe - the D1-owned liquidity_24h (Gate-2 source).

Source: 0500000 dv1_250 Gate-2 (vol_24h_usd >= min_volume_usd_daily $500k; "consumes the D1-owned
liquidity_24h value verbatim, does NOT recompute") + the liquidity cache spec (channel:kraken_rest_
Ticker liquidity probe at universe load + refresh cycle; param:liquidity_refresh_hours = 4 governs
the cache TTL, value home TB00000 sec 8) + ar:AR-070 (the monitored universe = pairs with 24h
volume >= the floor) + ar:AR-036 (the 1.1s inter-call stagger is a rate-limit wire fact, not a seed).

vol_24h_usd is REST-sourced (GetTicker: last-24h base volume * last-24h vwap), distinct from the
live WS bbo (bbo_cache.py). The probe refills the cache at universe load + every liquidity_refresh_
hours; a per-pair REST failure is isolated (the prior cached value stands). All I/O injected (the
REST client, the inter-call sleep, the clock) -> driven under asyncio.run over fakes, no network.
Decimal-only (ar:AR-047).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal

from ..config import registry
from ..rest.client import KrakenRestError

# param:liquidity_refresh_hours (value home TB00000 sec 8) -> the cache TTL in seconds.
LIQUIDITY_REFRESH_HOURS = float(registry.value("liquidity_refresh_hours"))   # 4h starting
LIQUIDITY_TTL_SEC = LIQUIDITY_REFRESH_HOURS * 3600.0
# ar:AR-036 rate-limit wire fact: 1.1s between consecutive public REST calls. NOT a CIATS seed.
TICKER_STAGGER_SEC = 1.1

EventSink = Callable[[object], None]
Sleep = Callable[[float], Awaitable[None]]
Clock = Callable[[], float]


@dataclass(frozen=True)
class LiquidityProbeFailed:
    """evt:LIQUIDITY_PROBE_FAIL [WARNING] {symbol, reason} - one pair's GetTicker probe failed;
    the pair keeps its prior cached liquidity (stale, not cleared) so a transient REST failure
    never silently drops it from the universe."""

    symbol: str
    reason: str
    code: str = field(default="LIQUIDITY_PROBE_FAIL", init=False)


@dataclass(frozen=True)
class _Entry:
    vol_24h_usd: Decimal
    refreshed_at: float


class LiquidityCache:
    """Sole-owner per-symbol 24h-USD-volume cache (the D1 liquidity_24h Gate-2 reads). put() stamps
    the refresh time; is_stale() drives the liquidity_refresh_hours TTL refresh decision."""

    def __init__(self) -> None:
        self._by_symbol: dict[str, _Entry] = {}

    def put(self, symbol: str, vol_24h_usd: object, *, at: float) -> None:
        v = vol_24h_usd if isinstance(vol_24h_usd, Decimal) else Decimal(str(vol_24h_usd))
        self._by_symbol[symbol] = _Entry(v, at)

    def get(self, symbol: str) -> Decimal | None:
        entry = self._by_symbol.get(symbol)
        return None if entry is None else entry.vol_24h_usd

    def refreshed_at(self, symbol: str) -> float | None:
        entry = self._by_symbol.get(symbol)
        return None if entry is None else entry.refreshed_at

    def is_stale(self, symbol: str, *, now: float, ttl_sec: float = LIQUIDITY_TTL_SEC) -> bool:
        """True if the pair has no cached value or its value is older than the TTL (so the probe
        should refresh it). A pair never probed is stale."""
        entry = self._by_symbol.get(symbol)
        return entry is None or (now - entry.refreshed_at) >= ttl_sec

    def stale_pairs(
        self, pairs: Sequence[str], *, now: float, ttl_sec: float = LIQUIDITY_TTL_SEC
    ) -> list[str]:
        return [p for p in pairs if self.is_stale(p, now=now, ttl_sec=ttl_sec)]

    @property
    def symbols(self) -> frozenset[str]:
        return frozenset(self._by_symbol)


class LiquidityProbe:
    """The REST GetTicker edge that fills the LiquidityCache (the universe-load + 4h refresh probe).

    refresh(pairs, cache) pulls each pair's 24h USD volume SEQUENTIALLY with the AR-036 1.1s stagger
    and stamps the cache with the injected clock; a per-pair REST failure is isolated (LIQUIDITY_
    PROBE_FAIL, the prior value kept). All I/O is injected (REST client, sleep, clock)."""

    def __init__(
        self,
        rest_client,
        *,
        stagger_sec: float = TICKER_STAGGER_SEC,
        sleep: Sleep = asyncio.sleep,
        on_event: EventSink | None = None,
        now: Clock = time.time,
    ) -> None:
        self._rest = rest_client
        self._stagger_sec = stagger_sec
        self._sleep = sleep
        self._on_event = on_event
        self._now = now

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    async def refresh(self, pairs: Sequence[str], cache: LiquidityCache) -> list[str]:
        """Probe each pair's 24h USD volume and fill the cache (stamped at the current clock).
        Sequential with the 1.1s stagger (ar:AR-036); per-pair failure isolated. Returns the
        symbols successfully refreshed this cycle."""
        at = self._now()
        refreshed: list[str] = []
        for index, symbol in enumerate(pairs):
            if index > 0:
                await self._sleep(self._stagger_sec)  # ar:AR-036 inter-call stagger
            try:
                vol_24h_usd = await self._rest.get_ticker_liquidity(symbol)
            except KrakenRestError as exc:
                self._emit(LiquidityProbeFailed(symbol, str(exc)))
                continue
            cache.put(symbol, vol_24h_usd, at=at)
            refreshed.append(symbol)
        return refreshed
