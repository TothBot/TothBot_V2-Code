"""Liquidity cache + REST GetTicker probe tests (exchange/liquidity_cache.py + rest GetTicker).

Covers parse_ticker_liquidity (vol_24h_usd = v[1]*p[1]), get_ticker_liquidity single-pair pick,
the cache TTL staleness (liquidity_refresh_hours), and the probe filling the cache with the
ar:AR-036 1.1s stagger + per-pair failure isolation (LIQUIDITY_PROBE_FAIL, prior value kept).
Driven with asyncio.run over a fake REST client + a no-op sleep + an injected clock - no network.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from tothbot.exchange.liquidity_cache import (
    LIQUIDITY_TTL_SEC,
    TICKER_STAGGER_SEC,
    LiquidityCache,
    LiquidityProbe,
    LiquidityProbeFailed,
)
from tothbot.rest.client import KrakenRestError, parse_ticker_liquidity


# --------------------------------------------------------------------------- REST parser
def test_parse_ticker_liquidity_is_vol_times_vwap():
    payload = {"error": [], "result": {
        "XXBTZUSD": {"v": ["10", "100"], "p": ["59000", "60000"]},  # 24h: 100 * 60000 = 6,000,000
    }}
    liq = parse_ticker_liquidity(payload)
    assert liq["XXBTZUSD"] == Decimal("100") * Decimal("60000")


def test_parse_ticker_skips_entry_without_v_p():
    payload = {"error": [], "result": {"X": {"v": ["1", "2"], "p": ["3", "4"]}, "Y": {"c": ["1"]}}}
    liq = parse_ticker_liquidity(payload)
    assert "X" in liq and "Y" not in liq


def test_parse_ticker_raises_on_error():
    try:
        parse_ticker_liquidity({"error": ["EQuery:Unknown asset pair"], "result": {}})
        assert False, "expected KrakenRestError"
    except KrakenRestError:
        pass


# --------------------------------------------------------------------------- fakes
class _FakeRest:
    def __init__(self, vols=None, fail=None) -> None:
        self._vols = vols or {}
        self._fail = fail or set()
        self.calls: list[str] = []

    async def get_ticker_liquidity(self, pair):
        self.calls.append(pair)
        if pair in self._fail:
            raise KrakenRestError([f"boom {pair}"])
        return self._vols.get(pair, Decimal("600000"))


class _SleepSpy:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds):
        self.delays.append(seconds)


# --------------------------------------------------------------------------- cache TTL
def test_cache_staleness():
    cache = LiquidityCache()
    assert cache.is_stale("BTC/USD", now=1000.0) is True       # never probed
    cache.put("BTC/USD", "600000", at=1000.0)
    assert cache.is_stale("BTC/USD", now=1000.0) is False
    assert cache.is_stale("BTC/USD", now=1000.0 + LIQUIDITY_TTL_SEC - 1) is False
    assert cache.is_stale("BTC/USD", now=1000.0 + LIQUIDITY_TTL_SEC) is True
    assert cache.get("BTC/USD") == Decimal("600000")


def test_stale_pairs_filters():
    cache = LiquidityCache()
    cache.put("BTC/USD", "1", at=1000.0)
    stale = cache.stale_pairs(["BTC/USD", "ETH/USD"], now=1000.0)
    assert stale == ["ETH/USD"]


# --------------------------------------------------------------------------- probe
def test_probe_fills_cache_with_stagger():
    rest = _FakeRest(vols={"ETH/USD": Decimal("700000")})
    spy = _SleepSpy()
    cache = LiquidityCache()
    refreshed = asyncio.run(
        LiquidityProbe(rest, sleep=spy, now=lambda: 5000.0).refresh(["BTC/USD", "ETH/USD"], cache)
    )
    assert set(refreshed) == {"BTC/USD", "ETH/USD"}
    assert cache.get("ETH/USD") == Decimal("700000")
    assert cache.refreshed_at("ETH/USD") == 5000.0
    assert spy.delays == [TICKER_STAGGER_SEC]   # one stagger between two pairs


def test_probe_isolates_failure_keeps_prior():
    cache = LiquidityCache()
    cache.put("BAD/USD", "999", at=10.0)   # prior value
    rest = _FakeRest(fail={"BAD/USD"})
    events = []
    refreshed = asyncio.run(
        LiquidityProbe(rest, sleep=_SleepSpy(), on_event=events.append, now=lambda: 6000.0)
        .refresh(["BAD/USD", "ETH/USD"], cache)
    )
    assert refreshed == ["ETH/USD"]
    assert cache.get("BAD/USD") == Decimal("999")   # prior kept, not cleared
    assert any(isinstance(e, LiquidityProbeFailed) and e.symbol == "BAD/USD" for e in events)
