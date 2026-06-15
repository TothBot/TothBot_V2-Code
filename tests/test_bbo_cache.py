"""BBO cache tests (exchange/bbo_cache.py; ar:AR-048/AR-069 the live bid/ask feed).

Covers the Decimal ticker-bbo parse, the sole-owner snapshot/update ingest (list data), the
(best_bid, best_ask) accessor + the None-on-missing guard, and a malformed entry being skipped.
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.bbo_cache import Bbo, BboCache, parse_ticker_bbo


def _entry(symbol="BTC/USD", bid="59990.0", ask="60000.0"):
    return {"symbol": symbol, "bid": bid, "ask": ask, "last": "59995", "volume": "123"}


def _frame(entries, type_="snapshot"):
    return {"channel": "ticker", "type": type_, "data": entries}


def test_parse_is_decimal():
    bbo = parse_ticker_bbo(_entry())
    assert isinstance(bbo, Bbo)
    assert bbo.best_bid == Decimal("59990.0") and bbo.best_ask == Decimal("60000.0")


def test_ingest_and_bbo_accessor():
    cache = BboCache()
    updated = cache.ingest(_frame([_entry("BTC/USD"), _entry("ETH/USD", bid="3000", ask="3001")]))
    assert set(updated) == {"BTC/USD", "ETH/USD"}
    assert cache.bbo("BTC/USD") == (Decimal("59990.0"), Decimal("60000.0"))
    assert cache.get("ETH/USD").best_ask == Decimal("3001")


def test_missing_symbol_returns_none():
    cache = BboCache()
    assert cache.bbo("NOPE/USD") is None


def test_update_overwrites_quote():
    cache = BboCache()
    cache.ingest(_frame([_entry("BTC/USD", bid="100", ask="101")]))
    cache.ingest(_frame([_entry("BTC/USD", bid="105", ask="106")], type_="update"))
    assert cache.bbo("BTC/USD") == (Decimal("105"), Decimal("106"))


def test_malformed_entry_skipped():
    cache = BboCache()
    updated = cache.ingest(_frame([{"symbol": "BTC/USD"}, _entry("ETH/USD")]))  # BTC missing bid/ask
    assert updated == ["ETH/USD"]
    assert cache.bbo("BTC/USD") is None
