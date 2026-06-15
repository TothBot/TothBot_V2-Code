"""Instrument cache + CR-06 base-size tests (exchange/instrument_cache.py; A-17/AR-028/CR-06).

Covers the Decimal instrument-pair parse (price_increment per A-17 GAP-C, marginable default), the
sole-owner snapshot/update ingest (skips a malformed pair, keeps the prior entry), and the CR-06
per-pair base order size max(50, 5*max(cost_min, qty_min*entry_ref)) with the floor / cost_min /
qty_min-priced branch each binding.
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.instrument_cache import (
    InstrumentCache,
    InstrumentInfo,
    base_per_trade_size_usd,
    parse_instrument_pair,
)


def _pair(symbol="BTC/USD", status="online", marginable=True, qty_min="0.0001",
          cost_min="0.5", price_increment="0.1", qty_increment="0.00000001"):
    return {"symbol": symbol, "status": status, "marginable": marginable,
            "qty_min": qty_min, "cost_min": cost_min,
            "price_increment": price_increment, "qty_increment": qty_increment}


def _frame(pairs, type_="snapshot"):
    return {"channel": "instrument", "type": type_, "data": {"pairs": pairs}}


# --------------------------------------------------------------------------- parse
def test_parse_is_decimal_and_online():
    info = parse_instrument_pair(_pair())
    assert isinstance(info, InstrumentInfo)
    assert info.is_online and info.marginable
    assert info.qty_min == Decimal("0.0001") and isinstance(info.cost_min, Decimal)
    assert info.price_increment == Decimal("0.1")


def test_marginable_defaults_false():
    info = parse_instrument_pair(_pair(marginable=None) | {"marginable": False})
    assert info.marginable is False
    # absent key -> default False
    elem = _pair()
    del elem["marginable"]
    assert parse_instrument_pair(elem).marginable is False


# --------------------------------------------------------------------------- cache ingest
def test_ingest_snapshot_fills_cache():
    cache = InstrumentCache()
    updated = cache.ingest(_frame([_pair("BTC/USD"), _pair("ETH/USD", marginable=False)]))
    assert set(updated) == {"BTC/USD", "ETH/USD"}
    assert cache.get("ETH/USD").marginable is False
    assert cache.symbols == frozenset({"BTC/USD", "ETH/USD"})


def test_update_changes_one_pair():
    cache = InstrumentCache()
    cache.ingest(_frame([_pair("BTC/USD", status="online")]))
    cache.ingest(_frame([_pair("BTC/USD", status="cancel_only")], type_="update"))
    assert cache.get("BTC/USD").status == "cancel_only"
    assert cache.get("BTC/USD").is_online is False


def test_malformed_pair_is_skipped_keeps_prior():
    cache = InstrumentCache()
    cache.ingest(_frame([_pair("BTC/USD")]))
    prior = cache.get("BTC/USD")
    bad = {"symbol": "BTC/USD"}  # missing qty_min/cost_min/etc
    updated = cache.ingest(_frame([bad, _pair("ETH/USD")]))
    assert updated == ["ETH/USD"]            # BTC skipped
    assert cache.get("BTC/USD") is prior     # prior entry stands


# --------------------------------------------------------------------------- CR-06 base size
def test_cr06_floor_binds():
    # qty_min*entry = 6, cost_min 0.5 -> pair_min 6 -> 5*6=30 -> max(50,30)=50
    assert base_per_trade_size_usd("0.5", "0.0001", "60000") == Decimal("50")


def test_cr06_cost_min_binds():
    # cost_min 20 > qty_min*entry 6 -> 5*20=100 -> max(50,100)=100
    assert base_per_trade_size_usd("20", "0.0001", "60000") == Decimal("100")


def test_cr06_qty_priced_min_binds():
    # qty_min*entry = 0.01*60000 = 600 -> 5*600 = 3000
    assert base_per_trade_size_usd("0.5", "0.01", "60000") == Decimal("3000")
