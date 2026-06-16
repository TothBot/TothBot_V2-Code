"""Tests: ar:AR-070 universe load (tothbot/app/universe.py).

Covers derive_universe (online + USD/USDC/USDT filter, the BTC/USD anchor union, offline/non-USD
exclusion, deterministic sort) and load_universe end to end over a fake Transport: it subscribes the
GLOBAL instrument channel, reads past a subscribe ACK to the snapshot, derives the universe, and closes
the socket; a missing snapshot or an empty result raises UniverseLoadError. Driven with asyncio.run -
no network.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from tothbot.app.universe import (
    UniverseLoadError,
    derive_universe,
    load_universe,
)
from tothbot.exchange.instrument_cache import InstrumentCache, InstrumentInfo


# --------------------------------------------------------------------------- fakes
def _info(symbol, *, status="online", marginable=True):
    return InstrumentInfo(
        symbol=symbol, status=status, marginable=marginable,
        qty_min=Decimal("0.1"), cost_min=Decimal("5"),
        price_increment=Decimal("0.01"), qty_increment=Decimal("0.1"),
    )


def _cache(*infos):
    c = InstrumentCache()
    for i in infos:
        c.put(i)
    return c


def _snapshot(pairs):
    """A Kraken WS v2 instrument snapshot frame (data.pairs)."""
    return {"channel": "instrument", "type": "snapshot", "data": {"pairs": pairs}}


def _pair(symbol, *, status="online", marginable=True):
    return {
        "symbol": symbol, "status": status, "marginable": marginable,
        "qty_min": "0.1", "cost_min": "5", "price_increment": "0.01", "qty_increment": "0.1",
    }


class _FakeTransport:
    """Hand-driven Transport: records sent frames, yields scripted recv frames, tracks close()."""

    def __init__(self, recv_frames) -> None:
        self.sent: list = []
        self._recv = list(recv_frames)
        self.closed = False

    async def send(self, message) -> None:
        self.sent.append(message)

    async def recv(self) -> dict:
        if not self._recv:
            raise AssertionError("recv() called with no scripted frames left")
        return self._recv.pop(0)

    async def close(self) -> None:
        self.closed = True


def _opener(transport):
    async def open_socket(shard_index):
        open_socket.shard = shard_index
        return transport
    return open_socket


# --------------------------------------------------------------------------- derive_universe
def test_derive_filters_online_usd_quotes_and_unions_anchor():
    cache = _cache(
        _info("ETH/USD"), _info("SOL/USDC"), _info("ADA/USDT"),
        _info("XRP/EUR"),                       # non-permitted quote -> excluded
        _info("DOGE/USD", status="maintenance"),  # offline -> excluded
    )
    uni = derive_universe(cache)  # default quotes USD/USDC/USDT, anchor BTC/USD
    assert uni == ("ADA/USDT", "BTC/USD", "ETH/USD", "SOL/USDC")  # sorted, anchor unioned, EUR/offline out


def test_derive_anchor_always_present_even_if_absent_from_snapshot():
    uni = derive_universe(_cache(_info("ETH/USD")))
    assert "BTC/USD" in uni  # ar:AR-074 anchor always included


def test_derive_dedups_when_anchor_also_in_snapshot():
    uni = derive_universe(_cache(_info("BTC/USD"), _info("ETH/USD")))
    assert uni == ("BTC/USD", "ETH/USD")  # BTC/USD not duplicated


def test_derive_custom_quotes():
    cache = _cache(_info("ETH/USD"), _info("SOL/USDC"))
    uni = derive_universe(cache, quotes=("USDC",), always_include=())
    assert uni == ("SOL/USDC",)  # only USDC, no anchor


# --------------------------------------------------------------------------- load_universe
def test_load_subscribes_instrument_reads_snapshot_and_closes():
    ack = {"method": "subscribe", "success": True, "result": {"channel": "instrument"}}
    snap = _snapshot([_pair("ETH/USD"), _pair("SOL/USDT"), _pair("XRP/EUR"),
                      _pair("OLD/USD", status="delisted")])
    transport = _FakeTransport([ack, snap])  # an ACK precedes the snapshot
    uni = asyncio.run(load_universe(_opener(transport)))
    # the GLOBAL instrument subscribe was sent (no per-pair symbol).
    assert transport.sent == [{"method": "subscribe", "params": {"channel": "instrument"}}]
    assert transport.closed is True                     # socket thrown away after the load
    assert uni == ("BTC/USD", "ETH/USD", "SOL/USDT")    # EUR + delisted excluded; anchor in


def test_load_raises_when_no_snapshot_arrives():
    transport = _FakeTransport([{"channel": "heartbeat"}, {"channel": "status"}])
    with pytest.raises(UniverseLoadError):
        asyncio.run(load_universe(_opener(transport), max_frames=2))
    assert transport.closed is True  # socket still closed on the failure path


def test_load_raises_on_empty_universe():
    # a snapshot with only non-permitted / offline pairs and the anchor suppressed -> empty -> error.
    snap = _snapshot([_pair("XRP/EUR"), _pair("OLD/USD", status="delisted")])
    transport = _FakeTransport([snap])
    with pytest.raises(UniverseLoadError):
        asyncio.run(load_universe(_opener(transport), always_include=()))


def test_load_opens_shard_zero():
    snap = _snapshot([_pair("ETH/USD")])
    opener = _opener(_FakeTransport([snap]))
    asyncio.run(load_universe(opener))
    assert opener.shard == 0  # the global instrument channel lives on shard 0
