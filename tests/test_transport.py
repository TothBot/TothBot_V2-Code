"""S2-integration tests: the async WS transport edge (transport.py).

Covers 0500000 dv1_241 sec 2 Image1 WS-LIB facts at the boundary: JSON encode on
send / decode on recv, and the mapping of every library connection-closed / OS
socket error onto the single transient TransportClosed (the type the per-shard
receive loop catches locally -> reconnect, rule:HR-WM-029).

Async edges are driven with stdlib asyncio.run() over a hand-built fake inner ws -
no network, no pytest-asyncio, no ``websockets`` install required (the real client
is imported lazily inside transport.connect(), which is not exercised here).
"""

from __future__ import annotations

import asyncio
import json

import pytest

from tothbot.exchange.transport import (
    Transport,
    TransportClosed,
    WebsocketsTransport,
)


class _FakeClosed(Exception):
    """Stand-in for a websockets ConnectionClosed exception class."""


class _FakeWS:
    """Hand-driven inner ws: scripted recv frames, captured sends, optional raises."""

    def __init__(self, *, incoming: list[str] | None = None) -> None:
        self.incoming = list(incoming or [])
        self.sent: list[str] = []
        self.closed = False
        self.raise_on_send: BaseException | None = None
        self.raise_on_recv: BaseException | None = None
        self.raise_on_close: BaseException | None = None

    async def send(self, raw: str) -> None:
        if self.raise_on_send is not None:
            raise self.raise_on_send
        self.sent.append(raw)

    async def recv(self) -> str:
        if self.raise_on_recv is not None:
            raise self.raise_on_recv
        return self.incoming.pop(0)

    async def close(self) -> None:
        if self.raise_on_close is not None:
            raise self.raise_on_close
        self.closed = True


def _transport(ws: _FakeWS) -> WebsocketsTransport:
    return WebsocketsTransport(ws, closed_excs=(_FakeClosed,))


# -- happy path: JSON encode / decode at the boundary -------------------

def test_send_json_encodes():
    ws = _FakeWS()
    t = _transport(ws)
    asyncio.run(t.send({"method": "ping", "req_id": 7}))
    assert ws.sent == [json.dumps({"method": "ping", "req_id": 7})]


def test_recv_json_decodes():
    ws = _FakeWS(incoming=[json.dumps({"channel": "ohlc", "type": "update"})])
    t = _transport(ws)
    frame = asyncio.run(t.recv())
    assert frame == {"channel": "ohlc", "type": "update"}


def test_transport_satisfies_protocol():
    # runtime_checkable Protocol: the real adapter is a Transport.
    assert isinstance(_transport(_FakeWS()), Transport)


# -- drop mapping: library/OS errors -> single transient TransportClosed -

def test_recv_closed_maps_to_transport_closed():
    ws = _FakeWS()
    ws.raise_on_recv = _FakeClosed("1006 abnormal closure")
    t = _transport(ws)
    with pytest.raises(TransportClosed):
        asyncio.run(t.recv())


def test_send_closed_maps_to_transport_closed():
    ws = _FakeWS()
    ws.raise_on_send = _FakeClosed("going away")
    t = _transport(ws)
    with pytest.raises(TransportClosed):
        asyncio.run(t.send({"method": "ping"}))


def test_recv_oserror_maps_to_transport_closed():
    ws = _FakeWS()
    ws.raise_on_recv = OSError("connection reset by peer")
    t = _transport(ws)
    with pytest.raises(TransportClosed):
        asyncio.run(t.recv())


def test_send_oserror_maps_to_transport_closed():
    ws = _FakeWS()
    ws.raise_on_send = OSError("broken pipe")
    t = _transport(ws)
    with pytest.raises(TransportClosed):
        asyncio.run(t.send({}))


# -- close is best-effort: never raises on a dying socket ---------------

def test_close_marks_closed():
    ws = _FakeWS()
    t = _transport(ws)
    asyncio.run(t.close())
    assert ws.closed is True


def test_close_swallows_errors():
    ws = _FakeWS()
    ws.raise_on_close = _FakeClosed("already closing")
    t = _transport(ws)
    # Must not raise - we are tearing down to reconnect.
    asyncio.run(t.close())
