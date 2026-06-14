"""S2c tests: the outbound dispatch-seam I/O bodies (outbound.py).

Covers 0500000 dv1_241 sec 12 Image7 + sec 12.3: the live transmitter
(ws_private.send over the single private Transport, late-bound + re-bound on
reconnect; OutboundNotConnectedError when no socket) and the paper boundary
(transmits nothing to Kraken; delegates to an injected fill simulator if wired).

Async edges driven with stdlib asyncio.run over a hand-built fake Transport - no
network, no pytest-asyncio.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from tothbot.exchange.outbound import (
    OutboundNotConnectedError,
    PaperDispatchSimulator,
    PrivateTransmitter,
)
from tothbot.exchange.seam import OutboundOp
from tothbot.exchange.transport import Transport, TransportClosed


class _FakeTransport:
    """Hand-driven private Transport: captures sends, optional raise on send."""

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self.raise_on_send: BaseException | None = None
        self.closed = False

    async def send(self, message: dict) -> None:
        if self.raise_on_send is not None:
            raise self.raise_on_send
        # mirror the real adapter's JSON-serialisability contract at the boundary
        json.dumps(message)
        self.sent.append(message)

    async def recv(self) -> dict:  # pragma: no cover - not exercised here
        raise TransportClosed("recv not used")

    async def close(self) -> None:
        self.closed = True


# -- PrivateTransmitter: the live ws_private.send body -------------------

def test_transmitter_satisfies_transport_is_protocol():
    assert isinstance(_FakeTransport(), Transport)


def test_transmitter_unbound_raises_never_silent_drop():
    tx = PrivateTransmitter()
    assert tx.is_connected is False
    with pytest.raises(OutboundNotConnectedError):
        asyncio.run(tx(OutboundOp.ADD_ORDER, {"order": 1}))


def test_transmitter_bound_sends_over_private_transport():
    tx = PrivateTransmitter()
    t = _FakeTransport()
    tx.bind(t)
    assert tx.is_connected is True
    asyncio.run(tx(OutboundOp.ADD_ORDER, {"method": "add_order", "cl_ord_id": "x1"}))
    assert t.sent == [{"method": "add_order", "cl_ord_id": "x1"}]


def test_transmitter_rebind_targets_fresh_socket_after_reconnect():
    tx = PrivateTransmitter()
    old, new = _FakeTransport(), _FakeTransport()
    tx.bind(old)
    asyncio.run(tx(OutboundOp.CANCEL_ORDER, {"a": 1}))
    tx.bind(new)  # reconnect swapped the private socket
    asyncio.run(tx(OutboundOp.CANCEL_ORDER, {"a": 2}))
    assert old.sent == [{"a": 1}]
    assert new.sent == [{"a": 2}]


def test_transmitter_unbind_disconnects():
    tx = PrivateTransmitter()
    tx.bind(_FakeTransport())
    tx.unbind()
    assert tx.is_connected is False
    with pytest.raises(OutboundNotConnectedError):
        asyncio.run(tx(OutboundOp.ADD_ORDER, {}))


def test_transmitter_send_failure_propagates_for_reconnect():
    tx = PrivateTransmitter()
    t = _FakeTransport()
    t.raise_on_send = TransportClosed("dropped")
    tx.bind(t)
    with pytest.raises(TransportClosed):
        asyncio.run(tx(OutboundOp.ADD_ORDER, {}))


# -- PaperDispatchSimulator: the paper boundary -------------------------

def test_paper_boundary_records_and_transmits_nothing():
    paper = PaperDispatchSimulator()
    asyncio.run(paper(OutboundOp.ADD_ORDER, {"pair": "BTC/USD"}))
    asyncio.run(paper(OutboundOp.DISPATCH_MARKET_SELL, {"pair": "ETH/USD"}))
    assert paper.simulated == [
        (OutboundOp.ADD_ORDER, {"pair": "BTC/USD"}),
        (OutboundOp.DISPATCH_MARKET_SELL, {"pair": "ETH/USD"}),
    ]


def test_paper_boundary_delegates_to_injected_fill_simulator():
    fills: list = []

    async def _sim(op, message):
        fills.append((op, message))

    paper = PaperDispatchSimulator(fill_simulator=_sim)
    asyncio.run(paper(OutboundOp.ADD_ORDER, {"pair": "BTC/USD"}))
    assert fills == [(OutboundOp.ADD_ORDER, {"pair": "BTC/USD"})]
    assert paper.simulated == [(OutboundOp.ADD_ORDER, {"pair": "BTC/USD"})]
