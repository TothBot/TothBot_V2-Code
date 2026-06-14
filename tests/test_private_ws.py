"""S2c tests: the private WS connection + executions ingest (private_ws.py).

Covers 0500000 dv1_241 sec 7 container:Private_WS_v2 + sec 2 Image1 AR-049 startup
steps 5/6 + the private restore subset: the live-only guard (PA-004 div #1), the
mandatory executions subscribe flags (rule:HR-WM-005 order_status:true / snap_orders
/ ratecounter), the inbound fill -> mirror routing (executions update ->
record_execution; snapshot -> restore_position_mirror), the transmitter bind on
connect (ws_private.send targets the live socket), and the reconnect
RESTORE_POSITION_MIRROR step wired to restore_position_mirror(snap_orders).

Async edges driven with stdlib asyncio.run over hand-built fakes - no network.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from tothbot.config.settings import Mode
from tothbot.exchange.channels import PrivateChannel
from tothbot.exchange.reconnect import RestoreStep, build_private_restore_sequence
from tothbot.exchange.private_ws import (
    PositionMirrorRestored,
    PrivateConnectionAssembler,
    balances_subscribe,
    executions_subscribe,
)
from tothbot.exchange.transport import Transport, TransportClosed
from tothbot.exchange.ws_manager import WSManager


class _FakeTransport:
    """Hand-driven private socket: captures sends, optional scripted recv frames."""

    def __init__(self, incoming: list[dict] | None = None) -> None:
        self.incoming = list(incoming or [])
        self.sent: list[dict] = []
        self.closed = False

    async def send(self, message: dict) -> None:
        self.sent.append(message)

    async def recv(self) -> dict:
        if self.incoming:
            return self.incoming.pop(0)
        raise TransportClosed("drained")

    async def close(self) -> None:
        self.closed = True


async def _noop_sleep(_seconds: float) -> None:
    return None


def _fill(symbol: str, side: str, qty: str, price: str) -> dict:
    return {"exec_type": "trade", "symbol": symbol, "side": side,
            "cum_qty": qty, "avg_price": price}


# -- subscribe frames: the mandatory flags ------------------------------

def test_executions_subscribe_mandatory_flags():
    rpc = executions_subscribe("TOKEN123")
    p = rpc["params"]
    assert rpc["method"] == "subscribe"
    assert p["channel"] == "executions"
    assert p["token"] == "TOKEN123"
    assert p["order_status"] is True   # rule:HR-WM-005 MANDATORY
    assert p["snap_orders"] is True    # AR-056 reconcile snapshot
    assert p["ratecounter"] is True    # A-1 / AR-030 maxratecount


def test_balances_subscribe_carries_token():
    rpc = balances_subscribe("TOKEN123")
    assert rpc["params"] == {"channel": "balances", "token": "TOKEN123"}


# -- live-only guard (PA-004 div #1 / HR-WM-022) ------------------------

def test_private_connection_rejected_in_paper():
    m = WSManager(Mode.PAPER)
    with pytest.raises(ValueError):
        PrivateConnectionAssembler(
            m, open_socket=lambda: _FakeTransport(), acquire_token=lambda: "t"
        )


# -- build: token -> open -> bind transmitter -> subscribe --------------

def test_build_subscribes_and_binds_transmitter():
    m = WSManager(Mode.LIVE)
    t = _FakeTransport()
    tokens = ["WSTOKEN"]

    async def _open():
        return t

    async def _acquire():
        return tokens[0]

    asm = PrivateConnectionAssembler(m, open_socket=_open, acquire_token=_acquire)
    pc = asyncio.run(asm.build())

    # executions + balances subscribed with the fresh token, on the opened socket.
    assert t.sent == [executions_subscribe("WSTOKEN"), balances_subscribe("WSTOKEN")]
    # the live transmitter now targets this socket (ws_private.send works)
    assert m.transmitter.is_connected is True
    asyncio.run(m.transmitter(_op(), {"hello": "kraken"}))
    assert t.sent[-1] == {"hello": "kraken"}
    assert pc.transport is t


def _op():
    from tothbot.exchange.seam import OutboundOp
    return OutboundOp.ADD_ORDER


# -- inbound fill -> mirror (closes the live loop) ----------------------

def test_executions_update_routes_to_record_execution():
    m = WSManager(Mode.LIVE)
    asm = PrivateConnectionAssembler(
        m, open_socket=lambda: _FakeTransport(), acquire_token=lambda: "t"
    )
    # route an executions update frame as the dispatch table would deliver it
    asm.ingest({"channel": "executions", "type": "update",
                "data": [_fill("BTC/USD", "buy", "0.5", "60000")]})
    assert m.has_position("BTC/USD")
    assert m.position("BTC/USD").avg_entry_price == Decimal("60000")


def test_executions_snapshot_reconciles_and_captures():
    m = WSManager(Mode.LIVE)
    # open two positions via fills
    m.record_execution(_fill("BTC/USD", "buy", "0.5", "60000"))
    m.record_execution(_fill("ETH/USD", "sell", "2", "3000"))
    asm = PrivateConnectionAssembler(
        m, open_socket=lambda: _FakeTransport(), acquire_token=lambda: "t"
    )
    events: list = []
    asm.ingest._on_event = events.append
    # snapshot shows only BTC/USD open -> ETH/USD closed during the gap (AR-056)
    asm.ingest({"channel": "executions", "type": "snapshot",
                "data": [{"symbol": "BTC/USD", "order_id": "O1"}]})
    assert m.open_position_symbols() == frozenset({"BTC/USD"})
    assert asm.ingest.last_snap_orders == [{"symbol": "BTC/USD", "order_id": "O1"}]
    assert any(isinstance(e, PositionMirrorRestored) and e.gap_closed == 1 for e in events)


# -- end to end: a fill frame through the receive loop opens the mirror --

def test_fill_frame_through_receive_loop_opens_mirror():
    m = WSManager(Mode.LIVE)
    t = _FakeTransport()

    async def _open():
        return t

    async def _acquire():
        return "t"

    asm = PrivateConnectionAssembler(m, open_socket=_open, acquire_token=_acquire)
    pc = asyncio.run(asm.build())
    # Kraken pushes an executions update frame; the loop routes it to the ingest,
    # which feeds record_execution (the mirror sole writer). Loop logic is sync.
    pc.loop.handle_message(
        {"channel": "executions", "type": "update",
         "data": [_fill("SOL/USD", "buy", "10", "150")]},
        now=0.0,
    )
    assert m.has_position("SOL/USD")
    assert m.position("SOL/USD").side.value == "long"


# -- reconnect: RESTORE_POSITION_MIRROR wired to restore_position_mirror -

def test_reconnect_runs_private_restore_and_restores_mirror():
    m = WSManager(Mode.LIVE)
    m.record_execution(_fill("BTC/USD", "buy", "0.5", "60000"))
    m.record_execution(_fill("ETH/USD", "sell", "2", "3000"))

    sockets = [_FakeTransport(), _FakeTransport()]  # initial + reconnect
    opened: list[_FakeTransport] = []
    token_calls = {"n": 0}

    async def _open():
        t = sockets[len(opened)]
        opened.append(t)
        return t

    async def _acquire():
        token_calls["n"] += 1
        return f"TOK{token_calls['n']}"

    async def _snap():
        # REST GetOpenOrders on reconnect: only BTC/USD still open
        return [{"symbol": "BTC/USD", "order_id": "O1"}]

    asm = PrivateConnectionAssembler(
        m, open_socket=_open, acquire_token=_acquire, fetch_snap_orders=_snap,
        sleep=_noop_sleep,
    )
    pc = asyncio.run(asm.build())
    assert token_calls["n"] == 1
    assert m.transmitter.is_connected and pc.transport is sockets[0]

    # drive one reconnect (Scenario A): the private restore subset runs end to end
    fresh = asyncio.run(pc.driver.initiate(0, _random_reason()))
    assert fresh is sockets[1]
    # fresh token re-acquired (never reused), transmitter re-bound to the new socket
    assert token_calls["n"] == 2
    assert m.transmitter.is_connected is True
    asyncio.run(m.transmitter(_op(), {"probe": 1}))
    assert sockets[1].sent[-1] == {"probe": 1}
    # private re-subscribe issued on the fresh socket with the fresh token
    assert executions_subscribe("TOK2") in sockets[1].sent
    assert balances_subscribe("TOK2") in sockets[1].sent
    # RESTORE_POSITION_MIRROR reconciled against snap_orders -> ETH/USD gap-closed
    assert m.open_position_symbols() == frozenset({"BTC/USD"})


def _random_reason():
    from tothbot.exchange.reconnect import DisconnectReason
    return DisconnectReason.RANDOM


# -- the private restore subset excludes public-channel steps -----------

def test_private_restore_sequence_is_private_subset():
    seq = build_private_restore_sequence()
    assert RestoreStep.RESTORE_POSITION_MIRROR in seq
    assert RestoreStep.RESUBSCRIBE_PRIVATE in seq
    assert RestoreStep.ACQUIRE_WS_TOKEN in seq
    # public-channel steps never run on the private connection
    assert RestoreStep.RESUBSCRIBE_PUBLIC not in seq
    assert RestoreStep.RESTORE_TICKER_TRIGGER not in seq
    # figure order preserved: token before socket before resubscribe before restore
    assert seq.index(RestoreStep.ACQUIRE_WS_TOKEN) < seq.index(RestoreStep.RECONNECT_SOCKET)
    assert seq.index(RestoreStep.RESUBSCRIBE_PRIVATE) < seq.index(RestoreStep.RESTORE_POSITION_MIRROR)


# -- the private connection satisfies the Transport protocol at its edge --

def test_fake_transport_is_transport():
    assert isinstance(_FakeTransport(), Transport)
