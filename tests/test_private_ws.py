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
from tothbot.exchange.rate_counter import (
    MaxRateCountSet,
    RateCounterUpdate,
    RateCounterWarning,
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


# -- ar:AR-030 rate counter: the executions ACK sets the operative ceiling --

def _exec_ack(maxratecount: int) -> dict:
    """An executions subscribe ACK carrying maxratecount (AR-030)."""
    return {"method": "subscribe", "success": True,
            "result": {"channel": "executions", "maxratecount": maxratecount}}


def _rate_fill(symbol: str, ratecount: int) -> dict:
    """A trade execution carrying the ratecounter:true per-pair counter (A-1)."""
    return {"exec_type": "trade", "symbol": symbol, "side": "buy",
            "cum_qty": "0.5", "avg_price": "60000", "ratecount": ratecount}


def _build_live(events: list):
    m = WSManager(Mode.LIVE)
    t = _FakeTransport()

    async def _open():
        return t

    async def _acquire():
        return "t"

    asm = PrivateConnectionAssembler(
        m, open_socket=_open, acquire_token=_acquire, on_event=events.append
    )
    return asm, asyncio.run(asm.build())


def test_executions_ack_sets_ceiling_and_emits_maxratecount_set():
    events: list = []
    asm, pc = _build_live(events)
    # the executions ACK (maxratecount=125, NOT the hardcoded literal) flows through the loop
    pc.loop.handle_message(_exec_ack(125), now=0.0)
    assert asm.rate_counter.ceiling == 125
    assert any(isinstance(e, MaxRateCountSet) and e.value == 125 for e in events)


def test_non_executions_ack_does_not_set_ceiling():
    events: list = []
    asm, pc = _build_live(events)
    # a balances ACK carries no maxratecount -> the ceiling stays unset (never assume 125)
    pc.loop.handle_message(
        {"method": "subscribe", "success": True, "result": {"channel": "balances"}}, now=0.0
    )
    assert asm.rate_counter.ceiling is None
    assert not any(isinstance(e, MaxRateCountSet) for e in events)


def test_executions_frame_ratecount_emits_update_and_warning():
    events: list = []
    asm, pc = _build_live(events)
    pc.loop.handle_message(_exec_ack(100), now=0.0)         # ceiling = 100
    # a fill carrying ratecount=85 (85% > the 80% warning fraction)
    pc.loop.handle_message(
        {"channel": "executions", "type": "update", "data": [_rate_fill("BTC/USD", 85)]},
        now=1.0,
    )
    updates = [e for e in events if isinstance(e, RateCounterUpdate)]
    warns = [e for e in events if isinstance(e, RateCounterWarning)]
    assert any(u.symbol == "BTC/USD" and u.value == 85 and u.maxratecount == 100 for u in updates)
    assert any(w.symbol == "BTC/USD" and w.value == 85 for w in warns)
    # the fill still reached the mirror (rate-counter feeding does not disturb routing)
    assert m_has(pc, "BTC/USD")


def m_has(pc, symbol: str) -> bool:
    return pc.ingest._wm.has_position(symbol)


def test_reconnect_reset_rate_ceiling_clears_stale_counters():
    m = WSManager(Mode.LIVE)
    sockets = [_FakeTransport(), _FakeTransport()]
    opened: list[_FakeTransport] = []

    async def _open():
        t = sockets[len(opened)]
        opened.append(t)
        return t

    async def _acquire():
        return "TOK"

    async def _snap():
        return []

    events: list = []
    asm = PrivateConnectionAssembler(
        m, open_socket=_open, acquire_token=_acquire, fetch_snap_orders=_snap,
        sleep=_noop_sleep, on_event=events.append,
    )
    pc = asyncio.run(asm.build())
    # set a ceiling + drive a pair into entry suppression (over the 95% critical fraction)
    pc.loop.handle_message(_exec_ack(100), now=0.0)
    pc.loop.handle_message(
        {"channel": "executions", "type": "update", "data": [_rate_fill("BTC/USD", 99)]},
        now=1.0,
    )
    assert asm.rate_counter.is_entry_suppressed("BTC/USD") is True

    # a reconnect runs RESET_RATE_CEILING -> the stale per-pair counters + latches drop
    asyncio.run(pc.driver.initiate(0, _random_reason()))
    assert asm.rate_counter.value("BTC/USD") is None
    assert asm.rate_counter.is_entry_suppressed("BTC/USD") is False
    # the ceiling is kept provisional until the fresh executions ACK re-sets it
    assert asm.rate_counter.ceiling == 100
    pc.loop.handle_message(_exec_ack(150), now=2.0)  # the fresh ACK after resubscribe
    assert asm.rate_counter.ceiling == 150


# -- the private connection satisfies the Transport protocol at its edge --

def test_fake_transport_is_transport():
    assert isinstance(_FakeTransport(), Transport)
