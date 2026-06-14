"""S2-integration tests: the per-shard receive loop (receive_loop.py).

Covers 0500000 dv1_241 sec 2 Image1 + sec 7 mod:WS_Manager: frame classification,
the keepalive / silent-pair / reconcile policy units driven at the socket edge, the
rule:HR-WM-012 candle-discard gate, the WS-REC-003 Scenario A/B reason selection,
and the canonical event_registry emissions. Sync logic (handle_message/on_tick) is
tested directly; the async shell (_step) is driven with stdlib asyncio.run() over a
hand-built fake Transport - no network, no pytest-asyncio.
"""

from __future__ import annotations

import asyncio

import pytest

from tothbot.exchange.channels import PrivateChannel, PublicChannel
from tothbot.exchange.dispatch import DispatchTable
from tothbot.exchange.keepalive import (
    PING_MESSAGE,
    PING_INTERVAL_SEC,
    PONG_TIMEOUT_SEC,
    ZOMBIE_THRESHOLD_SEC,
    ConnectionKeepalive,
)
from tothbot.exchange.reconcile import ReconciliationTracker, SequenceGap
from tothbot.exchange.reconnect import DisconnectReason
from tothbot.exchange.receive_loop import (
    MessageClass,
    PingSent,
    PingTimeout,
    PongReceived,
    ShardReceiveLoop,
    SubscriptionAck,
    UnknownMessage,
    WsConnected,
    ZombieDetected,
    classify,
)
from tothbot.exchange.silent_pair import (
    PairDataState,
    SilentPairEvent,
    SilentPairMachine,
)
from tothbot.exchange.transport import TransportClosed

T0 = 1_000.0  # a fixed monotonic baseline for deterministic timer math


def _keepalive() -> ConnectionKeepalive:
    # Fixed clock so the reset() baseline is deterministic; tests pass now explicitly.
    return ConnectionKeepalive(clock=lambda: T0)


# -- classify (pure) ----------------------------------------------------

def test_classify_all_envelopes():
    assert classify({"method": "pong", "req_id": 1}) is MessageClass.PONG
    assert classify({"method": "subscribe", "success": True}) is MessageClass.SUBSCRIBE_ACK
    assert classify({"channel": "heartbeat"}) is MessageClass.HEARTBEAT
    assert classify({"channel": "status", "data": []}) is MessageClass.STATUS
    assert classify({"channel": "ohlc", "data": []}) is MessageClass.CHANNEL_DATA
    assert classify({"foo": "bar"}) is MessageClass.UNKNOWN


# -- handle_message: pong / heartbeat / zombie reset --------------------

def test_pong_clears_outstanding_ping_and_emits():
    ka = _keepalive()
    ka.mark_ping_sent(T0)
    assert ka.pong_overdue(T0 + PONG_TIMEOUT_SEC) is True
    events: list = []
    loop = ShardReceiveLoop(_FakeTransport(), DispatchTable(), ka, on_event=events.append)
    loop.handle_message({"method": "pong"}, T0 + 1)
    assert ka.pong_overdue(T0 + PONG_TIMEOUT_SEC) is False
    assert events == [PongReceived(None)]


def test_heartbeat_does_not_reset_zombie_timer():
    ka = _keepalive()
    loop = ShardReceiveLoop(_FakeTransport(), DispatchTable(), ka)
    # 80 s of silence, then a heartbeat: the zombie timer must NOT be reset (AR-042).
    loop.handle_message({"channel": "heartbeat"}, T0 + 80)
    assert ka.seconds_since_real_data(T0 + 80) == pytest.approx(80.0)


def test_channel_data_resets_zombie_and_routes():
    ka = _keepalive()
    table = DispatchTable()
    got: list = []
    table.register(PublicChannel.OHLC_5M, got.append)
    loop = ShardReceiveLoop(_FakeTransport(), table, ka)
    frame = {"channel": "ohlc", "type": "update",
             "data": [{"symbol": "BTC/USD", "interval": 5, "close": "1"}]}
    loop.handle_message(frame, T0 + 50)
    assert ka.seconds_since_real_data(T0 + 50) == pytest.approx(0.0)  # real data reset it
    assert got == [frame]  # routed to the ohlc_5m handler


# -- handle_message: status (connection_id + maintenance) ---------------

def test_status_adopts_connection_id_and_routes():
    table = DispatchTable()
    got: list = []
    table.register(PublicChannel.STATUS, got.append)
    events: list = []
    loop = ShardReceiveLoop(_FakeTransport(), table, _keepalive(), on_event=events.append)
    frame = {"channel": "status", "type": "update",
             "data": [{"connection_id": 99, "system": "online"}]}
    loop.handle_message(frame, T0)
    assert loop.connection_id == 99
    assert events == [WsConnected(99)]
    assert got == [frame]  # also routed to the status consumer


def test_status_maintenance_arms_scenario_b():
    loop = ShardReceiveLoop(_FakeTransport(), DispatchTable(), _keepalive())
    loop.handle_message({"channel": "status", "data": [{"system": "maintenance"}]}, T0)
    assert loop._reason_for_drop() is DisconnectReason.MAINTENANCE
    loop.handle_message({"channel": "status", "data": [{"system": "online"}]}, T0)
    assert loop._reason_for_drop() is DisconnectReason.RANDOM


def test_status_does_not_reset_zombie_timer():
    ka = _keepalive()
    loop = ShardReceiveLoop(_FakeTransport(), DispatchTable(), ka)
    loop.handle_message({"channel": "status", "data": [{"system": "online"}]}, T0 + 70)
    assert ka.seconds_since_real_data(T0 + 70) == pytest.approx(70.0)


# -- handle_message: subscribe ACK arms the silent-pair timer -----------

def test_subscribe_ack_marks_subscribed_and_surfaces_warnings():
    machine = SilentPairMachine(clock=lambda: T0)
    events: list = []
    loop = ShardReceiveLoop(
        _FakeTransport(), DispatchTable(), _keepalive(),
        silent_pairs={"BTC/USD": machine}, on_event=events.append,
    )
    ack = {"method": "subscribe", "success": True,
           "result": {"channel": "ohlc", "symbol": "BTC/USD",
                      "warnings": ["timestamp is deprecated, use interval_begin"]}}
    loop.handle_message(ack, T0)
    assert machine.state is PairDataState.SUBSCRIBED
    assert events == [SubscriptionAck("ohlc", "BTC/USD", True,
                                      ("timestamp is deprecated, use interval_begin",))]


# -- handle_message: silent-pair first-data + recovery ------------------

def test_silent_pair_first_data_then_recovery():
    machine = SilentPairMachine(clock=lambda: T0)
    machine.mark_subscribed(T0)
    events: list = []
    loop = ShardReceiveLoop(
        _FakeTransport(), DispatchTable(), _keepalive(),
        silent_pairs={"BTC/USD": machine}, on_event=events.append,
    )
    table = loop._dispatch
    table.register(PublicChannel.TICKER, lambda f: None)
    # No event on the first-data SUBSCRIBED -> DATA_READY path.
    loop.handle_message({"channel": "ticker", "data": [{"symbol": "BTC/USD"}]}, T0 + 1)
    assert machine.state is PairDataState.DATA_READY
    assert events == []
    # Drive it to DATA_PENDING via a tick, then recover on the next data frame.
    machine.mark_subscribed(T0 + 10)
    loop.on_tick(T0 + 10 + 61)  # > T_silent
    assert machine.state is PairDataState.DATA_PENDING
    assert events[-1] is SilentPairEvent.DATA_PENDING
    loop.handle_message({"channel": "ticker", "data": [{"symbol": "BTC/USD"}]}, T0 + 100)
    assert machine.state is PairDataState.DATA_READY
    assert events[-1] is SilentPairEvent.DATA_READY_RECOVERED


# -- handle_message: private-channel sequence-gap detection -------------

def test_executions_sequence_gap_emitted():
    recon = ReconciliationTracker()
    events: list = []
    table = DispatchTable()
    table.register(PrivateChannel.EXECUTIONS, lambda f: None)
    loop = ShardReceiveLoop(
        _FakeTransport(), table, _keepalive(), recon=recon, on_event=events.append,
    )
    loop.handle_message({"channel": "executions", "data": [], "sequence": 1}, T0)  # baseline
    loop.handle_message({"channel": "executions", "data": [], "sequence": 4}, T0)  # gap
    gaps = [e for e in events if isinstance(e, SequenceGap)]
    assert len(gaps) == 1
    assert gaps[0].channel.value == "executions"
    assert gaps[0].missed == 2
    assert gaps[0].recovery_endpoint == "GetOpenOrders"


# -- handle_message: unknown frame never silently dropped ---------------

def test_unknown_channel_data_emits_unknown_message():
    events: list = []
    loop = ShardReceiveLoop(_FakeTransport(), DispatchTable(), _keepalive(), on_event=events.append)
    loop.handle_message({"channel": "orderbook", "data": []}, T0)
    assert events == [UnknownMessage("orderbook")]


def test_unknown_envelope_emits_unknown_message():
    events: list = []
    loop = ShardReceiveLoop(_FakeTransport(), DispatchTable(), _keepalive(), on_event=events.append)
    loop.handle_message({"weird": 1}, T0)
    assert isinstance(events[0], UnknownMessage)


# -- HR-WM-012: candle discarded while any shard is reconnecting --------

def test_candle_discarded_during_reconnect_but_liveness_still_applied():
    ka = _keepalive()
    table = DispatchTable()
    got: list = []
    table.register(PublicChannel.OHLC_5M, got.append)
    machine = SilentPairMachine(clock=lambda: T0)
    machine.mark_subscribed(T0)
    loop = ShardReceiveLoop(
        _FakeTransport(), table, ka,
        silent_pairs={"BTC/USD": machine},
        is_reconnecting=lambda: True,  # a shard is mid-reconnect
    )
    frame = {"channel": "ohlc", "type": "update",
             "data": [{"symbol": "BTC/USD", "interval": 5, "close": "1"}]}
    loop.handle_message(frame, T0 + 5)
    assert got == []  # HR-WM-012: pipeline must not fire on a partial universe
    assert ka.seconds_since_real_data(T0 + 5) == pytest.approx(0.0)  # zombie still reset
    assert machine.state is PairDataState.DATA_READY  # silent-pair still advanced


def test_non_candle_data_still_routes_during_reconnect():
    table = DispatchTable()
    got: list = []
    table.register(PublicChannel.TICKER, got.append)
    loop = ShardReceiveLoop(
        _FakeTransport(), table, _keepalive(), is_reconnecting=lambda: True,
    )
    frame = {"channel": "ticker", "data": [{"symbol": "BTC/USD"}]}
    loop.handle_message(frame, T0)
    assert got == [frame]  # only the ohlc_5m system clock is gated by HR-WM-012


# -- on_tick: ping scheduling + dead/zombie verdicts --------------------

def test_tick_schedules_ping_when_due():
    ka = _keepalive()
    loop = ShardReceiveLoop(_FakeTransport(), DispatchTable(), ka)
    assert loop.on_tick(T0 + PING_INTERVAL_SEC).send_ping is True
    assert loop.on_tick(T0 + 1).send_ping is False  # not yet due


def test_tick_dead_connection_forces_reconnect():
    ka = _keepalive()
    ka.mark_ping_sent(T0)
    events: list = []
    loop = ShardReceiveLoop(_FakeTransport(), DispatchTable(), ka, on_event=events.append)
    action = loop.on_tick(T0 + PONG_TIMEOUT_SEC + 0.1)  # no pong within 10 s
    assert action.reconnect_reason is DisconnectReason.RANDOM
    assert any(isinstance(e, PingTimeout) for e in events)


def test_tick_zombie_forces_reconnect():
    ka = _keepalive()
    events: list = []
    loop = ShardReceiveLoop(_FakeTransport(), DispatchTable(), ka, on_event=events.append)
    action = loop.on_tick(T0 + ZOMBIE_THRESHOLD_SEC + 1)  # no real data > 90 s
    assert action.reconnect_reason is DisconnectReason.RANDOM
    assert any(isinstance(e, ZombieDetected) for e in events)


# -- async shell (_step) ------------------------------------------------

class _FakeTransport:
    """Hand-driven Transport: scripted recv outcomes + captured sends."""

    def __init__(self, *, recv_script: list | None = None, url: str | None = None) -> None:
        self._recv_script = list(recv_script or [])
        self.sent: list[dict] = []
        self.closed = False
        self.url = url

    async def recv(self) -> dict:
        if not self._recv_script:
            raise asyncio.TimeoutError  # idle: behaves like a wait_for timeout tick
        outcome = self._recv_script.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        return outcome

    async def send(self, message: dict) -> None:
        self.sent.append(message)

    async def close(self) -> None:
        self.closed = True


def test_step_handles_a_received_frame():
    ka = _keepalive()
    transport = _FakeTransport(recv_script=[{"method": "pong"}])
    events: list = []
    loop = ShardReceiveLoop(
        transport, DispatchTable(), ka, on_event=events.append, clock=lambda: T0 + 1,
    )
    ka.mark_ping_sent(T0)
    asyncio.run(loop._step())
    assert any(isinstance(e, PongReceived) for e in events)
    assert ka.pong_overdue(T0 + PONG_TIMEOUT_SEC) is False  # pong cleared it


def test_step_sends_ping_on_idle_tick_when_due():
    ka = _keepalive()
    transport = _FakeTransport()  # recv raises TimeoutError -> idle tick
    loop = ShardReceiveLoop(
        transport, DispatchTable(), ka, clock=lambda: T0 + PING_INTERVAL_SEC,
    )
    asyncio.run(loop._step())
    assert transport.sent == [dict(PING_MESSAGE)]
    assert ka.due_for_ping(T0 + PING_INTERVAL_SEC) is False  # ping now in flight


def test_step_reconnects_and_swaps_transport_on_drop():
    ka = _keepalive()
    dropped = _FakeTransport(recv_script=[TransportClosed("1006")])
    fresh = _FakeTransport(url="wss://ws.kraken.com/v2")
    reasons: list = []

    async def fake_initiate(reason):
        reasons.append(reason)
        return fresh

    loop = ShardReceiveLoop(
        dropped, DispatchTable(), ka, initiate_reconnect=fake_initiate, clock=lambda: T0,
    )
    asyncio.run(loop._step())
    assert dropped.closed is True               # dead socket torn down
    assert reasons == [DisconnectReason.RANDOM]  # transient drop -> Scenario A
    assert loop.transport is fresh               # now reading from the restored socket


def test_step_drop_after_maintenance_is_scenario_b():
    ka = _keepalive()
    dropped = _FakeTransport(recv_script=[TransportClosed("engine maintenance")])
    fresh = _FakeTransport()
    reasons: list = []

    async def fake_initiate(reason):
        reasons.append(reason)
        return fresh

    loop = ShardReceiveLoop(
        dropped, DispatchTable(), ka, initiate_reconnect=fake_initiate, clock=lambda: T0,
    )
    loop._maintenance_announced = True  # a status frame announced maintenance
    asyncio.run(loop._step())
    assert reasons == [DisconnectReason.MAINTENANCE]  # WS-REC-003 Scenario B
    assert loop._maintenance_announced is False        # flag consumed
