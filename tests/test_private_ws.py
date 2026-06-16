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
    OrderAckHandler,
    OrderRejected,
    PositionMirrorRestored,
    PrivateConnectionAssembler,
    _response_cl_ord_ids,
    balances_subscribe,
    executions_subscribe,
)
from tothbot.exchange.rate_counter import (
    MaxRateCountSet,
    RateCounterUpdate,
    RateCounterWarning,
)
from tothbot.exchange.position_mirror import Position, PositionClosedDuringGap, PositionSide
from tothbot.exchange.transport import Transport, TransportClosed
from tothbot.exchange.ws_manager import GapCloseEstimated, WSManager


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


# -- I-6: an executions cancel-confirm feeds the cancel-ACK registry --------

def test_executions_cancel_confirm_records_cancel_ack():
    m = WSManager(Mode.LIVE)
    asm = PrivateConnectionAssembler(
        m, open_socket=lambda: _FakeTransport(), acquire_token=lambda: "t"
    )
    asm.ingest({"channel": "executions", "type": "update",
                "data": [{"exec_type": "canceled", "cl_ord_id": "cl-1-sl"}]})
    assert "cl-1-sl" in m._cancel_acks   # the I-6 "confirmed -> proceed" branch can now read it


def test_executions_canceled_without_cl_ord_id_records_nothing():
    m = WSManager(Mode.LIVE)
    asm = PrivateConnectionAssembler(
        m, open_socket=lambda: _FakeTransport(), acquire_token=lambda: "t"
    )
    asm.ingest({"channel": "executions", "type": "update",
                "data": [{"exec_type": "canceled"}]})
    assert m._cancel_acks == set()


# -- the C-1 order-ack reject handler (add_order RESPONSE -> reject registry) --

def test_response_cl_ord_ids_from_result_dict_list_and_top_level():
    assert _response_cl_ord_ids({"result": {"cl_ord_id": "A"}}) == ["A"]
    assert _response_cl_ord_ids({"result": [{"cl_ord_id": "A"}, {"cl_ord_id": "B"}]}) == ["A", "B"]
    assert _response_cl_ord_ids({"cl_ord_id": "C"}) == ["C"]   # top-level fallback
    assert _response_cl_ord_ids({"result": {}}) == []


def test_order_ack_handler_records_reject_and_surfaces_event():
    m = WSManager(Mode.LIVE)
    events: list = []
    handler = OrderAckHandler(m, on_event=events.append)
    handler({"method": "add_order", "success": False, "error": "EOrder:MPP",
             "result": {"cl_ord_id": "X-1"}})
    assert m._order_responses["X-1"] is True   # the C-1 reject probe will read True
    assert any(isinstance(e, OrderRejected) and e.cl_ord_id == "X-1" for e in events)


def test_order_ack_handler_records_accept_without_event():
    m = WSManager(Mode.LIVE)
    events: list = []
    handler = OrderAckHandler(m, on_event=events.append)
    handler({"method": "add_order", "success": True, "result": {"cl_ord_id": "X-2"}})
    assert m._order_responses["X-2"] is False   # accept -> the probe short-circuits to not-rejected
    assert not events


def test_order_ack_handler_ignores_cancel_order_response():
    m = WSManager(Mode.LIVE)
    handler = OrderAckHandler(m)
    handler({"method": "cancel_order", "success": True, "result": {"cl_ord_id": "cl-1-sl"}})
    assert m._order_responses == {}   # cancel ACK is sourced from executions, not here (I-6)


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


# -- ar:AR-056 gap-close: the WS-REC-004 restore emits TRADE_CLOSE w/ the QueryOrders fill --

from types import SimpleNamespace  # noqa: E402


class _GapRecordingWM:
    """A live wm stand-in: returns scripted gaps from restore_position_mirror + records each
    on_reconnect_gap_close (symbol, exit_price, fees_exit) so the actual-vs-degraded fill is asserted."""

    is_live = True

    def __init__(self, gaps) -> None:
        self._gaps = list(gaps)
        self.transmitter = SimpleNamespace(bind=lambda _t: None)
        self.gap_calls: list = []

    def restore_position_mirror(self, _snap):
        return list(self._gaps)

    def on_reconnect_gap_close(self, gap, *, exit_price=None, fees_exit=None):
        self.gap_calls.append((gap.symbol, exit_price, fees_exit))


def _gap(symbol="ETH/USD", emergsl_id="OEMERG", cl_ord_id="cl-e1"):
    pos = Position(symbol=symbol, side=PositionSide.SHORT, qty=Decimal("2"),
                   avg_entry_price=Decimal("3000"), cl_ord_id=cl_ord_id,
                   emergsl_id=emergsl_id, emergsl_price=Decimal("3100"))
    return PositionClosedDuringGap(symbol, pos)


async def _open_noop():
    return _FakeTransport()


async def _tok():
    return "t"


async def _one_snap():
    return [{"symbol": "BTC/USD", "order_id": "O1"}]


def test_reconnect_gap_close_uses_actual_query_orders_fill():
    wm = _GapRecordingWM([_gap()])

    async def _q(txids):
        assert "OEMERG" in txids   # the off-book emergSL leg queried first (REST-QOI-002)
        return {"OEMERG": {"status": "closed", "vol_exec": Decimal("2"),
                           "price": Decimal("3050"), "fee": Decimal("1.2")}}

    asm = PrivateConnectionAssembler(
        wm, open_socket=_open_noop, acquire_token=_tok,
        fetch_snap_orders=_one_snap, query_orders=_q, sleep=_noop_sleep,
    )
    asyncio.run(asm._restore_position_mirror())
    # FEE-CALC-006 record-of-truth: the ACTUAL close fill avg_price + fee (not the estimate).
    assert wm.gap_calls == [("ETH/USD", Decimal("3050"), Decimal("1.2"))]


def test_reconnect_gap_close_degraded_when_no_query_orders_edge():
    wm = _GapRecordingWM([_gap()])
    asm = PrivateConnectionAssembler(
        wm, open_socket=_open_noop, acquire_token=_tok,
        fetch_snap_orders=_one_snap, sleep=_noop_sleep,
    )
    asyncio.run(asm._restore_position_mirror())
    assert wm.gap_calls == [("ETH/USD", None, None)]   # degraded -> entry-time estimate path


def test_reconnect_gap_close_degraded_when_query_returns_no_closed_fill():
    wm = _GapRecordingWM([_gap()])

    async def _q(txids):
        return {"OEMERG": {"status": "open", "vol_exec": Decimal("0"), "price": Decimal("3100")}}

    asm = PrivateConnectionAssembler(
        wm, open_socket=_open_noop, acquire_token=_tok,
        fetch_snap_orders=_one_snap, query_orders=_q, sleep=_noop_sleep,
    )
    asyncio.run(asm._restore_position_mirror())
    assert wm.gap_calls == [("ETH/USD", None, None)]   # no closed fill -> degraded


class _IdleTransport(_FakeTransport):
    """A private socket that captures sends but whose recv never returns (so the receive loop's
    wait_for times out each tick) - lets a single _step exercise the idle-tick after_batch pump."""

    async def recv(self) -> dict:
        await asyncio.Event().wait()  # never set -> wait_for(recv, tick_interval) times out
        raise AssertionError  # pragma: no cover


async def _always_ack(cl_ord_id, timeout):
    return True


async def _never_reject(message):
    return False


def test_live_loop_pump_dispatches_a_detected_exit_end_to_end():
    # The capstone: the WIRED private loop, on an idle tick, PUMPS drive_live_exits so a detected L2
    # MAE intent (enqueued by handle_ticker) dispatches its cancel-then-sell over THIS bound socket.
    m = WSManager(Mode.LIVE, cancel_ack_wait=_always_ack, market_rejected=_never_reject)
    t = _IdleTransport()

    async def _open():
        return t

    asm = PrivateConnectionAssembler(
        m, open_socket=_open, acquire_token=_tok, sleep=_noop_sleep, tick_interval=0.01
    )
    pc = asyncio.run(asm.build())
    # open a live long (the L2 detector reads the entry-time atr) and detect an adverse breach
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "buy", "cum_qty": "0.05",
         "avg_price": "60000", "cl_ord_id": "cl-1", "fee": "7.8"},
        atr_14_entry="2000", emergsl_price="54000",
    )
    m.handle_ticker({"channel": "ticker", "type": "update",
                     "data": [{"symbol": "BTC/USD", "bid": "57000", "ask": "57100"}]})  # MAE 3000>=3000
    assert m.live_exit_intents_pending == 1

    # drive ONE loop step: recv idles -> the after_batch pump drains drive_live_exits.
    asyncio.run(pc.loop._step())
    assert m.live_exit_intents_pending == 0                  # the intent was dispatched
    methods = [msg.get("method") for msg in t.sent]
    assert "cancel_order" in methods                          # (1) cancel the off-book emergSL
    assert "add_order" in methods                             # (2) the market close, over the bound socket


def test_live_loop_pump_places_on_fill_emergsl_end_to_end():
    # The ENTRY capstone (slices a/b/c): the WIRED private loop drives a live ENTRY end-to-end - the
    # marketable-IOC add_order transmits over the bound socket, the IOC fills LATER on the executions
    # channel, and ONE idle _step runs the after_batch pump (drive_after_batch) which places the
    # AR-054 on-fill emergSL over the same bound socket. NOTHING connects to Kraken for real.
    m = WSManager(Mode.LIVE)
    t = _IdleTransport()

    async def _open():
        return t

    asm = PrivateConnectionAssembler(
        m, open_socket=_open, acquire_token=_tok, sleep=_noop_sleep, tick_interval=0.01
    )
    pc = asyncio.run(asm.build())
    t.sent.clear()   # drop the startup subscribe frames
    # (1) the live entry dispatches its marketable-IOC add_order over the bound socket. The RL-MON-003
    #     gate (bound to the rate counter in build) is quiet - no rate pressure -> the entry proceeds.
    dispatched = asyncio.run(m.dispatch_entry(
        PositionSide.LONG, "BTC/USD",
        order_qty="0.05", entry_limit_price="60000", emergsl_price="54000",
        atr_14_entry="2000", cl_ord_id="cl-1", deadline="d",
    ))
    assert dispatched is True
    assert [msg.get("method") for msg in t.sent] == ["add_order"]
    # (2) the IOC fills LATER on the executions channel -> OPENED attaches the D6 snapshot stashed at
    #     dispatch (no emergsl_price passed here) and enqueues the on-fill emergSL.
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "buy", "cum_qty": "0.05",
         "avg_price": "60000", "cl_ord_id": "cl-1", "fee": "7.8"},
    )
    assert len(m._pending_emergsl) == 1
    t.sent.clear()
    # (3) ONE idle loop step runs the after_batch pump -> the emergSL batch_add over the bound socket,
    #     in the same private step as the fill (the position is never left unprotected).
    asyncio.run(pc.loop._step())
    assert m._pending_emergsl == []
    leg = t.sent[-1]["params"]["orders"][0]
    assert t.sent[-1]["method"] == "batch_add"
    assert leg["side"] == "sell" and leg["triggers"]["price"] == "54000"   # LONG sell-stop below entry


def test_executions_snapshot_emits_gap_close_estimated_on_the_sync_path():
    events: list = []
    m = WSManager(Mode.LIVE, on_event=events.append)
    # an open SHORT carrying the entry-time emergsl_price (the degraded estimate basis)
    m.record_execution(
        {"exec_type": "filled", "symbol": "ETH/USD", "side": "sell", "cum_qty": "2",
         "avg_price": "3000", "cl_ord_id": "cl-e1", "fee": "1.0"},
        emergsl_price="3100", atr_14_entry="50",
    )
    asm = PrivateConnectionAssembler(
        m, open_socket=_open_noop, acquire_token=_tok, sleep=_noop_sleep
    )
    # the re-subscribe snapshot frame shows ETH/USD gone -> gap-closed during the disconnect
    asm.ingest({"channel": "executions", "type": "snapshot",
                "data": [{"symbol": "BTC/USD", "order_id": "O1"}]})
    assert not m.has_position("ETH/USD")
    assert any(isinstance(e, GapCloseEstimated) and e.symbol == "ETH/USD" for e in events)


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


def test_build_binds_entry_suppression_gate_to_the_rate_counter():
    # RL-MON-003: build() binds the WSManager entry-dispatch gate to THIS connection's RateCounter
    # critical-tier predicate, so a live entry on an armed pair is suppressed.
    m = WSManager(Mode.LIVE)
    opened: list[_FakeTransport] = []

    async def _open():
        t = _FakeTransport()
        opened.append(t)
        return t

    async def _acquire():
        return "TOK"

    async def _snap():
        return []

    asm = PrivateConnectionAssembler(
        m, open_socket=_open, acquire_token=_acquire, fetch_snap_orders=_snap,
        sleep=_noop_sleep, on_event=lambda _e: None,
    )
    pc = asyncio.run(asm.build())
    # the gate is wired and, with no rate pressure, does not suppress.
    assert m._entry_suppression_check is not None
    assert m._entry_suppression_check("BTC/USD") is False
    # drive BTC/USD above the 95% critical fraction -> the bound predicate now suppresses ITS entry
    # (and only its - a quiet pair is untouched).
    pc.loop.handle_message(_exec_ack(100), now=0.0)
    pc.loop.handle_message(
        {"channel": "executions", "type": "update", "data": [_rate_fill("BTC/USD", 99)]},
        now=1.0,
    )
    assert m._entry_suppression_check("BTC/USD") is True
    assert m._entry_suppression_check("ETH/USD") is False


def test_build_balances_handler_feeds_the_live_wallet_cache():
    # the wired balances channel (no longer a no-op) routes each frame into the WSManager live wallet
    # cache (WS-BAL-002/003), so the live G8 sizer's wallet source reflects the real Kraken balances.
    m = WSManager(Mode.LIVE)

    async def _open():
        return _IdleTransport()

    asm = PrivateConnectionAssembler(
        m, open_socket=_open, acquire_token=_tok, sleep=_noop_sleep,
    )
    pc = asyncio.run(asm.build())
    pc.loop.handle_message({"channel": "balances", "type": "snapshot", "data": [
        {"asset": "USD", "wallets": [
            {"type": "spot", "id": "main", "balance": "5000.0"},
            {"type": "margin", "id": "m1", "balance": "3000.0"}]}]}, now=0.0)
    assert m.live_spot_wallet_usd() == Decimal("5000.0")
    assert m.live_margin_wallet_usd() == Decimal("3000.0")
    # a reconnect drops the stale cache (WS-REC-004); the fresh snapshot re-seeds it.
    asyncio.run(pc.driver.initiate(0, _random_reason()))
    assert m.live_spot_wallet_usd() is None


def test_build_captures_the_long_startup_baseline_once_never_on_reconnect():
    # REST-BAL-004 / ar:AR-052: build() captures the LONG drawdown baseline ONCE at startup from the
    # GetAccountBalance USD edge (assigned directly as portfolio_baseline_USD); a reconnect NEVER
    # recaptures it (HR-WM-011 / AR-056 - the baseline survives the WS-REC-004 cache drop).
    m = WSManager(Mode.LIVE)
    calls: list = []

    async def _balance():
        calls.append(1)
        return Decimal("7500.0")

    async def _open():
        return _IdleTransport()

    asm = PrivateConnectionAssembler(
        m, open_socket=_open, acquire_token=_tok, fetch_account_balance=_balance,
        sleep=_noop_sleep,
    )
    pc = asyncio.run(asm.build())
    assert m.portfolio_baseline(PositionSide.LONG) == Decimal("7500.0")   # captured at startup
    assert m.portfolio_baseline(PositionSide.SHORT) is None               # short baseline deferred
    assert len(calls) == 1
    # a reconnect re-runs the restore steps but NOT the startup baseline capture (captured once).
    asyncio.run(pc.driver.initiate(0, _random_reason()))
    assert m.portfolio_baseline(PositionSide.LONG) == Decimal("7500.0")
    assert len(calls) == 1


def test_build_without_account_balance_edge_leaves_baseline_unset():
    # back-compat: no REST-BAL-004 edge wired -> no live baseline captured (the sweep skips the long
    # until one lands); build still succeeds (the edge is optional, like fetch_snap_orders).
    m = WSManager(Mode.LIVE)

    async def _open():
        return _IdleTransport()

    asm = PrivateConnectionAssembler(m, open_socket=_open, acquire_token=_tok, sleep=_noop_sleep)
    asyncio.run(asm.build())
    assert m.portfolio_baseline(PositionSide.LONG) is None


# -- the private connection satisfies the Transport protocol at its edge --

def test_fake_transport_is_transport():
    assert isinstance(_FakeTransport(), Transport)
