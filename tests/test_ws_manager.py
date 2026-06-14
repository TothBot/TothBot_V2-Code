"""S2b tests: connection lifecycle, inbound dispatch, outbound seam, WS_Manager.

Covers 0500000 dv1_240 sec 2 Image1 + sec 7 Image6 + sec 12 Image7:
the connection invariants (HR-WM-002/A-18), the O(1) inbound dispatch of the 7
channels (A-12/HR-WM-006 never-drop), and the paper/live outbound seam gate
(HR-WM-021/022/023, PA-004 divergence #2).
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from tothbot.config.settings import Mode
from tothbot.exchange.position_mirror import PositionAction, SoleWriterViolationError
from tothbot.exchange import connection as conn
from tothbot.exchange.channels import PrivateChannel, PublicChannel
from tothbot.exchange.connection import (
    ConnectionRole,
    ConnectionState,
    WSConnection,
)
from tothbot.exchange.dispatch import (
    DispatchTable,
    UnknownChannelError,
    channel_from_wire,
)
from tothbot.exchange.seam import (
    DispatchSeam,
    EntrySubmitted,
    OutboundOp,
    PaperDispatchBlocked,
    PaperDispatchBlockedError,
    PaperOrderSimulated,
)
from tothbot.exchange.ws_manager import WSManager


# -- connection: endpoints + invariants ---------------------------------

def test_endpoints_match_diagram():
    assert conn.PUBLIC_WS_URL == "wss://ws.kraken.com/v2"
    assert conn.PRIVATE_WS_URL == "wss://ws-auth.kraken.com/v2"
    assert conn.REST_BASE_URL == "https://api.kraken.com"
    assert conn.STATUS_API_URL.startswith("https://status.kraken.com/")


def test_ws_connect_invariants():
    kw = conn.ws_connect_kwargs()
    assert kw["max_size"] == 10 * 1024 * 1024  # A-18 10 MB
    assert kw["open_timeout"] == 10
    assert kw["max_queue"] is None
    assert kw["ping_interval"] is None  # library ping off; app ping in S2c
    # fresh dict each call - mutation must not leak
    kw["max_size"] = 1
    assert conn.ws_connect_kwargs()["max_size"] == 10 * 1024 * 1024


def test_endpoint_for_role():
    assert conn.endpoint_for(ConnectionRole.PUBLIC) == conn.PUBLIC_WS_URL
    assert conn.endpoint_for(ConnectionRole.PRIVATE) == conn.PRIVATE_WS_URL


# -- connection: lifecycle state machine --------------------------------

def test_connection_happy_path():
    c = WSConnection(ConnectionRole.PUBLIC)
    assert c.state is ConnectionState.DISCONNECTED
    assert not c.is_connected
    c.mark_connecting()
    assert c.state is ConnectionState.CONNECTING
    c.mark_connected(connection_id=42)
    assert c.is_connected
    assert c.connection_id == 42
    c.mark_closed()
    assert c.state is ConnectionState.CLOSED
    assert c.connection_id is None


def test_connection_reconnect_transition_allowed():
    c = WSConnection(ConnectionRole.PRIVATE)
    c.mark_connecting()
    c.mark_closed()  # CONNECTING -> CLOSED (failed connect)
    c.mark_connecting()  # CLOSED -> CONNECTING (reconnect path)
    assert c.state is ConnectionState.CONNECTING


def test_connection_illegal_transition_raises():
    c = WSConnection(ConnectionRole.PUBLIC)
    with pytest.raises(ValueError):
        c.mark_connected(connection_id=1)  # DISCONNECTED -> CONNECTED illegal


# -- inbound dispatch: wire resolver ------------------------------------

def test_channel_from_wire_ohlc_interval():
    assert channel_from_wire("ohlc", 5) is PublicChannel.OHLC_5M
    assert channel_from_wire("ohlc", 60) is PublicChannel.OHLC_60M


def test_channel_from_wire_by_name():
    assert channel_from_wire("ticker") is PublicChannel.TICKER
    assert channel_from_wire("instrument") is PublicChannel.INSTRUMENT
    assert channel_from_wire("status") is PublicChannel.STATUS
    assert channel_from_wire("executions") is PrivateChannel.EXECUTIONS
    assert channel_from_wire("balances") is PrivateChannel.BALANCES


def test_channel_from_wire_unknown_never_silent():
    with pytest.raises(UnknownChannelError):
        channel_from_wire("orderbook")
    with pytest.raises(UnknownChannelError):
        channel_from_wire("ohlc", 1440)


# -- inbound dispatch: O(1) table ---------------------------------------

def test_dispatch_routes_all_seven_channels():
    table = DispatchTable()
    seen: list[tuple[object, dict]] = []
    all_channels = list(PublicChannel) + list(PrivateChannel)
    assert len(all_channels) == 7
    for ch in all_channels:
        table.register(ch, lambda f, ch=ch: seen.append((ch, f)))
    assert table.registered_channels == frozenset(all_channels)
    table.route("ohlc", {"x": 1}, interval=5)
    table.route("executions", {"y": 2})
    assert seen == [(PublicChannel.OHLC_5M, {"x": 1}),
                    (PrivateChannel.EXECUTIONS, {"y": 2})]


def test_dispatch_double_register_rejected():
    table = DispatchTable()
    table.register(PublicChannel.TICKER, lambda f: None)
    with pytest.raises(ValueError):
        table.register(PublicChannel.TICKER, lambda f: None)


def test_dispatch_unregistered_channel_raises():
    table = DispatchTable()
    with pytest.raises(UnknownChannelError):
        table.dispatch(PublicChannel.STATUS, {})


# -- outbound seam: paper mode ------------------------------------------

def _seam(mode, **kw):
    sent: list = []
    simmed: list = []
    events: list = []

    async def _live(op, m):
        sent.append((op, m))

    async def _paper(op, m):
        simmed.append((op, m))

    seam = DispatchSeam(
        mode,
        live_sender=_live,
        paper_simulator=_paper,
        on_event=events.append,
        **kw,
    )
    return seam, sent, simmed, events


def test_seam_paper_simulates_never_transmits():
    seam, sent, simmed, events = _seam(Mode.PAPER)
    result = asyncio.run(seam.add_order({"pair": "BTC/USD"}))
    assert result.transmitted is False
    assert sent == []  # nothing to Kraken (PA-004 #2 / HR-WM-023)
    assert simmed == [(OutboundOp.ADD_ORDER, {"pair": "BTC/USD"})]
    assert events == [PaperOrderSimulated(OutboundOp.ADD_ORDER)]


def test_seam_paper_all_six_ops_simulated():
    seam, sent, simmed, events = _seam(Mode.PAPER)

    async def _drive():
        await seam.add_order({})
        await seam.batch_add({})
        await seam.cancel_order({})
        await seam.amend_order({})
        await seam.batch_cancel({})
        await seam.dispatch_market_sell({})

    asyncio.run(_drive())
    assert sent == []
    assert [op for op, _ in simmed] == list(OutboundOp)
    assert all(isinstance(e, PaperOrderSimulated) for e in events)


# -- outbound seam: live mode -------------------------------------------

def test_seam_live_transmits_and_emits_entry_submitted():
    seam, sent, simmed, events = _seam(Mode.LIVE)
    result = asyncio.run(seam.add_order({"pair": "ETH/USD"}))
    assert result.transmitted is True
    assert simmed == []
    assert sent == [(OutboundOp.ADD_ORDER, {"pair": "ETH/USD"})]
    assert events == [EntrySubmitted(OutboundOp.ADD_ORDER)]


def test_seam_live_non_entry_ops_transmit_without_entry_event():
    seam, sent, simmed, events = _seam(Mode.LIVE)

    async def _drive():
        await seam.cancel_order({})
        await seam.amend_order({})

    asyncio.run(_drive())
    assert [op for op, _ in sent] == [OutboundOp.CANCEL_ORDER, OutboundOp.AMEND_ORDER]
    assert events == []  # ENTRY_SUBMITTED only on the entry add_order


# -- outbound seam: PAPER_DISPATCH_BLOCKED canary (HR-WM-023) ------------

def test_seam_canary_blocks_live_branch_in_paper():
    events: list = []

    async def _noop(op, m):
        return None

    seam = DispatchSeam(
        Mode.PAPER,
        live_sender=_noop,
        paper_simulator=_noop,
        on_event=events.append,
    )
    # Reach the defensive live branch directly - must block and emit canary.
    with pytest.raises(PaperDispatchBlockedError):
        asyncio.run(seam._transmit_live(OutboundOp.ADD_ORDER, {}))
    assert events == [PaperDispatchBlocked(OutboundOp.ADD_ORDER)]


def test_seam_is_paper_property():
    seam, *_ = _seam(Mode.PAPER)
    assert seam.is_paper is True
    seam, *_ = _seam(Mode.LIVE)
    assert seam.is_paper is False


# -- WS_Manager shell ---------------------------------------------------

def test_manager_paper_has_no_private_connection():
    m = WSManager(Mode.PAPER)
    assert m.is_paper and not m.is_live
    assert m.private is None              # HR-WM-022
    assert m.has_private_connection is False
    assert m.public.role is ConnectionRole.PUBLIC


def test_manager_live_has_private_connection():
    m = WSManager(Mode.LIVE)
    assert m.is_live and not m.is_paper
    assert m.private is not None and m.private.role is ConnectionRole.PRIVATE
    assert m.has_private_connection is True


def test_manager_inbound_routing():
    m = WSManager(Mode.PAPER)
    got: list = []
    m.register_handler(PublicChannel.OHLC_5M, got.append)
    m.route_frame("ohlc", {"close": "1"}, interval=5)
    assert got == [{"close": "1"}]


def test_manager_seam_wired_to_mode():
    sent: list = []

    async def _live(op, msg):
        sent.append(op)

    m = WSManager(Mode.LIVE, live_sender=_live)
    asyncio.run(m.seam.add_order({}))
    assert sent == [OutboundOp.ADD_ORDER]


def test_manager_paper_dispatch_is_real_noop_boundary():
    # The shell now ships the real outbound bodies: in paper the boundary records
    # the dispatch and transmits NOTHING to Kraken (PA-004 div #2); no raise (a
    # paper order is a valid dispatch). The synthetic fill simulator is Path B.
    m = WSManager(Mode.PAPER)
    result = asyncio.run(m.seam.add_order({"pair": "BTC/USD"}))
    assert result.transmitted is False
    assert m.paper_dispatch.simulated == [(OutboundOp.ADD_ORDER, {"pair": "BTC/USD"})]
    assert m.transmitter.is_connected is False  # live transmitter never bound in paper


def test_manager_live_transmitter_unbound_raises_until_private_connected():
    # In live the seam routes to the transmitter; with no private WS bound yet
    # (startup Step 5 not run) a dispatch surfaces OutboundNotConnectedError, never
    # a silent drop.
    from tothbot.exchange.outbound import OutboundNotConnectedError

    m = WSManager(Mode.LIVE)
    with pytest.raises(OutboundNotConnectedError):
        asyncio.run(m.seam.add_order({"pair": "BTC/USD"}))


# -- WS_Manager as the sole writer to Position Mirror (HR-PM-009) --------

def test_manager_records_execution_as_sole_writer():
    m = WSManager(Mode.PAPER)
    outcome = m.record_execution(
        {"exec_type": "trade", "symbol": "BTC/USD", "side": "buy",
         "cum_qty": "0.5", "avg_price": "60000"}
    )
    assert outcome.action is PositionAction.OPENED
    assert m.has_position("BTC/USD")
    assert m.position("BTC/USD").avg_entry_price == Decimal("60000")
    assert m.open_position_symbols() == frozenset({"BTC/USD"})


def test_manager_is_only_writer_other_modules_use_read_helpers():
    # A consumer that bypasses WSManager and forges a write is rejected (HR-PM-009).
    m = WSManager(Mode.PAPER)
    with pytest.raises(SoleWriterViolationError):
        m.positions.apply_execution(
            {"exec_type": "trade", "symbol": "BTC/USD", "side": "buy", "cum_qty": "1"},
            writer="Risk_Engine",
        )


def test_manager_restore_position_mirror_returns_gap_closed():
    m = WSManager(Mode.PAPER)
    m.record_execution({"exec_type": "trade", "symbol": "BTC/USD", "side": "buy",
                        "cum_qty": "0.5", "avg_price": "60000"})
    m.record_execution({"exec_type": "trade", "symbol": "ETH/USD", "side": "sell",
                        "cum_qty": "2", "avg_price": "3000"})
    gap = m.restore_position_mirror([{"symbol": "BTC/USD"}])
    assert [g.symbol for g in gap] == ["ETH/USD"]
    assert m.open_position_symbols() == frozenset({"BTC/USD"})


def test_manager_mirror_events_routed_to_on_event_sink():
    events: list = []
    m = WSManager(Mode.PAPER, on_event=events.append)
    m.record_execution({"exec_type": "trade", "symbol": "BTC/USD", "side": "buy",
                        "cum_qty": "0.5", "avg_price": "60000"})
    assert any(getattr(e, "code", None) == "POSITION_STATE_WRITE" for e in events)
