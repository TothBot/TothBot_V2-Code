"""S2b tests: connection lifecycle, inbound dispatch, outbound seam, WS_Manager.

Covers 0500000 dv1_240 sec 2 Image1 + sec 7 Image6 + sec 12 Image7:
the connection invariants (HR-WM-002/A-18), the O(1) inbound dispatch of the 7
channels (A-12/HR-WM-006 never-drop), and the paper/live outbound seam gate
(HR-WM-021/022/023, PA-004 divergence #2).
"""

from __future__ import annotations

import pytest

from tothbot.config.settings import Mode
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
    seam = DispatchSeam(
        mode,
        live_sender=lambda op, m: sent.append((op, m)),
        paper_simulator=lambda op, m: simmed.append((op, m)),
        on_event=events.append,
        **kw,
    )
    return seam, sent, simmed, events


def test_seam_paper_simulates_never_transmits():
    seam, sent, simmed, events = _seam(Mode.PAPER)
    result = seam.add_order({"pair": "BTC/USD"})
    assert result.transmitted is False
    assert sent == []  # nothing to Kraken (PA-004 #2 / HR-WM-023)
    assert simmed == [(OutboundOp.ADD_ORDER, {"pair": "BTC/USD"})]
    assert events == [PaperOrderSimulated(OutboundOp.ADD_ORDER)]


def test_seam_paper_all_six_ops_simulated():
    seam, sent, simmed, events = _seam(Mode.PAPER)
    seam.add_order({})
    seam.batch_add({})
    seam.cancel_order({})
    seam.amend_order({})
    seam.batch_cancel({})
    seam.dispatch_market_sell({})
    assert sent == []
    assert [op for op, _ in simmed] == list(OutboundOp)
    assert all(isinstance(e, PaperOrderSimulated) for e in events)


# -- outbound seam: live mode -------------------------------------------

def test_seam_live_transmits_and_emits_entry_submitted():
    seam, sent, simmed, events = _seam(Mode.LIVE)
    result = seam.add_order({"pair": "ETH/USD"})
    assert result.transmitted is True
    assert simmed == []
    assert sent == [(OutboundOp.ADD_ORDER, {"pair": "ETH/USD"})]
    assert events == [EntrySubmitted(OutboundOp.ADD_ORDER)]


def test_seam_live_non_entry_ops_transmit_without_entry_event():
    seam, sent, simmed, events = _seam(Mode.LIVE)
    seam.cancel_order({})
    seam.amend_order({})
    assert [op for op, _ in sent] == [OutboundOp.CANCEL_ORDER, OutboundOp.AMEND_ORDER]
    assert events == []  # ENTRY_SUBMITTED only on the entry add_order


# -- outbound seam: PAPER_DISPATCH_BLOCKED canary (HR-WM-023) ------------

def test_seam_canary_blocks_live_branch_in_paper():
    events: list = []
    seam = DispatchSeam(
        Mode.PAPER,
        live_sender=lambda op, m: None,
        paper_simulator=lambda op, m: None,
        on_event=events.append,
    )
    # Reach the defensive live branch directly - must block and emit canary.
    with pytest.raises(PaperDispatchBlockedError):
        seam._transmit_live(OutboundOp.ADD_ORDER, {})
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
    m = WSManager(Mode.LIVE, live_sender=lambda op, msg: sent.append(op))
    m.seam.add_order({})
    assert sent == [OutboundOp.ADD_ORDER]


def test_manager_unwired_simulator_raises_in_paper():
    # The shell ships an unwired paper simulator (real one is S2c).
    m = WSManager(Mode.PAPER)
    with pytest.raises(NotImplementedError):
        m.seam.add_order({})
