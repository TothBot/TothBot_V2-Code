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
from tothbot.exchange.paper_exit import PaperEmergSlTriggered, PaperMaeDetected
from tothbot.exchange.ws_manager import (
    SelectionStateUpdated,
    TickerTriggerSwitched,
    WSManager,
)
from tothbot.execution.exit_controller import ExitReason, TradeClose


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


def test_manager_paper_dispatch_records_and_transmits_nothing():
    # In paper the boundary records the dispatch and transmits NOTHING to Kraken
    # (PA-004 div #2); no raise (a paper order is a valid dispatch). A placeholder
    # message without order fields produces no synthetic fill (no position opened).
    m = WSManager(Mode.PAPER)
    result = asyncio.run(m.seam.add_order({"pair": "BTC/USD"}))
    assert result.transmitted is False
    assert m.paper_dispatch.simulated == [(OutboundOp.ADD_ORDER, {"pair": "BTC/USD"})]
    assert m.transmitter.is_connected is False  # live transmitter never bound in paper
    assert m.open_position_symbols() == frozenset()  # non-fillable message -> no fill


# -- WS_Manager paper capital path: synthetic ledger + fill simulator ----

_PAPER_ENTRY = {
    "params": {"symbol": "BTC/USD", "side": "buy", "order_qty": "0.05",
               "limit_price": "60000", "cl_ord_id": "cl-1"}
}
_PAPER_EXIT = {
    "params": {"symbol": "BTC/USD", "side": "sell", "order_qty": "0.05",
               "limit_price": "66000", "cl_ord_id": "cl-2", "exit_reason": "L1A"}
}


def test_manager_paper_ledger_seeded_at_d05_starting_balance():
    m = WSManager(Mode.PAPER)
    assert m.ledger is not None
    assert m.spot_usd_balance == Decimal("5000.0")  # decision:D-05 $5,000/module
    assert m.paper_fill is not None


def test_manager_live_has_no_synthetic_ledger():
    m = WSManager(Mode.LIVE)
    assert m.ledger is None
    assert m.paper_fill is None
    assert m.spot_usd_balance is None  # real Kraken balance authoritative


def test_manager_paper_starting_balance_override():
    m = WSManager(Mode.PAPER, paper_starting_balance="7500")
    assert m.spot_usd_balance == Decimal("7500")


def test_manager_paper_full_cycle_entry_opens_mirror_and_debits_ledger():
    # The whole point of this slice: a paper dispatch runs the full inbound+outbound
    # cycle - the seam simulates locally, the fill writes the SAME mirror surface the
    # live executions stream feeds (D-06), and the synthetic ledger is debited.
    m = WSManager(Mode.PAPER)
    asyncio.run(m.seam.add_order(_PAPER_ENTRY))

    pos = m.position("BTC/USD")
    assert pos is not None
    assert pos.qty == Decimal("0.05")
    assert pos.avg_entry_price == Decimal("60000")
    # entry debit: proceeds 3000 + taker fee 7.8 -> 5000 - 3007.8
    assert m.spot_usd_balance == Decimal("1992.2")
    assert m.ledger.fees_entry_for("BTC/USD") == Decimal("7.8")


def test_manager_paper_full_cycle_exit_closes_mirror_and_credits_ledger():
    m = WSManager(Mode.PAPER)

    async def _round_trip():
        await m.seam.add_order(_PAPER_ENTRY)
        await m.seam.dispatch_market_sell(_PAPER_EXIT)

    asyncio.run(_round_trip())
    assert not m.has_position("BTC/USD")               # opposite-side fill closed it
    # exit credit: proceeds 3300 - taker fee 8.58 -> 1992.2 + 3291.42
    assert m.spot_usd_balance == Decimal("5283.62")
    assert m.ledger.fees_entry_for("BTC/USD") is None  # cleared on close


def test_manager_paper_ledger_is_sole_writer_guarded():
    from tothbot.exchange.ledger import LedgerSoleWriterViolationError

    m = WSManager(Mode.PAPER)
    with pytest.raises(LedgerSoleWriterViolationError):
        m.ledger.entry_fill_debit("BTC/USD", "0.05", "60000", writer="Risk_Engine")


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


# -- WS_Manager paper EXIT lifecycle (sec 12.5: detect -> credit -> close) ----

class _FakeSemaphore:
    def __init__(self):
        self.released = 0

    def release(self):
        self.released += 1


def _open_paper_long(m, *, atr="2000", emergsl="54000"):
    """Open a BTC/USD long paper position carrying the entry-time snapshot (the
    sole-writer record_execution surface) + the synthetic entry-fill debit (fees_entry)."""
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "buy",
         "cum_qty": "0.05", "avg_price": "60000", "cl_ord_id": "cl-1"},
        regime_at_entry="TRENDING_POS_NORMAL", atr_14_entry=atr, emergsl_price=emergsl,
    )
    m.apply_paper_entry_fill("BTC/USD", "0.05", "60000")  # debit: fees_entry 7.8


def _ticker(symbol, bid, ask):
    return {"channel": "ticker", "type": "update",
            "data": [{"symbol": symbol, "bid": bid, "ask": ask}]}


def test_open_switches_ticker_trigger_to_bbo():
    m = WSManager(Mode.PAPER)
    assert m.ticker_event_trigger("BTC/USD") == "trades"  # WS-TKR-002 default
    _open_paper_long(m)
    assert m.ticker_event_trigger("BTC/USD") == "bbo"      # WS-TKR-003 on open


def test_paper_exit_lifecycle_l2_mae_full_close():
    events: list = []
    sem = _FakeSemaphore()
    m = WSManager(Mode.PAPER, on_event=events.append,
                  now_monotonic=lambda: 1234.5, exit_semaphore=sem)
    _open_paper_long(m)
    assert m.spot_usd_balance == Decimal("1992.2")        # after entry debit

    # adverse bbo: bid 57000 -> mae 3000 >= atr 2000 * 1.5 -> L2 breach at the bid.
    m.handle_ticker(_ticker("BTC/USD", "57000", "57100"))

    # mirror cleared (sec 12.5 step 7), entry fee cleared, exit credited.
    assert not m.has_position("BTC/USD")
    assert m.ledger.fees_entry_for("BTC/USD") is None
    # exit credit at 57000: proceeds 2850 - taker 7.41 -> 1992.2 + 2842.59
    assert m.spot_usd_balance == Decimal("4834.79")

    # the TRADE_CLOSE record: net_pl = (57000-60000)*0.05 - 7.8 - 7.41 = -165.21 (loss)
    closes = [e for e in events if isinstance(e, TradeClose)]
    assert len(closes) == 1
    rec = closes[0]
    assert rec.exit_reason is ExitReason.MAE_THRESHOLD_BREACH
    assert rec.net_pl_usd == Decimal("-165.21")
    assert rec.net_loss_usd == Decimal("165.21")
    # detection telemetry + AR-073 loss + ticker trades-mode + semaphore release.
    assert any(isinstance(e, PaperMaeDetected) for e in events)
    assert m.consecutive_loss_count("BTC/USD") == 1
    assert m.exit_cooldown_at("BTC/USD") == 1234.5
    assert m.ticker_event_trigger("BTC/USD") == "trades"   # step 10
    assert sem.released == 1                                # step 9
    assert any(isinstance(e, SelectionStateUpdated) and not e.is_win for e in events)


def test_paper_exit_lifecycle_l3_emergsl_touch():
    events: list = []
    m = WSManager(Mode.PAPER, on_event=events.append)
    _open_paper_long(m, atr=None, emergsl="54000")  # no MAE context -> emergSL backstop
    m.handle_ticker(_ticker("BTC/USD", "53000", "53100"))  # bid <= emergsl 54000
    assert not m.has_position("BTC/USD")
    assert any(isinstance(e, PaperEmergSlTriggered) for e in events)
    rec = next(e for e in events if isinstance(e, TradeClose))
    assert rec.exit_reason is ExitReason.EMERGENCY_SL_FIRED
    assert rec.exit_price == Decimal("54000")


def test_paper_exit_win_resets_consecutive_loss_count():
    m = WSManager(Mode.PAPER)
    m._selection_consecutive_loss_count["BTC/USD"] = 2  # a prior streak
    _open_paper_long(m, atr="500", emergsl=None)        # threshold 750
    # bid 66000 is favorable for a long; force a regime-style profitable close directly.
    m.update_selection_state_on_close("BTC/USD", is_win=True)
    assert m.consecutive_loss_count("BTC/USD") == 0


def test_non_adverse_ticker_leaves_position_open():
    m = WSManager(Mode.PAPER)
    _open_paper_long(m)
    m.handle_ticker(_ticker("BTC/USD", "59000", "59100"))  # mae 1000 < 3000, bid > emergsl
    assert m.has_position("BTC/USD")


def test_handle_ticker_is_noop_in_live_mode():
    m = WSManager(Mode.LIVE)
    m.handle_ticker(_ticker("BTC/USD", "1", "2"))  # no exit_controller, must not raise
    assert m.exit_controller is None


def test_release_exit_semaphore_guarded_when_none():
    m = WSManager(Mode.PAPER)                  # no semaphore injected
    m.release_exit_semaphore()                 # must be a safe no-op
    assert m._exit_semaphore is None


def test_close_position_clears_mirror_and_fees_entry():
    m = WSManager(Mode.PAPER)
    _open_paper_long(m)
    assert m.has_position("BTC/USD") and m.fees_entry_for("BTC/USD") == Decimal("7.8")
    m.close_position("BTC/USD")
    assert not m.has_position("BTC/USD")
    assert m.fees_entry_for("BTC/USD") is None
