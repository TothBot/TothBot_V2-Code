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
from tothbot.exchange.position_mirror import (
    PositionAction,
    PositionSide,
    SoleWriterViolationError,
)
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
from tothbot.exchange.regime_exit import (
    L1aExitHeld,
    PairStatus,
    PaperRegimeExitDetected,
    RegimeExitNoQuote,
)
from tothbot.regime.engine import classify_from_indicators
from tothbot.regime.taxonomy import Regime
from tothbot.exchange.ws_manager import (
    SelectionStateUpdated,
    TickerTriggerSwitched,
    WSManager,
)
from tothbot.execution.exit_controller import ExitReason, PaperCloseSkipped, TradeClose


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
    m._selection_consecutive_loss[PositionSide.LONG]["BTC/USD"] = 2  # a prior streak (long wallet)
    _open_paper_long(m, atr="500", emergsl=None)        # threshold 750
    # bid 66000 is favorable for a long; force a regime-style profitable close directly.
    m.update_selection_state_on_close("BTC/USD", is_win=True)
    assert m.consecutive_loss_count("BTC/USD") == 0


# -- per-MODULE Exit Controllers + the CIATS learning-sink emission (sec 7, TB00748 (a)) -------

def _open_paper_short(m, *, atr="2000", emergsl="66000"):
    """Open a BTC/USD short paper position (sell-to-open) carrying the entry-time snapshot +
    the synthetic short entry-fill credit. A short's adverse move is a RISE (ar:AR-048 ask)."""
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "sell",
         "cum_qty": "0.05", "avg_price": "60000", "cl_ord_id": "cl-s1"},
        regime_at_entry="TRENDING_NEG_NORMAL", atr_14_entry=atr, emergsl_price=emergsl,
    )
    m.apply_paper_entry_fill("BTC/USD", "0.05", "60000", is_short=True)


def test_exit_controllers_are_per_module_wallet():
    m = WSManager(Mode.PAPER)
    assert set(m.exit_controllers) == {PositionSide.LONG, PositionSide.SHORT}
    assert m.exit_controllers[PositionSide.LONG] is not m.exit_controllers[PositionSide.SHORT]
    # the back-compat accessor is the LONG/default controller (mirrors ledger/spot_usd_balance).
    assert m.exit_controller is m.exit_controllers[PositionSide.LONG]


def test_exit_controllers_are_none_in_live():
    m = WSManager(Mode.LIVE)
    assert m.exit_controllers is None
    assert m.exit_controller is None


def test_set_ciats_exit_sinks_is_a_safe_noop_in_live():
    m = WSManager(Mode.LIVE)
    m.set_ciats_exit_sinks({PositionSide.LONG: lambda e: None})  # must not raise (no controllers)
    assert m.exit_controllers is None


def test_long_close_emits_through_the_long_ciats_sink_only():
    # TB00748 (a): with the per-side ciats_sinks wired, a LONG paper close emits its TRADE_CLOSE
    # THROUGH the LONG module's sink (the learning membrane) - never the short sink, never the
    # general telemetry on_event (which now carries only the non-close events).
    general: list = []
    long_sink: list = []
    short_sink: list = []
    m = WSManager(Mode.PAPER, on_event=general.append, now_monotonic=lambda: 1.0)
    m.set_ciats_exit_sinks({PositionSide.LONG: long_sink.append, PositionSide.SHORT: short_sink.append})
    _open_paper_long(m)
    m.handle_ticker(_ticker("BTC/USD", "57000", "57100"))   # L2 MAE breach -> long close
    assert not m.has_position("BTC/USD")
    assert len([e for e in long_sink if isinstance(e, TradeClose)]) == 1   # through the long sink
    assert not any(isinstance(e, TradeClose) for e in short_sink)          # never the short sink
    assert not any(isinstance(e, TradeClose) for e in general)             # not the general sink
    # the non-close detection telemetry still flows to the general on_event (unchanged partition).
    assert any(isinstance(e, PaperMaeDetected) for e in general)


def test_short_close_emits_through_the_short_ciats_sink_only():
    # the Long/Short mirror: a SHORT paper close routes to the SHORT module's sink (sec 7).
    long_sink: list = []
    short_sink: list = []
    m = WSManager(Mode.PAPER, on_event=lambda e: None, now_monotonic=lambda: 1.0)
    m.set_ciats_exit_sinks({PositionSide.LONG: long_sink.append, PositionSide.SHORT: short_sink.append})
    _open_paper_short(m)                                    # short entry at 60000, atr 2000
    m.handle_ticker(_ticker("BTC/USD", "62900", "63000"))   # ask 63000 -> short MAE 3000 >= 3000
    assert not m.has_position("BTC/USD")
    closes = [e for e in short_sink if isinstance(e, TradeClose)]
    assert len(closes) == 1 and closes[0].exit_reason is ExitReason.MAE_THRESHOLD_BREACH
    assert not any(isinstance(e, TradeClose) for e in long_sink)


def test_unwired_side_keeps_emitting_to_the_general_on_event():
    # before set_ciats_exit_sinks (or for a side with no sink) the close path is unchanged: the
    # controller's general on_event still sees the TRADE_CLOSE (back-compat / the default path).
    general: list = []
    m = WSManager(Mode.PAPER, on_event=general.append, now_monotonic=lambda: 1.0)
    _open_paper_long(m)
    m.handle_ticker(_ticker("BTC/USD", "57000", "57100"))
    assert len([e for e in general if isinstance(e, TradeClose)]) == 1


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


# -- layer:L1a regime-reversal exit drive (EC-L1A-001 / EC-L1A-002) ------------

def _trending_neg():
    """A daily classification landing TRENDING_NEG_NORMAL (a long-blocking downgrade)."""
    return classify_from_indicators("BTC/USD", "40", "95", "100", "1000", "50")


def test_l1a_daily_downgrade_full_close():
    events: list = []
    sem = _FakeSemaphore()
    m = WSManager(Mode.PAPER, on_event=events.append,
                  now_monotonic=lambda: 999.0, exit_semaphore=sem)
    _open_paper_long(m)
    assert m.spot_usd_balance == Decimal("1992.2")

    # 00:00 UTC regime refresh downgrades the pair; run-to-reversal close at the bid 61000.
    m.on_regime_classified("BTC/USD", _trending_neg(), bid="61000", ask="61100")

    assert not m.has_position("BTC/USD")               # mirror cleared (step 7)
    assert m.ledger.fees_entry_for("BTC/USD") is None  # entry fee cleared
    assert m.spot_usd_balance == Decimal("5034.27")    # 1992.2 + (3050 - 7.93)

    closes = [e for e in events if isinstance(e, TradeClose)]
    assert len(closes) == 1
    rec = closes[0]
    assert rec.exit_reason is ExitReason.DAILY_REGIME_DOWNGRADE
    assert rec.exit_price == Decimal("61000")          # ar:AR-048 bid for a long
    assert rec.net_pl_usd == Decimal("34.27")          # run-to-reversal in profit (a win)
    assert rec.net_gain_usd == Decimal("34.27")
    # detection telemetry + AR-073 win-reset + ticker trades-mode + semaphore release.
    det = [e for e in events if isinstance(e, PaperRegimeExitDetected)]
    assert len(det) == 1 and det[0].trigger == "EC-L1A-002"
    assert m.consecutive_loss_count("BTC/USD") == 0     # win path
    assert m.ticker_event_trigger("BTC/USD") == "trades"
    assert sem.released == 1
    assert any(isinstance(e, SelectionStateUpdated) and e.is_win for e in events)


def test_l1a_htf_reversal_full_close():
    events: list = []
    m = WSManager(Mode.PAPER, on_event=events.append)
    _open_paper_long(m)
    # 1H close: EMA20 < EMA50 -> HTF reversal; sell at the bid.
    m.on_htf_ohlc_close("BTC/USD", "99", "100", bid="61000", ask="61100")
    assert not m.has_position("BTC/USD")
    rec = next(e for e in events if isinstance(e, TradeClose))
    assert rec.exit_reason is ExitReason.HTF_REGIME_REVERSAL
    det = next(e for e in events if isinstance(e, PaperRegimeExitDetected))
    assert det.trigger == "EC-L1A-001"


def test_l1a_held_on_cancel_only_pair_status():
    # rule:HR-EC-016(a): cancel_only -> HOLD, CRITICAL alert, NO order, ledger untouched.
    events: list = []
    m = WSManager(Mode.PAPER, on_event=events.append)
    _open_paper_long(m)
    balance_before = m.spot_usd_balance
    m.on_regime_classified("BTC/USD", _trending_neg(), bid="61000", ask="61100",
                           pair_status=PairStatus.CANCEL_ONLY)
    assert m.has_position("BTC/USD")                   # position retained
    assert m.spot_usd_balance == balance_before        # ledger NOT moved
    held = [e for e in events if isinstance(e, L1aExitHeld)]
    assert len(held) == 1 and held[0].pair_status == "cancel_only"
    assert not any(isinstance(e, TradeClose) for e in events)


def test_l1a_no_double_close_against_ticker_path():
    # carry-forward (f): the L1a close is the SAME on_paper_close; a follow-on ticker
    # detection on the cleared mirror is a surfaced PAPER_CLOSE_SKIPPED, never a 2nd close.
    events: list = []
    m = WSManager(Mode.PAPER, on_event=events.append)
    _open_paper_long(m)                                 # atr 2000 -> L2 threshold 3000
    m.on_regime_classified("BTC/USD", _trending_neg(), bid="56000", ask="56100")
    assert not m.has_position("BTC/USD")
    # an adverse ticker arrives AFTER the L1a close: handle_ticker skips the now-empty symbol
    # (no detection, no second close); the single TRADE_CLOSE stands.
    m.handle_ticker(_ticker("BTC/USD", "56000", "56100"))
    assert len([e for e in events if isinstance(e, TradeClose)]) == 1
    # and the on_paper_close guard itself: a direct second close on the cleared symbol is a
    # surfaced PAPER_CLOSE_SKIPPED, never a duplicate TRADE_CLOSE record.
    assert m.exit_controller.on_paper_close(
        "BTC/USD", "56000", ExitReason.MAE_THRESHOLD_BREACH, "0", m
    ) is None
    assert any(isinstance(e, PaperCloseSkipped) for e in events)
    assert len([e for e in events if isinstance(e, TradeClose)]) == 1


def test_l1a_defers_when_no_realizable_quote():
    events: list = []
    m = WSManager(Mode.PAPER, on_event=events.append)
    _open_paper_long(m)
    # fired, precondition ok, but no bid for the long market sell -> deferred, position kept.
    m.on_regime_classified("BTC/USD", _trending_neg(), bid=None, ask="61100")
    assert m.has_position("BTC/USD")
    assert any(isinstance(e, RegimeExitNoQuote) for e in events)
    assert not any(isinstance(e, TradeClose) for e in events)


def test_l1a_holds_when_regime_still_supports_long():
    m = WSManager(Mode.PAPER)
    _open_paper_long(m)
    pos_regime = classify_from_indicators("BTC/USD", "40", "105", "100", "1000", "50")
    assert pos_regime.regime is Regime.TRENDING_POS_NORMAL
    m.on_regime_classified("BTC/USD", pos_regime, bid="61000", ask="61100")
    assert m.has_position("BTC/USD")                   # no downgrade -> no exit


def test_l1a_no_open_position_is_noop():
    events: list = []
    m = WSManager(Mode.PAPER, on_event=events.append)
    m.on_regime_classified("BTC/USD", _trending_neg(), bid="61000", ask="61100")
    assert not any(isinstance(e, (TradeClose, PaperRegimeExitDetected)) for e in events)


def test_l1a_is_noop_in_live_mode():
    m = WSManager(Mode.LIVE)
    # no exit_controller in live; the L1a handlers must be inert, not raise.
    m.on_regime_classified("BTC/USD", _trending_neg(), bid="61000", ask="61100")
    m.on_htf_ohlc_close("BTC/USD", "99", "100", bid="61000", ask="61100")
    assert m.exit_controller is None


# -- two independent per-module wallets (mod:Long_Module + mod:Short_Module, sec 7) ----

def test_paper_mode_builds_two_independent_wallets():
    m = WSManager(Mode.PAPER)
    assert set(m.modules) == {PositionSide.LONG, PositionSide.SHORT}
    assert m.modules[PositionSide.LONG] is not m.modules[PositionSide.SHORT]
    assert m.wallet_balance(PositionSide.LONG) == Decimal("5000.0")
    assert m.wallet_balance(PositionSide.SHORT) == Decimal("5000.0")


def test_short_entry_fill_hits_only_the_short_wallet():
    m = WSManager(Mode.PAPER)
    # a SHORT sell-to-open CREDITS the short wallet (proceeds net of taker + margin open fee);
    # the long wallet is untouched - per-wallet isolation (sec 7 / Gate-7).
    m.apply_paper_entry_fill("BTC/USD", "0.05", "60000", is_short=True)
    assert m.wallet_balance(PositionSide.SHORT) > Decimal("5000.0")   # credited
    assert m.wallet_balance(PositionSide.LONG) == Decimal("5000.0")   # untouched
    # the back-compat spot_usd_balance still reads the LONG wallet.
    assert m.spot_usd_balance == Decimal("5000.0")


def test_long_entry_fill_hits_only_the_long_wallet():
    m = WSManager(Mode.PAPER)
    m.apply_paper_entry_fill("BTC/USD", "0.05", "60000")  # long debit (is_short default False)
    assert m.wallet_balance(PositionSide.LONG) == Decimal("1992.2")
    assert m.wallet_balance(PositionSide.SHORT) == Decimal("5000.0")  # untouched


def test_fees_entry_routed_to_the_owning_wallet():
    m = WSManager(Mode.PAPER)
    m.apply_paper_entry_fill("ETH/USD", "1", "3000", is_short=True)   # short wallet holds the fee
    assert m.fees_entry_for("ETH/USD") is not None
    assert m.modules[PositionSide.SHORT].ledger.fees_entry_for("ETH/USD") is not None
    assert m.modules[PositionSide.LONG].ledger.fees_entry_for("ETH/USD") is None


def test_selection_state_is_per_side():
    m = WSManager(Mode.PAPER)
    m.update_selection_state_on_close("BTC/USD", is_win=False, side=PositionSide.SHORT)
    m.update_selection_state_on_close("BTC/USD", is_win=False, side=PositionSide.SHORT)
    assert m.consecutive_loss_count("BTC/USD", PositionSide.SHORT) == 2
    assert m.consecutive_loss_count("BTC/USD", PositionSide.LONG) == 0   # the long counter is its own


def test_paper_starting_balance_override_seeds_both_wallets():
    m = WSManager(Mode.PAPER, paper_starting_balance="7500")
    assert m.wallet_balance(PositionSide.LONG) == Decimal("7500")
    assert m.wallet_balance(PositionSide.SHORT) == Decimal("7500")


# -- the ENTRY dispatch flow (G8 accepted -> entry add_order -> on-fill emergSL) --------

def test_dispatch_entry_long_opens_position_places_emergsl_debits_long_wallet():
    events: list = []
    m = WSManager(Mode.PAPER, on_event=events.append)
    filled = asyncio.run(m.dispatch_entry(
        PositionSide.LONG, "BTC/USD",
        order_qty="0.05", entry_limit_price="60000", emergsl_price="57000",
        atr_14_entry="1000", regime_at_entry="TRENDING_POS_NORMAL",
        cl_ord_id="cl-1", deadline="2026-06-15T07:30:00Z",
        signal_params={"rsi_14": 42, "sss_pass": True, "side": "long"},
        market_regime="TRENDING_POS_ELEVATED", entry_timestamp_utc="2026-06-15T07:25:00+00:00",
    ))
    assert filled is True
    pos = m.position("BTC/USD")
    assert pos is not None and pos.side is PositionSide.LONG
    # the entry-time D6 snapshot was attached at the opening fill (Pending Order Registry).
    assert pos.emergsl_price == Decimal("57000")        # L3 stop below entry
    assert pos.atr_14_entry == Decimal("1000")          # L2 MAE basis
    assert pos.regime_at_entry == "TRENDING_POS_NORMAL"
    # the contract:TRADE_CLOSE entry-side producer fields rode the Pending Order Registry too.
    assert pos.signal_params == {"rsi_14": 42, "sss_pass": True, "side": "long"}
    assert pos.market_regime == "TRENDING_POS_ELEVATED"
    assert pos.entry_timestamp_utc == "2026-06-15T07:25:00+00:00"
    # the LONG wallet was debited (buy-to-open); the SHORT wallet is untouched.
    assert m.wallet_balance(PositionSide.LONG) < Decimal("5000.0")
    assert m.wallet_balance(PositionSide.SHORT) == Decimal("5000.0")
    # both the entry add_order AND the on-fill emergSL batch_add traversed the seam.
    ops = [e.op for e in events if isinstance(e, PaperOrderSimulated)]
    assert OutboundOp.ADD_ORDER in ops and OutboundOp.BATCH_ADD in ops
    # the pending registry was cleared after the entry resolved.
    assert m._pending_entries == {}


def test_dispatch_entry_short_opens_short_credits_short_wallet_emergsl_above():
    m = WSManager(Mode.PAPER)
    filled = asyncio.run(m.dispatch_entry(
        PositionSide.SHORT, "BTC/USD",
        order_qty="0.05", entry_limit_price="60000", emergsl_price="63000",
        atr_14_entry="1000", regime_at_entry="TRENDING_NEG_NORMAL",
        cl_ord_id="cl-2", deadline="2026-06-15T07:30:00Z",
    ))
    assert filled is True
    pos = m.position("BTC/USD")
    assert pos.side is PositionSide.SHORT              # sell-to-open opened a SHORT
    assert pos.emergsl_price == Decimal("63000")       # buy-to-cover stop ABOVE entry
    # the SHORT wallet was credited (sell-to-open proceeds); the LONG wallet is untouched.
    assert m.wallet_balance(PositionSide.SHORT) > Decimal("5000.0")
    assert m.wallet_balance(PositionSide.LONG) == Decimal("5000.0")
