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
from tothbot.exchange.balances_cache import BalancesSnapshotApplied, BalancesUpdated
from tothbot.exchange.ws_manager import (
    CancelAckTimeout,
    EmergSlPlaced,
    EntrySuppressed,
    ExitDispatchOutcome,
    PendingEmergSl,
    LiveExitDetected,
    LiveExitDispatched,
    GapCloseEstimated,
    LiveExitDeferredNoQuote,
    LiveExitDoubleDispatchSkipped,
    LiveExitHeldAmbiguous,
    LiveExitIntent,
    LiveExitMppExhausted,
    LiveExitPriorityOverride,
    MppRejectRetry,
    SelectionStateUpdated,
    TickerTriggerSwitched,
    WSManager,
)
from tothbot.execution.exit_controller import ExitReason, PaperCloseSkipped, TradeClose
from tothbot.execution.exit_dispatch import (
    build_cancel_order,
    build_limit_only_exit_order,
    build_market_sell_order,
    build_mpp_retry_order,
    mpp_retry_limit_price,
)


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


def test_open_acquires_only_this_modules_dispatch_semaphore():
    # ar:AR-043 (D2) per-module independence (sec 7): opening a LONG commitment acquires the LONG
    # dispatch semaphore but leaves the SHORT module's semaphore FREE (one side never blocks the other).
    m = WSManager(Mode.PAPER)
    assert m.dispatch_semaphore_locked(PositionSide.LONG) is False
    assert m.dispatch_semaphore_locked(PositionSide.SHORT) is False
    _open_paper_long(m)
    assert m.dispatch_semaphore_locked(PositionSide.LONG) is True    # LONG commitment held
    assert m.dispatch_semaphore_locked(PositionSide.SHORT) is False  # SHORT unaffected


def test_paper_exit_lifecycle_l2_mae_full_close():
    events: list = []
    m = WSManager(Mode.PAPER, on_event=events.append, now_monotonic=lambda: 1234.5)
    _open_paper_long(m)
    assert m.spot_usd_balance == Decimal("1992.2")        # after entry debit
    # ar:AR-043 (D2 position-lifetime): the fill ACQUIRED the LONG dispatch semaphore.
    assert m.dispatch_semaphore_locked(PositionSide.LONG) is True

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
    # step 9: the LONG dispatch semaphore was RELEASED at close (ar:AR-043 D2; free for the next).
    assert m.dispatch_semaphore_locked(PositionSide.LONG) is False
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


def test_exit_controllers_exist_in_live():
    # sec 12.5 LIVE FLOW: the Exit Controller is a pure close engine above the dispatch seam, so it
    # is constructed in BOTH modes - live needs it to emit evt:TRADE_CLOSE off the executions close.
    m = WSManager(Mode.LIVE)
    assert m.exit_controllers is not None
    assert set(m.exit_controllers) == {PositionSide.LONG, PositionSide.SHORT}
    assert m.exit_controller is m.exit_controllers[PositionSide.LONG]


def test_set_ciats_exit_sinks_wires_in_live():
    # the live close emits its TRADE_CLOSE through the same per-module CIATS membrane (sec 12.5
    # LIVE FLOW), so set_ciats_exit_sinks wires the live controllers (no longer a no-op).
    m = WSManager(Mode.LIVE)
    long_sink: list = []
    sink = long_sink.append
    m.set_ciats_exit_sinks({PositionSide.LONG: sink})  # must not raise; wires the sink
    assert m.exit_controllers[PositionSide.LONG]._on_event is sink


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


def test_handle_ticker_no_open_position_is_inert_in_live():
    # handle_ticker over a symbol with NO open position is inert in live (nothing to mark, nothing to
    # detect/enqueue) - the live exit path only engages an open position.
    events: list = []
    m = WSManager(Mode.LIVE, on_event=events.append)
    m.handle_ticker(_ticker("BTC/USD", "1", "2"))
    assert not any(isinstance(e, (TradeClose, LiveExitDetected)) for e in events)
    assert m.live_exit_intents_pending == 0


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
    m = WSManager(Mode.PAPER, on_event=events.append, now_monotonic=lambda: 999.0)
    _open_paper_long(m)
    assert m.spot_usd_balance == Decimal("1992.2")
    assert m.dispatch_semaphore_locked(PositionSide.LONG) is True   # acquired on the fill (D2)

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
    assert m.dispatch_semaphore_locked(PositionSide.LONG) is False  # released at close (D2 step 9)
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


def test_l1a_no_open_position_is_inert_in_live():
    # the L1a regime-exit handlers over a symbol with NO open position are inert in live (no detect,
    # no enqueue, no close, no raise) - the live L1a path only engages an open position.
    events: list = []
    m = WSManager(Mode.LIVE, on_event=events.append)
    m.on_regime_classified("BTC/USD", _trending_neg(), bid="61000", ask="61100")
    m.on_htf_ohlc_close("BTC/USD", "99", "100", bid="61000", ask="61100")
    assert not any(isinstance(e, (TradeClose, LiveExitDetected)) for e in events)
    assert m.live_exit_intents_pending == 0


# -- WS_Manager LIVE EXIT lifecycle (sec 12.5 LIVE FLOW: executions close -> TRADE_CLOSE) ----

def _open_live_long(m, *, atr="2000", emergsl="54000", fee="7.8"):
    """Open a BTC/USD long in LIVE via the executions channel (the live write source, PA-004 div #4):
    a buy fill carrying the entry-time D6 snapshot + the actual taker entry fee (FEE-CALC-006)."""
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "buy",
         "cum_qty": "0.05", "avg_price": "60000", "cl_ord_id": "cl-1", "fee": fee},
        regime_at_entry="TRENDING_POS_NORMAL", atr_14_entry=atr, emergsl_price=emergsl,
    )


def test_live_executions_close_emits_trade_close():
    # sec 12.5 LIVE FLOW: an opposite-side executions fill closes the position and emits the 25-field
    # TRADE_CLOSE off the close fill (no synthetic ledger). Default reason = EMERGENCY_SL_FIRED (the
    # off-book backstop fired - TothBot dispatched no exit, so nothing stamped the reason).
    closes: list = []
    m = WSManager(Mode.LIVE, on_event=closes.append)
    _open_live_long(m)
    assert m.has_position("BTC/USD")
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "sell",
         "cum_qty": "0.05", "avg_price": "66000", "fee": "8.58"}
    )
    assert not m.has_position("BTC/USD")
    tc = [e for e in closes if isinstance(e, TradeClose)]
    assert len(tc) == 1
    rec = tc[0]
    assert rec.exit_reason is ExitReason.EMERGENCY_SL_FIRED
    assert rec.symbol == "BTC/USD"
    assert rec.entry_fill_price == Decimal("60000")
    assert rec.exit_price == Decimal("66000")
    assert rec.qty == Decimal("0.05")
    assert rec.fees_entry_usd == Decimal("7.8") and rec.fees_exit_usd == Decimal("8.58")
    # net P&L (long): gross (66000-60000)*0.05 = 300, minus 7.8 entry, minus 8.58 exit = 283.62.
    assert rec.net_pl_usd == Decimal("283.62")
    assert rec.net_gain_usd == Decimal("283.62") and rec.net_loss_usd == Decimal("0")


def test_live_close_carries_dispatched_exit_reason():
    # a TothBot-dispatched live exit (L1a/L2 market sell) stamps the reason via note_live_exit_dispatch;
    # the executions close fill carries it onto the TRADE_CLOSE (not the emergSL default).
    closes: list = []
    m = WSManager(Mode.LIVE, on_event=closes.append)
    _open_live_long(m)
    m.note_live_exit_dispatch("BTC/USD", ExitReason.MAE_THRESHOLD_BREACH.value)
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "sell",
         "cum_qty": "0.05", "avg_price": "57000", "fee": "7.4"}
    )
    tc = [e for e in closes if isinstance(e, TradeClose)]
    assert len(tc) == 1 and tc[0].exit_reason is ExitReason.MAE_THRESHOLD_BREACH


def test_live_close_releases_g7_semaphore_and_clears_entry_fee():
    # the G7 capital-commitment semaphore acquired at the live OPEN (D2 position-lifetime) is released
    # at the close, and the retained live entry fee is dropped (no leak across trades).
    m = WSManager(Mode.LIVE)
    _open_live_long(m)
    assert m.dispatch_semaphore_locked(PositionSide.LONG)
    assert m.fees_entry_for("BTC/USD") == Decimal("7.8")
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "sell",
         "cum_qty": "0.05", "avg_price": "66000", "fee": "8.58"}
    )
    assert not m.dispatch_semaphore_locked(PositionSide.LONG)
    assert m.fees_entry_for("BTC/USD") is None


def test_live_close_emits_through_the_ciats_sink():
    # the live close emits its TRADE_CLOSE through the side's CIATS membrane when wired (sec 12.5
    # LIVE FLOW parity with the paper close), not the general telemetry sink.
    general: list = []
    long_sink: list = []
    m = WSManager(Mode.LIVE, on_event=general.append)
    m.set_ciats_exit_sinks({PositionSide.LONG: long_sink.append})
    _open_live_long(m)
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "sell",
         "cum_qty": "0.05", "avg_price": "66000", "fee": "8.58"}
    )
    assert len([e for e in long_sink if isinstance(e, TradeClose)]) == 1
    assert not any(isinstance(e, TradeClose) for e in general)


def test_live_short_close_mirror():
    # the Long/Short mirror: a SHORT live position (sell-to-open) closes on a buy-to-cover executions
    # fill; net P&L uses the short leg (entry - exit) * qty.
    closes: list = []
    m = WSManager(Mode.LIVE, on_event=closes.append)
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "sell",
         "cum_qty": "0.05", "avg_price": "60000", "cl_ord_id": "cl-s1", "fee": "7.8"},
        regime_at_entry="TRENDING_NEG_NORMAL", atr_14_entry="2000", emergsl_price="66000",
    )
    assert m.dispatch_semaphore_locked(PositionSide.SHORT)
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "buy",
         "cum_qty": "0.05", "avg_price": "57000", "fee": "7.4"}
    )
    tc = [e for e in closes if isinstance(e, TradeClose)]
    assert len(tc) == 1
    # short gross = (60000 - 57000) * 0.05 = 150; minus 7.8 entry, 7.4 exit = 134.8.
    assert tc[0].net_pl_usd == Decimal("134.8")
    assert not m.dispatch_semaphore_locked(PositionSide.SHORT)


def test_live_handle_ticker_marks_mae_and_a_benign_tick_enqueues_nothing():
    # the live MTM marking: handle_ticker marks the max-over-life MAE for an open position in LIVE
    # (the heat the close reads). A non-breaching tick enqueues NO exit and the position stays open.
    m = WSManager(Mode.LIVE)
    _open_live_long(m)                                      # atr 2000 -> L2 threshold 3000
    m.handle_ticker(_ticker("BTC/USD", "57500", "57600"))  # mae (60000-57500)=2500 < 3000 -> no breach
    assert m.has_position("BTC/USD")
    assert m.live_exit_intents_pending == 0                 # below threshold -> no intent
    assert m.mae_pct_high_for("BTC/USD") == Decimal(str(Decimal("2500") / Decimal("60000")))


def test_live_close_carries_max_over_life_heat_from_ticker_marking():
    # the live close lifts mae_pct_reached to the worst-over-the-hold heat the ticker marking saw -
    # a deep-then-benign trade (deep drawdown, favorable exit) reports the deep heat, not at-exit 0.
    closes: list = []
    m = WSManager(Mode.LIVE, on_event=closes.append)
    _open_live_long(m)
    m.handle_ticker(_ticker("BTC/USD", "57000", "57100"))  # deep heat 0.05 marked while open
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "sell",
         "cum_qty": "0.05", "avg_price": "66000", "fee": "8.58"}   # benign favorable exit (at-exit 0)
    )
    tc = [e for e in closes if isinstance(e, TradeClose)]
    assert len(tc) == 1 and tc[0].mae_pct_reached == Decimal("0.05")
    # the close drops the heat tracking - a reopened symbol starts fresh.
    assert m.mae_pct_high_for("BTC/USD") is None


# -- LIVE EXIT DETECTION -> MARKET-SELL DISPATCH (sec 3 Image3 / sec 4.1 / sec 12.5) ----------

async def _never_reject(message):
    """An add_order RESPONSE that ACCEPTED the order (the C-1 happy path: no MPP reject). The
    test default for the market-reject probe - the analogue of injecting cancel_ack_wait=_always_
    ack, so a dispatch test resolves the reject probe instantly instead of polling the response
    registry. Pass market_rejected=None to exercise the real registry-backed default instead."""
    return False


_REAL_DEFAULT_PROBE = object()  # sentinel: use WSManager's own registry-poll default probe


def _live_manager(*, market_rejected=_REAL_DEFAULT_PROBE, **kw):
    """A LIVE WSManager with an injected live_sender capturing every (op, message) to Kraken. The
    market-reject probe defaults to _never_reject (an accepted order - instant + deterministic);
    pass market_rejected=None to exercise WSManager's registry-poll default, or a stub to drive C-1."""
    sent: list = []
    events: list = []

    async def _live(op, message):
        sent.append((op, message))

    if market_rejected is _REAL_DEFAULT_PROBE:
        market_rejected = _never_reject     # the accepted-order test default
    if market_rejected is not None:
        kw["market_rejected"] = market_rejected   # None -> WSManager's registry-poll default
    mgr = WSManager(Mode.LIVE, live_sender=_live, on_event=events.append, **kw)
    return mgr, sent, events


def _open_live_short(m, *, atr="2000", emergsl="66000", fee="7.8"):
    """Open a BTC/USD SHORT in LIVE via the executions channel: a sell-to-open margin fill carrying
    the entry-time D6 snapshot (emergSL ABOVE entry) + the actual taker entry fee."""
    m.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "sell",
         "cum_qty": "0.05", "avg_price": "60000", "cl_ord_id": "cl-s1", "fee": fee},
        regime_at_entry="TRENDING_NEG_NORMAL", atr_14_entry=atr, emergsl_price=emergsl,
    )


async def _always_ack(cl_ord_id, timeout):
    return True


async def _always_timeout(cl_ord_id, timeout):
    return False


# --- the pure exit-order builders (exit_dispatch.py) ---

def test_build_market_sell_order_long_is_spot_market_sell():
    msg = build_market_sell_order(
        "BTC/USD", PositionSide.LONG, order_qty=Decimal("0.05"), cl_ord_id="x", deadline="d"
    )
    p = msg["params"]
    assert msg["method"] == "add_order"
    assert p["side"] == "sell" and p["order_type"] == "market" and p["order_qty"] == "0.05"
    assert p["stp_type"] == "cancel_newest"
    assert "margin" not in p and "reduce_only" not in p   # spot LONG sell carries neither (ar:AR-009)


def test_build_market_sell_order_short_is_margin_buy_to_cover_reduce_only():
    msg = build_market_sell_order(
        "BTC/USD", PositionSide.SHORT, order_qty=Decimal("0.05"), cl_ord_id="x", deadline="d"
    )
    p = msg["params"]
    assert p["side"] == "buy" and p["order_type"] == "market"   # SHORT buys to cover
    assert p["margin"] is True and p["reduce_only"] is True     # ar:AR-009 closes the margin short only


def test_build_cancel_order_targets_the_emergsl_cl_ord_id():
    msg = build_cancel_order("BTC/USD", cl_ord_id="cl-1-sl", deadline="d")
    assert msg["method"] == "cancel_order"
    assert msg["params"]["cl_ord_id"] == "cl-1-sl" and msg["params"]["symbol"] == "BTC/USD"


def test_mpp_retry_limit_price_walks_out_by_the_diagram_increment():
    # LONG sell walks DOWN from best_bid by 0.2%*n; SHORT buy-to-cover walks UP from best_ask.
    assert mpp_retry_limit_price(PositionSide.LONG, "57000", 1) == Decimal("57000") * Decimal("0.998")
    assert mpp_retry_limit_price(PositionSide.SHORT, "57000", 2) == Decimal("57000") * Decimal("1.004")


def test_build_mpp_retry_order_is_a_marketable_ioc_limit():
    msg = build_mpp_retry_order(
        "BTC/USD", PositionSide.LONG, order_qty=Decimal("0.05"),
        limit_price=Decimal("56886"), cl_ord_id="x", deadline="d",
    )
    p = msg["params"]
    assert p["order_type"] == "limit" and p["time_in_force"] == "ioc"
    assert p["side"] == "sell" and p["limit_price"] == "56886"


# --- the sync detection -> intent enqueue ---

def test_live_ticker_l2_breach_enqueues_a_market_sell_intent():
    mgr, sent, events = _live_manager()
    _open_live_long(mgr)                                       # atr 2000 -> L2 threshold 3000
    mgr.handle_ticker(_ticker("BTC/USD", "57000", "57100"))   # (60000-57000)=3000 >= 3000 breach
    assert mgr.live_exit_intents_pending == 1
    det = [e for e in events if isinstance(e, LiveExitDetected)]
    assert len(det) == 1 and det[0].exit_reason == "MAE_THRESHOLD_BREACH"


def test_live_l3_emergsl_touch_is_not_tothbot_dispatched():
    # the off-book layer:L3 emergSL fires Kraken-side (autonomously on the matching engine); TothBot
    # dispatches NO exit for it. A position with no L2 context (atr=None) only detects the L3 touch.
    mgr, sent, events = _live_manager()
    _open_live_long(mgr, atr=None)                            # no atr -> only the emergSL touch detects
    mgr.handle_ticker(_ticker("BTC/USD", "53000", "53100"))  # bid 53000 <= emergsl 54000 -> L3 touch
    assert mgr.live_exit_intents_pending == 0
    assert not any(isinstance(e, LiveExitDetected) for e in events)


def test_live_double_enqueue_is_guarded():
    mgr, sent, events = _live_manager()
    _open_live_long(mgr)
    mgr.handle_ticker(_ticker("BTC/USD", "57000", "57100"))
    mgr.handle_ticker(_ticker("BTC/USD", "56000", "56100"))   # a second, deeper breach
    assert mgr.live_exit_intents_pending == 1                 # one open position -> one queued intent


def test_live_l1a_htf_reversal_enqueues_an_intent():
    mgr, sent, events = _live_manager()
    _open_live_long(mgr)
    mgr.on_htf_ohlc_close("BTC/USD", "99", "100", bid="61000", ask="61100")  # EMA20 < EMA50
    assert mgr.live_exit_intents_pending == 1
    det = [e for e in events if isinstance(e, LiveExitDetected)]
    assert len(det) == 1 and det[0].trigger == "EC-L1A-001"


# --- the async cancel-then-sell driver ---

def test_live_exit_dispatch_cancel_then_market_sell_happy_path():
    mgr, sent, events = _live_manager(cancel_ack_wait=_always_ack)
    _open_live_long(mgr)
    mgr.handle_ticker(_ticker("BTC/USD", "57000", "57100"))
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.DISPATCHED]
    # SEQUENCE CRITICAL: the emergSL cancel_order THEN the market close (never the reverse).
    assert [op for op, _ in sent] == [OutboundOp.CANCEL_ORDER, OutboundOp.DISPATCH_MARKET_SELL]
    cancel_msg, sell_msg = sent[0][1], sent[1][1]
    assert cancel_msg["method"] == "cancel_order" and cancel_msg["params"]["cl_ord_id"] == "cl-1-sl"
    assert sell_msg["params"]["order_type"] == "market" and sell_msg["params"]["side"] == "sell"
    # the reason is stamped so the executions close fill carries it onto the TRADE_CLOSE.
    assert mgr._pending_exit_reason["BTC/USD"] == ExitReason.MAE_THRESHOLD_BREACH.value
    assert any(isinstance(e, LiveExitDispatched) for e in events)


def test_live_exit_dispatch_then_executions_close_carries_the_reason():
    mgr, sent, events = _live_manager(cancel_ack_wait=_always_ack)
    _open_live_long(mgr)
    mgr.handle_ticker(_ticker("BTC/USD", "57000", "57100"))
    asyncio.run(mgr.drive_live_exits())
    # the close fill arrives on the executions channel and carries the stamped reason (not the
    # EMERGENCY_SL_FIRED default) onto the 25-field TRADE_CLOSE.
    mgr.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "sell",
         "cum_qty": "0.05", "avg_price": "57000", "fee": "7.4"}
    )
    tc = [e for e in events if isinstance(e, TradeClose)]
    assert len(tc) == 1 and tc[0].exit_reason is ExitReason.MAE_THRESHOLD_BREACH
    # the close cleared the in-flight state (no leak across trades).
    assert "BTC/USD" not in mgr._pending_exit_reason
    assert mgr.live_exit_intents_pending == 0


def test_live_short_exit_dispatch_is_a_margin_buy_to_cover():
    mgr, sent, events = _live_manager(cancel_ack_wait=_always_ack)
    _open_live_short(mgr)                                     # entry 60000, emergSL 66000
    mgr.handle_ticker(_ticker("BTC/USD", "62900", "63000"))  # ask 63000: (63000-60000)=3000 >= 3000
    assert mgr.live_exit_intents_pending == 1
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.DISPATCHED]
    sell = [m for op, m in sent if op is OutboundOp.DISPATCH_MARKET_SELL][0]
    assert sell["params"]["side"] == "buy" and sell["params"]["reduce_only"] is True
    assert mgr._pending_exit_reason["BTC/USD"] == ExitReason.MAE_THRESHOLD_BREACH.value


def test_live_exit_dispatch_guard_skips_when_an_exit_is_already_in_flight():
    mgr, sent, events = _live_manager()
    _open_live_long(mgr)
    mgr.note_live_exit_dispatch("BTC/USD", ExitReason.MAE_THRESHOLD_BREACH.value)  # already in flight
    intent = LiveExitIntent(
        "BTC/USD", PositionSide.LONG, "MAE_THRESHOLD_BREACH", "L2_MAE", "L2_MAE", best_quote="57000"
    )
    outcome = asyncio.run(mgr.dispatch_live_exit(intent))
    assert outcome is ExitDispatchOutcome.SKIPPED_IN_FLIGHT
    assert sent == []                                        # no second cancel / sell
    assert any(isinstance(e, LiveExitDoubleDispatchSkipped) for e in events)


# --- the I-6 cancel-timeout fallback ---

def test_live_exit_i6_cancel_timeout_confirmed_by_executions_proceeds():
    mgr, sent, events = _live_manager(cancel_ack_wait=_always_timeout)
    _open_live_long(mgr)
    mgr.record_cancel_ack("cl-1-sl")                         # the executions channel confirms the cancel
    mgr.handle_ticker(_ticker("BTC/USD", "57000", "57100"))
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.DISPATCHED]      # proceeded despite the ACK timeout
    assert any(isinstance(e, CancelAckTimeout) and e.attempt == 1 for e in events)
    assert OutboundOp.DISPATCH_MARKET_SELL in [op for op, _ in sent]


def test_live_exit_i6_second_timeout_holds_ambiguous_and_never_sells():
    mgr, sent, events = _live_manager(cancel_ack_wait=_always_timeout)
    _open_live_long(mgr)
    mgr.handle_ticker(_ticker("BTC/USD", "57000", "57100"))
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.HELD_AMBIGUOUS]
    # two cancel attempts, NO market sell ("NEVER market sell with ambiguous order state").
    assert [op for op, _ in sent] == [OutboundOp.CANCEL_ORDER, OutboundOp.CANCEL_ORDER]
    assert OutboundOp.DISPATCH_MARKET_SELL not in [op for op, _ in sent]
    assert [e.attempt for e in events if isinstance(e, CancelAckTimeout)] == [1, 2]
    assert any(isinstance(e, LiveExitHeldAmbiguous) for e in events)
    # held -> the in-flight stamp released + the symbol suppressed from re-dispatch (operator clears).
    assert "BTC/USD" not in mgr._pending_exit_reason
    mgr.handle_ticker(_ticker("BTC/USD", "57000", "57100"))
    assert mgr.live_exit_intents_pending == 0
    # the operator clears the HOLD and a fresh detection can re-engage.
    mgr.clear_live_exit_held("BTC/USD")
    mgr.handle_ticker(_ticker("BTC/USD", "57000", "57100"))
    assert mgr.live_exit_intents_pending == 1


# --- the C-1 MPP-rejection retry ---

def test_live_exit_c1_mpp_rejection_retries_an_ioc_limit():
    rejects = {"n": 1}

    async def _reject_market_once(message):
        if rejects["n"] > 0 and message["params"]["order_type"] == "market":
            rejects["n"] -= 1
            return True
        return False

    mgr, sent, events = _live_manager(cancel_ack_wait=_always_ack, market_rejected=_reject_market_once)
    _open_live_long(mgr)
    mgr.handle_ticker(_ticker("BTC/USD", "57000", "57100"))
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.DISPATCHED]
    sells = [m for op, m in sent if op is OutboundOp.DISPATCH_MARKET_SELL]
    assert sells[0]["params"]["order_type"] == "market"      # the rejected market close
    assert sells[1]["params"]["order_type"] == "limit"       # the accepted C-1 IOC retry
    assert sells[1]["params"]["time_in_force"] == "ioc"
    assert sells[1]["params"]["limit_price"] == str(Decimal("57000") * Decimal("0.998"))  # 0.2%*1
    assert any(isinstance(e, MppRejectRetry) and e.attempt == 1 for e in events)


def test_live_exit_c1_mpp_all_retries_rejected_holds_and_alerts():
    async def _reject_all(message):
        return True

    mgr, sent, events = _live_manager(cancel_ack_wait=_always_ack, market_rejected=_reject_all)
    _open_live_long(mgr)
    mgr.handle_ticker(_ticker("BTC/USD", "57000", "57100"))
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.HELD_MPP_EXHAUSTED]
    # the market close + all param:mpp_retry_count (3) IOC retries were sent, all rejected.
    sells = [m for op, m in sent if op is OutboundOp.DISPATCH_MARKET_SELL]
    assert len(sells) == 1 + 3
    assert any(isinstance(e, LiveExitMppExhausted) for e in events)
    assert "BTC/USD" in mgr._live_exit_held


# --- the C-1 default MPP-reject probe over the order-response registry ---

class _AdvancingClock:
    """A monotonic clock that jumps far ahead after its first read, so a registry-poll wait
    crosses its deadline on the next check (deterministic timeout, no real sleeping)."""

    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        v = self.t
        self.t += 1000.0
        return v


def test_default_market_rejected_true_when_reject_recorded():
    mgr, *_ = _live_manager()  # no market_rejected injected -> the default registry probe
    mgr.record_order_response("X-1", rejected=True)
    msg = {"method": "add_order", "params": {"cl_ord_id": "X-1"}}
    assert asyncio.run(mgr._default_market_rejected(msg)) is True


def test_default_market_rejected_false_and_short_circuits_on_accept():
    mgr, *_ = _live_manager()
    mgr.record_order_response("X-2", rejected=False)   # an accept resolves it immediately
    msg = {"method": "add_order", "params": {"cl_ord_id": "X-2"}}
    assert asyncio.run(mgr._default_market_rejected(msg)) is False


def test_default_market_rejected_false_on_no_response_within_window():
    mgr, *_ = _live_manager(now_monotonic=_AdvancingClock())
    msg = {"method": "add_order", "params": {"cl_ord_id": "NEVER"}}
    assert asyncio.run(mgr._default_market_rejected(msg)) is False  # window elapsed, no response


def test_default_market_rejected_false_when_message_has_no_cl_ord_id():
    mgr, *_ = _live_manager()
    assert asyncio.run(mgr._default_market_rejected({"method": "add_order", "params": {}})) is False


def test_default_market_rejected_drives_the_full_c1_retry_via_registry():
    # End-to-end: the DEFAULT probe (no injected market_rejected) reads the registry. Pre-record a
    # reject for the FIRST exit order (the market close cl_ord_id is monotonic "<sym>-exit-1") and an
    # accept for the first IOC retry ("<sym>-mpp1-2"), so the driver walks out exactly one IOC retry.
    mgr, sent, events = _live_manager(cancel_ack_wait=_always_ack, market_rejected=None)
    _open_live_long(mgr)
    mgr.record_order_response("BTC/USD-exit-1", rejected=True)
    mgr.record_order_response("BTC/USD-mpp1-2", rejected=False)
    mgr.handle_ticker(_ticker("BTC/USD", "57000", "57100"))
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.DISPATCHED]
    sells = [m for op, m in sent if op is OutboundOp.DISPATCH_MARKET_SELL]
    assert sells[0]["params"]["order_type"] == "market"   # rejected per the registry
    assert sells[1]["params"]["order_type"] == "limit"    # the C-1 IOC retry that the accept cleared
    assert any(isinstance(e, MppRejectRetry) and e.attempt == 1 for e in events)


# --- the AR-040 pair-status precondition (Step 1) ---

def test_live_exit_ar040_pair_status_holds_before_any_order():
    mgr, sent, events = _live_manager(cancel_ack_wait=_always_ack)
    _open_live_long(mgr)
    # an L1a downgrade fires, but the pair is cancel_only -> HOLD, NO cancel, NO sell (ar:AR-040).
    mgr.on_regime_classified(
        "BTC/USD", _trending_neg(), bid="61000", ask="61100", pair_status=PairStatus.CANCEL_ONLY
    )
    assert mgr.live_exit_intents_pending == 1
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.HELD_PAIR_STATUS]
    assert sent == []                                        # NOTHING transmitted (resting emergSL holds)
    assert any(isinstance(e, L1aExitHeld) for e in events)
    assert "BTC/USD" not in mgr._pending_exit_reason          # never stamped (no dispatch)


# --- the HR-EC-016(b) Layer-2 priority over an in-progress / queued Layer-1a sequence ---

def test_live_hr_ec_016b_l2_breach_overrides_in_flight_l1a_reason():
    # HR-EC-016(b): an L2 MAE breach arriving WHILE the L1a cancel sequence is underway (the cancel
    # ACK is being awaited, the market sell not yet emitted) overrides the reason to L2 - the SAME
    # cancel-then-sell completes (one close), the original L1a reason suppressed.
    holder: dict = {}

    async def _ack_then_l2_breach(cl_ord_id, timeout):
        # the read loop runs an L2-breaching ticker during the in-flight cancel await.
        holder["m"].handle_ticker(_ticker("BTC/USD", "57000", "57100"))
        return True

    mgr, sent, events = _live_manager(cancel_ack_wait=_ack_then_l2_breach)
    holder["m"] = mgr
    _open_live_long(mgr)
    mgr.on_htf_ohlc_close("BTC/USD", "99", "100", bid="61000", ask="61100")  # an L1a reversal first
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.DISPATCHED]
    # one cancel-then-sell (one close, never two), reason overridden to L2.
    assert [op for op, _ in sent] == [OutboundOp.CANCEL_ORDER, OutboundOp.DISPATCH_MARKET_SELL]
    assert mgr._pending_exit_reason["BTC/USD"] == ExitReason.MAE_THRESHOLD_BREACH.value
    assert any(isinstance(e, LiveExitPriorityOverride) for e in events)
    disp = [e for e in events if isinstance(e, LiveExitDispatched)][0]
    assert disp.exit_reason == ExitReason.MAE_THRESHOLD_BREACH.value     # the FINAL (L2) reason


def test_live_hr_ec_016b_l2_breach_upgrades_a_queued_l1a_intent():
    # the queued-but-not-yet-dispatched variant: an L2 breach upgrades the queued L1a intent's reason
    # in place (still ONE intent -> one dispatch), not a second exit.
    mgr, sent, events = _live_manager(cancel_ack_wait=_always_ack)
    _open_live_long(mgr)
    mgr.on_htf_ohlc_close("BTC/USD", "99", "100", bid="61000", ask="61100")  # queue an L1a intent
    assert mgr.live_exit_intents_pending == 1
    mgr.handle_ticker(_ticker("BTC/USD", "57000", "57100"))                 # L2 breach upgrades it
    assert mgr.live_exit_intents_pending == 1                               # still one intent
    assert any(isinstance(e, LiveExitPriorityOverride) for e in events)
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.DISPATCHED]
    assert mgr._pending_exit_reason["BTC/USD"] == ExitReason.MAE_THRESHOLD_BREACH.value


# --- the AR-040 PAIR_LIMIT_ONLY_EXIT active exit (the 4th Exit-Controller reason) ---

def test_build_limit_only_exit_order_long_is_ioc_limit_at_best_bid():
    msg = build_limit_only_exit_order(
        "BTC/USD", PositionSide.LONG, order_qty=Decimal("0.05"),
        limit_price=Decimal("61000"), cl_ord_id="x", deadline="d",
    )
    p = msg["params"]
    assert p["side"] == "sell" and p["order_type"] == "limit" and p["time_in_force"] == "ioc"
    assert p["limit_price"] == "61000" and "margin" not in p


def test_build_limit_only_exit_order_short_is_margin_buy_to_cover_ioc_limit():
    msg = build_limit_only_exit_order(
        "BTC/USD", PositionSide.SHORT, order_qty=Decimal("0.05"),
        limit_price=Decimal("61100"), cl_ord_id="x", deadline="d",
    )
    p = msg["params"]
    assert p["side"] == "buy" and p["order_type"] == "limit" and p["time_in_force"] == "ioc"
    assert p["margin"] is True and p["reduce_only"] is True


def test_live_limit_only_enqueues_an_active_exit_intent():
    mgr, sent, events = _live_manager()
    _open_live_long(mgr)
    mgr.on_instrument_status("BTC/USD", PairStatus.LIMIT_ONLY, bid="61000", ask="61100")
    assert mgr.live_exit_intents_pending == 1
    det = [e for e in events if isinstance(e, LiveExitDetected)]
    assert len(det) == 1 and det[0].exit_reason == ExitReason.PAIR_LIMIT_ONLY_EXIT.value


def test_live_limit_only_dispatch_cancels_then_single_ioc_limit_close():
    mgr, sent, events = _live_manager(cancel_ack_wait=_always_ack)
    _open_live_long(mgr)
    mgr.on_instrument_status("BTC/USD", PairStatus.LIMIT_ONLY, bid="61000", ask="61100")
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.DISPATCHED]
    # cancel the emergSL FIRST (HR-EC-013), then a SINGLE IOC limit close at best_bid (NOT a market).
    assert [op for op, _ in sent] == [OutboundOp.CANCEL_ORDER, OutboundOp.DISPATCH_MARKET_SELL]
    close = sent[1][1]["params"]
    assert close["order_type"] == "limit" and close["time_in_force"] == "ioc"
    assert close["side"] == "sell" and close["limit_price"] == "61000"   # best_bid for a long
    assert mgr._pending_exit_reason["BTC/USD"] == ExitReason.PAIR_LIMIT_ONLY_EXIT.value


def test_live_limit_only_short_is_buy_to_cover_at_best_ask():
    mgr, sent, events = _live_manager(cancel_ack_wait=_always_ack)
    _open_live_short(mgr)
    mgr.on_instrument_status("BTC/USD", PairStatus.LIMIT_ONLY, bid="61000", ask="61100")
    asyncio.run(mgr.drive_live_exits())
    close = [m for op, m in sent if op is OutboundOp.DISPATCH_MARKET_SELL][0]["params"]
    assert close["side"] == "buy" and close["limit_price"] == "61100"   # best_ask for a short
    assert close["reduce_only"] is True


def test_live_limit_only_defers_when_no_realizable_quote():
    mgr, sent, events = _live_manager(cancel_ack_wait=_always_ack)
    _open_live_long(mgr)
    mgr.on_instrument_status("BTC/USD", PairStatus.LIMIT_ONLY, bid=None, ask="61100")  # no bid (long)
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.NO_QUOTE]
    assert any(isinstance(e, LiveExitDeferredNoQuote) for e in events)
    assert "BTC/USD" not in mgr._pending_exit_reason          # the stamp released on the defer
    assert OutboundOp.DISPATCH_MARKET_SELL not in [op for op, _ in sent]   # no close order out


def test_live_limit_only_then_executions_close_carries_the_reason():
    mgr, sent, events = _live_manager(cancel_ack_wait=_always_ack)
    _open_live_long(mgr)
    mgr.on_instrument_status("BTC/USD", PairStatus.LIMIT_ONLY, bid="61000", ask="61100")
    asyncio.run(mgr.drive_live_exits())
    mgr.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "sell",
         "cum_qty": "0.05", "avg_price": "61000", "fee": "7.9"}
    )
    tc = [e for e in events if isinstance(e, TradeClose)]
    assert len(tc) == 1 and tc[0].exit_reason is ExitReason.PAIR_LIMIT_ONLY_EXIT


def test_live_limit_only_other_statuses_are_noops():
    # only limit_only triggers the active exit; online / cancel_only / maintenance enqueue nothing
    # here (cancel_only / maintenance HOLD an L1a/L2 exit at dispatch, they do not themselves fire one).
    mgr, sent, events = _live_manager()
    _open_live_long(mgr)
    for status in (PairStatus.ONLINE, PairStatus.CANCEL_ONLY, PairStatus.MAINTENANCE):
        mgr.on_instrument_status("BTC/USD", status, bid="61000", ask="61100")
    assert mgr.live_exit_intents_pending == 0


# --- the AR-056 reconnect gap-close TRADE_CLOSE (an emergSL fired while offline) ---

def test_reconnect_gap_close_emits_trade_close_from_the_actual_fill():
    events: list = []
    m = WSManager(Mode.LIVE, on_event=events.append)
    _open_live_long(m)                                  # entry 60000, emergsl 54000, fee 7.8, qty 0.05
    assert m.dispatch_semaphore_locked(PositionSide.LONG)
    gaps = m.restore_position_mirror([])               # empty snapshot -> BTC/USD closed during the gap
    assert len(gaps) == 1 and gaps[0].symbol == "BTC/USD"
    # the ACTUAL emergSL fill from REST QueryOrders / ownTrades (FEE-CALC-006 record-of-truth).
    rec = m.on_reconnect_gap_close(gaps[0], exit_price="53900", fees_exit="7.0")
    assert rec.exit_reason is ExitReason.EMERGENCY_SL_FIRED
    assert rec.exit_price == Decimal("53900") and rec.fees_exit_usd == Decimal("7.0")
    # long net: (53900-60000)*0.05 = -305; -7.8 entry -7.0 exit = -319.8 (a loss).
    assert rec.net_pl_usd == Decimal("-319.8") and rec.net_loss_usd == Decimal("319.8")
    # the gap-close releases the G7 semaphore + clears the retained entry fee (no leak across trades).
    assert not m.dispatch_semaphore_locked(PositionSide.LONG)
    assert m.fees_entry_for("BTC/USD") is None
    assert not any(isinstance(e, GapCloseEstimated) for e in events)


def test_reconnect_gap_close_falls_back_to_the_emergsl_estimate_when_no_actual_fill():
    events: list = []
    m = WSManager(Mode.LIVE, on_event=events.append)
    _open_live_long(m)
    gaps = m.restore_position_mirror([])
    rec = m.on_reconnect_gap_close(gaps[0])             # no actual fill -> estimate from emergsl 54000
    assert rec.exit_price == Decimal("54000")
    assert rec.fees_exit_usd == Decimal("0.05") * Decimal("54000") * Decimal(str(0.0026))
    est = [e for e in events if isinstance(e, GapCloseEstimated)]
    assert len(est) == 1 and est[0].estimated_exit_price == Decimal("54000")


def test_reconnect_gap_close_short_mirror():
    m = WSManager(Mode.LIVE)
    _open_live_short(m)                                 # entry 60000, emergsl 66000, fee 7.8
    assert m.dispatch_semaphore_locked(PositionSide.SHORT)
    gaps = m.restore_position_mirror([])
    rec = m.on_reconnect_gap_close(gaps[0], exit_price="66100", fees_exit="7.5")
    assert rec.exit_reason is ExitReason.EMERGENCY_SL_FIRED
    # short net: (60000-66100)*0.05 = -305; -7.8 -7.5 = -320.3 (a loss).
    assert rec.net_pl_usd == Decimal("-320.3")
    assert not m.dispatch_semaphore_locked(PositionSide.SHORT)


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


# -- the LIVE ENTRY dispatch (slice a: PA-004 div #4 async entry; the fill arrives later) --------

def test_dispatch_entry_live_long_transmits_marketable_ioc_returns_dispatched():
    mgr, sent, events = _live_manager()
    dispatched = asyncio.run(mgr.dispatch_entry(
        PositionSide.LONG, "BTC/USD",
        order_qty="0.05", entry_limit_price="60000", emergsl_price="57000",
        atr_14_entry="1000", regime_at_entry="TRENDING_POS_NORMAL",
        cl_ord_id="cl-L1", deadline="2026-06-16T07:30:00Z",
        signal_params={"rsi_14": 42, "side": "long"},
        market_regime="TRENDING_POS_ELEVATED", entry_timestamp_utc="2026-06-16T07:25:00+00:00",
    ))
    # LIVE returns dispatched=True (the add_order went out); the fill is async, so NO position yet.
    assert dispatched is True
    assert mgr.position("BTC/USD") is None
    # exactly the marketable-IOC entry add_order traversed the seam - NOT the emergSL (that is placed
    # on the async fill, slice b), so no batch_add here.
    ops = [op for op, _ in sent]
    assert ops == [OutboundOp.ADD_ORDER]
    _, msg = sent[0]
    p = msg["params"]
    assert p["side"] == "buy" and p["order_type"] == "limit" and p["time_in_force"] == "ioc"
    assert p["order_qty"] == "0.05" and p["limit_price"] == "60000" and p["cl_ord_id"] == "cl-L1"
    assert "margin" not in p   # spot LONG buy-to-open
    # the entry-time D6 snapshot is RETAINED in the Pending Order Registry for the async opening fill
    # to attach (the paper path pops inline; live keeps it until record_execution OPENED).
    assert mgr._pending_entries["BTC/USD"]["emergsl_price"] == "57000"
    assert mgr._pending_entries["BTC/USD"]["entry_timestamp_utc"] == "2026-06-16T07:25:00+00:00"


def test_dispatch_entry_live_short_is_margin_sell_to_open():
    mgr, sent, _ = _live_manager()
    dispatched = asyncio.run(mgr.dispatch_entry(
        PositionSide.SHORT, "BTC/USD",
        order_qty="0.05", entry_limit_price="60000", emergsl_price="63000",
        cl_ord_id="cl-S1", deadline="2026-06-16T07:30:00Z",
    ))
    assert dispatched is True
    _, msg = sent[0]
    p = msg["params"]
    assert p["side"] == "sell" and p["margin"] is True   # SHORT sell-to-open on Kraken margin (AR-009)


# -- slice b: the AR-054 ON-FILL emergSL (the opening fill enqueues; the after_batch pump places) --

def test_live_opening_fill_enqueues_on_fill_emergsl_from_actual_qty():
    mgr, sent, events = _live_manager()
    # the ASYNC opening fill on the executions channel, carrying the entry-time D6 snapshot.
    mgr.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "buy",
         "cum_qty": "0.05", "avg_price": "60000", "cl_ord_id": "cl-L1", "fee": "7.8"},
        regime_at_entry="TRENDING_POS_NORMAL", atr_14_entry="1000", emergsl_price="57000",
    )
    # AR-054: the opening fill enqueued the on-fill emergSL, built from the ACTUAL filled qty + the
    # entry '-sl' cl_ord_id + the D6 emergsl_price; the Pending Order Registry entry resolved (popped).
    assert len(mgr._pending_emergsl) == 1
    intent = mgr._pending_emergsl[0]
    assert intent.side is PositionSide.LONG and intent.qty == Decimal("0.05")
    assert intent.emergsl_price == Decimal("57000") and intent.cl_ord_id == "cl-L1-sl"
    assert "BTC/USD" not in mgr._pending_entries
    # nothing was transmitted on the fill itself - the emergSL is placed by the pump, not inline.
    assert sent == []


def test_drive_pending_emergsls_places_long_sell_stop_below_entry():
    mgr, sent, events = _live_manager()
    mgr.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "buy",
         "cum_qty": "0.05", "avg_price": "60000", "cl_ord_id": "cl-L1", "fee": "7.8"},
        regime_at_entry="TRENDING_POS_NORMAL", atr_14_entry="1000", emergsl_price="57000",
    )
    placed = asyncio.run(mgr.drive_pending_emergsls())
    assert placed == ["cl-L1-sl"] and mgr._pending_emergsl == []
    op, msg = sent[-1]
    assert op is OutboundOp.BATCH_ADD
    leg = msg["params"]["orders"][0]
    assert leg["side"] == "sell" and leg["order_type"] == "stop-loss"        # LONG sell-stop
    assert leg["triggers"]["price"] == "57000" and leg["triggers"]["price_type"] == "static"
    assert "reduce_only" not in leg                                          # spot long carries none
    assert any(isinstance(e, EmergSlPlaced) and e.cl_ord_id == "cl-L1-sl" for e in events)


def test_drive_pending_emergsls_short_buy_to_cover_reduce_only_above_entry():
    mgr, sent, _ = _live_manager()
    _open_live_short(mgr)   # BTC/USD short, emergsl 66000 ABOVE entry, cl_ord_id cl-s1
    asyncio.run(mgr.drive_pending_emergsls())
    leg = sent[-1][1]["params"]["orders"][0]
    assert leg["side"] == "buy" and leg["reduce_only"] is True               # buy-to-cover, AR-009
    assert leg["triggers"]["price"] == "66000"


def test_on_fill_emergsl_skipped_without_d6_snapshot():
    # a restore/degraded open (no emergsl_price) does NOT enqueue a placement - its resting emergSL
    # was already on Kraken from the original open; the gap-close path owns that case.
    mgr, sent, _ = _live_manager()
    mgr.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "buy",
         "cum_qty": "0.05", "avg_price": "60000", "cl_ord_id": "cl-L1", "fee": "7.8"},
    )
    assert mgr._pending_emergsl == []


def test_drive_after_batch_places_emergsl_before_driving_exits():
    mgr, sent, _ = _live_manager(cancel_ack_wait=_always_ack)
    # symbol A: an opening fill enqueues its on-fill emergSL (AR-054).
    mgr.record_execution(
        {"exec_type": "filled", "symbol": "ETH/USD", "side": "buy",
         "cum_qty": "1.0", "avg_price": "3000", "cl_ord_id": "cl-A", "fee": "1"},
        regime_at_entry="TRENDING_POS_NORMAL", atr_14_entry="50", emergsl_price="2850",
    )
    # symbol B: an open SHORT (its own on-fill emergSL also queues) with a queued live exit.
    _open_live_short(mgr)
    mgr._enqueue_live_exit(LiveExitIntent(
        "BTC/USD", PositionSide.SHORT, ExitReason.MAE_THRESHOLD_BREACH.value,
        trigger="L2_MAE", layer="L2_MAE", best_quote="60000",
    ))
    asyncio.run(mgr.drive_after_batch())
    ops = [op for op, _ in sent]
    # the on-fill emergSL placements (BATCH_ADD) all precede the exit cancel (CANCEL_ORDER).
    assert ops.index(OutboundOp.BATCH_ADD) < ops.index(OutboundOp.CANCEL_ORDER)
    assert mgr._pending_emergsl == [] and mgr._live_exit_intents == []


# -- slice c: the RL-MON-003 entry-suppression gate (entry-only; exits/cancels NEVER gated) --------

def test_dispatch_entry_live_suppressed_sends_no_order_emits_event():
    mgr, sent, events = _live_manager()
    mgr.set_entry_suppression_check(lambda symbol: True)   # the pair armed above critical
    dispatched = asyncio.run(mgr.dispatch_entry(
        PositionSide.LONG, "BTC/USD",
        order_qty="0.05", entry_limit_price="60000", emergsl_price="57000",
        cl_ord_id="cl-L1", deadline="2026-06-16T07:30:00Z",
    ))
    # RL-MON-003: NO order on the wire, returns not-dispatched, the suppression is surfaced, and the
    # Pending Order Registry was not touched (nothing to resolve later).
    assert dispatched is False
    assert sent == []
    assert any(isinstance(e, EntrySuppressed) and e.symbol == "BTC/USD" for e in events)
    assert "BTC/USD" not in mgr._pending_entries


def test_dispatch_entry_live_not_suppressed_dispatches():
    mgr, sent, _ = _live_manager()
    mgr.set_entry_suppression_check(lambda symbol: False)
    dispatched = asyncio.run(mgr.dispatch_entry(
        PositionSide.LONG, "BTC/USD",
        order_qty="0.05", entry_limit_price="60000", emergsl_price="57000",
        cl_ord_id="cl-L1", deadline="2026-06-16T07:30:00Z",
    ))
    assert dispatched is True and [op for op, _ in sent] == [OutboundOp.ADD_ORDER]


def test_entry_suppression_never_gates_the_exit_path():
    # the loss-prevention invariant: a live exit ALWAYS dispatches even while entry is suppressed
    # (the suppression preserves the exit rate budget; a gated exit would leave a position unmanaged).
    mgr, sent, _ = _live_manager(cancel_ack_wait=_always_ack)
    mgr.set_entry_suppression_check(lambda symbol: True)
    _open_live_short(mgr)
    mgr._pending_emergsl.clear()   # isolate the exit path from the on-fill emergSL placement
    mgr._enqueue_live_exit(LiveExitIntent(
        "BTC/USD", PositionSide.SHORT, ExitReason.MAE_THRESHOLD_BREACH.value,
        trigger="L2_MAE", layer="L2_MAE", best_quote="60000",
    ))
    outcomes = asyncio.run(mgr.drive_live_exits())
    assert outcomes == [ExitDispatchOutcome.DISPATCHED]
    ops = [op for op, _ in sent]
    assert OutboundOp.CANCEL_ORDER in ops and OutboundOp.DISPATCH_MARKET_SELL in ops


# -- the LIVE wallet cache ingest (WS-BAL-002/003 - the live G8 sizer's wallet source) ------------

def test_ingest_balances_snapshot_then_update_feeds_live_wallet_reads():
    m, _, events = _live_manager()
    m.ingest_balances({"channel": "balances", "type": "snapshot", "data": [
        {"asset": "USD", "wallets": [
            {"type": "spot", "id": "main", "balance": "5000.0"},
            {"type": "margin", "id": "m1", "balance": "3000.0"}]}]})
    # Long reads spot/main, Short reads margin (WS-BAL-002, symmetric).
    assert m.live_spot_wallet_usd() == Decimal("5000.0")
    assert m.live_margin_wallet_usd() == Decimal("3000.0")
    # an update merges only the changed spot wallet; margin is retained (WS-BAL-003).
    m.ingest_balances({"channel": "balances", "type": "update", "data": [
        {"asset": "USD", "wallets": [{"type": "spot", "id": "main", "balance": "4200.0"}]}]})
    assert m.live_spot_wallet_usd() == Decimal("4200.0")
    assert m.live_margin_wallet_usd() == Decimal("3000.0")
    assert any(isinstance(e, BalancesSnapshotApplied) for e in events)
    assert any(isinstance(e, BalancesUpdated) for e in events)


def test_reset_balances_cache_drops_stale_for_reconnect_reseed():
    m, _, _ = _live_manager()
    m.ingest_balances({"type": "snapshot", "data": [
        {"asset": "USD", "wallets": [{"type": "spot", "id": "main", "balance": "5000.0"}]}]})
    m.reset_balances_cache()
    assert m.live_spot_wallet_usd() is None and m.live_margin_wallet_usd() is None


def test_paper_wallet_balance_unchanged_live_reads_the_cache():
    # paper still reads the synthetic per-module wallet; live wallet_balance(side) now reads the live
    # BalancesCache (AR-051 realized cash) - None before the first balances frame, the cache value
    # after (LONG spot/main, SHORT margin - symmetric WS-BAL-002).
    p = WSManager(Mode.PAPER, paper_starting_balance="5000")
    assert p.wallet_balance(PositionSide.LONG) == Decimal("5000")
    m, _, _ = _live_manager()
    assert m.wallet_balance(PositionSide.LONG) is None     # no balances frame yet
    assert m.wallet_balance(PositionSide.SHORT) is None
    m.ingest_balances({"type": "snapshot", "data": [
        {"asset": "USD", "wallets": [
            {"type": "spot", "id": "main", "balance": "5000.0"},
            {"type": "margin", "id": "m1", "balance": "3000.0"}]}]})
    assert m.wallet_balance(PositionSide.LONG) == Decimal("5000.0")    # spot/main (AR-050 long)
    assert m.wallet_balance(PositionSide.SHORT) == Decimal("3000.0")   # margin cash (AR-050 short)


def test_live_portfolio_baseline_captured_once_paper_reads_the_ledger():
    # PAPER: portfolio_baseline(side) reads the module ledger's captured baseline (the starting wallet).
    p = WSManager(Mode.PAPER, paper_starting_balance="5000")
    assert p.portfolio_baseline(PositionSide.LONG) == Decimal("5000")
    assert p.portfolio_baseline(PositionSide.SHORT) == Decimal("5000")
    # LIVE: None until the REST-BAL-004 startup capture; then the captured USD, per-module symmetric.
    m, _, _ = _live_manager()
    assert m.portfolio_baseline(PositionSide.LONG) is None
    m.set_live_portfolio_baseline(PositionSide.LONG, "5000")     # REST-BAL-004 spot
    m.set_live_portfolio_baseline(PositionSide.SHORT, "3000")    # REST-BAL-004 margin-account
    assert m.portfolio_baseline(PositionSide.LONG) == Decimal("5000")
    assert m.portfolio_baseline(PositionSide.SHORT) == Decimal("3000")


def test_live_portfolio_baseline_not_overwritten_on_reconnect_recapture():
    # HR-WM-011 / AR-056: captured ONCE at startup; a reconnect re-running the capture is IGNORED, and
    # reset_balances_cache (the WS-REC-004 wallet drop) NEVER touches the baseline.
    m, _, _ = _live_manager()
    m.set_live_portfolio_baseline(PositionSide.LONG, "5000")
    m.set_live_portfolio_baseline(PositionSide.LONG, "4200")   # a later recapture - ignored
    assert m.portfolio_baseline(PositionSide.LONG) == Decimal("5000")
    m.reset_balances_cache()                                    # reconnect drops the wallet cache...
    assert m.portfolio_baseline(PositionSide.LONG) == Decimal("5000")   # ...but not the baseline
