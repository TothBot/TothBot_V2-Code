"""mod:Exit_Controller close-path tests (0500000 dv1_242 sec 12.5 + sec 7 Image6).

Drives the sec-12.5 close sequence over a fake WSManager surface (the established
inject-the-sole-writer-surfaces pattern): net P&L per D1 FEE-CALC-004, the 24-field
evt:TRADE_CLOSE record, the HR-PM-009 mirror clear, the AR-073 Selection-Controller
win/loss update, and the BoundedSemaphore release.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from tothbot.exchange.position_mirror import Position, PositionSide
from tothbot.execution.exit_controller import (
    ExitController,
    ExitReason,
    PaperCloseSkipped,
    TradeClose,
)


class _FakeWM:
    """A stand-in for the WSManager sole-writer surfaces the Exit Controller calls."""

    def __init__(self, position, fees_entry, mae_high=None):
        self._position = position
        self._fees_entry = fees_entry
        self._mae_high = mae_high
        self.closed = []
        self.sc_updates = []
        self.sem_released = 0

    def position(self, symbol):
        return self._position if self._position and self._position.symbol == symbol else None

    def fees_entry_for(self, symbol):
        return self._fees_entry

    def mae_pct_high_for(self, symbol):
        return self._mae_high

    def close_position(self, symbol):
        self.closed.append(symbol)
        self._position = None
        return None

    def update_selection_state_on_close(self, symbol, is_win, side=None):
        self.sc_updates.append((symbol, is_win, side))

    def release_exit_semaphore(self, side=None):
        self.sem_released += 1
        self.sem_released_side = side


_CLOCK = lambda: datetime(2026, 6, 15, 0, 45, 0, tzinfo=timezone.utc)


def _pos(side=PositionSide.LONG, entry="60000", qty="0.05", atr="2000",
         regime="TRENDING_POS_NORMAL", signal_params=None, market_regime=None,
         entry_timestamp_utc=None):
    return Position(
        symbol="BTC/USD",
        side=side,
        qty=Decimal(qty),
        avg_entry_price=Decimal(entry),
        atr_14_entry=Decimal(atr) if atr is not None else None,
        regime_at_entry=regime,
        signal_params=signal_params,
        market_regime=market_regime,
        entry_timestamp_utc=entry_timestamp_utc,
    )


def _ec(events):
    return ExitController(on_event=events.append, clock=_CLOCK)


def test_long_win_net_pnl_and_trade_close_record():
    events = []
    wm = _FakeWM(_pos(), fees_entry=Decimal("7.8"))
    rec = _ec(events).on_paper_close(
        "BTC/USD", "66000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm
    )
    assert isinstance(rec, TradeClose)
    # D1 FEE-CALC-004: (66000-60000)*0.05 - 7.8 - 8.58 = 283.62
    assert rec.net_pl_usd == Decimal("283.62")
    assert rec.net_gain_usd == Decimal("283.62")
    assert rec.net_loss_usd == Decimal("0")
    assert rec.fees_total_usd == Decimal("16.38")
    # the 24-field record identity + carried fields
    assert (rec.event, rec.level, rec.component) == ("TRADE_CLOSE", "INFO", "EXIT_CTRL")
    assert rec.symbol == "BTC/USD"
    assert rec.entry_fill_price == Decimal("60000")
    assert rec.exit_price == Decimal("66000")
    assert rec.exit_reason is ExitReason.MAE_THRESHOLD_BREACH
    assert rec.vol_regime == "NORMAL_VOL"
    assert rec.asset_regime == "TRENDING_POS_NORMAL"
    assert rec.ts == "2026-06-15T00:45:00+00:00"
    # actual_RR = net_pl / (atr_14_entry * mae_mult(1.5) * qty) = 283.62 / (2000*1.5*0.05=150)
    assert rec.actual_rr == Decimal("283.62") / Decimal("150")
    # a favorable-side exit reached no adverse excursion at exit
    assert rec.mae_pct_reached == Decimal("0")
    # the record is emitted to the sink
    assert any(isinstance(e, TradeClose) for e in events)


def test_long_win_drives_close_clear_scwin_and_semaphore():
    wm = _FakeWM(_pos(), fees_entry=Decimal("7.8"))
    _ec([]).on_paper_close("BTC/USD", "66000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm)
    assert wm.closed == ["BTC/USD"]            # step 7 mirror clear (HR-PM-009)
    assert wm.sc_updates == [("BTC/USD", True, PositionSide.LONG)]  # step 8 AR-073 win, per-side
    assert wm.sem_released == 1                  # step 9 semaphore release


def test_on_live_close_emits_and_updates_but_does_not_clear_mirror():
    # sec 12.5 LIVE FLOW: on_live_close runs steps 6/8/9 (emit TRADE_CLOSE, SC update, semaphore
    # release) byte-identical to the paper close, but takes the position DIRECTLY (the executions
    # fill already cleared the mirror) so step 7 is skipped - no wm.close_position call.
    events = []
    pos = _pos()
    wm = _FakeWM(pos, fees_entry=Decimal("7.8"))
    rec = _ec(events).on_live_close(pos, "66000", ExitReason.EMERGENCY_SL_FIRED, "8.58", wm)
    assert isinstance(rec, TradeClose)
    assert rec.net_pl_usd == Decimal("283.62")        # identical net-P&L math to the paper close
    assert rec.exit_reason is ExitReason.EMERGENCY_SL_FIRED
    assert any(isinstance(e, TradeClose) for e in events)
    assert wm.closed == []                              # step 7 SKIPPED (no double-close)
    assert wm.sc_updates == [("BTC/USD", True, PositionSide.LONG)]  # step 8 still runs
    assert wm.sem_released == 1                          # step 9 still runs


def test_long_loss_increments_consecutive_loss_and_reaches_mae():
    wm = _FakeWM(_pos(), fees_entry=Decimal("7.8"))
    rec = _ec([]).on_paper_close(
        "BTC/USD", "54000", ExitReason.EMERGENCY_SL_FIRED, "7.02", wm
    )
    # (54000-60000)*0.05 - 7.8 - 7.02 = -314.82  -> loss
    assert rec.net_pl_usd == Decimal("-314.82")
    assert rec.net_gain_usd == Decimal("0")
    assert rec.net_loss_usd == Decimal("314.82")  # positive value if loss
    # adverse excursion at exit: (60000-54000)/60000 = 0.1
    assert rec.mae_pct_reached == Decimal("0.1")
    assert wm.sc_updates == [("BTC/USD", False, PositionSide.LONG)]   # AR-073 loss, per-side


def test_short_win_direction_symmetric_net_pnl():
    wm = _FakeWM(_pos(side=PositionSide.SHORT), fees_entry=Decimal("7.8"))
    rec = _ec([]).on_paper_close(
        "BTC/USD", "54000", ExitReason.MAE_THRESHOLD_BREACH, "7.02", wm
    )
    # short: (60000-54000)*0.05 - 7.8 - 7.02 = 285.18
    assert rec.net_pl_usd == Decimal("285.18")
    assert rec.net_gain_usd == Decimal("285.18")


def test_no_atr_snapshot_yields_no_actual_rr():
    wm = _FakeWM(_pos(atr=None), fees_entry=Decimal("7.8"))
    rec = _ec([]).on_paper_close(
        "BTC/USD", "66000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm
    )
    assert rec.actual_rr is None


def test_elevated_regime_maps_to_elevated_vol():
    wm = _FakeWM(_pos(regime="NON_DIR_ELEVATED"), fees_entry=Decimal("7.8"))
    rec = _ec([]).on_paper_close(
        "BTC/USD", "66000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm
    )
    assert rec.vol_regime == "ELEVATED_VOL"


def test_close_with_no_open_position_is_skipped_not_dropped():
    events = []
    wm = _FakeWM(None, fees_entry=None)
    rec = _ec(events).on_paper_close(
        "BTC/USD", "66000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm
    )
    assert rec is None
    assert any(isinstance(e, PaperCloseSkipped) for e in events)
    assert wm.closed == [] and wm.sc_updates == []  # nothing mutated


def test_missing_fees_entry_treated_as_zero():
    wm = _FakeWM(_pos(), fees_entry=None)
    rec = _ec([]).on_paper_close(
        "BTC/USD", "66000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm
    )
    # gross 300 - 0 entry fee - 8.58 exit fee
    assert rec.net_pl_usd == Decimal("291.42")
    assert rec.fees_entry_usd == Decimal("0")


# --- the entry-side producer fields emitted on the TRADE_CLOSE (sec 7 Image6 fields 8/10/18/19) ---

_SIGNAL_PARAMS = {
    "rsi_14": Decimal("42"), "ema_9": Decimal("60100"), "ema_21": Decimal("60000"),
    "volume_ratio": Decimal("1.3"), "sss_pass": True, "side": "long",
}


def test_producer_fields_emitted_from_the_entry_snapshot():
    # The D6 snapshot was captured on the position at entry; the close copies it onto the record.
    pos = _pos(
        signal_params=_SIGNAL_PARAMS, market_regime="TRENDING_POS_ELEVATED",
        entry_timestamp_utc="2026-06-15T00:00:00+00:00",
    )
    wm = _FakeWM(pos, fees_entry=Decimal("7.8"))
    rec = _ec([]).on_paper_close("BTC/USD", "66000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm)
    assert rec.signal_params == _SIGNAL_PARAMS         # (19) the per-trade SSS levels
    assert rec.market_regime == "TRENDING_POS_ELEVATED"  # (18) the BTC anchor regime at entry
    assert rec.entry_timestamp_utc == "2026-06-15T00:00:00+00:00"  # (8)
    # (10) hold_candle_count = (00:45 exit - 00:00 entry) = 2700s // 300 = 9 committed 5m candles
    assert rec.hold_candle_count == 9


def test_hold_candle_count_none_without_entry_stamp():
    # no entry stamp captured (e.g. a gap-closed / backfilled position) -> no fabricated count
    wm = _FakeWM(_pos(), fees_entry=Decimal("7.8"))
    rec = _ec([]).on_paper_close("BTC/USD", "66000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm)
    assert rec.hold_candle_count is None
    assert rec.entry_timestamp_utc is None
    assert rec.signal_params is None and rec.market_regime is None


def test_hold_candle_count_none_without_exit_clock():
    # no injected clock -> no exit stamp -> hold count is None (the entry stamp alone is not enough)
    pos = _pos(entry_timestamp_utc="2026-06-15T00:00:00+00:00")
    wm = _FakeWM(pos, fees_entry=Decimal("7.8"))
    rec = ExitController(on_event=None).on_paper_close(
        "BTC/USD", "66000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm
    )
    assert rec.hold_candle_count is None
    assert rec.exit_timestamp_utc is None


def test_same_candle_exit_floors_hold_to_zero():
    pos = _pos(entry_timestamp_utc="2026-06-15T00:45:00+00:00")   # == the exit clock instant
    wm = _FakeWM(pos, fees_entry=Decimal("7.8"))
    rec = _ec([]).on_paper_close("BTC/USD", "66000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm)
    assert rec.hold_candle_count == 0


# ------------------------------------------------ TB00757: the max-over-life MAE (MTM) lift - the
# TRADE_CLOSE mae_pct_reached is the WORST over the hold (wm.mae_pct_high_for), not the at-exit reading

def test_trade_close_carries_qty_field_24():
    # D1 (dv1_252): the close emits field (24) qty = the filled position quantity (Form 8949 source).
    wm = _FakeWM(_pos(qty="0.05"), fees_entry=Decimal("7.8"))
    rec = _ec([]).on_paper_close("BTC/USD", "66000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm)
    assert rec.qty == Decimal("0.05")


def test_mae_pct_reached_is_lifted_to_the_max_over_life_high():
    # a benign (favorable) exit reaches 0 adverse AT EXIT, but the trade ran deep against the position
    # over its life (the tracked high) -> mae_pct_reached carries the DEEP max-over-life heat.
    wm = _FakeWM(_pos(), fees_entry=Decimal("7.8"), mae_high=Decimal("0.0417"))
    rec = _ec([]).on_paper_close("BTC/USD", "66000", ExitReason.HTF_REGIME_REVERSAL, "8.58", wm)
    assert rec.mae_pct_reached == Decimal("0.0417")           # the tracked high, not the at-exit 0


def test_mae_pct_reached_keeps_the_deeper_at_exit_when_it_exceeds_the_high():
    # an L2 breach exit: the at-exit adverse excursion is the breach (deeper than any earlier mark) ->
    # max(at_exit, tracked_high) keeps the at-exit value.
    wm = _FakeWM(_pos(), fees_entry=Decimal("7.8"), mae_high=Decimal("0.01"))
    rec = _ec([]).on_paper_close("BTC/USD", "57000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm)
    # at-exit = (60000-57000)/60000 = 0.05 > tracked 0.01
    assert rec.mae_pct_reached == Decimal("3000") / Decimal("60000")


def test_mae_pct_reached_falls_back_to_at_exit_without_a_tracker():
    # a wm stand-in without mae_pct_high_for (None high) -> the at-exit reading stands (back-compat).
    wm = _FakeWM(_pos(), fees_entry=Decimal("7.8"))            # mae_high defaults None
    rec = _ec([]).on_paper_close("BTC/USD", "57000", ExitReason.MAE_THRESHOLD_BREACH, "8.58", wm)
    assert rec.mae_pct_reached == Decimal("3000") / Decimal("60000")
