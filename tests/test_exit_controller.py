"""mod:Exit_Controller close-path tests (0500000 dv1_242 sec 12.5 + sec 7 Image6).

Drives the sec-12.5 close sequence over a fake WSManager surface (the established
inject-the-sole-writer-surfaces pattern): net P&L per D1 FEE-CALC-004, the 23-field
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

    def __init__(self, position, fees_entry):
        self._position = position
        self._fees_entry = fees_entry
        self.closed = []
        self.sc_updates = []
        self.sem_released = 0

    def position(self, symbol):
        return self._position if self._position and self._position.symbol == symbol else None

    def fees_entry_for(self, symbol):
        return self._fees_entry

    def close_position(self, symbol):
        self.closed.append(symbol)
        self._position = None
        return None

    def update_selection_state_on_close(self, symbol, is_win):
        self.sc_updates.append((symbol, is_win))

    def release_exit_semaphore(self):
        self.sem_released += 1


_CLOCK = lambda: datetime(2026, 6, 15, 0, 45, 0, tzinfo=timezone.utc)


def _pos(side=PositionSide.LONG, entry="60000", qty="0.05", atr="2000",
         regime="TRENDING_POS_NORMAL"):
    return Position(
        symbol="BTC/USD",
        side=side,
        qty=Decimal(qty),
        avg_entry_price=Decimal(entry),
        atr_14_entry=Decimal(atr) if atr is not None else None,
        regime_at_entry=regime,
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
    # the 23-field record identity + carried fields
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
    assert wm.sc_updates == [("BTC/USD", True)]  # step 8 AR-073 win
    assert wm.sem_released == 1                  # step 9 semaphore release


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
    assert wm.sc_updates == [("BTC/USD", False)]   # AR-073 loss


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
