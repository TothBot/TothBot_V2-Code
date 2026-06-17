"""Tests: ar:AR-052 / REST-BAL-008 the periodic SHORT-equity TradeBalance reconcile carry-forward
(pipeline/equity_reconcile.py).

The reconcile fetches the true Kraken margin equity `e`, computes the RAW SHORT reconstruction, and
re-seeds wm.set_short_equity_reconcile_offset = e - raw so current_portfolio_usd re-anchors the SHORT
drawdown numerator to `e` at the poll instant. Covers: the offset math + the re-anchor end-to-end, the
event emission, the three clean SKIPs (fetch None, wallet not fed, missing quote), and the run/stop loop.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal

from tothbot.exchange.position_mirror import PositionSide
from tothbot.pipeline.equity_reconcile import (
    PeriodicTradeBalanceReconcile,
    ShortEquityReconciled,
)
from tothbot.pipeline.portfolio import current_portfolio_usd
from tothbot.pipeline.sweep import ProviderNotReady

_T0 = datetime(2026, 6, 16, 12, 0, 0, tzinfo=timezone.utc)


# --------------------------------------------------------------------------- fakes
class _Pos:
    def __init__(self, symbol, side, qty, price, *, entry_timestamp_utc=None) -> None:
        self.symbol = symbol
        self.side = side
        self.qty = Decimal(qty)
        self.avg_entry_price = Decimal(price)
        self.entry_timestamp_utc = entry_timestamp_utc


class _WM:
    def __init__(self, *, positions=None, short_wallet="5000") -> None:
        self._positions = positions or {}
        self._short_wallet = None if short_wallet is None else Decimal(short_wallet)
        self.offset = Decimal("0")

    def open_positions(self):
        return self._positions

    def wallet_balance(self, side):
        return self._short_wallet if side is PositionSide.SHORT else None

    def short_equity_reconcile_offset(self):
        return self.offset

    def set_short_equity_reconcile_offset(self, offset):
        self.offset = offset if isinstance(offset, Decimal) else Decimal(str(offset))


def _fetch(value):
    async def fetch():
        return value
    return fetch


def _run(coro):
    return asyncio.run(coro)


# --------------------------------------------------------------------------- the offset math + re-anchor
def test_reconcile_sets_offset_e_minus_raw_and_emits():
    # bleeding short: entry 60000, ask 65000, 0.1 qty -> raw MTM equity 5000 - 500 = 4500. The true
    # TradeBalance e = 4510 (e.g. the estimated rollover over-subtracted by 10) -> offset = 4510 - 4500 = 10.
    wm = _WM(positions={"BTC/USD": _Pos("BTC/USD", PositionSide.SHORT, "0.1", "60000")})
    events = []
    rec = PeriodicTradeBalanceReconcile(
        wm, fetch_trade_balance=_fetch(Decimal("4510")),
        bbo=lambda s: (Decimal("64990"), Decimal("65000")),
        now_utc=lambda: _T0, on_event=events.append,
    )
    event = _run(rec.reconcile_once())
    assert wm.offset == Decimal("10")
    assert isinstance(event, ShortEquityReconciled)
    assert event.equity == Decimal("4510")
    assert event.reconstructed == Decimal("4500")
    assert event.offset == Decimal("10")
    assert events == [event]


def test_reconcile_re_anchors_current_portfolio_to_true_equity():
    # end-to-end: after the reconcile, current_portfolio_usd (the G7 CHECK-1 read) lands EXACTLY on `e`.
    wm = _WM(positions={"BTC/USD": _Pos("BTC/USD", PositionSide.SHORT, "0.1", "60000")})
    rec = PeriodicTradeBalanceReconcile(
        wm, fetch_trade_balance=_fetch(Decimal("4510")),
        bbo=lambda s: (Decimal("64990"), Decimal("65000")), now_utc=lambda: _T0,
    )
    _run(rec.reconcile_once())
    # the default G7 path reads the wm offset (no override) -> raw 4500 + offset 10 == the true e 4510.
    got = current_portfolio_usd(
        wm, PositionSide.SHORT, bbo=lambda s: (Decimal("64990"), Decimal("65000")), now_utc=lambda: _T0
    )
    assert got == Decimal("4510")


def test_negative_offset_when_reconstruction_overstated():
    # the symmetric case: raw 5000 (flat), true e 4980 (a real borrow charge the estimate missed) ->
    # offset -20, so the numerator is pulled DOWN to the true equity (the breaker sees the real bleed).
    wm = _WM()
    rec = PeriodicTradeBalanceReconcile(
        wm, fetch_trade_balance=_fetch(Decimal("4980")), bbo=lambda s: (Decimal("1"), Decimal("2")),
        now_utc=lambda: _T0,
    )
    event = _run(rec.reconcile_once())
    assert wm.offset == Decimal("-20")
    assert event.offset == Decimal("-20")


# --------------------------------------------------------------------------- the clean SKIPs
def test_fetch_none_skips_no_offset_no_event():
    wm = _WM()
    events = []
    rec = PeriodicTradeBalanceReconcile(
        wm, fetch_trade_balance=_fetch(None), bbo=lambda s: (Decimal("1"), Decimal("2")),
        now_utc=lambda: _T0, on_event=events.append,
    )
    assert _run(rec.reconcile_once()) is None
    assert wm.offset == Decimal("0")  # the prior offset stands
    assert events == []


def test_wallet_not_fed_skips():
    wm = _WM(short_wallet=None)  # the SHORT wallet not yet fed (live pre-snapshot)
    rec = PeriodicTradeBalanceReconcile(
        wm, fetch_trade_balance=_fetch(Decimal("4510")), bbo=lambda s: (Decimal("1"), Decimal("2")),
        now_utc=lambda: _T0,
    )
    assert _run(rec.reconcile_once()) is None
    assert wm.offset == Decimal("0")


def test_missing_quote_skips_provider_not_ready():
    def _raise(_s):
        raise ProviderNotReady("BTC/USD", "bbo")

    wm = _WM(positions={"BTC/USD": _Pos("BTC/USD", PositionSide.SHORT, "0.1", "60000")})
    rec = PeriodicTradeBalanceReconcile(
        wm, fetch_trade_balance=_fetch(Decimal("4510")), bbo=_raise, now_utc=lambda: _T0,
    )
    assert _run(rec.reconcile_once()) is None
    assert wm.offset == Decimal("0")  # never set a bogus offset on a missing quote


# --------------------------------------------------------------------------- the run/stop loop
def test_run_polls_then_stops():
    wm = _WM()
    polls = []

    async def fetch():
        polls.append(1)
        return Decimal("5000")

    sleeps = []

    async def fake_sleep(_secs):
        # iter 1: sleep, poll runs; iter 2: stop on the second sleep so the post-sleep guard breaks
        # BEFORE a second poll -> exactly one poll total.
        sleeps.append(1)
        if len(sleeps) >= 2:
            rec.stop()

    rec = PeriodicTradeBalanceReconcile(
        wm, fetch_trade_balance=fetch, bbo=lambda s: (Decimal("1"), Decimal("2")),
        interval_seconds=0.0, sleep=fake_sleep, now_utc=lambda: _T0,
    )
    _run(rec.run())
    assert len(polls) == 1  # one poll ran after the first sleep; the second sleep's stop() broke the loop


def test_stop_before_run_does_no_poll():
    wm = _WM()
    polls = []

    async def fetch():
        polls.append(1)
        return Decimal("5000")

    async def fake_sleep(_secs):
        return None

    rec = PeriodicTradeBalanceReconcile(
        wm, fetch_trade_balance=fetch, bbo=lambda s: (Decimal("1"), Decimal("2")),
        interval_seconds=0.0, sleep=fake_sleep, now_utc=lambda: _T0,
    )
    rec.stop()
    _run(rec.run())
    assert polls == []  # _stopped is already True -> the while guard never enters the loop body
