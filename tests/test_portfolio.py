"""Tests: ar:AR-052 current_portfolio - the per-side MARK-TO-MARKET drawdown numerator (pipeline/
portfolio.py; gate:G7 CHECK 1 source).

Covers the resolved TB00766 Long/Short asymmetry: LONG current_portfolio = spot cash + sum(bid * qty)
(the full market value at the realizable bid, ar:AR-048); SHORT = margin collateral + sum((entry - ask)
* qty) (the open-short UNREALIZED P&L at the realizable ask) - the borrow rollover accrued (the 0.02%/4h
EXISTING CIATS seed). The not-ready wallet (None) skip, the gains-not-counted nature (raw, max(0,...) is
applied downstream in G7), and the rollover hold math. Pure save the injected now_utc clock. Decimal-only.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tothbot.exchange.position_mirror import PositionSide
from tothbot.pipeline.portfolio import (
    current_portfolio_usd,
    held_4h_blocks,
    short_rollover_accrued_usd,
)


# --------------------------------------------------------------------------- fakes
class _Pos:
    def __init__(self, symbol, side, qty, price, *, entry_timestamp_utc=None) -> None:
        self.symbol = symbol
        self.side = side
        self.qty = Decimal(qty)
        self.avg_entry_price = Decimal(price)
        self.entry_timestamp_utc = entry_timestamp_utc


class _WM:
    def __init__(self, *, positions=None, wallets=None, short_offset=None) -> None:
        self._positions = positions or {}
        self._wallets = (
            {PositionSide.LONG: Decimal("5000"), PositionSide.SHORT: Decimal("5000")}
            if wallets is None else wallets
        )
        self._short_offset = short_offset

    def open_positions(self):
        return self._positions

    def wallet_balance(self, side):
        return self._wallets.get(side)

    def short_equity_reconcile_offset(self):
        return Decimal("0") if self._short_offset is None else Decimal(self._short_offset)


def _fixed_clock(dt):
    return lambda: dt


_T0 = datetime(2026, 6, 16, 12, 0, 0, tzinfo=timezone.utc)


# --------------------------------------------------------------------------- not-ready / flat
def test_none_wallet_returns_none_not_ready():
    wm = _WM(wallets={})  # the wallet not yet fed (live pre-snapshot)
    assert current_portfolio_usd(wm, PositionSide.LONG, bbo=lambda s: (1, 1)) is None


def test_flat_long_current_equals_cash():
    wm = _WM()  # no positions
    got = current_portfolio_usd(wm, PositionSide.LONG, bbo=lambda s: (Decimal("1"), Decimal("2")))
    assert got == Decimal("5000")


def test_flat_short_current_equals_collateral():
    # flat-cold-start sanity: with no open shorts the reconstructed equity == the margin collateral cash.
    wm = _WM()
    got = current_portfolio_usd(wm, PositionSide.SHORT, bbo=lambda s: (Decimal("1"), Decimal("2")))
    assert got == Decimal("5000")


# --------------------------------------------------------------------------- LONG mark (bid * qty)
def test_long_marks_full_market_value_at_bid():
    # cash 5000 + 0.1 BTC * bid 60000 = 5000 + 6000 = 11000 (the long spot cash was debited at entry,
    # so adding back the current market value at the bid gives total equity).
    wm = _WM(positions={"BTC/USD": _Pos("BTC/USD", PositionSide.LONG, "0.1", "55000")})
    got = current_portfolio_usd(
        wm, PositionSide.LONG, bbo=lambda s: (Decimal("60000"), Decimal("60010"))
    )
    assert got == Decimal("11000")


def test_long_ignores_short_positions():
    wm = _WM(positions={
        "BTC/USD": _Pos("BTC/USD", PositionSide.LONG, "0.1", "55000"),
        "ETH/USD": _Pos("ETH/USD", PositionSide.SHORT, "1", "3000"),
    })
    got = current_portfolio_usd(
        wm, PositionSide.LONG, bbo=lambda s: (Decimal("60000"), Decimal("60010"))
    )
    assert got == Decimal("5000") + Decimal("0.1") * Decimal("60000")  # short isolated out


# --------------------------------------------------------------------------- SHORT mark ((entry-ask)*qty)
def test_short_winning_adds_unrealized_profit():
    # short 0.1 BTC entered at 60000, ask now 55000 -> profit (60000-55000)*0.1 = 500; equity 5000 + 500.
    wm = _WM(positions={"BTC/USD": _Pos("BTC/USD", PositionSide.SHORT, "0.1", "60000")})
    got = current_portfolio_usd(
        wm, PositionSide.SHORT, bbo=lambda s: (Decimal("54990"), Decimal("55000"))
    )
    assert got == Decimal("5500")


def test_short_bleeding_subtracts_unrealized_loss_the_fn_signal():
    # the FALSE-NEGATIVE case: an open short bled (price ROSE 60000 -> 65000), unrealized loss
    # (60000-65000)*0.1 = -500 -> equity 5000 - 500 = 4500. The margin CASH (5000) is BLIND to this;
    # only the equity basis sees the bleed (this is what makes G7 CHECK 1 fire on it).
    wm = _WM(positions={"BTC/USD": _Pos("BTC/USD", PositionSide.SHORT, "0.1", "60000")})
    got = current_portfolio_usd(
        wm, PositionSide.SHORT, bbo=lambda s: (Decimal("64990"), Decimal("65000"))
    )
    assert got == Decimal("4500")


# --------------------------------------------------------------------------- SHORT rollover accrual
def test_short_subtracts_borrow_rollover_over_the_hold():
    # short 0.1 BTC @ 60000 (notional 6000), price unchanged (ask 60000 -> 0 MTM), held 9h = 2 completed
    # 4h blocks -> rollover 6000 * 0.0002 * 2 = 2.40 subtracted -> equity 5000 - 2.40.
    entry = (_T0 - timedelta(hours=9)).isoformat()
    wm = _WM(positions={"BTC/USD": _Pos("BTC/USD", PositionSide.SHORT, "0.1", "60000",
                                        entry_timestamp_utc=entry)})
    got = current_portfolio_usd(
        wm, PositionSide.SHORT, bbo=lambda s: (Decimal("59990"), Decimal("60000")),
        now_utc=_fixed_clock(_T0),
    )
    assert got == Decimal("5000") - Decimal("6000") * Decimal("0.0002") * Decimal("2")


def test_long_pays_no_borrow_even_with_a_stamp():
    # a spot long never accrues margin borrow (the rollover path is short-only).
    entry = (_T0 - timedelta(hours=100)).isoformat()
    wm = _WM(positions={"BTC/USD": _Pos("BTC/USD", PositionSide.LONG, "0.1", "55000",
                                        entry_timestamp_utc=entry)})
    got = current_portfolio_usd(
        wm, PositionSide.LONG, bbo=lambda s: (Decimal("60000"), Decimal("60010")),
        now_utc=_fixed_clock(_T0),
    )
    assert got == Decimal("11000")  # no borrow subtracted


# ------------------------------------------------------------ SHORT reconcile carry-forward offset
def test_short_adds_reconcile_offset_from_wm():
    # the periodic TradeBalance reconcile re-anchored the SHORT equity: a +12.50 carry-forward offset
    # (REST-BAL-008 `e` exceeded the raw reconstruction, e.g. the estimated rollover over-subtracted) is
    # added back to the numerator. Flat short: 5000 collateral + 12.50 = 5012.50.
    wm = _WM(short_offset="12.50")
    got = current_portfolio_usd(wm, PositionSide.SHORT, bbo=lambda s: (Decimal("1"), Decimal("2")))
    assert got == Decimal("5012.50")


def test_short_reconcile_offset_param_overrides_wm():
    # the reconcile's OWN raw read passes reconcile_offset=0 to read the un-offset reconstruction even
    # when the wm carries a stale offset (so offset = e - raw is computed against the raw, not raw+offset).
    wm = _WM(short_offset="999")
    got = current_portfolio_usd(
        wm, PositionSide.SHORT, bbo=lambda s: (Decimal("1"), Decimal("2")),
        reconcile_offset=Decimal("0"),
    )
    assert got == Decimal("5000")  # the wm offset is ignored; the explicit 0 wins


def test_long_never_adds_the_short_offset():
    # the carry-forward is SHORT-only; a long numerator never reads it even if the wm carries one.
    wm = _WM(short_offset="999")
    got = current_portfolio_usd(wm, PositionSide.LONG, bbo=lambda s: (Decimal("60000"), Decimal("60010")))
    assert got == Decimal("5000")  # flat long, no offset


def test_short_offset_back_compat_wm_without_accessor():
    # a lightweight wm lacking the accessor (getattr None) leaves the reconstruction un-offset (0).
    class _Bare:
        def open_positions(self):
            return {}

        def wallet_balance(self, side):
            return Decimal("5000")

    got = current_portfolio_usd(_Bare(), PositionSide.SHORT, bbo=lambda s: (Decimal("1"), Decimal("2")))
    assert got == Decimal("5000")


# --------------------------------------------------------------------------- pure helpers
def test_held_4h_blocks_floor_and_guards():
    assert held_4h_blocks(None, _T0) == 0                                   # no stamp
    assert held_4h_blocks("not-a-date", _T0) == 0                           # unparseable
    assert held_4h_blocks((_T0 - timedelta(hours=3)).isoformat(), _T0) == 0  # < 4h -> 0 blocks
    assert held_4h_blocks((_T0 - timedelta(hours=4)).isoformat(), _T0) == 1
    assert held_4h_blocks((_T0 - timedelta(hours=11)).isoformat(), _T0) == 2  # floor(11/4)
    assert held_4h_blocks((_T0 + timedelta(hours=1)).isoformat(), _T0) == 0   # future -> 0


def test_held_4h_blocks_naive_stamp_assumed_utc():
    naive = (_T0 - timedelta(hours=8)).replace(tzinfo=None).isoformat()
    assert held_4h_blocks(naive, _T0) == 2


def test_rollover_accrued_decimal_only():
    got = short_rollover_accrued_usd(6000.0, 3, rollover_fee_pct=0.0002)
    assert got == Decimal("6000") * Decimal("0.0002") * Decimal("3")
    assert isinstance(got, Decimal)
