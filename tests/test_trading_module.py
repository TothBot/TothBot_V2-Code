"""Tests: mod:Long_Module / mod:Short_Module (modules/trading_module.py).

Covers 0500000 dv1_250 sec 7 + D-05 + ar:AR-009: the per-module wallet (independent Long/Short
balances, $5,000 each seed) + the side-bound order construction (long spot buy / short margin
sell + buy-to-cover emergSL). The two modules share nothing - independent wallets.
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.position_mirror import PositionSide
from tothbot.modules.trading_module import TradingModule

_DEADLINE = "2026-06-15T07:30:00Z"
_WRITER = "WS_Manager"


# -- per-module wallet --------------------------------------------------

def test_long_module_seeds_its_own_wallet():
    m = TradingModule(PositionSide.LONG)
    assert m.is_short is False
    assert m.wallet_balance == Decimal("5000.0")
    assert m.portfolio_baseline == Decimal("5000.0")


def test_short_module_seeds_its_own_wallet():
    m = TradingModule(PositionSide.SHORT)
    assert m.is_short is True
    assert m.wallet_balance == Decimal("5000.0")


def test_custom_starting_balance_override():
    m = TradingModule(PositionSide.SHORT, starting_balance="8000")
    assert m.wallet_balance == Decimal("8000")


def test_two_modules_have_independent_wallets():
    lng = TradingModule(PositionSide.LONG)
    sht = TradingModule(PositionSide.SHORT)
    # A short entry credits the short wallet; the long wallet is untouched (per-wallet isolation).
    sht.ledger.entry_fill_debit("BTC/USD", "0.05", "60000", writer=_WRITER, is_short=True)
    assert sht.wallet_balance != Decimal("5000.0")
    assert lng.wallet_balance == Decimal("5000.0")


# -- side-bound order construction --------------------------------------

def test_long_module_builds_spot_buy_entry():
    m = TradingModule(PositionSide.LONG)
    msg = m.build_entry(
        "BTC/USD", order_qty="0.05", entry_limit_price="60050", cl_ord_id="cl", deadline=_DEADLINE,
    )
    assert msg["params"]["side"] == "buy"
    assert "margin" not in msg["params"]


def test_short_module_builds_margin_sell_entry():
    m = TradingModule(PositionSide.SHORT)
    msg = m.build_entry(
        "BTC/USD", order_qty="0.05", entry_limit_price="59950", cl_ord_id="cl", deadline=_DEADLINE,
    )
    assert msg["params"]["side"] == "sell"
    assert msg["params"]["margin"] is True


def test_long_module_builds_sell_stop_emergsl():
    m = TradingModule(PositionSide.LONG)
    msg = m.build_emergsl(
        "BTC/USD", order_qty="0.05", emergsl_price="57000", cl_ord_id="cl", deadline=_DEADLINE,
    )
    leg = msg["params"]["orders"][0]
    assert leg["side"] == "sell"
    assert "reduce_only" not in leg            # spot long SL has no reduce_only (margin-only flag)


def test_short_module_builds_buy_to_cover_emergsl():
    m = TradingModule(PositionSide.SHORT)
    msg = m.build_emergsl(
        "BTC/USD", order_qty="0.05", emergsl_price="63000", cl_ord_id="cl", deadline=_DEADLINE,
    )
    leg = msg["params"]["orders"][0]
    assert leg["side"] == "buy"               # buy-to-cover (AR-009)
    assert leg["reduce_only"] is True
    assert leg["triggers"]["price"] == "63000"
