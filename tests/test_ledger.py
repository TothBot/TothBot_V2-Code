"""Tests: the synthetic capital ledger (ledger.py).

Covers 0500000 dv1_241 sec 12.4 (Synthetic Capital Ledger - Single-Owner) +
sec 12.6 (PAPER_LEDGER_UPDATED) + the FEE_TAKER_PCT fee math + AR-047 Decimal.
Pure state store - no network, no asyncio.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.config.fees import FEE_TAKER_PCT
from tothbot.exchange.ledger import (
    LedgerEventType,
    LedgerSoleWriterViolation,
    LedgerSoleWriterViolationError,
    PaperLedgerUpdated,
    SyntheticCapitalLedger,
)

WRITER = "WS_Manager"
_TAKER = Decimal(str(FEE_TAKER_PCT))


# -- initialization (sec 12.4 INIT) -------------------------------------

def test_init_seeds_balance_and_baseline():
    led = SyntheticCapitalLedger(5000)
    assert led.balance == Decimal("5000")
    assert led.portfolio_baseline == Decimal("5000")  # captured ONCE (HR-WM-011)


def test_init_no_float_enters_the_ledger():
    # AR-047: a float seed is taken as Decimal(str(value)), not Decimal(float).
    led = SyntheticCapitalLedger(5000.0)
    assert led.balance == Decimal("5000.0")
    assert isinstance(led.balance, Decimal)


def test_init_emits_paper_ledger_updated_init_event():
    events: list = []
    led = SyntheticCapitalLedger(5000, on_event=events.append)
    assert len(events) == 1
    e = events[0]
    assert isinstance(e, PaperLedgerUpdated)
    assert e.event_type is LedgerEventType.INIT
    assert e.new_balance == Decimal("5000")
    assert e.delta_usd == Decimal("0")
    assert e.code == "PAPER_LEDGER_UPDATED"
    assert led is not None


# -- entry-fill debit (sec 12.4 ENTRY-FILL DEBIT, taker) ----------------

def test_entry_fill_debit_arithmetic():
    events: list = []
    led = SyntheticCapitalLedger(5000, on_event=events.append)
    upd = led.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER)

    entry_proceeds = Decimal("0.05") * Decimal("60000")        # 3000
    fees_entry = entry_proceeds * _TAKER                        # 7.8
    assert fees_entry == Decimal("7.8")
    assert led.balance == Decimal("5000") - (entry_proceeds + fees_entry)  # 1992.2
    assert led.balance == Decimal("1992.2")
    assert upd.event_type is LedgerEventType.ENTRY_FILL
    assert upd.delta_usd == -(entry_proceeds + fees_entry)
    assert upd.fee_usd == fees_entry
    assert upd.new_balance == led.balance


def test_entry_fill_retains_fees_entry_for_net_pnl_on_close():
    led = SyntheticCapitalLedger(5000)
    led.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER)
    assert led.fees_entry_for("BTC/USD") == Decimal("7.8")  # pos.fees_entry_usd
    assert led.fees_entry_for("ETH/USD") is None


def test_entry_fill_emits_event_with_telemetry():
    events: list = []
    led = SyntheticCapitalLedger(5000, on_event=events.append)
    led.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER)
    e = events[-1]
    assert e.event_type is LedgerEventType.ENTRY_FILL
    assert e.symbol == "BTC/USD"
    assert e.fill_price == Decimal("60000")
    assert e.qty == Decimal("0.05")
    assert e.prior_balance == Decimal("5000")
    assert e.new_balance == Decimal("1992.2")


# -- exit-fill credit (sec 12.4 EXIT-FILL CREDIT, taker) ----------------

def test_exit_fill_credit_arithmetic():
    led = SyntheticCapitalLedger(5000)
    led.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER)   # balance 1992.2
    upd = led.exit_fill_credit("BTC/USD", "0.05", "66000", writer=WRITER, exit_reason="L1A")

    exit_proceeds = Decimal("0.05") * Decimal("66000")               # 3300
    fees_exit = exit_proceeds * _TAKER                               # 8.58
    assert fees_exit == Decimal("8.58")
    assert upd.delta_usd == exit_proceeds - fees_exit                # 3291.42
    assert upd.fee_usd == fees_exit
    assert led.balance == Decimal("1992.2") + (exit_proceeds - fees_exit)
    assert led.balance == Decimal("5283.62")
    assert upd.event_type is LedgerEventType.EXIT_FILL


def test_exit_fill_clears_retained_entry_fee():
    led = SyntheticCapitalLedger(5000)
    led.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER)
    led.exit_fill_credit("BTC/USD", "0.05", "66000", writer=WRITER)
    assert led.fees_entry_for("BTC/USD") is None


def test_exit_fill_event_carries_exit_reason():
    events: list = []
    led = SyntheticCapitalLedger(5000, on_event=events.append)
    led.exit_fill_credit("BTC/USD", "0.05", "66000", writer=WRITER, exit_reason="MAE")
    e = events[-1]
    assert e.event_type is LedgerEventType.EXIT_FILL
    assert e.exit_reason == "MAE"


def test_round_trip_profit_and_loss_shape():
    # A winning round trip nets above start; a losing one nets below (after both
    # taker fee legs) - the shape CIATS consumes (PA-005).
    win = SyntheticCapitalLedger(5000)
    win.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER)
    win.exit_fill_credit("BTC/USD", "0.05", "66000", writer=WRITER)
    assert win.balance > Decimal("5000")

    loss = SyntheticCapitalLedger(5000)
    loss.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER)
    loss.exit_fill_credit("BTC/USD", "0.05", "54000", writer=WRITER)
    assert loss.balance < Decimal("5000")


# -- single-owner guard (rule:HR-WM-032) --------------------------------

def test_entry_debit_rejects_foreign_writer():
    events: list = []
    led = SyntheticCapitalLedger(5000, on_event=events.append)
    with pytest.raises(LedgerSoleWriterViolationError):
        led.entry_fill_debit("BTC/USD", "0.05", "60000", writer="Risk_Engine")
    assert led.balance == Decimal("5000")  # write never happened
    assert any(isinstance(e, LedgerSoleWriterViolation) for e in events)
    assert not any(
        isinstance(e, PaperLedgerUpdated) and e.event_type is LedgerEventType.ENTRY_FILL
        for e in events
    )


def test_exit_credit_rejects_foreign_writer():
    led = SyntheticCapitalLedger(5000)
    with pytest.raises(LedgerSoleWriterViolationError):
        led.exit_fill_credit("BTC/USD", "0.05", "66000", writer="Long_Module")
    assert led.balance == Decimal("5000")
