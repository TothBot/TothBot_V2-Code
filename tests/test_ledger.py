"""Tests: the synthetic capital ledger (ledger.py).

Covers 0500000 dv1_241 sec 12.4 (Synthetic Capital Ledger - Single-Owner) +
sec 12.6 (PAPER_LEDGER_UPDATED) + the FEE_TAKER_PCT fee math + AR-047 Decimal.
Pure state store - no network, no asyncio.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.config import registry
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
_MARGIN_OPEN = Decimal(str(registry.value("margin_open_fee_pct")))


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


# -- SHORT direction (ar:AR-009 Kraken margin, DEC-A margin fees) -------
# A spot LONG buys-to-open (DEBIT) / sells-to-close (CREDIT). A margin SHORT is the
# mirror: sell-to-open CREDITS proceeds net of taker + margin_open_fee_pct; buy-to-cover
# DEBITS the cover cost + taker + the accrued margin_rollover. Short net P&L on a round
# trip is (entry - exit) * qty - fees (profit when price FALLS).

def test_short_entry_credits_proceeds_net_of_taker_and_margin_open():
    events: list = []
    led = SyntheticCapitalLedger(5000, on_event=events.append)
    upd = led.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER, is_short=True)

    entry_proceeds = Decimal("0.05") * Decimal("60000")              # 3000
    fees_entry = entry_proceeds * _TAKER + entry_proceeds * _MARGIN_OPEN  # 7.8 + 0.6 = 8.4
    assert fees_entry == Decimal("8.4")
    # sell-to-open is a CREDIT: balance RISES by proceeds - fees.
    assert upd.delta_usd == entry_proceeds - fees_entry              # +2991.6
    assert led.balance == Decimal("5000") + (entry_proceeds - fees_entry)
    assert led.balance == Decimal("7991.6")
    assert upd.fee_usd == fees_entry
    # the entry-side cost retained for net P&L on close includes the margin open fee.
    assert led.fees_entry_for("BTC/USD") == Decimal("8.4")


def test_short_exit_debits_cover_cost_plus_taker_and_rollover():
    led = SyntheticCapitalLedger(5000)
    led.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER, is_short=True)  # 7991.6
    upd = led.exit_fill_credit(
        "BTC/USD", "0.05", "54000", writer=WRITER, is_short=True,
        margin_rollover_usd="1.20", exit_reason="L1A",
    )
    exit_proceeds = Decimal("0.05") * Decimal("54000")              # 2700 (cover cost)
    fees_exit = exit_proceeds * _TAKER + Decimal("1.20")           # 7.02 + 1.20 = 8.22
    assert fees_exit == Decimal("8.22")
    # buy-to-cover is a DEBIT: balance FALLS by cover cost + fees.
    assert upd.delta_usd == -(exit_proceeds + fees_exit)           # -2708.22
    assert led.balance == Decimal("7991.6") - (exit_proceeds + fees_exit)
    assert led.balance == Decimal("5283.38")
    assert upd.fee_usd == fees_exit


def test_short_round_trip_profits_when_price_falls():
    # SHORT wins on a DROP (60000 -> 54000) and loses on a RISE (60000 -> 66000),
    # the exact mirror of the long round-trip shape - after both fee legs.
    win = SyntheticCapitalLedger(5000)
    win.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER, is_short=True)
    win.exit_fill_credit("BTC/USD", "0.05", "54000", writer=WRITER, is_short=True)
    assert win.balance > Decimal("5000")

    loss = SyntheticCapitalLedger(5000)
    loss.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER, is_short=True)
    loss.exit_fill_credit("BTC/USD", "0.05", "66000", writer=WRITER, is_short=True)
    assert loss.balance < Decimal("5000")


def test_short_net_pnl_equals_entry_minus_exit_times_qty_minus_fees():
    # net = (entry - exit) * qty - (fees_entry + fees_exit), no margin rollover.
    led = SyntheticCapitalLedger(5000)
    led.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER, is_short=True)
    led.exit_fill_credit("BTC/USD", "0.05", "54000", writer=WRITER, is_short=True)
    gross = (Decimal("60000") - Decimal("54000")) * Decimal("0.05")  # 300
    entry_proceeds = Decimal("0.05") * Decimal("60000")
    exit_proceeds = Decimal("0.05") * Decimal("54000")
    fees = (entry_proceeds * _TAKER + entry_proceeds * _MARGIN_OPEN) + exit_proceeds * _TAKER
    assert led.balance == Decimal("5000") + gross - fees


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


# -- sec-12.5 close: retain the entry fee through the credit, clear after ----

def test_exit_credit_retains_entry_fee_for_close():
    # The sec-12.5 close path applies the credit (step 2) BEFORE on_paper_close (step 5)
    # reads pos.fees_entry_usd for the net P&L - so retain_fees_entry keeps it.
    led = SyntheticCapitalLedger(5000)
    led.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER)
    led.exit_fill_credit("BTC/USD", "0.05", "57000", writer=WRITER, retain_fees_entry=True)
    assert led.fees_entry_for("BTC/USD") == Decimal("7.8")


def test_clear_fees_entry_drops_it_and_is_sole_writer_guarded():
    led = SyntheticCapitalLedger(5000)
    led.entry_fill_debit("BTC/USD", "0.05", "60000", writer=WRITER)
    with pytest.raises(LedgerSoleWriterViolationError):
        led.clear_fees_entry("BTC/USD", writer="Exit_Controller")
    assert led.fees_entry_for("BTC/USD") == Decimal("7.8")  # guarded - not cleared
    led.clear_fees_entry("BTC/USD", writer=WRITER)
    assert led.fees_entry_for("BTC/USD") is None


def test_clear_fees_entry_idempotent_for_unknown_symbol():
    led = SyntheticCapitalLedger(5000)
    led.clear_fees_entry("ETH/USD", writer=WRITER)  # no-op, must not raise
