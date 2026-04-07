"""
TothBot V2 — Unit Tests: Position Mirror
=============================================================
Test spec:   1021001 Unit_Test_Specification dv1_0 §4.3
Module:      tothbot/position_mirror.py
Coding spec: 1011006 Position_Mirror_Coding_Spec dv1_0
BP standard: 1011001 Engineering_Best_Practices dv1_6
=============================================================

Tests: UT-PM-001 through UT-PM-006

Position Mirror is an O(1) symbol-keyed dict. Written by
WS Manager exclusively. Tests verify all write operations,
dict key semantics, and reconciliation behaviour.

UT-FW-004: Standard asyncio. Do NOT use uvloop.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.position_mirror import PositionMirror
from tothbot.logger import initialize_logger


# =============================================================
# FIXTURES
# =============================================================

@pytest.fixture
def logger():
    _, listener, log = initialize_logger()
    yield log
    listener.stop()


@pytest.fixture
def pm(logger):
    return PositionMirror(logger=logger)


def _sample_record_kwargs() -> dict:
    """Minimal valid PositionRecord kwargs for create operations."""
    return {
        "symbol":      "BTC/USD",
        "cl_ord_id":   "BTCUSD1712345678901",
    }


# =============================================================
# UT-PM-001: Record created at dispatch — dict key = symbol
# HR-PM-001 / PM-WR-001
# =============================================================

class TestPositionMirrorCreate:

    def test_UT_PM_001_record_created_at_dispatch(self, pm):
        """
        UT-PM-001: HR-PM-001 — create_record() adds symbol key
        to internal dict. O(1) lookup. Record exists immediately
        after creation (no async gap).
        """
        pm.create_record(
            symbol="BTC/USD",
            cl_ord_id="BTCUSD1712345678901",
        )
        assert "BTC/USD" in pm.positions, (
            "HR-PM-001: Record must be in positions dict immediately "
            "after create_record()"
        )

    def test_UT_PM_001_one_position_per_symbol(self, pm):
        """
        UT-PM-001: HR-PM-008 — dict enforces one position per symbol.
        Second create overwrites first (or is silently rejected).
        Either way, at most one record per symbol.
        """
        pm.create_record(symbol="BTC/USD", cl_ord_id="ID1")
        pm.create_record(symbol="BTC/USD", cl_ord_id="ID2")
        assert len([k for k in pm.positions if k == "BTC/USD"]) == 1, (
            "HR-PM-008: Only one position record per symbol allowed"
        )

    def test_UT_PM_001_multiple_symbols_independent(self, pm):
        """
        UT-PM-001: Multiple different symbols are independent.
        Each has its own record.
        """
        pm.create_record(symbol="BTC/USD", cl_ord_id="ID1")
        pm.create_record(symbol="ETH/USD", cl_ord_id="ID2")
        assert "BTC/USD" in pm.positions
        assert "ETH/USD" in pm.positions
        assert pm.open_count == 2


# =============================================================
# UT-PM-002: entry_fill_price from fill event, NOT limit price
# HR-PM-002 / PM-WR-002
# =============================================================

class TestFillPriceUpdate:

    def test_UT_PM_002_entry_fill_price_from_avg_price(self, pm):
        """
        UT-PM-002: HR-PM-002 — entry_fill_price must be set from
        exec_type=filled avg_price, NEVER from the limit order price.
        on_entry_filled() must update the record's fill price.
        """
        pm.create_record(symbol="BTC/USD", cl_ord_id="ID1")

        limit_price = Decimal("65000.0")
        fill_price  = Decimal("64987.5")  # actual avg_price (slippage)

        pm.on_entry_filled(
            symbol="BTC/USD",
            avg_price=fill_price,
            qty=Decimal("0.0015"),
            timestamp="2026-04-06T12:00:00+00:00",
        )

        record = pm.positions["BTC/USD"]
        assert record.entry_fill_price == fill_price, (
            f"HR-PM-002: entry_fill_price must be avg_price "
            f"({fill_price}), not limit price ({limit_price}). "
            f"Got {record.entry_fill_price}"
        )

    def test_UT_PM_002_qty_set_on_fill(self, pm):
        """
        UT-PM-002: qty must be set from the fill event.
        """
        pm.create_record(symbol="BTC/USD", cl_ord_id="ID1")
        pm.on_entry_filled(
            symbol="BTC/USD",
            avg_price=Decimal("65000"),
            qty=Decimal("0.0015"),
            timestamp="2026-04-06T12:00:00+00:00",
        )
        record = pm.positions["BTC/USD"]
        assert record.qty == Decimal("0.0015"), (
            f"PM-WR-002: qty must be {Decimal('0.0015')}, "
            f"got {record.qty}"
        )


# =============================================================
# UT-PM-003: qty decrements correctly on TP partial fill
# PM-WR-004 / AR-066
# =============================================================

class TestPartialTPFill:

    def test_UT_PM_003_qty_decrements_on_partial_fill(self, pm):
        """
        UT-PM-003: PM-WR-004 — on TP partial fill (exec_type=trade),
        qty must decrement by the filled amount.
        Position remains open until qty reaches 0.
        """
        pm.create_record(symbol="BTC/USD", cl_ord_id="ID1")
        pm.on_entry_filled(
            symbol="BTC/USD",
            avg_price=Decimal("65000"),
            qty=Decimal("0.003"),
            timestamp="2026-04-06T12:00:00+00:00",
        )

        # TP partial fill: 0.001 filled, 0.002 remaining
        pm.on_tp_partial_fill(
            symbol="BTC/USD",
            filled_qty=Decimal("0.001"),
        )

        record = pm.positions["BTC/USD"]
        expected_qty = Decimal("0.002")
        assert record.qty == expected_qty, (
            f"PM-WR-004: After partial TP fill of 0.001, qty must be "
            f"{expected_qty}, got {record.qty}"
        )
        # Position must still exist
        assert "BTC/USD" in pm.positions, (
            "PM-WR-004: Position must remain open after partial fill"
        )


# =============================================================
# UT-PM-004: Record deleted on close
# PM-WR-005 / HR-PM-003
# =============================================================

class TestPositionClose:

    def test_UT_PM_004_record_deleted_on_close(self, pm):
        """
        UT-PM-004: HR-PM-003 / PM-WR-005 — clear_record() must
        remove the symbol key from the positions dict.
        O(1) delete. Record must not be accessible after close.
        """
        pm.create_record(symbol="BTC/USD", cl_ord_id="ID1")
        pm.on_entry_filled(
            symbol="BTC/USD",
            avg_price=Decimal("65000"),
            qty=Decimal("0.0015"),
            timestamp="2026-04-06T12:00:00+00:00",
        )

        pm.clear_record(symbol="BTC/USD")

        assert "BTC/USD" not in pm.positions, (
            "PM-WR-005: clear_record() must remove symbol from "
            "positions dict (position closed)"
        )
        assert pm.open_count == 0

    def test_UT_PM_004_clear_nonexistent_does_not_raise(self, pm):
        """
        UT-PM-004: Clearing a non-existent record must not raise.
        Defensive coding — close may be called on gap-reconciled positions.
        """
        pm.clear_record(symbol="BTC/USD")  # should not raise


# =============================================================
# UT-PM-005: hold_candle_count increments on every candle event
# PM-WR-003
# =============================================================

class TestCandleCountIncrement:

    def test_UT_PM_005_hold_candle_count_increments(self, pm):
        """
        UT-PM-005: PM-WR-003 — on_candle() must increment
        hold_candle_count for all open positions. Used by Exit
        Controller for time-based exits.
        """
        pm.create_record(symbol="BTC/USD", cl_ord_id="ID1")
        pm.on_entry_filled(
            symbol="BTC/USD",
            avg_price=Decimal("65000"),
            qty=Decimal("0.0015"),
            timestamp="2026-04-06T12:00:00+00:00",
        )

        initial_count = pm.positions["BTC/USD"].hold_candle_count

        pm.on_candle("BTC/USD")
        pm.on_candle("BTC/USD")
        pm.on_candle("BTC/USD")

        final_count = pm.positions["BTC/USD"].hold_candle_count
        assert final_count == initial_count + 3, (
            f"PM-WR-003: hold_candle_count must increment by 1 per candle. "
            f"Expected {initial_count + 3}, got {final_count}"
        )

    def test_UT_PM_005_candle_does_not_affect_other_symbols(self, pm):
        """
        UT-PM-005: on_candle for BTC/USD must not increment
        ETH/USD hold_candle_count.
        """
        pm.create_record(symbol="BTC/USD", cl_ord_id="ID1")
        pm.create_record(symbol="ETH/USD", cl_ord_id="ID2")
        pm.on_entry_filled("BTC/USD", Decimal("65000"),
                           Decimal("0.001"), "2026-04-06T12:00:00+00:00")
        pm.on_entry_filled("ETH/USD", Decimal("3000"),
                           Decimal("0.01"), "2026-04-06T12:00:00+00:00")

        eth_before = pm.positions["ETH/USD"].hold_candle_count
        pm.on_candle("BTC/USD")
        eth_after = pm.positions["ETH/USD"].hold_candle_count

        assert eth_after == eth_before, (
            "PM-WR-003: on_candle('BTC/USD') must not affect ETH/USD count"
        )


# =============================================================
# UT-PM-006: Reconciliation — gap-closed position detected
# PM-REC-001 / AR-003
# =============================================================

class TestReconciliation:

    def test_UT_PM_006_gap_closed_position_detected(self, pm):
        """
        UT-PM-006: PM-REC-001 / AR-003 — reconcile() must detect
        positions in the Mirror that are NOT in snap_orders.
        These are positions closed during a WS gap (TP or emergSL fired).
        Returns list of symbols that were gap-closed.
        """
        pm.create_record(symbol="BTC/USD", cl_ord_id="ID1")
        pm.on_entry_filled(
            symbol="BTC/USD",
            avg_price=Decimal("65000"),
            qty=Decimal("0.0015"),
            timestamp="2026-04-06T12:00:00+00:00",
        )

        # BTC/USD is in Mirror but NOT in snap_orders → gap-closed
        snap_orders = {}  # empty snap — position closed during gap

        gap_closed = pm.reconcile(snap_orders=snap_orders)

        assert "BTC/USD" in gap_closed, (
            "PM-REC-001: BTC/USD in Mirror but not in snap_orders "
            "must be detected as gap-closed"
        )

    def test_UT_PM_006_active_position_not_flagged(self, pm):
        """
        UT-PM-006: Position present in BOTH Mirror and snap_orders
        must NOT be flagged as gap-closed.
        """
        pm.create_record(symbol="BTC/USD", cl_ord_id="ID1")
        pm.on_entry_filled(
            symbol="BTC/USD",
            avg_price=Decimal("65000"),
            qty=Decimal("0.0015"),
            timestamp="2026-04-06T12:00:00+00:00",
        )

        # BTC/USD present in snap_orders — still active
        snap_orders = {"BTCUSD1712345678901": {"symbol": "BTC/USD"}}

        gap_closed = pm.reconcile(snap_orders=snap_orders)

        assert "BTC/USD" not in gap_closed, (
            "PM-REC-001: Active position (in snap_orders) must NOT "
            "be flagged as gap-closed"
        )
