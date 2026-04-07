"""
TothBot V2 — Unit Tests: Position Mirror
=============================================================
Test spec:   1021001 Unit_Test_Specification dv1_0 §4.3
Module:      tothbot/position_mirror.py
Coding spec: 1011006 Position_Mirror_Coding_Spec dv1_0
BP standard: 1011001 Engineering_Best_Practices dv1_6
=============================================================

Tests: UT-PM-001 through UT-PM-006

Actual PositionMirror interface (from source):
  Write: create(), on_entry_filled(), on_batch_add_ack(),
         on_tp_partial_fill(symbol, remaining_qty),
         on_candle_close(), close_position(symbol, exit_reason),
         reconcile(snap_orders) -> None
  Read:  get(symbol), has(symbol), open_count, all_records

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


def _create(pm, symbol="BTC/USD", cl_ord_id="ID1"):
    pm.create(
        symbol=symbol,
        entry_limit_price=Decimal("65000"),
        qty=Decimal("0.003"),
        cl_ord_id_entry=cl_ord_id,
        tp_cl_ord_id=f"T_{cl_ord_id}",
        emgsl_cl_ord_id=f"S_{cl_ord_id}",
        entry_atr_14=Decimal("1000"),
        asset_regime="TRENDING_POSITIVE",
        market_regime="TRENDING_POSITIVE",
        signal_params={},
    )


def _fill(pm, symbol="BTC/USD",
          avg_price=Decimal("64987.5"), qty=Decimal("0.003")):
    pm.on_entry_filled(
        symbol=symbol,
        avg_price=avg_price,
        cum_qty=qty,
        timestamp_utc="2026-04-06T12:00:00+00:00",
    )


# =============================================================
# UT-PM-001: Record created at dispatch (dict key = symbol)
# HR-PM-001 / HR-PM-002 / PM-CREATE-001
# =============================================================

class TestPositionMirrorCreate:

    def test_UT_PM_001_record_created_at_dispatch(self, pm):
        """
        UT-PM-001: HR-PM-002 — create() inserts symbol key immediately.
        O(1) lookup via has() and get(). No async gap.
        """
        _create(pm)
        assert pm.has("BTC/USD"), (
            "HR-PM-002: has() must return True immediately after create()"
        )
        assert pm.get("BTC/USD") is not None, (
            "HR-PM-001: get() must return PositionRecord after create()"
        )

    def test_UT_PM_001_one_position_per_symbol(self, pm):
        """
        UT-PM-001: HR-PM-008 — dict enforces one position per symbol.
        Second create() overwrites first. open_count stays 1.
        """
        _create(pm, "BTC/USD", "ID1")
        _create(pm, "BTC/USD", "ID2")
        assert pm.open_count == 1, (
            f"HR-PM-008: open_count must be 1 after duplicate create(). "
            f"Got {pm.open_count}"
        )

    def test_UT_PM_001_multiple_symbols_independent(self, pm):
        """
        UT-PM-001: Multiple different symbols are independent.
        """
        _create(pm, "BTC/USD", "ID1")
        _create(pm, "ETH/USD", "ID2")
        assert pm.has("BTC/USD")
        assert pm.has("ETH/USD")
        assert pm.open_count == 2


# =============================================================
# UT-PM-002: entry_fill_price from avg_price, NOT limit price
# HR-PM-003 / PM-FILL-001
# =============================================================

class TestFillPriceUpdate:

    def test_UT_PM_002_entry_fill_price_from_avg_price(self, pm):
        """
        UT-PM-002: HR-PM-003 — on_entry_filled() sets entry_fill_price
        from avg_price (actual fill), NEVER from entry_limit_price.
        """
        _create(pm)
        fill_price = Decimal("64987.5")
        _fill(pm, avg_price=fill_price)

        rec = pm.get("BTC/USD")
        assert rec.entry_fill_price == fill_price, (
            f"HR-PM-003: entry_fill_price must be avg_price ({fill_price}), "
            f"got {rec.entry_fill_price}"
        )

    def test_UT_PM_002_qty_set_from_cum_qty(self, pm):
        """
        UT-PM-002: qty set from cum_qty argument, not from create() qty.
        """
        _create(pm)
        _fill(pm, qty=Decimal("0.003"))
        rec = pm.get("BTC/USD")
        assert rec.qty == Decimal("0.003"), (
            f"PM-FILL-001: qty must equal cum_qty. Got {rec.qty}"
        )

    def test_UT_PM_002_fill_price_zero_before_fill(self, pm):
        """
        UT-PM-002: HR-PM-003 — entry_fill_price must be 0 before fill.
        Confirms limit price is NOT stored at dispatch time.
        """
        _create(pm)
        rec = pm.get("BTC/USD")
        assert rec.entry_fill_price == Decimal("0"), (
            "HR-PM-003: entry_fill_price must be 0 before fill event. "
            "Limit price must never be used."
        )


# =============================================================
# UT-PM-003: qty decrements on TP partial fill
# PM-PARTIAL-001
# =============================================================

class TestPartialTPFill:

    def test_UT_PM_003_remaining_qty_set_correctly(self, pm):
        """
        UT-PM-003: PM-PARTIAL-001 — on_tp_partial_fill(symbol, remaining_qty)
        sets qty to the remaining amount. Position stays open.
        """
        _create(pm)
        _fill(pm, qty=Decimal("0.003"))

        pm.on_tp_partial_fill(symbol="BTC/USD", remaining_qty=Decimal("0.002"))

        rec = pm.get("BTC/USD")
        assert rec is not None, "Position must remain open after partial fill"
        assert rec.qty == Decimal("0.002"), (
            f"PM-PARTIAL-001: qty must be remaining_qty=0.002, got {rec.qty}"
        )

    def test_UT_PM_003_position_still_open_after_partial(self, pm):
        """
        UT-PM-003: Position must not be closed by partial fill.
        """
        _create(pm)
        _fill(pm, qty=Decimal("0.003"))
        pm.on_tp_partial_fill(symbol="BTC/USD", remaining_qty=Decimal("0.001"))
        assert pm.has("BTC/USD"), (
            "PM-PARTIAL-001: Position must remain open after partial fill"
        )


# =============================================================
# UT-PM-004: Record deleted on close
# PM-CLOSE-001
# =============================================================

class TestPositionClose:

    def test_UT_PM_004_record_deleted_on_close(self, pm):
        """
        UT-PM-004: PM-CLOSE-001 — close_position() removes symbol from dict.
        O(1) delete. has() returns False after close.
        """
        _create(pm)
        _fill(pm)
        pm.close_position(symbol="BTC/USD", exit_reason="TP_FILL")

        assert not pm.has("BTC/USD"), (
            "PM-CLOSE-001: has() must return False after close_position()"
        )
        assert pm.get("BTC/USD") is None
        assert pm.open_count == 0

    def test_UT_PM_004_close_nonexistent_does_not_raise(self, pm):
        """
        UT-PM-004: Closing non-existent symbol must not raise.
        Defensive — gap-reconciled positions may already be removed.
        """
        pm.close_position(symbol="BTC/USD", exit_reason="TP_FILL")


# =============================================================
# UT-PM-005: hold_candle_count increments on every candle close
# PM-CANDLE-001
# =============================================================

class TestCandleCountIncrement:

    def test_UT_PM_005_count_increments_per_candle(self, pm):
        """
        UT-PM-005: PM-CANDLE-001 — on_candle_close() increments
        hold_candle_count by 1 per call.
        """
        _create(pm)
        _fill(pm)
        initial = pm.get("BTC/USD").hold_candle_count

        pm.on_candle_close("BTC/USD")
        pm.on_candle_close("BTC/USD")
        pm.on_candle_close("BTC/USD")

        final = pm.get("BTC/USD").hold_candle_count
        assert final == initial + 3, (
            f"PM-CANDLE-001: Expected {initial+3}, got {final}"
        )

    def test_UT_PM_005_candle_symbol_isolated(self, pm):
        """
        UT-PM-005: on_candle_close for BTC/USD must not affect ETH/USD.
        """
        _create(pm, "BTC/USD", "ID1")
        _create(pm, "ETH/USD", "ID2")
        _fill(pm, "BTC/USD")
        _fill(pm, "ETH/USD", Decimal("3000"), Decimal("0.01"))

        eth_before = pm.get("ETH/USD").hold_candle_count
        pm.on_candle_close("BTC/USD")
        eth_after = pm.get("ETH/USD").hold_candle_count

        assert eth_after == eth_before, (
            "PM-CANDLE-001: on_candle_close('BTC/USD') must not affect ETH/USD"
        )


# =============================================================
# UT-PM-006: Reconciliation — gap-closed position detection
# PM-RECON-001 / PM-RECON-002 / AR-003
# =============================================================

class TestReconciliation:

    def _setup_filled_with_orders(
        self, pm, symbol="BTC/USD",
        tp_order_id="TP_ORD_001", sl_order_id="SL_ORD_001"
    ):
        """Create fully confirmed position: filled + orders set."""
        _create(pm, symbol)
        _fill(pm, symbol)
        pm.on_batch_add_ack(
            symbol=symbol,
            tp_order_id=tp_order_id,
            emgsl_order_id=sl_order_id,
        )

    def test_UT_PM_006_gap_closed_position_removed(self, pm):
        """
        UT-PM-006: PM-RECON-002 / AR-003 — when position is filled and
        both tp_order_id and emgsl_order_id are absent from snap_orders,
        the position closed during the WS gap. reconcile() removes it.
        """
        self._setup_filled_with_orders(pm, "BTC/USD", "TP_001", "SL_001")
        assert pm.has("BTC/USD")

        pm.reconcile(snap_orders={})  # both orders gone

        assert not pm.has("BTC/USD"), (
            "PM-RECON-002: Gap-closed position must be removed by reconcile()"
        )

    def test_UT_PM_006_active_position_preserved(self, pm):
        """
        UT-PM-006: Position with orders present in snap_orders must
        NOT be removed. Still live.
        """
        self._setup_filled_with_orders(pm, "BTC/USD", "TP_001", "SL_001")

        snap_orders = {
            "TP_001": {"symbol": "BTC/USD"},
            "SL_001": {"symbol": "BTC/USD"},
        }
        pm.reconcile(snap_orders=snap_orders)

        assert pm.has("BTC/USD"), (
            "PM-RECON-001: Active position (orders in snap_orders) preserved"
        )

    def test_UT_PM_006_unfilled_position_not_gap_closed(self, pm):
        """
        UT-PM-006: PM-RECON-002 — position with entry_fill_price=0
        (pending fill, no orders set yet) must NOT be gap-closed.
        """
        _create(pm)  # not filled — entry_fill_price = 0

        pm.reconcile(snap_orders={})

        assert pm.has("BTC/USD"), (
            "PM-RECON-002: Unfilled position (entry_fill_price=0) must "
            "not be removed by reconcile()"
        )
