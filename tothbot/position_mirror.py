"""
TothBot V2 — Position Mirror Component
=============================================================
Coding spec:  1011006 Position_Mirror_Coding_Spec dv1_0
BP standard:  1011001 Engineering_Best_Practices dv1_6
Parent spec:  0511005 Position_Mirror_Specification dv1_0
=============================================================

Single source of truth for all open position state at runtime.
O(1) symbol-keyed dict. WS Manager is the SOLE writer.
Read by Signal Pipeline, Exit Controller, Execution Engine,
Risk Engine, and CIATS.

Hard Rules:
  HR-PM-001: O(1) dict. Never list or ordered structure.
  HR-PM-002: Record created at entry dispatch (before fill ACK).
  HR-PM-003: entry_fill_price = actual avg_price from fill event. NEVER limit price.
  HR-PM-004: All Decimal values: Decimal(str()) immediately on receipt.
  HR-PM-005: CIATS Trade Outcome Bus fires ONCE on full position close.
  HR-PM-006: On reconnect: baseline and drawdown_pct preserved.
  HR-PM-007: Unexpected snap_orders: alert + log. No auto-trade.
  HR-PM-008: One position per symbol. Dict enforces this.
  HR-PM-009: WS Manager is SOLE writer. No other component writes.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from tothbot.logger import _alert_operator_direct, log_record


# =============================================================
# DATA STRUCTURE — Section 4
# =============================================================

@dataclass
class PositionRecord:
    """
    Complete state for one open position.
    One record per symbol maximum (HR-PM-008).
    Keyed by symbol in position_mirror dict (HR-PM-001).
    """
    symbol:              str
    entry_limit_price:   Decimal       # used until fill confirmed
    entry_fill_price:    Decimal       # set on exec_type=filled (HR-PM-003)
    qty:                 Decimal       # current qty — decrements on partial TP
    cl_ord_id_entry:     str           # TothBot-assigned entry cl_ord_id
    tp_order_id:         str           # Kraken-assigned TP order_id (set on batch_add ACK)
    tp_cl_ord_id:        str           # TothBot-assigned TP cl_ord_id
    emgsl_order_id:      str           # Kraken-assigned emergSL order_id
    emgsl_cl_ord_id:     str           # TothBot-assigned emergSL cl_ord_id
    entry_timestamp_utc: str           # ISO 8601 from fill event
    entry_atr_14:        Decimal       # ATR(14) at entry — used in exit calcs
    asset_regime:        str           # regime_cache[symbol] at entry
    market_regime:       str           # regime_cache["BTC/USD"] at entry
    signal_params:       dict = field(default_factory=dict)   # SSS values at entry
    hold_candle_count:   int = 0       # incremented on each 5m candle close


# =============================================================
# POSITION MIRROR
# =============================================================

class PositionMirror:
    """
    In-memory O(1) symbol-keyed position store (HR-PM-001).

    WS Manager is the SOLE writer (HR-PM-009).
    All other components read only via the public interface.

    Usage:
        pm = PositionMirror(logger)

        # Write (WS Manager only)
        pm.create(symbol, ...)
        pm.on_entry_filled(symbol, ...)
        pm.on_batch_add_ack(symbol, ...)
        pm.on_tp_partial_fill(symbol, ...)
        pm.on_candle_close(symbol)
        pm.close_position(symbol, exit_reason)
        pm.reconcile(snap_orders)

        # Read (any component)
        rec = pm.get(symbol)           # None if not open
        is_open = pm.has(symbol)       # bool
        count = pm.open_count          # int
        records = pm.all_records       # dict[str, PositionRecord]
    """

    def __init__(self, logger: Any) -> None:
        self._logger = logger
        # HR-PM-001: O(1) symbol-keyed dict — NEVER list or ordered structure
        self._mirror: dict[str, PositionRecord] = {}

    # =============================================================
    # READ INTERFACE — all components (Section 7)
    # =============================================================

    def get(self, symbol: str) -> PositionRecord | None:
        """O(1) lookup. Returns None if symbol not open (PM-READ-001/003)."""
        return self._mirror.get(symbol)

    def has(self, symbol: str) -> bool:
        """Gate 5 SC-GATE-3 check: is position open for this symbol?"""
        return symbol in self._mirror

    @property
    def open_count(self) -> int:
        """Number of open positions (Gate 7 max_concurrent check)."""
        return len(self._mirror)

    @property
    def all_records(self) -> dict[str, PositionRecord]:
        """
        Read-only view of all open positions.
        Used by Risk Engine drawdown MTM (PM-READ-002).
        Callers MUST NOT write to the returned dict or records.
        """
        return self._mirror

    # =============================================================
    # WRITE OPERATIONS — WS Manager only (HR-PM-009)
    # =============================================================

    def create(
        self,
        symbol: str,
        entry_limit_price: Decimal,
        qty: Decimal,
        cl_ord_id_entry: str,
        tp_cl_ord_id: str,
        emgsl_cl_ord_id: str,
        entry_atr_14: Decimal,
        asset_regime: str,
        market_regime: str,
        signal_params: dict,
    ) -> None:
        """
        Create position record at entry dispatch — BEFORE Kraken fill ACK.
        HR-PM-002: no async gap between dispatch and record creation.
        HR-PM-008: one position per symbol — overwrites if duplicate (should not happen).
        PM-CREATE-001.

        Fields pending fill: entry_fill_price=0, tp_order_id="",
        emgsl_order_id="", entry_timestamp_utc="".
        """
        # HR-PM-004: Decimal(str()) on all numeric values
        self._mirror[symbol] = PositionRecord(
            symbol=symbol,
            entry_limit_price=Decimal(str(entry_limit_price)),
            entry_fill_price=Decimal("0"),   # pending — set on exec_type=filled
            qty=Decimal(str(qty)),
            cl_ord_id_entry=cl_ord_id_entry,
            tp_order_id="",                  # pending — set on batch_add ACK
            tp_cl_ord_id=tp_cl_ord_id,
            emgsl_order_id="",               # pending — set on batch_add ACK
            emgsl_cl_ord_id=emgsl_cl_ord_id,
            entry_timestamp_utc="",          # pending — set on exec_type=filled
            entry_atr_14=Decimal(str(entry_atr_14)),
            asset_regime=asset_regime,
            market_regime=market_regime,
            signal_params=signal_params,
            hold_candle_count=0,
        )
        self._logger.info(log_record({
            "event":     "POSITION_RECORD_CREATED",
            "level":     "INFO",
            "component": "POS_MIRROR",
            "symbol":    symbol,
            "cl_ord_id": cl_ord_id_entry,
            "qty":       Decimal(str(qty)),
        }))

    def on_entry_filled(
        self,
        symbol: str,
        avg_price: Decimal,
        cum_qty: Decimal,
        timestamp_utc: str = "",
    ) -> None:
        """
        Update entry_fill_price, qty, and timestamp on exec_type=filled.
        HR-PM-003: entry_fill_price = actual avg_price. NEVER limit price.
        PM-FILL-001.
        """
        if symbol not in self._mirror:
            self._logger.critical(log_record({
                "event":     "FILL_WITHOUT_RECORD",
                "level":     "CRITICAL",
                "component": "POS_MIRROR",
                "symbol":    symbol,
            }))
            return

        rec = self._mirror[symbol]
        rec.entry_fill_price = Decimal(str(avg_price))   # HR-PM-003/004
        rec.qty = Decimal(str(cum_qty))                   # HR-PM-004
        rec.entry_timestamp_utc = (
            timestamp_utc or datetime.now(timezone.utc).isoformat()
        )

        self._logger.info(log_record({
            "event":     "POSITION_FILL_CONFIRMED",
            "level":     "INFO",
            "component": "POS_MIRROR",
            "symbol":    symbol,
            "avg_price": rec.entry_fill_price,
            "qty":       rec.qty,
        }))

    def on_batch_add_ack(
        self,
        symbol: str,
        tp_order_id: str,
        emgsl_order_id: str,
    ) -> None:
        """
        Populate Kraken-assigned TP and emergSL order_ids from batch_add ACK.
        Called by WS Manager when batch_add response received.
        PM-ORDERS-001.
        """
        if symbol not in self._mirror:
            return

        rec = self._mirror[symbol]
        rec.tp_order_id = tp_order_id
        rec.emgsl_order_id = emgsl_order_id

        self._logger.info(log_record({
            "event":          "POSITION_ORDERS_SET",
            "level":          "INFO",
            "component":      "POS_MIRROR",
            "symbol":         symbol,
            "tp_order_id":    tp_order_id,
            "emgsl_order_id": emgsl_order_id,
        }))

    def on_tp_partial_fill(self, symbol: str, remaining_qty: Decimal) -> None:
        """
        Decrement qty on TP partial fill (exec_type=trade for TP order).
        PM-PARTIAL-001.
        """
        if symbol not in self._mirror:
            return

        self._mirror[symbol].qty = Decimal(str(remaining_qty))   # HR-PM-004

        self._logger.info(log_record({
            "event":     "POSITION_QTY_UPDATED",
            "level":     "INFO",
            "component": "POS_MIRROR",
            "symbol":    symbol,
            "new_qty":   Decimal(str(remaining_qty)),
        }))

    def on_candle_close(self, symbol: str) -> None:
        """
        Increment hold_candle_count on each 5m OHLC close.
        Called by WS Manager for every symbol with an open position.
        PM-CANDLE-001.
        """
        if symbol in self._mirror:
            self._mirror[symbol].hold_candle_count += 1

    def close_position(self, symbol: str, exit_reason: str) -> None:
        """
        Delete position record after confirmed close.
        NEVER delete before close is confirmed (PM-CLOSE-001).
        HR-PM-005: CIATS Trade Outcome Bus fires ONCE — caller's responsibility.
        """
        if symbol not in self._mirror:
            self._logger.warning(log_record({
                "event":       "CLOSE_WITHOUT_RECORD",
                "level":       "HIGH",
                "component":   "POS_MIRROR",
                "symbol":      symbol,
                "exit_reason": exit_reason,
            }))
            return

        del self._mirror[symbol]

        self._logger.info(log_record({
            "event":       "POSITION_CLOSED",
            "level":       "INFO",
            "component":   "POS_MIRROR",
            "symbol":      symbol,
            "exit_reason": exit_reason,
        }))

    # =============================================================
    # RECONCILIATION — Section 6
    # =============================================================

    def reconcile(self, snap_orders: dict) -> None:
        """
        Reconcile Position Mirror against Kraken snap_orders.
        Called at startup (Step 6 of 1011014) and on every reconnect.
        PM-RECON-001 through PM-RECON-005.

        snap_orders: dict from REST GetOpenOrders.
        Keys = Kraken order_ids. Values = order detail dicts.

        Algorithm:
          (A) Symbol in mirror, TP+emergSL NOT in snap_orders
              → gap-closed. Log HIGH. Delete.
          (B) Open order in snap_orders NOT in mirror
              → unexpected. Log WARN. Alert. No auto-trade.
        """
        # Build sets of known order_ids from snap_orders
        snap_order_ids: set[str] = set(snap_orders.keys())

        # Build set of known cl_ord_ids from snap_orders (userref field)
        snap_cl_ord_ids: set[str] = {
            v.get("userref", "") for v in snap_orders.values()
        }

        gap_closed_count = 0
        positions_confirmed = 0

        # (A) Gap-closed detection: PM-RECON-002
        gap_closed_symbols = []
        for symbol, rec in list(self._mirror.items()):
            tp_alive = rec.tp_order_id in snap_order_ids
            sl_alive = rec.emgsl_order_id in snap_order_ids
            # If both orders gone and entry not pending (fill confirmed)
            if rec.entry_fill_price > Decimal("0") and not tp_alive and not sl_alive:
                gap_closed_symbols.append(symbol)
            else:
                positions_confirmed += 1

        for symbol in gap_closed_symbols:
            self._logger.warning(log_record({
                "event":        "GAP_CLOSED_POSITION",
                "level":        "HIGH",
                "component":    "POS_MIRROR",
                "symbol":       symbol,
                "estimated_PL": "unknown",
            }))
            del self._mirror[symbol]
            gap_closed_count += 1

        # (B) Unexpected open orders: PM-RECON-003
        # Build all known order_ids from current mirror
        known_order_ids: set[str] = set()
        for rec in self._mirror.values():
            if rec.tp_order_id:
                known_order_ids.add(rec.tp_order_id)
            if rec.emgsl_order_id:
                known_order_ids.add(rec.emgsl_order_id)
            # Entry order may still be pending
            if rec.cl_ord_id_entry:
                known_order_ids.add(rec.cl_ord_id_entry)

        for order_id, order_data in snap_orders.items():
            if order_id not in known_order_ids:
                self._logger.warning(log_record({
                    "event":    "UNEXPECTED_OPEN_ORDER",
                    "level":    "HIGH",
                    "component": "POS_MIRROR",
                    "symbol":   order_data.get("symbol", ""),
                    "order_id": order_id,
                }))
                _alert_operator_direct(
                    f"UNEXPECTED open order found in snap_orders: "
                    f"order_id={order_id}. "
                    f"Bill must review. TothBot will NOT auto-manage."
                )

        # PM-RECON-005: Log reconciliation summary
        self._logger.info(log_record({
            "event":                  "RECONCILIATION_COMPLETE",
            "level":                  "INFO",
            "component":              "POS_MIRROR",
            "positions_confirmed":    positions_confirmed,
            "gap_closed_positions":   gap_closed_count,
        }))
