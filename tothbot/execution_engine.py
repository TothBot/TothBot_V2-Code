"""
TothBot V2 — Execution Engine
=============================================================
Coding spec:  1011004 Execution_Engine_Coding_Spec dv1_0
BP standard:  1011001 Engineering_Best_Practices dv1_6
Parent spec:  0511003 Execution_Engine_Specification dv1_0
=============================================================

Owns all order dispatch to Kraken for position entry and
TP/emergSL placement. No order dispatch code lives anywhere
else.

Phase 1 — Entry dispatch:
  Receives Gate 8 output from Signal Pipeline.
  Assigns cl_ord_id. Constructs and sends add_order
  (limit / post_only / GTD) via Private WS v2.
  Creates Position Mirror record at dispatch.
  Populates Pending Order Registry.

Phase 2 — TP + emergSL placement on fill:
  On exec_type=filled: recomputes TP and emergSL at actual
  avg_price. Constructs and sends batch_add (TP resting
  limit + emergSL stop-market, atomic) via Private WS v2.
  Updates Position Mirror with order IDs on batch_add ACK.

Hard Rules:
  post_only=True on ALL entry add_order.
  stp_type="cancel_newest" (underscore) on ALL WS v2 orders.
  deadline = UTC now + 5 seconds on ALL orders.
  triggers.reference="last" on ALL emergSL stop orders.
  TP and emergSL placed via batch_add atomic on fill.
  Never OTO conditional orders.
  cl_ord_id assigned at dispatch BEFORE Kraken response.
  Position Mirror record created at dispatch (no async gap).
  Pending Order Registry populated on every add_order.
  All Decimal values: Decimal(str()) on WS numeric fields.
  net_gain = net_loss * 1.5 HARDCODED. Not a parameter.
  entry_qty >= qty_min AND entry_qty * price >= cost_min
    checked before dispatch.
  amend_order is primary amendment. edit_order PROHIBITED.
  Never send cancel_all_orders_after.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Any

import orjson

from tothbot.logger import log_record

# =============================================================
# CONSTANTS — CIATS-owned starting values
# =============================================================

# entry_timeout_sec: GTD window. CIATS-owned. Starting value = 45s.
ENTRY_TIMEOUT_SEC: int = 45


# =============================================================
# CL_ORD_ID HELPERS
# =============================================================

def _make_entry_cl_ord_id(symbol: str) -> str:
    """
    EE-ID-001: cl_ord_id for entry order.
    Format: symbol_prefix(6) + timestamp_ms(13).
    Assigned at dispatch BEFORE Kraken response.
    """
    ts_ms = int(time.time() * 1000)
    return f"{symbol[:6].replace('/', '')}{ts_ms}"


def _make_tp_cl_ord_id(symbol: str, counter: int) -> str:
    """TP limit order cl_ord_id. Uses counter for guaranteed uniqueness."""
    return f"T{counter:010d}{symbol[:5].replace('/', '')}"


def _make_sl_cl_ord_id(symbol: str, counter: int) -> str:
    """emergSL stop-market cl_ord_id. Uses counter for guaranteed uniqueness."""
    return f"S{counter:010d}{symbol[:5].replace('/', '')}"


# =============================================================
# TIME HELPERS
# =============================================================

def _gtd_expire_time() -> str:
    """
    EE-ADD-004: GTD expire = UTC now + entry_timeout_sec.
    ISO 8601 format, UTC, no microseconds (Kraken format).
    """
    expire = datetime.now(timezone.utc) + timedelta(seconds=ENTRY_TIMEOUT_SEC)
    return expire.strftime("%Y-%m-%dT%H:%M:%S")


def _deadline_now_plus_5s() -> str:
    """
    EE-ADD-005 / AR-033: deadline = UTC now + 5 seconds on ALL orders.
    ISO 8601 format, UTC.
    """
    dl = datetime.now(timezone.utc) + timedelta(seconds=5)
    return dl.strftime("%Y-%m-%dT%H:%M:%S")


# =============================================================
# EXECUTION ENGINE
# =============================================================

class ExecutionEngine:
    """
    TothBot V2 Execution Engine.

    Phase 1: on_gate8_output() — entry add_order dispatch.
    Phase 2: on_execution_event() — TP + emergSL batch_add on fill.

    Injected dependencies:
        ws_manager:      WSManager — ws_private socket, ws_token,
                         pending_orders dict, req_id_registry dict,
                         pair_specs dict
        risk_engine:     RiskEngine — gate_8(), release_semaphore()
        position_mirror: PositionMirror — create_record(),
                         on_entry_filled(), on_batch_add_ack(),
                         clear_record()
        logger:          logging.Logger ("tothbot" instance)

    Called by WSManager:
        on_gate8_output(output)          — from Signal Pipeline pass
        on_execution_event(event)        — from executions channel push
        on_batch_add_response(msg)       — from batch_add ACK
        on_rate_limit_reject(req_id, sym) — from rate-limited add_order
    """

    def __init__(
        self,
        ws_manager: Any,
        risk_engine: Any,
        logger: Any,
    ) -> None:
        self._wm = ws_manager
        self._re = risk_engine
        self._logger = logger
        # Position Mirror writes go through self._wm.pm_*() methods.
        # HR-PM-009: WS Manager is SOLE WRITER. No _pm object injected.

        # Internal counter: unique IDs for req_id, TP/emergSL cl_ord_ids
        self._counter: int = 0

        # Entry order context: cl_ord_id → dispatch context
        # Needed for post-fill gate_8 recomputation and cleanup.
        #   symbol:            str
        #   entry_qty:         Decimal
        #   entry_limit_price: Decimal
        #   atr_14:            Decimal  (recovered from mae_pct at limit price)
        #   sizing_modifier:   Decimal  (from Gate 6 via pipeline output)
        #   pair_spec:         dict     (price/qty increments and mins)
        self._entry_orders: dict[str, dict] = {}

        # batch_add pending: req_id → context for ACK correlation (EE-BA-006)
        #   symbol:           str
        #   tp_cl_ord_id:     str
        #   emgsl_cl_ord_id:  str
        #   entry_cl_ord_id:  str
        self._batch_pending: dict[int, dict] = {}

    # =============================================================
    # INTERNAL HELPERS
    # =============================================================

    def _next_id(self) -> int:
        """Monotonically increasing counter. Thread-safe within asyncio."""
        self._counter += 1
        return self._counter

    # =============================================================
    # PHASE 1 — ENTRY ORDER DISPATCH
    # =============================================================

    async def on_gate8_output(self, output: dict) -> None:
        """
        Phase 1: dispatch entry add_order from Signal Pipeline Gate 8 pass.
        Called by WSManager when Signal Pipeline returns a full Gate 8 pass.

        EE-ADD-001 through EE-ADD-006, EE-ID-001 through EE-ID-003.

        output dict keys (from signal_pipeline.on_candle):
          symbol, entry_limit_price, sizing_modifier, signal_params,
          entry_qty, tp_price, emergsl_price, net_RR, mae_pct, gross_target
        """
        symbol = output["symbol"]
        entry_limit_price = Decimal(str(output["entry_limit_price"]))
        sizing_modifier   = Decimal(str(output["sizing_modifier"]))
        mae_pct           = Decimal(str(output["mae_pct"]))

        # pair_spec from WSManager pair_specs (pair_cache)
        pair_spec = self._wm.pair_specs.get(symbol, {})
        price_incr = Decimal(str(pair_spec["price_increment"]))
        qty_incr   = Decimal(str(pair_spec["qty_increment"]))
        qty_min    = Decimal(str(pair_spec["qty_min"]))
        cost_min   = Decimal(str(pair_spec["cost_min"]))

        # EE-ADD-002: quantize entry price DOWN to price_increment
        entry_limit_price = entry_limit_price.quantize(
            price_incr, rounding=ROUND_DOWN
        )

        # Use preview entry_qty from pipeline (gate_8 preview)
        entry_qty = Decimal(str(output["entry_qty"]))

        # EE-ADD-003: minimum size validation before dispatch
        if entry_qty < qty_min or entry_qty * entry_limit_price < cost_min:
            self._logger.info(log_record({
                "event":      "GATE_8_SIZE_BELOW_MIN",
                "level":      "INFO",
                "component":  "EXEC_ENG",
                "symbol":     symbol,
                "order_qty":  entry_qty,
                "qty_min":    qty_min,
                "cost_min":   cost_min,
                "fill_price": entry_limit_price,
            }))
            # Release semaphore — entry will not be dispatched
            self._re.release_semaphore()
            return

        # EE-ID-001/002: cl_ord_id assigned BEFORE send
        cl_ord_id = _make_entry_cl_ord_id(symbol)
        req_id    = self._next_id()

        # EE-ADD-001: construct add_order — all fields required
        entry_msg = {
            "method": "add_order",
            "params": {
                "order_type":    "limit",
                "side":          "buy",
                "symbol":        symbol,
                "limit_price":   str(entry_limit_price),   # Decimal → str (HR-EE-008)
                "order_qty":     str(entry_qty),            # Decimal → str
                "post_only":     True,                      # HR-EE-001
                "time_in_force": "gtd",
                "expire_time":   _gtd_expire_time(),        # EE-ADD-004
                "cl_ord_id":     cl_ord_id,
                "stp_type":      "cancel_newest",           # HR-EE-002 — underscore WS v2
                "deadline":      _deadline_now_plus_5s(),   # HR-EE-003
            },
            "req_id": req_id,
        }

        await self._wm.ws_private.send(orjson.dumps(entry_msg).decode())

        # EE-ADD-006: at-dispatch actions BEFORE any Kraken response
        # (a) Create Position Mirror record via WSManager (HR-PM-009, no async gap — EE-ID-002)
        self._wm.pm_create(symbol, cl_ord_id, entry_limit_price, entry_qty)

        # (b) Add to Pending Order Registry (available_USD tracking in Gate 7)
        self._wm.pending_orders[cl_ord_id] = entry_qty * entry_limit_price

        # (c) Register req_id (EE-ID-003)
        self._wm.req_id_registry[req_id] = {
            "method": "add_order",
            "ts":     time.monotonic(),
            "symbol": symbol,
        }

        # Recover atr_14 for post-fill recomputation (EE-BA-002).
        # mae_pct = atr_14 * mae_mult / entry_limit_price  →
        # atr_14 = mae_pct * entry_limit_price / mae_mult
        mae_mult = Decimal(str(self._re._params.get("mae_mult", "1.5")))
        atr_14   = (mae_pct * entry_limit_price / mae_mult).quantize(
            Decimal("0.00000001"), rounding=ROUND_DOWN
        )

        # Store entry context for post-fill processing
        self._entry_orders[cl_ord_id] = {
            "symbol":            symbol,
            "entry_qty":         entry_qty,
            "entry_limit_price": entry_limit_price,
            "atr_14":            atr_14,
            "sizing_modifier":   sizing_modifier,
            "pair_spec":         pair_spec,
        }

        self._logger.info(log_record({
            "event":       "ENTRY_DISPATCHED",
            "level":       "INFO",
            "component":   "EXEC_ENG",
            "symbol":      symbol,
            "cl_ord_id":   cl_ord_id,
            "entry_price": entry_limit_price,
            "qty":         entry_qty,
            "deadline":    entry_msg["params"]["deadline"],
        }))

    # =============================================================
    # EXECUTIONS CHANNEL EVENT ROUTING
    # =============================================================

    async def on_execution_event(self, event: dict) -> None:
        """
        Route executions channel push events for entry orders.
        Non-entry cl_ord_ids (TP, emergSL) belong to Exit Controller.

        EE-REJ-001, EE-REJ-002.
        """
        cl_ord_id = event.get("cl_ord_id", "")

        # Only handle events for orders we dispatched as entries
        if cl_ord_id not in self._entry_orders:
            return

        exec_type = event.get("exec_type", "")

        if exec_type == "filled":
            await self._on_entry_filled(event)
        elif exec_type == "canceled":
            await self._on_entry_canceled(event)
        elif exec_type == "expired":
            await self._on_entry_expired(event)
        else:
            # Pending / trade / other — no action required from EE
            pass

    # =============================================================
    # PHASE 1 — FILL AND REJECTION HANDLERS
    # =============================================================

    async def _on_entry_filled(self, event: dict) -> None:
        """
        Entry filled — Phase 2 trigger.
        Recomputes TP + emergSL at actual avg_price. Dispatches batch_add.
        EE-BA-001 through EE-BA-006.
        """
        cl_ord_id = event.get("cl_ord_id", "")
        ctx = self._entry_orders.pop(cl_ord_id, None)
        if ctx is None:
            return

        symbol = ctx["symbol"]

        # EE-BA-001: actual avg_price from fill event — NOT limit price
        entry_fill_price = Decimal(str(event["avg_price"]))

        # cum_qty from fill (handles partial fills treated as entry — EE-REJ-002)
        entry_qty = Decimal(str(
            event.get("cum_qty", str(ctx["entry_qty"]))
        ))

        # EE-BA-001: update Position Mirror with actual fill data via WSManager (HR-PM-009)
        self._wm.pm_on_fill(
            symbol,
            entry_fill_price,
            entry_qty,
            datetime.now(timezone.utc).isoformat(),
        )

        # Remove entry from Pending Order Registry (no longer pending)
        self._wm.pending_orders.pop(cl_ord_id, None)

        self._logger.info(log_record({
            "event":      "ENTRY_FILLED",
            "level":      "INFO",
            "component":  "EXEC_ENG",
            "symbol":     symbol,
            "cl_ord_id":  cl_ord_id,
            "fill_price": entry_fill_price,
            "qty":        entry_qty,
            "fees":       event.get("fees", "0"),
        }))

        # EE-BA-002: recompute TP and emergSL at ACTUAL fill price (HR-EE-009).
        # Pass sizing_modifier so gate_8 applies it correctly (PL-NEW-005 fix).
        extended_spec = dict(ctx["pair_spec"])
        extended_spec["sizing_modifier"] = ctx["sizing_modifier"]

        sizing = self._re.gate_8(
            symbol           = symbol,
            entry_fill_price = entry_fill_price,
            atr_14           = ctx["atr_14"],
            pair_spec        = extended_spec,
        )

        if sizing is None:
            # Extremely rare: fill price moved enough to violate minimum.
            # Position is open. Log CRITICAL — operator must review.
            self._logger.critical(log_record({
                "event":     "EXEC_ENG_GATE8_POST_FILL_FAIL",
                "level":     "CRITICAL",
                "component": "EXEC_ENG",
                "symbol":    symbol,
                "cl_ord_id": cl_ord_id,
                "note":      "gate_8 None post-fill — no TP/emergSL placed",
            }))
            # Semaphore is NOT released — position is open and tracked
            return

        tp_price      = sizing["tp_price"]
        emergsl_price = sizing["emergsl_price"]

        # EE-BA-003: atomic batch_add — TP limit + emergSL stop-market
        # Never OTO. Never separate add_order calls. (HR-EE-005)
        tp_id   = self._next_id()
        sl_id   = self._next_id()
        req_id  = self._next_id()

        tp_cl_ord_id    = _make_tp_cl_ord_id(symbol, tp_id)
        emgsl_cl_ord_id = _make_sl_cl_ord_id(symbol, sl_id)

        ba_msg = {
            "method": "batch_add",
            "params": {
                "orders": [
                    {
                        # TP — resting limit sell
                        "order_type":    "limit",
                        "side":          "sell",
                        "symbol":        symbol,
                        "limit_price":   str(tp_price),   # Decimal → str
                        "order_qty":     str(entry_qty),  # Decimal → str
                        "post_only":     False,            # TP is a taker/resting limit
                        "time_in_force": "gtc",
                        "cl_ord_id":     tp_cl_ord_id,
                        "stp_type":      "cancel_newest",  # HR-EE-002 underscore
                        "deadline":      _deadline_now_plus_5s(),  # HR-EE-003
                    },
                    {
                        # emergSL — stop-market sell (emergency brake only)
                        "order_type":    "stop-market",
                        "side":          "sell",
                        "symbol":        symbol,
                        "order_qty":     str(entry_qty),       # Decimal → str
                        "trigger_price": str(emergsl_price),   # Decimal → str
                        "triggers": {
                            "price":     str(emergsl_price),
                            "reference": "last",               # HR-EE-004 / AR-046
                        },
                        "time_in_force": "gtc",
                        "cl_ord_id":     emgsl_cl_ord_id,
                        "deadline":      _deadline_now_plus_5s(),  # HR-EE-003
                    },
                ],
                "token": self._wm.ws_token,
            },
            "req_id": req_id,
        }

        await self._wm.ws_private.send(orjson.dumps(ba_msg).decode())

        # Track batch_add for ACK correlation (EE-BA-006)
        self._batch_pending[req_id] = {
            "symbol":          symbol,
            "tp_cl_ord_id":    tp_cl_ord_id,
            "emgsl_cl_ord_id": emgsl_cl_ord_id,
            "entry_cl_ord_id": cl_ord_id,
        }
        self._wm.req_id_registry[req_id] = {
            "method": "batch_add",
            "ts":     time.monotonic(),
            "symbol": symbol,
        }

        self._logger.info(log_record({
            "event":     "TP_PLACED",
            "level":     "INFO",
            "component": "EXEC_ENG",
            "symbol":    symbol,
            "cl_ord_id": tp_cl_ord_id,
            "tp_price":  tp_price,
        }))
        self._logger.info(log_record({
            "event":      "EMERG_SL_PLACED",
            "level":      "INFO",
            "component":  "EXEC_ENG",
            "symbol":     symbol,
            "cl_ord_id":  emgsl_cl_ord_id,
            "sl_trigger": emergsl_price,
        }))

    async def _on_entry_canceled(self, event: dict) -> None:
        """
        Entry canceled — post_only rejection or other cancellation.
        EE-REJ-001: log, clear PM record, release semaphore.
        DO NOT retry. Pipeline re-evaluates at next candle close.
        """
        cl_ord_id = event.get("cl_ord_id", "")
        ctx = self._entry_orders.pop(cl_ord_id, None)
        if ctx is None:
            return

        symbol = ctx["symbol"]
        reason = event.get("reason", "unknown")

        self._logger.info(log_record({
            "event":     "ENTRY_REJECTED_POST_ONLY",
            "level":     "INFO",
            "component": "EXEC_ENG",
            "symbol":    symbol,
            "cl_ord_id": cl_ord_id,
            "reason":    reason,
        }))

        # EE-REJ-001: remove from registry, clear PM via WSManager, release semaphore
        self._wm.pending_orders.pop(cl_ord_id, None)
        self._wm.pm_clear(symbol)
        self._re.release_semaphore()

    async def _on_entry_expired(self, event: dict) -> None:
        """
        GTD expiry — entry_timeout_sec elapsed with no fill or partial fill.
        EE-REJ-002:
          cum_qty == 0 → clean up entirely (no fill).
          cum_qty  > 0 → partial fill — treat as entry fill, place TP+emergSL.
        """
        cl_ord_id = event.get("cl_ord_id", "")

        # Do NOT pop yet — _on_entry_filled will pop if we delegate
        ctx = self._entry_orders.get(cl_ord_id)
        if ctx is None:
            return

        symbol  = ctx["symbol"]
        cum_qty = Decimal(str(event.get("cum_qty", "0")))

        self._logger.info(log_record({
            "event":     "ENTRY_EXPIRED",
            "level":     "INFO",
            "component": "EXEC_ENG",
            "symbol":    symbol,
            "cl_ord_id": cl_ord_id,
            "cum_qty":   cum_qty,
        }))

        if cum_qty == Decimal("0"):
            # No fill — clean up fully (EE-REJ-002)
            self._entry_orders.pop(cl_ord_id, None)
            self._wm.pending_orders.pop(cl_ord_id, None)
            self._wm.pm_clear(symbol)
            self._re.release_semaphore()
        else:
            # Partial fill — treat as entry fill, dispatch TP+emergSL (EE-REJ-002)
            # Synthesise a filled-style event for _on_entry_filled
            fill_event = {
                "cl_ord_id": cl_ord_id,
                "exec_type": "filled",
                "avg_price": event.get("avg_price", str(ctx["entry_limit_price"])),
                "cum_qty":   str(cum_qty),
                "fees":      event.get("fees", "0"),
            }
            await self._on_entry_filled(fill_event)

    # =============================================================
    # PHASE 2 — BATCH_ADD ACK HANDLER
    # =============================================================

    async def on_batch_add_response(self, msg: dict) -> None:
        """
        Handle batch_add ACK with Kraken-assigned order_ids.
        EE-BA-006: update Position Mirror with tp_order_id and emgsl_order_id.
        Log TP_PLACED and EMERG_SL_PLACED already written at send time.
        Remove entry cl_ord_id from Pending Order Registry.
        """
        req_id = msg.get("req_id")
        ctx = self._batch_pending.pop(req_id, None)
        if ctx is None:
            return

        symbol = ctx["symbol"]

        # Extract Kraken-assigned order_ids from response
        # Kraken batch_add response: msg["result"]["orders"] list
        orders = []
        result = msg.get("result", {})
        if isinstance(result, dict):
            orders = result.get("orders", [])
        elif isinstance(result, list):
            orders = result

        if len(orders) < 2:
            self._logger.critical(log_record({
                "event":     "BATCH_ADD_ACK_INCOMPLETE",
                "level":     "CRITICAL",
                "component": "EXEC_ENG",
                "symbol":    symbol,
                "req_id":    req_id,
                "orders":    len(orders),
                "note":      "Expected 2 orders (TP + emergSL) in batch_add ACK",
            }))
            return

        # orders[0] = TP, orders[1] = emergSL (same order as batch_add)
        tp_order_id    = orders[0].get("order_id", "")
        emgsl_order_id = orders[1].get("order_id", "")

        # EE-BA-006: update Position Mirror with Kraken order IDs via WSManager (HR-PM-009)
        self._wm.pm_on_orders(symbol, tp_order_id, emgsl_order_id)

        # Clean up req_id_registry
        self._wm.req_id_registry.pop(req_id, None)

        self._logger.info(log_record({
            "event":          "BATCH_ADD_ACK_RECEIVED",
            "level":          "INFO",
            "component":      "EXEC_ENG",
            "symbol":         symbol,
            "tp_order_id":    tp_order_id,
            "emgsl_order_id": emgsl_order_id,
        }))

    # =============================================================
    # RATE LIMIT REJECTION
    # =============================================================

    async def on_rate_limit_reject(self, req_id: int, symbol: str) -> None:
        """
        Rate limit rejection on add_order dispatch.
        EE log: ENTRY_REJECTED_RATE_LIMIT. (EE-ADD-006 error path)
        Position Mirror and semaphore cleanup handled by caller if needed.
        """
        self._logger.warning(log_record({
            "event":     "ENTRY_REJECTED_RATE_LIMIT",
            "level":     "WARN",
            "component": "EXEC_ENG",
            "symbol":    symbol,
            "req_id":    req_id,
        }))
