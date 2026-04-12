"""
DocDCN:     1011008
DocTitle:   Long_Module
DocVersion: dv1_0
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/tothbot/long_module.py
DocDate:    04-12-2026
DocTime:    23:59:59 UTC

============================================================
REVISION HISTORY
============================================================

  dv1_0   04-12-2026  DC header added per 0311001 v1_1, 0311004 v1_1,
                      1011001 dv1_7. No code logic changes.

  dv1_0   04-05-2026  Initial Phase 8 implementation.
                      Written to 1011008 Long_Module_Coding_Spec dv1_0.

============================================================

Entry dispatch orchestration and position protection engine.
Owns the entry order state machine per symbol.
Manages BoundedSemaphore lifecycle from dispatch through close.
Handles all entry order outcomes:
  exec_type=new       → ON_BOOK state + ENTRY_ACCEPTED_ON_BOOK
  exec_type=trade     → PARTIAL_FILL state + ENTRY_PARTIAL_TRADE
  exec_type=filled    → FILLED + POSITION_OPENED (EE does batch_add)
  exec_type=expired   → zero fill: EE cleanup | partial: protect or IOC sell
  exec_type=canceled  → partial: protect or IOC sell | clean: EE cleanup

Hard Rules:
  Semaphore acquired at dispatch. Never released until position close,
    post_only rejection, zero-fill expiry, or below-min emergency sell.
  BoundedSemaphore ValueError = CRITICAL BUG. Halt system.
  batch_add dispatched only on exec_type=filled (EE handles).
  Exception: AR-054 partial fill protection on expired/canceled.
  Net 1:1.5 R:R HARDCODED (computed by EE via gate_8). Never a parameter.
  All Decimal arithmetic. No float.
  DMS (cancel_all_orders_after) PROHIBITED. AR-055.
  post_only=True on all entry orders (enforced by EE).
  stp_type=cancel_newest (underscore, WS v2 — enforced by EE).
  triggers.reference=last on emergSL (enforced by EE).
  One entry state machine per symbol. One concurrent entry per symbol.
  Below-minimum partial fill → IOC market sell immediately.
    No batch_add on qty < qty_min OR qty * price < cost_min.

Interfaces:
  Receives: Gate 8 output dict from Signal Pipeline
  Calls:    execution_engine.on_gate8_output()  — entry dispatch
            execution_engine.on_execution_event() — fill/cancel/expire routing
            risk_engine.acquire_semaphore()       — slot management
            risk_engine.release_semaphore()       — on no-fill outcomes
            position_mirror.on_entry_filled()     — PARTIAL_TRADE qty update
            position_mirror.close_position() — on no-fill or below-min
            ws_manager.ws_private.send()          — IOC market sell
            ws_manager.ws_token                   — auth token
            ws_manager.pending_orders             — registry clear
            ws_manager.pair_specs                 — qty_min, cost_min
            ws_manager.req_id_registry            — IOC req_id tracking
            logger                                — all events
============================================================
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import orjson

from tothbot.logger import log_record


# =============================================================
# ENTRY STATE MACHINE STATES  (LM-SM-001)
# =============================================================

class EntryState:
    IDLE         = "IDLE"
    DISPATCHED   = "DISPATCHED"
    ON_BOOK      = "ON_BOOK"
    PARTIAL_FILL = "PARTIAL_FILL"
    FILLED       = "FILLED"
    EXPIRED      = "EXPIRED"
    REJECTED     = "REJECTED"


# =============================================================
# ORDER HELPERS
# =============================================================

def _deadline_now_plus_5s() -> str:
    """AR-033: deadline = UTC now + 5s on ALL orders. ISO 8601 UTC."""
    dl = datetime.now(timezone.utc) + timedelta(seconds=5)
    return dl.strftime("%Y-%m-%dT%H:%M:%S")


def _make_ioc_req_id(counter: int) -> int:
    """Unique req_id for IOC market sell. Uses caller-managed counter."""
    return counter


# =============================================================
# LONG MODULE
# =============================================================

class LongModule:
    """
    TothBot V2 Long Module.

    Phase 1 — Entry Dispatch:
      Acquires BoundedSemaphore. Stores Gate 8 context.
      Delegates entry add_order construction to ExecutionEngine.
      Manages per-symbol entry state machine.

    Phase 2 — Position Protection:
      On exec_type=filled: logs POSITION_OPENED (EE dispatches batch_add).
      On partial fill + expiry/cancel: AR-054 protection or IOC sell.

    Injected dependencies:
      ws_manager:       WSManager  — ws_private, ws_token, pending_orders,
                                     pair_specs, req_id_registry
      execution_engine: ExecutionEngine — on_gate8_output(), on_execution_event()
      risk_engine:      RiskEngine — acquire_semaphore(), release_semaphore()
      position_mirror:  PositionMirror — on_entry_filled(), close_position()
      logger:           logging.Logger ("tothbot" instance)

    Called by WSManager:
      on_gate8_output(output)        — Gate 8 authorized dispatch
      on_execution_event(event)      — executions channel push events
      on_position_closed(symbol)     — called by ExitController on close
                                       (releases semaphore + clears context)
    """

    def __init__(
        self,
        ws_manager: Any,
        execution_engine: Any,
        risk_engine: Any,
        position_mirror: Any,
        logger: Any,
    ) -> None:
        self._wm = ws_manager
        self._ee = execution_engine
        self._re = risk_engine
        self._pm = position_mirror
        self._logger = logger

        # Per-symbol entry state machine (LM-SM-001).
        # symbol → {state, partial_qty, partial_price}
        self._entry_state: dict[str, dict] = {}

        # Gate 8 context stored at dispatch for POSITION_OPENED log.
        # symbol → gate8_output dict (signal_params, asset_regime, market_regime)
        self._gate8_context: dict[str, dict] = {}

        # Internal counter for IOC market sell req_ids
        self._ioc_counter: int = 0

    # =============================================================
    # PHASE 1 — GATE 8 DISPATCH  (LM-DISPATCH-001)
    # =============================================================

    async def on_gate8_output(self, output: dict) -> None:
        """
        Gate 8 authorized dispatch. Entry point for Long Module.
        LM-DISPATCH-001: acquire semaphore, store context, delegate to EE.

        output dict keys (from Signal Pipeline):
          symbol, entry_limit_price, sizing_modifier, signal_params,
          entry_qty, tp_price, emergsl_price, net_RR, mae_pct, gross_target,
          asset_regime, market_regime
        """
        symbol = output.get("symbol", "")

        # LM-SEM-001: acquire BoundedSemaphore slot at dispatch.
        # Gate 7 checked availability; acquire here locks the slot.
        acquired = await self._re.acquire_semaphore()
        if not acquired:
            # Semaphore full — Gate 7 check raced with concurrent dispatch.
            # Log and abort. Pipeline re-evaluates at next candle.
            self._logger.warning(log_record({
                "event":     "LM_SEMAPHORE_ACQUIRE_FAILED",
                "level":     "WARN",
                "component": "LONG_MOD",
                "symbol":    symbol,
                "note":      "max_concurrent reached at dispatch — skipping",
            }))
            return

        # Store Gate 8 context for POSITION_OPENED log on fill (LM-PHASE2-003).
        # Captures signal_params, asset_regime, market_regime before any
        # async operation — no gap.
        self._gate8_context[symbol] = output

        # Set entry state = DISPATCHED (LM-SM-001, LM-DISPATCH-001(f))
        self._entry_state[symbol] = {
            "state":         EntryState.DISPATCHED,
            "partial_qty":   Decimal("0"),
            "partial_price": Decimal("0"),
        }

        self._logger.info(log_record({
            "event":      "LM_DISPATCH_INITIATED",
            "level":      "INFO",
            "component":  "LONG_MOD",
            "symbol":     symbol,
            "entry_price": output.get("entry_limit_price"),
            "entry_qty":   output.get("entry_qty"),
        }))

        # Delegate order construction to ExecutionEngine.
        # EE assigns cl_ord_id, builds add_order, creates Position Mirror record,
        # populates Pending Order Registry, sends WS message.
        # EE calls release_semaphore internally on size-below-min failure.
        await self._ee.on_gate8_output(output)

    # =============================================================
    # EXECUTION CHANNEL EVENT ROUTING  (LM-SM-001, LM-DISPATCH-002/003/004)
    # =============================================================

    async def on_execution_event(self, event: dict) -> None:
        """
        Route executions channel events for entry orders.

        LM is the PRIMARY router. EE.on_execution_event is called for
        outcomes requiring order construction (fill → batch_add, cancel/expire
        → PM clear + semaphore release). LM intercepts exec_type=new and
        exec_type=trade (EE ignores these). LM intercepts below-minimum partial
        fill to prevent EE from attempting invalid batch_add (AR-054).
        """
        cl_ord_id = event.get("cl_ord_id", "")
        exec_type = event.get("exec_type", "")

        # Determine if this event belongs to an entry order managed by EE.
        # EE._entry_orders is the authoritative registry of active entry cl_ord_ids.
        ctx = self._ee._entry_orders.get(cl_ord_id)
        if ctx is None:
            # Not an active entry order — route to EE for exit order handling.
            await self._ee.on_execution_event(event)
            return

        symbol = ctx["symbol"]

        # Route by exec_type
        if exec_type == "new":
            # LM-DISPATCH-002: order accepted on book
            await self._on_entry_new(symbol, event)

        elif exec_type == "trade":
            # LM-DISPATCH-003: partial fill during GTD window, order still open
            await self._on_entry_trade(symbol, event)

        elif exec_type == "filled":
            # LM-DISPATCH-004: full fill — POSITION_OPENED log + Phase 2 via EE
            await self._on_entry_filled(symbol, event)
            # EE dispatches batch_add, updates PM with fill data
            await self._ee.on_execution_event(event)

        elif exec_type == "expired":
            await self._on_entry_expired(symbol, event, ctx)

        elif exec_type == "canceled":
            await self._on_entry_canceled(symbol, event, ctx)

        else:
            # Unexpected exec_type for entry order — log and pass to EE
            self._logger.warning(log_record({
                "event":     "LM_UNEXPECTED_EXEC_TYPE",
                "level":     "WARN",
                "component": "LONG_MOD",
                "symbol":    symbol,
                "cl_ord_id": cl_ord_id,
                "exec_type": exec_type,
            }))
            await self._ee.on_execution_event(event)

    # =============================================================
    # STATE HANDLERS — exec_type=new  (LM-DISPATCH-002)
    # =============================================================

    async def _on_entry_new(self, symbol: str, event: dict) -> None:
        """
        Order accepted on book (exec_type=new).
        Set state = ON_BOOK. Log ENTRY_ACCEPTED_ON_BOOK.
        EE does not handle exec_type=new — LM handles exclusively.
        """
        state_rec = self._entry_state.get(symbol, {})
        state_rec["state"] = EntryState.ON_BOOK
        self._entry_state[symbol] = state_rec

        self._logger.info(log_record({
            "event":      "ENTRY_ACCEPTED_ON_BOOK",
            "level":      "INFO",
            "component":  "LONG_MOD",
            "symbol":     symbol,
            "cl_ord_id":  event.get("cl_ord_id", ""),
            "limit_price": event.get("limit_price", ""),
        }))

    # =============================================================
    # STATE HANDLERS — exec_type=trade  (LM-DISPATCH-003)
    # =============================================================

    async def _on_entry_trade(self, symbol: str, event: dict) -> None:
        """
        Partial fill during GTD window (exec_type=trade). Order still open.
        Set state = PARTIAL_FILL. Update PM qty. Log ENTRY_PARTIAL_TRADE.
        DO NOT dispatch batch_add — order is still active on book.
        EE does not handle exec_type=trade — LM handles exclusively.
        """
        cum_qty   = Decimal(str(event.get("cum_qty", "0")))
        avg_price = Decimal(str(event.get("avg_price", "0")))

        state_rec = self._entry_state.get(symbol, {})
        state_rec["state"]         = EntryState.PARTIAL_FILL
        state_rec["partial_qty"]   = cum_qty
        state_rec["partial_price"] = avg_price
        self._entry_state[symbol]  = state_rec

        # Update Position Mirror qty to reflect partial accumulation
        self._pm.on_entry_filled(
            symbol,
            avg_price,
            cum_qty,
            datetime.now(timezone.utc).isoformat(),
        )

        self._logger.info(log_record({
            "event":     "ENTRY_PARTIAL_TRADE",
            "level":     "INFO",
            "component": "LONG_MOD",
            "symbol":    symbol,
            "cl_ord_id": event.get("cl_ord_id", ""),
            "cum_qty":   cum_qty,
            "avg_price": avg_price,
        }))

    # =============================================================
    # STATE HANDLERS — exec_type=filled  (LM-DISPATCH-004, LM-PHASE2-003)
    # =============================================================

    async def _on_entry_filled(self, symbol: str, event: dict) -> None:
        """
        Full fill confirmed. Log POSITION_OPENED with complete fields.
        EE handles ENTRY_FILLED log, PM update, and batch_add dispatch.

        POSITION_OPENED is logged here because LM holds signal_params,
        asset_regime, market_regime from the Gate 8 context — EE does not.
        LM-PHASE2-003.
        """
        state_rec = self._entry_state.get(symbol, {})
        state_rec["state"] = EntryState.FILLED
        self._entry_state[symbol] = state_rec

        fill_price = Decimal(str(event.get("avg_price", "0")))
        fill_qty   = Decimal(str(event.get("cum_qty", "0")))
        fees       = event.get("fees", "0")

        # Retrieve Gate 8 context for POSITION_OPENED fields
        g8 = self._gate8_context.get(symbol, {})
        signal_params  = g8.get("signal_params", {})
        asset_regime   = g8.get("asset_regime", "UNKNOWN")
        market_regime  = g8.get("market_regime", "UNKNOWN")
        tp_price       = g8.get("tp_price", "")
        emergsl_price  = g8.get("emergsl_price", "")

        self._logger.info(log_record({
            "event":          "POSITION_OPENED",
            "level":          "INFO",
            "component":      "LONG_MOD",
            "symbol":         symbol,
            "fill_price":     fill_price,
            "qty":            fill_qty,
            "tp_price":       tp_price,
            "emergsl_trigger": emergsl_price,
            "asset_regime":   asset_regime,
            "market_regime":  market_regime,
            "signal_params":  signal_params,
            "fees":           fees,
        }))

        # Context is retained until on_position_closed() clears it.
        # Position is open — semaphore slot HELD (LM-SEM-001).

    # =============================================================
    # STATE HANDLERS — exec_type=expired  (LM-OUT-002, LM-OUT-003, AR-054)
    # =============================================================

    async def _on_entry_expired(
        self, symbol: str, event: dict, ctx: dict
    ) -> None:
        """
        GTD expiry. Two sub-cases:
          cum_qty == 0 → no fill → EE handles cleanup (PM clear, semaphore release)
          cum_qty  > 0 → partial fill → AR-054 protection or IOC sell
        LM-OUT-002, LM-OUT-003.
        """
        cum_qty   = Decimal(str(event.get("cum_qty", "0")))
        cl_ord_id = event.get("cl_ord_id", "")

        if cum_qty == Decimal("0"):
            # LM-OUT-002: zero fill — log and delegate cleanup to EE
            state_rec = self._entry_state.get(symbol, {})
            state_rec["state"] = EntryState.EXPIRED
            self._entry_state[symbol] = state_rec

            self._logger.info(log_record({
                "event":     "ENTRY_EXPIRED_NO_FILL",
                "level":     "INFO",
                "component": "LONG_MOD",
                "symbol":    symbol,
                "cl_ord_id": cl_ord_id,
            }))

            # EE handles: PM.pm_clear (via wm), pending_orders.pop, release_semaphore
            await self._ee.on_execution_event(event)
            self._clear_context(symbol)

        else:
            # LM-OUT-003: partial fill — apply AR-054 protection
            avg_price = Decimal(str(event.get("avg_price", "0")))
            partial_qty = cum_qty

            self._logger.info(log_record({
                "event":     "ENTRY_EXPIRED_PARTIAL_FILL",
                "level":     "INFO",
                "component": "LONG_MOD",
                "symbol":    symbol,
                "cl_ord_id": cl_ord_id,
                "cum_qty":   partial_qty,
                "avg_price": avg_price,
            }))

            await self._apply_ar054(
                symbol, partial_qty, avg_price, cl_ord_id, ctx,
                reason="EXPIRED"
            )

    # =============================================================
    # STATE HANDLERS — exec_type=canceled  (LM-OUT-001, LM-OUT-004)
    # =============================================================

    async def _on_entry_canceled(
        self, symbol: str, event: dict, ctx: dict
    ) -> None:
        """
        Cancel during GTD. Two sub-cases:
          cum_qty == 0 → post_only rejection or other clean cancel
          cum_qty  > 0 → partial fill + cancel → AR-054 (LM-OUT-004)
        LM-OUT-001, LM-OUT-004.
        """
        cum_qty   = Decimal(str(event.get("cum_qty", "0")))
        cl_ord_id = event.get("cl_ord_id", "")
        reason    = event.get("reason", "unknown")

        if cum_qty == Decimal("0"):
            # LM-OUT-001: post_only rejection or clean cancel — delegate to EE
            state_rec = self._entry_state.get(symbol, {})
            state_rec["state"] = EntryState.REJECTED
            self._entry_state[symbol] = state_rec

            self._logger.info(log_record({
                "event":     "ENTRY_POST_ONLY_REJECTED",
                "level":     "INFO",
                "component": "LONG_MOD",
                "symbol":    symbol,
                "cl_ord_id": cl_ord_id,
                "reason":    reason,
                "asset_regime": self._gate8_context.get(
                    symbol, {}
                ).get("asset_regime", "UNKNOWN"),
            }))

            # EE handles: PM.pm_clear (via wm), pending_orders.pop, release_semaphore
            await self._ee.on_execution_event(event)
            self._clear_context(symbol)

        else:
            # LM-OUT-004: partial fill + cancel → same as expired+partial (AR-054)
            avg_price = Decimal(str(event.get("avg_price", "0")))
            partial_qty = cum_qty

            self._logger.info(log_record({
                "event":     "ENTRY_CANCELED_PARTIAL_FILL",
                "level":     "INFO",
                "component": "LONG_MOD",
                "symbol":    symbol,
                "cl_ord_id": cl_ord_id,
                "cum_qty":   partial_qty,
                "reason":    reason,
            }))

            await self._apply_ar054(
                symbol, partial_qty, avg_price, cl_ord_id, ctx,
                reason="CANCELED"
            )

    # =============================================================
    # PARTIAL FILL PROTECTION — AR-054
    # =============================================================

    async def _apply_ar054(
        self,
        symbol: str,
        partial_qty: Decimal,
        avg_price: Decimal,
        cl_ord_id: str,
        ctx: dict,
        reason: str,
    ) -> None:
        """
        AR-054 partial fill protection.
        Called on exec_type=expired or canceled when cum_qty > 0.

        Two outcomes:
          (A) partial_qty >= qty_min AND partial_qty * avg_price >= cost_min:
              Protect: log ENTRY_PARTIAL_FILL_PROTECTED, dispatch batch_add via EE.
          (B) Below minimum: IOC market sell immediately. Log PARTIAL_FILL_BELOW_MINIMUM.

        LM-PARTIAL-PROTECT-001 through -004, LM-PARTIAL-BELOW-MIN-001.
        """
        pair_spec = self._wm.pair_specs.get(symbol, {})
        qty_min   = Decimal(str(pair_spec.get("qty_min",  "0")))
        cost_min  = Decimal(str(pair_spec.get("cost_min", "0")))

        if partial_qty >= qty_min and partial_qty * avg_price >= cost_min:
            # CASE A: above minimum — protect partial fill as position
            self._logger.info(log_record({
                "event":     "ENTRY_PARTIAL_FILL_PROTECTED",
                "level":     "INFO",
                "component": "LONG_MOD",
                "symbol":    symbol,
                "cl_ord_id": cl_ord_id,
                "cum_qty":   partial_qty,
                "avg_price": avg_price,
            }))

            # EE.on_execution_event with a synthesized "filled" event triggers
            # Phase 2: TP + emergSL batch_add at actual fill price.
            # Construct synthetic filled event from the expired/canceled event.
            synth_event = {
                "cl_ord_id": cl_ord_id,
                "exec_type": "filled",
                "avg_price": str(avg_price),
                "cum_qty":   str(partial_qty),
                "fees":      "0",
            }

            # Log POSITION_OPENED for the partial-fill position
            g8 = self._gate8_context.get(symbol, {})
            self._logger.info(log_record({
                "event":          "POSITION_OPENED",
                "level":          "INFO",
                "component":      "LONG_MOD",
                "symbol":         symbol,
                "fill_price":     avg_price,
                "qty":            partial_qty,
                "tp_price":       g8.get("tp_price", ""),
                "emergsl_trigger": g8.get("emergsl_price", ""),
                "asset_regime":   g8.get("asset_regime", "UNKNOWN"),
                "market_regime":  g8.get("market_regime", "UNKNOWN"),
                "signal_params":  g8.get("signal_params", {}),
                "partial_fill":   True,
                "trigger_reason": reason,
            }))

            # Delegate to EE — it will compute TP/emergSL at avg_price,
            # dispatch batch_add, update Position Mirror.
            await self._ee.on_execution_event(synth_event)

            # Semaphore HELD — position is open (LM-SEM-001)
            # Context retained until on_position_closed() is called.

        else:
            # CASE B: below minimum — position too small for resting orders.
            # Emergency market sell. (LM-PARTIAL-BELOW-MIN-001)
            await self._below_min_emergency_sell(
                symbol, partial_qty, cl_ord_id, ctx
            )

    # =============================================================
    # BELOW-MINIMUM EMERGENCY SELL  (LM-PARTIAL-BELOW-MIN-001)
    # =============================================================

    async def _below_min_emergency_sell(
        self,
        symbol: str,
        partial_qty: Decimal,
        cl_ord_id: str,
        ctx: dict,
    ) -> None:
        """
        Partial fill below qty_min or cost_min.
        Position too small to protect with resting orders.
        Issue IOC market sell immediately. (LM-PARTIAL-BELOW-MIN-001)

        Clears EE entry state, PM, pending orders, and releases semaphore.
        Does NOT fire CIATS Trade Outcome Bus (insufficient trade data).
        """
        self._logger.warning(log_record({
            "event":     "PARTIAL_FILL_BELOW_MINIMUM",
            "level":     "WARN",
            "component": "LONG_MOD",
            "symbol":    symbol,
            "cl_ord_id": cl_ord_id,
            "cum_qty":   partial_qty,
            "note":      "Emergency IOC sell — position below minimum",
        }))

        # Clear EE._entry_orders so EE does not attempt batch_add on
        # this cl_ord_id if it receives any further events.
        self._ee._entry_orders.pop(cl_ord_id, None)

        # Clear Pending Order Registry
        self._wm.pending_orders.pop(cl_ord_id, None)

        # Clear Position Mirror record (PM-CLOSE-001)
        # Position never fully opened — emergency IOC sell. No CIATS bus.
        self._pm.close_position(symbol, "PARTIAL_FILL_BELOW_MINIMUM")

        # Release semaphore — position will not be opened
        self._re.release_semaphore()

        # Dispatch IOC market sell for the partial qty
        self._ioc_counter += 1
        req_id = self._ioc_counter

        ioc_msg = {
            "method": "add_order",
            "params": {
                "order_type":    "market",
                "side":          "sell",
                "symbol":        symbol,
                "order_qty":     str(partial_qty),
                "time_in_force": "ioc",
                "deadline":      _deadline_now_plus_5s(),
                "token":         self._wm.ws_token,
            },
            "req_id": req_id,
        }

        try:
            await self._wm.ws_private.send(
                orjson.dumps(ioc_msg).decode()
            )

            # Track in req_id_registry (EE-ID-003 pattern)
            self._wm.req_id_registry[req_id] = {
                "method": "ioc_emergency_sell",
                "ts":     time.monotonic(),
                "symbol": symbol,
            }

            self._logger.info(log_record({
                "event":     "IOC_EMERGENCY_SELL_SENT",
                "level":     "INFO",
                "component": "LONG_MOD",
                "symbol":    symbol,
                "qty":       partial_qty,
                "req_id":    req_id,
            }))

        except Exception as exc:
            # BP-ERR-001: no bare except. Log before handling.
            self._logger.critical(log_record({
                "event":     "IOC_EMERGENCY_SELL_FAILED",
                "level":     "CRITICAL",
                "component": "LONG_MOD",
                "symbol":    symbol,
                "qty":       partial_qty,
                "error":     str(exc),
                "note":      "IOC sell failed — position may be unprotected",
            }))

        finally:
            # Always clear LM context regardless of IOC outcome
            self._clear_context(symbol)

    # =============================================================
    # POSITION CLOSE CALLBACK  (LM-SEM-001)
    # =============================================================

    def on_position_closed(self, symbol: str) -> None:
        """
        Called by Exit Controller on every confirmed position close.
        Releases BoundedSemaphore slot and clears all LM context.
        LM-SEM-001: semaphore released ONLY here for normal closes,
        or on: post_only rejection, zero-fill expiry, below-min sell.

        Decision D-014: Selection Controller.on_position_closed() is also
        called by Exit Controller — LM does not chain to SC here.
        """
        self._re.release_semaphore()
        self._clear_context(symbol)

        self._logger.info(log_record({
            "event":     "LM_POSITION_SLOT_RELEASED",
            "level":     "INFO",
            "component": "LONG_MOD",
            "symbol":    symbol,
        }))

    # =============================================================
    # BATCH_ADD ACK PASSTHROUGH
    # =============================================================

    async def on_batch_add_response(self, msg: dict) -> None:
        """
        Passthrough to ExecutionEngine for batch_add ACK handling.
        EE updates Position Mirror with Kraken-assigned order IDs.
        Called by WSManager on batch_add response.
        """
        await self._ee.on_batch_add_response(msg)

    # =============================================================
    # INTERNAL HELPERS
    # =============================================================

    def _clear_context(self, symbol: str) -> None:
        """Clear gate8 context and entry state for a symbol."""
        self._gate8_context.pop(symbol, None)
        self._entry_state.pop(symbol, None)

    # =============================================================
    # STATE ACCESSORS
    # =============================================================

    def get_entry_state(self, symbol: str) -> str:
        """
        Return current entry state for symbol.
        Gate 1 State Machine uses this to enforce one-entry-per-symbol.
        Returns EntryState.IDLE if symbol not in state machine.
        """
        rec = self._entry_state.get(symbol)
        if rec is None:
            return EntryState.IDLE
        return rec.get("state", EntryState.IDLE)

    def active_entry_symbols(self) -> list[str]:
        """Return list of symbols with active (non-IDLE) entry state."""
        return [
            sym for sym, rec in self._entry_state.items()
            if rec.get("state") not in (EntryState.IDLE,)
        ]