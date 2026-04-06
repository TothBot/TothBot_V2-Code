"""
TothBot V2 — Risk Engine Component
=============================================================
Coding spec:  1011011 Risk_Engine_Coding_Spec dv1_3
BP standard:  1011001 Engineering_Best_Practices dv1_6
Parent specs: 0611001 Position_Sizing_Rules dv1_0
              0611002 Drawdown_and_Halt_Rules dv1_0
              0611003 Concentration_and_Exposure_Rules dv1_0
=============================================================

Implements Gate 7 (Risk Guard) and Gate 8 (Position Sizer).
Final filter before order dispatch.

The ONLY hardcoded constraint: net 1:1.5 R:R (AR-011).
All other values are CIATS-owned starting values.

Hard Rules:
  Net 1:1.5 R:R SACRED. Hardcoded. Never changes.
  portfolio_USD = spot_usd_balance only. Never MTM.
  All financial values: Python Decimal. Never float.
  All rounding: Decimal.quantize() only. Never math.floor/ceil.
  max_concurrent enforced by asyncio.BoundedSemaphore.
  BoundedSemaphore ValueError = critical bug, halt immediately.
  Drawdown uses bid price (realizable exit for longs).
  Drawdown baseline = startup cash. Never reset mid-session.
  Full halt requires operator restart to resume.
  Gate 7 uses available_USD (cash minus pending orders).
  emergSL remains active in all halt states.
  batch_cancel on full halt cancels ENTRY GTD only.
"""
from __future__ import annotations

import asyncio
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from enum import Enum
from typing import Any

from tothbot.logger import _alert_operator_direct, log_record

# =============================================================
# CONSTANTS — CIATS-owned starting values
# =============================================================

TRADEABLE_PCT: Decimal = Decimal("0.50")
PER_TRADE_PCT: Decimal = Decimal("0.05")
MAX_CONCURRENT: int = 20
MAE_MULT: Decimal = Decimal("1.5")
EMERGSL_MULT: Decimal = Decimal("3.0")
SESSION_PAUSE_DRAWDOWN: Decimal = Decimal("0.05")
FULL_HALT_DRAWDOWN: Decimal = Decimal("0.10")

# Fee rates — AR-012 (fixed — not CIATS-owned)
ENTRY_FEE_PCT: Decimal = Decimal("0.0016")   # Maker
TP_FEE_PCT: Decimal = Decimal("0.0016")      # Maker
SL_FEE_PCT: Decimal = Decimal("0.0026")      # Taker (emergSL stop-market)

# Sacred R:R — AR-011, HARDCODED, NEVER changes
SACRED_RR: Decimal = Decimal("1.5")


# =============================================================
# GATE RESULT ENUM
# =============================================================

class GateResult(str, Enum):
    PASS          = "PASS"
    BLOCK         = "BLOCK"
    SESSION_PAUSE = "SESSION_PAUSE"
    FULL_HALT     = "FULL_HALT"


# =============================================================
# SYSTEM STATE ENUM
# =============================================================

class SystemState(str, Enum):
    NORMAL        = "NORMAL"
    SESSION_PAUSED = "SESSION_PAUSED"
    FULL_HALT     = "FULL_HALT"


# =============================================================
# RISK ENGINE
# =============================================================

class RiskEngine:
    """
    TothBot V2 Risk Engine.
    Gate 7 (Risk Guard) + Gate 8 (Position Sizer).

    Injected dependencies:
        logger:              logging.Logger ("tothbot" instance)
        position_mirror:     PositionMirror — read-only for open count
        ws_manager:          WSManager — spot_usd_balance, pending_orders,
                             latest_bid, batch_cancel callable
        param_store:         dict — frozen CIATS Parameter Store snapshot

    Lifecycle:
        1. Instantiate at startup.
        2. Call set_portfolio_baseline(balance) once at startup Step 6.
        3. Call gate_7() and gate_8() on each pipeline evaluation.
        4. Call on_bbo_ticker() on every ticker bbo event.
        5. Call release_semaphore() on entry expiry / rejection.
    """

    def __init__(
        self,
        logger: Any,
        position_mirror: Any,
        ws_manager: Any,
        param_store: dict | None = None,
    ) -> None:
        self._logger = logger
        self._pm = position_mirror
        self._wm = ws_manager
        self._params: dict = param_store or {}

        # Concurrent position limit — RE-SEM-001
        max_conc = int(self._params.get("max_concurrent", MAX_CONCURRENT))
        self._position_semaphore = asyncio.BoundedSemaphore(max_conc)
        self._max_concurrent = max_conc

        # Portfolio tracking
        self._portfolio_baseline_usd: Decimal | None = None  # set ONCE
        self._peak_portfolio_usd: Decimal = Decimal("0")

        # Drawdown state
        self._drawdown_pct: Decimal = Decimal("0")
        self._system_state: SystemState = SystemState.NORMAL

        # Closed trade counter (for Half-Kelly activation threshold)
        self._closed_trade_count: int = 0

        # CIATS Half-Kelly inputs (updated by CIATS at 200 trades)
        self._win_rate: Decimal | None = None
        self._avg_rr: Decimal | None = None

    # =============================================================
    # STARTUP
    # =============================================================

    def set_portfolio_baseline(self, spot_usd_balance: Decimal) -> None:
        """
        Set portfolio_baseline_USD ONCE at startup Step 6.
        NEVER call again during operation or on reconnect.
        RE-DD-002, RE-SZ-001.
        """
        if self._portfolio_baseline_usd is not None:
            # Guard: baseline may only be set once
            self._logger.critical(log_record({
                "event":     "BASELINE_RESET_ATTEMPT",
                "level":     "CRITICAL",
                "component": "RISK_ENG",
                "note":      "portfolio_baseline_USD already set — ignoring",
            }))
            return

        self._portfolio_baseline_usd = Decimal(str(spot_usd_balance))
        self._peak_portfolio_usd = self._portfolio_baseline_usd

        self._logger.info(log_record({
            "event":                 "PORTFOLIO_BASELINE_SET",
            "level":                 "INFO",
            "component":             "RISK_ENG",
            "portfolio_baseline_usd": self._portfolio_baseline_usd,
        }))

    def update_param_store(self, param_store: dict) -> None:
        """Refresh Parameter Store snapshot (called at pipeline start — AR-I-4)."""
        self._params = param_store
        # Refresh max_concurrent from store (may have been updated by CIATS)
        new_max = int(self._params.get("max_concurrent", self._max_concurrent))
        if new_max != self._max_concurrent:
            self._max_concurrent = new_max
            self._position_semaphore = asyncio.BoundedSemaphore(new_max)

    # =============================================================
    # DRAWDOWN MONITORING — RE-DD-001 through RE-DD-008
    # =============================================================

    async def on_bbo_ticker(
        self,
        symbol: str,
        bid: Decimal,
    ) -> None:
        """
        Triggered on every ticker bbo event for pairs with open positions.
        Continuous drawdown monitoring — NOT limited to candle close (RE-DD-001).
        """
        await self._update_drawdown(symbol, bid)

    async def _update_drawdown(self, symbol: str, bid: Decimal) -> None:
        """
        Compute mark-to-market drawdown and enforce circuit breakers.
        RE-DD-002: drawdown_pct formula (authoritative).
        """
        if self._portfolio_baseline_usd is None:
            return  # Baseline not set yet (startup not complete)

        # MTM: cash + unrealized value of all open positions
        spot_balance = Decimal(str(self._wm.spot_usd_balance))

        # Update latest_bid in WS Manager for this symbol
        open_value = Decimal("0")
        for sym, rec in self._pm.all_records.items():
            bid_price = self._wm.latest_bid.get(sym, Decimal("0"))
            open_value += bid_price * rec.qty

        current_portfolio = spot_balance + open_value

        # RE-DD-002: drawdown formula
        self._drawdown_pct = max(
            Decimal("0"),
            (self._portfolio_baseline_usd - current_portfolio)
            / self._portfolio_baseline_usd,
        )

        # RE-DD-003: peak portfolio tracking (for CIATS only)
        if current_portfolio > self._peak_portfolio_usd:
            self._peak_portfolio_usd = current_portfolio

        self._logger.debug(log_record({
            "event":           "DRAWDOWN_UPDATE",
            "level":           "DEBUG",
            "component":       "RISK_ENG",
            "drawdown_pct":    self._drawdown_pct,
            "portfolio_usd":   current_portfolio,
            "portfolio_basis": self._portfolio_baseline_usd,
        }))

        await self._check_circuit_breakers(self._drawdown_pct)

    async def _check_circuit_breakers(self, drawdown_pct: Decimal) -> None:
        """
        Enforce SESSION_PAUSE and FULL_HALT thresholds.
        RE-DD-005/006/007/008.
        """
        full_halt_thresh = Decimal(str(
            self._params.get("full_halt_drawdown", FULL_HALT_DRAWDOWN)
        ))
        pause_thresh = Decimal(str(
            self._params.get("session_pause_drawdown", SESSION_PAUSE_DRAWDOWN)
        ))

        # FULL_HALT — RE-DD-005
        if (drawdown_pct >= full_halt_thresh and
                self._system_state != SystemState.FULL_HALT):
            self._system_state = SystemState.FULL_HALT
            self._logger.critical(log_record({
                "event":         "FULL_HALT_TRIGGERED",
                "level":         "CRITICAL",
                "component":     "RISK_ENG",
                "drawdown_pct":  drawdown_pct,
                "open_count":    self._pm.open_count,
                "threshold":     full_halt_thresh,
            }))
            _alert_operator_direct(
                f"FULL HALT: drawdown {float(drawdown_pct):.2%} >= "
                f"{float(full_halt_thresh):.2%}. All entries blocked. "
                f"emergSL remains active. Operator restart required."
            )
            # RE-DD-007: cancel ENTRY GTD only — emergSL NOT cancelled
            await self._wm.batch_cancel()
            return

        # SESSION_PAUSE — RE-DD-006
        if (drawdown_pct >= pause_thresh and
                self._system_state == SystemState.NORMAL):
            self._system_state = SystemState.SESSION_PAUSED
            self._logger.warning(log_record({
                "event":        "SESSION_PAUSE_TRIGGERED",
                "level":        "HIGH",
                "component":    "RISK_ENG",
                "drawdown_pct": drawdown_pct,
                "threshold":    pause_thresh,
            }))
            _alert_operator_direct(
                f"SESSION PAUSE: drawdown {float(drawdown_pct):.2%} >= "
                f"{float(pause_thresh):.2%}. New entries blocked. "
                f"Existing positions continue."
            )
            return

        # Recovery from SESSION_PAUSE (automatic when drawdown recovers)
        if (drawdown_pct < pause_thresh and
                self._system_state == SystemState.SESSION_PAUSED):
            self._system_state = SystemState.NORMAL
            self._logger.info(log_record({
                "event":        "SESSION_PAUSE_RECOVERED",
                "level":        "INFO",
                "component":    "RISK_ENG",
                "drawdown_pct": drawdown_pct,
            }))

        # FULL_HALT does NOT auto-recover — RE-DD-008
        # Requires operator restart (systemd restart → full startup)

    # =============================================================
    # GATE 7 — RISK GUARD — RE-G7-001
    # =============================================================

    async def gate_7(
        self,
        symbol: str,
        entry_price: Decimal,
    ) -> GateResult:
        """
        Gate 7 Risk Guard. Executes checks in order. Returns immediately
        on any BLOCK/HALT. RE-G7-001.

        Decision order:
          1. System state (FULL_HALT | SESSION_PAUSED)
          2. Max concurrent (BoundedSemaphore)
          3. Drawdown snapshot
          4. Available USD

        Returns: GateResult enum (PASS | BLOCK | SESSION_PAUSE | FULL_HALT)
        """
        # Check 1: System state — RE-SEM-006
        if self._system_state == SystemState.FULL_HALT:
            self._logger.info(log_record({
                "event":     "GATE_7_RESULT",
                "level":     "INFO",
                "component": "RISK_ENG",
                "symbol":    symbol,
                "result":    GateResult.FULL_HALT,
                "reason":    "SYSTEM_FULL_HALT",
            }))
            return GateResult.FULL_HALT

        if self._system_state == SystemState.SESSION_PAUSED:
            self._logger.info(log_record({
                "event":     "GATE_7_RESULT",
                "level":     "INFO",
                "component": "RISK_ENG",
                "symbol":    symbol,
                "result":    GateResult.SESSION_PAUSE,
                "reason":    "SYSTEM_SESSION_PAUSED",
            }))
            return GateResult.SESSION_PAUSE

        # Check 2: Max concurrent (RE-SEM-002)
        if self._position_semaphore._value <= 0:
            self._logger.info(log_record({
                "event":           "GATE_7_RESULT",
                "level":           "INFO",
                "component":       "RISK_ENG",
                "symbol":          symbol,
                "result":          GateResult.BLOCK,
                "reason":          "MAX_CONCURRENT_REACHED",
                "semaphore_count": self._max_concurrent,
            }))
            return GateResult.BLOCK

        # Check 3: Drawdown snapshot
        full_halt_thresh = Decimal(str(
            self._params.get("full_halt_drawdown", FULL_HALT_DRAWDOWN)
        ))
        pause_thresh = Decimal(str(
            self._params.get("session_pause_drawdown", SESSION_PAUSE_DRAWDOWN)
        ))

        if self._drawdown_pct >= full_halt_thresh:
            await self._check_circuit_breakers(self._drawdown_pct)
            return GateResult.FULL_HALT

        if self._drawdown_pct >= pause_thresh:
            await self._check_circuit_breakers(self._drawdown_pct)
            return GateResult.SESSION_PAUSE

        # Check 4: Available USD (RE-SZ-005)
        per_trade_usd = self._compute_per_trade_usd()
        spot_balance = Decimal(str(self._wm.spot_usd_balance))
        pending_total = sum(self._wm.pending_orders.values())
        available_usd = spot_balance - pending_total

        if available_usd < per_trade_usd:
            self._logger.info(log_record({
                "event":          "GATE_7_RESULT",
                "level":          "INFO",
                "component":      "RISK_ENG",
                "symbol":         symbol,
                "result":         GateResult.BLOCK,
                "reason":         "INSUFFICIENT_USD",
                "available_usd":  available_usd,
                "per_trade_usd":  per_trade_usd,
            }))
            return GateResult.BLOCK

        # PASS
        self._logger.info(log_record({
            "event":           "GATE_7_RESULT",
            "level":           "INFO",
            "component":       "RISK_ENG",
            "symbol":          symbol,
            "result":          GateResult.PASS,
            "reason":          "PASS",
            "drawdown_pct":    self._drawdown_pct,
            "semaphore_value": self._position_semaphore._value,
            "available_usd":   available_usd,
            "per_trade_usd":   per_trade_usd,
        }))
        return GateResult.PASS

    # =============================================================
    # GATE 8 — POSITION SIZER — RE-SIZER-001 through -007
    # =============================================================

    def gate_8(
        self,
        symbol: str,
        entry_fill_price: Decimal,
        atr_14: Decimal,
        pair_spec: dict,
    ) -> dict | None:
        """
        Gate 8 Position Sizer. Computes qty, TP, emergSL.
        Called AFTER entry fill (uses actual avg_price — RE-SIZER-001).
        Sacred 1:1.5 R:R hardcoded (AR-011).

        Args:
            symbol:           trading pair
            entry_fill_price: actual avg_price from exec_type=filled
            atr_14:           current ATR(14) for this symbol
            pair_spec:        {price_increment, qty_increment, qty_min, cost_min}

        Returns:
            dict with entry_qty, tp_price, emergsl_price, net_RR
            None if size below minimum (logs GATE_8_SIZE_BELOW_MIN)
        """
        entry_fill_price = Decimal(str(entry_fill_price))
        atr_14 = Decimal(str(atr_14))

        price_incr = Decimal(str(pair_spec["price_increment"]))
        qty_incr   = Decimal(str(pair_spec["qty_increment"]))
        qty_min    = Decimal(str(pair_spec["qty_min"]))
        cost_min   = Decimal(str(pair_spec["cost_min"]))

        # ── Sizing (RE-SZ-002/003/004) ────────────────────────────────────
        per_trade_usd = self._compute_per_trade_usd()
        # RE-SZ-001: portfolio_USD = spot_usd_balance (realized only, never MTM)
        raw_qty = per_trade_usd / entry_fill_price
        # BP-DEC-002: ROUND_DOWN for quantities (conservative)
        order_qty = raw_qty.quantize(qty_incr, rounding=ROUND_DOWN)

        # ── Minimum size validation (RE-SIZER-005) ────────────────────────
        if order_qty < qty_min or order_qty * entry_fill_price < cost_min:
            self._logger.info(log_record({
                "event":       "GATE_8_SIZE_BELOW_MIN",
                "level":       "INFO",
                "component":   "RISK_ENG",
                "symbol":      symbol,
                "order_qty":   order_qty,
                "qty_min":     qty_min,
                "cost_min":    cost_min,
                "fill_price":  entry_fill_price,
            }))
            return None   # RE-SIZER-006: block this pair, pipeline continues

        # ── MAE and gross target (RE-SIZER-002) ──────────────────────────
        mae_mult = Decimal(str(self._params.get("mae_mult", MAE_MULT)))

        mae_pct = atr_14 * mae_mult / entry_fill_price

        # Net loss = MAE % + entry maker fee + emergSL taker fee
        net_loss_pct = mae_pct + ENTRY_FEE_PCT + SL_FEE_PCT

        # Gross target: achieves NET 1:1.5 R:R after TP maker fee
        # gross_target = (1.5 * mae_pct + entry_fee + tp_fee) / (1 - tp_fee)
        gross_target = (
            (SACRED_RR * mae_pct + ENTRY_FEE_PCT + TP_FEE_PCT)
            / (Decimal("1") - TP_FEE_PCT)
        )

        # ── TP price (RE-SIZER-003) ───────────────────────────────────────
        raw_tp = entry_fill_price * (Decimal("1") + gross_target)
        # BP-DEC-004: ROUND_UP for TP — higher TP preserves net R:R
        tp_price = raw_tp.quantize(price_incr, rounding=ROUND_UP)

        # ── emergSL trigger price (RE-SIZER-004) ─────────────────────────
        emergsl_mult = Decimal(str(self._params.get("emergsl_mult", EMERGSL_MULT)))
        raw_sl = entry_fill_price - (atr_14 * emergsl_mult)
        # BP-DEC-005: ROUND_DOWN for emergSL — lower trigger fires earlier (safer)
        emergsl_price = raw_sl.quantize(price_incr, rounding=ROUND_DOWN)

        # ── Validate prices are sensible ─────────────────────────────────
        if tp_price <= entry_fill_price:
            self._logger.critical(log_record({
                "event":       "GATE_8_TP_INVALID",
                "level":       "CRITICAL",
                "component":   "RISK_ENG",
                "symbol":      symbol,
                "tp_price":    tp_price,
                "entry_price": entry_fill_price,
            }))
            return None

        if emergsl_price >= entry_fill_price:
            self._logger.critical(log_record({
                "event":       "GATE_8_SL_INVALID",
                "level":       "CRITICAL",
                "component":   "RISK_ENG",
                "symbol":      symbol,
                "sl_price":    emergsl_price,
                "entry_price": entry_fill_price,
            }))
            return None

        result = {
            "entry_qty":     order_qty,
            "tp_price":      tp_price,
            "emergsl_price": emergsl_price,
            "net_RR":        SACRED_RR,   # AR-011 HARDCODED — NEVER changes
            "mae_pct":       mae_pct,
            "gross_target":  gross_target,
        }

        self._logger.info(log_record({
            "event":         "GATE_8_RESULT",
            "level":         "INFO",
            "component":     "RISK_ENG",
            "symbol":        symbol,
            "entry_qty":     order_qty,
            "entry_price":   entry_fill_price,
            "tp_price":      tp_price,
            "sl_trigger":    emergsl_price,
            "net_RR":        SACRED_RR,
            "mae_pct":       mae_pct,
        }))

        return result

    # =============================================================
    # SIZING HELPERS
    # =============================================================

    def _compute_per_trade_usd(self) -> Decimal:
        """
        Compute per_trade_USD. Fixed PCT before 200 trades.
        Half-Kelly at 200+ trades (RE-SZ-003).
        RE-CIATS-002: CIATS writes Kelly fraction to Parameter Store.
        Risk Engine reads on next pipeline eval — no restart required.
        """
        spot_balance = Decimal(str(self._wm.spot_usd_balance))
        tradeable_pct = Decimal(str(
            self._params.get("tradeable_pct", TRADEABLE_PCT)
        ))
        tradeable_usd = spot_balance * tradeable_pct

        # Check for Half-Kelly activation (CIATS writes at 200 trades)
        kelly_fraction = self._params.get("kelly_fraction")
        if kelly_fraction is not None:
            # Half-Kelly active (RE-SZ-003)
            k_half = Decimal(str(kelly_fraction))
            if k_half <= Decimal("0"):
                # Negative Kelly → no trade, log CRITICAL
                self._logger.critical(log_record({
                    "event":     "KELLY_NEGATIVE",
                    "level":     "CRITICAL",
                    "component": "RISK_ENG",
                    "kelly":     k_half,
                }))
                return Decimal("0")
            max_conc = Decimal(str(self._max_concurrent))
            per_trade_usd = min(
                k_half * tradeable_usd,
                tradeable_usd / max_conc,
            )
        else:
            # Fixed PCT (pre-200 trades)
            per_trade_pct = Decimal(str(
                self._params.get("per_trade_pct", PER_TRADE_PCT)
            ))
            per_trade_usd = tradeable_usd * per_trade_pct

        return per_trade_usd

    # =============================================================
    # SEMAPHORE MANAGEMENT — RE-SEM-003 through -006
    # =============================================================

    async def acquire_semaphore(self) -> bool:
        """
        Acquire position semaphore before entry dispatch.
        Non-blocking check — returns False if full.
        RE-SEM-002/003.
        """
        if self._position_semaphore._value <= 0:
            return False
        try:
            await self._position_semaphore.acquire()
            return True
        except ValueError as e:
            # RE-SEM-005: BoundedSemaphore ValueError = CRITICAL BUG
            self._logger.critical(log_record({
                "event":     "INVARIANT_BREACH",
                "level":     "CRITICAL",
                "component": "RISK_ENG",
                "error":     str(e),
                "note":      "BoundedSemaphore ValueError — halt system",
            }))
            _alert_operator_direct(
                f"INVARIANT BREACH: BoundedSemaphore error: {e}. "
                f"System halted. Operator restart required."
            )
            self._system_state = SystemState.FULL_HALT
            return False

    def release_semaphore(self) -> None:
        """
        Release semaphore on entry expiry, rejection, or error.
        RE-SEM-004: release on GTD expiry, post-only rejection, or error path.
        RE-SEM-005: BoundedSemaphore ValueError = CRITICAL BUG.
        """
        try:
            self._position_semaphore.release()
        except ValueError as e:
            self._logger.critical(log_record({
                "event":     "INVARIANT_BREACH",
                "level":     "CRITICAL",
                "component": "RISK_ENG",
                "error":     str(e),
                "note":      "Semaphore released more times than acquired",
            }))
            _alert_operator_direct(
                f"INVARIANT BREACH: Semaphore over-released: {e}. "
                f"Position count may be wrong. Operator review required."
            )

    # =============================================================
    # STATE ACCESSORS
    # =============================================================

    @property
    def system_state(self) -> SystemState:
        """Current system state (NORMAL | SESSION_PAUSED | FULL_HALT)."""
        return self._system_state

    @property
    def drawdown_pct(self) -> Decimal:
        """Current drawdown percentage (latest computed value)."""
        return self._drawdown_pct

    @property
    def portfolio_baseline_usd(self) -> Decimal | None:
        """Portfolio baseline set once at startup. None until set."""
        return self._portfolio_baseline_usd

    @property
    def peak_portfolio_usd(self) -> Decimal:
        """Peak portfolio USD (for CIATS max drawdown tracking — RE-DD-003)."""
        return self._peak_portfolio_usd

    def update_half_kelly(
        self,
        win_rate: Decimal,
        avg_rr: Decimal,
        trade_count: int,
    ) -> None:
        """
        Update Half-Kelly inputs from CIATS at 200+ closed trades.
        K_full = W - ((1 - W) / R). K_half = K_full * 0.5.
        Writes kelly_fraction to param_store for next pipeline read.
        RE-CIATS-002, RE-SZ-003.
        """
        w = Decimal(str(win_rate))
        r = Decimal(str(avg_rr))

        if r == Decimal("0"):
            self._logger.critical(log_record({
                "event":     "KELLY_DIVISION_BY_ZERO",
                "level":     "CRITICAL",
                "component": "RISK_ENG",
                "note":      "avg_rr = 0. Kelly not updated.",
            }))
            return

        k_full = w - ((Decimal("1") - w) / r)
        k_half = k_full * Decimal("0.5")

        self._logger.info(log_record({
            "event":          "CIATS_KELLY_UPDATE",
            "level":          "INFO",
            "component":      "RISK_ENG",
            "kelly_fraction": k_half,
            "trade_count":    trade_count,
            "win_rate":       w,
            "avg_rr":         r,
        }))

        if k_half <= Decimal("0"):
            self._logger.critical(log_record({
                "event":     "KELLY_NEGATIVE",
                "level":     "CRITICAL",
                "component": "RISK_ENG",
                "kelly":     k_half,
                "note":      "Negative Kelly — no update applied",
            }))
            return

        self._params["kelly_fraction"] = str(k_half)
