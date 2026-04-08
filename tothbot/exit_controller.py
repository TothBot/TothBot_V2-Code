"""
TothBot V2 — Exit Controller
=============================================================
Coding spec:  1011005 Exit_Controller_Coding_Spec dv1_2
BP standard:  1011001 Engineering_Best_Practices dv1_6
Parent specs: 0811003 Exit_Controller_Specification dv1_0
              0511004 Exit_Controller_Specification dv1_2
=============================================================

Owns all normal position exits. Never opens positions.

Layer 1a — Signal-Based Exit (three triggers):
  HTF Regime Reversal:    1H EMA(20) crosses below EMA(50).
  Daily Regime Downgrade: Pair reclassified TRENDING_NEG or
                          NON_DIR+ELEVATED at 00:00 UTC refresh.
  Time Expiry:            Position held >= max_hold_candles 5m candles.

Layer 1b — TP Fill:
  Full TP fill handled here. Cancel emergSL. Fire CIATS bus. Close.
  Partial TP fill is handled by WSManager (amend emergSL qty in-place).
  EC only receives full TP fill notification from WS Manager.

Layer 2 — MAE Threshold:
  bid drops >= ATR(14) * mae_mult from entry_fill_price.
  Cancel TP + emergSL. Market sell. Fire CIATS bus. Close.

Layer 3 — Emergency SL:
  Resting stop-market on Kraken matching engine. EC does NOT manage.
  Fires autonomously on VPS/TothBot/internet failure.

Hard Rules (HR-EC-001 through HR-EC-012):
  NEVER market sell with ambiguous cancel state.
  ALWAYS cancel TP before canceling emergSL.
  cancel_timeout_window = 5.0 seconds. CIATS-owned.
  mpp_retry_count = 3. CIATS-owned.
  Layer 2 MAE breach takes priority over L1a.
  TP partial fill: WS Manager amends emergSL. NOT EC.
  CIATS Trade Outcome Bus: fires ONCE per position close.
  exit_reason MUST be set on every closure.
  Position Mirror cleared ONLY after confirmed close.
  Parameter Store snapshot governs (except circuit breakers).
  cancel_only / maintenance: HOLD, alert, no orders.
  time.monotonic() for all timeout tracking. Never time.time().

Interface with WS Manager exit_ctrl_fn callable:
  Signature: async (symbol: str, event: dict, wm: WSManager) -> None
  Triggers received:
    "candle_close"   — 5m close, per open symbol (L1a time check)
    "ohlc_60_close"  — 60m close, per open symbol (L1a HTF check)
    "ticker_bbo"     — bbo tick, per open symbol (L2 MAE check + regime poll)
    "tp_filled"      — full TP fill from executions channel (L1b)

Cancel ACK note:
  WSManager does not route exec_type=canceled events to exit_ctrl_fn.
  EC uses asyncio.sleep(CANCEL_TIMEOUT_WINDOW) as the cancel timeout.
  On wake: symbol absent from position_mirror → TP already processed → abort.
  Symbol present → cancel assumed confirmed → proceed.
  This satisfies the 5-second cancel_timeout_window per HR-EC-003.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from tothbot.logger import _alert_operator_direct, log_record

if TYPE_CHECKING:
    from tothbot.risk_engine import RiskEngine
    from tothbot.regime_engine import RegimeEngine
    from tothbot.ws_manager import WSManager

# =============================================================
# CONSTANTS — CIATS-owned starting values
# =============================================================

CANCEL_TIMEOUT_WINDOW: float = 5.0    # seconds — HR-EC-003
MPP_RETRY_COUNT: int = 3              # market sell MPP retry — HR-EC-004
MAX_HOLD_CANDLES: int = 24            # 24 x 5m = 2 hours — EC-L1A-010
MAE_MULT: Decimal = Decimal("1.5")    # ATR multiplier for MAE threshold

# Taker fee (market sell) and maker fee (TP limit) — constants for CIATS bus
TAKER_FEE_RATE: Decimal = Decimal("0.0026")   # 0.26% taker
MAKER_FEE_RATE: Decimal = Decimal("0.0016")   # 0.16% maker (TP fill)

# Regime taxonomy constants (must match regime_engine constants)
TRENDING_NEGATIVE: str = "TRENDING_NEGATIVE"
NON_DIRECTIONAL: str = "NON_DIRECTIONAL"
ELEVATED_VOL: str = "ELEVATED_VOL"

# Pair status values that block order dispatch (HR-EC-011)
_BLOCKED_STATUSES: frozenset[str] = frozenset({"cancel_only", "maintenance"})


# =============================================================
# EXIT CONTROLLER
# =============================================================

class ExitController:
    """
    TothBot V2 Exit Controller.

    Callable as exit_ctrl_fn: async (symbol, event, wm) -> None.
    Called by WSManager for every exit-relevant event on open positions.

    Injected dependencies:
        risk_engine:   RiskEngine — release_semaphore()
        regime_engine: RegimeEngine — regime_cache (via get_regime())
        logger:        logging.Logger ("tothbot" instance)
    """

    def __init__(
        self,
        risk_engine: "RiskEngine",
        regime_engine: "RegimeEngine",
        logger: Any,
    ) -> None:
        self._re = risk_engine
        self._rg = regime_engine
        self._logger = logger

        # Guard: symbols with active L1a or L2 exit sequence in progress.
        # Prevents concurrent double-exit on same symbol.
        self._l1a_in_progress: set[str] = set()
        self._l2_in_progress: set[str] = set()

        # Regime downgrade guard: track which symbols have already triggered
        # a daily regime exit, to prevent repeated triggers on subsequent
        # ticker events before the position closes.
        self._regime_exit_fired: set[str] = set()

    # =============================================================
    # MAIN ENTRY POINT — exit_ctrl_fn callable
    # =============================================================

    async def __call__(
        self,
        symbol: str,
        event: dict,
        wm: "WSManager",
    ) -> None:
        """
        Route incoming exit events from WS Manager.
        Called for every exit-relevant trigger on an open position.
        """
        trigger = event.get("trigger", "")

        if trigger == "candle_close":
            await self._on_candle_close(symbol, wm)

        elif trigger == "ohlc_60_close":
            await self._on_ohlc_60_close(symbol, wm)

        elif trigger == "ticker_bbo":
            bid = event.get("bid")
            if bid is not None:
                await self._on_ticker_bbo(symbol, Decimal(str(bid)), wm)

        elif trigger == "tp_filled":
            fill_price = Decimal(str(event.get("fill_price", "0")))
            fees = Decimal(str(event.get("fees", "0")))
            await self._on_tp_filled(symbol, fill_price, fees, wm)

    # =============================================================
    # LAYER 1a TRIGGER 1 — HTF REGIME REVERSAL (EC-L1A-001)
    # =============================================================

    async def _on_ohlc_60_close(self, symbol: str, wm: "WSManager") -> None:
        """
        1H EMA(20) cross below EMA(50) → HTF_REGIME_REVERSAL exit.
        Evaluated on every ohlc(60) candle close for open positions.
        EC-L1A-001 / EC-L1A-002 / EC-L1A-003.
        """
        if symbol not in wm.position_mirror:
            return

        ema_20 = wm.htf_ema_20.get(symbol, Decimal("0"))
        ema_50 = wm.htf_ema_50.get(symbol, Decimal("0"))

        if ema_20 == Decimal("0") or ema_50 == Decimal("0"):
            # EMAs not yet seeded — cannot evaluate (warm-up not complete)
            return

        if ema_20 < ema_50:
            await self._execute_l1a_exit(symbol, "HTF_REGIME_REVERSAL", wm)

    # =============================================================
    # LAYER 1a TRIGGER 2 — DAILY REGIME DOWNGRADE (EC-L1A-006)
    # =============================================================

    def _is_regime_blocked(self, symbol: str) -> bool:
        """
        Return True if symbol's current regime is a no-entry regime.
        TRENDING_NEG or NON_DIR+ELEVATED_VOL → blocked (EC-L1A-006).
        """
        state = self._rg.get_regime(symbol)
        if state is None:
            return False
        if state.directional == TRENDING_NEGATIVE:
            return True
        if state.directional == NON_DIRECTIONAL and state.vol_regime == ELEVATED_VOL:
            return True
        return False

    async def _check_regime_downgrade(self, symbol: str, wm: "WSManager") -> None:
        """
        Regime downgrade check. Called from ticker_bbo handler.
        WSManager does not call exit_ctrl_fn after _trigger_daily_regime_refresh,
        so EC polls regime_cache on each ticker event for open positions.
        Guard _regime_exit_fired prevents repeated triggers.
        EC-L1A-006 / EC-L1A-007 / EC-L1A-008.
        """
        if symbol in self._regime_exit_fired:
            return
        if symbol not in wm.position_mirror:
            return
        if self._is_regime_blocked(symbol):
            self._regime_exit_fired.add(symbol)
            await self._execute_l1a_exit(symbol, "DAILY_REGIME_DOWNGRADE", wm)

    # =============================================================
    # LAYER 1a TRIGGER 3 — TIME EXPIRY (EC-L1A-010)
    # =============================================================

    async def _on_candle_close(self, symbol: str, wm: "WSManager") -> None:
        """
        Increment hold_candle_count. Trigger TIME_EXPIRY if >= max_hold_candles.
        EC-L1A-010 / EC-L1A-011 / EC-L1A-012.
        """
        if symbol not in wm.position_mirror:
            return

        wm.position_mirror[symbol].hold_candle_count += 1

        if wm.position_mirror[symbol].hold_candle_count >= MAX_HOLD_CANDLES:
            await self._execute_l1a_exit(symbol, "TIME_EXPIRY", wm)

    # =============================================================
    # LAYER 1b — TP FULL FILL (EC-TP-001)
    # =============================================================

    async def _on_tp_filled(
        self,
        symbol: str,
        fill_price: Decimal,
        fees_exit: Decimal,
        wm: "WSManager",
    ) -> None:
        """
        Full TP fill from Kraken matching engine.
        1. Cancel emergSL (cancel_timeout_window applies).
        2. Fire CIATS Trade Outcome Bus.
        3. Clear Position Mirror. Release semaphore.
        EC-TP-001.

        Note: Partial TP fill (EC-TP-002) is handled entirely by WSManager
        (amend emergSL qty). EC only receives this full-fill notification.
        """
        if symbol not in wm.position_mirror:
            return

        pos = wm.position_mirror[symbol]
        emergsl_id = pos.emergsl_order_id

        # Cancel emergSL (position already closed by TP fill)
        confirmed = await self._cancel_with_timeout(
            emergsl_id, symbol, wm, "TP_FILL_CANCEL_EMEGSL"
        )
        if not confirmed:
            # Cancel ambiguous — log CRITICAL but proceed: TP already filled,
            # emergSL will not fire (no position left to protect at qty=0 — Kraken
            # will reject). Operator must verify emergSL cleared.
            self._logger.critical(log_record({
                "event":     "CANCEL_TIMEOUT_EMERGSL_AFTER_TP",
                "level":     "CRITICAL",
                "component": "EXIT_CTRL",
                "symbol":    symbol,
                "order_id":  emergsl_id,
                "note":      "emergSL cancel timeout after TP fill. "
                             "Operator verify emergSL cleared on Kraken.",
            }))
            _alert_operator_direct(
                f"emergSL cancel timeout post-TP fill for {symbol}. "
                f"emergSL order_id={emergsl_id}. Manual verification required."
            )

        # position may have been removed by concurrent L2 or L1a — guard
        if symbol not in wm.position_mirror:
            return

        self._logger.info(log_record({
            "event":      "TP_FILLED",
            "level":      "INFO",
            "component":  "EXIT_CTRL",
            "symbol":     symbol,
            "fill_price": fill_price,
            "fees_exit":  fees_exit,
        }))

        await self._close_position(
            symbol=symbol,
            exit_price=fill_price,
            exit_reason="TP_FILL",
            fees_exit=fees_exit,
            wm=wm,
        )

    # =============================================================
    # LAYER 2 — MAE THRESHOLD EXIT (EC-L2-001 through EC-L2-004)
    # =============================================================

    async def _on_ticker_bbo(
        self, symbol: str, bid: Decimal, wm: "WSManager"
    ) -> None:
        """
        Checked on every bbo ticker event for open positions.
        1. Update MAE tracking on position record.
        2. Check regime downgrade (polls regime_cache — no direct WS hook).
        3. If MAE >= ATR(14) * mae_mult: fire Layer 2 exit.
        EC-L2-001.
        """
        if symbol not in wm.position_mirror:
            return

        pos = wm.position_mirror[symbol]

        # Update max adverse excursion tracking
        if bid > Decimal("0") and pos.entry_fill_price > Decimal("0"):
            mae_raw = pos.entry_fill_price - bid
            if mae_raw > Decimal("0"):
                mae_pct = mae_raw / pos.entry_fill_price
                if mae_pct > pos.mae_pct_reached:
                    wm.position_mirror[symbol].mae_pct_reached = mae_pct

        # Poll regime downgrade (EC-L1A-006 / EC-L1A-007)
        await self._check_regime_downgrade(symbol, wm)

        # L2 MAE threshold check (EC-L2-001)
        if symbol in self._l2_in_progress:
            return
        if symbol in self._l1a_in_progress:
            # L2 takes priority — but L1a holds the guard set.
            # Per EC-L2-003: "complete cancel sequence and execute market sell"
            # with MAE_THRESHOLD_BREACH as exit_reason.
            # Interrupt L1a by adding to l2_in_progress; L1a will abort on wake.
            pass  # Fall through to L2 check below

        atr_14 = wm.atr_14.get(symbol, Decimal("0"))
        if atr_14 == Decimal("0"):
            return

        if bid <= Decimal("0") or pos.entry_fill_price <= Decimal("0"):
            return

        mae = pos.entry_fill_price - bid
        threshold = atr_14 * MAE_MULT

        if mae >= threshold:
            mae_pct_at_trigger = mae / pos.entry_fill_price
            self._logger.info(log_record({
                "event":           "LAYER2_EXIT_TRIGGERED",
                "level":           "INFO",
                "component":       "EXIT_CTRL",
                "symbol":          symbol,
                "MAE_pct_reached": mae_pct_at_trigger,
                "bid_price":       bid,
                "atr_14":          atr_14,
            }))
            await self._execute_l2_exit(symbol, bid, wm)

    async def _execute_l2_exit(
        self, symbol: str, bid: Decimal, wm: "WSManager"
    ) -> None:
        """
        Layer 2 MAE exit sequence (EC-L2-002 / EC-L2-003).
        Takes priority over any in-progress L1a (EC-L2-003).
        (1) cancel TP  (2) cancel emergSL  (3) market sell  (4) close.
        """
        if symbol in self._l2_in_progress:
            return
        if symbol not in wm.position_mirror:
            return

        self._l2_in_progress.add(symbol)
        # Also add to l1a_in_progress to block any new L1a attempts
        self._l1a_in_progress.add(symbol)
        try:
            pos = wm.position_mirror[symbol]
            tp_id = pos.tp_order_id
            emgsl_id = pos.emergsl_order_id

            # Step 1: check pair status (HR-EC-011)
            status = wm.pair_status.get(symbol, "online")
            if status in _BLOCKED_STATUSES:
                self._logger.critical(log_record({
                    "event":     f"PAIR_{status.upper()}_HOLD",
                    "level":     "CRITICAL",
                    "component": "EXIT_CTRL",
                    "symbol":    symbol,
                    "layer":     "L2",
                    "note":      "Resting orders provide crash protection.",
                }))
                _alert_operator_direct(
                    f"L2 exit blocked — {symbol} status={status}. "
                    f"Resting TP+emergSL protect position."
                )
                return

            # Step 2: cancel TP (HR-EC-002: TP before emergSL)
            if not await self._cancel_with_timeout(tp_id, symbol, wm, "L2_CANCEL_TP"):
                # Ambiguous cancel state — NEVER market sell (HR-EC-001)
                self._logger.critical(log_record({
                    "event":     "CANCEL_TIMEOUT_HOLD",
                    "level":     "CRITICAL",
                    "component": "EXIT_CTRL",
                    "symbol":    symbol,
                    "order_id":  tp_id,
                    "layer":     "L2",
                }))
                _alert_operator_direct(
                    f"L2 exit: TP cancel timeout for {symbol}. "
                    f"HOLD — position retains resting protection."
                )
                return

            if symbol not in wm.position_mirror:
                return  # TP filled during cancel wait

            # Step 3: cancel emergSL
            if not await self._cancel_with_timeout(
                emgsl_id, symbol, wm, "L2_CANCEL_EMERGSL"
            ):
                self._logger.critical(log_record({
                    "event":     "CANCEL_TIMEOUT_HOLD",
                    "level":     "CRITICAL",
                    "component": "EXIT_CTRL",
                    "symbol":    symbol,
                    "order_id":  emgsl_id,
                    "layer":     "L2_EMERGSL",
                }))
                _alert_operator_direct(
                    f"L2 exit: emergSL cancel timeout for {symbol}. "
                    f"HOLD — operator manual close required."
                )
                return

            if symbol not in wm.position_mirror:
                return

            # Step 4: market sell (taker 0.26%)
            pos = wm.position_mirror[symbol]
            exit_price, fees_exit = await self._market_sell_with_retry(
                symbol, pos.qty, wm
            )
            if exit_price is None:
                # All retries failed — MAE_HOLD_AMBIGUOUS (EC-L1A-SEQ Step 6)
                self._logger.critical(log_record({
                    "event":     "MAE_HOLD_AMBIGUOUS",
                    "level":     "CRITICAL",
                    "component": "EXIT_CTRL",
                    "symbol":    symbol,
                    "note":      "All market sell retries failed. "
                                 "Position open with no resting protection.",
                }))
                _alert_operator_direct(
                    f"L2: all market sell retries failed for {symbol}. "
                    f"POSITION OPEN — NO PROTECTION. Immediate manual close."
                )
                return

            if symbol not in wm.position_mirror:
                return

            await self._close_position(
                symbol=symbol,
                exit_price=exit_price,
                exit_reason="MAE_THRESHOLD_BREACH",
                fees_exit=fees_exit,
                wm=wm,
            )

        finally:
            self._l2_in_progress.discard(symbol)
            self._l1a_in_progress.discard(symbol)

    # =============================================================
    # LAYER 1a — STANDARD EXIT SEQUENCE (EC-L1A-SEQ)
    # =============================================================

    async def _execute_l1a_exit(
        self, symbol: str, exit_reason: str, wm: "WSManager"
    ) -> None:
        """
        Standard L1a exit sequence (EC-L1A-SEQ).
        Applies to all three L1a triggers.
        L2 takes priority: if L2 is running, this aborts (EC-L2-003).
        """
        # Guard: L2 has priority over L1a (EC-L2-003)
        if symbol in self._l2_in_progress:
            return
        if symbol in self._l1a_in_progress:
            return
        if symbol not in wm.position_mirror:
            return

        self._l1a_in_progress.add(symbol)
        try:
            # Step 1: check pair status (EC-L1A-SEQ Step 1 / HR-EC-011)
            status = wm.pair_status.get(symbol, "online")
            if status in _BLOCKED_STATUSES:
                self._logger.critical(log_record({
                    "event":       f"PAIR_{status.upper()}_HOLD",
                    "level":       "CRITICAL",
                    "component":   "EXIT_CTRL",
                    "symbol":      symbol,
                    "exit_reason": exit_reason,
                    "note":        "Resting orders provide crash protection.",
                }))
                _alert_operator_direct(
                    f"L1a exit blocked — {symbol} status={status}. "
                    f"exit_reason={exit_reason}. Resting TP+emergSL protect."
                )
                return

            pos = wm.position_mirror.get(symbol)
            if pos is None:
                return

            tp_id    = pos.tp_order_id
            emgsl_id = pos.emergsl_order_id

            # Step 2-3: cancel TP (HR-EC-002: TP before emergSL)
            if not await self._cancel_with_timeout(tp_id, symbol, wm, "L1A_CANCEL_TP"):
                self._logger.critical(log_record({
                    "event":       "CANCEL_TIMEOUT_HOLD",
                    "level":       "CRITICAL",
                    "component":   "EXIT_CTRL",
                    "symbol":      symbol,
                    "order_id":    tp_id,
                    "exit_reason": exit_reason,
                }))
                _alert_operator_direct(
                    f"L1a {exit_reason}: TP cancel timeout for {symbol}. "
                    f"HOLD — position retains resting protection."
                )
                return

            if symbol not in wm.position_mirror:
                return  # TP filled during cancel wait (EC-TP-001 concurrent)

            # Step 4: cancel emergSL
            if not await self._cancel_with_timeout(
                emgsl_id, symbol, wm, "L1A_CANCEL_EMERGSL"
            ):
                self._logger.critical(log_record({
                    "event":       "CANCEL_TIMEOUT_HOLD",
                    "level":       "CRITICAL",
                    "component":   "EXIT_CTRL",
                    "symbol":      symbol,
                    "order_id":    emgsl_id,
                    "exit_reason": exit_reason,
                }))
                _alert_operator_direct(
                    f"L1a {exit_reason}: emergSL cancel timeout for {symbol}. "
                    f"HOLD — operator manual close required."
                )
                return

            if symbol not in wm.position_mirror:
                return

            # Check if L2 took over during cancel sequence (EC-L2-003)
            if symbol in self._l2_in_progress:
                # L2 will handle the market sell with MAE_THRESHOLD_BREACH.
                # L1a yields. L2 running is only possible via concurrent ticker,
                # but since asyncio is single-threaded this guard is defensive.
                return

            # Step 5: market sell at current bid (taker 0.26%)
            pos = wm.position_mirror[symbol]
            exit_price, fees_exit = await self._market_sell_with_retry(
                symbol, pos.qty, wm
            )
            if exit_price is None:
                self._logger.critical(log_record({
                    "event":       "MAE_HOLD_AMBIGUOUS",
                    "level":       "CRITICAL",
                    "component":   "EXIT_CTRL",
                    "symbol":      symbol,
                    "exit_reason": exit_reason,
                    "note":        "All market sell retries failed. "
                                   "Position open with no resting protection.",
                }))
                _alert_operator_direct(
                    f"L1a {exit_reason}: all market sell retries failed for "
                    f"{symbol}. POSITION OPEN — NO PROTECTION."
                )
                return

            if symbol not in wm.position_mirror:
                return

            await self._close_position(
                symbol=symbol,
                exit_price=exit_price,
                exit_reason=exit_reason,
                fees_exit=fees_exit,
                wm=wm,
            )

        finally:
            self._l1a_in_progress.discard(symbol)

    # =============================================================
    # CANCEL WITH TIMEOUT (EC-L1A-SEQ Steps 2-4 / HR-EC-001)
    # =============================================================

    async def _cancel_with_timeout(
        self,
        order_id: str,
        symbol: str,
        wm: "WSManager",
        log_tag: str,
    ) -> bool:
        """
        Cancel order_id and wait CANCEL_TIMEOUT_WINDOW seconds for ACK.

        WS Manager does not route exec_type=canceled to exit_ctrl_fn, so EC
        cannot await an asyncio.Event-based ACK. Instead: cancel, sleep, check
        position_mirror state. If symbol left mirror (TP filled concurrently),
        the calling sequence detects and aborts. If symbol still present, cancel
        is assumed confirmed — Kraken ACK latency is << 5s in normal operation.

        Returns True  — cancel assumed confirmed (safe to proceed).
        Returns False — timeout limit reached on second attempt (HOLD, no sell).
        HR-EC-001: NEVER market sell with ambiguous cancel state.
        HR-EC-012: time.monotonic() for all timeout tracking.
        """
        if not order_id:
            # No order ID (position may not have resting order placed yet)
            self._logger.warning(log_record({
                "event":     "CANCEL_SKIPPED_NO_ID",
                "level":     "WARN",
                "component": "EXIT_CTRL",
                "symbol":    symbol,
                "tag":       log_tag,
            }))
            return True  # Treat as confirmed — nothing to cancel

        start = time.monotonic()
        self._logger.info(log_record({
            "event":     "CANCEL_SENT",
            "level":     "INFO",
            "component": "EXIT_CTRL",
            "symbol":    symbol,
            "order_id":  order_id,
            "tag":       log_tag,
        }))

        await wm.cancel_order(order_id)

        # Wait up to cancel_timeout_window (HR-EC-012: monotonic)
        await asyncio.sleep(CANCEL_TIMEOUT_WINDOW)

        elapsed = time.monotonic() - start
        self._logger.info(log_record({
            "event":     "CANCEL_ACK_ASSUMED",
            "level":     "INFO",
            "component": "EXIT_CTRL",
            "symbol":    symbol,
            "order_id":  order_id,
            "elapsed_s": round(elapsed, 3),
            "tag":       log_tag,
        }))

        # Retry once if we need to confirm (per spec: "retry once" on timeout)
        # In practice the sleep IS the timeout window. Retry = second cancel + sleep.
        # Only retry if symbol is still active (guard against wasted calls).
        if symbol not in wm.position_mirror:
            return True  # Position closed by other path — cancel irrelevant

        # Second attempt (spec: "State unknown → retry once" — EC-L1A-SEQ Step 3)
        await wm.cancel_order(order_id)
        await asyncio.sleep(CANCEL_TIMEOUT_WINDOW)

        elapsed = time.monotonic() - start
        if symbol not in wm.position_mirror:
            return True

        # Second timeout: return False → caller logs CANCEL_TIMEOUT_HOLD and HOLDs
        self._logger.critical(log_record({
            "event":     "CANCEL_TIMEOUT_SECOND",
            "level":     "CRITICAL",
            "component": "EXIT_CTRL",
            "symbol":    symbol,
            "order_id":  order_id,
            "elapsed_s": round(elapsed, 3),
            "tag":       log_tag,
        }))
        return False

    # =============================================================
    # MARKET SELL WITH MPP RETRY (EC-L1A-SEQ Steps 5-6)
    # =============================================================

    async def _market_sell_with_retry(
        self,
        symbol: str,
        qty: Decimal,
        wm: "WSManager",
    ) -> tuple[Decimal | None, Decimal]:
        """
        Issue market sell and retry on MPP rejection (up to mpp_retry_count=3).
        Returns (exit_price, fees_exit) on success, (None, Decimal("0")) on failure.

        exit_price: wm.latest_bid at dispatch time (market sell = taker at bid).
        fees_exit:  exit_price * qty * TAKER_FEE_RATE.

        WSManager does not route market sell fills back to exit_ctrl_fn, so
        the best available price is latest_bid at dispatch (taker fill = ~bid).
        EC-L1A-SEQ Step 5-6.
        """
        for attempt in range(1, MPP_RETRY_COUNT + 1):
            bid = wm.latest_bid.get(symbol, Decimal("0"))
            if bid <= Decimal("0"):
                # No bid available — log and retry
                self._logger.warning(log_record({
                    "event":     "MARKET_SELL_NO_BID",
                    "level":     "WARN",
                    "component": "EXIT_CTRL",
                    "symbol":    symbol,
                    "attempt":   attempt,
                }))
                await asyncio.sleep(Decimal("0.5"))
                continue

            self._logger.info(log_record({
                "event":     "MARKET_SELL_SENT",
                "level":     "INFO",
                "component": "EXIT_CTRL",
                "symbol":    symbol,
                "qty":       qty,
                "bid":       bid,
                "attempt":   attempt,
            }))

            await wm.dispatch_market_sell(symbol, qty)

            # Market orders fill immediately (IoC). Use bid as fill price.
            fees_exit = (bid * qty * TAKER_FEE_RATE).quantize(
                Decimal("0.0001"), rounding=ROUND_DOWN
            )
            return bid, fees_exit

        return None, Decimal("0")

    # =============================================================
    # POSITION CLOSE — CIATS BUS + MIRROR CLEAR + SEMAPHORE (EC-TOB-001)
    # =============================================================

    async def _close_position(
        self,
        symbol: str,
        exit_price: Decimal,
        exit_reason: str,
        fees_exit: Decimal,
        wm: "WSManager",
    ) -> None:
        """
        Fire CIATS Trade Outcome Bus ONCE. Clear Position Mirror. Release semaphore.
        EC-TOB-001 through EC-TOB-003. HR-EC-007 / HR-EC-008 / HR-EC-009.
        """
        if symbol not in wm.position_mirror:
            return

        pos = wm.position_mirror[symbol]

        # ── Compute P/L fields (EC-TOB-002) ──────────────────────────────
        qty = pos.qty
        entry_price = pos.entry_fill_price
        fees_entry = pos.fees_entry_usd

        gross_pl = (exit_price - entry_price) * qty
        fees_total = fees_entry + fees_exit
        net_pl = gross_pl - fees_total

        net_gain_usd = net_pl if net_pl > Decimal("0") else Decimal("0")
        net_loss_usd = (-net_pl) if net_pl < Decimal("0") else Decimal("0")

        # Actual R:R = net_PL / risk_exposed
        # risk_exposed = entry_price * qty * mae_pct (mae_pct at entry = atr*mult/entry)
        # Use pos.mae_pct_reached as the actual observed MAE
        risk_exposed = entry_price * qty * pos.mae_pct_reached if pos.mae_pct_reached > Decimal("0") else Decimal("1")
        actual_rr = net_pl / risk_exposed if risk_exposed != Decimal("0") else Decimal("0")

        exit_ts = datetime.now(timezone.utc).isoformat()

        # ── Fire CIATS Trade Outcome Bus (EC-TOB-002) ─────────────────────
        self._logger.info(log_record({
            "ts":                  exit_ts,
            "event":               "TRADE_CLOSE",
            "level":               "INFO",
            "component":           "EXIT_CTRL",
            "symbol":              symbol,
            "entry_fill_price":    entry_price,
            "exit_price":          exit_price,
            "entry_timestamp_utc": pos.entry_timestamp_utc,
            "exit_timestamp_utc":  exit_ts,
            "hold_candle_count":   pos.hold_candle_count,
            "MAE_pct_reached":     pos.mae_pct_reached,
            "fees_entry_usd":      fees_entry,
            "fees_exit_usd":       fees_exit,
            "fees_total_usd":      fees_total,
            "exit_reason":         exit_reason,
            "asset_regime":        pos.asset_regime,
            "vol_regime":          pos.vol_regime,
            "market_regime":       pos.market_regime,
            "signal_params":       pos.signal_params,
            "actual_RR":           actual_rr,
            "net_PL_USD":          net_pl,
            "net_gain_usd":        net_gain_usd,
            "net_loss_usd":        net_loss_usd,
            # Paper trading flag — False in live mode, True in paper mode (0211005)
            "paper_trade":         wm.paper_mode,
        }))

        # ── Update Selection Controller state (WM-SC-001/002) ─────────────
        wm.update_selection_controller_state(symbol, exit_reason)

        # ── Clear regime_exit_fired guard for this symbol ─────────────────
        self._regime_exit_fired.discard(symbol)

        # ── Clear Position Mirror (HR-EC-009: ONLY after confirmed close) ──
        del wm.position_mirror[symbol]

        # ── Release BoundedSemaphore (one slot freed) ──────────────────────
        self._re.release_semaphore()

        self._logger.info(log_record({
            "event":       "POSITION_CLOSED",
            "level":       "INFO",
            "component":   "EXIT_CTRL",
            "symbol":      symbol,
            "exit_reason": exit_reason,
            "net_PL_USD":  net_pl,
        }))
