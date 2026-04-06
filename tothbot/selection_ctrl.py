"""
TothBot V2 — Selection Controller
=============================================================
Coding spec:  1011009 Selection_Controller_Coding_Spec dv1_0
BP standard:  1011001 Engineering_Best_Practices dv1_6
Parent spec:  0811002 Selection_Controller_Specification dv1_0
=============================================================

Gate 5 of the 8-gate trading pipeline. Applies 6 quality
gates in strict sequence to every SSS PASS signal.

Gate 5 logic (all 6 gates + state dicts) is fully implemented
in SignalPipeline._gate_5() and SignalPipeline.on_trade_closed()
because Gate 5 is structurally embedded in the pipeline eval
loop. SelectionController is the canonical interface adapter:

  — on_position_closed(symbol, exit_reason): called by Exit
    Controller and Long Module on every position close.
    Delegates to SignalPipeline.on_trade_closed().

  — reset_consecutive_losses(symbol): called by CIATS on
    PDCA parameter update when manually clearing a streak.

  — State accessors: expose cooldown_registry and
    consecutive_loss_count for monitoring and CIATS reads.

Hard Rules (HR-SC-001 through -007):
  Gates 1-6 in strict sequence. First FAIL exits. (in SignalPipeline)
  FAIL = SKIP. Never HALT. Pipeline continues.
  sc_body_threshold applied to ATR(14) multiple.
  Cooldown uses time.monotonic(). Never time.time().
  State dicts preserved across WS reconnect (in SignalPipeline).
  All CIATS params from Parameter Store snapshot (I-4).
  Consecutive loss count resets on WIN only.

SC CIATS Parameters (starting values):
  sc_body_threshold:    0.3 (ATR multiple for candle body gate)
  sc_cooldown_seconds:  300 (seconds post-exit before re-entry)
  sc_consecutive_limit: 3   (consecutive losses before block)

Win/Loss classification (SC-024, AR-073):
  WIN:  exit_reason = TP_FILL | TP_PARTIAL_FILL_REMAINDER
        -> consecutive_loss_count[symbol] = 0
        -> cooldown_registry cleared (no re-entry penalty on win)
  LOSS: exit_reason = MAE_THRESHOLD_BREACH | TIME_EXPIRY |
                      HTF_REGIME_REVERSAL | DAILY_REGIME_DOWNGRADE |
                      SIGNAL_DECAY | MOMENTUM_LOSS
        -> consecutive_loss_count[symbol] += 1
        -> cooldown_registry[symbol] = time.monotonic()
"""
from __future__ import annotations

import time
from typing import Any

from tothbot.logger import log_record


# =============================================================
# WIN EXIT REASONS
# =============================================================

WIN_EXIT_REASONS: frozenset[str] = frozenset({
    "TP_FILL",
    "TP_PARTIAL_FILL_REMAINDER",
})


# =============================================================
# SELECTION CONTROLLER
# =============================================================

class SelectionController:
    """
    TothBot V2 Selection Controller -- Gate 5 adapter.

    Owns the on_position_closed() callback interface used by
    Exit Controller and Long Module. All gate evaluation and
    state management is performed inside SignalPipeline
    (which hosts the per-symbol state dicts so they survive
    reconnect without an additional persistence layer).

    Injected dependencies:
      signal_pipeline:  SignalPipeline -- on_trade_closed(),
                                          _cooldown_registry,
                                          _consecutive_loss_count
      logger:           logging.Logger ("tothbot" instance)

    Called by:
      ExitController on every position close (exit_reason string)
      LongModule for below-min emergency sells
      CIATS for PDCA parameter updates (reset_consecutive_losses)
    """

    def __init__(
        self,
        signal_pipeline: Any,
        logger: Any,
    ) -> None:
        self._sp = signal_pipeline
        self._logger = logger

    # =============================================================
    # POSITION CLOSE CALLBACK  (SC-STATE-004, D-014, AR-073)
    # =============================================================

    def on_position_closed(self, symbol: str, exit_reason: str) -> None:
        """
        Called by Exit Controller on every confirmed position close.
        Also called by Long Module for below-minimum emergency sells.

        SC-STATE-004:
          WIN (TP_FILL | TP_PARTIAL_FILL_REMAINDER):
            consecutive_loss_count[symbol] = 0
            cooldown_registry cleared (no re-entry penalty on win)
          LOSS (all other exit reasons):
            consecutive_loss_count[symbol] += 1
            cooldown_registry[symbol] = time.monotonic()

        HR-SC-007: all CIATS params from frozen snapshot in pipeline.
        HR-SC-006: monotonic clock only -- enforced in SignalPipeline.

        Args:
          symbol:      Kraken trading pair (e.g. "BTC/USD")
          exit_reason: exit code string from Exit Controller
        """
        was_win = exit_reason in WIN_EXIT_REASONS
        outcome = "WIN" if was_win else "LOSS"

        self._logger.info(log_record({
            "event":       "SC_POSITION_CLOSED",
            "level":       "INFO",
            "component":   "SELECTION_CTRL",
            "symbol":      symbol,
            "exit_reason": exit_reason,
            "outcome":     outcome,
        }))

        # Delegate to SignalPipeline canonical state.
        # WIN:  cooldown_registry.pop(symbol) + loss_count = 0
        # LOSS: cooldown_registry[symbol] = monotonic() + count += 1
        self._sp.on_trade_closed(symbol, outcome)

    # =============================================================
    # CIATS RESET  (PDCA)
    # =============================================================

    def reset_consecutive_losses(self, symbol: str) -> None:
        """
        Reset consecutive loss count for a symbol.
        Called by CIATS via PDCA -- not on normal close.
        Does NOT clear cooldown (time-based, self-clearing).
        """
        self._sp._consecutive_loss_count[symbol] = 0

        self._logger.info(log_record({
            "event":     "SC_LOSS_COUNT_RESET",
            "level":     "INFO",
            "component": "SELECTION_CTRL",
            "symbol":    symbol,
            "source":    "CIATS_PDCA",
        }))

    def reset_all_consecutive_losses(self) -> None:
        """Reset all symbols consecutive loss counts. Called by CIATS."""
        self._sp._consecutive_loss_count.clear()

        self._logger.info(log_record({
            "event":     "SC_ALL_LOSS_COUNTS_RESET",
            "level":     "INFO",
            "component": "SELECTION_CTRL",
            "source":    "CIATS_PDCA",
        }))

    # =============================================================
    # STATE ACCESSORS -- read-only views for monitoring and CIATS
    # =============================================================

    @property
    def cooldown_registry(self) -> dict[str, float]:
        """
        Read-only view of symbol -> monotonic time of last exit.
        HR-SC-005: preserved across WS reconnect (in SignalPipeline).
        """
        return self._sp._cooldown_registry

    @property
    def consecutive_loss_count(self) -> dict[str, int]:
        """
        Read-only view of symbol -> consecutive loss count.
        HR-SC-005: preserved across WS reconnect (in SignalPipeline).
        """
        return self._sp._consecutive_loss_count

    def get_loss_count(self, symbol: str) -> int:
        """Return consecutive loss count for symbol. 0 if not present."""
        return self._sp._consecutive_loss_count.get(symbol, 0)

    def is_in_cooldown(self, symbol: str, cooldown_seconds: float) -> bool:
        """
        Return True if symbol is currently in cooldown.
        Uses time.monotonic() -- HR-SC-006.
        Useful for monitoring and CIATS reads.
        """
        entry = self._sp._cooldown_registry.get(symbol)
        if entry is None:
            return False
        return (time.monotonic() - entry) < cooldown_seconds

    # =============================================================
    # SNAPSHOT -- for CIATS EWMA Monitor
    # =============================================================

    def state_snapshot(self) -> dict:
        """
        Point-in-time snapshot of SC state for CIATS monitoring.
        Returns cooldown ages (elapsed seconds) and loss counts.
        """
        now = time.monotonic()
        cooldown_ages: dict[str, float] = {
            sym: now - ts
            for sym, ts in self._sp._cooldown_registry.items()
        }
        return {
            "cooldown_ages_s":        cooldown_ages,
            "consecutive_loss_count": dict(self._sp._consecutive_loss_count),
            "symbols_in_cooldown":    len(cooldown_ages),
            "symbols_blocked_streak": sum(
                1 for c in self._sp._consecutive_loss_count.values()
                if c > 0
            ),
        }
