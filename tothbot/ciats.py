"""
TothBot V2 — CIATS (Continuous Improvement and Autonomous Trading System)
=============================================================
Coding spec:  1011010 CIATS_Coding_Spec dv1_6
BP standard:  1011001 Engineering_Best_Practices dv1_6
Parent spec:  0211001 CIATS_Specification dv1_0
=============================================================

Self-improvement engine. Reads only from Logger output files.
Writes only to Parameter Store. Never blocks trading hot path.
Deming PDCA governs every change.

Isolation Contract (CIATS-ISO-001 through -003):
  CIATS reads ONLY from Logger output files (log file tail).
  CIATS writes ONLY to Parameter Store.
  No direct calls to TothBot components.
  No in-memory shared state with hot path except Parameter Store dict.
  Parameter Store writes happen ONLY at inter-trade boundaries.
  CIATS runs as asyncio.Task in the same event loop.
  CPU-bound stats delegated to ThreadPoolExecutor.

Two parallel streams:
  Stream 1 — EWMA Monitor (valid at 50 candle evals):
    Three monitoring signals: rejection_rate_by_regime,
    rate_counter_by_pair, ws_roundtrip_latency_ms.
    EWMA lambda=0.2. CUSUM for drift detection.
    MONITORING ONLY — never drives parameter changes directly.
    CUSUM signal → PDCA PLAN phase.

  Stream 2 — Trade Outcome Bus (valid at 200 closed trades):
    Reads TRADE_CLOSE records from Logger.
    Runs Proposal Engine (Spearman correlation).
    Runs Statistical Engine (Mann-Whitney CHECK).
    Runs Half-Kelly at 200+ trades.
    Per-regime analysis at 600+ trades.
    PDCA cycle minimum 50-trade interval.

Statistical thresholds (TB00000 Section 7):
  50 candle evals:  EWMA Monitor valid. MONITORING only.
  200 trades:       HARD FLOOR. Inference valid. Half-Kelly active.
  600 trades:       Per-regime analysis valid (100+ per bucket).

Hard rules:
  200-trade HARD FLOOR for inference and parameter changes.
  PDCA min 50-trade interval between parameter changes.
  Mann-Whitney alpha=0.01 (one-sided).
  Spearman |rho|>0.3 AND p<0.05 required.
  CUSUM k=0.5sigma, h=4sigma (relative to metric sigma).
  All CPU-bound stats in run_in_executor(). Never block event loop.
  Half-Kelly normalized by concurrent position cap.
  Kelly W and R from NET realized P/L — never gross.
  Negative Kelly (K_full <= 0): CRITICAL alert, no update.
  Net 1:1.5 R:R is HARDCODED — CIATS does not touch it. Ever.
"""
from __future__ import annotations

import asyncio
import statistics
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from typing import Any

import orjson

from tothbot.logger import log_record

# =============================================================
# STATISTICAL THRESHOLDS (TB00000 Section 7)
# =============================================================

EWMA_LAMBDA: Decimal = Decimal("0.2")          # CIATS-EW-001
EWMA_FLOOR: int      = 50                       # candle evals before valid
TRADE_FLOOR: int     = 200                      # HARD FLOOR — inference + changes
REGIME_FLOOR: int    = 600                      # per-regime analysis floor
REGIME_BUCKET_MIN: int = 100                    # min trades per regime bucket
PDCA_MIN_INTERVAL: int = 50                     # min trades between changes

MW_ALPHA: float      = 0.01                     # CIATS-MW-001 one-sided
SP_RHO_THRESH: float = 0.3                      # CIATS-SP-001 |rho| threshold
SP_P_THRESH: float   = 0.05                     # CIATS-SP-001 p-value

CUSUM_K_MULT: Decimal = Decimal("0.5")          # CIATS-CU-001
CUSUM_H_MULT: Decimal = Decimal("4.0")          # CIATS-CU-001 (h=4sigma)
CUSUM_SIGMA_MIN: int  = 30                      # min obs for sigma estimate

POLL_INTERVAL_SEC: float = 30.0                 # log file poll interval

# CIATS-owned starting values (from Parameter Store at startup)
TRADEABLE_PCT_DEFAULT: Decimal  = Decimal("0.50")
PER_TRADE_PCT_DEFAULT: Decimal  = Decimal("0.05")
MAX_CONCURRENT_DEFAULT: int     = 20

# Exit reasons that count as wins (for Kelly W computation)
WIN_EXIT_REASONS: frozenset[str] = frozenset({
    "TP_FILL",
    "TP_PARTIAL_FILL_REMAINDER",
})


# =============================================================
# CPU-BOUND STATISTICAL FUNCTIONS (run in ThreadPoolExecutor)
# =============================================================

def _compute_mann_whitney(
    group_a: list[float],
    group_b: list[float],
) -> tuple[float, float]:
    """
    Mann-Whitney U test (one-sided: group_b > group_a).
    Returns (stat, p_value). CIATS-MW-001.
    """
    from scipy.stats import mannwhitneyu
    stat, p = mannwhitneyu(group_b, group_a, alternative="greater")
    return float(stat), float(p)


def _compute_spearman(
    x_vals: list[float],
    y_vals: list[float],
) -> tuple[float, float]:
    """
    Spearman rank correlation.
    Returns (rho, p_value). CIATS-SP-001.
    """
    from scipy.stats import spearmanr
    rho, p = spearmanr(x_vals, y_vals)
    return float(rho), float(p)


# =============================================================
# CIATS PDCA STATE ENUM
# =============================================================

class PDCAPhase:
    IDLE  = "IDLE"
    PLAN  = "PLAN"
    DO    = "DO"
    CHECK = "CHECK"
    ACT   = "ACT"


# =============================================================
# CUSUM STATE
# =============================================================

class CUSUMState:
    """Per-metric CUSUM state. CIATS-CU-001."""

    def __init__(self) -> None:
        self.values: deque[float] = deque(maxlen=200)  # rolling window
        self.S_pos: float = 0.0
        self.S_neg: float = 0.0

    def update(self, new_value: float) -> bool:
        """
        Update CUSUM with new observation. Returns True if drift signal.
        k=0.5sigma, h=4sigma where sigma is rolling std of metric values.
        CIATS-CU-001.
        """
        self.values.append(new_value)
        if len(self.values) < CUSUM_SIGMA_MIN:
            return False  # Insufficient for sigma estimate

        mu    = statistics.mean(self.values)
        sigma = statistics.stdev(self.values)
        if sigma == 0.0:
            return False

        k = 0.5 * sigma
        h = 4.0 * sigma
        x = new_value - mu

        self.S_pos = max(0.0, self.S_pos + x - k)
        self.S_neg = max(0.0, self.S_neg - x - k)

        return self.S_pos > h or self.S_neg > h

    def reset(self) -> None:
        self.S_pos = 0.0
        self.S_neg = 0.0


# =============================================================
# CIATS
# =============================================================

class CIATS:
    """
    TothBot V2 CIATS — Continuous Improvement and Autonomous Trading System.

    Runs as asyncio.Task. Polls Logger output file for new events.
    Two streams:
      Stream 1 — EWMA Monitor (candle evals → monitoring signals)
      Stream 2 — Trade Outcome Bus (TRADE_CLOSE → statistical engine)

    Injected dependencies:
      param_store:    dict — shared Parameter Store (CIATS writes here)
      log_file_path:  str | Path — Logger output file to tail
      logger:         logging.Logger ("tothbot" instance)

    Lifecycle:
      1. Instantiate at startup.
      2. Start with: asyncio.create_task(ciats.run())
      3. Runs indefinitely. Polls log file every POLL_INTERVAL_SEC.
    """

    def __init__(
        self,
        param_store: dict,
        log_file_path: str | Path,
        logger: Any,
    ) -> None:
        self._param_store = param_store
        self._log_file_path = Path(log_file_path)
        self._logger = logger

        # ThreadPoolExecutor for CPU-bound statistical computations.
        # CIATS-ISO-003: run_in_executor() for all CPU-bound stats.
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="ciats")

        # ── Stream 1 — EWMA Monitor ──────────────────────────────────────────
        # EWMA state per metric (CIATS-EW-001)
        self._ewma: dict[str, Decimal] = {}
        self._candle_eval_count: int = 0

        # CUSUM state per metric (CIATS-CU-001)
        self._cusum: dict[str, CUSUMState] = {}

        # ── Stream 2 — Trade Outcome Bus ─────────────────────────────────────
        # Rolling trade corpus: all TRADE_CLOSE records (dicts)
        self._trade_corpus: list[dict] = []

        # Per-regime corpus for 600+ analysis
        self._regime_corpus: dict[str, list[dict]] = {}

        # ── PDCA state ───────────────────────────────────────────────────────
        self._pdca_phase: str = PDCAPhase.IDLE
        self._last_change_trade_idx: int = 0   # corpus index when last change applied
        self._pending_proposal: dict | None = None  # {param, old_val, new_val}
        self._pre_change_corpus_len: int = 0   # corpus length before DO phase

        # ── Log file tail ────────────────────────────────────────────────────
        self._log_file_pos: int = 0             # byte offset — tail from here

    # =============================================================
    # MAIN LOOP
    # =============================================================

    async def run(self) -> None:
        """
        Main CIATS loop. Runs as asyncio.Task.
        Polls Logger file every POLL_INTERVAL_SEC.
        CIATS-ISO-003: asyncio.Task in the same event loop.
        """
        self._logger.info(log_record({
            "event":     "CIATS_STARTED",
            "level":     "INFO",
            "component": "CIATS",
            "note":      "CIATS task started. Polling log file.",
        }))

        while True:
            try:
                await self._poll_log_file()
            except Exception as exc:
                # BP-ERR-001: log before handling. Never crash the CIATS task.
                self._logger.error(log_record({
                    "event":     "CIATS_POLL_ERROR",
                    "level":     "ERROR",
                    "component": "CIATS",
                    "error":     str(exc),
                }))

            await asyncio.sleep(POLL_INTERVAL_SEC)

    # =============================================================
    # LOG FILE TAIL
    # =============================================================

    async def _poll_log_file(self) -> None:
        """
        Read new lines from Logger output file since last position.
        CIATS-ISO-001: reads ONLY from Logger output files.
        """
        if not self._log_file_path.exists():
            return

        loop = asyncio.get_event_loop()

        # Read file in executor to avoid blocking event loop
        new_lines = await loop.run_in_executor(
            self._executor,
            self._read_new_lines,
        )

        for line in new_lines:
            line = line.strip()
            if not line:
                continue
            try:
                record = orjson.loads(line)
            except Exception:
                continue  # Skip malformed lines

            await self._route_log_event(record)

    def _read_new_lines(self) -> list[str]:
        """Read new lines from log file since last byte position."""
        lines: list[str] = []
        try:
            with open(self._log_file_path, "r", encoding="utf-8") as f:
                f.seek(self._log_file_pos)
                lines = f.readlines()
                self._log_file_pos = f.tell()
        except OSError:
            pass
        return lines

    # =============================================================
    # EVENT ROUTING
    # =============================================================

    async def _route_log_event(self, record: dict) -> None:
        """Route log event to Stream 1 or Stream 2."""
        event = record.get("event", "")

        if event == "CANDLE_EVAL":
            await self._stream1_candle_eval(record)

        elif event in ("PIPELINE_GATE_REJECTED", "ENTRY_POST_ONLY_REJECTED"):
            await self._stream1_rejection_event(record)

        elif event == "WS_ROUNDTRIP":
            await self._stream1_roundtrip_event(record)

        elif event == "RATE_COUNTER_UPDATE":
            await self._stream1_rate_counter_event(record)

        elif event == "TRADE_CLOSE":
            await self._stream2_trade_close(record)

    # =============================================================
    # STREAM 1 — EWMA MONITOR  (CIATS-MON-001, CIATS-MON-002)
    # =============================================================

    async def _stream1_candle_eval(self, record: dict) -> None:
        """
        Process CANDLE_EVAL event. Increment count.
        Stream 1 valid from eval 50 onward (EWMA_FLOOR).
        """
        self._candle_eval_count += 1

    async def _stream1_rejection_event(self, record: dict) -> None:
        """
        Track rejection_rate_by_regime (Stream 1 signal 1).
        CIATS-MON-001. MONITORING ONLY.
        """
        if self._candle_eval_count < EWMA_FLOOR:
            return

        regime = record.get("asset_regime", record.get("regime", "UNKNOWN"))
        metric_key = f"rejection_rate_{regime}"
        obs = Decimal("1")  # rejection event = 1
        signal = self._update_ewma_and_cusum(metric_key, obs)

        if signal:
            await self._on_stream1_signal(metric_key, record)

    async def _stream1_roundtrip_event(self, record: dict) -> None:
        """
        Track ws_roundtrip_latency_ms (Stream 1 signal 3).
        CIATS-MON-001. MONITORING ONLY.
        """
        if self._candle_eval_count < EWMA_FLOOR:
            return

        latency_ms = record.get("latency_ms", 0)
        metric_key = "ws_roundtrip_latency_ms"
        signal = self._update_ewma_and_cusum(
            metric_key, Decimal(str(latency_ms))
        )

        if signal:
            await self._on_stream1_signal(metric_key, record)

    async def _stream1_rate_counter_event(self, record: dict) -> None:
        """
        Track rate_counter_by_pair (Stream 1 signal 2).
        CIATS-MON-001. MONITORING ONLY.
        """
        if self._candle_eval_count < EWMA_FLOOR:
            return

        symbol   = record.get("symbol", "UNKNOWN")
        count    = record.get("count", 0)
        metric_key = f"rate_counter_{symbol}"
        signal = self._update_ewma_and_cusum(
            metric_key, Decimal(str(count))
        )

        if signal:
            await self._on_stream1_signal(metric_key, record)

    def _update_ewma_and_cusum(
        self, metric_key: str, obs: Decimal
    ) -> bool:
        """
        Update EWMA for metric, apply CUSUM to EWMA output.
        Returns True if CUSUM signals drift.
        CIATS-EW-001, CIATS-CU-001.
        """
        # EWMA update: ewma_t = lambda * x_t + (1 - lambda) * ewma_{t-1}
        prev_ewma = self._ewma.get(metric_key, obs)
        new_ewma  = EWMA_LAMBDA * obs + (Decimal("1") - EWMA_LAMBDA) * prev_ewma
        self._ewma[metric_key] = new_ewma

        # CUSUM on EWMA output
        if metric_key not in self._cusum:
            self._cusum[metric_key] = CUSUMState()

        return self._cusum[metric_key].update(float(new_ewma))

    async def _on_stream1_signal(
        self, metric_key: str, trigger_record: dict
    ) -> None:
        """
        CUSUM drift signal detected on Stream 1 metric.
        Triggers PDCA PLAN phase. MONITORING ONLY — no automatic change.
        CIATS-CU-002.
        """
        self._logger.warning(log_record({
            "event":      "CIATS_CUSUM_SIGNAL",
            "level":      "WARN",
            "component":  "CIATS",
            "metric":     metric_key,
            "ewma_value": float(self._ewma.get(metric_key, Decimal("0"))),
            "note":       "CUSUM drift detected — initiating PDCA PLAN",
        }))

        # Reset CUSUM after signal (avoid repeated triggering on same drift)
        if metric_key in self._cusum:
            self._cusum[metric_key].reset()

        # PDCA PLAN: queue for investigation at next inter-trade boundary.
        # Stream 1 signals monitoring anomalies — they inform PDCA but
        # do not directly propose parameter values. Log for operator awareness.

    # =============================================================
    # STREAM 2 — TRADE OUTCOME BUS  (CIATS-TOB-001 through -003)
    # =============================================================

    async def _stream2_trade_close(self, record: dict) -> None:
        """
        Process TRADE_CLOSE event. Update trade corpus.
        Run Proposal Engine at 200+ trades.
        Run per-regime analysis at 600+ trades.
        CIATS-TOB-001, CIATS-TOB-002.
        """
        self._trade_corpus.append(record)

        # Update regime corpus
        asset_regime  = record.get("asset_regime", "UNKNOWN")
        market_regime = record.get("market_regime", "UNKNOWN")
        regime_key    = f"{asset_regime}_{market_regime}"

        if regime_key not in self._regime_corpus:
            self._regime_corpus[regime_key] = []
        self._regime_corpus[regime_key].append(record)

        corpus_len = len(self._trade_corpus)

        self._logger.info(log_record({
            "event":         "CIATS_TRADE_RECORDED",
            "level":         "INFO",
            "component":     "CIATS",
            "corpus_size":   corpus_len,
            "symbol":        record.get("symbol", ""),
            "exit_reason":   record.get("exit_reason", ""),
        }))

        # Below HARD FLOOR — no inference or parameter changes
        if corpus_len < TRADE_FLOOR:
            return

        # Half-Kelly update on every trade at 200+ (CIATS-KE-001 through -007)
        await self._update_half_kelly()

        # Per-regime analysis at 600+
        if corpus_len >= REGIME_FLOOR:
            await self._run_regime_analysis()

        # PDCA cycle — run if sufficient interval has elapsed
        trades_since_last = corpus_len - self._last_change_trade_idx
        if trades_since_last >= PDCA_MIN_INTERVAL:
            await self._run_pdca_cycle()

    # =============================================================
    # HALF-KELLY  (CIATS-KE-001 through -007)
    # =============================================================

    async def _update_half_kelly(self) -> None:
        """
        Compute Half-Kelly from net realized P/L in trade corpus.
        Normalize by concurrent position cap.
        Write per_trade_pct to Parameter Store at inter-trade boundary.
        CIATS-KE-001 through CIATS-KE-007.
        """
        corpus = self._trade_corpus

        # CIATS-KE-002: W and R from NET realized P/L — never gross
        winning_trades = [
            t for t in corpus
            if t.get("exit_reason", "") in WIN_EXIT_REASONS
        ]
        losing_trades = [
            t for t in corpus
            if t.get("exit_reason", "") not in WIN_EXIT_REASONS
        ]

        if not corpus:
            return

        total = len(corpus)
        wins  = len(winning_trades)

        W = wins / total  # net winning fraction

        # R = avg net gain / avg |net loss|
        net_gains  = [float(t.get("net_pl_usd", 0)) for t in winning_trades]
        net_losses = [abs(float(t.get("net_pl_usd", 0))) for t in losing_trades]

        if not net_gains or not net_losses:
            return  # Insufficient data for R

        avg_win  = statistics.mean(net_gains)
        avg_loss = statistics.mean(net_losses)

        if avg_loss == 0.0:
            return  # Degenerate case

        R = avg_win / avg_loss

        # CIATS-KE-001: K_full = W - ((1-W) / R)
        K_full = W - ((1.0 - W) / R)

        # CIATS-KE-006: Negative Kelly → CRITICAL alert, no update
        if K_full <= 0.0:
            self._logger.critical(log_record({
                "event":     "KELLY_NEGATIVE",
                "level":     "CRITICAL",
                "component": "CIATS",
                "K_full":    K_full,
                "W":         W,
                "R":         R,
                "corpus_n":  total,
                "note":      "Kelly <= 0 — per_trade_pct NOT updated",
            }))
            return

        K_half = K_full * 0.5

        # CIATS-KE-003: Normalize by concurrent position cap
        tradeable_pct = float(
            self._param_store.get("tradeable_pct", TRADEABLE_PCT_DEFAULT)
        )
        max_concurrent = int(
            self._param_store.get("max_concurrent", MAX_CONCURRENT_DEFAULT)
        )
        max_per_trade = tradeable_pct / max_concurrent
        applied_pct   = min(K_half, max_per_trade)

        # CIATS-KE-005: Floor — minimum viable trade size (5% as floor)
        min_per_trade = float(PER_TRADE_PCT_DEFAULT) * 0.5  # 2.5% floor
        applied_pct   = max(applied_pct, min_per_trade)

        binding = applied_pct < K_half  # normalization was binding

        # CIATS-KE-009: Log all Kelly update details
        self._logger.info(log_record({
            "event":             "CIATS_KELLY_UPDATE",
            "level":             "INFO",
            "component":         "CIATS",
            "W":                 round(W, 4),
            "R":                 round(R, 4),
            "K_full":            round(K_full, 4),
            "K_half":            round(K_half, 4),
            "applied_pct":       round(applied_pct, 4),
            "normalization_binding": binding,
            "corpus_n":          total,
        }))

        # CIATS-ISO-002: write to Parameter Store (inter-trade boundary)
        self._param_store["per_trade_pct"] = str(round(applied_pct, 6))

    # =============================================================
    # PDCA CYCLE  (CIATS-TOB-003)
    # =============================================================

    async def _run_pdca_cycle(self) -> None:
        """
        PDCA cycle. Minimum 50-trade interval. CIATS-TOB-003.
        PLAN → DO → CHECK → ACT.

        Current implementation:
          PLAN: Spearman correlation between mae_mult and net P/L.
          DO:   Write proposed mae_mult to Parameter Store.
          CHECK: Mann-Whitney U (pre vs post, alpha=0.01).
          ACT:  Keep or revert.

        Note: Full multi-parameter PDCA is incrementally built out
        as trade corpus grows. Initial implementation covers mae_mult
        as the primary loss-prevention parameter.
        """
        if self._pdca_phase == PDCAPhase.IDLE:
            await self._pdca_plan()

        elif self._pdca_phase == PDCAPhase.DO:
            # DO phase already applied — advance to CHECK
            await self._pdca_check()

        elif self._pdca_phase == PDCAPhase.CHECK:
            await self._pdca_act()

    async def _pdca_plan(self) -> None:
        """
        PDCA PLAN: Check Spearman correlation between a parameter
        value and trade P/L. If confirmed, propose change.
        CIATS-SP-001: |rho| > 0.3 AND p < 0.05.
        """
        corpus = self._trade_corpus
        if len(corpus) < TRADE_FLOOR:
            return

        # Extract mae_mult history and net_pl_usd for Spearman
        # In early corpus, mae_mult is constant (starting value)
        # Spearman requires variance — skip if no variance
        pl_values  = [float(t.get("net_pl_usd", 0)) for t in corpus]
        mae_values = [
            float(t.get("mae_mult_at_entry",
                  self._param_store.get("mae_mult", "1.5")))
            for t in corpus
        ]

        if len(set(mae_values)) < 2:
            # No variance in mae_mult yet — Spearman not meaningful
            return

        loop = asyncio.get_event_loop()
        try:
            rho, p = await loop.run_in_executor(
                self._executor,
                _compute_spearman,
                mae_values,
                pl_values,
            )
        except Exception as exc:
            self._logger.error(log_record({
                "event":     "CIATS_SPEARMAN_ERROR",
                "level":     "ERROR",
                "component": "CIATS",
                "error":     str(exc),
            }))
            return

        self._logger.info(log_record({
            "event":     "CIATS_SPEARMAN_RESULT",
            "level":     "INFO",
            "component": "CIATS",
            "parameter": "mae_mult",
            "rho":       round(rho, 4),
            "p_value":   round(p, 4),
            "threshold_rho": SP_RHO_THRESH,
            "threshold_p":   SP_P_THRESH,
        }))

        if abs(rho) > SP_RHO_THRESH and p < SP_P_THRESH:
            # Correlation confirmed — generate proposal
            # If negative rho: lower mae_mult improves P/L → propose reduction
            # If positive rho: higher mae_mult improves P/L → propose increase
            current_mae = float(
                self._param_store.get("mae_mult", "1.5")
            )
            # Conservative 10% adjustment
            if rho < 0:
                proposed = round(current_mae * 0.90, 2)
            else:
                proposed = round(current_mae * 1.10, 2)

            # Cap within reasonable bounds (0.5x to 3.0x ATR)
            proposed = max(0.5, min(proposed, 3.0))

            self._pending_proposal = {
                "param":   "mae_mult",
                "old_val": str(current_mae),
                "new_val": str(proposed),
            }
            self._pre_change_corpus_len = len(corpus)
            self._pdca_phase = PDCAPhase.DO

            self._logger.info(log_record({
                "event":     "CIATS_PDCA_PROPOSAL",
                "level":     "INFO",
                "component": "CIATS",
                "parameter": "mae_mult",
                "old_val":   str(current_mae),
                "new_val":   str(proposed),
                "rho":       round(rho, 4),
                "corpus_n":  len(corpus),
            }))

            # CIATS-ISO-002: apply at inter-trade boundary
            self._param_store["mae_mult"] = str(proposed)
            self._last_change_trade_idx = len(corpus)

    async def _pdca_check(self) -> None:
        """
        PDCA CHECK: Mann-Whitney U on pre vs post change performance.
        alpha=0.01, one-sided. CIATS-MW-001.
        """
        if self._pending_proposal is None:
            self._pdca_phase = PDCAPhase.IDLE
            return

        corpus   = self._trade_corpus
        pre_len  = self._pre_change_corpus_len
        post_len = len(corpus)

        # Minimum 50 trades post-change for CHECK
        if (post_len - pre_len) < PDCA_MIN_INTERVAL:
            return

        pre_group  = [float(t.get("net_pl_usd", 0)) for t in corpus[:pre_len]]
        post_group = [float(t.get("net_pl_usd", 0)) for t in corpus[pre_len:]]

        if len(pre_group) < 20 or len(post_group) < 20:
            return

        loop = asyncio.get_event_loop()
        try:
            stat, p = await loop.run_in_executor(
                self._executor,
                _compute_mann_whitney,
                pre_group,
                post_group,
            )
        except Exception as exc:
            self._logger.error(log_record({
                "event":     "CIATS_MW_ERROR",
                "level":     "ERROR",
                "component": "CIATS",
                "error":     str(exc),
            }))
            self._pdca_phase = PDCAPhase.IDLE
            return

        self._logger.info(log_record({
            "event":     "CIATS_MW_RESULT",
            "level":     "INFO",
            "component": "CIATS",
            "parameter": self._pending_proposal.get("param", ""),
            "stat":      round(stat, 4),
            "p_value":   round(p, 4),
            "alpha":     MW_ALPHA,
            "pre_n":     len(pre_group),
            "post_n":    len(post_group),
        }))

        self._pdca_phase = PDCAPhase.ACT
        self._pending_proposal["mw_stat"] = stat
        self._pending_proposal["mw_p"]    = p

    async def _pdca_act(self) -> None:
        """
        PDCA ACT: keep or revert based on Mann-Whitney CHECK result.
        CIATS-TOB-003.
        """
        if self._pending_proposal is None:
            self._pdca_phase = PDCAPhase.IDLE
            return

        p      = self._pending_proposal.get("mw_p", 1.0)
        param  = self._pending_proposal.get("param", "")
        new_val = self._pending_proposal.get("new_val", "")
        old_val = self._pending_proposal.get("old_val", "")

        if p < MW_ALPHA:
            # CHECK passed — keep change
            self._logger.info(log_record({
                "event":     "CIATS_PDCA_ACT_KEEP",
                "level":     "INFO",
                "component": "CIATS",
                "parameter": param,
                "value":     new_val,
                "p_value":   round(p, 4),
                "result":    "KEPT",
            }))
        else:
            # CHECK failed — revert
            self._param_store[param] = old_val
            self._logger.info(log_record({
                "event":     "CIATS_PDCA_ACT_REVERT",
                "level":     "INFO",
                "component": "CIATS",
                "parameter": param,
                "reverted_to": old_val,
                "p_value":   round(p, 4),
                "result":    "REVERTED",
            }))

        self._pending_proposal   = None
        self._pdca_phase         = PDCAPhase.IDLE
        self._last_change_trade_idx = len(self._trade_corpus)

    # =============================================================
    # PER-REGIME ANALYSIS  (CIATS-TOB-002(c), 600-trade floor)
    # =============================================================

    async def _run_regime_analysis(self) -> None:
        """
        Per-regime P/L analysis at 600+ trades.
        Requires 100+ trades per regime bucket. CIATS-ST-003.
        """
        for regime_key, trades in self._regime_corpus.items():
            if len(trades) < REGIME_BUCKET_MIN:
                continue

            pl_values = [float(t.get("net_pl_usd", 0)) for t in trades]
            avg_pl    = statistics.mean(pl_values)
            sharpe    = self._compute_sharpe(pl_values)

            self._logger.info(log_record({
                "event":       "CIATS_REGIME_ANALYSIS",
                "level":       "INFO",
                "component":   "CIATS",
                "regime":      regime_key,
                "n_trades":    len(trades),
                "avg_net_pl":  round(avg_pl, 4),
                "sharpe_ratio": round(sharpe, 4),
            }))

    def _compute_sharpe(self, pl_values: list[float]) -> float:
        """Sharpe ratio from net P/L series. Returns 0.0 if insufficient data."""
        if len(pl_values) < 2:
            return 0.0
        mu    = statistics.mean(pl_values)
        sigma = statistics.stdev(pl_values)
        if sigma == 0.0:
            return 0.0
        return mu / sigma

    # =============================================================
    # EXTERNAL INTERFACE — on_trade_close
    # =============================================================

    async def on_trade_close(self, trade_record: dict) -> None:
        """
        Direct injection point for TRADE_CLOSE events.
        Called by Exit Controller when a position closes.
        Supplements log file polling for immediate processing.

        The log file remains the authoritative source.
        This call provides same-cycle processing for Half-Kelly.
        CIATS-TOB-001.
        """
        await self._stream2_trade_close(trade_record)

    # =============================================================
    # SHUTDOWN
    # =============================================================

    def shutdown(self) -> None:
        """Clean shutdown of ThreadPoolExecutor."""
        self._executor.shutdown(wait=False)
        self._logger.info(log_record({
            "event":     "CIATS_SHUTDOWN",
            "level":     "INFO",
            "component": "CIATS",
        }))
