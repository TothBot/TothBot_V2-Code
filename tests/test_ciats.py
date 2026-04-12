"""
DocDCN:     1021001
DocTitle:   CIATS_Unit_Tests
DocVersion: dv1_0
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/tests/test_ciats.py
DocDate:    04-12-2026
DocTime:    23:59:59 UTC

============================================================
REVISION HISTORY
============================================================

  dv1_0   04-12-2026  DC header added per 0311001 v1_1,
                      0311004 v1_1, 1011001 dv1_7.
                      Unit tests UT-CI-001 through
                      UT-CI-008. Module: tothbot/ciats.py.
                      Governed by 1021001
                      Unit_Test_Specification dv1_0.

============================================================

TothBot V2 — Unit Tests: CIATS
=============================================================
Test spec:   1021001 Unit_Test_Specification dv1_0 (UT-CI)
Module:      tothbot/ciats.py
Coding spec: 1011010 CIATS_Coding_Spec dv1_6
BP standard: 1011001 Engineering_Best_Practices dv1_7
=============================================================

Tests: UT-CI-001 through UT-CI-008

Tests cover: EWMA computation, CUSUM drift detection,
half-Kelly activation floor, negative Kelly guard,
isolation contract (param_store writes only),
PDCA minimum interval, 200-trade hard floor,
and Stream 1 monitoring-only contract.

UT-FW-004: Standard asyncio. Do NOT use uvloop.
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tothbot.ciats import (
    CIATS,
    EWMA_FLOOR,
    EWMA_LAMBDA,
    PDCA_MIN_INTERVAL,
    TRADE_FLOOR,
    CUSUMState,
    WIN_EXIT_REASONS,
)
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
def param_store():
    return {
        "tradeable_pct":  Decimal("0.50"),
        "per_trade_pct":  Decimal("0.05"),
        "max_concurrent": 20,
        "mae_mult":       "1.5",
    }


@pytest.fixture
def tmp_log_file(tmp_path):
    """Temporary log file for CIATS to tail."""
    log_file = tmp_path / "tothbot.log"
    log_file.touch()
    return log_file


@pytest.fixture
def ciats(param_store, tmp_log_file, logger):
    return CIATS(
        param_store=param_store,
        log_file_path=tmp_log_file,
        logger=logger,
    )


def _make_trade_record(exit_reason: str = "TP_FILL", net_pl: float = 1.5) -> dict:
    """Generate a synthetic TRADE_CLOSE log record."""
    return {
        "event":            "TRADE_CLOSE",
        "level":            "INFO",
        "component":        "EXIT_CTRL",
        "symbol":           "BTC/USD",
        "exit_reason":      exit_reason,
        "net_pl_usd":       str(net_pl),
        "net_pnl_usd":      str(net_pl),
        "asset_regime":     "TRENDING_POSITIVE",
        "market_regime":    "TRENDING_POSITIVE",
        "vol_regime":       "NORMAL_VOL",
        "hold_candle_count": 3,
        "actual_rr":        "1.5",
    }


# =============================================================
# UT-CI-001: EWMA formula — lambda=0.2
# CIATS-EW-001
# =============================================================

class TestEWMA:

    def test_UT_CI_001_ewma_formula(self, ciats):
        """
        UT-CI-001: CIATS-EW-001 — EWMA formula:
        ewma_t = lambda * x_t + (1 - lambda) * ewma_{t-1}
        lambda = 0.2. First observation: ewma_0 = x_0.
        """
        # First observation: prev_ewma == obs (initial condition)
        obs1 = Decimal("100")
        ciats._update_ewma_and_cusum("test_metric", obs1)
        ewma1 = ciats._ewma["test_metric"]
        # First call: prev = obs (get default = obs), so ewma = 0.2*100 + 0.8*100 = 100
        assert ewma1 == Decimal("100"), (
            f"CIATS-EW-001: First EWMA must equal first observation. "
            f"Got {ewma1}"
        )

        # Second observation: x=50
        # ewma = 0.2 * 50 + 0.8 * 100 = 10 + 80 = 90
        obs2 = Decimal("50")
        ciats._update_ewma_and_cusum("test_metric", obs2)
        ewma2 = ciats._ewma["test_metric"]
        expected = EWMA_LAMBDA * obs2 + (Decimal("1") - EWMA_LAMBDA) * ewma1
        assert ewma2 == expected, (
            f"CIATS-EW-001: EWMA formula incorrect. "
            f"Expected {expected}, got {ewma2}"
        )

    def test_UT_CI_001_ewma_lambda_value(self):
        """
        UT-CI-001: CIATS-EW-001 — EWMA_LAMBDA must be exactly 0.2.
        """
        assert EWMA_LAMBDA == Decimal("0.2"), (
            f"CIATS-EW-001: EWMA lambda must be 0.2, got {EWMA_LAMBDA}"
        )

    def test_UT_CI_001_ewma_independent_per_metric(self, ciats):
        """
        UT-CI-001: EWMA state is independent per metric key.
        One metric's values must not affect another.
        """
        ciats._update_ewma_and_cusum("metric_a", Decimal("100"))
        ciats._update_ewma_and_cusum("metric_b", Decimal("200"))

        assert "metric_a" in ciats._ewma
        assert "metric_b" in ciats._ewma
        assert ciats._ewma["metric_a"] != ciats._ewma["metric_b"]


# =============================================================
# UT-CI-002: CUSUM drift detection
# CIATS-CU-001
# =============================================================

class TestCUSUM:

    def test_UT_CI_002_cusum_no_signal_stable_series(self):
        """
        UT-CI-002: CIATS-CU-001 — CUSUM must not signal for a
        stable (zero-variance) series. k=0.5sigma, h=4sigma.
        With sigma=0 there is no drift to detect.
        """
        cs = CUSUMState()
        # All same value — no drift
        for _ in range(100):
            signal = cs.update(50.0)
        # Stable series → no signal (sigma=0 → CUSUM returns False)
        assert signal is False, (
            "CIATS-CU-001: CUSUM must not signal on stable series"
        )

    def test_UT_CI_002_cusum_signals_on_drift(self):
        """
        UT-CI-002: CIATS-CU-001 — CUSUM must signal when a strong
        positive drift is introduced (mean shift upward).
        """
        cs = CUSUMState()

        # Stable baseline (30+ observations to estimate sigma)
        for _ in range(35):
            cs.update(50.0)

        # Large upward shift — should trigger CUSUM
        triggered = False
        for _ in range(20):
            if cs.update(150.0):  # strong drift
                triggered = True
                break

        assert triggered, (
            "CIATS-CU-001: CUSUM must signal on sustained upward drift"
        )

    def test_UT_CI_002_cusum_reset_clears_state(self):
        """
        UT-CI-002: CUSUMState.reset() must zero S_pos and S_neg.
        """
        cs = CUSUMState()
        for _ in range(35):
            cs.update(50.0)
        for _ in range(10):
            cs.update(150.0)

        cs.reset()

        assert cs.S_pos == 0.0, f"CUSUM S_pos must be 0 after reset, got {cs.S_pos}"
        assert cs.S_neg == 0.0, f"CUSUM S_neg must be 0 after reset, got {cs.S_neg}"

    def test_UT_CI_002_cusum_insufficient_for_sigma(self):
        """
        UT-CI-002: CUSUM requires CUSUM_SIGMA_MIN=30 observations
        to estimate sigma. Below that, returns False (no signal).
        """
        cs = CUSUMState()
        # Only 5 observations — below CUSUM_SIGMA_MIN
        for i in range(5):
            result = cs.update(float(i * 100))  # large variance
        assert result is False, (
            "CIATS-CU-001: CUSUM must return False below SIGMA_MIN observations"
        )


# =============================================================
# UT-CI-003: 200-trade HARD FLOOR — no inference below it
# CIATS-TOB-001 / TB00000 §7
# =============================================================

class TestTradeFloor:

    @pytest.mark.asyncio
    async def test_UT_CI_003_no_kelly_below_200_trades(self, ciats):
        """
        UT-CI-003: CIATS-TOB-001 — no per_trade_pct changes before
        200 closed trades. The 200-trade floor is the HARD FLOOR for
        all statistical inference and parameter changes.
        """
        initial_per_trade = ciats._param_store.get("per_trade_pct")

        # Inject 199 trade records — one below the floor
        for i in range(199):
            await ciats._stream2_trade_close(_make_trade_record())

        # per_trade_pct must NOT have been updated
        assert ciats._param_store.get("per_trade_pct") == initial_per_trade, (
            f"CIATS-TOB-001: per_trade_pct must not change before "
            f"200 closed trades. Changed at {len(ciats._trade_corpus)} trades."
        )

    @pytest.mark.asyncio
    async def test_UT_CI_003_trade_floor_constant(self):
        """
        UT-CI-003: TRADE_FLOOR must be exactly 200. TB00000 §7.
        This is the project-level hard floor. Never change.
        """
        assert TRADE_FLOOR == 200, (
            f"TB00000 §7: TRADE_FLOOR must be 200, got {TRADE_FLOOR}"
        )


# =============================================================
# UT-CI-004: Negative Kelly → no update + CRITICAL log
# CIATS-KE-006
# =============================================================

class TestNegativeKelly:

    @pytest.mark.asyncio
    async def test_UT_CI_004_negative_kelly_no_update(self, ciats):
        """
        UT-CI-004: CIATS-KE-006 — if K_full <= 0, per_trade_pct
        must NOT be updated. CRITICAL log emitted.
        W=0.1 (10% win rate), R=0.5 → K_full = 0.1 - 0.9/0.5
        = 0.1 - 1.8 = -1.7 < 0.
        """
        initial_val = ciats._param_store.get("per_trade_pct")

        # Build corpus with 10% win rate and very poor R:R
        win_count  = 20   # 20 wins
        loss_count = 180  # 180 losses → W = 0.1

        for _ in range(win_count):
            ciats._trade_corpus.append(_make_trade_record("TP_FILL", 0.5))
        for _ in range(loss_count):
            ciats._trade_corpus.append(_make_trade_record("MAE_THRESHOLD_BREACH", -2.0))

        await ciats._update_half_kelly()

        # per_trade_pct must be unchanged — negative Kelly
        assert ciats._param_store.get("per_trade_pct") == initial_val, (
            "CIATS-KE-006: Negative Kelly must NOT update per_trade_pct"
        )


# =============================================================
# UT-CI-005: Isolation contract — writes only to param_store
# CIATS-ISO-001 / CIATS-ISO-002
# =============================================================

class TestIsolationContract:

    @pytest.mark.asyncio
    async def test_UT_CI_005_ciats_writes_only_param_store(self, ciats):
        """
        UT-CI-005: CIATS-ISO-002 — CIATS writes ONLY to param_store.
        It must not write to any other shared state or component.
        Verify that _update_half_kelly only modifies _param_store.
        """
        # Build a positive Kelly corpus
        for _ in range(120):
            ciats._trade_corpus.append(_make_trade_record("TP_FILL", 2.0))
        for _ in range(80):
            ciats._trade_corpus.append(_make_trade_record("MAE_THRESHOLD_BREACH", -1.0))

        # Snapshot all attributes that must NOT change
        initial_corpus_len  = len(ciats._trade_corpus)
        initial_ewma        = dict(ciats._ewma)
        initial_cusum_keys  = set(ciats._cusum.keys())

        await ciats._update_half_kelly()

        # Corpus must not change (CIATS reads only)
        assert len(ciats._trade_corpus) == initial_corpus_len

        # EWMA and CUSUM must not change from half-kelly update
        assert dict(ciats._ewma) == initial_ewma
        assert set(ciats._cusum.keys()) == initial_cusum_keys

    def test_UT_CI_005_param_store_is_shared_reference(self, param_store, tmp_log_file, logger):
        """
        UT-CI-005: CIATS-ISO-002 — CIATS writes to the shared
        param_store dict. Changes must be visible to caller via
        the same dict reference (no copy).
        """
        ciats = CIATS(
            param_store=param_store,
            log_file_path=tmp_log_file,
            logger=logger,
        )
        # Directly set a value in param_store via CIATS internal
        ciats._param_store["test_key"] = "test_value"
        # Must be visible in the original param_store dict
        assert param_store.get("test_key") == "test_value", (
            "CIATS-ISO-002: param_store must be a shared reference"
        )


# =============================================================
# UT-CI-006: PDCA minimum 50-trade interval
# CIATS-TOB-003
# =============================================================

class TestPDCAInterval:

    @pytest.mark.asyncio
    async def test_UT_CI_006_pdca_min_interval(self):
        """
        UT-CI-006: CIATS-TOB-003 — PDCA_MIN_INTERVAL must be 50.
        TB00000 §7.
        """
        assert PDCA_MIN_INTERVAL == 50, (
            f"CIATS-TOB-003: PDCA_MIN_INTERVAL must be 50, "
            f"got {PDCA_MIN_INTERVAL}"
        )


# =============================================================
# UT-CI-007: EWMA_FLOOR = 50 candle evals before Stream 1 valid
# CIATS-MON-001
# =============================================================

class TestEWMAFloor:

    @pytest.mark.asyncio
    async def test_UT_CI_007_stream1_inactive_below_floor(self, ciats):
        """
        UT-CI-007: CIATS-MON-001 — Stream 1 rejection events must
        be ignored until 50 candle evals have been processed.
        """
        assert EWMA_FLOOR == 50, (
            f"CIATS-MON-001: EWMA_FLOOR must be 50, got {EWMA_FLOOR}"
        )

        # Below floor: _candle_eval_count = 0
        initial_ewma_keys = set(ciats._ewma.keys())

        rejection_record = {
            "event":       "PIPELINE_GATE_REJECTED",
            "level":       "INFO",
            "component":   "SIGNAL_PIPELINE",
            "asset_regime": "TRENDING_POSITIVE",
        }
        await ciats._stream1_rejection_event(rejection_record)

        # EWMA must not be updated (below floor)
        assert set(ciats._ewma.keys()) == initial_ewma_keys, (
            "CIATS-MON-001: Stream 1 must not process rejections "
            "before 50 candle evals"
        )

    @pytest.mark.asyncio
    async def test_UT_CI_007_stream1_active_at_floor(self, ciats):
        """
        UT-CI-007: At exactly EWMA_FLOOR candle evals, Stream 1
        becomes active and processes rejection events.
        """
        # Advance to floor
        ciats._candle_eval_count = EWMA_FLOOR

        rejection_record = {
            "event":       "PIPELINE_GATE_REJECTED",
            "level":       "INFO",
            "component":   "SIGNAL_PIPELINE",
            "asset_regime": "TRENDING_POSITIVE",
        }
        await ciats._stream1_rejection_event(rejection_record)

        # EWMA must have been updated
        assert len(ciats._ewma) > 0, (
            "CIATS-MON-001: Stream 1 must process rejections at EWMA_FLOOR"
        )


# =============================================================
# UT-CI-008: WIN_EXIT_REASONS — Kelly W/R classification
# CIATS-KE-002
# =============================================================

class TestWinExitReasons:

    def test_UT_CI_008_win_exit_reasons_defined(self):
        """
        UT-CI-008: CIATS-KE-002 — W (win rate) uses NET P/L.
        Only TP_FILL and TP_PARTIAL_FILL_REMAINDER are wins.
        All other exits (MAE, time, regime) are losses.
        """
        assert "TP_FILL" in WIN_EXIT_REASONS, (
            "CIATS-KE-002: TP_FILL must be a win exit reason"
        )
        assert "TP_PARTIAL_FILL_REMAINDER" in WIN_EXIT_REASONS, (
            "CIATS-KE-002: TP_PARTIAL_FILL_REMAINDER must be a win exit reason"
        )

    def test_UT_CI_008_loss_exits_not_in_win_reasons(self):
        """
        UT-CI-008: Loss exits must NOT be classified as wins.
        """
        loss_exits = [
            "MAE_THRESHOLD_BREACH",
            "TIME_EXPIRY",
            "HTF_REGIME_REVERSAL",
            "DAILY_REGIME_DOWNGRADE",
            "SIGNAL_DECAY",
            "MOMENTUM_LOSS",
        ]
        for exit_reason in loss_exits:
            assert exit_reason not in WIN_EXIT_REASONS, (
                f"CIATS-KE-002: {exit_reason} must NOT be a win "
                f"exit reason. Only TP exits are wins."
            )
