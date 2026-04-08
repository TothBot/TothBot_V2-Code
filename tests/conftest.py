"""
TothBot V2 — Shared Test Fixtures
=============================================================
Test specs: 1021001 Unit_Test_Specification dv1_0
            1021002 Integration_Test_Specification dv1_0
BP standard: 1011001 Engineering_Best_Practices dv1_6
=============================================================

Shared fixtures used across Tier 1 unit tests and
integration tests.

UT-FW-001: pytest
UT-FW-002: pytest-asyncio
UT-FW-003: unittest.mock
UT-FW-004: Do NOT use uvloop in tests (standard asyncio)
"""

import asyncio
import logging
import queue
import tempfile
import os
from decimal import Decimal
from unittest.mock import MagicMock, AsyncMock

import pytest
import pytest_asyncio


# =============================================================
# ASYNCIO CONFIGURATION
# =============================================================

# UT-FW-004: Standard asyncio only. uvloop prohibited in tests.


# =============================================================
# LOGGER FIXTURES
# =============================================================

@pytest.fixture
def log_queue():
    """
    Bounded queue mirroring the production logger queue.
    HR-LG-001: bounded queue size 10_000.
    Used by logger unit tests and any test needing
    a real queue instance.
    """
    return queue.Queue(maxsize=10_000)


@pytest.fixture
def test_logger():
    """
    Plain Python logger with NullHandler.
    No I/O. No file creation. Safe for all unit tests.
    Used by modules that require a logging.Logger instance.
    """
    logger = logging.getLogger(f"tothbot.test.{id(object())}")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.NullHandler())
    logger.propagate = False
    return logger


# =============================================================
# PARAMETER STORE FIXTURE
# =============================================================

@pytest.fixture
def param_store():
    """
    Minimal parameter store dict with CIATS starting values.
    Used by RiskEngine, RegimeEngine, and CIATS unit tests.
    All values are CIATS-owned starting values per 0500000.
    """
    return {
        "tradeable_pct":          Decimal("0.50"),
        "per_trade_pct":          Decimal("0.05"),
        "max_concurrent":         20,
        "mae_mult":               Decimal("1.5"),
        "emergency_sl_mult":      Decimal("3.0"),
        "full_halt_drawdown":     Decimal("0.10"),
        "session_pause_drawdown": Decimal("0.05"),
        "adx_threshold":          Decimal("25"),
        "atr_percentile_thresh":  Decimal("67"),
        "htf_ema_fast":           20,
        "htf_ema_slow":           50,
        "min_volume_usd_daily":   Decimal("500000"),
    }


# =============================================================
# TEMP DIRECTORY FIXTURE
# =============================================================

@pytest.fixture
def tmp_log_dir(tmp_path):
    """
    Temporary directory for logger file output tests.
    Cleaned up automatically by pytest tmp_path fixture.
    """
    log_dir = tmp_path / "logs"
    log_dir.mkdir()
    return str(log_dir)


# =============================================================
# ALERT EMAIL MOCK FIXTURE
# =============================================================

@pytest.fixture
def mock_alert(monkeypatch):
    """
    Monkeypatches _alert_operator_direct to suppress real
    email sends during unit tests.
    Returns the mock so tests can assert call counts.
    """
    mock = MagicMock()
    monkeypatch.setattr(
        "tothbot.logger._alert_operator_direct", mock
    )
    return mock
