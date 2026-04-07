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
from __future__ import annotations

import asyncio
import logging
import queue
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


# =============================================================
# EVENT LOOP — UT-FW-004: standard asyncio, NOT uvloop
# =============================================================

# pytest-asyncio uses standard asyncio event loop by default.
# No uvloop import here. uvloop is production-only (SS-PRE-005).


# =============================================================
# LOGGER FIXTURE
# =============================================================

@pytest.fixture
def tothbot_logger():
    """
    Minimal logger for injecting into components under test.
    Routes to /dev/null — tests inspect side effects,
    not log output.
    """
    logger = logging.getLogger("tothbot.test")
    logger.handlers = [logging.NullHandler()]
    logger.propagate = False
    return logger


# =============================================================
# PARAMETER STORE FIXTURE — CIATS starting values
# =============================================================

@pytest.fixture
def param_store():
    """
    CIATS Parameter Store with canonical starting values.
    Per TB00000 §9.17 and 1011010 dv1_6.
    """
    return {
        "tradeable_pct":           Decimal("0.50"),
        "per_trade_pct":           Decimal("0.05"),
        "max_concurrent":          20,
        "mae_mult":                Decimal("1.5"),
        "emergency_sl_mult":       Decimal("3.0"),
        "cancel_timeout":          5.0,
        "mpp_retry_count":         3,
        "max_hold_candles":        24,
        "entry_timeout_sec":       45,
        "full_halt_drawdown":      Decimal("0.10"),
        "session_pause_drawdown":  Decimal("0.05"),
        "adx_threshold":           Decimal("25"),
        "atr_percentile_thresh":   67,
        "htf_ema_fast":            20,
        "htf_ema_slow":            50,
        "min_volume_usd_daily":    Decimal("500000"),
        "sc_body_threshold":       Decimal("0.3"),
        "sc_cooldown_seconds":     300,
        "sc_consecutive_limit":    3,
        "half_kelly_active":       False,
        "kelly_win_rate":          None,
        "kelly_avg_rr":            None,
    }


# =============================================================
# MOCK WS MANAGER — used by RiskEngine, ExitController
# =============================================================

@pytest.fixture
def mock_ws_manager():
    """
    Mock WSManager with all attributes consumed by RiskEngine
    and other components. Tests set specific values per scenario.
    """
    wm = MagicMock()
    wm.spot_usd_balance   = Decimal("10000")
    wm.pending_orders     = {}
    wm.latest_bid         = {}
    wm.batch_cancel       = AsyncMock()
    wm.engine_state       = "online"
    wm.system_state       = "NORMAL"
    wm.pair_cache         = {}
    wm.atr_14             = {}
    wm.warm_up_state      = {}
    return wm


# =============================================================
# MOCK POSITION MIRROR — used by RiskEngine, SignalPipeline
# =============================================================

@pytest.fixture
def mock_position_mirror():
    """
    Mock PositionMirror with all attributes consumed by
    RiskEngine and other components.
    """
    pm = MagicMock()
    pm.all_records  = {}    # empty — no open positions by default
    pm.open_count   = 0
    return pm


# =============================================================
# PAIR SPEC FIXTURE — Kraken instrument data
# =============================================================

@pytest.fixture
def btc_usd_pair_spec():
    """
    Realistic BTC/USD pair specification from Kraken instrument channel.
    Values match production precision requirements.
    """
    return {
        "price_increment": Decimal("0.1"),
        "qty_increment":   Decimal("0.0001"),
        "qty_min":         Decimal("0.0001"),
        "cost_min":        Decimal("0.5"),
    }
