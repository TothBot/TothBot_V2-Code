"""
DocDCN:     1021001
DocTitle:   VPS_Deployment_Unit_Tests
DocVersion: dv1_0
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/tests/test_vps_deployment.py
DocDate:    04-12-2026
DocTime:    23:59:59 UTC

============================================================
REVISION HISTORY
============================================================

  dv1_0   04-12-2026  DC header added per 0311001 v1_1,
                      0311004 v1_1, 1011001 dv1_7.
                      Unit tests UT-VD-001 through
                      UT-VD-005. Module:
                      tothbot/vps_deployment.py.
                      Governed by 1021001
                      Unit_Test_Specification dv1_0.

============================================================

TothBot V2 — Unit Tests: VPS Deployment
=============================================================
Test spec:   1021001 Unit_Test_Specification dv1_0 (UT-VD)
Module:      tothbot/vps_deployment.py
Coding spec: 1011013 VPS_Deployment_Coding_Spec dv1_4
BP standard: 1011001 Engineering_Best_Practices dv1_7
=============================================================

Tests: UT-VD-001 through UT-VD-005

UT-FW-004: Standard asyncio. Do NOT use uvloop.
"""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
import pytest

from tothbot.vps_deployment import (
    ALERT_RATE_LIMIT_SEC,
    MAINTENANCE_ALERT_WINDOW_SEC,
    REQUIRED_ENV_VARS,
    SYSTEMD_SERVICE_UNIT,
    WATCHDOG_PING_INTERVAL,
    _alert_last_sent,
    _should_alert,
    check_kraken_status,
    validate_environment,
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


@pytest.fixture(autouse=True)
def clear_alert_rate_limiter():
    _alert_last_sent.clear()
    yield
    _alert_last_sent.clear()


def _make_aiohttp_mock(status: int = 200, body: bytes = b"{}"):
    """
    Build a properly nested aiohttp mock for:
      async with aiohttp.ClientSession() as session:
        async with session.get(URL) as resp:
    """
    # Inner context manager: async with session.get(URL) as resp
    mock_resp = MagicMock()
    mock_resp.status = status
    mock_resp.read = AsyncMock(return_value=body)

    resp_ctx = MagicMock()
    resp_ctx.__aenter__ = AsyncMock(return_value=mock_resp)
    resp_ctx.__aexit__ = AsyncMock(return_value=False)

    # session.get(URL) returns resp_ctx (sync call)
    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=resp_ctx)

    # Outer context manager: async with aiohttp.ClientSession() as session
    session_ctx = MagicMock()
    session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
    session_ctx.__aexit__ = AsyncMock(return_value=False)

    return session_ctx


# =============================================================
# UT-VD-001: check_kraken_status is non-blocking
# VD-STAT-001 / VD-STAT-002 / VD-STAT-003
# =============================================================

class TestKrakenStatusCheck:

    @pytest.mark.asyncio
    async def test_UT_VD_001_status_api_unreachable_continues(self, logger):
        """
        UT-VD-001: VD-STAT-001 — API unreachable must not raise.
        Startup always continues.
        """
        with patch("tothbot.vps_deployment.aiohttp.ClientSession",
                   side_effect=Exception("Connection refused")):
            await check_kraken_status(logger)  # must not raise

    @pytest.mark.asyncio
    async def test_UT_VD_001_non_200_response_continues(self, logger):
        """
        UT-VD-001: VD-STAT-001 — non-200 HTTP response must not raise.
        """
        session_ctx = _make_aiohttp_mock(status=503)
        with patch("tothbot.vps_deployment.aiohttp.ClientSession",
                   return_value=session_ctx):
            await check_kraken_status(logger)  # must not raise

    @pytest.mark.asyncio
    async def test_UT_VD_001_no_maintenance_clean_status(self, logger):
        """
        UT-VD-001: VD-STAT-002 — clean status (no maintenance) must
        complete without raising.
        """
        body = orjson.dumps({"scheduled_maintenances": []})
        session_ctx = _make_aiohttp_mock(status=200, body=body)
        with patch("tothbot.vps_deployment.aiohttp.ClientSession",
                   return_value=session_ctx):
            await check_kraken_status(logger)  # must not raise

    @pytest.mark.asyncio
    async def test_UT_VD_001_maintenance_within_2h_creates_alert_task(self, logger):
        """
        UT-VD-001: VD-STAT-003 — maintenance within 2 hours triggers
        asyncio.create_task(send_alert(...)). Startup still continues.

        Strategy: provide a maintenance entry with scheduled_for within
        2 hours. Patch asyncio.create_task to capture the call.
        """
        start_time = (
            datetime.now(timezone.utc) + timedelta(minutes=30)
        ).isoformat()
        body = orjson.dumps({
            "scheduled_maintenances": [{
                "name":          "Test Maintenance",
                "scheduled_for": start_time,
                "status":        "scheduled",
            }]
        })
        session_ctx = _make_aiohttp_mock(status=200, body=body)

        with patch("tothbot.vps_deployment.aiohttp.ClientSession",
                   return_value=session_ctx):
            with patch("tothbot.vps_deployment.asyncio.create_task") as mock_task:
                await check_kraken_status(logger)
                assert mock_task.called, (
                    "VD-STAT-003: asyncio.create_task must be called when "
                    "maintenance is within 2 hours"
                )


# =============================================================
# UT-VD-002: Alert rate limiting — 1 per event type per 60s
# VD-ALT-003
# =============================================================

class TestAlertRateLimiting:

    def test_UT_VD_002_first_alert_passes(self):
        """
        UT-VD-002: VD-ALT-003 — first alert passes rate limiter.
        """
        assert _should_alert("TEST_EVENT") is True

    def test_UT_VD_002_second_immediate_alert_blocked(self):
        """
        UT-VD-002: VD-ALT-003 — second alert within 60s is blocked.
        """
        _should_alert("TEST_EVENT")
        assert _should_alert("TEST_EVENT") is False, (
            "VD-ALT-003: Second alert within 60s must be rate limited"
        )

    def test_UT_VD_002_different_event_types_independent(self):
        """
        UT-VD-002: Rate limiter is per event type. Different types
        are independent.
        """
        _should_alert("EVENT_A")
        assert _should_alert("EVENT_B") is True

    def test_UT_VD_002_rate_limit_expires(self):
        """
        UT-VD-002: Alert passes after rate limit window expires.
        """
        _should_alert("TEST_EVENT")
        _alert_last_sent["TEST_EVENT"] = time.monotonic() - (ALERT_RATE_LIMIT_SEC + 1)
        assert _should_alert("TEST_EVENT") is True


# =============================================================
# UT-VD-003: Environment variable validation
# VD-KEY-003
# =============================================================

class TestEnvironmentValidation:

    def test_UT_VD_003_missing_required_var_raises(self):
        """
        UT-VD-003: VD-KEY-003 — KeyError raised when required var missing.
        """
        clean_env = {k: v for k, v in os.environ.items()
                     if k not in REQUIRED_ENV_VARS}
        with patch.dict(os.environ, clean_env, clear=True):
            with pytest.raises(KeyError):
                validate_environment()

    def test_UT_VD_003_all_vars_present_no_raise(self):
        """
        UT-VD-003: VD-KEY-003 — no raise when all required vars present.
        """
        env_override = {k: "test_value" for k in REQUIRED_ENV_VARS}
        with patch.dict(os.environ, env_override):
            validate_environment()  # must not raise


# =============================================================
# UT-VD-004: systemd service unit mandatory parameters
# VD-SYS-001 / VD-SYS-002 / VD-SYS-005 / VD-SYS-006
# =============================================================

class TestServiceUnit:

    def test_UT_VD_004_watchdog_sec_120(self):
        """VD-SYS-002: WatchdogSec=120 in service unit."""
        assert "WatchdogSec=120" in SYSTEMD_SERVICE_UNIT

    def test_UT_VD_004_restart_on_failure(self):
        """VD-SYS-005: Restart=on-failure in service unit."""
        assert "Restart=on-failure" in SYSTEMD_SERVICE_UNIT

    def test_UT_VD_004_type_notify(self):
        """VD-SYS-001: Type=notify required for sd_notify."""
        assert "Type=notify" in SYSTEMD_SERVICE_UNIT

    def test_UT_VD_004_limit_nofile(self):
        """VD-SYS-001: LimitNOFILE=65535 for WS file descriptors."""
        assert "LimitNOFILE=65535" in SYSTEMD_SERVICE_UNIT

    def test_UT_VD_004_start_limit_interval_zero(self):
        """VD-SYS-006: StartLimitIntervalSec=0 — infinite retries."""
        assert "StartLimitIntervalSec=0" in SYSTEMD_SERVICE_UNIT


# =============================================================
# UT-VD-005: Watchdog ping interval and maintenance window
# VD-WD-002 / VD-STAT-001
# =============================================================

class TestWatchdogInterval:

    def test_UT_VD_005_ping_interval_30s(self):
        """VD-WD-002: WATCHDOG_PING_INTERVAL must be 30.0s."""
        assert WATCHDOG_PING_INTERVAL == 30.0, (
            f"VD-WD-002: Must be 30.0s, got {WATCHDOG_PING_INTERVAL}"
        )

    def test_UT_VD_005_maintenance_alert_window_2h(self):
        """VD-STAT-001: MAINTENANCE_ALERT_WINDOW_SEC must be 7200.0."""
        assert MAINTENANCE_ALERT_WINDOW_SEC == 7200.0, (
            f"VD-STAT-001: Must be 7200.0, got {MAINTENANCE_ALERT_WINDOW_SEC}"
        )
