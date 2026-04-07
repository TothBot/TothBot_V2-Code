"""
TothBot V2 — Unit Tests: VPS Deployment
=============================================================
Test spec:   1021001 Unit_Test_Specification dv1_0 (UT-VD)
Module:      tothbot/vps_deployment.py
Coding spec: 1011013 VPS_Deployment_Coding_Spec dv1_4
BP standard: 1011001 Engineering_Best_Practices dv1_6
=============================================================

Tests: UT-VD-001 through UT-VD-005

Tests cover: Kraken Status API check (non-blocking contract),
alert rate limiting, environment variable validation,
service unit contents, and maintenance detection logic.

UT-FW-004: Standard asyncio. Do NOT use uvloop.
"""
from __future__ import annotations

import asyncio
import os
import time
from unittest.mock import AsyncMock, MagicMock, patch

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
    """Clear alert rate limiter state before each test."""
    _alert_last_sent.clear()
    yield
    _alert_last_sent.clear()


# =============================================================
# UT-VD-001: check_kraken_status is non-blocking
# VD-STAT-001
# =============================================================

class TestKrakenStatusCheck:

    @pytest.mark.asyncio
    async def test_UT_VD_001_status_api_unreachable_continues(self, logger):
        """
        UT-VD-001: VD-STAT-001 — if Kraken Status API is unreachable,
        check_kraken_status must return without raising.
        Startup always continues regardless. Non-blocking.
        """
        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=False)
            mock_session.get = AsyncMock(side_effect=Exception("Connection refused"))
            mock_session_cls.return_value = mock_session

            # Must not raise — VD-STAT-001: non-blocking always
            await check_kraken_status(logger)

    @pytest.mark.asyncio
    async def test_UT_VD_001_non_200_response_continues(self, logger):
        """
        UT-VD-001: VD-STAT-001 — non-200 HTTP response must not raise.
        Startup continues. INFO log emitted.
        """
        mock_resp = AsyncMock()
        mock_resp.status = 503
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.get.return_value = mock_resp

        with patch("tothbot.vps_deployment.aiohttp.ClientSession",
                   return_value=mock_session):
            await check_kraken_status(logger)  # must not raise

    @pytest.mark.asyncio
    async def test_UT_VD_001_no_maintenance_clean_status(self, logger):
        """
        UT-VD-001: VD-STAT-002 — clean status (no maintenance)
        must complete without raising.
        """
        import orjson
        clean_response = {"scheduled_maintenances": []}

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.read = AsyncMock(return_value=orjson.dumps(clean_response))
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.get.return_value = mock_resp

        with patch("tothbot.vps_deployment.aiohttp.ClientSession",
                   return_value=mock_session):
            await check_kraken_status(logger)  # must not raise

    @pytest.mark.asyncio
    async def test_UT_VD_001_maintenance_within_2h_detected(self, logger):
        """
        UT-VD-001: VD-STAT-003 — maintenance within 2 hours triggers
        CRITICAL log + alert task creation. Startup still continues.
        """
        import orjson
        from datetime import datetime, timezone, timedelta

        # Maintenance starting in 30 minutes
        start_time = (datetime.now(timezone.utc) + timedelta(minutes=30)).isoformat()
        maintenance_response = {
            "scheduled_maintenances": [
                {
                    "name":          "Test Maintenance",
                    "scheduled_for": start_time,
                    "status":        "scheduled",
                }
            ]
        }

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.read = AsyncMock(return_value=orjson.dumps(maintenance_response))
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        mock_session = AsyncMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.get.return_value = mock_resp

        with patch("tothbot.vps_deployment.aiohttp.ClientSession",
                   return_value=mock_session):
            with patch("tothbot.vps_deployment.asyncio.create_task") as mock_task:
                await check_kraken_status(logger)
                # Alert task must be created for maintenance within 2h
                mock_task.assert_called_once()


# =============================================================
# UT-VD-002: Alert rate limiting — 1 per event type per 60s
# VD-ALT-003
# =============================================================

class TestAlertRateLimiting:

    def test_UT_VD_002_first_alert_passes(self):
        """
        UT-VD-002: VD-ALT-003 — first alert for an event type
        must pass the rate limiter.
        """
        result = _should_alert("TEST_EVENT")
        assert result is True, (
            "VD-ALT-003: First alert for event type must be allowed"
        )

    def test_UT_VD_002_second_immediate_alert_blocked(self):
        """
        UT-VD-002: VD-ALT-003 — second alert for the same event type
        within 60 seconds must be blocked (rate limited).
        """
        _should_alert("TEST_EVENT")  # first — passes
        result = _should_alert("TEST_EVENT")  # immediate second — blocked
        assert result is False, (
            "VD-ALT-003: Second alert within 60s must be rate limited"
        )

    def test_UT_VD_002_different_event_types_independent(self):
        """
        UT-VD-002: VD-ALT-003 — rate limiter is per event type.
        Different event types are independent.
        """
        _should_alert("EVENT_A")   # blocks EVENT_A
        result = _should_alert("EVENT_B")  # EVENT_B should pass
        assert result is True, (
            "VD-ALT-003: Different event types must have independent "
            "rate limits"
        )

    def test_UT_VD_002_rate_limit_expires(self):
        """
        UT-VD-002: VD-ALT-003 — alert must pass after 60s expiry.
        Mock time to simulate elapsed interval.
        """
        _should_alert("TEST_EVENT")  # first — sets timestamp

        # Fake elapsed time > 60s
        _alert_last_sent["TEST_EVENT"] = time.monotonic() - (ALERT_RATE_LIMIT_SEC + 1)

        result = _should_alert("TEST_EVENT")
        assert result is True, (
            "VD-ALT-003: Alert must be allowed after rate limit window expires"
        )


# =============================================================
# UT-VD-003: Environment variable validation
# VD-KEY-003
# =============================================================

class TestEnvironmentValidation:

    def test_UT_VD_003_missing_required_var_raises(self):
        """
        UT-VD-003: VD-KEY-003 — validate_environment() must raise
        KeyError if any required environment variable is missing.
        Never continue with missing credentials.
        """
        clean_env = {k: v for k, v in os.environ.items()
                     if k not in REQUIRED_ENV_VARS}

        with patch.dict(os.environ, clean_env, clear=True):
            with pytest.raises(KeyError) as exc_info:
                validate_environment()
            assert "Missing required environment variables" in str(exc_info.value)

    def test_UT_VD_003_all_vars_present_no_raise(self):
        """
        UT-VD-003: VD-KEY-003 — validate_environment() must NOT raise
        when all required environment variables are present.
        """
        env_override = {k: "test_value" for k in REQUIRED_ENV_VARS}
        with patch.dict(os.environ, env_override):
            validate_environment()  # must not raise


# =============================================================
# UT-VD-004: systemd service unit contains mandatory parameters
# VD-SYS-001 / VD-SYS-002 / VD-SYS-005 / VD-SYS-006
# =============================================================

class TestServiceUnit:

    def test_UT_VD_004_watchdog_sec_120(self):
        """
        UT-VD-004: VD-SYS-002 — WatchdogSec=120 must be in service unit.
        Watchdog ping interval is 30s (4x margin).
        """
        assert "WatchdogSec=120" in SYSTEMD_SERVICE_UNIT, (
            "VD-SYS-002: WatchdogSec=120 must be in service unit"
        )

    def test_UT_VD_004_restart_on_failure(self):
        """
        UT-VD-004: VD-SYS-005 — Restart=on-failure must be in unit.
        systemd auto-restarts TothBot on fatal exit.
        """
        assert "Restart=on-failure" in SYSTEMD_SERVICE_UNIT, (
            "VD-SYS-005: Restart=on-failure must be in service unit"
        )

    def test_UT_VD_004_type_notify(self):
        """
        UT-VD-004: VD-SYS-001 — Type=notify required for
        sd_notify READY=1 / WATCHDOG=1 to work.
        """
        assert "Type=notify" in SYSTEMD_SERVICE_UNIT, (
            "VD-SYS-001: Type=notify must be in service unit"
        )

    def test_UT_VD_004_limit_nofile(self):
        """
        UT-VD-004: VD-SYS-001 — LimitNOFILE=65535 must be in unit.
        Required for WS connection file descriptor limits.
        """
        assert "LimitNOFILE=65535" in SYSTEMD_SERVICE_UNIT, (
            "VD-SYS-001: LimitNOFILE=65535 must be in service unit"
        )

    def test_UT_VD_004_start_limit_interval_zero(self):
        """
        UT-VD-004: VD-SYS-006 — StartLimitIntervalSec=0 ensures
        systemd retries indefinitely (no retry cap).
        """
        assert "StartLimitIntervalSec=0" in SYSTEMD_SERVICE_UNIT, (
            "VD-SYS-006: StartLimitIntervalSec=0 must be in service unit"
        )


# =============================================================
# UT-VD-005: Watchdog ping interval
# VD-WD-002
# =============================================================

class TestWatchdogInterval:

    def test_UT_VD_005_ping_interval_30s(self):
        """
        UT-VD-005: VD-WD-002 — watchdog_loop pings every 30 seconds.
        WatchdogSec=120 → 30s gives 4x safety margin.
        WATCHDOG_PING_INTERVAL must be 30.0.
        """
        assert WATCHDOG_PING_INTERVAL == 30.0, (
            f"VD-WD-002: WATCHDOG_PING_INTERVAL must be 30.0s, "
            f"got {WATCHDOG_PING_INTERVAL}"
        )

    def test_UT_VD_005_maintenance_alert_window_2h(self):
        """
        UT-VD-005: VD-STAT-001 — MAINTENANCE_ALERT_WINDOW_SEC must
        be 7200 (2 hours = 2 * 60 * 60).
        """
        assert MAINTENANCE_ALERT_WINDOW_SEC == 7200.0, (
            f"VD-STAT-001: MAINTENANCE_ALERT_WINDOW_SEC must be 7200.0, "
            f"got {MAINTENANCE_ALERT_WINDOW_SEC}"
        )
