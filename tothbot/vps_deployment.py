"""
DocDCN:     1011013
DocTitle:   VPS_Deployment
DocVersion: dv1_5
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/tothbot/vps_deployment.py
DocDate:    04-20-2026
DocTime:    23:00:00 UTC

============================================================
REVISION HISTORY
============================================================

  dv1_5   04-20-2026  OI-019 DEFECT-ALERT-ROUTING-001 fix.
                      send_alert() ALERT_EMAIL_TO default changed
                      from singular "alert@tothbot.com" to plural
                      "alerts@tothbot.com". Matches the real Proton
                      Mail mailbox; the singular mailbox does not
                      exist (Proton catch-all reserves the namespace,
                      returns "Address is already taken" on create
                      attempt). No other code logic changes.
                      Governed by 1011013 dv1_5 VD-ALT-001/002.

  dv1_4   04-12-2026  DC header added per 0311001 v1_1, 0311004 v1_1,
                      1011001 dv1_7. No code logic changes.

  dv1_4   04-05-2026  Initial Phase 8 implementation.
                      Written to 1011013 VPS_Deployment_Coding_Spec dv1_4.

============================================================

Infrastructure module. Three runtime responsibilities:
  1. Watchdog loop — pings systemd every 30 seconds. (VD-WD-001 through -006)
     WatchdogSec=120 in service unit. If loop hangs, systemd restarts TothBot.
  2. Kraken Status API check — queries maintenance schedule at startup. (VD-STAT-001)
     Non-blocking. Alerts if maintenance within 2 hours. Startup always continues.
  3. Async SMTP alert delivery — all operator alerts via aiosmtplib. (VD-ALT-002)
     Rate limited: 1 alert per event type per 60 seconds. (VD-ALT-003)

Hard Rules:
  WatchdogSec=120 in service unit — code pings every 30s (VD-WD-001/002).
  sd_notify READY=1 sent ONLY after all startup steps pass (VD-WD-004).
  API keys read from os.environ only — never hardcoded (VD-KEY-003).
  Kraken Status API check is NEVER a blocking condition (VD-STAT-001).
  Alert rate limit: 1 per event type per 60s (VD-ALT-003).
  Async SMTP — never blocks hot path (VD-ALT-003).

Deployment:
  VPS:    Hetzner CPX22, 87.99.141.44, Ubuntu 24.04.4 LTS
  Python: /root/tothbot_env/bin/python3 (3.12.3)
  Keys:   /root/.tothbot.env (chmod 600, never in Git)
  Logs:   /root/TothBot_V2-Code/logs/tothbot.log
  Service: /etc/systemd/system/tothbot.service
  Logrotate: /etc/logrotate.d/tothbot

Note: Recommended migration to Hillsboro OR (Hetzner) to reduce
round-trip latency to Kraken matching engine on AWS us-west-2.
Evaluate at paper trading start. Update IP whitelist on migration.
============================================================
"""
from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import aiohttp
import aiosmtplib
import orjson
import sdnotify

from email.message import EmailMessage

from tothbot.logger import log_record


# =============================================================
# CONSTANTS
# =============================================================

WATCHDOG_PING_INTERVAL: float = 30.0          # VD-WD-002: ping every 30s
KRAKEN_STATUS_API_URL: str = (
    "https://status.kraken.com/api/v2/scheduled-maintenances.json"
)
MAINTENANCE_ALERT_WINDOW_SEC: float = 7200.0  # 2 hours
ALERT_RATE_LIMIT_SEC: float = 60.0            # VD-ALT-003: 1 per event type per 60s


# =============================================================
# ALERT RATE LIMITER  (VD-ALT-003)
# =============================================================

# Module-level dict — persists for process lifetime
_alert_last_sent: dict[str, float] = {}


def _should_alert(event_type: str) -> bool:
    """
    Rate limit: 1 alert per event type per 60 seconds.
    VD-ALT-003.
    """
    now = time.monotonic()
    last = _alert_last_sent.get(event_type, 0.0)
    if now - last >= ALERT_RATE_LIMIT_SEC:
        _alert_last_sent[event_type] = now
        return True
    return False


# =============================================================
# ASYNC SMTP ALERT  (VD-ALT-002)
# =============================================================

async def send_alert(
    subject: str,
    body: str,
    event_type: str,
    logger: Any,
) -> None:
    """
    Send operator alert via async SMTP (aiosmtplib).
    Rate limited: 1 per event_type per 60 seconds.
    Never blocks hot path — runs as fire-and-forget task.
    VD-ALT-001 through VD-ALT-004.

    Credentials from environment (VD-KEY-003):
      SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS
      ALERT_EMAIL_FROM, ALERT_EMAIL_TO
    """
    if not _should_alert(event_type):
        return  # Rate limited — skip

    smtp_host = os.environ.get("SMTP_HOST", "")
    smtp_port = int(os.environ.get("SMTP_PORT", "587"))
    smtp_user = os.environ.get("SMTP_USER", "")
    smtp_pass = os.environ.get("SMTP_PASS", "")
    from_addr = os.environ.get("ALERT_EMAIL_FROM", "alerts@tothbot.com")
    to_addr   = os.environ.get("ALERT_EMAIL_TO",   "alerts@tothbot.com")  # OI-019

    if not smtp_host or not smtp_user:
        logger.warning(log_record({
            "event":      "ALERT_SMTP_NOT_CONFIGURED",
            "level":      "WARN",
            "component":  "VPS",
            "event_type": event_type,
            "note":       "SMTP not configured — alert not sent",
        }))
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"]    = from_addr
    msg["To"]      = to_addr
    msg.set_content(body)

    try:
        await aiosmtplib.send(
            msg,
            hostname=smtp_host,
            port=smtp_port,
            username=smtp_user,
            password=smtp_pass,
            start_tls=True,
        )
        logger.info(log_record({
            "event":      "ALERT_SENT",
            "level":      "INFO",
            "component":  "VPS",
            "event_type": event_type,
            "to":         to_addr,
            "subject":    subject,
        }))
    except Exception as exc:
        # BP-ERR-001: log before handling. Alert failure is non-fatal.
        logger.error(log_record({
            "event":      "ALERT_SMTP_FAILED",
            "level":      "ERROR",
            "component":  "VPS",
            "event_type": event_type,
            "error":      str(exc),
        }))


# =============================================================
# KRAKEN STATUS API CHECK  (VD-STAT-001 through -003)
# =============================================================

async def check_kraken_status(logger: Any) -> None:
    """
    Query Kraken Status API for upcoming scheduled maintenances.
    Called once at startup. NEVER a blocking condition.
    Startup always continues regardless of result.

    Three outcomes (VD-STAT-001):
      (a) Maintenance within 2 hours: CRITICAL log + alert operator.
      (b) No maintenance within 2 hours: INFO log only.
      (c) API unreachable: INFO log only.

    VD-STAT-001, VD-STAT-002, VD-STAT-003.
    """
    try:
        timeout = aiohttp.ClientTimeout(total=10.0)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(KRAKEN_STATUS_API_URL) as resp:
                if resp.status != 200:
                    logger.info(log_record({
                        "event":     "KRAKEN_STATUS_CHECK_FAILED",
                        "level":     "INFO",
                        "component": "VPS",
                        "http_status": resp.status,
                        "note":      "Non-200 response — startup continues",
                    }))
                    return

                raw  = await resp.read()
                data = orjson.loads(raw)

    except Exception as exc:
        logger.info(log_record({
            "event":     "KRAKEN_STATUS_CHECK_FAILED",
            "level":     "INFO",
            "component": "VPS",
            "error":     str(exc),
            "note":      "Status API unreachable — startup continues",
        }))
        return

    # Parse scheduled maintenances
    maintenances = data.get("scheduled_maintenances", [])
    now_utc = time.time()
    within_2h = []

    for m in maintenances:
        # Kraken returns maintenance start in scheduled_for field
        start_iso = m.get("scheduled_for", "")
        if not start_iso:
            continue
        try:
            from datetime import datetime, timezone
            start_dt = datetime.fromisoformat(
                start_iso.replace("Z", "+00:00")
            )
            start_ts = start_dt.timestamp()
            if 0 < (start_ts - now_utc) < MAINTENANCE_ALERT_WINDOW_SEC:
                within_2h.append({
                    "name":          m.get("name", "unknown"),
                    "scheduled_for": start_iso,
                    "status":        m.get("status", "unknown"),
                })
        except (ValueError, KeyError):
            continue

    if within_2h:
        detail = "; ".join(
            f"{m['name']} at {m['scheduled_for']}" for m in within_2h
        )
        logger.critical(log_record({
            "event":        "KRAKEN_MAINTENANCE_SCHEDULED",
            "level":        "CRITICAL",
            "component":    "VPS",
            "maintenances": within_2h,
            "note":         "Scheduled maintenance within 2 hours",
        }))
        # Fire alert to operator — rate limited
        asyncio.create_task(send_alert(
            subject="TothBot: Kraken maintenance within 2 hours",
            body=f"Scheduled maintenance detected:\n{detail}\n\n"
                 f"Consider delaying startup or monitoring open positions.",
            event_type="KRAKEN_MAINTENANCE_SCHEDULED",
            logger=logger,
        ))
    else:
        logger.info(log_record({
            "event":     "KRAKEN_STATUS_CLEAN",
            "level":     "INFO",
            "component": "VPS",
            "note":      "No scheduled maintenance within 2 hours",
        }))


# =============================================================
# SYSTEMD WATCHDOG LOOP  (VD-WD-001 through -006)
# =============================================================

async def watchdog_loop(logger: Any) -> None:
    """
    Systemd watchdog loop. Pings systemd every 30 seconds.
    WatchdogSec=120 in service unit — must ping within 120s.
    30-second interval gives 4x margin. (VD-WD-001, VD-WD-002)

    If this coroutine hangs, no ping is sent. Systemd restarts
    TothBot after 120 seconds — crash protection. (VD-WD-003)

    Runs as a named asyncio.Task for the lifetime of the process.
    Stored in WSManager as self._watchdog_task. (VD-WD-005)
    """
    notifier = sdnotify.SystemdNotifier()

    while True:
        try:
            notifier.notify("WATCHDOG=1")
            logger.debug(log_record({
                "event":     "SYSTEMD_WATCHDOG_PING",
                "level":     "DEBUG",
                "component": "VPS",
            }))
        except Exception as exc:
            # BP-ERR-001: log before handling.
            logger.warning(log_record({
                "event":     "WATCHDOG_NOTIFY_FAILED",
                "level":     "WARN",
                "component": "VPS",
                "error":     str(exc),
            }))

        await asyncio.sleep(WATCHDOG_PING_INTERVAL)


def notify_ready(logger: Any) -> None:
    """
    Send READY=1 to systemd. Called ONLY after all startup steps pass.
    VD-WD-004: sd_notify READY=1 sent ONLY after complete startup.
    """
    try:
        notifier = sdnotify.SystemdNotifier()
        notifier.notify("READY=1")
        logger.info(log_record({
            "event":     "VPS_STARTUP_COMPLETE",
            "level":     "INFO",
            "component": "VPS",
            "note":      "READY=1 sent to systemd",
        }))
    except Exception as exc:
        logger.error(log_record({
            "event":     "SYSTEMD_NOTIFY_READY_FAILED",
            "level":     "ERROR",
            "component": "VPS",
            "error":     str(exc),
        }))


def notify_stopping(logger: Any) -> None:
    """
    Send STOPPING=1 to systemd on graceful shutdown.
    Allows systemd to track clean vs crash termination.
    """
    try:
        notifier = sdnotify.SystemdNotifier()
        notifier.notify("STOPPING=1")
        logger.info(log_record({
            "event":     "VPS_SHUTDOWN_INITIATED",
            "level":     "INFO",
            "component": "VPS",
            "note":      "STOPPING=1 sent to systemd",
        }))
    except Exception as exc:
        logger.error(log_record({
            "event":     "SYSTEMD_NOTIFY_STOPPING_FAILED",
            "level":     "ERROR",
            "component": "VPS",
            "error":     str(exc),
        }))


# =============================================================
# ENVIRONMENT VALIDATION  (VD-KEY-003)
# =============================================================

REQUIRED_ENV_VARS: list[str] = [
    "KRAKEN_DATA_API_KEY",
    "KRAKEN_DATA_API_SECRET",
    "KRAKEN_TRADE_API_KEY",
    "KRAKEN_TRADE_API_SECRET",
    "ALERT_EMAIL_TO",
]


def validate_environment() -> None:
    """
    Validate all required environment variables are present.
    Raises KeyError at startup if any required variable missing.
    VD-KEY-003: raise KeyError — never continue with missing credentials.
    Called before any TothBot component is initialized.
    """
    missing = [k for k in REQUIRED_ENV_VARS if not os.environ.get(k)]
    if missing:
        raise KeyError(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"Check /root/.tothbot.env (chmod 600)."
        )


# =============================================================
# LOGROTATE CONFIGURATION TEXT  (VD-LOG-002)
# =============================================================

LOGROTATE_CONFIG: str = """\
/root/TothBot_V2-Code/logs/tothbot.log {
    daily
    rotate 90
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
"""
# Install at: /etc/logrotate.d/tothbot
# Test: logrotate -d /etc/logrotate.d/tothbot
# Force: logrotate -f /etc/logrotate.d/tothbot


# =============================================================
# SYSTEMD SERVICE UNIT TEXT  (VD-SYS-001 through -007)
# =============================================================

SYSTEMD_SERVICE_UNIT: str = """\
[Unit]
Description=TothBot V2 Automated Cryptocurrency Trading System
After=network-online.target
Wants=network-online.target
StartLimitIntervalSec=0

[Service]
Type=notify
ExecStart=/root/tothbot_env/bin/python3 -m tothbot
WorkingDirectory=/root/TothBot_V2-Code
EnvironmentFile=/root/.tothbot.env
Restart=on-failure
RestartSec=5
WatchdogSec=120
LimitNOFILE=65535
StandardOutput=journal
StandardError=journal
SyslogIdentifier=tothbot

[Install]
WantedBy=multi-user.target
"""
# Install at: /etc/systemd/system/tothbot.service
# systemctl daemon-reload
# systemctl enable tothbot
# systemctl start tothbot
# VD-SYS-005: Restart=on-failure with RestartSec=5
# VD-SYS-001: LimitNOFILE=65535 mandatory
# VD-SYS-002: WatchdogSec=120 mandatory
# VD-SYS-006: StartLimitIntervalSec=0 — systemd retries indefinitely