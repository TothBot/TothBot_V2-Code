"""
DocDCN:     1011007
DocTitle:   Logger
DocVersion: dv1_3
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/tothbot/logger.py
DocDate:    04-20-2026
DocTime:    23:00:00 UTC

============================================================
REVISION HISTORY
============================================================

  dv1_3   04-20-2026  OI-019 DEFECT-ALERT-ROUTING-001 fix. ALERT_EMAIL
                      changed from singular "alert@tothbot.com" to
                      plural "alerts@tothbot.com". Proton Mail mailbox
                      exists only at the plural address; the singular
                      mailbox cannot be created (Proton catch-all
                      reserves the namespace and returns "Address is
                      already taken"). 100% of post-deploy critical
                      alerts bounced until this fix (TB00105 Query 9).
                      No other code logic changes. Governed by
                      1011007 dv1_3 Section 8 LG-ALERT-001/002/004.

  dv1_2   04-12-2026  DC header added per 0311001 v1_1, 0311004 v1_1,
                      1011001 dv1_7. No code logic changes.

  dv1_2   04-05-2026  Initial Phase 8 implementation.
                      Written to 1011007 Logger_Coding_Spec dv1_2.

============================================================

The Logger is the sole data interface between TothBot and CIATS.
It NEVER blocks the trading hot path under any condition.
All callers fire-and-forget via QueueHandler.put_nowait().
CIATS reads log files directly — Logger does not push to CIATS.

Hard Rules (HR-LG-001 through HR-LG-011):
  HR-LG-001: NEVER block hot path under any condition.
  HR-LG-002: queue.Queue (stdlib), NOT asyncio.Queue.
  HR-LG-003: maxsize=10,000 entries.
  HR-LG-004: log_listener.stop() on clean shutdown.
  HR-LG-005: All TRADE_CLOSE fields mandatory (see 1011007 S7).
  HR-LG-006: TRADE_CLOSE fires ONCE per position close.
  HR-LG-007: ws_roundtrip_latency_ms from WS v2 time_in/time_out.
  HR-LG-008: connection_id logged on every WS event.
  HR-LG-009: All numeric values: string repr of Decimal. No float.
  HR-LG-010: Log format: NDJSON (one JSON object per line).
  HR-LG-011: Queue full -> stderr + HIGH alert. Do not block.
============================================================
"""
from __future__ import annotations

import asyncio
import logging
import logging.handlers
import os
import queue
import smtplib
import sys
from datetime import datetime, timezone
from decimal import Decimal
from email.mime.text import MIMEText
from typing import Any

import orjson

# =============================================================
# CONSTANTS
# =============================================================

LOG_DIR: str = "/home/tothbot/logs"
LOG_FILE: str = "/home/tothbot/logs/tothbot.log"

# LG-QUEUE-002: 20 positions x 10 events/candle x 50-candle buffer
LOG_QUEUE_MAXSIZE: int = 10_000

# LG-QUEUE-006: Alert threshold
LOG_QUEUE_WARN_PCT: float = 0.80

# LG-FILE-002: 50 MB per file, 90 rotated files (~280 days)
LOG_ROTATE_BYTES: int = 50 * 1024 * 1024
LOG_ROTATE_COUNT: int = 90

# Alert routing — LG-ALERT-001/002 (OI-019: plural — Proton mailbox)
ALERT_EMAIL: str = "alerts@tothbot.com"
ALERT_SMTP_TIMEOUT_SEC: int = 5     # fire-and-forget
ALERT_LEVELS: frozenset[str] = frozenset({"HIGH", "CRITICAL"})

# Valid log level values (LG-FMT-002)
LOG_LEVELS: frozenset[str] = frozenset({"DEBUG", "INFO", "HIGH", "WARN", "CRITICAL"})

# Valid component names (LG-FMT-002)
LOG_COMPONENTS: frozenset[str] = frozenset({
    "WS_MGR", "SIGNAL_PIPELINE", "EXEC_ENG",
    "EXIT_CTRL", "POS_MIRROR", "REGIME_ENG", "LOGGER", "CIATS",
})


# =============================================================
# JSON SERIALIZATION — BP-JSON-005
# =============================================================

def _json_default(obj: Any) -> Any:
    """
    orjson Decimal serialization callback — MANDATORY per BP-JSON-005.
    orjson does not natively serialize Decimal.
    Without this callback, orjson raises TypeError on any Decimal value.
    """
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(
        f"Object of type {type(obj)} is not JSON serializable"
    )


def log_record(event_dict: dict[str, Any]) -> str:
    """
    Serialize event_dict to a single NDJSON line (HR-LG-010).

    Mandatory fields the caller MUST provide:
        event:     SCREAMING_SNAKE_CASE event code (Section 6 registry)
        level:     DEBUG | INFO | HIGH | WARN | CRITICAL
        component: WS_MGR | SIGNAL_PIPELINE | EXEC_ENG | EXIT_CTRL |
                   POS_MIRROR | REGIME_ENG | LOGGER | CIATS

    ts is injected automatically (ISO 8601 UTC with microseconds,
    BP-ENV-005) if not already present in event_dict.

    All Decimal values serialized as JSON strings (HR-LG-009).
    Returns: UTF-8 decoded JSON string, no trailing newline.
    """
    if "ts" not in event_dict:
        event_dict["ts"] = datetime.now(timezone.utc).isoformat()
    return orjson.dumps(event_dict, default=_json_default).decode()


# =============================================================
# ALERT — LG-ALERT-004
# Bypasses Logger queue entirely — no circularity risk.
# Used for: CRITICAL/HIGH events, queue-full conditions.
# Credentials from environment variables — never hardcoded.
# =============================================================

def _alert_operator_direct(message: str) -> None:
    """
    Send alert email directly via SMTP, bypassing the Logger queue.

    Environment variables required:
        TOTHBOT_SMTP_HOST  SMTP server hostname
        TOTHBOT_SMTP_PORT  SMTP port (default: 587)
        TOTHBOT_SMTP_USER  SMTP username / from address
        TOTHBOT_SMTP_PASS  SMTP password

    Timeout: 5 seconds (fire-and-forget).
    On SMTP failure: write to stderr only. Never raises.
    """
    smtp_host = os.environ.get("TOTHBOT_SMTP_HOST", "")
    smtp_port = int(os.environ.get("TOTHBOT_SMTP_PORT", "587"))
    smtp_user = os.environ.get("TOTHBOT_SMTP_USER", "")
    smtp_pass = os.environ.get("TOTHBOT_SMTP_PASS", "")

    if not smtp_host or not smtp_user:
        # SMTP not configured — stderr fallback only
        print(
            f"[TOTHBOT ALERT — NO SMTP CONFIG]: {message}",
            file=sys.stderr,
            flush=True,
        )
        return

    try:
        msg = MIMEText(message)
        msg["Subject"] = f"[TothBot ALERT] {message[:80]}"
        msg["From"] = smtp_user
        msg["To"] = ALERT_EMAIL

        with smtplib.SMTP(
            smtp_host, smtp_port, timeout=ALERT_SMTP_TIMEOUT_SEC
        ) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, [ALERT_EMAIL], msg.as_string())

    except Exception as exc:  # noqa: broad — SMTP is fire-and-forget
        # Cannot log via Logger (circularity). Stderr only.
        print(
            f"[TOTHBOT ALERT — SMTP FAILED ({exc})]: {message}",
            file=sys.stderr,
            flush=True,
        )


# =============================================================
# TothBotQueueHandler — Hot Path, Non-Blocking
# LG-QUEUE-003 / HR-LG-001 / HR-LG-011
# =============================================================

class TothBotQueueHandler(logging.handlers.QueueHandler):
    """
    Non-blocking QueueHandler for the TothBot hot path.

    put_nowait() returns immediately — hot path never blocks (HR-LG-001).
    queue.Full -> stderr write + direct operator alert (HR-LG-011).
    No Logger queue involved in the queue-full alert path (LG-ALERT-004).
    """

    def enqueue(self, record: logging.LogRecord) -> None:
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            # HR-LG-011: Queue full -> stderr + alert. NEVER block.
            print(
                f"[TOTHBOT LOGGER QUEUE FULL] record dropped: "
                f"{record.getMessage()[:200]}",
                file=sys.stderr,
                flush=True,
            )
            # Direct alert bypasses the Logger queue (LG-ALERT-004)
            _alert_operator_direct(
                "CRITICAL: Logger queue full. Log records are being dropped. "
                "Investigate immediately. TothBot may be data-blind."
            )


# =============================================================
# TothBotAlertHandler — Background Thread (in QueueListener)
# LG-ALERT-001 / LG-ALERT-002
# NOT on the hot path. Runs in QueueListener's background thread.
# =============================================================

class TothBotAlertHandler(logging.Handler):
    """
    Sends email alerts for HIGH and CRITICAL log events.

    Attached to QueueListener — runs in its background thread.
    NEVER on the hot path. SMTP blocking here is acceptable.

    Parses the pre-serialized JSON message string to extract the
    "level" field, because all TothBot log records are JSON strings
    passed to logger.info()/logger.debug()/etc. The Python logging
    level does not carry our semantic HIGH/CRITICAL distinction.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message_str = record.getMessage()
            parsed: dict[str, Any] = orjson.loads(message_str)
            level_field: str = parsed.get("level", "")

            if level_field in ALERT_LEVELS:
                event = parsed.get("event", "UNKNOWN_EVENT")
                component = parsed.get("component", "UNKNOWN")
                _alert_operator_direct(
                    f"[{level_field}] {event} | {component} | "
                    f"{message_str[:500]}"
                )
        except Exception as exc:  # noqa: broad — alert handler must not crash
            print(
                f"[TothBotAlertHandler.emit ERROR ({exc})]: "
                f"{record.getMessage()[:200]}",
                file=sys.stderr,
                flush=True,
            )


# =============================================================
# Queue Health Monitor — LG-QUEUE-006
# Run as asyncio task at startup.
# =============================================================

async def monitor_log_queue(log_queue: "queue.Queue[Any]") -> None:
    """
    Monitor log queue fill level every 60 seconds (LG-QUEUE-006).
    Alert operator directly if fill >= 80%.
    Caller: start as asyncio.create_task(monitor_log_queue(log_queue)).
    Runs indefinitely until task is cancelled on shutdown.
    """
    while True:
        await asyncio.sleep(60)
        qsize: int = log_queue.qsize()
        fill_pct: float = qsize / LOG_QUEUE_MAXSIZE
        if fill_pct >= LOG_QUEUE_WARN_PCT:
            _alert_operator_direct(
                f"Logger queue at {fill_pct:.0%} ({qsize}/{LOG_QUEUE_MAXSIZE}). "
                f"Risk of overflow. Investigate log write throughput."
            )


# =============================================================
# LOGGER INITIALIZATION — Section 10
# =============================================================

def initialize_logger() -> tuple[
        "queue.Queue[Any]",
        logging.handlers.QueueListener,
        logging.Logger,
]:
    """
    Initialize TothBot Logger. Call ONCE at startup before any
    other module is initialized.

    Returns:
        log_queue:    queue.Queue — pass to monitor_log_queue()
        log_listener: QueueListener — call .stop() on shutdown
        logger:       logging.Logger — "tothbot" instance

    After calling this function:
        1. Start monitor:  asyncio.create_task(
                               monitor_log_queue(log_queue))
        2. Pass logger to all modules via dependency injection.

    Shutdown sequence (mandatory — LG-QUEUE-005):
        1. Stop accepting new events (set shutdown flag)
        2. log_listener.stop()     <- drains queue, joins thread
        3. file_handler.close()    <- caller must retain reference
        4. Shutdown asyncio event loop

    Architecture:
        Hot path:    Module -> logger.info(log_record(d))
                              -> TothBotQueueHandler.enqueue()
                              -> queue.put_nowait()  [returns immediately]

        Background:  QueueListener thread:
                         -> RotatingFileHandler -> tothbot.log (NDJSON)
                         -> TothBotAlertHandler -> SMTP on HIGH/CRITICAL
    """
    # LG-FILE-001: Create log directory at startup
    os.makedirs(LOG_DIR, exist_ok=True)

    # LG-QUEUE-001: queue.Queue (stdlib) NOT asyncio.Queue
    # HR-LG-002: stdlib queue required — QueueListener is non-asyncio thread
    # HR-LG-003: maxsize=10,000
    log_queue: "queue.Queue[Any]" = queue.Queue(maxsize=LOG_QUEUE_MAXSIZE)

    # LG-FILE-002: RotatingFileHandler — 50 MB per file, 90 files
    file_handler = logging.handlers.RotatingFileHandler(
        filename=LOG_FILE,
        maxBytes=LOG_ROTATE_BYTES,
        backupCount=LOG_ROTATE_COUNT,
        encoding="utf-8",
    )
    # %(message)s emits the pre-serialized JSON string.
    # One LogRecord.getMessage() = one complete JSON line (HR-LG-010).
    file_handler.setFormatter(logging.Formatter("%(message)s"))

    # LG-ALERT-001/002: Alert handler — background thread, not hot path
    alert_handler = TothBotAlertHandler()

    # LG-QUEUE-004: QueueListener — background thread
    # Reads from log_queue, dispatches to file_handler + alert_handler.
    # respect_handler_level=True: honours per-handler level filters.
    log_listener = logging.handlers.QueueListener(
        log_queue,
        file_handler,
        alert_handler,
        respect_handler_level=True,
    )
    log_listener.start()

    # LG-QUEUE-003: TothBotQueueHandler — non-blocking hot path entry
    queue_handler = TothBotQueueHandler(log_queue)

    # "tothbot" logger — sole logger instance for all TothBot modules
    tothbot_logger = logging.getLogger("tothbot")
    tothbot_logger.handlers = [queue_handler]   # exclusive handler list
    tothbot_logger.setLevel(logging.DEBUG)
    tothbot_logger.propagate = False            # no root logger leakage

    return log_queue, log_listener, tothbot_logger


# =============================================================
# USAGE REFERENCE (not executable — for developer reference)
# =============================================================
#
# --- Startup (in main.py / startup_sequence.py) ---
#
#   from tothbot.logger import initialize_logger, monitor_log_queue
#
#   log_queue, log_listener, logger = initialize_logger()
#   asyncio.create_task(monitor_log_queue(log_queue))
#
# --- Logging from any module ---
#
#   from tothbot.logger import log_record
#   from decimal import Decimal
#
#   logger.info(log_record({
#       "event":     "CANDLE_CLOSE",
#       "level":     "INFO",
#       "component": "WS_MGR",
#       "symbol":    "BTC/USD",
#       "close":     Decimal("65432.1"),
#       "atr_14":    Decimal("312.5"),
#   }))
#
# --- Shutdown (in exit_controller / startup_sequence.py) ---
#
#   log_listener.stop()   # drains queue, joins background thread
#   file_handler.close()  # caller must retain file_handler reference
#