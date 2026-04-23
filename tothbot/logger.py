"""
DocDCN:     1011007
DocTitle:   Logger
DocVersion: dv1_5
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/tothbot/logger.py
DocDate:    04-23-2026
DocTime:    04:30:00 UTC

============================================================
REVISION HISTORY
============================================================

  dv1_5   04-23-2026  OI-029 DEFECT-ALERT-SMTP-TIMEOUT-001 fix.
                      _alert_operator_direct SMTP conversation
                      now wrapped in a bounded retry loop to
                      absorb transient Proton SMTP read-timeouts
                      observed in the TB00105 / TB00111 / TB00117
                      windows. Three related changes:

                      (1) ALERT_SMTP_TIMEOUT_SEC raised from 5
                      to 20 seconds. The 5s ceiling applied to
                      each of four sequential network round-
                      trips (connect, starttls, login, sendmail);
                      any single stage blocking beyond 5s
                      produced "The read operation timed out"
                      and lost the alert. 20s absorbs realistic
                      transient server-load / slow-TLS-handshake
                      without blocking excessively.

                      (2) New constants ALERT_SMTP_MAX_ATTEMPTS
                      (2) and ALERT_SMTP_RETRY_BACKOFF_SEC (1).
                      SMTP conversation runs in a for-attempt
                      loop; on exception, sleeps
                      ALERT_SMTP_RETRY_BACKOFF_SEC and retries.
                      ALERT_SMTP_FAILED emits only if every
                      attempt fails — preserving the existing
                      event surface and the anti-recursion
                      semantics downstream
                      (TothBotAlertHandler.emit still skips
                      event=="ALERT_SMTP_FAILED").

                      (3) Worst-case wall time bounded at
                      20 + 1 + 20 = 41s. This function runs
                      off the trading hot path (HR-LG-001
                      preserved via QueueHandler; this path is
                      QueueListener background only) so the
                      raised ceiling has no impact on signal
                      evaluation or execution latency.

                      Added `import time` for the blocking
                      sleep between attempts (synchronous is
                      correct here — we are already in the
                      QueueListener thread, not the asyncio
                      event loop; asyncio.sleep would be a
                      category error). Governed by 1011007
                      dv1_5 Section 6.9 and Section 11.

  dv1_4   04-21-2026  TB00108 bundled: OI-023 (CRITICAL) +
                      OI-022 (LOW).

                      OI-023 DEFECT-TRADE-RECORD-001 fix
                      (AI-1). TRADE_CLOSE records are now
                      written to BOTH tothbot.log (existing,
                      rotating, diagnostic) AND an append-only
                      permanent file at
                      /home/tothbot/records/trades_<YYYY>.jsonl
                      (annual-segmented, no rotation, no size
                      cap). New module constant
                      TRADES_RECORD_DIR. New handler class
                      TothBotTradeRecordHandler attached to
                      the existing QueueListener alongside
                      file_handler and alert_handler. Hot
                      path unchanged (HR-LG-001 preserved by
                      the existing queue architecture — the
                      new handler runs in the QueueListener
                      background thread, not on the trading
                      hot path). File permissions hard-pinned
                      to 0o644 via os.open O_CREAT mode
                      argument. fsync after each write for
                      compliance-grade durability against
                      VPS power loss / kernel panic. Write
                      failure path emits structured
                      TRADE_RECORD_WRITE_FAILED (HIGH) event
                      back through the Logger queue (lands
                      in tothbot.log via file_handler,
                      triggers operator email via
                      alert_handler) plus stderr as ultimate
                      fallback. Never raises. Never blocks
                      tothbot.log write (QueueListener
                      dispatches each handler independently;
                      handler exceptions are isolated by
                      logging.Handler.handleError). Records
                      directory created at startup via
                      os.makedirs(TRADES_RECORD_DIR,
                      exist_ok=True). Governed by 1011007
                      dv1_4 Section 11 and 0511006 dv1_1.

                      OI-022 DEFECT-OBSERVABILITY-ALERT-SENT-001
                      fix (AI-2). _alert_operator_direct now
                      emits a structured ALERT_SENT (INFO)
                      event on SMTP success and a structured
                      ALERT_SMTP_FAILED (HIGH) event on SMTP
                      failure, via the standard Logger path
                      (lands in tothbot.log). Prior behavior
                      emitted nothing on success and stderr
                      only on failure. Signature extended
                      with keyword-only parameters subject,
                      triggering_event, and emit_structured
                      (defaults preserve callable shape).
                      emit_structured=False used from the
                      queue-full path to avoid recursive
                      enqueue when the queue is already full.
                      TothBotAlertHandler.emit skips
                      event=="ALERT_SMTP_FAILED" to break
                      the potential alert-failure feedback
                      loop (persistent SMTP outage would
                      otherwise self-trigger indefinitely).
                      Governed by 1011007 dv1_4 Section 6.9
                      and Section 8 LG-ALERT-005.

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

Two output streams on disk (two-stream record architecture per
0511006 dv1_1):
  (1) /home/tothbot/logs/tothbot.log
        RotatingFileHandler, 50 MB / 90 files, NDJSON.
        Diagnostic stream. ALL events land here. CIATS reads.
  (2) /home/tothbot/records/trades_<YYYY>.jsonl
        TothBotTradeRecordHandler, append-only, no rotation,
        one JSON per line, permanent. TRADE_CLOSE events only.
        Authoritative permanent trade record (tax / CIATS
        200-trade floor / audit trail).

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
import time
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

# OI-023 / AI-1: Permanent trade record directory.
# Annual-segmented JSONL files written here. No rotation.
# See 1011007 dv1_4 Section 11 and 0511006 dv1_1.
TRADES_RECORD_DIR: str = "/home/tothbot/records"

# LG-QUEUE-002: 20 positions x 10 events/candle x 50-candle buffer
LOG_QUEUE_MAXSIZE: int = 10_000

# LG-QUEUE-006: Alert threshold
LOG_QUEUE_WARN_PCT: float = 0.80

# LG-FILE-002: 50 MB per file, 90 rotated files (~280 days)
LOG_ROTATE_BYTES: int = 50 * 1024 * 1024
LOG_ROTATE_COUNT: int = 90

# Alert routing — LG-ALERT-001/002 (OI-019: plural — Proton mailbox)
ALERT_EMAIL: str = "alerts@tothbot.com"
ALERT_SMTP_TIMEOUT_SEC: int = 20          # OI-029: raised 5 → 20 to
                                          # absorb transient Proton
                                          # SMTP read-timeouts
ALERT_SMTP_MAX_ATTEMPTS: int = 2          # OI-029: bounded retry loop
ALERT_SMTP_RETRY_BACKOFF_SEC: int = 1     # OI-029: sleep between
                                          # attempts on exception
ALERT_LEVELS: frozenset[str] = frozenset({"HIGH", "CRITICAL"})

# Valid log level values (LG-FMT-002)
LOG_LEVELS: frozenset[str] = frozenset({"DEBUG", "INFO", "HIGH", "WARN", "CRITICAL"})

# Valid component names (LG-FMT-002)
LOG_COMPONENTS: frozenset[str] = frozenset({
    "WS_MGR", "SIGNAL_PIPELINE", "EXEC_ENG",
    "EXIT_CTRL", "POS_MIRROR", "REGIME_ENG", "LOGGER", "CIATS",
})

# OI-023 / AI-1: hard-pinned permanent trade record file mode.
# VD-KEY / Deming 6.1 — explicit permission contract in source,
# independent of runtime umask.
TRADES_RECORD_FILE_MODE: int = 0o644


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
# ALERT — LG-ALERT-004 / LG-ALERT-005
# Bypasses Logger queue entirely — no circularity risk.
# Used for: CRITICAL/HIGH events, queue-full conditions.
# Credentials from environment variables — never hardcoded.
#
# dv1_4 (AI-2, OI-022): On SMTP success emits ALERT_SENT (INFO)
# via the standard Logger path. On SMTP failure emits
# ALERT_SMTP_FAILED (HIGH) via the standard Logger path PLUS
# stderr. Prior behavior: no event on success; stderr only on
# failure.
# =============================================================

def _alert_operator_direct(
    message: str,
    *,
    subject: str | None = None,
    triggering_event: str = "",
    emit_structured: bool = True,
) -> None:
    """
    Send alert email directly via SMTP, bypassing the Logger queue.

    Environment variables required:
        TOTHBOT_SMTP_HOST  SMTP server hostname
        TOTHBOT_SMTP_PORT  SMTP port (default: 587)
        TOTHBOT_SMTP_USER  SMTP username / from address
        TOTHBOT_SMTP_PASS  SMTP password

    Timeout: 5 seconds (fire-and-forget).
    On SMTP failure: write to stderr AND (if emit_structured)
    emit an ALERT_SMTP_FAILED (HIGH) event via the Logger queue.
    Never raises.

    Parameters:
        message:          Human-readable alert message body.
        subject:          Optional explicit subject line. If None,
                          derived from the first 80 chars of message.
        triggering_event: Event code that caused the alert (e.g.,
                          "PING_TIMEOUT", "LOG_QUEUE_FULL"). Included
                          in the ALERT_SENT / ALERT_SMTP_FAILED payload.
        emit_structured:  If True (default), emit ALERT_SENT or
                          ALERT_SMTP_FAILED via the Logger queue.
                          Set False when calling from contexts where
                          the Logger queue is known unreliable (e.g.,
                          the queue-full handler itself).
    """
    smtp_host = os.environ.get("TOTHBOT_SMTP_HOST", "")
    smtp_port = int(os.environ.get("TOTHBOT_SMTP_PORT", "587"))
    smtp_user = os.environ.get("TOTHBOT_SMTP_USER", "")
    smtp_pass = os.environ.get("TOTHBOT_SMTP_PASS", "")

    effective_subject: str = (
        subject if subject is not None
        else f"[TothBot ALERT] {message[:80]}"
    )

    if not smtp_host or not smtp_user:
        # SMTP not configured — stderr fallback only.
        # Do NOT emit ALERT_SMTP_FAILED: SMTP not being configured
        # is an environmental condition, not an operational failure.
        print(
            f"[TOTHBOT ALERT — NO SMTP CONFIG]: {message}",
            file=sys.stderr,
            flush=True,
        )
        return

    try:
        msg = MIMEText(message)
        msg["Subject"] = effective_subject
        msg["From"] = smtp_user
        msg["To"] = ALERT_EMAIL

        # OI-029: bounded retry loop. ALERT_SMTP_MAX_ATTEMPTS=2,
        # ALERT_SMTP_RETRY_BACKOFF_SEC=1. Worst-case wall time
        # 20 + 1 + 20 = 41s — off the hot path, acceptable.
        last_exc: Exception | None = None
        sent: bool = False
        for attempt in range(1, ALERT_SMTP_MAX_ATTEMPTS + 1):
            try:
                with smtplib.SMTP(
                    smtp_host, smtp_port,
                    timeout=ALERT_SMTP_TIMEOUT_SEC,
                ) as server:
                    server.starttls()
                    server.login(smtp_user, smtp_pass)
                    server.sendmail(
                        smtp_user, [ALERT_EMAIL], msg.as_string()
                    )
                sent = True
                break
            except Exception as attempt_exc:  # noqa: broad
                last_exc = attempt_exc
                if attempt < ALERT_SMTP_MAX_ATTEMPTS:
                    # Synchronous sleep is correct here — this
                    # function runs on the QueueListener background
                    # thread, NOT the asyncio event loop.
                    time.sleep(ALERT_SMTP_RETRY_BACKOFF_SEC)

        if not sent:
            # All attempts failed — re-raise to the outer handler
            # below, which writes stderr and emits
            # ALERT_SMTP_FAILED. Preserves the existing event
            # surface: one event per alert, not one per attempt.
            if last_exc is not None:
                raise last_exc
            raise RuntimeError(
                "SMTP retry loop exited without success or exception"
            )

        # OI-022 / AI-2: structured success event via Logger path.
        if emit_structured:
            try:
                logging.getLogger("tothbot").info(log_record({
                    "event":             "ALERT_SENT",
                    "level":             "INFO",
                    "component":         "LOGGER",
                    "to":                ALERT_EMAIL,
                    "subject":           effective_subject,
                    "triggering_event":  triggering_event,
                }))
            except Exception:  # noqa: broad — alert path must not raise
                pass

    except Exception as exc:  # noqa: broad — SMTP is fire-and-forget
        # Cannot log via Logger as primary channel (circularity on
        # persistent SMTP outage). Stderr is the primary failure sink.
        print(
            f"[TOTHBOT ALERT — SMTP FAILED ({exc})]: {message}",
            file=sys.stderr,
            flush=True,
        )
        # OI-022 / AI-2: structured failure event via Logger path.
        # TothBotAlertHandler.emit skips event=="ALERT_SMTP_FAILED"
        # to prevent recursive alerting on persistent SMTP failure.
        if emit_structured:
            try:
                logging.getLogger("tothbot").info(log_record({
                    "event":             "ALERT_SMTP_FAILED",
                    "level":             "HIGH",
                    "component":         "LOGGER",
                    "to":                ALERT_EMAIL,
                    "subject":           effective_subject,
                    "triggering_event":  triggering_event,
                    "error":             str(exc),
                }))
            except Exception:  # noqa: broad — ultimate fallback is stderr
                pass


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
            # Direct alert bypasses the Logger queue (LG-ALERT-004).
            # emit_structured=False: queue is full, structured emit
            # would re-enter the same put_nowait path and fail again.
            _alert_operator_direct(
                "CRITICAL: Logger queue full. Log records are being dropped. "
                "Investigate immediately. TothBot may be data-blind.",
                subject="[TothBot ALERT] LOG_QUEUE_FULL",
                triggering_event="LOG_QUEUE_FULL",
                emit_structured=False,
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

    dv1_4 (AI-2): events with event=="ALERT_SMTP_FAILED" are skipped
    so that a persistent SMTP outage cannot self-trigger recursively.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message_str = record.getMessage()
            parsed: dict[str, Any] = orjson.loads(message_str)
            level_field: str = parsed.get("level", "")
            event_field: str = parsed.get("event", "UNKNOWN_EVENT")

            if level_field not in ALERT_LEVELS:
                return

            # Anti-recursion guard — ALERT_SMTP_FAILED exists precisely
            # because alerting itself has broken; do not attempt to
            # alert on the alert-failure record.
            if event_field == "ALERT_SMTP_FAILED":
                return

            component = parsed.get("component", "UNKNOWN")
            subject = f"[{level_field}] {event_field} | {component}"
            _alert_operator_direct(
                f"[{level_field}] {event_field} | {component} | "
                f"{message_str[:500]}",
                subject=subject,
                triggering_event=event_field,
                emit_structured=True,
            )
        except Exception as exc:  # noqa: broad — alert handler must not crash
            print(
                f"[TothBotAlertHandler.emit ERROR ({exc})]: "
                f"{record.getMessage()[:200]}",
                file=sys.stderr,
                flush=True,
            )


# =============================================================
# TothBotTradeRecordHandler — Background Thread (in QueueListener)
# OI-023 / AI-1 — LG-TREC-001 through LG-TREC-005 (1011007 dv1_4 S11)
# NOT on the hot path. Runs in QueueListener's background thread.
# =============================================================

class TothBotTradeRecordHandler(logging.Handler):
    """
    Append TRADE_CLOSE records to /home/tothbot/records/trades_<YYYY>.jsonl.

    Attached to QueueListener alongside file_handler and alert_handler.
    Runs in the QueueListener background thread — NEVER on the hot
    path. HR-LG-001 is preserved by the existing queue architecture:
    the hot path does put_nowait() only; this handler executes after
    the record has been dequeued by the QueueListener thread.

    Contract:
      (a) Filter: event == "TRADE_CLOSE" only. All other events skipped.
      (b) Target: /home/tothbot/records/trades_<YYYY>.jsonl where
          <YYYY> is the current UTC calendar year at time of write
          (per 1011007 dv1_4 LG-TREC-002 and TB00107 Section 3.6).
      (c) Format: JSONL — append the exact pre-serialized NDJSON line
          already formed by log_record(), plus "\\n". Identical to
          tothbot.log payload (TB00107 §3.6 FP/DP lock).
      (d) Permanence: mode "a" only, no rotation, no size cap.
          File created on first TRADE_CLOSE of the year with
          permissions 0o644 (hard-pinned via os.open, independent
          of runtime umask).
      (e) Durability: os.fsync after each write. Compliance-grade
          records must survive VPS power loss / kernel panic.
          fsync cost is negligible because TRADE_CLOSE frequency
          is low (few per day in paper-trade throughput) and
          fsync executes in the QueueListener thread, not the
          hot path.
      (f) Independence: Exceptions here are isolated by
          logging.Handler.handleError and do not prevent
          file_handler (tothbot.log) from writing the same record.
          tothbot.log remains an operational fallback if this
          file write fails.
      (g) Failure reporting: On write failure, emit a structured
          TRADE_RECORD_WRITE_FAILED (HIGH) event via the standard
          Logger path. That event lands in tothbot.log
          (diagnostic forensics) AND triggers operator email
          (via alert_handler), per LG-ALERT-002. Stderr used as
          ultimate fallback. Never raises.
    """

    def emit(self, record: logging.LogRecord) -> None:
        path: str = ""
        try:
            message_str = record.getMessage()
            parsed: dict[str, Any] = orjson.loads(message_str)

            # LG-TREC-001 — filter: TRADE_CLOSE only.
            if parsed.get("event") != "TRADE_CLOSE":
                return

            # LG-TREC-002 — annual segmentation at time of write
            # (TB00107 §3.6 FP/DP lock: "current UTC calendar year
            # at time of write").
            year = datetime.now(timezone.utc).strftime("%Y")
            path = f"{TRADES_RECORD_DIR}/trades_{year}.jsonl"

            # LG-TREC-003 — hard-pinned 0o644 permissions on create,
            # independent of runtime umask (Deming 6.1).
            fd = os.open(
                path,
                os.O_WRONLY | os.O_APPEND | os.O_CREAT,
                TRADES_RECORD_FILE_MODE,
            )
            try:
                # LG-TREC-004 — single write of JSONL line + newline.
                # Python single write() is atomic for sizes < PIPE_BUF
                # (4096). TRADE_CLOSE records are ~500-800 bytes.
                # Single writer (QueueListener thread) — no lock needed.
                os.write(fd, (message_str + "\n").encode("utf-8"))
                # LG-TREC-005 — fsync for compliance-grade durability
                # against VPS power loss / kernel panic (TB00108
                # decision locked by Bill).
                os.fsync(fd)
            finally:
                os.close(fd)

        except Exception as exc:  # noqa: broad — handler must not raise
            # OI-023 failure contract: emit structured HIGH event and
            # also write to stderr. Do not block tothbot.log write.
            try:
                logging.getLogger("tothbot").info(log_record({
                    "event":     "TRADE_RECORD_WRITE_FAILED",
                    "level":     "HIGH",
                    "component": "LOGGER",
                    "path":      path,
                    "error":     str(exc),
                }))
            except Exception:
                pass
            print(
                f"[TRADE_RECORD_WRITE_FAILED ({exc}) path={path}]: "
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
                f"Risk of overflow. Investigate log write throughput.",
                subject="[TothBot ALERT] LOG_QUEUE_WARN",
                triggering_event="LOG_QUEUE_WARN",
                emit_structured=True,
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

    Architecture (two-stream record — 0511006 dv1_1):
        Hot path:    Module -> logger.info(log_record(d))
                              -> TothBotQueueHandler.enqueue()
                              -> queue.put_nowait()  [returns immediately]

        Background:  QueueListener thread:
                         -> RotatingFileHandler -> tothbot.log (NDJSON)
                         -> TothBotAlertHandler -> SMTP on HIGH/CRITICAL
                         -> TothBotTradeRecordHandler ->
                            trades_<YYYY>.jsonl (TRADE_CLOSE only,
                            append-only, fsync, permanent)
    """
    # LG-FILE-001: Create log directory at startup
    os.makedirs(LOG_DIR, exist_ok=True)

    # OI-023 / AI-1 / LG-TREC-001: Create permanent trade records
    # directory at startup. exist_ok=True so subsequent restarts
    # are idempotent.
    os.makedirs(TRADES_RECORD_DIR, exist_ok=True)

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

    # OI-023 / AI-1: Permanent trade record handler — background thread,
    # not hot path. Filters for event=="TRADE_CLOSE" only.
    trade_record_handler = TothBotTradeRecordHandler()

    # LG-QUEUE-004: QueueListener — background thread.
    # Dispatches each record to ALL handlers. logging.Handler.handleError
    # isolates per-handler exceptions, so a failing trade_record_handler
    # cannot prevent file_handler from writing to tothbot.log.
    # respect_handler_level=True: honours per-handler level filters.
    log_listener = logging.handlers.QueueListener(
        log_queue,
        file_handler,
        alert_handler,
        trade_record_handler,
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
# --- Logging a TRADE_CLOSE (Exit Controller) ---
#
#   Exact shape per 1011007 dv1_4 Section 7 (mandatory fields).
#   The record lands in BOTH tothbot.log (diagnostic, rotating)
#   AND /home/tothbot/records/trades_<YYYY>.jsonl (permanent).
#
#   logger.info(log_record({
#       "event":              "TRADE_CLOSE",
#       "level":              "INFO",
#       "component":          "EXIT_CTRL",
#       "symbol":             "BTC/USD",
#       "entry_fill_price":   Decimal("42350.1"),
#       "exit_price":         Decimal("42720.5"),
#       # ... remaining mandatory fields per 1011007 S7 ...
#   }))
#
# --- Shutdown (in exit_controller / startup_sequence.py) ---
#
#   log_listener.stop()   # drains queue, joins background thread
#   file_handler.close()  # caller must retain file_handler reference
#
