"""mod:Logger rule:HR-LG-009 - the C1 IMMEDIATE operator-alert SMTP seam (the PUSH track).

Source: 0500000 dv1_251 sec 7 mod:Logger desc q1_do "SMTP ALERT DELIVERY CONTRACT [rule:HR-LG-009]" +
event_registry (evt:ALERT_SENT [INFO], evt:ALERT_SMTP_FAILED [CRITICAL]) + rule:HR-LG-011/012 +
rule:LG-ALERT-005 + contract:Operator_Reporting_Hierarchy (C1 IMMEDIATE push).

This is the C1 IMMEDIATE operator email - DISTINCT from the C2-C6 periodic PULL track (report_transport.
py). The contract (built as the diagram literal):
  - a DIRECT SMTP send to the operator that BYPASSES the mod:Logger queue (avoids circularity) and runs
    on the QueueListener background thread (synchronous, NOT the asyncio loop - so the trading hot path
    is never blocked per ar:AR-014 / rule:HR-LG-001).
  - a BOUNDED RETRY LOOP: per-attempt timeout 20s (ALERT_SMTP_TIMEOUT_SEC, on the socket edge);
    ALERT_SMTP_MAX_ATTEMPTS = 2; ALERT_SMTP_RETRY_BACKOFF_SEC = 1 (sleep between attempts on exception);
    worst-case wall time bounded at 41s (20 + 1 + 20).
  - SMTP credentials read from environment variables, NEVER hardcoded (the runner reads them).
  - on success emit evt:ALERT_SENT [INFO] (rule:HR-LG-011); evt:ALERT_SMTP_FAILED [CRITICAL]
    (rule:HR-LG-012) emitted ONCE only if EVERY attempt fails (one event per alert, not per attempt).
  - rule:LG-ALERT-005 ANTI-RECURSION: the alert handler SKIPS an evt:ALERT_SMTP_FAILED record so a
    persistent SMTP outage does not self-trigger; stderr is the ultimate fallback observability surface.

The low-level SMTP send + the sleep are INJECTED (the smtplib edge from report_transport.smtplib_send by
default, carrying the 20s timeout) so the retry policy + the message construction stay unit-testable
without a socket. Used as the mod:Logger on_alert sink (Logger(on_alert=AlertEmailSender(...))): a record
routed to logger.alert / a CRITICAL escalation is PUSHED to the operator here.
"""

from __future__ import annotations

import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field

from .report_transport import SmtpSend

# rule:HR-LG-009 bounded-retry constants (diagram literals; NOT CIATS-tuned seeds - fixed delivery
# policy. Value home: this contract. The 20s timeout lives on the smtplib socket edge).
ALERT_SMTP_TIMEOUT_SEC = 20
ALERT_SMTP_MAX_ATTEMPTS = 2
ALERT_SMTP_RETRY_BACKOFF_SEC = 1

StderrWrite = Callable[[str], None]
AlertEvent = Callable[[object], None]
Sleep = Callable[[float], None]


@dataclass(frozen=True)
class AlertSent:
    """evt:ALERT_SENT [INFO] {to, subject, triggering_event} (rule:HR-LG-011) - the C1 SMTP push
    delivered."""

    to: str
    subject: str
    triggering_event: str
    level: str = field(default="INFO", init=False)
    component: str = field(default="LOGGER", init=False)
    code: str = field(default="ALERT_SENT", init=False)


@dataclass(frozen=True)
class AlertSmtpFailed:
    """evt:ALERT_SMTP_FAILED [CRITICAL] {to, subject, triggering_event, error} (rule:HR-LG-012) -
    EVERY send attempt failed. ALERT-EXEMPT per rule:LG-ALERT-005 (the alert handler skips it, no
    self-trigger). Emitted ONCE per alert."""

    to: str
    subject: str
    triggering_event: str
    error: str
    level: str = field(default="CRITICAL", init=False)
    component: str = field(default="LOGGER", init=False)
    code: str = field(default="ALERT_SMTP_FAILED", init=False)


def _record_code(record: object) -> str:
    return str(getattr(record, "code", None) or getattr(record, "event", None) or type(record).__name__)


class AlertEmailSender:
    """The rule:HR-LG-009 C1 alert send (the mod:Logger on_alert sink). An alert RECORD -> a direct
    SMTP send (bounded retry), emitting evt:ALERT_SENT on success / evt:ALERT_SMTP_FAILED CRITICAL once
    on total failure. rule:LG-ALERT-005 anti-recursion: an evt:ALERT_SMTP_FAILED record is SKIPPED. The
    low-level send + sleep are injected (no socket / no real sleep in tests)."""

    def __init__(
        self,
        send: SmtpSend,
        *,
        sender: str,
        recipients: Sequence[str],
        on_event: AlertEvent | None = None,
        stderr_write: StderrWrite | None = None,
        sleep: Sleep | None = None,
        max_attempts: int = ALERT_SMTP_MAX_ATTEMPTS,
        backoff_sec: float = ALERT_SMTP_RETRY_BACKOFF_SEC,
    ) -> None:
        self._send = send
        self._sender = sender
        self._recipients = tuple(recipients)
        self._on_event = on_event
        self._stderr_write = stderr_write or sys.stderr.write
        self._sleep = sleep or time.sleep
        self._max_attempts = max(1, int(max_attempts))
        self._backoff_sec = backoff_sec

    def build_message(self, subject: str, body: str) -> str:
        """The RFC-822 C1 alert message (the push track marker DISTINCT from the periodic pull)."""
        headers = [
            f"From: {self._sender}",
            f"To: {', '.join(self._recipients)}",
            f"Subject: {subject}",
            "MIME-Version: 1.0",
            'Content-Type: text/plain; charset="utf-8"',
            "X-TothBot-Track: c1-immediate",          # DISTINCT from the periodic-pull track
        ]
        return "\r\n".join(headers) + "\r\n\r\n" + body

    def __call__(self, record: object) -> None:
        """The on_alert sink: PUSH the alert record to the operator over SMTP (bounded retry)."""
        triggering = _record_code(record)
        # rule:LG-ALERT-005: never raise an alert FOR a failed alert (no self-trigger).
        if triggering == "ALERT_SMTP_FAILED":
            return
        subject = f"TothBot C1 ALERT - {triggering}"
        body = self._format(record, triggering)
        message = self.build_message(subject, body)
        last_error: Exception | None = None
        for attempt in range(self._max_attempts):
            try:
                self._send(self._sender, self._recipients, message)
            except Exception as exc:   # delivery failure - retry within the bounded loop
                last_error = exc
                if attempt + 1 < self._max_attempts:
                    self._sleep(self._backoff_sec)
                continue
            self._emit(AlertSent(to=", ".join(self._recipients), subject=subject,
                                 triggering_event=triggering))
            return
        # every attempt failed: stderr fallback + the single CRITICAL event (NOT itself re-alerted).
        err = f"{type(last_error).__name__}: {last_error}"
        self._stderr_write(f"ALERT_SMTP_FAILED to={', '.join(self._recipients)} "
                           f"subject={subject} error={err}\n")
        self._emit(AlertSmtpFailed(to=", ".join(self._recipients), subject=subject,
                                   triggering_event=triggering, error=err))

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    @staticmethod
    def _format(record: object, triggering: str) -> str:
        """A plain-text alert body: the event code + the record's notable fields (0211004 sec-6
        action-statement spirit: event + key values). Decimal-safe via str()."""
        skip = {"code", "event", "level", "component"}
        fields = [
            f"{name}: {value}"
            for name, value in vars(record).items()
            if name not in skip and not name.startswith("_")
        ] if hasattr(record, "__dict__") else []
        lines = [f"event: {triggering}", f"level: {getattr(record, 'level', 'CRITICAL')}", *fields]
        return "\n".join(lines)
