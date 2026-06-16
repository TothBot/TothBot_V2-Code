"""rule:HR-LG-009 - the C1 IMMEDIATE operator-alert SMTP seam (recorder/alert_transport.py).

Exercises AlertEmailSender: the bounded-retry direct send (success / retry-then-success / total
failure), evt:ALERT_SENT on success + evt:ALERT_SMTP_FAILED CRITICAL ONCE on total failure, the
rule:LG-ALERT-005 anti-recursion skip, and the message construction. The low-level send + sleep are
injected (no socket, no real sleep).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from tothbot.recorder.alert_transport import (
    AlertEmailSender,
    AlertSent,
    AlertSmtpFailed,
)


@dataclass(frozen=True)
class _Critical:
    """A stand-in CRITICAL alert record (e.g. evt:FULL_HALT_TRIGGERED)."""

    drawdown_pct: str
    level: str = field(default="CRITICAL", init=False)
    code: str = field(default="FULL_HALT_TRIGGERED", init=False)


def _sender(send, *, events=None, stderr=None, sleeps=None, max_attempts=2):
    return AlertEmailSender(
        send, sender="tothbot@tothbot.com", recipients=["alerts@tothbot.com", "wstothjr@gmail.com"],
        on_event=(events.append if events is not None else None),
        stderr_write=(stderr.append if stderr is not None else (lambda _s: None)),
        sleep=(sleeps.append if sleeps is not None else (lambda _s: None)),
        max_attempts=max_attempts, backoff_sec=1,
    )


def test_alert_send_success_first_attempt_emits_alert_sent():
    sent, events = [], []
    _sender(lambda frm, to, msg: sent.append((frm, to, msg)), events=events)(_Critical("10"))
    assert len(sent) == 1
    frm, to, msg = sent[0]
    assert frm == "tothbot@tothbot.com" and to == ("alerts@tothbot.com", "wstothjr@gmail.com")
    assert "Subject: TothBot C1 ALERT - FULL_HALT_TRIGGERED" in msg
    assert "X-TothBot-Track: c1-immediate" in msg            # the C1 push track marker
    assert "drawdown_pct: 10" in msg                          # the record's fields in the body
    assert len(events) == 1 and isinstance(events[0], AlertSent)
    assert events[0].triggering_event == "FULL_HALT_TRIGGERED"


def test_alert_send_retries_then_succeeds():
    calls, events, sleeps = [], [], []

    def send(frm, to, msg):
        calls.append(1)
        if len(calls) == 1:
            raise OSError("connection refused")              # first attempt fails

    _sender(send, events=events, sleeps=sleeps)(_Critical("10"))
    assert len(calls) == 2                                    # retried once -> succeeded
    assert sleeps == [1]                                      # one backoff sleep between attempts
    assert len(events) == 1 and isinstance(events[0], AlertSent)


def test_alert_total_failure_emits_one_critical_and_stderr():
    calls, events, stderr, sleeps = [], [], [], []

    def send(frm, to, msg):
        calls.append(1)
        raise OSError("smtp down")

    _sender(send, events=events, stderr=stderr, sleeps=sleeps)(_Critical("10"))
    assert len(calls) == 2                                    # ALERT_SMTP_MAX_ATTEMPTS
    assert sleeps == [1]                                      # backoff between the two attempts
    # exactly ONE evt:ALERT_SMTP_FAILED CRITICAL (one event per alert, not per attempt).
    assert len(events) == 1 and isinstance(events[0], AlertSmtpFailed)
    assert events[0].level == "CRITICAL" and "smtp down" in events[0].error
    assert stderr and "ALERT_SMTP_FAILED" in stderr[0]        # the ultimate fallback surface


def test_lg_alert_005_anti_recursion_skips_a_failed_alert_record():
    # rule:LG-ALERT-005: a persistent SMTP outage routes its own evt:ALERT_SMTP_FAILED [CRITICAL] back
    # through the Logger -> on_alert; the sender MUST skip it (no self-trigger, no infinite loop).
    sent = []
    _sender(lambda *a: sent.append(a))(
        AlertSmtpFailed(to="x", subject="s", triggering_event="X", error="e")
    )
    assert sent == []                                         # no send attempted


def test_alert_message_is_deterministic():
    s = _sender(lambda *a: None)
    assert s.build_message("S", "B") == s.build_message("S", "B")
    assert "To: alerts@tothbot.com, wstothjr@gmail.com" in s.build_message("S", "B")
