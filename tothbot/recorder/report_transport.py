"""contract:Operator_Reporting_Hierarchy - the C2-C6 PULL report REAL EMIT TRANSPORT + CADENCE SCHEDULER.

Source: 0500000 dv1_251 sec 7 mod:Logger desc `reporting_hierarchy` (contract:Operator_Reporting_
Hierarchy) + rule:HR-RPT-001/002/003. The report VIEW layer (recorder/reporting.py, TB00752) produces
the STRUCTURED content; the render layer (recorder/report_render.py, TB00753) renders it to the
operator-facing body and EMITS it through an INJECTED sink. THIS module is the delivery edge + the
cadence trigger: it wires the injected emit sink to the ACTUAL operator surface (the periodic-pull
retrieval over SMTP / the C2 operational dashboard) and fires the PULL on each category's cadence.

THE PULL TRACK IS DISTINCT FROM THE C1 IMMEDIATE PUSH. The C1 set (rule:HR-RPT-001) is the
mod:Logger.alert -> SMTP alert seam: a PROFITABLE tested theory is PUSHED there immediately. The C2-C6
reports are PULLED - retrieved/scheduled - and an UNPROFITABLE reported theory rides them, NEVER a C1
alert. So this transport NEVER touches logger.alert; it carries the X-TothBot-Track: periodic-pull
marker so the operator surface can tell the two tracks apart.

The low-level send/write edge is INJECTED (mirroring the C1 SMTP seam - the socket is the process edge,
bound at cold-start assembly) so the message construction + the cadence logic stay unit-testable without
I/O. The cadence is driven by a DETERMINISTIC injected clock (the OHLC_5m_System_Clock tick or a
UTC-calendar tick); the cadence anchors are already in recorder/reporting.report_window.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Callable

from .report_render import PullReportService, RenderedReport, ReportSink
from .reporting import ReportCategory, report_window

# The low-level SMTP send edge, mirroring smtplib.SMTP.sendmail(from_addr, to_addrs, message). Injected
# so the transport is testable without a socket; in the assembled organism it is bound to the real SMTP.
SmtpSend = Callable[[str, Sequence[str], str], None]
# The low-level dashboard/file write edge (the C2 operational dashboard surface). Injected likewise.
DashboardWrite = Callable[[str], None]


# ============================================================================ the SMTP transport edge
class SmtpReportTransport:
    """The REAL emit transport for the C2-C6 PULL track: a RenderedReport -> an email message -> the
    INJECTED low-level SMTP send. Used as the PullReportService `emit` sink (it is callable). Mirrors
    the C1 alert SMTP seam but on the DISTINCT pull track: it NEVER calls mod:Logger.alert, and the
    message carries X-TothBot-Track: periodic-pull so the operator surface separates it from the C1
    IMMEDIATE push. The socket-level send is injected (no I/O in tests)."""

    def __init__(
        self,
        send: SmtpSend,
        *,
        sender: str,
        recipients: Sequence[str],
    ) -> None:
        self._send = send
        self._sender = sender
        self._recipients = tuple(recipients)

    def build_message(self, rendered: RenderedReport) -> str:
        """Construct the RFC-822 message for a rendered report: the From/To/Subject headers + the
        track markers + the body verbatim (the Decimal-as-string operator body, never re-wrapped so
        the numbers stay intact). DETERMINISTIC: the same rendered report builds the same message."""
        headers = [
            f"From: {self._sender}",
            f"To: {', '.join(self._recipients)}",
            f"Subject: {rendered.subject}",
            "MIME-Version: 1.0",
            'Content-Type: text/plain; charset="utf-8"',
            f"X-TothBot-Report: {rendered.code}",          # the category code (for operator filtering)
            f"X-TothBot-Cadence: {rendered.cadence}",
            "X-TothBot-Track: periodic-pull",              # DISTINCT from the C1 immediate push
        ]
        return "\r\n".join(headers) + "\r\n\r\n" + rendered.body

    def __call__(self, rendered: RenderedReport) -> None:
        """Deliver the rendered report over the injected SMTP send (the periodic-pull retrieval edge).
        NEVER a C1 alert - this is the distinct pull track."""
        self._send(self._sender, self._recipients, self.build_message(rendered))


# =================================================================== the real smtplib socket binding
def smtplib_send(
    host: str,
    port: int = 0,
    *,
    smtp_factory: Callable[[str, int], object] | None = None,
    starttls: bool = False,
    username: str | None = None,
    password: str | None = None,
) -> SmtpSend:
    """Build the LOW-LEVEL SmtpSend bound to a real smtplib.SMTP server - the actual operator delivery
    edge for the periodic-pull track (mirroring the C1 alert SMTP seam, on the DISTINCT pull track).
    Each send opens a connection, optionally STARTTLS + logs in, sendmails the RFC-822 message, and
    quits. `smtp_factory` defaults to smtplib.SMTP (lazy-imported at the edge, never at module load -
    the same pattern transport.py uses for websockets); a unit test injects a fake factory so NO
    socket opens. This is the process edge bound at cold-start assembly; the message construction +
    the cadence stay I/O-free in SmtpReportTransport / PullCadenceScheduler."""

    def send(from_addr: str, to_addrs: Sequence[str], message: str) -> None:
        factory = smtp_factory
        if factory is None:
            import smtplib  # lazy: the socket library is the edge, not a module-load dependency

            factory = smtplib.SMTP
        client = factory(host, port)
        try:
            if starttls:
                client.starttls()
            if username is not None:
                client.login(username, password)
            client.sendmail(from_addr, list(to_addrs), message)
        finally:
            client.quit()

    return send


# ======================================================================= the dashboard/file transport
class DashboardReportTransport:
    """The REAL emit transport for the C2 operational dashboard / a file surface: a RenderedReport ->
    its body -> the INJECTED low-level write. Used as the PullReportService `emit` sink (it is
    callable). The write edge is injected (no I/O in tests); in the assembled organism it is bound to
    the dashboard write / a durable file append."""

    def __init__(self, write: DashboardWrite) -> None:
        self._write = write

    def __call__(self, rendered: RenderedReport) -> None:
        self._write(rendered.body)


# ================================================================================ fan-out to N sinks
def fan_out(*sinks: ReportSink) -> ReportSink:
    """Compose several emit sinks into one (e.g. SMTP + the dashboard). Each rendered report is
    delivered to every sink in order. Used to wire one PullReportService emit to many operator
    surfaces at once."""

    def emit(rendered: object) -> None:
        for sink in sinks:
            sink(rendered)

    return emit


# ===================================================================== the PULL cadence scheduler
def _period_key(category: ReportCategory, now: datetime) -> datetime:
    """The cadence bucket `now` falls in - the anchor the scheduler watches for a rollover. For the
    calendar categories (C2-C5) it is the report_window start (the calendar day / Monday-week / month
    / year). The C6 rolling-12mo report has no calendar boundary of its own (its window is the
    trailing 12 months), so it is PULLED on the MONTHLY cadence - its bucket is the calendar month."""
    if category is ReportCategory.C6_ROLLING_12MO:
        return report_window(ReportCategory.C4_MONTHLY, now).start
    return report_window(category, now).start


class PullCadenceScheduler:
    """The PULL CADENCE SCHEDULER: fires PullReportService.pull on each category's cadence (C2 daily /
    C3 weekly / C4 monthly / C5 annual / C6 rolling-12mo monthly) off a DETERMINISTIC injected clock.

    Drive it with tick(now) from the time edge (the OHLC_5m_System_Clock close or a UTC-calendar
    tick). On each tick, for every scheduled category, if the cadence bucket has rolled over since the
    last tick, the JUST-COMPLETED period is reported: the scheduler calls service.pull(category,
    as_of=<the completed period anchor>) - which builds + renders + EMITS that period's report through
    the wired transport. The FIRST observation of a category establishes the baseline (no spurious
    startup fire). DETERMINISTIC: the same tick sequence fires the same reports.

    The scheduler assumes ticks at least once per the shortest scheduled cadence (true for the 5m
    system clock vs a daily/weekly/monthly cadence); it reports the most recently completed period on a
    rollover and does not back-fill a period skipped entirely between two distant ticks."""

    def __init__(
        self,
        service: PullReportService,
        categories: Sequence[ReportCategory],
        *,
        on_fire: Callable[[ReportCategory, RenderedReport], None] | None = None,
    ) -> None:
        self._service = service
        self._categories = tuple(categories)
        self._on_fire = on_fire
        self._last_key: dict[ReportCategory, datetime] = {}

    def tick(self, now: datetime) -> list[tuple[ReportCategory, RenderedReport]]:
        """Advance the scheduler to the clock instant `now`. Fire the pull for any category whose
        cadence bucket rolled over since the last tick (reporting the just-completed period), emitting
        each through the service's wired transport. Returns the (category, RenderedReport) pairs fired
        on this tick (empty when no bucket rolled). The first time a category is seen, its baseline is
        recorded and nothing fires."""
        fired: list[tuple[ReportCategory, RenderedReport]] = []
        for category in self._categories:
            key = _period_key(category, now)
            prev = self._last_key.get(category)
            if prev is None:
                self._last_key[category] = key
                continue
            if key > prev:
                # the bucket rolled: `prev` is the period that just ended - report it as of its anchor
                # (which lies inside its own window, so report_window lands the pull on that period).
                rendered = self._service.pull(category, prev)
                fired.append((category, rendered))
                if self._on_fire is not None:
                    self._on_fire(category, rendered)
                self._last_key[category] = key
        return fired
