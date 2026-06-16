"""contract:Operator_Reporting_Hierarchy - the C2-C6 PULL REAL EMIT TRANSPORT + CADENCE SCHEDULER.

Exercises the delivery edge (tothbot/recorder/report_transport.py): the SMTP transport (message
construction + the injected socket send, the periodic-pull track marker), the dashboard/file transport,
the fan-out, and the cadence scheduler (the deterministic injected-clock trigger that fires the pull on
each category's cadence, reporting the just-completed period). The low-level send/write is injected so
the whole layer is unit-testable without I/O.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal

from tothbot.ciats.parameter_store import ParameterStore
from tothbot.execution.exit_controller import ExitReason, TradeClose
from tothbot.recorder.logger import Logger
from tothbot.recorder.report_render import PullReportService, RenderedReport
from tothbot.recorder.report_transport import (
    DashboardReportTransport,
    PullCadenceScheduler,
    SmtpReportTransport,
    fan_out,
)
from tothbot.recorder.reporting import ReportCategory, report_window

UTC = timezone.utc


def _rendered(category=ReportCategory.C4_MONTHLY, *, body="BODY-LINE net P/L: -200 USD"):
    as_of = datetime(2026, 6, 30, 12, 0, tzinfo=UTC)
    return RenderedReport(
        category=category, code=category.code, cadence=category.cadence, as_of=as_of,
        subject=f"TothBot {category.code} {category.cadence.upper()} - X (as of {as_of.isoformat()})",
        body=body,
    )


# ----------------------------------------------------------------------------- the SMTP transport
def test_smtp_transport_builds_a_message_and_calls_the_injected_send():
    sent: list = []
    transport = SmtpReportTransport(
        send=lambda frm, to, msg: sent.append((frm, to, msg)),
        sender="tothbot@toth.bot", recipients=["wstothjr@gmail.com"])
    rendered = _rendered()
    transport(rendered)

    assert len(sent) == 1
    frm, to, msg = sent[0]
    assert frm == "tothbot@toth.bot" and to == ("wstothjr@gmail.com",)
    # the headers name the operator, the subject, and the category.
    assert "From: tothbot@toth.bot" in msg and "To: wstothjr@gmail.com" in msg
    assert f"Subject: {rendered.subject}" in msg
    assert "X-TothBot-Report: C4" in msg and "X-TothBot-Cadence: monthly" in msg
    # the PULL track marker - DISTINCT from the C1 immediate push.
    assert "X-TothBot-Track: periodic-pull" in msg
    # the body is carried VERBATIM (the Decimal numbers are never re-wrapped).
    assert "net P/L: -200 USD" in msg
    assert msg.endswith(rendered.body)


def test_smtp_message_is_deterministic():
    transport = SmtpReportTransport(send=lambda *a: None, sender="a@b", recipients=["c@d", "e@f"])
    rendered = _rendered()
    assert transport.build_message(rendered) == transport.build_message(rendered)
    # multiple recipients are joined in the To header.
    assert "To: c@d, e@f" in transport.build_message(rendered)


def test_smtp_transport_is_a_usable_pull_emit_sink():
    # the transport is callable, so it plugs straight into PullReportService(emit=...).
    sent: list = []
    transport = SmtpReportTransport(
        send=lambda frm, to, msg: sent.append(msg), sender="t@b", recipients=["w@g"])
    logger = Logger()
    logger.record(_close("10", "2026-06-10T12:00:00+00:00"), module="long")
    service = PullReportService(
        logger, {"long": ParameterStore(), "short": ParameterStore()}, emit=transport)
    rendered = service.pull(ReportCategory.C4_MONTHLY, datetime(2026, 6, 15, 18, 0, tzinfo=UTC))

    assert len(sent) == 1 and rendered.subject in sent[0]
    # the periodic pull NEVER raises a C1 alert (the transport is the distinct pull track).
    assert logger.alerts == []


# ------------------------------------------------------------------------- the dashboard transport
def test_dashboard_transport_writes_the_body():
    written: list = []
    transport = DashboardReportTransport(write=written.append)
    rendered = _rendered(body="dashboard body")
    transport(rendered)
    assert written == ["dashboard body"]


def test_fan_out_delivers_to_every_sink():
    a: list = []
    b: list = []
    emit = fan_out(a.append, b.append)
    rendered = _rendered()
    emit(rendered)
    assert a == [rendered] and b == [rendered]


# --------------------------------------------------------------------------- the cadence scheduler
class _StubService:
    """A PullReportService stand-in that records (category, as_of) calls and returns a marker report
    (so the scheduler's cadence logic is tested in isolation from the VIEW/render)."""

    def __init__(self) -> None:
        self.calls: list = []

    def pull(self, category, as_of, **_kw):
        self.calls.append((category, as_of))
        return RenderedReport(
            category=category, code=category.code, cadence=category.cadence, as_of=as_of,
            subject=f"{category.code}@{as_of.isoformat()}", body="x")


def test_scheduler_baseline_does_not_fire_on_first_observation():
    svc = _StubService()
    sched = PullCadenceScheduler(svc, [ReportCategory.C4_MONTHLY])
    assert sched.tick(datetime(2026, 6, 15, 12, 0, tzinfo=UTC)) == []
    assert svc.calls == []


def test_scheduler_does_not_fire_within_the_same_period():
    svc = _StubService()
    sched = PullCadenceScheduler(svc, [ReportCategory.C2_DAILY])
    sched.tick(datetime(2026, 6, 15, 8, 0, tzinfo=UTC))            # baseline
    assert sched.tick(datetime(2026, 6, 15, 20, 0, tzinfo=UTC)) == []  # same calendar day
    assert svc.calls == []


def test_scheduler_fires_the_completed_day_on_a_daily_rollover():
    svc = _StubService()
    sched = PullCadenceScheduler(svc, [ReportCategory.C2_DAILY])
    sched.tick(datetime(2026, 6, 15, 12, 0, tzinfo=UTC))           # baseline = June 15
    fired = sched.tick(datetime(2026, 6, 16, 0, 5, tzinfo=UTC))    # rolled into June 16
    assert [c for c, _ in fired] == [ReportCategory.C2_DAILY]
    # the COMPLETED period (June 15) is reported: the pull as_of is June 15 00:00 (inside its window).
    (_cat, as_of), = svc.calls
    assert as_of == datetime(2026, 6, 15, 0, 0, tzinfo=UTC)
    assert report_window(ReportCategory.C2_DAILY, as_of).start == as_of


def test_scheduler_fires_the_completed_month_on_a_monthly_rollover():
    svc = _StubService()
    sched = PullCadenceScheduler(svc, [ReportCategory.C4_MONTHLY])
    sched.tick(datetime(2026, 6, 15, 12, 0, tzinfo=UTC))           # baseline = June
    sched.tick(datetime(2026, 6, 30, 23, 0, tzinfo=UTC))           # still June -> no fire
    fired = sched.tick(datetime(2026, 7, 1, 0, 1, tzinfo=UTC))     # rolled into July
    assert [c for c, _ in fired] == [ReportCategory.C4_MONTHLY]
    (_cat, as_of), = svc.calls
    assert as_of == datetime(2026, 6, 1, 0, 0, tzinfo=UTC)         # June reported
    assert report_window(ReportCategory.C4_MONTHLY, as_of) == report_window(
        ReportCategory.C4_MONTHLY, datetime(2026, 6, 20, tzinfo=UTC))


def test_scheduler_c6_fires_on_the_monthly_cadence_with_a_trailing_window():
    svc = _StubService()
    sched = PullCadenceScheduler(svc, [ReportCategory.C6_ROLLING_12MO])
    sched.tick(datetime(2026, 6, 15, 12, 0, tzinfo=UTC))           # baseline (June bucket)
    fired = sched.tick(datetime(2026, 7, 1, 0, 1, tzinfo=UTC))     # rolled into July -> fires
    assert [c for c, _ in fired] == [ReportCategory.C6_ROLLING_12MO]
    (_cat, as_of), = svc.calls
    assert as_of == datetime(2026, 6, 1, 0, 0, tzinfo=UTC)
    # the C6 window is the trailing 12 months ending at the completed monthly anchor (rolling).
    w = report_window(ReportCategory.C6_ROLLING_12MO, as_of)
    assert w.end == as_of and w.start == datetime(2025, 6, 1, 0, 0, tzinfo=UTC)


def test_scheduler_fires_multiple_categories_on_a_shared_boundary():
    svc = _StubService()
    cats = [ReportCategory.C2_DAILY, ReportCategory.C4_MONTHLY, ReportCategory.C5_ANNUAL]
    sched = PullCadenceScheduler(svc, cats)
    sched.tick(datetime(2026, 12, 31, 12, 0, tzinfo=UTC))          # baseline (day/month/year all set)
    fired = sched.tick(datetime(2027, 1, 1, 0, 1, tzinfo=UTC))     # day + month + year all rolled
    assert {c for c, _ in fired} == set(cats)
    # each reports its own completed period anchor.
    by_cat = dict(svc.calls)
    assert by_cat[ReportCategory.C2_DAILY] == datetime(2026, 12, 31, 0, 0, tzinfo=UTC)
    assert by_cat[ReportCategory.C4_MONTHLY] == datetime(2026, 12, 1, 0, 0, tzinfo=UTC)
    assert by_cat[ReportCategory.C5_ANNUAL] == datetime(2026, 1, 1, 0, 0, tzinfo=UTC)


def test_scheduler_is_deterministic_over_the_same_tick_sequence():
    ticks = [datetime(2026, 6, 15, tzinfo=UTC) + timedelta(hours=12 * i) for i in range(6)]
    runs = []
    for _ in range(2):
        svc = _StubService()
        sched = PullCadenceScheduler(svc, [ReportCategory.C2_DAILY])
        for t in ticks:
            sched.tick(t)
        runs.append(list(svc.calls))
    assert runs[0] == runs[1] and len(runs[0]) >= 2     # at least 2 day-boundaries crossed


def _close(net, when):
    n = Decimal(net)
    win = n > 0
    return TradeClose(
        symbol="BTC/USD", entry_fill_price=Decimal("60000"), exit_price=Decimal("66000"),
        exit_reason=ExitReason.HTF_REGIME_REVERSAL,
        fees_entry_usd=Decimal("0"), fees_exit_usd=Decimal("0"), fees_total_usd=Decimal("1"),
        net_pl_usd=n, net_gain_usd=(n if win else Decimal("0")),
        net_loss_usd=(Decimal("0") if win else -n), asset_regime="TRENDING_POS_NORMAL",
        exit_timestamp_utc=when, actual_rr=(Decimal("1.6") if win else Decimal("-1")),
    )
