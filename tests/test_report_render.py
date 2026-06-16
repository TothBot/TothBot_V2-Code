"""contract:Operator_Reporting_Hierarchy - the C2-C6 PULL report RENDER + EMIT surface (TB00753).

Exercises the render layer (tothbot/recorder/report_render.py) over the structured OperatorReport the
VIEW layer produces: the deterministic Decimal-as-string body (per-module + combined + the FP5 labels),
and the PULL trigger service (build + render + EMIT through an injected sink, distinct from the C1 push).
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from tothbot.ciats.conductor import ApprovalRequested, DeferredCandidate
from tothbot.ciats.parameter_store import ParameterStore
from tothbot.ciats.pdca_engine import CheckResult
from tothbot.execution.exit_controller import ExitReason, TradeClose
from tothbot.recorder.logger import Logger
from tothbot.recorder.report_render import (
    PullReportService,
    RenderedReport,
    render_operator_report,
)
from tothbot.recorder.reporting import ReportCategory, build_operator_report

JUN = "2026-06-15T12:00:00+00:00"
MAY = "2026-05-10T12:00:00+00:00"
AS_OF = datetime(2026, 6, 15, 18, 0, tzinfo=timezone.utc)


def _tc(*, net, when, regime="TRENDING_POS_NORMAL", rr="1.6", fees="2", symbol="BTC/USD"):
    n = Decimal(net)
    win = n > 0
    return TradeClose(
        symbol=symbol, entry_fill_price=Decimal("60000"), exit_price=Decimal("66000"),
        exit_reason=ExitReason.HTF_REGIME_REVERSAL,
        fees_entry_usd=Decimal("0"), fees_exit_usd=Decimal("0"), fees_total_usd=Decimal(fees),
        net_pl_usd=n, net_gain_usd=(n if win else Decimal("0")),
        net_loss_usd=(Decimal("0") if win else -n), asset_regime=regime,
        exit_timestamp_utc=when, actual_rr=(Decimal(rr) if rr is not None else None),
    )


def _check(passed):
    return CheckResult(
        passed=passed, mw_z=Decimal("1.1"), mw_crit=Decimal("2.326348"),
        sharpe_candidate=Decimal("0.2"), sharpe_baseline=Decimal("0.4"),
        sharpe_improved=False, spearman=None,
    )


def _logger_with(long_records, short_records=()):
    logger = Logger()
    for rec in long_records:
        logger.record(rec, module="long")
    for rec in short_records:
        logger.record(rec, module="short")
    return logger


# ----------------------------------------------------------------------------- the render layer
def test_render_produces_a_deterministic_decimal_body():
    logger = _logger_with([_tc(net="-10", when=JUN), _tc(net="20", when=JUN)])
    report = build_operator_report(
        logger, {"long": ParameterStore(), "short": ParameterStore()},
        category=ReportCategory.C4_MONTHLY, as_of=AS_OF)
    rendered = render_operator_report(report)
    assert isinstance(rendered, RenderedReport)
    assert rendered.code == "C4" and rendered.cadence == "monthly"
    # the subject names the category + cadence + the pull instant.
    assert "C4 MONTHLY" in rendered.subject and AS_OF.isoformat() in rendered.subject
    # Decimal-as-string, never a float repr; the combined net P/L is 10.
    assert "net P/L: 10 USD" in rendered.body
    assert "module: LONG" in rendered.body and "module: SHORT" in rendered.body
    # determinism: rendering the same report twice is byte-identical.
    assert render_operator_report(report).body == rendered.body


def test_render_surfaces_the_fp5_validity_label():
    # below the 200-trade floor -> the monitoring-only label is surfaced verbatim, not dropped.
    logger = _logger_with([_tc(net="5", when=JUN)])
    report = build_operator_report(
        logger, {"long": ParameterStore()}, category=ReportCategory.C4_MONTHLY, as_of=AS_OF)
    body = render_operator_report(report).body
    assert "insufficient-data 1-of-200 (FP5)" in body


def test_render_includes_reported_theories_and_proposals_and_evolution():
    t1 = _tc(net="-10", when=JUN)
    check = _check(False)
    deferred = DeferredCandidate(candidate=SimpleNamespace(level_key="ema_9", rho=Decimal("0.4"), n=200),
                                 reason="level cannot re-decide a period")
    proposal = SimpleNamespace(param_name="mae_mult", current_value=Decimal("1.5"),
                               proposed_value=Decimal("1.35"), rationale="heat predicts loss")
    approval = ApprovalRequested(request_id=1, proposal=proposal, check=check, kind="pdca")
    logger = Logger()
    logger.record(t1, module="long")
    logger.record(check)
    logger.record(deferred)
    logger.record(approval)
    store = ParameterStore(initial={"mae_mult": Decimal("1.5")})
    store.apply(SimpleNamespace(proposal=SimpleNamespace(
        param_name="mae_mult", proposed_value=Decimal("1.35"))), at_trade_count=1)

    report = build_operator_report(
        logger, {"long": store}, category=ReportCategory.C4_MONTHLY, as_of=AS_OF)
    body = render_operator_report(report).body
    # the disproven theory's CHECK statistics are rendered (mw_z vs crit, Sharpe).
    assert "CHECK failed: mw_z 1.1 vs crit 2.326348" in body
    # the deferred candidate's level_key + reason.
    assert "ema_9" in body and "level cannot re-decide a period" in body
    # the proposed change old -> new.
    assert "mae_mult: 1.5 -> 1.35" in body and "heat predicts loss" in body
    # the parameter-evolution log old -> new @ the trade count.
    assert "mae_mult: 1.5 -> 1.35 @ trade 1" in body


def test_c5_render_includes_the_tax_projection():
    logger = _logger_with([_tc(net="10", when=JUN)])
    report = build_operator_report(
        logger, {"long": ParameterStore()}, category=ReportCategory.C5_ANNUAL, as_of=AS_OF)
    body = render_operator_report(report).body
    assert "C5 tax projection" in body and "BTC/USD" in body


# ----------------------------------------------------------------------------- the PULL trigger + emit
def test_pull_service_builds_renders_and_emits_through_the_injected_sink():
    logger = _logger_with([_tc(net="-10", when=JUN), _tc(net="20", when=JUN)])
    emitted = []
    service = PullReportService(
        logger, {"long": ParameterStore(), "short": ParameterStore()}, emit=emitted.append)
    rendered = service.pull(ReportCategory.C4_MONTHLY, AS_OF)
    # the emit seam received exactly the rendered report.
    assert emitted == [rendered]
    assert isinstance(rendered, RenderedReport) and rendered.code == "C4"
    assert "net P/L: 10 USD" in rendered.body


def test_pull_service_does_not_raise_a_c1_alert():
    # the periodic PULL path is DISTINCT from the C1 IMMEDIATE push: emitting a pull report must NOT
    # route anything to mod:Logger.alerts (the SMTP seam) - it goes only to the injected emit sink.
    logger = _logger_with([_tc(net="-10", when=JUN)])
    before = list(logger.alerts)
    emitted = []
    PullReportService(logger, {"long": ParameterStore()}, emit=emitted.append).pull(
        ReportCategory.C4_MONTHLY, AS_OF)
    assert logger.alerts == before        # no C1 alert was raised by the pull
    assert len(emitted) == 1              # the report went to the pull sink only


def test_pull_service_with_no_sink_still_returns_the_rendered_report():
    logger = _logger_with([_tc(net="5", when=JUN)])
    rendered = PullReportService(logger, {"long": ParameterStore()}).pull(
        ReportCategory.C2_DAILY, AS_OF)
    assert isinstance(rendered, RenderedReport) and rendered.code == "C2"
