"""contract:Operator_Reporting_Hierarchy - the C2-C6 PULL report RENDER + EMIT surface.

Source: 0500000 dv1_251 sec 7 mod:Logger desc `reporting_hierarchy` (contract:Operator_Reporting_
Hierarchy) + rule:HR-RPT-002/003. The report VIEW layer (recorder/reporting.py, TB00752) produces the
STRUCTURED report CONTENT for any PULL window - the per-module (Long / Short) + combined trade
performance, the REPORTED (disproven) theories, the deferred candidates, the proposed changes, the
windowed parameter-evolution log, the current CIATS values, and the C5 tax projection - a pure FP8 view
over the already-captured record. THIS module is the contract's "implementation coded from this contract
-- NOT preserved as architecture" step: it RENDERS that structured OperatorReport to the operator-facing
form (the email body / dashboard text) and EMITS it on the C2-C6 PULL path.

The render is DETERMINISTIC and Decimal-as-string (ar:AR-047 - never float in an operator surface); a
None metric surfaces as "n/a" and the FP5 validity labels (the 200/600 floors) are surfaced verbatim,
never silently dropped. Per-module sections + the combined roll-up, C4 MONTHLY the priority shape the
others share.

THE PULL PATH IS DISTINCT FROM THE C1 IMMEDIATE PUSH. The C1 set (rule:HR-RPT-001) is the SMTP alert
seam already wired (mod:Logger.alert -> the on_approval edge -> SMTP); a PROFITABLE tested theory is
PUSHED there. The C2-C6 reports are PULLED - operator-invoked / scheduled - and an UNPROFITABLE reported
theory rides them, NOT a C1 alert. So the PULL EMIT goes through an INJECTED sink (testable without I/O),
NOT through logger.alert: rendering + emitting a periodic report NEVER raises a C1 alert.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Callable

from .reporting import (
    ModuleReport,
    OperatorReport,
    ReportCategory,
    TradePerformance,
    build_operator_report,
)

ReportSink = Callable[[object], None]

# The human-readable title per category (the report's evolution/role, the contract's sec-7 names).
_CATEGORY_TITLE: dict[ReportCategory, str] = {
    ReportCategory.C2_DAILY: "Daily Operational Dashboard",
    ReportCategory.C3_WEEKLY: "Weekly Trend-Watch",
    ReportCategory.C4_MONTHLY: "Monthly Evolution Report",
    ReportCategory.C5_ANNUAL: "Annual Compliance & P&L",
    ReportCategory.C6_ROLLING_12MO: "Rolling-12-Month Trajectory",
}


def _s(value: object) -> str:
    """Render a value for the operator surface: a Decimal/None-safe string (never a float repr).
    None -> "n/a"; a Decimal -> its exact str; a datetime -> ISO 8601; anything else -> str()."""
    if value is None:
        return "n/a"
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


# ============================================================================ the rendered artifact
@dataclass(frozen=True)
class RenderedReport:
    """A rendered C2-C6 PULL report ready for the operator surface (the email body / dashboard text).
    `code` + `cadence` identify the category; `subject` is the one-line header; `body` is the
    deterministic Decimal-as-string text. This is what the PULL EMIT sink receives - it is NOT a C1
    alert record (the periodic-pull track is distinct from the C1 IMMEDIATE push)."""

    category: ReportCategory
    code: str
    cadence: str
    as_of: datetime
    subject: str
    body: str


# ============================================================================ the section renderers
def _render_performance(p: TradePerformance, indent: str = "  ") -> list[str]:
    """Render one TradePerformance block (the realized-trade-performance content the contract names).
    Decimal-as-string throughout; the FP5 validity label is surfaced verbatim."""
    lines = [
        f"{indent}trades: {p.trade_count}  (wins {p.wins} / losses {p.losses})",
        f"{indent}net P/L: {_s(p.net_pl_usd)} USD  (gain {_s(p.net_gain_usd)} / loss {_s(p.net_loss_usd)})",
        f"{indent}win rate: {_s(p.win_rate)}  [{p.floor_label}]",
        f"{indent}R:R  avg {_s(p.avg_rr)}  min {_s(p.rr_min)}  median {_s(p.rr_median)}  max {_s(p.rr_max)}",
        f"{indent}best {_s(p.best_trade_usd)}  worst {_s(p.worst_trade_usd)} USD",
        f"{indent}Sharpe: {_s(p.sharpe)}   Half-Kelly: {_s(p.kelly_half)} (full {_s(p.kelly_full)})",
        f"{indent}fees: {_s(p.fees_total_usd)} USD  ({_s(p.fees_pct_of_gross)} of gross)",
    ]
    if p.per_regime:
        lines.append(f"{indent}per-regime:")
        for rp in p.per_regime:
            lines.append(
                f"{indent}  {rp.regime}: {rp.bucket_count} trades, net {_s(rp.net_pl_usd)} USD, "
                f"win rate {_s(rp.win_rate)}"
            )
    return lines


def _render_module(m: ModuleReport) -> list[str]:
    """Render one module's (Long / Short) section: trade performance + the reported theories + the
    deferred candidates + the proposed changes + the parameter-evolution log + the current CIATS
    values + the operational signals + the floor progress (+ the C5 tax lots when present)."""
    lines = [f"-- module: {m.module.upper()} " + "-" * (56 - len(m.module))]
    lines.append("trade performance:")
    lines += _render_performance(m.performance)

    lines.append(f"floor progress: inference {m.progress_to_inference_floor}  "
                 f"per-regime {m.progress_to_regime_floor}  (cumulative {m.cumulative_trade_count})")
    lines.append(f"operational signals: drift {m.drift_signals}  session-pause {m.session_pauses}  "
                 f"CRITICAL {m.critical_events}")

    lines.append(f"REPORTED theories (disproven, not pushed): {len(m.reported_theories)}")
    for t in m.reported_theories:
        lines.append(
            f"  - CHECK failed: mw_z {_s(getattr(t, 'mw_z', None))} vs crit "
            f"{_s(getattr(t, 'mw_crit', None))}; Sharpe cand {_s(getattr(t, 'sharpe_candidate', None))} "
            f"vs base {_s(getattr(t, 'sharpe_baseline', None))} (improved "
            f"{_s(getattr(t, 'sharpe_improved', None))})"
        )

    lines.append(f"deferred candidates (filed, not yet testable): {len(m.deferred_candidates)}")
    for d in m.deferred_candidates:
        cand = getattr(d, "candidate", None)
        lines.append(
            f"  - {_s(getattr(cand, 'level_key', None))} (rho {_s(getattr(cand, 'rho', None))}, "
            f"n {_s(getattr(cand, 'n', None))}): {_s(getattr(d, 'reason', None))}"
        )

    lines.append(f"proposed changes (awaiting Bill's HR-CI-011 decision): {len(m.proposed_changes)}")
    for a in m.proposed_changes:
        prop = getattr(a, "proposal", None)
        lines.append(
            f"  - [{_s(getattr(a, 'kind', None))}] {_s(getattr(prop, 'param_name', None))}: "
            f"{_s(getattr(prop, 'current_value', None))} -> {_s(getattr(prop, 'proposed_value', None))} "
            f"({_s(getattr(prop, 'rationale', None))})"
        )

    lines.append(f"parameter evolution this window: {len(m.parameter_evolution)}")
    for e in m.parameter_evolution:
        lines.append(
            f"  - {e.param_name}: {_s(e.old_value)} -> {_s(e.new_value)} "
            f"@ trade {e.at_trade_count} ({_s(e.time)})"
        )

    lines.append("current CIATS values:")
    for name, val in m.current_ciats_values.items():
        lines.append(f"  - {name}: {_s(val)}")

    if m.tax_lots:
        lines.append(f"C5 tax projection (Form 8949 lots, qty-dependent fields flagged): {len(m.tax_lots)}")
        for lot in m.tax_lots:
            lines.append(
                f"  - {lot.symbol}: acquired {_s(lot.acquired_utc)} disposed {_s(lot.disposed_utc)}; "
                f"entry {_s(lot.entry_fill_price)} exit {_s(lot.exit_price)}; gain/loss "
                f"{_s(lot.gain_loss_usd)} USD; fees {_s(lot.fees_total_usd)} USD"
            )
    return lines


# ===================================================================================== the renderer
def render_operator_report(report: OperatorReport) -> RenderedReport:
    """Render a structured OperatorReport to the operator-facing form (the email body / dashboard
    text) - DETERMINISTIC, Decimal-as-string, per-module sections + the combined roll-up + the FP5
    validity labels surfaced. The contract's implementation step (NOT architecture): a pure transform
    of the already-built VIEW content, NO new capture, NO C1 alert."""
    title = _CATEGORY_TITLE.get(report.category, report.category.code)
    subject = (
        f"TothBot {report.category.code} {report.cadence.upper()} - {title} "
        f"(as of {report.as_of.isoformat()})"
    )
    lines = [
        subject,
        "=" * len(subject),
        f"window: [{report.window.start.isoformat()}, {report.window.end.isoformat()})",
        "",
        "COMBINED (all modules):",
    ]
    lines += _render_performance(report.combined)
    for module in report.per_module:
        lines.append("")
        lines += _render_module(report.per_module[module])
    lines.append("")
    lines.append("-- end of report (periodic PULL; not a C1 immediate alert) --")
    return RenderedReport(
        category=report.category, code=report.category.code, cadence=report.cadence,
        as_of=report.as_of, subject=subject, body="\n".join(lines),
    )


# ====================================================================== the PULL trigger + emit seam
class PullReportService:
    """The operator-invoked C2-C6 PULL trigger surface: build (the VIEW) + render + EMIT a periodic
    report on request. DISTINCT from the C1 IMMEDIATE SMTP push - the EMIT goes through an INJECTED
    sink (testable without I/O), NEVER through mod:Logger.alert, so a periodic pull NEVER raises a C1
    alert. The operator (or a scheduler) calls pull(category, as_of); the service reads the
    already-captured record off the same mod:Logger membrane the VIEW reads, renders, emits, and
    returns the rendered report."""

    def __init__(
        self,
        logger: object,
        parameter_stores: Mapping[str, object],
        *,
        emit: ReportSink | None = None,
    ) -> None:
        self._logger = logger
        self._parameter_stores = parameter_stores
        self._emit = emit

    def pull(
        self,
        category: ReportCategory,
        as_of: datetime,
        *,
        modules: Sequence[str] | None = None,
    ) -> RenderedReport:
        """Build + render + emit the C2-C6 PULL report for `category` at the pull instant `as_of`.
        Returns the rendered report; if an emit sink is wired, the rendered report is delivered to it
        (the operator surface). PURE except the injected emit edge - no new capture, no C1 alert."""
        report = build_operator_report(
            self._logger, self._parameter_stores,
            category=category, as_of=as_of, modules=modules,
        )
        rendered = render_operator_report(report)
        if self._emit is not None:
            self._emit(rendered)
        return rendered
