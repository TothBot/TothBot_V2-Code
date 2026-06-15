"""contract:Operator_Reporting_Hierarchy - the C2-C6 periodic PULL report VIEWS.

Source: 0500000 dv1_251 sec 7 mod:Logger desc `reporting_hierarchy` (contract:Operator_Reporting_
Hierarchy) + rule:HR-RPT-001/002/003 + sec 5 statistical thresholds (the 200 / 600 floors). The
contract: "Operator reporting surface = 1 PUSH + 5 PULL categories, all VIEWS over the two Logger
streams + CIATS computed values (FP8 single-source; no new data capture)." The C1 IMMEDIATE PUSH
(the SMTP email bar, rule:HR-RPT-001) is ALREADY built - the approval/alert track (mod:Logger.alert
-> the on_approval edge -> SMTP). THIS module builds the FIVE PULL VIEWS:

  C2 DAILY    operational dashboard  - calendar day
  C3 WEEKLY   trend-watch            - calendar week (Monday-anchored)
  C4 MONTHLY  evolution              - calendar month
  C5 ANNUAL   compliance + P&L       - calendar year (Jan 1 - Dec 31; FP6 / IRS Form 8949)
  C6 ROLLING-12MO trajectory         - trailing 12 months, no calendar edge

PURE VIEW LAYER (FP8): every report is computed from the ALREADY-CAPTURED record - the per-module
Stream-2 corpus (the durable closed-trade outcomes, contract:CIATS_Trade_Outcome_Bus, the
authoritative C5 source per rule:HR-LG-013), Stream-1 (the ordered operational stream the reported
theories + proposed changes ride), and the per-module Parameter Store evolution log. NO NEW CAPTURE,
Decimal-only (ar:AR-047). The rendered output form (email body / dashboard layout) is implementation
"coded from this contract -- NOT preserved as architecture" (the contract's own SSS-precedent note),
so this module produces the STRUCTURED report content; formatting to email/dashboard is a separate
trivial render step.

WINDOWING THE TIMESTAMP-LESS CIATS EVENTS (the no-new-capture key). A TRADE_CLOSE carries
exit_timestamp_utc; the CIATS events it triggers (CheckResult / DeferredCandidate / ApprovalRequested
/ DriftSignal) carry NO wall-clock and NO side - they are emitted DURING that close's on_close, so in
the ordered Stream-1 each lands immediately AFTER the TRADE_CLOSE that produced it. A single cursor
over Stream-1 therefore gives every CIATS event BOTH its time (the preceding TRADE_CLOSE's
exit_timestamp_utc) AND its module (that trade's side, recovered by record identity from the per-
module corpus). That is a faithful VIEW - the event genuinely belongs to that side's loop processing
that trade - with zero added capture.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum

from ..config import registry
from ..ciats.statistical_engine import sharpe_ratio

# Diagram-named statistical floors (sec 5; transcribed, NOT CIATS seeds): inference valid at 200
# closed trades, per-regime analysis valid at 600. FP5: win-rate / Sharpe / Spearman are
# monitoring-only and LABELED insufficient-data until the 200-trade floor is reached.
INFERENCE_FLOOR = 200
REGIME_ANALYSIS_FLOOR = 600
PER_REGIME_BUCKET_FLOOR = 100  # the per-regime bucket target (100+ per regime, sec 5 / the contract)

# The CIATS-owned operating values the reports surface ("the current CIATS values"): the resolved
# live value is the Parameter-Store-owned value if CIATS has written it, else the registry seed.
# The self-tuning dials the FORM->TEST->ROUTE loop moves (the stop-width + the entry filters) +
# the sizing dial. A name absent from the registry is simply skipped (defensive).
REPORTED_CIATS_PARAMS: tuple[str, ...] = (
    "mae_mult", "mae_mult_nudge_pct", "per_trade_size_usd",
    "volume_sss_threshold", "rsi_long_low", "rsi_long_high",
    "rsi_short_low", "rsi_short_high",
)

_DEFAULT_MODULES = ("long", "short")


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


# ============================================================ the PULL window (the cadence trigger)
class ReportCategory(Enum):
    """The five PULL report categories (the C1 IMMEDIATE PUSH is the separate approval/alert track).
    The value is the (code, cadence) the operator pulls on."""

    C2_DAILY = ("C2", "daily")
    C3_WEEKLY = ("C3", "weekly")
    C4_MONTHLY = ("C4", "monthly")
    C5_ANNUAL = ("C5", "annual")
    C6_ROLLING_12MO = ("C6", "rolling-12mo")

    @property
    def code(self) -> str:
        return self.value[0]

    @property
    def cadence(self) -> str:
        return self.value[1]


@dataclass(frozen=True)
class Window:
    """A half-open report window [start, end) in UTC. A record is in-window iff start <= t < end."""

    start: datetime
    end: datetime

    def contains(self, t: datetime | None) -> bool:
        return t is not None and self.start <= t < self.end


def _as_utc(dt: datetime) -> datetime:
    """Normalize a datetime to a UTC-aware instant (a naive datetime is read as UTC)."""
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def _midnight(dt: datetime) -> datetime:
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _first_of_next_month(dt: datetime) -> datetime:
    return (dt.replace(day=1) + timedelta(days=32)).replace(day=1)


def _year_ago(dt: datetime) -> datetime:
    """The same instant 12 calendar months earlier (Feb 29 -> Feb 28, no calendar-edge surprise)."""
    try:
        return dt.replace(year=dt.year - 1)
    except ValueError:
        return dt.replace(year=dt.year - 1, day=28)


def report_window(category: ReportCategory, as_of: datetime) -> Window:
    """The PULL window for a category at the pull moment `as_of` (the cadence trigger; distinct from
    the C1 IMMEDIATE push). C2 = the calendar day; C3 = the Monday-anchored calendar week; C4 = the
    calendar month; C5 = the calendar year (Jan 1 - Dec 31); C6 = the trailing 12 months ending at
    `as_of` (rolling, no calendar edge). All UTC, half-open [start, end)."""
    a = _as_utc(as_of)
    if category is ReportCategory.C2_DAILY:
        start = _midnight(a)
        return Window(start, start + timedelta(days=1))
    if category is ReportCategory.C3_WEEKLY:
        start = _midnight(a) - timedelta(days=a.weekday())  # Monday 00:00 UTC
        return Window(start, start + timedelta(days=7))
    if category is ReportCategory.C4_MONTHLY:
        start = _midnight(a).replace(day=1)
        return Window(start, _first_of_next_month(start))
    if category is ReportCategory.C5_ANNUAL:
        start = _midnight(a).replace(month=1, day=1)
        return Window(start, start.replace(year=start.year + 1))
    # C6 rolling-12mo: trailing window ending at the pull instant.
    return Window(_year_ago(a), a)


def record_time(record: object) -> datetime | None:
    """The wall-clock instant a captured record belongs to. A TRADE_CLOSE uses exit_timestamp_utc
    (the close instant; field 9), falling back to ts (field 1); any other record returns its ts if it
    carries one, else None. Parses ISO 8601 (a trailing 'Z' -> +00:00), naive read as UTC. Returns
    None on an absent/unparseable stamp (no fabricated time)."""
    raw = getattr(record, "exit_timestamp_utc", None) or getattr(record, "ts", None)
    if not raw:
        return None
    try:
        return _as_utc(datetime.fromisoformat(str(raw).replace("Z", "+00:00")))
    except ValueError:
        return None


# ============================================================ trade performance (the Stream-2 view)
def _is_trade_close(record: object) -> bool:
    return getattr(record, "event", None) == "TRADE_CLOSE" or getattr(record, "code", None) == "TRADE_CLOSE"


@dataclass(frozen=True)
class RegimePerformance:
    """Per-regime realized performance (asset_regime bucket): the C3/C4/C5/C6 per-regime breakdown.
    bucket_count is the cardinality toward the 100+ per-regime target (sec 5 / the contract)."""

    regime: str
    bucket_count: int
    net_pl_usd: Decimal
    win_rate: Decimal | None


@dataclass(frozen=True)
class TradePerformance:
    """Realized trade performance over a record set (the contract's trade-performance content): net
    P/L, win rate, the R:R distribution, Sharpe, the Half-Kelly state, fee economics, and the
    per-regime breakdown. FP5: win_rate / sharpe / spearman / kelly are LABELED inference_valid only
    at the 200-trade floor (else monitoring-only / insufficient-data-N-of-200)."""

    trade_count: int
    wins: int
    losses: int
    net_pl_usd: Decimal
    net_gain_usd: Decimal
    net_loss_usd: Decimal
    fees_total_usd: Decimal
    fees_pct_of_gross: Decimal | None
    win_rate: Decimal | None
    avg_rr: Decimal | None
    rr_min: Decimal | None
    rr_median: Decimal | None
    rr_max: Decimal | None
    best_trade_usd: Decimal | None
    worst_trade_usd: Decimal | None
    sharpe: Decimal | None
    kelly_full: Decimal | None
    kelly_half: Decimal | None
    inference_valid: bool
    floor_label: str
    per_regime: tuple[RegimePerformance, ...]


def _median(values: Sequence[Decimal]) -> Decimal | None:
    if not values:
        return None
    s = sorted(values)
    n = len(s)
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / Decimal(2)


def _floor_label(count: int) -> str:
    if count >= INFERENCE_FLOOR:
        return "inference-valid"
    return f"monitoring-only: insufficient-data {count}-of-{INFERENCE_FLOOR} (FP5)"


def _kelly(win_rate: Decimal | None, r: Decimal | None) -> tuple[Decimal | None, Decimal | None]:
    """Half-Kelly from the realized W and net R:R: K_full = W - (1-W)/R, K_half = K_full/2.
    None when W or R is undefined (no wins, no losses, or R == 0)."""
    if win_rate is None or r is None or r == 0:
        return None, None
    k_full = win_rate - (Decimal(1) - win_rate) / r
    return k_full, k_full / Decimal(2)


def trade_performance(records: Sequence[object]) -> TradePerformance:
    """Compute the realized trade performance over a set of TRADE_CLOSE records (the Stream-2 view).
    PURE, Decimal-only. The R:R distribution uses each record's actual_rr (field 20) where present;
    the Sharpe / Half-Kelly are over net_pl_usd. Per-regime buckets group by asset_regime."""
    trades = [r for r in records if _is_trade_close(r)]
    count = len(trades)
    wins = sum(1 for r in trades if _dec(r.net_pl_usd) > 0)
    losses = sum(1 for r in trades if _dec(r.net_pl_usd) < 0)
    net_pl = sum((_dec(r.net_pl_usd) for r in trades), Decimal(0))
    net_gain = sum((_dec(r.net_gain_usd) for r in trades), Decimal(0))
    net_loss = sum((_dec(r.net_loss_usd) for r in trades), Decimal(0))
    fees = sum((_dec(getattr(r, "fees_total_usd", 0) or 0) for r in trades), Decimal(0))
    gross = net_pl + fees  # gross = net + fees (fees were subtracted to reach net, ar:AR-065)
    fees_pct = (fees / gross) if gross != 0 else None

    win_rate = (Decimal(wins) / Decimal(count)) if count else None
    rrs = [_dec(r.actual_rr) for r in trades if getattr(r, "actual_rr", None) is not None]
    avg_rr = (sum(rrs, Decimal(0)) / Decimal(len(rrs))) if rrs else None
    pls = [_dec(r.net_pl_usd) for r in trades]
    # R (net reward:risk) for Half-Kelly: avg(net_gain over wins) / avg(net_loss over losses).
    r_ratio = (
        (net_gain / Decimal(wins)) / (net_loss / Decimal(losses))
        if wins and losses and net_loss != 0
        else None
    )
    k_full, k_half = _kelly(win_rate, r_ratio)
    # Sharpe is undefined for < 2 points or a zero-variance (degenerate / uniform) cohort - a report
    # must never crash on real-but-uniform data, so a degenerate series surfaces as None, not a raise.
    try:
        sharpe = sharpe_ratio(pls) if count >= 2 else None
    except ValueError:
        sharpe = None

    per_regime: list[RegimePerformance] = []
    by_regime: dict[str, list[object]] = {}
    for r in trades:
        by_regime.setdefault(str(getattr(r, "asset_regime", None) or "UNCLASSIFIED"), []).append(r)
    for regime, rs in sorted(by_regime.items()):
        rw = sum(1 for r in rs if _dec(r.net_pl_usd) > 0)
        per_regime.append(RegimePerformance(
            regime=regime, bucket_count=len(rs),
            net_pl_usd=sum((_dec(r.net_pl_usd) for r in rs), Decimal(0)),
            win_rate=(Decimal(rw) / Decimal(len(rs))) if rs else None,
        ))

    return TradePerformance(
        trade_count=count, wins=wins, losses=losses,
        net_pl_usd=net_pl, net_gain_usd=net_gain, net_loss_usd=net_loss,
        fees_total_usd=fees, fees_pct_of_gross=fees_pct,
        win_rate=win_rate, avg_rr=avg_rr,
        rr_min=(min(rrs) if rrs else None), rr_median=_median(rrs), rr_max=(max(rrs) if rrs else None),
        best_trade_usd=(max(pls) if pls else None), worst_trade_usd=(min(pls) if pls else None),
        sharpe=sharpe, kelly_full=k_full, kelly_half=k_half,
        inference_valid=count >= INFERENCE_FLOOR, floor_label=_floor_label(count),
        per_regime=tuple(per_regime),
    )


# ============================================================ the Stream-1 cursor (time + side view)
@dataclass(frozen=True)
class AttributedEvent:
    """A Stream-1 CIATS event attributed to its (module, time) by the cursor: the side + the instant
    of the TRADE_CLOSE that triggered it (the no-new-capture windowing key)."""

    module: str | None
    time: datetime | None
    event: object


def attribute_stream1(operational: Sequence[object], corpus: Mapping[str, Sequence[object]]) -> list[AttributedEvent]:
    """Walk the ordered Stream-1, attributing every non-trade CIATS event to the (module, time) of
    the most recent preceding TRADE_CLOSE - the close it was emitted during. A TRADE_CLOSE's module
    is recovered by record identity from the per-module corpus; its time from exit_timestamp_utc.
    Events before any TRADE_CLOSE (the cold start) carry (None, None)."""
    id_to_module = {id(r): m for m, rs in corpus.items() for r in rs}
    out: list[AttributedEvent] = []
    cur_module: str | None = None
    cur_time: datetime | None = None
    for rec in operational:
        if _is_trade_close(rec):
            cur_module = id_to_module.get(id(rec), cur_module)
            cur_time = record_time(rec) or cur_time
            continue
        out.append(AttributedEvent(cur_module, cur_time, rec))
    return out


def _code(event: object) -> str:
    return str(getattr(event, "code", "") or getattr(event, "event", ""))


# ============================================================ the per-module + operator report views
@dataclass(frozen=True)
class ParameterEvolutionEntry:
    """One windowed parameter-evolution entry (the C4 parameter-evolution log): the owned change
    old -> new at the closed-trade count it was written, with the wall-clock time mapped from the
    module corpus (the at_trade_count-th trade's exit instant)."""

    param_name: str
    old_value: object
    new_value: object
    at_trade_count: int
    time: datetime | None


@dataclass(frozen=True)
class TaxLot:
    """A C5 Form 8949 / 26 CFR 1.6001-1 lot projection (the available 23-field fields): the acquired
    + disposed instants, the entry/exit prices, the realized gain/loss (net_pl_usd), and the fees.
    NOTE: proceeds / cost-basis in dollars need the position QTY, which the 23-field TRADE_CLOSE
    schema does not carry - surfaced as a known gap (see module/SL carry-forward), not fabricated."""

    symbol: str
    acquired_utc: str | None
    disposed_utc: str | None
    entry_fill_price: Decimal
    exit_price: Decimal
    gain_loss_usd: Decimal
    fees_total_usd: Decimal


@dataclass(frozen=True)
class ModuleReport:
    """One module's (Long / Short) report content for a window: the realized trade performance, the
    REPORTED theories (CHECK-failed CheckResults + filed DeferredCandidates), the proposed changes
    (ApprovalRequested, the C1-track items echoed for the parameter-change-this-period line), the
    applied parameter-evolution log, the current CIATS values, the operational signals (drift /
    session-pause / CRITICAL counts), and the cumulative-as-of trade-count progress to the floors."""

    module: str
    performance: TradePerformance
    reported_theories: tuple[object, ...]
    deferred_candidates: tuple[object, ...]
    proposed_changes: tuple[object, ...]
    parameter_evolution: tuple[ParameterEvolutionEntry, ...]
    current_ciats_values: Mapping[str, object]
    drift_signals: int
    session_pauses: int
    critical_events: int
    cumulative_trade_count: int
    progress_to_inference_floor: str
    progress_to_regime_floor: str
    tax_lots: tuple[TaxLot, ...]


@dataclass(frozen=True)
class OperatorReport:
    """A C2-C6 PULL report: the category + cadence + window + the per-module sections + a combined
    trade-performance roll-up. A pure VIEW over the captured record (no new capture)."""

    category: ReportCategory
    cadence: str
    window: Window
    as_of: datetime
    per_module: Mapping[str, ModuleReport]
    combined: TradePerformance


def current_ciats_values(store: object) -> dict[str, object]:
    """The module's current CIATS operating values: the Parameter-Store-owned value where CIATS has
    written it, else the registry seed. A name absent from the registry is skipped (defensive)."""
    out: dict[str, object] = {}
    for name in REPORTED_CIATS_PARAMS:
        owned = store.get(name) if store is not None else None
        if owned is not None:
            out[name] = owned
            continue
        try:
            out[name] = registry.value(name)
        except KeyError:
            continue
    return out


def _evolution_in_window(store: object, module_corpus: Sequence[object], window: Window) -> list[ParameterEvolutionEntry]:
    """The parameter-evolution entries whose write falls in the window. A change's time is the
    exit instant of the at_trade_count-th closed trade in the module corpus (1-indexed; the count
    AFTER that trade). Entries outside the corpus index range carry time None and are kept only when
    the window is the rolling/annual all-time view's open end (defensive: included iff time in window
    OR time is None and no other anchor)."""
    if store is None:
        return []
    trades = [r for r in module_corpus if _is_trade_close(r)]
    out: list[ParameterEvolutionEntry] = []
    for change in store.evolution_log:
        k = change.at_trade_count
        t = record_time(trades[k - 1]) if 1 <= k <= len(trades) else None
        if window.contains(t):
            out.append(ParameterEvolutionEntry(
                param_name=change.param_name, old_value=change.old_value,
                new_value=change.new_value, at_trade_count=k, time=t,
            ))
    return out


def _progress(count: int, floor: int) -> str:
    return f"{min(count, floor)}/{floor}" + (" (reached)" if count >= floor else "")


def build_module_report(
    module: str,
    *,
    corpus: Sequence[object],
    attributed: Sequence[AttributedEvent],
    store: object,
    category: ReportCategory,
    window: Window,
) -> ModuleReport:
    """Build one module's report content for the window from the captured record (PURE). `corpus` is
    the module's Stream-2 closed-trade outcomes; `attributed` is the Stream-1 cursor walk (time +
    side); `store` is the module Parameter Store (the evolution log + the owned values)."""
    in_window = [r for r in corpus if window.contains(record_time(r))]
    mine = [a for a in attributed if a.module == module and window.contains(a.time)]

    reported = tuple(a.event for a in mine
                     if _code(a.event) == "PDCA_CHECK_RESULT" and getattr(a.event, "passed", None) is False)
    deferred = tuple(a.event for a in mine if _code(a.event) == "CIATS_CANDIDATE_DEFERRED")
    proposed = tuple(a.event for a in mine if _code(a.event) == "CIATS_APPROVAL_REQUESTED")
    drift = sum(1 for a in mine if _code(a.event) == "CIATS_DRIFT_SIGNAL")
    pauses = sum(1 for a in mine if _code(a.event) == "SESSION_PAUSE_TRIGGERED")
    criticals = sum(1 for a in mine if getattr(a.event, "level", None) == "CRITICAL")

    tax_lots: tuple[TaxLot, ...] = ()
    if category is ReportCategory.C5_ANNUAL:
        tax_lots = tuple(
            TaxLot(
                symbol=str(getattr(r, "symbol", "")),
                acquired_utc=getattr(r, "entry_timestamp_utc", None),
                disposed_utc=getattr(r, "exit_timestamp_utc", None),
                entry_fill_price=_dec(r.entry_fill_price), exit_price=_dec(r.exit_price),
                gain_loss_usd=_dec(r.net_pl_usd),
                fees_total_usd=_dec(getattr(r, "fees_total_usd", 0) or 0),
            )
            for r in in_window
        )

    cumulative = len([r for r in corpus if _is_trade_close(r)])
    return ModuleReport(
        module=module,
        performance=trade_performance(in_window),
        reported_theories=reported,
        deferred_candidates=deferred,
        proposed_changes=proposed,
        parameter_evolution=tuple(_evolution_in_window(store, corpus, window)),
        current_ciats_values=current_ciats_values(store),
        drift_signals=drift, session_pauses=pauses, critical_events=criticals,
        cumulative_trade_count=cumulative,
        progress_to_inference_floor=_progress(cumulative, INFERENCE_FLOOR),
        progress_to_regime_floor=_progress(cumulative, REGIME_ANALYSIS_FLOOR),
        tax_lots=tax_lots,
    )


def build_operator_report(
    logger: object,
    parameter_stores: Mapping[str, object],
    *,
    category: ReportCategory,
    as_of: datetime,
    modules: Sequence[str] | None = None,
) -> OperatorReport:
    """Build a C2-C6 PULL report VIEW over the captured record (FP8, no new capture). `logger` is the
    mod:Logger membrane (Stream-1 .operational + the per-module Stream-2 .corpus); `parameter_stores`
    maps each module name (the side) to its Parameter Store (the evolution log + the owned values).
    `as_of` is the pull instant the window is anchored on. PURE."""
    window = report_window(category, _as_utc(as_of))
    corpus_map = dict(getattr(logger, "corpus", {}) or {})
    operational = list(getattr(logger, "operational", []) or [])
    mods = tuple(modules) if modules else tuple(
        dict.fromkeys((*_DEFAULT_MODULES, *corpus_map.keys(), *parameter_stores.keys()))
    )
    attributed = attribute_stream1(operational, corpus_map)

    per_module: dict[str, ModuleReport] = {}
    for m in mods:
        per_module[m] = build_module_report(
            m, corpus=corpus_map.get(m, []), attributed=attributed,
            store=parameter_stores.get(m), category=category, window=window,
        )

    all_in_window = [
        r for m in mods for r in corpus_map.get(m, []) if window.contains(record_time(r))
    ]
    return OperatorReport(
        category=category, cadence=category.cadence, window=window, as_of=_as_utc(as_of),
        per_module=per_module, combined=trade_performance(all_in_window),
    )
