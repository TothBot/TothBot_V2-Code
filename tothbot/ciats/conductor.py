"""mod:CIATS conductor - the per-module learning loop that DRIVES the CIATS units on the organism.

Source: 0500000 dv1_250 sec 6/7 mod:CIATS (the per-module framework) + the rule:HR-CI-* family +
contract:CIATS_Trade_Outcome_Bus (the Logger Stream-2 corpus this consumes). The individual CIATS
elements are built as PURE units (pool / statistical_engine / ewma_monitor / pdca_engine /
proposal_engine / parameter_store / regime_library); this is the ORCHESTRATION that ties them into
one running learning loop per module (Long / Short), off that module's Stream-2 corpus slice.

THE LOOP (one CiatsConductor per wallet, like every other CIATS unit - NO cross-module pooling):

  ingest_close      - accumulate one closed-trade outcome into the pool + the Regime Library bucket +
                      the per-trade net-P/L series the drift detectors read (the learning-loop close).
  recompute_kelly   - at the 50-trade Half-Kelly cadence (ProposalEngine.kelly_due) emit KELLY_UPDATE /
                      KELLY_NEGATIVE and STAGE the per_trade_size proposal for the HR-CI-011 approval
                      surface (HR-CI-008: a negative edge makes NO positive sizing proposal).
  scan_drift        - the PDCA PLAN trigger: a cusum_lower lower-arm breach on the net-P/L series
                      (HR-CI-007, forces an OUT-OF-CYCLE PLAN) or an optional EWMA sustained divergence.
  identify_drift_candidate - the diagram's PLAN-candidate identification: rank each per-trade
                      signal_params LEVEL against the realized outcome (the Spearman gate, strongest
                      |rho|) over the corpus + surface the candidate the data supports. Drives off the
                      producer wired TB00750 a/b; the owned-threshold mapping + proposed value it would
                      need to advance to a write are an unspecified design decision (surfaced, not made).
  open_pdca         - on a constructed candidate proposal: PLAN (200-trade floor / sacred-R:R gated) -> DO
                      (shadow-replay the candidate over THIS corpus -> a candidate vs baseline cohort)
                      -> CHECK (the absolute Mann-Whitney gate) -> on a pass STAGE the proposal for
                      approval. NEVER auto-applies.
  submit_approval   - on Bill approval (HR-CI-011) at a confirmed inter-trade boundary (HR-CI-003),
                      apply the staged change to the Parameter Store (which owns the HR-CI-005 50-trade
                      interval) + log the parameter-evolution entry. A failed gate keeps the pending
                      request retryable at the next boundary.
  on_inter_trade_boundary - the HR-CI-003 boundary edge (driven off the exit path's confirmed close):
                      poll the operator ApprovalInbox + submit_approval any change Bill has decided on
                      at THIS boundary. Never auto-applies - it acts only on Bill's recorded decision.
  disallowed_regimes- the Regime Library's protective param:disallowed_regimes list (the ACTIVE,
                      negative-edge regimes) surfaced for gate:G3_Regime_Filter to read.

EVERYTHING here is PROPOSE/DETECT only: the conductor STAGES proposals and routes them to Bill; only an
approved change at an inter-trade boundary reaches the Parameter Store, and the sacred 1:1.5 R:R is
never a candidate. The two injected edges are the approval surface (`on_approval`, the mod:Logger
HR-LG-009 SMTP alert seam) + the inter-trade-boundary + approval signals (passed to submit_approval).
The PDCA DO phase replays the REAL gate/exit counterfactual over this module's corpus by default
(ciats/shadow_replay.build_shadow_evaluator: a gating change includes/excludes a trade, a sizing/exit
change scales it; an `evaluator` may still be injected). PURE state + Decimal-only (ar:AR-047) apart
from the injected sync edges.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal

from ..config import registry
from .pdca_engine import PdcaEngine, PlanBlocked
from .proposal_engine import (
    MAE_MULT_PARAM,
    PER_TRADE_SIZE_PARAM,
    IdentifiedCandidate,
    KellyNegative,
    KellyUpdate,
    ParameterChangeProposal,
    ProposalEngine,
    identify_spearman_candidate,
    plan_entry_filter_proposal,
    plan_stop_width_proposal,
)
from .shadow_replay import build_shadow_evaluator
from .statistical_engine import cusum_lower


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _net_pl(record: object) -> Decimal:
    """The realized net P/L of a Stream-2 TRADE_CLOSE record (ar:AR-065, NET of fees)."""
    return _dec(record.net_pl_usd)


@dataclass(frozen=True)
class DriftSignal:
    """evt:CIATS_DRIFT_SIGNAL [HIGH] - a performance-degradation signal that initiates a PDCA PLAN.
    kind is "cusum_lower" (the HR-CI-007 lower-arm breach, an OUT-OF-CYCLE PLAN) or "ewma_sustained"
    (the optional EWMA sustained-divergence monitor). detail surfaces the breach location."""

    kind: str
    detail: str
    code: str = field(default="CIATS_DRIFT_SIGNAL", init=False)


@dataclass(frozen=True)
class ApprovalRequested:
    """evt:CIATS_APPROVAL_REQUESTED [HIGH] (rule:HR-CI-011) - a CIATS-owned parameter change staged
    for Bill's approval, routed to the approval surface (the mod:Logger HR-LG-009 SMTP alert seam).
    request_id keys the later submit_approval; check is the PDCA evidence (None for a Kelly recompute,
    whose evidence is the Half-Kelly math itself); kind is "kelly" or "pdca". The change is NEVER
    applied here - only an approved submit_approval at an inter-trade boundary reaches the store."""

    request_id: int
    proposal: object
    check: object | None
    kind: str
    code: str = field(default="CIATS_APPROVAL_REQUESTED", init=False)


@dataclass(frozen=True)
class DeferredCandidate:
    """evt:CIATS_CANDIDATE_DEFERRED [INFO] - an IDENTIFIED PLAN candidate that has no faithful
    testable mapping yet, so it is FILED to the report track (the contract:Operator_Reporting_
    Hierarchy periodic PULL reports, NOT a C1 alert), never sham-tested (Bill's directive: a not-yet-
    testable theory is REPORTED, not brought for a decision). Today this is the ema level->period
    candidate (a stored level cannot re-decide a period change), an entry-filter direction a single
    bound cannot express, or a candidate with too few losing samples. Carries the IdentifiedCandidate
    + the reason it was deferred."""

    candidate: object
    reason: str
    code: str = field(default="CIATS_CANDIDATE_DEFERRED", init=False)


@dataclass
class _Pending:
    """One staged-but-unapproved parameter change awaiting Bill's HR-CI-011 decision."""

    request_id: int
    proposal: object
    check: object | None
    kind: str


class ApprovalInbox:
    """The operator's HR-CI-011 approval inbox: where Bill's yes/no decision on a staged parameter
    change lands (keyed by the ApprovalRequested.request_id). The approval RETURN is an INJECTED
    operator edge - the organism never decides for Bill; it polls this inbox at each inter-trade
    boundary. submit() is the operator surface (Bill records a decision); decision() is the conductor's
    read; clear() drops a resolved decision. PURE state (no I/O)."""

    def __init__(self) -> None:
        self._decisions: dict[int, bool] = {}

    def submit(self, request_id: int, *, approved: bool) -> None:
        """Bill's decision for a staged change (the operator edge). A later submit overrides an
        undecided one (Bill may change his mind before the boundary applies it)."""
        self._decisions[request_id] = bool(approved)

    def decision(self, request_id: int) -> bool | None:
        """The operator's decision for a request, or None if Bill has not decided yet (the conductor
        leaves the change pending and re-polls at the next boundary). A read, never a pop."""
        return self._decisions.get(request_id)

    def clear(self, request_id: int) -> None:
        """Drop a resolved decision (the change was applied, or Bill's rejection was consumed)."""
        self._decisions.pop(request_id, None)

    @property
    def pending_decisions(self) -> dict[int, bool]:
        """The outstanding operator decisions (for inspection/telemetry)."""
        return dict(self._decisions)


def shadow_cohorts(
    records: Sequence[object],
    evaluator: Callable[[object], object | None],
    *,
    baseline: Callable[[object], object] = _net_pl,
) -> tuple[list[Decimal], list[Decimal]]:
    """The PDCA DO-phase corpus replay (PURE). Returns (candidate_cohort, baseline_cohort) over the
    module's Stream-2 records: the baseline is each record's realized outcome (default net_pl_usd);
    the candidate is the counterfactual `evaluator(record)` - returning None for a record the candidate
    parameter would NOT have traded (so a gating change shrinks the cohort) or a Decimal for the
    re-evaluated outcome (so a sizing/exit change scales it). The evaluator is the injected
    trade-resimulation model (a fuller build replaces it with a real gate/exit replay)."""
    baseline_cohort = [_dec(baseline(r)) for r in records]
    candidate_cohort: list[Decimal] = []
    for r in records:
        outcome = evaluator(r)
        if outcome is not None:
            candidate_cohort.append(_dec(outcome))
    return candidate_cohort, baseline_cohort


class CiatsConductor:
    """One module's CIATS learning loop (per-wallet). Composes the pool, the proposal/PDCA engines,
    the parameter store, and the regime library into a running detect->propose->(Bill approval)->apply
    cycle off the module's Stream-2 corpus. PROPOSE/DETECT only - it never writes a parameter without
    an approved submit_approval at an inter-trade boundary, and never touches the sacred R:R."""

    def __init__(
        self,
        *,
        module: str,
        pool,
        regime_library,
        parameter_store,
        proposal_engine: ProposalEngine | None = None,
        pdca_engine: PdcaEngine | None = None,
        drift_monitor=None,
        on_event: Callable[[object], None] | None = None,
        on_approval: Callable[[object], None] | None = None,
    ) -> None:
        self._module = module
        self._pool = pool
        self._regimes = regime_library
        self._store = parameter_store
        self._proposals = proposal_engine or ProposalEngine(trade_floor=pool._trade_floor)
        self._pdca = pdca_engine or PdcaEngine(trade_floor=pool._trade_floor)
        self._drift = drift_monitor
        self._on_event = on_event
        self._on_approval = on_approval
        self._records: list[object] = []
        self._net_pl: list[Decimal] = []
        self._pending: dict[int, _Pending] = {}
        self._req_id = 0

    @property
    def module(self) -> str:
        return self._module

    @property
    def parameter_store(self):
        """The module's CIATS Parameter Store (the owned-value write destination + the frozen
        per-cycle read source). Exposed so the live layer can take the per-cycle Parameter_Store_
        Snapshot (CI-IF-003) the gates read - build_cycle_parameters(conductor.parameter_store, ...)."""
        return self._store

    @property
    def trade_count(self) -> int:
        """The module's closed-trade count - the inter-trade-boundary anchor + the cadence clock."""
        return self._pool.trade_count

    @property
    def pending(self) -> tuple[_Pending, ...]:
        """The staged-but-unapproved parameter changes (awaiting Bill's HR-CI-011 decision)."""
        return tuple(self._pending.values())

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    # --- the learning-loop close: accumulate one outcome ----------------------------------------
    def ingest_close(self, record: object, *, regime: object = None) -> None:
        """Accumulate one closed-trade TRADE_CLOSE record (the Stream-2 corpus shape) into the module
        pool + the per-trade net-P/L series the drift detectors read, and (when `regime` is given)
        into that asset_regime's Regime Library bucket. The learning-loop close (sec 7)."""
        self._pool.ingest(record)
        self._records.append(record)
        self._net_pl.append(_net_pl(record))
        if regime is not None:
            self._regimes.ingest_trade_close(regime, record)
        if self._drift is not None:
            self._drift.update(_net_pl(record))

    # --- the full per-close cadence (driven off the running TRADE_CLOSE, sec 7 / HR-CI-003) -------
    def on_close(
        self,
        record: object,
        *,
        regime: object = None,
        wallet_balance: object = None,
        inbox: "ApprovalInbox | None" = None,
    ) -> "tuple[object, object, list]":
        """The complete per-close PROPOSE/DETECT cadence the running exit path drives at each
        confirmed TRADE_CLOSE (the close IS the cadence clock + the HR-CI-003 inter-trade boundary):

          1. ingest_close      - the learning-loop accumulate (pool + drift series + regime bucket).
          2. recompute_kelly    - at the 50-trade Half-Kelly boundary (after the 200-trade activation)
                                  STAGE the per_trade_size_usd proposal to Bill; SKIPPED when
                                  wallet_balance is None (live mode has no synthetic wallet; the seed
                                  sizing stands). current_size is read from THIS module's store.
          3. scan_drift         - the HR-CI-007 net-P/L CUSUM out-of-cycle PLAN trigger (DETECT/emit).
                                  On a drift signal plan_from_drift runs the FORM -> TEST -> ROUTE loop
                                  (Bill's TB00750 directive): for each TESTABLE dial it FORMS a data-
                                  derived candidate, TESTS it via open_pdca (the real shadow replay +
                                  CHECK), and ROUTES by result - a CHECK pass STAGES it for the HR-CI-011
                                  C1 alert (profitable -> brought to Bill), a fail emits the CheckResult
                                  (unprofitable -> the report track). A not-yet-testable entry-filter
                                  candidate is FILED to the report track (DeferredCandidate), never sham-
                                  tested. The sacred R:R is never a candidate.
          4. on_inter_trade_boundary - poll the operator inbox + APPLY any Bill-approved change at this
                                  confirmed boundary (never auto-applied).

        Returns (kelly_outcome, drift_signal, applied_outcomes). PROPOSE/DETECT only - the sacred R:R
        is never a candidate and nothing is applied without Bill's recorded approval at the boundary."""
        self.ingest_close(record, regime=regime)
        kelly = None
        if wallet_balance is not None:
            kelly = self.recompute_kelly(
                wallet_balance=wallet_balance,
                current_size=self._store.get(PER_TRADE_SIZE_PARAM),
            )
        drift = self.scan_drift()
        if drift is not None:
            # The drift-triggered FORM -> TEST -> ROUTE loop (Bill's TB00750 directive): test each
            # testable dial off the running close + file any not-yet-testable candidate to the report
            # track. The two-track routing lives in open_pdca; this feeds it real testable candidates.
            self.plan_from_drift()
        applied = self.on_inter_trade_boundary(inbox) if inbox is not None else []
        return kelly, drift, applied

    # --- the Half-Kelly cadence ------------------------------------------------------------------
    def recompute_kelly(
        self, *, wallet_balance: object, current_size: object = None
    ) -> "KellyUpdate | KellyNegative | None":
        """At a 50-trade Half-Kelly boundary (after the 200-trade activation) recompute the
        per_trade_size_usd: emit KELLY_UPDATE + STAGE the proposal for approval, or emit KELLY_NEGATIVE
        [CRITICAL] (HR-CI-008 - no positive sizing proposal, the seed sizing stands). Returns the event
        (or None off a cadence boundary / before W,R are both defined)."""
        if not self._proposals.kelly_due(self.trade_count):
            return None
        sizing = self._proposals.kelly_sizing(self._pool, wallet_balance)
        if sizing is None:
            return None
        self._emit(sizing)
        if isinstance(sizing, KellyNegative):
            return sizing
        proposal = ParameterChangeProposal(
            param_name=PER_TRADE_SIZE_PARAM,
            current_value=current_size,
            proposed_value=sizing.per_trade_size_usd,
            rationale=f"Half-Kelly K_half={sizing.k_half} * wallet (K_full={sizing.k_full})",
        )
        self._stage_approval(proposal, check=None, kind="kelly")
        return sizing

    # --- the PDCA drift trigger + cycle ----------------------------------------------------------
    def scan_drift(self, *, mu: object = None, sigma: object = None) -> DriftSignal | None:
        """The PDCA PLAN trigger. A cusum_lower LOWER-arm breach on the net-P/L series signals
        performance degradation (HR-CI-007: it forces an OUT-OF-CYCLE PLAN); an optional drift_monitor
        whose `sustained` flag is set is the EWMA secondary. Returns the first DriftSignal that fires,
        or None. mu/sigma default to the series' own estimate (pass a fixed pre-floor baseline to
        monitor against it). Needs >= 2 closed trades for the CUSUM."""
        if len(self._net_pl) >= 2:
            cusum = cusum_lower(self._net_pl, mu=mu, sigma=sigma)
            if cusum.breached:
                signal = DriftSignal(
                    "cusum_lower", f"net-P/L CUSUM lower-arm breach at trade index {cusum.breach_index}"
                )
                self._emit(signal)
                return signal
        if self._drift is not None and self._drift.sustained:
            signal = DriftSignal("ewma_sustained", "EWMA sustained divergence on the net-P/L signal")
            self._emit(signal)
            return signal
        return None

    def identify_drift_candidate(self) -> "IdentifiedCandidate | None":
        """The diagram's PLAN-candidate IDENTIFICATION over THIS module's Stream-2 corpus: rank each
        continuous per-trade signal_params LEVEL against the realized net-P/L via the CIATS Spearman gate
        (|rho| > 0.3 AND p < 0.05) and return the strongest-|rho| qualifying candidate, or None. The
        producer wired in TB00750 (a/b) put the per-trade signal_params levels into the corpus, so this
        runs faithfully off the running close. It IDENTIFIES the parameter the data supports; it does NOT
        construct an owned-parameter proposal or a proposed value (the level-to-owned-threshold mapping +
        the magnitude/direction are an unspecified design decision, surfaced - never fabricated)."""
        return identify_spearman_candidate(self._records)

    def _resolve_mae_mult(self) -> Decimal:
        """The module's current mae_mult: the CIATS-owned store value if written, else the registry
        seed (the stop-width theory needs the current value to nudge from)."""
        owned = self._store.get(MAE_MULT_PARAM)
        return _dec(owned if owned is not None else registry.value(MAE_MULT_PARAM))

    def _side(self) -> str:
        """The module's trading side token ('long' / 'short') for the entry-filter bound mapping."""
        return "short" if "short" in self._module.lower() else "long"

    def plan_from_drift(self, *, current_mae_mult: object = None) -> list:
        """The drift-triggered FORM -> TEST -> ROUTE loop (Bill's TB00750 directive). For each TESTABLE
        dial FORM a data-derived candidate, TEST it via open_pdca (the real shadow replay + the absolute
        CHECK), and ROUTE by result; FILE any not-yet-testable entry-filter candidate to the report
        track (never sham-tested). Returns the list of routed outcomes (an ApprovalRequested = the C1
        alert track / a CheckResult = the report track / a DeferredCandidate = filed pending a mapping /
        a PlanBlocked). The two-track routing already lives in open_pdca; this feeds it real candidates.

          Track 1 - the STOP-LOSS-WIDTH dial (fully testable today): correlate each trade's heat-taken
            (mae_pct_reached) against its outcome; on a qualifying Spearman FORM a mae_mult nudge (data-
            derived direction, bounded seed magnitude) and TEST it (shadow_replay scales the losses).
          Track 2 - the ENTRY-FILTER candidate identified over the per-trade signal_params LEVELS: map
            it to an owned SSS threshold + a data-derived bound and TEST it via the entry re-simulation
            (re-decide "would this trade have been entered"); an ema-period / unexpressible candidate is
            FILED to the report track (DeferredCandidate) instead.

        The sacred 1:1.5 R:R is never a candidate; nothing is applied without Bill's approval."""
        outcomes: list = []

        # Track 1: the stop-loss-width dial (mae_pct_reached -> mae_mult).
        cur = current_mae_mult if current_mae_mult is not None else self._resolve_mae_mult()
        theory = plan_stop_width_proposal(self._records, current_mae_mult=cur)
        if theory is not None:
            outcomes.append(
                self.open_pdca(
                    theory.proposal,
                    spearman_xy=(theory.levels, theory.outcomes),
                    out_of_cycle=True,
                )
            )

        # Track 2: the entry-filter candidate over the signal_params level series.
        candidate = self.identify_drift_candidate()
        if candidate is not None:
            self._emit(candidate)  # always SURFACE the identification (evt:CIATS_PLAN_CANDIDATE)
            proposal = plan_entry_filter_proposal(self._records, candidate, side=self._side())
            if proposal is not None:
                outcomes.append(
                    self.open_pdca(
                        proposal,
                        spearman_xy=(candidate.levels, candidate.outcomes),
                        out_of_cycle=True,
                    )
                )
            else:
                deferred = DeferredCandidate(
                    candidate=candidate,
                    reason=(
                        "no faithful testable mapping (ema level->period, an unexpressible single-bound "
                        "direction, or too few losing samples) - filed to the report track"
                    ),
                )
                self._emit(deferred)
                outcomes.append(deferred)
        return outcomes

    def _current_value(self, proposal: object) -> object:
        """The candidate parameter's current (pre-change) value the DO replay's scale ratio needs:
        the live store-owned value if CIATS has written it, else the proposal's own current_value."""
        name = str(getattr(proposal, "param_name", "") or getattr(proposal, "param", ""))
        owned = self._store.get(name)
        return owned if owned is not None else getattr(proposal, "current_value", None)

    def open_pdca(
        self,
        proposal: object,
        *,
        evaluator: "Callable[[object], object | None] | None" = None,
        spearman_xy: "tuple[Sequence[object], Sequence[object]] | None" = None,
        out_of_cycle: bool = False,
    ) -> object:
        """Run one PDCA cycle for a candidate change: PLAN (200-trade floor / sacred-R:R gated) -> DO
        (shadow_cohorts replays `evaluator` over THIS module's corpus -> candidate vs baseline) ->
        CHECK (the absolute Mann-Whitney gate). On a CHECK pass STAGE the proposal for the HR-CI-011
        approval surface (returns the ApprovalRequested); on a fail returns the CheckResult (REJECTED);
        a blocked PLAN / an empty candidate cohort returns a PlanBlocked. NEVER auto-applies.

        `evaluator` defaults to the REAL gate/exit corpus replay (build_shadow_evaluator over the
        proposal + this module's current parameter value): a gating change includes/excludes a record,
        a sizing/exit change scales its outcome. An evaluator may still be injected (tests / a custom
        counterfactual)."""
        if evaluator is None:
            evaluator = build_shadow_evaluator(proposal, current_value=self._current_value(proposal))
        candidate, baseline = shadow_cohorts(self._records, evaluator)
        if not candidate or not baseline:
            blocked = PlanBlocked("shadow-eval produced an empty cohort (no replayable corpus)")
            self._emit(blocked)
            return blocked
        planned = self._pdca.initiate_plan(
            proposal, trade_count=self.trade_count, out_of_cycle=out_of_cycle
        )
        if isinstance(planned, PlanBlocked):
            self._emit(planned)
            return planned
        self._pdca.run_do(candidate, baseline)
        check = self._pdca.run_check(spearman_xy=spearman_xy)
        self._emit(check)
        if check.passed:
            return self._stage_approval(proposal, check=check, kind="pdca")
        return check

    # --- the HR-CI-011 approval surface ----------------------------------------------------------
    def _stage_approval(self, proposal: object, *, check: object | None, kind: str) -> ApprovalRequested:
        """Stage a parameter change for Bill's HR-CI-011 decision: record it pending + emit
        ApprovalRequested through both the event sink and the approval surface (the SMTP alert seam)."""
        self._req_id += 1
        self._pending[self._req_id] = _Pending(self._req_id, proposal, check, kind)
        event = ApprovalRequested(self._req_id, proposal, check, kind)
        self._emit(event)
        if self._on_approval is not None:
            self._on_approval(event)
        return event

    def submit_approval(
        self, request_id: int, *, approved: bool, at_inter_trade_boundary: bool
    ) -> object:
        """Resolve a pending approval. On Bill approval (HR-CI-011) at a confirmed inter-trade boundary
        (HR-CI-003) apply the staged change to the Parameter Store (which owns + enforces the HR-CI-005
        50-trade interval) and emit + return the ParameterWritten/ParameterWriteRejected. A PDCA change
        runs through the PdcaEngine.act gate first. A failed gate returns a PlanBlocked and KEEPS the
        request pending (retryable at the next boundary); a successful write drops it."""
        pending = self._pending.get(request_id)
        if pending is None:
            raise ValueError(f"no pending approval with request_id {request_id}")

        if pending.kind == "pdca":
            trades_since = self._store.trades_since_last_change(self.trade_count)
            acted = self._pdca.act(
                approved=approved,
                at_inter_trade_boundary=at_inter_trade_boundary,
                trades_since_last_change=trades_since,
            )
            if isinstance(acted, PlanBlocked):
                self._emit(acted)
                return acted
            approved_change = acted
        else:  # "kelly" - no PDCA cohort gate; the boundary + approval are checked here
            if not approved:
                blocked = PlanBlocked("HR-CI-011: Bill approval required for every parameter change")
                self._emit(blocked)
                return blocked
            if not at_inter_trade_boundary:
                blocked = PlanBlocked("HR-CI-003: writes only at a confirmed inter-trade boundary")
                self._emit(blocked)
                return blocked
            approved_change = _KellyApprovedChange(proposal=pending.proposal)

        written = self._store.apply(approved_change, at_trade_count=self.trade_count)
        self._emit(written)
        self._pending.pop(request_id, None)
        return written

    def on_inter_trade_boundary(self, inbox: "ApprovalInbox") -> list:
        """The HR-CI-003 inter-trade-boundary edge: at a confirmed close with no order pending, poll
        the operator approval inbox and APPLY any staged change Bill has decided on. For each pending
        request with an inbox decision, run submit_approval at this boundary (at_inter_trade_boundary=
        True). A successful write drops the request (the decision is cleared); a Bill REJECTION is
        consumed (cleared); a change still gated (e.g. the HR-CI-005 50-trade interval not yet met)
        stays pending WITH its decision so it retries at the next boundary. NEVER auto-applies - it
        only acts on Bill's recorded decision. Returns the list of submit_approval outcomes."""
        outcomes: list = []
        for request_id in list(self._pending):
            decided = inbox.decision(request_id)
            if decided is None:
                continue  # Bill has not decided - leave it pending, re-poll next boundary
            outcome = self.submit_approval(
                request_id, approved=decided, at_inter_trade_boundary=True
            )
            outcomes.append(outcome)
            # Resolve the inbox decision once it is applied (no longer pending) or on a rejection;
            # a still-pending APPROVAL (an interval/gate not yet met) keeps its decision to retry.
            if request_id not in self._pending or decided is False:
                inbox.clear(request_id)
        return outcomes

    # --- the Gate-3 protective feed --------------------------------------------------------------
    def disallowed_regimes(self) -> list:
        """param:disallowed_regimes for gate:G3_Regime_Filter: the Regime Library's ACTIVE,
        negative-edge regimes (the protective block list). A regime with too little evidence is never
        disallowed. This is a DERIVED protective read (not a Bill-tuned parameter) the per-cycle
        snapshot surfaces to Gate-3."""
        return self._regimes.disallowed_regimes()


@dataclass(frozen=True)
class _KellyApprovedChange:
    """A Kelly recompute's hand-off to the Parameter Store: carries the proposal the store writes
    (its .param_name + .proposed_value). The store double-checks immutability + the 50-trade interval;
    the Half-Kelly math is the evidence (no PDCA cohort CHECK is run for a sizing recompute)."""

    proposal: object
    check: object | None = None
