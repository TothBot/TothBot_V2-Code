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
  open_pdca         - on a drift signal: PLAN a candidate (200-trade floor / sacred-R:R gated) -> DO
                      (shadow-replay the candidate over THIS corpus -> a candidate vs baseline cohort)
                      -> CHECK (the absolute Mann-Whitney gate) -> on a pass STAGE the proposal for
                      approval. NEVER auto-applies.
  submit_approval   - on Bill approval (HR-CI-011) at a confirmed inter-trade boundary (HR-CI-003),
                      apply the staged change to the Parameter Store (which owns the HR-CI-005 50-trade
                      interval) + log the parameter-evolution entry. A failed gate keeps the pending
                      request retryable at the next boundary.
  disallowed_regimes- the Regime Library's protective param:disallowed_regimes list (the ACTIVE,
                      negative-edge regimes) surfaced for gate:G3_Regime_Filter to read.

EVERYTHING here is PROPOSE/DETECT only: the conductor STAGES proposals and routes them to Bill; only an
approved change at an inter-trade boundary reaches the Parameter Store, and the sacred 1:1.5 R:R is
never a candidate. The two injected edges are the approval surface (`on_approval`, the mod:Logger
HR-LG-009 SMTP alert seam) + the inter-trade-boundary + approval signals (passed to submit_approval).
The shadow replay's per-trade counterfactual is the injected `evaluator` (a fuller build replaces it
with a real gate/exit replay; the seed-then-correct discipline). PURE state + Decimal-only (ar:AR-047)
apart from the injected sync edges.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal

from .pdca_engine import PdcaEngine, PlanBlocked
from .proposal_engine import (
    PER_TRADE_SIZE_PARAM,
    KellyNegative,
    KellyUpdate,
    ParameterChangeProposal,
    ProposalEngine,
)
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


@dataclass
class _Pending:
    """One staged-but-unapproved parameter change awaiting Bill's HR-CI-011 decision."""

    request_id: int
    proposal: object
    check: object | None
    kind: str


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

    def open_pdca(
        self,
        proposal: object,
        *,
        evaluator: Callable[[object], object | None],
        spearman_xy: "tuple[Sequence[object], Sequence[object]] | None" = None,
        out_of_cycle: bool = False,
    ) -> object:
        """Run one PDCA cycle for a candidate change: PLAN (200-trade floor / sacred-R:R gated) -> DO
        (shadow_cohorts replays `evaluator` over THIS module's corpus -> candidate vs baseline) ->
        CHECK (the absolute Mann-Whitney gate). On a CHECK pass STAGE the proposal for the HR-CI-011
        approval surface (returns the ApprovalRequested); on a fail returns the CheckResult (REJECTED);
        a blocked PLAN / an empty candidate cohort returns a PlanBlocked. NEVER auto-applies."""
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
