"""mod:CIATS_PDCA_Engine - the Plan-Do-Check-Act parameter-improvement cycle (PROPOSAL ONLY).

Source: 0500000 dv1_250 sec 6/7 mod:CIATS_PDCA_Engine + the rule:HR-CI-* family:
  HR-CI-004  the 200-trade HARD FLOOR - no inference / no PLAN before 200 closed trades (absolute).
  HR-CI-007  Mann-Whitney U alpha=0.01 ONE-SIDED vs the null (no-change baseline) is the ABSOLUTE
             CHECK-phase advancement gate; an OUT-OF-CYCLE PLAN initiates on a net_gain performance
             CUSUM lower-arm signal REGARDLESS of the regular inter-trade cadence.
  HR-CI-003  ACT writes ONLY at a confirmed inter-trade boundary (never mid-pipeline, never while an
             order is pending).
  HR-CI-005  a 50-trade MINIMUM interval between parameter changes (gates the ACT write).
  HR-CI-011  Bill approval is required for ALL parameter changes (the ACT gate).
  Sacred_R_R_1_to_1_5  the 1:1.5 R:R is NEVER a tunable - it can never be a PDCA candidate.

THE CYCLE (a PURE state machine; DETECTION/PROPOSAL ONLY - it NEVER writes a parameter, the ACT
produces an approved change the mod:CIATS_Parameter_Store applies):
  PLAN  - a statistical signal (a cusum_lower breach on net_gain, an EWMA sustained divergence, a
          threshold cross) nominates a candidate parameter change. Gated by the 200-trade floor; a
          CUSUM lower-arm breach forces an OUT-OF-CYCLE PLAN. The sacred R:R is rejected outright.
  DO    - shadow-evaluate the candidate against the historical corpus -> a candidate outcome cohort
          (vs the baseline cohort = the actual outcomes under the current parameter). The cohorts are
          injected (the shadow-evaluation/simulation is the caller's; the engine evaluates the series).
  CHECK - the ABSOLUTE gate: Mann-Whitney U one-sided alpha=0.01, candidate vs baseline (the candidate
          must rank significantly HIGHER = better). Corroborating evidence surfaced for the HR-CI-011
          approval: the Sharpe ratio of both cohorts (did risk-adjusted return improve?) and, when a
          paired (param, outcome) series is supplied, Spearman rho (|rho| > 0.3 AND p < 0.05).
  ACT   - ONLY on Bill approval (HR-CI-011) AT a confirmed inter-trade boundary (HR-CI-003) with >= 50
          trades since the last change (HR-CI-005): emit the ApprovedChange for the Parameter Store.

PURE, Decimal-only (ar:AR-047). The statistics come from statistical_engine.py; the corpus stats from
pool.py. The CHECK thresholds (alpha=0.01, |rho|>0.3, p<0.05, the 200/50-trade counts) are the
diagram-named rule constants - transcribed, not new seeds. The Spearman p<0.05 uses the large-sample
t -> normal approximation (|t| > 1.96), valid because CHECK runs only at n >= 200 (HR-CI-004).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

from .pool import CIATS_TRADE_FLOOR
from .statistical_engine import mann_whitney_u, sharpe_ratio, spearman_significant

# --- diagram-named rule constants (transcribed; not CIATS-owned seeds) --------------------------
CHECK_MW_ALPHA_ONE_SIDED = Decimal("0.01")     # HR-CI-007 absolute gate
MIN_TRADES_BETWEEN_CHANGES = 50                # HR-CI-005
SACRED_RR_PARAM = "rr_floor"                   # the 1:1.5 R:R - NEVER a PDCA candidate

# One-sided normal critical z for the Mann-Whitney CHECK gate (method constants, not CIATS seeds).
_Z_ONE_SIDED: dict[str, Decimal] = {"0.01": Decimal("2.326348"), "0.05": Decimal("1.644854"),
                                    "0.001": Decimal("3.090232")}


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


class PdcaPhase(Enum):
    """The PDCA cycle phase for one candidate parameter change."""

    IDLE = "idle"
    PLAN = "plan"                          # a candidate nominated, awaiting the DO shadow-eval
    DO = "do"                              # cohorts recorded, awaiting CHECK
    AWAITING_APPROVAL = "awaiting_approval"  # CHECK passed; awaiting the HR-CI-011 Bill approval
    ADVANCED = "advanced"                  # ACT emitted the ApprovedChange (Parameter Store writes)
    REJECTED = "rejected"                  # CHECK failed the absolute gate (no change)


@dataclass(frozen=True)
class PlanBlocked:
    """evt:PDCA_PLAN_BLOCKED [INFO] {reason} - a PLAN could not initiate (below the 200-trade floor,
    or the candidate is the sacred R:R). Surfaced, never silently dropped."""

    reason: str
    code: str = field(default="PDCA_PLAN_BLOCKED", init=False)


@dataclass(frozen=True)
class SpearmanEvidence:
    """The optional Spearman corroboration (|rho| > 0.3 AND p < 0.05): a monotone association between
    the candidate parameter level and the outcome (when a paired series is supplied to CHECK)."""

    rho: Decimal
    significant: bool


@dataclass(frozen=True)
class CheckResult:
    """The CHECK-phase verdict. passed == the ABSOLUTE Mann-Whitney one-sided alpha=0.01 gate
    (HR-CI-007: the candidate ranks significantly HIGHER than the baseline). mw_z is the signed
    statistic; sharpe_* + spearman are corroborating evidence surfaced for the HR-CI-011 approval."""

    passed: bool
    mw_z: Decimal
    mw_crit: Decimal
    sharpe_candidate: Decimal
    sharpe_baseline: Decimal
    sharpe_improved: bool
    spearman: SpearmanEvidence | None
    code: str = field(default="PDCA_CHECK_RESULT", init=False)


@dataclass(frozen=True)
class ApprovedChange:
    """evt:PDCA_CHANGE_ADVANCED [HIGH] - the ACT-phase output: a Bill-approved parameter change ready
    for the mod:CIATS_Parameter_Store to write at this inter-trade boundary (HR-CI-003/011). The PDCA
    engine NEVER writes the parameter itself - this is the hand-off, not the write."""

    proposal: object
    check: CheckResult
    code: str = field(default="PDCA_CHANGE_ADVANCED", init=False)


def _proposal_param(proposal: object) -> str:
    """The parameter name a proposal targets (duck-typed: .param_name or .param)."""
    return str(getattr(proposal, "param_name", None) or getattr(proposal, "param", ""))


def check_phase(
    candidate_outcomes: Sequence[object],
    baseline_outcomes: Sequence[object],
    *,
    alpha: object = CHECK_MW_ALPHA_ONE_SIDED,
    spearman_xy: tuple[Sequence[object], Sequence[object]] | None = None,
) -> CheckResult:
    """The CHECK-phase statistics. The ABSOLUTE gate (HR-CI-007): a one-sided Mann-Whitney U at
    `alpha` - the candidate cohort must rank significantly HIGHER (better net outcomes) than the
    baseline. Corroboration: the Sharpe ratio of each cohort (improved?) and, if `spearman_xy` is
    supplied, the Spearman rho gate (|rho| > 0.3 AND p < 0.05, large-sample t). PURE."""
    mw = mann_whitney_u(candidate_outcomes, baseline_outcomes)
    crit = _Z_ONE_SIDED.get(str(_dec(alpha)), _Z_ONE_SIDED["0.01"])
    # One-sided: the candidate (sample A) ranks ABOVE the baseline -> z > +crit.
    passed = mw.z > crit

    sharpe_c = sharpe_ratio(candidate_outcomes)
    sharpe_b = sharpe_ratio(baseline_outcomes)

    spearman_ev: SpearmanEvidence | None = None
    if spearman_xy is not None:
        rho, sig = spearman_significant(spearman_xy[0], spearman_xy[1])
        spearman_ev = SpearmanEvidence(rho=rho, significant=sig)

    return CheckResult(
        passed=passed,
        mw_z=mw.z,
        mw_crit=crit,
        sharpe_candidate=sharpe_c,
        sharpe_baseline=sharpe_b,
        sharpe_improved=sharpe_c > sharpe_b,
        spearman=spearman_ev,
    )


class PdcaEngine:
    """One module's PDCA cycle controller (per-module, like the pool). Drives ONE candidate change
    at a time through PLAN -> DO -> CHECK -> (Bill approval) -> ACT. DETECTION/PROPOSAL ONLY - the
    ACT emits an ApprovedChange; the mod:CIATS_Parameter_Store performs the actual write."""

    def __init__(self, *, trade_floor: int = CIATS_TRADE_FLOOR) -> None:
        self._trade_floor = trade_floor
        self._phase = PdcaPhase.IDLE
        self._proposal: object | None = None
        self._check: CheckResult | None = None

    @property
    def phase(self) -> PdcaPhase:
        return self._phase

    def initiate_plan(
        self,
        proposal: object,
        *,
        trade_count: int,
        out_of_cycle: bool = False,
    ) -> object:
        """PLAN: nominate a candidate change. Gated by the 200-trade floor (HR-CI-004); the sacred
        R:R can NEVER be a candidate (returns PlanBlocked). `out_of_cycle` records that a CUSUM
        lower-arm breach forced this PLAN off the regular cadence (HR-CI-007) - it does NOT relax the
        floor. Returns the proposal (now in PLAN) or a PlanBlocked event."""
        if _proposal_param(proposal) == SACRED_RR_PARAM:
            return PlanBlocked("sacred 1:1.5 R:R is never a tunable (Sacred_R_R_1_to_1_5)")
        if trade_count < self._trade_floor:
            return PlanBlocked(
                f"below the {self._trade_floor}-trade hard floor (HR-CI-004): {trade_count}"
            )
        self._proposal = proposal
        self._check = None
        self._phase = PdcaPhase.PLAN
        return proposal

    def run_do(
        self,
        candidate_outcomes: Sequence[object],
        baseline_outcomes: Sequence[object],
    ) -> None:
        """DO: record the shadow-evaluated candidate cohort + the baseline cohort (the outcomes under
        the proposed vs the current parameter). Only valid from PLAN."""
        if self._phase is not PdcaPhase.PLAN:
            raise ValueError(f"run_do requires the PLAN phase, in {self._phase}")
        self._candidate = list(candidate_outcomes)
        self._baseline = list(baseline_outcomes)
        self._phase = PdcaPhase.DO

    def run_check(
        self,
        *,
        spearman_xy: tuple[Sequence[object], Sequence[object]] | None = None,
        alpha: object = CHECK_MW_ALPHA_ONE_SIDED,
    ) -> CheckResult:
        """CHECK: run the absolute Mann-Whitney gate + corroboration over the DO cohorts. Advances to
        AWAITING_APPROVAL on a pass, REJECTED on a fail. Only valid from DO."""
        if self._phase is not PdcaPhase.DO:
            raise ValueError(f"run_check requires the DO phase, in {self._phase}")
        result = check_phase(self._candidate, self._baseline, alpha=alpha, spearman_xy=spearman_xy)
        self._check = result
        self._phase = PdcaPhase.AWAITING_APPROVAL if result.passed else PdcaPhase.REJECTED
        return result

    def act(
        self,
        *,
        approved: bool,
        at_inter_trade_boundary: bool,
        trades_since_last_change: int,
    ) -> object:
        """ACT: emit the Bill-approved change for the Parameter Store. Requires the HR-CI-011 Bill
        approval AND a confirmed inter-trade boundary (HR-CI-003) AND >= 50 trades since the last
        change (HR-CI-005). Returns an ApprovedChange (-> ADVANCED) or a PlanBlocked (the gate that
        failed; the engine stays AWAITING_APPROVAL so the ACT can retry at the next boundary). Only
        valid from AWAITING_APPROVAL. The engine NEVER writes the parameter - this is the hand-off."""
        if self._phase is not PdcaPhase.AWAITING_APPROVAL:
            raise ValueError(f"act requires the AWAITING_APPROVAL phase, in {self._phase}")
        if not approved:
            return PlanBlocked("HR-CI-011: Bill approval required for every parameter change")
        if not at_inter_trade_boundary:
            return PlanBlocked("HR-CI-003: writes only at a confirmed inter-trade boundary")
        if trades_since_last_change < MIN_TRADES_BETWEEN_CHANGES:
            return PlanBlocked(
                f"HR-CI-005: {trades_since_last_change} < {MIN_TRADES_BETWEEN_CHANGES}-trade interval"
            )
        assert self._check is not None
        self._phase = PdcaPhase.ADVANCED
        return ApprovedChange(proposal=self._proposal, check=self._check)
