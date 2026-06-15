"""mod:CIATS conductor tests (ciats/conductor.py) - the per-module learning loop orchestration.

Covers the loop that ties the PURE CIATS units onto the organism: the learning-loop close
(ingest_close -> pool + regime bucket + the net-P/L series), the Half-Kelly 50-trade cadence
(KELLY_UPDATE stages a proposal; KELLY_NEGATIVE makes none, HR-CI-008), the PDCA drift trigger
(a cusum_lower lower-arm breach), the PLAN->DO(shadow replay)->CHECK cycle (open_pdca: a better
cohort stages for approval, an identical one rejects, the 200-trade floor + sacred R:R block), the
HR-CI-011 approval surface + the inter-trade-boundary/50-trade-interval gates at submit_approval,
and the protective param:disallowed_regimes read. PROPOSE/DETECT only - no write without an
approved submit_approval at a boundary.
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from tothbot.ciats.conductor import (
    ApprovalInbox,
    ApprovalRequested,
    CiatsConductor,
    DeferredCandidate,
    DriftSignal,
    shadow_cohorts,
)
from tothbot.ciats.parameter_store import ParameterStore, ParameterWritten
from tothbot.ciats.pdca_engine import CheckResult, PlanBlocked
from tothbot.ciats.pool import CiatsPool
from tothbot.ciats.proposal_engine import IdentifiedCandidate, KellyNegative, KellyUpdate
from tothbot.ciats.regime_library import RegimeLibrary
from tothbot.regime.taxonomy import Regime


def _close(net_pl, net_gain="0", net_loss="0"):
    """A minimal Stream-2 TRADE_CLOSE shape (the three NET-P/L fields the pool/library read)."""
    return SimpleNamespace(
        net_pl_usd=Decimal(net_pl), net_gain_usd=Decimal(net_gain), net_loss_usd=Decimal(net_loss)
    )


def _win(gain="2"):
    return _close(net_pl=gain, net_gain=gain, net_loss="0")


def _loss(loss="1"):
    return _close(net_pl=f"-{loss}", net_gain="0", net_loss=loss)


def _make(**kwargs):
    """A conductor over fresh per-module units (a small floor keeps the tests fast unless overridden)."""
    floor = kwargs.pop("floor", 200)
    events: list = []
    approvals: list = []
    pool = CiatsPool(trade_floor=floor)
    conductor = CiatsConductor(
        module="Long",
        pool=pool,
        regime_library=RegimeLibrary(),
        parameter_store=ParameterStore(),
        on_event=events.append,
        on_approval=approvals.append,
        **kwargs,
    )
    return conductor, events, approvals


def _seed_pool(conductor, *, wins, losses, gain="2", loss="1", regime=None):
    for _ in range(wins):
        conductor.ingest_close(_win(gain), regime=regime)
    for _ in range(losses):
        conductor.ingest_close(_loss(loss), regime=regime)


# --------------------------------------------------------------------------- shadow replay (pure)
def test_shadow_cohorts_baseline_is_realized_candidate_is_counterfactual():
    records = [_close("1"), _close("2"), _close("3")]
    cand, base = shadow_cohorts(records, lambda r: r.net_pl_usd + Decimal("10"))
    assert base == [Decimal("1"), Decimal("2"), Decimal("3")]
    assert cand == [Decimal("11"), Decimal("12"), Decimal("13")]


def test_shadow_cohorts_none_filters_a_record_from_the_candidate():
    records = [_close("1"), _close("2"), _close("3")]
    cand, base = shadow_cohorts(records, lambda r: None if r.net_pl_usd < 2 else r.net_pl_usd)
    assert base == [Decimal("1"), Decimal("2"), Decimal("3")]
    assert cand == [Decimal("2"), Decimal("3")]  # the gated-out trade left the candidate cohort


# --------------------------------------------------------------------------- ingest / learning close
def test_ingest_close_accumulates_pool_series_and_regime():
    conductor, _, _ = _make()
    conductor.ingest_close(_win("2"), regime=Regime.TRENDING_POS_NORMAL)
    conductor.ingest_close(_loss("1"), regime=Regime.TRENDING_POS_NORMAL)
    assert conductor.trade_count == 2
    assert conductor._net_pl == [Decimal("2"), Decimal("-1")]
    assert conductor._regimes.bucket_count(Regime.TRENDING_POS_NORMAL) == 2


# --------------------------------------------------------------------------- the Half-Kelly cadence
def test_kelly_returns_none_below_the_floor_and_off_cadence():
    conductor, _, _ = _make()
    _seed_pool(conductor, wins=60, losses=40)            # 100 trades < 200 floor
    assert conductor.recompute_kelly(wallet_balance=Decimal("1000")) is None


def test_kelly_update_at_cadence_emits_and_stages_a_proposal():
    conductor, events, approvals = _make()
    _seed_pool(conductor, wins=120, losses=80)           # 200 trades: W=0.6, R=2 -> K_full=0.4 > 0
    out = conductor.recompute_kelly(wallet_balance=Decimal("1000"))
    assert isinstance(out, KellyUpdate)
    assert out.k_full == Decimal("0.4")
    # the proposal was staged for the HR-CI-011 approval surface (kelly kind, no PDCA check)
    assert len(conductor.pending) == 1
    req = approvals[-1]
    assert isinstance(req, ApprovalRequested) and req.kind == "kelly" and req.check is None
    assert any(isinstance(e, KellyUpdate) for e in events)


def test_kelly_negative_makes_no_proposal_hr_ci_008():
    conductor, _, approvals = _make()
    _seed_pool(conductor, wins=60, losses=140, gain="1", loss="1")   # W=0.3, R=1 -> K_full=-0.4
    out = conductor.recompute_kelly(wallet_balance=Decimal("1000"))
    assert isinstance(out, KellyNegative)
    assert conductor.pending == ()          # no positive sizing proposal staged
    assert approvals == []


# --------------------------------------------------------------------------- the PDCA drift trigger
def test_scan_drift_quiet_on_a_stable_series():
    conductor, _, _ = _make()
    for _ in range(20):
        conductor.ingest_close(_close("5"))
        conductor.ingest_close(_close("4"))
    assert conductor.scan_drift() is None


def test_scan_drift_fires_a_cusum_lower_breach_on_degradation():
    conductor, events, _ = _make()
    for _ in range(5):
        conductor.ingest_close(_close("-5"))            # recent net-P/L well below an in-control mean
    signal = conductor.scan_drift(mu=Decimal("5"), sigma=Decimal("1"))
    assert isinstance(signal, DriftSignal)
    assert signal.kind == "cusum_lower"
    assert signal in events


# ------------------------------------------------------ the full per-close cadence (on_close, TB00749)
def test_on_close_runs_the_full_cadence_below_the_floor():
    # below the 200-trade activation: on_close ingests + scans (quiet) but stages nothing.
    conductor, _, approvals = _make()
    k, d, applied = conductor.on_close(_win(), wallet_balance=Decimal("1000"))
    assert conductor.trade_count == 1
    assert k is None and d is None and applied == []     # off cadence, no drift, no inbox
    assert approvals == []


def test_on_close_skips_the_kelly_recompute_when_no_wallet_balance():
    # live mode has no synthetic wallet (wallet_balance None): the recompute is skipped at the
    # cadence boundary (no _dec(None) crash) and the seed sizing stands.
    conductor, _, approvals = _make()
    _seed_pool(conductor, wins=119, losses=80)           # 199
    k, _d, _a = conductor.on_close(_win())               # 200th, wallet_balance None
    assert conductor.trade_count == 200
    assert k is None and conductor.pending == () and approvals == []


def test_on_close_stages_kelly_at_the_cadence_then_applies_at_the_next_boundary():
    # the running close drives the WHOLE PROPOSE->APPLY loop with no manual recompute: the 200th
    # close STAGES the Half-Kelly per_trade_size proposal; Bill approves it into the inbox; the next
    # confirmed close (the HR-CI-003 boundary) APPLIES it.
    conductor, _, approvals = _make()
    inbox = ApprovalInbox()
    _seed_pool(conductor, wins=119, losses=80)           # 199: W=0.6, R=2 -> K_full=0.4 > 0
    k, _d, applied = conductor.on_close(_win(), wallet_balance=Decimal("1000"), inbox=inbox)
    assert isinstance(k, KellyUpdate)                    # staged off the close, not a manual call
    assert len(conductor.pending) == 1 and applied == [] # pending, not applied (no decision yet)
    req = conductor.pending[0]
    assert isinstance(approvals[-1], ApprovalRequested) and approvals[-1].kind == "kelly"
    inbox.submit(req.request_id, approved=True)
    _k2, _d2, applied2 = conductor.on_close(_win(), wallet_balance=Decimal("1000"), inbox=inbox)
    assert conductor.parameter_store.get("per_trade_size_usd") == Decimal("200")  # K_half 0.2 * 1000
    assert applied2 and isinstance(applied2[0], ParameterWritten)


class _SustainedMon:
    """A drift monitor whose EWMA divergence is permanently sustained (forces a DriftSignal)."""

    sustained = True

    def update(self, _x):
        pass


def _close_sp(net_pl, *, rsi, vol=1):
    """A Stream-2 close carrying a signal_params dict (the TB00750 producer shape) - rsi_14 + a flat
    volume_ratio - plus the realized net-P/L the Spearman gate ranks the levels against."""
    rec = _close(net_pl=str(net_pl), net_gain=str(net_pl) if net_pl > 0 else "0",
                 net_loss="0" if net_pl > 0 else str(-net_pl))
    rec.signal_params = {"rsi_14": Decimal(str(rsi)), "volume_ratio": Decimal(str(vol))}
    return rec


def test_on_close_drives_the_drift_scan():
    # on_close runs scan_drift (the HR-CI-007 out-of-cycle PLAN trigger) at each close and surfaces
    # the DriftSignal. With no signal_params in the corpus no candidate is identified (nothing fabricated).
    conductor, events, _ = _make(drift_monitor=_SustainedMon())
    _k, drift, _a = conductor.on_close(_win())
    assert isinstance(drift, DriftSignal) and drift.kind == "ewma_sustained"
    assert drift in events
    assert not any(isinstance(e, IdentifiedCandidate) for e in events)   # no level series -> no candidate


# --------------------------------------------------- the drift-triggered PLAN candidate (TB00750 c)
def test_identify_drift_candidate_reads_the_per_trade_signal_params_levels():
    conductor, _, _ = _make()
    # a corpus whose per-trade rsi_14 level tracks the realized outcome (volume_ratio is flat).
    for i in range(10):
        conductor.ingest_close(_close_sp(net_pl=i, rsi=i))
    cand = conductor.identify_drift_candidate()
    assert isinstance(cand, IdentifiedCandidate)
    assert cand.level_key == "rsi_14"        # the strongest-|rho| qualifying level (volume_ratio is flat)
    assert cand.n == 10


def test_identify_drift_candidate_none_without_levels():
    conductor, _, _ = _make()
    for _ in range(10):
        conductor.ingest_close(_win())       # no signal_params on the records
    assert conductor.identify_drift_candidate() is None


def test_on_close_surfaces_a_plan_candidate_on_drift():
    # the full drift-triggered PLAN identification off the running close: drift fires + the corpus
    # carries a Spearman-qualified per-trade level -> the candidate is SURFACED (CIATS_PLAN_CANDIDATE).
    conductor, events, _ = _make(drift_monitor=_SustainedMon())
    for i in range(8):
        conductor.ingest_close(_close_sp(net_pl=i, rsi=i))
    conductor.on_close(_close_sp(net_pl=8, rsi=8))     # this close fires drift -> identify + surface
    cands = [e for e in events if isinstance(e, IdentifiedCandidate)]
    assert len(cands) == 1
    assert cands[0].level_key == "rsi_14"
    assert cands[0].code == "CIATS_PLAN_CANDIDATE"


# --------------------------------------------------------------------------- the PDCA cycle (open_pdca)
def _candidate(param="mae_mult", proposed="0.9"):
    return SimpleNamespace(param_name=param, current_value=Decimal("0.8"), proposed_value=Decimal(proposed))


def test_open_pdca_stages_a_clearly_better_candidate_for_approval():
    conductor, _, approvals = _make()
    _seed_pool(conductor, wins=120, losses=80)           # >= 200 floor
    out = conductor.open_pdca(_candidate(), evaluator=lambda r: r.net_pl_usd + Decimal("100"))
    assert isinstance(out, ApprovalRequested)
    assert out.kind == "pdca" and out.check.passed is True
    assert len(conductor.pending) == 1
    assert approvals[-1] is out


def test_open_pdca_rejects_an_identical_cohort():
    conductor, _, approvals = _make()
    _seed_pool(conductor, wins=120, losses=80)
    out = conductor.open_pdca(_candidate(), evaluator=lambda r: r.net_pl_usd)  # candidate == baseline
    assert isinstance(out, CheckResult) and out.passed is False
    assert conductor.pending == () and approvals == []


def test_open_pdca_blocked_below_the_floor():
    conductor, _, _ = _make()
    _seed_pool(conductor, wins=10, losses=10)            # 20 < 200
    out = conductor.open_pdca(_candidate(), evaluator=lambda r: r.net_pl_usd + Decimal("100"))
    assert isinstance(out, PlanBlocked)
    assert "floor" in out.reason


def test_open_pdca_blocks_the_sacred_rr():
    conductor, _, _ = _make()
    _seed_pool(conductor, wins=120, losses=80)
    out = conductor.open_pdca(_candidate(param="rr_floor"), evaluator=lambda r: r.net_pl_usd + Decimal("1"))
    assert isinstance(out, PlanBlocked)


def test_open_pdca_blocks_an_empty_candidate_cohort():
    conductor, _, _ = _make()
    _seed_pool(conductor, wins=120, losses=80)
    out = conductor.open_pdca(_candidate(), evaluator=lambda r: None)   # candidate would trade nothing
    assert isinstance(out, PlanBlocked)
    assert "empty" in out.reason


# --------------------------------------------------------------------- the REAL replay (default evaluator)
def _winr(regime, gain="2"):
    return SimpleNamespace(net_pl_usd=Decimal(gain), net_gain_usd=Decimal(gain), net_loss_usd=Decimal("0"),
                           asset_regime=regime.value)


def _lossr(regime, loss="1"):
    return SimpleNamespace(net_pl_usd=Decimal(f"-{loss}"), net_gain_usd=Decimal("0"),
                           net_loss_usd=Decimal(loss), asset_regime=regime.value)


def test_open_pdca_default_replay_gates_out_a_losing_regime():
    # No injected evaluator -> the conductor uses the REAL gate/exit corpus replay. A disallowed-regime
    # candidate excludes the losers booked in that regime, so the candidate cohort clearly out-ranks
    # the realized baseline -> CHECK passes and it stages for approval.
    conductor, _, approvals = _make()
    for i in range(120):                                      # varied winner gains (non-degenerate)
        conductor.ingest_close(_winr(Regime.TRENDING_POS_NORMAL, gain="2" if i % 2 else "3"),
                               regime=Regime.TRENDING_POS_NORMAL)
    for i in range(80):
        conductor.ingest_close(_lossr(Regime.NON_DIR_NORMAL, loss="1" if i % 2 else "2"),
                               regime=Regime.NON_DIR_NORMAL)
    proposal = SimpleNamespace(param_name="disallowed_regimes", current_value=None,
                               proposed_value=Regime.NON_DIR_NORMAL)
    out = conductor.open_pdca(proposal)                       # default = build_shadow_evaluator
    assert isinstance(out, ApprovalRequested) and out.check.passed is True
    assert approvals[-1] is out


def test_open_pdca_default_replay_rejects_a_no_op_gating_candidate():
    # Blocking a regime that booked NO trades changes nothing -> candidate == baseline -> CHECK fails.
    conductor, _, approvals = _make()
    for _ in range(120):
        conductor.ingest_close(_winr(Regime.TRENDING_POS_NORMAL), regime=Regime.TRENDING_POS_NORMAL)
    for _ in range(80):
        conductor.ingest_close(_lossr(Regime.TRENDING_POS_NORMAL), regime=Regime.TRENDING_POS_NORMAL)
    proposal = SimpleNamespace(param_name="disallowed_regimes", current_value=None,
                               proposed_value=Regime.NON_DIR_ELEVATED)   # no trades booked here
    out = conductor.open_pdca(proposal)
    assert isinstance(out, CheckResult) and out.passed is False
    assert conductor.pending == () and approvals == []


# --------------------------------------------------------------------------- the approval surface
def test_submit_approval_kelly_writes_the_store_on_approval_at_a_boundary():
    conductor, _, approvals = _make()
    _seed_pool(conductor, wins=120, losses=80)
    conductor.recompute_kelly(wallet_balance=Decimal("1000"))
    req = approvals[-1]
    written = conductor.submit_approval(req.request_id, approved=True, at_inter_trade_boundary=True)
    assert isinstance(written, ParameterWritten)
    assert conductor._store.get("per_trade_size_usd") == written.change.new_value
    assert conductor.pending == ()          # consumed


def test_submit_approval_without_bill_approval_keeps_the_request_pending():
    conductor, _, approvals = _make()
    _seed_pool(conductor, wins=120, losses=80)
    conductor.recompute_kelly(wallet_balance=Decimal("1000"))
    req = approvals[-1]
    out = conductor.submit_approval(req.request_id, approved=False, at_inter_trade_boundary=True)
    assert isinstance(out, PlanBlocked)
    assert len(conductor.pending) == 1      # retryable, not consumed


def test_submit_approval_off_a_boundary_keeps_the_request_pending():
    conductor, _, approvals = _make()
    _seed_pool(conductor, wins=120, losses=80)
    conductor.recompute_kelly(wallet_balance=Decimal("1000"))
    req = approvals[-1]
    out = conductor.submit_approval(req.request_id, approved=True, at_inter_trade_boundary=False)
    assert isinstance(out, PlanBlocked)
    assert len(conductor.pending) == 1


def test_submit_approval_pdca_runs_act_then_writes_the_store():
    conductor, _, approvals = _make()
    _seed_pool(conductor, wins=120, losses=80)           # 200 trades; no prior change -> interval met
    conductor.open_pdca(_candidate(), evaluator=lambda r: r.net_pl_usd + Decimal("100"))
    req = approvals[-1]
    written = conductor.submit_approval(req.request_id, approved=True, at_inter_trade_boundary=True)
    assert isinstance(written, ParameterWritten)
    assert conductor._store.get("mae_mult") == Decimal("0.9")


def test_submit_approval_unknown_request_raises():
    conductor, _, _ = _make()
    with pytest.raises(ValueError):
        conductor.submit_approval(999, approved=True, at_inter_trade_boundary=True)


# --------------------------------------------------------------------------- the approval inbox + boundary
def test_inbox_records_and_clears_an_operator_decision():
    inbox = ApprovalInbox()
    assert inbox.decision(1) is None             # undecided
    inbox.submit(1, approved=True)
    assert inbox.decision(1) is True
    inbox.clear(1)
    assert inbox.decision(1) is None


def test_boundary_applies_a_bill_approved_change():
    conductor, _, approvals = _make()
    _seed_pool(conductor, wins=120, losses=80)   # 200 trades; no prior change -> interval met
    conductor.recompute_kelly(wallet_balance=Decimal("1000"))
    req = approvals[-1]
    inbox = ApprovalInbox()
    inbox.submit(req.request_id, approved=True)   # Bill approves (the injected operator edge)
    outcomes = conductor.on_inter_trade_boundary(inbox)
    assert len(outcomes) == 1 and isinstance(outcomes[0], ParameterWritten)
    assert conductor.pending == ()                # applied + consumed
    assert inbox.decision(req.request_id) is None  # the decision was cleared


def test_boundary_is_a_no_op_without_a_decision():
    conductor, _, approvals = _make()
    _seed_pool(conductor, wins=120, losses=80)
    conductor.recompute_kelly(wallet_balance=Decimal("1000"))
    req = approvals[-1]
    assert conductor.on_inter_trade_boundary(ApprovalInbox()) == []  # Bill has not decided
    assert len(conductor.pending) == 1            # left pending, re-polled next boundary
    # this is the never-auto-apply invariant: no decision -> no write
    assert conductor._store.get("per_trade_size_usd") is None


def test_boundary_consumes_a_bill_rejection():
    conductor, _, approvals = _make()
    _seed_pool(conductor, wins=120, losses=80)
    conductor.recompute_kelly(wallet_balance=Decimal("1000"))
    req = approvals[-1]
    inbox = ApprovalInbox()
    inbox.submit(req.request_id, approved=False)  # Bill rejects
    outcomes = conductor.on_inter_trade_boundary(inbox)
    assert len(outcomes) == 1 and isinstance(outcomes[0], PlanBlocked)
    assert inbox.decision(req.request_id) is None  # the rejection was consumed (not re-applied)
    assert conductor._store.get("per_trade_size_usd") is None  # never written


# ------------------------------------------------ the drift-triggered FORM->TEST->ROUTE loop (TB00751 b)
def _heat_close(net_pl, heat, **sp):
    """A TRADE_CLOSE-shaped record: net P/L + a heat-taken level (mae_pct_reached) + signal_params."""
    n = Decimal(str(net_pl))
    rec = _close(net_pl=str(n), net_gain=str(n) if n > 0 else "0", net_loss="0" if n > 0 else str(-n))
    rec.mae_pct_reached = Decimal(str(heat))
    if sp:
        rec.signal_params = {k: Decimal(str(v)) for k, v in sp.items()}
    return rec


def _ingest(conductor, records):
    for r in records:
        conductor.ingest_close(r)


def test_plan_from_drift_stages_a_stop_width_tighten_for_approval():
    # Track 1: losers ran HOT (heat 5, -10) and winners ran COOL (heat 1, +5) -> heat predicts loss
    # (rho < 0) -> a mae_mult TIGHTEN; the real replay scales the losses down -> CHECK passes -> a C1
    # alert (ApprovalRequested) reaches Bill.
    conductor, _, approvals = _make()
    _ingest(conductor, [_heat_close(5, 1) for _ in range(100)]      # winners, cool
                       + [_heat_close(-10, 5) for _ in range(100)])  # losers, hot
    outcomes = conductor.plan_from_drift()
    assert any(isinstance(o, ApprovalRequested) and o.check.passed for o in outcomes)
    assert any(isinstance(a, ApprovalRequested) and a.proposal.param_name == "mae_mult" for a in approvals)


def test_plan_from_drift_reports_a_stop_width_loosen_no_alert():
    # Track 1, the report track: winners ran HOT and losers ran COOL -> heat predicts a WIN (rho > 0)
    # -> a LOOSEN; the replay can only scale the losses UP (it cannot credit saved winners) -> CHECK
    # fails -> the CheckResult is REPORTED (no ApprovalRequested, no C1 alert).
    conductor, _, approvals = _make()
    _ingest(conductor, [_heat_close(5, 5) for _ in range(100)]       # winners, hot
                       + [_heat_close(-10, 1) for _ in range(100)])  # losers, cool
    outcomes = conductor.plan_from_drift()
    assert any(isinstance(o, CheckResult) and o.passed is False for o in outcomes)
    assert not any(isinstance(a, ApprovalRequested) and a.proposal.param_name == "mae_mult" for a in approvals)


def test_plan_from_drift_tests_a_volume_entry_filter_candidate():
    # Track 2 (the entry re-simulation): low-volume trades lose (rho > 0) -> raise volume_sss_threshold
    # to the loser-median; the re-gate replay EXCLUDES the low-volume losers -> CHECK passes -> alert.
    conductor, _, approvals = _make()
    # varied winner gains keep the post-exclusion cohort non-degenerate (real net P/L always varies).
    _ingest(conductor, [_heat_close(5 if i % 2 else 6, 1, volume_ratio=2) for i in range(100)]  # high vol
                       + [_heat_close(-10, 1, volume_ratio="1.2") for _ in range(100)])  # losers, low vol
    conductor.plan_from_drift()
    assert any(isinstance(a, ApprovalRequested) and a.proposal.param_name == "volume_sss_threshold"
               for a in approvals)


def test_plan_from_drift_files_a_deferred_ema_candidate_to_the_report_track():
    # Track 2, the report track: an ema_9 level correlates with outcome but maps to a PERIOD, not a
    # threshold -> not re-simulatable -> FILED (DeferredCandidate), never sham-tested.
    conductor, events, approvals = _make()
    _ingest(conductor, [_heat_close(i - 5, 1, ema_9=i) for i in range(12)])   # ema_9 tracks the outcome
    outcomes = conductor.plan_from_drift()
    deferred = [o for o in outcomes if isinstance(o, DeferredCandidate)]
    assert len(deferred) == 1 and deferred[0].candidate.level_key == "ema_9"
    assert any(isinstance(e, DeferredCandidate) for e in events)
    assert not any(isinstance(a, ApprovalRequested) for a in approvals)   # not brought to Bill


def test_on_close_runs_plan_from_drift_on_a_drift_signal():
    # the full wiring: a degrading close fires scan_drift -> on_close runs plan_from_drift -> the
    # stop-width tighten is staged off the running close (no manual plan_from_drift call).
    conductor, _, approvals = _make(drift_monitor=_SustainedMon())
    _ingest(conductor, [_heat_close(5, 1) for _ in range(100)] + [_heat_close(-10, 5) for _ in range(99)])
    conductor.on_close(_heat_close(-10, 5))   # the 200th close fires drift -> plan_from_drift
    assert any(isinstance(a, ApprovalRequested) and a.proposal.param_name == "mae_mult" for a in approvals)


# --------------------------------------------------------------------------- the Gate-3 protective feed
def test_disallowed_regimes_surfaces_the_regime_library_block_list():
    conductor, _, _ = _make()
    # A negative-edge regime, ACTIVE once the library has 600 total + 100 in the bucket.
    bad = Regime.NON_DIR_NORMAL
    for _ in range(40):
        conductor.ingest_close(_win("1"), regime=bad)
    for _ in range(100):
        conductor.ingest_close(_loss("5"), regime=bad)   # heavy losses -> negative edge
    # pad the library total to >= 600 with another regime's trades (its own bucket stays < 100-active)
    for _ in range(460):
        conductor.ingest_close(_win("1"), regime=Regime.TRENDING_POS_NORMAL)
    assert bad in conductor.disallowed_regimes()
