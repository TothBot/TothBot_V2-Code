"""mod:CIATS_Proposal_Engine tests (ciats/proposal_engine.py).

Covers the Half-Kelly per_trade_size_usd proposal (K_full = W-(1-W)/R, K_half, K_half*wallet; the
50-trade recompute cadence; KellyNegative when K_full <= 0 per HR-CI-008; None below the 200-trade
floor) and the Spearman-qualified candidate-parameter proposal (|rho|>0.3 AND p<0.05 qualifies; a
constant/no-association series or the sacred R:R never qualifies).
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.config import registry
from tothbot.ciats.pool import CiatsPool
from tothbot.ciats.proposal_engine import (
    KELLY_RECOMPUTE_INTERVAL,
    IdentifiedCandidate,
    KellyNegative,
    KellyUpdate,
    ParameterChangeProposal,
    ProposalEngine,
    StopWidthTheory,
    identify_spearman_candidate,
    plan_entry_filter_proposal,
    plan_stop_width_proposal,
)


def _winning_pool(wins=120, losses=80, gain=2, loss=1):
    # W = wins/(wins+losses); R = gain/loss. wins=120 losses=80 gain2 loss1 -> W=0.6 R=2 K_full=0.4.
    p = CiatsPool()
    for _ in range(wins):
        p.ingest_outcome(net_pl=1, net_gain=gain, net_loss=0)
    for _ in range(losses):
        p.ingest_outcome(net_pl=-1, net_gain=0, net_loss=loss)
    return p


def _losing_pool():
    p = CiatsPool()
    for _ in range(60):
        p.ingest_outcome(net_pl=1, net_gain=1, net_loss=0)
    for _ in range(140):
        p.ingest_outcome(net_pl=-1, net_gain=0, net_loss=1)
    return p


def _seq(lo, hi):
    return [Decimal(x) for x in range(lo, hi)]


# --------------------------------------------------------------------------- kelly cadence
def test_kelly_due_at_floor_and_every_50():
    pe = ProposalEngine()
    assert pe.kelly_due(199) is False
    assert pe.kelly_due(200) is True
    assert pe.kelly_due(250) is True
    assert pe.kelly_due(251) is False
    assert pe.kelly_due(200 + KELLY_RECOMPUTE_INTERVAL) is True


# --------------------------------------------------------------------------- Half-Kelly sizing
def test_kelly_sizing_half_of_full_times_wallet():
    pe = ProposalEngine()
    k = pe.kelly_sizing(_winning_pool(), Decimal("5000"))
    assert isinstance(k, KellyUpdate)
    assert k.k_full == Decimal("0.4")          # 0.6 - 0.4/2
    assert k.k_half == Decimal("0.20")         # half
    assert k.per_trade_size_usd == Decimal("1000.00")  # 0.20 * 5000


def test_kelly_negative_when_edge_is_negative():
    pe = ProposalEngine()
    out = pe.kelly_sizing(_losing_pool(), Decimal("5000"))
    assert isinstance(out, KellyNegative)
    assert out.k_full <= Decimal("0")


def test_kelly_none_below_the_floor():
    pe = ProposalEngine()
    p = CiatsPool()
    for _ in range(50):
        p.ingest_outcome(net_pl=1, net_gain=2, net_loss=0)
    assert pe.kelly_sizing(p, Decimal("5000")) is None


def test_per_trade_size_proposal_wraps_kelly():
    pe = ProposalEngine()
    prop = pe.per_trade_size_proposal(_winning_pool(), Decimal("5000"), current_size=Decimal("50"))
    assert isinstance(prop, ParameterChangeProposal)
    assert prop.param_name == "per_trade_size_usd"
    assert prop.proposed_value == Decimal("1000.00")
    assert prop.current_value == Decimal("50")


def test_per_trade_size_proposal_passes_through_kelly_negative():
    pe = ProposalEngine()
    assert isinstance(pe.per_trade_size_proposal(_losing_pool(), Decimal("5000")), KellyNegative)


# --------------------------------------------------------------------------- candidate proposals
def test_candidate_qualifies_on_strong_monotone_association():
    pe = ProposalEngine()
    prop = pe.candidate_proposal(
        "mae_mult", "1.5", "1.6", param_levels=_seq(0, 220), outcomes=_seq(0, 220)
    )
    assert isinstance(prop, ParameterChangeProposal)
    assert prop.param_name == "mae_mult"


def test_candidate_rejected_when_no_association():
    pe = ProposalEngine()
    flat = [Decimal("1") for _ in range(220)]
    assert pe.candidate_proposal(
        "mae_mult", "1.5", "1.6", param_levels=_seq(0, 220), outcomes=flat
    ) is None


def test_candidate_never_proposes_sacred_rr():
    pe = ProposalEngine()
    assert pe.candidate_proposal(
        "rr_floor", "1.5", "1.6", param_levels=_seq(0, 220), outcomes=_seq(0, 220)
    ) is None


def test_candidate_carries_regime_tag():
    pe = ProposalEngine()
    prop = pe.candidate_proposal(
        "adx_threshold", "25", "27", param_levels=_seq(0, 220), outcomes=_seq(0, 220),
        regime="TRENDING_POS_NORMAL",
    )
    assert prop is not None
    assert prop.regime == "TRENDING_POS_NORMAL"


# --------------------------------------------------------------------------- PLAN identification
class _Rec:
    """A minimal Stream-2 TRADE_CLOSE-shaped record: signal_params dict + net_pl_usd outcome."""

    def __init__(self, signal_params, net_pl):
        self.signal_params = signal_params
        self.net_pl_usd = Decimal(str(net_pl))


def _corpus(levels_by_key, outcomes):
    """Build N records: each carries a signal_params dict {key: levels_by_key[key][i]} + outcomes[i]."""
    n = len(outcomes)
    out = []
    for i in range(n):
        sp = {key: Decimal(str(levels[i])) for key, levels in levels_by_key.items()}
        out.append(_Rec(sp, outcomes[i]))
    return out


def test_identify_picks_the_strongest_qualifying_level():
    # rsi_14 is perfectly monotone with the outcome (|rho|=1); volume_ratio is flat (no association).
    n = 60
    records = _corpus(
        {"rsi_14": list(range(n)), "volume_ratio": [1] * n},
        outcomes=list(range(n)),
    )
    cand = identify_spearman_candidate(records)
    assert isinstance(cand, IdentifiedCandidate)
    assert cand.level_key == "rsi_14"
    assert cand.n == n
    assert abs(cand.rho) == Decimal(1)
    assert cand.code == "CIATS_PLAN_CANDIDATE"
    # it surfaces the qualifying series (the open_pdca CHECK Spearman-corroboration input)
    assert len(cand.levels) == n and len(cand.outcomes) == n


def test_identify_returns_none_when_no_level_qualifies():
    n = 40
    records = _corpus({"rsi_14": [5] * n, "ema_9": list(range(n))}, outcomes=[7] * n)
    # rsi_14 constant + the outcome constant -> no monotone association on any key.
    assert identify_spearman_candidate(records) is None


def test_identify_skips_records_without_signal_params():
    # records with signal_params None / missing the key are skipped, never crash the rank.
    n = 50
    good = _corpus({"rsi_14": list(range(n))}, outcomes=list(range(n)))
    noisy = good + [_Rec(None, 5), _Rec({"sss_pass": True}, 9)]
    cand = identify_spearman_candidate(noisy)
    assert cand is not None and cand.level_key == "rsi_14"
    assert cand.n == n          # only the n records carrying rsi_14 were paired


def test_identify_skips_a_degenerate_key_under_min_pairs():
    # only 2 records carry the key -> below min_pairs -> skipped (no spurious candidate).
    records = [_Rec({"rsi_14": Decimal(1)}, 1), _Rec({"rsi_14": Decimal(2)}, 2)]
    assert identify_spearman_candidate(records) is None


# ---------------------------------------------------- stop-loss-width theory (mae_mult, TB00751 a)
class _HeatRec:
    """A minimal TRADE_CLOSE-shaped record: a heat-taken level (mae_pct_reached) + the net-P/L."""

    def __init__(self, mae_pct_reached, net_pl):
        self.mae_pct_reached = Decimal(str(mae_pct_reached))
        self.net_pl_usd = Decimal(str(net_pl))


def test_seed_mae_mult_nudge_pct_registered():
    # the genuine new seed: a CIATS-owned per-module relative step, default 0.10 (10pct).
    assert registry.value("mae_mult_nudge_pct") == 0.10


def test_stop_width_tightens_when_more_heat_predicts_loss():
    # heat rises as the outcome falls (rho = -1): more heat predicts a worse outcome -> TIGHTEN.
    records = [_HeatRec(mae_pct_reached=i, net_pl=(20 - i)) for i in range(20)]
    theory = plan_stop_width_proposal(records, current_mae_mult=Decimal("1.5"))
    assert isinstance(theory, StopWidthTheory)
    assert theory.tighten is True
    assert theory.rho < 0
    assert theory.proposal.param_name == "mae_mult"
    assert theory.proposal.proposed_value == Decimal("1.5") * Decimal("0.9")   # 10pct tighter
    assert len(theory.levels) == 20 and len(theory.outcomes) == 20             # the CHECK series


def test_stop_width_loosens_when_more_heat_then_recovers():
    # heat rises with the outcome (rho = +1): the trades that ran hot then recovered -> LOOSEN.
    records = [_HeatRec(mae_pct_reached=i, net_pl=i) for i in range(20)]
    theory = plan_stop_width_proposal(records, current_mae_mult=Decimal("1.5"))
    assert isinstance(theory, StopWidthTheory)
    assert theory.tighten is False
    assert theory.proposal.proposed_value == Decimal("1.5") * Decimal("1.1")   # 10pct looser


def test_stop_width_none_without_a_qualifying_correlation():
    # a flat heat series -> no rank variance -> no monotone association -> no theory.
    records = [_HeatRec(mae_pct_reached=1, net_pl=i) for i in range(20)]
    assert plan_stop_width_proposal(records, current_mae_mult=Decimal("1.5")) is None


def test_stop_width_none_without_enough_heat_samples():
    # records carry no mae_pct_reached (heat unset) -> below min_pairs -> no theory (never crashes).
    records = [_Rec({"rsi_14": Decimal(i)}, i) for i in range(20)]
    assert plan_stop_width_proposal(records, current_mae_mult=Decimal("1.5")) is None


def test_stop_width_honours_an_overridden_nudge():
    records = [_HeatRec(mae_pct_reached=i, net_pl=(20 - i)) for i in range(20)]
    theory = plan_stop_width_proposal(
        records, current_mae_mult=Decimal("2.0"), nudge_pct=Decimal("0.25")
    )
    assert theory.proposal.proposed_value == Decimal("2.0") * Decimal("0.75")


# ------------------------------------------------ entry-filter proposal construction (TB00751 c)
def _cand(level_key, rho):
    return IdentifiedCandidate(level_key=level_key, rho=Decimal(str(rho)), n=0, levels=(), outcomes=())


def _filter_records(key, *, loser_levels, winner_levels):
    """Records carrying signal_params[key]: losers (net<0) at loser_levels, winners (net>0) at winner_levels."""
    out = [_Rec({key: Decimal(str(v))}, -1) for v in loser_levels]
    out += [_Rec({key: Decimal(str(v))}, 2) for v in winner_levels]
    return out


def test_entry_filter_volume_raises_the_floor_to_the_loser_median():
    # volume_ratio rho > 0 (low volume loses) -> raise volume_sss_threshold to the loser-median level.
    records = _filter_records("volume_ratio", loser_levels=[1.2, 1.4, 1.6], winner_levels=[2.0, 2.5])
    prop = plan_entry_filter_proposal(records, _cand("volume_ratio", "0.8"))
    assert prop.param_name == "volume_sss_threshold"
    assert prop.proposed_value == Decimal("1.4")          # median of the losers' volume_ratio


def test_entry_filter_volume_defers_a_high_volume_loses_theory():
    # rho < 0 (high volume loses) cannot be expressed by a single floor -> DEFER (None).
    records = _filter_records("volume_ratio", loser_levels=[2.0, 2.4, 2.8], winner_levels=[1.1, 1.2])
    assert plan_entry_filter_proposal(records, _cand("volume_ratio", "-0.8")) is None


def test_entry_filter_rsi_high_rho_raises_the_low_bound():
    # rsi_14 rho > 0 (low rsi loses) -> raise rsi_long_low to the loser-median.
    records = _filter_records("rsi_14", loser_levels=[32, 34, 38], winner_levels=[45, 48])
    prop = plan_entry_filter_proposal(records, _cand("rsi_14", "0.7"))
    assert prop.param_name == "rsi_long_low"
    assert prop.proposed_value == Decimal("34")


def test_entry_filter_rsi_low_rho_lowers_the_high_bound():
    # rsi_14 rho < 0 (high rsi loses) -> lower rsi_long_high to the loser-median.
    records = _filter_records("rsi_14", loser_levels=[44, 46, 48], winner_levels=[33, 35])
    prop = plan_entry_filter_proposal(records, _cand("rsi_14", "-0.7"))
    assert prop.param_name == "rsi_long_high"
    assert prop.proposed_value == Decimal("46")


def test_entry_filter_rsi_short_side_moves_the_mirror_bound():
    # the short module moves the mirror bound: rho > 0 -> rsi_short_high (the short zone low edge).
    records = _filter_records("rsi_14", loser_levels=[55, 58, 60], winner_levels=[65, 68])
    prop = plan_entry_filter_proposal(records, _cand("rsi_14", "0.7"), side="short")
    assert prop.param_name == "rsi_short_high"


def test_entry_filter_defers_ema_period_candidate():
    # ema_9/ema_21 map to PERIODS, not levels -> not re-simulatable -> DEFER (None).
    records = _filter_records("ema_9", loser_levels=[100, 101, 102], winner_levels=[103, 104])
    assert plan_entry_filter_proposal(records, _cand("ema_9", "0.9")) is None


def test_entry_filter_defers_when_too_few_losers():
    records = _filter_records("volume_ratio", loser_levels=[1.5], winner_levels=[2.0, 2.5, 3.0])
    assert plan_entry_filter_proposal(records, _cand("volume_ratio", "0.8")) is None
