"""mod:CIATS_Proposal_Engine tests (ciats/proposal_engine.py).

Covers the Half-Kelly per_trade_size_usd proposal (K_full = W-(1-W)/R, K_half, K_half*wallet; the
50-trade recompute cadence; KellyNegative when K_full <= 0 per HR-CI-008; None below the 200-trade
floor) and the Spearman-qualified candidate-parameter proposal (|rho|>0.3 AND p<0.05 qualifies; a
constant/no-association series or the sacred R:R never qualifies).
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.ciats.pool import CiatsPool
from tothbot.ciats.proposal_engine import (
    KELLY_RECOMPUTE_INTERVAL,
    IdentifiedCandidate,
    KellyNegative,
    KellyUpdate,
    ParameterChangeProposal,
    ProposalEngine,
    identify_spearman_candidate,
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
