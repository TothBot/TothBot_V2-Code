"""mod:CIATS_Proposal_Engine - generate CIATS-owned parameter-change proposals (PROPOSAL ONLY).

Source: 0500000 dv1_250 sec 6/7 mod:CIATS_Proposal_Engine: "parameter-change proposals from the
Stream 2 trade corpus (>= 200 closed trades HARD FLOOR per rule:HR-CI-004) ... owned params to the
Parameter Store at inter-trade boundaries ONLY per ar:AR-072 + rule:HR-CI-003 ... queued for Bill
approval via the PDCA Engine (HR-CI-011)". Two proposal generators:

  per_trade_size_usd (Half-Kelly): K_full = W - (1-W)/R (NET P/L, ar:AR-065), K_half = K_full * 0.5
    (the conservative half), per_trade_size_usd = K_half * module_wallet_balance. Recomputed every 50
    closed trades after the 200-trade activation (evt:KELLY_UPDATE). K_full <= 0 -> evt:KELLY_NEGATIVE
    [CRITICAL] (rule:HR-CI-008) and NO positive sizing proposal (the seed sizing stands).
  candidate parameters (mae_mult / emergency_sl_mult / adx_threshold / atr_percentile_thresh /
    disallowed_regimes / ...): a parameter QUALIFIES as a PLAN candidate IFF the Spearman gate passes
    (|rho| > 0.3 AND p < 0.05, both gates, statistical_engine.spearman_significant) between the
    parameter level and the realized outcome - else no proposal (the diagram's PLAN-candidate rule).

The sacred 1:1.5 R:R is NEVER a proposal target (rule:Sacred_R_R_1_to_1_5). Every proposal is a
CANDIDATE - the mod:CIATS_PDCA_Engine shadow-evaluates + CHECK-gates it and the ACT writes the
mod:CIATS_Parameter_Store ONLY on Bill approval at an inter-trade boundary. This engine NEVER writes
a parameter. PER-MODULE (one per wallet, like the pool). PURE, Decimal-only (ar:AR-047).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from decimal import Decimal

from .pdca_engine import SACRED_RR_PARAM
from .pool import CIATS_TRADE_FLOOR
from .statistical_engine import spearman_significant

# Kelly is recomputed every 50 closed trades after the 200-trade activation (HR-CI-004 / CI-KE-007).
KELLY_RECOMPUTE_INTERVAL = 50
PER_TRADE_SIZE_PARAM = "per_trade_size_usd"

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HALF = Decimal("0.5")


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


@dataclass(frozen=True)
class ParameterChangeProposal:
    """A CIATS-owned parameter-change CANDIDATE queued for the PDCA cycle (PLAN). param_name is the
    owned parameter; proposed_value the candidate; rationale the evidence; regime tags a regime-
    segmented proposal (mod:CIATS_Regime_Library). Never the sacred R:R."""

    param_name: str
    current_value: object
    proposed_value: object
    rationale: str
    regime: str | None = None
    code: str = field(default="PARAMETER_CHANGE_PROPOSAL", init=False)


@dataclass(frozen=True)
class KellyUpdate:
    """evt:KELLY_UPDATE [INFO] - a Half-Kelly recompute at a 50-trade boundary: K_full, K_half, and
    the resulting per_trade_size_usd = K_half * wallet (the proposed value)."""

    k_full: Decimal
    k_half: Decimal
    per_trade_size_usd: Decimal
    code: str = field(default="KELLY_UPDATE", init=False)


@dataclass(frozen=True)
class KellyNegative:
    """evt:KELLY_NEGATIVE [CRITICAL] (rule:HR-CI-008) - K_full <= 0 (the realized edge is negative):
    NO positive sizing proposal is made (the seed sizing stands) and the operator is alerted."""

    k_full: Decimal
    code: str = field(default="KELLY_NEGATIVE", init=False)


class ProposalEngine:
    """One module's CIATS proposal generator (per-module, like the pool). Produces the Half-Kelly
    per_trade_size_usd proposal + the Spearman-qualified candidate proposals; NEVER writes a
    parameter (the PDCA ACT + Parameter Store do, on Bill approval)."""

    def __init__(
        self,
        *,
        trade_floor: int = CIATS_TRADE_FLOOR,
        recompute_interval: int = KELLY_RECOMPUTE_INTERVAL,
    ) -> None:
        self._trade_floor = trade_floor
        self._recompute_interval = recompute_interval

    def kelly_due(self, trade_count: int) -> bool:
        """True at the Half-Kelly recompute boundaries: at the 200-trade activation and every 50
        closed trades thereafter (HR-CI-004 floor + the CI-KE-007 interval)."""
        if trade_count < self._trade_floor:
            return False
        return (trade_count - self._trade_floor) % self._recompute_interval == 0

    def kelly_sizing(self, pool, wallet_balance: object) -> "KellyUpdate | KellyNegative | None":
        """The Half-Kelly per-trade size from the module pool: K_full = W - (1-W)/R, K_half =
        K_full/2 clamped [0,1], per_trade_size_usd = K_half * wallet_balance. Returns None below the
        floor or before W/R are both defined (the seed sizing stands); KellyNegative (HR-CI-008) when
        K_full <= 0 (no positive sizing)."""
        if not pool.ready:
            return None
        w = pool.win_rate
        r = pool.net_reward_risk
        if w is None or r is None:
            return None
        k_full = w - (_ONE - w) / r
        if k_full <= _ZERO:
            return KellyNegative(k_full=k_full)
        k_half = k_full * _HALF
        if k_half > _ONE:
            k_half = _ONE
        per_trade = k_half * _dec(wallet_balance)
        return KellyUpdate(k_full=k_full, k_half=k_half, per_trade_size_usd=per_trade)

    def per_trade_size_proposal(
        self, pool, wallet_balance: object, *, current_size: object = None
    ) -> "ParameterChangeProposal | KellyNegative | None":
        """Wrap the Half-Kelly sizing into a per_trade_size_usd ParameterChangeProposal (the PDCA
        PLAN candidate). Returns KellyNegative (no positive sizing) or None (below floor / W,R not
        ready) unchanged so the caller can route the CRITICAL alert / fall back to the seed."""
        sizing = self.kelly_sizing(pool, wallet_balance)
        if not isinstance(sizing, KellyUpdate):
            return sizing
        return ParameterChangeProposal(
            param_name=PER_TRADE_SIZE_PARAM,
            current_value=current_size,
            proposed_value=sizing.per_trade_size_usd,
            rationale=f"Half-Kelly K_half={sizing.k_half} * wallet (K_full={sizing.k_full})",
        )

    def candidate_proposal(
        self,
        param_name: str,
        current_value: object,
        proposed_value: object,
        *,
        param_levels: Sequence[object],
        outcomes: Sequence[object],
        rationale: str = "",
        regime: str | None = None,
    ) -> ParameterChangeProposal | None:
        """A non-sizing parameter proposal: qualifies as a PLAN candidate ONLY IF the Spearman gate
        passes (|rho| > 0.3 AND p < 0.05) between param_levels and outcomes - the diagram's PLAN-
        candidate rule. The sacred 1:1.5 R:R is NEVER proposed (returns None). Returns the proposal,
        or None if it does not qualify."""
        if param_name == SACRED_RR_PARAM:
            return None
        _rho, qualifies = spearman_significant(param_levels, outcomes)
        if not qualifies:
            return None
        note = rationale or f"Spearman-qualified (rho={_rho})"
        return ParameterChangeProposal(
            param_name=param_name,
            current_value=current_value,
            proposed_value=proposed_value,
            rationale=note,
            regime=regime,
        )
