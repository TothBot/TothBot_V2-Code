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

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal

from ..config import registry
from .pdca_engine import SACRED_RR_PARAM
from .pool import CIATS_TRADE_FLOOR
from .statistical_engine import spearman_significant

# Kelly is recomputed every 50 closed trades after the 200-trade activation (HR-CI-004 / CI-KE-007).
KELLY_RECOMPUTE_INTERVAL = 50
PER_TRADE_SIZE_PARAM = "per_trade_size_usd"

# The fully-testable stop-loss-width dial (TB00751 (a)): the L2 risk-leg multiplier param and the
# per-trade heat-taken field (contract:TRADE_CLOSE field 11) the theory correlates against outcome.
MAE_MULT_PARAM = "mae_mult"
MAE_HEAT_FIELD = "mae_pct_reached"
# The conservative RELATIVE step the stop-width theory nudges mae_mult by (the genuine new seed,
# value home TB00000 sec 8 / 0500000 provenance). Direction is data-derived; only this magnitude is
# a seed - see the registry note. NEVER applied to the sacred R:R.
MAE_MULT_NUDGE_PCT = "mae_mult_nudge_pct"

# The CONTINUOUS per-trade signal_params LEVEL keys the Spearman PLAN-candidate gate ranks against the
# realized outcome (the SSS indicator levels the entry was taken under, contract:TRADE_CLOSE field 19).
# sss_pass (bool) + side (categorical) are NOT continuous levels and are excluded from the rank gate.
SPEARMAN_CANDIDATE_LEVELS: tuple[str, ...] = ("rsi_14", "ema_9", "ema_21", "volume_ratio")

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
class IdentifiedCandidate:
    """evt:CIATS_PLAN_CANDIDATE [HIGH] - the PLAN candidate the diagram's Spearman gate IDENTIFIES over
    the Stream-2 corpus (0500000 sec 6/7 + lines 4111/4187): the per-trade signal_params LEVEL key whose
    level series has the STRONGEST qualifying monotone association (|rho| > 0.3 AND p < 0.05) with the
    realized outcome - the single parameter the diagram advances (one candidate per cycle, strongest
    |rho|). level_key is the signal_params field; rho the rank correlation; n the paired-sample count;
    (levels, outcomes) the qualifying series (the open_pdca CHECK Spearman-corroboration input).

    This NAMES the candidate the data supports - the diagram-specified PLAN identification. It does NOT
    carry an owned-parameter name or a proposed value: mapping a signal_params indicator level to the
    owned threshold it informs (e.g. rsi_14 -> which rsi bound) + deriving the proposed value (magnitude
    + direction) is a downstream construction the diagram does not specify (surfaced for a design ruling,
    never fabricated). Until that mapping is ruled, the candidate is surfaced, not advanced to a write."""

    level_key: str
    rho: Decimal
    n: int
    levels: tuple
    outcomes: tuple
    code: str = field(default="CIATS_PLAN_CANDIDATE", init=False)


def _record_net_pl(record: object) -> Decimal | None:
    """The realized net P/L of a TRADE_CLOSE record (ar:AR-065), or None if the field is absent."""
    value = getattr(record, "net_pl_usd", None)
    return None if value is None else _dec(value)


def identify_spearman_candidate(
    records: Sequence[object],
    *,
    level_keys: Sequence[str] = SPEARMAN_CANDIDATE_LEVELS,
    outcome: Callable[[object], object | None] = _record_net_pl,
    min_pairs: int = 3,
) -> "IdentifiedCandidate | None":
    """The diagram's PLAN-candidate IDENTIFICATION (PURE): over the Stream-2 corpus, for each continuous
    signal_params LEVEL key build the aligned (level, outcome) pairs and run the CIATS Spearman gate
    (|rho| > 0.3 AND p < 0.05); return the qualifying key with the STRONGEST |rho| (one candidate per
    cycle, the diagram's tie-break) or None when none qualifies. A record without signal_params, without
    the key, or without an outcome is skipped; a key with < min_pairs paired samples is skipped (a
    degenerate series). NEVER proposes a value - it identifies the parameter the data supports; the
    owned-threshold mapping + the proposed value are surfaced downstream (not derived here)."""
    best: IdentifiedCandidate | None = None
    for key in level_keys:
        levels: list = []
        outcomes: list = []
        for record in records:
            sp = getattr(record, "signal_params", None)
            if not isinstance(sp, dict):
                continue
            level = sp.get(key)
            if level is None:
                continue
            out = outcome(record)
            if out is None:
                continue
            levels.append(level)
            outcomes.append(out)
        if len(levels) < min_pairs:
            continue
        rho, qualifies = spearman_significant(levels, outcomes)
        if not qualifies:
            continue
        if best is None or abs(rho) > abs(best.rho):
            best = IdentifiedCandidate(
                level_key=key, rho=rho, n=len(levels),
                levels=tuple(levels), outcomes=tuple(outcomes),
            )
    return best


@dataclass(frozen=True)
class StopWidthTheory:
    """The mae_mult stop-loss-width theory FORMED from the corpus (TB00751 (a)) - a fully testable
    dial. proposal is the candidate mae_mult ParameterChangeProposal; (levels, outcomes) is the
    per-trade (heat, net-P/L) series the open_pdca CHECK corroborates (the Spearman gate input); rho
    is the heat-vs-outcome rank correlation; tighten is the data-derived direction (True = more heat
    predicts a worse outcome -> a TIGHTER stop; False = the converse -> a LOOSER stop)."""

    proposal: ParameterChangeProposal
    levels: tuple
    outcomes: tuple
    rho: Decimal
    tighten: bool


def _record_heat(record: object) -> Decimal | None:
    """The per-trade heat-taken of a TRADE_CLOSE record (field 11 mae_pct_reached), or None if the
    field is absent / unset (the at-exit MAE until the max-over-life MTM tracker lands)."""
    value = getattr(record, MAE_HEAT_FIELD, None)
    return None if value is None else _dec(value)


def plan_stop_width_proposal(
    records: Sequence[object],
    *,
    current_mae_mult: object,
    nudge_pct: object | None = None,
    min_pairs: int = 3,
) -> "StopWidthTheory | None":
    """FORM the stop-loss-width theory from the Stream-2 corpus (PURE; the fully-testable dial). For
    every record carrying a heat-taken level (mae_pct_reached, field 11) build the aligned (heat,
    net-P/L) pairs and run the CIATS Spearman gate (|rho| > 0.3 AND p < 0.05). On a qualifying
    correlation propose a mae_mult change: rho < 0 (MORE heat predicts a WORSE outcome) TIGHTENS the
    stop (mae_mult * (1 - nudge)); rho > 0 (heat then recovers to a better outcome) LOOSENS it
    (mae_mult * (1 + nudge)). Returns a StopWidthTheory (proposal + the Spearman series + direction)
    or None (no qualifying correlation / < min_pairs paired heat samples).

    The DIRECTION is fully data-derived (the Spearman sign); only the MAGNITUDE is the bounded seed
    nudge_pct (registry mae_mult_nudge_pct - mae_mult is an ATR multiple while heat is a price
    fraction, so an exact data value is not derivable; the PDCA CHECK + Bill approval gate every
    application). NEVER the sacred R:R (mae_mult is a CIATS-owned stop leg, never the 1:1.5 floor)."""
    step = _dec(registry.value(MAE_MULT_NUDGE_PCT) if nudge_pct is None else nudge_pct)
    levels: list = []
    outcomes: list = []
    for record in records:
        heat = _record_heat(record)
        if heat is None:
            continue
        out = _record_net_pl(record)
        if out is None:
            continue
        levels.append(heat)
        outcomes.append(out)
    if len(levels) < min_pairs:
        return None
    rho, qualifies = spearman_significant(levels, outcomes)
    if not qualifies:
        return None
    cur = _dec(current_mae_mult)
    tighten = rho < _ZERO
    factor = (_ONE - step) if tighten else (_ONE + step)
    direction = "tighten" if tighten else "loosen"
    proposal = ParameterChangeProposal(
        param_name=MAE_MULT_PARAM,
        current_value=cur,
        proposed_value=cur * factor,
        rationale=(
            f"stop-width drift: heat~outcome Spearman rho={rho} -> {direction} mae_mult by "
            f"{step} (data-derived direction, bounded seed nudge)"
        ),
    )
    return StopWidthTheory(
        proposal=proposal, levels=tuple(levels), outcomes=tuple(outcomes), rho=rho, tighten=tighten
    )


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
