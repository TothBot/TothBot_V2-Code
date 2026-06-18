"""TothBot V2 parameter registry - CIATS-owned SEED values.

Source of truth: the 0500000 dv1_240 design diagrams (section 9 Complete
CIATS Parameter Registry); mirrored in TB00000 v2_97 section 8. DIAGRAMS
GOVERN - if these ever disagree with the live 0500000 figures, the
figures win and this file is corrected.

These are STARTING values only. CIATS owns every operating parameter and
replaces each seed with data over paper trading (the 200-trade hard floor
gates live-data tuning; see tothbot... statistical thresholds). The ONLY
hardcoded, non-CIATS-tunable value is the SACRED net 1:1.5 R:R minimum.

Parameter classes (section 8):
  SACRED   - hardcoded; not CIATS-tunable; Bill-revisable UPWARD only.
  OPERATOR - Bill controls.
  CIATS    - starting value, CIATS-owned, per-module unless noted universal.

Recipe seeds: some seeds are not scalars but per-pair/per-regime recipes
(per_trade_size_usd, expected_reward_estimator_seed, mpp_abs_cap_pct).
Those carry value=None here; the computation is canonical in the diagram
element named in the note, and is implemented in its build session.

Retired seeds (NOT active, recorded for traceability): entry_timeout_sec
(RETIRED DEC-122 CR-17 - no GTD window under the marketable-IOC entry;
the MPP slippage cap is the CIATS-owned bound). min_order_size_usd
(RETIRED - superseded by the per_trade_size_usd $50 floor formula).
max_hold_candles (RETIRED DEC-124 - run-to-reversal exit, no time cap).
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ParamClass(Enum):
    SACRED = "sacred"
    OPERATOR = "operator"
    CIATS = "ciats"


class Scope(Enum):
    UNIVERSAL = "universal"          # one value across the whole system
    PER_MODULE = "per_module"        # instantiated per trading module
    PER_PAIR_SIDE = "per_pair_side"  # instantiated per pair and side


@dataclass(frozen=True)
class Param:
    """One registry entry: a named seed with its governance metadata."""

    name: str
    value: object  # scalar seed, or None when the seed is a recipe (see note)
    cls: ParamClass
    scope: Scope
    unit: str = ""
    note: str = ""


# Convenience aliases for terse table construction below.
_S, _O, _C = ParamClass.SACRED, ParamClass.OPERATOR, ParamClass.CIATS
_U, _M, _PS = Scope.UNIVERSAL, Scope.PER_MODULE, Scope.PER_PAIR_SIDE

REGISTRY: tuple[Param, ...] = (
    # -- SACRED --------------------------------------------------------
    Param(
        "rr_minimum_net", 1.5, _S, _U, "reward:risk",
        "Net (after-fee) floor on expected R:R, all modules. The only "
        "hardcoded value. Entry-acceptance FLOOR only - never a cap, never "
        "an exit price (DEC-124). Operator-controlled MINIMUM: NEVER lowered "
        "below 1.5; RAISABLE by Bill (e.g. 2.0) on CIATS evidence a higher "
        "floor preserves trade flow + improves net edge. Not CIATS-tunable; "
        "CIATS analyzes floor/trade-count/net-edge and recommends, Bill "
        "ratifies (hardcoded-constant change). admit iff "
        "expected_reward / net_loss >= 1.5; acceptance rule A1.",
    ),

    # -- OPERATOR ------------------------------------------------------
    Param("paper_starting_balance_long_usd", 5000.0, _O, _M, "USD", "Paper only."),
    Param("paper_starting_balance_short_usd", 5000.0, _O, _M, "USD", "Paper only."),

    # -- CIATS: sizing -------------------------------------------------
    Param(
        "per_trade_size_usd", None, _C, _PS, "USD",
        "Recipe (DEC-122 CR-06): max(per_trade_size_floor_usd, "
        "per_trade_size_margin_mult * max(costmin, ordermin * "
        "entry_limit_price)); live-read per pair each candle; becomes a "
        "function of expected R:R from 200 trades on. Canonical at D2 Gate-8.",
    ),
    Param("per_trade_size_floor_usd", 50.0, _C, _PS, "USD", "$50 floor in the per_trade_size_usd recipe."),
    Param("per_trade_size_margin_mult", 5.0, _C, _PS, "x", "5x margin above each pair's real minimum."),

    # -- CIATS: drawdown halts (per-wallet) ----------------------------
    Param("session_pause_drawdown_pct", 0.05, _C, _M, "fraction", "5% per-wallet session pause."),
    Param("full_halt_drawdown_pct", 0.10, _C, _M, "fraction", "10% per-wallet full halt."),

    # -- CIATS: Gate-7 risk limits (per-module) ------------------------
    Param(
        "concentration_limit_per_module", 1.0, _C, _M, "fraction",
        "100% of wallet. SEED, NON-BINDING; the module wallet balance is the "
        "sole sizing boundary; CIATS may set up to 100% of wallet in one "
        "position. CIATS-owned from the 200-trade floor. DEC-115.",
    ),
    Param(
        "exposure_limit_pct", 1.0, _C, _M, "fraction",
        "100% of wallet. SEED, NON-BINDING; CIATS may use up to 100% of "
        "wallet across all open positions. CIATS-owned from 200-trade. DEC-115.",
    ),

    # -- CIATS: exit controller ----------------------------------------
    Param("mae_mult", 1.5, _C, _M, "x ATR(14)", "Layer 2 MAE threshold breach multiplier (legacy 5m path + unit tests; the live 24h-decision long-only path uses decision_atr_stop_mult)."),
    Param("emergency_sl_mult", 3.0, _C, _M, "x ATR(14)", "Layer 3 Kraken resting emergency stop (off-book, failure only)."),

    # -- CIATS: 24h DECISION (the validated long-only strategy, TB00786/787/788/790) --
    # The derive seeds (TB00787/788, diagram-sited ar:AR-045): mod:OhlcAggregator folds 24
    # contiguous 1H closes into one 24h DECISION bar; the per-pair DailyDecisionCache holds
    # EMA(fast)/EMA(slow)/ATR on that daily series. The long-only ENTRY = EMA(fast) bullish
    # cross above EMA(slow) on a Closed24H; the EXIT = the bearish-cross reversal (layer:L1a)
    # OR the wide decision_atr_stop_mult x daily-ATR stop (layer:L2). Only the net 1:1.5 R:R
    # floor stays hardcoded (rule:Sacred_R_R_1_to_1_5).
    Param("decision_bar_interval_min", 1440, _C, _U, "min", "The 24h DECISION-bar cadence (the validated long-only strategy decides on the daily candle)."),
    Param("decision_ema_fast", 12, _C, _U, "bars", "Fast EMA on the 24h decision closes; the bullish cross above decision_ema_slow is the long-only entry trigger, the bearish cross the layer:L1a reversal exit."),
    Param("decision_ema_slow", 26, _C, _U, "bars", "Slow EMA on the 24h decision closes (the entry/exit cross partner of decision_ema_fast)."),
    Param("decision_atr_period", 14, _C, _U, "bars", "ATR period on the 24h decision bars - the wide-stop volatility basis."),
    Param("decision_atr_stop_mult", 2.5, _C, _M, "x ATR(14)-daily", "The WIDE Layer-2 volatility stop multiple on the DAILY decision-bar ATR(14), TB00790 - REPLACES the tight 1.5x mae_mult for the 24h-decision long-only path (stop-width sweet spot ~2-2.5x ATR, TB00784/786). The ONE shared 1R basis: gate:G8 net_loss + layer:L2 stop + the close actual_rr; L3 emergSL at emergency_sl_mult x the same daily ATR stays outermost. NEVER the sacred R:R floor."),
    Param(
        "cancel_timeout_window", 5.0, _C, _M, "s",
        "I-6 cancel-timeout fallback: how long mod:Exit_Controller waits for a "
        "cancel_order ACK on the resting emergSL before the executions-channel "
        "state check (confirmed -> proceed; unknown -> retry once; 2nd timeout "
        "-> HOLD + alert, NEVER market sell with ambiguous order state). "
        "Canonical at mod:Exit_Controller D3; value home here.",
    ),
    Param(
        "mpp_retry_count", 3, _C, _M, "count",
        "C-1 MPP-rejection retry count: the number of marketable IOC-limit retries "
        "after a market close rejects on a wide spread (Kraken Max-Price-Protection), "
        "each walked best_bid -/+ 0.2% out per attempt. Canonical at "
        "mod:Exit_Controller D3; value home here.",
    ),

    # -- CIATS: self-tuning step (the FORM->TEST->ROUTE loop's bounded magnitude) --
    Param(
        "mae_mult_nudge_pct", 0.10, _C, _M, "fraction",
        "The conservative RELATIVE step CIATS nudges param:mae_mult by per "
        "qualifying stop-width drift signal (TB00751 FORM->TEST->ROUTE loop). The "
        "DIRECTION is data-derived - the sign of the Spearman heat-vs-outcome "
        "correlation - tighten mae_mult when more heat predicts a worse outcome, "
        "loosen on the converse. Only this MAGNITUDE is a seed: mae_mult is an "
        "ATR(14) multiple while mae_pct_reached is a price fraction, and the "
        "per-trade ATR is NOT stored on the contract:TRADE_CLOSE record, so an "
        "exact data-derived mae_mult value cannot be computed - a bounded 10pct "
        "nudge stands in, and every application is gated by the PDCA CHECK + Bill "
        "approval HR-CI-011. NEVER the sacred R:R. CIATS-owned, refined from paper.",
    ),

    # -- CIATS: run-to-reversal expected-reward estimator (DEC-124) ----
    Param(
        "expected_reward_estimator_seed", None, _C, _M, "fraction of entry_fill_price",
        "Recipe (Bill ruling TB00654): replay the EC-L1A-001/EC-L1A-002 "
        "regime-reversal exit over historical OHLC per pair/regime; seed = "
        "central-tendency realized favorable excursion. Feeds the A1 "
        "acceptance floor ONLY; never an exit price. Per-module; tuned from "
        "paper data from 200 trades. Canonical at D9 compute + D3.",
    ),

    # -- CIATS: entry slippage cap (DEC-128) ---------------------------
    Param(
        "mpp_abs_cap_pct", None, _C, _PS, "fraction",
        "Recipe: per-pair/side empirical Q95 of the adverse close-to-next-open "
        "displacement on the 5m OHLC series, over the historical-OHLC backtest "
        "dataset (nonparametric quantile; heavy-tailed gaps). Combined at "
        "AR-069 as mpp_cap_pct = min(mpp_abs_cap_pct, rr_headroom) where "
        "rr_headroom = expected_reward - 1.5*net_loss (the sacred floor at the "
        "execution boundary; NOT tunable). CIATS-owned from the start; 200-trade "
        "floor gates tuning only. Canonical at D1 WS-ADD-002 / D2 / AR-069.",
    ),

    # -- CIATS: regime + signal model ----------------------------------
    Param("adx_threshold", 25, _C, _M, "ADX", ""),
    Param("atr_percentile_thresh", 67, _C, _M, "percentile", ""),
    Param("atr_rolling_window", 50, _C, _M, "days", ""),
    Param("htf_ema_periods", (20, 50), _C, _M, "daily periods", "20/50 daily HTF EMAs."),
    Param("min_volume_usd_daily", 500_000.0, _C, _M, "USD/day", ""),

    # -- CIATS: SSS signal model ---------------------------------------
    Param("rsi_long_low", 30, _C, _M, "RSI", "Long only."),
    Param("rsi_long_high", 50, _C, _M, "RSI", "Long only."),
    Param("rsi_short_low", 70, _C, _M, "RSI", "Short only; mirror of rsi_long_low."),
    Param("rsi_short_high", 50, _C, _M, "RSI", "Short only; mirror of rsi_long_high."),
    Param("sss_ema_short", 9, _C, _M, "periods", "Both modules."),
    Param("sss_ema_long", 21, _C, _M, "periods", "Both modules."),
    Param("volume_sss_threshold", 1.0, _C, _M, "x MA(20)", "Both modules."),

    # -- CIATS: selection controller (per-module) ----------------------
    Param("sc_body_threshold", 0.3, _C, _M, "body vs ATR", ""),
    Param("sc_cooldown_seconds", 300, _C, _M, "s", "1 candle."),
    Param("sc_consecutive_limit", 3, _C, _M, "count", "Consecutive-loss limit."),

    # -- CIATS: short-side specific (Short only) -----------------------
    Param("leverage_cap_short", 3, _C, _M, "x", "Kraken max 10x; held low for loss-min. Short only."),

    # -- CIATS: short-side margin fees (Short only; Kraken spot-margin, ar:AR-009) --
    Param(
        "margin_open_fee_pct", 0.0002, _C, _PS, "fraction",
        "0.02% Kraken spot-margin OPENING fee on a short sell-to-open (Bill ruling "
        "TB00728 DEC-A; TB00000 v2_100 sec 8). Short only - a spot long pays neither. "
        "Kraken's published spot-margin rate for major pairs (0.01-0.05% per pair); "
        "CIATS-owned seed, per-pair/side, refined from paper from the 200-trade floor. "
        "Canonical at 0500000 D1 FEE block.",
    ),
    Param(
        "margin_rollover_fee_pct", 0.0002, _C, _PS, "fraction per 4h",
        "0.02% per 4h Kraken spot-margin ROLLOVER fee charged every 4 hours a short "
        "position stays open (Bill ruling TB00728 DEC-A; TB00000 v2_100 sec 8). Short "
        "only. The 0500000 token param:margin_borrow_fee = open + rollover x held-4h-"
        "blocks, added to short net_loss / net P&L and the sacred 1:1.5 R:R floor "
        "(D9 net_loss / D3 R:R). CIATS-owned seed, refined from paper.",
    ),

    # -- CIATS: VPS deployment (single process, universal) -------------
    Param("StartLimitBurst", 3, _C, _U, "count", "systemd restart cap."),
    Param("StartLimitIntervalSec", 600, _C, _U, "s", "systemd restart window."),

    # -- CIATS: liquidity cache (universal) ----------------------------
    Param("liquidity_refresh_hours", 4, _C, _U, "hours", ""),

    # -- CIATS: threshold monitoring (universal) -----------------------
    Param("rl_warning_threshold_pct", 0.80, _C, _U, "fraction", ""),
    Param("rl_critical_threshold_pct", 0.95, _C, _U, "fraction", "Suppresses entry orders to preserve exit budget."),
    Param("fee_tier_divergence_threshold", 0.0002, _C, _U, "fraction", "2 bps."),
    Param("fee_tier_divergence_sustained_trades", 50, _C, _U, "count", ""),
    Param("rejection_rate_regime_threshold_pct", 0.30, _C, _U, "fraction", ""),
    Param("rejection_rate_eval_window_count", 50, _C, _U, "count", ""),

    # -- CIATS: acceptance ---------------------------------------------
    Param("expected_rr_acceptance_rule", "A1", _C, _U, "rule", "Reject below 1:1.5 at point estimate."),
)


# Fast lookup by name. Names are unique within REGISTRY.
_BY_NAME: dict[str, Param] = {p.name: p for p in REGISTRY}


def get(name: str) -> Param:
    """Return the Param named `name`, or raise KeyError."""
    return _BY_NAME[name]


def value(name: str) -> object:
    """Return the seed value of the Param named `name`."""
    return _BY_NAME[name].value


def by_class(cls: ParamClass) -> tuple[Param, ...]:
    """All registry entries of a given class, in registry order."""
    return tuple(p for p in REGISTRY if p.cls is cls)


def scalar_seeds() -> dict[str, object]:
    """Name -> value for every seed that carries a scalar starting value.

    Excludes recipe seeds (value is None); those are computed in their own
    build sessions from the diagram element named in their note.
    """
    return {p.name: p.value for p in REGISTRY if p.value is not None}
