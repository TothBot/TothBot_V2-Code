"""gate:G7_Risk_Guard - the per-module risk gate (Diagram 8 of 10).

Source: 0500000 dv1_250 sec 8 Image8 (0500000_Image8_T3_R7_Gate_7_Risk_Guard_Detail)
+ rule:HR-WM-024 + the per-wallet drawdown halts (ar:AR-052) + ar:AR-009 (short = Kraken
margin) + ar:AR-043 (per-module BoundedSemaphore dispatch serialization).

Gate 7 runs FOUR checks in STRICT order; the FIRST failure short-circuits the rest
(rule:HR-WM-024). All evaluation is PER-MODULE / PER-WALLET - the Long module evaluates
against its spot wallet + own positions + own semaphore, the Short module against its
Kraken-margin-equity wallet + own positions + own semaphore; one side never blocks the
other (TB00000 sec 7).

  CHECK 1  Drawdown   drawdown_pct = (portfolio_baseline - wallet_balance) / portfolio_baseline
                      PASS iff drawdown_pct < full_halt AND < session_pause. A two-threshold
                      cascade: >= full_halt_drawdown_pct (10% seed) -> HALT disposition
                      (module-wide stop until Bill ratifies); >= session_pause_drawdown_pct
                      (5% seed) -> PAUSE disposition (this session stops, resumes next).
                      FAIL emits evt:G7_DRAWDOWN_HALT and short-circuits CHECK 2-4.
  CHECK 2  Concentration  concentration_ratio = candidate_committed_usd / wallet_balance
                      PASS iff <= concentration_limit_per_module. A CAPITAL fraction of
                      wallet (NOT a position count - the count form is obsolete legacy).
                      The 100% seed is NON-BINDING; 100% CIATS-owned from the 200-trade
                      floor with no cap below 100% of wallet, identical Long/Short. FAIL
                      emits evt:G7_CONCENTRATION_BREACH (BLOCK this candidate).
  CHECK 3  Exposure   exposure_ratio = total_committed_usd / wallet_balance
                      PASS iff <= exposure_limit_pct (inclusive - CIATS may commit up to
                      100% of wallet, no cap). For a SHORT the committed capital is the
                      Kraken margin/collateral requirement bounded by param:leverage_cap_short
                      (ar:AR-053), NOT full spot notional - that bounding is applied UPSTREAM
                      when committed_usd is computed; this gate compares the ratio. FAIL emits
                      evt:G7_EXPOSURE_BREACH (BLOCK this candidate).
  CHECK 4  Semaphore  Non-blocking probe of the per-module BoundedSemaphore (locked state
                      only; does NOT acquire). PASS iff FREE (not locked). LOCKED (an
                      in-flight dispatch holds it) emits evt:G7_SEMAPHORE_BUSY -> SKIP cycle
                      (retry next candle; transient, NOT a policy reject).

ALL 4 PASS -> proceed to gate:G8_Position_Sizer.

PURE compute (mirrors taxonomy.py / position_sizer.py - no socket, no asyncio; Decimal-only
per ar:AR-047). The semaphore is passed in as its probed lock state (a bool) so the gate
stays pure; the BoundedSemaphore itself lives with the module's dispatch owner. The four
CIATS-owned limits default to their registry seeds and may be overridden per call (each
module instantiates its own values). Side only selects which wallet/semaphore the caller
supplies and labels the events; the check arithmetic is identical both sides.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

from ..config import registry
from ..exchange.position_mirror import PositionSide

# CIATS-owned per-module seeds, taken as Decimal exactly once (AR-047). Per-module
# instantiated: each module may carry its own value (override at call time).
_FULL_HALT = Decimal(str(registry.value("full_halt_drawdown_pct")))        # 10% seed
_SESSION_PAUSE = Decimal(str(registry.value("session_pause_drawdown_pct")))  # 5% seed
_CONCENTRATION_LIMIT = Decimal(str(registry.value("concentration_limit_per_module")))  # 100% seed
_EXPOSURE_LIMIT = Decimal(str(registry.value("exposure_limit_pct")))        # 100% seed


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the gate (AR-047)."""
    return Decimal(str(value))


class RiskDisposition(Enum):
    """The Gate-7 outcome dispositions (Image8). PASS proceeds to Gate 8; the four
    fail dispositions differ in severity + recovery."""

    PASS = "PASS"      # all 4 checks pass -> Gate 8
    HALT = "HALT"      # CHECK 1 full-halt drawdown: module-wide stop until Bill ratifies
    PAUSE = "PAUSE"    # CHECK 1 session-pause drawdown: this session stops, resumes next
    BLOCK = "BLOCK"    # CHECK 2/3 concentration or exposure: drop this candidate
    SKIP = "SKIP"      # CHECK 4 semaphore busy: transient, retry next candle


@dataclass(frozen=True)
class G7DrawdownHalt:
    """evt:G7_DRAWDOWN_HALT [HIGH] (Image8 CHECK 1) - the per-wallet drawdown breached a
    threshold. threshold_crossed names which (full_halt -> HALT, session_pause -> PAUSE);
    disposition carries the resulting stop. Per-wallet: the Long wallet is spot USD, the
    Short wallet is Kraken margin equity (ar:AR-052)."""

    side: PositionSide
    wallet_balance: Decimal
    portfolio_baseline: Decimal
    drawdown_pct: Decimal
    threshold_crossed: str   # "full_halt" | "session_pause"
    disposition: str         # "HALT" | "PAUSE"
    code: str = field(default="G7_DRAWDOWN_HALT", init=False)


@dataclass(frozen=True)
class G7ConcentrationBreach:
    """evt:G7_CONCENTRATION_BREACH [INFO] (Image8 CHECK 2) - the candidate's committed
    capital as a fraction of wallet exceeded the CIATS concentration limit. BLOCK (normal
    flow control, no alert)."""

    side: PositionSide
    concentration_ratio: Decimal
    concentration_limit: Decimal
    code: str = field(default="G7_CONCENTRATION_BREACH", init=False)


@dataclass(frozen=True)
class G7ExposureBreach:
    """evt:G7_EXPOSURE_BREACH [INFO] (Image8 CHECK 3) - total committed capital across the
    module's open positions exceeded the CIATS exposure limit (fraction of wallet). BLOCK.
    For a SHORT the committed capital is the leverage-bounded margin requirement."""

    side: PositionSide
    total_committed_usd: Decimal
    wallet_balance: Decimal
    exposure_ratio: Decimal
    exposure_limit_pct: Decimal
    code: str = field(default="G7_EXPOSURE_BREACH", init=False)


@dataclass(frozen=True)
class G7SemaphoreBusy:
    """evt:G7_SEMAPHORE_BUSY [INFO] (Image8 CHECK 4) - the per-module dispatch semaphore is
    LOCKED (an in-flight dispatch holds it). SKIP this cycle and retry next candle (transient,
    not a policy reject; no alert)."""

    side: PositionSide
    code: str = field(default="G7_SEMAPHORE_BUSY", init=False)


@dataclass(frozen=True)
class RiskGuardOutcome:
    """The result of one Gate-7 evaluation. passed=True (disposition PASS, event None) means
    proceed to Gate 8; otherwise disposition names the stop and event carries the labeled
    failure (the first failing check - strict short-circuit order)."""

    passed: bool
    disposition: RiskDisposition
    event: object | None  # None on PASS; the failing check's event otherwise


def evaluate_risk_guard(
    side: PositionSide,
    *,
    wallet_balance: object,
    portfolio_baseline: object,
    candidate_committed_usd: object,
    total_committed_usd: object,
    semaphore_locked: bool,
    full_halt_drawdown_pct: object | None = None,
    session_pause_drawdown_pct: object | None = None,
    concentration_limit: object | None = None,
    exposure_limit_pct: object | None = None,
) -> RiskGuardOutcome:
    """Run the four Gate-7 checks in strict order; the first failure short-circuits (Image8).

    wallet_balance / portfolio_baseline / committed amounts are the MODULE's own (Long spot
    wallet / Short Kraken-margin-equity wallet); semaphore_locked is the non-blocking probe of
    the module's BoundedSemaphore. The four CIATS-owned limits default to their registry seeds
    and may be overridden per module. PURE - emits nothing; the caller logs the returned event."""
    wallet = _dec(wallet_balance)
    baseline = _dec(portfolio_baseline)
    if baseline <= 0:
        # portfolio_baseline is captured once at module creation from the starting wallet,
        # always > 0; a non-positive baseline is a malformed state (defect), never normal.
        raise ValueError(f"portfolio_baseline must be > 0 (HR-WM-011); got {baseline}")

    full_halt = _FULL_HALT if full_halt_drawdown_pct is None else _dec(full_halt_drawdown_pct)
    session_pause = (
        _SESSION_PAUSE if session_pause_drawdown_pct is None else _dec(session_pause_drawdown_pct)
    )
    conc_limit = _CONCENTRATION_LIMIT if concentration_limit is None else _dec(concentration_limit)
    exp_limit = _EXPOSURE_LIMIT if exposure_limit_pct is None else _dec(exposure_limit_pct)

    # --- CHECK 1: per-wallet drawdown (two-threshold cascade) ------------------------------
    drawdown_pct = (baseline - wallet) / baseline
    if drawdown_pct >= full_halt:
        return RiskGuardOutcome(
            passed=False,
            disposition=RiskDisposition.HALT,
            event=G7DrawdownHalt(side, wallet, baseline, drawdown_pct, "full_halt", "HALT"),
        )
    if drawdown_pct >= session_pause:
        return RiskGuardOutcome(
            passed=False,
            disposition=RiskDisposition.PAUSE,
            event=G7DrawdownHalt(side, wallet, baseline, drawdown_pct, "session_pause", "PAUSE"),
        )

    # --- CHECK 2: concentration (candidate committed / wallet, a CAPITAL fraction) ---------
    concentration_ratio = _dec(candidate_committed_usd) / wallet
    if concentration_ratio > conc_limit:
        return RiskGuardOutcome(
            passed=False,
            disposition=RiskDisposition.BLOCK,
            event=G7ConcentrationBreach(side, concentration_ratio, conc_limit),
        )

    # --- CHECK 3: exposure (total committed / wallet, inclusive <= up to 100%) -------------
    total_committed = _dec(total_committed_usd)
    exposure_ratio = total_committed / wallet
    if exposure_ratio > exp_limit:
        return RiskGuardOutcome(
            passed=False,
            disposition=RiskDisposition.BLOCK,
            event=G7ExposureBreach(side, total_committed, wallet, exposure_ratio, exp_limit),
        )

    # --- CHECK 4: per-module semaphore (non-blocking probe; FREE passes) --------------------
    if semaphore_locked:
        return RiskGuardOutcome(
            passed=False,
            disposition=RiskDisposition.SKIP,
            event=G7SemaphoreBusy(side),
        )

    # ALL 4 CHECKS PASS -> proceed to gate:G8_Position_Sizer.
    return RiskGuardOutcome(passed=True, disposition=RiskDisposition.PASS, event=None)
