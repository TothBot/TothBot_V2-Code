"""param:perp_short_breaker_config - the per-module hedge breaker (recalibrated, TB00806e V2).

Source: 0500000 sec 13.9 HEDGE-BREAKER RECALIBRATION + Image10 param:perp_short_breaker_config
+ TB00000 v2_103 sec 8 perp_short_breaker_config. Mirrors the TB00806 oracle
(scripts/tb00806e_hedge_breaker.py run_module_hedge, lines 67-160), the V2 winner.

The whole-organism analysis (tb00806d) found the LIVE 5% frozen-baseline session-PAUSE breaker
STRANGLES a thin-edge hedge: a hedge sits at a drawdown-from-deposit BY DESIGN, and a PAUSED
hedge cannot self-equity-recover (the TB00804 DEADLOCK LAW: resume must be EXOGENOUS, never
self-equity). The FIX keeps the HALT on the FROZEN deposit (the ruin floor) but moves the PAUSE
to an EXOGENOUSLY-re-arming rolling-window basis wide enough to clear the hedge's ~7.5% natural
drawdown. The V2 winner: PAUSE = rolling-window 10% (exogenous re-arm) / HALT = frozen-deposit
20% (ruin floor) - the tightest safe ruin floor + a deadlock-safe re-arm.

NOT A NEW GATE. This is implemented ENTIRELY through pipeline.risk_guard.evaluate_risk_guard's
EXISTING override params (the baseline-source + the two drawdown thresholds), set per-module:
two evaluate_risk_guard calls per evaluation -

  HALT call:  portfolio_baseline = deposit (FROZEN), full_halt = halt_pct, session_pause = OFF
  PAUSE call: portfolio_baseline = deposit (frozen) OR a rolling-window high-water (rolling),
              session_pause = pause_pct, full_halt = OFF

The spot LONG module keeps its existing 5%/10% frozen-baseline breaker, UNAFFECTED. The
frozen-deposit ruin HALT + the sacred 1:1.5 R:R floor are UNTOUCHED.

PURE compute, Decimal-only (ar:AR-047). The RollingPeakTracker holds the trailing high-water
the rolling PAUSE baseline reads.
"""

from __future__ import annotations

import bisect
from dataclasses import dataclass
from decimal import Decimal

from ..config import registry
from ..exchange.position_mirror import PositionSide
from ..pipeline.risk_guard import RiskDisposition, evaluate_risk_guard

_ZERO = Decimal("0")
# Sentinel that DISABLES a drawdown threshold in an evaluate_risk_guard call (the threshold can
# never be reached). Mirrors tb00806e's 1e9 sentinel - one threshold per call, the other off.
_DISABLED = Decimal("1000000000")
# Seconds per day, for converting the registry window_days into the timestamp units the
# RollingPeakTracker compares (epoch-seconds). Callers using bar-index timestamps pass their
# own window instead.
DAY_SECONDS = Decimal("86400")


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the breaker (ar:AR-047)."""
    return Decimal(str(value))


@dataclass(frozen=True)
class HedgeBreakerConfig:
    """The per-module hedge-breaker config (param:perp_short_breaker_config). pause_baseline is
    'rolling' (exogenous re-arm over window) or 'frozen' (deposit). window is in the timestamp
    units the RollingPeakTracker compares (default: window_days * DAY_SECONDS)."""

    pause_pct: Decimal
    halt_pct: Decimal
    pause_baseline: str  # "rolling" | "frozen"
    window: Decimal

    @classmethod
    def from_registry(cls, *, name: str = "perp_short_breaker_config") -> "HedgeBreakerConfig":
        """Build from the registry seed (pause_pct, halt_pct, pause_baseline, window_days)."""
        pause_pct, halt_pct, pause_baseline, window_days = registry.value(name)
        return cls(
            pause_pct=_dec(pause_pct),
            halt_pct=_dec(halt_pct),
            pause_baseline=str(pause_baseline),
            window=_dec(window_days) * DAY_SECONDS,
        )


class RollingPeakTracker:
    """A trailing-window high-water tracker for the rolling PAUSE baseline (the exogenous,
    time-based re-arm that makes the breaker deadlock-safe). observe() records (time, wallet);
    peak(tnow) returns the max wallet over [tnow - window, tnow], never below the current
    wallet. Mirrors tb00806e's rolling_peak."""

    def __init__(self, window: object) -> None:
        self._window = _dec(window)
        self._t: list[Decimal] = []
        self._w: list[Decimal] = []

    def observe(self, time: object, wallet: object) -> None:
        """Record a (time, wallet) sample. Times must be non-decreasing (a monotonic clock)."""
        t = _dec(time)
        if self._t and t < self._t[-1]:
            raise ValueError("RollingPeakTracker times must be non-decreasing")
        self._t.append(t)
        self._w.append(_dec(wallet))

    def peak(self, tnow: object, current_wallet: object) -> Decimal:
        """Max wallet over [tnow - window, tnow], never below current_wallet (so trailing
        drawdown is always >= 0). Empty window -> current_wallet."""
        t = _dec(tnow)
        cur = _dec(current_wallet)
        lo = t - self._window
        k = bisect.bisect_left(self._t, lo)
        hi = max(self._w[k:], default=cur)
        return hi if hi > cur else cur


@dataclass(frozen=True)
class HedgeBreakerOutcome:
    """The outcome of one per-module hedge-breaker evaluation. disposition is the recalibrated
    breaker verdict (PASS/PAUSE/HALT/BLOCK/SKIP); baseline_used names the PAUSE baseline that
    was applied; event carries the underlying evaluate_risk_guard event on a non-pass."""

    disposition: RiskDisposition
    baseline_used: Decimal
    event: object | None
    code: str = "PERP_HEDGE_BREAKER"


def evaluate_hedge_breaker(
    side: PositionSide,
    *,
    wallet_balance: object,
    deposit: object,
    config: HedgeBreakerConfig,
    rolling_peak: object | None = None,
    candidate_committed_usd: object = 0,
    total_committed_usd: object = 0,
    semaphore_locked: bool = False,
) -> HedgeBreakerOutcome:
    """Evaluate the recalibrated per-module breaker via TWO evaluate_risk_guard calls (no new
    gate). The HALT call uses the FROZEN deposit (the ruin floor); the PAUSE call uses the
    rolling-window high-water (rolling) or the frozen deposit (frozen). Strict order: a HALT
    short-circuits a PAUSE, exactly as the single-gate cascade would.

    rolling_peak is the RollingPeakTracker.peak() value for a 'rolling' baseline (REQUIRED when
    pause_baseline == 'rolling'); ignored for 'frozen'. committed amounts default to 0 so the
    breaker isolates the DRAWDOWN dimension (the recalibrated part); the live pipeline's G7 still
    runs the concentration/exposure/semaphore checks on the real committed capital."""
    wallet = _dec(wallet_balance)
    dep = _dec(deposit)

    # --- HALT tier: frozen-deposit ruin floor (session_pause disabled) ---------------------
    halt = evaluate_risk_guard(
        side,
        wallet_balance=wallet,
        portfolio_baseline=dep,
        candidate_committed_usd=candidate_committed_usd,
        total_committed_usd=total_committed_usd,
        semaphore_locked=semaphore_locked,
        full_halt_drawdown_pct=config.halt_pct,
        session_pause_drawdown_pct=_DISABLED,
    )
    if halt.disposition is RiskDisposition.HALT:
        return HedgeBreakerOutcome(RiskDisposition.HALT, dep, halt.event)

    # --- PAUSE tier: rolling-window (exogenous re-arm) or frozen baseline -------------------
    if config.pause_baseline == "rolling":
        if rolling_peak is None:
            raise ValueError("rolling pause_baseline requires a rolling_peak value")
        base = _dec(rolling_peak)
        if base < wallet:
            base = wallet  # never below current wallet (drawdown >= 0)
    elif config.pause_baseline == "frozen":
        base = dep
    else:
        raise ValueError(f"unknown pause_baseline {config.pause_baseline!r}")

    pause = evaluate_risk_guard(
        side,
        wallet_balance=wallet,
        portfolio_baseline=base,
        candidate_committed_usd=candidate_committed_usd,
        total_committed_usd=total_committed_usd,
        semaphore_locked=semaphore_locked,
        full_halt_drawdown_pct=_DISABLED,
        session_pause_drawdown_pct=config.pause_pct,
    )
    if pause.disposition is RiskDisposition.PAUSE:
        return HedgeBreakerOutcome(RiskDisposition.PAUSE, base, pause.event)
    if not pause.passed:
        # Concentration / exposure / semaphore (the unchanged G7 checks).
        return HedgeBreakerOutcome(pause.disposition, base, pause.event)
    return HedgeBreakerOutcome(RiskDisposition.PASS, base, None)
