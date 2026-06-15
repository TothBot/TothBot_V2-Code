"""mod:CIATS_EWMA_Monitor - the exponentially-weighted drift monitor (lambda = 0.2).

Source: 0500000 dv1_250 sec 6/7 mod:CIATS_EWMA_Monitor + the D1 CIATS-FEE-002/003 fee-tier
divergence detection (the canonical worked use: "the EWMA (lambda=0.2) of actual_entry_fee_pct
diverges from the FEE_TAKER_PCT baseline by more than fee_tier_divergence_threshold sustained
over fee_tier_divergence_sustained_trades -> FEE_TIER_CHANGE_DETECTED").

A signals-ONLY monitor (D5: CIATS monitors + alerts, NEVER adjusts exchange-defined params): it
tracks an exponentially-weighted moving average of a per-trade signal and flags SUSTAINED
divergence from a baseline. The recurrence (lambda = 0.2, more weight on history):

    ewma_t = lambda * x_t + (1 - lambda) * ewma_{t-1}      (ewma_0 = x_0)

A reusable monitor over ANY CIATS signal (fee pct, reject rate, rate-counter, ...): construct
with the signal's baseline + divergence threshold + the sustained-count bar, feed each
observation, and read `sustained` (the divergence has held over >= sustained_n observations -
the fire condition that logs the labelled event + alerts the operator).

PURE state, Decimal-only (ar:AR-047). The labelled event emission + the operator alert are the
caller's (the Logger membrane); this is the EWMA + sustained-divergence math.
"""

from __future__ import annotations

from decimal import Decimal

_ONE = Decimal("1")
_DEFAULT_LAMBDA = Decimal("0.2")


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the monitor (AR-047)."""
    return Decimal(str(value))


class EwmaMonitor:
    """An EWMA drift monitor over one CIATS signal. Feed observations with update(); read value /
    diverging / sustained. Optionally configured with a baseline + threshold + sustained_n to
    track SUSTAINED divergence (the fire condition)."""

    def __init__(
        self,
        *,
        lambda_: object = _DEFAULT_LAMBDA,
        baseline: object | None = None,
        threshold: object | None = None,
        sustained_n: int = 1,
    ) -> None:
        self._lambda = _dec(lambda_)
        self._ewma: Decimal | None = None
        self._baseline = None if baseline is None else _dec(baseline)
        self._threshold = None if threshold is None else _dec(threshold)
        self._sustained_n = sustained_n
        self._consecutive = 0

    def update(self, x: object) -> Decimal:
        """Fold one observation into the EWMA (ewma_0 = x; else lambda*x + (1-lambda)*prev) and,
        when a baseline+threshold are configured, advance/reset the consecutive-divergence run.
        Returns the new EWMA."""
        xv = _dec(x)
        self._ewma = xv if self._ewma is None else self._lambda * xv + (_ONE - self._lambda) * self._ewma
        if self._baseline is not None and self._threshold is not None:
            if abs(self._ewma - self._baseline) > self._threshold:
                self._consecutive += 1
            else:
                self._consecutive = 0
        return self._ewma

    @property
    def value(self) -> Decimal | None:
        """The current EWMA, or None before the first observation."""
        return self._ewma

    @property
    def consecutive_divergences(self) -> int:
        """How many consecutive recent observations have left the EWMA beyond the threshold."""
        return self._consecutive

    @property
    def diverging(self) -> bool:
        """Whether the CURRENT EWMA sits beyond baseline +/- threshold (a configured monitor)."""
        if self._ewma is None or self._baseline is None or self._threshold is None:
            return False
        return abs(self._ewma - self._baseline) > self._threshold

    @property
    def sustained(self) -> bool:
        """Whether the divergence has held over >= sustained_n consecutive observations (the
        labelled-event fire condition, e.g. FEE_TIER_CHANGE_DETECTED). False if unconfigured."""
        if self._baseline is None or self._threshold is None:
            return False
        return self._consecutive >= self._sustained_n
