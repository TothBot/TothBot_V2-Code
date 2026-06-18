"""CIATS-owned expected_reward seed estimator - the DEC-124 run-to-reversal backtest harness.

Source: 0500000 dv1_250 D9 compute:expected_reward_run_to_reversal + g8d_a1_acceptance (rule:
Expected_RR_A1_Acceptance) + sec 7 ar:AR-062 (the two layer:L1a regime-reversal exit triggers) +
DEC-124 (run-to-reversal: NO fixed take-profit, NO max-hold; the take-profit IS the regime
reversal). Value home TB00000 sec 8 (expected_reward_estimator_seed; Bill ruling TB00654).

expected_reward is the CIATS run-to-reversal estimate fed to the SACRED A1 entry-acceptance floor
ONLY (admit iff expected_reward / net_loss >= 1.5); it NEVER constructs an exit price and NEVER
closes a position. It is a SEED COMPUTED FROM DATA - CIATS owning the value day one (valid from the
first trade); the 200-trade floor gates live paper tuning ONLY, never ownership. NOT a hardcoded
literal.

THE HARNESS (a PURE Decimal backtest over injected historical OHLC; no network, ar:AR-047):
replay the layer:L1a regime-reversal exit per pair/regime over a historical bar series and take the
per-regime MEDIAN realized favorable excursion as a fraction of entry_fill_price.

  1. Precompute the rolling daily regime ONCE per bar: classes[j] = compute_regime(bars[:j+1])
     (exclude_forming=False - every historical bar is committed; the scheduler's as-of-close
     classification). O(n) compute_regime calls, not O(n^2) per-entry - the replay then reads them.
  2. For each entry bar i whose regime permits an entry (gate:G3 permitted_sides), open a position
     at bars[i].close on each permitted side and walk forward j = i+1, i+2, ...; at each day check
     the SAME two production detectors fed from classes[j]:
        EC-L1A-002 detect_daily_regime_downgrade(position, classes[j])    (the daily downgrade)
        EC-L1A-001 detect_htf_regime_reversal(position, ema20, ema50)     (the EMA cross-below)
     The FIRST day either fires is the run-to-reversal exit; exit at bars[j].close.
  3. excursion (direction-symmetric, DEC-124): LONG (exit - entry)/entry, SHORT (entry - exit)/
     entry. Grouped by the ENTRY regime. An entry whose reversal never fires within the series is
     DISCARDED (no realized run-to-reversal to measure - it would bias the median to an artificial
     end-of-data exit). The per-regime seed = the MEDIAN of the realized excursions.

DERIVATIONS (transparent; faithful to the figure, no value invented - an obvious method choice
done automatically per Bill ruling TB00731, not a new tunable):
 (1) The two L1a detectors are composed EXACTLY as production runs them (regime_exit.py), fed from
     the rolling classification - so the replayed "reversal" is the production reversal. EC-L1A-001
     is the live 1H EMA cross; in this single-series daily backtest it is fed the rolling daily
     EMA20/EMA50 from the same classification (the series' own EMA reversal). The live organism
     supplies the true 1H series; the seed is later tuned from paper data, so the daily-EMA proxy is
     the faithful day-one estimate (the daily downgrade EC-L1A-002 is the dominant exit either way).
 (2) The excursion is SIGN-PRESERVING: a reversal that exits below entry contributes a NEGATIVE
     excursion. The median realized run-to-reversal return is the honest central tendency; a regime
     whose median is <= 0 simply never clears the A1 floor (the system correctly declines to trade a
     regime that historically does not pay running to reversal). No clamping (sec-8: "measure
     (exit_ref - entry)/entry ... take the per-regime median").
 (3) NON_DIR_NORMAL permits BOTH sides (Bill ruling DEC-B half-size mirror); both sides' excursions
     pool into the one (pair, NON_DIR_NORMAL) bucket - matching the (symbol, regime) provider
     contract (the seam reads expected_reward(symbol, regime), side-agnostic).
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from ..exchange.daily_decision import DECISION_EMA_FAST, DECISION_EMA_SLOW
from ..exchange.position_mirror import Position, PositionSide
from ..exchange.regime_exit import (
    detect_daily_regime_downgrade,
    detect_htf_regime_reversal,
)
from ..pipeline.sweep import permitted_sides
from ..regime.engine import RegimeClassification, RegimeComputeError, compute_regime
from ..regime.indicators import ema
from ..regime.taxonomy import Regime
from .seed_estimators import quantile

# The DEC-124 central-tendency recipe: the per-regime seed is the MEDIAN realized excursion (the
# 0.5 quantile, type-7 - identical to the standard median). The quantile level is the method, not a
# tunable seed.
EXPECTED_REWARD_QUANTILE = Decimal("0.5")


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def rolling_classifications(
    symbol: str, bars: Sequence[object]
) -> list[RegimeClassification | None]:
    """classes[j] = the daily regime as of bars[:j+1] (exclude_forming=False), or None where the
    series is too short to classify (the early bars before the max(28, EMA50) floor). One
    compute_regime call per bar - the replay reads this, so the whole harness is O(n) classifies."""
    out: list[RegimeClassification | None] = []
    for j in range(len(bars)):
        try:
            out.append(compute_regime(symbol, bars[: j + 1], exclude_forming=False))
        except RegimeComputeError:
            out.append(None)
    return out


def rolling_ema(closes: Sequence[Decimal], period: int) -> list[Decimal | None]:
    """ema_at[j] = the EMA(period) over closes[:j+1] (None before the SMA seed at index period-1),
    computed in ONE O(n) pass (seed = SMA of the first `period`, then the standard EMA step matching
    live_driver._step_htf / DailyDecisionCache.advance). TB00790: the replay reads ema_fast/ema_slow
    at each bar so the run-to-reversal exit is the 24h EMA(decision_ema_fast)/EMA(decision_ema_slow)
    bearish cross - the SAME L1a reversal the live exit now fires on (was the daily EMA20/50)."""
    out: list[Decimal | None] = []
    if period <= 0:
        return [None] * len(closes)
    alpha = Decimal(2) / (period + 1)
    prev: Decimal | None = None
    for j, close in enumerate(closes):
        if j + 1 < period:
            out.append(None)
        elif j + 1 == period:
            prev = ema(list(closes[: j + 1]), period)  # SMA-seeded EMA over the first `period`
            out.append(prev)
        else:
            prev = (close - prev) * alpha + prev
            out.append(prev)
    return out


def _excursion(side: PositionSide, entry: Decimal, exit_price: Decimal) -> Decimal:
    """The direction-symmetric realized excursion as a fraction of entry (DEC-124): LONG (exit -
    entry)/entry, SHORT (entry - exit)/entry. Sign-preserving (a losing reversal is negative)."""
    if side is PositionSide.LONG:
        return (exit_price - entry) / entry
    return (entry - exit_price) / entry


def replay_excursions(
    symbol: str, bars: Sequence[object]
) -> dict[Regime, list[Decimal]]:
    """The per-entry-regime realized run-to-reversal excursions over the historical `bars`.

    For every bar whose regime permits an entry, open on each permitted side, hold until the FIRST
    layer:L1a reversal (EC-L1A-001 / EC-L1A-002) fires, and record the direction-symmetric excursion
    under the ENTRY regime. Entries whose reversal never fires within the series are discarded.
    `bars` expose .close (DailyBar / RestOhlcBar). PURE - reads only the precomputed classifications.
    """
    classes = rolling_classifications(symbol, bars)
    # TB00790: the run-to-reversal exit is now the 24h EMA(decision_ema_fast)/EMA(decision_ema_slow)
    # bearish cross (the live L1a), so the replay walks each entry to the FIRST bar where that cross
    # fires (OR the daily downgrade, still a live L1a). Rolling EMAs over the daily decision closes,
    # one O(n) pass each (the same series the live DailyDecisionCache seeds + advances on).
    closes = [_dec(b.close) for b in bars]
    ema_fast = rolling_ema(closes, DECISION_EMA_FAST)
    ema_slow = rolling_ema(closes, DECISION_EMA_SLOW)
    by_regime: dict[Regime, list[Decimal]] = {}
    n = len(bars)
    for i in range(n - 1):
        cls_i = classes[i]
        if cls_i is None:
            continue
        entry = closes[i]
        if entry == 0:
            continue
        for side in permitted_sides(cls_i.regime):
            position = Position(symbol=symbol, side=side, qty=Decimal(0), avg_entry_price=entry)
            for j in range(i + 1, n):
                cls_j = classes[j]
                if cls_j is None:
                    continue
                ef, es = ema_fast[j], ema_slow[j]
                htf_reversed = (
                    ef is not None
                    and es is not None
                    and detect_htf_regime_reversal(position, ef, es) is not None
                )
                fired = detect_daily_regime_downgrade(position, cls_j) is not None or htf_reversed
                if fired:
                    excursion = _excursion(side, entry, _dec(bars[j].close))
                    by_regime.setdefault(cls_i.regime, []).append(excursion)
                    break
    return by_regime


def compute_expected_reward(
    symbol: str, bars: Sequence[object]
) -> dict[Regime, Decimal]:
    """The per-regime expected_reward seed for one pair: the MEDIAN realized run-to-reversal
    excursion over the historical `bars`, per entry regime. Only regimes with at least one realized
    reversal appear (a regime never observed, or whose entries never reversed in-window, has no
    seed - the provider raises ProviderNotReady there)."""
    return {
        regime: quantile(excursions, EXPECTED_REWARD_QUANTILE)
        for regime, excursions in replay_excursions(symbol, bars).items()
        if excursions
    }


class ExpectedRewardStore:
    """The per-(pair, regime) expected_reward seed store (computed once from historical OHLC at
    universe load; CIATS refines per-module from the 200-trade floor). LiveProviders.expected_reward
    reads it (side-agnostic - the seam passes (symbol, regime))."""

    def __init__(self) -> None:
        self._by_key: dict[tuple[str, Regime], Decimal] = {}

    def put(self, symbol: str, regime: Regime, value: object) -> None:
        self._by_key[(symbol, regime)] = _dec(value)

    def get(self, symbol: str, regime: Regime) -> Decimal | None:
        return self._by_key.get((symbol, regime))

    def seed_from_bars(self, symbol: str, bars: Sequence[object]) -> None:
        """Compute + store every observed regime's expected_reward seed for `symbol` from one
        historical daily bar series."""
        for regime, value in compute_expected_reward(symbol, bars).items():
            self._by_key[(symbol, regime)] = value
