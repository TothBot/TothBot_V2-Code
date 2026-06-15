"""CIATS-owned seed estimators - compute the sec-8 seed values from historical OHLC (not literals).

Source: 0500000 dv1_250 ar:AR-069 + DEC-128 (mpp_abs_cap_pct = the per-pair/side empirical Q95 of
the adverse close-to-next-open displacement on the 5m OHLC series, over the historical-OHLC backtest
dataset; nonparametric quantile for the heavy-tailed gap distribution; value home TB00000 sec 8).
CIATS-OWNED FROM THE START - the historical seed IS CIATS setting the value from data (valid day
one); the 200-trade floor gates live tuning ONLY, never ownership.

This module holds the PURE statistical seed computation (no I/O, Decimal-only, ar:AR-047). The
historical bars are pulled by the REST GetOHLCData edge at universe load; the computed per-(pair,
side) caps live in an MppCapStore the LiveProviders.mpp_abs_cap_pct reads. The expected_reward
estimator (DEC-124 regime-reversal replay) is the companion seed and lands separately.

ADVERSE DISPLACEMENT (the method, an obvious choice - not a new tunable): for each consecutive
5m candle pair, the close-to-next-open gap fraction = (open[i+1] - close[i]) / close[i]. A LONG
entry pays MORE when the next open gaps UP (adverse = +gap); a SHORT receives LESS when it gaps
DOWN (adverse = -gap). A favorable move contributes 0 adverse displacement. The Q95 over the whole
series is the cap: 95% of adverse gaps fall within it. The 0.95 quantile is the DEC-128 recipe,
not a CIATS seed (the cap VALUE is the seed; the quantile level is the method).
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

from ..exchange.position_mirror import PositionSide

# The DEC-128 quantile level (the recipe, not a tunable seed).
MPP_QUANTILE = Decimal("0.95")


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def adverse_gap_fractions(bars: Sequence[object], side: PositionSide) -> list[Decimal]:
    """The per-candle adverse close-to-next-open displacement fractions (>= 0) for `side`.

    For consecutive bars i, i+1: gap = (open[i+1] - close[i]) / close[i]; LONG adverse = max(0, gap)
    (a gap UP costs a buyer more), SHORT adverse = max(0, -gap) (a gap DOWN pays a seller less). A
    favorable move contributes 0. `bars` expose .open/.close (RestOhlcBar)."""
    out: list[Decimal] = []
    for i in range(len(bars) - 1):
        close_i = _dec(bars[i].close)
        if close_i == 0:
            continue
        gap = (_dec(bars[i + 1].open) - close_i) / close_i
        adverse = gap if side is PositionSide.LONG else -gap
        out.append(adverse if adverse > 0 else Decimal(0))
    return out


def quantile(values: Sequence[object], q: object = MPP_QUANTILE) -> Decimal:
    """The nonparametric q-quantile by linear interpolation between closest ranks (the common
    'type 7' / numpy-default method). Decimal-exact. Raises on an empty series."""
    xs = sorted(_dec(v) for v in values)
    n = len(xs)
    if n == 0:
        raise ValueError("quantile of an empty series")
    if n == 1:
        return xs[0]
    rank = _dec(q) * (n - 1)              # 0 .. n-1
    lo = int(rank)                       # floor (rank >= 0)
    if lo >= n - 1:
        return xs[-1]
    frac = rank - lo
    return xs[lo] + frac * (xs[lo + 1] - xs[lo])


def compute_mpp_cap(
    bars: Sequence[object], side: PositionSide, *, q: object = MPP_QUANTILE
) -> Decimal:
    """The DEC-128 mpp_abs_cap_pct seed for one pair/side: the Q95 adverse close-to-next-open
    displacement over the historical 5m OHLC `bars`. Needs >= 2 bars (one gap)."""
    fractions = adverse_gap_fractions(bars, side)
    if not fractions:
        raise ValueError("compute_mpp_cap needs at least 2 bars")
    return quantile(fractions, q)


class MppCapStore:
    """The per-(pair, side) mpp_abs_cap_pct seed store (computed once from historical OHLC at
    universe load; CIATS refines per-pair/side from the 200-trade floor). LiveProviders.mpp_abs_
    cap_pct reads it."""

    def __init__(self) -> None:
        self._by_key: dict[tuple[str, PositionSide], Decimal] = {}

    def put(self, symbol: str, side: PositionSide, value: object) -> None:
        self._by_key[(symbol, side)] = _dec(value)

    def get(self, symbol: str, side: PositionSide) -> Decimal | None:
        return self._by_key.get((symbol, side))

    def seed_from_bars(self, symbol: str, bars: Sequence[object]) -> None:
        """Compute + store BOTH sides' caps for `symbol` from one historical 5m bar series."""
        for side in (PositionSide.LONG, PositionSide.SHORT):
            self._by_key[(symbol, side)] = compute_mpp_cap(bars, side)
