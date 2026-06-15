"""Daily regime indicators - pure Decimal units (mod:Regime_Engine, 0500000 Image4 R9).

Source: 0500000 dv1_242 sec 5 (Regime Taxonomy) + Image4 mod:Regime_Engine computation:
  RE-008  indicator:ADX_14  - Wilder DMI/ADX on daily high/low/close.
  RE-009  indicator:EMA20_daily / EMA50_daily - standard EMA, SMA seed.
  RE-010  indicator:ATR_14  - Wilder SMMA of the true range.
  RE-012  ATR(14) percentile rank over a 50-day rolling window (param:atr_rolling_window).

All regime arithmetic is Python Decimal; float is PROHIBITED (rule:HR-REGIME-006, ar:AR-047).
The daily candle source is REST channel:kraken_rest_GetOHLCData (interval=1440) ONLY
(rule:HR-REGIME-003); response[-1] (the uncommitted forming candle) is excluded BEFORE these
units run, by the caller / engine.compute_regime per ar:AR-017. These functions are PURE: a
committed high/low/close series in, one Decimal (or a Decimal series) out - no I/O, no state,
no candle-exclusion logic. A minimum of 28 daily candles is required for ADX (Image4); the
GetOHLCData 720-candle window is adequate.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import Decimal

# Wilder's smoothing period for ADX / ATR; standard EMA periods are passed in.
_PERIOD = 14


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the math (ar:AR-047)."""
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _coerce(values: Sequence[object]) -> list[Decimal]:
    return [_dec(v) for v in values]


def true_ranges(
    highs: Sequence[object], lows: Sequence[object], closes: Sequence[object]
) -> list[Decimal]:
    """The Wilder true-range series TR[t] for t = 1 .. n-1 (length n-1).

    TR = max(high - low, abs(high - prev_close), abs(low - prev_close)) per RE-008/RE-010.
    The first candle has no prior close, so the series begins at the second candle.
    """
    h, l, c = _coerce(highs), _coerce(lows), _coerce(closes)
    n = len(c)
    if not (len(h) == len(l) == n):
        raise ValueError("highs, lows, closes must be equal length")
    out: list[Decimal] = []
    for t in range(1, n):
        prev_close = c[t - 1]
        out.append(max(h[t] - l[t], abs(h[t] - prev_close), abs(l[t] - prev_close)))
    return out


def _wilder_smma(values: Sequence[Decimal], period: int) -> list[Decimal]:
    """Wilder's smoothed moving average (SMMA) series of `values`.

    Seed = sum(values[:period]) / period (placed at index period-1); thereafter
    smma = (prev_smma * (period - 1) + value) / period. Returns the smoothed values
    only (length len(values) - period + 1), element 0 being the seed.
    """
    if len(values) < period:
        raise ValueError("not enough values to seed the Wilder SMMA")
    seed = sum(values[:period], Decimal(0)) / period
    out = [seed]
    prev = seed
    for v in values[period:]:
        prev = (prev * (period - 1) + v) / period
        out.append(prev)
    return out


def directional_movement(
    highs: Sequence[object], lows: Sequence[object]
) -> tuple[list[Decimal], list[Decimal]]:
    """The Wilder +DM / -DM series for t = 1 .. n-1 (each length n-1), per RE-008.

    +DM = max(high - prev_high, 0) when (high - prev_high) > (prev_low - low) else 0.
    -DM = max(prev_low - low, 0) when (prev_low - low) > (high - prev_high) else 0.
    """
    h, l = _coerce(highs), _coerce(lows)
    if len(h) != len(l):
        raise ValueError("highs, lows must be equal length")
    plus: list[Decimal] = []
    minus: list[Decimal] = []
    for t in range(1, len(h)):
        up = h[t] - h[t - 1]          # high - prev_high
        down = l[t - 1] - l[t]        # prev_low - low
        plus.append(up if (up > down and up > 0) else Decimal(0))
        minus.append(down if (down > up and down > 0) else Decimal(0))
    return plus, minus


def adx_14(
    highs: Sequence[object],
    lows: Sequence[object],
    closes: Sequence[object],
    period: int = _PERIOD,
) -> Decimal:
    """indicator:ADX_14 - the final Wilder Average Directional Index (RE-008).

    Requires at least 2 * period (28) candles. TR / +DM / -DM are Wilder-smoothed; then
    +DI = 100 * smma(+DM) / smma(TR), -DI = 100 * smma(-DM) / smma(TR),
    DX = 100 * abs(+DI - -DI) / (+DI + -DI), and ADX = Wilder SMMA(period) of DX.
    Degenerate zero-denominator days (no range / DI sum 0) contribute DX = 0.
    """
    if len(closes) < 2 * period:
        raise ValueError(f"ADX needs at least {2 * period} candles, got {len(closes)}")
    tr = true_ranges(highs, lows, closes)
    plus_dm, minus_dm = directional_movement(highs, lows)

    smma_tr = _wilder_smma(tr, period)
    smma_plus = _wilder_smma(plus_dm, period)
    smma_minus = _wilder_smma(minus_dm, period)

    hundred = Decimal(100)
    dx: list[Decimal] = []
    for s_tr, s_plus, s_minus in zip(smma_tr, smma_plus, smma_minus):
        if s_tr == 0:
            dx.append(Decimal(0))
            continue
        plus_di = hundred * s_plus / s_tr
        minus_di = hundred * s_minus / s_tr
        di_sum = plus_di + minus_di
        dx.append(hundred * abs(plus_di - minus_di) / di_sum if di_sum != 0 else Decimal(0))

    return _wilder_smma(dx, period)[-1]


def ema(values: Sequence[object], period: int) -> Decimal:
    """The final standard EMA of `values` (RE-009): alpha = 2 / (period + 1), SMA seed.

    Seed = sum(values[:period]) / period; thereafter ema = (value - ema) * alpha + ema.
    EMA20 uses alpha = 2/21, EMA50 uses alpha = 2/51 (Image4).
    """
    v = _coerce(values)
    if len(v) < period:
        raise ValueError(f"EMA{period} needs at least {period} values, got {len(v)}")
    alpha = Decimal(2) / (period + 1)
    e = sum(v[:period], Decimal(0)) / period
    for x in v[period:]:
        e = (x - e) * alpha + e
    return e


def atr_14_series(
    highs: Sequence[object],
    lows: Sequence[object],
    closes: Sequence[object],
    period: int = _PERIOD,
) -> list[Decimal]:
    """The per-day indicator:ATR_14 series (Wilder SMMA of the true range, RE-010).

    Returned newest-last; element 0 is the seed (mean of the first `period` TRs). The
    percentile-rank buffer is taken from the tail of this series.
    """
    return _wilder_smma(true_ranges(highs, lows, closes), period)


def atr_percentile_rank(atr_series: Sequence[object], window: int = 50) -> Decimal:
    """RE-012 percentile rank of the current ATR over the trailing `window` (50-day) buffer.

    buffer = the last `window` ATR values (or all, if fewer); current = the last value.
    rank = count(values strictly LESS THAN current) / len(buffer) * 100. Volatility is
    ELEVATED_VOL when rank > param:atr_percentile_thresh (67th) else NORMAL_VOL.
    """
    series = _coerce(atr_series)
    if not series:
        raise ValueError("atr_series is empty")
    buffer = series[-window:]
    current = series[-1]
    below = sum(1 for v in buffer if v < current)
    return Decimal(below) / Decimal(len(buffer)) * Decimal(100)
