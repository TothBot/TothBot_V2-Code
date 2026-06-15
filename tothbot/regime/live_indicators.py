"""LiveIndicators - the per-symbol incremental running indicator state (ar:AR-016 / AR-075).

Source: 0500000 dv1_250 ar:AR-016 (ATR(14) incremental running calculation, O(1) per update) +
ar:AR-075 (the WS-Manager per-symbol state dict carries the SSS indicators as incremental running
values - rsi_14_avg_gain, rsi_14_avg_loss, ema_9, ema_21, volume_ma_20 - "updated incrementally on
each ohlc(5m) candle close, same pattern as ATR(14)") + ar:AR-076 (RSI(14) = Wilder SMMA, EMA(9)/
EMA(21) = standard EMA) + ar:AR-044 / AR-068 (the GetOHLCData(interval=5) warm-up seeds all five
per-pair indicators + the 5m ATR(14); SMA/Wilder seed over the first period, incremental thereafter).

THE DESIGN DECISION (TB00736 NSI sec 3 B2a, settled from the AR text): option (i) - maintain EXACT
incremental running values and feed the gates those (the Pre-Step-2 pre-computation cache read,
"serves them to the gates without recomputation"). This is what AR-016/AR-075/AR-076 literally
describe. Because the Wilder SMMA (ATR, RSI), the standard EMA, and the running-SMA volume
recurrences are mathematically IDENTICAL to the batch computation over the full committed series
(given the same seed), the live running value after seed_from_bars(warm-up) + N x update() is
bit-identical to the batch evaluate_sss / atr_14_series over [warm-up ++ the N committed candles].
No new tunable, no behavioral divergence from the pure batch units - just O(1) maintenance, exactly
as the diagram specifies. The equivalence is asserted directly in the unit tests (the divergence
guard). No new seed falls out (NSI sec 4: not a Bill question).

This is a PURE unit: no I/O, no WS state, Decimal-only (ar:AR-047). The warm-up orchestrator
(REST edge) seeds it; the WS-Manager holds one per monitored pair and steps it on each committed
5m candle close; the per-5m sweep reads .atr_14 (G8) + .sss_verdict(side) (the SSS gate).
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from ..config import registry
from .indicators import atr_14_series, ema
from .sss import (
    SignalSide,
    SssVerdict,
    rsi_from_state,
    rsi_state,
    sss_verdict_from_indicators,
    step_rsi_state,
    volume_ma_20,
)

_RSI_PERIOD = 14
_ATR_PERIOD = 14
_EMA_SHORT = int(registry.value("sss_ema_short"))            # 9
_EMA_LONG = int(registry.value("sss_ema_long"))              # 21
_VOLUME_MA_PERIOD = 20


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


class IndicatorSeedError(ValueError):
    """Too few committed candles to seed the live indicator state (warm-up incomplete)."""


@dataclass(frozen=True)
class OhlcCandle:
    """One committed 5-minute OHLC candle - the unit of LiveIndicators.update (Decimal-only)."""

    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


def _wilder_step(prev: Decimal, value: Decimal, period: int) -> Decimal:
    """One Wilder SMMA step (ar:AR-016): (prev*(period-1) + value)/period. Same recurrence as
    indicators._wilder_smma stepping and sss.step_rsi_state - shared math, no divergence."""
    return (prev * (period - 1) + value) / period


class LiveIndicators:
    """The per-symbol incremental running indicator state (ar:AR-016/AR-075).

    Seeded once from the GetOHLCData(interval=5) committed warm-up series (seed_from_bars), then
    stepped O(1) on each committed 5m candle close (update). Holds the five SSS running values +
    the 5m ATR(14) (the G8 trade-ATR, distinct from the DAILY regime ATR). All values are
    bit-identical to the batch units over the same committed history (see module docstring)."""

    __slots__ = (
        "symbol",
        "_rsi_period",
        "_atr_period",
        "_ema_short",
        "_ema_long",
        "_volume_period",
        "_alpha_short",
        "_alpha_long",
        "_atr",
        "_prev_close",
        "_avg_gain",
        "_avg_loss",
        "_ema9",
        "_ema21",
        "_volume_ma20",
        "_current_volume",
        "_seeded",
    )

    def __init__(
        self,
        symbol: str,
        *,
        rsi_period: int = _RSI_PERIOD,
        atr_period: int = _ATR_PERIOD,
        ema_short: int = _EMA_SHORT,
        ema_long: int = _EMA_LONG,
        volume_period: int = _VOLUME_MA_PERIOD,
    ) -> None:
        self.symbol = symbol
        self._rsi_period = int(rsi_period)
        self._atr_period = int(atr_period)
        self._ema_short = int(ema_short)
        self._ema_long = int(ema_long)
        self._volume_period = int(volume_period)
        self._alpha_short = Decimal(2) / (self._ema_short + 1)   # standard EMA(9) alpha 2/10
        self._alpha_long = Decimal(2) / (self._ema_long + 1)     # standard EMA(21) alpha 2/22
        self._atr: Decimal | None = None
        self._prev_close: Decimal | None = None
        self._avg_gain: Decimal | None = None
        self._avg_loss: Decimal | None = None
        self._ema9: Decimal | None = None
        self._ema21: Decimal | None = None
        self._volume_ma20: Decimal | None = None
        self._current_volume: Decimal | None = None
        self._seeded = False

    @property
    def min_seed_closes(self) -> int:
        """Minimum committed closes to seed all five indicators (EMA21 is the binding floor)."""
        return max(self._ema_long, self._rsi_period + 1, self._atr_period + 1)

    @property
    def seeded(self) -> bool:
        """True once all five indicators + the 5m ATR are seeded (ar:AR-068 READY precondition)."""
        return self._seeded

    def seed_from_bars(self, bars: object) -> None:
        """Seed the running state from the committed GetOHLCData(interval=5) warm-up bars.

        `bars` is the OhlcResponse.committed tuple (response[:-1] per ar:AR-017/AR-044 - already
        excludes the forming candle); each bar exposes .high/.low/.close/.volume. Computes the
        SMA/Wilder seeds via the SAME batch units the live steps mirror (ar:AR-068)."""
        highs = [_dec(getattr(b, "high")) for b in bars]
        lows = [_dec(getattr(b, "low")) for b in bars]
        closes = [_dec(getattr(b, "close")) for b in bars]
        volumes = [_dec(getattr(b, "volume")) for b in bars]
        n = len(closes)
        if n < self.min_seed_closes or len(volumes) < self._volume_period:
            raise IndicatorSeedError(
                f"{self.symbol}: {n} closes / {len(volumes)} volumes < seed minimum "
                f"({self.min_seed_closes} closes, {self._volume_period} volumes)"
            )
        self._atr = atr_14_series(highs, lows, closes, self._atr_period)[-1]
        self._avg_gain, self._avg_loss = rsi_state(closes, self._rsi_period)
        self._ema9 = ema(closes, self._ema_short)
        self._ema21 = ema(closes, self._ema_long)
        self._volume_ma20 = volume_ma_20(volumes, self._volume_period)
        self._current_volume = volumes[-1]
        self._prev_close = closes[-1]
        self._seeded = True

    def update(self, candle: OhlcCandle) -> None:
        """Step every running value O(1) on one committed 5m candle close (ar:AR-016/AR-075).

        ATR via the Wilder TR step (the new TR uses the prior committed close); RSI via the Wilder
        avg_gain/avg_loss step; EMA9/EMA21 via the standard EMA step; VolumeMA20 via the running
        SMA step. After the step, prev_close advances to this candle's close."""
        if not self._seeded:
            raise IndicatorSeedError(f"{self.symbol}: update before seed_from_bars")
        h, l, c, v = _dec(candle.high), _dec(candle.low), _dec(candle.close), _dec(candle.volume)
        prev = self._prev_close
        assert prev is not None  # _seeded guarantees this

        true_range = max(h - l, abs(h - prev), abs(l - prev))
        self._atr = _wilder_step(self._atr, true_range, self._atr_period)

        gain = max(Decimal(0), c - prev)
        loss = max(Decimal(0), prev - c)
        self._avg_gain = step_rsi_state(self._avg_gain, gain, self._rsi_period)
        self._avg_loss = step_rsi_state(self._avg_loss, loss, self._rsi_period)

        self._ema9 = (c - self._ema9) * self._alpha_short + self._ema9
        self._ema21 = (c - self._ema21) * self._alpha_long + self._ema21

        self._volume_ma20 = _wilder_step(self._volume_ma20, v, self._volume_period)
        self._current_volume = v
        self._prev_close = c

    # --- the served running values (the Pre-Step-2 pre-computation cache read, ar:AR-075) -------
    @property
    def atr_14(self) -> Decimal | None:
        """The 5m ATR(14) running value (G8 trade-ATR, ar:AR-016). None until seeded."""
        return self._atr

    @property
    def rsi_14(self) -> Decimal | None:
        """indicator:RSI from the running (avg_gain, avg_loss) with the HR-SSS-004 guard."""
        if self._avg_gain is None:
            return None
        return rsi_from_state(self._avg_gain, self._avg_loss)

    @property
    def ema9(self) -> Decimal | None:
        return self._ema9

    @property
    def ema21(self) -> Decimal | None:
        return self._ema21

    @property
    def volume_ma20(self) -> Decimal | None:
        return self._volume_ma20

    @property
    def current_volume(self) -> Decimal | None:
        return self._current_volume

    def sss_verdict(
        self,
        side: SignalSide,
        *,
        rsi_low: object | None = None,
        rsi_high: object | None = None,
        volume_threshold: object | None = None,
    ) -> SssVerdict:
        """The SSS verdict for `side` from the running values (the LIVE path, ar:AR-075).

        Reuses sss.sss_verdict_from_indicators - the identical three-factor PASS rule the batch
        evaluate_sss applies - so the live verdict matches the batch verdict over the same history.
        rsi_low/rsi_high/volume_threshold default to the side's registry seeds in the SSS unit."""
        if not self._seeded:
            raise IndicatorSeedError(f"{self.symbol}: sss_verdict before seed_from_bars")
        kwargs = {} if volume_threshold is None else {"volume_threshold": volume_threshold}
        return sss_verdict_from_indicators(
            self.symbol,
            side=side,
            rsi=self.rsi_14,
            ema9=self._ema9,
            ema21=self._ema21,
            volume=self._current_volume,
            volume_ma20=self._volume_ma20,
            rsi_low=rsi_low,
            rsi_high=rsi_high,
            **kwargs,
        )
