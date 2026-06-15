"""mod:Regime_Engine daily classification - the pure compute path (0500000 Image4 R9).

Source: 0500000 dv1_242 sec 5 + Image4 mod:Regime_Engine q1_do / computation. compute_regime
is the per-pair daily-00:00-UTC classifier core: it takes the REST GetOHLCData daily series,
excludes response[-1] (the uncommitted forming candle) per ar:AR-017, runs the RE-008/009/010/
012 indicators (indicators.py), and emits one of the six regime: tokens (taxonomy.py). Both
asset_regime (per pair) and market_regime (BTC/USD anchor, ar:AR-074) are produced by calling
this once per pair and once for BTC/USD.

PURE: a daily-bar series in, a RegimeClassification out. No REST I/O, no scheduler, no cache,
no 1.1s AR-036 stagger - those are the daily-compute orchestrator's edges, wired with the REST
client (Path B). Decimal-only (rule:HR-REGIME-006 / ar:AR-047); the WS ohlc channel (5m/1h)
MUST NOT feed this (rule:HR-REGIME-003) - daily candles only.

A complete classification requires enough committed candles for EVERY indicator: ADX(14) needs
28 (Image4), EMA50 needs htf_ema_long (50), ATR(14) needs 15. The binding floor is therefore
max(28, htf_ema_long) - EMA50 dominates at the 50 seed. GetOHLCData returns 720 committed, so
this is always satisfied in practice; a short series raises RegimeComputeError, which the
orchestrator logs as evt:REGIME_COMPUTE_FAIL. On success the result carries the REGIME_CLASSIFIED
event the orchestrator logs (q5_logs).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

from ..config import registry
from . import indicators, taxonomy
from .taxonomy import DirectionalState, Regime, VolatilityState

_ADX_MIN_CANDLES = 28  # Image4: minimum 28 daily candles for ADX(14).

# CIATS-owned seed defaults (value home TB00000 sec 8); per-module overridable at the call site.
_ADX_THRESHOLD = registry.value("adx_threshold")
_ATR_PCT_THRESH = registry.value("atr_percentile_thresh")
_ATR_WINDOW = registry.value("atr_rolling_window")
_EMA_SHORT, _EMA_LONG = registry.value("htf_ema_periods")  # (20, 50)


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


class RegimeComputeError(ValueError):
    """Insufficient / malformed daily series - the orchestrator logs evt:REGIME_COMPUTE_FAIL."""


@dataclass(frozen=True)
class DailyBar:
    """One committed daily OHLC candle (REST GetOHLCData, interval=1440). Decimal on receipt."""

    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal

    @classmethod
    def of(cls, open_: object, high: object, low: object, close: object, volume: object = 0) -> "DailyBar":
        return cls(_dec(open_), _dec(high), _dec(low), _dec(close), _dec(volume))


@dataclass(frozen=True)
class RegimeClassified:
    """evt:REGIME_CLASSIFIED [INFO] (Image4 q5_logs) - the per-pair classification result with
    the classifier inputs (selected regime token + ADX / ATR percentile / EMA20 / EMA50)."""

    symbol: str
    regime: str
    adx: Decimal
    ema20: Decimal
    ema50: Decimal
    atr_14: Decimal
    atr_percentile: Decimal
    code: str = "REGIME_CLASSIFIED"


@dataclass(frozen=True)
class RegimeClassification:
    """The classification of one pair for the day: the regime token + its policy profile + the
    component indicator values + the REGIME_CLASSIFIED event to log."""

    symbol: str
    regime: Regime
    directional: DirectionalState
    volatility: VolatilityState
    adx: Decimal
    ema20: Decimal
    ema50: Decimal
    atr_14: Decimal
    atr_percentile: Decimal

    @property
    def profile(self) -> taxonomy.RegimeProfile:
        return taxonomy.profile(self.regime)

    @property
    def classified_event(self) -> RegimeClassified:
        return RegimeClassified(
            symbol=self.symbol,
            regime=self.regime.value,
            adx=self.adx,
            ema20=self.ema20,
            ema50=self.ema50,
            atr_14=self.atr_14,
            atr_percentile=self.atr_percentile,
        )


def classify_from_indicators(
    symbol: str,
    adx: object,
    ema20: object,
    ema50: object,
    atr_14: object,
    atr_percentile: object,
    *,
    adx_threshold: object = _ADX_THRESHOLD,
    atr_percentile_thresh: object = _ATR_PCT_THRESH,
) -> RegimeClassification:
    """Classify from already-computed indicator values (the pure cell decision; Image4)."""
    adx_d, ema20_d, ema50_d = _dec(adx), _dec(ema20), _dec(ema50)
    atr_d, pct_d = _dec(atr_14), _dec(atr_percentile)
    directional = taxonomy.classify_directional(adx_d, ema20_d, ema50_d, adx_threshold)
    volatility = taxonomy.classify_volatility(pct_d, atr_percentile_thresh)
    regime = taxonomy.classify(directional, volatility)
    return RegimeClassification(
        symbol=symbol,
        regime=regime,
        directional=directional,
        volatility=volatility,
        adx=adx_d,
        ema20=ema20_d,
        ema50=ema50_d,
        atr_14=atr_d,
        atr_percentile=pct_d,
    )


def compute_regime(
    symbol: str,
    bars: Sequence[DailyBar],
    *,
    exclude_forming: bool = True,
    adx_threshold: object = _ADX_THRESHOLD,
    atr_percentile_thresh: object = _ATR_PCT_THRESH,
    atr_rolling_window: object = _ATR_WINDOW,
    ema_short: object = _EMA_SHORT,
    ema_long: object = _EMA_LONG,
) -> RegimeClassification:
    """Classify one pair from its daily OHLC series (Image4 q1_do).

    bars is the raw REST GetOHLCData daily series (interval=1440). With exclude_forming=True
    (default) the last bar (response[-1], the uncommitted forming candle) is dropped per
    ar:AR-017 before any computation. Requires >= 28 committed candles, else RegimeComputeError.
    """
    committed = list(bars[:-1] if exclude_forming else bars)
    # The binding floor across all indicators: ADX needs 28, EMA50 needs ema_long (50).
    min_required = max(_ADX_MIN_CANDLES, int(ema_long))
    if len(committed) < min_required:
        raise RegimeComputeError(
            f"{symbol}: {len(committed)} committed daily candles < {min_required} required"
        )

    highs = [b.high for b in committed]
    lows = [b.low for b in committed]
    closes = [b.close for b in committed]

    adx = indicators.adx_14(highs, lows, closes)
    ema20 = indicators.ema(closes, int(ema_short))
    ema50 = indicators.ema(closes, int(ema_long))
    atr_series = indicators.atr_14_series(highs, lows, closes)
    atr_14 = atr_series[-1]
    atr_pct = indicators.atr_percentile_rank(atr_series, int(atr_rolling_window))

    return classify_from_indicators(
        symbol,
        adx,
        ema20,
        ema50,
        atr_14,
        atr_pct,
        adx_threshold=adx_threshold,
        atr_percentile_thresh=atr_percentile_thresh,
    )
