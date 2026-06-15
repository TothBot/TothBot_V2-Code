"""SSS Signal Engine - the three-factor momentum signal model (mod:Signal_Pipeline).

Source: 0500000 dv1_242 sec 3 Image2 R14 mod:Signal_Pipeline (element sss_engine) q1_do +
ar:AR-067 (direction-symmetric three-factor momentum scoring on committed 5-minute candles) +
rule:HR-WM-018 / ar:AR-076 (RSI(14) Wilder math) + rule:HR-SSS-004 (RSI division guard) +
rule:HR-SP-008 / ar:AR-047 (Decimal-only). Pure units: a committed close/volume series in, the
indicator values + the SSS verdict out. No WS state, no incremental per-candle dict (that is the
WS-Manager's AR-016/AR-075 live concern); the canonical math is identical and computed here in
batch over the committed series.

THE THREE FACTORS (all three simultaneously true at the committed candle close = PASS; two-of-
three is a FAIL; no partial credit, no weighted score - q1_do):
  SC-SSS-1  RSI(14) strictly inside the side's momentum zone.
              long : param:rsi_long_low  < RSI < param:rsi_long_high   (30 .. 50)
              short: param:rsi_short_high < RSI < param:rsi_short_low   (50 .. 70, the mirror)
  SC-SSS-2  the EMA9/EMA21 crossover STATE that must hold at candle close (NOT the event).
              long : EMA9 > EMA21        short: EMA9 < EMA21   (direction-symmetric)
  SC-SSS-3  current_candle_volume > VolumeMA20 * param:volume_sss_threshold (direction-independent).

RSI(14) Wilder (ar:AR-076; standard EMA alpha 2/15 MUST NOT be used - HR-WM-018):
  gain = max(0, close - prev_close); loss = max(0, prev_close - close).
  SEED over the first 14 deltas: avg_gain = sum(gains)/14, avg_loss = sum(losses)/14.
  INCREMENTAL: avg_gain = (prev_avg_gain*13 + gain)/14; avg_loss = (prev_avg_loss*13 + loss)/14.
  RS = avg_gain/avg_loss; RSI = 100 - 100/(1 + RS).
  DIVISION GUARD (HR-SSS-004): avg_loss == 0 and avg_gain > 0 -> RSI = 100; both 0 -> RSI = 50.

EMA9 / EMA21 are STANDARD EMA (alpha 2/10, 2/22) - indicators.ema (alpha = 2/(period+1)) is the
same recurrence, reused here. VolumeMA20 is the running-SMA approximation per q1_do (seed SMA over
the first 20, then ((prev*19) + current)/20 - NOT a true rolling window; CIATS may revisit after
the 200-trade floor). Minimum committed candles to evaluate = sss_ema_long (21, EMA21-bound);
GetOHLCData(interval=5) returns 700+ committed - adequate.

SHORT-SIDE NOTE (carry-forward, for Bill confirmation): Image2 writes SC-SSS-2 generically as
"EMA9 > EMA21" and the short RSI bounds as the registry "mirror" of the long bounds; it states
the engine is "direction-symmetric ... short-side per mod:Short_Module" but does not spell the
short inequalities literally. This module implements the only coherent symmetric reading - short
SC-SSS-2 = EMA9 < EMA21, and the short RSI zone = the open interval between the two short bounds
(min..max, i.e. 50..70). No value is invented (the bounds are the registry seeds); only the
inequality direction is completed from "direction-symmetric". Flagged for ratification.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum

from ..config import registry
from .indicators import ema  # standard EMA (alpha = 2/(period+1)) - reused for EMA9 / EMA21

_RSI_PERIOD = 14

# CIATS-owned SSS seeds (value home TB00000 sec 8); per-module overridable at the call site.
_RSI_LONG_LOW = registry.value("rsi_long_low")
_RSI_LONG_HIGH = registry.value("rsi_long_high")
_RSI_SHORT_LOW = registry.value("rsi_short_low")
_RSI_SHORT_HIGH = registry.value("rsi_short_high")
_EMA_SHORT = registry.value("sss_ema_short")          # 9
_EMA_LONG = registry.value("sss_ema_long")            # 21
_VOLUME_THRESHOLD = registry.value("volume_sss_threshold")  # 1.0x MA(20)
_VOLUME_MA_PERIOD = 20


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


class SignalSide(Enum):
    LONG = "long"
    SHORT = "short"


class SssComputeError(ValueError):
    """Insufficient committed candles to seed the SSS indicators (< sss_ema_long)."""


def rsi_state(closes: Sequence[object], period: int = _RSI_PERIOD) -> tuple[Decimal, Decimal]:
    """The Wilder running (avg_gain, avg_loss) after the full committed close series (ar:AR-076).

    This is the incremental RSI state the WS-Manager maintains per ar:AR-075 (the per-symbol
    state dict's rsi_14_avg_gain / rsi_14_avg_loss): seeded over the first `period` deltas, then
    stepped one delta at a time. Computed here in batch over the warm-up series; the live
    maintainer seeds from this and steps with the identical recurrence (the two are bit-identical
    by construction). Needs at least period + 1 closes (period deltas to seed)."""
    c = [_dec(x) for x in closes]
    if len(c) < period + 1:
        raise SssComputeError(f"RSI({period}) needs at least {period + 1} closes, got {len(c)}")

    gains = [max(Decimal(0), c[i] - c[i - 1]) for i in range(1, len(c))]
    losses = [max(Decimal(0), c[i - 1] - c[i]) for i in range(1, len(c))]

    avg_gain = sum(gains[:period], Decimal(0)) / period
    avg_loss = sum(losses[:period], Decimal(0)) / period
    for g, l in zip(gains[period:], losses[period:]):
        avg_gain = step_rsi_state(avg_gain, g, period)
        avg_loss = step_rsi_state(avg_loss, l, period)
    return avg_gain, avg_loss


def step_rsi_state(prev_avg: object, delta: object, period: int = _RSI_PERIOD) -> Decimal:
    """One Wilder SMMA step of a running avg_gain/avg_loss (ar:AR-076): O(1) per candle close.

    avg = (prev_avg * (period - 1) + delta) / period. The single recurrence used by both the
    batch seed loop above and the live per-candle maintainer (no divergence)."""
    return (_dec(prev_avg) * (period - 1) + _dec(delta)) / period


def rsi_from_state(avg_gain: object, avg_loss: object) -> Decimal:
    """indicator:RSI from the running Wilder (avg_gain, avg_loss) with the HR-SSS-004 guard.

    avg_loss == 0 and avg_gain > 0 -> RSI 100; both 0 (flat) -> RSI 50; else 100 - 100/(1+RS)."""
    ag, al = _dec(avg_gain), _dec(avg_loss)
    if al == 0:
        return Decimal(100) if ag > 0 else Decimal(50)
    rs = ag / al
    return Decimal(100) - Decimal(100) / (Decimal(1) + rs)


def rsi_14(closes: Sequence[object], period: int = _RSI_PERIOD) -> Decimal:
    """indicator:RSI - the final Wilder RSI(period) over a committed close series (ar:AR-076).

    Needs at least period + 1 closes (period deltas to seed). Applies the HR-SSS-004 division
    guard for a zero average loss (RSI 100 on pure gains, 50 on a flat series)."""
    return rsi_from_state(*rsi_state(closes, period))


def volume_ma_20(volumes: Sequence[object], period: int = _VOLUME_MA_PERIOD) -> Decimal:
    """indicator:VolumeMA20 - the running-SMA approximation per q1_do (NOT a true rolling window).

    SEED = SMA over the first `period` volumes; thereafter ma = (ma*(period-1) + volume)/period.
    """
    v = [_dec(x) for x in volumes]
    if len(v) < period:
        raise SssComputeError(f"VolumeMA{period} needs at least {period} volumes, got {len(v)}")
    ma = sum(v[:period], Decimal(0)) / period
    for x in v[period:]:
        ma = (ma * (period - 1) + x) / period
    return ma


def _rsi_in_zone(rsi: Decimal, bound_a: object, bound_b: object) -> bool:
    """SC-SSS-1: RSI strictly inside the side's open zone (min(bound) < RSI < max(bound)).

    The bound ordering is direction-agnostic: long (30, 50) and short (70, 50) both yield the
    correct open interval, so the same predicate serves both sides (the short mirror)."""
    lo, hi = sorted((_dec(bound_a), _dec(bound_b)))
    return lo < rsi < hi


def three_factor_pass(
    rsi: object,
    ema9: object,
    ema21: object,
    volume: object,
    volume_ma20: object,
    *,
    side: SignalSide,
    rsi_low: object,
    rsi_high: object,
    volume_threshold: object = _VOLUME_THRESHOLD,
) -> tuple[bool, bool, bool]:
    """The three SC-SSS factor booleans (sc_sss_1, sc_sss_2, sc_sss_3) for the given side.

    PASS is the AND of the three (caller's responsibility). Direction-symmetric: SC-SSS-2 is
    EMA9 > EMA21 for long, EMA9 < EMA21 for short; SC-SSS-3 (volume) is side-independent.
    """
    rsi_d, e9, e21 = _dec(rsi), _dec(ema9), _dec(ema21)
    vol, vma = _dec(volume), _dec(volume_ma20)

    sc1 = _rsi_in_zone(rsi_d, rsi_low, rsi_high)
    sc2 = e9 > e21 if side is SignalSide.LONG else e9 < e21
    sc3 = vol > vma * _dec(volume_threshold)
    return sc1, sc2, sc3


@dataclass(frozen=True)
class SssVerdict:
    """The SSS evaluation of one pair for one side (Image2 q5_logs fields). passed is the
    three-factor AND; sc_sss is the (1,2,3) factor tuple; sss_score is the count of factors
    met (audit only - the PASS rule is strict AND, no weighting). event_type is the q5_logs
    SIGNAL_PASS / SIGNAL_REJECTED decision."""

    symbol: str
    side: SignalSide
    rsi_14: Decimal
    ema9: Decimal
    ema21: Decimal
    ema_cross: bool          # SC-SSS-2 state
    volume: Decimal
    volume_ma20: Decimal
    volume_vs_ma20: bool     # SC-SSS-3 state
    sc_sss: tuple[bool, bool, bool]
    passed: bool

    @property
    def sss_score(self) -> int:
        return sum(self.sc_sss)

    @property
    def event_type(self) -> str:
        return "SIGNAL_PASS" if self.passed else "SIGNAL_REJECTED"

    @property
    def signal_params(self) -> dict:
        """evt:TRADE_CLOSE field (19) signal_params - the SSS output dict the diagram names the
        canonical schema source (0500000 schema_fields_canonical: {rsi_14, ema_9, ema_21,
        volume_ratio, sss_pass}; the SSS q5_logs adds `side` for per-module routing). These are the
        PER-TRADE SSS indicator LEVELS the entry was taken under - the per-trade parameter-level
        series the CIATS Spearman PLAN-candidate gate reads. volume_ratio is the SC-SSS-3 ratio
        current_volume / VolumeMA20 (the level the volume_sss_threshold gates; 0 when MA20 is 0)."""
        ratio = self.volume / self.volume_ma20 if self.volume_ma20 != 0 else Decimal(0)
        return {
            "rsi_14": self.rsi_14,
            "ema_9": self.ema9,
            "ema_21": self.ema21,
            "volume_ratio": ratio,
            "sss_pass": self.passed,
            "side": self.side.value,
        }


def _resolve_rsi_bounds(
    side: SignalSide, rsi_low: object | None, rsi_high: object | None
) -> tuple[object, object]:
    """Default rsi_low / rsi_high to the side's registry seeds (long 30/50, short 70/50)."""
    if rsi_low is not None and rsi_high is not None:
        return rsi_low, rsi_high
    if side is SignalSide.LONG:
        return _RSI_LONG_LOW, _RSI_LONG_HIGH
    return _RSI_SHORT_LOW, _RSI_SHORT_HIGH


def sss_verdict_from_indicators(
    symbol: str,
    *,
    side: SignalSide,
    rsi: object,
    ema9: object,
    ema21: object,
    volume: object,
    volume_ma20: object,
    rsi_low: object | None = None,
    rsi_high: object | None = None,
    volume_threshold: object = _VOLUME_THRESHOLD,
) -> SssVerdict:
    """Build the SssVerdict from already-computed indicator scalars - the LIVE path (ar:AR-075).

    The WS-Manager maintains rsi_14 / ema_9 / ema_21 / volume_ma_20 as incremental running
    values per ar:AR-016/AR-075 and serves them to the gates WITHOUT recomputation (the Pre-Step-2
    pre-computation cache read). This applies the identical three-factor PASS rule the batch
    evaluate_sss uses, so the live verdict is bit-identical to the batch verdict over the same
    committed history. `volume` is the current (signal) candle's volume vs the running MA(20)."""
    rsi_low, rsi_high = _resolve_rsi_bounds(side, rsi_low, rsi_high)
    sc1, sc2, sc3 = three_factor_pass(
        rsi, ema9, ema21, volume, volume_ma20,
        side=side, rsi_low=rsi_low, rsi_high=rsi_high, volume_threshold=volume_threshold,
    )
    return SssVerdict(
        symbol=symbol,
        side=side,
        rsi_14=_dec(rsi),
        ema9=_dec(ema9),
        ema21=_dec(ema21),
        ema_cross=sc2,
        volume=_dec(volume),
        volume_ma20=_dec(volume_ma20),
        volume_vs_ma20=sc3,
        sc_sss=(sc1, sc2, sc3),
        passed=sc1 and sc2 and sc3,
    )


def evaluate_sss(
    symbol: str,
    closes: Sequence[object],
    volumes: Sequence[object],
    *,
    side: SignalSide,
    rsi_low: object | None = None,
    rsi_high: object | None = None,
    ema_short: object = _EMA_SHORT,
    ema_long: object = _EMA_LONG,
    volume_threshold: object = _VOLUME_THRESHOLD,
) -> SssVerdict:
    """Run the SSS Signal Engine on one pair's committed 5-minute candle series for one side.

    The BATCH path: computes RSI(14), EMA9, EMA21, VolumeMA20 over the committed closes/volumes
    and applies the three-factor PASS rule via sss_verdict_from_indicators (one verdict path).
    rsi_low / rsi_high default to the side's registry seeds (long 30/50, short 70/50). Requires
    >= ema_long (21) committed candles, else SssComputeError. The live maintainer
    (LiveIndicators) seeds from this same math and steps incrementally per ar:AR-016/AR-075.
    """
    n = len(closes)
    if n < int(ema_long) or len(volumes) < _VOLUME_MA_PERIOD:
        raise SssComputeError(
            f"{symbol}: {n} closes / {len(volumes)} volumes < seeding minimum "
            f"({int(ema_long)} closes, {_VOLUME_MA_PERIOD} volumes)"
        )

    return sss_verdict_from_indicators(
        symbol,
        side=side,
        rsi=rsi_14(closes),
        ema9=ema(closes, int(ema_short)),
        ema21=ema(closes, int(ema_long)),
        volume=_dec(volumes[-1]),
        volume_ma20=volume_ma_20(volumes),
        rsi_low=rsi_low,
        rsi_high=rsi_high,
        volume_threshold=volume_threshold,
    )
