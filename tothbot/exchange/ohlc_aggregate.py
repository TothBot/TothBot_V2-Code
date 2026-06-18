"""Derive the live 1H (ohlc_60m) candle by folding the ohlc_5m stream (TB00768, Opt 5).

Kraken WS v2 permits only ONE ohlc interval per symbol per connection: a second
subscribe (ohlc 5m + ohlc 60m on the same shard connection, as Image1 specified) is
REFUSED ("Already subscribed to one ohlc interval on this symbol"). The live paper run
exposed this - the 1H HtfCache, REST-seeded at warm-up, would FREEZE because no live WS
60m close ever arrives to advance it, and the EC-L1A-001 1H reversal exit could never
fire (a structural FALSE NEGATIVE).

A 1H candle IS the exact fold of its twelve contiguous 5m candles - the twelve 5m
sub-windows partition the hour, so:

    open_1h  = open  of the [:00,:05) candle      (the earliest interval_begin)
    close_1h = close of the [:55,:00) candle      (the latest interval_begin)
    high_1h  = max(high_i)   low_1h = min(low_i)   volume_1h = sum(volume_i)

This is information-theoretically LOSSLESS (it equals Kraken's own 1H candle whenever
all twelve 5m candles are present), zero added latency (the 1H close coincides with the
:55->:00 5m close), and zero incremental connection / rate-limit cost. The clock shard
already carries every 5m close, so the 1H feed inherits the 5m feed's liveness for free.

COMPLETENESS GATE (drives the false-positive rate to zero, the HR-WM-012 "never act on a
partial" principle applied to the fold): a synthetic 1H candle is emitted ONLY for an
hour whose bucket holds all twelve interval_begin-contiguous 5m candles. An incomplete
bucket is NOT folded into a (corrupt) candle:

  - a bucket that BEGAN at the hour boundary but is missing slots = a mid-session gap
    (a reconnect dropped 5m candles) -> Htf1hGap (the caller self-heals via one targeted
    REST GetOHLCData(interval=60); until then the HtfCache simply misses one 1H step and
    resumes on the next complete hour - bounded, never frozen);
  - a bucket that began MID-hour = the expected startup / post-gap partial (the warm-up
    REST seed already covers continuity up to the last complete pre-startup hour) -> None,
    discarded silently.

PURE: no I/O, no clock, Decimal-only (ar:AR-047). The caller (live_driver) folds each
closed 5m CommittedCandle and routes a Closed1H to the same HtfCache-advance + EC-L1A-001
path the WS 60m frame used to drive.

TB00787 (the validated long-only strategy) adds a SECOND fold stage one timeframe up:
``fold_hour`` folds the twenty-four contiguous hour-aligned 1H closes that partition a UTC
day into one exact 24h DECISION candle (the same lossless open/close/high/low/volume fold,
the same completeness gate, the same gap-or-discard rollover and self-heal, all at day
granularity). The 24h close is the decision bar the forthcoming long-only entry/exit compute
on. Each Closed1H ``fold`` emits is the input to ``fold_hour``; the 1H path is untouched.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .candle_close import CommittedCandle

# Seconds per 1H bucket and per 5m candle; twelve 5m candles compose one complete hour.
_HOUR_SECONDS = 3600
_FIVE_MIN_SECONDS = 300
_CANDLES_PER_HOUR = _HOUR_SECONDS // _FIVE_MIN_SECONDS  # 12

# Seconds per UTC day; twenty-four contiguous 1H candles compose one complete day. The
# Unix epoch is itself 00:00 UTC, so floor-dividing by _DAY_SECONDS lands the day boundary
# exactly on midnight UTC - the same boundary Kraken's native 1440 daily candle uses.
_DAY_SECONDS = 86400
_HOURS_PER_DAY = _DAY_SECONDS // _HOUR_SECONDS  # 24


def hour_begin_of(interval_begin: int) -> int:
    """The Unix-second start of the 1H bucket a 5m candle's interval_begin falls in."""
    return (interval_begin // _HOUR_SECONDS) * _HOUR_SECONDS


def day_begin_of(interval_begin: int) -> int:
    """The Unix-second start of the UTC day a 1H candle's interval_begin falls in (00:00 UTC)."""
    return (interval_begin // _DAY_SECONDS) * _DAY_SECONDS


@dataclass(frozen=True)
class Closed1H:
    """A complete, exact 1H candle folded from twelve contiguous 5m candles - fed to the
    same HtfCache-advance + EC-L1A-001 path the WS ohlc_60m frame used to drive."""

    candle: CommittedCandle


@dataclass(frozen=True)
class Htf1hGap:
    """evt:HTF_1H_GAP [WARNING] {symbol, hour_begin} - an hour-aligned 1H bucket closed
    with fewer than twelve 5m candles (a mid-session reconnect dropped 5m closes). The 1H
    fold is suppressed (never emit a corrupt candle); the caller self-heals the HtfCache
    from one targeted REST GetOHLCData(interval=60). Surfaced, never silently dropped."""

    symbol: str
    hour_begin: int
    code: str = field(default="HTF_1H_GAP", init=False)


@dataclass(frozen=True)
class Htf1hHealed:
    """evt:HTF_1H_HEAL [INFO] {symbol, hour_begin} - a gapped pair's 1H HtfCache was RE-SEEDED from
    one targeted REST GetOHLCData(interval=60) after a Htf1hGap (the auto-refetch that closes the
    self-heal). The REST 1H series is authoritative (it already folds in the dropped hour), so the
    re-seed restores the exact EMA(20)/EMA(50) the frozen cache would have missed - and the caller
    drives the EC-L1A-001 1H reversal once on the fresh EMAs (a reversal hidden by the gap still
    fires). A REST failure leaves the cache untouched (it resumes on the next complete hour, bounded
    - the Htf1hGap already surfaced the miss), so HTF_1H_HEAL marks only a SUCCESSFUL refetch."""

    symbol: str
    hour_begin: int
    code: str = field(default="HTF_1H_HEAL", init=False)


@dataclass(frozen=True)
class Closed24H:
    """A complete, exact 24h DECISION candle folded from twenty-four contiguous hour-aligned
    1H candles (TB00787, the validated long-only strategy). The 24h close is the DECISION bar:
    the forthcoming long-only entry (EMA12/26 bullish cross) and exit (EMA12/26 reversal OR a
    wide ATR disaster stop) compute on this series. EAGER-emitted the instant the 00:00-UTC 1H
    close completes the UTC day, so the daily decision fires on the same boundary as Kraken's
    native 1440 candle, zero rollover lag."""

    candle: CommittedCandle


@dataclass(frozen=True)
class Htf24hGap:
    """evt:HTF_24H_GAP [WARNING] {symbol, day_begin} - a day-aligned 24h bucket closed with
    fewer than twenty-four 1H candles (a 1H step the TB00769 self-heal could not recover). The
    24h fold is suppressed (never emit a corrupt decision candle); the caller self-heals from one
    targeted REST GetOHLCData(interval=1440) (the exact mirror of the 1H heal). Surfaced, never
    silently dropped."""

    symbol: str
    day_begin: int
    code: str = field(default="HTF_24H_GAP", init=False)


@dataclass(frozen=True)
class Htf24hHealed:
    """evt:HTF_24H_HEAL [INFO] {symbol, day_begin} - a gapped pair's 24h decision series was
    RE-SEEDED from one targeted REST GetOHLCData(interval=1440) after a Htf24hGap. The REST 1440
    daily series is authoritative (it already folds in the missed hours), so the re-seed restores
    the exact decision bar + indicators the gapped fold would have missed. A REST failure leaves
    the series to resume on the next complete day (bounded), so HTF_24H_HEAL marks only a
    SUCCESSFUL refetch."""

    symbol: str
    day_begin: int
    code: str = field(default="HTF_24H_HEAL", init=False)


@dataclass
class _Bucket:
    """The accumulating 5m candles of one in-progress hour for one symbol."""

    symbol: str
    hour_begin: int
    candles: list[CommittedCandle] = field(default_factory=list)
    emitted: bool = False  # the complete 1H candle was already eager-emitted

    def add(self, candle: CommittedCandle) -> None:
        self.candles.append(candle)

    def is_complete(self) -> bool:
        """All twelve interval_begin-contiguous slots present: hour-aligned, count 12,
        spanning exactly [hour, hour+3300]. Distinct begins + that span => every slot."""
        begins = {c.interval_begin for c in self.candles}
        return (
            len(begins) == _CANDLES_PER_HOUR
            and min(begins) == self.hour_begin
            and max(begins) == self.hour_begin + (_CANDLES_PER_HOUR - 1) * _FIVE_MIN_SECONDS
        )

    def _began_hour_aligned(self) -> bool:
        """The bucket's earliest 5m candle sits on the hour boundary (so a shortfall is a
        genuine mid-session gap, not the expected startup / post-gap partial hour)."""
        return bool(self.candles) and min(c.interval_begin for c in self.candles) == self.hour_begin

    def fold_1h(self) -> CommittedCandle:
        """The exact 1H candle: open of the earliest slot, close of the latest, max high,
        min low, summed volume (lossless - the twelve 5m sub-windows partition the hour)."""
        ordered = sorted(self.candles, key=lambda c: c.interval_begin)
        return CommittedCandle(
            symbol=self.symbol,
            interval_begin=self.hour_begin,
            open=ordered[0].open,
            high=max(c.high for c in ordered),
            low=min(c.low for c in ordered),
            close=ordered[-1].close,
            volume=sum((c.volume for c in ordered), start=type(ordered[0].volume)(0)),
        )

    def gap_or_none(self) -> "Htf1hGap | None":
        """On an UNEMITTED rollover: an hour-aligned shortfall -> Htf1hGap (a mid-session
        gap to self-heal); a mid-hour partial -> None (the expected startup/post-gap part)."""
        return Htf1hGap(self.symbol, self.hour_begin) if self._began_hour_aligned() else None


@dataclass
class _DayBucket:
    """The accumulating 1H candles of one in-progress UTC day for one symbol (TB00787).

    Structural mirror of _Bucket one timeframe up: twenty-four hour-aligned 1H closes partition a
    UTC day exactly as twelve 5m closes partition an hour, so the completeness gate, the
    began-day-aligned test, the lossless fold, and the gap-or-discard rollover are identical with
    day-level constants. Twenty-four contiguous 1H candles is information-theoretically lossless -
    it equals Kraken's own 1440 daily candle whenever all twenty-four are present."""

    symbol: str
    day_begin: int
    candles: list[CommittedCandle] = field(default_factory=list)
    emitted: bool = False  # the complete 24h candle was already eager-emitted

    def add(self, candle: CommittedCandle) -> None:
        self.candles.append(candle)

    def is_complete(self) -> bool:
        """All twenty-four interval_begin-contiguous 1H slots present: day-aligned, count 24,
        spanning exactly [day, day+23h]. Distinct begins + that span => every slot."""
        begins = {c.interval_begin for c in self.candles}
        return (
            len(begins) == _HOURS_PER_DAY
            and min(begins) == self.day_begin
            and max(begins) == self.day_begin + (_HOURS_PER_DAY - 1) * _HOUR_SECONDS
        )

    def _began_day_aligned(self) -> bool:
        """The bucket's earliest 1H candle sits on the 00:00-UTC day boundary (so a shortfall is a
        genuine mid-day gap, not the expected startup / post-gap partial day)."""
        return bool(self.candles) and min(c.interval_begin for c in self.candles) == self.day_begin

    def fold_24h(self) -> CommittedCandle:
        """The exact 24h decision candle: open of the earliest 1H, close of the latest, max high,
        min low, summed volume (lossless - the twenty-four 1H sub-windows partition the day)."""
        ordered = sorted(self.candles, key=lambda c: c.interval_begin)
        return CommittedCandle(
            symbol=self.symbol,
            interval_begin=self.day_begin,
            open=ordered[0].open,
            high=max(c.high for c in ordered),
            low=min(c.low for c in ordered),
            close=ordered[-1].close,
            volume=sum((c.volume for c in ordered), start=type(ordered[0].volume)(0)),
        )

    def gap_or_none(self) -> "Htf24hGap | None":
        """On an UNEMITTED rollover: a day-aligned shortfall -> Htf24hGap (a mid-day gap to
        self-heal); a mid-day partial -> None (the expected startup/post-gap part)."""
        return Htf24hGap(self.symbol, self.day_begin) if self._began_day_aligned() else None


class OhlcAggregator:
    """Folds the per-symbol ohlc_5m close stream into exact 1H candles (Opt 5).

    Drive ``fold(closed_5m)`` with each CLOSED 5m CommittedCandle (the same candle the 5m
    detector hands the sweep), in interval_begin order per symbol. A complete hour is
    EAGER-emitted the instant its twelfth contiguous 5m candle closes (the [:55,:00) close,
    which coincides with the native 1H close - NO rollover lag, so the EC-L1A-001 1H
    reversal exit fires on the same boundary the WS 60m feed used to). ``fold`` returns:
      - Closed1H : the exact 1H candle (on the twelfth contiguous close of a fresh hour);
      - Htf1hGap : an hour-aligned bucket that rolled over short of twelve (a reconnect
                   dropped 5m closes) - the caller self-heals the HtfCache from REST;
      - None     : still accumulating, an already-emitted hour rolling over, or an
                   expected mid-hour partial (startup / post-gap) discarded."""

    def __init__(self) -> None:
        self._buckets: dict[str, _Bucket] = {}
        self._day_buckets: dict[str, _DayBucket] = {}

    def fold(self, closed_5m: CommittedCandle) -> "Closed1H | Htf1hGap | None":
        hour = hour_begin_of(closed_5m.interval_begin)
        bucket = self._buckets.get(closed_5m.symbol)
        rolled: "Htf1hGap | None" = None
        if bucket is not None and hour > bucket.hour_begin:
            # A newer hour began: a complete hour already eager-emitted on its twelfth
            # close (rolled stays None); an unemitted prior bucket -> gap / discard.
            rolled = None if bucket.emitted else bucket.gap_or_none()
            bucket = None
        elif bucket is not None and hour < bucket.hour_begin:
            # An out-of-order / stale candle for an already-closed hour: ignore it (the
            # detector feeds ascending closes, so this is a defensive no-op).
            return None
        if bucket is None:
            bucket = _Bucket(symbol=closed_5m.symbol, hour_begin=hour)
            self._buckets[closed_5m.symbol] = bucket
        bucket.add(closed_5m)
        # Eager-emit the exact 1H candle the moment the hour completes (native timing).
        if not bucket.emitted and bucket.is_complete():
            bucket.emitted = True
            return Closed1H(bucket.fold_1h())
        return rolled

    def fold_hour(self, closed_1h: CommittedCandle) -> "Closed24H | Htf24hGap | None":
        """Fold each CLOSED 1H candle into the exact 24h DECISION candle (TB00787, the second
        fold stage, one timeframe up from ``fold``). Drive it with each Closed1H ``fold`` emits
        (or a TB00769-healed 1H, so the self-heal chains through), in interval_begin order per
        symbol. The UTC day is EAGER-emitted the instant its twenty-fourth contiguous 1H candle
        closes (the 23:00->00:00 close, coinciding with the native 1440 daily close - NO rollover
        lag, so the daily long-only decision fires on the same boundary Kraken's daily candle
        uses). Returns:
          - Closed24H : the exact 24h decision candle (on the twenty-fourth contiguous close);
          - Htf24hGap : a day-aligned bucket that rolled over short of twenty-four (a 1H step the
                        TB00769 heal could not recover) - the caller self-heals from REST 1440;
          - None      : still accumulating, an already-emitted day rolling over, or an expected
                        mid-day partial (startup / post-gap) discarded.
        Structural mirror of ``fold`` with day-level constants - the 1H path is untouched."""
        day = day_begin_of(closed_1h.interval_begin)
        bucket = self._day_buckets.get(closed_1h.symbol)
        rolled: "Htf24hGap | None" = None
        if bucket is not None and day > bucket.day_begin:
            # A newer day began: a complete day already eager-emitted on its twenty-fourth
            # close (rolled stays None); an unemitted prior bucket -> gap / discard.
            rolled = None if bucket.emitted else bucket.gap_or_none()
            bucket = None
        elif bucket is not None and day < bucket.day_begin:
            # An out-of-order / stale 1H for an already-closed day: ignore it (fold feeds
            # ascending closes, so this is a defensive no-op).
            return None
        if bucket is None:
            bucket = _DayBucket(symbol=closed_1h.symbol, day_begin=day)
            self._day_buckets[closed_1h.symbol] = bucket
        bucket.add(closed_1h)
        # Eager-emit the exact 24h decision candle the moment the day completes (native timing).
        if not bucket.emitted and bucket.is_complete():
            bucket.emitted = True
            return Closed24H(bucket.fold_24h())
        return rolled
