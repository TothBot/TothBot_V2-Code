"""ar:AR-045 OHLC candle-close detection - the in-progress vs closed candle boundary.

Source: 0500000 dv1_250 ar:AR-045 ("WS OHLC fires on EVERY trade event within the candle period,
not only on candle close. WS Manager MUST track per symbol last_interval_begin + last_complete_
candle. On each ohlc message: if msg.interval_begin == last_interval_begin -> update the in-progress
candle, DO NOT fire; else the candle for last_interval_begin is NOW CLOSED -> fire the pipeline with
last_complete_candle, then last_complete_candle = current, last_interval_begin = msg.interval_begin.
Initialize last_interval_begin from the startup GetOHLCData(5) response[-2] committed candle. Same
logic for ohlc(60) with a separate last_interval_begin_60.").

Kraken WS v2 sends the CUMULATIVE OHLC of the in-progress candle on each update (A-16: parse
interval_begin, the candle START, ignore the deprecated timestamp). So the final pre-roll snapshot
of a candle IS its closed OHLC; this detector emits exactly that committed candle the moment a newer
interval_begin arrives. PURE + per-symbol: no I/O, no clock. The 5m detector drives the universe
sweep (the OHLC_5m_System_Clock tick); the 1H detector drives the HTF cache + wm.on_htf_ohlc_close.

Decimal-only (ar:AR-047): every WS price/qty is converted to Decimal(str(value)) on receipt, NEVER
Decimal(float). interval_begin is normalized to an integer Unix-second key so the REST warm-up
seed (RestOhlcBar.time, Unix seconds) and the live WS frame (RFC3339 interval_begin string) compare
on the SAME axis.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from .candle import parse_interval_begin


def to_interval_unix(value: object) -> int:
    """Normalize a candle interval_begin to an integer Unix-second key.

    Accepts the REST seed form (int/float Unix seconds, RestOhlcBar.time) and the live WS form
    (an RFC3339 string or a datetime). This single key axis lets the warm-up trackers and the WS
    frames compare directly (ar:AR-045 init from GetOHLCData vs. the live ohlc stream)."""
    if isinstance(value, bool):  # guard: bool is an int subclass
        raise TypeError("interval_begin cannot be a bool")
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, datetime):
        return int(parse_interval_begin(value).timestamp())
    return int(parse_interval_begin(str(value)).timestamp())


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


@dataclass(frozen=True)
class CommittedCandle:
    """One committed OHLC candle (Decimal, ar:AR-047). interval_begin is the integer Unix-second
    candle-start key (ar:AR-045 / A-16). This is the unit the sweep feeds to LiveIndicators.update
    + the pipeline (entry_ref_price = close, the G5 signal candle = open/high/low/close)."""

    symbol: str
    interval_begin: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


def committed_candle_from_bar(symbol: str, bar: object) -> CommittedCandle:
    """Build a CommittedCandle from a REST warm-up bar (RestOhlcBar: time/open/high/low/close/
    volume, no symbol). Used to seed the detector's last_complete_candle from the warm-up's last
    committed candle so the first ar:AR-045 fire carries a symbol-bearing candle to the sweep."""
    return CommittedCandle(
        symbol=symbol,
        interval_begin=to_interval_unix(bar.time),
        open=_dec(bar.open),
        high=_dec(bar.high),
        low=_dec(bar.low),
        close=_dec(bar.close),
        volume=_dec(bar.volume),
    )


def committed_candle_from_frame(elem: dict) -> CommittedCandle:
    """Parse one Kraken WS v2 ohlc data element into a Decimal CommittedCandle (ar:AR-047 / A-16).

    `elem` is one entry of a frame's data[] list (symbol/interval_begin/open/high/low/close/volume).
    Every numeric value is Decimal(str(value)) on receipt; interval_begin is normalized to the
    integer Unix-second key."""
    return CommittedCandle(
        symbol=elem["symbol"],
        interval_begin=to_interval_unix(elem["interval_begin"]),
        open=_dec(elem["open"]),
        high=_dec(elem["high"]),
        low=_dec(elem["low"]),
        close=_dec(elem["close"]),
        volume=_dec(elem["volume"]),
    )


class CandleCloseDetector:
    """Per-symbol ar:AR-045 in-progress vs closed candle boundary detector.

    Seed last_interval_begin + last_complete_candle from the warm-up's last committed candle
    (GetOHLCData response[-2]); then feed every live ohlc update through observe(). observe returns
    the just-CLOSED committed candle on a roll (a newer interval_begin), else None (still in
    progress - the in-progress snapshot is retained as the latest complete candle). One detector
    per (symbol, interval): the 5m drives the sweep, the 1H drives the HTF cache."""

    __slots__ = ("symbol", "_begin", "_complete")

    def __init__(self, symbol: str, *, last_interval_begin: object, last_complete_candle: object) -> None:
        self.symbol = symbol
        self._begin = to_interval_unix(last_interval_begin)
        self._complete = last_complete_candle

    @property
    def last_interval_begin(self) -> int:
        return self._begin

    @property
    def last_complete_candle(self) -> object:
        """The latest snapshot of the candle currently being accumulated (ar:AR-045)."""
        return self._complete

    def observe(self, candle: CommittedCandle) -> CommittedCandle | None:
        """Apply one ohlc update (ar:AR-045). Same interval_begin -> update the in-progress
        snapshot, return None (DO NOT fire). A newer interval_begin -> the prior candle CLOSED:
        return it (the sweep/HTF fires on it), then begin accumulating the new candle."""
        if candle.interval_begin == self._begin:
            self._complete = candle  # newer cumulative snapshot of the same (in-progress) candle
            return None
        closed = self._complete
        self._complete = candle
        self._begin = candle.interval_begin
        return closed
