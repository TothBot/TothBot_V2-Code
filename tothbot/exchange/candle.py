"""OHLC candle model and Kraken WS v2 OHLC parsing.

A-16 (GAP-B): the Kraken WS v2 OHLC channel deprecated the `timestamp`
field. The subscription ACK warns "timestamp is deprecated, use
interval_begin". TothBot MUST parse `interval_begin` (an RFC3339
nanosecond string) as the candle START time, and ignore `timestamp`.

Source: 0500000 dv1_240 (A-16; contract:OHLC_5m_System_Clock q5_logs
fields pair / candle_open_ts / candle_close_ts / ohlc / volume).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum

# RFC3339 fractional-seconds group. RFC3339 contains exactly one '.', marking
# fractional seconds (the date uses '-', the time uses ':'), so this is
# unambiguous. datetime supports only microseconds, so we truncate to 6 digits.
_FRACTION = re.compile(r"\.(\d+)")


def parse_interval_begin(value: str | datetime) -> datetime:
    """Parse an RFC3339 (nanosecond) candle-start string to a UTC datetime.

    Accepts a trailing 'Z' and any fractional-second precision (truncated to
    microseconds). A naive/already-parsed datetime is assumed UTC.
    """
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    m = _FRACTION.search(s)
    if m:
        s = s[: m.start()] + "." + m.group(1)[:6] + s[m.end() :]
    dt = datetime.fromisoformat(s)
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


# Back-compat internal alias (the original private name); prefer parse_interval_begin.
_parse_interval_begin = parse_interval_begin


class Interval(Enum):
    """Candle intervals TothBot subscribes to (minutes)."""

    FIVE_MIN = 5    # the system-clock interval
    ONE_HOUR = 60   # HTF confirmation interval

    @property
    def minutes(self) -> int:
        return self.value

    @property
    def delta(self) -> timedelta:
        return timedelta(minutes=self.value)


@dataclass(frozen=True)
class Candle:
    """A closed OHLC candle. `interval_begin` is the candle START (A-16)."""

    pair: str
    interval: Interval
    interval_begin: datetime  # tz-aware UTC; candle start time
    open: float
    high: float
    low: float
    close: float
    volume: float

    @property
    def open_ts(self) -> datetime:
        """Candle start time (== interval_begin, A-16)."""
        return self.interval_begin

    @property
    def close_ts(self) -> datetime:
        """Candle close time (interval_begin + interval)."""
        return self.interval_begin + self.interval.delta

    @classmethod
    def from_kraken(cls, data: dict) -> "Candle":
        """Build a Candle from one Kraken WS v2 ohlc data element.

        Parses `interval_begin` per A-16; the deprecated `timestamp` field is
        ignored. `interval` is the candle width in minutes.
        """
        return cls(
            pair=data["symbol"],
            interval=Interval(int(data["interval"])),
            interval_begin=_parse_interval_begin(data["interval_begin"]),
            open=float(data["open"]),
            high=float(data["high"]),
            low=float(data["low"]),
            close=float(data["close"]),
            volume=float(data["volume"]),
        )
