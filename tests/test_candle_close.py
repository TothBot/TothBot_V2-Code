"""ar:AR-045 candle-close detection tests (exchange/candle_close.py).

Covers the interval_begin Unix-second normalization across the REST seed form (int) and the live
WS form (RFC3339 string / datetime), the Decimal frame parse (ar:AR-047), and the in-progress vs
closed boundary: same interval_begin -> update the in-progress snapshot + no fire; a newer
interval_begin -> emit the just-closed committed candle. The detector seeds from the warm-up's last
committed candle (RestOhlcBar.time) and then compares the live WS frames on the same Unix axis.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

import pytest

from tothbot.exchange.candle_close import (
    CandleCloseDetector,
    CommittedCandle,
    committed_candle_from_frame,
    to_interval_unix,
)
from tothbot.rest.client import RestOhlcBar


# --------------------------------------------------------------------------- to_interval_unix
def test_unix_from_int_passthrough():
    assert to_interval_unix(1700000000) == 1700000000


def test_unix_from_rfc3339_z():
    dt = datetime(2026, 6, 15, 0, 0, 0, tzinfo=timezone.utc)
    assert to_interval_unix("2026-06-15T00:00:00Z") == int(dt.timestamp())


def test_unix_from_rfc3339_nanos_truncates():
    # Nanosecond precision (9 digits) must parse (truncated to micros), same second key.
    base = to_interval_unix("2026-06-15T00:00:00Z")
    assert to_interval_unix("2026-06-15T00:00:00.123456789Z") == base


def test_unix_from_datetime():
    dt = datetime(2026, 6, 15, 0, 5, 0, tzinfo=timezone.utc)
    assert to_interval_unix(dt) == int(dt.timestamp())


def test_bool_rejected():
    with pytest.raises(TypeError):
        to_interval_unix(True)


# --------------------------------------------------------------------------- frame parse (AR-047)
def test_frame_parse_is_decimal():
    elem = {
        "symbol": "BTC/USD",
        "interval_begin": "2026-06-15T00:00:00Z",
        "open": "100.5", "high": "101.0", "low": "99.5", "close": "100.0", "volume": "12.3",
    }
    c = committed_candle_from_frame(elem)
    assert isinstance(c.open, Decimal) and c.close == Decimal("100.0")
    assert c.symbol == "BTC/USD"
    assert c.interval_begin == to_interval_unix("2026-06-15T00:00:00Z")


def test_frame_parse_floats_via_str():
    # A float in the frame must go through Decimal(str(value)) - never Decimal(float).
    elem = {"symbol": "X", "interval_begin": 1700000000,
            "open": 1.1, "high": 1.2, "low": 1.0, "close": 1.15, "volume": 5.0}
    c = committed_candle_from_frame(elem)
    assert c.close == Decimal("1.15")  # exact, as Decimal(str(1.15))


# --------------------------------------------------------------------------- detector
def _candle(begin: int, close: str) -> CommittedCandle:
    d = Decimal(close)
    return CommittedCandle(symbol="BTC/USD", interval_begin=begin,
                           open=d, high=d + 1, low=d - 1, close=d, volume=Decimal(10))


def test_seed_from_rest_bar_then_in_progress_returns_none():
    seed = RestOhlcBar(time=1700000000, open=Decimal(100), high=Decimal(101),
                       low=Decimal(99), close=Decimal(100), volume=Decimal(10))
    det = CandleCloseDetector("BTC/USD", last_interval_begin=seed.time, last_complete_candle=seed)
    # A WS update for the NEXT candle (begin = seed + 300) is a roll -> the seed candle closed.
    closed = det.observe(_candle(1700000300, "100.5"))
    assert closed is seed


def test_same_interval_updates_snapshot_no_fire():
    first = _candle(1700000300, "100.0")
    det = CandleCloseDetector("BTC/USD", last_interval_begin=1700000300, last_complete_candle=first)
    snap2 = _candle(1700000300, "100.8")  # newer cumulative snapshot, same candle
    assert det.observe(snap2) is None
    assert det.last_complete_candle is snap2  # in-progress snapshot advanced
    assert det.last_interval_begin == 1700000300


def test_roll_emits_prior_snapshot_then_accumulates_new():
    first = _candle(1700000300, "100.0")
    det = CandleCloseDetector("BTC/USD", last_interval_begin=1700000300, last_complete_candle=first)
    det.observe(_candle(1700000300, "100.9"))            # in-progress update
    rolled = _candle(1700000600, "101.0")                # next candle's first tick
    closed = det.observe(rolled)
    assert closed.close == Decimal("100.9")              # the final pre-roll snapshot
    assert det.last_interval_begin == 1700000600
    assert det.last_complete_candle is rolled


def test_string_interval_begin_matches_int_seed_axis():
    # Seed via REST int unix; the live WS frame carries the SAME instant as an RFC3339 string.
    unix = to_interval_unix("2026-06-15T00:00:00Z")
    seed = _candle(unix, "100.0")
    det = CandleCloseDetector("BTC/USD", last_interval_begin=unix, last_complete_candle=seed)
    same = committed_candle_from_frame({
        "symbol": "BTC/USD", "interval_begin": "2026-06-15T00:00:00.5Z",
        "open": "100", "high": "101", "low": "99", "close": "100.4", "volume": "11",
    })
    assert det.observe(same) is None  # same second -> still in progress
