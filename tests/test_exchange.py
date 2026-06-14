"""S2a data-layer tests: channels, candle parsing (A-16), and system clock."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tothbot.exchange import channels
from tothbot.exchange.candle import Candle, Interval
from tothbot.exchange.channels import PublicChannel, PrivateChannel
from tothbot.exchange.clock import SystemClock
from tothbot.exchange.events import CandleCloseTrigger


# -- channels -----------------------------------------------------------

def test_channel_inventory():
    assert len(PublicChannel) == 5
    assert len(PrivateChannel) == 2


def test_system_clock_channel_is_5m_ohlc():
    assert channels.SYSTEM_CLOCK_CHANNEL is PublicChannel.OHLC_5M


# -- candle: A-16 interval_begin parsing --------------------------------

def _kraken_ohlc(interval: int = 5, begin: str = "2026-06-14T00:05:00.000000000Z"):
    return {
        "symbol": "BTC/USD",
        "interval": interval,
        "interval_begin": begin,
        "timestamp": "DEPRECATED-MUST-IGNORE",  # A-16: must NOT be used
        "open": "100.0",
        "high": "110.0",
        "low": "95.0",
        "close": "105.0",
        "volume": "12.5",
    }


def test_from_kraken_parses_interval_begin_nanoseconds():
    c = Candle.from_kraken(_kraken_ohlc())
    assert c.pair == "BTC/USD"
    assert c.interval is Interval.FIVE_MIN
    # interval_begin parsed as tz-aware UTC, nanoseconds truncated to micros.
    assert c.interval_begin == datetime(2026, 6, 14, 0, 5, 0, tzinfo=timezone.utc)
    assert (c.open, c.high, c.low, c.close, c.volume) == (100.0, 110.0, 95.0, 105.0, 12.5)


def test_from_kraken_ignores_deprecated_timestamp():
    # The bogus `timestamp` value must never surface on the candle.
    c = Candle.from_kraken(_kraken_ohlc())
    assert "DEPRECATED" not in repr(c)


def test_candle_open_and_close_ts():
    c = Candle.from_kraken(_kraken_ohlc())
    assert c.open_ts == c.interval_begin
    assert c.close_ts == c.interval_begin + timedelta(minutes=5)


def test_htf_candle_interval():
    c = Candle.from_kraken(_kraken_ohlc(interval=60))
    assert c.interval is Interval.ONE_HOUR
    assert c.close_ts == c.interval_begin + timedelta(minutes=60)


def test_interval_begin_without_subseconds_or_z():
    c = Candle.from_kraken(_kraken_ohlc(begin="2026-06-14T00:05:00+00:00"))
    assert c.interval_begin == datetime(2026, 6, 14, 0, 5, 0, tzinfo=timezone.utc)


# -- system clock -------------------------------------------------------

def _candle(interval: Interval) -> Candle:
    return Candle(
        pair="BTC/USD",
        interval=interval,
        interval_begin=datetime(2026, 6, 14, 0, 5, 0, tzinfo=timezone.utc),
        open=1.0, high=2.0, low=0.5, close=1.5, volume=10.0,
    )


def test_clock_fires_trigger_on_5m_close():
    clock = SystemClock()
    received: list[CandleCloseTrigger] = []
    clock.subscribe(received.append)
    trigger = clock.on_candle_close(_candle(Interval.FIVE_MIN))
    assert isinstance(trigger, CandleCloseTrigger)
    assert received == [trigger]
    assert trigger.pair == "BTC/USD"
    assert trigger.close_ts == datetime(2026, 6, 14, 0, 10, 0, tzinfo=timezone.utc)


def test_clock_dispatches_to_all_listeners_in_order():
    clock = SystemClock()
    order: list[str] = []
    clock.subscribe(lambda t: order.append("a"))
    clock.subscribe(lambda t: order.append("b"))
    clock.on_candle_close(_candle(Interval.FIVE_MIN))
    assert order == ["a", "b"]


def test_clock_rejects_non_5m_candle():
    clock = SystemClock()
    with pytest.raises(ValueError):
        clock.on_candle_close(_candle(Interval.ONE_HOUR))
