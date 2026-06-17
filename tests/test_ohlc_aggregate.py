"""Unit tests for the 1H-from-5m aggregator (TB00768 Opt 5, ohlc_aggregate.py).

The aggregator folds the closed ohlc_5m stream into EXACT 1H candles so the HtfCache
advances live without a (Kraken-refused) ohlc_60m subscription. Covers: the lossless
twelve-candle fold, the completeness gate (an hour-aligned shortfall -> Htf1hGap, a
mid-hour partial -> discarded), rollover timing, and per-symbol independence.
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.candle_close import CommittedCandle
from tothbot.exchange.ohlc_aggregate import (
    Closed1H,
    Htf1hGap,
    OhlcAggregator,
    hour_begin_of,
)

HOUR = 1_699_999_200  # a Unix second that is an exact hour boundary (divisible by 3600)
assert HOUR % 3600 == 0


def _c(symbol: str, begin: int, o, h, l, c, v) -> CommittedCandle:
    return CommittedCandle(
        symbol=symbol, interval_begin=begin,
        open=Decimal(o), high=Decimal(h), low=Decimal(l), close=Decimal(c), volume=Decimal(v),
    )


def _full_hour(symbol: str, hour: int) -> list[CommittedCandle]:
    """Twelve 5m candles spanning [hour, hour+3600): close ramps 100..111, vol all 2."""
    return [
        _c(symbol, hour + 300 * i, 100 + i, 100 + i + 5, 100 + i - 3, 100 + i + 1, 2)
        for i in range(12)
    ]


def test_hour_begin_of_floors_to_the_hour():
    assert hour_begin_of(HOUR) == HOUR
    assert hour_begin_of(HOUR + 300) == HOUR
    assert hour_begin_of(HOUR + 3599) == HOUR
    assert hour_begin_of(HOUR + 3600) == HOUR + 3600


def test_complete_hour_folds_losslessly_and_eager_emits_on_the_twelfth_close():
    agg = OhlcAggregator()
    candles = _full_hour("BTC/USD", HOUR)
    # The first eleven closes accumulate with NO emission...
    for candle in candles[:11]:
        assert agg.fold(candle) is None
    # ...the TWELFTH (the [:55,:00) close) eager-emits the exact 1H candle (native timing).
    result = agg.fold(candles[11])
    assert isinstance(result, Closed1H)
    one_h = result.candle
    assert one_h.symbol == "BTC/USD"
    assert one_h.interval_begin == HOUR            # the 1H candle is keyed to the hour start
    assert one_h.open == Decimal(100)              # open of the [:00,:05) candle
    assert one_h.close == Decimal(112)             # close of the [:55,:00) candle (100+11+1)
    assert one_h.high == Decimal(116)              # max high (last candle: 100+11+5)
    assert one_h.low == Decimal(97)                # min low (first candle: 100+0-3)
    assert one_h.volume == Decimal(24)             # sum of twelve volumes (12 * 2)


def test_hour_aligned_shortfall_is_a_gap_not_a_corrupt_candle():
    agg = OhlcAggregator()
    # Eleven of twelve slots (a reconnect dropped one 5m close mid-hour), hour-aligned.
    for candle in _full_hour("ETH/USD", HOUR)[:11]:
        assert agg.fold(candle) is None
    result = agg.fold(_c("ETH/USD", HOUR + 3600, 50, 51, 49, 50, 1))
    assert result == Htf1hGap("ETH/USD", HOUR)     # self-heal signal, NOT a Closed1H


def test_mid_hour_partial_is_discarded_silently():
    agg = OhlcAggregator()
    # Startup mid-hour: the bucket's first candle is NOT on the hour boundary.
    for candle in _full_hour("SOL/USD", HOUR)[6:]:   # slots :30.. only (began mid-hour)
        assert agg.fold(candle) is None
    result = agg.fold(_c("SOL/USD", HOUR + 3600, 50, 51, 49, 50, 1))
    assert result is None                            # expected partial -> no gap, no candle


def test_symbols_are_independent():
    agg = OhlcAggregator()
    btc = _full_hour("BTC/USD", HOUR)
    eth = _full_hour("ETH/USD", HOUR)
    # Interleave two symbols' full hours; each eager-emits on its OWN twelfth close.
    results = {"BTC/USD": [], "ETH/USD": []}
    for i, (b, e) in enumerate(zip(btc, eth)):
        rb, re = agg.fold(b), agg.fold(e)
        if rb is not None:
            results["BTC/USD"].append((i, rb))
        if re is not None:
            results["ETH/USD"].append((i, re))
    # Exactly one emission each, both on the twelfth candle (index 11), both Closed1H.
    assert [i for i, _ in results["BTC/USD"]] == [11]
    assert [i for i, _ in results["ETH/USD"]] == [11]
    assert isinstance(results["BTC/USD"][0][1], Closed1H)
    assert isinstance(results["ETH/USD"][0][1], Closed1H)


def test_out_of_order_stale_candle_for_a_closed_hour_is_ignored():
    agg = OhlcAggregator()
    for candle in _full_hour("BTC/USD", HOUR):
        agg.fold(candle)                                         # emits on the twelfth
    agg.fold(_c("BTC/USD", HOUR + 3600, 200, 205, 199, 201, 2))  # roll into hour H+1
    # A late candle belonging to the already-closed hour H must be a defensive no-op.
    assert agg.fold(_c("BTC/USD", HOUR + 300, 9, 9, 9, 9, 9)) is None


def test_already_emitted_hour_rolling_over_does_not_re_emit():
    agg = OhlcAggregator()
    emits = [r for c in _full_hour("BTC/USD", HOUR) if (r := agg.fold(c)) is not None]
    assert len(emits) == 1 and isinstance(emits[0], Closed1H)   # eager-emitted once
    # The first candle of hour H+1 rolls the (already-emitted) bucket -> NOT a second emit.
    assert agg.fold(_c("BTC/USD", HOUR + 3600, 1, 1, 1, 1, 1)) is None


def test_two_consecutive_complete_hours_each_eager_emit():
    agg = OhlcAggregator()
    first = [r for c in _full_hour("BTC/USD", HOUR) if (r := agg.fold(c)) is not None]
    second = [r for c in _full_hour("BTC/USD", HOUR + 3600) if (r := agg.fold(c)) is not None]
    assert len(first) == 1 and first[0].candle.interval_begin == HOUR
    assert len(second) == 1 and second[0].candle.interval_begin == HOUR + 3600
