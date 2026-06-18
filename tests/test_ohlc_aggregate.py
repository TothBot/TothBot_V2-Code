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
    Closed24H,
    Htf1hGap,
    Htf24hGap,
    OhlcAggregator,
    day_begin_of,
    hour_begin_of,
)

HOUR = 1_699_999_200  # a Unix second that is an exact hour boundary (divisible by 3600)
assert HOUR % 3600 == 0

DAY = 1_700_006_400  # a Unix second that is an exact UTC-day boundary (divisible by 86400)
assert DAY % 86400 == 0


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


# --- TB00787: the 24h DECISION fold (fold_hour), the second stage one timeframe up ----------

def _full_day(symbol: str, day: int) -> list[CommittedCandle]:
    """Twenty-four 1H candles spanning [day, day+86400): close ramps 100..123, vol all 2."""
    return [
        _c(symbol, day + 3600 * i, 100 + i, 100 + i + 5, 100 + i - 3, 100 + i + 1, 2)
        for i in range(24)
    ]


def test_day_begin_of_floors_to_the_day():
    assert day_begin_of(DAY) == DAY
    assert day_begin_of(DAY + 3600) == DAY
    assert day_begin_of(DAY + 86399) == DAY
    assert day_begin_of(DAY + 86400) == DAY + 86400


def test_complete_day_folds_losslessly_and_eager_emits_on_the_twentyfourth_close():
    agg = OhlcAggregator()
    candles = _full_day("BTC/USD", DAY)
    # The first twenty-three 1H closes accumulate with NO emission...
    for candle in candles[:23]:
        assert agg.fold_hour(candle) is None
    # ...the TWENTY-FOURTH (the 23:00->00:00 close) eager-emits the exact 24h decision candle.
    result = agg.fold_hour(candles[23])
    assert isinstance(result, Closed24H)
    day = result.candle
    assert day.symbol == "BTC/USD"
    assert day.interval_begin == DAY               # the 24h candle is keyed to 00:00 UTC
    assert day.open == Decimal(100)                # open of the [00:00,01:00) 1H
    assert day.close == Decimal(124)               # close of the [23:00,00:00) 1H (100+23+1)
    assert day.high == Decimal(128)                # max high (last 1H: 100+23+5)
    assert day.low == Decimal(97)                  # min low (first 1H: 100+0-3)
    assert day.volume == Decimal(48)               # sum of twenty-four volumes (24 * 2)


def test_day_aligned_shortfall_is_a_gap_not_a_corrupt_candle():
    agg = OhlcAggregator()
    # Twenty-three of twenty-four slots (an unrecovered 1H step mid-day), day-aligned.
    for candle in _full_day("ETH/USD", DAY)[:23]:
        assert agg.fold_hour(candle) is None
    result = agg.fold_hour(_c("ETH/USD", DAY + 86400, 50, 51, 49, 50, 1))
    assert result == Htf24hGap("ETH/USD", DAY)     # self-heal signal, NOT a Closed24H


def test_mid_day_partial_is_discarded_silently():
    agg = OhlcAggregator()
    # Startup mid-day: the bucket's first 1H is NOT on the day boundary.
    for candle in _full_day("SOL/USD", DAY)[12:]:   # slots 12:00.. only (began mid-day)
        assert agg.fold_hour(candle) is None
    result = agg.fold_hour(_c("SOL/USD", DAY + 86400, 50, 51, 49, 50, 1))
    assert result is None                            # expected partial -> no gap, no candle


def test_24h_symbols_are_independent():
    agg = OhlcAggregator()
    btc, eth = _full_day("BTC/USD", DAY), _full_day("ETH/USD", DAY)
    results = {"BTC/USD": [], "ETH/USD": []}
    for i, (b, e) in enumerate(zip(btc, eth)):
        rb, re = agg.fold_hour(b), agg.fold_hour(e)
        if rb is not None:
            results["BTC/USD"].append((i, rb))
        if re is not None:
            results["ETH/USD"].append((i, re))
    # Exactly one emission each, both on the twenty-fourth 1H (index 23), both Closed24H.
    assert [i for i, _ in results["BTC/USD"]] == [23]
    assert [i for i, _ in results["ETH/USD"]] == [23]
    assert isinstance(results["BTC/USD"][0][1], Closed24H)
    assert isinstance(results["ETH/USD"][0][1], Closed24H)


def test_out_of_order_stale_1h_for_a_closed_day_is_ignored():
    agg = OhlcAggregator()
    for candle in _full_day("BTC/USD", DAY):
        agg.fold_hour(candle)                                       # emits on the twenty-fourth
    agg.fold_hour(_c("BTC/USD", DAY + 86400, 200, 205, 199, 201, 2))  # roll into day D+1
    # A late 1H belonging to the already-closed day D must be a defensive no-op.
    assert agg.fold_hour(_c("BTC/USD", DAY + 3600, 9, 9, 9, 9, 9)) is None


def test_already_emitted_day_rolling_over_does_not_re_emit():
    agg = OhlcAggregator()
    emits = [r for c in _full_day("BTC/USD", DAY) if (r := agg.fold_hour(c)) is not None]
    assert len(emits) == 1 and isinstance(emits[0], Closed24H)      # eager-emitted once
    # The first 1H of day D+1 rolls the (already-emitted) bucket -> NOT a second emit.
    assert agg.fold_hour(_c("BTC/USD", DAY + 86400, 1, 1, 1, 1, 1)) is None


def test_two_consecutive_complete_days_each_eager_emit():
    agg = OhlcAggregator()
    first = [r for c in _full_day("BTC/USD", DAY) if (r := agg.fold_hour(c)) is not None]
    second = [r for c in _full_day("BTC/USD", DAY + 86400) if (r := agg.fold_hour(c)) is not None]
    assert len(first) == 1 and first[0].candle.interval_begin == DAY
    assert len(second) == 1 and second[0].candle.interval_begin == DAY + 86400


def test_5m_to_1h_to_24h_chains_end_to_end():
    """The real path: 288 contiguous 5m closes -> fold emits 24 Closed1H -> each drives fold_hour
    -> exactly one Closed24H on the last. Proves the two stages compose losslessly over a full day."""
    agg = OhlcAggregator()
    day_results: list[Closed24H] = []
    one_h_count = 0
    for i in range(288):  # 288 contiguous 5m candles partition one UTC day (24h * 12)
        begin = DAY + 300 * i
        base = 100 + i
        folded = agg.fold(_c("BTC/USD", begin, base, base + 5, base - 3, base + 1, 1))
        if isinstance(folded, Closed1H):
            one_h_count += 1
            r = agg.fold_hour(folded.candle)
            if isinstance(r, Closed24H):
                day_results.append(r)
    assert one_h_count == 24                         # twenty-four exact 1H folds across the day
    assert len(day_results) == 1                     # one decision candle, on the final close
    day = day_results[0].candle
    assert day.interval_begin == DAY
    assert day.open == Decimal(100)                  # open of the very first 5m
    assert day.close == Decimal(100 + 287 + 1)       # close of the very last 5m (388)
    assert day.high == Decimal(100 + 287 + 5)        # global max high (392)
    assert day.low == Decimal(100 + 0 - 3)           # global min low (97)
    assert day.volume == Decimal(288)                # 288 * 1, lossless sum
