"""Per-pair startup warm-up orchestrator tests (exchange/warmup.py; ar:AR-044/AR-068/AR-045).

Covers the two-call seed per pair (GetOHLCData interval 5 -> LiveIndicators + 5m trackers;
interval 60 -> HtfCache + 1H trackers), the ar:AR-036 1.1s same-pair stagger, the ar:AR-044
cross-pair concurrency, per-pair failure isolation (WARM_UP_FAIL, omitted), the ar:AR-045
candle-close-detection init from the last committed candle, and the ar:AR-068 WARM_UP -> READY
gate (5m + 1H seeded AND regime present). Driven with asyncio.run over a fake REST client + a
no-op sleep - no network, no timers.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from tothbot.exchange.warmup import (
    HTF_EMA_LONG,
    INTERVAL_5_MIN,
    INTERVAL_60_MIN,
    WARMUP_STAGGER_SEC,
    HtfCache,
    PairWarmup,
    WarmupOrchestrator,
    WarmUpFailed,
    ready_pairs,
)
from tothbot.regime.indicators import ema
from tothbot.regime.live_indicators import LiveIndicators
from tothbot.rest.client import KrakenRestError, OhlcResponse, RestOhlcBar


# --------------------------------------------------------------------------- helpers
def _ohlc_response(n=60, start=100, step=1, span=2, base_time=1700000000, interval_sec=300):
    """An OhlcResponse with n committed linear-trend bars + 1 forming bar (already split off, as
    rest.client.parse_ohlc would). n>=50 satisfies EMA50 (the 1H HTF floor)."""
    committed = tuple(
        RestOhlcBar(
            time=base_time + i * interval_sec,
            open=Decimal(start) + Decimal(step) * i,
            high=Decimal(start) + Decimal(step) * i + span,
            low=Decimal(start) + Decimal(step) * i - span,
            close=Decimal(start) + Decimal(step) * i,
            volume=Decimal(1000 + (i * 37) % 500),
        )
        for i in range(n)
    )
    forming = RestOhlcBar(time=base_time + n * interval_sec, open=Decimal(9), high=Decimal(9),
                          low=Decimal(9), close=Decimal(9), volume=Decimal(1))
    return OhlcResponse(committed=committed, forming=forming, last=committed[-1].time)


class _FakeRest:
    """REST client scripted per (pair, interval): default linear responses; per-pair Exception to
    drive failure isolation. Records every call as (pair, interval)."""

    def __init__(self, responses=None, default5=None, default60=None) -> None:
        self._responses = responses or {}
        self._d5 = default5 if default5 is not None else _ohlc_response(interval_sec=300)
        self._d60 = default60 if default60 is not None else _ohlc_response(interval_sec=3600)
        self.calls: list[tuple[str, int]] = []

    async def get_ohlc_data(self, pair, interval, *, since=None):
        self.calls.append((pair, interval))
        scripted = self._responses.get(pair)
        if isinstance(scripted, Exception):
            raise scripted
        if scripted is not None:
            return scripted.get(interval, self._d5 if interval == INTERVAL_5_MIN else self._d60)
        return self._d5 if interval == INTERVAL_5_MIN else self._d60


class _SleepSpy:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds):
        self.delays.append(seconds)


# --------------------------------------------------------------------------- warm_pair
def test_warm_pair_seeds_indicators_and_htf():
    rest = _FakeRest()
    pw = asyncio.run(WarmupOrchestrator(rest, sleep=_SleepSpy()).warm_pair("ETH/USD"))
    assert isinstance(pw, PairWarmup)
    assert isinstance(pw.indicators, LiveIndicators) and pw.indicators.seeded
    assert pw.indicators.atr_14 is not None and pw.indicators.rsi_14 is not None
    assert isinstance(pw.htf, HtfCache)


def test_warm_pair_calls_interval_5_then_60_with_stagger():
    rest = _FakeRest()
    spy = _SleepSpy()
    asyncio.run(WarmupOrchestrator(rest, sleep=spy).warm_pair("ETH/USD"))
    assert [interval for _, interval in rest.calls] == [INTERVAL_5_MIN, INTERVAL_60_MIN]
    assert spy.delays == [WARMUP_STAGGER_SEC]  # one stagger between the same pair's two calls


def test_warm_pair_htf_ema_matches_batch():
    r60 = _ohlc_response(n=60, interval_sec=3600)
    rest = _FakeRest(responses={"ETH/USD": {INTERVAL_60_MIN: r60}})
    pw = asyncio.run(WarmupOrchestrator(rest, sleep=_SleepSpy()).warm_pair("ETH/USD"))
    closes60 = [b.close for b in r60.committed]
    assert pw.htf.close_1h == r60.committed[-1].close
    assert pw.htf.ema20_1h == ema(closes60, 20)
    assert pw.htf.ema50_1h == ema(closes60, HTF_EMA_LONG)


def test_ar045_trackers_init_from_last_committed():
    r5 = _ohlc_response(n=60, interval_sec=300)
    r60 = _ohlc_response(n=60, interval_sec=3600)
    rest = _FakeRest(responses={"ETH/USD": {INTERVAL_5_MIN: r5, INTERVAL_60_MIN: r60}})
    pw = asyncio.run(WarmupOrchestrator(rest, sleep=_SleepSpy()).warm_pair("ETH/USD"))
    assert pw.last_interval_begin == r5.committed[-1].time
    assert pw.last_complete_candle == r5.committed[-1]
    assert pw.last_interval_begin_60 == r60.committed[-1].time
    assert pw.last_complete_candle_60 == r60.committed[-1]


# --------------------------------------------------------------------------- warm_all (concurrency)
def test_warm_all_warms_every_pair():
    rest = _FakeRest()
    warmups = asyncio.run(WarmupOrchestrator(rest, sleep=_SleepSpy()).warm_all(["ETH/USD", "SOL/USD"]))
    assert set(warmups) == {"ETH/USD", "SOL/USD"}
    assert all(w.indicators.seeded for w in warmups.values())


def test_warm_all_emits_ready_event_per_pair():
    rest = _FakeRest()
    events = []
    asyncio.run(
        WarmupOrchestrator(rest, sleep=_SleepSpy(), on_event=events.append).warm_all(["ETH/USD"])
    )
    assert [e.code for e in events] == ["WARM_UP_READY"]


# --------------------------------------------------------------------------- failure isolation
def test_rest_error_isolates_one_pair():
    rest = _FakeRest(responses={"BAD/USD": KrakenRestError(["EAPI:Rate limit exceeded"])})
    events = []
    warmups = asyncio.run(
        WarmupOrchestrator(rest, sleep=_SleepSpy(), on_event=events.append)
        .warm_all(["ETH/USD", "BAD/USD"])
    )
    assert set(warmups) == {"ETH/USD"}             # the good pair seeded
    fails = [e for e in events if isinstance(e, WarmUpFailed)]
    assert len(fails) == 1 and fails[0].symbol == "BAD/USD"


def test_too_few_candles_emits_warm_up_fail():
    short5 = _ohlc_response(n=15, interval_sec=300)  # < 21 closes -> seed raises
    rest = _FakeRest(responses={"TINY/USD": {INTERVAL_5_MIN: short5}})
    events = []
    warmups = asyncio.run(
        WarmupOrchestrator(rest, sleep=_SleepSpy(), on_event=events.append).warm_all(["TINY/USD"])
    )
    assert warmups == {}
    assert any(isinstance(e, WarmUpFailed) for e in events)


# --------------------------------------------------------------------------- READY gate (ar:AR-068)
def test_is_ready_requires_regime_present():
    rest = _FakeRest()
    pw = asyncio.run(WarmupOrchestrator(rest, sleep=_SleepSpy()).warm_pair("ETH/USD"))
    assert pw.is_ready({}) is False                       # seeded but no regime -> WARM_UP
    assert pw.is_ready({"ETH/USD": object()}) is True     # regime present -> READY


def test_ready_pairs_filters_to_regime_backed():
    rest = _FakeRest()
    warmups = asyncio.run(
        WarmupOrchestrator(rest, sleep=_SleepSpy()).warm_all(["ETH/USD", "SOL/USD"])
    )
    regime = {"ETH/USD": object()}  # only ETH has a regime yet
    ready = ready_pairs(warmups, regime)
    assert set(ready) == {"ETH/USD"}
