"""mod:Regime_Engine daily-compute orchestrator tests (regime/scheduler.py).

Covers 0500000 dv1_250 Image4 daily 00:00 UTC compute: the REST-driven sweep that fills the
symbol-keyed RegimeCache, the BTC/USD market_regime anchor (ar:AR-074, always computed), the
ar:AR-036 1.1s inter-call stagger, the AR-017 exclusion delegated to the REST parser
(exclude_forming=False), per-pair failure isolation (REGIME_COMPUTE_FAIL, prior entry kept),
and the optional EC-L1A-002 wiring into WSManager.on_regime_classified.

Driven with stdlib asyncio.run over a fake REST client + a no-op sleep - no network, no timers.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from tothbot.regime.engine import RegimeClassification
from tothbot.regime.scheduler import (
    DAILY_INTERVAL_MIN,
    MARKET_ANCHOR,
    OHLC_STAGGER_SEC,
    DailyRegimeCompute,
    RegimeCache,
    RegimeComputeFailed,
)
from tothbot.regime.taxonomy import Regime
from tothbot.rest.client import KrakenRestError, OhlcResponse, RestOhlcBar


# --------------------------------------------------------------------------- helpers
def _ohlc_response(n=60, start=100, step=1, span=2):
    """An OhlcResponse with n committed linear-trend bars + 1 forming bar (already split off,
    as rest.client.parse_ohlc would). n>=50 satisfies compute_regime's EMA50 floor."""
    committed = tuple(
        RestOhlcBar(
            time=1700000000 + i * 86400,
            open=Decimal(start) + Decimal(step) * i,
            high=Decimal(start) + Decimal(step) * i + span,
            low=Decimal(start) + Decimal(step) * i - span,
            close=Decimal(start) + Decimal(step) * i,
            volume=Decimal(10),
        )
        for i in range(n)
    )
    forming = RestOhlcBar(time=1700000000 + n * 86400, open=Decimal(9), high=Decimal(9),
                          low=Decimal(9), close=Decimal(9), volume=Decimal(1))
    return OhlcResponse(committed=committed, forming=forming, last=committed[-1].time)


class _FakeRest:
    """Hand-driven REST client: scripted OhlcResponse (or Exception) per pair; records calls."""

    def __init__(self, responses=None, default=None) -> None:
        self._responses = responses or {}
        self._default = default if default is not None else _ohlc_response()
        self.calls: list[tuple[str, int]] = []

    async def get_ohlc_data(self, pair, interval, *, since=None):
        self.calls.append((pair, interval))
        r = self._responses.get(pair, self._default)
        if isinstance(r, Exception):
            raise r
        return r


class _SleepSpy:
    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, seconds):
        self.delays.append(seconds)


# --------------------------------------------------------------------------- RegimeCache
def test_cache_market_regime_is_anchor():
    cache = RegimeCache("BTC/USD")
    assert cache.market_regime is None
    rest = _FakeRest()
    cache = asyncio.run(DailyRegimeCompute(rest).compute_all(["ETH/USD"]))
    assert cache.market_regime is not None
    assert cache.market_regime == cache.regime("BTC/USD")


# --------------------------------------------------------------------------- compute_all
def test_compute_all_fills_cache_for_pairs_and_anchor():
    rest = _FakeRest()
    cache = asyncio.run(DailyRegimeCompute(rest, sleep=_SleepSpy()).compute_all(["ETH/USD", "SOL/USD"]))
    assert cache.symbols == frozenset({"ETH/USD", "SOL/USD", "BTC/USD"})
    assert isinstance(cache.get("ETH/USD"), RegimeClassification)


def test_compute_all_uses_daily_interval_1440():
    rest = _FakeRest()
    asyncio.run(DailyRegimeCompute(rest, sleep=_SleepSpy()).compute_all(["ETH/USD"]))
    assert all(interval == DAILY_INTERVAL_MIN for _, interval in rest.calls)
    assert DAILY_INTERVAL_MIN == 1440


def test_compute_all_staggers_between_calls():
    rest = _FakeRest()
    spy = _SleepSpy()
    # 2 pairs + anchor = 3 targets -> 2 inter-call staggers.
    asyncio.run(DailyRegimeCompute(rest, sleep=spy).compute_all(["ETH/USD", "SOL/USD"]))
    assert spy.delays == [OHLC_STAGGER_SEC, OHLC_STAGGER_SEC]


def test_anchor_not_double_called_when_in_pairs():
    rest = _FakeRest()
    asyncio.run(DailyRegimeCompute(rest, sleep=_SleepSpy()).compute_all(["BTC/USD", "ETH/USD"]))
    called = [pair for pair, _ in rest.calls]
    assert called.count("BTC/USD") == 1
    assert set(called) == {"BTC/USD", "ETH/USD"}


def test_classified_events_emitted():
    rest = _FakeRest()
    events = []
    asyncio.run(
        DailyRegimeCompute(rest, sleep=_SleepSpy(), on_event=events.append).compute_all(["ETH/USD"])
    )
    codes = [e.code for e in events]
    assert codes.count("REGIME_CLASSIFIED") == 2  # ETH/USD + BTC/USD anchor


# --------------------------------------------------------------------------- failure isolation
def test_rest_error_emits_compute_fail_and_skips_pair():
    rest = _FakeRest(responses={"ETH/USD": KrakenRestError(["EAPI:Rate limit exceeded"])})
    events = []
    cache = asyncio.run(
        DailyRegimeCompute(rest, sleep=_SleepSpy(), on_event=events.append).compute_all(["ETH/USD"])
    )
    assert cache.get("ETH/USD") is None          # skipped
    assert cache.get("BTC/USD") is not None       # the sweep continued to the anchor
    fails = [e for e in events if isinstance(e, RegimeComputeFailed)]
    assert len(fails) == 1 and fails[0].symbol == "ETH/USD"


def test_too_few_candles_emits_compute_fail():
    rest = _FakeRest(responses={"ETH/USD": _ohlc_response(n=30)})  # < EMA50 floor
    events = []
    cache = asyncio.run(
        DailyRegimeCompute(rest, sleep=_SleepSpy(), on_event=events.append).compute_all(["ETH/USD"])
    )
    assert cache.get("ETH/USD") is None
    assert any(isinstance(e, RegimeComputeFailed) for e in events)


def test_failure_preserves_prior_cache_entry():
    rest_ok = _FakeRest()
    compute = DailyRegimeCompute(rest_ok, sleep=_SleepSpy())
    cache = asyncio.run(compute.compute_all(["ETH/USD"]))
    prior = cache.get("ETH/USD")
    assert prior is not None
    # Next day: ETH fails; the prior entry must remain (stale, not cleared).
    rest_fail = _FakeRest(responses={"ETH/USD": KrakenRestError(["boom"])})
    asyncio.run(DailyRegimeCompute(rest_fail, sleep=_SleepSpy()).compute_all(["ETH/USD"], cache=cache))
    assert cache.get("ETH/USD") is prior


# --------------------------------------------------------------------------- L1a exit wiring
class _FakeWM:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def on_regime_classified(self, symbol, classification, *, bid=None, ask=None, **_):
        self.calls.append((symbol, classification.regime, bid, ask))


def test_drives_on_regime_classified_when_wm_wired():
    rest = _FakeRest()
    wm = _FakeWM()
    asyncio.run(
        DailyRegimeCompute(rest, sleep=_SleepSpy(), ws_manager=wm,
                           bbo_provider=lambda s: (Decimal("10"), Decimal("11"))).compute_all(["ETH/USD"])
    )
    symbols = [c[0] for c in wm.calls]
    assert "ETH/USD" in symbols and "BTC/USD" in symbols
    eth = next(c for c in wm.calls if c[0] == "ETH/USD")
    assert eth[2] == Decimal("10") and eth[3] == Decimal("11")  # bbo passed through


def test_no_wm_is_fine():
    rest = _FakeRest()
    cache = asyncio.run(DailyRegimeCompute(rest, sleep=_SleepSpy()).compute_all(["ETH/USD"]))
    assert cache.get("ETH/USD") is not None  # no crash without a WSManager
