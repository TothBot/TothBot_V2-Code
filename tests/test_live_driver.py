"""Live-driver tests (pipeline/live_driver.py): the ohlc(5m)/ohlc(60m) stream -> sweep + HTF wiring.

Covers: on_ohlc_5m detects the candle close (ar:AR-045), steps the LiveIndicators ONLY for a
genuinely-new candle (the first fire re-emits the already-seeded committed[-1] and must NOT
double-count it - the ar:AR-016/AR-075 step guard), and runs sweep_pair per permitted side;
rule:HR-WM-012 skips the whole frame while reconnecting; an unknown pair is ignored. on_ohlc_60m
advances the HtfCache EMA(20)/EMA(50) incrementally (ar:AR-044) and drives wm.on_htf_ohlc_close
(EC-L1A-001 1H reversal). make_ciats_sink records every event to mod:Logger and ingests a
TRADE_CLOSE into the module's CiatsPool. Driven with asyncio.run over fakes - no network.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from tothbot.ciats.pool import CiatsPool
from tothbot.exchange.position_mirror import PositionSide
from tothbot.exchange.warmup import WarmupOrchestrator
from tothbot.pipeline.live_driver import LiveSweepDriver, make_ciats_sink
from tothbot.pipeline.sweep import LiveProviders
from tothbot.regime.taxonomy import Regime
from tothbot.rest.client import OhlcResponse, RestOhlcBar


# --------------------------------------------------------------------------- fakes
def _ohlc_response(n=60, start=100, step=1, span=2, base_time=1700000000, interval_sec=300):
    committed = tuple(
        RestOhlcBar(time=base_time + i * interval_sec,
                    open=Decimal(start) + Decimal(step) * i,
                    high=Decimal(start) + Decimal(step) * i + span,
                    low=Decimal(start) + Decimal(step) * i - span,
                    close=Decimal(start) + Decimal(step) * i,
                    volume=Decimal(1000 + (i * 37) % 500))
        for i in range(n)
    )
    forming = RestOhlcBar(time=base_time + n * interval_sec, open=Decimal(9), high=Decimal(9),
                          low=Decimal(9), close=Decimal(9), volume=Decimal(1))
    return OhlcResponse(committed=committed, forming=forming, last=committed[-1].time)


class _FakeRest:
    async def get_ohlc_data(self, pair, interval, *, since=None):
        return _ohlc_response(interval_sec=300 if interval == 5 else 3600)


class _NoSleep:
    async def __call__(self, _seconds):
        return None


def _warm(symbol="BTC/USD"):
    return asyncio.run(WarmupOrchestrator(_FakeRest(), sleep=_NoSleep()).warm_pair(symbol))


class _Pos:
    def __init__(self, side, qty, price):
        self.side, self.qty, self.avg_entry_price = side, Decimal(qty), Decimal(price)


class _FakeWM:
    def __init__(self):
        self._wallets = {PositionSide.LONG: Decimal("5000"), PositionSide.SHORT: Decimal("5000")}
        self.modules = {s: SimpleNamespace(portfolio_baseline=Decimal("5000")) for s in self._wallets}
        self.dispatched = []
        self.htf_calls = []

    def open_positions(self):
        return {}

    def position(self, symbol):
        return None

    def exit_cooldown_at(self, symbol, side):
        return None

    def consecutive_loss_count(self, symbol, side):
        return 0

    def wallet_balance(self, side):
        return self._wallets.get(side)

    async def dispatch_entry(self, side, symbol, **kw):
        self.dispatched.append((side, symbol))
        return True

    def on_htf_ohlc_close(self, symbol, ema_short, ema_long, *, bid=None, ask=None, **_):
        self.htf_calls.append((symbol, ema_short, ema_long, bid, ask))


class _FakeLogger:
    def __init__(self):
        self.records = []

    def record(self, record, *, module="default"):
        self.records.append((module, record))


def _cache(regime=Regime.TRENDING_POS_NORMAL):
    classification = SimpleNamespace(regime=regime, ema20=Decimal("105"), ema50=Decimal("100"))
    return SimpleNamespace(get=lambda s: classification)


def _providers():
    return LiveProviders(
        instrument=lambda s: ("online", True, "600000"),
        bbo=lambda s: (Decimal("59990"), Decimal("60000")),
        expected_reward=lambda s, r: Decimal("0.05"),
        mpp_abs_cap_pct=lambda s, side: Decimal("0.01"),
        base_per_trade_size=lambda s, side, ref: Decimal("50"),
        ws_state=lambda s: "Subscribed",
        new_cl_ord_id=lambda: "cl-1",
        new_deadline=lambda: "2026-06-15T07:30:00Z",
    )


def _driver(warmups, wm, regime_cache=None, **over):
    return LiveSweepDriver(
        warmups=warmups, regime_cache=regime_cache or _cache(), providers=_providers(),
        wm=wm, logger=_FakeLogger(), **over,
    )


def _frame_5m(symbol, begin, *, close="200000", high="201000", low="199000", vol="9000"):
    return {"data": [{"symbol": symbol, "interval_begin": begin, "open": "199500",
                      "high": high, "low": low, "close": close, "volume": vol}]}


# --------------------------------------------------------------------------- make_ciats_sink
def test_sink_records_and_ingests_trade_close():
    logger, pool = _FakeLogger(), CiatsPool()
    sink = make_ciats_sink(logger, "long", pool)
    tc = SimpleNamespace(event="TRADE_CLOSE", net_pl_usd=Decimal("10"),
                         net_gain_usd=Decimal("10"), net_loss_usd=Decimal("0"))
    sink(tc)
    assert pool.trade_count == 1
    assert logger.records and logger.records[0][0] == "long"


def test_sink_ignores_non_trade_close_for_ciats():
    logger, pool = _FakeLogger(), CiatsPool()
    sink = make_ciats_sink(logger, "short", pool)
    sink(SimpleNamespace(code="SIGNAL_REJECTED"))
    assert pool.trade_count == 0
    assert len(logger.records) == 1


def test_sink_chains_downstream():
    seen = []
    sink = make_ciats_sink(_FakeLogger(), "long", CiatsPool(), downstream=seen.append)
    evt = SimpleNamespace(code="X")
    sink(evt)
    assert seen == [evt]


# --------------------------------------------------------------------------- on_ohlc_5m
def test_in_progress_candle_does_not_fire():
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm)
    # A message for the SAME interval as the seed -> in-progress, no close, no sweep.
    results = asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", pw.last_interval_begin)))
    assert results == []
    assert wm.dispatched == []


def test_first_roll_sweeps_without_stepping_indicators():
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm)
    atr_before = pw.indicators.atr_14
    # First roll (begin = seed + 300) -> fires the seeded committed[-1] (sweep) but does NOT
    # re-step the indicators (the guard: that candle is already in the seed).
    results = asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", pw.last_interval_begin + 300)))
    assert len(results) == 1                       # TRENDING_POS_NORMAL -> long only
    assert pw.indicators.atr_14 == atr_before      # NOT double-counted


def test_second_roll_steps_indicators():
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm)
    atr_before = pw.indicators.atr_14
    seed = pw.last_interval_begin
    asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", seed + 300)))   # first roll (no step)
    asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", seed + 600)))   # rolls the +300 candle -> steps
    assert pw.indicators.atr_14 != atr_before      # the +300 candle (big TR) was stepped in


def test_hr_wm_012_skips_while_reconnecting():
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm, is_reconnecting=lambda: True)
    atr_before = pw.indicators.atr_14
    results = asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", pw.last_interval_begin + 300)))
    assert results == []
    assert pw.indicators.atr_14 == atr_before      # no step on a partial universe


def test_unknown_pair_ignored():
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm)
    results = asyncio.run(driver.on_ohlc_5m(_frame_5m("DOGE/USD", 1700000000)))
    assert results == []


# --------------------------------------------------------------------------- on_ohlc_60m
def test_60m_drives_htf_close_and_advances_emas():
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm)
    ema20_seed = pw.htf.ema20_1h
    seed60 = pw.last_interval_begin_60
    frame1 = {"data": [{"symbol": "BTC/USD", "interval_begin": seed60 + 3600,
                        "open": "150", "high": "151", "low": "149", "close": "150", "volume": "5"}]}
    frame2 = {"data": [{"symbol": "BTC/USD", "interval_begin": seed60 + 7200,
                        "open": "300", "high": "301", "low": "299", "close": "300", "volume": "5"}]}
    driver.on_ohlc_60m(frame1)   # first 1H roll: fires committed[-1], no EMA step
    driver.on_ohlc_60m(frame2)   # rolls the +3600 (close 150) candle -> steps the 1H EMAs
    assert len(wm.htf_calls) == 2
    assert wm.htf_calls[0][0] == "BTC/USD"
    assert wm.htf_calls[-1][3] == Decimal("59990") and wm.htf_calls[-1][4] == Decimal("60000")  # bbo
    assert pw.htf.ema20_1h != ema20_seed           # EMA advanced on the genuine 1H close
