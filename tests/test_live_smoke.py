"""Live END-TO-END smoke (the capstone): drive the ASSEMBLED organism over a fake public-WS frame
stream and prove a full decide->size->dispatch tick + the TRADE_CLOSE -> CiatsPool learning close.

Source: 0500000 dv1_250 ar:AR-049 (the cold-start assembly) + Image2 (the 8-gate pipeline) + the
contract:OHLC_5m_System_Clock tick + sec 7 (mod:Logger Stream-2 -> the per-module CiatsPool). The unit
tests cover each piece in isolation; THIS test runs the whole thing wired together by assemble_
operational: a frame stream (INSTRUMENT snapshot -> TICKER -> a 5m close -> a 1H close) flows through
the assembled data layer + driver, drives one (pair, side) candidate through every gate, and dispatches
the entry into the module wallet - paper mode, no network, no timers (asyncio.run over fakes).

DETERMINISM: the public-WS frames + the warm-up/daily REST series are crafted so the pair classifies
TRENDING_POS (long permitted), the warmed 5m indicators yield a passing long SSS (a pullback-in-uptrend
- RSI in (30,50) with EMA9 > EMA21 + a closing volume spike), and the candidate clears the sacred 1:1.5
R:R floor. The two CIATS seed values the A1 floor reads (expected_reward DEC-124, mpp DEC-128) are put
into their stores as known values after the load seeding (the store IS the CIATS-owned source - this
fixes the gate input, it does not bypass any wiring). The TRADE_CLOSE learning close is exercised with
a schema-valid 23-field record routed through the per-module Logger Stream-2 sink + the CiatsConductor.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from tothbot.ciats.conductor import CiatsConductor
from tothbot.ciats.expected_reward import ExpectedRewardStore
from tothbot.ciats.parameter_store import ParameterStore
from tothbot.ciats.pool import CiatsPool
from tothbot.ciats.regime_library import RegimeLibrary
from tothbot.ciats.seed_estimators import MppCapStore
from tothbot.config.settings import Mode
from tothbot.exchange.channels import PublicChannel
from tothbot.exchange.pacing import SubscribeTokenBucket
from tothbot.exchange.position_mirror import PositionSide
from tothbot.execution.exit_controller import ExitReason, TradeClose
from tothbot.pipeline.live_driver import make_ciats_sink
from tothbot.pipeline.operational import assemble_operational
from tothbot.recorder.logger import Logger
from tothbot.regime.taxonomy import Regime
from tothbot.rest.client import OhlcResponse, RestOhlcBar


# --------------------------------------------------------------------------- crafted REST series
def _resp(closes, *, base=1700000000, interval, last_vol=None, vol=1000):
    committed = []
    for i, c in enumerate(closes):
        o = Decimal(closes[i - 1] if i else c)
        cc = Decimal(c)
        v = Decimal(last_vol) if (last_vol is not None and i == len(closes) - 1) else Decimal(vol)
        committed.append(RestOhlcBar(time=base + i * interval, open=o, high=max(o, cc) + 2,
                                     low=min(o, cc) - 2, close=cc, volume=v))
    forming = RestOhlcBar(time=base + len(closes) * interval, open=Decimal(9), high=Decimal(9),
                          low=Decimal(9), close=Decimal(9), volume=Decimal(1))
    return OhlcResponse(committed=tuple(committed), forming=forming, last=committed[-1].time)


def _daily_closes():
    # net-rising with periodic dips -> TRENDING_POS + run-to-reversal reversals (DEC-124 seeds).
    out, v = [], 100
    for i in range(85):
        v += 6 if i % 5 else -4
        out.append(v)
    return out


def _5m_closes():
    # a 40-bar steep uptrend (EMA9 >> EMA21) then an 18-bar mild pullback (RSI down into (30,50)).
    out, v = [], 100
    for _ in range(40):
        v += 5
        out.append(v)
    for _ in range(18):
        v -= 2
        out.append(v)
    return out


class _FakeRest:
    async def get_ohlc_data(self, pair, interval, *, since=None):
        if interval == 1440:
            return _resp(_daily_closes(), interval=86400)
        if interval == 5:
            return _resp(_5m_closes(), interval=300, last_vol=5000)
        return _resp([100 + i for i in range(60)], interval=3600)

    async def get_ticker_liquidity(self, pair):
        return Decimal("600000")


class _FakeTransport:
    def __init__(self):
        self.sent = []

    async def send(self, m):
        self.sent.append(m)

    async def recv(self):  # pragma: no cover
        raise AssertionError

    async def close(self):  # pragma: no cover
        pass


def _opener():
    async def open_socket(_k):
        return _FakeTransport()
    return open_socket


class _TradeWM:
    """A paper WSManager stand-in with the full decide->size->dispatch surface + the HTF close seam."""

    is_live = False

    def __init__(self):
        self._w = {PositionSide.LONG: Decimal("5000"), PositionSide.SHORT: Decimal("5000")}
        self.modules = {s: SimpleNamespace(portfolio_baseline=Decimal("5000")) for s in self._w}
        self.dispatched: list = []
        self.htf_calls: list = []
        self.regime_calls: list = []

    def open_positions(self):
        return {}

    def position(self, _s):
        return None

    def exit_cooldown_at(self, _s, _side):
        return None

    def consecutive_loss_count(self, _s, _side):
        return 0

    def wallet_balance(self, side):
        return self._w.get(side)

    async def dispatch_entry(self, side, symbol, **kw):
        self.dispatched.append((side, symbol))
        return True

    def on_regime_classified(self, symbol, *a, **k):
        self.regime_calls.append(symbol)

    def on_htf_ohlc_close(self, symbol, ema_s, ema_l, *, bid=None, ask=None, **_):
        self.htf_calls.append((symbol, bid, ask))


def _trade_close(symbol="BTC/USD", net="120"):
    """A schema-valid 23-field TRADE_CLOSE (the real exit_controller dataclass) for the corpus."""
    return TradeClose(
        symbol=symbol,
        entry_fill_price=Decimal("60000"), exit_price=Decimal("66000"),
        exit_reason=ExitReason.HTF_REGIME_REVERSAL,
        fees_entry_usd=Decimal("7.8"), fees_exit_usd=Decimal("8.58"), fees_total_usd=Decimal("16.38"),
        net_pl_usd=Decimal(net), net_gain_usd=Decimal(net), net_loss_usd=Decimal("0"),
    )


def _assemble():
    """Assemble the paper organism over the crafted fakes + a tradeable wm + a real Logger, then fix
    the CIATS A1-floor inputs (expected_reward / mpp) to known seeds so the gate is deterministic."""
    rest = _FakeRest()
    wm, logger = _TradeWM(), Logger()
    mpp, reward = MppCapStore(), ExpectedRewardStore()

    async def no_sleep(_s):
        return None

    system = asyncio.run(assemble_operational(
        universe=["BTC/USD"], rest_client=rest, open_socket=_opener(),
        bucket=SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0),
        wm=wm, logger=logger, mpp_store=mpp, reward_store=reward, mode=Mode.PAPER,
        now_utc=lambda: datetime(2026, 6, 15, 7, 30, tzinfo=timezone.utc),
        rest_sleep=no_sleep, pace_sleep=no_sleep,
    ))
    # The store IS the CIATS-owned source: fix the A1-floor inputs to known values (DEC-124 / DEC-128).
    for r in Regime:
        reward.put("BTC/USD", r, Decimal("0.5"))
    mpp.put("BTC/USD", PositionSide.LONG, Decimal("0.01"))
    mpp.put("BTC/USD", PositionSide.SHORT, Decimal("0.01"))
    # The pair is Subscribed (the receive loop's ACK) so gate:G1 PASSes; populate the snapshot caches.
    system.silent_pairs["BTC/USD"].mark_subscribed(now=0.0)
    shard0 = system.data_layer.shards[0]
    shard0.dispatch.dispatch(PublicChannel.INSTRUMENT, {"data": {"pairs": [
        {"symbol": "BTC/USD", "status": "online", "marginable": True, "qty_min": "0.0001",
         "cost_min": "0.5", "price_increment": "0.1", "qty_increment": "0.00000001"}]}})
    shard0.dispatch.dispatch(PublicChannel.TICKER,
                             {"data": [{"symbol": "BTC/USD", "bid": "176", "ask": "177"}]})
    return system, wm, logger


# --------------------------------------------------------------------------- the smoke
def test_frame_stream_populates_the_snapshot_caches():
    system, _wm, _logger = _assemble()
    # The INSTRUMENT + TICKER frames flowed through the assembled shard dispatch into the shared caches.
    assert system.instrument_cache.get("BTC/USD") is not None
    assert system.bbo_cache.bbo("BTC/USD") == (Decimal("176"), Decimal("177"))
    # The pair classified into a long-permitting trending regime during the cold-start.
    assert system.regime_cache.get("BTC/USD").regime in (
        Regime.TRENDING_POS_NORMAL, Regime.TRENDING_POS_ELEVATED
    )


def test_5m_close_runs_a_full_decide_size_dispatch_tick():
    system, wm, logger = _assemble()
    pw = system.warmups["BTC/USD"]
    # A 5m close (the contract:OHLC_5m_System_Clock tick) drives the whole pipeline on the warmed pair.
    frame = {"data": [{"symbol": "BTC/USD", "interval_begin": pw.last_interval_begin + 300,
                       "open": "264", "high": "266", "low": "262", "close": "264", "volume": "5000"}]}
    results = asyncio.run(system.driver.on_ohlc_5m(frame))
    assert len(results) == 1                                   # TRENDING_POS -> long only
    assert results[0].outcome.accepted is True
    assert results[0].outcome.reason == "G8_SIZED"
    assert results[0].dispatched is True and results[0].filled is True
    # The entry dispatched into the LONG module wallet; the tick logged to mod:Logger Stream-1.
    assert wm.dispatched == [(PositionSide.LONG, "BTC/USD")]
    assert logger.operational  # the pipeline outcome reached Stream-1


def test_1h_close_drives_htf_maintenance():
    system, wm, _logger = _assemble()
    pw = system.warmups["BTC/USD"]
    seed60 = pw.last_interval_begin_60
    # Two 1H rolls: the first fires committed[-1] (no EMA step), the second steps the 1H EMAs (AR-044).
    for k in (1, 2):
        system.driver.on_ohlc_60m({"data": [{"symbol": "BTC/USD", "interval_begin": seed60 + 3600 * k,
                                              "open": "150", "high": "151", "low": "149",
                                              "close": "150", "volume": "5"}]})
    # EC-L1A-001: wm.on_htf_ohlc_close was driven with the live bbo from the ticker frame.
    assert wm.htf_calls
    assert wm.htf_calls[-1] == ("BTC/USD", Decimal("176"), Decimal("177"))


def test_trade_close_closes_the_ciats_learning_loop():
    system, _wm, logger = _assemble()
    # The per-module learning close (sec 7): a schema-valid TRADE_CLOSE -> Stream-2 corpus + the pool,
    # and the CiatsConductor (the per-module loop) ingests it off that corpus.
    pool = CiatsPool()
    conductor = CiatsConductor(
        module="long", pool=CiatsPool(), regime_library=RegimeLibrary(), parameter_store=ParameterStore(),
    )
    sink = make_ciats_sink(logger, "long", pool)
    tc = _trade_close(net="120")
    sink(tc)                                   # -> Logger Stream-2 corpus + pool ingest
    conductor.ingest_close(tc, regime=Regime.TRENDING_POS_NORMAL)
    assert pool.trade_count == 1
    assert len(logger.corpus_for("long")) == 1            # the 23-field record entered the corpus
    assert conductor.trade_count == 1                     # the conductor's per-module pool learned it
