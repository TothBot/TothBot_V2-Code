"""Capstone WS-wiring tests (pipeline/operational.py): the handler_provider binding + the AR-049
public-data-layer assembly.

Covers make_public_handler_provider (each public channel -> its sole consumer; unknown channel
raises) and assemble_operational (the cold-start sequence: REST warm-up + daily regime + liquidity
probe -> providers -> driver -> the built DataLayer with handlers bound + the SHARED silent-pair
registry behind ws_state). Driven with asyncio.run over fakes - no network, no timers.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from tothbot.ciats.expected_reward import ExpectedRewardStore
from tothbot.ciats.seed_estimators import MppCapStore
from tothbot.config.settings import Mode
from tothbot.exchange.bbo_cache import BboCache
from tothbot.exchange.channels import PrivateChannel, PublicChannel
from tothbot.exchange.instrument_cache import InstrumentCache
from tothbot.exchange.pacing import SubscribeTokenBucket
from tothbot.exchange.position_mirror import PositionSide
from tothbot.exchange.regime_exit import PairStatus
from tothbot.pipeline.operational import (
    assemble_operational,
    make_instrument_handler,
    make_public_handler_provider,
    make_ticker_handler,
)
from tothbot.pipeline.providers import WS_STATE_SUBSCRIBED
from tothbot.regime.taxonomy import Regime
from tothbot.rest.client import OhlcResponse, RestOhlcBar


# --------------------------------------------------------------------------- fakes
def _ohlc_response(n=60, start=100, step=1, span=2, base_time=1700000000, interval_sec=300):
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


def _reversing_daily_response(base_time=1700000000, interval_sec=86400):
    # 55 rising then 30 falling daily bars: classifies + the DEC-124 reward replay finds reversals
    # (a pure monotonic series never reverses -> empty seed), so reward_store actually populates.
    closes = [100 + i * 2 for i in range(55)] + [210 - i * 3 for i in range(1, 31)]
    prev = closes[0]
    bars = []
    for i, c in enumerate(closes):
        o, cc = Decimal(prev), Decimal(c)
        bars.append(RestOhlcBar(time=base_time + i * interval_sec, open=o,
                                high=max(o, cc) + 1, low=min(o, cc) - 1, close=cc, volume=Decimal(1000)))
        prev = c
    forming = RestOhlcBar(time=base_time + len(closes) * interval_sec, open=Decimal(9),
                          high=Decimal(9), low=Decimal(9), close=Decimal(9), volume=Decimal(1))
    return OhlcResponse(committed=tuple(bars), forming=forming, last=bars[-1].time)


class _FakeRest:
    def __init__(self) -> None:
        self.calls: list = []

    async def get_ohlc_data(self, pair, interval, *, since=None):
        self.calls.append((pair, interval))
        if interval == 1440:  # the daily series the regime compute + DEC-124 reward seed read
            return _reversing_daily_response()
        return _ohlc_response(interval_sec=interval * 60)

    async def get_ticker_liquidity(self, pair):
        self.calls.append((pair, "ticker"))
        return Decimal("600000")


class _FakeTransport:
    def __init__(self) -> None:
        self.sent: list = []

    async def send(self, message: dict) -> None:
        self.sent.append(message)

    async def recv(self) -> dict:  # pragma: no cover
        raise AssertionError("recv not exercised")

    async def close(self) -> None:  # pragma: no cover
        pass


def _opener():
    opened: dict[int, list] = {}

    async def open_socket(k: int):
        t = _FakeTransport()
        opened.setdefault(k, []).append(t)
        return t

    return open_socket, opened


class _FakeLogger:
    def __init__(self) -> None:
        self.records: list = []

    def record(self, event, *, module: str = "default") -> None:
        self.records.append((module, event))


class _FakeWM:
    def __init__(self) -> None:
        self.regime_calls: list = []

    def on_regime_classified(self, symbol, classification, *, bid=None, ask=None) -> None:
        self.regime_calls.append(symbol)

    def on_htf_ohlc_close(self, *a, **k) -> None:  # pragma: no cover - not driven here
        pass

    def wallet_balance(self, side):
        return None  # no module wired -> sweep_pair skips each side (no deep wm interface needed)


class _LiveWM(_FakeWM):
    """A live WSManager stand-in: carries is_live + the mirror sole-writer surface the private
    connection binds/feeds (PA-004 div #1)."""

    is_live = True

    def __init__(self) -> None:
        super().__init__()
        self.transmitter = SimpleNamespace(bind=lambda _t: None)
        self.baselines: dict = {}          # ar:AR-052 startup captures (REST-BAL-004 / REST-BAL-008)
        self.short_offset = None           # the periodic reconcile carry-forward (REST-BAL-008)

    def restore_position_mirror(self, _snap):  # pragma: no cover - not driven in the build test
        return []

    def record_execution(self, _event):  # pragma: no cover - not driven in the build test
        pass

    def set_live_portfolio_baseline(self, side, usd) -> None:
        self.baselines.setdefault(side, usd)  # once-only, mirrors the real surface

    def open_positions(self):
        return {}

    def short_equity_reconcile_offset(self):
        return self.short_offset

    def set_short_equity_reconcile_offset(self, offset) -> None:
        self.short_offset = offset


def _private_opener():
    sockets: list = []

    async def open_private():
        t = _FakeTransport()
        sockets.append(t)
        return t

    return open_private, sockets


def _stores():
    # OPS-1: the stores are passed in EMPTY - assemble_operational seeds them from the historical
    # bars the warm-up (5m -> mpp) + daily-regime (1440 -> expected_reward) phases fetch.
    return MppCapStore(), ExpectedRewardStore()


# --------------------------------------------------------------------------- handler_provider
def _fake_driver():
    return SimpleNamespace(
        ohlc_5m_handler=lambda: ("H5",),
        ohlc_60m_handler=lambda: ("H60",),
    )


def test_handler_provider_binds_each_public_channel():
    instr, bbo = InstrumentCache(), BboCache()
    provider = make_public_handler_provider(
        instrument_cache=instr, bbo_cache=bbo, driver=_fake_driver()
    )
    # INSTRUMENT/TICKER route to the cache ingest (asserted by effect - bound methods are not
    # identity-stable in Python, so compare behavior, not the callable object).
    provider(0, PublicChannel.INSTRUMENT)({"data": {"pairs": [
        {"symbol": "BTC/USD", "status": "online", "marginable": True, "qty_min": "0.0001",
         "cost_min": "0.5", "price_increment": "0.1", "qty_increment": "0.00000001"}]}})
    assert instr.get("BTC/USD") is not None
    provider(0, PublicChannel.TICKER)({"data": [{"symbol": "BTC/USD", "bid": "1", "ask": "2"}]})
    assert bbo.bbo("BTC/USD") == (Decimal("1"), Decimal("2"))
    # OHLC channels return the driver's own handlers (the sentinels the fake driver yields).
    assert provider(0, PublicChannel.OHLC_5M) == ("H5",)
    assert provider(0, PublicChannel.OHLC_60M) == ("H60",)
    # STATUS gets the default no-op (callable, returns None).
    assert provider(0, PublicChannel.STATUS)({}) is None


def test_handler_provider_custom_status_handler():
    seen: list = []
    provider = make_public_handler_provider(
        instrument_cache=InstrumentCache(), bbo_cache=BboCache(), driver=_fake_driver(),
        status_handler=lambda f: seen.append(f),
    )
    provider(0, PublicChannel.STATUS)({"x": 1})
    assert seen == [{"x": 1}]


def test_handler_provider_unknown_channel_raises():
    provider = make_public_handler_provider(
        instrument_cache=InstrumentCache(), bbo_cache=BboCache(), driver=_fake_driver()
    )
    with pytest.raises(ValueError):
        provider(0, PrivateChannel.EXECUTIONS)  # never a public-shard channel


# --- ar:AR-040 instrument-status exit wiring (make_instrument_handler) -----------------

class _StatusRecordingWM:
    """A wm stand-in recording on_instrument_status calls, with one open-position symbol."""

    def __init__(self, open_symbols=()):  # noqa: B006
        self._open = set(open_symbols)
        self.status_calls: list = []

    def has_position(self, symbol):
        return symbol in self._open

    def on_instrument_status(self, symbol, pair_status, *, bid=None, ask=None):
        self.status_calls.append((symbol, pair_status, bid, ask))


def _instrument_frame(symbol, status):
    return {"data": {"pairs": [
        {"symbol": symbol, "status": status, "marginable": True, "qty_min": "0.0001",
         "cost_min": "0.5", "price_increment": "0.1", "qty_increment": "0.00000001"}]}}


def test_instrument_handler_drives_on_instrument_status_for_open_position_pair():
    instr = InstrumentCache()
    wm = _StatusRecordingWM(open_symbols={"BTC/USD"})
    handler = make_instrument_handler(
        instr, wm=wm, bbo_provider=lambda s: (Decimal("60000"), Decimal("60010"))
    )
    handler(_instrument_frame("BTC/USD", "limit_only"))
    assert instr.get("BTC/USD").status == "limit_only"        # still ingested into the cache
    assert wm.status_calls == [("BTC/USD", PairStatus.LIMIT_ONLY, Decimal("60000"), Decimal("60010"))]


def test_instrument_handler_skips_pairs_without_open_position():
    instr = InstrumentCache()
    wm = _StatusRecordingWM(open_symbols=set())   # no open position
    handler = make_instrument_handler(instr, wm=wm)
    handler(_instrument_frame("BTC/USD", "limit_only"))
    assert wm.status_calls == []                  # cache updated, but no exit-controller drive


def test_instrument_handler_is_bare_ingest_without_wm():
    instr = InstrumentCache()
    handler = make_instrument_handler(instr)       # no wm -> the bare cache ingest
    out = handler(_instrument_frame("BTC/USD", "online"))
    assert out == ["BTC/USD"] and instr.get("BTC/USD") is not None


# --- the ticker channel drives the L2 MAE detector (make_ticker_handler) ----------------

class _TickerRecordingWM:
    """A wm stand-in recording handle_ticker calls (the sec-12.5 exit detector feed)."""

    def __init__(self) -> None:
        self.ticker_frames: list = []

    def handle_ticker(self, frame):
        self.ticker_frames.append(frame)


def test_ticker_handler_ingests_bbo_and_drives_handle_ticker():
    bbo = BboCache()
    wm = _TickerRecordingWM()
    handler = make_ticker_handler(bbo, wm=wm)
    frame = {"data": [{"symbol": "BTC/USD", "bid": "60000", "ask": "60010"}]}
    handler(frame)
    assert bbo.bbo("BTC/USD") == (Decimal("60000"), Decimal("60010"))  # cache updated (ar:AR-048)
    assert wm.ticker_frames == [frame]                                 # exit detector driven


def test_ticker_handler_is_bare_ingest_without_wm():
    bbo = BboCache()
    handler = make_ticker_handler(bbo)             # no wm -> the bare cache ingest
    handler({"data": [{"symbol": "BTC/USD", "bid": "1", "ask": "2"}]})
    assert bbo.bbo("BTC/USD") == (Decimal("1"), Decimal("2"))


# --------------------------------------------------------------------------- assemble_operational
def _assemble(universe=("BTC/USD", "ETH/USD")):
    rest = _FakeRest()
    open_socket, opened = _opener()
    wm, logger = _FakeWM(), _FakeLogger()
    mpp, reward = _stores()

    async def no_sleep(_s):
        return None

    system = asyncio.run(assemble_operational(
        universe=list(universe),
        rest_client=rest,
        open_socket=open_socket,
        bucket=SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0),
        wm=wm,
        logger=logger,
        mpp_store=mpp,
        reward_store=reward,
        mode=Mode.PAPER,
        now_utc=lambda: datetime(2026, 6, 15, 7, 30, tzinfo=timezone.utc),
        rest_sleep=no_sleep,
        pace_sleep=no_sleep,
    ))
    return system, rest, opened, wm, mpp, reward


def test_assemble_runs_rest_phases_and_builds_layer():
    system, rest, opened, wm, _mpp, _reward = _assemble()
    # Warm-up (5m + 60m per pair) + daily regime (1440 per pair + BTC anchor) + liquidity all ran.
    assert ("BTC/USD", 5) in rest.calls and ("BTC/USD", 60) in rest.calls
    assert ("BTC/USD", 1440) in rest.calls and ("ETH/USD", 1440) in rest.calls
    assert ("BTC/USD", "ticker") in rest.calls
    # The regime cache filled (the warmed pairs are READY) + EC-L1A-002 was offered each pair.
    assert system.regime_cache.get("BTC/USD") is not None
    assert "BTC/USD" in wm.regime_calls
    # The data layer built + the initial paced subscribe was sent on the one shard's socket.
    assert len(system.data_layer.shards) == 1
    assert len(opened[0][0].sent) == system.data_layer.shards[0].assignment.subscribe_count


def test_assemble_seeds_ciats_stores_at_load():
    # OPS-1: the empty mpp + expected_reward stores are seeded in-line from the warm-up 5m series and
    # the daily-regime 1440 series (no separate seeding pass) - CIATS owns the values from load.
    _system, _rest, _opened, _wm, mpp, reward = _assemble()
    assert mpp.get("BTC/USD", PositionSide.LONG) is not None     # DEC-128 Q95 cap seeded from 5m
    assert mpp.get("ETH/USD", PositionSide.SHORT) is not None
    # DEC-124 run-to-reversal seeds for at least one regime (the reversing daily series reverses).
    assert any(reward.get("BTC/USD", r) is not None for r in Regime)


def test_paper_mode_has_no_private_connection():
    # PA-004 div #1: paper NEVER connects the private WS.
    system, *_ = _assemble()
    assert system.private_connection is None


def test_live_mode_builds_private_connection():
    # OPS-2: live mode ALSO assembles the private executions/balances connection (AR-049 steps 5/6).
    rest = _FakeRest()
    open_socket, _opened = _opener()
    open_private, priv_sockets = _private_opener()
    wm, logger = _LiveWM(), _FakeLogger()
    mpp, reward = _stores()

    async def no_sleep(_s):
        return None

    async def acquire_token():
        return "tok-abc"

    system = asyncio.run(assemble_operational(
        universe=["BTC/USD"],
        rest_client=rest,
        open_socket=open_socket,
        bucket=SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0),
        wm=wm,
        logger=logger,
        mpp_store=mpp,
        reward_store=reward,
        mode=Mode.LIVE,
        open_private_socket=open_private,
        acquire_token=acquire_token,
        now_utc=lambda: datetime(2026, 6, 15, 7, 30, tzinfo=timezone.utc),
        rest_sleep=no_sleep,
        pace_sleep=no_sleep,
    ))
    assert system.private_connection is not None
    # The private socket opened and issued the executions subscribe (order_status/snap_orders).
    assert len(priv_sockets) == 1
    first = priv_sockets[0].sent[0]
    assert first["params"]["channel"] == "executions"
    assert first["params"]["token"] == "tok-abc"


def test_live_mode_wires_baselines_and_equity_reconcile():
    # ar:AR-052: the threaded REST drawdown-baseline edges reach _capture_startup_baseline (LONG
    # REST-BAL-004 / SHORT REST-BAL-008), and the SHORT TradeBalance edge ALSO builds the periodic
    # equity reconcile (REST-BAL-008 carry-forward). OperationalSystem.run() would drive its loop.
    rest = _FakeRest()
    open_socket, _opened = _opener()
    open_private, _priv = _private_opener()
    wm, logger = _LiveWM(), _FakeLogger()
    mpp, reward = _stores()

    async def no_sleep(_s):
        return None

    async def acquire_token():
        return "tok-abc"

    async def fetch_account_balance():
        return Decimal("5000")     # REST-BAL-004 long spot USD

    async def fetch_trade_balance():
        return Decimal("4800")     # REST-BAL-008 short margin equity `e`

    system = asyncio.run(assemble_operational(
        universe=["BTC/USD"], rest_client=rest, open_socket=open_socket,
        bucket=SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0),
        wm=wm, logger=logger, mpp_store=mpp, reward_store=reward,
        mode=Mode.LIVE, open_private_socket=open_private, acquire_token=acquire_token,
        fetch_account_balance=fetch_account_balance, fetch_trade_balance=fetch_trade_balance,
        now_utc=lambda: datetime(2026, 6, 15, 7, 30, tzinfo=timezone.utc),
        rest_sleep=no_sleep, pace_sleep=no_sleep,
    ))
    # Both startup baselines captured (the previously-dead edges now reach the capture).
    assert wm.baselines[PositionSide.LONG] == Decimal("5000")
    assert wm.baselines[PositionSide.SHORT] == Decimal("4800")
    # The periodic SHORT-equity reconcile is built (the SHORT TradeBalance edge is wired).
    assert system.equity_reconcile is not None
    # And a poll re-seeds the carry-forward offset (flat short: raw 0-collateral wallet None -> skip;
    # here _LiveWM.wallet_balance is None so the poll cleanly SKIPS, leaving the offset unset).
    assert asyncio.run(system.equity_reconcile.reconcile_once()) is None
    assert wm.short_offset is None  # skipped (wallet not fed) - never a bogus offset


def test_live_mode_without_trade_balance_edge_has_no_reconcile():
    # The reconcile is built ONLY when the SHORT TradeBalance edge is wired (no edge -> no reconcile).
    rest = _FakeRest()
    open_socket, _opened = _opener()
    open_private, _priv = _private_opener()
    mpp, reward = _stores()

    async def no_sleep(_s):
        return None

    async def acquire_token():
        return "tok-abc"

    system = asyncio.run(assemble_operational(
        universe=["BTC/USD"], rest_client=rest, open_socket=open_socket,
        bucket=SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0),
        wm=_LiveWM(), logger=_FakeLogger(), mpp_store=mpp, reward_store=reward,
        mode=Mode.LIVE, open_private_socket=open_private, acquire_token=acquire_token,
        now_utc=lambda: datetime(2026, 6, 15, 7, 30, tzinfo=timezone.utc),
        rest_sleep=no_sleep, pace_sleep=no_sleep,
    ))
    assert system.equity_reconcile is None


def test_paper_mode_has_no_equity_reconcile():
    system, *_ = _assemble()
    assert system.equity_reconcile is None


def test_live_mode_requires_private_edges():
    # Live mode without the private socket opener / token acquire is a wiring error, surfaced.
    rest = _FakeRest()
    open_socket, _ = _opener()
    mpp, reward = _stores()

    async def no_sleep(_s):
        return None

    with pytest.raises(ValueError):
        asyncio.run(assemble_operational(
            universe=["BTC/USD"], rest_client=rest, open_socket=open_socket,
            bucket=SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0),
            wm=_LiveWM(), logger=_FakeLogger(), mpp_store=mpp, reward_store=reward,
            mode=Mode.LIVE, rest_sleep=no_sleep, pace_sleep=no_sleep,
        ))


def test_assemble_binds_handlers_into_dispatch():
    system, *_ = _assemble()
    shard0 = system.data_layer.shards[0]
    # An instrument frame routed through the shard's dispatch populates the shared instrument cache.
    shard0.dispatch.dispatch(PublicChannel.INSTRUMENT, {"data": {"pairs": [
        {"symbol": "BTC/USD", "status": "online", "marginable": True, "qty_min": "0.0001",
         "cost_min": "0.5", "price_increment": "0.1", "qty_increment": "0.00000001"}]}})
    assert system.instrument_cache.get("BTC/USD") is not None
    # A ticker frame populates the shared bbo cache.
    shard0.dispatch.dispatch(PublicChannel.TICKER,
                             {"data": [{"symbol": "BTC/USD", "bid": "59990", "ask": "60000"}]})
    assert system.bbo_cache.bbo("BTC/USD") == (Decimal("59990"), Decimal("60000"))


def test_assemble_shares_silent_pairs_behind_ws_state():
    system, *_ = _assemble()
    # The SAME machine instance backs the shard runtime and the ws_state provider (one registry).
    shard0 = system.data_layer.shards[0]
    for symbol, machine in shard0.silent_pairs.items():
        assert system.silent_pairs[symbol] is machine
    # Subscribe ACK -> SUBSCRIBED -> ws_state reads "Subscribed" for the G1 gate.
    machine = system.silent_pairs["BTC/USD"]
    machine.mark_subscribed(now=0.0)
    assert system.providers.ws_state("BTC/USD") == WS_STATE_SUBSCRIBED
