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
from tothbot.pipeline.operational import (
    assemble_operational,
    make_public_handler_provider,
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


class _FakeRest:
    def __init__(self) -> None:
        self.calls: list = []

    async def get_ohlc_data(self, pair, interval, *, since=None):
        self.calls.append((pair, interval))
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


def _stores():
    mpp = MppCapStore()
    mpp.put("BTC/USD", PositionSide.LONG, "0.01")
    reward = ExpectedRewardStore()
    reward.put("BTC/USD", Regime.TRENDING_POS_NORMAL, "0.05")
    return mpp, reward


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
    return system, rest, opened, wm


def test_assemble_runs_rest_phases_and_builds_layer():
    system, rest, opened, wm = _assemble()
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


def test_assemble_binds_handlers_into_dispatch():
    system, _, _, _ = _assemble()
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
    system, _, _, _ = _assemble()
    # The SAME machine instance backs the shard runtime and the ws_state provider (one registry).
    shard0 = system.data_layer.shards[0]
    for symbol, machine in shard0.silent_pairs.items():
        assert system.silent_pairs[symbol] is machine
    # Subscribe ACK -> SUBSCRIBED -> ws_state reads "Subscribed" for the G1 gate.
    machine = system.silent_pairs["BTC/USD"]
    machine.mark_subscribed(now=0.0)
    assert system.providers.ws_state("BTC/USD") == WS_STATE_SUBSCRIBED
