"""LiveProviders assembly tests (pipeline/providers.py) + the sweep ProviderNotReady skip.

Covers make_live_providers wiring the InstrumentCache/BboCache/LiquidityCache + CR-06 into the
LiveProviders callables, the ProviderNotReady raised on a cache miss (instrument / liquidity / bbo),
the injected CIATS-seed + ws_state pass-through, the cl_ord_id + now+5s deadline generators, and
sweep_pair skipping a (pair, side) when a provider is not ready.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

import pytest

from tothbot.exchange.bbo_cache import BboCache
from tothbot.exchange.instrument_cache import InstrumentCache
from tothbot.exchange.liquidity_cache import LiquidityCache
from tothbot.exchange.position_mirror import PositionSide
from tothbot.exchange.silent_pair import SilentPairMachine
from tothbot.pipeline.providers import (
    WS_STATE_SUBSCRIBED,
    make_deadline,
    make_live_providers,
    make_ws_state_provider,
    new_cl_ord_id,
)
from tothbot.pipeline.sweep import ProviderNotReady, sweep_pair
from tothbot.regime.taxonomy import Regime
from tothbot.exchange.candle_close import CommittedCandle


# --------------------------------------------------------------------------- cache fixtures
def _instr_cache(symbol="BTC/USD", status="online", marginable=True):
    cache = InstrumentCache()
    cache.ingest({"data": {"pairs": [{"symbol": symbol, "status": status, "marginable": marginable,
                                      "qty_min": "0.0001", "cost_min": "0.5",
                                      "price_increment": "0.1", "qty_increment": "0.00000001"}]}})
    return cache


def _bbo_cache(symbol="BTC/USD"):
    cache = BboCache()
    cache.ingest({"data": [{"symbol": symbol, "bid": "59990", "ask": "60000"}]})
    return cache


def _liq_cache(symbol="BTC/USD", vol="600000"):
    cache = LiquidityCache()
    cache.put(symbol, vol, at=0.0)
    return cache


def _providers(instrument_cache=None, bbo_cache=None, liquidity_cache=None):
    return make_live_providers(
        instrument_cache=instrument_cache or _instr_cache(),
        bbo_cache=bbo_cache or _bbo_cache(),
        liquidity_cache=liquidity_cache or _liq_cache(),
        expected_reward=lambda s, r: Decimal("0.05"),
        mpp_abs_cap_pct=lambda s, side: Decimal("0.01"),
        ws_state=lambda s: "Subscribed",
        now_utc=lambda: datetime(2026, 6, 15, 7, 30, 0, tzinfo=timezone.utc),
    )


# --------------------------------------------------------------------------- provider wiring
def test_instrument_provider_combines_caches():
    p = _providers()
    assert p.instrument("BTC/USD") == ("online", True, Decimal("600000"))


def test_bbo_provider_reads_cache():
    assert _providers().bbo("BTC/USD") == (Decimal("59990"), Decimal("60000"))


def test_base_size_uses_cr06_over_cached_minimums():
    # cost_min 0.5, qty_min 0.0001, entry 60000 -> max(50, 5*max(0.5, 6)) = 50 (floor)
    assert _providers().base_per_trade_size("BTC/USD", PositionSide.LONG, "60000") == Decimal("50")


def test_injected_seeds_and_ws_state_passthrough():
    p = _providers()
    assert p.expected_reward("BTC/USD", Regime.TRENDING_POS_NORMAL) == Decimal("0.05")
    assert p.mpp_abs_cap_pct("BTC/USD", PositionSide.SHORT) == Decimal("0.01")
    assert p.ws_state("BTC/USD") == "Subscribed"
    assert p.semaphore_locked(PositionSide.LONG) is False


# --------------------------------------------------------------------------- ProviderNotReady
def test_instrument_miss_raises_not_ready():
    p = _providers(instrument_cache=InstrumentCache())
    with pytest.raises(ProviderNotReady):
        p.instrument("BTC/USD")


def test_liquidity_miss_raises_not_ready():
    p = _providers(liquidity_cache=LiquidityCache())  # empty
    with pytest.raises(ProviderNotReady):
        p.instrument("BTC/USD")


def test_bbo_miss_raises_not_ready():
    p = _providers(bbo_cache=BboCache())
    with pytest.raises(ProviderNotReady):
        p.bbo("BTC/USD")


# --------------------------------------------------------------------------- generators
def test_cl_ord_id_unique():
    assert new_cl_ord_id() != new_cl_ord_id()


def test_deadline_is_now_plus_offset_iso_z():
    clock = lambda: datetime(2026, 6, 15, 7, 30, 0, tzinfo=timezone.utc)
    assert make_deadline(clock, offset_sec=5)() == "2026-06-15T07:30:05.000Z"


# --------------------------------------------------------------------------- ws_state provider
def _machine(clock):
    return SilentPairMachine(clock=clock)


def test_ws_state_subscribed_after_ack():
    m = _machine(lambda: 0.0)
    m.mark_subscribed(now=0.0)  # INITIAL -> SUBSCRIBED
    provider = make_ws_state_provider({"BTC/USD": m}.get)
    assert provider("BTC/USD") == WS_STATE_SUBSCRIBED


def test_ws_state_subscribed_after_data_ready():
    m = _machine(lambda: 0.0)
    m.mark_subscribed(now=0.0)
    m.mark_data(now=1.0)  # SUBSCRIBED -> DATA_READY
    provider = make_ws_state_provider({"BTC/USD": m}.get)
    assert provider("BTC/USD") == WS_STATE_SUBSCRIBED


def test_ws_state_initial_is_not_subscribed():
    m = _machine(lambda: 0.0)  # never ACKed: INITIAL
    provider = make_ws_state_provider({"BTC/USD": m}.get)
    assert provider("BTC/USD") != WS_STATE_SUBSCRIBED


def test_ws_state_data_pending_is_not_subscribed():
    m = _machine(lambda: 0.0)
    m.mark_subscribed(now=0.0)
    m.evaluate(now=100.0)  # > T_silent (60s) -> DATA_PENDING
    assert m.is_data_pending
    provider = make_ws_state_provider({"BTC/USD": m}.get)
    assert provider("BTC/USD") != WS_STATE_SUBSCRIBED


def test_ws_state_missing_machine_is_not_subscribed():
    provider = make_ws_state_provider({}.get)  # pair not tracked
    assert provider("BTC/USD") != WS_STATE_SUBSCRIBED


# --------------------------------------------------------------------------- sweep skip
class _FakeWM:
    modules = {PositionSide.LONG: SimpleNamespace(portfolio_baseline=Decimal("5000"))}

    def open_positions(self):
        return {}

    def position(self, s):
        return None

    def exit_cooldown_at(self, s, side):
        return None

    def consecutive_loss_count(self, s, side):
        return 0

    def wallet_balance(self, side):
        return Decimal("5000") if side is PositionSide.LONG else None

    async def dispatch_entry(self, *a, **k):
        return True


def test_sweep_skips_when_provider_not_ready():
    # Empty caches -> instrument provider raises ProviderNotReady -> the (pair, side) is skipped.
    providers = _providers(instrument_cache=InstrumentCache())
    warmup = SimpleNamespace(
        indicators=SimpleNamespace(atr_14=Decimal("1000"),
                                   sss_verdict=lambda side: SimpleNamespace(passed=True)),
        htf=SimpleNamespace(close_1h=Decimal("106"), ema20_1h=Decimal("104")),
    )
    cache = SimpleNamespace(get=lambda s: SimpleNamespace(
        regime=Regime.TRENDING_POS_NORMAL, ema20=Decimal("105"), ema50=Decimal("100")))
    candle = CommittedCandle(symbol="BTC/USD", interval_begin=1, open=Decimal("59000"),
                             high=Decimal("60100"), low=Decimal("58900"), close=Decimal("60000"),
                             volume=Decimal("2000"))
    logger = SimpleNamespace(record=lambda *a, **k: None)
    results = asyncio.run(sweep_pair(_FakeWM(), logger, candle=candle, warmup=warmup,
                                     regime_cache=cache, providers=providers))
    assert results == []   # skipped, not crashed
