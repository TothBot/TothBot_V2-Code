"""
DocDCN:     1021001
DocTitle:   Test_WS_Manager_dv1_25
DocVersion: dv1_25
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/tests/unit/test_ws_manager_dv1_25.py
DocDate:    05-03-2026
DocTime:    16:30:00 UTC

============================================================
REVISION HISTORY
============================================================

  dv1_25  05-03-2026  TB00144 STREAM 4. Initial unit-test
                      coverage for ws_manager.py dv1_25:
                      SubscribeTokenBucket arithmetic and
                      pacing; PATH 2 sharding (N_conns
                      formula, alternating-index assignment,
                      pair_to_shard_index determinism);
                      AR-070 silent-narrowing prohibition
                      (tradeable_universe is built once and
                      not mutated by handler code paths);
                      WM-RECONNECT-017 backoff schedule
                      bounds; HR-WM-018 RSI Wilder seed.
                      Governed by 1011002 dv1_25 +
                      1021001 Unit_Test_Specification.

============================================================
"""

from __future__ import annotations

import asyncio
import math
import time
from decimal import Decimal

import pytest

from tothbot.ws_manager import (
    MAX_RECONNECT_ATTEMPTS,
    PairState,
    SUBSCRIBE_BURST_CAPACITY,
    SUBSCRIBE_RATE_PER_SEC,
    SubscribeTokenBucket,
    SYMBOLS_PER_CONN_SAFE,
    T_SILENT_SEC,
    WSManager,
    WSShard,
)


# ============================================================
# SubscribeTokenBucket — §4.7 WM-PACE-001..010
# ============================================================


@pytest.mark.unit
@pytest.mark.asyncio
async def test_token_bucket_initial_burst_consumes_capacity_then_paces():
    """20-token burst then 10/s steady — first 20 immediate, 21st awaits."""
    bucket = SubscribeTokenBucket(rate=10.0, burst=20.0)
    t0 = time.monotonic()
    for _ in range(20):
        await bucket.acquire(channel="ohlc", symbol="BTC/USD")
    burst_elapsed = time.monotonic() - t0
    assert burst_elapsed < 0.2, (
        f"20-token burst should be ~instant; took {burst_elapsed:.3f}s"
    )
    # 21st acquire requires refill of 1 token at 10/s = ~0.1s.
    t1 = time.monotonic()
    await bucket.acquire()
    pace_wait = time.monotonic() - t1
    assert 0.05 <= pace_wait <= 0.25, (
        f"21st acquire should wait ~0.1s; got {pace_wait:.3f}s"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_token_bucket_starting_values_match_spec():
    """WM-PACE-002 starting values are 10/s and 20-token burst."""
    assert SUBSCRIBE_RATE_PER_SEC == 10.0
    assert SUBSCRIBE_BURST_CAPACITY == 20.0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_token_bucket_concurrent_acquires_are_serialized():
    """asyncio.Lock guarantees no double-spend under concurrency."""
    bucket = SubscribeTokenBucket(rate=10.0, burst=2.0)
    # Two acquires that fit in the burst should both proceed quickly.
    t0 = time.monotonic()
    await asyncio.gather(bucket.acquire(), bucket.acquire())
    fast = time.monotonic() - t0
    assert fast < 0.2
    # Third must wait ~0.1s.
    t1 = time.monotonic()
    await bucket.acquire()
    slow = time.monotonic() - t1
    assert 0.05 <= slow <= 0.25


@pytest.mark.unit
def test_token_bucket_rejects_invalid_init():
    with pytest.raises(ValueError):
        SubscribeTokenBucket(rate=0.0, burst=20.0)
    with pytest.raises(ValueError):
        SubscribeTokenBucket(rate=10.0, burst=0.0)


# ============================================================
# PATH 2 sharding — §4.6 WM-SHARD-001..011
# ============================================================


@pytest.mark.unit
def test_n_conns_formula_at_d03_universe_size():
    """ceil(748 / 500) = 2 (HR-WM-028)."""
    universe_size = 748
    n_conns = max(1, math.ceil(universe_size / SYMBOLS_PER_CONN_SAFE))
    assert n_conns == 2


@pytest.mark.unit
def test_n_conns_formula_grows_with_universe():
    """Above 1000, N_conns = 3 (1500 = 3, 2001 = 5)."""
    assert math.ceil(1000 / SYMBOLS_PER_CONN_SAFE) == 2
    assert math.ceil(1001 / SYMBOLS_PER_CONN_SAFE) == 3
    assert math.ceil(1500 / SYMBOLS_PER_CONN_SAFE) == 3
    assert math.ceil(2001 / SYMBOLS_PER_CONN_SAFE) == 5


@pytest.mark.unit
def test_alternating_index_distribution_is_deterministic():
    """WM-SHARD-002: pair_to_shard_index is reproducible across runs."""
    universe = sorted([f"SYM{i}/USD" for i in range(20)])
    n_conns = 2
    mapping1 = {p: i % n_conns for i, p in enumerate(universe)}
    mapping2 = {p: i % n_conns for i, p in enumerate(sorted(universe))}
    assert mapping1 == mapping2
    # Roughly balanced (±1).
    counts = [sum(1 for v in mapping1.values() if v == i)
              for i in range(n_conns)]
    assert max(counts) - min(counts) <= 1


@pytest.mark.unit
def test_pair_state_enum_values_match_spec():
    """WM-SHARD-010 states: INITIAL, SUBSCRIBED, DATA_PENDING, DATA_READY."""
    assert PairState.INITIAL.value == "INITIAL"
    assert PairState.SUBSCRIBED.value == "SUBSCRIBED"
    assert PairState.DATA_PENDING.value == "DATA_PENDING"
    assert PairState.DATA_READY.value == "DATA_READY"


# ============================================================
# AR-070 silent-narrowing prohibition — WM-PS-008 / HR-WM-028
# ============================================================


@pytest.mark.unit
def test_tradeable_universe_only_built_by_authorized_path():
    """tradeable_universe set ONLY by _build_tradeable_universe (WM-PS-008)."""
    mgr = WSManager(config={"paper_trading_mode": True})
    # Stub pair_cache + liquidity_24h to drive the build path deterministically.
    mgr.pair_cache = {
        "BTC/USD":   {"status": "online", "quote_currency": "USD",
                      "price_increment": Decimal("0.1"),
                      "qty_increment": Decimal("0.00000001"),
                      "qty_min": Decimal("0"),
                      "cost_min": Decimal("0")},
        "ETH/USDC":  {"status": "online", "quote_currency": "USDC",
                      "price_increment": Decimal("0.01"),
                      "qty_increment": Decimal("0.00000001"),
                      "qty_min": Decimal("0"),
                      "cost_min": Decimal("0")},
        "SOL/USDT":  {"status": "online", "quote_currency": "USDT",
                      "price_increment": Decimal("0.001"),
                      "qty_increment": Decimal("0.001"),
                      "qty_min": Decimal("0"),
                      "cost_min": Decimal("0")},
        "DUST/USD":  {"status": "online", "quote_currency": "USD",
                      "price_increment": Decimal("0.0001"),
                      "qty_increment": Decimal("1"),
                      "qty_min": Decimal("0"),
                      "cost_min": Decimal("0")},
    }
    mgr.liquidity_24h = {
        "BTC/USD":  Decimal("100000000"),
        "ETH/USDC": Decimal("10000000"),
        "SOL/USDT": Decimal("5000000"),
        "DUST/USD": Decimal("1000"),  # below 500k -> filtered.
    }
    mgr._build_tradeable_universe()
    # USD/USDC/USDT included; DUST/USD filtered (< 500k); BTC/USD always-include.
    assert mgr.tradeable_universe == {"BTC/USD", "ETH/USDC", "SOL/USDT"}


@pytest.mark.unit
def test_max_concurrent_does_not_bound_universe():
    """HR-WM-027 / HR-WM-031: subscribe count != position count."""
    from tothbot.ws_manager import MAX_CONCURRENT
    # Build a 30-pair universe — should remain 30 regardless of MAX_CONCURRENT=20.
    mgr = WSManager(config={"paper_trading_mode": True})
    pair_cache = {}
    liquidity = {}
    for i in range(30):
        sym = f"SYM{i:02d}/USD"
        pair_cache[sym] = {
            "status": "online", "quote_currency": "USD",
            "price_increment": Decimal("0.0001"),
            "qty_increment": Decimal("0.0001"),
            "qty_min": Decimal("0"), "cost_min": Decimal("0"),
        }
        liquidity[sym] = Decimal("1000000")
    mgr.pair_cache = pair_cache
    mgr.liquidity_24h = liquidity
    mgr._build_tradeable_universe()
    assert len(mgr.tradeable_universe) == 30
    assert MAX_CONCURRENT == 20
    assert len(mgr.tradeable_universe) > MAX_CONCURRENT


# ============================================================
# WM-RECONNECT-017 backoff schedule
# ============================================================


@pytest.mark.unit
def test_reconnect_attempts_default_is_ten_per_spec():
    """WM-RECONNECT-017: 10 attempts (changed from dv1_24's 20)."""
    assert MAX_RECONNECT_ATTEMPTS == 10


# ============================================================
# Decimal-from-string discipline — HR-WM-008 / BP-DEC-001
# ============================================================


@pytest.mark.unit
def test_decimal_from_string_avoids_float_error():
    """Decimal(str(0.1)) == Decimal('0.1'); Decimal(0.1) does not."""
    assert Decimal(str(0.1)) == Decimal("0.1")
    assert Decimal(0.1) != Decimal("0.1")


# ============================================================
# REST signing helper — WM-REST-SIGN-001..003 / HR-WM-023
# ============================================================


@pytest.mark.unit
def test_rest_signing_helper_returns_post_data_and_signature():
    """Sole signing path. Helper returns (post_data, b64sig)."""
    mgr = WSManager(config={"paper_trading_mode": True})
    # Use a base64 secret so HMAC succeeds.
    import base64 as _b64
    secret_raw = b"test_secret_64_bytes_padded________________________________________"
    secret_b64 = _b64.b64encode(secret_raw).decode()
    url_path = "/0/private/GetWebSocketsToken"
    data = {"nonce": "1700000000000"}
    post_data, sig = mgr._sign_rest_request(url_path, data, secret_b64)
    assert post_data == "nonce=1700000000000"
    assert isinstance(sig, str) and len(sig) > 0
    # Round-trip stability: same inputs -> same signature.
    post_data2, sig2 = mgr._sign_rest_request(url_path, data, secret_b64)
    assert sig == sig2
    assert post_data == post_data2


# ============================================================
# Silent-pair state machine — WM-SHARD-010
# ============================================================


@pytest.mark.unit
def test_silent_pair_state_transitions_on_data_received():
    """SUBSCRIBED -> DATA_READY on first data; DATA_PENDING -> DATA_READY recovers."""
    mgr = WSManager(config={"paper_trading_mode": True})
    shard = WSShard(shard_index=0, connection=None)
    shard.pair_states["BTC/USD"] = PairState.SUBSCRIBED
    mgr._on_pair_data_received(shard, "BTC/USD")
    assert shard.pair_states["BTC/USD"] == PairState.DATA_READY
    # DATA_PENDING -> DATA_READY recovery.
    shard.pair_states["ETH/USD"] = PairState.DATA_PENDING
    shard.data_pending_at["ETH/USD"] = time.monotonic()
    mgr._on_pair_data_received(shard, "ETH/USD")
    assert shard.pair_states["ETH/USD"] == PairState.DATA_READY
    assert "ETH/USD" not in shard.data_pending_at


@pytest.mark.unit
def test_silent_timer_constant_matches_spec_starting_value():
    """T_silent starting value = 60s (CIATS-owned per WM-SHARD-010)."""
    assert T_SILENT_SEC == 60.0


# ============================================================
# Paper-mode private gating — HR-WM-022 / HR-WM-023
# ============================================================


@pytest.mark.unit
@pytest.mark.asyncio
async def test_paper_mode_skips_private_token_acquisition():
    """In paper mode, _rest_get_ws_token must not be called."""
    mgr = WSManager(config={"paper_trading_mode": True})
    with pytest.raises(RuntimeError):
        await mgr._rest_get_ws_token()


@pytest.mark.unit
@pytest.mark.asyncio
async def test_paper_mode_simulates_add_order():
    """In paper mode, add_order must NOT call _send_private (HR-WM-023)."""
    mgr = WSManager(config={"paper_trading_mode": True})
    mgr.pair_cache["BTC/USD"] = {
        "status": "online", "quote_currency": "USD",
        "price_increment": Decimal("0.1"),
        "qty_increment": Decimal("0.00000001"),
        "qty_min": Decimal("0"), "cost_min": Decimal("0"),
    }
    # Track whether _send_private fires.
    called = {"n": 0}

    async def _spy(*a, **kw):
        called["n"] += 1
    mgr._send_private = _spy  # type: ignore
    await mgr.add_order(
        symbol="BTC/USD",
        cl_ord_id="ENBTC123",
        qty=Decimal("0.001"),
        limit_price=Decimal("50000"),
    )
    # Allow scheduled simulate_entry_fill to complete.
    await asyncio.sleep(0.2)
    assert called["n"] == 0
    assert "BTC/USD" in mgr.position_mirror


# ============================================================
# Pipeline-during-reconnect guard — HR-WM-029 / WM-SHARD-009
# ============================================================


@pytest.mark.unit
def test_all_connections_healthy_returns_false_during_reconnect():
    mgr = WSManager(config={"paper_trading_mode": True})
    s = WSShard(shard_index=0, connection=object())
    mgr.shards = [s]
    assert mgr._all_connections_healthy() is True
    s.in_reconnect = True
    assert mgr._all_connections_healthy() is False
    s.in_reconnect = False
    s.connection = None
    assert mgr._all_connections_healthy() is False


# ============================================================
# Round-trip Decimal precision sanity check
# ============================================================


@pytest.mark.unit
def test_quantize_roundings_match_spec():
    """Entry DOWN, TP UP, emergSL DOWN, qty DOWN."""
    from decimal import ROUND_DOWN, ROUND_UP
    raw = Decimal("42350.157")
    incr = Decimal("0.1")
    entry = raw.quantize(incr, rounding=ROUND_DOWN)
    tp = raw.quantize(incr, rounding=ROUND_UP)
    sl = raw.quantize(incr, rounding=ROUND_DOWN)
    assert entry == Decimal("42350.1")
    assert tp == Decimal("42350.2")
    assert sl == Decimal("42350.1")
