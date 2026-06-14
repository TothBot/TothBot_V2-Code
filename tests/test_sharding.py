"""S2c2 tests: PATH-2 connection sharding (the subscribe fan-out).

Covers 0500000 dv1_240 sec 2 Image1 SHARD ASSIGNMENT block + sec 7
mod:WS_Manager desc: N_conns = ceil(universe/SYMBOLS_PER_CONN_SAFE=500)
(contract:WM-SHARD-001), shard_index = i % N_conns (alternating-index,
balanced +/-1), global channels (instrument, status) on shard 0 only,
ohlc_5m + ticker partitioned per pair, shard 0 = system-clock shard, and
rule:HR-WM-031 no-automatic-universe-removal (every pair assigned exactly once).
"""

from __future__ import annotations

import pytest

from tothbot.exchange.channels import PublicChannel
from tothbot.exchange.sharding import (
    GLOBAL_CHANNELS,
    PER_PAIR_CHANNELS,
    SYMBOLS_PER_CONN_SAFE,
    ShardPlan,
    n_conns,
    shard_index_for,
)


def _universe(size: int) -> list[str]:
    return [f"PAIR{i}/USD" for i in range(size)]


# -- fixed engineering constant + channel sets --------------------------

def test_symbols_per_conn_safe_matches_diagram():
    assert SYMBOLS_PER_CONN_SAFE == 500  # WM-SHARD-001 fixed engineering constant


def test_global_channels_are_instrument_and_status():
    assert GLOBAL_CHANNELS == (PublicChannel.INSTRUMENT, PublicChannel.STATUS)


def test_per_pair_channels_are_ohlc5m_and_ticker():
    assert PER_PAIR_CHANNELS == (PublicChannel.OHLC_5M, PublicChannel.TICKER)


# -- N_conns = ceil(universe / 500), floored at 1 -----------------------

@pytest.mark.parametrize(
    "size,expected",
    [(0, 1), (1, 1), (499, 1), (500, 1), (501, 2), (700, 2), (1000, 2), (1001, 3)],
)
def test_n_conns_ceiling(size, expected):
    assert n_conns(size) == expected


def test_n_conns_is_at_least_one_for_empty_universe():
    # shard 0 must exist so the global channels have a home (AR-038 startup)
    assert n_conns(0) == 1


def test_n_conns_rejects_negative():
    with pytest.raises(ValueError):
        n_conns(-1)


# -- shard_index = i % N_conns ------------------------------------------

def test_shard_index_is_pair_index_mod_n():
    assert [shard_index_for(i, 3) for i in range(7)] == [0, 1, 2, 0, 1, 2, 0]


def test_shard_index_rejects_bad_args():
    with pytest.raises(ValueError):
        shard_index_for(0, 0)
    with pytest.raises(ValueError):
        shard_index_for(-1, 2)


# -- plan structure: N=2 (the depicted case, ~700 pairs) ----------------

def test_plan_two_shards_when_universe_exceeds_one_conn():
    plan = ShardPlan(_universe(700))
    assert plan.n_conns == 2
    assert len(plan.shards) == 2


def test_pairs_partition_by_index_mod_n():
    plan = ShardPlan(_universe(600))  # N=2
    assert plan.n_conns == 2
    s0, s1 = plan.shards
    # pair i -> shard i%2: even indices on shard 0, odd on shard 1
    assert s0.pairs == tuple(f"PAIR{i}/USD" for i in range(0, 600, 2))
    assert s1.pairs == tuple(f"PAIR{i}/USD" for i in range(1, 600, 2))


def test_shard_sizes_balanced_within_one():
    plan = ShardPlan(_universe(701))  # 701 across 2 shards -> 351 / 350
    sizes = [len(s.pairs) for s in plan.shards]
    assert max(sizes) - min(sizes) <= 1
    assert sorted(sizes) == [350, 351]


# -- channel placement (Image1 shard block) -----------------------------

def test_shard0_carries_global_channels_only():
    plan = ShardPlan(_universe(600))
    s0, s1 = plan.shards
    assert s0.global_channels == (PublicChannel.INSTRUMENT, PublicChannel.STATUS)
    assert s1.global_channels == ()  # shards 1..N-1 carry no global channels


def test_every_shard_carries_per_pair_channels():
    plan = ShardPlan(_universe(600))
    for s in plan.shards:
        assert s.per_pair_channels == (PublicChannel.OHLC_5M, PublicChannel.TICKER)


def test_shard0_is_the_clock_shard():
    plan = ShardPlan(_universe(600))
    assert plan.shards[0].is_clock_shard is True
    assert plan.shards[1].is_clock_shard is False


# -- pair_to_shard_index map (Image6 desc) ------------------------------

def test_pair_to_shard_index_map_round_trips():
    plan = ShardPlan(_universe(600))
    m = plan.pair_to_shard_index
    for i, pair in enumerate(plan.universe):
        assert m[pair] == i % plan.n_conns
        assert plan.shard_for(pair) == i % plan.n_conns


# -- rule:HR-WM-031 no automatic universe removal -----------------------

def test_every_universe_pair_assigned_exactly_once():
    universe = _universe(701)
    plan = ShardPlan(universe)
    assigned = [p for s in plan.shards for p in s.pairs]
    # no pair dropped (anti-AR-070) and none duplicated
    assert sorted(assigned) == sorted(universe)
    assert len(assigned) == len(universe)


def test_subscribe_count_decoupled_from_connection_count():
    # rule:HR-WM-031: subscribe-count = globals + pairs*per_pair_channels,
    # independent of N_conns. Shard 0 has the 2 globals; total subscribes are
    # 2 (globals) + 600 pairs * 2 per-pair channels = 1202.
    plan = ShardPlan(_universe(600))
    total = sum(s.subscribe_count for s in plan.shards)
    assert total == 2 + 600 * 2


# -- single-shard universe (<= 500 pairs) -------------------------------

def test_single_shard_carries_everything():
    plan = ShardPlan(_universe(500))
    assert plan.n_conns == 1
    (s0,) = plan.shards
    assert len(s0.pairs) == 500
    assert s0.global_channels == (PublicChannel.INSTRUMENT, PublicChannel.STATUS)
    assert s0.is_clock_shard is True


def test_empty_universe_still_has_clock_shard_with_globals():
    plan = ShardPlan([])
    assert plan.n_conns == 1
    (s0,) = plan.shards
    assert s0.pairs == ()
    assert s0.global_channels == (PublicChannel.INSTRUMENT, PublicChannel.STATUS)
