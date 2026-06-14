"""PATH-2 connection sharding: split the universe across N WS connections.

Source: 0500000 dv1_240 sec 2 Image1 (the SHARD ASSIGNMENT block,
contract:WM-SHARD-001/002) + sec 7 mod:WS_Manager desc (rule:HR-WM-028
PATH-2 sharding, rule:HR-WM-029 shard independence, rule:HR-WM-031
subscribe-count decoupling / no automatic universe removal, anti-ar:AR-070).

Kraken's public WS v2 caps the symbols one connection can safely carry, so the
monitored universe (every USD/USDC/USDT spot pair per decision:D-03, ~500-700
pairs) is fanned out across N shards. The split is a PURE partition function:

  SYMBOLS_PER_CONN_SAFE = 500   fixed engineering constant (WM-SHARD-001)
  N_conns      = ceil(universe_size / SYMBOLS_PER_CONN_SAFE)   (>= 1 shard)
  shard_index  = i % N_conns    pair i's shard (alternating-index, balanced +/-1)

Channel placement (Image1 shard block):
  - GLOBAL channels (instrument, status) subscribe ONCE, on shard 0 only.
  - PER-PAIR channels (ohlc_5m, ticker) follow each pair to its shard: shard k
    carries the per-pair channels for every pair whose universe index i has
    i % N_conns == k. Shard 0 additionally carries the global channels and so
    is the SYSTEM-CLOCK shard (the ohlc_5m partition that fires the pipeline).

Assigning pair i to shard (i % N_conns) over an ordered universe distributes
the pairs round-robin, so shard sizes differ by at most 1 (balanced +/-1).

rule:HR-WM-029 shard independence: each shard owns its own connection and
reconnects independently (the per-shard reconnect coordinator lands in S2c3);
the partition here gives each shard its disjoint pair set.

rule:HR-WM-031 (anti-ar:AR-070): subscribe-count is decoupled from
max-concurrent and there is NO automatic universe removal - the plan assigns
EVERY universe pair to exactly one shard and never drops one. A pair that goes
silent is handled by the silent-pair state machine (S2c3), never by eviction.

This module is PURE (no socket, no asyncio): a partition function plus the
pair->shard assignment map. The WS client consumes the plan to open N
connections and pace their subscribes (see pacing.py); that I/O is the edge.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from .channels import PublicChannel

# --- fixed engineering constant (WM-SHARD-001; not CIATS-owned) ---------------
SYMBOLS_PER_CONN_SAFE = 500  # max symbols one shard safely carries (WM-SHARD-001)

# Global channels: a single subscription serves the whole universe, so they live
# on shard 0 only (Image1: instrument "global, shard 0 only"; status "global,
# AR-038 startup"). instrument feeds Pre-Gate-1 per-pair status; status carries
# the Kraken engine state.
GLOBAL_CHANNELS: tuple[PublicChannel, ...] = (
    PublicChannel.INSTRUMENT,
    PublicChannel.STATUS,
)

# Per-pair channels: each pair's subscriptions live on that pair's shard
# (Image1 partitions ohlc(5m) + ticker by i%N). ohlc_5m on shard 0's partition
# is the system clock.
PER_PAIR_CHANNELS: tuple[PublicChannel, ...] = (
    PublicChannel.OHLC_5M,
    PublicChannel.TICKER,
)


def n_conns(universe_size: int) -> int:
    """N_conns = ceil(universe_size / SYMBOLS_PER_CONN_SAFE), floored at 1.

    Integer-only ceil (no float). At least one shard always exists so the
    global channels (instrument, status) have a home even before any pair is
    assigned (AR-038 startup needs the status channel regardless).
    """
    if universe_size < 0:
        raise ValueError(f"universe_size must be >= 0, got {universe_size}")
    return max(1, -(-universe_size // SYMBOLS_PER_CONN_SAFE))


def shard_index_for(pair_index: int, n: int) -> int:
    """shard_index = i % N_conns (the alternating-index assignment, balanced +/-1)."""
    if n < 1:
        raise ValueError(f"n must be >= 1, got {n}")
    if pair_index < 0:
        raise ValueError(f"pair_index must be >= 0, got {pair_index}")
    return pair_index % n


@dataclass(frozen=True)
class ShardAssignment:
    """One shard's subscription plan: its pairs and the channels it subscribes."""

    shard_index: int
    pairs: tuple[str, ...]                     # pairs whose per-pair channels this shard owns
    global_channels: tuple[PublicChannel, ...]  # instrument+status on shard 0, else empty
    per_pair_channels: tuple[PublicChannel, ...]  # ohlc_5m + ticker (every shard)

    @property
    def is_clock_shard(self) -> bool:
        """Shard 0 carries the global channels and the ohlc_5m partition that is
        the system clock."""
        return self.shard_index == 0

    @property
    def subscribe_count(self) -> int:
        """Number of subscribe RPCs this shard issues: one per global channel
        plus one per (pair x per-pair channel). Decoupled from the connection
        count (rule:HR-WM-031)."""
        return len(self.global_channels) + len(self.pairs) * len(self.per_pair_channels)


class ShardPlan:
    """The PATH-2 fan-out over an ordered universe: N shards + the pair->shard map.

    Built from the ordered universe (the AR-070/D-03 pair list). Pair at index i
    is assigned to shard (i % N_conns); shard 0 also carries the global channels.
    Every universe pair is assigned exactly once (rule:HR-WM-031: no automatic
    universe removal).
    """

    def __init__(self, universe: Sequence[str]) -> None:
        self._universe: tuple[str, ...] = tuple(universe)
        self._n = n_conns(len(self._universe))

        buckets: list[list[str]] = [[] for _ in range(self._n)]
        pair_to_shard: dict[str, int] = {}
        for i, pair in enumerate(self._universe):
            k = i % self._n
            buckets[k].append(pair)
            pair_to_shard[pair] = k
        self._pair_to_shard = pair_to_shard

        self._shards: tuple[ShardAssignment, ...] = tuple(
            ShardAssignment(
                shard_index=k,
                pairs=tuple(buckets[k]),
                global_channels=GLOBAL_CHANNELS if k == 0 else (),
                per_pair_channels=PER_PAIR_CHANNELS,
            )
            for k in range(self._n)
        )

    @property
    def n_conns(self) -> int:
        return self._n

    @property
    def shards(self) -> tuple[ShardAssignment, ...]:
        return self._shards

    @property
    def universe(self) -> tuple[str, ...]:
        return self._universe

    @property
    def pair_to_shard_index(self) -> dict[str, int]:
        """The pair_to_shard_index map the WS_Manager keeps (Image6 desc)."""
        return dict(self._pair_to_shard)

    def shard_for(self, pair: str) -> int:
        """The shard index a pair's per-pair channels live on."""
        return self._pair_to_shard[pair]
