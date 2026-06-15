"""S2 tests: the PATH-2 startup assembler (assembler.py).

Covers 0500000 dv1_241 sec 2 Image1 + sec 7 mod:WS_Manager startup cold-start:
the per-shard fan-out from a ShardPlan, the process-singleton subscribe token-bucket
pacing (WM-PACE-001/002), the silent-pair registry wiring, the shared
ReconnectDriver/coordinator (one HR-WM-012 gate), and the reconnect re-subscribe +
silent-pair re-arm. Driven with stdlib asyncio.run over fakes - no network, no timers.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

from tothbot.config.settings import Mode
from tothbot.exchange.assembler import (
    DataLayer,
    DataLayerAssembler,
    SubscribePaceBatchComplete,
    SubscribePaceWait,
    SubscribeRequest,
    subscribe_requests,
    to_wire,
)
from tothbot.exchange.channels import PublicChannel
from tothbot.exchange.pacing import SubscribeTokenBucket
from tothbot.exchange.reconnect import DisconnectReason
from tothbot.exchange.sharding import ShardPlan
from tothbot.exchange.silent_pair import PairDataState


class _FakeTransport:
    def __init__(self) -> None:
        self.sent: list = []
        self.closed = False

    async def send(self, message: dict) -> None:
        self.sent.append(message)

    async def recv(self) -> dict:  # pragma: no cover - build tests never read
        raise AssertionError("recv not exercised in assembler build tests")

    async def close(self) -> None:
        self.closed = True


def _opener():
    """An open_socket factory returning a fresh fake per call, tracked per shard."""
    opened: dict[int, list] = {}

    async def open_socket(k: int) -> _FakeTransport:
        t = _FakeTransport()
        opened.setdefault(k, []).append(t)
        return t

    return open_socket, opened


def _big_bucket():
    # Capacity high enough that every subscribe acquires immediately (no pacing waits)
    # so wiring tests are not entangled with the pacing math.
    return SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0)


# -- subscribe request construction (Image1 shard block) ----------------

def test_subscribe_requests_count_matches_shard_subscribe_count():
    plan = ShardPlan(["BTC/USD", "ETH/USD", "SOL/USD"])
    assignment = plan.shards[0]
    reqs = subscribe_requests(assignment)
    assert len(reqs) == assignment.subscribe_count == 2 + 3 * 3  # globals + pairs x 3 channels
    # Global channels (instrument, status) come first, on shard 0 only.
    assert reqs[0] == SubscribeRequest(PublicChannel.INSTRUMENT)
    assert reqs[1] == SubscribeRequest(PublicChannel.STATUS)
    assert all(r.symbol is not None for r in reqs[2:])


def test_to_wire_frames():
    assert to_wire(SubscribeRequest(PublicChannel.OHLC_5M, "BTC/USD")) == {
        "method": "subscribe",
        "params": {"channel": "ohlc", "symbol": ["BTC/USD"], "interval": 5},
    }
    assert to_wire(SubscribeRequest(PublicChannel.TICKER, "BTC/USD")) == {
        "method": "subscribe",
        "params": {"channel": "ticker", "symbol": ["BTC/USD"], "event_trigger": "trades"},
    }
    assert to_wire(SubscribeRequest(PublicChannel.INSTRUMENT)) == {
        "method": "subscribe", "params": {"channel": "instrument"},
    }
    assert to_wire(SubscribeRequest(PublicChannel.STATUS)) == {
        "method": "subscribe", "params": {"channel": "status"},
    }


# -- build: shard fan-out + wiring --------------------------------------

def test_build_opens_one_socket_per_shard_and_wires_globals_on_shard0():
    universe = [f"P{i}/USD" for i in range(501)]  # 501 pairs -> 2 shards
    plan = ShardPlan(universe)
    assert plan.n_conns == 2
    open_socket, opened = _opener()
    asm = DataLayerAssembler(plan, mode=Mode.PAPER, open_socket=open_socket, bucket=_big_bucket())
    data = asyncio.run(asm.build())

    assert isinstance(data, DataLayer)
    assert len(data.shards) == 2
    assert set(opened) == {0, 1} and all(len(v) == 1 for v in opened.values())
    s0, s1 = data.shards
    assert s0.assignment.is_clock_shard and not s1.assignment.is_clock_shard
    assert s0.assignment.global_channels == (PublicChannel.INSTRUMENT, PublicChannel.STATUS)
    assert s1.assignment.global_channels == ()
    # one silent-pair machine per pair on each shard
    assert len(s0.silent_pairs) == len(s0.assignment.pairs)
    assert len(s1.silent_pairs) == len(s1.assignment.pairs)
    # is_reconnecting wired to the shared coordinator gate (HR-WM-012)
    assert data.coordinator.any_reconnecting() is False


def test_initial_paced_subscribe_sends_every_request():
    plan = ShardPlan(["BTC/USD", "ETH/USD"])  # 1 shard
    open_socket, opened = _opener()
    events: list = []
    asm = DataLayerAssembler(plan, mode=Mode.PAPER, open_socket=open_socket,
                             bucket=_big_bucket(), on_event=events.append)
    data = asyncio.run(asm.build())

    socket0 = opened[0][0]
    expected = [to_wire(r) for r in subscribe_requests(data.shards[0].assignment)]
    assert socket0.sent == expected
    batch = next(e for e in events if isinstance(e, SubscribePaceBatchComplete))
    assert batch.subscribe_count == len(expected)


# -- WM-PACE-001: the shared token bucket paces the subscribe storm ------

def test_subscribe_blocks_on_empty_bucket_and_paces():
    plan = ShardPlan(["BTC/USD", "ETH/USD", "SOL/USD"])  # 11 subscribe RPCs on 1 shard
    open_socket, opened = _opener()
    events: list = []
    clk = {"t": 0.0}
    # Tiny bucket: capacity 2 -> only 2 immediate, the rest must wait for refill.
    bucket = SubscribeTokenBucket(clock=lambda: clk["t"], rate_per_sec=10.0, burst_capacity=2.0)

    async def sleep(seconds: float) -> None:
        # A real asyncio.sleep(w) never returns early, so model a small overshoot;
        # advancing by EXACTLY the computed wait would let token accrual asymptote to
        # 1.0 without crossing it (a float artifact of the synthetic clock, not the
        # pacing unit, which runs on a real monotonic clock in production).
        clk["t"] += seconds + 0.05

    asm = DataLayerAssembler(plan, mode=Mode.PAPER, open_socket=open_socket, bucket=bucket,
                             on_event=events.append, sleep=sleep)
    data = asyncio.run(asm.build())

    # All 11 RPCs still get sent (no bypass, no drop) ...
    assert len(opened[0][0].sent) == data.shards[0].assignment.subscribe_count == 11
    # ... but the bucket forced pace-waits after the initial burst of 2.
    waits = [e for e in events if isinstance(e, SubscribePaceWait)]
    assert len(waits) == 11 - 2  # 9 requests had to wait for a token
    assert all(w.wait_seconds > 0 for w in waits)


def test_one_bucket_shared_across_shards():
    universe = [f"P{i}/USD" for i in range(501)]  # 2 shards
    plan = ShardPlan(universe)
    open_socket, _ = _opener()
    # Frozen clock: no refill during the build, so the drain is exact and we can prove
    # BOTH shards drew from the ONE bucket (WM-PACE-001 process-singleton).
    bucket = SubscribeTokenBucket(clock=lambda: 0.0, rate_per_sec=10.0, burst_capacity=100000.0)
    asm = DataLayerAssembler(plan, mode=Mode.PAPER, open_socket=open_socket, bucket=bucket)
    data = asyncio.run(asm.build())
    assert data.bucket is bucket
    total = sum(s.assignment.subscribe_count for s in data.shards)
    assert total == 1505  # 755 (shard0: 2 globals + 251x3) + 750 (shard1: 250x3)
    assert bucket.available(now=0.0) == 100000.0 - total  # one bucket drained by both


# -- reconnect: re-open + re-subscribe + silent-pair re-arm -------------

def test_reconnect_reopens_socket_resubscribes_and_rearms_silent_pairs():
    plan = ShardPlan(["BTC/USD", "ETH/USD"])  # 1 shard
    open_socket, opened = _opener()
    asm = DataLayerAssembler(plan, mode=Mode.PAPER, open_socket=open_socket, bucket=_big_bucket())
    data = asyncio.run(asm.build())
    runtime = data.shards[0]

    # Drive a reconnect for shard 0 through the SHARED driver (what the receive loop
    # awaits on a local TransportClosed).
    fresh = asyncio.run(data.driver.initiate(0, DisconnectReason.RANDOM))

    assert len(opened[0]) == 2          # a second socket was opened on reconnect
    assert fresh is opened[0][1]
    # the fresh socket received the full re-subscribe batch (paced)
    expected = [to_wire(r) for r in subscribe_requests(runtime.assignment)]
    assert opened[0][1].sent == expected
    # every pair re-armed its first-data timer ((any) -> SUBSCRIBED)
    assert all(m.state is PairDataState.SUBSCRIBED for m in runtime.silent_pairs.values())
    # the HR-WM-012 gate lifted once restore completed
    assert data.coordinator.any_reconnecting() is False


def test_reconnect_gate_is_raised_during_restore():
    plan = ShardPlan(["BTC/USD"])
    seen: list = []

    async def open_socket(k: int) -> _FakeTransport:
        # observed at RECONNECT_SOCKET time (after begin(), before complete())
        seen.append((len(seen), None))
        return _FakeTransport()

    asm = DataLayerAssembler(plan, mode=Mode.PAPER, open_socket=open_socket, bucket=_big_bucket())
    data = asyncio.run(asm.build())

    # Re-wire the socket opener to record the gate state during the reconnect open.
    gate_states: list = []

    async def recording_open(k: int) -> _FakeTransport:
        gate_states.append(data.coordinator.any_reconnecting())
        return _FakeTransport()

    asm._open_shard_socket = recording_open  # the reconnect socket opener
    asyncio.run(data.driver.initiate(0, DisconnectReason.RANDOM))
    assert gate_states == [True]                      # gate up during the open
    assert data.coordinator.any_reconnecting() is False  # lifted after


# -- DataLayer.run / stop fan-out --------------------------------------

def test_datalayer_run_and_stop_fan_out():
    class _FakeLoop:
        def __init__(self) -> None:
            self.ran = False
            self.stopped = False

        async def run(self) -> None:
            self.ran = True

        def stop(self) -> None:
            self.stopped = True

    loops = [_FakeLoop(), _FakeLoop(), _FakeLoop()]
    shards = [SimpleNamespace(loop=loop) for loop in loops]
    data = DataLayer(shards, coordinator=None, driver=None, bucket=None, mode=Mode.PAPER)

    asyncio.run(data.run())
    assert all(loop.ran for loop in loops)
    data.stop()
    assert all(loop.stopped for loop in loops)
