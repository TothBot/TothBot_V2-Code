"""S2c2 tests: the process-singleton subscribe token bucket (subscribe pacing).

Covers 0500000 dv1_240 sec 2 Image1 Subscribe Token Bucket block + sec 7
mod:WS_Manager desc: contract:WM-PACE-002 SUBSCRIBE_RATE_PER_SEC=10 /
SUBSCRIBE_BURST_CAPACITY=20, refill 10 tok/sec, acquire blocks until a token is
available, and the WM-PACE-005 state value. Driven by an injected monotonic
clock (no asyncio) so the refill/acquire math is deterministic.
"""

from __future__ import annotations

import pytest

from tothbot.exchange.pacing import (
    PACE_STATE_LOG_KEY,
    SUBSCRIBE_BURST_CAPACITY,
    SUBSCRIBE_RATE_PER_SEC,
    SubscribeTokenBucket,
)


class FakeClock:
    """A manually advanced monotonic clock for deterministic pacing tests."""

    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def _bucket(clock: FakeClock) -> SubscribeTokenBucket:
    return SubscribeTokenBucket(clock=clock)


# -- constants match the diagram (WM-PACE-002) --------------------------

def test_constants_match_diagram():
    assert SUBSCRIBE_RATE_PER_SEC == 10.0
    assert SUBSCRIBE_BURST_CAPACITY == 20.0
    assert PACE_STATE_LOG_KEY == "SUBSCRIBE_TOKEN_BUCKET_STATE"  # WM-PACE-005


# -- starts full: the allowed startup burst -----------------------------

def test_starts_full_at_burst_capacity():
    clk = FakeClock()
    b = _bucket(clk)
    assert b.available() == 20.0
    assert b.capacity == 20.0
    assert b.rate_per_sec == 10.0


def test_burst_of_capacity_then_empty():
    clk = FakeClock()
    b = _bucket(clk)
    # 20 immediate subscribes succeed (the burst), the 21st does not
    assert all(b.try_acquire() for _ in range(20))
    assert b.try_acquire() is False
    assert b.available() == pytest.approx(0.0)


# -- refill at 10 tok/sec -----------------------------------------------

def test_refill_rate_is_ten_per_second():
    clk = FakeClock()
    b = _bucket(clk)
    for _ in range(20):
        b.try_acquire()           # drain to empty
    clk.advance(1.0)              # +1s -> +10 tokens
    assert b.available() == pytest.approx(10.0)
    assert sum(b.try_acquire() for _ in range(10)) == 10
    assert b.try_acquire() is False


def test_refill_caps_at_capacity():
    clk = FakeClock()
    b = _bucket(clk)
    for _ in range(20):
        b.try_acquire()           # empty
    clk.advance(100.0)           # would be 1000 tokens, but capped at 20
    assert b.available() == pytest.approx(20.0)


def test_partial_refill_fractional_tokens():
    clk = FakeClock()
    b = _bucket(clk)
    for _ in range(20):
        b.try_acquire()           # empty
    clk.advance(0.25)            # +2.5 tokens
    assert b.available() == pytest.approx(2.5)
    assert b.try_acquire() is True
    assert b.try_acquire() is True
    assert b.try_acquire() is False  # only 0.5 token left


# -- acquire blocks until a token is available (time_until_next) ---------

def test_time_until_next_zero_when_token_available():
    clk = FakeClock()
    b = _bucket(clk)
    assert b.time_until_next() == 0.0  # full bucket


def test_time_until_next_when_empty_is_one_over_rate():
    clk = FakeClock()
    b = _bucket(clk)
    for _ in range(20):
        b.try_acquire()           # empty
    # need 1 whole token at 10/s -> 0.1s
    assert b.time_until_next() == pytest.approx(0.1)


def test_time_until_next_accounts_for_partial_token():
    clk = FakeClock()
    b = _bucket(clk)
    for _ in range(20):
        b.try_acquire()           # empty
    clk.advance(0.05)            # +0.5 token; need 0.5 more at 10/s -> 0.05s
    assert b.time_until_next() == pytest.approx(0.05)


def test_acquire_succeeds_after_waiting_the_indicated_time():
    clk = FakeClock()
    b = _bucket(clk)
    for _ in range(20):
        b.try_acquire()           # empty
    wait = b.time_until_next()
    assert b.try_acquire() is False
    clk.advance(wait)            # sleep exactly the indicated wait (the I/O edge)
    assert b.try_acquire() is True


# -- backward clock step is harmless ------------------------------------

def test_backward_clock_step_does_not_drain():
    clk = FakeClock()
    b = _bucket(clk)
    b.try_acquire()              # 19 left
    clk.advance(-5.0)            # clock steps backwards
    assert b.available() == pytest.approx(19.0)  # neither drained nor over-filled


# -- construction guards ------------------------------------------------

def test_rejects_non_positive_rate_or_capacity():
    clk = FakeClock()
    with pytest.raises(ValueError):
        SubscribeTokenBucket(clock=clk, rate_per_sec=0)
    with pytest.raises(ValueError):
        SubscribeTokenBucket(clock=clk, burst_capacity=0)
