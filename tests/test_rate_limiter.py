"""Tests: the GLOBAL REST rate governor (rest/rate_limiter.py).

The genuine warm-up-flood fix: a single shared pacer every REST call awaits, so calls are spaced
>= min_interval apart GLOBALLY no matter how many coroutines call concurrently (a leaky bucket at
rate 1/min_interval, no burst credit). Covers: the zero-interval no-op, the inter-call spacing, the
no-burst-credit property (a long gap does not let two calls fire back-to-back), the AIMD penalize
back-off, and N concurrent acquirers released one per interval. Driven with a fake clock + a sleep
that advances it - no real timers.
"""

from __future__ import annotations

import asyncio

import pytest

from tothbot.rest.rate_limiter import RestRateLimiter


class _Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def _instruments():
    """A fake clock + a sleep that records its arg AND advances the clock (time passes)."""
    clock = _Clock()
    sleeps: list[float] = []

    async def sleep(dt: float) -> None:
        sleeps.append(dt)
        clock.t += dt

    return clock, sleeps, sleep


def _run(coro):
    return asyncio.run(coro)


def _limiter(clock, sleep, *, interval=1.1, penalty=3.0):
    return RestRateLimiter(
        min_interval_sec=interval, backoff_penalty_sec=penalty, clock=clock, sleep=sleep
    )


# --------------------------------------------------------------------------- zero-interval no-op
def test_zero_interval_never_sleeps():
    clock, sleeps, sleep = _instruments()
    lim = _limiter(clock, sleep, interval=0.0)

    async def main():
        for _ in range(5):
            await lim.acquire()

    _run(main())
    assert sleeps == []  # disabled / test-fast mode: acquire is a no-op


# --------------------------------------------------------------------------- inter-call spacing
def test_spaces_sequential_calls_by_interval():
    clock, sleeps, sleep = _instruments()
    lim = _limiter(clock, sleep)

    async def main():
        await lim.acquire()  # first call: no wait
        await lim.acquire()  # spaced 1.1
        await lim.acquire()  # spaced 1.1

    _run(main())
    assert sleeps == pytest.approx([1.1, 1.1])  # first call instant, each subsequent paced by the interval


# --------------------------------------------------------------------------- no burst credit
def test_no_burst_credit_after_a_gap():
    clock, sleeps, sleep = _instruments()
    lim = _limiter(clock, sleep)

    async def main():
        await lim.acquire()      # now=0 -> next_allowed=1.1
        clock.t = 10.0           # a long idle gap (> interval)
        await lim.acquire()      # now=10 > next_allowed -> fires immediately, NO accumulated credit
        await lim.acquire()      # the very next call is still paced by one interval

    _run(main())
    # the gap call does NOT sleep (slot reset to now), but it does NOT grant a second free call either.
    assert sleeps == pytest.approx([1.1])


# --------------------------------------------------------------------------- AIMD penalize back-off
def test_penalize_pushes_the_next_slot_out():
    clock, sleeps, sleep = _instruments()
    lim = _limiter(clock, sleep, interval=1.1, penalty=3.0)

    async def main():
        await lim.acquire()  # now=0 -> next_allowed=1.1
        lim.penalize()       # Kraken rate-limit response -> next_allowed = 1.1 + 3.0 = 4.1
        await lim.acquire()  # now=0 -> wait 4.1

    _run(main())
    assert sleeps == pytest.approx([4.1])


def test_penalize_from_cold_uses_now_plus_penalty():
    clock, sleeps, sleep = _instruments()
    lim = _limiter(clock, sleep, penalty=3.0)
    lim.penalize()  # no prior acquire: base = now (0) + penalty

    async def main():
        await lim.acquire()

    _run(main())
    assert sleeps == pytest.approx([3.0])


# --------------------------------------------------------------------------- concurrent acquirers
def test_concurrent_acquirers_released_one_per_interval():
    clock, sleeps, sleep = _instruments()
    lim = _limiter(clock, sleep)

    async def main():
        # 5 coroutines (the warm-up-gather shape) all hit the ONE governor at once.
        await asyncio.gather(*(lim.acquire() for _ in range(5)))

    _run(main())
    # first acquirer instant, the other 4 each paced by the interval (FIFO asyncio.Lock).
    assert sleeps == pytest.approx([1.1, 1.1, 1.1, 1.1])
    assert clock.t == pytest.approx(4.4)  # total span = (N-1) * interval
