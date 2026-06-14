"""Subscribe pacing: the process-singleton subscribe token bucket.

Source: 0500000 dv1_240 sec 2 Image1 (the Subscribe Token Bucket block) + sec 7
mod:WS_Manager desc (contract:WM-PACE-001 process-singleton token bucket,
contract:WM-PACE-002 SUBSCRIBE_RATE_PER_SEC=10 / SUBSCRIBE_BURST_CAPACITY=20,
contract:WM-PACE-005 token-bucket state logging; ar:AR-080 Cloudflare ceiling).

Kraken fronts its WS endpoint with Cloudflare, which caps subscribe RPCs at
~150 per 10 minutes per IP (ar:AR-080). A burst of subscribes at startup or on a
mass reconnect would trip that ceiling and trigger evt:WS_SUBSCRIBE_RATE_LIMIT,
stalling the whole data layer. The token bucket is the code-layer defence:

  SUBSCRIBE_RATE_PER_SEC   = 10   tokens refilled per second (WM-PACE-002)
  SUBSCRIBE_BURST_CAPACITY = 20   max tokens (the allowed startup burst)

PROCESS-SINGLETON (WM-PACE-001): ONE bucket governs EVERY outbound subscribe
across ALL shards, because the AR-080 ceiling is per-IP for the whole process. A
per-shard bucket would let N shards each burst and collectively breach the
ceiling, so the bucket is shared, never reset on a single shard's reconnect.

EVERY outbound subscribe awaits acquire() - NO bypass, NO priority lane: even
reconnect re-subscribes are paced, since that is exactly the moment the ceiling
is most at risk.

PURE unit over an injected monotonic clock (mirrors keepalive.py): the
refill/acquire math is unit-testable without asyncio. The bucket exposes
try_acquire() + time_until_next(); the async subscribe loop (the I/O edge)
drives them:

    while not bucket.try_acquire():
        await asyncio.sleep(bucket.time_until_next())   # emits evt:SUBSCRIBE_PACE_WAIT
    send_subscribe(...)                                 # one token spent

available() exposes the live token level for the WM-PACE-005 state log.
"""

from __future__ import annotations

import time
from collections.abc import Callable

# --- fixed engineering constants (WM-PACE-002; not CIATS-owned) ---------------
SUBSCRIBE_RATE_PER_SEC = 10.0    # tokens refilled per second (WM-PACE-002)
SUBSCRIBE_BURST_CAPACITY = 20.0  # bucket capacity = startup burst allowance (WM-PACE-002)

# Canonical log key for the token-bucket state line (WM-PACE-005). The subscribe
# loop logs the bucket level so CIATS can validate the pacing (WM-PACE-005 is the
# source for CIATS WM-PACE-002 validation per the mod:WS_Manager desc).
PACE_STATE_LOG_KEY = "SUBSCRIBE_TOKEN_BUCKET_STATE"  # WM-PACE-005

# An injectable monotonic clock (time.monotonic by default; never wall-clock -
# the refill measures an elapsed interval, so a clock that can step backwards
# would corrupt the token accounting). Same contract as keepalive.Clock.
Clock = Callable[[], float]


class SubscribeTokenBucket:
    """Process-singleton subscribe rate limiter (contract:WM-PACE-001/002).

    Construct ONCE per process and share it across every shard's subscribe path.
    Starts full (a fresh process may burst up to SUBSCRIBE_BURST_CAPACITY
    subscribes immediately), then refills at SUBSCRIBE_RATE_PER_SEC. It is NOT
    reset on a shard reconnect - the per-IP AR-080 ceiling spans the whole
    process, so paced re-subscribes must draw from the same bucket.
    """

    def __init__(
        self,
        *,
        clock: Clock = time.monotonic,
        rate_per_sec: float = SUBSCRIBE_RATE_PER_SEC,
        burst_capacity: float = SUBSCRIBE_BURST_CAPACITY,
    ) -> None:
        if rate_per_sec <= 0:
            raise ValueError(f"rate_per_sec must be > 0, got {rate_per_sec}")
        if burst_capacity <= 0:
            raise ValueError(f"burst_capacity must be > 0, got {burst_capacity}")
        self._clock = clock
        self._rate = rate_per_sec
        self._capacity = burst_capacity
        self._tokens = burst_capacity  # start full: the allowed startup burst
        self._last_refill = clock()

    @property
    def rate_per_sec(self) -> float:
        return self._rate

    @property
    def capacity(self) -> float:
        return self._capacity

    def _refill(self, now: float) -> None:
        """Accrue tokens for the elapsed interval, capped at capacity. A backward
        clock step accrues nothing (and never drains the bucket)."""
        elapsed = now - self._last_refill
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last_refill = now

    def available(self, now: float | None = None) -> float:
        """Current token level after refill - the WM-PACE-005 state value."""
        t = self._clock() if now is None else now
        self._refill(t)
        return self._tokens

    def try_acquire(self, now: float | None = None) -> bool:
        """Consume one token if at least one is available; return whether the
        subscribe may proceed. Non-blocking - the I/O edge sleeps when False."""
        t = self._clock() if now is None else now
        self._refill(t)
        if self._tokens >= 1.0:
            self._tokens -= 1.0
            return True
        return False

    def time_until_next(self, now: float | None = None) -> float:
        """Seconds until at least one token is available (0.0 if one is now). The
        subscribe loop sleeps this long before retrying acquire."""
        t = self._clock() if now is None else now
        self._refill(t)
        if self._tokens >= 1.0:
            return 0.0
        return (1.0 - self._tokens) / self._rate
