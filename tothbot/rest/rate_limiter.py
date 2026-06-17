"""The GLOBAL REST rate governor - one per-IP call budget shared across every REST phase.

Source: 0500000 sec 7 container:Kraken_REST_API + ar:AR-036 (the 1.1s GetOHLCData per-IP stagger).
THE DEFECT THIS FIXES: the cold-start phases (the warm-up gather, the daily regime compute, the
liquidity probe) + the periodic reconcile all drive the ONE KrakenRestClient, but the rate control
was applied at the WRONG SCOPE - the warm-up's per-pair 1.1s sleep only spaced a SINGLE pair's own
two calls, while WarmupOrchestrator.warm_all fans out one coroutine PER PAIR concurrently
(asyncio.gather). So a large universe fired ~one REST call per pair at once (bounded only by the
aiohttp connector to ~5-10 concurrent = a sustained ~50-100 calls/sec drain), far over Kraken's
~1 call/sec public budget -> mass rate-limit -> mass WARM_UP_FAIL -> a large fraction of the universe
silently never seeds (a systemic false-negative).

THE FIX: a SINGLE shared pacer every _public/_private call awaits, so calls are spaced >= min_interval
apart GLOBALLY no matter how many coroutines call concurrently (a leaky bucket at rate 1/min_interval,
NO burst credit - REST wants even spacing, not bursts; a sparse gap does NOT accumulate credit). An
asyncio.Lock serializes the slot computation and is held across the spacing sleep, so concurrent
acquirers are released one per min_interval. AIMD-style back-off: a Kraken rate-limit RESPONSE
penalizes (pushes the next slot out) so the governor eases off on the exchange's OWN throttle signal.

PURE save the injected monotonic clock + sleep, so it is driven under asyncio.run with a fake clock -
no real timers. min_interval_sec <= 0 makes acquire() a no-op (the test-fast / disabled mode).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]

# ar:AR-036 the Kraken public GetOHLCData per-IP budget: ~1 call/sec + ~10% margin. The shared REST
# governor's default spacing - an ENGINEERING cadence (a per-IP rate budget), NOT a CIATS seed.
DEFAULT_MIN_INTERVAL_SEC = 1.1
# On a Kraken rate-limit response the governor pushes the next slot out by this penalty (the
# multiplicative back-off step) so it eases off the exchange's own throttle signal.
DEFAULT_BACKOFF_PENALTY_SEC = 3.0


class RestRateLimiter:
    """A shared leaky-bucket pacer at rate 1/min_interval (no burst). acquire() awaits the next slot;
    penalize() backs the governor off on a Kraken rate-limit response. One instance per process (the
    shared KrakenRestClient owns it), so every REST phase honors the one per-IP budget."""

    def __init__(
        self,
        *,
        min_interval_sec: float = DEFAULT_MIN_INTERVAL_SEC,
        backoff_penalty_sec: float = DEFAULT_BACKOFF_PENALTY_SEC,
        clock: Clock = time.monotonic,
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self._min_interval = float(min_interval_sec)
        self._backoff_penalty = float(backoff_penalty_sec)
        self._clock = clock
        self._sleep = sleep
        self._lock = asyncio.Lock()
        self._next_allowed: float | None = None  # monotonic ts of the next permitted call

    async def acquire(self) -> None:
        """Block until the next permitted slot, then reserve the following one. Spacing is GLOBAL: N
        concurrent acquirers are released one per min_interval. A no-op when min_interval <= 0."""
        if self._min_interval <= 0:
            return
        async with self._lock:
            now = self._clock()
            # No burst credit: a gap longer than the interval resets the clock to now (not earlier).
            if self._next_allowed is None or self._next_allowed < now:
                self._next_allowed = now
            wait = self._next_allowed - now
            if wait > 0:
                await self._sleep(wait)
            self._next_allowed += self._min_interval

    def penalize(self) -> None:
        """A Kraken rate-limit response was seen - push the next slot out by the back-off penalty so
        the next acquire() eases off (best-effort; the AIMD multiplicative-decrease step)."""
        now = self._clock()
        base = self._next_allowed if (self._next_allowed is not None and self._next_allowed > now) else now
        self._next_allowed = base + self._backoff_penalty
