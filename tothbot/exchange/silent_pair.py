"""mod:WS_Manager silent-pair state machine: per-pair first-data liveness.

Source: 0500000 dv1_240 sec 7 mod:WS_Manager desc (the Silent-Pair State Machine
block: contract:WM-SHARD-010 + contract:WM-PS-007 + rule:HR-WM-030) and sec 2
Image1 (governs rule:HR-WM-030).

A freshly-subscribed pair can ACK its subscription yet never push a data frame -
an illiquid pair, or a Kraken-side stall. Evaluating that pair on stale seed
indicators is a loss event, so WS_Manager tracks every pair's FIRST-DATA liveness
with a per-pair state machine over an injected monotonic clock (mirrors
keepalive.py / pacing.py - pure policy, I/O at the edges).

States + transitions, exactly as the figure draws them:
    INITIAL      --[sub ack]------------------> SUBSCRIBED
    SUBSCRIBED   --[update <= T_silent]-------> DATA_READY
    SUBSCRIBED   --[no data > T_silent]-------> DATA_PENDING
    DATA_PENDING --[update]-------------------> DATA_READY
    (any)        --[shard reconnect]---------> SUBSCRIBED   (the reconnecting
                                               shard's pairs re-arm the timer)

T_silent = 60 s (fixed engineering constant, WM-SHARD-010; not CIATS-owned).

rule:HR-WM-030: a DATA_PENDING pair is gated at Pre-Gate-1 BEFORE indicator eval
- the pipeline SKIPS it (gate_blocks() below). rule:HR-WM-031 / anti-ar:AR-070: a
silent pair is HELD, never auto-evicted from the universe - this machine only
MARKS it; it never removes the pair.

Emits (the receive loop routes these to mod:Logger):
    evt:PAIR_DATA_PENDING          on SUBSCRIBED -> DATA_PENDING (T_silent expiry)
    evt:PAIR_DATA_READY_RECOVERED  on DATA_PENDING -> DATA_READY (a silent pair
                                   recovers)
The normal SUBSCRIBED -> DATA_READY first-data path emits NO event (the figure
names only the two events above; produces: lists exactly those two).

The receive loop drives the marker methods at the I/O edge: mark_subscribed() on
the subscribe ACK, mark_data() on every real data frame for the pair,
mark_shard_reconnect() when the pair's shard reconnects, and evaluate() on its
scheduler tick to detect T_silent expiry. Each returns the event to emit (or
None), mirroring keepalive.liveness() returning a verdict.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from enum import Enum

# --- fixed engineering constant (WM-SHARD-010; not CIATS-owned) ---------------
T_SILENT_SEC = 60.0  # max silence after a subscribe ACK before a pair is PENDING

# Canonical log keys for the two emitted events (the receive loop logs these so
# mod:Logger - the sole CIATS data source - sees the silent-pair telemetry).
PAIR_DATA_PENDING_LOG_KEY = "PAIR_DATA_PENDING"                  # evt:PAIR_DATA_PENDING
PAIR_DATA_READY_RECOVERED_LOG_KEY = "PAIR_DATA_READY_RECOVERED"  # evt:PAIR_DATA_READY_RECOVERED

# An injectable monotonic clock (time.monotonic by default; never wall-clock -
# the T_silent timer measures an elapsed interval, so a clock that can step
# backwards would corrupt the silence decision). Same contract as keepalive.Clock.
Clock = Callable[[], float]


class PairDataState(Enum):
    """The first-data liveness state of one subscribed pair."""

    INITIAL = "initial"            # constructed; no subscribe ACK yet
    SUBSCRIBED = "subscribed"      # ACKed; awaiting first data within T_silent
    DATA_READY = "data_ready"      # first data arrived; tradeable
    DATA_PENDING = "data_pending"  # no data within T_silent; gated at Gate 1, held


class SilentPairEvent(Enum):
    """The two events the machine emits (figure produces: list). A marker/poll
    method returns one of these to emit, or None for a no-op / unnamed path."""

    DATA_PENDING = "data_pending"                  # evt:PAIR_DATA_PENDING
    DATA_READY_RECOVERED = "data_ready_recovered"  # evt:PAIR_DATA_READY_RECOVERED


class SilentPairMachine:
    """Per-pair silent-pair state machine (WM-SHARD-010 + WM-PS-007 + HR-WM-030).

    Construct one per pair when the pair enters a shard's universe (state INITIAL).
    The receive loop then drives:
      mark_subscribed(now)      on the subscribe ACK            -> SUBSCRIBED
      mark_data(now)            on every real data frame        -> DATA_READY
      evaluate(now)             on each scheduler tick           -> DATA_PENDING on expiry
      mark_shard_reconnect(now) when the pair's shard reconnects -> SUBSCRIBED (re-arm)
    and consults gate_blocks() at Pre-Gate-1 to skip a DATA_PENDING pair.
    """

    def __init__(self, *, clock: Clock = time.monotonic) -> None:
        self._clock = clock
        self._state = PairDataState.INITIAL
        self._subscribed_at: float | None = None  # T_silent baseline while SUBSCRIBED

    @property
    def state(self) -> PairDataState:
        return self._state

    @property
    def is_data_ready(self) -> bool:
        return self._state is PairDataState.DATA_READY

    @property
    def is_data_pending(self) -> bool:
        return self._state is PairDataState.DATA_PENDING

    def gate_blocks(self) -> bool:
        """True when the pipeline must SKIP this pair at Pre-Gate-1 (HR-WM-030).
        Only a DATA_PENDING pair is skipped - and it is HELD, never evicted
        (HR-WM-031 anti-AR-070)."""
        return self._state is PairDataState.DATA_PENDING

    # --- markers (driven by the receive loop at the I/O edge) -----------------
    def mark_subscribed(self, now: float | None = None) -> None:
        """Subscribe ACK received: INITIAL -> SUBSCRIBED, arm the T_silent timer."""
        self._state = PairDataState.SUBSCRIBED
        self._subscribed_at = self._clock() if now is None else now

    def mark_data(self, now: float | None = None) -> SilentPairEvent | None:
        """A real data frame arrived for this pair.

        From SUBSCRIBED  -> DATA_READY (the normal first-data path; no event).
        From DATA_PENDING -> DATA_READY (recovery; emits DATA_READY_RECOVERED).
        From DATA_READY   -> stays DATA_READY (idempotent; no event).
        From INITIAL      -> ignored (data should not precede the ACK).

        Data presence always wins: an update that races in just after T_silent but
        before the next evaluate() tick still resolves the pair to DATA_READY
        (loss-min - a pair that just delivered is tradeable, not silent).
        """
        if self._state is PairDataState.DATA_PENDING:
            self._state = PairDataState.DATA_READY
            return SilentPairEvent.DATA_READY_RECOVERED
        if self._state is PairDataState.SUBSCRIBED:
            self._state = PairDataState.DATA_READY
            return None
        # INITIAL (pre-ACK) or already DATA_READY: no transition, no event.
        return None

    def evaluate(self, now: float | None = None) -> SilentPairEvent | None:
        """Scheduler-tick poll for T_silent expiry.

        SUBSCRIBED with (now - subscribed_at) > T_silent -> DATA_PENDING; emits
        DATA_PENDING. Every other state is a no-op (the DATA_PENDING edge exists
        only out of SUBSCRIBED per the figure)."""
        if self._state is not PairDataState.SUBSCRIBED:
            return None
        t = self._clock() if now is None else now
        assert self._subscribed_at is not None  # set on entry to SUBSCRIBED
        if (t - self._subscribed_at) > T_SILENT_SEC:
            self._state = PairDataState.DATA_PENDING
            return SilentPairEvent.DATA_PENDING
        return None

    def mark_shard_reconnect(self, now: float | None = None) -> None:
        """The pair's shard reconnected: (any) -> SUBSCRIBED, re-arm the timer.
        A reconnect re-subscribes the shard's pairs, so first-data liveness is
        re-measured from this moment (the figure's (any)->SUBSCRIBED edge)."""
        self._state = PairDataState.SUBSCRIBED
        self._subscribed_at = self._clock() if now is None else now
