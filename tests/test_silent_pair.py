"""S2c3 tests: the per-pair silent-pair state machine.

Covers 0500000 dv1_240 sec 7 mod:WS_Manager desc Silent-Pair State Machine block:
contract:WM-SHARD-010 + contract:WM-PS-007 + rule:HR-WM-030, T_silent=60s, the
five drawn transitions, the Gate-1 skip (HR-WM-030), the held-not-evicted rule
(HR-WM-031), and the two emitted events. Driven by an injected monotonic clock
(no asyncio) so the T_silent timing is deterministic.
"""

from __future__ import annotations

from tothbot.exchange.silent_pair import (
    PAIR_DATA_PENDING_LOG_KEY,
    PAIR_DATA_READY_RECOVERED_LOG_KEY,
    T_SILENT_SEC,
    PairDataState,
    SilentPairEvent,
    SilentPairMachine,
)


class FakeClock:
    """A manually advanced monotonic clock for deterministic silent-pair tests."""

    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> None:
        self.t += dt


def _machine(clock: FakeClock) -> SilentPairMachine:
    return SilentPairMachine(clock=clock)


# -- constants match the diagram (WM-SHARD-010) -------------------------
def test_t_silent_constant_is_60s() -> None:
    assert T_SILENT_SEC == 60.0


def test_log_keys_match_event_names() -> None:
    assert PAIR_DATA_PENDING_LOG_KEY == "PAIR_DATA_PENDING"
    assert PAIR_DATA_READY_RECOVERED_LOG_KEY == "PAIR_DATA_READY_RECOVERED"


# -- initial state ------------------------------------------------------
def test_starts_in_initial() -> None:
    m = _machine(FakeClock())
    assert m.state is PairDataState.INITIAL
    assert not m.is_data_ready
    assert not m.is_data_pending
    assert not m.gate_blocks()


# -- INITIAL --[sub ack]--> SUBSCRIBED ----------------------------------
def test_sub_ack_moves_to_subscribed() -> None:
    m = _machine(FakeClock())
    m.mark_subscribed()
    assert m.state is PairDataState.SUBSCRIBED
    assert not m.gate_blocks()


# -- SUBSCRIBED --[update <= T_silent]--> DATA_READY (no event) ----------
def test_data_within_t_silent_goes_ready_no_event() -> None:
    c = FakeClock()
    m = _machine(c)
    m.mark_subscribed()
    c.advance(T_SILENT_SEC - 1.0)  # still within the window
    event = m.mark_data()
    assert m.state is PairDataState.DATA_READY
    assert m.is_data_ready
    assert event is None  # the normal first-data path emits no event


def test_data_exactly_at_t_silent_boundary_goes_ready() -> None:
    c = FakeClock()
    m = _machine(c)
    m.mark_subscribed()
    c.advance(T_SILENT_SEC)  # update <= T_silent -> DATA_READY (inclusive boundary)
    assert m.mark_data() is None
    assert m.state is PairDataState.DATA_READY


# -- SUBSCRIBED --[no data > T_silent]--> DATA_PENDING (emits PENDING) ---
def test_no_data_beyond_t_silent_goes_pending_with_event() -> None:
    c = FakeClock()
    m = _machine(c)
    m.mark_subscribed()
    c.advance(T_SILENT_SEC + 0.001)  # strictly beyond the window
    event = m.evaluate()
    assert m.state is PairDataState.DATA_PENDING
    assert m.is_data_pending
    assert event is SilentPairEvent.DATA_PENDING


def test_evaluate_at_exactly_t_silent_does_not_fire() -> None:
    c = FakeClock()
    m = _machine(c)
    m.mark_subscribed()
    c.advance(T_SILENT_SEC)  # not strictly > T_silent
    assert m.evaluate() is None
    assert m.state is PairDataState.SUBSCRIBED


def test_evaluate_before_expiry_is_noop() -> None:
    c = FakeClock()
    m = _machine(c)
    m.mark_subscribed()
    c.advance(T_SILENT_SEC - 5.0)
    assert m.evaluate() is None
    assert m.state is PairDataState.SUBSCRIBED


# -- DATA_PENDING --[update]--> DATA_READY (emits RECOVERED) -------------
def test_pending_recovers_on_update_with_event() -> None:
    c = FakeClock()
    m = _machine(c)
    m.mark_subscribed()
    c.advance(T_SILENT_SEC + 10.0)
    m.evaluate()
    assert m.is_data_pending
    event = m.mark_data()
    assert m.state is PairDataState.DATA_READY
    assert event is SilentPairEvent.DATA_READY_RECOVERED


# -- HR-WM-030 Gate-1 gate + HR-WM-031 held-not-evicted -----------------
def test_only_pending_blocks_gate() -> None:
    c = FakeClock()
    m = _machine(c)
    assert not m.gate_blocks()           # INITIAL
    m.mark_subscribed()
    assert not m.gate_blocks()           # SUBSCRIBED
    m.mark_data()
    assert not m.gate_blocks()           # DATA_READY
    # drive a fresh machine to PENDING
    m2 = _machine(c)
    m2.mark_subscribed()
    c.advance(T_SILENT_SEC + 1.0)
    m2.evaluate()
    assert m2.gate_blocks()              # DATA_PENDING -> Gate 1 skips it


def test_pending_pair_is_held_not_evicted() -> None:
    # The machine has no removal/evict API - a silent pair is only marked
    # (HR-WM-031 anti-AR-070). Assert it stays addressable and recoverable.
    c = FakeClock()
    m = _machine(c)
    m.mark_subscribed()
    c.advance(T_SILENT_SEC + 1.0)
    m.evaluate()
    assert m.is_data_pending
    # still drivable -> recovers, never dropped
    assert m.mark_data() is SilentPairEvent.DATA_READY_RECOVERED


# -- (any) --[shard reconnect]--> SUBSCRIBED (re-arm) -------------------
def test_shard_reconnect_rearms_from_pending() -> None:
    c = FakeClock()
    m = _machine(c)
    m.mark_subscribed()
    c.advance(T_SILENT_SEC + 1.0)
    m.evaluate()
    assert m.is_data_pending
    m.mark_shard_reconnect()
    assert m.state is PairDataState.SUBSCRIBED
    # timer is re-armed: a fresh full T_silent must elapse before PENDING again
    c.advance(T_SILENT_SEC - 1.0)
    assert m.evaluate() is None
    c.advance(2.0)
    assert m.evaluate() is SilentPairEvent.DATA_PENDING


def test_shard_reconnect_rearms_from_data_ready() -> None:
    c = FakeClock()
    m = _machine(c)
    m.mark_subscribed()
    m.mark_data()
    assert m.is_data_ready
    m.mark_shard_reconnect()
    assert m.state is PairDataState.SUBSCRIBED


def test_shard_reconnect_from_initial_goes_subscribed() -> None:
    # the figure's edge is "(any) -> SUBSCRIBED"
    c = FakeClock()
    m = _machine(c)
    m.mark_shard_reconnect()
    assert m.state is PairDataState.SUBSCRIBED


# -- edge cases ---------------------------------------------------------
def test_data_before_ack_is_ignored() -> None:
    m = _machine(FakeClock())
    assert m.mark_data() is None
    assert m.state is PairDataState.INITIAL


def test_data_ready_is_idempotent() -> None:
    m = _machine(FakeClock())
    m.mark_subscribed()
    m.mark_data()
    assert m.mark_data() is None  # no spurious recovered event
    assert m.state is PairDataState.DATA_READY


def test_data_racing_just_after_expiry_still_goes_ready() -> None:
    # update arrives after T_silent but before the next evaluate() tick: data
    # presence wins (loss-min) -> DATA_READY, no recovered event (never PENDING).
    c = FakeClock()
    m = _machine(c)
    m.mark_subscribed()
    c.advance(T_SILENT_SEC + 5.0)
    event = m.mark_data()
    assert m.state is PairDataState.DATA_READY
    assert event is None


def test_evaluate_after_ready_is_noop() -> None:
    c = FakeClock()
    m = _machine(c)
    m.mark_subscribed()
    m.mark_data()
    c.advance(T_SILENT_SEC + 100.0)
    assert m.evaluate() is None  # no SUBSCRIBED->PENDING edge out of DATA_READY
    assert m.state is PairDataState.DATA_READY


def test_explicit_now_overrides_clock() -> None:
    c = FakeClock(t=0.0)
    m = _machine(c)
    m.mark_subscribed(now=100.0)
    # clock still reads 0.0 but explicit now drives the timer
    assert m.evaluate(now=100.0 + T_SILENT_SEC + 1.0) is SilentPairEvent.DATA_PENDING
