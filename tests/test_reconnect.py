"""S2c3 tests: the per-shard reconnect coordinator (unblocked parts).

Covers 0500000 dv1_240 sec 2 Image1 (A-5) + sec 7 mod:WS_Manager desc Per-Shard
Reconnect Coordinator block: WS-REC-003 Scenario A/B selection + the stated
constants, contract:WM-RECONNECT-016 + rule:HR-WM-029 per-shard independence,
rule:HR-WM-012 in-progress gate, the ar:AR-056 / WS-REC-004 restore sequence, and
contract:WM-RECONNECT-019 paper gating.

The Scenario-A exponential backoff schedule (181s) is BLOCKED (TB00709 NSI sec 6):
the test asserts reconnect_delay_sec() raises in the backoff phase rather than
returning an invented value.
"""

from __future__ import annotations

import pytest

from tothbot.exchange.reconnect import (
    CLOUDFLARE_RECONNECT_LIMIT,
    CLOUDFLARE_WINDOW_SEC,
    RESTORE_SEQUENCE,
    SCENARIO_A_BACKOFF_BASE_SEC,
    SCENARIO_A_IMMEDIATE_ATTEMPTS,
    SCENARIO_B_MIN_DELAY_SEC,
    WS_RECONNECT_LOG_KEY,
    DisconnectReason,
    ReconnectScenario,
    RestoreStep,
    ShardReconnectCoordinator,
    build_restore_sequence,
    is_immediate_attempt,
    reconnect_delay_sec,
    select_scenario,
)


# -- stated constants match the diagram (WS-REC-003 / AR-080) ------------
def test_stated_constants() -> None:
    assert SCENARIO_A_IMMEDIATE_ATTEMPTS == 5
    assert SCENARIO_A_BACKOFF_BASE_SEC == 1.0
    assert SCENARIO_B_MIN_DELAY_SEC == 5.0
    assert CLOUDFLARE_RECONNECT_LIMIT == 150
    assert CLOUDFLARE_WINDOW_SEC == 600.0
    assert WS_RECONNECT_LOG_KEY == "WS_RECONNECT"


# -- scenario selection (WS-REC-003) ------------------------------------
def test_random_selects_scenario_a() -> None:
    assert select_scenario(DisconnectReason.RANDOM) is ReconnectScenario.SCENARIO_A


def test_maintenance_selects_scenario_b() -> None:
    assert select_scenario(DisconnectReason.MAINTENANCE) is ReconnectScenario.SCENARIO_B


# -- immediate-attempt phase (WS-REC-003 "up to 5 immediate attempts") --
def test_immediate_attempt_window() -> None:
    assert is_immediate_attempt(1)
    assert is_immediate_attempt(SCENARIO_A_IMMEDIATE_ATTEMPTS)
    assert not is_immediate_attempt(SCENARIO_A_IMMEDIATE_ATTEMPTS + 1)


def test_immediate_attempt_rejects_below_one() -> None:
    with pytest.raises(ValueError):
        is_immediate_attempt(0)


# -- reconnect_delay_sec: code-complete cases ---------------------------
def test_scenario_a_immediate_attempts_zero_delay() -> None:
    for attempt in range(1, SCENARIO_A_IMMEDIATE_ATTEMPTS + 1):
        assert reconnect_delay_sec(ReconnectScenario.SCENARIO_A, attempt) == 0.0


def test_scenario_b_always_five_second_floor() -> None:
    assert reconnect_delay_sec(ReconnectScenario.SCENARIO_B, 1) == SCENARIO_B_MIN_DELAY_SEC
    assert reconnect_delay_sec(ReconnectScenario.SCENARIO_B, 99) == SCENARIO_B_MIN_DELAY_SEC


def test_delay_rejects_below_one() -> None:
    with pytest.raises(ValueError):
        reconnect_delay_sec(ReconnectScenario.SCENARIO_A, 0)


# -- reconnect_delay_sec: BLOCKED backoff phase (NSI sec 6) --------------
def test_scenario_a_backoff_phase_is_blocked() -> None:
    with pytest.raises(NotImplementedError):
        reconnect_delay_sec(ReconnectScenario.SCENARIO_A, SCENARIO_A_IMMEDIATE_ATTEMPTS + 1)


# -- WS-REC-004 restore sequence: order + paper gating ------------------
def test_restore_sequence_live_full_order() -> None:
    steps = build_restore_sequence(paper_mode=False)
    assert steps == [
        RestoreStep.ACQUIRE_WS_TOKEN,
        RestoreStep.RECONNECT_SOCKET,
        RestoreStep.RESUBSCRIBE_PUBLIC,
        RestoreStep.RESUBSCRIBE_PRIVATE,
        RestoreStep.RESET_RATE_CEILING,
        RestoreStep.RESUME_KEEPALIVE,
        RestoreStep.RESTORE_POSITION_MIRROR,
        RestoreStep.RESTORE_TICKER_TRIGGER,
    ]


def test_restore_sequence_paper_skips_private_side() -> None:
    steps = build_restore_sequence(paper_mode=True)
    assert steps == [
        RestoreStep.RECONNECT_SOCKET,
        RestoreStep.RESUBSCRIBE_PUBLIC,
        RestoreStep.RESUME_KEEPALIVE,
        RestoreStep.RESTORE_TICKER_TRIGGER,
    ]


def test_paper_sequence_is_a_subsequence_of_live() -> None:
    live = build_restore_sequence(paper_mode=False)
    paper = build_restore_sequence(paper_mode=True)
    # paper preserves live order, only dropping private-side steps
    assert paper == [s for s in live if s in set(paper)]


def test_private_side_steps_are_exactly_the_private_set() -> None:
    private = {s.step for s in RESTORE_SEQUENCE if s.private_side}
    assert private == {
        RestoreStep.ACQUIRE_WS_TOKEN,
        RestoreStep.RESUBSCRIBE_PRIVATE,
        RestoreStep.RESET_RATE_CEILING,
        RestoreStep.RESTORE_POSITION_MIRROR,
    }


def test_restore_specs_cover_every_step_once() -> None:
    steps = [s.step for s in RESTORE_SEQUENCE]
    assert len(steps) == len(set(steps)) == len(RestoreStep)


# -- per-shard coordinator: HR-WM-029 independence + HR-WM-012 gate ------
def test_coordinator_starts_idle() -> None:
    c = ShardReconnectCoordinator()
    assert not c.any_reconnecting()
    assert not c.is_reconnecting(0)
    assert c.reconnecting_shards == frozenset()


def test_begin_records_scenario_and_in_progress() -> None:
    c = ShardReconnectCoordinator()
    scenario = c.begin(2, DisconnectReason.MAINTENANCE)
    assert scenario is ReconnectScenario.SCENARIO_B
    assert c.is_reconnecting(2)
    assert c.scenario_for(2) is ReconnectScenario.SCENARIO_B
    assert c.any_reconnecting()


def test_shards_reconnect_independently() -> None:
    c = ShardReconnectCoordinator()
    c.begin(0, DisconnectReason.RANDOM)
    c.begin(1, DisconnectReason.MAINTENANCE)
    assert c.reconnecting_shards == frozenset({0, 1})
    assert c.scenario_for(0) is ReconnectScenario.SCENARIO_A
    assert c.scenario_for(1) is ReconnectScenario.SCENARIO_B
    # completing one leaves the other running (HR-WM-029)
    c.complete(0)
    assert not c.is_reconnecting(0)
    assert c.is_reconnecting(1)
    assert c.any_reconnecting()


def test_any_reconnecting_clears_when_all_complete() -> None:
    c = ShardReconnectCoordinator()
    c.begin(0, DisconnectReason.RANDOM)
    c.begin(1, DisconnectReason.RANDOM)
    c.complete(0)
    c.complete(1)
    assert not c.any_reconnecting()  # HR-WM-012 gate releases the pipeline


def test_complete_unknown_shard_is_noop() -> None:
    c = ShardReconnectCoordinator()
    c.complete(7)  # no raise
    assert not c.any_reconnecting()


def test_begin_rejects_negative_shard() -> None:
    c = ShardReconnectCoordinator()
    with pytest.raises(ValueError):
        c.begin(-1, DisconnectReason.RANDOM)


def test_scenario_for_unknown_shard_is_none() -> None:
    c = ShardReconnectCoordinator()
    assert c.scenario_for(3) is None
