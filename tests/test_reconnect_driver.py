"""S2-integration tests: the async reconnect driver (reconnect_driver.py).

Covers 0500000 dv1_241 WS-REC-003 / WS-REC-004 in real-time orchestration over the
PURE reconnect policy: the rule:HR-WM-012 in-progress gate held across the whole
reconnect, the CIATS-owned Scenario-A backoff seed schedule applied per attempt,
the Scenario-B 5 s floor, the contract:WM-RECONNECT-019 paper skip of private-side
steps, never-abandon retry, and partial-socket cleanup. Driven with stdlib
asyncio.run over fakes - no network, no real sleeps.
"""

from __future__ import annotations

import asyncio

from tothbot.exchange.reconnect import (
    RECONNECT_BACKOFF_SEED_SEC,
    SCENARIO_B_MIN_DELAY_SEC,
    DisconnectReason,
    RestoreStep,
    ShardReconnectCoordinator,
)
from tothbot.exchange.reconnect_driver import (
    ReconnectComplete,
    ReconnectDriver,
    ReconnectInitiated,
)
from tothbot.exchange.transport import TransportClosed


class _FakeTransport:
    def __init__(self) -> None:
        self.closed = False

    async def recv(self) -> dict:  # pragma: no cover - not exercised here
        raise TransportClosed("idle")

    async def send(self, message: dict) -> None:  # pragma: no cover
        pass

    async def close(self) -> None:
        self.closed = True


def _driver(coordinator, *, paper_mode, open_socket, run_step, sleeps, events):
    async def sleep(delay):
        sleeps.append(delay)

    return ReconnectDriver(
        coordinator,
        paper_mode=paper_mode,
        open_socket=open_socket,
        run_step=run_step,
        sleep=sleep,
        on_event=events.append,
    )


# -- happy path: paper skips private steps, returns fresh socket --------

def test_paper_restore_sequence_skips_private_steps():
    coordinator = ShardReconnectCoordinator()
    fresh = _FakeTransport()
    steps: list = []
    sleeps: list = []
    events: list = []

    async def open_socket():
        return fresh

    async def run_step(step):
        steps.append(step)

    driver = _driver(coordinator, paper_mode=True, open_socket=open_socket,
                     run_step=run_step, sleeps=sleeps, events=events)
    result = asyncio.run(driver.initiate(2, DisconnectReason.RANDOM))

    assert result is fresh
    # WM-RECONNECT-019: private-side steps (token / private resub / rate ceiling /
    # mirror) skipped in paper; only the public-side steps run, in figure order.
    assert steps == [
        RestoreStep.RESUBSCRIBE_PUBLIC,
        RestoreStep.RESUME_KEEPALIVE,
        RestoreStep.RESTORE_TICKER_TRIGGER,
    ]
    assert isinstance(events[0], ReconnectInitiated)
    assert isinstance(events[-1], ReconnectComplete)
    assert coordinator.any_reconnecting() is False  # gate lifted after restore


def test_live_restore_includes_private_steps():
    coordinator = ShardReconnectCoordinator()
    steps: list = []

    async def open_socket():
        return _FakeTransport()

    async def run_step(step):
        steps.append(step)

    driver = _driver(coordinator, paper_mode=False, open_socket=open_socket,
                     run_step=run_step, sleeps=[], events=[])
    asyncio.run(driver.initiate(0, DisconnectReason.RANDOM))

    assert steps == [
        RestoreStep.ACQUIRE_WS_TOKEN,
        RestoreStep.RESUBSCRIBE_PUBLIC,
        RestoreStep.RESUBSCRIBE_PRIVATE,
        RestoreStep.RESET_RATE_CEILING,
        RestoreStep.RESUME_KEEPALIVE,
        RestoreStep.RESTORE_POSITION_MIRROR,
        RestoreStep.RESTORE_TICKER_TRIGGER,
    ]


# -- HR-WM-012: the in-progress gate is held DURING restore -------------

def test_gate_held_during_restore_and_lifted_after():
    coordinator = ShardReconnectCoordinator()
    seen_during: list = []

    async def open_socket():
        seen_during.append(coordinator.any_reconnecting())
        return _FakeTransport()

    async def run_step(step):
        seen_during.append(coordinator.any_reconnecting())

    driver = _driver(coordinator, paper_mode=True, open_socket=open_socket,
                     run_step=run_step, sleeps=[], events=[])
    asyncio.run(driver.initiate(1, DisconnectReason.RANDOM))

    assert all(seen_during)  # gate up for every step (candles discarded meanwhile)
    assert coordinator.any_reconnecting() is False  # cleared once restore completes


# -- WS-REC-003: Scenario-A backoff seed schedule across attempts -------

def test_scenario_a_backoff_seed_applied_until_success():
    coordinator = ShardReconnectCoordinator()
    sleeps: list = []
    fails = {"left": 6}  # fail the socket open 6 times, succeed on attempt 7

    async def open_socket():
        if fails["left"] > 0:
            fails["left"] -= 1
            raise TransportClosed("still down")
        return _FakeTransport()

    async def run_step(step):
        pass

    driver = _driver(coordinator, paper_mode=True, open_socket=open_socket,
                     run_step=run_step, sleeps=sleeps, events=[])
    asyncio.run(driver.initiate(0, DisconnectReason.RANDOM))

    # Attempts 1..7 -> seed[0..6] = 5 immediate (0 s) then 1 s, 2 s.
    assert sleeps == list(RECONNECT_BACKOFF_SEED_SEC[:7])
    assert sleeps == [0.0, 0.0, 0.0, 0.0, 0.0, 1.0, 2.0]
    assert coordinator.any_reconnecting() is False


def test_scenario_b_uses_five_second_floor():
    coordinator = ShardReconnectCoordinator()
    sleeps: list = []

    async def open_socket():
        return _FakeTransport()

    async def run_step(step):
        pass

    driver = _driver(coordinator, paper_mode=True, open_socket=open_socket,
                     run_step=run_step, sleeps=sleeps, events=[])
    asyncio.run(driver.initiate(0, DisconnectReason.MAINTENANCE))

    assert sleeps == [SCENARIO_B_MIN_DELAY_SEC]  # 5 s post-maintenance floor


# -- never abandon + partial-socket cleanup -----------------------------

def test_partial_restore_closes_socket_then_retries():
    coordinator = ShardReconnectCoordinator()
    opened: list = []
    attempts = {"n": 0}

    async def open_socket():
        t = _FakeTransport()
        opened.append(t)
        return t

    async def run_step(step):
        # Fail the first attempt's first run_step (after the socket opened) so the
        # partial socket must be closed and the whole sequence retried.
        if step is RestoreStep.RESUBSCRIBE_PUBLIC and attempts["n"] == 0:
            attempts["n"] = 1
            raise TransportClosed("resubscribe failed")

    driver = _driver(coordinator, paper_mode=True, open_socket=open_socket,
                     run_step=run_step, sleeps=[], events=[])
    result = asyncio.run(driver.initiate(0, DisconnectReason.RANDOM))

    assert len(opened) == 2          # first socket abandoned, second succeeded
    assert opened[0].closed is True  # partial socket torn down
    assert result is opened[1]
    assert coordinator.any_reconnecting() is False
