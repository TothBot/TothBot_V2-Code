"""mod:Risk_Engine ar:AR-043 - the per-module capital-commitment dispatch semaphore (G7 CHECK 4).

Exercises tothbot/risk/dispatch_semaphore.py: the bound-1 non-blocking acquire, the bounded release
(over-release raises), the non-mutating locked() probe, and the position-lifetime cycle (D2 ruling:
acquire at entry, release at close, max one open commitment).
"""

from __future__ import annotations

import pytest

from tothbot.risk.dispatch_semaphore import DispatchSemaphore


def test_acquire_then_locked_then_release():
    s = DispatchSemaphore()
    assert s.locked() is False
    assert s.acquire() is True          # acquired the single slot
    assert s.locked() is True
    s.release()
    assert s.locked() is False


def test_second_acquire_while_held_returns_false_and_does_not_mutate():
    s = DispatchSemaphore()
    assert s.acquire() is True
    assert s.acquire() is False         # bound 1 - no double commit
    assert s.locked() is True           # still exactly one held
    s.release()
    assert s.locked() is False


def test_over_release_raises():
    s = DispatchSemaphore()
    with pytest.raises(ValueError):
        s.release()                     # nothing held -> over-release (a lifecycle defect)


def test_reacquire_after_release():
    s = DispatchSemaphore()
    s.acquire()
    s.release()
    assert s.acquire() is True          # the slot is free again (next position)
    assert s.locked() is True


def test_locked_probe_does_not_acquire():
    s = DispatchSemaphore()
    assert s.locked() is False
    assert s.locked() is False          # probing never mutates
    assert s.acquire() is True          # still free to acquire after probing
