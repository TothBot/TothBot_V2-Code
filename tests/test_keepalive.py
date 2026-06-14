"""S2c tests: connection-liveness keepalive (ping/pong + zombie detection).

Covers 0500000 dv1_240 sec 2 Image1 A-7 / A-8 + sec 7 mod:WS_Manager
rule:HR-WM-003 / rule:HR-WM-004 (D1 wire facts WS-PING-002, WS-ZOM-003):
30 s application ping, 10 s pong timeout = dead, 90 s no-real-data = zombie,
real-data-only timer reset (pong/heartbeat do NOT reset it).
"""

from __future__ import annotations

from tothbot.exchange.keepalive import (
    PING_INTERVAL_SEC,
    PING_MESSAGE,
    PONG_TIMEOUT_SEC,
    ZOMBIE_LOG_KEY,
    ZOMBIE_THRESHOLD_SEC,
    ConnectionKeepalive,
    Liveness,
)


class FakeClock:
    """A manually advanced monotonic clock for deterministic timer tests."""

    def __init__(self, t: float = 1000.0) -> None:
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, dt: float) -> float:
        self.t += dt
        return self.t


# -- constants match the diagram ----------------------------------------

def test_keepalive_constants_match_diagram():
    assert PING_INTERVAL_SEC == 30.0       # A-7 / WS-PING-002
    assert PONG_TIMEOUT_SEC == 10.0        # A-7 / WS-PING-002
    assert ZOMBIE_THRESHOLD_SEC == 90.0    # A-8 / WS-ZOM-003
    assert PING_MESSAGE == {"method": "ping"}
    assert ZOMBIE_LOG_KEY == "ZOMBIE_CONNECTION_DETECTED"


# -- ping cadence (A-7 / HR-WM-003) -------------------------------------

def test_ping_due_after_30s():
    clk = FakeClock()
    ka = ConnectionKeepalive(clock=clk)
    assert not ka.due_for_ping()       # just connected
    clk.advance(29.9)
    assert not ka.due_for_ping()       # not yet 30 s
    clk.advance(0.1)
    assert ka.due_for_ping()           # exactly 30 s


def test_ping_not_stacked_while_awaiting_pong():
    clk = FakeClock()
    ka = ConnectionKeepalive(clock=clk)
    clk.advance(30)
    assert ka.due_for_ping()
    ka.mark_ping_sent()
    clk.advance(30)                    # another interval, but pong still pending
    assert not ka.due_for_ping()       # never stack a ping on an unanswered one


def test_pong_clears_and_next_ping_schedules():
    clk = FakeClock()
    ka = ConnectionKeepalive(clock=clk)
    clk.advance(30)
    ka.mark_ping_sent()
    clk.advance(2)
    ka.mark_pong()
    assert not ka.pong_overdue()
    assert not ka.due_for_ping()       # 2 s since the ping
    clk.advance(28)
    assert ka.due_for_ping()           # 30 s since the ping -> next one due


# -- pong timeout = dead (WS-PING-002) ----------------------------------

def test_no_pong_within_10s_is_dead():
    clk = FakeClock()
    ka = ConnectionKeepalive(clock=clk)
    clk.advance(30)
    ka.mark_ping_sent()
    clk.advance(9.9)
    assert not ka.pong_overdue()
    assert ka.liveness() is Liveness.ALIVE
    clk.advance(0.1)                   # 10 s with no pong
    assert ka.pong_overdue()
    assert ka.liveness() is Liveness.DEAD_NO_PONG


def test_pong_before_deadline_keeps_alive():
    clk = FakeClock()
    ka = ConnectionKeepalive(clock=clk)
    clk.advance(30)
    ka.mark_ping_sent()
    clk.advance(9)
    ka.mark_pong()
    clk.advance(30)                    # past the would-be 10 s deadline, under zombie
    assert not ka.pong_overdue()       # pong cleared the deadline
    assert ka.liveness() is Liveness.ALIVE


# -- zombie detection (A-8 / WS-ZOM-003) --------------------------------

def test_zombie_after_90s_no_real_data():
    clk = FakeClock()
    ka = ConnectionKeepalive(clock=clk)
    clk.advance(90.0)
    assert not ka.is_zombie()          # 90 s is the threshold, not yet > 90
    clk.advance(0.1)
    assert ka.is_zombie()
    assert ka.liveness() is Liveness.ZOMBIE


def test_real_data_resets_zombie_timer():
    clk = FakeClock()
    ka = ConnectionKeepalive(clock=clk)
    clk.advance(80)
    ka.mark_real_data()                # real market data arrives
    clk.advance(80)                    # 80 s since that data: still < 90
    assert not ka.is_zombie()
    assert abs(ka.seconds_since_real_data() - 80) < 1e-9


def test_pong_does_not_reset_zombie_timer():
    # A-8: a connection can pass ping/pong while delivering NO real data.
    clk = FakeClock()
    ka = ConnectionKeepalive(clock=clk)
    clk.advance(30)
    ka.mark_ping_sent()
    ka.mark_pong()                     # ping/pong healthy...
    clk.advance(61)                    # ...but 91 s total with no real data
    assert ka.is_zombie()              # pong must NOT have reset the timer
    assert ka.liveness() is Liveness.ZOMBIE


def test_dead_pong_reported_before_zombie():
    # Both failures stale at once; the acuter pong timeout wins the verdict.
    clk = FakeClock()
    ka = ConnectionKeepalive(clock=clk)
    clk.advance(95)                    # zombie territory already
    ka.mark_ping_sent()
    clk.advance(11)                    # pong overdue too
    assert ka.is_zombie()
    assert ka.pong_overdue()
    assert ka.liveness() is Liveness.DEAD_NO_PONG


# -- reset on reconnect (WS-REC-004) ------------------------------------

def test_reset_clears_outstanding_ping_and_zombie():
    clk = FakeClock()
    ka = ConnectionKeepalive(clock=clk)
    clk.advance(95)
    ka.mark_ping_sent()
    clk.advance(20)
    assert ka.liveness() is not Liveness.ALIVE
    ka.reset()                         # reconnect resumes ping + zombie tasks
    assert not ka.pong_overdue()
    assert not ka.is_zombie()
    assert ka.liveness() is Liveness.ALIVE
    assert not ka.due_for_ping()
