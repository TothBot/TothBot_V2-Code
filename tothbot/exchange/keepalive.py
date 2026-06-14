"""mod:WS_Manager connection-liveness: application ping keepalive + zombie detection.

Source: 0500000 dv1_240 sec 2 Image1 (A-7 / A-8) + sec 7 mod:WS_Manager desc
(rule:HR-WM-003, rule:HR-WM-004; D1 keepalive wire facts WS-PING-002, WS-ZOM-003).

Two independent liveness defences run on EVERY active WS connection:

  PING / PONG (A-7 / rule:HR-WM-003 / WS-PING-002): the Kraken WS server drops a
    connection after ~60 s of inactivity (Cloudflare idle ~100 s). WS_Manager
    sends an application-level JSON ping {"method": "ping"} every 30 s and expects
    {"method": "pong"} within 10 s. No pong within 10 s = DEAD connection ->
    reconnect immediately. This is distinct from TCP keepalive (the library TCP
    PING is disabled, ping_interval=None per rule:HR-WM-002).

  ZOMBIE (A-8 / rule:HR-WM-004 / WS-ZOM-003): a connection can pass ping/pong
    while delivering NO real market data. WS_Manager tracks last_real_data_time
    per connection via time.monotonic(). ONLY actual market-data events reset it
    - heartbeat AND pong messages do NOT. If elapsed > 90 s: log
    ZOMBIE_CONNECTION_DETECTED, alert the operator, reconnect.

This module is a PURE timer over an injected monotonic clock - no socket, no
asyncio - so the policy is unit-testable without a network. The async read loop
(the I/O edge) drives the marker methods and acts on the liveness verdict.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from enum import Enum

# --- fixed engineering constants (A-7 / A-8; not CIATS-owned) -----------------
PING_INTERVAL_SEC = 30.0     # send an application ping this often (A-7 / WS-PING-002)
PONG_TIMEOUT_SEC = 10.0      # no pong within this window = dead (A-7 / WS-PING-002)
ZOMBIE_THRESHOLD_SEC = 90.0  # no real data beyond this = zombie (A-8 / WS-ZOM-003)

# The application-level ping frame (A-7). Distinct from the library TCP PING,
# which is disabled (rule:HR-WM-002 ping_interval=None).
PING_MESSAGE: dict[str, str] = {"method": "ping"}

# Canonical log keys the read loop emits on a liveness failure (the diagram
# names ZOMBIE_CONNECTION_DETECTED explicitly; the dead-pong key mirrors it).
ZOMBIE_LOG_KEY = "ZOMBIE_CONNECTION_DETECTED"  # WS-ZOM-003
DEAD_CONNECTION_LOG_KEY = "WS_PONG_TIMEOUT"    # WS-PING-002 dead-connection limb

# An injectable monotonic clock (time.monotonic by default; never wall-clock -
# the timers measure elapsed intervals, so a clock that can jump backwards or
# is subject to NTP steps would corrupt the dead/zombie decisions).
Clock = Callable[[], float]


class Liveness(Enum):
    """The health verdict for one connection at a point in time."""

    ALIVE = "alive"
    DEAD_NO_PONG = "dead_no_pong"  # ping outstanding > 10 s (WS-PING-002) -> reconnect
    ZOMBIE = "zombie"             # no real data > 90 s (WS-ZOM-003)    -> reconnect


class ConnectionKeepalive:
    """Per-connection ping/pong liveness + zombie-data-staleness timer.

    Construct (or reset()) at the moment a connection reaches CONNECTED. The
    read loop then: calls due_for_ping(now) on its scheduler tick and, when True,
    sends PING_MESSAGE and calls mark_ping_sent(now); calls mark_pong(now) on a
    pong frame; calls mark_real_data(now) on every real market-data frame (NOT on
    pong or heartbeat); and consults liveness(now) to decide whether to reconnect.
    """

    def __init__(self, *, clock: Clock = time.monotonic) -> None:
        self._clock = clock
        self.reset()

    def reset(self, now: float | None = None) -> None:
        """(Re)initialise all timers - called on every (re)connect (WS-REC-004
        resumes the ping + zombie tasks). A fresh connection has no outstanding
        ping and a just-now real-data baseline.
        """
        t = self._clock() if now is None else now
        self._last_ping_at = t       # when the last scheduled ping was sent
        self._awaiting_pong = False  # True between ping sent and pong received
        self._pong_deadline = 0.0    # _last_ping_at + PONG_TIMEOUT_SEC when awaiting
        self._last_real_data = t     # last real market-data event (zombie timer)

    # --- ping / pong (A-7 / rule:HR-WM-003 / WS-PING-002) --------------------
    def due_for_ping(self, now: float | None = None) -> bool:
        """True when >=30 s have elapsed since the last ping and none is in
        flight. A ping is never stacked on an unanswered one: if a pong is still
        outstanding at the next interval, the 10 s pong timeout (4x shorter) has
        already condemned the connection.
        """
        t = self._clock() if now is None else now
        return not self._awaiting_pong and (t - self._last_ping_at) >= PING_INTERVAL_SEC

    def mark_ping_sent(self, now: float | None = None) -> None:
        """Record that an application ping was just transmitted; arm the 10 s
        pong deadline (WS-PING-002).
        """
        t = self._clock() if now is None else now
        self._last_ping_at = t
        self._awaiting_pong = True
        self._pong_deadline = t + PONG_TIMEOUT_SEC

    def mark_pong(self, now: float | None = None) -> None:
        """Record a pong; clears the outstanding-ping state. A pong does NOT
        reset the zombie timer (A-8: only real market data does).
        """
        self._awaiting_pong = False

    def pong_overdue(self, now: float | None = None) -> bool:
        """True when a ping is outstanding and its 10 s pong window has passed -
        a DEAD connection (WS-PING-002), reconnect immediately.
        """
        if not self._awaiting_pong:
            return False
        t = self._clock() if now is None else now
        return t >= self._pong_deadline

    # --- zombie detection (A-8 / rule:HR-WM-004 / WS-ZOM-003) ----------------
    def mark_real_data(self, now: float | None = None) -> None:
        """Reset the zombie timer. Call ONLY on actual market-data events -
        never on pong or heartbeat frames (A-8 / rule:HR-WM-004).
        """
        self._last_real_data = self._clock() if now is None else now

    def seconds_since_real_data(self, now: float | None = None) -> float:
        t = self._clock() if now is None else now
        return t - self._last_real_data

    def is_zombie(self, now: float | None = None) -> bool:
        """True when no real market data has arrived for > 90 s (WS-ZOM-003)."""
        return self.seconds_since_real_data(now) > ZOMBIE_THRESHOLD_SEC

    # --- combined verdict ----------------------------------------------------
    def liveness(self, now: float | None = None) -> Liveness:
        """The connection's health. Both failure modes mandate a reconnect; the
        pong timeout is reported first as the more acute (10 s vs 90 s) failure.
        """
        t = self._clock() if now is None else now
        if self.pong_overdue(t):
            return Liveness.DEAD_NO_PONG
        if self.is_zombie(t):
            return Liveness.ZOMBIE
        return Liveness.ALIVE

    @property
    def is_alive(self) -> bool:
        return self.liveness() is Liveness.ALIVE
