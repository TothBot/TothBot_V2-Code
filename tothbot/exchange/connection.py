"""mod:WS_Manager connection lifecycle shell + Kraken endpoint/library invariants.

Source: 0500000 dv1_240 sec 2 Image1 (Kraken API Connection Architecture).
This module holds the cold-start connection PRIMITIVES only - the fixed
endpoint URLs, the WS-client connection invariants, and a pure connection
lifecycle state machine. The actual async socket I/O is injected at the edge
(the websockets read loop) so the lifecycle is unit-testable without a network.

DEFERRED to S2c (per TB00703): mid-session reconnect (ar:AR-056 /
contract:WM-RECONNECT-016), the 30s application ping keepalive (A-7 /
rule:HR-WM-003), zombie-connection detection (A-8 / rule:HR-WM-004), the
Cloudflare reconnect ceiling (ar:AR-080), PATH-2 connection sharding
(contract:WM-SHARD-001) and subscribe pacing (contract:WM-PACE-001/002).
"""

from __future__ import annotations

from enum import Enum


# --- Kraken endpoints (WS-EP-003, REST-HTTP-006/007, REST-STAT-002) ----------
# Public WS v2 (no auth): instrument / status / ohlc_5m / ohlc_60m / ticker.
PUBLIC_WS_URL = "wss://ws.kraken.com/v2"
# Private WS v2 (authenticated): executions / balances + order RPCs. Live only
# (PA-004 divergence #1); NEVER connected in paper mode (rule:HR-WM-022).
PRIVATE_WS_URL = "wss://ws-auth.kraken.com/v2"
# REST base for all /0/public/* (GET) and /0/private/* (POST) endpoints.
REST_BASE_URL = "https://api.kraken.com"
# Kraken Status page API (public, no auth) - startup maintenance check (AR-038).
STATUS_API_URL = "https://status.kraken.com/api/v2/scheduled-maintenances/upcoming.json"

# WS client library (WS-LIB-001): the asyncio-native client is mandatory; the
# legacy top-level ``websockets.connect()`` path is PROHIBITED.
WS_CLIENT_IMPORT = "websockets.asyncio.client.connect"

# --- WS connection invariants (rule:HR-WM-002 / WS-LIB-002..004 / A-18) -------
# 10 MB read cap: the instrument snapshot for 500+ pairs exceeds the 1 MB
# library default, which would silently disconnect with PayloadTooBig (A-18).
WS_MAX_SIZE = 10 * 1024 * 1024
# Connection-open timeout in seconds (A-18).
WS_OPEN_TIMEOUT = 10
# No inbound-frame queue bound - the dispatch loop must never apply backpressure
# to the socket (rule:HR-WM-002).
WS_MAX_QUEUE = None
# Library TCP-level PING disabled; TothBot sends its own application-level JSON
# ping every 30 s instead (rule:HR-WM-003, wired in S2c).
WS_PING_INTERVAL = None


def ws_connect_kwargs() -> dict[str, object]:
    """The mandatory keyword arguments for every Kraken WS v2 connect() call.

    A fresh dict each call so a caller can extend it without mutating shared
    state. These four values are fixed engineering invariants (HR-WM-002),
    not CIATS-owned parameters.
    """
    return {
        "max_size": WS_MAX_SIZE,
        "open_timeout": WS_OPEN_TIMEOUT,
        "max_queue": WS_MAX_QUEUE,
        "ping_interval": WS_PING_INTERVAL,
    }


class ConnectionRole(Enum):
    """Which Kraken WS endpoint a connection serves."""

    PUBLIC = "public"    # wss://ws.kraken.com/v2
    PRIVATE = "private"  # wss://ws-auth.kraken.com/v2 (live only; PA-004 #1)


def endpoint_for(role: ConnectionRole) -> str:
    """Resolve the WS URL for a connection role."""
    return PUBLIC_WS_URL if role is ConnectionRole.PUBLIC else PRIVATE_WS_URL


class ConnectionState(Enum):
    """The lifecycle state of a single WS connection."""

    DISCONNECTED = "disconnected"  # initial / not yet opened
    CONNECTING = "connecting"      # connect() in flight
    CONNECTED = "connected"        # open and serving frames
    CLOSED = "closed"              # closed (clean or dropped)


# Allowed lifecycle transitions. CLOSED -> CONNECTING permits the S2c reconnect
# path; an illegal transition is a programming error and raises.
_ALLOWED: dict[ConnectionState, frozenset[ConnectionState]] = {
    ConnectionState.DISCONNECTED: frozenset({ConnectionState.CONNECTING}),
    ConnectionState.CONNECTING: frozenset(
        {ConnectionState.CONNECTED, ConnectionState.CLOSED}
    ),
    ConnectionState.CONNECTED: frozenset({ConnectionState.CLOSED}),
    ConnectionState.CLOSED: frozenset({ConnectionState.CONNECTING}),
}


class WSConnection:
    """Pure lifecycle state for one Kraken WS connection.

    Tracks role, state, and connection_id. connection_id is MANDATORY to log
    on every new connection (WS-STAT-006) and is supplied by the WS read loop
    once Kraken returns it on the status frame. No socket lives here; the I/O
    edge drives the marker methods as the real connection progresses.
    """

    def __init__(self, role: ConnectionRole) -> None:
        self.role = role
        self.state = ConnectionState.DISCONNECTED
        self.connection_id: int | None = None

    @property
    def url(self) -> str:
        return endpoint_for(self.role)

    @property
    def is_connected(self) -> bool:
        return self.state is ConnectionState.CONNECTED

    def _transition(self, target: ConnectionState) -> None:
        if target not in _ALLOWED[self.state]:
            raise ValueError(
                f"illegal {self.role.value} connection transition "
                f"{self.state.value} -> {target.value}"
            )
        self.state = target

    def mark_connecting(self) -> None:
        """connect() is in flight."""
        self._transition(ConnectionState.CONNECTING)
        self.connection_id = None

    def mark_connected(self, connection_id: int) -> None:
        """Connection is open; bind the Kraken connection_id (WS-STAT-006)."""
        self._transition(ConnectionState.CONNECTED)
        self.connection_id = connection_id

    def mark_closed(self) -> None:
        """Connection closed (clean shutdown or drop)."""
        self._transition(ConnectionState.CLOSED)
        self.connection_id = None
