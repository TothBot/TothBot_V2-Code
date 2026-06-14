"""Async WS transport edge - the injectable socket boundary for the receive loop.

Source: 0500000 dv1_241 sec 2 Image1 (D1 CONNECTION/LIBRARY wire facts
WS-LIB-001..004, WS-EP-003) + sec 7 mod:WS_Manager. This is the SINGLE place the
real Kraken WebSocket library is touched on the inbound side. Everything above it
(the per-shard receive loop, the keepalive/silent-pair/reconcile policy) drives a
Transport through three async methods and is therefore unit-testable without a
network: a hand-driven fake Transport feeds frames and raises drops.

WS-LIB-001: the mandatory client is the asyncio-native ``websockets.asyncio.client
.connect``; the legacy top-level ``websockets.connect()`` is PROHIBITED. WS-LIB-002
..004 / rule:HR-WM-002 fix the four connection invariants (max_size=10 MB,
open_timeout=10 s, max_queue=None, ping_interval=None) - supplied by
connection.ws_connect_kwargs(). The ``websockets`` package is a VPS-runtime
dependency (Python 3.12.3); it is imported LAZILY inside connect() so importing
this module - and the whole test suite - never requires the library to be present.

Kraken WS v2 frames are JSON text objects; the transport encodes/decodes JSON at
this boundary so the layers above deal only in dicts (mirrors the dispatch-table /
candle parsing contract). A dropped or closed socket surfaces as TransportClosed -
a transient error the per-shard receive loop catches LOCALLY and turns into a
reconnect (rule:HR-WM-029 shard independence; line "transient WS errors caught
LOCALLY -> _initiate_reconnect").
"""

from __future__ import annotations

import json
from typing import Protocol, runtime_checkable

from .connection import ConnectionRole, endpoint_for, ws_connect_kwargs


class TransportClosed(Exception):
    """The underlying socket closed or dropped (clean close or mid-stream drop).

    A TRANSIENT error: the shard receive loop catches it locally and drives
    _initiate_reconnect for that shard only (rule:HR-WM-029). The real adapter
    maps every websockets connection-closed / OS socket error onto this single
    type so the loop never imports - or branches on - library exception classes.
    """


@runtime_checkable
class Transport(Protocol):
    """The async socket contract the receive loop drives at the I/O edge.

    recv() returns the next already-decoded JSON frame (a dict) or raises
    TransportClosed when the socket is gone. send() transmits one JSON-encodable
    message (the application ping, a subscribe RPC). close() tears the socket down
    on a deliberate reconnect. Implemented for real by WebsocketsTransport and for
    tests by a hand-driven fake.
    """

    async def send(self, message: dict) -> None: ...

    async def recv(self) -> dict: ...

    async def close(self) -> None: ...


class WebsocketsTransport:
    """Real adapter over one open ``websockets`` asyncio-native connection.

    Wraps an already-connected client object (opened by connect() below) and does
    only three things: JSON-encode on send, JSON-decode on recv, and map the
    library's connection-closed / OS errors onto TransportClosed. It holds NO
    policy - the receive loop owns liveness, dispatch, and reconnect decisions.

    The wrapped client and the library exception types are passed in (or bound by
    connect()) so this class itself never imports ``websockets`` at definition
    time. The Kraken connection_id (WS-STAT-005/006) is NOT known here - it arrives
    on the status frame and is logged by the receive loop.
    """

    def __init__(self, ws: object, *, closed_excs: tuple[type[BaseException], ...]) -> None:
        self._ws = ws
        # The library connection-closed exception classes to translate (passed in
        # so this module needs no static dependency on ``websockets``). OSError is
        # always translated too (a TCP-level drop).
        self._closed_excs = closed_excs

    async def send(self, message: dict) -> None:
        try:
            await self._ws.send(json.dumps(message))  # type: ignore[attr-defined]
        except self._closed_excs as exc:
            raise TransportClosed(f"send failed: {exc!r}") from exc
        except OSError as exc:
            raise TransportClosed(f"send failed: {exc!r}") from exc

    async def recv(self) -> dict:
        try:
            raw = await self._ws.recv()  # type: ignore[attr-defined]
        except self._closed_excs as exc:
            raise TransportClosed(f"recv failed: {exc!r}") from exc
        except OSError as exc:
            raise TransportClosed(f"recv failed: {exc!r}") from exc
        return json.loads(raw)

    async def close(self) -> None:
        # A deliberate close must never raise: we are tearing down to reconnect.
        try:
            await self._ws.close()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - close is best-effort on a dying socket
            pass


async def connect(role: ConnectionRole, *, auth_token: str | None = None) -> WebsocketsTransport:
    """Open one Kraken WS v2 connection for ``role`` and wrap it as a Transport.

    LAZILY imports ``websockets`` (the VPS-runtime dependency) so this module's
    import - and the test suite - never needs the library. Uses the mandatory
    asyncio-native client (WS-LIB-001) with the fixed connection invariants
    (WS-LIB-002..004 / rule:HR-WM-002 via ws_connect_kwargs()).

    auth_token is reserved for the private endpoint (live only; PA-004 div #1):
    the token from REST GetWebSocketsToken authorises the private subscribe RPCs
    after connect; it is not part of the connect() handshake itself, so it is
    accepted here only to keep the public/private call sites symmetric.
    """
    from websockets.asyncio.client import connect as ws_connect  # lazy (WS-LIB-001)
    from websockets.exceptions import ConnectionClosed

    url = endpoint_for(role)
    ws = await ws_connect(url, **ws_connect_kwargs())
    return WebsocketsTransport(ws, closed_excs=(ConnectionClosed,))
