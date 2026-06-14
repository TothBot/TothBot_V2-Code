"""mod:WS_Manager async reconnect driver - the _initiate_reconnect I/O body.

Source: 0500000 dv1_241 sec 2 Image1 (ar:AR-056 mid-session restore; ar:AR-080
Cloudflare ceiling; "transient WS errors caught LOCALLY -> _initiate_reconnect") +
sec 7 mod:WS_Manager desc (the D1 RECONNECT-RESIDUAL wire facts WS-REC-003 /
WS-REC-004; rule:HR-WM-012 in-progress gate; rule:HR-WM-016 separate-code-path;
contract:WM-RECONNECT-019 paper gating) + the event_registry (RECONNECT_INITIATED /
RECONNECT_COMPLETE).

This is the async orchestration that wraps the PURE reconnect policy
(reconnect.py): it does NOT decide the scenario, the per-attempt delay, or the
restore-step set - those are reconnect.select_scenario / reconnect_delay_sec /
build_restore_sequence. The driver only SEQUENCES them in real time:

  1. coordinator.begin(shard, reason) selects the scenario and raises the
     rule:HR-WM-012 in-progress flag (so every shard's receive loop now discards
     ohlc_5m candles - the pipeline must not fire on a partial universe). The flag
     stays up for the WHOLE reconnect and is cleared ONLY once restore completes.
  2. For each attempt: sleep reconnect_delay_sec(scenario, attempt) - the
     CIATS-owned paper-validated backoff seed for Scenario A (5 immediate, then
     1-16 s, then the 30 s cap), or the Scenario-B 5 s floor - then run the
     WS-REC-004 restore sequence. A transient failure (TransportClosed) drops to
     the next attempt; reconnection is NEVER abandoned (loss-min: a stopped
     reconnect leaves positions unmanaged with only the L3 emergSL; ar:AR-080
     still bounds the rate as the delay holds at the cap).
  3. The restore sequence is build_restore_sequence(paper_mode) - the private-side
     steps (token, private re-subscribe, rate ceiling, Position Mirror) are skipped
     in paper (contract:WM-RECONNECT-019; PA-004 div #1). RECONNECT_SOCKET opens
     the fresh socket (returned to the shard receive loop to read from next); every
     other step is delegated to the injected run_step, which performs the real
     side-effects (REST token, paced re-subscribe + ACK parse, maxratecount reset,
     keepalive.reset + zombie/ping resume, silent-pair re-arm, reconcile.reset,
     Position Mirror restore, ticker event_trigger restore). Keeping run_step
     injected makes the driver a faithful, unit-testable SEQUENCER of the figure's
     own ordered steps.

The driver is shared across all shards (the coordinator is the single source of
the global HR-WM-012 gate). sleep, the socket opener, and the per-step executor are
injected so the entire backoff + restore sequence is driven with stdlib asyncio.run
over fakes - no network, no real timers.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

from .reconnect import (
    DisconnectReason,
    RestoreStep,
    ShardReconnectCoordinator,
    build_restore_sequence,
    reconnect_delay_sec,
)
from .transport import Transport, TransportClosed

OpenSocket = Callable[[], Awaitable[Transport]]
RunStep = Callable[[RestoreStep], Awaitable[None]]
Sleep = Callable[[float], Awaitable[None]]
EventSink = Callable[[object], None]


@dataclass(frozen=True)
class ReconnectInitiated:
    """RECONNECT_INITIATED [WARNING] {connection_id, attempt_number} - emitted at
    the start of every reconnect attempt (the backoff sleep precedes the restore)."""

    shard_index: int
    attempt_number: int
    connection_id: int | None = None
    code: str = field(default="RECONNECT_INITIATED", init=False)


@dataclass(frozen=True)
class ReconnectComplete:
    """RECONNECT_COMPLETE [INFO] {connection_id} - the WS-REC-004 restore sequence
    finished; the shard is live again and the HR-WM-012 gate lifts."""

    shard_index: int
    connection_id: int | None = None
    code: str = field(default="RECONNECT_COMPLETE", init=False)


class ReconnectDriver:
    """Async per-shard reconnect orchestration over the pure coordinator + policy.

    Construct ONCE per process with the shared ShardReconnectCoordinator (the
    single HR-WM-012 gate), the run mode, the async open_socket (RECONNECT_SOCKET),
    and run_step (every other WS-REC-004 step). initiate() is bound per shard into
    each receive loop as its initiate_reconnect callback and returns the fresh
    Transport once restore completes.
    """

    def __init__(
        self,
        coordinator: ShardReconnectCoordinator,
        *,
        paper_mode: bool,
        open_socket: OpenSocket,
        run_step: RunStep,
        sleep: Sleep = asyncio.sleep,
        on_event: EventSink | None = None,
    ) -> None:
        self._coordinator = coordinator
        self._paper_mode = paper_mode
        self._open_socket = open_socket
        self._run_step = run_step
        self._sleep = sleep
        self._on_event = on_event

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    async def initiate(
        self,
        shard_index: int,
        reason: DisconnectReason,
        *,
        connection_id: int | None = None,
    ) -> Transport:
        """Run the full reconnect for one shard and return its fresh Transport.

        Holds the rule:HR-WM-012 in-progress flag for the whole reconnect (cleared
        only when restore completes) and never abandons (loss-min)."""
        scenario = self._coordinator.begin(shard_index, reason)  # HR-WM-012 gate ON
        attempt = 0
        try:
            while True:
                attempt += 1
                self._emit(ReconnectInitiated(shard_index, attempt, connection_id))
                # Backoff BEFORE the attempt: Scenario-A seed schedule (0 s for the
                # 5 immediate attempts) or the Scenario-B 5 s floor.
                await self._sleep(reconnect_delay_sec(scenario, attempt))
                try:
                    transport = await self._run_restore()
                except TransportClosed:
                    continue  # attempt failed; never abandon - next attempt
                self._emit(ReconnectComplete(shard_index, connection_id))
                return transport
        finally:
            # Gate lifts ONLY here - after restore completes (or on an unexpected
            # propagating error). The never-abandon loop otherwise only exits via
            # the success return above.
            self._coordinator.complete(shard_index)

    async def _run_restore(self) -> Transport:
        """Execute the WS-REC-004 restore sequence for this mode in figure order.

        RECONNECT_SOCKET opens the fresh socket; every other (mode-eligible) step is
        delegated to run_step. A failure part-way closes the partial socket and
        re-raises TransportClosed so initiate() retries the whole sequence."""
        transport: Transport | None = None
        try:
            for step in build_restore_sequence(paper_mode=self._paper_mode):
                if step is RestoreStep.RECONNECT_SOCKET:
                    transport = await self._open_socket()
                else:
                    await self._run_step(step)
            if transport is None:  # RECONNECT_SOCKET is always in the sequence
                raise TransportClosed("restore sequence opened no socket")
            return transport
        except TransportClosed:
            if transport is not None:
                await transport.close()
            raise
