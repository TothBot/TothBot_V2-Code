"""mod:WS_Manager private connection + executions ingest (live only; PA-004 div #1).

Source: 0500000 dv1_241 sec 7 container:Private_WS_v2 (executions/balances streams +
order-dispatch RPCs; order_status:true MANDATORY on every subscribe per rule:HR-WM-005;
snap_orders:true; ratecounter:true) + sec 2 Image1 AR-049 startup sequence steps 5/6
(connect private WS + subscribe executions/balances; snap_orders reconciliation ->
Position Mirror) + the ar:AR-056 / WS-REC-004 restore sequence (private subset).

This is the SINGLE private connection (NOT a public shard). It exists ONLY in live
mode (rule:HR-WM-022; paper keeps self._ws_private None for the whole session,
PA-004 divergence point #1). It closes the live fill -> mirror loop:

  inbound   the private receive loop routes each executions frame to the ingest,
            which feeds WSManager.record_execution() (the rule:HR-PM-009 sole-writer
            surface) per fill, and reconciles the snapshot (snap_orders) through
            WSManager.restore_position_mirror() (AR-056 / startup Step 6).
  outbound  on (re)connect it BINDS WSManager.transmitter to the fresh private
            Transport, so ws_private.send (the live order-dispatch body) transmits on
            the current socket (Image7 dispatch seam; sec 12.3 _send_private).
  reconnect its own ReconnectDriver runs the PRIVATE restore subset
            (build_private_restore_sequence): fresh token -> reconnect socket ->
            re-subscribe executions/balances -> reset rate ceiling -> resume keepalive
            -> RESTORE_POSITION_MIRROR from snap_orders.

All I/O is injected (the socket opener, the REST GetWebSocketsToken token acquire,
the snap_orders source, the per-step sleep, the clock) so the whole connect +
ingest + reconnect path is driven with stdlib asyncio.run over fakes - no network,
no real timers. The REST bodies (GetWebSocketsToken, GetOpenOrders) and the
maxratecount rate-ceiling unit are LATER slices; here they are injected edges.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field

from .channels import PrivateChannel
from .dispatch import DispatchTable, Handler
from .position_mirror import PositionSide
from .keepalive import ConnectionKeepalive
from .rate_counter import RateCounter
from .reconcile import ReconciliationTracker
from .reconnect import (
    DisconnectReason,
    RestoreStep,
    ShardReconnectCoordinator,
    build_private_restore_sequence,
)
from .reconnect_driver import ReconnectDriver
from .receive_loop import ShardReceiveLoop, SubscriptionAck
from .transport import Transport
from ..rest.client import gap_close_fill

# The private connection is a single connection; it reuses the shard machinery with
# a fixed index so the one ShardReconnectCoordinator / ReconnectDriver drive it.
PRIVATE_SHARD_INDEX = 0

# Injected I/O edges.
OpenPrivateSocket = Callable[[], Awaitable[Transport]]   # open one private WS socket
AcquireToken = Callable[[], Awaitable[str]]              # REST GetWebSocketsToken (live)
# snap_orders source for the RESTORE_POSITION_MIRROR step (REST GetOpenOrders /
# executions snapshot). Returns the open-order snapshot to reconcile against.
FetchSnapOrders = Callable[[], Awaitable[Sequence[Mapping[str, object]]]]
# The ar:AR-056 gap-close ACTUAL-fill source (REST QueryOrders / QueryOrdersInfo, REST-QOI-002):
# given the gap-closed position's order id(s), return {txid: order-info} so gap_close_fill reads the
# real exit avg_price + fee (FEE-CALC-006 record-of-truth). None -> the degraded entry-time estimate.
QueryOrders = Callable[[Sequence[str]], Awaitable[Mapping[str, Mapping[str, object]]]]
# The REST-BAL-004 startup drawdown-baseline source (ar:AR-052): the GetAccountBalance spot/main USD
# assigned DIRECTLY as portfolio_baseline_USD, captured ONCE at startup (after the snap_orders Step 6),
# never on reconnect (HR-WM-011 / AR-056). Returns the LONG (spot) portfolio USD; None -> no baseline
# captured (the sweep skips the long until it lands). The SHORT margin-account-equity baseline source
# (the REST endpoint for the margin equity) is a follow-on - the diagram leaves it undecided.
FetchAccountBalance = Callable[[], Awaitable[object]]
BalancesHandler = Handler                                # balances frame consumer (ledger = Path B)
Clock = Callable[[], float]
Sleep = Callable[[float], Awaitable[None]]
EventSink = Callable[[object], None]


# --- private subscribe frames (container:Private_WS_v2) ------------------------

def executions_subscribe(token: str) -> dict:
    """The Kraken WS v2 executions subscribe RPC. order_status:true is MANDATORY on
    EVERY subscribe incl reconnects (rule:HR-WM-005; without it amended+restated
    exec_types are silently dropped and Layer 3 monitoring is non-functional);
    snap_orders:true delivers the open-order snapshot for the AR-056 reconcile;
    ratecounter:true returns maxratecount (A-1 / AR-030)."""
    return {
        "method": "subscribe",
        "params": {
            "channel": "executions",
            "token": token,
            "snap_orders": True,
            "order_status": True,
            "ratecounter": True,
        },
    }


def balances_subscribe(token: str) -> dict:
    """The Kraken WS v2 balances subscribe RPC (real-time settlement; AR-050 per-module
    wallet sourcing). The synthetic/real capital ledger that consumes balances is a
    LATER slice (PA-004 div #3, Path B); this connection only subscribes the stream."""
    return {"method": "subscribe", "params": {"channel": "balances", "token": token}}


# --- canonical events ---------------------------------------------------------

@dataclass(frozen=True)
class PrivateConnected:
    """PRIVATE_WS_CONNECTED [INFO] {} - the single private connection opened + the
    executions/balances subscribes were issued (live only)."""

    code: str = field(default="PRIVATE_WS_CONNECTED", init=False)


@dataclass(frozen=True)
class OrderRejected:
    """ORDER_REJECTED [WARNING] {cl_ord_id, error} - an add_order RESPONSE came back success:false
    (a Kraken Max-Price-Protection reject on a wide spread, the C-1 condition). Surfaced for audit;
    the dispatch driver's reject probe reads the registry record_order_response feeds and walks out
    the marketable IOC retry (sec 4.1 C-1)."""

    cl_ord_id: str
    error: str
    code: str = field(default="ORDER_REJECTED", init=False)


@dataclass(frozen=True)
class PositionMirrorRestored:
    """POSITION_MIRROR_RESTORED [INFO] {gap_closed} - the RESTORE_POSITION_MIRROR step
    reconciled the mirror against snap_orders; gap_closed counts positions that closed
    during the disconnect (AR-056)."""

    gap_closed: int
    code: str = field(default="POSITION_MIRROR_RESTORED", init=False)


# --- executions ingest (the inbound fill -> mirror surface) -------------------

class ExecutionsIngest:
    """Routes private executions frames onto the WSManager mirror sole-writer surface.

    A Kraken executions channel frame is {channel:"executions", type:"snapshot"|"update",
    data:[...], sequence:N}. The data elements are the per-execution events.
      snapshot - the open-order snapshot (snap_orders): reconcile the mirror through
                 WSManager.restore_position_mirror() (AR-056 / startup Step 6). The
                 snapshot is also CAPTURED so the reconnect RESTORE_POSITION_MIRROR
                 step can reuse it when no REST GetOpenOrders source is injected.
      update   - one or more fill / cancel / amend events: each is fed to
                 WSManager.record_execution(), the WS-EXE-009 exec_type dispatch
                 (the mirror sole writer; rule:HR-PM-009).
    """

    def __init__(
        self,
        ws_manager,
        *,
        on_event: EventSink | None = None,
        rate_counter: RateCounter | None = None,
    ) -> None:
        self._wm = ws_manager
        self._on_event = on_event
        self._rate_counter = rate_counter
        self.last_snap_orders: list[Mapping[str, object]] | None = None

    def __call__(self, message: dict) -> None:
        data = list(message.get("data") or [])
        # A-1: with ratecounter:true each execution carries the live per-pair rate counter;
        # feed every value (snapshot or update) to the RateCounter before the mirror routing.
        self._observe_rate_counter(data)
        if message.get("type") == "snapshot":
            self.last_snap_orders = data
            gaps = self._wm.restore_position_mirror(data)
            # ar:AR-056: each gap-closed position (its off-book L3 emergSL fired while disconnected)
            # emits its evt:TRADE_CLOSE. The SYNC ingest path uses the DEGRADED entry-time estimate
            # (it cannot await the REST QueryOrders actual fill); the async WS-REC-004 RESTORE_
            # POSITION_MIRROR step supersedes with the actual fill when a query_orders edge is wired.
            # Whichever reconciles the symbol FIRST wins - restore_from_snapshot removes it, so the
            # other path sees no gap (never a double TRADE_CLOSE). getattr-guarded for a stand-in wm.
            on_gap = getattr(self._wm, "on_reconnect_gap_close", None)
            if callable(on_gap):
                for gap in gaps:
                    on_gap(gap)
            self._emit(PositionMirrorRestored(len(gaps)))
            return
        for event in data:
            # I-6: a cancel-confirm (exec_type=canceled) for the resting off-book emergSL feeds the
            # cancel-ACK registry, so the cancel-then-sell driver's "confirmed -> proceed" branch sees
            # it (WS-EXE-009 canceled). Fed alongside the mirror dispatch (the mirror IGNORES canceled).
            self._maybe_record_cancel_ack(event)
            self._wm.record_execution(event)

    def _maybe_record_cancel_ack(self, event: Mapping[str, object]) -> None:
        """Feed WSManager.record_cancel_ack from an executions cancel-confirm frame (the I-6
        req_id registry). A canceled exec_type carrying a cl_ord_id confirms that order's cancel;
        a canceled frame with no cl_ord_id carries no correlation key and is left to the mirror."""
        if event.get("exec_type") != "canceled":
            return
        cl_ord_id = event.get("cl_ord_id")
        if cl_ord_id is not None:
            self._wm.record_cancel_ack(str(cl_ord_id))

    def _observe_rate_counter(self, data: Sequence[Mapping[str, object]]) -> None:
        """ar:AR-030 / A-1: route each ratecounter:true per-pair value to the RateCounter,
        emitting RATE_COUNTER_UPDATE (+ RATE_COUNTER_WARNING above the warning fraction).
        An execution without a ratecount field (or symbol) carries no rate-counter info."""
        if self._rate_counter is None:
            return
        for elem in data:
            rc = elem.get("ratecount")
            symbol = elem.get("symbol")
            if rc is None or symbol is None:
                continue
            for event in self._rate_counter.observe(str(symbol), int(rc)):
                self._emit(event)

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)


def _response_cl_ord_ids(message: Mapping[str, object]) -> list[str]:
    """The cl_ord_id(s) echoed on an add_order/batch_add RESPONSE frame. WS v2 carries the client
    id in ``result`` - a single dict for add_order ({cl_ord_id, order_id}) or a list for batch_add;
    falls back to a top-level cl_ord_id (some reject envelopes echo it there). Empty when none."""
    out: list[str] = []
    result = message.get("result")
    elems = result if isinstance(result, (list, tuple)) else [result]
    for elem in elems:
        if isinstance(elem, Mapping) and elem.get("cl_ord_id") is not None:
            out.append(str(elem["cl_ord_id"]))
    if not out and message.get("cl_ord_id") is not None:
        out.append(str(message["cl_ord_id"]))
    return out


class OrderAckHandler:
    """Routes Kraken add_order RESPONSE frames to the C-1 MPP-reject registry (the loop's on_order_ack).

    The order-RPC response is a method frame {method:"add_order", success:bool, result:{cl_ord_id,..},
    error:..}: success:false is the Max-Price-Protection reject the dispatch driver's reject probe
    reads, success:true the accept (the probe short-circuits on it). Records EVERY add_order/batch_add
    response through WSManager.record_order_response(cl_ord_id, rejected=not success). The cancel ACK
    is NOT sourced here - it comes from the executions cancel-confirm frame (I-6); a cancel_order
    response frame is ignored by this handler."""

    _ADD_METHODS = frozenset({"add_order", "batch_add"})

    def __init__(self, ws_manager, *, on_event: EventSink | None = None) -> None:
        self._wm = ws_manager
        self._on_event = on_event

    def __call__(self, message: dict) -> None:
        if message.get("method") not in self._ADD_METHODS:
            return
        rejected = not bool(message.get("success", True))
        for cl_ord_id in _response_cl_ord_ids(message):
            self._wm.record_order_response(cl_ord_id, rejected=rejected)
            if rejected and self._on_event is not None:
                self._on_event(OrderRejected(cl_ord_id, str(message.get("error") or "")))


def _noop_balances_handler(_frame: dict) -> None:
    """Fallback balances consumer when the wm has no balances ingest (a lightweight stand-in): the
    seq-gap tracking still happens in the receive loop; this just drops the frame."""


def _balances_handler_for(wm):
    """The default balances consumer: route each balances frame to the WSManager live wallet cache
    (WS-BAL-002/003 ingest_balances - snapshot replaces, update merges). Guarded so a wm stand-in
    built before the live wallet cache (no ingest_balances) falls back to the no-op."""
    ingest = getattr(wm, "ingest_balances", None)
    if not callable(ingest):
        return _noop_balances_handler

    def _handler(frame: dict) -> None:
        ingest(frame)

    return _handler


# --- the assembled private connection ----------------------------------------

@dataclass
class PrivateConnection:
    """The wired, runnable private connection. run() drives the receive loop until
    stop(); the ReconnectDriver re-opens + re-subscribes + restores on a drop."""

    transport: Transport
    loop: ShardReceiveLoop
    ingest: ExecutionsIngest
    keepalive: ConnectionKeepalive
    coordinator: ShardReconnectCoordinator
    driver: ReconnectDriver

    async def run(self) -> None:
        await self.loop.run()

    def stop(self) -> None:
        self.loop.stop()


class PrivateConnectionAssembler:
    """Builds + runs the single private connection (live only; PA-004 div #1).

    Construct with the live WSManager (its mirror sole-writer surface + transmitter),
    the injected private-socket opener, the token acquire (REST GetWebSocketsToken),
    and optionally a snap_orders source (REST GetOpenOrders) + balances handler.
    build() acquires a token, opens the socket, binds the transmitter, issues the
    executions/balances subscribes, and wires the receive loop + the private
    ReconnectDriver; the returned PrivateConnection is ready to run().
    """

    def __init__(
        self,
        ws_manager,
        *,
        open_socket: OpenPrivateSocket,
        acquire_token: AcquireToken,
        fetch_snap_orders: FetchSnapOrders | None = None,
        query_orders: QueryOrders | None = None,
        fetch_account_balance: FetchAccountBalance | None = None,
        balances_handler: BalancesHandler | None = None,
        coordinator: ShardReconnectCoordinator | None = None,
        on_event: EventSink | None = None,
        rate_counter: RateCounter | None = None,
        clock: Clock = time.monotonic,
        sleep: Sleep = asyncio.sleep,
        tick_interval: float = 1.0,
    ) -> None:
        if not ws_manager.is_live:
            # PA-004 divergence point #1 / rule:HR-WM-022: paper NEVER connects the
            # private WS. Building it in paper is a divergence-point violation.
            raise ValueError(
                "private WS connection is live-only (PA-004 div #1 / HR-WM-022); "
                "WSManager is in paper mode"
            )
        self._wm = ws_manager
        self._open_private_socket = open_socket
        self._acquire_token = acquire_token
        self._fetch_snap_orders = fetch_snap_orders
        self._query_orders = query_orders
        self._fetch_account_balance = fetch_account_balance
        self._balances_handler = balances_handler or _balances_handler_for(ws_manager)
        self._coordinator = coordinator or ShardReconnectCoordinator()
        self._external_on_event = on_event
        self._clock = clock
        self._sleep = sleep
        self._tick_interval = tick_interval

        # ar:AR-030: the per-pair rate-counter unit. The sink wraps the external event
        # sink so every executions ACK (initial + each reconnect resubscribe) flowing
        # through the loop sets the operative ceiling from maxratecount (never 125).
        self._rate_counter = rate_counter or RateCounter()
        self._latest_maxratecount: int | None = None
        self._on_event = self._make_ceiling_sink(on_event)

        self._ingest = ExecutionsIngest(
            ws_manager, on_event=self._on_event, rate_counter=self._rate_counter
        )
        self._keepalive = ConnectionKeepalive(clock=clock)
        self._token: str | None = None
        self._pending: Transport | None = None  # the freshest socket (build + reconnect)

        # The private connection's own driver runs the PRIVATE restore subset (token,
        # private re-subscribe, rate ceiling, RESTORE_POSITION_MIRROR) - never the
        # public-channel steps. paper_mode=False (this connection exists only in live).
        self._driver = ReconnectDriver(
            self._coordinator,
            paper_mode=False,
            open_socket=self._open_socket,
            run_step=self._run_step,
            sleep=sleep,
            on_event=self._on_event,
            restore_sequence=build_private_restore_sequence(),
        )

    def _make_ceiling_sink(self, external: EventSink | None) -> EventSink:
        """Wrap the external event sink: when an executions SUBSCRIPTION_ACK carrying
        maxratecount passes through (the initial subscribe + every reconnect resubscribe),
        set the RateCounter operative ceiling and emit MAXRATECOUNT_SET (AR-030). Every
        other event passes straight through unchanged."""

        def sink(event: object) -> None:
            ceiling_event = None
            if (
                isinstance(event, SubscriptionAck)
                and event.channel == "executions"
                and event.maxratecount is not None
            ):
                self._latest_maxratecount = int(event.maxratecount)
                ceiling_event = self._rate_counter.set_ceiling(self._latest_maxratecount)
            if external is not None:
                external(event)
                if ceiling_event is not None:
                    external(ceiling_event)

        return sink

    def _emit(self, event: object) -> None:
        self._on_event(event)

    @property
    def rate_counter(self) -> RateCounter:
        return self._rate_counter

    @property
    def driver(self) -> ReconnectDriver:
        return self._driver

    @property
    def ingest(self) -> ExecutionsIngest:
        return self._ingest

    # --- build (startup steps 5/6) -------------------------------------------
    async def build(self) -> PrivateConnection:
        """Token -> open socket + bind transmitter -> subscribe executions/balances ->
        wire the receive loop + private reconnect driver."""
        self._token = await self._acquire_token()
        transport = await self._open_socket(PRIVATE_SHARD_INDEX)
        await self._subscribe(transport, self._token)
        self._emit(PrivateConnected())
        await self._capture_startup_baseline()

        dispatch = DispatchTable()
        dispatch.register(PrivateChannel.EXECUTIONS, self._ingest)
        dispatch.register(PrivateChannel.BALANCES, self._balances_handler)

        # The sec-12.5 / AR-054 LIVE FLOW pump: after each inbound batch (and every idle tick), drain
        # BOTH the on-fill emergSL placement queue (a just-opened position's off-book L3 stop) AND the
        # live-exit driver (a detected exit's cancel-then-sell) over THIS bound private socket within
        # one tick_interval. SKIPPED mid-reconnect (the loop's own guard). The C-1 MPP-reject
        # feed routes add_order RESPONSE frames -> WSManager.record_order_response. Both are guarded
        # by getattr so a lightweight wm test stand-in (no exit surface) is left unwired (the
        # operational.py set_ciats_exit_sinks pattern).
        # Prefer the combined pump (drains the AR-054 on-fill emergSL queue AND the live-exit queue);
        # fall back to drive_live_exits for a wm stand-in built before the entry path (back-compat).
        after_batch = getattr(self._wm, "drive_after_batch", None) or getattr(
            self._wm, "drive_live_exits", None
        )
        on_order_ack = (
            OrderAckHandler(self._wm, on_event=self._on_event)
            if callable(getattr(self._wm, "record_order_response", None))
            else None
        )

        # RL-MON-003 entry-suppression gate (ar:AR-030): bind THIS private connection's RateCounter
        # critical-tier predicate as the WSManager entry-dispatch gate, so a live entry add_order is
        # SUPPRESSED while the pair is armed above rl_critical_threshold_pct (the exit/cancel budget is
        # preserved - exits are never gated). The predicate lives on the rate counter (it reflects the
        # per-pair executions-feed counter, A-1); the dispatch gate is WSManager's (the add_order
        # owner). Guarded by getattr for a wm stand-in built before the entry path (back-compat).
        bind_suppression = getattr(self._wm, "set_entry_suppression_check", None)
        if callable(bind_suppression):
            bind_suppression(self._rate_counter.is_entry_suppressed)

        loop = ShardReceiveLoop(
            transport,
            dispatch,
            self._keepalive,
            recon=ReconciliationTracker(),  # executions/balances seq-gap (A-9/A-10)
            is_reconnecting=self._coordinator.any_reconnecting,  # rule:HR-WM-012
            initiate_reconnect=self._bind_reconnect(),
            on_event=self._on_event,
            on_order_ack=on_order_ack,
            after_batch=after_batch if callable(after_batch) else None,
            clock=self._clock,
            tick_interval=self._tick_interval,
        )
        return PrivateConnection(
            transport=transport,
            loop=loop,
            ingest=self._ingest,
            keepalive=self._keepalive,
            coordinator=self._coordinator,
            driver=self._driver,
        )

    async def _capture_startup_baseline(self) -> None:
        """REST-BAL-004 / ar:AR-052: capture the LONG drawdown baseline ONCE at startup - the
        GetAccountBalance spot/main USD assigned directly as portfolio_baseline_USD (rule:HR-WM-011:
        never updated, never reset on reconnect; WSManager.set_live_portfolio_baseline enforces the
        once-only capture). Runs ONLY in build() (the initial startup), never in a reconnect restore
        step (AR-056). A no-op when no REST edge is wired (a test stand-in) or the wm lacks the baseline
        surface (back-compat). The SHORT margin-account-equity baseline source is a follow-on - the
        diagram pins the long (spot USD) but leaves the short margin-equity REST endpoint undecided."""
        if self._fetch_account_balance is None:
            return
        capture = getattr(self._wm, "set_live_portfolio_baseline", None)
        if not callable(capture):
            return
        usd = await self._fetch_account_balance()
        if usd is not None:
            capture(PositionSide.LONG, usd)   # ar:AR-052 long; the short baseline is deferred

    def _bind_reconnect(self):
        async def initiate(reason: DisconnectReason) -> Transport:
            return await self._driver.initiate(PRIVATE_SHARD_INDEX, reason)

        return initiate

    async def _subscribe(self, transport: Transport, token: str) -> None:
        """Issue the executions + balances subscribes on a socket (rule:HR-WM-005
        order_status:true). Only two RPCs - far under the AR-080 ceiling the public
        subscribe storm pacing defends - so they are sent directly."""
        await transport.send(executions_subscribe(token))
        await transport.send(balances_subscribe(token))

    # --- reconnect callbacks (the private restore subset) --------------------
    async def _open_socket(self, _shard_index: int) -> Transport:
        """RECONNECT_SOCKET (and the initial open): open the fresh private socket,
        bind the transmitter to it (so ws_private.send targets the live socket), and
        stash it for the in-restore re-subscribe."""
        transport = await self._open_private_socket()
        self._pending = transport
        self._wm.transmitter.bind(transport)
        return transport

    async def _run_step(self, _shard_index: int, step: RestoreStep) -> None:
        """Execute one private restore step (build_private_restore_sequence order)."""
        if step is RestoreStep.ACQUIRE_WS_TOKEN:
            # Fresh token every reconnect; never cached/reused (REST-WST-004 / WS-REC-004).
            self._token = await self._acquire_token()
        elif step is RestoreStep.RESUBSCRIBE_PRIVATE:
            assert self._pending is not None and self._token is not None
            await self._subscribe(self._pending, self._token)
        elif step is RestoreStep.RESET_RATE_CEILING:
            # AR-030 / WS-REC-004: the engine-side per-pair counters reset/decay across the
            # disconnect, so drop the stale per-pair values + suppression latches now. The
            # operative ceiling is re-set from the FRESH executions ACK that the RESUBSCRIBE_
            # PRIVATE step just issued - it flows back through the loop's ceiling sink and
            # re-emits MAXRATECOUNT_SET (never the hardcoded 125). The live wallet cache (WS-BAL-
            # 002/003) is dropped here too - the same stale-per-connection-state drop - and re-seeded
            # by the fresh balances SNAPSHOT the resubscribe issues (WS-REC-004).
            self._rate_counter.reset()
            reset_balances = getattr(self._wm, "reset_balances_cache", None)
            if callable(reset_balances):
                reset_balances()
        elif step is RestoreStep.RESUME_KEEPALIVE:
            self._keepalive.reset()  # 30s ping + zombie timers (HR-WM-003/004)
        elif step is RestoreStep.RESTORE_POSITION_MIRROR:
            await self._restore_position_mirror()
        # No public-channel step is reachable here (private restore subset).

    async def _restore_position_mirror(self) -> None:
        """RESTORE_POSITION_MIRROR (AR-056 / startup Step 6): reconcile the mirror
        against snap_orders. Source = the injected REST GetOpenOrders edge if wired,
        else the last executions snapshot captured by the ingest. If neither exists
        the mirror is left untouched (never reconcile against an empty set - that
        would falsely gap-close every open position). Each gap-closed position then
        emits its evt:TRADE_CLOSE through on_reconnect_gap_close, with the ACTUAL close
        fill fetched via REST QueryOrders (FEE-CALC-006) when a query_orders edge is wired."""
        if self._fetch_snap_orders is not None:
            snap = await self._fetch_snap_orders()
        else:
            snap = self._ingest.last_snap_orders
        if snap is None:
            return
        gaps = self._wm.restore_position_mirror(snap)
        for gap in gaps:
            await self._emit_gap_close(gap)
        self._emit(PositionMirrorRestored(len(gaps)))

    async def _emit_gap_close(self, gap) -> None:
        """ar:AR-056: emit the gap-close evt:TRADE_CLOSE for one PositionClosedDuringGap. Fetches the
        ACTUAL close fill via REST QueryOrders (the FEE-CALC-006 record-of-truth: the off-book emergSL
        order's real exit avg_price + fee); absent an actual fill, on_reconnect_gap_close falls to the
        entry-time emergsl_price estimate (surfaced GapCloseEstimated). getattr-guarded for a stand-in."""
        on_gap = getattr(self._wm, "on_reconnect_gap_close", None)
        if not callable(on_gap):
            return
        fill = await self._resolve_gap_fill(gap)
        if fill is None:
            on_gap(gap)
        else:
            exit_price, fee = fill
            on_gap(gap, exit_price=exit_price, fees_exit=fee)

    async def _resolve_gap_fill(self, gap):
        """REST-QOI-002: query the gap-closed position's order id(s) (the off-book emergSL leg, then
        the entry cl_ord_id) and return the first (exit_price, fee) of a closed/filled order, else None
        (the degraded entry-time estimate). No query_orders edge / no order id -> None."""
        if self._query_orders is None:
            return None
        position = gap.position
        txids = [
            t for t in (getattr(position, "emergsl_id", None), getattr(position, "cl_ord_id", None)) if t
        ]
        if not txids:
            return None
        orders = await self._query_orders(txids)
        for order in (orders or {}).values():
            fill = gap_close_fill(order)
            if fill is not None:
                return fill
        return None
