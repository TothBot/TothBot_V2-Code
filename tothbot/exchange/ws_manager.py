"""mod:WS_Manager - the sole dispatch gatekeeper shell (PA-001).

Source: 0500000 dv1_240 sec 7 Image6 (mod:WS_Manager) + sec 2 Image1 + sec 12
Image7. WS_Manager is the SINGLE point through which all Kraken traffic flows:
every inbound push frame is routed by the O(1) dispatch table (dispatch.py),
every outbound order RPC passes through the paper/live seam (seam.py), and it is
the SOLE writer to mod:Position_Mirror (rule:HR-PM-009). That write authority is
wired here: WSManager holds the PositionMirror and is the ONLY caller that tags a
write with the WRITER_ID sentinel (position_mirror.py); every other module reads
the mirror through the helper methods below, never by direct dict access.

This S2b shell assembles the three pieces and binds the run mode ONCE at
construction (rule:HR-WM-021, immutable for the process lifetime):
  - one public WS connection (always);
  - one private WS connection ONLY in live mode - private WS is NEVER
    connected in paper (rule:HR-WM-022 / PA-004 divergence #1);
  - the inbound DispatchTable and the outbound DispatchSeam.

The outbound seam I/O bodies are WIRED (outbound.py): the live transmitter
(PrivateTransmitter / ws_private.send over the single private Transport) and the
paper boundary (PaperDispatchSimulator). The live transmitter's socket is
late-bound by the private_ws assembler once the private connection opens (startup
Step 5) and re-bound on each reconnect; WSManager exposes it as self.transmitter.

The PAPER capital path is now WIRED (PA-004 div #3 / #4, paper side). In paper mode
WSManager owns the synthetic spot_usd_balance (ledger.py, sec 12.4 single-owner) and
a PaperFillSimulator (paper_fill.py) is bound into the paper boundary's fill_simulator
hook: a paper dispatch produces a synthetic fill that writes the Position Mirror via
the byte-identical record_execution surface (D-06) and debits/credits the synthetic
ledger through the sole-writer methods below. WSManager is the SOLE writer of BOTH the
mirror (rule:HR-PM-009) and the ledger (rule:HR-WM-032).

The paper EXIT lifecycle is now WIRED (sec 12.5) across ALL THREE exit layers. handle_ticker
runs the ticker-bbo adverse-price detector (paper_exit.py, ar:AR-048 L2 MAE + the synthetic
L3 emergSL touch) over each open position; on_regime_classified + on_htf_ohlc_close run the
layer:L1a regime-reversal detector (regime_exit.py, the EC-L1A-002 daily downgrade + the
EC-L1A-001 1H HTF reversal, ar:AR-062) with the rule:HR-EC-016(a) pair-status precondition.
Any fired exit applies the synthetic ledger credit and routes through mod:Exit_Controller
(execution/exit_controller.py) for the SAME close sequence - the 25-field evt:TRADE_CLOSE
record, the rule:HR-PM-009 mirror clear (close_position), the ar:AR-073 Selection-Controller
state update, the semaphore release, and the WS-TKR-003 ticker trades-mode switch. The L1a
sell is the SAME on_paper_close mechanism (a cleared mirror makes any follow-on detection a
surfaced PAPER_CLOSE_SKIPPED, never a double-close).

DEFERRED: the daily-compute ORCHESTRATOR that calls on_regime_classified / on_htf_ohlc_close
on the 00:00 UTC tick + every 1H close (the REST GetOHLCData edges, path C) - these handlers
are driven directly for now; the producer-sourced TRADE_CLOSE fields (entry/exit timestamps,
hold_candle_count, market_regime + signal_params - entry-side producers; a max-over-life MAE
tracker); the live instrument-status channel feeding the pair-status precondition (the status
is passed in for now); the G7 BoundedSemaphore acquire side (mod:Risk_Engine); and the REST
contract:Reconciliation_REST fallback.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from enum import Enum

from .connection import ConnectionRole, WSConnection
from .dispatch import Channel, DispatchTable, Handler
from .ledger import LedgerUpdate, SyntheticCapitalLedger
from .outbound import PaperDispatchSimulator, PrivateTransmitter
from .paper_exit import (
    MaeHighWaterTracker,
    PaperEmergSlTriggered,
    PaperMaeDetected,
    detect_paper_exit,
)
from .paper_fill import PaperFillSimulator
from .regime_exit import (
    L1aExitHeld,
    PairStatus,
    PaperRegimeExitDetected,
    RegimeExitNoQuote,
    RegimeExitSignal,
    detect_daily_regime_downgrade,
    detect_htf_regime_reversal,
    l1a_precondition_blocks,
)
from .position_mirror import (
    WRITER_ID,
    ExecOutcome,
    Position,
    PositionAction,
    PositionClosedDuringGap,
    PositionMirror,
    PositionSide,
)
from .seam import DispatchSeam, EventSink, LiveSender, PaperSimulator
from ..config import registry
from ..config.fees import FEE_TAKER_PCT
from ..config.settings import Mode
from ..execution.entry_dispatch import build_emergsl_order, build_entry_order
from ..execution.exit_controller import ExitController, ExitReason
from ..execution.exit_dispatch import (
    build_cancel_order,
    build_limit_only_exit_order,
    build_market_sell_order,
    build_mpp_retry_order,
    mpp_retry_limit_price,
)
from ..risk.dispatch_semaphore import DispatchSemaphore
from ..modules.trading_module import TradingModule
from ..regime.engine import RegimeClassification

# Per-symbol ticker event_trigger modes (D1 WS-TKR-002/003): a pair WITH an open position
# uses bbo (faster adverse-price detection - for a long, bid = realizable exit price); a
# pair WITHOUT one uses trades (sufficient for the Gate-2 24h-volume read). WS_Manager
# switches the mode on every position open -> bbo / close -> trades (WS-TKR-003).
_TRIGGER_BBO = "bbo"
_TRIGGER_TRADES = "trades"

MonotonicClock = Callable[[], float]
UtcClock = Callable[[], datetime]
# The injected async edges of the live cancel-then-sell driver (defaults wire the live behavior;
# tests inject deterministic stubs). CancelAckWait awaits a cancel_order ACK for a cl_ord_id up to
# the timeout window, returning True if ACKed (I-6). MarketRejectedProbe reports whether a sent
# close order was rejected by Kraken Max-Price-Protection on a wide spread (C-1).
CancelAckWait = Callable[[str, float], Awaitable[bool]]
MarketRejectedProbe = Callable[[dict], Awaitable[bool]]


@dataclass(frozen=True)
class SelectionStateUpdated:
    """SELECTION_STATE_UPDATED [INFO] (ar:AR-073 / rule:HR-EC-014) - mod:Exit_Controller is
    the SOLE updater of mod:Selection_Controller state, executed via this WSManager path on
    every close. A LOSS increments consecutive_loss_count[symbol] + sets exit_cooldown_log
    [symbol] (the monotonic close instant, SC-Gate-2 cooldown reads it per rule:HR-SC-006);
    a WIN resets consecutive_loss_count[symbol] to 0. source = mod:Exit_Controller."""

    symbol: str
    is_win: bool
    consecutive_loss_count: int
    source: str = "mod:Exit_Controller"
    code: str = field(default="SELECTION_STATE_UPDATED", init=False)


@dataclass(frozen=True)
class TickerTriggerSwitched:
    """TICKER_TRIGGER_SWITCHED [INFO] (D1 WS-TKR-003) - the per-pair ticker event_trigger was
    switched on a position-state change (opened -> bbo, closed -> trades). The WS-client
    re-subscribe is the data-layer edge (S2c); this records the mode decision."""

    symbol: str
    event_trigger: str
    code: str = field(default="TICKER_TRIGGER_SWITCHED", init=False)


# --- LIVE EXIT DETECTION -> MARKET-SELL DISPATCH (sec 3 Image3 / sec 4.1 / sec 12.5 LIVE FLOW) ---
# The sync read-loop handler DETECTS the exit (ticker bbo L2 MAE / regime L1a) and enqueues an
# INTENT; the async drive_live_exits driver owns the SEQUENCE-CRITICAL "(1) cancel emergSL THEN
# (2) market sell" with the I-6 cancel-timeout fallback + the C-1 MPP-rejection retry. The
# sync->async seam is an engineering choice (the read loop cannot await the order seam): a sync
# detector + an intent queue + an async dispatch_live_exit, mirroring dispatch_entry.


class ExitDispatchOutcome(Enum):
    """The terminal state of one dispatch_live_exit run (for the driver + tests)."""

    DISPATCHED = "dispatched"               # cancel confirmed -> close order out (reason stamped)
    SKIPPED_IN_FLIGHT = "skipped_in_flight"  # the double-dispatch guard fired (exit already in flight)
    NO_POSITION = "no_position"             # the intent's symbol had no open position (already closed)
    NO_QUOTE = "no_quote"                   # no realizable bbo to price an IOC-limit close -> deferred
    HELD_PAIR_STATUS = "held_pair_status"   # AR-040 precondition: cancel_only / maintenance -> HOLD
    HELD_AMBIGUOUS = "held_ambiguous"       # I-6 2nd cancel timeout, state unknown -> HOLD + alert
    HELD_MPP_EXHAUSTED = "held_mpp_exhausted"  # C-1: all mpp_retry_count IOC retries rejected -> HOLD


# The close-order kind a live exit dispatches: a MARKET close (the layer:L1a / L2 normal exits) or a
# single IOC-LIMIT close (the ar:AR-040 PAIR_LIMIT_ONLY_EXIT - "NOT a market order", priced at the bbo).
_CLOSE_MARKET = "market"
_CLOSE_LIMIT_ONLY = "limit_only"


@dataclass(frozen=True)
class LiveExitIntent:
    """A detected live exit awaiting async dispatch (the sync detector enqueues it). Carries the
    side (the close direction mirror), the reason/trigger (stamped onto the executions TRADE_CLOSE),
    the realizable bbo bound for the C-1 MPP retry (ar:AR-048: bid for a long, ask for a short), and
    the ar:AR-040 pair status for the L1a Step-1 precondition (ONLINE until the live instrument-
    status channel lands - a later slice)."""

    symbol: str
    side: PositionSide
    exit_reason: str
    trigger: str
    layer: str
    best_quote: object | None = None
    pair_status: PairStatus = PairStatus.ONLINE
    close_type: str = _CLOSE_MARKET   # _CLOSE_MARKET (L1a/L2) | _CLOSE_LIMIT_ONLY (AR-040 limit_only)


@dataclass(frozen=True)
class LiveExitDetected:
    """LIVE_EXIT_DETECTED [HIGH] - the sync detector enqueued a TothBot-dispatched live exit intent
    (the sec-12.5 step-3 detection event for the live flow). The async driver runs the cancel-then-
    sell next; the close itself emits evt:TRADE_CLOSE off the executions confirm fill."""

    symbol: str
    exit_reason: str
    trigger: str
    code: str = field(default="LIVE_EXIT_DETECTED", init=False)


@dataclass(frozen=True)
class LiveExitDispatched:
    """LIVE_EXIT_DISPATCHED [INFO] - the SEQUENCE-CRITICAL cancel-then-sell completed: the emergSL
    cancel confirmed and the market close (or its C-1 IOC retry) is out, with the reason stamped via
    note_live_exit_dispatch so the executions close fill carries it onto the TRADE_CLOSE."""

    symbol: str
    exit_reason: str
    code: str = field(default="LIVE_EXIT_DISPATCHED", init=False)


@dataclass(frozen=True)
class LiveExitPriorityOverride:
    """LIVE_EXIT_PRIORITY_OVERRIDE [HIGH] (rule:HR-EC-016(b)) - an L2 MAE breach arrived while an
    L1a regime-exit was in progress (a reason stamped) or queued; the L2 takes PRIORITY: the same
    cancel-then-sell completes but the reason carried onto the ONE TRADE_CLOSE is overridden to
    MAE_THRESHOLD_BREACH (the original L1a reason suppressed, never a second dispatch/record)."""

    symbol: str
    code: str = field(default="LIVE_EXIT_PRIORITY_OVERRIDE", init=False)


@dataclass(frozen=True)
class LiveExitDoubleDispatchSkipped:
    """LIVE_EXIT_DOUBLE_DISPATCH_SKIPPED [WARNING] - a dispatch was requested for a symbol that
    already has an exit in flight (a reason stamped) or HELD; skipped to avoid a double cancel/sell.
    Surfaced, never a silent drop."""

    symbol: str
    code: str = field(default="LIVE_EXIT_DOUBLE_DISPATCH_SKIPPED", init=False)


@dataclass(frozen=True)
class CancelAckTimeout:
    """CANCEL_ACK_TIMEOUT [WARNING] (I-6) - a cancel_order ACK did not arrive within param:cancel_
    timeout_window; the req_id registry logs it before the executions-channel state check (attempt 1
    -> retry once; attempt 2 -> the ambiguous-state HOLD). q5_logs: "cancel-ACK timeout events
    logged via req_id registry"."""

    symbol: str
    cl_ord_id: str
    attempt: int
    code: str = field(default="CANCEL_ACK_TIMEOUT", init=False)


@dataclass(frozen=True)
class LiveExitHeldAmbiguous:
    """LIVE_EXIT_HELD_AMBIGUOUS [CRITICAL] (I-6) - the cancel ACK timed out TWICE and the executions
    channel could not confirm the cancel, so the order state is AMBIGUOUS: the position is HELD, the
    operator alerted, and NO market sell is issued ("NEVER market sell with ambiguous order state").
    The resting emergSL may still be live - it is the only protection until the operator clears."""

    symbol: str
    cl_ord_id: str
    code: str = field(default="LIVE_EXIT_HELD_AMBIGUOUS", init=False)


@dataclass(frozen=True)
class MppRejectRetry:
    """MPP_REJECT_RETRY [WARNING] (C-1) - the market close rejected on a wide spread (Kraken Max-
    Price-Protection); retrying with a marketable IOC limit at best_bid -/+ 0.2%*attempt (the n-th
    of up to param:mpp_retry_count attempts)."""

    symbol: str
    attempt: int
    code: str = field(default="MPP_REJECT_RETRY", init=False)


@dataclass(frozen=True)
class LiveExitMppExhausted:
    """LIVE_EXIT_MPP_EXHAUSTED [CRITICAL] (C-1) - the market close and all param:mpp_retry_count IOC
    retries rejected; the position is HELD and the operator alerted (the emergSL was already
    cancelled, so the position is unprotected until the operator intervenes)."""

    symbol: str
    code: str = field(default="LIVE_EXIT_MPP_EXHAUSTED", init=False)


@dataclass(frozen=True)
class GapCloseEstimated:
    """GAP_CLOSE_ESTIMATED [WARNING] (ar:AR-056 / FEE-CALC-006) - a reconnect gap-close TRADE_CLOSE was
    emitted from the ENTRY-TIME emergsl_price estimate + a calculated taker fee because the ACTUAL
    close fill was not available (the REST QueryOrders / ownTrades backfill did not supply it). The
    authoritative record-of-truth is the actual executions fill (FEE-CALC-006); this is the degraded
    fallback, surfaced so a later actual-fill backfill can supersede it."""

    symbol: str
    estimated_exit_price: Decimal
    code: str = field(default="GAP_CLOSE_ESTIMATED", init=False)


@dataclass(frozen=True)
class LiveExitDeferredNoQuote:
    """LIVE_EXIT_DEFERRED_NO_QUOTE [WARNING] - an AR-040 limit_only active exit fired but no realizable
    bbo (best_bid for a long / best_ask for a short) was available to price the single IOC limit close,
    so the close is deferred (the position retained, re-detected on the next event). The emergSL cancel
    already ran, so the reason stamp is released. Surfaced, never silently dropped."""

    symbol: str
    exit_reason: str
    code: str = field(default="LIVE_EXIT_DEFERRED_NO_QUOTE", init=False)


# --- THE LIVE ENTRY PATH (sec 7 mod:WS_Manager add_order + on-fill batch_add / ar:AR-054 / PA-004) ---
# The async counterpart of the paper synchronous entry flow. dispatch_entry transmits the marketable-
# IOC entry over the seam and RETURNS; the fill arrives LATER on the executions channel (PA-004 div #4).
# record_execution's OPENED branch then attaches the entry-time D6 snapshot AND enqueues the AR-054
# on-fill emergSL the after_batch pump places via seam.batch_add - the SAME sync->async seam the live
# exit uses, placed in the same private _step as the fill so the position is never left unprotected.


@dataclass(frozen=True)
class PendingEmergSl:
    """A just-opened LIVE position's ar:AR-054 on-fill emergSL awaiting async placement (the
    executions OPENED handler enqueues it; drive_pending_emergsls drains it via seam.batch_add).
    Carries the ACTUAL filled qty (position.qty, AR-054 "any filled qty is a real position") + side
    (the direction mirror: a LONG places a SELL stop BELOW entry, a SHORT a BUY-to-cover reduce_only
    stop ABOVE, ar:AR-009), the D6 emergsl_price, the entry-derived '-sl' cl_ord_id, and a fresh
    now+5s deadline (the off-book L3 crash brake placed the instant the entry fills)."""

    symbol: str
    side: PositionSide
    qty: Decimal
    emergsl_price: Decimal
    cl_ord_id: str
    deadline: str


@dataclass(frozen=True)
class EmergSlPlaced:
    """EMERGSL_PLACED [INFO] (ar:AR-054) - the on-fill off-book emergSL batch_add for a just-opened
    LIVE position was transmitted over the seam (the position is now protected at layer:L3). The
    after_batch pump placed it in the SAME private _step as the opening fill (loss-prevention: a live
    position is NEVER left with no resting emergSL)."""

    symbol: str
    cl_ord_id: str
    code: str = field(default="EMERGSL_PLACED", init=False)


@dataclass(frozen=True)
class EntrySuppressed:
    """ENTRY_SUPPRESSED [HIGH] (RL-MON-003 CRITICAL tier) - a LIVE entry add_order was SUPPRESSED
    because the pair's rate_counter armed above param:rl_critical_threshold_pct of its operative
    ceiling (ar:AR-030). The entry is skipped (NO order sent) to PRESERVE the exit rate budget under
    rate pressure - exits/cancels are NEVER gated (a suppressed exit would leave a position
    unmanaged). The pair's entry placement resumes once the counter decays back below the warning
    fraction (the RateCounter hysteresis latch). Surfaced so the suppression is never a silent drop."""

    symbol: str
    code: str = field(default="ENTRY_SUPPRESSED", init=False)


# Default paper wallet seed (decision:D-05): $5,000 each for the Long + Short module
# wallets. Sourced from the registry (the two seeds are equal); the per-module assembler
# passes the wallet's own seed explicitly when it constructs each module's WSManager.
_DEFAULT_PAPER_STARTING_BALANCE: object = registry.value("paper_starting_balance_long_usd")


class WSManager:
    """The sole dispatch gatekeeper: inbound routing + outbound mode gate."""

    def __init__(
        self,
        mode: Mode,
        *,
        live_sender: LiveSender | None = None,
        paper_simulator: PaperSimulator | None = None,
        on_event: EventSink | None = None,
        paper_starting_balance: object | None = None,
        now_monotonic: MonotonicClock | None = None,
        now_utc: UtcClock | None = None,
        exit_semaphore: object | None = None,
        cancel_ack_wait: "CancelAckWait | None" = None,
        market_rejected: "MarketRejectedProbe | None" = None,
    ) -> None:
        self._mode = mode  # frozen for process lifetime (rule:HR-WM-021)
        self._on_event = on_event
        # Clocks injected (the keepalive/silent_pair pattern) so the close path is
        # deterministic under test. The SC-Gate-2 cooldown MUST be measured with a
        # MONOTONIC clock (rule:HR-SC-006); the TRADE_CLOSE ts uses UTC wall time.
        self._now_monotonic: MonotonicClock = now_monotonic or time.monotonic
        self._now_utc: UtcClock = now_utc or (lambda: datetime.now(timezone.utc))
        # The G7 capital-commitment BoundedSemaphore (ar:AR-043), POSITION-LIFETIME per the TB00758 D2
        # ruling: per-module, acquired at entry ON A FILL + released on close (max one open commitment
        # per module). G7 CHECK 4 probes dispatch_semaphore_locked(side). `exit_semaphore` stays as an
        # optional legacy injected override for the no-side release_exit_semaphore() path (back-compat).
        self._exit_semaphore = exit_semaphore
        self._dispatch_sem: dict[PositionSide, DispatchSemaphore] = {
            PositionSide.LONG: DispatchSemaphore(), PositionSide.SHORT: DispatchSemaphore(),
        }
        # mod:Selection_Controller state, updated ONLY by the Exit Controller via the AR-073
        # path (rule:HR-EC-014), PER-MODULE - each side carries its OWN consecutive-loss counter
        # + cooldown log (G5 SC-Gate-3/2 read "this side's" state; sec 7 per-module rule). Keyed
        # by side then symbol.
        self._selection_consecutive_loss: dict[PositionSide, dict[str, int]] = {
            PositionSide.LONG: {}, PositionSide.SHORT: {},
        }
        self._selection_cooldown: dict[PositionSide, dict[str, float]] = {
            PositionSide.LONG: {}, PositionSide.SHORT: {},
        }
        # Pending Order Registry (ar:AR-053 / UT-EE-010): the entry-time D6 snapshot
        # (emergsl_price + atr_14_entry + regime_at_entry) stashed per symbol at entry dispatch,
        # so record_execution can attach it to the position at the opening fill (the snapshot is
        # TothBot-internal context, not on the Kraken wire frame). Cleared once the entry resolves.
        self._pending_entries: dict[str, dict] = {}
        # LIVE entry path (ar:AR-054). _pending_emergsl: the on-fill emergSL placement queue - the
        # async opening fill (record_execution OPENED, live) enqueues an intent built from the
        # just-opened Position; the after_batch pump (drive_pending_emergsls) drains it via seam.
        # batch_add so the off-book L3 stop is placed in the same private _step as the fill (the
        # sync->async seam mirror of the live exit - the executions handler cannot await the seam).
        # _entry_suppression_check: the RL-MON-003 dispatch gate the private assembler binds to its
        # RateCounter.is_entry_suppressed (set_entry_suppression_check); None = no suppression (paper /
        # a test without a rate counter). Checked FIRST by the live entry dispatch (entry-only by
        # construction - the exit/cancel path never consults it).
        self._pending_emergsl: list[PendingEmergSl] = []
        self._entry_suppression_check: "Callable[[str], bool] | None" = None
        # LIVE exit bookkeeping (sec 12.5 LIVE FLOW). _pending_exit_reason: the exit_reason stamped
        # by a TothBot-dispatched live exit (L1a/L2 market sell) at dispatch, read when the executions
        # close fill confirms (an un-stamped close IS the off-book emergSL backstop firing - mod:Exit_
        # Controller EMERGENCY_SL_FIRED "backfilled from the executions channel", sec 7). _live_fees_
        # entry: the actual taker entry fee from the live opening fill's executions fee field (FEE-CALC-
        # 006), retained per symbol for the close net-P&L (the synthetic ledger is paper-only).
        self._pending_exit_reason: dict[str, str] = {}
        self._live_fees_entry: dict[str, Decimal] = {}
        # LIVE EXIT DETECTION -> MARKET-SELL DISPATCH (sec 3 Image3 / sec 4.1). _live_exit_intents:
        # the sync-detector -> async-driver queue (the read loop enqueues; drive_live_exits drains).
        # _live_exit_held: symbols HELD after an I-6 ambiguous-state 2nd cancel timeout or a C-1
        # MPP-exhaustion - re-dispatch is suppressed until the operator clears (clear_live_exit_held).
        # _cancel_acks: the I-6 req_id registry - cl_ord_ids the executions channel confirmed cancelled
        # (record_cancel_ack); the cancel-timeout fallback reads it for the "confirmed -> proceed" branch.
        self._live_exit_intents: list[LiveExitIntent] = []
        self._live_exit_held: set[str] = set()
        self._cancel_acks: set[str] = set()
        # The C-1 order-response registry (the MPP-reject probe feed): cl_ord_id -> rejected?, set by
        # record_order_response when an add_order RESPONSE frame arrives on the private connection
        # (rejected True on a Kraken Max-Price-Protection reject, False on an accept). The default
        # _default_market_rejected polls it; recording BOTH outcomes lets the probe short-circuit on
        # an accept (no happy-path full-window wait).
        self._order_responses: dict[str, bool] = {}
        self._exit_seq = 0
        # The two CIATS-owned exit params (existence canonical at mod:Exit_Controller D3; values home
        # TB00000 sec 8 via the registry): the cancel-ACK timeout window (I-6) + the MPP retry count (C-1).
        self._cancel_timeout_window = float(registry.value("cancel_timeout_window"))
        self._mpp_retry_count = int(registry.value("mpp_retry_count"))
        # The async I-6 / C-1 edges are INJECTED (the seam-style "injected async I/O body" pattern), so
        # the cancel-then-sell driver is deterministic under test. Defaults wire the live behavior: the
        # cancel-ACK wait polls the _cancel_acks registry up to the timeout window; the MPP-reject probe
        # reports no rejection (the live reject-frame wiring is a later slice).
        self._cancel_ack_wait: CancelAckWait = cancel_ack_wait or self._default_cancel_ack_wait
        self._market_rejected: MarketRejectedProbe = market_rejected or self._default_market_rejected
        self._cancel_ack_poll_s = 0.05
        # Per-pair ticker event_trigger mode (WS-TKR-003); a pair appears here once it has
        # carried an open position (default trades elsewhere).
        self._ticker_event_trigger: dict[str, str] = {}

        # Public connection always exists; private exists ONLY in live mode
        # (rule:HR-WM-022 - private WS never connected in paper).
        self.public = WSConnection(ConnectionRole.PUBLIC)
        self.private: WSConnection | None = (
            None if mode is Mode.PAPER else WSConnection(ConnectionRole.PRIVATE)
        )

        # Inbound O(1) routing of the 7 channels.
        self.inbound = DispatchTable()

        # The live outbound transmitter (ws_private.send) - late-bound to the private
        # Transport by the private_ws assembler once the connection opens (startup
        # Step 5) and re-bound on each reconnect. Overridable by injection (tests).
        self.transmitter = PrivateTransmitter()

        # Sole-writer mirror of all open positions (rule:HR-PM-009). WSManager is the
        # only writer; the write source diverges by mode upstream (PA-004 div #4:
        # paper = local sim fills; live = executions) but is byte-identical here.
        self.positions = PositionMirror(on_event=on_event)
        # The max-over-life MAE (MTM) tracker (rule:HR-LG-013-adjacent heat signal): per open position
        # the running-max adverse excursion over the whole hold, fed onto the TRADE_CLOSE at close so
        # the CIATS stop-width theory reads true heat (not the at-exit reading). Marked on each ticker.
        self._mae_tracker = MaeHighWaterTracker()

        # PAPER capital path (PA-004 div #3 / #4, paper side). In paper mode WSManager
        # owns the synthetic spot_usd_balance (sec 12.4 single-owner) and binds a
        # PaperFillSimulator into the paper boundary's fill_simulator hook so a paper
        # dispatch produces a synthetic fill -> record_execution (D-06) + ledger
        # debit/credit. In live mode there is no synthetic ledger (real Kraken balances
        # are authoritative) and the paper boundary is inert (the seam uses the
        # transmitter), so both stay None / a no-op boundary.
        # TWO independent per-module wallets (mod:Long_Module + mod:Short_Module, sec 7
        # parallel-module framework): each owns its OWN synthetic ledger (the Long wallet is
        # spot USD, the Short wallet is Kraken margin equity), seeded per side (D-05 $5,000
        # each; paper_starting_balance overrides both when given). The two share ONLY this WS
        # data layer (mirror + seam); a loss in one never touches the other (the per-wallet
        # isolation Gate-7 enforces). Routed by side: a LONG fill hits the Long wallet, a SHORT
        # fill the Short wallet (apply_paper_*_fill route via is_short).
        self.modules: dict[PositionSide, TradingModule] | None = None
        self.paper_fill: PaperFillSimulator | None = None
        if mode is Mode.PAPER:
            self.modules = {
                PositionSide.LONG: TradingModule(
                    PositionSide.LONG, starting_balance=paper_starting_balance, on_event=on_event
                ),
                PositionSide.SHORT: TradingModule(
                    PositionSide.SHORT, starting_balance=paper_starting_balance, on_event=on_event
                ),
            }
            self.paper_fill = PaperFillSimulator(
                record_execution=self.record_execution,
                apply_entry_fill=self.apply_paper_entry_fill,
                apply_exit_fill=self.apply_paper_exit_fill,
                on_event=on_event,
            )
            self.paper_dispatch = PaperDispatchSimulator(fill_simulator=self.paper_fill)
        else:
            self.paper_dispatch = PaperDispatchSimulator()

        # mod:Exit_Controller owns the sec-12.5 close path in BOTH modes (it is a pure close
        # engine above the dispatch seam - "no paper_trading_mode branch above seam", sec 7). PAPER:
        # WSManager detects the exit on the ticker bbo and routes it through on_paper_close (the
        # synthetic close). LIVE: the exit is executions-driven (sec 12.5 LIVE FLOW) - the close fill
        # on the executions channel runs on_live_close, byte-identical emit (rule:HR-EC-013 / PA-005).
        # ONE controller PER MODULE WALLET (sec 7 - "one per module wallet"; the runtime partition IS
        # the emitting wallet, and the TRADE_CLOSE record self-carries (25) side since dv1_253 so the
        # partition survives to disk for the per-module restore). Each side's controller
        # routes its close to that side (_exit_controller_for). Constructed with the general on_event
        # (telemetry); set_ciats_exit_sinks rebinds it to the side's CIATS learning sink so a close
        # emits THROUGH the per-module membrane.
        self.exit_controllers: dict[PositionSide, ExitController] = {
            side: ExitController(on_event=on_event, clock=self._now_utc)
            for side in (PositionSide.LONG, PositionSide.SHORT)
        }

        # Outbound order-dispatch mode gate (contract:WSManager_Dispatch_Seam).
        self.seam = DispatchSeam(
            mode,
            live_sender=live_sender or self.transmitter,
            paper_simulator=paper_simulator or self.paper_dispatch,
            on_event=on_event,
        )

    @property
    def mode(self) -> Mode:
        return self._mode

    @property
    def is_paper(self) -> bool:
        return self._mode is Mode.PAPER

    @property
    def is_live(self) -> bool:
        return self._mode is Mode.LIVE

    @property
    def has_private_connection(self) -> bool:
        """False in paper (rule:HR-WM-022); True in live."""
        return self.private is not None

    @property
    def exit_controller(self) -> ExitController:
        """Back-compat accessor - the LONG/default module's Exit Controller (both modes). The
        mirror of the ledger/spot_usd_balance default-wallet accessors; use exit_controllers[side]
        for the short module's controller (sec 7 per-module)."""
        return self.exit_controllers[PositionSide.LONG]

    def _exit_controller_for(self, side: PositionSide) -> ExitController:
        """The mod:Exit_Controller for the closing position's side (sec 7 per-module wallet). Valid
        in BOTH modes - paper routes the ticker-detected close through on_paper_close, live routes
        the executions-confirmed close through on_live_close (sec 12.5 LIVE FLOW)."""
        return self.exit_controllers[side]

    def set_ciats_exit_sinks(self, sinks: Mapping[PositionSide, EventSink]) -> None:
        """Wire each side's CIATS learning sink as that side's Exit_Controller event sink, so a
        paper close emits its evt:TRADE_CLOSE THROUGH the EMITTING module's ciats_sink (sec 7):
        the conductor's learning close + the HR-CI-003 inter-trade-boundary inbox poll, plus
        mod:Logger Stream-1/Stream-2 with the module tag - all in the running organism, no manual
        sink call. The operational assembly calls this after it builds the per-side sinks (the
        construction-order tie-in: the wm is injected, the conductors are built inside assemble_
        operational). Wired in BOTH modes (the live close emits its TRADE_CLOSE through the same
        per-module CIATS membrane, sec 12.5 LIVE FLOW); a no-op for any side without a sink.
        Idempotent - a later call rebinds (e.g. a re-assembly)."""
        for side, controller in self.exit_controllers.items():
            sink = sinks.get(side)
            if sink is not None:
                controller.set_event_sink(sink)

    # --- inbound (read side) -------------------------------------------------
    def register_handler(self, channel: Channel, handler: Handler) -> None:
        """Bind the sole consumer for a channel's push frames."""
        self.inbound.register(channel, handler)

    def route_frame(self, name: str, frame: dict, interval: int | None = None) -> None:
        """Entry point for the WS read loop: resolve + dispatch one frame."""
        self.inbound.route(name, frame, interval)

    # --- Position Mirror: WSManager is the SOLE writer (rule:HR-PM-009) -------
    # These two methods are the ONLY callers that tag a mirror write with WRITER_ID,
    # so the sole-writer contract holds by construction: any other module that
    # reached for the mirror would have to forge the WS_Manager identity (and be
    # caught by the position_mirror sole-writer guard).
    def record_execution(
        self,
        event: Mapping[str, object],
        *,
        regime_at_entry: str | None = None,
        emergsl_id: str | None = None,
        atr_14_entry: object | None = None,
        emergsl_price: object | None = None,
    ) -> ExecOutcome:
        """Apply one executions-channel frame to the mirror (WS-EXE-009 dispatch).
        The write source diverged by mode upstream (PA-004 div #4); the frame reaches
        here byte-identical in both paper and live. atr_14_entry + emergsl_price are the
        D6 entry-time snapshot the sole writer attaches when OPENING (dv1_242), read later
        by the Exit Controller for L2 MAE / L3 emergSL detection. When the snapshot is not
        passed explicitly (the paper simulator records the bare Kraken frame), it is taken from
        the Pending Order Registry stashed at entry dispatch (AR-053) so the opening fill still
        carries it."""
        symbol = _opt_str(event.get("symbol"))
        pending = self._pending_entries.get(symbol) if symbol is not None else None
        signal_params = market_regime = entry_timestamp_utc = None
        if pending is not None:
            if regime_at_entry is None:
                regime_at_entry = pending.get("regime_at_entry")
            if atr_14_entry is None:
                atr_14_entry = pending.get("atr_14_entry")
            if emergsl_price is None:
                emergsl_price = pending.get("emergsl_price")
            signal_params = pending.get("signal_params")
            market_regime = pending.get("market_regime")
            entry_timestamp_utc = pending.get("entry_timestamp_utc")
        outcome = self.positions.apply_execution(
            event,
            writer=WRITER_ID,
            regime_at_entry=regime_at_entry,
            emergsl_id=emergsl_id,
            atr_14_entry=atr_14_entry,
            emergsl_price=emergsl_price,
            signal_params=signal_params,
            market_regime=market_regime,
            entry_timestamp_utc=entry_timestamp_utc,
        )
        # WS-TKR-003: a pair with an open position uses ticker event_trigger:bbo (faster
        # adverse-price detection); switch it on the opening fill (the close switches it
        # back to trades in the sec-12.5 step 10 / the DISPATCH-path executions close).
        if outcome.action is PositionAction.OPENED and outcome.position is not None:
            self._update_ticker_event_trigger(outcome.position.symbol, has_position=True)
            # ar:AR-043 (D2 position-lifetime): the capital commitment opened -> ACQUIRE this module's
            # dispatch semaphore (held until close). G7 CHECK 4 SKIPs the module's next candidate while
            # held. acquire() is False only if already held (a gate-bypass defect, the position still
            # opened) - the defensive no-double-commit marker; the close releases once.
            self._dispatch_sem[outcome.position.side].acquire()
            # LIVE: retain the opening fill's actual taker entry fee (executions fee field, FEE-CALC-
            # 006) for the close net-P&L - there is no synthetic ledger in live to hold it - and
            # enqueue the ar:AR-054 ON-FILL emergSL the after_batch pump places this same private
            # _step (the position is never left unprotected). The entry-time D6 snapshot the opening
            # fill just attached has resolved, so drop the Pending Order Registry entry (the paper
            # path pops inline in dispatch_entry's finally; the live fill pops here).
            if self.is_live:
                self._live_fees_entry[outcome.position.symbol] = _frame_fee(event)
                self._enqueue_on_fill_emergsl(outcome.position)
                self._pending_entries.pop(outcome.position.symbol, None)
        elif outcome.action is PositionAction.CLOSED:
            closed_symbol = _opt_str(event.get("symbol"))
            if closed_symbol is not None:
                self._update_ticker_event_trigger(closed_symbol, has_position=False)
            # sec 12.5 LIVE FLOW: the executions-confirmed close emits evt:TRADE_CLOSE through the
            # side's Exit Controller (steps 6/8/9), byte-identical to the paper close (PA-005). The
            # mirror was already cleared by the opposite-side fill, so on_live_close skips step 7.
            if self.is_live and outcome.closed_position is not None:
                self._drive_live_close(outcome.closed_position, outcome.exit_fill_price, event)
        return outcome

    def note_live_exit_dispatch(self, symbol: str, exit_reason: str) -> None:
        """Stamp the exit_reason for a TothBot-dispatched LIVE exit (the L1a/L2 market-sell close)
        so the executions close fill carries it through to evt:TRADE_CLOSE. Called by the live exit
        DISPATCH path when it sends the market sell; the executions confirm pops it. A close with no
        stamp is the off-book emergSL backstop firing (EMERGENCY_SL_FIRED), per mod:Exit_Controller
        ('in live it is backfilled from the executions channel', sec 7)."""
        self._pending_exit_reason[symbol] = exit_reason

    def _drive_live_close(
        self, closed_position: Position, exit_fill_price: "Decimal | None", event: Mapping[str, object]
    ) -> None:
        """Run the sec-12.5 LIVE close for an executions-confirmed close fill: resolve the exit_reason
        (the TothBot-dispatched stamp, else EMERGENCY_SL_FIRED), read the close fill avg_price + the
        actual executions exit fee (FEE-CALC-006), route through the side's Exit Controller, then drop
        the symbol's retained live entry fee."""
        symbol = closed_position.symbol
        reason_str = self._pending_exit_reason.pop(symbol, None) or ExitReason.EMERGENCY_SL_FIRED.value
        exit_price = exit_fill_price if exit_fill_price is not None else closed_position.avg_entry_price
        self._exit_controller_for(closed_position.side).on_live_close(
            closed_position, exit_price, ExitReason(reason_str), _frame_fee(event), self
        )
        # Drop the per-symbol live state the executions close leaves behind (the live mirror clear
        # ran in apply_execution, not via close_position, so do it here): the retained entry fee and
        # the max-over-life MAE high-water - a reopened symbol starts fresh. The Exit Controller
        # already read the heat for the TRADE_CLOSE before this clear.
        self._live_fees_entry.pop(symbol, None)
        self._mae_tracker.clear(symbol)
        # Drop any live-exit dispatch state for the now-closed symbol: a queued intent that the close
        # (this dispatch, or the off-book emergSL firing first) has overtaken must not later re-dispatch
        # onto an empty symbol; the held marker + cancel-ACK registry reset for a clean reopen.
        self._live_exit_intents = [i for i in self._live_exit_intents if i.symbol != symbol]
        self._live_exit_held.discard(symbol)
        self._cancel_acks.discard(_emergsl_cl_ord_id(closed_position))

    # --- the LIVE EXIT DETECTION -> MARKET-SELL DISPATCH (sec 3 Image3 / sec 4.1 / sec 12.5) -------
    # The sync read-loop handlers DETECT (handle_ticker L2 MAE / on_*_close L1a) and enqueue an
    # INTENT; drive_live_exits (async) drains the queue and runs dispatch_live_exit per intent. The
    # sync->async seam: a sync detector cannot await the order seam, so it hands off through the queue.
    def record_cancel_ack(self, cl_ord_id: str) -> None:
        """I-6 req_id registry: the executions handler records a confirmed cancel ACK for cl_ord_id
        here when a cancel confirmation frame arrives on the executions channel. The cancel-timeout
        fallback reads it (the default cancel-ACK wait + the "confirmed -> proceed" state check). The
        live wire detail (the exact exec_type that confirms a cancel) is a later slice; this is the
        surface it feeds."""
        self._cancel_acks.add(cl_ord_id)

    def record_order_response(self, cl_ord_id: str, *, rejected: bool) -> None:
        """C-1 registry: the order-ack handler records each add_order RESPONSE here when it arrives
        on the private connection - rejected=True on a Kraken Max-Price-Protection reject (the wire
        success:false), False on an accept. The default MPP-reject probe (_default_market_rejected)
        reads it to resolve the cancel-then-sell C-1 branch (retry on reject, proceed on accept);
        recording the accept too lets the probe short-circuit without a full-window wait. The live
        wire detail (the exact reject error class) is the order-ack handler's concern."""
        self._order_responses[cl_ord_id] = rejected

    def clear_live_exit_held(self, symbol: str) -> None:
        """Operator surface: clear a symbol HELD after an I-6 ambiguous-state cancel timeout or a C-1
        MPP exhaustion, so a fresh detection can re-dispatch (the diagram's 'alert operator' terminal
        state is resolved by hand). A no-op for a symbol that is not held."""
        self._live_exit_held.discard(symbol)

    @property
    def live_exit_intents_pending(self) -> int:
        """The number of detected live-exit intents awaiting async dispatch (read helper for the read
        loop + tests)."""
        return len(self._live_exit_intents)

    def _enqueue_live_exit(self, intent: LiveExitIntent) -> None:
        """Sync-side: queue a detected live exit for the async driver. The double-dispatch guard skips
        a symbol that already has an exit in flight (a reason stamped), is HELD, or is already queued -
        one exit per open position (no double cancel/sell).

        rule:HR-EC-016(b) PRIORITY: an L2 MAE breach takes priority over an in-progress OR queued L1a
        regime exit. Rather than a second dispatch, it OVERRIDES the in-flight stamped reason / the
        queued intent's reason to MAE_THRESHOLD_BREACH so the SAME cancel-then-sell completes and the
        ONE close fill carries the L2 reason (the original L1a reason suppressed - never two records)."""
        symbol = intent.symbol
        is_mae = intent.exit_reason == ExitReason.MAE_THRESHOLD_BREACH.value
        # In flight: an exit reason is stamped (the L1a cancel sequence is underway, sec HR-EC-016(b)
        # timing window: cancel submitted, market sell not yet emitted). L2 overrides the stamp.
        if symbol in self._pending_exit_reason:
            if is_mae and self._pending_exit_reason[symbol] != ExitReason.MAE_THRESHOLD_BREACH.value:
                self._pending_exit_reason[symbol] = ExitReason.MAE_THRESHOLD_BREACH.value
                self._emit_event(LiveExitPriorityOverride(symbol))
            return
        if symbol in self._live_exit_held:
            return
        # Queued but not yet dispatched: an L2 breach upgrades the queued L1a intent's reason in place
        # (still ONE intent -> one dispatch); any other repeat detection is the plain double guard.
        queued = next((i for i in self._live_exit_intents if i.symbol == symbol), None)
        if queued is not None:
            if is_mae and queued.exit_reason != ExitReason.MAE_THRESHOLD_BREACH.value:
                self._live_exit_intents[self._live_exit_intents.index(queued)] = intent
                self._emit_event(LiveExitPriorityOverride(symbol))
            return
        self._live_exit_intents.append(intent)
        self._emit_event(LiveExitDetected(symbol, intent.exit_reason, intent.trigger))

    def _detect_and_enqueue_live_exit(
        self, position: Position, bid: object | None, ask: object | None
    ) -> None:
        """LIVE ticker-bbo exit detection (sec 12.5 step 1, live). Reuses the same ar:AR-048 L2 MAE
        detector as paper (detect_paper_exit), but ONLY the layer:L2_MAE_Threshold breach is a
        TothBot-dispatched live exit: the layer:L3 off-book emergSL is Kraken-side (it fires
        autonomously on the matching engine; TothBot never dispatches it). A fired L2 enqueues an
        intent (best_quote = the realizable bbo: bid for a long, ask for a short, for the C-1 retry)."""
        signal = detect_paper_exit(position, bid, ask)
        if signal is None or signal.layer != "L2_MAE":
            return
        quote = bid if position.side is PositionSide.LONG else ask
        self._enqueue_live_exit(
            LiveExitIntent(
                position.symbol, position.side, signal.exit_reason,
                trigger="L2_MAE", layer="L2_MAE", best_quote=quote,
            )
        )

    def _enqueue_live_regime_exit(
        self,
        position: Position,
        signal: RegimeExitSignal,
        bid: object | None,
        ask: object | None,
        pair_status: PairStatus,
    ) -> None:
        """LIVE layer:L1a regime-exit detection (the EC-L1A-001/002 mirror of the paper drive). Carries
        the ar:AR-040 pair_status into the intent for the Step-1 precondition (checked at dispatch),
        and the realizable bbo for the C-1 MPP retry."""
        quote = bid if position.side is PositionSide.LONG else ask
        self._enqueue_live_exit(
            LiveExitIntent(
                position.symbol, position.side, signal.exit_reason,
                trigger=signal.trigger, layer=signal.layer,
                best_quote=quote, pair_status=pair_status,
            )
        )

    # --- the ar:AR-054 ON-FILL emergSL (the live entry's sync->async seam, mirror of the exit) -----
    def _enqueue_on_fill_emergsl(self, position: Position) -> None:
        """LIVE (ar:AR-054): on an opening fill, queue the off-book emergSL placement for the after_
        batch pump (drive_pending_emergsls) - the SYNC->ASYNC seam mirror of the live exit (the
        executions handler cannot await the order seam). Built from the just-opened Position's ACTUAL
        filled qty + side + the D6 emergsl_price the opening fill attached; the cl_ord_id is the
        entry's + '-sl' (the same id the cancel path resolves via _emergsl_cl_ord_id), the deadline a
        fresh now+5s. A position with NO emergsl_price (a restore/degraded open, no D6 snapshot) is
        skipped - its resting emergSL was already placed on Kraken at the original open; the gap-close
        path owns that case (never an unprotected double-placement)."""
        if position.emergsl_price is None:
            return
        self._pending_emergsl.append(
            PendingEmergSl(
                symbol=position.symbol,
                side=position.side,
                qty=position.qty,
                emergsl_price=position.emergsl_price,
                cl_ord_id=_emergsl_cl_ord_id(position),
                deadline=self._dispatch_deadline(),
            )
        )

    async def drive_pending_emergsls(self) -> list[str]:
        """Drain the ar:AR-054 on-fill emergSL queue: for each just-opened LIVE position place its
        off-book layer:L3 emergSL via the shared seam batch_add (LONG sell-stop BELOW entry / SHORT
        buy-to-cover reduce_only stop ABOVE, ar:AR-009). Run by the after_batch pump in the SAME
        private _step as the opening fill so the position is never left unprotected. Returns the
        cl_ord_ids placed (for the driver/tests)."""
        placed: list[str] = []
        while self._pending_emergsl:
            intent = self._pending_emergsl.pop(0)
            await self.seam.batch_add(
                build_emergsl_order(
                    intent.symbol, intent.side,
                    order_qty=intent.qty, emergsl_price=intent.emergsl_price,
                    cl_ord_id=intent.cl_ord_id, deadline=intent.deadline,
                )
            )
            self._emit_event(EmergSlPlaced(intent.symbol, intent.cl_ord_id))
            placed.append(intent.cl_ord_id)
        return placed

    async def drive_after_batch(self) -> None:
        """The live private-loop pump the receive loop runs after each inbound batch (and every idle
        tick): drain BOTH the ar:AR-054 on-fill emergSL placement queue AND the sec-12.5 live-exit
        intent queue. emergSLs FIRST so a just-opened position is protected before any exit work this
        tick (loss-prevention: NEVER an open live position with no resting emergSL); then the cancel-
        then-sell exits. SKIPPED mid-reconnect by the receive loop's own guard."""
        await self.drive_pending_emergsls()
        await self.drive_live_exits()

    async def drive_live_exits(self) -> list[ExitDispatchOutcome]:
        """Drain the live-exit intent queue, dispatching each through dispatch_live_exit. The async
        seam driver the live read loop pumps after a batch of inbound frames: the sync detector
        enqueues, this owns the cancel-then-sell sequence. Returns the per-intent outcomes."""
        outcomes: list[ExitDispatchOutcome] = []
        while self._live_exit_intents:
            intent = self._live_exit_intents.pop(0)
            outcomes.append(await self.dispatch_live_exit(intent))
        return outcomes

    async def dispatch_live_exit(self, intent: LiveExitIntent) -> ExitDispatchOutcome:
        """The async live exit driver (sec 4.1 SEQUENCE CRITICAL): the AR-040 Step-1 precondition ->
        the double-dispatch guard -> stamp the reason -> (1) cancel the off-book emergSL with the I-6
        cancel-timeout fallback -> (2) the market close with the C-1 MPP-rejection retry -> the reason
        already stamped carries onto the executions-confirmed TRADE_CLOSE. Mirrors dispatch_entry; the
        actual close fill arrives later on the executions channel (record_execution -> _drive_live_
        close). LIVE only (paper routes the ticker-detected close through on_paper_close)."""
        symbol = intent.symbol
        # Step-1 pair-status precondition FIRST (ar:AR-040 / rule:HR-EC-016(a)): cancel_only /
        # maintenance -> HOLD + CRITICAL alert + NO order (the resting emergSL is the only protection).
        # Canonically the L1a Step-1 gate; applied to every dispatch (you never cancel/sell in a pair-
        # disruption state). For ticker-L2 the pair_status defaults ONLINE until the live instrument-
        # status channel lands, so this is a no-op there for now.
        if l1a_precondition_blocks(intent.pair_status):
            self._emit_event(
                L1aExitHeld(symbol, intent.pair_status.value, intent.exit_reason, intent.trigger)
            )
            return ExitDispatchOutcome.HELD_PAIR_STATUS
        # Double-dispatch guard: a reason already stamped = an exit in flight; a held symbol awaits the
        # operator. Skip the re-dispatch (no double cancel/sell).
        if symbol in self._pending_exit_reason or symbol in self._live_exit_held:
            self._emit_event(LiveExitDoubleDispatchSkipped(symbol))
            return ExitDispatchOutcome.SKIPPED_IN_FLIGHT
        position = self.positions.get(symbol)
        if position is None:
            # The symbol closed (the emergSL fired first, or a manual close) between detection and
            # dispatch - nothing to close. Surfaced, never a silent drop.
            self._emit_event(LiveExitDoubleDispatchSkipped(symbol))
            return ExitDispatchOutcome.NO_POSITION
        # Stamp the reason NOW: it is BOTH the in-flight marker (the guard above) AND the reason the
        # executions close fill carries onto the 25-field TRADE_CLOSE (note_live_exit_dispatch).
        self.note_live_exit_dispatch(symbol, intent.exit_reason)
        side = position.side
        emergsl_cl_ord_id = _emergsl_cl_ord_id(position)
        deadline = self._dispatch_deadline()
        # (1) SEQUENCE CRITICAL: cancel the resting off-book emergSL and CONFIRM it (I-6) before any
        # sell - a close fill must never race a still-resting stop into a double exit.
        confirmed = await self._cancel_emergsl_with_i6(symbol, emergsl_cl_ord_id, deadline)
        if not confirmed:
            # I-6 2nd timeout, state unknown -> ambiguous order state: HOLD + alert operator, issue NO
            # market sell. Release the in-flight stamp (no exit went out, so a later emergSL fire is the
            # EMERGENCY_SL_FIRED backstop, not this reason) and mark the symbol HELD (operator clears).
            self._pending_exit_reason.pop(symbol, None)
            self._live_exit_held.add(symbol)
            self._emit_event(LiveExitHeldAmbiguous(symbol, emergsl_cl_ord_id))
            return ExitDispatchOutcome.HELD_AMBIGUOUS
        # (2) the close order. L1a/L2 = a market close with the C-1 MPP-rejection retry; AR-040
        # limit_only = a SINGLE IOC limit close at the bbo ("NOT a market order"), no retry.
        if intent.close_type == _CLOSE_LIMIT_ONLY:
            if intent.best_quote is None:
                # no bbo to price the single IOC limit -> defer (retain the position, re-detect later).
                # The emergSL was already cancelled, so release the stamp; the position is unprotected
                # until the re-detection re-closes, but limit_only forbids a market backstop anyway.
                self._pending_exit_reason.pop(symbol, None)
                self._emit_event(LiveExitDeferredNoQuote(symbol, intent.exit_reason))
                return ExitDispatchOutcome.NO_QUOTE
            await self.seam.dispatch_market_sell(
                build_limit_only_exit_order(
                    symbol, side, order_qty=position.qty,
                    limit_price=Decimal(str(intent.best_quote)),
                    cl_ord_id=self._exit_cl_ord_id(symbol, 0), deadline=deadline,
                )
            )
            sold = True   # the single IOC limit is out; the executions channel confirms the fill
        else:
            sold = await self._dispatch_market_sell_with_mpp_retry(
                symbol, side, position.qty, intent.best_quote, deadline
            )
        if not sold:
            # C-1: the market close + all mpp_retry_count IOC retries rejected. The emergSL is already
            # cancelled, so the position is unprotected -> HOLD + alert operator. Release the stamp.
            self._pending_exit_reason.pop(symbol, None)
            self._live_exit_held.add(symbol)
            self._emit_event(LiveExitMppExhausted(symbol))
            return ExitDispatchOutcome.HELD_MPP_EXHAUSTED
        # The reason the close fill will carry: the stamp may have been overridden to MAE mid-flight by
        # an HR-EC-016(b) L2-priority breach during the cancel await; report the FINAL reason.
        final_reason = self._pending_exit_reason.get(symbol, intent.exit_reason)
        self._emit_event(LiveExitDispatched(symbol, final_reason))
        return ExitDispatchOutcome.DISPATCHED

    async def _cancel_emergsl_with_i6(
        self, symbol: str, cl_ord_id: str, deadline: str
    ) -> bool:
        """Step (1) of the cancel-then-sell with the I-6 cancel-timeout fallback (sec 4.1): send the
        cancel, await the ACK up to param:cancel_timeout_window; on timeout check the executions
        channel state (confirmed -> proceed; unknown -> retry ONCE); on a 2nd timeout still unknown
        return False (the caller HOLDs - NEVER a market sell with ambiguous order state). True = the
        emergSL cancel is confirmed and the market sell may proceed."""
        timeout = self._cancel_timeout_window
        for attempt in (1, 2):
            await self.seam.cancel_order(
                build_cancel_order(symbol, cl_ord_id=cl_ord_id, deadline=deadline)
            )
            if await self._cancel_ack_wait(cl_ord_id, timeout):
                return True
            self._emit_event(CancelAckTimeout(symbol, cl_ord_id, attempt))
            # timeout -> check the executions channel: confirmed-cancelled proceeds even without the ACK.
            if cl_ord_id in self._cancel_acks:
                return True
            # attempt 1: state unknown -> retry the cancel once (loop). attempt 2: fall through to HOLD.
        return False

    async def _dispatch_market_sell_with_mpp_retry(
        self,
        symbol: str,
        side: PositionSide,
        qty: object,
        best_quote: object | None,
        deadline: str,
    ) -> bool:
        """Step (2): the market close, with the C-1 MPP-rejection retry. Sends the whole-position
        market order; if Kraken rejects it (MPP on a wide spread) retries up to param:mpp_retry_count
        marketable IOC limits walked out by 0.2%*attempt (best_bid - for a long sell, best_ask + for a
        short buy-to-cover). True = a close order was accepted (the executions fill confirms the close);
        False = every attempt rejected (the caller HOLDs + alerts)."""
        market = build_market_sell_order(
            symbol, side, order_qty=qty, cl_ord_id=self._exit_cl_ord_id(symbol, 0), deadline=deadline
        )
        await self.seam.dispatch_market_sell(market)
        if not await self._market_rejected(market):
            return True
        # C-1 MPP rejection. The IOC retry needs the realizable bbo to price the marketable limit; with
        # no quote it cannot retry safely.
        if best_quote is None:
            return False
        for attempt in range(1, self._mpp_retry_count + 1):
            self._emit_event(MppRejectRetry(symbol, attempt))
            limit_price = mpp_retry_limit_price(side, best_quote, attempt)
            retry = build_mpp_retry_order(
                symbol, side, order_qty=qty, limit_price=limit_price,
                cl_ord_id=self._exit_cl_ord_id(symbol, attempt), deadline=deadline,
            )
            await self.seam.dispatch_market_sell(retry)
            if not await self._market_rejected(retry):
                return True
        return False

    def _exit_cl_ord_id(self, symbol: str, attempt: int) -> str:
        """A unique client order id for an exit order (the market close attempt 0, then the mpp{n}
        IOC retries). Monotonic per WSManager so a retry never collides with the original."""
        self._exit_seq += 1
        tag = "exit" if attempt == 0 else f"mpp{attempt}"
        return f"{symbol}-{tag}-{self._exit_seq}"

    def _dispatch_deadline(self) -> str:
        """The A-2 deadline:now+5s for an outbound exit order (the same 5s window as the entry)."""
        return (self._now_utc() + timedelta(seconds=5)).isoformat()

    async def _default_cancel_ack_wait(self, cl_ord_id: str, timeout: float) -> bool:
        """Default I-6 cancel-ACK wait (live): poll the _cancel_acks registry up to the timeout window
        (the monotonic clock is injected, so the window is honored deterministically). Tests inject a
        stub for fully deterministic timing. True = the ACK arrived within the window."""
        deadline = self._now_monotonic() + timeout
        while cl_ord_id not in self._cancel_acks:
            if self._now_monotonic() >= deadline:
                return cl_ord_id in self._cancel_acks
            await asyncio.sleep(self._cancel_ack_poll_s)
        return True

    async def _default_market_rejected(self, message: dict) -> bool:
        """Default C-1 MPP-reject probe (live): poll the order-response registry for THIS order's
        cl_ord_id up to param:cancel_timeout_window. The on_order_ack handler records each add_order
        RESPONSE (record_order_response: rejected on a Kraken Max-Price-Protection reject, accepted
        otherwise), so the probe resolves as soon as EITHER arrives - an accept short-circuits it with
        no happy-path full-window wait. Absent any response within the window, treat as NOT rejected
        (the executions fill is the close confirmation). Tests inject a stub for deterministic timing."""
        cl_ord_id = _order_cl_ord_id(message)
        if cl_ord_id is None:
            return False
        deadline = self._now_monotonic() + self._cancel_timeout_window
        while cl_ord_id not in self._order_responses:
            if self._now_monotonic() >= deadline:
                break
            await asyncio.sleep(self._cancel_ack_poll_s)
        return self._order_responses.pop(cl_ord_id, False)

    def restore_position_mirror(
        self, snap_orders: Sequence[Mapping[str, object]]
    ) -> list[PositionClosedDuringGap]:
        """Reconcile the mirror against the executions snapshot on reconnect/startup
        (AR-056 RESTORE_POSITION_MIRROR / Step 6); returns the gap-closed positions. The caller
        emits the Trade Outcome Bus record for each via on_reconnect_gap_close (the gap-close fill
        comes from the REST QueryOrders / executions backfill, a separate edge)."""
        return self.positions.restore_from_snapshot(snap_orders, writer=WRITER_ID)

    def on_reconnect_gap_close(
        self,
        gap: PositionClosedDuringGap,
        *,
        exit_price: object | None = None,
        fees_exit: object | None = None,
    ) -> TradeClose:
        """ar:AR-056: emit the evt:TRADE_CLOSE for a position that closed during a disconnect gap -
        its off-book layer:L3 emergSL fired while TothBot was offline (the PositionClosedDuringGap
        restore_from_snapshot returned). exit_reason = EMERGENCY_SL_FIRED (the layer:L3 q5_logs crash-
        recovery reason "backfilled from the executions channel on TothBot recovery").

        EXIT-PRICE / FEES SOURCE (FEE-CALC-006, the record-of-truth): the ACTUAL close fill - the REST
        QueryOrders / ownTrades (or executions backfill) avg_price + fee - passed in as exit_price +
        fees_exit. When the actual fill is unavailable, the DEGRADED fallback estimates the close from
        the entry-time emergsl_price (the stop trigger) + a calculated taker fee (FEE-CALC-001), and
        surfaces GAP_CLOSE_ESTIMATED so a later actual backfill can supersede it.

        Routed through the side's mod:Exit_Controller on_live_close with clear_mirror=False -
        restore_from_snapshot already removed the symbol (no double-close); steps 6/8/9 (emit, the
        AR-073 Selection-Controller update, the G7 semaphore release) run byte-identical to every
        close. Then the per-symbol live state is dropped (the retained entry fee + the MAE tracker +
        any stale live-exit intent), like _drive_live_close."""
        position = gap.position
        symbol = position.symbol
        if exit_price is not None:
            px = _dec(exit_price)
            fee = _dec(fees_exit) if fees_exit is not None else Decimal("0")
        else:
            # DEGRADED fallback: no actual fill -> the entry-time emergsl_price estimate + a calculated
            # taker fee (FEE-CALC-001 fee_exit = qty * exit_price * FEE_TAKER_PCT). Surfaced as estimated.
            px = _dec(position.emergsl_price) if position.emergsl_price is not None else position.avg_entry_price
            fee = position.qty * px * Decimal(str(FEE_TAKER_PCT))
            self._emit_event(GapCloseEstimated(symbol, px))
        record = self._exit_controller_for(position.side).on_live_close(
            position, px, ExitReason.EMERGENCY_SL_FIRED, fee, self
        )
        self._live_fees_entry.pop(symbol, None)
        self._mae_tracker.clear(symbol)
        self._live_exit_intents = [i for i in self._live_exit_intents if i.symbol != symbol]
        self._live_exit_held.discard(symbol)
        return record

    # --- Synthetic Capital Ledger: WSManager is the SOLE writer (rule:HR-WM-032) --
    # The mirror image of the Position-Mirror sole-writer pattern: these are the ONLY
    # callers that tag a ledger write with WRITER_ID (sec 12.4 single-owner). Paper
    # mode only - in live the wallets are None (real Kraken balances are authoritative).
    # The two per-module wallets (self.modules) are routed by side; a fill's is_short (the
    # order side) selects the wallet, so a LONG fill never touches the Short wallet.
    def _ledger(self, is_short: bool) -> SyntheticCapitalLedger:
        """The per-module synthetic ledger for the fill's side (HR-WM-032 sole-owner write
        surface). Raises in live mode (no synthetic wallets - real Kraken balances rule)."""
        if self.modules is None:
            raise RuntimeError(
                "no synthetic wallet (live mode - real Kraken balances are authoritative; "
                "HR-WM-032 paper-only)"
            )
        return self.modules[PositionSide.SHORT if is_short else PositionSide.LONG].ledger

    @property
    def ledger(self) -> SyntheticCapitalLedger | None:
        """The LONG module's synthetic ledger (paper), or None in live. Back-compat accessor -
        the default/long wallet; use _ledger(is_short) or modules[side] for the short wallet."""
        return None if self.modules is None else self.modules[PositionSide.LONG].ledger

    def apply_paper_entry_fill(
        self, symbol: str, qty: object, entry_fill_price: object, *, is_short: bool = False
    ) -> LedgerUpdate:
        """Apply the synthetic entry-fill cash flow (sec 12.4) to the SIDE's wallet,
        direction-aware: a LONG (default) buy-to-open DEBITS the Long wallet; a SHORT
        sell-to-open CREDITS the Short wallet net of the taker + margin OPEN fee (ar:AR-009).
        Paper mode only."""
        return self._ledger(is_short).entry_fill_debit(
            symbol, qty, entry_fill_price, writer=WRITER_ID, is_short=is_short
        )

    def apply_paper_exit_fill(
        self,
        symbol: str,
        qty: object,
        exit_price: object,
        *,
        is_short: bool = False,
        exit_reason: str | None = None,
        retain_fees_entry: bool = False,
    ) -> LedgerUpdate:
        """Credit the synthetic spot_usd_balance for a simulated exit fill (sec 12.4
        EXIT-FILL CREDIT, FEE_TAKER_PCT). Paper mode only. retain_fees_entry=True (the
        sec-12.5 close path, step 2) keeps the symbol's entry fee for on_paper_close to
        read; the close then clears it via close_position (step 7)."""
        return self._ledger(is_short).exit_fill_credit(
            symbol,
            qty,
            exit_price,
            writer=WRITER_ID,
            is_short=is_short,
            exit_reason=exit_reason,
            retain_fees_entry=retain_fees_entry,
        )

    def fees_entry_for(self, symbol: str) -> Decimal | None:
        """The taker entry fee retained for the symbol's open position, or None. The Exit
        Controller reads this through wm in the sec-12.5 close to compute net P&L. LIVE: the actual
        opening-fill executions fee retained at the open (FEE-CALC-006; no synthetic ledger). PAPER:
        the synthetic ledger's pos.fees_entry_usd. A symbol is open in at most ONE wallet (the mirror
        is symbol-keyed), so the first wallet holding a retained fee for it is the owner."""
        if self.modules is None:
            return self._live_fees_entry.get(symbol)
        for module in self.modules.values():
            fee = module.ledger.fees_entry_for(symbol)
            if fee is not None:
                return fee
        return None

    # --- the ENTRY dispatch flow (mod:Long_Module / mod:Short_Module -> shared seam) ---
    async def dispatch_entry(
        self,
        side: PositionSide,
        symbol: str,
        *,
        order_qty: object,
        entry_limit_price: object,
        emergsl_price: object,
        atr_14_entry: object | None = None,
        regime_at_entry: str | None = None,
        cl_ord_id: str,
        deadline: str,
        signal_params: dict | None = None,
        market_regime: str | None = None,
        entry_timestamp_utc: str | None = None,
    ) -> bool:
        """Dispatch a gate:G8-accepted entry for THIS side through the shared seam, then place
        its on-fill emergSL. The marketable-IOC entry fills-or-kills atomically (AR-054): in
        paper the simulator opens the position (record_execution attaches the Pending-Order-
        Registry D6 snapshot) + moves THIS side's wallet (routed by the order side); on a fill
        (UT-EE-005: batch_add ONLY on exec_type=filled) the off-book emergSL is placed (LONG
        sell-stop below / SHORT buy-to-cover reduce_only above). Returns True if the entry
        filled (a position opened). Everything traverses the seam (rule:HR-EE-013); a module
        NEVER calls ws_private directly.

        RETURN CONTRACT diverges by mode (PA-004 div #4). PAPER -> filled:bool (the simulator opened
        the position synchronously inside the seam.add_order call). LIVE -> dispatched:bool (the
        marketable-IOC entry transmitted; the fill is ASYNC, arriving later on the executions channel
        as record_execution OPENED, which attaches the D6 snapshot + enqueues the on-fill emergSL the
        after_batch pump places). LIVE returns False when the RL-MON-003 gate suppressed the entry (no
        order sent). LIVE NEVER checks has_position (the position is unknowable synchronously)."""
        if self.modules is None:
            return await self._dispatch_entry_live(
                side, symbol,
                order_qty=order_qty, entry_limit_price=entry_limit_price, emergsl_price=emergsl_price,
                atr_14_entry=atr_14_entry, regime_at_entry=regime_at_entry,
                cl_ord_id=cl_ord_id, deadline=deadline, signal_params=signal_params,
                market_regime=market_regime, entry_timestamp_utc=entry_timestamp_utc,
            )
        module = self.modules[side]
        # Pending Order Registry (AR-053 / UT-EE-010): stash the entry-time D6 snapshot so the
        # opening fill attaches it (emergsl_price for L3, atr_14_entry for L2, regime for tagging,
        # plus the contract:TRADE_CLOSE entry-side producer fields signal_params / market_regime /
        # entry_timestamp_utc the close emits - the per-trade level + entry context, sec 7).
        self._pending_entries[symbol] = {
            "emergsl_price": emergsl_price,
            "atr_14_entry": atr_14_entry,
            "regime_at_entry": regime_at_entry,
            "signal_params": signal_params,
            "market_regime": market_regime,
            "entry_timestamp_utc": entry_timestamp_utc,
        }
        try:
            # 1. ENTRY add_order (marketable IOC) -> the side's wallet + the opening position.
            await self.seam.add_order(
                module.build_entry(
                    symbol,
                    order_qty=order_qty,
                    entry_limit_price=entry_limit_price,
                    cl_ord_id=cl_ord_id,
                    deadline=deadline,
                )
            )
            # 2. ON FILL: place the off-book emergSL (UT-EE-005). A zero-fill IOC opens nothing,
            #    so there is nothing to protect - skip the batch_add.
            filled = self.positions.has_position(symbol)
            if filled:
                await self.seam.batch_add(
                    module.build_emergsl(
                        symbol,
                        order_qty=order_qty,
                        emergsl_price=emergsl_price,
                        cl_ord_id=f"{cl_ord_id}-sl",
                        deadline=deadline,
                    )
                )
            return filled
        finally:
            self._pending_entries.pop(symbol, None)

    async def _dispatch_entry_live(
        self,
        side: PositionSide,
        symbol: str,
        *,
        order_qty: object,
        entry_limit_price: object,
        emergsl_price: object,
        atr_14_entry: object | None = None,
        regime_at_entry: str | None = None,
        cl_ord_id: str,
        deadline: str,
        signal_params: dict | None = None,
        market_regime: str | None = None,
        entry_timestamp_utc: str | None = None,
    ) -> bool:
        """The LIVE entry dispatch (PA-004 div #4) - the async counterpart of the paper synchronous
        flow. The RL-MON-003 gate is checked FIRST: a suppressed entry sends NO order and returns
        False (the exit budget is preserved; exits/cancels are never gated). Otherwise the entry-time
        D6 snapshot is stashed in the Pending Order Registry (AR-053) so the ASYNC opening fill
        (record_execution OPENED) attaches it + enqueues the AR-054 on-fill emergSL the after_batch
        pump places, and the marketable-IOC entry add_order transmits over the seam (LONG spot buy /
        SHORT margin sell-to-open, ar:AR-009). Returns True (the entry was dispatched); the fill is
        confirmed later on the executions channel - this NEVER checks has_position (it is async)."""
        # RL-MON-003 (CRITICAL tier): suppress a new entry add_order while the pair is armed above the
        # critical rate fraction - the resting-exit rate budget is preserved (loss-prevention).
        if self._entry_suppression_check is not None and self._entry_suppression_check(symbol):
            self._emit_event(EntrySuppressed(symbol))
            return False
        # Pending Order Registry (AR-053 / UT-EE-010): the entry-time D6 snapshot the async opening
        # fill attaches (the emergsl_price for L3, atr_14_entry for L2, regime + the TRADE_CLOSE
        # entry-side producer fields). Kept until the fill resolves (record_execution OPENED pops it);
        # the paper path pops inline in its finally, but the live fill is a later executions frame.
        self._pending_entries[symbol] = {
            "emergsl_price": emergsl_price,
            "atr_14_entry": atr_14_entry,
            "regime_at_entry": regime_at_entry,
            "signal_params": signal_params,
            "market_regime": market_regime,
            "entry_timestamp_utc": entry_timestamp_utc,
        }
        # The marketable-IOC entry add_order (the emergSL is placed on the fill, not here - AR-054).
        await self.seam.add_order(
            build_entry_order(
                symbol, side,
                order_qty=order_qty, entry_limit_price=entry_limit_price,
                cl_ord_id=cl_ord_id, deadline=deadline,
            )
        )
        return True

    def set_entry_suppression_check(self, check: "Callable[[str], bool] | None") -> None:
        """Wire the RL-MON-003 entry-suppression predicate (the PrivateConnectionAssembler binds its
        private-connection RateCounter.is_entry_suppressed here - the predicate lives on the rate
        counter, the dispatch GATE is owned by WSManager per the sec-7 add_order OWNERSHIP note). The
        LIVE entry dispatch calls it FIRST and SKIPs the entry add_order when it returns True. The gate
        is ENTRY-only by construction (only the entry path consults it; the exit/cancel dispatch never
        does). None unwires it (the default - no suppression, e.g. paper / a rate-counter-less test)."""
        self._entry_suppression_check = check

    # --- sec 12.5 close surfaces (the Exit Controller calls these through `wm`) -------
    def close_position(self, symbol: str) -> Position | None:
        """sec 12.5 step 7: clear the symbol from the Position Mirror (rule:HR-PM-009 sole
        writer) and drop its retained entry fee from the synthetic ledger. The Exit
        Controller requests the clear here - it never mutates the mirror directly. The retained
        fee lives in the wallet that opened the symbol; clear_fees_entry is idempotent, so
        clearing it on BOTH wallets drops it from the owner and no-ops on the other."""
        cleared = self.positions.close(symbol, writer=WRITER_ID)
        if self.modules is not None:
            for module in self.modules.values():
                module.ledger.clear_fees_entry(symbol, writer=WRITER_ID)
        # Drop the symbol's max-over-life MAE tracking (a reopened symbol starts fresh). The Exit
        # Controller has already read it for the TRADE_CLOSE before requesting this clear.
        self._mae_tracker.clear(symbol)
        return cleared

    def mae_pct_high_for(self, symbol: str) -> "Decimal | None":
        """The max-over-life adverse excursion (pct of entry) tracked for the open symbol, or None if
        never marked. mod:Exit_Controller reads it at close to set the TRADE_CLOSE mae_pct_reached to
        the worst-over-the-hold heat (sharper than the at-exit reading) for the CIATS stop-width
        theory."""
        return self._mae_tracker.high(symbol)

    def update_selection_state_on_close(
        self, symbol: str, is_win: bool, side: PositionSide = PositionSide.LONG
    ) -> None:
        """sec 12.5 step 8 (ar:AR-073 / rule:HR-EC-014): the Exit Controller's sole-updater
        path into THIS SIDE's mod:Selection_Controller state (per-module, sec 7). LOSS ->
        increment this side's consecutive_loss_count + stamp its cooldown log (monotonic,
        rule:HR-SC-006); WIN -> reset this side's count to 0."""
        losses = self._selection_consecutive_loss[side]
        if is_win:
            losses[symbol] = 0
        else:
            losses[symbol] = losses.get(symbol, 0) + 1
            self._selection_cooldown[side][symbol] = self._now_monotonic()
        self._emit_event(SelectionStateUpdated(symbol, is_win, losses[symbol]))

    def release_exit_semaphore(self, side: PositionSide | None = None) -> None:
        """sec 12.5 step 9: release the G7 capital-commitment dispatch semaphore acquired at entry
        (ar:AR-043, D2 position-lifetime). `side` releases THAT module's per-side semaphore (the Exit
        Controller passes the closing position's side); guarded so a spurious release (no commitment
        held) cannot crash the close path. side=None is the legacy path -> the optional injected
        _exit_semaphore (back-compat)."""
        target = self._dispatch_sem[side] if side is not None else self._exit_semaphore
        if target is None:
            return
        try:
            target.release()
        except ValueError:
            # BoundedSemaphore over-release (nothing was acquired) - benign, never raises out.
            pass

    def dispatch_semaphore_locked(self, side: PositionSide) -> bool:
        """gate:G7 CHECK 4 - the non-blocking probe of THIS module's capital-commitment dispatch
        semaphore (ar:AR-043). True while the module holds an open commitment (D2 position-lifetime);
        the gate SKIPs a new candidate while locked. Does NOT acquire/mutate."""
        return self._dispatch_sem[side].locked()

    def consecutive_loss_count(self, symbol: str, side: PositionSide = PositionSide.LONG) -> int:
        """mod:Selection_Controller read helper - THIS SIDE's consecutive-loss count for the
        symbol (SC-Gate-3 reads it against param:sc_consecutive_limit; per-module, sec 7)."""
        return self._selection_consecutive_loss[side].get(symbol, 0)

    def exit_cooldown_at(
        self, symbol: str, side: PositionSide = PositionSide.LONG
    ) -> float | None:
        """mod:Selection_Controller read helper - the monotonic instant of THIS SIDE's last
        loss-exit on the symbol (SC-Gate-2 measures the cooldown against param:sc_cooldown_
        seconds; per-module, sec 7), or None if no loss-exit is on record."""
        return self._selection_cooldown[side].get(symbol)

    # --- ticker-driven paper exit detection + the sec-12.5 close drive ----------------
    def _emit_event(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    def handle_ticker(self, frame: Mapping[str, object]) -> None:
        """Ticker-channel handler (D1 WS-TKR-002 event_trigger:bbo for open-position pairs).
        BOTH modes mark the max-over-life MAE (the live MTM marking of open positions) so the close
        reads true worst-over-the-hold heat. Then each mode runs the ticker-bbo L2 MAE DETECTION
        (ar:AR-048): PAPER routes the fired exit through the sec-12.5 synthetic close (on_paper_close);
        LIVE enqueues a market-sell exit INTENT the async drive_live_exits driver dispatches (the sync
        read loop cannot await the order seam). The layer:L3 off-book emergSL is Kraken-side in both."""
        for entry in _ticker_entries(frame):
            symbol = _opt_str(entry.get("symbol"))
            if symbol is None:
                continue
            position = self.positions.get(symbol)
            if position is None:
                continue
            # Mark the max-over-life MAE on EVERY ticker in BOTH modes - the heat the close reads. In
            # live this feeds the executions-confirmed TRADE_CLOSE mae_pct_reached (sec 12.5 LIVE FLOW)
            # the same as paper: true worst-over-the-hold excursion, not the at-exit fallback. Mark
            # FIRST (incl. the breaching tick) so the running max captures the tick that fires the exit.
            self._mae_tracker.mark(position, entry.get("bid"), entry.get("ask"))
            if self.is_paper:
                signal = detect_paper_exit(position, entry.get("bid"), entry.get("ask"))
                if signal is not None:
                    self._drive_paper_exit(position, signal)
            else:
                # LIVE: detect the L2 MAE on the ticker bbo and ENQUEUE the market-sell intent; the
                # async driver owns the cancel-then-sell (the L3 emergSL fires Kraken-side, not here).
                self._detect_and_enqueue_live_exit(position, entry.get("bid"), entry.get("ask"))

    def _drive_paper_exit(self, position: Position, signal) -> None:
        """The sec-12.5 paper EXIT FLOW for one detected condition: (2) apply the synthetic
        ledger credit (retaining the entry fee for net P&L); (3) log PAPER_<EXIT_TYPE>_
        DETECTED; (4) hand off to the Exit Controller close path; (10) switch the symbol's
        ticker event_trigger back to trades-mode (no open position)."""
        symbol = position.symbol
        # 2. WSManager applies the HR-WM-032 ledger credit (retain the entry fee for step 5).
        update = self.apply_paper_exit_fill(
            symbol,
            position.qty,
            signal.exit_price,
            is_short=position.side is PositionSide.SHORT,
            exit_reason=signal.exit_reason,
            retain_fees_entry=True,
        )
        # 3. log PAPER_<EXIT_TYPE>_DETECTED (sec 12.6 taxonomy).
        if signal.layer == "L3_EMERGSL":
            self._emit_event(PaperEmergSlTriggered(symbol, signal.exit_price, signal.mae_pct))
        else:
            self._emit_event(PaperMaeDetected(symbol, signal.exit_price, signal.mae_pct))
        # 4-9. the EMITTING module's Exit Controller close path (TRADE_CLOSE through the side's
        # ciats_sink + clear mirror + AR-073 + sem). The side is known at close (position.side).
        self._exit_controller_for(position.side).on_paper_close(
            symbol, signal.exit_price, ExitReason(signal.exit_reason), update.fee_usd, self
        )
        # 10. switch the symbol's ticker event_trigger to trades-mode (position closed).
        self._update_ticker_event_trigger(symbol, has_position=False)

    # --- layer:L1a regime-reversal exit drive (EC-L1A-001 / EC-L1A-002) ----------------
    def on_regime_classified(
        self,
        symbol: str,
        classification: RegimeClassification,
        *,
        bid: object | None = None,
        ask: object | None = None,
        pair_status: PairStatus = PairStatus.ONLINE,
    ) -> None:
        """EC-L1A-002 Daily Regime Downgrade (ar:AR-062): run the L1a daily-downgrade check on
        a fresh mod:Regime_Engine classification for an open-position pair, immediately after
        the 00:00 UTC compute_regime. PAPER routes the fired downgrade through the sec-12.5 synthetic
        close; LIVE enqueues an L1a market-sell intent the async driver dispatches (cancel emergSL ->
        market sell). bid/ask are the realizable market-sell fill prices (ar:AR-048: bid for a long,
        ask for a short), supplied from the latest ticker by the daily-compute orchestrator (path C);
        pair_status feeds the rule:HR-EC-016(a) Step-1 precondition (checked at dispatch in live)."""
        position = self.positions.get(symbol)
        if position is None:
            return
        signal = detect_daily_regime_downgrade(position, classification)
        if signal is None:
            return
        if self.is_paper:
            self._drive_regime_exit(position, signal, bid, ask, pair_status)
        else:
            self._enqueue_live_regime_exit(position, signal, bid, ask, pair_status)

    def on_htf_ohlc_close(
        self,
        symbol: str,
        htf_ema_short: object,
        htf_ema_long: object,
        *,
        bid: object | None = None,
        ask: object | None = None,
        pair_status: PairStatus = PairStatus.ONLINE,
    ) -> None:
        """EC-L1A-001 HTF Regime Reversal (ar:AR-062): run the L1a 1H-EMA-reversal check on
        every 1H ohlc(60) close for an open-position pair. htf_ema_short / htf_ema_long are the
        current 1H EMA(20) / EMA(50). PAPER routes the fired reversal through the sec-12.5 synthetic
        close; LIVE enqueues an L1a market-sell intent the async driver dispatches. bid/ask +
        pair_status as on_regime_classified."""
        position = self.positions.get(symbol)
        if position is None:
            return
        signal = detect_htf_regime_reversal(position, htf_ema_short, htf_ema_long)
        if signal is None:
            return
        if self.is_paper:
            self._drive_regime_exit(position, signal, bid, ask, pair_status)
        else:
            self._enqueue_live_regime_exit(position, signal, bid, ask, pair_status)

    def on_instrument_status(
        self,
        symbol: str,
        pair_status: PairStatus,
        *,
        bid: object | None = None,
        ask: object | None = None,
    ) -> None:
        """ar:AR-040 instrument-status handler for an open-position pair (mod:Exit_Controller
        q4_triggers, the 4th normal-operation reason). A transition to limit_only triggers an ACTIVE
        exit via a SINGLE IOC limit close (LONG sell at best_bid / SHORT buy-to-cover at best_ask),
        exit_reason PAIR_LIMIT_ONLY_EXIT - distinct from the cancel_only / maintenance HOLD branch
        (which submits NO order; that gate lives in the L1a/L2 dispatch precondition). LIVE enqueues
        the intent the async driver dispatches (cancel emergSL -> the single IOC limit, rule:HR-EC-013
        cancel-then-close); a no-op for any other status. The live instrument-status channel that
        drives this is a later slice (driven directly for now, like on_regime_classified)."""
        if pair_status is not PairStatus.LIMIT_ONLY or not self.is_live:
            return
        position = self.positions.get(symbol)
        if position is None:
            return
        quote = bid if position.side is PositionSide.LONG else ask
        self._enqueue_live_exit(
            LiveExitIntent(
                symbol, position.side, ExitReason.PAIR_LIMIT_ONLY_EXIT.value,
                trigger="AR-040_LIMIT_ONLY", layer="L1a_LIMIT_ONLY",
                best_quote=quote, pair_status=pair_status, close_type=_CLOSE_LIMIT_ONLY,
            )
        )

    def _drive_regime_exit(
        self,
        position: Position,
        signal: RegimeExitSignal,
        bid: object | None,
        ask: object | None,
        pair_status: PairStatus,
    ) -> None:
        """The sec-12.5 paper EXIT FLOW for a fired layer:L1a regime exit. rule:HR-EC-016(a)
        Step 1 (pair-status precondition) runs FIRST - BEFORE the ledger credit - so a HELD
        exit never moves the synthetic ledger. Then the same sec-12.5 sequence as the ticker
        path: (2) ledger credit (retain entry fee); (3) PAPER_REGIME_EXIT_DETECTED; (4-9) the
        Exit Controller close; (10) ticker trades-mode."""
        symbol = position.symbol
        # 1. rule:HR-EC-016(a) Step 1: pair-status precondition. cancel_only / maintenance ->
        # HOLD + CRITICAL alert + NO order (the resting emergSL is the only protection).
        if l1a_precondition_blocks(pair_status):
            self._emit_event(
                L1aExitHeld(symbol, pair_status.value, signal.exit_reason, signal.trigger)
            )
            return
        # The realizable market-sell fill price (ar:AR-048: bid for a long, ask for a short).
        quote = bid if position.side is PositionSide.LONG else ask
        if quote is None:
            self._emit_event(RegimeExitNoQuote(symbol, signal.exit_reason, signal.trigger))
            return
        exit_price = Decimal(str(quote))
        # 2. WSManager applies the HR-WM-032 ledger credit (retain the entry fee for net P&L).
        update = self.apply_paper_exit_fill(
            symbol,
            position.qty,
            exit_price,
            is_short=position.side is PositionSide.SHORT,
            exit_reason=signal.exit_reason,
            retain_fees_entry=True,
        )
        # 3. log PAPER_REGIME_EXIT_DETECTED (sec 12.6).
        self._emit_event(
            PaperRegimeExitDetected(symbol, exit_price, signal.exit_reason, signal.trigger)
        )
        # 4-9. the EMITTING module's Exit Controller close path (the SAME on_paper_close, routed by
        # position.side - no double-close: a cleared mirror makes any follow-on ticker detection a
        # surfaced PAPER_CLOSE_SKIPPED).
        self._exit_controller_for(position.side).on_paper_close(
            symbol, exit_price, ExitReason(signal.exit_reason), update.fee_usd, self
        )
        # 10. switch the symbol's ticker event_trigger to trades-mode (position closed).
        self._update_ticker_event_trigger(symbol, has_position=False)

    def _update_ticker_event_trigger(self, symbol: str, *, has_position: bool) -> None:
        """WS-TKR-003: switch a pair's ticker event_trigger on a position-state change
        (opened -> bbo, closed -> trades). Records the mode + emits TICKER_TRIGGER_SWITCHED;
        the WS-client re-subscribe is the data-layer edge (S2c)."""
        new_trigger = _TRIGGER_BBO if has_position else _TRIGGER_TRADES
        if self._ticker_event_trigger.get(symbol) == new_trigger:
            return
        self._ticker_event_trigger[symbol] = new_trigger
        self._emit_event(TickerTriggerSwitched(symbol, new_trigger))

    def ticker_event_trigger(self, symbol: str) -> str:
        """The pair's current ticker event_trigger mode (bbo for an open-position pair,
        else trades - the WS-TKR-002 default)."""
        return self._ticker_event_trigger.get(symbol, _TRIGGER_TRADES)

    @property
    def spot_usd_balance(self) -> Decimal | None:
        """The LONG module's synthetic spot_usd_balance (paper), or None in live mode (real
        Kraken balance authoritative, sec 12.4). Back-compat accessor - the long/default wallet;
        use wallet_balance(side) for either side's wallet."""
        return None if self.modules is None else self.modules[PositionSide.LONG].wallet_balance

    def wallet_balance(self, side: PositionSide) -> Decimal | None:
        """THIS SIDE's synthetic wallet balance (paper): the Long wallet is spot USD, the Short
        wallet is Kraken margin equity (sec 7 per-module). None in live mode."""
        return None if self.modules is None else self.modules[side].wallet_balance

    # --- Position Mirror read helpers (the rule:HR-PM-009 read contract) ------
    def position(self, symbol: str) -> Position | None:
        """The open position for a symbol (frozen record), or None."""
        return self.positions.get(symbol)

    def has_position(self, symbol: str) -> bool:
        return self.positions.has_position(symbol)

    def open_position_symbols(self) -> frozenset[str]:
        return self.positions.open_symbols()

    def open_positions(self) -> dict[str, Position]:
        """A copy of the open-position store (records are frozen)."""
        return self.positions.positions()


def _ticker_entries(frame: Mapping[str, object]) -> list[Mapping[str, object]]:
    """The per-symbol ticker entries in a Kraken WS v2 ticker frame. The wire shape wraps
    them in a "data" array ({channel:"ticker", type:..., data:[{symbol, bid, ask, ...}]});
    a flat single-symbol dict is accepted too (the field names are identical either way)."""
    data = frame.get("data")
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes)):
        return [e for e in data if isinstance(e, Mapping)]
    if frame.get("symbol") is not None:
        return [frame]
    return []


def _opt_str(value: object) -> str | None:
    return None if value is None else str(value)


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the math (ar:AR-047)."""
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _emergsl_cl_ord_id(position: Position) -> str:
    """The cl_ord_id of the resting off-book emergSL leg for build_cancel_order. Prefers the
    position's recorded emergsl_id (the AR-054 ON-FILL batch_add leg); falls back to the entry
    cl_ord_id with the '-sl' suffix dispatch_entry stamps on the emergSL leg, else a symbol-derived
    id so the cancel always carries a client id."""
    if position.emergsl_id is not None:
        return position.emergsl_id
    if position.cl_ord_id is not None:
        return f"{position.cl_ord_id}-sl"
    return f"{position.symbol}-sl"


def _order_cl_ord_id(message: Mapping[str, object]) -> str | None:
    """The cl_ord_id of an OUTBOUND order message (build_market_sell_order / build_mpp_retry_order /
    build_limit_only_exit_order) - the key the C-1 MPP-reject probe correlates the add_order response
    against. Reads params.cl_ord_id (the WS v2 shape); None when the message carries no client id."""
    params = message.get("params")
    if isinstance(params, Mapping):
        cl = params.get("cl_ord_id")
        if cl is not None:
            return str(cl)
    cl = message.get("cl_ord_id")
    return str(cl) if cl is not None else None


def _frame_fee(event: Mapping[str, object]) -> Decimal:
    """The actual fee on an executions fill frame (FEE-CALC-006: the Kraken executions-channel fee
    field, Decimal(str(fee)) on receipt - the closed-trade record-of-truth, never the calculated
    estimate). Decimal('0') when the frame carries no fee field (a frame that never moved capital)."""
    fee = event.get("fee")
    return Decimal(str(fee)) if fee is not None else Decimal("0")
