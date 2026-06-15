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
(execution/exit_controller.py) for the SAME close sequence - the 23-field evt:TRADE_CLOSE
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

import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from .connection import ConnectionRole, WSConnection
from .dispatch import Channel, DispatchTable, Handler
from .ledger import LedgerUpdate, SyntheticCapitalLedger
from .outbound import PaperDispatchSimulator, PrivateTransmitter
from .paper_exit import PaperEmergSlTriggered, PaperMaeDetected, detect_paper_exit
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
from ..config.settings import Mode
from ..execution.exit_controller import ExitController, ExitReason
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
    ) -> None:
        self._mode = mode  # frozen for process lifetime (rule:HR-WM-021)
        self._on_event = on_event
        # Clocks injected (the keepalive/silent_pair pattern) so the close path is
        # deterministic under test. The SC-Gate-2 cooldown MUST be measured with a
        # MONOTONIC clock (rule:HR-SC-006); the TRADE_CLOSE ts uses UTC wall time.
        self._now_monotonic: MonotonicClock = now_monotonic or time.monotonic
        self._now_utc: UtcClock = now_utc or (lambda: datetime.now(timezone.utc))
        # The G7 capital-commitment BoundedSemaphore (acquired at entry, released on close,
        # sec 12.5 step 9). Injected; None until mod:Risk_Engine wires the acquire side.
        self._exit_semaphore = exit_semaphore
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
            # mod:Exit_Controller owns the sec-12.5 close path; in paper mode WSManager
            # detects the exit on the ticker bbo and routes it here. Live exit handling is
            # executions-driven (a later slice), so the controller is paper-only for now.
            self.exit_controller: ExitController | None = ExitController(
                on_event=on_event, clock=self._now_utc
            )
        else:
            self.paper_dispatch = PaperDispatchSimulator()
            self.exit_controller = None

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
        by the Exit Controller for L2 MAE / L3 emergSL detection."""
        outcome = self.positions.apply_execution(
            event,
            writer=WRITER_ID,
            regime_at_entry=regime_at_entry,
            emergsl_id=emergsl_id,
            atr_14_entry=atr_14_entry,
            emergsl_price=emergsl_price,
        )
        # WS-TKR-003: a pair with an open position uses ticker event_trigger:bbo (faster
        # adverse-price detection); switch it on the opening fill (the close switches it
        # back to trades in the sec-12.5 step 10 / the DISPATCH-path executions close).
        if outcome.action is PositionAction.OPENED and outcome.position is not None:
            self._update_ticker_event_trigger(outcome.position.symbol, has_position=True)
        elif outcome.action is PositionAction.CLOSED:
            closed_symbol = _opt_str(event.get("symbol"))
            if closed_symbol is not None:
                self._update_ticker_event_trigger(closed_symbol, has_position=False)
        return outcome

    def restore_position_mirror(
        self, snap_orders: Sequence[Mapping[str, object]]
    ) -> list[PositionClosedDuringGap]:
        """Reconcile the mirror against the executions snapshot on reconnect/startup
        (AR-056 RESTORE_POSITION_MIRROR / Step 6); returns the gap-closed positions."""
        return self.positions.restore_from_snapshot(snap_orders, writer=WRITER_ID)

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
        """The taker entry fee retained for the symbol's open paper position
        (pos.fees_entry_usd), or None (no open paper position / live mode). The Exit
        Controller reads this through wm in the sec-12.5 close to compute net P&L. A symbol is
        open in at most ONE wallet (the mirror is symbol-keyed), so the first wallet holding a
        retained fee for it is the owner."""
        if self.modules is None:
            return None
        for module in self.modules.values():
            fee = module.ledger.fees_entry_for(symbol)
            if fee is not None:
                return fee
        return None

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
        return cleared

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

    def release_exit_semaphore(self) -> None:
        """sec 12.5 step 9: release the G7 capital-commitment BoundedSemaphore acquired at
        entry. A no-op until mod:Risk_Engine wires the acquire side (the semaphore stays
        None); guarded so a spurious release on an un-acquired semaphore cannot raise."""
        if self._exit_semaphore is None:
            return
        try:
            self._exit_semaphore.release()
        except ValueError:
            # BoundedSemaphore.release() past its initial value - nothing was acquired.
            pass

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
        In paper mode, evaluates each open position against the bbo tick (ar:AR-048) and
        routes any fired exit through the sec-12.5 close. Live exit detection is executions-
        driven (a later slice), so this is a paper-mode no-op in live."""
        if not self.is_paper:
            return
        for entry in _ticker_entries(frame):
            symbol = _opt_str(entry.get("symbol"))
            if symbol is None:
                continue
            position = self.positions.get(symbol)
            if position is None:
                continue
            signal = detect_paper_exit(position, entry.get("bid"), entry.get("ask"))
            if signal is not None:
                self._drive_paper_exit(position, signal)

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
        # 4-9. the Exit Controller close path (TRADE_CLOSE + clear mirror + AR-073 + sem).
        self.exit_controller.on_paper_close(
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
        the 00:00 UTC compute_regime. Paper-mode close routing; a no-op in live (exit detection
        is executions-driven there, a later slice). bid/ask are the realizable market-sell fill
        prices (ar:AR-048: bid for a long, ask for a short), supplied from the latest ticker by
        the daily-compute orchestrator (path C); pair_status feeds the rule:HR-EC-016(a)
        Step-1 precondition (the live instrument-status channel is a later slice)."""
        if not self.is_paper:
            return
        position = self.positions.get(symbol)
        if position is None:
            return
        signal = detect_daily_regime_downgrade(position, classification)
        if signal is not None:
            self._drive_regime_exit(position, signal, bid, ask, pair_status)

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
        current 1H EMA(20) / EMA(50). Paper-mode close routing; a no-op in live. bid/ask +
        pair_status as on_regime_classified."""
        if not self.is_paper:
            return
        position = self.positions.get(symbol)
        if position is None:
            return
        signal = detect_htf_regime_reversal(position, htf_ema_short, htf_ema_long)
        if signal is not None:
            self._drive_regime_exit(position, signal, bid, ask, pair_status)

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
        # 4-9. the Exit Controller close path (the SAME on_paper_close - no double-close: a
        # cleared mirror makes any follow-on ticker detection a surfaced PAPER_CLOSE_SKIPPED).
        self.exit_controller.on_paper_close(
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
