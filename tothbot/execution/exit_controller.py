"""mod:Exit_Controller - the normal-exit owner + the paper close path (sec 12.5).

Source: 0500000 dv1_242 sec 12.5 (Paper Exit Routing Through Exit Controller) +
sec 3 Image3 (Exit Architecture: L1a/L2/L3) + sec 7 Image6 (mod:Exit_Controller +
evt:TRADE_CLOSE 23-field schema) + the D1 FEE-CALC-004 net-P&L formula + ar:AR-048
(bid/ask MAE) + ar:AR-073 / rule:HR-EC-014 (Selection Controller state update).

mod:Exit_Controller owns ALL normal exits (layer:L1a regime-reversal take-profit +
layer:L2 MAE threshold); layer:L3 off-book emergSL is Kraken-side crash protection
only. This module builds the CLOSE PATH - the common close sequence every paper exit
routes through (sec 12.5), the "cornerstone of paper-live CIATS data-flow parity"
(PA-005). The exit DETECTION (which condition fired, on the ticker bbo) is upstream in
mod:WS_Manager (paper_exit.py); this module is handed the decided (symbol, exit_price,
exit_reason, fees_exit) and runs the close.

THE sec-12.5 CLOSE SEQUENCE (steps 5-9 are owned here; 1-4 + 10 are WSManager):
  5. on_paper_close -> _close_position(...)
  6. emit evt:TRADE_CLOSE - the 23-field contract:CIATS_Trade_Outcome_Bus record
     (component EXIT_CTRL), the PRIMARY DATA SUBSTRATE for CIATS Stream 2 inference.
  7. clear the Position Mirror - via the WSManager sole-writer surface (rule:HR-PM-009:
     "WS Manager is the SOLE writer to Position Mirror"; the EC does NOT mutate it
     directly - it requests the close through wm.close_position).
  8. update Selection Controller state - via the WSManager AR-073 win/loss path
     (rule:HR-EC-014: the EC is the SOLE updater of SC state, executed via WSManager):
     a LOSS increments consecutive_loss_count[symbol] + sets exit_cooldown_log[symbol];
     a WIN resets consecutive_loss_count[symbol] to 0.
  9. release the BoundedSemaphore - the G7 capital-commitment guard acquired at entry.

NET P&L (D1 FEE-CALC-004, all Decimal, NO float per ar:AR-047), direction-symmetric:
  long_gross  = (exit_price - entry_fill_price) * qty
  short_gross = (entry_fill_price - exit_price) * qty
  net_pl_usd  = gross - fees_entry_usd - fees_exit_usd
The figure writes the long form and states it is "applied identically to every exit
path" (the four reasons); the short leg is the directional mirror (mod:Short_Module).

PRODUCER-SOURCED record fields not yet wired (no inference loss - the schema is whole,
the values fill in as their producers land): entry_timestamp_utc + hold_candle_count
(no entry-time / candle-count writer yet), asset_regime + market_regime + signal_params
(mod:Regime_Engine + mod:Signal_Pipeline are S3). vol_regime is derived from the
position's regime_at_entry token when present. MAE_pct_reached is the adverse excursion
AT THE EXIT TICK (the bbo bid/ask that fired the exit); a full max-over-life MAE wants
the MTM tracker (carry-forward) - until then the at-exit value is the faithful floor.

This is a PURE close engine: no socket, no asyncio. It reads the Position Mirror + the
synthetic ledger through the WSManager helper surfaces passed in as `wm`, and emits the
TRADE_CLOSE record to the injected event sink. One per module (per wallet).
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum

from ..config import registry

EventSink = Callable[[object], None]
Clock = Callable[[], datetime]


class ExitReason(Enum):
    """The exit reasons carried on a TRADE_CLOSE record (sec 7 Image6 + Image3).

    The FOUR mod:Exit_Controller normal-operation reasons, plus EMERGENCY_SL_FIRED -
    the layer:L3_Emergency_SL crash-recovery reason (a DISTINCT producer on the same
    contract:CIATS_Trade_Outcome_Bus; in live it is backfilled from the executions
    channel on TothBot recovery, in paper it is the synthetic emergSL bbo touch)."""

    MAE_THRESHOLD_BREACH = "MAE_THRESHOLD_BREACH"
    HTF_REGIME_REVERSAL = "HTF_REGIME_REVERSAL"
    DAILY_REGIME_DOWNGRADE = "DAILY_REGIME_DOWNGRADE"
    PAIR_LIMIT_ONLY_EXIT = "PAIR_LIMIT_ONLY_EXIT"
    EMERGENCY_SL_FIRED = "EMERGENCY_SL_FIRED"


@dataclass(frozen=True)
class TradeClose:
    """evt:TRADE_CLOSE - the 23-field contract:CIATS_Trade_Outcome_Bus record (sec 7
    Image6 schema_fields_canonical; THIS event is the canonical schema source). Emitted
    by mod:Exit_Controller on every COMPLETE position close; mod:Logger appends it to the
    durable Stream-2 corpus. Byte-identical paper <-> live (PA-005). Field order follows
    the figure's 23-field enumeration; producer-unsourced fields default to None."""

    # (1)-(4) record identity (constant literals per the figure)
    symbol: str                                  # (5)
    entry_fill_price: Decimal                    # (6) avg_price at entry fill
    exit_price: Decimal                          # (7)
    exit_reason: ExitReason                       # (15)
    fees_entry_usd: Decimal                       # (12) taker fee on entry
    fees_exit_usd: Decimal                        # (13) taker fee on exit
    fees_total_usd: Decimal                       # (14)
    net_pl_usd: Decimal                           # (21)
    net_gain_usd: Decimal                         # (22) 0.0 if loss
    net_loss_usd: Decimal                         # (23) 0.0 if gain, positive if loss
    ts: str | None = None                        # (1) ISO 8601 UTC
    entry_timestamp_utc: str | None = None       # (8)
    exit_timestamp_utc: str | None = None        # (9)
    hold_candle_count: int | None = None          # (10) 5m candles held
    mae_pct_reached: Decimal | None = None        # (11) max adverse move as pct
    asset_regime: str | None = None               # (16) pair regime at entry
    vol_regime: str | None = None                 # (17) NORMAL_VOL | ELEVATED_VOL
    market_regime: str | None = None              # (18) BTC/USD anchor regime
    signal_params: dict | None = None             # (19)
    actual_rr: Decimal | None = None              # (20) net_PL / risk_exposed
    event: str = field(default="TRADE_CLOSE", init=False)   # (2)
    level: str = field(default="INFO", init=False)          # (3)
    component: str = field(default="EXIT_CTRL", init=False)  # (4)


@dataclass(frozen=True)
class PaperCloseSkipped:
    """PAPER_CLOSE_SKIPPED [WARNING] - on_paper_close was called for a symbol with no
    open position in the mirror (already closed, or never opened). Surfaced, never a
    silent no-op (the close path must never silently drop a requested close)."""

    symbol: str
    reason: str
    code: str = field(default="PAPER_CLOSE_SKIPPED", init=False)


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the math (ar:AR-047)."""
    return Decimal(str(value))


# param:mae_mult (CIATS-owned, 1.5x starting) - the L2 risk-leg multiplier; the close
# uses it to express actual_RR's risk_exposed = atr_14_entry * mae_mult * qty (the MAE
# dollar risk). Converted to Decimal once (ar:AR-047).
_MAE_MULT = _dec(registry.value("mae_mult"))

# The position-side tokens (mirrors position_mirror.PositionSide.value) used to apply the
# direction-symmetric net-P&L leg without importing the enum (keeps this a pure close
# engine over the wm-read Position record).
_SIDE_LONG = "long"
_SIDE_SHORT = "short"


class ExitController:
    """mod:Exit_Controller close path (sec 12.5). One per module wallet.

    Constructed with the event sink + an optional UTC clock (injected so the close is
    deterministic under test, per the keepalive/silent_pair clock-injection pattern).
    on_paper_close is handed the decided exit by WSManager and reads/writes all position
    + ledger + SC + semaphore state through the `wm` handle's sole-writer surfaces.
    """

    def __init__(
        self,
        *,
        on_event: EventSink | None = None,
        clock: Clock | None = None,
        mae_mult: object | None = None,
    ) -> None:
        self._on_event = on_event
        self._clock = clock
        self._mae_mult = _dec(mae_mult) if mae_mult is not None else _MAE_MULT

    def set_event_sink(self, on_event: EventSink | None) -> None:
        """Rebind the close-path event sink. The operational assembly calls this (via
        wm.set_ciats_exit_sinks) to make THIS module's TRADE_CLOSE emit THROUGH the side's
        CIATS learning sink (sec 7): the conductor's learning close + the HR-CI-003 inbox
        boundary poll, plus mod:Logger Stream-1/Stream-2 with the module tag. One per module
        wallet; until wired the sink is the WSManager's general on_event (telemetry only)."""
        self._on_event = on_event

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    def _now_iso(self) -> str | None:
        return None if self._clock is None else self._clock().isoformat()

    def on_paper_close(
        self,
        symbol: str,
        exit_price: object,
        exit_reason: ExitReason,
        fees_exit: object,
        wm: object,
    ) -> TradeClose | None:
        """sec 12.5 step 4/5: the WSManager-invoked close entry point. Reads the open
        position from the mirror via wm helpers and runs _close_position. Returns the
        emitted TradeClose, or None if there is no open position to close (surfaced as
        PAPER_CLOSE_SKIPPED, never silently dropped)."""
        position = wm.position(symbol)
        if position is None:
            self._emit(PaperCloseSkipped(symbol, "no open position in the mirror to close"))
            return None
        return self._close_position(position, _dec(exit_price), exit_reason, _dec(fees_exit), wm)

    def _close_position(
        self,
        position: object,
        exit_price: Decimal,
        exit_reason: ExitReason,
        fees_exit: Decimal,
        wm: object,
    ) -> TradeClose:
        """sec 12.5 steps 6-9: assemble + emit the 23-field TRADE_CLOSE record, then
        clear the mirror, update Selection Controller state (AR-073), and release the
        semaphore - the mirror + SC writes routed through the WSManager sole writer."""
        symbol = position.symbol
        entry = position.avg_entry_price
        qty = position.qty
        is_long = getattr(position.side, "value", position.side) == _SIDE_LONG

        # fees_entry retained per symbol on the synthetic ledger since the entry fill
        # (the diagram's pos.fees_entry_usd, "required for net P&L on close"); read it
        # BEFORE the close clears it.
        fe = wm.fees_entry_for(symbol)
        fees_entry = _dec(fe) if fe is not None else Decimal("0")

        # Net P&L - D1 FEE-CALC-004, direction-symmetric (all Decimal, ar:AR-047).
        gross = (exit_price - entry) * qty if is_long else (entry - exit_price) * qty
        net_pl = gross - fees_entry - fees_exit
        is_win = net_pl > 0
        net_gain = net_pl if is_win else Decimal("0")
        net_loss = Decimal("0") if is_win else -net_pl
        fees_total = fees_entry + fees_exit

        # MAE_pct_reached - the adverse excursion at the exit tick (ar:AR-048: long uses
        # the bid, short the ask; exit_price IS that bbo price for an MAE/emergSL exit).
        # Clamped at 0 (a favorable-side exit, e.g. a regime reversal in profit, reached
        # no adverse excursion at exit). A max-over-life MAE wants the MTM tracker.
        adverse = (entry - exit_price) if is_long else (exit_price - entry)
        mae_pct = (adverse / entry) if entry != 0 and adverse > 0 else Decimal("0")

        # actual_RR = net_PL / risk_exposed, risk_exposed = the MAE dollar risk leg
        # (atr_14_entry * mae_mult * qty); None when no entry-time ATR snapshot is on the
        # position (the entry-side sizing producer has not run).
        atr = position.atr_14_entry
        risk_exposed = (_dec(atr) * self._mae_mult * qty) if atr is not None else None
        actual_rr = (net_pl / risk_exposed) if risk_exposed not in (None, Decimal("0")) else None

        # vol_regime derived from the entry regime token (NORMAL_VOL | ELEVATED_VOL);
        # asset_regime carries the token verbatim; market_regime + signal_params are
        # S3 producers (None until wired).
        regime = position.regime_at_entry
        vol_regime = _vol_regime_of(regime)

        now = self._now_iso()
        record = TradeClose(
            symbol=symbol,
            entry_fill_price=entry,
            exit_price=exit_price,
            exit_reason=exit_reason,
            fees_entry_usd=fees_entry,
            fees_exit_usd=fees_exit,
            fees_total_usd=fees_total,
            net_pl_usd=net_pl,
            net_gain_usd=net_gain,
            net_loss_usd=net_loss,
            ts=now,
            exit_timestamp_utc=now,
            mae_pct_reached=mae_pct,
            asset_regime=regime,
            vol_regime=vol_regime,
            actual_rr=actual_rr,
        )
        # 6. emit TRADE_CLOSE (the canonical Stream-2 record).
        self._emit(record)
        # 7. clear the Position Mirror through the sole writer (rule:HR-PM-009).
        wm.close_position(symbol)
        # 8. update Selection Controller state via the WSManager AR-073 path (HR-EC-014).
        wm.update_selection_state_on_close(symbol, is_win, position.side)
        # 9. release the G7 capital-commitment semaphore.
        wm.release_exit_semaphore()
        return record


def _vol_regime_of(regime: str | None) -> str | None:
    """vol_regime enum (NORMAL_VOL | ELEVATED_VOL) derived from the asset regime token
    (the six-regime taxonomy carries the volatility tier in its name, e.g.
    TRENDING_POS_ELEVATED -> ELEVATED_VOL). None when no regime was tagged at entry."""
    if regime is None:
        return None
    return "ELEVATED_VOL" if "ELEVATED" in regime.upper() else "NORMAL_VOL"
