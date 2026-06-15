"""contract:Synthetic_Capital_Ledger - the paper-mode synthetic spot_usd_balance.

Source: 0500000 dv1_241 sec 12.4 (Synthetic Capital Ledger - Single-Owner
Architecture) + sec 12 Image7 PA-004(3) Capital Ledger paper cell + sec 12.6
Paper Telemetry (evt:PAPER_LEDGER_UPDATED) + the FEE-CALC fee math
(FEE_MAKER_PCT / FEE_TAKER_PCT) + ar:AR-047 Decimal-on-receipt.

PA-004 divergence point #3 (capital ledger). In paper mode no real capital
moves, so a synthetic spot_usd_balance is maintained ENTIRELY inside WSManager to
give CIATS a P&L signal. The synthetic ledger uses IDENTICAL fee arithmetic to
live (FEE_TAKER_PCT on both entry and exit legs), so paper P&L is shape-equivalent
to live P&L and the CIATS Half-Kelly corpus is unified across modes (PA-005). In
live mode this contract is bypassed (real Kraken balances are authoritative); the
single-owner write property holds in BOTH modes - WSManager is the sole writer of
spot_usd_balance either way (sec 12.4 OWNERSHIP).

SINGLE-OWNER (rule:HR-WM-032). WSManager is the SOLE writer of spot_usd_balance.
No other module may mutate it. This mirrors the position_mirror.py sole-writer
pattern (rule:HR-PM-009): every mutating method requires the writer identity
WRITER_ID; any other writer raises LedgerSoleWriterViolationError. This eliminates
the distributed-write defect class by construction (sec 12.4 WHY SINGLE-OWNER).

This is a PURE state store (mirrors position_mirror.py / keepalive.py - no socket,
no asyncio): the init / entry-fill debit / exit-fill credit arithmetic is
unit-testable without a network. NO float ever enters the ledger (AR-047):
every value is taken as Decimal(str(value)) on receipt, and the fee percentages
are converted to Decimal exactly once here.

Per-module wallet (sec 7): each module's synthetic wallet seeds at
paper_starting_balance ($5,000 long / $5,000 short per decision:D-05); construct
one ledger per module wallet (the assembler passes the wallet's seed). The
ledger also retains the per-symbol entry fee (pos.fees_entry_usd in the diagram)
so the Exit Controller can compute net P&L on close (a LATER module) - it is
synthetic-capital state, so its single owner is this ledger.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

from ..config.fees import FEE_TAKER_PCT

# rule:HR-WM-032 / sec 12.4 OWNERSHIP - the ONLY identity permitted to write the
# synthetic ledger. Logged as writer_id on every mutation; any other writer is a
# sole-writer violation. Same sentinel as the Position Mirror (position_mirror.py
# WRITER_ID): WSManager owns BOTH the mirror writes and the ledger writes.
WRITER_ID = "WS_Manager"

# FEE_TAKER_PCT as Decimal, converted exactly once (AR-047: never Decimal(float)).
# The marketable-IOC entry fills at taker and ALL exits close at taker (sec 12.4),
# so the synthetic ledger uses the taker rate on both legs.
_FEE_TAKER = Decimal(str(FEE_TAKER_PCT))

EventSink = Callable[[object], None]


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the ledger (AR-047)."""
    return Decimal(str(value))


class LedgerEventType(Enum):
    """The three synthetic-ledger mutation kinds (sec 12.6 PAPER_LEDGER_UPDATED
    payload event_type)."""

    INIT = "INIT"              # startup seed (spot_usd_balance = paper_starting_balance)
    ENTRY_FILL = "ENTRY_FILL"  # simulated entry-fill debit (proceeds + taker fees)
    EXIT_FILL = "EXIT_FILL"    # simulated exit-fill credit (proceeds - taker fees)


@dataclass(frozen=True)
class PaperLedgerUpdated:
    """evt:PAPER_LEDGER_UPDATED [MEDIUM] - every synthetic ledger mutation, routed to
    mod:Logger via channel:logger_async_queue and consumed downstream by per-wallet
    drawdown evaluation (AR-052). The sec 12.6 payload spine (event_type, symbol,
    delta_usd, fee_usd, new_balance, exit_reason) plus the Image7 q5 per-update
    telemetry (prior_balance, fill_price, qty). delta_usd is the SIGNED balance change
    (negative on ENTRY_FILL debit, positive on EXIT_FILL credit; 0 on INIT)."""

    event_type: LedgerEventType
    new_balance: Decimal
    prior_balance: Decimal
    delta_usd: Decimal = Decimal("0")
    fee_usd: Decimal = Decimal("0")
    symbol: str | None = None
    fill_price: Decimal | None = None
    qty: Decimal | None = None
    exit_reason: str | None = None
    writer_id: str = WRITER_ID
    code: str = field(default="PAPER_LEDGER_UPDATED", init=False)


@dataclass(frozen=True)
class LedgerUpdate:
    """The result of one ledger mutation (for the caller + tests): the new balance,
    the signed delta, and the fee leg charged."""

    event_type: LedgerEventType
    new_balance: Decimal
    delta_usd: Decimal
    fee_usd: Decimal


class LedgerSoleWriterViolationError(RuntimeError):
    """Raised when a ledger mutation is attempted by a writer other than WS_Manager
    (rule:HR-WM-032 single-owner). The accompanying PAPER_LEDGER_UPDATED is NOT
    emitted - the write never happened."""


@dataclass(frozen=True)
class LedgerSoleWriterViolation:
    """LEDGER_SOLE_WRITER_VIOLATION [CRITICAL] {attempted_writer, method} - a ledger
    write attempted by a writer other than WS_Manager (rule:HR-WM-032 defense-in-depth,
    the sec 12.4 single-owner invariant)."""

    attempted_writer: str
    method: str
    code: str = field(default="LEDGER_SOLE_WRITER_VIOLATION", init=False)


class SyntheticCapitalLedger:
    """The single-owner synthetic spot_usd_balance (contract:Synthetic_Capital_Ledger).

    Construct ONE per module wallet, seeded with that wallet's paper_starting_balance
    ($5,000 long / $5,000 short per D-05). WSManager - and only WSManager - drives the
    write surface (entry_fill_debit / exit_fill_credit, writer=WRITER_ID); every other
    module READS via helpers (sec 12.4 HR-PM-009 read-only-via-helpers pattern).

    INITIALIZATION (sec 12.4): on construction spot_usd_balance = paper_starting_balance,
    portfolio_baseline_USD = spot_usd_balance captured ONCE (rule:HR-WM-011), and a
    PAPER_LEDGER_UPDATED (event_type=INIT) is emitted.
    """

    def __init__(
        self,
        starting_balance: object,
        *,
        on_event: EventSink | None = None,
    ) -> None:
        self._balance = _dec(starting_balance)
        # portfolio_baseline_USD captured ONCE at startup (rule:HR-WM-011); the
        # per-wallet drawdown halts measure against this baseline (AR-052 / TB00000 sec 7).
        self._portfolio_baseline = self._balance
        self._on_event = on_event
        # Per-symbol entry fee retained for the Exit Controller's net P&L on close
        # (the diagram's pos.fees_entry_usd; synthetic-capital state, single-owner here).
        self._fees_entry: dict[str, Decimal] = {}
        self._emit(
            PaperLedgerUpdated(
                event_type=LedgerEventType.INIT,
                new_balance=self._balance,
                prior_balance=self._balance,
            )
        )

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    def _guard_writer(self, writer: str, method: str) -> None:
        """rule:HR-WM-032: only WS_Manager may write the synthetic ledger. Emit the
        CRITICAL violation event and raise on any other writer."""
        if writer != WRITER_ID:
            self._emit(LedgerSoleWriterViolation(attempted_writer=writer, method=method))
            raise LedgerSoleWriterViolationError(
                f"{method}: only {WRITER_ID!r} may write the synthetic ledger "
                f"(HR-WM-032); got {writer!r}"
            )

    # --- read helpers (the single-owner READ contract - sec 12.4) ----------------
    @property
    def balance(self) -> Decimal:
        """The current synthetic spot_usd_balance (frozen Decimal)."""
        return self._balance

    @property
    def portfolio_baseline(self) -> Decimal:
        """portfolio_baseline_USD - captured ONCE at init (rule:HR-WM-011)."""
        return self._portfolio_baseline

    def fees_entry_for(self, symbol: str) -> Decimal | None:
        """The taker entry fee charged when the symbol's open paper position was filled
        (the diagram's pos.fees_entry_usd), or None if no open paper position. The Exit
        Controller (LATER) reads this to compute net P&L on close."""
        return self._fees_entry.get(symbol)

    # --- write surface (WS_Manager only - rule:HR-WM-032) ------------------------
    def entry_fill_debit(
        self,
        symbol: str,
        qty: object,
        entry_fill_price: object,
        *,
        writer: str,
    ) -> LedgerUpdate:
        """ENTRY-FILL DEBIT (sec 12.4). On a simulated entry fill at FEE_TAKER_PCT (the
        marketable-IOC entry fills at taker):
            entry_proceeds = qty * entry_fill_price
            fees_entry     = entry_proceeds * FEE_TAKER_PCT
            spot_usd_balance -= (entry_proceeds + fees_entry)
        Retains fees_entry for the symbol (pos.fees_entry_usd, required for net P&L on
        close) and emits PAPER_LEDGER_UPDATED (event_type=ENTRY_FILL)."""
        self._guard_writer(writer, "entry_fill_debit")
        q = _dec(qty)
        price = _dec(entry_fill_price)
        entry_proceeds = q * price
        fees_entry = entry_proceeds * _FEE_TAKER
        prior = self._balance
        delta = -(entry_proceeds + fees_entry)
        self._balance = prior + delta
        self._fees_entry[symbol] = fees_entry
        self._emit(
            PaperLedgerUpdated(
                event_type=LedgerEventType.ENTRY_FILL,
                new_balance=self._balance,
                prior_balance=prior,
                delta_usd=delta,
                fee_usd=fees_entry,
                symbol=symbol,
                fill_price=price,
                qty=q,
            )
        )
        return LedgerUpdate(LedgerEventType.ENTRY_FILL, self._balance, delta, fees_entry)

    def exit_fill_credit(
        self,
        symbol: str,
        qty: object,
        exit_price: object,
        *,
        writer: str,
        exit_reason: str | None = None,
        retain_fees_entry: bool = False,
    ) -> LedgerUpdate:
        """EXIT-FILL CREDIT (sec 12.4). On a simulated exit fill at FEE_TAKER_PCT (all
        exits close at taker - L1a run-to-reversal, L2 MAE, L3 emergSL):
            exit_proceeds = qty * exit_price
            fees_exit     = exit_proceeds * FEE_TAKER_PCT
            spot_usd_balance += (exit_proceeds - fees_exit)
        Emits PAPER_LEDGER_UPDATED (event_type=EXIT_FILL). The symbol's retained entry
        fee (pos.fees_entry_usd) is cleared here by default (the standalone exit fill);
        BUT when this credit is step 2 of the sec-12.5 Exit-Controller close sequence,
        retain_fees_entry=True keeps it so on_paper_close (step 5) can read it for the
        TRADE_CLOSE net P&L - the close path then clears it via clear_fees_entry (step 7)."""
        self._guard_writer(writer, "exit_fill_credit")
        q = _dec(qty)
        price = _dec(exit_price)
        exit_proceeds = q * price
        fees_exit = exit_proceeds * _FEE_TAKER
        prior = self._balance
        delta = exit_proceeds - fees_exit
        self._balance = prior + delta
        if not retain_fees_entry:
            self._fees_entry.pop(symbol, None)
        self._emit(
            PaperLedgerUpdated(
                event_type=LedgerEventType.EXIT_FILL,
                new_balance=self._balance,
                prior_balance=prior,
                delta_usd=delta,
                fee_usd=fees_exit,
                symbol=symbol,
                fill_price=price,
                qty=q,
                exit_reason=exit_reason,
            )
        )
        return LedgerUpdate(LedgerEventType.EXIT_FILL, self._balance, delta, fees_exit)

    def clear_fees_entry(self, symbol: str, *, writer: str) -> None:
        """Drop the symbol's retained entry fee (pos.fees_entry_usd) after the sec-12.5
        close has consumed it for the TRADE_CLOSE net P&L (step 7, alongside the Position
        Mirror clear). Sole-writer guarded (rule:HR-WM-032). Idempotent - a no-op if the
        symbol carries no retained fee."""
        self._guard_writer(writer, "clear_fees_entry")
        self._fees_entry.pop(symbol, None)
