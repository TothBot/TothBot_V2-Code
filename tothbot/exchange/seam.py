"""contract:WSManager_Dispatch_Seam - the outbound order-dispatch mode gate.

Source: 0500000 dv1_240 sec 12 Image7 (Paper-Mode Dispatch Boundary) +
the D2 dispatch-seam coder detail + sec 7 mod:WS_Manager.

This is the SINGLE place paper_trading_mode is read and the SINGLE point at
which paper and live diverge for order dispatch (PA-004 divergence #2). Every
outbound order RPC from every caller (Long_Module, Short_Module,
Execution_Engine, Exit_Controller emergency paths) flows through these six
methods - callers are mode-OPAQUE (rule:HR-EE-015): they never read the mode
flag, never branch on it, and must not inspect the return value to infer it.

  paper (rule:HR-WM-023): route to the local fill simulator; emit
        evt:PAPER_ORDER_SIMULATED; NOTHING is transmitted to Kraken.
  live  : ws_private.send(...); emit evt:ENTRY_SUBMITTED on the entry add_order.

The mode is frozen at construction (rule:HR-WM-021, immutable for the process
lifetime). evt:PAPER_DISPATCH_BLOCKED is a defensive canary (rule:HR-WM-023):
if any code path ever reaches the live transmit branch while in paper mode it
fires CRITICAL and blocks - it must be unreachable under correct flow.

The live transmitter and the paper simulator are INJECTED async I/O bodies
(outbound.py): container:Private_WS_v2 send (live, ws_private.send) and
contract:Synthetic_Capital_Ledger / _simulate_entry_fill (paper). This module owns
only the gate, the branch, the canary, and the events.

The six order methods are ASYNC: the live body awaits the async private
Transport.send and the paper body may schedule a simulation task, so the
rule:HR-EE-013 mode-opaque caller awaits ONE coroutine regardless of mode (the
sec 12.3 caller contract - "Returns after dispatch", EE awaits the seam).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum

from ..config.settings import Mode


class OutboundOp(Enum):
    """The outbound order operations that traverse the dispatch seam.

    All others (subscribe pacing, reconnect) are byte-identical paper <-> live
    and do not pass through this mode gate (PA-005).
    """

    ADD_ORDER = "add_order"                    # marketable IOC entry (WS-ADD-002)
    BATCH_ADD = "batch_add"                     # atomic emergSL leg on fill (WS-BA-002)
    CANCEL_ORDER = "cancel_order"              # exit-leg / MAE cleanup
    AMEND_ORDER = "amend_order"                # sole modification method (AR-018)
    BATCH_CANCEL = "batch_cancel"             # Full-Halt multi-order cancel
    DISPATCH_MARKET_SELL = "dispatch_market_sell"  # L1a/L2 exit market sell


# Injected async I/O edges. Each receives the operation + the already-built message
# and awaits its side effect (live: ws_private.send; paper: local simulation).
LiveSender = Callable[[OutboundOp, dict], Awaitable[None]]
PaperSimulator = Callable[[OutboundOp, dict], Awaitable[None]]
EventSink = Callable[[object], None]


@dataclass(frozen=True)
class EntrySubmitted:
    """evt:ENTRY_SUBMITTED - a live entry add_order was transmitted to Kraken."""

    op: OutboundOp


@dataclass(frozen=True)
class PaperOrderSimulated:
    """evt:PAPER_ORDER_SIMULATED - an order was simulated locally (paper mode)."""

    op: OutboundOp


@dataclass(frozen=True)
class PaperDispatchBlocked:
    """evt:PAPER_DISPATCH_BLOCKED - defensive canary; outbound reached in paper."""

    op: OutboundOp


class PaperDispatchBlockedError(RuntimeError):
    """Raised when a paper-mode dispatch reaches the live transmit branch."""


@dataclass(frozen=True)
class SeamDispatch:
    """Outcome of one seam dispatch. For logging/audit only - callers MUST NOT
    branch on this to infer mode (rule:HR-EE-015 mode-opaque caller contract).
    """

    op: OutboundOp
    mode: Mode
    transmitted: bool  # True = sent to Kraken (live); False = simulated (paper)


class DispatchSeam:
    """The single paper/live order-dispatch gate (contract:WSManager_Dispatch_Seam)."""

    def __init__(
        self,
        mode: Mode,
        *,
        live_sender: LiveSender,
        paper_simulator: PaperSimulator,
        on_event: EventSink | None = None,
    ) -> None:
        self._mode = mode  # frozen for process lifetime (rule:HR-WM-021)
        self._live_sender = live_sender
        self._paper_simulator = paper_simulator
        self._on_event = on_event

    @property
    def is_paper(self) -> bool:
        return self._mode is Mode.PAPER

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    async def _dispatch(self, op: OutboundOp, message: dict) -> SeamDispatch:
        if self._mode is Mode.PAPER:
            await self._paper_simulator(op, message)
            self._emit(PaperOrderSimulated(op))
            return SeamDispatch(op=op, mode=self._mode, transmitted=False)
        return await self._transmit_live(op, message)

    async def _transmit_live(self, op: OutboundOp, message: dict) -> SeamDispatch:
        # Defensive canary (rule:HR-WM-023): the outbound branch must be
        # unreachable in paper mode. If it is ever reached, block and alert.
        if self._mode is Mode.PAPER:
            self._emit(PaperDispatchBlocked(op))
            raise PaperDispatchBlockedError(
                f"paper-mode dispatch reached the live transmit branch: {op.value}"
            )
        await self._live_sender(op, message)
        if op is OutboundOp.ADD_ORDER:
            self._emit(EntrySubmitted(op))
        return SeamDispatch(op=op, mode=self._mode, transmitted=True)

    # --- the six outbound order methods (the sole order-dispatch surface) -----
    async def add_order(self, message: dict) -> SeamDispatch:
        return await self._dispatch(OutboundOp.ADD_ORDER, message)

    async def batch_add(self, message: dict) -> SeamDispatch:
        return await self._dispatch(OutboundOp.BATCH_ADD, message)

    async def cancel_order(self, message: dict) -> SeamDispatch:
        return await self._dispatch(OutboundOp.CANCEL_ORDER, message)

    async def amend_order(self, message: dict) -> SeamDispatch:
        return await self._dispatch(OutboundOp.AMEND_ORDER, message)

    async def batch_cancel(self, message: dict) -> SeamDispatch:
        return await self._dispatch(OutboundOp.BATCH_CANCEL, message)

    async def dispatch_market_sell(self, message: dict) -> SeamDispatch:
        return await self._dispatch(OutboundOp.DISPATCH_MARKET_SELL, message)
