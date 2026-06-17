"""ar:AR-052 / REST-BAL-008 - the periodic SHORT-equity TradeBalance reconcile carry-forward (live only).

Source: 0500000 dv1_256 ar:AR-052 (the SHORT current_portfolio = the running margin equity, reconstructed
from WS state between the periodic TradeBalance reconciles) + ar:AR-009 (short = Kraken margin, borrow-
adjusted) + the resolved TB00766 FP/DP ruling (the SHORT drawdown basis is the EQUITY `e`, not the margin
cash). REST-BAL-008 = the Kraken TradeBalance endpoint; `e` = trade balance + unrealized net P&L, the true
borrow-adjusted margin EQUITY.

THE DRIFT IT BOUNDS. The SHORT drawdown numerator (gate:G7 CHECK 1) is current_portfolio_usd's
reconstruction: margin_collateral + sum((avg_entry - ask) * qty) - sum(rollover_accrued). The rollover term
is an ESTIMATE (param:margin_rollover_fee_pct per 4h block); the collateral cash + the MTM marks drift from
Kraken's true ledger. Left un-reconciled the reconstruction is conservative (the estimated rollover OVER-
subtracts -> the breaker fires SOONER -> FN-safe, never a FALSE POSITIVE) but it accumulates error.

THE CARRY-FORWARD. Each poll this fetches the true equity `e` (REST-BAL-008), computes the RAW reconstruction
(current_portfolio_usd with reconcile_offset=0), and re-seeds the carry-forward offset on the WSManager:

    offset = e_true - raw_reconstruction

current_portfolio_usd adds that offset back for the SHORT, so the numerator lands EXACTLY on `e` at the
reconcile instant and tracks MTM between polls (the next poll re-anchors). UNLIKE the once-only startup
baseline (HR-WM-011), the offset is UPDATED every poll - it is the running carry-forward.

LIVE ONLY (paper has no margin account; PA-004 div #1 / HR-WM-022). All I/O is injected (the TradeBalance
fetch edge, the bbo provider, the sleep, the UTC clock) so the whole loop is driven under stdlib asyncio.run
over fakes - no network, no real timers. A poll that cannot compute (the SHORT wallet not yet fed, a missing
open-position quote, or the fetch returning None) is SKIPPED cleanly (the prior offset stands), never a crash.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from ..exchange.position_mirror import PositionSide
from .portfolio import current_portfolio_usd
from .sweep import ProviderNotReady

# The reconcile poll cadence (seconds) - an ENGINEERING constant (an operational poll interval, like the
# keepalive ping; NOT a CIATS trading seed). Hourly: Kraken charges the borrow rollover per 4h, so an hourly
# re-anchor keeps the rollover-estimate drift well under one charge period while costing one private REST
# call/hour. Overridable at construction for tests / tuning.
DEFAULT_RECONCILE_INTERVAL_SEC = 3600.0

# Injected I/O edges.
FetchTradeBalance = Callable[[], Awaitable[object]]   # REST-BAL-008 TradeBalance `e` (the margin equity)
Bbo = Callable[[str], "tuple[object, object]"]        # the realizable (bid, ask); raises on a missing quote
Sleep = Callable[[float], Awaitable[None]]
UtcClock = Callable[[], datetime]
EventSink = Callable[[object], None]


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class ShortEquityReconciled:
    """evt:SHORT_EQUITY_RECONCILED [MEDIUM] - one periodic TradeBalance carry-forward poll (ar:AR-052 /
    REST-BAL-008). `equity` = the true Kraken margin equity `e`; `reconstructed` = the RAW WS-reconstructed
    SHORT current_portfolio at the poll instant (no offset); `offset` = equity - reconstructed, the
    carry-forward re-seeded on the WSManager (positive when the un-reconciled reconstruction under-stated
    the equity, e.g. the estimated rollover over-subtracted). Routed to mod:Logger Stream-1."""

    equity: Decimal
    reconstructed: Decimal
    offset: Decimal
    code: str = field(default="SHORT_EQUITY_RECONCILED", init=False)


class PeriodicTradeBalanceReconcile:
    """The live periodic SHORT-equity reconcile loop (ar:AR-052 / REST-BAL-008). run() polls every
    interval, re-seeding wm.set_short_equity_reconcile_offset to re-anchor the reconstructed SHORT
    current_portfolio to the true TradeBalance `e`. stop() halts the loop. Live only."""

    def __init__(
        self,
        wm,
        *,
        fetch_trade_balance: FetchTradeBalance,
        bbo: Bbo,
        interval_seconds: float = DEFAULT_RECONCILE_INTERVAL_SEC,
        now_utc: UtcClock = _utc_now,
        sleep: Sleep = asyncio.sleep,
        on_event: EventSink | None = None,
    ) -> None:
        self._wm = wm
        self._fetch_trade_balance = fetch_trade_balance
        self._bbo = bbo
        self._interval = interval_seconds
        self._now_utc = now_utc or _utc_now  # tolerate an explicit None (the providers' clock may be unset)
        self._sleep = sleep
        self._on_event = on_event
        self._stopped = False

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    async def reconcile_once(self) -> ShortEquityReconciled | None:
        """One poll: fetch the true margin equity `e`, compute the RAW SHORT reconstruction, re-seed the
        carry-forward offset, emit the telemetry. Returns the event, or None when the poll is SKIPPED (the
        fetch returned None, the SHORT wallet is not yet fed, or an open-position quote is missing) - the
        prior offset stands, never a crash (FN-safe: the un-reconciled reconstruction over-subtracts)."""
        equity = await self._fetch_trade_balance()
        if equity is None:
            return None  # the REST edge is unwired / returned nothing this poll - skip
        try:
            raw = current_portfolio_usd(
                self._wm,
                PositionSide.SHORT,
                bbo=self._bbo,
                now_utc=self._now_utc,
                reconcile_offset=Decimal("0"),  # the RAW reconstruction (no carry-forward)
            )
        except ProviderNotReady:
            return None  # a missing open-position quote - skip this poll (the prior offset stands)
        if raw is None:
            return None  # the SHORT wallet not yet fed (wallet_balance None) - skip
        offset = _dec(equity) - raw
        self._wm.set_short_equity_reconcile_offset(offset)
        event = ShortEquityReconciled(equity=_dec(equity), reconstructed=raw, offset=offset)
        self._emit(event)
        return event

    async def run(self) -> None:
        """Drive the reconcile loop until stop(): sleep one interval, then reconcile_once(). The sleep
        leads the first poll (the AR-049 startup capture already seeded the SHORT baseline + an initial
        run reconstructs cleanly until the first poll re-anchors)."""
        while not self._stopped:
            await self._sleep(self._interval)
            if self._stopped:
                break
            await self.reconcile_once()

    def stop(self) -> None:
        self._stopped = True
