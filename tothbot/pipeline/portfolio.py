"""ar:AR-052 current_portfolio - the per-side MARK-TO-MARKET drawdown numerator (gate:G7 CHECK 1).

Source: 0500000 dv1_253 ar:AR-052 (Portfolio drawdown formula + circuit breakers) + ar:AR-048
(bid for a long mark, ask for a short mark - the realizable exit price) + ar:AR-009 (short = Kraken
margin) + ar:AR-051 (SIZING stays realized cash; drawdown is a DISTINCT computation) + the registry
short-margin borrow seeds (param:margin_open_fee_pct / param:margin_rollover_fee_pct, Bill ruling
TB00728 DEC-A).

THE LONG/SHORT ASYMMETRY (the resolved TB00766 ruling - the SHORT drawdown basis is NOT a mirror of
the long's cash). drawdown_pct = max(0, (baseline - current_portfolio) / baseline), per-side:

  LONG  current_portfolio = spot_cash + sum(bid * qty)  over open longs
        (the long spot cash was DEBITED at entry, so adding back the current market value at the BID
        - the realizable long exit, ar:AR-048 - gives total equity; baseline = spot cash captured once).

  SHORT current_portfolio = margin_collateral + sum((avg_entry - ask) * qty) - sum(rollover_accrued)
        (the Kraken MARGIN-account EQUITY = TradeBalance `e`, reconstructed from WS state between the
        periodic TradeBalance reconciles: the margin collateral cash + the open-short UNREALIZED P&L
        marked at the ASK - the realizable buy-to-cover, ar:AR-048 - minus the borrow ROLLOVER accrued
        but not yet charged to cash; baseline = the margin equity captured once. The at-OPEN margin fee
        is already in the cash at entry, so only the per-4h rollover is subtracted here).

The SHORT mark is the UNREALIZED P&L ((entry - ask) * qty), NOT the full market value: this is exactly
why the margin CASH balance is a structural FALSE NEGATIVE for the breaker (it is blind to the open-
short MTM - the breaker would sleep while a short bleeds) and only the EQUITY basis fires. The LONG mark
is the full market value (bid * qty) because the long cash already paid out the entry notional.

PURE save the injected now_utc clock (the short rollover accrual needs the hold so far). Decimal-only
(ar:AR-047). The periodic TradeBalance `e` reconcile that re-seeds the reconstructed short equity to bound
the rollover-accrual estimate is a follow-on wiring (it needs the REST poll scheduler); the reconstruction
here is conservative (an un-reconciled rollover OVER-subtracts, firing the breaker SOONER - FN-safe).
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal

from ..config import registry
from ..exchange.position_mirror import PositionSide

# The Kraken spot-margin ROLLOVER seed (param:margin_rollover_fee_pct, 0.02%/4h; Bill TB00728 DEC-A).
# An EXISTING CIATS seed (NOT a new seed) - the same borrow fee the SHORT net_loss / R:R already carries
# (position_sizer.py). Taken as Decimal once (ar:AR-047).
_ROLLOVER_FEE = Decimal(str(registry.value("margin_rollover_fee_pct")))

# 4 hours in seconds - the Kraken margin rollover-charge period (the "per 4h" in margin_rollover_fee_pct).
_ROLLOVER_PERIOD_SECONDS = Decimal(4 * 3600)

UtcClock = Callable[[], datetime]


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def held_4h_blocks(entry_timestamp_utc: object, now: datetime) -> int:
    """The number of COMPLETED 4h rollover periods an open short has been held (floor((now - entry)/4h),
    >= 0). entry_timestamp_utc is the open position's entry-trigger ISO stamp (contract:TRADE_CLOSE field
    8); a missing/unparseable stamp yields 0 (the position simply accrues no rollover until its stamp is
    known - conservative-neutral, never a crash). At admission the hold is 0 blocks (only the at-OPEN fee,
    already in cash); the per-4h rollover accrues from the first completed 4h period."""
    if entry_timestamp_utc is None:
        return 0
    try:
        entry = datetime.fromisoformat(str(entry_timestamp_utc))
    except ValueError:
        return 0
    if entry.tzinfo is None:
        entry = entry.replace(tzinfo=timezone.utc)
    elapsed = Decimal(str((now - entry).total_seconds()))
    if elapsed <= 0:
        return 0
    return int(elapsed // _ROLLOVER_PERIOD_SECONDS)


def short_rollover_accrued_usd(
    notional: object, blocks: int, *, rollover_fee_pct: object = _ROLLOVER_FEE
) -> Decimal:
    """The borrow ROLLOVER accrued on ONE open short over its hold so far = notional * rollover_fee_pct
    * blocks (param:margin_rollover_fee_pct, the 0.02%/4h EXISTING CIATS seed). The accrual the running
    margin equity carries and the true TradeBalance `e` reflects (ar:AR-009); subtracted from the
    reconstructed short equity. notional = qty * avg_entry_price (the value borrowed at open). PURE."""
    return _dec(notional) * _dec(rollover_fee_pct) * Decimal(int(blocks))


def current_portfolio_usd(
    wm,
    side: PositionSide,
    *,
    bbo: Callable[[str], "tuple[object, object]"],
    now_utc: UtcClock = _utc_now,
    rollover_fee_pct: object = _ROLLOVER_FEE,
) -> Decimal | None:
    """THIS side's ar:AR-052 current_portfolio - the MARK-TO-MARKET drawdown numerator for gate:G7
    CHECK 1 (DISTINCT from the realized-cash SIZING read wallet_balance(side) that feeds CHECK 2/3 + G8).

    wm exposes wallet_balance(side) (the realized cash: long spot USD / short margin collateral, ar:AR-051)
    and open_positions() (the same-side open positions marked here). bbo(symbol) -> (best_bid, best_ask)
    is the per-symbol realizable quote (the live ticker bbo / the injected provider); it MAY raise to signal
    a missing quote for an open-position pair, which the caller treats as a not-ready tick (skip), never a
    crash. Returns None when wallet_balance(side) is None (the wallet not yet fed - the sweep skips the side).

    LONG  = spot_cash + sum(bid * qty);  SHORT = margin_collateral + sum((avg_entry - ask) * qty) - rollover.
    """
    cash = wm.wallet_balance(side)
    if cash is None:
        return None
    total = _dec(cash)
    now = now_utc()
    for pos in wm.open_positions().values():
        if pos.side is not side:
            continue
        best_bid, best_ask = bbo(pos.symbol)
        qty = _dec(pos.qty)
        if side is PositionSide.LONG:
            # ar:AR-048 long mark = the realizable BID (best price an open long can be sold at NOW).
            total += _dec(best_bid) * qty
        else:
            # ar:AR-048 short mark = the realizable ASK (best buy-to-cover price); the open-short
            # UNREALIZED P&L is (avg_entry - ask) * qty (positive when the short is winning).
            entry = _dec(pos.avg_entry_price)
            total += (entry - _dec(best_ask)) * qty
            # The borrow rollover accrued but not yet charged to the margin cash (TradeBalance `e`
            # carries it); subtract it so the reconstructed equity matches the true margin equity.
            blocks = held_4h_blocks(getattr(pos, "entry_timestamp_utc", None), now)
            total -= short_rollover_accrued_usd(
                entry * qty, blocks, rollover_fee_pct=rollover_fee_pct
            )
    return total
