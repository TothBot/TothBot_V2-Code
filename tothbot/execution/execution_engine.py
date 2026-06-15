"""mod:Execution_Engine - the connector from a gate:G8-accepted candidate to a live order.

Source: 0500000 dv1_250 sec 7 mod:Execution_Engine + the entry order path (entry_dispatch.py)
+ the MPP sizing (execution_sizer.py) + ar:AR-007/AR-008 (emergSL from the ACTUAL fill price,
UT-EE-007). This is the final tie in the entry pipeline: it takes the gate:G8_Position_Sizer
ACCEPTED order (the G8Sized) plus the regime-sized USD (G6) and the current bbo, sizes the
marketable-IOC order, derives the off-book emergSL from the ACTUAL fill, and dispatches the
whole entry through mod:WS_Manager (which routes it to THIS side's wallet and the shared seam).

  1. compute_entry_order (ar:AR-069 MPP cap)            -> order_qty + entry_limit_price
  2. emergSL from the ACTUAL fill (UT-EE-007, ar:AR-008): the absolute crash-brake distance is
     ATR(14)*emergency_sl_mult = G8's emergsl_dist * its entry estimate; applied to the actual
     fill (paper fills at the limit) so the L3 stop tracks the real entry, not the estimate.
     LONG: fill - dist (below); SHORT: fill + dist (above, buy-to-cover).
  3. wm.dispatch_entry: the entry add_order + on-fill emergSL batch_add through the seam, into
     the side's wallet, with the entry-time D6 snapshot (Pending Order Registry).

Async (dispatch traverses the async seam). Returns True if the entry filled (a position opened).
Decimal-only downstream (ar:AR-047).
"""

from __future__ import annotations

from ..exchange.position_mirror import PositionSide
from .execution_sizer import compute_entry_order


async def execute_entry(
    wm,
    side: PositionSide,
    symbol: str,
    sized,                       # the gate:G8_Position_Sizer G8Sized (the accepted order)
    *,
    sized_usd: object,          # the regime-sized order USD (gate:G6_Regime_Sizer)
    best_bid: object,
    best_ask: object,
    mpp_abs_cap_pct: object,
    atr_14_entry: object | None = None,
    regime_at_entry: str | None = None,
    cl_ord_id: str,
    deadline: str,
) -> bool:
    """Execute a G8-accepted entry: size the marketable-IOC order (MPP cap), derive the emergSL
    from the actual fill, and dispatch through mod:WS_Manager into THIS side's wallet. `sized` is
    the G8Sized (carries expected_reward / net_loss / emergsl_dist / entry_fill_price). Returns
    True if the entry filled. PURE composition over compute_entry_order + wm.dispatch_entry."""
    sizing = compute_entry_order(
        side,
        sized_usd=sized_usd,
        best_bid=best_bid,
        best_ask=best_ask,
        expected_reward=sized.expected_reward,
        net_loss=sized.net_loss,
        mpp_abs_cap_pct=mpp_abs_cap_pct,
    )
    # UT-EE-007 / ar:AR-008: the emergSL is computed from the ACTUAL fill, not the G8 estimate.
    # The absolute crash-brake distance is ATR(14)*emergency_sl_mult = emergsl_dist (a fraction
    # of the G8 estimate) * that estimate; apply it to the actual fill (paper fills at the limit).
    fill = sizing.entry_limit_price
    abs_emergsl_dist = sized.emergsl_dist * sized.entry_fill_price
    emergsl_price = (
        fill + abs_emergsl_dist if side is PositionSide.SHORT else fill - abs_emergsl_dist
    )
    return await wm.dispatch_entry(
        side,
        symbol,
        order_qty=sizing.order_qty,
        entry_limit_price=fill,
        emergsl_price=emergsl_price,
        atr_14_entry=atr_14_entry,
        regime_at_entry=regime_at_entry,
        cl_ord_id=cl_ord_id,
        deadline=deadline,
    )
