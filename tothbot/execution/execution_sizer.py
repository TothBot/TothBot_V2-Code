"""The Execution-Engine order sizing - order_qty + the MPP-capped marketable limit price.

Source: 0500000 dv1_250 sec 7 (add_order: marketable IOC limit + MPP slippage cap) + ar:AR-069
(the maximum-price-penetration cap) + the param:mpp_abs_cap_pct recipe (DEC-128) + sec 3 G6
(per_trade_size_usd -> the order's USD size). This is the boundary between a gate-accepted
candidate and the wire order built by execution/entry_dispatch.py.

Given the regime-sized order value (G6 sized_usd) and the current bbo, it computes:

  mpp_cap_pct      = min(mpp_abs_cap_pct, rr_headroom)   clamped at 0      (ar:AR-069)
                     where rr_headroom = expected_reward - 1.5 * net_loss  - the sacred-floor
                     slack at the EXECUTION boundary (slippage must never eat the 1:1.5 R:R
                     floor; not tunable). A G8-accepted candidate has expected_reward >=
                     1.5*net_loss, so rr_headroom >= 0.
  entry_limit_price= the marketable bound capped by mpp_cap_pct, direction-symmetric: a LONG
                     buy crosses UP to best_ask*(1 + cap); a SHORT sell crosses DOWN to
                     best_bid*(1 - cap). The fill is at/inside this bound (paper fills at it -
                     the conservative price, ar:AR-008).
  order_qty        = sized_usd / entry_limit_price - the base-unit quantity whose value at the
                     limit equals the regime-sized USD order. (For a SHORT the committed
                     collateral is order notional / leverage_cap_short - bounded UPSTREAM at
                     Gate-7's exposure check, ar:AR-053; the order qty/notional is the same
                     base-unit sizing both sides.)

PURE compute (Decimal-only, ar:AR-047). Returns the two numbers entry_dispatch.build_entry_order
consumes; the caller dispatches via WSManager.dispatch_entry.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from ..exchange.position_mirror import PositionSide

_FLOOR = Decimal("1.5")   # the sacred R:R floor (rule:Sacred_R_R_1_to_1_5) - NOT tunable here
_ZERO = Decimal("0")
_ONE = Decimal("1")


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the sizer (AR-047)."""
    return Decimal(str(value))


@dataclass(frozen=True)
class EntryOrderSizing:
    """The execution-sized order: the MPP-capped marketable limit + the base-unit qty + the
    mpp_cap_pct actually applied (for the evt:ENTRY_SUBMITTED log)."""

    side: PositionSide
    order_qty: Decimal
    entry_limit_price: Decimal
    mpp_cap_pct: Decimal
    code: str = field(default="ENTRY_ORDER_SIZED", init=False)


def compute_entry_order(
    side: PositionSide,
    *,
    sized_usd: object,
    best_bid: object,
    best_ask: object,
    expected_reward: object,
    net_loss: object,
    mpp_abs_cap_pct: object,
) -> EntryOrderSizing:
    """Size the marketable-IOC entry order (ar:AR-069 MPP cap + the G6 sized USD). LONG crosses
    up to best_ask*(1+cap); SHORT crosses down to best_bid*(1-cap); order_qty = sized_usd /
    limit. PURE. Inputs from G6 (sized_usd), the bbo, and G8 (expected_reward / net_loss)."""
    is_short = side is PositionSide.SHORT
    exp_reward = _dec(expected_reward)
    nloss = _dec(net_loss)
    abs_cap = _dec(mpp_abs_cap_pct)

    # ar:AR-069: cap the slippage by the smaller of the empirical cap and the sacred-floor slack;
    # clamp at 0 (a candidate exactly at the floor admits no slippage).
    rr_headroom = exp_reward - _FLOOR * nloss
    cap = min(abs_cap, rr_headroom)
    if cap < _ZERO:
        cap = _ZERO

    # The MPP-capped marketable bound (direction-symmetric).
    if is_short:
        limit = _dec(best_bid) * (_ONE - cap)
    else:
        limit = _dec(best_ask) * (_ONE + cap)

    order_qty = _dec(sized_usd) / limit
    return EntryOrderSizing(
        side=side, order_qty=order_qty, entry_limit_price=limit, mpp_cap_pct=cap,
    )
