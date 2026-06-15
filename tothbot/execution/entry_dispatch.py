"""The entry order path - direction-symmetric add_order + on-fill emergSL construction.

Source: 0500000 dv1_250 sec 7 Private-WS-v2-outbound (add_order entry: marketable IOC limit +
MPP slippage cap / cl_ord_id / stp_type:cancel_newest / deadline:now+5s; batch_add: emergSL
single leg on fill) + Image1 outbound (L: spot buy / S: margin sell leverage_cap_short;
reduce_only on short cover) + the G5 entry_fill_disposition (ON FILL batch_add emergency-SL,
ar:AR-007/AR-008: long fill batches a SELL emergSL stop BELOW entry, a short fill batches a
BUY-to-cover emergSL stop reduce_only=true ABOVE entry per ar:AR-009) + the A-2/A-4 wire facts
(req_id + cl_ord_id + stp_type:cancel_newest on every message, WS v2 underscore format).

This is the MARGIN ORDER PATH: it turns a gate:G8_Position_Sizer ACCEPTED order into the two
outbound messages a dispatched entry produces -

  1) the ENTRY add_order - a marketable-IOC limit at the MPP-capped marketable bound. LONG is a
     spot buy-to-open; SHORT is a Kraken MARGIN sell-to-open (margin=true, sized via
     leverage_cap_short upstream, ar:AR-009).
  2) the ON-FILL emergSL batch_add - the off-book layer:L3 crash brake placed the instant the
     entry fills. LONG = a SELL stop BELOW entry; SHORT = a BUY-to-cover stop reduce_only=true
     ABOVE entry. Both at the G8 emergsl_price (entry -/+ ATR(14)*emergency_sl_mult). It is
     stop-market (no limit_price) - the take-profit is the run-to-reversal regime exit, never a
     resting order.

PURE message construction (no I/O): order_qty + entry_limit_price (the MPP-capped marketable
bound) + emergsl_price are computed UPSTREAM (the sizer / MPP cap / leverage) and passed in; this
module only assembles the direction-symmetric wire messages. The caller dispatches them through
contract:WSManager_Dispatch_Seam (seam.add_order / seam.batch_add). Decimal-safe: every numeric
field is stringified from its Decimal so no float reaches the wire (ar:AR-047).
"""

from __future__ import annotations

from ..exchange.position_mirror import PositionSide

# WS v2 constants (underscore format, A-4). The entry is a marketable IOC limit; the emergSL is
# an off-book stop-market (a stop-loss order with no limit_price -> triggers a market exit).
_STP_CANCEL_NEWEST = "cancel_newest"
_ORDER_TYPE_LIMIT = "limit"
_ORDER_TYPE_STOP = "stop-loss"
_TIF_IOC = "ioc"


def _s(value: object) -> str:
    """Stringify a numeric field for the wire - no float ever reaches Kraken (AR-047)."""
    return str(value)


def build_entry_order(
    symbol: str,
    side: PositionSide,
    *,
    order_qty: object,
    entry_limit_price: object,
    cl_ord_id: str,
    deadline: str,
) -> dict:
    """Build the ENTRY add_order message (marketable-IOC limit, sec 7 outbound). LONG = spot
    buy-to-open; SHORT = Kraken margin sell-to-open (margin=true, ar:AR-009). entry_limit_price is
    the MPP-capped marketable bound (computed upstream). The seam transmits this via add_order."""
    is_short = side is PositionSide.SHORT
    params: dict[str, object] = {
        "symbol": symbol,
        # LONG buys to open (spot); SHORT sells to open (margin).
        "side": "sell" if is_short else "buy",
        "order_type": _ORDER_TYPE_LIMIT,        # marketable IOC limit (CR-03), not a resting order
        "order_qty": _s(order_qty),
        "limit_price": _s(entry_limit_price),   # the MPP-capped marketable bound
        "time_in_force": _TIF_IOC,              # fills-or-kills atomically (AR-054)
        "cl_ord_id": cl_ord_id,                 # A-2
        "stp_type": _STP_CANCEL_NEWEST,         # A-4
        "deadline": deadline,                   # now+5s (A-2)
    }
    if is_short:
        # SHORT trades Kraken spot-margin (ar:AR-009); the open leg is NOT reduce_only (it OPENS).
        params["margin"] = True
    return {"method": "add_order", "params": params}


def build_emergsl_order(
    symbol: str,
    side: PositionSide,
    *,
    order_qty: object,
    emergsl_price: object,
    cl_ord_id: str,
    deadline: str,
) -> dict:
    """Build the ON-FILL emergSL batch_add message (single leg, sec 7 + ar:AR-007/AR-008). The
    off-book layer:L3 crash brake placed when the entry fills: LONG = a SELL stop BELOW entry;
    SHORT = a BUY-to-cover stop reduce_only=true ABOVE entry (ar:AR-009). Stop-market at
    emergsl_price (the G8 entry -/+ ATR(14)*emergency_sl_mult). The seam transmits via batch_add."""
    is_short = side is PositionSide.SHORT
    leg: dict[str, object] = {
        # The emergSL CLOSES the position, so its side is OPPOSITE the entry: a long (bought to
        # open) sells to stop out; a short (sold to open) BUYS to cover.
        "side": "buy" if is_short else "sell",
        "order_type": _ORDER_TYPE_STOP,         # stop-market (off-book; no limit_price)
        "order_qty": _s(order_qty),
        # The stop trigger: BELOW entry for a long, ABOVE entry for a short (the G8 emergsl_price
        # already carries the correct side of entry). triggers.reference="last" is mandatory on
        # every emergSL (UT-EE-004 / rule:HR-EI) - trigger on the last trade price.
        "triggers": {"reference": "last", "price": _s(emergsl_price)},
        "cl_ord_id": cl_ord_id,
        "stp_type": _STP_CANCEL_NEWEST,         # A-4 / UT-EE-002
        "deadline": deadline,                   # now+5s / UT-EE-003
    }
    if is_short:
        # reduce_only is a Kraken MARGIN flag - mandatory on the SHORT buy-to-cover so it can only
        # CLOSE the margin short (ar:AR-009), never flip long. A spot LONG sell-stop carries NO
        # reduce_only (spot has no position to "reduce"; the flag is invalid on a spot order).
        leg["reduce_only"] = True
    return {"method": "batch_add", "params": {"symbol": symbol, "orders": [leg]}}
