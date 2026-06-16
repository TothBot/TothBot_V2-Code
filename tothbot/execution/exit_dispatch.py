"""The live exit order path - the L1a/L2 cancel-emergSL-then-market-sell close.

Source: 0500000 dv1_253 sec 3 Image3 (layer:L2_MAE_Threshold q1_do: the ordered
sequence "(1) WS cancel_order(emergSL off-book) -> (2) WS add_order — LONG spot
market SELL / SHORT Kraken MARGIN market BUY-to-cover (reduce_only) per ar:AR-009";
layer:L1a_Regime_Exit q1_do: "cancel_order(emergSL) then the WS market close") +
sec 4.1 (SEQUENCE CRITICAL: "(1) cancel emergSL THEN (2) market sell"; the CANCEL
TIMEOUT FALLBACK I-6; the MPP RISK C-1) + ar:AR-009 (SHORT closes via Kraken MARGIN
market BUY-to-cover reduce_only) + the A-2/A-4 wire facts (cl_ord_id + stp_type:
cancel_newest + deadline:now+5s on every message, WS v2 underscore format).

This is the EXIT counterpart to entry_dispatch.py: PURE message construction (no I/O)
for the messages a TothBot-dispatched live exit produces -

  1) build_cancel_order  - the WS cancel_order that pulls the resting off-book emergSL
     (layer:L3) BEFORE the market close. SEQUENCE CRITICAL (rule:HR-EC-013 / HR-EC-014):
     the emergSL cancel MUST confirm before the market sell, so a close fill never races
     a still-resting stop into a double exit.
  2) build_market_sell_order - the layer:L1a / L2 market close. LONG is a spot market
     SELL; SHORT is a Kraken MARGIN market BUY-to-cover (reduce_only=true, ar:AR-009).
     Whole-position qty, order_type=market (no limit_price).
  3) build_mpp_retry_order - the C-1 MPP-rejection retry. A market close may reject on a
     wide spread (Kraken Max-Price-Protection); the retry is a marketable IOC LIMIT at
     best_bid - 0.2%*n (LONG sell) / best_ask + 0.2%*n (SHORT buy-to-cover), the n-th of
     up to param:mpp_retry_count attempts. The 0.2% increment is the diagram literal
     ("best_bid - 0.2% increments"); mpp_retry_limit_price computes the n-th bound.

PURE: order_qty + the realizable bbo bound are read/computed UPSTREAM and passed in; this
module only assembles the direction-symmetric wire messages. The caller dispatches them
through the seam (seam.cancel_order / seam.dispatch_market_sell). Decimal-safe: every
numeric field is stringified from its Decimal so no float reaches the wire (ar:AR-047).
"""

from __future__ import annotations

from decimal import Decimal

from ..exchange.position_mirror import PositionSide

# WS v2 constants (underscore format, A-4). The close is a market order; the C-1 retry is a
# marketable IOC limit.
_STP_CANCEL_NEWEST = "cancel_newest"
_ORDER_TYPE_MARKET = "market"
_ORDER_TYPE_LIMIT = "limit"
_TIF_IOC = "ioc"

# The C-1 MPP IOC-retry price increment - the diagram literal (layer:L2 / mod:Exit_Controller
# q1_do: "best_bid - 0.2% increments / best_ask + 0.2% increments"). NOT one of the four named
# CIATS params (mae_mult / emergency_sl_mult / cancel_timeout_window / mpp_retry_count); it is
# the per-attempt step the retry walks the marketable limit out by, sourced verbatim from 0500000.
_MPP_RETRY_INCREMENT_PCT = Decimal("0.002")


def _s(value: object) -> str:
    """Stringify a numeric field for the wire - no float ever reaches Kraken (AR-047)."""
    return str(value)


def build_cancel_order(symbol: str, *, cl_ord_id: str, deadline: str) -> dict:
    """Build the WS cancel_order that pulls the resting off-book emergSL (layer:L3) by its
    cl_ord_id - step (1) of the SEQUENCE-CRITICAL L1a/L2 close (sec 4.1). The emergSL cancel
    MUST confirm before the market sell (rule:HR-EC-013 / HR-EC-014); the caller tracks the ACK
    through the I-6 cancel-timeout fallback. The seam transmits this via seam.cancel_order."""
    return {
        "method": "cancel_order",
        "params": {
            "symbol": symbol,
            "cl_ord_id": cl_ord_id,     # the ON-FILL batch_add emergSL leg (AR-054)
            "deadline": deadline,        # now+5s (A-2)
        },
    }


def build_market_sell_order(
    symbol: str,
    side: PositionSide,
    *,
    order_qty: object,
    cl_ord_id: str,
    deadline: str,
) -> dict:
    """Build the layer:L1a / L2 market close - step (2) of the cancel-then-sell sequence. The
    side is OPPOSITE the position: a LONG closes via a spot market SELL; a SHORT closes via a
    Kraken MARGIN market BUY-to-cover (margin=true, reduce_only=true so it can only CLOSE the
    margin short, never flip long - ar:AR-009). Whole-position order_qty, order_type=market (no
    limit_price - the take-profit is run-to-reversal, never a resting target). The seam transmits
    this via seam.dispatch_market_sell."""
    is_short = side is PositionSide.SHORT
    params: dict[str, object] = {
        "symbol": symbol,
        # LONG sells to close; SHORT buys to cover.
        "side": "buy" if is_short else "sell",
        "order_type": _ORDER_TYPE_MARKET,       # a market close (L1a/L2), not a resting order
        "order_qty": _s(order_qty),
        "cl_ord_id": cl_ord_id,                 # A-2
        "stp_type": _STP_CANCEL_NEWEST,         # A-4
        "deadline": deadline,                   # now+5s (A-2)
    }
    if is_short:
        # ar:AR-009: the SHORT buy-to-cover trades Kraken margin and is reduce_only - it CLOSES
        # the margin short only. A spot LONG sell carries NO margin / reduce_only (spot has no
        # position to "reduce"; the flags are invalid on a spot order).
        params["margin"] = True
        params["reduce_only"] = True
    return {"method": "add_order", "params": params}


def mpp_retry_limit_price(side: PositionSide, best_quote: object, attempt: int) -> Decimal:
    """The marketable-limit bound for the n-th C-1 MPP retry (sec 4.1 / mod:Exit_Controller
    q1_do). attempt is 1-based: a LONG sell walks DOWN from best_bid by 0.2%*attempt (accepts
    more slippage to clear); a SHORT buy-to-cover walks UP from best_ask by 0.2%*attempt. The
    increment is the diagram literal _MPP_RETRY_INCREMENT_PCT. PURE Decimal (ar:AR-047)."""
    quote = best_quote if isinstance(best_quote, Decimal) else Decimal(str(best_quote))
    step = _MPP_RETRY_INCREMENT_PCT * Decimal(attempt)
    factor = (Decimal(1) - step) if side is PositionSide.LONG else (Decimal(1) + step)
    return quote * factor


def build_mpp_retry_order(
    symbol: str,
    side: PositionSide,
    *,
    order_qty: object,
    limit_price: object,
    cl_ord_id: str,
    deadline: str,
) -> dict:
    """Build the C-1 MPP-rejection retry (sec 4.1 "EC must handle and retry") - a marketable IOC
    LIMIT replacing a market close that Kraken rejected on a wide spread. Same OPPOSITE side as
    the market close (LONG sells / SHORT buys-to-cover reduce_only, ar:AR-009); limit_price is the
    mpp_retry_limit_price bound for this attempt. IOC fills-or-kills atomically. The seam transmits
    it via seam.dispatch_market_sell (the same exit op)."""
    is_short = side is PositionSide.SHORT
    params: dict[str, object] = {
        "symbol": symbol,
        "side": "buy" if is_short else "sell",
        "order_type": _ORDER_TYPE_LIMIT,        # marketable IOC limit (the C-1 retry), not a rest
        "order_qty": _s(order_qty),
        "limit_price": _s(limit_price),         # best_bid -/+ 0.2%*n (mpp_retry_limit_price)
        "time_in_force": _TIF_IOC,              # fills-or-kills atomically
        "cl_ord_id": cl_ord_id,
        "stp_type": _STP_CANCEL_NEWEST,
        "deadline": deadline,
    }
    if is_short:
        params["margin"] = True
        params["reduce_only"] = True            # ar:AR-009 buy-to-cover closes the margin short only
    return {"method": "add_order", "params": params}


def build_limit_only_exit_order(
    symbol: str,
    side: PositionSide,
    *,
    order_qty: object,
    limit_price: object,
    cl_ord_id: str,
    deadline: str,
) -> dict:
    """Build the ar:AR-040 PAIR_LIMIT_ONLY_EXIT close - the ACTIVE exit when an open-position pair
    transitions to limit_only (the instrument-status channel reports the pair accepts limit orders
    ONLY). A SINGLE marketable IOC LIMIT (mod:Exit_Controller q4_triggers: "NOT a market order"):
    LONG sells at best_bid, SHORT buys-to-cover at best_ask (reduce_only, ar:AR-009). The emergSL is
    cancelled FIRST by the same sequence-critical cancel (rule:HR-EC-013 - never an active close with
    a resting stop), then this single IOC limit closes the position; exit_reason PAIR_LIMIT_ONLY_EXIT.
    Same wire shape as the C-1 retry but priced AT the bbo (no walk-out) and a one-shot (no retry)."""
    is_short = side is PositionSide.SHORT
    params: dict[str, object] = {
        "symbol": symbol,
        "side": "buy" if is_short else "sell",   # LONG sells at best_bid; SHORT buys-to-cover at best_ask
        "order_type": _ORDER_TYPE_LIMIT,         # a SINGLE IOC limit (limit_only accepts limit orders only)
        "order_qty": _s(order_qty),
        "limit_price": _s(limit_price),          # best_bid for a long, best_ask for a short
        "time_in_force": _TIF_IOC,
        "cl_ord_id": cl_ord_id,
        "stp_type": _STP_CANCEL_NEWEST,
        "deadline": deadline,
    }
    if is_short:
        params["margin"] = True
        params["reduce_only"] = True
    return {"method": "add_order", "params": params}
