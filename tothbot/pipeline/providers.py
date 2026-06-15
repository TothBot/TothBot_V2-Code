"""Build a real LiveProviders from the runtime caches - the live-layer plug-in wiring.

Source: 0500000 dv1_250 - the sweep's LiveProviders (pipeline/sweep.py) is the seam between the
8-gate pipeline and the live data layer. This module backs the cache-fed callables with the real
runtime caches: instrument_status/marginable from the InstrumentCache (A-17), vol_24h_usd from the
LiquidityCache (the D1 liquidity_24h REST probe), best_bid/best_ask from the BboCache (ar:AR-048),
and base_per_trade_size from the CR-06 formula over the pair's cached cost_min/qty_min. The two
CIATS-OWNED SEED estimators (expected_reward DEC-124, mpp_abs_cap_pct DEC-128) are still INJECTED
(the historical-OHLC seed units land separately); ws_state is injected (the subscription lifecycle).

A cache miss for a symbol raises ProviderNotReady -> the sweep skips that (pair, side) tick (the
instrument/ticker/liquidity snapshot simply has not arrived yet; a READY pair will have them after
the AR-049 startup sequence). cl_ord_id = a fresh uuid; deadline = now + 5s ISO8601 (ar:AR-069 /
A-2). PURE wiring - no I/O of its own; the clock is injected for the deadline.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from decimal import Decimal

from ..exchange.instrument_cache import base_per_trade_size_usd
from ..exchange.position_mirror import PositionSide
from ..regime.taxonomy import Regime
from .sweep import LiveProviders, ProviderNotReady

# A-2 / ar:AR-069: the marketable-IOC entry carries a short order deadline (now + 5s).
DEFAULT_DEADLINE_OFFSET_SEC = 5.0
UtcClock = Callable[[], datetime]


def new_cl_ord_id() -> str:
    """A fresh client order id (A-2 cl_ord_id - an arbitrary unique string). uuid4 hex."""
    return uuid.uuid4().hex


def make_mpp_provider(mpp_store) -> Callable[[str, PositionSide], Decimal]:
    """The mpp_abs_cap_pct provider backed by the DEC-128 MppCapStore (the historical Q95 seed
    per pair/side). Raises ProviderNotReady if the pair/side has no seed yet (the universe-load
    historical probe has not populated it) so the sweep skips that candidate."""

    def mpp_abs_cap_pct(symbol: str, side: PositionSide) -> Decimal:
        value = mpp_store.get(symbol, side)
        if value is None:
            raise ProviderNotReady(symbol, "mpp_abs_cap_pct")
        return value

    return mpp_abs_cap_pct


def make_deadline(
    now_utc: UtcClock, *, offset_sec: float = DEFAULT_DEADLINE_OFFSET_SEC
) -> Callable[[], str]:
    """A deadline generator: now + offset as an ISO8601 'Z' string (A-2 deadline = now+5s)."""

    def _deadline() -> str:
        when = now_utc() + timedelta(seconds=offset_sec)
        return when.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    return _deadline


def make_live_providers(
    *,
    instrument_cache,
    bbo_cache,
    liquidity_cache,
    expected_reward: Callable[[str, Regime], object],
    mpp_abs_cap_pct: Callable[[str, PositionSide], object],
    ws_state: Callable[[str], str],
    now_utc: UtcClock | None = None,
    deadline_offset_sec: float = DEFAULT_DEADLINE_OFFSET_SEC,
    semaphore_locked: Callable[[PositionSide], bool] | None = None,
) -> LiveProviders:
    """Assemble a LiveProviders backed by the runtime caches. The instrument/bbo/liquidity/base
    callables READ the caches (raising ProviderNotReady on a cache miss so the sweep skips that
    tick); expected_reward + mpp_abs_cap_pct + ws_state are injected (the CIATS seeds + the
    subscription lifecycle). now_utc defaults to the wall clock for the deadline."""
    clock: UtcClock = now_utc or (lambda: datetime.now(timezone.utc))

    def instrument(symbol: str) -> "tuple[str, bool, object]":
        info = instrument_cache.get(symbol)
        if info is None:
            raise ProviderNotReady(symbol, "instrument")
        vol_24h = liquidity_cache.get(symbol)
        if vol_24h is None:
            raise ProviderNotReady(symbol, "liquidity")
        return info.status, info.marginable, vol_24h

    def bbo(symbol: str) -> "tuple[object, object]":
        quote = bbo_cache.bbo(symbol)
        if quote is None:
            raise ProviderNotReady(symbol, "bbo")
        return quote

    def base_per_trade_size(symbol: str, side: PositionSide, entry_ref_price: object) -> Decimal:
        info = instrument_cache.get(symbol)
        if info is None:
            raise ProviderNotReady(symbol, "instrument")
        return base_per_trade_size_usd(info.cost_min, info.qty_min, entry_ref_price)

    return LiveProviders(
        instrument=instrument,
        bbo=bbo,
        expected_reward=expected_reward,
        mpp_abs_cap_pct=mpp_abs_cap_pct,
        base_per_trade_size=base_per_trade_size,
        ws_state=ws_state,
        new_cl_ord_id=new_cl_ord_id,
        new_deadline=make_deadline(clock, offset_sec=deadline_offset_sec),
        semaphore_locked=semaphore_locked or (lambda _side: False),
    )
