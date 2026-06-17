"""Phase-3 viability screen - the async I/O orchestrator for the CIATS-owned viability cut.

Source: TB00772 phase 3 (NSI). The PURE re-sim + selection math lives in ciats/viability.py; THIS module
is the startup edge that drives it: for each phase-2 liquidity-screened candidate, fetch its historical
DAILY OHLC series (GetOHLCData interval=1440) under the GLOBAL REST governor (so a bounded candidate set
cannot flood the per-IP budget, ar:AR-036), seed the ViabilityStore from the forward re-sim, then let
CIATS pick the monitored cardinality N (ViabilityStore.select - greedy-optimal under the capacity budget).

THE TWO-STAGE CUT (the FP/DP-determined path): 735 pairs --[phase-2 bulk Ticker, ONE call]--> top-N_liq
candidates --[phase-3 per-candidate daily re-sim, governed]--> the monitored top-N by expectancy. Phase 2
makes the daily-bar fetch here BOUNDED (we only re-sim the already-liquid candidates, never all 735).

A candidate whose daily fetch fails / is too short to classify is simply not seeded (get() -> None ->
dropped as non-viable) - FN-safe degrade, never a crash. The ar:AR-074 anchor is always monitored.

PURE save the injected rest_client edge (get_ohlc_data). Driven in tests with a fake REST over crafted
daily series; no network, no timers."""

from __future__ import annotations

from collections.abc import Sequence

from ..ciats.viability import ViabilityStore
from .universe import DEFAULT_ALWAYS_INCLUDE

# The daily-regime interval (minutes) - ar:AR-074 / AR-044 daily series for the run-to-reversal re-sim.
_DAILY_INTERVAL_MIN = 1440


async def screen_viable_universe(
    rest_client: object,
    candidates: Sequence[str],
    *,
    capacity_n: int,
    always_include: Sequence[str] = DEFAULT_ALWAYS_INCLUDE,
    store: ViabilityStore | None = None,
) -> tuple[str, ...]:
    """Seed a ViabilityStore from each candidate's historical daily OHLC (one governed GetOHLCData call
    per candidate) and return the CIATS-picked monitored universe (top-N by viability under capacity_n,
    the anchor always kept). capacity_n <= 0 keeps all viable candidates (no capacity cut). A candidate
    whose fetch raises or whose series is too short to classify is skipped (not viable). rest_client must
    expose async get_ohlc_data(pair, interval) -> an OhlcResponse with .committed bars (the KrakenRest
    client does); every call funnels through the global RestRateLimiter."""
    store = store if store is not None else ViabilityStore()
    for pair in candidates:
        try:
            resp = await rest_client.get_ohlc_data(pair, _DAILY_INTERVAL_MIN)  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001 - a fetch failure -> drop the pair, never crash the cold-start
            continue
        bars = getattr(resp, "committed", ())
        if bars:
            store.seed_from_bars(pair, bars)
    return store.select(capacity_n=capacity_n, always_include=always_include)
