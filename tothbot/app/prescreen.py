"""Phase-2 bulk pre-screen - trim the ar:AR-070 derived universe to the top-N most liquid pairs.

Source: 0500000 ar:AR-070 (universe) + ar:AR-036 (the per-IP REST budget, now enforced globally by
rest/rate_limiter) + the TB00771 universe-expansion FP/DP determination (over-admission CAUSES a
systemic false-negative via REST flooding -> an interior optimum N; the heavy-tailed liquidity
distribution gives a sharp cutoff). THE DEFECT THIS AVOIDS: subscribing + warming the full 735-pair
AR-070 set firehoses the data layer + the warm-up REST budget; a sane top-N keeps the paper run (and
later the live run) inside the VPS + per-IP budget while still covering the liquid, tradeable pairs.

THE DESIGN: ONE bulk Kraken Ticker call (rest/client.get_all_ticker_liquidity, O(1) REST) ranks every
pair by 24h USD volume; ONE AssetPairs call (get_asset_pairs) maps the REST pair key -> the WS v2 symbol
('XXBTZUSD' -> 'BTC/USD') so the bulk liquidity joins onto the derived set (which speaks WS v2 symbols).
We then take the top-N by liquidity, ALWAYS keeping the ar:AR-074 anchor(s) (BTC/USD) regardless of rank,
and feed that smaller set into the data-layer assembly (screen AFTER derive, BEFORE the subscribes/warm).

This is the PHASE-2 "temporary data-gathering" cut (the operator pins N via TOTHBOT_TOP_N). PHASE 3 makes
N a CIATS-OWNED viability seed (a forward historical re-sim picks N by captured expectancy s.t. capacity);
this module's select_top_n stays the mechanism - CIATS just supplies the N + (later) a richer score.

PURE save the injected rest_client edge: select_top_n + liquidity_by_symbol are pure (driven directly in
tests); screen_universe makes exactly two governed REST calls (both await the global RestRateLimiter)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal

from .universe import DEFAULT_ALWAYS_INCLUDE


def liquidity_by_symbol(
    liquidity_by_rest_key: Mapping[str, Decimal],
    rest_key_to_wsname: Mapping[str, str],
) -> dict[str, Decimal]:
    """Join the REST-keyed bulk-ticker liquidity onto WS v2 symbols via the AssetPairs map -> {wsname:
    vol_24h_usd}. A REST key with no wsname mapping (a pair Kraken does not stream over WS v2) is dropped
    - it can never be in the WS universe. PURE."""
    out: dict[str, Decimal] = {}
    for rest_key, vol in liquidity_by_rest_key.items():
        wsname = rest_key_to_wsname.get(rest_key)
        if wsname is not None:
            out[wsname] = vol
    return out


def select_top_n(
    derived: Sequence[str],
    liquidity: Mapping[str, Decimal],
    *,
    top_n: int,
    always_include: Sequence[str] = DEFAULT_ALWAYS_INCLUDE,
) -> tuple[str, ...]:
    """Trim the derived AR-070 universe to the top-N pairs by 24h USD liquidity, ALWAYS keeping the
    always_include anchor(s) (ar:AR-074 BTC/USD) no matter their rank or whether they have a liquidity
    reading. A derived pair with no liquidity reading sorts last (score 0) - it is admittable only if it
    falls inside top_n after the ranked ones. top_n <= 0 (or >= len(derived)) is a no-op pass-through (the
    full derived set, anchors unioned). The result is SORTED + de-duplicated so the downstream ShardPlan
    partition is deterministic. PURE."""
    derived_set = set(derived)
    anchors = {a for a in always_include if a in derived_set}
    if top_n <= 0 or top_n >= len(derived_set):
        return tuple(sorted(derived_set | set(always_include)))
    # Rank the derived pairs by liquidity desc; a missing reading scores 0. Tie-break by symbol so the
    # selection is deterministic across runs (no Date.now / set-iteration nondeterminism).
    ranked = sorted(
        derived_set,
        key=lambda s: (liquidity.get(s, Decimal(0)), s),
        reverse=True,
    )
    selected = set(ranked[:top_n]) | anchors
    return tuple(sorted(selected))


async def screen_universe(
    rest_client: object,
    derived: Sequence[str],
    *,
    top_n: int,
    always_include: Sequence[str] = DEFAULT_ALWAYS_INCLUDE,
) -> tuple[str, ...]:
    """The phase-2 bulk pre-screen orchestrator: two governed REST calls (bulk Ticker + AssetPairs) ->
    rank the derived AR-070 set by 24h USD liquidity -> top-N (anchors always kept). top_n <= 0 short-
    circuits to the full derived set WITHOUT any REST call (the disabled / no-screen mode). rest_client
    must expose get_all_ticker_liquidity() + get_asset_pairs() (the KrakenRestClient does); both calls go
    through the global RestRateLimiter so the screen itself honors the per-IP budget."""
    if top_n <= 0 or top_n >= len(set(derived)):
        return tuple(sorted(set(derived) | set(always_include)))
    liquidity_by_rest_key = await rest_client.get_all_ticker_liquidity()  # type: ignore[attr-defined]
    rest_key_to_wsname = await rest_client.get_asset_pairs()              # type: ignore[attr-defined]
    by_symbol = liquidity_by_symbol(liquidity_by_rest_key, rest_key_to_wsname)
    return select_top_n(derived, by_symbol, top_n=top_n, always_include=always_include)
