"""ar:AR-070 universe load - derive the monitored pair set from the instrument-channel snapshot.

Source: 0500000 dv1_254 ar:AR-070 ("Universe populated at startup from instrument channel snapshot";
monitored set = Kraken spot pairs with (1) quote in USD/USDC/USDT, (2) status online, (3) 24h volume
>= min_volume_usd_daily) + ar:AR-074 (BTC/USD always included as the market_regime anchor) + A-17/A-18
(the instrument snapshot carries the full pair set). The 24h-volume floor (condition 3) is applied
DOWNSTREAM at gate:G2_Liquidity (the LiquidityProbe fills the cache, Gate 2 blocks a thin pair) - so
this load applies conditions (1)+(2) and lets Gate 2 enforce (3); a thin pair is simply blocked, never
omitted from the data layer.

assemble_operational needs the universe list UP FRONT (it builds the ShardPlan + per-pair ohlc/ticker
subscribes), but the instrument metadata only arrives over the WS. This one-shot loader resolves that:
open ONE public WS, subscribe the GLOBAL instrument channel (it delivers every pair without a per-pair
subscribe), read until the snapshot, derive the universe, close. The socket is then thrown away - the
running organism opens its own sharded sockets in assemble_operational.

PURE save the injected open_socket I/O edge (the same Transport contract the receive loop drives), so
the loader is driven under asyncio.run over a fake transport - no network.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence

from ..exchange.assembler import SubscribeRequest, to_wire
from ..exchange.channels import PublicChannel
from ..exchange.instrument_cache import InstrumentCache
from ..exchange.transport import Transport

# ar:AR-070 condition (1): the permitted quote currencies (the D-03 universe scope).
DEFAULT_QUOTES: tuple[str, ...] = ("USD", "USDC", "USDT")
# ar:AR-074: BTC/USD is always in the universe as the market_regime anchor proxy.
DEFAULT_ALWAYS_INCLUDE: tuple[str, ...] = ("BTC/USD",)
# A bound on how many frames to read while waiting for the snapshot (a subscribe ACK / status /
# heartbeat may precede it). The instrument snapshot is the first frame Kraken pushes on the channel.
_DEFAULT_MAX_FRAMES = 100

OpenSocket = Callable[[int], Awaitable[Transport]]


class UniverseLoadError(RuntimeError):
    """The instrument snapshot did not arrive (no snapshot within max_frames, or an empty result).
    Startup cannot proceed without a universe; the caller HALTs + alerts (a cold-start cannot trade
    against an unknown pair set), mirroring REST-WST-006 (no token -> HALT startup)."""


def _quote_of(symbol: str) -> str | None:
    """The quote currency of a Kraken WS v2 symbol ('BTC/USD' -> 'USD'); None if not 'BASE/QUOTE'."""
    return symbol.split("/")[-1] if "/" in symbol else None


def derive_universe(
    cache: InstrumentCache,
    *,
    quotes: Sequence[str] = DEFAULT_QUOTES,
    always_include: Sequence[str] = DEFAULT_ALWAYS_INCLUDE,
) -> tuple[str, ...]:
    """ar:AR-070 conditions (1)+(2): the online pairs quoted in `quotes`, UNION the always_include
    anchor(s) (ar:AR-074 BTC/USD). Sorted + de-duplicated, so the ShardPlan partition is deterministic.
    The 24h-volume floor (condition 3) is NOT applied here - gate:G2_Liquidity enforces it downstream."""
    quote_set = {q.upper() for q in quotes}
    selected: set[str] = set(always_include)
    for symbol in cache.symbols:
        info = cache.get(symbol)
        if info is None or not info.is_online:
            continue
        quote = _quote_of(symbol)
        if quote is not None and quote.upper() in quote_set:
            selected.add(symbol)
    return tuple(sorted(selected))


async def load_universe(
    open_socket: OpenSocket,
    *,
    quotes: Sequence[str] = DEFAULT_QUOTES,
    always_include: Sequence[str] = DEFAULT_ALWAYS_INCLUDE,
    max_frames: int = _DEFAULT_MAX_FRAMES,
) -> tuple[str, ...]:
    """ar:AR-070 universe load: open one public WS (open_socket(0) - shard 0 carries the global
    channels), subscribe the instrument channel, read until the snapshot, derive + return the
    universe, and close the socket. `open_socket` is the same shard-indexed opener the data layer
    uses (called with index 0). Raises UniverseLoadError if no instrument snapshot arrives within
    max_frames (the cold-start HALTs - never trade against an unknown universe)."""
    transport = await open_socket(0)
    try:
        await transport.send(to_wire(SubscribeRequest(PublicChannel.INSTRUMENT)))
        cache = InstrumentCache()
        snapshot_seen = False
        for _ in range(max_frames):
            frame = await transport.recv()
            if frame.get("channel") == "instrument" and frame.get("type") == "snapshot":
                cache.ingest(frame)
                snapshot_seen = True
                break
    finally:
        await transport.close()
    if not snapshot_seen:
        raise UniverseLoadError(
            f"no instrument snapshot within {max_frames} frames (ar:AR-070 universe load failed)"
        )
    universe = derive_universe(cache, quotes=quotes, always_include=always_include)
    if not universe:
        raise UniverseLoadError("instrument snapshot held no online USD/USDC/USDT pairs")
    return universe
