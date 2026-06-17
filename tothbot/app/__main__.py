"""python -m tothbot.app : the real cold-start (paper default) - the live-deploy edge construction.

Source: 0500000 dv1_254 ar:AR-049 (the cold-start startup sequence) + ar:AR-070 (the universe load) +
Image1 (the public WS endpoint). app/runner.py is the composition root that takes the data-layer I/O as
INJECTED edges (so it is driven over fakes in test); THIS module constructs the REAL edges and runs it:

  - rest_client   = KrakenRestClient()      - real aiohttp REST (PUBLIC calls only in paper: GetOHLCData
                    warm-up/regime + GetTicker liquidity; no credentials needed, the private WS is never
                    connected in paper per PA-004 div #1 / rule:HR-WM-022).
  - open_socket   = a public WS opener over exchange/transport.connect(PUBLIC) (wss://ws.kraken.com/v2).
  - bucket        = the process-singleton SubscribeTokenBucket (contract:WM-PACE-001).
  - mpp/reward    = the EMPTY DEC-128 / DEC-124 CIATS seed stores (seeded in-line during the AR-049
                    warm-up/regime phases from the bars those phases already fetch, OPS-1).
  - universe      = ar:AR-070 load from the instrument snapshot (app/universe.load_universe), BEFORE the
                    data layer assembles (it needs the pair set to build the ShardPlan + per-pair subs).

PAPER needs NO API keys + NO SMTP (the C1/C2-C6 email seams stay unwired without TOTHBOT_SMTP_*; the
organism runs and the reports simply do not emit). LIVE additionally needs the private-WS edges
(open_private_socket / acquire_token / fetch_snap_orders / balances_handler). TB00769 wired those
straight THROUGH build_system to assemble_operational (the plumbing seam is done + tested), so the only
remaining live-deploy slice is THIS entrypoint constructing the REAL private edges - the authenticated
PRIVATE socket over wss://ws-auth.kraken.com/v2 + a token from REST GetWebSocketsToken, both needing real
Kraken API credentials from the environment (rule:HR-LG-009 / REST-KEY-004, NEVER hardcoded). Until those
creds + an explicit operator go are in place, this entrypoint still HALTs on TOTHBOT_MODE=live rather than
silently running a half-wired live path.

The seams (connect_fn / run_fn) are injected so the composition is unit-tested without a socket; main()
binds the real edges and blocks in run() until the organism stops.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace

from ..ciats.expected_reward import ExpectedRewardStore
from ..ciats.seed_estimators import MppCapStore
from ..config.settings import Mode
from ..exchange.connection import ConnectionRole
from ..exchange.pacing import SubscribeTokenBucket
from ..exchange.transport import Transport, connect
from ..rest.client import KrakenRestClient
from .prescreen import screen_universe
from .runner import OpsSettings, run
from .universe import DEFAULT_ALWAYS_INCLUDE, load_universe
from .viability_screen import screen_viable_universe

ConnectFn = Callable[..., Awaitable[Transport]]


def console_event_sink(event: object) -> None:
    """An ADDITIVE stdout telemetry tap (bound as the mod:Logger on_event) so a smoke / first run is
    observable: each organism event prints one concise line (its code + a truncated repr). The records
    still flow to the Logger's Stream-1/Stream-2 corpus; this only mirrors them to the console (the
    nohup log). Low-volume for a small universe (warm-up / regime / subscribe / sweep / close events)."""
    code = getattr(event, "code", None) or type(event).__name__
    print(f"[evt] {code}: {event!r}"[:280], flush=True)


def _parse_universe_override(environ: Mapping[str, str] | None) -> tuple[str, ...]:
    """The TOTHBOT_UNIVERSE pin (comma-separated WS-v2 symbols) for a smoke / first-run test, or ()
    when unset/blank (-> the full ar:AR-070 load). The BTC/USD anchor (ar:AR-074) is always unioned in
    so the daily market_regime anchor computes even when the operator did not list it. Sorted +
    de-duplicated so the ShardPlan partition is deterministic."""
    import os

    env = environ if environ is not None else os.environ
    raw = env.get("TOTHBOT_UNIVERSE")
    pinned = {p.strip() for p in raw.split(",") if p.strip()} if raw else set()
    if not pinned:
        return ()
    return tuple(sorted(pinned | set(DEFAULT_ALWAYS_INCLUDE)))


def _parse_top_n(environ: Mapping[str, str] | None) -> int:
    """The TOTHBOT_TOP_N phase-2 pre-screen cut (an int) - trim the ar:AR-070 derived universe to the
    top-N most liquid pairs before the data layer subscribes/warms. 0 / unset / unparseable -> 0 (NO
    screen, the full AR-070 set). Ignored when TOTHBOT_UNIVERSE pins an explicit set (the pin wins).
    PHASE 2 = the operator picks N for data gathering; PHASE 3 hands N to CIATS (a viability seed)."""
    import os

    env = environ if environ is not None else os.environ
    raw = (env.get("TOTHBOT_TOP_N") or "").strip()
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def _parse_viability_n(environ: Mapping[str, str] | None) -> int:
    """The TOTHBOT_VIABILITY_N phase-3 capacity budget (an int) - the monitored cardinality CIATS picks
    the top-N viable pairs up to, from a forward historical re-sim of the phase-2 liquidity candidates.
    0 / unset / unparseable -> 0 (NO viability screen; the phase-2 / full set stands). Ignored when
    TOTHBOT_UNIVERSE pins an explicit set. The capacity budget is an ENGINEERING constant (VPS + per-IP
    data-layer capacity), NOT a strategy seed - CIATS picks WHICH pairs, the operator sizes the budget."""
    import os

    env = environ if environ is not None else os.environ
    raw = (env.get("TOTHBOT_VIABILITY_N") or "").strip()
    if not raw:
        return 0
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def make_public_open_socket(connect_fn: ConnectFn = connect) -> Callable[[int], Awaitable[Transport]]:
    """The data layer's open_socket(shard_index) -> Transport over the PUBLIC Kraken WS v2 endpoint
    (wss://ws.kraken.com/v2). Every public shard opens the same public endpoint, so the shard index is
    accepted (the DataLayerAssembler contract) but does not vary the URL. connect_fn is injected in
    tests; the default is the real exchange/transport.connect (lazy websockets, WS-LIB-001)."""

    async def open_socket(shard_index: int) -> Transport:
        return await connect_fn(ConnectionRole.PUBLIC)

    return open_socket


async def _amain(
    environ: Mapping[str, str] | None = None,
    *,
    connect_fn: ConnectFn = connect,
    run_fn: Callable[..., Awaitable[None]] = run,
    rest_client: object | None = None,
) -> None:
    """The cold-start body: read settings, build the real edges, load the ar:AR-070 universe, run.
    rest_client is injected in tests (it drives the phase-2 pre-screen + the warm-up/regime/liquidity
    REST phases); the default constructs the real public-only KrakenRestClient."""
    settings = OpsSettings.from_env(environ, universe=())
    if settings.mode is Mode.LIVE:
        raise SystemExit(
            "tothbot.app: live mode is not yet enabled here. build_system now forwards the private-WS "
            "edges (TB00769), so the only remaining slice is constructing the REAL private socket "
            "(wss://ws-auth.kraken.com/v2) + a REST GetWebSocketsToken, which need real Kraken API "
            "credentials from the environment (HR-LG-009 / REST-KEY-004) and an explicit operator go. "
            "Set TOTHBOT_MODE=paper to run."
        )

    open_socket = make_public_open_socket(connect_fn)
    # public-only in paper (no credentials); also drives the phase-2 pre-screen. Injected in tests.
    if rest_client is None:
        rest_client = KrakenRestClient()
    # TOTHBOT_UNIVERSE override (a comma-separated pin, e.g. "BTC/USD,ETH/USD,SOL/USD") - a small fixed
    # universe for a smoke / first-run test: it SKIPS the AR-070 instrument-snapshot load and uses the
    # pinned pairs directly (the BTC/USD anchor is always unioned in, ar:AR-074, so the daily market_
    # regime anchor still computes). Empty / unset -> the full ar:AR-070 load from the instrument snapshot.
    pinned = _parse_universe_override(environ)
    if pinned:
        universe = pinned
        print(f"tothbot.app: TOTHBOT_UNIVERSE pinned ({len(universe)} pairs): {', '.join(universe)}")
    else:
        # ar:AR-070: derive the monitored universe from the instrument snapshot BEFORE assembling the
        # data layer (it needs the pair set up front). A failed load raises UniverseLoadError -> the
        # process exits (never trade against an unknown universe; mirrors REST-WST-006 HALT-on-no-token).
        universe = await load_universe(open_socket)
        print(f"tothbot.app: AR-070 universe loaded ({len(universe)} pairs)")
        # Phase-2 bulk pre-screen: trim the derived AR-070 set to the top-N most liquid pairs (TOTHBOT_
        # TOP_N) BEFORE the data layer subscribes/warms, so a large universe does not firehose the data
        # layer + REST budget. 0 / unset -> no screen (the full set). The two screening REST calls go
        # through the global governor. A TOTHBOT_UNIVERSE pin is NOT screened (the explicit pin wins).
        top_n = _parse_top_n(environ)
        if top_n:
            universe = await screen_universe(rest_client, universe, top_n=top_n)
            print(f"tothbot.app: pre-screened to top-{top_n} by liquidity ({len(universe)} pairs)")
        # Phase-3 CIATS-owned viability cut (TOTHBOT_VIABILITY_N): a forward historical re-sim per
        # candidate (one governed daily-OHLC call each) -> CIATS picks the top-N viable by run-to-
        # reversal expectancy under the capacity budget. Runs over the phase-2 candidates so the daily
        # re-sim is bounded. 0 / unset -> no viability screen (the phase-2 / full set stands).
        viability_n = _parse_viability_n(environ)
        if viability_n:
            universe = await screen_viable_universe(rest_client, universe, capacity_n=viability_n)
            print(f"tothbot.app: viability-screened to top-{viability_n} by expectancy "
                  f"({len(universe)} pairs)")
    settings = replace(settings, universe=universe)
    print(f"tothbot.app: starting {settings.mode.value} organism")

    await run_fn(
        settings,
        rest_client=rest_client,
        open_socket=open_socket,
        on_event=console_event_sink,      # mirror organism telemetry to stdout (the nohup log)
        bucket=SubscribeTokenBucket(),
        mpp_store=MppCapStore(),
        reward_store=ExpectedRewardStore(),
    )


def main() -> None:
    """Console entry: drive the cold-start under asyncio.run, blocking until the organism stops."""
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
