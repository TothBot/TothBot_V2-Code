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
(open_private_socket / acquire_token) which build_system does not yet pass through - that wiring is the
remaining live-deploy slice, so this entrypoint HALTs on TOTHBOT_MODE=live with a clear message rather
than silently running a half-wired live path.

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
from .runner import OpsSettings, run
from .universe import DEFAULT_ALWAYS_INCLUDE, load_universe

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
) -> None:
    """The cold-start body: read settings, build the real edges, load the ar:AR-070 universe, run."""
    settings = OpsSettings.from_env(environ, universe=())
    if settings.mode is Mode.LIVE:
        raise SystemExit(
            "tothbot.app: live mode is not yet wired here (the private-WS edges - open_private_socket "
            "+ acquire_token - are the remaining live-deploy slice). Set TOTHBOT_MODE=paper to run."
        )

    open_socket = make_public_open_socket(connect_fn)
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
    settings = replace(settings, universe=universe)
    print(f"tothbot.app: starting {settings.mode.value} organism")

    await run_fn(
        settings,
        rest_client=KrakenRestClient(),   # public-only in paper (no credentials)
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
