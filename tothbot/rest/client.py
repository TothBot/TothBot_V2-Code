"""Kraken Spot REST client - pure response parsers + the single HTTP edge.

Source: 0500000 dv1_250 sec 7 container:Kraken_REST_API + contract:Reconciliation_REST
(GetWebSocketsToken / GetOHLCData / GetOpenOrders / GetAccountBalance) + AR-017 (daily
regime, exclude response[-1]) + AR-044 (warm-up seeding intervals 5/60/1440, response[:-1])
+ AR-021 (executions seq-gap -> GetOpenOrders reconcile).

The response PARSERS are pure + Decimal-typed (raise_for_error / parse_ohlc /
parse_open_orders / parse_account_balance / parse_websockets_token): given a decoded
Kraken JSON envelope they validate and shape it, with NO I/O. The HTTP call is the lone
edge - a RestTransport (default the lazily-imported AiohttpRestTransport, which reuses one
session per A-23 connection-reuse) - so the client is driven in tests with a fake
transport, no network and no aiohttp dependency at import time.

The endpoint PATHS are the real Kraken Spot wire paths (the diagram channel names
GetOHLCData / GetOpenOrders / GetAccountBalance map to /0/public/OHLC, /0/private/
OpenOrders, /0/private/Balance). The AR-036/AR-044 1.1s GetOHLCData stagger is the daily-
compute orchestrator's edge (Path B), NOT applied here - this client is one call per call.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, runtime_checkable

from ..exchange.connection import REST_BASE_URL
from .auth import Credentials, NonceGenerator, auth_headers, sign

# --- Kraken Spot REST endpoint paths (wire facts) ----------------------------
PATH_WS_TOKEN = "/0/private/GetWebSocketsToken"   # channel:kraken_rest_GetWebSocketsToken
PATH_OHLC = "/0/public/OHLC"                      # channel:kraken_rest_GetOHLCData
PATH_TICKER = "/0/public/Ticker"                  # channel:kraken_rest_Ticker (liquidity probe)
PATH_OPEN_ORDERS = "/0/private/OpenOrders"        # channel:kraken_rest_GetOpenOrders
PATH_BALANCE = "/0/private/Balance"               # channel:kraken_rest_GetAccountBalance


class KrakenRestError(RuntimeError):
    """A Kraken REST envelope returned a non-empty ``error`` array. Carries the raw
    error strings so the caller (reconcile cycle / daily compute) can log + branch."""

    def __init__(self, errors: Sequence[str]) -> None:
        self.errors = list(errors)
        super().__init__("; ".join(self.errors) or "unknown Kraken REST error")


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


# --- pure response parsers ----------------------------------------------------

def raise_for_error(payload: Mapping[str, object]) -> dict:
    """Validate a Kraken envelope {error:[...], result:{...}} and return ``result``.

    Kraken signals failure with a non-empty ``error`` array (the HTTP status can
    still be 200), so the error array - not the status code - is authoritative."""
    errors = payload.get("error") or []
    if errors:
        raise KrakenRestError([str(e) for e in errors])
    result = payload.get("result")
    if not isinstance(result, Mapping):
        raise KrakenRestError(["malformed Kraken REST envelope: missing result"])
    return dict(result)


def parse_websockets_token(payload: Mapping[str, object]) -> str:
    """GetWebSocketsToken -> the WS auth token (result.token). Fresh per call; never
    cached/reused across reconnects (REST-WST-004 / WS-REC-004)."""
    result = raise_for_error(payload)
    token = result.get("token")
    if not token:
        raise KrakenRestError(["GetWebSocketsToken returned no token"])
    return str(token)


@dataclass(frozen=True)
class RestOhlcBar:
    """One committed OHLC bar from GetOHLCData. Decimal on receipt (rule:HR-REGIME-006).
    Feeds DailyBar.of() / the ATR/EMA seed series directly; ``time`` is unix seconds
    (candle start)."""

    time: int
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal


@dataclass(frozen=True)
class OhlcResponse:
    """Parsed GetOHLCData result with the AR-017 forming candle ALREADY split off.

    ``committed`` is response[:-1] (the closed candles - what every indicator seed
    consumes). ``forming`` is response[-1] (the current uncommitted candle, EXCLUDED
    from all computation per AR-017). ``last`` is Kraken's incremental cursor (the
    ``since`` for the next call). committed[-1] is the last committed candle whose
    interval_begin seeds AR-045's last_interval_begin (response[-2])."""

    committed: tuple[RestOhlcBar, ...]
    forming: RestOhlcBar | None
    last: int


def parse_ohlc(payload: Mapping[str, object], pair: str | None = None) -> OhlcResponse:
    """GetOHLCData -> OhlcResponse, EXCLUDING response[-1] (AR-017, the forming candle).

    Kraken returns result = {<pair_or_altname>: [[time,o,h,l,c,vwap,vol,count], ...],
    "last": <cursor>}. The pair key may be the altname rather than the requested pair,
    so the candle array is the one non-"last" entry (``pair`` is accepted for clarity
    but the lookup falls back to that single array). A Kraken OHLC row is a positional
    list [time, open, high, low, close, vwap, volume, count]."""
    result = raise_for_error(payload)
    last = int(result.get("last", 0))
    rows: object = None
    if pair is not None and pair in result:
        rows = result[pair]
    else:
        for key, value in result.items():
            if key != "last":
                rows = value
                break
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise KrakenRestError(["GetOHLCData returned no candle array"])

    bars = [
        RestOhlcBar(
            time=int(row[0]),
            open=_dec(row[1]),
            high=_dec(row[2]),
            low=_dec(row[3]),
            close=_dec(row[4]),
            volume=_dec(row[6]),
        )
        for row in rows
    ]
    if not bars:
        return OhlcResponse(committed=(), forming=None, last=last)
    # AR-017: the LAST bar is the current forming candle - split it off, never seed it.
    return OhlcResponse(committed=tuple(bars[:-1]), forming=bars[-1], last=last)


def parse_ticker_liquidity(payload: Mapping[str, object]) -> dict[str, Decimal]:
    """GetTicker -> {pair_key: vol_24h_usd} - the D1 liquidity_24h probe (channel:kraken_rest_
    Ticker; liquidity_refresh_hours=4 cache TTL). Kraken returns result = {<pair_key>: {a, b, c,
    v:[today, last24h], p:[today, last24h], t, l, h, o}}. The 24h USD volume = v[1] * p[1] (the
    last-24h base volume times the last-24h vwap). A key missing the v/p arrays is skipped."""
    result = raise_for_error(payload)
    out: dict[str, Decimal] = {}
    for key, ticker in result.items():
        if not isinstance(ticker, Mapping):
            continue
        v = ticker.get("v")
        p = ticker.get("p")
        if not (isinstance(v, Sequence) and isinstance(p, Sequence) and len(v) > 1 and len(p) > 1):
            continue
        out[str(key)] = _dec(v[1]) * _dec(p[1])
    return out


def parse_open_orders(payload: Mapping[str, object]) -> list[dict]:
    """GetOpenOrders -> the open orders as a list of order mappings (AR-021 reconcile
    fallback / snap_orders source). Kraken returns result = {"open": {txid: {...}}};
    each order dict is returned with its txid attached so it reconciles against the
    Position Mirror (restore_position_mirror)."""
    result = raise_for_error(payload)
    open_orders = result.get("open") or {}
    if not isinstance(open_orders, Mapping):
        raise KrakenRestError(["GetOpenOrders returned a malformed open map"])
    return [{"txid": txid, **dict(order)} for txid, order in open_orders.items()]


def parse_account_balance(payload: Mapping[str, object]) -> dict[str, Decimal]:
    """GetAccountBalance -> {asset: Decimal balance} (balance reconcile fallback)."""
    result = raise_for_error(payload)
    return {asset: _dec(amount) for asset, amount in result.items()}


# --- the HTTP edge ------------------------------------------------------------

@runtime_checkable
class RestTransport(Protocol):
    """The async HTTP contract the client drives at the I/O edge. get() is the public
    surface (params -> query string); post() is the private surface (form body + the
    signed headers). Both return the already-decoded JSON envelope (a dict). Driven
    for real by AiohttpRestTransport and for tests by a hand-built fake."""

    async def get(self, url: str, params: Mapping[str, object]) -> dict: ...

    async def post(self, url: str, data: Mapping[str, object], headers: Mapping[str, str]) -> dict: ...

    async def close(self) -> None: ...


class AiohttpRestTransport:
    """Real RestTransport over one reused ``aiohttp`` session (A-23 connection reuse).

    ``aiohttp`` is a VPS-runtime dependency imported LAZILY on first use, so importing
    this module - and the test suite - never requires the library. The session is
    created on first request and reused for every subsequent call until close()."""

    def __init__(self) -> None:
        self._session: object | None = None

    async def _ensure_session(self) -> object:
        if self._session is None:
            import aiohttp  # lazy (VPS-runtime dependency)

            self._session = aiohttp.ClientSession()
        return self._session

    async def get(self, url: str, params: Mapping[str, object]) -> dict:
        session = await self._ensure_session()
        async with session.get(url, params=dict(params)) as resp:  # type: ignore[attr-defined]
            return await resp.json()

    async def post(self, url: str, data: Mapping[str, object], headers: Mapping[str, str]) -> dict:
        session = await self._ensure_session()
        async with session.post(url, data=dict(data), headers=dict(headers)) as resp:  # type: ignore[attr-defined]
            return await resp.json()

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()  # type: ignore[attr-defined]
            self._session = None


# --- the client ---------------------------------------------------------------

class KrakenRestClient:
    """Async Kraken Spot REST client - signs private requests + parses every response.

    Construct with operator Credentials (private calls only) + optionally a RestTransport
    (default AiohttpRestTransport) and a NonceGenerator (default millisecond clock). The
    methods are thin: build the request, sign the private ones, drive the transport, run
    the pure parser. get_websockets_token() plugs straight into private_ws AcquireToken;
    get_open_orders() into FetchSnapOrders.
    """

    def __init__(
        self,
        credentials: Credentials | None = None,
        *,
        transport: RestTransport | None = None,
        nonce: NonceGenerator | None = None,
        base_url: str = REST_BASE_URL,
    ) -> None:
        self._creds = credentials
        self._http = transport or AiohttpRestTransport()
        self._nonce = nonce or NonceGenerator()
        self._base_url = base_url

    async def close(self) -> None:
        await self._http.close()

    # --- request primitives ---------------------------------------------------
    async def _public(self, path: str, params: Mapping[str, object]) -> dict:
        return await self._http.get(self._base_url + path, params)

    async def _private(self, path: str, data: Mapping[str, object]) -> dict:
        if self._creds is None:
            raise KrakenRestError([f"{path} requires credentials (none configured)"])
        body = {**data, "nonce": self._nonce.next()}
        signature = sign(path, body, self._creds.api_secret)
        headers = auth_headers(self._creds.api_key, signature)
        return await self._http.post(self._base_url + path, body, headers)

    # --- endpoints ------------------------------------------------------------
    async def get_websockets_token(self) -> str:
        """GetWebSocketsToken (private). Returns the fresh WS auth token (WS-AUTH-002);
        the live private connection acquires a NEW token on every (re)connect."""
        payload = await self._private(PATH_WS_TOKEN, {})
        return parse_websockets_token(payload)

    async def get_ohlc_data(
        self, pair: str, interval: int, *, since: int | None = None
    ) -> OhlcResponse:
        """GetOHLCData (public). interval in minutes (5 / 60 / 1440 per AR-044). The
        returned committed series ALREADY excludes response[-1] (AR-017)."""
        params: dict[str, object] = {"pair": pair, "interval": interval}
        if since is not None:
            params["since"] = since
        payload = await self._public(PATH_OHLC, params)
        return parse_ohlc(payload, pair)

    async def get_ticker_liquidity(self, pair: str) -> Decimal:
        """GetTicker (public) -> the pair's 24h USD volume (the D1 liquidity_24h probe; Gate-2).
        Called one pair at a time at universe load + the liquidity_refresh_hours=4 refresh; the
        result key may be the altname, so the single returned ticker's vol_24h_usd is taken."""
        payload = await self._public(PATH_TICKER, {"pair": pair})
        liquidity = parse_ticker_liquidity(payload)
        if pair in liquidity:
            return liquidity[pair]
        for vol in liquidity.values():
            return vol
        raise KrakenRestError([f"GetTicker returned no ticker for {pair}"])

    async def get_open_orders(self) -> list[dict]:
        """GetOpenOrders (private). The AR-021 reconcile fallback / snap_orders source."""
        payload = await self._private(PATH_OPEN_ORDERS, {})
        return parse_open_orders(payload)

    async def get_account_balance(self) -> dict[str, Decimal]:
        """GetAccountBalance (private). The balance reconcile fallback."""
        payload = await self._private(PATH_BALANCE, {})
        return parse_account_balance(payload)
