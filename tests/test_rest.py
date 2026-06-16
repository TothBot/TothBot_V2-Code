"""REST client edge tests: auth signing + response parsers + the client (rest/).

Covers 0500000 dv1_250 container:Kraken_REST_API: the A-14/WS-AUTH-002 HMAC-SHA512
signing (validated against Kraken's PUBLISHED test vector - a real cryptographic
anchor, not a self-referential round-trip), the AR-017 response[-1] exclusion in
GetOHLCData parsing, the reconcile-fallback parsers, and the client driving a fake
RestTransport (private requests carry a fresh nonce + API-Sign; no network, no aiohttp).
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest

from tothbot.exchange.connection import REST_BASE_URL
from tothbot.rest.auth import (
    Credentials,
    NonceGenerator,
    auth_headers,
    sign,
)
from tothbot.rest.client import (
    KrakenRestClient,
    KrakenRestError,
    OhlcResponse,
    PATH_OHLC,
    PATH_OPEN_ORDERS,
    PATH_QUERY_ORDERS,
    PATH_TRADE_BALANCE,
    PATH_WS_TOKEN,
    RestOhlcBar,
    gap_close_fill,
    parse_account_balance,
    parse_trade_balance,
    parse_ohlc,
    parse_open_orders,
    parse_query_orders,
    parse_websockets_token,
    raise_for_error,
)


# --- signing: Kraken's published test vector ---------------------------------
# https://docs.kraken.com/api/docs/guides/spot-rest-auth - the canonical example.
_VECTOR_PATH = "/0/private/AddOrder"
_VECTOR_SECRET = (
    "kQH5HW/8p1uGOVjbgWA7FunAmGO8lsSUXNsu3eow76sz84Q18fWxnyRzBHCd3pd5nE9qa99HAZtuZuj6F1huXg=="
)
# Insertion order reproduces the vector's urlencoded body exactly.
_VECTOR_DATA = {
    "nonce": "1616492376594",
    "ordertype": "limit",
    "pair": "XBTUSD",
    "price": 37500,
    "type": "buy",
    "volume": 1.25,
}
_VECTOR_SIG = (
    "4/dpxb3iT4tp/ZCVEwSnEsLxx0bqyhLpdfOpc6fn7OR8+UClSV5n9E6aSS8MPtnRfp32bAb0nmbRn6H8ndwLUQ=="
)


def test_sign_matches_kraken_published_vector():
    assert sign(_VECTOR_PATH, _VECTOR_DATA, _VECTOR_SECRET) == _VECTOR_SIG


def test_sign_changes_with_nonce():
    other = {**_VECTOR_DATA, "nonce": "1616492376595"}
    assert sign(_VECTOR_PATH, other, _VECTOR_SECRET) != _VECTOR_SIG


def test_auth_headers_shape():
    headers = auth_headers("KEY", "SIG")
    assert headers == {"API-Key": "KEY", "API-Sign": "SIG"}


# --- nonce generator ----------------------------------------------------------

def test_nonce_is_strictly_increasing_even_with_frozen_clock():
    gen = NonceGenerator(clock=lambda: 1000.0)  # frozen
    a, b, c = gen.next(), gen.next(), gen.next()
    assert a < b < c


def test_nonce_tracks_clock_milliseconds():
    gen = NonceGenerator(clock=lambda: 1616492376.594)
    assert gen.next() == 1616492376594


# --- envelope error handling --------------------------------------------------

def test_raise_for_error_returns_result_on_success():
    assert raise_for_error({"error": [], "result": {"ok": 1}}) == {"ok": 1}


def test_raise_for_error_raises_on_nonempty_error():
    with pytest.raises(KrakenRestError) as exc:
        raise_for_error({"error": ["EAPI:Invalid key"], "result": {}})
    assert exc.value.errors == ["EAPI:Invalid key"]


def test_raise_for_error_raises_on_missing_result():
    with pytest.raises(KrakenRestError):
        raise_for_error({"error": []})


# --- GetWebSocketsToken parser ------------------------------------------------

def test_parse_websockets_token():
    payload = {"error": [], "result": {"token": "abc123", "expires": 900}}
    assert parse_websockets_token(payload) == "abc123"


def test_parse_websockets_token_missing_raises():
    with pytest.raises(KrakenRestError):
        parse_websockets_token({"error": [], "result": {}})


# --- GetOHLCData parser (AR-017 exclusion) ------------------------------------

def _ohlc_payload(pair="XXBTZUSD"):
    # rows: [time, open, high, low, close, vwap, volume, count]
    return {
        "error": [],
        "result": {
            pair: [
                [1700000000, "100.0", "110.0", "90.0", "105.0", "102.0", "12.5", 30],
                [1700000300, "105.0", "115.0", "95.0", "108.0", "107.0", "9.0", 22],
                [1700000600, "108.0", "120.0", "100.0", "118.0", "112.0", "5.0", 11],
            ],
            "last": 1700000600,
        },
    }


def test_parse_ohlc_excludes_forming_last_candle():
    resp = parse_ohlc(_ohlc_payload(), "XXBTZUSD")
    assert isinstance(resp, OhlcResponse)
    # 3 rows in -> 2 committed (AR-017 drops response[-1]).
    assert len(resp.committed) == 2
    assert resp.committed[-1].close == Decimal("108.0")
    assert resp.forming.close == Decimal("118.0")
    assert resp.last == 1700000600


def test_parse_ohlc_decimal_typed():
    resp = parse_ohlc(_ohlc_payload(), "XXBTZUSD")
    bar = resp.committed[0]
    assert isinstance(bar, RestOhlcBar)
    assert (bar.open, bar.high, bar.low, bar.close, bar.volume) == (
        Decimal("100.0"), Decimal("110.0"), Decimal("90.0"), Decimal("105.0"), Decimal("12.5"),
    )
    assert bar.time == 1700000000


def test_parse_ohlc_falls_back_to_altname_key():
    # Caller asks for "XBTUSD" but Kraken keys the result by the altname "XXBTZUSD".
    resp = parse_ohlc(_ohlc_payload("XXBTZUSD"), "XBTUSD")
    assert len(resp.committed) == 2


def test_parse_ohlc_empty_series():
    payload = {"error": [], "result": {"XXBTZUSD": [], "last": 0}}
    resp = parse_ohlc(payload, "XXBTZUSD")
    assert resp.committed == ()
    assert resp.forming is None


# --- reconcile-fallback parsers -----------------------------------------------

def test_parse_open_orders_attaches_txid():
    payload = {
        "error": [],
        "result": {"open": {"OABC-123": {"status": "open", "vol": "1.0"}}},
    }
    orders = parse_open_orders(payload)
    assert orders == [{"txid": "OABC-123", "status": "open", "vol": "1.0"}]


def test_parse_open_orders_empty():
    assert parse_open_orders({"error": [], "result": {"open": {}}}) == []


def test_parse_account_balance_decimal():
    payload = {"error": [], "result": {"ZUSD": "1500.5000", "XXBT": "0.25"}}
    bal = parse_account_balance(payload)
    assert bal == {"ZUSD": Decimal("1500.5000"), "XXBT": Decimal("0.25")}


def test_parse_trade_balance_decimal_equity():
    # REST-BAL-008: the SHORT margin-equity baseline source - every field Decimal, `e` = equity.
    payload = {"error": [], "result": {
        "eb": "5000.0000", "tb": "5000.0000", "m": "0.0000", "n": "0.0000",
        "c": "0.0000", "v": "0.0000", "e": "5000.0000", "mf": "5000.0000",
    }}
    tb = parse_trade_balance(payload)
    assert tb["e"] == Decimal("5000.0000")          # the borrow-adjusted margin EQUITY (the baseline)
    assert all(isinstance(v, Decimal) for v in tb.values())
    # flat cold start sanity: equity == equivalent balance == trade balance (no open positions yet).
    assert tb["e"] == tb["eb"] == tb["tb"]


# --- the client over a fake transport -----------------------------------------

class _FakeRestTransport:
    """Hand-driven RestTransport: records calls, returns scripted envelopes."""

    def __init__(self, *, get_result=None, post_result=None) -> None:
        self.get_calls: list[tuple[str, dict]] = []
        self.post_calls: list[tuple[str, dict, dict]] = []
        self._get_result = get_result or {"error": [], "result": {}}
        self._post_result = post_result or {"error": [], "result": {}}
        self.closed = False

    async def get(self, url, params):
        self.get_calls.append((url, dict(params)))
        return self._get_result

    async def post(self, url, data, headers):
        self.post_calls.append((url, dict(data), dict(headers)))
        return self._post_result

    async def close(self):
        self.closed = True


_CREDS = Credentials(api_key="KEY", api_secret=_VECTOR_SECRET)


def test_client_get_websockets_token_signs_private_request():
    fake = _FakeRestTransport(post_result={"error": [], "result": {"token": "tok-1"}})
    client = KrakenRestClient(_CREDS, transport=fake, nonce=NonceGenerator(clock=lambda: 1.0))
    token = asyncio.run(client.get_websockets_token())
    assert token == "tok-1"
    url, data, headers = fake.post_calls[0]
    assert url == REST_BASE_URL + PATH_WS_TOKEN
    assert "nonce" in data  # nonce stamped into the signed body
    assert headers["API-Key"] == "KEY"
    assert headers["API-Sign"]  # a signature was attached


def test_client_get_ohlc_data_is_public_and_excludes_last():
    fake = _FakeRestTransport(get_result=_ohlc_payload())
    client = KrakenRestClient(_CREDS, transport=fake)
    resp = asyncio.run(client.get_ohlc_data("XBTUSD", 1440))
    assert len(resp.committed) == 2  # AR-017
    url, params = fake.get_calls[0]
    assert url == REST_BASE_URL + PATH_OHLC
    assert params == {"pair": "XBTUSD", "interval": 1440}
    assert not fake.post_calls  # public: never signed


def test_client_get_ohlc_data_passes_since_cursor():
    fake = _FakeRestTransport(get_result=_ohlc_payload())
    client = KrakenRestClient(_CREDS, transport=fake)
    asyncio.run(client.get_ohlc_data("XBTUSD", 5, since=1700000000))
    _, params = fake.get_calls[0]
    assert params["since"] == 1700000000


def test_client_get_open_orders_signs_and_parses():
    fake = _FakeRestTransport(
        post_result={"error": [], "result": {"open": {"O1": {"status": "open"}}}}
    )
    client = KrakenRestClient(_CREDS, transport=fake)
    orders = asyncio.run(client.get_open_orders())
    assert orders == [{"txid": "O1", "status": "open"}]
    assert fake.post_calls[0][0] == REST_BASE_URL + PATH_OPEN_ORDERS


def test_client_get_account_balance():
    fake = _FakeRestTransport(post_result={"error": [], "result": {"ZUSD": "10.0"}})
    client = KrakenRestClient(_CREDS, transport=fake)
    bal = asyncio.run(client.get_account_balance())
    assert bal == {"ZUSD": Decimal("10.0")}


def test_client_get_trade_balance_signs_and_carries_asset():
    # REST-BAL-008 TradeBalance: signed private call, asset=ZUSD in the body, result['e'] the equity.
    fake = _FakeRestTransport(post_result={"error": [], "result": {"e": "4980.50", "tb": "5000.0"}})
    client = KrakenRestClient(_CREDS, transport=fake)
    tb = asyncio.run(client.get_trade_balance())
    assert tb["e"] == Decimal("4980.50")
    url, data, headers = fake.post_calls[0]
    assert url == REST_BASE_URL + PATH_TRADE_BALANCE
    assert data["asset"] == "ZUSD" and "nonce" in data
    assert headers["API-Sign"]  # private: signed


def test_client_nonce_increases_across_private_calls():
    fake = _FakeRestTransport(post_result={"error": [], "result": {"token": "t"}})
    client = KrakenRestClient(_CREDS, transport=fake, nonce=NonceGenerator(clock=lambda: 1.0))
    asyncio.run(client.get_websockets_token())
    asyncio.run(client.get_websockets_token())
    n1 = int(fake.post_calls[0][1]["nonce"])
    n2 = int(fake.post_calls[1][1]["nonce"])
    assert n2 > n1


def test_client_private_without_credentials_raises():
    fake = _FakeRestTransport()
    client = KrakenRestClient(None, transport=fake)
    with pytest.raises(KrakenRestError):
        asyncio.run(client.get_websockets_token())


def test_client_propagates_kraken_error():
    fake = _FakeRestTransport(get_result={"error": ["EGeneral:Invalid arguments"], "result": {}})
    client = KrakenRestClient(_CREDS, transport=fake)
    with pytest.raises(KrakenRestError):
        asyncio.run(client.get_ohlc_data("XBTUSD", 5))


def test_client_close_delegates_to_transport():
    fake = _FakeRestTransport()
    client = KrakenRestClient(_CREDS, transport=fake)
    asyncio.run(client.close())
    assert fake.closed


# --- QueryOrders (REST-QOI) + gap_close_fill (the ar:AR-056 gap-close actual fill) ----

def test_parse_query_orders_decimal_typed_and_keyed_by_txid():
    payload = {
        "error": [],
        "result": {
            "OEMERG-1": {"status": "closed", "vol": "0.5", "vol_exec": "0.5",
                         "price": "99.50", "fee": "0.12", "cost": "49.75"},
        },
    }
    parsed = parse_query_orders(payload)
    assert set(parsed) == {"OEMERG-1"}
    o = parsed["OEMERG-1"]
    assert o["price"] == Decimal("99.50")
    assert o["fee"] == Decimal("0.12")
    assert o["vol_exec"] == Decimal("0.5")
    assert isinstance(o["price"], Decimal)


def test_gap_close_fill_returns_actual_price_and_fee_on_closed_fill():
    order = {"status": "closed", "vol_exec": Decimal("0.5"), "price": Decimal("99.50"),
             "fee": Decimal("0.12")}
    assert gap_close_fill(order) == (Decimal("99.50"), Decimal("0.12"))


def test_gap_close_fill_none_when_not_closed_or_zero_fill():
    assert gap_close_fill({"status": "open", "vol_exec": Decimal("0.5"), "price": Decimal("1")}) is None
    assert gap_close_fill({"status": "closed", "vol_exec": Decimal("0"), "price": Decimal("1")}) is None
    assert gap_close_fill({"status": "canceled", "vol_exec": Decimal("0"), "price": Decimal("1")}) is None


def test_gap_close_fill_defaults_fee_to_zero_when_absent():
    order = {"status": "closed", "vol_exec": Decimal("0.5"), "price": Decimal("99.50")}
    assert gap_close_fill(order) == (Decimal("99.50"), Decimal("0"))


def test_client_query_orders_signs_and_parses():
    fake = _FakeRestTransport(
        post_result={"error": [], "result": {"O9": {"status": "closed", "vol_exec": "1",
                                                    "price": "100", "fee": "0.5"}}}
    )
    client = KrakenRestClient(_CREDS, transport=fake)
    out = asyncio.run(client.query_orders(["O9"]))
    assert out["O9"]["price"] == Decimal("100")
    url, data, _ = fake.post_calls[0]
    assert url == REST_BASE_URL + PATH_QUERY_ORDERS
    assert data["txid"] == "O9"
    assert data["trades"] is True


def test_client_query_orders_empty_short_circuits_without_call():
    fake = _FakeRestTransport()
    client = KrakenRestClient(_CREDS, transport=fake)
    out = asyncio.run(client.query_orders([]))
    assert out == {}
    assert not fake.post_calls
