"""rest - the Kraken Spot REST client edges (container:Kraken_REST_API).

Source: 0500000 dv1_250 sec 2 Image1 (Kraken API Connection Architecture, the REST
endpoint surface) + sec 7 container:Kraken_REST_API + contract:Reconciliation_REST.
Houses the synchronous request/reconcile surface the WS layers depend on:
  GetWebSocketsToken - startup/reconnect private-WS auth (HMAC-SHA512, A-14/WS-AUTH-002)
  GetOHLCData        - daily regime compute + ATR/EMA warm-up seeding (AR-017/AR-044);
                       CRITICAL: ALWAYS exclude response[-1] (the forming candle)
  GetOpenOrders      - executions seq-gap / restart reconcile fallback (AR-021)
  GetAccountBalance  - balance reconcile fallback

Split mirrors the WS edge convention (transport.py): the SIGNING + response PARSING
are PURE, Decimal-typed, fully unit-testable cores (auth.py + the client parsers);
the HTTP call is the single I/O edge (a RestTransport, default a lazily-imported
aiohttp adapter) so importing this package - and the whole test suite - never needs
aiohttp. PA-005: the REST surface is byte-identical paper/live.

DIAGRAMS GOVERN: implement strictly from the 0500000 figures. REST signing constants
(nonce, endpoint paths) are Kraken wire facts, not CIATS-owned seeds.
"""
