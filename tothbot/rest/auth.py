"""Kraken Spot REST request authentication - the PURE signing core (A-14 / WS-AUTH-002).

Source: 0500000 dv1_250 A-14 (Spot REST authentication CONFIRMED UNCHANGED - PL-014
resolved) + sec 7 channel:kraken_rest_GetWebSocketsToken. The October 1, 2025 auth
breaking change applied to FUTURES REST ONLY; Kraken Spot REST signing is unchanged.

The signature (the ``API-Sign`` header value) is, per A-14:

    HMAC-SHA512( base64_decode(api_secret),
                 uri_path + SHA256(nonce + url_encoded_POST_data) )           -> base64

The nonce is an always-increasing integer stamped into the POST data; it is also the
prefix of the SHA256 input. This module is pure + deterministic (no clock, no I/O of
its own): the clock is injected into the nonce generator so the whole signing path is
unit-testable against Kraken's published test vector.

Credentials are operator-supplied at the edge (never committed, never a seed); they are
held in an injectable Credentials value, not read from any config file here.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import time
import urllib.parse
from collections.abc import Callable, Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class Credentials:
    """Operator Kraken API credentials. api_secret is the base64 secret AS ISSUED by
    Kraken (base64-decoded only inside sign()); never logged, never a seed value."""

    api_key: str
    api_secret: str


def sign(uri_path: str, data: Mapping[str, object], api_secret: str) -> str:
    """Compute the Kraken ``API-Sign`` header value for one private request (A-14).

    ``data`` MUST already contain the ``nonce`` (so the signed body and the
    transmitted body are identical). The algorithm is byte-for-byte the canonical
    Kraken Spot scheme; validated in tests against Kraken's documented vector.
    """
    post_data = urllib.parse.urlencode(data)
    encoded = (str(data["nonce"]) + post_data).encode()
    message = uri_path.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(api_secret), message, hashlib.sha512)
    return base64.b64encode(mac.digest()).decode()


def auth_headers(api_key: str, signature: str) -> dict[str, str]:
    """The two Kraken private-request headers: the API key and the request signature."""
    return {"API-Key": api_key, "API-Sign": signature}


class NonceGenerator:
    """Strictly-increasing millisecond nonce source for private REST requests.

    Kraken rejects a nonce that does not exceed the previous one for the key. The
    base value is a millisecond clock (injected so signing is deterministic in
    tests); a stateful guard guarantees strict monotonicity even when two requests
    fall in the same millisecond or the clock does not advance. The clock is the
    only edge here - kept as an injected callable so this stays unit-testable.
    """

    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._last = 0

    def next(self) -> int:
        candidate = int(self._clock() * 1000)
        if candidate <= self._last:
            candidate = self._last + 1
        self._last = candidate
        return candidate
