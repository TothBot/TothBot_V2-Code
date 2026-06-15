"""The per-pair best bid/offer cache - live quotes from channel:ticker (the WS ticker feed).

Source: 0500000 dv1_250 ar:AR-048 (the Exit_Controller MAE/drawdown reads bid for a long, ask for
a short, from event_trigger:bbo - bid AND ask both update immediately on an adversarial move) +
ar:AR-069 (entry limit-price construction reads the bbo) + WS-TKR-002/003 (a fresh ticker subscribes
event_trigger=trades for a pair with no open position, flips to bbo when a position opens). This is
the SINGLE runtime accessor for the latest bid/ask per symbol; the sweep's ExecutionContext + the
on_htf_ohlc_close / on_regime_classified exit drivers read it.

Sole-owner per-symbol cache (the RegimeCache / InstrumentCache pattern), fed by the inbound ticker
handler. PURE + Decimal-only (ar:AR-047): bid/ask are Decimal(str(value)) on receipt. The 24h USD
VOLUME (Gate-2 liquidity) is NOT here - it is the D1-owned liquidity_24h value from the separate
REST Ticker probe (liquidity_refresh_hours=4); this cache is only the live bbo.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


@dataclass(frozen=True)
class Bbo:
    """One pair's latest best bid/offer (ar:AR-048). best_bid = the realizable long exit; best_ask
    = the realizable short cover (the buy-back price)."""

    symbol: str
    best_bid: Decimal
    best_ask: Decimal


def parse_ticker_bbo(elem: Mapping[str, object]) -> Bbo:
    """Parse one Kraken WS v2 ticker data entry into a Bbo (Decimal, ar:AR-047). The ticker entry
    carries bid/ask (the best prices) alongside last/volume; only bid/ask are the bbo."""
    return Bbo(symbol=str(elem["symbol"]), best_bid=_dec(elem["bid"]), best_ask=_dec(elem["ask"]))


class BboCache:
    """Sole-owner per-symbol bbo cache fed by the channel:ticker frames. ingest() applies a snapshot
    or update frame (data is a list of ticker entries); the sweep + exit drivers read by symbol."""

    def __init__(self) -> None:
        self._by_symbol: dict[str, Bbo] = {}

    def put(self, bbo: Bbo) -> None:
        self._by_symbol[bbo.symbol] = bbo

    def get(self, symbol: str) -> Bbo | None:
        return self._by_symbol.get(symbol)

    def bbo(self, symbol: str) -> "tuple[Decimal, Decimal] | None":
        """(best_bid, best_ask) for `symbol`, or None if no quote yet - the LiveProviders.bbo
        shape. The sweep guards a missing quote (a pair with no ticker yet is not entered)."""
        entry = self._by_symbol.get(symbol)
        return None if entry is None else (entry.best_bid, entry.best_ask)

    def ingest(self, frame: Mapping[str, object]) -> list[str]:
        """Apply one ticker frame (snapshot or update; data is a list of entries). Each entry with a
        symbol + bid + ask is parsed + stored. Returns the symbols updated. A malformed entry is
        skipped (the prior quote stands)."""
        data = frame.get("data")
        entries: Sequence[Mapping[str, object]] = data if isinstance(data, list) else []
        updated: list[str] = []
        for elem in entries:
            try:
                bbo = parse_ticker_bbo(elem)
            except (KeyError, TypeError, ValueError):
                continue
            self._by_symbol[bbo.symbol] = bbo
            updated.append(bbo.symbol)
        return updated

    @property
    def symbols(self) -> frozenset[str]:
        return frozenset(self._by_symbol)
