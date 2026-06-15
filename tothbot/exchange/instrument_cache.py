"""The per-pair instrument cache - status / marginable / order minimums (channel:instrument).

Source: 0500000 dv1_250 A-17 (the instrument channel provides price_increment [GAP-C: tick_size
deprecated, use price_increment exclusively], qty_min, cost_min) + ar:AR-028 (qty_min/cost_min
validated in Gate 8 before every add_order) + Pre-Gate-1 (instrument_status online-only + the
marginable short subset, ar:AR-009/AR-070) + the CR-06 per-pair base order size (per_trade_size_usd
= max(50, 5 * max(cost_min, qty_min * entry_limit_price)), value home TB00000 sec 8).

The instrument channel snapshot carries the full pair set (A-18: it can exceed 1MB for 500+ pairs);
updates carry the changed pairs. This is a SOLE-OWNER per-symbol cache (the pattern of RegimeCache):
the inbound instrument handler ingest()s each frame, the gates + the CR-06 sizer read it. PURE +
Decimal-only (ar:AR-047): every price/qty is Decimal(str(value)) on receipt, never Decimal(float).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal

from ..config import registry

# CR-06 per-pair base-size seeds (value home TB00000 sec 8), Decimal once (ar:AR-047).
_SIZE_FLOOR_USD = Decimal(str(registry.value("per_trade_size_floor_usd")))   # $50 floor
_SIZE_MARGIN_MULT = Decimal(str(registry.value("per_trade_size_margin_mult")))  # 5x
_ONLINE = "online"


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


@dataclass(frozen=True)
class InstrumentInfo:
    """One pair's instrument metadata (A-17 / ar:AR-028). status drives Pre-Gate-1; marginable
    partitions the short universe; qty_min/cost_min gate G8 + seed CR-06; the increments quantize
    orders (ar:AR-047 - price_increment, NOT the deprecated tick_size)."""

    symbol: str
    status: str
    marginable: bool
    qty_min: Decimal
    cost_min: Decimal
    price_increment: Decimal
    qty_increment: Decimal

    @property
    def is_online(self) -> bool:
        return self.status == _ONLINE


def parse_instrument_pair(elem: Mapping[str, object]) -> InstrumentInfo:
    """Parse one Kraken WS v2 instrument `pairs` entry into an InstrumentInfo (Decimal, ar:AR-047).

    Uses price_increment (A-17 GAP-C: tick_size is deprecated). marginable defaults False (a pair
    with no margin flag cannot be shorted, ar:AR-009)."""
    return InstrumentInfo(
        symbol=str(elem["symbol"]),
        status=str(elem.get("status", "")),
        marginable=bool(elem.get("marginable", False)),
        qty_min=_dec(elem["qty_min"]),
        cost_min=_dec(elem["cost_min"]),
        price_increment=_dec(elem["price_increment"]),
        qty_increment=_dec(elem["qty_increment"]),
    )


class InstrumentCache:
    """Sole-owner per-symbol instrument cache fed by the channel:instrument frames. ingest() applies
    a snapshot or update frame (both carry data.pairs); the gates + CR-06 sizer read by symbol."""

    def __init__(self) -> None:
        self._by_symbol: dict[str, InstrumentInfo] = {}

    def put(self, info: InstrumentInfo) -> None:
        self._by_symbol[info.symbol] = info

    def get(self, symbol: str) -> InstrumentInfo | None:
        return self._by_symbol.get(symbol)

    def ingest(self, frame: Mapping[str, object]) -> list[str]:
        """Apply one instrument frame (snapshot or update). Both shapes carry data.pairs (a list of
        pair objects); each is parsed + stored. Returns the symbols updated. A pair missing a
        required field is skipped (never raises on a partial wire frame - the prior entry stands)."""
        data = frame.get("data") or {}
        pairs: Sequence[Mapping[str, object]] = data.get("pairs") or [] if isinstance(data, Mapping) else []
        updated: list[str] = []
        for elem in pairs:
            try:
                info = parse_instrument_pair(elem)
            except (KeyError, TypeError, ValueError):
                continue  # a malformed/partial pair entry: keep the prior cache entry, skip it
            self._by_symbol[info.symbol] = info
            updated.append(info.symbol)
        return updated

    @property
    def symbols(self) -> frozenset[str]:
        return frozenset(self._by_symbol)


def base_per_trade_size_usd(
    cost_min: object,
    qty_min: object,
    entry_ref_price: object,
    *,
    floor_usd: object = _SIZE_FLOOR_USD,
    margin_mult: object = _SIZE_MARGIN_MULT,
) -> Decimal:
    """The CR-06 per-pair base order size (value home TB00000 sec 8):

        max(floor, margin_mult * max(cost_min, qty_min * entry_ref_price))

    = max($50, 5 * max(cost_min, qty_min * entry_limit_price)) - the $50 floor + 5x margin above the
    pair's real minimum (cost_min, or qty_min priced at the entry reference). The flat-across-R:R
    starting form; CIATS differentiates by expected_R:R from the 200-trade floor (Gate-6 reads this
    base, the regime multiplier scales it). PURE, Decimal-only (ar:AR-047)."""
    pair_min = max(_dec(cost_min), _dec(qty_min) * _dec(entry_ref_price))
    return max(_dec(floor_usd), _dec(margin_mult) * pair_min)
