"""mod:CIATS_Regime_Library - per-regime parameter buckets (activate at 600 trades, 100+ per bucket).

Source: 0500000 dv1_250 sec 6/7 mod:CIATS_Regime_Library: ">= 600-trade per-regime accumulation,
100+ per bucket"; "produces regime-segmented parameter proposals + a disallowed-regime list
(param:disallowed_regimes consumed by gate:G3_Regime_Filter)". The per-regime floor (600 total AND
100+ per asset_regime bucket) is DISTINCT from and STRICTER than the 200-trade module inference floor
(rule:HR-CI-004) - regime segmentation needs more data than module-level tuning.

The library accumulates each closed trade into its asset_regime bucket (one CiatsPool per regime, the
same NET-P/L accumulator). A regime is ACTIVE once the library total has reached 600 AND that bucket
has reached 100 trades - only then do its per-regime statistics (the regime-segmented Half-Kelly
edge) become inference-valid. A regime whose realized edge is non-positive (K_full = W-(1-W)/R <= 0,
ar:AR-065) once active is DISALLOWED (added to param:disallowed_regimes -> Gate-3 blocks entries in
it). Every resulting parameter change still routes PDCA -> Bill approval (HR-CI-011) - the library
PROPOSES, it never writes.

PER-MODULE (one library per wallet, like the pool). PURE state, Decimal-only (ar:AR-047).
"""

from __future__ import annotations

from decimal import Decimal

from ..regime.taxonomy import Regime
from .pool import CiatsPool

# The per-regime activation floors (diagram-named; distinct from the 200-trade module floor).
REGIME_ACTIVATION_TRADES = 600   # library total before any regime segmentation is valid
REGIME_BUCKET_MIN = 100          # 100+ trades in a bucket before that regime activates

_ZERO = Decimal("0")
_ONE = Decimal("1")


class RegimeLibrary:
    """One module's per-regime statistics library. Routes each closed trade to its asset_regime
    bucket, gates each regime on the 600-total / 100-per-bucket floor, and exposes the active
    regimes + the disallowed-regime list (non-positive realized edge). PROPOSE-only."""

    def __init__(
        self,
        *,
        activation_trades: int = REGIME_ACTIVATION_TRADES,
        bucket_min: int = REGIME_BUCKET_MIN,
    ) -> None:
        self._activation = activation_trades
        self._bucket_min = bucket_min
        self._buckets: dict[Regime, CiatsPool] = {}
        self._total = 0

    def ingest(self, regime: Regime, *, net_pl: object, net_gain: object, net_loss: object) -> None:
        """Accumulate one closed-trade outcome into its asset_regime bucket (NET P/L, ar:AR-065)."""
        bucket = self._buckets.get(regime)
        if bucket is None:
            bucket = CiatsPool(trade_floor=self._bucket_min)
            self._buckets[regime] = bucket
        bucket.ingest_outcome(net_pl=net_pl, net_gain=net_gain, net_loss=net_loss)
        self._total += 1

    def ingest_trade_close(self, regime: Regime, trade_close: object) -> None:
        """Accumulate one evt:TRADE_CLOSE record into its regime bucket (the Logger Stream-2 shape)."""
        self.ingest(
            regime,
            net_pl=trade_close.net_pl_usd,
            net_gain=trade_close.net_gain_usd,
            net_loss=trade_close.net_loss_usd,
        )

    def bucket(self, regime: Regime) -> CiatsPool | None:
        return self._buckets.get(regime)

    def bucket_count(self, regime: Regime) -> int:
        bucket = self._buckets.get(regime)
        return 0 if bucket is None else bucket.trade_count

    @property
    def total_count(self) -> int:
        return self._total

    @property
    def activated(self) -> bool:
        """True once the library total has reached the 600-trade activation floor."""
        return self._total >= self._activation

    def regime_active(self, regime: Regime) -> bool:
        """True when regime segmentation is inference-valid for `regime`: the library is activated
        (>= 600 total) AND the bucket has >= 100 trades."""
        return self.activated and self.bucket_count(regime) >= self._bucket_min

    def active_regimes(self) -> list[Regime]:
        """The regimes whose per-regime statistics are inference-valid (600 total + 100/bucket)."""
        return [r for r in self._buckets if self.regime_active(r)]

    def regime_edge(self, regime: Regime) -> Decimal | None:
        """The realized Kelly edge K_full = W - (1-W)/R (NET P/L, ar:AR-065) for an ACTIVE regime,
        or None if the regime is not active or W/R are not both defined. > 0 is a positive edge."""
        if not self.regime_active(regime):
            return None
        bucket = self._buckets[regime]
        w = bucket.win_rate
        r = bucket.net_reward_risk
        if w is None or r is None:
            return None
        return w - (_ONE - w) / r

    def disallowed_regimes(self) -> list[Regime]:
        """param:disallowed_regimes (-> gate:G3_Regime_Filter): the ACTIVE regimes whose realized
        edge is non-positive (K_full <= 0) - the module should not trade them. A regime with too few
        trades (not active) is NEVER disallowed (insufficient evidence; it stays tradeable)."""
        out: list[Regime] = []
        for regime in self._buckets:
            edge = self.regime_edge(regime)
            if edge is not None and edge <= _ZERO:
                out.append(regime)
        return out
