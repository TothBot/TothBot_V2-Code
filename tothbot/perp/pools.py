"""mod:Long_Spot_Pool / mod:Long_Perp_Pool / mod:Short_Perp_Pool - the three ring-fenced pools.

Source: 0500000 sec 13.7 "Three Separately-Funded Pools" + Image10 mod:Long_Spot_Pool /
mod:Long_Perp_Pool / mod:Short_Perp_Pool + sec 13.5 MARGIN MODEL (isolated FCM wallet) +
TB00806 battery C6 (ring-fence / byte-isolation, validated). The three-pool structure mirrors
the TB00806 oracle's build_spot_pool / build_perp_pool (scripts/tb00806_perp_account_sim.py).

The perps organism is funded as THREE separately-funded, ring-fenced pools - re-groundings of
the EXISTING TB00000 sec 7 parallel modules, NOT new module types (section 13.9 part-vs-whole
law):

  - Long_Spot_Pool  = the EXISTING live-in-paper spot organism (mod:Long_Module), its own full
    Kraken account, UNAFFECTED by the perps build. No leverage / no liquidation (spot held
    outright; margin_frac = 1).
  - Long_Perp_Pool  = the dormant LONG capability re-grounded to the Kraken-Pro perps route.
  - Short_Perp_Pool = the dormant mod:Short_Module re-grounded from the section 2 spot-margin
    path to the perps route (the revived systematic short, section 13.4).

Each perp pool is a linked derivatives subaccount: its OWN operator-funded isolated-margin
wallet, its OWN halts, ISOLATED-margin mode. One pool's liquidation CANNOT reach another - the
RING-FENCE. In code that ring-fence is STRUCTURAL: each pool is a separate object holding its
own equity, so mutating one (even a 99% crash) leaves the others bit-for-bit unchanged (battery
C6 byte-isolation). The wallet here is the isolated FCM margin equity (USD collateral, section
13.5) - genuinely distinct from the spot SyntheticCapitalLedger, which is why it is modelled as
its own equity rather than routed through spot fill arithmetic.

PURE state, Decimal-only (ar:AR-047). The realized perp P&L (price + funding - fees, CAPPED by
rule:Perp_Isolated_Margin_Loss_Cap, margin.py) is applied to a pool via apply_realized_pnl -
the sole mutator, isolated to that pool.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

from ..config import registry
from ..exchange.position_mirror import PositionSide
from .margin import PerpContractSpec

EventSink = Callable[[object], None]

_ZERO = Decimal("0")


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters a pool (ar:AR-047)."""
    return Decimal(str(value))


class PoolKind(Enum):
    """The three ring-fenced pools (section 13.7; Image10). Each names its canonical token."""

    LONG_SPOT = "mod:Long_Spot_Pool"     # the existing live-in-paper spot organism (Pool 1)
    LONG_PERP = "mod:Long_Perp_Pool"     # the LONG capability re-grounded to the perps route
    SHORT_PERP = "mod:Short_Perp_Pool"   # the dormant Short_Module re-grounded to the perps route


_POOL_SIDE = {
    PoolKind.LONG_SPOT: PositionSide.LONG,
    PoolKind.LONG_PERP: PositionSide.LONG,
    PoolKind.SHORT_PERP: PositionSide.SHORT,
}


@dataclass(frozen=True)
class PoolEquityUpdated:
    """evt:POOL_EQUITY_UPDATED - one pool's isolated wallet changed by a realized perp P&L
    (the ring-fenced mutation). delta_usd is signed; liquidated flags a loss-cap liquidation."""

    pool: PoolKind
    new_equity: Decimal
    prior_equity: Decimal
    delta_usd: Decimal
    liquidated: bool = False
    code: str = field(default="POOL_EQUITY_UPDATED", init=False)


class PerpPool:
    """One ring-fenced pool: a re-grounded TB00000 sec 7 module (side + own isolated wallet) plus
    its perp margin spec. Construct one per kind; the three pools share NOTHING - each holds its
    own equity, the structural ring-fence. apply_realized_pnl is the ONLY mutator and touches
    only THIS pool (battery C6 byte-isolation)."""

    def __init__(
        self,
        kind: PoolKind,
        *,
        deposit: object,
        spec: PerpContractSpec | None = None,
        on_event: EventSink | None = None,
    ) -> None:
        self.kind = kind
        self.side = _POOL_SIDE[kind]
        # The operator-funded isolated margin wallet (section 13.5) - "what Bill can afford to
        # lose on THAT side", the primary loss control, ring-fenced to this pool.
        self._deposit = _dec(deposit)
        self._equity = self._deposit
        # The spot pool holds outright - no leverage / no liquidation (spec is None). The perp
        # pools carry the isolated-margin spec (rule:Perp_Isolated_Margin_Loss_Cap).
        if kind is PoolKind.LONG_SPOT:
            if spec is not None:
                raise ValueError("LONG_SPOT pool is spot - it carries no perp margin spec")
            self.spec: PerpContractSpec | None = None
        else:
            self.spec = spec if spec is not None else PerpContractSpec()
        self._on_event = on_event

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    @property
    def deposit(self) -> Decimal:
        """The frozen operator-funded deposit - the HALT ruin-floor baseline (HR-WM-011)."""
        return self._deposit

    @property
    def equity(self) -> Decimal:
        """The pool's current isolated-wallet equity (USD collateral)."""
        return self._equity

    @property
    def is_spot(self) -> bool:
        """True for the Long-Spot pool (held outright, no isolated-margin liquidation)."""
        return self.kind is PoolKind.LONG_SPOT

    @property
    def drawdown_from_deposit(self) -> Decimal:
        """Drawdown of the wallet below the frozen deposit, as a fraction (>=0 means a loss).
        The frozen-deposit ruin floor (the HALT baseline) measures against this."""
        return (self._deposit - self._equity) / self._deposit

    def apply_realized_pnl(self, amount: object, *, liquidated: bool = False) -> Decimal:
        """Apply a realized perp P&L (signed) to THIS pool's isolated wallet - the sole mutator,
        ring-fenced to this pool. amount is the net realized P&L already CAPPED by the loss cap
        (margin.py) for a liquidation. Returns the new equity."""
        delta = _dec(amount)
        prior = self._equity
        self._equity = prior + delta
        self._emit(
            PoolEquityUpdated(
                pool=self.kind,
                new_equity=self._equity,
                prior_equity=prior,
                delta_usd=delta,
                liquidated=liquidated,
            )
        )
        return self._equity


class ThreePoolWallet:
    """The three separately-funded ring-fenced pools (section 13.7). Holds the Long-Spot,
    Long-Perp, and Short-Perp pools as independent objects - the ring-fence is that they share
    no state, so a crash in one is bit-for-bit invisible to the others (battery C6)."""

    def __init__(
        self,
        *,
        long_spot_deposit: object | None = None,
        long_perp_deposit: object | None = None,
        short_perp_deposit: object | None = None,
        long_perp_spec: PerpContractSpec | None = None,
        short_perp_spec: PerpContractSpec | None = None,
        on_event: EventSink | None = None,
    ) -> None:
        # Default each deposit to the paper_starting_balance seed (D-05; per-module wallet).
        ls = (
            registry.value("paper_starting_balance_long_usd")
            if long_spot_deposit is None
            else long_spot_deposit
        )
        lp = (
            registry.value("paper_starting_balance_long_usd")
            if long_perp_deposit is None
            else long_perp_deposit
        )
        sp = (
            registry.value("paper_starting_balance_short_usd")
            if short_perp_deposit is None
            else short_perp_deposit
        )
        self.long_spot = PerpPool(PoolKind.LONG_SPOT, deposit=ls, on_event=on_event)
        self.long_perp = PerpPool(
            PoolKind.LONG_PERP, deposit=lp, spec=long_perp_spec, on_event=on_event
        )
        self.short_perp = PerpPool(
            PoolKind.SHORT_PERP, deposit=sp, spec=short_perp_spec, on_event=on_event
        )

    @property
    def pools(self) -> tuple[PerpPool, PerpPool, PerpPool]:
        """The three pools in canonical order (Long-Spot, Long-Perp, Short-Perp)."""
        return (self.long_spot, self.long_perp, self.short_perp)

    @property
    def total_equity(self) -> Decimal:
        """Combined equity across the three ring-fenced pools (the whole-account view)."""
        return sum((p.equity for p in self.pools), _ZERO)

    def snapshot(self) -> dict[PoolKind, Decimal]:
        """A point-in-time {pool: equity} map - the ring-fence verification handle (compare a
        snapshot before/after crashing one pool; the others must be byte-identical, battery C6)."""
        return {p.kind: p.equity for p in self.pools}
