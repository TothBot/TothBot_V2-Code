"""mod:CIATS_Parameter_Store - the per-module owned-parameter store + the frozen read snapshot.

Source: 0500000 dv1_250 sec 6/7 mod:CIATS_Parameter_Store + rule:HR-CI-002 ("the SOLE CIATS write
destination, holding ALL tunable parameters EXCEPT the sacred rule:Sacred_R_R_1_to_1_5 floor, which
is hardcoded outside CIATS authority") + contract:Parameter_Store_Snapshot ("TothBot reads a FROZEN
snapshot at the start of each pipeline eval per CI-IF-003, preventing parameter drift WITHIN a
cycle") + rule:HR-CI-003 (writes only at a confirmed inter-trade boundary) + rule:HR-CI-005 (a
50-trade minimum interval between any two parameter changes) + rule:HR-CI-011 (Bill approval).

The WRITE-owner: apply() takes a Bill-approved PDCA ApprovedChange and writes the owned parameter,
recording the parameter-evolution log entry + the trade count at the change. Two HARD invariants
enforced HERE as defense-in-depth (even though the PDCA ACT already gated them):
  - IMMUTABILITY (HR-CI-002): the sacred 1:1.5 R:R + any exchange-defined parameter (e.g. the taker
    fee, the margin leverage cap - external facts CIATS DETECTS but never SETS) are NEVER written.
  - the 50-TRADE INTERVAL (HR-CI-005): two changes may not fall within 50 closed trades; the store
    owns last_change_trade_count, so it is the authoritative interval source the PDCA ACT consults.

The READ-owner: snapshot() returns a FROZEN read-only mapping (contract:Parameter_Store_Snapshot);
TothBot takes ONE at the start of each pipeline cycle and reads from it for the whole cycle, so a
mid-cycle write never perturbs an in-flight evaluation (CI-IF-003 no-drift).

PER-MODULE (one store per wallet, like the pool). PURE state, Decimal values pass through unchanged
(ar:AR-047 - the values are already Decimal where numeric). The store NEVER writes the sacred R:R.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType

from .pdca_engine import MIN_TRADES_BETWEEN_CHANGES, SACRED_RR_PARAM


@dataclass(frozen=True)
class ParameterChange:
    """One parameter-evolution log entry: the owned parameter, its old + new value, and the closed-
    trade count at which the change was written (the audit trail + the 50-trade-interval anchor)."""

    param_name: str
    old_value: object
    new_value: object
    at_trade_count: int


@dataclass(frozen=True)
class ParameterWritten:
    """evt:PARAMETER_WRITTEN [HIGH] - an approved change applied to the store at an inter-trade
    boundary (the parameter-evolution log gains a ParameterChange)."""

    change: ParameterChange
    code: str = field(default="PARAMETER_WRITTEN", init=False)


@dataclass(frozen=True)
class ParameterWriteRejected:
    """evt:PARAMETER_WRITE_REJECTED [CRITICAL] - a write was refused by a store invariant (an
    immutable/sacred parameter, or the 50-trade interval). Surfaced, never silently dropped - a
    rejected write of an immutable parameter is a defense-in-depth invariant breach indicator."""

    param_name: str
    reason: str
    code: str = field(default="PARAMETER_WRITE_REJECTED", init=False)


class ParameterStore:
    """One module's CIATS-owned parameter store (per-wallet). Holds the live tunable values, applies
    Bill-approved PDCA changes at inter-trade boundaries (never the sacred R:R / exchange params),
    and serves the frozen per-cycle read snapshot."""

    def __init__(
        self,
        *,
        initial: Mapping[str, object] | None = None,
        immutable: Iterable[str] | None = None,
        min_trades_between_changes: int = MIN_TRADES_BETWEEN_CHANGES,
    ) -> None:
        self._values: dict[str, object] = dict(initial or {})
        # The sacred R:R is always immutable (HR-CI-002); the caller adds exchange-defined params.
        self._immutable: frozenset[str] = frozenset({SACRED_RR_PARAM, *(immutable or ())})
        self._min_interval = min_trades_between_changes
        self._last_change_trade_count: int | None = None
        self._evolution: list[ParameterChange] = []

    def get(self, name: str) -> object | None:
        return self._values.get(name)

    def is_immutable(self, name: str) -> bool:
        """True for the sacred R:R + any exchange-defined parameter (never CIATS-writable)."""
        return name in self._immutable

    @property
    def last_change_trade_count(self) -> int | None:
        return self._last_change_trade_count

    def trades_since_last_change(self, current_trade_count: int) -> int:
        """Closed trades since the last parameter change (the HR-CI-005 interval source the PDCA ACT
        consults). With no prior change the full count is returned (the interval is trivially met)."""
        if self._last_change_trade_count is None:
            return current_trade_count
        return current_trade_count - self._last_change_trade_count

    @property
    def evolution_log(self) -> tuple[ParameterChange, ...]:
        """The append-only parameter-evolution log (the audit trail)."""
        return tuple(self._evolution)

    def apply(self, approved_change: object, *, at_trade_count: int) -> object:
        """Apply a Bill-approved PDCA ApprovedChange (its .proposal carries param_name +
        proposed_value). Enforces the HR-CI-002 immutability invariant + the HR-CI-005 50-trade
        interval as defense-in-depth. Returns ParameterWritten on success, or ParameterWriteRejected
        (the invariant that failed) - the store NEVER writes the sacred R:R / an exchange param."""
        proposal = approved_change.proposal
        name = proposal.param_name
        if self.is_immutable(name):
            return ParameterWriteRejected(
                name, f"immutable parameter (HR-CI-002): {name} is never CIATS-writable"
            )
        if (
            self._last_change_trade_count is not None
            and (at_trade_count - self._last_change_trade_count) < self._min_interval
        ):
            return ParameterWriteRejected(
                name,
                f"HR-CI-005: {at_trade_count - self._last_change_trade_count} < "
                f"{self._min_interval}-trade interval",
            )
        old = self._values.get(name)
        new = proposal.proposed_value
        self._values[name] = new
        change = ParameterChange(name, old, new, at_trade_count)
        self._evolution.append(change)
        self._last_change_trade_count = at_trade_count
        return ParameterWritten(change)

    def snapshot(self) -> Mapping[str, object]:
        """A FROZEN read-only copy of the current owned-parameter values (contract:Parameter_Store_
        Snapshot). TothBot takes ONE at the start of a pipeline cycle and reads it for the whole
        cycle - a later apply() never perturbs an in-flight snapshot (CI-IF-003 no-drift)."""
        return MappingProxyType(dict(self._values))
