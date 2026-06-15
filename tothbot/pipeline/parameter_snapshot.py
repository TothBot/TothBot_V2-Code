"""contract:Parameter_Store_Snapshot - the frozen per-cycle CIATS parameter view the gates read.

Source: 0500000 dv1_250 contract:Parameter_Store_Snapshot / CI-IF-003 ("TothBot reads a FROZEN
snapshot at the start of each pipeline eval, preventing parameter drift WITHIN a cycle") +
contract:Pre_Computation_Cache_Read + sec 6/7 mod:CIATS_Parameter_Store (the per-module owned-value
store) + mod:CIATS_Regime_Library (param:disallowed_regimes -> gate:G3_Regime_Filter).

The pipeline gates each carry a CIATS-owned tunable as a registry SEED (mae_mult, sc_body_threshold,
the drawdown halts, the liquidity floor, ...). Once CIATS owns a value (writes it to its per-module
Parameter Store after the 200-trade floor), the gate must read the OWNED value, not the seed - and it
must read ONE frozen value for the whole cycle so a mid-cycle CIATS write never perturbs an in-flight
evaluation (CI-IF-003 no-drift). This module is that read boundary:

  build_cycle_parameters(store, disallowed_regimes)  takes ONE frozen ParameterStore.snapshot() at the
    START of the cycle + the Regime Library's protective block list, and returns a CycleParameters.
  CycleParameters.get(name)  resolves a CIATS-owned value: the store-owned value if CIATS has written
    it, else the registry SEED (config.registry). The same frozen value for the whole cycle.
  CycleParameters.regime_disallowed(regime)  the param:disallowed_regimes read gate:G3 consumes.

THE SACRED 1:1.5 R:R IS NEVER SERVED HERE (rule:Sacred_R_R_1_to_1_5): it is hardcoded in
gate:G8_Position_Sizer (SACRED_RR_FLOOR), never CIATS-owned, never read from the store or the seed
registry. get() on the sacred name raises - a defense-in-depth guard against the floor ever flowing
through the tunable path. PURE; the resolved values pass through to the gates' existing override
kwargs (each gate coerces to Decimal/int on receipt, ar:AR-047)."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from types import MappingProxyType

from ..config import registry

# The sacred R:R names that must NEVER resolve through the tunable snapshot path: the CIATS owned-param
# key (pdca/parameter_store) + the registry SACRED seed. The floor lives hardcoded in G8 only.
_SACRED_NAMES: frozenset[str] = frozenset({"rr_floor", "rr_minimum_net"})


class CycleParameters:
    """A FROZEN per-cycle view of the CIATS-owned parameter values + the protective disallowed-regime
    list (contract:Parameter_Store_Snapshot). Built once at the start of a pipeline cycle; reading it
    can never drift mid-cycle (the underlying snapshot is a frozen copy). The sacred R:R is never
    served (it is hardcoded in gate:G8)."""

    def __init__(
        self,
        *,
        owned: Mapping[str, object] | None = None,
        disallowed_regimes: Iterable[object] = (),
    ) -> None:
        # A private frozen copy of the owned values - the snapshot can never be mutated after the read.
        self._owned: Mapping[str, object] = MappingProxyType(dict(owned or {}))
        self._disallowed: frozenset = frozenset(disallowed_regimes)

    def get(self, name: str) -> object:
        """The CIATS-owned value for `name` if the store has written it, else the registry SEED. The
        same frozen value for the whole cycle. Raises on the sacred R:R (never served here) or an
        unknown name (a wiring bug, never silently defaulted)."""
        if name in _SACRED_NAMES:
            raise ValueError(f"the sacred 1:1.5 R:R ({name}) is hardcoded in G8, never read from the snapshot")
        if name in self._owned:
            return self._owned[name]
        return registry.value(name)  # the seed default (raises KeyError on an unknown name)

    def regime_disallowed(self, regime: object) -> bool:
        """param:disallowed_regimes (-> gate:G3_Regime_Filter): True when the Regime Library has marked
        `regime` a non-positive-edge regime the module should not trade this cycle."""
        return regime in self._disallowed

    @property
    def disallowed_regimes(self) -> frozenset:
        return self._disallowed

    @property
    def owned(self) -> Mapping[str, object]:
        """The frozen owned-value overlay (the store snapshot), for inspection/telemetry."""
        return self._owned


def build_cycle_parameters(
    store: object | None = None, *, disallowed_regimes: Iterable[object] = ()
) -> CycleParameters:
    """Take ONE frozen snapshot at the START of a pipeline cycle: the per-module Parameter Store's
    owned values (store.snapshot(), already a frozen MappingProxyType) overlaid on the registry seeds,
    plus the Regime Library's protective block list. `store` None -> a SEED-only view (identical to the
    pre-CIATS behavior: every gate reads its registry seed). The returned CycleParameters is frozen for
    the cycle (CI-IF-003 no-drift)."""
    owned = dict(store.snapshot()) if store is not None else {}
    return CycleParameters(owned=owned, disallowed_regimes=disallowed_regimes)
