"""CIATS-owned pair VIABILITY seed - which pairs are worth monitoring, picked by a forward re-sim.

Source: TB00772 phase 3 of the universe-expansion arc (NSI) + the TB00771 FP/DP determination (over-
admission CAUSES a systemic false-negative via REST/data-layer flooding -> an interior optimum N; the
liquidity heavy tail gives a sharp cutoff). PHASE 2 (app/prescreen) makes the CHEAP cut: ONE bulk Ticker
ranks the 735-pair ar:AR-070 set by liquidity -> a bounded candidate set. PHASE 3 (this module) makes the
PRINCIPLED cut: a FORWARD historical re-simulation per candidate estimates its run-to-reversal expectancy,
and CIATS sets the monitored cardinality N by optimizing captured expectancy subject to the capacity
budget. This is the never-traded tail's day-one estimate (DEC-124 "seed-then-correct"); CIATS then TUNES
it live from the trade record via the EXISTING shadow_replay PDCA loop (it does not stay frozen).

THE RE-SIM IS THE SAME ENGINE the expected_reward seed already uses (compute_expected_reward /
replay_excursions): replay the layer:L1a regime-reversal exit per pair over a historical daily series,
take the per-regime realized run-to-reversal excursions. We reduce that per-regime structure to ONE per-
pair viability scalar = the FREQUENCY-WEIGHTED mean expected excursion across the pair's realized entry
mix (sum_r (n_r / N_total) * median_r): a pair's expected per-trade run-to-reversal return over the
regimes it actually entered, weighted by how often it entered each. This is a faithful method choice (not
a new tunable, Bill ruling TB00731) - it reuses the already-ratified DEC-124 harness + median recipe.

WHY frequency-weighted (not the max regime): a pair viable only in a rare regime should rank below a pair
that pays steadily across its common regimes - the weighting is the honest "expected capturable edge".

CIATS PICKS N (Bill's directive - NOT the operator): a pair is VIABLE iff its weighted expectancy > 0
(it historically pays to run to reversal). Under a HARD capacity budget (an engineering constant: the VPS
+ per-IP data-layer capacity, NOT a strategy seed - so TB00000 stays v2_101), maximizing summed captured
expectancy over a cardinality <= capacity_n is GREEDY-OPTIMAL: take the top-capacity_n viable pairs by
viability. That greedy cut IS the optimization. The ar:AR-074 anchor (BTC/USD) is ALWAYS monitored (the
market_regime proxy) regardless of its own viability or rank.

This module is PURE (no I/O): compute_pair_viability + select_viable_universe + ViabilityStore are driven
directly in tests over crafted bar series. The async startup orchestrator that fetches each candidate's
daily bars under the global REST governor and seeds the store lives in app/viability_screen.py (the I/O
edge), mirroring app/prescreen.py for the phase-2 cut."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal

from .expected_reward import replay_excursions
from .seed_estimators import quantile

# The DEC-124 central-tendency recipe (identical to expected_reward): the per-regime realized excursion
# summary is the MEDIAN (the 0.5 quantile). The method, not a tunable seed.
VIABILITY_QUANTILE = Decimal("0.5")


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def weighted_viability(by_regime: Mapping[object, Sequence[object]]) -> Decimal | None:
    """The FREQUENCY-WEIGHTED mean of the per-regime median excursions (sum_r (n_r / N_total) * median_r)
    over a per-regime excursion structure (the replay_excursions output). Returns None when there is NO
    realized reversal at all (every list empty / no regime). PURE - the viability reduction in isolation,
    so the weighting is unit-tested without crafting a full bar series."""
    total = sum(len(excursions) for excursions in by_regime.values())
    if total <= 0:
        return None
    weighted = Decimal(0)
    for excursions in by_regime.values():
        if not excursions:
            continue
        median = quantile(excursions, VIABILITY_QUANTILE)
        weighted += (Decimal(len(excursions)) / Decimal(total)) * median
    return weighted


def compute_pair_viability(symbol: str, bars: Sequence[object]) -> Decimal | None:
    """The single per-pair viability scalar from the forward re-sim: the FREQUENCY-WEIGHTED mean of the
    per-regime median run-to-reversal excursions. Returns None when the pair has NO realized reversal
    in-window (never permitted to enter, or no entry reversed) - simply not viable to monitor (the caller
    drops it). PURE - reuses the ratified DEC-124 harness (replay_excursions) + the weighting reduction."""
    return weighted_viability(replay_excursions(symbol, bars))


def select_viable_universe(
    viability: Mapping[str, Decimal | None],
    *,
    capacity_n: int,
    always_include: Sequence[str] = (),
) -> tuple[str, ...]:
    """CIATS picks the monitored universe: drop non-viable pairs (viability None or <= 0), rank the rest
    by viability desc (tie-break by symbol for determinism), take the top capacity_n (greedy-optimal for
    summed captured expectancy under the hard capacity budget), and ALWAYS union the always_include
    anchor(s) (ar:AR-074 BTC/USD) regardless of their own viability or rank. capacity_n <= 0 keeps ALL
    viable pairs (no capacity cut). Result SORTED + de-duplicated so the ShardPlan partition is
    deterministic. PURE."""
    viable = [(sym, v) for sym, v in viability.items() if v is not None and v > 0]
    ranked = sorted(viable, key=lambda kv: (kv[1], kv[0]), reverse=True)
    if capacity_n > 0:
        ranked = ranked[:capacity_n]
    selected = {sym for sym, _ in ranked} | set(always_include)
    return tuple(sorted(selected))


class ViabilityStore:
    """The per-pair viability seed store - a CIATS-OWNED parameter computed once from historical OHLC at
    universe load (the forward re-sim), then TUNED live from the trade record via shadow_replay PDCA. It
    holds the day-one estimate for the never-traded tail; select() applies the CIATS capacity cut."""

    def __init__(self) -> None:
        self._by_symbol: dict[str, Decimal] = {}

    def put(self, symbol: str, value: object) -> None:
        self._by_symbol[symbol] = _dec(value)

    def get(self, symbol: str) -> Decimal | None:
        return self._by_symbol.get(symbol)

    def seed_from_bars(self, symbol: str, bars: Sequence[object]) -> None:
        """Compute + store one pair's viability from its historical daily bar series. A non-viable pair
        (no realized reversal) is NOT stored (get() returns None -> select() drops it)."""
        value = compute_pair_viability(symbol, bars)
        if value is not None:
            self._by_symbol[symbol] = value

    def select(self, *, capacity_n: int, always_include: Sequence[str] = ()) -> tuple[str, ...]:
        """The CIATS monitored-universe cut over the seeded viabilities (select_viable_universe)."""
        return select_viable_universe(
            dict(self._by_symbol), capacity_n=capacity_n, always_include=always_include
        )
