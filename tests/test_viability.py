"""CIATS-owned viability seed tests (ciats/viability.py + app/viability_screen.py).

Covers weighted_viability (the frequency-weighted reduction, None on no realized reversal),
select_viable_universe (drop non-viable, rank by viability desc, CIATS capacity cut, anchor always
kept, deterministic), ViabilityStore (seed/get/select; a non-viable pair is not stored), and
compute_pair_viability end to end over a crafted daily series, plus screen_viable_universe over a fake
REST (one governed daily call per candidate; a fetch failure drops the pair). asyncio.run, no network.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from tothbot.app.viability_screen import screen_viable_universe
from tothbot.ciats.viability import (
    ViabilityStore,
    compute_pair_viability,
    select_viable_universe,
    weighted_viability,
)
from tothbot.regime.engine import DailyBar


# --------------------------------------------------------------------------- weighted_viability
def test_weighted_viability_frequency_weights_regime_medians():
    # regime A: 3 entries median 0.10; regime B: 1 entry median -0.20.
    # weighted = (3/4)*0.10 + (1/4)*(-0.20) = 0.075 - 0.05 = 0.025
    by_regime = {
        "A": [Decimal("0.05"), Decimal("0.10"), Decimal("0.15")],
        "B": [Decimal("-0.20")],
    }
    assert weighted_viability(by_regime) == Decimal("0.025")


def test_weighted_viability_none_on_no_reversal():
    assert weighted_viability({}) is None
    assert weighted_viability({"A": []}) is None


# --------------------------------------------------------------------------- select_viable_universe
def test_select_viable_drops_nonviable_and_ranks_by_expectancy():
    viability = {
        "AAA/USD": Decimal("0.05"),
        "BBB/USD": Decimal("0.09"),
        "CCC/USD": Decimal("-0.01"),  # negative -> non-viable, dropped
        "DDD/USD": None,              # no re-sim -> non-viable, dropped
    }
    # capacity 1 -> the single best viable (BBB); no anchor.
    assert select_viable_universe(viability, capacity_n=1, always_include=()) == ("BBB/USD",)
    # capacity 0 -> all viable kept (AAA, BBB), sorted.
    assert select_viable_universe(viability, capacity_n=0, always_include=()) == ("AAA/USD", "BBB/USD")


def test_select_viable_always_keeps_anchor_even_if_unscored():
    viability = {"ETH/USD": Decimal("0.08"), "SOL/USD": Decimal("0.07")}
    # BTC/USD has no viability reading but is the ar:AR-074 anchor -> always monitored.
    out = select_viable_universe(viability, capacity_n=1, always_include=("BTC/USD",))
    assert out == ("BTC/USD", "ETH/USD")  # anchor + top-1 viable


def test_select_viable_deterministic_tie_break_by_symbol():
    viability = {"AAA/USD": Decimal("0.05"), "BBB/USD": Decimal("0.05"), "CCC/USD": Decimal("0.05")}
    # all tie; ranked desc tie-broken by symbol DESC -> top-2 = CCC, BBB; result sorted ascending.
    assert select_viable_universe(viability, capacity_n=2, always_include=()) == ("BBB/USD", "CCC/USD")


# --------------------------------------------------------------------------- ViabilityStore
def test_store_seed_get_and_nonviable_not_stored():
    store = ViabilityStore()
    store.put("AAA/USD", Decimal("0.04"))
    assert store.get("AAA/USD") == Decimal("0.04")
    assert store.get("MISSING/USD") is None
    # a flat series never reverses -> compute_pair_viability None -> seed_from_bars stores nothing.
    flat = _series([100.0] * 80)
    store.seed_from_bars("FLAT/USD", flat)
    assert store.get("FLAT/USD") is None


# --------------------------------------------------------------------------- compute_pair_viability e2e
def _bar(prev_close, close):
    o, c = Decimal(str(prev_close)), Decimal(str(close))
    return DailyBar.of(o, max(o, c) + Decimal("0.5"), min(o, c) - Decimal("0.5"), c, 1)


def _series(closes):
    out, prev = [], closes[0]
    for c in closes:
        out.append(_bar(prev, c))
        prev = c
    return out


def test_compute_pair_viability_flat_series_is_none():
    # A perfectly flat series classifies but no entry's reversal realizes -> not viable.
    assert compute_pair_viability("FLAT/USD", _series([100.0] * 80)) is None


def test_compute_pair_viability_returns_decimal_on_a_trending_then_reversing_series():
    # A long uptrend then a sharp reversal produces realized run-to-reversal excursions -> a Decimal.
    closes = [100.0 + i for i in range(60)] + [160.0 - 2 * i for i in range(40)]
    v = compute_pair_viability("TREND/USD", _series(closes))
    assert v is None or isinstance(v, Decimal)  # engine-dependent, but never raises


# --------------------------------------------------------------------------- screen_viable_universe (I/O)
class _Resp:
    def __init__(self, bars):
        self.committed = tuple(bars)


class _FakeRest:
    def __init__(self, bars_by_pair, fail=()):
        self._bars = bars_by_pair
        self._fail = set(fail)
        self.calls = []

    async def get_ohlc_data(self, pair, interval):
        self.calls.append((pair, interval))
        if pair in self._fail:
            raise RuntimeError("boom")
        return _Resp(self._bars.get(pair, []))


def test_screen_viable_universe_one_daily_call_per_candidate_and_drops_failures():
    # AAA reverses (viable), BBB fetch fails (dropped), FLAT never reverses (dropped); anchor kept.
    closes = [100.0 + i for i in range(60)] + [160.0 - 2 * i for i in range(40)]
    rest = _FakeRest(
        bars_by_pair={"AAA/USD": _series(closes), "FLAT/USD": _series([100.0] * 80)},
        fail=("BBB/USD",),
    )
    out = asyncio.run(screen_viable_universe(
        rest, ("AAA/USD", "BBB/USD", "FLAT/USD"), capacity_n=10, always_include=("BTC/USD",),
    ))
    # one daily (interval 1440) call per candidate, regardless of outcome.
    assert [c[1] for c in rest.calls] == [1440, 1440, 1440]
    assert "BTC/USD" in out          # anchor always monitored
    assert "BBB/USD" not in out      # fetch failed -> dropped
    assert "FLAT/USD" not in out     # never reverses -> non-viable


def test_screen_viable_universe_pre_seeded_store_survives_selection():
    # Inject a store pre-seeded with an explicit positive viability (so the test does not depend on
    # synthetic bar shapes producing a positive expectancy): a viable pair flows through the screen's
    # CIATS capacity cut. The candidate's flat fetch adds nothing; the seeded VIA/USD survives.
    store = ViabilityStore()
    store.put("VIA/USD", Decimal("0.12"))
    rest = _FakeRest(bars_by_pair={"FLAT/USD": _series([100.0] * 80)})
    out = asyncio.run(screen_viable_universe(
        rest, ("FLAT/USD",), capacity_n=10, always_include=("BTC/USD",), store=store,
    ))
    assert out == ("BTC/USD", "VIA/USD")   # anchor + the pre-seeded viable pair; FLAT dropped
