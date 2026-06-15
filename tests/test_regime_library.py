"""mod:CIATS_Regime_Library tests (ciats/regime_library.py).

Covers per-regime bucket accumulation, the 600-total / 100-per-bucket activation floor (distinct
from the 200-trade module floor), the per-regime realized edge K_full = W-(1-W)/R, the active-regime
list, and the disallowed-regime list (active regimes with a non-positive edge; an under-100 bucket is
never disallowed).
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.ciats.regime_library import (
    REGIME_ACTIVATION_TRADES,
    REGIME_BUCKET_MIN,
    RegimeLibrary,
)
from tothbot.regime.taxonomy import Regime


def _fill(lib, regime, n, win_rate, gain=2, loss=1):
    wins = int(n * win_rate)
    for _ in range(wins):
        lib.ingest(regime, net_pl=1, net_gain=gain, net_loss=0)
    for _ in range(n - wins):
        lib.ingest(regime, net_pl=-1, net_gain=0, net_loss=loss)


# --------------------------------------------------------------------------- accumulation
def test_ingest_routes_to_regime_buckets():
    lib = RegimeLibrary()
    _fill(lib, Regime.TRENDING_POS_NORMAL, 30, 0.6)
    _fill(lib, Regime.NON_DIR_NORMAL, 20, 0.5)
    assert lib.bucket_count(Regime.TRENDING_POS_NORMAL) == 30
    assert lib.bucket_count(Regime.NON_DIR_NORMAL) == 20
    assert lib.total_count == 50
    assert lib.bucket_count(Regime.TRENDING_NEG_NORMAL) == 0  # never seen


# --------------------------------------------------------------------------- activation floor
def test_not_activated_below_600_total():
    lib = RegimeLibrary()
    _fill(lib, Regime.TRENDING_POS_NORMAL, 150, 0.6)  # bucket > 100 but total < 600
    assert lib.activated is False
    assert lib.regime_active(Regime.TRENDING_POS_NORMAL) is False
    assert lib.active_regimes() == []


def test_regime_active_requires_total_600_and_bucket_100():
    lib = RegimeLibrary()
    _fill(lib, Regime.TRENDING_POS_NORMAL, 150, 0.6)
    _fill(lib, Regime.NON_DIR_NORMAL, 400, 0.6)
    _fill(lib, Regime.TRENDING_NEG_NORMAL, 60, 0.6)  # bucket < 100
    assert lib.total_count >= REGIME_ACTIVATION_TRADES
    assert lib.activated is True
    assert lib.regime_active(Regime.TRENDING_POS_NORMAL) is True   # 150 >= 100
    assert lib.regime_active(Regime.TRENDING_NEG_NORMAL) is False  # 60 < 100
    assert set(lib.active_regimes()) == {Regime.TRENDING_POS_NORMAL, Regime.NON_DIR_NORMAL}


def test_bucket_min_constant_is_100():
    assert REGIME_BUCKET_MIN == 100
    assert REGIME_ACTIVATION_TRADES == 600


# --------------------------------------------------------------------------- edge + disallowed
def test_regime_edge_is_kelly_full():
    lib = RegimeLibrary()
    _fill(lib, Regime.TRENDING_POS_NORMAL, 200, 0.6, gain=2, loss=1)  # W=0.6 R=2 -> K_full=0.4
    _fill(lib, Regime.NON_DIR_NORMAL, 410, 0.6)
    assert lib.regime_edge(Regime.TRENDING_POS_NORMAL) == Decimal("0.4")


def test_regime_edge_none_when_not_active():
    lib = RegimeLibrary()
    _fill(lib, Regime.TRENDING_POS_NORMAL, 50, 0.6)  # below floors
    assert lib.regime_edge(Regime.TRENDING_POS_NORMAL) is None


def test_disallowed_regimes_are_active_with_non_positive_edge():
    lib = RegimeLibrary()
    _fill(lib, Regime.TRENDING_POS_NORMAL, 200, 0.6, gain=2, loss=1)   # K_full +0.4
    _fill(lib, Regime.NON_DIR_NORMAL, 200, 0.3, gain=1, loss=1)        # W=0.3 R=1 -> K_full -0.4
    _fill(lib, Regime.TRENDING_NEG_NORMAL, 220, 0.6)                   # positive, reach 600 total
    assert lib.activated is True
    assert lib.disallowed_regimes() == [Regime.NON_DIR_NORMAL]


def test_under_100_bucket_never_disallowed():
    lib = RegimeLibrary()
    _fill(lib, Regime.NON_DIR_NORMAL, 600, 0.6)        # carries the total past 600
    _fill(lib, Regime.NON_DIR_ELEVATED, 40, 0.0, gain=1, loss=1)  # terrible edge but only 40 trades
    assert lib.regime_active(Regime.NON_DIR_ELEVATED) is False
    assert Regime.NON_DIR_ELEVATED not in lib.disallowed_regimes()
