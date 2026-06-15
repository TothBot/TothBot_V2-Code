"""mod:CIATS_Statistical_Engine tests (ciats/statistical_engine.py).

Covers the four detection-only statistics against known reference values: Mann-Whitney U (disjoint /
overlapping samples, the tie-corrected continuity-corrected z), Sharpe ratio, Spearman rho (perfect
monotone +/-1, tie-aware), and the one-sided LOWER CUSUM (k=0.5*sigma, h=4*sigma starting values;
detects a downward shift, ignores an upward one). Reference z / rho / U values verified vs scipy.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.ciats.statistical_engine import (
    CUSUM_H_SIGMA,
    CUSUM_K_SIGMA,
    cusum_lower,
    mann_whitney_u,
    mean,
    sharpe_ratio,
    spearman_rho,
    stdev,
)


def _approx(value, target, tol="0.0000001"):
    return abs(Decimal(str(value)) - Decimal(str(target))) <= Decimal(tol)


# --------------------------------------------------------------------------- mean / stdev
def test_mean_and_sample_stdev():
    assert mean([1, 2, 3, 4, 5]) == Decimal("3")
    # sample stdev of 1..5 = sqrt(10/4) = sqrt(2.5) = 1.58113883...
    assert _approx(stdev([1, 2, 3, 4, 5]), "1.5811388300841896")


def test_population_stdev():
    # population stdev of 1..5 = sqrt(10/5) = sqrt(2) = 1.41421356...
    assert _approx(stdev([1, 2, 3, 4, 5], sample=False), "1.4142135623730951")


def test_stdev_too_few_points_raises():
    with pytest.raises(ValueError):
        stdev([1])  # sample stdev needs >= 2


# --------------------------------------------------------------------------- Mann-Whitney U
def test_mann_whitney_disjoint_samples():
    # A entirely below B: U1 = 0, U2 = 16; z (continuity-corrected) = -2.1651 -> significant.
    r = mann_whitney_u([1, 2, 3, 4], [5, 6, 7, 8])
    assert r.u1 == Decimal("0")
    assert r.u2 == Decimal("16")
    assert r.u == Decimal("0")
    assert _approx(r.z, "-2.1650635", tol="0.0001")
    assert r.significant is True


def test_mann_whitney_overlapping_not_significant():
    # Interleaved samples: U1 = 6, U2 = 10, z = -0.433 -> not significant at 0.05.
    r = mann_whitney_u([1, 3, 5, 7], [2, 4, 6, 8])
    assert r.u1 == Decimal("6")
    assert r.u2 == Decimal("10")
    assert _approx(r.z, "-0.4330127", tol="0.0001")
    assert r.significant is False


def test_mann_whitney_u_sum_identity():
    # u1 + u2 == n1 * n2 always.
    r = mann_whitney_u([1, 2, 2, 9, 4], [3, 3, 5, 6])
    assert r.u1 + r.u2 == Decimal(5 * 4)


def test_mann_whitney_empty_raises():
    with pytest.raises(ValueError):
        mann_whitney_u([], [1, 2])


# --------------------------------------------------------------------------- Sharpe
def test_sharpe_ratio_known_series():
    # returns 1..5 % ; mean 0.03, sample stdev sqrt(0.00025) -> 1.8973666
    rs = [Decimal(x) for x in ["0.01", "0.02", "0.03", "0.04", "0.05"]]
    assert _approx(sharpe_ratio(rs), "1.8973665961010275", tol="0.000001")


def test_sharpe_with_risk_free_shifts_mean():
    rs = [Decimal("0.02"), Decimal("0.04"), Decimal("0.06")]
    # excess over 0.02 = [0, 0.02, 0.04]; mean 0.02, sample stdev 0.02 -> sharpe 1.0
    assert _approx(sharpe_ratio(rs, risk_free="0.02"), "1.0", tol="0.0000001")


def test_sharpe_zero_variance_raises():
    with pytest.raises(ValueError):
        sharpe_ratio([Decimal("0.03"), Decimal("0.03"), Decimal("0.03")])


# --------------------------------------------------------------------------- Spearman
def test_spearman_perfect_monotone():
    assert spearman_rho([1, 2, 3, 4, 5], [10, 20, 30, 40, 50]) == Decimal("1")
    assert spearman_rho([1, 2, 3, 4, 5], [50, 40, 30, 20, 10]) == Decimal("-1")


def test_spearman_with_ties_matches_reference():
    # x has a tie; scipy spearmanr([1,2,2,3],[1,2,3,4]) = 0.9486832980505138
    assert _approx(spearman_rho([1, 2, 2, 3], [1, 2, 3, 4]), "0.9486832980505138", tol="0.0000001")


def test_spearman_length_mismatch_raises():
    with pytest.raises(ValueError):
        spearman_rho([1, 2, 3], [1, 2])


def test_spearman_constant_series_raises():
    with pytest.raises(ValueError):
        spearman_rho([1, 1, 1, 1], [1, 2, 3, 4])


# --------------------------------------------------------------------------- CUSUM (lower arm)
def test_cusum_starting_values_are_diagram_constants():
    assert CUSUM_K_SIGMA == Decimal("0.5")
    assert CUSUM_H_SIGMA == Decimal("4")


def test_cusum_lower_detects_downward_shift_vs_baseline():
    # In-control net_gain ~10 (mu=10, sigma=1); then it collapses to 2 = a large downward shift.
    # K = 0.5, H = 4; each low point adds (10 - 0.5) - 2 = 7.5 > H -> breach on the first low point.
    series = [10, 10, 10, 2, 2, 2]
    r = cusum_lower(series, mu=10, sigma=1)
    assert r.k == Decimal("0.5") and r.h == Decimal("4")
    assert r.breached is True
    assert r.breach_index == 3  # the first below-baseline point


def test_cusum_lower_stable_series_no_breach():
    r = cusum_lower([10, 10, 10, 10, 10], mu=10, sigma=1)
    assert r.breached is False
    assert r.breach_index is None
    assert all(c == Decimal("0") for c in r.c_lower)


def test_cusum_lower_ignores_upward_shift():
    # The LOWER arm is loss-prevention only: an UPWARD shift (net_gain improving) never breaches.
    r = cusum_lower([10, 10, 10, 50, 50, 50], mu=10, sigma=1)
    assert r.breached is False


def test_cusum_empty_raises():
    with pytest.raises(ValueError):
        cusum_lower([])
