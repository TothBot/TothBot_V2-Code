"""mod:CIATS_Statistical_Engine - the post-200-trade significance/drift statistics (DETECTION ONLY).

Source: 0500000 dv1_250 sec 6/7 mod:CIATS_Statistical_Engine: "Methods CIATS uses: Mann-Whitney U,
Sharpe ratio, Spearman correlation, EWMA (lambda=0.2), CUSUM, Half-Kelly." This module is the four
significance/association/drift statistics (EWMA is ewma_monitor.py; Half-Kelly is pool.py):

  mann_whitney_u   - the nonparametric two-sample U test (with tie-corrected normal approximation):
                     does a parameter change shift the net-outcome distribution? (before vs after).
  sharpe_ratio     - mean/stdev of a return series (the risk-adjusted-performance metric).
  spearman_rho     - the rank correlation coefficient (tie-aware: Pearson over average ranks):
                     monotone association between two CIATS series (e.g. a param vs net_gain).
  cusum_lower      - the one-sided LOWER-arm CUSUM (k=0.5*sigma, h=4*sigma STARTING VALUES, the
                     diagram-named parameters): detect a DOWNWARD shift in the Stream-2 net_gain
                     series (degradation -> a mod:CIATS_PDCA_Engine PLAN-phase signal). Lower arm
                     ONLY - the loss-prevention-relevant direction (the figure's DIRECTION note).

ALL are DETECTION/MONITORING ONLY (D5): they observe + signal, they NEVER write a parameter (the
sacred 1:1.5 R:R is never tuned; exchange-defined params are never adjusted). Every statistic is
gated by the 200-trade HARD floor at the caller (pool.py CIATS_TRADE_FLOOR) - the engine itself is
pure math over the series handed in. PURE, Decimal-only (ar:AR-047); float never enters the math.

The CUSUM k=0.5*sigma / h=4*sigma are the diagram's STARTING VALUES (transcribed, like the EWMA
lambda=0.2 in ewma_monitor.py; value home TB00000 sec 8) - textbook CUSUM defaults, not invented
here. The Mann-Whitney / Spearman significance level (alpha) is a method choice supplied by the
caller (default 0.05), not a CIATS-owned seed.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from decimal import Decimal

# CUSUM starting values (the diagram-named parameters; value home TB00000 sec 8), in sigma units.
CUSUM_K_SIGMA = Decimal("0.5")   # the allowance K = 0.5 * sigma
CUSUM_H_SIGMA = Decimal("4")     # the decision interval H = 4 * sigma


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the statistics (ar:AR-047)."""
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _coerce(values: Sequence[object]) -> list[Decimal]:
    return [_dec(v) for v in values]


def mean(values: Sequence[object]) -> Decimal:
    """The arithmetic mean (raises on an empty series)."""
    xs = _coerce(values)
    if not xs:
        raise ValueError("mean of an empty series")
    return sum(xs, Decimal(0)) / len(xs)


def stdev(values: Sequence[object], *, sample: bool = True) -> Decimal:
    """The standard deviation: sample (ddof=1, default) or population (ddof=0). Decimal-exact sqrt.
    Raises on too few points (sample needs >= 2)."""
    xs = _coerce(values)
    n = len(xs)
    ddof = 1 if sample else 0
    if n - ddof <= 0:
        raise ValueError("stdev needs more data points")
    mu = sum(xs, Decimal(0)) / n
    var = sum(((x - mu) * (x - mu) for x in xs), Decimal(0)) / (n - ddof)
    return var.sqrt()


def _average_ranks(values: Sequence[Decimal]) -> list[Decimal]:
    """The 1-based average ranks of `values` (ties share the mean of the ranks they span). The
    rank vector both Mann-Whitney U and Spearman consume."""
    order = sorted(range(len(values)), key=lambda i: values[i])
    ranks = [Decimal(0)] * len(values)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and values[order[j + 1]] == values[order[i]]:
            j += 1
        # positions i..j (0-based) are tied -> share the average 1-based rank.
        avg = Decimal((i + 1) + (j + 1)) / 2
        for k in range(i, j + 1):
            ranks[order[k]] = avg
        i = j + 1
    return ranks


def _tie_correction(values: Sequence[Decimal]) -> Decimal:
    """sum(t^3 - t) over the tie groups of `values` (the Mann-Whitney variance tie term)."""
    counts: dict[Decimal, int] = {}
    for v in values:
        counts[v] = counts.get(v, 0) + 1
    total = Decimal(0)
    for t in counts.values():
        td = Decimal(t)
        total += td * td * td - td
    return total


@dataclass(frozen=True)
class MannWhitneyResult:
    """The two-sample Mann-Whitney U test. u1/u2 are the per-sample U statistics (u1 + u2 = n1*n2);
    u is min(u1, u2); z is the tie-corrected continuity-corrected normal approximation of u1 (sign:
    z < 0 when sample A ranks LOWER than B). significant flags |z| beyond the alpha two-sided z."""

    u1: Decimal
    u2: Decimal
    u: Decimal
    z: Decimal
    significant: bool


# Two-sided normal critical z for the common alpha levels (method constants, not CIATS seeds).
_Z_CRIT: dict[str, Decimal] = {"0.05": Decimal("1.959964"), "0.01": Decimal("2.575829"),
                               "0.10": Decimal("1.644854")}


def mann_whitney_u(
    sample_a: Sequence[object], sample_b: Sequence[object], *, alpha: object = Decimal("0.05")
) -> MannWhitneyResult:
    """The Mann-Whitney U statistic for samples A vs B + the tie-corrected normal-approximation z.

    Ranks the pooled sample (average ranks for ties), takes R1 = sum of A's ranks, U1 = R1 -
    n1(n1+1)/2, U2 = n1*n2 - U1. z applies the mean n1*n2/2, the tie-corrected variance, and a 0.5
    continuity correction toward the mean. `significant` compares |z| to the two-sided critical z
    for `alpha` (0.05 / 0.01 / 0.10 supported; default 0.05). Needs both samples non-empty."""
    a, b = _coerce(sample_a), _coerce(sample_b)
    n1, n2 = len(a), len(b)
    if n1 == 0 or n2 == 0:
        raise ValueError("mann_whitney_u needs both samples non-empty")
    pooled = a + b
    ranks = _average_ranks(pooled)
    r1 = sum(ranks[:n1], Decimal(0))
    u1 = r1 - Decimal(n1 * (n1 + 1)) / 2
    u2 = Decimal(n1 * n2) - u1
    u = min(u1, u2)

    n = n1 + n2
    mu_u = Decimal(n1 * n2) / 2
    tie = _tie_correction(pooled)
    # sigma^2 = (n1*n2/12) * ((n+1) - tie/(n*(n-1)))
    var_u = (
        Decimal(n1 * n2) / 12
        * (Decimal(n + 1) - tie / Decimal(n * (n - 1)))
    ) if n > 1 else Decimal(0)
    if var_u <= 0:
        z = Decimal(0)
    else:
        diff = u1 - mu_u
        # continuity correction: shrink |diff| by 0.5 toward 0.
        if diff > 0:
            diff -= Decimal("0.5")
            if diff < 0:
                diff = Decimal(0)
        elif diff < 0:
            diff += Decimal("0.5")
            if diff > 0:
                diff = Decimal(0)
        z = diff / var_u.sqrt()

    z_crit = _Z_CRIT.get(str(_dec(alpha)), _Z_CRIT["0.05"])
    return MannWhitneyResult(u1=u1, u2=u2, u=u, z=z, significant=abs(z) > z_crit)


def sharpe_ratio(
    returns: Sequence[object], *, risk_free: object = Decimal(0), sample: bool = True
) -> Decimal:
    """The Sharpe ratio of a return series: mean(excess) / stdev(excess), excess = return -
    risk_free (per-period; no annualization - the caller scales if needed). sample stdev (ddof=1)
    by default. Raises on < 2 points or a zero-variance (degenerate) series."""
    rf = _dec(risk_free)
    excess = [r - rf for r in _coerce(returns)]
    sd = stdev(excess, sample=sample)
    if sd == 0:
        raise ValueError("sharpe_ratio is undefined for a zero-variance series")
    return mean(excess) / sd


def spearman_rho(x: Sequence[object], y: Sequence[object]) -> Decimal:
    """Spearman's rank correlation rho between x and y: Pearson correlation over the AVERAGE ranks
    (tie-aware - the general definition, valid with ties, not the 1 - 6 sum d^2 shortcut). Returns
    a value in [-1, 1]. Raises on length mismatch, < 2 points, or a constant series (no variance)."""
    xs, ys = _coerce(x), _coerce(y)
    if len(xs) != len(ys):
        raise ValueError("spearman_rho needs equal-length series")
    if len(xs) < 2:
        raise ValueError("spearman_rho needs at least 2 points")
    rx, ry = _average_ranks(xs), _average_ranks(ys)
    mx, my = mean(rx), mean(ry)
    cov = sum(((a - mx) * (b - my) for a, b in zip(rx, ry)), Decimal(0))
    vx = sum(((a - mx) * (a - mx) for a in rx), Decimal(0))
    vy = sum(((b - my) * (b - my) for b in ry), Decimal(0))
    if vx == 0 or vy == 0:
        raise ValueError("spearman_rho is undefined for a constant series")
    return cov / (vx.sqrt() * vy.sqrt())


@dataclass(frozen=True)
class CusumResult:
    """The one-sided LOWER-arm CUSUM run (loss-prevention direction). c_lower is the cumulative-sum
    series; breached is True once it exceeds h; breach_index is the first crossing index (or None).
    k / h are the resolved allowance + decision interval (in the series' own units)."""

    c_lower: tuple[Decimal, ...]
    breached: bool
    breach_index: int | None
    k: Decimal
    h: Decimal


def cusum_lower(
    series: Sequence[object],
    *,
    mu: object | None = None,
    sigma: object | None = None,
    k_sigma: object = CUSUM_K_SIGMA,
    h_sigma: object = CUSUM_H_SIGMA,
) -> CusumResult:
    """The one-sided LOWER CUSUM detecting a DOWNWARD shift below the in-control mean (the net_gain
    degradation detector). C-_t = max(0, (mu - K) - x_t + C-_{t-1}); SIGNAL when C-_t > H, with
    K = k_sigma*sigma, H = h_sigma*sigma (the diagram's 0.5*sigma / 4*sigma starting values).

    mu / sigma default to the series' own mean + sample stdev (the in-control estimate); pass them
    to monitor against a fixed pre-floor baseline. Needs >= 2 points when sigma is estimated."""
    xs = _coerce(series)
    if not xs:
        raise ValueError("cusum_lower needs a non-empty series")
    mu_d = mean(xs) if mu is None else _dec(mu)
    sigma_d = stdev(xs) if sigma is None else _dec(sigma)
    k = _dec(k_sigma) * sigma_d
    h = _dec(h_sigma) * sigma_d

    c_lower: list[Decimal] = []
    prev = Decimal(0)
    breach_index: int | None = None
    for i, x in enumerate(xs):
        cur = (mu_d - k) - x + prev
        if cur < 0:
            cur = Decimal(0)
        c_lower.append(cur)
        if breach_index is None and cur > h:
            breach_index = i
        prev = cur
    return CusumResult(
        c_lower=tuple(c_lower),
        breached=breach_index is not None,
        breach_index=breach_index,
        k=k,
        h=h,
    )
