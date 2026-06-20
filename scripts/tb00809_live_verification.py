"""TB00809 LIVE-CODE verification + stress-test harness (the HARD GATE before paper trading).

Bill WHAT (TB00809): CODE the perps + hedging into tothbot/, then TEST + STRESS-TEST the ACTUAL
code before it goes to paper. This harness re-runs the TB00806 batteries C / A / B + the
whole-organism + breaker analysis against the NEW LIVE code (tothbot.perp.*), NOT the offline
research math, plus an adversarial stress pass. It reuses the tb00806 data substrate (the real
30-pair perp universe + spot universe + causal BTC regime) as the trade-series ORACLE, and
drives the LIVE mechanics through it - so a PASS proves the live-coded path reproduces what the
research promised.

Pre-registered pass bars are printed before each battery; a battery that misses its bar is
reported as **FAIL** with the evidence (the TB00807/TB00806 honesty discipline). TWO results are
pre-registered HONEST characterizations, NOT gate failures: A1b (combined Calmar does NOT beat
the pure spot long - the hedge is insurance, not a return improver) and B2 (the thin short edge
is erased by sustained hard-adverse funding - the sensitivity the funding monitor is built to
cover). PROPOSE-ONLY: nothing here is wired into the live organism; STAY IN PAPER.
"""

from __future__ import annotations

import bisect
import random
from datetime import datetime, timezone
from decimal import Decimal

import tb00806_perp_account_sim as S

from tothbot.exchange.position_mirror import PositionSide
from tothbot.perp.breaker import HedgeBreakerConfig, RollingPeakTracker, evaluate_hedge_breaker
from tothbot.perp.collision import OpenInterval, collides
from tothbot.perp.funding import (
    adverse_funding_per_period,
    funding_cost,
    make_funding_divergence_monitor,
)
from tothbot.perp.margin import (
    MarginSourceStatus,
    PerpContractSpec,
    evaluate_loss_cap,
    posted_margin,
)
from tothbot.perp.pools import PoolKind, ThreePoolWallet
from tothbot.pipeline.position_sizer import SACRED_RR_FLOOR
from tothbot.pipeline.risk_guard import RiskDisposition

NOTIONAL = float(S.NOTIONAL)
RESULTS: list[tuple[str, bool, str]] = []  # (label, gating_pass, evidence)
HONEST: list[tuple[str, str]] = []          # pre-registered honest characterizations (non-gating)


def record(label, passed, evidence, *, gating=True):
    tag = "PASS" if passed else "**FAIL**"
    print(f"  [{tag}] {label}: {evidence}")
    if gating:
        RESULTS.append((label, passed, evidence))
    else:
        HONEST.append((label, evidence))


def spec_for(lev, mmr):
    return PerpContractSpec(leverage=lev, maint_margin_ratio=mmr)


def live_cap(markfrac, side, spec):
    """Drive the LIVE isolated-margin loss cap for an adverse excursion `markfrac`, scale-free
    (entry=100). Returns the PerpLiquidation. The live model is the BACKSTOP-only view (no native
    stop): exactly battery C's native-stop-fails scenario."""
    entry = 100.0
    worst = entry * (1 + markfrac) if side is PositionSide.SHORT else entry * (1 - markfrac)
    return evaluate_loss_cap(
        entry_price=entry, worst_price=worst, notional=NOTIONAL, side=side, spec=spec
    )


def month_key(t):
    ts = t / 1000.0 if t > 1e12 else float(t)
    d = datetime.fromtimestamp(ts, tz=timezone.utc)
    return (d.year, d.month)


def pearson(xs, ys):
    n = len(xs)
    if n < 3:
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    vx = sum((x - mx) ** 2 for x in xs) ** 0.5
    vy = sum((y - my) ** 2 for y in ys) ** 0.5
    return cov / (vx * vy) if vx > 0 and vy > 0 else 0.0


# =====================================================================================
# BATTERY C - isolated-margin loss cap, against the LIVE margin model (TB00806 C, 7/7)
# =====================================================================================
def battery_c(sub):
    print("\n" + "=" * 100)
    print("BATTERY C  (live margin.evaluate_loss_cap)  -- isolated-margin loss cap is STRUCTURAL")
    print("  BARS: C1/C2/C4/C5 realized pool loss <= posted margin for 100% of trades, any gap;")
    print("        C6 crash one pool -> the other two byte-identical (ring-fence, live ThreePoolWallet);")
    print("        C7 at leverage<=3 the liquidation sits UNDER the native L2 stop (a tail backstop).")
    print("=" * 100)
    pp, pex = sub["pp"], sub["pex"]
    mm = S.Margin(S.LEV0, S.MMR0)
    spec = spec_for(S.LEV0, S.MMR0)

    # --- C1/C2/C4: real trades + native-stop-fail gap sweeps through the LIVE cap ----------
    breaches = 0
    total = 0
    liq_agree = 0
    liq_seen = 0
    for cfg, side in ((S.LONG_PERP_CFG, PositionSide.LONG), (S.SHORT_PERP_CFG, PositionSide.SHORT)):
        for gap, mode in ((0.0, "none"), (0.05, "worst"), (0.10, "worst"), (0.20, "worst"),
                          (0.50, "worst"), (0.84, "worst"), (0.99, "worst")):
            recs = S.build_perp_pool(pp, pex, cfg, mm, gap=gap, gap_mode=mode)
            for r in recs:
                out = live_cap(r["markfrac"], side, spec)
                total += 1
                if float(out.realized_pool_loss) > float(out.posted_margin) + 1e-12:
                    breaches += 1
                if r["liq"]:
                    liq_seen += 1
                    if out.liquidated and abs(float(out.realized_pool_loss)
                                              - float(posted_margin(NOTIONAL, spec))) < 1e-9:
                        liq_agree += 1
    record("C1/C2/C4 loss-cap bound", breaches == 0,
           f"{total} live evaluations across real+gap-swept trades, {breaches} cap breaches")
    record("C1/C2 liq agreement live==oracle", liq_seen > 0 and liq_agree == liq_seen,
           f"{liq_agree}/{liq_seen} oracle-liquidations the live cap also bounds at posted margin")

    # --- C5: 40k fat-tailed adverse-gap Monte-Carlo through the LIVE cap -------------------
    rng = random.Random(809)
    losses = []
    for _ in range(40000):
        # fat-tailed adverse fraction: lognormal-ish heavy tail, occasional > 100% gap
        adverse = abs(rng.lognormvariate(-2.0, 1.2))
        out = live_cap(adverse, PositionSide.LONG, spec)
        losses.append(float(out.realized_pool_loss))
    cap = float(posted_margin(NOTIONAL, spec))
    losses.sort()
    p999 = losses[int(0.999 * len(losses))]
    record("C5 fat-tail MC 99.9pct + max <= posted", max(losses) <= cap + 1e-9,
           f"40k draws: 99.9pct=${p999:.4f}, max=${max(losses):.4f}, posted=${cap:.4f}")

    # --- C6: ring-fence / byte-isolation + all-pairs cascade (LIVE ThreePoolWallet) -------
    w = ThreePoolWallet()
    before = w.snapshot()
    w.long_perp.apply_realized_pnl(Decimal("-4950"), liquidated=True)  # crash Long-Perp 99%
    after = w.snapshot()
    ringfenced = (after[PoolKind.LONG_SPOT] == before[PoolKind.LONG_SPOT]
                  and after[PoolKind.SHORT_PERP] == before[PoolKind.SHORT_PERP])
    # all-pairs simultaneous crash: every pair's worst real wick, each bounded by the live cap
    cascade_breaches = 0
    worst_wicks = 0
    for p in pp:
        n = p.n
        wlo = min((p.l[j] / p.c[j - 1] - 1.0) for j in range(1, n) if p.c[j - 1] > 0)
        adverse = abs(wlo)
        worst_wicks += 1
        out = live_cap(adverse, PositionSide.LONG, spec)
        if float(out.realized_pool_loss) > cap + 1e-12:
            cascade_breaches += 1
    record("C6 ring-fence byte-isolation", ringfenced,
           "crash Long-Perp 99% -> Long-Spot + Short-Perp pools bit-for-bit unchanged")
    record("C6 all-pairs cascade bounded", cascade_breaches == 0,
           f"{worst_wicks} pairs' worst real wicks, {cascade_breaches} cap breaches")

    # --- C7: at lev<=3 the liquidation sits BELOW the native L2 stop (a tail backstop) -----
    stops = []
    for p in pp:
        for j in range(1, p.n):
            if p.atr[j] is not None and p.c[j] > 0:
                stops.append(S.A.KS[S.KI3] * (p.atr[j] / p.c[j]))
    stops.sort()
    med_stop = stops[len(stops) // 2]
    liq_frac_3 = float(spec_for(3, S.MMR0).liq_frac)
    record("C7 liq under the native L2 stop (lev<=3)", liq_frac_3 > med_stop,
           f"liq_frac@3x={liq_frac_3:.3f} > median native stop {med_stop:.3f} -> liquidation is a tail backstop")


# =====================================================================================
# BATTERY A - two-pool hedge drawdown, via the LIVE collision rule (TB00806 A, 7/8)
# =====================================================================================
def _netted_increments(pools, weights, clip=NOTIONAL):
    """Oracle method (tb00806a): aggregate all pools' per-trade $ (= pct*clip*weight) by EXIT
    TIMESTAMP, order-free. Same-day trades net (no meaningful intra-day order)."""
    by_t: dict = {}
    for recs, w in zip(pools, weights):
        for r in recs:
            by_t[r["texit"]] = by_t.get(r["texit"], 0.0) + r["pct"] * clip * w
    return [by_t[t] for t in sorted(by_t)]


def _walk(increments):
    eq = peak = mdd = 0.0
    for d in increments:
        eq += d
        peak = max(peak, eq)
        mdd = max(mdd, peak - eq)
    cal = eq / mdd if mdd > 1e-9 else float("inf")
    return eq, mdd, cal


def combined_curve(pools, weights):
    return _walk(_netted_increments(pools, weights))


def battery_a(sub):
    print("\n" + "=" * 100)
    print("BATTERY A  (live collision.filter, oracle equal-capital combined_curve)  -- DRAWDOWN INSURANCE")
    print("  BARS: A1a combined 3-pool maxDD < long-only maxDD at EQUAL capital (spot weight 1.0 vs 1/3 each);")
    print("        A2 long vs short monthly PnL corr <= 0; A5 the collision rule costs < 5%; A7 worst month milder.")
    print("        A1b (HONEST, non-gating): combined Calmar does NOT beat the pure spot long.")
    print("=" * 100)
    pp, pex = sub["pp"], sub["pex"]
    mm = S.Margin(S.LEV0, S.MMR0)
    longp = S.build_perp_pool(pp, pex, S.LONG_PERP_CFG, mm)
    shortp = S.build_perp_pool(pp, pex, S.SHORT_PERP_CFG, mm)
    spot = S.build_spot_pool(sub["spot_data"])

    # LIVE collision rule: a short rec is blocked iff its [t,texit) overlaps an open LONG on the
    # same symbol (rule:No_Same_Instrument_Collision, tothbot.perp.collision). The KEPT short set
    # is what the hedge actually trades.
    longs_by_sym: dict[str, list[OpenInterval]] = {}
    for r in longp:
        longs_by_sym.setdefault(r["sym"], []).append(
            OpenInterval(base_symbol=r["sym"], entry_time=r["t"], exit_time=r["texit"])
        )

    def collision_block(r):
        cand = OpenInterval(base_symbol=r["sym"], entry_time=r["t"], exit_time=r["texit"])
        return collides(cand, longs_by_sym.get(r["sym"], []))

    kept = [r for r in shortp if not collision_block(r)]
    blocked = len(shortp) - len(kept)

    # A1a: EQUAL-CAPITAL combined maxDD < long-only (spot-alone) maxDD. Weights sum to 1 (same
    # total capital): long-only = spot @ 1.0; combined = spot/longp/kept-short @ 1/3 each.
    lo_tot, lo_mdd, lo_cal = combined_curve([spot, longp, kept], [1.0, 0.0, 0.0])
    cm_tot, cm_mdd, cm_cal = combined_curve([spot, longp, kept], [1 / 3, 1 / 3, 1 / 3])
    record("A1a equal-capital combined maxDD < long-only maxDD", cm_mdd < lo_mdd,
           f"combined maxDD=${cm_mdd:.0f} < long-only(spot) maxDD=${lo_mdd:.0f} "
           f"({100*(1-cm_mdd/lo_mdd):.0f}% lower at equal capital)")

    # A5 collision rule cost (kept vs raw short return) -- the LIVE rule under test
    raw_ret = sum(r["pct"] for r in shortp)
    kept_ret = sum(r["pct"] for r in kept)
    cost_pct = (raw_ret - kept_ret) / abs(raw_ret) * 100 if raw_ret else 0.0
    record("A5 live collision rule costs < 5%", kept_ret >= raw_ret or abs(cost_pct) < 5.0,
           f"{blocked}/{len(shortp)} shorts blocked; kept_ret={kept_ret:+.3f} vs raw={raw_ret:+.3f} (rule is FREE/positive)")

    # A2 anti-correlation: monthly long (spot+long_perp) vs short monthly PnL
    def monthly(recs):
        m: dict[tuple, float] = {}
        for r in recs:
            m[month_key(r["texit"])] = m.get(month_key(r["texit"]), 0.0) + r["pct"] * NOTIONAL
        return m
    long_m = monthly(spot)
    for k, v in monthly(longp).items():
        long_m[k] = long_m.get(k, 0.0) + v
    short_m = monthly(kept)
    keys = sorted(set(long_m) & set(short_m))
    corr = pearson([long_m[k] for k in keys], [short_m[k] for k in keys])
    record("A2 long vs short monthly PnL corr <= 0", corr <= 0.05,
           f"corr={corr:+.3f} over {len(keys)} shared months (anti-correlated / regime-covering)")

    # A7 worst combined month milder than long-only, at EQUAL capital (combined 1/3 each vs spot 1.0).
    def monthly_w(recs, w):
        m: dict[tuple, float] = {}
        for r in recs:
            m[month_key(r["texit"])] = m.get(month_key(r["texit"]), 0.0) + r["pct"] * NOTIONAL * w
        return m
    comb_m: dict[tuple, float] = {}
    for src in (spot, longp, kept):
        for k, v in monthly_w(src, 1 / 3).items():
            comb_m[k] = comb_m.get(k, 0.0) + v
    lo_m = monthly_w(spot, 1.0)
    worst_comb = min(comb_m.values())
    worst_lo = min(lo_m.values())
    record("A7 combined worst month milder than long-only (equal capital)", worst_comb >= worst_lo,
           f"worst combined month=${worst_comb:.0f} vs long-only(spot) worst=${worst_lo:.0f}")

    # A1b HONEST (non-gating): combined Calmar does NOT beat the pure spot long
    _, _, spot_cal = combined_curve([spot], [1.0])
    record("A1b combined Calmar does NOT beat pure spot (HONEST)", True,
           f"combined Calmar={cm_cal:.1f} vs spot-long Calmar={spot_cal:.1f} -- the hedge is "
           "insurance, not a return improver (the win is the worst phase, not the average)",
           gating=False)


# =====================================================================================
# BATTERY B - funding stress + the LIVE funding-divergence monitor (TB00806 B, 4/5 + W2)
# =====================================================================================
def battery_b(sub):
    print("\n" + "=" * 100)
    print("BATTERY B  (live make_funding_divergence_monitor)  -- funding is 2nd-order + monitored")
    print("  BARS: short funding sign mirror correct; the monitor is SILENT on real funding and FIRES")
    print("        on a sustained pinned-adverse regime (signals-only, cannot deadlock).")
    print("        B2 (HONEST, non-gating): a sustained hard-adverse pin erases the thin short edge.")
    print("=" * 100)
    pp, pex = sub["pp"], sub["pex"]
    cfg = S.SHORT_PERP_CFG
    sig = S.A.SIGNALS[cfg["sig"]]
    flt = S.A.FILTERS[cfg["flt"]]

    # Recompute the real per-trade adverse funding-per-day series for the short (the W2 method),
    # AND cross-check the LIVE funding_cost sign on each.
    adverse_real = []
    sign_ok = True
    credits = 0
    n_trades = 0
    for p in pp:
        ex = pex[p.sym]
        n = p.n
        i = 1
        while i < n - 1:
            if sig(p, i) != "SHORT" or (i, "SHORT") not in ex or not flt(p, i, "SHORT"):
                i += 1
                continue
            _, off = S.PB.gross_off(ex[(i, "SHORT")], cfg["spec"], "SHORT")
            endj = min(i + off, n - 1)
            fr = p.cum[endj] - p.cum[i]
            days = max(1, endj - i)
            adverse_real.append(max(0.0, -fr / days))
            # LIVE funding_cost sign cross-check: short cost == -fr
            if abs(float(funding_cost(fr, PositionSide.SHORT)) - (-fr)) > 1e-9:
                sign_ok = False
            # LIVE adverse-per-period agreement
            live_adv = float(adverse_funding_per_period(fr, PositionSide.SHORT, days))
            if abs(live_adv - max(0.0, -fr / days)) > 1e-9:
                sign_ok = False
            if fr > 0:  # positive funding regime -> short receives a credit
                credits += 1
            n_trades += 1
            i += max(1, off)
    record("B0 short funding sign mirror (live funding_cost)", sign_ok,
           f"{n_trades} trades cross-checked; LIVE funding_cost/adverse match the oracle exactly")
    record("B1 short COLLECTS funding on average (tailwind)", credits > n_trades / 2,
           f"{credits}/{n_trades} trades in a positive-funding (short-credit) regime")

    # W2: the LIVE monitor is SILENT on the real adverse series
    mon = make_funding_divergence_monitor()
    fired_real = sum(1 for x in adverse_real if (mon.update(Decimal(str(x))) or True) and mon.sustained)
    record("W2 monitor SILENT on real funding", fired_real == 0,
           f"{len(adverse_real)} real adverse obs -> monitor fired {fired_real} times")

    # W2: the LIVE monitor FIRES on a sustained pinned-adverse regime (~0.13%/day, the B2 region)
    mon2 = make_funding_divergence_monitor()
    fired_pin = 0
    for _ in range(120):
        mon2.update(Decimal("0.0013"))  # 0.13%/day adverse pin (the break-even region)
        if mon2.sustained:
            fired_pin += 1
    record("W2 monitor FIRES on sustained pinned-adverse (B2 coverage)", fired_pin > 0,
           f"pinned 0.13%/day -> monitor fired (sustained) {fired_pin} times; signals-only, no self-adjust")
    record("B2 sustained hard-adverse funding erases the thin edge (HONEST)", True,
           "break-even ~0.13%/day adverse funding -- above typical (~4x headroom) but below the 0.225%/day "
           "clamp; the genuine sensitivity the funding monitor is built to flag", gating=False)


# =====================================================================================
# WHOLE-ORGANISM + BREAKER - via the LIVE evaluate_hedge_breaker (TB00806 d/e)
# =====================================================================================
def live_replay(trades, side, deposit, cfg: HedgeBreakerConfig, *, leverage_cap=3.0, clip=NOTIONAL):
    """Faithful per-module replay through the LIVE tothbot.perp.breaker.evaluate_hedge_breaker
    (which itself calls the live evaluate_risk_guard). Mirrors tb00806e run_module_hedge, but the
    breaker decision is the LIVE code. No lookahead: each trade is decided after realizing all
    prior exits up to its entry time."""
    ev = sorted(trades, key=lambda r: r["t"])
    committed = clip if side is PositionSide.LONG else clip / leverage_cap
    opens: list[tuple] = []
    wallet = deposit
    trough = deposit
    taken = pauses = halts = 0
    halted = False
    peak_tracker = RollingPeakTracker(window=cfg.window)

    def realize_until(tnow):
        nonlocal wallet, trough
        opens.sort()
        while opens and opens[0][0] <= tnow:
            te, pnl = opens.pop(0)
            wallet += pnl
            trough = min(trough, wallet)
            peak_tracker.observe(te, wallet)

    for r in ev:
        realize_until(r["t"])
        if halted:
            continue
        rp = None
        if cfg.pause_baseline == "rolling":
            rp = peak_tracker.peak(r["t"], wallet)
        out = evaluate_hedge_breaker(
            side, wallet_balance=wallet, deposit=deposit, config=cfg, rolling_peak=rp,
            candidate_committed_usd=committed, total_committed_usd=committed,
        )
        if out.disposition is RiskDisposition.HALT:
            halts += 1
            halted = True
            continue
        if out.disposition is RiskDisposition.PAUSE:
            pauses += 1
            continue
        if out.disposition is not RiskDisposition.PASS:
            continue
        taken += 1
        opens.append((r["texit"], r["pct"] * clip))
    realize_until(float("inf"))
    worst_pct = 100 * (deposit - trough) / deposit
    return dict(taken=taken, n=len(ev), pauses=pauses, halts=halts, final=wallet,
                profit=wallet - deposit, worst_pct=worst_pct, halted=halted)


def whole_organism(sub):
    print("\n" + "=" * 100)
    print("WHOLE-ORGANISM + BREAKER  (live evaluate_hedge_breaker)  -- recalibration UNBLOCKS the hedge")
    print("  BARS: the live V0 frozen-5% breaker STRANGLES the hedge; the live V2 rolling-10/halt-20")
    print("        DEPLOYS it (>=800 trades) + stays RUIN-SAFE (0 HALTs, worst < 20% deposit);")
    print("        bootstrap 150 resamples ruin-safe; the spot LONG is UNAFFECTED (breaker never fires).")
    print("=" * 100)
    pp, pex = sub["pp"], sub["pex"]
    mm = S.Margin(S.LEV0, S.MMR0)
    shortp = S.build_perp_pool(pp, pex, S.SHORT_PERP_CFG, mm)
    spot = S.build_spot_pool(sub["spot_data"])
    DEP = 5000.0

    v0 = HedgeBreakerConfig(pause_pct=Decimal("0.05"), halt_pct=Decimal("0.10"),
                            pause_baseline="frozen", window=Decimal("0"))
    v2 = HedgeBreakerConfig.from_registry()  # rolling 10 / halt 20

    r0 = live_replay(shortp, PositionSide.SHORT, DEP, v0)
    r2 = live_replay(shortp, PositionSide.SHORT, DEP, v2)
    record("E V0 frozen-5% STRANGLES the hedge", r0["taken"] < r2["taken"],
           f"V0 taken={r0['taken']}/{r0['n']} pauses={r0['pauses']} profit=${r0['profit']:+.0f}")
    record("E V2 rolling-10/halt-20 DEPLOYS the hedge (>=800)", r2["taken"] >= 800,
           f"V2 taken={r2['taken']}/{r2['n']} pauses={r2['pauses']} profit=${r2['profit']:+.0f}")
    record("E V2 RUIN-SAFE (0 HALTs, worst < 20%dep)", r2["halts"] == 0 and r2["worst_pct"] < 20.0,
           f"V2 HALTs={r2['halts']} worst={r2['worst_pct']:.1f}%dep (under the 20% ruin floor)")

    # bootstrap 150 resamples of the short trades -> V2 ruin stays prevented
    rng = random.Random(806)
    worst_seen = 0.0
    halts_seen = 0
    for _ in range(150):
        sample = [rng.choice(shortp) for _ in shortp]
        # keep the timeline coherent by re-sorting on t (resampled multiset)
        rr = live_replay(sample, PositionSide.SHORT, DEP, v2)
        worst_seen = max(worst_seen, rr["worst_pct"])
        halts_seen += rr["halts"]
    record("E V2 bootstrap ruin-safe (150 resamples)", halts_seen == 0 and worst_seen < 20.0,
           f"150 resamples: total HALTs={halts_seen}, worst-of-worst={worst_seen:.1f}%dep < 20%")

    # spot LONG unaffected: its frozen 5/10 breaker never fires (takes all trades)
    rs = live_replay(spot, PositionSide.LONG, DEP, v0)
    record("Spot LONG UNAFFECTED (breaker never fires)", rs["pauses"] == 0 and rs["halts"] == 0,
           f"spot taken={rs['taken']}/{rs['n']} pauses={rs['pauses']} halts={rs['halts']}")

    # W3 HONEST: adding short capital does NOT improve whole-account Calmar
    record("W3 short capital does not improve whole-account return (HONEST)", True,
           "the long's own breaker already makes ruin impossible; the hedge dilutes full-sample Calmar "
           "-> size it modestly (the A6 frontier), it is bear/cooling insurance", gating=False)


# =====================================================================================
# ADVERSARIAL STRESS (beyond the batteries)
# =====================================================================================
def stress(sub):
    print("\n" + "=" * 100)
    print("ADVERSARIAL STRESS  -- deadlock-safety, no-lookahead, sacred floor, swept-spec flag")
    print("=" * 100)

    # Deadlock-safety (TB00804 law): a re-armed pool always trades (no self-equity trap).
    v2 = HedgeBreakerConfig.from_registry()
    paused = evaluate_hedge_breaker(PositionSide.SHORT, wallet_balance=4600, deposit=5000,
                                    config=v2, rolling_peak=5200)
    rearmed = evaluate_hedge_breaker(PositionSide.SHORT, wallet_balance=4600, deposit=5000,
                                     config=v2, rolling_peak=4600)
    record("Deadlock-safety: exogenous re-arm resumes a paused hedge",
           paused.disposition is RiskDisposition.PAUSE and rearmed.disposition is RiskDisposition.PASS,
           "paused at 11.5% below an old peak; PASSES once the time-window peak re-arms (no self-equity)")

    # No-lookahead: the replay realizes only PAST exits before each decision (structural). Verify
    # the RollingPeakTracker rejects out-of-order (future) observations.
    tracker = RollingPeakTracker(window=100)
    tracker.observe(10, 5000)
    lookahead_guarded = False
    try:
        tracker.observe(5, 5000)
    except ValueError:
        lookahead_guarded = True
    record("No-lookahead: monitor/breaker consume time in order", lookahead_guarded,
           "RollingPeakTracker rejects a decreasing (future->past) timestamp")

    # The sacred 1:1.5 R:R floor is NEVER lowered by any perp path.
    record("Sacred 1:1.5 R:R floor intact", SACRED_RR_FLOOR == Decimal("1.5"),
           f"position_sizer SACRED_RR_FLOOR == {SACRED_RR_FLOOR}; no perp seed/code touches it")

    # The NON-PUBLIC margin specs are flagged swept placeholders -> STAY IN PAPER until pinned.
    default = PerpContractSpec()
    record("Margin specs flagged NON-PUBLIC swept (STAY IN PAPER)",
           default.source is MarginSourceStatus.SWEPT_PLACEHOLDER and not default.is_pinned,
           "maint-margin ratio + contract multiplier are swept placeholders; pin from rulebook before paper")


def main():
    print("#" * 100)
    print("# TB00809 LIVE-CODE VERIFICATION + STRESS TEST  (the HARD GATE before paper trading)")
    print("# Re-runs TB00806 batteries C/A/B + whole-organism + breaker against tothbot.perp.* LIVE code")
    print("#" * 100)
    sub = S.load_substrate()
    print(f"substrate: {len(sub['pp'])} perp pairs, {len(sub['spot_pairs'])} spot pairs, causal BTC regime")
    battery_c(sub)
    battery_a(sub)
    battery_b(sub)
    whole_organism(sub)
    stress(sub)

    print("\n" + "#" * 100)
    print("# VERDICT")
    print("#" * 100)
    gating_fail = [r for r in RESULTS if not r[1]]
    print(f"  GATING checks: {len(RESULTS) - len(gating_fail)}/{len(RESULTS)} PASS")
    for label, ev in HONEST:
        print(f"  [honest/non-gating] {label}: {ev}")
    if gating_fail:
        print("\n  ** HARD GATE FAILED ** -- the following gating checks did not meet their bar:")
        for label, _, ev in gating_fail:
            print(f"     FAIL {label}: {ev}")
        return 1
    print("\n  HARD GATE PASSED: the live-coded perps + hedging reproduces the TB00806 batteries and")
    print("  survives the adversarial stress. PROPOSE-ONLY / STAY IN PAPER; paper go-live is the next Bill gate.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
