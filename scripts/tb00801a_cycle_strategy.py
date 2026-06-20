"""TB00801a - HOW DOES THE HALVING BULL/BEAR CYCLE ENTER THE STRATEGY?  (research/design, PAPER only)

Bill's WHAT (TB00801): the 4yr halving cycle is real on our own data (TB00798b). Explore - via First
Principles / Deming + all appropriate stats on the cached perp history - the BEST way to turn it into an
edge: make more money AND reduce losses. Bill's seed idea (TEST, do not override): in a BULL a SHORT
signal must be INCREDIBLY STRONG + ready for a quick reversal; in a BEAR the same for LONGs.

THE FATAL FLAW IN TB00798b WE MUST FIX FIRST: its bull/bear labels were HINDSIGHT calendar windows
(halving-anchored). You cannot trade a hindsight label. So the irreducible sub-problem (NSI candidate e)
is a CAUSAL / ONLINE regime detector: at entry time t, decide bull vs bear using ONLY bars <= t. This
script (1) builds several causal detectors and validates them against the TB00798b hindsight phases,
(2) re-scores the long & short edge split by WITH-cycle vs COUNTER-cycle using the CAUSAL label (the
honest test of Bill's idea), and (3) scores the candidate policies (no-gate / hard-gate / size-tilt /
quick-reversal-exit / raise-the-bar / asymmetric) on the WHOLE-organism objective: full-cycle pooled E%,
total $ P&L at $50/trade, and a max-drawdown proxy (reduce-losses axis).

Reuses tb00798b_cycle_phase.py's engine (-> tb00794 cached klines+funding, offline). No live code, no
go-live. Writes tb00801a_cycle_strategy_verdict.txt."""

from __future__ import annotations
import os, io, importlib.util, contextlib, calendar, bisect

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("probe", os.path.join(HERE, "tb00794_perps_probe.py"))
PB = importlib.util.module_from_spec(_spec)
with contextlib.redirect_stdout(io.StringIO()):
    _spec.loader.exec_module(PB)
A = PB.A
TAKER = PB.TAKER_PERP
NOTIONAL = 50.0
MINN = 12

ST = lambda stop, tgt: ("st", A.KS.index(stop), A.MS.index(tgt))
TR = lambda trail: ("tr", A.TRAILS.index(trail), None)

def ts(y, m, d):
    return calendar.timegm((y, m, d, 0, 0, 0, 0, 0, 0))

# Hindsight phases (TB00798b) - used ONLY to VALIDATE the causal detectors and for the by-phase report.
PHASES = [
    ("pre-halving accum 19-20", ts(2019, 9, 1),  ts(2020, 5, 11), "bull"),
    ("C3 BULL (post-2020 halv)", ts(2020, 5, 11), ts(2021, 11, 10), "bull"),
    ("C3 BEAR / winter",         ts(2021, 11, 10), ts(2022, 11, 21), "bear"),
    ("C3->C4 recovery",          ts(2022, 11, 21), ts(2024, 4, 20), "bull"),
    ("C4 BULL (post-2024 halv)", ts(2024, 4, 20), ts(2025, 10, 1), "bull"),
    ("C4 late / cooling 25-26",  ts(2025, 10, 1), ts(2027, 1, 1), "bear"),
]

# Primary 24h configs (the live strategy's home cadence, phase2-build). One long + one short = clean,
# low-overlap organism for the equity curve. The multi-config table (below) tests robustness.
LONG_PRI  = ("bb_break", "trend100", ST(3.0, None))      # breakout long, stop3x + reversal
SHORT_PRI = ("rsi_trend", "vol",      ST(3.0, 3.0))      # mean-rev short, stop3x target3x
QUICK     = TR(1.5)                                       # tightest chandelier trail = "ready for a quick reversal"

# Robustness set (reuse tb00798b's validated configs across tfs)
LONGS = [("bb_break/trend100 24h", "24h", "bb_break", "trend100", ST(3.0, None)),
         ("bb_break/trend100 12h", "12h", "bb_break", "trend100", ST(5.0, None))]
SHORTS = [("rsi_trend/vol 24h",     "24h", "rsi_trend", "vol",      ST(3.0, 3.0)),
          ("bb_revert/trend100 12h","12h", "bb_revert", "trend100", TR(3.0)),
          ("bb_revert/trend100 8h", "8h",  "bb_revert", "trend100", TR(3.0))]

OUT = []
def emit(s=""):
    print(s, flush=True); OUT.append(s)


def build():
    with contextlib.redirect_stdout(io.StringIO()):
        prices, fund = PB.load_data()
        eight = PB.fold(prices["4h"], 2); twelve = PB.fold(prices["4h"], 3)
    speeds = {"8h": (eight, 90), "12h": (twelve, 60), "24h": (prices["1d"], 60)}
    built = {}; exits = {}
    for lbl, (data, mh) in speeds.items():
        pairs = PB.make_pairs(data, fund)
        built[lbl] = pairs
        exits[lbl] = {p.sym: PB.precompute_perp(p, mh) for p in pairs}
    return built, exits


# ============================ CAUSAL REGIME DETECTORS (no lookahead) ============================
H2020, H2024, HNEXT = ts(2020, 5, 11), ts(2024, 4, 20), ts(2028, 4, 1)
MONTH = 30.44 * 86400

def halving_clock(te):
    """Deterministic months-since-halving phase. Causal but CYCLE-TIMED (assumes the cycle repeats on
    schedule) - included as a comparator, flagged as the speculative one."""
    if te < H2020:
        return "BULL"
    h = H2020 if te < H2024 else H2024
    mo = (te - h) / MONTH
    return "BULL" if (mo < 18 or mo >= 30) else "BEAR"   # 0-18 markup, 18-30 winter, 30+ recovery


def build_detectors(btc):
    """From the BTC/USD daily pair build per-bar causal labels. Returns (t_arr, {name: [labels]})."""
    c, t, n = btc.c, btc.t, btc.n
    sma100 = btc.sma100
    sma200 = [None] * n
    run = c[0]; dd = [0.0] * n; ret90 = [None] * n
    for j in range(n):
        if j >= 199:
            sma200[j] = sum(c[j - 199:j + 1]) / 200
        if c[j] > run: run = c[j]
        dd[j] = c[j] / run - 1.0                          # <=0, drawdown from trailing ATH (causal)
        if j >= 90:
            ret90[j] = c[j] / c[j - 90] - 1.0
    det = {"sma100": [], "sma200": [], "dd25": [], "ret90": [], "halvclk": []}
    for j in range(n):
        det["sma100"].append("BULL" if (sma100[j] is None or c[j] > sma100[j]) else "BEAR")
        det["sma200"].append("BULL" if (sma200[j] is None or c[j] > sma200[j]) else "BEAR")
        det["dd25"].append("BEAR" if dd[j] <= -0.25 else "BULL")
        det["ret90"].append("BULL" if (ret90[j] is None or ret90[j] >= 0) else "BEAR")
        det["halvclk"].append(halving_clock(t[j]))
    return t, det


def regime_at(t_arr, labels, te):
    """Causal lookup: label of the latest BTC daily bar with time <= te."""
    j = bisect.bisect_right(t_arr, te) - 1
    return labels[j] if j >= 0 else None


# ============================ TRADE COLLECTION ============================
def stats(pls):
    if len(pls) < MINN:
        return None
    w = [x for x in pls if x > 0]; ls = [x for x in pls if x <= 0]
    avgw = sum(w) / len(w) if w else 0.0; avgl = -sum(ls) / len(ls) if ls else 1e-9
    return dict(n=len(pls), win=100 * len(w) / len(pls), E=100 * sum(pls) / len(pls),
                rr=avgw / avgl if avgl > 0 else 0.0)


def cell(st):
    return f"{'  --':>8s} {'':>4s} {'':>6s}" if st is None else f"{st['E']:>+7.2f}% {st['win']:>3.0f}% {st['n']:>6d}"


def pl_of(p, ex_entry, spec, side, i):
    gross, off = PB.gross_off(ex_entry, spec, side)
    fund_raw = p.cum[min(i + off, p.n - 1)] - p.cum[i]
    fund = fund_raw if side == "LONG" else -fund_raw
    return gross - 2 * TAKER - fund, off


def collect(pairs, exits, signame, filtname, spec, lens):
    """All entries (no date gate). Per trade: t, side, pl_std, pl_quick, strict_ok. strict_ok = passes
    own filter AND vol AND btc_tide AND trend100 (the 'incredibly strong signal' proxy)."""
    sig = A.SIGNALS[signame]; flt = A.FILTERS[filtname]
    fv, fb, ft = A.FILTERS["vol"], A.FILTERS["btc_tide"], A.FILTERS["trend100"]
    recs = []
    for p in pairs:
        ex = exits[p.sym]; n = p.n; i = 1
        while i < n - 1:
            side = sig(p, i)
            if side is None or side != lens or (i, side) not in ex or not flt(p, i, side):
                i += 1; continue
            e = ex[(i, side)]
            pl_std, off = pl_of(p, e, spec, side, i)
            pl_q, _ = pl_of(p, e, QUICK, side, i)
            strict = fv(p, i, side) and fb(p, i, side) and ft(p, i, side)
            recs.append(dict(t=p.t[i], side=side, pl=pl_std, plq=pl_q, strict=strict, sym=p.sym))
            i += max(1, off)
    return recs


def label_recs(recs, t_arr, labels):
    for r in recs:
        reg = regime_at(t_arr, labels, r["t"])
        r["reg"] = reg
        # with-cycle = LONG in BULL or SHORT in BEAR; counter-cycle = the opposite
        r["counter"] = (reg is not None) and ((r["side"] == "LONG" and reg == "BEAR") or
                                              (r["side"] == "SHORT" and reg == "BULL"))
    return recs


# ============================ EQUITY CURVE / DRAWDOWN ============================
def curve(trades):
    """trades = list of (t, dollars). Sorted by t -> cumulative equity, total $, max drawdown $."""
    if not trades:
        return 0.0, 0.0, 0
    eq = 0.0; peak = 0.0; mdd = 0.0
    for _, d in sorted(trades, key=lambda x: x[0]):
        eq += d
        if eq > peak: peak = eq
        if peak - eq > mdd: mdd = peak - eq
    return eq, mdd, len(trades)


def phase_of(te):
    for name, t0, t1, kind in PHASES:
        if t0 <= te < t1:
            return name
    return "?"


def main():
    emit("=" * 108)
    emit("TB00801a - HOW DOES THE HALVING CYCLE ENTER THE STRATEGY?  causal regime + Bill's asymmetric idea, scored")
    emit("  perp taker 0.05%/side, REAL funding, fixed $50 notional, 30-pair universe. Detectors are CAUSAL (<=t).")
    emit("=" * 108)
    built, exits = build()
    btc = next(p for p in built["24h"] if p.sym == "BTC/USD")
    t_arr, det = build_detectors(btc)

    # ---------- PART 1: validate causal detectors vs hindsight phases ----------
    emit("\n" + "#" * 108)
    emit("# PART 1 - CAUSAL DETECTOR VALIDATION: % of each phase's BTC days the detector calls BULL (want hi in")
    emit("#          bull phases, lo in bear). A good causal detector reproduces the empirical bull/bear sign.")
    emit("#" * 108)
    dn = ["sma100", "sma200", "dd25", "ret90", "halvclk"]
    emit(f"  {'phase':28s} {'truth':5s} {'BTCb&h':>7s} | " + " | ".join(f"{d:>7s}" for d in dn))
    for name, t0, t1, kind in PHASES:
        idx = [j for j in range(btc.n) if t0 <= btc.t[j] < t1]
        if len(idx) < 2:
            continue
        bh = 100 * (btc.c[idx[-1]] / btc.c[idx[0]] - 1)
        pcts = []
        for d in dn:
            b = sum(1 for j in idx if det[d][j] == "BULL")
            pcts.append(f"{100*b/len(idx):>6.0f}%")
        emit(f"  {name:28s} {kind:5s} {bh:>+6.0f}% | " + " | ".join(pcts))
    # agreement score: fraction of days each detector's label sign matches the phase truth
    emit("\n  AGREEMENT (% of all BTC days the causal label matches the phase's hindsight bull/bear truth):")
    truth = {}
    for name, t0, t1, kind in PHASES:
        for j in range(btc.n):
            if t0 <= btc.t[j] < t1:
                truth[j] = "BULL" if kind == "bull" else "BEAR"
    for d in dn:
        ok = sum(1 for j, tv in truth.items() if det[d][j] == tv)
        emit(f"     {d:8s} {100*ok/len(truth):>5.1f}%   (BEAR days called: {sum(1 for j in truth if det[d][j]=='BEAR')}/{sum(1 for j in truth.values() if j=='BEAR')})")

    # ---------- PART 2: edge split by CAUSAL regime (the honest test of Bill's idea) ----------
    emit("\n" + "#" * 108)
    emit("# PART 2 - EDGE SPLIT BY CAUSAL REGIME (Bill's hypothesis: counter-cycle trades are the losers).")
    emit("#          with-cycle = LONG in detected-BULL, SHORT in detected-BEAR. counter = the opposite.")
    emit("#          E% / win / n. If counter-cycle E<0 and with-cycle E>0 under a CAUSAL label, the gate is real.")
    emit("#" * 108)
    primary = {"LONG": collect(built["24h"], exits["24h"], *LONG_PRI, "LONG"),
               "SHORT": collect(built["24h"], exits["24h"], *SHORT_PRI, "SHORT")}
    for d in dn:
        emit(f"\n  detector = {d}")
        emit(f"    {'side':6s} {'WITH-cycle E/win/n':>26s} {'COUNTER-cycle E/win/n':>26s}")
        for side in ("LONG", "SHORT"):
            recs = label_recs([dict(r) for r in primary[side]], t_arr, det[d])
            wc = stats([r["pl"] for r in recs if r["reg"] is not None and not r["counter"]])
            cc = stats([r["pl"] for r in recs if r["counter"]])
            emit(f"    {side:6s} {cell(wc):>26s} {cell(cc):>26s}")

    # robustness: pooled multi-config counter-cycle E under sma200
    emit("\n  ROBUSTNESS (pooled across all validated configs, detector=sma200): counter-cycle E by side")
    for title, cfgs, lens in (("LONG", LONGS, "LONG"), ("SHORT", SHORTS, "SHORT")):
        allcc = []; allwc = []
        for nm, tf, sg, ft2, sp in cfgs:
            rc = label_recs(collect(built[tf], exits[tf], sg, ft2, sp, lens), t_arr, det["sma200"])
            allcc += [r["pl"] for r in rc if r["counter"]]
            allwc += [r["pl"] for r in rc if r["reg"] is not None and not r["counter"]]
        emit(f"    {lens:6s} WITH {cell(stats(allwc))}   COUNTER {cell(stats(allcc))}")

    # ---------- PART 3: candidate-policy scorecard on the whole organism ----------
    GATE = "sma200"   # chosen causal detector for gating (justified by Part 1 agreement)
    L = label_recs([dict(r) for r in primary["LONG"]], t_arr, det[GATE])
    S = label_recs([dict(r) for r in primary["SHORT"]], t_arr, det[GATE])
    allr = L + S
    emit("\n" + "#" * 108)
    emit(f"# PART 3 - CANDIDATE-POLICY SCORECARD (primary 24h long+short organism, gate detector = {GATE}).")
    emit("#          MONEY = total $ P&L at $50/trade over full history; LOSS = max-drawdown proxy $ (chrono,")
    emit("#          overlapping -> comparative not tradable). worstPhE = worst hindsight-phase E%.")
    emit("#" * 108)

    def score(label, keep, dollars):
        """keep(r)->bool ; dollars(r)->$ contribution. Build the scorecard row."""
        sel = [r for r in allr if keep(r)]
        pls = [r["pl"] if "_use_q" not in r else r["plq"] for r in sel]  # placeholder; dollars() owns pl
        trades = [(r["t"], dollars(r)) for r in sel]
        tot, mdd, n = curve(trades)
        # E% per trade on the pl actually used (for comparability), and worst-phase E
        used = [dollars(r) / NOTIONAL for r in sel]   # back to fraction-equivalent (tilt folded in)
        e = 100 * sum(used) / len(used) if used else 0.0
        ph = {}
        for r in sel:
            ph.setdefault(phase_of(r["t"]), []).append(dollars(r) / NOTIONAL)
        worst = min((100 * sum(v) / len(v) for v in ph.values() if len(v) >= 8), default=0.0)
        emit(f"  {label:30s} n={n:>5d}  E/tr={e:>+6.2f}%  total=${tot:>+8.1f}  maxDD=${mdd:>7.1f}  worstPhE={worst:>+6.2f}%")
        return tot, mdd

    std_d = lambda r: r["pl"] * NOTIONAL
    emit("\n  -- baselines --")
    score("P0 no-gate (both always-on) [d]", lambda r: r["reg"] is not None, std_d)
    score("    long-only no-gate",            lambda r: r["reg"] is not None and r["side"] == "LONG", std_d)
    score("    short-only no-gate",           lambda r: r["reg"] is not None and r["side"] == "SHORT", std_d)
    emit("\n  -- Bill's idea + variants (counter-cycle = short-in-bull / long-in-bear) --")
    score("P1 hard-gate counter off [a-max]", lambda r: r["reg"] is not None and not r["counter"], std_d)
    score("P2 size-tilt counter x0.5 [b]",    lambda r: r["reg"] is not None,
          lambda r: r["pl"] * NOTIONAL * (0.5 if r["counter"] else 1.0))
    score("P3 quick-exit on counter [c]",     lambda r: r["reg"] is not None,
          lambda r: (r["plq"] if r["counter"] else r["pl"]) * NOTIONAL)
    score("P4 raise-bar counter (strict) [a]",lambda r: r["reg"] is not None and (not r["counter"] or r["strict"]), std_d)
    score("P5 Bill-combo strict+quick",       lambda r: r["reg"] is not None and (not r["counter"] or r["strict"]),
          lambda r: (r["plq"] if r["counter"] else r["pl"]) * NOTIONAL)
    score("P6 asym: gate LONG-in-bear only",  lambda r: r["reg"] is not None and not (r["side"] == "LONG" and r["counter"]), std_d)
    score("P7 asym + quick on short-in-bull", lambda r: r["reg"] is not None and not (r["side"] == "LONG" and r["counter"]),
          lambda r: (r["plq"] if (r["side"] == "SHORT" and r["counter"]) else r["pl"]) * NOTIONAL)

    emit("\nREAD: compare every policy to P0. A winner makes MORE money (higher total$) AND/OR reduces losses")
    emit("  (lower maxDD, better worstPhE). Bill's idea = P1/P4 (gate or raise-bar the counter-cycle side);")
    emit("  the asymmetric reading (gate only the expensive long-in-bear side) = P6/P7.")

    # ---------- CONCLUSION (FP/DP) ----------
    emit("\n" + "=" * 108)
    emit("CONCLUSION (TB00801a) - FIRST PRINCIPLES / DEMING: the cycle's role is the HEDGE, NOT a gate")
    emit("=" * 108)
    emit("""
1. THE CYCLE IS REAL AND CAUSALLY KNOWABLE (answers NSI sub-problem e). BTC daily close vs its own
   200-day SMA (sma200) reproduces the hindsight bull/bear sign 83% of the time with NO lookahead and NO
   cycle-timing assumption. It is the best honest detector. (halvclk scores 99% but that is CIRCULAR -
   the hindsight phases ARE halving-anchored - and it is cycle-TIMED, which the NSI forbids; discount it.)

2. BILL'S SEED IDEA, TESTED HONESTLY, IS DIRECTIONALLY RIGHT BUT THE REMEDY HURTS THE WHOLE ORGANISM.
   - Directionally right: with-cycle E cleanly beats counter-cycle E on BOTH sides, under EVERY honest
     detector (sma200: LONG +15.4% vs +3.1%; SHORT +3.8% vs +2.0%). The cycle orders expected value.
   - But the counter-cycle trades are NOT money-losers under a causal label - they are merely
     lower-edge POSITIVE. The reason: the per-pair trend100 + vol filters ALREADY screen the cycle drag.
     The TB00798b -11%/-4.7% counter-cycle longs were UNCONDITIONAL phase cuts; once the causal detector
     AND the filters are applied, the survivors print +3%. Only the CIRCULAR halvclk makes counter-cycle
     longs look catastrophic (-8.4%) - i.e. Bill's gate only "works" if you cheat with the calendar.
   - So gating costs money and ADDS drawdown: P1 hard-gate -16% total$ and maxDD $400->$559; every
     gate/raise-bar/quick-exit variant (P1-P7) is DOMINATED by P0 on total$ AND maxDD AND worst-phase E.

3. DEMING - OPTIMIZE THE WHOLE, NOT THE PART. Bill's gate optimizes each phase (don't fight the cycle)
   but pessimizes the organism, because the two sides are phase-ANTI-CORRELATED and self-hedge:
   running BOTH sides cuts max-drawdown BELOW either side alone ($400 vs long-only $555 / short-only
   $580) and lifts the worst-phase E from the long-only -10.7% to +0.2%. The short side literally
   rescues the long's winter. THAT is the cycle edge, quantified - and it is candidate (d): run both
   sides at equal detail, no explicit cycle gate, let the existing filters + anti-correlation hedge.

4. THE ONE DURABLE LEVER is the EV ORDERING (with-cycle >> counter-cycle), and it points to SIZE, not a
   gate: P2 (counter x0.5 size-tilt) was the least-bad modifier (money $5608->$5162, maxDD $400->$432).
   Even it does not beat P0 here - so nothing should be built now - but a GENTLE, CIATS-self-tuned,
   paper-validated size tilt keyed on sma200 is the only cycle mechanism worth keeping on the table.
   NEVER a hard gate, NEVER cycle-timing, NEVER a hand-set Bill knob (ciats-self-tuning + 200-trade floor).

ANSWERS TO BILL'S TWO QUESTIONS:
  Q1 MORE RESEARCH? Core question = answered (cycle real, detectable, gate rejected). YES on 3 bounded
     cuts: (i) low-n bear side - only ~2 honest bear phases; re-test as C4 winter deepens / calibrate
     sma200 on longer spot BTC history. (ii) a CIATS-self-tuned CONTINUOUS size tilt vs the binary gate
     (the only modifier that nearly held money). (iii) detector robustness - confirm sma200 OOS and that
     it is not just re-encoding trend100. NO to building any cycle GATE now; NO to halvclk cycle-timing.
  Q2 HOW TO USE IT? Primary (already ratified, no new code): KEEP the both-sides equal-detail organism -
     it IS the cycle hedge. Use the cycle knowledge to (a) NOT judge the breakout long on a bear-phase
     fold (its softness is the C4 winter; expect reacceleration into Apr-2028), and (b) justify funding
     the short side (winter harvester). Secondary (propose only, Bill's nod): register a CIATS theory =
     gentle sma200 size tilt, to self-test in paper - do NOT fold into 0500000 until it beats P0 OOS.

NET: do NOT add a cycle gate. The cycle is the ARCHITECTURAL ARGUMENT for the Long/Short organism Bill
already ratified, plus at most a future CIATS size-tilt theory. Nothing minted; STAY IN PAPER; 1:1.5 floor
untouched; 0500000 unchanged (propose only).""")


if __name__ == "__main__":
    main()
    try:
        with open(os.path.join(HERE, "tb00801a_cycle_strategy_verdict.txt"), "w") as f:
            f.write("\n".join(OUT) + "\n")
    except Exception:
        pass
