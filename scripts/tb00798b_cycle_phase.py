"""TB00798b - HALVING-CYCLE PHASE CUT: does the perp breakout-LONG track the bull phases and the
mean-reversion SHORT carry the bear phases? Bill's point (correct): the TB00798 walk-forward's "long
softens in the most-recent fold" likely reflects the 4-year Bitcoin halving CYCLE (we are ~2yr past the
Apr-2024 halving = bear/winter; the next bull builds into the Apr-2028 halving), NOT structural alpha
decay. A generic sequential-fold split blurs this because the folds are calendar-arbitrary. This re-cuts
the SAME cached perp history into halving-cycle PHASES and reports, per phase: BTC's own return (so the
bull/bear label is self-justifying), the breakout-LONG edge, and the mean-reversion-SHORT edge.

HYPOTHESIS (Bill's): LONG is strongly positive in BULL phases, quiet/negative in BEAR; SHORT carries the
BEAR/chop phases. If so, the long is a cycle harvester (expect reacceleration into 2028), and the
Long/Short organism is itself the cycle hedge - longs for summer, shorts for winter.

Reuses tb00794_perps_probe.py (cached klines + REAL funding, offline) + auto_strategy_search.py. The
phase windows are halving-anchored calendar windows; BTC buy&hold return per window is printed so the
label is not assumed but shown. PAPER research only. Writes tb00798b_cycle_phase_verdict.txt."""

from __future__ import annotations
import os, io, importlib.util, contextlib, calendar

HERE = os.path.dirname(os.path.abspath(__file__))
_spec = importlib.util.spec_from_file_location("probe", os.path.join(HERE, "tb00794_perps_probe.py"))
PB = importlib.util.module_from_spec(_spec)
with contextlib.redirect_stdout(io.StringIO()):
    _spec.loader.exec_module(PB)
A = PB.A
TAKER = PB.TAKER_PERP
MINN = 12

ST = lambda stop, tgt: ("st", A.KS.index(stop), A.MS.index(tgt))
TR = lambda trail: ("tr", A.TRAILS.index(trail), None)

def ts(y, m, d):
    return calendar.timegm((y, m, d, 0, 0, 0, 0, 0, 0))

# Halving-anchored phases. Halvings: 2020-05-11, 2024-04-20 (next ~2028-04). Bull = halving->blow-off top;
# Bear = top->capitulation; Recovery = capitulation->next halving. BTC return per window is shown to
# justify each label empirically (not asserted).
PHASES = [
    ("pre-halving accum 19-20", ts(2019, 9, 1),  ts(2020, 5, 11)),
    ("C3 BULL (post-2020 halv)", ts(2020, 5, 11), ts(2021, 11, 10)),
    ("C3 BEAR / winter",         ts(2021, 11, 10), ts(2022, 11, 21)),
    ("C3->C4 recovery",          ts(2022, 11, 21), ts(2024, 4, 20)),
    ("C4 BULL (post-2024 halv)", ts(2024, 4, 20), ts(2025, 10, 1)),
    ("C4 late / cooling 25-26",  ts(2025, 10, 1), ts(2027, 1, 1)),
]

# top configs from TB00794 / TB00798 (the two mechanisms, at their home tfs)
LONGS = [
    ("bb_break/trend100/stop3x/rev", "24h", "bb_break", "trend100", ST(3.0, None)),
    ("bb_break/trend100/stop5x/rev", "12h", "bb_break", "trend100", ST(5.0, None)),
]
SHORTS = [
    ("rsi_trend/vol/stop3x/target3.0x", "24h", "rsi_trend", "vol",      ST(3.0, 3.0)),
    ("bb_revert/trend100/trail3.0x",    "12h", "bb_revert", "trend100", TR(3.0)),
    ("bb_revert/trend100/trail3.0x",    "8h",  "bb_revert", "trend100", TR(3.0)),
]

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


def eval_window(pairs, exits, signame, filtname, spec, lens, t0, t1, taker):
    """Pooled stats over entries whose bar-time falls inside [t0,t1). Date-gated mirror of run_combo_perp."""
    sig = A.SIGNALS[signame]; flt = A.FILTERS[filtname]; pls = []; bypair = {}
    for p in pairs:
        ex = exits[p.sym]; cum = p.cum; n = p.n; t = p.t; i = 1
        while i < n - 1:
            if not (t0 <= t[i] < t1):
                i += 1; continue
            side = sig(p, i)
            if side is None or side != lens or (i, side) not in ex or not flt(p, i, side):
                i += 1; continue
            gross, off = PB.gross_off(ex[(i, side)], spec, side)
            fund_raw = cum[min(i + off, n - 1)] - cum[i]
            fund = fund_raw if side == "LONG" else -fund_raw
            pl = gross - 2 * taker - fund
            pls.append(pl); bypair.setdefault(p.sym, []).append(pl)
            i += max(1, off)
    if len(pls) < MINN:
        return None
    w = [x for x in pls if x > 0]; ls = [x for x in pls if x <= 0]
    avgw = sum(w) / len(w) if w else 0.0; avgl = -sum(ls) / len(ls) if ls else 1e-9
    return dict(n=len(pls), win=100 * len(w) / len(pls), E=100 * sum(pls) / len(pls),
                rr=avgw / avgl if avgl > 0 else 0.0, npairs=len(bypair))


def btc_ret(pairs, t0, t1):
    """BTC buy&hold % over the window (first/last close with bar-time in [t0,t1)), to justify the label."""
    for p in pairs:
        if p.sym == "BTC/USD":
            idx = [k for k in range(p.n) if t0 <= p.t[k] < t1]
            if len(idx) < 2:
                return None
            c0 = p.c[idx[0]]; c1 = p.c[idx[-1]]
            return 100 * (c1 / c0 - 1)
    return None


def line(stat):
    if stat is None:
        return f"{'  --':>8s} {'':>5s} {'':>5s} {'':>6s}"
    return f"{stat['E']:>+7.2f}% {stat['rr']:>5.2f} {stat['win']:>4.0f}% {stat['n']:>6d}"


def main():
    emit("=" * 104)
    emit("TB00798b HALVING-CYCLE PHASE CUT - does the breakout-LONG track bulls and the mean-rev SHORT carry bears?")
    emit("  same cached perp history, re-cut by halving-anchored phase; BTC buy&hold % shown to justify each label.")
    emit("  perp taker 0.05%/side, REAL funding, fixed $50 notional, pooled across the 30-pair universe.")
    emit("=" * 104)
    built, exits = build()

    for title, configs, lens in (("BREAKOUT LONG", LONGS, "LONG"), ("MEAN-REVERSION SHORT", SHORTS, "SHORT")):
        emit("\n" + "#" * 104)
        emit(f"# {title}  (E% / rr / win / n  per halving-cycle phase)")
        emit("#" * 104)
        emit(f"  {'phase':28s} {'BTC b&h':>9s} | " + " | ".join(f"{name.split('/')[0]+' '+tf:>14s}" for name, tf, *_ in configs))
        for ptitle, t0, t1 in PHASES:
            br = btc_ret(built["24h"], t0, t1)
            cells = []
            for name, tf, sg, ft, sp in configs:
                cells.append(line(eval_window(built[tf], exits[tf], sg, ft, sp, lens, t0, t1, TAKER)))
            brs = (f"{br:>+7.0f}%" if br is not None else "    --")
            emit(f"  {ptitle:28s} {brs:>9s} | " + " | ".join(cells))

    emit("\nREAD: a BULL phase = large positive BTC b&h; a BEAR phase = negative/flat. HYPOTHESIS (Bill):")
    emit("  LONG E strong in BULL phases, weak/negative in BEAR; SHORT E carries the BEAR/chop phases. If the")
    emit("  pattern holds, the long's recent softness is CYCLE-PHASE (between bulls), not alpha decay - expect")
    emit("  reacceleration into the Apr-2028 halving - and the Long/Short organism is itself the cycle hedge.")
    emit("  CAVEAT: only ~2 full halving cycles of perp data (low n); a maturing/ETF market may dampen the")
    emit("  amplitude or shift timing -> design cycle-ROBUST (both sides + filters), NOT cycle-TIMED.")


if __name__ == "__main__":
    main()
    try:
        with open(os.path.join(HERE, "tb00798b_cycle_phase_verdict.txt"), "w") as f:
            f.write("\n".join(OUT) + "\n")
    except Exception:
        pass
