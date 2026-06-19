"""TB00798 - PERPS HARDENING: walk-forward persistence + fee-breakeven on the TB00794 survivor mechanisms.

WHY (the gap TB00794 left). The TB00794 probe found 40-63 regime-robust SHORT survivors on perps and a
breakout LONG, but flagged three caveats it did NOT close: (a) the short edge is THIN and fee-sensitive
(flips negative at ~10-20 bp added cost) AND the live Kraken-perp fee is route-dependent / unknown
(0500000 13.6 slice E); (c) selection risk from a 525-combo search; (d) ~5-6yr history with a thin deep-
bear sample. This run hardens the ALREADY-CHOSEN survivor configs (no fresh search -> no new selection
bias) two ways, to firm up the proposed-seed STARTING VALUES before they graduate into TB00000 sec 8:

  1. WALK-FORWARD PERSISTENCE. Evaluate each fixed survivor config across K=6 SEQUENTIAL time folds
     (pooled across the 30-pair universe), at the perp taker. A seed mechanism we trust should be E>0 in
     most/all folds, not just on one 50/50 OOS block. This is a persistence test of a fixed mechanism
     across sub-periods (it catches era-fragility, e.g. the 24h alt-bull inflation), NOT a true forward
     holdout (the configs were chosen on this same 2020-2025 span - stated honestly).
  2. FEE BREAKEVEN. Sweep the perp taker from 0.04% to 0.25%/side on each SHORT config and find where the
     pooled edge crosses zero. This is the single most decision-relevant number: it states exactly how
     much fee headroom the thin short edge has, so the Kraken maker/taker schedule (vs the 0.05% Binance-
     UM assumption) can be checked against it the moment Kraken support answers.

Reuses tb00794_perps_probe.py (cached perp klines + REAL funding, offline) and operations/
auto_strategy_search.py (SIGNALS / FILTERS / KS / MS / TRAILS). PAPER research only; no network if caches
present. Writes a plain-text verdict tb00798_perps_harden_verdict.txt."""

from __future__ import annotations
import os, io, importlib.util, contextlib

HERE = os.path.dirname(os.path.abspath(__file__))
# import the probe module (reuse its load/fold/make_pairs/precompute_perp/gross_off/run_combo_perp)
_spec = importlib.util.spec_from_file_location("probe", os.path.join(HERE, "tb00794_perps_probe.py"))
PB = importlib.util.module_from_spec(_spec)
with contextlib.redirect_stdout(io.StringIO()):      # silence the probe module import-time prints
    _spec.loader.exec_module(PB)
A = PB.A
TAKER_PERP = PB.TAKER_PERP                            # 0.0005 = 0.05%/side (Binance-UM / Kraken-Futures assumption)

# KS=[2,3,5] -> idx 0,1,2 ; MS=[1.5,2,3,None] -> idx 0,1,2,3 ; TRAILS=[1.5,2,3] -> idx 0,1,2
ST = lambda stop, tgt: ("st", A.KS.index(stop), A.MS.index(tgt))   # fixed-stop + target/reversal exit
TR = lambda trail: ("tr", A.TRAILS.index(trail), None)            # chandelier trail exit

# ---- the TB00794 survivor configs (name, tf, signal, filter, exit-spec) ----
# SHORT cluster = mean-reversion / filtered entry + trailing-or-target exit + regime filter (verdict sec 1).
SHORT = [
    ("rsi_trend/vol/stop3x/target3.0x",   "24h", "rsi_trend", "vol",      ST(3.0, 3.0)),
    ("rsi_trend/trend100/stop2x/target3", "24h", "rsi_trend", "trend100", ST(2.0, 3.0)),
    ("ema_cross/vol/stop2x/target3.0x",   "24h", "ema_cross", "vol",      ST(2.0, 3.0)),
    ("bb_revert/trend100/trail3.0x",      "12h", "bb_revert", "trend100", TR(3.0)),
    ("bb_revert/trend100/trail3.0x",      "8h",  "bb_revert", "trend100", TR(3.0)),
    ("macd/vol/trail2.0x",                "12h", "macd",      "vol",      TR(2.0)),
    ("macd/btc_tide/trail3.0x",           "8h",  "macd",      "btc_tide", TR(3.0)),
    ("ema_cross/btc_tide/trail3.0x",      "8h",  "ema_cross", "btc_tide", TR(3.0)),
]
# LONG = breakout entry + wide stop + run-to-reversal (verdict sec 3). MS=None => reversal exit.
LONG = [
    ("bb_break/trend100/stop5x/rev", "12h", "bb_break", "trend100", ST(5.0, None)),
    ("bb_break/vol/stop3x/rev",      "8h",  "bb_break", "vol",      ST(3.0, None)),
    ("donchian/trend100/stop5x/rev", "12h", "donchian", "trend100", ST(5.0, None)),
    ("bb_break/trend100/stop3x/rev", "24h", "bb_break", "trend100", ST(3.0, None)),
]

K_FOLDS = 6
FEE_BPS = [4, 5, 7, 10, 13, 16, 20, 25]   # taker per side, basis points, for the breakeven sweep

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
        built[lbl] = (pairs, mh)
        exits[lbl] = {p.sym: PB.precompute_perp(p, mh) for p in pairs}
    return built, exits


def ev(built, exits, tf, signame, filtname, spec, lens, lo, hi, taker, minn):
    pairs, _ = built[tf]
    return PB.run_combo_perp(pairs, exits[tf], A.SIGNALS[signame], A.FILTERS[filtname],
                             spec, lens, lo, hi, taker, minn)


def walkforward(built, exits, name, tf, signame, filtname, spec, lens):
    """E per sequential fold (pooled across pairs) at the perp taker. Returns (fold_Es, pooled)."""
    fold_Es = []
    for k in range(K_FOLDS):
        r = ev(built, exits, tf, signame, filtname, spec, lens, k / K_FOLDS, (k + 1) / K_FOLDS, TAKER_PERP, 12)
        fold_Es.append(None if r is None else r["E"])
    pooled = ev(built, exits, tf, signame, filtname, spec, lens, 0.0, 1.0, TAKER_PERP, 40)
    return fold_Es, pooled


def breakeven(built, exits, tf, signame, filtname, spec, lens):
    """Pooled E vs taker; return (list of (bp,E), breakeven_bp_or_None)."""
    curve = []
    for bp in FEE_BPS:
        r = ev(built, exits, tf, signame, filtname, spec, lens, 0.0, 1.0, bp / 1e4, 40)
        curve.append((bp, None if r is None else r["E"]))
    be = None
    for a_, b_ in zip(curve, curve[1:]):
        if a_[1] is not None and b_[1] is not None and a_[1] > 0 >= b_[1]:
            # linear interpolate the zero-crossing in bp
            be = a_[0] + (b_[0] - a_[0]) * (a_[1] - 0) / (a_[1] - b_[1]); break
    if be is None and curve and curve[0][1] is not None and curve[0][1] <= 0:
        be = 0.0   # already underwater at the lowest fee
    return curve, be


def fmt_folds(fe):
    return " ".join((f"{x:+5.1f}" if x is not None else "  .  ") for x in fe)


def main():
    emit("=" * 96)
    emit("TB00798 PERPS HARDENING - walk-forward persistence + fee-breakeven on the TB00794 survivors")
    emit(f"  perp taker {TAKER_PERP*100:.2f}%/side (the assumption under test); K={K_FOLDS} sequential folds;")
    emit("  fixed pre-chosen configs (NO fresh search -> no new selection bias); cached data, offline.")
    emit("=" * 96)
    built, exits = build()
    for tf in ("8h", "12h", "24h"):
        emit(f"    {tf:4s} pairs={len(built[tf][0])}")

    for title, lens, configs in (("SHORT", "SHORT", SHORT), ("LONG", "LONG", LONG)):
        emit("\n" + "#" * 96)
        emit(f"# {title} SURVIVORS - WALK-FORWARD ({K_FOLDS} folds, E% per fold, pooled across pairs, perp taker)")
        emit("#" * 96)
        emit(f"  {'config':34s} {'tf':4s} {'folds(E% oldest->newest)':38s} {'+f/K':5s} {'min':>6s} {'pooledE':>8s} {'rr':>5s} {'n':>6s}")
        for name, tf, sg, ft, sp in configs:
            fe, pooled = walkforward(built, exits, name, tf, sg, ft, sp, lens)
            npos = sum(1 for x in fe if x is not None and x > 0)
            valid = [x for x in fe if x is not None]
            mn = min(valid) if valid else float("nan")
            pe = pooled["E"] if pooled else float("nan"); rr = pooled["rr"] if pooled else float("nan")
            n = pooled["n"] if pooled else 0
            emit(f"  {name:34s} {tf:4s} {fmt_folds(fe):38s} {npos:>2d}/{K_FOLDS:<2d} {mn:>+6.1f} "
                 f"{pe:>+7.2f}% {rr:>5.2f} {n:>6d}")

    emit("\n" + "#" * 96)
    emit("# SHORT SURVIVORS - FEE BREAKEVEN (pooled E% vs taker/side; the route-dependent unknown, 13.6)")
    emit("#" * 96)
    hdr = "  " + f"{'config':34s} {'tf':4s} " + " ".join(f"{bp:>5d}bp" for bp in FEE_BPS) + f" {'breakeven':>10s}"
    emit(hdr)
    for name, tf, sg, ft, sp in SHORT:
        curve, be = breakeven(built, exits, tf, sg, ft, sp, "SHORT")
        cells = " ".join((f"{e:>+6.2f}" if e is not None else "   .  ") for _, e in curve)
        bestr = (f"~{be:.1f}bp" if be is not None else ">25bp")
        emit(f"  {name:34s} {tf:4s} {cells} {bestr:>10s}")

    emit("\nNOTE: walk-forward = a fixed-mechanism persistence test across 6 sequential sub-periods of the")
    emit("  2020-2025 perp span (catches era-fragility); it is NOT a fresh forward holdout (configs were")
    emit("  chosen on this span - stated honestly). Breakeven = the taker/side at which pooled E crosses 0;")
    emit("  compare it to the LIVE Kraken perp maker/taker once support answers. % = return on capital/trade,")
    emit("  unlevered, fixed $50 notional, REAL Binance-UM funding. Sacred 1:1.5 floor unaffected (sizing).")


if __name__ == "__main__":
    main()
    try:
        with open(os.path.join(HERE, "tb00798_perps_harden_verdict.txt"), "w") as f:
            f.write("\n".join(OUT) + "\n")
    except Exception:
        pass
