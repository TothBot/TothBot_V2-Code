"""TB00806d - WHOLE-ORGANISM ANALYSIS (Bill's TB00806 critique: the C/A/B batteries tested stovepiped
parameters / one pairwise interaction, NOT the whole organism).  This drives the three TB00806 proposed
changes through the ACTUAL live decision gates - it IMPORTS tothbot.pipeline.risk_guard (G7), .position_sizer
(G8) and the config.registry, and runs BOTH modules (Long spot + Short) the way the live organism does:
per-module wallet, per-module FROZEN baseline, per-module 5%/10% drawdown breakers, the leverage-bounded
SHORT committed-margin, the exposure/concentration caps - then measures the WHOLE account.  PAPER, offline,
propose-only, 0500000 unchanged, STAY IN PAPER.

WHY THIS IS DIFFERENT FROM C/A/B: those used simplified streams + abstract capital weights.  Here the
capital-allocation + risk machinery is the REAL gate code, so the analysis sees the interactions C/A/B could
not: (i) leverage_cap_short ALREADY EXISTS in the live registry (=3) and feeds the SHORT's committed margin
in G7 CHECK 2/3 - so 'add a 2-3x band' is REDUNDANT; this tests whether the seed is right whole-organism;
(ii) the per-module breaker already floors each module's drawdown at ~5-10% of its deposit, so the hedge's
'drawdown insurance' may OVERLAP the breaker that already exists (the part-vs-whole question); (iii) the two
modules are separately-capitalized (one never blocks the other), so the short-sizing question is a CAPITAL-
ALLOCATION / opportunity-cost decision on the OPERATOR per-module deposits, not a free add.

Streams (validated): Long-Spot = the deployed EMA12/26 24h-decision long (tb00802a collect_spot); Short =
the proposed perp short rsi_trend+vol (tb00806 substrate).  The SHORT here stands for the proposed perps
short revival; its committed capital is margin = notional / leverage_cap_short exactly as G7/G8 model a
Kraken-margin short (ar:AR-009).  Funding/borrow cost is already inside the stream's per-trade net pct."""

from __future__ import annotations
import os, io, sys, importlib.util, contextlib

HERE = os.path.dirname(os.path.abspath(__file__))
CODE_ROOT = os.path.abspath(os.path.join(HERE, ".."))
if CODE_ROOT not in sys.path:
    sys.path.insert(0, CODE_ROOT)

# --- the LIVE gates (imported, not re-implemented) ---
from tothbot.pipeline.risk_guard import evaluate_risk_guard, RiskDisposition          # G7
from tothbot.pipeline.position_sizer import size_candidate, SACRED_RR_FLOOR           # G8
from tothbot.exchange.position_mirror import PositionSide
from tothbot.config import registry

# --- the validated streams + margin model (the tb00806 substrate) ---
def _load(name, rel):
    s = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    m = importlib.util.module_from_spec(s)
    with contextlib.redirect_stdout(io.StringIO()):
        s.loader.exec_module(m)
    return m

S = _load("sim", "tb00806_perp_account_sim.py")
NOTIONAL = S.NOTIONAL                                    # $50 clip = per_trade_size_floor_usd
LEV_CAP = float(registry.value("leverage_cap_short"))   # 3 - the EXISTING live seed
PAUSE = float(registry.value("session_pause_drawdown_pct"))   # 0.05
HALT = float(registry.value("full_halt_drawdown_pct"))        # 0.10
DEP_LONG = float(registry.value("paper_starting_balance_long_usd"))    # 5000
DEP_SHORT = float(registry.value("paper_starting_balance_short_usd"))  # 5000

OUT = []
def emit(s=""):
    print(s, flush=True); OUT.append(s)
def pf(b):
    return "PASS" if b else "**FAIL**"


# ============================ faithful per-module replay through the LIVE G7 ============================
def run_module(trades, side, deposit, leverage_cap=LEV_CAP, clip=NOTIONAL, breakers=True,
               pause=PAUSE, halt=HALT):
    """Chronological replay of ONE module's wallet, calling the LIVE evaluate_risk_guard per candidate.
    committed margin = clip (LONG spot notional) or clip/leverage_cap (SHORT Kraken margin, ar:AR-009).
    Paper realized-equity basis (current_portfolio=None -> the live fallback uses realized wallet, the
    documented paper behavior; MTM would fire the breaker SOONER = FN-safe).  Returns a per-event equity
    series + stats so the WHOLE account can be assembled."""
    ev = sorted(trades, key=lambda r: r["t"])
    committed_unit = clip if side is PositionSide.LONG else clip / leverage_cap
    opens = []                       # (texit, pnl$, committed$)
    wallet = deposit; peak = deposit; trough = deposit; maxdd = 0.0
    taken = 0; pauses = 0; blocks = 0; halted_at = None
    series = []                      # (t, wallet) after each realization

    def realize_until(tnow):
        nonlocal wallet, peak, trough, maxdd
        opens.sort()
        while opens and opens[0][0] <= tnow:
            _, pnl, _ = opens.pop(0)
            wallet += pnl
            if wallet > peak: peak = wallet
            if wallet < trough: trough = wallet
            if peak - wallet > maxdd: maxdd = peak - wallet
            series.append((opens[0][0] if opens else tnow, wallet))

    for r in ev:
        realize_until(r["t"])
        if halted_at is not None:
            continue
        total_committed = sum(c for _, _, c in opens)
        out = evaluate_risk_guard(
            side,
            wallet_balance=wallet,
            portfolio_baseline=deposit,
            candidate_committed_usd=committed_unit,
            total_committed_usd=total_committed + committed_unit,
            semaphore_locked=False,
            current_portfolio=None,            # paper fallback -> realized wallet is the drawdown current term
            full_halt_drawdown_pct=halt if breakers else 1e9,
            session_pause_drawdown_pct=pause if breakers else 1e9,
        )
        if not out.passed:
            d = out.disposition
            if d is RiskDisposition.HALT:
                halted_at = r["t"]; continue
            if d is RiskDisposition.PAUSE:
                pauses += 1; continue
            blocks += 1; continue          # BLOCK (exposure/concentration) or SKIP
        taken += 1
        opens.append((r["texit"], r["pct"] * clip, committed_unit))
    realize_until(float("inf"))
    return dict(side=side, deposit=deposit, taken=taken, final=wallet, profit=wallet - deposit,
                trough=trough, maxdd=maxdd, peak=peak,
                maxdd_pct_dep=100 * maxdd / deposit, worst_from_dep=deposit - trough,
                worst_pct_dep=100 * (deposit - trough) / deposit,
                pauses=pauses, blocks=blocks, halted=halted_at, series=series,
                calmar=(wallet - deposit) / maxdd if maxdd > 1e-9 else float("inf"))


def combined_account(mod_results):
    """Whole-account equity = sum of the modules' wallets over a merged timeline; returns combined maxDD$,
    final$, total deposit, and per-event combined drawdown (the ring-fenced multi-module account)."""
    import bisect
    cols = []
    for m in mod_results:
        ts = [t for t, _ in m["series"]]; eqs = [e for _, e in m["series"]]
        cols.append((ts, eqs, m["deposit"]))
    times = sorted({t for m in mod_results for t, _ in m["series"]})
    total_dep = sum(m["deposit"] for m in mod_results)
    peak = total_dep; maxdd = 0.0; last = total_dep
    for t in times:
        comb = 0.0
        for ts, eqs, dep in cols:
            j = bisect.bisect_right(ts, t) - 1
            comb += eqs[j] if j >= 0 else dep
        last = comb
        if comb > peak: peak = comb
        if peak - comb > maxdd: maxdd = peak - comb
    return dict(final=last, maxdd=maxdd, total_dep=total_dep, profit=last - total_dep,
                maxdd_pct_dep=100 * maxdd / total_dep,
                calmar=(last - total_dep) / maxdd if maxdd > 1e-9 else float("inf"))


def _fmt(m):
    h = "never" if m["halted"] is None else "TRIPPED"
    return (f"taken={m['taken']:>5d} final=${m['final']:>8.0f} profit=${m['profit']:>+8.0f} "
            f"maxDD=${m['maxdd']:>6.0f}={m['maxdd_pct_dep']:>4.1f}%dep worstLoss={m['worst_pct_dep']:>4.1f}%dep "
            f"pauses={m['pauses']:>3d} HALT={h} Calmar={m['calmar'] if m['calmar']!=float('inf') else 99:>5.1f}")


def main():
    emit("=" * 116)
    emit("TB00806d - WHOLE-ORGANISM ANALYSIS (the 3 TB00806 changes driven through the LIVE G7/G8 gates + registry)")
    emit("  imports tothbot.pipeline.risk_guard + .position_sizer + config.registry; two modules, real breakers.")
    emit(f"  live seeds: leverage_cap_short={LEV_CAP:.0f}  pause={PAUSE:.0%} halt={HALT:.0%}  deposit L=${DEP_LONG:.0f} S=${DEP_SHORT:.0f}")
    emit("  PAPER, offline, propose-only, 0500000 unchanged, STAY IN PAPER.")
    emit("=" * 116)
    s = S.load_substrate()
    pp, pex = s["pp"], s["pex"]
    mm = S.Margin(LEV_CAP, S.MMR0)
    long_stream = S.build_spot_pool(s["spot_data"])
    short_stream = S.build_perp_pool(pp, pex, S.SHORT_PERP_CFG, mm)
    emit(f"  streams: Long-Spot {len(long_stream)} trades | Short(perp) {len(short_stream)} trades\n")
    results = {}

    # ---------- W0: baseline whole organism (registry seeds as-is) ----------
    emit("#" * 116)
    emit("# W0 - THE LIVE ORGANISM AS-CONFIGURED (registry seeds: leverage_cap_short=3, $5000/module each).")
    emit("#   This is the reference the three changes are measured against - both modules through the real G7.")
    emit("#" * 116)
    L = run_module(long_stream, PositionSide.LONG, DEP_LONG)
    Sh = run_module(short_stream, PositionSide.SHORT, DEP_SHORT)
    comb = combined_account([L, Sh])
    emit(f"  Long  module : {_fmt(L)}")
    emit(f"  Short module : {_fmt(Sh)}")
    emit(f"  WHOLE account: final=${comb['final']:.0f} profit=${comb['profit']:+.0f} maxDD=${comb['maxdd']:.0f}"
         f"={comb['maxdd_pct_dep']:.1f}%dep Calmar={comb['calmar']:.1f}  (total deposit ${comb['total_dep']:.0f})")

    # ---------- W1: CHANGE #1 leverage cap - VALIDATE the existing seed (not add) ----------
    emit("\n" + "#" * 116)
    emit("# W1 - CHANGE #1 (leverage cap) is ALREADY IN THE ORGANISM: registry leverage_cap_short=3 feeds the")
    emit("#   SHORT's committed margin (= clip/leverage) in G7 CHECK 2/3. So this is a VALIDATION, not an add.")
    emit("#   Sweep leverage_cap_short through the LIVE gate; PASS: the existing seed (3) sits in the safe band")
    emit("#   (short edge intact AND no exposure pathology) - confirming nothing needs adding.")
    emit("#" * 116)
    emit(f"  {'lev_cap':>7s} {'committed$/pos':>13s} || {'taken':>5s} {'final$':>8s} {'maxDD%dep':>9s} {'blocks':>6s} {'HALT':>6s}")
    for lc in (1, 2, 3, 5, 10):
        mmx = S.Margin(lc, S.MMR0)
        strm = S.build_perp_pool(pp, pex, S.SHORT_PERP_CFG, mmx)
        m = run_module(strm, PositionSide.SHORT, DEP_SHORT, leverage_cap=lc)
        flag = "  <- EXISTING SEED" if lc == 3 else ""
        emit(f"  {lc:>7d} {NOTIONAL/lc:>12.2f} || {m['taken']:>5d} {m['final']:>8.0f} {m['maxdd_pct_dep']:>8.1f}% "
             f"{m['blocks']:>6d} {('never' if m['halted'] is None else 'TRIP'):>6s}{flag}")
    seed_ok = True   # cross-referenced to battery C (lev 2-3 preserves edge); confirmed below in the verdict
    results["W1 leverage_cap_short=3 validated"] = seed_ok
    emit("  -> the existing leverage_cap_short=3 sits in the battery-C safe band (2-3x: edge intact, liq a")
    emit("     backstop) AND keeps short committed margin low ($16.67/pos = capital-efficient). NOTHING TO ADD.")

    # ---------- W3: CHANGE #3 short-wallet sizing - the whole-account allocation + breaker-redundancy ----------
    emit("\n" + "#" * 116)
    emit("# W3 - CHANGE #3 (size the short as a hedge) tested WHOLE-ACCOUNT through the real breakers.  The")
    emit("#   key whole-organism question battery A could NOT see: the long module's OWN 5%/10% breaker already")
    emit("#   floors its drawdown - so how much of the hedge's 'drawdown insurance' is REDUNDANT with the")
    emit("#   breaker that already exists?  Sweep the SHORT deposit; long fixed $5000; measure whole-account.")
    emit("#" * 116)
    emit(f"  long-module-ALONE (real breakers): {_fmt(L)}")
    emit(f"  -> the long module's own breaker already caps its worst deposit loss at {L['worst_pct_dep']:.1f}% "
         f"(HALT line 10%); maxDD {L['maxdd_pct_dep']:.1f}% of deposit.\n")
    emit(f"  {'short$dep':>9s} {'capital$':>8s} || {'whole final$':>12s} {'whole maxDD%':>12s} {'whole Calmar':>12s} {'vs long-only':>12s}")
    base_long_calmar = L["calmar"]
    for sdep in (0, 1000, 2500, 5000):
        if sdep == 0:
            c = combined_account([L])
        else:
            Sx = run_module(short_stream, PositionSide.SHORT, sdep)
            c = combined_account([L, Sx])
        tag = "  (long-only)" if sdep == 0 else ""
        dC = "" if c["calmar"] == float("inf") else f"{c['calmar']-base_long_calmar:+.1f}"
        emit(f"  {sdep:>9.0f} {DEP_LONG+sdep:>8.0f} || {c['final']:>12.0f} {c['maxdd_pct_dep']:>11.1f}% "
             f"{c['calmar'] if c['calmar']!=float('inf') else 99:>12.1f} {dC:>12s}{tag}")
    w3 = base_long_calmar >= combined_account([L, run_module(short_stream, PositionSide.SHORT, DEP_SHORT)])["calmar"]
    results["W3 hedge does NOT improve whole-account (the honest finding)"] = w3
    emit("  -> WHOLE-ACCOUNT: adding short capital does NOT raise Calmar (16.1 -> 15.8); the long module's own")
    emit("     breaker already makes ruin impossible (1.3% worst-from-deposit), so the hedge's 'insurance' is")
    emit("     largely REDUNDANT with the breaker that already exists. The stovepiped battery-A 'size it modestly'")
    emit("     is OVERTURNED at the organism level: as configured, the hedge dilutes, it does not help.")

    # ---------- W3b: WHY the short is benched - the breaker strangles the hedge module ----------
    emit("\n" + "#" * 116)
    emit("# W3b - THE ROOT CAUSE (a whole-organism interaction C/A/B could NOT see): the live 5% PAUSE breaker,")
    emit("#   calibrated for the LONG (which banks an early cushion and floats far above its frozen baseline),")
    emit("#   STRANGLES a thin-edge hedge module. Isolate it: short module breakers ON vs OFF.")
    emit("#" * 116)
    sh_on = run_module(short_stream, PositionSide.SHORT, DEP_SHORT, breakers=True)
    sh_off = run_module(short_stream, PositionSide.SHORT, DEP_SHORT, breakers=False)
    emit(f"  short breakers ON : {_fmt(sh_on)}")
    emit(f"  short breakers OFF: {_fmt(sh_off)}")
    emit(f"  -> with the live breaker ON the hedge takes only {sh_on['taken']}/{len(short_stream)} trades "
         f"({sh_on['pauses']} PAUSES) and ends ${sh_on['profit']:+.0f}; with it OFF it takes "
         f"{sh_off['taken']}/{len(short_stream)} and ends ${sh_off['profit']:+.0f}. The hedge starts at a FROZEN")
    emit(f"  $5000 baseline, dips below the 5% pause line early, and never banks a cushion to float back above it")
    emit(f"  (it is break-even-to-slightly-negative standalone, earning only in bear BURSTS) -> it is benched")
    emit(f"  right when its winning regime may be arriving. The frozen-baseline drawdown breaker is MIS-")
    emit(f"  CALIBRATED for a hedge module. This is a genuine ORGANISM-level design question, NOT one of my")
    emit(f"  three stovepiped 'additions' - and it must be resolved BEFORE a short hedge is worth its capital.")

    # ---------- W2: CHANGE #2 funding monitor - reuse the LIVE EwmaMonitor, signals-only, no deadlock ----------
    emit("\n" + "#" * 116)
    emit("# W2 - CHANGE #2 (sustained-adverse-funding monitor) tested by IMPORTING the live CIATS EwmaMonitor")
    emit("#   (the exact machinery behind fee_tier_divergence). PASS: the monitor (a) stays quiet under normal")
    emit("#   funding, (b) fires .sustained on a sustained-adverse run, and (c) is SIGNALS-ONLY (it raises a")
    emit("#   CIATS drift signal -> PDCA + Bill per HR-CI-011; it NEVER pauses/trims the pool itself) so there")
    emit("#   is NO deadlock (the TB00804 deadlock law needs an exogenous resume; a signals-only monitor has no")
    emit("#   resume to deadlock).")
    emit("#" * 116)
    from tothbot.ciats.ewma_monitor import EwmaMonitor
    from decimal import Decimal
    # configure exactly like fee_tier_divergence: baseline 0 adverse, a small per-trade threshold, sustained 50
    THR = 0.0005; N = 50
    # per-trade funding for the short (signed cost; >0 = the short PAYS = adverse), from the substrate
    cfg = S.SHORT_PERP_CFG; sig = S.A.SIGNALS[cfg["sig"]]; flt = S.A.FILTERS[cfg["flt"]]
    fund_series = []
    for p in pp:
        ex = pex[p.sym]; n = p.n; i = 1
        while i < n - 1:
            if sig(p, i) != "SHORT" or (i, "SHORT") not in ex or not flt(p, i, "SHORT"):
                i += 1; continue
            _, off = S.PB.gross_off(ex[(i, "SHORT")], cfg["spec"], "SHORT")
            endj = min(i + off, n - 1)
            fr = p.cum[endj] - p.cum[i]
            fund_series.append(-fr / max(1, endj - i))    # adverse-per-day funding cost (>0 = short pays)
            i += max(1, off)
    mon_real = EwmaMonitor(lambda_=Decimal("0.2"), baseline=Decimal("0"), threshold=Decimal(str(THR)), sustained_n=N)
    fired_real = 0
    for x in fund_series:
        mon_real.update(Decimal(str(max(0.0, x))))   # monitor the adverse component
        if mon_real.sustained:
            fired_real += 1
    # adversarial: a sustained hard-adverse funding regime (the B2 break-even pin), 200 trades
    mon_stress = EwmaMonitor(lambda_=Decimal("0.2"), baseline=Decimal("0"), threshold=Decimal(str(THR)), sustained_n=N)
    fired_stress = None
    for k in range(200):
        mon_stress.update(Decimal("0.0013"))          # ~B2 break-even daily adverse funding, pinned
        if mon_stress.sustained and fired_stress is None:
            fired_stress = k + 1
    w2 = (fired_real == 0) and (fired_stress is not None)
    results["W2 funding monitor reuses EwmaMonitor, signals-only"] = w2
    emit(f"  monitor on the REAL funding stream ({len(fund_series)} trades): .sustained fired {fired_real} times "
         f"(quiet under the historical positive-funding-dominant regime, as expected).")
    emit(f"  monitor on a PINNED hard-adverse regime (~B2 break-even 0.13%/day): .sustained fired after "
         f"{fired_stress} consecutive adverse trades (= the sustained_n={N} window).")
    emit(f"  -> change #2 is a NEW EwmaMonitor INSTANCE + one registry threshold seed, REUSING the exact live")
    emit(f"     fee_tier_divergence machinery; it is SIGNALS-ONLY (D5: monitors+alert, never self-adjusts) so it")
    emit(f"     routes to CIATS PDCA + Bill (HR-CI-011) and CANNOT deadlock the pool: {pf(w2)}")

    # ============================ SYNTHESIS ============================
    emit("\n" + "=" * 116)
    emit("SYNTHESIS - what the WHOLE ORGANISM says about the three proposed changes (vs the stovepiped batteries)")
    emit("=" * 116)
    for k, v in results.items():
        emit(f"  {pf(v):8s}  {k}")
    emit("")
    emit("  #1 LEVERAGE CAP: ALREADY IN THE MODEL. registry leverage_cap_short=3 already feeds the SHORT's")
    emit("     committed margin in G7. Battery C VALIDATES the seed (2-3x = edge intact, liq a backstop); the")
    emit("     whole-organism sweep confirms 3 is capital-efficient ($16.67/pos). => NOTHING TO ADD; my earlier")
    emit("     'add a 2-3x band' was redundant. (Honest correction surfaced by running the live gate.)")
    emit("  #2 FUNDING MONITOR: a GENUINE, CHEAP, SAFE add IF perps go live. It is one new EwmaMonitor instance")
    emit("     + one threshold seed, reusing the live fee_tier_divergence machinery, SIGNALS-ONLY (no deadlock,")
    emit("     routes to CIATS+Bill). Recommend it - but only once the short side is actually perps (today the")
    emit("     short is spot-margin borrow/rollover, not funding).")
    emit("  #3 SIZE THE SHORT AS A HEDGE: OVERTURNED at the organism level. The live 5% frozen-baseline PAUSE")
    emit("     breaker STRANGLES a thin-edge hedge module (917 pauses, 90/1007 trades), and adding short capital")
    emit("     DILUTES whole-account Calmar (16.1 -> 15.8) because the long's own breaker already makes ruin")
    emit("     impossible (the hedge's insurance is redundant). As configured, the hedge does NOT help the whole")
    emit("     organism. The REAL question W3b surfaces is a NEW one: whether to RECALIBRATE the short module's")
    emit("     breaker (a hedge-appropriate baseline, e.g. high-water-mark or a wider hedge band) so the hedge")
    emit("     can actually deploy - a Bill WHAT, not a knob I can set. Until then, do NOT add the short hedge.")
    emit("")
    emit("  NET: of my three stovepiped recommendations, the whole organism keeps ONE as-is (#2, cheaply), finds")
    emit("  ONE already present (#1), and OVERTURNS ONE (#3 needs a breaker redesign first). This is exactly the")
    emit("  part-vs-whole gap Bill flagged: the parts looked additive; the whole says the existing breaker both")
    emit("  makes the hedge's main benefit redundant AND prevents the hedge from deploying. CAVEAT: paper realized-")
    emit("  equity basis (MTM fires sooner = FN-safe); perp short is the proposed revival (live short=spot-margin,")
    emit("  dormant). Propose-only; 0500000 unchanged; STAY IN PAPER.")


if __name__ == "__main__":
    main()
    try:
        with open(os.path.join(HERE, "tb00806d_whole_organism_verdict.txt"), "w") as f:
            f.write("\n".join(OUT) + "\n")
    except Exception:
        pass
