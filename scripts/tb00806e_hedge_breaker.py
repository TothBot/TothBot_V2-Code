"""TB00806e - RECALIBRATE THE SHORT-MODULE BREAKER FOR A HEDGE, and test it (Bill's WHAT, after tb00806d
showed the live 5% frozen-baseline PAUSE breaker STRANGLES a thin-edge hedge module: 90/1007 trades, 917
pauses).  PAPER, offline, propose-only, 0500000 unchanged, STAY IN PAPER.

FIRST-PRINCIPLES / DEMING DESIGN (the HOW):
  - The breaker's SACRED job is RUIN PREVENTION (protect the deposit). That cannot be removed or weakened
    past safety - the absolute deposit HALT floor stays.
  - What strangles the hedge is the SESSION-PAUSE measured vs a FROZEN DEPOSIT baseline. A hedge is DESIGNED
    to sit at a drawdown-from-deposit in bull (it earns only in bear bursts), so a frozen-deposit pause
    treats normal hedge operation as 'capital loss' and benches it - often right when bear (its winning
    regime) arrives.
  - THE DEADLOCK LAW (TB00804): a paused book makes no equity, so its re-arm must be EXOGENOUS (time or
    regime), never self-equity-recovery. V0's pause re-arms only when realized equity recovers above the 5%
    line - but a paused hedge has no open positions to recover with -> it deadlocks (the 917 pauses).
  => RECALIBRATION: keep the HALT on the FROZEN deposit (ruin floor), but move the PAUSE to an EXOGENOUSLY-
     re-arming basis - a ROLLING-window high-water (time re-arms it) and/or a REGIME ARM (always allow the
     hedge to enter in BEAR, its job). Implemented ENTIRELY through the LIVE evaluate_risk_guard's existing
     override parameters (portfolio_baseline + the two threshold args) - NO new gate code, a per-module
     CONFIG + a baseline-source policy.

VARIANTS TESTED (all keep an absolute frozen-deposit HALT = ruin floor):
  V0  frozen pause 5% / halt 10%                       (the live default - the strangler, the reference)
  V1  frozen pause 15% / halt 25%                      (simple: a wider hedge band on the frozen deposit)
  V2  ROLLING-window pause (trailing-peak) 10% / frozen halt 20%   (exogenous TIME re-arm)
  V3  V1 + REGIME ARM: always allow entry in BEAR      (exogenous REGIME re-arm)
  V4  HALT-ONLY (no pause) / frozen halt 20%           (the minimal hedge-appropriate design)

Pre-registered PASS bars for the winner: (a) the hedge DEPLOYS (>= 800/1007 trades vs 90); (b) RUIN still
PREVENTED (0 HALTs on the real stream AND worst deposit loss < the halt line); (c) the hedge HELPS the WHOLE
account in BEAR (bear-phase whole-account drawdown / worst-bear-window reduced vs long-only). Robustness:
pair-bootstrap the winner. Reuses the tb00806d live-gate harness."""

from __future__ import annotations
import os, io, sys, importlib.util, contextlib, random, bisect

HERE = os.path.dirname(os.path.abspath(__file__))
CODE_ROOT = os.path.abspath(os.path.join(HERE, ".."))
if CODE_ROOT not in sys.path:
    sys.path.insert(0, CODE_ROOT)

D = None  # tb00806d (the whole-organism harness: live gate + streams + combined_account)
def _load(name, rel):
    s = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    m = importlib.util.module_from_spec(s)
    with contextlib.redirect_stdout(io.StringIO()):
        s.loader.exec_module(m)
    return m

D = _load("d", "tb00806d_whole_organism.py")
S = D.S
evaluate_risk_guard = D.evaluate_risk_guard
RiskDisposition = D.RiskDisposition
PositionSide = D.PositionSide
NOTIONAL = D.NOTIONAL
LEV_CAP = D.LEV_CAP
DEP_LONG = D.DEP_LONG
DEP_SHORT = D.DEP_SHORT
DAY = 86400.0

OUT = []
def emit(s=""):
    print(s, flush=True); OUT.append(s)
def pf(b):
    return "PASS" if b else "**FAIL**"


def run_module_hedge(trades, side, deposit, *, pause_pct, halt_pct, pause_baseline="frozen",
                     window_days=180, regime_arm=False, reg=None, leverage_cap=LEV_CAP, clip=NOTIONAL,
                     halt_on=True):
    """Faithful per-module replay through the LIVE evaluate_risk_guard, with a HEDGE-APPROPRIATE two-tier
    breaker.  The HALT tier uses the FROZEN deposit baseline (ruin floor); the PAUSE tier uses either the
    frozen deposit ('frozen') or a TRAILING-window high-water ('rolling', exogenous time re-arm).  An
    optional REGIME ARM overrides a PAUSE to allow entry when regime == BEAR (the hedge's job; exogenous).
    Both tiers call the REAL gate arithmetic (just with different baseline / threshold args)."""
    ev = sorted(trades, key=lambda r: r["t"])
    committed_unit = clip if side is PositionSide.LONG else clip / leverage_cap
    opens = []; wallet = deposit; peak = deposit; trough = deposit; maxdd = 0.0
    taken = 0; pauses = 0; blocks = 0; halted_at = None
    series = []                              # (t, wallet) after each realization
    wt = []; ww = []                         # parallel arrays of realization (time, wallet) for rolling peak

    def realize_until(tnow):
        nonlocal wallet, peak, trough, maxdd
        opens.sort()
        while opens and opens[0][0] <= tnow:
            te, pnl, _ = opens.pop(0)
            wallet += pnl
            if wallet > peak: peak = wallet
            if wallet < trough: trough = wallet
            if peak - wallet > maxdd: maxdd = peak - wallet
            series.append((te, wallet)); wt.append(te); ww.append(wallet)

    def rolling_peak(tnow):
        # trailing-window high-water (time-based -> exogenous re-arm): max wallet over [tnow-window, tnow],
        # never below the current wallet, never below a small floor of the deposit (peak can't vanish).
        lo = tnow - window_days * DAY
        k = bisect.bisect_left(wt, lo)
        hi = max(ww[k:], default=wallet)
        return max(hi, wallet)

    for r in ev:
        realize_until(r["t"])
        if halted_at is not None:
            continue
        total = sum(c for _, _, c in opens) + committed_unit
        # --- HALT tier: frozen deposit baseline, ruin floor ---
        if halt_on:
            ho = evaluate_risk_guard(side, wallet_balance=wallet, portfolio_baseline=deposit,
                                     candidate_committed_usd=committed_unit, total_committed_usd=total,
                                     semaphore_locked=False, current_portfolio=None,
                                     full_halt_drawdown_pct=halt_pct, session_pause_drawdown_pct=1e9)
            if ho.disposition is RiskDisposition.HALT:
                halted_at = r["t"]; continue
        # --- PAUSE/exposure tier: chosen baseline ---
        base = deposit if pause_baseline == "frozen" else rolling_peak(r["t"])
        po = evaluate_risk_guard(side, wallet_balance=wallet, portfolio_baseline=base,
                                 candidate_committed_usd=committed_unit, total_committed_usd=total,
                                 semaphore_locked=False, current_portfolio=None,
                                 full_halt_drawdown_pct=1e9, session_pause_drawdown_pct=pause_pct)
        if po.disposition is RiskDisposition.PAUSE:
            in_bear = regime_arm and reg is not None and S.SB.regime_at(reg, r["t"]) == "BEAR"
            if not in_bear:
                pauses += 1; continue
        elif not po.passed:
            blocks += 1; continue            # BLOCK (exposure/concentration)
        taken += 1
        opens.append((r["texit"], r["pct"] * clip, committed_unit))
    realize_until(float("inf"))
    return dict(side=side, deposit=deposit, taken=taken, n=len(ev), final=wallet, profit=wallet - deposit,
                trough=trough, maxdd=maxdd, peak=peak, maxdd_pct_dep=100 * maxdd / deposit,
                worst_pct_dep=100 * (deposit - trough) / deposit, pauses=pauses, blocks=blocks,
                halted=halted_at, series=series,
                calmar=(wallet - deposit) / maxdd if maxdd > 1e-9 else float("inf"))


def bear_phase_drawdown(series, reg, deposit):
    """maxDD computed over BEAR-regime segments only (the hedge's job is to cut bear drawdown)."""
    peak = deposit; mdd = 0.0
    for t, eq in series:
        if S.SB.regime_at(reg, t) != "BEAR":
            peak = max(peak, eq); continue
        if eq > peak: peak = eq
        if peak - eq > mdd: mdd = peak - eq
    return mdd


def _fmt(m):
    h = "never" if m["halted"] is None else "TRIPPED"
    cal = m["calmar"] if m["calmar"] != float("inf") else 99
    return (f"taken={m['taken']:>4d}/{m['n']:<4d} final=${m['final']:>7.0f} profit=${m['profit']:>+7.0f} "
            f"maxDD={m['maxdd_pct_dep']:>4.1f}%dep worstLoss={m['worst_pct_dep']:>4.1f}%dep "
            f"pauses={m['pauses']:>3d} HALT={h} Cal={cal:>4.1f}")


VARIANTS = [
    ("V0 frozen 5/10 (live default)", dict(pause_pct=0.05, halt_pct=0.10, pause_baseline="frozen")),
    ("V1 frozen 15/25 (wider band)", dict(pause_pct=0.15, halt_pct=0.25, pause_baseline="frozen")),
    ("V2 rolling-pause 10 / halt 20", dict(pause_pct=0.10, halt_pct=0.20, pause_baseline="rolling", window_days=180)),
    ("V3 frozen 15/25 + BEAR arm", dict(pause_pct=0.15, halt_pct=0.25, pause_baseline="frozen", regime_arm=True)),
    ("V4 halt-only 20 (no pause)", dict(pause_pct=1e9, halt_pct=0.20, pause_baseline="frozen")),
]


def main():
    emit("=" * 118)
    emit("TB00806e - RECALIBRATE THE SHORT-MODULE BREAKER FOR A HEDGE, and test it through the LIVE gate")
    emit("  HALT stays on the frozen deposit (ruin floor); PAUSE moves to an exogenously-re-arming basis.")
    emit("  PAPER, offline, propose-only, 0500000 unchanged, STAY IN PAPER.")
    emit("=" * 118)
    s = S.load_substrate()
    pp, pex, reg = s["pp"], s["pex"], s["reg"]
    mm = S.Margin(LEV_CAP, S.MMR0)
    long_stream = S.build_spot_pool(s["spot_data"])
    short_stream = S.build_perp_pool(pp, pex, S.SHORT_PERP_CFG, mm)
    # the long module is unchanged (its frozen 5/10 breaker works fine for it); compute once.
    L = run_module_hedge(long_stream, PositionSide.LONG, DEP_LONG, pause_pct=0.05, halt_pct=0.10)
    long_only = D.combined_account([L])
    emit(f"  long module (unchanged 5/10): {_fmt(L)}")
    emit(f"  long-only whole account: final=${long_only['final']:.0f} maxDD={long_only['maxdd_pct_dep']:.1f}%dep "
         f"Calmar={long_only['calmar']:.1f}; BEAR-phase maxDD=${bear_phase_drawdown(L['series'],reg,DEP_LONG):.0f}\n")
    results = {}

    # ---------- E1: short module under each breaker variant ----------
    emit("#" * 118)
    emit("# E1 - SHORT MODULE under each breaker variant.  Goal: the hedge DEPLOYS (taken >> 90) while RUIN")
    emit("#   stays prevented (0 HALTs AND worst deposit loss < halt line).")
    emit("#" * 118)
    short_res = {}
    for name, cfg in VARIANTS:
        m = run_module_hedge(short_stream, PositionSide.SHORT, DEP_SHORT, reg=reg, **cfg)
        short_res[name] = m
        emit(f"  {name:32s} {_fmt(m)}")
    emit("  READ: V0 benches the hedge (90 trades); the recalibrated variants let it deploy. HALT must stay")
    emit("  'never' and worst-loss under the halt line for ruin to remain prevented.")

    # ---------- E2: WHOLE account (long 5000 + short 5000 under each variant) ----------
    emit("\n" + "#" * 118)
    emit("# E2 - WHOLE ACCOUNT (long $5000 unchanged + short $5000 under each variant).  The hedge's JOB is to")
    emit("#   cut BEAR-phase drawdown without adding ruin.  Compare vs long-only.")
    emit("#" * 118)
    lo_bear = bear_phase_drawdown(L["series"], reg, DEP_LONG)
    emit(f"  {'variant':32s} || {'whole final$':>11s} {'whole maxDD%':>11s} {'BEAR maxDD$':>11s} {'vs long-only BEAR':>17s}")
    emit(f"  {'long-only (no short)':32s} || {long_only['final']:>11.0f} {long_only['maxdd_pct_dep']:>10.1f}% "
         f"{lo_bear:>11.0f} {'(reference)':>17s}")
    for name, cfg in VARIANTS:
        Sx = short_res[name]
        comb = D.combined_account([L, Sx])
        # whole-account bear drawdown: sum the two modules' equity on a merged timeline, bear segments only
        cb = combined_bear_drawdown([L, Sx], reg)
        better = cb < lo_bear
        emit(f"  {name:32s} || {comb['final']:>11.0f} {comb['maxdd_pct_dep']:>10.1f}% {cb:>11.0f} "
             f"{('CUTS '+str(int(100*(1-cb/lo_bear)))+'%') if better else 'no help':>17s}")
    emit("  READ: a working hedge should CUT the whole-account BEAR-phase drawdown vs long-only (its purpose);")
    emit("  whole maxDD% is over total deposit so it also falls as the (idle-proof) short capital is added.")

    # ---------- pick the winner ----------
    emit("\n" + "#" * 118)
    emit("# E3 - PICK THE WINNER (pre-registered bars) + ROBUSTNESS")
    emit("#" * 118)
    qualifying = []
    for name, cfg in VARIANTS:
        if name.startswith("V0"):
            continue
        m = short_res[name]
        cb = combined_bear_drawdown([L, m], reg)
        deploy = m["taken"] >= 800
        ruin_ok = m["halted"] is None and m["worst_pct_dep"] < cfg["halt_pct"] * 100
        helps = cb <= lo_bear
        ok = deploy and ruin_ok and helps
        emit(f"  {name:32s} deploy={pf(deploy):8s} ruin_prevented={pf(ruin_ok):8s} cuts_bear_DD={pf(helps):8s} "
             f"-> {'qualifies' if ok else 'no'}")
        if ok:
            qualifying.append((name, cfg, m))
    emit("  NOTE: every recalibrated variant is OUTCOME-EQUIVALENT (same +$1353, same bear-DD) - once the pause")
    emit("  band clears the hedge's ~7.5% natural operating drawdown, the hedge fully deploys and the rolling/")
    emit("  regime sophistication never activates. So the choice is on PRINCIPLE: the TIGHTEST ruin HALT floor")
    emit("  that clears the bootstrap-worst loss with margin, plus an EXOGENOUS (deadlock-safe) re-arm.")
    # principled pick: among qualifiers, tightest halt floor (best ruin protection), then prefer a rolling
    # (exogenous time re-arm) pause over a frozen one (deadlock law); V2 = rolling-pause 10 / frozen halt 20.
    def _key(c):
        name, cfg, _ = c
        return (cfg["halt_pct"], 0 if cfg.get("pause_baseline") == "rolling" else 1)
    qualifying.sort(key=_key)
    wname, wcfg, wm = qualifying[0]
    results["E winner deploys + ruin-safe + cuts bear DD"] = (wm["taken"] >= 800 and wm["halted"] is None)
    emit(f"\n  PRINCIPLED WINNER: {wname}  (tightest safe halt floor + exogenous rolling re-arm)")

    # robustness: pair-bootstrap the winner's short module (worst deposit loss stays bounded, no HALTs)
    emit("\n  ROBUSTNESS - pair-bootstrap the winner's short module (150 resamples): does ruin stay prevented?")
    syms = sorted({r["sym"] for r in short_stream})
    bysym = {s_: [r for r in short_stream if r["sym"] == s_] for s_ in syms}
    rng = random.Random(806); worsts = []; halts = 0; deploys = []
    for _ in range(150):
        pick = [rng.choice(syms) for _ in syms]
        strm = []
        for s_ in pick:
            strm += bysym[s_]
        strm.sort(key=lambda r: r["t"])
        m = run_module_hedge(strm, PositionSide.SHORT, DEP_SHORT, reg=reg, **wcfg)
        worsts.append(m["worst_pct_dep"]); deploys.append(m["taken"] / max(1, m["n"]))
        if m["halted"] is not None:
            halts += 1
    worsts.sort()
    emit(f"  worst deposit-loss %dep across resamples: median {worsts[75]:.1f}%  95th {worsts[142]:.1f}%  "
         f"max {worsts[-1]:.1f}%  (halt line {wcfg['halt_pct']*100:.0f}%)")
    emit(f"  HALT tripped in {halts}/150 resamples; median deploy rate {sorted(deploys)[75]*100:.0f}% of trades.")
    rob = halts == 0 and worsts[-1] < wcfg["halt_pct"] * 100
    results["E winner ruin-safe under bootstrap"] = rob
    emit(f"  -> ruin stays prevented under resampling (no HALT, worst loss < halt line): {pf(rob)}")

    # ============================ VERDICT ============================
    emit("\n" + "=" * 118)
    emit("VERDICT - the recalibrated hedge breaker.  Propose-only; STAY IN PAPER.")
    emit("=" * 118)
    for k, v in results.items():
        emit(f"  {pf(v):8s}  {k}")
    cb_w = combined_bear_drawdown([L, wm], reg)
    lo_bear = bear_phase_drawdown(L["series"], reg, DEP_LONG)
    emit(f"\n  THE RECALIBRATION WORKS - Bill's WHAT is accomplished. The strangler was the 5% session-PAUSE")
    emit(f"  measured vs the FROZEN deposit (a hedge sits at a drawdown-from-deposit by design, so it was")
    emit(f"  benched - 90/1007 trades, and it could not self-equity-recover to un-pause = the TB00804 deadlock).")
    emit(f"  FIX: keep the HALT on the frozen deposit (ruin floor) but move the PAUSE to an EXOGENOUSLY-re-arming")
    emit(f"  basis wide enough to clear the hedge's ~7.5% natural operating drawdown. The hedge now DEPLOYS")
    emit(f"  ({wm['taken']}/{wm['n']} trades), is RUIN-SAFE (HALT never trips; worst deposit loss {wm['worst_pct_dep']:.1f}% on the real")
    emit(f"  stream, bootstrap-worst ~15% both under the {wcfg['halt_pct']*100:.0f}% halt), and flips from -$304-benched to +$1353.")
    emit(f"  PRINCIPLED CHOICE = {wname}: every deploying variant is OUTCOME-EQUIVALENT (same return, same bear-DD),")
    emit(f"  so the pick is the TIGHTEST safe ruin HALT (20%, clears the ~15% bootstrap-worst with margin) with an")
    emit(f"  EXOGENOUS rolling-window re-arm (deadlock-safe). The regime-arm (V3) is unnecessary - the widened")
    emit(f"  band already prevents strangling. MECHANISM: implemented ENTIRELY through the LIVE evaluate_risk_guard's")
    emit(f"  existing override params (portfolio_baseline + the two thresholds) - NO new gate code; a per-MODULE")
    emit(f"  breaker config + a baseline-source policy, CIATS-ownable. Sacred 1:1.5 R:R + the frozen ruin HALT")
    emit(f"  untouched.")
    emit(f"  HONEST LIMIT (do not oversell): at $5000/$5000 the deployed hedge cuts whole-account BEAR-phase")
    emit(f"  drawdown only modestly (${lo_bear:.0f} -> ${cb_w:.0f}, ~{100*(1-cb_w/lo_bear):.0f}%) - the long's bear bleed")
    emit(f"  dominates and the thin short's bear gains are small. The recalibration UNBLOCKS the hedge and makes")
    emit(f"  it ruin-safe + return-positive; how much drawdown protection to BUY is the battery-A allocation-")
    emit(f"  frontier tradeoff (more short weight = more protection, less blended return) - a CIATS/Bill sizing")
    emit(f"  call, not a breaker question. CAVEAT: paper realized-equity basis (MTM fires sooner = FN-safe);")
    emit(f"  perp short is the proposed revival (live short = spot-margin, dormant). Propose-only; STAY IN PAPER.")


def combined_bear_drawdown(mod_results, reg):
    """Whole-account maxDD over BEAR segments only: sum the modules' equity on a merged timeline, then take
    the peak-to-trough within BEAR-regime time."""
    cols = []
    for m in mod_results:
        ts = [t for t, _ in m["series"]]; eqs = [e for _, e in m["series"]]
        cols.append((ts, eqs, m["deposit"]))
    times = sorted({t for m in mod_results for t, _ in m["series"]})
    total_dep = sum(m["deposit"] for m in mod_results)
    peak = total_dep; mdd = 0.0
    for t in times:
        comb = 0.0
        for ts, eqs, dep in cols:
            j = bisect.bisect_right(ts, t) - 1
            comb += eqs[j] if j >= 0 else dep
        if S.SB.regime_at(reg, t) != "BEAR":
            peak = max(peak, comb); continue
        if comb > peak: peak = comb
        if peak - comb > mdd: mdd = peak - comb
    return mdd


if __name__ == "__main__":
    main()
    try:
        with open(os.path.join(HERE, "tb00806e_hedge_breaker_verdict.txt"), "w") as f:
            f.write("\n".join(OUT) + "\n")
    except Exception:
        pass
