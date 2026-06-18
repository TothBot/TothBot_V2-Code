"""TB00778 - STRESS-TEST plain Supertrend + Bill's structure exits HARDER (durable or lucky?).

TB00777 found plain Supertrend (entry) + Bill's structure-stop / 3R-scale-out / run-the-winner (exit)
was the FIRST entry candidate positive OUT-OF-SAMPLE in BOTH halves AND BOTH directions - but on a single
contiguous 2yr daily window. Bill's TB00778 WHAT: is it durable across ERAS and TIMEFRAMES, or one lucky
window? This harness answers that without adding a single new indicator (TB00777 proved bolting parts on
churns/overfits; the edge, if real, is in the STRUCTURE).

On EACH available timeframe (daily ~2yr; 1h ~30d; the growing live 5m corpus):
  (A) WALK-FORWARD across M consecutive ERAS - the FIXED strategy must earn its keep in EACH era
      independently, not merely net over one contiguous window. Per-era n / ER(R) / RR / win% + per side.
  (B) PARAMETER PLATEAU - sweep Supertrend (period, mult) AROUND (10,3). A real edge is a PLATEAU
      (neighbours all positive); a lone positive point is overfit to one parameter spike.
  (C) BOOTSTRAP CI by pair (resample whole-pair trade streams) over the full window - is net E[R]>0
      robust to which pairs we happened to pick?
Structure-exit lookback windows are SCALED to each timeframe so the intraday test is FAIR.

Reuses tb00777.sim_trade (the structure exit) + _supertrend, and auto_strategy_search data/indicators.
Pure research, PAPER only; read-only on the corpus; public Kraken pulls only."""

from __future__ import annotations
import asyncio, os, importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
def _load(name, rel):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m); return m

A = _load("ass", os.path.join("..", "operations", "auto_strategy_search.py"))
T = _load("t777", "tb00777_structure_strategy.py")

MINN = 25          # min trades for a stats cell to be reported
PARAM_GRID = [(7, 3.0), (10, 2.0), (10, 3.0), (10, 4.0), (14, 3.0)]   # (10,3) is the centre
CENTRE = (10, 3.0)


def run_window(pairs, lo, hi):
    """Run Supertrend-flip entry + structure exit on entries in [lo,hi). p.stdir must be preset.
    Returns (recs, bypair): recs=[(entry_frac, netR, side)], bypair={sym:[netR,...]}."""
    recs = []; bypair = {}
    for p in pairs:
        c, h, l, n = p.c, p.h, p.l, p.n; rf, rs = p.sma50, p.sma200
        i = int(n * lo); end = int(n * hi)
        while i < end - 1:
            s = T.t_supertrend(p, i)
            if s is None: i += 1; continue
            r = T.sim_trade(c, h, l, rf, rs, i, s)
            if r is None: i += 1; continue
            netR, days, hit3, fs, jend = r
            recs.append((i / n, netR, s)); bypair.setdefault(p.sym, []).append(netR)
            i = max(i + 1, jend)
    return recs, bypair


def statR(recs):
    if len(recs) < MINN: return None
    rs = [r[1] for r in recs]; w = [x for x in rs if x > 0]; ls = [x for x in rs if x <= 0]
    avgw = sum(w) / len(w) if w else 0.0; avgl = -sum(ls) / len(ls) if ls else 1e-9
    def de(sd):
        v = [r[1] for r in recs if r[2] == sd]; return len(v), (sum(v) / len(v) if v else 0.0)
    ln, le = de("LONG"); sn, se = de("SHORT")
    return dict(n=len(rs), ER=sum(rs) / len(rs), win=100 * len(w) / len(rs),
                rr=avgw / avgl if avgl > 0 else 0.0, ln=ln, le=le, sn=sn, se=se)


def bootR(bypair, iters=600, seed=7):
    """Block bootstrap: resample whole-pair trade streams. CI in R units."""
    pp = list(bypair); m = len(pp)
    if m < 3: return None
    s = seed; vals = []
    for _ in range(iters):
        tot = 0.0; cnt = 0
        for _ in range(m):
            s = (1103515245 * s + 12345) & 0x7FFFFFFF; pr = pp[(s * m) >> 31]
            tot += sum(bypair[pr]); cnt += len(bypair[pr])
        vals.append(tot / (cnt or 1))
    vals.sort(); return vals[int(.05 * iters)], vals[iters // 2], vals[int(.95 * iters)], m


def prep_pairs(data, l_init, l_run):
    pairs = []
    for sym, (c, h, l, t) in data.items():
        if len(c) < 220: continue
        p = A.P(); p.sym = sym; p.c = c; p.h = h; p.l = l; p.n = len(c)
        p.sma50 = A.sma(c, 50); p.sma200 = A.sma(c, 200)
        pairs.append(p)
    return pairs


def analyse(label, pairs, l_init, l_run, m_eras):
    T.L_INIT = l_init; T.L_RUN = l_run; T.SCALE_AT = 3.0; T.USE_REVERSAL = True
    out = [f"\n{'='*92}\n[{label}]  pairs={len(pairs)}  bars/pair~{pairs[0].n if pairs else 0}  "
           f"structure L_init={l_init} L_run={l_run}  scale@3R rev=50/200"]
    if len(pairs) < 8:
        out.append(f"  ACCUMULATING - only {len(pairs)} pairs with >=220 bars (need >=8). Skipping."); return out

    # (B) PARAMETER PLATEAU over the full window
    out.append(f"\n  (B) PARAMETER PLATEAU (full window)  -- a real edge is a plateau, not one spike")
    out.append(f"      {'super(p,m)':12s} {'n':>5s} {'ER(R)':>7s} {'win%':>5s} {'RR':>5s}  "
               f"{'L:n@ER':>14s} {'S:n@ER':>14s}  boot5/50/95(R)")
    centre_recs = centre_bypair = None
    for period, mult in PARAM_GRID:
        for p in pairs: p.stdir = T._supertrend(p.h, p.l, p.c, period, mult)
        recs, bypair = run_window(pairs, 0.0, 1.0); st = statR(recs)
        if (period, mult) == CENTRE: centre_recs, centre_bypair = recs, bypair
        if not st: out.append(f"      ({period},{mult}) too few"); continue
        b = bootR(bypair); bs = f"[{b[0]:+.3f}..{b[2]:+.3f}]" if b else "n/a"
        star = " <-centre" if (period, mult) == CENTRE else ""
        out.append(f"      ({period},{mult}){'':4s} {st['n']:>5d} {st['ER']:>+7.3f} {st['win']:>5.0f} "
                   f"{st['rr']:>5.2f}  L:{st['ln']:>3d}@{st['le']:>+5.2f} S:{st['sn']:>3d}@{st['se']:>+5.2f}  {bs}{star}")

    # (A) WALK-FORWARD across eras at the CENTRE params
    for p in pairs: p.stdir = T._supertrend(p.h, p.l, p.c, *CENTRE)
    out.append(f"\n  (A) WALK-FORWARD across {m_eras} consecutive eras @ super{CENTRE}  "
               f"-- must earn its keep in EACH era")
    out.append(f"      {'era':>4s} {'window':>11s} {'n':>5s} {'ER(R)':>7s} {'win%':>5s} {'RR':>5s}  "
               f"{'L:n@ER':>13s} {'S:n@ER':>13s}")
    pos_eras = counted = 0
    for e in range(m_eras):
        lo, hi = e / m_eras, (e + 1) / m_eras
        recs, _ = run_window(pairs, lo, hi); st = statR(recs)
        if not st:
            out.append(f"      {e+1:>4d} {lo:.2f}-{hi:.2f}   too few"); continue
        counted += 1; pos = st['ER'] > 0; pos_eras += 1 if pos else 0
        flag = "" if pos else "  <-- LOSING ERA"
        out.append(f"      {e+1:>4d} {lo:>5.2f}-{hi:<4.2f} {st['n']:>5d} {st['ER']:>+7.3f} {st['win']:>5.0f} "
                   f"{st['rr']:>5.2f}  L:{st['ln']:>2d}@{st['le']:>+5.2f} S:{st['sn']:>2d}@{st['se']:>+5.2f}{flag}")

    # verdict
    if centre_recs:
        st = statR(centre_recs); b = bootR(centre_bypair)
        boot_sig = b and b[0] > 0
        twoside = st['ln'] >= 8 and st['sn'] >= 8
        dir_ok = (min(st['le'], st['se']) > 0 and min(st['le'], st['se']) >= 0.2 * max(st['le'], st['se'])) if twoside else (st['ER'] > 0)
        era_ok = counted >= 3 and pos_eras >= counted - 1
        out.append(f"\n  VERDICT @ super{CENTRE}: full-window ER {st['ER']:+.3f}R rr {st['rr']:.2f} win {st['win']:.0f}% | "
                   f"boot {'SIG(>0)' if boot_sig else 'CI~0'} | dir {'OK' if dir_ok else 'ONE-SIDED'} | "
                   f"eras {pos_eras}/{counted} positive {'OK' if era_ok else 'FRAGILE'}")
        durable = boot_sig and dir_ok and era_ok and st['rr'] >= 1.5
        out.append(f"  => {'DURABLE on this timeframe' if durable else 'NOT durable here (see flags)'}")
    return out


async def main():
    sources = []
    # daily ~2yr (the TB00777 window), 6 eras ~4mo each
    d = await A.fetch_kraken(1440)
    sources.append(("DAILY 1440m", d, 10, 5, 6))
    # 1h ~30d, structure scaled (~1 day / ~8h), 5 eras ~6d each
    h1 = await A.fetch_kraken(60)
    sources.append(("1H 60m", h1, 24, 8, 5))
    # live 5m corpus (VPS only); structure ~4h / ~1h, 5 eras
    corp = A.load_corpus()
    if corp:
        sources.append(("5M corpus", corp, 48, 12, 5))

    print("TB00778 walk-forward stress test: plain Supertrend entry + Bill's structure exit.")
    print("ER(R)=net expectancy/trade in R after fees+rollover. RR=avg win/avg loss. "
          "DURABLE = boot CI>0 AND both sides positive AND positive in >=(eras-1) eras AND RR>=1.5.")
    for label, data, li, lr, me in sources:
        pairs = prep_pairs(data, li, lr)
        try:
            for line in analyse(label, pairs, li, lr, me): print(line)
        except Exception as e:
            print(f"\n[{label}] ERROR {type(e).__name__}: {e}")

    if not corp:
        print("\n[5M corpus] not present locally (lives on the VPS); run there for the 5m test once "
              ">=8 pairs reach >=220 bars.")


if __name__ == "__main__":
    asyncio.run(main())
