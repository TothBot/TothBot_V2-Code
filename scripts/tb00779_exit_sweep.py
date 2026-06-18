"""TB00779 - the EXIT / STOP / SPEED sweep (Bill's 3 tests).

Hold the ENTRY strategy FIXED (the live-bot-style EMA-cross trend trigger) and vary ONLY the exit. Answers
Bill's three questions head-to-head, all measured in R (1R = the stop distance = "for each $1 risked"), so
the 1:1.5 goal is directly readable:

  Q1 TAKE PROFIT vs HOLD-AND-HOPE: does banking profit beat just holding to a trend reversal?
     policies: hold (no target, exit on reversal/stop) | tp1.5R / tp2R / tp3R (take FULL profit at the
     target) | scale3R (take HALF at +3R, run the rest to reversal/stop = Bill's "take SOME profit").
  Q2 STOP SIZE: sweep the stop from TINY (0.5x ATR ~ the jitter the live bot uses) out to WIDE (5x ATR),
     ATR = the standard "real price swing" measure. Which size stops noise from tagging us without
     bleeding too much when wrong?
  Q3 SPEED: run the whole sweep on every timeframe Kraken serves (5m, 15m, 30m, 1h, 4h, 1d) - which speed
     trades this strategy best?

Rigor: nested in-sample(0-0.5)/out-of-sample(0.5-1.0), per-direction, era stability, block-bootstrap by
pair, fees (taker x2 + short open) + per-bar short rollover - all charged in R. Reuses auto_strategy_search
data/indicators/fees. PAPER research only; public Kraken pulls only.

CAVEAT on speed: Kraken serves ~720 bars per timeframe, so a 5m run covers ~2.5 days (one regime) while a
1d run covers ~2 years (many regimes). Shorter speeds are therefore more regime-fragile here - the live 5m
corpus on the VPS will give a longer 5m window as it grows. Reported honestly per timeframe."""

from __future__ import annotations
import asyncio, os, importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("ass", os.path.join(HERE, "..", "operations", "auto_strategy_search.py"))
A = importlib.util.module_from_spec(spec); spec.loader.exec_module(A)

TAKER = A.TAKER; OPEN_FEE = A.OPEN_FEE; ROLL_DAY = A.ROLL_DAY
STOPS = [0.5, 1.0, 1.5, 2.0, 3.0, 5.0]                 # ATR multiples = stop SIZE (Q2)
POLICIES = ["hold", "tp1.5", "tp2", "tp3", "scale3"]   # Q1
TP = {"tp1.5": 1.5, "tp2": 2.0, "tp3": 3.0}
MINN = 25


def t_emacross_flip(p, i):
    """Live-bot-style entry: enter on the EMA12/26 cross (the crossover EVENT)."""
    a, b, pa, pb = p.e12[i], p.e26[i], p.e12[i - 1], p.e26[i - 1]
    if None in (a, b, pa, pb): return None
    if pa <= pb and a > b: return "LONG"
    if pa >= pb and a < b: return "SHORT"
    return None

def t_mom_flip(p, i):
    """Alt entry for a robustness cross-check: 20-bar momentum crossing +/-5%."""
    m, pm = p.mom[i], p.mom[i - 1]
    if m is None or pm is None: return None
    if pm <= 0.05 and m > 0.05: return "LONG"
    if pm >= -0.05 and m < -0.05: return "SHORT"
    return None

ENTRIES = {"emacross": t_emacross_flip, "momentum": t_mom_flip}


def sim(p, i, side, s_mult, policy, roll_per_bar):
    """One trade from close[i]. 1R = s_mult*ATR. Returns (netR, off, side) or None."""
    c, h, l, n = p.c, p.h, p.l, p.n
    if p.atr[i] is None: return None
    e = c[i]; atrf = p.atr[i] / e
    if atrf <= 0: return None
    R = s_mult * atrf                       # 1R as a price fraction
    tp = TP.get(policy)
    pos = 1.0; realizedR = 0.0; scaled = False; legs = 2; off = n - 1 - i
    for o, j in enumerate(range(i + 1, n), 1):
        adv = ((h[j] - e) if side == "SHORT" else (e - l[j])) / e   # adverse excursion (toward stop)
        fav = ((e - l[j]) if side == "SHORT" else (h[j] - e)) / e   # favorable excursion (toward profit)
        # 1) adverse FIRST (conservative): stop tagged?
        if adv >= R:
            realizedR += pos * (-1.0); off = o; break
        favR = fav / R
        # 2) full take-profit target
        if tp is not None and favR >= tp:
            realizedR += pos * tp; pos = 0.0; legs += 1; off = o; break
        # 3) scale-out: bank HALF at +3R, let the rest run
        if policy == "scale3" and not scaled and favR >= 3.0:
            realizedR += 0.5 * 3.0; pos = 0.5; scaled = True; legs += 1
        # 4) reversal exit (EMA flip against us) closes whatever remains
        rev = p.e12[j] is not None and ((p.e12[j] < p.e26[j]) if side == "LONG" else (p.e12[j] > p.e26[j]))
        if rev and pos > 0:
            closeR = (((e - c[j]) if side == "SHORT" else (c[j] - e)) / e) / R
            realizedR += pos * closeR; pos = 0.0; off = o; break
    else:
        if pos > 0:
            j = n - 1; closeR = (((e - c[j]) if side == "SHORT" else (c[j] - e)) / e) / R
            realizedR += pos * closeR
    fee_frac = TAKER * legs + (OPEN_FEE if side == "SHORT" else 0.0)
    roll_frac = (roll_per_bar * off) if side == "SHORT" else 0.0
    netR = realizedR - (fee_frac + roll_frac) / R
    return netR, off, side, R   # R = stop distance as a fraction of price (for % on capital)


def run_combo(pairs, sig, s_mult, policy, roll_per_bar):
    recs = []; bypair = {}
    for p in pairs:
        i = 1; n = p.n
        while i < n - 1:
            s = sig(p, i)
            if s is None: i += 1; continue
            r = sim(p, i, s, s_mult, policy, roll_per_bar)
            if r is None: i += 1; continue
            netR, off, side, rfrac = r
            recs.append((i / n, netR, side, netR * rfrac)); bypair.setdefault(p.sym, []).append(netR)
            i = max(i + 1, i + off)
    return recs, bypair


def statR(recs, lo=0.0, hi=1.0):
    sel = [r for r in recs if lo <= r[0] < hi]
    if len(sel) < MINN: return None
    rr = [r[1] for r in sel]; sd = [r[2] for r in sel]
    pct = [(r[3] if len(r) > 3 else 0.0) for r in sel]   # per-trade return on capital deployed (fraction)
    w = [x for x in rr if x > 0]; ls = [x for x in rr if x <= 0]
    avgw = sum(w) / len(w) if w else 0.0; avgl = -sum(ls) / len(ls) if ls else 1e-9
    ln = sum(1 for s in sd if s == "LONG"); sn = len(sd) - ln
    le = (sum(rr[k] for k in range(len(sd)) if sd[k] == "LONG") / ln) if ln else 0.0
    se = (sum(rr[k] for k in range(len(sd)) if sd[k] == "SHORT") / sn) if sn else 0.0
    lep = (100 * sum(pct[k] for k in range(len(sd)) if sd[k] == "LONG") / ln) if ln else 0.0
    sep = (100 * sum(pct[k] for k in range(len(sd)) if sd[k] == "SHORT") / sn) if sn else 0.0
    return dict(n=len(sel), ER=sum(rr) / len(rr), win=100 * len(w) / len(rr),
                rr=avgw / avgl if avgl > 0 else 0.0, ln=ln, le=le, sn=sn, se=se,
                ERpct=100 * sum(pct) / len(pct), lep=lep, sep=sep)   # ERpct = avg % return on capital/trade


def bootR(bypair, iters=600, seed=7):
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


def era_pos(recs, m_eras):
    pos = cnt = 0
    for e in range(m_eras):
        st = statR(recs, e / m_eras, (e + 1) / m_eras)
        if st: cnt += 1; pos += 1 if st["ER"] > 0 else 0
    return pos, cnt


def analyse_tf(label, data, iv_min, m_eras, entry_name):
    pairs = A.to_pairs(data)
    if len(pairs) < 8:
        return [f"\n[{label}] ACCUMULATING - only {len(pairs)} pairs with >=200 bars."], None
    sig = ENTRIES[entry_name]
    roll_per_bar = (ROLL_DAY / 6.0) * ((iv_min / 60) / 4)
    out = [f"\n{'='*100}\n[{label}]  entry={entry_name}  pairs={len(pairs)}  bars/pair~{pairs[0].n}  "
           f"eras={m_eras}   (OOS E[R] grid; 1R = stop distance)"]
    out.append(f"      {'stop(xATR)':>10s} | " + " ".join(f"{p:>8s}" for p in POLICIES))
    best = None  # (ER, stop, policy, oos_recs, oos_bypair, full_recs)
    grid = {}
    for s_mult in STOPS:
        cells = []
        for policy in POLICIES:
            recs, bypair = run_combo(pairs, sig, s_mult, policy, roll_per_bar)
            oos = statR(recs, 0.5, 1.0); iss = statR(recs, 0.0, 0.5)
            grid[(s_mult, policy)] = (recs, bypair, oos, iss)
            cells.append(f"{oos['ER']:>+8.3f}" if oos else f"{'--':>8s}")
            # candidate = OOS positive AND in-sample positive (nested discipline)
            if oos and iss and oos["ER"] > 0 and iss["ER"] > 0:
                if best is None or oos["ER"] > best[0]:
                    best = (oos["ER"], s_mult, policy, recs)
        out.append(f"      {s_mult:>10.1f} | " + " ".join(cells))

    # detail on the best nested-validated cell
    if best:
        _, s_mult, policy, recs = best
        oos = statR(recs, 0.5, 1.0)
        bypair = grid[(s_mult, policy)][1]   # full-window pair streams for the block bootstrap
        bf = bootR(bypair)
        pos, cnt = era_pos(recs, m_eras)
        bs = f"boot[{bf[0]:+.3f}..{bf[2]:+.3f}]R {'SIG' if bf and bf[0] > 0 else 'ci~0'}" if bf else ""
        out.append(f"  BEST nested-valid cell: stop {s_mult}xATR + {policy}  ->  OOS E {oos['ER']:+.3f}R "
                   f"rr {oos['rr']:.2f} win {oos['win']:.0f}%  L:{oos['ln']}@{oos['le']:+.2f} "
                   f"S:{oos['sn']}@{oos['se']:+.2f}  eras+{pos}/{cnt}  {bs}")
        twoside = oos["ln"] >= 8 and oos["sn"] >= 8
        dir_ok = (min(oos['le'], oos['se']) > 0) if twoside else True
        durable = bf and bf[0] > 0 and dir_ok and cnt >= 3 and pos >= cnt - 1 and oos["rr"] >= 1.5
        out.append(f"  => {'DURABLE here (meets 1:1.5, both sides, era-stable, boot>0)' if durable else 'positive but NOT fully durable (see flags)'}")
        return out, (label, best[0], s_mult, policy, oos, durable)
    out.append("  no cell positive in BOTH halves (nested-validated) -> nothing survives here")
    return out, (label, None, None, None, None, False)


async def main():
    print("TB00779 EXIT / STOP / SPEED sweep - entry held FIXED, exit varied. All in R (1R = stop distance).")
    print("Q1 does taking profit beat holding? Q2 best stop size? Q3 best speed? GOAL = E[R]>0 AND rr>=1.5 OOS.")
    tfs = [("5m", 5, 4), ("15m", 15, 4), ("30m", 30, 4), ("1h", 60, 5), ("4h", 240, 5), ("1d", 1440, 6)]
    for entry_name in ("emacross", "momentum"):
        print(f"\n\n##################  ENTRY = {entry_name}  ##################")
        summary = []
        for label, iv, me in tfs:
            data = await A.fetch_kraken(iv)
            try:
                lines, summ = analyse_tf(label, data, iv, me, entry_name)
                for ln in lines: print(ln)
                if summ: summary.append(summ)
            except Exception as ex:
                print(f"\n[{label}] ERROR {type(ex).__name__}: {ex}")
        print(f"\n  ---- SPEED SUMMARY (entry={entry_name}): best nested-valid OOS cell per timeframe ----")
        for lab, er, s_mult, policy, oos, durable in summary:
            if er is None: print(f"      {lab:>4s}: nothing survives nested validation")
            else: print(f"      {lab:>4s}: stop {s_mult}xATR + {policy:7s} OOS {er:+.3f}R rr {oos['rr']:.2f} "
                        f"win {oos['win']:.0f}%  {'DURABLE' if durable else 'fragile'}")


if __name__ == "__main__":
    asyncio.run(main())
