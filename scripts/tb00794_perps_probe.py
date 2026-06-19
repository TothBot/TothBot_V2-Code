"""TB00794 - CRYPTO-PERPS research probe: re-run the spot-arc battery (TB00779-786) on PERPETUAL
FUTURES, for BOTH Longs and Shorts, with the ACTUAL historical funding series swapped in for the spot
borrow fee. The central question: does removing borrow + collecting/paying real funding REVIVE the
regime-gated SHORT (spot couldn't) and change the best LONG?

WHY PERPS MIGHT CHANGE THE ANSWER (two independent levers, decomposed below):
  1. FEE: Kraken SPOT taker = 0.26%/side (the organism's real cost); Kraken FUTURES taker ~= 0.05%/side.
     Round-trip 0.52% -> 0.10% = a 5x fee cut, independent of funding. The spot arc's edge was largely
     eaten by the 0.26% taker; perps hand most of it back.
  2. CARRY: spot shorts paid a margin OPEN fee + a per-bar BORROW/rollover. Perps replace BOTH with
     FUNDING - and in crypto's typical positive-funding regime (longs pay shorts) a SHORT *collects*
     funding. That is the reason to re-test shorts: their structural carry flips from a cost to a credit.

DATA (the new variable is real funding):
  - Binance USDT-margined (UM) PERPETUAL futures klines + the real fundingRate history, pulled from the
    public bulk mirror data.binance.vision (monthly CSV-in-ZIP; data-api/fapi REST is geo-blocked 451).
    Native 4h + 1d klines; 8h/12h folded from 4h (TB00780 method). Funding summed per-event over each
    trade's actual hold window via a per-bar cumulative-funding array (lookahead-free: each funding rate
    is known at its calc_time). Perp history is ~2020-> (perps are younger than spot's 7.5yr) - honestly
    shorter, but still spans 2021 bull / 2022 bear / 2023-24 recovery / 2025 = the 8-era multi-regime test.
  - Live read maps to Kraken's perp economics: realistic Kraken-Futures taker (0.05%) + the same funding.

BATTERY (replicate, don't cherry-pick):
  Engine A = the FULL combinatorial nested-validated search (the auto_strategy_search engine: 9 entry
    signals x 4 filters x 15 exits incl wide-ATR stops, fixed targets, chandelier trails), perp cost model,
    run THREE direction lenses (LONG-only / SHORT-only / BOTH) per slow timeframe {8h,12h,24h}. Nested
    IS-select / OOS-validate, block-bootstrap CI, regime-robustness (not one-sided, stable across sub-
    windows). Answers "best system" + "is there a regime-robust SHORT survivor now?".
  Engine B = the focused TB00786 carry-forward sweep: entry {emacross, momentum} x exit {wide kxATR stop +
    reversal} x stop {2/2.5/3/5 xATR} x slippage {0/5/10/20 bp} x direction {LONG, SHORT}, per tf
    {4h,8h,12h,24h}, fixed $50 notional. Reports n, win%, E in R AND % on capital AND $/trade, 8-era
    stability, OOS block-bootstrap CI, and the FUNDING DECOMPOSITION (avg funding bps/trade + the E with
    funding zeroed) so funding's size and sign are explicit. This is the headline.

Reuses operations/auto_strategy_search.py for indicators / SIGNALS / FILTERS / boot / KS,MS,TRAILS / stats
scaffolding. PAPER research only; public bulk GET only; results cached to gitignored _perp_cache_*.json."""

from __future__ import annotations
import asyncio, os, io, json, time, zipfile, importlib.util, urllib.request
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("ass", os.path.join(HERE, "..", "operations", "auto_strategy_search.py"))
A = importlib.util.module_from_spec(spec); spec.loader.exec_module(A)

# ---- perp cost model (the swapped-in variable) ----
TAKER_PERP = 0.0005      # Kraken Futures taker ~0.05%/side (Binance UM ~0.04%); realistic perp taker
TAKER_SPOT = float(A.TAKER)   # 0.0026 - the spot organism's real cost, for the decomposition line
NOTIONAL = 50.0
MINN = 20

BULK = "https://data.binance.vision/data/futures/um/monthly"
# Liquid Binance-UM perp universe -> BASE/USD keys (to_pairs' BTC-tide logic keys on 'BTC/USD').
# Subset is also Kraken-Futures-listed (BTC ETH SOL XRP ADA AVAX DOGE LINK DOT LTC BCH ... = the live read).
UNIVERSE = ["BTC","ETH","SOL","XRP","ADA","AVAX","DOGE","LINK","DOT","LTC","BCH","UNI","ATOM","FIL",
    "NEAR","APT","OP","ARB","SUI","INJ","TIA","SEI","WLD","AAVE","ETC","XLM","TRX","LDO","RUNE","ALGO"]
KRAKEN_PERP = {"BTC","ETH","SOL","XRP","ADA","AVAX","DOGE","LINK","DOT","LTC","BCH"}  # live-read core


def _months(start=(2019, 9)):
    """All (year,month) from start to last COMPLETE month (bulk monthly only publishes finished months)."""
    g = time.gmtime(); cy, cm = g.tm_year, g.tm_mon
    out = []; y, m = start
    while (y, m) < (cy, cm):           # strictly before current month
        out.append((y, m)); m += 1
        if m > 12: y += 1; m = 1
    return out


def _getbytes(url):
    req = urllib.request.Request(url, headers={"User-Agent": "tb794"})
    with urllib.request.urlopen(req, timeout=40) as r:
        return r.read()


def _month_rows(url, is_funding):
    """Download one monthly zip and parse to rows. Returns [] on 404/error (not-yet-listed month)."""
    try:
        z = zipfile.ZipFile(io.BytesIO(_getbytes(url)))
        data = z.read(z.namelist()[0]).decode()
    except Exception:
        return []
    out = []
    for ln in data.splitlines():
        p = ln.split(",")
        try:
            if is_funding:
                out.append((int(p[0]) // 1000, float(p[-1])))             # (calc_sec, rate)
            else:
                out.append((int(p[0]) // 1000, float(p[2]), float(p[3]), float(p[4])))  # (t,h,l,c)
        except Exception:
            continue                                                       # header / blank row
    return out


# One shared pool: months within a symbol fetched concurrently (the I/O-bound win vs sequential).
_POOL = ThreadPoolExecutor(max_workers=24)


def fetch_klines(base, interval):
    """Concurrent monthly UM perp klines -> (closes,highs,lows,times_sec), header-robust."""
    sym = base + "USDT"
    urls = [f"{BULK}/klines/{sym}/{interval}/{sym}-{interval}-{y}-{m:02d}.zip" for (y, m) in _months()]
    rows = []
    for r in _POOL.map(lambda u: _month_rows(u, False), urls): rows += r
    rows.sort()
    if len(rows) < 200: return None
    return ([r[3] for r in rows], [r[1] for r in rows], [r[2] for r in rows], [r[0] for r in rows])


def fetch_funding(base):
    """Concurrent monthly UM fundingRate -> sorted [(calc_time_sec, rate)].
    rate = per-event fraction the LONG pays the SHORT (positive = long pays); sum events over a hold."""
    sym = base + "USDT"
    urls = [f"{BULK}/fundingRate/{sym}/{sym}-fundingRate-{y}-{m:02d}.zip" for (y, m) in _months()]
    evts = []
    for r in _POOL.map(lambda u: _month_rows(u, True), urls): evts += r
    evts.sort()
    return evts


def load_data():
    """Fetch (per-symbol cached) all perp klines (4h,1d) + funding. Per-symbol JSON caches make the pull
    resumable; a crash/kill loses at most the in-flight symbol. Returns prices[iv]={sym:(c,h,l,t)}, fund={}."""
    prices = {"4h": {}, "1d": {}}; fund = {}
    for base in UNIVERSE:
        key = base + "/USD"; scache = os.path.join(HERE, f"_perp_cache_{base}.json")
        rec = None
        if os.path.exists(scache):
            try:
                with open(scache) as f: rec = json.load(f)
            except Exception:
                rec = None
        if rec is None:
            rec = {"4h": fetch_klines(base, "4h"), "1d": fetch_klines(base, "1d"), "fund": fetch_funding(base)}
            try:
                with open(scache, "w") as f: json.dump(rec, f)
            except Exception:
                pass
        if rec.get("4h"): prices["4h"][key] = tuple(rec["4h"])
        if rec.get("1d"): prices["1d"][key] = tuple(rec["1d"])
        if rec.get("fund"): fund[key] = [tuple(e) for e in rec["fund"]]
        print(f"    {base:6s} 4h={len(prices['4h'].get(key,([],))[0]):>5d} "
              f"1d={len(prices['1d'].get(key,([],))[0]):>5d} fund_evts={len(fund.get(key,[]))}", flush=True)
    return prices, fund


def fold(data, k):
    """Fold k contiguous 4h bars into one, aligned to k*4h UTC buckets (TB00780). {sym:(c,h,l,t_sec)}."""
    period = 14400 * k; out = {}
    for sym, (c, h, l, t) in data.items():
        buckets = {}
        for idx in range(len(t)):
            buckets.setdefault(t[idx] // period, []).append(idx)
        cc = []; hh = []; ll = []; tt = []
        for b in sorted(buckets):
            ids = buckets[b]
            if len(ids) < k: continue
            cc.append(c[ids[-1]]); hh.append(max(h[x] for x in ids))
            ll.append(min(l[x] for x in ids)); tt.append(t[ids[0]])
        if len(cc) >= 200: out[sym] = (cc, hh, ll, tt)
    return out


def cumfund(t_bars, evts):
    """Per-bar cumulative funding: cum[j] = sum of rates with calc_time <= t_bars[j]. Two-pointer.
    Funding paid by a LONG over hold i->i+off = cum[i+off]-cum[i] (a SHORT receives the same)."""
    n = len(t_bars); cum = [0.0] * n; ei = 0; acc = 0.0; m = len(evts)
    for j in range(n):
        while ei < m and evts[ei][0] <= t_bars[j]:
            acc += evts[ei][1]; ei += 1
        cum[j] = acc
    return cum


def make_pairs(data, fund):
    """Build A.build_pair indicator pairs and attach p.t + p.cum (cumulative funding)."""
    pairs = A.to_pairs(data)
    for p in pairs:
        t = data[p.sym][3]; p.t = t
        p.cum = cumfund(t, fund.get(p.sym, []))
    return pairs


# ======================= ENGINE B: focused carry-forward sweep =======================
def sim_focus(p, i, side, atr_mult, slip, stop_slip, taker):
    """Wide kxATR fixed stop + reversal exit, fixed notional, with slippage + REAL funding.
    Returns (net_pct, netR, fund_frac, off): fund_frac = funding cost charged (>0 = drag, <0 = credit)."""
    c, h, l, n = p.c, p.h, p.l, p.n
    if p.atr[i] is None: return None
    e = c[i]; atr0 = p.atr[i]
    if atr0 <= 0 or e <= 0: return None
    entry = e * (1 + slip) if side == "LONG" else e * (1 - slip)
    stop = e - atr_mult * atr0 if side == "LONG" else e + atr_mult * atr0
    off = n - 1 - i; exit_px = None
    for o, j in enumerate(range(i + 1, n), 1):
        hit = (l[j] <= stop) if side == "LONG" else (h[j] >= stop)
        if hit:
            exit_px = stop * (1 - stop_slip) if side == "LONG" else stop * (1 + stop_slip); off = o; break
        rev = p.e12[j] is not None and ((p.e12[j] < p.e26[j]) if side == "LONG" else (p.e12[j] > p.e26[j]))
        if rev:
            exit_px = c[j] * (1 - slip) if side == "LONG" else c[j] * (1 + slip); off = o; break
    if exit_px is None:
        exit_px = c[n - 1] * (1 - slip) if side == "LONG" else c[n - 1] * (1 + slip)
    gross = (exit_px - entry) / entry if side == "LONG" else (entry - exit_px) / entry
    fund_raw = p.cum[min(i + off, n - 1)] - p.cum[i]       # long pays this fraction; short receives it
    fund = fund_raw if side == "LONG" else -fund_raw       # signed cost charged to THIS trade
    net = gross - 2 * taker - fund
    R = atr_mult * atr0 / e                                # 1R = stop distance as a price fraction
    return net, (net / R if R > 0 else 0.0), fund, off


def run_focus(pairs, sig, side, atr_mult, slip, stop_slip, taker):
    recs = []; bypair = {}
    for p in pairs:
        i = 1; n = p.n
        while i < n - 1:
            if sig(p, i) != side: i += 1; continue
            r = sim_focus(p, i, side, atr_mult, slip, stop_slip, taker)
            if r is None: i += 1; continue
            net, netR, fund, off = r
            recs.append((i / n, net, netR, fund, p.sym)); bypair.setdefault(p.sym, []).append(net)
            i = max(i + 1, i + off)
    return recs, bypair


def fstat(recs, lo=0.0, hi=1.0):
    sel = [r for r in recs if lo <= r[0] < hi]
    if len(sel) < MINN: return None
    pct = [r[1] for r in sel]; rr = [r[2] for r in sel]; fnd = [r[3] for r in sel]
    w = [x for x in pct if x > 0]; ls = [x for x in pct if x <= 0]
    avgw = sum(w) / len(w) if w else 0.0; avgl = -sum(ls) / len(ls) if ls else 1e-9
    return dict(n=len(sel), win=100 * len(w) / len(sel), E=100 * sum(pct) / len(pct),
                ER=sum(rr) / len(rr), rr=avgw / avgl if avgl > 0 else 0.0,
                fund_bps=1e4 * sum(fnd) / len(fnd), Enf=100 * sum(pct[k] + fnd[k] for k in range(len(pct))) / len(pct))


def fboot(bypair, iters=600, seed=7):
    pp = list(bypair); m = len(pp)
    if m < 3: return None
    s = seed; vals = []
    for _ in range(iters):
        tot = 0.0; cnt = 0
        for _ in range(m):
            s = (1103515245 * s + 12345) & 0x7FFFFFFF; pr = pp[(s * m) >> 31]
            tot += sum(bypair[pr]); cnt += len(bypair[pr])
        vals.append(100 * tot / (cnt or 1))
    vals.sort(); return vals[int(.05 * iters)], vals[iters // 2], vals[int(.95 * iters)]


def feras(recs, m=8):
    pos = cnt = 0
    for e_ in range(m):
        s = fstat(recs, e_ / m, (e_ + 1) / m)
        if s: cnt += 1; pos += 1 if s["E"] > 0 else 0
    return pos, cnt


def engine_b(label, pairs, taker):
    print(f"\n{'='*120}\n[ENGINE B / {label}] focused carry-forward: kxATR stop + reversal, fixed ${NOTIONAL:.0f}, "
          f"perp taker {taker*100:.2f}%/side, REAL funding")
    for entry in ("emacross", "momentum"):
        sig = A.SIGNALS["ema_cross"] if entry == "emacross" else A.SIGNALS["momentum"]
        for side in ("LONG", "SHORT"):
            print(f"  -- entry={entry} dir={side} --")
            print(f"     {'cfg':22s} {'n':>5s} {'win%':>5s} {'ER':>6s} {'E%/tr':>7s} {'$/tr':>6s} "
                  f"{'fund_bp':>7s} {'E_noFund%':>9s} {'eras':>5s}  OOS-boot%")
            for atr_mult in (2.0, 2.5, 3.0, 5.0):
                for sl, ssl in ((0, 0), (5, 10), (10, 20), (20, 40)):
                    recs, bp = run_focus(pairs, sig, side, atr_mult, sl / 1e4, ssl / 1e4, taker)
                    full = fstat(recs); oos = fstat(recs, 0.5, 1.0)
                    if not full: continue
                    bo = fboot(bpslice(recs, 0.5, 1.0)); pos, cnt = feras(recs)
                    bs = (f"[{bo[0]:+.2f}..{bo[2]:+.2f}] {'SIG' if bo[0] > 0 else 'ci~0'}" if bo else "n/a")
                    tag = f"stop{atr_mult}x slip{sl}/{ssl}bp"
                    print(f"     {tag:22s} {full['n']:>5d} {full['win']:>5.0f} {full['ER']:>+6.2f} "
                          f"{full['E']:>+6.2f}% {full['E']/100*NOTIONAL:>+5.2f} {full['fund_bps']:>+7.1f} "
                          f"{full['Enf']:>+8.2f}% {pos:>2d}/{cnt:<2d}  {bs}")


def bpslice(recs, lo, hi):
    """Per-pair net streams over the [lo,hi) entry-fraction slice, for the OOS block bootstrap."""
    d = {}
    for fr, net, netR, fund, sym in recs:
        if lo <= fr < hi: d.setdefault(sym, []).append(net)
    return d


# ======================= ENGINE A: full combinatorial nested search (perp cost) =======================
def precompute_perp(p, maxhold):
    """A.precompute minus the spot rollover (perp carry is funding, applied later by index)."""
    ex = {}
    for i in range(p.n - 1):
        if p.atr[i] is None: continue
        e = p.c[i]; atrf = p.atr[i] / e
        if atrf <= 0: continue
        for side in ("LONG", "SHORT"):
            so = [-1] * len(A.KS); to = [-1] * len(A.MS); madv = mfav = 0.0; revexc = None; hold = None
            peak = 0.0; tro = [-1] * len(A.TRAILS); trx = [None] * len(A.TRAILS); jmax = min(p.n, i + 1 + maxhold)
            for off, j in enumerate(range(i + 1, jmax), start=1):
                advpx = p.h[j] if side == "SHORT" else p.l[j]; favpx = p.l[j] if side == "SHORT" else p.h[j]
                adv = ((advpx - e) if side == "SHORT" else (e - advpx)) / e
                fav = ((e - favpx) if side == "SHORT" else (favpx - e)) / e
                if adv > madv: madv = adv
                if fav > mfav: mfav = fav
                for ki, k in enumerate(A.KS):
                    if so[ki] == -1 and madv >= k * atrf: so[ki] = off
                for mi, mm in enumerate(A.MS):
                    if mm is not None and to[mi] == -1 and mfav >= mm * atrf: to[mi] = off
                cw = -adv
                for ti, tr in enumerate(A.TRAILS):
                    if tro[ti] == -1 and cw <= (peak - tr * atrf): tro[ti] = off; trx[ti] = peak - tr * atrf
                if fav > peak: peak = fav
                if revexc is None and p.e12[j] is not None and ((side == "LONG" and p.e12[j] < p.e26[j]) or (side == "SHORT" and p.e12[j] > p.e26[j])):
                    revexc = ((e - p.c[j]) if side == "SHORT" else (p.c[j] - e)) / e; hold = off
            if revexc is None:
                j = jmax - 1; revexc = ((e - p.c[j]) if side == "SHORT" else (p.c[j] - e)) / e; hold = jmax - 1 - i
            for ti in range(len(A.TRAILS)):
                if tro[ti] == -1: j = jmax - 1; tro[ti] = jmax - 1 - i; trx[ti] = ((e - p.c[j]) if side == "SHORT" else (p.c[j] - e)) / e
            ex[(i, side)] = (atrf, so, to, revexc, hold, tro, trx)
    return ex


def gross_off(entry, spec, side):
    """Gross return (before fee+funding) and hold offset for an exit spec. Mirrors A.trade_pnl pre-cost."""
    atrf, so, to, revexc, hold, tro, trx = entry
    if spec[0] == "tr":
        ti = spec[1]; return trx[ti], tro[ti]
    ki, mi = spec[1], spec[2]; k = A.KS[ki]; m = A.MS[mi]; cands = [(hold, revexc, 2)]
    if so[ki] != -1: cands.append((so[ki], -(k * atrf), 0))
    if m is not None and to[mi] != -1: cands.append((to[mi], m * atrf, 1))
    off, gross, _ = min(cands, key=lambda c: (c[0], c[2]))
    return gross, off


def run_combo_perp(pairs, exits, sig, filt, spec, lens, lo, hi, taker, minn=40):
    pls = []; holds = []; bypair = {}; bydir = {"LONG": [], "SHORT": []}; thirds = [[], [], []]
    span = (hi - lo) or 1.0
    for p in pairs:
        ex = exits[p.sym]; cum = p.cum; n = p.n; i = int(n * lo); end = int(n * hi)
        while i < end - 1:
            side = sig(p, i)
            if side is None or (lens != "BOTH" and side != lens) or (i, side) not in ex or not filt(p, i, side):
                i += 1; continue
            gross, off = gross_off(ex[(i, side)], spec, side)
            fund_raw = cum[min(i + off, n - 1)] - cum[i]
            fund = fund_raw if side == "LONG" else -fund_raw
            pl = gross - 2 * taker - fund
            pls.append(pl); holds.append(off); bypair.setdefault(p.sym, []).append(pl); bydir[side].append(pl)
            t = min(2, int(((i / n) - lo) / span * 3)); thirds[t].append(pl)
            i += max(1, off)
    if len(pls) < minn: return None
    w = [x for x in pls if x > 0]; ls = [x for x in pls if x <= 0]
    avgw = sum(w) / len(w) if w else 0.0; avgl = -sum(ls) / len(ls) if ls else 1e-9
    def de(v): return (len(v), (sum(v) / len(v) * 100 if v else 0.0))
    ln, le = de(bydir["LONG"]); sn, se = de(bydir["SHORT"])
    tcounted = [seg for seg in thirds if len(seg) >= 8]; tpos = sum(1 for seg in tcounted if sum(seg) > 0)
    return {"n": len(pls), "win": 100 * len(w) / len(pls), "E": sum(pls) / len(pls) * 100,
            "rr": avgw / avgl if avgl > 0 else 0.0, "npairs": len(bypair), "bypair": bypair,
            "longE": le, "longn": ln, "shortE": se, "shortn": sn, "tpos": tpos, "tcount": len(tcounted)}


def search_perp(pairs, maxhold, lens, taker, label):
    exits = {p.sym: precompute_perp(p, maxhold) for p in pairs}
    specs = [("st", ki, mi, f"{A.KS[ki]:.0f}x/{'rev' if A.MS[mi] is None else f'{A.MS[mi]:.1f}x'}")
             for ki in range(len(A.KS)) for mi in range(len(A.MS))]
    specs += [("tr", ti, None, f"trail{A.TRAILS[ti]:.1f}x") for ti in range(len(A.TRAILS))]
    rows = []
    for sn, sig in A.SIGNALS.items():
        for fn, filt in A.FILTERS.items():
            for sp in specs:
                isr = run_combo_perp(pairs, exits, sig, filt, (sp[0], sp[1], sp[2]), lens, 0.0, 0.5, taker)
                oos = run_combo_perp(pairs, exits, sig, filt, (sp[0], sp[1], sp[2]), lens, 0.5, 1.0, taker)
                if isr and oos: rows.append((sn, fn, sp[3], isr, oos))
    isg = [r for r in rows if r[3]["E"] > 0 and r[3]["rr"] >= 1.5]
    surv = [r for r in isg if r[4]["E"] > 0 and r[4]["rr"] >= 1.5]

    def robust(o):
        twoside = o["longn"] >= 8 and o["shortn"] >= 8
        if twoside:
            lo_e, hi_e = sorted((o["longE"], o["shortE"])); dir_ok = lo_e > 0 and lo_e >= 0.20 * hi_e
        else:
            dir_ok = True
        time_ok = o["tpos"] >= max(2, o["tcount"] - 1) if o["tcount"] >= 2 else False
        return dir_ok and time_ok
    robn = sum(1 for r in surv if robust(r[4]))
    out = [f"[A/{label}/{lens}] pairs={len(pairs)} combos={len(rows)} | IS-goal-meeters={len(isg)} | "
           f"OOS-SURVIVORS={len(surv)} | REGIME-ROBUST={robn}"]
    for sn, fn, xl, a, o in sorted(surv, key=lambda r: r[4]["E"], reverse=True)[:6]:
        bo = A.boot(o["bypair"]); sig = "SIG" if bo[0] > 0 else "ci~0"; rob = "ROBUST" if robust(o) else "regime-fragile"
        out.append(f"    SURVIVOR {sn}/{fn}/{xl}: OOS E {o['E']:+.3f}% rr {o['rr']:.2f} win {o['win']:.0f}% "
                   f"n={o['n']} np={o['npairs']} boot[{bo[0]:+.3f}..{bo[2]:+.3f}]% {sig} "
                   f"L:{o['longn']}@{o['longE']:+.2f}% S:{o['shortn']}@{o['shortE']:+.2f}% {rob}")
    if not surv:
        out.append("    (no OOS survivor meeting E>0 AND rr>=1.5 in both halves)")
    return out


# ======================= driver =======================
async def main():
    print("TB00794 CRYPTO-PERPS probe - spot-arc battery on PERPETUAL FUTURES, real funding vs spot borrow.")
    print(f"perp taker {TAKER_PERP*100:.2f}%/side (spot was {TAKER_SPOT*100:.2f}%); funding = real Binance-UM history.")
    print("\n... loading perp klines (4h,1d) + funding from data.binance.vision (cached) ...")
    prices, fund = load_data()
    print(f"    loaded: 4h={len(prices['4h'])} pairs, 1d={len(prices['1d'])} pairs, funding={len(fund)} symbols")
    kp = sorted(s for s in fund if s.split('/')[0] in KRAKEN_PERP)
    print(f"    Kraken-Futures-listed subset (live read): {', '.join(s.split('/')[0] for s in kp)}")

    eight = fold(prices["4h"], 2); twelve = fold(prices["4h"], 3)
    speeds = [("4h", prices["4h"], 90), ("8h(fold)", eight, 90), ("12h(fold)", twelve, 60), ("24h(1d)", prices["1d"], 60)]
    built = {}
    for lbl, data, mh in speeds:
        pairs = make_pairs(data, fund)
        built[lbl] = (pairs, mh)
        span = (max(p.n for p in pairs) * {"4h": 4, "8h(fold)": 8, "12h(fold)": 12, "24h(1d)": 24}[lbl] / 24 / 365) if pairs else 0
        print(f"    {lbl:10s} pairs={len(pairs)} maxbars~{max((p.n for p in pairs), default=0)} ~{span:.1f}yr")

    # ---- ENGINE B: focused carry-forward sweep (headline R / % / $ + funding decomposition) ----
    for lbl in ("4h", "8h(fold)", "12h(fold)", "24h(1d)"):
        pairs, _ = built[lbl]
        if len(pairs) >= 8: engine_b(lbl, pairs, TAKER_PERP)

    # ---- ENGINE A: full combinatorial nested search, 3 direction lenses, slow tfs ----
    print(f"\n{'#'*120}\n# ENGINE A - full 9-signal x 4-filter x 15-exit nested search (perp cost), LONG/SHORT/BOTH lenses")
    for lbl in ("8h(fold)", "12h(fold)", "24h(1d)"):
        pairs, mh = built[lbl]
        if len(pairs) < 8:
            print(f"\n[A/{lbl}] only {len(pairs)} pairs - skip"); continue
        print(f"\n{'='*120}\n[ENGINE A / {lbl}]  pairs={len(pairs)}  (the SLOW sweet spot from the spot arc)")
        for lens in ("LONG", "SHORT", "BOTH"):
            for line in search_perp(pairs, mh, lens, TAKER_PERP, lbl): print(line)

    print("\nNOTE: % = return on capital per trade (unlevered; leverage is a sizing multiplier + liquidation "
          "caveat, NOT edge). fund_bp = avg funding charged/trade in bps (>0 drag, <0 CREDIT - shorts collect "
          "positive funding). E_noFund = E with funding zeroed (isolates funding's contribution). Perp history "
          "~2020-> (younger than spot's 7.5yr) but spans 2021 bull/2022 bear/2023-25. DURABLE = OOS E>0 AND "
          "rr>=1.5 AND era-stable AND bootstrap CI>0 AND (both-sides) regime-robust.")


if __name__ == "__main__":
    asyncio.run(main())
