"""TB00786 - FINAL stress test of the long-only strategy with Bill's exact organism parameters.

Bill's corrections folded in:
  1. Universe = USD/USDC/USDT pairs (Binance USDT mirror is the proxy) - EXPANDED to ~60 liquid pairs
     (more than the prior 32) = partial out-of-sample on pairs.
  2. 24h ONLY (drop 12h).
  3. FIXED-NOTIONAL sizing (CIATS-owned, $50 min/trade), NOT risk-based 1R -> report % return on capital
     + dollars at $50/trade. The 2-2.5x ATR stop only sets the EXIT PRICE; $ risk/trade varies.
  4. SLIPPAGE model added - fill worse than the printed price each side; stops slip extra. Swept.
  5. Stress test with all the above (8 yearly eras + block bootstrap, full 7.5yr deep history).
  6. Report.

Strategy under test: LONG-ONLY. Entry = EMA12/26 bullish cross (momentum-cross as a cross-check). Exit =
reversal (EMA bearish cross) OR a wide ~2-2.5x ATR disaster stop, whichever first. No take-profit.
PAPER research only; public Binance mirror, cached."""

from __future__ import annotations
import asyncio, os, json, time, importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("t781", os.path.join(HERE, "tb00781_deep_history.py"))
T81 = importlib.util.module_from_spec(spec); spec.loader.exec_module(T81)
A = T81.A; E = T81.E; ENTRIES = E.ENTRIES; fetch_binance = T81.fetch_binance
TAKER = E.TAKER
NOTIONAL = 50.0
MINN = 20

# Expanded liquid USD/USDC/USDT universe (Binance USDT proxy); pairs without deep history just fall out.
UNIVERSE = ["BTC","ETH","SOL","XRP","ADA","AVAX","DOGE","LINK","DOT","LTC","BCH","UNI","AAVE","ATOM","FIL",
    "NEAR","ALGO","GRT","FET","INJ","RUNE","HBAR","APT","OP","TIA","SUI","SEI","WLD","PENDLE","ARKM","TAO",
    "RENDER","TRX","ETC","XLM","EOS","XTZ","NEO","IOTA","VET","ZEC","DASH","WAVES","QTUM","ONT","BAT","ENJ",
    "ZIL","THETA","MANA","SAND","CHZ","EGLD","AXS","COMP","SNX","CRV","SUSHI","KAVA","ICX","ZRX","OMG"]


def fetch_big(interval):
    cache = os.path.join(HERE, f"_binance_cache_{interval}_big.json")
    if os.path.exists(cache):
        try:
            with open(cache) as f: return {k: tuple(v) for k, v in json.load(f).items()}
        except Exception: pass
    out = {}
    for base in UNIVERSE:
        syms = ["RENDERUSDT", "RNDRUSDT"] if base == "RENDER" else [base + "USDT"]
        for s in syms:
            d = fetch_binance(s, interval)
            if d: out[base + "/USD"] = d; break
        time.sleep(0.04)
    try:
        with open(cache, "w") as f: json.dump(out, f)
    except Exception: pass
    return out


def sim_long(p, i, atr_mult, slip, stop_slip):
    """LONG, fixed notional. Entry fills worse by slip; reversal exit by slip; stop exit by stop_slip.
    Returns (pct_on_capital, off). Fees = taker x2 (longs: no short open/rollover)."""
    c, h, l, n = p.c, p.h, p.l, p.n
    if p.atr[i] is None: return None
    e_mid = c[i]; atr0 = p.atr[i]
    if atr0 <= 0 or e_mid <= 0: return None
    entry = e_mid * (1 + slip)                 # pay up on entry
    stop = e_mid - atr_mult * atr0
    off = n - 1 - i; exit_px = None
    for o, j in enumerate(range(i + 1, n), 1):
        if l[j] <= stop:                       # adverse first: stop tagged, fills worse
            exit_px = stop * (1 - stop_slip); off = o; break
        if p.e12[j] is not None and p.e12[j] < p.e26[j]:   # reversal exit
            exit_px = c[j] * (1 - slip); off = o; break
    if exit_px is None:
        exit_px = c[n - 1] * (1 - slip)
    gross = (exit_px - entry) / entry
    return gross - 2 * TAKER, off              # net % on capital deployed


def run(pairs, sig, atr_mult, slip, stop_slip):
    recs = []
    for p in pairs:
        i = 1; n = p.n
        while i < n - 1:
            if sig(p, i) != "LONG": i += 1; continue
            r = sim_long(p, i, atr_mult, slip, stop_slip)
            if r is None: i += 1; continue
            pct, off = r
            recs.append((i / n, pct, p.sym))
            i = max(i + 1, i + off)
    return recs


def bp_slice(recs, lo=0.0, hi=1.0):
    """per-pair pct streams for the [lo,hi) entry-fraction slice (for block bootstrap)."""
    d = {}
    for fr, pct, sym in recs:
        if lo <= fr < hi: d.setdefault(sym, []).append(pct)
    return d


def st(recs, lo=0.0, hi=1.0):
    sel = [r[1] for r in recs if lo <= r[0] < hi]
    if len(sel) < MINN: return None
    w = [x for x in sel if x > 0]
    return dict(n=len(sel), win=100 * len(w) / len(sel), E=100 * sum(sel) / len(sel), tot=sum(sel))


def boot(bypair, iters=600, seed=7):
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


def eras(recs, m=8):
    pos = cnt = 0
    for e_ in range(m):
        s = st(recs, e_ / m, (e_ + 1) / m)
        if s: cnt += 1; pos += 1 if s["E"] > 0 else 0
    return pos, cnt


def analyse(pairs, entry, years):
    sig = ENTRIES[entry]
    print(f"\n{'='*108}\n[24h] entry={entry}  pairs={len(pairs)}  ~{years:.1f}yr  | LONG-ONLY, fixed ${NOTIONAL:.0f}/trade, "
          f"reversal+ATR stop")
    print(f"  {'config':18s} {'n':>5s} {'tr/yr':>6s} {'win%':>5s} {'avg%/tr':>8s} {'avg$/tr':>8s} "
          f"{'total$':>9s} {'eras':>6s}  OOS-boot% (per-trade)")
    for atr_mult in (2.0, 2.5):
        for slip_bps, sslip_bps in ((0, 0), (5, 10), (10, 20)):
            slip = slip_bps / 10000.0; sslip = sslip_bps / 10000.0
            recs = run(pairs, sig, atr_mult, slip, sslip)
            full = st(recs); oos = st(recs, 0.5, 1.0)
            if not full: print(f"  stop{atr_mult} slip{slip_bps}bp  too few"); continue
            bo = boot(bp_slice(recs, 0.5, 1.0))   # true OOS (second-half) block bootstrap by pair
            pos, cnt = eras(recs)
            tryr = full["n"] / years
            bs = f"[{bo[0]:+.2f}..{bo[2]:+.2f}]% {'SIG' if bo and bo[0] > 0 else 'ci~0'}" if bo else "n/a"
            tag = f"stop{atr_mult} slip{slip_bps}/{sslip_bps}bp"
            print(f"  {tag:18s} {full['n']:>5d} {tryr:>6.0f} {full['win']:>5.0f} {full['E']:>+7.2f}% "
                  f"{full['E']/100*NOTIONAL:>+7.2f} {full['tot']*NOTIONAL:>+8.0f} {pos:>3d}/{cnt:<2d}  {bs}")


async def main():
    print("TB00786 FINAL stress test - long-only, 24h, USD/USDC/USDT (expanded), fixed $50/trade, WITH slippage.")
    data = fetch_big("1d")
    pairs = A.to_pairs(data)
    years = max(p.n for p in pairs) / 365.0
    print(f"universe: {len(pairs)} pairs with deep history; ~{years:.1f} years")
    for entry in ("emacross", "momentum"):
        analyse(pairs, entry, years)
    print("\nNOTE: % = return on the $50 deployed per trade. total$ = sum over ALL signals at $50 each "
          "(gross, no concurrency cap). slip a/b bp = a bp per side on normal fills, b bp on stop fills.")


if __name__ == "__main__":
    asyncio.run(main())
