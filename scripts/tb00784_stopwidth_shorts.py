"""TB00784 - (A) find the stop-width sweet spot; (B) make SHORTS profitable with stricter entry gates.

Bill's two WHATs:
  A. "Find the sweet spot for the stop width - disaster insurance seems to dominate." Sweep the WIDE fixed
     ATR stop from tight (0.5x) to very wide (6x) plus reversal-only (no stop), LONG side, and find where
     % return on capital peaks.
  B. "Tighten the entry requirements for SHORTS to make shorts profitable." The short side bleeds because
     crypto trends up; only short when a DOWNTREND is confirmed. Test short-entry gates (price below its
     SMA100/SMA200, 50<200 death-cross regime, below-MA + negative momentum) and see which flips the short
     side to a positive, era-stable % on capital.

Design under test (from TB00783): WIDE ATR stop + exit on REVERSAL, slow timeframe. Cached 7.5yr Binance
deep history. Measured in % return on capital + win rate. PAPER research only."""

from __future__ import annotations
import asyncio, os, importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("t781", os.path.join(HERE, "tb00781_deep_history.py"))
T81 = importlib.util.module_from_spec(spec); spec.loader.exec_module(T81)
A = T81.A; E = T81.E; fetch_all = T81.fetch_all; ENTRIES = E.ENTRIES
TAKER, OPEN_FEE, ROLL_DAY = E.TAKER, E.OPEN_FEE, E.ROLL_DAY
_s83 = importlib.util.spec_from_file_location("t783", os.path.join(HERE, "tb00783_adaptive_stop.py"))
_m83 = importlib.util.module_from_spec(_s83); _s83.loader.exec_module(_m83)
boot = _m83.boot
MINN = 20


def prep(pairs):
    for p in pairs:
        p.sma50 = A.sma(p.c, 50); p.sma200 = A.sma(p.c, 200)   # sma100 + mom already on the pair


def sim_wide(p, i, side, atr_mult, roll):
    """Wide FIXED ATR stop (atr_mult; None = no stop) + exit on reversal. Returns (pct, off, side)."""
    c, h, l, n = p.c, p.h, p.l, p.n
    if p.atr[i] is None: return None
    e = c[i]; atr0 = p.atr[i]
    if atr0 <= 0 or e <= 0: return None
    stop = None if atr_mult is None else (e - atr_mult * atr0 if side == "LONG" else e + atr_mult * atr0)
    legs = 2; off = n - 1 - i; exit_px = None
    for o, j in enumerate(range(i + 1, n), 1):
        if stop is not None:
            if (l[j] <= stop) if side == "LONG" else (h[j] >= stop):
                exit_px = stop; off = o; break
        rev = p.e12[j] is not None and ((p.e12[j] < p.e26[j]) if side == "LONG" else (p.e12[j] > p.e26[j]))
        if rev:
            exit_px = c[j]; off = o; break
    if exit_px is None: exit_px = c[n - 1]
    gross = (exit_px - e) / e if side == "LONG" else (e - exit_px) / e
    fee = TAKER * legs + (OPEN_FEE if side == "SHORT" else 0.0)
    roll = (roll * off) if side == "SHORT" else 0.0
    return gross - fee - roll, off, side


def run(pairs, sig, atr_mult, want_side, filt, roll):
    recs = []; bypair = {}
    for p in pairs:
        i = 1; n = p.n
        while i < n - 1:
            s = sig(p, i)
            if s is None or s != want_side or not filt(p, i): i += 1; continue
            r = sim_wide(p, i, s, atr_mult, roll)
            if r is None: i += 1; continue
            pct, off, side = r
            recs.append((i / n, pct, side)); bypair.setdefault(p.sym, []).append(pct)
            i = max(i + 1, i + off)
    return recs, bypair


def st(recs, lo=0.0, hi=1.0):
    sel = [r[1] for r in recs if lo <= r[0] < hi]
    if len(sel) < MINN: return None
    w = [x for x in sel if x > 0]
    return dict(n=len(sel), win=100 * len(w) / len(sel), E=100 * sum(sel) / len(sel))


def eras(recs, m=8):
    pos = cnt = 0
    for e_ in range(m):
        s = st(recs, e_ / m, (e_ + 1) / m)
        if s: cnt += 1; pos += 1 if s["E"] > 0 else 0
    return pos, cnt


# ---- short-entry gates (confirmed-downtrend filters) ----
def f_none(p, i): return True
def f_below100(p, i): return p.sma100[i] is not None and p.c[i] < p.sma100[i]
def f_below200(p, i): return p.sma200[i] is not None and p.c[i] < p.sma200[i]
def f_below200_dc(p, i):
    return (p.sma200[i] is not None and p.sma50[i] is not None
            and p.c[i] < p.sma200[i] and p.sma50[i] < p.sma200[i])
def f_below100_momdn(p, i):
    return p.sma100[i] is not None and p.c[i] < p.sma100[i] and p.mom[i] is not None and p.mom[i] < 0
SHORT_FILTERS = [("none", f_none), ("below_sma100", f_below100), ("below_sma200", f_below200),
                 ("below200+50<200", f_below200_dc), ("below100+mom<0", f_below100_momdn)]
ATRS = [0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0, 6.0, None]


def partA(label, pairs, entry, roll):
    sig = ENTRIES[entry]
    print(f"\n[{label}] entry={entry}  PART A: stop-width sweep, LONG-only, wide ATR + reversal")
    print(f"  {'stop(xATR)':>10s} {'Lwin%':>6s} {'LongE%/tr':>10s} {'eras':>6s}  boot%")
    for am in ATRS:
        recs, bp = run(pairs, sig, am, "LONG", f_none, roll)
        o = st(recs, 0.5, 1.0)
        tag = "rev-only" if am is None else f"{am:.1f}"
        if not o: print(f"  {tag:>10s} too few"); continue
        b = boot(bp); pos, cnt = eras(recs)
        bs = f"[{b[0]:+.2f}..{b[2]:+.2f}]% {'SIG' if b and b[0] > 0 else 'ci~0'}" if b else ""
        print(f"  {tag:>10s} {o['win']:>6.0f} {o['E']:>+9.2f}% {pos:>3d}/{cnt:<2d} {bs}")


def partB(label, pairs, entry, roll, atr_mult=3.0):
    sig = ENTRIES[entry]
    print(f"\n[{label}] entry={entry}  PART B: SHORT-only, wide {atr_mult}xATR + reversal, downtrend gates")
    print(f"  {'filter':16s} {'n':>5s} {'Swin%':>6s} {'ShortE%/tr':>11s} {'eras':>6s}  boot%")
    for name, filt in SHORT_FILTERS:
        recs, bp = run(pairs, sig, atr_mult, "SHORT", filt, roll)
        o = st(recs, 0.5, 1.0)
        if not o: print(f"  {name:16s} too few"); continue
        b = boot(bp); pos, cnt = eras(recs)
        bs = f"[{b[0]:+.2f}..{b[2]:+.2f}]% {'SIG' if b and b[0] > 0 else 'ci~0'}" if b else ""
        print(f"  {name:16s} {o['n']:>5d} {o['win']:>6.0f} {o['E']:>+10.2f}% {pos:>3d}/{cnt:<2d} {bs}")


async def main():
    print("TB00784 (A) stop-width sweet spot (long) + (B) downtrend gates to make shorts profitable. % on capital.")
    for label, iv, iv_min in [("12h", "12h", 720), ("1d (24h)", "1d", 1440)]:
        data = fetch_all(iv); pairs = A.to_pairs(data); prep(pairs)
        roll = (ROLL_DAY / 6.0) * ((iv_min / 60) / 4)
        for entry in ("emacross", "momentum"):
            print(f"\n{'='*100}")
            partA(label, pairs, entry, roll)
            partB(label, pairs, entry, roll)


if __name__ == "__main__":
    asyncio.run(main())
