"""TB00783 - Bill's question: why must the stop only TIGHTEN? Test stops that adapt BOTH ways.

Bill's points:
  - A one-way ratchet (tighten-only) means the stop does NOT widen when noise/volatility increases, so
    ordinary larger pullbacks tag the tightened stop = premature exits = the low (~18%) win rate.
  - Proposal: size the stop FROM THE DATA each bar (current support and current volatility), let it move
    BOTH up and down, and only really exit on a REVERSAL.
  - How do we increase the NUMBER OF WINS?

This isolates exactly that. Same entry (EMA-cross / momentum), same run-to-reversal, on the cached 7.5yr
deep history. Measured in % RETURN ON CAPITAL (the honest yardstick) + WIN RATE. Stop modes:
  revonly  : no stop at all - exit ONLY on reversal (the win-rate ceiling for this entry+exit)
  wide     : fixed 3xATR floor (never moves)
  ratchet  : structure trail, tighten-ONLY (the current design)
  free     : structure level recomputed each bar, moves BOTH ways (Bill's "let it loosen with the data")
  adapt    : structure level minus k*current_ATR each bar, BOTH ways (Bill's "size it from volatility")
Question: does adapting both ways RAISE the win rate, and does it raise PROFIT (% on capital)? PAPER only."""

from __future__ import annotations
import asyncio, os, importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("t781", os.path.join(HERE, "tb00781_deep_history.py"))
T81 = importlib.util.module_from_spec(spec); spec.loader.exec_module(T81)
A = T81.A; E = T81.E; fetch_all = T81.fetch_all; ENTRIES = E.ENTRIES
TAKER, OPEN_FEE, ROLL_DAY = E.TAKER, E.OPEN_FEE, E.ROLL_DAY
BUF = 0.001; MINN = 25


def sim(p, i, side, mode, L, k, roll_per_bar):
    """Returns (pct_on_capital, off, side) - net P/L as a fraction of notional. None if unsizable."""
    c, h, l, n = p.c, p.h, p.l, p.n
    if p.atr[i] is None: return None
    e = c[i]; atr0 = p.atr[i]
    if atr0 <= 0 or e <= 0: return None

    def level(j):   # data-driven structure (+/- volatility) level on the stop side, at bar j
        if side == "LONG":
            base = min(l[max(i, j - L + 1):j + 1])
            return base * (1 - BUF) - (k * p.atr[j] if (k and p.atr[j]) else 0.0)
        base = max(h[max(i, j - L + 1):j + 1])
        return base * (1 + BUF) + (k * p.atr[j] if (k and p.atr[j]) else 0.0)

    if mode == "revonly":
        stop = None
    elif mode == "wide":
        stop = e - 3 * atr0 if side == "LONG" else e + 3 * atr0
    else:  # ratchet / free / adapt all start at the bar-i structure(+vol) level
        stop = level(i)
    legs = 2; off = n - 1 - i; exit_px = None
    for o, j in enumerate(range(i + 1, n), 1):
        if stop is not None:
            hit = (l[j] <= stop) if side == "LONG" else (h[j] >= stop)
            if hit:
                exit_px = stop; off = o; break
            if mode == "ratchet":
                lv = level(j)
                if (lv > stop) if side == "LONG" else (lv < stop): stop = lv
            elif mode in ("free", "adapt"):
                stop = level(j)   # both ways
            # wide: fixed, no update
        rev = p.e12[j] is not None and ((p.e12[j] < p.e26[j]) if side == "LONG" else (p.e12[j] > p.e26[j]))
        if rev:
            exit_px = c[j]; off = o; break
    if exit_px is None:
        exit_px = c[n - 1]
    gross = (exit_px - e) / e if side == "LONG" else (e - exit_px) / e
    fee = TAKER * legs + (OPEN_FEE if side == "SHORT" else 0.0)
    roll = (roll_per_bar * off) if side == "SHORT" else 0.0
    return gross - fee - roll, off, side


def run(pairs, sig, mode, L, k, roll):
    recs = []; bypair = {}
    for p in pairs:
        i = 1; n = p.n
        while i < n - 1:
            s = sig(p, i)
            if s is None: i += 1; continue
            r = sim(p, i, s, mode, L, k, roll)
            if r is None: i += 1; continue
            pct, off, side = r
            recs.append((i / n, pct, side)); bypair.setdefault(p.sym, []).append(pct)
            i = max(i + 1, i + off)
    return recs, bypair


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


def stat(recs, lo, hi):
    sel = [r for r in recs if lo <= r[0] < hi]
    if len(sel) < MINN: return None
    p = [r[1] for r in sel]; sd = [r[2] for r in sel]
    w = [x for x in p if x > 0]
    longs = [p[k] for k in range(len(sd)) if sd[k] == "LONG"]
    return dict(n=len(p), win=100 * len(w) / len(p), E=100 * sum(p) / len(p),
                Elong=100 * sum(longs) / len(longs) if longs else 0.0, nlong=len(longs),
                wlong=100 * sum(1 for x in longs if x > 0) / len(longs) if longs else 0.0)


def era_pos(recs, m):
    pos = cnt = 0
    for e_ in range(m):
        st = stat(recs, e_ / m, (e_ + 1) / m)
        if st: cnt += 1; pos += 1 if st["Elong"] > 0 else 0
    return pos, cnt


def analyse(label, data, iv_min, entry_name):
    pairs = A.to_pairs(data)
    if len(pairs) < 8: print(f"[{label}] skip"); return
    sig = ENTRIES[entry_name]; roll = (ROLL_DAY / 6.0) * ((iv_min / 60) / 4)
    print(f"\n{'='*100}\n[{label}] entry={entry_name} pairs={len(pairs)}   (LONG side; % return on capital)")
    print(f"  {'mode':16s} {'Lwin%':>6s} {'Long E%/tr':>11s} {'blend E%':>9s} {'eras':>6s}  bootLong%")
    modes = [("revonly", 0, 0.0), ("wide", 0, 0.0), ("ratchet L20", 20, 0.0),
             ("free L20", 20, 0.0), ("adapt L20 k1.5", 20, 1.5), ("adapt L10 k1.0", 10, 1.0)]
    for tag, L, k in modes:
        mode = tag.split()[0]
        recs, bp = run(pairs, sig, mode, L if L else 20, k, roll)
        o = stat(recs, 0.5, 1.0)
        if not o: print(f"  {tag:16s} too few"); continue
        b = boot(bp)   # blended pair-bootstrap as the significance proxy
        pos, cnt = era_pos(recs, 8)
        bs = f"[{b[0]:+.2f}..{b[2]:+.2f}]% {'SIG' if b and b[0] > 0 else 'ci~0'}" if b else ""
        print(f"  {tag:16s} {o['wlong']:>6.0f} {o['Elong']:>+10.2f}% {o['E']:>+8.2f}% {pos:>3d}/{cnt:<2d} {bs}")


async def main():
    print("TB00783 do stops that ADAPT BOTH WAYS raise the win rate and profit? % on capital is the yardstick.")
    print("revonly=no stop (win ceiling) | wide=fixed 3xATR | ratchet=tighten-only (current) | "
          "free=structure both-ways | adapt=structure-k*ATR both-ways")
    for label, iv, iv_min in [("12h", "12h", 720), ("1d (24h)", "1d", 1440)]:
        data = fetch_all(iv)
        for entry_name in ("emacross", "momentum"):
            analyse(label, data, iv_min, entry_name)


if __name__ == "__main__":
    asyncio.run(main())
