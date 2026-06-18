"""TB00785 - make SHORTS profitable with a DIFFERENT mechanism: mean-reversion (short euphoric tops).

TB00784 proved trend-following shorts lose structurally in crypto (up-drift + borrow fee + bear rallies).
Bill's WHAT stands: make shorts profitable. The opposite mechanism: short STRENGTH - enter when price is
euphorically OVERBOUGHT/stretched and bet on a snap-back to the mean. Quick in/out (target = the mean),
NOT run-to-reversal.

Entries (short on overbought strength): RSI>70, RSI>75, above upper Bollinger (sma20+2*std20), stretched
>=3*ATR above sma20. Exit: TARGET = revert to sma20 (take profit), STOP = entry + k*ATR (above), or a max
hold. Measured in % return on capital + win rate, on cached deep history, faster speeds too (reversion is
quick). The question: does shorting overbought extremes flip the short side positive + era-stable? PAPER."""

from __future__ import annotations
import asyncio, os, importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("t781", os.path.join(HERE, "tb00781_deep_history.py"))
T81 = importlib.util.module_from_spec(spec); spec.loader.exec_module(T81)
A = T81.A; E = T81.E; fetch_all = T81.fetch_all
TAKER, OPEN_FEE, ROLL_DAY = E.TAKER, E.OPEN_FEE, E.ROLL_DAY
_s84 = importlib.util.spec_from_file_location("t784", os.path.join(HERE, "tb00784_stopwidth_shorts.py"))
_m84 = importlib.util.module_from_spec(_s84); _s84.loader.exec_module(_m84)
boot, st, eras, prep = _m84.boot, _m84.st, _m84.eras, _m84.prep
MINN = 20
STOP_K = 3.0      # stop = entry + 3*ATR (above) - wide so noise can't tag
MAXHOLD = 10      # reversion should resolve quickly


def e_rsi70(p, i): return p.rsi[i] is not None and p.rsi[i] > 70
def e_rsi75(p, i): return p.rsi[i] is not None and p.rsi[i] > 75
def e_bbupper(p, i):
    return p.sma20[i] is not None and p.std20[i] is not None and p.c[i] > p.sma20[i] + 2 * p.std20[i]
def e_stretch3(p, i):
    return p.sma20[i] is not None and p.atr[i] is not None and p.c[i] > p.sma20[i] + 3 * p.atr[i]
ENTRIES = [("rsi>70", e_rsi70), ("rsi>75", e_rsi75), ("bb-upper", e_bbupper), ("stretch>=3ATR", e_stretch3)]


def sim_mr(p, i, roll):
    """Short an overbought bar; target = revert to sma20, stop = entry+3*ATR, else max-hold. (pct, off)."""
    c, h, l, n = p.c, p.h, p.l, p.n
    if p.atr[i] is None or p.sma20[i] is None: return None
    e = c[i]; atr0 = p.atr[i]
    if atr0 <= 0 or e <= 0: return None
    stop = e + STOP_K * atr0
    target = p.sma20[i]                  # the mean - below price for an overbought entry
    if target >= e: return None          # not actually above the mean -> skip
    off = 0; exit_px = c[min(n - 1, i + MAXHOLD)]
    for o, j in enumerate(range(i + 1, min(n, i + MAXHOLD + 1)), 1):
        off = o
        if h[j] >= stop: exit_px = stop; break          # adverse first
        if l[j] <= target: exit_px = target; break      # took profit at the mean
    else:
        off = min(n - 1, i + MAXHOLD) - i
    gross = (e - exit_px) / e
    fee = TAKER * 2 + OPEN_FEE
    return gross - fee - roll * off, off


def run(pairs, ent, roll):
    recs = []; bypair = {}
    for p in pairs:
        i = 1; n = p.n
        while i < n - 1:
            if not ent(p, i): i += 1; continue
            r = sim_mr(p, i, roll)
            if r is None: i += 1; continue
            pct, off = r
            recs.append((i / n, pct, "SHORT")); bypair.setdefault(p.sym, []).append(pct)
            i = max(i + 1, i + off)
    return recs, bypair


def analyse(label, pairs, roll):
    print(f"\n{'='*96}\n[{label}] MEAN-REVERSION SHORTS (short overbought, target=mean, stop=+3ATR, maxhold={MAXHOLD})")
    print(f"  {'entry':14s} {'n':>6s} {'win%':>5s} {'E%/tr':>8s} {'eras':>6s}  boot%")
    for name, ent in ENTRIES:
        recs, bp = run(pairs, ent, roll)
        o = st(recs, 0.5, 1.0)
        if not o: print(f"  {name:14s} too few"); continue
        b = boot(bp); pos, cnt = eras(recs)
        bs = f"[{b[0]:+.2f}..{b[2]:+.2f}]% {'SIG' if b and b[0] > 0 else 'ci~0'}" if b else ""
        print(f"  {name:14s} {o['n']:>6d} {o['win']:>5.0f} {o['E']:>+7.2f}% {pos:>3d}/{cnt:<2d} {bs}")


async def main():
    print("TB00785 mean-reversion shorts (short euphoric overbought tops, revert to mean). % on capital.")
    print("Does shorting STRENGTH (vs trend-following weakness) flip the short side positive + era-stable?")
    for label, iv, iv_min in [("4h", "4h", 240), ("6h", "6h", 360), ("12h", "12h", 720), ("1d (24h)", "1d", 1440)]:
        data = fetch_all(iv); pairs = A.to_pairs(data); prep(pairs)
        roll = (ROLL_DAY / 6.0) * ((iv_min / 60) / 4)
        analyse(label, pairs, roll)


if __name__ == "__main__":
    asyncio.run(main())
