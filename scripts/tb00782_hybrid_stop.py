"""TB00782 - Bill's HYBRID stop: a WIDE initial stop that then TRAILS up to new supports.

Bill's idea: combine the two stops to keep the wide stop's HIGH win rate (a wide initial floor can't be
tagged by early noise -> trade survives pullbacks) AND get the structure stop's SMALL losses (trail the
stop up under each new support, so a late failure exits near breakeven or in profit). Best of both, maybe.

Implementation (LONG; mirror for SHORT): initial stop = entry - 3xATR (the WIDE floor; 1R = that distance,
so R and win-rate are directly comparable to the pure wide stop). Each bar, compute the structure level =
rolling extreme of the last L bars on the stop side (just below recent support for longs / above recent
resistance for shorts) and move the stop toward price ONLY if it tightens (never loosen). Run to reversal
(EMA flip) otherwise. So early the wide floor dominates (structure is lower); once the trend prints higher
supports, the structure trail takes over and caps the loss.

Head-to-head on the cached 7.5yr Binance deep history (TB00781) vs: pure WIDE (3xATR fixed + reversal) and
pure STRUCTURE (trail from a structure-level initial stop). Reports BOTH R and % (return on capital). The
question: does the hybrid beat BOTH parents, or land in the middle? PAPER research only."""

from __future__ import annotations
import asyncio, os, importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("t781", os.path.join(HERE, "tb00781_deep_history.py"))
T81 = importlib.util.module_from_spec(spec); spec.loader.exec_module(T81)
A = T81.A; E = T81.E; T80 = T81.T80
fetch_all = T81.fetch_all
statR, bootR, era_pos, ENTRIES = E.statR, E.bootR, E.era_pos, E.ENTRIES
line, run_struct = T80.line, T80.run_struct
TAKER, OPEN_FEE, ROLL_DAY = E.TAKER, E.OPEN_FEE, E.ROLL_DAY

BUF = 0.001
ATR_MULT = 3.0     # the WIDE initial floor (matches the pure-wide comparison)


def sim_hybrid(p, i, side, L, roll_per_bar):
    """Wide initial stop (3xATR) that then trails up to new supports (only tightens). Run to reversal."""
    c, h, l, n = p.c, p.h, p.l, p.n
    if p.atr[i] is None: return None
    e = c[i]; atr0 = p.atr[i]
    if atr0 <= 0: return None
    stop = e - ATR_MULT * atr0 if side == "LONG" else e + ATR_MULT * atr0
    R = abs(e - stop)
    if R <= 0: return None
    Rfrac = R / e; legs = 2; off = n - 1 - i; exitR = None
    for o, j in enumerate(range(i + 1, n), 1):
        # 1) adverse FIRST: stop tagged?
        hit = (l[j] <= stop) if side == "LONG" else (h[j] >= stop)
        if hit:
            exitR = ((stop - e) if side == "LONG" else (e - stop)) / R; off = o; break
        # 2) structure trail - move stop toward price ONLY if it tightens (never loosen the wide floor)
        if side == "LONG":
            lvl = min(l[max(i, j - L + 1):j + 1]) * (1 - BUF)
            if lvl > stop: stop = lvl
        else:
            lvl = max(h[max(i, j - L + 1):j + 1]) * (1 + BUF)
            if lvl < stop: stop = lvl
        # 3) run to reversal
        rev = p.e12[j] is not None and ((p.e12[j] < p.e26[j]) if side == "LONG" else (p.e12[j] > p.e26[j]))
        if rev:
            exitR = ((c[j] - e) if side == "LONG" else (e - c[j])) / R; off = o; break
    if exitR is None:
        j = n - 1; exitR = ((c[j] - e) if side == "LONG" else (e - c[j])) / R
    fee_frac = TAKER * legs + (OPEN_FEE if side == "SHORT" else 0.0)
    roll_frac = (roll_per_bar * off) if side == "SHORT" else 0.0
    return exitR - (fee_frac + roll_frac) / Rfrac, off, side, Rfrac


def run_hybrid(pairs, sig, L, roll_per_bar):
    recs = []; bypair = {}
    for p in pairs:
        i = 1; n = p.n
        while i < n - 1:
            s = sig(p, i)
            if s is None: i += 1; continue
            r = sim_hybrid(p, i, s, L, roll_per_bar)
            if r is None: i += 1; continue
            netR, off, side, rfrac = r
            recs.append((i / n, netR, side, netR * rfrac)); bypair.setdefault(p.sym, []).append(netR)
            i = max(i + 1, i + off)
    return recs, bypair


def analyse(label, data, iv_min, m_eras, entry_name):
    pairs = A.to_pairs(data)
    if len(pairs) < 8:
        print(f"\n[{label}] only {len(pairs)} pairs - skip"); return
    sig = ENTRIES[entry_name]
    roll = (ROLL_DAY / 6.0) * ((iv_min / 60) / 4)
    print(f"\n{'='*104}\n[{label}]  entry={entry_name}  pairs={len(pairs)}  eras={m_eras}   (focus the LONG side L:)")
    print("  PURE WIDE (3xATR fixed + reversal):")
    recs, bp = E.run_combo(pairs, sig, ATR_MULT, "hold", roll); print(line("wide", recs, bp, m_eras)[0])
    print("  PURE STRUCTURE (structure-level initial + trail):")
    for L in (10, 20):
        recs, bp = run_struct(pairs, sig, L, roll); print(line(f"struct L={L}", recs, bp, m_eras)[0])
    print("  HYBRID (3xATR wide floor + trail up to new supports):")
    for L in (10, 20):
        recs, bp = run_hybrid(pairs, sig, L, roll); print(line(f"hybrid L={L}", recs, bp, m_eras)[0])


async def main():
    print("TB00782 HYBRID stop = wide initial floor + trail up to new supports. Head-to-head vs pure wide / "
          "pure structure on cached 7.5yr deep history. Both R and %. Does combining beat BOTH parents?")
    for label, iv, iv_min, me in [("8h", "8h", 480, 8), ("12h", "12h", 720, 8), ("1d (24h)", "1d", 1440, 8)]:
        data = fetch_all(iv)   # cached
        for entry_name in ("emacross", "momentum"):
            try:
                analyse(label, data, iv_min, me, entry_name)
            except Exception as ex:
                print(f"\n[{label}/{entry_name}] ERROR {type(ex).__name__}: {ex}")
    print("\nWIN-RATE is the key tell: if the hybrid keeps the wide stop's win% while lifting E[R] above both "
          "parents, Bill's combine-the-stops idea works. If win% drops to the structure stop's level, the "
          "trail re-introduces the premature-exit problem.")


if __name__ == "__main__":
    asyncio.run(main())
