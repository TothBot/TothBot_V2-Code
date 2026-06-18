"""TB00780 - TRAILING STRUCTURE STOP (support/resistance) run-to-reversal, on the LONGER speeds.

Bill's WHAT (this turn):
  1. Test speeds LONGER than 4h: 8h, 12h, 24h. (24h = the prior '1d' run; 8h/12h have NO native Kraken
     feed, so they are FOLDED losslessly from 4h bars - 2 and 3 contiguous 4h bars. Folded 8h/12h reach
     back only ~120 days = one era; native 24h reaches ~2 years. Flagged per speed.)
  2. LONG stop just BELOW the most recent SUPPORT; as new (higher) supports form, ratchet the stop UP.
  3. SHORT stop just ABOVE the most recent RESISTANCE; as new (lower) resistances form, ratchet the stop
     DOWN.
  4. Let it RUN until reversal (NO take-profit - Test-1 showed take-profit is a wash).

Operational, lookahead-free defn of "the most recent support/resistance": the rolling extreme of the last
L bars on the stop side - recent swing LOW for longs (support), recent swing HIGH for shorts (resistance) -
ratcheted ONLY in our favor. Buffer BUF just beyond it. Reversal = EMA12/26 flip against the trade. 1R =
the initial stop distance, position sized to 1R (full stop = -1R), so results are in R and comparable to
the TB00779 ATR sweep. Entry held FIXED (live-bot-style EMA cross + momentum cross-check). Sweep L.

Rigor: nested IS/OOS, per-direction, era stability, block-bootstrap by pair, fees + short rollover, all in
R. Reuses tb00779_exit_sweep (data/indicators/fees/stats) + auto_strategy_search. PAPER research only."""

from __future__ import annotations
import asyncio, os, importlib.util

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("t779", os.path.join(HERE, "tb00779_exit_sweep.py"))
E = importlib.util.module_from_spec(spec); spec.loader.exec_module(E)
A = E.A
statR, bootR, era_pos, ENTRIES = E.statR, E.bootR, E.era_pos, E.ENTRIES
TAKER, OPEN_FEE, ROLL_DAY = E.TAKER, E.OPEN_FEE, E.ROLL_DAY

LS = [5, 10, 20]      # support/resistance lookback (bars) to sweep
BUF = 0.001           # 0.1% just beyond the level
MINN = 25


def fold(data, k):
    """Fold k contiguous 4h bars into one, aligned to k*4h UTC buckets. {sym:(c,h,l,t)}."""
    period = 14400 * k
    out = {}
    for sym, (c, h, l, t) in data.items():
        buckets = {}
        for idx in range(len(t)):
            buckets.setdefault(t[idx] // period, []).append(idx)
        cc = []; hh = []; ll = []; tt = []
        for b in sorted(buckets):
            ids = buckets[b]
            if len(ids) < k: continue   # drop incomplete edge buckets
            cc.append(c[ids[-1]]); hh.append(max(h[x] for x in ids))
            ll.append(min(l[x] for x in ids)); tt.append(t[ids[0]])
        if len(cc) >= 200: out[sym] = (cc, hh, ll, tt)
    return out


def sim_struct(p, i, side, L, roll_per_bar):
    """Trailing structure-stop, run-to-reversal, no take-profit. Returns (netR, off, side) or None."""
    c, h, l, n = p.c, p.h, p.l, p.n
    e = c[i]
    if side == "LONG":
        ref = min(l[max(0, i - L + 1):i + 1]); stop = ref * (1 - BUF); R = e - stop
    else:
        ref = max(h[max(0, i - L + 1):i + 1]); stop = ref * (1 + BUF); R = stop - e
    if R <= 0: return None
    Rfrac = R / e; legs = 2; off = n - 1 - i; exitR = None
    for o, j in enumerate(range(i + 1, n), 1):
        # 1) adverse FIRST (conservative): structural stop tagged?
        hit = (l[j] <= stop) if side == "LONG" else (h[j] >= stop)
        if hit:
            exitR = ((stop - e) if side == "LONG" else (e - stop)) / R   # <0 at initial; >0 if trailed past entry
            off = o; break
        # 2) ratchet the structural trail ONLY in our favor (new support up / new resistance down)
        if side == "LONG":
            nref = min(l[max(i, j - L + 1):j + 1]) * (1 - BUF)
            if nref > stop: stop = nref
        else:
            nref = max(h[max(i, j - L + 1):j + 1]) * (1 + BUF)
            if nref < stop: stop = nref
        # 3) run until REVERSAL (EMA12/26 flip against the trade)
        rev = p.e12[j] is not None and ((p.e12[j] < p.e26[j]) if side == "LONG" else (p.e12[j] > p.e26[j]))
        if rev:
            exitR = ((c[j] - e) if side == "LONG" else (e - c[j])) / R; off = o; break
    if exitR is None:
        j = n - 1; exitR = ((c[j] - e) if side == "LONG" else (e - c[j])) / R
    fee_frac = TAKER * legs + (OPEN_FEE if side == "SHORT" else 0.0)
    roll_frac = (roll_per_bar * off) if side == "SHORT" else 0.0
    return exitR - (fee_frac + roll_frac) / Rfrac, off, side


def run_struct(pairs, sig, L, roll_per_bar):
    recs = []; bypair = {}
    for p in pairs:
        i = 1; n = p.n
        while i < n - 1:
            s = sig(p, i)
            if s is None: i += 1; continue
            r = sim_struct(p, i, s, L, roll_per_bar)
            if r is None: i += 1; continue
            netR, off, side = r
            recs.append((i / n, netR, side)); bypair.setdefault(p.sym, []).append(netR)
            i = max(i + 1, i + off)
    return recs, bypair


def line(tag, recs, bypair, m_eras):
    oos = statR(recs, 0.5, 1.0); iss = statR(recs, 0.0, 0.5)
    if not oos: return f"      {tag:12s} too few trades", False
    b = bootR(bypair); pos, cnt = era_pos(recs, m_eras)
    bs = f"boot[{b[0]:+.3f}..{b[2]:+.3f}]R {'SIG' if b and b[0] > 0 else 'ci~0'}" if b else ""
    twoside = oos["ln"] >= 8 and oos["sn"] >= 8
    dir_ok = (min(oos['le'], oos['se']) > 0) if twoside else True
    nested = iss and iss["ER"] > 0 and oos["ER"] > 0
    durable = bool(b) and b[0] > 0 and dir_ok and cnt >= 3 and pos >= cnt - 1 and oos["rr"] >= 1.5 and nested
    flag = "DURABLE" if durable else ("+OOS" if oos["ER"] > 0 else "neg")
    return (f"      {tag:12s} OOS E {oos['ER']:+.3f}R rr {oos['rr']:.2f} win {oos['win']:.0f}%  "
            f"L:{oos['ln']}@{oos['le']:+.2f} S:{oos['sn']}@{oos['se']:+.2f}  eras+{pos}/{cnt}  {bs}  {flag}"), durable


def analyse(label, data, iv_min, m_eras, entry_name):
    pairs = A.to_pairs(data)
    if len(pairs) < 8:
        print(f"\n[{label}] ACCUMULATING - only {len(pairs)} pairs with >=200 bars."); return
    sig = ENTRIES[entry_name]
    roll_per_bar = (ROLL_DAY / 6.0) * ((iv_min / 60) / 4)
    span_days = pairs[0].n * iv_min / 1440
    print(f"\n{'='*100}\n[{label}]  entry={entry_name}  pairs={len(pairs)}  bars~{pairs[0].n}  "
          f"~{span_days:.0f} days  eras={m_eras}")
    print("  TRAILING STRUCTURE STOP (support/resistance), run-to-reversal, NO take-profit:")
    for L in LS:
        recs, bypair = run_struct(pairs, sig, L, roll_per_bar)
        txt, _ = line(f"L={L}", recs, bypair, m_eras); print(txt)
    # comparison: wide ATR stop (3x) + reversal, same entry (the TB00779 'hold' policy)
    recs, bypair = E.run_combo(pairs, sig, 3.0, "hold", roll_per_bar)
    txt, _ = line("ATR3x+rev", recs, bypair, m_eras); print("  COMPARE fixed wide stop:\n" + txt)


async def main():
    print("TB00780 trailing structure stop (support/resistance) run-to-reversal, on longer speeds.")
    print("All in R (1R = initial stop distance). GOAL = E[R]>0 AND rr>=1.5 OOS, both sides, era-stable.")
    base4h = await A.fetch_kraken(240)     # native 4h (for 4h baseline + folding 8h/12h)
    base1d = await A.fetch_kraken(1440)     # native 24h
    speeds = [
        ("4h", base4h, 240, 5),
        ("8h (fold 4h x2)", fold(base4h, 2), 480, 5),
        ("12h (fold 4h x3)", fold(base4h, 3), 720, 4),
        ("24h (native 1d)", base1d, 1440, 6),
    ]
    for entry_name in ("emacross", "momentum"):
        print(f"\n\n##################  ENTRY = {entry_name}  ##################")
        for label, data, iv, me in speeds:
            try:
                analyse(label, data, iv, me, entry_name)
            except Exception as ex:
                print(f"\n[{label}] ERROR {type(ex).__name__}: {ex}")
    print("\nNOTE: folded 8h/12h reach back only ~120 days (one era) - era test is weaker there; native 24h "
          "spans ~2 years. 'DURABLE' = +OOS AND +in-sample AND rr>=1.5 AND both sides + AND era-stable AND boot>0.")


if __name__ == "__main__":
    asyncio.run(main())
