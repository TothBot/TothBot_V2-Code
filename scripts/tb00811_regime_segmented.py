"""TB00811 - validate the OPERATOR-TIMED regime-segmented strategy (Bill's WHAT):
   LONG only during crypto SUMMER (bull), SHORT only during crypto WINTER (bear); Bill flips the
   switch. Plus the claim: in the current sideways CHOP, neither side does well.

Method: reuse the TB00806 data substrate (real 30-pair perp universe + funding, daily, 2019-2026)
and the CAUSAL BTC regime detector (BTC vs its 200-day SMA, no lookahead - the TB00801a detector,
~83% phase-accurate) as the systematic proxy for the operator's summer/winter call. A SIDEWAYS/CHOP
mask is layered on top: |BTC close / SMA200 - 1| < band = price hugging the SMA = consolidation.

Each "book" = a set of trades; profit = sum(pct * $50 clip); maxDD from the equity curve netted by
exit timestamp; Calmar = profit / maxDD. Honest, propose-only, STAY IN PAPER, no lookahead.
"""

from __future__ import annotations

import bisect

import tb00806_perp_account_sim as S

from tothbot.perp.collision import OpenInterval, collides

CLIP = float(S.NOTIONAL)   # $50 fixed-notional clip per trade
DEP = 5000.0               # per-module deposit (Calmar context)
CHOP_BAND = 0.04           # +/- 4% of the 200d SMA = sideways/consolidation


def regime_at(reg, te):
    t, lab = reg
    j = bisect.bisect_right(t, te) - 1
    return lab[j] if j >= 0 else None


def build_chop(sub):
    """Causal |c/sma200 - 1| series for BTC -> a chop lookup (within +/-band of the SMA = sideways)."""
    c, _, _, t = sub["spot_data"]["BTC/USD"]
    sma = [None] * len(c)
    for j in range(len(c)):
        if j >= 199:
            sma[j] = sum(c[j - 199:j + 1]) / 200
    dev = [abs(c[j] / sma[j] - 1.0) if sma[j] else None for j in range(len(c))]
    return t, dev


def is_chop(chop, te):
    t, dev = chop
    j = bisect.bisect_right(t, te) - 1
    return j >= 0 and dev[j] is not None and dev[j] < CHOP_BAND


def equity_stats(recs):
    """(profit$, maxDD$, calmar, n, win%, E%) from trades netted by EXIT timestamp (order-free)."""
    if not recs:
        return dict(profit=0.0, maxdd=0.0, calmar=0.0, n=0, win=0.0, e=0.0)
    by_t: dict = {}
    for r in recs:
        by_t[r["texit"]] = by_t.get(r["texit"], 0.0) + r["pct"] * CLIP
    eq = peak = mdd = 0.0
    for t in sorted(by_t):
        eq += by_t[t]
        peak = max(peak, eq)
        mdd = max(mdd, peak - eq)
    wins = sum(1 for r in recs if r["pct"] > 0)
    e = 100 * sum(r["pct"] for r in recs) / len(recs)
    cal = eq / mdd if mdd > 1e-9 else float("inf")
    return dict(profit=eq, maxdd=mdd, calmar=cal, n=len(recs), win=100 * wins / len(recs), e=e)


def row(label, st):
    cal = "inf " if st["calmar"] == float("inf") else f"{st['calmar']:>5.1f}"
    print(f"  {label:<34s} n={st['n']:>5d}  profit=${st['profit']:>+8.0f}  maxDD=${st['maxdd']:>7.0f}  "
          f"Calmar={cal}  win={st['win']:>4.1f}%  E/trade={st['e']:>+5.2f}%")


def main():
    print("#" * 104)
    print("# TB00811  OPERATOR-TIMED REGIME-SEGMENTED STRATEGY  (long in summer / short in winter; chop check)")
    print("# substrate: real 30-pair perps + funding, daily 2019-2026; causal BTC 200d-SMA regime (no lookahead)")
    print("#" * 104)
    sub = S.load_substrate()
    reg = sub["reg"]
    chop = build_chop(sub)
    mm = S.Margin(S.LEV0, S.MMR0)
    longp = S.build_perp_pool(sub["pp"], sub["pex"], S.LONG_PERP_CFG, mm)
    shortp = S.build_perp_pool(sub["pp"], sub["pex"], S.SHORT_PERP_CFG, mm)

    # live no-same-instrument-collision filter (applied to the short side when run with longs)
    longs_by_sym: dict = {}
    for r in longp:
        longs_by_sym.setdefault(r["sym"], []).append(
            OpenInterval(base_symbol=r["sym"], entry_time=r["t"], exit_time=r["texit"]))

    def not_collide(r):
        return not collides(OpenInterval(base_symbol=r["sym"], entry_time=r["t"], exit_time=r["texit"]),
                            longs_by_sym.get(r["sym"], []))

    L_bull = [r for r in longp if regime_at(reg, r["t"]) == "BULL"]
    L_bear = [r for r in longp if regime_at(reg, r["t"]) == "BEAR"]
    S_bull = [r for r in shortp if regime_at(reg, r["t"]) == "BULL"]
    S_bear = [r for r in shortp if regime_at(reg, r["t"]) == "BEAR"]

    # ---- 1) the 2x2 EV table: which side, which phase, makes money ----
    print("\n--- 1) WHERE THE MONEY IS: each side x each cycle phase (the core question) ---")
    row("LONG  in BULL  (summer long)", equity_stats(L_bull))
    row("LONG  in BEAR  (winter long)", equity_stats(L_bear))
    row("SHORT in BULL  (summer short)", equity_stats(S_bull))
    row("SHORT in BEAR  (winter short)", equity_stats(S_bear))

    # ---- 2) the strategies compared (full sample) ----
    print("\n--- 2) STRATEGY COMPARISON (full sample 2019-2026) ---")
    both = longp + [r for r in shortp if not_collide(r)]
    cycle = L_bull + [r for r in S_bear if not_collide(r)]      # Bill's: long-summer + short-winter
    anti = L_bear + [r for r in S_bull if not_collide(r)]       # the counter-cycle bleed
    row("ALWAYS LONG  (all longs)", equity_stats(longp))
    row("ALWAYS SHORT (all shorts)", equity_stats(shortp))
    row("ALWAYS BOTH  (the hedge)", equity_stats(both))
    row("CYCLE-TIMED  long-summer+short-winter", equity_stats(cycle))
    row("ANTI-CYCLE   long-winter+short-summer", equity_stats(anti))

    # ---- 3) the chop / sideways claim: does anything work in consolidation? ----
    print(f"\n--- 3) SIDEWAYS CHOP (|BTC/SMA200-1| < {CHOP_BAND:.0%}) vs TREND ---")
    Lc = [r for r in longp if is_chop(chop, r["t"])]
    Lt = [r for r in longp if not is_chop(chop, r["t"])]
    Sc = [r for r in shortp if is_chop(chop, r["t"])]
    St = [r for r in shortp if not is_chop(chop, r["t"])]
    row("LONG  in CHOP", equity_stats(Lc))
    row("LONG  in TREND", equity_stats(Lt))
    row("SHORT in CHOP", equity_stats(Sc))
    row("SHORT in TREND", equity_stats(St))
    cyc_chop = [r for r in L_bull if is_chop(chop, r["t"])] + [r for r in S_bear if is_chop(chop, r["t"]) and not_collide(r)]
    row("CYCLE-TIMED in CHOP", equity_stats(cyc_chop))

    # ---- 4) the CURRENT winter (BEAR from ~Nov-2025): what is each side doing now? ----
    print("\n--- 4) THE CURRENT CRYPTO WINTER (BTC BEAR, ~Nov-2025 -> Jun-2026) ---")
    t_reg = reg[0]
    # find first index where the tail BEAR run begins (last contiguous BEAR block)
    cutoff = None
    for j in range(len(t_reg) - 1, -1, -1):
        if reg[1][j] == "BULL":
            cutoff = t_reg[j + 1] if j + 1 < len(t_reg) else t_reg[j]
            break
    L_now = [r for r in longp if r["t"] >= cutoff]
    S_now = [r for r in shortp if r["t"] >= cutoff]
    Sn_chop = [r for r in S_now if is_chop(chop, r["t"])]
    row("LONG  in current winter", equity_stats(L_now))
    row("SHORT in current winter", equity_stats(S_now))
    row("SHORT in current winter+CHOP", equity_stats(Sn_chop))

    print("\n" + "#" * 104)
    print("# Read the E/trade column (edge per trade) + profit$. Positive E with enough trades = a real edge;")
    print("# near-zero E = chop death by fees. Propose-only, STAY IN PAPER, no lookahead (causal 200d SMA).")
    print("#" * 104)


if __name__ == "__main__":
    main()
