"""TB00812 - test ALL FIVE trading modes individually (Bill's WHAT), for a credible cross-mode analysis:
   (1) Spot Longs, (2) Long Perps only, (3) Long Perps + Short Hedge, (4) Short Perps only,
   (5) Short Perps + Long Hedge. Each reported individually, full-sample + by cycle phase.

Method: the TB00806 data substrate (real spot universe + 30-pair perp universe + funding, daily
2019-2026) + the CAUSAL BTC 200d-SMA regime (no lookahead). Profit = sum(pct * $50 clip) netted by
exit timestamp; maxDD from that curve; Calmar = profit/maxDD. The "+ hedge" modes run the PRIMARY
side at full size and the OPPOSITE side as a LIGHT insurance overlay (HEDGE_W), with the live
no-same-instrument-collision rule on the hedge side. Honest, propose-only, STAY IN PAPER, no lookahead.

NOTE on comparability: Spot (62 spot pairs, 0.26% taker) and Perps (30 perp pairs, ~0.05% taker +
funding) are DIFFERENT universes/fee regimes, so read each mode on its own terms, not $-for-$.
"""

from __future__ import annotations

import bisect

import tb00806_perp_account_sim as S

from tothbot.perp.collision import OpenInterval, collides

CLIP = float(S.NOTIONAL)   # $50 fixed-notional clip per trade
HEDGE_W = 0.30             # light insurance overlay weight for the "+ hedge" modes (CIATS-tunable knob)


def regime_at(reg, te):
    t, lab = reg
    j = bisect.bisect_right(t, te) - 1
    return lab[j] if j >= 0 else None


def winter_cutoff(reg):
    """Start time of the last contiguous BEAR block (the current crypto winter)."""
    t, lab = reg
    for j in range(len(t) - 1, -1, -1):
        if lab[j] == "BULL":
            return t[j + 1] if j + 1 < len(t) else t[j]
    return t[0]


def stats(book):
    """book = [(recs, weight), ...]. (profit$, maxDD$, calmar, n, win%, E%) netted by exit timestamp."""
    by_t: dict = {}
    allrecs = []
    for recs, w in book:
        for r in recs:
            by_t[r["texit"]] = by_t.get(r["texit"], 0.0) + r["pct"] * CLIP * w
            allrecs.append(r)
    if not allrecs:
        return dict(profit=0.0, maxdd=0.0, calmar=0.0, n=0, win=0.0, e=0.0)
    eq = peak = mdd = 0.0
    for t in sorted(by_t):
        eq += by_t[t]
        peak = max(peak, eq)
        mdd = max(mdd, peak - eq)
    wins = sum(1 for r in allrecs if r["pct"] > 0)
    e = 100 * sum(r["pct"] for r in allrecs) / len(allrecs)
    cal = eq / mdd if mdd > 1e-9 else float("inf")
    return dict(profit=eq, maxdd=mdd, calmar=cal, n=len(allrecs), win=100 * wins / len(allrecs), e=e)


def by_phase(book, reg, phase, cutoff=None):
    """Restrict each pool in the book to trades whose ENTRY regime == phase (or t >= cutoff)."""
    out = []
    for recs, w in book:
        if cutoff is not None:
            sel = [r for r in recs if r["t"] >= cutoff]
        else:
            sel = [r for r in recs if regime_at(reg, r["t"]) == phase]
        out.append((sel, w))
    return out


def prow(tag, st):
    cal = " inf" if st["calmar"] == float("inf") else f"{st['calmar']:>5.1f}"
    print(f"    {tag:<16s} n={st['n']:>5d}  profit=${st['profit']:>+8.0f}  maxDD=${st['maxdd']:>7.0f}  "
          f"Calmar={cal}  win={st['win']:>4.1f}%  E/trade={st['e']:>+5.2f}%")


def report(name, book, reg, cutoff):
    print(f"\n  {name}")
    prow("FULL 2019-2026", stats(book))
    prow("BULL (summer)", stats(by_phase(book, reg, "BULL")))
    prow("BEAR (winter)", stats(by_phase(book, reg, "BEAR")))
    prow("CURRENT winter", stats(by_phase(book, reg, None, cutoff)))


def main():
    print("#" * 104)
    print("# TB00812  ALL FIVE TRADING MODES, REPORTED INDIVIDUALLY  (for a credible cross-mode analysis)")
    print("# substrate: real spot + 30-pair perp universe + funding, daily 2019-2026; causal BTC 200d-SMA regime")
    print(f"# clip=${CLIP:.0f}/trade; '+ hedge' = primary full + opposite side a LIGHT {HEDGE_W:.0%} insurance overlay")
    print("#" * 104)
    sub = S.load_substrate()
    reg = sub["reg"]
    cutoff = winter_cutoff(reg)
    mm = S.Margin(S.LEV0, S.MMR0)

    spot = S.build_spot_pool(sub["spot_data"])
    longp = S.build_perp_pool(sub["pp"], sub["pex"], S.LONG_PERP_CFG, mm)
    shortp = S.build_perp_pool(sub["pp"], sub["pex"], S.SHORT_PERP_CFG, mm)

    # live no-same-instrument-collision filter (hedge side vs the primary side's open intervals)
    def iv_by_sym(recs):
        d: dict = {}
        for r in recs:
            d.setdefault(r["sym"], []).append(
                OpenInterval(base_symbol=r["sym"], entry_time=r["t"], exit_time=r["texit"]))
        return d

    long_iv = iv_by_sym(longp)
    short_iv = iv_by_sym(shortp)

    def keep(recs, opp_iv):
        return [r for r in recs if not collides(
            OpenInterval(base_symbol=r["sym"], entry_time=r["t"], exit_time=r["texit"]),
            opp_iv.get(r["sym"], []))]

    short_hedge = keep(shortp, long_iv)   # short trades not colliding with an open long
    long_hedge = keep(longp, short_iv)    # long trades not colliding with an open short

    # ---- the five modes ----
    report("MODE 1  SPOT LONGS (the deployed EMA12/26 spot organism; spot, no leverage)",
           [(spot, 1.0)], reg, cutoff)
    report("MODE 2  LONG PERPS ONLY (perp breakout long; ~0.05% fee + funding)",
           [(longp, 1.0)], reg, cutoff)
    report(f"MODE 3  LONG PERPS + SHORT HEDGE (long full + short {HEDGE_W:.0%} insurance overlay)",
           [(longp, 1.0), (short_hedge, HEDGE_W)], reg, cutoff)
    report("MODE 4  SHORT PERPS ONLY (perp mean-reversion short)",
           [(shortp, 1.0)], reg, cutoff)
    report(f"MODE 5  SHORT PERPS + LONG HEDGE (short full + long {HEDGE_W:.0%} insurance overlay)",
           [(shortp, 1.0), (long_hedge, HEDGE_W)], reg, cutoff)

    print("\n" + "#" * 104)
    print("# READ: E/trade = the per-trade edge (the durable signal); profit$ scales with trade count + clip.")
    print("# maxDD/Calmar = the risk. BULL/BEAR/CURRENT-winter rows show WHEN each mode earns (the on/off call).")
    print(f"# '+ hedge' uses a {HEDGE_W:.0%} overlay (a CIATS-tunable knob); heavier hedge -> more DD cut, less return.")
    print("# Spot (0.26% fee, 62 pairs) vs Perps (~0.05% fee + funding, 30 pairs) are different universes - read each")
    print("# on its own terms. Propose-only, STAY IN PAPER, no lookahead (causal 200d SMA).")
    print("#" * 104)


if __name__ == "__main__":
    main()
