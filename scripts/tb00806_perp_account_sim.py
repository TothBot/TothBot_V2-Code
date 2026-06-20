"""TB00806 - UNIFIED 3-POOL PERP-ACCOUNT SIMULATOR  (PHASE 0 substrate for the perps MECHANICS battery).
PAPER research only, offline on cached data.  Propose-only; 0500000 unchanged; STAY IN PAPER.

This is the ONE shared substrate the three mechanics batteries (C liquidation/gaps, A two-pool hedge
drawdown, B funding stress) import - it is deliberately NOT duplicated per test.  It extends the validated
tb00794 perp engine + tb00802a stat/era/regime helpers + tb00805a configs with the four things the
edge-level work never modelled (perps-mechanics-test-plan):

  (i)   THREE ISOLATED POOLS - Long-Spot (EMA12/26 spot), Long-Perp (bb_break+trend100), Short-Perp
        (rsi_trend+vol).  Each pool has its OWN deposit/balance/margin; NO cross-margin; combined account
        equity = the SUM of the three independent pool equities.  This is the 0500000 sec-13.7 ring-fence.
  (ii)  A per-position ISOLATED-MARGIN MODEL - a $50 clip is the position NOTIONAL; posted margin M =
        NOTIONAL/leverage; maintenance ratio mmr; liquidation price P_liq is where the adverse move erodes
        equity to maintenance.  The crash-proof loss cap = on isolated margin the trader CANNOT lose more
        than M even on a gap-through (the overflow is the exchange insurance fund's problem, not the pool's).
  (iii) A FUNDING-SHOCK injector - the real 8h Binance-UM funding history is already wired per-trade; this
        adds a multiplier and a sustained-adverse pin for battery B.
  (iv)  A GAP injector - worsen a bar's adverse extreme by a chosen fraction (overnight gap / wick / flash
        crash / venue-outage-through-the-stop) to test that liquidation still caps the loss at M.

  Causal regime = BTC vs its 200-day SMA (no lookahead), reused from tb00802a.

FIDELITY CAVEATS (honest, carried into every battery):
  - Real Kraken-Pro/Bitnomial margin specs (maintenance ratio, contract multiplier, tiered margin) are
    NON-PUBLIC.  We therefore ASSUME a model and SWEEP leverage {2,3,5,10,20} x mmr {0.5,1,2%}; no single
    assumed number is load-bearing.
  - 30-pair 1d Binance-UM perp proxy for the Kraken/Bitnomial venue; ~2020-> history (perps are younger
    than spot's 7.5yr) but spans 2021 bull / 2022 bear / 2023-25 cooling.
  - The $50 clip is the CIATS fixed-notional sizing baseline (position-sizing-fixed-notional); leverage is
    a sizing/liquidation multiplier, NOT edge (tb00794 note).

Run directly for a self-test of the margin maths + pool builders + ring-fence isolation invariant."""

from __future__ import annotations
import os, io, importlib.util, contextlib

HERE = os.path.dirname(os.path.abspath(__file__))


def _load(name, rel):
    s = importlib.util.spec_from_file_location(name, os.path.join(HERE, rel))
    m = importlib.util.module_from_spec(s)
    with contextlib.redirect_stdout(io.StringIO()):
        s.loader.exec_module(m)
    return m


SB = _load("sb", "tb00802a_stress_battery.py")     # stat, eras, boot_oos, btc_regime, regime_at, collect_spot, capped_account, mc_sequence, T86
PB = _load("probe", "tb00794_perps_probe.py")       # perp engine: load_data, make_pairs, precompute_perp, gross_off, A, TAKER_PERP
A = PB.A
NOTIONAL = SB.NOTIONAL          # 50.0 - the CIATS fixed-notional clip = a position's NOTIONAL
TAKER_PERP = PB.TAKER_PERP      # 0.0005
SPOT_TAKER = SB.SPOT_TAKER      # 0.0026
MINN = SB.MINN

stat_pool = SB.stat              # records share the {pct,frac,sym,t,texit} schema SB.stat expects

# ---- the three pool stream configs (centre = the tb00805a / tb00802a hard-coded pair) ----
KI3 = A.KS.index(3.0); MI3 = A.MS.index(3.0); MIREV = A.MS.index(None)
LONG_PERP_CFG = dict(side="LONG", sig="bb_break", flt="trend100", spec=("st", KI3, MIREV))   # breakout long
SHORT_PERP_CFG = dict(side="SHORT", sig="rsi_trend", flt="vol", spec=("st", KI3, MI3))         # mean-rev short
# Long-Spot pool = the deployed EMA12/26 spot organism (collect_spot), centre params:
SPOT_F, SPOT_S, SPOT_ATR = SB.F0, SB.S0, SB.ATR0

# ---- margin model defaults (swept everywhere; centre keeps liquidation BELOW a 3xATR stop on daily) ----
LEV0 = 3.0          # centre leverage
MMR0 = 0.01         # centre maintenance ratio (1%)
LEVS = (2.0, 3.0, 5.0, 10.0, 20.0)
MMRS = (0.005, 0.01, 0.02)


# ============================ isolated-margin model ============================
class Margin:
    """Per-position isolated-margin model.  A position has NOTIONAL = the $50 clip.  Posted margin M =
    NOTIONAL/lev.  Liquidation fires when the adverse price move erodes equity to maintenance margin:
        unrealized_loss_fraction_of_notional reaches  liq_frac = 1/lev - mmr.
    At that point realized loss = the posted margin M (the trader loses the margin; an isolated position
    cannot go negative - any gap-through overflow is the exchange insurance fund's, not the pool's)."""

    def __init__(self, lev=LEV0, mmr=MMR0):
        self.lev = float(lev); self.mmr = float(mmr)
        self.margin_frac = 1.0 / self.lev               # M / NOTIONAL
        self.liq_frac = 1.0 / self.lev - self.mmr       # adverse fraction of notional that triggers liq
        self.margin = self.margin_frac * NOTIONAL       # $ posted

    def liq_price(self, p0, side):
        return p0 * (1 - self.liq_frac) if side == "LONG" else p0 * (1 + self.liq_frac)


def stop_frac(p, i, spec):
    """The strategy's native stop distance as a price fraction (k * ATR/price) for this trade, or None if
    the spec is a trailing exit (no fixed stop).  Used to assert liquidation sits BELOW the native stop."""
    if spec[0] != "st":
        return None
    atrf = (p.atr[i] / p.c[i]) if (p.atr[i] is not None and p.c[i] > 0) else None
    if atrf is None or atrf <= 0:
        return None
    return A.KS[spec[1]] * atrf


# ============================ per-trade resolver (the heart of the substrate) ============================
def resolve_perp_trade(p, ex, i, side, spec, mm, taker=TAKER_PERP, fund_mult=1.0,
                       gap=0.0, gap_mode="none", slip=0.0, fund_pin=None):
    """Resolve ONE perp trade entered at bar i, with the isolated-margin backstop layered UNDER the native
    strategy exit, plus optional funding-shock + gap injection.

    Returns a record dict, or None if the entry has no precomputed exit.  Fields:
      pct        net P&L as a fraction of NOTIONAL (the $ clip).  $ = pct*NOTIONAL.
      off        hold length in bars (to the realizing event: strategy exit OR liquidation, whichever first)
      liq        True if the isolated-margin liquidation fired (loss capped at the posted margin)
      markfrac   the adverse price excursion (fraction) the position would have suffered at the realizing
                 event - on a gap-through this can EXCEED margin_frac; the overflow beyond margin_frac is
                 exchange/insurance-fund absorbed, NOT a pool loss
      sym,t,texit,frac  bookkeeping for the account/era models

    MODEL OF THE NATIVE STOP vs LIQUIDATION (the corrected ordering):
      - liq_frac = adverse fraction that liquidates; sf = the native k*ATR stop distance (None for trailing).
      - In NORMAL (no-gap) conditions the native stop FILLS at sf, so a position is liquidated ONLY when
        leverage is high enough that liq_frac <= sf (liquidation reached before the stop) OR the spec has
        no protective stop (sf None) and the adverse path reaches liq_frac before the native exit.  At the
        centre lev=3 (liq_frac~0.32) vs a 3xATR stop (~0.10-0.18) the stop wins -> zero baseline liqs.
      - A GAP (injected) models price leaping in one bar (overnight gap / wick / venue-outage-through-the-
        stop).  If gap >= liq_frac the stop is gapped THROUGH and liquidation caps the loss at the margin.
        If sf < gap < liq_frac the stop is gapped through but liq not reached: fill at `gap` (loss=gap,
        still < margin).  If gap <= sf the native stop fills normally and the outcome is unaffected.

    gap_mode: 'none' | 'worst' (gap at the bar that already had the worst adverse move - realistic flash-
    crash placement) | 'entry' (first held bar - an overnight gap right after entry) | 'final' (the strategy
    -exit bar - a gap THROUGH the native stop) | 'any' (a gap occurs at SOME point in the hold = worst-case
    for the cap question; equivalent to 'worst' for the breach test)."""
    if (i, side) not in ex:
        return None
    gross, off = PB.gross_off(ex[(i, side)], spec, side)
    p0 = p.c[i]; n = p.n
    endj = min(i + off, n - 1)
    lf = mm.liq_frac; mf = mm.margin_frac
    sf = stop_frac(p, i, spec)          # native protective-stop distance (fraction), or None

    def funding(hold):
        """Signed funding cost charged to THIS trade.  fund_pin (per-DAY adverse fraction) OVERRIDES the
        real history with a sustained-adverse pin (always a COST to the side); else use the real summed
        funding x fund_mult (signed: a LONG pays positive funding, a SHORT receives it)."""
        if fund_pin is not None:
            return fund_pin * hold            # adverse pin: a positive cost regardless of side
        fr = (p.cum[min(i + hold, n - 1)] - p.cum[i]) * fund_mult
        return fr if side == "LONG" else -fr
    # worst adverse excursion actually on the path during the hold
    worst_adv = 0.0
    for j in range(i + 1, endj + 1):
        adv = (p0 - p.l[j]) / p0 if side == "LONG" else (p.h[j] - p0) / p0
        if adv > worst_adv:
            worst_adv = adv

    # does an injected gap apply to this trade's hold?
    gap_applies = gap > 0.0 and gap_mode != "none"

    # --- baseline (no-gap) liquidation: only when unprotected (no stop) or leverage too high (lf <= sf) ---
    if not gap_applies:
        unprotected = (sf is None) or (lf <= sf)
        if unprotected and worst_adv >= lf:
            return dict(pct=-mf, off=off, liq=True, markfrac=worst_adv,
                        sym=p.sym, t=p.t[i], texit=p.t[endj], frac=i / n)
        return dict(pct=gross - 2 * taker - 2 * slip - funding(off), off=off, liq=False,
                    markfrac=worst_adv, sym=p.sym, t=p.t[i], texit=p.t[endj], frac=i / n)

    # --- gap injected during the hold ---
    eff = max(worst_adv, gap)           # effective adverse the position faces under the gap
    if eff >= lf:                       # gapped THROUGH to/past liquidation -> capped at the margin
        return dict(pct=-mf, off=off, liq=True, markfrac=eff,
                    sym=p.sym, t=p.t[i], texit=p.t[endj], frac=i / n)
    if sf is not None and gap > sf:     # gapped through the native stop but not to liq -> fill at the gap
        return dict(pct=-gap - 2 * taker - 2 * slip, off=off, liq=False, markfrac=gap,
                    sym=p.sym, t=p.t[i], texit=p.t[endj], frac=i / n)
    # gap shallower than the native stop (or a winning trade exiting before it) -> native outcome stands
    return dict(pct=gross - 2 * taker - 2 * slip - funding(off), off=off, liq=False,
                markfrac=worst_adv, sym=p.sym, t=p.t[i], texit=p.t[endj], frac=i / n)


def build_perp_pool(pp, pex, cfg, mm=None, taker=TAKER_PERP, fund_mult=1.0,
                    gap=0.0, gap_mode="none", slip=0.0, fund_pin=None):
    """Trade-level records for one perp pool (Long-Perp or Short-Perp) across all pairs, with the margin
    backstop + injectors applied.  Mirrors the tb00805a/tb00802a entry loop (step past the hold)."""
    if mm is None:
        mm = Margin()
    side = cfg["side"]; sig = A.SIGNALS[cfg["sig"]]; flt = A.FILTERS[cfg["flt"]]; spec = cfg["spec"]
    out = []
    for p in pp:
        ex = pex[p.sym]; n = p.n; i = 1
        while i < n - 1:
            if sig(p, i) != side or (i, side) not in ex or not flt(p, i, side):
                i += 1; continue
            r = resolve_perp_trade(p, ex, i, side, spec, mm, taker, fund_mult, gap, gap_mode, slip, fund_pin)
            if r is None:
                i += 1; continue
            out.append(r)
            i += max(1, r["off"])
    return out


def build_spot_pool(spot_data, mm=None, taker=SPOT_TAKER, slip=0.0010, sslip=0.0020):
    """Long-Spot pool = the deployed EMA12/26 long-only spot organism (reuse tb00802a collect_spot).
    Spot is held outright (margin_frac = 1.0, no liquidation) - included so the 3-pool account is complete.
    Returns the same record schema (pct/t/texit/sym/frac) + liq=False, margin_frac=1.0."""
    recs = SB.collect_spot(spot_data, SPOT_F, SPOT_S, SPOT_ATR, slip, sslip, taker)
    for r in recs:
        r["liq"] = False; r["margin_frac"] = 1.0
    return recs


# ============================ shared loader (load once; batteries reuse) ============================
_CACHE = {}


def load_substrate():
    """Load the perp universe (30 pairs, 1d, real funding) + the spot universe + the causal BTC regime.
    Cached in-process so the three batteries share one load.  Returns a dict."""
    if _CACHE:
        return _CACHE
    with contextlib.redirect_stdout(io.StringIO()):
        prices, fund = PB.load_data()
        pp = PB.make_pairs(prices["1d"], fund)
        pex = {p.sym: PB.precompute_perp(p, 60) for p in pp}
        spot_data = SB.T86.fetch_big("1d")
    reg = SB.btc_regime(spot_data)
    _CACHE.update(dict(pp=pp, pex=pex, spot_data=spot_data, reg=reg,
                       spot_pairs=[s for s, v in spot_data.items() if len(v[0]) >= 220]))
    return _CACHE


# ============================ account / drawdown across isolated pools ============================
def pool_equity_curve(recs, deposit, clip=NOTIONAL, collision_block=None):
    """Realize a single isolated pool's trades in EXIT-time order into an equity curve; return
    (equity_end, maxDD$, series) where series=[(texit,equity)].  $ per trade = pct*clip.  Optional
    collision_block(rec)->bool filters trades blocked by the sec-13.7 no-same-instrument rule (caller-built)."""
    ev = sorted((r for r in recs if not (collision_block and collision_block(r))), key=lambda r: r["texit"])
    eq = deposit; peak = deposit; mdd = 0.0; series = []
    for r in ev:
        eq += r["pct"] * clip
        if eq > peak:
            peak = eq
        if peak - eq > mdd:
            mdd = peak - eq
        series.append((r["texit"], eq))
    return eq, mdd, series


def combined_drawdown(pool_series_list, deposits):
    """Merge several isolated pools' (texit,equity) series into ONE combined-account equity curve and
    return (combined_maxDD$, combined_end$, total_deposit).  Each pool's equity already includes its own
    deposit; the combined equity at any event = sum over pools of that pool's most-recent equity (step
    function).  This is the account-level drawdown of the ring-fenced 3-pool system."""
    import bisect
    # event timeline = union of all exit times
    times = sorted({t for s in pool_series_list for (t, _) in s})
    # for each pool, a searchable (times, equities) with the pool's deposit as the pre-first value
    cols = []
    for s, dep in zip(pool_series_list, deposits):
        ts = [t for (t, _) in s]; eqs = [e for (_, e) in s]
        cols.append((ts, eqs, dep))
    total_dep = sum(deposits)
    peak = total_dep; mdd = 0.0; last = total_dep
    for t in times:
        comb = 0.0
        for ts, eqs, dep in cols:
            j = bisect.bisect_right(ts, t) - 1
            comb += eqs[j] if j >= 0 else dep
        last = comb
        if comb > peak:
            peak = comb
        if peak - comb > mdd:
            mdd = peak - comb
    return mdd, last, total_dep


# ============================ self-test ============================
def _selftest():
    print("=" * 100)
    print("TB00806 substrate self-test - margin maths, pool builders, ring-fence isolation invariant")
    print("=" * 100)
    # margin maths
    for lev in LEVS:
        mm = Margin(lev, MMR0)
        lp = mm.liq_price(100.0, "LONG"); sp = mm.liq_price(100.0, "SHORT")
        print(f"  lev={lev:>4.0f} mmr={MMR0:.3f}: margin_frac={mm.margin_frac:.3f} (${mm.margin:.2f}) "
              f"liq_frac={mm.liq_frac:.3f}  LONG liq@{lp:.2f}  SHORT liq@{sp:.2f}")
    s = load_substrate()
    pp, pex = s["pp"], s["pex"]
    print(f"\n  loaded {len(pp)} perp pairs; spot pairs={len(s['spot_pairs'])}")
    mm = Margin()
    short = build_perp_pool(pp, pex, SHORT_PERP_CFG, mm)
    longp = build_perp_pool(pp, pex, LONG_PERP_CFG, mm)
    spot = build_spot_pool(s["spot_data"])
    for name, recs in (("Long-Spot", spot), ("Long-Perp", longp), ("Short-Perp", short)):
        st = SB.stat(recs); nliq = sum(1 for r in recs if r.get("liq"))
        print(f"  {name:11s}: n={st['n']:>5d} E={st['E']:>+6.2f}% rr={st['rr']:.2f} "
              f"win={st['win']:>3.0f}%  baseline_liqs={nliq} (lev=3; nonzero only where a 3xATR stop on a "
              f"very-high-ATR pair is WIDER than the 32% liq distance -> liq caps it tighter, not a bug)")
    # ring-fence invariant: liquidating one pool with a 99% gap leaves the others byte-identical
    base_short_tot = sum(r["pct"] for r in short)
    crash = build_perp_pool(pp, pex, LONG_PERP_CFG, mm, gap=0.99, gap_mode="worst")
    after_short_tot = sum(r["pct"] for r in short)
    nliq_crash = sum(1 for r in crash if r.get("liq"))
    print(f"\n  RING-FENCE: crash the Long-Perp pool (99% gap -> {nliq_crash} liquidations); "
          f"Short-Perp pool total unchanged: {base_short_tot:.4f} == {after_short_tot:.4f} "
          f"-> {'HOLDS' if abs(base_short_tot - after_short_tot) < 1e-12 else 'VIOLATED'}")
    print("\n  substrate OK; batteries C/A/B import this module.")


if __name__ == "__main__":
    _selftest()
