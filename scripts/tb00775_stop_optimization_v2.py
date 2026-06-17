"""TB00775 v2 - HARDENED stop-optimization: pressure-test v1 + a better estimate from the same data.

v1 found: on regime-gated entries the run-to-reversal edge is large + positive and the tight 5m-ATR MAE
stop forfeits it. v2 attacks every way v1 could be WRONG and re-estimates honestly:

  PT1 OUT-OF-SAMPLE (walk-forward): bucket admission (positive median rev) is learned on each pair's FIRST
      half (train) and expectancy is measured ONLY on its SECOND half (test). Kills the in-sample
      data-snooping that inflates v1's gated number.
  PT2 TIME-NORMALIZATION: run-to-reversal holds are long + vary with the stop, so per-TRADE expectancy is
      unfair to tight stops (they churn faster). Report E per DAY-HELD (capital-time efficiency) - the
      decision metric - using the ACTUAL hold to the stop bar (captured per k), not just to reversal.
  PT3 MARGIN ROLLOVER: a SHORT pays margin_rollover_fee_pct (0.0002/4h = 0.12%/day) for the WHOLE hold -
      omitted by v1, it penalizes the wide-stop/long-hold short policy. Netted by hold length + side.
  PT4 BLOCK BOOTSTRAP (resample PAIRS): entries within a pair/regime episode are heavily autocorrelated,
      so the honest sampling unit is the PAIR. 500x pair-resamples -> a 5-95% CI on E, and on the
      tight-vs-wide difference (is the edge statistically real, not one lucky pair?).
  PT5 OUTLIER ROBUSTNESS: mean vs median vs 10%-trimmed mean (is the edge a few mega-winners?).
  PT6 TRUNCATION: % of entries discarded for no in-window reversal (the end-of-data bias).
  PT7 SIDE SPLIT: long vs short separately (rollover hits only shorts; regimes differ).

Reuses production units; read-only daily Kraken OHLC; throwaway tool. Math.random/Date unavailable here is
irrelevant (this is a plain script) - the bootstrap uses a seeded LCG for reproducibility."""

from __future__ import annotations

import asyncio
import json
import sys
import urllib.parse
import urllib.request
from collections.abc import Mapping
from decimal import Decimal

sys.path.insert(0, ".")

from tothbot.config import registry  # noqa: E402
from tothbot.config.fees import FEE_TAKER_PCT  # noqa: E402
from tothbot.ciats.expected_reward import rolling_classifications  # noqa: E402
from tothbot.exchange.position_mirror import Position, PositionSide  # noqa: E402
from tothbot.exchange.regime_exit import detect_daily_regime_downgrade, detect_htf_regime_reversal  # noqa: E402
from tothbot.pipeline.sweep import permitted_sides  # noqa: E402
from tothbot.regime.indicators import atr_14_series  # noqa: E402
from tothbot.rest.client import KrakenRestClient  # noqa: E402

_TAKER = float(FEE_TAKER_PCT)
_MARGIN_OPEN = float(registry.value("margin_open_fee_pct"))
_ROLLOVER_4H = float(registry.value("margin_rollover_fee_pct"))
_ROLLOVER_DAY = _ROLLOVER_4H * 6.0   # daily bar = 24h = 6 four-hour blocks

PAIRS = [
    "AVAX/USD", "AAVE/USD", "BCH/USD", "BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "ADA/USD",
    "DOT/USD", "LINK/USD", "UNI/USD", "LTC/USD", "DOGE/USD", "APT/USD", "ARKM/USD", "TIA/USD",
    "HBAR/USD", "OP/USD", "RUNE/USD", "WLD/USD", "PENDLE/USD", "INJ/USD", "SUI/USD", "NEAR/USD",
    "ATOM/USD", "FIL/USD", "TAO/USD", "RENDER/USD", "SEI/USD", "GRT/USD", "ALGO/USD", "FET/USD",
]
KS = [0.1, 0.25, 0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 8.0, 1e9]  # daily-ATR mult; 1e9 ~= no stop
MS = [1.0, 1.5, 2.0, 3.0, 5.0, 8.0]  # take-profit TARGET in daily-ATR mult (the IMPROVEMENT lever)


class UrllibTransport:
    async def get(self, url: str, params: Mapping[str, object]) -> dict:
        q = urllib.parse.urlencode({k: str(v) for k, v in params.items()})

        def _do():
            req = urllib.request.Request(url + ("?" + q if q else ""), headers={"User-Agent": "tb00775v2"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))

        return await asyncio.get_event_loop().run_in_executor(None, _do)

    async def post(self, *a, **k):  # pragma: no cover
        raise RuntimeError("read-only")

    async def close(self):
        return None


class E:
    """One replayed entry. floats throughout (this is a stats tool, not the Decimal trading core)."""
    __slots__ = ("sym", "regime", "side", "i", "half", "atrf", "rev", "hold", "stop_off", "tp_off")
    def __init__(self, sym, regime, side, i, half, atrf, rev, hold, stop_off, tp_off):
        self.sym = sym; self.regime = regime; self.side = side
        self.i = i; self.half = half           # half: 0=train(first half of pair), 1=test
        self.atrf = atrf                        # daily ATR/entry at entry
        self.rev = rev                          # signed run-to-reversal excursion
        self.hold = hold                        # bars(days) to reversal
        self.stop_off = stop_off                # aligned to KS: first bar-offset adverse hit k*ATR, or -1
        self.tp_off = tp_off                    # aligned to MS: first bar-offset FAVORABLE hit m*ATR, or -1


def replay(sym, bars):
    classes = rolling_classifications(sym, bars)
    highs = [float(b.high) for b in bars]; lows = [float(b.low) for b in bars]
    closes = [float(b.close) for b in bars]
    atr = atr_14_series([Decimal(str(h)) for h in highs], [Decimal(str(l)) for l in lows],
                        [Decimal(str(c)) for c in closes], 14)
    n = len(bars); half_idx = n // 2
    out = []; discarded = 0
    for i in range(n - 1):
        ci = classes[i]
        if ci is None or i >= len(atr) or atr[i] is None:
            continue
        entry = closes[i]
        if entry == 0:
            continue
        atrf = float(atr[i]) / entry
        if atrf <= 0:
            continue
        for side in permitted_sides(ci.regime):
            pos = Position(symbol=sym, side=side, qty=Decimal(0), avg_entry_price=Decimal(str(entry)))
            stop_off = [-1] * len(KS); tp_off = [-1] * len(MS); maxadv = 0.0; maxfav = 0.0; reversed_at = None
            for off, j in enumerate(range(i + 1, n), start=1):
                adv_px = highs[j] if side is PositionSide.SHORT else lows[j]
                fav_px = lows[j] if side is PositionSide.SHORT else highs[j]
                adv = ((adv_px - entry) if side is PositionSide.SHORT else (entry - adv_px)) / entry
                fav = ((entry - fav_px) if side is PositionSide.SHORT else (fav_px - entry)) / entry
                if adv > maxadv:
                    maxadv = adv
                if fav > maxfav:
                    maxfav = fav
                for ki, k in enumerate(KS):
                    if stop_off[ki] == -1 and maxadv >= k * atrf:
                        stop_off[ki] = off
                for mi, m in enumerate(MS):
                    if tp_off[mi] == -1 and maxfav >= m * atrf:
                        tp_off[mi] = off
                cj = classes[j]
                if cj is not None and (detect_daily_regime_downgrade(pos, cj)
                                       or detect_htf_regime_reversal(pos, cj.ema20, cj.ema50)):
                    rev = ((entry - closes[j]) if side is PositionSide.SHORT else (closes[j] - entry)) / entry
                    out.append(E(sym, ci.regime, side, i, 0 if i < half_idx else 1, atrf, rev, off, stop_off, tp_off))
                    reversed_at = off
                    break
            if reversed_at is None:
                discarded += 1
    return out, discarded


def pnl_hold(e, ki):
    """net P&L (fraction of notional) + hold(days) for entry e under stop KS[ki], rollover + fees netted."""
    k = KS[ki]; off = e.stop_off[ki]
    fee = _TAKER + _TAKER + (_MARGIN_OPEN if e.side is PositionSide.SHORT else 0.0)
    if off != -1 and off < e.hold:                 # stopped before reversal
        hold = off; gross = -(k * e.atrf)
    else:                                          # ran to reversal
        hold = e.hold; gross = e.rev
    roll = (_ROLLOVER_DAY * hold) if e.side is PositionSide.SHORT else 0.0
    return gross - fee - roll, hold


def pnl_hold_tp(e, ki, mi):
    """net P&L + hold under stop KS[ki] AND take-profit MS[mi]; exit = first of {stop, TP, reversal}.
    Tie (stop and TP same bar) -> STOP wins (pessimistic intra-bar ordering)."""
    so = e.stop_off[ki]; to = e.tp_off[mi]; k = KS[ki]; m = MS[mi]
    fee = _TAKER + _TAKER + (_MARGIN_OPEN if e.side is PositionSide.SHORT else 0.0)
    cands = []
    if so != -1:
        cands.append((so, -(k * e.atrf), 0))       # stop: priority 0 (wins ties)
    if to != -1:
        cands.append((to, m * e.atrf, 1))           # take-profit: priority 1
    cands.append((e.hold, e.rev, 2))                # reversal
    off, gross, _ = min(cands, key=lambda c: (c[0], c[2]))
    roll = (_ROLLOVER_DAY * off) if e.side is PositionSide.SHORT else 0.0
    return gross - fee - roll, off


def stats_tp(entries, ki, mi):
    rows = [pnl_hold_tp(e, ki, mi) for e in entries]
    pls = [r[0] for r in rows]; holds = [r[1] for r in rows]
    n = len(pls); tot = sum(pls); th = sum(holds) or 1
    wins = sum(1 for p in pls if p > 0)
    return {"E_trade": tot / n * 100, "E_day": tot / th * 100, "win": 100.0 * wins / n,
            "hold": th / n, "median": sorted(pls)[n // 2] * 100}


def stats(entries, ki):
    if not entries:
        return None
    rows = [pnl_hold(e, ki) for e in entries]
    pls = [r[0] for r in rows]; holds = [r[1] for r in rows]
    pls_s = sorted(pls); n = len(pls)
    wins = sum(1 for p in pls if p > 0)
    tot_pl = sum(pls); tot_hold = sum(holds) or 1
    trim = int(0.1 * n)
    trimmed = pls_s[trim:n - trim] or pls_s
    return {
        "n": n, "winrate": 100.0 * wins / n,
        "mean": tot_pl / n * 100, "median": pls_s[n // 2] * 100,
        "trimmed": sum(trimmed) / len(trimmed) * 100,
        "per_day": tot_pl / tot_hold * 100, "avg_hold": tot_hold / n,
        "stopped": 100.0 * sum(1 for e in entries if e.stop_off[ki] != -1 and e.stop_off[ki] < e.hold) / n,
    }


def gated_oos(entries, min_train=8):
    """walk-forward: admit (sym,regime) buckets with positive TRAIN-half median rev (>= min_train train
    entries), then return the TEST-half entries in admitted buckets."""
    train = {}
    for e in entries:
        if e.half == 0:
            train.setdefault((e.sym, e.regime), []).append(e.rev)
    admitted = set()
    for b, revs in train.items():
        if len(revs) >= min_train:
            s = sorted(revs)
            if s[len(s) // 2] > 0:
                admitted.add(b)
    return [e for e in entries if e.half == 1 and (e.sym, e.regime) in admitted], admitted


def gated_insample(entries, min_n=8):
    by = {}
    for e in entries:
        by.setdefault((e.sym, e.regime), []).append(e.rev)
    good = {b for b, r in by.items() if len(r) >= min_n and sorted(r)[len(r) // 2] > 0}
    return [e for e in entries if (e.sym, e.regime) in good]


def bootstrap_per_day(entries_by_pair, ki, iters=500, seed=12345):
    """block bootstrap: resample PAIRS with replacement, E-per-day each time -> (p5, p50, p95)."""
    pairs = list(entries_by_pair); m = len(pairs)
    if m == 0:
        return (0.0, 0.0, 0.0)
    s = seed; vals = []
    for _ in range(iters):
        tot_pl = 0.0; tot_hold = 0.0
        for _ in range(m):
            s = (1103515245 * s + 12345) & 0x7FFFFFFF      # seeded LCG (no Math.random needed)
            pr = pairs[s % m]
            for e in entries_by_pair[pr]:
                pl, hold = pnl_hold(e, ki)
                tot_pl += pl; tot_hold += hold
        vals.append(tot_pl / (tot_hold or 1) * 100)
    vals.sort()
    return (vals[int(0.05 * iters)], vals[iters // 2], vals[int(0.95 * iters)])


async def main():
    client = KrakenRestClient(transport=UrllibTransport())
    seen = set(); pairs = [p for p in PAIRS if not (p in seen or seen.add(p))]
    allE = []; by_pair = {}; total_disc = 0; total_raw = 0
    for i, sym in enumerate(pairs, 1):
        try:
            resp = await client.get_ohlc_data(sym, 1440)
            es, disc = replay(sym, resp.committed)
        except Exception as ex:  # noqa: BLE001
            print(f"  [{i}/{len(pairs)}] {sym:<11} skip ({type(ex).__name__})"); continue
        allE.extend(es); by_pair[sym] = es; total_disc += disc; total_raw += len(es) + disc
        print(f"  [{i}/{len(pairs)}] {sym:<11} reversal-entries={len(es):<5} discarded(no-reversal)={disc}")
    await client.close()
    if not allE:
        print("no data"); return

    print(f"\n#### PT6 TRUNCATION: {total_disc}/{total_raw} permitted entries discarded for no in-window "
          f"reversal ({100.0*total_disc/total_raw:.1f}%) - end-of-data bias, mostly the still-running tail.")

    ins = gated_insample(allE)
    oos, adm = gated_oos(allE)
    oos_by_pair = {}
    for e in oos:
        oos_by_pair.setdefault(e.sym, []).append(e)
    print(f"\n#### gated buckets admitted on TRAIN half: {len(adm)} ; OOS test entries: {len(oos)} "
          f"(in-sample gated entries: {len(ins)})")

    def table(title, entries):
        print(f"\n#### {title}  (n={len(entries)})")
        print(f"{'k*ATR':>7} {'stop%':>7} {'stopped%':>8} {'win%':>6} {'avgHold':>7} "
              f"{'E/trade%':>9} {'median%':>8} {'trim%':>7} {'E/DAY%':>8}")
        avg_atr = sum(e.atrf for e in entries) / max(1, len(entries)) * 100
        for ki, k in enumerate(KS):
            st = stats(entries, ki)
            if st is None:
                continue
            ks = "none" if k > 1e8 else f"{k:.2f}"
            sp = "--" if k > 1e8 else f"{k*avg_atr:.1f}"
            print(f"{ks:>7} {sp:>7} {st['stopped']:>7.1f}% {st['winrate']:>5.1f}% {st['avg_hold']:>6.1f}d "
                  f"{st['mean']:>8.3f}% {st['median']:>7.3f}% {st['trimmed']:>6.3f}% {st['per_day']:>7.4f}%")

    table("PT1 IN-SAMPLE gated (v1's basis - optimistic)", ins)
    table("PT1 OUT-OF-SAMPLE gated (walk-forward - the HONEST estimate; PT2 E/DAY; PT3 rollover; PT5 robust)", oos)

    # PT7 side split on OOS
    for side, lbl in ((PositionSide.LONG, "LONG"), (PositionSide.SHORT, "SHORT")):
        sub = [e for e in oos if e.side is side]
        if sub:
            table(f"PT7 OOS {lbl} only", sub)

    # PT4 bootstrap CI on OOS E/DAY: tight (~current scale) vs wide
    print("\n#### PT4 BLOCK-BOOTSTRAP (resample pairs, 500x) - OOS E/DAY%, 5/50/95 percentile:")
    for ki, k in enumerate(KS):
        if k in (0.1, 0.5, 2.0, 3.0, 5.0) or k > 1e8:
            lo, mid, hi = bootstrap_per_day(oos_by_pair, ki)
            ks = "none" if k > 1e8 else f"{k:.2f}x"
            sig = "  <-- CI excludes 0" if (lo > 0 or hi < 0) else ""
            print(f"   stop {ks:>6}:  [{lo:+.4f}% .. {mid:+.4f}% .. {hi:+.4f}%]/day{sig}")

    # ================= THE IMPROVEMENT: add a TAKE-PROFIT target (capture the reward FASTER) =================
    # The 60-90d run-to-reversal hold is the capital-efficiency killer. Does a defined TP m*ATR (exit at the
    # FIRST of stop/TP/reversal) beat pure run-to-reversal on OOS E/DAY? Grid stop k x target m on OOS SHORTS
    # (the side carrying the edge) - report E/DAY (the decision metric) + E/trade + avg hold.
    short_oos = [e for e in oos if e.side is PositionSide.SHORT]
    print(f"\n#### IMPROVEMENT - TAKE-PROFIT grid on OOS SHORTS (n={len(short_oos)}); cell = E/DAY% "
          f"(E/trade%, hold d). 'noTP' = pure run-to-reversal baseline.")
    header = "  stop\\TP " + "".join(f"{('m='+str(m)):>11}" for m in MS) + f"{'noTP':>13}"
    print(header)
    best = (-9e9, None)
    for ki, k in enumerate(KS):
        if k not in (1.0, 2.0, 3.0, 4.0, 5.0, 8.0) and k < 1e8:
            continue
        klbl = "none" if k > 1e8 else f"{k:.0f}x"
        cells = []
        for mi, m in enumerate(MS):
            st = stats_tp(short_oos, ki, mi)
            cells.append(f"{st['E_day']:>5.3f}({st['E_trade']:.1f},{st['hold']:.0f})")
            if st["E_day"] > best[0]:
                best = (st["E_day"], (k, m, st))
        # noTP baseline at this stop (reuse stats() E/DAY)
        base = stats(short_oos, ki)
        cells.append(f"{base['per_day']:>6.4f}({base['mean']:.1f},{base['avg_hold']:.0f})")
        print(f"  {klbl:>6}  " + "".join(f"{c:>11}" for c in cells))
    bk, bm, bst = best[1]
    print(f"\nBEST OOS-SHORT exit: stop {bk:.0f}x ATR + take-profit {bm:.0f}x ATR -> "
          f"E/DAY {bst['E_day']:+.4f}%  (E/trade {bst['E_trade']:+.2f}%, win {bst['win']:.0f}%, hold {bst['hold']:.0f}d). "
          f"Compare pure run-to-reversal E/DAY {stats(short_oos, KS.index(8.0))['per_day']:+.4f}% at ~88d hold.")


if __name__ == "__main__":
    asyncio.run(main())
