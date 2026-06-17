"""TB00775 - STOP-OPTIMIZATION FP/DP analysis: where must the MAE stop sit to stop forfeiting the edge?

FIRST PRINCIPLES. Expectancy E = P(win)*avg_win - P(loss)*avg_loss. A stop that is too tight raises
P(loss) and shrinks avg_loss (good for "loss prevention") but DESTROYS P(win) and avg_win because winners
are stopped on noise before they can run to the reward. "Stove-piping" on loss-prevention => negative E.
The cure is NOT a tweak to the 5m multiplier; it is to anchor the stop to the SAME volatility clock that
produces the reward. The reward is a run-to-reversal move over the DAILY regime horizon, so the stop must
survive DAILY-horizon adverse noise, not 5m noise.

THE METHOD (Maximum Adverse Excursion analysis, Sweeney; over the production run-to-reversal replay):
  1. Replay every historical entry to its L1a regime reversal on DAILY bars (the SAME detectors production
     uses - detect_daily_regime_downgrade / detect_htf_regime_reversal). The reversal IS the take-profit.
  2. Per entry capture: atr_frac = daily ATR(14)/entry at entry; MAE = the worst adverse excursion (using
     each daily bar's adverse EXTREME: high for a short, low for a long) over the hold; R = the signed
     run-to-reversal excursion at the reversal close.
  3. SWEEP a stop placed at k * daily-ATR. For each k and each entry: if MAE >= k*atr_frac the trade is
     STOPPED (P&L = -(k*atr_frac) - fees); else it runs to reversal (P&L = R - fees). E(k) = mean net P&L.
  4. Find k* that maximizes E. Report the whole curve (is the optimum a robust PLATEAU or a fragile peak?),
     the win/loss split, and the eventual-WINNER vs eventual-LOSER MAE separation (can a stop even
     separate them?). k=inf (never stop) = the raw run-to-reversal edge; if a finite k can't beat it, the
     honest answer is "stops near the reward horizon / structural invalidation, not noise-tight."

Reuses production units (faithful): rolling_classifications + the two reversal detectors + permitted_sides
+ atr_14_series + FEE_TAKER + margin_open_fee. Read-only public Kraken daily OHLC. Throwaway tool."""

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
from tothbot.exchange.regime_exit import (  # noqa: E402
    detect_daily_regime_downgrade,
    detect_htf_regime_reversal,
)
from tothbot.pipeline.sweep import permitted_sides  # noqa: E402
from tothbot.regime.indicators import atr_14_series  # noqa: E402
from tothbot.rest.client import KrakenRestClient  # noqa: E402

_TAKER = Decimal(str(FEE_TAKER_PCT))
_MARGIN_OPEN = Decimal(str(registry.value("margin_open_fee_pct")))

PAIRS = [
    "AVAX/USD", "AAVE/USD", "BCH/USD", "BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "ADA/USD",
    "DOT/USD", "LINK/USD", "UNI/USD", "LTC/USD", "DOGE/USD", "APT/USD", "ARKM/USD", "TIA/USD",
    "HBAR/USD", "OP/USD", "RUNE/USD", "WLD/USD", "PENDLE/USD", "INJ/USD", "SUI/USD", "NEAR/USD",
    "ATOM/USD", "FIL/USD", "TAO/USD", "RENDER/USD", "SEI/USD", "GRT/USD", "ALGO/USD", "FET/USD",
]

# stop multipliers in DAILY-ATR units to sweep (k * daily ATR). inf = never stop (raw reversal edge).
KS = [Decimal(s) for s in ("0.5", "1", "1.5", "2", "2.5", "3", "4", "5", "6", "8", "10", "15")]


class UrllibTransport:
    async def get(self, url: str, params: Mapping[str, object]) -> dict:
        q = urllib.parse.urlencode({k: str(v) for k, v in params.items()})

        def _do() -> dict:
            req = urllib.request.Request(url + ("?" + q if q else ""), headers={"User-Agent": "tb00775"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))

        return await asyncio.get_event_loop().run_in_executor(None, _do)

    async def post(self, *a, **k):  # pragma: no cover
        raise RuntimeError("read-only")

    async def close(self):
        return None


def _dec(v):
    return v if isinstance(v, Decimal) else Decimal(str(v))


class Entry:
    __slots__ = ("atr_frac", "mae", "rev", "bucket")
    def __init__(self, atr_frac, mae, rev, bucket):
        self.atr_frac = atr_frac   # daily ATR / entry  (the volatility unit)
        self.mae = mae             # worst adverse excursion fraction over the hold
        self.rev = rev             # signed run-to-reversal excursion at the reversal close
        self.bucket = bucket       # (symbol, regime) - the expected_reward provider key


def replay_entries(symbol, bars):
    """Per entry: (atr_frac at entry, MAE-to-reversal, signed reversal excursion). Entries with no
    in-window reversal are discarded (no realized run-to-reversal)."""
    classes = rolling_classifications(symbol, bars)
    highs = [_dec(b.high) for b in bars]
    lows = [_dec(b.low) for b in bars]
    closes = [_dec(b.close) for b in bars]
    atr = atr_14_series(highs, lows, closes, 14)  # aligns to closes (None/short early handled by len)
    out: list[Entry] = []
    n = len(bars)
    for i in range(n - 1):
        ci = classes[i]
        if ci is None or i >= len(atr) or atr[i] is None:
            continue
        entry = closes[i]
        if entry == 0:
            continue
        atr_frac = _dec(atr[i]) / entry
        if atr_frac <= 0:
            continue
        for side in permitted_sides(ci.regime):
            pos = Position(symbol=symbol, side=side, qty=Decimal(0), avg_entry_price=entry)
            mae = Decimal(0)
            for j in range(i + 1, n):
                cj = classes[j]
                # adverse extreme of THIS daily bar (short hurt by the high, long by the low)
                adv_px = highs[j] if side is PositionSide.SHORT else lows[j]
                adv = (adv_px - entry) if side is PositionSide.SHORT else (entry - adv_px)
                if adv > mae * entry:
                    mae = adv / entry
                if cj is None:
                    continue
                if detect_daily_regime_downgrade(pos, cj) or detect_htf_regime_reversal(pos, cj.ema20, cj.ema50):
                    rev = ((entry - closes[j]) / entry) if side is PositionSide.SHORT else ((closes[j] - entry) / entry)
                    out.append(Entry(atr_frac, mae, rev, (symbol, ci.regime)))
                    break
    return out


def _median(xs):
    s = sorted(xs)
    return s[len(s) // 2] if s else Decimal(0)


def gate_admitted(entries):
    """Proxy for the expected_reward viability gate: keep entries in (symbol, regime) buckets whose
    MEDIAN run-to-reversal excursion is positive (the bucket historically pays to run to reversal -
    the buckets the gate admits). The SSS 5m entry-timing filter cannot be replayed on daily bars, so
    this is the REGIME-gated subset (an upper-middle estimate of the strategy's real selectivity)."""
    by_bucket: dict = {}
    for e in entries:
        by_bucket.setdefault(e.bucket, []).append(e.rev)
    good = {b for b, revs in by_bucket.items() if _median(revs) > 0}
    return [e for e in entries if e.bucket in good], len(good), len(by_bucket)


def fees_for():
    # round-trip taker both sides; the SHORT borrow open fee is small + side-agnostic here -> include avg.
    return _TAKER + _TAKER


def expectancy(entries, k):
    """Mean net P&L (fraction of notional) if the stop sits at k*daily-ATR. k None = never stop."""
    fee = fees_for()
    pls = []
    stopped = 0
    for e in entries:
        if k is not None and e.mae >= k * e.atr_frac:
            pls.append(-(k * e.atr_frac) - fee)   # stopped at the k*ATR level
            stopped += 1
        else:
            pls.append(e.rev - fee)                # ran to the reversal
    m = sum(pls) / Decimal(len(pls))
    wins = [p for p in pls if p > 0]
    return {
        "k": k, "n": len(pls), "stopped_pct": 100.0 * stopped / len(pls),
        "win_rate": 100.0 * len(wins) / len(pls),
        "mean_pl_pct": float(m) * 100,
        "avg_win_pct": float(sum(wins) / len(wins)) * 100 if wins else 0.0,
        "losses": [p for p in pls if p <= 0],
        "avg_loss_pct": float(sum(p for p in pls if p <= 0) / max(1, len(pls) - len(wins))) * 100,
    }


async def main():
    client = KrakenRestClient(transport=UrllibTransport())
    seen = set()
    pairs = [p for p in PAIRS if not (p in seen or seen.add(p))]
    entries: list[Entry] = []
    for i, sym in enumerate(pairs, 1):
        try:
            resp = await client.get_ohlc_data(sym, 1440)
            es = replay_entries(sym, resp.committed)
        except Exception as e:  # noqa: BLE001
            print(f"  [{i}/{len(pairs)}] {sym:<12} skip ({type(e).__name__})")
            continue
        entries.extend(es)
        print(f"  [{i}/{len(pairs)}] {sym:<12} entries-with-reversal={len(es)}")
    await client.close()

    if not entries:
        print("no entries"); return
    print(f"\n================ STOP-OPTIMIZATION over {len(entries)} run-to-reversal entries ================")
    # the eventual winner/loser MAE separation (can a stop even separate them?)
    wmae = sorted(e.mae for e in entries if e.rev > 0)
    lmae = sorted(e.mae for e in entries if e.rev <= 0)
    def q(xs, p):
        return float(xs[min(len(xs) - 1, int(p * len(xs)))]) * 100 if xs else 0.0
    print(f"eventual WINNERS={len(wmae)}  LOSERS={len(lmae)}  (raw run-to-reversal win rate {100.0*len(wmae)/len(entries):.1f}%)")
    print(f"  winners' MAE  (median / p75 / p90): {q(wmae,.5):.2f}% / {q(wmae,.75):.2f}% / {q(wmae,.9):.2f}%")
    print(f"  losers'  MAE  (median / p75 / p90): {q(lmae,.5):.2f}% / {q(lmae,.75):.2f}% / {q(lmae,.9):.2f}%")
    avg_atr = float(sum(e.atr_frac for e in entries) / len(entries)) * 100
    print(f"  avg daily ATR = {avg_atr:.2f}% of price  (so k*ATR translates k -> a % stop)")

    print(f"\n{'stop k*ATR':>10} {'stop%':>7} {'stopped%':>9} {'winrate%':>9} {'avgWin%':>8} {'avgLoss%':>9} {'E[net]%/trade':>14}")
    rows = []
    for k in KS:
        r = expectancy(entries, k)
        stop_pct = float(k) * avg_atr
        rows.append((r["mean_pl_pct"], k, r))
        print(f"{float(k):>9.1f}x {stop_pct:>6.2f}% {r['stopped_pct']:>8.1f}% {r['win_rate']:>8.1f}% "
              f"{r['avg_win_pct']:>7.2f}% {r['avg_loss_pct']:>8.2f}% {r['mean_pl_pct']:>13.4f}%")
    rinf = expectancy(entries, None)
    print(f"{'inf (none)':>10} {'--':>7} {rinf['stopped_pct']:>8.1f}% {rinf['win_rate']:>8.1f}% "
          f"{rinf['avg_win_pct']:>7.2f}% {rinf['avg_loss_pct']:>8.2f}% {rinf['mean_pl_pct']:>13.4f}%")

    best = max(rows + [(rinf["mean_pl_pct"], None, rinf)], key=lambda t: t[0])
    bk = best[1]
    print(f"\nUNCONDITIONAL OPTIMUM: k* = {('infinite (no stop)' if bk is None else f'{float(bk):.1f}x daily ATR = {float(bk)*avg_atr:.2f}% stop')}"
          f"  ->  E[net] = {best[0]:+.4f}% / trade, win rate {best[2]['win_rate']:.1f}%")
    print("CURRENT 1.5x 5m-ATR sits FAR LEFT (5m ATR << daily ATR) -> ~the most-stopped row.")

    # ---- the REGIME-GATED subset (what the expected_reward gate actually admits) ----
    adm, ng, nb = gate_admitted(entries)
    if adm:
        wa = sorted(e.mae for e in adm if e.rev > 0)
        la = sorted(e.mae for e in adm if e.rev <= 0)
        print(f"\n================ REGIME-GATED subset (admitted {ng}/{nb} buckets, {len(adm)} entries) ================")
        print(f"  gated raw win rate {100.0*len(wa)/len(adm):.1f}%  winners' MAE med/p90 {q(wa,.5):.2f}%/{q(wa,.9):.2f}%  "
              f"losers' MAE med/p90 {q(la,.5):.2f}%/{q(la,.9):.2f}%")
        print(f"{'stop k*ATR':>10} {'stop%':>7} {'stopped%':>9} {'winrate%':>9} {'avgWin%':>8} {'avgLoss%':>9} {'E[net]%/trade':>14}")
        grows = []
        for k in KS:
            r = expectancy(adm, k)
            grows.append((r["mean_pl_pct"], k, r))
            print(f"{float(k):>9.1f}x {float(k)*avg_atr:>6.2f}% {r['stopped_pct']:>8.1f}% {r['win_rate']:>8.1f}% "
                  f"{r['avg_win_pct']:>7.2f}% {r['avg_loss_pct']:>8.2f}% {r['mean_pl_pct']:>13.4f}%")
        gri = expectancy(adm, None)
        print(f"{'inf (none)':>10} {'--':>7} {gri['stopped_pct']:>8.1f}% {gri['win_rate']:>8.1f}% "
              f"{gri['avg_win_pct']:>7.2f}% {gri['avg_loss_pct']:>8.2f}% {gri['mean_pl_pct']:>13.4f}%")
        gbest = max(grows + [(gri["mean_pl_pct"], None, gri)], key=lambda t: t[0])
        gk = gbest[1]
        print(f"GATED OPTIMUM: k* = {('infinite (no stop)' if gk is None else f'{float(gk):.1f}x daily ATR = {float(gk)*avg_atr:.2f}% stop')}"
              f"  ->  E[net] = {gbest[0]:+.4f}% / trade, win rate {gbest[2]['win_rate']:.1f}%")


if __name__ == "__main__":
    asyncio.run(main())
