"""TB00774 (#1-A) - INDICATIVE realized-exit backtest over the available 5m window.

Throwaway tool (NOT committed). Drives the REAL production units over fetched Kraken history to test the
#2 hypothesis empirically: does the L2 MAE stop (1.5x ATR on 5m) fire on intraday noise before the
daily run-to-reversal REWARD can be reached, and at what net R:R?

REUSES PRODUCTION UNITS (faithful, not a reimplementation):
  - tothbot.regime.live_indicators.LiveIndicators  - the real incremental RSI/EMA9/EMA21/ATR(14) + the
    real SSS entry verdict (the G5 signal gate), seeded + stepped exactly as the live WS-Manager does.
  - tothbot.regime.engine.compute_regime            - the daily regime -> permitted side(s) (G3).
  - the production stop rule: MAE >= atr_14_entry * mae_mult (L2) / emergency_sl_mult (L3), the same
    registry constants paper_exit.py uses, with the direction-symmetric ar:AR-048 adverse leg.
  - FEE_TAKER_PCT + margin_open_fee_pct + the actual_rr fee-inclusive net_loss basis (the TB00774 fix).

SCOPE / HONESTY (the data wall): Kraken public OHLC caps at ~720 bars, so 5m = ~2.5 days/pair. The
REWARD leg (the L1a daily/1H regime reversal) operates on a DAILY timescale and essentially CANNOT fire
inside a 2.5-day window - which IS the heart of finding #2. So this run measures the STOP dynamics + the
favorable-vs-adverse excursion an entry actually sees intraday; the reward leg needs the deep 5m corpus
that #1-B accumulates. Peripheral gates (G1 liquidity / G2 / G7 risk-guard / cooldown) are skipped - they
only REDUCE entries, never change the exit race. bbo is approximated by the bar extremes (high=worst for a
short, low=worst for a long) - the faithful intra-bar stop-touch; a stopped trade fills AT the stop
threshold (entry +/- mae_mult*ATR), not the overshoot."""

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
from tothbot.exchange.position_mirror import PositionSide  # noqa: E402
from tothbot.pipeline.sweep import permitted_sides  # noqa: E402
from tothbot.regime.engine import RegimeComputeError, compute_regime  # noqa: E402
from tothbot.regime.live_indicators import LiveIndicators  # noqa: E402
from tothbot.regime.sss import SignalSide  # noqa: E402
from tothbot.rest.client import KrakenRestClient  # noqa: E402

_TAKER = Decimal(str(FEE_TAKER_PCT))
_MAE_MULT = Decimal(str(registry.value("mae_mult")))               # 1.5x ATR -> L2 stop
_EMERG_MULT = Decimal(str(registry.value("emergency_sl_mult")))    # 3.0x ATR -> L3 backstop
_MARGIN_OPEN = Decimal(str(registry.value("margin_open_fee_pct"))) # short borrow open fee

# A representative liquid/viable slice (the 3 that traded live + majors + top-viable from the screen).
PAIRS = [
    "AVAX/USD", "AAVE/USD", "BCH/USD", "BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "ADA/USD",
    "DOT/USD", "LINK/USD", "UNI/USD", "LTC/USD", "DOGE/USD", "APT/USD", "ARKM/USD", "TIA/USD",
    "HBAR/USD", "OP/USD", "RUNE/USD", "WLD/USD", "PENDLE/USD", "INJ/USD", "SUI/USD", "NEAR/USD",
    "ATOM/USD", "FIL/USD", "AAVE/USD", "TAO/USD", "RENDER/USD", "SEI/USD",
]


class UrllibTransport:
    async def get(self, url: str, params: Mapping[str, object]) -> dict:
        q = urllib.parse.urlencode({k: str(v) for k, v in params.items()})
        full = url + ("?" + q if q else "")

        def _do() -> dict:
            req = urllib.request.Request(full, headers={"User-Agent": "tb00774-backtest"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))

        return await asyncio.get_event_loop().run_in_executor(None, _do)

    async def post(self, *a, **k):  # pragma: no cover
        raise RuntimeError("read-only")

    async def close(self) -> None:
        return None


def _sig_side(side: PositionSide) -> SignalSide:
    return SignalSide.LONG if side is PositionSide.LONG else SignalSide.SHORT


class Trade:
    __slots__ = ("side", "entry", "atr", "exit_reason", "exit_price", "mfe_pct", "mae_pct")

    def __init__(self, side, entry, atr):
        self.side = side
        self.entry = entry
        self.atr = atr
        self.exit_reason = None
        self.exit_price = None
        self.mfe_pct = Decimal(0)   # max FAVORABLE excursion over the hold (the reachable reward proxy)
        self.mae_pct = Decimal(0)   # max ADVERSE excursion over the hold (the heat)

    def adverse(self, px):
        return (px - self.entry) if self.side is PositionSide.SHORT else (self.entry - px)

    def favorable(self, px):
        return (self.entry - px) if self.side is PositionSide.SHORT else (px - self.entry)

    def net_loss_R(self):  # the fee-inclusive 1R (the entry-gate / TB00774 actual_rr basis), as a fraction
        borrow = _MARGIN_OPEN if self.side is PositionSide.SHORT else Decimal(0)
        return (self.atr * _MAE_MULT / self.entry) + _TAKER + _TAKER + borrow

    def net_pl_pct(self):
        if self.exit_price is None:
            return None
        gross = self.favorable(self.exit_price) / self.entry
        borrow = _MARGIN_OPEN if self.side is PositionSide.SHORT else Decimal(0)
        return gross - _TAKER - _TAKER - borrow

    def actual_rr(self):
        pl = self.net_pl_pct()
        return None if pl is None else pl / self.net_loss_R()


async def backtest_pair(client, symbol):
    daily = await client.get_ohlc_data(symbol, 1440)
    try:
        cls = compute_regime(symbol, daily.committed, exclude_forming=False)
    except RegimeComputeError:
        return None
    sides = permitted_sides(cls.regime)
    if not sides:
        return None
    five = await client.get_ohlc_data(symbol, 5)
    bars = five.committed
    ind = LiveIndicators(symbol)
    seed_n = ind.min_seed_closes
    if len(bars) <= seed_n + 5:
        return None
    ind.seed_from_bars(bars[:seed_n])
    trades: list[Trade] = []
    open_by_side: dict[PositionSide, Trade] = {}
    for bar in bars[seed_n:]:
        # mark + exit any open positions on THIS bar (intra-bar stop touch via the adverse extreme)
        for side in list(open_by_side):
            t = open_by_side[side]
            adv_extreme = bar.high if side is PositionSide.SHORT else bar.low
            fav_extreme = bar.low if side is PositionSide.SHORT else bar.high
            t.mae_pct = max(t.mae_pct, t.adverse(adv_extreme) / t.entry)
            t.mfe_pct = max(t.mfe_pct, t.favorable(fav_extreme) / t.entry)
            stop_dist = t.atr * _MAE_MULT
            emerg_dist = t.atr * _EMERG_MULT
            if t.adverse(adv_extreme) >= emerg_dist:           # L3 backstop (fills at the emergSL level)
                t.exit_reason = "EMERGENCY_SL"
                t.exit_price = t.entry + emerg_dist if side is PositionSide.SHORT else t.entry - emerg_dist
                del open_by_side[side]
            elif t.adverse(adv_extreme) >= stop_dist:          # L2 MAE stop (fills at the threshold)
                t.exit_reason = "MAE_STOP"
                t.exit_price = t.entry + stop_dist if side is PositionSide.SHORT else t.entry - stop_dist
                del open_by_side[side]
        # step the indicators on this close, then look for new entries
        ind.update(bar)
        if not ind.seeded or ind.atr_14 is None or ind.atr_14 <= 0:
            continue
        for side in sides:
            if side in open_by_side:
                continue
            if ind.sss_verdict(_sig_side(side)).passed:
                t = Trade(side, Decimal(str(bar.close)), Decimal(str(ind.atr_14)))
                open_by_side[side] = t
                trades.append(t)
    for side, t in open_by_side.items():     # window end: unrealized
        t.exit_reason = "OPEN_AT_END"
    return {"symbol": symbol, "regime": cls.regime.value, "sides": [s.value for s in sides], "trades": trades}


async def main():
    client = KrakenRestClient(transport=UrllibTransport())
    seen = set()
    pairs = [p for p in PAIRS if not (p in seen or seen.add(p))]
    all_trades: list[Trade] = []
    per_pair = []
    for i, sym in enumerate(pairs, 1):
        try:
            r = await backtest_pair(client, sym)
        except Exception as e:  # noqa: BLE001
            print(f"  [{i}/{len(pairs)}] {sym:<12} skip ({type(e).__name__}: {e})")
            continue
        if r is None:
            print(f"  [{i}/{len(pairs)}] {sym:<12} skip (no regime / too few bars)")
            continue
        per_pair.append(r)
        all_trades.extend(r["trades"])
        n = len(r["trades"])
        stops = sum(1 for t in r["trades"] if t.exit_reason in ("MAE_STOP", "EMERGENCY_SL"))
        print(f"  [{i}/{len(pairs)}] {sym:<12} {r['regime']:<22} entries={n:<4} stopped={stops}")
    await client.close()

    print("\n================ INDICATIVE REALIZED-EXIT BACKTEST (5m, ~2.5-day window) ================")
    n = len(all_trades)
    if n == 0:
        print("no entries generated"); return
    by_reason = {}
    for t in all_trades:
        by_reason[t.exit_reason] = by_reason.get(t.exit_reason, 0) + 1
    closed = [t for t in all_trades if t.exit_price is not None]
    wins = [t for t in closed if t.net_pl_pct() > 0]
    losses = [t for t in closed if t.net_pl_pct() <= 0]
    print(f"total entries (SSS-gated, real signal): {n}")
    print("exit-reason distribution:")
    for k in ("MAE_STOP", "EMERGENCY_SL", "OPEN_AT_END"):
        c = by_reason.get(k, 0)
        print(f"    {k:<14} {c:>5}  ({100.0*c/n:5.1f}%)")
    if closed:
        rrs = sorted(t.actual_rr() for t in closed)
        pls = sorted(t.net_pl_pct() for t in closed)
        mid = len(rrs) // 2
        med_rr = rrs[mid] if len(rrs) % 2 else (rrs[mid-1] + rrs[mid]) / 2
        med_pl = pls[mid] if len(pls) % 2 else (pls[mid-1] + pls[mid]) / 2
        tot_pl = sum(pls)
        print(f"\nCLOSED (exited within window): {len(closed)}  wins={len(wins)} losses={len(losses)} "
              f"win-rate={100.0*len(wins)/len(closed):.1f}%")
        print(f"  net R:R  (fee-inclusive, TB00774 basis): mean={sum(rrs)/len(rrs):+.3f}  median={med_rr:+.3f}")
        print(f"  net P&L %% of notional:                   mean={sum(pls)/len(pls)*100:+.4f}%  median={med_pl*100:+.4f}%")
        print(f"  summed net P&L %% over all closed trades: {tot_pl*100:+.3f}%  (avg {tot_pl/len(closed)*100:+.4f}%/trade)")
    # the reward-reachability picture: did favorable movement ever exceed the stop distance?
    print("\nFAVORABLE-vs-ADVERSE excursion over the hold (the reward-reachability proxy):")
    stop_fracs = [t.atr * _MAE_MULT / t.entry for t in all_trades]
    mfes = sorted(t.mfe_pct for t in all_trades)
    maes = sorted(t.mae_pct for t in all_trades)
    avg_stop = sum(stop_fracs) / len(stop_fracs)
    reached_1x = sum(1 for t in all_trades if t.mfe_pct >= t.atr * _MAE_MULT / t.entry)
    reached_15x = sum(1 for t in all_trades if t.mfe_pct >= Decimal("1.5") * t.atr * _MAE_MULT / t.entry)
    m = len(all_trades) // 2
    print(f"  avg L2 stop distance:        {float(avg_stop)*100:.3f}% of entry")
    print(f"  median MFE (best favorable): {float(mfes[m])*100:.3f}%   median MAE (worst adverse): {float(maes[m])*100:.3f}%")
    print(f"  entries whose MFE reached >= 1.0x the stop distance: {reached_1x}/{n} ({100.0*reached_1x/n:.1f}%)")
    print(f"  entries whose MFE reached >= 1.5x the stop distance: {reached_15x}/{n} ({100.0*reached_15x/n:.1f}%)")


if __name__ == "__main__":
    asyncio.run(main())
