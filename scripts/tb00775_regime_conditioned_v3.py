"""TB00775 v3 - pressure-test the (wide-stop + fast-TP) method on BOTH sides, then the IMPROVEMENT:
condition entries on the BTC MACRO regime ("trade with the tide") to fix the dead long side + build a
regime-ROBUST organism (makes money in bull AND bear), not a bear-only short bot.

CHAIN OF REASONING (no stove-piping - this targets the organism's profitability, not one knob):
  v2 showed: tight stop robustly loses; wide-stop+fast-TP is capital-efficient but SHORT-ONLY in a
  2-yr BEARISH crypto sample; longs lose everywhere. HYPOTHESIS: longs lose because they FIGHT the
  bear tide, not because longs are bad. TEST: classify the BTC/USD daily regime at each entry
  (BULL=TRENDING_POS*, BEAR=TRENDING_NEG*, NEUTRAL=NON_DIR*) and measure per-side expectancy WITH vs
  AGAINST the tide. If longs pay in BULL-BTC and shorts in BEAR-BTC, the elegant rule is: go with the
  macro tide, side-specific wide-stop + fast-TP exit. That is two engines = profit in any regime.

RIGOR (all OUT-OF-SAMPLE walk-forward + block-bootstrap; same hardening as v2):
  - bucket admission (positive median rev) learned on each pair's TRAIN half; everything measured on TEST.
  - E/DAY (capital-time) is the decision metric; margin rollover netted for shorts; fees netted.
  - block bootstrap (resample PAIRS, 500x) -> 5/50/95 CI; "significant" = CI excludes 0.
  - per-side grids + the combined with-tide rule, each with a CI, vs the current tight-stop baseline.

Reuses production units; read-only daily Kraken OHLC; throwaway tool; seeded LCG bootstrap."""

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
_OPEN = float(registry.value("margin_open_fee_pct"))
_ROLL_DAY = float(registry.value("margin_rollover_fee_pct")) * 6.0

PAIRS = [
    "AVAX/USD", "AAVE/USD", "BCH/USD", "BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "ADA/USD",
    "DOT/USD", "LINK/USD", "UNI/USD", "LTC/USD", "DOGE/USD", "APT/USD", "ARKM/USD", "TIA/USD",
    "HBAR/USD", "OP/USD", "RUNE/USD", "WLD/USD", "PENDLE/USD", "INJ/USD", "SUI/USD", "NEAR/USD",
    "ATOM/USD", "FIL/USD", "TAO/USD", "RENDER/USD", "SEI/USD", "GRT/USD", "ALGO/USD", "FET/USD",
]
KS = [0.5, 1.0, 2.0, 3.0, 5.0, 8.0, 1e9]
MS = [1.0, 1.5, 2.0, 3.0, 1e9]   # 1e9 = no TP (pure run-to-reversal)


class UrllibTransport:
    async def get(self, url, params):
        q = urllib.parse.urlencode({k: str(v) for k, v in params.items()})
        def _do():
            req = urllib.request.Request(url + ("?" + q if q else ""), headers={"User-Agent": "tb775v3"})
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read().decode("utf-8"))
        return await asyncio.get_event_loop().run_in_executor(None, _do)
    async def post(self, *a, **k): raise RuntimeError("read-only")
    async def close(self): return None


def mkt_bucket(regime_value: str) -> str:
    if "POS" in regime_value: return "BULL"
    if "NEG" in regime_value: return "BEAR"
    return "NEUTRAL"


class Ent:
    __slots__ = ("sym","side","half","atrf","rev","hold","stop_off","tp_off","day","tide")
    def __init__(s, sym, side, half, atrf, rev, hold, stop_off, tp_off, day):
        s.sym=sym; s.side=side; s.half=half; s.atrf=atrf; s.rev=rev; s.hold=hold
        s.stop_off=stop_off; s.tp_off=tp_off; s.day=day; s.tide=None


def replay(sym, bars):
    classes = rolling_classifications(sym, bars)
    highs=[float(b.high) for b in bars]; lows=[float(b.low) for b in bars]; closes=[float(b.close) for b in bars]
    times=[int(getattr(b,"time",0)) for b in bars]
    atr = atr_14_series([Decimal(str(h)) for h in highs],[Decimal(str(l)) for l in lows],
                        [Decimal(str(c)) for c in closes],14)
    n=len(bars); half=n//2; out=[]
    for i in range(n-1):
        ci=classes[i]
        if ci is None or i>=len(atr) or atr[i] is None: continue
        entry=closes[i]
        if entry==0: continue
        atrf=float(atr[i])/entry
        if atrf<=0: continue
        for side in permitted_sides(ci.regime):
            pos=Position(symbol=sym,side=side,qty=Decimal(0),avg_entry_price=Decimal(str(entry)))
            so=[-1]*len(KS); to=[-1]*len(MS); madv=0.0; mfav=0.0; revd=None
            for off,j in enumerate(range(i+1,n),start=1):
                advpx=highs[j] if side is PositionSide.SHORT else lows[j]
                favpx=lows[j] if side is PositionSide.SHORT else highs[j]
                adv=((advpx-entry) if side is PositionSide.SHORT else (entry-advpx))/entry
                fav=((entry-favpx) if side is PositionSide.SHORT else (favpx-entry))/entry
                if adv>madv: madv=adv
                if fav>mfav: mfav=fav
                for ki,k in enumerate(KS):
                    if so[ki]==-1 and madv>=k*atrf: so[ki]=off
                for mi,m in enumerate(MS):
                    if to[mi]==-1 and mfav>=m*atrf: to[mi]=off
                cj=classes[j]
                if cj is not None and (detect_daily_regime_downgrade(pos,cj) or detect_htf_regime_reversal(pos,cj.ema20,cj.ema50)):
                    rev=((entry-closes[j]) if side is PositionSide.SHORT else (closes[j]-entry))/entry
                    out.append(Ent(sym,side,0 if i<half else 1,atrf,rev,off,so,to,times[i]//86400))
                    revd=off; break
    return out


def pnl_hold(e, ki, mi):
    so=e.stop_off[ki]; to=e.tp_off[mi]; k=KS[ki]; m=MS[mi]
    fee=_TAKER+_TAKER+(_OPEN if e.side is PositionSide.SHORT else 0.0)
    cands=[(e.hold, e.rev, 2)]
    if so!=-1: cands.append((so,-(k*e.atrf),0))
    if to!=-1 and m<1e8: cands.append((to, m*e.atrf, 1))
    off,gross,_=min(cands,key=lambda c:(c[0],c[2]))
    roll=(_ROLL_DAY*off) if e.side is PositionSide.SHORT else 0.0
    return gross-fee-roll, off


def agg(entries, ki, mi):
    if not entries: return None
    rows=[pnl_hold(e,ki,mi) for e in entries]; pls=[r[0] for r in rows]; hold=[r[1] for r in rows]
    n=len(pls); tot=sum(pls); th=sum(hold) or 1; w=sum(1 for p in pls if p>0)
    return {"n":n,"win":100.0*w/n,"E_trade":tot/n*100,"E_day":tot/th*100,"hold":th/n,
            "median":sorted(pls)[n//2]*100}


def boot(by_pair, ki, mi, iters=500, seed=999):
    pp=list(by_pair); m=len(pp)
    if not m: return (0,0,0)
    s=seed; vals=[]
    for _ in range(iters):
        tp=0.0; th=0.0
        for _ in range(m):
            s=(1103515245*s+12345)&0x7FFFFFFF; pr=pp[s%m]
            for e in by_pair[pr]:
                pl,h=pnl_hold(e,ki,mi); tp+=pl; th+=h
        vals.append(tp/(th or 1)*100)
    vals.sort()
    return (vals[int(.05*iters)], vals[iters//2], vals[int(.95*iters)])


def gated_oos(entries, min_train=8):
    tr={}
    for e in entries:
        if e.half==0: tr.setdefault((e.sym,e.side),[]).append(e.rev)
    adm={b for b,r in tr.items() if len(r)>=min_train and sorted(r)[len(r)//2]>0}
    return [e for e in entries if e.half==1 and (e.sym,e.side) in adm]


async def main():
    client=KrakenRestClient(transport=UrllibTransport())
    seen=set(); pairs=[p for p in PAIRS if not (p in seen or seen.add(p))]
    allE=[]
    for i,sym in enumerate(pairs,1):
        try:
            resp=await client.get_ohlc_data(sym,1440); es=replay(sym,resp.committed)
        except Exception as ex:
            print(f"  [{i}/{len(pairs)}] {sym:<11} skip ({type(ex).__name__})"); continue
        allE.extend(es); print(f"  [{i}/{len(pairs)}] {sym:<11} entries={len(es)}")
    # BTC macro-regime by day (the tide)
    btc=await client.get_ohlc_data("BTC/USD",1440); await client.close()
    bclasses=rolling_classifications("BTC/USD",btc.committed)
    bmap={int(getattr(b,"time",0))//86400: mkt_bucket(c.regime.value)
          for b,c in zip(btc.committed,bclasses) if c is not None}
    for e in allE:
        e.tide=bmap.get(e.day)

    oos=gated_oos(allE)
    print(f"\n=== OOS gated entries: {len(oos)}  (LONG {sum(1 for e in oos if e.side is PositionSide.LONG)} / "
          f"SHORT {sum(1 for e in oos if e.side is PositionSide.SHORT)}) ===")

    # ---- PRESSURE TEST: per-side OOS exit grid (E/DAY%), find each side's best ----
    for side,lbl in ((PositionSide.LONG,"LONG"),(PositionSide.SHORT,"SHORT")):
        sub=[e for e in oos if e.side is side]
        print(f"\n#### {lbl} OOS exit grid - E/DAY% (E/trade%, hold d) ; rows=stop k*ATR, cols=TP m*ATR")
        print("  stop  " + "".join(f"{('TP'+str(m) if m<1e8 else 'noTP'):>14}" for m in MS))
        for ki,k in enumerate(KS):
            cells=[]
            for mi,m in enumerate(MS):
                a=agg(sub,ki,mi)
                cells.append(f"{a['E_day']:>5.3f}({a['E_trade']:.1f},{a['hold']:.0f})" if a else "-")
            print(f"  {('none' if k>1e8 else f'{k:.0f}x'):>5} "+"".join(f"{c:>14}" for c in cells))

    # ---- IMPROVEMENT: market-tide conditioning (fixed conservative exit: stop 5x + TP 1.5x) ----
    KI=KS.index(5.0); MI=MS.index(1.5)
    print(f"\n=== TIDE CONDITIONING (exit: stop 5x ATR + TP 1.5x ATR) - OOS E/trade%, E/day%, win%, n ===")
    for side,lbl in ((PositionSide.LONG,"LONG"),(PositionSide.SHORT,"SHORT")):
        print(f"  {lbl}:")
        for tide in ("BULL","NEUTRAL","BEAR"):
            sub=[e for e in oos if e.side is side and e.tide==tide]
            a=agg(sub,KI,MI)
            if a: print(f"     BTC {tide:<8} E/trade {a['E_trade']:+7.2f}%  E/day {a['E_day']:+7.4f}%  win {a['win']:4.0f}%  n={a['n']}")

    # ---- THE COMBINED WITH-TIDE RULE: long only in non-BEAR BTC, short only in non-BULL BTC ----
    def with_tide(e):
        if e.side is PositionSide.LONG: return e.tide in ("BULL","NEUTRAL")
        return e.tide in ("BEAR","NEUTRAL")
    rule=[e for e in oos if with_tide(e)]
    rule_bp={}
    for e in rule: rule_bp.setdefault(e.sym,[]).append(e)
    a_rule=agg(rule,KI,MI); lo,mid,hi=boot(rule_bp,KI,MI)
    a_all=agg(oos,KI,MI); all_bp={}
    for e in oos: all_bp.setdefault(e.sym,[]).append(e)
    alo,amid,ahi=boot(all_bp,KI,MI)
    # current-equivalent: tight stop (0.5x), no TP, ALL gated entries
    KT=KS.index(0.5); MT=MS.index(1e9); a_cur=agg(oos,KT,MT); clo,cmid,chi=boot(all_bp,KT,MT)

    print(f"\n=== PROGRESSION (OOS, block-bootstrap CI on E/DAY%) ===")
    print(f"  (1) CURRENT  tight 0.5x stop, no TP, all gated : E/trade {a_cur['E_trade']:+.2f}% E/day {a_cur['E_day']:+.4f}%  "
          f"win {a_cur['win']:.0f}%  CI[{clo:+.4f}..{chi:+.4f}]{'  SIG' if (clo>0 or chi<0) else ''}")
    print(f"  (2) METHOD   wide 5x stop + TP 1.5x, all gated : E/trade {a_all['E_trade']:+.2f}% E/day {a_all['E_day']:+.4f}%  "
          f"win {a_all['win']:.0f}%  CI[{alo:+.4f}..{ahi:+.4f}]{'  SIG' if (alo>0 or ahi<0) else ''}")
    print(f"  (3) +TIDE    method + with-macro-tide entries  : E/trade {a_rule['E_trade']:+.2f}% E/day {a_rule['E_day']:+.4f}%  "
          f"win {a_rule['win']:.0f}%  n={a_rule['n']}  CI[{lo:+.4f}..{hi:+.4f}]{'  SIG (CI excludes 0)' if (lo>0 or hi<0) else ''}")


if __name__ == "__main__":
    asyncio.run(main())
