"""TB00775 - STRATEGY-COMBINATION SEARCH: hunt the tool space for a config that clears 1:1.5 R:R net OOS.

Bill's directive: TothBot CAN be profitable; the problem is the COMBINATION of tools, not the tools. So
open the whole box - don't restrict to the current RSI/EMA/volume -> run-to-reversal config. This engine
composes ENTRY signals x FILTERS x EXITS over 2yr daily Kraken data (32 pairs), nets fees+rollover, and
ranks by OUT-OF-SAMPLE expectancy subject to realized R:R >= 1.5, with block-bootstrap on the winners +
a BREADTH check (do MANY combos work = robust, or one = overfit luck).

EXPANDED TOOL LIBRARY (none of this is in the current organism except s_rsi_trend / reversal):
  ENTRY  : rsi_meanrev, rsi_trend(current SSS-ish), ema_cross, macd, bb_break, bb_revert, donchian,
           momentum, rs_vs_btc (relative strength).
  FILTER : none, trend100 (close vs SMA100), vol (ATR%ile band), btc_tide (macro regime).
  EXIT   : stop k*ATR in {2,3,5} x target m*ATR in {1.5,2,3} OR run-to-reversal ; MAXHOLD time-stop.

RIGOR: walk-forward (each pair's first half = TRAIN, second = TEST; the leaderboard is TEST-only);
realized R:R = avgWin/avgLoss; block-bootstrap (resample pairs) CI on the top combos; report how many of
the N combos are OOS-positive. Daily testbed = the SWING version of the tools (most history/power); the
winning PRINCIPLES port to the 5m system as the #1-B corpus fills. Read-only public Kraken; throwaway."""

from __future__ import annotations

import asyncio
import json
import math
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, ".")

from tothbot.config import registry  # noqa: E402
from tothbot.config.fees import FEE_TAKER_PCT  # noqa: E402
from tothbot.rest.client import KrakenRestClient  # noqa: E402

TAKER = float(FEE_TAKER_PCT)
ROLL_DAY = float(registry.value("margin_rollover_fee_pct")) * 6.0
OPEN_FEE = float(registry.value("margin_open_fee_pct"))
MAXHOLD = 60          # cap the forward walk = a max-hold / time-stop tool (days)
KS = [2.0, 3.0, 5.0]                 # stop multipliers (x daily ATR)
MS = [1.5, 2.0, 3.0, None]           # target multipliers; None = run-to-reversal
TRAILS = [1.5, 2.0, 3.0]             # CHANDELIER trailing-stop distances (x daily ATR) - let winners run

PAIRS = ["AVAX/USD","AAVE/USD","BCH/USD","BTC/USD","ETH/USD","SOL/USD","XRP/USD","ADA/USD","DOT/USD",
    "LINK/USD","UNI/USD","LTC/USD","DOGE/USD","APT/USD","ARKM/USD","TIA/USD","HBAR/USD","OP/USD",
    "RUNE/USD","WLD/USD","PENDLE/USD","INJ/USD","SUI/USD","NEAR/USD","ATOM/USD","FIL/USD","TAO/USD",
    "RENDER/USD","SEI/USD","GRT/USD","ALGO/USD","FET/USD"]


class T:
    async def get(self, url, params):
        q = urllib.parse.urlencode({k: str(v) for k, v in params.items()})
        def _do():
            req = urllib.request.Request(url+("?"+q if q else ""), headers={"User-Agent":"tb775search"})
            with urllib.request.urlopen(req, timeout=20) as r: return json.loads(r.read().decode())
        return await asyncio.get_event_loop().run_in_executor(None, _do)
    async def post(self,*a,**k): raise RuntimeError("ro")
    async def close(self): return None


# ---------------- indicators (float; research tool, not the Decimal core) ----------------
def ema(xs, n):
    a = 2.0/(n+1); out = [None]*len(xs); e = None
    for i, x in enumerate(xs):
        e = x if e is None else (x-e)*a+e
        out[i] = e
    return out

def rsi(xs, n=14):
    out=[None]*len(xs); ag=al=None
    for i in range(1,len(xs)):
        d=xs[i]-xs[i-1]; g=max(0.0,d); l=max(0.0,-d)
        if ag is None:
            if i>=n:
                ag=sum(max(0.0,xs[k]-xs[k-1]) for k in range(i-n+1,i+1))/n
                al=sum(max(0.0,xs[k-1]-xs[k]) for k in range(i-n+1,i+1))/n
        else:
            ag=(ag*(n-1)+g)/n; al=(al*(n-1)+l)/n
        if ag is not None:
            out[i]=100.0 if al==0 else 100.0-100.0/(1+ag/al)
    return out

def atr(h,l,c,n=14):
    out=[None]*len(c); tr=[]; a=None
    for i in range(len(c)):
        t=h[i]-l[i] if i==0 else max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1]))
        tr.append(t)
        if i==n: a=sum(tr[1:n+1])/n; out[i]=a
        elif i>n: a=(a*(n-1)+t)/n; out[i]=a
    return out

def sma(xs,n):
    out=[None]*len(xs); s=0.0
    for i,x in enumerate(xs):
        s+=x
        if i>=n: s-=xs[i-n]
        if i>=n-1: out[i]=s/n
    return out

def rstd(xs,n):
    out=[None]*len(xs)
    for i in range(n-1,len(xs)):
        w=xs[i-n+1:i+1]; m=sum(w)/n; out[i]=math.sqrt(sum((x-m)**2 for x in w)/n)
    return out

def roll_max(xs,n):
    return [None if i<n else max(xs[i-n:i]) for i in range(len(xs))]
def roll_min(xs,n):
    return [None if i<n else min(xs[i-n:i]) for i in range(len(xs))]


class Pair:
    pass

def build_pair(sym, bars, btc_ret):
    c=[float(b.close) for b in bars]; h=[float(b.high) for b in bars]; l=[float(b.low) for b in bars]
    n=len(c)
    if n<160: return None
    p=Pair(); p.sym=sym; p.c=c; p.h=h; p.l=l; p.n=n; p.half=n//2
    p.rsi=rsi(c); p.e12=ema(c,12); p.e26=ema(c,26)
    macd=[(p.e12[i]-p.e26[i]) if p.e12[i] is not None else None for i in range(n)]
    p.macd=macd; p.macdsig=ema([m if m is not None else 0.0 for m in macd],9)
    p.sma20=sma(c,20); p.std20=rstd(c,20); p.atr=atr(h,l,c,14); p.sma100=sma(c,100)
    p.dchi=roll_max(h,20); p.dclo=roll_min(l,20)
    p.mom=[None if i<20 else c[i]/c[i-20]-1 for i in range(n)]
    p.ret=[None if i<1 else c[i]/c[i-1]-1 for i in range(n)]
    p.btc_ret=btc_ret   # aligned-by-index proxy: BTC 20d momentum sign at this bar (the tide)
    # ATR percentile (rolling 100) for the vol filter
    p.atrpct=[None]*n
    for i in range(n):
        if p.atr[i] is None: continue
        w=[p.atr[k] for k in range(max(0,i-100),i+1) if p.atr[k] is not None]
        if len(w)>20: p.atrpct[i]=sum(1 for x in w if x<=p.atr[i])/len(w)
    return p


# ---------------- entry signals: (pair,i) -> "LONG"/"SHORT"/None ----------------
def s_rsi_meanrev(p,i):
    r=p.rsi[i]
    if r is None: return None
    return "LONG" if r<30 else "SHORT" if r>70 else None
def s_rsi_trend(p,i):
    r=p.rsi[i]; e9=p.e12[i]; e21=p.e26[i]
    if None in (r,e9,e21): return None
    if 45<r<70 and e9>e21: return "LONG"
    if 30<r<55 and e9<e21: return "SHORT"
    return None
def s_ema_cross(p,i):
    if p.e12[i] is None or p.e26[i] is None: return None
    return "LONG" if p.e12[i]>p.e26[i] else "SHORT"
def s_macd(p,i):
    if p.macd[i] is None: return None
    return "LONG" if p.macd[i]>p.macdsig[i] else "SHORT"
def s_bb_break(p,i):
    if p.sma20[i] is None or p.std20[i] is None: return None
    u=p.sma20[i]+2*p.std20[i]; d=p.sma20[i]-2*p.std20[i]
    return "LONG" if p.c[i]>u else "SHORT" if p.c[i]<d else None
def s_bb_revert(p,i):
    if p.sma20[i] is None or p.std20[i] is None: return None
    u=p.sma20[i]+2*p.std20[i]; d=p.sma20[i]-2*p.std20[i]
    return "LONG" if p.c[i]<d else "SHORT" if p.c[i]>u else None
def s_donchian(p,i):
    if p.dchi[i] is None: return None
    return "LONG" if p.c[i]>=p.dchi[i] else "SHORT" if p.c[i]<=p.dclo[i] else None
def s_momentum(p,i):
    if p.mom[i] is None: return None
    return "LONG" if p.mom[i]>0.05 else "SHORT" if p.mom[i]<-0.05 else None
def s_rs_btc(p,i):
    if p.mom[i] is None or p.btc_ret[i] is None: return None
    rel=p.mom[i]-p.btc_ret[i]
    return "LONG" if rel>0.05 else "SHORT" if rel<-0.05 else None

SIGNALS={"rsi_meanrev":s_rsi_meanrev,"rsi_trend":s_rsi_trend,"ema_cross":s_ema_cross,"macd":s_macd,
    "bb_break":s_bb_break,"bb_revert":s_bb_revert,"donchian":s_donchian,"momentum":s_momentum,"rs_btc":s_rs_btc}

# ---------------- filters: (pair,i,side) -> bool keep ----------------
def f_none(p,i,s): return True
def f_trend100(p,i,s):
    if p.sma100[i] is None: return True
    return (p.c[i]>p.sma100[i]) if s=="LONG" else (p.c[i]<p.sma100[i])
def f_vol(p,i,s):
    return p.atrpct[i] is not None and 0.2<=p.atrpct[i]<=0.85
def f_btc_tide(p,i,s):
    b=p.btc_ret[i]
    if b is None: return True
    return (b>-0.02) if s=="LONG" else (b<0.02)
FILTERS={"none":f_none,"trend100":f_trend100,"vol":f_vol,"btc_tide":f_btc_tide}


def precompute_exits(p):
    """per (i, side): atrf, stop_off[KS], tp_off[MS_targets], rev_exc(signed at reversal-or-cap), hold."""
    ex={}
    classes_ok = True
    for i in range(p.n-1):
        if p.atr[i] is None: continue
        e=p.c[i]; atrf=p.atr[i]/e
        if atrf<=0: continue
        for side in ("LONG","SHORT"):
            so=[-1]*len(KS); to=[-1]*len(MS); madv=mfav=0.0; revexc=None; hold=None
            peak=0.0; tro=[-1]*len(TRAILS); trx=[None]*len(TRAILS)   # chandelier trailing
            jmax=min(p.n, i+1+MAXHOLD)
            for off,j in enumerate(range(i+1,jmax),start=1):
                advpx=p.h[j] if side=="SHORT" else p.l[j]
                favpx=p.l[j] if side=="SHORT" else p.h[j]
                adv=((advpx-e) if side=="SHORT" else (e-advpx))/e
                fav=((e-favpx) if side=="SHORT" else (favpx-e))/e
                if adv>madv: madv=adv
                if fav>mfav: mfav=fav
                for ki,k in enumerate(KS):
                    if so[ki]==-1 and madv>=k*atrf: so[ki]=off
                for mi,m in enumerate(MS):
                    if m is not None and to[mi]==-1 and mfav>=m*atrf: to[mi]=off
                # CHANDELIER trailing: stop = peak_favorable - trail*ATR; check vs THIS bar's worst
                # favorable (-adv) using the PRIOR peak (pessimistic intra-bar order), then raise peak.
                cur_worst = -adv
                for ti,tr in enumerate(TRAILS):
                    if tro[ti]==-1 and cur_worst <= (peak - tr*atrf):
                        tro[ti]=off; trx[ti]=peak - tr*atrf       # fill at the trailing-stop level
                if fav>peak: peak=fav
                # reversal proxy on daily: EMA12/26 cross against the position (a generic trend-exit)
                if revexc is None and p.e12[j] is not None and ((side=="LONG" and p.e12[j]<p.e26[j]) or (side=="SHORT" and p.e12[j]>p.e26[j])):
                    revexc=((e-p.c[j]) if side=="SHORT" else (p.c[j]-e))/e; hold=off
            if revexc is None:  # timed out at cap
                j=jmax-1; revexc=((e-p.c[j]) if side=="SHORT" else (p.c[j]-e))/e; hold=jmax-1-i
            for ti,tr in enumerate(TRAILS):
                if tro[ti]==-1:  # trail never hit -> exit at cap with the final excursion
                    j=jmax-1; tro[ti]=jmax-1-i; trx[ti]=((e-p.c[j]) if side=="SHORT" else (p.c[j]-e))/e
            ex[(i,side)]=(atrf,so,to,revexc,hold,tro,trx)
    return ex


def _net(gross, off, side):
    fee=TAKER+TAKER+(OPEN_FEE if side=="SHORT" else 0.0)
    roll=(ROLL_DAY*off) if side=="SHORT" else 0.0
    return gross-fee-roll, off

def trade_pnl(entry, spec, side):
    atrf,so,to,revexc,hold,tro,trx=entry
    if spec[0]=="tr":                       # chandelier trailing exit
        ti=spec[1]; return _net(trx[ti], tro[ti], side)
    ki,mi=spec[1],spec[2]; k=KS[ki]; m=MS[mi]   # stop + target + reversal
    cands=[(hold,revexc,2)]
    if so[ki]!=-1: cands.append((so[ki],-(k*atrf),0))
    if m is not None and to[mi]!=-1: cands.append((to[mi],m*atrf,1))
    off,gross,_=min(cands,key=lambda c:(c[0],c[2]))
    return _net(gross, off, side)


def run_combo(pairs, exits, sig, filt, spec, lo_f, hi_f, min_trades=40):
    # NON-OVERLAPPING position management over the bar-fraction window [lo_f, hi_f) of EACH pair.
    pls=[]; holds=[]; bypair={}
    for p in pairs:
        ex=exits[p.sym]; i=int(p.n*lo_f); end=int(p.n*hi_f)
        while i < end-1:
            side=sig(p,i)
            if side is None or (i,side) not in ex or not filt(p,i,side):
                i+=1; continue
            pl,off=trade_pnl(ex[(i,side)],spec,side)
            pls.append(pl); holds.append(off); bypair.setdefault(p.sym,[]).append(pl)
            i+=max(1,off)
    if len(pls)<min_trades: return None
    w=[x for x in pls if x>0]; ls=[x for x in pls if x<=0]
    avgw=sum(w)/len(w) if w else 0.0; avgl=-sum(ls)/len(ls) if ls else 1e-9
    rr=avgw/avgl if avgl>0 else 0.0
    return {"n":len(pls),"win":100*len(w)/len(pls),"E":sum(pls)/len(pls)*100,
            "Eday":sum(pls)/(sum(holds) or 1)*100,"rr":rr,"npairs":len(bypair),"bypair":bypair}


def boot(bypair, iters=400, seed=7):
    pp=list(bypair); m=len(pp)
    if m<2: return (0.0,0.0,0.0,m)
    s=seed; vals=[]
    for _ in range(iters):
        tot=0.0; c=0
        for _ in range(m):
            s=(1103515245*s+12345)&0x7FFFFFFF; pr=pp[s%m]
            tot+=sum(bypair[pr]); c+=len(bypair[pr])
        vals.append(tot/(c or 1)*100)
    vals.sort(); return (vals[int(.05*iters)],vals[iters//2],vals[int(.95*iters)],m)


async def main():
    cl=KrakenRestClient(transport=T()); seen=set(); pl=[p for p in PAIRS if not(p in seen or seen.add(p))]
    raw={}
    for i,sym in enumerate(pl,1):
        try: raw[sym]=(await cl.get_ohlc_data(sym,1440)).committed
        except Exception as e: print(f"  skip {sym} ({type(e).__name__})")
    await cl.close()
    btc=raw.get("BTC/USD")
    btc_mom={int(getattr(b,"time",0))//86400: (float(btc[i].close)/float(btc[i-20].close)-1 if i>=20 else None)
             for i,b in enumerate(btc)} if btc else {}
    pairs=[]
    for sym,bars in raw.items():
        days=[int(getattr(b,"time",0))//86400 for b in bars]
        btc_ret=[btc_mom.get(d) for d in days]
        p=build_pair(sym,bars,btc_ret)
        if p: pairs.append(p)
    print(f"built {len(pairs)} pairs; precomputing exit tables (~O(n*MAXHOLD))...")
    exits={p.sym:precompute_exits(p) for p in pairs}

    exit_specs=[("st",ki,mi,f"{KS[ki]:.0f}x/{'rev' if MS[mi] is None else f'{MS[mi]:.1f}x'}")
                for ki in range(len(KS)) for mi in range(len(MS))]
    exit_specs+=[("tr",ti,None,f"trail{TRAILS[ti]:.1f}x") for ti in range(len(TRAILS))]

    # NESTED VALIDATION: SELECT goal-meeting combos on the IN-SAMPLE half (0-50%); then report those
    # SAME combos on the truly UNTOUCHED OOS half (50-100%). This kills the select-on-test bias.
    rows=[]
    for sname,sig in SIGNALS.items():
        for fname,filt in FILTERS.items():
            for spec in exit_specs:
                isr=run_combo(pairs,exits,sig,filt,(spec[0],spec[1],spec[2]),0.0,0.5)
                oos=run_combo(pairs,exits,sig,filt,(spec[0],spec[1],spec[2]),0.5,1.0)
                if isr and oos: rows.append((sname,fname,spec[3],isr,oos))
    print(f"\nsearched {len(rows)} combos with trades in BOTH halves")
    is_goal=[r for r in rows if r[3]["E"]>0 and r[3]["rr"]>=1.5]
    print(f"  IN-SAMPLE met the goal (E>0 AND R:R>=1.5): {len(is_goal)}/{len(rows)}")
    survivors=[r for r in is_goal if r[4]["E"]>0 and r[4]["rr"]>=1.5]
    print(f"  ...of those, SURVIVED on the untouched OOS half (E>0 AND R:R>=1.5): {len(survivors)}  <== the honest count")

    show=sorted(is_goal,key=lambda r:r[4]["E"],reverse=True)[:18]
    print(f"\n=== IN-SAMPLE goal-meeters, ranked by OOS E/trade (IS -> OOS) ===")
    print(f"{'signal':>12} {'filter':>8} {'exit':>10} | {'IS n':>5} {'IS rr':>5} {'IS E%':>7} | {'OOSn':>5} {'OOSnp':>5} {'OOSwin':>6} {'OOSrr':>6} {'OOS E%':>7} {'OOSE/d':>7}")
    for sname,fname,xlbl,a,o in show:
        flag=" <-SURV" if (o["E"]>0 and o["rr"]>=1.5) else ""
        print(f"{sname:>12} {fname:>8} {xlbl:>10} | {a['n']:>5} {a['rr']:>5.2f} {a['E']:>6.3f}% | "
              f"{o['n']:>5} {o['npairs']:>5} {o['win']:>5.0f}% {o['rr']:>6.2f} {o['E']:>6.3f}% {o['Eday']:>6.3f}%{flag}")
    print(f"\n=== bootstrap (resample pairs, 400x) on OOS of the TOP survivors - E/trade% [5/50/95] (npairs) ===")
    for sname,fname,xlbl,a,o in [r for r in show if (r[4]['E']>0 and r[4]['rr']>=1.5)][:6]:
        lo,mid,hi,m=boot(o["bypair"])
        s="  SIG (CI>0)" if lo>0 else "  (CI spans 0)"
        print(f"  {sname}/{fname}/{xlbl}: [{lo:+.3f} .. {mid:+.3f} .. {hi:+.3f}]%  npairs={m}{s}")


if __name__=="__main__":
    asyncio.run(main())
