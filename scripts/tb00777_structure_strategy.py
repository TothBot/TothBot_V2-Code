"""TB00777 - Bill's structure-stop / scale-out / run-the-winner strategy backtest (PAPER research).

Bill's strategy (both directions, daily-chart structure):
- Entry: the bot's existing directional trigger (we test several signals - the experiment ISOLATES the
  EXIT structure, the lever the TB00776 research said matters).
- Initial stop: just BEYOND the last daily resistance level - ABOVE it for shorts, BELOW it for longs.
  Wide by design so intraday noise cannot tag it. entry->stop distance = 1R; the position is sized to 1R,
  so a full stop-out = exactly -1R for every trade (pair-size-neutral; we can sum R directly).
- Trail: when price reaches the next daily level in our favor, ratchet the stop to just beyond it.
- At +3R favorable excursion: BANK HALF (realize +1.5R - a guaranteed >=1.5:1 winner), let the rest run.
- Runner (the other half): exit on EMA reversal OR a TIGHTER trail (a shorter-window level).
- Mirror for both directions.

Operational defn of "the last resistance level" (lookahead-free, robust): the rolling extreme of the
last L daily bars on the stop side (recent swing high for shorts / low for longs), ratcheted only in our
favor. Wide initial window L_INIT; tighter runner window L_RUN after the 3R scale-out.

Conservative intrabar order: each day we check the ADVERSE extreme (stop) BEFORE the favorable one (3R).
Rigor: nested in-sample(0-0.5)/out-of-sample(0.5-1.0), per-direction split, 3-way OOS sub-window
stability (REGIME-ROBUST), fees (taker x2 + short open) + per-day short rollover - all charged in R.
Goal: net E[R] > 0 AND realized R:R (avg win / avg loss) >= 1.5, OOS and regime-robust."""

from __future__ import annotations
import asyncio, os, sys, importlib.util

HERE=os.path.dirname(os.path.abspath(__file__))
spec=importlib.util.spec_from_file_location("ass", os.path.join(HERE,"..","operations","auto_strategy_search.py"))
A=importlib.util.module_from_spec(spec); spec.loader.exec_module(A)

L_INIT=10            # daily bars for the WIDE initial structural level (~2 weeks)
L_RUN=5             # tighter window for the runner trail after the 3R scale-out
BUF=0.001           # 0.1% beyond the level
SCALE_AT=3.0        # bank half at +3R
SCALE_FRAC=0.5      # fraction banked
USE_REVERSAL=True   # exit the runner on EMA reversal (vs let it run to the structural trail only)
MINN=30

TAKER=A.TAKER; OPEN_FEE=A.OPEN_FEE; ROLL_DAY=A.ROLL_DAY


def sim_trade(c,h,l,e12,e26,t,side):
    """Simulate one trade opened at close[t]. Returns (netR, days, hit3, fullstop, exit_off) or None."""
    n=len(c); entry=c[t]
    if side=="SHORT":
        ref=max(h[max(0,t-L_INIT+1):t+1]); stop=ref*(1+BUF); Rp=stop-entry
        if Rp<=0: return None
        target3=entry-SCALE_AT*Rp
    else:
        ref=min(l[max(0,t-L_INIT+1):t+1]); stop=ref*(1-BUF); Rp=entry-stop
        if Rp<=0: return None
        target3=entry+SCALE_AT*Rp
    Rfrac=Rp/entry
    pos=1.0; realizedR=0.0; scaled=False; hit3=0; fullstop=0; legs=2  # entry+exit legs (taker)
    j=t
    for j in range(t+1,n):
        dh=h[j]; dl=l[j]
        # 1) adverse FIRST (conservative): stop touched?
        hit = (dh>=stop) if side=="SHORT" else (dl<=stop)
        if hit:
            exitR = ((entry-stop)/Rp) if side=="SHORT" else ((stop-entry)/Rp)
            realizedR += pos*exitR
            if not scaled: fullstop=1
            break
        # 2) favorable: 3R scale-out
        fav_hit = (dl<=target3) if side=="SHORT" else (dh>=target3)
        if not scaled and fav_hit:
            realizedR += SCALE_FRAC*SCALE_AT   # bank +1.5R
            pos-=SCALE_FRAC; scaled=True; hit3=1; legs+=1   # extra exit leg
        # 3) ratchet the structural trail in our favor
        win=L_RUN if scaled else L_INIT
        if side=="SHORT":
            nref=max(h[max(t,j-win+1):j+1])*(1+BUF)
            if nref<stop: stop=nref
        else:
            nref=min(l[max(t,j-win+1):j+1])*(1-BUF)
            if nref>stop: stop=nref
        # 4) reversal exit (EMA12 vs EMA26 flips against us)
        rev = USE_REVERSAL and e12[j] is not None and ((e12[j]>e26[j]) if side=="SHORT" else (e12[j]<e26[j]))
        if rev:
            exitR = ((entry-c[j])/Rp) if side=="SHORT" else ((c[j]-entry)/Rp)
            realizedR += pos*exitR; break
    else:
        exitR = ((entry-c[n-1])/Rp) if side=="SHORT" else ((c[n-1]-entry)/Rp)
        realizedR += pos*exitR; j=n-1
    days=j-t
    fee_pct = TAKER*legs + (OPEN_FEE if side=="SHORT" else 0.0)
    roll_pct = (ROLL_DAY*days) if side=="SHORT" else 0.0
    netR = realizedR - (fee_pct/Rfrac) - (roll_pct/Rfrac)
    return netR, days, hit3, fullstop, j


def run(pairs, signame, sig, lo, hi):
    recs=[]  # (entry_frac, netR, side, hit3, fullstop)
    for p in pairs:
        c,h,l,e12,e26,n=p.c,p.h,p.l,p.e12,p.e26,p.n
        i=int(n*lo); end=int(n*hi)
        while i<end-1:
            s=sig(p,i)
            if s is None: i+=1; continue
            r=sim_trade(c,h,l,e12,e26,i,s)
            if r is None: i+=1; continue
            netR,days,hit3,fs,jend=r
            recs.append((i/n,netR,s,hit3,fs)); i=max(i+1,jend)
    return recs


def stats(recs):
    if len(recs)<MINN: return None
    rs=[r[1] for r in recs]; w=[x for x in rs if x>0]; ls=[x for x in rs if x<=0]
    avgw=sum(w)/len(w) if w else 0.0; avgl=-sum(ls)/len(ls) if ls else 1e-9
    def de(sd):
        v=[r[1] for r in recs if r[2]==sd]; return (len(v), sum(v)/len(v) if v else 0.0)
    ln,le=de("LONG"); sn,se=de("SHORT")
    thirds=[[],[],[]]
    fr0=min(r[0] for r in recs); fr1=max(r[0] for r in recs)+1e-9; span=(fr1-fr0) or 1
    for fr,nr,_,_,_ in recs: thirds[min(2,int((fr-fr0)/span*3))].append(nr)
    tc=[t for t in thirds if len(t)>=5]; tpos=sum(1 for t in tc if sum(t)>0)
    return {"n":len(rs),"ER":sum(rs)/len(rs),"win":100*len(w)/len(rs),"rr":avgw/avgl if avgl>0 else 0.0,
            "hit3":100*sum(r[3] for r in recs)/len(rs),"fstop":100*sum(r[4] for r in recs)/len(rs),
            "ln":ln,"le":le,"sn":sn,"se":se,"tpos":tpos,"tc":len(tc)}


def one(pairs,sn):
    sig=A.SIGNALS[sn]
    a=stats(run(pairs,sn,sig,0.0,1.0)); o=stats(run(pairs,sn,sig,0.5,1.0)); ii=stats(run(pairs,sn,sig,0.0,0.5))
    if not a: return f"{sn:17s} | too few trades"
    if o and ii:
        twoside=o["ln"]>=5 and o["sn"]>=5
        dok=(min(o['le'],o['se'])>0 and min(o['le'],o['se'])>=0.2*max(o['le'],o['se'])) if twoside else True
        tok=o["tpos"]>=max(2,o["tc"]-1) if o["tc"]>=2 else False
        goal=(ii["ER"]>0 and ii["rr"]>=1.5 and o["ER"]>0 and o["rr"]>=1.5)
        verdict=("ROBUST-GOAL" if (goal and dok and tok) else "OOS-goal(fragile)" if goal else "+OOS" if o["ER"]>0 else "fails")
        return (f"{sn:17s} | {a['n']:<4d} {a['ER']:+.3f} {a['win']:4.0f} {a['rr']:4.2f} {a['hit3']:5.0f} {a['fstop']:5.0f} | "
                f"{o['n']:<4d}{o['ER']:+.3f} {o['win']:4.0f} {o['rr']:4.2f} +{o['tpos']}/{o['tc']} {verdict}")
    return f"{sn:17s} | {a['n']:<4d} {a['ER']:+.3f} {a['win']:4.0f} {a['rr']:4.2f} {a['hit3']:5.0f} {a['fstop']:5.0f} | OOS too few"

async def main():
    global SCALE_AT, USE_REVERSAL
    d=await A.fetch_kraken(1440)   # daily, ~720 bars = ~2 years (fetched ONCE; swept below)
    pairs=A.to_pairs(d)
    sigs=["momentum","donchian","bb_break","rsi_trend"]
    for sa,rev in [(3.0,True),(2.0,True),(1.5,True),(2.0,False),(1.5,False)]:
        SCALE_AT=sa; USE_REVERSAL=rev
        print(f"\n=== scale@{sa}R  reversal_exit={rev}  (L_init={L_INIT} L_run={L_RUN}) | pairs={len(pairs)} ===")
        print("signal            | n    ER(R)  win%  RR   hit3%  fstop% | OOS: n   ER    win  RR  thirds VERDICT")
        for sn in sigs: print(one(pairs,sn))
    print("\nER(R)=net expectancy/trade in R after fees+rollover. RR=avg win/avg loss. hit3=% reaching +3R. "
          "fstop=% full-stopped pre-scale. GOAL=ER>0 AND RR>=1.5 in BOTH halves + regime-robust.")

if __name__=="__main__":
    asyncio.run(main())
