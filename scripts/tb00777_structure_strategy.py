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


def sim_trade(c,h,l,rf,rs,t,side):
    """Simulate one trade opened at close[t]. rf/rs = the reversal-exit MA pair (fast/slow). Returns
    (netR, days, hit3, fullstop, exit_off) or None."""
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
        # 4) reversal exit (the chosen MA pair flips against us)
        rev = USE_REVERSAL and rf[j] is not None and rs[j] is not None and ((rf[j]>rs[j]) if side=="SHORT" else (rf[j]<rs[j]))
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


# ---------------- STRONGER TREND PREDICTORS (the TB00777 entry hunt) ----------------
def _adx(h,l,c,n=14):
    N=len(c); pdm=[0.0]*N; mdm=[0.0]*N; tr=[0.0]*N
    for i in range(1,N):
        up=h[i]-h[i-1]; dn=l[i-1]-l[i]
        pdm[i]=up if (up>dn and up>0) else 0.0
        mdm[i]=dn if (dn>up and dn>0) else 0.0
        tr[i]=max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1]))
    def wil(x):
        o=[None]*N; s=None
        for i in range(1,N):
            if i<n: continue
            s=sum(x[1:n+1]) if i==n else (s-s/n+x[i]); o[i]=s
        return o
    st,sp,sm=wil(tr),wil(pdm),wil(mdm)
    pdi=[None]*N; mdi=[None]*N; dx=[None]*N
    for i in range(N):
        if st[i] and st[i]>0:
            pdi[i]=100*sp[i]/st[i]; mdi[i]=100*sm[i]/st[i]; s=pdi[i]+mdi[i]
            dx[i]=100*abs(pdi[i]-mdi[i])/s if s>0 else 0.0
    adxv=[None]*N; a=None; cnt=0; acc=0.0
    for i in range(N):
        if dx[i] is None: continue
        cnt+=1
        if cnt<=n: acc+=dx[i]
        if cnt==n: a=acc/n; adxv[i]=a
        elif cnt>n: a=(a*(n-1)+dx[i])/n; adxv[i]=a
    return adxv,pdi,mdi

def _supertrend(h,l,c,n=10,mult=3.0):
    N=len(c); at=A.atr(h,l,c,n); d=[None]*N; fu=fl=None; pd=1
    for i in range(N):
        if at[i] is None: continue
        mid=(h[i]+l[i])/2; bu=mid+mult*at[i]; bl=mid-mult*at[i]
        if fu is None: fu,fl,pd=bu,bl,1; d[i]=1; continue
        fu=bu if (bu<fu or c[i-1]>fu) else fu
        fl=bl if (bl>fl or c[i-1]<fl) else fl
        nd=(-1 if c[i]<fl else 1) if pd==1 else (1 if c[i]>fu else -1)
        d[i]=nd; pd=nd
    return d

def prep(p):
    p.sma50=A.sma(p.c,50); p.sma200=A.sma(p.c,200)
    p.adx,p.pdi,p.mdi=_adx(p.h,p.l,p.c,14)
    p.stdir=_supertrend(p.h,p.l,p.c,10,3.0)
    p.dc55hi=A.rmax(p.h,55); p.dc55lo=A.rmin(p.l,55)
    p.mom100=[None if i<100 else p.c[i]/p.c[i-100]-1 for i in range(p.n)]

def t_ma_cross(p,i):
    a,b,pa,pb=p.sma50[i],p.sma200[i],p.sma50[i-1],p.sma200[i-1]
    if None in (a,b,pa,pb): return None
    if pa<=pb and a>b: return "LONG"
    if pa>=pb and a<b: return "SHORT"
    return None
def t_ma_align(p,i):
    a,b=p.sma50[i],p.sma200[i]
    if a is None or b is None: return None
    if p.c[i]>a>b: return "LONG"
    if p.c[i]<a<b: return "SHORT"
    return None
def t_adx(p,i):
    ad,pd,md=p.adx[i],p.pdi[i],p.mdi[i]
    if None in (ad,pd,md) or ad<25: return None
    return "LONG" if pd>md else "SHORT"
def t_supertrend(p,i):
    d,pdd=p.stdir[i],p.stdir[i-1]
    if d is None or pdd is None or d==pdd: return None
    return "LONG" if d==1 else "SHORT"
def t_donch55(p,i):
    if p.dc55hi[i] is None: return None
    if p.c[i]>=p.dc55hi[i]: return "LONG"
    if p.c[i]<=p.dc55lo[i]: return "SHORT"
    return None
def t_mom100(p,i):
    m=p.mom100[i]
    if m is None: return None
    return "LONG" if m>0.10 else "SHORT" if m<-0.10 else None

TREND_SIGS={"ma_cross_50_200":t_ma_cross,"ma_align_50_200":t_ma_align,"adx_dmi":t_adx,
    "supertrend":t_supertrend,"donchian_55":t_donch55,"momentum_100":t_mom100}


def run(pairs, signame, sig, lo, hi, rfa="sma50", rsa="sma200"):
    recs=[]  # (entry_frac, netR, side, hit3, fullstop)
    for p in pairs:
        c,h,l,n=p.c,p.h,p.l,p.n; rf=getattr(p,rfa); rs=getattr(p,rsa)
        i=int(n*lo); end=int(n*hi)
        while i<end-1:
            s=sig(p,i)
            if s is None: i+=1; continue
            r=sim_trade(c,h,l,rf,rs,i,s)
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
    sig=TREND_SIGS[sn]
    a=stats(run(pairs,sn,sig,0.0,1.0)); o=stats(run(pairs,sn,sig,0.5,1.0)); ii=stats(run(pairs,sn,sig,0.0,0.5))
    if not a: return f"{sn:17s} | too few trades"
    if o and ii:
        twoside=o["ln"]>=5 and o["sn"]>=5
        dok=(min(o['le'],o['se'])>0 and min(o['le'],o['se'])>=0.2*max(o['le'],o['se'])) if twoside else True
        tok=o["tpos"]>=max(2,o["tc"]-1) if o["tc"]>=2 else False
        goal=(ii["ER"]>0 and ii["rr"]>=1.5 and o["ER"]>0 and o["rr"]>=1.5)
        verdict=("ROBUST-GOAL" if (goal and dok and tok) else "OOS-goal(fragile)" if goal else "+OOS" if o["ER"]>0 else "fails")
        return (f"{sn:17s} | {a['n']:<4d} {a['ER']:+.3f} {a['win']:4.0f} {a['rr']:4.2f} {a['hit3']:5.0f} {a['fstop']:5.0f} | "
                f"{o['n']:<4d}{o['ER']:+.3f} {o['win']:4.0f} {o['rr']:4.2f} L{o['ln']}@{o['le']:+.2f} S{o['sn']}@{o['se']:+.2f} +{o['tpos']}/{o['tc']} {verdict}")
    return f"{sn:17s} | {a['n']:<4d} {a['ER']:+.3f} {a['win']:4.0f} {a['rr']:4.2f} {a['hit3']:5.0f} {a['fstop']:5.0f} | OOS too few"

async def main():
    global SCALE_AT, USE_REVERSAL
    d=await A.fetch_kraken(1440)   # daily, ~720 bars = ~2 years (fetched ONCE; swept below)
    pairs=A.to_pairs(d)
    for p in pairs: prep(p)
    sigs=list(TREND_SIGS)
    print("STRONGER TREND PREDICTORS as entries + Bill's structure exits (reversal = 50/200 flip). daily, 32 pairs.")
    for sa in (3.0,2.0):
        SCALE_AT=sa; USE_REVERSAL=True
        print(f"\n=== scale@{sa}R  reversal=50/200-flip  (L_init={L_INIT} L_run={L_RUN}) ===")
        print("signal            | n    ER(R)  win%  RR   hit3%  fstop% | OOS: n   ER    win  RR  thirds VERDICT")
        for sn in sigs: print(one(pairs,sn))
    print("\nER(R)=net expectancy/trade in R after fees+rollover. RR=avg win/avg loss. hit3=% reaching +3R. "
          "fstop=% full-stopped pre-scale. GOAL=ER>0 AND RR>=1.5 in BOTH halves + regime-robust.")

if __name__=="__main__":
    asyncio.run(main())
