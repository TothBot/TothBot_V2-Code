"""TB00775 - AUTOMATED strategy-combination search (server-side cron; the standing hunt for a 1:1.5-R:R edge).

Bill's directive: set the 5m search to run automatically as the corpus accumulates - but DO NOT limit to
5m; keep the entire universe open and let the data lead. The real question: "How do we achieve 1:1.5 R:R
or better?"

WHAT IT DOES each run (a VPS cron): for EVERY data source - the GROWING live 5m corpus (records_dir
ohlc5m_*.jsonl) PLUS fresh Kraken 1h / 4h / 1d pulls - it runs the full combination search (9 entry
signals x 4 filters x 14 exits incl chandelier trailing) with HARD nested validation: SELECT goal-meeting
combos (E>0 AND realized R:R>=1.5) on the in-sample half, then report only those that SURVIVE on the
truly untouched out-of-sample half, with a block-bootstrap CI. So the email can NEVER oversell an overfit
combo (the trap that killed the daily search). A source with too little data reports "accumulating, ETA".

It writes a dated report to records_dir/strategy_search_report.txt and EMAILs a summary (the same SMTP as
the operator reports). If an OOS SURVIVOR appears, the subject flags it loudly.

Pure-stdlib + the tothbot config/fees + the REST client; no new organism coupling. Idempotent + safe to
cron (read-only on the corpus; only fetches public Kraken + writes its own report)."""

from __future__ import annotations

import asyncio
import glob
import json
import math
import os
import smtplib
import sys
import time
import urllib.parse
import urllib.request
from email.message import EmailMessage

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tothbot.config import registry  # noqa: E402
from tothbot.config.fees import FEE_TAKER_PCT  # noqa: E402

TAKER = float(FEE_TAKER_PCT)
ROLL_DAY = float(registry.value("margin_rollover_fee_pct")) * 6.0   # per daily bar; scaled per-tf below
OPEN_FEE = float(registry.value("margin_open_fee_pct"))
KS = [2.0, 3.0, 5.0]; MS = [1.5, 2.0, 3.0, None]; TRAILS = [1.5, 2.0, 3.0]
RECORDS_DIR = os.environ.get("TOTHBOT_RECORDS_DIR", "/root/tothbot_records")
REST_BASE = "https://api.kraken.com"
PAIRS = ["AVAX/USD","AAVE/USD","BCH/USD","BTC/USD","ETH/USD","SOL/USD","XRP/USD","ADA/USD","DOT/USD",
    "LINK/USD","UNI/USD","LTC/USD","DOGE/USD","APT/USD","ARKM/USD","TIA/USD","HBAR/USD","OP/USD",
    "RUNE/USD","WLD/USD","PENDLE/USD","INJ/USD","SUI/USD","NEAR/USD","ATOM/USD","FIL/USD","TAO/USD",
    "RENDER/USD","SEI/USD","GRT/USD","ALGO/USD","FET/USD"]


# ---------------- indicators (float) ----------------
def ema(xs,n):
    a=2.0/(n+1); o=[None]*len(xs); e=None
    for i,x in enumerate(xs):
        e=x if e is None else (x-e)*a+e; o[i]=e
    return o
def rsi(xs,n=14):
    o=[None]*len(xs); ag=al=None
    for i in range(1,len(xs)):
        d=xs[i]-xs[i-1]; g=max(0.0,d); l=max(0.0,-d)
        if ag is None:
            if i>=n:
                ag=sum(max(0.0,xs[k]-xs[k-1]) for k in range(i-n+1,i+1))/n
                al=sum(max(0.0,xs[k-1]-xs[k]) for k in range(i-n+1,i+1))/n
        else: ag=(ag*(n-1)+g)/n; al=(al*(n-1)+l)/n
        if ag is not None: o[i]=100.0 if al==0 else 100.0-100.0/(1+ag/al)
    return o
def atr(h,l,c,n=14):
    o=[None]*len(c); tr=[]; a=None
    for i in range(len(c)):
        t=h[i]-l[i] if i==0 else max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])); tr.append(t)
        if i==n: a=sum(tr[1:n+1])/n; o[i]=a
        elif i>n: a=(a*(n-1)+t)/n; o[i]=a
    return o
def sma(xs,n):
    o=[None]*len(xs); s=0.0
    for i,x in enumerate(xs):
        s+=x
        if i>=n: s-=xs[i-n]
        if i>=n-1: o[i]=s/n
    return o
def rstd(xs,n):
    o=[None]*len(xs)
    for i in range(n-1,len(xs)):
        w=xs[i-n+1:i+1]; m=sum(w)/n; o[i]=math.sqrt(sum((x-m)**2 for x in w)/n)
    return o
def rmax(xs,n): return [None if i<n else max(xs[i-n:i]) for i in range(len(xs))]
def rmin(xs,n): return [None if i<n else min(xs[i-n:i]) for i in range(len(xs))]


class P: pass

def build_pair(sym,c,h,l,btc_ret):
    n=len(c)
    if n<200: return None
    p=P(); p.sym=sym; p.c=c; p.h=h; p.l=l; p.n=n
    p.rsi=rsi(c); p.e12=ema(c,12); p.e26=ema(c,26)
    macd=[(p.e12[i]-p.e26[i]) if p.e12[i] is not None else None for i in range(n)]
    p.macd=macd; p.macdsig=ema([m if m is not None else 0.0 for m in macd],9)
    p.sma20=sma(c,20); p.std20=rstd(c,20); p.atr=atr(h,l,c,14); p.sma100=sma(c,100)
    p.dchi=rmax(h,20); p.dclo=rmin(l,20)
    p.mom=[None if i<20 else c[i]/c[i-20]-1 for i in range(n)]
    p.btc_ret=btc_ret
    p.atrpct=[None]*n
    for i in range(n):
        if p.atr[i] is None: continue
        w=[p.atr[k] for k in range(max(0,i-100),i+1) if p.atr[k] is not None]
        if len(w)>20: p.atrpct[i]=sum(1 for x in w if x<=p.atr[i])/len(w)
    return p


def s_rsi_meanrev(p,i):
    r=p.rsi[i]; return None if r is None else ("LONG" if r<30 else "SHORT" if r>70 else None)
def s_rsi_trend(p,i):
    r,e9,e21=p.rsi[i],p.e12[i],p.e26[i]
    if None in (r,e9,e21): return None
    return "LONG" if (45<r<70 and e9>e21) else "SHORT" if (30<r<55 and e9<e21) else None
def s_ema_cross(p,i):
    return None if p.e12[i] is None or p.e26[i] is None else ("LONG" if p.e12[i]>p.e26[i] else "SHORT")
def s_macd(p,i):
    return None if p.macd[i] is None else ("LONG" if p.macd[i]>p.macdsig[i] else "SHORT")
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
    rel=p.mom[i]-p.btc_ret[i]; return "LONG" if rel>0.05 else "SHORT" if rel<-0.05 else None
SIGNALS={"rsi_meanrev":s_rsi_meanrev,"rsi_trend":s_rsi_trend,"ema_cross":s_ema_cross,"macd":s_macd,
    "bb_break":s_bb_break,"bb_revert":s_bb_revert,"donchian":s_donchian,"momentum":s_momentum,"rs_btc":s_rs_btc}

def f_none(p,i,s): return True
def f_trend100(p,i,s):
    return True if p.sma100[i] is None else ((p.c[i]>p.sma100[i]) if s=="LONG" else (p.c[i]<p.sma100[i]))
def f_vol(p,i,s): return p.atrpct[i] is not None and 0.2<=p.atrpct[i]<=0.85
def f_btc(p,i,s):
    b=p.btc_ret[i]; return True if b is None else ((b>-0.02) if s=="LONG" else (b<0.02))
FILTERS={"none":f_none,"trend100":f_trend100,"vol":f_vol,"btc_tide":f_btc}


def precompute(p, maxhold, roll_per_bar):
    ex={}
    for i in range(p.n-1):
        if p.atr[i] is None: continue
        e=p.c[i]; atrf=p.atr[i]/e
        if atrf<=0: continue
        for side in ("LONG","SHORT"):
            so=[-1]*len(KS); to=[-1]*len(MS); madv=mfav=0.0; revexc=None; hold=None
            peak=0.0; tro=[-1]*len(TRAILS); trx=[None]*len(TRAILS); jmax=min(p.n,i+1+maxhold)
            for off,j in enumerate(range(i+1,jmax),start=1):
                advpx=p.h[j] if side=="SHORT" else p.l[j]; favpx=p.l[j] if side=="SHORT" else p.h[j]
                adv=((advpx-e) if side=="SHORT" else (e-advpx))/e; fav=((e-favpx) if side=="SHORT" else (favpx-e))/e
                if adv>madv: madv=adv
                if fav>mfav: mfav=fav
                for ki,k in enumerate(KS):
                    if so[ki]==-1 and madv>=k*atrf: so[ki]=off
                for mi,m in enumerate(MS):
                    if m is not None and to[mi]==-1 and mfav>=m*atrf: to[mi]=off
                cw=-adv
                for ti,tr in enumerate(TRAILS):
                    if tro[ti]==-1 and cw<=(peak-tr*atrf): tro[ti]=off; trx[ti]=peak-tr*atrf
                if fav>peak: peak=fav
                if revexc is None and p.e12[j] is not None and ((side=="LONG" and p.e12[j]<p.e26[j]) or (side=="SHORT" and p.e12[j]>p.e26[j])):
                    revexc=((e-p.c[j]) if side=="SHORT" else (p.c[j]-e))/e; hold=off
            if revexc is None:
                j=jmax-1; revexc=((e-p.c[j]) if side=="SHORT" else (p.c[j]-e))/e; hold=jmax-1-i
            for ti in range(len(TRAILS)):
                if tro[ti]==-1: j=jmax-1; tro[ti]=jmax-1-i; trx[ti]=((e-p.c[j]) if side=="SHORT" else (p.c[j]-e))/e
            ex[(i,side)]=(atrf,so,to,revexc,hold,tro,trx,roll_per_bar)
    return ex

def trade_pnl(entry,spec,side):
    atrf,so,to,revexc,hold,tro,trx,rpb=entry
    fee=TAKER+TAKER+(OPEN_FEE if side=="SHORT" else 0.0)
    if spec[0]=="tr": ti=spec[1]; gross,off=trx[ti],tro[ti]
    else:
        ki,mi=spec[1],spec[2]; k=KS[ki]; m=MS[mi]; cands=[(hold,revexc,2)]
        if so[ki]!=-1: cands.append((so[ki],-(k*atrf),0))
        if m is not None and to[mi]!=-1: cands.append((to[mi],m*atrf,1))
        off,gross,_=min(cands,key=lambda c:(c[0],c[2]))
    roll=(rpb*off) if side=="SHORT" else 0.0
    return gross-fee-roll, off

def run_combo(pairs,exits,sig,filt,spec,lo,hi,minn=40):
    pls=[]; holds=[]; bypair={}
    for p in pairs:
        ex=exits[p.sym]; i=int(p.n*lo); end=int(p.n*hi)
        while i<end-1:
            side=sig(p,i)
            if side is None or (i,side) not in ex or not filt(p,i,side): i+=1; continue
            pl,off=trade_pnl(ex[(i,side)],spec,side); pls.append(pl); holds.append(off)
            bypair.setdefault(p.sym,[]).append(pl); i+=max(1,off)
    if len(pls)<minn: return None
    w=[x for x in pls if x>0]; ls=[x for x in pls if x<=0]
    avgw=sum(w)/len(w) if w else 0.0; avgl=-sum(ls)/len(ls) if ls else 1e-9
    return {"n":len(pls),"win":100*len(w)/len(pls),"E":sum(pls)/len(pls)*100,
            "Eday":sum(pls)/(sum(holds) or 1)*100,"rr":avgw/avgl if avgl>0 else 0.0,
            "npairs":len(bypair),"bypair":bypair}

def boot(bypair,iters=400,seed=7):
    pp=list(bypair); m=len(pp)
    if m<3: return (0.0,0.0,0.0,m)
    s=seed; vals=[]
    for _ in range(iters):
        tot=0.0; c=0
        for _ in range(m):
            s=(1103515245*s+12345)&0x7FFFFFFF; pr=pp[(s*m)>>31]; tot+=sum(bypair[pr]); c+=len(bypair[pr])
        vals.append(tot/(c or 1)*100)
    vals.sort(); return (vals[int(.05*iters)],vals[iters//2],vals[int(.95*iters)],m)


def search(pairs, maxhold, roll_per_bar, label):
    """Nested IS-select / OOS-validate over all combos. Returns (summary_lines, n_survivors)."""
    exits={p.sym:precompute(p,maxhold,roll_per_bar) for p in pairs}
    specs=[("st",ki,mi,f"{KS[ki]:.0f}x/{'rev' if MS[mi] is None else f'{MS[mi]:.1f}x'}")
           for ki in range(len(KS)) for mi in range(len(MS))]
    specs+=[("tr",ti,None,f"trail{TRAILS[ti]:.1f}x") for ti in range(len(TRAILS))]
    rows=[]
    for sn,sig in SIGNALS.items():
        for fn,filt in FILTERS.items():
            for sp in specs:
                isr=run_combo(pairs,exits,sig,filt,(sp[0],sp[1],sp[2]),0.0,0.5)
                oos=run_combo(pairs,exits,sig,filt,(sp[0],sp[1],sp[2]),0.5,1.0)
                if isr and oos: rows.append((sn,fn,sp[3],isr,oos))
    isg=[r for r in rows if r[3]["E"]>0 and r[3]["rr"]>=1.5]
    surv=[r for r in isg if r[4]["E"]>0 and r[4]["rr"]>=1.5]
    out=[f"[{label}] pairs={len(pairs)} combos={len(rows)} | IS-goal-meeters={len(isg)} | OOS-SURVIVORS={len(surv)}"]
    for sn,fn,xl,a,o in sorted(surv,key=lambda r:r[4]["E"],reverse=True)[:8]:
        lo,mid,hi,m=boot(o["bypair"])
        sig="SIG" if lo>0 else "ci~0"
        out.append(f"    SURVIVOR {sn}/{fn}/{xl}: OOS E/trade {o['E']:+.3f}% rr {o['rr']:.2f} win {o['win']:.0f}% "
                   f"n={o['n']} npairs={o['npairs']} boot[{lo:+.3f}..{hi:+.3f}]% {sig}")
    return out, len(surv)


# ---------------- data sources ----------------
async def _get(url, params):
    q=urllib.parse.urlencode(params)
    def _do():
        req=urllib.request.Request(url+"?"+q, headers={"User-Agent":"tb775auto"})
        with urllib.request.urlopen(req,timeout=25) as r: return json.loads(r.read().decode())
    return await asyncio.get_event_loop().run_in_executor(None,_do)

async def fetch_kraken(interval):
    """{sym: (closes,highs,lows,times)} from Kraken OHLC at `interval` minutes (~720 bars)."""
    out={}
    for sym in PAIRS:
        try:
            pair=sym.replace("/","")
            d=await _get(REST_BASE+"/0/public/OHLC", {"pair":pair,"interval":interval})
            res=d.get("result",{}); rows=next((v for k,v in res.items() if k!="last"), None)
            if not rows: continue
            rows=rows[:-1]  # drop forming
            c=[float(r[4]) for r in rows]; h=[float(r[2]) for r in rows]; l=[float(r[3]) for r in rows]; t=[int(r[0]) for r in rows]
            if len(c)>=200: out[sym]=(c,h,l,t)
        except Exception:
            continue
        await asyncio.sleep(1.1)  # honor the public budget
    return out

def load_corpus():
    """{sym: (closes,highs,lows,times)} from the live 5m corpus ohlc5m_*.jsonl (sorted, deduped)."""
    rowsby={}
    for path in sorted(glob.glob(os.path.join(RECORDS_DIR,"ohlc5m_*.jsonl"))):
        try:
            with open(path) as f:
                for line in f:
                    try: d=json.loads(line)
                    except Exception: continue
                    rowsby.setdefault(d["symbol"],{})[int(d["interval_begin"])]=d
        except Exception: continue
    out={}
    for sym,bym in rowsby.items():
        ts=sorted(bym);
        if len(ts)<200: continue
        out[sym]=([float(bym[t]["close"]) for t in ts],[float(bym[t]["high"]) for t in ts],
                  [float(bym[t]["low"]) for t in ts], ts)
    return out


def to_pairs(data):
    btc=data.get("BTC/USD")
    btc_mom={}
    if btc:
        c,_,_,t=btc
        for i in range(len(c)):
            btc_mom[t[i]//86400]=(c[i]/c[i-20]-1) if i>=20 else None
    pairs=[]
    for sym,(c,h,l,t) in data.items():
        btc_ret=[btc_mom.get(tt//86400) for tt in t]
        p=build_pair(sym,c,h,l,btc_ret)
        if p: pairs.append(p)
    return pairs


def email_report(subject, body):
    host=os.environ.get("TOTHBOT_SMTP_HOST"); user=os.environ.get("TOTHBOT_SMTP_USER")
    pw=os.environ.get("TOTHBOT_SMTP_PASSWORD") or os.environ.get("TOTHBOT_SMTP_PASS")
    to=os.environ.get("TOTHBOT_REPORT_RECIPIENTS") or "nwoguy56@proton.me"
    if not (host and user and pw):
        print("(no SMTP env - skipping email)"); return
    msg=EmailMessage(); msg["From"]=os.environ.get("TOTHBOT_EMAIL_SENDER",user); msg["To"]=to
    msg["Subject"]=subject; msg.set_content(body)
    try:
        s=smtplib.SMTP(host,int(os.environ.get("TOTHBOT_SMTP_PORT","587")),timeout=20)
        s.starttls(); s.login(user,pw); s.send_message(msg); s.quit(); print("emailed", to)
    except Exception as e:
        print("email failed:", repr(e))


async def main():
    # roll_per_bar = the SHORT margin rollover charged per BAR of hold, scaled to the bar's hours.
    # 5m bar = 5/60 h -> ROLL_4H * (bar_h/4); daily = ROLL_DAY. (ROLL_DAY already = ROLL_4H*6.)
    ROLL_4H=ROLL_DAY/6.0
    sources=[]
    corpus=load_corpus()
    if corpus: sources.append(("5m-corpus", corpus, 144, ROLL_4H*(5/60)/4))   # maxhold 144 bars = 12h
    for iv,lbl,maxhold in ((60,"1h",120),(240,"4h",90),(1440,"1d",60)):
        d=await fetch_kraken(iv)
        if d: sources.append((lbl, d, maxhold, ROLL_4H*((iv/60)/4)))
    report=[f"=== TothBot automated strategy search  {time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime())} ==="]
    total_surv=0
    for lbl,data,maxhold,rpb in sources:
        pairs=to_pairs(data)
        if len(pairs)<8:
            report.append(f"[{lbl}] ACCUMULATING - only {len(pairs)} pairs with >=200 bars (need >=8). Waiting for more data.")
            continue
        try:
            lines,ns=search(pairs,maxhold,rpb,lbl); total_surv+=ns; report.extend(lines)
        except Exception as e:
            report.append(f"[{lbl}] ERROR {type(e).__name__}: {e}")
    report.append(f"\nGOAL = a config that meets 1:1.5 R:R AND positive expectancy that SURVIVES out-of-sample. "
                  f"Total OOS survivors this run: {total_surv}.")
    body="\n".join(report)
    print(body)
    try:
        with open(os.path.join(RECORDS_DIR,"strategy_search_report.txt"),"a") as f: f.write(body+"\n\n")
    except Exception: pass
    flag="*** OOS SURVIVOR(S) FOUND ***  " if total_surv else ""
    email_report(f"{flag}TothBot strategy search ({total_surv} OOS survivors)", body)


if __name__=="__main__":
    asyncio.run(main())
