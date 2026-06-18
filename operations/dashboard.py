"""TothBot read-only operator dashboard (0500000 mod:Logger STATE SNAPSHOT FILE consumer, TB00793).

A SEPARATE read-only process: it reads the organism's durable files (state_snapshot.json + the
permanent trades_<YYYY>.jsonl + the tothbot.log tail) and serves a single auto-refreshing HTML page +
a /api/state JSON endpoint. It NEVER touches the running organism and writes nothing. Bind to localhost
and reach it over an SSH tunnel:

    # on the VPS:
    python3 operations/dashboard.py --records-dir /root/tothbot_records --log /root/tothbot_paper.log
    # on your laptop:
    ssh -L 8787:localhost:8787 root@<vps>
    # then open http://localhost:8787

No public port, no auth surface, no second writer.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Make `tothbot` importable when launched as `python operations/dashboard.py` from any cwd.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)

from tothbot.recorder.dashboard_data import build_payload  # noqa: E402


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>TothBot Dashboard</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  :root{--bg:#0b1020;--panel:#141a2e;--line:#26304d;--txt:#dfe6f5;--mut:#8aa0c8;--pos:#34d399;--neg:#f87171;--accent:#60a5fa}
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--txt);font:13px/1.45 -apple-system,Segoe UI,Roboto,Helvetica,Arial,monospace}
  header{display:flex;flex-wrap:wrap;gap:14px;align-items:baseline;padding:12px 18px;border-bottom:1px solid var(--line);background:#0e1426;position:sticky;top:0;z-index:5}
  header h1{font-size:16px;margin:0;letter-spacing:.5px}
  header .pill{background:var(--panel);border:1px solid var(--line);border-radius:999px;padding:2px 10px;color:var(--mut)}
  header .pill b{color:var(--txt)}
  .stale{color:var(--neg)!important}
  main{display:grid;grid-template-columns:repeat(auto-fit,minmax(330px,1fr));gap:14px;padding:16px}
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px 14px;overflow:auto}
  .panel h2{font-size:12px;text-transform:uppercase;letter-spacing:1px;color:var(--mut);margin:0 0 10px}
  .wide{grid-column:1/-1}
  table{width:100%;border-collapse:collapse;font-size:12px}
  th,td{text-align:right;padding:4px 7px;border-bottom:1px solid #1d2540;white-space:nowrap}
  th:first-child,td:first-child{text-align:left}
  th{color:var(--mut);font-weight:600}
  .pos{color:var(--pos)} .neg{color:var(--neg)} .mut{color:var(--mut)}
  .bull{color:var(--pos);font-weight:700} .bear{color:var(--neg);font-weight:700}
  .kpis{display:flex;flex-wrap:wrap;gap:18px}
  .kpi{min-width:110px}.kpi .v{font-size:22px;font-weight:700}.kpi .l{color:var(--mut);font-size:11px;text-transform:uppercase;letter-spacing:.5px}
  .evt{font-family:ui-monospace,Menlo,Consolas,monospace;font-size:11px;color:var(--mut);white-space:pre-wrap;word-break:break-all}
  .near{background:#1b2342}
  .empty{color:var(--mut);padding:6px 2px}
  svg{display:block;width:100%;height:60px}
</style></head>
<body>
<header>
  <h1>TothBot &middot; <span id="mode" class="pill">paper</span></h1>
  <span class="pill">snapshot <b id="snapts">--</b></span>
  <span class="pill">warm <b id="warm">--</b></span>
  <span class="pill">open <b id="openc">--</b></span>
  <span class="pill">refreshed <b id="refreshed">--</b></span>
</header>
<main>
  <section class="panel wide"><h2>Performance (realized, paper)</h2>
    <div class="kpis">
      <div class="kpi"><div class="v" id="k_trades">--</div><div class="l">trades / floor</div></div>
      <div class="kpi"><div class="v" id="k_win">--</div><div class="l">win rate (target ~28%)</div></div>
      <div class="kpi"><div class="v" id="k_net">--</div><div class="l">net P/L $</div></div>
      <div class="kpi"><div class="v" id="k_avg">--</div><div class="l">avg $/trade</div></div>
      <div class="kpi"><div class="v" id="k_rr">--</div><div class="l">avg actual_rr</div></div>
    </div>
    <svg id="equity" viewBox="0 0 600 60" preserveAspectRatio="none"></svg>
  </section>

  <section class="panel"><h2>Open positions</h2><div id="positions"></div></section>
  <section class="panel"><h2>CIATS (per module)</h2><div id="ciats"></div></section>
  <section class="panel wide"><h2>Decision board (nearest 24h cross first)</h2><div id="board"></div></section>
  <section class="panel"><h2>Recent trades</h2><div id="trades"></div></section>
  <section class="panel"><h2>Event tail</h2><div id="events" class="evt"></div></section>
</main>

<script>
const $=id=>document.getElementById(id);
const num=(v,d=2)=>v===null||v===undefined?'--':Number(v).toLocaleString(undefined,{maximumFractionDigits:d,minimumFractionDigits:d});
const pct=v=>v===null||v===undefined?'--':(Number(v)*100).toFixed(1)+'%';
const sign=v=>v===null||v===undefined?'mut':(Number(v)>0?'pos':(Number(v)<0?'neg':'mut'));
function tbl(cols,rows){if(!rows.length)return '<div class="empty">none</div>';
  let h='<table><thead><tr>'+cols.map(c=>'<th>'+c[0]+'</th>').join('')+'</tr></thead><tbody>';
  for(const r of rows){h+='<tr'+(r._cls?' class="'+r._cls+'"':'')+'>'+cols.map(c=>'<td'+(c[2]?' class="'+c[2](r)+'"':'')+'>'+c[1](r)+'</td>').join('')+'</tr>';}
  return h+'</tbody></table>';}
function spark(vals){const el=$('equity');if(!vals||vals.length<2){el.innerHTML='';return;}
  const W=600,H=60,mn=Math.min(...vals),mx=Math.max(...vals),rng=(mx-mn)||1;
  const pts=vals.map((v,i)=>[(i/(vals.length-1))*W,H-((v-mn)/rng)*(H-6)-3]);
  const d=pts.map((p,i)=>(i?'L':'M')+p[0].toFixed(1)+' '+p[1].toFixed(1)).join(' ');
  const last=vals[vals.length-1],col=last>=0?'#34d399':'#f87171';const zero=mn<=0&&mx>=0?H-((0-mn)/rng)*(H-6)-3:null;
  el.innerHTML=(zero!==null?'<line x1="0" y1="'+zero.toFixed(1)+'" x2="600" y2="'+zero.toFixed(1)+'" stroke="#26304d" stroke-dasharray="3 3"/>':'')
    +'<path d="'+d+'" fill="none" stroke="'+col+'" stroke-width="1.5"/>';}
function render(s){
  const snap=s.snapshot||{},p=s.performance||{},h=snap.health||{};
  $('mode').textContent=snap.mode||'paper';
  $('snapts').textContent=snap.ts?snap.ts.replace('T',' ').slice(0,19):'(no snapshot yet)';
  const age=snap.ts?(Date.now()-Date.parse(snap.ts))/1000:null;
  $('snapts').className=age!==null&&age>180?'stale':'';
  $('warm').textContent=h.warm_pairs??'--';$('openc').textContent=h.open_position_count??'--';
  $('refreshed').textContent=new Date(s.server_ts).toLocaleTimeString();
  $('k_trades').textContent=p.progress_to_floor||'--';
  $('k_win').textContent=pct(p.win_rate);
  $('k_net').innerHTML='<span class="'+sign(p.net_pl_usd)+'">'+num(p.net_pl_usd)+'</span>';
  $('k_avg').innerHTML='<span class="'+sign(p.avg_pl_per_trade)+'">'+num(p.avg_pl_per_trade)+'</span>';
  $('k_rr').innerHTML='<span class="'+sign(p.avg_rr)+'">'+num(p.avg_rr)+'</span>';
  spark(p.equity_curve);
  $('positions').innerHTML=tbl(
    [['sym',r=>r.symbol],['side',r=>r.side],['qty',r=>num(r.qty,6)],['entry',r=>num(r.entry,4)],
     ['mark',r=>num(r.mark,4)],['uP/L',r=>num(r.unrealized_usd),r=>sign(r.unrealized_usd)],
     ['L2 stop',r=>num(r.l2_stop,4)],['L3',r=>num(r.l3_emergsl,4)]],snap.positions||[]);
  const board=(snap.decision_board||[]).slice().sort((a,b)=>Math.abs(a.gap_pct??99)-Math.abs(b.gap_pct??99));
  $('board').innerHTML=tbl(
    [['sym',r=>r.symbol],['regime',r=>'<span class="mut">'+(r.regime||'--')+'</span>'],
     ['state',r=>r.bullish===true?'<span class="bull">BULL</span>':(r.bullish===false?'<span class="bear">BEAR</span>':'<span class="mut">--</span>')],
     ['EMA12',r=>num(r.ema_fast,2)],['EMA26',r=>num(r.ema_slow,2)],
     ['gap %',r=>num(r.gap_pct,3),r=>sign(r.gap_pct)],['close24h',r=>num(r.close_24h,2)]],
    board.map(r=>({...r,_cls:(r.gap_pct!==null&&r.gap_pct!==undefined&&Math.abs(r.gap_pct)<0.5)?'near':''})));
  const c=snap.ciats||{};
  $('ciats').innerHTML=tbl(
    [['module',r=>r.k.toUpperCase()],['trades',r=>r.trade_count??'--'],['win',r=>pct(r.win_rate)],
     ['net rr',r=>num(r.net_rr)],['pending',r=>r.pending??'--'],['floor',r=>r.progress_to_floor||'--']],
    ['long','short'].map(k=>({k,...(c[k]||{})})));
  $('trades').innerHTML=tbl(
    [['sym',r=>r.symbol],['side',r=>r.side],['reason',r=>'<span class="mut">'+(r.exit_reason||'')+'</span>'],
     ['net$',r=>num(r.net_pl_usd),r=>sign(r.net_pl_usd)],['rr',r=>num(r.actual_rr),r=>sign(r.actual_rr)],
     ['when',r=>(r.exit_ts||'').replace('T',' ').slice(5,19)]],p.recent||[]);
  $('events').textContent=(s.events||[]).join('\n')||'(no events)';
}
async function tick(){try{const r=await fetch('/api/state',{cache:'no-store'});render(await r.json());}catch(e){}}
tick();setInterval(tick,5000);
</script>
</body></html>"""


class _Handler(BaseHTTPRequestHandler):
    config: dict = {}

    def log_message(self, *a):  # silence the default stderr request log
        pass

    def _send(self, code, body, ctype):
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path in ("/", "/index.html"):
            self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
            return
        if path == "/api/state":
            cfg = self.config
            payload = build_payload(
                snapshot_path=cfg["snapshot"], trades_path=cfg["trades"], log_path=cfg["log"],
                now_iso=datetime.now(timezone.utc).isoformat(),
            )
            self._send(200, json.dumps(payload).encode("utf-8"), "application/json")
            return
        self._send(404, b"not found", "text/plain")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="TothBot read-only operator dashboard")
    ap.add_argument("--records-dir", default="/root/tothbot_records",
                    help="dir holding state_snapshot.json + trades_<YYYY>.jsonl")
    ap.add_argument("--snapshot", default=None, help="override path to state_snapshot.json")
    ap.add_argument("--trades", default=None, help="override path to trades_<YYYY>.jsonl")
    ap.add_argument("--log", default="/root/tothbot_paper.log", help="path to the organism log")
    ap.add_argument("--year", default=str(datetime.now(timezone.utc).year), help="trades file year")
    ap.add_argument("--host", default="127.0.0.1", help="bind host (localhost only by default)")
    ap.add_argument("--port", type=int, default=8787)
    args = ap.parse_args(argv)

    snapshot = args.snapshot or os.path.join(args.records_dir, "state_snapshot.json")
    trades = args.trades or os.path.join(args.records_dir, f"trades_{args.year}.jsonl")
    _Handler.config = {"snapshot": snapshot, "trades": trades, "log": args.log}

    server = ThreadingHTTPServer((args.host, args.port), _Handler)
    print(f"TothBot dashboard (read-only) on http://{args.host}:{args.port}")
    print(f"  snapshot: {snapshot}")
    print(f"  trades:   {trades}")
    print(f"  log:      {args.log}")
    print(f"  tunnel:   ssh -L {args.port}:localhost:{args.port} root@<vps>  then open http://localhost:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
