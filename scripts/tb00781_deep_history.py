"""TB00781 - re-test the 8h sweet spot on DEEP history (Bill: don't rely solely on Kraken).

Kraken caps intraday history at ~720 bars, so TB00780's 8h/12h sweet-spot finding rode a single ~120-day
window (one regime) and the bootstrap CI spanned zero. Bill: pull longer history from another source.

Binance's PUBLIC market-data mirror (data-api.binance.vision, no auth, not geo-blocked) serves NATIVE 4h /
6h / 8h / 12h / 1d back to ~2019 = 7+ years across multiple FULL bull/bear cycles. This re-runs Bill's
trailing STRUCTURE STOP (support/resistance, ratchet in favor, run-to-reversal, NO take-profit; tb00780)
plus the wide-ATR comparison, now with a real MULTI-ERA walk-forward (8 eras ~1yr each) + a bootstrap that
can actually reach significance. The question: is the 8h edge DURABLE across eras, or was it that one
window?

Reuses tb00780 (sim_struct/run_struct) + tb00779 (stats/bootstrap/entries) + auto_strategy_search. All in R
(1R = initial stop distance). PAPER research only; public GET only."""

from __future__ import annotations
import asyncio, os, json, time, importlib.util, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("t780", os.path.join(HERE, "tb00780_structure_speed.py"))
T80 = importlib.util.module_from_spec(spec); spec.loader.exec_module(T80)
E = T80.E; A = E.A
statR, bootR, era_pos, ENTRIES = E.statR, E.bootR, E.era_pos, E.ENTRIES
run_struct, line, LS = T80.run_struct, T80.line, T80.LS

BINANCE = "https://data-api.binance.vision/api/v3/klines"
START_MS = 1546300800000   # 2019-01-01
# our universe -> Binance USDT symbols (pairs not listed / too short just fall out)
UNIVERSE = ["BTC", "ETH", "SOL", "XRP", "ADA", "AVAX", "DOGE", "LINK", "DOT", "LTC", "BCH", "UNI",
            "AAVE", "ATOM", "FIL", "NEAR", "ALGO", "GRT", "FET", "INJ", "RUNE", "HBAR", "APT", "OP",
            "TIA", "SUI", "SEI", "WLD", "PENDLE", "ARKM", "TAO", "RENDER"]


def _get(url):
    req = urllib.request.Request(url, headers={"User-Agent": "tb781"})
    with urllib.request.urlopen(req, timeout=25) as r:
        return json.loads(r.read().decode())


def fetch_binance(symbol, interval):
    """Paginated native klines -> (closes, highs, lows, times_sec). Drops the final (forming) bar."""
    rows = []; cur = START_MS
    for _ in range(20):
        try:
            batch = _get(f"{BINANCE}?symbol={symbol}&interval={interval}&startTime={cur}&limit=1000")
        except Exception:
            break
        if not batch: break
        rows += batch
        if len(batch) < 1000: break
        cur = batch[-1][0] + 1
        time.sleep(0.12)
    if len(rows) < 2: return None
    rows = rows[:-1]   # drop forming
    c = [float(r[4]) for r in rows]; h = [float(r[2]) for r in rows]
    l = [float(r[3]) for r in rows]; t = [int(r[0] // 1000) for r in rows]
    return (c, h, l, t) if len(c) >= 200 else None


def fetch_all(interval):
    cache = os.path.join(HERE, f"_binance_cache_{interval}.json")   # local-only, not committed
    if os.path.exists(cache):
        try:
            with open(cache) as f: d = json.load(f)
            return {k: tuple(v) for k, v in d.items()}
        except Exception:
            pass
    out = {}
    for base in UNIVERSE:
        syms = ["RENDERUSDT", "RNDRUSDT"] if base == "RENDER" else [base + "USDT"]
        for s in syms:
            d = fetch_binance(s, interval)
            if d: out[base + "/USD"] = d; break
        time.sleep(0.05)
    try:
        with open(cache, "w") as f: json.dump(out, f)
    except Exception:
        pass
    return out


def analyse(label, data, iv_min, m_eras, entry_name):
    pairs = A.to_pairs(data)
    if len(pairs) < 8:
        print(f"\n[{label}] only {len(pairs)} pairs - skip"); return
    sig = ENTRIES[entry_name]
    roll_per_bar = (A.ROLL_DAY / 6.0) * ((iv_min / 60) / 4)
    span = max(p.n for p in pairs) * iv_min / 1440
    print(f"\n{'='*104}\n[{label}]  entry={entry_name}  pairs={len(pairs)}  maxbars~{max(p.n for p in pairs)}  "
          f"~{span:.0f} days (~{span/365:.1f} yr)  eras={m_eras}")
    print("  TRAILING STRUCTURE STOP (support/resistance), run-to-reversal, NO take-profit:")
    for L in LS:
        recs, bypair = run_struct(pairs, sig, L, roll_per_bar)
        txt, _ = line(f"L={L}", recs, bypair, m_eras); print(txt)
    recs, bypair = E.run_combo(pairs, sig, 3.0, "hold", roll_per_bar)
    txt, _ = line("ATR3x+rev", recs, bypair, m_eras); print("  COMPARE fixed wide stop:\n" + txt)


async def main():
    print("TB00781 deep-history (Binance public mirror, native bars back to 2019) - is the 8h edge durable?")
    print("All in R (1R = initial stop distance). DURABLE = +OOS AND +in-sample AND rr>=1.5 AND both sides + "
          "AND era-stable AND bootstrap CI>0.")
    intervals = [("4h", "4h", 240, 8), ("6h", "6h", 360, 8), ("8h", "8h", 480, 8),
                 ("12h", "12h", 720, 8), ("1d (24h)", "1d", 1440, 8)]
    cache = {}
    for label, iv, iv_min, me in intervals:
        print(f"\n... fetching {label} from Binance mirror ...")
        cache[iv] = fetch_all(iv)
        print(f"    got {len(cache[iv])} pairs")
    for entry_name in ("emacross", "momentum"):
        print(f"\n\n##################  ENTRY = {entry_name}  ##################")
        for label, iv, iv_min, me in intervals:
            try:
                analyse(label, cache[iv], iv_min, me, entry_name)
            except Exception as ex:
                print(f"\n[{label}] ERROR {type(ex).__name__}: {ex}")
    print("\nNOTE: deep native history (~7yr, multiple bull/bear cycles) - the era test + bootstrap are now "
          "meaningful. Compare directly to TB00780's single-120d-window 8h result.")


if __name__ == "__main__":
    asyncio.run(main())
