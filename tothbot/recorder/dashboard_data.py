"""Read-only data layer for the operator dashboard (0500000 mod:Logger STATE SNAPSHOT FILE consumer).

Pure functions over the organism's DURABLE artifacts -- the state_snapshot.json (live state, written by
the StateSnapshotEmitter), the permanent trades_<YYYY>.jsonl (rule:HR-LG-013 realized-trade corpus), and
the diagnostic tothbot.log tail. The dashboard process is a SEPARATE read-only consumer: it reads files
only, never imports/touches the running organism, never writes anything. Decimal-on-wire values are
parsed to float for display aggregation only (a dashboard, not the authoritative tax/CIATS corpus).
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterable, Mapping, Sequence


def _f(value: object) -> "float | None":
    """A best-effort float for display aggregation (the durable record stores Decimals-as-strings)."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def read_snapshot(path: str) -> dict:
    """The live state_snapshot.json (or {} when absent / mid-write / unparseable -- the dashboard shows
    last-known state, never errors)."""
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.loads(fh.read())
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError):
        return {}


def load_trades(path: str) -> list:
    """Parse the permanent trades_<YYYY>.jsonl (one TRADE_CLOSE NDJSON object per line). Blank / bad
    lines are skipped; a missing file is an empty corpus."""
    records: list = []
    try:
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except ValueError:
                    continue
                if isinstance(obj, dict):
                    records.append(obj)
    except OSError:
        return []
    return records


def compute_performance(records: Sequence[Mapping], *, floor: int = 200, recent: int = 25) -> dict:
    """Realized-performance VIEW over the trade records: count, win rate, cumulative + per-trade P/L,
    avg actual_rr, the equity curve (cumulative net P/L), progress to the 200-trade CIATS floor, and the
    most-recent trades. All display floats; the durable JSONL stays the authoritative Decimal source."""
    count = len(records)
    wins = losses = 0
    net_pl = 0.0
    rr_values: list = []
    equity: list = []
    running = 0.0
    for r in records:
        pl = _f(r.get("net_pl_usd")) or 0.0
        net_pl += pl
        running += pl
        equity.append(round(running, 4))
        if pl > 0:
            wins += 1
        elif pl < 0:
            losses += 1
        rr = _f(r.get("actual_rr"))
        if rr is not None:
            rr_values.append(rr)
    win_rate = (wins / count) if count else None
    avg_pl = (net_pl / count) if count else None
    avg_rr = (sum(rr_values) / len(rr_values)) if rr_values else None

    def _row(r: Mapping) -> dict:
        return {
            "symbol": r.get("symbol"),
            "side": r.get("side"),
            "exit_reason": r.get("exit_reason"),
            "net_pl_usd": _f(r.get("net_pl_usd")),
            "actual_rr": _f(r.get("actual_rr")),
            "exit_ts": r.get("exit_timestamp_utc") or r.get("ts"),
            "entry": _f(r.get("entry_fill_price")),
            "exit": _f(r.get("exit_price")),
        }

    return {
        "trade_count": count,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "net_pl_usd": round(net_pl, 4),
        "avg_pl_per_trade": (round(avg_pl, 4) if avg_pl is not None else None),
        "avg_rr": (round(avg_rr, 4) if avg_rr is not None else None),
        "progress_to_floor": f"{count}/{floor}",
        "floor": floor,
        "equity_curve": equity,
        "recent": [_row(r) for r in list(records)[-recent:][::-1]],   # newest first
    }


def tail_events(log_path: str, *, n: int = 40, only_events: bool = True) -> list:
    """The last `n` log lines (default only the [evt] lines -- the event stream the operator watches).
    A missing log is an empty list."""
    try:
        with open(log_path, encoding="utf-8", errors="replace") as fh:
            lines = fh.read().splitlines()
    except OSError:
        return []
    if only_events:
        lines = [ln for ln in lines if "[evt]" in ln]
    return [ln.strip() for ln in lines[-n:][::-1]]   # newest first


def build_payload(
    *, snapshot_path: str, trades_path: str, log_path: str, now_iso: str, floor: int = 200
) -> dict:
    """The full /api/state payload: the live snapshot VIEW + the realized-performance VIEW + the event
    tail, each read fresh from disk. Pure assembly -- read-only, no organism touch."""
    return {
        "server_ts": now_iso,
        "snapshot": read_snapshot(snapshot_path),
        "performance": compute_performance(load_trades(trades_path), floor=floor),
        "events": tail_events(log_path),
    }
