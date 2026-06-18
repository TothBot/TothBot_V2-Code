"""Tests for the read-only dashboard data layer (tothbot/recorder/dashboard_data.py, TB00793).

Pure functions over the durable artifacts (state_snapshot.json + trades_<YYYY>.jsonl + log tail). The
dashboard reads files only -- never the organism -- and tolerates missing / partial / bad inputs."""

from __future__ import annotations

import json
import os
import tempfile

from tothbot.recorder.dashboard_data import (
    build_payload,
    compute_performance,
    load_trades,
    read_snapshot,
    tail_events,
)


def _trade(net, rr, *, symbol="BTC/USD", side="LONG", reason="HTF_REGIME_REVERSAL", ts="2026-06-19T00:00:00+00:00"):
    return {"event": "TRADE_CLOSE", "symbol": symbol, "side": side, "exit_reason": reason,
            "net_pl_usd": str(net), "actual_rr": str(rr), "exit_timestamp_utc": ts,
            "entry_fill_price": "60000", "exit_price": "60500"}


# --------------------------------------------------------------------------- compute_performance
def test_performance_aggregates_count_winrate_pl_and_equity():
    recs = [_trade("2", "1.6"), _trade("-1", "-1"), _trade("3", "2.0"), _trade("-1", "-1")]
    p = compute_performance(recs, floor=200)
    assert p["trade_count"] == 4 and p["wins"] == 2 and p["losses"] == 2
    assert p["win_rate"] == 0.5
    assert p["net_pl_usd"] == 3.0                       # 2-1+3-1
    assert p["avg_pl_per_trade"] == 0.75
    assert p["avg_rr"] == round((1.6 - 1 + 2.0 - 1) / 4, 4)
    assert p["equity_curve"] == [2.0, 1.0, 4.0, 3.0]    # cumulative
    assert p["progress_to_floor"] == "4/200"
    assert p["recent"][0]["symbol"] == "BTC/USD"        # newest first


def test_performance_empty_corpus_is_safe():
    p = compute_performance([], floor=200)
    assert p["trade_count"] == 0 and p["win_rate"] is None and p["avg_pl_per_trade"] is None
    assert p["net_pl_usd"] == 0.0 and p["equity_curve"] == [] and p["recent"] == []
    assert p["progress_to_floor"] == "0/200"


def test_recent_is_capped_and_newest_first():
    recs = [_trade(str(i), "1", ts=f"2026-06-19T00:{i:02d}:00+00:00") for i in range(30)]
    p = compute_performance(recs, recent=10)
    assert len(p["recent"]) == 10
    assert p["recent"][0]["net_pl_usd"] == 29.0 and p["recent"][-1]["net_pl_usd"] == 20.0


# --------------------------------------------------------------------------- load_trades / read_snapshot
def test_load_trades_skips_blank_and_bad_lines():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "trades_2026.jsonl")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(json.dumps(_trade("2", "1.6")) + "\n")
            fh.write("\n")                       # blank
            fh.write("{not json}\n")             # bad
            fh.write(json.dumps(_trade("-1", "-1")) + "\n")
        recs = load_trades(path)
        assert len(recs) == 2
    assert load_trades(os.path.join(d, "gone.jsonl")) == []   # missing file -> empty


def test_read_snapshot_missing_or_bad_is_empty_dict():
    assert read_snapshot("/no/such/file.json") == {}
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "s.json")
        open(path, "w").write("{partial")                    # mid-write / unparseable
        assert read_snapshot(path) == {}
        json.dump({"mode": "paper", "positions": []}, open(path, "w"))
        assert read_snapshot(path)["mode"] == "paper"


# --------------------------------------------------------------------------- tail_events
def test_tail_events_filters_to_event_lines_newest_first():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "log")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("starting paper organism\n")
            for i in range(5):
                fh.write(f"[evt] PING_SENT id={i}\n")
        ev = tail_events(path, n=3)
        assert len(ev) == 3 and ev[0].endswith("id=4")       # newest first
        assert all("[evt]" in e for e in ev)
    assert tail_events("/no/log") == []


# --------------------------------------------------------------------------- build_payload
def test_build_payload_assembles_all_three_views():
    with tempfile.TemporaryDirectory() as d:
        snap = os.path.join(d, "state_snapshot.json")
        trades = os.path.join(d, "trades_2026.jsonl")
        log = os.path.join(d, "log")
        json.dump({"mode": "paper", "ts": "2026-06-19T00:00:00+00:00", "positions": []}, open(snap, "w"))
        open(trades, "w").write(json.dumps(_trade("2", "1.6")) + "\n")
        open(log, "w").write("[evt] WS_CONNECTED\n")
        payload = build_payload(snapshot_path=snap, trades_path=trades, log_path=log,
                                now_iso="2026-06-19T00:01:00+00:00")
        assert payload["snapshot"]["mode"] == "paper"
        assert payload["performance"]["trade_count"] == 1
        assert payload["events"] == ["[evt] WS_CONNECTED"]
        assert payload["server_ts"] == "2026-06-19T00:01:00+00:00"
