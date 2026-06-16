"""rule:HR-LG-013 - the permanent trade-record FILE sink (the durable Stream-2 corpus).

Exercises tothbot/recorder/trade_record_file.py: the canonical NDJSON serialize/parse round-trip, the
durable append-only sink (open O_WRONLY|O_APPEND|O_CREAT mode 0o644, fsync-per-write before close, no
handle across events, annual UTC segmentation, the TRADE_CLOSE-only filter, the local-catch failure
path -> evt:TRADE_RECORD_WRITE_FAILED + stderr), and the corpus read-back. The low-level os edges are
injected so the whole layer is unit-testable without real disk I/O.
"""

from __future__ import annotations

import os
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal

from tothbot.execution.exit_controller import ExitReason, TradeClose
from tothbot.recorder.logger import Logger
from tothbot.recorder.trade_record_file import (
    PermanentTradeRecordSink,
    TradeRecordWriteFailed,
    load_trade_records,
    load_trade_records_dir,
    load_trade_records_file,
    parse_trade_close,
    serialize_trade_close,
)

UTC = timezone.utc


def _tc(net="120", *, when="2026-06-10T12:00:00+00:00", regime="TRENDING_POS_NORMAL"):
    n = Decimal(net)
    win = n > 0
    return TradeClose(
        symbol="BTC/USD", entry_fill_price=Decimal("60000"), exit_price=Decimal("66000.5"),
        exit_reason=ExitReason.HTF_REGIME_REVERSAL,
        fees_entry_usd=Decimal("7.8"), fees_exit_usd=Decimal("8.58"), fees_total_usd=Decimal("16.38"),
        net_pl_usd=n, net_gain_usd=(n if win else Decimal("0")),
        net_loss_usd=(Decimal("0") if win else -n), asset_regime=regime, vol_regime="NORMAL_VOL",
        market_regime="TRENDING_POS_NORMAL", exit_timestamp_utc=when, hold_candle_count=12,
        actual_rr=Decimal("1.6"), mae_pct_reached=Decimal("0.4"), qty=Decimal("0.05"), side="LONG",
        signal_params={"rsi_14": Decimal("41.5"), "ema_9": Decimal("100.2")},
    )


# --------------------------------------------------------------------------- a recording fake fs
class _FakeFs:
    def __init__(self):
        self.makedirs_calls: list = []
        self.files: dict[str, bytearray] = {}
        self.ops: list = []
        self._fd_path: dict[int, str] = {}
        self._next_fd = 100

    def makedirs(self, path, *, exist_ok=False):
        self.makedirs_calls.append((path, exist_ok))

    def open(self, path, flags, mode):
        fd = self._next_fd
        self._next_fd += 1
        self._fd_path[fd] = path
        self.files.setdefault(path, bytearray())
        self.ops.append(("open", path, flags, mode))
        return fd

    def write(self, fd, data):
        self.files[self._fd_path[fd]].extend(data)
        self.ops.append(("write", fd, len(data)))
        return len(data)

    def fsync(self, fd):
        self.ops.append(("fsync", fd))

    def close(self, fd):
        self.ops.append(("close", fd))

    def _sink(self, **kw):
        return PermanentTradeRecordSink(
            "/records", os_open=self.open, os_write=self.write, os_fsync=self.fsync,
            os_close=self.close, makedirs=self.makedirs, **kw)


# ----------------------------------------------------------------------------- serialize / parse
def test_serialize_is_single_line_decimal_string_ndjson():
    line = serialize_trade_close(_tc("120"))
    assert "\n" not in line                                   # one record, one line
    import json
    doc = json.loads(line)
    # the mandatory record-identity fields are present + lead the record (insertion order).
    assert list(doc)[:4] == ["ts", "event", "level", "component"]
    assert doc["event"] == "TRADE_CLOSE" and doc["level"] == "INFO" and doc["component"] == "EXIT_CTRL"
    # every numeric is a JSON STRING, never a float (ar:AR-047).
    assert doc["net_pl_usd"] == "120" and doc["exit_price"] == "66000.5"
    assert doc["actual_rr"] == "1.6" and doc["fees_total_usd"] == "16.38"
    assert isinstance(doc["net_pl_usd"], str) and isinstance(doc["exit_price"], str)
    # the exit_reason enum renders its value; signal_params is a nested Decimal-as-string dict.
    assert doc["exit_reason"] == "HTF_REGIME_REVERSAL"
    assert doc["signal_params"] == {"rsi_14": "41.5", "ema_9": "100.2"}
    # the int count stays an exact JSON number (not precision-sensitive).
    assert doc["hold_candle_count"] == 12


def test_serialize_parse_round_trips_to_decimal():
    import json
    tc = _tc("-55")
    back = parse_trade_close(json.loads(serialize_trade_close(tc)))
    assert back == tc                                         # full dataclass equality (Decimals + enum)
    assert isinstance(back.net_pl_usd, Decimal) and back.exit_reason is ExitReason.HTF_REGIME_REVERSAL
    assert back.signal_params["rsi_14"] == Decimal("41.5")


def test_serialize_is_deterministic():
    tc = _tc("7")
    assert serialize_trade_close(tc) == serialize_trade_close(tc)


# ----------------------------------------------------------------------------- the durable sink
def test_sink_makes_the_records_dir_at_construction():
    fs = _FakeFs()
    fs._sink()
    assert fs.makedirs_calls == [("/records", True)]


def test_sink_appends_a_trade_close_with_fsync_before_close():
    fs = _FakeFs()
    sink = fs._sink(now_utc=lambda: datetime(2026, 6, 15, 8, 0, tzinfo=UTC))
    sink(_tc("120"))

    path = os.path.join("/records", "trades_2026.jsonl")
    # the bytes are exactly the NDJSON line + a trailing newline, utf-8.
    written = bytes(fs.files[path])
    assert written.endswith(b"\n")
    assert written == (serialize_trade_close(_tc("120")) + "\n").encode("utf-8")
    # the open used append-only create flags + mode 0o644.
    op_open = next(o for o in fs.ops if o[0] == "open")
    assert op_open[1] == path and op_open[3] == 0o644
    assert op_open[2] & os.O_WRONLY and op_open[2] & os.O_APPEND and op_open[2] & os.O_CREAT
    # fsync ran AFTER write and BEFORE close (durability per write).
    kinds = [o[0] for o in fs.ops]
    assert kinds == ["open", "write", "fsync", "close"]


def test_sink_ignores_non_trade_close_events():
    fs = _FakeFs()
    sink = fs._sink()
    sink(TradeRecordWriteFailed(path="/x", error="boom"))     # not a TRADE_CLOSE
    sink({"event": "CANDLE_CLOSE"})
    assert fs.ops == []                                        # nothing opened/written


def test_sink_annual_utc_segmentation_opens_a_new_file_per_year():
    fs = _FakeFs()
    years = iter([datetime(2026, 12, 31, 23, 0, tzinfo=UTC), datetime(2027, 1, 1, 0, 30, tzinfo=UTC)])
    sink = fs._sink(now_utc=lambda: next(years))
    sink(_tc("1"))
    sink(_tc("2"))
    assert os.path.join("/records", "trades_2026.jsonl") in fs.files
    assert os.path.join("/records", "trades_2027.jsonl") in fs.files
    # no handle held across events: two opens, two closes.
    assert [o[0] for o in fs.ops] == ["open", "write", "fsync", "close",
                                      "open", "write", "fsync", "close"]


def test_sink_failure_path_emits_critical_and_stderr_never_raises():
    class _BoomFs(_FakeFs):
        def write(self, fd, data):
            raise OSError("disk full")

    fs = _BoomFs()
    events: list = []
    stderr: list = []
    sink = fs._sink(now_utc=lambda: datetime(2026, 6, 1, tzinfo=UTC),
                    on_event=events.append, stderr_write=stderr.append)
    sink(_tc("120"))                                          # must NOT raise

    assert len(events) == 1 and isinstance(events[0], TradeRecordWriteFailed)
    assert events[0].code == "TRADE_RECORD_WRITE_FAILED" and events[0].level == "CRITICAL"
    assert events[0].path == os.path.join("/records", "trades_2026.jsonl")
    assert "disk full" in events[0].error
    assert stderr and "TRADE_RECORD_WRITE_FAILED" in stderr[0]
    # the fd was still closed despite the write error (the finally ran).
    assert [o[0] for o in fs.ops] == ["open", "close"]


def test_sink_failure_event_escalates_to_a_c1_alert_through_the_logger():
    # wired through the real mod:Logger as on_event, the CRITICAL failure event escalates to the alert
    # seam (the C1 IMMEDIATE operator email) - and is NOT mistaken for a corpus TRADE_CLOSE.
    class _BoomFs(_FakeFs):
        def open(self, path, flags, mode):
            raise OSError("read-only fs")

    logger = Logger()
    fs = _BoomFs()
    sink = fs._sink(now_utc=lambda: datetime(2026, 6, 1, tzinfo=UTC),
                    on_event=logger.record, stderr_write=lambda _s: None)
    sink(_tc("120"))
    assert any(isinstance(a, TradeRecordWriteFailed) for a in logger.alerts)   # C1 escalation
    assert logger.corpus_for("default") == []                                  # not a corpus trade


# ----------------------------------------------------------------------------- the corpus read-back
def test_load_trade_records_round_trips_and_skips_blanks():
    lines = [serialize_trade_close(_tc("10")), "", "   ", serialize_trade_close(_tc("-20"))]
    recs = load_trade_records(lines)
    assert [r.net_pl_usd for r in recs] == [Decimal("10"), Decimal("-20")]
    assert all(isinstance(r, TradeClose) for r in recs)


def test_load_trade_records_file_with_injected_opener():
    blob = serialize_trade_close(_tc("5")) + "\n" + serialize_trade_close(_tc("6")) + "\n"
    recs = load_trade_records_file("/records/trades_2026.jsonl", open_text=lambda _p: blob.splitlines())
    assert [r.net_pl_usd for r in recs] == [Decimal("5"), Decimal("6")]


def test_load_records_dir_concatenates_years_in_order_missing_is_empty():
    store = {
        "/r/trades_2025.jsonl": serialize_trade_close(_tc("1")) + "\n",
        "/r/trades_2026.jsonl": serialize_trade_close(_tc("2")) + "\n",
    }

    def opener(path):
        return store.get(path, "").splitlines()

    # patch the file loader's opener by reading each year through the injected opener.
    from tothbot.recorder import trade_record_file as trf
    recs = []
    for year in (2025, 2026, 2027):
        recs += trf.load_trade_records_file(f"/r/trades_{year}.jsonl", open_text=opener)
    assert [r.net_pl_usd for r in recs] == [Decimal("1"), Decimal("2")]   # 2027 missing -> nothing


def test_serialize_carries_field_24_qty_and_round_trips():
    import json
    doc = json.loads(serialize_trade_close(_tc("120")))
    assert doc["qty"] == "0.05" and isinstance(doc["qty"], str)   # field 24, Decimal-as-string
    assert parse_trade_close(doc).qty == Decimal("0.05")          # parses back to Decimal


def test_serialize_carries_field_25_side_last_and_round_trips():
    # (25) side is the final canonical field (0500000 dv1_253, TB00762 ruling A) and survives the
    # NDJSON round-trip as the LONG | SHORT string (not a Decimal) - the per-module restore key.
    import json
    doc = json.loads(serialize_trade_close(_tc("120")))
    assert doc["side"] == "LONG" and isinstance(doc["side"], str)  # field 25, str enum
    assert list(doc)[-1] == "side"                                 # last in the canonical order
    assert parse_trade_close(doc).side == "LONG"                   # round-trips unchanged


def test_load_trade_records_by_side_partitions_the_durable_corpus(tmp_path):
    # the TB00762 ruling-A payoff: the durable (25) side field lets a cold-start rebuild the per-module
    # Long/Short CIATS corpus from disk - the runtime in-memory partition is otherwise lost on restart.
    from tothbot.recorder.trade_record_file import load_trade_records_by_side

    sink = PermanentTradeRecordSink(str(tmp_path), now_utc=lambda: datetime(2026, 6, 1, tzinfo=UTC))
    long_rec = _tc("120")                                  # side defaults to "LONG" in _tc
    short_rec = replace(_tc("90"), side="SHORT")
    sink(long_rec)
    sink(short_rec)
    pools = load_trade_records_by_side(str(tmp_path), [2026])
    assert [r.net_pl_usd for r in pools["LONG"]] == [Decimal("120")]
    assert [r.net_pl_usd for r in pools["SHORT"]] == [Decimal("90")]


def test_load_trade_records_by_side_drops_a_sideless_legacy_record(tmp_path):
    # a legacy pre-dv1_253 record (no side) cannot be attributed to a module -> dropped from BOTH side
    # pools (it still counts in the combined load_trade_records / C5 view, which is sideless by design).
    from tothbot.recorder.trade_record_file import load_trade_records_by_side

    sink = PermanentTradeRecordSink(str(tmp_path), now_utc=lambda: datetime(2026, 6, 1, tzinfo=UTC))
    sink(replace(_tc("120"), side=None))                   # a sideless legacy record
    pools = load_trade_records_by_side(str(tmp_path), [2026])
    assert pools["LONG"] == [] and pools["SHORT"] == []


def test_c5_from_durable_file_computes_proceeds_and_cost_basis(tmp_path):
    # the D1 payoff: C5 Form 8949 proceeds (exit_price*qty) + cost_basis (entry_fill_price*qty) now
    # compute from the durable record's field-24 qty.
    from tothbot.recorder.trade_record_file import build_c5_from_durable_file

    sink = PermanentTradeRecordSink(str(tmp_path), now_utc=lambda: datetime(2026, 6, 1, tzinfo=UTC))
    sink(_tc("120"))
    report = build_c5_from_durable_file(str(tmp_path), 2026)
    lot = report.per_module["all"].tax_lots[0]
    assert lot.qty == Decimal("0.05")
    assert lot.proceeds_usd == Decimal("66000.5") * Decimal("0.05")     # exit_price * qty
    assert lot.cost_basis_usd == Decimal("60000") * Decimal("0.05")     # entry_fill_price * qty


def test_load_trade_records_dir_real_roundtrip(tmp_path):
    # write two years via the sink to a REAL temp dir, then read them back with the dir loader.
    sink_2025 = PermanentTradeRecordSink(str(tmp_path), now_utc=lambda: datetime(2025, 5, 1, tzinfo=UTC))
    sink_2026 = PermanentTradeRecordSink(str(tmp_path), now_utc=lambda: datetime(2026, 5, 1, tzinfo=UTC))
    sink_2025(_tc("11"))
    sink_2026(_tc("22"))
    recs = load_trade_records_dir(str(tmp_path), [2025, 2026])
    assert [r.net_pl_usd for r in recs] == [Decimal("11"), Decimal("22")]
