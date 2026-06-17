"""TB00775 #1-B - the durable 5m-OHLC stream sink (recorder/ohlc_record_file.py).

Exercises the canonical JSONL serialize, the append-only os edge (open O_WRONLY|O_APPEND|O_CREAT,
fsync-per-write before close, no handle held), the per-candle UTC-year segmentation (by the candle's OWN
interval_begin, not the wall clock), and the local-catch failure path -> evt:OHLC_RECORD_WRITE_FAILED +
stderr (never raised). The low-level os edges are injected so it is unit-tested without real disk."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

from tothbot.exchange.candle_close import CommittedCandle
from tothbot.recorder.ohlc_record_file import (
    Ohlc5mRecordWriteFailed,
    PermanentOhlc5mSink,
    serialize_ohlc_5m,
)

# 2026-06-10T12:00:00Z = 1781438400 ; 2027-01-01T00:00:05Z = 1798761605 (the year-rollover candle)
_BEGIN_2026 = int(datetime(2026, 6, 10, 12, 0, 0, tzinfo=timezone.utc).timestamp())
_BEGIN_2027 = int(datetime(2027, 1, 1, 0, 0, 0, tzinfo=timezone.utc).timestamp())


def _candle(begin=_BEGIN_2026, symbol="BTC/USD"):
    return CommittedCandle(
        symbol=symbol, interval_begin=begin,
        open=Decimal("60000.0"), high=Decimal("60500.5"), low=Decimal("59900.25"),
        close=Decimal("60250.0"), volume=Decimal("12.3456"),
    )


class _FakeFs:
    def __init__(self):
        self.files: dict[str, bytearray] = {}
        self.ops: list = []
        self._fd_path: dict[int, str] = {}
        self._next_fd = 200

    def makedirs(self, path, *, exist_ok=False):
        self.ops.append(("makedirs", path, exist_ok))

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

    def sink(self, **kw):
        return PermanentOhlc5mSink(
            "/records", os_open=self.open, os_write=self.write, os_fsync=self.fsync,
            os_close=self.close, makedirs=self.makedirs, **kw)


def test_serialize_is_single_line_decimal_string_jsonl():
    line = serialize_ohlc_5m(_candle())
    assert "\n" not in line
    doc = json.loads(line)
    assert doc["symbol"] == "BTC/USD" and doc["interval_begin"] == _BEGIN_2026
    assert doc["ts"] == "2026-06-10T12:00:00+00:00"
    # every price/volume is a lossless STRING (ar:AR-047), never a float.
    assert doc["open"] == "60000.0" and doc["high"] == "60500.5" and doc["low"] == "59900.25"
    assert doc["close"] == "60250.0" and doc["volume"] == "12.3456"
    assert all(isinstance(doc[k], str) for k in ("open", "high", "low", "close", "volume"))


def test_append_fsync_close_per_candle_no_handle_held():
    fs = _FakeFs()
    sink = fs.sink()
    sink(_candle()); sink(_candle(symbol="ETH/USD"))
    kinds = [o[0] for o in fs.ops if o[0] in ("open", "write", "fsync", "close")]
    # each candle: open -> write -> fsync -> close, in order, twice (no handle held across candles).
    assert kinds == ["open", "write", "fsync", "close", "open", "write", "fsync", "close"]
    flags = next(o for o in fs.ops if o[0] == "open")[2]
    import os as _os
    assert flags & _os.O_APPEND and flags & _os.O_CREAT and flags & _os.O_WRONLY


def test_year_segmented_by_candle_interval_begin_not_wall_clock():
    fs = _FakeFs()
    sink = fs.sink()
    sink(_candle(_BEGIN_2026)); sink(_candle(_BEGIN_2027))
    paths = set(fs.files)
    assert any(p.endswith("ohlc5m_2026.jsonl") for p in paths)
    assert any(p.endswith("ohlc5m_2027.jsonl") for p in paths)
    # the 2026 candle landed in the 2026 file (the candle's own year, not 'now').
    body = bytes(fs.files[next(p for p in paths if p.endswith("2026.jsonl"))]).decode()
    assert json.loads(body.strip())["interval_begin"] == _BEGIN_2026


def test_write_failure_is_caught_and_surfaced_never_raised():
    fs = _FakeFs()
    events: list = []
    errs: list = []

    def boom(fd, data):
        raise OSError("disk full")

    sink = PermanentOhlc5mSink(
        "/records", os_open=fs.open, os_write=boom, os_fsync=fs.fsync, os_close=fs.close,
        makedirs=fs.makedirs, on_event=events.append, stderr_write=errs.append,
    )
    sink(_candle())  # must NOT raise
    assert len(events) == 1 and isinstance(events[0], Ohlc5mRecordWriteFailed)
    assert events[0].level == "WARNING" and "disk full" in events[0].error
    assert errs and "OHLC_RECORD_WRITE_FAILED" in errs[0]
