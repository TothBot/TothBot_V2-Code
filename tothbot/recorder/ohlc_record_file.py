"""The durable append-only 5m-OHLC stream sink (TB00775 #1-B).

Source: the TB00775 stop/exit FP-DP analysis - a faithful realized backtest (the real entry signal + the
MAE-stop vs reversal exit race) needs INTRADAY 5m history, but Kraken public OHLC caps at ~720 bars
(~2.5 days), so the only way to a DEEP backtest corpus is to persist the organism's own live 5m stream.
THIS is that sink: a callable handed each genuinely-closed 5m CommittedCandle (mod:LiveSweepDriver, one
call per new close), durably appended to ohlc5m_<YYYY>.jsonl under the records dir.

Mirrors recorder/trade_record_file.PermanentTradeRecordSink EXACTLY (rule:HR-LG-013 discipline): append-
only os.open(O_WRONLY|O_APPEND|O_CREAT), fsync-per-write, NO handle held across candles, the year segment
taken from the CANDLE's own interval_begin (so a backfilled/replayed candle lands in the right year file,
not the wall clock). A write failure is caught locally (evt:OHLC_RECORD_WRITE_FAILED + stderr), NEVER
raised - a persistence hiccup must never crash the 5m pipeline. The low-level os edges are injected (the
real os by default) so the sink is unit-tested without disk. PURE-ish save the lone file edge."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal


def _s(value: object) -> str:
    """Decimal/number -> a lossless string (ar:AR-047: never float-format a financial value)."""
    return str(value) if not isinstance(value, Decimal) else format(value, "f")


def serialize_ohlc_5m(candle: object) -> str:
    """One CommittedCandle -> a JSONL line: the symbol, the interval_begin (Unix-second candle start),
    its ISO 8601 UTC stamp, and the OHLCV as lossless strings. Deterministic field order."""
    begin = int(getattr(candle, "interval_begin"))
    doc = {
        "ts": datetime.fromtimestamp(begin, tz=timezone.utc).isoformat(),
        "interval_begin": begin,
        "symbol": str(getattr(candle, "symbol")),
        "open": _s(getattr(candle, "open")),
        "high": _s(getattr(candle, "high")),
        "low": _s(getattr(candle, "low")),
        "close": _s(getattr(candle, "close")),
        "volume": _s(getattr(candle, "volume")),
    }
    return json.dumps(doc, separators=(",", ":"))


@dataclass(frozen=True)
class Ohlc5mRecordWriteFailed:
    """evt:OHLC_RECORD_WRITE_FAILED [WARNING] {path, error}. The durable 5m sink failed to persist a
    candle - surfaced + stderr'd, NEVER raised. WARNING (not CRITICAL like the trade sink): a dropped
    5m bar is a backtest-corpus gap, not a lost trade/tax record, so it does not trip the C1 alert."""

    path: str
    error: str
    level: str = field(default="WARNING", init=False)
    component: str = field(default="LOGGER", init=False)
    code: str = field(default="OHLC_RECORD_WRITE_FAILED", init=False)


class PermanentOhlc5mSink:
    """The durable append-only 5m-OHLC sink. CALLABLE with a closed CommittedCandle; appends one JSONL
    line to ohlc5m_<YYYY>.jsonl (the candle's own UTC year), fsync-per-write, no handle held. The os
    edges + stderr are injected (the real os by default) so it is unit-tested without disk."""

    _FLAGS = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_BINARY", 0)
    _MODE = 0o644

    def __init__(
        self,
        records_dir: str,
        *,
        on_event: Callable[[object], None] | None = None,
        stderr_write: Callable[[str], object] | None = None,
        os_open: Callable[..., int] = os.open,
        os_write: Callable[[int, bytes], int] = os.write,
        os_fsync: Callable[[int], None] = os.fsync,
        os_close: Callable[[int], None] = os.close,
        makedirs: Callable[..., None] = os.makedirs,
    ) -> None:
        self._dir = records_dir
        self._on_event = on_event
        self._stderr_write = stderr_write or sys.stderr.write
        self._open = os_open
        self._write = os_write
        self._fsync = os_fsync
        self._close = os_close
        makedirs(self._dir, exist_ok=True)

    def path_for(self, interval_begin: int) -> str:
        """The durable file path for a candle's UTC year: ohlc5m_<YYYY>.jsonl."""
        year = datetime.fromtimestamp(int(interval_begin), tz=timezone.utc).strftime("%Y")
        return os.path.join(self._dir, f"ohlc5m_{year}.jsonl")

    def __call__(self, candle: object) -> None:
        """Durably append one closed 5m CommittedCandle. open/write/fsync/close per record (no handle
        across candles; the annual rollover is implicit in the per-candle path). Any failure is caught
        locally -> evt:OHLC_RECORD_WRITE_FAILED + stderr; never raised (must not crash the 5m clock)."""
        path = "<unresolved>"
        try:
            begin = int(getattr(candle, "interval_begin"))
            path = self.path_for(begin)
            data = (serialize_ohlc_5m(candle) + "\n").encode("utf-8")
            fd = self._open(path, self._FLAGS, self._MODE)
            try:
                self._write(fd, data)
                self._fsync(fd)
            finally:
                self._close(fd)
        except Exception as exc:  # noqa: BLE001 - never raise out of the 5m pipeline
            failure = Ohlc5mRecordWriteFailed(path=path, error=f"{type(exc).__name__}: {exc}")
            self._stderr_write(f"OHLC_RECORD_WRITE_FAILED path={path} error={failure.error}\n")
            if self._on_event is not None:
                self._on_event(failure)
