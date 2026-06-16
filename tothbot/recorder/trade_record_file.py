"""rule:HR-LG-013 - the permanent trade-record FILE sink (the durable Stream-2 corpus).

Source: 0500000 dv1_251 sec 7 mod:Logger desc q5_logs "PERMANENT TRADE-RECORD FILE [rule:HR-LG-013;
Stream 2 durable sink, the authoritative CIATS-inference and tax/audit corpus]" + contract:Two_Stream_
Record_Architecture. The in-memory mod:Logger Stream-2 corpus (recorder/logger.py) is the runtime
learning substrate, but the finite-retention tothbot.log self-deletes on rotation and would PERMANENTLY
LOSE closed trades; THIS module is the second, INDEPENDENT, append-only sink that makes the closed-trade
corpus durable - the authoritative source for the CIATS 200/600-trade floors AND IRS Form 8949 / 26 CFR
1.6001-1 tax recordkeeping.

THE CONTRACT (verbatim intent, built as the diagram literal):
  - path /home/tothbot/records/trades_<YYYY>.jsonl; the records dir is created at startup (makedirs
    exist_ok=True).
  - filter event=="TRADE_CLOSE" ONLY (every other event is a no-op for this sink).
  - one valid JSON object per line (NDJSON) + a trailing newline; ALL numeric (price/qty/fee/P-L/ratio)
    values are JSON STRINGS, never JSON float (Decimal-as-string, ar:AR-047) - CIATS parses them back to
    Decimal on read; the mandatory fields ts/event/level/component lead the record.
  - open append-only via os.open O_WRONLY|O_APPEND|O_CREAT (never truncate/seek/rewrite), file mode
    0o644 pinned on create; os.fsync(fd) after EACH os.write and BEFORE os.close (durability per write);
    a single os.write per record (~500-800 bytes, atomic under PIPE_BUF); no open handle held across
    events (open/write/fsync/close per record - this also handles the annual UTC year rollover).
  - ANNUAL UTC SEGMENTATION: <YYYY> = the close instant's UTC year; the first TRADE_CLOSE of the next
    year opens trades_<YYYY+1>.jsonl. NO rotation, NO size cap, NO TTL (the whole reason the sink
    exists).
  - FAILURE PATH: any exception in path construction / open / write / fsync / close is caught LOCALLY
    (never raised out - it would crash the writer thread); emit evt:TRADE_RECORD_WRITE_FAILED CRITICAL
    {path, error} (a C1 IMMEDIATE operator email per rule:HR-RPT-001, routed by the CRITICAL level
    through the mod:Logger alert seam) and write the same detail to stderr as the ultimate fallback; the
    in-memory / tothbot.log write of the original TRADE_CLOSE is UNAFFECTED (handler-local isolation).

The low-level os edges (open/write/fsync/close/makedirs) + the UTC clock + the stderr write are INJECTED
so the sink is unit-testable without real disk I/O (defaults are the real os functions). serialize_trade_
close + parse_trade_close are the canonical NDJSON wire-format for the durable Stream-2 record (when the
tothbot.log NDJSON serializer is built it MUST share this serializer so the two sinks stay byte-identical
per the contract). orjson is the diagram's named serializer; it is not installed here, so stdlib json
with a Decimal-as-string default is used - the WIRE FORMAT (one JSON object per line, every numeric a
string, parsed back to Decimal) is the architecture, the library is the SSS implementation choice.
"""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal

from ..execution.exit_controller import ExitReason, TradeClose

# The 23-field canonical order (sec 7 Image6 enumeration 1..23): the mandatory record-identity fields
# (ts/event/level/component) lead, then the schema fields in figure order. json.dumps preserves dict
# insertion order, so this is the on-disk field order.
_CANONICAL_ORDER: tuple[str, ...] = (
    "ts", "event", "level", "component", "symbol",
    "entry_fill_price", "exit_price", "entry_timestamp_utc", "exit_timestamp_utc",
    "hold_candle_count", "mae_pct_reached", "fees_entry_usd", "fees_exit_usd", "fees_total_usd",
    "exit_reason", "asset_regime", "vol_regime", "market_regime", "signal_params", "actual_rr",
    "net_pl_usd", "net_gain_usd", "net_loss_usd", "qty",
)
# The price/fee/P-L/ratio/qty fields parsed BACK to Decimal on read (the JSON-string numerics).
_DECIMAL_FIELDS: frozenset[str] = frozenset({
    "entry_fill_price", "exit_price", "mae_pct_reached", "fees_entry_usd", "fees_exit_usd",
    "fees_total_usd", "actual_rr", "net_pl_usd", "net_gain_usd", "net_loss_usd", "qty",
})

DurableRecordsDir = str
RecordsEvent = Callable[[object], None]
StderrWrite = Callable[[str], None]


def _jsonable(value: object) -> object:
    """Render one value to its NDJSON form: a Decimal -> its exact str (ar:AR-047, never a float); an
    Enum -> its value; a dict (signal_params) -> recursively jsonable; None / str / int pass through."""
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, ExitReason):
        return value.value
    if isinstance(value, dict):
        return {k: _jsonable(v) for k, v in value.items()}
    return value


def serialize_trade_close(record: object) -> str:
    """Serialize a TRADE_CLOSE record to its canonical single-line NDJSON form (NO trailing newline -
    the sink adds it). Decimal-as-string throughout; the mandatory fields lead; the field order is the
    23-field figure enumeration. This is the durable Stream-2 wire-format."""
    doc = {name: _jsonable(getattr(record, name, None)) for name in _CANONICAL_ORDER}
    return json.dumps(doc, separators=(",", ":"))


def parse_trade_close(doc: dict) -> TradeClose:
    """Reconstruct a TradeClose from a parsed NDJSON object: the JSON-string numerics are parsed back
    to Decimal (ar:AR-047), exit_reason back to the ExitReason enum, signal_params back to a Decimal
    dict. event/level/component are constant init=False fields (not passed). The inverse of serialize_
    trade_close."""
    def dec(name: str) -> Decimal | None:
        raw = doc.get(name)
        return None if raw is None else Decimal(str(raw))

    sp = doc.get("signal_params")
    return TradeClose(
        symbol=doc["symbol"],
        entry_fill_price=dec("entry_fill_price"),
        exit_price=dec("exit_price"),
        exit_reason=ExitReason(doc["exit_reason"]),
        fees_entry_usd=dec("fees_entry_usd"),
        fees_exit_usd=dec("fees_exit_usd"),
        fees_total_usd=dec("fees_total_usd"),
        net_pl_usd=dec("net_pl_usd"),
        net_gain_usd=dec("net_gain_usd"),
        net_loss_usd=dec("net_loss_usd"),
        ts=doc.get("ts"),
        entry_timestamp_utc=doc.get("entry_timestamp_utc"),
        exit_timestamp_utc=doc.get("exit_timestamp_utc"),
        hold_candle_count=doc.get("hold_candle_count"),
        mae_pct_reached=dec("mae_pct_reached"),
        asset_regime=doc.get("asset_regime"),
        vol_regime=doc.get("vol_regime"),
        market_regime=doc.get("market_regime"),
        signal_params=({k: Decimal(str(v)) for k, v in sp.items()} if sp else None),
        actual_rr=dec("actual_rr"),
        qty=dec("qty"),
    )


# =================================================================== evt:TRADE_RECORD_WRITE_FAILED
@dataclass(frozen=True)
class TradeRecordWriteFailed:
    """evt:TRADE_RECORD_WRITE_FAILED [CRITICAL] {path, error} (rule:HR-LG-010). The durable Stream-2
    sink failed to persist a closed trade - a C1 IMMEDIATE condition (the trade is lost from the
    authoritative CIATS + tax corpus). The CRITICAL level routes it through the mod:Logger alert seam
    to the operator; the original TRADE_CLOSE write is unaffected (handler-local isolation)."""

    path: str
    error: str
    level: str = field(default="CRITICAL", init=False)
    component: str = field(default="LOGGER", init=False)
    code: str = field(default="TRADE_RECORD_WRITE_FAILED", init=False)


def _is_trade_close(event: object) -> bool:
    return getattr(event, "event", None) == "TRADE_CLOSE"


# ============================================================================ the durable file sink
class PermanentTradeRecordSink:
    """rule:HR-LG-013 - the durable append-only Stream-2 trade-record sink. CALLABLE, so it chains as a
    `downstream` of the per-module CIATS learning sink: every event flows in, only a TRADE_CLOSE is
    durably appended to trades_<YYYY>.jsonl (the close instant's UTC year), fsync-per-write, no handle
    held across events. The low-level os edges + the UTC clock + the stderr write are injected (the real
    os by default) so the sink is unit-testable without disk."""

    def __init__(
        self,
        records_dir: DurableRecordsDir,
        *,
        now_utc: Callable[[], datetime] | None = None,
        on_event: RecordsEvent | None = None,
        stderr_write: StderrWrite | None = None,
        os_open: Callable[..., int] = os.open,
        os_write: Callable[[int, bytes], int] = os.write,
        os_fsync: Callable[[int], None] = os.fsync,
        os_close: Callable[[int], None] = os.close,
        makedirs: Callable[..., None] = os.makedirs,
    ) -> None:
        self._dir = records_dir
        self._now_utc = now_utc or (lambda: datetime.now(timezone.utc))
        self._on_event = on_event
        self._stderr_write = stderr_write or sys.stderr.write
        self._open = os_open
        self._write = os_write
        self._fsync = os_fsync
        self._close = os_close
        # The records directory is created at startup (exist_ok=True) - the open path always exists.
        makedirs(self._dir, exist_ok=True)

    # The append-only create flags (O_BINARY is OR'd where present so "\n" is never CRLF-translated -
    # a no-op 0 on POSIX, byte fidelity on Windows). Mode 0o644 is pinned on CREATE.
    _FLAGS = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_BINARY", 0)
    _MODE = 0o644

    def path_for(self, when: datetime) -> str:
        """The durable file path for a close instant's UTC year: trades_<YYYY>.jsonl."""
        year = when.astimezone(timezone.utc).strftime("%Y")
        return os.path.join(self._dir, f"trades_{year}.jsonl")

    def __call__(self, event: object) -> None:
        """Durably append a TRADE_CLOSE (a no-op for any other event). open/write/fsync/close per
        record - no handle held across events; the annual year rollover is implicit in the per-record
        path. Any failure is caught locally -> evt:TRADE_RECORD_WRITE_FAILED + stderr; never raised."""
        if not _is_trade_close(event):
            return
        path = "<unresolved>"
        try:
            path = self.path_for(self._now_utc())
            data = (serialize_trade_close(event) + "\n").encode("utf-8")
            fd = self._open(path, self._FLAGS, self._MODE)
            try:
                self._write(fd, data)          # single atomic write (~500-800 bytes < PIPE_BUF)
                self._fsync(fd)                 # durability per write (survives power loss / panic)
            finally:
                self._close(fd)                 # no handle held across events
        except Exception as exc:                # never raise out - would crash the writer thread
            self._on_write_failed(path, exc)

    def _on_write_failed(self, path: str, exc: Exception) -> None:
        failure = TradeRecordWriteFailed(path=path, error=f"{type(exc).__name__}: {exc}")
        # stderr is the ultimate fallback observability surface (always written).
        self._stderr_write(f"TRADE_RECORD_WRITE_FAILED path={path} error={failure.error}\n")
        # the CRITICAL event routes to the operator through the mod:Logger alert seam (a C1 email).
        if self._on_event is not None:
            self._on_event(failure)


# ============================================================================ the corpus read-back
def load_trade_records(lines: Iterable[str]) -> list[TradeClose]:
    """Parse a durable JSONL stream back into TradeClose records (the cold-start corpus restore +
    the C5 ANNUAL authoritative read). Skips blank lines; each non-blank line is one NDJSON object.
    NOTE: the 23-field schema carries NO side field (the per-module partition IS the emitting side, sec
    7), so this restores the COMBINED corpus (the C5 / tax / total-floor source); per-module Long/Short
    restoration would need a side field the durable schema does not carry (a documented gap)."""
    out: list[TradeClose] = []
    for line in lines:
        s = line.strip()
        if not s:
            continue
        out.append(parse_trade_close(json.loads(s)))
    return out


def load_trade_records_file(
    path: str,
    *,
    open_text: Callable[[str], Iterable[str]] | None = None,
) -> list[TradeClose]:
    """Load + parse one durable trades_<YYYY>.jsonl file into TradeClose records. `open_text` is
    injected for tests; the default reads the real file (utf-8). A missing file -> empty (a year with
    no closed trades is not an error)."""
    if open_text is not None:
        return load_trade_records(open_text(path))
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as fh:
        return load_trade_records(fh)


def load_trade_records_dir(records_dir: str, years: Sequence[int]) -> list[TradeClose]:
    """Load + concatenate the durable corpus across the given UTC years (e.g. the C5 ANNUAL single
    year, or the multi-year total-floor restore), in year order. Each year reads its trades_<YYYY>.
    jsonl; a missing year file contributes nothing."""
    out: list[TradeClose] = []
    for year in years:
        out.extend(load_trade_records_file(os.path.join(records_dir, f"trades_{year}.jsonl")))
    return out


# C5 reads from the durable file as its authoritative source (the module label for the combined,
# sideless C5 view - see build_c5_from_durable_file).
C5_DURABLE_MODULE = "all"


def build_c5_from_durable_file(
    records_dir: str,
    year: int,
    *,
    parameter_stores: "dict | None" = None,
    as_of: datetime | None = None,
):
    """Build the C5 ANNUAL OperatorReport from the durable trades_<year>.jsonl - the authoritative
    source per rule:HR-LG-013 (the contract: "C5 reads from the permanent trade-record file"). Loads
    the calendar-year records off disk + views them through the report builder as ONE combined corpus:
    the durable schema carries NO side field (the per-module partition IS the emitting side, sec 7), so
    Long/Short is not recoverable from the file - and C5 IS the combined calendar-year compliance + IRS
    Form 8949 view (all trades), which the combined roll-up + the single 'all' module's tax lots give
    exactly. Independent of the live in-memory corpus (a cold-start / audit can build C5 from disk
    alone)."""
    from .reporting import ReportCategory, build_operator_report  # local: avoid an import cycle

    from types import SimpleNamespace

    records = load_trade_records_dir(records_dir, [year])
    when = as_of or datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
    logger_view = SimpleNamespace(corpus={C5_DURABLE_MODULE: records}, operational=list(records))
    return build_operator_report(
        logger_view, parameter_stores or {}, category=ReportCategory.C5_ANNUAL,
        as_of=when, modules=(C5_DURABLE_MODULE,),
    )
