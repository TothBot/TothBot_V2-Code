"""mod:Logger - the two-stream record membrane and the SOLE CIATS data source.

Source: 0500000 dv1_250 sec 7 mod:Logger + contract:Two_Stream_Record_Architecture +
contract:CIATS_Trade_Outcome_Bus + ar:AR-014 (async bounded queue) + the 23-field evt:TRADE_
CLOSE schema (sec 7 Image6, line "23-field schema {...}") + rule:HR-LG-007/009 (CRITICAL
escalation + SMTP alert).

mod:Logger is the single membrane every event flows through; it is the ONLY component CIATS
reads from (no other CIATS data source exists). It writes TWO streams:

  Stream-1  OPERATIONAL  - every emitted event, in order (the tothbot.log operational record).
  Stream-2  TRADE OUTCOME (contract:CIATS_Trade_Outcome_Bus) - the durable closed-trade corpus
            CIATS learns from. ONLY evt:TRADE_CLOSE records enter it, and ONLY after passing the
            23-FIELD SCHEMA VALIDATION (a record that fails is logged SCHEMA_FINGERPRINT_MISMATCH
            and kept OUT of the corpus - never silently dropped, never corrupting the corpus).

PER-MODULE pools (sec 7 / line 373: "CIATS is a PER-MODULE framework ... each with its own
statistical pool ... sharing only the mod:Logger / CIATS_Trade_Outcome_Bus membrane; no
cross-module pooling"). The membrane is shared; the Stream-2 corpus is partitioned by the
emitting module (Long / Short), so each side's CIATS instance reads only its own outcomes. The
emitter tags each record with its module (the side), since the 23-field schema itself carries
no side field - the partition IS the side.

CRITICAL escalation (rule:HR-LG-007/009): a CRITICAL-level event is additionally routed to the
alert sink (the contract:Operator_Reporting_Hierarchy C1 IMMEDIATE set -> SMTP to the operator).

PURE membrane logic (the async bounded queue AR-014 is the transport edge; this is the routing
+ validation that plugs into it). Sinks are injected so it is unit-testable without I/O.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields
from typing import Callable

# The 23 canonical evt:TRADE_CLOSE field names (0500000 sec 7 Image6 schema). A Stream-2 record
# MUST carry EXACTLY these (the schema fingerprint). Mirrors execution.exit_controller.TradeClose.
TRADE_CLOSE_SCHEMA: frozenset[str] = frozenset({
    "ts", "event", "level", "component", "symbol",
    "entry_fill_price", "exit_price", "entry_timestamp_utc", "exit_timestamp_utc",
    "hold_candle_count", "mae_pct_reached", "fees_entry_usd", "fees_exit_usd", "fees_total_usd",
    "exit_reason", "asset_regime", "vol_regime", "market_regime", "signal_params", "actual_rr",
    "net_pl_usd", "net_gain_usd", "net_loss_usd",
})

# The default per-module corpus partitions (the two trading modules). A record's module tag keys
# its Stream-2 pool; an untagged record falls to the "default" pool.
EventSink = Callable[[object], None]


@dataclass(frozen=True)
class SchemaFingerprintMismatch:
    """SCHEMA_FINGERPRINT_MISMATCH [WARNING] - an evt:TRADE_CLOSE failed the 23-field schema
    validation (missing or extra fields); kept OUT of the CIATS corpus, surfaced not dropped."""

    missing: frozenset
    extra: frozenset
    code: str = field(default="SCHEMA_FINGERPRINT_MISMATCH", init=False)


def _field_names(record: object) -> frozenset[str] | None:
    """The dataclass field-name set of a record, or None if it is not a dataclass instance."""
    try:
        return frozenset(f.name for f in fields(record))
    except TypeError:
        return None


def validate_trade_close(record: object) -> SchemaFingerprintMismatch | None:
    """Validate a TRADE_CLOSE record against the 23-field schema. Returns None if it matches
    exactly, else a SchemaFingerprintMismatch naming the missing + extra fields."""
    names = _field_names(record)
    if names is None:
        return SchemaFingerprintMismatch(missing=TRADE_CLOSE_SCHEMA, extra=frozenset())
    if names == TRADE_CLOSE_SCHEMA:
        return None
    return SchemaFingerprintMismatch(
        missing=TRADE_CLOSE_SCHEMA - names, extra=names - TRADE_CLOSE_SCHEMA,
    )


class Logger:
    """The two-stream record membrane (mod:Logger). One per process; the SOLE CIATS data source.

    Stream-1 (operational) captures every record; Stream-2 (the per-module CIATS_Trade_Outcome_
    Bus corpus) captures schema-valid TRADE_CLOSE records keyed by the emitting module. CRITICAL
    records are escalated to the alert sink. The async-bounded-queue transport (AR-014) is the
    injected edge; this object is the routing + validation."""

    def __init__(self, *, on_event: EventSink | None = None, on_alert: EventSink | None = None) -> None:
        self.operational: list = []                  # Stream-1
        self.corpus: dict[str, list] = {}            # Stream-2, per-module pools
        self.alerts: list = []                        # CRITICAL escalation (HR-LG-007/009)
        self._on_event = on_event
        self._on_alert = on_alert

    def _emit(self, event: object) -> None:
        if self._on_event is not None:
            self._on_event(event)

    def record(self, record: object, *, module: str = "default") -> None:
        """Route one emitted record. Stream-1 always; a TRADE_CLOSE additionally enters the
        module's Stream-2 corpus IFF it passes the 23-field schema (else SCHEMA_FINGERPRINT_
        MISMATCH, kept out); a CRITICAL-level record is escalated to the alert sink. `module`
        tags the per-module CIATS pool (the side - the schema carries no side field)."""
        self.operational.append(record)

        if getattr(record, "event", None) == "TRADE_CLOSE" or getattr(record, "code", None) == "TRADE_CLOSE":
            mismatch = validate_trade_close(record)
            if mismatch is None:
                self.corpus.setdefault(module, []).append(record)
            else:
                self.operational.append(mismatch)
                self._emit(mismatch)

        if _is_critical(record):
            self.alerts.append(record)
            if self._on_alert is not None:
                self._on_alert(record)

    def alert(self, record: object) -> None:
        """The HR-LG-009 operator-alert seam (the contract:Operator_Reporting_Hierarchy C1 IMMEDIATE
        surface -> SMTP to the operator). Routes a record to the operator REGARDLESS of its level -
        the level-driven CRITICAL auto-escalation in record() is the passive path; this is the
        EXPLICIT operator-surface push for an event that needs the operator's attention even though it
        is not CRITICAL (e.g. an HR-CI-011 evt:CIATS_APPROVAL_REQUESTED [HIGH] awaiting Bill's
        decision). Surfaced, never dropped."""
        self.alerts.append(record)
        if self._on_alert is not None:
            self._on_alert(record)

    def set_alert_sink(self, on_alert: EventSink | None) -> None:
        """Rebind the HR-LG-009 operator-alert sink. The cold-start runner wires the real SMTP alert
        send here AFTER both the Logger and the sender are constructed (the sender's evt:ALERT_SENT /
        evt:ALERT_SMTP_FAILED route back through this Logger's on_event, so the two are mutually
        dependent - this setter resolves the construction cycle)."""
        self._on_alert = on_alert

    def corpus_for(self, module: str) -> list:
        """The module's Stream-2 CIATS corpus (the closed-trade outcomes that side has produced).
        Empty if the module has no closed trades yet. No cross-module pooling (sec 7)."""
        return self.corpus.get(module, [])


def _is_critical(record: object) -> bool:
    """A record is CRITICAL when its level field is CRITICAL (rule:HR-LG-007 escalation)."""
    return getattr(record, "level", None) == "CRITICAL"
