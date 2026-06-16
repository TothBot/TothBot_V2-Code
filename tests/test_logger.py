"""Tests: mod:Logger - the two-stream record membrane (recorder/logger.py).

Covers 0500000 dv1_250 sec 7: Stream-1 operational capture, Stream-2 per-module CIATS corpus
(only schema-valid TRADE_CLOSE, no cross-module pooling), the 24-field schema validation
(SCHEMA_FINGERPRINT_MISMATCH kept out of the corpus), and CRITICAL alert escalation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from tothbot.execution.exit_controller import ExitReason, TradeClose
from tothbot.recorder.logger import (
    TRADE_CLOSE_SCHEMA,
    Logger,
    SchemaFingerprintMismatch,
    validate_trade_close,
)


def _trade_close(symbol="BTC/USD", net="100"):
    """A schema-valid 24-field TRADE_CLOSE record (the real exit_controller dataclass)."""
    return TradeClose(
        symbol=symbol,
        entry_fill_price=Decimal("60000"), exit_price=Decimal("66000"),
        exit_reason=ExitReason.HTF_REGIME_REVERSAL,
        fees_entry_usd=Decimal("7.8"), fees_exit_usd=Decimal("8.58"), fees_total_usd=Decimal("16.38"),
        net_pl_usd=Decimal(net), net_gain_usd=Decimal(net), net_loss_usd=Decimal("0"),
    )


# -- the 24-field schema (dv1_252: field 24 qty added per the D1 ruling) ---

def test_schema_has_24_fields():
    assert len(TRADE_CLOSE_SCHEMA) == 24
    assert "qty" in TRADE_CLOSE_SCHEMA


def test_real_trade_close_passes_schema():
    assert validate_trade_close(_trade_close()) is None


def test_non_dataclass_record_fails_schema():
    fake = type("Fake", (), {"event": "TRADE_CLOSE"})()
    mismatch = validate_trade_close(fake)
    assert isinstance(mismatch, SchemaFingerprintMismatch)
    assert mismatch.missing == TRADE_CLOSE_SCHEMA


# -- two-stream routing --------------------------------------------------

def test_trade_close_enters_stream1_and_the_module_corpus():
    log = Logger()
    tc = _trade_close()
    log.record(tc, module="long")
    assert tc in log.operational              # Stream-1
    assert log.corpus_for("long") == [tc]     # Stream-2 (the long CIATS pool)
    assert log.corpus_for("short") == []      # no cross-module pooling


def test_per_module_pools_are_independent():
    log = Logger()
    long_tc = _trade_close("BTC/USD")
    short_tc = _trade_close("ETH/USD")
    log.record(long_tc, module="long")
    log.record(short_tc, module="short")
    assert log.corpus_for("long") == [long_tc]
    assert log.corpus_for("short") == [short_tc]


def test_non_trade_close_event_only_in_stream1():
    log = Logger()
    evt = type("Evt", (), {"event": "PAPER_LEDGER_UPDATED"})()
    log.record(evt, module="long")
    assert evt in log.operational
    assert log.corpus_for("long") == []       # not a trade outcome -> not in the corpus


def test_malformed_trade_close_is_kept_out_of_corpus():
    log = Logger()
    events: list = []
    log = Logger(on_event=events.append)
    bad = type("BadClose", (), {"event": "TRADE_CLOSE"})()  # claims TRADE_CLOSE but wrong schema
    log.record(bad, module="long")
    assert log.corpus_for("long") == []       # rejected - corpus never corrupted
    assert any(isinstance(e, SchemaFingerprintMismatch) for e in events)
    assert any(isinstance(r, SchemaFingerprintMismatch) for r in log.operational)


# -- CRITICAL escalation (HR-LG-007/009) --------------------------------

@dataclass(frozen=True)
class _Critical:
    level: str = field(default="CRITICAL")
    event: str = field(default="DRAWDOWN_HALT_TRIPPED")


def test_critical_event_is_escalated_to_alerts():
    alerts: list = []
    log = Logger(on_alert=alerts.append)
    crit = _Critical()
    log.record(crit)
    assert crit in log.alerts
    assert crit in alerts
    assert crit in log.operational            # also in Stream-1


def test_info_trade_close_is_not_an_alert():
    log = Logger()
    log.record(_trade_close(), module="long")
    assert log.alerts == []                    # INFO-level TRADE_CLOSE is not escalated


def test_alert_seam_routes_a_non_critical_record_to_the_operator():
    # HR-LG-009 explicit operator-surface push: a [HIGH] record (e.g. an HR-CI-011 approval request)
    # reaches the operator even though it is not CRITICAL (the level-driven path would miss it).
    alerts: list = []
    log = Logger(on_alert=alerts.append)
    req = type("ApprovalRequested", (), {"level": "HIGH", "code": "CIATS_APPROVAL_REQUESTED"})()
    log.alert(req)
    assert req in log.alerts
    assert req in alerts
    assert req not in log.operational          # the alert seam is the operator push, not a Stream-1 write
