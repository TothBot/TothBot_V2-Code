"""Unit tests: the Position Mirror (position_mirror.py).

Covers 0500000 dv1_241 mod:Position_Mirror + the WS-EXE-009 exec_type dispatch
(10 values), the cum_qty/avg_price authoritative fill fields (WS-EXE-012), the
AR-024 restated no-update rule, the AR-057 amended integrity alert, the AR-047
Decimal-no-float discipline, the AR-056 snap_orders gap-closed reconcile, and the
rule:HR-PM-009 sole-writer enforcement. PURE - no socket, no asyncio.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from tothbot.exchange.position_mirror import (
    WRITER_ID,
    ExecType,
    PositionAction,
    PositionClosedDuringGap,
    PositionMirror,
    PositionSide,
    PositionStateWrite,
    RestatedOrderAlert,
    SoleWriterViolation,
    SoleWriterViolationError,
    UnexpectedOrderAmended,
    UnknownExecType,
    classify_exec_type,
)


def _mirror():
    events: list = []
    return PositionMirror(on_event=events.append), events


def _fill(symbol="BTC/USD", side="buy", cum_qty="0.5", avg_price="60000.0", **extra):
    base = {"exec_type": "trade", "symbol": symbol, "side": side, "cum_qty": cum_qty,
            "avg_price": avg_price}
    base.update(extra)
    return base


# -- exec_type classification: all 10 + unknown (AR-023) ----------------

def test_all_ten_exec_types_classify():
    wire = ["pending_new", "new", "trade", "filled", "iceberg_refill", "canceled",
            "expired", "amended", "restated", "status"]
    assert [classify_exec_type(w) for w in wire] == list(ExecType)
    assert len(ExecType) == 10


def test_unknown_exec_type_is_logged_never_dropped():
    mirror, events = _mirror()
    outcome = mirror.apply_execution({"exec_type": "teleported"}, writer=WRITER_ID)
    assert outcome.action is PositionAction.UNKNOWN
    assert classify_exec_type("teleported") is None
    assert isinstance(events[-1], UnknownExecType)
    assert events[-1].raw_exec_type == "teleported"


# -- rule:HR-PM-009 sole-writer enforcement -----------------------------

def test_non_ws_manager_write_raises_and_emits_critical():
    mirror, events = _mirror()
    with pytest.raises(SoleWriterViolationError):
        mirror.apply_execution(_fill(), writer="Signal_Pipeline")
    assert isinstance(events[-1], SoleWriterViolation)
    assert events[-1].attempted_writer == "Signal_Pipeline"
    assert len(mirror) == 0  # nothing written


def test_snapshot_restore_also_sole_writer_guarded():
    mirror, _ = _mirror()
    with pytest.raises(SoleWriterViolationError):
        mirror.restore_from_snapshot([], writer="Exit_Controller")


def test_writer_id_is_ws_manager():
    assert WRITER_ID == "WS_Manager"


# -- fills: open / update / close (WS-EXE-012 cum_qty + avg_price) -------

def test_buy_fill_opens_long_position():
    mirror, events = _mirror()
    outcome = mirror.apply_execution(
        _fill(side="buy", cum_qty="0.5", avg_price="60000.0", cl_ord_id="cl-1", sequence=7),
        writer=WRITER_ID,
        regime_at_entry="TRENDING_POS_NORMAL",
        emergsl_id="sl-1",
    )
    assert outcome.action is PositionAction.OPENED
    pos = mirror.get("BTC/USD")
    assert pos.side is PositionSide.LONG
    assert pos.qty == Decimal("0.5")
    assert pos.avg_entry_price == Decimal("60000.0")
    assert pos.cl_ord_id == "cl-1"
    assert pos.emergsl_id == "sl-1"
    assert pos.fill_sequence_id == 7
    assert pos.regime_at_entry == "TRENDING_POS_NORMAL"
    write = next(e for e in events if isinstance(e, PositionStateWrite))
    assert write.action is PositionAction.OPENED
    assert write.writer_id == "WS_Manager"


def test_open_attaches_the_entry_side_producer_snapshot():
    # The sole writer attaches the contract:TRADE_CLOSE entry-side producer fields at the open fill.
    mirror, _events = _mirror()
    sp = {"rsi_14": Decimal(42), "ema_9": Decimal(101), "sss_pass": True, "side": "long"}
    outcome = mirror.apply_execution(
        _fill(side="buy"),
        writer=WRITER_ID,
        signal_params=sp,
        market_regime="TRENDING_POS_ELEVATED",
        entry_timestamp_utc="2026-06-15T00:00:00+00:00",
    )
    assert outcome.action is PositionAction.OPENED
    pos = mirror.get("BTC/USD")
    assert pos.signal_params == sp
    assert pos.market_regime == "TRENDING_POS_ELEVATED"
    assert pos.entry_timestamp_utc == "2026-06-15T00:00:00+00:00"


def test_open_without_producer_snapshot_defaults_to_none():
    mirror, _events = _mirror()
    mirror.apply_execution(_fill(side="buy"), writer=WRITER_ID)
    pos = mirror.get("BTC/USD")
    assert pos.signal_params is None
    assert pos.market_regime is None
    assert pos.entry_timestamp_utc is None


def test_sell_fill_opens_short_position():
    mirror, _ = _mirror()
    mirror.apply_execution(_fill(symbol="ETH/USD", side="sell"), writer=WRITER_ID)
    assert mirror.get("ETH/USD").side is PositionSide.SHORT


def test_opposite_side_fill_closes_long():
    mirror, events = _mirror()
    mirror.apply_execution(_fill(side="buy"), writer=WRITER_ID)
    outcome = mirror.apply_execution(
        _fill(side="sell", exec_type="filled", cum_qty="0.5", sequence=9), writer=WRITER_ID
    )
    assert outcome.action is PositionAction.CLOSED
    assert "BTC/USD" not in mirror
    assert len(mirror) == 0
    closed = [e for e in events if isinstance(e, PositionStateWrite)
              and e.action is PositionAction.CLOSED]
    assert closed and closed[0].symbol == "BTC/USD"


def test_buy_fill_closes_short():
    mirror, _ = _mirror()
    mirror.apply_execution(_fill(symbol="ETH/USD", side="sell"), writer=WRITER_ID)
    outcome = mirror.apply_execution(
        _fill(symbol="ETH/USD", side="buy", exec_type="filled"), writer=WRITER_ID
    )
    assert outcome.action is PositionAction.CLOSED
    assert "ETH/USD" not in mirror


def test_same_side_fill_updates_authoritative_cum_qty_avg_price():
    mirror, _ = _mirror()
    mirror.apply_execution(_fill(side="buy", cum_qty="0.3", avg_price="60000"), writer=WRITER_ID)
    outcome = mirror.apply_execution(
        _fill(side="buy", cum_qty="0.5", avg_price="60100"), writer=WRITER_ID
    )
    assert outcome.action is PositionAction.UPDATED
    pos = mirror.get("BTC/USD")
    assert pos.qty == Decimal("0.5")            # cumulative cum_qty wins
    assert pos.avg_entry_price == Decimal("60100")


# -- AR-047: Decimal, never float ---------------------------------------

def test_fill_fields_are_decimal_not_float():
    mirror, _ = _mirror()
    mirror.apply_execution(_fill(cum_qty=0.1, avg_price=60000.7), writer=WRITER_ID)
    pos = mirror.get("BTC/USD")
    assert isinstance(pos.qty, Decimal)
    assert isinstance(pos.avg_entry_price, Decimal)
    # Decimal(str(0.1)) == Decimal("0.1") - no binary-float contamination.
    assert pos.qty == Decimal("0.1")


# -- AR-024 restated: never mutates the mirror --------------------------

def test_restated_does_not_update_mirror():
    mirror, events = _mirror()
    mirror.apply_execution(_fill(side="buy"), writer=WRITER_ID)
    before = mirror.get("BTC/USD")
    outcome = mirror.apply_execution(
        {"exec_type": "restated", "symbol": "BTC/USD", "order_id": "o-1"}, writer=WRITER_ID
    )
    assert outcome.action is PositionAction.IGNORED
    assert mirror.get("BTC/USD") == before  # unchanged
    assert any(isinstance(e, RestatedOrderAlert) for e in events)  # elevated alert


def test_restated_with_no_position_is_silent_noop():
    mirror, events = _mirror()
    outcome = mirror.apply_execution(
        {"exec_type": "restated", "symbol": "BTC/USD"}, writer=WRITER_ID
    )
    assert outcome.action is PositionAction.IGNORED
    assert not any(isinstance(e, RestatedOrderAlert) for e in events)


# -- AR-057 amended: unexpected integrity alert, no mutation ------------

def test_amended_emits_critical_and_does_not_mutate():
    mirror, events = _mirror()
    mirror.apply_execution(_fill(side="buy"), writer=WRITER_ID)
    outcome = mirror.apply_execution(
        {"exec_type": "amended", "symbol": "BTC/USD", "order_id": "o-9"}, writer=WRITER_ID
    )
    assert outcome.action is PositionAction.ALERTED
    assert len(mirror) == 1  # position untouched
    amend = next(e for e in events if isinstance(e, UnexpectedOrderAmended))
    assert amend.order_id == "o-9"


# -- acknowledged-only exec_types: no position change -------------------

@pytest.mark.parametrize("exec_type", ["pending_new", "new", "canceled", "expired",
                                       "iceberg_refill", "status"])
def test_non_fill_exec_types_do_not_change_positions(exec_type):
    mirror, _ = _mirror()
    outcome = mirror.apply_execution(
        {"exec_type": exec_type, "symbol": "BTC/USD", "side": "buy"}, writer=WRITER_ID
    )
    assert outcome.action is PositionAction.IGNORED
    assert len(mirror) == 0


# -- malformed fill: never silently dropped -----------------------------

def test_fill_missing_cum_qty_is_flagged_not_dropped():
    mirror, events = _mirror()
    outcome = mirror.apply_execution(
        {"exec_type": "trade", "symbol": "BTC/USD", "side": "buy"}, writer=WRITER_ID
    )
    assert outcome.action is PositionAction.UNKNOWN
    assert any(isinstance(e, UnknownExecType) for e in events)
    assert len(mirror) == 0


# -- AR-056 snap_orders reconcile ---------------------------------------

def test_snapshot_reconcile_detects_gap_closed_positions():
    mirror, events = _mirror()
    mirror.apply_execution(_fill(symbol="BTC/USD", side="buy"), writer=WRITER_ID)
    mirror.apply_execution(_fill(symbol="ETH/USD", side="sell"), writer=WRITER_ID)
    # Snapshot shows only BTC still open: ETH closed during the disconnect gap.
    gap = mirror.restore_from_snapshot([{"symbol": "BTC/USD", "order_id": "sl-btc"}],
                                       writer=WRITER_ID)
    assert [g.symbol for g in gap] == ["ETH/USD"]
    assert isinstance(gap[0], PositionClosedDuringGap)
    assert gap[0].position.side is PositionSide.SHORT
    assert "ETH/USD" not in mirror          # gap-closed removed
    assert "BTC/USD" in mirror              # still-present retained verbatim
    assert any(isinstance(e, PositionClosedDuringGap) for e in events)


def test_snapshot_reconcile_retains_entry_price_for_open_positions():
    mirror, _ = _mirror()
    mirror.apply_execution(_fill(symbol="BTC/USD", side="buy", avg_price="60000"), writer=WRITER_ID)
    mirror.restore_from_snapshot([{"symbol": "BTC/USD"}], writer=WRITER_ID)
    # snap_orders cannot reconstruct avg_entry_price; the retained record keeps it.
    assert mirror.get("BTC/USD").avg_entry_price == Decimal("60000")


def test_empty_snapshot_gap_closes_everything():
    mirror, _ = _mirror()
    mirror.apply_execution(_fill(side="buy"), writer=WRITER_ID)
    gap = mirror.restore_from_snapshot([], writer=WRITER_ID)
    assert len(gap) == 1
    assert len(mirror) == 0


# -- read contract: helpers expose a frozen, copy-safe view -------------

def test_positions_returns_a_copy():
    mirror, _ = _mirror()
    mirror.apply_execution(_fill(side="buy"), writer=WRITER_ID)
    snapshot = mirror.positions()
    snapshot.clear()
    assert len(mirror) == 1  # mutating the returned dict does not touch the store


def test_position_record_is_frozen():
    mirror, _ = _mirror()
    mirror.apply_execution(_fill(side="buy"), writer=WRITER_ID)
    pos = mirror.get("BTC/USD")
    with pytest.raises(Exception):
        pos.qty = Decimal("99")  # frozen dataclass - read consumers cannot mutate


def test_open_symbols_view():
    mirror, _ = _mirror()
    mirror.apply_execution(_fill(symbol="BTC/USD", side="buy"), writer=WRITER_ID)
    mirror.apply_execution(_fill(symbol="ETH/USD", side="sell"), writer=WRITER_ID)
    assert mirror.open_symbols() == frozenset({"BTC/USD", "ETH/USD"})


# -- D6 entry-time snapshot fields (dv1_242) + the sole-writer close ----------

def test_open_records_entry_time_snapshot_fields():
    mirror, _ = _mirror()
    mirror.apply_execution(
        _fill(side="buy"), writer=WRITER_ID, atr_14_entry="2000.5", emergsl_price="54000",
    )
    pos = mirror.get("BTC/USD")
    # Decimal-on-receipt (ar:AR-047), never live-recomputed
    assert pos.atr_14_entry == Decimal("2000.5")
    assert pos.emergsl_price == Decimal("54000")


def test_snapshot_fields_default_none_when_not_supplied():
    mirror, _ = _mirror()
    mirror.apply_execution(_fill(side="buy"), writer=WRITER_ID)
    pos = mirror.get("BTC/USD")
    assert pos.atr_14_entry is None and pos.emergsl_price is None


def test_close_clears_and_emits_state_write():
    mirror, events = _mirror()
    mirror.apply_execution(_fill(side="buy"), writer=WRITER_ID)
    cleared = mirror.close("BTC/USD", writer=WRITER_ID)
    assert cleared is not None and cleared.symbol == "BTC/USD"
    assert not mirror.has_position("BTC/USD")
    assert isinstance(events[-1], PositionStateWrite)
    assert events[-1].action is PositionAction.CLOSED


def test_close_absent_symbol_returns_none():
    mirror, _ = _mirror()
    assert mirror.close("BTC/USD", writer=WRITER_ID) is None


def test_close_is_sole_writer_guarded():
    mirror, _ = _mirror()
    mirror.apply_execution(_fill(side="buy"), writer=WRITER_ID)
    with pytest.raises(SoleWriterViolationError):
        mirror.close("BTC/USD", writer="Exit_Controller")
    assert mirror.has_position("BTC/USD")  # the guarded write never happened
