"""mod:CIATS_Parameter_Store tests (ciats/parameter_store.py).

Covers apply() writing an approved change + the evolution log + last-change bookkeeping, the
HR-CI-002 immutability invariant (the sacred R:R + an exchange-defined param are never written),
the HR-CI-005 50-trade interval (defense-in-depth), trades_since_last_change, and the frozen
per-cycle snapshot (read-only + a copy that a later write cannot perturb).
"""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

import pytest

from tothbot.ciats.parameter_store import (
    ParameterStore,
    ParameterWriteRejected,
    ParameterWritten,
)
from tothbot.ciats.pdca_engine import MIN_TRADES_BETWEEN_CHANGES


def _change(param_name, proposed_value):
    # A minimal stand-in for a PDCA ApprovedChange (the store reads .proposal.param_name/.proposed_value).
    return SimpleNamespace(proposal=SimpleNamespace(param_name=param_name, proposed_value=proposed_value))


# --------------------------------------------------------------------------- apply / evolution
def test_apply_writes_value_and_records_evolution():
    store = ParameterStore(initial={"mae_mult": Decimal("1.5")})
    out = store.apply(_change("mae_mult", Decimal("1.6")), at_trade_count=250)
    assert isinstance(out, ParameterWritten)
    assert store.get("mae_mult") == Decimal("1.6")
    assert len(store.evolution_log) == 1
    rec = store.evolution_log[0]
    assert (rec.param_name, rec.old_value, rec.new_value, rec.at_trade_count) == (
        "mae_mult", Decimal("1.5"), Decimal("1.6"), 250,
    )
    assert store.last_change_trade_count == 250


def test_get_returns_initial_values():
    store = ParameterStore(initial={"adx_threshold": 25})
    assert store.get("adx_threshold") == 25
    assert store.get("missing") is None


# --------------------------------------------------------------------------- immutability (HR-CI-002)
def test_sacred_rr_is_never_written():
    store = ParameterStore(initial={"rr_floor": Decimal("1.5")})
    out = store.apply(_change("rr_floor", Decimal("1.2")), at_trade_count=300)
    assert isinstance(out, ParameterWriteRejected)
    assert store.get("rr_floor") == Decimal("1.5")  # unchanged
    assert store.evolution_log == ()


def test_exchange_defined_param_is_immutable():
    store = ParameterStore(initial={"fee_taker_pct": Decimal("0.0026")}, immutable={"fee_taker_pct"})
    assert store.is_immutable("fee_taker_pct") is True
    out = store.apply(_change("fee_taker_pct", Decimal("0.0030")), at_trade_count=300)
    assert isinstance(out, ParameterWriteRejected)
    assert store.get("fee_taker_pct") == Decimal("0.0026")


# --------------------------------------------------------------------------- 50-trade interval (HR-CI-005)
def test_second_change_within_50_trades_rejected():
    store = ParameterStore(initial={"mae_mult": Decimal("1.5")})
    assert isinstance(store.apply(_change("mae_mult", Decimal("1.6")), at_trade_count=250), ParameterWritten)
    out = store.apply(_change("mae_mult", Decimal("1.7")), at_trade_count=250 + MIN_TRADES_BETWEEN_CHANGES - 1)
    assert isinstance(out, ParameterWriteRejected)
    assert store.get("mae_mult") == Decimal("1.6")  # the within-interval write did not land


def test_change_at_exactly_50_trades_allowed():
    store = ParameterStore(initial={"mae_mult": Decimal("1.5")})
    store.apply(_change("mae_mult", Decimal("1.6")), at_trade_count=250)
    out = store.apply(_change("mae_mult", Decimal("1.7")), at_trade_count=250 + MIN_TRADES_BETWEEN_CHANGES)
    assert isinstance(out, ParameterWritten)
    assert store.get("mae_mult") == Decimal("1.7")


def test_trades_since_last_change():
    store = ParameterStore(initial={"mae_mult": Decimal("1.5")})
    assert store.trades_since_last_change(250) == 250  # never changed -> full count
    store.apply(_change("mae_mult", Decimal("1.6")), at_trade_count=250)
    assert store.trades_since_last_change(310) == 60


# --------------------------------------------------------------------------- frozen snapshot
def test_snapshot_is_read_only():
    store = ParameterStore(initial={"mae_mult": Decimal("1.5")})
    snap = store.snapshot()
    assert snap["mae_mult"] == Decimal("1.5")
    with pytest.raises(TypeError):
        snap["mae_mult"] = Decimal("9")  # MappingProxyType is read-only


def test_snapshot_is_a_frozen_copy_no_drift():
    store = ParameterStore(initial={"mae_mult": Decimal("1.5")})
    snap = store.snapshot()  # taken at cycle start
    store.apply(_change("mae_mult", Decimal("1.6")), at_trade_count=250)  # mid-cycle write
    assert snap["mae_mult"] == Decimal("1.5")          # the in-flight snapshot did not drift
    assert store.snapshot()["mae_mult"] == Decimal("1.6")  # a fresh snapshot sees the new value
