"""Unit tests: the ar:AR-030 per-pair order-rate-counter (rate_counter.py).

Covers 0500000 dv1_252 sec 7 mod:WS_Manager AR-030 + the D1 RATE-COUNTER PROTECTIVE
RESPONSE (RL-MON-002 warning / RL-MON-003 critical-suppression) + sec 2.4 A-1 + the
sec 7 event_registry WS_MGR codes (MAXRATECOUNT_SET / RATE_COUNTER_UPDATE /
RATE_COUNTER_WARNING) + the section-9 seeds rl_warning_threshold_pct=0.80 /
rl_critical_threshold_pct=0.95. PURE + clock-free - no I/O, no timers.
"""

from __future__ import annotations

import pytest

from tothbot.config import registry
from tothbot.exchange.rate_counter import (
    MaxRateCountSet,
    RateCounter,
    RateCounterUpdate,
    RateCounterWarning,
)


# -- the seeds wire to the registry (never hardcoded) -------------------

def test_thresholds_default_from_registry_seeds():
    rc = RateCounter()
    rc.set_ceiling(125)
    # 0.80 / 0.95 of the operative ceiling, sourced from config/registry.py.
    assert rc.warning_threshold() == 125 * float(registry.value("rl_warning_threshold_pct"))
    assert rc.critical_threshold() == 125 * float(registry.value("rl_critical_threshold_pct"))


# -- set_ceiling: MAXRATECOUNT_SET (AR-030; never 125 hardcoded) --------

def test_set_ceiling_emits_maxratecount_set():
    rc = RateCounter()
    event = rc.set_ceiling(200)
    assert isinstance(event, MaxRateCountSet)
    assert event.code == "MAXRATECOUNT_SET" and event.value == 200
    assert rc.ceiling == 200


def test_set_ceiling_rejects_nonpositive():
    rc = RateCounter()
    with pytest.raises(ValueError):
        rc.set_ceiling(0)
    with pytest.raises(ValueError):
        rc.set_ceiling(-5)


# -- observe: RATE_COUNTER_UPDATE always; WARNING above the warning band -

def test_observe_below_warning_emits_only_update():
    rc = RateCounter(warning_pct=0.80, critical_pct=0.95)
    rc.set_ceiling(100)
    events = rc.observe("BTC/USD", 50)  # 50% - well below the 80% band
    assert len(events) == 1
    upd = events[0]
    assert isinstance(upd, RateCounterUpdate)
    assert upd.code == "RATE_COUNTER_UPDATE"
    assert (upd.symbol, upd.value, upd.maxratecount) == ("BTC/USD", 50, 100)
    assert rc.is_warning("BTC/USD") is False
    assert rc.is_entry_suppressed("BTC/USD") is False
    assert rc.value("BTC/USD") == 50


def test_observe_in_warning_band_emits_warning_not_suppressed():
    rc = RateCounter(warning_pct=0.80, critical_pct=0.95)
    rc.set_ceiling(100)
    events = rc.observe("ETH/USD", 85)  # 85% - warning, not yet critical
    kinds = [type(e) for e in events]
    assert kinds == [RateCounterUpdate, RateCounterWarning]
    warn = events[1]
    assert warn.code == "RATE_COUNTER_WARNING"
    assert (warn.symbol, warn.value, warn.maxratecount) == ("ETH/USD", 85, 100)
    assert rc.is_warning("ETH/USD") is True
    assert rc.is_entry_suppressed("ETH/USD") is False  # below critical


def test_observe_at_exact_warning_threshold_is_not_exceeded():
    rc = RateCounter(warning_pct=0.80, critical_pct=0.95)
    rc.set_ceiling(100)
    # exactly 80 == the warning fraction; "exceeds" is strict, so no warning yet
    events = rc.observe("SOL/USD", 80)
    assert [type(e) for e in events] == [RateCounterUpdate]
    assert rc.is_warning("SOL/USD") is False


# -- RL-MON-003 critical suppression + hysteresis -----------------------

def test_critical_arms_entry_suppression():
    rc = RateCounter(warning_pct=0.80, critical_pct=0.95)
    rc.set_ceiling(100)
    events = rc.observe("BTC/USD", 97)  # 97% - over critical
    # still a warning event (97 > 80) plus the latch arms; no critical EVENT (unregistered)
    assert [type(e) for e in events] == [RateCounterUpdate, RateCounterWarning]
    assert rc.is_entry_suppressed("BTC/USD") is True


def test_suppression_hysteresis_holds_through_band_releases_below_warning():
    rc = RateCounter(warning_pct=0.80, critical_pct=0.95)
    rc.set_ceiling(100)
    rc.observe("BTC/USD", 97)               # arm (over critical)
    assert rc.is_entry_suppressed("BTC/USD") is True
    rc.observe("BTC/USD", 88)               # in the hysteresis band (80<v<=95): HOLD
    assert rc.is_entry_suppressed("BTC/USD") is True
    rc.observe("BTC/USD", 80)               # decayed back to the warning fraction: RELEASE
    assert rc.is_entry_suppressed("BTC/USD") is False


def test_suppression_independent_per_pair():
    rc = RateCounter(warning_pct=0.80, critical_pct=0.95)
    rc.set_ceiling(100)
    rc.observe("BTC/USD", 99)
    rc.observe("ETH/USD", 10)
    assert rc.is_entry_suppressed("BTC/USD") is True
    assert rc.is_entry_suppressed("ETH/USD") is False


# -- no ceiling yet (pre-ACK): cannot derive bands, never assume 125 ----

def test_no_ceiling_emits_update_only_no_band():
    rc = RateCounter()
    events = rc.observe("BTC/USD", 9999)  # absurd value, but no ACK ceiling known
    assert [type(e) for e in events] == [RateCounterUpdate]
    assert events[0].maxratecount is None
    assert rc.warning_threshold() is None
    assert rc.critical_threshold() is None
    assert rc.is_warning("BTC/USD") is False
    assert rc.is_entry_suppressed("BTC/USD") is False


# -- reconnect reset: drop stale per-pair state, keep the ceiling -------

def test_reset_clears_values_and_suppression_keeps_ceiling():
    rc = RateCounter(warning_pct=0.80, critical_pct=0.95)
    rc.set_ceiling(100)
    rc.observe("BTC/USD", 99)  # suppressed + a stale value
    assert rc.is_entry_suppressed("BTC/USD") is True
    rc.reset()
    assert rc.value("BTC/USD") is None
    assert rc.is_entry_suppressed("BTC/USD") is False
    # the operative ceiling stays provisional until the fresh ACK re-sets it
    assert rc.ceiling == 100
