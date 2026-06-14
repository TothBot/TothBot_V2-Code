"""S2c tests: contract:Reconciliation_REST private-channel sequence-gap detection.

Covers 0500000 dv1_240 sec 2 Image1 A-9 / A-10 + sec 7 mod:WS_Manager D1 wire
facts WS-EXE-019 / WS-BAL-005: per-channel last-seq tracking, forward gap
(skip > 1) -> alert + REST recovery endpoint, independent executions/balances
trackers, executions higher severity, reset on reconnect (no cross-session
gap-detection).
"""

from __future__ import annotations

from tothbot.exchange.reconcile import (
    ReconChannel,
    ReconciliationTracker,
    SequenceGap,
    SequenceGapDetector,
)


# -- single-channel detector --------------------------------------------

def test_first_message_is_baseline_no_gap():
    d = SequenceGapDetector(ReconChannel.EXECUTIONS)
    assert d.last_seq is None
    assert d.observe(100) is None      # baseline
    assert d.last_seq == 100


def test_clean_advance_no_gap():
    d = SequenceGapDetector(ReconChannel.BALANCES)
    d.observe(1)
    assert d.observe(2) is None
    assert d.observe(3) is None
    assert d.last_seq == 3


def test_forward_gap_detected():
    d = SequenceGapDetector(ReconChannel.EXECUTIONS)
    d.observe(10)
    gap = d.observe(13)                # skipped 11, 12
    assert isinstance(gap, SequenceGap)
    assert gap.last_seq == 10
    assert gap.received_seq == 13
    assert gap.missed == 2
    assert d.last_seq == 13            # watermark advanced past the gap


def test_no_re_alert_after_gap_advance():
    d = SequenceGapDetector(ReconChannel.EXECUTIONS)
    d.observe(10)
    assert d.observe(13) is not None   # gap
    assert d.observe(14) is None       # clean continuation, no second alert
    assert d.observe(15) is None


def test_duplicate_or_out_of_order_is_not_a_gap():
    d = SequenceGapDetector(ReconChannel.BALANCES)
    d.observe(5)
    d.observe(6)
    assert d.observe(6) is None        # duplicate
    assert d.observe(4) is None        # out-of-order replay
    assert d.last_seq == 6             # watermark never moves backwards


# -- per-channel recovery wiring (A-9 / A-10) ---------------------------

def test_executions_gap_routes_to_getopenorders_high_severity():
    d = SequenceGapDetector(ReconChannel.EXECUTIONS)
    d.observe(1)
    gap = d.observe(5)
    assert gap.alert_key == "EXECUTIONS_SEQUENCE_GAP"
    assert gap.recovery_endpoint == "GetOpenOrders"
    assert gap.severity == "HIGH"


def test_balances_gap_routes_to_getaccountbalance_medium_severity():
    d = SequenceGapDetector(ReconChannel.BALANCES)
    d.observe(1)
    gap = d.observe(5)
    assert gap.alert_key == "BALANCES_SEQUENCE_GAP"
    assert gap.recovery_endpoint == "GetAccountBalance"
    assert gap.severity == "MEDIUM"    # A-10: executions outranks balances


# -- reset on reconnect (A-10: no cross-session gap-detection) -----------

def test_reset_drops_watermark():
    d = SequenceGapDetector(ReconChannel.EXECUTIONS)
    d.observe(100)
    d.reset()
    assert d.last_seq is None
    assert d.observe(5000) is None     # new subscription baseline, not a gap


# -- tracker owns two independent channels ------------------------------

def test_tracker_channels_are_independent():
    t = ReconciliationTracker()
    t.observe_executions(1)
    t.observe_balances(1)
    # a gap on executions must not affect the balances watermark
    exec_gap = t.observe_executions(9)
    assert exec_gap is not None and exec_gap.channel is ReconChannel.EXECUTIONS
    assert t.observe_balances(2) is None
    assert t.balances.last_seq == 2
    assert t.executions.last_seq == 9


def test_tracker_reset_clears_both():
    t = ReconciliationTracker()
    t.observe_executions(10)
    t.observe_balances(20)
    t.reset()
    assert t.executions.last_seq is None
    assert t.balances.last_seq is None
