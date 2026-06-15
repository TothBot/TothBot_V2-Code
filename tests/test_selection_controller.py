"""Tests: gate:G5_Selection_Controller (pipeline/selection_controller.py).

Covers 0500000 dv1_250 Image2 G5 + ar:AR-072/073 + HR-SC-006 (monotonic cooldown): the four
per-module quality sub-gates (body strength, post-exit cooldown, consecutive-loss, no active
same-side position), the first-failure report, and per-side independence. Decimal-only (AR-047).

Seeds: sc_body_threshold 0.3, sc_cooldown_seconds 300, sc_consecutive_limit 3.
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.position_mirror import PositionSide
from tothbot.pipeline.selection_controller import (
    G5SelectionPass,
    G5SelectionRejected,
    evaluate_selection,
)


def _eval(**over):
    """A candidate that passes all four sub-gates unless overridden.
    Default candle: open 100, high 110, low 99, close 108 -> body 8 / range 11 = 0.727 >= 0.3."""
    kw = dict(
        candle_open="100", candle_high="110", candle_low="99", candle_close="108",
        seconds_since_last_exit="600",       # > 300 cooldown
        consecutive_loss_count=0,            # < 3
        has_active_same_side_position=False,
    )
    kw.update(over)
    return evaluate_selection(PositionSide.LONG, **kw)


def test_all_four_pass():
    out = _eval()
    assert out.passed is True
    assert isinstance(out.event, G5SelectionPass)
    assert out.event.sc_gate_results == (True, True, True, True)
    assert out.event.code == "SELECTION_PASS"


# -- SC-Gate-1 body strength --------------------------------------------

def test_weak_body_rejects_gate1():
    # body 1 / range 11 = 0.09 < 0.3 -> reject at gate 1.
    out = _eval(candle_open="100", candle_close="101", candle_high="110", candle_low="99")
    assert out.passed is False
    assert isinstance(out.event, G5SelectionRejected)
    assert out.event.rejecting_sub_gate == 1
    assert out.event.sc_gate_results[0] is False


def test_zero_range_candle_fails_body_gate():
    out = _eval(candle_open="100", candle_close="100", candle_high="100", candle_low="100")
    assert out.event.rejecting_sub_gate == 1


def test_body_threshold_is_inclusive():
    # body exactly at threshold: body 3 / range 10 = 0.3 >= 0.3 -> gate 1 passes.
    out = _eval(candle_open="100", candle_close="103", candle_high="105", candle_low="95")
    assert out.event.sc_gate_results[0] is True if out.passed else None
    assert out.passed is True


# -- SC-Gate-2 cooldown (monotonic) -------------------------------------

def test_cooldown_not_elapsed_rejects_gate2():
    out = _eval(seconds_since_last_exit="120")  # < 300
    assert out.passed is False
    assert out.event.rejecting_sub_gate == 2


def test_cooldown_inclusive_at_threshold():
    out = _eval(seconds_since_last_exit="300")  # == 300 -> PASS
    assert out.passed is True


def test_no_prior_exit_passes_cooldown():
    out = _eval(seconds_since_last_exit=None)
    assert out.passed is True


# -- SC-Gate-3 consecutive loss -----------------------------------------

def test_consecutive_loss_limit_rejects_gate3():
    out = _eval(consecutive_loss_count=3)  # not < 3
    assert out.passed is False
    assert out.event.rejecting_sub_gate == 3


def test_consecutive_loss_under_limit_passes():
    out = _eval(consecutive_loss_count=2)
    assert out.passed is True


# -- SC-Gate-4 active same-side position ---------------------------------

def test_active_same_side_position_rejects_gate4():
    out = _eval(has_active_same_side_position=True)
    assert out.passed is False
    assert out.event.rejecting_sub_gate == 4


# -- first-failure ordering ---------------------------------------------

def test_reports_first_failing_subgate():
    # both gate 1 (weak body) and gate 3 (loss limit) fail; gate 1 reported first.
    out = _eval(
        candle_open="100", candle_close="100.5", candle_high="110", candle_low="99",
        consecutive_loss_count=5,
    )
    assert out.event.rejecting_sub_gate == 1
    assert out.event.sc_gate_results == (False, True, False, True)


# -- per-side independence + symmetry -----------------------------------

def test_short_evaluates_identically_to_long():
    common = dict(
        candle_open="100", candle_high="110", candle_low="99", candle_close="108",
        seconds_since_last_exit="600", consecutive_loss_count=0,
        has_active_same_side_position=False,
    )
    lng = evaluate_selection(PositionSide.LONG, **common)
    sht = evaluate_selection(PositionSide.SHORT, **common)
    assert lng.passed is sht.passed is True
    assert sht.event.side is PositionSide.SHORT


def test_body_fraction_is_absolute_so_a_down_candle_can_pass():
    # A strong DOWN candle (close < open) has the same |body| strength as the up candle - a
    # short candidate is not penalised for a red candle (direction-symmetric body fraction).
    out = evaluate_selection(
        PositionSide.SHORT,
        candle_open="108", candle_high="110", candle_low="99", candle_close="100",
        seconds_since_last_exit="600", consecutive_loss_count=0,
        has_active_same_side_position=False,
    )
    assert out.passed is True


# -- per-call override + AR-047 -----------------------------------------

def test_override_thresholds():
    out = _eval(consecutive_loss_count=3, sc_consecutive_limit=5)  # 3 < 5 now
    assert out.passed is True


def test_no_float_enters_the_gate():
    out = evaluate_selection(
        PositionSide.LONG,
        candle_open=100.0, candle_high=110.0, candle_low=99.0, candle_close=108.0,
        seconds_since_last_exit=600.0, consecutive_loss_count=0,
        has_active_same_side_position=False,
    )
    assert out.passed is True
    assert isinstance(out.event.sc_gate_results, tuple)
