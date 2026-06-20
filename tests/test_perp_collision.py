"""Tests: rule:No_Same_Instrument_Collision (tothbot/perp/collision.py).

Covers the no-same-instrument long+short collision rule (0500000 sec 13.7; TB00806 battery A5,
validated FREE): same base_symbol + overlapping holding interval -> blocked; different symbols
or non-overlapping intervals -> kept; the rule is symmetric (binds both perp pools identically).
"""

from __future__ import annotations

from tothbot.perp.collision import OpenInterval, collides, filter_collisions


def _iv(sym, t0, t1):
    return OpenInterval(base_symbol=sym, entry_time=t0, exit_time=t1)


# -- collides ------------------------------------------------------------

def test_same_symbol_overlapping_collides():
    cand = _iv("PBTCUC", 10, 20)
    assert collides(cand, [_iv("PBTCUC", 15, 25)]) is True


def test_same_symbol_nested_collides():
    cand = _iv("PBTCUC", 12, 18)
    assert collides(cand, [_iv("PBTCUC", 10, 30)]) is True


def test_different_symbol_does_not_collide():
    cand = _iv("PBTCUC", 10, 20)
    assert collides(cand, [_iv("PETHUC", 10, 20)]) is False


def test_non_overlapping_same_symbol_does_not_collide():
    cand = _iv("PBTCUC", 10, 20)
    assert collides(cand, [_iv("PBTCUC", 25, 35)]) is False


def test_touching_at_exit_does_not_collide_half_open():
    # Exit is EXCLUSIVE: an open that exits exactly at the candidate's entry does not collide.
    cand = _iv("PBTCUC", 20, 30)
    assert collides(cand, [_iv("PBTCUC", 10, 20)]) is False


def test_empty_opposite_set_never_collides():
    assert collides(_iv("PBTCUC", 10, 20), []) is False


# -- filter_collisions ---------------------------------------------------

def test_filter_partitions_kept_and_blocked():
    longs = [_iv("PBTCUC", 0, 100)]  # an open LONG on BTC the whole window
    shorts = [
        _iv("PBTCUC", 10, 20),   # collides -> blocked
        _iv("PETHUC", 10, 20),   # different symbol -> kept
        _iv("PBTCUC", 200, 210), # non-overlapping -> kept
    ]
    kept, blocked = filter_collisions(shorts, longs)
    assert [k.base_symbol for k in kept] == ["PETHUC", "PBTCUC"]
    assert [b.base_symbol for b in blocked] == ["PBTCUC"]
    assert blocked[0].entry_time == 10


def test_rule_is_symmetric_both_directions():
    # The same partition holds whether shorts are checked against longs or vice versa.
    a = [_iv("PBTCUC", 0, 100)]
    b = [_iv("PBTCUC", 10, 20)]
    _, blocked_b = filter_collisions(b, a)
    _, blocked_a = filter_collisions(a, b)
    assert len(blocked_b) == 1 and len(blocked_a) == 1


def test_filter_with_no_opposite_keeps_all():
    shorts = [_iv("PBTCUC", 10, 20), _iv("PETHUC", 30, 40)]
    kept, blocked = filter_collisions(shorts, [])
    assert len(kept) == 2 and blocked == []
