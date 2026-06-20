"""rule:No_Same_Instrument_Collision - no simultaneous long+short on the same perp instrument.

Source: 0500000 sec 13.7 "CIATS RULE TO ADD - no same-instrument long+short collision" +
Image10 rule:No_Same_Instrument_Collision. Mirrors the TB00806 oracle
(scripts/tb00806a_twopool_hedge.py A5, lines 214-248), validated FREE (+5.6%, battery A5).

Because Long-Perp and Short-Perp trade the SAME futures venue, a simultaneous LONG and SHORT
on the SAME instrument (both in PBTCUC, say) would NET against each other inside ONE futures
engine - defeating the ring-fence and wasting margin. The rule: NO same-instrument long+short
open at once across the two perp pools. It is SYMMETRIC by construction (binds the Long-Perp
CIATS and the Short-Perp CIATS identically; equal detail). DIFFERENT instruments on opposite
sides are fine; only the SAME base_symbol is blocked from simultaneous long+short.

PURE compute. A candidate on one side COLLIDES iff its [entry, exit) holding interval overlaps
an OPEN position on the SAME base_symbol on the OPPOSITE side.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True)
class OpenInterval:
    """An open position's holding interval on a base_symbol: [entry_time, exit_time).

    entry_time / exit_time are any monotonically-orderable timestamps (epoch seconds, ns, or
    bar index) - only their ordering is used. exit_time is EXCLUSIVE (a position that exits at
    t does not collide with one entering at t)."""

    base_symbol: str
    entry_time: object
    exit_time: object


def _overlaps(a_entry, a_exit, b_entry, b_exit) -> bool:
    """Half-open interval overlap [a_entry, a_exit) ∩ [b_entry, b_exit) != empty."""
    return a_entry < b_exit and b_entry < a_exit


def collides(
    candidate: OpenInterval,
    opposite_open: Iterable[OpenInterval],
) -> bool:
    """Whether a candidate position on one side COLLIDES with any open position on the opposite
    side: same base_symbol AND overlapping holding interval. Mirrors tb00806a's `collides`."""
    for other in opposite_open:
        if other.base_symbol != candidate.base_symbol:
            continue
        if _overlaps(candidate.entry_time, candidate.exit_time, other.entry_time, other.exit_time):
            return True
    return False


def filter_collisions(
    candidates: Iterable[OpenInterval],
    opposite_open: Iterable[OpenInterval],
) -> tuple[list[OpenInterval], list[OpenInterval]]:
    """Partition candidate positions into (kept, blocked) by the no-same-instrument-collision
    rule against the opposite side's open positions. Returns (kept, blocked).

    The rule is symmetric: call with the Short-Perp candidates against the Long-Perp open set,
    or vice versa - the partition logic is identical (equal detail, section 13.7)."""
    opp = list(opposite_open)
    kept: list[OpenInterval] = []
    blocked: list[OpenInterval] = []
    for c in candidates:
        (blocked if collides(c, opp) else kept).append(c)
    return kept, blocked
