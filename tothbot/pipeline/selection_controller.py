"""gate:G5_Selection_Controller - the 4-gate per-module quality filter (0500000 Image2 G5).

Source: 0500000 dv1_250 sec 3 Image2 gate:G5_Selection_Controller + ar:AR-072 / ar:AR-073 +
rule:HR-SC-006 (monotonic cooldown clock) + rule:HR-EC-014 (Exit_Controller sole state updater).

Gate 5 applies mod:Selection_Controller's four quality sub-gates to each SSS-passed candidate,
PER-MODULE INDEPENDENT - the Long module and the Short module each carry their OWN
consecutive-loss counter + active-position set, and the gates evaluate against the candidate's
OWN side's state, never the sibling's (the clean Long/Short separation):

  SC-Gate-1 Body strength   |close-open| / (high-low) >= sc_body_threshold. An ABSOLUTE body
                            fraction - direction-symmetric (long and short both require a
                            strong-bodied candle).
  SC-Gate-2 Post-exit       seconds since THIS side's last exit on the pair >= sc_cooldown_
            cooldown        seconds. The elapsed interval MUST be measured with a MONOTONIC
                            clock (rule:HR-SC-006) - a backward wall-clock correction could
                            falsely satisfy the cooldown and re-enter a just-stopped-out pair.
                            The caller supplies the monotonically-measured elapsed seconds;
                            None = no prior exit (cooldown not applicable -> PASS).
  SC-Gate-3 Consecutive     this side's consecutive_loss_count < sc_consecutive_limit.
            loss
  SC-Gate-4 No active       no active SAME-SIDE position on the pair (mod:Exit_Controller is
            same-side pos   the SOLE updater of this state, rule:HR-EC-014).

PASS on all four -> proceed to gate:G6_Regime_Sizer; otherwise SKIP (evt:SELECTION_REJECTED)
carrying which sub-gate rejected. All four results are reported (sc_gate_results[1-4]).

PURE compute (Decimal-only, ar:AR-047). The Selection_Controller's mutable state (consecutive-
loss counter, last-exit cooldown log, active-position set) is owned + updated by mod:Exit_
Controller via the HR-EC-014 sole-updater path; this gate reads it as inputs and renders the
quality verdict. The three thresholds are CIATS-owned per-module (registry seeds, overridable).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from ..config import registry
from ..exchange.position_mirror import PositionSide

# CIATS-owned per-module seeds (value home TB00000 sec 8), Decimal/int once (AR-047).
_BODY_THRESHOLD = Decimal(str(registry.value("sc_body_threshold")))      # 0.3 seed
_COOLDOWN_SECONDS = Decimal(str(registry.value("sc_cooldown_seconds")))  # 300s seed (1 candle)
_CONSECUTIVE_LIMIT = int(registry.value("sc_consecutive_limit"))         # 3 seed

_ZERO = Decimal("0")


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the gate (AR-047)."""
    return Decimal(str(value))


@dataclass(frozen=True)
class G5SelectionPass:
    """evt:SELECTION_PASS [INFO] (Image2 G5) - all four SC sub-gates passed; the candidate
    proceeds to Gate 6. sc_gate_results carries the four per-gate booleans (all True)."""

    side: PositionSide
    sc_gate_results: tuple   # (gate1, gate2, gate3, gate4) booleans
    code: str = field(default="SELECTION_PASS", init=False)


@dataclass(frozen=True)
class G5SelectionRejected:
    """evt:SELECTION_REJECTED [INFO] (Image2 G5 skip) - a SC sub-gate failed; SKIP this
    candidate (feeds the SKIP REJECT REGISTRY). rejecting_sub_gate is the 1-based index of the
    FIRST failing gate; sc_gate_results carries all four booleans."""

    side: PositionSide
    sc_gate_results: tuple   # (gate1, gate2, gate3, gate4) booleans
    rejecting_sub_gate: int  # 1..4
    code: str = field(default="SELECTION_REJECTED", init=False)


@dataclass(frozen=True)
class SelectionOutcome:
    """The result of one Gate-5 evaluation. passed=True carries a G5SelectionPass; otherwise a
    G5SelectionRejected naming the first failing sub-gate."""

    passed: bool
    event: object  # G5SelectionPass on pass, G5SelectionRejected on reject


def evaluate_selection(
    side: PositionSide,
    *,
    candle_open: object,
    candle_high: object,
    candle_low: object,
    candle_close: object,
    seconds_since_last_exit: object | None,
    consecutive_loss_count: int,
    has_active_same_side_position: bool,
    sc_body_threshold: object | None = None,
    sc_cooldown_seconds: object | None = None,
    sc_consecutive_limit: object | None = None,
) -> SelectionOutcome:
    """Apply the four Selection-Controller sub-gates to one candidate (Image2 G5, AR-072/073).

    Evaluates against the candidate's OWN side's state (per-module independent). All four gates
    are computed; the verdict is PASS iff all pass, else SKIP naming the first failure. PURE -
    emits nothing; the caller logs the returned event. seconds_since_last_exit is the caller's
    MONOTONICALLY-measured elapsed time (HR-SC-006), or None if this side never exited the pair."""
    body_thresh = _BODY_THRESHOLD if sc_body_threshold is None else _dec(sc_body_threshold)
    cooldown = _COOLDOWN_SECONDS if sc_cooldown_seconds is None else _dec(sc_cooldown_seconds)
    consec_limit = _CONSECUTIVE_LIMIT if sc_consecutive_limit is None else int(sc_consecutive_limit)

    # SC-Gate-1: absolute body fraction |close-open| / (high-low). A zero-range candle has no
    # body strength (degenerate doji) -> fails (never a strong-bodied entry).
    o, h, low, c = _dec(candle_open), _dec(candle_high), _dec(candle_low), _dec(candle_close)
    rng = h - low
    body_fraction = (abs(c - o) / rng) if rng > 0 else _ZERO
    gate1 = body_fraction >= body_thresh

    # SC-Gate-2: post-exit cooldown (monotonic elapsed, HR-SC-006). None = no prior exit -> PASS.
    gate2 = True if seconds_since_last_exit is None else _dec(seconds_since_last_exit) >= cooldown

    # SC-Gate-3: this side's consecutive-loss count below the limit.
    gate3 = int(consecutive_loss_count) < consec_limit

    # SC-Gate-4: no active same-side position on the pair (HR-EC-014 sole-updated state).
    gate4 = not has_active_same_side_position

    results = (gate1, gate2, gate3, gate4)
    if all(results):
        return SelectionOutcome(passed=True, event=G5SelectionPass(side, results))
    rejecting = next(i for i, ok in enumerate(results, start=1) if not ok)
    return SelectionOutcome(
        passed=False,
        event=G5SelectionRejected(side, results, rejecting),
    )
