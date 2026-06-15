"""The pre-signal entry-eligibility gates: Pre-Gate-1, Gate-1, Gate-2 (0500000 Image2).

Source: 0500000 dv1_250 sec 3 Image2 gate:Pre_Gate_1_Per_Pair_Status (HR-SP-003,
D-03_Universe_Scope) + gate:G1_State_Machine (HR-SP-003) + gate:G2_Liquidity
(D-04_Liquidity_Floor, HR-SP-004). These are the three cheap eligibility checks that run
BEFORE the regime/HTF/SSS brains in the Signal_Pipeline gate chain (Pre-G1 -> G1 -> G2 ->
G3...). Each is a PURE check (Decimal-only, ar:AR-047); they share this module as the
"is this pair eligible to even consider right now" prefilter.

  Pre-Gate-1  Per-pair instrument status. PASS iff status == online (the sole entry-eligible
              status under the CR-03 taker marketable-IOC entry). ALSO partitions the per-side
              universe (ar:AR-073 / ar:AR-009): the LONG universe is the full D-03 spot
              universe; the SHORT universe is the MARGINABLE SUBSET only - a non-marginable
              pair PASSES for long but is SHORT_INELIGIBLE (shorts trade Kraken margin, so a
              non-marginable pair can never emit a short candidate). SKIP otherwise.
  Gate-1      WS subscription state-machine readiness. PASS iff state == Subscribed. SIDE-SHARED
              - the WS data layer is the single shared layer between Long and Short (TB00000
              sec 7), so one Subscribed state serves both side paths. Non-ready is a WAIT
              (logged-only, re-evaluated next tick), NOT a pipeline SKIP.
  Gate-2      24h USD-volume liquidity floor. PASS iff vol_24h_usd >= min_volume_usd_daily
              ($500k/day seed). Consumes the D1-owned liquidity_24h value verbatim (does NOT
              recompute it). SKIP otherwise. Side-neutral.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

from ..config import registry
from ..exchange.position_mirror import PositionSide

# The sole entry-eligible instrument status under CR-03 taker marketable-IOC entry (WS-INST-009).
_ONLINE = "online"

# CIATS-owned per-module liquidity floor (value home TB00000 sec 8), Decimal once (AR-047).
_MIN_VOLUME_USD = Decimal(str(registry.value("min_volume_usd_daily")))  # $500k/day seed


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the gates (AR-047)."""
    return Decimal(str(value))


# --------------------------------------------------------------------------- Pre-Gate-1
class PreGate1Disposition(Enum):
    PASS = "PASS"
    INSTRUMENT_STATUS_BLOCKED = "INSTRUMENT_STATUS_BLOCKED"  # status != online
    SHORT_INELIGIBLE = "SHORT_INELIGIBLE"                    # online but non-marginable short


@dataclass(frozen=True)
class PreGate1Decision:
    """evt:PRE_GATE_1_DECISION (Image2 Pre-Gate-1). passed proceeds to Gate 1; otherwise the
    disposition names the block (INSTRUMENT_STATUS_BLOCKED for a non-online status, or
    SHORT_INELIGIBLE for a short candidate on a non-marginable pair)."""

    side: PositionSide
    instrument_status: str
    marginable: bool
    disposition: PreGate1Disposition
    passed: bool
    code: str = field(default="PRE_GATE_1_DECISION", init=False)


def check_pair_status(
    side: PositionSide, instrument_status: str, *, marginable: bool
) -> PreGate1Decision:
    """Pre-Gate-1 (Image2, HR-SP-003): per-pair instrument status + per-side universe partition.

    online + (LONG, or SHORT on a marginable pair) -> PASS. online + SHORT on a non-marginable
    pair -> SHORT_INELIGIBLE (no short candidate). Any non-online status -> INSTRUMENT_STATUS_
    BLOCKED. PURE - the caller logs the decision."""
    if instrument_status != _ONLINE:
        disp = PreGate1Disposition.INSTRUMENT_STATUS_BLOCKED
    elif side is PositionSide.SHORT and not marginable:
        # A short trades Kraken margin (ar:AR-009); a non-marginable pair cannot be shorted.
        disp = PreGate1Disposition.SHORT_INELIGIBLE
    else:
        disp = PreGate1Disposition.PASS
    return PreGate1Decision(
        side=side,
        instrument_status=instrument_status,
        marginable=marginable,
        disposition=disp,
        passed=disp is PreGate1Disposition.PASS,
    )


# --------------------------------------------------------------------------- Gate-1
# The WS subscription state-machine ready state (the only one that admits a candidate).
_SUBSCRIBED = "Subscribed"


@dataclass(frozen=True)
class G1StateDecision:
    """evt:G1_STATE_DECISION (Image2 Gate-1). passed iff the pair's WS state is Subscribed.
    A non-ready state is a WAIT (rejection_code SYSTEM_STATE_BLOCKED) - logged-only, the pair is
    re-evaluated next tick; it is NOT a pipeline SKIP (existing positions stay Exit-Controller
    managed). SIDE-SHARED: one Subscribed serves both side paths."""

    ws_state: str
    passed: bool
    rejection_code: str | None  # None on PASS, "SYSTEM_STATE_BLOCKED" on WAIT
    code: str = field(default="G1_STATE_DECISION", init=False)


def check_state_machine(ws_state: str) -> G1StateDecision:
    """Gate-1 (Image2, HR-SP-003): WS subscription readiness. PASS iff state == Subscribed; else
    WAIT (SYSTEM_STATE_BLOCKED, logged-only). Side-shared (the WS data layer is shared). PURE."""
    passed = ws_state == _SUBSCRIBED
    return G1StateDecision(
        ws_state=ws_state,
        passed=passed,
        rejection_code=None if passed else "SYSTEM_STATE_BLOCKED",
    )


# --------------------------------------------------------------------------- Gate-2
@dataclass(frozen=True)
class G2LiquidityDecision:
    """evt:G2_LIQUIDITY_DECISION (Image2 Gate-2). passed iff the pair's 24h USD volume meets the
    floor; otherwise SKIP (LIQUIDITY_REJECTED). Carries the compared values for the log."""

    vol_24h_usd: Decimal
    floor_usd: Decimal
    passed: bool
    code: str = field(default="G2_LIQUIDITY_DECISION", init=False)


def check_liquidity(
    vol_24h_usd: object, *, min_volume_usd_daily: object | None = None
) -> G2LiquidityDecision:
    """Gate-2 (Image2, D-04_Liquidity_Floor / HR-SP-004): 24h USD-volume floor. PASS iff
    vol_24h_usd >= floor ($500k/day seed; consumes the D1-owned liquidity_24h value, does NOT
    recompute). Side-neutral. PURE."""
    floor = _MIN_VOLUME_USD if min_volume_usd_daily is None else _dec(min_volume_usd_daily)
    vol = _dec(vol_24h_usd)
    return G2LiquidityDecision(vol_24h_usd=vol, floor_usd=floor, passed=vol >= floor)
