"""gate:G4_HTF_Confirmation - the 1H higher-timeframe directional confirmation gate.

Source: 0500000 dv1_250 sec 3 Image2 gate:G4_HTF_Confirmation + rule:HR-SP-006 + the
channel:kraken_ws_ohlc_60m 1H feed + param:htf_ema_periods.

Gate 4 confirms the candidate's direction is backed by higher-timeframe momentum before the
SSS signal is evaluated. It selects its test from the gate:G3_Regime_Filter permitted_side
(the regime decides which side and therefore which test):

  LONG  (permitted_side=LONG, regime:TRENDING_POS):  EMA20_daily > EMA50_daily
                                                      AND last 1H close > 1H EMA20
  SHORT (permitted_side=SHORT, regime:TRENDING_NEG):  EMA20_daily < EMA50_daily
                                                      AND last 1H close < 1H EMA20   (mirror)

PASS on alignment; SKIP (evt:HTF_GATE_REJECTED, rule:HR-SP-006) otherwise. The two tests are
exact mirrors - the short reverses every inequality of the long (the full Long/Short mirror).

NON_DIR_NORMAL BYPASS: a regime:NON_DIR_NORMAL pair (permitted_side=BOTH, entry-permitted at
50% at gate:G6_Regime_Sizer) has NO directional 1H trend to confirm, so it SKIPS Gate 4 and
proceeds directly to the SSS engine. Modelled here as a BYPASS outcome (passed, no test run) -
distinct from a tested PASS.

PURE compute (Decimal-only, ar:AR-047). The daily EMAs come from the Regime_Engine's daily
series; the 1H close + 1H EMA20 come from the 60m channel (computed off the hot path). Gate 4
is the pure comparison given those values + the candidate side/regime.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from ..exchange.position_mirror import PositionSide
from ..regime.taxonomy import Regime


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the gate (AR-047)."""
    return Decimal(str(value))


@dataclass(frozen=True)
class G4HtfRejected:
    """evt:HTF_GATE_REJECTED [INFO] (Image2 G4 skip, rule:HR-SP-006) - the 1H higher-timeframe
    momentum did NOT align with the candidate direction. SKIP this candidate (feeds the SKIP
    REJECT REGISTRY). Carries the side + the four compared values for the G4_HTF_DECISION log."""

    side: PositionSide
    ema20_daily: Decimal
    ema50_daily: Decimal
    close_1h: Decimal
    ema20_1h: Decimal
    code: str = field(default="HTF_GATE_REJECTED", init=False)


@dataclass(frozen=True)
class G4HtfDecision:
    """The result of one Gate-4 evaluation. passed=True proceeds to the SSS engine; bypassed
    flags the NON_DIR_NORMAL pass-through (no directional test run). event carries the
    G4HtfRejected on a tested fail, else None."""

    passed: bool
    bypassed: bool
    event: object | None  # G4HtfRejected on tested fail; None on PASS or BYPASS


def confirm_htf(
    side: PositionSide,
    regime: Regime,
    *,
    ema20_daily: object,
    ema50_daily: object,
    close_1h: object,
    ema20_1h: object,
) -> G4HtfDecision:
    """Run the Gate-4 1H HTF confirmation (Image2 G4, HR-SP-006), selecting the long/short
    test from the candidate side. NON_DIR_NORMAL bypasses (no trend to confirm). PURE - emits
    nothing; the caller logs the returned event."""
    # NON_DIR_NORMAL: no 1H directional trend to confirm -> BYPASS straight to the SSS engine.
    if regime is Regime.NON_DIR_NORMAL:
        return G4HtfDecision(passed=True, bypassed=True, event=None)

    e20d = _dec(ema20_daily)
    e50d = _dec(ema50_daily)
    c1h = _dec(close_1h)
    e20h = _dec(ema20_1h)

    # Long requires bullish daily EMA alignment AND a 1H close above the 1H EMA20; short is the
    # exact mirror (every inequality reversed). ar:AR-074 / the clean Long/Short mirror.
    if side is PositionSide.LONG:
        aligned = e20d > e50d and c1h > e20h
    else:
        aligned = e20d < e50d and c1h < e20h

    if aligned:
        return G4HtfDecision(passed=True, bypassed=False, event=None)
    return G4HtfDecision(
        passed=False,
        bypassed=False,
        event=G4HtfRejected(side, e20d, e50d, c1h, e20h),
    )
