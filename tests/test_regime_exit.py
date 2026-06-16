"""Pure layer:L1a regime-reversal exit detector tests (0500000 dv1_242 sec 3 Image3 +
ar:AR-062 + rule:HR-EC-016(a)).

EC-L1A-002 daily downgrade fires when the fresh classification turns against the open
position's direction (LONG: TRENDING_NEGATIVE or NON_DIR_ELEVATED; SHORT: the mirror);
EC-L1A-001 HTF reversal fires on the 1H EMA(20) < EMA(50) cross-below STATE for a long
(mirror for a short). The pair-status precondition (HR-EC-016(a)) HOLDs on cancel_only /
maintenance.
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.position_mirror import Position, PositionSide
from tothbot.exchange.regime_exit import (
    PairStatus,
    detect_daily_regime_downgrade,
    detect_htf_regime_reversal,
    l1a_precondition_blocks,
    pair_status_from_wire,
)
from tothbot.regime.engine import classify_from_indicators
from tothbot.regime.taxonomy import Regime


def _pos(side=PositionSide.LONG):
    return Position(
        symbol="BTC/USD",
        side=side,
        qty=Decimal("0.05"),
        avg_entry_price=Decimal("60000"),
    )


def _classification(regime: Regime):
    """Build a RegimeClassification landing in `regime` by feeding indicator values that
    select that cell (adx_threshold 25, atr pct threshold 67 - the registry seeds)."""
    # ADX, EMA20, EMA50, ATR, ATR-percentile chosen to land each cell deterministically.
    if regime is Regime.TRENDING_POS_NORMAL:
        adx, ema20, ema50, pct = "40", "105", "100", "50"
    elif regime is Regime.TRENDING_POS_ELEVATED:
        adx, ema20, ema50, pct = "40", "105", "100", "80"
    elif regime is Regime.NON_DIR_NORMAL:
        adx, ema20, ema50, pct = "10", "100", "100", "50"
    elif regime is Regime.NON_DIR_ELEVATED:
        adx, ema20, ema50, pct = "10", "100", "100", "80"
    elif regime is Regime.TRENDING_NEG_NORMAL:
        adx, ema20, ema50, pct = "40", "95", "100", "50"
    elif regime is Regime.TRENDING_NEG_ELEVATED:
        adx, ema20, ema50, pct = "40", "95", "100", "80"
    else:  # pragma: no cover - defensive
        raise AssertionError(regime)
    c = classify_from_indicators("BTC/USD", adx, ema20, ema50, "1000", pct)
    assert c.regime is regime  # the fixture lands the intended cell
    return c


# --- EC-L1A-002 Daily Regime Downgrade (long) ------------------------------------------
def test_daily_downgrade_long_trending_neg_normal_fires():
    sig = detect_daily_regime_downgrade(_pos(), _classification(Regime.TRENDING_NEG_NORMAL))
    assert sig is not None
    assert sig.exit_reason == "DAILY_REGIME_DOWNGRADE"
    assert sig.trigger == "EC-L1A-002"
    assert sig.layer == "L1a_DAILY"


def test_daily_downgrade_long_trending_neg_elevated_fires():
    sig = detect_daily_regime_downgrade(_pos(), _classification(Regime.TRENDING_NEG_ELEVATED))
    assert sig is not None and sig.exit_reason == "DAILY_REGIME_DOWNGRADE"


def test_daily_downgrade_long_non_dir_elevated_fires():
    # The whipsaw cell blocks a long -> a downgrade for an open long (HR-REGIME-008).
    sig = detect_daily_regime_downgrade(_pos(), _classification(Regime.NON_DIR_ELEVATED))
    assert sig is not None and sig.exit_reason == "DAILY_REGIME_DOWNGRADE"


def test_daily_downgrade_long_holds_in_trending_pos():
    assert detect_daily_regime_downgrade(_pos(), _classification(Regime.TRENDING_POS_NORMAL)) is None
    assert detect_daily_regime_downgrade(_pos(), _classification(Regime.TRENDING_POS_ELEVATED)) is None


def test_daily_downgrade_long_holds_in_non_dir_normal():
    # NON_DIR_NORMAL still permits a long (half size) - NOT a downgrade target (derivation (1)).
    assert detect_daily_regime_downgrade(_pos(), _classification(Regime.NON_DIR_NORMAL)) is None


# --- EC-L1A-002 Daily Regime Downgrade (short mirror) ----------------------------------
def test_daily_downgrade_short_trending_pos_fires():
    sig = detect_daily_regime_downgrade(
        _pos(PositionSide.SHORT), _classification(Regime.TRENDING_POS_NORMAL)
    )
    assert sig is not None and sig.trigger == "EC-L1A-002"


def test_daily_downgrade_short_non_dir_elevated_fires():
    sig = detect_daily_regime_downgrade(
        _pos(PositionSide.SHORT), _classification(Regime.NON_DIR_ELEVATED)
    )
    assert sig is not None


def test_daily_downgrade_short_holds_in_trending_neg():
    assert detect_daily_regime_downgrade(
        _pos(PositionSide.SHORT), _classification(Regime.TRENDING_NEG_NORMAL)
    ) is None


# --- EC-L1A-001 HTF Regime Reversal ----------------------------------------------------
def test_htf_reversal_long_fires_below():
    sig = detect_htf_regime_reversal(_pos(), htf_ema_short="99", htf_ema_long="100")
    assert sig is not None
    assert sig.exit_reason == "HTF_REGIME_REVERSAL"
    assert sig.trigger == "EC-L1A-001"
    assert sig.layer == "L1a_HTF"


def test_htf_reversal_long_holds_above():
    assert detect_htf_regime_reversal(_pos(), htf_ema_short="101", htf_ema_long="100") is None


def test_htf_reversal_long_tie_holds():
    # An exact EMA20 == EMA50 tie is not "below" (strict) -> hold (derivation (2)).
    assert detect_htf_regime_reversal(_pos(), htf_ema_short="100", htf_ema_long="100") is None


def test_htf_reversal_short_mirror_fires_above():
    sig = detect_htf_regime_reversal(_pos(PositionSide.SHORT), htf_ema_short="101", htf_ema_long="100")
    assert sig is not None and sig.exit_reason == "HTF_REGIME_REVERSAL"


def test_htf_reversal_short_mirror_holds_below():
    assert detect_htf_regime_reversal(
        _pos(PositionSide.SHORT), htf_ema_short="99", htf_ema_long="100"
    ) is None


def test_htf_reversal_decimal_coercion_no_float():
    # str/float inputs are coerced to Decimal on receipt (ar:AR-047) - no float in the compare.
    sig = detect_htf_regime_reversal(_pos(), htf_ema_short=99.5, htf_ema_long=100)
    assert sig is not None


# --- rule:HR-EC-016(a) Step-1 pair-status precondition ---------------------------------
def test_precondition_blocks_cancel_only_and_maintenance():
    assert l1a_precondition_blocks(PairStatus.CANCEL_ONLY) is True
    assert l1a_precondition_blocks(PairStatus.MAINTENANCE) is True


def test_precondition_allows_online():
    assert l1a_precondition_blocks(PairStatus.ONLINE) is False


# --- WS-INST-008 wire trading-status -> PairStatus mapping -----------------------------
def test_pair_status_from_wire_maps_the_three_exit_relevant_states():
    assert pair_status_from_wire("limit_only") is PairStatus.LIMIT_ONLY
    assert pair_status_from_wire("cancel_only") is PairStatus.CANCEL_ONLY
    assert pair_status_from_wire("maintenance") is PairStatus.MAINTENANCE
    assert pair_status_from_wire("online") is PairStatus.ONLINE


def test_pair_status_from_wire_other_states_map_to_online():
    # post_only / reduce_only / work_in_progress / delisted / unknown -> ONLINE (inert at the
    # instrument handler; on_instrument_status only acts on limit_only, HOLD set at dispatch).
    for s in ("post_only", "reduce_only", "work_in_progress", "delisted", "nonsense"):
        assert pair_status_from_wire(s) is PairStatus.ONLINE
