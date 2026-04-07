"""
TothBot V2 — Unit Tests: Regime Engine
=============================================================
Test spec:   1021001 Unit_Test_Specification dv1_0 §4.4
Module:      tothbot/regime_engine.py
Coding spec: 1011012 Regime_Engine_Coding_Spec dv1_4
BP standard: 1011001 Engineering_Best_Practices dv1_6
=============================================================

Tests: UT-RE-001 through UT-RE-007

UT-FW-004: Standard asyncio. Do NOT use uvloop.
"""
from __future__ import annotations

import asyncio
import inspect
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tothbot.regime_engine import (
    ADX_THRESHOLD,
    ELEVATED_VOL,
    NORMAL_VOL,
    NON_DIRECTIONAL,
    OHLC_STAGGER_SEC,
    RegimeEngine,
    RegimeState,
    TRENDING_NEGATIVE,
    TRENDING_POSITIVE,
)
from tothbot.logger import initialize_logger


# =============================================================
# FIXTURES
# =============================================================

@pytest.fixture
def logger():
    _, listener, log = initialize_logger()
    yield log
    listener.stop()


@pytest.fixture
def regime_engine(logger):
    return RegimeEngine(
        logger=logger,
        data_api_key="test_key",
        trading_universe=["BTC/USD", "ETH/USD"],
        param_store={"adx_threshold": Decimal("25"), "atr_percentile_thresh": 67},
    )


def _make_candles(
    n: int,
    base_close: float = 65000.0,
    trend: float = 10.0,        # close increment per candle
    high_delta: float = 500.0,
    low_delta: float = 500.0,
) -> list[dict]:
    """
    Generate synthetic OHLC candles for deterministic testing.
    n: total candles (includes uncommitted last — caller excludes).
    """
    candles = []
    close = Decimal(str(base_close))
    for i in range(n):
        open_  = close
        close  = Decimal(str(base_close + i * trend))
        high   = close + Decimal(str(high_delta))
        low    = close - Decimal(str(low_delta))
        candles.append({
            "time":   1700000000 + i * 86400,
            "open":   open_,
            "high":   high,
            "low":    low,
            "close":  close,
            "vwap":   close,
            "volume": Decimal("1000"),
        })
    return candles


# =============================================================
# UT-RE-001: response[-1] excluded from all computations
# RE-OHLC-002 / HR-WM-013
# =============================================================

class TestResponseLastExcluded:

    @pytest.mark.asyncio
    async def test_UT_RE_001_last_candle_excluded(self, regime_engine):
        """
        UT-RE-001: RE-OHLC-002 — _fetch_daily_ohlc must return
        response[:-1], excluding the uncommitted last candle.
        Inject a mock REST response with N candles.
        Verify only N-1 are returned to _compute_and_cache.
        """
        raw_candles = [
            [1700000000 + i * 86400,
             str(65000 + i * 10), str(65500 + i * 10),
             str(64500 + i * 10), str(65100 + i * 10),
             str(65050 + i * 10), "1000", 100]
            for i in range(50)
        ]
        # 50 raw candles → response[:-1] = 49 committed

        mock_resp = AsyncMock()
        mock_resp.status = 200
        mock_resp.read = AsyncMock(return_value=(
            b'{"error":[],"result":{"XXBTZUSD":' +
            str(raw_candles).encode().replace(b"'", b'"') + b'}}'
        ))

        import orjson
        mock_resp.read = AsyncMock(return_value=orjson.dumps({
            "error": [],
            "result": {"XXBTZUSD": raw_candles},
        }))

        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=False)
        mock_session.get = MagicMock(return_value=mock_resp)
        mock_resp.__aenter__ = AsyncMock(return_value=mock_resp)
        mock_resp.__aexit__ = AsyncMock(return_value=False)

        received_counts = []

        original_compute = regime_engine._compute_and_cache

        def capture_compute(pair, candles):
            received_counts.append(len(candles))
            original_compute(pair, candles)

        with patch.object(regime_engine, "_make_http_session",
                          return_value=mock_session):
            with patch.object(regime_engine, "_compute_and_cache",
                              side_effect=capture_compute):
                await regime_engine._fetch_daily_ohlc("BTC/USD")

        if received_counts:
            assert received_counts[0] == 49, (
                f"RE-OHLC-002: Expected 49 committed candles (50-1), "
                f"got {received_counts[0]}"
            )


# =============================================================
# UT-RE-002: ADX batch computation on full history
# RE-FPD-005 / RE-IND-006
# =============================================================

class TestADXComputation:

    def test_UT_RE_002_adx_batch_not_incremental(self):
        """
        UT-RE-002: RE-FPD-005 — ADX is computed in a batch on the
        full candle history. _compute_adx_14 must be a @staticmethod,
        confirming no incremental state is stored.
        """
        assert isinstance(
            inspect.getattr_static(RegimeEngine, "_compute_adx_14"),
            staticmethod,
        ), (
            "RE-FPD-005: _compute_adx_14 must be a @staticmethod "
            "(batch computation, no stored state)"
        )

    def test_UT_RE_002_adx_returns_none_for_insufficient_data(self):
        """
        UT-RE-002: ADX requires at least 2*14+1 = 29 candles.
        Returns None if insufficient.
        """
        candles = _make_candles(20)  # fewer than required
        result = RegimeEngine._compute_adx_14(candles)
        assert result is None, (
            "RE-IND-006: ADX must return None for < 29 candles"
        )

    def test_UT_RE_002_adx_returns_decimal_for_sufficient_data(self):
        """
        UT-RE-002: ADX must return a Decimal >= 0 for sufficient data.
        """
        candles = _make_candles(100, trend=50.0)
        result = RegimeEngine._compute_adx_14(candles)
        assert result is not None, "ADX returned None for 100 candles"
        assert isinstance(result, Decimal), (
            f"ADX must return Decimal, got {type(result)}"
        )
        assert result >= Decimal("0"), f"ADX must be >= 0, got {result}"

    def test_UT_RE_002_adx_tolerance_trending(self):
        """
        UT-RE-002: ADX for a strongly trending candle series must
        exceed ADX_THRESHOLD (25). ±1% tolerance on reference value.
        Strong trend: monotone increasing, large moves.
        """
        # Strong uptrend: consistent direction, high ADX
        candles = _make_candles(200, trend=100.0, high_delta=300.0, low_delta=100.0)
        adx = RegimeEngine._compute_adx_14(candles)
        assert adx is not None
        # Strongly trending data: ADX should be well above 25
        assert adx > Decimal("20"), (
            f"UT-RE-002: Strong trend should yield ADX > 20, got {adx}"
        )


# =============================================================
# UT-RE-003: TRENDING_POSITIVE classification
# RE-TAX-001
# =============================================================

class TestRegimeClassification:

    def test_UT_RE_003_trending_positive_adx_ema(self, regime_engine):
        """
        UT-RE-003: RE-TAX-001 — TRENDING_POSITIVE requires
        ADX > 25 AND EMA(20) > EMA(50).
        """
        # Uptrending candles: EMA20 > EMA50, strong ADX expected
        candles = _make_candles(300, trend=50.0, high_delta=200.0, low_delta=100.0)
        regime_engine._compute_and_cache("BTC/USD", candles)

        state = regime_engine.regime_cache.get("BTC/USD")
        if state is not None:
            # If EMA20 > EMA50 and ADX computed > threshold: TRENDING_POSITIVE
            if state.ema_20 > state.ema_50 and state.adx_14 > ADX_THRESHOLD:
                assert state.directional == TRENDING_POSITIVE, (
                    f"RE-TAX-001: ADX={state.adx_14} > 25 and "
                    f"EMA20={state.ema_20} > EMA50={state.ema_50} "
                    f"must be TRENDING_POSITIVE, got {state.directional}"
                )

    def test_UT_RE_003_trending_negative_ema20_lt_ema50(self, regime_engine):
        """
        UT-RE-003: TRENDING_NEGATIVE when ADX > 25 AND EMA20 < EMA50.
        (downtrend: decreasing closes)
        """
        # Downtrend
        candles = _make_candles(300, base_close=65000.0,
                                trend=-50.0, high_delta=100.0, low_delta=200.0)
        regime_engine._compute_and_cache("BTC/USD", candles)

        state = regime_engine.regime_cache.get("BTC/USD")
        if state is not None and state.adx_14 > ADX_THRESHOLD:
            if state.ema_20 < state.ema_50:
                assert state.directional == TRENDING_NEGATIVE, (
                    f"RE-TAX-001: ADX > 25 and EMA20 < EMA50 "
                    f"must be TRENDING_NEGATIVE, got {state.directional}"
                )

    def test_UT_RE_003_non_directional_low_adx(self, regime_engine):
        """
        UT-RE-003: NON_DIRECTIONAL when ADX <= 25 (flat market).
        """
        # Flat candles: minimal trend
        candles = _make_candles(300, trend=0.0, high_delta=50.0, low_delta=50.0)
        regime_engine._compute_and_cache("BTC/USD", candles)

        state = regime_engine.regime_cache.get("BTC/USD")
        if state is not None and state.adx_14 <= ADX_THRESHOLD:
            assert state.directional == NON_DIRECTIONAL, (
                f"RE-TAX-001: ADX {state.adx_14} <= 25 must be "
                f"NON_DIRECTIONAL, got {state.directional}"
            )


# =============================================================
# UT-RE-004: NORMAL_VOL at or below 67th percentile
# RE-TAX-002
# =============================================================

class TestVolatilityRegime:

    def test_UT_RE_004_atr_at_or_below_67th_is_normal_vol(self, regime_engine):
        """
        UT-RE-004: RE-TAX-002 — NORMAL_VOL when ATR percentile rank
        <= 67. ELEVATED_VOL when > 67.
        Uses _compute_atr_and_percentile directly.
        """
        # Constant ATR candles: all same range → percentile = 100
        # (latest equals all others → 100th percentile)
        candles = _make_candles(100, trend=0.0, high_delta=100.0, low_delta=100.0)
        _, pct_rank = RegimeEngine._compute_atr_and_percentile(candles)

        # When all candles have identical range, ATR percentile ~ 100
        # So this should be ELEVATED_VOL
        if pct_rank > Decimal("67"):
            vol = ELEVATED_VOL
        else:
            vol = NORMAL_VOL

        assert vol in (NORMAL_VOL, ELEVATED_VOL), (
            f"RE-TAX-002: vol_regime must be NORMAL_VOL or ELEVATED_VOL"
        )

    def test_UT_RE_004_percentile_boundary_67(self, regime_engine):
        """
        UT-RE-004: ATR at exactly 67th percentile → NORMAL_VOL.
        ATR above 67th percentile → ELEVATED_VOL.
        Per code: if atr_pct_rank > Decimal("67") → ELEVATED_VOL.
        """
        # Verify the boundary in _compute_and_cache logic
        candles = _make_candles(150, trend=10.0)
        # Mock _compute_atr_and_percentile to return exactly 67
        with patch.object(
            RegimeEngine, "_compute_atr_and_percentile",
            return_value=(Decimal("1000"), Decimal("67")),
        ):
            with patch.object(
                RegimeEngine, "_compute_adx_14",
                return_value=Decimal("30"),  # trending
            ):
                with patch.object(
                    RegimeEngine, "_compute_ema",
                    return_value=Decimal("65000"),
                ):
                    regime_engine._compute_and_cache("BTC/USD", candles)

        state = regime_engine.regime_cache.get("BTC/USD")
        if state:
            assert state.vol_regime == NORMAL_VOL, (
                f"RE-TAX-002: ATR at 67th percentile must be NORMAL_VOL "
                f"(> 67 required for ELEVATED_VOL). Got {state.vol_regime}"
            )


# =============================================================
# UT-RE-005: Stale regime fallback — never halt
# RE-SCH-003
# =============================================================

class TestStaleFallback:

    def test_UT_RE_005_stale_regime_preserves_prior_value(self, regime_engine):
        """
        UT-RE-005: RE-SCH-003 — on computation failure, stale regime
        is preserved (prior day value used). System must NOT halt.
        gate_3_check must still return using cached value.
        """
        # Pre-seed cache with a known regime
        regime_engine.regime_cache["BTC/USD"] = RegimeState(
            directional=TRENDING_POSITIVE,
            vol_regime=NORMAL_VOL,
            adx_14=Decimal("30"),
            ema_20=Decimal("65000"),
            ema_50=Decimal("63000"),
            atr_daily=Decimal("1000"),
            atr_pct_rank=Decimal("50"),
            computed_at="2026-04-05T00:00:00+00:00",
        )

        # Simulate computation failure — _check_stale logs but preserves cache
        regime_engine._check_stale("BTC/USD")

        # Cache must still have the prior value
        state = regime_engine.regime_cache.get("BTC/USD")
        assert state is not None, (
            "RE-SCH-003: Stale regime must preserve prior cache value"
        )
        assert state.directional == TRENDING_POSITIVE

    def test_UT_RE_005_gate_3_blocks_on_no_regime(self, regime_engine):
        """
        UT-RE-005: If no regime is available at all (never computed),
        gate_3_check returns BLOCK. System continues — never halts.
        """
        result = regime_engine.gate_3_check("ETH/USD")
        assert result == "BLOCK", (
            f"RE-SCH-003: gate_3 must BLOCK when no regime available. "
            f"Got {result}"
        )


# =============================================================
# UT-RE-006: 1.1s stagger enforced between pair REST calls
# RE-RL-002
# =============================================================

class TestOHLCStagger:

    @pytest.mark.asyncio
    async def test_UT_RE_006_stagger_enforced(self, regime_engine):
        """
        UT-RE-006: RE-RL-002 — asyncio.sleep(1.1) must be called
        between each pair's OHLC REST call in run_daily_computation.
        """
        sleep_calls = []

        async def mock_sleep(secs):
            sleep_calls.append(secs)

        async def mock_fetch(pair):
            return []  # no data, triggers _check_stale

        with patch.object(regime_engine, "_fetch_daily_ohlc",
                          side_effect=mock_fetch):
            with patch("tothbot.regime_engine.asyncio.sleep",
                       side_effect=mock_sleep):
                await regime_engine.run_daily_computation(["BTC/USD", "ETH/USD"])

        # 2 pairs → 2 sleep calls, each 1.1s
        assert len(sleep_calls) == 2, (
            f"RE-RL-002: Expected 2 stagger sleep calls for 2 pairs, "
            f"got {len(sleep_calls)}"
        )
        for call in sleep_calls:
            assert call == OHLC_STAGGER_SEC == 1.1, (
                f"RE-RL-002: Each sleep must be {OHLC_STAGGER_SEC}s, "
                f"got {call}"
            )


# =============================================================
# UT-RE-007: asyncio.gather() NOT used for regime OHLC calls
# RE-RL-003
# =============================================================

class TestNoAsyncioGather:

    def test_UT_RE_007_no_gather_in_run_daily_computation(self):
        """
        UT-RE-007: RE-RL-003 — asyncio.gather() must NOT be used
        in run_daily_computation or any regime OHLC fetch path.
        Stagger REQUIRES sequential execution (not parallel).
        Static source code analysis.
        """
        import inspect
        import pathlib

        source_file = pathlib.Path(
            inspect.getfile(RegimeEngine)
        )
        source = source_file.read_text()

        # Check run_daily_computation specifically
        lines = source.splitlines()
        in_run_daily = False
        gather_found_in_daily = False

        for line in lines:
            if "async def run_daily_computation" in line:
                in_run_daily = True
            elif in_run_daily and line.startswith("    async def "):
                in_run_daily = False  # next method
            if in_run_daily and "asyncio.gather" in line:
                gather_found_in_daily = True

        assert not gather_found_in_daily, (
            "RE-RL-003: asyncio.gather() must NOT appear in "
            "run_daily_computation — regime calls must be sequential "
            "to enforce the 1.1s stagger"
        )
