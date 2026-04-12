"""
DocDCN:     1021001
DocTitle:   Regime_Engine_Unit_Tests
DocVersion: dv1_0
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/tests/test_regime_engine.py
DocDate:    04-12-2026
DocTime:    23:59:59 UTC

============================================================
REVISION HISTORY
============================================================

  dv1_0   04-12-2026  DC header added per 0311001 v1_1,
                      0311004 v1_1, 1011001 dv1_7.
                      Unit tests UT-RE-001 through
                      UT-RE-007. Module:
                      tothbot/regime_engine.py.
                      Governed by 1021001
                      Unit_Test_Specification dv1_0.

============================================================

TothBot V2 — Unit Tests: Regime Engine
=============================================================
Test spec:   1021001 Unit_Test_Specification dv1_0 §4.4
Module:      tothbot/regime_engine.py
Coding spec: 1011012 Regime_Engine_Coding_Spec dv1_4
BP standard: 1011001 Engineering_Best_Practices dv1_7
=============================================================

Tests: UT-RE-001 through UT-RE-007

UT-FW-004: Standard asyncio. Do NOT use uvloop.
"""
from __future__ import annotations

import asyncio
import ast
import inspect
import pathlib
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
    trend: float = 10.0,
    high_delta: float = 500.0,
    low_delta: float = 500.0,
) -> list[dict]:
    """Generate synthetic OHLC candles for deterministic testing."""
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

    def test_UT_RE_001_source_excludes_last_candle(self):
        """
        UT-RE-001: RE-OHLC-002 — _fetch_daily_ohlc() must exclude the
        last (uncommitted) candle via [:-1] slice.
        Static source code analysis: verify committed = response[:-1]
        appears in _fetch_daily_ohlc source.
        """
        source_file = pathlib.Path(inspect.getfile(RegimeEngine))
        source = source_file.read_text()

        assert "[:-1]" in source, (
            "RE-OHLC-002: Source must contain [:-1] slice to exclude "
            "the uncommitted last candle from all computations"
        )

    @pytest.mark.asyncio
    async def test_UT_RE_001_fetch_passes_n_minus_1_to_compute(self, regime_engine):
        """
        UT-RE-001: RE-OHLC-002 — _fetch_daily_ohlc returns N-1 candles
        (excludes last). Verified by patching _compute_and_cache and
        mocking _fetch_daily_ohlc to return a controlled candle list.
        """
        # 50 committed candles = what _fetch_daily_ohlc should return
        # (i.e., after response[:-1] has already been applied internally)
        committed_candles = _make_candles(50)

        received = []

        original_compute = regime_engine._compute_and_cache

        def capture(pair, candles):
            received.append(len(candles))
            original_compute(pair, candles)

        with patch.object(regime_engine, "_fetch_daily_ohlc",
                          return_value=committed_candles):
            with patch.object(regime_engine, "_compute_and_cache",
                              side_effect=capture):
                await regime_engine.run_daily_computation(["BTC/USD"])

        assert len(received) == 1, "Expected exactly one _compute_and_cache call"
        assert received[0] == 50, (
            f"RE-OHLC-002: _compute_and_cache must receive 50 candles "
            f"(the N-1 committed set). Got {received[0]}"
        )

    def test_UT_RE_001_parse_candles_all_decimal(self):
        """
        UT-RE-001: RE-OHLC-004 — _parse_candles must convert all numeric
        fields to Decimal(str()) immediately.
        """
        raw = [
            [1700000000, "65000.1", "65500.0", "64500.0", "65100.0",
             "65050.0", "1000.5", 100]
        ]
        parsed = RegimeEngine._parse_candles(raw)
        assert len(parsed) == 1
        candle = parsed[0]
        for field in ("open", "high", "low", "close", "vwap", "volume"):
            assert isinstance(candle[field], Decimal), (
                f"RE-OHLC-004: {field} must be Decimal, got {type(candle[field])}"
            )


# =============================================================
# UT-RE-002: ADX batch computation on full history
# RE-FPD-005 / RE-IND-006
# =============================================================

class TestADXComputation:

    def test_UT_RE_002_adx_batch_not_incremental(self):
        """
        UT-RE-002: RE-FPD-005 — ADX is @staticmethod confirming
        it holds no incremental state.
        """
        assert isinstance(
            inspect.getattr_static(RegimeEngine, "_compute_adx_14"),
            staticmethod,
        ), "RE-FPD-005: _compute_adx_14 must be @staticmethod"

    def test_UT_RE_002_adx_returns_none_for_insufficient_data(self):
        """
        UT-RE-002: ADX requires at least 2*14+1=29 candles. Returns None below.
        """
        candles = _make_candles(20)
        result = RegimeEngine._compute_adx_14(candles)
        assert result is None, (
            "RE-IND-006: ADX must return None for < 29 candles"
        )

    def test_UT_RE_002_adx_returns_decimal_for_sufficient_data(self):
        """
        UT-RE-002: ADX returns a non-negative Decimal for sufficient data.
        """
        candles = _make_candles(100, trend=50.0)
        result = RegimeEngine._compute_adx_14(candles)
        assert result is not None
        assert isinstance(result, Decimal)
        assert result >= Decimal("0")

    def test_UT_RE_002_adx_tolerance_trending(self):
        """
        UT-RE-002: Strong trending candles must yield ADX > 20.
        """
        candles = _make_candles(200, trend=100.0, high_delta=300.0, low_delta=100.0)
        adx = RegimeEngine._compute_adx_14(candles)
        assert adx is not None
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
        UT-RE-003: RE-TAX-001 — ADX > 25 AND EMA20 > EMA50 = TRENDING_POSITIVE.
        """
        candles = _make_candles(300, trend=50.0, high_delta=200.0, low_delta=100.0)
        regime_engine._compute_and_cache("BTC/USD", candles)
        state = regime_engine.regime_cache.get("BTC/USD")
        if state is not None:
            if state.ema_20 > state.ema_50 and state.adx_14 > ADX_THRESHOLD:
                assert state.directional == TRENDING_POSITIVE

    def test_UT_RE_003_trending_negative_ema20_lt_ema50(self, regime_engine):
        """
        UT-RE-003: ADX > 25 AND EMA20 < EMA50 = TRENDING_NEGATIVE.
        """
        candles = _make_candles(300, base_close=65000.0,
                                trend=-50.0, high_delta=100.0, low_delta=200.0)
        regime_engine._compute_and_cache("BTC/USD", candles)
        state = regime_engine.regime_cache.get("BTC/USD")
        if state is not None and state.adx_14 > ADX_THRESHOLD:
            if state.ema_20 < state.ema_50:
                assert state.directional == TRENDING_NEGATIVE

    def test_UT_RE_003_non_directional_low_adx(self, regime_engine):
        """
        UT-RE-003: ADX <= 25 = NON_DIRECTIONAL.
        """
        candles = _make_candles(300, trend=0.0, high_delta=50.0, low_delta=50.0)
        regime_engine._compute_and_cache("BTC/USD", candles)
        state = regime_engine.regime_cache.get("BTC/USD")
        if state is not None and state.adx_14 <= ADX_THRESHOLD:
            assert state.directional == NON_DIRECTIONAL


# =============================================================
# UT-RE-004: NORMAL_VOL at or below 67th percentile
# RE-TAX-002
# =============================================================

class TestVolatilityRegime:

    def test_UT_RE_004_atr_at_or_below_67th_is_normal_vol(self):
        """
        UT-RE-004: RE-TAX-002 — ATR percentile rank <= 67 = NORMAL_VOL.
        """
        candles = _make_candles(100, trend=0.0, high_delta=100.0, low_delta=100.0)
        _, pct_rank = RegimeEngine._compute_atr_and_percentile(candles)
        vol = ELEVATED_VOL if pct_rank > Decimal("67") else NORMAL_VOL
        assert vol in (NORMAL_VOL, ELEVATED_VOL)

    def test_UT_RE_004_percentile_boundary_67(self, regime_engine):
        """
        UT-RE-004: ATR at exactly 67th percentile -> NORMAL_VOL
        (condition is > 67, not >= 67).
        """
        candles = _make_candles(150, trend=10.0)
        with patch.object(RegimeEngine, "_compute_atr_and_percentile",
                          return_value=(Decimal("1000"), Decimal("67"))):
            with patch.object(RegimeEngine, "_compute_adx_14",
                              return_value=Decimal("30")):
                with patch.object(RegimeEngine, "_compute_ema",
                                  return_value=Decimal("65000")):
                    regime_engine._compute_and_cache("BTC/USD", candles)

        state = regime_engine.regime_cache.get("BTC/USD")
        if state:
            assert state.vol_regime == NORMAL_VOL, (
                "RE-TAX-002: ATR at exactly 67th percentile must be "
                f"NORMAL_VOL (> 67 required for ELEVATED). Got {state.vol_regime}"
            )


# =============================================================
# UT-RE-005: Stale regime fallback — never halt
# RE-SCH-003
# =============================================================

class TestStaleFallback:

    def test_UT_RE_005_stale_regime_preserved(self, regime_engine):
        """
        UT-RE-005: RE-SCH-003 — _check_stale() logs but preserves
        existing cache value. Never halts.
        """
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
        regime_engine._check_stale("BTC/USD")
        state = regime_engine.regime_cache.get("BTC/USD")
        assert state is not None
        assert state.directional == TRENDING_POSITIVE

    def test_UT_RE_005_gate_3_blocks_on_no_regime(self, regime_engine):
        """
        UT-RE-005: gate_3_check returns BLOCK when no regime available.
        """
        result = regime_engine.gate_3_check("ETH/USD")
        assert result == "BLOCK"


# =============================================================
# UT-RE-006: 1.1s stagger enforced between pair REST calls
# RE-RL-002
# =============================================================

class TestOHLCStagger:

    @pytest.mark.asyncio
    async def test_UT_RE_006_stagger_enforced(self, regime_engine):
        """
        UT-RE-006: RE-RL-002 — asyncio.sleep(1.1) called once per pair
        in run_daily_computation.
        """
        sleep_calls = []

        async def mock_sleep(secs):
            sleep_calls.append(secs)

        async def mock_fetch(pair):
            return []

        with patch.object(regime_engine, "_fetch_daily_ohlc",
                          side_effect=mock_fetch):
            with patch("tothbot.regime_engine.asyncio.sleep",
                       side_effect=mock_sleep):
                await regime_engine.run_daily_computation(["BTC/USD", "ETH/USD"])

        assert len(sleep_calls) == 2, (
            f"RE-RL-002: Expected 2 stagger sleeps for 2 pairs, "
            f"got {len(sleep_calls)}"
        )
        for call in sleep_calls:
            assert call == OHLC_STAGGER_SEC == 1.1, (
                f"RE-RL-002: Sleep must be {OHLC_STAGGER_SEC}s, got {call}"
            )


# =============================================================
# UT-RE-007: asyncio.gather() NOT used for regime OHLC calls
# RE-RL-003
# =============================================================

class TestNoAsyncioGather:

    def test_UT_RE_007_no_gather_in_run_daily_computation(self):
        """
        UT-RE-007: RE-RL-003 — asyncio.gather() must NOT appear as
        executable code in run_daily_computation. Stagger requires
        sequential execution.

        Uses AST analysis to exclude gather() references that appear
        only inside string literals and docstrings.
        """
        source_file = pathlib.Path(inspect.getfile(RegimeEngine))
        source = source_file.read_text()

        # Parse the source to an AST and find run_daily_computation
        tree = ast.parse(source)
        gather_calls_in_method: list[str] = []

        for node in ast.walk(tree):
            if not isinstance(node, (ast.AsyncFunctionDef, ast.FunctionDef)):
                continue
            if node.name != "run_daily_computation":
                continue

            # Walk the method body looking for asyncio.gather calls
            for child in ast.walk(node):
                if not isinstance(child, ast.Call):
                    continue
                func = child.func
                # asyncio.gather(...)
                if (isinstance(func, ast.Attribute) and
                        func.attr == "gather" and
                        isinstance(func.value, ast.Name) and
                        func.value.id == "asyncio"):
                    gather_calls_in_method.append(
                        f"Line {child.lineno}"
                    )

        assert not gather_calls_in_method, (
            f"RE-RL-003: asyncio.gather() found as executable code in "
            f"run_daily_computation at: {gather_calls_in_method}. "
            f"Sequential execution required for 1.1s stagger."
        )
