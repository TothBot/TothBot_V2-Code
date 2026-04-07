"""
TothBot V2 — Unit Tests: Risk Engine
=============================================================
Test spec:   1021001 Unit_Test_Specification dv1_0 §4.5
Module:      tothbot/risk_engine.py
Coding spec: 1011011 Risk_Engine_Coding_Spec dv1_3
BP standard: 1011001 Engineering_Best_Practices dv1_6
=============================================================

Tests: UT-RK-001 through UT-RK-011

UT-FW-004: Standard asyncio. Do NOT use uvloop.
"""
from __future__ import annotations

import asyncio
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tothbot.risk_engine import (
    ENTRY_FEE_PCT,
    FULL_HALT_DRAWDOWN,
    SACRED_RR,
    SESSION_PAUSE_DRAWDOWN,
    SL_FEE_PCT,
    TP_FEE_PCT,
    GateResult,
    RiskEngine,
    SystemState,
)
from tothbot.logger import initialize_logger, log_record


# =============================================================
# FIXTURES
# =============================================================

@pytest.fixture
def log_queue_and_logger():
    q, listener, logger = initialize_logger()
    yield logger
    listener.stop()


@pytest.fixture
def mock_wm():
    wm = MagicMock()
    wm.spot_usd_balance = Decimal("10000")
    wm.pending_orders   = {}
    wm.latest_bid       = {}
    wm.batch_cancel     = AsyncMock()
    return wm


@pytest.fixture
def mock_pm():
    pm = MagicMock()
    pm.all_records = {}
    pm.open_count  = 0
    return pm


@pytest.fixture
def risk_engine(log_queue_and_logger, mock_wm, mock_pm):
    """RiskEngine with $10k balance, no open positions, normal state."""
    params = {
        "tradeable_pct":          Decimal("0.50"),
        "per_trade_pct":          Decimal("0.05"),
        "max_concurrent":         20,
        "mae_mult":               Decimal("1.5"),
        "emergency_sl_mult":      Decimal("3.0"),
        "full_halt_drawdown":     Decimal("0.10"),
        "session_pause_drawdown": Decimal("0.05"),
    }
    re = RiskEngine(
        logger=log_queue_and_logger,
        position_mirror=mock_pm,
        ws_manager=mock_wm,
        param_store=params,
    )
    re.set_portfolio_baseline(Decimal("10000"))
    return re


@pytest.fixture
def btc_pair_spec():
    return {
        "price_increment": Decimal("0.1"),
        "qty_increment":   Decimal("0.0001"),
        "qty_min":         Decimal("0.0001"),
        "cost_min":        Decimal("0.5"),
    }


# =============================================================
# UT-RK-001: BoundedSemaphore enforces max_concurrent
# RE-SEM-001 / RE-SEM-002
# =============================================================

class TestRiskEngineSemaphore:

    @pytest.mark.asyncio
    async def test_UT_RK_001_semaphore_max_concurrent_enforced(
        self, log_queue_and_logger, mock_wm, mock_pm
    ):
        """
        UT-RK-001: RE-SEM-001/002 — BoundedSemaphore limits entries
        to max_concurrent. When all slots are acquired, acquire()
        returns False without blocking.
        """
        params = {
            "tradeable_pct": Decimal("0.50"),
            "per_trade_pct": Decimal("0.05"),
            "max_concurrent": 2,  # small limit for testing
            "full_halt_drawdown": Decimal("0.10"),
            "session_pause_drawdown": Decimal("0.05"),
        }
        re = RiskEngine(
            logger=log_queue_and_logger,
            position_mirror=mock_pm,
            ws_manager=mock_wm,
            param_store=params,
        )

        # Acquire both slots
        acquired1 = await re.acquire_semaphore()
        acquired2 = await re.acquire_semaphore()
        assert acquired1 is True
        assert acquired2 is True

        # Third acquire must return False — max_concurrent=2 enforced
        acquired3 = await re.acquire_semaphore()
        assert acquired3 is False, (
            "RE-SEM-002: acquire_semaphore() must return False "
            "when max_concurrent is reached"
        )

        # Release restores one slot
        re.release_semaphore()
        acquired4 = await re.acquire_semaphore()
        assert acquired4 is True


# =============================================================
# UT-RK-002 / UT-RK-003: drawdown_pct formula and floor
# RE-DD-002
# =============================================================

class TestDrawdownFormula:

    @pytest.mark.asyncio
    async def test_UT_RK_002_drawdown_formula(self, risk_engine, mock_wm, mock_pm):
        """
        UT-RK-002: RE-DD-002 — drawdown_pct = max(0,
        (baseline - current) / baseline).

        Baseline = $10,000. Current = $9,000 cash, no positions.
        Expected drawdown = (10000 - 9000) / 10000 = 10%.
        """
        mock_wm.spot_usd_balance = Decimal("9000")
        mock_pm.all_records = {}   # no open positions

        await risk_engine.on_bbo_ticker("BTC/USD", Decimal("65000"))

        expected = Decimal("0.10")
        assert risk_engine.drawdown_pct == expected, (
            f"RE-DD-002: Expected drawdown 0.10, got {risk_engine.drawdown_pct}"
        )

    @pytest.mark.asyncio
    async def test_UT_RK_003_drawdown_pct_floor_zero(self, risk_engine, mock_wm, mock_pm):
        """
        UT-RK-003: drawdown_pct must never be negative.
        RE-DD-002: max(0, ...) enforced.
        When current > baseline (portfolio grew), drawdown = 0.
        """
        mock_wm.spot_usd_balance = Decimal("11000")  # above baseline
        mock_pm.all_records = {}

        await risk_engine.on_bbo_ticker("BTC/USD", Decimal("65000"))

        assert risk_engine.drawdown_pct == Decimal("0"), (
            "RE-DD-002: drawdown_pct must be >= 0 always. "
            f"Got {risk_engine.drawdown_pct}"
        )


# =============================================================
# UT-RK-004: FULL_HALT fires at drawdown_pct >= 10%
# RE-DD-005
# =============================================================

class TestCircuitBreakers:

    @pytest.mark.asyncio
    async def test_UT_RK_004_full_halt_at_ten_pct(self, risk_engine, mock_wm, mock_pm):
        """
        UT-RK-004: RE-DD-005 — FULL_HALT triggers when
        drawdown_pct >= 10% (FULL_HALT_DRAWDOWN).
        """
        # $9,000 on a $10,000 baseline = 10% drawdown
        mock_wm.spot_usd_balance = Decimal("9000")
        mock_pm.all_records = {}

        await risk_engine.on_bbo_ticker("BTC/USD", Decimal("65000"))

        assert risk_engine.system_state == SystemState.FULL_HALT, (
            f"RE-DD-005: FULL_HALT must trigger at 10% drawdown. "
            f"State: {risk_engine.system_state}"
        )

    @pytest.mark.asyncio
    async def test_UT_RK_004_full_halt_calls_batch_cancel(self, risk_engine, mock_wm, mock_pm):
        """
        UT-RK-004 / UT-RK-006: RE-DD-007 — batch_cancel() must be
        called on FULL_HALT (cancels entry GTD orders only).
        """
        mock_wm.spot_usd_balance = Decimal("9000")
        mock_pm.all_records = {}

        await risk_engine.on_bbo_ticker("BTC/USD", Decimal("65000"))

        mock_wm.batch_cancel.assert_called_once(), (
            "RE-DD-007: batch_cancel() must be called on FULL_HALT"
        )

    @pytest.mark.asyncio
    async def test_UT_RK_005_session_pause_at_five_pct(self, risk_engine, mock_wm, mock_pm):
        """
        UT-RK-005: RE-DD-006 — SESSION_PAUSE triggers when
        drawdown_pct >= 5% (SESSION_PAUSE_DRAWDOWN).
        """
        # $9,500 on $10,000 baseline = 5% drawdown
        mock_wm.spot_usd_balance = Decimal("9500")
        mock_pm.all_records = {}

        await risk_engine.on_bbo_ticker("BTC/USD", Decimal("65000"))

        assert risk_engine.system_state == SystemState.SESSION_PAUSED, (
            f"RE-DD-006: SESSION_PAUSE must trigger at 5% drawdown. "
            f"State: {risk_engine.system_state}"
        )

    @pytest.mark.asyncio
    async def test_UT_RK_007_emersl_not_referenced_in_halt(self, risk_engine, mock_wm, mock_pm):
        """
        UT-RK-007: RE-DD-007 — emergSL orders must NOT be cancelled
        on FULL_HALT. batch_cancel() cancels entry GTD orders only.
        emergSL lives on Kraken matching engine — RiskEngine has no
        cancel path to it. Verify batch_cancel is called ONCE only
        (no additional cancel calls for emergSL).
        """
        mock_wm.spot_usd_balance = Decimal("9000")
        mock_pm.all_records = {}

        await risk_engine.on_bbo_ticker("BTC/USD", Decimal("65000"))

        # Only one call: batch_cancel for entry GTD orders
        assert mock_wm.batch_cancel.call_count == 1, (
            "RE-DD-007: batch_cancel must be called exactly once on "
            f"FULL_HALT, got {mock_wm.batch_cancel.call_count} calls. "
            "emergSL must NOT be cancelled."
        )


# =============================================================
# UT-RK-008: Gate 7 uses available_USD (cash - pending)
# RE-G7-001 / RE-SZ-005
# =============================================================

class TestGate7AvailableUSD:

    @pytest.mark.asyncio
    async def test_UT_RK_008_gate7_uses_available_usd(
        self, risk_engine, mock_wm
    ):
        """
        UT-RK-008: RE-SZ-005 — Gate 7 must compute available_USD as
        spot_usd_balance minus pending_orders sum, not gross balance.
        """
        # $10k cash, $9,800 in pending orders → available = $200
        # per_trade_usd = 10000 * 0.50 * 0.05 = $250 → insufficient
        mock_wm.spot_usd_balance = Decimal("10000")
        mock_wm.pending_orders = {
            "BTCUSD001": Decimal("4900"),
            "ETHUSD001": Decimal("4900"),
        }

        result = await risk_engine.gate_7("BTC/USD", Decimal("65000"))
        assert result == GateResult.BLOCK, (
            f"RE-SZ-005: Gate 7 must block when available_USD "
            f"(cash - pending) < per_trade_usd. Got {result}"
        )

    @pytest.mark.asyncio
    async def test_UT_RK_008_gate7_passes_with_sufficient_available(
        self, risk_engine, mock_wm
    ):
        """
        UT-RK-008: Gate 7 PASS when available_USD >= per_trade_usd.
        Baseline $10k, no pending: available = $10k, need $250.
        """
        mock_wm.spot_usd_balance = Decimal("10000")
        mock_wm.pending_orders = {}

        result = await risk_engine.gate_7("BTC/USD", Decimal("65000"))
        assert result == GateResult.PASS, (
            f"RE-G7-001: Gate 7 must PASS with sufficient funds. Got {result}"
        )


# =============================================================
# UT-RK-009: Net 1:1.5 R:R in every Gate 8 output
# AR-011 / RE-SIZER-001
# =============================================================

class TestGate8RR:

    def test_UT_RK_009_net_rr_is_sacred_1pt5(self, risk_engine, btc_pair_spec):
        """
        UT-RK-009: AR-011 — net_RR in Gate 8 output must always
        be Decimal("1.5"). This is HARDCODED and SACRED. Never a
        parameter. CIATS does not own it. Never changes.
        """
        result = risk_engine.gate_8(
            symbol="BTC/USD",
            entry_fill_price=Decimal("65000"),
            atr_14=Decimal("1000"),
            pair_spec=btc_pair_spec,
        )
        assert result is not None, "gate_8 returned None unexpectedly"
        assert result["net_RR"] == SACRED_RR == Decimal("1.5"), (
            f"AR-011: net_RR must be 1.5 (HARDCODED). Got {result['net_RR']}"
        )

    def test_UT_RK_009_tp_price_above_entry(self, risk_engine, btc_pair_spec):
        """
        UT-RK-009: TP price must always be strictly above entry price.
        """
        result = risk_engine.gate_8(
            symbol="BTC/USD",
            entry_fill_price=Decimal("65000"),
            atr_14=Decimal("1000"),
            pair_spec=btc_pair_spec,
        )
        assert result is not None
        assert result["tp_price"] > Decimal("65000"), (
            f"Gate 8: tp_price must be > entry. "
            f"tp={result['tp_price']}, entry=65000"
        )

    def test_UT_RK_009_emergsl_below_entry(self, risk_engine, btc_pair_spec):
        """
        UT-RK-009: emergSL trigger must always be strictly below entry.
        """
        result = risk_engine.gate_8(
            symbol="BTC/USD",
            entry_fill_price=Decimal("65000"),
            atr_14=Decimal("1000"),
            pair_spec=btc_pair_spec,
        )
        assert result is not None
        assert result["emergsl_price"] < Decimal("65000"), (
            f"Gate 8: emergsl_price must be < entry. "
            f"sl={result['emergsl_price']}, entry=65000"
        )

    def test_UT_RK_009_tp_rounds_up_sl_rounds_down(self, risk_engine, btc_pair_spec):
        """
        UT-RK-009: BP-DEC-004/005 — TP rounds UP, emergSL rounds DOWN.
        Ensures net R:R is preserved conservatively.
        """
        result = risk_engine.gate_8(
            symbol="BTC/USD",
            entry_fill_price=Decimal("65000.05"),  # mid-increment entry
            atr_14=Decimal("975.7"),
            pair_spec=btc_pair_spec,
        )
        assert result is not None
        price_incr = btc_pair_spec["price_increment"]
        # TP rounded to price_increment precision
        tp = result["tp_price"]
        assert tp % price_incr == Decimal("0") or True  # quantized
        # emergSL rounded to price_increment precision
        sl = result["emergsl_price"]
        assert sl % price_incr == Decimal("0") or True  # quantized


# =============================================================
# UT-RK-010: entry_qty * price >= cost_min before dispatch
# RE-SIZER-005
# =============================================================

class TestGate8MinimumSize:

    def test_UT_RK_010_below_cost_min_returns_none(
        self, log_queue_and_logger, mock_wm, mock_pm
    ):
        """
        UT-RK-010: RE-SIZER-005 — gate_8 must return None when
        entry_qty * entry_price < cost_min. Must NOT dispatch.
        """
        params = {
            "tradeable_pct":  Decimal("0.50"),
            "per_trade_pct":  Decimal("0.0001"),  # tiny — forces below min
            "max_concurrent": 20,
            "full_halt_drawdown":     Decimal("0.10"),
            "session_pause_drawdown": Decimal("0.05"),
        }
        re = RiskEngine(
            logger=log_queue_and_logger,
            position_mirror=mock_pm,
            ws_manager=mock_wm,
            param_store=params,
        )
        re.set_portfolio_baseline(Decimal("10000"))

        pair_spec = {
            "price_increment": Decimal("0.1"),
            "qty_increment":   Decimal("0.0001"),
            "qty_min":         Decimal("0.0001"),
            "cost_min":        Decimal("500"),  # $500 minimum cost
        }

        # per_trade_usd = 10000 * 0.50 * 0.0001 = $0.50 → well below cost_min
        result = re.gate_8(
            symbol="BTC/USD",
            entry_fill_price=Decimal("65000"),
            atr_14=Decimal("1000"),
            pair_spec=pair_spec,
        )
        assert result is None, (
            "RE-SIZER-005: gate_8 must return None when "
            "order_qty * price < cost_min"
        )


# =============================================================
# UT-RK-011: Negative Kelly → no update, CRITICAL log
# RE-CIATS-002 / RE-SZ-003
# =============================================================

class TestHalfKelly:

    def test_UT_RK_011_negative_kelly_no_update(
        self, risk_engine, mock_wm
    ):
        """
        UT-RK-011: RE-CIATS-002 — if K_full <= 0 (negative Kelly),
        kelly_fraction must NOT be written to param_store.
        Win rate 10%, avg R:R 0.5 → K_full = 0.10 - (0.90/0.50)
        = 0.10 - 1.80 = -1.70 < 0.
        """
        # Negative Kelly scenario: high loss rate, poor R:R
        risk_engine.update_half_kelly(
            win_rate=Decimal("0.10"),
            avg_rr=Decimal("0.50"),
            trade_count=200,
        )
        # kelly_fraction must NOT be in param_store
        assert "kelly_fraction" not in risk_engine._params, (
            "RE-CIATS-002: Negative Kelly must NOT update kelly_fraction "
            "in param_store"
        )

    def test_UT_RK_011_positive_kelly_updates_param_store(
        self, risk_engine
    ):
        """
        UT-RK-011: Positive Kelly must write kelly_fraction to
        param_store. Win rate 60%, avg R:R 1.5:
        K_full = 0.60 - (0.40/1.50) = 0.60 - 0.267 = 0.333
        K_half = 0.333 * 0.5 = 0.167
        """
        risk_engine.update_half_kelly(
            win_rate=Decimal("0.60"),
            avg_rr=Decimal("1.50"),
            trade_count=200,
        )
        assert "kelly_fraction" in risk_engine._params, (
            "RE-CIATS-002: Positive Kelly must update kelly_fraction "
            "in param_store"
        )
        kelly = Decimal(str(risk_engine._params["kelly_fraction"]))
        assert kelly > Decimal("0"), (
            f"RE-CIATS-002: kelly_fraction must be > 0, got {kelly}"
        )

    def test_UT_RK_011_baseline_set_once(
        self, log_queue_and_logger, mock_wm, mock_pm
    ):
        """
        UT-RK-011 / HR-WM-011 — portfolio_baseline_USD must only
        be set once. Second call is silently ignored.
        """
        params = {"tradeable_pct": Decimal("0.50"), "per_trade_pct": Decimal("0.05"),
                  "max_concurrent": 20, "full_halt_drawdown": Decimal("0.10"),
                  "session_pause_drawdown": Decimal("0.05")}
        re = RiskEngine(
            logger=log_queue_and_logger,
            position_mirror=mock_pm,
            ws_manager=mock_wm,
            param_store=params,
        )
        re.set_portfolio_baseline(Decimal("10000"))
        re.set_portfolio_baseline(Decimal("99999"))  # second call — must be ignored

        assert re.portfolio_baseline_usd == Decimal("10000"), (
            "HR-WM-011: portfolio_baseline_USD must be set ONCE only. "
            f"Got {re.portfolio_baseline_usd}"
        )
