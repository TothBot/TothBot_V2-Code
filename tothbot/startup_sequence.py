"""
DocDCN:     1011014
DocTitle:   Startup_Sequence
DocVersion: dv1_4
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/tothbot/startup_sequence.py
DocDate:    04-13-2026
DocTime:    02:30:00 UTC

============================================================
REVISION HISTORY
============================================================

  dv1_4   04-13-2026  DEFECT FIX: ExecutionEngine instantiation missing
                      position_mirror=pm argument. Added position_mirror=pm
                      to ExecutionEngine() call in _build_components().
                      Crash on startup resolved.

  dv1_3   04-12-2026  DC header added per 0311001 v1_1, 0311004 v1_1,
                      1011001 dv1_7. No code logic changes.

  dv1_3   04-05-2026  Initial Phase 8 implementation.
                      Written to 1011014 Startup_Sequence_Coding_Spec dv1_3.

============================================================

Top-level entry point and system orchestrator for TothBot V2.

Responsibilities:
  1. Load configuration from environment (VD-KEY-003).
  2. Initialise Logger (sole data interface to CIATS).
  3. Execute Step 1 of startup: Kraken Status API check
     (non-blocking — SS-STARTUP-001 through SS-STARTUP-003).
  4. Build shared Parameter Store with CIATS starting values.
  5. Instantiate and wire all system components:
       RegimeEngine, RiskEngine, PositionMirror,
       ExecutionEngine, ExitController, SignalPipeline,
       LongModule, SelectionController, CIATS, WSManager.
  6. Launch CIATS as a background asyncio.Task.
  7. Run WSManager.run() — drives startup Steps 2–10
     and continuous operation thereafter.

Steps 2–10 (WS connect, subscribe, reconcile, warm-up,
regime seed, READY detection, pipeline, watchdog) are
owned and executed by WSManager per 1011014 dv1_3 Sections
6–9. startup_sequence.py does NOT duplicate WS logic.

Startup Sequence (1011014 dv1_3 Section 6):
  Step 1   Kraken Status API check         — THIS MODULE
  Step 2   Acquire WS token                — WSManager
  Step 3   Connect public WS + subscribe   — WSManager
  Step 4   Process instrument snapshot     — WSManager
  Step 5   Connect private WS + subscribe  — WSManager
  Step 6   Reconcile open positions        — WSManager
  Step 7   Restore ticker for positions    — WSManager
  Step 8   REST warm-up GetOHLCData        — WSManager
  Step 9   At least one READY pair         — WSManager
  Step 10  Continuous operation            — WSManager

sd_notify READY=1: sent by WSManager.run() after Step 9
(SS-STARTUP-026). This module does NOT send sd_notify.

Reconnect sequence (1011014 dv1_3 Section 9):
  Steps R1–R11 owned entirely by WSManager.
  portfolio_baseline_USD NEVER reset on reconnect.
  system_state PRESERVED across reconnect.

Hard Rules (this module):
  SS-PRE-001  Python 3.12.3, Ubuntu 24.04.4 LTS.
  SS-PRE-002  API keys DATA + TRADE from os.environ only.
  SS-PRE-005  uvloop.new_event_loop() for asyncio event loop.
  HR-WM-011   portfolio_baseline_USD set ONCE at startup (Step 5).
  VD-KEY-003  ALL credentials from os.environ. NEVER hardcoded.
  AR-055      cancel_all_orders_after PROHIBITED. Never referenced here.
  AR-058      max_queue=None on all WS connections (enforced by WSManager).
  AR-059      ping_interval=None (enforced by WSManager).
  AR-060      websockets.asyncio.client.connect (enforced by WSManager).

Deployment:
  VPS:    Hetzner CPX22, 87.99.141.44, Ubuntu 24.04.4 LTS
  Python: /root/tothbot_env/bin/python3 (3.12.3)
  Keys:   /root/.tothbot.env (chmod 600, sourced by systemd EnvironmentFile)
  Entry:  python -m tothbot.startup_sequence  (or via tothbot.service)
  Logs:   /root/TothBot_V2-Code/logs/tothbot.log
============================================================
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
from decimal import Decimal
from pathlib import Path
from typing import Any

import uvloop

from tothbot.vps_deployment import check_kraken_status
from tothbot.logger import (
    LOG_FILE,
    initialize_logger,
    log_record,
)
from tothbot.regime_engine import RegimeEngine
from tothbot.risk_engine import RiskEngine
from tothbot.position_mirror import PositionMirror
from tothbot.execution_engine import ExecutionEngine
from tothbot.exit_controller import ExitController
from tothbot.signal_pipeline import SignalPipeline
from tothbot.long_module import LongModule
from tothbot.selection_ctrl import SelectionController
from tothbot.ciats import CIATS
from tothbot.ws_manager import WSManager, OHLCCandle


# =============================================================
# REQUIRED ENVIRONMENT VARIABLES  (VD-KEY-003)
# =============================================================

_REQUIRED_ENV_VARS: tuple[str, ...] = (
    "KRAKEN_DATA_API_KEY",
    "KRAKEN_DATA_API_SECRET",
    "KRAKEN_TRADE_API_KEY",
    "KRAKEN_TRADE_API_SECRET",
    # SMTP vars are optional — alert silently fails if absent
)


# =============================================================
# CIATS PARAMETER STORE — STARTING VALUES  (TB00000 §9.17)
# =============================================================
# All values are CIATS-owned starting values.
# CIATS overwrites them at inter-trade boundaries.
# The only truly immutable value is net 1:1.5 R:R, which is
# hardcoded in RiskEngine and ExecutionEngine — not here.

def _build_param_store() -> dict:
    """
    Build the shared CIATS Parameter Store with starting values.
    CIATS writes to this dict; all other components read
    a frozen snapshot at pipeline start (AR-I-4).

    Starting values per TB00000 §9.17 and 1011010 dv1_6.
    """
    return {
        # Position sizing (§9.3, §9.4)
        "tradeable_pct":           Decimal("0.50"),
        "per_trade_pct":           Decimal("0.05"),
        # Max concurrent positions (§9.5)
        "max_concurrent":          20,
        # Exit Controller parameters (1011005 dv1_2)
        "mae_mult":                Decimal("1.5"),    # ATR(14) x 1.5x for Layer 2
        "emergency_sl_mult":       Decimal("3.0"),    # ATR(14) x 3.0x for Layer 3
        "cancel_timeout":          5.0,               # seconds — cancel ACK wait
        "mpp_retry_count":         3,                 # MPP IOC retry attempts
        "max_hold_candles":        24,                # max hold in 5m candles
        # Entry timing
        "entry_timeout_sec":       45,                # GTD window seconds
        # Drawdown circuit breakers (§9.17)
        "full_halt_drawdown":      Decimal("0.10"),   # 10% → full halt
        "session_pause_drawdown":  Decimal("0.05"),   # 5% → session pause
        # Signal / regime parameters (§9.17)
        "adx_threshold":           Decimal("25"),
        "atr_percentile_thresh":   67,                # 67th percentile
        "htf_ema_fast":            20,                # daily EMA fast period
        "htf_ema_slow":            50,                # daily EMA slow period
        "min_volume_usd_daily":    Decimal("500000"), # $500k USD Gate 2
        # Selection Controller parameters (1011009 dv1_0)
        "sc_body_threshold":       Decimal("0.3"),    # ATR multiple
        "sc_cooldown_seconds":     300,               # seconds post-exit
        "sc_consecutive_limit":    3,                 # consecutive loss limit
        # CIATS Half-Kelly (activated at 200 closed trades)
        "half_kelly_active":       False,
        "kelly_win_rate":          None,              # None until 200 trades
        "kelly_avg_rr":            None,              # None until 200 trades
    }


# =============================================================
# CONFIGURATION LOADER
# =============================================================

def _load_config() -> dict:
    """
    Load all configuration from environment variables.
    Fails fast if any required variable is missing (SS-PRE-002).
    API keys read from os.environ ONLY — NEVER hardcoded (VD-KEY-003).
    """
    missing: list[str] = [v for v in _REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        # BP-ERR-001: log before handling
        print(
            f"[STARTUP FATAL] Missing required environment variables: {missing}",
            file=sys.stderr,
        )
        sys.exit(1)

    return {
        # Kraken DATA key pair — market data, public REST (AR-004)
        "kraken_data_api_key":    os.environ["KRAKEN_DATA_API_KEY"],
        "kraken_data_api_secret": os.environ["KRAKEN_DATA_API_SECRET"],
        # Kraken TRADE key pair — private WS, order dispatch (AR-004)
        "kraken_trade_api_key":   os.environ["KRAKEN_TRADE_API_KEY"],
        "kraken_trade_api_secret": os.environ["KRAKEN_TRADE_API_SECRET"],
        # SMTP alert routing (optional — alert silently fails if absent)
        "smtp_host":    os.environ.get("SMTP_HOST",          ""),
        "smtp_port":    int(os.environ.get("SMTP_PORT",      "587")),
        "smtp_user":    os.environ.get("SMTP_USER",          ""),
        "smtp_pass":    os.environ.get("SMTP_PASS",          ""),
        "alert_from":   os.environ.get("ALERT_EMAIL_FROM",  "alerts@tothbot.com"),
        "alert_to":     os.environ.get("ALERT_EMAIL_TO",    "alert@tothbot.com"),
        # Log file path — used by CIATS for tail (1011010 dv1_6)
        "log_file_path": os.environ.get("TOTHBOT_LOG_FILE", LOG_FILE),
    }


# =============================================================
# MONITORED UNIVERSE
# =============================================================

def _load_universe(config: dict) -> list[str]:
    """
    Load the monitored trading universe from environment.
    BTC/USD ALWAYS included (RE-TAG-002).
    Env var TOTHBOT_UNIVERSE: comma-separated symbol list.
    If not set, returns BTC/USD only for a safe initial state.
    """
    raw = os.environ.get("TOTHBOT_UNIVERSE", "BTC/USD")
    pairs = [p.strip() for p in raw.split(",") if p.strip()]
    # RE-TAG-002: BTC/USD ALWAYS in universe
    if "BTC/USD" not in pairs:
        pairs.insert(0, "BTC/USD")
    return pairs


# =============================================================
# COMPONENT FACTORY
# =============================================================

def _build_components(
    config: dict,
    param_store: dict,
    universe: list[str],
    logger: logging.Logger,
) -> dict[str, Any]:
    """
    Instantiate all TothBot V2 components and wire dependencies.

    Dependency order (must match WSManager constructor docstring):
      1. PositionMirror    — no external deps
      2. RegimeEngine      — no TothBot deps
      3. WSManager         — must exist before RiskEngine (wm ref)
      4. RiskEngine        — needs wm, pm, logger
      5. ExecutionEngine   — needs wm, re, pm, logger
      6. ExitController    — needs wm, re, pm, logger
      7. SignalPipeline    — needs wm, re, pm, rge
      8. LongModule        — needs wm, re, pm, ee
      9. SelectionController — needs sp (signal_pipeline ref)
      10. CIATS            — needs param_store, log_file, logger

    WSManager receives callbacks for pipeline, exec engine,
    exit controller, and regime engine (injected callables).
    """
    # ── 1. PositionMirror ─────────────────────────────────────
    pm = PositionMirror(logger=logger)

    # ── 2. RegimeEngine ───────────────────────────────────────
    rge = RegimeEngine(
        logger=logger,
        data_api_key=config["kraken_data_api_key"],
        trading_universe=universe,
        param_store=param_store,
    )

    # ── 3. WSManager (stub — pipeline callbacks injected below) ─
    #    WSManager is the hub: all other components hold a ref to
    #    it for WS socket access. Build it first, then inject the
    #    downstream callbacks after all components are created.
    wm = WSManager(
        logger=logger,
        config=config,
        # Pipeline callbacks injected after all components are built
        signal_pipeline_fn=None,
        exec_engine_fn=None,
        exit_ctrl_fn=None,
        regime_engine_fn=None,
        ciats_param_store=param_store,
    )

    # ── 4. RiskEngine ─────────────────────────────────────────
    re = RiskEngine(
        logger=logger,
        position_mirror=pm,
        ws_manager=wm,
        param_store=param_store,
    )

    # ── 5. ExecutionEngine ────────────────────────────────────
    ee = ExecutionEngine(
        ws_manager=wm,
        risk_engine=re,
        position_mirror=pm,
        logger=logger,
    )

    # ── 6. ExitController ─────────────────────────────────────
    ec = ExitController(
        risk_engine=re,
        regime_engine=rge,
        logger=logger,
    )

    # ── 7. SignalPipeline ─────────────────────────────────────
    sp = SignalPipeline(
        wm=wm,
        re=re,
        pm=pm,
        rge=rge,
    )

    # ── 8. LongModule ─────────────────────────────────────────
    lm = LongModule(
        ws_manager=wm,
        execution_engine=ee,
        risk_engine=re,
        position_mirror=pm,
        logger=logger,
    )

    # ── 9. SelectionController ────────────────────────────────
    sc = SelectionController(
        signal_pipeline=sp,
        logger=logger,
    )

    # ── 10. CIATS ─────────────────────────────────────────────
    ciats = CIATS(
        param_store=param_store,
        log_file_path=config["log_file_path"],
        logger=logger,
    )

    # ── Wire pipeline callbacks into WSManager ─────────────────
    #
    # WSManager._process_ohlc_5m calls:
    #   signal_pipeline_fn(OHLCCandle, pre_comp_cache, params_snapshot)
    # Convert OHLCCandle → dict, run sp.on_candle, pass gate-8 output
    # to lm.on_gate8_output if the pipeline passes.
    #
    async def _pipeline_wrapper(
        candle: OHLCCandle, pre_comp: dict, params: dict
    ) -> None:
        candle_dict = {
            "open":           candle.open,
            "high":           candle.high,
            "low":            candle.low,
            "close":          candle.close,
            "volume":         candle.volume,
            "vwap":           candle.vwap,
            "interval_begin": candle.interval_begin,
        }
        result = await sp.on_candle(candle.symbol, candle_dict, params)
        if result is not None:
            # Enrich market_regime from BTC/USD proxy if pipeline left it blank.
            if not result.get("market_regime"):
                btc_state = rge.get_regime("BTC/USD")
                result["market_regime"] = (
                    btc_state.directional if btc_state else ""
                )
            await lm.on_gate8_output(result)

    # WSManager._handle_filled calls:
    #   exec_engine_fn(event_dict, wm_instance)  [2 positional args]
    # lm.on_execution_event takes (event_dict) only — wrap to drop wm arg.
    #
    async def _exec_wrapper(event: dict, wm_inst) -> None:  # noqa: wm_inst unused
        await lm.on_execution_event(event)

    # WSManager._trigger_daily_regime_refresh calls:
    #   regime_engine_fn(daily_candles, "BTC/USD")
    # rge.run_daily_computation() fetches its own OHLC — ignore passed args.
    #
    async def _regime_wrapper(candles, symbol: str) -> None:  # noqa: args unused
        await rge.run_daily_computation()

    # ExitController is callable via __call__(symbol, event, wm).
    # WSManager calls exit_ctrl_fn(symbol, event, wm) — wire ec directly.
    #
    wm._signal_pipeline_fn = _pipeline_wrapper
    wm._exec_engine_fn     = _exec_wrapper
    wm._exit_ctrl_fn       = ec                   # ExitController.__call__
    wm._regime_engine_fn   = _regime_wrapper

    return {
        "wm":    wm,
        "pm":    pm,
        "rge":   rge,
        "re":    re,
        "ee":    ee,
        "ec":    ec,
        "sp":    sp,
        "lm":    lm,
        "sc":    sc,
        "ciats": ciats,
    }


# =============================================================
# ASYNC MAIN — STARTUP ORCHESTRATOR
# =============================================================

async def _async_main() -> None:
    """
    Async entry point. Executes the startup sequence and
    drives continuous operation.

    Startup sequence per 1011014 dv1_3:
      Step 1   — THIS FUNCTION (Kraken Status API check)
      Steps 2–10 — WSManager.run()

    CIATS runs as an asyncio.Task in the same event loop.
    Fatal exceptions propagate up to run() which exits(1).
    """
    # ── Load config and build Parameter Store ──────────────────
    config      = _load_config()
    param_store = _build_param_store()
    universe    = _load_universe(config)

    # ── Initialise Logger (must be first — all others depend on it) ─
    log_queue, log_listener, logger = initialize_logger()
    logger.info(log_record({
        "event":     "TOTHBOT_STARTING",
        "level":     "INFO",
        "component": "STARTUP",
        "universe":  universe,
        "pairs":     len(universe),
    }))

    # ── STEP 1: Kraken Status API Check (SS-STARTUP-001/002/003) ─
    # Non-blocking. Startup always continues regardless of outcome.
    await check_kraken_status(logger)

    # ── Build and wire all components ─────────────────────────
    components = _build_components(config, param_store, universe, logger)
    wm:    WSManager   = components["wm"]
    ciats: CIATS       = components["ciats"]

    logger.info(log_record({
        "event":     "COMPONENTS_WIRED",
        "level":     "INFO",
        "component": "STARTUP",
        "note":      "All components instantiated and wired",
    }))

    # ── Launch CIATS as background asyncio.Task ────────────────
    # CIATS-ISO-001: runs in same event loop, never blocks hot path.
    # Starts polling immediately; Stream 1 valid at 50 candle evals,
    # Stream 2 active at 200 closed trades.
    ciats_task = asyncio.create_task(
        ciats.run(),
        name="ciats",
    )

    # ── Run WSManager — Steps 2–10 and continuous operation ────
    # WSManager.run() blocks until fatal failure or shutdown.
    # systemd WatchdogSec=120 restarts TothBot on fatal exit.
    # Open positions protected by resting emergSL orders (AR-046).
    try:
        await wm.run()
    except Exception as exc:
        # WSManager has already logged CRITICAL and alerted.
        # Re-raise so run() exits with sys.exit(1) → systemd restart.
        logger.critical(log_record({
            "event":     "WSMGR_FATAL_PROPAGATED",
            "level":     "CRITICAL",
            "component": "STARTUP",
            "error":     str(exc),
            "note":      "systemd will restart TothBot",
        }))
        raise
    finally:
        # Cancel CIATS task on clean exit or fatal error.
        # BP-ERR-001: cancel before awaiting.
        if not ciats_task.done():
            ciats_task.cancel()
        try:
            await ciats_task
        except asyncio.CancelledError:
            pass
        except Exception as ciats_exc:
            logger.error(log_record({
                "event":     "CIATS_TASK_ERROR",
                "level":     "ERROR",
                "component": "STARTUP",
                "error":     str(ciats_exc),
            }))


# =============================================================
# SYNCHRONOUS ENTRY POINT
# =============================================================

def run() -> None:
    """
    Process entry point. Sets up uvloop and runs the async main.

    uvloop.new_event_loop() — SS-PRE-005.
    asyncio.Runner wraps the event loop cleanly (Python 3.11+).

    Called by:
      - systemd via tothbot.service (ExecStart)
      - Direct: python -m tothbot.startup_sequence
      - __main__ block below
    """
    try:
        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
            runner.run(_async_main())
    except KeyboardInterrupt:
        # Operator Ctrl-C during development — clean exit, no traceback.
        print("\n[STARTUP] KeyboardInterrupt — TothBot stopped.")
        sys.exit(0)
    except Exception as exc:
        # Fatal error propagated from _async_main.
        # systemd restarts TothBot. Log to stderr (logger may be down).
        print(f"[STARTUP FATAL] {exc}", file=sys.stderr)
        sys.exit(1)


# =============================================================
# MODULE ENTRY
# =============================================================

if __name__ == "__main__":
    run()