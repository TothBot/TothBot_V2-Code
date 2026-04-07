"""
TothBot V2 — Startup Sequence
=============================================================
Coding spec:  1011014 Startup_Sequence_Coding_Spec dv1_3
BP standard:  1011001 Engineering_Best_Practices dv1_6
Parent spec:  0500000 System_Architecture_Overview dv1_18
=============================================================

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
  7. Run WSManager.run() — drives startup Steps 2-10
     and continuous operation thereafter.

Steps 2-10 (WS connect, subscribe, reconcile, warm-up,
regime seed, READY detection, pipeline, watchdog) are
owned and executed by WSManager per 1011014 dv1_3 Sections
6-9. startup_sequence.py does NOT duplicate WS logic.

Startup Sequence (1011014 dv1_3 Section 6):
  Step 1   Kraken Status API check         -- THIS MODULE
  Step 2   Acquire WS token                -- WSManager
  Step 3   Connect public WS + subscribe   -- WSManager
  Step 4   Process instrument snapshot     -- WSManager
  Step 5   Connect private WS + subscribe  -- WSManager
  Step 6   Reconcile open positions        -- WSManager
  Step 7   Restore ticker for positions    -- WSManager
  Step 8   REST warm-up GetOHLCData        -- WSManager
  Step 9   At least one READY pair         -- WSManager
  Step 10  Continuous operation            -- WSManager

sd_notify READY=1: sent by WSManager.run() after Step 9
(SS-STARTUP-026). This module does NOT send sd_notify.

Reconnect sequence (1011014 dv1_3 Section 9):
  Steps R1-R11 owned entirely by WSManager.
  portfolio_baseline_USD NEVER reset on reconnect.
  system_state PRESERVED across reconnect.

Hard Rules (this module):
  SS-PRE-001  Python 3.12.3, Ubuntu 24.04.4 LTS.
  SS-PRE-002  API keys DATA + TRADE from os.environ only.
  SS-PRE-005  uvloop.new_event_loop() for asyncio event loop.
  HR-LG-004   log_listener.stop() called on shutdown (Logger drain).
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

Revision history:
  v1_0  Phase 9 Module 13. Initial commit.
  v1_1  BUG FIX: setup_logger() does not exist in tothbot.logger.
        Corrected to initialize_logger() per 1011007 dv1_2.
        initialize_logger() returns (log_queue, log_listener, logger).
        log_listener.stop() now called in finally block (HR-LG-004).
        monitor_log_queue task now started per 1011007 Section 10.
        monitor_task cancelled in finally block alongside ciats_task.
"""
from __future__ import annotations

import asyncio
import logging
import logging.handlers
import os
import sys
from decimal import Decimal
from typing import Any

import uvloop

from tothbot.vps_deployment import check_kraken_status
from tothbot.logger import (
    LOG_FILE,
    initialize_logger,
    log_record,
    monitor_log_queue,
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
from tothbot.ws_manager import WSManager


# =============================================================
# REQUIRED ENVIRONMENT VARIABLES  (VD-KEY-003)
# =============================================================

_REQUIRED_ENV_VARS: tuple[str, ...] = (
    "KRAKEN_DATA_API_KEY",
    "KRAKEN_DATA_API_SECRET",
    "KRAKEN_TRADE_API_KEY",
    "KRAKEN_TRADE_API_SECRET",
    # SMTP vars are optional -- alert silently fails if absent
)


# =============================================================
# CIATS PARAMETER STORE -- STARTING VALUES  (TB00000 S9.17)
# =============================================================
# All values are CIATS-owned starting values.
# CIATS overwrites them at inter-trade boundaries.
# Net 1:1.5 R:R is hardcoded in RiskEngine and ExecutionEngine.
# It is NOT a parameter. It does NOT appear here.

def _build_param_store() -> dict:
    """
    Build the shared CIATS Parameter Store with starting values.
    CIATS writes to this dict; all other components read
    a frozen snapshot at pipeline start (AR-I-4).

    Starting values per TB00000 S9.17 and 1011010 dv1_6.
    """
    return {
        # Position sizing (S9.3, S9.4)
        "tradeable_pct":           Decimal("0.50"),
        "per_trade_pct":           Decimal("0.05"),
        # Max concurrent positions (S9.5)
        "max_concurrent":          20,
        # Exit Controller parameters (1011005 dv1_2)
        "mae_mult":                Decimal("1.5"),    # ATR(14) x 1.5x Layer 2
        "emergency_sl_mult":       Decimal("3.0"),    # ATR(14) x 3.0x Layer 3
        "cancel_timeout":          5.0,               # seconds -- cancel ACK wait
        "mpp_retry_count":         3,                 # MPP IOC retry attempts
        "max_hold_candles":        24,                # max hold in 5m candles
        # Entry timing
        "entry_timeout_sec":       45,                # GTD window seconds
        # Drawdown circuit breakers (S9.17)
        "full_halt_drawdown":      Decimal("0.10"),   # 10% -> full halt
        "session_pause_drawdown":  Decimal("0.05"),   # 5% -> session pause
        # Signal / regime parameters (S9.17)
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
    API keys read from os.environ ONLY -- NEVER hardcoded (VD-KEY-003).
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
        # Kraken DATA key pair -- market data, public REST (AR-004)
        "kraken_data_api_key":     os.environ["KRAKEN_DATA_API_KEY"],
        "kraken_data_api_secret":  os.environ["KRAKEN_DATA_API_SECRET"],
        # Kraken TRADE key pair -- private WS, order dispatch (AR-004)
        "kraken_trade_api_key":    os.environ["KRAKEN_TRADE_API_KEY"],
        "kraken_trade_api_secret": os.environ["KRAKEN_TRADE_API_SECRET"],
        # SMTP alert routing (optional -- alert silently fails if absent)
        "smtp_host":   os.environ.get("SMTP_HOST",         ""),
        "smtp_port":   int(os.environ.get("SMTP_PORT",     "587")),
        "smtp_user":   os.environ.get("SMTP_USER",         ""),
        "smtp_pass":   os.environ.get("SMTP_PASS",         ""),
        "alert_from":  os.environ.get("ALERT_EMAIL_FROM",  "alerts@tothbot.com"),
        "alert_to":    os.environ.get("ALERT_EMAIL_TO",    "alert@tothbot.com"),
        # Log file path -- used by CIATS for tail (1011010 dv1_6)
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
      1. PositionMirror      -- no external deps
      2. RegimeEngine        -- no TothBot deps
      3. WSManager           -- hub; wired after other components
      4. RiskEngine          -- needs wm, pm, logger
      5. ExecutionEngine     -- needs wm, re, pm, logger
      6. ExitController      -- needs wm, re, pm, logger
      7. SignalPipeline      -- needs wm, re, pm, rge
      8. LongModule          -- needs wm, re, pm, ee
      9. SelectionController -- needs sp
      10. CIATS              -- needs param_store, log_file, logger

    WSManager constructed first with None callbacks.
    Callbacks injected after all downstream components are built.
    This avoids circular constructor dependency.
    """
    # -- 1. PositionMirror -------------------------------------------
    pm = PositionMirror(logger=logger)

    # -- 2. RegimeEngine ---------------------------------------------
    rge = RegimeEngine(
        logger=logger,
        data_api_key=config["kraken_data_api_key"],
        trading_universe=universe,
        param_store=param_store,
    )

    # -- 3. WSManager (callbacks None until all components built) ----
    wm = WSManager(
        logger=logger,
        config=config,
        signal_pipeline_fn=None,
        exec_engine_fn=None,
        exit_ctrl_fn=None,
        regime_engine_fn=None,
        ciats_param_store=param_store,
    )

    # -- 4. RiskEngine -----------------------------------------------
    re = RiskEngine(
        logger=logger,
        position_mirror=pm,
        ws_manager=wm,
        param_store=param_store,
    )

    # -- 5. ExecutionEngine ------------------------------------------
    ee = ExecutionEngine(
        ws_manager=wm,
        risk_engine=re,
        position_mirror=pm,
        logger=logger,
    )

    # -- 6. ExitController -------------------------------------------
    ec = ExitController(
        ws_manager=wm,
        risk_engine=re,
        position_mirror=pm,
        logger=logger,
    )

    # -- 7. SignalPipeline -------------------------------------------
    sp = SignalPipeline(
        wm=wm,
        re=re,
        pm=pm,
        rge=rge,
    )

    # -- 8. LongModule -----------------------------------------------
    lm = LongModule(
        ws_manager=wm,
        risk_engine=re,
        position_mirror=pm,
        exec_engine=ee,
        logger=logger,
    )

    # -- 9. SelectionController -------------------------------------
    sc = SelectionController(
        signal_pipeline=sp,
        logger=logger,
    )

    # -- 10. CIATS --------------------------------------------------
    ciats = CIATS(
        param_store=param_store,
        log_file_path=config["log_file_path"],
        logger=logger,
    )

    # -- Wire pipeline callbacks into WSManager ---------------------
    wm._signal_pipeline_fn = sp.on_candle
    wm._exec_engine_fn     = lm.on_execution_event
    wm._exit_ctrl_fn       = ec.on_ticker_bbo
    wm._regime_engine_fn   = rge.refresh_all

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
# ASYNC MAIN -- STARTUP ORCHESTRATOR
# =============================================================

async def _async_main() -> None:
    """
    Async entry point. Executes the startup sequence and
    drives continuous operation.

    Logger shutdown (HR-LG-004):
      log_listener.stop() called in finally block regardless
      of exit path. Drains queue and joins background thread.
      This MUST be the last shutdown action.
    """
    # -- Load config ------------------------------------------------
    config      = _load_config()
    param_store = _build_param_store()
    universe    = _load_universe(config)

    # -- Initialise Logger (MUST be first) -------------------------
    # initialize_logger() returns (log_queue, log_listener, logger).
    # log_listener runs a background thread (NOT asyncio).
    # log_listener.stop() MUST be called on shutdown (HR-LG-004).
    log_queue, log_listener, logger = initialize_logger()

    # Start queue health monitor (LG-QUEUE-006)
    monitor_task: asyncio.Task = asyncio.create_task(
        monitor_log_queue(log_queue),
        name="log_queue_monitor",
    )

    logger.info(log_record({
        "event":     "TOTHBOT_STARTING",
        "level":     "INFO",
        "component": "STARTUP",
        "universe":  universe,
        "pairs":     len(universe),
    }))

    ciats_task: asyncio.Task | None = None

    try:
        # -- STEP 1: Kraken Status API Check -----------------------
        # SS-STARTUP-001/002/003. Non-blocking always.
        await check_kraken_status(logger)

        # -- Build and wire all components -------------------------
        components = _build_components(config, param_store, universe, logger)
        wm:    WSManager = components["wm"]
        ciats: CIATS     = components["ciats"]

        logger.info(log_record({
            "event":     "COMPONENTS_WIRED",
            "level":     "INFO",
            "component": "STARTUP",
            "note":      "All components instantiated and wired",
        }))

        # -- Launch CIATS as background asyncio.Task ---------------
        # CIATS-ISO-001: same event loop, never blocks hot path.
        ciats_task = asyncio.create_task(
            ciats.run(),
            name="ciats",
        )

        # -- Run WSManager -- Steps 2-10 + continuous operation ----
        # Blocks until fatal failure or shutdown.
        # systemd WatchdogSec=120 restarts TothBot on fatal exit.
        await wm.run()

    except Exception as exc:
        logger.critical(log_record({
            "event":     "WSMGR_FATAL_PROPAGATED",
            "level":     "CRITICAL",
            "component": "STARTUP",
            "error":     str(exc),
            "note":      "systemd will restart TothBot",
        }))
        raise

    finally:
        # Cancel background tasks before draining Logger
        for task in [ciats_task, monitor_task]:
            if task is not None and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as task_exc:
                    print(
                        f"[STARTUP] Task shutdown error: {task_exc}",
                        file=sys.stderr,
                    )

        # HR-LG-004: drain queue and join background thread.
        # Must be last -- ensures all logs are flushed to disk.
        log_listener.stop()


# =============================================================
# SYNCHRONOUS ENTRY POINT
# =============================================================

def run() -> None:
    """
    Process entry point. Sets up uvloop and runs the async main.
    uvloop.new_event_loop() per SS-PRE-005.
    asyncio.Runner requires Python 3.11+ (satisfied: 3.12.3).
    """
    try:
        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
            runner.run(_async_main())
    except KeyboardInterrupt:
        print("\n[STARTUP] KeyboardInterrupt -- TothBot stopped.")
        sys.exit(0)
    except Exception as exc:
        print(f"[STARTUP FATAL] {exc}", file=sys.stderr)
        sys.exit(1)


# =============================================================
# MODULE ENTRY
# =============================================================

if __name__ == "__main__":
    run()
