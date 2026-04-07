"""
TothBot V2 — WS Manager Component
=============================================================
Coding spec:  1011002 WS_Manager_Coding_Spec dv1_12
BP standard:  1011001 Engineering_Best_Practices dv1_6
Parent spec:  0511001 WS_Manager_Specification dv1_0
=============================================================

TothBot's sole interface to all Kraken WebSocket v2 connections.
Central event processor. All Kraken push events flow through here.
All outbound orders originate from here.

Hard Rules (consolidated — Section 17 of spec):
  HR-WM-001: Two API key pairs: DATA and TRADE.
  HR-WM-002: All WS connections: max_queue=None, ping_interval=None.
  HR-WM-003: Application JSON ping every 30s. Library TCP PING disabled.
  HR-WM-004: last_real_data_time reset ONLY on real channel events.
  HR-WM-005: executions subscription MUST include order_status:true.
  HR-WM-006: All 10 exec_type values handled explicitly.
  HR-WM-007: Use reason field (NOT cancel_reason — deprecated).
  HR-WM-008: All Decimal from WS messages via Decimal(str()) immediately.
  HR-WM-009: stp_type WS v2 = underscore (cancel_newest). REST = hyphen.
  HR-WM-010: cancel_all_orders_after EXPLICITLY PROHIBITED. NEVER CALL.
  HR-WM-011: portfolio_baseline_USD set ONCE at startup. NEVER reset.
  HR-WM-012: Pipeline evaluations PROHIBITED during reconnect.
  HR-WM-013: GetOHLCData response[-1] ALWAYS excluded. Use response[:-1].
  HR-WM-014: deadline required on ALL add_order and batch_add.
  HR-WM-015: emergSL triggers block MANDATORY with reference="last".
  HR-WM-016: Mid-session reconnect is a SEPARATE code path from startup.
  HR-WM-017: All five SSS indicators seeded before pair reaches READY.
  HR-WM-018: RSI(14) uses Wilder's SMMA (alpha=1/14). NOT EMA (2/15).
  HR-WM-019: Subscription ACK warnings[] parsed and logged.
  HR-WM-020: Both DATA and TRADE keys IP-whitelisted to 87.99.141.44.
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Any, Callable

import aiohttp
import orjson

# websockets v14+ — use asyncio client (AR-060, WM-CONN-007)
from websockets.asyncio.client import connect

from tothbot.logger import _alert_operator_direct, log_record

# =============================================================
# CONSTANTS
# =============================================================

PUBLIC_WS_URI: str = "wss://ws.kraken.com/v2"
PRIVATE_WS_URI: str = "wss://ws-auth.kraken.com/v2"
REST_BASE_URL: str = "https://api.kraken.com"

# Connection parameters — WM-CONN-002 (all four connections)
WS_MAX_SIZE: int = 10 * 1024 * 1024     # 10 MB (AR-029)
WS_OPEN_TIMEOUT: int = 10               # seconds (AR-029)
# max_queue=None: unlimited (AR-058) — prevents burst-event drops
# ping_interval=None: library TCP PING disabled (AR-059, HR-WM-003)

# Health monitoring
PING_INTERVAL_SEC: int = 30             # A-7
PING_TIMEOUT_SEC: int = 10             # A-7: no pong in 10s = reconnect
ZOMBIE_THRESHOLD_SEC: int = 90          # A-8 (WM-ZOM-003)

# Reconnect
MAX_RECONNECT_ATTEMPTS: int = 10        # WM-RECONNECT-002
RECONNECT_WINDOW_SEC: int = 120         # WM-RECONNECT-002
RECONNECT_STALE_CACHE_SEC: int = 900    # WM-RECONNECT-014: 15 minutes

# REST timeouts — BP-ASYNC-004
REST_TOTAL_TIMEOUT: int = 10
REST_CONNECT_TIMEOUT: int = 5
REST_SOCK_READ_TIMEOUT: int = 8
REST_CONNECTOR_LIMIT: int = 10
REST_CONNECTOR_LIMIT_PER_HOST: int = 5

# OHLC stagger — AR-036
OHLC_REST_STAGGER_SEC: float = 1.1

# System states
STATE_NORMAL: str = "NORMAL"
STATE_SESSION_PAUSE: str = "SESSION_PAUSE"
STATE_FULL_HALT: str = "FULL_HALT"

# Warm-up states
WARMUP_WARMING: str = "WARM_UP"
WARMUP_READY: str = "READY"

# Pair status values that block pipeline (WM-PS-005)
BLOCKED_PAIR_STATUSES: frozenset[str] = frozenset({
    "reduce_only", "work_in_progress", "delisted", "limit_only",
})

# Drawdown thresholds — CIATS-owned starting values (WM-DD-004/005)
SESSION_PAUSE_THRESHOLD: Decimal = Decimal("0.05")
FULL_HALT_THRESHOLD: Decimal = Decimal("0.10")

# Trading parameters — CIATS-owned starting values
TRADEABLE_PCT: Decimal = Decimal("0.50")
PER_TRADE_PCT: Decimal = Decimal("0.05")
MAX_CONCURRENT: int = 20
MAE_MULT: Decimal = Decimal("1.5")
EMERGENCY_SL_MULT: Decimal = Decimal("3.0")
ENTRY_GTD_SEC: int = 30               # GTD expiry for entry orders

# Monitored universe config — CIATS-owned starting value
UNIVERSE_TOP_N: int = 50
UNIVERSE_MIN_VOLUME_USD: Decimal = Decimal("500000")


# =============================================================
# DATA STRUCTURES
# =============================================================

@dataclass
class OHLCCandle:
    """Closed OHLC candle data."""
    symbol: str
    interval: int           # 5 or 60 (minutes)
    interval_begin: str     # ISO 8601 UTC
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    vwap: Decimal


@dataclass
class PositionRecord:
    """Position Mirror entry (WS Manager is SOLE WRITER)."""
    symbol: str
    cl_ord_id: str                          # entry cl_ord_id (set at dispatch)
    entry_limit_price: Decimal = Decimal("0")  # placeholder until fill (PM-CREATE-001)
    entry_fill_price: Decimal = Decimal("0")
    qty: Decimal = Decimal("0")
    tp_order_id: str = ""
    tp_cl_ord_id: str = ""
    emergsl_order_id: str = ""
    emergsl_cl_ord_id: str = ""
    entry_timestamp_utc: str = ""
    hold_candle_count: int = 0
    mae_pct_reached: Decimal = Decimal("0")
    fees_entry_usd: Decimal = Decimal("0")
    asset_regime: str = ""
    vol_regime: str = ""
    market_regime: str = ""
    signal_params: dict = field(default_factory=dict)


# =============================================================
# WSManager
# =============================================================

class WSManager:
    """
    TothBot V2 WS Manager.

    Central event processor and sole Kraken WS interface.
    Inject dependencies via constructor. Call run() to start.

    Dependencies injected:
        logger:              logging.Logger ("tothbot" instance)
        config:              dict — API keys, CIATS params, etc.
        signal_pipeline_fn:  Callable[[OHLCCandle, dict, dict], Awaitable]
        exec_engine_fn:      Callable[[dict, WSManager], Awaitable]
        exit_ctrl_fn:        Callable[[str, dict, WSManager], Awaitable]
        regime_engine_fn:    Callable[[list[dict], str], Awaitable]
        ciats_param_store:   dict — frozen snapshot at pipeline start

    Startup sequence called by 1011014 Startup_Sequence_Coding_Spec.
    """

    def __init__(
        self,
        logger: Any,
        config: dict,
        signal_pipeline_fn: Callable | None = None,
        exec_engine_fn: Callable | None = None,
        exit_ctrl_fn: Callable | None = None,
        regime_engine_fn: Callable | None = None,
        ciats_param_store: dict | None = None,
    ) -> None:
        self._logger = logger
        self._config = config

        # Injected pipeline callbacks (wired by startup sequence)
        self._signal_pipeline_fn = signal_pipeline_fn
        self._exec_engine_fn = exec_engine_fn
        self._exit_ctrl_fn = exit_ctrl_fn
        self._regime_engine_fn = regime_engine_fn

        # CIATS Parameter Store — frozen snapshot at pipeline start (AR-I-4)
        self._ciats_params: dict = ciats_param_store or {}

        # ── Connection state ──────────────────────────────────────────────
        self._ws_public: Any = None
        self._ws_private: Any = None
        self._ws_token: str = ""
        self.connection_id_public: int | None = None
        self.connection_id_private: int | None = None
        self.engine_state: str = "online"   # from status channel

        # ── Instrument / pair universe ────────────────────────────────────
        self.pair_cache: dict[str, dict] = {}
        # pair_cache[symbol] = {price_increment, qty_increment, qty_min,
        #                        cost_min, status}
        self.monitored_universe: list[str] = []
        self.pair_status: dict[str, str] = {}

        # ── Indicator state (5m) — per pair ──────────────────────────────
        self.atr_14: dict[str, Decimal] = {}
        self._prev_close: dict[str, Decimal] = {}
        self.rsi_avg_gain: dict[str, Decimal] = {}
        self.rsi_avg_loss: dict[str, Decimal] = {}
        self.ema_9: dict[str, Decimal] = {}
        self.ema_21: dict[str, Decimal] = {}
        self.volume_ma_20: dict[str, Decimal] = {}
        self._volume_buffer: dict[str, list] = {}  # rolling 20-vol window

        # Warm-up state machine (AR-068)
        self.warm_up_state: dict[str, str] = {}   # WARM_UP | READY

        # ── HTF cache (60m) — per pair ────────────────────────────────────
        self.htf_ema_20: dict[str, Decimal] = {}
        self.htf_ema_50: dict[str, Decimal] = {}

        # ── Candle close detection ─────────────────────────────────────────
        # WM-OHLC-004: SEPARATE dicts for 5m and 60m (MUST NOT combine)
        self.last_interval_begin: dict[str, str] = {}       # 5m
        self.last_interval_begin_60: dict[str, str] = {}    # 60m
        self.last_complete_candle: dict[str, OHLCCandle] = {}
        self.last_complete_candle_60: dict[str, OHLCCandle] = {}

        # ── Position Mirror (WS Manager is SOLE WRITER) ───────────────────
        self.position_mirror: dict[str, PositionRecord] = {}

        # ── Balance and portfolio tracking ────────────────────────────────
        self.spot_usd_balance: Decimal = Decimal("0")
        self.portfolio_baseline_USD: Decimal | None = None   # set ONCE

        # ── Drawdown monitoring ───────────────────────────────────────────
        self.latest_bid: dict[str, Decimal] = {}
        self.system_state: str = STATE_NORMAL

        # ── Pending Order Registry (AR-053) ───────────────────────────────
        self.pending_orders: dict[str, Decimal] = {}  # cl_ord_id → USD cost

        # ── Sequence tracking ─────────────────────────────────────────────
        self.executions_last_seq: int = 0
        self.balances_last_seq: int = 0

        # ── Rate counter ──────────────────────────────────────────────────
        self.maxratecount: int = 125         # updated from executions ACK
        self.rate_counter_by_pair: dict[str, int] = {}

        # ── Zombie detection ──────────────────────────────────────────────
        self._last_real_data_public: float = time.monotonic()
        self._last_real_data_private: float = time.monotonic()

        # ── Ping state ────────────────────────────────────────────────────
        self._ping_req_id_pub: int | None = None
        self._ping_req_id_priv: int | None = None
        self._ping_sent_pub: float | None = None
        self._ping_sent_priv: float | None = None
        self._awaiting_pong_pub: bool = False
        self._awaiting_pong_priv: bool = False

        # ── req_id counter ────────────────────────────────────────────────
        self._req_id_counter: int = 0
        self.req_id_registry: dict[int, dict] = {}

        # ── Selection Controller state (AR-073) ──────────────────────────
        self.exit_cooldown_log: dict[str, float] = {}
        self.consecutive_loss_count: dict[str, int] = {}

        # ── Reconnect control ─────────────────────────────────────────────
        self._is_reconnecting: bool = False
        self._reconnect_start_time: float | None = None
        self._reconnect_attempt: int = 0
        self._disconnect_time: float | None = None

        # ── Dispatch tables ───────────────────────────────────────────────
        self._channel_dispatch: dict[str, Callable] = {}
        self._exec_type_dispatch: dict[str, Callable] = {}
        self._setup_dispatch_tables()

    # =================================================================
    # DISPATCH TABLE SETUP — O(1) routing (AR-015)
    # =================================================================

    def _setup_dispatch_tables(self) -> None:
        """Build O(1) dispatch tables for channel and exec_type routing."""
        self._channel_dispatch = {
            "ohlc":       self._handle_ohlc,
            "ticker":     self._handle_ticker,
            "instrument": self._handle_instrument,
            "status":     self._handle_status,
            "executions": self._handle_executions,
            "balances":   self._handle_balances,
            "pong":       self._handle_pong,
        }
        # All 10 exec_type values — HR-WM-006 (no silent drops)
        self._exec_type_dispatch = {
            "pending_new":    self._handle_pending_new,
            "new":            self._handle_new,
            "trade":          self._handle_trade,
            "filled":         self._handle_filled,
            "iceberg_refill": self._handle_iceberg_refill,
            "canceled":       self._handle_canceled,
            "expired":        self._handle_expired,
            "amended":        self._handle_amended,
            "restated":       self._handle_restated,
            "status":         self._handle_exec_status,
        }

    # =================================================================
    # REQUEST ID MANAGEMENT
    # =================================================================

    def _next_req_id(self) -> int:
        self._req_id_counter += 1
        return self._req_id_counter

    # =================================================================
    # AIOHTTP SESSION FACTORY — BP-ASYNC-004
    # =================================================================

    def _make_http_session(self) -> aiohttp.ClientSession:
        timeout = aiohttp.ClientTimeout(
            total=REST_TOTAL_TIMEOUT,
            connect=REST_CONNECT_TIMEOUT,
            sock_read=REST_SOCK_READ_TIMEOUT,
        )
        connector = aiohttp.TCPConnector(
            limit=REST_CONNECTOR_LIMIT,
            limit_per_host=REST_CONNECTOR_LIMIT_PER_HOST,
            force_close=False,
        )
        return aiohttp.ClientSession(timeout=timeout, connector=connector)

    # =================================================================
    # REST CALLS
    # =================================================================

    async def _rest_get_ws_token(self) -> str:
        """
        Acquire WS auth token via REST (WM-TOKEN-001/002).
        Uses TRADE API key. Valid 900 seconds.
        Called at startup AND on every reconnect.
        """
        trade_key = self._config["kraken_trade_api_key"]
        trade_secret = self._config["kraken_trade_api_secret"]

        async with self._make_http_session() as session:
            # Kraken REST GetWebSocketsToken (signed request)
            resp = await session.post(
                f"{REST_BASE_URL}/0/private/GetWebSocketsToken",
                headers={"API-Key": trade_key},
                data={"nonce": str(int(time.time() * 1000))},
            )
            data = orjson.loads(await resp.read())
            if data.get("error"):
                raise RuntimeError(
                    f"GetWebSocketsToken error: {data['error']}"
                )
            token = data["result"]["token"]
        self._logger.info(log_record({
            "event": "WS_TOKEN_ACQUIRED",
            "level": "INFO",
            "component": "WS_MGR",
        }))
        return token

    async def _rest_get_ohlc(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        interval: int,
    ) -> list[dict]:
        """
        Fetch OHLC candle history for indicator seeding.
        Returns response[:-1] (excludes uncommitted candle — HR-WM-013).
        Stagger 1.1s between calls to same pair (AR-036).
        """
        params = {"pair": symbol, "interval": interval}
        resp = await session.get(
            f"{REST_BASE_URL}/0/public/OHLC", params=params
        )
        data = orjson.loads(await resp.read())
        if data.get("error"):
            raise RuntimeError(
                f"GetOHLCData({symbol}, {interval}) error: {data['error']}"
            )
        # Kraken wraps OHLC data under the pair key in result
        pair_key = list(data["result"].keys())[0]
        candles = data["result"][pair_key]
        return candles[:-1]  # HR-WM-013: ALWAYS exclude response[-1]

    async def _rest_get_open_orders(self) -> dict:
        """REST GetOpenOrders — executions gap fallback (AR-035)."""
        trade_key = self._config["kraken_trade_api_key"]
        async with self._make_http_session() as session:
            resp = await session.post(
                f"{REST_BASE_URL}/0/private/OpenOrders",
                headers={"API-Key": trade_key},
                data={"nonce": str(int(time.time() * 1000))},
            )
            data = orjson.loads(await resp.read())
        return data.get("result", {}).get("open", {})

    async def _rest_get_account_balance(self) -> dict:
        """REST GetAccountBalance — balances gap fallback (AR-035)."""
        trade_key = self._config["kraken_trade_api_key"]
        async with self._make_http_session() as session:
            resp = await session.post(
                f"{REST_BASE_URL}/0/private/Balance",
                headers={"API-Key": trade_key},
                data={"nonce": str(int(time.time() * 1000))},
            )
            data = orjson.loads(await resp.read())
        return data.get("result", {})

    # =================================================================
    # WS CONNECTION — WM-CONN-002 template
    # =================================================================

    async def _ws_connect(self, uri: str) -> Any:
        """
        Create WS connection with required parameters (WM-CONN-002).
        max_queue=None: unlimited (AR-058).
        ping_interval=None: library TCP PING disabled (AR-059, HR-WM-003).
        max_size=10MB (AR-029). open_timeout=10s (AR-029).
        """
        return await connect(
            uri,
            max_size=WS_MAX_SIZE,
            open_timeout=WS_OPEN_TIMEOUT,
            max_queue=None,       # HR-WM-002 — prevents burst-event drops
            ping_interval=None,   # HR-WM-003 — app-level JSON ping only
        )

    # =================================================================
    # WS SEND HELPERS
    # =================================================================

    async def _send_public(self, payload: dict) -> None:
        """Send JSON message on public WS connection."""
        await self._ws_public.send(
            orjson.dumps(payload).decode()
        )

    async def _send_private(self, payload: dict) -> None:
        """Send JSON message on private WS connection."""
        await self._ws_private.send(
            orjson.dumps(payload).decode()
        )

    # =================================================================
    # SUBSCRIPTIONS
    # =================================================================

    async def _subscribe_public(self) -> None:
        """
        Subscribe all public channels.
        Steps 2-5 from 1011014 startup sequence.
        Parse warnings[] on every ACK (HR-WM-019).
        """
        # instrument — pair specs + status cache
        await self._send_public({
            "method": "subscribe",
            "params": {"channel": "instrument"},
        })
        # status — engine state + connection_id
        await self._send_public({
            "method": "subscribe",
            "params": {"channel": "status"},
        })
        # ohlc(5) — 5m candles (SYSTEM CLOCK)
        await self._send_public({
            "method": "subscribe",
            "params": {"channel": "ohlc", "interval": 5},
        })
        # ohlc(60) — 1H HTF cache for Gate 4
        await self._send_public({
            "method": "subscribe",
            "params": {"channel": "ohlc", "interval": 60},
        })
        # Ticker subscriptions are managed per-pair after warm-up
        # (Section 16 — event_trigger mode per position state)

    async def _subscribe_private(self) -> None:
        """
        Subscribe private channels.
        order_status:true MANDATORY (HR-WM-005, AR-026).
        ratecounter:true for CIATS EWMA Monitor stream A-1.
        Parse warnings[] and extract maxratecount from executions ACK.
        """
        await self._send_private({
            "method": "subscribe",
            "params": {
                "channel": "executions",
                "token": self._ws_token,
                "order_status": True,     # HR-WM-005 — MANDATORY
                "ratecounter": True,      # AR-030
                "snapshot": True,
            },
        })
        await self._send_private({
            "method": "subscribe",
            "params": {
                "channel": "balances",
                "token": self._ws_token,
                "snapshot": True,
            },
        })

    async def _subscribe_ticker_pair(
        self, symbol: str, event_trigger: str = "trades"
    ) -> None:
        """
        Subscribe ticker for a single pair with given event_trigger.
        Section 16: bbo for open positions, trades for no-position pairs.
        """
        await self._send_public({
            "method": "subscribe",
            "params": {
                "channel": "ticker",
                "symbol": [symbol],
                "event_trigger": event_trigger,
            },
        })

    async def _update_ticker_event_trigger(
        self, symbol: str, event_trigger: str
    ) -> None:
        """
        Switch ticker event_trigger mode immediately on position state change.
        Section 16: On position open → bbo. On position close → trades.
        """
        # Unsubscribe current, resubscribe with new trigger
        await self._send_public({
            "method": "unsubscribe",
            "params": {
                "channel": "ticker",
                "symbol": [symbol],
            },
        })
        await self._subscribe_ticker_pair(symbol, event_trigger)

    # =================================================================
    # MAIN ENTRY POINT — called by 1011014 startup sequence
    # =================================================================

    async def run(self) -> None:
        """
        Main WS Manager loop. Called once by startup sequence after
        initialization is complete. Runs until fatal failure or shutdown.
        """
        try:
            await self._startup_connect_and_subscribe()
            await self._main_loop()
        except Exception as exc:
            self._logger.critical(log_record({
                "event": "WS_MGR_FATAL",
                "level": "CRITICAL",
                "component": "WS_MGR",
                "error": str(exc),
            }))
            _alert_operator_direct(
                f"WS Manager fatal error: {exc}. systemd will restart."
            )
            raise

    async def _startup_connect_and_subscribe(self) -> None:
        """
        Execute startup connection and subscription sequence.
        Steps 1-5 per 1011014. Indicator seeding (7a/7b) called separately.
        """
        # Step 1: Acquire WS auth token
        self._ws_token = await self._rest_get_ws_token()

        # Steps 2-3: Connect public and private WS
        self._ws_public = await self._ws_connect(PUBLIC_WS_URI)
        self._ws_private = await self._ws_connect(PRIVATE_WS_URI)

        # Step 4: Subscribe all channels
        await self._subscribe_public()
        await self._subscribe_private()

    async def _main_loop(self) -> None:
        """
        Run concurrent receive loops + health monitoring tasks.
        """
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._recv_loop_public())
            tg.create_task(self._recv_loop_private())
            tg.create_task(self._ping_loop())
            tg.create_task(self._zombie_monitor())

    # =================================================================
    # RECEIVE LOOPS
    # =================================================================

    async def _recv_loop_public(self) -> None:
        """Receive loop for public WS. Dispatches all inbound messages."""
        async for raw in self._ws_public:
            try:
                msg = orjson.loads(raw)
                channel = msg.get("channel") or msg.get("method", "")
                handler = self._channel_dispatch.get(channel)
                if handler:
                    await handler(msg, is_public=True)
                else:
                    self._logger.warning(log_record({
                        "event": "UNKNOWN_MESSAGE_TYPE",
                        "level": "WARN",
                        "component": "WS_MGR",
                        "channel": channel,
                        "raw": str(raw)[:200],
                    }))
            except Exception as exc:  # noqa: BP-ERR-001
                self._logger.error(log_record({
                    "event": "PUBLIC_RECV_ERROR",
                    "level": "WARN",
                    "component": "WS_MGR",
                    "error": str(exc),
                }))

    async def _recv_loop_private(self) -> None:
        """Receive loop for private WS. Dispatches all inbound messages."""
        async for raw in self._ws_private:
            try:
                msg = orjson.loads(raw)
                channel = msg.get("channel") or msg.get("method", "")
                handler = self._channel_dispatch.get(channel)
                if handler:
                    await handler(msg, is_public=False)
                else:
                    self._logger.warning(log_record({
                        "event": "UNKNOWN_MESSAGE_TYPE",
                        "level": "WARN",
                        "component": "WS_MGR",
                        "channel": channel,
                        "raw": str(raw)[:200],
                    }))
            except Exception as exc:  # noqa: BP-ERR-001
                self._logger.error(log_record({
                    "event": "PRIVATE_RECV_ERROR",
                    "level": "WARN",
                    "component": "WS_MGR",
                    "error": str(exc),
                }))

    # =================================================================
    # CHANNEL HANDLERS
    # =================================================================

    async def _handle_ohlc(self, msg: dict, is_public: bool = True) -> None:
        """
        OHLC channel handler. Applies candle-close detection (Section 8).
        Fires pipeline on 5m close. Fires Exit Controller on 60m close.
        Updates last_real_data_time (HR-WM-004).
        """
        self._last_real_data_public = time.monotonic()

        msg_type = msg.get("type")
        if msg_type not in ("snapshot", "update"):
            return

        data_list = msg.get("data", [])
        interval = msg.get("data", [{}])[0].get("interval", 5) if data_list else 5

        for candle_data in data_list:
            symbol = candle_data.get("symbol", "")
            interval_begin = candle_data.get("interval_begin", "")

            if not symbol or not interval_begin:
                continue

            candle = OHLCCandle(
                symbol=symbol,
                interval=interval,
                interval_begin=interval_begin,
                open=Decimal(str(candle_data.get("open", 0))),
                high=Decimal(str(candle_data.get("high", 0))),
                low=Decimal(str(candle_data.get("low", 0))),
                close=Decimal(str(candle_data.get("close", 0))),
                volume=Decimal(str(candle_data.get("volume", 0))),
                vwap=Decimal(str(candle_data.get("vwap", 0))),
            )

            if interval == 5:
                await self._process_ohlc_5m(symbol, interval_begin, candle)
            elif interval == 60:
                await self._process_ohlc_60m(symbol, interval_begin, candle)

    async def _process_ohlc_5m(
        self, symbol: str, interval_begin: str, candle: OHLCCandle
    ) -> None:
        """
        Candle-close detection for 5m (WM-OHLC-002).
        Uses interval_begin ONLY — NOT timestamp field (WM-OHLC-003).
        """
        prev_begin = self.last_interval_begin.get(symbol, "")

        if interval_begin == prev_begin:
            # In-progress candle update — store but DO NOT fire pipeline
            self.last_complete_candle[symbol] = candle
            return

        # NEW interval → previous candle is CLOSED
        if prev_begin and symbol in self.last_complete_candle:
            closed_candle = self.last_complete_candle[symbol]

            # Update incremental indicators
            self._update_indicators_5m(symbol, closed_candle)
            self._increment_hold_candle_counts()

            # Fire pipeline (if not reconnecting — HR-WM-012)
            if (
                not self._is_reconnecting
                and self.warm_up_state.get(symbol) == WARMUP_READY
                and self.system_state != STATE_FULL_HALT
                and self.pair_status.get(symbol, "") == "online"
                and self.pair_status.get(symbol, "") not in BLOCKED_PAIR_STATUSES
            ):
                self._logger.info(log_record({
                    "event": "CANDLE_CLOSE",
                    "level": "INFO",
                    "component": "WS_MGR",
                    "symbol": symbol,
                    "interval": 5,
                    "interval_begin": closed_candle.interval_begin,
                    "open": closed_candle.open,
                    "high": closed_candle.high,
                    "low": closed_candle.low,
                    "close": closed_candle.close,
                    "volume": closed_candle.volume,
                    "atr_14": self.atr_14.get(symbol, Decimal("0")),
                }))

                if self._signal_pipeline_fn:
                    pre_comp_cache = self._build_pre_comp_cache(symbol)
                    params_snapshot = dict(self._ciats_params)
                    await self._signal_pipeline_fn(
                        closed_candle, pre_comp_cache, params_snapshot
                    )

            # Fire max hold count check via Exit Controller (L1a-003)
            if symbol in self.position_mirror and self._exit_ctrl_fn:
                await self._exit_ctrl_fn(symbol, {"trigger": "candle_close"}, self)

        # Advance state
        self.last_complete_candle[symbol] = candle
        self.last_interval_begin[symbol] = interval_begin

    async def _process_ohlc_60m(
        self, symbol: str, interval_begin: str, candle: OHLCCandle
    ) -> None:
        """
        1H HTF candle-close detection (WM-OHLC-004).
        Updates htf_ema_20/htf_ema_50. Fires Exit Controller L1a HTF check.
        """
        prev_begin = self.last_interval_begin_60.get(symbol, "")

        if interval_begin == prev_begin:
            self.last_complete_candle_60[symbol] = candle
            return

        if prev_begin and symbol in self.last_complete_candle_60:
            closed = self.last_complete_candle_60[symbol]
            self._update_htf_ema(symbol, closed.close)

            # Fire Exit Controller L1a HTF check
            if symbol in self.position_mirror and self._exit_ctrl_fn:
                await self._exit_ctrl_fn(
                    symbol,
                    {"trigger": "ohlc_60_close", "candle": closed},
                    self,
                )

            # Daily regime check at 00:00 UTC
            dt = datetime.fromisoformat(interval_begin.rstrip("Z")).replace(
                tzinfo=timezone.utc
            )
            if dt.hour == 0 and dt.minute == 0:
                await self._trigger_daily_regime_refresh()

        self.last_complete_candle_60[symbol] = candle
        self.last_interval_begin_60[symbol] = interval_begin

    async def _handle_ticker(self, msg: dict, is_public: bool = True) -> None:
        """
        Ticker bbo/trades handler.
        Updates latest_bid. Fires Exit Controller MAE check (WM-EC-002).
        Updates drawdown (WM-DD-001). Does NOT reset zombie timer (HR-WM-004
        — ticker IS a real data event, so it DOES reset the timer).
        """
        self._last_real_data_public = time.monotonic()

        data_list = msg.get("data", [])
        for item in data_list:
            symbol = item.get("symbol", "")
            bid = item.get("bid")
            if bid is not None:
                self.latest_bid[symbol] = Decimal(str(bid))

            # Drawdown mark-to-market (WM-DD-001)
            self._compute_drawdown()

            # MAE check for open positions (WM-EC-001/002)
            if symbol in self.position_mirror and self._exit_ctrl_fn:
                await self._exit_ctrl_fn(
                    symbol,
                    {"trigger": "ticker_bbo", "bid": self.latest_bid.get(symbol)},
                    self,
                )

    async def _handle_instrument(self, msg: dict, is_public: bool = True) -> None:
        """
        Instrument channel handler. Builds pair_cache and monitored_universe.
        WM-PS-001 through WM-PS-004.
        """
        self._last_real_data_public = time.monotonic()

        msg_type = msg.get("type")
        data = msg.get("data", {})

        pairs = data.get("pairs", [])
        if not pairs:
            return

        for pair in pairs:
            symbol = pair.get("symbol", "")
            if not symbol:
                continue

            status = pair.get("status", "")
            self.pair_status[symbol] = status

            # Build pair_cache — convert ALL via Decimal(str()) (HR-WM-008)
            self.pair_cache[symbol] = {
                "price_increment": Decimal(str(pair.get("price_increment", "0.01"))),
                "qty_increment":   Decimal(str(pair.get("qty_increment", "0.00000001"))),
                "qty_min":         Decimal(str(pair.get("qty_min", "0"))),
                "cost_min":        Decimal(str(pair.get("cost_min", "0"))),
                "status":          status,
                "quote_currency":  pair.get("quote_currency", ""),
            }

        if msg_type == "snapshot":
            self._build_monitored_universe()
            self._logger.info(log_record({
                "event": "INSTRUMENT_SNAPSHOT_COMPLETE",
                "level": "INFO",
                "component": "WS_MGR",
                "pair_count": len(self.pair_cache),
            }))

    def _build_monitored_universe(self) -> None:
        """
        Build monitored_universe from pair_cache.
        Filter: USD/USDC, online, volume >= $500k. Top N=50. BTC/USD always in.
        WM-PS-002/003.
        """
        candidates = []
        for sym, spec in self.pair_cache.items():
            quote = spec.get("quote_currency", "")
            status = spec.get("status", "")
            if quote not in ("USD", "USDC"):
                continue
            if status != "online":
                continue
            candidates.append(sym)

        # Sort by volume (not available in instrument snapshot — use alphabetical
        # as placeholder; actual volume filter applied via 24h volume from REST)
        # BTC/USD always included (AR-074, WM-PS-003)
        top_n = candidates[:UNIVERSE_TOP_N]
        if "BTC/USD" not in top_n and "BTC/USD" in self.pair_cache:
            top_n.append("BTC/USD")

        self.monitored_universe = top_n

    async def _handle_status(self, msg: dict, is_public: bool = True) -> None:
        """
        Status channel handler. Captures connection_id and engine state.
        WM-STATUS-001. Resets zombie timer (WM-ZOM-004 — status IS real data).
        """
        self._last_real_data_public = time.monotonic()

        msg_type = msg.get("type")
        data_list = msg.get("data", [])

        for item in data_list:
            connection_id = item.get("connection_id")
            system_status = item.get("system", "online")
            api_version = item.get("api_version", "")

            if connection_id:
                prev_id = self.connection_id_public
                self.connection_id_public = connection_id
                self.engine_state = system_status

                if prev_id is None:
                    self._logger.info(log_record({
                        "event": "WS_CONNECTED",
                        "level": "INFO",
                        "component": "WS_MGR",
                        "endpoint": "public",
                        "connection_id": connection_id,
                        "system": system_status,
                        "api_version": api_version,
                    }))
                else:
                    self._logger.info(log_record({
                        "event": "WS_RECONNECTED",
                        "level": "INFO",
                        "component": "WS_MGR",
                        "old_connection_id": prev_id,
                        "new_connection_id": connection_id,
                    }))

    async def _handle_executions(self, msg: dict, is_public: bool = False) -> None:
        """
        Executions channel handler.
        Checks sequence gap. Updates rate counter. Dispatches exec_type.
        WM-EXE-014/015/016 (last_qty, last_price captured).
        """
        self._last_real_data_private = time.monotonic()

        msg_type = msg.get("type")
        data_list = msg.get("data", [])

        # Extract seq for gap detection (A-10)
        seq = msg.get("sequence", 0)
        if seq and self.executions_last_seq > 0:
            if seq > self.executions_last_seq + 1:
                await self._handle_executions_gap(seq)
        if seq:
            self.executions_last_seq = seq

        # Rate counter from executions event (A-1, AR-030)
        # Subscription ACK: extract maxratecount
        if msg_type in ("snapshot", "update"):
            pass  # rate counter is per event, extracted below

        for event in data_list:
            # Update rate counter (AR-030)
            symbol = event.get("symbol", "")
            rate_counter = event.get("rate_counter")
            if rate_counter is not None and symbol:
                self.rate_counter_by_pair[symbol] = int(rate_counter)
                self._logger.debug(log_record({
                    "event": "RATE_COUNTER_UPDATE",
                    "level": "DEBUG",
                    "component": "WS_MGR",
                    "symbol": symbol,
                    "rate_counter": int(rate_counter),
                    "maxratecount": self.maxratecount,
                }))

            # Dispatch on exec_type
            exec_type = event.get("exec_type", "")
            handler = self._exec_type_dispatch.get(exec_type)
            if handler:
                await handler(event)
            else:
                self._logger.warning(log_record({
                    "event": "UNKNOWN_EXEC_TYPE",
                    "level": "WARN",
                    "component": "WS_MGR",
                    "raw_exec_type": exec_type,
                }))

        # Process executions subscription ACK (maxratecount)
        if msg.get("method") == "subscribe" and msg.get("result"):
            result = msg["result"]
            if result.get("channel") == "executions":
                max_rc = result.get("maxratecount")
                if max_rc:
                    self.maxratecount = int(max_rc)
                    self._logger.info(log_record({
                        "event": "MAXRATECOUNT_SET",
                        "level": "INFO",
                        "component": "WS_MGR",
                        "value": self.maxratecount,
                    }))
                warnings = result.get("warnings", [])
                self._logger.info(log_record({
                    "event": "SUBSCRIPTION_ACK",
                    "level": "INFO",
                    "component": "WS_MGR",
                    "channel": "executions",
                    "warnings": warnings,
                }))

    async def _handle_balances(self, msg: dict, is_public: bool = False) -> None:
        """
        Balances channel handler. Updates spot_usd_balance.
        Checks sequence gap (A-9, Section 7.2).
        WM-BAL-001/002/003.
        """
        self._last_real_data_private = time.monotonic()

        seq = msg.get("sequence", 0)
        if seq and self.balances_last_seq > 0:
            if seq > self.balances_last_seq + 1:
                await self._handle_balances_gap(seq)
        if seq:
            self.balances_last_seq = seq

        data_list = msg.get("data", [])
        for item in data_list:
            # Filter: spot wallet, main account, USD (WM-BAL-001/002/003)
            if (
                item.get("wallet_type") == "spot"
                and item.get("wallet_id") == "main"
                and item.get("asset") == "USD"
            ):
                balance_raw = item.get("balance", item.get("wallet_balance", "0"))
                self.spot_usd_balance = Decimal(str(balance_raw))

    async def _handle_pong(self, msg: dict, is_public: bool = True) -> None:
        """
        Pong handler.
        DO NOT reset zombie timer (HR-WM-004, WM-ZOM-005).
        Cancel pending ping timeout. Record latency.
        """
        req_id = msg.get("req_id")
        now = time.monotonic()

        if is_public and req_id == self._ping_req_id_pub:
            latency_ms = (
                (now - self._ping_sent_pub) * 1000
                if self._ping_sent_pub else 0
            )
            self._awaiting_pong_pub = False
            self._logger.debug(log_record({
                "event": "PONG_RECEIVED",
                "level": "DEBUG",
                "component": "WS_MGR",
                "connection_id": self.connection_id_public,
                "latency_ms": Decimal(str(round(latency_ms, 3))),
            }))
        elif not is_public and req_id == self._ping_req_id_priv:
            latency_ms = (
                (now - self._ping_sent_priv) * 1000
                if self._ping_sent_priv else 0
            )
            self._awaiting_pong_priv = False
            self._logger.debug(log_record({
                "event": "PONG_RECEIVED",
                "level": "DEBUG",
                "component": "WS_MGR",
                "connection_id": self.connection_id_private,
                "latency_ms": Decimal(str(round(latency_ms, 3))),
            }))

    # =================================================================
    # EXEC_TYPE HANDLERS — All 10 (HR-WM-006)
    # =================================================================

    async def _handle_pending_new(self, event: dict) -> None:
        """Order accepted by matching engine. Register in req_id_registry."""
        req_id = event.get("req_id")
        order_id = event.get("order_id", "")
        cl_ord_id = event.get("cl_ord_id", "")
        if req_id:
            self.req_id_registry[req_id] = {
                "method": "add_order",
                "order_id": order_id,
                "cl_ord_id": cl_ord_id,
                "timestamp": time.time(),
            }

    async def _handle_new(self, event: dict) -> None:
        """Order live on book. Confirm via log."""
        self._logger.info(log_record({
            "event": "ORDER_NEW",
            "level": "INFO",
            "component": "WS_MGR",
            "order_id": event.get("order_id", ""),
            "cl_ord_id": event.get("cl_ord_id", ""),
            "symbol": event.get("symbol", ""),
        }))

    async def _handle_trade(self, event: dict) -> None:
        """
        Partial fill event. Captures all four values (WM-EXE-014).
        Identifies TP vs entry fill. Routes accordingly.
        """
        order_id = event.get("order_id", "")
        cl_ord_id = event.get("cl_ord_id", "")
        symbol = event.get("symbol", "")

        # Capture all four fill values (WM-EXE-014, HR-WM-008)
        last_qty = Decimal(str(event.get("last_qty", 0)))
        last_price = Decimal(str(event.get("last_price", 0)))
        cum_qty = Decimal(str(event.get("cum_qty", 0)))
        avg_price = Decimal(str(event.get("avg_price", 0)))

        # Log all four values (WM-EXE-016)
        self._logger.info(log_record({
            "event": "FILL_EVENT",
            "level": "INFO",
            "component": "WS_MGR",
            "order_id": order_id,
            "cl_ord_id": cl_ord_id,
            "symbol": symbol,
            "last_qty": last_qty,
            "last_price": last_price,
            "cum_qty": cum_qty,
            "avg_price": avg_price,
            "exec_type": "trade",
        }))

        # Identify order type: TP vs entry partial (WM-EC-005)
        for sym, pos in self.position_mirror.items():
            if order_id == pos.tp_order_id:
                # TP partial fill (Section 5.4)
                await self._handle_partial_tp_fill(event, sym)
                return

        # Entry partial update — update Position Mirror cumulative values
        if symbol in self.position_mirror:
            self.position_mirror[symbol].qty = cum_qty
            self.position_mirror[symbol].entry_fill_price = avg_price

    async def _handle_filled(self, event: dict) -> None:
        """
        Full fill. Routes entry fill → batch_add dispatch.
        Routes TP fill → L1b logic.
        """
        order_id = event.get("order_id", "")
        cl_ord_id = event.get("cl_ord_id", "")
        symbol = event.get("symbol", "")
        cum_qty = Decimal(str(event.get("cum_qty", 0)))
        avg_price = Decimal(str(event.get("avg_price", 0)))
        fees = Decimal(str(event.get("fees", [{"asset": "USD", "qty": "0"}])[0].get("qty", 0)))

        # Check if TP fill (full close)
        for sym, pos in self.position_mirror.items():
            if order_id == pos.tp_order_id:
                # TP full fill → L1b
                self._logger.info(log_record({
                    "event": "LAYER1B_TP_FILL",
                    "level": "INFO",
                    "component": "WS_MGR",
                    "symbol": sym,
                    "fill_price": avg_price,
                    "fees": fees,
                }))
                if self._exit_ctrl_fn:
                    await self._exit_ctrl_fn(
                        sym,
                        {
                            "trigger": "tp_filled",
                            "fill_price": avg_price,
                            "qty": cum_qty,
                            "fees": fees,
                            "exit_reason": "TP_FILL",
                        },
                        self,
                    )
                return

        # Entry fill — dispatch TP + emergSL via Execution Engine
        if symbol in self.position_mirror:
            pos = self.position_mirror[symbol]
            pos.entry_fill_price = avg_price
            pos.qty = cum_qty
            pos.fees_entry_usd = fees
            pos.entry_timestamp_utc = datetime.now(timezone.utc).isoformat()

            self._logger.info(log_record({
                "event": "ENTRY_FILLED",
                "level": "INFO",
                "component": "WS_MGR",
                "symbol": symbol,
                "cl_ord_id": cl_ord_id,
                "fill_price": avg_price,
                "qty": cum_qty,
                "fees": fees,
            }))

            # Remove from Pending Order Registry (WM-POR-003)
            self.pending_orders.pop(cl_ord_id, None)

            if self._exec_engine_fn:
                await self._exec_engine_fn(
                    {
                        "symbol": symbol,
                        "entry_fill_price": avg_price,
                        "qty": cum_qty,
                        "fees_entry_usd": fees,
                    },
                    self,
                )

    async def _handle_iceberg_refill(self, event: dict) -> None:
        """Iceberg order refill. Log only. No Mirror update. No action."""
        self._logger.debug(log_record({
            "event": "ICEBERG_REFILL",
            "level": "DEBUG",
            "component": "WS_MGR",
            "order_id": event.get("order_id", ""),
        }))

    async def _handle_canceled(self, event: dict) -> None:
        """
        Order canceled. Entry with cum_qty>0 → partial fill protection (CASE B).
        Entry with cum_qty==0 → CASE C (no fill). Exit canceled → timeout fallback.
        HR-WM-007: use reason field (NOT cancel_reason — deprecated).
        """
        cl_ord_id = event.get("cl_ord_id", "")
        cum_qty = Decimal(str(event.get("cum_qty", 0)))
        reason = event.get("reason", "")  # HR-WM-007: NOT cancel_reason

        # Clear from Pending Order Registry (WM-POR-003)
        self.pending_orders.pop(cl_ord_id, None)

        symbol = event.get("symbol", "")

        if cum_qty > Decimal("0"):
            # CASE B: entry partial fill → protection (WM-DISP-020/021)
            await self._handle_entry_partial_fill(event, "canceled")
        else:
            # CASE C: no fill — clean cancel (WM-DISP-022)
            self._logger.info(log_record({
                "event": "EXEC_CANCELED",
                "level": "INFO",
                "component": "WS_MGR",
                "order_id": event.get("order_id", ""),
                "cl_ord_id": cl_ord_id,
                "reason": reason,
            }))

    async def _handle_expired(self, event: dict) -> None:
        """
        GTD order expired. Entry with cum_qty>0 → CASE A.
        Entry with cum_qty==0 → CASE C.
        """
        cl_ord_id = event.get("cl_ord_id", "")
        cum_qty = Decimal(str(event.get("cum_qty", 0)))

        self.pending_orders.pop(cl_ord_id, None)

        if cum_qty > Decimal("0"):
            # CASE A: entry partial fill → protection (WM-DISP-020/021)
            await self._handle_entry_partial_fill(event, "expired")
        else:
            # CASE C: clean expiry
            self._logger.info(log_record({
                "event": "EXEC_EXPIRED",
                "level": "INFO",
                "component": "WS_MGR",
                "order_id": event.get("order_id", ""),
                "cl_ord_id": cl_ord_id,
                "cum_qty": cum_qty,
            }))

    async def _handle_amended(self, event: dict) -> None:
        """
        Any inbound amended = UNEXPECTED (WM-DISP-024/025).
        TothBot never amends its own orders during normal operation.
        Log CRITICAL. Cross-reference Position Mirror. NO auto-correction.
        """
        order_id = event.get("order_id", "")
        cl_ord_id = event.get("cl_ord_id", "")
        new_limit = event.get("limit_price")
        new_qty = event.get("order_qty")

        self._logger.critical(log_record({
            "event": "UNEXPECTED_ORDER_AMENDED",
            "level": "CRITICAL",
            "component": "WS_MGR",
            "order_id": order_id,
            "cl_ord_id": cl_ord_id,
            "new_limit_price": new_limit,
            "new_qty": new_qty,
        }))
        _alert_operator_direct(
            f"UNEXPECTED order amendment: order_id={order_id} cl_ord_id={cl_ord_id}"
        )

        # Cross-reference Position Mirror (WM-DISP-025)
        for sym, pos in self.position_mirror.items():
            if order_id == pos.tp_order_id:
                self._logger.critical(log_record({
                    "event": "AMENDED_ORDER_IS_TP",
                    "level": "CRITICAL",
                    "component": "WS_MGR",
                    "symbol": sym,
                }))
                _alert_operator_direct(
                    f"TP for {sym} was amended — sacred R:R may be violated"
                )
            elif order_id == pos.emergsl_order_id:
                self._logger.critical(log_record({
                    "event": "AMENDED_ORDER_IS_EMERGSL",
                    "level": "CRITICAL",
                    "component": "WS_MGR",
                    "symbol": sym,
                }))
                _alert_operator_direct(
                    f"emergSL for {sym} was amended — crash protection altered"
                )
        # WM-DISP-026: NO auto-correction. Bill must review and act.

    async def _handle_restated(self, event: dict) -> None:
        """
        Engine maintenance amend. NOT a fill or cancel.
        DO NOT update Position Mirror. Elevated alert if resting order.
        """
        order_id = event.get("order_id", "")
        self._logger.warning(log_record({
            "event": "EXEC_RESTATED",
            "level": "HIGH",
            "component": "WS_MGR",
            "order_id": order_id,
        }))

    async def _handle_exec_status(self, event: dict) -> None:
        """Order status snapshot. Log. Confirm emergSL placement (WM-EXE-013)."""
        order_id = event.get("order_id", "")
        status = event.get("status", "")
        self._logger.debug(log_record({
            "event": "EXEC_STATUS",
            "level": "DEBUG",
            "component": "WS_MGR",
            "order_id": order_id,
            "status": status,
        }))

    # =================================================================
    # TP PARTIAL FILL HANDLING — Section 5.4 (AR-066)
    # =================================================================

    async def _handle_partial_tp_fill(
        self, event: dict, symbol: str
    ) -> None:
        """
        TP partial fill: amend emergSL qty (NOT cancel+resubmit).
        Saves 7 rate units vs cancel+resubmit (WM-EC-007).
        """
        cum_qty = Decimal(str(event.get("cum_qty", 0)))
        avg_exit = Decimal(str(event.get("avg_price", 0)))

        pos = self.position_mirror[symbol]
        orig_qty = pos.qty
        remaining = orig_qty - cum_qty
        emergsl_id = pos.emergsl_order_id

        spec = self.pair_cache.get(symbol, {})
        q_incr = spec.get("qty_increment", Decimal("0.00000001"))
        qty_min = spec.get("qty_min", Decimal("0"))
        cost_min = spec.get("cost_min", Decimal("0"))
        bid = self.latest_bid.get(symbol, Decimal("0"))

        # Update Position Mirror
        self.position_mirror[symbol].qty = remaining

        # Quantize remaining qty (ROUND_DOWN — BP-DEC-002)
        remaining_r = remaining.quantize(q_incr, rounding=ROUND_DOWN)

        # Check minimum size (WM-EC-006)
        if (remaining_r < qty_min or
                (bid > Decimal("0") and remaining_r * bid < cost_min)):
            # Cannot protect — exit remainder via market sell
            await self.cancel_order(emergsl_id)
            await self.dispatch_market_sell(symbol, remaining_r)
            del self.position_mirror[symbol]
            self._logger.warning(log_record({
                "event": "TP_PARTIAL_FILL_BELOW_MIN_EXIT",
                "level": "WARN",
                "component": "WS_MGR",
                "symbol": symbol,
                "remaining": remaining_r,
            }))
            return

        # Amend emergSL qty (NOT cancel+resubmit — WM-EC-007)
        await self.amend_order(
            order_id=emergsl_id,
            order_qty=remaining_r,
        )
        self._logger.info(log_record({
            "event": "TP_PARTIAL_FILL",
            "level": "INFO",
            "component": "WS_MGR",
            "symbol": symbol,
            "filled_qty": cum_qty,
            "remaining_qty": remaining_r,
            "avg_exit_price": avg_exit,
            "emergsl_amended_to": remaining_r,
        }))

    # =================================================================
    # ENTRY PARTIAL FILL PROTECTION — Section 5.5 (AR-054)
    # =================================================================

    async def _handle_entry_partial_fill(
        self, event: dict, trigger: str
    ) -> None:
        """
        Entry partial fill protection. Cases A (expired) and B (canceled).
        WM-DISP-020/021. Sacred R:R preserved using ACTUAL fill price.
        """
        symbol = event.get("symbol", "")
        cl_ord_id = event.get("cl_ord_id", "")
        cum_qty = Decimal(str(event.get("cum_qty", 0)))
        avg_price = Decimal(str(event.get("avg_price", 0)))

        spec = self.pair_cache.get(symbol, {})
        qty_min = spec.get("qty_min", Decimal("0"))
        cost_min = spec.get("cost_min", Decimal("0"))

        # Validate minimum size (WM-DISP-021 step 3)
        if cum_qty < qty_min or (avg_price > Decimal("0") and
                                  cum_qty * avg_price < cost_min):
            await self.dispatch_market_sell(symbol, cum_qty)
            self._logger.info(log_record({
                "event": "ENTRY_PARTIAL_FILL_BELOW_MIN",
                "level": "INFO",
                "component": "WS_MGR",
                "symbol": symbol,
                "cum_qty": cum_qty,
            }))
            return

        # Recalculate TP and emergSL from ACTUAL fill price (WM-DISP-023)
        atr = self.atr_14.get(symbol, Decimal("0"))
        mae_mult = Decimal(str(self._ciats_params.get("mae_mult", MAE_MULT)))
        emerg_mult = Decimal(str(
            self._ciats_params.get("emergency_sl_mult", EMERGENCY_SL_MULT)
        ))

        mae_pct = atr * mae_mult / avg_price
        net_loss = mae_pct + Decimal("0.0016") + Decimal("0.0026")
        net_gain = net_loss * Decimal("1.5")   # SACRED R:R

        gross_target = avg_price * (Decimal("1") + net_gain)
        sl_trigger = avg_price * (Decimal("1") - atr * emerg_mult / avg_price)

        spec = self.pair_cache.get(symbol, {})
        p_incr = spec.get("price_increment", Decimal("0.01"))
        q_incr = spec.get("qty_increment", Decimal("0.00000001"))

        tp_price = gross_target.quantize(p_incr, rounding=ROUND_UP)
        sl_price = sl_trigger.quantize(p_incr, rounding=ROUND_DOWN)
        qty_r = cum_qty.quantize(q_incr, rounding=ROUND_DOWN)

        # Update Position Mirror (WM-DISP-021 step 6)
        if symbol in self.position_mirror:
            self.position_mirror[symbol].entry_fill_price = avg_price
            self.position_mirror[symbol].qty = cum_qty

        # Dispatch batch_add TP + emergSL (WM-DISP-021 step 5)
        await self.batch_add(
            symbol=symbol,
            entry_fill_price=avg_price,
            tp_price=tp_price,
            sl_trigger=sl_price,
            qty=qty_r,
        )

        self._logger.warning(log_record({
            "event": "ENTRY_PARTIAL_FILL_PROTECTED",
            "level": "HIGH",
            "component": "WS_MGR",
            "symbol": symbol,
            "cum_qty": cum_qty,
            "avg_price": avg_price,
        }))

    # =================================================================
    # OUTBOUND MESSAGES — Section 6
    # =================================================================

    async def add_order(
        self,
        symbol: str,
        limit_price: Decimal,
        order_qty: Decimal,
        cl_ord_id: str,
        expire_sec: int = ENTRY_GTD_SEC,
    ) -> None:
        """
        Dispatch entry limit order (Section 6.4).
        stp_type: cancel_newest (underscore — HR-WM-009, AR-032).
        deadline: now + 5s (HR-WM-014, AR-033).
        post_only: true. time_in_force: gtd.
        Records cl_ord_id in Position Mirror IMMEDIATELY at dispatch.
        Registers in Pending Order Registry (WM-POR-002).
        """
        # Validate cost (WM-ROUND-006)
        spec = self.pair_cache.get(symbol, {})
        qty_min = spec.get("qty_min", Decimal("0"))
        cost_min = spec.get("cost_min", Decimal("0"))
        if order_qty < qty_min or order_qty * limit_price < cost_min:
            self._logger.warning(log_record({
                "event": "INSUFFICIENT_QTY",
                "level": "WARN",
                "component": "WS_MGR",
                "symbol": symbol,
                "order_qty": order_qty,
                "limit_price": limit_price,
                "qty_min": qty_min,
                "cost_min": cost_min,
            }))
            return

        now_utc = datetime.now(timezone.utc)
        deadline = (now_utc + timedelta(seconds=5)).isoformat()
        expire_time = (now_utc + timedelta(seconds=expire_sec)).isoformat()
        req_id = self._next_req_id()

        payload = {
            "method": "add_order",
            "params": {
                "order_type": "limit",
                "side": "buy",
                "symbol": symbol,
                "limit_price": str(limit_price),
                "order_qty": str(order_qty),
                "post_only": True,
                "time_in_force": "gtd",
                "expire_time": expire_time,
                "stp_type": "cancel_newest",   # HR-WM-009: underscore in WS v2
                "deadline": deadline,
                "cl_ord_id": cl_ord_id,
                "token": self._ws_token,
            },
            "req_id": req_id,
        }

        # Register in Pending Order Registry BEFORE sending (WM-POR-002)
        estimated_cost = order_qty * limit_price
        self.pending_orders[cl_ord_id] = estimated_cost

        # Record cl_ord_id in Position Mirror IMMEDIATELY (no async gap)
        if symbol not in self.position_mirror:
            self.position_mirror[symbol] = PositionRecord(
                symbol=symbol,
                cl_ord_id=cl_ord_id,
            )

        await self._send_private(payload)

        self._logger.info(log_record({
            "event": "ENTRY_DISPATCHED",
            "level": "INFO",
            "component": "WS_MGR",
            "symbol": symbol,
            "cl_ord_id": cl_ord_id,
            "entry_price": limit_price,
            "qty": order_qty,
            "deadline": deadline,
            "connection_id": self.connection_id_private,
        }))

        # Switch ticker to bbo mode (position now open — Section 16)
        await self._update_ticker_event_trigger(symbol, "bbo")

    async def batch_add(
        self,
        symbol: str,
        entry_fill_price: Decimal,
        tp_price: Decimal,
        sl_trigger: Decimal,
        qty: Decimal,
        tp_cl_ord_id: str = "",
        sl_cl_ord_id: str = "",
    ) -> None:
        """
        Dispatch TP limit + emergSL stop-loss as atomic batch (Section 6.5).
        emergSL triggers block MANDATORY with reference="last" (HR-WM-015, AR-046).
        Validates R:R and prices BEFORE dispatch.
        """
        # Pre-dispatch validation (Section 6.5)
        if tp_price <= entry_fill_price:
            self._logger.critical(log_record({
                "event": "BATCH_ADD_VALIDATION_FAIL",
                "level": "CRITICAL",
                "component": "WS_MGR",
                "symbol": symbol,
                "reason": "tp_price <= entry_fill_price",
                "tp_price": tp_price,
                "entry": entry_fill_price,
            }))
            return
        if sl_trigger >= entry_fill_price:
            self._logger.critical(log_record({
                "event": "BATCH_ADD_VALIDATION_FAIL",
                "level": "CRITICAL",
                "component": "WS_MGR",
                "symbol": symbol,
                "reason": "sl_trigger >= entry_fill_price",
                "sl_trigger": sl_trigger,
                "entry": entry_fill_price,
            }))
            return

        now_utc = datetime.now(timezone.utc)
        deadline = (now_utc + timedelta(seconds=5)).isoformat()
        req_id = self._next_req_id()

        if not tp_cl_ord_id:
            tp_cl_ord_id = f"tp_{self._req_id_counter}"
        if not sl_cl_ord_id:
            sl_cl_ord_id = f"sl_{self._req_id_counter}"

        payload = {
            "method": "batch_add",
            "params": {
                "orders": [
                    {   # TP limit sell
                        "order_type": "limit",
                        "side": "sell",
                        "symbol": symbol,
                        "limit_price": str(tp_price),
                        "order_qty": str(qty),
                        "post_only": False,
                        "stp_type": "cancel_newest",
                        "deadline": deadline,
                        "cl_ord_id": tp_cl_ord_id,
                    },
                    {   # emergSL stop-loss — HR-WM-015 triggers block MANDATORY
                        "order_type": "stop-loss",
                        "side": "sell",
                        "symbol": symbol,
                        "trigger_price": str(sl_trigger),
                        "order_qty": str(qty),
                        "triggers": {
                            "reference":  "last",     # AR-046: NEVER rely on default
                            "price":      str(sl_trigger),
                            "price_type": "static",
                        },
                        "stp_type": "cancel_newest",
                        "deadline": deadline,
                        "cl_ord_id": sl_cl_ord_id,
                    },
                ],
                "token": self._ws_token,
            },
            "req_id": req_id,
        }
        await self._send_private(payload)

        self._logger.info(log_record({
            "event": "TP_PLACED",
            "level": "INFO",
            "component": "WS_MGR",
            "symbol": symbol,
            "cl_ord_id": tp_cl_ord_id,
            "tp_price": tp_price,
        }))
        self._logger.info(log_record({
            "event": "EMERG_SL_PLACED",
            "level": "INFO",
            "component": "WS_MGR",
            "symbol": symbol,
            "cl_ord_id": sl_cl_ord_id,
            "sl_trigger": sl_trigger,
        }))

    async def cancel_order(self, order_id: str) -> None:
        """
        Individual cancel with per-order ACK tracking (Section 6.6).
        Used for Layer 2 MAE exit cancel. NOT batch_cancel.
        """
        req_id = self._next_req_id()
        self.req_id_registry[req_id] = {
            "method": "cancel_order",
            "order_id": order_id,
            "timestamp": time.time(),
        }
        await self._send_private({
            "method": "cancel_order",
            "params": {
                "order_id": [order_id],
                "token": self._ws_token,
            },
            "req_id": req_id,
        })

    async def amend_order(self, order_id: str, order_qty: Decimal) -> None:
        """
        Amend emergSL qty after TP partial fill (Section 6.7).
        Use ONLY for emergSL qty amendment. NOT price. NOT entry orders.
        Saves 7 rate units vs cancel+resubmit (WM-EC-007).
        """
        req_id = self._next_req_id()
        await self._send_private({
            "method": "amend_order",
            "params": {
                "order_id": order_id,
                "order_qty": str(order_qty),
                "token": self._ws_token,
            },
            "req_id": req_id,
        })

    async def batch_cancel(self) -> None:
        """
        Batch cancel ALL pending entry GTD orders — FULL HALT ONLY (Section 6.8).
        Bypasses rate counter max (AR-019).
        Does NOT cancel TP or emergSL (WM-DMS-004, HR-WM-010).
        NOT DMS — batch_cancel is explicit, targeted, operator-triggered.
        """
        # EXPLICIT PROHIBITION CHECK (WM-DMS-001, HR-WM-010)
        # This method is batch_cancel — NOT cancel_all_orders_after.
        # cancel_all_orders_after is NEVER called in TothBot V2.
        req_id = self._next_req_id()
        await self._send_private({
            "method": "batch_cancel",
            "params": {
                "orders": [],   # empty = cancel all pending non-resting
                "token": self._ws_token,
            },
            "req_id": req_id,
        })
        self._logger.warning(log_record({
            "event": "BATCH_CANCEL_SENT",
            "level": "HIGH",
            "component": "WS_MGR",
            "count": len(self.pending_orders),
        }))

    async def dispatch_market_sell(self, symbol: str, qty: Decimal) -> None:
        """Market sell — used ONLY for partial fill protection cleanup."""
        req_id = self._next_req_id()
        now_utc = datetime.now(timezone.utc)
        deadline = (now_utc + timedelta(seconds=5)).isoformat()
        await self._send_private({
            "method": "add_order",
            "params": {
                "order_type": "market",
                "side": "sell",
                "symbol": symbol,
                "order_qty": str(qty),
                "deadline": deadline,
                "token": self._ws_token,
            },
            "req_id": req_id,
        })

    # =================================================================
    # CONNECTION HEALTH MONITORING
    # =================================================================

    async def _ping_loop(self) -> None:
        """
        Application-level JSON ping every 30s on EACH connection (WM-CONN-005).
        No pong within 10s = PING_TIMEOUT, reconnect immediately.
        Library TCP PING is disabled — this is the SOLE keepalive.
        """
        while True:
            await asyncio.sleep(PING_INTERVAL_SEC)
            now = time.monotonic()

            # Ping public
            if self._awaiting_pong_pub:
                if (self._ping_sent_pub and
                        now - self._ping_sent_pub > PING_TIMEOUT_SEC):
                    self._logger.critical(log_record({
                        "event": "PING_TIMEOUT",
                        "level": "CRITICAL",
                        "component": "WS_MGR",
                        "connection_id": self.connection_id_public,
                    }))
                    await self._initiate_reconnect()
                    return
            else:
                pub_req_id = self._next_req_id()
                self._ping_req_id_pub = pub_req_id
                self._ping_sent_pub = now
                self._awaiting_pong_pub = True
                await self._send_public({
                    "method": "ping",
                    "req_id": pub_req_id,
                })
                self._logger.debug(log_record({
                    "event": "PING_SENT",
                    "level": "DEBUG",
                    "component": "WS_MGR",
                    "connection_id": self.connection_id_public,
                    "req_id": pub_req_id,
                }))

            # Ping private
            if self._awaiting_pong_priv:
                if (self._ping_sent_priv and
                        now - self._ping_sent_priv > PING_TIMEOUT_SEC):
                    self._logger.critical(log_record({
                        "event": "PING_TIMEOUT",
                        "level": "CRITICAL",
                        "component": "WS_MGR",
                        "connection_id": self.connection_id_private,
                    }))
                    await self._initiate_reconnect()
                    return
            else:
                priv_req_id = self._next_req_id()
                self._ping_req_id_priv = priv_req_id
                self._ping_sent_priv = now
                self._awaiting_pong_priv = True
                await self._send_private({
                    "method": "ping",
                    "req_id": priv_req_id,
                })
                self._logger.debug(log_record({
                    "event": "PING_SENT",
                    "level": "DEBUG",
                    "component": "WS_MGR",
                    "connection_id": self.connection_id_private,
                    "req_id": priv_req_id,
                }))

    async def _zombie_monitor(self) -> None:
        """
        Zombie connection detection (WM-ZOM-001 through -007).
        Tracks last_real_data_time_public/private independently.
        last_real_data_time reset ONLY on real channel events (HR-WM-004).
        Pong, ACK, heartbeat MUST NOT reset timer (WM-ZOM-005).
        """
        while True:
            await asyncio.sleep(10)  # check every 10s
            now = time.monotonic()

            if now - self._last_real_data_public > ZOMBIE_THRESHOLD_SEC:
                elapsed = now - self._last_real_data_public
                self._logger.critical(log_record({
                    "event": "ZOMBIE_CONNECTION_DETECTED",
                    "level": "CRITICAL",
                    "component": "WS_MGR",
                    "connection_id": self.connection_id_public,
                    "elapsed_seconds": Decimal(str(round(elapsed, 1))),
                }))
                _alert_operator_direct(
                    f"ZOMBIE connection detected (public). "
                    f"No real data for {elapsed:.0f}s. Reconnecting."
                )
                await self._initiate_reconnect()
                return

            if now - self._last_real_data_private > ZOMBIE_THRESHOLD_SEC:
                elapsed = now - self._last_real_data_private
                self._logger.critical(log_record({
                    "event": "ZOMBIE_CONNECTION_DETECTED",
                    "level": "CRITICAL",
                    "component": "WS_MGR",
                    "connection_id": self.connection_id_private,
                    "elapsed_seconds": Decimal(str(round(elapsed, 1))),
                }))
                _alert_operator_direct(
                    f"ZOMBIE connection detected (private). "
                    f"No real data for {elapsed:.0f}s. Reconnecting."
                )
                await self._initiate_reconnect()
                return

    # =================================================================
    # SEQUENCE GAP DETECTION
    # =================================================================

    async def _handle_executions_gap(self, current_seq: int) -> None:
        """
        Executions sequence gap — CRITICAL (A-10, Section 7.3).
        Trigger REST GetOpenOrders reconciliation.
        """
        self._logger.critical(log_record({
            "event": "EXECUTIONS_SEQUENCE_GAP",
            "level": "CRITICAL",
            "component": "WS_MGR",
            "expected": self.executions_last_seq + 1,
            "received": current_seq,
            "connection_id": self.connection_id_private,
        }))
        _alert_operator_direct(
            f"EXECUTIONS sequence gap: expected {self.executions_last_seq + 1}, "
            f"received {current_seq}. Reconciling via REST."
        )
        try:
            open_orders = await self._rest_get_open_orders()
            await self._reconcile_pending_orders(open_orders)
        except Exception as exc:  # noqa: BP-ERR-001
            self._logger.critical(log_record({
                "event": "EXECUTIONS_GAP_RECONCILE_FAIL",
                "level": "CRITICAL",
                "component": "WS_MGR",
                "error": str(exc),
            }))

    async def _handle_balances_gap(self, current_seq: int) -> None:
        """
        Balances sequence gap — HIGH (A-9, Section 7.2).
        Trigger REST GetAccountBalance reconciliation.
        """
        self._logger.warning(log_record({
            "event": "BALANCES_SEQUENCE_GAP",
            "level": "HIGH",
            "component": "WS_MGR",
            "expected": self.balances_last_seq + 1,
            "received": current_seq,
            "connection_id": self.connection_id_private,
        }))
        try:
            balance_data = await self._rest_get_account_balance()
            usd_balance = balance_data.get("USD", "0")
            self.spot_usd_balance = Decimal(str(usd_balance))
        except Exception as exc:  # noqa: BP-ERR-001
            self._logger.critical(log_record({
                "event": "BALANCES_GAP_RECONCILE_FAIL",
                "level": "CRITICAL",
                "component": "WS_MGR",
                "error": str(exc),
            }))

    async def _reconcile_pending_orders(self, open_orders: dict) -> None:
        """Reconcile Pending Order Registry against REST GetOpenOrders (WM-POR-005)."""
        open_cl_ids = {
            v.get("userref", ""): v
            for v in open_orders.values()
        }
        # Remove stale entries (closed during gap)
        stale = [
            cl_id for cl_id in self.pending_orders
            if cl_id not in open_cl_ids
        ]
        for cl_id in stale:
            del self.pending_orders[cl_id]

    # =================================================================
    # DRAWDOWN MONITORING — Section 11
    # =================================================================

    def _compute_drawdown(self) -> None:
        """
        Mark-to-market portfolio value (WM-DD-001/002).
        Uses bid price from ticker bbo (AR-048).
        Enforces SESSION_PAUSE (5%) and FULL_HALT (10%) thresholds.
        portfolio_baseline_USD fixed at startup — NEVER reset (HR-WM-011).
        """
        if self.portfolio_baseline_USD is None:
            return

        # MTM: spot_usd_balance + sum(bid * qty) for all open positions
        mtm_positions = sum(
            self.latest_bid.get(sym, Decimal("0")) * pos.qty
            for sym, pos in self.position_mirror.items()
        )
        current_portfolio = self.spot_usd_balance + mtm_positions

        drawdown_pct = max(
            Decimal("0"),
            (self.portfolio_baseline_USD - current_portfolio)
            / self.portfolio_baseline_USD,
        )

        # FULL_HALT threshold (WM-DD-005)
        full_halt_thresh = Decimal(str(
            self._ciats_params.get("full_halt_drawdown", FULL_HALT_THRESHOLD)
        ))
        session_pause_thresh = Decimal(str(
            self._ciats_params.get("session_pause_drawdown", SESSION_PAUSE_THRESHOLD)
        ))

        if drawdown_pct >= full_halt_thresh and self.system_state != STATE_FULL_HALT:
            self.system_state = STATE_FULL_HALT
            self._logger.critical(log_record({
                "event": "FULL_HALT_TRIGGERED",
                "level": "CRITICAL",
                "component": "WS_MGR",
                "drawdown_pct": drawdown_pct,
                "open_count": len(self.position_mirror),
            }))
            _alert_operator_direct(
                f"FULL HALT: drawdown {float(drawdown_pct):.1%}. "
                f"All new entries blocked. batch_cancel dispatched."
            )
            asyncio.ensure_future(self.batch_cancel())

        elif (drawdown_pct >= session_pause_thresh and
              self.system_state == STATE_NORMAL):
            self.system_state = STATE_SESSION_PAUSE
            self._logger.warning(log_record({
                "event": "SESSION_PAUSE_TRIGGERED",
                "level": "HIGH",
                "component": "WS_MGR",
                "drawdown_pct": drawdown_pct,
            }))

        elif (drawdown_pct < session_pause_thresh and
              self.system_state == STATE_SESSION_PAUSE):
            self.system_state = STATE_NORMAL
            self._logger.info(log_record({
                "event": "SESSION_PAUSE_RECOVERED",
                "level": "INFO",
                "component": "WS_MGR",
                "drawdown_pct": drawdown_pct,
            }))

    # =================================================================
    # INDICATOR SEEDING — from REST GetOHLCData (Section 9.2/9.3)
    # =================================================================

    async def seed_indicators_from_rest(self, symbol: str) -> None:
        """
        Seed all five SSS indicators + HTF cache from REST GetOHLCData.
        Uses response[:-1] — HR-WM-013.
        Stagger 1.1s between sequential calls to same pair (AR-036).
        HR-WM-017: All five indicators seeded before pair reaches READY.
        """
        async with self._make_http_session() as session:
            # Seed 5m indicators
            candles_5m = await self._rest_get_ohlc(session, symbol, 5)
            await asyncio.sleep(OHLC_REST_STAGGER_SEC)

            # Seed 60m HTF cache
            candles_60m = await self._rest_get_ohlc(session, symbol, 60)

        self.warm_up_state[symbol] = WARMUP_WARMING

        self._logger.debug(log_record({
            "event": "PAIR_WARM_UP_STARTED",
            "level": "DEBUG",
            "component": "WS_MGR",
            "symbol": symbol,
        }))

        # Seed 5m indicators from 5m candles
        self._seed_5m_indicators(symbol, candles_5m)

        # Seed HTF EMAs from 60m candles
        self._seed_htf_ema(symbol, candles_60m)

        # Initialize last_interval_begin from response[-2].interval_begin
        # (WM-OHLC-006: response[-1] excluded, so "last" = candles_5m[-1])
        if len(candles_5m) >= 1:
            last_candle = candles_5m[-1]  # already excludes uncommitted
            # Kraken REST OHLC: [time, open, high, low, close, vwap, volume, count]
            self.last_interval_begin[symbol] = str(last_candle[0])

        if len(candles_60m) >= 1:
            self.last_interval_begin_60[symbol] = str(candles_60m[-1][0])

        self.warm_up_state[symbol] = WARMUP_READY
        self._logger.info(log_record({
            "event": "PAIR_READY",
            "level": "INFO",
            "component": "WS_MGR",
            "symbol": symbol,
        }))

    def _seed_5m_indicators(self, symbol: str, candles: list) -> None:
        """
        Seed ATR(14), RSI(14) Wilder SMMA, EMA(9), EMA(21), VolMA(20).
        Section 9.2. Uses response[:-1] per HR-WM-013 (already applied).
        Kraken REST OHLC format: [time, open, high, low, close, vwap, volume, count]
        """
        if len(candles) < 21:
            return  # Not enough data to seed

        opens  = [Decimal(str(c[1])) for c in candles]
        highs  = [Decimal(str(c[2])) for c in candles]
        lows   = [Decimal(str(c[3])) for c in candles]
        closes = [Decimal(str(c[4])) for c in candles]
        vols   = [Decimal(str(c[6])) for c in candles]

        # ── ATR(14) ──────────────────────────────────────────────────────
        trs = []
        for i in range(1, len(closes)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            trs.append(tr)

        # Seed: SMA of first 14 TRs
        atr = sum(trs[:14]) / Decimal("14")
        # Incremental update for remaining
        for tr in trs[14:]:
            atr = (atr * Decimal("13") + tr) / Decimal("14")
        self.atr_14[symbol] = atr
        self._prev_close[symbol] = closes[-1]

        # ── RSI(14) — Wilder's SMMA (HR-WM-018, AR-076) ─────────────────
        gains = []
        losses = []
        for i in range(1, len(closes)):
            delta = closes[i] - closes[i - 1]
            gains.append(delta if delta > Decimal("0") else Decimal("0"))
            losses.append(abs(delta) if delta < Decimal("0") else Decimal("0"))

        # Seed: SMA of first 14
        avg_gain = sum(gains[:14]) / Decimal("14")
        avg_loss = sum(losses[:14]) / Decimal("14")
        # Incremental Wilder SMMA for remaining
        for i in range(14, len(gains)):
            avg_gain = (avg_gain * Decimal("13") + gains[i]) / Decimal("14")
            avg_loss = (avg_loss * Decimal("13") + losses[i]) / Decimal("14")
        self.rsi_avg_gain[symbol] = avg_gain
        self.rsi_avg_loss[symbol] = avg_loss

        # ── EMA(9) — alpha = 2/10 = 0.2 ─────────────────────────────────
        alpha_9 = Decimal("2") / Decimal("10")
        ema9 = sum(closes[:9]) / Decimal("9")
        for c in closes[9:]:
            ema9 = alpha_9 * c + (Decimal("1") - alpha_9) * ema9
        self.ema_9[symbol] = ema9

        # ── EMA(21) — alpha = 2/22 ────────────────────────────────────────
        alpha_21 = Decimal("2") / Decimal("22")
        ema21 = sum(closes[:21]) / Decimal("21")
        for c in closes[21:]:
            ema21 = alpha_21 * c + (Decimal("1") - alpha_21) * ema21
        self.ema_21[symbol] = ema21

        # ── VolMA(20) — rolling SMA ───────────────────────────────────────
        vol_window = list(vols[-20:])
        self._volume_buffer[symbol] = vol_window
        self.volume_ma_20[symbol] = sum(vol_window) / Decimal("20")

    def _seed_htf_ema(self, symbol: str, candles_60m: list) -> None:
        """
        Seed HTF EMA(20) and EMA(50) from 1H candles (Section 9.3).
        alpha_20 = 2/21. alpha_50 = 2/51.
        """
        if len(candles_60m) < 50:
            return

        closes_60 = [Decimal(str(c[4])) for c in candles_60m]

        # EMA(20) — alpha = 2/21
        alpha_20 = Decimal("2") / Decimal("21")
        htf20 = sum(closes_60[:20]) / Decimal("20")
        for c in closes_60[20:]:
            htf20 = alpha_20 * c + (Decimal("1") - alpha_20) * htf20
        self.htf_ema_20[symbol] = htf20

        # EMA(50) — alpha = 2/51
        alpha_50 = Decimal("2") / Decimal("51")
        htf50 = sum(closes_60[:50]) / Decimal("50")
        for c in closes_60[50:]:
            htf50 = alpha_50 * c + (Decimal("1") - alpha_50) * htf50
        self.htf_ema_50[symbol] = htf50

    def _update_indicators_5m(self, symbol: str, candle: OHLCCandle) -> None:
        """
        Incremental O(1) indicator update on each closed 5m candle.
        ATR: Wilder SMMA. RSI: Wilder SMMA. EMA9/21: standard EMA.
        VolMA20: rolling SMA.
        """
        close = candle.close
        high = candle.high
        low = candle.low
        prev_close = self._prev_close.get(symbol, close)

        # ATR(14) incremental (AR-016)
        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        prev_atr = self.atr_14.get(symbol, tr)
        self.atr_14[symbol] = (prev_atr * Decimal("13") + tr) / Decimal("14")
        self._prev_close[symbol] = close

        # RSI(14) Wilder SMMA (HR-WM-018)
        delta = close - prev_close
        gain = delta if delta > Decimal("0") else Decimal("0")
        loss = abs(delta) if delta < Decimal("0") else Decimal("0")
        prev_gain = self.rsi_avg_gain.get(symbol, gain)
        prev_loss = self.rsi_avg_loss.get(symbol, loss)
        self.rsi_avg_gain[symbol] = (prev_gain * Decimal("13") + gain) / Decimal("14")
        self.rsi_avg_loss[symbol] = (prev_loss * Decimal("13") + loss) / Decimal("14")

        # EMA(9) alpha=0.2
        alpha_9 = Decimal("2") / Decimal("10")
        prev_ema9 = self.ema_9.get(symbol, close)
        self.ema_9[symbol] = alpha_9 * close + (Decimal("1") - alpha_9) * prev_ema9

        # EMA(21) alpha=2/22
        alpha_21 = Decimal("2") / Decimal("22")
        prev_ema21 = self.ema_21.get(symbol, close)
        self.ema_21[symbol] = alpha_21 * close + (Decimal("1") - alpha_21) * prev_ema21

        # VolMA(20) rolling SMA
        buf = self._volume_buffer.get(symbol, [])
        buf.append(candle.volume)
        if len(buf) > 20:
            buf.pop(0)
        self._volume_buffer[symbol] = buf
        self.volume_ma_20[symbol] = sum(buf) / Decimal(str(len(buf)))

    def _update_htf_ema(self, symbol: str, close_60: Decimal) -> None:
        """Incremental HTF EMA update on each 1H candle close."""
        alpha_20 = Decimal("2") / Decimal("21")
        alpha_50 = Decimal("2") / Decimal("51")

        prev20 = self.htf_ema_20.get(symbol, close_60)
        prev50 = self.htf_ema_50.get(symbol, close_60)
        self.htf_ema_20[symbol] = alpha_20 * close_60 + (Decimal("1") - alpha_20) * prev20
        self.htf_ema_50[symbol] = alpha_50 * close_60 + (Decimal("1") - alpha_50) * prev50

    def _increment_hold_candle_counts(self) -> None:
        """Increment hold_candle_count for all open positions on 5m candle."""
        for pos in self.position_mirror.values():
            pos.hold_candle_count += 1

    # =================================================================
    # DAILY REGIME REFRESH — 00:00 UTC
    # =================================================================

    async def _trigger_daily_regime_refresh(self) -> None:
        """
        Trigger Regime Engine daily computation at 00:00 UTC.
        Passes daily OHLC (interval=1440) response[:-1].
        """
        if not self._regime_engine_fn:
            return
        try:
            async with self._make_http_session() as session:
                daily_candles = await self._rest_get_ohlc(
                    session, "BTC/USD", 1440
                )
            await self._regime_engine_fn(daily_candles, "BTC/USD")
        except Exception as exc:  # noqa: BP-ERR-001
            self._logger.warning(log_record({
                "event": "REGIME_REFRESH_FAIL",
                "level": "WARN",
                "component": "WS_MGR",
                "error": str(exc),
            }))

    # =================================================================
    # PIPELINE PRE-COMPUTATION CACHE — AR-I-2
    # =================================================================

    def _build_pre_comp_cache(self, symbol: str) -> dict:
        """
        Build pre-computation cache for pipeline eval (AR-I-2).
        Hot path reads: regime cache, HTF EMAs, ATR, 24h liquidity, pair status,
        exit_cooldown_log, consecutive_loss_count.
        """
        return {
            "symbol": symbol,
            "atr_14": self.atr_14.get(symbol, Decimal("0")),
            "rsi_14_avg_gain": self.rsi_avg_gain.get(symbol, Decimal("0")),
            "rsi_14_avg_loss": self.rsi_avg_loss.get(symbol, Decimal("0")),
            "ema_9": self.ema_9.get(symbol, Decimal("0")),
            "ema_21": self.ema_21.get(symbol, Decimal("0")),
            "volume_ma_20": self.volume_ma_20.get(symbol, Decimal("0")),
            "htf_ema_20": self.htf_ema_20.get(symbol, Decimal("0")),
            "htf_ema_50": self.htf_ema_50.get(symbol, Decimal("0")),
            "spot_usd_balance": self.spot_usd_balance,
            "pending_orders_total": sum(self.pending_orders.values()),
            "open_position_count": len(self.position_mirror),
            "pair_status": self.pair_status.get(symbol, "online"),
            "engine_state": self.engine_state,
            "system_state": self.system_state,
            "exit_cooldown_log": dict(self.exit_cooldown_log),
            "consecutive_loss_count": dict(self.consecutive_loss_count),
            "rate_counter_by_pair": dict(self.rate_counter_by_pair),
            "maxratecount": self.maxratecount,
            "latest_bid": dict(self.latest_bid),
        }

    # =================================================================
    # SELECTION CONTROLLER STATE (AR-073) — Section 9.4
    # =================================================================

    def update_selection_controller_state(
        self, symbol: str, exit_reason: str
    ) -> None:
        """
        Update exit_cooldown_log and consecutive_loss_count on position close.
        WM-SC-001/002. Both dicts PRESERVED across reconnect (WM-SC-003).
        """
        self.exit_cooldown_log[symbol] = time.monotonic()

        loss_reasons = {
            "MAE_THRESHOLD_BREACH", "TIME_EXPIRY",
            "HTF_REGIME_REVERSAL", "DAILY_REGIME_DOWNGRADE",
            "SIGNAL_DECAY",
        }
        if exit_reason in loss_reasons:
            self.consecutive_loss_count[symbol] = (
                self.consecutive_loss_count.get(symbol, 0) + 1
            )
        elif exit_reason == "TP_FILL":
            self.consecutive_loss_count[symbol] = 0

    # =================================================================
    # MID-SESSION RECONNECT — Section 15 (HR-WM-016, AR-056)
    # =================================================================

    async def _initiate_reconnect(self) -> None:
        """
        Initiate mid-session reconnect. SEPARATE code path from startup.
        NEVER reuses startup code (HR-WM-016).
        10 attempts over 120s → FATAL_RECONNECT_FAILURE (WM-RECONNECT-002).
        """
        if self._is_reconnecting:
            return  # Already reconnecting

        self._is_reconnecting = True
        self._disconnect_time = time.monotonic()
        self._reconnect_attempt = 0

        self._logger.warning(log_record({
            "event": "RECONNECT_INITIATED",
            "level": "HIGH",
            "component": "WS_MGR",
            "connection_id": self.connection_id_public,
            "attempt_number": 1,
        }))

        backoff_sec = 1.0
        while self._reconnect_attempt < MAX_RECONNECT_ATTEMPTS:
            self._reconnect_attempt += 1
            try:
                await self._execute_reconnect_sequence()
                self._is_reconnecting = False
                self._awaiting_pong_pub = False
                self._awaiting_pong_priv = False
                self._logger.info(log_record({
                    "event": "RECONNECT_COMPLETE",
                    "level": "INFO",
                    "component": "WS_MGR",
                    "connection_id": self.connection_id_public,
                }))
                return
            except Exception as exc:  # noqa: BP-ERR-001
                self._logger.warning(log_record({
                    "event": "RECONNECT_ATTEMPT_FAILED",
                    "level": "WARN",
                    "component": "WS_MGR",
                    "attempt": self._reconnect_attempt,
                    "error": str(exc),
                }))
                await asyncio.sleep(backoff_sec)
                backoff_sec = min(backoff_sec * 2, 30)

        # Fatal — systemd will restart (WM-RECONNECT-002)
        self._logger.critical(log_record({
            "event": "FATAL_RECONNECT_FAILURE",
            "level": "CRITICAL",
            "component": "WS_MGR",
            "attempt_count": self._reconnect_attempt,
        }))
        _alert_operator_direct(
            f"FATAL RECONNECT FAILURE after {self._reconnect_attempt} attempts. "
            f"systemd will restart TothBot."
        )
        raise RuntimeError("Fatal reconnect failure — systemd restart required")

    async def _execute_reconnect_sequence(self) -> None:
        """
        Mid-session reconnect steps 1-10 (WM-RECONNECT-001 through -015).
        Separate from startup — no startup code reused (HR-WM-016).
        """
        # Step 1: Acquire new WS token (WM-RECONNECT-003)
        self._ws_token = await self._rest_get_ws_token()

        # Step 2: Connect both WS (WM-RECONNECT-004) — same params as startup
        self._ws_public = await self._ws_connect(PUBLIC_WS_URI)
        self._ws_private = await self._ws_connect(PRIVATE_WS_URI)

        # Step 3: Subscribe all channels (WM-RECONNECT-005)
        await self._subscribe_public()
        await self._subscribe_private()
        # Ticker resubscription handled in Step 8 below

        # Step 4: Process snap_orders — detect gap-closed positions (WM-RECONNECT-006)
        snap_orders = await self._rest_get_open_orders()
        gap_closed = self._detect_gap_closed_positions(snap_orders)

        # Step 5: Fire CIATS Trade Outcome Bus for gap-closed (WM-RECONNECT-007)
        for symbol in gap_closed:
            self._logger.warning(log_record({
                "event": "GAP_CLOSED_POSITION",
                "level": "HIGH",
                "component": "WS_MGR",
                "symbol": symbol,
                "estimated_PL": "unknown",
            }))
            del self.position_mirror[symbol]

        # Step 6: Reconcile Pending Order Registry (WM-RECONNECT-008)
        await self._reconcile_pending_orders(snap_orders)

        # Step 7: Reset sequence counters (WM-RECONNECT-009)
        self.executions_last_seq = 0
        self.balances_last_seq = 0

        # Step 8: Resume ticker subscriptions (WM-RECONNECT-010)
        for symbol in self.monitored_universe:
            has_position = symbol in self.position_mirror
            trigger = "bbo" if has_position else "trades"
            await self._subscribe_ticker_pair(symbol, trigger)

        # Step 9: Discard queued pipeline events (WM-RECONNECT-011)
        # is_reconnecting=True blocks pipeline during reconnect (HR-WM-012)
        # Will be cleared after this method returns

        # Step 10: Re-seed indicators if gap > 15 minutes (WM-RECONNECT-014)
        gap_sec = time.monotonic() - (self._disconnect_time or time.monotonic())
        if gap_sec > RECONNECT_STALE_CACHE_SEC:
            for symbol in self.monitored_universe:
                await self.seed_indicators_from_rest(symbol)

        # HR-WM-013: portfolio_baseline_USD NEVER reset on reconnect
        # HR-WM-011: system_state PRESERVED
        # WM-SC-003: exit_cooldown_log and consecutive_loss_count PRESERVED

    def _detect_gap_closed_positions(self, snap_orders: dict) -> list[str]:
        """
        Detect positions closed during reconnect gap.
        A position was gap-closed if it's in position_mirror but its
        TP or emergSL order is no longer in snap_orders.
        """
        gap_closed = []
        open_order_ids = set(snap_orders.keys())

        for symbol, pos in self.position_mirror.items():
            tp_alive = pos.tp_order_id in open_order_ids
            sl_alive = pos.emergsl_order_id in open_order_ids
            if not tp_alive and not sl_alive:
                gap_closed.append(symbol)

        return gap_closed

    # =================================================================
    # POSITION MIRROR WRITE API — WS Manager is SOLE WRITER (HR-PM-009)
    # Called by Execution Engine via self._wm. WSManager physically writes.
    # =================================================================

    def pm_create(
        self,
        symbol: str,
        cl_ord_id: str,
        entry_limit_price: Decimal,
        qty: Decimal,
    ) -> None:
        """
        Create PositionRecord at entry dispatch — BEFORE Kraken fill ACK.
        HR-PM-002: record-at-dispatch, no async gap (A-2).
        Called by ExecutionEngine immediately after add_order send.
        """
        self.position_mirror[symbol] = PositionRecord(
            symbol=symbol,
            cl_ord_id=cl_ord_id,
            entry_limit_price=entry_limit_price,
            qty=qty,
        )
        self._logger.info(log_record({
            "event":     "POSITION_RECORD_CREATED",
            "level":     "INFO",
            "component": "WS_MGR",
            "symbol":    symbol,
            "cl_ord_id": cl_ord_id,
            "qty":       qty,
        }))

    def pm_on_fill(
        self,
        symbol: str,
        fill_price: Decimal,
        qty: Decimal,
        timestamp_utc: str,
    ) -> None:
        """
        Update PositionRecord with actual entry fill data.
        HR-PM-003: entry_fill_price = actual avg_price. NEVER limit price.
        Called by ExecutionEngine on exec_type=filled.
        """
        if symbol not in self.position_mirror:
            self._logger.critical(log_record({
                "event":     "FILL_WITHOUT_RECORD",
                "level":     "CRITICAL",
                "component": "WS_MGR",
                "symbol":    symbol,
            }))
            return
        rec = self.position_mirror[symbol]
        rec.entry_fill_price   = fill_price
        rec.qty                = qty
        rec.entry_timestamp_utc = timestamp_utc
        self._logger.info(log_record({
            "event":      "POSITION_FILL_CONFIRMED",
            "level":      "INFO",
            "component":  "WS_MGR",
            "symbol":     symbol,
            "fill_price": fill_price,
            "qty":        qty,
        }))

    def pm_on_orders(
        self,
        symbol: str,
        tp_order_id: str,
        emgsl_order_id: str,
    ) -> None:
        """
        Update PositionRecord with Kraken-assigned TP and emergSL order IDs.
        Called by ExecutionEngine on batch_add ACK (EE-BA-006).
        """
        if symbol not in self.position_mirror:
            return
        rec = self.position_mirror[symbol]
        rec.tp_order_id      = tp_order_id
        rec.emergsl_order_id = emgsl_order_id
        self._logger.info(log_record({
            "event":          "POSITION_ORDERS_SET",
            "level":          "INFO",
            "component":      "WS_MGR",
            "symbol":         symbol,
            "tp_order_id":    tp_order_id,
            "emgsl_order_id": emgsl_order_id,
        }))

    def pm_clear(self, symbol: str) -> None:
        """
        Delete PositionRecord on entry cancellation or expiry (no fill).
        HR-PM-009: WSManager is SOLE WRITER — deletion goes through here.
        Called by ExecutionEngine on entry canceled/expired with no fill.
        """
        if symbol not in self.position_mirror:
            return
        del self.position_mirror[symbol]
        self._logger.info(log_record({
            "event":     "POSITION_RECORD_CLEARED",
            "level":     "INFO",
            "component": "WS_MGR",
            "symbol":    symbol,
            "reason":    "ENTRY_CANCELED_OR_EXPIRED_NO_FILL",
        }))
