"""
DocDCN:     1011002
DocTitle:   WS_Manager
DocVersion: dv1_25
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/tothbot/ws_manager.py
DocDate:    05-03-2026
DocTime:    16:30:00 UTC

============================================================
REVISION HISTORY
============================================================

  dv1_25  05-03-2026  TB00144 STREAM 4 - clean-slate rewrite
                      to the 1011002 dv1_25 coding contract
                      (PATH 2 sharding §4.6 +
                      Subscribe-Pacing Token Bucket §4.7 +
                      AR-070 silent-narrowing prohibition
                      WM-PS-008/HR-WM-028 + silent-pair state
                      machine WM-SHARD-010 + all HR-WM-001
                      through HR-WM-032).

                      Major changes vs dv1_24 deployed code:
                        (a) Public WS now sharded per
                            §4.6 WM-SHARD-001..011. Single
                            self._ws_public connection
                            replaced by self.shards: list of
                            WSShard objects, one per shard,
                            each independent reconnect domain.
                            N_conns = ceil(universe_size /
                            SYMBOLS_PER_CONN_SAFE) computed at
                            startup.
                        (b) New SubscribeTokenBucket process-
                            singleton enforces aggregate
                            outbound subscribe rate (§4.7
                            WM-PACE-001..010). Every
                            subscribe - startup, per-shard
                            reconnect, ad-hoc, ticker-mode-
                            switch, private - passes through
                            the bucket.
                        (c) Per-pair PairState machine
                            (INITIAL -> SUBSCRIBED ->
                            DATA_READY / DATA_PENDING ->
                            DATA_READY) attached to each
                            shard per WM-SHARD-010. Long-
                            DATA_PENDING alert per HR-WM-030.
                        (d) tradeable_universe is the
                            canonical universe set
                            (replaces former
                            monitored_universe). Mutation
                            outside Universe Re-evaluation
                            is PROHIBITED per WM-PS-008 /
                            HR-WM-028. AR-070 silent-
                            narrowing prohibition codified
                            in code.
                        (e) Per-shard recv loop, ping loop,
                            zombie monitor; per-shard
                            reconnect (loss of one shard
                            does not pull down siblings).
                        (f) MAX_RECONNECT_ATTEMPTS=10 per
                            spec WM-RECONNECT-017
                            (origin-investigated; was 20
                            in dv1_24 carrying TB00105
                            OI-020 transient bridge).
                        (g) Pipeline-during-reconnect guard
                            generalized to all-shards-
                            healthy AND private-healthy
                            (live mode) per HR-WM-029.
                        (h) Code-deploy closure of OI-060
                            WS_SUBSCRIBE_RATE_LIMIT_STORM,
                            OI-045 (USDT inclusion), OI-046
                            (Top-N retired), OI-062 (PATH 2
                            implementation), and
                            FINDING-TB00135-001 pending
                            STREAM 5 deploy + observation.

                      DC-PREFLIGHT-006 Q1-Q5 pre-deploy
                      analysis documented in TB00144
                      Claude Code session log per HR-WM-030.

                      RR-SCOPE-009 four-question
                      investigation documented in TB00144
                      session log: rewrite is constraint-
                      additive; sole value change
                      (MAX_RECONNECT_ATTEMPTS 20 -> 10) is
                      spec-conforming with loss-prevention
                      preserved by per-shard reconnect
                      independence + emergSL resting +
                      systemd StartLimit (1011013 dv1_6
                      VD-SYS-008).

                      Governed by 1011002 dv1_25.

  dv1_24  04-25-2026  D-03 USDT inclusion + D-08 Top-N cap
                      full retirement. Closes OI-045
                      (USDT exclusion), OI-046 (Top-N cap
                      retired), and FINDING-TB00135-001
                      (D-03 + D-08 AA=10 cascade) at code
                      deploy. Governed by 1011002 dv1_23.
                      Six-site fix per RR-SCOPE-007 self-
                      catch at TB00140 STREAM 1 open.

  dv1_23  04-23-2026  OI-041 DEFECT-WM-REST-SIGN-001 fix.
                      _sign_rest_request HMAC-SHA512 helper
                      added; all three private REST sites
                      attach API-Sign per WM-REST-SIGN-001
                      through WM-REST-SIGN-003. Governed by
                      1011002 dv1_22.

  dv1_21  04-23-2026  OI-028 fix. WM-RECONNECT-019 paper-
                      mode reconnect skips private steps.

  dv1_19  04-18-2026  DEFECT-WM-RECONNECT-001 fix.
                      WM-RECONNECT-016/017/018 - local
                      catch of transient WS disconnects;
                      single-cycle reconnect via
                      asyncio.Event; FATAL guard via
                      _fatal_reconnect_failure flag.

  dv1_18  04-14-2026  WM-RUN-001 ExceptionGroup sub-
                      exception extraction; WM-WARMUP-004
                      regime startup seed.

  dv1_15  04-13-2026  WM-LIQ-001..010 24h Liquidity Cache.

  dv1_14  04-13-2026  DEFECT-SS-004 / OI-NEW-001.
                      ohlc(60) WS subscription removed;
                      _warm_up_all_pairs() trigger added.

  dv1_13  04-11-2026  DEFECT-WM-OHLC-001 ohlc symbol-list
                      fix.

============================================================
TothBot's sole interface to all Kraken WebSocket v2
connections. PATH 2 multi-connection batched-subscribe
(0511001 dv1_4 §17 ADR; 1011002 dv1_25 §4.6) plus
Subscribe-Pacing Token Bucket (§4.7) + AR-070 silent-
narrowing prohibition (§9.1) + silent-pair state machine.

PAPER TRADING ONLY. NO REAL MONEY. Paper-mode mirrors live
public-side processing identically per HR-WM-026; private-
side calls are gated and simulated locally per HR-WM-022/023.
============================================================
"""

from __future__ import annotations

import asyncio
import base64
import dataclasses
import hashlib
import hmac
import math
import os
import time
import traceback
import typing as _t
import urllib.parse
from datetime import datetime, timedelta, timezone
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from enum import Enum

import aiohttp
import orjson
from websockets.asyncio.client import connect as _ws_connect_factory


# ============================================================
# Module constants - no magic numbers (DC-4 / BP-LIB / spec)
# ============================================================

# WS endpoints (0411001 WS-EP-001 / WS-EP-002).
# Note: 0411001 dv1_11 WS-EP-002 declares wss://ws-l3.kraken.com/v2 as the
# canonical private endpoint; 1011002 dv1_25 §4.1 uses wss://ws-auth.kraken.com/v2.
# Paper trading does not exercise the private endpoint (HR-WM-022). Constant
# below tracks 1011002 dv1_25 verbatim. Pre-live deploy MUST reconcile this
# with 0411001 wire contract.
PUBLIC_WS_URI: str = "wss://ws.kraken.com/v2"
PRIVATE_WS_URI: str = "wss://ws-auth.kraken.com/v2"
REST_BASE_URL: str = "https://api.kraken.com"
KRAKEN_STATUS_URL: str = (
    "https://status.kraken.com/api/v2/scheduled-maintenances/upcoming.json"
)

# WS connection params (WM-CONN-002 / WS-LIB-002..004).
WS_MAX_SIZE_BYTES: int = 10 * 1024 * 1024
WS_OPEN_TIMEOUT_SEC: int = 10

# Application JSON ping (WM-CONN-005 / WS-PING-001..003).
PING_INTERVAL_SEC: int = 30
PING_TIMEOUT_SEC: int = 10

# Zombie detection (WM-ZOM-003 / WS-ZOM-003).
ZOMBIE_THRESHOLD_SEC: int = 90

# Reconnect schedule (WM-RECONNECT-002 / WM-RECONNECT-017).
MAX_RECONNECT_ATTEMPTS: int = 10
RECONNECT_BACKOFF_BASE_SEC: float = 1.0
RECONNECT_BACKOFF_CAP_SEC: float = 30.0
RECONNECT_BACKOFF_FACTOR: float = 2.0
RECONNECT_STALE_CACHE_SEC: int = 15 * 60  # WM-RECONNECT-014: 15 min

# REST stagger (AR-036 / REST-OHLC-007).
REST_PAIR_STAGGER_SEC: float = 1.1

# REST timeouts (BP-HTTP-002 / REST-HTTP-002).
REST_TIMEOUT_TOTAL_SEC: int = 10
REST_TIMEOUT_CONNECT_SEC: int = 5
REST_TIMEOUT_SOCK_READ_SEC: int = 8
REST_CONNECTOR_LIMIT: int = 10
REST_CONNECTOR_LIMIT_PER_HOST: int = 5

# Sharding starting values (1011002 §4.6 WM-SHARD-001 / 0511001 §17.6;
# CIATS-owned per TB00000 §9.16).
SYMBOLS_PER_CONN_SAFE: int = 500

# Subscribe-pacing bucket starting values (§4.7 WM-PACE-002; CIATS-owned).
SUBSCRIBE_RATE_PER_SEC: float = 10.0
SUBSCRIBE_BURST_CAPACITY: float = 20.0

# Silent-pair state machine (WM-SHARD-010; CIATS-owned starting value).
T_SILENT_SEC: float = 60.0
DATA_PENDING_LONG_ALERT_SEC: float = 3600.0  # HR-WM-030 long-pending alert.

# Universe filter (D-03 LOCKED + D-04 LOCKED + D-08 LOCKED).
ALLOWED_QUOTE_CURRENCIES: tuple = ("USD", "USDC", "USDT")
MIN_VOLUME_USD_DAILY_DEFAULT: Decimal = Decimal("500000")
REGIME_ANCHOR: str = "BTC/USD"

# Trading params (CIATS-owned starting values; sourced from config snapshot).
MAX_CONCURRENT: int = 20
TRADEABLE_PCT: Decimal = Decimal("0.50")
PER_TRADE_PCT: Decimal = Decimal("0.05")
MAE_MULT: Decimal = Decimal("1.5")
EMERGENCY_SL_MULT: Decimal = Decimal("3.0")
SESSION_PAUSE_DRAWDOWN: Decimal = Decimal("0.05")
FULL_HALT_DRAWDOWN: Decimal = Decimal("0.10")
ENTRY_GTD_SECONDS: int = 30
DEADLINE_OFFSET_SEC: int = 5
LIQUIDITY_REFRESH_HOURS: int = 4
PAPER_STARTING_BALANCE_DEFAULT: Decimal = Decimal("1000")  # D-05 LOCKED.
CANCEL_TIMEOUT_FALLBACK_SEC: float = 5.0

# Sacred R:R (HARDCODED - never CIATS-owned).
NET_RR_RATIO: Decimal = Decimal("1.5")
FEE_MAKER_PCT: Decimal = Decimal("0.0016")
FEE_TAKER_PCT: Decimal = Decimal("0.0026")


# ============================================================
# Logger shim (Logger module is 0511006 / 1011007; this module
# emits structured records via a callable injected at __init__)
# ============================================================


def _ts() -> str:
    """UTC ISO 8601 with microseconds (BP-ENV-005)."""
    return datetime.now(timezone.utc).isoformat()


def _json_default(obj: _t.Any) -> _t.Any:
    """orjson Decimal serializer (BP-JSON-005)."""
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


# ============================================================
# Data classes
# ============================================================


@dataclasses.dataclass
class OHLCCandle:
    """Closed candle data."""
    interval_begin: str
    open: Decimal
    high: Decimal
    low: Decimal
    close: Decimal
    volume: Decimal
    interval: int  # 5 or 60


@dataclasses.dataclass
class PositionRecord:
    """Position Mirror entry - WS Manager is sole writer (0511005)."""
    symbol: str
    qty: Decimal
    entry_price: Decimal
    cl_ord_id: str
    tp_order_id: _t.Optional[str] = None
    emergsl_order_id: _t.Optional[str] = None
    tp_price: _t.Optional[Decimal] = None
    emergsl_price: _t.Optional[Decimal] = None
    opened_at_monotonic: float = dataclasses.field(default_factory=time.monotonic)
    hold_candle_count: int = 0


class PairState(Enum):
    """Per-pair runtime state per shard (WM-SHARD-010 / WM-PS-007)."""
    INITIAL = "INITIAL"
    SUBSCRIBED = "SUBSCRIBED"
    DATA_PENDING = "DATA_PENDING"
    DATA_READY = "DATA_READY"


@dataclasses.dataclass
class WSShard:
    """Public WS shard (one per N_conns; WM-SHARD-003)."""
    shard_index: int
    connection: _t.Any  # websockets connection
    connection_id: _t.Optional[int] = None
    symbols: _t.List[str] = dataclasses.field(default_factory=list)
    last_real_data_time: float = dataclasses.field(default_factory=time.monotonic)
    last_pong_time: float = dataclasses.field(default_factory=time.monotonic)
    pair_states: _t.Dict[str, PairState] = dataclasses.field(default_factory=dict)
    silent_timer_tasks: _t.Dict[str, asyncio.Task] = dataclasses.field(
        default_factory=dict
    )
    data_pending_at: _t.Dict[str, float] = dataclasses.field(default_factory=dict)
    in_reconnect: bool = False


# ============================================================
# Subscribe Pacing Token Bucket (§4.7 WM-PACE-001..010)
# ============================================================


class SubscribeTokenBucket:
    """Process-singleton outbound-subscribe rate-limiter.

    Every outbound subscribe - startup, per-shard reconnect, ad-hoc,
    private - passes through this single bucket per WM-PACE-004 /
    HR-WM-032. There is no bypass. Refill rate and burst capacity
    are CIATS-owned starting values per WM-PACE-002.
    """

    def __init__(
        self,
        rate: float,
        burst: float,
        log_callable: _t.Optional[_t.Callable[..., None]] = None,
    ) -> None:
        if rate <= 0:
            raise ValueError("SubscribeTokenBucket: rate must be > 0")
        if burst <= 0:
            raise ValueError("SubscribeTokenBucket: burst must be > 0")
        self.rate: float = float(rate)
        self.capacity: float = float(burst)
        self.tokens: float = float(burst)
        self.last_refill: float = time.monotonic()
        self.lock: asyncio.Lock = asyncio.Lock()
        self._log: _t.Optional[_t.Callable[..., None]] = log_callable

    async def acquire(
        self,
        channel: str = "",
        symbol: str = "",
    ) -> None:
        """Acquire one token. Blocks (await) until a token is available."""
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            self.tokens = min(
                self.capacity,
                self.tokens + elapsed * self.rate,
            )
            self.last_refill = now
            if self.tokens < 1:
                wait = (1 - self.tokens) / self.rate
                wait_ms = wait * 1000.0
                if self._log is not None:
                    self._log(
                        "SUBSCRIBE_PACE_WAIT",
                        level="DEBUG",
                        wait_ms=round(wait_ms, 2),
                        tokens_remaining=round(self.tokens, 4),
                        channel=channel,
                        symbol=symbol,
                    )
                await asyncio.sleep(wait)
                self.tokens = 0.0
            else:
                self.tokens -= 1.0


# ============================================================
# WS Manager
# ============================================================


class WSManager:
    """TothBot V2 WS Manager (1011002 dv1_25).

    Sole interface to Kraken WS v2. PATH 2 multi-connection sharded
    public WS + single private WS (live only). Token bucket bounds
    aggregate outbound subscribe rate. Anti-AR-070 universe-mutation
    guard. Per-shard reconnect domains.
    """

    # ----------------------------------------------------------
    # Initialization
    # ----------------------------------------------------------

    def __init__(
        self,
        config: _t.Optional[dict] = None,
        log_callable: _t.Optional[_t.Callable[..., None]] = None,
        signal_pipeline_fn: _t.Optional[_t.Callable[..., _t.Any]] = None,
        execution_engine_fn: _t.Optional[_t.Callable[..., _t.Any]] = None,
        exit_controller_fn: _t.Optional[_t.Callable[..., _t.Any]] = None,
        regime_engine_fn: _t.Optional[_t.Callable[..., _t.Any]] = None,
        ciats_outcome_bus_fn: _t.Optional[_t.Callable[..., _t.Any]] = None,
        alert_fn: _t.Optional[_t.Callable[..., _t.Any]] = None,
        sd_notify_ready_fn: _t.Optional[_t.Callable[[], None]] = None,
    ) -> None:
        self._cfg: dict = dict(config or {})
        self._log: _t.Callable[..., None] = log_callable or self._default_log
        self._signal_pipeline_fn = signal_pipeline_fn
        self._execution_engine_fn = execution_engine_fn
        self._exit_controller_fn = exit_controller_fn
        self._regime_engine_fn = regime_engine_fn
        self._ciats_outcome_bus_fn = ciats_outcome_bus_fn
        self._alert_fn = alert_fn or (lambda *a, **kw: None)
        self._sd_notify_ready_fn = sd_notify_ready_fn

        # HR-WM-021: paper_mode read once at __init__; cannot change at runtime.
        self.paper_mode: bool = bool(self._cfg.get("paper_trading_mode", False))

        # Subscribe pacing (§4.7 WM-PACE-001).
        self.subscribe_bucket: SubscribeTokenBucket = SubscribeTokenBucket(
            rate=float(self._cfg.get(
                "subscribe_rate_per_sec", SUBSCRIBE_RATE_PER_SEC
            )),
            burst=float(self._cfg.get(
                "subscribe_burst_capacity", SUBSCRIBE_BURST_CAPACITY
            )),
            log_callable=self._log,
        )

        # Sharding (§4.6 WM-SHARD-001..003).
        self.symbols_per_conn_safe: int = int(self._cfg.get(
            "symbols_per_conn_safe", SYMBOLS_PER_CONN_SAFE
        ))
        self.shards: _t.List[WSShard] = []
        self.pair_to_shard_index: _t.Dict[str, int] = {}

        # Private WS (live only; HR-WM-022 paper guard).
        self._ws_private: _t.Any = None
        self._private_connection_id: _t.Optional[int] = None
        self._private_in_reconnect: bool = False
        self._last_real_data_time_private: float = time.monotonic()
        self._last_pong_time_private: float = time.monotonic()

        # Universe state (WM-PS-008 / HR-WM-028 - sole authoritative set).
        self.tradeable_universe: _t.Set[str] = set()
        self.regime_anchor: str = REGIME_ANCHOR

        # Per-pair caches (instrument channel; AR-028).
        self.pair_cache: _t.Dict[str, _t.Dict[str, _t.Any]] = {}

        # Pair-name maps (WM-LIQ-003 / WM-LIQ-011).
        self._wsname_to_classic: _t.Dict[str, str] = {}
        self._classic_to_wsname: _t.Dict[str, str] = {}

        # 24h liquidity (WM-LIQ-001).
        self.liquidity_24h: _t.Dict[str, Decimal] = {}

        # Indicator caches (WM-PS / Section 9).
        self.atr_14: _t.Dict[str, Decimal] = {}
        self.prev_tr: _t.Dict[str, Decimal] = {}
        self.rsi_14_avg_gain: _t.Dict[str, Decimal] = {}
        self.rsi_14_avg_loss: _t.Dict[str, Decimal] = {}
        self.ema_9: _t.Dict[str, Decimal] = {}
        self.ema_21: _t.Dict[str, Decimal] = {}
        self.volume_ma_20: _t.Dict[str, Decimal] = {}
        self.volume_history: _t.Dict[str, _t.List[Decimal]] = {}
        self.last_interval_begin: _t.Dict[str, str] = {}
        self.last_complete_candle: _t.Dict[str, OHLCCandle] = {}

        # HTF cache (WM-HTF / Section 9.3).
        self.htf_ema_20: _t.Dict[str, Decimal] = {}
        self.htf_ema_50: _t.Dict[str, Decimal] = {}
        self.last_interval_begin_60: _t.Dict[str, str] = {}

        # Warm-up state (WM-WARMUP / AR-068).
        self.warm_up_state: _t.Dict[str, str] = {}
        self.previous_close_5m: _t.Dict[str, Decimal] = {}

        # Bid cache (WM-EC-003 - drives MAE + drawdown).
        self.latest_bid: _t.Dict[str, Decimal] = {}

        # Selection Controller state (WM-SC-001..003 - preserved on reconnect).
        self.exit_cooldown_log: _t.Dict[str, float] = {}
        self.consecutive_loss_count: _t.Dict[str, int] = {}

        # Position + drawdown state (WM-BAL / WM-DD; preserved on reconnect).
        self.spot_usd_balance: Decimal = Decimal("0")
        self.portfolio_baseline_USD: _t.Optional[Decimal] = None
        self.system_state: str = "NORMAL"  # NORMAL | SESSION_PAUSE | FULL_HALT
        self.position_mirror: _t.Dict[str, PositionRecord] = {}
        self.pending_orders: _t.Dict[str, Decimal] = {}

        # Sequence tracking (private; live only).
        self.executions_last_seq: int = 0
        self.balances_last_seq: int = 0
        self.maxratecount: _t.Optional[int] = None
        self.rate_counter_by_pair: _t.Dict[str, Decimal] = {}

        # Order tracking.
        self._req_id_counter: int = 0
        self._req_id_registry: _t.Dict[int, dict] = {}

        # Token (live only).
        self._ws_token: _t.Optional[str] = None

        # HTTP session (one per process lifetime; BP-HTTP-001..006).
        self._http: _t.Optional[aiohttp.ClientSession] = None

        # Coordination guards.
        self._reconnect_done_events: _t.Dict[int, asyncio.Event] = {}
        self._private_reconnect_done: asyncio.Event = asyncio.Event()
        self._private_reconnect_done.set()
        self._fatal_reconnect_failure: bool = False

        # Dispatch tables.
        self._channel_dispatch: _t.Dict[str, _t.Callable[..., _t.Any]] = {}
        self._exec_dispatch: _t.Dict[str, _t.Callable[..., _t.Any]] = {}
        self._setup_dispatch_tables()

        # Background task handles.
        self._liquidity_refresh_task: _t.Optional[asyncio.Task] = None
        self._daily_regime_task: _t.Optional[asyncio.Task] = None
        self._shard_recv_tasks: _t.List[asyncio.Task] = []

    # ----------------------------------------------------------
    # Default logger fallback
    # ----------------------------------------------------------

    def _default_log(self, event: str, **fields: _t.Any) -> None:
        """Last-resort logger - Logger module is the production sink."""
        record = {"ts": _ts(), "component": "WS_MGR", "event": event, **fields}
        try:
            line = orjson.dumps(record, default=_json_default).decode()
        except Exception:
            line = f'{{"event": "{event}"}}'
        # NOTE: production deployment installs Logger callable; this is
        # a debug fallback for unit-test / standalone use.
        # No stdout in production hot path (BP-LOG-001).
        if os.environ.get("WS_MANAGER_DEFAULT_LOG_TO_STDERR") == "1":
            import sys
            print(line, file=sys.stderr)

    # ----------------------------------------------------------
    # Dispatch table setup (AR-015 / WM-DISP)
    # ----------------------------------------------------------

    def _setup_dispatch_tables(self) -> None:
        self._channel_dispatch = {
            "ohlc": self._handle_ohlc,
            "ticker": self._handle_ticker,
            "instrument": self._handle_instrument,
            "status": self._handle_status,
            "executions": self._handle_executions,
            "balances": self._handle_balances,
        }
        self._exec_dispatch = {
            "pending_new": self._handle_pending_new,
            "new": self._handle_new,
            "trade": self._handle_trade,
            "filled": self._handle_filled,
            "iceberg_refill": self._handle_iceberg_refill,
            "canceled": self._handle_canceled,
            "expired": self._handle_expired,
            "amended": self._handle_amended,
            "restated": self._handle_restated,
            "status": self._handle_exec_status,
        }

    # ----------------------------------------------------------
    # Helpers
    # ----------------------------------------------------------

    def _next_req_id(self) -> int:
        self._req_id_counter += 1
        return self._req_id_counter

    def _make_http_session(self) -> aiohttp.ClientSession:
        """Build the shared aiohttp session (BP-HTTP / REST-HTTP-002)."""
        timeout = aiohttp.ClientTimeout(
            total=REST_TIMEOUT_TOTAL_SEC,
            connect=REST_TIMEOUT_CONNECT_SEC,
            sock_read=REST_TIMEOUT_SOCK_READ_SEC,
        )
        connector = aiohttp.TCPConnector(
            limit=REST_CONNECTOR_LIMIT,
            limit_per_host=REST_CONNECTOR_LIMIT_PER_HOST,
            force_close=False,
        )
        return aiohttp.ClientSession(timeout=timeout, connector=connector)

    @staticmethod
    def _normalize_classic_key(key: str) -> str:
        """Normalize Kraken classic name (XXBTZUSD -> XBTUSD)."""
        if key.startswith("X") and "Z" in key[1:]:
            base = key[1:].split("Z", 1)[0]
            quote = key.split("Z", 1)[1] if "Z" in key else ""
            if base and quote:
                return f"{base}{quote}"
        return key

    # ----------------------------------------------------------
    # REST signing helper (WM-REST-SIGN-001..003 / HR-WM-023)
    # ----------------------------------------------------------

    def _sign_rest_request(
        self,
        url_path: str,
        data: dict,
        api_secret: str,
    ) -> _t.Tuple[str, str]:
        """Sole REST signing implementation (WM-REST-SIGN-002).

        Returns (post_data_string, api_sign_b64). Caller MUST transmit
        the returned post_data_string verbatim - re-encoding the dict
        breaks the signature (WM-REST-SIGN-003).
        """
        post_data = urllib.parse.urlencode(data)
        nonce = data.get("nonce", "")
        sha256 = hashlib.sha256((str(nonce) + post_data).encode("utf-8")).digest()
        message = url_path.encode("utf-8") + sha256
        secret_bytes = base64.b64decode(api_secret)
        signature = hmac.new(secret_bytes, message, hashlib.sha512).digest()
        return post_data, base64.b64encode(signature).decode()

    # ----------------------------------------------------------
    # REST endpoints (live-only paths gated on paper_mode)
    # ----------------------------------------------------------

    async def _rest_get_ws_token(self) -> str:
        """GetWebSocketsToken - TRADE key signed (WM-TOKEN / WM-REST-SIGN-001)."""
        if self.paper_mode:
            raise RuntimeError("_rest_get_ws_token must not be called in paper mode")
        trade_key = os.environ["KRAKEN_TRADE_API_KEY"]
        trade_secret = os.environ["KRAKEN_TRADE_API_SECRET"]
        url_path = "/0/private/GetWebSocketsToken"
        data = {"nonce": str(int(time.time() * 1000))}
        post_data, signature = self._sign_rest_request(url_path, data, trade_secret)
        assert self._http is not None
        async with self._http.post(
            f"{REST_BASE_URL}{url_path}",
            headers={
                "API-Key": trade_key,
                "API-Sign": signature,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data=post_data,
        ) as resp:
            payload = await resp.json(loads=orjson.loads)
        errors = payload.get("error") or []
        if errors:
            self._log("REST_ERROR", level="CRITICAL",
                      endpoint="GetWebSocketsToken", errors=errors)
            raise RuntimeError(f"GetWebSocketsToken failed: {errors}")
        token = payload["result"]["token"]
        self._log("WS_TOKEN_ACQUIRED", level="INFO")
        return token

    async def _rest_get_ohlc(self, pair: str, interval: int) -> list:
        """Public OHLC fetch (REST-OHLC-001..009)."""
        assert self._http is not None
        kraken_pair = self._wsname_to_classic.get(pair, pair)
        params = {"pair": kraken_pair, "interval": interval}
        async with self._http.get(
            f"{REST_BASE_URL}/0/public/OHLC", params=params
        ) as resp:
            payload = await resp.json(loads=orjson.loads)
        errors = payload.get("error") or []
        if errors:
            self._log("REST_ERROR", level="WARN",
                      endpoint="GetOHLCData", pair=pair, errors=errors)
            return []
        result = payload.get("result", {})
        for k, v in result.items():
            if k == "last":
                continue
            return v
        return []

    async def _rest_get_open_orders(self) -> dict:
        """OpenOrders - TRADE key signed (live only)."""
        if self.paper_mode:
            return {}
        trade_key = os.environ["KRAKEN_TRADE_API_KEY"]
        trade_secret = os.environ["KRAKEN_TRADE_API_SECRET"]
        url_path = "/0/private/OpenOrders"
        data = {"nonce": str(int(time.time() * 1000))}
        post_data, signature = self._sign_rest_request(url_path, data, trade_secret)
        assert self._http is not None
        async with self._http.post(
            f"{REST_BASE_URL}{url_path}",
            headers={
                "API-Key": trade_key,
                "API-Sign": signature,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data=post_data,
        ) as resp:
            payload = await resp.json(loads=orjson.loads)
        errors = payload.get("error") or []
        if errors:
            self._log("REST_ERROR", level="CRITICAL",
                      endpoint="OpenOrders", errors=errors)
            return {}
        return payload.get("result", {})

    async def _rest_get_account_balance(self) -> dict:
        """Balance - TRADE key signed (live only)."""
        if self.paper_mode:
            return {}
        trade_key = os.environ["KRAKEN_TRADE_API_KEY"]
        trade_secret = os.environ["KRAKEN_TRADE_API_SECRET"]
        url_path = "/0/private/Balance"
        data = {"nonce": str(int(time.time() * 1000))}
        post_data, signature = self._sign_rest_request(url_path, data, trade_secret)
        assert self._http is not None
        async with self._http.post(
            f"{REST_BASE_URL}{url_path}",
            headers={
                "API-Key": trade_key,
                "API-Sign": signature,
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data=post_data,
        ) as resp:
            payload = await resp.json(loads=orjson.loads)
        errors = payload.get("error") or []
        if errors:
            self._log("REST_ERROR", level="CRITICAL",
                      endpoint="Balance", errors=errors)
            return {}
        return payload.get("result", {})

    async def _rest_get_ticker(
        self, candidates: _t.List[str]
    ) -> _t.Dict[str, Decimal]:
        """REST Ticker for 24h USD volume (WM-LIQ-002 / REST-TKR-001..010)."""
        if not candidates:
            return {}
        classic_list: _t.List[str] = []
        for ws_name in candidates:
            classic = self._wsname_to_classic.get(ws_name)
            if classic:
                classic_list.append(classic)
        if not classic_list:
            self._log("LIQUIDITY_REFRESH_FAILED", level="WARN",
                      reason="no_classic_mappings")
            return {}
        result: _t.Dict[str, Decimal] = {}
        try:
            assert self._http is not None
            params = {"pair": ",".join(classic_list)}
            async with self._http.get(
                f"{REST_BASE_URL}/0/public/Ticker", params=params
            ) as resp:
                payload = await resp.json(loads=orjson.loads)
            errors = payload.get("error") or []
            if errors:
                self._log("LIQUIDITY_REFRESH_FAILED",
                          level="WARN", errors=errors)
                return {}
            for classic_key, info in payload.get("result", {}).items():
                v = info.get("v") or []
                p = info.get("p") or []
                if len(v) < 2 or len(p) < 2:
                    continue
                try:
                    usd_vol = Decimal(str(v[1])) * Decimal(str(p[1]))
                except Exception:
                    continue
                ws_name = self._classic_to_wsname.get(classic_key)
                if not ws_name:
                    norm = self._normalize_classic_key(classic_key)
                    ws_name = self._classic_to_wsname.get(norm)
                if ws_name:
                    result[ws_name] = usd_vol
                else:
                    self._log("LIQUIDITY_PAIR_NOT_FOUND", level="WARN",
                              pair="", classic_name=classic_key)
        except Exception as e:
            self._log("LIQUIDITY_REFRESH_FAILED",
                      level="WARN", error=str(e))
            return {}
        return result

    async def _rest_build_pair_maps(self) -> None:
        """AssetPairs -> wsname<->classic map (WM-LIQ-011)."""
        try:
            assert self._http is not None
            async with self._http.get(
                f"{REST_BASE_URL}/0/public/AssetPairs"
            ) as resp:
                payload = await resp.json(loads=orjson.loads)
            errors = payload.get("error") or []
            if errors:
                self._log("PAIR_MAP_BUILD_FAILED",
                          level="WARN", reason=str(errors))
                return
            count = 0
            for classic_key, info in payload.get("result", {}).items():
                altname = info.get("altname", "")
                wsname_raw = info.get("wsname", "")
                if not altname or not wsname_raw:
                    continue
                ws_v2_name = (
                    wsname_raw.replace("XBT/", "BTC/").replace("/XBT", "/BTC")
                )
                if ws_v2_name in self.pair_cache:
                    self._wsname_to_classic[ws_v2_name] = altname
                    self._classic_to_wsname[altname] = ws_v2_name
                    norm = self._normalize_classic_key(classic_key)
                    self._classic_to_wsname[norm] = ws_v2_name
                    self._classic_to_wsname[classic_key] = ws_v2_name
                    count += 1
                elif wsname_raw in self.pair_cache:
                    self._wsname_to_classic[wsname_raw] = altname
                    self._classic_to_wsname[altname] = wsname_raw
                    self._classic_to_wsname[classic_key] = wsname_raw
                    count += 1
            self._log("PAIR_MAPS_BUILT", level="INFO", pairs_mapped=count)
        except Exception as e:
            self._log("PAIR_MAP_BUILD_FAILED", level="WARN", reason=str(e))

    # ----------------------------------------------------------
    # WS connection low-level
    # ----------------------------------------------------------

    async def _ws_connect_one(self, uri: str) -> _t.Any:
        """Open a single WS connection per WM-CONN-002 / WS-LIB-002..004."""
        return await _ws_connect_factory(
            uri,
            max_size=WS_MAX_SIZE_BYTES,
            open_timeout=WS_OPEN_TIMEOUT_SEC,
            max_queue=None,
            ping_interval=None,
        )

    async def _send_shard(self, shard: WSShard, payload: dict) -> None:
        """Send a payload to a specific shard, swallowing transient disconnects."""
        if shard.connection is None or shard.in_reconnect:
            return
        try:
            await shard.connection.send(orjson.dumps(payload).decode())
        except Exception as e:
            self._log("RECONNECT_TRIGGERED", level="HIGH",
                      source="_send_shard",
                      error_type=type(e).__name__,
                      shard_index=shard.shard_index)
            asyncio.create_task(
                self._initiate_reconnect_shard(shard.shard_index)
            )

    async def _send_private(self, payload: dict) -> None:
        if self.paper_mode or self._ws_private is None or self._private_in_reconnect:
            return
        try:
            await self._ws_private.send(orjson.dumps(payload).decode())
        except Exception as e:
            self._log("RECONNECT_TRIGGERED", level="HIGH",
                      source="_send_private",
                      error_type=type(e).__name__)
            asyncio.create_task(self._initiate_reconnect_private())

    # ----------------------------------------------------------
    # Subscribe primitives - every call passes through bucket
    # ----------------------------------------------------------

    async def _subscribe_on_shard(
        self,
        shard: WSShard,
        channel: str,
        symbols: _t.Optional[_t.List[str]] = None,
        extra: _t.Optional[dict] = None,
    ) -> None:
        """Send a subscribe to a shard (HR-WM-032: bucket-bound)."""
        token_label = symbols[0] if symbols else channel
        await self.subscribe_bucket.acquire(channel=channel, symbol=token_label)
        params: _t.Dict[str, _t.Any] = {"channel": channel}
        if symbols is not None:
            params["symbol"] = symbols
        if extra:
            params.update(extra)
        payload = {
            "method": "subscribe",
            "params": params,
            "req_id": self._next_req_id(),
        }
        await self._send_shard(shard, payload)

    async def _subscribe_private(self, channel: str, extra: dict) -> None:
        """Authenticated channel subscribe (live only; bucket-bound)."""
        if self.paper_mode:
            return
        await self.subscribe_bucket.acquire(channel=channel)
        params: _t.Dict[str, _t.Any] = {"channel": channel, **extra}
        payload = {
            "method": "subscribe",
            "params": params,
            "req_id": self._next_req_id(),
        }
        await self._send_private(payload)

    # ----------------------------------------------------------
    # Top-level run() - entry point
    # ----------------------------------------------------------

    async def run(self) -> None:
        """Top-level entry. Performs startup, then runs main loop."""
        self._http = self._make_http_session()
        try:
            await self._startup()
            await self._main_loop()
        except BaseException as exc:
            tb_str = traceback.format_exc()
            sub_msgs: _t.List[str] = []
            try:
                # Python 3.11+: ExceptionGroup is built-in.
                eg_type = BaseExceptionGroup  # type: ignore[name-defined]
                if isinstance(exc, eg_type):
                    for i, sub in enumerate(exc.exceptions):
                        sub_tb = "".join(
                            traceback.format_tb(sub.__traceback__)
                        )
                        sub_msgs.append(
                            f"[{i}] {type(sub).__name__}: {sub} | tb: {sub_tb}"
                        )
            except NameError:
                pass
            self._log(
                "WS_MGR_FATAL", level="CRITICAL",
                error=str(exc),
                traceback=tb_str[-2000:],
                sub_exceptions=sub_msgs or None,
            )
            try:
                self._alert_fn(
                    "CRITICAL",
                    f"WS_MGR_FATAL: {type(exc).__name__}: {exc}",
                )
            except Exception:
                pass
            raise
        finally:
            await self._shutdown()

    async def _shutdown(self) -> None:
        """Best-effort cleanup of connections + http session."""
        for shard in self.shards:
            try:
                if shard.connection is not None:
                    await shard.connection.close()
            except Exception:
                pass
        try:
            if self._ws_private is not None:
                await self._ws_private.close()
        except Exception:
            pass
        if self._http is not None:
            try:
                await self._http.close()
            except Exception:
                pass

    # ----------------------------------------------------------
    # Startup sequence (1011014 + §4.5)
    # ----------------------------------------------------------

    async def _startup(self) -> None:
        """Steps 0..11 from 0511001 dv1_4 §4 / WM-STARTUP-001."""
        self._log("VPS_STARTUP_BEGIN", level="INFO", paper_mode=self.paper_mode)

        # Step 0: Kraken Status maintenance check (AR-038 / VD-STAT).
        await self._check_kraken_maintenance()

        # Step 1: Acquire WS token (live only).
        if not self.paper_mode:
            self._ws_token = await self._rest_get_ws_token()

        # Step 2: Open shard 0 (provisional N_conns=1; full sharding after Step 4).
        first_shard_conn = await self._ws_connect_one(PUBLIC_WS_URI)
        first_shard = WSShard(
            shard_index=0,
            connection=first_shard_conn,
        )
        self.shards = [first_shard]
        self._reconnect_done_events[0] = asyncio.Event()
        self._reconnect_done_events[0].set()
        self._log("WS_CONNECTED", level="INFO",
                  endpoint="public", shard_index=0)

        # Start shard-0 recv loop now so we can receive ACKs.
        first_shard_recv_task = asyncio.create_task(
            self._recv_loop_shard(first_shard),
            name=f"shard_{0}_recv",
        )
        self._shard_recv_tasks = [first_shard_recv_task]

        # Step 3: instrument + Step 5: status - connection-wide on shard 0.
        await self._subscribe_on_shard(
            first_shard, "instrument", extra={"snapshot": True}
        )
        await self._subscribe_on_shard(
            first_shard, "status", extra={"snapshot": True}
        )

        # Wait for instrument snapshot to populate pair_cache.
        await self._wait_for_instrument_snapshot()

        # Step 4: REST AssetPairs map + REST Ticker liquidity filter.
        await self._rest_build_pair_maps()
        all_candidates = self._build_initial_candidates()
        self.liquidity_24h = await self._rest_get_ticker(all_candidates)
        self._log("LIQUIDITY_SEEDED", level="INFO",
                  pairs_count=len(self.liquidity_24h))

        # Build tradeable_universe (D-03/D-04/D-08).
        self._build_tradeable_universe()
        self._log("UNIVERSE_BUILT", level="INFO",
                  pairs=len(self.tradeable_universe))

        # Step 5a: SHARDING.
        await self._compute_and_connect_remaining_shards()

        # Step 6: ohlc(5) per pair, dispatched to assigned shard.
        await self._subscribe_ohlc_5m_all_pairs()

        # Step 7: ticker per pair.
        await self._subscribe_ticker_all_pairs()

        # Step 8: Indicator warm-up + regime startup seed.
        await self._warm_up_all_pairs()

        # Step 9 (live): private WS connect; Step 9b (paper): set balance.
        if self.paper_mode:
            self.spot_usd_balance = Decimal(str(
                self._cfg.get("paper_starting_balance",
                              PAPER_STARTING_BALANCE_DEFAULT)
            ))
            self.portfolio_baseline_USD = self.spot_usd_balance
            self._log("PAPER_BALANCE_SET", level="INFO",
                      balance=self.spot_usd_balance)
        else:
            await self._connect_private_and_subscribe()

        # Liquidity refresh task (both modes).
        self._liquidity_refresh_task = asyncio.create_task(
            self._liquidity_refresh_loop(),
            name="liquidity_refresh",
        )

        # sd_notify READY=1 only after all subscribes acked + warm-up done.
        if self._sd_notify_ready_fn is not None:
            try:
                self._sd_notify_ready_fn()
            except Exception:
                pass
        self._log("VPS_STARTUP_COMPLETE", level="INFO")
        self._log("SYSTEM_OPERATIONAL", level="INFO")

    async def _check_kraken_maintenance(self) -> None:
        try:
            assert self._http is not None
            async with self._http.get(KRAKEN_STATUS_URL) as resp:
                data = await resp.json(loads=orjson.loads)
            maintenances = data.get("scheduled_maintenances", [])
            now = datetime.now(timezone.utc)
            warn_threshold = now + timedelta(hours=2)
            clean = True
            for m in maintenances:
                start_str = m.get("scheduled_for", "")
                if not start_str:
                    continue
                start_time = datetime.fromisoformat(
                    start_str.replace("Z", "+00:00"))
                if start_time <= warn_threshold:
                    clean = False
                    self._log(
                        "KRAKEN_MAINTENANCE_SCHEDULED",
                        level="CRITICAL",
                        within_hours=round(
                            (start_time - now).total_seconds() / 3600, 1),
                        description=m.get("name", ""),
                    )
                    try:
                        self._alert_fn(
                            "CRITICAL",
                            f"Kraken maintenance in <2 hrs: "
                            f"{m.get('name','')} at {start_time}",
                        )
                    except Exception:
                        pass
            if clean:
                self._log("KRAKEN_STATUS_CLEAN", level="INFO")
        except Exception as e:
            self._log("KRAKEN_STATUS_CHECK_FAILED",
                      level="INFO", error=str(e))

    # ----------------------------------------------------------
    # Universe building (WM-PS-001..008 / HR-WM-027 / D-03 D-04 D-08)
    # ----------------------------------------------------------

    async def _wait_for_instrument_snapshot(
        self, timeout_sec: float = 30.0
    ) -> None:
        deadline = time.monotonic() + timeout_sec
        while time.monotonic() < deadline:
            if self.pair_cache:
                return
            await asyncio.sleep(0.1)
        self._log("INSTRUMENT_SNAPSHOT_TIMEOUT",
                  level="CRITICAL", timeout_sec=timeout_sec)
        raise RuntimeError("Instrument snapshot did not arrive within timeout")

    def _build_initial_candidates(self) -> _t.List[str]:
        """All online USD/USDC/USDT pairs in pair_cache (no volume filter yet)."""
        candidates: _t.List[str] = []
        for symbol, info in self.pair_cache.items():
            if info.get("status") != "online":
                continue
            quote = info.get("quote_currency", "")
            if quote not in ALLOWED_QUOTE_CURRENCIES:
                continue
            candidates.append(symbol)
        return candidates

    def _build_tradeable_universe(self) -> None:
        """Apply D-03 + D-04 (volume) filter to produce tradeable_universe.

        WM-PS-002/003/006 + HR-WM-027 + WM-PS-008. AR-070 silent-narrowing
        prohibition: this is the ONLY code path that mutates
        tradeable_universe; subsequent runtime code reads but never mutates.
        """
        min_volume = Decimal(str(self._cfg.get(
            "min_volume_usd_daily", MIN_VOLUME_USD_DAILY_DEFAULT
        )))
        new_universe: _t.Set[str] = set()
        for symbol, info in self.pair_cache.items():
            if info.get("status") != "online":
                continue
            quote = info.get("quote_currency", "")
            if quote not in ALLOWED_QUOTE_CURRENCIES:
                continue
            usd_vol = self.liquidity_24h.get(symbol, Decimal("0"))
            if usd_vol >= min_volume:
                new_universe.add(symbol)
        # AR-074: regime_anchor (BTC/USD) ALWAYS included regardless of volume.
        if (REGIME_ANCHOR in self.pair_cache and
                self.pair_cache[REGIME_ANCHOR].get("status") == "online"):
            new_universe.add(REGIME_ANCHOR)
        # D-08 LOCKED: NO Top-N cap.
        self.tradeable_universe = new_universe
        for symbol in self.tradeable_universe:
            self.warm_up_state.setdefault(symbol, "WARM_UP")

    # ----------------------------------------------------------
    # Sharding (§4.6 WM-SHARD-001..011)
    # ----------------------------------------------------------

    async def _compute_and_connect_remaining_shards(self) -> None:
        """WM-SHARD-001..004: compute N_conns, parallel-connect."""
        universe_size = len(self.tradeable_universe)
        assert self.symbols_per_conn_safe > 0
        if universe_size == 0:
            self._log("EMPTY_TRADEABLE_UNIVERSE", level="CRITICAL")
            raise RuntimeError("tradeable_universe is empty after filter")
        n_conns = max(1, math.ceil(universe_size / self.symbols_per_conn_safe))

        sorted_universe = sorted(self.tradeable_universe)
        self.pair_to_shard_index = {
            pair: i % n_conns for i, pair in enumerate(sorted_universe)
        }

        # Shard 0 was opened in Step 2. Add its symbol partition now.
        self.shards[0].symbols = [
            p for p in sorted_universe
            if self.pair_to_shard_index[p] == 0
        ]
        for p in self.shards[0].symbols:
            self.shards[0].pair_states[p] = PairState.INITIAL

        if n_conns == 1:
            return

        async def _connect_extra(idx: int) -> WSShard:
            conn = await self._ws_connect_one(PUBLIC_WS_URI)
            self._log("WS_CONNECTED", level="INFO",
                      endpoint="public", shard_index=idx)
            shard = WSShard(shard_index=idx, connection=conn)
            shard.symbols = [
                p for p in sorted_universe
                if self.pair_to_shard_index[p] == idx
            ]
            for p in shard.symbols:
                shard.pair_states[p] = PairState.INITIAL
            self._reconnect_done_events[idx] = asyncio.Event()
            self._reconnect_done_events[idx].set()
            return shard

        new_shards = await asyncio.gather(
            *[_connect_extra(i) for i in range(1, n_conns)],
            return_exceptions=False,
        )
        self.shards.extend(new_shards)

    # ----------------------------------------------------------
    # ohlc + ticker subscribe (per-pair, dispatch to shard)
    # ----------------------------------------------------------

    async def _subscribe_ohlc_5m_all_pairs(self) -> None:
        """ohlc(5) per pair -> assigned shard. Bucket-bound. WM-PACE-004."""
        for symbol in sorted(self.tradeable_universe):
            shard = self.shards[self.pair_to_shard_index[symbol]]
            await self._subscribe_on_shard(
                shard,
                "ohlc",
                symbols=[symbol],
                extra={"interval": 5, "snapshot": True},
            )
            shard.pair_states[symbol] = PairState.SUBSCRIBED
            self._arm_silent_timer(shard, symbol)

    async def _subscribe_ticker_all_pairs(self) -> None:
        """ticker per pair -> assigned shard. event_trigger per state."""
        for symbol in sorted(self.tradeable_universe):
            shard = self.shards[self.pair_to_shard_index[symbol]]
            trigger = self._ticker_trigger_for(symbol)
            await self._subscribe_on_shard(
                shard,
                "ticker",
                symbols=[symbol],
                extra={"event_trigger": trigger, "snapshot": True},
            )

    def _ticker_trigger_for(self, symbol: str) -> str:
        """bbo for pairs with positions; trades otherwise (AR-034)."""
        if symbol in self.position_mirror:
            return "bbo"
        return "trades"

    async def _update_ticker_event_trigger(self, symbol: str, trigger: str) -> None:
        """Re-subscribe ticker for a pair on position state change (AR-034)."""
        shard_idx = self.pair_to_shard_index.get(symbol)
        if shard_idx is None:
            return
        shard = self.shards[shard_idx]
        await self._subscribe_on_shard(
            shard,
            "ticker",
            symbols=[symbol],
            extra={"event_trigger": trigger, "snapshot": True},
        )

    # ----------------------------------------------------------
    # Silent-pair state machine (WM-SHARD-010 / HR-WM-030)
    # ----------------------------------------------------------

    def _arm_silent_timer(self, shard: WSShard, symbol: str) -> None:
        """Start T_silent timer for one pair; cancel any prior timer."""
        prev = shard.silent_timer_tasks.get(symbol)
        if prev is not None and not prev.done():
            prev.cancel()
        shard.silent_timer_tasks[symbol] = asyncio.create_task(
            self._silent_timer_task(shard, symbol),
            name=f"silent_{shard.shard_index}_{symbol}",
        )

    async def _silent_timer_task(self, shard: WSShard, symbol: str) -> None:
        try:
            await asyncio.sleep(T_SILENT_SEC)
        except asyncio.CancelledError:
            return
        if shard.pair_states.get(symbol) == PairState.SUBSCRIBED:
            shard.pair_states[symbol] = PairState.DATA_PENDING
            shard.data_pending_at[symbol] = time.monotonic()
            self._log("PAIR_DATA_PENDING", level="INFO",
                      pair=symbol, shard=shard.shard_index)

    def _on_pair_data_received(self, shard: WSShard, symbol: str) -> None:
        """Handler-side state transition on first/subsequent data."""
        prev = shard.pair_states.get(symbol)
        shard.pair_states[symbol] = PairState.DATA_READY
        if prev == PairState.DATA_PENDING:
            data_pending_age = (
                time.monotonic() - shard.data_pending_at.pop(symbol, 0.0)
            )
            self._log("PAIR_DATA_READY_RECOVERED", level="INFO",
                      pair=symbol, shard=shard.shard_index,
                      data_pending_age_sec=round(data_pending_age, 2))
        timer = shard.silent_timer_tasks.pop(symbol, None)
        if timer is not None and not timer.done():
            timer.cancel()

    # ----------------------------------------------------------
    # Indicator warm-up (WM-WARMUP-001..004 / AR-044 / AR-068)
    # ----------------------------------------------------------

    async def _warm_up_all_pairs(self) -> None:
        symbols = sorted(self.tradeable_universe)
        if not symbols:
            self._log("WARMUP_REST_NO_PAIRS", level="WARN")
            return
        self._log("WARMUP_REST_STARTED", level="INFO", pairs=len(symbols))

        async def _seed(sym: str) -> bool:
            try:
                await self.seed_indicators_from_rest(sym)
                return True
            except Exception as e:
                self._log("WARMUP_REST_FAILED", level="WARN",
                          pair=sym, error=str(e))
                return False

        results = await asyncio.gather(
            *[_seed(s) for s in symbols], return_exceptions=True
        )
        ready_count = sum(1 for r in results if r is True)
        self._log("WARMUP_REST_COMPLETE", level="INFO",
                  ready_pairs=ready_count, total_pairs=len(symbols))

        # WM-WARMUP-004: regime startup seed BEFORE SYSTEM_OPERATIONAL.
        if self._regime_engine_fn is not None:
            await self._trigger_daily_regime_refresh()

    async def seed_indicators_from_rest(self, symbol: str) -> None:
        """Seed all 5 SSS indicators + HTF EMA from REST OHLC."""
        candles_5m = await self._rest_get_ohlc(symbol, interval=5)
        if not candles_5m:
            raise RuntimeError(f"no 5m candles returned for {symbol}")
        self._seed_5m_indicators(symbol, candles_5m[:-1])
        await asyncio.sleep(REST_PAIR_STAGGER_SEC)
        candles_60m = await self._rest_get_ohlc(symbol, interval=60)
        if candles_60m:
            self._seed_htf_ema(symbol, candles_60m[:-1])
        if (self.atr_14.get(symbol) is not None
                and self.rsi_14_avg_gain.get(symbol) is not None
                and self.ema_9.get(symbol) is not None
                and self.ema_21.get(symbol) is not None
                and self.volume_ma_20.get(symbol) is not None):
            self.warm_up_state[symbol] = "READY"
            self._log("PAIR_READY", level="INFO", pair=symbol)

    def _seed_5m_indicators(self, symbol: str, candles: list) -> None:
        """Seed ATR, RSI(Wilder), EMA9, EMA21, VolMA20 from 5m candles."""
        if len(candles) < 21:
            return
        closes = [Decimal(str(c[4])) for c in candles]
        highs = [Decimal(str(c[2])) for c in candles]
        lows = [Decimal(str(c[3])) for c in candles]
        vols = [Decimal(str(c[6])) for c in candles]

        # ATR(14) seed.
        trs: _t.List[Decimal] = []
        for i in range(1, len(candles)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            trs.append(tr)
        if len(trs) >= 14:
            atr = sum(trs[:14], Decimal("0")) / Decimal("14")
            for tr in trs[14:]:
                atr = (atr * Decimal("13") + tr) / Decimal("14")
            self.atr_14[symbol] = atr
            self.prev_tr[symbol] = trs[-1] if trs else Decimal("0")

        # RSI(14) Wilder SMMA seed (HR-WM-018).
        gains: _t.List[Decimal] = []
        losses: _t.List[Decimal] = []
        for i in range(1, len(closes)):
            d = closes[i] - closes[i - 1]
            if d > 0:
                gains.append(d)
                losses.append(Decimal("0"))
            else:
                gains.append(Decimal("0"))
                losses.append(-d)
        if len(gains) >= 14:
            avg_g = sum(gains[:14], Decimal("0")) / Decimal("14")
            avg_l = sum(losses[:14], Decimal("0")) / Decimal("14")
            for g, l in zip(gains[14:], losses[14:]):
                avg_g = (avg_g * Decimal("13") + g) / Decimal("14")
                avg_l = (avg_l * Decimal("13") + l) / Decimal("14")
            self.rsi_14_avg_gain[symbol] = avg_g
            self.rsi_14_avg_loss[symbol] = avg_l

        # EMA(9): SMA(9) seed, alpha=2/10.
        if len(closes) >= 9:
            ema = sum(closes[:9], Decimal("0")) / Decimal("9")
            alpha = Decimal("2") / Decimal("10")
            for c in closes[9:]:
                ema = alpha * c + (Decimal("1") - alpha) * ema
            self.ema_9[symbol] = ema

        # EMA(21): SMA(21) seed, alpha=2/22.
        if len(closes) >= 21:
            ema = sum(closes[:21], Decimal("0")) / Decimal("21")
            alpha = Decimal("2") / Decimal("22")
            for c in closes[21:]:
                ema = alpha * c + (Decimal("1") - alpha) * ema
            self.ema_21[symbol] = ema

        # Vol MA(20): rolling SMA.
        if len(vols) >= 20:
            window = vols[-20:]
            self.volume_ma_20[symbol] = (
                sum(window, Decimal("0")) / Decimal("20")
            )
            self.volume_history[symbol] = list(window)

        # Last interval_begin per AR-045.
        if len(candles) >= 1:
            self.last_interval_begin[symbol] = str(candles[-1][0])
            try:
                last = candles[-1]
                self.last_complete_candle[symbol] = OHLCCandle(
                    interval_begin=str(last[0]),
                    open=Decimal(str(last[1])),
                    high=Decimal(str(last[2])),
                    low=Decimal(str(last[3])),
                    close=Decimal(str(last[4])),
                    volume=Decimal(str(last[6])),
                    interval=5,
                )
                self.previous_close_5m[symbol] = Decimal(str(last[4]))
            except Exception:
                pass

    def _seed_htf_ema(self, symbol: str, candles_60m: list) -> None:
        if len(candles_60m) < 50:
            return
        closes = [Decimal(str(c[4])) for c in candles_60m]
        ema20 = sum(closes[:20], Decimal("0")) / Decimal("20")
        a20 = Decimal("2") / Decimal("21")
        for c in closes[20:]:
            ema20 = a20 * c + (Decimal("1") - a20) * ema20
        ema50 = sum(closes[:50], Decimal("0")) / Decimal("50")
        a50 = Decimal("2") / Decimal("51")
        for c in closes[50:]:
            ema50 = a50 * c + (Decimal("1") - a50) * ema50
        self.htf_ema_20[symbol] = ema20
        self.htf_ema_50[symbol] = ema50
        if candles_60m:
            self.last_interval_begin_60[symbol] = str(candles_60m[-1][0])

    async def _trigger_daily_regime_refresh(self) -> None:
        if self._regime_engine_fn is None:
            return
        self._log("REGIME_STARTUP_SEED_STARTED", level="INFO")
        try:
            res = self._regime_engine_fn(self.tradeable_universe)
            if asyncio.iscoroutine(res):
                await res
        except Exception as e:
            self._log("REGIME_REFRESH_FAILED", level="WARN", error=str(e))
        self._log("REGIME_STARTUP_SEED_COMPLETE", level="INFO")

    # ----------------------------------------------------------
    # Liquidity refresh loop (WM-LIQ-005..008)
    # ----------------------------------------------------------

    async def _liquidity_refresh_loop(self) -> None:
        sleep_sec = int(self._cfg.get(
            "liquidity_refresh_hours", LIQUIDITY_REFRESH_HOURS
        )) * 3600
        while True:
            try:
                await asyncio.sleep(sleep_sec)
                result = await self._rest_get_ticker(
                    sorted(self.tradeable_universe)
                )
                if result:
                    self.liquidity_24h.update(result)
                    self._log("LIQUIDITY_REFRESHED", level="INFO",
                              pairs_count=len(result), timestamp=_ts())
                else:
                    self._log("LIQUIDITY_REFRESH_FAILED", level="WARN",
                              timestamp=_ts(), error="empty_result")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log("LIQUIDITY_REFRESH_FAILED",
                          level="WARN", error=str(e))

    # ----------------------------------------------------------
    # Private connect (live only)
    # ----------------------------------------------------------

    async def _connect_private_and_subscribe(self) -> None:
        if self.paper_mode:
            return
        self._ws_private = await self._ws_connect_one(PRIVATE_WS_URI)
        self._log("WS_CONNECTED", level="INFO", endpoint="private")
        await self._subscribe_private(
            "executions",
            extra={
                "token": self._ws_token,
                "snap_orders": True,
                "snap_trades": True,
                "order_status": True,  # HR-WM-005 - MANDATORY.
                "ratecounter": True,
            },
        )
        await self._subscribe_private(
            "balances",
            extra={"token": self._ws_token, "snapshot": True},
        )

    # ----------------------------------------------------------
    # Main loop - TaskGroup with per-shard recv + ping + zombie
    # ----------------------------------------------------------

    async def _main_loop(self) -> None:
        async with asyncio.TaskGroup() as tg:
            for shard in self.shards:
                already_started = (shard.shard_index == 0)
                if not already_started:
                    tg.create_task(
                        self._recv_loop_shard(shard),
                        name=f"shard_{shard.shard_index}_recv",
                    )
                tg.create_task(
                    self._ping_loop_shard(shard),
                    name=f"shard_{shard.shard_index}_ping",
                )
                tg.create_task(
                    self._zombie_loop_shard(shard),
                    name=f"shard_{shard.shard_index}_zombie",
                )
            if not self.paper_mode and self._ws_private is not None:
                tg.create_task(
                    self._recv_loop_private(),
                    name="private_recv",
                )
                tg.create_task(
                    self._ping_loop_private(),
                    name="private_ping",
                )
                tg.create_task(
                    self._zombie_loop_private(),
                    name="private_zombie",
                )

    # ----------------------------------------------------------
    # Recv loops (per-shard + private)
    # ----------------------------------------------------------

    async def _recv_loop_shard(self, shard: WSShard) -> None:
        """Per-shard inbound dispatch (WM-SHARD-006)."""
        while not self._fatal_reconnect_failure:
            conn = shard.connection
            if conn is None:
                ev = self._reconnect_done_events.get(shard.shard_index)
                if ev is not None:
                    await ev.wait()
                continue
            try:
                async for raw in conn:
                    try:
                        msg = orjson.loads(raw)
                    except Exception:
                        self._log("UNKNOWN_MESSAGE_TYPE",
                                  level="WARN", reason="json_parse_fail")
                        continue
                    msg["_shard_index"] = shard.shard_index
                    await self._dispatch_inbound(msg, shard=shard)
            except Exception as e:
                self._log("RECONNECT_TRIGGERED", level="HIGH",
                          source="_recv_loop_shard",
                          error_type=type(e).__name__,
                          shard_index=shard.shard_index)
                await self._initiate_reconnect_shard(shard.shard_index)
                continue
            # Clean exit (WM-RECONNECT-016).
            self._log("RECONNECT_TRIGGERED", level="HIGH",
                      source="_recv_loop_shard",
                      error_type="clean_exit",
                      shard_index=shard.shard_index)
            await self._initiate_reconnect_shard(shard.shard_index)

    async def _recv_loop_private(self) -> None:
        if self.paper_mode:
            return
        while not self._fatal_reconnect_failure:
            conn = self._ws_private
            if conn is None:
                await self._private_reconnect_done.wait()
                continue
            try:
                async for raw in conn:
                    try:
                        msg = orjson.loads(raw)
                    except Exception:
                        self._log("UNKNOWN_MESSAGE_TYPE",
                                  level="WARN", reason="json_parse_fail")
                        continue
                    await self._dispatch_inbound(msg, shard=None)
            except Exception as e:
                self._log("RECONNECT_TRIGGERED", level="HIGH",
                          source="_recv_loop_private",
                          error_type=type(e).__name__)
                await self._initiate_reconnect_private()
                continue
            self._log("RECONNECT_TRIGGERED", level="HIGH",
                      source="_recv_loop_private",
                      error_type="clean_exit")
            await self._initiate_reconnect_private()

    # ----------------------------------------------------------
    # Inbound dispatch
    # ----------------------------------------------------------

    async def _dispatch_inbound(
        self,
        msg: dict,
        shard: _t.Optional[WSShard],
    ) -> None:
        if "method" in msg and msg.get("method") == "pong":
            await self._handle_pong(msg, shard)
            return
        if "method" in msg and "result" in msg:
            await self._handle_method_ack(msg, shard)
            return
        channel = msg.get("channel")
        if channel:
            handler = self._channel_dispatch.get(channel)
            if handler is None:
                self._log("UNKNOWN_MESSAGE_TYPE", level="WARN",
                          channel=channel)
                return
            try:
                await handler(msg, shard)
            except Exception as e:
                self._log("DISPATCH_HANDLER_ERROR", level="WARN",
                          channel=channel, error=str(e))
            return
        if "error" in msg and msg.get("error"):
            self._log("WS_ERROR", level="WARN",
                      error=msg.get("error"),
                      req_id=msg.get("req_id"))
            return

    async def _handle_method_ack(
        self, msg: dict, shard: _t.Optional[WSShard]
    ) -> None:
        warnings = msg.get("warnings") or []
        if warnings:
            self._log("SUBSCRIPTION_ACK_WARNING", level="INFO",
                      warnings=warnings,
                      req_id=msg.get("req_id"))
        result = msg.get("result") or {}
        if isinstance(result, dict) and "maxratecount" in result:
            try:
                self.maxratecount = int(result["maxratecount"])
                self._log("MAXRATECOUNT_SET", level="INFO",
                          maxratecount=self.maxratecount)
            except Exception:
                pass

    # ----------------------------------------------------------
    # Channel handlers
    # ----------------------------------------------------------

    async def _handle_ohlc(
        self, msg: dict, shard: _t.Optional[WSShard]
    ) -> None:
        data_list = msg.get("data") or []
        if not data_list:
            return
        if shard is not None:
            shard.last_real_data_time = time.monotonic()
        for entry in data_list:
            symbol = entry.get("symbol")
            if not symbol:
                continue
            interval = int(entry.get("interval", 5))
            interval_begin = str(entry.get("interval_begin", ""))
            try:
                close = Decimal(str(entry.get("close", 0)))
                high = Decimal(str(entry.get("high", 0)))
                low = Decimal(str(entry.get("low", 0)))
                op = Decimal(str(entry.get("open", 0)))
                vol = Decimal(str(entry.get("volume", 0)))
            except Exception:
                continue
            if shard is not None:
                self._on_pair_data_received(shard, symbol)
            if interval == 5:
                prev_begin = self.last_interval_begin.get(symbol)
                if prev_begin == interval_begin:
                    self.last_complete_candle[symbol] = OHLCCandle(
                        interval_begin=interval_begin,
                        open=op, high=high, low=low,
                        close=close, volume=vol, interval=5,
                    )
                else:
                    closed_candle = self.last_complete_candle.get(symbol)
                    if closed_candle is not None and prev_begin is not None:
                        self._update_indicators_5m(symbol, closed_candle)
                        await self._fire_pipeline(symbol, closed_candle)
                    self.last_complete_candle[symbol] = OHLCCandle(
                        interval_begin=interval_begin,
                        open=op, high=high, low=low,
                        close=close, volume=vol, interval=5,
                    )
                    self.last_interval_begin[symbol] = interval_begin
                    self._log("CANDLE_CLOSE", level="DEBUG",
                              symbol=symbol, interval=5,
                              interval_begin=interval_begin)
            elif interval == 60:
                # ohlc(60) is REST-only per HR-WM-021; if it arrives, log only.
                self._log("UNEXPECTED_OHLC_60", level="WARN", symbol=symbol)

    async def _handle_ticker(
        self, msg: dict, shard: _t.Optional[WSShard]
    ) -> None:
        data_list = msg.get("data") or []
        if not data_list:
            return
        if shard is not None:
            shard.last_real_data_time = time.monotonic()
        for entry in data_list:
            symbol = entry.get("symbol")
            if not symbol:
                continue
            try:
                bid = Decimal(str(entry.get("bid", 0)))
                ask = Decimal(str(entry.get("ask", 0)))
            except Exception:
                continue
            if shard is not None:
                self._on_pair_data_received(shard, symbol)
            self.latest_bid[symbol] = bid
            if symbol in self.position_mirror:
                self._compute_drawdown_for_pair(symbol)
                if self.paper_mode:
                    await self._maybe_paper_fill(symbol, bid=bid, ask=ask)
                if self._exit_controller_fn is not None:
                    try:
                        res = self._exit_controller_fn(
                            event="bbo", symbol=symbol, bid=bid, ask=ask
                        )
                        if asyncio.iscoroutine(res):
                            await res
                    except Exception as e:
                        self._log("EXIT_CONTROLLER_ERROR", level="WARN",
                                  error=str(e), symbol=symbol)

    async def _handle_instrument(
        self, msg: dict, shard: _t.Optional[WSShard]
    ) -> None:
        if shard is not None:
            shard.last_real_data_time = time.monotonic()
        data_list = msg.get("data") or []
        msg_type = msg.get("type", "snapshot")
        for entry in data_list:
            pairs = entry.get("pairs") or []
            for pair in pairs:
                symbol = pair.get("symbol", "")
                if not symbol:
                    continue
                quote_split = symbol.split("/", 1)
                quote_currency = quote_split[1] if len(quote_split) == 2 else ""
                try:
                    self.pair_cache[symbol] = {
                        "status": pair.get("status", ""),
                        "quote_currency": quote_currency,
                        "price_increment": Decimal(
                            str(pair.get("price_increment", "0"))),
                        "qty_increment": Decimal(
                            str(pair.get("qty_increment", "0"))),
                        "qty_min": Decimal(str(pair.get("qty_min", "0"))),
                        "cost_min": Decimal(str(pair.get("cost_min", "0"))),
                    }
                except Exception:
                    continue
        self._log(
            "INSTRUMENT_SNAPSHOT_COMPLETE",
            level="INFO" if msg_type == "snapshot" else "DEBUG",
        )

    async def _handle_status(
        self, msg: dict, shard: _t.Optional[WSShard]
    ) -> None:
        if shard is not None:
            shard.last_real_data_time = time.monotonic()
        data_list = msg.get("data") or []
        for entry in data_list:
            connection_id = entry.get("connection_id")
            system_status = entry.get("system", "")
            api_version = entry.get("api_version", "")
            if shard is not None and connection_id is not None:
                try:
                    shard.connection_id = int(connection_id)
                except Exception:
                    pass
            self._log(
                "WS_CONNECTED", level="INFO",
                endpoint=("private" if shard is None else "public"),
                shard_index=(shard.shard_index if shard is not None else None),
                connection_id=connection_id,
                system=system_status, api_version=api_version,
            )

    async def _handle_executions(
        self, msg: dict, shard: _t.Optional[WSShard]
    ) -> None:
        # Private channel - shard is None.
        self._last_real_data_time_private = time.monotonic()
        data_list = msg.get("data") or []
        seq = msg.get("sequence")
        if isinstance(seq, int):
            if (self.executions_last_seq != 0
                    and seq != self.executions_last_seq + 1):
                await self._handle_executions_gap(seq)
            self.executions_last_seq = seq
        rate_counter = msg.get("ratecounter")
        if rate_counter is not None:
            for entry in data_list:
                sym = entry.get("symbol")
                if sym:
                    try:
                        self.rate_counter_by_pair[sym] = Decimal(
                            str(rate_counter)
                        )
                    except Exception:
                        pass
        for entry in data_list:
            etype = entry.get("exec_type", "status")
            handler = self._exec_dispatch.get(etype)
            if handler is None:
                self._log("UNKNOWN_EXEC_TYPE", level="WARN", exec_type=etype)
                continue
            try:
                await handler(entry)
            except Exception as e:
                self._log("EXEC_HANDLER_ERROR", level="WARN",
                          exec_type=etype, error=str(e))

    async def _handle_balances(
        self, msg: dict, shard: _t.Optional[WSShard]
    ) -> None:
        self._last_real_data_time_private = time.monotonic()
        seq = msg.get("sequence")
        if isinstance(seq, int):
            if (self.balances_last_seq != 0
                    and seq != self.balances_last_seq + 1):
                await self._handle_balances_gap(seq)
            self.balances_last_seq = seq
        for wallet in msg.get("data") or []:
            if (wallet.get("wallet_type") == "spot"
                    and wallet.get("wallet_id") == "main"
                    and wallet.get("asset") == "USD"):
                try:
                    self.spot_usd_balance = Decimal(
                        str(wallet.get("balance", "0"))
                    )
                except Exception:
                    continue
                # HR-WM-011: set baseline ONCE in live mode (paper sets it earlier).
                if self.portfolio_baseline_USD is None and not self.paper_mode:
                    self.portfolio_baseline_USD = self.spot_usd_balance
                    self._log("PORTFOLIO_BASELINE_SET", level="INFO",
                              baseline=self.portfolio_baseline_USD)

    async def _handle_pong(
        self, msg: dict, shard: _t.Optional[WSShard]
    ) -> None:
        if shard is not None:
            shard.last_pong_time = time.monotonic()
        else:
            self._last_pong_time_private = time.monotonic()
        self._log(
            "PONG_RECEIVED", level="DEBUG",
            shard=(shard.shard_index if shard is not None else "private"),
        )

    # ----------------------------------------------------------
    # exec_type handlers (Section 5.3)
    # ----------------------------------------------------------

    async def _handle_pending_new(self, event: dict) -> None:
        self._log("EXEC_PENDING_NEW", level="DEBUG",
                  order_id=event.get("order_id"),
                  cl_ord_id=event.get("cl_ord_id"))

    async def _handle_new(self, event: dict) -> None:
        self._log("EXEC_NEW", level="DEBUG",
                  order_id=event.get("order_id"),
                  cl_ord_id=event.get("cl_ord_id"))

    async def _handle_trade(self, event: dict) -> None:
        try:
            cum_qty = Decimal(str(event.get("cum_qty", 0)))
            avg_price = Decimal(str(event.get("avg_price", 0)))
            last_qty = Decimal(str(event.get("last_qty", 0)))
            last_price = Decimal(str(event.get("last_price", 0)))
        except Exception:
            return
        order_id = event.get("order_id", "")
        cl_ord_id = event.get("cl_ord_id", "")
        symbol = event.get("symbol", "")
        for sym, pos in self.position_mirror.items():
            if pos.tp_order_id and order_id == pos.tp_order_id:
                await self._handle_partial_tp_fill(sym, event)
                break
            if pos.cl_ord_id and (cl_ord_id == pos.cl_ord_id
                                   or order_id == pos.cl_ord_id):
                pos.qty = cum_qty
                pos.entry_price = avg_price
                break
        self._log(
            "FILL_EVENT", level="INFO",
            order_id=order_id, cl_ord_id=cl_ord_id, symbol=symbol,
            last_qty=last_qty, last_price=last_price,
            cum_qty=cum_qty, avg_price=avg_price, exec_type="trade",
        )

    async def _handle_filled(self, event: dict) -> None:
        order_id = event.get("order_id", "")
        cl_ord_id = event.get("cl_ord_id", "")
        for sym, pos in list(self.position_mirror.items()):
            if pos.tp_order_id and order_id == pos.tp_order_id:
                if pos.emergsl_order_id:
                    await self.cancel_order(pos.emergsl_order_id)
                self._fire_ciats_outcome(sym, exit_reason="TP_FILL", pos=pos)
                self.position_mirror.pop(sym, None)
                await self._update_ticker_event_trigger(sym, "trades")
                self._log("TP_FILLED", level="INFO", symbol=sym)
                return
            if pos.cl_ord_id and pos.cl_ord_id == cl_ord_id:
                if self._execution_engine_fn is not None:
                    try:
                        res = self._execution_engine_fn(
                            event="entry_filled",
                            symbol=sym,
                            cum_qty=Decimal(str(event.get("cum_qty", 0))),
                            avg_price=Decimal(str(event.get("avg_price", 0))),
                        )
                        if asyncio.iscoroutine(res):
                            await res
                    except Exception as e:
                        self._log("EXEC_ENGINE_ERROR", level="WARN",
                                  error=str(e), symbol=sym)
                return
        self._log("EXEC_FILLED_UNMATCHED", level="WARN",
                  order_id=order_id, cl_ord_id=cl_ord_id)

    async def _handle_iceberg_refill(self, event: dict) -> None:
        self._log("EXEC_ICEBERG_REFILL", level="DEBUG",
                  order_id=event.get("order_id"))

    async def _handle_canceled(self, event: dict) -> None:
        order_id = event.get("order_id", "")
        cl_ord_id = event.get("cl_ord_id", "")
        try:
            cum_qty = Decimal(str(event.get("cum_qty", 0)))
        except Exception:
            cum_qty = Decimal("0")
        self.pending_orders.pop(cl_ord_id, None)
        self._log("EXEC_CANCELED", level="INFO",
                  order_id=order_id, cl_ord_id=cl_ord_id,
                  cum_qty=cum_qty, reason=event.get("reason", ""))
        if cum_qty > 0:
            await self._handle_entry_partial_fill(event)

    async def _handle_expired(self, event: dict) -> None:
        order_id = event.get("order_id", "")
        cl_ord_id = event.get("cl_ord_id", "")
        try:
            cum_qty = Decimal(str(event.get("cum_qty", 0)))
        except Exception:
            cum_qty = Decimal("0")
        self.pending_orders.pop(cl_ord_id, None)
        self._log("EXEC_EXPIRED", level="INFO",
                  order_id=order_id, cl_ord_id=cl_ord_id,
                  cum_qty=cum_qty, reason=event.get("reason", ""))
        if cum_qty > 0:
            await self._handle_entry_partial_fill(event)

    async def _handle_amended(self, event: dict) -> None:
        """UNEXPECTED - TothBot V2 never amends its own orders.

        AR-057 / WM-DISP-024..026.
        """
        order_id = event.get("order_id", "")
        cl_ord_id = event.get("cl_ord_id", "")
        new_limit = event.get("limit_price")
        new_qty = event.get("order_qty")
        self._log(
            "UNEXPECTED_ORDER_AMENDED", level="CRITICAL",
            order_id=order_id, cl_ord_id=cl_ord_id,
            new_limit_price=new_limit, new_qty=new_qty,
        )
        try:
            self._alert_fn(
                "HIGH",
                f"UNEXPECTED order amendment: order_id={order_id} "
                f"cl_ord_id={cl_ord_id}",
            )
        except Exception:
            pass
        for sym, pos in self.position_mirror.items():
            if pos.tp_order_id and order_id == pos.tp_order_id:
                self._log("AMENDED_ORDER_IS_TP",
                          level="CRITICAL", symbol=sym)
                try:
                    self._alert_fn(
                        "CRITICAL",
                        f"TP for {sym} amended - sacred R:R may be violated",
                    )
                except Exception:
                    pass
            elif pos.emergsl_order_id and order_id == pos.emergsl_order_id:
                self._log("AMENDED_ORDER_IS_EMERGSL",
                          level="CRITICAL", symbol=sym)
                try:
                    self._alert_fn(
                        "CRITICAL",
                        f"emergSL for {sym} amended - "
                        f"crash protection altered",
                    )
                except Exception:
                    pass

    async def _handle_restated(self, event: dict) -> None:
        self._log("EXEC_RESTATED", level="WARN",
                  order_id=event.get("order_id"))

    async def _handle_exec_status(self, event: dict) -> None:
        self._log("EXEC_STATUS", level="DEBUG",
                  order_id=event.get("order_id"))

    # ----------------------------------------------------------
    # Partial fill handlers (Section 5.4 / 5.5)
    # ----------------------------------------------------------

    async def _handle_partial_tp_fill(
        self, symbol: str, event: dict
    ) -> None:
        try:
            cum_qty = Decimal(str(event.get("cum_qty", 0)))
            avg_exit = Decimal(str(event.get("avg_price", 0)))
        except Exception:
            return
        pos = self.position_mirror.get(symbol)
        if pos is None:
            return
        orig_qty = pos.qty
        remaining = orig_qty - cum_qty
        cache = self.pair_cache.get(symbol, {})
        q_incr = cache.get("qty_increment") or Decimal("0")
        qty_min = cache.get("qty_min") or Decimal("0")
        cost_min = cache.get("cost_min") or Decimal("0")
        bid = self.latest_bid.get(symbol, Decimal("0"))
        if q_incr > 0:
            remaining = remaining.quantize(q_incr, rounding=ROUND_DOWN)
        if remaining < qty_min or remaining * bid < cost_min:
            if pos.emergsl_order_id:
                await self.cancel_order(pos.emergsl_order_id)
            await self.dispatch_market_sell(symbol, remaining)
            self._log("TP_PARTIAL_FILL_BELOW_MIN_EXIT", level="INFO",
                      symbol=symbol, remaining=remaining)
            self.position_mirror.pop(symbol, None)
            await self._update_ticker_event_trigger(symbol, "trades")
            return
        pos.qty = remaining
        if pos.emergsl_order_id:
            await self.amend_order(pos.emergsl_order_id, remaining)
        self._log(
            "TP_PARTIAL_FILL", level="INFO",
            symbol=symbol, filled_qty=cum_qty,
            remaining_qty=remaining, avg_exit_price=avg_exit,
            emergsl_amended_to=remaining,
        )

    async def _handle_entry_partial_fill(self, event: dict) -> None:
        """AR-054: GTD entry partial-fill protection."""
        try:
            cum_qty = Decimal(str(event.get("cum_qty", 0)))
            entry_price = Decimal(str(event.get("avg_price", 0)))
        except Exception:
            return
        symbol = event.get("symbol", "")
        if not symbol:
            return
        cache = self.pair_cache.get(symbol, {})
        qty_min = cache.get("qty_min") or Decimal("0")
        cost_min = cache.get("cost_min") or Decimal("0")
        if cum_qty < qty_min or cum_qty * entry_price < cost_min:
            await self.dispatch_market_sell(symbol, cum_qty)
            self._log("ENTRY_PARTIAL_FILL_BELOW_MIN", level="INFO",
                      symbol=symbol)
            return
        atr = self.atr_14.get(symbol)
        if atr is None or atr <= 0:
            await self.dispatch_market_sell(symbol, cum_qty)
            self._log("ENTRY_PARTIAL_FILL_NO_ATR", level="WARN",
                      symbol=symbol)
            return
        mae_pct = atr * MAE_MULT / entry_price
        net_loss = mae_pct + FEE_MAKER_PCT + FEE_TAKER_PCT
        net_gain = net_loss * NET_RR_RATIO
        tp_price = entry_price * (Decimal("1") + net_gain)
        sl_trigger = entry_price * (Decimal("1") - mae_pct)
        cl_ord_id = self._make_cl_ord_id(symbol, prefix="EP")
        await self.batch_add(
            symbol=symbol,
            cl_ord_id=cl_ord_id,
            qty=cum_qty,
            tp_price=tp_price,
            emergsl_trigger_price=sl_trigger,
        )
        self.position_mirror[symbol] = PositionRecord(
            symbol=symbol, qty=cum_qty, entry_price=entry_price,
            cl_ord_id=cl_ord_id,
            tp_price=tp_price, emergsl_price=sl_trigger,
            tp_order_id=f"PAPER_TP_{cl_ord_id}" if self.paper_mode else None,
            emergsl_order_id=(f"PAPER_SL_{cl_ord_id}"
                              if self.paper_mode else None),
        )
        self._log(
            "ENTRY_PARTIAL_FILL_PROTECTED", level="INFO",
            symbol=symbol, qty=cum_qty, entry=entry_price,
            tp=tp_price, sl=sl_trigger,
        )

    @staticmethod
    def _make_cl_ord_id(symbol: str, prefix: str = "EN") -> str:
        ts_ms = int(time.time() * 1000) % 1_000_000_000
        sym_part = symbol.replace("/", "")[:6]
        s = f"{prefix}{sym_part}{ts_ms}"
        return s[:18]

    # ----------------------------------------------------------
    # Outbound order methods (Section 6 / paper-mode gates)
    # ----------------------------------------------------------

    async def add_order(
        self,
        symbol: str,
        cl_ord_id: str,
        qty: Decimal,
        limit_price: Decimal,
    ) -> None:
        cache = self.pair_cache.get(symbol, {})
        q_incr = cache.get("qty_increment") or Decimal("0")
        p_incr = cache.get("price_increment") or Decimal("0")
        if q_incr > 0:
            qty = qty.quantize(q_incr, rounding=ROUND_DOWN)
        if p_incr > 0:
            limit_price = limit_price.quantize(p_incr, rounding=ROUND_DOWN)
        self.pending_orders[cl_ord_id] = qty * limit_price
        if self.paper_mode:
            self._log(
                "PAPER_ORDER_SIMULATED", level="INFO",
                symbol=symbol, cl_ord_id=cl_ord_id,
                qty=qty, price=limit_price,
            )
            asyncio.create_task(
                self._simulate_entry_fill(cl_ord_id, symbol, limit_price, qty),
                name=f"paper_entry_{cl_ord_id}",
            )
            return
        deadline = (datetime.now(timezone.utc)
                    + timedelta(seconds=DEADLINE_OFFSET_SEC)).isoformat()
        expire_time = (datetime.now(timezone.utc)
                       + timedelta(seconds=ENTRY_GTD_SECONDS)).isoformat()
        payload = {
            "method": "add_order",
            "params": {
                "order_type": "limit",
                "side": "buy",
                "symbol": symbol,
                "limit_price": str(limit_price),
                "order_qty": str(qty),
                "post_only": True,
                "time_in_force": "gtd",
                "expire_time": expire_time,
                "stp_type": "cancel_newest",
                "deadline": deadline,
                "cl_ord_id": cl_ord_id,
                "token": self._ws_token,
            },
            "req_id": self._next_req_id(),
        }
        await self._send_private(payload)
        self._log("ENTRY_DISPATCHED", level="INFO",
                  symbol=symbol, cl_ord_id=cl_ord_id,
                  qty=qty, limit_price=limit_price)

    async def _simulate_entry_fill(
        self,
        cl_ord_id: str,
        symbol: str,
        price: Decimal,
        qty: Decimal,
    ) -> None:
        await asyncio.sleep(0.1)
        self.position_mirror[symbol] = PositionRecord(
            symbol=symbol, qty=qty, entry_price=price,
            cl_ord_id=cl_ord_id,
        )
        self.pending_orders.pop(cl_ord_id, None)
        self._log("PAPER_ENTRY_FILLED", level="INFO",
                  symbol=symbol, cl_ord_id=cl_ord_id,
                  qty=qty, price=price)
        await self._update_ticker_event_trigger(symbol, "bbo")
        if self._execution_engine_fn is not None:
            try:
                res = self._execution_engine_fn(
                    event="entry_filled",
                    symbol=symbol, cum_qty=qty, avg_price=price,
                )
                if asyncio.iscoroutine(res):
                    await res
            except Exception as e:
                self._log("EXEC_ENGINE_ERROR", level="WARN",
                          error=str(e), symbol=symbol)

    async def batch_add(
        self,
        symbol: str,
        cl_ord_id: str,
        qty: Decimal,
        tp_price: Decimal,
        emergsl_trigger_price: Decimal,
    ) -> None:
        cache = self.pair_cache.get(symbol, {})
        q_incr = cache.get("qty_increment") or Decimal("0")
        p_incr = cache.get("price_increment") or Decimal("0")
        if q_incr > 0:
            qty = qty.quantize(q_incr, rounding=ROUND_DOWN)
        if p_incr > 0:
            tp_price = tp_price.quantize(p_incr, rounding=ROUND_UP)
            emergsl_trigger_price = emergsl_trigger_price.quantize(
                p_incr, rounding=ROUND_DOWN
            )
        tp_cl = f"TP_{cl_ord_id}"[:18]
        sl_cl = f"SL_{cl_ord_id}"[:18]
        if self.paper_mode:
            pos = self.position_mirror.get(symbol)
            if pos is not None:
                pos.tp_price = tp_price
                pos.emergsl_price = emergsl_trigger_price
                pos.tp_order_id = f"PAPER_TP_{cl_ord_id}"
                pos.emergsl_order_id = f"PAPER_SL_{cl_ord_id}"
            self._log("PAPER_BATCH_ADD_SIMULATED", level="INFO",
                      symbol=symbol, cl_ord_id=cl_ord_id,
                      tp_price=tp_price,
                      emergsl_price=emergsl_trigger_price)
            return
        deadline = (datetime.now(timezone.utc)
                    + timedelta(seconds=DEADLINE_OFFSET_SEC)).isoformat()
        payload = {
            "method": "batch_add",
            "params": {
                "orders": [
                    {
                        "order_type": "limit",
                        "side": "sell",
                        "symbol": symbol,
                        "limit_price": str(tp_price),
                        "order_qty": str(qty),
                        "cl_ord_id": tp_cl,
                        "stp_type": "cancel_newest",
                        "deadline": deadline,
                    },
                    {
                        "order_type": "stop-loss",
                        "side": "sell",
                        "symbol": symbol,
                        "limit_price": str(emergsl_trigger_price),
                        "order_qty": str(qty),
                        "cl_ord_id": sl_cl,
                        "triggers": {
                            "reference": "last",
                            "price": str(emergsl_trigger_price),
                            "price_type": "static",
                        },
                        "stp_type": "cancel_newest",
                        "deadline": deadline,
                    },
                ],
                "token": self._ws_token,
            },
            "req_id": self._next_req_id(),
        }
        await self._send_private(payload)
        self._log("BATCH_ADD_DISPATCHED", level="INFO",
                  symbol=symbol, cl_ord_id=cl_ord_id)

    async def cancel_order(self, order_id: str) -> None:
        if self.paper_mode:
            self._log("PAPER_CANCEL_SIMULATED", level="INFO",
                      order_id=order_id)
            return
        payload = {
            "method": "cancel_order",
            "params": {
                "order_id": [order_id],
                "token": self._ws_token,
            },
            "req_id": self._next_req_id(),
        }
        await self._send_private(payload)

    async def amend_order(self, order_id: str, order_qty: Decimal) -> None:
        if self.paper_mode:
            self._log("PAPER_AMEND_SIMULATED", level="INFO",
                      order_id=order_id, new_qty=order_qty)
            return
        payload = {
            "method": "amend_order",
            "params": {
                "order_id": order_id,
                "order_qty": str(order_qty),
                "token": self._ws_token,
            },
            "req_id": self._next_req_id(),
        }
        await self._send_private(payload)

    async def batch_cancel(self, order_ids: _t.List[str]) -> None:
        if self.paper_mode:
            for oid in order_ids:
                self._log("PAPER_CANCEL_SIMULATED",
                          level="INFO", order_id=oid)
            return
        payload = {
            "method": "batch_cancel",
            "params": {
                "orders": order_ids,
                "token": self._ws_token,
            },
            "req_id": self._next_req_id(),
        }
        await self._send_private(payload)

    async def dispatch_market_sell(
        self, symbol: str, qty: Decimal
    ) -> None:
        if self.paper_mode:
            self._log("PAPER_MARKET_SELL_SIMULATED", level="INFO",
                      symbol=symbol, qty=qty)
            self.position_mirror.pop(symbol, None)
            return
        cl_ord_id = self._make_cl_ord_id(symbol, prefix="MS")
        deadline = (datetime.now(timezone.utc)
                    + timedelta(seconds=DEADLINE_OFFSET_SEC)).isoformat()
        payload = {
            "method": "add_order",
            "params": {
                "order_type": "market",
                "side": "sell",
                "symbol": symbol,
                "order_qty": str(qty),
                "stp_type": "cancel_newest",
                "deadline": deadline,
                "cl_ord_id": cl_ord_id,
                "token": self._ws_token,
            },
            "req_id": self._next_req_id(),
        }
        await self._send_private(payload)
        self._log("MARKET_SELL_DISPATCHED", level="INFO",
                  symbol=symbol, qty=qty)

    # ----------------------------------------------------------
    # Paper fill detection (HR-WM-024 / Section 13.3)
    # ----------------------------------------------------------

    async def _maybe_paper_fill(
        self, symbol: str, bid: Decimal, ask: Decimal
    ) -> None:
        pos = self.position_mirror.get(symbol)
        if pos is None or pos.tp_price is None or pos.emergsl_price is None:
            return
        # TP check first (HR-WM-024).
        if ask >= pos.tp_price:
            self._log("PAPER_TP_FILL_DETECTED", level="INFO",
                      symbol=symbol, tp_price=pos.tp_price, ask=ask)
            self._fire_ciats_outcome(symbol, exit_reason="TP_FILL", pos=pos)
            self.position_mirror.pop(symbol, None)
            await self._update_ticker_event_trigger(symbol, "trades")
            return
        if bid <= pos.emergsl_price:
            self._log("PAPER_EMERG_SL_TRIGGERED", level="INFO",
                      symbol=symbol, emergsl_price=pos.emergsl_price,
                      bid=bid)
            self._fire_ciats_outcome(
                symbol, exit_reason="EMERGENCY_SL", pos=pos
            )
            self.position_mirror.pop(symbol, None)
            await self._update_ticker_event_trigger(symbol, "trades")

    def _fire_ciats_outcome(
        self, symbol: str, exit_reason: str, pos: PositionRecord
    ) -> None:
        # Selection Controller state update (WM-SC-002).
        self.exit_cooldown_log[symbol] = time.monotonic()
        if exit_reason == "TP_FILL":
            self.consecutive_loss_count[symbol] = 0
        elif exit_reason in (
            "EMERGENCY_SL", "MAE_THRESHOLD_BREACH", "TIME_EXPIRY",
            "HTF_REGIME_REVERSAL", "DAILY_REGIME_DOWNGRADE",
            "SIGNAL_DECAY",
        ):
            self.consecutive_loss_count[symbol] = (
                self.consecutive_loss_count.get(symbol, 0) + 1
            )
        if self._ciats_outcome_bus_fn is not None:
            try:
                res = self._ciats_outcome_bus_fn(
                    symbol=symbol, exit_reason=exit_reason, pos=pos
                )
                if asyncio.iscoroutine(res):
                    asyncio.create_task(res, name=f"ciats_{symbol}")
            except Exception as e:
                self._log("CIATS_OUTCOME_ERROR", level="WARN",
                          error=str(e), symbol=symbol)

    # ----------------------------------------------------------
    # Indicator updates on candle close (Section 9.2)
    # ----------------------------------------------------------

    def _update_indicators_5m(
        self, symbol: str, candle: OHLCCandle
    ) -> None:
        prev_close = self.previous_close_5m.get(symbol)
        prev_atr = self.atr_14.get(symbol)
        if prev_atr is not None and prev_close is not None:
            tr = max(
                candle.high - candle.low,
                abs(candle.high - prev_close),
                abs(candle.low - prev_close),
            )
            self.atr_14[symbol] = (
                (prev_atr * Decimal("13") + tr) / Decimal("14")
            )
            self.prev_tr[symbol] = tr
        if prev_close is not None:
            d = candle.close - prev_close
            gain = d if d > 0 else Decimal("0")
            loss = -d if d < 0 else Decimal("0")
            ag = self.rsi_14_avg_gain.get(symbol)
            al = self.rsi_14_avg_loss.get(symbol)
            if ag is not None and al is not None:
                self.rsi_14_avg_gain[symbol] = (
                    (ag * Decimal("13") + gain) / Decimal("14")
                )
                self.rsi_14_avg_loss[symbol] = (
                    (al * Decimal("13") + loss) / Decimal("14")
                )
        for k, alpha in (
            ("ema_9", Decimal("2") / Decimal("10")),
            ("ema_21", Decimal("2") / Decimal("22")),
        ):
            prev = getattr(self, k).get(symbol)
            if prev is not None:
                getattr(self, k)[symbol] = (
                    alpha * candle.close + (Decimal("1") - alpha) * prev
                )
        hist = self.volume_history.setdefault(symbol, [])
        hist.append(candle.volume)
        if len(hist) > 20:
            hist.pop(0)
        if len(hist) == 20:
            self.volume_ma_20[symbol] = (
                sum(hist, Decimal("0")) / Decimal("20")
            )
        if symbol in self.position_mirror:
            self.position_mirror[symbol].hold_candle_count += 1
        self.previous_close_5m[symbol] = candle.close

    # ----------------------------------------------------------
    # Pipeline trigger (HR-WM-012 / WM-SHARD-009 generalized)
    # ----------------------------------------------------------

    async def _fire_pipeline(
        self, symbol: str, candle: OHLCCandle
    ) -> None:
        if not self._all_connections_healthy():
            return
        if symbol == REGIME_ANCHOR and not bool(
            self._cfg.get("ciats_btc_entry_enabled", False)
        ):
            return
        if self.warm_up_state.get(symbol) != "READY":
            return
        shard_idx = self.pair_to_shard_index.get(symbol)
        if shard_idx is None:
            return
        shard = self.shards[shard_idx]
        if shard.pair_states.get(symbol) != PairState.DATA_READY:
            return
        if self.system_state in ("SESSION_PAUSE", "FULL_HALT"):
            return
        if self._signal_pipeline_fn is None:
            return
        try:
            res = self._signal_pipeline_fn(
                symbol=symbol, candle=candle,
                pre_comp_cache=self._build_pre_comp_cache(symbol),
            )
            if asyncio.iscoroutine(res):
                await res
        except Exception as e:
            self._log("SIGNAL_PIPELINE_ERROR", level="WARN",
                      symbol=symbol, error=str(e))

    def _all_connections_healthy(self) -> bool:
        for shard in self.shards:
            if shard.in_reconnect or shard.connection is None:
                return False
        if not self.paper_mode:
            if self._private_in_reconnect or self._ws_private is None:
                return False
        return True

    def _build_pre_comp_cache(self, symbol: str) -> dict:
        return {
            "atr_14": self.atr_14.get(symbol),
            "rsi_14": self._compute_rsi_for(symbol),
            "ema_9": self.ema_9.get(symbol),
            "ema_21": self.ema_21.get(symbol),
            "volume_ma_20": self.volume_ma_20.get(symbol),
            "htf_ema_20": self.htf_ema_20.get(symbol),
            "htf_ema_50": self.htf_ema_50.get(symbol),
            "liquidity_24h": self.liquidity_24h.get(symbol, Decimal("0")),
            "spot_usd_balance": self.spot_usd_balance,
            "portfolio_baseline_USD": self.portfolio_baseline_USD,
            "exit_cooldown_log": dict(self.exit_cooldown_log),
            "consecutive_loss_count": dict(self.consecutive_loss_count),
            "max_concurrent": MAX_CONCURRENT,
            "tradeable_pct": TRADEABLE_PCT,
            "per_trade_pct": PER_TRADE_PCT,
        }

    def _compute_rsi_for(self, symbol: str) -> _t.Optional[Decimal]:
        ag = self.rsi_14_avg_gain.get(symbol)
        al = self.rsi_14_avg_loss.get(symbol)
        if ag is None or al is None:
            return None
        if al == 0:
            return Decimal("100")
        rs = ag / al
        return Decimal("100") - (Decimal("100") / (Decimal("1") + rs))

    # ----------------------------------------------------------
    # Drawdown computation (Section 11)
    # ----------------------------------------------------------

    def _compute_drawdown_for_pair(self, symbol: str) -> None:
        if self.portfolio_baseline_USD is None:
            return
        current = self.spot_usd_balance
        for sym, pos in self.position_mirror.items():
            bid = self.latest_bid.get(sym, Decimal("0"))
            current += bid * pos.qty
        if self.portfolio_baseline_USD <= 0:
            return
        drawdown = max(
            Decimal("0"),
            (self.portfolio_baseline_USD - current) /
            self.portfolio_baseline_USD,
        )
        if drawdown >= FULL_HALT_DRAWDOWN and self.system_state != "FULL_HALT":
            self.system_state = "FULL_HALT"
            self._log("FULL_HALT_TRIGGERED",
                      level="CRITICAL", drawdown=drawdown)
            try:
                self._alert_fn("CRITICAL", "FULL_HALT_TRIGGERED")
            except Exception:
                pass
        elif (drawdown >= SESSION_PAUSE_DRAWDOWN
              and self.system_state == "NORMAL"):
            self.system_state = "SESSION_PAUSE"
            self._log("SESSION_PAUSE_TRIGGERED",
                      level="HIGH", drawdown=drawdown)
            try:
                self._alert_fn("HIGH", "SESSION_PAUSE_TRIGGERED")
            except Exception:
                pass
        elif (drawdown < SESSION_PAUSE_DRAWDOWN
              and self.system_state == "SESSION_PAUSE"):
            self.system_state = "NORMAL"
            self._log("SESSION_PAUSE_RECOVERED",
                      level="INFO", drawdown=drawdown)

    # ----------------------------------------------------------
    # Sequence-gap recovery (Section 7.2 / 7.3)
    # ----------------------------------------------------------

    async def _handle_executions_gap(self, current_seq: int) -> None:
        self._log("EXECUTIONS_SEQUENCE_GAP", level="CRITICAL",
                  expected=self.executions_last_seq + 1, actual=current_seq)
        try:
            self._alert_fn("CRITICAL", "EXECUTIONS_SEQUENCE_GAP")
        except Exception:
            pass
        open_orders = await self._rest_get_open_orders()
        await self._reconcile_pending_orders(open_orders)

    async def _handle_balances_gap(self, current_seq: int) -> None:
        self._log("BALANCES_SEQUENCE_GAP", level="HIGH",
                  expected=self.balances_last_seq + 1, actual=current_seq)
        try:
            self._alert_fn("HIGH", "BALANCES_SEQUENCE_GAP")
        except Exception:
            pass
        balances = await self._rest_get_account_balance()
        usd_str = balances.get("ZUSD") or balances.get("USD")
        if usd_str is not None:
            try:
                self.spot_usd_balance = Decimal(str(usd_str))
            except Exception:
                pass

    async def _reconcile_pending_orders(
        self, open_orders: dict
    ) -> None:
        live_clord = set()
        for oid, info in (open_orders.get("open") or {}).items():
            cl = info.get("descr", {}).get("close") or info.get("userref", "")
            if cl:
                live_clord.add(str(cl))
        for cl_ord_id in list(self.pending_orders.keys()):
            if cl_ord_id not in live_clord:
                self.pending_orders.pop(cl_ord_id, None)

    # ----------------------------------------------------------
    # Ping + zombie monitors (per shard + private)
    # ----------------------------------------------------------

    async def _ping_loop_shard(self, shard: WSShard) -> None:
        while not self._fatal_reconnect_failure:
            try:
                if shard.connection is None or shard.in_reconnect:
                    await asyncio.sleep(PING_INTERVAL_SEC)
                    continue
                req_id = self._next_req_id()
                payload = {"method": "ping", "req_id": req_id}
                t0 = time.monotonic()
                await self._send_shard(shard, payload)
                deadline = t0 + PING_TIMEOUT_SEC
                pong_seen = False
                while time.monotonic() < deadline:
                    if shard.last_pong_time >= t0:
                        pong_seen = True
                        break
                    await asyncio.sleep(0.5)
                if not pong_seen:
                    self._log("PING_TIMEOUT", level="ERROR",
                              shard_index=shard.shard_index)
                    await self._initiate_reconnect_shard(shard.shard_index)
                    continue
                await asyncio.sleep(PING_INTERVAL_SEC)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log("PING_LOOP_ERROR", level="WARN",
                          shard_index=shard.shard_index, error=str(e))
                await asyncio.sleep(PING_INTERVAL_SEC)

    async def _ping_loop_private(self) -> None:
        while not self._fatal_reconnect_failure:
            try:
                if self._ws_private is None or self._private_in_reconnect:
                    await asyncio.sleep(PING_INTERVAL_SEC)
                    continue
                req_id = self._next_req_id()
                payload = {"method": "ping", "req_id": req_id}
                t0 = time.monotonic()
                await self._send_private(payload)
                deadline = t0 + PING_TIMEOUT_SEC
                pong_seen = False
                while time.monotonic() < deadline:
                    if self._last_pong_time_private >= t0:
                        pong_seen = True
                        break
                    await asyncio.sleep(0.5)
                if not pong_seen:
                    self._log("PING_TIMEOUT", level="ERROR",
                              endpoint="private")
                    await self._initiate_reconnect_private()
                    continue
                await asyncio.sleep(PING_INTERVAL_SEC)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log("PING_LOOP_ERROR", level="WARN",
                          endpoint="private", error=str(e))
                await asyncio.sleep(PING_INTERVAL_SEC)

    async def _zombie_loop_shard(self, shard: WSShard) -> None:
        while not self._fatal_reconnect_failure:
            try:
                if shard.connection is None or shard.in_reconnect:
                    await asyncio.sleep(15)
                    continue
                age = time.monotonic() - shard.last_real_data_time
                if age > ZOMBIE_THRESHOLD_SEC:
                    self._log(
                        "ZOMBIE_CONNECTION_DETECTED", level="CRITICAL",
                        shard_index=shard.shard_index, age_sec=round(age, 2),
                    )
                    try:
                        self._alert_fn(
                            "HIGH",
                            f"Zombie shard {shard.shard_index} age {age:.1f}s",
                        )
                    except Exception:
                        pass
                    await self._initiate_reconnect_shard(shard.shard_index)
                    continue
                # Long-DATA_PENDING alert (HR-WM-030).
                now = time.monotonic()
                for sym, started in list(shard.data_pending_at.items()):
                    if now - started > DATA_PENDING_LONG_ALERT_SEC:
                        self._log("PAIR_DATA_PENDING_LONG", level="WARN",
                                  pair=sym, shard=shard.shard_index,
                                  age_sec=round(now - started, 2))
                        # Reset trigger so we alert at most once per hour.
                        shard.data_pending_at[sym] = now
                await asyncio.sleep(15)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log("ZOMBIE_LOOP_ERROR", level="WARN",
                          shard_index=shard.shard_index, error=str(e))
                await asyncio.sleep(15)

    async def _zombie_loop_private(self) -> None:
        while not self._fatal_reconnect_failure:
            try:
                if self._ws_private is None or self._private_in_reconnect:
                    await asyncio.sleep(15)
                    continue
                age = time.monotonic() - self._last_real_data_time_private
                if age > ZOMBIE_THRESHOLD_SEC:
                    self._log("ZOMBIE_CONNECTION_DETECTED",
                              level="CRITICAL", endpoint="private",
                              age_sec=round(age, 2))
                    await self._initiate_reconnect_private()
                    continue
                await asyncio.sleep(15)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self._log("ZOMBIE_LOOP_ERROR", level="WARN",
                          endpoint="private", error=str(e))
                await asyncio.sleep(15)

    # ----------------------------------------------------------
    # Reconnect (per-shard + private)
    # ----------------------------------------------------------

    async def _initiate_reconnect_shard(self, shard_index: int) -> None:
        ev = self._reconnect_done_events.get(shard_index)
        if ev is None:
            return
        if not ev.is_set():
            await ev.wait()
            return
        if self._fatal_reconnect_failure:
            return
        ev.clear()
        try:
            shard = self.shards[shard_index]
            shard.in_reconnect = True
            try:
                if shard.connection is not None:
                    await shard.connection.close()
            except Exception:
                pass
            shard.connection = None
            for attempt in range(1, MAX_RECONNECT_ATTEMPTS + 1):
                try:
                    new_conn = await self._ws_connect_one(PUBLIC_WS_URI)
                    shard.connection = new_conn
                    shard.last_real_data_time = time.monotonic()
                    shard.last_pong_time = time.monotonic()
                    if shard.shard_index == 0:
                        await self._subscribe_on_shard(
                            shard, "instrument", extra={"snapshot": True}
                        )
                        await self._subscribe_on_shard(
                            shard, "status", extra={"snapshot": True}
                        )
                    for sym in list(shard.symbols):
                        await self._subscribe_on_shard(
                            shard, "ohlc", symbols=[sym],
                            extra={"interval": 5, "snapshot": True},
                        )
                        trigger = self._ticker_trigger_for(sym)
                        await self._subscribe_on_shard(
                            shard, "ticker", symbols=[sym],
                            extra={"event_trigger": trigger,
                                   "snapshot": True},
                        )
                        shard.pair_states[sym] = PairState.SUBSCRIBED
                        shard.data_pending_at.pop(sym, None)
                        self._arm_silent_timer(shard, sym)
                    shard.in_reconnect = False
                    self._log("RECONNECT_COMPLETE", level="INFO",
                              shard_index=shard_index, attempts=attempt)
                    return
                except Exception as e:
                    self._log("RECONNECT_ATTEMPT_FAILED", level="WARN",
                              shard_index=shard_index, attempt=attempt,
                              error=str(e))
                    backoff = min(
                        RECONNECT_BACKOFF_CAP_SEC,
                        RECONNECT_BACKOFF_BASE_SEC
                        * (RECONNECT_BACKOFF_FACTOR ** (attempt - 1)),
                    )
                    await asyncio.sleep(backoff)
            self._fatal_reconnect_failure = True
            self._log("FATAL_RECONNECT_FAILURE", level="CRITICAL",
                      shard_index=shard_index)
            try:
                self._alert_fn(
                    "CRITICAL",
                    f"FATAL_RECONNECT_FAILURE shard {shard_index}",
                )
            except Exception:
                pass
            raise RuntimeError(
                f"FATAL_RECONNECT_FAILURE shard {shard_index}"
            )
        finally:
            ev.set()

    async def _initiate_reconnect_private(self) -> None:
        if self.paper_mode:
            return
        if not self._private_reconnect_done.is_set():
            await self._private_reconnect_done.wait()
            return
        if self._fatal_reconnect_failure:
            return
        self._private_reconnect_done.clear()
        self._private_in_reconnect = True
        try:
            try:
                if self._ws_private is not None:
                    await self._ws_private.close()
            except Exception:
                pass
            self._ws_private = None
            for attempt in range(1, MAX_RECONNECT_ATTEMPTS + 1):
                try:
                    self._ws_token = await self._rest_get_ws_token()
                    self._ws_private = await self._ws_connect_one(
                        PRIVATE_WS_URI
                    )
                    self._last_real_data_time_private = time.monotonic()
                    self._last_pong_time_private = time.monotonic()
                    await self._subscribe_private(
                        "executions",
                        extra={
                            "token": self._ws_token,
                            "snap_orders": True,
                            "snap_trades": True,
                            "order_status": True,
                            "ratecounter": True,
                        },
                    )
                    await self._subscribe_private(
                        "balances",
                        extra={"token": self._ws_token, "snapshot": True},
                    )
                    self.executions_last_seq = 0
                    self.balances_last_seq = 0
                    open_orders = await self._rest_get_open_orders()
                    await self._reconcile_pending_orders(open_orders)
                    self._private_in_reconnect = False
                    self._log("RECONNECT_COMPLETE", level="INFO",
                              endpoint="private", attempts=attempt)
                    return
                except Exception as e:
                    self._log("RECONNECT_ATTEMPT_FAILED", level="WARN",
                              endpoint="private", attempt=attempt,
                              error=str(e))
                    backoff = min(
                        RECONNECT_BACKOFF_CAP_SEC,
                        RECONNECT_BACKOFF_BASE_SEC
                        * (RECONNECT_BACKOFF_FACTOR ** (attempt - 1)),
                    )
                    await asyncio.sleep(backoff)
            self._fatal_reconnect_failure = True
            self._log("FATAL_RECONNECT_FAILURE", level="CRITICAL",
                      endpoint="private")
            try:
                self._alert_fn(
                    "CRITICAL", "FATAL_RECONNECT_FAILURE private",
                )
            except Exception:
                pass
            raise RuntimeError("FATAL_RECONNECT_FAILURE private")
        finally:
            self._private_reconnect_done.set()


# ============================================================
# Module entry point - kept minimal; tothbot.__main__ wires
# Logger, Execution Engine, Exit Controller, and CIATS bus.
# ============================================================


async def _amain() -> None:
    """Standalone harness - not used in production wire-up."""
    mgr = WSManager(config={"paper_trading_mode": True})
    await mgr.run()


if __name__ == "__main__":
    asyncio.run(_amain())
