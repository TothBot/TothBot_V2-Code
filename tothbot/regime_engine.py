"""
DocDCN:     1011012
DocTitle:   Regime_Engine
DocVersion: dv1_4
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/tothbot/regime_engine.py
DocDate:    04-12-2026
DocTime:    23:59:59 UTC

============================================================
REVISION HISTORY
============================================================

  dv1_4   04-12-2026  DC header added per 0311001 v1_1, 0311004 v1_1,
                      1011001 dv1_7. No code logic changes.

  dv1_4   04-05-2026  Initial Phase 8 implementation.
                      Written to 1011012 Regime_Engine_Coding_Spec dv1_4.

============================================================

Classifies market regime for each pair daily at 00:00 UTC.
Regime classification is the highest-priority correctness
requirement in the system. An incorrect regime causes loss.

Six regimes (directional x volatility):
  TRENDING_POSITIVE + NORMAL_VOL     <- ENTRY ALLOWED
  TRENDING_POSITIVE + ELEVATED_VOL   <- ENTRY ALLOWED (50% size)
  NON_DIRECTIONAL   + NORMAL_VOL     <- NO ENTRY
  NON_DIRECTIONAL   + ELEVATED_VOL   <- NO ENTRY
  TRENDING_NEGATIVE + NORMAL_VOL     <- NO ENTRY
  TRENDING_NEGATIVE + ELEVATED_VOL   <- NO ENTRY

Hard Rules:
  ALWAYS exclude response[-1] (uncommitted candle).
  ALWAYS use committed_candles = candles[:-1].
  ALWAYS stagger 1.1s between pair REST calls.
  NEVER use asyncio.gather() for regime OHLC calls (RE-RL-003).
  NEVER halt on regime failure -- use stale (RE-SCH-003).
  ADX batch computation on full 719-candle history (RE-FPD-005).
  NEVER compute ADX incrementally in Regime Engine.
  BTC/USD ALWAYS in trading universe (RE-TAG-002).
  regime_cache updated atomically per pair.
  Stale regime: use prior day, log REGIME_STALE_WARNING.
============================================================
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import aiohttp
import orjson

from tothbot.logger import _alert_operator_direct, log_record

# =============================================================
# CONSTANTS -- CIATS-owned starting values
# =============================================================

ADX_THRESHOLD: Decimal = Decimal("25")
ATR_PERCENTILE_WINDOW: int = 50
ATR_PERCENTILE_MIN: int = 30
EMA_PERIOD_FAST: int = 20
EMA_PERIOD_SLOW: int = 50

OHLC_STAGGER_SEC: float = 1.1
MAX_RETRIES: int = 3
RETRY_BACKOFF: list[float] = [2.0, 4.0, 8.0]
STALE_ALERT_HOURS: float = 24.0

REST_TIMEOUT_TOTAL: int = 10
REST_TIMEOUT_CONNECT: int = 5
REST_TIMEOUT_SOCK_READ: int = 8
REST_CONNECTOR_LIMIT: int = 10
REST_CONNECTOR_LIMIT_PER_HOST: int = 5

TRENDING_POSITIVE: str = "TRENDING_POSITIVE"
TRENDING_NEGATIVE: str = "TRENDING_NEGATIVE"
NON_DIRECTIONAL: str = "NON_DIRECTIONAL"
NORMAL_VOL: str = "NORMAL_VOL"
ELEVATED_VOL: str = "ELEVATED_VOL"

REST_BASE_URL: str = "https://api.kraken.com"


# =============================================================
# REGIME STATE DATACLASS -- RE-TAX-002
# =============================================================

@dataclass
class RegimeState:
    """Complete regime classification for one pair."""
    directional:  str
    vol_regime:   str
    adx_14:       Decimal
    ema_20:       Decimal
    ema_50:       Decimal
    atr_daily:    Decimal
    atr_pct_rank: Decimal
    computed_at:  str


# =============================================================
# REGIME ENGINE
# =============================================================

class RegimeEngine:
    """
    TothBot V2 Regime Engine.

    Computes daily market regime for all pairs at 00:00 UTC.
    Reads from Kraken REST GetOHLCData (interval=1440).
    Stores results in regime_cache.

    Injected dependencies:
        logger:           logging.Logger ("tothbot" instance)
        data_api_key:     str -- Kraken DATA API key (not TRADE)
        trading_universe: list[str] -- all monitored pair symbols
        param_store:      dict -- CIATS parameter snapshot
    """

    def __init__(
        self,
        logger: Any,
        data_api_key: str,
        trading_universe: list[str],
        param_store: dict | None = None,
    ) -> None:
        self._logger = logger
        self._data_api_key = data_api_key
        self._universe = trading_universe
        self._params: dict = param_store or {}
        self.regime_cache: dict[str, RegimeState] = {}
        self._midnight_task: asyncio.Task | None = None

    # =============================================================
    # PUBLIC INTERFACE
    # =============================================================

    def get_regime(self, symbol: str) -> RegimeState | None:
        """O(1) regime cache lookup. None -> Gate 3 blocks entry."""
        return self.regime_cache.get(symbol)

    def update_param_store(self, param_store: dict) -> None:
        self._params = param_store

    async def run_daily_computation(
        self,
        universe: list[str] | None = None,
    ) -> None:
        """
        Compute regime for all pairs sequentially with 1.1s stagger.
        NEVER asyncio.gather() (RE-RL-003).
        On failure: use stale, never halt (RE-SCH-003).
        """
        pairs = list(universe or self._universe)
        if "BTC/USD" not in pairs:
            pairs.append("BTC/USD")

        for pair in pairs:
            try:
                candles = await self._fetch_daily_ohlc(pair)
                if candles:
                    self._compute_and_cache(pair, candles)
            except Exception as exc:  # noqa: broad -- RE-SCH-003 never halt
                self._logger.warning(log_record({
                    "event":     "WARMUP_REST_FAILED",
                    "level":     "WARN",
                    "component": "REGIME_ENG",
                    "pair":      pair,
                    "interval":  1440,
                    "error":     str(exc),
                }))
                self._check_stale(pair)

            # RE-RL-002: MANDATORY 1.1s stagger
            await asyncio.sleep(OHLC_STAGGER_SEC)

    async def schedule_midnight_task(self) -> None:
        """Start persistent daily midnight task (RE-SCH-001)."""
        self._midnight_task = asyncio.create_task(
            self._midnight_loop(), name="regime_midnight_task"
        )

    # =============================================================
    # GATE HELPERS
    # =============================================================

    def gate_3_check(self, symbol: str) -> str:
        """
        Gate 3 Regime Pre-Filter. RE-TAX-003.
        Returns: "PASS" | "STOP" | "BLOCK"
        """
        state = self.regime_cache.get(symbol)
        if state is None:
            return "BLOCK"
        if state.directional == TRENDING_NEGATIVE:
            return "STOP"
        if state.directional == NON_DIRECTIONAL and state.vol_regime == ELEVATED_VOL:
            return "STOP"
        return "PASS"

    def gate_6_modifier(self, symbol: str) -> Decimal:
        """
        Gate 6 sizing modifier.
        TRENDING_POS + ELEVATED_VOL -> 0.50. All others -> 1.00.
        """
        state = self.regime_cache.get(symbol)
        if state is None:
            return Decimal("1.0")
        if state.directional == TRENDING_POSITIVE and state.vol_regime == ELEVATED_VOL:
            return Decimal("0.50")
        return Decimal("1.0")

    # =============================================================
    # MIDNIGHT LOOP -- RE-SCH-001
    # =============================================================

    async def _midnight_loop(self) -> None:
        while True:
            await asyncio.sleep(self._seconds_until_midnight_utc())
            self._logger.info(log_record({
                "event":       "REGIME_DAILY_TRIGGER",
                "level":       "INFO",
                "component":   "REGIME_ENG",
                "computed_at": datetime.now(timezone.utc).isoformat(),
            }))
            await self.run_daily_computation()
            await asyncio.sleep(60)  # avoid re-trigger at midnight edge

    @staticmethod
    def _seconds_until_midnight_utc() -> float:
        now = datetime.now(timezone.utc)
        midnight = now.replace(
            hour=0, minute=0, second=0, microsecond=0
        ) + timedelta(days=1)
        return (midnight - now).total_seconds()

    # =============================================================
    # REST DATA FETCH
    # =============================================================

    def _make_http_session(self) -> aiohttp.ClientSession:
        timeout = aiohttp.ClientTimeout(
            total=REST_TIMEOUT_TOTAL,
            connect=REST_TIMEOUT_CONNECT,
            sock_read=REST_TIMEOUT_SOCK_READ,
        )
        connector = aiohttp.TCPConnector(
            limit=REST_CONNECTOR_LIMIT,
            limit_per_host=REST_CONNECTOR_LIMIT_PER_HOST,
            force_close=False,
        )
        return aiohttp.ClientSession(timeout=timeout, connector=connector)

    async def _fetch_daily_ohlc(self, pair: str) -> list[dict] | None:
        """
        Fetch daily OHLC. Returns committed_candles = response[:-1].
        RE-OHLC-002: ALWAYS exclude last candle.
        RE-RL-004: retry on 429 with backoff.
        """
        for attempt in range(MAX_RETRIES):
            try:
                async with self._make_http_session() as session:
                    resp = await session.get(
                        f"{REST_BASE_URL}/0/public/OHLC",
                        params={"pair": pair, "interval": 1440},
                    )
                    if resp.status == 429:
                        backoff = RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                        self._logger.warning(log_record({
                            "event":       "REGIME_RATE_LIMIT_HIT",
                            "level":       "WARN",
                            "component":   "REGIME_ENG",
                            "pair":        pair,
                            "retry_count": attempt + 1,
                            "backoff_sec": Decimal(str(backoff)),
                        }))
                        await asyncio.sleep(backoff)
                        continue

                    data = orjson.loads(await resp.read())
                    if data.get("error"):
                        raise RuntimeError(f"OHLC error {pair}: {data['error']}")

                    pair_key = list(data["result"].keys())[0]
                    # RE-OHLC-002: exclude last (uncommitted) candle
                    committed = data["result"][pair_key][:-1]
                    return self._parse_candles(committed)

            except Exception as exc:  # noqa: broad -- retry logic
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(
                        RETRY_BACKOFF[min(attempt, len(RETRY_BACKOFF) - 1)]
                    )
                else:
                    raise exc
        return None

    @staticmethod
    def _parse_candles(raw: list) -> list[dict]:
        """
        Kraken OHLC format: [time, open, high, low, close, vwap, volume, count]
        RE-OHLC-004: Decimal(str()) immediately.
        """
        return [
            {
                "time":   int(c[0]),
                "open":   Decimal(str(c[1])),
                "high":   Decimal(str(c[2])),
                "low":    Decimal(str(c[3])),
                "close":  Decimal(str(c[4])),
                "vwap":   Decimal(str(c[5])),
                "volume": Decimal(str(c[6])),
            }
            for c in raw
        ]

    # =============================================================
    # REGIME COMPUTATION
    # =============================================================

    def _compute_and_cache(self, pair: str, candles: list[dict]) -> None:
        """Compute regime and update cache atomically (RE-STL-003)."""
        adx   = self._compute_adx_14(candles)
        ema20 = self._compute_ema(candles, EMA_PERIOD_FAST)
        ema50 = self._compute_ema(candles, EMA_PERIOD_SLOW)
        atr_daily, atr_pct_rank = self._compute_atr_and_percentile(candles)

        if adx is None or ema20 is None or ema50 is None:
            self._check_stale(pair)
            return

        adx_thresh = Decimal(str(self._params.get("adx_threshold", ADX_THRESHOLD)))
        if adx > adx_thresh:
            directional = TRENDING_POSITIVE if ema20 > ema50 else TRENDING_NEGATIVE
        else:
            directional = NON_DIRECTIONAL

        vol_regime = ELEVATED_VOL if atr_pct_rank > Decimal("67") else NORMAL_VOL
        computed_at = datetime.now(timezone.utc).isoformat()

        self.regime_cache[pair] = RegimeState(
            directional=directional,
            vol_regime=vol_regime,
            adx_14=adx,
            ema_20=ema20,
            ema_50=ema50,
            atr_daily=atr_daily or Decimal("0"),
            atr_pct_rank=atr_pct_rank,
            computed_at=computed_at,
        )

        self._logger.info(log_record({
            "event":        "REGIME_COMPUTED",
            "level":        "INFO",
            "component":    "REGIME_ENG",
            "pair":         pair,
            "directional":  directional,
            "vol_regime":   vol_regime,
            "adx_14":       adx,
            "ema_20":       ema20,
            "ema_50":       ema50,
            "atr_daily":    atr_daily or Decimal("0"),
            "atr_pct_rank": atr_pct_rank,
            "computed_at":  computed_at,
        }))

    # =============================================================
    # INDICATORS
    # =============================================================

    @staticmethod
    def _compute_adx_14(candles: list[dict]) -> Decimal | None:
        """
        Batch ADX(14) -- Wilder's method on full history.
        RE-IND-006, RE-FPD-005. NEVER incremental.
        alpha = 1/N (Wilder, not standard EMA 2/(N+1)).
        """
        N = Decimal("14")
        N_int = 14
        if len(candles) < N_int * 2 + 1:
            return None

        plus_dm_list: list[Decimal] = []
        minus_dm_list: list[Decimal] = []
        tr_list: list[Decimal] = []

        prev_high  = candles[0]["high"]
        prev_low   = candles[0]["low"]
        prev_close = candles[0]["close"]

        for c in candles[1:]:
            H, L, C = c["high"], c["low"], c["close"]
            up   = H - prev_high
            down = prev_low - L
            plus_dm_list.append(
                up if up > down and up > Decimal("0") else Decimal("0")
            )
            minus_dm_list.append(
                down if down > up and down > Decimal("0") else Decimal("0")
            )
            tr_list.append(max(H - L, abs(H - prev_close), abs(L - prev_close)))
            prev_high, prev_low, prev_close = H, L, C

        if len(tr_list) < N_int:
            return None

        tr_s  = sum(tr_list[:N_int])
        pdm_s = sum(plus_dm_list[:N_int])
        mdm_s = sum(minus_dm_list[:N_int])
        dx_list: list[Decimal] = []

        for i in range(N_int, len(tr_list)):
            tr_s  = tr_s  - tr_s  / N + tr_list[i]
            pdm_s = pdm_s - pdm_s / N + plus_dm_list[i]
            mdm_s = mdm_s - mdm_s / N + minus_dm_list[i]
            pdi = pdm_s / tr_s * Decimal("100") if tr_s else Decimal("0")
            mdi = mdm_s / tr_s * Decimal("100") if tr_s else Decimal("0")
            denom = pdi + mdi
            dx_list.append(
                abs(pdi - mdi) / denom * Decimal("100")
                if denom else Decimal("0")
            )

        if len(dx_list) < N_int:
            return None

        adx = sum(dx_list[:N_int]) / N
        for dx in dx_list[N_int:]:
            adx = adx - adx / N + dx / N
        return adx

    @staticmethod
    def _compute_ema(candles: list[dict], period: int) -> Decimal | None:
        """EMA alpha = 2/(period+1). Warm-up = SMA of first N. RE-IND-007."""
        if len(candles) < period:
            return None
        closes = [c["close"] for c in candles]
        alpha = Decimal("2") / Decimal(str(period + 1))
        ema = sum(closes[:period]) / Decimal(str(period))
        for close in closes[period:]:
            ema = alpha * close + (Decimal("1") - alpha) * ema
        return ema

    @staticmethod
    def _compute_atr_and_percentile(
        candles: list[dict],
    ) -> tuple[Decimal | None, Decimal]:
        """Daily ATR(14) + percentile rank. RE-IND-001 through RE-IND-004."""
        if len(candles) < 15:
            return None, Decimal("50")

        tr_list: list[Decimal] = []
        prev_close = candles[0]["close"]
        for c in candles[1:]:
            tr_list.append(max(
                c["high"] - c["low"],
                abs(c["high"] - prev_close),
                abs(c["low"]  - prev_close),
            ))
            prev_close = c["close"]

        if len(tr_list) < 14:
            return None, Decimal("50")

        atr = sum(tr_list[:14]) / Decimal("14")
        atr_series: list[Decimal] = [atr]
        for tr in tr_list[14:]:
            atr = (atr * Decimal("13") + tr) / Decimal("14")
            atr_series.append(atr)

        latest_atr = atr_series[-1]
        window = atr_series[-ATR_PERCENTILE_WINDOW:]

        if len(window) < ATR_PERCENTILE_MIN:
            return latest_atr, Decimal("50")

        try:
            n_below = sum(1 for v in window if v <= latest_atr)
            pct_rank = Decimal(str(round(n_below / len(window) * 100, 1)))
        except Exception:  # noqa: broad -- fallback
            pct_rank = Decimal("50")

        return latest_atr, pct_rank

    # =============================================================
    # STALE HANDLING
    # =============================================================

    def _check_stale(self, pair: str) -> None:
        """Log stale regime. Alert if > 24h. NEVER halt (RE-SCH-003)."""
        state = self.regime_cache.get(pair)
        if state is None:
            self._logger.warning(log_record({
                "event":              "REGIME_STALE_WARNING",
                "level":              "WARN",
                "component":          "REGIME_ENG",
                "pair":               pair,
                "hours_since_update": Decimal("999"),
                "note":               "No prior data -- Gate 3 will block",
            }))
            return

        try:
            computed = datetime.fromisoformat(state.computed_at)
            if computed.tzinfo is None:
                computed = computed.replace(tzinfo=timezone.utc)
            hours_stale = Decimal(str(
                round((datetime.now(timezone.utc) - computed).total_seconds() / 3600, 2)
            ))
        except Exception:  # noqa: broad -- fallback
            hours_stale = Decimal("999")

        self._logger.warning(log_record({
            "event":              "REGIME_STALE_WARNING",
            "level":              "WARN",
            "component":          "REGIME_ENG",
            "pair":               pair,
            "hours_since_update": hours_stale,
        }))

        if float(hours_stale) > STALE_ALERT_HOURS:
            _alert_operator_direct(
                f"REGIME STALE: {pair} not updated for "
                f"{float(hours_stale):.1f}h. Using prior value."
            )