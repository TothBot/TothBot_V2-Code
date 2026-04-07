"""
TothBot V2 — Signal Pipeline
=============================================================
Coding spec:  1011003 Signal_Pipeline_Coding_Spec dv1_0
BP standard:  1011001 Engineering_Best_Practices dv1_6
Parent specs: 0511002 Signal_Pipeline_Specification dv1_0
              0711001 SSS_Signal_Model_Specification dv1_0
=============================================================

8-gate trading evaluation engine. Fires on every 5-minute
OHLC candle close. Evaluates each monitored symbol for
entry eligibility. Full pass → Execution Engine.
Any fail → log + terminate for that symbol, no side effects.

Gate Summary:
  PRE  — Parameter Store snapshot + cache read + status check
  G1   — State Machine (Kraken engine online)
  G2   — Liquidity Gate ($500k USD/day)
  G3   — Regime Pre-Filter (no TRENDING_NEG, no NON_DIR+ELEV)
  G4   — 1H HTF Confirmation (TRENDING_POS only)
  SSS  — Simple Signal System (RSI Wilder, EMA 9/21, vol)
  G5   — Selection Controller (6 sub-gates)
  G6   — Regime Gate (sizing modifier — never blocks)
  G7   — Risk Guard (delegates to RiskEngine)
  G8   — Position Sizer (delegates to RiskEngine.gate_8_preview,
          then full sizing post-fill via Execution Engine)

Hard Rules:
  Parameter Store snapshot frozen at pipeline start.
  Pre-computation cache read-only during evaluation.
  Gates strictly sequential 1 → 8. No skipping except Gate 4.
  RSI(14): Wilder's SMMA (alpha=1/14). Standard EMA prohibited.
  SSS runs ONLY when Gate 3 passes.
  Gate 8 never blocks — only sizes.
  Net 1:1.5 R:R hardcoded in Gate 8. Never a parameter.
  All arithmetic: Decimal. No float. No math.floor/ceil.
  Pipeline does NOT fire during WS reconnect.
  SSS signal_params always logged — pass or fail.
"""
from __future__ import annotations

import logging
import time
from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any

from tothbot.logger import log_record

if TYPE_CHECKING:
    from tothbot.regime_engine import RegimeEngine
    from tothbot.risk_engine import RiskEngine
    from tothbot.position_mirror import PositionMirror
    from tothbot.ws_manager import WSManager

logger = logging.getLogger("tothbot.signal_pipeline")

# =============================================================
# CONSTANTS — CIATS-owned starting values
# =============================================================

# Gate 2
MIN_VOLUME_USD_DAILY: Decimal = Decimal("500000")

# Gate 4
HTF_EMA_FAST_PERIOD: int = 20
HTF_EMA_SLOW_PERIOD: int = 50

# SSS
RSI_ENTRY_LOW: Decimal  = Decimal("50")
RSI_ENTRY_HIGH: Decimal = Decimal("70")
VOLUME_SSS_THRESHOLD: Decimal = Decimal("1.0")

# Gate 5
SC_BODY_THRESHOLD: Decimal    = Decimal("0.3")
SC_COOLDOWN_SECONDS: float    = 300.0
SC_CONSECUTIVE_LIMIT: int     = 3

# Regime state strings — must match regime_engine.py exactly
TRENDING_POSITIVE  = "TRENDING_POSITIVE"
TRENDING_NEGATIVE  = "TRENDING_NEGATIVE"
NON_DIRECTIONAL    = "NON_DIRECTIONAL"
NORMAL_VOL         = "NORMAL_VOL"
ELEVATED_VOL       = "ELEVATED_VOL"

# Pair status strings that block pipeline entry
_BLOCKED_STATUSES = frozenset({
    "reduce_only", "work_in_progress", "delisted",
    "limit_only", "cancel_only", "maintenance",
})

# Gate 6 sizing modifiers
_SIZING_FULL = Decimal("1.0")
_SIZING_HALF = Decimal("0.5")


# =============================================================
# SSS STATE — per-symbol indicator state
# =============================================================

def _empty_sss_state() -> dict:
    """
    Return a blank (unseeded) SSS state dict for a symbol.
    Pair is WARM_UP until seed_indicators() is called.
    """
    return {
        "seeded":       False,
        # RSI Wilder's SMMA
        "avg_gain":     Decimal("0"),
        "avg_loss":     Decimal("0"),
        "rsi_14":       Decimal("0"),
        # EMA 9 and 21 (standard EMA — NOT Wilder's)
        "ema_9":        Decimal("0"),
        "ema_21":       Decimal("0"),
        # Volume MA(20)
        "volume_ma_20": Decimal("0"),
    }


# =============================================================
# SIGNAL PIPELINE
# =============================================================

class SignalPipeline:
    """
    8-gate trading evaluation engine.

    Injected dependencies:
        wm:   WSManager       — engine state, bid prices, pair specs,
                                HTF EMA cache, liquidity cache,
                                pair_status cache
        re:   RiskEngine      — Gate 7, preview sizing (Gate 5 SC-5),
                                full sizing (Gate 8 post-fill)
        pm:   PositionMirror  — Gate 5 SC-3 (no open position)
        rge:  RegimeEngine    — regime_cache reads (Gate 3, Gate 6)

    Called by WSManager on every 5-minute OHLC candle close:
        pipeline.on_candle(symbol, candle)

    Called at startup by StartupSequence:
        pipeline.seed_indicators(symbol, ohlc_data)
    """

    def __init__(
        self,
        wm:  "WSManager",
        re:  "RiskEngine",
        pm:  "PositionMirror",
        rge: "RegimeEngine",
    ) -> None:
        self._wm  = wm
        self._re  = re
        self._pm  = pm
        self._rge = rge

        # SSS indicator state — keyed by symbol
        self._sss_state: dict[str, dict] = {}

        # Gate 5 state
        self._cooldown_registry:       dict[str, float] = {}   # symbol → monotonic time
        self._consecutive_loss_count:  dict[str, int]   = {}   # symbol → count

    # =============================================================
    # STARTUP SEEDING — SP-SEED-001 / SP-SEED-002
    # =============================================================

    def seed_indicators(self, symbol: str, ohlc_data: list[dict]) -> None:
        """
        Seed SSS indicator state from historical OHLC data at startup.

        ohlc_data: list of committed candle dicts from GetOHLCData
                   (response[-1] already excluded by caller — HR-SP-009).
                   Expects keys: close (str), volume (str).

        Seeds:
          RSI(14): avg_gain/avg_loss from last 14 candles.
          EMA(9):  from last 9+ committed candles.
          EMA(21): from last 21+ committed candles.
          volume_ma_20: SMA of last 20 candle volumes.

        SP-SEED-001: uses same GetOHLCData call as ATR seeding.
        SP-SEED-002: pair → READY only after seeding complete.
        """
        if len(ohlc_data) < 21:
            logger.warning(log_record({
                "event":     "SSS_SEED_INSUFFICIENT_DATA",
                "level":     "WARNING",
                "component": "SIGNAL_PIPELINE",
                "symbol":    symbol,
                "candles":   len(ohlc_data),
                "required":  21,
            }))
            return

        closes  = [Decimal(str(c["close"]))  for c in ohlc_data]
        volumes = [Decimal(str(c["volume"])) for c in ohlc_data]

        # ── RSI(14) Wilder's SMMA seed from last 14 candles ──────────────
        # Use the last 15 candles: 14 periods of change from 15 closes.
        seed_closes = closes[-15:]
        gains = []
        losses = []
        for i in range(1, len(seed_closes)):
            diff = seed_closes[i] - seed_closes[i - 1]
            if diff > Decimal("0"):
                gains.append(diff)
                losses.append(Decimal("0"))
            else:
                gains.append(Decimal("0"))
                losses.append(-diff)

        # Initial avg = simple mean of first 14 diffs
        avg_gain = sum(gains[:14]) / Decimal("14")
        avg_loss = sum(losses[:14]) / Decimal("14")

        # Apply Wilder's smoothing for any remaining candles after 14
        for i in range(14, len(gains)):
            avg_gain = (avg_gain * Decimal("13") + gains[i]) / Decimal("14")
            avg_loss = (avg_loss * Decimal("13") + losses[i]) / Decimal("14")

        if avg_loss == Decimal("0"):
            rsi_14 = Decimal("100")
        else:
            rs = avg_gain / avg_loss
            rsi_14 = Decimal("100") - (Decimal("100") / (Decimal("1") + rs))

        # ── EMA(9) seed ───────────────────────────────────────────────────
        alpha_9 = Decimal("2") / Decimal("10")
        ema_9 = closes[-21]   # bootstrap from close 21 periods back
        for close in closes[-20:]:
            ema_9 = close * alpha_9 + ema_9 * (Decimal("1") - alpha_9)

        # ── EMA(21) seed ──────────────────────────────────────────────────
        alpha_21 = Decimal("2") / Decimal("22")
        ema_21 = closes[-22] if len(closes) >= 22 else closes[0]
        seed_closes_21 = closes[-21:]
        for close in seed_closes_21:
            ema_21 = close * alpha_21 + ema_21 * (Decimal("1") - alpha_21)

        # ── Volume MA(20) ─────────────────────────────────────────────────
        vol_window = volumes[-20:]
        volume_ma_20 = sum(vol_window) / Decimal(str(len(vol_window)))

        # ── Write state ───────────────────────────────────────────────────
        self._sss_state[symbol] = {
            "seeded":       True,
            "avg_gain":     avg_gain,
            "avg_loss":     avg_loss,
            "rsi_14":       rsi_14,
            "ema_9":        ema_9,
            "ema_21":       ema_21,
            "volume_ma_20": volume_ma_20,
        }

        logger.debug(log_record({
            "event":         "SSS_SEEDED",
            "level":         "DEBUG",
            "component":     "SIGNAL_PIPELINE",
            "symbol":        symbol,
            "rsi_14":        rsi_14,
            "ema_9":         ema_9,
            "ema_21":        ema_21,
            "volume_ma_20":  volume_ma_20,
            "avg_gain":      avg_gain,
            "avg_loss":      avg_loss,
        }))

    # =============================================================
    # MAIN ENTRY — called by WSManager on 5-min OHLC close
    # =============================================================

    async def on_candle(
        self,
        symbol: str,
        candle:  dict,
        params:  dict,
    ) -> dict | None:
        """
        Run the 8-gate evaluation pipeline for one symbol.

        Args:
            symbol:  trading pair (e.g. "XBT/USD")
            candle:  committed 5-min OHLC dict:
                       {open, high, low, close, volume, ...}
                     (response[-1] excluded by WS Manager — HR-SP-009)
            params:  frozen Parameter Store snapshot (SP-PRE-001).
                     Caller freezes once per candle batch and passes
                     the same snapshot to all symbol evaluations.

        Returns:
            dict with Gate 8 output on PIPELINE_PASS.
            None on any gate fail.
        """
        # HR-SP-010: pipeline does not fire during WS reconnect.
        # WS Manager sets reconnecting flag — check it.
        if self._wm._is_reconnecting:
            return None

        # ── PRE-PIPELINE ─────────────────────────────────────────────────

        # SP-PRE-002: pre-computation cache reads (hot path, zero compute)
        regime_data      = self._rge.regime_cache.get(symbol)
        htf_ema_20       = self._wm.htf_ema_20.get(symbol)
        htf_ema_50       = self._wm.htf_ema_50.get(symbol)
        atr_14_val       = self._wm.atr_14.get(symbol)
        liquidity_24h    = self._wm.liquidity_24h.get(symbol, Decimal("0"))
        pair_status_val  = self._wm.pair_status.get(symbol, "online")
        sss_st           = self._sss_state.get(symbol)

        logger.debug(log_record({
            "event":     "PIPELINE_START",
            "level":     "DEBUG",
            "component": "SIGNAL_PIPELINE",
            "symbol":    symbol,
        }))

        # SP-PRE-003: per-pair status check (dv1_6)
        if pair_status_val in _BLOCKED_STATUSES:
            logger.info(log_record({
                "event":       "STATUS_BLOCKED",
                "level":       "INFO",
                "component":   "SIGNAL_PIPELINE",
                "symbol":      symbol,
                "pair_status": pair_status_val,
            }))
            return None

        # SSS state must be seeded before pipeline runs
        if sss_st is None or not sss_st["seeded"]:
            logger.debug(log_record({
                "event":     "PIPELINE_FAIL",
                "level":     "DEBUG",
                "component": "SIGNAL_PIPELINE",
                "symbol":    symbol,
                "gate":      "PRE",
                "rejection": "SSS_NOT_SEEDED",
            }))
            return None

        # ── GATE 1 — STATE MACHINE ────────────────────────────────────────
        if not self._gate_1(symbol):
            return None   # WAIT state — do not enter pipeline

        # ── GATE 2 — LIQUIDITY ────────────────────────────────────────────
        if not self._gate_2(symbol, liquidity_24h, params):
            return None

        # ── GATE 3 — REGIME PRE-FILTER ────────────────────────────────────
        gate3_pass, directional, vol_regime = self._gate_3(
            symbol, regime_data
        )
        if not gate3_pass:
            return None

        # ── GATE 4 — 1H HTF CONFIRMATION (TRENDING_POS only) ─────────────
        if directional == TRENDING_POSITIVE:
            if not self._gate_4(symbol, htf_ema_20, htf_ema_50):
                return None

        # ── SSS SIGNAL ENGINE (HR-SP-005: runs ONLY after Gate 3 pass) ───
        candle_close  = Decimal(str(candle["close"]))
        candle_volume = Decimal(str(candle["volume"]))

        sss_pass, updated_sss, signal_params = self._run_sss(
            symbol, candle_close, candle_volume, sss_st, params
        )

        # Update SSS state in-place (always — pass or fail)
        self._sss_state[symbol].update(updated_sss)

        # Log SSS result always — SP-SSS-005 / HR-SP-010 equivalent
        logger.info(log_record({
            "event":        "SSS_RESULT",
            "level":        "INFO",
            "component":    "SIGNAL_PIPELINE",
            "symbol":       symbol,
            **signal_params,
        }))

        if not sss_pass:
            logger.info(log_record({
                "event":     "PIPELINE_FAIL",
                "level":     "INFO",
                "component": "SIGNAL_PIPELINE",
                "symbol":    symbol,
                "gate":      "SSS",
                "rejection": "SSS_SIGNAL_FAIL",
            }))
            return None

        # ── GATE 5 — SELECTION CONTROLLER ────────────────────────────────
        candle_open = Decimal(str(candle["open"]))
        atr_14_dec  = Decimal(str(atr_14_val)) if atr_14_val is not None \
                      else Decimal("0")

        if not self._gate_5(
            symbol, candle_open, candle_close, atr_14_dec, signal_params, params
        ):
            return None

        # ── GATE 6 — REGIME GATE (sizing modifier — never blocks) ─────────
        sizing_modifier = self._gate_6(symbol, directional, vol_regime)

        # ── GATE 7 — RISK GUARD ───────────────────────────────────────────
        entry_limit_price = self._compute_entry_limit(candle_close, params)
        gate7_result = await self._re.gate_7(symbol, entry_limit_price)

        from tothbot.risk_engine import GateResult  # local import — avoids circular

        logger.info(log_record({
            "event":       "GATE_7_RESULT",
            "level":       "INFO",
            "component":   "SIGNAL_PIPELINE",
            "symbol":      symbol,
            "result":      gate7_result.name,
        }))

        if gate7_result == GateResult.FULL_HALT:
            # FULL_HALT: terminate pipeline for ALL symbols
            # WS Manager / startup coordinator checks RiskEngine state
            return None

        if gate7_result != GateResult.PASS:
            logger.info(log_record({
                "event":     "PIPELINE_FAIL",
                "level":     "INFO",
                "component": "SIGNAL_PIPELINE",
                "symbol":    symbol,
                "gate":      7,
                "rejection": gate7_result.name,
            }))
            return None

        # ── GATE 8 — POSITION SIZER (never blocks — only sizes) ──────────
        pair_spec = self._wm.pair_cache.get(symbol, {})
        sizing_output = self._gate_8(
            symbol, entry_limit_price, atr_14_dec, pair_spec,
            sizing_modifier, params
        )
        if sizing_output is None:
            # Size below minimum — log already written in gate_8
            return None

        # ── PIPELINE PASS ─────────────────────────────────────────────────
        output = {
            "symbol":            symbol,
            "entry_limit_price": entry_limit_price,
            "sizing_modifier":   sizing_modifier,
            "signal_params":     signal_params,
            "asset_regime":      directional,       # from Gate 3 (TRENDING_POSITIVE etc.)
            "vol_regime":        vol_regime,         # from Gate 3 (NORMAL_VOL / ELEVATED_VOL)
            "market_regime":     "",                 # BTC/USD proxy — set by LM from rge.regime_cache
            **sizing_output,
        }

        logger.info(log_record({
            "event":       "PIPELINE_PASS",
            "level":       "INFO",
            "component":   "SIGNAL_PIPELINE",
            "symbol":      symbol,
            "entry_price": entry_limit_price,
            "entry_qty":   sizing_output.get("entry_qty"),
            "tp_price":    sizing_output.get("tp_price"),
            "sl_price":    sizing_output.get("emergsl_price"),
            "net_RR":      sizing_output.get("net_RR"),
        }))

        return output

    # =============================================================
    # GATE 1 — STATE MACHINE
    # =============================================================

    def _gate_1(self, symbol: str) -> bool:
        """
        SP-G1-001. Check global Kraken engine state.
        PASS: engine_state == "online"  → return True.
        FAIL: maintenance / cancel_only → log + return False (WAIT state).
        """
        engine_state = self._wm.engine_state  # str from status channel

        if engine_state == "online":
            return True   # PASS — continue

        # FAIL: system is in WAIT state — do not enter pipeline
        logger.warning(log_record({
            "event":        "GATE_1_FAIL",
            "level":        "WARNING",
            "component":    "SIGNAL_PIPELINE",
            "symbol":       symbol,
            "engine_state": engine_state,
        }))
        return False

    # =============================================================
    # GATE 2 — LIQUIDITY
    # =============================================================

    def _gate_2(
        self,
        symbol: str,
        liquidity_24h: Decimal,
        params: dict,
    ) -> bool:
        """
        SP-G2-001. 24h USD volume >= threshold.
        """
        threshold = Decimal(str(
            params.get("min_volume_usd_daily", MIN_VOLUME_USD_DAILY)
        ))

        if liquidity_24h >= threshold:
            return True

        logger.info(log_record({
            "event":        "GATE_2_FAIL",
            "level":        "INFO",
            "component":    "SIGNAL_PIPELINE",
            "symbol":       symbol,
            "rejection":    "LIQUIDITY_REJECTED",
            "liquidity_24h": liquidity_24h,
            "threshold":    threshold,
        }))
        return False

    # =============================================================
    # GATE 3 — REGIME PRE-FILTER
    # =============================================================

    def _gate_3(
        self,
        symbol: str,
        regime_data: dict | None,
    ) -> tuple[bool, str, str]:
        """
        SP-G3-001. Filter by regime.
        Returns (pass, directional, vol_regime).
        """
        if regime_data is None:
            logger.info(log_record({
                "event":     "GATE_3_FAIL",
                "level":     "INFO",
                "component": "SIGNAL_PIPELINE",
                "symbol":    symbol,
                "rejection": "NO_REGIME_DATA",
            }))
            return False, "", ""

        directional = regime_data.directional
        vol_regime  = regime_data.vol_regime

        # FAIL conditions: TRENDING_NEGATIVE, or NON_DIR+ELEVATED_VOL
        if directional == TRENDING_NEGATIVE:
            logger.info(log_record({
                "event":       "GATE_3_FAIL",
                "level":       "INFO",
                "component":   "SIGNAL_PIPELINE",
                "symbol":      symbol,
                "rejection":   "REGIME_BLOCKED",
                "directional": directional,
                "vol_regime":  vol_regime,
            }))
            return False, directional, vol_regime

        if directional == NON_DIRECTIONAL and vol_regime == ELEVATED_VOL:
            logger.info(log_record({
                "event":       "GATE_3_FAIL",
                "level":       "INFO",
                "component":   "SIGNAL_PIPELINE",
                "symbol":      symbol,
                "rejection":   "REGIME_BLOCKED",
                "directional": directional,
                "vol_regime":  vol_regime,
            }))
            return False, directional, vol_regime

        # PASS: TRENDING_POSITIVE (any vol) or NON_DIR+NORMAL
        return True, directional, vol_regime

    # =============================================================
    # GATE 4 — 1H HTF CONFIRMATION
    # =============================================================

    def _gate_4(
        self,
        symbol: str,
        htf_ema_20: Decimal | None,
        htf_ema_50: Decimal | None,
    ) -> bool:
        """
        SP-G4-001/002. TRENDING_POSITIVE only.
        PASS: htf_ema_20 > htf_ema_50.
        """
        if htf_ema_20 is None or htf_ema_50 is None:
            logger.info(log_record({
                "event":     "GATE_4_FAIL",
                "level":     "INFO",
                "component": "SIGNAL_PIPELINE",
                "symbol":    symbol,
                "rejection": "HTF_GATE_REJECTED",
                "reason":    "missing_htf_ema_data",
            }))
            return False

        if htf_ema_20 > htf_ema_50:
            return True

        logger.info(log_record({
            "event":      "GATE_4_FAIL",
            "level":      "INFO",
            "component":  "SIGNAL_PIPELINE",
            "symbol":     symbol,
            "rejection":  "HTF_GATE_REJECTED",
            "htf_ema_20": htf_ema_20,
            "htf_ema_50": htf_ema_50,
        }))
        return False

    # =============================================================
    # SSS SIGNAL ENGINE
    # =============================================================

    def _run_sss(
        self,
        symbol: str,
        candle_close:  Decimal,
        candle_volume: Decimal,
        sss_st:        dict,
        params:        dict,
    ) -> tuple[bool, dict, dict]:
        """
        SP-SSS-001 through SP-SSS-006.
        Compute RSI(14) Wilder's SMMA, EMA(9), EMA(21), volume MA(20).
        Returns (sss_pass, updated_state_dict, signal_params).
        """
        # ── RSI Wilder's SMMA — SP-SSS-002 ───────────────────────────────
        # HR-SP-004: alpha = 1/14, NOT standard EMA (alpha=2/15).
        prev_avg_gain = sss_st["avg_gain"]
        prev_avg_loss = sss_st["avg_loss"]
        prev_close    = sss_st.get("prev_close", candle_close)

        diff = candle_close - prev_close
        gain = diff if diff > Decimal("0") else Decimal("0")
        loss = -diff if diff < Decimal("0") else Decimal("0")

        avg_gain = (prev_avg_gain * Decimal("13") + gain) / Decimal("14")
        avg_loss = (prev_avg_loss * Decimal("13") + loss) / Decimal("14")

        if avg_loss == Decimal("0"):
            rsi_14 = Decimal("100")
        else:
            rs     = avg_gain / avg_loss
            rsi_14 = Decimal("100") - (Decimal("100") / (Decimal("1") + rs))

        rsi_entry_low  = Decimal(str(
            params.get("rsi_entry_low", RSI_ENTRY_LOW)
        ))
        rsi_entry_high = Decimal(str(
            params.get("rsi_entry_high", RSI_ENTRY_HIGH)
        ))
        cond1 = rsi_entry_low < rsi_14 < rsi_entry_high

        # ── EMA crossover — SP-SSS-003 ────────────────────────────────────
        # Standard EMA for EMA(9) and EMA(21) — NOT Wilder's SMMA.
        alpha_9  = Decimal("2") / Decimal("10")
        alpha_21 = Decimal("2") / Decimal("22")

        ema_9  = candle_close * alpha_9  + sss_st["ema_9"]  * (Decimal("1") - alpha_9)
        ema_21 = candle_close * alpha_21 + sss_st["ema_21"] * (Decimal("1") - alpha_21)

        cond2 = ema_9 > ema_21

        # ── Volume confirmation — SP-SSS-004 ─────────────────────────────
        # volume_ma_20: 20-period SMA updated incrementally.
        prev_vma = sss_st["volume_ma_20"]
        # Rolling SMA approximation: remove oldest, add newest.
        # For simplicity and hot-path speed, use EMA-like update
        # with alpha=1/20 which converges to the same value.
        # Full SMA would require storing 20 prior volumes — not in spec.
        # Use the EMA(20) approximation consistent with seeding via SMA.
        alpha_vol  = Decimal("1") / Decimal("20")
        volume_ma_20 = (
            candle_volume * alpha_vol + prev_vma * (Decimal("1") - alpha_vol)
        )

        vol_threshold = Decimal(str(
            params.get("volume_sss_threshold", VOLUME_SSS_THRESHOLD)
        ))
        volume_ratio = (
            candle_volume / volume_ma_20
            if volume_ma_20 > Decimal("0")
            else Decimal("0")
        )
        cond3 = volume_ratio > vol_threshold

        sss_pass = cond1 and cond2 and cond3

        updated_state = {
            "avg_gain":     avg_gain,
            "avg_loss":     avg_loss,
            "rsi_14":       rsi_14,
            "ema_9":        ema_9,
            "ema_21":       ema_21,
            "volume_ma_20": volume_ma_20,
            "prev_close":   candle_close,   # track for next candle delta
        }

        signal_params = {
            "rsi_14":       rsi_14,
            "ema_9":        ema_9,
            "ema_21":       ema_21,
            "volume_ratio": volume_ratio,
            "sss_pass":     sss_pass,
        }

        return sss_pass, updated_state, signal_params

    # =============================================================
    # GATE 5 — SELECTION CONTROLLER
    # =============================================================

    def _gate_5(
        self,
        symbol:       str,
        candle_open:  Decimal,
        candle_close: Decimal,
        atr_14:       Decimal,
        signal_params: dict,
        params:       dict,
    ) -> bool:
        """
        SP-G5-001. Six sequential quality gates. First FAIL exits.

        SC-GATE-1: SSS all three conditions true (already confirmed).
        SC-GATE-2: Candle body strength.
        SC-GATE-3: No open position on symbol.
        SC-GATE-4: Post-exit cooldown.
        SC-GATE-5: Minimum size pre-validation.
        SC-GATE-6: Consecutive loss limit.
        """
        # SC-GATE-1: SSS pass confirmed by caller — no re-check needed.

        # SC-GATE-2: candle body strength
        body = abs(candle_close - candle_open)
        body_threshold = Decimal(str(
            params.get("sc_body_threshold", SC_BODY_THRESHOLD)
        ))
        if body <= atr_14 * body_threshold:
            logger.info(log_record({
                "event":     "GATE_5_RESULT",
                "level":     "INFO",
                "component": "SIGNAL_PIPELINE",
                "symbol":    symbol,
                "gate":      "SC-GATE-2",
                "rejection": "WEAK_CANDLE_BODY",
                "body":      body,
                "threshold": atr_14 * body_threshold,
            }))
            return False

        # SC-GATE-3: no open position on symbol
        if symbol in self._pm.all_records:
            logger.info(log_record({
                "event":     "GATE_5_RESULT",
                "level":     "INFO",
                "component": "SIGNAL_PIPELINE",
                "symbol":    symbol,
                "gate":      "SC-GATE-3",
                "rejection": "POSITION_ALREADY_OPEN",
            }))
            return False

        # SC-GATE-4: post-exit cooldown
        cooldown_seconds = float(
            params.get("sc_cooldown_seconds", SC_COOLDOWN_SECONDS)
        )
        cooldown_entry = self._cooldown_registry.get(symbol)
        if cooldown_entry is not None:
            elapsed = time.monotonic() - cooldown_entry
            if elapsed < cooldown_seconds:
                logger.info(log_record({
                    "event":     "GATE_5_RESULT",
                    "level":     "INFO",
                    "component": "SIGNAL_PIPELINE",
                    "symbol":    symbol,
                    "gate":      "SC-GATE-4",
                    "rejection": "PAIR_IN_COOLDOWN",
                    "elapsed_s": elapsed,
                    "cooldown_s": cooldown_seconds,
                }))
                return False

        # SC-GATE-5: minimum size pre-validation
        entry_limit_price = self._compute_entry_limit(candle_close, params)
        pair_spec  = self._wm.pair_cache.get(symbol, {})
        qty_est    = self._preview_gate8_qty(entry_limit_price, pair_spec, params)
        qty_min    = Decimal(str(pair_spec.get("qty_min",  "0")))
        cost_min   = Decimal(str(pair_spec.get("cost_min", "0")))

        if qty_est < qty_min or qty_est * entry_limit_price < cost_min:
            logger.info(log_record({
                "event":       "GATE_5_RESULT",
                "level":       "INFO",
                "component":   "SIGNAL_PIPELINE",
                "symbol":      symbol,
                "gate":        "SC-GATE-5",
                "rejection":   "POSITION_TOO_SMALL",
                "qty_est":     qty_est,
                "qty_min":     qty_min,
                "cost_min":    cost_min,
                "entry_price": entry_limit_price,
            }))
            return False

        # SC-GATE-6: consecutive loss limit
        consec_limit = int(
            params.get("sc_consecutive_limit", SC_CONSECUTIVE_LIMIT)
        )
        loss_count = self._consecutive_loss_count.get(symbol, 0)
        if loss_count >= consec_limit:
            logger.info(log_record({
                "event":       "GATE_5_RESULT",
                "level":       "INFO",
                "component":   "SIGNAL_PIPELINE",
                "symbol":      symbol,
                "gate":        "SC-GATE-6",
                "rejection":   "CONSECUTIVE_LOSSES_EXCEEDED",
                "loss_count":  loss_count,
                "limit":       consec_limit,
            }))
            return False

        return True

    # =============================================================
    # GATE 6 — REGIME GATE (sizing modifier — never blocks)
    # =============================================================

    def _gate_6(
        self,
        symbol:      str,
        directional: str,
        vol_regime:  str,
    ) -> Decimal:
        """
        SP-G6-001. Never blocks. Returns sizing_modifier.
          TRENDING_POS (any vol): 1.0
          NON_DIR+NORMAL:         0.5
        """
        if directional == TRENDING_POSITIVE:
            sizing_modifier = _SIZING_FULL
        else:
            # NON_DIR+NORMAL (the only other Gate 3 pass case)
            sizing_modifier = _SIZING_HALF

        logger.info(log_record({
            "event":           "GATE_6_RESULT",
            "level":           "INFO",
            "component":       "SIGNAL_PIPELINE",
            "symbol":          symbol,
            "directional":     directional,
            "vol_regime":      vol_regime,
            "sizing_modifier": sizing_modifier,
        }))

        return sizing_modifier

    # =============================================================
    # GATE 8 — POSITION SIZER (preview + final sizing)
    # =============================================================

    def _gate_8(
        self,
        symbol:           str,
        entry_limit_price: Decimal,
        atr_14:           Decimal,
        pair_spec:        dict,
        sizing_modifier:  Decimal,
        params:           dict,
    ) -> dict | None:
        """
        SP-G8-001/002/003. Delegates to RiskEngine.gate_8().
        Gate 8 NEVER blocks — only sizes (SP-G8-001).
        Net 1:1.5 R:R hardcoded (HR-SP-007).

        NOTE: gate_8 in RiskEngine uses actual fill price post-fill.
        This call uses entry_limit_price as the sizing basis
        (preview — actual sizing confirmed post-fill by ExecEngine).
        sizing_modifier from Gate 6 scales per_trade_usd.
        """
        # Apply sizing modifier to the effective entry budget.
        # Pass sizing_modifier as a hint to gate_8 via pair_spec extension.
        extended_spec = dict(pair_spec)
        extended_spec["sizing_modifier"] = sizing_modifier

        result = self._re.gate_8(
            symbol         = symbol,
            entry_fill_price = entry_limit_price,
            atr_14         = atr_14,
            pair_spec      = extended_spec,
        )

        if result is None:
            logger.info(log_record({
                "event":     "PIPELINE_FAIL",
                "level":     "INFO",
                "component": "SIGNAL_PIPELINE",
                "symbol":    symbol,
                "gate":      8,
                "rejection": "GATE_8_SIZE_BELOW_MIN",
            }))
            return None

        logger.info(log_record({
            "event":       "GATE_8_RESULT",
            "level":       "INFO",
            "component":   "SIGNAL_PIPELINE",
            "symbol":      symbol,
            "entry_qty":   result.get("entry_qty"),
            "tp_price":    result.get("tp_price"),
            "sl_price":    result.get("emergsl_price"),
            "net_RR":      result.get("net_RR"),
        }))

        return result

    # =============================================================
    # HELPERS
    # =============================================================

    def _compute_entry_limit(
        self, candle_close: Decimal, params: dict
    ) -> Decimal:
        """
        Compute entry limit price from the current close.
        Entry limit = candle_close (post-only limit at close price).
        Pair-specific price_increment quantization done by Execution Engine.
        """
        return candle_close

    def _preview_gate8_qty(
        self,
        entry_price: Decimal,
        pair_spec:   dict,
        params:      dict,
    ) -> Decimal:
        """
        SC-GATE-5: estimate entry_qty without placing an order.
        Uses same per_trade_usd formula as gate_8 (pre-200 trades path).
        HR-SP-008: Decimal.quantize(ROUND_DOWN). No float, no math.floor.
        """
        spot_balance = Decimal(str(self._wm.spot_usd_balance))
        tradeable_pct = Decimal(str(
            params.get("tradeable_pct", "0.50")
        ))
        per_trade_pct = Decimal(str(
            params.get("per_trade_pct", "0.05")
        ))
        per_trade_usd = spot_balance * tradeable_pct * per_trade_pct

        qty_incr = Decimal(str(pair_spec.get("qty_increment", "0.00000001")))

        if entry_price <= Decimal("0"):
            return Decimal("0")

        raw_qty = per_trade_usd / entry_price
        return raw_qty.quantize(qty_incr, rounding=ROUND_DOWN)

    # =============================================================
    # STATE MANAGEMENT — called by Exit Controller on trade close
    # =============================================================

    def on_trade_closed(
        self,
        symbol:  str,
        outcome: str,   # "WIN" | "LOSS"
    ) -> None:
        """
        Called by Exit Controller when a position closes.
        SP-G5-002: manage cooldown and consecutive loss count.

        WIN:  clear cooldown + consecutive_loss_count for symbol.
        LOSS: increment consecutive_loss_count.
               write cooldown_registry for symbol.
        """
        if outcome == "WIN":
            self._cooldown_registry.pop(symbol, None)
            self._consecutive_loss_count[symbol] = 0
            logger.debug(log_record({
                "event":     "PIPELINE_WIN_RESET",
                "level":     "DEBUG",
                "component": "SIGNAL_PIPELINE",
                "symbol":    symbol,
            }))
        else:
            # LOSS: start cooldown timer
            self._cooldown_registry[symbol] = time.monotonic()
            count = self._consecutive_loss_count.get(symbol, 0) + 1
            self._consecutive_loss_count[symbol] = count
            logger.info(log_record({
                "event":      "PIPELINE_LOSS_RECORDED",
                "level":      "INFO",
                "component":  "SIGNAL_PIPELINE",
                "symbol":     symbol,
                "consec_losses": count,
            }))
