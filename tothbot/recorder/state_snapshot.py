"""The read-only STATE SNAPSHOT EMITTER (0500000 mod:Logger q5_logs STATE SNAPSHOT FILE, TB00793).

The local-file materialization of the C2 DAILY operational-dashboard VIEW: on the
contract:OHLC_5m_System_Clock cadence (the same clock that drives the periodic-pull scheduler,
THROTTLED to ~one write per `interval_sec` of wall clock, not one per 5m close per pair) the emitter
freezes the current organism state and writes it ATOMICALLY to a local JSON file (serialize to a
sibling temp file, then os.replace onto the target -- a reader never observes a partial object).

It is a VIEW over live in-memory state (FP8 single-source, no new data capture): open positions,
wallet balances, the per-pair 24h DECISION cache, the per-pair regime, and the CIATS per-module
summary. READ-ONLY -- it NEVER writes into the trading loop (rule:HR-PM-009 sole-writer preserved) and
is NOT authoritative for CIATS inference or tax records (the permanent trade-record file rule:HR-LG-013
remains the sole durable Stream 2 corpus). A failed emit is caught locally, never raised into the hot
path, and leaves the prior snapshot in place (the dashboard shows last-known state). Decimal-only on
the wire (every numeric is a JSON string, never a float -- mirrors the Logger NDJSON contract).

Consumed by a SEPARATE read-only localhost-bound dashboard process the operator reaches over an SSH
tunnel (operations/dashboard.py) -- no public port, no second writer.
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from datetime import datetime, timezone
from decimal import Decimal

from ..exchange.position_mirror import PositionSide

_SIDES = (PositionSide.LONG, PositionSide.SHORT)


def _d(value: object) -> "str | None":
    """Decimal/number -> string (the on-wire form); None passes through."""
    if value is None:
        return None
    try:
        return str(value)
    except Exception:  # pragma: no cover - defensive
        return None


class StateSnapshotEmitter:
    """Emit a read-only JSON snapshot of organism state on the OHLC_5m clock tick (throttled).

    All reads are best-effort + guarded: a missing/None surface degrades that field to null, never
    raises (the snapshot is a non-critical dashboard feed). The os edges are injected for tests."""

    def __init__(
        self,
        path: str,
        *,
        wm,
        conductors,
        decision_store,
        regime_cache,
        bbo_cache,
        warmups,
        mode: str = "paper",
        interval_sec: float = 45.0,
        now_wall: Callable[[], float] | None = None,
        _replace: Callable[[str, str], None] = os.replace,
        _open=open,
    ) -> None:
        self._path = path
        self._tmp = path + ".tmp"
        self._wm = wm
        self._conductors = conductors
        self._decision = decision_store
        self._regime = regime_cache
        self._bbo = bbo_cache
        self._warmups = warmups
        self._mode = mode
        self._interval = float(interval_sec)
        self._replace = _replace
        self._open = _open
        self._last_emit_ts: float | None = None  # the tick UTC instant (epoch seconds) of last write

    # --- the OHLC_5m clock-tick consumer (throttled) -------------------------------------------
    def on_tick(self, now_utc: datetime) -> None:
        """The injected clock-tick callback (mirrors PullCadenceScheduler.tick's signature). Emits a
        fresh snapshot at most once per `interval_sec` of clock time; never raises."""
        ts = now_utc.timestamp()
        if self._last_emit_ts is not None and 0.0 <= (ts - self._last_emit_ts) < self._interval:
            return  # throttled (a non-monotonic jump back re-emits, the bounded-miss is harmless)
        self._last_emit_ts = ts
        self.emit(now_utc)

    def emit(self, now_utc: datetime) -> None:
        """Build + atomically write one snapshot. Best-effort: any failure is swallowed (the prior
        snapshot stays in place); the trading hot path is never affected."""
        try:
            snapshot = self.build(now_utc)
            data = json.dumps(snapshot, default=str)
            with self._open(self._tmp, "w", encoding="utf-8") as fh:
                fh.write(data)
            self._replace(self._tmp, self._path)  # atomic swap onto the target
        except Exception:
            # READ-ONLY best-effort feed: never raise into the hot path; leave the prior snapshot.
            return

    # --- the VIEW builder (all reads guarded) --------------------------------------------------
    def build(self, now_utc: "datetime | None" = None) -> dict:
        now = now_utc or datetime.now(timezone.utc)
        return {
            "ts": now.isoformat(),
            "mode": self._mode,
            "health": self._health(now),
            "balances": self._balances(),
            "positions": self._positions(),
            "decision_board": self._decision_board(),
            "ciats": self._ciats(),
        }

    def _health(self, now: datetime) -> dict:
        warm = 0
        try:
            warm = len(self._warmups or {})
        except Exception:
            pass
        open_count = 0
        try:
            open_count = len(self._wm.open_positions())
        except Exception:
            pass
        return {
            "warm_pairs": warm,
            "open_position_count": open_count,
            "snapshot_ts": now.isoformat(),
        }

    def _balances(self) -> dict:
        out: dict = {}
        for side in _SIDES:
            wallet = baseline = None
            try:
                wallet = self._wm.wallet_balance(side)
            except Exception:
                pass
            try:
                baseline = self._wm.portfolio_baseline(side)
            except Exception:
                pass
            out[side.value] = {"wallet": _d(wallet), "baseline": _d(baseline)}
        return out

    def _stop_mult(self) -> Decimal:
        try:
            return Decimal(str(getattr(self._wm, "_decision_stop_mult", "2.5")))
        except Exception:  # pragma: no cover - defensive
            return Decimal("2.5")

    def _positions(self) -> list:
        rows: list = []
        try:
            positions = self._wm.open_positions()
        except Exception:
            return rows
        mult = self._stop_mult()
        for symbol, pos in positions.items():
            row = {"symbol": symbol}
            try:
                side = pos.side
                entry = pos.avg_entry_price
                qty = pos.qty
                atr = pos.atr_14_entry
                _sv = side.value if hasattr(side, "value") else str(side)
                row["side"] = _sv.upper()   # LONG / SHORT (matches the trades-file side convention)
                row["qty"] = _d(qty)
                row["entry"] = _d(entry)
                row["atr_daily"] = _d(atr)
                row["l3_emergsl"] = _d(getattr(pos, "emergsl_price", None))
                row["entry_ts"] = getattr(pos, "entry_timestamp_utc", None)
                # mark = the realizable exit quote (LONG sells at the bid, SHORT covers at the ask).
                mark = None
                quote = self._bbo.bbo(symbol) if self._bbo is not None else None
                if quote is not None:
                    bid, ask = quote
                    mark = bid if side is PositionSide.LONG else ask
                row["mark"] = _d(mark)
                # unrealized P/L (LONG: (mark-entry)*qty; SHORT: (entry-mark)*qty).
                if mark is not None and entry is not None and qty is not None:
                    delta = (mark - entry) if side is PositionSide.LONG else (entry - mark)
                    row["unrealized_usd"] = _d(delta * qty)
                # the WIDE layer:L2 stop level (entry -/+ decision_atr_stop_mult x daily ATR).
                if entry is not None and atr is not None:
                    off = mult * Decimal(str(atr))
                    row["l2_stop"] = _d(entry - off if side is PositionSide.LONG else entry + off)
            except Exception:
                pass
            rows.append(row)
        return rows

    def _decision_board(self) -> list:
        rows: list = []
        symbols = ()
        try:
            symbols = sorted(self._regime.symbols)
        except Exception:
            return rows
        for symbol in symbols:
            row = {"symbol": symbol}
            try:
                regime = self._regime.regime(symbol)
                row["regime"] = regime.value if hasattr(regime, "value") else (
                    str(regime) if regime is not None else None
                )
            except Exception:
                row["regime"] = None
            cache = None
            try:
                cache = self._decision.get(symbol) if self._decision is not None else None
            except Exception:
                cache = None
            if cache is not None:
                try:
                    fast, slow = cache.ema_fast_24h, cache.ema_slow_24h
                    row["ema_fast"] = _d(fast)
                    row["ema_slow"] = _d(slow)
                    row["bullish"] = bool(cache.bullish)
                    row["atr_daily"] = _d(cache.atr_14_24h)
                    row["close_24h"] = _d(cache.close_24h)
                    # gap_pct = (fast-slow)/slow*100, signed (positive = bullish margin, the
                    # distance-to-cross the dashboard sorts on; near 0 = about to cross).
                    if slow not in (None, Decimal("0")):
                        row["gap_pct"] = _d((fast - slow) / slow * Decimal("100"))
                except Exception:
                    pass
            else:
                row["bullish"] = None
            rows.append(row)
        return rows

    def _ciats(self) -> dict:
        out: dict = {}
        for side in _SIDES:
            summary: dict = {"trade_count": None, "win_rate": None, "net_rr": None,
                             "pending": None, "progress_to_floor": None}
            conductor = None
            try:
                conductor = self._conductors.get(side) if self._conductors is not None else None
            except Exception:
                conductor = None
            if conductor is not None:
                try:
                    summary["trade_count"] = int(conductor.trade_count)
                except Exception:
                    pass
                try:
                    summary["pending"] = len(conductor.pending)
                except Exception:
                    pass
                pool = getattr(conductor, "_pool", None)
                if pool is not None:
                    try:
                        summary["win_rate"] = _d(pool.win_rate)
                    except Exception:
                        pass
                    try:
                        summary["net_rr"] = _d(pool.net_reward_risk)
                    except Exception:
                        pass
                    floor = getattr(pool, "_trade_floor", 200)
                    tc = summary["trade_count"]
                    if tc is not None:
                        summary["progress_to_floor"] = f"{tc}/{floor}"
            out[side.value] = summary
        return out
