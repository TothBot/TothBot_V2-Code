"""mod:WS_Manager - the ar:AR-030 per-pair order-rate-counter unit (RL-MON-002/003).

Source: 0500000 dv1_252 sec 7 mod:WS_Manager (ar:AR-030: the rate-counter operative
ceiling per pair = maxratecount extracted from the executions subscription ACK, NEVER
hardcode 125; the D1 RATE-COUNTER PROTECTIVE RESPONSE two-tier RL-MON-002 WARNING /
RL-MON-003 CRITICAL) + sec 2.4 A-1 (executions ratecounter:true delivers the real-time
rate counter per pair) + sec 7 event_registry WS_MGR (MAXRATECOUNT_SET / RATE_COUNTER_UPDATE
/ RATE_COUNTER_WARNING) + section-9 registry seeds rl_warning_threshold_pct=0.80 /
rl_critical_threshold_pct=0.95 (config/registry.py; canonical TB00000 sec 8).

PURE + CLOCK-FREE: Kraken delivers the live per-pair rate counter on the executions feed
(A-1), and the engine-side decay (2.34/s, sec 2.3) happens on Kraken - this unit only
REFLECTS each pushed value against the operative ceiling. No wall clock, no I/O, no asyncio;
the feed edge (the executions ACK + the per-frame ratecount) is injected by mod:WS_Manager
(exchange/private_ws), exactly the "clock-free counter; the feed edge injected" contract.

  set_ceiling(maxratecount)  the executions ACK operative ceiling (AR-030)  -> MAXRATECOUNT_SET
  observe(symbol, value)     one pushed per-pair counter value (A-1)        -> RATE_COUNTER_UPDATE
                             value exceeds the warning fraction (RL-MON-002) -> + RATE_COUNTER_WARNING

The RL-MON-003 CRITICAL tier (entry-order suppression to preserve the exit rate budget) is a
PURE PREDICATE here - is_entry_suppressed(symbol) - that the order-dispatch gate reads; this
unit emits NO critical event (none is registered; LG-EVT-001 rejects unregistered codes - the
RL_CRITICAL prose at RL-MON-003 has no event_registry entry, so suppression is an ACTION, not
a logged event). The latch has hysteresis: it ARMS when the counter exceeds the critical
fraction and RELEASES only when it decays back to/below the warning fraction (RL-MON-003).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from ..config import registry


# --- canonical Logger events (WS_MGR family; event_registry codes) ------------

@dataclass(frozen=True)
class MaxRateCountSet:
    """MAXRATECOUNT_SET [INFO] {value} - the operative per-pair rate-counter ceiling
    was set from the executions subscription ACK (AR-030; never the hardcoded 125)."""

    value: int
    code: str = field(default="MAXRATECOUNT_SET", init=False)


@dataclass(frozen=True)
class RateCounterUpdate:
    """RATE_COUNTER_UPDATE [INFO] {symbol, value, maxratecount} - a fresh per-pair rate
    counter value arrived on the executions feed (A-1). maxratecount is the operative
    ceiling (None until the executions ACK sets it)."""

    symbol: str
    value: int
    maxratecount: int | None
    code: str = field(default="RATE_COUNTER_UPDATE", init=False)


@dataclass(frozen=True)
class RateCounterWarning:
    """RATE_COUNTER_WARNING [WARNING] {symbol, value, maxratecount} - the per-pair rate
    counter exceeded the rl_warning_threshold_pct fraction of its operative ceiling
    (RL-MON-002). Sustained-over-3-evaluations operator escalation is downstream
    reporting-hierarchy routing, not this unit's concern."""

    symbol: str
    value: int
    maxratecount: int | None
    code: str = field(default="RATE_COUNTER_WARNING", init=False)


# --- the unit -----------------------------------------------------------------

class RateCounter:
    """ar:AR-030 per-pair order-rate-counter against the executions-ACK operative ceiling.

    One instance per private connection (mod:WS_Manager owns it). Clock-free: it stores the
    operative ceiling (maxratecount) and the latest pushed per-pair counter value, derives
    the RL-MON-002 warning band and the RL-MON-003 critical-suppression latch from the
    section-9 seed fractions, and returns the registry events the caller routes to mod:Logger.
    """

    __slots__ = ("_warning_pct", "_critical_pct", "_ceiling", "_value", "_suppressed")

    def __init__(
        self,
        *,
        warning_pct: float | None = None,
        critical_pct: float | None = None,
    ) -> None:
        # Seeds default from the registry (CIATS-owned, refined from paper); the diagram
        # figures govern (rl_warning_threshold_pct=0.80, rl_critical_threshold_pct=0.95).
        self._warning_pct = (
            float(registry.value("rl_warning_threshold_pct"))
            if warning_pct is None
            else float(warning_pct)
        )
        self._critical_pct = (
            float(registry.value("rl_critical_threshold_pct"))
            if critical_pct is None
            else float(critical_pct)
        )
        self._ceiling: int | None = None
        self._value: dict[str, int] = {}
        self._suppressed: set[str] = set()

    # --- the executions-ACK ceiling (AR-030) ---------------------------------
    def set_ceiling(self, maxratecount: int) -> MaxRateCountSet:
        """Set the operative per-pair ceiling from the executions ACK (AR-030). Returns the
        MAXRATECOUNT_SET event. A non-positive ceiling is a malformed ACK and is rejected."""
        value = int(maxratecount)
        if value <= 0:
            raise ValueError(f"maxratecount must be a positive ceiling, got {maxratecount!r}")
        self._ceiling = value
        return MaxRateCountSet(value)

    # --- one pushed per-pair counter value (A-1) -----------------------------
    def observe(self, symbol: str, value: int) -> list[object]:
        """Record one pushed per-pair rate counter value (A-1). Returns the events to route:
        always a RATE_COUNTER_UPDATE, plus a RATE_COUNTER_WARNING when the value exceeds the
        warning fraction of the operative ceiling (RL-MON-002). Also drives the RL-MON-003
        critical-suppression latch (hysteresis: arm > critical fraction, release <= warning
        fraction). Below an ACK ceiling the bands cannot be derived (never 125), so only the
        RATE_COUNTER_UPDATE (maxratecount=None) is emitted."""
        value = int(value)
        self._value[symbol] = value
        events: list[object] = [RateCounterUpdate(symbol, value, self._ceiling)]
        if self._ceiling is None:
            return events
        if value > self._ceiling * self._warning_pct:
            events.append(RateCounterWarning(symbol, value, self._ceiling))
        if value > self._ceiling * self._critical_pct:
            self._suppressed.add(symbol)            # RL-MON-003 arm
        elif value <= self._ceiling * self._warning_pct:
            self._suppressed.discard(symbol)        # RL-MON-003 release (decayed back below warning)
        return events

    # --- reconnect (WS-REC-004 RESET_RATE_CEILING) ---------------------------
    def reset(self) -> None:
        """Reconnect reset (the WS-REC-004 / RESET_RATE_CEILING step): the engine-side
        per-pair counters reset/decay across a disconnect, so drop the stale per-pair values
        and suppression latches. The operative ceiling is KEPT as provisional (so the warning
        band stays live) until the fresh executions ACK re-sets it on re-subscribe."""
        self._value.clear()
        self._suppressed.clear()

    # --- non-mutating probes -------------------------------------------------
    @property
    def ceiling(self) -> int | None:
        """The operative per-pair ceiling (maxratecount), or None before the executions ACK."""
        return self._ceiling

    def value(self, symbol: str) -> int | None:
        """The latest pushed counter value for a pair, or None if none observed."""
        return self._value.get(symbol)

    def warning_threshold(self) -> float | None:
        """The RL-MON-002 warning level (ceiling x warning fraction), or None pre-ACK."""
        return None if self._ceiling is None else self._ceiling * self._warning_pct

    def critical_threshold(self) -> float | None:
        """The RL-MON-003 critical level (ceiling x critical fraction), or None pre-ACK."""
        return None if self._ceiling is None else self._ceiling * self._critical_pct

    def is_warning(self, symbol: str) -> bool:
        """True if the pair's latest value exceeds the warning fraction (RL-MON-002)."""
        v = self._value.get(symbol)
        return v is not None and self._ceiling is not None and v > self._ceiling * self._warning_pct

    def is_entry_suppressed(self, symbol: str) -> bool:
        """The RL-MON-003 non-blocking predicate the order-dispatch gate reads: True while the
        pair's entry add_order placement is suppressed (armed above critical, held through the
        hysteresis band, released once decayed back below warning). Exit/cancel orders are
        NEVER gated by this - the suppression exists to PRESERVE the exit rate budget."""
        return symbol in self._suppressed
