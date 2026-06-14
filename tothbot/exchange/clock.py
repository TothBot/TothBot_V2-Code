"""contract:OHLC_5m_System_Clock - the universal system clock.

Source: 0500000 dv1_240 (contract:OHLC_5m_System_Clock, AR-005). Consumes
Kraken WS v2 5-minute OHLC candle-close events and converts each into the
system tick (evt:CANDLE_CLOSE_TRIGGER) that fires the trading pipeline.

NO timers, NO polling: every pipeline activation is gated on this clock
signal. There are 288 candle closes per day per subscribed pair. The clock
source is ONLY the 5-minute OHLC channel; 60-minute (HTF) and other candles
are data, not clock ticks.
"""

from __future__ import annotations

from collections.abc import Callable

from .candle import Candle, Interval
from .events import CandleCloseTrigger

ClockListener = Callable[[CandleCloseTrigger], None]


class SystemClock:
    """Turns closed 5-minute candles into pipeline ticks. Input-driven only."""

    def __init__(self) -> None:
        self._listeners: list[ClockListener] = []

    def subscribe(self, listener: ClockListener) -> None:
        """Register a listener fired on every 5-minute candle-close tick."""
        self._listeners.append(listener)

    def on_candle_close(self, candle: Candle) -> CandleCloseTrigger:
        """Convert a closed 5m candle into the system tick and dispatch it.

        Raises ValueError if handed a non-5-minute candle: the system clock
        source is exclusively the 5-minute OHLC channel.
        """
        if candle.interval is not Interval.FIVE_MIN:
            raise ValueError(
                f"system clock source must be a 5-minute candle, got {candle.interval.name}"
            )
        trigger = CandleCloseTrigger(candle=candle)
        for listener in self._listeners:
            listener(trigger)
        return trigger
