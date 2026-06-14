"""TothBot-internal data-layer events.

evt:CANDLE_CLOSE_TRIGGER is the system tick produced by the 5-minute OHLC
system clock (contract:OHLC_5m_System_Clock). It is the sole trigger that
fires the trading pipeline (Signal_Pipeline + Selection_Controller +
Regime_Engine consume it). Source: 0500000 dv1_240.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from .candle import Candle


@dataclass(frozen=True)
class CandleCloseTrigger:
    """evt:CANDLE_CLOSE_TRIGGER - carries the closed 5-minute candle."""

    candle: Candle

    @property
    def pair(self) -> str:
        return self.candle.pair

    @property
    def close_ts(self) -> datetime:
        return self.candle.close_ts
