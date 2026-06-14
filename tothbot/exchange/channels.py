"""Kraken WS v2 channel definitions (the data-layer surface).

Source: 0500000 dv1_240 Image1 (Kraken API Connection Architecture) and
Image6 (Complete System). Public WS v2 delivers five push channels; Private
WS v2 delivers two authenticated channels. The 5-minute OHLC channel is the
universal system clock (contract:OHLC_5m_System_Clock).

Private WS is NEVER connected in paper mode (PA-004 divergence #1); these
identifiers name the logical channels. The Kraken wire-protocol mapping
(e.g. channel="ohlc", interval=5) is applied by the WS client in S2b/S2c.
"""

from __future__ import annotations

from enum import Enum


class PublicChannel(Enum):
    """Kraken Public WS v2 push channels (no authentication)."""

    OHLC_5M = "ohlc_5m"        # SYSTEM CLOCK - 5-min candle closes (288/day/pair)
    OHLC_60M = "ohlc_60m"      # 1-hour HTF candle closes (G4 HTF confirmation)
    TICKER = "ticker"          # last/bid/ask (drawdown monitor + sizing)
    INSTRUMENT = "instrument"  # pair status + tradable flag (Pre-Gate-1)
    STATUS = "status"          # Kraken system-status broadcasts


class PrivateChannel(Enum):
    """Kraken Private WS v2 push channels (live mode only; PA-004 div #1)."""

    EXECUTIONS = "executions"  # order fills / cancels / amends (per-symbol sequence)
    BALANCES = "balances"      # account balance updates


# The single channel that drives the trading pipeline (contract:OHLC_5m_System_Clock).
SYSTEM_CLOCK_CHANNEL: PublicChannel = PublicChannel.OHLC_5M
