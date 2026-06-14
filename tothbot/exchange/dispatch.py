"""Inbound O(1) channel dispatch table - the WS_Manager read-side gatekeeper.

Source: 0500000 dv1_240 sec 7 Image6 (mod:WS_Manager) +
contract:WSManager_Dispatch_Seam. mod:WS_Manager is the SOLE dispatch
gatekeeper (PA-001): every Kraken WS push frame is routed to exactly one
internal consumer through an O(1) dispatch-table lookup.

This module is the INBOUND half (Kraken -> TothBot). The OUTBOUND half
(TothBot -> Kraken order RPCs, the paper/live mode gate) is seam.py.

An unknown channel is NEVER silently dropped (A-12 / rule:HR-WM-006): a frame
that resolves to no known channel raises UnknownChannelError so the read loop
logs it WARN and alerts, rather than losing the event.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Union

from .channels import PrivateChannel, PublicChannel

# A resolved logical channel - one of the 5 public + 2 private channels.
Channel = Union[PublicChannel, PrivateChannel]
# A frame handler. The frame is the already-parsed Kraken message (a dict).
Handler = Callable[[dict], None]


class UnknownChannelError(KeyError):
    """A wire frame did not resolve to any known channel (never dropped)."""


def channel_from_wire(name: str, interval: int | None = None) -> Channel:
    """Resolve a Kraken WS v2 channel name (+ interval) to a logical Channel.

    The ohlc channel carries an interval that distinguishes the 5-minute
    system clock from the 60-minute HTF feed; every other channel maps by
    name alone. An unrecognized (name, interval) raises UnknownChannelError.
    """
    if name == "ohlc":
        if interval == 5:
            return PublicChannel.OHLC_5M
        if interval == 60:
            return PublicChannel.OHLC_60M
        raise UnknownChannelError(f"ohlc interval not handled: {interval!r}")
    by_name: dict[str, Channel] = {
        "ticker": PublicChannel.TICKER,
        "instrument": PublicChannel.INSTRUMENT,
        "status": PublicChannel.STATUS,
        "executions": PrivateChannel.EXECUTIONS,
        "balances": PrivateChannel.BALANCES,
    }
    try:
        return by_name[name]
    except KeyError:
        raise UnknownChannelError(f"unknown channel: {name!r}") from None


class DispatchTable:
    """O(1) channel -> handler routing. One handler per channel (sole owner)."""

    def __init__(self) -> None:
        self._table: dict[Channel, Handler] = {}

    def register(self, channel: Channel, handler: Handler) -> None:
        """Bind a handler to a channel. Re-registering a channel is an error
        (a channel has exactly one consumer; double-binding hides a wiring bug).
        """
        if channel in self._table:
            raise ValueError(f"channel already has a handler: {channel}")
        self._table[channel] = handler

    def dispatch(self, channel: Channel, frame: dict) -> None:
        """Route a frame to its handler (O(1)). Unknown channel never dropped."""
        try:
            handler = self._table[channel]
        except KeyError:
            raise UnknownChannelError(f"no handler registered for {channel}") from None
        handler(frame)

    def route(self, name: str, frame: dict, interval: int | None = None) -> None:
        """Resolve a wire frame and dispatch it in one step (read-loop entry)."""
        self.dispatch(channel_from_wire(name, interval), frame)

    @property
    def registered_channels(self) -> frozenset[Channel]:
        return frozenset(self._table)
