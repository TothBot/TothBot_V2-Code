"""contract:Reconciliation_REST - private-channel sequence-gap detection + recovery trigger.

Source: 0500000 dv1_240 sec 2 Image1 (A-9 / A-10) + sec 7 mod:WS_Manager desc
(D1 keepalive/seq-gap wire facts WS-EXE-019, WS-BAL-005).

Every Kraken private-channel message carries a monotonically increasing sequence
integer. WS_Manager tracks the last-seen sequence per channel, INDEPENDENTLY for
executions and balances, and on any forward gap (a jump of more than 1) alerts
and triggers a REST reconciliation to repair the local state:

  EXECUTIONS (A-10 / WS-EXE-019): a gap means missed order/fill events ->
    Position Mirror does not know a position opened or closed -> the next pipeline
    evaluation operates on stale position data. Alert EXECUTIONS_SEQUENCE_GAP,
    trigger REST GetOpenOrders. HIGHER SEVERITY than balances.

  BALANCES (A-9 / WS-BAL-005): a gap means a missed ledger event -> incorrect
    available-capital calculation -> sizing against a wrong balance. Alert
    BALANCES_SEQUENCE_GAP, trigger REST GetAccountBalance.

Sequence tracking RESETS on each new subscription (reconnect) - do NOT gap-detect
across sessions (A-10). reset() is called inside the reconnect restore sequence
(WS-REC-004). The REST recovery wire contracts (GetOpenOrders REST-OO-*,
GetAccountBalance REST-BAL-*) are canonical in the D1 container:Kraken_REST_API
desc; this module owns gap DETECTION and names the recovery endpoint to call -
the async REST call itself is the I/O edge, driven later.

Pure (no I/O): observe() returns a SequenceGap describing the recovery to run, or
None when the sequence advanced cleanly, so it is unit-testable without a socket.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


class ReconChannel(Enum):
    """The two independently-tracked private channels (A-9 / A-10)."""

    EXECUTIONS = "executions"  # A-10 / WS-EXE-019 -> GetOpenOrders (higher severity)
    BALANCES = "balances"      # A-9  / WS-BAL-005 -> GetAccountBalance


# Per-channel recovery wiring (the alert log key, the REST recovery endpoint, and
# the relative severity). executions outranks balances: a missed fill leaves a
# live position unmanaged, a strictly worse failure than a stale balance (A-10).
_ALERT_KEY: dict[ReconChannel, str] = {
    ReconChannel.EXECUTIONS: "EXECUTIONS_SEQUENCE_GAP",  # WS-EXE-019
    ReconChannel.BALANCES: "BALANCES_SEQUENCE_GAP",      # WS-BAL-005
}
_RECOVERY_ENDPOINT: dict[ReconChannel, str] = {
    ReconChannel.EXECUTIONS: "GetOpenOrders",     # REST-OO-002 (restore Position Mirror)
    ReconChannel.BALANCES: "GetAccountBalance",   # REST-BAL-002 (restore balance)
}
_SEVERITY: dict[ReconChannel, str] = {
    ReconChannel.EXECUTIONS: "HIGH",    # A-10: higher severity than balances
    ReconChannel.BALANCES: "MEDIUM",    # A-9
}


@dataclass(frozen=True)
class SequenceGap:
    """A detected forward gap on one channel - the recovery the read loop runs."""

    channel: ReconChannel
    last_seq: int       # last cleanly-seen sequence before the gap
    received_seq: int   # the sequence that exposed the gap (> last_seq + 1)
    missed: int         # count of skipped events = received_seq - last_seq - 1
    alert_key: str      # the alert log key (EXECUTIONS_/BALANCES_SEQUENCE_GAP)
    recovery_endpoint: str  # the REST endpoint to call (GetOpenOrders/GetAccountBalance)
    severity: str       # HIGH (executions) | MEDIUM (balances)


class SequenceGapDetector:
    """Last-seen sequence tracker for ONE channel. Forward gap (jump > 1) -> alert.

    The first message after construction or reset() establishes the baseline and
    never reports a gap. A non-advancing sequence (duplicate or out-of-order
    replay, received_seq <= last_seq) is not a forward gap and is ignored without
    moving the watermark backwards.
    """

    def __init__(self, channel: ReconChannel) -> None:
        self.channel = channel
        self._last_seq: int | None = None

    @property
    def last_seq(self) -> int | None:
        return self._last_seq

    def reset(self) -> None:
        """Drop the watermark - called on each new subscription / reconnect
        (A-10: sequence tracking resets on reconnect; do not gap-detect across
        sessions).
        """
        self._last_seq = None

    def observe(self, seq: int) -> SequenceGap | None:
        """Record a channel message's sequence. Returns a SequenceGap when a
        forward gap (skip > 1) is detected, else None.
        """
        if self._last_seq is None:
            self._last_seq = seq  # baseline; no gap on the first message
            return None
        if seq <= self._last_seq:
            return None  # duplicate / out-of-order replay; not a forward gap
        if seq == self._last_seq + 1:
            self._last_seq = seq  # clean advance
            return None
        # seq > last_seq + 1: a forward gap. Advance the watermark to seq so we
        # do not re-alert on every subsequent in-order message after the gap.
        gap = SequenceGap(
            channel=self.channel,
            last_seq=self._last_seq,
            received_seq=seq,
            missed=seq - self._last_seq - 1,
            alert_key=_ALERT_KEY[self.channel],
            recovery_endpoint=_RECOVERY_ENDPOINT[self.channel],
            severity=_SEVERITY[self.channel],
        )
        self._last_seq = seq
        return gap


class ReconciliationTracker:
    """Owns the two independent per-channel detectors (executions + balances).

    The read loop calls observe_executions / observe_balances as private frames
    arrive and runs the returned SequenceGap's recovery_endpoint when non-None.
    reset() (on reconnect) clears BOTH watermarks.
    """

    def __init__(self) -> None:
        self.executions = SequenceGapDetector(ReconChannel.EXECUTIONS)
        self.balances = SequenceGapDetector(ReconChannel.BALANCES)

    def observe_executions(self, seq: int) -> SequenceGap | None:
        return self.executions.observe(seq)

    def observe_balances(self, seq: int) -> SequenceGap | None:
        return self.balances.observe(seq)

    def reset(self) -> None:
        """Reset both channels' sequence tracking on reconnect (A-9 / A-10)."""
        self.executions.reset()
        self.balances.reset()
