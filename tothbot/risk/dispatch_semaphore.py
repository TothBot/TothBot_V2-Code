"""mod:Risk_Engine - ar:AR-043 the per-module capital-commitment dispatch semaphore (G7 CHECK 4).

Source: 0500000 dv1_252 sec 8 Image8 gate:G7_Risk_Guard CHECK 4 + ar:AR-043 ("BoundedSemaphore =
per-module dispatch serialization without blocking the broader pipeline; Long_Module has its own
semaphore, Short_Module has its own; independent serialization"). BILL TB00758 D2 RULING: the semantics
are POSITION-LIFETIME - the semaphore is ACQUIRED at entry dispatch (on a fill) and RELEASED on close,
so a module holds at most ONE open capital commitment at a time; G7 CHECK 4 probes locked() and SKIPs a
new candidate while it is held (a transient SKIP - retry next candle - never a policy reject).

This is the primitive (a BoundedSemaphore of bound 1): a non-blocking acquire, a bounded release (an
over-release past the bound raises, surfacing a lifecycle defect), and a non-mutating locked() probe for
the gate. PURE - no I/O, no clock, no asyncio. One instance per module wallet (no cross-module coupling,
sec 7); the dispatch owner (mod:WS_Manager) holds the per-side instances + acquires/releases them.
"""

from __future__ import annotations


class DispatchSemaphore:
    """ar:AR-043 per-module capital-commitment dispatch serialization (BoundedSemaphore, bound 1),
    position-lifetime (D2 ruling): acquire() at entry on a fill, release() on close, locked() is the G7
    CHECK 4 non-blocking probe. Bound 1 -> at most one open commitment per module at a time."""

    __slots__ = ("_held",)

    def __init__(self) -> None:
        self._held = False

    def acquire(self) -> bool:
        """Non-blocking acquire of the single capital-commitment slot. Returns True if acquired, False
        if already held (the caller should not have reached dispatch - G7 CHECK 4 probes locked()
        first; False is the defensive no-double-commit guard)."""
        if self._held:
            return False
        self._held = True
        return True

    def release(self) -> None:
        """Release the slot at close. BoundedSemaphore semantics: releasing when nothing is held is an
        over-release (a lifecycle defect) and raises ValueError - the caller (mod:WS_Manager) guards it
        so a spurious release cannot crash the close path."""
        if not self._held:
            raise ValueError("DispatchSemaphore released past its bound (no commitment held)")
        self._held = False

    def locked(self) -> bool:
        """The G7 CHECK 4 non-blocking probe: True while a capital commitment is held (does NOT
        acquire or mutate)."""
        return self._held
