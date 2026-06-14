"""mod:WS_Manager per-shard reconnect coordinator: scenario selection, the
HR-WM-012 in-progress gate, and the ar:AR-056 / WS-REC-004 restore sequence.

Source: 0500000 dv1_240 sec 2 Image1 (the A-5 reconnect distinction; ar:AR-056
mid-session restore path; ar:AR-080 Cloudflare ceiling) + sec 7 mod:WS_Manager
desc (the Per-Shard Reconnect Coordinator block: contract:WM-RECONNECT-016 +
rule:HR-WM-029 per-shard independence; the D1 RECONNECT-RESIDUAL wire facts
WS-REC-003 / WS-REC-004; rule:HR-WM-012 pipeline-no-fire;
contract:WM-RECONNECT-019 paper-mode gating).

Each shard reconnects INDEPENDENTLY (rule:HR-WM-029): a transient WS error is
caught LOCALLY on that shard and drives _initiate_reconnect for THAT shard only;
the other shards keep running. This module is the PURE policy core (mirrors
keepalive.py / pacing.py): the Scenario A/B selection, the in-progress gate that
backs rule:HR-WM-012, and the WS-REC-004 restore-step SEQUENCE. The socket
reconnect, the REST GetWebSocketsToken call, and the subscribe RPCs are the I/O
edge, wired later.

TWO disconnect scenarios (WS-REC-003 / A-5):
  Scenario A - random disconnect: up to SCENARIO_A_IMMEDIATE_ATTEMPTS (5)
    immediate attempts, then exponential backoff starting at
    SCENARIO_A_BACKOFF_BASE_SEC (1 s).
  Scenario B - after a CONFIRMED Kraken trading-engine maintenance disconnect:
    a MINIMUM SCENARIO_B_MIN_DELAY_SEC (5 s) delay before reconnecting, so we do
    not hammer an engine still completing maintenance.

The ar:AR-080 Cloudflare ceiling (CLOUDFLARE_RECONNECT_LIMIT connection
establishments per CLOUDFLARE_WINDOW_SEC per IP) is the hard bound any backoff
schedule must respect.

  *** BLOCKED - TB00709 NSI sec 6 / TB00708 housekeeping 8b ***
  The EXACT Scenario-A exponential backoff schedule - the per-attempt delay
  growth, the 30 s per-attempt cap, and the "181s max sleep" cumulative cap drawn
  in the D1 visual annotation - is NOT stated in the WS-REC-003 prose; 181 s is a
  reconstruction (1+2+4+8+16 then 30 x5 = 181), not a diagram read. Per DIAGRAMS
  GOVERN (TB00000 sec 4.2) the backoff numbers are NOT invented here:
  reconnect_delay_sec() raises NotImplementedError for the backoff phase pending
  Bill pinning the exact schedule INTO the figure. Everything else in this module
  is read directly from the diagram and is complete.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

# --- stated reconnect constants (WS-REC-003 / ar:AR-080; not CIATS-owned) -----
SCENARIO_A_IMMEDIATE_ATTEMPTS = 5      # WS-REC-003 "up to 5 immediate attempts"
SCENARIO_A_BACKOFF_BASE_SEC = 1.0      # WS-REC-003 "exponential backoff starting at 1s"
SCENARIO_B_MIN_DELAY_SEC = 5.0         # WS-REC-003 "MINIMUM 5-second delay"
CLOUDFLARE_RECONNECT_LIMIT = 150       # ar:AR-080 connection establishments / window / IP
CLOUDFLARE_WINDOW_SEC = 600.0          # ar:AR-080 rolling 10-minute window

# Canonical log key for the reconnect event (the receive loop logs this on every
# reconnect; evt:WS_RECONNECT in the mod:WS_Manager produces: list).
WS_RECONNECT_LOG_KEY = "WS_RECONNECT"  # evt:WS_RECONNECT


class DisconnectReason(Enum):
    """Why a shard dropped - selects the reconnect scenario (WS-REC-003)."""

    RANDOM = "random"            # transient/random disconnect          -> Scenario A
    MAINTENANCE = "maintenance"  # confirmed Kraken engine maintenance  -> Scenario B


class ReconnectScenario(Enum):
    """The two reconnect timing scenarios (WS-REC-003 / A-5)."""

    SCENARIO_A = "scenario_a"  # immediate attempts then exponential backoff
    SCENARIO_B = "scenario_b"  # minimum 5 s delay (post-maintenance)


class RestoreStep(Enum):
    """The ordered ar:AR-056 / WS-REC-004 mid-session restore steps.

    WS-REC-004's single "re-subscribe all channels" step is split here along the
    diagram's own public/private channel partition (public: instrument, status,
    ohlc_5m, ticker; private: executions, balances) so the WM-RECONNECT-019 paper
    gate - skip the private-side steps - is exact. This is a diagram read (the
    channel sets and PA-004 div #1 "private WS skipped in paper" are explicit),
    not an added step."""

    ACQUIRE_WS_TOKEN = "acquire_ws_token"                # fresh REST GetWebSocketsToken
    RECONNECT_SOCKET = "reconnect_socket"                # WS-LIB params (transport)
    RESUBSCRIBE_PUBLIC = "resubscribe_public"            # instrument/status/ohlc_5m/ticker + ACK parse
    RESUBSCRIBE_PRIVATE = "resubscribe_private"          # executions/balances + ACK parse
    RESET_RATE_CEILING = "reset_rate_ceiling"            # maxratecount from executions ACK (AR-030)
    RESUME_KEEPALIVE = "resume_keepalive"                # 30s ping + zombie tasks
    RESTORE_POSITION_MIRROR = "restore_position_mirror"  # from snap_orders
    RESTORE_TICKER_TRIGGER = "restore_ticker_trigger"    # per-pair bbo/trades event_trigger


@dataclass(frozen=True)
class RestoreStepSpec:
    """One restore step plus whether it is private-side (skipped in paper)."""

    step: RestoreStep
    private_side: bool  # True -> skipped in paper mode (WM-RECONNECT-019)
    summary: str


# Canonical restore order (ar:AR-056 / WS-REC-004). Private-side steps touch the
# private WS / executions stream / snap_orders Position Mirror - the paper mode
# has no private WS (PA-004 divergence point #1) so they are skipped in paper.
RESTORE_SEQUENCE: tuple[RestoreStepSpec, ...] = (
    RestoreStepSpec(RestoreStep.ACQUIRE_WS_TOKEN, True,
                    "fresh WebSocket token via REST GetWebSocketsToken"),
    RestoreStepSpec(RestoreStep.RECONNECT_SOCKET, False,
                    "reconnect with WS-LIB params (max_size/open_timeout/max_queue=None/ping_interval=None)"),
    RestoreStepSpec(RestoreStep.RESUBSCRIBE_PUBLIC, False,
                    "re-subscribe public channels, parsing each ACK warnings[]"),
    RestoreStepSpec(RestoreStep.RESUBSCRIBE_PRIVATE, True,
                    "re-subscribe private channels (executions/balances), parsing each ACK"),
    RestoreStepSpec(RestoreStep.RESET_RATE_CEILING, True,
                    "extract maxratecount from the executions ACK; reset the ceiling (never hardcode 125)"),
    RestoreStepSpec(RestoreStep.RESUME_KEEPALIVE, False,
                    "resume the 30s application ping + zombie-detection tasks"),
    RestoreStepSpec(RestoreStep.RESTORE_POSITION_MIRROR, True,
                    "restore the Position Mirror from snap_orders"),
    RestoreStepSpec(RestoreStep.RESTORE_TICKER_TRIGGER, False,
                    "restore per-pair ticker event_trigger (bbo for open-position pairs)"),
)


def select_scenario(reason: DisconnectReason) -> ReconnectScenario:
    """Map a disconnect reason to its reconnect scenario (WS-REC-003).
    Only a CONFIRMED maintenance disconnect takes Scenario B; everything else is
    a random disconnect and takes Scenario A."""
    if reason is DisconnectReason.MAINTENANCE:
        return ReconnectScenario.SCENARIO_B
    return ReconnectScenario.SCENARIO_A


def is_immediate_attempt(attempt: int) -> bool:
    """True while a Scenario-A attempt is still in the immediate (zero-delay)
    phase. attempt is 1-based; the first SCENARIO_A_IMMEDIATE_ATTEMPTS are
    immediate (WS-REC-003), after which the BLOCKED exponential backoff begins."""
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")
    return attempt <= SCENARIO_A_IMMEDIATE_ATTEMPTS


def reconnect_delay_sec(scenario: ReconnectScenario, attempt: int) -> float:
    """Seconds to wait before reconnect attempt `attempt` (1-based).

    Code-complete cases (read directly from WS-REC-003):
      Scenario B, any attempt          -> SCENARIO_B_MIN_DELAY_SEC (5 s floor)
      Scenario A, immediate phase       -> 0.0 (the first 5 attempts)

    BLOCKED case (TB00709 NSI sec 6 - DIAGRAMS GOVERN, do not invent):
      Scenario A, backoff phase (attempt > 5) -> raises NotImplementedError; the
      exact exponential schedule + 30 s cap + 181 s max sleep must be pinned into
      the figure by Bill first.
    """
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")
    if scenario is ReconnectScenario.SCENARIO_B:
        return SCENARIO_B_MIN_DELAY_SEC
    # Scenario A
    if is_immediate_attempt(attempt):
        return 0.0
    raise NotImplementedError(
        "Scenario-A exponential backoff schedule is BLOCKED pending Bill's pin "
        "of the exact per-attempt delays / 30s cap / 181s max sleep into the D1 "
        "figure (TB00709 NSI sec 6; DIAGRAMS GOVERN - do not invent)."
    )


def build_restore_sequence(*, paper_mode: bool) -> list[RestoreStep]:
    """The ordered ar:AR-056 / WS-REC-004 restore steps for this mode. In paper
    mode the private-side steps are skipped (WM-RECONNECT-019; there is no private
    WS - PA-004 divergence point #1)."""
    return [
        spec.step
        for spec in RESTORE_SEQUENCE
        if not (paper_mode and spec.private_side)
    ]


class ShardReconnectCoordinator:
    """Per-shard reconnect state (rule:HR-WM-029 independence + rule:HR-WM-012).

    PURE: no I/O. Tracks which shards are mid-reconnect and which scenario each is
    running. The receive loop reads is_reconnecting()/any_reconnecting() to honour
    rule:HR-WM-012 - while ANY shard is reconnecting, pipeline evaluations are
    PROHIBITED and candle events that arrive during the reconnect are DISCARDED.
    """

    def __init__(self) -> None:
        self._in_progress: dict[int, ReconnectScenario] = {}

    def begin(self, shard_index: int, reason: DisconnectReason) -> ReconnectScenario:
        """A transient error was caught locally on `shard_index`: start its
        reconnect, record the selected scenario, and return it."""
        if shard_index < 0:
            raise ValueError(f"shard_index must be >= 0, got {shard_index}")
        scenario = select_scenario(reason)
        self._in_progress[shard_index] = scenario
        return scenario

    def complete(self, shard_index: int) -> None:
        """The shard's restore sequence finished; clear its in-progress flag."""
        self._in_progress.pop(shard_index, None)

    def is_reconnecting(self, shard_index: int) -> bool:
        return shard_index in self._in_progress

    def any_reconnecting(self) -> bool:
        """True while at least one shard is mid-reconnect - the rule:HR-WM-012
        pipeline-no-fire / candle-discard condition."""
        return bool(self._in_progress)

    def scenario_for(self, shard_index: int) -> ReconnectScenario | None:
        return self._in_progress.get(shard_index)

    @property
    def reconnecting_shards(self) -> frozenset[int]:
        return frozenset(self._in_progress)
