"""The cold-start composition root - read ops settings, BIND the real delivery edges, run the organism.

Source: 0500000 dv1_251 ar:AR-049 (the cold-start startup sequence) + rule:HR-LG-009 (the C1 alert SMTP
contract) + rule:HR-LG-013 (the durable trade-record file) + contract:Operator_Reporting_Hierarchy (the
C1 push + C2-C6 pull tracks). Every organism subsystem is built + tested as INJECTED seams; THIS module
is the deploy-wiring that constructs the real edges from ops settings and runs assemble_operational.

THE EDGES IT BINDS (all from settings, the low-level send/file/socket still injectable for tests):
  - the mod:Logger HR-LG-009 C1 alert SMTP send (AlertEmailSender) -> Logger.set_alert_sink (the PUSH
    track: a profitable tested theory / a CRITICAL event is emailed to the operator immediately).
  - the C2-C6 periodic-pull SMTP transport (SmtpReportTransport) -> assemble_operational(report_emit=)
    (the PULL track: the cadence scheduler fires it off the OHLC_5m clock).
  - the rule:HR-LG-013 durable trade-record dir -> assemble_operational(records_dir=).
  - the data-layer I/O edges (rest_client / open_socket / bucket) are passed in - the real websockets +
    REST construction is the live-deploy slice; the cold-start over fakes (asyncio.run) proves the wiring.

SMTP credentials are read from ENVIRONMENT VARIABLES, never hardcoded (rule:HR-LG-009). The low-level
smtplib send carries the per-attempt 20s timeout (ALERT_SMTP_TIMEOUT_SEC).
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

from ..config.settings import Mode
from ..exchange.ws_manager import WSManager
from ..pipeline.operational import OperationalSystem, assemble_operational
from ..recorder.alert_transport import ALERT_SMTP_TIMEOUT_SEC, AlertEmailSender
from ..recorder.logger import Logger
from ..recorder.report_render import ReportSink
from ..recorder.report_transport import SmtpReportTransport, SmtpSend, smtplib_send


def _csv(value: str | None) -> tuple[str, ...]:
    """Parse a comma-separated env value into a tuple (blank -> empty)."""
    return tuple(p.strip() for p in value.split(",") if p.strip()) if value else ()


@dataclass(frozen=True)
class OpsSettings:
    """The cold-start ops configuration: the universe + the run mode + the SMTP delivery edge + the
    durable records dir. SMTP creds come from the environment (rule:HR-LG-009), NEVER hardcoded."""

    universe: tuple[str, ...] = ()
    mode: Mode = Mode.PAPER
    email_sender: str = "tothbot@tothbot.com"
    alert_recipients: tuple[str, ...] = ()          # C1 IMMEDIATE push (rule:HR-RPT-001)
    report_recipients: tuple[str, ...] = ()         # C2-C6 periodic pull
    smtp_host: str | None = None
    smtp_port: int = 0
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_starttls: bool = False
    records_dir: str | None = None

    @property
    def smtp_configured(self) -> bool:
        return bool(self.smtp_host)

    @classmethod
    def from_env(
        cls, environ: Mapping[str, str] | None = None, *, universe: Sequence[str] = ()
    ) -> "OpsSettings":
        """Build from TOTHBOT_* environment variables (creds NEVER hardcoded). `universe` is passed in
        (it is large / not an env value). A missing var falls to the dataclass default."""
        import os

        env = environ if environ is not None else os.environ
        mode = Mode.LIVE if (env.get("TOTHBOT_MODE", "paper").lower() == "live") else Mode.PAPER
        port_raw = env.get("TOTHBOT_SMTP_PORT")
        return cls(
            universe=tuple(universe),
            mode=mode,
            email_sender=env.get("TOTHBOT_EMAIL_SENDER", "tothbot@tothbot.com"),
            alert_recipients=_csv(env.get("TOTHBOT_ALERT_RECIPIENTS")),
            report_recipients=_csv(env.get("TOTHBOT_REPORT_RECIPIENTS")),
            smtp_host=env.get("TOTHBOT_SMTP_HOST") or None,
            smtp_port=int(port_raw) if port_raw else 0,
            smtp_username=env.get("TOTHBOT_SMTP_USER") or None,
            smtp_password=env.get("TOTHBOT_SMTP_PASSWORD") or None,
            smtp_starttls=env.get("TOTHBOT_SMTP_STARTTLS", "").lower() in ("1", "true", "yes"),
            records_dir=env.get("TOTHBOT_RECORDS_DIR") or None,
        )


def _settings_smtp_send(settings: OpsSettings) -> SmtpSend:
    """The real smtplib socket edge from settings (env creds; the HR-LG-009 20s per-attempt timeout)."""
    return smtplib_send(
        settings.smtp_host, settings.smtp_port,
        starttls=settings.smtp_starttls, username=settings.smtp_username,
        password=settings.smtp_password, timeout=ALERT_SMTP_TIMEOUT_SEC,
    )


def make_alert_sink(
    settings: OpsSettings,
    *,
    smtp_send: SmtpSend | None = None,
    on_event: Callable[[object], None] | None = None,
    sleep: Callable[[float], None] | None = None,
) -> AlertEmailSender | None:
    """The rule:HR-LG-009 C1 alert SMTP sender (the Logger on_alert sink), or None when SMTP / the
    alert recipients are not configured (the alert seam stays unwired - paper/dev). `smtp_send` is
    injected in tests; otherwise the real smtplib edge from settings."""
    if not settings.smtp_configured or not settings.alert_recipients:
        return None
    send = smtp_send or _settings_smtp_send(settings)
    return AlertEmailSender(
        send, sender=settings.email_sender, recipients=settings.alert_recipients,
        on_event=on_event, sleep=sleep,
    )


def make_report_transport(
    settings: OpsSettings, *, smtp_send: SmtpSend | None = None
) -> ReportSink | None:
    """The C2-C6 periodic-pull SMTP transport (the assemble_operational report_emit), or None when
    SMTP / the report recipients are not configured (the cadence runs but emits nowhere)."""
    if not settings.smtp_configured or not settings.report_recipients:
        return None
    send = smtp_send or _settings_smtp_send(settings)
    return SmtpReportTransport(
        send, sender=settings.email_sender, recipients=settings.report_recipients
    )


async def build_system(
    settings: OpsSettings,
    *,
    rest_client: object,
    open_socket: Callable,
    bucket: object,
    mpp_store: object,
    reward_store: object,
    smtp_send: SmtpSend | None = None,
    wm: object | None = None,
    on_event: Callable[[object], None] | None = None,
    now_utc: Callable[[], datetime] | None = None,
    now_monotonic: Callable[[], float] = time.monotonic,
    rest_sleep: Callable = asyncio.sleep,
    pace_sleep: Callable = asyncio.sleep,
    alert_sleep: Callable[[float], None] | None = None,
    open_private_socket: Callable | None = None,
    acquire_token: Callable | None = None,
    fetch_snap_orders: Callable | None = None,
    balances_handler: Callable | None = None,
) -> OperationalSystem:
    """The ar:AR-049 cold-start composition: construct mod:Logger with the HR-LG-009 C1 alert SMTP seam
    bound (Logger.set_alert_sink), wire the C2-C6 pull transport + the HR-LG-013 records dir from
    settings, and run assemble_operational over the (injected) data-layer edges. Returns the runnable
    OperationalSystem (the caller drives system.run()). The low-level send/socket/file edges are
    injectable, so the whole composition is testable without I/O.

    on_event is an ADDITIVE telemetry tap: the WSManager AND the warm-up / regime / liquidity / pacing
    / sweep components route their events through _record, which always appends to the mod:Logger
    Stream-1/Stream-2 corpus (the CIATS data source) AND, when on_event is given, mirrors each event to
    it. The deploy entrypoint passes a console printer so a smoke run is observable (warm-up, pair
    READY, sweeps, gate decisions, position writes); None leaves the path on the corpus alone (the test
    default - no console mirror).

    The private-WS edges (open_private_socket / acquire_token / fetch_snap_orders / balances_handler)
    are the LIVE-only seam (PA-004 div #1 / rule:HR-WM-022: the separate authenticated executions/
    balances connection). They are passed straight through to assemble_operational, which connects the
    private socket ONLY in Mode.LIVE (and raises if live is requested without them). PAPER leaves them
    None (never connects the private WS); the deploy entrypoint binds the real edges for live."""
    logger = Logger(on_event=None)
    console = on_event

    def _record(event: object) -> None:
        """Tee every organism event to the Logger corpus AND (when wired) the console mirror."""
        logger.record(event)
        if console is not None:
            console(event)

    # The C1 alert seam: its evt:ALERT_SENT / evt:ALERT_SMTP_FAILED route back through _record (corpus
    # + console), so build the sender on _record THEN bind it as the Logger on_alert (resolves the cycle).
    alert_sink = make_alert_sink(
        settings, smtp_send=smtp_send, on_event=_record, sleep=alert_sleep
    )
    if alert_sink is not None:
        logger.set_alert_sink(alert_sink)

    report_emit = make_report_transport(settings, smtp_send=smtp_send)

    if wm is None:
        wm = WSManager(
            settings.mode, on_event=_record,
            now_monotonic=now_monotonic, now_utc=now_utc,
        )

    return await assemble_operational(
        universe=settings.universe,
        rest_client=rest_client,
        open_socket=open_socket,
        bucket=bucket,
        wm=wm,
        logger=logger,
        mpp_store=mpp_store,
        reward_store=reward_store,
        mode=settings.mode,
        on_event=_record,
        now_utc=now_utc,
        rest_sleep=rest_sleep,
        pace_sleep=pace_sleep,
        report_emit=report_emit,
        records_dir=settings.records_dir,
        open_private_socket=open_private_socket,
        acquire_token=acquire_token,
        fetch_snap_orders=fetch_snap_orders,
        balances_handler=balances_handler,
    )


async def run(settings: OpsSettings, **edges: object) -> None:
    """Cold-start + RUN: build the system (build_system) then drive it (OperationalSystem.run() - the
    public data layer, and the private connection in live). Blocks until the organism stops. `edges`
    are the data-layer I/O (rest_client / open_socket / bucket / mpp_store / reward_store) + any
    injectable overrides build_system accepts."""
    system = await build_system(settings, **edges)
    await system.run()
