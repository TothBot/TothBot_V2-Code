"""ar:AR-049 cold-start runner (tothbot/app/runner.py) - the top-level deploy-wiring.

Exercises OpsSettings.from_env, the SMTP edge factories (make_alert_sink / make_report_transport), and
build_system end-to-end over fakes: the cold-start composition binds the HR-LG-009 C1 alert SMTP seam on
mod:Logger, the C2-C6 periodic-pull SMTP transport, and the HR-LG-013 durable records dir - all from
settings, the low-level send injected (no socket, no real disk beyond a tmp records dir).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from tothbot.app.runner import (
    OpsSettings,
    build_system,
    make_alert_sink,
    make_report_transport,
)
from tothbot.ciats.expected_reward import ExpectedRewardStore
from tothbot.ciats.seed_estimators import MppCapStore
from tothbot.config.settings import Mode
from tothbot.exchange.pacing import SubscribeTokenBucket
from tothbot.recorder.alert_transport import AlertEmailSender
from tothbot.recorder.report_transport import SmtpReportTransport
from tothbot.rest.client import OhlcResponse, RestOhlcBar

UTC = timezone.utc


# --------------------------------------------------------------------------- minimal data-layer fakes
def _resp(closes, *, base=1700000000, interval, last_vol=None, vol=1000):
    committed = []
    for i, c in enumerate(closes):
        o = Decimal(closes[i - 1] if i else c)
        cc = Decimal(c)
        v = Decimal(last_vol) if (last_vol is not None and i == len(closes) - 1) else Decimal(vol)
        committed.append(RestOhlcBar(time=base + i * interval, open=o, high=max(o, cc) + 2,
                                     low=min(o, cc) - 2, close=cc, volume=v))
    forming = RestOhlcBar(time=base + len(closes) * interval, open=Decimal(9), high=Decimal(9),
                          low=Decimal(9), close=Decimal(9), volume=Decimal(1))
    return OhlcResponse(committed=tuple(committed), forming=forming, last=committed[-1].time)


class _FakeRest:
    async def get_ohlc_data(self, pair, interval, *, since=None):
        if interval == 1440:
            return _resp([100 + i for i in range(85)], interval=86400)
        if interval == 5:
            return _resp([100 + i for i in range(58)], interval=300, last_vol=5000)
        return _resp([100 + i for i in range(60)], interval=3600)

    async def get_ticker_liquidity(self, pair):
        return Decimal("600000")


class _FakeTransport:
    async def send(self, m):
        pass

    async def recv(self):  # pragma: no cover
        raise AssertionError

    async def close(self):  # pragma: no cover
        pass


def _opener():
    async def open_socket(_k):
        return _FakeTransport()
    return open_socket


async def _nosleep(_s):
    return None


def _now():
    return datetime(2026, 6, 15, 7, 30, tzinfo=UTC)


# ----------------------------------------------------------------------------- OpsSettings.from_env
def test_from_env_reads_the_tothbot_vars():
    env = {
        "TOTHBOT_MODE": "paper", "TOTHBOT_SMTP_HOST": "mail.x", "TOTHBOT_SMTP_PORT": "587",
        "TOTHBOT_SMTP_USER": "u", "TOTHBOT_SMTP_PASSWORD": "p", "TOTHBOT_SMTP_STARTTLS": "true",
        "TOTHBOT_ALERT_RECIPIENTS": "alerts@tothbot.com, wstothjr@gmail.com",
        "TOTHBOT_REPORT_RECIPIENTS": "wstothjr@gmail.com", "TOTHBOT_EMAIL_SENDER": "bot@x",
        "TOTHBOT_RECORDS_DIR": "/home/tothbot/records",
    }
    s = OpsSettings.from_env(env, universe=["BTC/USD"])
    assert s.mode is Mode.PAPER and s.smtp_host == "mail.x" and s.smtp_port == 587
    assert s.smtp_username == "u" and s.smtp_password == "p" and s.smtp_starttls is True
    assert s.alert_recipients == ("alerts@tothbot.com", "wstothjr@gmail.com")
    assert s.report_recipients == ("wstothjr@gmail.com",)
    assert s.records_dir == "/home/tothbot/records" and s.universe == ("BTC/USD",)
    assert s.smtp_configured is True


def test_from_env_defaults_to_paper_and_unconfigured():
    s = OpsSettings.from_env({}, universe=[])
    assert s.mode is Mode.PAPER and s.smtp_configured is False
    assert s.alert_recipients == () and s.report_recipients == () and s.records_dir is None


def test_from_env_live_mode():
    assert OpsSettings.from_env({"TOTHBOT_MODE": "live"}).mode is Mode.LIVE


# ----------------------------------------------------------------------------- the SMTP edge factories
def test_make_alert_sink_none_when_unconfigured():
    assert make_alert_sink(OpsSettings(universe=("BTC/USD",))) is None              # no SMTP host
    assert make_alert_sink(OpsSettings(smtp_host="m")) is None                      # no alert recipients


def test_make_report_transport_none_when_unconfigured():
    assert make_report_transport(OpsSettings(smtp_host="m")) is None                # no report recipients


def test_factories_build_the_edges_when_configured():
    s = OpsSettings(universe=("BTC/USD",), smtp_host="m", alert_recipients=("a@x",),
                    report_recipients=("r@z",))
    sink = make_alert_sink(s, smtp_send=lambda f, t, m: None)
    tr = make_report_transport(s, smtp_send=lambda f, t, m: None)
    assert isinstance(sink, AlertEmailSender) and isinstance(tr, SmtpReportTransport)


# ----------------------------------------------------------------------------- build_system end-to-end
def test_build_system_binds_alert_report_and_durable_edges(tmp_path):
    sent: list = []
    settings = OpsSettings(
        universe=("BTC/USD",), mode=Mode.PAPER, smtp_host="mail",
        alert_recipients=("alerts@tothbot.com",), report_recipients=("wstothjr@gmail.com",),
        records_dir=str(tmp_path),
    )
    system = asyncio.run(build_system(
        settings,
        rest_client=_FakeRest(), open_socket=_opener(),
        bucket=SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0),
        mpp_store=MppCapStore(), reward_store=ExpectedRewardStore(),
        smtp_send=lambda frm, to, msg: sent.append((frm, to, msg)),
        now_utc=_now, now_monotonic=lambda: 1.0, rest_sleep=_nosleep, pace_sleep=_nosleep,
    ))

    # the C2-C6 cadence (report transport) + the HR-LG-013 durable sink were wired from settings.
    assert system.pull_scheduler is not None
    assert system.trade_record_sink is not None

    # the HR-LG-009 C1 alert SMTP seam is bound on mod:Logger: a forced alert reaches the SMTP edge.
    logger = system.driver._logger
    assert logger._on_alert is not None
    logger.alert(SimpleNamespace(code="FULL_HALT_TRIGGERED", level="CRITICAL", drawdown_pct="10"))
    c1 = [m for (_f, _t, m) in sent if "X-TothBot-Track: c1-immediate" in m]
    assert len(c1) == 1 and "FULL_HALT_TRIGGERED" in c1[0]


def test_build_system_without_smtp_leaves_alert_seam_unwired(tmp_path):
    # no SMTP host -> no alert sink, no report transport; the durable sink still wires from records_dir.
    settings = OpsSettings(universe=("BTC/USD",), mode=Mode.PAPER, records_dir=str(tmp_path))
    system = asyncio.run(build_system(
        settings,
        rest_client=_FakeRest(), open_socket=_opener(),
        bucket=SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0),
        mpp_store=MppCapStore(), reward_store=ExpectedRewardStore(),
        now_utc=_now, now_monotonic=lambda: 1.0, rest_sleep=_nosleep, pace_sleep=_nosleep,
    ))
    assert system.driver._logger._on_alert is None            # no SMTP -> alert seam unwired
    assert system.pull_scheduler is None                      # no report transport -> no cadence
    assert system.trade_record_sink is not None               # the durable sink still built
