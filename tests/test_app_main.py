"""ar:AR-049/AR-070 cold-start entrypoint (tothbot/app/__main__.py) - the real-edge composition.

Exercises make_public_open_socket (routes to the PUBLIC endpoint) and _amain end to end over injected
seams (connect_fn yields a fake transport scripted with an instrument snapshot; run_fn captures the
call): the universe is loaded from the snapshot and threaded into settings, the paper edges are built
(KrakenRestClient / SubscribeTokenBucket / the CIATS seed stores), and live mode HALTs with a clear
message. No network, no real run loop.
"""

from __future__ import annotations

import asyncio

import pytest

from tothbot.app.__main__ import _amain, console_event_sink, make_public_open_socket
from tothbot.ciats.expected_reward import ExpectedRewardStore
from tothbot.ciats.seed_estimators import MppCapStore
from tothbot.config.settings import Mode
from tothbot.exchange.connection import ConnectionRole
from tothbot.exchange.pacing import SubscribeTokenBucket
from tothbot.rest.client import KrakenRestClient


# --------------------------------------------------------------------------- fakes
class _FakeTransport:
    def __init__(self, recv_frames) -> None:
        self.sent: list = []
        self._recv = list(recv_frames)
        self.closed = False

    async def send(self, m) -> None:
        self.sent.append(m)

    async def recv(self) -> dict:
        return self._recv.pop(0)

    async def close(self) -> None:
        self.closed = True


def _snapshot(symbols):
    pairs = [{
        "symbol": s, "status": "online", "marginable": True,
        "qty_min": "0.1", "cost_min": "5", "price_increment": "0.01", "qty_increment": "0.1",
    } for s in symbols]
    return {"channel": "instrument", "type": "snapshot", "data": {"pairs": pairs}}


# --------------------------------------------------------------------------- make_public_open_socket
def test_open_socket_routes_to_public_endpoint():
    seen: list = []

    async def fake_connect(role):
        seen.append(role)
        return _FakeTransport([])

    opener = make_public_open_socket(fake_connect)
    asyncio.run(opener(3))                       # any shard index
    assert seen == [ConnectionRole.PUBLIC]        # always the public endpoint


# --------------------------------------------------------------------------- _amain composition
def test_amain_loads_universe_and_runs_paper_edges():
    captured: dict = {}

    async def fake_connect(role):
        # the universe-load opens ONE socket; script its instrument snapshot.
        return _FakeTransport([_snapshot(["ETH/USD", "SOL/USDT"])])

    async def fake_run(settings, **edges):
        captured["settings"] = settings
        captured["edges"] = edges

    asyncio.run(_amain({"TOTHBOT_MODE": "paper"}, connect_fn=fake_connect, run_fn=fake_run))

    settings = captured["settings"]
    assert settings.mode is Mode.PAPER
    # the AR-070 universe was loaded from the snapshot (+ the BTC/USD anchor) and threaded in.
    assert settings.universe == ("BTC/USD", "ETH/USD", "SOL/USDT")
    edges = captured["edges"]
    assert isinstance(edges["rest_client"], KrakenRestClient)
    assert isinstance(edges["bucket"], SubscribeTokenBucket)
    assert isinstance(edges["mpp_store"], MppCapStore)
    assert isinstance(edges["reward_store"], ExpectedRewardStore)
    assert callable(edges["open_socket"])


def test_console_event_sink_prints_event_code(capsys):
    from types import SimpleNamespace
    console_event_sink(SimpleNamespace(code="PAIR_DATA_READY_RECOVERED", symbol="BTC/USD"))
    out = capsys.readouterr().out
    assert "PAIR_DATA_READY_RECOVERED" in out and out.startswith("[evt]")


def test_amain_wires_console_event_sink_as_on_event():
    async def fake_connect(role):
        return _FakeTransport([_snapshot(["ETH/USD"])])

    captured: dict = {}

    async def fake_run(settings, **edges):
        captured["edges"] = edges

    asyncio.run(_amain({"TOTHBOT_MODE": "paper"}, connect_fn=fake_connect, run_fn=fake_run))
    assert captured["edges"]["on_event"] is console_event_sink


def test_amain_universe_override_pins_pairs_and_skips_ar070_load():
    # TOTHBOT_UNIVERSE pins a small fixed universe for a smoke run: load_universe is SKIPPED (the
    # socket is never opened for a snapshot), the pinned pairs are used directly, and the BTC/USD
    # anchor is unioned in (ar:AR-074) even though the operator listed only ETH/USD + SOL/USD.
    connected: list = []

    async def fake_connect(role):  # pragma: no cover - must NOT be called on the pinned path
        connected.append(role)
        raise AssertionError("pinned universe must not open a socket for the AR-070 load")

    captured: dict = {}

    async def fake_run(settings, **edges):
        captured["settings"] = settings

    asyncio.run(_amain(
        {"TOTHBOT_MODE": "paper", "TOTHBOT_UNIVERSE": "ETH/USD, SOL/USD"},
        connect_fn=fake_connect, run_fn=fake_run,
    ))
    assert captured["settings"].universe == ("BTC/USD", "ETH/USD", "SOL/USD")  # anchor unioned, sorted
    assert connected == []  # the AR-070 snapshot load was skipped


def test_amain_live_mode_halts_with_clear_message():
    async def fake_connect(role):  # pragma: no cover - never reached (the guard fires first)
        raise AssertionError("live must not connect")

    async def fake_run(settings, **edges):  # pragma: no cover
        raise AssertionError("live must not run")

    with pytest.raises(SystemExit) as exc:
        asyncio.run(_amain({"TOTHBOT_MODE": "live"}, connect_fn=fake_connect, run_fn=fake_run))
    assert "live" in str(exc.value).lower()
