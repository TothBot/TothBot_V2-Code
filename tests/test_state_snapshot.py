"""Tests for the read-only STATE SNAPSHOT EMITTER (tothbot/recorder/state_snapshot.py, TB00793).

The emitter is a VIEW over live organism state written atomically to a local JSON file on the OHLC_5m
clock tick (throttled). It is READ-ONLY + best-effort: a missing/raising surface degrades that field to
null and NEVER raises into the hot path. Driven over hand fakes - no network, no organism."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from types import SimpleNamespace

from tothbot.exchange.daily_decision import DailyDecisionCache
from tothbot.exchange.position_mirror import PositionSide
from tothbot.recorder.state_snapshot import StateSnapshotEmitter
from tothbot.regime.taxonomy import Regime

UTC = timezone.utc


def _pos(side, qty, entry, atr, emergsl, ts="2026-06-18T20:00:00+00:00"):
    return SimpleNamespace(
        side=side, qty=Decimal(qty), avg_entry_price=Decimal(entry),
        atr_14_entry=Decimal(atr), emergsl_price=Decimal(emergsl), entry_timestamp_utc=ts,
    )


class _FakeWM:
    def __init__(self, positions=None, wallets=None, baselines=None, stop_mult="2.5"):
        self._positions = positions or {}
        self._wallets = wallets or {PositionSide.LONG: Decimal("5000"), PositionSide.SHORT: Decimal("5000")}
        self._baselines = baselines or {PositionSide.LONG: Decimal("5000"), PositionSide.SHORT: Decimal("5000")}
        self._decision_stop_mult = Decimal(stop_mult)

    def open_positions(self):
        return dict(self._positions)

    def wallet_balance(self, side):
        return self._wallets.get(side)

    def portfolio_baseline(self, side):
        return self._baselines.get(side)


class _FakeBbo:
    def __init__(self, quotes):
        self._q = quotes

    def bbo(self, symbol):
        return self._q.get(symbol)


class _FakeRegime:
    def __init__(self, by_symbol):
        self._b = by_symbol

    @property
    def symbols(self):
        return frozenset(self._b)

    def regime(self, symbol):
        return self._b.get(symbol)


class _FakeStore:
    def __init__(self, caches):
        self._c = caches

    def get(self, symbol):
        return self._c.get(symbol)


def _cache(fast, slow, atr="700", close="60000"):
    return DailyDecisionCache(
        close_24h=Decimal(close), ema_fast_24h=Decimal(fast),
        ema_slow_24h=Decimal(slow), atr_14_24h=Decimal(atr),
    )


def _conductor(trade_count, *, win_rate=None, net_rr=None, pending=0, floor=200):
    pool = SimpleNamespace(
        win_rate=(Decimal(win_rate) if win_rate is not None else None),
        net_reward_risk=(Decimal(net_rr) if net_rr is not None else None),
        _trade_floor=floor,
    )
    return SimpleNamespace(trade_count=trade_count, pending=tuple(range(pending)), _pool=pool)


def _emitter(path, **over):
    base = dict(
        wm=_FakeWM(), conductors={PositionSide.LONG: _conductor(0), PositionSide.SHORT: _conductor(0)},
        decision_store=_FakeStore({}), regime_cache=_FakeRegime({}), bbo_cache=_FakeBbo({}),
        warmups={}, interval_sec=45.0,
    )
    base.update(over)
    return StateSnapshotEmitter(path, **base)


# --------------------------------------------------------------------------- atomic write + structure
def test_emit_writes_a_valid_json_object_atomically():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "state_snapshot.json")
        em = _emitter(path, warmups={"BTC/USD": object(), "ETH/USD": object()})
        em.emit(datetime(2026, 6, 18, 22, 0, tzinfo=UTC))
        assert os.path.exists(path)
        assert not os.path.exists(path + ".tmp")        # the temp file was os.replace'd onto the target
        snap = json.loads(open(path, encoding="utf-8").read())
        assert snap["ts"] == "2026-06-18T22:00:00+00:00"
        assert snap["mode"] == "paper"
        assert snap["health"]["warm_pairs"] == 2
        assert set(snap["balances"]) == {"long", "short"}
        assert snap["balances"]["long"]["wallet"] == "5000"      # Decimal -> string on the wire
        assert isinstance(snap["positions"], list) and isinstance(snap["decision_board"], list)


def test_positions_carry_mark_unrealized_and_the_wide_l2_stop():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "s.json")
        pos = {"BTC/USD": _pos(PositionSide.LONG, "0.05", "60000", "700", "57900")}
        em = _emitter(
            path, wm=_FakeWM(positions=pos, stop_mult="2.5"),
            bbo_cache=_FakeBbo({"BTC/USD": (Decimal("60500"), Decimal("60600"))}),
        )
        snap = em.build(datetime(2026, 6, 18, 22, 0, tzinfo=UTC))
        row = snap["positions"][0]
        assert row["symbol"] == "BTC/USD" and row["side"] == "LONG"
        assert row["entry"] == "60000" and row["mark"] == "60500"       # LONG marks at the bid
        assert Decimal(row["unrealized_usd"]) == Decimal("25")           # (60500-60000)*0.05
        # the WIDE layer:L2 stop = entry - decision_atr_stop_mult(2.5) x daily ATR(700) = 60000 - 1750.
        assert Decimal(row["l2_stop"]) == Decimal("58250")
        assert row["l3_emergsl"] == "57900"


def test_decision_board_carries_cross_state_and_signed_gap():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "s.json")
        caches = {"AAA/USD": _cache("110", "100"), "BBB/USD": _cache("90", "100")}
        regime = _FakeRegime({"AAA/USD": Regime.TRENDING_POS_NORMAL, "BBB/USD": Regime.TRENDING_NEG_NORMAL})
        em = _emitter(path, decision_store=_FakeStore(caches), regime_cache=regime)
        board = {r["symbol"]: r for r in em.build()["decision_board"]}
        assert board["AAA/USD"]["bullish"] is True and board["BBB/USD"]["bullish"] is False
        assert board["AAA/USD"]["regime"] == "TRENDING_POS_NORMAL"
        # gap_pct = (fast-slow)/slow*100: AAA +10%, BBB -10% (the distance-to-cross sign).
        assert Decimal(board["AAA/USD"]["gap_pct"]) == Decimal("10")
        assert Decimal(board["BBB/USD"]["gap_pct"]) == Decimal("-10")


def test_ciats_summary_per_module():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "s.json")
        conductors = {
            PositionSide.LONG: _conductor(57, win_rate="0.28", net_rr="1.6", pending=1),
            PositionSide.SHORT: _conductor(0),
        }
        em = _emitter(path, conductors=conductors)
        ciats = em.build()["ciats"]
        assert ciats["long"]["trade_count"] == 57 and ciats["long"]["progress_to_floor"] == "57/200"
        assert ciats["long"]["win_rate"] == "0.28" and ciats["long"]["net_rr"] == "1.6"
        assert ciats["long"]["pending"] == 1
        assert ciats["short"]["trade_count"] == 0 and ciats["short"]["pending"] == 0


# --------------------------------------------------------------------------- throttle
def test_on_tick_throttles_to_the_interval():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "s.json")
        em = _emitter(path, interval_sec=45.0)
        t0 = datetime(2026, 6, 18, 22, 0, 0, tzinfo=UTC)
        em.on_tick(t0)
        mtime1 = os.path.getmtime(path)
        # within the interval -> no re-write (the file is untouched).
        em.on_tick(t0 + timedelta(seconds=30))
        assert os.path.getmtime(path) == mtime1
        # past the interval -> a fresh emit (content re-rendered, last-emit advances).
        snap_before = json.loads(open(path, encoding="utf-8").read())
        em.on_tick(t0 + timedelta(seconds=60))
        snap_after = json.loads(open(path, encoding="utf-8").read())
        assert snap_after["ts"] != snap_before["ts"]


def test_first_tick_always_emits():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "s.json")
        em = _emitter(path)
        em.on_tick(datetime(2026, 6, 18, 22, 0, tzinfo=UTC))
        assert os.path.exists(path)


# --------------------------------------------------------------------------- best-effort (never raises)
class _BoomWM:
    def open_positions(self):
        raise RuntimeError("boom")

    def wallet_balance(self, side):
        raise RuntimeError("boom")

    def portfolio_baseline(self, side):
        raise RuntimeError("boom")


def test_emit_is_best_effort_and_never_raises_on_bad_surfaces():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "s.json")
        em = _emitter(path, wm=_BoomWM())
        em.emit(datetime(2026, 6, 18, 22, 0, tzinfo=UTC))   # must not raise
        snap = json.loads(open(path, encoding="utf-8").read())
        # the raising surfaces degrade to null/empty, the snapshot is still a valid object.
        assert snap["health"]["open_position_count"] == 0
        assert snap["balances"]["long"]["wallet"] is None
        assert snap["positions"] == []


def test_a_write_failure_is_swallowed_and_leaves_the_prior_snapshot():
    with tempfile.TemporaryDirectory() as d:
        path = os.path.join(d, "s.json")
        em = _emitter(path)
        em.emit(datetime(2026, 6, 18, 22, 0, tzinfo=UTC))    # writes a first good snapshot
        good = open(path, encoding="utf-8").read()

        def _boom_replace(src, dst):
            raise OSError("disk full")

        em._replace = _boom_replace
        em.emit(datetime(2026, 6, 18, 23, 0, tzinfo=UTC))    # the swap fails -> swallowed
        assert open(path, encoding="utf-8").read() == good   # the prior snapshot is intact
