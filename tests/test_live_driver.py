"""Live-driver tests (pipeline/live_driver.py): the ohlc(5m)/ohlc(60m) stream -> sweep + HTF wiring.

Covers: on_ohlc_5m detects the candle close (ar:AR-045), steps the LiveIndicators ONLY for a
genuinely-new candle (the first fire re-emits the already-seeded committed[-1] and must NOT
double-count it - the ar:AR-016/AR-075 step guard), and runs sweep_pair per permitted side;
rule:HR-WM-012 skips the whole frame while reconnecting; an unknown pair is ignored. on_ohlc_60m
advances the HtfCache EMA(20)/EMA(50) incrementally (ar:AR-044) and drives wm.on_htf_ohlc_close
(EC-L1A-001 1H reversal). make_ciats_sink records every event to mod:Logger and ingests a
TRADE_CLOSE into the module's CiatsPool. Driven with asyncio.run over fakes - no network.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

from tothbot.ciats.conductor import ApprovalInbox, CiatsConductor
from tothbot.ciats.parameter_store import ParameterStore
from tothbot.ciats.pool import CiatsPool
from tothbot.ciats.regime_library import RegimeLibrary
from tothbot.exchange.candle_close import CommittedCandle
from tothbot.exchange.daily_decision import DailyDecisionCache, DailyDecisionStore
from tothbot.exchange.position_mirror import PositionSide
from tothbot.exchange.warmup import WarmupOrchestrator
from tothbot.pipeline.live_driver import (
    LiveSweepDriver,
    make_approval_alert_sink,
    make_ciats_learning_sink,
    make_ciats_sink,
)
from tothbot.pipeline.sweep import LiveProviders
from tothbot.recorder.logger import Logger
from tothbot.regime.taxonomy import Regime
from tothbot.rest.client import OhlcResponse, RestOhlcBar


# --------------------------------------------------------------------------- fakes
def _ohlc_response(n=60, start=100, step=1, span=2, base_time=1700000000, interval_sec=300):
    committed = tuple(
        RestOhlcBar(time=base_time + i * interval_sec,
                    open=Decimal(start) + Decimal(step) * i,
                    high=Decimal(start) + Decimal(step) * i + span,
                    low=Decimal(start) + Decimal(step) * i - span,
                    close=Decimal(start) + Decimal(step) * i,
                    volume=Decimal(1000 + (i * 37) % 500))
        for i in range(n)
    )
    forming = RestOhlcBar(time=base_time + n * interval_sec, open=Decimal(9), high=Decimal(9),
                          low=Decimal(9), close=Decimal(9), volume=Decimal(1))
    return OhlcResponse(committed=committed, forming=forming, last=committed[-1].time)


class _FakeRest:
    async def get_ohlc_data(self, pair, interval, *, since=None):
        return _ohlc_response(interval_sec=300 if interval == 5 else 3600)


class _NoSleep:
    async def __call__(self, _seconds):
        return None


def _warm(symbol="BTC/USD"):
    return asyncio.run(WarmupOrchestrator(_FakeRest(), sleep=_NoSleep()).warm_pair(symbol))


class _Pos:
    def __init__(self, side, qty, price):
        self.side, self.qty, self.avg_entry_price = side, Decimal(qty), Decimal(price)


class _FakeWM:
    def __init__(self):
        self._wallets = {PositionSide.LONG: Decimal("5000"), PositionSide.SHORT: Decimal("5000")}
        self.modules = {s: SimpleNamespace(portfolio_baseline=Decimal("5000")) for s in self._wallets}
        self.dispatched = []
        self.htf_calls = []

    def open_positions(self):
        return {}

    def position(self, symbol):
        return None

    def exit_cooldown_at(self, symbol, side):
        return None

    def consecutive_loss_count(self, symbol, side):
        return 0

    def wallet_balance(self, side):
        return self._wallets.get(side)

    def portfolio_baseline(self, side):
        mod = self.modules.get(side)
        return mod.portfolio_baseline if mod is not None else None

    async def dispatch_entry(self, side, symbol, **kw):
        self.dispatched.append((side, symbol))
        return True

    def on_htf_ohlc_close(self, symbol, ema_short, ema_long, *, bid=None, ask=None, **_):
        self.htf_calls.append((symbol, ema_short, ema_long, bid, ask))


class _FakeLogger:
    def __init__(self):
        self.records = []

    def record(self, record, *, module="default"):
        self.records.append((module, record))


def _cache(regime=Regime.TRENDING_POS_NORMAL):
    classification = SimpleNamespace(regime=regime, ema20=Decimal("105"), ema50=Decimal("100"))
    return SimpleNamespace(get=lambda s: classification)


def _providers():
    return LiveProviders(
        instrument=lambda s: ("online", True, "600000"),
        bbo=lambda s: (Decimal("59990"), Decimal("60000")),
        expected_reward=lambda s, r: Decimal("0.05"),
        mpp_abs_cap_pct=lambda s, side: Decimal("0.01"),
        base_per_trade_size=lambda s, side, ref: Decimal("50"),
        ws_state=lambda s: "Subscribed",
        new_cl_ord_id=lambda: "cl-1",
        new_deadline=lambda: "2026-06-15T07:30:00Z",
    )


def _driver(warmups, wm, regime_cache=None, **over):
    return LiveSweepDriver(
        warmups=warmups, regime_cache=regime_cache or _cache(), providers=_providers(),
        wm=wm, logger=_FakeLogger(), **over,
    )


def _frame_5m(symbol, begin, *, close="200000", high="201000", low="199000", vol="9000"):
    return {"data": [{"symbol": symbol, "interval_begin": begin, "open": "199500",
                      "high": high, "low": low, "close": close, "volume": vol}]}


# --------------------------------------------------------------------------- make_ciats_sink
def test_sink_records_and_ingests_trade_close():
    logger, pool = _FakeLogger(), CiatsPool()
    sink = make_ciats_sink(logger, "long", pool)
    tc = SimpleNamespace(event="TRADE_CLOSE", net_pl_usd=Decimal("10"),
                         net_gain_usd=Decimal("10"), net_loss_usd=Decimal("0"))
    sink(tc)
    assert pool.trade_count == 1
    assert logger.records and logger.records[0][0] == "long"


def test_sink_ignores_non_trade_close_for_ciats():
    logger, pool = _FakeLogger(), CiatsPool()
    sink = make_ciats_sink(logger, "short", pool)
    sink(SimpleNamespace(code="SIGNAL_REJECTED"))
    assert pool.trade_count == 0
    assert len(logger.records) == 1


def test_sink_chains_downstream():
    seen = []
    sink = make_ciats_sink(_FakeLogger(), "long", CiatsPool(), downstream=seen.append)
    evt = SimpleNamespace(code="X")
    sink(evt)
    assert seen == [evt]


# --------------------------------------------------------------------------- make_ciats_learning_sink
def _conductor(module="long"):
    return CiatsConductor(
        module=module, pool=CiatsPool(), regime_library=RegimeLibrary(),
        parameter_store=ParameterStore(),
    )


def _tc(net="10", regime="TRENDING_POS_NORMAL"):
    return SimpleNamespace(event="TRADE_CLOSE", net_pl_usd=Decimal(net), net_gain_usd=Decimal(net),
                           net_loss_usd=Decimal("0"), asset_regime=regime)


def test_learning_sink_drives_conductor_ingest_and_logs():
    logger, conductor = _FakeLogger(), _conductor()
    sink = make_ciats_learning_sink(logger, "long", conductor)
    sink(_tc())
    # the conductor's full learning loop ingested it (pool + the net-P/L drift series + regime bucket)
    assert conductor.trade_count == 1
    assert conductor._net_pl == [Decimal("10")]
    assert conductor._regimes.bucket_count(Regime.TRENDING_POS_NORMAL) == 1
    assert logger.records and logger.records[0][0] == "long"


def test_learning_sink_ignores_non_trade_close_for_ciats():
    logger, conductor = _FakeLogger(), _conductor("short")
    sink = make_ciats_learning_sink(logger, "short", conductor)
    sink(SimpleNamespace(code="SIGNAL_REJECTED"))
    assert conductor.trade_count == 0
    assert len(logger.records) == 1


def test_learning_sink_tolerates_an_unknown_or_missing_regime_token():
    conductor = _conductor()
    sink = make_ciats_learning_sink(_FakeLogger(), "long", conductor)
    sink(_tc(regime="NOT_A_REGIME"))         # unparseable token -> bucket skipped, pool still learns
    sink(SimpleNamespace(event="TRADE_CLOSE", net_pl_usd=Decimal("5"),
                         net_gain_usd=Decimal("5"), net_loss_usd=Decimal("0")))  # no asset_regime
    assert conductor.trade_count == 2
    assert conductor._regimes.total_count == 0


def test_learning_sink_chains_downstream():
    seen = []
    sink = make_ciats_learning_sink(_FakeLogger(), "long", _conductor(), downstream=seen.append)
    evt = SimpleNamespace(code="X")
    sink(evt)
    assert seen == [evt]


# --------------------------------------------------------------------------- approval + boundary edges
def test_approval_alert_sink_routes_to_the_logger_operator_seam():
    logger = Logger()
    on_approval = make_approval_alert_sink(logger)
    req = SimpleNamespace(code="CIATS_APPROVAL_REQUESTED", level="HIGH")
    on_approval(req)
    assert req in logger.alerts                       # the HR-LG-009 operator surface received it


def test_learning_sink_applies_an_inbox_approved_change_at_the_boundary():
    pool = CiatsPool(trade_floor=4)
    conductor = CiatsConductor(module="long", pool=pool, regime_library=RegimeLibrary(),
                               parameter_store=ParameterStore())
    for _ in range(3):
        conductor.ingest_close(_tc(net="2"))          # 3 wins
    conductor.ingest_close(SimpleNamespace(event="TRADE_CLOSE", net_pl_usd=Decimal("-1"),
                                           net_gain_usd=Decimal("0"), net_loss_usd=Decimal("1"),
                                           asset_regime=None))                          # 1 loss -> 4
    conductor.recompute_kelly(wallet_balance=Decimal("1000"))   # at the floor -> stages a proposal
    req = conductor.pending[0]
    inbox = ApprovalInbox()
    inbox.submit(req.request_id, approved=True)        # Bill approves (the injected operator edge)
    sink = make_ciats_learning_sink(Logger(), "long", conductor, inbox=inbox)
    sink(_tc(net="2"))                                 # a confirmed close = the HR-CI-003 boundary
    assert conductor.parameter_store.get("per_trade_size_usd") is not None  # applied at the boundary
    assert conductor.pending == ()


def test_learning_sink_never_applies_without_an_inbox_decision():
    pool = CiatsPool(trade_floor=4)
    conductor = CiatsConductor(module="long", pool=pool, regime_library=RegimeLibrary(),
                               parameter_store=ParameterStore())
    for _ in range(3):
        conductor.ingest_close(_tc(net="2"))
    conductor.ingest_close(SimpleNamespace(event="TRADE_CLOSE", net_pl_usd=Decimal("-1"),
                                           net_gain_usd=Decimal("0"), net_loss_usd=Decimal("1"),
                                           asset_regime=None))
    conductor.recompute_kelly(wallet_balance=Decimal("1000"))
    sink = make_ciats_learning_sink(Logger(), "long", conductor, inbox=ApprovalInbox())
    sink(_tc(net="2"))                                 # boundary fires but Bill never decided
    assert conductor.parameter_store.get("per_trade_size_usd") is None  # NEVER auto-applied
    assert len(conductor.pending) == 1                 # still pending, re-polled next boundary


def test_learning_sink_stages_kelly_off_the_close_with_a_wallet_balance():
    # TB00749: the running close drives the Half-Kelly recompute THROUGH the sink (no manual call) -
    # when wallet_balance is wired, the cadence boundary STAGES the per_trade_size proposal to Bill.
    approvals: list = []
    pool = CiatsPool(trade_floor=4)
    conductor = CiatsConductor(module="long", pool=pool, regime_library=RegimeLibrary(),
                               parameter_store=ParameterStore(), on_approval=approvals.append)
    conductor.ingest_close(_tc(net="2"))
    conductor.ingest_close(_tc(net="2"))
    conductor.ingest_close(SimpleNamespace(event="TRADE_CLOSE", net_pl_usd=Decimal("-1"),
                                           net_gain_usd=Decimal("0"), net_loss_usd=Decimal("1"),
                                           asset_regime=None))                  # 2 wins, 1 loss = 3
    sink = make_ciats_learning_sink(Logger(), "long", conductor,
                                    wallet_balance=lambda: Decimal("1000"))
    sink(_tc(net="2"))                                 # the 4th close = the cadence boundary -> stages
    assert len(conductor.pending) == 1
    assert approvals and approvals[-1].kind == "kelly"   # the proposal reached the HR-CI-011 surface


def test_learning_sink_without_wallet_balance_does_not_recompute_kelly():
    # the live-mode guard: no wallet_balance thunk -> the sink never drives the Half-Kelly recompute
    # (no _dec(None) crash at the cadence boundary), the seed sizing stands.
    approvals: list = []
    pool = CiatsPool(trade_floor=4)
    conductor = CiatsConductor(module="short", pool=pool, regime_library=RegimeLibrary(),
                               parameter_store=ParameterStore(), on_approval=approvals.append)
    conductor.ingest_close(_tc(net="2"))
    conductor.ingest_close(_tc(net="2"))
    conductor.ingest_close(SimpleNamespace(event="TRADE_CLOSE", net_pl_usd=Decimal("-1"),
                                           net_gain_usd=Decimal("0"), net_loss_usd=Decimal("1"),
                                           asset_regime=None))                  # 3
    sink = make_ciats_learning_sink(Logger(), "short", conductor)   # no wallet_balance
    sink(_tc(net="2"))                                 # the 4th close = the cadence boundary
    assert conductor.pending == () and approvals == []   # nothing staged (recompute skipped)


# --------------------------------------------------------------------------- on_ohlc_5m
def test_in_progress_candle_does_not_fire():
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm)
    # A message for the SAME interval as the seed -> in-progress, no close, no sweep.
    results = asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", pw.last_interval_begin)))
    assert results == []
    assert wm.dispatched == []


def test_first_roll_sweeps_without_stepping_indicators():
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm)
    atr_before = pw.indicators.atr_14
    # First roll (begin = seed + 300) -> fires the seeded committed[-1] (sweep) but does NOT
    # re-step the indicators (the guard: that candle is already in the seed).
    results = asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", pw.last_interval_begin + 300)))
    assert len(results) == 1                       # TRENDING_POS_NORMAL -> long only
    assert pw.indicators.atr_14 == atr_before      # NOT double-counted


def test_second_roll_steps_indicators():
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm)
    atr_before = pw.indicators.atr_14
    seed = pw.last_interval_begin
    asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", seed + 300)))   # first roll (no step)
    asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", seed + 600)))   # rolls the +300 candle -> steps
    assert pw.indicators.atr_14 != atr_before      # the +300 candle (big TR) was stepped in


def test_on_committed_5m_persists_only_genuinely_new_closes():
    # TB00775 #1-B: the durable 5m sink fires once per genuinely-new close, NOT for the re-emitted seed
    # candle (it shares the indicator-step guard, so it dedups identically).
    pw = _warm()
    wm = _FakeWM()
    persisted = []
    driver = _driver({"BTC/USD": pw}, wm, on_committed_5m=persisted.append)
    seed = pw.last_interval_begin
    asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", seed + 300)))   # first roll = the SEEDED candle
    assert persisted == []                                            # not persisted (already counted)
    asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", seed + 600)))   # rolls the +300 candle -> persist
    assert len(persisted) == 1
    assert persisted[0].symbol == "BTC/USD" and persisted[0].interval_begin == seed + 300


def test_hr_wm_012_skips_while_reconnecting():
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm, is_reconnecting=lambda: True)
    atr_before = pw.indicators.atr_14
    results = asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", pw.last_interval_begin + 300)))
    assert results == []
    assert pw.indicators.atr_14 == atr_before      # no step on a partial universe


def test_unknown_pair_ignored():
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm)
    results = asyncio.run(driver.on_ohlc_5m(_frame_5m("DOGE/USD", 1700000000)))
    assert results == []


# --------------------------------------------------------------------------- on_ohlc_60m
def test_60m_drives_htf_close_and_advances_emas():
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm)
    ema20_seed = pw.htf.ema20_1h
    seed60 = pw.last_interval_begin_60
    frame1 = {"data": [{"symbol": "BTC/USD", "interval_begin": seed60 + 3600,
                        "open": "150", "high": "151", "low": "149", "close": "150", "volume": "5"}]}
    frame2 = {"data": [{"symbol": "BTC/USD", "interval_begin": seed60 + 7200,
                        "open": "300", "high": "301", "low": "299", "close": "300", "volume": "5"}]}
    driver.on_ohlc_60m(frame1)   # first 1H roll: fires committed[-1], no EMA step
    driver.on_ohlc_60m(frame2)   # rolls the +3600 (close 150) candle -> steps the 1H EMAs
    assert len(wm.htf_calls) == 2
    assert wm.htf_calls[0][0] == "BTC/USD"
    assert wm.htf_calls[-1][3] == Decimal("59990") and wm.htf_calls[-1][4] == Decimal("60000")  # bbo
    assert pw.htf.ema20_1h != ema20_seed           # EMA advanced on the genuine 1H close


# ----------------------------------------------------- TB00768 Opt 5: 1H DERIVED from the 5m stream
def test_derived_1h_from_5m_advances_htf_and_drives_reversal():
    # Kraken refuses the WS ohlc_60m subscription, so the 1H feed is folded from the 5m stream: a
    # complete hour of 5m closes must advance the HtfCache + drive EC-L1A-001, NOT freeze the cache.
    pw = _warm()
    wm = _FakeWM()
    driver = _driver({"BTC/USD": pw}, wm)
    ema20_seed = pw.htf.ema20_1h
    base = max(pw.last_interval_begin, pw.last_interval_begin_60)
    hour = (base // 3600 + 2) * 3600           # a round hour well past both warm-up seeds
    # Feed hour .. hour+3600: the closed-candle lag means the 12th contiguous close (hour+3300)
    # folds on the hour+3600 frame and EAGER-emits the derived 1H candle.
    for k in range(13):
        asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", hour + k * 300, close=str(150 + k))))
    assert any(c[0] == "BTC/USD" for c in wm.htf_calls)   # the derived 1H close drove EC-L1A-001
    assert pw.htf.ema20_1h != ema20_seed                  # HtfCache advanced (no longer frozen)
    assert pw.htf.close_1h == Decimal(161)                # close of the [:55,:00) slot (k=11) - lossless


def test_derived_1h_hour_aligned_gap_is_surfaced_not_a_corrupt_candle():
    # A reconnect drops a 5m close mid-hour: the hour-aligned bucket rolls over short of twelve ->
    # an HTF_1H_GAP is recorded (self-heal signal) and the corrupt hour is NOT folded into a candle.
    pw = _warm()
    wm = _FakeWM()
    logger = _FakeLogger()
    driver = LiveSweepDriver(
        warmups={"BTC/USD": pw}, regime_cache=_cache(), providers=_providers(), wm=wm, logger=logger,
    )
    base = max(pw.last_interval_begin, pw.last_interval_begin_60)
    hour = (base // 3600 + 2) * 3600
    # Frames hour, hour+300, ... but SKIP one slot (hour+1500); the closed-candle lag means we must
    # feed two next-hour frames to roll the (short, hour-aligned) bucket over -> the gap fires.
    begins = [hour + k * 300 for k in range(12) if k != 5] + [hour + 3600, hour + 3900]
    for b in begins:
        asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", b)))
    gaps = [r for _, r in logger.records if getattr(r, "code", None) == "HTF_1H_GAP"]
    assert any(g.symbol == "BTC/USD" and g.hour_begin == hour for g in gaps)


def test_htf_1h_gap_self_heals_from_rest_when_a_client_is_wired():
    # TB00769: with an htf_rest_client wired, a Htf1hGap auto-refetches GetOHLCData(60) and RE-SEEDS
    # the HtfCache (HTF_1H_HEAL), then drives EC-L1A-001 once on the healed EMAs (a reversal the gap
    # would have hidden still fires). The _FakeRest 60m series ends at close 159 (start 100 + 59).
    pw = _warm()
    wm = _FakeWM()
    logger = _FakeLogger()
    driver = LiveSweepDriver(
        warmups={"BTC/USD": pw}, regime_cache=_cache(), providers=_providers(), wm=wm, logger=logger,
        htf_rest_client=_FakeRest(),
    )
    base = max(pw.last_interval_begin, pw.last_interval_begin_60)
    hour = (base // 3600 + 2) * 3600
    begins = [hour + k * 300 for k in range(12) if k != 5] + [hour + 3600, hour + 3900]
    for b in begins:
        asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", b)))
    heals = [r for _, r in logger.records if getattr(r, "code", None) == "HTF_1H_HEAL"]
    assert any(h.symbol == "BTC/USD" and h.hour_begin == hour for h in heals)
    assert pw.htf.close_1h == Decimal(159)            # HtfCache RE-SEEDED from the REST 1H series
    assert any(c[0] == "BTC/USD" for c in wm.htf_calls)  # EC-L1A-001 driven on the healed EMAs


def test_htf_1h_gap_without_a_rest_client_records_gap_but_does_not_heal():
    # No htf_rest_client wired (a bring-up/unit assembly): the gap is recorded but NOT auto-healed
    # (the cache resumes on the next complete hour, bounded) - no HTF_1H_HEAL, no reversal drive.
    pw = _warm()
    wm = _FakeWM()
    logger = _FakeLogger()
    driver = LiveSweepDriver(
        warmups={"BTC/USD": pw}, regime_cache=_cache(), providers=_providers(), wm=wm, logger=logger,
    )
    base = max(pw.last_interval_begin, pw.last_interval_begin_60)
    hour = (base // 3600 + 2) * 3600
    begins = [hour + k * 300 for k in range(12) if k != 5] + [hour + 3600, hour + 3900]
    for b in begins:
        asyncio.run(driver.on_ohlc_5m(_frame_5m("BTC/USD", b)))
    assert not any(getattr(r, "code", None) == "HTF_1H_HEAL" for _, r in logger.records)
    assert wm.htf_calls == []


# ------------------------------------------- TB00789: the 24h DECISION feed (fold_hour -> DailyDecisionCache)
DAY = 86400


def _daily_bars(n, symbol="BTC/USD", start=100):
    """n daily bars (newest-last) with a gently rising close so EMA(12) seeds above EMA(26)."""
    out = []
    for i in range(n):
        close = Decimal(start + i)
        out.append(CommittedCandle(symbol=symbol, interval_begin=i * DAY, open=close - 1,
                                   high=close + 4, low=close - 5, close=close, volume=Decimal(1)))
    return out


def _h1(symbol, begin, close):
    """A folded 1H CommittedCandle (the input fold_hour folds one timeframe up into the 24h decision)."""
    c = Decimal(close)
    return CommittedCandle(symbol=symbol, interval_begin=begin, open=c - 1, high=c + 2, low=c - 2,
                           close=c, volume=Decimal(5))


def _seeded_store(symbol="BTC/USD", n=30):
    store = DailyDecisionStore()
    store.seed_from_bars(symbol, _daily_bars(n, symbol))
    return store


def test_advance_decision_steps_the_daily_cache_on_a_complete_day():
    # Feed twenty-four contiguous day-aligned 1H closes: the OhlcAggregator second fold stage eager-
    # emits a Closed24H on the twenty-fourth, which advances the pair's DailyDecisionCache.
    pw = _warm()
    store = _seeded_store()
    seed_close = store.get("BTC/USD").close_24h
    driver = _driver({"BTC/USD": pw}, _FakeWM(), decision_store=store)
    day = (max(pw.last_interval_begin, pw.last_interval_begin_60) // DAY + 2) * DAY
    for h in range(24):
        asyncio.run(driver._advance_decision(_h1("BTC/USD", day + h * 3600, 5000 + h)))
    cache = store.get("BTC/USD")
    assert cache.close_24h == Decimal(5023)        # the 24h close = the last (hour-23) 1H close - lossless
    assert cache.close_24h != seed_close           # the cache advanced off the seed


def test_advance_decision_no_store_is_a_noop():
    # A unit assembly that wires no decision_store: the second fold stage is simply not driven.
    pw = _warm()
    driver = _driver({"BTC/USD": pw}, _FakeWM())   # decision_store defaults to None
    day = (max(pw.last_interval_begin, pw.last_interval_begin_60) // DAY + 2) * DAY
    for h in range(24):
        asyncio.run(driver._advance_decision(_h1("BTC/USD", day + h * 3600, 5000 + h)))  # no crash


def test_24h_day_aligned_gap_self_heals_from_rest_1440_when_a_client_is_wired():
    # A 1H step the TB00769 heal could not recover leaves the day short of twenty-four -> Htf24hGap;
    # with htf_rest_client wired, the decision cache re-seeds from one REST GetOHLCData(1440)
    # (HTF_24H_HEAL), the exact mirror of the 1H heal one timeframe up.
    pw = _warm()
    store = _seeded_store()
    logger = _FakeLogger()
    driver = LiveSweepDriver(
        warmups={"BTC/USD": pw}, regime_cache=_cache(), providers=_providers(), wm=_FakeWM(),
        logger=logger, decision_store=store, htf_rest_client=_FakeRest(),
    )
    day = (max(pw.last_interval_begin, pw.last_interval_begin_60) // DAY + 2) * DAY
    hours = [day + h * 3600 for h in range(24) if h != 5] + [day + DAY]   # skip hour 5, then next day
    for b in hours:
        asyncio.run(driver._advance_decision(_h1("BTC/USD", b, 7000)))
    gaps = [r for _, r in logger.records if getattr(r, "code", None) == "HTF_24H_GAP"]
    heals = [r for _, r in logger.records if getattr(r, "code", None) == "HTF_24H_HEAL"]
    assert any(g.symbol == "BTC/USD" and g.day_begin == day for g in gaps)
    assert any(h.symbol == "BTC/USD" and h.day_begin == day for h in heals)
    assert store.get("BTC/USD") is not None         # cache re-seeded from the REST 1440 series


def test_24h_gap_without_a_rest_client_records_gap_but_does_not_heal():
    pw = _warm()
    store = _seeded_store()
    logger = _FakeLogger()
    driver = LiveSweepDriver(
        warmups={"BTC/USD": pw}, regime_cache=_cache(), providers=_providers(), wm=_FakeWM(),
        logger=logger, decision_store=store,                       # no htf_rest_client
    )
    day = (max(pw.last_interval_begin, pw.last_interval_begin_60) // DAY + 2) * DAY
    hours = [day + h * 3600 for h in range(24) if h != 5] + [day + DAY]
    for b in hours:
        asyncio.run(driver._advance_decision(_h1("BTC/USD", b, 7000)))
    assert any(getattr(r, "code", None) == "HTF_24H_GAP" for _, r in logger.records)
    assert not any(getattr(r, "code", None) == "HTF_24H_HEAL" for _, r in logger.records)


def test_derived_24h_from_the_5m_stream_advances_the_daily_cache_end_to_end():
    # The full chain through on_ohlc_5m: a clean UTC day of 5m closes folds to twenty-four 1H candles
    # (fold) which fold one timeframe up (fold_hour) into one Closed24H that advances the cache. One
    # frame carries the whole day + the rollover candle (the closed-candle lag fires the 23:55 close).
    pw = _warm()
    store = _seeded_store()
    seed_close = store.get("BTC/USD").close_24h
    driver = _driver({"BTC/USD": pw}, _FakeWM(), decision_store=store)
    day = (max(pw.last_interval_begin, pw.last_interval_begin_60) // DAY + 2) * DAY
    # 289 contiguous 5m candles: day .. day+DAY inclusive (the last fires the 23:55 close).
    data = [{"symbol": "BTC/USD", "interval_begin": day + k * 300, "open": "199500",
             "high": str(200000 + k), "low": "199000", "close": str(200000 + k), "volume": "9000"}
            for k in range(289)]
    asyncio.run(driver.on_ohlc_5m({"data": data}))
    cache = store.get("BTC/USD")
    # 24h close = close of the [23:00,00:00) hour = close of its 23:55 5m slot = candle k=287.
    assert cache.close_24h == Decimal(200000 + 287)
    assert cache.close_24h != seed_close
