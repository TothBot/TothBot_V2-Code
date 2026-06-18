"""Live END-TO-END smoke (the capstone): drive the ASSEMBLED organism over a fake public-WS frame
stream and prove a full decide->size->dispatch tick + the TRADE_CLOSE -> CiatsPool learning close.

Source: 0500000 dv1_250 ar:AR-049 (the cold-start assembly) + Image2 (the 8-gate pipeline) + the
contract:OHLC_5m_System_Clock tick + sec 7 (mod:Logger Stream-2 -> the per-module CiatsPool). The unit
tests cover each piece in isolation; THIS test runs the whole thing wired together by assemble_
operational: a frame stream (INSTRUMENT snapshot -> TICKER -> a 5m close -> a 1H close) flows through
the assembled data layer + driver, drives one (pair, side) candidate through every gate, and dispatches
the entry into the module wallet - paper mode, no network, no timers (asyncio.run over fakes).

DETERMINISM: the public-WS frames + the warm-up/daily REST series are crafted so the pair classifies
TRENDING_POS (long permitted), the warmed 5m indicators yield a passing long SSS (a pullback-in-uptrend
- RSI in (30,50) with EMA9 > EMA21 + a closing volume spike), and the candidate clears the sacred 1:1.5
R:R floor. The two CIATS seed values the A1 floor reads (expected_reward DEC-124, mpp DEC-128) are put
into their stores as known values after the load seeding (the store IS the CIATS-owned source - this
fixes the gate input, it does not bypass any wiring). The TRADE_CLOSE learning close is exercised with
a schema-valid 25-field record routed through the per-module Logger Stream-2 sink + the CiatsConductor.
"""

from __future__ import annotations

import asyncio
import os
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from tothbot.ciats.conductor import CiatsConductor
from tothbot.ciats.expected_reward import ExpectedRewardStore
from tothbot.ciats.parameter_store import ParameterStore
from tothbot.ciats.pool import CiatsPool
from tothbot.ciats.regime_library import RegimeLibrary
from tothbot.ciats.seed_estimators import MppCapStore
from tothbot.config.settings import Mode
from tothbot.exchange.channels import PublicChannel
from tothbot.exchange.pacing import SubscribeTokenBucket
from tothbot.exchange.candle_close import CommittedCandle
from tothbot.exchange.daily_decision import DailyDecisionStore
from tothbot.exchange.position_mirror import PositionSide
from tothbot.exchange.warmup import WarmupOrchestrator
from tothbot.exchange.ws_manager import WSManager
from tothbot.execution.exit_controller import ExitReason, TradeClose
from tothbot.pipeline.live_driver import LiveSweepDriver, make_ciats_sink
from tothbot.pipeline.operational import assemble_operational
from tothbot.pipeline.sweep import LiveProviders
from tothbot.recorder.logger import Logger
from tothbot.regime.taxonomy import Regime
from tothbot.rest.client import OhlcResponse, RestOhlcBar


# --------------------------------------------------------------------------- crafted REST series
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


def _daily_closes():
    # net-rising with periodic dips -> TRENDING_POS + run-to-reversal reversals (DEC-124 seeds).
    out, v = [], 100
    for i in range(85):
        v += 6 if i % 5 else -4
        out.append(v)
    return out


def _5m_closes():
    # a 40-bar steep uptrend (EMA9 >> EMA21) then an 18-bar mild pullback (RSI down into (30,50)).
    out, v = [], 100
    for _ in range(40):
        v += 5
        out.append(v)
    for _ in range(18):
        v -= 2
        out.append(v)
    return out


class _FakeRest:
    async def get_ohlc_data(self, pair, interval, *, since=None):
        if interval == 1440:
            return _resp(_daily_closes(), interval=86400)
        if interval == 5:
            return _resp(_5m_closes(), interval=300, last_vol=5000)
        return _resp([100 + i for i in range(60)], interval=3600)

    async def get_ticker_liquidity(self, pair):
        return Decimal("600000")


class _FakeTransport:
    def __init__(self):
        self.sent = []

    async def send(self, m):
        self.sent.append(m)

    async def recv(self):  # pragma: no cover
        raise AssertionError

    async def close(self):  # pragma: no cover
        pass


def _opener():
    async def open_socket(_k):
        return _FakeTransport()
    return open_socket


class _TradeWM:
    """A paper WSManager stand-in with the full decide->size->dispatch surface + the HTF close seam."""

    is_live = False

    def __init__(self):
        self._w = {PositionSide.LONG: Decimal("5000"), PositionSide.SHORT: Decimal("5000")}
        self.modules = {s: SimpleNamespace(portfolio_baseline=Decimal("5000")) for s in self._w}
        self.dispatched: list = []
        self.htf_calls: list = []
        self.regime_calls: list = []

    def open_positions(self):
        return {}

    def position(self, _s):
        return None

    def exit_cooldown_at(self, _s, _side):
        return None

    def consecutive_loss_count(self, _s, _side):
        return 0

    def wallet_balance(self, side):
        return self._w.get(side)

    def portfolio_baseline(self, side):
        mod = self.modules.get(side)
        return mod.portfolio_baseline if mod is not None else None

    async def dispatch_entry(self, side, symbol, **kw):
        self.dispatched.append((side, symbol))
        return True

    def on_regime_classified(self, symbol, *a, **k):
        self.regime_calls.append(symbol)

    def on_htf_ohlc_close(self, symbol, ema_s, ema_l, *, bid=None, ask=None, **_):
        self.htf_calls.append((symbol, bid, ask))


def _trade_close(symbol="BTC/USD", net="120"):
    """A schema-valid 25-field TRADE_CLOSE (the real exit_controller dataclass) for the corpus."""
    return TradeClose(
        symbol=symbol,
        entry_fill_price=Decimal("60000"), exit_price=Decimal("66000"),
        exit_reason=ExitReason.HTF_REGIME_REVERSAL,
        fees_entry_usd=Decimal("7.8"), fees_exit_usd=Decimal("8.58"), fees_total_usd=Decimal("16.38"),
        net_pl_usd=Decimal(net), net_gain_usd=Decimal(net), net_loss_usd=Decimal("0"),
    )


def _assemble():
    """Assemble the paper organism over the crafted fakes + a tradeable wm + a real Logger, then fix
    the CIATS A1-floor inputs (expected_reward / mpp) to known seeds so the gate is deterministic."""
    rest = _FakeRest()
    wm, logger = _TradeWM(), Logger()
    mpp, reward = MppCapStore(), ExpectedRewardStore()

    async def no_sleep(_s):
        return None

    system = asyncio.run(assemble_operational(
        universe=["BTC/USD"], rest_client=rest, open_socket=_opener(),
        bucket=SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0),
        wm=wm, logger=logger, mpp_store=mpp, reward_store=reward, mode=Mode.PAPER,
        now_utc=lambda: datetime(2026, 6, 15, 7, 30, tzinfo=timezone.utc),
        rest_sleep=no_sleep, pace_sleep=no_sleep,
    ))
    # The store IS the CIATS-owned source: fix the A1-floor inputs to known values (DEC-124 / DEC-128).
    for r in Regime:
        reward.put("BTC/USD", r, Decimal("0.5"))
    mpp.put("BTC/USD", PositionSide.LONG, Decimal("0.01"))
    mpp.put("BTC/USD", PositionSide.SHORT, Decimal("0.01"))
    # The pair is Subscribed (the receive loop's ACK) so gate:G1 PASSes; populate the snapshot caches.
    system.silent_pairs["BTC/USD"].mark_subscribed(now=0.0)
    shard0 = system.data_layer.shards[0]
    shard0.dispatch.dispatch(PublicChannel.INSTRUMENT, {"data": {"pairs": [
        {"symbol": "BTC/USD", "status": "online", "marginable": True, "qty_min": "0.0001",
         "cost_min": "0.5", "price_increment": "0.1", "qty_increment": "0.00000001"}]}})
    shard0.dispatch.dispatch(PublicChannel.TICKER,
                             {"data": [{"symbol": "BTC/USD", "bid": "176", "ask": "177"}]})
    return system, wm, logger


# --------------------------------------------------------------------------- the smoke
def test_frame_stream_populates_the_snapshot_caches():
    system, _wm, _logger = _assemble()
    # The INSTRUMENT + TICKER frames flowed through the assembled shard dispatch into the shared caches.
    assert system.instrument_cache.get("BTC/USD") is not None
    assert system.bbo_cache.bbo("BTC/USD") == (Decimal("176"), Decimal("177"))
    # The pair classified into a long-permitting trending regime during the cold-start.
    assert system.regime_cache.get("BTC/USD").regime in (
        Regime.TRENDING_POS_NORMAL, Regime.TRENDING_POS_ELEVATED
    )


def test_5m_close_does_not_dispatch_an_entry_decision_entry_owns_entries():
    # TB00790: the assembled organism routes entries through the 24h DECISION (decision_entry=True),
    # so a 5m close NO LONGER dispatches an entry (the 5m sweep entry is DISABLED - the validated
    # long-only ENTRY is the 24h EMA12/26 bullish cross). The 5m close still STEPS the LiveIndicators
    # (the clock + state maintenance) and drives the ticker-bbo exits; only the entry trigger moved.
    system, wm, logger = _assemble()
    pw = system.warmups["BTC/USD"]
    # Two 5m rolls: the first re-emits the warm-up seed candle (AR-016 guard - no entry under the old
    # path's re-emit either), the second is a genuinely-new close that the OLD architecture would have
    # swept into a dispatch. Under the 24h-decision routing NEITHER dispatches an entry.
    for k in (1, 2):
        frame = {"data": [{"symbol": "BTC/USD", "interval_begin": pw.last_interval_begin + 300 * k,
                           "open": "264", "high": "266", "low": "262", "close": "264", "volume": "5000"}]}
        results = asyncio.run(system.driver.on_ohlc_5m(frame))
        assert results == []                                   # the 5m sweep entry is disabled
    assert wm.dispatched == []                                 # no entry dispatched on any 5m close


def test_1h_close_maintains_htf_cache_without_the_retired_1h_reversal_drive():
    # TB00790: the 1H close still ADVANCES the HtfCache EMA(20)/EMA(50) (gate:G4 HTF confirmation reads
    # them), but the legacy 1H EMA20/50 reversal drive is RETIRED - the 24h EMA12/26 bearish cross owns
    # layer:L1a now, so wm.on_htf_ohlc_close is NOT driven from the 1H path.
    system, wm, _logger = _assemble()
    pw = system.warmups["BTC/USD"]
    seed60 = pw.last_interval_begin_60
    ema20_before = pw.htf.ema20_1h
    # Two 1H rolls: the first fires committed[-1] (no EMA step), the second steps the 1H EMAs (AR-044).
    for k in (1, 2):
        system.driver.on_ohlc_60m({"data": [{"symbol": "BTC/USD", "interval_begin": seed60 + 3600 * k,
                                              "open": "150", "high": "151", "low": "149",
                                              "close": "150", "volume": "5"}]})
    assert pw.htf.ema20_1h != ema20_before                     # the HtfCache EMA was maintained (gate:G4)
    assert wm.htf_calls == []                                  # the 1H reversal drive is retired (24h owns L1a)


def test_trade_close_closes_the_ciats_learning_loop():
    system, _wm, logger = _assemble()
    # The per-module learning close (sec 7): a schema-valid TRADE_CLOSE -> Stream-2 corpus + the pool,
    # and the CiatsConductor (the per-module loop) ingests it off that corpus.
    pool = CiatsPool()
    conductor = CiatsConductor(
        module="long", pool=CiatsPool(), regime_library=RegimeLibrary(), parameter_store=ParameterStore(),
    )
    sink = make_ciats_sink(logger, "long", pool)
    tc = _trade_close(net="120")
    sink(tc)                                   # -> Logger Stream-2 corpus + pool ingest
    conductor.ingest_close(tc, regime=Regime.TRENDING_POS_NORMAL)
    assert pool.trade_count == 1
    assert len(logger.corpus_for("long")) == 1            # the 25-field record entered the corpus
    assert conductor.trade_count == 1                     # the conductor's per-module pool learned it


# --------------------------------------------------------------------------- TB00748 (c): the
# RUNNING wm emits each exit THROUGH the side's CIATS sink (no manual sink call)

def _assemble_real_wm(*, report_emit=None, report_categories=None, records_dir=None):
    """Assemble the paper organism over a REAL WSManager (not the decide-size stand-in), so the
    exit path is wired end-to-end: assemble_operational hands the per-side ciats_sinks to the wm's
    per-module Exit Controllers (TB00748 (b)). `report_emit` / `report_categories` wire the
    contract:Operator_Reporting_Hierarchy C2-C6 PULL cadence into the OHLC_5m system clock (TB00755).
    Returns (system, wm, logger)."""
    wm, logger = WSManager(Mode.PAPER, now_monotonic=lambda: 1.0), Logger()

    async def no_sleep(_s):
        return None

    system = asyncio.run(assemble_operational(
        universe=["BTC/USD"], rest_client=_FakeRest(), open_socket=_opener(),
        bucket=SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0),
        wm=wm, logger=logger, mpp_store=MppCapStore(), reward_store=ExpectedRewardStore(),
        mode=Mode.PAPER, now_utc=lambda: datetime(2026, 6, 15, 7, 30, tzinfo=timezone.utc),
        rest_sleep=no_sleep, pace_sleep=no_sleep,
        report_emit=report_emit, report_categories=report_categories, records_dir=records_dir,
    ))
    return system, wm, logger


def _open_real_long(wm, *, atr="2000", emergsl="54000"):
    """Open a BTC/USD long paper position on the REAL wm (the sole-writer record_execution surface
    + the synthetic entry-fill debit), carrying the entry-time D6 snapshot for the L2 MAE detector."""
    wm.record_execution(
        {"exec_type": "filled", "symbol": "BTC/USD", "side": "buy",
         "cum_qty": "0.05", "avg_price": "60000", "cl_ord_id": "cl-1"},
        regime_at_entry="TRENDING_POS_NORMAL", atr_14_entry=atr, emergsl_price=emergsl,
    )
    wm.apply_paper_entry_fill("BTC/USD", "0.05", "60000")


def _adverse_ticker():
    # TB00790: the assembled organism's L2 stop is the WIDE param:decision_atr_stop_mult (2.5x). bid
    # 55000 -> long MAE 5000 >= atr 2000 * 2.5 = 5000 -> L2 threshold breach at the bid (and 55000 >
    # emergsl 54000, so L2 fires first, not the L3 backstop).
    return {"channel": "ticker", "type": "update",
            "data": [{"symbol": "BTC/USD", "bid": "55000", "ask": "55100"}]}


def test_running_exit_drives_the_trade_close_through_the_wired_sink():
    # The capstone: a REAL ticker-bbo exit on the assembled organism drives a schema-valid
    # TRADE_CLOSE THROUGH the LONG module's wired ciats_sink into that side's conductor - the
    # learning close (trade_count++, the Stream-2 corpus grows) - with NO manual sink call.
    system, wm, logger = _assemble_real_wm()
    conductor = system.conductors[PositionSide.LONG]
    assert conductor.trade_count == 0
    _open_real_long(wm)
    wm.handle_ticker(_adverse_ticker())                    # detect -> the per-module close path
    assert not wm.has_position("BTC/USD")                  # the mirror cleared (the close ran)
    assert conductor.trade_count == 1                      # the LONG conductor learned the close
    assert system.conductors[PositionSide.SHORT].trade_count == 0   # the short loop did NOT (per-module)
    assert len(logger.corpus_for("long")) == 1             # the 25-field record entered the Stream-2 corpus


def test_running_exit_applies_an_inbox_approved_change_at_the_boundary():
    # The HR-CI-003 boundary, end-to-end: a Bill-approved change in the side's inbox is APPLIED by
    # the REAL exit close (the confirmed inter-trade boundary the wired sink polls) - never auto-
    # applied, never a manual sink call. Stage a Half-Kelly proposal (200-trade activation), Bill
    # approves it into the inbox, then a running paper exit closes the boundary and applies it.
    system, wm, _ = _assemble_real_wm()
    side = PositionSide.LONG
    conductor = system.conductors[side]
    win = SimpleNamespace(net_pl_usd=Decimal("2"), net_gain_usd=Decimal("2"), net_loss_usd=Decimal("0"))
    loss = SimpleNamespace(net_pl_usd=Decimal("-1"), net_gain_usd=Decimal("0"), net_loss_usd=Decimal("1"))
    for _ in range(120):
        conductor.ingest_close(win)
    for _ in range(80):
        conductor.ingest_close(loss)                       # 200 trades: W=0.6, R=2 -> K_full=0.4 > 0
    conductor.recompute_kelly(wallet_balance=Decimal("5000"))
    req = conductor.pending[0]
    system.approval_inboxes[side].submit(req.request_id, approved=True)
    assert conductor.parameter_store.get("per_trade_size_usd") is None   # not applied yet (no boundary)
    # the running exit IS the confirmed inter-trade boundary: it ingests the close AND polls the inbox.
    _open_real_long(wm)
    wm.handle_ticker(_adverse_ticker())
    assert conductor.parameter_store.get("per_trade_size_usd") is not None   # applied at the real close
    assert conductor.trade_count == 201                    # the boundary close was the 201st learned trade


# --------------------------------------------------------------------------- TB00749 (c): the running
# close DRIVES the propose/detect cadence (stage to Bill + the drift trigger), through the wired sink

def _close_record(net, *, heat=None, **sp):
    """A schema-valid 25-field TRADE_CLOSE for the corpus; net>0 = a win, net<0 = a loss. `heat` is
    the per-trade mae_pct_reached (field 11) the stop-width theory reads; **sp are signal_params
    LEVELS the entry-filter theory reads."""
    n = Decimal(net)
    win = n > 0
    return TradeClose(
        symbol="BTC/USD", entry_fill_price=Decimal("60000"), exit_price=Decimal("66000"),
        exit_reason=ExitReason.HTF_REGIME_REVERSAL,
        fees_entry_usd=Decimal("0"), fees_exit_usd=Decimal("0"), fees_total_usd=Decimal("0"),
        net_pl_usd=n, net_gain_usd=(n if win else Decimal("0")),
        net_loss_usd=(Decimal("0") if win else -n), asset_regime="TRENDING_POS_NORMAL",
        mae_pct_reached=(Decimal(str(heat)) if heat is not None else None),
        signal_params=({k: Decimal(str(v)) for k, v in sp.items()} or None),
    )


def test_running_closes_stage_a_half_kelly_proposal_at_activation():
    # TB00749: closes flowing THROUGH the wired sink reach the 200-trade activation and STAGE the
    # Half-Kelly per_trade_size proposal to Bill (the alert reaches mod:Logger.alerts) with NO manual
    # recompute_kelly - the sink's on_close reads wm.wallet_balance(LONG) for the sizing.
    system, _wm, logger = _assemble_real_wm()
    side = PositionSide.LONG
    sink, conductor = system.ciats_sinks[side], system.conductors[side]
    for _ in range(120):
        sink(_close_record("2"))                           # wins
    for _ in range(80):
        sink(_close_record("-1"))                          # losses -> 200: W=0.6, R=2, K_full=0.4 > 0
    assert conductor.trade_count == 200
    assert len(logger.corpus_for("long")) == 200           # every close entered the Stream-2 corpus
    assert conductor.pending                               # a proposal was STAGED off the running close
    assert any(getattr(a, "code", None) == "CIATS_APPROVAL_REQUESTED" for a in logger.alerts)
    # the SHORT module is isolated - its loop staged nothing (per-module, sec 7).
    assert system.conductors[PositionSide.SHORT].pending == ()


def test_running_closes_then_approval_applies_at_the_next_close():
    # the full PROPOSE->APPROVE->APPLY loop on the organism, all through the wired sink (no manual
    # call): stage at the 200-trade activation -> Bill approves into the inbox -> the next close (the
    # HR-CI-003 boundary) APPLIES it.
    system, _wm, _logger = _assemble_real_wm()
    side = PositionSide.LONG
    sink = system.ciats_sinks[side]
    conductor, inbox = system.conductors[side], system.approval_inboxes[side]
    for _ in range(120):
        sink(_close_record("2"))
    for _ in range(80):
        sink(_close_record("-1"))                          # 200th close stages the proposal
    req = conductor.pending[0]
    inbox.submit(req.request_id, approved=True)
    assert conductor.parameter_store.get("per_trade_size_usd") is None   # staged, not yet applied
    sink(_close_record("2"))                               # the 201st close = the boundary -> applies
    assert conductor.parameter_store.get("per_trade_size_usd") is not None


def test_running_closes_emit_a_drift_signal_on_degradation():
    # a degrading net-P/L run drives scan_drift (the HR-CI-007 out-of-cycle PLAN trigger) at each
    # close; the DriftSignal is emitted into the module's Stream-1. (The drift-triggered candidate
    # PLAN awaits the per-trade param-level signal_params producer - not fabricated here.)
    system, _wm, logger = _assemble_real_wm()
    sink = system.ciats_sinks[PositionSide.LONG]
    for _ in range(30):
        sink(_close_record("2"))
    for _ in range(10):
        sink(_close_record("-10"))                         # a sharp drop -> the net-P/L CUSUM breaches
    assert any(getattr(e, "code", None) == "CIATS_DRIFT_SIGNAL" for e in logger.operational)


# ------------------------------------------------ TB00751 (b): the drift-triggered FORM->TEST->ROUTE
# loop on the assembled real-wm organism - a tested stop-width tighten EMAILS Bill / a loosen REPORTS

def _mae_alert(logger):
    return [a for a in logger.alerts if getattr(a, "code", None) == "CIATS_APPROVAL_REQUESTED"
            and getattr(getattr(a, "proposal", None), "param_name", None) == "mae_mult"]


def test_running_closes_email_bill_a_tested_stop_width_tighten():
    # FORM->TEST->ROUTE track 1 end-to-end through the wired ciats_sink: losers ran HOT and winners
    # COOL -> heat predicts loss -> on the drift signal plan_from_drift FORMS a mae_mult TIGHTEN, the
    # REAL shadow replay scales the losses down -> the absolute CHECK passes -> a C1 alert
    # (ApprovalRequested for mae_mult) reaches mod:Logger.alerts (the HR-RPT-001 push to Bill).
    system, _wm, logger = _assemble_real_wm()
    sink = system.ciats_sinks[PositionSide.LONG]
    for _ in range(120):
        sink(_close_record("5", heat=1))                   # winners, cool
    for _ in range(80):
        sink(_close_record("-10", heat=5))                 # losers, hot -> 200 + the CUSUM breaches
    assert _mae_alert(logger)                               # the tested tighten was brought to Bill
    assert system.conductors[PositionSide.SHORT].trade_count == 0   # the short loop is isolated


def test_running_closes_report_a_stop_width_loosen_without_alerting():
    # track 1 report track: winners ran HOT, losers COOL -> heat predicts a WIN -> a LOOSEN; the
    # replay can only scale the losses UP (it cannot credit saved winners) -> CHECK fails -> the
    # CheckResult is REPORTED into Stream-1 with NO mae_mult C1 alert (unprofitable -> reported).
    system, _wm, logger = _assemble_real_wm()
    sink = system.ciats_sinks[PositionSide.LONG]
    for _ in range(120):
        sink(_close_record("5", heat=5))                   # winners, hot
    for _ in range(80):
        sink(_close_record("-10", heat=1))                 # losers, cool
    assert any(getattr(e, "code", None) == "PDCA_CHECK_RESULT" and getattr(e, "passed", None) is False
               for e in logger.operational)                # the disproven theory is reported
    assert not _mae_alert(logger)                          # never brought to Bill


# ------------------------------------------------ TB00752 (c): the C4 MONTHLY PULL report VIEW over
# the captured record - a degrading run -> the reported theory surfaces in the view, no new capture,
# no C1 alert (the email track stays separate from the periodic pull report)

def _close_at(net, when, *, heat=None):
    """A schema-valid 25-field TRADE_CLOSE stamped with an exit instant (so the report windowing has
    a wall-clock to bucket on - in the running organism the exit_controller sets exit_timestamp_utc)."""
    n = Decimal(net)
    win = n > 0
    return TradeClose(
        symbol="BTC/USD", entry_fill_price=Decimal("60000"), exit_price=Decimal("66000"),
        exit_reason=ExitReason.HTF_REGIME_REVERSAL,
        fees_entry_usd=Decimal("0"), fees_exit_usd=Decimal("0"), fees_total_usd=Decimal("1"),
        net_pl_usd=n, net_gain_usd=(n if win else Decimal("0")),
        net_loss_usd=(Decimal("0") if win else -n), asset_regime="TRENDING_POS_NORMAL",
        exit_timestamp_utc=when, actual_rr=(Decimal("1.6") if win else Decimal("-1")),
        mae_pct_reached=(Decimal(str(heat)) if heat is not None else None),
    )


def test_monthly_report_view_surfaces_the_reported_theory_no_new_capture():
    # The capstone for the report VIEWS: a degrading run through the wired ciats_sink (winners hot,
    # losers cool -> a stop-width LOOSEN the replay cannot credit -> a CHECK-failed CheckResult
    # REPORTED into Stream-1, NO C1 alert) -> the C4 MONTHLY report VIEW, built purely from the
    # captured record, surfaces that reported theory + the realized trade performance for the LONG
    # module, with NO new capture and NO email-track alert for the reported item.
    from tothbot.recorder.reporting import ReportCategory, build_operator_report

    system, _wm, logger = _assemble_real_wm()
    sink = system.ciats_sinks[PositionSide.LONG]
    for _ in range(120):
        sink(_close_at("5", "2026-06-10T12:00:00+00:00", heat=5))   # winners, hot
    for _ in range(80):
        sink(_close_at("-10", "2026-06-12T12:00:00+00:00", heat=1))  # losers, cool -> 200 + CUSUM breach

    stores = {s.value: system.conductors[s].parameter_store for s in (PositionSide.LONG, PositionSide.SHORT)}
    report = build_operator_report(
        logger, stores, category=ReportCategory.C4_MONTHLY,
        as_of=datetime(2026, 6, 15, 18, 0, tzinfo=timezone.utc),
    )
    lm = report.per_module["long"]
    # the realized trade performance for June is the full degrading run (120*5 + 80*-10 = -200).
    assert lm.performance.trade_count == 200
    assert lm.performance.net_pl_usd == Decimal("-200")
    assert lm.performance.inference_valid is True            # the 200-trade floor is reached
    assert lm.progress_to_inference_floor == "200/200 (reached)"
    # the disproven stop-width theory is REPORTED in the view (the SAME object captured in Stream-1 -
    # a VIEW, not a re-derivation -> no new capture).
    assert lm.reported_theories
    assert all(getattr(t, "passed", None) is False for t in lm.reported_theories)
    assert all(t in logger.operational for t in lm.reported_theories)
    # the email track stays separate: NO C1 mae_mult alert was raised for the reported item.
    assert not _mae_alert(logger)
    # the short module is isolated (no closes flowed to it) - per-module, sec 7.
    assert report.per_module["short"].performance.trade_count == 0


# ------------------------------------------------ TB00753: the C4 MONTHLY PULL report RENDERED + EMITTED
# to the operator surface - the degrading run -> the PULL trigger builds + renders + emits a body
# carrying the trade performance + the reported theory + the parameter evolution, no new capture, no C1

def test_monthly_pull_trigger_renders_and_emits_the_operator_report():
    # The TB00753 capstone: the same degrading run as the VIEW capstone -> the C4 MONTHLY PULL trigger
    # (PullReportService) builds (the VIEW) + RENDERS (the operator-facing body) + EMITS (the injected
    # sink), distinct from the C1 push. The rendered body carries the realized trade performance + the
    # REPORTED disproven theory; NO new capture; NO C1 alert was raised by the pull.
    from tothbot.recorder.report_render import PullReportService, RenderedReport
    from tothbot.recorder.reporting import ReportCategory

    system, _wm, logger = _assemble_real_wm()
    sink = system.ciats_sinks[PositionSide.LONG]
    for _ in range(120):
        sink(_close_at("5", "2026-06-10T12:00:00+00:00", heat=5))   # winners, hot
    for _ in range(80):
        sink(_close_at("-10", "2026-06-12T12:00:00+00:00", heat=1))  # losers, cool -> 200 + CUSUM breach

    stores = {s.value: system.conductors[s].parameter_store for s in (PositionSide.LONG, PositionSide.SHORT)}
    captured_before = len(logger.operational)
    emitted: list = []
    service = PullReportService(logger, stores, emit=emitted.append)
    rendered = service.pull(
        ReportCategory.C4_MONTHLY, datetime(2026, 6, 15, 18, 0, tzinfo=timezone.utc))

    # the operator received exactly the rendered report on the PULL path (not the C1 SMTP seam).
    assert emitted == [rendered] and isinstance(rendered, RenderedReport)
    assert rendered.code == "C4" and "C4 MONTHLY" in rendered.subject
    # the rendered body carries the realized trade performance (the LONG module, 200 trades, net -200).
    assert "module: LONG" in rendered.body
    assert "trades: 200" in rendered.body
    assert "net P/L: -200 USD" in rendered.body
    assert "inference-valid" in rendered.body                 # the 200-trade floor is reached
    # the disproven stop-width theory is REPORTED in the rendered body (CHECK failed), not pushed.
    assert "REPORTED theories" in rendered.body
    assert "CHECK failed" in rendered.body
    # NO new capture: the pull read the same Stream-1, it did not append to it.
    assert len(logger.operational) == captured_before
    # NO C1 alert was raised by the pull (the periodic-pull track is distinct from the C1 push).
    assert not _mae_alert(logger)
    # the SHORT module section is isolated (no closes flowed to it).
    assert "module: SHORT" in rendered.body


# ------------------------------------------------ TB00754: the C4 MONTHLY pull DELIVERED end-to-end by
# the cadence scheduler over the REAL SMTP transport - a degrading run -> the deterministic clock rolls
# from June into July -> the scheduler fires the C4 MONTHLY pull -> the SMTP transport delivers the
# rendered body to the operator surface, on the DISTINCT periodic-pull track, no new capture, no C1

def test_cadence_scheduler_delivers_the_monthly_report_over_the_smtp_transport():
    # The TB00754 capstone: the same degrading June run -> a PullCadenceScheduler driven by a
    # deterministic injected clock; when the clock rolls from June into July the C4 MONTHLY bucket
    # rolls over and the scheduler fires the pull for the COMPLETED month - which builds + renders +
    # EMITS through the wired SmtpReportTransport. The real transport (its socket send injected and
    # captured) delivers the rendered body carrying the LONG module's realized trade performance + the
    # REPORTED disproven theory, on the periodic-pull track (NOT the C1 SMTP alert seam), with no new
    # capture and no C1 alert raised by the scheduled pull.
    from tothbot.recorder.report_render import PullReportService
    from tothbot.recorder.report_transport import (
        PullCadenceScheduler,
        SmtpReportTransport,
    )
    from tothbot.recorder.reporting import ReportCategory

    system, _wm, logger = _assemble_real_wm()
    sink = system.ciats_sinks[PositionSide.LONG]
    for _ in range(120):
        sink(_close_at("5", "2026-06-10T12:00:00+00:00", heat=5))   # winners, hot
    for _ in range(80):
        sink(_close_at("-10", "2026-06-12T12:00:00+00:00", heat=1))  # losers, cool -> 200 + CUSUM breach

    stores = {s.value: system.conductors[s].parameter_store for s in (PositionSide.LONG, PositionSide.SHORT)}
    captured_before = len(logger.operational)

    # wire the REAL delivery edge: the SMTP transport with its socket-level send injected (captured).
    sent: list = []
    transport = SmtpReportTransport(
        send=lambda frm, to, msg: sent.append((frm, to, msg)),
        sender="tothbot@toth.bot", recipients=["wstothjr@gmail.com"])
    service = PullReportService(logger, stores, emit=transport)
    scheduler = PullCadenceScheduler(service, [ReportCategory.C4_MONTHLY])

    # the deterministic clock: a June tick (baseline, no fire) then a July tick (the month rolls over).
    assert scheduler.tick(datetime(2026, 6, 30, 23, 55, tzinfo=timezone.utc)) == []
    assert sent == []
    fired = scheduler.tick(datetime(2026, 7, 1, 0, 5, tzinfo=timezone.utc))

    # the scheduler fired exactly the C4 MONTHLY pull for the completed month.
    assert [c for c, _ in fired] == [ReportCategory.C4_MONTHLY]
    # the REAL transport delivered exactly one message to the operator surface.
    assert len(sent) == 1
    frm, to, msg = sent[0]
    assert frm == "tothbot@toth.bot" and to == ("wstothjr@gmail.com",)
    # the delivered body carries the LONG module's realized trade performance (200 trades, net -200).
    assert "Subject: TothBot C4 MONTHLY" in msg
    assert "module: LONG" in msg and "trades: 200" in msg and "net P/L: -200 USD" in msg
    assert "inference-valid" in msg                            # the 200-trade floor is reached
    # the REPORTED disproven stop-width theory rides the pull report, not a C1 push.
    assert "REPORTED theories" in msg and "CHECK failed" in msg
    # the periodic-pull track marker - DISTINCT from the C1 immediate push.
    assert "X-TothBot-Track: periodic-pull" in msg
    # NO new capture: the scheduled pull read the same Stream-1, it did not append to it.
    assert len(logger.operational) == captured_before
    # NO C1 alert was raised by the scheduled pull (the pull track never touches logger.alert).
    assert not _mae_alert(logger)


# ------------------------------------------------ TB00755: the cadence scheduler + SMTP transport BOUND
# INTO THE LIVE ORGANISM CLOCK - assemble_operational wires the scheduler off the OHLC_5m system clock;
# a run of 5m closes crossing a calendar month boundary FIRES the C4 MONTHLY pull over the real SMTP
# transport, no manual tick, no new capture, no C1

class _CaptureSmtp:
    """A smtplib.SMTP stand-in for the live-organism proof: captures the (from, to, message) the
    transport delivers, no socket. Shared sink list so the test reads what was sent."""

    def __init__(self, sink):
        self._sink = sink

    def starttls(self):  # pragma: no cover - not configured in this proof
        pass

    def login(self, user, password):  # pragma: no cover - not configured in this proof
        pass

    def sendmail(self, from_addr, to_addrs, message):
        self._sink.append((from_addr, to_addrs, message))

    def quit(self):
        pass


def test_live_clock_fires_the_monthly_pull_over_the_smtp_transport():
    # The TB00755 capstone: assemble_operational WIRES the PullCadenceScheduler off the OHLC_5m system
    # clock + a real SmtpReportTransport (its socket the injected smtplib edge). A degrading June run
    # builds the corpus + the reported theory; then 5m closes whose UTC instants cross the June->July
    # month boundary drive the driver's clock -> the scheduler FIRES the C4 MONTHLY pull for June ->
    # the SMTP transport delivers the rendered body, with NO manual tick, NO new capture, NO C1 alert.
    from tothbot.exchange.candle_close import CandleCloseDetector, committed_candle_from_frame
    from tothbot.recorder.report_transport import SmtpReportTransport, smtplib_send
    from tothbot.recorder.reporting import ReportCategory

    sent: list = []
    transport = SmtpReportTransport(
        send=smtplib_send("mail", smtp_factory=lambda host, port: _CaptureSmtp(sent)),
        sender="tothbot@toth.bot", recipients=["wstothjr@gmail.com"])
    system, _wm, logger = _assemble_real_wm(
        report_emit=transport, report_categories=[ReportCategory.C4_MONTHLY])

    # the assembly built the cadence scheduler AND wired its tick into the OHLC_5m system clock.
    assert system.pull_scheduler is not None
    assert system.driver._on_clock_tick.__self__ is system.pull_scheduler

    # a degrading June run (winners hot, losers cool -> a stop-width LOOSEN -> a CHECK-failed theory
    # REPORTED into Stream-1, no C1) builds the LONG corpus the C4 MONTHLY report reads.
    sink = system.ciats_sinks[PositionSide.LONG]
    for _ in range(120):
        sink(_close_at("5", "2026-06-10T12:00:00+00:00", heat=5))
    for _ in range(80):
        sink(_close_at("-10", "2026-06-12T12:00:00+00:00", heat=1))
    captured_before = len(logger.operational)

    # Reseed the 5m detector to a clean June-2026 boundary: a real organism warmed in June 2026 has a
    # contemporaneous clock, but the _FakeRest warm-up uses a 2023 epoch base - so set the live clock
    # origin to the test timeline (the cadence reads the candle interval_begin as a UTC instant).
    JUN1 = int(datetime(2026, 6, 1, tzinfo=timezone.utc).timestamp())
    DAY = 86400

    def _candle(begin):
        return {"symbol": "BTC/USD", "interval_begin": begin, "open": "100", "high": "101",
                "low": "99", "close": "100", "volume": "1000"}

    system.driver._det5["BTC/USD"] = CandleCloseDetector(
        "BTC/USD", last_interval_begin=JUN1, last_complete_candle=committed_candle_from_frame(_candle(JUN1)))
    system.driver._stepped5["BTC/USD"] = JUN1

    # Drive the OHLC_5m system clock. The detector fires the PRIOR candle on each roll, so the fired-
    # close UTC instants run: June 1 (baseline), June 20 (same month), July 2 (rolls -> fire June).
    asyncio.run(system.driver.on_ohlc_5m({"data": [_candle(JUN1 + 19 * DAY)]}))   # fires June 1
    assert sent == []                                                              # baseline, no fire
    asyncio.run(system.driver.on_ohlc_5m({"data": [_candle(JUN1 + 31 * DAY)]}))   # fires June 20
    assert sent == []                                                              # same month, no fire
    asyncio.run(system.driver.on_ohlc_5m({"data": [_candle(JUN1 + 40 * DAY)]}))   # fires July 2 -> roll

    # the month boundary fired exactly the C4 MONTHLY pull for June, delivered over the SMTP transport.
    assert len(sent) == 1
    frm, to, msg = sent[0]
    assert frm == "tothbot@toth.bot" and to == ["wstothjr@gmail.com"]
    assert "Subject: TothBot C4 MONTHLY" in msg
    # the delivered body carries the LONG module's realized June performance + the reported theory.
    assert "module: LONG" in msg and "trades: 200" in msg and "net P/L: -200 USD" in msg
    assert "REPORTED theories" in msg and "CHECK failed" in msg
    # the periodic-pull track marker, DISTINCT from the C1 immediate push.
    assert "X-TothBot-Track: periodic-pull" in msg
    # NO new pipeline capture by the clock-driven pull, and NO C1 alert raised. (The deliberately
    # day-spaced clock stream makes the TB00768 derived-1H aggregator flag each sparse hour as a
    # feed gap, which the TB00769 self-heal then refetches/re-seeds from REST; both HTF_1H_GAP and
    # its HTF_1H_HEAL are WS observability orthogonal to the monthly-pull capture under test.)
    new_records = [r for r in logger.operational[captured_before:]
                   if getattr(r, "code", None) not in ("HTF_1H_GAP", "HTF_1H_HEAL")]
    assert new_records == []
    assert not _mae_alert(logger)


# ------------------------------------------------ TB00756: the rule:HR-LG-013 DURABLE trade-record FILE
# sink wired into the live organism - a run of closes is BOTH learned in-memory AND durably appended to
# trades_<YYYY>.jsonl; a cold-start load reconstructs the corpus + the C5 ANNUAL report reads the file as
# its authoritative source, no C1 write-failure

def test_running_closes_persist_to_the_durable_file_and_c5_reads_it_back():
    # The TB00756 capstone: assemble_operational wires the PermanentTradeRecordSink (rule:HR-LG-013) as
    # the per-module learning-sink downstream over a REAL temp records dir. 200 closes flow through the
    # LONG ciats_sink -> learned in-memory AND durably appended to trades_2026.jsonl (fsync-per-write).
    # Then a FRESH load off disk reconstructs the 200-trade corpus (cold-start restore) and the C5
    # ANNUAL report is built from the durable file alone - independent of the live in-memory state.
    import tempfile

    from tothbot.recorder.trade_record_file import (
        build_c5_from_durable_file,
        load_trade_records_dir,
    )

    with tempfile.TemporaryDirectory() as records_dir:
        system, _wm, logger = _assemble_real_wm(records_dir=records_dir)
        assert system.trade_record_sink is not None        # the durable sink was wired by the assembly

        sink = system.ciats_sinks[PositionSide.LONG]
        for _ in range(150):
            sink(_close_at("5", "2026-06-10T12:00:00+00:00"))    # winners
        for _ in range(50):
            sink(_close_at("-10", "2026-06-12T12:00:00+00:00"))  # losers -> 200 total, net 750-500=250

        # the durable file exists with one NDJSON line per closed trade (200), each parseable.
        path = os.path.join(records_dir, "trades_2026.jsonl")
        assert os.path.exists(path)
        with open(path, encoding="utf-8") as fh:
            lines = [ln for ln in fh.read().splitlines() if ln.strip()]
        assert len(lines) == 200

        # a FRESH cold-start load off disk reconstructs the full 200-trade corpus.
        restored = load_trade_records_dir(records_dir, [2026])
        assert len(restored) == 200
        assert sum(r.net_pl_usd for r in restored) == Decimal("250")   # 150*5 + 50*-10

        # the C5 ANNUAL report is built from the durable file ALONE (the authoritative source).
        report = build_c5_from_durable_file(records_dir, 2026)
        assert report.category.code == "C5"
        assert report.combined.trade_count == 200                      # all 200 in the calendar year
        assert report.combined.net_pl_usd == Decimal("250")
        assert report.combined.inference_valid is True                 # the 200-trade floor, restored
        # the C5 Form 8949 tax lots are present over the durable (combined, sideless) corpus.
        assert len(report.per_module["all"].tax_lots) == 200

        # NO durable-write failure was raised (no C1 TRADE_RECORD_WRITE_FAILED alert).
        assert not any(getattr(a, "code", None) == "TRADE_RECORD_WRITE_FAILED" for a in logger.alerts)


# ------------------------------------------------ TB00757: the max-over-life MAE (MTM) tracker on the
# live organism - a position runs DEEP against itself (a deep non-triggering ticker marks the high) then
# exits BENIGN (an HTF regime reversal in profit); the TRADE_CLOSE carries the DEEP max-over-life heat,
# not the shallow at-exit reading

def test_running_position_reports_max_over_life_mae_not_the_benign_at_exit():
    system, wm, logger = _assemble_real_wm()
    _open_real_long(wm)                                        # entry 60000, atr 2000 -> L2 thr 3000

    # a DEEP but sub-threshold adverse ticker (bid 57100 -> adverse 2900 < 3000): no exit fires, but
    # the MTM tracker marks the heat (2900/60000). emergSL 54000 is far, so no L3 either.
    wm.handle_ticker({"channel": "ticker", "type": "update",
                      "data": [{"symbol": "BTC/USD", "bid": "57100", "ask": "57200"}]})
    assert wm.has_position("BTC/USD")                          # still open (no exit fired)
    assert wm.mae_pct_high_for("BTC/USD") == Decimal("2900") / Decimal("60000")

    # then a BENIGN exit: an HTF 1H reversal (EMA20 < EMA50 for a long) at a PROFITABLE bid 60500 ->
    # the run-to-reversal take-profit closes in profit (at-exit adverse excursion = 0).
    wm.on_htf_ohlc_close("BTC/USD", "10", "20", bid="60500", ask="60600")
    assert not wm.has_position("BTC/USD")                      # the regime exit closed it

    # the emitted TRADE_CLOSE (the LONG Stream-2 corpus) carries the MAX-OVER-LIFE heat (the deep
    # 2900/60000), NOT the benign at-exit 0 - the sharper signal the CIATS stop-width theory reads.
    rec = logger.corpus_for("long")[-1]
    assert rec.exit_reason is ExitReason.HTF_REGIME_REVERSAL
    assert rec.net_pl_usd > 0                                  # the exit was in profit (benign)
    assert rec.mae_pct_reached == Decimal("2900") / Decimal("60000")
    assert rec.mae_pct_reached > Decimal("0")                  # the at-exit reading would have been 0
    assert wm.mae_pct_high_for("BTC/USD") is None              # the tracker was cleared at close


# ------------------------------------------------ TB00791 (F1): the validated long-only 24h-DECISION
# strategy driven FULL LIFECYCLE over the ASSEMBLED REAL WSManager (decision_entry=True) - a crafted
# 24h EMA12/26 BULLISH CROSS fires a REAL paper LONG (daily-ATR stamped), which then EXITS two ways
# (the 24h bearish-cross reversal OR the wide 2.5x-daily-ATR L2 stop); the emitted TRADE_CLOSE carries
# the ONE-basis actual_rr + the field-19 24h signal_params + the right exit_reason, learned by the LONG
# conductor. The wm comes from _assemble_real_wm (modules + seam + exit controllers + per-side
# conductors wired, set_decision_stop_mult(2.5) applied by the assembly); the cross is driven through a
# clean-providers LiveSweepDriver (bbo ~60k matching the entry scale - the assembled organism's caches
# are crafted for the regime classification, this driver owns the decision entry/exit seam under test).
DAY_F1 = 86400


class _NoSleepF1:
    async def __call__(self, _seconds):
        return None


def _warm_f1(symbol="BTC/USD"):
    return asyncio.run(WarmupOrchestrator(_FakeRest(), sleep=_NoSleepF1()).warm_pair(symbol))


def _regime_cache_f1(regime=Regime.TRENDING_POS_NORMAL):
    classification = SimpleNamespace(regime=regime, ema20=Decimal("105"), ema50=Decimal("100"))
    return SimpleNamespace(get=lambda s: classification)


def _providers_f1():
    """Clean providers for the 24h-decision driver: a stable ~60k bbo (the long entry buys at the ask,
    the reversal sells at the bid) + a high expected_reward so the daily-ATR-override candidate robustly
    clears the sacred 1:1.5 floor (the WIDE 2.5x net_loss is small vs this reward)."""
    return LiveProviders(
        instrument=lambda s: ("online", True, "600000"),
        bbo=lambda s: (Decimal("59990"), Decimal("60000")),
        expected_reward=lambda s, r: Decimal("0.3"),
        mpp_abs_cap_pct=lambda s, side: Decimal("0.01"),
        base_per_trade_size=lambda s, side, ref: Decimal("50"),
        ws_state=lambda s: "Subscribed",
        new_cl_ord_id=lambda: "cl-f1",
        new_deadline=lambda: "2026-06-15T07:30:00Z",
    )


def _declining_bars_f1(n=30, symbol="BTC/USD", start=60030):
    """n daily bars (newest-last) gently FALLING so EMA(12) seeds BELOW EMA(26) - a BEARISH cache, the
    precondition for a bullish-cross entry on the advance; tight high/low -> a small seed ATR."""
    out = []
    for i in range(n):
        close = Decimal(start - i)
        out.append(CommittedCandle(symbol=symbol, interval_begin=i * DAY_F1, open=close + 1,
                                   high=close + 1, low=close - 1, close=close, volume=Decimal(1)))
    return out


def _h1_f1(symbol, begin, close):
    """A folded 1H CommittedCandle - the input the second fold stage (fold_hour) folds into the 24h bar."""
    c = Decimal(close)
    return CommittedCandle(symbol=symbol, interval_begin=begin, open=c - 1, high=c + 2, low=c - 2,
                           close=c, volume=Decimal(5))


def _f1_driver(wm, logger):
    """A decision_entry=True LiveSweepDriver over a fresh BEARISH-seeded DailyDecisionStore, pointed at
    the assembled REAL wm. Returns (driver, store)."""
    store = DailyDecisionStore()
    store.seed_from_bars("BTC/USD", _declining_bars_f1())
    driver = LiveSweepDriver(
        warmups={"BTC/USD": _warm_f1()}, regime_cache=_regime_cache_f1(), providers=_providers_f1(),
        wm=wm, logger=logger, decision_store=store, decision_entry=True,
    )
    return driver, store


def _drive_bullish_cross(driver, *, day_index=200):
    """Drive twenty-four contiguous RISING 1H closes (69000 -> ~70000): the second fold stage emits one
    Closed24H whose strong bullish body flips the bearish seed -> the EMA12/26 bullish cross fires the
    LONG entry. Returns the day epoch used."""
    day = day_index * DAY_F1
    for h in range(24):
        asyncio.run(driver._advance_decision(_h1_f1("BTC/USD", day + h * 3600, 69000 + h * 43)))
    return day


def test_f1_24h_bullish_cross_opens_a_real_long_stamped_with_the_daily_atr():
    # (B) the entry leg: the 24h EMA12/26 BULLISH CROSS opens a REAL paper LONG on the assembled wm,
    # stamped with the DAILY decision-bar ATR (the ONE 1R basis), the L3 emergSL at 3x that daily ATR,
    # and the field-19 24h signal_params (the DECISION_24H_BULLISH_CROSS trigger the close emits).
    system, wm, logger = _assemble_real_wm()
    driver, store = _f1_driver(wm, logger)
    assert store.get("BTC/USD").bullish is False           # bearish seed (the cross precondition)
    assert not wm.has_position("BTC/USD")
    _drive_bullish_cross(driver)
    assert store.get("BTC/USD").bullish is True            # the cross fired (post-advance bullish)
    assert wm.has_position("BTC/USD")                      # a REAL paper LONG opened on the cross
    pos = wm.position("BTC/USD")
    assert pos.side is PositionSide.LONG                   # long-only (the short module is dormant)
    daily_atr = store.get("BTC/USD").atr_14_24h
    assert pos.atr_14_entry == daily_atr                  # stamped with the DAILY ATR (not the 5m ATR)
    # the L3 emergSL sits at 3x the SAME daily ATR below entry (outermost, DEC-124 no-TP).
    assert pos.emergsl_price == pos.avg_entry_price - Decimal("3") * daily_atr
    assert pos.signal_params["trigger"] == "DECISION_24H_BULLISH_CROSS"
    assert pos.signal_params["side"] == "long"


def test_f1_lifecycle_entry_then_wide_2_5x_atr_l2_stop_close():
    # (C) exit leg (b): the WIDE 2.5x-daily-ATR L2 stop. After the cross entry, an adverse ticker at
    # 2.6x the daily ATR (past the 2.5x L2 threshold but INSIDE the 3x L3 emergSL) fires the L2 MAE
    # breach; the emitted TRADE_CLOSE carries exit_reason MAE_THRESHOLD_BREACH, the ONE-basis actual_rr
    # (net_PL / [decision_atr_stop_mult x daily-ATR risk]), and the field-19 24h signal_params - learned
    # by the LONG conductor (the short loop is isolated).
    system, wm, logger = _assemble_real_wm()
    driver, store = _f1_driver(wm, logger)
    assert system.conductors[PositionSide.LONG].trade_count == 0
    _drive_bullish_cross(driver)
    pos = wm.position("BTC/USD")
    entry, atr = pos.avg_entry_price, Decimal(pos.atr_14_entry)
    adverse_bid = entry - Decimal("2.6") * atr               # past 2.5x L2, inside 3x L3 -> L2 fires
    assert adverse_bid > pos.emergsl_price                   # the L2 stop fires FIRST, not the L3 backstop
    wm.handle_ticker({"channel": "ticker", "type": "update",
                      "data": [{"symbol": "BTC/USD", "bid": str(adverse_bid), "ask": str(adverse_bid + 10)}]})
    assert not wm.has_position("BTC/USD")                    # the wide L2 stop closed it
    rec = logger.corpus_for("long")[-1]
    assert rec.exit_reason is ExitReason.MAE_THRESHOLD_BREACH
    assert rec.actual_rr is not None and rec.actual_rr < 0   # a stopped loss on the ONE 2.5x basis
    assert rec.signal_params["trigger"] == "DECISION_24H_BULLISH_CROSS"   # field-19 24h entry levels
    assert system.conductors[PositionSide.LONG].trade_count == 1          # the LONG conductor learned it
    assert system.conductors[PositionSide.SHORT].trade_count == 0         # the short loop is isolated


def test_f1_lifecycle_entry_then_24h_bearish_cross_reversal_close():
    # (C) exit leg (a): the 24h bearish-cross REVERSAL (layer:L1a). After the cross entry, a falling day
    # (twenty-four 1H closes crashing the 24h bar) flips the cache bearish -> the Closed24H branch drives
    # wm.on_htf_ohlc_close with the 24h EMAs -> the EC-L1A-001 reversal closes the open LONG; the emitted
    # TRADE_CLOSE carries exit_reason HTF_REGIME_REVERSAL, the ONE-basis actual_rr, and the field-19 24h
    # signal_params, learned by the LONG conductor.
    system, wm, logger = _assemble_real_wm()
    driver, store = _f1_driver(wm, logger)
    _drive_bullish_cross(driver)
    assert wm.has_position("BTC/USD")
    # a falling day: twenty-four contiguous 1H closes far below the cross -> the 24h EMA fast dives under
    # the slow (the bearish cross), so _drive_decision_reversal fires the L1a reversal on the Closed24H.
    day2 = 201 * DAY_F1
    for h in range(24):
        asyncio.run(driver._advance_decision(_h1_f1("BTC/USD", day2 + h * 3600, 1000)))
    assert store.get("BTC/USD").bullish is False            # the bearish cross fired
    assert not wm.has_position("BTC/USD")                    # the 24h reversal closed the LONG
    rec = logger.corpus_for("long")[-1]
    assert rec.exit_reason is ExitReason.HTF_REGIME_REVERSAL
    assert rec.actual_rr is not None                         # the close re-anchored onto the ONE basis
    assert rec.signal_params["trigger"] == "DECISION_24H_BULLISH_CROSS"
    assert system.conductors[PositionSide.LONG].trade_count == 1
    assert system.conductors[PositionSide.SHORT].trade_count == 0
