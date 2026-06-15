"""contract:Operator_Reporting_Hierarchy C2-C6 PULL report VIEWS - unit coverage.

Exercises the report-view layer (tothbot/recorder/reporting.py) over a synthetic captured record:
the PULL windowing (day / week / month / year / rolling-12mo), the trade-performance metrics (net
P/L, win rate, R:R distribution, Half-Kelly, per-regime, the FP5 floor label), the Stream-1 cursor
that attributes timestamp-less CIATS events to (module, time), and the assembled per-module +
operator report (reported theories, deferred candidates, proposed changes, parameter evolution,
current CIATS values, the C5 tax projection, the floor progress, the combined roll-up). A pure VIEW
over the already-captured record - no new capture.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from tothbot.ciats.conductor import DeferredCandidate, DriftSignal
from tothbot.ciats.parameter_store import ParameterStore
from tothbot.ciats.pdca_engine import CheckResult
from tothbot.execution.exit_controller import ExitReason, TradeClose
from tothbot.recorder.logger import Logger
from tothbot.recorder.reporting import (
    INFERENCE_FLOOR,
    ReportCategory,
    Window,
    attribute_stream1,
    build_operator_report,
    record_time,
    report_window,
    trade_performance,
)


def _tc(*, net, when, regime="TRENDING_POS_NORMAL", rr="1.6", fees="2", symbol="BTC/USD"):
    """A schema-valid 23-field TRADE_CLOSE for the corpus, stamped with an exit instant."""
    n = Decimal(net)
    win = n > 0
    return TradeClose(
        symbol=symbol, entry_fill_price=Decimal("60000"), exit_price=Decimal("66000"),
        exit_reason=ExitReason.HTF_REGIME_REVERSAL,
        fees_entry_usd=Decimal("0"), fees_exit_usd=Decimal("0"), fees_total_usd=Decimal(fees),
        net_pl_usd=n, net_gain_usd=(n if win else Decimal("0")),
        net_loss_usd=(Decimal("0") if win else -n), asset_regime=regime,
        exit_timestamp_utc=when, actual_rr=(Decimal(rr) if rr is not None else None),
    )


def _check(passed):
    return CheckResult(
        passed=passed, mw_z=Decimal("1"), mw_crit=Decimal("2.326348"),
        sharpe_candidate=Decimal("0"), sharpe_baseline=Decimal("0"),
        sharpe_improved=False, spearman=None,
    )


JUN = "2026-06-15T12:00:00+00:00"   # in the June 2026 month / week of Jun 15 (Mon) / 2026 year
MAY = "2026-05-10T12:00:00+00:00"   # prior month, same year
LAST_YEAR = "2025-06-15T12:00:00+00:00"
AS_OF = datetime(2026, 6, 15, 18, 0, tzinfo=timezone.utc)


# ----------------------------------------------------------------------- the PULL window (cadence)
def test_report_window_per_category():
    assert report_window(ReportCategory.C2_DAILY, AS_OF) == Window(
        datetime(2026, 6, 15, tzinfo=timezone.utc), datetime(2026, 6, 16, tzinfo=timezone.utc))
    # Jun 15 2026 is a Monday -> the week is [Jun 15, Jun 22).
    assert report_window(ReportCategory.C3_WEEKLY, AS_OF) == Window(
        datetime(2026, 6, 15, tzinfo=timezone.utc), datetime(2026, 6, 22, tzinfo=timezone.utc))
    assert report_window(ReportCategory.C4_MONTHLY, AS_OF) == Window(
        datetime(2026, 6, 1, tzinfo=timezone.utc), datetime(2026, 7, 1, tzinfo=timezone.utc))
    assert report_window(ReportCategory.C5_ANNUAL, AS_OF) == Window(
        datetime(2026, 1, 1, tzinfo=timezone.utc), datetime(2027, 1, 1, tzinfo=timezone.utc))
    # C6 rolling-12mo ends AT the pull instant, starts 12 calendar months earlier.
    c6 = report_window(ReportCategory.C6_ROLLING_12MO, AS_OF)
    assert c6.start == datetime(2025, 6, 15, 18, 0, tzinfo=timezone.utc)
    assert c6.end == AS_OF


def test_record_time_parses_exit_stamp_and_z_suffix():
    assert record_time(_tc(net="1", when=JUN)) == datetime(2026, 6, 15, 12, 0, tzinfo=timezone.utc)
    assert record_time(_tc(net="1", when="2026-06-15T12:00:00Z")) == datetime(
        2026, 6, 15, 12, 0, tzinfo=timezone.utc)
    assert record_time(SimpleNamespace(ts=None)) is None


# ----------------------------------------------------------------------- trade performance metrics
def test_trade_performance_core_metrics():
    recs = [_tc(net="10", when=JUN, rr="2.0"), _tc(net="10", when=JUN, rr="1.8"),
            _tc(net="-5", when=JUN, rr="-0.5")]
    p = trade_performance(recs)
    assert p.trade_count == 3 and p.wins == 2 and p.losses == 1
    assert p.net_pl_usd == Decimal("15")
    assert p.win_rate == Decimal(2) / Decimal(3)
    assert p.rr_min == Decimal("-0.5") and p.rr_max == Decimal("2.0") and p.rr_median == Decimal("1.8")
    assert p.best_trade_usd == Decimal("10") and p.worst_trade_usd == Decimal("-5")
    # fees=2 each -> 6 total; gross = net(15) + fees(6) = 21.
    assert p.fees_total_usd == Decimal("6")
    assert p.fees_pct_of_gross == Decimal("6") / Decimal("21")
    # below 200 trades -> monitoring-only label (FP5).
    assert p.inference_valid is False
    assert "insufficient-data 3-of-200" in p.floor_label


def test_trade_performance_half_kelly_and_per_regime():
    # W=0.6, R = avg_gain/avg_loss = 2/1 = 2 -> K_full = 0.6 - 0.4/2 = 0.4, K_half = 0.2.
    recs = [_tc(net="2", when=JUN, regime="TRENDING_POS_NORMAL") for _ in range(6)]
    recs += [_tc(net="-1", when=JUN, regime="RANGE_NORMAL") for _ in range(4)]
    p = trade_performance(recs)
    assert p.kelly_full == Decimal("0.4") and p.kelly_half == Decimal("0.2")
    regimes = {r.regime: r for r in p.per_regime}
    assert regimes["TRENDING_POS_NORMAL"].bucket_count == 6
    assert regimes["RANGE_NORMAL"].bucket_count == 4
    assert regimes["TRENDING_POS_NORMAL"].win_rate == Decimal("1")


def test_trade_performance_inference_valid_at_floor():
    recs = [_tc(net="1", when=JUN) for _ in range(INFERENCE_FLOOR)]
    p = trade_performance(recs)
    assert p.inference_valid is True and p.floor_label == "inference-valid"


# ----------------------------------------------------------------------- the Stream-1 cursor view
def test_attribute_stream1_carries_time_and_side():
    long_tc = _tc(net="-5", when=JUN)
    short_tc = _tc(net="-5", when=JUN)
    corpus = {"long": [long_tc], "short": [short_tc]}
    check = _check(False)
    drift = DriftSignal("cusum_lower", "x")
    # ordering: long close -> its check; short close -> its drift (the real on_close emission order).
    operational = [long_tc, check, short_tc, drift]
    attributed = attribute_stream1(operational, corpus)
    assert len(attributed) == 2
    assert attributed[0].event is check and attributed[0].module == "long"
    assert attributed[0].time == datetime(2026, 6, 15, 12, 0, tzinfo=timezone.utc)
    assert attributed[1].event is drift and attributed[1].module == "short"


# ----------------------------------------------------------------------- the assembled report views
def _logger_with(long_records, short_records=()):
    """A real mod:Logger membrane loaded by routing each record through record() (the real capture):
    a TRADE_CLOSE -> Stream-1 + the module corpus; a CIATS event -> Stream-1 only (module default)."""
    logger = Logger()
    for rec in long_records:
        logger.record(rec, module="long")
    for rec in short_records:
        logger.record(rec, module="short")
    return logger


def test_build_operator_report_windows_trades_and_theories():
    long_jun = _tc(net="-10", when=JUN)
    long_may = _tc(net="100", when=MAY)        # out of the June month window
    check = _check(False)                        # a disproven theory emitted during long_jun's close
    deferred = DeferredCandidate(candidate=SimpleNamespace(level_key="ema_9"), reason="ema")
    # the real emission order: the close, then its CIATS events.
    logger = Logger()
    logger.record(long_may, module="long")
    logger.record(long_jun, module="long")
    logger.record(check)
    logger.record(deferred)

    rep = build_operator_report(
        logger, {"long": ParameterStore()}, category=ReportCategory.C4_MONTHLY, as_of=AS_OF)
    lm = rep.per_module["long"]
    # only the June trade is in the monthly window.
    assert lm.performance.trade_count == 1 and lm.performance.net_pl_usd == Decimal("-10")
    assert lm.reported_theories == (check,)
    assert lm.deferred_candidates == (deferred,)
    # cumulative is all-time (both trades), the window count is one.
    assert lm.cumulative_trade_count == 2
    assert lm.progress_to_inference_floor == "2/200"
    # the combined roll-up sees the one in-window trade.
    assert rep.combined.trade_count == 1


def test_build_operator_report_parameter_evolution_in_window():
    t1, t2 = _tc(net="1", when=JUN), _tc(net="2", when=JUN)
    logger = _logger_with([t1, t2])
    store = ParameterStore(initial={"mae_mult": Decimal("1.5")})
    # apply a change at the 2nd closed trade (at_trade_count=2 -> mapped to t2's exit instant, in window)
    store.apply(SimpleNamespace(proposal=SimpleNamespace(
        param_name="mae_mult", proposed_value=Decimal("1.35"))), at_trade_count=2)
    rep = build_operator_report(
        logger, {"long": store}, category=ReportCategory.C4_MONTHLY, as_of=AS_OF)
    evo = rep.per_module["long"].parameter_evolution
    assert len(evo) == 1
    assert evo[0].param_name == "mae_mult"
    assert evo[0].old_value == Decimal("1.5") and evo[0].new_value == Decimal("1.35")
    assert evo[0].time == datetime(2026, 6, 15, 12, 0, tzinfo=timezone.utc)
    # the current CIATS values reflect the written value (store-owned overrides the seed).
    assert rep.per_module["long"].current_ciats_values["mae_mult"] == Decimal("1.35")


def test_build_operator_report_current_values_fall_back_to_seed():
    logger = _logger_with([_tc(net="1", when=JUN)])
    rep = build_operator_report(
        logger, {"long": ParameterStore()}, category=ReportCategory.C4_MONTHLY, as_of=AS_OF)
    vals = rep.per_module["long"].current_ciats_values
    # unwritten -> the registry seed (mae_mult seed = 1.5, the nudge seed = 0.10).
    assert vals["mae_mult"] == Decimal("1.5") or vals["mae_mult"] == 1.5
    assert "mae_mult_nudge_pct" in vals


def test_c5_annual_emits_the_tax_projection():
    recs = [_tc(net="10", when=JUN), _tc(net="-4", when="2026-02-01T00:00:00+00:00"),
            _tc(net="99", when=LAST_YEAR)]   # last year -> excluded from the 2026 annual window
    logger = _logger_with(recs)
    rep = build_operator_report(
        logger, {"long": ParameterStore()}, category=ReportCategory.C5_ANNUAL, as_of=AS_OF)
    lots = rep.per_module["long"].tax_lots
    assert len(lots) == 2                                   # the two 2026 trades, not 2025
    assert {l.gain_loss_usd for l in lots} == {Decimal("10"), Decimal("-4")}
    assert all(l.disposed_utc is not None for l in lots)
    # the annual realized P&L is the in-window sum.
    assert rep.per_module["long"].performance.net_pl_usd == Decimal("6")


def test_c6_rolling_includes_trailing_year():
    # a trade 11 months ago IS in the rolling-12mo window; one 13 months ago is NOT.
    recent = _tc(net="5", when="2025-07-15T12:00:00+00:00")     # ~11 months before AS_OF
    old = _tc(net="5", when="2025-05-15T12:00:00+00:00")        # ~13 months before AS_OF
    logger = _logger_with([recent, old])
    rep = build_operator_report(
        logger, {"long": ParameterStore()}, category=ReportCategory.C6_ROLLING_12MO, as_of=AS_OF)
    assert rep.per_module["long"].performance.trade_count == 1


def test_report_is_per_module_isolated():
    logger = _logger_with([_tc(net="10", when=JUN)], [_tc(net="-3", when=JUN)])
    rep = build_operator_report(
        logger, {"long": ParameterStore(), "short": ParameterStore()},
        category=ReportCategory.C4_MONTHLY, as_of=AS_OF)
    assert rep.per_module["long"].performance.net_pl_usd == Decimal("10")
    assert rep.per_module["short"].performance.net_pl_usd == Decimal("-3")
    assert rep.combined.trade_count == 2 and rep.combined.net_pl_usd == Decimal("7")
