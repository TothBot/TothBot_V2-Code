"""Per-5m universe-sweep tests (pipeline/sweep.py; the live driver of process_candidate).

Covers permitted-side selection (gate:G3 mirror), the ar:AR-053 capital-commitment helpers
(candidate vs total committed, long USD-debit vs short leverage-bounded collateral, per-module
isolation), the LiveIndicators-backed sss_evaluator (ar:AR-075 - verdict from the running values,
the empty series ignored), assemble_candidate field sourcing, and sweep_pair end to end: an
all-pass candidate dispatches the entry into THIS side's wallet, an SSS reject stops at SSS, a
NON_DIR_NORMAL pair sweeps BOTH sides, a side with no module is skipped, and a no-regime (WARM_UP)
pair is skipped. Driven with asyncio.run over fakes - no network.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal
from types import SimpleNamespace

import pytest

from tothbot.config.settings import Mode
from tothbot.exchange.position_mirror import PositionSide
from tothbot.exchange.seam import OutboundOp
from tothbot.exchange.warmup import HtfCache
from tothbot.exchange.ws_manager import WSManager
from tothbot.pipeline.sweep import (
    LiveProviders,
    assemble_candidate,
    candidate_committed_usd,
    live_sss_evaluator,
    permitted_sides,
    sweep_pair,
    total_committed_usd,
)
from tothbot.regime.sss import SignalSide, sss_verdict_from_indicators
from tothbot.regime.taxonomy import Regime
from tothbot.exchange.candle_close import CommittedCandle


# --------------------------------------------------------------------------- fakes
class _FakeIndicators:
    """Stands in for LiveIndicators: a fixed atr_14 + a controllable SSS verdict per side."""

    def __init__(self, *, atr=Decimal("1000"), passing=True) -> None:
        self.atr_14 = atr
        self._passing = passing

    def sss_verdict(self, side: SignalSide):
        if self._passing:
            # long zone 30-50 + EMA9>EMA21 + volume spike (mirror for short).
            if side is SignalSide.LONG:
                rsi, e9, e21 = 40, 110, 100
            else:
                rsi, e9, e21 = 60, 100, 110
            vol, vma = 2000, 1000
        else:
            rsi, e9, e21, vol, vma = 55, 99, 100, 1000, 1000  # fails SC-SSS-1/2/3
        return sss_verdict_from_indicators(
            "BTC/USD", side=side, rsi=rsi, ema9=e9, ema21=e21, volume=vol, volume_ma20=vma
        )


class _Pos:
    def __init__(self, side, qty, price, *, symbol="X/USD", entry_timestamp_utc=None) -> None:
        self.side, self.qty, self.avg_entry_price = side, Decimal(qty), Decimal(price)
        self.symbol = symbol
        self.entry_timestamp_utc = entry_timestamp_utc


class _FakeWM:
    def __init__(self, *, positions=None, wallets=None, baselines=None) -> None:
        self._positions = positions or {}
        self._wallets = wallets or {PositionSide.LONG: Decimal("5000"), PositionSide.SHORT: Decimal("5000")}
        # The per-side drawdown baseline read through wm.portfolio_baseline(side) (mode-aware accessor);
        # defaults to each wired side's $5,000 starting wallet. None -> the default; {} -> no baseline
        # captured yet (the live pre-REST-BAL-004 startup case), distinct from the default.
        self._baselines = (
            {side: Decimal("5000") for side in self._wallets} if baselines is None else baselines
        )
        self.dispatched: list[dict] = []

    def open_positions(self):
        return self._positions

    def position(self, symbol):
        return self._positions.get(symbol)

    def exit_cooldown_at(self, symbol, side):
        return None

    def consecutive_loss_count(self, symbol, side):
        return 0

    def wallet_balance(self, side):
        return self._wallets.get(side)

    def portfolio_baseline(self, side):
        return self._baselines.get(side)

    async def dispatch_entry(self, side, symbol, **kw):
        self.dispatched.append({"side": side, "symbol": symbol, **kw})
        return True


class _FakeLogger:
    def __init__(self) -> None:
        self.records: list = []

    def record(self, record, *, module="default"):
        self.records.append((module, record))


def _cache(regime=Regime.TRENDING_POS_NORMAL, ema20="105", ema50="100",
           market_regime=Regime.NON_DIR_NORMAL):
    classification = SimpleNamespace(regime=regime, ema20=Decimal(ema20), ema50=Decimal(ema50))
    return SimpleNamespace(get=lambda s: classification, market_regime=market_regime)


def _warmup(indicators=None, *, close_1h="106", ema20_1h="104"):
    return SimpleNamespace(
        indicators=indicators or _FakeIndicators(),
        htf=HtfCache(close_1h=Decimal(close_1h), ema20_1h=Decimal(ema20_1h), ema50_1h=Decimal("100")),
    )


def _candle(close="60000", open_="59000", high="60100", low="58900", vol="2000"):
    return CommittedCandle(symbol="BTC/USD", interval_begin=1700000300, open=Decimal(open_),
                           high=Decimal(high), low=Decimal(low), close=Decimal(close), volume=Decimal(vol))


def _providers(**over):
    base = dict(
        instrument=lambda s: ("online", True, "600000"),
        bbo=lambda s: (Decimal("59990"), Decimal("60000")),
        expected_reward=lambda s, r: Decimal("0.05"),
        mpp_abs_cap_pct=lambda s, side: Decimal("0.01"),
        base_per_trade_size=lambda s, side, ref: Decimal("50"),
        ws_state=lambda s: "Subscribed",
        new_cl_ord_id=lambda: "cl-1",
        new_deadline=lambda: "2026-06-15T07:30:00Z",
    )
    base.update(over)
    return LiveProviders(**base)


# --------------------------------------------------------------------------- permitted_sides
def test_permitted_sides_trending_pos_is_long_only():
    assert permitted_sides(Regime.TRENDING_POS_NORMAL) == [PositionSide.LONG]


def test_permitted_sides_trending_neg_is_short_only():
    assert permitted_sides(Regime.TRENDING_NEG_NORMAL) == [PositionSide.SHORT]


def test_permitted_sides_non_dir_normal_is_both():
    assert permitted_sides(Regime.NON_DIR_NORMAL) == [PositionSide.LONG, PositionSide.SHORT]


# --------------------------------------------------------------------------- commitment helpers
def test_candidate_committed_long_is_usd_debit():
    # notional 100 * (1 + 0.0026 taker) = 100.26
    assert candidate_committed_usd(PositionSide.LONG, "100") == Decimal("100") * Decimal("1.0026")


def test_candidate_committed_short_is_leverage_bounded():
    # notional 300 / 3x leverage * (1 + 0.0026) = 100.26
    got = candidate_committed_usd(PositionSide.SHORT, "300")
    assert got == Decimal("300") / Decimal("3") * Decimal("1.0026")


def test_total_committed_sums_same_side_only():
    wm = _FakeWM(positions={
        "BTC/USD": _Pos(PositionSide.LONG, "1", "100"),
        "ETH/USD": _Pos(PositionSide.LONG, "2", "50"),
        "SOL/USD": _Pos(PositionSide.SHORT, "10", "30"),
    })
    # LONG: 1*100 + 2*50 = 200 (the short position is isolated out).
    assert total_committed_usd(wm, PositionSide.LONG) == Decimal("200")
    # SHORT: 10*30 / 3x leverage = 100.
    assert total_committed_usd(wm, PositionSide.SHORT) == Decimal("300") / Decimal("3")


# --------------------------------------------------------------------------- live evaluator
def test_live_evaluator_uses_indicators_not_series():
    ind = _FakeIndicators(passing=True)
    ev = live_sss_evaluator(ind)
    v = ev("BTC/USD", (), (), side=SignalSide.LONG)  # empty series ignored
    assert v.passed is True
    assert ev("BTC/USD", (), (), side=SignalSide.SHORT).passed is True


# --------------------------------------------------------------------------- assemble field sourcing
def test_assemble_sources_each_field():
    wm = _FakeWM()
    inputs, ctx = assemble_candidate(
        "BTC/USD", PositionSide.LONG, candle=_candle(), warmup=_warmup(), regime_cache=_cache(),
        providers=_providers(), wm=wm,
    )
    assert inputs.regime is Regime.TRENDING_POS_NORMAL
    assert inputs.ema20_daily == Decimal("105") and inputs.ema50_daily == Decimal("100")
    assert inputs.close_1h == Decimal("106") and inputs.ema20_1h == Decimal("104")
    assert inputs.closes == () and inputs.volumes == ()            # ar:AR-075 live cache path
    assert inputs.entry_fill_price == Decimal("60000")            # ar:AR-069 = candle close
    assert inputs.atr_14 == Decimal("1000")                       # ar:AR-016 live ATR
    assert inputs.candle_open == Decimal("59000")
    assert inputs.wallet_balance == Decimal("5000")
    assert inputs.portfolio_baseline == Decimal("5000")
    assert inputs.expected_reward == Decimal("0.05")
    assert ctx.sized_usd == Decimal("50")                         # base 50 * 1.0 (TRENDING_POS long)
    assert ctx.atr_14_entry == Decimal("1000")
    assert ctx.regime_at_entry == Regime.TRENDING_POS_NORMAL.value
    assert ctx.mpp_abs_cap_pct == Decimal("0.01")
    assert ctx.cl_ord_id == "cl-1"
    # the entry-side producer context: the BTC anchor regime + the entry-trigger candle ISO stamp.
    assert ctx.market_regime == Regime.NON_DIR_NORMAL.value
    assert ctx.entry_timestamp_utc == "2023-11-14T22:18:20+00:00"   # interval_begin 1700000300 UTC


# --------------------------------------------------------------------------- sweep_pair
def test_sweep_accepts_and_dispatches():
    wm = _FakeWM()
    logger = _FakeLogger()
    results = asyncio.run(sweep_pair(
        wm, logger, candle=_candle(), warmup=_warmup(_FakeIndicators(passing=True)),
        regime_cache=_cache(), providers=_providers(),
    ))
    assert len(results) == 1                       # TRENDING_POS_NORMAL -> long only
    assert results[0].outcome.accepted is True
    assert results[0].dispatched is True and results[0].filled is True
    assert wm.dispatched and wm.dispatched[0]["side"] is PositionSide.LONG
    assert logger.records and logger.records[0][0] == "long"


def test_sweep_sss_reject_stops_at_sss():
    wm = _FakeWM()
    logger = _FakeLogger()
    results = asyncio.run(sweep_pair(
        wm, logger, candle=_candle(), warmup=_warmup(_FakeIndicators(passing=False)),
        regime_cache=_cache(), providers=_providers(),
    ))
    assert len(results) == 1
    assert results[0].outcome.accepted is False
    assert results[0].outcome.stage == "SSS"
    assert results[0].dispatched is False
    assert wm.dispatched == []


def test_sweep_non_dir_normal_sweeps_both_sides():
    wm = _FakeWM()
    logger = _FakeLogger()
    results = asyncio.run(sweep_pair(
        wm, logger, candle=_candle(), warmup=_warmup(_FakeIndicators(passing=True)),
        regime_cache=_cache(regime=Regime.NON_DIR_NORMAL), providers=_providers(),
    ))
    sides = {r.outcome.side for r in results}
    assert sides == {PositionSide.LONG, PositionSide.SHORT}


def test_sweep_skips_side_without_module():
    wm = _FakeWM(wallets={PositionSide.LONG: Decimal("5000")})  # no short module
    logger = _FakeLogger()
    results = asyncio.run(sweep_pair(
        wm, logger, candle=_candle(), warmup=_warmup(_FakeIndicators(passing=True)),
        regime_cache=_cache(regime=Regime.NON_DIR_NORMAL), providers=_providers(),
    ))
    assert [r.outcome.side for r in results] == [PositionSide.LONG]


def test_assemble_feeds_ar052_mark_to_market_current_portfolio():
    # the candidate's PipelineInputs.current_portfolio is the MTM value (cash + bid MTM for a long),
    # DISTINCT from wallet_balance (realized cash). 5000 cash + 0.1 BTC * bid 59990 = 10999.
    wm = _FakeWM(positions={"BTC/USD": _Pos(PositionSide.LONG, "0.1", "55000", symbol="BTC/USD")})
    inputs, _ctx = assemble_candidate(
        "BTC/USD", PositionSide.LONG, candle=_candle(), warmup=_warmup(), regime_cache=_cache(),
        providers=_providers(), wm=wm,
    )
    assert inputs.wallet_balance == Decimal("5000")                       # ar:AR-051 realized cash
    assert inputs.current_portfolio == Decimal("5000") + Decimal("0.1") * Decimal("59990")  # ar:AR-052


def test_sweep_short_bleed_drawdown_halt_the_fn_case():
    # END-TO-END FALSE-NEGATIVE case (SAFETY): an OPEN short bleeding (price ROSE far above entry) ->
    # the ar:AR-052 equity drawdown FIRES at G7 CHECK 1; the realized margin CASH (flat at baseline)
    # would NOT have fired. NON_DIR_NORMAL so the SHORT side is swept; the bbo ask is well above entry.
    # the bleeding short is on ETH/USD (a DIFFERENT symbol than the BTC/USD candle) so the BTC/USD
    # candidate clears the Gate-5 same-side guard and reaches G7, where the module-wide equity
    # drawdown (driven by the ETH/USD short) halts it.
    short = _Pos(PositionSide.SHORT, "0.5", "60000", symbol="ETH/USD")
    wm = _FakeWM(positions={"ETH/USD": short})
    # MTM: 5000 collateral + (60000 - 75000)*0.5 = 5000 - 7500 = -2500 -> a massive drawdown (>10%).
    providers = _providers(bbo=lambda s: (Decimal("74990"), Decimal("75000")))
    logger = _FakeLogger()
    results = asyncio.run(sweep_pair(
        wm, logger, candle=_candle(), warmup=_warmup(_FakeIndicators(passing=True)),
        regime_cache=_cache(regime=Regime.NON_DIR_NORMAL), providers=providers,
    ))
    short_res = [r for r in results if r.outcome.side is PositionSide.SHORT]
    assert len(short_res) == 1
    out = short_res[0].outcome
    assert out.accepted is False
    assert out.stage == "G7"
    assert out.reason == "HALT"                       # full_halt drawdown -> module HALT
    assert short_res[0].dispatched is False           # no entry while the wallet is bleeding


def test_sweep_no_regime_is_skipped():
    wm = _FakeWM()
    logger = _FakeLogger()
    empty_cache = SimpleNamespace(get=lambda s: None)
    results = asyncio.run(sweep_pair(
        wm, logger, candle=_candle(), warmup=_warmup(), regime_cache=empty_cache,
        providers=_providers(),
    ))
    assert results == []


def test_sweep_skips_side_until_live_baseline_captured():
    # LIVE-readiness (slice d): the wallet is fed but the REST-BAL-004 startup baseline is NOT yet
    # captured -> portfolio_baseline(side) is None -> the side is skipped this tick (G7 CHECK 1 would
    # reject a None/<=0 baseline), exactly like a not-yet-ready wallet. The wallet alone is not enough.
    wm = _FakeWM(baselines={})  # both wallets present, no baseline captured yet
    logger = _FakeLogger()
    results = asyncio.run(sweep_pair(
        wm, logger, candle=_candle(), warmup=_warmup(_FakeIndicators(passing=True)),
        regime_cache=_cache(regime=Regime.NON_DIR_NORMAL), providers=_providers(),
    ))
    assert results == []


# --------------------------------------------------------------------------- LIVE sizing capstone
def test_live_sweep_sizes_and_dispatches_a_live_entry_add_order():
    # The capstone of live sizing (the entry mirror of the TB00764 exit capstone): a REAL live
    # WSManager with a FED BalancesCache (the live G8 wallet) + a captured REST-BAL-004 baseline runs
    # the full sweep over fakes and dispatches a LIVE entry add_order on the bound socket. NOTHING
    # connects to Kraken for real - the live_sender captures every transmitted message.
    sent: list = []

    async def _live(op, message):
        sent.append((op, message))

    wm = WSManager(Mode.LIVE, live_sender=_live)
    # the live G8 reads come ready: the balances snapshot feeds the wallet (WS-BAL-002) + the
    # REST-BAL-004 startup capture sets the baseline (LONG spot, the TRENDING_POS_NORMAL side).
    wm.ingest_balances({"type": "snapshot", "data": [
        {"asset": "USD", "wallets": [{"type": "spot", "id": "main", "balance": "5000.0"}]}]})
    wm.set_live_portfolio_baseline(PositionSide.LONG, "5000.0")
    assert wm.wallet_balance(PositionSide.LONG) == Decimal("5000.0")
    assert wm.portfolio_baseline(PositionSide.LONG) == Decimal("5000.0")

    logger = _FakeLogger()
    results = asyncio.run(sweep_pair(
        wm, logger, candle=_candle(), warmup=_warmup(_FakeIndicators(passing=True)),
        regime_cache=_cache(), providers=_providers(),   # TRENDING_POS_NORMAL -> LONG only
    ))
    assert len(results) == 1
    assert results[0].outcome.accepted is True
    # LIVE return contract (PA-004 div #4): dispatched True, filled False (the IOC fill is async).
    assert results[0].dispatched is True and results[0].filled is False
    # the marketable-IOC entry add_order transmitted over the bound socket (the live dispatch_entry).
    assert [op for op, _msg in sent] == [OutboundOp.ADD_ORDER]


def test_live_sweep_skips_when_wallet_cache_not_yet_fed():
    # the mirror case: a live WSManager whose balances snapshot has NOT arrived -> wallet_balance is
    # None -> the sweep skips the side (no dispatch), never a crash on wm.modules being None in live.
    sent: list = []

    async def _live(op, message):
        sent.append((op, message))

    wm = WSManager(Mode.LIVE, live_sender=_live)
    wm.set_live_portfolio_baseline(PositionSide.LONG, "5000.0")  # baseline set, but no wallet yet
    logger = _FakeLogger()
    results = asyncio.run(sweep_pair(
        wm, logger, candle=_candle(), warmup=_warmup(_FakeIndicators(passing=True)),
        regime_cache=_cache(), providers=_providers(),
    ))
    assert results == []
    assert sent == []
