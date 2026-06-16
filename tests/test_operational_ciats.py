"""The CIATS brain wired into the running organism (pipeline/operational.py slice (a)).

Covers the per-module assembly + the backed Parameter_Store_Snapshot provider (TB00747 (a)):
assemble_ciats_modules builds one CiatsConductor + one TRADE_CLOSE learning sink per side (Long /
Short, no cross-module pooling); make_cycle_parameters_provider backs the sweep's
LiveProviders.cycle_parameters(side) with build_cycle_parameters(the side's store, the conductor's
disallowed_regimes) so a CIATS-owned value AND the protective block list genuinely FLOW into the
gates per cycle (CI-IF-003 frozen, sacred R:R never served); and assemble_operational exposes the
conductors + the providers' live cycle_parameters. Driven with asyncio.run over fakes - no network.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from tothbot.config import registry
from tothbot.config.settings import Mode
from tothbot.exchange.pacing import SubscribeTokenBucket
from tothbot.exchange.position_mirror import PositionSide
from tothbot.pipeline.operational import assemble_ciats_modules, assemble_operational
from tothbot.pipeline.providers import make_cycle_parameters_provider
from tothbot.recorder.logger import Logger
from tothbot.regime.taxonomy import Regime
from tothbot.rest.client import OhlcResponse, RestOhlcBar


def _own(store, name, value, *, at=300):
    """Write an owned value into a Parameter Store (the apply() proposal shape)."""
    store.apply(
        SimpleNamespace(proposal=SimpleNamespace(param_name=name, proposed_value=value)),
        at_trade_count=at,
    )


# --------------------------------------------------------------------------- assemble_ciats_modules
def test_assemble_ciats_modules_builds_one_conductor_and_sink_per_side():
    conductors, sinks, _ = assemble_ciats_modules(Logger())
    assert set(conductors) == {PositionSide.LONG, PositionSide.SHORT}
    assert set(sinks) == {PositionSide.LONG, PositionSide.SHORT}
    assert conductors[PositionSide.LONG].module == "long"
    assert conductors[PositionSide.SHORT].module == "short"
    # the per-module pools are independent (no cross-module pooling, sec 7)
    assert conductors[PositionSide.LONG] is not conductors[PositionSide.SHORT]


def test_ciats_sink_feeds_the_matching_module_conductor():
    logger = Logger()
    conductors, sinks, _ = assemble_ciats_modules(logger)
    tc = SimpleNamespace(event="TRADE_CLOSE", net_pl_usd=Decimal("10"), net_gain_usd=Decimal("10"),
                         net_loss_usd=Decimal("0"), asset_regime="TRENDING_POS_NORMAL")
    sinks[PositionSide.LONG](tc)
    assert conductors[PositionSide.LONG].trade_count == 1   # the long loop learned it
    assert conductors[PositionSide.SHORT].trade_count == 0  # the short loop did NOT (per-module)
    assert len(logger.corpus_for("long")) == 0              # (not a 25-field record - corpus skips it)


# ------------------------------------------------------------------ make_cycle_parameters_provider
def test_provider_is_seed_only_until_the_store_owns_a_value():
    conductors, _, _ = assemble_ciats_modules(Logger())
    provider = make_cycle_parameters_provider(conductors)
    params = provider(PositionSide.LONG)
    assert params.get("mae_mult") == registry.value("mae_mult")        # the seed (no owned value yet)
    assert params.disallowed_regimes == frozenset()


def test_provider_flows_an_owned_value_into_the_cycle_view():
    conductors, _, _ = assemble_ciats_modules(Logger())
    _own(conductors[PositionSide.LONG].parameter_store, "mae_mult", Decimal("3.0"))
    provider = make_cycle_parameters_provider(conductors)
    assert provider(PositionSide.LONG).get("mae_mult") == Decimal("3.0")   # the owned value flows
    # the SHORT module is isolated - its store still serves the seed (no cross-module bleed)
    assert provider(PositionSide.SHORT).get("mae_mult") == registry.value("mae_mult")


def test_provider_snapshot_is_frozen_against_a_mid_cycle_write():
    conductors, _, _ = assemble_ciats_modules(Logger())
    provider = make_cycle_parameters_provider(conductors)
    params = provider(PositionSide.LONG)                       # the frozen read for this cycle
    _own(conductors[PositionSide.LONG].parameter_store, "mae_mult", Decimal("9.0"))
    assert params.get("mae_mult") == registry.value("mae_mult")  # CI-IF-003: this cycle is unchanged


def test_provider_surfaces_the_conductor_disallowed_regimes():
    conductors, _, _ = assemble_ciats_modules(Logger())
    conductor = conductors[PositionSide.LONG]
    bad = Regime.NON_DIR_NORMAL
    win = SimpleNamespace(net_pl_usd=Decimal("1"), net_gain_usd=Decimal("1"), net_loss_usd=Decimal("0"))
    loss = SimpleNamespace(net_pl_usd=Decimal("-5"), net_gain_usd=Decimal("0"), net_loss_usd=Decimal("5"))
    for _ in range(40):
        conductor.ingest_close(win, regime=bad)
    for _ in range(100):
        conductor.ingest_close(loss, regime=bad)              # negative-edge, ACTIVE bucket
    for _ in range(460):                                       # pad the library total to >= 600
        conductor.ingest_close(win, regime=Regime.TRENDING_POS_NORMAL)
    params = make_cycle_parameters_provider(conductors)(PositionSide.LONG)
    assert params.regime_disallowed(bad) is True              # the protective block list -> Gate-3
    assert params.regime_disallowed(Regime.TRENDING_POS_NORMAL) is False


def test_provider_unwired_side_is_seed_only():
    provider = make_cycle_parameters_provider({})              # no conductors
    assert provider(PositionSide.LONG).get("mae_mult") == registry.value("mae_mult")


# --------------------------------------------------------------------------- the full assembly
def _resp(closes, *, interval, base=1700000000, vol=1000):
    committed = [RestOhlcBar(time=base + i * interval, open=Decimal(c), high=Decimal(c) + 2,
                             low=Decimal(c) - 2, close=Decimal(c), volume=Decimal(vol))
                 for i, c in enumerate(closes)]
    forming = RestOhlcBar(time=base + len(closes) * interval, open=Decimal(9), high=Decimal(9),
                          low=Decimal(9), close=Decimal(9), volume=Decimal(1))
    return OhlcResponse(committed=tuple(committed), forming=forming, last=committed[-1].time)


class _FakeRest:
    async def get_ohlc_data(self, pair, interval, *, since=None):
        n = 60
        return _resp([100 + i for i in range(n)], interval=300 if interval == 5 else
                     (86400 if interval == 1440 else 3600))

    async def get_ticker_liquidity(self, pair):
        return Decimal("600000")


class _Transport:
    async def send(self, m):
        return None

    async def recv(self):  # pragma: no cover
        raise AssertionError

    async def close(self):  # pragma: no cover
        return None


class _WM:
    is_live = False
    modules = {PositionSide.LONG: SimpleNamespace(portfolio_baseline=Decimal("5000")),
               PositionSide.SHORT: SimpleNamespace(portfolio_baseline=Decimal("5000"))}

    def open_positions(self):
        return {}

    def position(self, _s):
        return None

    def wallet_balance(self, side):
        return Decimal("5000")

    def on_regime_classified(self, *a, **k):
        return None


def _assemble():
    async def no_sleep(_s):
        return None

    async def opener(_k):
        return _Transport()

    from tothbot.ciats.expected_reward import ExpectedRewardStore
    from tothbot.ciats.seed_estimators import MppCapStore
    logger = Logger()
    system = asyncio.run(assemble_operational(
        universe=["BTC/USD"], rest_client=_FakeRest(), open_socket=opener,
        bucket=SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0),
        wm=_WM(), logger=logger, mpp_store=MppCapStore(), reward_store=ExpectedRewardStore(),
        mode=Mode.PAPER, now_utc=lambda: datetime(2026, 6, 15, 7, 30, tzinfo=timezone.utc),
        rest_sleep=no_sleep, pace_sleep=no_sleep,
    ))
    return system, logger


def test_assemble_operational_exposes_the_per_module_conductors():
    system, _ = _assemble()
    assert set(system.conductors) == {PositionSide.LONG, PositionSide.SHORT}
    assert set(system.ciats_sinks) == {PositionSide.LONG, PositionSide.SHORT}
    assert set(system.approval_inboxes) == {PositionSide.LONG, PositionSide.SHORT}


def _assemble_with(wm, logger=None):
    async def no_sleep(_s):
        return None

    async def opener(_k):
        return _Transport()

    from tothbot.ciats.expected_reward import ExpectedRewardStore
    from tothbot.ciats.seed_estimators import MppCapStore
    logger = logger or Logger()
    system = asyncio.run(assemble_operational(
        universe=["BTC/USD"], rest_client=_FakeRest(), open_socket=opener,
        bucket=SubscribeTokenBucket(rate_per_sec=1000.0, burst_capacity=100000.0),
        wm=wm, logger=logger, mpp_store=MppCapStore(), reward_store=ExpectedRewardStore(),
        mode=Mode.PAPER, now_utc=lambda: datetime(2026, 6, 15, 7, 30, tzinfo=timezone.utc),
        rest_sleep=no_sleep, pace_sleep=no_sleep,
    ))
    return system, logger


def test_assemble_operational_hands_the_exit_sinks_to_the_wm():
    # TB00748 (b): the assembly wires each side's ciats_sink into the wm's per-module Exit
    # Controller (wm.set_ciats_exit_sinks), resolving the construction-order seam - the wm is
    # injected, the sinks are built here, the assembly hands them over.
    captured: dict = {}

    class _SinkWM(_WM):
        def set_ciats_exit_sinks(self, sinks):
            captured.update(sinks)

    system, _ = _assemble_with(_SinkWM())
    assert captured == dict(system.ciats_sinks)            # exactly the per-side assembled sinks
    assert set(captured) == {PositionSide.LONG, PositionSide.SHORT}


def test_assemble_operational_leaves_a_wm_without_the_surface_untouched():
    # the guard: a wm that does NOT expose set_ciats_exit_sinks (a lightweight stand-in) assembles
    # cleanly - the tie-in is skipped, not an error (operational.py owns construction order only).
    system, _ = _assemble_with(_WM())                      # _WM has no set_ciats_exit_sinks
    assert set(system.ciats_sinks) == {PositionSide.LONG, PositionSide.SHORT}


def test_assemble_operational_backs_the_cycle_parameters_provider_with_the_store():
    system, _ = _assemble()
    # the providers' per-cycle snapshot is LIVE (not None / seed-only-by-default): an owned store
    # value flows through the provider the sweep reads each cycle.
    assert system.providers.cycle_parameters is not None
    _own(system.conductors[PositionSide.LONG].parameter_store, "min_volume_usd_daily", Decimal("123"))
    params = system.providers.cycle_parameters(PositionSide.LONG)
    assert params.get("min_volume_usd_daily") == Decimal("123")


def test_assemble_operational_routes_a_staged_approval_to_the_logger_alert_seam():
    system, logger = _assemble()
    conductor = system.conductors[PositionSide.LONG]
    win = SimpleNamespace(net_pl_usd=Decimal("2"), net_gain_usd=Decimal("2"), net_loss_usd=Decimal("0"))
    loss = SimpleNamespace(net_pl_usd=Decimal("-1"), net_gain_usd=Decimal("0"), net_loss_usd=Decimal("1"))
    for _ in range(120):
        conductor.ingest_close(win)
    for _ in range(80):
        conductor.ingest_close(loss)                     # 200 trades: W=0.6, R=2 -> K_full=0.4 > 0
    conductor.recompute_kelly(wallet_balance=Decimal("5000"))
    # the HR-CI-011 ApprovalRequested reached the mod:Logger operator-alert seam (HR-LG-009).
    assert any(getattr(a, "code", None) == "CIATS_APPROVAL_REQUESTED" for a in logger.alerts)


def test_assemble_operational_applies_an_inbox_approved_change_through_the_sink():
    system, _ = _assemble()
    side = PositionSide.LONG
    conductor = system.conductors[side]
    win = SimpleNamespace(net_pl_usd=Decimal("2"), net_gain_usd=Decimal("2"), net_loss_usd=Decimal("0"),
                          event="TRADE_CLOSE", asset_regime="TRENDING_POS_NORMAL")
    loss = SimpleNamespace(net_pl_usd=Decimal("-1"), net_gain_usd=Decimal("0"), net_loss_usd=Decimal("1"),
                           event="TRADE_CLOSE", asset_regime="TRENDING_POS_NORMAL")
    for _ in range(119):
        conductor.ingest_close(win)
    for _ in range(80):
        conductor.ingest_close(loss)                     # 199 trades, just below the cadence boundary
    # the 200th close arrives THROUGH the wired sink (the exit-path membrane): it ingests, but the
    # proposal is staged explicitly here (the sink does not recompute Kelly) - then Bill approves.
    system.ciats_sinks[side](win)                        # 200th -> learning close + (empty) boundary
    conductor.recompute_kelly(wallet_balance=Decimal("5000"))
    req = conductor.pending[0]
    system.approval_inboxes[side].submit(req.request_id, approved=True)
    system.ciats_sinks[side](win)                        # next confirmed close = the boundary -> apply
    assert conductor.parameter_store.get("per_trade_size_usd") is not None
