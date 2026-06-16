"""Tests for the live balances cache (WS-BAL-002/003 wallet state for live sizing).

Covers 0500000 dv1_253 sec 12.4 + the WS-BAL-002 spot/main-only LONG read + the ar:AR-050 margin
SHORT read + the WS-BAL-003 snapshot-replaces / update-merges semantics, all Decimal-on-receipt
(ar:AR-047) and Long/Short symmetric.
"""

from __future__ import annotations

from decimal import Decimal

from tothbot.exchange.balances_cache import (
    BalancesCache,
    BalancesSnapshotApplied,
    BalancesUpdated,
)


def _snapshot(spot_usd="5000.0", margin_usd="3000.0"):
    """A WS v2 balances SNAPSHOT carrying the USD spot/main (long) + margin (short) wallets, plus a
    futures wallet that WS-BAL-002 MUST filter out of the long portfolio."""
    return [
        {"asset": "USD", "wallets": [
            {"type": "spot", "id": "main", "balance": spot_usd},
            {"type": "margin", "id": "margin1", "balance": margin_usd},
            {"type": "futures", "id": "main", "balance": "9999.0"},
        ]},
        {"asset": "XBT", "wallets": [{"type": "spot", "id": "main", "balance": "0.5"}]},
    ]


def test_snapshot_seeds_long_spot_main_and_short_margin():
    c = BalancesCache()
    evt = c.apply_snapshot(_snapshot())
    assert c.spot_main_usd() == Decimal("5000.0")    # LONG = wallets[spot][main] USD
    assert c.margin_usd() == Decimal("3000.0")       # SHORT = the USD margin wallet
    assert isinstance(evt, BalancesSnapshotApplied)
    assert evt.spot_main_usd == Decimal("5000.0") and evt.margin_usd == Decimal("3000.0")


def test_ws_bal_002_filters_non_spot_main_out_of_the_long_wallet():
    # the futures USD wallet (9999) must NEVER reach the long portfolio read.
    c = BalancesCache()
    c.apply_snapshot(_snapshot(spot_usd="5000.0"))
    assert c.spot_main_usd() == Decimal("5000.0")
    assert c.balance("USD", "futures") == Decimal("9999.0")   # cached, but not the long read


def test_balances_are_decimal_on_receipt_never_float():
    c = BalancesCache()
    c.apply_snapshot(_snapshot(spot_usd="5000.55"))
    bal = c.spot_main_usd()
    assert isinstance(bal, Decimal) and bal == Decimal("5000.55")


def test_update_merges_only_the_changed_wallet_keeps_the_rest():
    c = BalancesCache()
    c.apply_snapshot(_snapshot(spot_usd="5000.0", margin_usd="3000.0"))
    # an UPDATE delta carrying ONLY the spot/main change (a buy debited it); margin is untouched.
    evt = c.apply_update([{"asset": "USD", "wallets": [
        {"type": "spot", "id": "main", "balance": "4200.0"},
    ]}])
    assert c.spot_main_usd() == Decimal("4200.0")    # merged
    assert c.margin_usd() == Decimal("3000.0")       # retained (not in the delta)
    assert isinstance(evt, BalancesUpdated) and evt.spot_main_usd == Decimal("4200.0")


def test_snapshot_replaces_wholesale_dropping_a_stale_wallet():
    c = BalancesCache()
    c.apply_snapshot(_snapshot(spot_usd="5000.0", margin_usd="3000.0"))
    # a fresh snapshot with NO margin wallet -> the stale margin balance must not linger.
    c.apply_snapshot([{"asset": "USD", "wallets": [{"type": "spot", "id": "main", "balance": "5100.0"}]}])
    assert c.spot_main_usd() == Decimal("5100.0")
    assert c.margin_usd() is None


def test_margin_usd_aggregates_only_margin_ignores_spot():
    c = BalancesCache()
    c.apply_snapshot([{"asset": "USD", "wallets": [
        {"type": "spot", "id": "main", "balance": "5000.0"},
        {"type": "margin", "id": "m1", "balance": "1000.0"},
        {"type": "margin", "id": "m2", "balance": "250.0"},
    ]}])
    assert c.margin_usd() == Decimal("1250.0")       # sum of the margin USD wallets only
    assert c.spot_main_usd() == Decimal("5000.0")


def test_empty_cache_reads_none():
    c = BalancesCache()
    assert c.spot_main_usd() is None and c.margin_usd() is None


def test_reset_clears_for_reconnect_reseed():
    c = BalancesCache()
    c.apply_snapshot(_snapshot())
    c.reset()
    assert c.spot_main_usd() is None and c.margin_usd() is None


def test_malformed_frame_is_ignored_not_a_crash():
    c = BalancesCache()
    c.apply_snapshot("not-a-list")
    c.apply_update([{"asset": "USD"}, {"no_asset": True}, 42, {"asset": "USD", "wallets": "bad"}])
    assert c.spot_main_usd() is None and c.margin_usd() is None
