"""Tests: phase-2 bulk pre-screen (tothbot/app/prescreen.py).

Covers liquidity_by_symbol (the REST-key -> wsname join, drop unmapped keys), select_top_n (rank by
24h USD liquidity desc, the ar:AR-074 anchor always kept regardless of rank, missing-liquidity sorts
last, deterministic sort, top_n<=0 / >=len no-op), and screen_universe end to end over a fake REST
client (two governed calls -> top-N; the disabled mode makes ZERO REST calls). Driven with asyncio.run -
no network.
"""

from __future__ import annotations

import asyncio
from decimal import Decimal

from tothbot.app.prescreen import liquidity_by_symbol, screen_universe, select_top_n


# --------------------------------------------------------------------------- liquidity_by_symbol
def test_liquidity_by_symbol_joins_and_drops_unmapped():
    by_key = {"XXBTZUSD": Decimal("5000000"), "XETHZUSD": Decimal("600000"), "ORPHAN": Decimal("99")}
    keymap = {"XXBTZUSD": "BTC/USD", "XETHZUSD": "ETH/USD"}  # ORPHAN has no wsname
    out = liquidity_by_symbol(by_key, keymap)
    assert out == {"BTC/USD": Decimal("5000000"), "ETH/USD": Decimal("600000")}


# --------------------------------------------------------------------------- select_top_n
def test_select_top_n_ranks_by_liquidity_desc():
    derived = ("BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD")
    liq = {"BTC/USD": Decimal("100"), "ETH/USD": Decimal("80"),
           "SOL/USD": Decimal("60"), "DOGE/USD": Decimal("40")}
    # top-2 by liquidity = BTC + ETH (anchor BTC also kept, already in).
    assert select_top_n(derived, liq, top_n=2) == ("BTC/USD", "ETH/USD")


def test_select_top_n_always_keeps_anchor_even_if_illiquid():
    derived = ("BTC/USD", "ETH/USD", "SOL/USD")
    liq = {"BTC/USD": Decimal("1"), "ETH/USD": Decimal("90"), "SOL/USD": Decimal("80")}
    # top-1 by liquidity = ETH, but BTC/USD (ar:AR-074 anchor) is kept regardless of rank.
    out = select_top_n(derived, liq, top_n=1)
    assert "BTC/USD" in out and "ETH/USD" in out
    assert out == ("BTC/USD", "ETH/USD")


def test_select_top_n_missing_liquidity_sorts_last():
    derived = ("AAA/USD", "BBB/USD", "CCC/USD")
    liq = {"AAA/USD": Decimal("10"), "BBB/USD": Decimal("5")}  # CCC has no reading -> score 0
    # top-2 -> AAA + BBB; CCC (unscored) excluded. No anchor in this derived set.
    out = select_top_n(derived, liq, top_n=2, always_include=())
    assert out == ("AAA/USD", "BBB/USD")


def test_select_top_n_noop_passthrough_when_n_ge_len_or_le_zero():
    derived = ("BTC/USD", "ETH/USD")
    liq = {"BTC/USD": Decimal("1"), "ETH/USD": Decimal("2")}
    assert select_top_n(derived, liq, top_n=0) == ("BTC/USD", "ETH/USD")
    assert select_top_n(derived, liq, top_n=99) == ("BTC/USD", "ETH/USD")


def test_select_top_n_deterministic_tie_break_by_symbol():
    derived = ("AAA/USD", "BBB/USD", "CCC/USD")
    liq = {"AAA/USD": Decimal("5"), "BBB/USD": Decimal("5"), "CCC/USD": Decimal("5")}  # all tie
    # ties broken by symbol DESC in the ranking; top-2 -> CCC, BBB; result sorted ascending.
    out = select_top_n(derived, liq, top_n=2, always_include=())
    assert out == ("BBB/USD", "CCC/USD")


# --------------------------------------------------------------------------- screen_universe
class _FakeRest:
    def __init__(self, liquidity_by_key, keymap) -> None:
        self._liq = liquidity_by_key
        self._keymap = keymap
        self.ticker_calls = 0
        self.asset_pairs_calls = 0

    async def get_all_ticker_liquidity(self):
        self.ticker_calls += 1
        return self._liq

    async def get_asset_pairs(self):
        self.asset_pairs_calls += 1
        return self._keymap


def test_screen_universe_end_to_end_two_calls_top_n():
    derived = ("BTC/USD", "ETH/USD", "SOL/USD", "DOGE/USD")
    rest = _FakeRest(
        liquidity_by_key={"XXBTZUSD": Decimal("100"), "XETHZUSD": Decimal("80"),
                          "SOLUSD": Decimal("60"), "DOGEUSD": Decimal("40")},
        keymap={"XXBTZUSD": "BTC/USD", "XETHZUSD": "ETH/USD",
                "SOLUSD": "SOL/USD", "DOGEUSD": "DOGE/USD"},
    )
    out = asyncio.run(screen_universe(rest, derived, top_n=2))
    assert out == ("BTC/USD", "ETH/USD")
    assert rest.ticker_calls == 1 and rest.asset_pairs_calls == 1  # exactly two bulk calls


def test_screen_universe_disabled_makes_no_rest_calls():
    derived = ("BTC/USD", "ETH/USD", "SOL/USD")
    rest = _FakeRest(liquidity_by_key={}, keymap={})
    out = asyncio.run(screen_universe(rest, derived, top_n=0))
    assert out == ("BTC/USD", "ETH/USD", "SOL/USD")
    assert rest.ticker_calls == 0 and rest.asset_pairs_calls == 0  # short-circuit: no REST
