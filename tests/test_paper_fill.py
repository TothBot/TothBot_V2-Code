"""Tests: the paper fill simulator (paper_fill.py).

Covers 0500000 dv1_241 sec 12 Image7 paper side (_simulate_entry_fill /
_maybe_paper_fill) + the byte-identical record_execution surface (D-06) + sec 12.4
ledger drive. The hook is async (awaited by the paper boundary) - driven with
stdlib asyncio.run over hand-built fakes; no network.
"""

from __future__ import annotations

import asyncio

from tothbot.exchange.paper_fill import (
    PaperFillKind,
    PaperFillSimulated,
    PaperFillSkipped,
    PaperFillSimulator,
)
from tothbot.exchange.seam import OutboundOp


def _sim(events: list | None = None):
    """A simulator wired to capturing fakes for record_execution + ledger applies."""
    frames: list = []
    entries: list = []
    exits: list = []

    def _record(frame):
        frames.append(dict(frame))
        return None

    def _entry(symbol, qty, price):
        entries.append((symbol, qty, price))
        return None

    def _exit(symbol, qty, price, *, exit_reason=None):
        exits.append((symbol, qty, price, exit_reason))
        return None

    sim = PaperFillSimulator(
        record_execution=_record,
        apply_entry_fill=_entry,
        apply_exit_fill=_exit,
        on_event=(events.append if events is not None else None),
    )
    return sim, frames, entries, exits


_ENTRY_MSG = {
    "method": "add_order",
    "params": {
        "symbol": "BTC/USD",
        "side": "buy",
        "order_qty": "0.05",
        "limit_price": "60000",
        "cl_ord_id": "cl-entry-1",
    },
}
_EXIT_MSG = {
    "method": "add_order",
    "params": {
        "symbol": "BTC/USD",
        "side": "sell",
        "order_qty": "0.05",
        "limit_price": "66000",
        "cl_ord_id": "cl-exit-1",
        "exit_reason": "L1A",
    },
}


# -- entry fill (ADD_ORDER) ---------------------------------------------

def test_entry_produces_filled_frame_to_record_execution():
    events: list = []
    sim, frames, entries, exits = _sim(events)
    asyncio.run(sim(OutboundOp.ADD_ORDER, _ENTRY_MSG))

    assert len(frames) == 1
    f = frames[0]
    assert f["exec_type"] == "filled"            # marketable-IOC fills-or-kills (AR-054)
    assert f["symbol"] == "BTC/USD"
    assert f["side"] == "buy"
    assert f["cum_qty"] == "0.05"                # WS-EXE-012 authoritative qty
    assert f["avg_price"] == "60000"
    assert f["cl_ord_id"] == "cl-entry-1"
    assert f["order_id"] == "PAPER_FILL_1"
    assert f["sequence"] == 1
    assert entries == [("BTC/USD", "0.05", "60000")]
    assert exits == []
    assert any(
        isinstance(e, PaperFillSimulated) and e.kind is PaperFillKind.ENTRY for e in events
    )


def test_entry_accepts_flat_message_without_params_envelope():
    sim, frames, entries, _ = _sim()
    flat = {"symbol": "ETH/USD", "side": "buy", "order_qty": "2", "limit_price": "3000"}
    asyncio.run(sim(OutboundOp.ADD_ORDER, flat))
    assert frames[0]["symbol"] == "ETH/USD"
    assert entries == [("ETH/USD", "2", "3000")]


# -- exit fill (DISPATCH_MARKET_SELL) -----------------------------------

def test_exit_produces_closing_frame_and_credits_ledger():
    events: list = []
    sim, frames, entries, exits = _sim(events)
    asyncio.run(sim(OutboundOp.DISPATCH_MARKET_SELL, _EXIT_MSG))

    f = frames[0]
    assert f["exec_type"] == "filled"
    assert f["side"] == "sell"                   # opposite side -> CLOSE in the mirror
    assert f["avg_price"] == "66000"
    assert entries == []
    assert exits == [("BTC/USD", "0.05", "66000", "L1A")]  # exit_reason threaded through
    assert any(
        isinstance(e, PaperFillSimulated) and e.kind is PaperFillKind.EXIT for e in events
    )


# -- non-fill ops (clean no-op) -----------------------------------------

def test_non_fill_ops_produce_no_fill():
    sim, frames, entries, exits = _sim()

    async def _drive():
        await sim(OutboundOp.BATCH_ADD, _ENTRY_MSG)       # emergSL leg - resting, no fill
        await sim(OutboundOp.CANCEL_ORDER, _ENTRY_MSG)
        await sim(OutboundOp.AMEND_ORDER, _ENTRY_MSG)
        await sim(OutboundOp.BATCH_CANCEL, _ENTRY_MSG)

    asyncio.run(_drive())
    assert frames == [] and entries == [] and exits == []


# -- malformed fill-producing message -> surfaced skip, never silent ----

def test_entry_missing_field_skips_and_surfaces_warning():
    events: list = []
    sim, frames, entries, _ = _sim(events)
    bad = {"params": {"symbol": "BTC/USD", "side": "buy", "order_qty": "0.05"}}  # no price
    asyncio.run(sim(OutboundOp.ADD_ORDER, bad))
    assert frames == [] and entries == []
    assert any(isinstance(e, PaperFillSkipped) for e in events)


# -- sequence monotonicity (Executions_Channel_Sequence) ----------------

def test_sequence_increments_monotonically_across_fills():
    sim, frames, *_ = _sim()

    async def _drive():
        await sim(OutboundOp.ADD_ORDER, _ENTRY_MSG)
        await sim(OutboundOp.DISPATCH_MARKET_SELL, _EXIT_MSG)

    asyncio.run(_drive())
    assert [f["sequence"] for f in frames] == [1, 2]
