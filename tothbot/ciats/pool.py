"""mod:CIATS per-module trade-outcome pool - the accumulator, the 200-trade floor, Half-Kelly.

Source: 0500000 dv1_250 sec 6/7 mod:CIATS (per-module framework) + ar:AR-065 (Half-Kelly uses
NET P/L exclusively) + the 200-trade hard data floor (sec 9 / registry "200-trade hard floor")
+ contract:CIATS_Trade_Outcome_Bus (the Logger's Stream-2 corpus this reads).

CIATS is a PER-MODULE engine: one pool per trading module (Long + Short), each with its OWN
statistical pool, its OWN 200-trade floor, and its OWN Parameter Store - NO cross-module pooling
(sec 7). This class is one such pool: it ingests a module's closed-trade outcomes (the schema-
valid evt:TRADE_CLOSE records mod:Logger routed to this module's corpus) and exposes the
realized statistics CIATS sizes from.

THE 200-TRADE HARD FLOOR. CIATS may NOT tune any parameter from realized data until the module
has booked at least 200 closed trades (a hard architectural floor, never lowered - it gates
statistical validity, like the sacred R:R is the one hardcoded acceptance value). Below the
floor the seed values stand; at/above it the realized statistics drive the sizing curve.

HALF-KELLY (ar:AR-065, NET P/L). The Kelly-optimal fraction of capital to risk is
    f* = W - (1 - W) / R
where W = realized win rate and R = realized net reward:risk = avg(net_gain) / avg(net_loss)
(both NET of fees - the only basis the Kelly math uses, ar:AR-065; a SHORT's net P&L includes
the margin borrow fee). TothBot sizes at HALF-Kelly (f*/2, the conservative half) and clamps it
to [0, 1] (never negative, never above the whole wallet - the wallet is the sole hard bound).

PURE state + Decimal-only (ar:AR-047). The async EWMA monitor / PDCA / Proposal Engine / the
full stat suite (Mann-Whitney U, Sharpe, Spearman, CUSUM) are separate elements that read this
pool; this is the accumulator + floor + Half-Kelly core.
"""

from __future__ import annotations

from decimal import Decimal

# The 200-trade hard data floor (sec 9) - CIATS tunes from realized data ONLY at/above it. A
# fixed architectural floor, never CIATS-lowered.
CIATS_TRADE_FLOOR = 200

_ZERO = Decimal("0")
_ONE = Decimal("1")
_TWO = Decimal("2")


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the pool (AR-047)."""
    return Decimal(str(value))


class CiatsPool:
    """One module's CIATS trade-outcome pool (Long or Short). Accumulates net-P/L outcomes, gates
    at the 200-trade floor, and computes the realized win rate + net R:R + the Half-Kelly fraction.
    No cross-module pooling - construct ONE per side."""

    def __init__(self, *, trade_floor: int = CIATS_TRADE_FLOOR) -> None:
        self._trade_floor = trade_floor
        self._trades = 0
        self._wins = 0
        self._sum_gain = _ZERO   # sum of net_gain over winning trades (NET of fees)
        self._sum_loss = _ZERO   # sum of net_loss over losing trades (NET of fees, positive)

    def ingest_outcome(self, *, net_pl: object, net_gain: object, net_loss: object) -> None:
        """Accumulate one closed-trade outcome (net of fees). A WIN (net_pl > 0) adds net_gain;
        a LOSS adds net_loss (the positive loss magnitude). Both NET P/L per ar:AR-065."""
        self._trades += 1
        if _dec(net_pl) > _ZERO:
            self._wins += 1
            self._sum_gain += _dec(net_gain)
        else:
            self._sum_loss += _dec(net_loss)

    def ingest(self, trade_close: object) -> None:
        """Accumulate one evt:TRADE_CLOSE record (the Logger Stream-2 corpus shape): reads
        net_pl_usd / net_gain_usd / net_loss_usd."""
        self.ingest_outcome(
            net_pl=trade_close.net_pl_usd,
            net_gain=trade_close.net_gain_usd,
            net_loss=trade_close.net_loss_usd,
        )

    @property
    def trade_count(self) -> int:
        return self._trades

    @property
    def ready(self) -> bool:
        """True once the module has booked >= the 200-trade floor (CIATS may tune from data)."""
        return self._trades >= self._trade_floor

    @property
    def win_rate(self) -> Decimal | None:
        """Realized win rate (wins / trades), or None with no trades yet."""
        return None if self._trades == 0 else Decimal(self._wins) / Decimal(self._trades)

    @property
    def net_reward_risk(self) -> Decimal | None:
        """Realized net reward:risk R = avg(net_gain) / avg(net_loss), or None until there is at
        least one win AND one loss (both legs needed for the ratio)."""
        losses = self._trades - self._wins
        if self._wins == 0 or losses == 0 or self._sum_loss == _ZERO:
            return None
        avg_gain = self._sum_gain / Decimal(self._wins)
        avg_loss = self._sum_loss / Decimal(losses)
        return avg_gain / avg_loss

    def half_kelly_fraction(self) -> Decimal | None:
        """The Half-Kelly fraction f*/2 = (W - (1-W)/R) / 2, clamped to [0, 1] (ar:AR-065, NET
        P/L). Returns None below the 200-trade floor (the seed sizing stands) or when W / R are
        not yet both defined - the caller falls back to the seed sizing in that case."""
        if not self.ready:
            return None
        w = self.win_rate
        r = self.net_reward_risk
        if w is None or r is None:
            return None
        f_star = w - (_ONE - w) / r
        half = f_star / _TWO
        if half < _ZERO:
            return _ZERO
        if half > _ONE:
            return _ONE
        return half
