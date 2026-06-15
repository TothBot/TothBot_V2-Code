"""mod:Long_Module / mod:Short_Module - the per-module trading wallet + dispatch identity.

Source: 0500000 dv1_250 sec 7 mod:Long_Module + mod:Short_Module (the parallel-module framework,
TB00000 sec 7) + decision:D-05 (paper_starting_balance $5,000 long / $5,000 short) + ar:AR-009
(short = Kraken margin) + the entry order path (execution/entry_dispatch.py).

Long_Module and Short_Module are PARALLEL SIBLINGS: each owns its OWN wallet, its OWN CIATS
instance, and its OWN per-wallet drawdown halts (TB00000 sec 7 / sec 8 per-wallet rule), while
SHARING the single WS data layer (the sockets + Position Mirror + the dispatch seam - never
duplicated). This class is that per-module entity: it holds the module's side + its own
synthetic wallet (contract:Synthetic_Capital_Ledger, seeded per side) and builds the module's
direction-correct outbound orders for the SHARED seam to transmit (a module NEVER calls
ws_private directly - HR-EE-013; everything traverses contract:WSManager_Dispatch_Seam).

The two instances are independent: the Long wallet is spot USD, the Short wallet is Kraken
margin equity; a loss in one never touches the other (the per-wallet isolation that the Gate-7
drawdown / concentration / exposure checks enforce). Constructing two of these (LONG + SHORT)
is how the organism runs both sides against one shared data layer.

Order construction is the direction mirror (execution/entry_dispatch.py, bound to this module's
side): the LONG module builds spot buy-to-open entries + sell-stop emergSLs; the SHORT module
builds Kraken margin sell-to-open entries + buy-to-cover reduce_only emergSLs (ar:AR-009). The
numeric sizing (order_qty / the MPP-capped entry limit / emergsl_price) is computed upstream
(gate:G8_Position_Sizer + the MPP cap); this module assembles its side's messages.
"""

from __future__ import annotations

from ..config import registry
from ..exchange.ledger import SyntheticCapitalLedger
from ..exchange.position_mirror import PositionSide
from ..execution.entry_dispatch import build_emergsl_order, build_entry_order

# The per-side paper wallet seed param names (decision:D-05; value home TB00000 sec 8).
_SEED_PARAM = {
    PositionSide.LONG: "paper_starting_balance_long_usd",
    PositionSide.SHORT: "paper_starting_balance_short_usd",
}


class TradingModule:
    """One trading module (mod:Long_Module or mod:Short_Module): a side + its own wallet + the
    side's order construction. Construct ONE per side; the two share only the injected WS data
    layer (seam / mirror), never their wallets."""

    def __init__(
        self,
        side: PositionSide,
        *,
        starting_balance: object | None = None,
        on_event=None,
    ) -> None:
        self.side = side
        # Own wallet, seeded with THIS side's paper_starting_balance (D-05; $5,000 each seed).
        seed = (
            starting_balance
            if starting_balance is not None
            else registry.value(_SEED_PARAM[side])
        )
        self.ledger = SyntheticCapitalLedger(seed, on_event=on_event)

    @property
    def is_short(self) -> bool:
        return self.side is PositionSide.SHORT

    @property
    def wallet_balance(self):
        """The module's own synthetic wallet balance (Long spot USD / Short margin equity)."""
        return self.ledger.balance

    @property
    def portfolio_baseline(self):
        """The module's own drawdown baseline (captured once at construction, HR-WM-011)."""
        return self.ledger.portfolio_baseline

    def build_entry(
        self,
        symbol: str,
        *,
        order_qty: object,
        entry_limit_price: object,
        cl_ord_id: str,
        deadline: str,
    ) -> dict:
        """Build this module's ENTRY add_order (LONG spot buy-to-open / SHORT margin sell-to-open)
        for the shared seam to transmit. Numeric fields computed upstream (G8 + MPP)."""
        return build_entry_order(
            symbol, self.side,
            order_qty=order_qty, entry_limit_price=entry_limit_price,
            cl_ord_id=cl_ord_id, deadline=deadline,
        )

    def build_emergsl(
        self,
        symbol: str,
        *,
        order_qty: object,
        emergsl_price: object,
        cl_ord_id: str,
        deadline: str,
    ) -> dict:
        """Build this module's ON-FILL emergSL batch_add (LONG sell-stop below / SHORT
        buy-to-cover reduce_only stop above entry, ar:AR-009) for the shared seam."""
        return build_emergsl_order(
            symbol, self.side,
            order_qty=order_qty, emergsl_price=emergsl_price,
            cl_ord_id=cl_ord_id, deadline=deadline,
        )
