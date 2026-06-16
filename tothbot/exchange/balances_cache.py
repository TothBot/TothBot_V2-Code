"""mod:WS_Manager - the live balances cache (the WS-BAL-002/003 wallet state for live sizing).

Source: 0500000 dv1_253 sec 12.4 (live mode: real Kraken balances are authoritative, the synthetic
ledger is bypassed) + sec 7 mod:WS_Manager balances channel + the D1 PRIVATE-CHANNEL WIRE FACTS:
  WS-BAL-002 -- MANDATORY: use wallets[type=spot][id=main] ONLY for the LONG (spot) module's portfolio
    USD; filter out any non-spot, non-main entry (futures/staking are NEVER in the long wallet). The
    SHORT module trades Kraken MARGIN (ar:AR-009) and sources its wallet from the margin account
    (balance + open-margin-position equity reconciled via REST OpenPositions, ar:AR-050), DISTINCT
    from the long spot/main wallet.
  WS-BAL-003 -- a SNAPSHOT carries the full wallet state at subscription time; an UPDATE carries the
    changed (delta) entries only, and on update the delta is merged into the cached balance.
  REST-BAL-002/003/004 -- the GetAccountBalance REST snapshot (result {ZUSD, ...}, every value a
    string -> Decimal on parse) seeds / reconciles the same cached USD balance (the WS-BAL-005
    BALANCES_SEQUENCE_GAP -> REST reconcile path, a later wiring slice).

PURE + CLOCK-FREE (mirrors ledger.py / rate_counter.py - no socket, no asyncio, no wall clock). The
private-WS balances handler (a later wiring slice) injects each parsed frame; this unit only HOLDS the
per-(asset, wallet-type, wallet-id) USD balance and reflects WS-BAL-002's spot/main + margin reads.
The LONG and SHORT wallets are cached SYMMETRICALLY (the diagram's co-equal Long/Short paradigm): the
long reads (USD, spot, main), the short reads (USD, margin) - the open-margin-position equity that
AR-050 adds to the short wallet is layered on by the OpenPositions reconcile downstream, not here.
NO float ever enters the cache (ar:AR-047): every balance is Decimal(str(value)) on receipt.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal

EventSink = Callable[[object], None]

# WS-BAL-002 wallet selectors (the WS v2 wallet type/id tokens).
_WALLET_SPOT = "spot"
_WALLET_MAIN = "main"
_WALLET_MARGIN = "margin"
# The sizing currency: Kraken reports USD spot as "USD" on the WS v2 balances channel (the REST
# GetAccountBalance ZUSD asset is normalized to USD by the REST seed before it reaches this cache).
_USD = "USD"


def _dec(value: object) -> Decimal:
    """Decimal(str(value)) on receipt - NO float ever enters the cache (ar:AR-047)."""
    return value if isinstance(value, Decimal) else Decimal(str(value))


@dataclass(frozen=True)
class BalancesSnapshotApplied:
    """BALANCES_SNAPSHOT_APPLIED [INFO] (WS-BAL-003) - the full wallet state at subscription replaced
    the cache; spot_main_usd is the LONG portfolio USD, margin_usd the SHORT margin-account balance."""

    spot_main_usd: Decimal | None
    margin_usd: Decimal | None
    code: str = field(default="BALANCES_SNAPSHOT_APPLIED", init=False)


@dataclass(frozen=True)
class BalancesUpdated:
    """BALANCES_UPDATED [INFO] (WS-BAL-003) - a delta merged into the cache (only the changed wallet
    entries; the rest retained). Carries the post-merge spot/main + margin USD for telemetry."""

    spot_main_usd: Decimal | None
    margin_usd: Decimal | None
    code: str = field(default="BALANCES_UPDATED", init=False)


class BalancesCache:
    """The live per-wallet USD balance cache (WS-BAL-002/003). One instance per WSManager (live only;
    paper uses the synthetic ledger). Clock-free + pure: the balances handler feeds parsed frames, the
    G8 live sizer reads spot_main_usd (LONG) / margin_usd (SHORT). Keyed (asset, wallet-type,
    wallet-id) so the WS-BAL-002 filters are exact lookups, never a scan that could leak a futures or
    staking wallet into the long portfolio."""

    __slots__ = ("_balances",)

    def __init__(self) -> None:
        # (asset, wallet_type, wallet_id) -> Decimal USD balance. The spot/main long wallet and the
        # margin short wallet live in the SAME store, read by distinct keys (symmetric Long/Short).
        self._balances: dict[tuple[str, str, str], Decimal] = {}

    # --- ingest (WS-BAL-003 snapshot vs update) ------------------------------
    def apply_snapshot(self, data: object) -> BalancesSnapshotApplied:
        """WS-BAL-003 SNAPSHOT: the full wallet state at subscription - REPLACE the cache wholesale
        (a stale wallet absent from the snapshot must not linger), then merge in the snapshot entries.
        Returns the telemetry event with the resulting long/short USD reads."""
        self._balances.clear()
        self._merge(data)
        return BalancesSnapshotApplied(self.spot_main_usd(), self.margin_usd())

    def apply_update(self, data: object) -> BalancesUpdated:
        """WS-BAL-003 UPDATE: a delta carrying ONLY the changed wallet entries - MERGE each carried
        entry's new balance into the cache (the untouched wallets are retained). Returns the telemetry
        event with the post-merge long/short USD reads."""
        self._merge(data)
        return BalancesUpdated(self.spot_main_usd(), self.margin_usd())

    def _merge(self, data: object) -> None:
        """Parse a WS v2 balances payload (a list of per-asset records, each with a `wallets` list of
        {type, id, balance}) and set each (asset, type, id) -> Decimal(balance). Tolerant of a missing
        wallets list (an asset-level-only record is skipped - WS-BAL-002 reads are wallet-scoped)."""
        if not isinstance(data, (list, tuple)):
            return
        for record in data:
            if not isinstance(record, dict):
                continue
            asset = record.get("asset")
            wallets = record.get("wallets")
            if asset is None or not isinstance(wallets, (list, tuple)):
                continue
            for wallet in wallets:
                if not isinstance(wallet, dict):
                    continue
                wtype, wid, bal = wallet.get("type"), wallet.get("id"), wallet.get("balance")
                if wtype is None or wid is None or bal is None:
                    continue
                self._balances[(str(asset), str(wtype), str(wid))] = _dec(bal)

    # --- the WS-BAL-002 reads (the live G8 sizer's wallet sources) -----------
    def spot_main_usd(self) -> Decimal | None:
        """The LONG (spot) module's portfolio USD = wallets[type=spot][id=main] ONLY (WS-BAL-002), or
        None before any balances frame. A futures/staking wallet can NEVER reach this read - it is an
        exact (USD, spot, main) lookup, not a scan."""
        return self._balances.get((_USD, _WALLET_SPOT, _WALLET_MAIN))

    def margin_usd(self) -> Decimal | None:
        """The SHORT module's margin-account USD BALANCE component (WS-BAL-002 / ar:AR-050), or None
        before any balances frame. The sum over the USD margin wallet(s) (robust to the exact margin
        wallet id token, which the live balances handler confirms), filtered to type=margin so a spot
        or futures wallet can never leak into the short. This is the cash balance only; the open-
        margin-position equity that AR-050 adds to the full short wallet is layered on by the REST
        OpenPositions reconcile downstream (it is not carried on the balances channel)."""
        margins = [
            bal for (asset, wtype, _wid), bal in self._balances.items()
            if asset == _USD and wtype == _WALLET_MARGIN
        ]
        return sum(margins, Decimal(0)) if margins else None

    def balance(self, asset: str, wallet_type: str, wallet_id: str = _WALLET_MAIN) -> Decimal | None:
        """A general exact-key read of any cached (asset, wallet-type, wallet-id) USD/asset balance, or
        None. The spot_main_usd / margin_usd reads are the WS-BAL-002 named cases of this."""
        return self._balances.get((str(asset), str(wallet_type), str(wallet_id)))

    # --- reconnect (WS-REC-004 - the cache is reseeded from the fresh snapshot) ----
    def reset(self) -> None:
        """Drop the cached wallet state on reconnect (the fresh balances SNAPSHOT after re-subscribe
        re-seeds it, WS-REC-004 / WS-BAL-003), so a stale pre-disconnect balance is never read."""
        self._balances.clear()
