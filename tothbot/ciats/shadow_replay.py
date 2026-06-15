"""mod:CIATS_PDCA_Engine DO-phase REAL shadow replay - re-evaluate the Stream-2 corpus under a
candidate parameter (replaces the injected counterfactual `evaluator`).

Source: 0500000 dv1_250 sec 6/7 mod:CIATS_PDCA_Engine ("DO - shadow-evaluate the candidate against
the historical corpus -> a candidate outcome cohort") + ar:AR-065 (NET P/L) + gate:G3_Regime_Filter
(param:disallowed_regimes) + mod:Exit_Controller (the L2 mae_mult / L3 emergency_sl_mult stop legs +
the direction-symmetric net-P&L). The PDCA DO phase needs a candidate cohort to rank against the
realized baseline; this is the REAL replay that produces it from the durable Stream-2 records (the
fuller build the conductor's docstring promised in place of the seed `evaluator`).

THE REPLAY re-runs the relevant gate/exit decision on each historical evt:TRADE_CLOSE under the
candidate parameter, composing the existing units:

  GATING change  (param:disallowed_regimes - a regime added to gate:G3's block list): a record whose
                 asset_regime the candidate would BLOCK leaves the candidate cohort (the evaluator
                 returns None -> shadow_cohorts drops it); every other record keeps its realized
                 outcome. INCLUDES/EXCLUDES the trade.
  SIZING change  (param:per_trade_size_usd): net P/L is proportional to position size, so every
                 outcome SCALES by proposed/current (a larger per-trade size scales both gains and
                 losses).
  EXIT-stop change (param:mae_mult / param:emergency_sl_mult): the stop distance is proportional to
                 the multiplier, so a LOSS scales by proposed/current (a tighter stop cuts the loss
                 sooner, a looser one lets it run) while a GAIN is unchanged (the stop did not bind a
                 winner) - the mod:Exit_Controller net-P&L sign semantics.
  ENTRY-FILTER change (param:volume_sss_threshold / the rsi bounds - the SSS gate:G5 entry re-
                 simulation, TB00751 (c)): re-decide "would this trade have been ENTERED under the
                 candidate threshold?" from the record's stored signal_params LEVELS. A record whose
                 stored level no longer satisfies the moved bound is EXCLUDED (it would not have been
                 entered -> the evaluator returns None); every still-passing record keeps its realized
                 outcome. INCLUDES/EXCLUDES the trade. Only a TIGHTENING is re-decidable (a loosening
                 would ADD never-recorded trades) and ema (a level->period change) is not re-decidable
                 from a stored level - both fall to seed-then-correct below.

SEED-THEN-CORRECT (the same discipline as the historical estimators): a parameter the replay cannot
re-evaluate from the record's stored fields (e.g. an ema period change, or an entry-filter loosening
whose added trades were never recorded) falls back to the realized outcome (the candidate == the
baseline for that record), so CHECK simply finds no improvement - never a crash, never a fabricated
counterfactual. PURE, Decimal-only (ar:AR-047).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from decimal import Decimal

from ..regime.taxonomy import Regime

# The CIATS-owned tunables whose change SCALES a realized outcome (vs INCLUDES/EXCLUDES it).
SIZING_PARAMS: frozenset[str] = frozenset({"per_trade_size_usd"})
EXIT_STOP_PARAMS: frozenset[str] = frozenset({"mae_mult", "emergency_sl_mult"})
# The gating tunable the replay can re-evaluate from a record's stored asset_regime (gate:G3).
REGIME_GATING_PARAMS: frozenset[str] = frozenset({"disallowed_regimes"})

# The ENTRY-FILTER tunables the replay re-decides from a record's stored signal_params LEVELS (the
# TB00751 (c) entry re-simulation): "would this trade have been ENTERED under the candidate
# threshold?" Each maps an owned entry-filter param -> (signal_params level key, comparator) for the
# factor it gates (gate:G5 / mod:Signal_Pipeline SSS, sec 3 Image2). A record is INCLUDED (keeps its
# realized outcome) iff its stored level still satisfies the comparator against the proposed bound;
# otherwise it is EXCLUDED (it would NOT have been entered). Only single-bound TIGHTENINGS are
# re-decidable: a loosening would ADD never-recorded trades (not in the corpus), so it is not
# creditable here (seed-then-correct). The corpus holds only ENTERED trades (they passed the original
# gate), so checking just the MOVED bound is sufficient - the unchanged bound still holds.
#   SC-SSS-3 volume_sss_threshold: volume_ratio > threshold (a single floor; clean 1:1).
#   SC-SSS-1 rsi bounds: long zone rsi_long_low < rsi_14 < rsi_long_high (short the mirror) - move
#            ONE bound, re-decide against it (the faithful call: the Spearman sign picks the bound).
# ema_9 / ema_21 are signal_params LEVELS but the owned params are EMA PERIODS (sss_ema_short/long) -
# a stored level cannot re-decide a period change, so ema is NOT re-simulatable (deferred upstream).
ENTRY_FILTER_GATES: dict[str, tuple[str, str]] = {
    "volume_sss_threshold": ("volume_ratio", ">"),   # pass iff volume_ratio > threshold
    "rsi_long_low": ("rsi_14", ">"),                 # long zone low edge: pass iff rsi_14 > low
    "rsi_long_high": ("rsi_14", "<"),                # long zone high edge: pass iff rsi_14 < high
    "rsi_short_high": ("rsi_14", ">"),               # short zone low edge (50): pass iff rsi_14 > it
    "rsi_short_low": ("rsi_14", "<"),                # short zone high edge (70): pass iff rsi_14 < it
}

_ZERO = Decimal("0")


def _dec(value: object) -> Decimal:
    return value if isinstance(value, Decimal) else Decimal(str(value))


def _net_pl(record: object) -> Decimal:
    """The realized net P/L of a Stream-2 TRADE_CLOSE record (ar:AR-065, NET of fees)."""
    return _dec(record.net_pl_usd)


def _as_regimes(value: object) -> frozenset[Regime]:
    """Coerce a candidate disallowed-regime value (a Regime, a token string, or an iterable of
    either) into a frozenset[Regime]; unknown tokens are skipped (never block on a non-taxonomy
    value)."""
    items: Iterable
    if isinstance(value, (Regime, str)):
        items = (value,)
    elif isinstance(value, Iterable):
        items = value
    else:
        return frozenset()
    out: set[Regime] = set()
    for item in items:
        if isinstance(item, Regime):
            out.add(item)
            continue
        try:
            out.add(Regime(item))
        except ValueError:
            continue
    return frozenset(out)


def _record_regime(record: object) -> Regime | None:
    token = getattr(record, "asset_regime", None)
    if token is None:
        return None
    try:
        return Regime(token)
    except ValueError:
        return None


def build_shadow_evaluator(
    proposal: object, *, current_value: object = None
) -> Callable[[object], object | None]:
    """Build the PURE DO-phase counterfactual evaluator for a candidate `proposal`, to feed
    conductor.open_pdca / shadow_cohorts. evaluator(record) returns None (the candidate would not
    have traded this record - a gating exclusion) or a Decimal (the re-evaluated net outcome). The
    mode is chosen by the proposal's param_name:
      - a REGIME-GATING param blocks records whose asset_regime is in the proposed set;
      - a SIZING param scales every outcome by proposed/current;
      - an EXIT-stop param scales the LOSSES by proposed/current (gains unchanged);
      - an ENTRY-FILTER param re-decides entry from the stored signal_params LEVEL (a record failing
        the moved bound is EXCLUDED - the TB00751 (c) entry re-simulation);
      - any other param replays as the realized outcome (seed-then-correct - no counterfactual).
    `current_value` is the parameter's current (pre-change) value the scale ratio needs; it defaults
    to the proposal's own current_value when present."""
    name = str(getattr(proposal, "param_name", "") or getattr(proposal, "param", ""))
    proposed = getattr(proposal, "proposed_value", None)
    current = current_value if current_value is not None else getattr(proposal, "current_value", None)

    if name in REGIME_GATING_PARAMS:
        blocked = _as_regimes(proposed)

        def gating(record: object) -> object | None:
            regime = _record_regime(record)
            return None if (regime is not None and regime in blocked) else _net_pl(record)

        return gating

    if name in SIZING_PARAMS and current not in (None, 0) and _dec(current) != _ZERO:
        factor = _dec(proposed) / _dec(current)

        def sizing(record: object) -> object | None:
            return _net_pl(record) * factor

        return sizing

    if name in EXIT_STOP_PARAMS and current not in (None, 0) and _dec(current) != _ZERO:
        factor = _dec(proposed) / _dec(current)

        def exit_stop(record: object) -> object | None:
            pl = _net_pl(record)
            return pl * factor if pl < _ZERO else pl  # the stop binds only on a loss

        return exit_stop

    if name in ENTRY_FILTER_GATES:
        level_key, op = ENTRY_FILTER_GATES[name]
        threshold = _dec(proposed)

        def entry_filter(record: object) -> object | None:
            sp = getattr(record, "signal_params", None)
            if not isinstance(sp, dict):
                return _net_pl(record)  # no stored levels -> cannot re-decide (seed-then-correct)
            level = sp.get(level_key)
            if level is None:
                return _net_pl(record)
            lv = _dec(level)
            passes = lv > threshold if op == ">" else lv < threshold
            # INCLUDE the trade (its realized outcome) iff it still passes; else it would NOT have
            # been entered under the candidate threshold -> EXCLUDE it (shadow_cohorts drops None).
            return _net_pl(record) if passes else None

        return entry_filter

    # seed-then-correct: a parameter the record cannot re-evaluate replays as its realized outcome.
    def baseline(record: object) -> object | None:
        return _net_pl(record)

    return baseline
