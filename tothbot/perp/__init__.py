"""tothbot.perp - the perpetual-futures (perps) hedge organism (GRADUATED TB00807).

Source of truth: 0500000 section 13.1-13.10 + Image10 (0500000_Image10_T2_R1_Perps_Hedge_
Organism, R1) + TB00000 v2_103 sec 8 "CIATS - perps" + the TB00806 mechanics batteries.

This package CODES the perps + hedging architecture that section 13 graduated. The binding
design discipline (section 13.9 part-vs-whole law) is RE-GROUND, do NOT rebuild: every piece
here REUSES an existing live primitive rather than adding a new module or a new gate -

  - the perp pools (mod:Long_Spot_Pool / mod:Long_Perp_Pool / mod:Short_Perp_Pool, pools.py)
    re-ground the existing mod:Long_Module / mod:Short_Module (TB00000 sec 7) onto the
    Kraken-Pro / Bitnomial perps route - same TradingModule + SyntheticCapitalLedger, not a
    new module type;
  - the isolated-margin loss cap (rule:Perp_Isolated_Margin_Loss_Cap, margin.py) is a pure
    Decimal compute, the structural loss boundary TB00806 battery C validated 7/7;
  - the funding-divergence monitor (mod:Perp_Funding_Divergence_Monitor, funding.py) is ONE
    new instance of the live ciats.EwmaMonitor, signals-only (TB00000 D5, cannot deadlock);
  - the per-module hedge breaker (param:perp_short_breaker_config, breaker.py) is REUSE of
    pipeline.risk_guard.evaluate_risk_guard's existing override params - NOT a new gate;
  - the no-same-instrument-collision rule (rule:No_Same_Instrument_Collision, collision.py).

PROPOSE-ONLY / DORMANT: this package is NOT wired into the live run_pipeline this session - it
is coded + tested + stress-tested, the HARD GATE Bill set in front of paper trading. The
deployed long-only SPOT organism is UNAFFECTED. The perps organism STAYS IN PAPER; the sacred
1:1.5 R:R floor is NEVER lowered. The real Kraken / Bitnomial maintenance-margin ratio +
per-contract multiplier are NON-PUBLIC (section 13.1 item 3) - coded as a flagged, swept
config (margin.py PerpContractSpec) until pinned from the rulebook at code time.
"""

from __future__ import annotations
