"""ciats - the continuous-improvement engine (Zone 3).

Skeleton anchor; populated in session S6. Houses, per the 0500000 dv1_240
organism decomposition (sections 6, 7):
  mod:CIATS                   - the per-module improvement engine
  mod:CIATS_EWMA_Monitor      - 50-candle EWMA monitor (lambda=0.2)
  mod:CIATS_Statistical_Engine- Mann-Whitney U, Sharpe, Spearman, CUSUM, Half-Kelly
  mod:CIATS_PDCA_Engine       - plan-do-check-act improvement cycle
  mod:CIATS_Proposal_Engine   - parameter-change proposals (200-trade floor)
  mod:CIATS_Regime_Library    - per-regime parameter sets
  mod:CIATS_Parameter_Store   - the live owned-parameter store
  contract:CIATS_Trade_Outcome_Bus, contract:Parameter_Store_Snapshot

CIATS owns every operating parameter (the registry seeds are its starting
values), instantiated per module, replaced by data over paper trading.
The sacred net 1:1.5 R:R floor is the one value CIATS may never tune.

DIAGRAMS GOVERN: implement strictly from the 0500000 figures. This package
partition is provisional and may be refined as each figure is read.
"""
