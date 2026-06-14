"""regime - regime taxonomy and the SSS signal model.

Skeleton anchor; populated in session S3. Houses, per the 0500000 dv1_240
organism decomposition (sections 5, 7):
  mod:Regime_Engine - daily regime classification (ADX / ATR percentile /
                      HTF 20-50 daily EMA); LONG-blocked / SHORT-permitted
                      logic per regime
  SSS signal model  - RSI (long 30/50; short mirror 70/50), 9/21 EMA,
                      volume vs MA(20)

DIAGRAMS GOVERN: implement strictly from the 0500000 figures. This package
partition is provisional and may be refined as each figure is read.
"""
