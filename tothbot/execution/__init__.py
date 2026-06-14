"""execution - order entry/exit, position mirror, and the paper/live seam.

Skeleton anchor; populated in session S5. Houses, per the 0500000 dv1_240
organism decomposition (sections 4, 7, 12):
  entry dispatch        - marketable-IOC entry (CR-03) + MPP slippage cap
                          (AR-069); no GTD window, no partial-fill handling
  mod:Exit_Controller   - owns all normal exits:
                            Layer 1a regime-reversal exit = THE take-profit
                              (run to reversal; no fixed TP, no max-hold)
                            Layer 2 MAE threshold breach (1.5x ATR)
                            Layer 3 Kraken resting emergency SL (off-book;
                              fires only on VPS/TothBot/internet failure)
  mod:Position_Mirror   - position state (WS_Manager is sole writer, HR-PM-009)
  paper/live dispatch   - the four PA-004 divergence points resolve here

DIAGRAMS GOVERN: implement strictly from the 0500000 figures. This package
partition is provisional and may be refined as each figure is read.
"""
