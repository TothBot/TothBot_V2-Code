"""modules - the parallel sibling trading modules (Long, Short, future).

Skeleton anchor; populated in session S4. Houses, per the 0500000 dv1_240
organism decomposition (TB00000 section 7 PARALLEL MODULES):
  mod:Long_Module  - long-side trading module
  mod:Short_Module - short-side trading module (cornerstone; profit in either
                     market direction; must be as obvious as Long everywhere)
  module spine     - each module owns its universe, gate thresholds, signal
                     params, trading wallet, sizing function, CIATS instance +
                     statistical pool, and per-wallet drawdown halts

Modules share ONLY the single Kraken WebSocket connection at the data layer
(via mod:WS_Manager). Everything above the WS manager is module-independent;
no cross-module data contamination.

DIAGRAMS GOVERN: implement strictly from the 0500000 figures. This package
partition is provisional and may be refined as each figure is read.
"""
