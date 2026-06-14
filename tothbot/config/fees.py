"""Kraken fee constants (universal seed; Kraken account tier).

Source: 0500000 dv1_240 / TB00000 v2_97 section 8 (CIATS - fees).

Kraken Spot fees are straight percentages with NO flat per-trade fee, so
larger positions earn no rate discount; fewer trades cut total cost
(fewer round-trip taker tolls). The sacred net 1:1.5 R:R floor is computed
AFTER all fees: net_loss includes both taker legs (entry + exit).
"""

FEE_MAKER_PCT: float = 0.0016  # 0.16%
FEE_TAKER_PCT: float = 0.0026  # 0.26%
