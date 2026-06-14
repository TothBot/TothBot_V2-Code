"""risk - the risk engine: drawdown halts, risk limits, loss-exposure sizing.

Skeleton anchor; populated in sessions S4-S5. Houses, per the 0500000
dv1_240 organism decomposition (section 7):
  mod:Risk_Engine - per-wallet drawdown monitor (session pause 5% / full halt
                    10%); Gate-7 risk limits (concentration + exposure, both
                    100%-of-wallet non-binding seeds, DEC-115); ATR-based
                    loss-exposure multipliers (mae_mult 1.5x, emergency_sl_mult
                    3.0x); balance observation (never owns the balance)

Loss prevention takes priority over profit; this engine is the primary
loss-control surface above the operator-funded per-module wallet.

DIAGRAMS GOVERN: implement strictly from the 0500000 figures. This package
partition is provisional and may be refined as each figure is read.
"""
