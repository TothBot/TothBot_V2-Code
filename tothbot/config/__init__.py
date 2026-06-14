"""Configuration foundation: run-mode flag, parameter registry, fee constants.

This package holds the static starting state of the organism:
  settings - the single paper/live mode flag (TB00000 section 4.3)
  registry - CIATS-owned SEED values (0500000 dv1_240 / TB00000 section 8)
  fees     - Kraken fee constants

These are seeds only. CIATS owns every operating parameter and replaces
each seed with data over paper trading. The sole hardcoded, non-tunable
value is the SACRED net 1:1.5 R:R minimum.
"""
