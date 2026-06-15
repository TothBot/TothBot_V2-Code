"""regime - regime taxonomy and the SSS signal model.

Built in session TB00723 (Path A) strictly from the 0500000 dv1_242 figures (sec 5 Image4 +
sec 3 Image2), all Decimal (rule:HR-REGIME-006 / HR-SP-008):
  indicators.py - mod:Regime_Engine daily indicators: ADX(14) Wilder DMI (RE-008), EMA20/50
                  (RE-009), ATR(14) + 50-day percentile rank (RE-010/RE-012).
  taxonomy.py   - the six-regime 3x2 grid tokens + their per-cell G3/G6 entry policy
                  (LONG-block / SHORT-route cascade per HR-REGIME-007; HR-REGIME-008 block).
  engine.py     - compute_regime: the per-pair daily classifier core (ar:AR-017 response[-1]
                  exclusion; the EMA50-bound candle minimum) -> asset_regime / market_regime.
  sss.py        - the SSS Signal Engine (ar:AR-067): RSI(14) Wilder (AR-076), EMA9/EMA21,
                  VolumeMA20, and the direction-symmetric three-factor PASS rule.

PURE units only: the daily-compute orchestrator (REST GetOHLCData fetch, 00:00 UTC scheduler,
AR-036 1.1s stagger, pre-comp cache) and the 8-gate Signal_Pipeline orchestration are the I/O
edges, wired in their own slices. DIAGRAMS GOVERN: these implement the figures verbatim.
"""
