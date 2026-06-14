"""exchange - Kraken event source (Zone 1) and the WS dispatch gatekeeper.

Skeleton anchor; populated in session S2 (data layer). Houses, per the
0500000 dv1_240 Image6 organism decomposition (section 7):
  container:Public_WS_v2      - 5 public push channels; ohlc_5m = system clock
  container:Private_WS_v2     - executions + balances (live only; PA-004 div #1)
  container:Kraken_REST_API   - synchronous order/account/OHLC surface
  container:Matching_Engine   - off-book L3 emergSL only (crash protection)
  mod:WS_Manager              - SOLE dispatch gatekeeper (PA-001); SOLE writer
                                to Position Mirror (HR-PM-009)
  contract:WSManager_Dispatch_Seam, contract:OHLC_5m_System_Clock,
  contract:Executions_Channel_Sequence

DIAGRAMS GOVERN: implement strictly from the 0500000 figures. This package
partition is provisional and may be refined as each figure is read.
"""
