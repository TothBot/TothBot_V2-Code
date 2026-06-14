"""TothBot V2 - automated Kraken spot trading organism.

A three-system organism operating at three time scales:
  Kraken  - event source       (5-minute OHLC candle close is the clock)
  TothBot - event processor     (this package)
  CIATS   - continuous-improvement engine

CIATS owns every operating parameter. The ONLY hardcoded constraint is
the sacred net (after-fee) 1:1.5 R:R minimum. Mission: maximize profit,
minimize loss - with loss prevention taking priority over profit.

Build provenance (Phase 2, Option B clean-slate): coded directly from the
0500000 System Architecture Overview design diagrams, dv1_240 (the clean,
verified, sole build source; md5 64f3b870d0d649e12b4dd5b572589a5f), per
TB00000 v2_97. DIAGRAMS GOVERN: every component is implemented strictly
from the 0500000 figures. Paper is the master (TB00000 section 4.3); one
codebase governs paper and live, selected by a single config flag.
"""

__version__ = "0.0.0"
