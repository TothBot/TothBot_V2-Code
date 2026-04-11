<!--
DocDCN:     1010000
DocTitle:   Code_Repository_README
DocVersion: dv1_0
DocOwner:   Bill
DocPath:    github.com/TothBot/TothBot_V2-Code/README.md
DocDate:    04-11-2026
DocTime:    23:59:59 UTC

REVISION HISTORY

  dv1_0  04-11-2026  Document control header added per
                     BP-HDR-001. Governed by 1010000
                     Trading_System_Coding_Anchor dv1_1.
                     TB00080 corrective action. First
                     DC-compliant version. Content
                     unchanged from prior uncontrolled
                     state.
-->

# TothBot V2 -- Code Repository

Automated cryptocurrency spot trading on Kraken.
Python 3.12.3. 24/7 on Hetzner VPS. Long-only momentum.

Sacred constraint: Net 1:1.5 R:R -- HARDCODED.

## Package Structure

  tothbot/ws_manager         WS connections and order routing
  tothbot/signal_pipeline    8-gate pipeline and SSS engine
  tothbot/regime_engine      Daily regime classification
  tothbot/risk_engine        Gate 7/8, drawdown, sizing
  tothbot/exit_controller    Layer 1a/1b/2 exits
  tothbot/execution_engine   Order dispatch and GTD management
  tothbot/selection_controller  Gate 5 quality gates
  tothbot/long_module        Long-only trade logic
  tothbot/position_mirror    Position state management
  tothbot/ciats              CIATS, EWMA, Kelly, PDCA
  tothbot/logger             Async queue logger

## Governing Documents

  github.com/TothBot/TothBot_V2-Docs
