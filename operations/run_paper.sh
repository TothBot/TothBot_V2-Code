#!/usr/bin/env bash
# TothBot V2 - paper smoke run (ar:AR-049 cold-start, paper mode / PA-004 div #1: no private WS, no
# credentials). Launches the organism DETACHED (nohup) so it survives the SSH session, logs every event
# to $LOG (the console telemetry tap), and writes durable evt:TRADE_CLOSE records to $TOTHBOT_RECORDS_DIR.
#
# Usage:   bash operations/run_paper.sh
# Override: TOTHBOT_UNIVERSE / TOTHBOT_RECORDS_DIR / TOTHBOT_LOG / TOTHBOT_PYTHON env vars.
# Stop:    pkill -f 'tothbot.app'        Watch: tail -f "$LOG"
set -euo pipefail

# Resolve the repo root (this script lives in <repo>/operations) and run from there so `-m tothbot.app`
# imports the package.
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

export TOTHBOT_MODE=paper
export TOTHBOT_UNIVERSE="${TOTHBOT_UNIVERSE:-BTC/USD,ETH/USD,SOL/USD}"   # small first-run universe
export TOTHBOT_RECORDS_DIR="${TOTHBOT_RECORDS_DIR:-$HOME/tothbot_records}"
LOG="${TOTHBOT_LOG:-$HOME/tothbot_paper.log}"
PY="${TOTHBOT_PYTHON:-python3}"

mkdir -p "$TOTHBOT_RECORDS_DIR"

# Single-instance guard: do not stack a second organism on the same wallet/universe.
if pgrep -f 'tothbot\.app' >/dev/null 2>&1; then
  echo "tothbot.app already running (pid $(pgrep -f 'tothbot\.app' | tr '\n' ' ')) - not starting another. Stop it first: pkill -f 'tothbot.app'"
  exit 1
fi

# Detached launch: nohup + & so it outlives the SSH session; stdout+stderr -> the log (the console tap).
nohup "$PY" -m tothbot.app > "$LOG" 2>&1 &
PID=$!
echo "tothbot paper STARTED pid=$PID"
echo "  universe = $TOTHBOT_UNIVERSE"
echo "  log      = $LOG"
echo "  records  = $TOTHBOT_RECORDS_DIR"
