#!/bin/bash
# ============================================================
# DocDCN:     1011001
# DocTitle:   TothBot_Sweep
# DocVersion: dv1_0
# DocOwner:   Bill
# DocPath:    github.com/TothBot/TothBot_V2-Code/operations/tothbot_sweep.sh
# DocDate:    04-23-2026
# DocTime:    04:30:00 UTC
# ============================================================
#
# PURPOSE
# ------------------------------------------------------------
# Operator-facing VPS health-sweep report for the TothBot V2
# trading service. Reads /home/tothbot/logs/tothbot.log and
# emits a compact status summary suitable for session-open
# verification and periodic drive-by inspection.
#
# First-class controlled operational-support script per
# TB00000 v2_12 Section 3.4.2 / 3.4.3 HR-TP004 and 0311001
# v1_3 HR-DC-001. Lives in the TothBot_V2-Code operations/
# subfolder parallel to the tothbot/ Python package.
# Registered in 1010000 dv1_13 Section 5.7.
#
# Deploy via `git pull` on the VPS. Never via scp from a
# local path outside the governed repository.
#
# USAGE
# ------------------------------------------------------------
#   /root/TothBot_V2-Code/operations/tothbot_sweep.sh
#
# or via symlink:
#
#   ssh root@<vps> /root/tothbot_sweep.sh
#
# ------------------------------------------------------------
# REVISION HISTORY
# ------------------------------------------------------------
#
#   dv1_0  04-23-2026  OI-030 remediation. First controlled
#                      version. Promoted from uncontrolled
#                      /root/tothbot_sweep.sh. Two pattern
#                      defects corrected versus the
#                      uncontrolled predecessor:
#
#                      (1) Tripwire counter used
#                          '"level":"FATAL"' — no code path
#                          ever emits "level":"FATAL"; the
#                          highest level is CRITICAL. 69
#                          CRITICAL events counted 0 in the
#                          broken tripwire. Corrected to
#                          '"level":"CRITICAL"'.
#
#                      (2) Reconnect-exhaustion counter used
#                          '"event":"MAX_RECONNECT_ATTEMPTS_
#                          EXCEEDED"' — no code path emits
#                          that event name. The exhaustion
#                          event is FATAL_RECONNECT_FAILURE
#                          (ws_manager.py:3465 in dv1_22).
#                          Corrected accordingly.
#
#                      Event-name patterns derived by
#                      scanning the codebase, not paraphrased
#                      from session-log text (per TB00111 NSI
#                      Section 6 discipline and OI-037
#                      lesson). Registered in 1010000 dv1_13
#                      Section 5.7 as first entry.
# ============================================================

set -u

LOG=/home/tothbot/logs/tothbot.log

if [[ ! -f "$LOG" ]]; then
    echo "FATAL: log file not found: $LOG" >&2
    exit 1
fi

# ------------------------------------------------------------
# 1. SERVICE STATUS
# ------------------------------------------------------------
echo '===SERVICE==='
systemctl status tothbot --no-pager | head -10
echo

# ------------------------------------------------------------
# 2. TRIPWIRE COUNTERS
# Events whose non-zero count indicates operational concern.
# ------------------------------------------------------------
echo '===TRIPWIRES==='
printf '  %-40s %s\n' 'CRITICAL-level events (all components):' \
    "$(grep -c '"level":"CRITICAL"' "$LOG")"
printf '  %-40s %s\n' 'FATAL_RECONNECT_FAILURE (OI-020 / OI-028):' \
    "$(grep -c '"event":"FATAL_RECONNECT_FAILURE"' "$LOG")"
printf '  %-40s %s\n' 'WS_MGR_FATAL (OI-018):' \
    "$(grep -c '"event":"WS_MGR_FATAL"' "$LOG")"
printf '  %-40s %s\n' 'WSMGR_FATAL_PROPAGATED:' \
    "$(grep -c '"event":"WSMGR_FATAL_PROPAGATED"' "$LOG")"
printf '  %-40s %s\n' 'ALERT_SMTP_FAILED (OI-029):' \
    "$(grep -c '"event":"ALERT_SMTP_FAILED"' "$LOG")"
printf '  %-40s %s\n' 'TRADE_RECORD_WRITE_FAILED (OI-023):' \
    "$(grep -c '"event":"TRADE_RECORD_WRITE_FAILED"' "$LOG")"
printf '  %-40s %s\n' 'WARMUP_REST_FAILED:' \
    "$(grep -c '"event":"WARMUP_REST_FAILED"' "$LOG")"
echo

# ------------------------------------------------------------
# 3. RECONNECT DIAGNOSTIC COUNTERS
# Ratio of COMPLETE to ATTEMPT_FAILED is the health signal.
# ------------------------------------------------------------
echo '===RECONNECT DIAGNOSTICS==='
printf '  %-40s %s\n' 'RECONNECT_INITIATED:' \
    "$(grep -c '"event":"RECONNECT_INITIATED"' "$LOG")"
printf '  %-40s %s\n' 'RECONNECT_TRIGGERED:' \
    "$(grep -c '"event":"RECONNECT_TRIGGERED"' "$LOG")"
printf '  %-40s %s\n' 'RECONNECT_ATTEMPT_FAILED:' \
    "$(grep -c '"event":"RECONNECT_ATTEMPT_FAILED"' "$LOG")"
printf '  %-40s %s\n' 'RECONNECT_COMPLETE:' \
    "$(grep -c '"event":"RECONNECT_COMPLETE"' "$LOG")"
echo

# ------------------------------------------------------------
# 4. OBSERVABILITY / TRADE COUNTERS
# ------------------------------------------------------------
echo '===OBSERVABILITY==='
printf '  %-40s %s\n' 'ALERT_SENT (OI-019 / OI-022):' \
    "$(grep -c '"event":"ALERT_SENT"' "$LOG")"
printf '  %-40s %s\n' 'TRADE_CLOSE (OI-023):' \
    "$(grep -c '"event":"TRADE_CLOSE"' "$LOG")"
printf '  %-40s %s\n' 'PAPER_BALANCE_SET (starts):' \
    "$(grep -c '"event":"PAPER_BALANCE_SET"' "$LOG")"
printf '  %-40s %s\n' 'SYSTEM_OPERATIONAL (starts):' \
    "$(grep -c '"event":"SYSTEM_OPERATIONAL"' "$LOG")"
echo

# ------------------------------------------------------------
# 5. LATEST FATAL DETAILS
# ------------------------------------------------------------
echo '===LAST 3 FATAL_RECONNECT_FAILURE==='
grep '"event":"FATAL_RECONNECT_FAILURE"' "$LOG" | tail -3
echo

echo '===LAST 3 WS_MGR_FATAL==='
grep '"event":"WS_MGR_FATAL"' "$LOG" | tail -3
echo

# ------------------------------------------------------------
# 6. LAST 20 RECONNECT_ATTEMPT_FAILED ERROR STRINGS
# High-signal diagnostic for transient-vs-persistent failure
# classification.
# ------------------------------------------------------------
echo '===LAST 20 RECONNECT_ATTEMPT_FAILED (error)==='
grep '"event":"RECONNECT_ATTEMPT_FAILED"' "$LOG" | tail -20 \
    | grep -oE '"error":"[^"]*"'
echo

# ------------------------------------------------------------
# 7. PAPER MODE CONFIRM
# ------------------------------------------------------------
echo '===PAPER MODE CONFIRM==='
grep '"event":"PAPER_BALANCE_SET"' "$LOG" | tail -2
echo

# ------------------------------------------------------------
# 8. LOG FILE FOOTPRINT
# ------------------------------------------------------------
echo '===LOG FOOTPRINT==='
ls -la /home/tothbot/logs/ 2>/dev/null | head -10
