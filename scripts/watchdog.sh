#!/usr/bin/env bash
# watchdog.sh — kill orphaned sleep processes that block auto-execute.sh pipeline.
# Run in background while auto-execute.sh is running.
# The custom timeout() bash function spawns `sleep N` with the pipe write-end open;
# once the claude session completes, that sleep must be killed so tee gets EOF.
set -euo pipefail

LOG="logs/watchdog.log"
mkdir -p logs

echo "[$(date '+%H:%M:%S')] Watchdog started (PID $$)" | tee -a "$LOG"

while true; do
  # Find every tee writing to a sessions log
  TEE_PIDS=$(pgrep -f "tee.*logs/sessions/" 2>/dev/null || true)

  for tee_pid in $TEE_PIDS; do
    # Get the "other end" node that tee's stdin pipe points to (the write end).
    # lsof shows stdin as: FD=0 TYPE=PIPE NODE=<read-end> NAME=-><write-end>
    # We need the write-end node (NAME field, strip leading "->").
    write_node=$(lsof -p "$tee_pid" 2>/dev/null | awk '$5=="PIPE" && $4=="0" {n=$NF; sub(/^->/, "", n); print n}' | head -1)
    [ -z "$write_node" ] && continue

    # Find processes whose stdout/stderr IS that write-end node (they hold it open).
    holders=$(lsof 2>/dev/null | awk -v node="$write_node" '$7==node && ($4=="1" || $4=="2") {print $2, $1}' | grep -v "^$tee_pid " | sort -u || true)

    while IFS= read -r line; do
      [ -z "$line" ] && continue
      holder_pid=$(echo "$line" | awk '{print $1}')
      holder_cmd=$(echo "$line" | awk '{print $2}')

      # Only kill sleep processes (the killer subshell) — never kill claude or bash
      if [[ "$holder_cmd" == "sleep" ]]; then
        session_log=$(lsof -p "$tee_pid" 2>/dev/null | awk '$4~/3w/ {print $NF}' | head -1)
        echo "[$(date '+%H:%M:%S')] Killing stale sleep $holder_pid blocking tee $tee_pid (${session_log##*/})" | tee -a "$LOG"
        kill "$holder_pid" 2>/dev/null || true
      fi
    done <<< "$holders"
  done

  sleep 15
done
