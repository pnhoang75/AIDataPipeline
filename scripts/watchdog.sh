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
    # On macOS lsof PIPE lines: $6=pipe-end-address, $NF=->other-end-address.
    # tee's stdin (FD=0) reads from the read-end; its NAME shows ->write-end.
    # Strip "->" to get the write-end address.
    write_node=$(lsof -p "$tee_pid" 2>/dev/null \
      | awk '$5=="PIPE" && $4=="0" {n=$NF; sub(/^->/, "", n); print n}' \
      | head -1)
    [ -z "$write_node" ] && continue

    # Find all processes whose $6 (pipe-end address) equals write_node — they hold the write end.
    has_claude=false
    has_sleep=false
    sleep_pids=()
    while IFS= read -r line; do
      [ -z "$line" ] && continue
      h_pid=$(echo "$line" | awk '{print $1}')
      h_cmd=$(echo "$line" | awk '{print $2}')
      [[ "$h_cmd" == "sleep" ]] && { has_sleep=true; sleep_pids+=("$h_pid"); }
      # claude shows as its version number (e.g. "2.1.167") — match numeric-dot pattern
      echo "$h_cmd" | grep -qE '^[0-9]+\.[0-9]' && has_claude=true
    done < <(lsof 2>/dev/null \
      | awk -v node="$write_node" '$6==node && ($4=="1" || $4=="2") {print $2, $1}' \
      | grep -v "^$tee_pid " | sort -u || true)

    # Only kill sleep if claude has already exited from this pipe (session done).
    if $has_sleep && ! $has_claude; then
      session_log=$(lsof -p "$tee_pid" 2>/dev/null | awk '$4~/3w/ {print $NF}' | head -1)
      for sp in "${sleep_pids[@]}"; do
        echo "[$(date '+%H:%M:%S')] Killing stale sleep $sp blocking tee $tee_pid (${session_log##*/})" | tee -a "$LOG"
        kill "$sp" 2>/dev/null || true
      done
    fi
  done

  sleep 15
done
