#!/usr/bin/env bash
# =============================================================================
# sdk-auto-execute.sh — Autonomous SDK / MCP implementation driver
#
# Walks through all 24 sessions in docs/sdk-sessions.json using `claude -p`.
# When the Claude Pro usage limit is hit, sleeps and retries automatically.
# Progress is persisted to docs/sdk-execution-progress.json after every session.
#
# CAPACITY RESERVATION
#   Set SESSIONS_PER_WINDOW (env var or --sessions-per-window flag) to stop
#   voluntarily after N sessions in one reset window, leaving headroom for
#   manual Claude use on other machines.  Default: 3.
#   Set to 0 to disable the cap and run until the hard limit fires.
#
# Usage:
#   ./scripts/sdk-auto-execute.sh                          # start / resume
#   ./scripts/sdk-auto-execute.sh --from 8-A              # resume from session
#   ./scripts/sdk-auto-execute.sh --sessions-per-window 2 # tighter cap
#   ./scripts/sdk-auto-execute.sh --sessions-per-window 0 # no cap
#   ./scripts/sdk-auto-execute.sh --dry-run               # print only
#
# Prerequisites:
#   - claude CLI installed and authenticated (claude --version)
#   - jq installed
# =============================================================================
set -euo pipefail

export PATH="/Users/phuonghoang/.local/bin:$PATH"

# macOS does not ship GNU timeout.
if ! command -v timeout &>/dev/null; then
  timeout() {
    local secs="$1"; shift
    "$@" &
    local pid=$!
    (sleep "$secs" && kill "$pid" 2>/dev/null) </dev/null >/dev/null 2>&1 &
    local killer=$!
    wait "$pid" 2>/dev/null
    local rc=$?
    kill "$killer" 2>/dev/null
    wait "$killer" 2>/dev/null
    return $rc
  }
fi

# ── Paths ────────────────────────────────────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSIONS_FILE="$REPO_ROOT/docs/sdk-sessions.json"
PROGRESS_FILE="$REPO_ROOT/docs/sdk-execution-progress.json"
LOG_DIR="$REPO_ROOT/logs/sdk-sessions"
SESSIONS_PROMPT_DIR="$REPO_ROOT/scripts/sdk-sessions"

# ── Config ───────────────────────────────────────────────────────────────────
RETRY_LIMIT=10
USAGE_LIMIT_SLEEP=21600           # 6h fallback hard cap
USAGE_LIMIT_POLL=900              # probe every 15 min
MAX_TURNS=200
CLAUDE_TIMEOUT=7200               # 2h wall-clock per session

# ── Capacity reservation ──────────────────────────────────────────────────────
# Stop voluntarily after this many sessions in one reset window.
# This leaves ~10 % headroom for manual Claude use on other machines.
# Override via env var or --sessions-per-window flag.
# Set to 0 to disable (run until the hard limit fires).
SESSIONS_PER_WINDOW="${SESSIONS_PER_WINDOW:-3}"

ALLOWED_TOOLS="Bash,Read,Write,Edit,Glob,Grep,LS"

USAGE_LIMIT_PATTERNS=(
  "usage limit"
  "session limit"
  "rate limit"
  "claude pro limit"
  "you've reached your limit"
  "you've hit your"
  "exceeded.*limit"
  "too many requests"
  "quota exceeded"
  "resets.*[0-9]:[0-9][0-9]"
)

RED='\033[0;31m'; YELLOW='\033[1;33m'; GREEN='\033[0;32m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

log()  { echo -e "${CYAN}[$(date '+%H:%M:%S')]${RESET} $*"; }
ok()   { echo -e "${GREEN}[$(date '+%H:%M:%S')] ✓${RESET} $*"; }
warn() { echo -e "${YELLOW}[$(date '+%H:%M:%S')] ⚠${RESET}  $*"; }
err()  { echo -e "${RED}[$(date '+%H:%M:%S')] ✗${RESET}  $*" >&2; }
sep()  { echo -e "${BOLD}────────────────────────────────────────────────────────${RESET}"; }

# ── Argument parsing ─────────────────────────────────────────────────────────
DRY_RUN=false
FORCE_FROM=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)                DRY_RUN=true; shift ;;
    --from)                   FORCE_FROM="$2"; shift 2 ;;
    --sessions-per-window)    SESSIONS_PER_WINDOW="$2"; shift 2 ;;
    --help|-h)
      sed -n '3,16p' "$0" | sed 's/^# //; s/^#//'
      exit 0
      ;;
    *) err "Unknown argument: $1"; exit 1 ;;
  esac
done

# ── Dependency checks ─────────────────────────────────────────────────────────
check_deps() {
  local missing=()
  for cmd in claude jq git; do
    command -v "$cmd" &>/dev/null || missing+=("$cmd")
  done
  if [[ ${#missing[@]} -gt 0 ]]; then
    err "Missing required tools: ${missing[*]}"
    exit 1
  fi
}

# ── Billing safety check ──────────────────────────────────────────────────────
check_billing_safety() {
  if [[ -n "${ANTHROPIC_API_KEY:-}" ]]; then
    err "ANTHROPIC_API_KEY is set — this script is for Claude Pro subscription only."
    err "Unset it, run 'claude login', then retry."
    exit 1
  fi
  log "Billing check: Claude Pro subscription (no API key, no card charges)."
}

# ── Progress helpers ─────────────────────────────────────────────────────────
py_progress() {
  # py_progress KEY VALUE [KEY VALUE ...]
  python3 - "$PROGRESS_FILE" "$@" <<'PYEOF'
import sys, json
f = sys.argv[1]
with open(f) as fh:
    data = json.load(fh)
args = sys.argv[2:]
for i in range(0, len(args), 2):
    key, val = args[i], args[i+1]
    try:
        data[key] = int(val)
    except ValueError:
        data[key] = val
with open(f, 'w') as fh:
    json.dump(data, fh, indent=2)
PYEOF
}

progress_get() { jq -r ".$1" "$PROGRESS_FILE"; }

progress_append_completed() {
  local sid="$1"
  local tmp; tmp=$(mktemp)
  jq --arg s "$sid" '.sessions_completed += [$s]' "$PROGRESS_FILE" > "$tmp"
  mv "$tmp" "$PROGRESS_FILE"
}

progress_append_failed() {
  local sid="$1"
  local tmp; tmp=$(mktemp)
  jq --arg s "$sid" '.sessions_failed += [$s]' "$PROGRESS_FILE" > "$tmp"
  mv "$tmp" "$PROGRESS_FILE"
}

# ── Session helpers ───────────────────────────────────────────────────────────
session_count() { jq 'length' "$SESSIONS_FILE"; }
session_field() { jq -r ".[$1].$2" "$SESSIONS_FILE"; }

find_index_by_id() {
  local id="$1"
  jq -r --arg id "$id" 'to_entries[] | select(.value.id == $id) | .key' "$SESSIONS_FILE"
}

# ── Usage-limit detection ─────────────────────────────────────────────────────
is_usage_limit_error() {
  local output="$1"
  for pattern in "${USAGE_LIMIT_PATTERNS[@]}"; do
    echo "$output" | grep -qi "$pattern" && return 0
  done
  return 1
}

# ── Done-check ────────────────────────────────────────────────────────────────
run_done_check() {
  local check="$1"
  [[ -z "$check" || "$check" == "null" ]] && return 0
  (cd "$REPO_ROOT" && eval "$check" &>/dev/null)
}

# ── Prompt builder ────────────────────────────────────────────────────────────
build_prompt() {
  local index="$1"
  local session_id phase name done_check
  session_id=$(session_field "$index" "id")
  phase=$(session_field "$index" "phase")
  name=$(session_field "$index" "name")
  done_check=$(session_field "$index" "done_check")

  local override="$SESSIONS_PROMPT_DIR/${session_id}.md"
  if [[ -f "$override" ]]; then
    cat "$override"
    return
  fi

  cat <<PROMPT
You are implementing the AI Data Pipeline SDK & MCP project autonomously (session ${session_id}).

## Your task
Read docs/sdk-implementation-plan.md. Find the Sessions table inside Phase ${phase}.
Locate the row for session **${session_id}** ("${name}").
Execute the task described in the "Prompt to Claude" column completely and correctly.

## Rules for this session
1. Read ONLY the design docs and API specs referenced for this session's phase.
2. Run tests scoped to this service only — never the full suite.
3. After EVERY passing test run, make a git commit:
   git add -A && git commit -m "session ${session_id}: <what you did>"
4. When the session is fully complete, run the done-check command below, create
   the sentinel file .sessions-done/${session_id}, and make a final commit.
5. If a test fails after 5 fix attempts, add a TODO comment, commit what works,
   and create the sentinel anyway so the executor can advance.
6. Do NOT begin the next session. Stop after the sentinel is created.

## Done-check command
${done_check}

## Sentinel to create when done
mkdir -p .sessions-done && touch .sessions-done/${session_id}
git add .sessions-done/${session_id} docs/sdk-execution-progress.json
git commit -m "session ${session_id}: complete — sentinel created"
PROMPT
}

# ── Commit progress ───────────────────────────────────────────────────────────
commit_progress() {
  local message="$1"
  (cd "$REPO_ROOT" && \
    git add docs/sdk-execution-progress.json && \
    git commit -m "sdk-executor: $message" --allow-empty 2>/dev/null || true)
}

# ── Capacity reservation check ────────────────────────────────────────────────
# Returns 0 (ok to run) or 1 (cap reached — stop for this window).
check_capacity() {
  [[ "$SESSIONS_PER_WINDOW" -eq 0 ]] && return 0  # cap disabled

  local sessions_this_window
  sessions_this_window=$(progress_get "sessions_this_window")
  [[ "$sessions_this_window" == "null" ]] && sessions_this_window=0

  if [[ "$sessions_this_window" -ge "$SESSIONS_PER_WINDOW" ]]; then
    return 1
  fi
  return 0
}

# Increment sessions_this_window in the progress file.
increment_window_count() {
  local current
  current=$(progress_get "sessions_this_window")
  [[ "$current" == "null" ]] && current=0
  py_progress "sessions_this_window" "$(( current + 1 ))"
}

# Reset the per-window counter (called after a usage-limit wait completes).
reset_window_count() {
  py_progress "sessions_this_window" "0"
}

# ── Smart limit-wait ──────────────────────────────────────────────────────────
wait_for_limit_reset() {
  local output="$1"

  local time_str floor_epoch=0
  time_str=$(echo "$output" \
    | grep -oiE 'resets [0-9]+:[0-9]+(am|pm)' \
    | head -1 \
    | grep -oiE '[0-9]+:[0-9]+(am|pm)' \
    | tr '[:upper:]' '[:lower:]' || true)

  if [[ -n "$time_str" ]]; then
    local hour min ampm
    hour=$(echo "$time_str" | grep -oE '^[0-9]+')
    min=$(echo "$time_str" | grep -oE ':[0-9]+' | tr -d ':')
    ampm=$(echo "$time_str" | grep -oiE '(am|pm)$' | tr '[:upper:]' '[:lower:]')
    if [[ "$ampm" == "pm" && "$hour" -ne 12 ]]; then
      hour=$(( hour + 12 ))
    elif [[ "$ampm" == "am" && "$hour" -eq 12 ]]; then
      hour=0
    fi
    floor_epoch=$(date -j -f "%H:%M" "$(printf '%02d:%02d' "$hour" "$min")" "+%s" 2>/dev/null || echo 0)
    local now_ts; now_ts=$(date +%s)
    [[ $floor_epoch -le $now_ts ]] && floor_epoch=$(( floor_epoch + 86400 ))
    warn "Claude reports reset at ${time_str} — will probe then ($(( (floor_epoch - now_ts) / 60 ))m away)."
  else
    warn "Could not parse reset time — probing every $(( USAGE_LIMIT_POLL / 60 ))m."
  fi

  local deadline=$(( $(date +%s) + USAGE_LIMIT_SLEEP ))
  local next_probe=$(( $(date +%s) + USAGE_LIMIT_POLL ))
  [[ $floor_epoch -gt $next_probe ]] && next_probe=$floor_epoch

  py_progress "window_reset_at" \
    "$(date -u -r "$next_probe" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || \
       date -u -d "@$next_probe" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || echo 'unknown')"

  while true; do
    local now; now=$(date +%s)
    [[ $now -ge $deadline ]] && { warn "Hard cap reached — resuming."; reset_window_count; return 0; }

    local wait_secs=$(( next_probe - now ))
    if [[ $wait_secs -gt 0 ]]; then
      log "Limit active — probing in $(( wait_secs / 60 ))m $(( wait_secs % 60 ))s..."
      local remaining=$wait_secs
      while [[ $remaining -gt 0 ]]; do
        local chunk=$(( remaining < 60 ? remaining : 60 ))
        sleep "$chunk"
        remaining=$(( next_probe - $(date +%s) ))
        [[ $remaining -lt 0 ]] && remaining=0
      done
    fi

    log "Probing Claude to check if limit has reset..."
    local probe_out
    probe_out=$(timeout 60 claude --max-turns 1 -p "Reply with the single word: ready" 2>&1 || true)

    if is_usage_limit_error "$probe_out"; then
      warn "Still rate-limited. Next probe in $(( USAGE_LIMIT_POLL / 60 ))m."
      next_probe=$(( $(date +%s) + USAGE_LIMIT_POLL ))
    else
      ok "Claude limit cleared — resuming!"
      reset_window_count   # new window: reset the per-window counter
      return 0
    fi
  done
}

# ── Run one session ───────────────────────────────────────────────────────────
run_session() {
  local index="$1"
  local session_id phase name done_check
  session_id=$(session_field "$index" "id")
  phase=$(session_field "$index" "phase")
  name=$(session_field "$index" "name")
  done_check=$(session_field "$index" "done_check")

  sep
  log "${BOLD}Session ${session_id}${RESET} [phase ${phase}] — ${name}"
  log "Index ${index}/$(( $(session_count) - 1 ))"

  mkdir -p "$LOG_DIR"
  local logfile="$LOG_DIR/${session_id}.log"
  local sentinel="$REPO_ROOT/.sessions-done/${session_id}"

  if [[ -f "$sentinel" ]]; then
    ok "Session ${session_id} already complete. Skipping."
    return 0
  fi

  if $DRY_RUN; then
    warn "[DRY-RUN] Would run session ${session_id}: ${name}"
    return 0
  fi

  # ── Capacity check ────────────────────────────────────────────────────────
  if ! check_capacity; then
    warn "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    warn "CAPACITY RESERVATION: ${SESSIONS_PER_WINDOW} sessions/window limit reached."
    warn "Stopping now to leave ~10%% headroom for manual Claude use."
    warn "Remaining sessions will run in the next reset window."
    warn "Re-run this script after your Claude Pro window resets to continue."
    warn "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    py_progress "status" "capacity_reserved"
    commit_progress "capacity reserved at session ${session_id} — resuming next window"
    exit 0
  fi

  py_progress \
    "current_session" "$session_id" \
    "current_index" "$index" \
    "status" "running" \
    "last_attempt_at" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "retry_count" "0"
  commit_progress "start session ${session_id}"

  local prompt
  prompt=$(build_prompt "$index")

  local attempt=0
  while true; do
    attempt=$(( attempt + 1 ))
    pkill -f "tee.*${session_id}\.log" 2>/dev/null || true
    pkill -f "claude.*allowedTools" 2>/dev/null || true
    sleep 1

    # Pre-flight probe
    local probe_out
    probe_out=$(timeout 90 claude --max-turns 1 -p "Reply with the word ready" 2>&1 || true)
    if is_usage_limit_error "$probe_out"; then
      warn "Pre-flight: limit active before attempt ${attempt}. Waiting..."
      py_progress "status" "waiting_for_window_reset"
      wait_for_limit_reset "$probe_out"
      py_progress "status" "running"
    fi

    log "Attempt ${attempt} for session ${session_id}..."

    local tmpout; tmpout=$(mktemp)
    local exit_code=0
    local stall_limit=1200

    timeout "$CLAUDE_TIMEOUT" \
      claude --allowedTools "$ALLOWED_TOOLS" \
             --max-turns "$MAX_TURNS" \
             -p "$prompt" \
      2>&1 | tee "$logfile" > "$tmpout" &
    local pipe_pid=$!

    local captured_pipe_pid="$pipe_pid"
    (
      local deadline=$(( $(date +%s) + stall_limit ))
      while [[ $(date +%s) -lt $deadline ]]; do
        local chunk=$(( deadline - $(date +%s) ))
        [[ $chunk -gt 60 ]] && chunk=60
        [[ $chunk -le 0 ]] && break
        sleep "$chunk"
      done
      if [[ $(wc -c < "$logfile") -eq 0 ]]; then
        echo "[watchdog] No output for ${stall_limit}s — killing stalled pipeline." >&2
        kill "$captured_pipe_pid" 2>/dev/null || true
        pkill -f "claude.*allowedTools" 2>/dev/null || true
      fi
    ) &
    local watchdog_pid=$!

    wait "$pipe_pid" 2>/dev/null && exit_code=0 || exit_code=$?
    kill "$watchdog_pid" 2>/dev/null || true
    wait "$watchdog_pid" 2>/dev/null || true

    local output; output=$(cat "$tmpout"); rm -f "$tmpout"

    # Usage limit?
    if is_usage_limit_error "$output"; then
      warn "Usage limit hit on session ${session_id}."
      py_progress "status" "waiting_for_window_reset"
      commit_progress "usage limit hit — waiting for window reset"
      wait_for_limit_reset "$output"
      log "Retrying session ${session_id}..."
      continue
    fi

    # Timeout?
    if [[ $exit_code -eq 124 ]]; then
      warn "Session ${session_id} timed out."
      if [[ $attempt -ge $RETRY_LIMIT ]]; then
        err "Session ${session_id} exceeded retry limit (timeout). Marking failed."
        progress_append_failed "$session_id"
        commit_progress "session ${session_id} FAILED (timeout)"
        return 1
      fi
      warn "Retrying in 60s..."
      sleep 60
      continue
    fi

    # Other non-zero exit?
    if [[ $exit_code -ne 0 ]]; then
      warn "claude exited ${exit_code} on session ${session_id}."
      if [[ $attempt -ge $RETRY_LIMIT ]]; then
        err "Session ${session_id} failed ${RETRY_LIMIT} times. Marking failed."
        progress_append_failed "$session_id"
        commit_progress "session ${session_id} FAILED (exit ${exit_code})"
        return 1
      fi
      warn "Retrying in 30s..."
      sleep 30
      continue
    fi

    # Sentinel check
    if [[ -f "$sentinel" ]]; then
      ok "Sentinel found for session ${session_id}."
    else
      warn "Sentinel missing — running done-check..."
      if run_done_check "$done_check"; then
        ok "Done-check passed. Creating sentinel manually."
        mkdir -p "$REPO_ROOT/.sessions-done"
        touch "$sentinel"
        (cd "$REPO_ROOT" && git add .sessions-done/"${session_id}" && \
          git commit -m "sdk-executor: sentinel for ${session_id} (done-check passed)" || true)
      else
        warn "Done-check failed for ${session_id}."
        if [[ $attempt -ge $RETRY_LIMIT ]]; then
          err "Session ${session_id}: done-check still failing after ${RETRY_LIMIT} attempts."
          progress_append_failed "$session_id"
          commit_progress "session ${session_id} FAILED (done-check)"
          return 1
        fi
        warn "Retrying session..."
        continue
      fi
    fi

    # Success — update progress and increment window counter
    py_progress \
      "last_completed" "$session_id" \
      "last_completed_at" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      "status" "session_complete" \
      "current_index" "$(( index + 1 ))"
    progress_append_completed "$session_id"
    increment_window_count
    commit_progress "session ${session_id} complete"
    ok "Session ${session_id} done. ($(progress_get sessions_this_window)/${SESSIONS_PER_WINDOW} this window)"
    return 0
  done
}

# ── Main ─────────────────────────────────────────────────────────────────────
main() {
  check_deps
  check_billing_safety

  log "${BOLD}AI Data Pipeline SDK & MCP — Autonomous Executor${RESET}"
  log "Repo: $REPO_ROOT"
  log "Sessions file: $SESSIONS_FILE"
  log "Progress file: $PROGRESS_FILE"

  if [[ "$SESSIONS_PER_WINDOW" -eq 0 ]]; then
    warn "Capacity reservation: DISABLED (--sessions-per-window 0)"
  else
    log "Capacity reservation: ${SESSIONS_PER_WINDOW} sessions/window (stops to leave ~10%% headroom)"
  fi

  $DRY_RUN && warn "DRY-RUN mode"

  # ── Auto-reset window counter on fresh start ──────────────────────────────
  # If the previous run stopped via capacity reservation (not a crash or limit),
  # the user has manually re-run the script — meaning their Claude Pro window
  # has reset. Clear the per-window counter so sessions can run again.
  local prev_status
  prev_status=$(progress_get "status")
  if [[ "$prev_status" == "capacity_reserved" && "$SESSIONS_PER_WINDOW" -gt 0 ]]; then
    log "Previous run stopped for capacity reservation — resetting window counter for new window."
    reset_window_count
    py_progress "status" "resuming"
  fi

  sep

  local total; total=$(session_count)
  log "Total sessions: $total"

  local start_index=0
  if [[ -n "$FORCE_FROM" ]]; then
    start_index=$(find_index_by_id "$FORCE_FROM")
    [[ -z "$start_index" ]] && { err "Session '$FORCE_FROM' not found."; exit 1; }
    log "Forced start from $FORCE_FROM (index $start_index)"
  else
    start_index=$(progress_get "current_index")
    [[ "$start_index" == "null" || -z "$start_index" ]] && start_index=0
    log "Resuming from index $start_index (session $(session_field "$start_index" "id"))"
  fi

  local failed_sessions=()
  for (( i = start_index; i < total; i++ )); do
    if ! run_session "$i"; then
      local sid; sid=$(session_field "$i" "id")
      failed_sessions+=("$sid")
      warn "Session $sid failed — continuing to next session"
    fi
  done

  sep
  if [[ ${#failed_sessions[@]} -eq 0 ]]; then
    ok "${BOLD}All $total SDK sessions complete!${RESET}"
  else
    warn "${BOLD}Completed with ${#failed_sessions[@]} failed session(s):${RESET}"
    for s in "${failed_sessions[@]}"; do
      warn "  - $s (see logs/sdk-sessions/${s}.log)"
    done
    warn "Run: ./scripts/sdk-auto-execute.sh --from <session-id> to retry"
    exit 1
  fi
}

main "$@"
