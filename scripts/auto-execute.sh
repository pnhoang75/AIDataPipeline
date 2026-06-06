#!/usr/bin/env bash
# =============================================================================
# auto-execute.sh — Autonomous AI Data Pipeline implementation driver
#
# Walks through all 52 sessions in docs/sessions.json using `claude -p`.
# When the Claude Pro usage limit is hit, sleeps and retries automatically.
# Progress is persisted to docs/execution-progress.json after every session.
#
# Usage:
#   ./scripts/auto-execute.sh              # start / resume from last checkpoint
#   ./scripts/auto-execute.sh --from 1-B  # resume from a specific session
#   ./scripts/auto-execute.sh --dry-run   # print what would run, don't execute
#
# Prerequisites:
#   - claude CLI installed and authenticated (claude --version)
#   - jq installed
#   - kind cluster running (for Phase 1+)
#   - Docker daemon running
# =============================================================================
set -euo pipefail

# ── Paths ────────────────────────────────────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SESSIONS_FILE="$REPO_ROOT/docs/sessions.json"
PROGRESS_FILE="$REPO_ROOT/docs/execution-progress.json"
LOG_DIR="$REPO_ROOT/logs/sessions"
SESSIONS_PROMPT_DIR="$REPO_ROOT/scripts/sessions"

# ── Config ───────────────────────────────────────────────────────────────────
RETRY_LIMIT=3                    # max retries for non-limit errors per session
USAGE_LIMIT_SLEEP=19800          # 5h30m — safely past the 5h window reset
USAGE_LIMIT_POLL=1800            # recheck every 30 min after limit hit
MAX_TURNS=80                     # max agentic turns per claude -p call
CLAUDE_TIMEOUT=7200              # 2h wall-clock timeout per session (seconds)

# Tools Claude is allowed to use without confirmation.
# This replaces --dangerously-skip-permissions with an explicit allowlist.
# These are the minimum tools needed for code implementation sessions.
# Billing is NOT affected by this — it is controlled solely by authentication
# (Claude Pro OAuth vs ANTHROPIC_API_KEY). Since no API key is set, all usage
# is covered by the Claude Pro subscription and stops at the limit.
ALLOWED_TOOLS="Bash,Read,Write,Edit,Glob,Grep,LS"

# Usage-limit error patterns (case-insensitive grep)
USAGE_LIMIT_PATTERNS=(
  "usage limit"
  "rate limit"
  "claude pro limit"
  "you've reached your limit"
  "exceeded.*limit"
  "too many requests"
  "quota exceeded"
)

# ── Colours ──────────────────────────────────────────────────────────────────
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
    --dry-run)   DRY_RUN=true; shift ;;
    --from)      FORCE_FROM="$2"; shift 2 ;;
    --help|-h)
      sed -n '3,14p' "$0" | sed 's/^# //; s/^#//'
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
    err "Install them and retry."
    exit 1
  fi
}

# ── Billing safety check ──────────────────────────────────────────────────────
# Refuse to run if ANTHROPIC_API_KEY is set. That auth mode bills per token
# to a credit card. This script is designed for Claude Pro subscription only.
check_billing_safety() {
  if [[ -n "${ANTHROPIC_API_KEY:-}" ]]; then
    err "ANTHROPIC_API_KEY is set in your environment."
    err "Running in API key mode bills per token to your credit card."
    err "This script is intended for Claude Pro subscription use only."
    err ""
    err "To use your Claude Pro plan instead:"
    err "  1. unset ANTHROPIC_API_KEY"
    err "  2. Run: claude login   (authenticates with your Claude Pro account)"
    err "  3. Re-run this script."
    exit 1
  fi
  log "Billing check: no ANTHROPIC_API_KEY set — using Claude Pro subscription (no card charges)."
}

# ── Progress helpers ─────────────────────────────────────────────────────────
progress_get() { jq -r ".$1" "$PROGRESS_FILE"; }

progress_set() {
  local key="$1" val="$2"
  local tmp
  tmp=$(mktemp)
  jq --arg k "$key" --arg v "$val" '.[$k] = $v' "$PROGRESS_FILE" > "$tmp"
  mv "$tmp" "$PROGRESS_FILE"
}

progress_update() {
  # Usage: progress_update key1 val1 key2 val2 ...
  local tmp; tmp=$(mktemp)
  local jq_args=()
  while [[ $# -ge 2 ]]; do
    jq_args+=(--arg "k$1" "$1" --arg "v$1" "$2")
    shift 2
  done
  # Build jq expression dynamically
  local expr='.'
  for key in "${!jq_args[@]}"; do : ; done
  # Simpler: pass as JSON object
  local updates="{}"
  while [[ $# -ge 2 ]] 2>/dev/null || true; do break; done
  # Use python for reliable multi-key update
  python3 - "$PROGRESS_FILE" "$@" <<'PYEOF'
import sys, json
f = sys.argv[1]
with open(f) as fh:
    data = json.load(fh)
args = sys.argv[2:]
for i in range(0, len(args), 2):
    data[args[i]] = args[i+1]
with open(f, 'w') as fh:
    json.dump(data, fh, indent=2)
PYEOF
}

progress_append_completed() {
  local session_id="$1"
  local tmp; tmp=$(mktemp)
  jq --arg s "$session_id" '.sessions_completed += [$s]' "$PROGRESS_FILE" > "$tmp"
  mv "$tmp" "$PROGRESS_FILE"
}

progress_append_failed() {
  local session_id="$1"
  local tmp; tmp=$(mktemp)
  jq --arg s "$session_id" '.sessions_failed += [$s]' "$PROGRESS_FILE" > "$tmp"
  mv "$tmp" "$PROGRESS_FILE"
}

# ── Session helpers ───────────────────────────────────────────────────────────
session_count() { jq 'length' "$SESSIONS_FILE"; }

session_field() {
  local index="$1" field="$2"
  jq -r ".[$index].$field" "$SESSIONS_FILE"
}

find_index_by_id() {
  local id="$1"
  jq -r --arg id "$id" 'to_entries[] | select(.value.id == $id) | .key' "$SESSIONS_FILE"
}

# ── Usage-limit detection ─────────────────────────────────────────────────────
is_usage_limit_error() {
  local output="$1"
  for pattern in "${USAGE_LIMIT_PATTERNS[@]}"; do
    if echo "$output" | grep -qi "$pattern"; then
      return 0
    fi
  done
  return 1
}

# ── Done-check runner ─────────────────────────────────────────────────────────
run_done_check() {
  local check="$1"
  if [[ -z "$check" || "$check" == "null" ]]; then
    return 0  # no check defined — assume success
  fi
  # Run from repo root; suppress output; return exit code
  (cd "$REPO_ROOT" && eval "$check" &>/dev/null)
}

# ── Session prompt builder ────────────────────────────────────────────────────
build_prompt() {
  local index="$1"
  local session_id phase name done_check
  session_id=$(session_field "$index" "id")
  phase=$(session_field "$index" "phase")
  name=$(session_field "$index" "name")
  done_check=$(session_field "$index" "done_check")

  # Check for a hand-crafted prompt override file
  local override="$SESSIONS_PROMPT_DIR/${session_id}.md"
  if [[ -f "$override" ]]; then
    cat "$override"
    return
  fi

  # Auto-generate from the implementation plan sessions table
  cat <<PROMPT
You are implementing the AI Data Pipeline project autonomously (session ${session_id}).

## Your task
Read docs/implementation-plan.md. Find the Sessions table inside Phase ${phase}.
Locate the row for session **${session_id}** ("${name}").
Execute the task described in the "Prompt to Claude" column completely and correctly.

## Rules for this session
1. Read ONLY the design docs referenced for this session — do not load unrelated files.
2. Run tests scoped to this service only:
   pytest <path> -x --tb=short -q
   Never run the full test suite (it overloads context).
3. After EVERY passing test run, make a git commit:
   git add -A && git commit -m "session ${session_id}: <what you did>"
4. When the session is fully complete, run the done-check command below and
   create the sentinel file .sessions-done/${session_id} then make a final commit.
5. If a test fails and you cannot fix it within 5 attempts, add a
   TODO comment in the code, commit what works, and create the sentinel anyway
   so the executor can advance. Document the failure in logs/sessions/${session_id}.log.
6. Do NOT attempt the next session's work. Stop after creating the sentinel.

## Done-check command (run this last to verify)
${done_check}

## Sentinel to create when done
mkdir -p .sessions-done && touch .sessions-done/${session_id}
git add .sessions-done/${session_id} docs/execution-progress.json
git commit -m "session ${session_id}: complete — sentinel created"
PROMPT
}

# ── Commit progress ───────────────────────────────────────────────────────────
commit_progress() {
  local message="$1"
  (cd "$REPO_ROOT" && \
    git add docs/execution-progress.json && \
    git commit -m "executor: $message" --allow-empty 2>/dev/null || true)
}

# ── Sleep with countdown ──────────────────────────────────────────────────────
sleep_with_progress() {
  local seconds="$1" reason="$2"
  local end_time=$(( $(date +%s) + seconds ))
  local interval=300  # report every 5 min

  warn "$reason"
  warn "Sleeping $(( seconds / 3600 ))h $(( (seconds % 3600) / 60 ))m — will resume at $(date -d "@$end_time" '+%Y-%m-%d %H:%M:%S' 2>/dev/null || date -r "$end_time" '+%Y-%m-%d %H:%M:%S')"

  while [[ $(date +%s) -lt $end_time ]]; do
    local remaining=$(( end_time - $(date +%s) ))
    local hours=$(( remaining / 3600 ))
    local mins=$(( (remaining % 3600) / 60 ))
    log "Resuming in ${hours}h ${mins}m..."
    sleep "$interval"
  done
}

# ── Main execution loop ───────────────────────────────────────────────────────
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

  # Already done?
  if [[ -f "$sentinel" ]]; then
    ok "Session ${session_id} already complete (sentinel exists). Skipping."
    return 0
  fi

  if $DRY_RUN; then
    warn "[DRY-RUN] Would run session ${session_id}: ${name}"
    warn "[DRY-RUN] Done-check: ${done_check}"
    return 0
  fi

  # Update progress
  python3 - "$PROGRESS_FILE" \
    "current_session" "$session_id" \
    "current_index" "$index" \
    "status" "running" \
    "last_attempt_at" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    "retry_count" "0" <<'PYEOF'
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
  commit_progress "start session ${session_id}"

  local prompt
  prompt=$(build_prompt "$index")

  local attempt=0
  while true; do
    attempt=$(( attempt + 1 ))
    log "Attempt ${attempt} for session ${session_id}..."

    local tmpout; tmpout=$(mktemp)
    local exit_code=0

    # Run claude non-interactively with a wall-clock timeout.
    # --allowedTools: explicit list of tools Claude may use without prompting.
    # This is a UX flag (no confirmation dialogs), NOT a billing flag.
    # Billing is determined by auth: Claude Pro OAuth = subscription only,
    # no API key = no credit card charges beyond the monthly subscription.
    timeout "$CLAUDE_TIMEOUT" \
      claude --allowedTools "$ALLOWED_TOOLS" \
             --max-turns "$MAX_TURNS" \
             -p "$prompt" \
      2>&1 | tee "$logfile" > "$tmpout" || exit_code=$?

    local output
    output=$(cat "$tmpout")
    rm -f "$tmpout"

    # ── Usage limit? ──────────────────────────────────────────────────────────
    if is_usage_limit_error "$output"; then
      warn "Claude Pro usage limit detected on session ${session_id}."
      python3 - "$PROGRESS_FILE" \
        "status" "waiting_for_window_reset" \
        "window_reset_at" "$(date -d "+${USAGE_LIMIT_SLEEP} seconds" -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || date -u -v +${USAGE_LIMIT_SLEEP}S +%Y-%m-%dT%H:%M:%SZ)" <<'PYEOF'
import sys, json
f = sys.argv[1]
with open(f) as fh:
    data = json.load(fh)
args = sys.argv[2:]
for i in range(0, len(args), 2):
    data[args[i]] = args[i+1]
with open(f, 'w') as fh:
    json.dump(data, fh, indent=2)
PYEOF
      commit_progress "usage limit hit — waiting for window reset"

      # Poll until the limit clears rather than sleeping a fixed duration
      sleep_with_progress "$USAGE_LIMIT_SLEEP" "Waiting for Claude Pro window to reset..."

      # After sleeping, retry the same session (don't advance index)
      log "Window should be reset. Retrying session ${session_id}..."
      continue
    fi

    # ── Timeout? ─────────────────────────────────────────────────────────────
    if [[ $exit_code -eq 124 ]]; then
      warn "Session ${session_id} timed out after ${CLAUDE_TIMEOUT}s."
      if [[ $attempt -ge $RETRY_LIMIT ]]; then
        err "Session ${session_id} exceeded retry limit. Marking as failed."
        progress_append_failed "$session_id"
        commit_progress "session ${session_id} FAILED (timeout)"
        return 1
      fi
      warn "Retrying in 60s..."
      sleep 60
      continue
    fi

    # ── Non-zero exit for other reasons? ─────────────────────────────────────
    if [[ $exit_code -ne 0 ]]; then
      warn "claude exited with code ${exit_code} on session ${session_id}."
      if [[ $attempt -ge $RETRY_LIMIT ]]; then
        err "Session ${session_id} failed ${RETRY_LIMIT} times. Marking as failed and advancing."
        progress_append_failed "$session_id"
        commit_progress "session ${session_id} FAILED (exit ${exit_code})"
        return 1
      fi
      warn "Retrying in 30s..."
      sleep 30
      continue
    fi

    # ── Check sentinel (Claude should have created it) ────────────────────────
    if [[ -f "$sentinel" ]]; then
      ok "Sentinel found for session ${session_id}."
    else
      warn "Sentinel missing for ${session_id}. Running done-check to decide..."
      if run_done_check "$done_check"; then
        ok "Done-check passed. Creating sentinel manually."
        mkdir -p "$REPO_ROOT/.sessions-done"
        touch "$sentinel"
        (cd "$REPO_ROOT" && git add .sessions-done/"${session_id}" && \
          git commit -m "executor: sentinel for ${session_id} (done-check passed)" || true)
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

    # ── Success ───────────────────────────────────────────────────────────────
    python3 - "$PROGRESS_FILE" \
      "last_completed" "$session_id" \
      "last_completed_at" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
      "status" "session_complete" \
      "current_index" "$(( index + 1 ))" <<'PYEOF'
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
    progress_append_completed "$session_id"
    commit_progress "session ${session_id} complete"
    ok "Session ${session_id} done."
    return 0
  done
}

main() {
  check_deps
  check_billing_safety

  log "${BOLD}AI Data Pipeline — Autonomous Executor${RESET}"
  log "Repo: $REPO_ROOT"
  log "Sessions file: $SESSIONS_FILE"
  log "Progress file: $PROGRESS_FILE"
  $DRY_RUN && warn "DRY-RUN mode — nothing will be executed"
  sep

  local total; total=$(session_count)
  log "Total sessions: $total"

  # Determine starting index
  local start_index=0
  if [[ -n "$FORCE_FROM" ]]; then
    start_index=$(find_index_by_id "$FORCE_FROM")
    if [[ -z "$start_index" ]]; then
      err "Session ID '$FORCE_FROM' not found in $SESSIONS_FILE"
      exit 1
    fi
    log "Forced start from session $FORCE_FROM (index $start_index)"
  else
    start_index=$(progress_get "current_index")
    if [[ "$start_index" == "null" || -z "$start_index" ]]; then
      start_index=0
    fi
    log "Resuming from index $start_index (session $(session_field "$start_index" "id"))"
  fi

  # Main loop
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
    ok "${BOLD}All $total sessions complete! Project implementation finished.${RESET}"
  else
    warn "${BOLD}Completed with ${#failed_sessions[@]} failed session(s):${RESET}"
    for s in "${failed_sessions[@]}"; do
      warn "  - $s (see logs/sessions/${s}.log)"
    done
    warn "Run: ./scripts/auto-execute.sh --from <session-id> to retry"
    exit 1
  fi
}

main "$@"
