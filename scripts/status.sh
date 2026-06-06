#!/usr/bin/env bash
# status.sh — Show execution progress at a glance
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROGRESS_FILE="$REPO_ROOT/docs/execution-progress.json"
SESSIONS_FILE="$REPO_ROOT/docs/sessions.json"

CYAN='\033[0;36m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
RED='\033[0;31m'; BOLD='\033[1m'; DIM='\033[2m'; RESET='\033[0m'

total=$(jq 'length' "$SESSIONS_FILE")
current_index=$(jq -r '.current_index' "$PROGRESS_FILE")
current_session=$(jq -r '.current_session' "$PROGRESS_FILE")
status=$(jq -r '.status' "$PROGRESS_FILE")
last_completed=$(jq -r '.last_completed' "$PROGRESS_FILE")
last_completed_at=$(jq -r '.last_completed_at' "$PROGRESS_FILE")
window_reset_at=$(jq -r '.window_reset_at' "$PROGRESS_FILE")
completed_count=$(jq '.sessions_completed | length' "$PROGRESS_FILE")
failed_count=$(jq '.sessions_failed | length' "$PROGRESS_FILE")

echo -e "${BOLD}AI Data Pipeline — Executor Status${RESET}"
echo -e "${DIM}────────────────────────────────────────${RESET}"
echo -e "Progress:      ${GREEN}${completed_count}${RESET} / ${total} sessions"
echo -e "Current:       ${CYAN}${current_session}${RESET} (index ${current_index})"
case $status in
  running)                  status_label="${CYAN}Running${RESET}" ;;
  waiting_for_window_reset) status_label="${YELLOW}Waiting for window reset${RESET}" ;;
  session_complete)         status_label="${GREEN}Session complete${RESET}" ;;
  not_started)              status_label="${DIM}Not started${RESET}" ;;
  *)                        status_label="$status" ;;
esac
echo -e "Status:        ${status_label}"
[[ "$last_completed" != "null" ]] && echo -e "Last done:     ${last_completed} at ${last_completed_at}"
[[ "$window_reset_at" != "null" ]] && echo -e "Window resets: ${YELLOW}${window_reset_at}${RESET}"
[[ "$failed_count" -gt 0 ]] && echo -e "Failed:        ${RED}${failed_count} session(s)${RESET}"

echo ""
echo -e "${BOLD}Session list:${RESET}"

# Print each session with status indicator
jq -r '.[] | "\(.index) \(.id) \(.name)"' "$SESSIONS_FILE" | while read -r idx sid name; do
  sentinel="$REPO_ROOT/.sessions-done/$sid"
  failed=$(jq -r --arg s "$sid" '.sessions_failed | index($s) != null' "$PROGRESS_FILE")

  if [[ -f "$sentinel" ]]; then
    echo -e "  ${GREEN}✓${RESET} ${sid}  ${DIM}${name}${RESET}"
  elif [[ "$failed" == "true" ]]; then
    echo -e "  ${RED}✗${RESET} ${sid}  ${name}"
  elif [[ "$sid" == "$current_session" ]]; then
    echo -e "  ${CYAN}▶${RESET} ${BOLD}${sid}  ${name}${RESET}  ← current"
  else
    echo -e "  ${DIM}○ ${sid}  ${name}${RESET}"
  fi
done

echo ""
echo -e "${DIM}Logs: logs/sessions/<session-id>.log${RESET}"
echo -e "${DIM}Resume: ./scripts/auto-execute.sh${RESET}"
echo -e "${DIM}Force session: ./scripts/auto-execute.sh --from <session-id>${RESET}"
