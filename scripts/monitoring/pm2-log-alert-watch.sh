#!/usr/bin/env bash
set -euo pipefail

APP_NAME="${APP_NAME:-streamtip-api}"
LOG_FILE="${LOG_FILE:-/home/ubuntu/.pm2/logs/${APP_NAME}-error.log}"
BOT_TOKEN="${MONITORING_TELEGRAM_BOT_TOKEN:-${TELEGRAM_BOT_TOKEN:-}}"
CHAT_ID="${MONITORING_ALERT_CHAT_ID:-${TELEGRAM_WITHDRAWAL_CHAT_ID:-}}"
COOLDOWN_SECONDS="${MONITORING_ALERT_COOLDOWN_SECONDS:-300}"

if [[ -z "${BOT_TOKEN}" || -z "${CHAT_ID}" ]]; then
  echo "[pm2-log-alert-watch] Missing BOT token/chat id env. Set MONITORING_TELEGRAM_BOT_TOKEN and MONITORING_ALERT_CHAT_ID."
  exit 1
fi

if [[ ! -f "${LOG_FILE}" ]]; then
  echo "[pm2-log-alert-watch] Log file not found: ${LOG_FILE}"
  exit 1
fi

declare -A LAST_SENT_EPOCH

send_alert() {
  local key="$1"
  local text="$2"
  local now
  now="$(date +%s)"
  local last="${LAST_SENT_EPOCH[$key]:-0}"
  local elapsed=$(( now - last ))
  if (( elapsed < COOLDOWN_SECONDS )); then
    return
  fi

  LAST_SENT_EPOCH["$key"]="$now"
  local payload
  payload="$(printf 'StreamTip log alert\n\nType: %s\nTime: %s\n\n%s' "$key" "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$text")"

  curl -sS -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
    --data-urlencode "chat_id=${CHAT_ID}" \
    --data-urlencode "text=${payload}" \
    --data-urlencode "disable_web_page_preview=true" \
    >/dev/null || true
}

echo "[pm2-log-alert-watch] Watching ${LOG_FILE}"
tail -Fn0 "${LOG_FILE}" | while IFS= read -r line; do
  case "${line}" in
    *"socket.redis.pub.error"*|*"socket.redis.sub.error"*)
      send_alert "socket.redis.error" "${line}"
      ;;
    *"webhook.queue.monnify.failed"*)
      send_alert "webhook.queue.failed" "${line}"
      ;;
    *"monitor.event"*'"eventType":"webhook.queue.lag"'*)
      send_alert "webhook.queue.lag" "${line}"
      ;;
    *"monitor.event"*'"eventType":"mongo.slow_query"'*)
      send_alert "mongo.slow_query" "${line}"
      ;;
  esac
done
