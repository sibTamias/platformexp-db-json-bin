#!/usr/bin/env bash
# run_db_json_epoch_watch.sh — на platformExp (96.43): полный generate только при смене эпохи.
# Transfer на 201/254 не выполняется (сайты обновляют db.json локально через patch_db_json_live.sh).
#
# Cron (пример, каждые 30 мин):
#   */30 * * * * /home/mno/bin/run_db_json_epoch_watch.sh >> /home/mno/tmp/cron.log 2>&1
#
# Переменные (.env):
#   SAVE_DIR, PLATFORM_EXPLORER_URL, SKIP_TRANSFER=1 (по умолчанию 1)
#   EPOCH_STATE_FILE=$SAVE_DIR/last_epoch_for_generate.txt
set -euo pipefail

export TZ=Asia/Irkutsk

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -r "$HOME/bin/.env" ]] && source "$HOME/bin/.env"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"

SAVE_DIR="${SAVE_DIR:-/home/mno/tmp/withdrawals}"
EPOCH_STATE_FILE="${EPOCH_STATE_FILE:-$SAVE_DIR/last_epoch_for_generate.txt}"
PLATFORM_EXPLORER_URL="${PLATFORM_EXPLORER_URL:-http://localhost:3005}"
GENERATOR="${DB_JSON_GENERATOR:-$BIN/generate_db_json_local.sh}"
SKIP_TRANSFER="${SKIP_TRANSFER:-1}"

mkdir -p "$SAVE_DIR"

get_api_epoch() {
    curl -sS --max-time 20 "${PLATFORM_EXPLORER_URL%/}/status" \
        | jq -r '.epoch.number // .epochs.current // empty'
}

api_epoch=$(get_api_epoch)
[[ -n "$api_epoch" && "$api_epoch" =~ ^[0-9]+$ ]] || {
    echo "[$(date -Is)] run_db_json_epoch_watch: не удалось получить epoch из /status" >&2
    exit 1
}

last_epoch=""
[[ -f "$EPOCH_STATE_FILE" ]] && last_epoch=$(tr -d '\r\n ' <"$EPOCH_STATE_FILE")

if [[ "$last_epoch" == "$api_epoch" ]]; then
    echo "[$(date -Is)] epoch=$api_epoch без изменений — полный generate не нужен"
    exit 0
fi

echo "[$(date -Is)] смена эпохи: ${last_epoch:-none} -> $api_epoch — полный generate_db_json_local.sh"
export SAVE_DIR PLATFORM_EXPLORER_URL
bash "$GENERATOR"
echo "$api_epoch" >"$EPOCH_STATE_FILE"

if [[ "$SKIP_TRANSFER" == "1" ]]; then
    echo "[$(date -Is)] SKIP_TRANSFER=1 — transfer_db.sh не вызывается (сайты: patch_db_json_live / refresh_db_json_baseline)"
else
    transfer="${BIN}/transfer_db.sh"
    [[ -x "$transfer" ]] || transfer="$HOME/bin/transfer_db.sh"
    if [[ -x "$transfer" ]]; then
        bash "$transfer"
    else
        echo "[$(date -Is)] WARN: transfer_db.sh не найден" >&2
    fi
fi
