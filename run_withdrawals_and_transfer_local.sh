#!/bin/bash
# run_withdrawals_and_transfer_local.sh
#
# Запуск формирования db.json с ЛОКАЛЬНЫМ platform-explorer (localhost:3005),
# затем отправка на целевой сервер (если нужен transfer).
#
export TZ=Asia/Irkutsk

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"

LOG_FILE="${LOG_FILE:-/home/mno/tmp/cron.log}"
> "$LOG_FILE"
start_time=$(date +%s)
trap 's=$(($(date +%s) - start_time)); m=$((s / 60)); echo "[$(date +"%a %d %b %Y %H:%M:%S %Z")] Execution time: ${m} min" >> "$LOG_FILE"' EXIT

echo "[$(TZ=Asia/Irkutsk date +'%a %d %b %Y %H:%M:%S IRKT')] Starting run_withdrawals_and_transfer_local.sh $*" >> "$LOG_FILE"

echo "[$(date +'%a %d %b %Y %H:%M:%S %Z')] Running generate_db_json_local.sh $*" >> "$LOG_FILE"
if "$BIN/generate_db_json_local.sh" "$@" >> "$LOG_FILE" 2>&1; then
    echo "[$(date +'%a %d %b %Y %H:%M:%S %Z')] db.json generated successfully" >> "$LOG_FILE"
    _inc=0
    for _a in "$@"; do [[ "$_a" == "--incremental-v1" ]] && _inc=1; done
    if [[ "$_inc" -eq 0 && "${VERSION_SCAN_SKIP:-0}" != "1" && -x "$BIN/scan_node_versions.sh" ]]; then
        echo "[$(date +'%a %d %b %Y %H:%M:%S %Z')] Running scan_node_versions.sh" >> "$LOG_FILE"
        "$BIN/scan_node_versions.sh" >> "$LOG_FILE" 2>&1 || {
            echo "[$(date +'%a %d %b %Y %H:%M:%S %Z')] WARNING: scan_node_versions.sh failed (db.json без core_version)" >> "$LOG_FILE"
        }
    fi
else
    echo "[$(date +'%a %d %b %Y %H:%M:%S %Z')] ERROR: generate_db_json_local.sh failed" >> "$LOG_FILE"
    exit 1
fi

if [[ "${SKIP_TRANSFER:-0}" == "1" ]]; then
    echo "[$(date +'%a %d %b %Y %H:%M:%S %Z')] SKIP_TRANSFER=1, transfer skipped" >> "$LOG_FILE"
else
    echo "[$(date +'%a %d %b %Y %H:%M:%S %Z')] Running transfer_db.sh" >> "$LOG_FILE"
    if "$BIN/transfer_db.sh" >> "$LOG_FILE" 2>&1; then
        echo "[$(date +'%a %d %b %Y %H:%M:%S %Z')] transfer_db.sh completed successfully" >> "$LOG_FILE"
    else
        echo "[$(date +'%a %d %b %Y %H:%M:%S %Z')] ERROR: transfer_db.sh failed" >> "$LOG_FILE"
        exit 2
    fi
fi

echo "[$(date +'%a %d %b %Y %H:%M:%S %Z')] All scripts completed" >> "$LOG_FILE"
