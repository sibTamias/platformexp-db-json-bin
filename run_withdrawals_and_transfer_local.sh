#!/bin/bash
# run_withdrawals_and_transfer_local.sh — cron на platformExp (96.43).
#
# Всегда patch поверх готового db.json (--incremental-v1):
#   identityBalance, rating, blocks/withdrawal текущей эпохи.
# Полный rebuild_arrays / generate_json_db не вызывается.
#
# Переменные (.env): SAVE_DIR, PLATFORM_EXPLORER_URL, SKIP_TRANSFER
#
# Исключения:
#   --test [N]  — передать в generate_db_json_local.sh как есть
#   DB_JSON_FORCE_FULL=1 — только для ручного аварийного полного прогона
#
export TZ=Asia/Irkutsk

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"

SAVE_DIR="${SAVE_DIR:-/home/mno/tmp/withdrawals}"
DB_JSON="${DB_JSON:-$SAVE_DIR/db.json}"
LOG_FILE="${LOG_FILE:-/home/mno/tmp/cron.log}"

start_time=$(date +%s)
trap 's=$(($(date +%s) - start_time)); m=$((s / 60)); r=$((s % 60)); echo "[$(date +"%a %d %b %Y %H:%M:%S %Z")] Execution time: ${m} min ${r} sec (wrapper)" >> "$LOG_FILE"' EXIT

log() {
    echo "[$(TZ=Asia/Irkutsk date +'%a %d %b %Y %H:%M:%S IRKT')] $*" | tee -a "$LOG_FILE"
}

{
    echo ""
    echo "========== run_withdrawals_and_transfer_local.sh $* =========="
} >> "$LOG_FILE"

log "Starting run_withdrawals_and_transfer_local.sh $*"

if [[ ! -f "$DB_JSON" ]]; then
    log "ERROR: нет baseline db.json ($DB_JSON) — положите готовый файл, полный прогон отключён"
    exit 1
fi

gen_args=()
if [[ "${DB_JSON_FORCE_FULL:-0}" == "1" ]]; then
    log "WARN: DB_JSON_FORCE_FULL=1 — ручной полный прогон"
    gen_args=("$@")
elif [[ $# -gt 0 ]]; then
    case "$1" in
        --test*)
            log "Режим: --test (явные аргументы)"
            gen_args=("$@")
            ;;
        --incremental-v1)
            log "Режим: --incremental-v1 (явно)"
            gen_args=("$@")
            ;;
        *)
            log "Режим: --incremental-v1 + extra args: $*"
            gen_args=(--incremental-v1 "$@")
            ;;
    esac
else
    log "Режим: --incremental-v1 (patch готового db.json)"
    gen_args=(--incremental-v1)
fi

t_gen=$(date +%s)
log "Running generate_db_json_local.sh ${gen_args[*]}"
if "$BIN/generate_db_json_local.sh" "${gen_args[@]}" >> "$LOG_FILE" 2>&1; then
    log "db.json OK (generate $(( $(date +%s) - t_gen )) sec)"
else
    log "ERROR: generate_db_json_local.sh failed"
    exit 1
fi

if [[ "${SKIP_TRANSFER:-0}" == "1" ]]; then
    log "SKIP_TRANSFER=1, transfer skipped"
else
    t_tr=$(date +%s)
    log "Running transfer_db.sh"
    if "$BIN/transfer_db.sh" >> "$LOG_FILE" 2>&1; then
        log "transfer_db.sh OK ($(( $(date +%s) - t_tr )) sec)"
    else
        log "ERROR: transfer_db.sh failed"
        exit 2
    fi
fi

log "All scripts completed"
