#!/bin/bash
# run_withdrawals_and_transfer_local.sh — cron на platformExp (96.43).
#
# При той же эпохе: generate_db_json_local.sh --incremental-v1
#   (только identityBalance, rating, blocks/withdrawal текущей эпохи).
# При смене эпохи или отсутствии db.json: полный прогон (rebuild_arrays + generate_json_db).
#
# Переменные (.env):
#   SAVE_DIR, PLATFORM_EXPLORER_URL, SKIP_TRANSFER
#   DB_JSON_FORCE_FULL=1 — принудительно полный прогон
#
# Примеры:
#   ./run_withdrawals_and_transfer_local.sh
#   DB_JSON_FORCE_FULL=1 ./run_withdrawals_and_transfer_local.sh
#   ./run_withdrawals_and_transfer_local.sh --test 10
#
export TZ=Asia/Irkutsk

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"

SAVE_DIR="${SAVE_DIR:-/home/mno/tmp/withdrawals}"
PLATFORM_EXPLORER_URL="${PLATFORM_EXPLORER_URL:-http://127.0.0.1:3005}"
DB_JSON="${DB_JSON:-$SAVE_DIR/db.json}"
LOG_FILE="${LOG_FILE:-/home/mno/tmp/cron.log}"

start_time=$(date +%s)
trap 's=$(($(date +%s) - start_time)); m=$((s / 60)); r=$((s % 60)); echo "[$(date +"%a %d %b %Y %H:%M:%S %Z")] Execution time: ${m} min ${r} sec (total wrapper)" >> "$LOG_FILE"' EXIT

log() {
    echo "[$(TZ=Asia/Irkutsk date +'%a %d %b %Y %H:%M:%S IRKT')] $*" | tee -a "$LOG_FILE"
}

# Не затираем весь cron.log — дописываем блок прогона
{
    echo ""
    echo "========== run_withdrawals_and_transfer_local.sh $* =========="
} >> "$LOG_FILE"

log "Starting run_withdrawals_and_transfer_local.sh $*"

get_api_epoch() {
    local h
    h=$(curl -sS --max-time 20 "${PLATFORM_EXPLORER_URL%/}/status" \
        | jq -r '.epoch.number // .epochs.current // .data.epoch.number // empty')
    [[ -n "$h" && "$h" != "null" && "$h" =~ ^[0-9]+$ ]] || return 1
    echo "$h"
}

choose_generate_mode() {
    local -a user_args=("$@")
    local arg
    for arg in "${user_args[@]}"; do
        case "$arg" in
            --incremental-v1|--test*)
                echo "explicit"
                return 0
                ;;
        esac
    done

    if [[ "${DB_JSON_FORCE_FULL:-0}" == "1" ]]; then
        echo "full_forced"
        return 0
    fi

    if [[ ! -f "$DB_JSON" ]]; then
        echo "full_no_db"
        return 0
    fi

    local db_epoch api_epoch
    db_epoch=$(jq -r '.current_epoch // empty' "$DB_JSON")
    api_epoch=$(get_api_epoch) || {
        log "WARN: не удалось получить epoch из /status — полный прогон"
        echo "full_api_fail"
        return 0
    }

    if [[ -z "$db_epoch" || "$db_epoch" == "null" ]]; then
        echo "full_no_epoch_in_db"
        return 0
    fi

    if [[ "$db_epoch" == "$api_epoch" ]]; then
        echo "incremental"
    else
        log "Смена эпохи: db.json=$db_epoch, API=$api_epoch — полный прогон"
        echo "full_epoch_change"
    fi
}

mode=$(choose_generate_mode "$@")
gen_args=()

case "$mode" in
    incremental)
        log "Режим: --incremental-v1 (эпоха без изменений, patch volatile полей)"
        gen_args=(--incremental-v1)
        ;;
    explicit)
        log "Режим: аргументы переданы явно ($*)"
        gen_args=("$@")
        ;;
    full_forced)
        log "Режим: полный прогон (DB_JSON_FORCE_FULL=1)"
        ;;
    full_no_db)
        log "Режим: полный прогон (нет $DB_JSON)"
        ;;
    full_epoch_change|full_api_fail|full_no_epoch_in_db)
        log "Режим: полный прогон ($mode)"
        ;;
    *)
        log "Режим: полный прогон (default)"
        ;;
esac

if [[ "$mode" == "explicit" ]]; then
    :
elif [[ ${#gen_args[@]} -gt 0 ]]; then
    set -- "${gen_args[@]}"
else
    set --
fi

t_gen=$(date +%s)
log "Running generate_db_json_local.sh $*"
if "$BIN/generate_db_json_local.sh" "$@" >> "$LOG_FILE" 2>&1; then
    log "db.json OK (${mode}, generate $(( $(date +%s) - t_gen )) sec)"
else
    log "ERROR: generate_db_json_local.sh failed (mode=$mode)"
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

log "All scripts completed (mode=$mode)"
