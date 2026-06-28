#!/usr/bin/env bash
# patch_db_json_live.sh — live-обновление volatile полей db.json на сервере сайта (201/254).
#
# Baseline db.json уже лежит на диске (~~/db.json). Скрипт обновляет только:
#   identityBalance, rating, blocks/withdrawal текущей эпохи.
#
# Источник: локальный platform-explorer (PLATFORM_EXPLORER_URL, по умолчанию http://localhost:3005).
# Логика — generate_db_json_local.sh --incremental-v1 (без rebuild_arrays / полной генерации).
#
# Нужны артефакты после одного полного прогона (см. DB_JSON_LIVE_BASELINE.md):
#   $SAVE_DIR/tmp/epoch_bounds.txt
#   $SAVE_DIR/epoch_intervals.txt
#   $SAVE_DIR/epoch_blocks_count_L1.txt, epoch_blocks_count_L2.txt
#   $SAVE_DIR/all_validators_list.txt (или будет собран из ключей db.json)
#
# Переменные (.env в ~/bin/.env или окружение):
#   DB_JSON=/home/mno/db.json
#   SAVE_DIR=/home/mno/tmp/withdrawals
#   PLATFORM_EXPLORER_URL=http://localhost:3005
#   Только localhost — внешний API (pshenmic.dev) не используется.
#   PARALLEL_JOBS=4
#   DB_JSON_LIVE_LOG=/home/mno/logs/db_json_live.log
#   DB_JSON_EPOCH_MISMATCH_LOG=/home/mno/logs/db_json_epoch_mismatch.log
#
# Коды выхода: 0 — ok; 1 — ошибка; 2 — смена эпохи (patch пропущен, нужен baseline refresh)
set -euo pipefail

export TZ=Asia/Irkutsk
export LC_ALL=en_US.UTF-8

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -r "${BIN_DIR:-$HOME/bin}/.env" ]] && source "${BIN_DIR:-$HOME/bin}/.env"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"

DB_JSON="${DB_JSON:-${DB_JSON_PATH:-$HOME/db.json}}"
SAVE_DIR="${SAVE_DIR:-$HOME/tmp/withdrawals}"
PLATFORM_EXPLORER_URL="${PLATFORM_EXPLORER_URL:-http://localhost:3005}"
# Жёстко localhost — без fallback на внешние API
if [[ "$PLATFORM_EXPLORER_URL" != http://localhost:* && "$PLATFORM_EXPLORER_URL" != http://127.0.0.1:* ]]; then
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] ERROR: PLATFORM_EXPLORER_URL must be localhost (got: $PLATFORM_EXPLORER_URL)" | tee -a "${DB_JSON_LIVE_LOG:-$HOME/logs/db_json_live.log}"
    exit 1
fi
LIVE_LOG="${DB_JSON_LIVE_LOG:-$HOME/logs/db_json_live.log}"
EPOCH_MISMATCH_LOG="${DB_JSON_EPOCH_MISMATCH_LOG:-$HOME/logs/db_json_epoch_mismatch.log}"
GENERATOR="${DB_JSON_GENERATOR:-$BIN/generate_db_json_local.sh}"
[[ -x "$GENERATOR" ]] || GENERATOR="${GENERATOR_ALT:-$HOME/bin/generate_db_json_local.sh}"

mkdir -p "$SAVE_DIR/tmp" "$SAVE_DIR/cache/validators" "$(dirname "$LIVE_LOG")" "$(dirname "$EPOCH_MISMATCH_LOG")"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LIVE_LOG"
}

log_epoch_mismatch() {
    local db_epoch="$1" api_epoch="$2"
    local line="[$(date +'%Y-%m-%d %H:%M:%S')] EPOCH_MISMATCH db.json=$db_epoch API=$api_epoch — patch пропущен, нужен полный baseline (scp с 96.43 или refresh_db_json_baseline.sh)"
    echo "$line" | tee -a "$EPOCH_MISMATCH_LOG" "$LIVE_LOG"
}

pe_status_epoch() {
    local base="$1" body epoch
    body=$(curl -sS --max-time 15 "${base%/}/status" 2>/dev/null) || return 1
    epoch=$(echo "$body" | jq -r '.epoch.number // .epochs.current // .data.epoch.number // empty')
    [[ -n "$epoch" && "$epoch" != "null" && "$epoch" =~ ^[0-9]+$ ]] || return 1
    echo "$epoch"
}

pick_platform_explorer_url() {
    local epoch
    if epoch=$(pe_status_epoch "$PLATFORM_EXPLORER_URL"); then
        log "platform-explorer OK: $PLATFORM_EXPLORER_URL (epoch=$epoch)"
        return 0
    fi
    log "ERROR: локальный platform-explorer недоступен ($PLATFORM_EXPLORER_URL) — нужны dashmate + PE на этом сервере"
    return 1
}

preflight() {
    [[ -f "$DB_JSON" ]] || { log "ERROR: нет baseline db.json: $DB_JSON"; exit 1; }
    [[ -f "$SAVE_DIR/tmp/epoch_bounds.txt" ]] || {
        log "ERROR: нет $SAVE_DIR/tmp/epoch_bounds.txt — см. DB_JSON_LIVE_BASELINE.md"
        exit 1
    }
    [[ -f "$SAVE_DIR/epoch_intervals.txt" ]] || {
        log "ERROR: нет $SAVE_DIR/epoch_intervals.txt"
        exit 1
    }
    [[ -f "$SAVE_DIR/epoch_blocks_count_L1.txt" && -f "$SAVE_DIR/epoch_blocks_count_L2.txt" ]] || {
        log "ERROR: нет epoch_blocks_count_L1/L2.txt в $SAVE_DIR"
        exit 1
    }
    pick_platform_explorer_url || exit 1

    if [[ -f "$BIN/check_platform_explorer_vs_dashmate.sh" ]]; then
        bash "$BIN/check_platform_explorer_vs_dashmate.sh" --warn-only 2>&1 | tee -a "$LIVE_LOG" || true
    elif [[ -f "$HOME/bin/check_platform_explorer_vs_dashmate.sh" ]]; then
        bash "$HOME/bin/check_platform_explorer_vs_dashmate.sh" --warn-only 2>&1 | tee -a "$LIVE_LOG" || true
    fi
}

check_epoch_match() {
    local api_epoch db_epoch
    api_epoch=$(pe_status_epoch "$PLATFORM_EXPLORER_URL") || { log "ERROR: /status"; exit 1; }
    db_epoch=$(jq -r '.current_epoch // empty' "$DB_JSON")
    if [[ -z "$db_epoch" || "$db_epoch" == "null" ]]; then
        log "ERROR: в db.json нет current_epoch"
        exit 1
    fi
    if [[ "$db_epoch" != "$api_epoch" ]]; then
        log_epoch_mismatch "$db_epoch" "$api_epoch"
        exit 2
    fi
}

prepare_working_copy() {
    local list_file="$SAVE_DIR/all_validators_list.txt"
    if [[ ! -s "$list_file" ]]; then
        log "Сбор all_validators_list.txt из ключей $DB_JSON"
        jq -r '.validators | keys[]' "$DB_JSON" | tr '[:upper:]' '[:lower:]' >"$list_file"
    fi
    # Рабочая копия для incremental-v1 (пишет в SAVE_DIR/db.json)
    if [[ -L "$SAVE_DIR/db.json" ]]; then
        rm -f "$SAVE_DIR/db.json"
    fi
    cp -p "$DB_JSON" "$SAVE_DIR/db.json"
}

apply_result() {
    local tmp
    tmp=$(mktemp "${DB_JSON}.tmp.XXXXXX")
    jq . "$SAVE_DIR/db.json" >"$tmp" && mv -f "$tmp" "$DB_JSON"
    log "Записан $DB_JSON ($(stat -c%s "$DB_JSON" 2>/dev/null || stat -f%z "$DB_JSON") bytes)"
}

main() {
    log "=== patch_db_json_live start ==="
    preflight
    check_epoch_match
    prepare_working_copy

    [[ -x "$GENERATOR" ]] || {
        log "ERROR: нет generate_db_json_local.sh: $GENERATOR"
        exit 1
    }

    export SAVE_DIR PLATFORM_EXPLORER_URL
    export DB_JSON_KEEP_DIAG=1
    export SKIP_EXPLORER_DASHMATE_HEIGHT_CHECK="${SKIP_EXPLORER_DASHMATE_HEIGHT_CHECK:-}"

    if ! bash "$GENERATOR" --incremental-v1 >>"$LIVE_LOG" 2>&1; then
        log "ERROR: generate_db_json_local.sh --incremental-v1 failed"
        exit 1
    fi

    apply_result
    log "=== patch_db_json_live done ==="
}

main "$@"
