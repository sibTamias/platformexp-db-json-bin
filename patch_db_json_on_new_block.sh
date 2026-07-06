#!/usr/bin/env bash
# patch_db_json_on_new_block.sh — +1 blocks в db.json proposer'у каждого нового platform-блока.
#
# Proposer: GET /blocks?height_min=..&height_max=.. → header.validator (proTxHash, lower case).
# Патч: validators[proTx].withdrawals[] где epoch == current_epoch → blocks += 1.
#
# Состояние: SAVE_DIR/tmp/patch_db_json_last_block_height.txt
# Лог: SAVE_DIR/tmp/patch_db_json_on_new_block.log (или DB_JSON_LIVE_LOG)
#
# Первый запуск после baseline (счётчики в db.json уже верны):
#   ./patch_db_json_on_new_block.sh --reset
# Затем cron, например каждую минуту:
#   * * * * * flock -n /tmp/db-json-block.lock /home/mno/bin/patch_db_json_on_new_block.sh --once
#
# При смене эпохи (exit 2): refresh_db_json_baseline.sh + --reset
# При большом отставании: patch_db_json_live.sh (полный incremental-v1), затем --reset
set -euo pipefail

export TZ=Asia/Irkutsk

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib_db_json_blocks.sh
source "$BIN/lib_db_json_blocks.sh"
[[ -r "${BIN_DIR:-$HOME/bin}/.env" ]] && source "${BIN_DIR:-$HOME/bin}/.env"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"

SAVE_DIR="${SAVE_DIR:-$HOME/tmp/withdrawals}"
DB_JSON="${DB_JSON:-${DB_JSON_PATH:-$HOME/db.json}}"
PLATFORM_EXPLORER_URL="${PLATFORM_EXPLORER_URL:-http://127.0.0.1:3005}"
STATE_FILE="${PATCH_DB_JSON_STATE:-$SAVE_DIR/tmp/patch_db_json_last_block_height.txt}"
LOG_FILE="${PATCH_DB_JSON_LOG:-$SAVE_DIR/tmp/patch_db_json_on_new_block.log}"

MODE_ONCE=1
MODE_LOOP=0
INTERVAL=30
DRY_RUN=0
DO_RESET=0
MAX_PAGES=50

usage() {
    cat <<'USAGE'
Использование: patch_db_json_on_new_block.sh [опции]

  --once         Один проход (по умолчанию).
  --loop         Цикл до Ctrl+C, пауза --interval сек.
  --interval N   Пауза в --loop (по умолчанию 30).
  --dry-run      Не писать db.json и STATE, только stderr.
  --reset        STATE = текущая .api.block.height (хвост не разбирается).

После baseline db.json один раз: --reset. Дальше каждый новый L2-блок → +1 blocks proposer'у.

Коды выхода: 0 ok; 1 ошибка; 2 смена эпохи (нужен baseline refresh).
USAGE
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --once) MODE_ONCE=1; MODE_LOOP=0; shift ;;
        --loop) MODE_LOOP=1; MODE_ONCE=0; shift ;;
        --interval)
            INTERVAL="${2:-30}"
            [[ "$INTERVAL" =~ ^[0-9]+$ ]] || { echo "--interval: нужно число" >&2; exit 1; }
            shift 2
            ;;
        --dry-run) DRY_RUN=1; shift ;;
        --reset) DO_RESET=1; shift ;;
        -h|--help) usage ;;
        *) echo "Неизвестный аргумент: $1" >&2; exit 1 ;;
    esac
done

mkdir -p "$SAVE_DIR/tmp" "$(dirname "$LOG_FILE")"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

api_status_json() {
    curl -sS --max-time 15 "${PLATFORM_EXPLORER_URL%/}/status" 2>/dev/null
}

api_status_height() {
    api_status_json | jq -r '.api.block.height // empty'
}

api_status_epoch() {
    api_status_json | jq -r '.epoch.number // .epochs.current // empty'
}

reset_state_to_current() {
    local h
    h=$(api_status_height) || { log "ERROR: /status"; return 1; }
    [[ -n "$h" && "$h" =~ ^[0-9]+$ ]] || { log "ERROR: нет api.block.height"; return 1; }
    if [[ "$DRY_RUN" -eq 0 ]]; then
        printf '%s\n' "$h" >"$STATE_FILE"
    fi
    log "STATE -> $h (текущая высота, хвост не патчился)"
}

read_last_height() {
    if [[ -f "$STATE_FILE" ]]; then
        tr -d '\r\n ' <"$STATE_FILE"
    else
        echo "0"
    fi
}

fetch_blocks_page() {
    local h_min="$1" h_max="$2" page="$3"
    local url resp
    url="${PLATFORM_EXPLORER_URL%/}/blocks?height_min=${h_min}&height_max=${h_max}&limit=100&page=${page}&order=asc"
    resp=$(curl -sS --max-time 30 "$url" 2>/dev/null) || return 1
    echo "$resp" | jq -c '(.resultSet // []) | map({
        height: .header.height,
        validator: ((.header.validator // "") | ascii_downcase),
        timestamp: (.header.timestamp // "")
    })'
}

check_epoch_match() {
    local api_epoch db_epoch
    api_epoch=$(api_status_epoch) || { log "ERROR: epoch из /status"; return 1; }
    db_epoch=$(jq -r '.current_epoch // empty' "$DB_JSON")
    [[ -n "$db_epoch" && "$db_epoch" =~ ^[0-9]+$ ]] || { log "ERROR: нет current_epoch в $DB_JSON"; return 1; }
    if [[ "$db_epoch" != "$api_epoch" ]]; then
        log "EPOCH_MISMATCH db.json=$db_epoch API=$api_epoch — нужен refresh_db_json_baseline.sh"
        return 2
    fi
    echo "$api_epoch"
}

# Один новый блок → +1 blocks для proposer в текущей эпохе.
patch_one_block() {
    local db="$1" protx="$2" epoch="$3" height="$4"
    local tmp
    tmp=$(mktemp "${db}.patch.XXXXXX")
    if ! jq --arg h "$protx" --argjson ce "$epoch" --argjson ht "$height" '
        if .validators[$h] == null then
            .
        else
            .validators[$h].withdrawals |= (
                if any(.[]?; .epoch == $ce) then
                    map(if .epoch == $ce then {epoch: $ce, blocks: ((.blocks // 0) + 1), withdrawal: (.withdrawal // 0)} else {epoch, blocks, withdrawal} end)
                else
                    (. + [{epoch: $ce, blocks: 1, withdrawal: 0}]) | map({epoch, blocks, withdrawal}) | sort_by(.epoch)
                end
            )
        end
    ' "$db" >"$tmp"; then
        rm -f "$tmp"
        return 1
    fi
    mv -f "$tmp" "$db"
}

run_once() {
    local cur last next h_min h_max page blocks_json n gap max_fetch updates
    local ht vt cur_epoch db_epoch

    [[ -f "$DB_JSON" ]] || { log "ERROR: нет $DB_JSON"; return 1; }

    cur=$(api_status_height) || { log "ERROR: /status"; return 1; }
    [[ -n "$cur" && "$cur" =~ ^[0-9]+$ ]] || { log "ERROR: api.block.height=$cur"; return 1; }

    cur_epoch=$(check_epoch_match) || return $?

    if [[ "$DRY_RUN" -eq 0 && ! -f "$STATE_FILE" ]]; then
        log "ERROR: нет $STATE_FILE — после baseline выполните: $0 --reset"
        return 1
    fi

    last=$(read_last_height)
    [[ "$last" =~ ^[0-9]+$ ]] || last=0

    if (( last >= cur )); then
        log "нет новых блоков (last=$last cur=$cur epoch=$cur_epoch)"
        return 0
    fi

    next=$((last + 1))
    h_min=$next
    h_max=$cur
    max_fetch=$((MAX_PAGES * 100))
    gap=$((cur - last))
    if (( gap > max_fetch )); then
        log "ERROR: хвост $gap блоков > лимит $max_fetch — patch_db_json_live.sh, затем --reset"
        return 1
    fi

    updates=0
    page=1
    while (( page <= MAX_PAGES )); do
        blocks_json=$(fetch_blocks_page "$h_min" "$h_max" "$page") || return 1
        n=$(echo "$blocks_json" | jq 'length')
        [[ "$n" -eq 0 ]] && break

        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            ht=$(echo "$line" | jq -r '.height // empty')
            vt=$(echo "$line" | jq -r '.validator // empty')
            bts=$(echo "$line" | jq -r '.timestamp // empty')
            [[ -z "$vt" ]] && continue

            if [[ "$DRY_RUN" -eq 1 ]]; then
                log "DRY +1 epoch=$cur_epoch height=$ht proposer=${vt:0:16}…"
            else
                if jq -e --arg h "$vt" '.validators[$h]' "$DB_JSON" >/dev/null 2>&1; then
                    patch_one_block "$DB_JSON" "$vt" "$cur_epoch" "$ht"
                    append_validator_block_cache "$vt" "$ht" || true
                    write_last_block_meta "$vt" "$ht" "$bts" "$cur_epoch" || true
                    log "+1 epoch=$cur_epoch height=$ht proposer=${vt:0:16}…"
                else
                    log "WARN: proposer $vt не в db.json (height=$ht) — пропуск"
                fi
            fi
            updates=$((updates + 1))
        done < <(echo "$blocks_json" | jq -c '.[]')

        (( n < 100 )) && break
        page=$((page + 1))
    done

    if [[ "$updates" -gt 0 && "$DRY_RUN" -eq 0 ]]; then
        recalc_ratings_db_json "$DB_JSON" "$cur_epoch"
        log "пересчитан rating для epoch=$cur_epoch"
    fi

    if [[ "$DRY_RUN" -eq 0 ]]; then
        printf '%s\n' "$cur" >"$STATE_FILE"
    fi
    log "готово: +$updates блок(ов), STATE=$cur (было last=$last)"
}

if [[ "$DO_RESET" -eq 1 ]]; then
    reset_state_to_current
    exit 0
fi

if [[ "$MODE_LOOP" -eq 1 ]]; then
    while true; do
        run_once || rc=$?
        if [[ "${rc:-0}" -eq 2 ]]; then
            exit 2
        fi
        sleep "$INTERVAL"
    done
else
    run_once
fi
