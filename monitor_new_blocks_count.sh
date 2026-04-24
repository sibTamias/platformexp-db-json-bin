#!/usr/bin/env bash
# Тест: новые Platform-блоки -> +1 счётчику по header.validator (proTxHash).
# Не вызывает generate_db_json_local.sh и не трогает db.json.
#
# Состояние: SAVE_DIR/tmp/monitor_last_block_height.txt — последняя обработанная высота (.api.block.height).
# Счётчики: SAVE_DIR/tmp/monitor_proposer_counts.json — { "protxhash": N, ... } (нижний регистр).
# Журнал: SAVE_DIR/tmp/monitor_new_blocks.log — строки height validator ts
#
# Перед продакшеном: ./check_platform_explorer_vs_dashmate.sh — explorer не должен отставать от dashmate.
#
# Запуск (на 96.43): из /home/mno/bin или с .env:
#   ./monitor_new_blocks_count.sh --once
#   ./monitor_new_blocks_count.sh --loop --interval 10
#   ./monitor_new_blocks_count.sh --reset   # выставить курсор на текущую высоту без разбора хвоста

set -euo pipefail

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"
SAVE_DIR="${SAVE_DIR:-/home/mno/tmp/withdrawals}"
PLATFORM_EXPLORER_URL="${PLATFORM_EXPLORER_URL:-http://127.0.0.1:3005}"

STATE_FILE="$SAVE_DIR/tmp/monitor_last_block_height.txt"
COUNTS_FILE="$SAVE_DIR/tmp/monitor_proposer_counts.json"
LOG_FILE="$SAVE_DIR/tmp/monitor_new_blocks.log"

MODE_ONCE=1
MODE_LOOP=0
INTERVAL=10
DRY_RUN=0
DO_RESET=0

usage() {
    cat <<'USAGE'
Использование: monitor_new_blocks_count.sh [опции]

  --once       Один проход (по умолчанию).
  --loop       Крутиться до Ctrl+C, между проходами --interval сек.
  --interval N секунд паузы в режиме --loop (по умолчанию 10).
  --dry-run    Не писать STATE и counts, только вывод в stderr.
  --reset      Записать STATE = текущая .api.block.height из /status, выйти (хвост не разбирается).

Первый запуск: сделайте --reset, иначе с last=0 запросит огромный height_min..height_max (тысячи страниц).
Нужен PLATFORM_EXPLORER_URL (см. .env). Макс. 50 страниц по 100 блоков за один --once.
USAGE
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --once) MODE_ONCE=1; MODE_LOOP=0; shift ;;
        --loop) MODE_LOOP=1; MODE_ONCE=0; shift ;;
        --interval)
            INTERVAL="${2:-10}"
            [[ "$INTERVAL" =~ ^[0-9]+$ ]] || { echo "--interval: нужно число" >&2; exit 1; }
            shift 2
            ;;
        --dry-run) DRY_RUN=1; shift ;;
        --reset) DO_RESET=1; shift ;;
        -h|--help) usage ;;
        *) echo "Неизвестный аргумент: $1 (см. $0 --help)" >&2; exit 1 ;;
    esac
done

mkdir -p "$SAVE_DIR/tmp"

api_status_height() {
    local b
    b=$(curl -sS --max-time 8 "${PLATFORM_EXPLORER_URL}/status" 2>/dev/null) || return 1
    echo "$b" | jq -r '.api.block.height // empty'
}

reset_state_to_current() {
    local h
    h=$(api_status_height) || { echo "ОШИБКА: /status" >&2; return 1; }
    [[ -n "$h" && "$h" != "null" ]] || { echo "ОШИБКА: нет .api.block.height" >&2; return 1; }
    if [[ "$DRY_RUN" -eq 0 ]]; then
        printf '%s\n' "$h" >"$STATE_FILE"
    fi
    echo "STATE -> $h (текущая высота API)"
}

read_last_height() {
    if [[ -f "$STATE_FILE" ]]; then
        tr -d '\r\n ' <"$STATE_FILE"
    else
        echo "0"
    fi
}

load_counts() {
    if [[ -f "$COUNTS_FILE" ]]; then
        jq -c '.' "$COUNTS_FILE" 2>/dev/null || echo '{}'
    else
        echo '{}'
    fi
}

save_counts() {
    local j="$1"
    if [[ "$DRY_RUN" -eq 0 ]]; then
        echo "$j" | jq . >"$COUNTS_FILE"
    fi
}

# Возвращает JSON-массив блоков {header:{height,validator},...} из одной страницы
fetch_blocks_page() {
    local h_min="$1" h_max="$2" page="$3"
    local url resp
    url="${PLATFORM_EXPLORER_URL}/blocks?height_min=${h_min}&height_max=${h_max}&limit=100&page=${page}&order=asc"
    resp=$(curl -sS --max-time 30 "$url" 2>/dev/null) || return 1
    echo "$resp" | jq -c '(.resultSet // []) | map({height: .header.height, validator: (.header.validator // "")})'
}

run_once() {
    local cur last next h_min h_max page blocks_json counts updates max_pages n ht vt gap max_fetch
    cur=$(api_status_height) || return 1
    [[ -n "$cur" && "$cur" =~ ^[0-9]+$ ]] || { echo "ОШИБКА: api.block.height=$cur" >&2; return 1; }
    if [[ "$DRY_RUN" -eq 0 && ! -f "$STATE_FILE" ]]; then
        echo "Нет курсора $STATE_FILE — один раз выполните: $0 --reset" >&2
        return 1
    fi
    last=$(read_last_height)
    [[ "$last" =~ ^[0-9]+$ ]] || last=0
    if (( last >= cur )); then
        echo "$(date -Is) нет новых блоков (last=$last cur=$cur)" >&2
        return 0
    fi
    next=$((last + 1))
    h_min=$next
    h_max=$cur
    max_pages=50
    max_fetch=$((max_pages * 100))
    gap=$((cur - last))
    if (( gap > max_fetch )); then
        echo "Слишком большой хвост: $gap блоков (лимит за проход $max_fetch). Сделайте --reset или уменьшите разрыв." >&2
        return 1
    fi
    counts=$(load_counts)
    updates=0
    page=1
    while (( page <= max_pages )); do
        blocks_json=$(fetch_blocks_page "$h_min" "$h_max" "$page") || return 1
        n=$(echo "$blocks_json" | jq 'length')
        if [[ "$n" -eq 0 ]]; then
            break
        fi
        while IFS= read -r line; do
            [[ -z "$line" ]] && continue
            ht=$(echo "$line" | jq -r '.height // empty')
            vt=$(echo "$line" | jq -r '.validator // empty' | tr '[:upper:]' '[:lower:]')
            [[ -z "$vt" ]] && continue
            counts=$(echo "$counts" | jq --arg k "$vt" '.[$k] = ((.[$k] // 0) + 1)')
            updates=$((updates + 1))
            if [[ "$DRY_RUN" -eq 0 ]]; then
                printf '%s height=%s validator=%s\n' "$(date -Is)" "$ht" "$vt" >>"$LOG_FILE"
            fi
            echo "$(date -Is) +1 proposer=$vt height=$ht" >&2
        done < <(echo "$blocks_json" | jq -c '.[]')
        (( n < 100 )) && break
        page=$((page + 1))
    done
    save_counts "$counts"
    if [[ "$DRY_RUN" -eq 0 ]]; then
        printf '%s\n' "$cur" >"$STATE_FILE"
    fi
    echo "$(date -Is) обработано блоков (событий proposer): $updates, STATE=$cur (было last=$last)" >&2
}

if [[ "$DO_RESET" -eq 1 ]]; then
    reset_state_to_current
    exit 0
fi

if [[ "$MODE_LOOP" -eq 1 ]]; then
    while true; do
        run_once || true
        sleep "$INTERVAL"
    done
else
    run_once
fi
