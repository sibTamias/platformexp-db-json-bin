#!/usr/bin/env bash
# refresh_db_json_baseline.sh — разовая подстановка полного db.json + артефактов эпох.
#
# Использование на сервере сайта (201/254):
#   ./refresh_db_json_baseline.sh mno@161.97.96.43
#   ./refresh_db_json_baseline.sh /path/to/db.json   # локальный файл
#
# Копирует с platformExp (или из файла):
#   ~/db.json
#   ~/tmp/withdrawals/tmp/epoch_bounds.txt
#   ~/tmp/withdrawals/epoch_intervals.txt
#   ~/tmp/withdrawals/epoch_blocks_count_L1.txt
#   ~/tmp/withdrawals/epoch_blocks_count_L2.txt
#   ~/tmp/withdrawals/all_validators_list.txt (если есть)
set -euo pipefail

REMOTE_USER="${REMOTE_USER:-mno}"
REMOTE_HOST="${REMOTE_HOST:-161.97.96.43}"
REMOTE_SAVE="${REMOTE_SAVE:-/home/mno/tmp/withdrawals}"
LOCAL_DB="${DB_JSON:-$HOME/db.json}"
LOCAL_SAVE="${SAVE_DIR:-$HOME/tmp/withdrawals}"

usage() {
    cat <<'USAGE'
Использование:
  refresh_db_json_baseline.sh [user@host]
  refresh_db_json_baseline.sh /path/to/db.json

  Без аргументов — SCP с mno@161.97.96.43 (REMOTE_HOST).
USAGE
    exit 1
}

copy_from_remote() {
    local host="$1"
    mkdir -p "$LOCAL_SAVE/tmp" "$(dirname "$LOCAL_DB")"
    echo "=== baseline с $host ==="
    scp "${host}:${REMOTE_SAVE}/db.json" "${LOCAL_DB}.tmp"
    mv -f "${LOCAL_DB}.tmp" "$LOCAL_DB"
    mkdir -p "$LOCAL_SAVE/tmp"
    scp "${host}:${REMOTE_SAVE}/tmp/epoch_bounds.txt" "${LOCAL_SAVE}/tmp/epoch_bounds.txt.tmp" 2>/dev/null \
        && mv -f "${LOCAL_SAVE}/tmp/epoch_bounds.txt.tmp" "${LOCAL_SAVE}/tmp/epoch_bounds.txt" \
        && echo "  OK tmp/epoch_bounds.txt" || echo "  skip tmp/epoch_bounds.txt"
    for f in epoch_intervals.txt epoch_blocks_count_L1.txt epoch_blocks_count_L2.txt all_validators_list.txt; do
        if scp "${host}:${REMOTE_SAVE}/${f}" "${LOCAL_SAVE}/${f}.tmp" 2>/dev/null; then
            mv -f "${LOCAL_SAVE}/${f}.tmp" "${LOCAL_SAVE}/${f}"
            echo "  OK $f"
        else
            echo "  skip $f (нет на источнике)"
        fi
    done
    echo "Готово: $LOCAL_DB + артефакты в $LOCAL_SAVE"
}

copy_local_file() {
    local src="$1"
    [[ -f "$src" ]] || { echo "Нет файла: $src" >&2; exit 1; }
    mkdir -p "$(dirname "$LOCAL_DB")"
    cp -p "$src" "$LOCAL_DB"
    echo "Скопирован $src -> $LOCAL_DB"
    echo "Артефакты epoch_* скопируйте отдельно или запустите refresh с user@host."
}

[[ $# -le 1 ]] || usage

if [[ $# -eq 0 ]]; then
    copy_from_remote "${REMOTE_USER}@${REMOTE_HOST}"
elif [[ -f "$1" ]]; then
    copy_local_file "$1"
elif [[ "$1" == *@* ]]; then
    copy_from_remote "$1"
else
    usage
fi
