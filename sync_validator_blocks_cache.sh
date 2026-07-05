#!/usr/bin/env bash
# sync_validator_blocks_cache.sh — rsync/scp cache/validators с platformExp на сервер сайта (254).
#
# Копирует ~/tmp/withdrawals/cache/validators/{proTx}_all_blocks.txt и *_info.txt
# (списки высот блоков, proposed каждым валидатором — см. get_validator_blocks в generate_db_json_local.sh).
#
#   ./sync_validator_blocks_cache.sh mno@161.97.96.43
#   ./sync_validator_blocks_cache.sh --dry-run mno@161.97.96.43
set -euo pipefail

REMOTE_USER="${REMOTE_USER:-mno}"
REMOTE_HOST="${REMOTE_HOST:-161.97.96.43}"
REMOTE_SAVE="${REMOTE_SAVE:-/home/mno/tmp/withdrawals}"
LOCAL_SAVE="${SAVE_DIR:-$HOME/tmp/withdrawals}"
DRY_RUN=0

usage() {
    cat <<'USAGE'
Использование: sync_validator_blocks_cache.sh [опции] [user@host]

  user@host     по умолчанию mno@161.97.96.43
  --dry-run     только показать, что будет скопировано

Переменные: SAVE_DIR, REMOTE_SAVE
USAGE
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        -h|--help) usage ;;
        *@*) REMOTE_USER="${1%@*}"; REMOTE_HOST="${1#*@}"; shift ;;
        *) echo "Неизвестный аргумент: $1" >&2; exit 1 ;;
    esac
done

REMOTE="${REMOTE_USER}@${REMOTE_HOST}"
LOCAL_DIR="$LOCAL_SAVE/cache/validators"
REMOTE_DIR="$REMOTE_SAVE/cache/validators"
mkdir -p "$LOCAL_DIR"

echo "=== sync validator blocks cache: $REMOTE:$REMOTE_DIR -> $LOCAL_DIR ==="

if command -v rsync >/dev/null 2>&1; then
    RSYNC_OPTS=(-a --include='*/' --include='*_all_blocks.txt' --include='*_info.txt' --exclude='*')
    if [[ "$DRY_RUN" -eq 1 ]]; then
        rsync "${RSYNC_OPTS[@]}" --dry-run --itemize-changes "$REMOTE:$REMOTE_DIR/" "$LOCAL_DIR/"
    else
        rsync "${RSYNC_OPTS[@]}" "$REMOTE:$REMOTE_DIR/" "$LOCAL_DIR/"
    fi
else
    echo "rsync не найден — tar+ssh"
    if [[ "$DRY_RUN" -eq 1 ]]; then
        ssh "$REMOTE" "find '$REMOTE_DIR' -maxdepth 1 -type f \\( -name '*_all_blocks.txt' -o -name '*_info.txt' \\) | wc -l"
        exit 0
    fi
    ssh "$REMOTE" "tar -C '$REMOTE_DIR' -cf - --wildcards '*_all_blocks.txt' '*_info.txt'" \
        | tar -C "$LOCAL_DIR" -xf -
fi

blocks_n=$(find "$LOCAL_DIR" -maxdepth 1 -name '*_all_blocks.txt' 2>/dev/null | wc -l | tr -d '[:space:]')
echo "Готово: $blocks_n файлов *_all_blocks.txt в $LOCAL_DIR"
