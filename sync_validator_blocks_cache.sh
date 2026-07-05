#!/usr/bin/env bash
# sync_validator_blocks_cache.sh — cache/validators между platformExp и сервером сайта (254).
#
# Pull (на 254, с 96.43):
#   ./sync_validator_blocks_cache.sh mno@161.97.96.43
#
# Push (на 96.43, если с 254 ssh/rsync refused):
#   ./sync_validator_blocks_cache.sh --push mno@161.97.100.254
#
# Файлы: {proTx}_all_blocks.txt, {proTx}_info.txt
set -euo pipefail

REMOTE_USER="${REMOTE_USER:-mno}"
REMOTE_HOST="${REMOTE_HOST:-161.97.96.43}"
REMOTE_SAVE="${REMOTE_SAVE:-/home/mno/tmp/withdrawals}"
LOCAL_SAVE="${SAVE_DIR:-$HOME/tmp/withdrawals}"
DRY_RUN=0
PUSH_MODE=0
PUSH_TARGET=""
RETRIES="${SYNC_CACHE_RETRIES:-3}"
RETRY_SLEEP="${SYNC_CACHE_RETRY_SLEEP:-5}"

usage() {
    cat <<'USAGE'
Использование:
  sync_validator_blocks_cache.sh [опции] [user@host]

  user@host          источник (pull) или цель (--push); по умолчанию mno@161.97.96.43
  --push user@host  залить cache С ЭТОГО сервера на user@host (удобно с 96.43 на 254)
  --dry-run         только показать
  --retries N       попыток (по умолчанию 3)

Переменные: SAVE_DIR, REMOTE_SAVE, SYNC_CACHE_RETRIES, SYNC_CACHE_RETRY_SLEEP
USAGE
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --push) PUSH_MODE=1; PUSH_TARGET="${2:-}"; [[ -n "$PUSH_TARGET" ]] || { echo "--push: нужен user@host" >&2; exit 1; }; shift 2 ;;
        --retries)
            RETRIES="${2:-3}"
            [[ "$RETRIES" =~ ^[0-9]+$ ]] || { echo "--retries: число" >&2; exit 1; }
            shift 2
            ;;
        -h|--help) usage ;;
        *@*)
            if [[ "$PUSH_MODE" -eq 1 && -z "$PUSH_TARGET" ]]; then
                PUSH_TARGET="$1"
            else
                REMOTE_USER="${1%@*}"
                REMOTE_HOST="${1#*@}"
            fi
            shift
            ;;
        *) echo "Неизвестный аргумент: $1" >&2; exit 1 ;;
    esac
done

LOCAL_DIR="$LOCAL_SAVE/cache/validators"
REMOTE_DIR="$REMOTE_SAVE/cache/validators"
mkdir -p "$LOCAL_DIR"

rsync_cache() {
    local src="$1" dst="$2"
    local -a opts=(-a --include='*/' --include='*_all_blocks.txt' --include='*_info.txt' --exclude='*')
    if [[ "$DRY_RUN" -eq 1 ]]; then
        rsync "${opts[@]}" --dry-run --itemize-changes "$src" "$dst"
    else
        rsync "${opts[@]}" "$src" "$dst"
    fi
}

tar_pull() {
    local remote="$1"
    ssh "$remote" "tar -C '$REMOTE_DIR' -cf - --wildcards '*_all_blocks.txt' '*_info.txt' 2>/dev/null" \
        | tar -C "$LOCAL_DIR" -xf -
}

tar_push() {
    local target="$1"
    ssh "$target" "mkdir -p '$REMOTE_DIR'"
    tar -C "$LOCAL_DIR" -cf - --wildcards '*_all_blocks.txt' '*_info.txt' 2>/dev/null \
        | ssh "$target" "tar -C '$REMOTE_DIR' -xf -"
}

try_pull() {
    local remote="${REMOTE_USER}@${REMOTE_HOST}"
    local attempt=1
    while (( attempt <= RETRIES )); do
        echo "=== pull cache: $remote:$REMOTE_DIR -> $LOCAL_DIR (попытка $attempt/$RETRIES) ==="
        if [[ "$DRY_RUN" -eq 1 ]]; then
            if command -v rsync >/dev/null 2>&1; then
                rsync_cache "$remote:$REMOTE_DIR/" "$LOCAL_DIR/" && return 0
            fi
            ssh "$remote" "find '$REMOTE_DIR' -maxdepth 1 -type f \\( -name '*_all_blocks.txt' -o -name '*_info.txt' \\) | wc -l"
            return 0
        fi
        if command -v rsync >/dev/null 2>&1; then
            if rsync_cache "$remote:$REMOTE_DIR/" "$LOCAL_DIR/"; then
                return 0
            fi
            echo "rsync failed, пробуем tar+ssh…" >&2
        else
            echo "rsync не найден — tar+ssh" >&2
        fi
        if tar_pull "$remote"; then
            return 0
        fi
        echo "WARN: pull не удался (попытка $attempt)" >&2
        (( attempt < RETRIES )) && sleep "$RETRY_SLEEP"
        attempt=$((attempt + 1))
    done
    return 1
}

try_push() {
    local target="$PUSH_TARGET"
    local attempt=1
    while (( attempt <= RETRIES )); do
        echo "=== push cache: $LOCAL_DIR -> $target:$REMOTE_DIR (попытка $attempt/$RETRIES) ==="
        if [[ "$DRY_RUN" -eq 1 ]]; then
            find "$LOCAL_DIR" -maxdepth 1 -name '*_all_blocks.txt' 2>/dev/null | wc -l
            return 0
        fi
        if command -v rsync >/dev/null 2>&1; then
            if rsync_cache "$LOCAL_DIR/" "$target:$REMOTE_DIR/"; then
                return 0
            fi
            echo "rsync push failed, пробуем tar+ssh…" >&2
        fi
        if tar_push "$target"; then
            return 0
        fi
        echo "WARN: push не удался (попытка $attempt)" >&2
        (( attempt < RETRIES )) && sleep "$RETRY_SLEEP"
        attempt=$((attempt + 1))
    done
    return 1
}

if [[ "$PUSH_MODE" -eq 1 ]]; then
    try_push
else
    try_pull
fi

blocks_n=$(find "$LOCAL_DIR" -maxdepth 1 -name '*_all_blocks.txt' 2>/dev/null | wc -l | tr -d '[:space:]')
echo "Готово: $blocks_n файлов *_all_blocks.txt в $LOCAL_DIR"
