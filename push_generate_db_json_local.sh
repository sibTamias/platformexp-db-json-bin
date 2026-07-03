#!/usr/bin/env bash
# Репозиторий: ~/Projects/platformexp-db-json-bin
# Залить generate_db_json_local.sh (+ check) на mno@161.97.96.43 (platformExp), где генерируется db.json.
# 161.97.100.254 — GeoDashboard: готовый db.json; этот push туда не копирует.
# Если целевой файл уже есть — копия в .bak, затем scp и chmod +x.
set -euo pipefail

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOCAL="$BIN/generate_db_json_local.sh"
LOCAL_CHECK="$BIN/check_platform_explorer_vs_dashmate.sh"
REMOTE_BIN="${REMOTE_BIN:-/home/mno/bin/generate_db_json_local.sh}"
REMOTE_CHECK="${REMOTE_CHECK:-/home/mno/bin/check_platform_explorer_vs_dashmate.sh}"

usage() {
    echo "Использование: $0 user@host [user@host ...]" >&2
    echo "  Перед записью: на хосте cp \"\$REMOTE_BIN\" \"\${REMOTE_BIN}.bak\" (если файл существует)." >&2
    echo "  Путь на хосте: REMOTE_BIN (по умолчанию $REMOTE_BIN), переопределение: REMOTE_BIN=/path $0 ..." >&2
    echo "  Цель деплоя скрипта: mno@161.97.96.43 (генерация db.json). 100.254 — сайт, только готовый db.json." >&2
    exit 1
}

[[ $# -ge 1 ]] || usage
[[ -f "$LOCAL" ]] || { echo "Нет файла: $LOCAL" >&2; exit 1; }
[[ -f "$LOCAL_CHECK" ]] || { echo "Нет файла: $LOCAL_CHECK" >&2; exit 1; }

for host in "$@"; do
    echo "=== $host ==="
    ssh "$host" "if [[ -f \"$REMOTE_BIN\" ]]; then cp -p -- \"$REMOTE_BIN\" \"${REMOTE_BIN}.bak\" && echo \"Сохранено: ${REMOTE_BIN}.bak\"; else echo \"Нет $REMOTE_BIN — бэкап пропущен\"; fi"
    scp "$LOCAL" "$host:$REMOTE_BIN"
    scp "$LOCAL_CHECK" "$host:$REMOTE_CHECK"
    ssh "$host" "chmod +x \"$REMOTE_BIN\" \"$REMOTE_CHECK\""
    echo "Готово: $host:$REMOTE_BIN $REMOTE_CHECK"
done
