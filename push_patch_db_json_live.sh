#!/usr/bin/env bash
# Залить patch_db_json_live.sh (+ refresh, epoch watch, generate) на серверы сайта или platformExp.
# Пример:
#   ./push_patch_db_json_live.sh mno@46.19.66.201 mno@161.97.100.254
#   ./push_patch_db_json_live.sh mno@161.97.96.43
set -euo pipefail

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_BIN="${REMOTE_BIN:-/home/mno/bin}"

FILES=(
    patch_db_json_live.sh
    refresh_db_json_baseline.sh
    run_db_json_epoch_watch.sh
    generate_db_json_local.sh
    check_platform_explorer_vs_dashmate.sh
)

[[ $# -ge 1 ]] || {
    echo "Использование: $0 user@host [user@host ...]" >&2
    exit 1
}

for f in "${FILES[@]}"; do
    [[ -f "$BIN/$f" ]] || { echo "Нет $BIN/$f" >&2; exit 1; }
done

for host in "$@"; do
    echo "=== $host ==="
    for f in "${FILES[@]}"; do
        ssh "$host" "mkdir -p \"$REMOTE_BIN\""
        scp "$BIN/$f" "$host:$REMOTE_BIN/$f"
        ssh "$host" "chmod +x \"$REMOTE_BIN/$f\""
        echo "  $f"
    done
    echo "Готово: $host:$REMOTE_BIN/"
done
