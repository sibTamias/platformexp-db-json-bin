#!/usr/bin/env bash
# Залить скрипты IP2Location на platformExp (96.43) — обычно достаточно git pull + install_symlinks.
set -euo pipefail

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_BIN="${REMOTE_BIN:-/home/mno/bin}"

FILES=(
    update_ip2location_db.sh
    transfer_ip2location_db.sh
    update_and_transfer_ip2location_db.sh
)

usage() {
    echo "Использование: $0 user@host" >&2
    echo "  Цель: mno@161.97.96.43 (platformExp)" >&2
    exit 1
}

[[ $# -ge 1 ]] || usage

for host in "$@"; do
    echo "=== $host ==="
    for f in "${FILES[@]}"; do
        [[ -f "$BIN/$f" ]] || { echo "Нет $BIN/$f" >&2; exit 1; }
        ssh "$host" "if [[ -f \"$REMOTE_BIN/$f\" ]]; then cp -p \"$REMOTE_BIN/$f\" \"${REMOTE_BIN}/${f}.bak\"; fi"
        scp "$BIN/$f" "$host:$REMOTE_BIN/$f"
    done
    ssh "$host" "chmod +x ${REMOTE_BIN}/update_ip2location_db.sh ${REMOTE_BIN}/transfer_ip2location_db.sh ${REMOTE_BIN}/update_and_transfer_ip2location_db.sh"
    echo "Готово: $host:$REMOTE_BIN/{update,transfer}_ip2location_db.sh"
done
