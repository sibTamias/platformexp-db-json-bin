#!/usr/bin/env bash
# На platformExp (96.43): transfer db.json только на evowatch.ru (201), без 254.
set -euo pipefail

BIN="${BIN_DIR:-$HOME/bin}"
mkdir -p "$BIN"

if [[ -f "$BIN/transfer_db.conf.example" ]]; then
    cp "$BIN/transfer_db.conf.example" "$BIN/transfer_db.conf"
else
    cat >"$BIN/transfer_db.conf" <<'EOF'
# db.json SCP: only evowatch.ru (201). dashnodes.net (254) = live-patch locally.
TARGET_SERVERS="46.19.66.201"
REMOTE_USER="mno"
EOF
fi

if grep -q '^TARGET_SERVERS=' "$BIN/transfer_db.conf"; then
    sed -i.bak 's|^TARGET_SERVERS=.*|TARGET_SERVERS="46.19.66.201"|' "$BIN/transfer_db.conf"
else
    echo 'TARGET_SERVERS="46.19.66.201"' >>"$BIN/transfer_db.conf"
fi
chmod 644 "$BIN/transfer_db.conf"

echo "OK: $BIN/transfer_db.conf"
cat "$BIN/transfer_db.conf"
echo ""
echo "Проверка transfer_db.sh default (если conf удалить — тоже только 201):"
grep 'TARGET_SERVERS=' "$BIN/transfer_db.sh" 2>/dev/null | head -1 || true
echo ""
if grep -q '^SKIP_TRANSFER=1' "${BIN}/.env" 2>/dev/null || grep -q '^SKIP_TRANSFER=1' "$HOME/bin/.env" 2>/dev/null; then
    echo "WARN: SKIP_TRANSFER=1 в .env — transfer не пойдёт. Уберите или SKIP_TRANSFER=0"
else
    echo "SKIP_TRANSFER не мешает transfer."
fi
echo ""
echo "Cron (если нет):"
echo '  0 */2 * * * /home/mno/bin/run_withdrawals_and_transfer_local.sh >> /home/mno/tmp/cron.log 2>&1'
