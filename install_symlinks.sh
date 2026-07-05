#!/usr/bin/env bash
# Один раз на platformExp: симлинки ~/bin/*.sh -> этот репозиторий.
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="${TARGET_BIN:-$HOME/bin}"
mkdir -p "$TARGET"
for f in generate_db_json_local.sh run_withdrawals_and_transfer_local.sh transfer_db.sh setup_transfer_db_conf.sh transfer_db.conf.example patch_db_json_live.sh patch_db_json_on_new_block.sh refresh_db_json_baseline.sh reconcile_db_json_blocks_from_cache.sh sync_validator_blocks_cache.sh lib_db_json_blocks.sh run_db_json_epoch_watch.sh check_platform_explorer_vs_dashmate.sh monitor_new_blocks_count.sh push_generate_db_json_local.sh scan_core_versions.sh update_ip2location_db.sh transfer_ip2location_db.sh update_and_transfer_ip2location_db.sh; do
  if [[ -f "$REPO/$f" ]]; then
    ln -sf "$REPO/$f" "$TARGET/$f"
  else
    echo "skip (нет в репо): $f" >&2
  fi
done
chmod +x "$REPO"/*.sh 2>/dev/null || true
echo "Готово. Ссылки в $TARGET/ -> $REPO/"
echo "Обновление: cd $REPO && git pull"
