#!/usr/bin/env bash
# Один раз на platformExp: симлинки ~/bin/*.sh -> этот репозиторий.
# Дальше: cd ~/platformexp-db-json-bin && git pull — запуск как раньше: /home/mno/bin/generate_db_json_local.sh
# .env остаётся в ~/bin/.env (скрипт подхватывает BIN=/home/mno/bin при вызове через symlink из bin).
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="${TARGET_BIN:-$HOME/bin}"
mkdir -p "$TARGET"
for f in generate_db_json_local.sh check_platform_explorer_vs_dashmate.sh monitor_new_blocks_count.sh push_generate_db_json_local.sh; do
  [[ -f "$REPO/$f" ]] || { echo "Нет файла: $REPO/$f" >&2; exit 1; }
  ln -sf "$REPO/$f" "$TARGET/$f"
done
chmod +x "$REPO"/*.sh 2>/dev/null || true
echo "Готово. Ссылки: $TARGET/{generate,check,monitor,push}_*.sh -> $REPO/"
echo "Обновление скриптов: cd $REPO && git pull"
echo "Запуск (как раньше): $TARGET/generate_db_json_local.sh"
