#!/usr/bin/env bash
# Сверка: высота из PLATFORM_EXPLORER_URL/status (.api.block.height) vs реальная Platform
# из `dashmate status --format json` (.platform.tenderdash.latestBlockHeight).
# Если эксплорер отстаёт от ноды — типичная поломка индексера на :3005.
#
# Переменные (рядом .env или окружение):
#   PLATFORM_EXPLORER_URL — по умолчанию http://localhost:3005
#   EXPLORER_DASHMATE_DRIFT_MAX — допустимое отставание API в блоках (по умолчанию 25)
#   DASHMATE_CONFIG — опционально имя конфига для dashmate, например mainnet
#
# Флаги:
#   --warn-only  — только сообщение в stderr, код выхода 0
#
# Выход: 0 — ок или проверка пропущена; 1 — превышен допуск; 2 — нет высоты у API
set -euo pipefail

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"

PLATFORM_EXPLORER_URL="${PLATFORM_EXPLORER_URL:-http://localhost:3005}"
DRIFT_MAX="${EXPLORER_DASHMATE_DRIFT_MAX:-25}"
WARN_ONLY=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --warn-only) WARN_ONLY=1; shift ;;
        -h|--help)
            cat <<'USAGE'
Сверка высоты platform-explorer (/status, .api.block.height) с dashmate (Platform).

Переменные: PLATFORM_EXPLORER_URL, EXPLORER_DASHMATE_DRIFT_MAX (по умолчанию 25),
  DASHMATE_CONFIG (опционально, имя конфига dashmate).

  --warn-only  только предупреждение в stderr, выход 0

Коды выхода: 0 — ок или проверка пропущена; 1 — слишком большой разрыв; 2 — нет высоты в API.
USAGE
            exit 0
            ;;
        *)
            echo "Неизвестный аргумент: $1 (см. $0 --help)" >&2
            exit 1
            ;;
    esac
done

api_height() {
    local b h
    b=$(curl -sS --max-time 15 "${PLATFORM_EXPLORER_URL}/status" 2>/dev/null) || return 1
    h=$(echo "$b" | jq -r '.api.block.height // empty')
    [[ -n "$h" && "$h" != "null" && "$h" =~ ^[0-9]+$ ]] || return 1
    echo "$h"
}

dashmate_platform_height() {
    local json h
    local -a dm=(dashmate status)
    [[ -n "${DASHMATE_CONFIG:-}" ]] && dm+=(--config "$DASHMATE_CONFIG")
    dm+=(--format json)
    json=$("${dm[@]}" 2>/dev/null) || return 1
    h=$(echo "$json" | jq -r '
      (.platform.tenderdash.latestBlockHeight // .platform.tenderdash.blockHeight // empty)
      | if type == "number" then tostring else . end
    ')
    [[ -n "$h" && "$h" != "null" && "$h" =~ ^[0-9]+$ ]] || return 1
    echo "$h"
}

ex=$(api_height) || {
    echo "check_platform_explorer_vs_dashmate: нет числовой .api.block.height с ${PLATFORM_EXPLORER_URL}/status" >&2
    exit 2
}

if ! command -v dashmate >/dev/null 2>&1; then
    echo "check_platform_explorer_vs_dashmate: dashmate не в PATH — сравнение с нодой пропущено (API height=$ex)" >&2
    exit 0
fi

node=$(dashmate_platform_height) || {
    echo "check_platform_explorer_vs_dashmate: не удалось взять Platform height из dashmate status — пропуск (API height=$ex)" >&2
    exit 0
}

diff=$((node - ex))
if (( diff > DRIFT_MAX )); then
    echo "check_platform_explorer_vs_dashmate: ОШИБКА — platform-explorer ОТСТАЁТ от dashmate на ${diff} блок(ов)." >&2
    echo "  dashmate Platform height: $node" >&2
    echo "  ${PLATFORM_EXPLORER_URL}/status .api.block.height: $ex" >&2
    echo "  Допуск EXPLORER_DASHMATE_DRIFT_MAX=${DRIFT_MAX}. Почините индексер/API на эксплорере или временно SKIP_EXPLORER_DASHMATE_HEIGHT_CHECK=1." >&2
    [[ "$WARN_ONLY" -eq 1 ]] && exit 0
    exit 1
fi

if (( diff < -DRIFT_MAX )); then
    echo "check_platform_explorer_vs_dashmate: ПРЕДУПРЕЖДЕНИЕ — API выше ноды на $((-diff)) блок(ов) (API=$ex dashmate=$node). Проверьте URL/сеть." >&2
    [[ "$WARN_ONLY" -eq 1 ]] && exit 0
    exit 1
fi

echo "check_platform_explorer_vs_dashmate: ok (explorer=$ex dashmate=$node Δ=${diff})" >&2
exit 0
