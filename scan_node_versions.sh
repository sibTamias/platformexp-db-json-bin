#!/usr/bin/env bash
# scan_node_versions.sh — опрос Core RPC (getnetworkinfo) по IP валидаторов из db.json.
# Записывает core_version / platform_version / version_scanned_at в validators[proTx]
# и reference_node в корень db.json (версия локального dashmate на platformExp).
#
# Переменные (.env в ~/bin или рядом со скриптом):
#   SAVE_DIR=/home/mno/tmp/withdrawals
#   DB_JSON=$SAVE_DIR/db.json  (или ~/db.json после transfer)
#   DASH_CLI — dash-cli через dashmate/RPC (для reference_node)
#   VERSION_SCAN_RPC_PORTS="9998 19998" — порты JSON-RPC Core
#   VERSION_SCAN_WORKERS=16 — параллельных опросов IP
#   VERSION_SCAN_RPC_TIMEOUT=3 — секунд curl --max-time на IP
#   VERSION_SCAN_SKIP=1 — пропустить (для тестов)
#
# Не вызывается из --incremental-v1. Cron: раз в сутку или после полного generate.
set -euo pipefail

export TZ=Asia/Irkutsk
export LC_ALL=en_US.UTF-8

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"
[[ -r "${BIN_DIR:-$HOME/bin}/.env" ]] && source "${BIN_DIR:-$HOME/bin}/.env"

SAVE_DIR="${SAVE_DIR:-/home/mno/tmp/withdrawals}"
DB_JSON="${DB_JSON:-${DB_JSON_PATH:-$SAVE_DIR/db.json}}"
DASH_CLI="${DASH_CLI:-dashmate exec dash_core dash-cli}"
VERSION_SCAN_RPC_PORTS="${VERSION_SCAN_RPC_PORTS:-9998 19998}"
VERSION_SCAN_WORKERS="${VERSION_SCAN_WORKERS:-16}"
VERSION_SCAN_RPC_TIMEOUT="${VERSION_SCAN_RPC_TIMEOUT:-3}"

if [[ "${VERSION_SCAN_SKIP:-0}" == "1" ]]; then
    echo "scan_node_versions.sh: VERSION_SCAN_SKIP=1 — пропуск."
    exit 0
fi

[[ -f "$DB_JSON" ]] || {
    echo "scan_node_versions.sh: нет $DB_JSON" >&2
    exit 1
}

command -v jq >/dev/null 2>&1 || { echo "scan_node_versions.sh: нужен jq" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "scan_node_versions.sh: нужен curl" >&2; exit 1; }

normalize_core_version() {
    local raw="$1"
    [[ -z "$raw" || "$raw" == "null" ]] && return 0
    if [[ "$raw" =~ ([0-9]+\.[0-9]+\.[0-9]+) ]]; then
        echo "${BASH_REMATCH[1]}"
    fi
}

# JSON-RPC getnetworkinfo → subversion
probe_core_rpc() {
    local ip="$1"
    local port body subver ver
    for port in $VERSION_SCAN_RPC_PORTS; do
        body=$(curl -sS --max-time "$VERSION_SCAN_RPC_TIMEOUT" \
            -H 'Content-Type: application/json' \
            -d '{"jsonrpc":"1.0","id":"scan","method":"getnetworkinfo","params":[]}' \
            "http://${ip}:${port}/" 2>/dev/null) || continue
        subver=$(echo "$body" | jq -r '.result.subversion // empty' 2>/dev/null)
        ver=$(normalize_core_version "$subver")
        [[ -n "$ver" ]] && { echo "$ver"; return 0; }
    done
    return 1
}

fetch_reference_node() {
    local core_ver="" plat_ver="" dm_json
    if $DASH_CLI getnetworkinfo &>/dev/null; then
        local sub
        sub=$($DASH_CLI getnetworkinfo 2>/dev/null | jq -r '.subversion // empty')
        core_ver=$(normalize_core_version "$sub")
    fi
    if command -v dashmate >/dev/null 2>&1; then
        local -a dm=(dashmate status)
        [[ -n "${DASHMATE_CONFIG:-}" ]] && dm+=(--config "$DASHMATE_CONFIG")
        dm+=(--format json)
        dm_json=$("${dm[@]}" 2>/dev/null) || dm_json=""
        if [[ -n "$dm_json" ]]; then
            plat_ver=$(echo "$dm_json" | jq -r '
              (.platform.tenderdash.version // .platform.version // .platform.tenderdash.appVersion // empty)
              | if type == "number" then tostring else . end
            ' 2>/dev/null)
            [[ -z "$core_ver" ]] && core_ver=$(echo "$dm_json" | jq -r '
              (.core.version // .core.dashd.version // empty)
              | if type == "number" then tostring else . end
            ' 2>/dev/null)
            core_ver=$(normalize_core_version "$core_ver")
        fi
    fi
    jq -nc \
        --arg core "${core_ver:-}" \
        --arg platform "${plat_ver:-}" \
        --arg scanned "$(date -Is)" \
        '{
          core_version: (if $core == "" then null else $core end),
          platform_version: (if $platform == "" then null else $platform end),
          scanned_at: $scanned
        }'
}

SCANNED_AT=$(date -Is)
TMP_DIR="$SAVE_DIR/tmp/version_scan"
mkdir -p "$TMP_DIR"
IP_MAP="$TMP_DIR/ip_to_core.txt"
: >"$IP_MAP"

mapfile -t UNIQUE_IPS < <(jq -r '.validators | to_entries[] | .value.IP // empty' "$DB_JSON" | tr -d '\r' | awk 'NF && !seen[$0]++')

echo "scan_node_versions.sh: уникальных IP=${#UNIQUE_IPS[@]} workers=$VERSION_SCAN_WORKERS"

export -f probe_core_rpc normalize_core_version
export VERSION_SCAN_RPC_PORTS VERSION_SCAN_RPC_TIMEOUT

if [[ ${#UNIQUE_IPS[@]} -gt 0 ]]; then
    printf '%s\n' "${UNIQUE_IPS[@]}" | xargs -P "$VERSION_SCAN_WORKERS" -I{} bash -c '
        ip="{}"
        ver=$(probe_core_rpc "$ip" 2>/dev/null) || ver=""
        if [[ -n "$ver" ]]; then
            printf "%s\t%s\n" "$ip" "$ver"
        fi
    ' >>"$IP_MAP"
fi

declare -A IP_CORE=()
while IFS=$'\t' read -r ip ver; do
    [[ -n "$ip" && -n "$ver" ]] && IP_CORE["$ip"]="$ver"
done <"$IP_MAP"

FOUND=${#IP_CORE[@]}
echo "scan_node_versions.sh: Core version найдена для $FOUND IP"

REF_JSON=$(fetch_reference_node)
IP_CORES_JSON="$TMP_DIR/ip_cores.json"
if [[ -s "$IP_MAP" ]]; then
    jq -Rn '
        [inputs | select(length > 0) | split("\t") | select(length == 2) | {(.[0]): .[1]}]
        | add // {}
    ' "$IP_MAP" >"$IP_CORES_JSON"
else
    echo '{}' >"$IP_CORES_JSON"
fi

MERGED=$(mktemp)
jq \
    --arg scanned "$SCANNED_AT" \
    --argjson ref "$REF_JSON" \
    --slurpfile cores "$IP_CORES_JSON" \
    '
    ($cores[0] // {}) as $ipcores |
    .reference_node = $ref |
    .validators = (
      .validators | to_entries | map(
        .key as $h |
        .value as $v |
        ($v.IP // "") as $ip |
        {
          key: $h,
          value: ($v + {
            core_version: ($ipcores[$ip] // $v.core_version // null),
            platform_version: ($v.platform_version // null),
            version_scanned_at: $scanned
          })
        }
      ) | from_entries
    )
    ' "$DB_JSON" >"$MERGED"

mv "$MERGED" "$DB_JSON"
echo "scan_node_versions.sh: обновлён $DB_JSON (reference_node + validators[].core_version)"
