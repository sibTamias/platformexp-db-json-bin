#!/usr/bin/env bash
# reconcile_db_json_blocks_from_cache.sh — точный blocks текущей эпохи из cache/validators/*_all_blocks.txt
#
# Закрывает дыру между длинным generate (~50 мин) и baseline+--reset на 254:
# считает blocks по спискам высот (как generate_db_json_local.sh), upper bound = tip PE.
#
#   ./sync_validator_blocks_cache.sh mno@161.97.96.43   # один раз / после generate
#   ./reconcile_db_json_blocks_from_cache.sh
#   ./patch_db_json_on_new_block.sh --reset             # опционально; reconcile уже пишет STATE
#
# Переменные: DB_JSON, SAVE_DIR, PLATFORM_EXPLORER_URL, PATCH_DB_JSON_STATE
set -euo pipefail

export TZ=Asia/Irkutsk

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib_db_json_blocks.sh
source "$BIN/lib_db_json_blocks.sh"

[[ -r "${BIN_DIR:-$HOME/bin}/.env" ]] && source "${BIN_DIR:-$HOME/bin}/.env"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"

DB_JSON="${DB_JSON:-${DB_JSON_PATH:-$HOME/db.json}}"
SAVE_DIR="${SAVE_DIR:-$HOME/tmp/withdrawals}"
PLATFORM_EXPLORER_URL="${PLATFORM_EXPLORER_URL:-http://127.0.0.1:3005}"
STATE_FILE="${PATCH_DB_JSON_STATE:-$SAVE_DIR/tmp/patch_db_json_last_block_height.txt}"
LOG_FILE="${RECONCILE_BLOCKS_LOG:-$SAVE_DIR/tmp/reconcile_db_json_blocks.log}"
BOUNDS_FILE="$SAVE_DIR/tmp/epoch_bounds.txt"

DRY_RUN=0
NO_STATE=0
UPPER_HEIGHT=""
QUIET=0

usage() {
    cat <<'USAGE'
Использование: reconcile_db_json_blocks_from_cache.sh [опции]

  --dry-run           не писать db.json / STATE
  --no-state          не обновлять patch_db_json_last_block_height.txt
  --upper-height N    верхняя граница эпохи (по умолчанию api.block.height)
  --quiet             меньше stdout

Считает blocks текущей эпохи: строки в cache/validators/{proTx}_all_blocks.txt
  где firstBlockEpoch <= height <= upperBound (tip PE для текущей эпохи).

Нужны: db.json, tmp/epoch_bounds.txt, cache/validators/*_all_blocks.txt
USAGE
    exit 0
}

log() {
    [[ "$QUIET" -eq 1 ]] && return 0
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

log_always() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run) DRY_RUN=1; shift ;;
        --no-state) NO_STATE=1; shift ;;
        --upper-height)
            UPPER_HEIGHT="${2:-}"
            [[ "$UPPER_HEIGHT" =~ ^[0-9]+$ ]] || { echo "--upper-height: число" >&2; exit 1; }
            shift 2
            ;;
        --quiet) QUIET=1; shift ;;
        -h|--help) usage ;;
        *) echo "Неизвестный аргумент: $1" >&2; exit 1 ;;
    esac
done

mkdir -p "$SAVE_DIR/tmp" "$(dirname "$LOG_FILE")"

[[ -f "$DB_JSON" ]] || { log_always "ERROR: нет $DB_JSON"; exit 1; }
[[ -f "$BOUNDS_FILE" ]] || { log_always "ERROR: нет $BOUNDS_FILE"; exit 1; }

api_body=$(curl -sS --max-time 15 "${PLATFORM_EXPLORER_URL%/}/status" 2>/dev/null) || api_body=""
api_epoch=$(echo "$api_body" | jq -r '.epoch.number // .epochs.current // empty')
api_height=$(echo "$api_body" | jq -r '.api.block.height // empty')
db_epoch=$(jq -r '.current_epoch // empty' "$DB_JSON")
if [[ -z "$api_epoch" || "$api_epoch" == "null" || ! "$api_epoch" =~ ^[0-9]+$ ]]; then
    api_epoch="$db_epoch"
    log "epoch из /status пуст — используем db.json current_epoch=$api_epoch"
fi
[[ -n "$api_epoch" && "$api_epoch" =~ ^[0-9]+$ ]] || { log_always "ERROR: epoch (API и db.json)"; exit 1; }
[[ -n "$api_height" && "$api_height" =~ ^[0-9]+$ ]] || { log_always "ERROR: height из /status"; exit 1; }

[[ "$db_epoch" == "$api_epoch" ]] || {
    log_always "EPOCH_MISMATCH db.json=$db_epoch API=$api_epoch — нужен refresh_db_json_baseline.sh"
    exit 2
}

bounds=$(read_epoch_bounds "$api_epoch" "$BOUNDS_FILE") || {
    log_always "ERROR: нет границ эпохи $api_epoch в $BOUNDS_FILE"
    exit 1
}
read -r epoch_first _epoch_last_from_bounds <<<"$bounds"
upper_bound="${UPPER_HEIGHT:-$api_height}"
if (( upper_bound < epoch_first )); then
    log_always "ERROR: upper_bound=$upper_bound < epoch_first=$epoch_first"
    exit 1
fi

log "reconcile epoch=$api_epoch heights ${epoch_first}..${upper_bound} (PE tip=$api_height)"

export DB_JSON SAVE_DIR API_EPOCH="$api_epoch" EPOCH_FIRST="$epoch_first" UPPER_BOUND="$upper_bound"
counts_json=$(python3 <<'PY'
import json, os, sys

db_path = os.environ["DB_JSON"]
save = os.environ["SAVE_DIR"]
cache_dir = os.path.join(save, "cache", "validators")
epoch = int(os.environ["API_EPOCH"])
fb = int(os.environ["EPOCH_FIRST"])
ub = int(os.environ["UPPER_BOUND"])

with open(db_path) as f:
    db = json.load(f)

validators = db.get("validators") or {}
counts = {}
missing_cache = 0
changed = 0
same = 0

for protx, v in validators.items():
    protx_l = protx.lower()
    cf = os.path.join(cache_dir, f"{protx_l}_all_blocks.txt")
    if not os.path.isfile(cf):
        cnt = 0
        missing_cache += 1
    else:
        cnt = 0
        with open(cf) as fh:
            for line in fh:
                line = line.strip()
                if not line.isdigit():
                    continue
                h = int(line)
                if fb <= h <= ub:
                    cnt += 1
    counts[protx_l] = cnt
    wds = v.get("withdrawals") or []
    old = 0
    for w in wds:
        if w.get("epoch") == epoch:
            old = w.get("blocks") or 0
            break
    if old != cnt:
        changed += 1
    else:
        same += 1

print(json.dumps({
    "counts": counts,
    "changed": changed,
    "same": same,
    "missing_cache": missing_cache,
    "total": len(validators),
}))
PY
)

changed_n=$(echo "$counts_json" | jq -r '.changed')
missing_n=$(echo "$counts_json" | jq -r '.missing_cache')
total_n=$(echo "$counts_json" | jq -r '.total')

log "валидаторов=$total_n изменено blocks=$changed_n без cache=$missing_n"

if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "$counts_json" | jq -r '.counts | to_entries | map(select(.value > 0)) | sort_by(-.value) | .[0:10][] | "\(.value)\t\(.key[0:20])…"' \
        | while IFS=$'\t' read -r c k; do log "DRY sample: blocks=$c $k"; done
    log "DRY RUN — db.json не изменён"
    exit 0
fi

tmp=$(mktemp "${DB_JSON}.reconcile.XXXXXX")
if ! jq --argjson ce "$api_epoch" --arg counts "$counts_json" '
    ($counts | fromjson) as $meta |
    .validators |= with_entries(
        .key as $k |
        .value.withdrawals |= (
            if any(.[]?; .epoch == $ce) then
                map(if .epoch == $ce then
                    .blocks = ($meta.counts[($k | ascii_downcase)] // .blocks // 0)
                else . end)
            else
                (. + [{epoch: $ce, blocks: ($meta.counts[($k | ascii_downcase)] // 0), withdrawal: 0}])
                | map({epoch, blocks, withdrawal}) | sort_by(.epoch)
            end
        )
    )
' "$DB_JSON" >"$tmp"; then
    rm -f "$tmp"
    log_always "ERROR: jq patch failed"
    exit 1
fi
mv -f "$tmp" "$DB_JSON"

recalc_ratings_db_json "$DB_JSON" "$api_epoch"
log "пересчитан rating для epoch=$api_epoch"

if [[ "$NO_STATE" -eq 0 ]]; then
    printf '%s\n' "$upper_bound" >"$STATE_FILE"
    log "STATE -> $upper_bound"
fi

log "готово: обновлено $changed_n валидаторов в $DB_JSON"
