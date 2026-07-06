#!/usr/bin/env bash
# seed_current_epoch_blocks_cache.sh — cache + blocks текущей эпохи (локальный PE).
#
# Прошлые эпохи в db.json не трогаем (заморожены baseline). Текущая эпоха N:
#   blocks в db.json baseline (96.43) НЕ используем — обнуляем перед прогоном.
#   1) (fresh) blocks=0 в db.json, cache без высот h>=firstBlock_N
#   2) PE-прогон: все валидаторы → высоты эпохи N только из PE
#   3) post-patch: блоки, появившиеся пока шёл прогон
#   4) reconcile: blocks = count(cache) для epoch N
#
#   ./seed_current_epoch_blocks_cache.sh
#   ./seed_current_epoch_blocks_cache.sh --reset-patch   # то же (reconcile пишет STATE)
#
# Переменные: DB_JSON, SAVE_DIR, PLATFORM_EXPLORER_URL, PARALLEL_JOBS (default 4)
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
LOG_FILE="${SEED_EPOCH_BLOCKS_LOG:-$SAVE_DIR/tmp/seed_current_epoch_blocks.log}"
BOUNDS_FILE="$SAVE_DIR/tmp/epoch_bounds.txt"
PARALLEL_JOBS="${PARALLEL_JOBS:-4}"

DO_RESET_PATCH=1
DRY_RUN=0
QUIET=0
SKIP_PRE_PATCH=0
SKIP_POST_PATCH=0
SEED_FRESH=1

usage() {
    cat <<'USAGE'
Использование: seed_current_epoch_blocks_cache.sh [опции]

  --reset-patch      после seed: reconcile пишет STATE = tip PE (по умолчанию включено)
  --no-reset-patch   не обновлять STATE (reconcile --no-state)
  --skip-pre-patch   patch --once до PE (только с --no-fresh / повторный догон)
  --skip-post-patch  не вызывать patch --once после PE-прогона
  --fresh            по умолчанию: обнулить blocks эпохи N в db.json, очистить cache h>=firstBlock
  --no-fresh         не чистить; разрешить pre-patch при наличии STATE
  --dry-run          не писать db.json / cache / STATE
  --quiet            меньше stdout

Закрывает окно во время PE-прогона:
  fresh      → эпоха N в db.json/cache не из baseline, только PE
  post-patch → блоки N, появившиеся пока шёл прогон
  reconcile  → blocks = count(cache)

Нужны: db.json, tmp/epoch_bounds.txt, локальный PE (localhost:3005).
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

api_status_height() {
    curl -sS --max-time 15 "${PLATFORM_EXPLORER_URL%/}/status" 2>/dev/null \
        | jq -r '.api.block.height // empty'
}

run_patch_once() {
    local label="$1"
    local patch="$BIN/patch_db_json_on_new_block.sh"
    [[ -x "$patch" ]] || { log "skip $label: нет $patch"; return 0; }
    log "$label: $patch --once"
    if bash "$patch" --once >>"$LOG_FILE" 2>&1; then
        return 0
    fi
    local rc=$?
    if [[ "$rc" -eq 2 ]]; then
        log_always "ERROR: $label — смена эпохи (нужен refresh_db_json_baseline.sh)"
        exit 2
    fi
    log "WARN: $label вернул rc=$rc (см. $LOG_FILE)"
    return "$rc"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --reset-patch) DO_RESET_PATCH=1; shift ;;
        --no-reset-patch) DO_RESET_PATCH=0; shift ;;
        --skip-pre-patch) SKIP_PRE_PATCH=1; shift ;;
        --skip-post-patch) SKIP_POST_PATCH=1; shift ;;
        --fresh) SEED_FRESH=1; shift ;;
        --no-fresh) SEED_FRESH=0; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        --quiet) QUIET=1; shift ;;
        -h|--help) usage ;;
        *) echo "Неизвестный аргумент: $1" >&2; exit 1 ;;
    esac
done

mkdir -p "$SAVE_DIR/tmp" "$SAVE_DIR/cache/validators" "$(dirname "$LOG_FILE")"

[[ -f "$DB_JSON" ]] || { log_always "ERROR: нет $DB_JSON"; exit 1; }
[[ -f "$BOUNDS_FILE" ]] || { log_always "ERROR: нет $BOUNDS_FILE"; exit 1; }

api_body=$(curl -sS --max-time 15 "${PLATFORM_EXPLORER_URL%/}/status" 2>/dev/null) || api_body=""
api_epoch=$(echo "$api_body" | jq -r '.epoch.number // .epochs.current // empty')
api_height=$(echo "$api_body" | jq -r '.api.block.height // empty')
db_epoch=$(jq -r '.current_epoch // empty' "$DB_JSON")

if [[ -z "$api_epoch" || "$api_epoch" == "null" || ! "$api_epoch" =~ ^[0-9]+$ ]]; then
    api_epoch="$db_epoch"
    log "epoch из /status пуст — db.json current_epoch=$api_epoch"
fi
[[ -n "$api_epoch" && "$api_epoch" =~ ^[0-9]+$ ]] || { log_always "ERROR: epoch"; exit 1; }
[[ -n "$api_height" && "$api_height" =~ ^[0-9]+$ ]] || { log_always "ERROR: height из /status"; exit 1; }

[[ "$db_epoch" == "$api_epoch" ]] || {
    log_always "EPOCH_MISMATCH db.json=$db_epoch API=$api_epoch — сначала refresh_db_json_baseline.sh"
    exit 2
}

bounds=$(read_epoch_bounds "$api_epoch" "$BOUNDS_FILE") || {
    log_always "ERROR: нет границ эпохи $api_epoch в $BOUNDS_FILE"
    exit 1
}
read -r epoch_first _epoch_last_from_bounds <<<"$bounds"

log "=== seed epoch=$api_epoch validators=$(jq -r '.validators | length' "$DB_JSON") parallel=$PARALLEL_JOBS fresh=$SEED_FRESH ==="

if [[ "$DRY_RUN" -eq 0 && "$SEED_FRESH" -eq 1 ]]; then
    log "fresh: blocks эпохи $api_epoch в db.json → 0 (baseline 96.43 не используем)"
    clear_db_json_current_epoch_blocks "$DB_JSON" "$api_epoch"
    log "fresh: cache — убрать высоты h>=${epoch_first} (только PE заполнит эпоху $api_epoch)"
    trim_cache_from_epoch_first "$epoch_first"
    SKIP_PRE_PATCH=1
fi

if [[ "$DRY_RUN" -eq 0 && "$SKIP_PRE_PATCH" -eq 0 ]]; then
    if [[ -f "$STATE_FILE" ]]; then
        run_patch_once "pre-patch" || true
    else
        log "pre-patch: пропуск (нет $STATE_FILE — первый seed после baseline)"
    fi
fi

tip_snapshot=$(api_status_height)
[[ -n "$tip_snapshot" && "$tip_snapshot" =~ ^[0-9]+$ ]] || { log_always "ERROR: tip перед PE"; exit 1; }

log "PE-прогон: heights ${epoch_first}..${tip_snapshot}"

export DB_JSON SAVE_DIR PLATFORM_EXPLORER_URL API_EPOCH="$api_epoch" EPOCH_FIRST="$epoch_first" UPPER_BOUND="$tip_snapshot" PARALLEL_JOBS DRY_RUN
result_json=$(python3 <<'PY'
import json
import os
import sys
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

save = os.environ["SAVE_DIR"]
pe_base = os.environ["PLATFORM_EXPLORER_URL"].rstrip("/")
db_path = os.environ["DB_JSON"]
epoch_n = int(os.environ["API_EPOCH"])
fb = int(os.environ["EPOCH_FIRST"])
ub = int(os.environ["UPPER_BOUND"])
parallel = max(1, int(os.environ.get("PARALLEL_JOBS", "4")))
dry_run = os.environ.get("DRY_RUN", "0") == "1"
cache_dir = os.path.join(save, "cache", "validators")

with open(db_path) as f:
    db = json.load(f)
validators = list((db.get("validators") or {}).keys())
if not validators:
    print(json.dumps({"error": "no validators"}))
    sys.exit(1)


def iso_to_ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def write_last_block_meta(protx_l: str, height: int, timestamp: str) -> None:
    if dry_run or not timestamp or height <= 0:
        return
    try:
        ts_ms = iso_to_ms(timestamp)
    except ValueError:
        return
    path = os.path.join(cache_dir, f"{protx_l}_last_block.json")
    with open(path, "w") as fh:
        json.dump({"height": height, "timestamp_ms": ts_ms, "epoch": epoch_n}, fh)


def fetch_epoch_heights(protx_l: str) -> tuple[list[int], int, str]:
    heights: list[int] = []
    last_h = 0
    last_ts = ""
    page = 1
    stop = False
    while page <= 50 and not stop:
        url = f"{pe_base}/validator/{protx_l}/blocks?page={page}&limit=100&order=desc"
        with urllib.request.urlopen(urllib.request.Request(url), timeout=30) as resp:
            payload = json.load(resp)
        rows = payload.get("resultSet") or []
        if not rows:
            break
        for item in rows:
            header = item.get("header") or {}
            h = int(header.get("height") or 0)
            ts = header.get("timestamp") or ""
            if h < fb:
                stop = True
                break
            if fb <= h <= ub:
                heights.append(h)
                if h >= last_h:
                    last_h = h
                    last_ts = ts
        if len(rows) < 100:
            break
        page += 1
    return sorted(set(heights)), last_h, last_ts


def replace_epoch_cache(protx_l: str, epoch_heights: list[int]) -> int:
    if dry_run:
        return len(epoch_heights)
    path = os.path.join(cache_dir, f"{protx_l}_all_blocks.txt")
    info = os.path.join(cache_dir, f"{protx_l}_info.txt")
    outside: set[int] = set()
    if os.path.isfile(path):
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if line.isdigit():
                    h = int(line)
                    if h < fb:
                        outside.add(h)
    merged = sorted(outside | set(epoch_heights))
    with open(path, "w") as fh:
        for h in merged:
            fh.write(f"{h}\n")
    with open(info, "w") as fh:
        fh.write(str(len(merged)))
    return len(epoch_heights)


def process(protx: str) -> dict:
    protx_l = protx.lower()
    try:
        heights, last_h, last_ts = fetch_epoch_heights(protx_l)
        n = replace_epoch_cache(protx_l, heights)
        write_last_block_meta(protx_l, last_h, last_ts)
        return {"protx": protx_l, "fetched": n, "ok": True}
    except Exception as exc:
        return {"protx": protx_l, "fetched": 0, "ok": False, "error": str(exc)}


errors: list[dict] = []
fetched_total = 0
with ThreadPoolExecutor(max_workers=parallel) as pool:
    futures = {pool.submit(process, v): v for v in validators}
    done = 0
    total = len(validators)
    for fut in as_completed(futures):
        done += 1
        row = fut.result()
        fetched_total += row.get("fetched", 0)
        if not row.get("ok"):
            errors.append(row)
        if done % 50 == 0 or done == total:
            print(f"progress {done}/{total}", file=sys.stderr, flush=True)

print(json.dumps({
    "epoch_first": fb,
    "upper": ub,
    "validators": len(validators),
    "fetched_rows": fetched_total,
    "errors": len(errors),
    "error_samples": errors[:5],
    "dry_run": dry_run,
}))
PY
)

if echo "$result_json" | jq -e '.error' >/dev/null 2>&1; then
    log_always "ERROR: $(echo "$result_json" | jq -r '.error')"
    exit 1
fi

err_n=$(echo "$result_json" | jq -r '.errors')
fetched_n=$(echo "$result_json" | jq -r '.fetched_rows')
log "PE готов: fetched=$fetched_n ошибок=$err_n (эпоха $api_epoch только из PE → cache)"

if [[ "$DRY_RUN" -eq 1 ]]; then
    log "DRY RUN — post-patch/reconcile пропущены"
    exit 0
fi

if [[ "$SKIP_POST_PATCH" -eq 0 ]]; then
    if [[ ! -f "$STATE_FILE" ]]; then
        printf '%s\n' "$tip_snapshot" >"$STATE_FILE"
        log "STATE init -> $tip_snapshot (догон блоков во время PE-прогона)"
    fi
    run_patch_once "post-patch" || true
fi

reconcile_args=(--quiet)
[[ "$DO_RESET_PATCH" -eq 0 ]] && reconcile_args+=(--no-state)

log "reconcile: blocks из cache (дедуп patch + PE)"
bash "$BIN/reconcile_db_json_blocks_from_cache.sh" "${reconcile_args[@]}"

if [[ "$err_n" -gt 0 ]]; then
    echo "$result_json" | jq -r '.error_samples[]? | "\(.protx): \(.error)"' | while read -r line; do
        log "WARN PE: $line"
    done
fi

log "=== seed готов: epoch=$api_epoch, дальше cron patch +1 ==="
