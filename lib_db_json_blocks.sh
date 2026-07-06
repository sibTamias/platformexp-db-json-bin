#!/usr/bin/env bash
# lib_db_json_blocks.sh — blocks/rating/cache для live db.json (source, не exec).
# Используют: patch_db_json_on_new_block.sh, reconcile_db_json_blocks_from_cache.sh

recalc_ratings_db_json() {
    local db="$1" epoch="$2"
    local tmp list rating prev_blocks prev_rating blocks key
    tmp=$(mktemp "${db}.rating.XXXXXX")
    list=$(mktemp)
    jq -r --argjson ce "$epoch" '
        .validators | to_entries[] |
        ((.value.withdrawals // []) | map(select(.epoch == $ce)) | if length > 0 then .[0].blocks else 0 end) as $b |
        "\($b)\t\(.key)"
    ' "$db" | sort -t$'\t' -k1,1nr -k2,2 >"$list"

    rating=1
    prev_blocks=-1
    prev_rating=1
    declare -A rating_map=()
    while IFS=$'\t' read -r blocks key; do
        if [[ "$blocks" != "$prev_blocks" ]]; then
            if [[ "$prev_blocks" -ne -1 ]]; then
                rating=$((prev_rating + 1))
            fi
            prev_rating=$rating
            prev_blocks=$blocks
        fi
        rating_map[$key]=$rating
    done <"$list"
    rm -f "$list"

    {
        echo -n '{'
        first=1
        for key in "${!rating_map[@]}"; do
            [[ $first -eq 1 ]] && first=0 || echo -n ','
            printf '"%s":%s' "$key" "${rating_map[$key]}"
        done
        echo '}'
    } >"${tmp}.ratings.json"

    jq --slurpfile r "${tmp}.ratings.json" '
        .validators |= with_entries(.value.rating = ($r[0][.key] // 999999))
    ' "$db" >"$tmp"
    rm -f "${tmp}.ratings.json"
    mv -f "$tmp" "$db"
}

# Добавить высоту блока в cache/validators/{proTx}_all_blocks.txt (для reconcile на 254).
append_validator_block_cache() {
    local protx="${1,,}" height="$2"
    local save="${SAVE_DIR:-$HOME/tmp/withdrawals}"
    local cache_dir="$save/cache/validators"
    local cache_file="$cache_dir/${protx}_all_blocks.txt"
    local info_file="$cache_dir/${protx}_info.txt"
    [[ -n "$protx" && "$height" =~ ^[0-9]+$ ]] || return 1
    mkdir -p "$cache_dir"
    if [[ -f "$cache_file" ]] && grep -qx "$height" "$cache_file" 2>/dev/null; then
        return 0
    fi
    echo "$height" >>"$cache_file"
    local lines
    lines=$(wc -l <"$cache_file" | tr -d '[:space:]')
    echo "$lines" >"$info_file"
}

count_epoch_blocks_from_cache() {
    local protx="${1,,}" fb="$2" ub="$3"
    local save="${SAVE_DIR:-$HOME/tmp/withdrawals}"
    local cache_file="$save/cache/validators/${protx}_all_blocks.txt"
    [[ -f "$cache_file" ]] || { echo 0; return; }
    awk -v b="$fb" -v c="$ub" '$1 >= b && $1 <= c {count++} END {print count+0}' "$cache_file"
}

read_epoch_bounds() {
    local epoch="$1" bounds_file="${2:-${SAVE_DIR:-$HOME/tmp/withdrawals}/tmp/epoch_bounds.txt}"
    local line fb lb
    [[ -f "$bounds_file" ]] || return 1
    line=$(awk -F: -v e="$epoch" '$1 == e {print; exit}' "$bounds_file") || return 1
    [[ -n "$line" ]] || return 1
    IFS=: read -r _ fb lb <<<"$line"
    [[ "$fb" =~ ^[0-9]+$ && "$lb" =~ ^[0-9]+$ ]] || return 1
    echo "$fb $lb"
}

# Эпохи 1..N-1 в db.json не трогаем. Текущая N — только PE-прогон, не baseline с 96.43.
clear_db_json_current_epoch_blocks() {
    local db="$1" epoch="$2"
    local tmp
    tmp=$(mktemp "${db}.clearce.XXXXXX")
    jq --argjson ce "$epoch" '
        .validators |= with_entries(
            .value.withdrawals |= (
                if any(.[]?; .epoch == $ce) then
                    map(if .epoch == $ce then .blocks = 0 else . end)
                else
                    (. + [{epoch: $ce, blocks: 0, withdrawal: (.[0].withdrawal // 0)}])
                    | map({epoch, blocks, withdrawal}) | sort_by(.epoch)
                end
            )
        )
    ' "$db" >"$tmp"
    mv -f "$tmp" "$db"
}

# Убрать из cache высоты текущей эпохи (h >= firstBlock) перед PE-прогоном.
trim_cache_from_epoch_first() {
    local fb="$1"
    local save="${SAVE_DIR:-$HOME/tmp/withdrawals}"
    local cache_dir="$save/cache/validators"
    local f protx lines
    [[ "$fb" =~ ^[0-9]+$ ]] || return 1
    [[ -d "$cache_dir" ]] || return 0
    for f in "$cache_dir"/*_all_blocks.txt; do
        [[ -f "$f" ]] || continue
        protx=$(basename "$f" _all_blocks.txt)
        lines=$(awk -v b="$fb" '$1 < b' "$f" 2>/dev/null || true)
        if [[ -n "$lines" ]]; then
            printf '%s\n' "$lines" >"$f"
        else
            : >"$f"
        fi
        wc -l <"$f" | tr -d '[:space:]' >"${cache_dir}/${protx}_info.txt"
        rm -f "${cache_dir}/${protx}_last_block.json"
    done
}

# {proTx}_last_block.json — время последнего предложенного блока (PE / patch / seed).
write_last_block_meta() {
    local protx="${1,,}" height="$2" timestamp="${3:-}" epoch="${4:-}"
    local save="${SAVE_DIR:-$HOME/tmp/withdrawals}"
    local cache_dir="$save/cache/validators"
    local meta_file="$cache_dir/${protx}_last_block.json"
    local ts_ms="" existing_h=0
    [[ -n "$protx" && "$height" =~ ^[0-9]+$ ]] || return 1
    mkdir -p "$cache_dir"

    if [[ -f "$meta_file" ]]; then
        existing_h=$(jq -r '.height // 0' "$meta_file" 2>/dev/null || echo 0)
        if [[ "$existing_h" =~ ^[0-9]+$ ]] && (( existing_h > height )); then
            return 0
        fi
    fi

    if [[ -n "$timestamp" ]]; then
        ts_ms=$(python3 -c "from datetime import datetime; print(int(datetime.fromisoformat('${timestamp}'.replace('Z','+00:00')).timestamp()*1000))" 2>/dev/null || true)
    fi
    if [[ -z "$ts_ms" || ! "$ts_ms" =~ ^[0-9]+$ ]]; then
        local pe_url="${PLATFORM_EXPLORER_URL:-http://127.0.0.1:3005}"
        local bts
        bts=$(curl -sS --max-time 15 "${pe_url%/}/blocks?height_min=${height}&height_max=${height}&limit=1" 2>/dev/null \
            | jq -r '.resultSet[0].header.timestamp // empty')
        if [[ -n "$bts" ]]; then
            ts_ms=$(python3 -c "from datetime import datetime; print(int(datetime.fromisoformat('${bts}'.replace('Z','+00:00')).timestamp()*1000))" 2>/dev/null || true)
        fi
    fi
    [[ -n "$ts_ms" && "$ts_ms" =~ ^[0-9]+$ ]] || return 1

    jq -n --argjson h "$height" --argjson t "$ts_ms" \
        --argjson e "$([[ -n "$epoch" && "$epoch" =~ ^[0-9]+$ ]] && echo "$epoch" || echo null)" \
        '{height: $h, timestamp_ms: $t, epoch: (if $e == null then null else $e end)}' >"$meta_file"
}
