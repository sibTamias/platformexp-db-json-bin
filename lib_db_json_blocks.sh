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
