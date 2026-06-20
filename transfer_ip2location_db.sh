#!/bin/bash
# Раздача IP2Location баз с platformExp (96.43) на серверы GeoDashboard.
# Источник: IP2LOCATION_STAGING_DIR (~/tmp/ip2location).
# Цель на каждом хосте: REMOTE_GEO_DB_DIR (~/bin/db).

set -euo pipefail

export TZ="${TZ:-Asia/Irkutsk}"

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"

SOURCE_DIR="${IP2LOCATION_STAGING_DIR:-$HOME/tmp/ip2location}"
REMOTE_DB_DIR="${REMOTE_GEO_DB_DIR:-/home/mno/bin/db}"
LOG_FILE="${LOG_FILE:-$HOME/logs/ip2location_transfer.log}"
TARGET_SERVERS="${TARGET_SERVERS:-46.19.66.201 161.97.100.254}"
REMOTE_USER="${REMOTE_USER:-mno}"
RSYNC_RETRY_ATTEMPTS="${RSYNC_RETRY_ATTEMPTS:-5}"
RSYNC_RETRY_DELAY_SEC="${RSYNC_RETRY_DELAY_SEC:-10}"
# После rsync: пересчёт geo/Redis на серверах сайта (--update-all --force)
REFRESH_GEO_CACHE_AFTER_TRANSFER="${REFRESH_GEO_CACHE_AFTER_TRANSFER:-1}"
REMOTE_GEO_LOG="${REMOTE_GEO_LOG:-/home/mno/logs/geo_cache_update.log}"

CONFIG_FILE="${TRANSFER_IP2LOCATION_CONF:-$BIN/transfer_ip2location.conf}"
[[ -f "$CONFIG_FILE" ]] && source "$CONFIG_FILE"

SSH_OPTS=(
    -o BatchMode=yes
    -o ConnectTimeout=30
    -o StrictHostKeyChecking=accept-new
)
RSYNC_SSH="ssh ${SSH_OPTS[*]}"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

ensure_host_key() {
    local host="$1"
    mkdir -p "$HOME/.ssh"
    chmod 700 "$HOME/.ssh"
    touch "$HOME/.ssh/known_hosts"
    chmod 600 "$HOME/.ssh/known_hosts"
    if ! ssh-keygen -F "$host" -f "$HOME/.ssh/known_hosts" >/dev/null 2>&1; then
        log "Adding $host to known_hosts..."
        ssh-keyscan -H "$host" 2>/dev/null >>"$HOME/.ssh/known_hosts" || true
    fi
}

required_files=(
    "$SOURCE_DIR/IP2LOCATION-LITE-DB11.BIN"
    "$SOURCE_DIR/IP2LOCATION-LITE-ASN.BIN/IP2LOCATION-LITE-ASN.BIN"
    "$SOURCE_DIR/IP2LOCATION-LITE-ASN.MMDB/IP2LOCATION-LITE-ASN.MMDB"
)

log "IP2Location transfer: $SOURCE_DIR → targets: $TARGET_SERVERS ($REMOTE_DB_DIR)"

for f in "${required_files[@]}"; do
    if [[ ! -f "$f" ]]; then
        log "ERROR: нет файла $f — сначала запустите update_ip2location_db.sh"
        exit 1
    fi
done

rsync_with_retry() {
    local host="$1"
    local remote="${REMOTE_USER}@${host}:${REMOTE_DB_DIR}/"
    local attempt=1 out status
    while (( attempt <= RSYNC_RETRY_ATTEMPTS )); do
        out=$(rsync -avz -e "$RSYNC_SSH" "$SOURCE_DIR/" "$remote" 2>&1)
        status=$?
        if [[ $status -eq 0 ]]; then
            printf '%s\n' "$out"
            return 0
        fi
        log "rsync attempt $attempt/$RSYNC_RETRY_ATTEMPTS failed for $host: $out"
        (( attempt < RSYNC_RETRY_ATTEMPTS )) && sleep "$RSYNC_RETRY_DELAY_SEC"
        attempt=$((attempt + 1))
    done
    printf '%s\n' "$out"
    return 1
}

refresh_geo_cache_remote() {
    local host="$1"
    [[ "${REFRESH_GEO_CACHE_AFTER_TRANSFER}" == "1" ]] || return 0

    log "Geo cache --force on $host (background, log: $REMOTE_GEO_LOG)"
    ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${host}" bash -s <<REMOTE
set -e
mkdir -p "\$(dirname "$REMOTE_GEO_LOG")"
if pgrep -f "python3.*geolocation_hybrid.py --update-all --force" >/dev/null 2>&1; then
    echo "[\$(date '+%F %T')] skip: geolocation_hybrid --force already running" >> "$REMOTE_GEO_LOG"
    exit 0
fi
nohup bash -c '
    echo "=== IP2Location post-transfer --force \$(date) ===" >> "$REMOTE_GEO_LOG"
    cd "$REMOTE_DB_DIR"
    nice -n 19 ionice -c 3 python3 geolocation_hybrid.py --update-all --force --quiet >> "$REMOTE_GEO_LOG" 2>&1
    redis-cli -n 1 del geo_dashboard_stats >/dev/null 2>&1 || true
    echo "=== IP2Location post-transfer done \$(date) ===" >> "$REMOTE_GEO_LOG"
' >> /home/mno/logs/ip2location_geo_refresh.log 2>&1 &
echo "started pid \$!"
REMOTE
}

errors=0
for TARGET_SERVER in $TARGET_SERVERS; do
    log "Transfer to $TARGET_SERVER"
    ensure_host_key "$TARGET_SERVER"

    ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${TARGET_SERVER}" \
        "mkdir -p '$REMOTE_DB_DIR/IP2LOCATION-LITE-ASN.BIN' '$REMOTE_DB_DIR/IP2LOCATION-LITE-ASN.MMDB'"

    if rsync_with_retry "$TARGET_SERVER"; then
        log "✓ $TARGET_SERVER OK"
        ssh "${SSH_OPTS[@]}" "${REMOTE_USER}@${TARGET_SERVER}" \
            "chmod 644 '$REMOTE_DB_DIR/IP2LOCATION-LITE-DB11.BIN' \
                '$REMOTE_DB_DIR/IP2LOCATION-LITE-ASN.BIN/IP2LOCATION-LITE-ASN.BIN' \
                '$REMOTE_DB_DIR/IP2LOCATION-LITE-ASN.MMDB/IP2LOCATION-LITE-ASN.MMDB' 2>/dev/null || true"
        refresh_geo_cache_remote "$TARGET_SERVER"
    else
        log "✗ $TARGET_SERVER FAILED"
        errors=$((errors + 1))
    fi
done

if [[ "$errors" -gt 0 ]]; then
    log "Finished with $errors error(s)"
    exit 1
fi

log "✓ IP2Location bases sent to all targets"
if [[ "${REFRESH_GEO_CACHE_AFTER_TRANSFER}" == "1" ]]; then
    log "Geo --force refresh queued on site servers (see $REMOTE_GEO_LOG on each host)"
fi
exit 0
