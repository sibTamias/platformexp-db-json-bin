#!/bin/bash
# Отправка db.json и proTxEvoNodeAll.txt на сервер(ы) evowatch (источник: 161.97.96.43).
# Список хостов: transfer_db.conf (TARGET_SERVERS) или по умолчанию.

export TZ=Asia/Irkutsk

SOURCE_FILE="${SOURCE_FILE:-/home/mno/tmp/withdrawals/db.json}"
PROTX_FILE="${PROTX_FILE:-/home/mno/tmp/withdrawals/proTxEvoNodeAll.txt}"
LOG_FILE="${LOG_FILE:-/home/mno/tmp/withdrawals/transfer.log}"
# По умолчанию только evowatch.ru (201). dashnodes.net (254) — live-patch локально, без SCP.
TARGET_SERVERS="${TARGET_SERVERS:-46.19.66.201}"
REMOTE_USER="${REMOTE_USER:-mno}"
SCP_RETRY_ATTEMPTS="${SCP_RETRY_ATTEMPTS:-5}"
SCP_RETRY_DELAY_SEC="${SCP_RETRY_DELAY_SEC:-5}"

# Первый контакт с хостом: принять ключ; не интерактивно.
SCP_SSH_OPTS=(
    -o BatchMode=yes
    -o ConnectTimeout=30
    -o StrictHostKeyChecking=accept-new
)

CONFIG_FILE="/home/mno/bin/transfer_db.conf"
if [[ -f "$CONFIG_FILE" ]]; then
    # shellcheck source=/dev/null
    source "$CONFIG_FILE"
fi

echo "[$(date +'%Y-%m-%d %H:%M:%S')] Starting $0 with args: $*" >> /home/mno/tmp/withdrawals/detailed.log

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

ensure_host_key() {
    local host="$1"
    mkdir -p "$HOME/.ssh"
    chmod 700 "$HOME/.ssh"
    touch "$HOME/.ssh/known_hosts"
    chmod 600 "$HOME/.ssh/known_hosts"
    if ! ssh-keygen -F "$host" -f "$HOME/.ssh/known_hosts" >/dev/null 2>&1; then
        log "Adding $host to known_hosts (ssh-keyscan)..."
        ssh-keyscan -H "$host" 2>/dev/null >>"$HOME/.ssh/known_hosts" || true
    fi
}

scp_with_retry() {
    local src="$1" dest="$2"
    local attempt=1 out status
    while (( attempt <= SCP_RETRY_ATTEMPTS )); do
        out=$(scp "${SCP_SSH_OPTS[@]}" "$src" "$dest" 2>&1)
        status=$?
        if [[ $status -eq 0 ]]; then
            printf '%s' "$out"
            return 0
        fi
        log "SCP attempt $attempt/$SCP_RETRY_ATTEMPTS failed for $dest: $out"
        (( attempt < SCP_RETRY_ATTEMPTS )) && sleep "$SCP_RETRY_DELAY_SEC"
        attempt=$((attempt + 1))
    done
    printf '%s' "$out"
    return 1
}

ssh_remote() {
    ssh "${SCP_SSH_OPTS[@]}" "$@"
}

log "Starting database transfer (targets: $TARGET_SERVERS)"

if [[ ! -f "$SOURCE_FILE" ]]; then
    log "ERROR: Source file $SOURCE_FILE not found!"
    exit 1
fi

if [[ ! -f "$PROTX_FILE" ]]; then
    log "WARNING: ProTx file $PROTX_FILE not found! Continuing without it..."
fi

for TARGET_SERVER in $TARGET_SERVERS; do
    log "Transferring files to $TARGET_SERVER"
    ensure_host_key "$TARGET_SERVER"

    scp_output=$(scp_with_retry "$SOURCE_FILE" "${REMOTE_USER}@${TARGET_SERVER}:$HOME/db.tmp")
    scp_status=$?

    if [[ $scp_status -ne 0 ]]; then
        log "ERROR: SCP transfer failed for db.json to $TARGET_SERVER! Details:"
        echo "$scp_output" | tee -a "$LOG_FILE"
        continue
    fi

    if [[ -f "$PROTX_FILE" ]]; then
        scp_output_protx=$(scp_with_retry "$PROTX_FILE" "${REMOTE_USER}@${TARGET_SERVER}:$HOME/proTxEvoNodeAll.tmp")
        scp_status_protx=$?

        if [[ $scp_status_protx -ne 0 ]]; then
            log "ERROR: SCP transfer failed for proTxEvoNodeAll.txt to $TARGET_SERVER! Details:"
            echo "$scp_output_protx" | tee -a "$LOG_FILE"
        else
            log "proTxEvoNodeAll.txt transferred successfully to $TARGET_SERVER"
        fi
    fi

    log "Executing remote commands on $TARGET_SERVER"
    remote_commands=$(ssh_remote "${REMOTE_USER}@${TARGET_SERVER}" "
        mkdir -p '$HOME/WebMuxValidator'
        mv -v '$HOME/db.tmp' '$HOME/db.json'
        if [[ -f '$HOME/proTxEvoNodeAll.tmp' ]]; then
            mv -v '$HOME/proTxEvoNodeAll.tmp' '$HOME/WebMuxValidator/proTxEvoNodeAll.txt'
        fi
        chmod 644 '$HOME/db.json'
        [[ -f '$HOME/WebMuxValidator/proTxEvoNodeAll.txt' ]] && chmod 644 '$HOME/WebMuxValidator/proTxEvoNodeAll.txt'
    " 2>&1)

    remote_status=$?
    echo "Remote commands on $TARGET_SERVER status: $remote_status" | tee -a "$LOG_FILE"
    echo "$remote_commands" | tee -a "$LOG_FILE"

    if [[ $remote_status -ne 0 ]]; then
        log "WARNING: Remote commands on $TARGET_SERVER failed with status $remote_status"
    else
        log "Completed successfully for $TARGET_SERVER"
    fi
done

log "Database and ProTx files transfer finished (targets: $TARGET_SERVERS)"
