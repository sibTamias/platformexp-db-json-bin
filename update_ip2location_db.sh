#!/bin/bash
# Скачивание IP2Location LITE (ZIP) и DB-IP ASN (MMDB) на platformExp (161.97.96.43).
# Token: IP2LOCATION_DOWNLOAD_TOKEN в ~/bin/.env (см. .env.example).
# Результат: IP2LOCATION_STAGING_DIR (по умолчанию ~/tmp/ip2location).
#
#   ./update_ip2location_db.sh --force
#   ./update_ip2location_db.sh --dry-run
#   ./update_ip2location_db.sh --only db11|asn|dbip|all
#
# После загрузки: ./transfer_ip2location_db.sh  или  ./update_and_transfer_ip2location_db.sh

set -euo pipefail

export TZ="${TZ:-Asia/Irkutsk}"
export PATH="${HOME}/local/usr/bin:/usr/bin:/bin:${PATH:-}"

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"

TARGET_DIR="${IP2LOCATION_STAGING_DIR:-${IP2LOCATION_DB_DIR:-$HOME/tmp/ip2location}}"
LOG_DIR="${LOG_DIR:-$HOME/logs}"
LOG_FILE="$LOG_DIR/ip2location_update.log"
DOWNLOAD_BASE="https://www.ip2location.com/download"
DBIP_DOWNLOAD_BASE="${DBIP_DOWNLOAD_BASE:-https://download.db-ip.com/free}"

ONLY="all"
DRY_RUN=0
FORCE=0

usage() {
    cat <<'EOF'
update_ip2location_db.sh — загрузка IP2Location LITE на platformExp

  --only db11|asn|dbip|all   что скачивать (default: all)
  --dry-run             только показать план
  --force               без подтверждения (cron)
  -h, --help

Token: IP2LOCATION_DOWNLOAD_TOKEN в ~/bin/.env
Коды IP2Location: DB11LITEBIN, DBASNLITEBIN, DBASNLITEMMDB
DB-IP: dbip-asn-lite-YYYY-MM.mmdb (без token; override: DBIP_ASN_LITE_URL)
EOF
}

log() {
    local msg="[$(date '+%Y-%m-%d %H:%M:%S')] $*"
    echo "$msg"
    mkdir -p "$LOG_DIR"
    echo "$msg" >> "$LOG_FILE"
}

load_token() {
    if [[ -n "${IP2LOCATION_DOWNLOAD_TOKEN:-}" ]]; then
        TOKEN="$IP2LOCATION_DOWNLOAD_TOKEN"
        return 0
    fi
    echo "✗ IP2LOCATION_DOWNLOAD_TOKEN не задан. Добавьте в $BIN/.env" >&2
    exit 1
}

download_url() {
    local code="$1"
    printf '%s?token=%s&file=%s' "$DOWNLOAD_BASE" "$TOKEN" "$code"
}

extract_zip() {
    local zip="$1" dest="$2"
    if command -v unzip >/dev/null 2>&1; then
        unzip -q "$zip" -d "$dest"
        return $?
    fi
    if python3 -c "import zipfile" 2>/dev/null; then
        python3 -c "import zipfile, sys; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])" "$zip" "$dest"
        return $?
    fi
    log "✗ Нужен unzip или python3+zipfile"
    return 1
}

is_zip_file() {
    local zip="$1"
    [[ -f "$zip" ]] || return 1
    [[ "$(wc -c < "$zip" | tr -d ' ')" -ge 10000 ]] || return 1
    [[ "$(head -c 2 "$zip" | od -An -tx1 | tr -d ' \n')" == "504b" ]]
}

use_existing_staging() {
    local code="$1"
    local dest="$2"
    local expected="$3"
    local src="$dest/$expected"
    local size

    [[ -f "$src" ]] || return 1
    size="$(wc -c < "$src" | tr -d ' ')"
    if [[ "$size" -lt 1000000 ]]; then
        return 1
    fi
    log "⚠ $code: лимит/ошибка загрузки IP2Location — оставляем существующий $src ($size bytes)"
    return 0
}

install_from_zip() {
    local code="$1"
    local expected="$2"
    local dest="$3"
    local url tmpdir zip extracted src backup size

    url="$(download_url "$code")"
    log "=== $code → $dest/$expected ==="

    if [[ "$DRY_RUN" -eq 1 ]]; then
        echo "  URL: ${url/token=*/token=***}"
        echo "  dest: $dest/$expected"
        return 0
    fi

    tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/ip2loc.XXXXXX")"
    zip="$tmpdir/archive.zip"

    if ! curl -fSL --retry 3 --retry-delay 5 -o "$zip" "$url"; then
        rm -rf "$tmpdir"
        if use_existing_staging "$code" "$dest" "$expected"; then
            return 0
        fi
        log "✗ Ошибка загрузки $code"
        return 1
    fi

    if ! is_zip_file "$zip"; then
        local api_msg
        api_msg="$(tr -d '\0' < "$zip" | head -c 160)"
        rm -rf "$tmpdir"
        if use_existing_staging "$code" "$dest" "$expected"; then
            return 0
        fi
        log "✗ $code: ответ IP2Location не ZIP (${api_msg:-empty})"
        return 1
    fi

    if ! extract_zip "$zip" "$tmpdir/extracted"; then
        rm -rf "$tmpdir"
        if use_existing_staging "$code" "$dest" "$expected"; then
            return 0
        fi
        log "✗ Ошибка распаковки $code"
        return 1
    fi

    extracted="$(find "$tmpdir/extracted" -type f -name "$expected" | head -1)"
    if [[ -z "$extracted" ]]; then
        extracted="$(find "$tmpdir/extracted" -type f \( -name '*.BIN' -o -name '*.MMDB' \) ! -name 'README*' ! -name 'LICENSE*' | head -1)"
    fi
    if [[ -z "$extracted" || ! -f "$extracted" ]]; then
        log "✗ В архиве $code не найден $expected"
        find "$tmpdir/extracted" -type f >> "$LOG_FILE" 2>/dev/null || true
        rm -rf "$tmpdir"
        return 1
    fi

    size="$(wc -c < "$extracted" | tr -d ' ')"
    if [[ "$size" -lt 1000000 ]]; then
        log "✗ Подозрительно маленький файл ($size bytes) для $code"
        rm -rf "$tmpdir"
        return 1
    fi

    mkdir -p "$dest"
    src="$dest/$expected"
    if [[ -f "$src" ]]; then
        backup="${src}.backup.$(date +%Y%m%d_%H%M%S)"
        cp -a "$src" "$backup"
        log "  backup: $backup"
    fi

    cp -a "$extracted" "$src"
    chmod 644 "$src"
    log "✓ Установлено: $src ($size bytes)"

    rm -rf "$tmpdir"
    return 0
}

dbip_year_month_candidates() {
    python3 - <<'PY'
import datetime
today = datetime.date.today()
for off in range(4):
    m = today.month - off
    y = today.year
    while m < 1:
        m += 12
        y -= 1
    print(f"{y:04d}-{m:02d}")
PY
}

install_dbip_asn() {
    local dest="$1"
    local ym base gz mmdb url tmpdir size old

    if [[ -n "${DBIP_ASN_LITE_URL:-}" ]]; then
        url="$DBIP_ASN_LITE_URL"
        base="$(basename "$url")"
        base="${base%.gz}"
        log "=== DB-IP ASN (DBIP_ASN_LITE_URL) → $dest/$base ==="
    else
        url=""
        base=""
        while IFS= read -r ym; do
            [[ -n "$ym" ]] || continue
            base="dbip-asn-lite-${ym}.mmdb"
            url="${DBIP_DOWNLOAD_BASE}/${base}.gz"
            if curl -fsI --retry 2 --retry-delay 3 "$url" >/dev/null 2>&1; then
                break
            fi
            url=""
        done < <(dbip_year_month_candidates)

        if [[ -z "$url" ]]; then
            log "✗ DB-IP ASN: не найден релиз (проверены последние 4 месяца)"
            return 1
        fi
        log "=== DB-IP ASN $base → $dest/$base ==="
    fi

    if [[ "$DRY_RUN" -eq 1 ]]; then
        echo "  URL: $url"
        echo "  dest: $dest/$base"
        return 0
    fi

    tmpdir="$(mktemp -d "${TMPDIR:-/tmp}/dbip.XXXXXX")"
    gz="$tmpdir/archive.gz"

    if ! curl -fSL --retry 3 --retry-delay 5 -o "$gz" "$url"; then
        rm -rf "$tmpdir"
        log "✗ Ошибка загрузки DB-IP ASN"
        return 1
    fi

    if ! gunzip -c "$gz" > "$tmpdir/$base"; then
        rm -rf "$tmpdir"
        log "✗ Ошибка распаковки DB-IP ASN"
        return 1
    fi

    size="$(wc -c < "$tmpdir/$base" | tr -d ' ')"
    if [[ "$size" -lt 1000000 ]]; then
        log "✗ Подозрительно маленький DB-IP файл ($size bytes)"
        rm -rf "$tmpdir"
        return 1
    fi

    mkdir -p "$dest"
    mmdb="$dest/$base"
    if [[ -f "$mmdb" ]]; then
        cp -a "$mmdb" "${mmdb}.backup.$(date +%Y%m%d_%H%M%S)"
    fi

    cp -a "$tmpdir/$base" "$mmdb"
    chmod 644 "$mmdb"
    log "✓ Установлено: $mmdb ($size bytes)"

    for old in "$dest"/dbip-asn-lite-*.mmdb; do
        [[ -e "$old" ]] || continue
        [[ "$old" == "$mmdb" ]] && continue
        rm -f "$old"
        log "  removed old staging: $(basename "$old")"
    done

    rm -rf "$tmpdir"
    return 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --only) ONLY="${2:-}"; shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        --force) FORCE=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *) echo "Неизвестный аргумент: $1" >&2; usage >&2; exit 1 ;;
    esac
done

case "$ONLY" in
    db11|asn|dbip|all) ;;
    *) echo "✗ --only: db11, asn, dbip или all" >&2; exit 1 ;;
esac

if [[ "$ONLY" == "all" || "$ONLY" == "db11" || "$ONLY" == "asn" ]]; then
    load_token
fi

if [[ "$DRY_RUN" -eq 0 && "$FORCE" -eq 0 && ! -t 0 ]]; then
    FORCE=1
fi

if [[ "$DRY_RUN" -eq 0 && "$FORCE" -eq 0 ]]; then
    echo "Staging: $TARGET_DIR"
    read -p "Скачать geo-базы (only=$ONLY)? (yes/no): " confirm
    [[ "$confirm" == "yes" ]] || { echo "Отменено."; exit 1; }
fi

log "=========================================="
log "Geo DB download (only=$ONLY, staging=$TARGET_DIR)"

errors=0

if [[ "$ONLY" == "all" || "$ONLY" == "db11" ]]; then
    install_from_zip "DB11LITEBIN" "IP2LOCATION-LITE-DB11.BIN" "$TARGET_DIR" || errors=$((errors + 1))
fi

if [[ "$ONLY" == "all" || "$ONLY" == "asn" ]]; then
    install_from_zip "DBASNLITEBIN" "IP2LOCATION-LITE-ASN.BIN" "$TARGET_DIR/IP2LOCATION-LITE-ASN.BIN" || errors=$((errors + 1))
    install_from_zip "DBASNLITEMMDB" "IP2LOCATION-LITE-ASN.MMDB" "$TARGET_DIR/IP2LOCATION-LITE-ASN.MMDB" || errors=$((errors + 1))
fi

if [[ "$ONLY" == "all" || "$ONLY" == "dbip" ]]; then
    install_dbip_asn "$TARGET_DIR" || errors=$((errors + 1))
fi

[[ "$DRY_RUN" -eq 1 ]] && exit 0

if [[ "$errors" -gt 0 ]]; then
    log "✗ Ошибок: $errors"
    exit 1
fi

log "✓ Загрузка завершена → $TARGET_DIR"
log "Дальше: $BIN/transfer_ip2location_db.sh"
exit 0
