#!/bin/bash
# Cron на platformExp: скачать IP2Location и раздать на 46.19.66.201 + 161.97.100.254.
# Token в ~/bin/.env → IP2LOCATION_DOWNLOAD_TOKEN
#
#   0 3 2 * * /home/mno/bin/update_and_transfer_ip2location_db.sh >> /home/mno/logs/ip2location_update.log 2>&1

set -euo pipefail

export PATH="${HOME}/local/usr/bin:/usr/bin:/bin:${PATH:-}"

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=========================================="
echo "IP2Location update+transfer $(date)"

"$BIN/update_ip2location_db.sh" --force "$@"
"$BIN/transfer_ip2location_db.sh"

echo "Done $(date)"
