#!/bin/bash
# generate_db_json_local.sh — формирование db.json с ЛОКАЛЬНЫМ platform-explorer
# Версия: 2026-02-27 (см. CHANGELOG_generate_db_json_local.md)
#
# Для сервера, где установлен platform-explorer (API на localhost:3005).
# Быстрее и не зависит от внешних серверов (platform-explorer.pshenmic.dev).
#
# Отличия от generate_db_json_test.sh:
#   - PLATFORM_EXPLORER_URL по умолчанию http://localhost:3005
#   - Проверка dashd: также учитывает dashmate (Core в Docker)
#
# В .env задай (рядом со скриптом: /home/mno/bin/.env, см. bin/.env.example):
#   PLATFORM_EXPLORER_URL=http://localhost:3005
#   DASH_CLI — dashmate exec не работает, используй dash_cli_rpc.sh (RPC через curl)
#   SAVE_DIR=/home/mno/tmp/withdrawals
#   PARALLEL_JOBS=8 — число параллельных задач (по умолчанию 8)
#   CURL_WITH_RETRY_MAX_TIME=20 — сек. --max-time в curl_with_retry (по умолчанию 20)
#   CURL_WITH_RETRY_ATTEMPTS=5 — попыток в curl_with_retry (по умолчанию 5)
#   CURL_WITH_RETRY_TRACE=1 — на каждую попытку curl_with_retry в diag: CURL_TRACE + CURL_TRACE_CMD
#                          (готовая команда curl) + тело между CURL_TRACE_BODY_BEGIN/END (до 64 KiB);
#                          на stderr — команда и укороченное тело. RETRY_LIVE/RETRY_FULL для этих вызовов отключены.
#   DB_JSON_TRUNCATE_DIAG_ON_START=1 — в начале прогона обнулить generate_db_json_diag.log,
#                          recover_list.txt, recover_failed.txt (удобно перед ручной отладкой; cron — не задавать).
#   DB_JSON_DIAG_QUIET=1 — 1: в generate_db_json_diag.log только RETRY/FAIL/HTTP и т.д.;
#                          0: плюс строки SECTION (границы этапов). По умолчанию 1, если не задано.
#   SKIP_EXPLORER_DASHMATE_HEIGHT_CHECK=1 — не вызывать сверку высоты API с dashmate (см. check_platform_explorer_vs_dashmate.sh).
#
# Диагностика — один файл: $SAVE_DIR/generate_db_json_diag.log
#   Как в старых service_ip_issues + api_request_empty: поля validator=/reason=/context=/url=/detail=
#   В конце строки после «|» — краткое пояснение по-русски.
# Тест на N случайных нод: ./generate_db_json_local.sh --test [N]  (см. --help)
#
# Скрипт выполняется на 161.97.96.43 (platform-explorer, здесь собирается db.json). Сервер сайта
# 161.97.100.254 использует уже сгенерированный db.json — не этот скрипт. Деплой скрипта: push на 96.43.
#
# set -x

export TZ=Asia/Irkutsk
export LC_ALL=en_US.UTF-8
# Отсчёт общего времени: от этой точки до конца скрипта (перед «Время выполнения»)
start_time=$(date +%s)
REBUILD_TOTAL_SEC=0
GENERATE_TOTAL_SEC=0

BIN="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
[[ -r "$BIN/.env" ]] && source "$BIN/.env"
DATA_DIR="${DATA_DIR:-$BIN/data}"
SAVE_DIR="${SAVE_DIR:-/home/mno/tmp/withdrawals}"

# Локальный platform-explorer API (порт 3005 — из server.js)
PLATFORM_EXPLORER_URL="${PLATFORM_EXPLORER_URL:-http://localhost:3005}"

# DASH_CLI: для dashmate — dashmate exec dash_core dash-cli
# для отдельного dashd — sudo -u dash01 /opt/dash/bin/dash-cli
DASH_CLI="${DASH_CLI:-dashmate exec dash_core dash-cli}"

# Ручной тест на подмножестве нод (--test [N]): не пересекается с cron, если cron вызывает скрипт без аргументов.
TEST_RANDOM_LIMIT=0
# Инкрементальное обновление db.json при той же эпохе (см. --incremental-v1).
INCREMENTAL_V1=0
# С opt --fresh-logs или DB_JSON_TRUNCATE_DIAG_ON_START=1 — обнулить diag/recover в начале прогона.
FRESH_LOGS=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --fresh-logs)
            FRESH_LOGS=1
            shift
            ;;
        --incremental-v1)
            INCREMENTAL_V1=1
            shift
            ;;
        --test)
            if [[ "${2:-}" =~ ^[0-9]+$ ]]; then
                TEST_RANDOM_LIMIT="$2"
                shift 2
            else
                TEST_RANDOM_LIMIT=10
                shift
            fi
            ;;
        --help|-h)
            cat <<'USAGE'
Использование: generate_db_json_local.sh [опции]

  --test [N]   Режим проверки: не вызывать platform_validators_list (долгий опрос API
              списка страниц). Берётся N случайных proTxHash из уже существующего файла
              all_validators_list.txt в SAVE_DIR (по умолчанию N=10).
              Перед первым --test один раз запустите скрипт без --test или скопируйте список.

  --incremental-v1
              Быстрый режим при неизменной эпохе: не выполнять rebuild_arrays и полный
              generate_json_db. Нужны готовые $SAVE_DIR/db.json (current_epoch = API),
              tmp/epoch_bounds.txt, epoch_intervals.txt, epoch_blocks_count_L1/L2.txt после
              полного прогона. Обновляет по каждому валидатору: identityBalance, rating,
              строку withdrawals для текущей эпохи (blocks, withdrawal, validator_credits_value).
              Скорость: две фазы идут параллельно батчами по PARALLEL_JOBS (как rebuild_arrays);
              при PARALLEL_JOBS=1 будет очень долго на сотнях валидаторов.

  --fresh-logs Обнулить в начале прогона $SAVE_DIR/generate_db_json_diag.log и tmp/recover_*.txt
              (эквивалент DB_JSON_TRUNCATE_DIAG_ON_START=1). Файлы вроде /tmp/run.txt скрипт не трогает.

  --help, -h  Эта справка.

Переменные окружения (см. .env и bin/.env.example):
  VALIDATOR_HASH=…     — один валидатор (имеет приоритет над --test).
  LIMIT_VALIDATORS=N  — первые N из полного списка (не используется при --test).
  DB_JSON_DIAG_QUIET=0|1 — подробность generate_db_json_diag.log (0 = писать SECTION).
  CURL_WITH_RETRY_MAX_TIME=20 — секунды --max-time для curl в curl_with_retry (по умолчанию 20).
  CURL_WITH_RETRY_ATTEMPTS=5 — число попыток curl_with_retry (по умолчанию 5; RECOVER — CURL_WITH_RETRY_RECOVER_ATTEMPTS=6).
  CURL_WITH_RETRY_TRACE=1 — по каждой попытке в diag: готовая строка curl + тело ответа (см. шапку скрипта).
  DB_JSON_TRUNCATE_DIAG_ON_START=1 — обнулить diag/recover перед прогоном (см. --fresh-logs).

Перед работой (если в PATH есть dashmate): сверка .api.block.height с нодой; при отставании
  эксплорера скрипт завершится с ошибкой. Обойти: SKIP_EXPLORER_DASHMATE_HEIGHT_CHECK=1 в .env.
  Ручная проверка: ./check_platform_explorer_vs_dashmate.sh

Чтобы не мешать cron:
  • Закомментируйте строку в crontab: crontab -e
  • Или запускайте тест в минуту, когда cron не срабатывает.

Перед тестом без заливки на другой сервер: в .env можно SKIP_TRANSFER=1 (если обёртка это учитывает).

Пример:
  /home/mno/bin/generate_db_json_local.sh --test 10
USAGE
            exit 0
            ;;
        *)
            echo "Неизвестный аргумент: $1 (см. $0 --help)" >&2
            exit 1
            ;;
    esac
done

###############################################################################
#                              Configuration                                  #
###############################################################################

mkdir -p "$SAVE_DIR"
# Меньше страниц -> меньше запросов -> не упираемся в 429 Too Many Requests
LIMIT=100
BLOCK_REWARD_L1=0.49787579
PLATFORM_VALIDATORS_LIST_FILE="$SAVE_DIR/all_validators_list.txt"
LAST_META_FILE="$SAVE_DIR/last_metadata.txt"
VALIDATORS_FILE="$SAVE_DIR/validators.txt"
IDENTITIES_FILE="$SAVE_DIR/identities.txt"
IDENTITYBALANCE_FILE="$SAVE_DIR/identityBalance.txt"
WITHDRAWAL_TABLE_FILE="$SAVE_DIR/withdrawal_table.txt"
CUR_EPOCH_FILE="$SAVE_DIR/cur_epoch.txt"
CUR_EPOCH_BLOCKS_FILE="$SAVE_DIR/cur_epoch_blocks.txt"
LIST_BLOCKS_FILE="$SAVE_DIR/listProposedBlocks.txt"
CONFIRMED_BLOCKS_PER_EPOCH_FILE="$SAVE_DIR/confirmed_blocks_per_epoch.txt"
CUR_EPOCH_BLOCKS_PER_VALIDATOR_FILE="$SAVE_DIR/cur_epoch_blocks_per_validator.txt"
RATING_FILE="$SAVE_DIR/validator_ratings.txt"
JSON_FILE="$SAVE_DIR/db.json"
EPOCH_BLOCKS_COUNT_L1_FILE="$SAVE_DIR/epoch_blocks_count_L1.txt"
EPOCH_BLOCKS_COUNT_L2_FILE="$SAVE_DIR/epoch_blocks_count_L2.txt"
# Единый лог (раньше было два файла): пустые HTTP/JSON, IP/service, границы этапов.
DB_JSON_BUILD_DIAG_LOG="${SAVE_DIR}/generate_db_json_diag.log"
DB_JSON_BUILD_DIAG_LOCK="${SAVE_DIR}/tmp/generate_db_json_diag.lock"
DB_JSON_RECOVER_LIST="${SAVE_DIR}/tmp/recover_list.txt"
DB_JSON_RECOVER_FAILED="${SAVE_DIR}/tmp/recover_failed.txt"
# 1 = в diag.log не писать SECTION (только RETRY/RECOVER/FAIL и сводки). 0 = как раньше.
DB_JSON_DIAG_QUIET="${DB_JSON_DIAG_QUIET:-1}"

# Одна строка: убрать переводы строк и лишние пробелы.
_diag_sanitize_line() {
    local s="$1"
    s="${s//$'\r'/ }"
    s="${s//$'\n'/ }"
    s="${s//$'\t'/ }"
    while [[ "$s" == *"  "* ]]; do s="${s//  / }"; done
    printf '%s' "${s# }"
}

log_diag_append() {
    local line="$1"
    line=$(_diag_sanitize_line "$line")
    local logf="$DB_JSON_BUILD_DIAG_LOG"
    local lock="$DB_JSON_BUILD_DIAG_LOCK"
    mkdir -p "$(dirname "$logf")" "$(dirname "$lock")"
    [[ ${#line} -gt 2400 ]] && line="${line:0:2400}…"
    (
        flock 400
        printf '%s\n' "$line" >>"$logf"
    ) 400>"$lock"
}

# Полный ответ HTTP (RETRY_FULL): три записи в diag — BEGIN сразу виден в tail -f, затем тело, END.
log_diag_retry_full_block() {
    local ctx="$1" validator="$2" url="$3" required="$4" resp="$5" validate_ok="$6"
    local logf="$DB_JSON_BUILD_DIAG_LOG"
    local lock="$DB_JSON_BUILD_DIAG_LOCK"
    local body meta bytes
    bytes=$(printf '%s' "${resp:-}" | wc -c | tr -d ' ')
    body=$(printf '%s' "${resp:-}" | head -c 65536)
    meta="$(date -Is) RETRY_FULL_BEGIN ctx=${ctx} validator=${validator} validate_ok=${validate_ok} body_bytes=${bytes} url=${url} jq_required=${required:-_none_}"
    meta=$(_diag_sanitize_line "$meta")
    mkdir -p "$(dirname "$logf")" "$(dirname "$lock")"
    (
        flock 400
        printf '%s\n' "$meta"
    ) 400>"$lock"
    (
        flock 400
        printf '%s\n' "$body"
    ) 400>"$lock"
    (
        flock 400
        printf '%s\n' "$(date -Is) RETRY_FULL_END ctx=${ctx} validator=${validator}"
    ) 400>"$lock"
}

# При CURL_WITH_RETRY_TRACE=1 — человекочитаемая трассировка: команда curl и тело ответа на каждую попытку.
log_curl_trace_attempt() {
    local attempt="$1" ctx="$2" validator="$3" url="$4" required="$5" resp="$6" validate_ok="$7"
    [[ "${CURL_WITH_RETRY_TRACE:-0}" != "1" ]] && return 0
    local logf="$DB_JSON_BUILD_DIAG_LOG" lock="$DB_JSON_BUILD_DIAG_LOCK"
    local ts max="${CURL_WITH_RETRY_MAX_TIME:-20}" bytes body cmd_line stderr_body
    ts=$(date -Is)
    bytes=$(printf '%s' "${resp:-}" | wc -c | tr -d ' ')
    body=$(printf '%s' "${resp:-}" | head -c 65536)
    cmd_line="curl -sS --max-time ${max} $(printf '%q' "$url")"
    ctx=$(_diag_sanitize_line "$ctx")
    validator=$(_diag_sanitize_line "$validator")
    local req_disp="${required:-_none_}"
    req_disp=$(_diag_sanitize_line "$req_disp")
    (
        flock 400
        printf '%s\n' "${ts} CURL_TRACE attempt=${attempt} ctx=${ctx} validator=${validator} validate_ok=${validate_ok} body_bytes=${bytes} jq_required=${req_disp}"
        printf '%s\n' "${ts} CURL_TRACE_CMD ${cmd_line}"
        printf '%s\n' "${ts} CURL_TRACE_BODY_BEGIN"
        printf '%s\n' "$body"
        printf '%s\n' "$(date -Is) CURL_TRACE_BODY_END attempt=${attempt}"
    ) 400>"$lock"
    stderr_body=$(printf '%s' "${resp:-}" | head -c 1200)
    printf '%s\n' "${ts} CURL_TRACE attempt=${attempt} ctx=${ctx} validator=${validator} validate_ok=${validate_ok} body_bytes=${bytes}" >&2
    printf '%s\n' "${ts} CURL_TRACE_CMD ${cmd_line}" >&2
    if [[ "$bytes" -gt 1200 ]]; then
        printf '%s\n' "${ts} CURL_TRACE_BODY (первые 1200 байт, полностью в diag.log):" >&2
        printf '%s\n' "$stderr_body" >&2
        printf '%s\n' "… [ещё $((bytes - 1200)) байт → $logf]" >&2
    else
        printf '%s\n' "${ts} CURL_TRACE_BODY:" >&2
        printf '%s\n' "$stderr_body" >&2
    fi
}

# Граница этапа (как раньше по смыслу — отдельные блоки в логе).
log_diag_section() {
    [[ "${DB_JSON_DIAG_QUIET:-0}" == "1" ]] && return 0
    local title_ru="$1"
    local kv="${2:-}"
    title_ru=$(_diag_sanitize_line "$title_ru")
    kv=$(_diag_sanitize_line "$kv")
    local ts="$(date -Is)"
    local line="${ts} SECTION | ${title_ru}"
    [[ -n "$kv" ]] && line+=" | ${kv}"
    (
        flock 400
        {
            printf '\n'
            printf '%s\n' "$line"
        } >>"$DB_JSON_BUILD_DIAG_LOG"
    ) 400>"$DB_JSON_BUILD_DIAG_LOCK"
}

# Формат как в старом service_ip_issues.log + пояснение после |
log_diag_ip() {
    local validator="$1" reason="$2" detail="${3:-}" msg_ru="$4"
    validator=$(_diag_sanitize_line "$validator")
    detail=$(_diag_sanitize_line "$detail")
    msg_ru=$(_diag_sanitize_line "$msg_ru")
    local ts="$(date -Is)"
    local line="${ts} IP validator=${validator} reason=${reason}"
    [[ -n "$detail" ]] && line+=" detail=${detail}"
    line+=" | ${msg_ru}"
    log_diag_append "$line"
}

# Формат как в старом api_request_empty.log: kind HTTP|JSON, context=, url=, хвост extra, затем | пояснение
log_diag_req() {
    local kind="$1" context="$2" url="$3" extra="${4:-}" msg_ru="$5"
    context=$(_diag_sanitize_line "$context")
    url=$(_diag_sanitize_line "$url")
    extra=$(_diag_sanitize_line "$extra")
    msg_ru=$(_diag_sanitize_line "$msg_ru")
    local ts="$(date -Is)"
    local line="${ts} ${kind} context=${context} url=${url}"
    [[ -n "$extra" ]] && line+=" ${extra}"
    line+=" | ${msg_ru}"
    log_diag_append "$line"
}

log_api_empty_response() {
    local context="$1" url="$2" extra="${3:-}"
    local kind="HTTP"
    local msg=""
    case "$context" in
        GET_status)
            if [[ "$extra" == *empty_http* ]]; then
                msg="Пустое тело ответа на GET /status — platform-explorer недоступен или вернул пустоту."
            else
                kind="JSON"
                msg="В ответе GET /status нет номера текущей эпохи (ожидались epoch.number, epochs.current или data.epoch.number)."
            fi
            ;;
        GET_epoch)
            msg="Пустое тело ответа на GET /epoch/{N} — нет данных эпохи в JSON."
            ;;
        GET_validator_blocks_meta)
            msg="Пустой ответ на запрос метаданных блоков валидатора (/validator/…/blocks)."
            ;;
        jq_empty)
            kind="JSON"
            msg="После разбора JSON ожидаемое поле пустое или null — см. detail (field=…)."
            ;;
        GET_validator_blocks_page)
            msg="Пустой ответ на постраничный запрос блоков валидатора."
            ;;
        GET_validators)
            msg="Пустой ответ на GET /validators (индекс списка валидаторов)."
            ;;
        GET_validators_page)
            msg="Пустой ответ на одну страницу GET /validators?page=…"
            ;;
        GET_validators_page_retry)
            msg="После повтора запроса страницы /validators ответ по-прежнему пустой."
            ;;
        GET_validator)
            msg="Пустой ответ на GET /validator/{proTxHash}."
            ;;
        GET_status_epoch_bounds)
            msg="Пустой GET /status при расчёте границ эпох (нужна высота блока api.block.height)."
            ;;
        GET_epoch_interval)
            msg="Пустой GET /epoch/{N} при построении файла интервалов эпох."
            ;;
        GET_epoch_interval_next)
            msg="Пустой GET /epoch/{N+1} для подстановки времени конца интервала эпохи."
            ;;
        GET_identity_withdrawals)
            msg="Пустой ответ или ошибка HTTP на GET /identity/{id}/withdrawals (используется curl -sf)."
            ;;
        *)
            msg="Зафиксировано пустое или неожиданное значение при обращении к platform-explorer API."
            ;;
    esac
    log_diag_req "$kind" "$context" "$url" "$extra" "$msg"
}

mkdir -p "$SAVE_DIR/cache/epochs"
mkdir -p "$SAVE_DIR/cache/validators"
mkdir -p "$SAVE_DIR/cache/dash"
mkdir -p "$SAVE_DIR/tmp"
mkdir -p "$SAVE_DIR/tmp/rebuild_parallel"
mkdir -p "$SAVE_DIR/tmp/generate_parallel"

# Перед новым прогоном: пустой diag + recover (ручная отладка). В cron обычно не включают.
if [[ "$FRESH_LOGS" == "1" || "${DB_JSON_TRUNCATE_DIAG_ON_START:-0}" == "1" ]]; then
    : >"$DB_JSON_BUILD_DIAG_LOG"
    : >"$DB_JSON_RECOVER_LIST"
    : >"$DB_JSON_RECOVER_FAILED"
fi

# Параллелизм: по умолчанию = число ядер CPU (nproc), можно задать PARALLEL_JOBS в .env
NPROC=$(nproc 2>/dev/null)
[[ -z "$NPROC" || "$NPROC" -lt 1 ]] && NPROC=8
PARALLEL_JOBS="${PARALLEL_JOBS:-$NPROC}"
[[ "$PARALLEL_JOBS" -lt 1 ]] && PARALLEL_JOBS=1

# Таймаут curl в curl_with_retry (GET validator / withdrawals и т.д.). Было 6 — мало для тяжёлых ответов.
CURL_WITH_RETRY_MAX_TIME="${CURL_WITH_RETRY_MAX_TIME:-20}"
[[ "$CURL_WITH_RETRY_MAX_TIME" =~ ^[0-9]+$ ]] || CURL_WITH_RETRY_MAX_TIME=20
[[ "$CURL_WITH_RETRY_MAX_TIME" -lt 1 ]] && CURL_WITH_RETRY_MAX_TIME=20

# Число попыток curl_with_retry (по умолчанию 5; при RECOVER_MODE — 6).
CURL_WITH_RETRY_ATTEMPTS="${CURL_WITH_RETRY_ATTEMPTS:-5}"
[[ "$CURL_WITH_RETRY_ATTEMPTS" =~ ^[0-9]+$ ]] || CURL_WITH_RETRY_ATTEMPTS=5
[[ "$CURL_WITH_RETRY_ATTEMPTS" -lt 1 ]] && CURL_WITH_RETRY_ATTEMPTS=5
CURL_WITH_RETRY_RECOVER_ATTEMPTS="${CURL_WITH_RETRY_RECOVER_ATTEMPTS:-6}"
[[ "$CURL_WITH_RETRY_RECOVER_ATTEMPTS" =~ ^[0-9]+$ ]] || CURL_WITH_RETRY_RECOVER_ATTEMPTS=6

if [[ -z "${SKIP_EXPLORER_DASHMATE_HEIGHT_CHECK:-}" ]]; then
    if [[ -f "$BIN/check_platform_explorer_vs_dashmate.sh" ]]; then
        bash "$BIN/check_platform_explorer_vs_dashmate.sh" || {
            echo "generate_db_json_local.sh: остановка из-за рассинхрона explorer/dashmate (см. выше)." >&2
            exit 1
        }
    fi
fi

log_diag_section "Старт скрипта, единый лог" "PLATFORM_EXPLORER_URL=$PLATFORM_EXPLORER_URL SAVE_DIR=$SAVE_DIR"

###############################################################################
#                    Cache Functions                                          #
###############################################################################

# Получить текущую эпоху из /status. Поддержка разных форматов API.
get_cur_epoch_from_status() {
    local status_json
    status_json=$(curl -sX GET "$PLATFORM_EXPLORER_URL/status")
    if [[ -z "${status_json//[$'\t\r\n ']}" ]]; then
        log_api_empty_response "GET_status" "$PLATFORM_EXPLORER_URL/status" "empty_http_body"
    fi
    local epoch
    epoch=$(echo "$status_json" | jq -r '.epoch.number // .epochs.current // .data.epoch.number // empty')
    if [[ -z "$epoch" || "$epoch" == "null" ]]; then
        if [[ -n "${status_json//[$'\t\r\n ']}" ]]; then
            log_api_empty_response "jq_empty" "$PLATFORM_EXPLORER_URL/status" "field=epoch.number|epochs.current|data.epoch.number"
        fi
        echo "ОШИБКА: /status не вернул номер эпохи. Ответ API:" >&2
        echo "$status_json" | jq . 2>/dev/null || echo "$status_json" >&2
        echo "Проверьте: curl -s http://localhost:3005/status | jq ." >&2
        return 1
    fi
    echo "$epoch"
}

get_epoch_first_block_height() {
    echo "$1" | jq -r '.epoch.firstBlockHeight // empty'
}
get_epoch_first_core_block_height() {
    echo "$1" | jq -r '.epoch.firstCoreBlockHeight // empty'
}

get_epoch_data() {
    local epoch=$1
    local cache_dir="$SAVE_DIR/cache/epochs"
    local cache_file="$cache_dir/epoch_${epoch}.json"
    mkdir -p "$cache_dir"
    if [[ -f "$cache_file" && $epoch -ne $curEpoch ]]; then
        cat "$cache_file"
        return
    fi
    local data
    data=$(curl -sX GET "$PLATFORM_EXPLORER_URL/epoch/$epoch")
    if [[ -z "${data//[$'\t\r\n ']}" ]]; then
        log_api_empty_response "GET_epoch" "$PLATFORM_EXPLORER_URL/epoch/$epoch" "epoch=$epoch empty_http_body"
    fi
    echo "$data" > "$cache_file"
    echo "$data"
}

# Кэш DASH_CLI — timestamp блока по высоте (getblockhash + getblock)
cached_block_timestamp() {
    local height=$1
    local cache_file="$SAVE_DIR/cache/dash/block_ts_${height}.txt"
    if [[ -f "$cache_file" ]]; then
        local cached=$(cat "$cache_file")
        # 0 мог остаться от старого запуска до починки RPC — не доверяем, перезапрашиваем
        if [[ -n "$cached" && "$cached" =~ ^[0-9]+$ && "$cached" -gt 0 ]]; then
            echo "$cached"
            return
        fi
    fi
    local hash ts
    hash=$($DASH_CLI getblockhash "$height" 2>/dev/null | tr -d '\r"')
    if [[ -z "$hash" || "$hash" == *"error"* ]]; then
        [[ -n "${DEBUG:-}" ]] && echo "  [DEBUG] cached_block_timestamp height=$height: getblockhash вернул пусто или error" >&2
        echo "0"
        return
    fi
    ts=$($DASH_CLI getblock "$hash" 2>/dev/null | jq -r '.time // .mediantime // 0')
    if [[ -n "${VERBOSE:-}" && ("$ts" == "0" || -z "$ts") ]]; then
        echo "  [Проверь RPC] height=$height hash=$hash: getblock .time=$ts. Убедись: DASH_RPC_URL, DASH_RPC_USER, DASH_RPC_PASS в .env; dash_cli_rpc.sh getblock использует verbosity 2." >&2
    fi
    [[ -n "$ts" && "$ts" =~ ^[0-9]+$ && "$ts" -gt 0 ]] && echo "$ts" > "$cache_file"
    echo "${ts:-0}"
}

get_validator_blocks() {
    local validator=$1
    local cache_dir="$SAVE_DIR/cache/validators"
    local cache_file="$cache_dir/${validator}_all_blocks.txt"
    local cache_info_file="$cache_dir/${validator}_info.txt"
    local limit=100
    mkdir -p "$cache_dir"
    local current_total
    local blocks_meta
    blocks_meta=$(curl -sS --max-time "$CURL_WITH_RETRY_MAX_TIME" -X GET "$PLATFORM_EXPLORER_URL/validator/${validator}/blocks?")
    if [[ -z "${blocks_meta//[$'\t\r\n ']}" ]]; then
        log_api_empty_response "GET_validator_blocks_meta" "$PLATFORM_EXPLORER_URL/validator/${validator}/blocks?" "validator=$validator empty_http_body"
    fi
    current_total=$(echo "$blocks_meta" | jq -r '.pagination.total // 0')
    if [[ -z "$current_total" || "$current_total" == "null" ]]; then
        log_api_empty_response "jq_empty" "$PLATFORM_EXPLORER_URL/validator/${validator}/blocks?" "validator=$validator field=pagination.total"
        current_total=0
    fi
    local cached_total="" cached_lines=""
    if [[ -f "$cache_info_file" && -f "$cache_file" ]]; then
        cached_total=$(tr -d '\r\n ' <"$cache_info_file")
        cached_lines=$(wc -l <"$cache_file" | tr -d '\r\n ')
        # Раньше сравнивали только total — при неполной выгрузке страниц в кэше на строку меньше, чем в API (часто «−1 блок»).
        if [[ "$cached_total" =~ ^[0-9]+$ && "$cached_lines" =~ ^[0-9]+$ && "$cached_total" -eq "$current_total" && "$cached_total" -ne 0 && "$cached_lines" -eq "$current_total" ]]; then
            cat "$cache_file"
            return
        fi
    fi
    local temp_file="$SAVE_DIR/tmp/${validator}_blocks.txt"
    local tail_file="$SAVE_DIR/tmp/${validator}_blocks_tail.txt"
    # Инкремент: полный кэш (строк == старый total), API total вырос — догружаем с последней затронутой страницы, merge sort|uniq.
    if [[ -f "$cache_file" && -f "$cache_info_file" && "$cached_total" =~ ^[0-9]+$ && "$cached_lines" =~ ^[0-9]+$ && "$current_total" =~ ^[0-9]+$ \
        && "$cached_total" -gt 0 && "$cached_lines" -eq "$cached_total" && "$current_total" -gt "$cached_total" ]]; then
        local numPage_new j_start inc_lines inc_extra gj p_url p_body
        numPage_new=$(( (current_total + limit - 1) / limit ))
        j_start=$(( (cached_total + limit - 1) / limit ))
        >"$tail_file"
        for (( gj=j_start; gj <= numPage_new; gj++ )); do
            p_url="${PLATFORM_EXPLORER_URL}/validator/${validator}/blocks?page=$gj&limit=100&order=asc"
            p_body=$(curl -sS --max-time "$CURL_WITH_RETRY_MAX_TIME" -X GET "$p_url")
            if [[ -z "${p_body//[$'\t\r\n ']}" ]]; then
                log_api_empty_response "GET_validator_blocks_page" "$p_url" "validator=$validator page=$gj empty_http_body"
            fi
            echo "$p_body" | jq -r '(.resultSet // [])[].header.height' >> "$tail_file"
        done
        sort -n "$cache_file" "$tail_file" | uniq >"$temp_file"
        inc_lines=$(wc -l <"$temp_file" | tr -d '\r\n ')
        inc_extra=0
        while [[ "$current_total" =~ ^[0-9]+$ && "$inc_lines" -lt "$current_total" && $inc_extra -lt 40 ]]; do
            inc_extra=$((inc_extra + 1))
            gj=$((numPage_new + inc_extra))
            p_url="${PLATFORM_EXPLORER_URL}/validator/${validator}/blocks?page=$gj&limit=100&order=asc"
            p_body=$(curl -sS --max-time "$CURL_WITH_RETRY_MAX_TIME" -X GET "$p_url")
            [[ -n "${p_body//[$'\t\r\n ']}" ]] && echo "$p_body" | jq -r '(.resultSet // [])[].header.height' >>"$temp_file"
            sort -n "$temp_file" | uniq >"${temp_file}.sorted" && mv "${temp_file}.sorted" "$temp_file"
            inc_lines=$(wc -l <"$temp_file" | tr -d '\r\n ')
        done
        if [[ "$inc_lines" -eq "$current_total" ]]; then
            [[ "$inc_extra" -gt 0 ]] && log_diag_append "$(date -Is) GET_validator_blocks extra_pages validator=$validator extra=$inc_extra total=$current_total lines=$inc_lines"
            log_diag_append "$(date -Is) GET_validator_blocks incremental validator=$validator cached_total=$cached_total current_total=$current_total pages=${j_start}-${numPage_new} extra_tail=${inc_extra}"
            cp "$temp_file" "$cache_file"
            echo "$current_total" >"$cache_info_file"
            cat "$temp_file"
            return
        fi
        # Несовпадение после merge — полная перезагрузка ниже
    fi
    > "$temp_file"
    local numPage=0 j page_url page_body line_count extra
    if [[ "$current_total" =~ ^[0-9]+$ ]] && (( current_total > 0 )); then
        numPage=$(( (current_total + limit - 1) / limit ))
    fi
    for (( j=1; j <= numPage; j++ )); do
        page_url="${PLATFORM_EXPLORER_URL}/validator/${validator}/blocks?page=$j&limit=100&order=asc"
        page_body=$(curl -sS --max-time "$CURL_WITH_RETRY_MAX_TIME" -X GET "$page_url")
        if [[ -z "${page_body//[$'\t\r\n ']}" ]]; then
            log_api_empty_response "GET_validator_blocks_page" "$page_url" "validator=$validator page=$j empty_http_body"
        fi
        echo "$page_body" | jq -r '(.resultSet // [])[].header.height' >> "$temp_file"
    done
    line_count=$(wc -l <"$temp_file" | tr -d '\r\n ')
    extra=0
    while [[ "$current_total" =~ ^[0-9]+$ && "$line_count" -lt "$current_total" && $extra -lt 40 ]]; do
        extra=$((extra + 1))
        j=$((numPage + extra))
        page_url="${PLATFORM_EXPLORER_URL}/validator/${validator}/blocks?page=$j&limit=100&order=asc"
        page_body=$(curl -sS --max-time "$CURL_WITH_RETRY_MAX_TIME" -X GET "$page_url")
        [[ -n "${page_body//[$'\t\r\n ']}" ]] && echo "$page_body" | jq -r '(.resultSet // [])[].header.height' >> "$temp_file"
        line_count=$(wc -l <"$temp_file" | tr -d '\r\n ')
    done
    if [[ "$extra" -gt 0 ]]; then
        log_diag_append "$(date -Is) GET_validator_blocks extra_pages validator=$validator extra=$extra total=$current_total lines=$line_count"
    fi
    cp "$temp_file" "$cache_file"
    echo "$current_total" > "$cache_info_file"
    cat "$temp_file"
}

###############################################################################
#                  Проверка доступности Dash Core / dashmate                  #
###############################################################################

dashd_available=1
dashuser=$(ps -o user= -C dashd | head -n1 | tr -d '[:space:]')
# dashmate: Core в Docker, dashd не виден через ps
if [[ -z "$dashuser" ]]; then
    if $DASH_CLI getblockcount &>/dev/null; then
        echo "Dash Core доступен через DASH_CLI (dashmate или RPC)"
    else
        echo -e "\n⚠️  dashd/dashmate не отвечает! Таблица без APY данных."
        dashd_available=0
    fi
fi

###############################################################################
#                   Функции получения списков валидаторов                    #
###############################################################################

arrayValidators() {
    all_protx_list_registered=$($DASH_CLI protx list registered 1 2>/dev/null)
    > "$SAVE_DIR/proTxEvoNodeAll.txt"
    jq -r '.[] | select(.state.PoSeBanHeight == -1 and .type == "Evo") | .proTxHash' \
        <<< "$all_protx_list_registered" > "$SAVE_DIR/proTxEvoNodeAll.txt"
}

arrayValidators

platform_validators_list() {
    mkdir -p "$SAVE_DIR"
    touch "$PLATFORM_VALIDATORS_LIST_FILE"
    last_page=0
    last_total=0
    [[ -f "$LAST_META_FILE" ]] && read last_page last_total < "$LAST_META_FILE"
    current_data=$(curl -sX GET "$PLATFORM_EXPLORER_URL/validators")
    if [[ -z "${current_data//[$'\t\r\n ']}" ]]; then
        log_api_empty_response "GET_validators" "$PLATFORM_EXPLORER_URL/validators" "empty_http_body"
    fi
    new_total=$(jq -r '.pagination.total // empty' <<< "$current_data")
    if [[ -z "$new_total" || "$new_total" == "null" ]]; then
        log_api_empty_response "jq_empty" "$PLATFORM_EXPLORER_URL/validators" "field=pagination.total"
    fi
    new_page=$(( (new_total + LIMIT - 1) / LIMIT ))
    echo "Validators на ${PLATFORM_EXPLORER_URL#*://} : $new_total (было $last_total)"
    if (( new_total <= last_total )); then
        echo "Нет изменений. Total: $new_total"
        return
    fi
    tmp_new=$(mktemp)
    if (( last_page > 0 )); then
        echo "Проверка страницы $last_page..."
        _vurl="${PLATFORM_EXPLORER_URL}/validators?page=$last_page&limit=$LIMIT"
        _vbody=$(curl -sX GET "$_vurl")
        if [[ -z "${_vbody//[$'\t\r\n ']}" ]]; then
            log_api_empty_response "GET_validators_page" "$_vurl" "page=$last_page empty_http_body"
        fi
        echo "$_vbody" |
        jq -r '(.resultSet // [])[].proTxHash' | tr '[:upper:]' '[:lower:]' |
        while read -r hash; do
            [[ -n "$hash" ]] && { grep -qFxm 1 "$hash" "$PLATFORM_VALIDATORS_LIST_FILE" 2>/dev/null || echo "$hash"; }
        done >> "$tmp_new"
    fi
    if (( new_page > last_page )); then
        echo "Обработка новых страниц $((last_page + 1))-$new_page"
        for (( p=last_page + 1; p <= new_page; p++ )); do
            [[ $p -gt $((last_page + 1)) ]] && sleep 10
            echo "  page=$p"
            resp=$(curl -sX GET "$PLATFORM_EXPLORER_URL/validators?page=$p&limit=$LIMIT")
            if [[ -z "${resp//[$'\t\r\n ']}" ]]; then
                log_api_empty_response "GET_validators_page" "$PLATFORM_EXPLORER_URL/validators?page=$p&limit=$LIMIT" "page=$p empty_http_body"
            fi
            if ! echo "$resp" | jq -e '.resultSet != null' >/dev/null 2>&1; then
                err=$(echo "$resp" | jq -r '.error // empty')
                echo "$resp" > "$SAVE_DIR/tmp/validators_page_${p}_null.json"
                echo "  ВНИМАНИЕ: page=$p — ответ без resultSet (error: ${err:-—}), пауза 2 сек и повтор" >&2
                sleep 2
                resp=$(curl -sX GET "$PLATFORM_EXPLORER_URL/validators?page=$p&limit=$LIMIT")
                if [[ -z "${resp//[$'\t\r\n ']}" ]]; then
                    log_api_empty_response "GET_validators_page_retry" "$PLATFORM_EXPLORER_URL/validators?page=$p&limit=$LIMIT" "page=$p empty_http_body_after_retry"
                fi
                if ! echo "$resp" | jq -e '.resultSet != null' >/dev/null 2>&1; then
                    echo "$resp" > "$SAVE_DIR/tmp/validators_page_${p}_null.json"
                    echo "  ВНИМАНИЕ: page=$p после повтора всё ещё без resultSet" >&2
                fi
            fi
            sleep 0.3
            echo "$resp" | jq -r '(.resultSet // [])[].proTxHash' | tr '[:upper:]' '[:lower:]' |
            while read -r hash; do
                [[ -n "$hash" ]] && { grep -qFxm 1 "$hash" "$PLATFORM_VALIDATORS_LIST_FILE" 2>/dev/null || echo "$hash"; }
            done >> "$tmp_new"
        done
    fi
    if [[ -s "$tmp_new" ]]; then
        cat "$tmp_new" >> "$PLATFORM_VALIDATORS_LIST_FILE"
        echo "Добавлено: $(wc -l < "$tmp_new")"
    else
        echo "Новых уникальных хэшей не найдено"
    fi
    echo "$new_page $new_total" > "$LAST_META_FILE"
    rm "$tmp_new"
}

if [[ "$INCREMENTAL_V1" -eq 1 ]]; then
    echo "[incremental-v1] пропуск platform_validators_list (список из $PLATFORM_VALIDATORS_LIST_FILE)."
elif [[ "$TEST_RANDOM_LIMIT" -gt 0 ]]; then
    echo "Режим --test: пропуск platform_validators_list (список с API не обновляется)."
    echo "  Случайных валидаторов: $TEST_RANDOM_LIMIT из $PLATFORM_VALIDATORS_LIST_FILE"
else
    platform_validators_list
fi

VALIDATORS=()
# Один валидатор по хэшу (файл списка не нужен): VALIDATOR_HASH=8117f7bf... ./generate_db_json_local.sh
if [[ -n "${VALIDATOR_HASH:-}" ]]; then
    [[ "$TEST_RANDOM_LIMIT" -gt 0 ]] && echo "Предупреждение: задан VALIDATOR_HASH — опция --test игнорируется." >&2
    h=$(echo "${VALIDATOR_HASH}" | tr '[:upper:]' '[:lower:]')
    VALIDATORS=("$h")
    echo "Режим одного валидатора: ${VALIDATORS[0]:0:16}..."
    echo "  [файл] вывод в те же каталоги: $SAVE_DIR/tmp/rebuild_parallel/, $SAVE_DIR/tmp/generate_parallel/, итог $SAVE_DIR/db.json"
elif [[ "$TEST_RANDOM_LIMIT" -gt 0 ]]; then
    if [[ ! -s "$PLATFORM_VALIDATORS_LIST_FILE" ]]; then
        echo "Ошибка: --test требует непустой файл $PLATFORM_VALIDATORS_LIST_FILE" >&2
        echo "  Запустите скрипт один раз без --test или восстановите список с бэкапа." >&2
        exit 1
    fi
    if ! command -v shuf &>/dev/null; then
        echo "Ошибка: для --test нужна утилита shuf (пакет coreutils)." >&2
        exit 1
    fi
    mapfile -t VALIDATORS < <(shuf -n "$TEST_RANDOM_LIMIT" "$PLATFORM_VALIDATORS_LIST_FILE")
    echo "Режим --test: выбрано ${#VALIDATORS[@]} случайных валидаторов (shuf -n $TEST_RANDOM_LIMIT)."
elif [[ -f "$PLATFORM_VALIDATORS_LIST_FILE" && -s "$PLATFORM_VALIDATORS_LIST_FILE" ]]; then
    mapfile -t VALIDATORS < "$PLATFORM_VALIDATORS_LIST_FILE"
    echo "Загружено ${#VALIDATORS[@]} валидаторов"
    echo "  [файл] список валидаторов: $PLATFORM_VALIDATORS_LIST_FILE"
    if [[ -n "${LIMIT_VALIDATORS:-}" && "${LIMIT_VALIDATORS}" -gt 0 ]]; then
        VALIDATORS=("${VALIDATORS[@]:0:LIMIT_VALIDATORS}")
        echo "Ограничено до ${#VALIDATORS[@]} валидаторов (LIMIT_VALIDATORS=$LIMIT_VALIDATORS)"
    fi
else
    echo "Файл валидаторов пуст или не существует. Задай VALIDATOR_HASH=... для одного валидатора."
    exit 1
fi

[[ "$TEST_RANDOM_LIMIT" -gt 0 && ${#VALIDATORS[@]} -gt 0 ]] && log_diag_section "Режим --test: случайная выборка" "валидаторов=${#VALIDATORS[@]} лимит_shuf=$TEST_RANDOM_LIMIT"

###############################################################################
#                          Data Loading Functions                             #
###############################################################################

save_array() {
    local file="$1"
    shift
    local array=("$@")
    > "$file"
    for element in "${array[@]}"; do echo "$element" >> "$file"; done
}

# Построчно в массив; пустые строки сохраняются. Нельзя VAR=($(…)) через echo —
# пустые элементы теряются, а sed '/^$/d' удалял пустые service → сдвиг IP по строкам.
load_lines_into_array() {
    local -n _lines_dest="$1"
    local file="$2"
    _lines_dest=()
    [[ -f "$file" && -s "$file" ]] || return 0
    mapfile -t _lines_dest < <(tr -d '\r' < "$file")
}

save_arrays_blocks() {
    local file="$1"
    shift
    local arrays=("$@")
    > "$file"
    for array in "${arrays[@]}"; do echo "$array" >> "$file"; done
}

load_lines_into_array SAVED_VALIDATORS "$VALIDATORS_FILE"
load_lines_into_array IDENTITIES "$IDENTITIES_FILE"
load_lines_into_array IDENTITYBALANCE "$IDENTITYBALANCE_FILE"
if (( dashd_available )); then
    load_lines_into_array REGISTERED_TIMES "$SAVE_DIR/registered_times.txt"
else
    REGISTERED_TIMES=()
fi
load_lines_into_array VALIDATOR_RATINGS "$RATING_FILE"

###############################################################################
#                  Main Data Processing / rebuild_arrays                      #
###############################################################################

calculate_epoch_blocks_count_L1() {
    local curEpoch=$1
    echo "Вычисление блоков L1..."
    declare -A epoch_blocks_count_L1
    for ((epoch=1; epoch<curEpoch; epoch++)); do
        epoch_data=$(get_epoch_data $epoch)
        next_epoch_data=$(get_epoch_data $((epoch + 1)))
        first_core_block_height=$(get_epoch_first_core_block_height "$epoch_data")
        next_first_core_block_height=$(get_epoch_first_core_block_height "$next_epoch_data")
        total_blocks_in_epoch_L1=$(( next_first_core_block_height - first_core_block_height ))
        epoch_blocks_count_L1[$epoch]="$total_blocks_in_epoch_L1"
    done
    > "$EPOCH_BLOCKS_COUNT_L1_FILE"
    for epoch in "${!epoch_blocks_count_L1[@]}"; do
        echo "$epoch:${epoch_blocks_count_L1[$epoch]}" >> "$EPOCH_BLOCKS_COUNT_L1_FILE"
    done
    echo "  [файл] блоки L1 по эпохам -> $EPOCH_BLOCKS_COUNT_L1_FILE"
}

calculate_epoch_blocks_count_L2() {
    local curEpoch=$1
    echo "Вычисление блоков L2..."
    if ! [[ "$curEpoch" =~ ^[0-9]+$ ]] || (( curEpoch < 2 )); then return 1; fi
    declare -A epoch_data_first_block
    for ((epoch=1; epoch<=curEpoch; epoch++)); do
        data=$(get_epoch_first_block_height "$(get_epoch_data $epoch)")
        if [[ -z "$data" ]]; then
            log_api_empty_response "jq_empty" "$PLATFORM_EXPLORER_URL/epoch/$epoch" "epoch=$epoch field=epoch.firstBlockHeight"
            echo "ОШИБКА: эпоха $epoch — API не вернул .epoch.firstBlockHeight. Проверь: curl -s $PLATFORM_EXPLORER_URL/epoch/$epoch | jq .epoch" >&2
            return 1
        fi
        epoch_data_first_block[$epoch]=$data
    done
    declare -A epoch_blocks_count_L2
    for ((epoch=1; epoch<curEpoch; epoch++)); do
        a=${epoch_data_first_block[$epoch]}
        b=${epoch_data_first_block[$((epoch+1))]}
        [[ -z "$a" || -z "$b" ]] && continue
        total_epoch_blocks_L2=$((b - a))
        epoch_blocks_count_L2[$epoch]="$total_epoch_blocks_L2"
    done
    > "$EPOCH_BLOCKS_COUNT_L2_FILE"
    for epoch in "${!epoch_blocks_count_L2[@]}"; do
        echo "$epoch:${epoch_blocks_count_L2[$epoch]}" >> "$EPOCH_BLOCKS_COUNT_L2_FILE"
    done
    echo "  [файл] блоки L2 по эпохам -> $EPOCH_BLOCKS_COUNT_L2_FILE"
}

# Проблемы proTxInfo / service / IP в ответе GET /validator/{hash}
log_validator_service_issue() {
    local validator="$1"
    local reason="$2"
    local detail="${3:-}"
    local msg=""
    case "$reason" in
        invalid_json) msg="Ответ /validator/{hash} не является JSON или повреждён." ;;
        missing_proTxInfo) msg="В JSON нет proTxInfo — IP/service из explorer не извлечь (в кошельке protx info может быть в порядке)." ;;
        empty_service) msg="В proTxInfo пустое поле state.service." ;;
        host_empty_after_parse) msg="Не удалось выделить хост из строки service (IPv6 в скобках, host:port)." ;;
        *) msg="Ошибка разбора service/IP валидатора." ;;
    esac
    log_diag_ip "$validator" "$reason" "$detail" "$msg"
}

# Обработка одного валидатора для rebuild_arrays (вызывается в параллельных процессах)
process_validator_rebuild() {
    local i=$1
    local validator=$2
    local blocks_file="$SAVE_DIR/tmp/listProposedBlocks_${i}.txt"
    local out_file="$SAVE_DIR/tmp/rebuild_parallel/validator_${i}.json"
    get_validator_blocks "$validator" > "$blocks_file"
    declare -A epoch_first_blocks epoch_last_blocks
    while IFS=: read -r e first last; do
        [[ -n "$e" && "$e" =~ ^[0-9]+$ ]] && epoch_first_blocks[$e]="$first" && epoch_last_blocks[$e]="$last"
    done < "$SAVE_DIR/tmp/epoch_bounds.txt"
    confirmed_blocks_str=""
    cache_file="$SAVE_DIR/cache/validators/${validator}_confirmed_through_${fixed_max}.txt"
    if [[ $fixed_max -ge 0 && -f "$cache_file" ]]; then
        cached=$(cat "$cache_file")
        confirmed_blocks_str="$cached "
        for (( epoch=$((fixed_max + 1)); epoch < curEpoch; epoch++ )); do
            firstBlockEpoch=${epoch_first_blocks[$epoch]}
            lastBlockEpoch=${epoch_last_blocks[$epoch]}
            blocks_count=$(awk -v b="$firstBlockEpoch" -v c="$lastBlockEpoch" '$1 >= b && $1 < c {count++} END {print count+0}' "$blocks_file")
            confirmed_blocks_str+="$blocks_count "
        done
    else
        for (( epoch=0; epoch < curEpoch; epoch++ )); do
            firstBlockEpoch=${epoch_first_blocks[$epoch]}
            lastBlockEpoch=${epoch_last_blocks[$epoch]}
            blocks_count=$(awk -v b="$firstBlockEpoch" -v c="$lastBlockEpoch" '$1 >= b && $1 < c {count++} END {print count+0}' "$blocks_file")
            confirmed_blocks_str+="$blocks_count "
        done
        if [[ $fixed_max -ge 0 ]]; then
            mkdir -p "$SAVE_DIR/cache/validators"
            echo "$confirmed_blocks_str" | awk -v n=$((fixed_max + 1)) '{for(i=1;i<=n&&i<=NF;i++) printf "%s%s", $i, (i<n?" ":"\n")}' > "$cache_file"
        fi
    fi
    firstBlockEpoch=${epoch_first_blocks[$curEpoch]}
    lastBlockEpoch=${epoch_last_blocks[$curEpoch]}
    current_epoch_blocks=$(awk -v b="$firstBlockEpoch" -v c="$lastBlockEpoch" '$1 >= b && $1 <= c {count++} END {print count+0}' "$blocks_file")
    response=$(curl_with_retry "GET_validator" "$validator" "$PLATFORM_EXPLORER_URL/validator/$validator" '.proTxInfo != null')
    identityBalance=$(echo "$response" | jq -r '.identityBalance // 0')
    identityBalance=$(awk -v ib="${identityBalance:-0}" 'BEGIN {printf "%.4f", (ib+0)/100000000000}')
    identity=$(echo "$response" | jq -r '.identityId // .identity // empty')
    [[ -z "$identity" || "$identity" == "null" ]] && identity=""

    service_raw=$(echo "$response" | jq -r '.proTxInfo.state.service // empty' 2>/dev/null) || service_raw=""
    if [[ -z "${response//[$'\t\r\n ']}" ]]; then
        :
    elif ! echo "$response" | jq -e . &>/dev/null; then
        : # quieted: covered by RETRY/FAIL
    elif ! echo "$response" | jq -e '.proTxInfo != null' &>/dev/null; then
        : # quieted: covered by RETRY/FAIL
    elif [[ -z "$service_raw" || "$service_raw" == "null" ]]; then
        : # quieted: covered by RETRY/FAIL
    fi

    service=$(echo "$response" | jq -r '
      (.proTxInfo.state.service // "") |
      if . == "" or . == null then ""
      elif test("^\\[[^]]+\\]:") then (capture("^\\[(?<h>[^]]+)\\]:") | .h)
      elif test(":[0-9]+$") then sub(":[0-9]+$"; "")
      else . end
    ' 2>/dev/null) || service=""

    if [[ -n "$service_raw" && "$service_raw" != "null" && -z "$service" ]]; then
        : # quieted: covered by RETRY/FAIL
    fi
    registeredHeight=$(echo "$response" | jq -r '.proTxInfo.state.registeredHeight // empty')
    registeredTime=""
    if [[ -n "$registeredHeight" && "$registeredHeight" != "null" ]]; then
        ts=$(cached_block_timestamp "$registeredHeight")
        [[ -n "$ts" && "$ts" =~ ^[0-9]+$ && "$ts" -gt 0 ]] && registeredTime="${ts}000"
    fi
    jq -n \
        --arg validator "$validator" \
        --arg cb "$confirmed_blocks_str" \
        --argjson ceb "$current_epoch_blocks" \
        --arg id "$identity" \
        --arg ib "$identityBalance" \
        --arg svc "$service" \
        --arg rt "$registeredTime" \
        '{validator: $validator, confirmed_blocks: $cb, cur_epoch_blocks: $ceb, identity: $id, identityBalance: $ib, service: $svc, registeredTime: $rt}' > "$out_file"
}


# === curl_with_retry: тихий ретрай для одиночных GET к platform-explorer ===
# При каждой неудачной попытке: RETRY_LIVE в diag + stderr (сразу в tail -f diag.log и в cron.log при 2>&1).
# На 2-й попытке — RETRY_FULL_* (BEGIN/тело/END отдельными flock). При полном FAIL — ещё один RETRY_FULL последнего ответа.
# Короткий RETRY/RECOVER — только при успехе с 3-й+.
# Использование:
#   resp=$(curl_with_retry CTX HASH URL [JQ_REQUIRED])
curl_with_retry() {
    local ctx="$1" validator="$2" url="$3" required="${4:-}"
    local resp="" attempt=0 ok=0 validate_ok=0
    local max="${CURL_WITH_RETRY_ATTEMPTS:-5}" delay=1.0
    local empty_flag jqfail_flag live_line u
    if [[ "${RECOVER_MODE:-0}" == "1" ]]; then
        max="${CURL_WITH_RETRY_RECOVER_ATTEMPTS:-6}"
        delay=3
    fi
    for ((attempt=1; attempt<=max; attempt++)); do
        resp=$(curl -sS --max-time "$CURL_WITH_RETRY_MAX_TIME" "$url" 2>/dev/null)
        validate_ok=0
        if [[ -n "${resp//[$'\t\r\n ']}" ]]; then
            if [[ -z "$required" ]] || echo "$resp" | jq -e "$required" >/dev/null 2>&1; then
                validate_ok=1
            fi
        fi
        log_curl_trace_attempt "$attempt" "$ctx" "$validator" "$url" "$required" "$resp" "$validate_ok"
        if [[ "$validate_ok" -eq 0 ]]; then
            empty_flag=1
            jqfail_flag=0
            [[ -n "${resp//[$'\t\r\n ']}" ]] && empty_flag=0
            if [[ "$empty_flag" -eq 0 && -n "$required" ]]; then
                echo "$resp" | jq -e "$required" >/dev/null 2>&1 || jqfail_flag=1
            fi
            if [[ "${CURL_WITH_RETRY_TRACE:-0}" != "1" ]]; then
                u=$(_diag_sanitize_line "$url")
                live_line="$(date -Is) RETRY_LIVE attempt=${attempt} ctx=${ctx} validator=${validator} empty=${empty_flag} jq_fail=${jqfail_flag} url=${u}"
                log_diag_append "$live_line"
                printf '%s\n' "$live_line" >&2
            fi
        fi
        if [[ "$attempt" -eq 2 && "${CURL_WITH_RETRY_TRACE:-0}" != "1" ]]; then
            log_diag_retry_full_block "$ctx" "$validator" "$url" "$required" "$resp" "$validate_ok"
        fi
        if [[ "$validate_ok" -eq 1 ]]; then
            ok=1
            break
        fi
        sleep "$delay"
    done
    if (( ok )); then
        if [[ "${RECOVER_MODE:-0}" == "1" ]]; then
            (( attempt > 2 )) && log_diag_append "$(date -Is) RECOVER ctx=${ctx} validator=${validator} attempt=${attempt}"
        elif (( attempt > 2 )); then
            log_diag_append "$(date -Is) RETRY ctx=${ctx} validator=${validator} attempt=${attempt}"
        fi
        printf '%s' "$resp"
    else
        if [[ "${RECOVER_MODE:-0}" == "1" ]]; then
            [[ "${CURL_WITH_RETRY_TRACE:-0}" != "1" ]] && log_diag_retry_full_block "$ctx" "$validator" "$url" "$required" "$resp" "0"
            log_diag_append "$(date -Is) FAIL  ctx=${ctx} validator=${validator} attempts=${max}"
            printf '%s\n' "$validator" >> "${DB_JSON_RECOVER_FAILED}"
        else
            [[ "${CURL_WITH_RETRY_TRACE:-0}" != "1" ]] && log_diag_retry_full_block "$ctx" "$validator" "$url" "$required" "$resp" "0"
            (
                flock 401
                printf '%s\t%s\t%s\n' "$ctx" "$validator" "$url" >> "${DB_JSON_RECOVER_LIST}"
            ) 401>"${DB_JSON_RECOVER_LIST}.lock"
        fi
    fi
}



# === recover_pass: второй проход по FAIL-валидаторам текущей стадии ===
# Аргументы:
#   $1 stage_name: 'rebuild_arrays' | 'generate_json_db'
#   $2 func_name : process_validator_rebuild | process_validator_generate
# Читает $DB_JSON_RECOVER_LIST, берёт уникальные validator_hash,
# для каждого находит индекс в $VALIDATORS и перезапускает обработку
# с RECOVER_MODE=1 (4 попытки, sleep 3s). В лог: RETRY/RECOVER(>1)/FAIL + RECOVER_SUMMARY.
recover_pass() {
    local stage="$1" fn="$2"
    [[ -s "${DB_JSON_RECOVER_LIST:-}" ]] || return 0
    local fails_total
    fails_total=$(wc -l < "$DB_JSON_RECOVER_LIST")
    mapfile -t _rec_hashes < <(awk -F'\t' '{print $2}' "$DB_JSON_RECOVER_LIST" | sort -u)
    local uniq=${#_rec_hashes[@]}
    [[ $uniq -eq 0 ]] && return 0
    : > "${DB_JSON_RECOVER_FAILED}"
    log_diag_append "$(date -Is) RECOVER_START stage=${stage} fails=${fails_total} unique=${uniq}"
    : > "$DB_JSON_RECOVER_LIST"
    sleep 8
    export RECOVER_MODE=1
    local h i total=${#VALIDATORS[@]}
    for h in "${_rec_hashes[@]}"; do
        for ((i=0; i<total; i++)); do
            if [[ "${VALIDATORS[$i]}" == "$h" ]]; then
                if [[ "$fn" == "process_validator_rebuild" ]]; then
                    "$fn" "$i" "$h"
                else
                    "$fn" "$i"
                fi
                break
            fi
        done
    done
    unset RECOVER_MODE
    local _rf="${DB_JSON_RECOVER_FAILED}"
    local _fc=0
    [[ -s "$_rf" ]] && _fc=$(wc -l < "$_rf" | tr -d ' ')
    local _ok=$(( uniq - _fc ))
    log_diag_append "$(date -Is) RECOVER_SUMMARY stage=${stage} unique=${uniq} ok=${_ok} fail=${_fc}"
}

rebuild_arrays() {
    local t_start t_end
    t_start=$(date +%s)
    echo "=== rebuild_arrays ==="
    mkdir -p "$SAVE_DIR/tmp" "$SAVE_DIR/tmp/rebuild_parallel"
    > "$CONFIRMED_BLOCKS_PER_EPOCH_FILE"
    > "$CUR_EPOCH_BLOCKS_PER_VALIDATOR_FILE"
    curEpoch=$(get_cur_epoch_from_status) || exit 1
    saved_epoch=""
    [[ -f "$CUR_EPOCH_FILE" ]] && saved_epoch=$(cat "$CUR_EPOCH_FILE")
    echo "$curEpoch" > "$CUR_EPOCH_FILE"
    echo "Текущая эпоха: $curEpoch"
    # Всегда пересчитываем L1/L2 — иначе epoch_block_reward_L2 пустой и validator_credits_value везде 0
    t_l1=$(date +%s)
    calculate_epoch_blocks_count_L1 $curEpoch
    t_l1_end=$(date +%s)
    echo "  [rebuild_arrays] calculate_epoch_blocks_count_L1: $((t_l1_end - t_l1)) сек"
    t_l2=$(date +%s)
    calculate_epoch_blocks_count_L2 $curEpoch
    t_l2_end=$(date +%s)
    echo "  [rebuild_arrays] calculate_epoch_blocks_count_L2: $((t_l2_end - t_l2)) сек"
    IDENTITIES=()
    IDENTITYBALANCE=()
    SERVICES=()
    REGISTERED_TIMES=()
    ALL_CONFIRMED_BLOCKS=()
    ALL_CUR_EPOCH_BLOCKS=()
    VALIDATOR_RATINGS=()
    declare -A epoch_first_blocks
    declare -A epoch_last_blocks
    t_epoch=$(date +%s)
    for epoch in $(seq 0 "$curEpoch"); do
        epoch_data=$(get_epoch_data $epoch)
        epoch_first_blocks[$epoch]=$(get_epoch_first_block_height "$epoch_data")
        if [[ $epoch -eq $curEpoch ]]; then
            st_body=$(curl -sX GET "$PLATFORM_EXPLORER_URL/status")
            if [[ -z "${st_body//[$'\t\r\n ']}" ]]; then
                log_api_empty_response "GET_status_epoch_bounds" "$PLATFORM_EXPLORER_URL/status" "epoch=$curEpoch rebuild_bounds empty_http_body"
            fi
            bh=$(echo "$st_body" | jq -r '.api.block.height // empty')
            if [[ -z "$bh" || "$bh" == "null" ]]; then
                log_api_empty_response "jq_empty" "$PLATFORM_EXPLORER_URL/status" "field=api.block.height epoch=$curEpoch rebuild_bounds"
            fi
            epoch_last_blocks[$epoch]=$bh
        else
            next_epoch_data=$(get_epoch_data $((epoch+1)))
            epoch_last_blocks[$epoch]=$(get_epoch_first_block_height "$next_epoch_data")
        fi
    done
    t_epoch_end=$(date +%s)
    echo "  [rebuild_arrays] epoch_first/last_blocks: $((t_epoch_end - t_epoch)) сек"
    # Эпохи 0..(curEpoch-1) закрыты и не меняются — кэшируем. Только curEpoch пересчитываем.
    fixed_max=$((curEpoch - 1))
    > "$SAVE_DIR/tmp/epoch_bounds.txt"
    for epoch in $(seq 0 "$curEpoch"); do
        echo "${epoch}:${epoch_first_blocks[$epoch]}:${epoch_last_blocks[$epoch]}" >> "$SAVE_DIR/tmp/epoch_bounds.txt"
    done
    echo "  [файл] границы эпох (first/last block) -> $SAVE_DIR/tmp/epoch_bounds.txt"
    export fixed_max curEpoch SAVE_DIR PLATFORM_EXPLORER_URL DASH_CLI
    declare -a validator_blocks_temp=()
    t_validators=$(date +%s)
    total=${#VALIDATORS[@]}
    echo "Обработка $total валидаторов параллельно (PARALLEL_JOBS=$PARALLEL_JOBS, ядер: ${NPROC:-?})..."
    echo "  [процесс] каждый валидатор -> $SAVE_DIR/tmp/rebuild_parallel/validator_N.json (и listProposedBlocks_N.txt, кэш cache/validators/)"
    echo "  [лог] единый файл (поля как раньше + пояснение после |) -> $DB_JSON_BUILD_DIAG_LOG"
    : > "$DB_JSON_RECOVER_LIST" 2>/dev/null
    log_diag_section "Этап rebuild_arrays: параллельная обработка валидаторов" "curEpoch=$curEpoch validators=$total"
    for ((batch_start=0; batch_start < total; batch_start+=PARALLEL_JOBS)); do
        for ((j=0; j < PARALLEL_JOBS && batch_start+j < total; j++)); do
            i=$((batch_start + j))
            process_validator_rebuild $i "${VALIDATORS[$i]}" &
        done
        wait
        batch=$((batch_start + (PARALLEL_JOBS < total - batch_start ? PARALLEL_JOBS : total - batch_start)))
        [[ $((batch % 100)) -eq 0 || $batch -eq $total ]] && echo "    batch $batch/$total"
    done
    echo "  Обработано $total/$total валидаторов"
    recover_pass "rebuild_arrays" process_validator_rebuild
    t_validators_end=$(date +%s)
    echo "  [rebuild_arrays] цикл по валидаторам (параллельно): $((t_validators_end - t_validators)) сек"
    t_merge=$(date +%s)
    echo "  Слияние результатов ($total файлов): читаем $SAVE_DIR/tmp/rebuild_parallel/validator_*.json -> пишем в массивы ниже"
    for ((i=0; i<total; i++)); do
        [[ $(( (i + 1) % 100 )) -eq 0 || $i -eq 0 ]] && echo "    прочитано $((i+1))/$total"
        f="$SAVE_DIR/tmp/rebuild_parallel/validator_${i}.json"
        if [[ ! -f "$f" ]]; then
            echo "ОШИБКА: отсутствует $f для индекса $i" >&2
            exit 1
        fi
        stored_validator=$(jq -r '.validator // empty' "$f")
        expected_validator="${VALIDATORS[$i]}"
        if [[ "$stored_validator" != "$expected_validator" ]]; then
            echo "ОШИБКА: несовпадение валидатора в $f: ожидался $expected_validator, получен $stored_validator" >&2
            exit 1
        fi
        cb=$(jq -r '.confirmed_blocks' "$f")
        ceb=$(jq -r '.cur_epoch_blocks' "$f")
        id=$(jq -r '.identity' "$f")
        [[ -z "$id" || "$id" == "null" ]] && id=""
        ib=$(jq -r '.identityBalance' "$f")
        svc=$(jq -r '.service' "$f")
        rt=$(jq -r '.registeredTime' "$f")
        ALL_CONFIRMED_BLOCKS[$i]="$cb"
        ALL_CUR_EPOCH_BLOCKS[$i]="$ceb"
        IDENTITIES[$i]="$id"
        IDENTITYBALANCE[$i]="$ib"
        SERVICES[$i]="$svc"
        REGISTERED_TIMES[$i]="$rt"
        validator_blocks_temp[$i]="$ceb"
    done
    t_merge_end=$(date +%s)
    echo "  [rebuild_arrays] слияние результатов: $((t_merge_end - t_merge)) сек"
    t_ratings=$(date +%s)
    echo "  Расчёт рейтингов..."
    declare -a indices=($(seq 0 $((${#VALIDATORS[@]}-1))))
    for ((i=0; i<${#indices[@]}; i++)); do
        for ((j=i+1; j<${#indices[@]}; j++)); do
            if [[ ${validator_blocks_temp[${indices[j]}]} -gt ${validator_blocks_temp[${indices[i]}]} ]]; then
                temp=${indices[i]}
                indices[i]=${indices[j]}
                indices[j]=$temp
            fi
        done
    done
    declare -a ratings
    current_rating=1
    current_blocks=-1
    for idx in "${indices[@]}"; do
        blocks=${validator_blocks_temp[$idx]}
        if [[ $blocks -ne $current_blocks ]]; then
            current_rating=$((current_rating + (current_blocks != -1 ? 1 : 0)))
            current_blocks=$blocks
        fi
        ratings[$idx]=$current_rating
    done
    for idx in "${!ratings[@]}"; do VALIDATOR_RATINGS[$idx]=${ratings[$idx]}; done
    t_ratings_end=$(date +%s)
    echo "  [rebuild_arrays] рейтинги: $((t_ratings_end - t_ratings)) сек"
    echo "  Сохранение массивов (процесс -> файл):"
    echo "    confirmed_blocks, cur_epoch_blocks -> $CONFIRMED_BLOCKS_PER_EPOCH_FILE, $CUR_EPOCH_BLOCKS_PER_VALIDATOR_FILE"
    save_arrays_blocks "$CONFIRMED_BLOCKS_PER_EPOCH_FILE" "${ALL_CONFIRMED_BLOCKS[@]}"
    save_arrays_blocks "$CUR_EPOCH_BLOCKS_PER_VALIDATOR_FILE" "${ALL_CUR_EPOCH_BLOCKS[@]}"
    echo "    validators, identities, identityBalance -> $VALIDATORS_FILE, $IDENTITIES_FILE, $IDENTITYBALANCE_FILE"
    save_array "$VALIDATORS_FILE" "${VALIDATORS[@]}"
    save_array "$IDENTITIES_FILE" "${IDENTITIES[@]}"
    save_array "$IDENTITYBALANCE_FILE" "${IDENTITYBALANCE[@]}"
    echo "    registered_times, services, рейтинги -> $SAVE_DIR/registered_times.txt, $SAVE_DIR/services.txt, $RATING_FILE"
    save_array "$SAVE_DIR/registered_times.txt" "${REGISTERED_TIMES[@]}"
    save_array "$SAVE_DIR/services.txt" "${SERVICES[@]}"
    save_array "$RATING_FILE" "${VALIDATOR_RATINGS[@]}"
    t_end=$(date +%s)
    REBUILD_TOTAL_SEC=$((t_end - t_start))
    echo "[rebuild_arrays] ИТОГО: ${REBUILD_TOTAL_SEC} сек"
    echo "Перестроение завершено. Валидаторов: ${#VALIDATORS[@]}"
}

if [[ "$INCREMENTAL_V1" -eq 1 ]]; then
    REBUILD_TOTAL_SEC=0
    echo "[incremental-v1] пропуск rebuild_arrays и перезагрузки массивов из txt после rebuild."
else
    rebuild_arrays
    load_lines_into_array VALIDATORS "$VALIDATORS_FILE"
    load_lines_into_array IDENTITIES "$IDENTITIES_FILE"
    load_lines_into_array IDENTITYBALANCE "$IDENTITYBALANCE_FILE"
    load_lines_into_array VALIDATOR_RATINGS "$RATING_FILE"
    load_lines_into_array SERVICES "$SAVE_DIR/services.txt"
    load_lines_into_array REGISTERED_TIMES "$SAVE_DIR/registered_times.txt"
    ALL_CONFIRMED_BLOCKS=()
    [[ -f "$CONFIRMED_BLOCKS_PER_EPOCH_FILE" && -s "$CONFIRMED_BLOCKS_PER_EPOCH_FILE" ]] && mapfile -t ALL_CONFIRMED_BLOCKS < "$CONFIRMED_BLOCKS_PER_EPOCH_FILE"
    ALL_CUR_EPOCH_BLOCKS=()
    [[ -f "$CUR_EPOCH_BLOCKS_PER_VALIDATOR_FILE" && -s "$CUR_EPOCH_BLOCKS_PER_VALIDATOR_FILE" ]] && mapfile -t ALL_CUR_EPOCH_BLOCKS < "$CUR_EPOCH_BLOCKS_PER_VALIDATOR_FILE"
fi

###############################################################################
#                     Epoch Intervals                                          #
###############################################################################

timestamp_to_epoch() {
    local timestamp="$1"
    unix_time=$(date -d "$timestamp" +%s%3N 2>/dev/null)
    [[ -z "$unix_time" ]] && echo "" && return
    epoch_intervals_file="$SAVE_DIR/epoch_intervals.txt"
    [[ ! -f "$epoch_intervals_file" ]] && echo "" && return
    declare -a epoch_intervals
    mapfile -t epoch_intervals < "$epoch_intervals_file"
    found_epoch=""
    for interval in "${epoch_intervals[@]}"; do
        epoch_number=$(echo "$interval" | awk '{print $2}' | tr -d ':')
        startTime=$(echo "$interval" | awk '{print $3}')
        endTime=$(echo "$interval" | awk '{print $5}')
        if [[ "$startTime" =~ ^[0-9]+$ && "$endTime" =~ ^[0-9]+$ ]]; then
            if (( unix_time >= startTime && unix_time <= endTime )); then
                found_epoch="$epoch_number"
                break
            fi
        fi
    done
    echo "$found_epoch"
}

# Одна строка для эпохи: "Epoch N: startMs - endMs". endMs для эпохи N = startMs эпохи N+1 (берём из API следующей эпохи при пустом)
fetch_epoch_line() {
    local epoch=$1
    local data
    data=$(curl -sX GET "$PLATFORM_EXPLORER_URL/epoch/$epoch")
    if [[ -z "${data//[$'\t\r\n ']}" ]]; then
        log_api_empty_response "GET_epoch_interval" "$PLATFORM_EXPLORER_URL/epoch/$epoch" "epoch=$epoch empty_http_body"
    fi
    local start end
    start=$(echo "$data" | jq -r '.epoch.startTime // empty')
    end=$(echo "$data" | jq -r '.epoch.endTime // empty')
    if [[ -n "${data//[$'\t\r\n ']}" ]] && [[ -z "$start" || "$start" == "null" ]]; then
        log_api_empty_response "jq_empty" "$PLATFORM_EXPLORER_URL/epoch/$epoch" "epoch=$epoch field=epoch.startTime"
    fi
    if [[ -z "$end" || "$end" == "null" ]]; then
        end=$(echo "$data" | jq -r '.nextEpoch.startTime // empty')
    fi
    if [[ -z "$end" || "$end" == "null" ]]; then
        data=$(curl -sX GET "$PLATFORM_EXPLORER_URL/epoch/$((epoch + 1))")
        if [[ -z "${data//[$'\t\r\n ']}" ]]; then
            log_api_empty_response "GET_epoch_interval_next" "$PLATFORM_EXPLORER_URL/epoch/$((epoch + 1))" "for_epoch=$epoch empty_http_body"
        fi
        end=$(echo "$data" | jq -r '.epoch.startTime // empty')
        if [[ -n "${data//[$'\t\r\n ']}" ]] && [[ -z "$end" || "$end" == "null" ]]; then
            log_api_empty_response "jq_empty" "$PLATFORM_EXPLORER_URL/epoch/$((epoch + 1))" "for_epoch=$epoch field=epoch.startTime_fallback"
        fi
    fi
    printf "Epoch %2d: %s - %s\n" "$epoch" "${start:-}" "${end:-}"
}

get_existing_epochs() {
    [[ -f "$1" ]] && awk '{print $2}' "$1" | tr -d ':' | sort -n | uniq || echo ""
}

# Файл интервалов эпох (кэш). При очистке кэша удалять и его: rm -f "$SAVE_DIR/epoch_intervals.txt"
# Формируется один раз за запуск: одна строка на эпоху 1..curEpoch, при повторном формировании старый файл перезаписывается (>).
epoch_intervals_file="$SAVE_DIR/epoch_intervals.txt"
curEpoch_global=$(get_cur_epoch_from_status) || exit 1
need_rebuild=0
if [[ ! -f "$epoch_intervals_file" ]] || [[ $(get_existing_epochs "$epoch_intervals_file" | wc -l) -eq 0 ]]; then
    need_rebuild=1
else
    lastInFile=$(get_existing_epochs "$epoch_intervals_file" | tail -1)
    countInFile=$(get_existing_epochs "$epoch_intervals_file" | wc -l)
    if [[ $curEpoch_global -gt "${lastInFile:-0}" ]] || [[ $countInFile -lt $curEpoch_global ]]; then
        need_rebuild=1
    fi
fi
if [[ $need_rebuild -eq 1 ]]; then
    echo "[этап] интервалы эпох (start/end time): процесс fetch_epoch_line по эпохам -> $epoch_intervals_file"
    log_diag_section "Этап fetch_epoch_line: интервалы эпох" "epochs=1..$curEpoch_global файл=$epoch_intervals_file"
    printf 'Обновление %s (эпохи 1..%s)...\n' "$epoch_intervals_file" "$curEpoch_global"
    > "$epoch_intervals_file"
    for epoch in $(seq 1 "$curEpoch_global"); do
        fetch_epoch_line "$epoch" >> "$epoch_intervals_file"
    done
fi

################################################################################
#                     JSON Database Generation                                 #
################################################################################

# Обработка одного валидатора для generate_json_db (вызывается в параллельных процессах)
process_validator_generate() {
    local i=$1
    local out_file="$SAVE_DIR/tmp/generate_parallel/fragment_${i}.json"
    local validator_hash="${VALIDATORS[$i]}"
    local identity="${IDENTITIES[$i]}"
    [[ -z "$identity" || "$identity" == "null" ]] && identity=""
    local service="${SERVICES[$i]}"
    local identityBalance="${IDENTITYBALANCE[$i]}"
    local registered_time="${REGISTERED_TIMES[$i]}"
    [[ "$registered_time" == "0" || "$registered_time" == "0000" || -z "$registered_time" ]] && registered_time=""
    local rating="${VALIDATOR_RATINGS[$i]}"
    local registration_epoch=""
    if [[ -n "$registered_time" && "$registered_time" =~ ^[0-9]+$ && "$registered_time" -gt 0 ]]; then
        registration_date=$(date -d "@$(($registered_time/1000))" "+%Y-%m-%d %H:%M:%S" 2>/dev/null)
        if [[ -n "$registration_date" && ${#EPOCH_START[@]} -gt 0 ]]; then
            unix_time=$(date -d "$registration_date" +%s%3N 2>/dev/null)
            for idx in "${!EPOCH_START[@]}"; do
                if [[ "${EPOCH_START[$idx]}" =~ ^[0-9]+$ && "${EPOCH_END[$idx]}" =~ ^[0-9]+$ ]]; then
                    if (( unix_time >= ${EPOCH_START[$idx]} && unix_time <= ${EPOCH_END[$idx]} )); then
                        registration_epoch="${EPOCH_NUM[$idx]}"
                        break
                    fi
                fi
            done
        fi
    fi
    local confirmed_blocks_str="${ALL_CONFIRMED_BLOCKS[$i]}"
    local cur_epoch_blocks="${ALL_CUR_EPOCH_BLOCKS[$i]}"
    IFS=' ' read -r -a confirmed_blocks_array <<< "$confirmed_blocks_str"
    declare -A epoch_amounts=() epoch_blocks=()
    for ((epoch=1; epoch<${#confirmed_blocks_array[@]}; epoch++)); do epoch_blocks["$epoch"]=${confirmed_blocks_array[$epoch]}; done
    [[ $curEpoch -gt 0 ]] && epoch_blocks["$curEpoch"]="$cur_epoch_blocks"
    local response
    if [[ -n "$identity" ]]; then
        local _wurl="${PLATFORM_EXPLORER_URL}/identity/${identity}/withdrawals?page=1&limit=100"
        response=$(curl_with_retry "GET_identity_withdrawals" "$validator_hash" "$_wurl" '.resultSet != null')
    else
        response='{"resultSet":[]}'
    fi
    while IFS=$'\t' read -r timestamp amount; do
        [[ -z "$timestamp" || "$amount" == "null" ]] && continue
        unix_time=$(date -d "$timestamp" +%s%3N 2>/dev/null)
        nEpoch=""
        if [[ -n "$unix_time" && ${#EPOCH_START[@]} -gt 0 ]]; then
            for idx in "${!EPOCH_START[@]}"; do
                if [[ "${EPOCH_START[$idx]}" =~ ^[0-9]+$ && "${EPOCH_END[$idx]}" =~ ^[0-9]+$ ]]; then
                    if (( unix_time >= ${EPOCH_START[$idx]} && unix_time <= ${EPOCH_END[$idx]} )); then
                        nEpoch="${EPOCH_NUM[$idx]}"
                        break
                    fi
                fi
            done
        fi
        [[ -z "$nEpoch" || ! "$nEpoch" =~ ^[0-9]+$ || $nEpoch -eq 0 ]] && continue
        epoch_amounts[$nEpoch]=$((${epoch_amounts[$nEpoch]:-0} + amount))
    done < <(echo "$response" | jq -r '.resultSet[]? | [.timestamp, .amount] | @tsv' 2>/dev/null)
    local epochs_json="[]"
    local withdrawals_cache_file="$SAVE_DIR/cache/validators/${validator_hash}_withdrawals_through_${fixed_epoch_max}.json"
    if [[ $fixed_epoch_max -ge 1 && -f "$withdrawals_cache_file" ]]; then
        cached_epochs=$(cat "$withdrawals_cache_file")
        recent_json="[]"
        for epoch in "${!epoch_blocks[@]}"; do
            [[ $epoch -eq 0 || $epoch -lt $((fixed_epoch_max + 1)) ]] && continue
            [[ -n "$registration_epoch" && "$registration_epoch" =~ ^[0-9]+$ && "$epoch" -lt "$registration_epoch" ]] && continue
            amount=${epoch_amounts[$epoch]:-0}
            amount=$(awk "BEGIN {printf \"%.1f\", $amount/100000000000}")
            validator_blocks="${epoch_blocks[$epoch]}"
            validator_credits_value="0"
            if [[ -n "${epoch_block_reward_L2[$epoch]}" && "${epoch_block_reward_L2[$epoch]}" != "0" && -n "$validator_blocks" && "$validator_blocks" -gt 0 ]]; then
                validator_credits_value=$(echo "scale=8; $validator_blocks * ${epoch_block_reward_L2[$epoch]}" | bc)
            fi
            recent_json=$(jq --arg e "$epoch" --arg b "${epoch_blocks[$epoch]}" --arg a "$amount" --arg vcv "$validator_credits_value" '. += [{"epoch": $e | tonumber, "blocks": $b | tonumber, "withdrawal": $a | tonumber, "validator_credits_value": (if $vcv == "" then "0" else $vcv | tonumber end)}]' <<< "$recent_json")
        done
        for epoch in "${!epoch_amounts[@]}"; do
            [[ ! "$epoch" =~ ^[0-9]+$ || $epoch -eq 0 || $epoch -lt $((fixed_epoch_max + 1)) ]] && continue
            [[ -n "$registration_epoch" && "$registration_epoch" =~ ^[0-9]+$ && "$epoch" -lt "$registration_epoch" ]] && continue
            [[ -z "${epoch_blocks[$epoch]}" ]] || continue
            amount=$(awk "BEGIN {printf \"%.1f\", ${epoch_amounts[$epoch]}/100000000000}")
            recent_json=$(jq --arg e "$epoch" --arg a "$amount" '. += [{"epoch": $e | tonumber, "blocks": 0, "withdrawal": $a | tonumber, "validator_credits_value": 0}]' <<< "$recent_json")
        done
        epochs_json=$(jq -n --argjson c "$cached_epochs" --argjson r "$recent_json" '($c + $r) | sort_by(.epoch)')
    else
        for epoch in "${!epoch_blocks[@]}"; do
            [[ $epoch -eq 0 ]] && continue
            [[ -n "$registration_epoch" && "$registration_epoch" =~ ^[0-9]+$ && "$epoch" -lt "$registration_epoch" ]] && continue
            amount=${epoch_amounts[$epoch]:-0}
            amount=$(awk "BEGIN {printf \"%.1f\", $amount/100000000000}")
            validator_blocks="${epoch_blocks[$epoch]}"
            validator_credits_value="0"
            if [[ -n "${epoch_block_reward_L2[$epoch]}" && "${epoch_block_reward_L2[$epoch]}" != "0" && -n "$validator_blocks" && "$validator_blocks" -gt 0 ]]; then
                validator_credits_value=$(echo "scale=8; $validator_blocks * ${epoch_block_reward_L2[$epoch]}" | bc)
            fi
            epochs_json=$(jq --arg e "$epoch" --arg b "${epoch_blocks[$epoch]}" --arg a "$amount" --arg vcv "$validator_credits_value" '. += [{"epoch": $e | tonumber, "blocks": $b | tonumber, "withdrawal": $a | tonumber, "validator_credits_value": (if $vcv == "" then "0" else $vcv | tonumber end)}]' <<< "$epochs_json")
        done
        for epoch in "${!epoch_amounts[@]}"; do
            [[ ! "$epoch" =~ ^[0-9]+$ || $epoch -eq 0 ]] && continue
            [[ -n "$registration_epoch" && "$registration_epoch" =~ ^[0-9]+$ && "$epoch" -lt "$registration_epoch" ]] && continue
            [[ -z "${epoch_blocks[$epoch]}" ]] || continue
            amount=$(awk "BEGIN {printf \"%.1f\", ${epoch_amounts[$epoch]}/100000000000}")
            epochs_json=$(jq --arg e "$epoch" --arg a "$amount" '. += [{"epoch": $e | tonumber, "blocks": 0, "withdrawal": $a | tonumber, "validator_credits_value": 0}]' <<< "$epochs_json")
        done
        epochs_json=$(jq 'sort_by(.epoch)' <<< "$epochs_json")
        if [[ $fixed_epoch_max -ge 1 ]]; then
            cached_part=$(echo "$epochs_json" | jq --argjson m "$fixed_epoch_max" '[.[] | select(.epoch <= $m)]')
            echo "$cached_part" > "$withdrawals_cache_file"
        fi
    fi
    epochs_json=$(echo "$epochs_json" | jq --argjson rewards "$REWARDS_JSON" 'map(. + {"validator_credits_value": (if .blocks != null and (.blocks > 0) then ((.blocks * (($rewards[.epoch|tostring] // 0) | tonumber)) * 100000000 | round | . / 100000000) else 0 end)})')
    epochs_json_compact=$(echo "$epochs_json" | jq -c '.')
    local validator_fragment
    validator_fragment=$(jq -c -n --arg hash "$validator_hash" --arg ip "$service" --arg identity "$identity" --arg registration_epoch "$registration_epoch" --arg identityBalance "$identityBalance" --arg rating "$rating" --argjson withdrawals "$epochs_json_compact" '{($hash): { "IP": $ip, "identity": $identity, "identityBalance": $identityBalance, "registration_epoch": (if $registration_epoch == "" then null else ($registration_epoch | tonumber) end), "rating": ($rating | tonumber), "withdrawals": $withdrawals }}')
    echo "$validator_fragment" > "$out_file"
}

# Воркеры для --incremental-v1 (параллель по PARALLEL_JOBS). Глобальные: VALIDATORS, REWARDS_JSON, EPOCH_*.
incremental_v1_blocks_worker() {
    local i="$1" json_file="$2" curEpoch="$3" bounds_file="$4" out_dir="$5"
    local h tmpf ceb fb lb
    h="${VALIDATORS[$i]}"
    if ! jq -e --arg xx "$h" '.validators[$xx]' "$json_file" >/dev/null 2>&1; then
        echo "  ВНИМАНИЕ [incremental-v1]: нет .validators[$h] в db.json — блоки=0." >&2
        echo 0 >"${out_dir}/ceb_${i}.txt"
        return 0
    fi
    tmpf=$(mktemp)
    get_validator_blocks "$h" >"$tmpf"
    fb=$(awk -F: -v e="$curEpoch" '$1==e{print $2; exit}' "$bounds_file")
    lb=$(awk -F: -v e="$curEpoch" '$1==e{print $3; exit}' "$bounds_file")
    ceb=$(awk -v b="$fb" -v c="$lb" '$1 >= b && $1 <= c {count++} END {print count+0}' "$tmpf")
    rm -f "$tmpf"
    echo "$ceb" >"${out_dir}/ceb_${i}.txt"
}

incremental_v1_patch_worker() {
    local i="$1" json_file="$2" curEpoch="$3" rating="$4" ceb="$5" out_dir="$6"
    local h identity identityBalance response wresp _wurl wd_raw wd one_row epochs_with_credits vcv_epoch
    local timestamp amount unix_time nEpoch idx2
    h="${VALIDATORS[$i]}"
    if ! jq -e --arg xx "$h" '.validators[$xx]' "$json_file" >/dev/null 2>&1; then
        return 0
    fi
    identity=$(jq -r --arg xx "$h" '.validators[$xx].identity // empty' "$json_file")
    [[ -z "$identity" || "$identity" == "null" ]] && identity=""

    response=$(curl_with_retry "GET_validator" "$h" "$PLATFORM_EXPLORER_URL/validator/$h" '.proTxInfo != null')
    identityBalance=$(echo "$response" | jq -r '.identityBalance // 0')
    identityBalance=$(awk -v ib="${identityBalance:-0}" 'BEGIN {printf "%.4f", (ib+0)/100000000000}')
    if [[ -z "$identity" ]]; then
        identity=$(echo "$response" | jq -r '.identityId // .identity // empty')
        [[ -z "$identity" || "$identity" == "null" ]] && identity=""
    fi

    declare -A epoch_amounts=()
    if [[ -n "$identity" ]]; then
        _wurl="${PLATFORM_EXPLORER_URL}/identity/${identity}/withdrawals?page=1&limit=100"
        wresp=$(curl_with_retry "GET_identity_withdrawals" "$h" "$_wurl" '.resultSet != null')
        while IFS=$'\t' read -r timestamp amount; do
            [[ -z "$timestamp" || "$amount" == "null" ]] && continue
            unix_time=$(date -d "$timestamp" +%s%3N 2>/dev/null)
            nEpoch=""
            if [[ -n "$unix_time" && ${#EPOCH_START[@]} -gt 0 ]]; then
                for idx2 in "${!EPOCH_START[@]}"; do
                    if [[ "${EPOCH_START[$idx2]}" =~ ^[0-9]+$ && "${EPOCH_END[$idx2]}" =~ ^[0-9]+$ ]]; then
                        if (( unix_time >= ${EPOCH_START[$idx2]} && unix_time <= ${EPOCH_END[$idx2]} )); then
                            nEpoch="${EPOCH_NUM[$idx2]}"
                            break
                        fi
                    fi
                done
            fi
            [[ -z "$nEpoch" || ! "$nEpoch" =~ ^[0-9]+$ || $nEpoch -eq 0 ]] && continue
            epoch_amounts[$nEpoch]=$((${epoch_amounts[$nEpoch]:-0} + amount))
        done < <(echo "$wresp" | jq -r '.resultSet[]? | [.timestamp, .amount] | @tsv' 2>/dev/null)
    else
        echo "  ВНИМАНИЕ [incremental-v1]: нет identity для $h — withdrawal для эпохи $curEpoch считается 0 (без GET withdrawals)." >&2
    fi

    wd_raw=${epoch_amounts[$curEpoch]:-0}
    wd=$(LC_NUMERIC=C awk "BEGIN {printf \"%.1f\", ${wd_raw}/100000000000}")
    one_row=$(jq -n --argjson epoch "$curEpoch" --argjson blocks "$ceb" '[{epoch: $epoch, blocks: $blocks}]')
    epochs_with_credits=$(echo "$one_row" | jq --argjson rewards "$REWARDS_JSON" 'map(. + {"validator_credits_value": (if .blocks != null and (.blocks > 0) then ((.blocks * (($rewards[.epoch|tostring] // 0) | tonumber)) * 100000000 | round | . / 100000000) else 0 end)})')
    vcv_epoch=$(echo "$epochs_with_credits" | jq -c '(.[0].validator_credits_value // 0)')
    unset epoch_amounts

    jq -nc \
        --arg h "$h" \
        --arg ib "$identityBalance" \
        --argjson rating "$rating" \
        --argjson ce "$curEpoch" \
        --argjson blocks "$ceb" \
        --argjson wd "$wd" \
        --argjson vcv "$vcv_epoch" \
        '{h:$h, ib:$ib, rating:$rating, ce:$ce, blocks:$blocks, wd:$wd, vcv:$vcv}' >"${out_dir}/patch_${i}.ndjson"
}

# При той же эпохе: без rebuild_arrays / полного generate_json_db — только поля,
# которые меняются в течение эпохи (см. --incremental-v1 в справке).
incremental_v1_run() {
    local t_start t_end t_phase1 t_phase2 json_file bounds_file patches_ndjson merged_out patches_json
    local curEpoch db_epoch total i j temp idx blocks inc_dir
    local current_rating current_blocks

    t_start=$(date +%s)
    echo ""
    echo "=== incremental-v1 ==="
    json_file="$SAVE_DIR/db.json"
    bounds_file="$SAVE_DIR/tmp/epoch_bounds.txt"

    [[ -f "$json_file" ]] || { echo "ОШИБКА [incremental-v1]: нет $json_file (нужен полный прогон)." >&2; return 1; }
    [[ -f "$bounds_file" ]] || { echo "ОШИБКА [incremental-v1]: нет $bounds_file (нужен полный rebuild_arrays)." >&2; return 1; }
    [[ -f "$SAVE_DIR/epoch_intervals.txt" ]] || { echo "ОШИБКА [incremental-v1]: нет $SAVE_DIR/epoch_intervals.txt." >&2; return 1; }
    [[ -f "$EPOCH_BLOCKS_COUNT_L1_FILE" && -f "$EPOCH_BLOCKS_COUNT_L2_FILE" ]] || {
        echo "ОШИБКА [incremental-v1]: нужны $EPOCH_BLOCKS_COUNT_L1_FILE и $EPOCH_BLOCKS_COUNT_L2_FILE." >&2
        return 1
    }

    curEpoch=$(get_cur_epoch_from_status) || return 1
    db_epoch=$(jq -r '.current_epoch // empty' "$json_file")
    if [[ -z "$db_epoch" || "$db_epoch" == "null" ]]; then
        echo "ОШИБКА [incremental-v1]: в db.json нет .current_epoch." >&2
        return 1
    fi
    if [[ "$db_epoch" != "$curEpoch" ]]; then
        echo "ОШИБКА [incremental-v1]: current_epoch в db.json=$db_epoch, в API=$curEpoch — сделайте полный прогон без --incremental-v1." >&2
        return 1
    fi

    EPOCH_NUM=()
    EPOCH_START=()
    EPOCH_END=()
    while IFS= read -r line; do
        epoch_number=$(echo "$line" | awk '{print $2}' | tr -d ':')
        startTime=$(echo "$line" | awk '{print $3}')
        endTime=$(echo "$line" | awk '{print $5}')
        [[ -z "$epoch_number" || -z "$startTime" || -z "$endTime" ]] && continue
        EPOCH_NUM+=("$epoch_number")
        EPOCH_START+=("$startTime")
        EPOCH_END+=("$endTime")
    done < "$SAVE_DIR/epoch_intervals.txt"

    declare -A epoch_blocks_count_L1 epoch_blocks_count_L2 epoch_total_reward epoch_block_reward_L2
    while IFS=':' read -r epoch blocks; do [[ -n "$epoch" && -n "$blocks" ]] && epoch_blocks_count_L1[$epoch]="$blocks"; done < "$EPOCH_BLOCKS_COUNT_L1_FILE"
    while IFS=':' read -r epoch blocks; do [[ -n "$epoch" && -n "$blocks" ]] && epoch_blocks_count_L2[$epoch]="$blocks"; done < "$EPOCH_BLOCKS_COUNT_L2_FILE"
    for epoch in "${!epoch_blocks_count_L1[@]}"; do
        total_reward=$(echo "scale=8; ${epoch_blocks_count_L1[$epoch]} * $BLOCK_REWARD_L1" | bc)
        epoch_total_reward[$epoch]="$total_reward"
        if [[ -n "${epoch_blocks_count_L2[$epoch]}" && "${epoch_blocks_count_L2[$epoch]}" -gt 0 ]]; then
            block_reward_L2=$(echo "scale=8; $total_reward / ${epoch_blocks_count_L2[$epoch]}" | bc)
            epoch_block_reward_L2[$epoch]="$block_reward_L2"
        else
            epoch_block_reward_L2[$epoch]="0"
        fi
    done
    local rewards_pairs=()
    for e in "${!epoch_block_reward_L2[@]}"; do
        val="${epoch_block_reward_L2[$e]}"
        val="${val//[$'\n\r']}"
        [[ -z "$val" ]] && val="0"
        rewards_pairs+=("\"$e\":$val")
    done
    if [[ ${#rewards_pairs[@]} -eq 0 ]]; then
        REWARDS_JSON="{}"
    else
        REWARDS_JSON="{"$(IFS=,; echo "${rewards_pairs[*]}")"}"
    fi

    total=${#VALIDATORS[@]}
    if (( total <= 0 )); then
        echo "ОШИБКА [incremental-v1]: пустой список валидаторов." >&2
        return 1
    fi

    declare -a validator_blocks_temp=()
    inc_dir="$SAVE_DIR/tmp/incremental_v1"
    mkdir -p "$inc_dir"
    rm -f "$inc_dir"/ceb_*.txt "$inc_dir"/patch_*.ndjson 2>/dev/null || true

    echo "  [incremental-v1] фаза 1 — блоки (get_validator_blocks), PARALLEL_JOBS=$PARALLEL_JOBS, валидаторов=$total"
    t_phase1=$(date +%s)
    for ((batch_start=0; batch_start < total; batch_start+=PARALLEL_JOBS)); do
        for ((j=0; j<PARALLEL_JOBS && batch_start+j < total; j++)); do
            i=$((batch_start + j))
            incremental_v1_blocks_worker "$i" "$json_file" "$curEpoch" "$bounds_file" "$inc_dir" &
        done
        wait
        batch=$((batch_start + (PARALLEL_JOBS < total - batch_start ? PARALLEL_JOBS : total - batch_start)))
        [[ $((batch % 100)) -eq 0 || $batch -eq $total ]] && echo "    фаза 1: $batch/$total"
    done
    echo "  [incremental-v1] фаза 1: $(( $(date +%s) - t_phase1 )) сек"

    for ((i=0; i<total; i++)); do
        if [[ -f "${inc_dir}/ceb_${i}.txt" ]]; then
            validator_blocks_temp[i]=$(tr -d '\r\n' <"${inc_dir}/ceb_${i}.txt")
        else
            validator_blocks_temp[i]=0
        fi
    done

    declare -a indices=($(seq 0 $((total - 1))))
    for ((i=0; i<${#indices[@]}; i++)); do
        for ((j=i+1; j<${#indices[@]}; j++)); do
            if [[ ${validator_blocks_temp[${indices[j]}]} -gt ${validator_blocks_temp[${indices[i]}]} ]]; then
                temp=${indices[i]}
                indices[i]=${indices[j]}
                indices[j]=$temp
            fi
        done
    done
    declare -a ratings=()
    current_rating=1
    current_blocks=-1
    for idx in "${indices[@]}"; do
        blocks=${validator_blocks_temp[$idx]}
        if [[ $blocks -ne $current_blocks ]]; then
            current_rating=$((current_rating + (current_blocks != -1 ? 1 : 0)))
            current_blocks=$blocks
        fi
        ratings[$idx]=$current_rating
    done

    patches_ndjson=$(mktemp)
    : >"$patches_ndjson"
    echo "  [incremental-v1] фаза 2 — API validator + withdrawals, PARALLEL_JOBS=$PARALLEL_JOBS"
    t_phase2=$(date +%s)
    for ((batch_start=0; batch_start < total; batch_start+=PARALLEL_JOBS)); do
        for ((j=0; j<PARALLEL_JOBS && batch_start+j < total; j++)); do
            i=$((batch_start + j))
            incremental_v1_patch_worker "$i" "$json_file" "$curEpoch" "${ratings[$i]}" "${validator_blocks_temp[$i]}" "$inc_dir" &
        done
        wait
        batch=$((batch_start + (PARALLEL_JOBS < total - batch_start ? PARALLEL_JOBS : total - batch_start)))
        [[ $((batch % 100)) -eq 0 || $batch -eq $total ]] && echo "    фаза 2: $batch/$total"
    done
    echo "  [incremental-v1] фаза 2: $(( $(date +%s) - t_phase2 )) сек"

    for ((i=0; i<total; i++)); do
        [[ -f "${inc_dir}/patch_${i}.ndjson" && -s "${inc_dir}/patch_${i}.ndjson" ]] && cat "${inc_dir}/patch_${i}.ndjson" >>"$patches_ndjson"
    done
    rm -f "$inc_dir"/ceb_*.txt "$inc_dir"/patch_*.ndjson 2>/dev/null || true

    merged_out=$(mktemp)
    patches_json=$(jq -s '.' "$patches_ndjson")
    rm -f "$patches_ndjson"
    jq --argjson PATCHES "$patches_json" '
        reduce $PATCHES[] as $p (.;
            if .validators[$p.h] == null then .
            else
                .validators[$p.h].identityBalance = $p.ib
                | .validators[$p.h].rating = $p.rating
                | .validators[$p.h].withdrawals |= (
                    ($p.ce) as $ce
                    | if any(.[]; .epoch == $ce) then
                        map(if .epoch == $ce then {epoch: $ce, blocks: $p.blocks, withdrawal: $p.wd, validator_credits_value: $p.vcv} else . end)
                      else
                        . + [{epoch: $ce, blocks: $p.blocks, withdrawal: $p.wd, validator_credits_value: $p.vcv}] | sort_by(.epoch)
                      end
                    )
            end
        )
    ' "$json_file" >"$merged_out" && mv "$merged_out" "$json_file"

    t_end=$(date +%s)
    GENERATE_TOTAL_SEC=$((t_end - t_start))
    echo "[incremental-v1] запись $json_file, сек=$GENERATE_TOTAL_SEC"
}

generate_json_db() {
    local t_start t_end
    t_start=$(date +%s)
    echo ""
    echo "=== generate_json_db ==="
    : > "$DB_JSON_RECOVER_LIST" 2>/dev/null
    log_diag_section "Этап generate_json_db: выплаты по identity" "итог=$SAVE_DIR/db.json"
    local json_file="$SAVE_DIR/db.json"
    local curEpoch
    curEpoch=$(get_cur_epoch_from_status) || return 1
    local current_epoch_data=$(get_epoch_data $curEpoch)
    local current_start_time=$(echo "$current_epoch_data" | jq -r '.epoch.startTime')
    local current_end_time=$(echo "$current_epoch_data" | jq -r '.epoch.endTime')
    if [[ "$current_end_time" == "null" || -z "$current_end_time" ]]; then
        current_end_time=$(echo "$current_epoch_data" | jq -r '.nextEpoch.startTime // empty')
    fi
    if [[ "$current_end_time" == "null" || -z "$current_end_time" ]]; then
        next_epoch_data=$(get_epoch_data $((curEpoch + 1)))
        current_end_time=$(echo "$next_epoch_data" | jq -r '.epoch.startTime')
    fi
    local next_epoch=$((curEpoch + 1))
    local next_epoch_start_timestamp=""
    [[ -n "$current_end_time" && "$current_end_time" =~ ^[0-9]+$ ]] && next_epoch_start_timestamp=$((current_end_time + 1))
    declare -A epoch_blocks_count_L1 epoch_blocks_count_L2
    [[ -f "$EPOCH_BLOCKS_COUNT_L1_FILE" ]] && while IFS=':' read -r epoch blocks; do [[ -n "$epoch" && -n "$blocks" ]] && epoch_blocks_count_L1[$epoch]="$blocks"; done < "$EPOCH_BLOCKS_COUNT_L1_FILE"
    [[ -f "$EPOCH_BLOCKS_COUNT_L2_FILE" ]] && while IFS=':' read -r epoch blocks; do [[ -n "$epoch" && -n "$blocks" ]] && epoch_blocks_count_L2[$epoch]="$blocks"; done < "$EPOCH_BLOCKS_COUNT_L2_FILE"
    declare -A epoch_total_reward epoch_block_reward_L2
    for epoch in "${!epoch_blocks_count_L1[@]}"; do
        total_reward=$(echo "scale=8; ${epoch_blocks_count_L1[$epoch]} * $BLOCK_REWARD_L1" | bc)
        epoch_total_reward[$epoch]="$total_reward"
        if [[ -n "${epoch_blocks_count_L2[$epoch]}" && "${epoch_blocks_count_L2[$epoch]}" -gt 0 ]]; then
            block_reward_L2=$(echo "scale=8; $total_reward / ${epoch_blocks_count_L2[$epoch]}" | bc)
            epoch_block_reward_L2[$epoch]="$block_reward_L2"
        else
            epoch_block_reward_L2[$epoch]="0"
        fi
    done
    local json_data=$(jq -n \
        --arg current_epoch "$curEpoch" \
        --arg current_start_ts "$current_start_time" \
        --arg current_end_ts "$current_end_time" \
        --arg next_epoch "$next_epoch" \
        --arg next_start_ts "$next_epoch_start_timestamp" \
        '{ "current_epoch": (($current_epoch | tonumber) // 0), "epoch_timestamps": { "current_epoch_start": (if $current_start_ts == "" then null else ($current_start_ts | tonumber) end), "current_epoch_end": (if $current_end_ts == "" then null else ($current_end_ts | tonumber) end), "next_epoch_start": (if $next_start_ts == "" then null else ($next_start_ts | tonumber) end) }, "epochs": {}, "validators": {} }')
    sorted_epochs=($(printf '%s\n' "${!epoch_blocks_count_L1[@]}" | sort -n))
    for epoch in "${sorted_epochs[@]}"; do
        json_data=$(jq --arg e "$epoch" --arg tb_L1 "${epoch_blocks_count_L1[$epoch]}" --arg tb_L2 "${epoch_blocks_count_L2[$epoch]}" --arg tr "${epoch_total_reward[$epoch]}" --arg br_L2 "${epoch_block_reward_L2[$epoch]}" '.epochs += {($e): { "total_epoch_blocks_L1": $tb_L1 | tonumber, "total_epoch_blocks_L2": (if $tb_L2 == "" then null else $tb_L2 | tonumber end), "total_epoch_reward": $tr | tonumber, "block_reward_L2": $br_L2 | tonumber }}' <<< "$json_data")
    done
    for epoch in "${!epoch_blocks_count_L2[@]}"; do
        if [[ -z "${epoch_blocks_count_L1[$epoch]}" ]]; then
            json_data=$(jq --arg e "$epoch" --arg tb_L2 "${epoch_blocks_count_L2[$epoch]}" '.epochs += {($e): { "total_epoch_blocks_L1": null, "total_epoch_blocks_L2": $tb_L2 | tonumber, "total_epoch_reward": null, "block_reward_L2": null }}' <<< "$json_data")
        fi
    done
    # JSON для пересчёта validator_credits_value (кэш мог быть записан с нулями)
    rewards_pairs=()
    for e in "${!epoch_block_reward_L2[@]}"; do
        val="${epoch_block_reward_L2[$e]}"
        val="${val//[$'\n\r']}"
        [[ -z "$val" ]] && val="0"
        rewards_pairs+=("\"$e\":$val")
    done
    if [[ ${#rewards_pairs[@]} -eq 0 ]]; then
        REWARDS_JSON="{}"
    else
        REWARDS_JSON="{"$(IFS=,; echo "${rewards_pairs[*]}")"}"
    fi
    if [[ -n "${DEBUG_VALIDATOR_CREDITS:-}" ]]; then
        echo "  [DEBUG_VALIDATOR_CREDITS] Эпох в REWARDS_JSON (block_reward_L2): ${#rewards_pairs[@]}" >&2
        echo "$REWARDS_JSON" | jq -r 'to_entries | .[0:3][] | "    эпоха \(.key): block_reward_L2=\(.value)"' 2>/dev/null || true
    fi
    # Интервалы эпох — загружаем один раз (раньше timestamp_to_epoch читал файл на каждый withdrawal = минуты на валидатора)
    EPOCH_NUM=()
    EPOCH_START=()
    EPOCH_END=()
    if [[ -f "$SAVE_DIR/epoch_intervals.txt" ]]; then
        while IFS= read -r line; do
            epoch_number=$(echo "$line" | awk '{print $2}' | tr -d ':')
            startTime=$(echo "$line" | awk '{print $3}')
            endTime=$(echo "$line" | awk '{print $5}')
            [[ -z "$epoch_number" || -z "$startTime" || -z "$endTime" ]] && continue
            EPOCH_NUM+=("$epoch_number")
            EPOCH_START+=("$startTime")
            EPOCH_END+=("$endTime")
        done < "$SAVE_DIR/epoch_intervals.txt"
    fi
    # Эпохи 1..(curEpoch-1) закрыты и не меняются — кэшируем. Только curEpoch пересчитываем.
    fixed_epoch_max=$((curEpoch - 1))
    t_setup_end=$(date +%s)
    echo "  [generate_json_db] подготовка (epochs, rewards, json_data): $((t_setup_end - t_start)) сек"
    echo "  [файл] подготовка: читаем $EPOCH_BLOCKS_COUNT_L1_FILE, $EPOCH_BLOCKS_COUNT_L2_FILE, $SAVE_DIR/epoch_intervals.txt"
    mkdir -p "$SAVE_DIR/tmp/generate_parallel"
    t_validators=$(date +%s)
    total_gen=${#VALIDATORS[@]}
    echo "Формирование db.json (валидаторов: $total_gen) последовательно..."
    echo "  [процесс] каждый валидатор -> $SAVE_DIR/tmp/generate_parallel/fragment_N.json (и кэш cache/validators/*_withdrawals_*.json)"
    for ((i=0; i<total_gen; i++)); do
        process_validator_generate $i
        [[ $(( (i + 1) % 100 )) -eq 0 || $i -eq 0 ]] && echo "    обработано $((i+1))/$total_gen"
    done
    echo "  Обработано $total_gen/$total_gen валидаторов"
    recover_pass "generate_json_db" process_validator_generate
    t_validators_end=$(date +%s)
    echo "  [generate_json_db] цикл по валидаторам (последовательно): $((t_validators_end - t_validators)) сек"
    t_merge=$(date +%s)
    echo "  Слияние фрагментов: читаем $SAVE_DIR/tmp/generate_parallel/fragment_*.json -> пишем $SAVE_DIR/tmp/validators_merged.json"
    validators_merged_file="$SAVE_DIR/tmp/validators_merged.json"
    for i in $(seq 0 $((total_gen - 1))); do
        [[ -f "$SAVE_DIR/tmp/generate_parallel/fragment_${i}.json" ]] && cat "$SAVE_DIR/tmp/generate_parallel/fragment_${i}.json"
    done | jq -s 'add' > "$validators_merged_file"
    t_merge_end=$(date +%s)
    echo "  [generate_json_db] слияние фрагментов: $((t_merge_end - t_merge)) сек"
    echo "  [файл] итог: json_data + validators_merged -> $json_file"
    jq_merge_filter='.validators = ($v | .[0])'
    json_data=$(echo "$json_data" | jq --slurpfile v "$validators_merged_file" "$jq_merge_filter")
    echo "$json_data" | jq . > "$json_file"
    validators_in_json=$(jq '.validators | keys | length' "$json_file")
    if [[ "$validators_in_json" != "${#VALIDATORS[@]}" ]]; then
        echo "  ВНИМАНИЕ: в db.json записано validators: $validators_in_json, ожидалось: ${#VALIDATORS[@]}" >&2
    fi
    json_size=$(stat -c%s "$json_file" 2>/dev/null || echo 0)
    t_end=$(date +%s)
    GENERATE_TOTAL_SEC=$((t_end - t_start))
    echo "[generate_json_db] ИТОГО: ${GENERATE_TOTAL_SEC} сек"
    echo "JSON database: $json_file ($json_size bytes)"
}

if [[ "$INCREMENTAL_V1" -eq 1 ]]; then
    incremental_v1_run || exit 1
else
    generate_json_db
fi

end_time=$(date +%s)
elapsed=$((end_time - start_time))
elapsed_min=$((elapsed / 60))
elapsed_sec=$((elapsed % 60))
echo ""
echo "========== ИТОГ: ОТСЧЁТ ВРЕМЕНИ =========="
echo "Отсчёт «Время выполнения» — от старта generate_db_json_local.sh (start_time) до выхода."
echo "«Execution time: N min» в cron.log — от обёртки run_withdrawals_and_transfer_local.sh (включает этот скрипт + transfer_db.sh)."
echo ""
if [[ "$INCREMENTAL_V1" -eq 1 ]]; then
    echo "Режим --incremental-v1: rebuild_arrays не выполнялся; обновление db.json (баланс, рейтинг, withdrawals текущей эпохи) — ${GENERATE_TOTAL_SEC} сек."
    echo "  • Использованы существующие $SAVE_DIR/tmp/epoch_bounds.txt, epoch_intervals.txt, epoch_blocks_count_L1/L2."
else
    echo "Этап 1 — rebuild_arrays (всего ${REBUILD_TOTAL_SEC} сек):"
    echo "  • Вычисление блоков L1 по эпохам -> $EPOCH_BLOCKS_COUNT_L1_FILE"
    echo "  • Вычисление блоков L2 по эпохам -> $EPOCH_BLOCKS_COUNT_L2_FILE"
    echo "  • Границы эпох (first/last block) -> $SAVE_DIR/tmp/epoch_bounds.txt"
    echo "  • Обработка каждого валидатора (API блоки, validator, L1 время блока) -> $SAVE_DIR/tmp/rebuild_parallel/validator_*.json"
    echo "  • Слияние в массивы -> validators.txt, identities.txt, identityBalance.txt, confirmed_blocks_per_epoch.txt, cur_epoch_blocks_per_validator.txt, registered_times.txt, services.txt, $RATING_FILE"
    echo ""
    echo "Этап 2 — generate_json_db (всего ${GENERATE_TOTAL_SEC} сек):"
    echo "  • Подготовка (эпохи, награды, интервалы) -> данные из $SAVE_DIR/epoch_intervals.txt и ранее сохранённых файлов"
    echo "  • Обработка каждого валидатора (withdrawals, JSON-фрагмент) -> $SAVE_DIR/tmp/generate_parallel/fragment_*.json"
    echo "  • Слияние фрагментов и запись -> $SAVE_DIR/db.json"
fi
echo ""
echo "Время выполнения (generate_db_json_local.sh): ${elapsed_min} мин ${elapsed_sec} сек"
