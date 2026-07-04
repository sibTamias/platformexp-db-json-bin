# platformexp-db-json-bin

Скрипты для **mno@161.97.96.43** (platformExp): `generate_db_json_local.sh`, проверка **explorer vs dashmate**, опционально монитор блоков.

Локальная копия — каталог **`~/Projects/platformexp-db-json-bin`** (отдельно от GeoDashboard на 100.254).

## db.json: пилот 254 vs контроль 201

| Сервер | Схема |
|--------|--------|
| **161.97.100.254** dashnodes.net | live-patch локально (`patch_db_json_on_new_block`, `patch_db_json_live`) |
| **46.19.66.201** evowatch.ru | SCP `db.json` с 96.43 каждые 2 ч |
| **161.97.96.43** platformExp | полный generate + transfer **только на 201** |

Подробно: **`evowatch-server-git/bin/DB_JSON_LIVE_BASELINE.md`**

Заливка live-скриптов на **254**:

```bash
./push_patch_db_json_live.sh mno@161.97.100.254
```

## Заливка на 96.43 (с Mac)

```bash
./push_generate_db_json_local.sh mno@161.97.96.43
./push_patch_db_json_live.sh mno@161.97.96.43
```

На 96.43 один раз:

```bash
cp ~/bin/transfer_db.conf.example ~/bin/transfer_db.conf
# TARGET_SERVERS="46.19.66.201"  — без 254
```

### Cron (96.43): generate + transfer на 201

Полный `generate_db_json_local.sh` (без `--incremental-v1`), затем `transfer_db.sh`:

```cron
# db.json + transfer every 2 h (201 only; 254 = live-patch locally)
0 */2 * * * /home/mno/bin/run_withdrawals_and_transfer_local.sh >> /home/mno/tmp/cron.log 2>&1
```

`SKIP_TRANSFER=1` в `.env` **не** ставить, если нужен transfer на 201.

## На сервере без `cp` каждый раз

1. Один раз клон и симлинки из репозитория в **`~/bin/`** (рядом кладите **`.env`** — как раньше):

```bash
cd ~
git clone git@github.com:sibTamias/platformexp-db-json-bin.git
cd platformexp-db-json-bin
./install_symlinks.sh
```

2. Обновление только **`git pull`** в каталоге клона — **перекопировать ничего не нужно**, **`/home/mno/bin/generate_db_json_local.sh`** остаётся ссылкой на файл в репо.

При вызове через **`~/bin/...`** скрипт видит **`BIN=/home/mno/bin`** и читает **`~/bin/.env`**.

## Синхронизация с evowatch-server-git

При правках в другом дереве — вручную `cp` нужных `*.sh` сюда или отсюда, чтобы не расходились две копии.

## IP2Location + DB-IP (раз в месяц, cron на 96.43)

Базы скачиваются **только на platformExp** (IP2Location по token, DB-IP бесплатно), затем `rsync` на **46.19.66.201** и **161.97.100.254** (`~/bin/db/`).

1. В **`~/bin/.env`**: `IP2LOCATION_DOWNLOAD_TOKEN=...` (см. `.env.example`)
2. Проверка:
   ```bash
   /home/mno/bin/update_ip2location_db.sh --dry-run
   /home/mno/bin/update_and_transfer_ip2location_db.sh
   ```
3. Cron (2-е число, 03:00):
   ```bash
   0 3 2 * * /home/mno/bin/update_and_transfer_ip2location_db.sh >> /home/mno/logs/ip2location_update.log 2>&1
   ```

Заливка скриптов с Mac: `./push_ip2location_db.sh mno@161.97.96.43` или `git pull` + `./install_symlinks.sh`.

## GitHub (pull / push)

1. Создайте **пустой** репозиторий: https://github.com/new — имя **`platformexp-db-json-bin`**, без README/.gitignore (уже есть локально), владелец **`sibTamias`**.
2. Локально уже настроено:
   - `git remote -v` → `origin git@github.com:sibTamias/platformexp-db-json-bin.git`
3. Первый пуш (ветка **`main`**):
   ```bash
   cd ~/Projects/platformexp-db-json-bin
   git push -u origin main
   ```
4. Дальше: **`git pull`** перед правками, после — **`git add` / `git commit` / `git push`**.

Альтернатива без веб-формы: **`gh auth login`**, затем  
`gh repo create platformexp-db-json-bin --public --source=. --remote=origin --push` (из каталога репозитория; ветка по умолчанию у вас — **`main`**).
