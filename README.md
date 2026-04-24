# platformexp-db-json-bin

Скрипты для **mno@161.97.96.43** (platformExp): `generate_db_json_local.sh`, проверка **explorer vs dashmate**, опционально монитор блоков.

Локальная копия — каталог **`~/Projects/platformexp-db-json-bin`** (отдельно от GeoDashboard на 100.254).

## Заливка на 96.43

```bash
./push_generate_db_json_local.sh mno@161.97.96.43
```

На сервере: **`/home/mno/bin/`**, рядом **`.env`** (из **`.env.example`**).

## Синхронизация с evowatch-server-git

При правках в другом дереве — вручную `cp` нужных `*.sh` сюда или отсюда, чтобы не расходились две копии.
