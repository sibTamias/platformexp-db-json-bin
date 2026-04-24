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
`gh repo create platformexp-db-json-bin --public --source=. --remote=origin --push` (из каталога репозитория).
