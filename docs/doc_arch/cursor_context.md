# Cursor Context Guide — восстановление контекста из GitHub

Этот документ помогает быстро восстановить контекст изменений, если агент был деактивирован, но изменения уже попали в GitHub (PR смержен или есть коммит в основной ветке).

## Фиксация изменений из GitHub для нового агента

Если агент был деактивирован, но изменения уже попали в GitHub (PR смержен или есть коммит в master/main), можно быстро восстановить контекст так:

### 1) Сохранить список изменённых файлов

Замените COMMIT_SHA на нужный SHA из GitHub (например, `7cfd32a0`).

```bash
git show --name-status --pretty="" COMMIT_SHA > docs/changed_files_$(date +%Y-%m-%d).txt
```

Это создаст файл со списком изменённых файлов и их статусом (A — добавлен, M — изменён, D — удалён).

---

### 2) Сохранить полный diff

```bash
git diff COMMIT_SHA^! > docs/changes_$(date +%Y-%m-%d).diff
```

Если это merge-коммит (у него два родителя), diff берётся относительно первого родителя:

```bash
PARENT=$(git rev-parse COMMIT_SHA^1)
git diff $PARENT COMMIT_SHA > docs/changes_$(date +%Y-%m-%d).diff
```

---

### 3) Сохранить архив текущего состояния (по желанию)

```bash
git archive -o snapshot_$(date +%Y-%m-%d).tar.gz HEAD
```

---

### 4) Закоммитить эти файлы в репозиторий

```bash
git add docs/changed_files_*.txt docs/changes_*.diff
git commit -m "ДД.ММ — фиксация diff и списка файлов для контекста нового агента"
git push
```

---

## Зачем это нужно

- Новый агент сможет сразу «прочитать» diff и понять, какие именно изменения были внесены в файлы, даже если старый агент был деактивирован.
- Это работает и после squash/force-push, если сохранить SHA актуального коммита из GitHub.

---

## Шаблон быстрой команды (с автоматическим определением merge-коммита)

Замените `COMMIT_SHA` на нужное значение и выполните:

```bash
COMMIT_SHA=7cfd32a0; \
DATE=$(date +%Y-%m-%d); \
git show --name-status --pretty="" $COMMIT_SHA > docs/changed_files_${DATE}.txt; \
if [ $(git rev-list --parents -n 1 $COMMIT_SHA | wc -w) -gt 2 ]; then \
  PARENT=$(git rev-parse $COMMIT_SHA^1); \
  git diff $PARENT $COMMIT_SHA > docs/changes_${DATE}.diff; \
else \
  git diff ${COMMIT_SHA}^! > docs/changes_${DATE}.diff; \
fi
```

После чего закоммитьте файлы как в шаге 4.

---

Проект: Helicopter Component Lifecycle (ETL на ClickHouse, Excel).