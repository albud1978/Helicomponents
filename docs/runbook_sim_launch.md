# Runbook: загрузка данных и запуск симуляции LIMITER V8

**Единственный источник истины** по загрузке данных и командам запуска симуляции.
Создан по решению Алексея 2026-07-03, чтобы ни один агент больше не искал команды по коду и чатам.

---

## 1. Источники данных: 3 из DWH + 4 статичных Excel. Пробелов в данных нет

### 3 источника из DWH (по дате среза `--report-date`)

| # | Источник DWH | Таблица Project CH | Загрузчик |
|---|---|---|---|
| 1 | `reports.amos_heli_rotables_components_status` | `heli_raw` + `heli_pandas` | `dwh_loader.py --step status_components` |
| 2 | реестр планеров | `program_ac` | `dwh_loader.py --step program_ac` |
| 3 | статусы капремонта | `status_overhaul` | `dwh_loader.py --step status_overhaul` |

После загрузки обязателен `--step enrich` (post-enrichment `heli_pandas`: статусы, group_by, repair_days и т.д.).

### 4 статичных источника из Excel (последний датасет `v_2026-04-08` + master_data)

| # | Excel | Таблица / потребитель | Загрузчик / резолвер |
|---|---|---|---|
| 1 | `Program_heli.xlsx` (из `data_input/source_data/v_2026-04-08/`) | `flight_program_ac` (MP4-таргеты, spawn_limit) | `code/extract/program_ac_direct_loader.py --version-date <vd> --version-id 1 --dataset-path <v_*>` |
| 2 | `Program.xlsx` (оттуда же) | `flight_program_fl` (MP5, суточные налёты) | `code/extract/program_fl_direct_loader.py` (те же аргументы; запускать ПОСЛЕ ac-загрузчика) |
| 3 | `MD_Сomponents.xlsx` (`data_input/master_data/`) | `md_components` (справочник, без привязки к дате heli) | `code/extract/md_components_loader.py`; резолвер `static_data_resolver.resolve_md_components_workbook()` |
| 4 | `Economics.xlsx` (`data_input/master_data/`) | Экономика (daily costs) — читается симулятором напрямую при старте (`compute_economics_daily_costs`), таблица не нужна | `static_data_resolver.resolve_economics_workbook()` |

**Ключевое:** Excel-источники 1–2 генерируются на дату среза (`version_date` = дата DWH-среза, `version_id=1`), содержимое — из последнего датасета `v_2026-04-08`. Программа полётов вне DWH-scope. Никакого «пробела» в данных нет: для каждой version_date DWH-среза flight_program_* создаётся загрузчиками (идемпотентно, rewrite по своему срезу).

### Полный пайплайн одной командой (batch)

```bash
# DWH load + flight_program (Excel) + sim + валидация на несколько дат:
.venv/bin/python code/utils/dwh_batch_sim_gate.py   # даты в DATES_DEFAULT или аргументами
```

### Пошагово (одна дата, пример 2026-06-29)

```bash
export DWH_CLICKHOUSE_CA_CERT=/media/DATA_BIG/Projects/Heli/Helicomponents/config/certs/yandex_cloud_RootCA.pem

# Шаг 1: 3 источника из DWH + enrich
.venv/bin/python code/utils/dwh_loader.py --report-date 2026-06-29 --version-id 1 --step all --skip-existing

# Шаг 2: flight_program из Excel (порядок важен: сначала ac, потом fl)
.venv/bin/python code/extract/program_ac_direct_loader.py --version-date 2026-06-29 --version-id 1 --dataset-path data_input/source_data/v_2026-04-08
.venv/bin/python code/extract/program_fl_direct_loader.py --version-date 2026-06-29 --version-id 1 --dataset-path data_input/source_data/v_2026-04-08
```

> ⚠️ Повторный `--step enrich` на уже обогащённом `heli_pandas` падает — enrich только на свежем срезе (детали: `docs/dwh_sim_gate.md`).

---

## 2. Запуск симуляции: seatbelts ON / OFF

Два conda-окружения с pyflamegpu 2.0.0rc4+cuda130 (проверено 2026-07-03):

| Окружение | SEATBELTS | Назначение | Интерпретатор |
|---|---|---|---|
| `cuda13` | **ON** (`pyflamegpu.SEATBELTS=True`) | отладка, диагностируемые ошибки device-кода | `/home/albud/miniconda3/envs/cuda13/bin/python3` |
| `cuda13_nosb` | **OFF** (`pyflamegpu.SEATBELTS=False`) | производительные/канонические прогоны, замеры | `/home/albud/miniconda3/envs/cuda13_nosb/bin/python3` |

**Результаты побитово идентичны** (подтверждено 2026-07-03: EXCEPT обеих таблиц в обе стороны = 0 на 2026-06-29).

> ⚠️ **`conda run -n cuda13 python3` НЕ работает** (не видит pyflamegpu). Использовать только прямые пути к интерпретаторам, как ниже. `.venv` репозитория тоже без pyflamegpu — он для ETL/валидации.

### Канонический запуск (пример: срез 2026-06-29, 10 лет)

```bash
cd /media/DATA_BIG/Projects/Heli/Helicomponents
set -a && source .env && set +a
export AGENT_KG_WORKFLOW_ID=<активный workflow>   # обязателен (KG-guard)

# SEATBELTS OFF (канонический производительный прогон):
/home/albud/miniconda3/envs/cuda13_nosb/bin/python3 \
  code/sim_v2/messaging/orchestrator_limiter_v8.py \
  --version-date 2026-06-29 --version-id 1 --input-version-id 1

# SEATBELTS ON (отладка): тот же вызов через /home/albud/miniconda3/envs/cuda13/bin/python3
```

Дефолты: `--end-day 3650` (10 лет), `--max-steps 10000` — для полного прогона указывать не нужно.

### Семантика version_id (критично, источник прошлых «глупых вопросов»)

- `--input-version-id` — срез входа (`heli_pandas`); `--version-id` — id выхода (`sim_masterv2_v9`, `sim_repairline_v9`, витрина daily).
- **Валидаторы ищут `flight_program_ac` и входной `heli_pandas` по ВЫХОДНОМУ version_id.** Поэтому канонический прогон — всегда `--version-id 1`.
- Прогон с выходным id ≠ 1 (инженерный) даст ложные FAIL: INV-2/11/13 («flight_program_ac пуст») и INV-12 (входной переналёт не классифицируется → «sim_introduced»). Это артефакт связки, а не дефект алгоритма.
- Симулятор идемпотентен: перед INSERT чистит **только свой срез** (`version_date`,`version_id`). Дроп таблиц не нужен никогда.

### 🚫 Тотальный запрет DROP (решение Алексея 2026-07-03)

- **`--drop-table`, `DROP TABLE`, `TRUNCATE` — запрещены без явного согласия Алексея в текущем чате.**
- Enforcement: `.cursor/hooks/clickhouse_drop_guard.py` (deny в preToolUse); bypass-токен `DROP_APPROVED_BY_ALEXEY=1` — только после согласия.
- Причина: инцидент 2026-07-03 — `--drop-table` пересоздал `sim_masterv2_v9`/`sim_repairline_v9`, уничтожив все version_id, включая канонический `1`.

---

## 3. Ограничение рождений (spawn_limit) и сценарные прогоны с диапазоном рождений

### Что такое spawn_limit

- **spawn_limit — месячный потолок динамических рождений новых Ми-17.** Источник: строка `spawn_limit` в `Program_heli.xlsx` (помесячные значения) → колонка `spawn_limit` в `flight_program_ac` (значение в первый день месяца, остальные дни 0).
- **Активация presence-based** (`flight_program_ac.spawn_limit_active`): `1` — если строка `spawn_limit` присутствовала в Excel (даже со всеми нулями → динамический спавн Ми-17 полностью заблокирован); `0` — строки не было → без ограничения (uncapped).
- На GPU лимит работает как **кумулятив** (`spawn_limit_cumulative`, считается на хосте в `sim_env_setup.prepare_env_arrays`): суммарные динамические рождения Ми-17 к дню D не могут превысить сумму месячных лимитов к этому дню. Детерминированный спавн (плановые поставки `new_counter_mi17`) лимитом не ограничивается.

### Sweep: кривая чувствительности по диапазону рождений (cap 0..60 + uncapped)

Трёхслойный контур (один RTC-компайл, параллельные GPU-прогоны через CUDAEnsemble):

```bash
# 1. CUDAEnsemble sweep: синтетические cap=0..60 + uncapped baseline.
#    Вход общий (--input-version-id), выход: vid = out_base + cap (default 1000+N), uncapped = 1061.
/home/albud/miniconda3/envs/cuda13_nosb/bin/python3 code/utils/spawn_cap_ensemble.py \
  --version-date 2026-06-22 --input-version-id 3 \
  --caps 0 1 2 ... 60 \            # default: все 0..60
  --repair-quota 18 \              # обязателен (из sim-SSoT max(mp1_repair_number))
  --concurrent-runs 4              # эмпирический оптимум (cr>4 медленнее)
# Пайплайн внутри: GPU ensemble → batch loader (ensemble_mp2_loader.py) → chunked materializer.

# 2. Сбор кривой чувствительности в CSV (без GPU):
python3 code/utils/spawn_cap_curve.py --collect \
  --version-date 2026-06-22 --cap-min 0 --cap-max 60 --out-base 1000 \
  --baseline-vid 1061 --src-vid 3 --csv-path output/spawn_cap_curve_b3_20260622.csv
# Колонки: cap, version_id, births, deficit_total, deficit_post180, unmet_hours_*, d_deficit_per_cap.
```

- Семантика cap-синтеза: форма шага берётся из реального входа (`--reference-version-id`/`--threshold`), cap N масштабирует потолок; `cap=61`/uncapped — `spawn_limit_active=0`.
- Проверенные свойства (2026-06-28): births линейны cap=N→N; uncapped == референсный одиночный прогон (EXCEPT=0); полный свип 62 vid E2E ≈ 308 с.
- Выходные vid сценариев (1000..1061) — **инженерные**: валидировать их полным набором инвариантов нельзя (см. семантику version_id выше); кривая собирается `--collect`, а не `run_all_stream`.
- `spawn_cap_curve.py --run` / `spawn_cap_curve_launcher.py` — старый одиночный слой (субпроцессы по одному cap); для свипов использовать `spawn_cap_ensemble.py`.

## 4. Валидация инвариантов

```bash
# Полный поток (16 проверок: INV-1..13, TEMP-1/4/5) на канонический срез:
.venv/bin/python code/validation/run_all_stream.py --dataset 20260629:1
```

- INV-12 классифицирует переналёт: «входной» (борт уже в `heli_pandas` с `ppr>oh`) → **WARNING, PASS**; «переналёт движка» (введён симуляцией) → FAIL. Классификация работает только при выходном id = входному (канонический `1`).
- Известный кейс: `acn=22497` (2026-06-29) — входной переналёт из источника, признан валидным для симуляции (решение Алексея 2026-07-01), правка данных на стороне DWH.

## 5. Профилирование ядер и замер скорости

### Уровень 1 — wall-time (без инструментов)

- Оркестратор сам печатает: `Время общее / Время GPU / дней-сек (GPU и общая)`.
- Sweep-harness (`spawn_cap_ensemble.py`) печатает phase timings: GPU ensemble / loader / materializer.
- Точнее: `/usr/bin/time -v <команда>` (peak RSS, user/sys).
- **Первый прогон всегда включает одноразовый RTC JIT-компайл (~1.2 с и более при смене model hash)** — скорость мерить по «прогретому» прогону или вычитать cold-фазу.

### Уровень 2 — nsys (Nsight Systems): таймлайн ядер, memcpy, CUDA API, host-локи

> ⚠️ **Blackwell (sm_120): системный nsys 2024.5 из `/usr/local/cuda-12.6` СЛЕП** — CUDA-trace будет пустым. Использовать только **nsys 2025.3.1** из окружения `cuda13_nosb`.

```bash
NSYS=/home/albud/miniconda3/envs/cuda13_nosb/nsight-compute-2025.3.1/host/target-linux-x64/nsys

# Профиль полного прогона (ядра + memcpy + CUDA API + OS runtime локи):
$NSYS profile --trace=cuda,nvtx,osrt,cuda-hw --sample=none --stats=true --show-output=true \
  -f true -o output/nsys_<label> \
  /home/albud/miniconda3/envs/cuda13_nosb/bin/python3 -u <сим-команда из раздела 2>

# Готовые отчёты из .nsys-rep:
$NSYS stats --report cuda_gpu_kern_sum  --format column output/nsys_<label>.nsys-rep  # топ ядер
$NSYS stats --report cuda_api_sum       --format column output/nsys_<label>.nsys-rep  # sync/launch/memcpy API
$NSYS stats --report cuda_gpu_mem_time_sum --format column output/nsys_<label>.nsys-rep  # H2D/D2H
$NSYS stats --report osrt_sum           --format column output/nsys_<label>.nsys-rep  # pthread/GIL contention
```

- `--stats=true` заодно кладёт рядом `.sqlite` — для кастомного анализа: `sqlite3 output/nsys_<label>.sqlite` (таблицы `CUPTI_ACTIVITY_KIND_KERNEL`, `CUPTI_ACTIVITY_KIND_RUNTIME`, `OSRT_API`; в multi-run сравнениях суммы считать per-thread).
- Пример разбора и выводов: `output/nsys_blackwell_verdict_20260630.md` (kernels ~1 с из wall ~10 с; узкие места — sync/launch/H2D, не compute), артефакты замеров: `output/nsys_off_scaling/` (точные команды сохранены в `*_command.txt`).

### Уровень 3 — ncu (Nsight Compute): глубокий разбор одного ядра

Occupancy, memory throughput, стойлы конкретного RTC-ядра:

```bash
/home/albud/miniconda3/envs/cuda13_nosb/nsight-compute-2025.3.1/ncu \
  --kernel-name <regex_имени_ядра> --launch-count 3 \
  /home/albud/miniconda3/envs/cuda13_nosb/bin/python3 <сим-команда>
```

Нужен, только если nsys показал, что доминирует конкретное ядро (в текущей модели compute ~10% wall — обычно не нужен).

### Уровень 4 — py-spy: host/Python-сторона (GIL, сериализация Init)

```bash
/home/albud/miniconda3/envs/cuda13/bin/py-spy dump --pid <PID>            # мгновенный стек всех потоков
/home/albud/miniconda3/envs/cuda13/bin/py-spy record -o prof.svg --pid <PID> --duration 30  # flamegraph
```

### Методика честного замера (обязательно)

1. Одинаковый датасет/version_date/caps у сравниваемых вариантов.
2. Warm-up прогон перед замером (исключить JIT/NVRTC cold-компайл).
3. Бит-идентичность результата до сравнения скоростей (EXCEPT both-ways = 0) — оптимизация, меняющая результат, не считается.
4. Фиксировать команду замера рядом с артефактом (`*_command.txt` в `output/`).

## Связанные документы

- `docs/dwh_sim_gate.md` — история приёмок DWH-срезов, тайминги, нюансы enrich
- `config/transitions/invariants.json` — SSoT инвариантов
- `.cursor/rules/00_global_always.mdc` — правила (в т.ч. запрет DROP)
