# Runbook: загрузка данных и запуск симуляции LIMITER V8

**Единственный источник истины** по загрузке day0 и командам запуска симуляции.
Агентам **запрещено** собирать команды из кода/чатов/старых handoff — только этот файл (+ `.cursor/rules/10_extract_and_env.mdc` для Extract).

---

## 0. Day0 prepare-to-sim — КОПИРУЙ ЭТО (канон с 2026-07-23)

Одна команда готовит срез до «можно запускать sim»: DWH×3 + enrich + `flight_program_*` + статусы/BR + **demote** + gate OPS==MP4.

```bash
cd /media/DATA_BIG/Projects/Heli/Helicomponents
source ~/miniconda3/etc/profile.d/conda.sh && conda activate cuda13
source config/load_env.sh
export CUBE_CONFIG_PATH="$PWD/config"
export DWH_CLICKHOUSE_CA_CERT="$PWD/config/certs/yandex_cloud_RootCA.pem"
export AGENT_KG_WORKFLOW_ID=<активный workflow>   # обязателен, если включён KG-guard

# Подставь дату среза и папку с Program_heli.xlsx + Program.xlsx:
.venv/bin/python code/extract/extract_master.py \
  --source dwh --mode prod \
  --version-date 2026-07-22 --version-id 1 \
  --dataset-path data_input/source_data/v_2026-07-22
```

| Флаг | Смысл |
|---|---|
| `--source dwh` | канон (AMOS DWH). Не Excel-меню. |
| `--mode prod` | версионированный срез `version_id` |
| `--version-date` | = day0 = дата DWH-среза |
| `--version-id` | канон для sim/валидаторов = **1** |
| `--dataset-path` | папка `v_YYYY-MM-DD` с `Program_heli.xlsx` и `Program.xlsx` (желательно та же дата, что `--version-date`) |
| `--replace-slice` | только если нужно **перезалить** `heli_pandas` с DWH (иначе idempotent skip существующих таблиц) |

**Acceptance (master сам проверяет в конце):** exit 0; `status_id=0` = 0; OPS Mi-8/Mi-17 == `ops_counter_*` day0 в `flight_program_ac`.

Smoke-доказательство: `output/extract_master_smoke_2026-07-22.log` (2026-07-23, exit 0, OPS 49/88).

### Запрещено агентам (частые ошибки)

1. Собирать day0 руками: `dwh_loader` → `program_*_loader` → `day0_ops_deficit_demote_runner` **по отдельности**.
2. Запускать только `dwh_loader --step all` / `--step enrich` и считать срез готовым к sim (**без demote OPS ≠ MP4**).
3. Запускать demote до `flight_program_ac` или после «голого» enrich без повторного demote через master.
4. Искать «как раньше» в чатах / `extract_master` interactive / `printf "1\n2\n"` для DWH-канона.
5. Путать интерпретаторы: Extract/ETL = `.venv` + conda `cuda13` (env vars); **sim** = `cuda13_nosb` (см. §2).

Leaf-скрипты (`dwh_loader.py`, `program_*_direct_loader.py`, `day0_ops_deficit_demote_runner.py`) — только внутренняя реализация master / отладка по явной задаче человека.

### Batch (несколько дат + sim)

```bash
.venv/bin/python code/utils/dwh_batch_sim_gate.py --dates 2026-07-22
# внутри: extract_master --source dwh, затем sim на cuda13_nosb
```

### Excel legacy (не канон day0)

Только если Алексей явно просит Excel-path: interactive `extract_master` без `--source dwh`, либо `--source excel`. Demote всё равно последний шаг master.

---

## 1. Что внутри канона (справочно, не для ручной сборки)

### 3 источника из DWH (`--version-date` = report-date)

| # | Источник DWH | Таблица Project CH | Шаг внутри master |
|---|---|---|---|
| 1 | `reports.amos_heli_rotables_components_status` | `heli_pandas` | `dwh_loader --step all` |
| 2 | реестр планеров | `program_ac` | то же |
| 3 | статусы капремонта | `status_overhaul` | то же (+ enrich cascade) |

### 4 статичных Excel

| # | Excel | Таблица / потребитель |
|---|---|---|
| 1 | `Program_heli.xlsx` (`--dataset-path`) | `flight_program_ac` (MP4, spawn) |
| 2 | `Program.xlsx` (тот же path) | `flight_program_fl` (MP5) |
| 3 | `MD_Сomponents.xlsx` (`data_input/master_data/`) | `md_components` |
| 4 | `Economics.xlsx` (`master_data/`) | читает sim при старте, таблица не нужна |

Порядок фаз master (`--source dwh`):  
`md_components` → `dwh_loader --step all` → … → `program_ac_direct` → `program_fl_direct` → precheck/statuses/repair/BR → **`day0_ops_deficit_demote_runner`** → acceptance OPS==MP4.

Demote **не** внутри enrich: нужен MP4 из `flight_program_ac`. Master всегда вызывает demote последним — отдельно demote после re-enrich агенту вызывать не нужно.

### Day0: воронка статусов планеров

Канон логики: [docs/architecture/extract.md](architecture/extract.md) §Day0; приёмка — [docs/backlog.md](backlog.md) §2026-07-21.

1. overhaul → `4`; program_ac as-of → `2`; хвост → `1`
2. **3b** на `status=1`: program→calendar; calendar = treq OH(D); fallback 10y **выкл**
3. precheck D1 на `status=2`: часы на 1-й день MP5
4. **demote** (финал master): excess OPS vs MP4; + fallback `oh_at+10y−1д` только demote при hist с 2025-07-04

Destination: нет hist → planer 1 / agg 7; hist+remain>0 → 3/3; hist без календаря → 1/3.

---

## 2. Запуск симуляции: seatbelts ON / OFF

Два conda-окружения с pyflamegpu 2.0.0rc4+cuda130 (проверено 2026-07-03):

| Окружение | SEATBELTS | Назначение | Интерпретатор |
|---|---|---|---|
| **`cuda13_nosb`** (дефолт) | **OFF** (`pyflamegpu.SEATBELTS=False`) | **любые** прогоны: single-run, CUDAEnsemble, канон, замеры | `/home/albud/miniconda3/envs/cuda13_nosb/bin/python3` |
| `cuda13` | **ON** (`pyflamegpu.SEATBELTS=True`) | только явная отладка device-ошибок | `/home/albud/miniconda3/envs/cuda13/bin/python3` |

**Политика (решение Алексея 2026-07-23):** `cuda13_nosb` — дефолт для **всех** sim-прогонов, не только ансамблей. `cuda13` не использовать «на всякий случай». Результаты ON/OFF побитово идентичны (EXCEPT обеих таблиц в обе стороны = 0 на 2026-06-29). Agent-правило: `.cursor/rules/15_flame_environment.mdc`.

> ⚠️ **`conda run -n cuda13[_nosb] python3` НЕ работает** (не видит pyflamegpu). Использовать только прямые пути к интерпретаторам, как ниже. `.venv` репозитория тоже без pyflamegpu — он для ETL/валидации.

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

### Ускоренный свип: мульти-процессный шардинг под CUDA MPS

Для **множественных прогонов** (полный свип 62 ранов) есть лончер `code/utils/spawn_cap_ensemble_mps.py`: сам поднимает MPS-демон в начале, шардит caps по N процессам `spawn_cap_ensemble.py` (GPU-фаза), после завершения всех шардов выполняет loader+materializer одним проходом и **всегда гасит демон в конце** (finally).

```bash
# Полный свип 0..60 + uncapped, 4 шарда × cr=8 (замер 2026-07-03: GPU-фаза 19.4s
# против 28.0s в одном процессе cr=8; бит-идентичность 2108 .bin diff=0):
/home/albud/miniconda3/envs/cuda13_nosb/bin/python3 code/utils/spawn_cap_ensemble_mps.py \
  --version-date 2026-06-29 --input-version-id 1 --repair-quota 18 \
  --shards 4 --concurrent-runs 8 --out-base 1000 --threshold 180
```

- Режим только для множественных прогонов: одиночные/малые запуски гонять обычным `spawn_cap_ensemble.py` без MPS.
- Без MPS шардинг **вреден** (time-slicing контекстов: 43s против 28s baseline) — лончер fail-fast, если демон не поднялся.
- Логи шардов: `output/mps_shard/<label>_shardN.log`; при rc!=0 любого шарда лончер печатает хвост лога, гасит остальные шарды и MPS, выходит с 1.

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
