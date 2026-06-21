# DWH sim-gate: приёмка среза через симуляцию

**Branch:** `feature/dwh-bb8`  
**Целевой срез:** `heli_pandas` `version_date=2026-06-12`, `version_id=1`  
**Workflow:** `W_dwh_sim_gate_20260612` (Agent KG)

## Зачем

Extract/load + post-enrichment **недостаточны**. Пригодность DWH-среза для GPU подтверждается только:

1. Прогоном **LIMITER V8** → `sim_masterv2_v9`, `sim_repairline_v9`
2. Post-sim validators **INV-1…INV-12** (+ TEMP по SSoT) — `code/validation/run_all.py`

Churn/regression по агрегатам (`docs/dwh_aggregate_churn_analytics.md`) — **pre-sim**, не финальный verdict.

## Предусловия (Project CH)

| Таблица | version_date | Примечание |
|---|---|---|
| `heli_pandas` | 2026-06-12 v1 | DWH load + `dwh_post_enrichment.py` |
| `program_ac`, `status_overhaul` | 2026-06-12 v1 | DWH load |
| `md_components` | 2026-06-11 v1 | Excel SSoT (не в DWH daily) |
| `flight_program_fl`, `flight_program_ac` | **2026-06-12 v1** | **Не в DWH**; interim: клон из 2026-04-08 (Program.xlsx вне scope DWH) |

> Assumption (interim): программа полётов не менялась между 08.04 и 12.06; для sim-gate клонируем `flight_program_*` на дату среза. Отдельная загрузка Program — follow-up через `extract_master`.

## Команды

### 0. KG (traceability)

```bash
export AGENT_KG_WORKFLOW_ID=W_dwh_sim_gate_20260612
# или init через agent_kg.py --init-workflow
```

### 1. Flight program (interim clone)

```sql
-- flight_program_fl / flight_program_ac: INSERT SELECT 2026-04-08 → 2026-06-12
```

### 2. Sim (FLAME — conda cuda13)

```bash
export CUDA_PATH="$HOME/miniconda3/targets/x86_64-linux"
export LD_LIBRARY_PATH="$HOME/miniconda3/lib:$LD_LIBRARY_PATH"
export AGENT_KG_WORKFLOW_ID=W_dwh_sim_gate_20260612

/home/albud/miniconda3/envs/cuda13/bin/python3 \
  code/sim_v2/messaging/orchestrator_limiter_v8.py \
  --version-date 2026-06-12 \
  --end-day 3650 \
  --max-steps 10000
```

### 3. Validators

```bash
python3 code/validation/run_all.py \
  --version-id 1 \
  --version-date 20260612 \
  --table-main sim_masterv2_v9 \
  --table-repair sim_repairline_v9
```

## SuccessCriteria

- `script`: orchestrator exit 0, строки в `sim_masterv2_v9` / `sim_repairline_v9` для `version_date=20260612`, `version_id=1`
- `invariant`: INV-1…INV-12 PASS (`run_all.py`)
- Extract smoke (отдельно): `status_id=0` count = 0 на входе sim

## Результат прогона (2026-06-13)

**Workflow:** `W_dwh_sim_gate_20260612` | **Branch:** `feature/dwh-bb8`

| Шаг | Результат | Evidence |
|---|---|---|
| Клон `flight_program_*` 2026-04-08 → 2026-06-12 | OK | `flight_program_fl=1_392_000`, `flight_program_ac=4_000` |
| `orchestrator_limiter_v8.py` | **PASS** exit 0 | `output/sim_gate_2026-06-12_orchestrator.log` — 264 шага, 82_350 строк masterv2, 65_700 repairline |
| `run_all.py` INV-1…INV-12 | **PASS** (12/12) | `output/sim_gate_2026-06-12_validation.log` |
| TEMP-5 (claim metadata) | **FAIL** (5 mismatch day=180, 4→2 без RL claim) | **исправлено** 2026-06-16 — см. секцию ниже (`W_repair_time_perboard_20260616`) |

**Assumption зафиксирована:** flight program клонирован с 2026-04-08 (Program.xlsx вне DWH scope).

**Verdict sim-gate (2026-06-13, до fix):** **PASS** по обязательным INV-1…INV-12; TEMP-5 — follow-up (закрыт 2026-06-16).

## Post-fix: per-board repair_time (`W_repair_time_perboard_20260616`, 2026-06-16)

**Суть:** day-0 status=4 планеры получают per-board `repair_time` из активного `status_overhaul` (полный цикл); `repair_days` = elapsed. Sim: `exit_date = repair_time − repair_days`. Сброс к стандарту (180) — при переходе 2→7.

**Целевой срез `2026-06-12 v1` (re-run после правки):**

| Шаг | Результат | Evidence |
|---|---|---|
| `orchestrator_limiter_v8.py` | **PASS** exit 0 | `output/sim_gate_2026-06-12_orchestrator.log` |
| `run_all.py` INV-1…INV-12 + TEMP-1/4/5 | **PASS** (15/15) | validator-judge V1; TEMP-5 mismatch=0, TEMP-4 violations=0 |
| Acceptance SQL (22485/22517) | first exit **day=202** (не 180), `repair_time` 218/224 | handoff `…validator-judge_550688db` |

**Verdict sim-gate (post-fix):** **PASS** по INV-1…INV-12 **и** TEMP-1/4/5; полная приёмка DWH-среза `2026-06-12 v1` для GPU — **подтверждена**.

## Batch-regression: 5 срезов после 2026-04-08 (2026-06-13)

**Скрипт:** `code/utils/dwh_batch_sim_gate.py`  
**Assumption:** `flight_program_*` клон с `2026-04-08` на каждую дату (как для 12.06).

| version_date | heli_pandas | OPS planers | sim rows | INV-1…INV-12 | Примечание |
|---|---:|---:|---:|---|---|
| 2026-04-15 | 11 499 | 171 | 82 100 | **PASS** | |
| 2026-05-01 | 11 500 | 171 | 83 904 | **PASS** | |
| 2026-05-20 | 11 530 | 171 | 81 863 | **FAIL** | INV-12: **85 day-rows**, **1 планер** acn=24223 (gb=1), day 2293–3650; см. `docs/dwh_inv12_acn24223_triage.md` |
| 2026-06-05 | 11 533 | 169 | 80 890 | **PASS** | |
| 2026-06-11 | 11 540 | 169 | 82 703 | **PASS** | |
| 2026-06-12 *(ранее)* | 11 540 | 169 | 82 350 | **PASS** | |

**Итог batch (2026-06-13, до fix):** 5/5 загрузок OK, **4/5 sim-gate PASS**, 1 дата с INV-12 FAIL → triage **планер 24223** (не load, не агрегаты).

**Follow-up workflow:** `W_inv12_acn24223_20260520` — алгоритм limiter/quota для acn=24223 (`docs/dwh_inv12_acn24223_triage.md`).

**Evidence:** `output/dwh_sim_batch/sim_gate_*.log`, `output/dwh_sim_batch/summary.txt`.

**Fix infra (batch):** `dwh_loader._batch_insert` и `dual_loader.insert_data` — явный список колонок при INSERT (таблица 33 col, DataFrame 26 col).

### Batch re-run после per-board repair_time (2026-06-16)

**Workflow:** `W_repair_time_perboard_20260616` | **Скрипт:** `dwh_batch_sim_gate.py` (re-run 5 дат)

| version_date | INV-1…INV-12 | TEMP-1/4/5 | Примечание |
|---|---|---|---|
| 2026-04-15 | PASS | PASS (15/15) | |
| 2026-05-01 | PASS | PASS (15/15) | |
| 2026-05-20 | PASS | PASS (15/15) | |
| 2026-06-05 | PASS | PASS (15/15) | TEMP-5 mismatch=0 (был FAIL day=180 до fix) |
| 2026-06-11 | PASS | PASS (15/15) | TEMP-5 mismatch=0 (был FAIL day=180 до fix) |

**Итог re-run:** **5/5 PASS** по INV-1…INV-12 + TEMP-1/4/5; TEMP-5 mismatch=0, TEMP-4 violations=0 на всех датах.

**Triage (не блокирует sim-gate):**
- Idempotency gap экспортёра планеров (INSERT без DELETE по version slice) — дубли при повторном прогоне; обход: ручная очистка среза.
- TEMP-4 порог 210: в батче max remaining=209; при срезе вскоре после `act_start` капремонта remaining может превысить 210 — решение за Алексеем.

## Прогон 2026-06-20 (date-based load, `W_dwh_sim_gate_20260620`, 2026-06-21)

**Дата среза = 2026-06-20** (не 21-06): витрина `reports.amos_heli_rotables_components_status` имеет max `report_date=2026-06-20`; на 2026-06-21 строк нет.

### Воспроизведение (по шагам)

```bash
# 0. ОБЯЗАТЕЛЬНО: CA-сертификат DWH (иначе SSL FileNotFoundError на program_ac)
export DWH_CLICKHOUSE_CA_CERT=/media/DATA_BIG/Projects/Heli/Helicomponents/config/certs/yandex_cloud_RootCA.pem

# 1. Загрузка из DWH по выбранной дате (all = program_ac+status_overhaul+status_components+enrich)
.venv/bin/python code/utils/dwh_loader.py --report-date 2026-06-20 --version-id 1 --step all --skip-existing

# 2. Клон flight_program (Program.xlsx вне DWH scope) 2026-04-08 -> 2026-06-20
#    (через dwh_batch_sim_gate.clone_flight_program или INSERT SELECT)

# 3. Sim V8 (conda cuda13)
export CUDA_PATH="$HOME/miniconda3/targets/x86_64-linux"
export LD_LIBRARY_PATH="$HOME/miniconda3/lib:$LD_LIBRARY_PATH"
/home/albud/miniconda3/envs/cuda13/bin/python3 \
  code/sim_v2/messaging/orchestrator_limiter_v8.py --version-date 2026-06-20 --end-day 3650 --max-steps 10000

# 4. Инварианты
.venv/bin/python code/validation/run_all.py --version-id 1 --version-date 20260620 \
  --table-main sim_masterv2_v9 --table-repair sim_repairline_v9
```

> ⚠️ Повторный `--step enrich` на уже обработанном `heli_pandas` падает (`repair_days: required argument is not an integer`): post-enrichment читает обогащённый срез, где `repair_days` уже не raw. Чистый перезапуск enrich — только после восстановления `heli_pandas` из `status_components` (`--no-enrich`), затем `--step enrich`.

### Результат

| Шаг | Результат | Evidence |
|---|---|---|
| DWH load | OK | heli_pandas=11 480, program_ac=174, status_overhaul=58, heli_raw=202 216 |
| Клон `flight_program_*` 2026-04-08 → 2026-06-20 | OK | fl=1 392 000, ac=4 000 |
| `orchestrator_limiter_v8.py` | **PASS** exit 0 | 251 шаг; masterv2=78 842, repairline=65 700; ops=target (Mi-8 10/10, Mi-17 121/121) |
| `run_all.py` INV-1…12 + TEMP-1/4/5 | **PASS** (15/15) | `output/dwh_load_sim_20260620/validation.log` |
| Полная валидация датасета | **ГОДЕН** | `output/dwh_load_sim_20260620/dataset_validation_20260620.txt` |

**Валидация датасета (DQ):** `status_id=0`=0; планеры gb(1,2) ops с превышением ресурса=0; `partseqno_i↔group_by` 1:1; 18 точных дублей AMOS (компоненты, 0.16%, к сведению); `ppr>oh`/`sne>ll` в осн. в статусах 6/7 (норма).

### Тайминг (wall-clock, cold run) — `output/dwh_load_sim_20260620/timing_summary.txt`

- **Extract+Load ~107 s**: `status_components` **56.4 s** (самый долгий), `enrich` 43.4 s (в нём `repair_days_calculator.py` **14.8 s**), program_ac/overhaul ~3.5 s каждый. Чистый DWH fetch — 3.3 s.
- **Sim V8 225.6 s**: GPU `simulate()` 3.2 s; остальное — RTC compile + MP2 drain + CH export.
- **Validation 12.1 s**. Pipeline итого **~345 s (~5.8 min)**.

**Verdict 2026-06-20:** sim-gate **PASS** (INV-1…12 + TEMP-1/4/5 = 15/15); датасет годен для GPU.

## Связанные документы

- `docs/changelog.md` — секция **2026-06-21** (date-based load, sim-gate 06-20, engine repair export); **2026-06-16** (per-board repair_time, TEMP-5 fix); секции 2026-06-13 (retract aircraft.status, sim-gate обязателен)
- `output/dwh_admin_request_amos_heli_od_detail_fields.md` — запрос админам DWH (поля `od_detail`: orig_target_date/confirmed_date/del_date)
- `docs/de_tasks.md` — приёмка DWH-среза
- `docs/validation_capsule.md` — INV registry
- `config/transitions/invariants.json` — SSoT инвариантов
