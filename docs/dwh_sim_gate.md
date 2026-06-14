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
| TEMP-5 (claim metadata) | **FAIL** (5 mismatch day=180, 4→2 без RL claim) | follow-up RTC/repairline; **не блокирует** sim-gate по INV-1…INV-12 |

**Assumption зафиксирована:** flight program клонирован с 2026-04-08 (Program.xlsx вне DWH scope).

**Verdict sim-gate:** **PASS** по обязательным INV-1…INV-12; полная приёмка DWH-среза `2026-06-12 v1` для GPU — **подтверждена**.

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

**Итог batch:** 5/5 загрузок OK, **4/5 sim-gate PASS**, 1 дата с INV-12 FAIL → triage **планер 24223** (не load, не агрегаты).

**Follow-up workflow:** `W_inv12_acn24223_20260520` — алгоритм limiter/quota для acn=24223 (`docs/dwh_inv12_acn24223_triage.md`).

**Evidence:** `output/dwh_sim_batch/sim_gate_*.log`, `output/dwh_sim_batch/summary.txt`.

**Fix infra (batch):** `dwh_loader._batch_insert` и `dual_loader.insert_data` — явный список колонок при INSERT (таблица 33 col, DataFrame 26 col).

## Связанные документы

- `docs/changelog.md` — секции 2026-06-13 (retract aircraft.status, sim-gate обязателен)
- `docs/de_tasks.md` — приёмка DWH-среза
- `docs/validation_capsule.md` — INV registry
- `config/transitions/invariants.json` — SSoT инвариантов
