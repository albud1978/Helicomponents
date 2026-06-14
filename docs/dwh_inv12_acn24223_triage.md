# INV-12 triage: планер acn=24223 (срез 2026-05-20)

**Branch:** `feature/dwh-bb8`  
**Workflow (investigation):** `W_inv12_acn24223_20260520`  
**Parent context:** batch sim-gate `docs/dwh_sim_gate.md`

## Терминология (проект)

| Термин | В sim / heli_pandas |
|---|---|
| **Планer** | `group_by IN (1, 2)` — Mi-8 / Mi-17, основа multiBOM |
| **Агрегат** | `group_by > 2` — компоненты на борту |
| **Борт / вертолёт** | планер + агрегаты; в таблицах **`aircraft_number` у планера = serialno/регистрация планера** (24223), не «сборка целиком» |

## INV-12 (SSoT)

- **Инвариант:** `forall agent where group_by in {1,2}: ppr <= oh` (`config/transitions/invariants.json`, INV-12)
- **Validator:** `code/validation/inv12_ppr_le_oh.py` — post-sim, **вся траектория** `sim_masterv2_v9`, без фильтра по `status_id`
- **Агрегаты не проверяются**

## Симптом batch (2026-05-20)

| Метрика | Значение |
|---|---|
| INV-12 violations | **85** |
| Это 85 планеров? | **Нет** — **85 day-rows одного планера** |
| Нарушитель | **1 планer:** `aircraft_number=24223`, `group_by=1` (Ми-8Т, `partseqno_i=70387`) |
| Агрегаты `ppr>oh` в sim | **0** |

## На каком этапе обнаружено?

| Этап | `ppr > oh` (только planners) |
|---|---|
| `heli_pandas` вход extract | 9 **других** планеров с `ppr>oh` в DWH-полях; **24223 не среди них** |
| sim `day=0`, `day=1` | **0** |
| sim первое нарушение | **day=2293** |
| sim последнее | **day=3650** (85 шагов подряд в st=7) |

**Вывод:** FAIL — **динамика sim в конце горизонта**, не дефект загрузки на t=0.

## Вход vs нормы (24223)

| Источник | ppr | oh | status_id |
|---|---:|---:|---|
| heli_pandas 2026-05-20 | 151 044 | 180 000 (DWH) | 2 (ops) |
| heli_pandas 2026-06-12 | 154 707 | 180 000 | 2 |
| md_components / sim mp1 | — | **270 000** (`oh_mi8`) | — |

Sim использует **`oh=270000` из md_components**, не 180000 из extract.

## Траектория sim (ключевые переходы)

### 2026-05-20 — **FAIL**

```
day=1     st=2 ops          ppr=151134  oh=270000
day=1364  st=3 serviceable  ppr=269908  oh=270000  (у порога)
day=2288  st=2 ops          ppr=269908  oh=270000
day=2293  st=7 unsvc       ppr=270398  oh=270000  (+398, INV-12)
… day 2293..3650 — 85 строк ppr>oh в st=7
```

### 2026-06-12 — **PASS** (тот же планер)

```
day=1     st=2 ops   ppr=154797  oh=270000
day=1318  st=7 unsvc ppr=269939  oh=270000  (−61 до порога)
```

На 12.06 переход в unserviceable **до** превышения oh; на 20.05 sim дал краткий возврат в ops (2288–2292) и перелёт **+398 мин**.

## Гипотезы (open)

1. **Limiter / quota / MP4** — разный тайминг demote/promote для Mi-8 ops между срезами (разный начальный `ppr/sne`, состав флота ops=171 на обоих).
2. **Путь через st=3** — на 20.05: ops → serviceable → ops → unsvc; на 12.06: ops → unsvc напрямую.
3. **Не DWH-loader** — вход 24223 корректен; 9 «грязных» ppr>oh на входе — другие борта, sim их не ломает по INV-12 на 20.05.

## План исследования (следующий workflow)

**Risk:** high (`code/sim_v2/**`, RTC/limiter)

1. **Repro:** фиксированный прогон `version_date=2026-05-20`, фильтр логов по `acn=24223`.
2. **Счётчики:** `ppr`, `oh`, `sne`, `limiter`, `daily_today_u32`, `status_id`, MP4 target Mi-8 — по дням 2280–2300 и 1310–1325 (эталон 12.06).
3. **Diff срезов:** `heli_pandas` + `program_ac` + `status_overhaul` + flight_program для 24223 / Mi-8 квоты.
4. **Root cause:** почему limiter не отправил в st=7/repair до `ppr>oh` на 20.05.
5. **Fix:** только после согласования архитектуры (RTC / limiter / invariant interpretation).

**SuccessCriteria (investigation):**

- `SQL:` траектория 24223 day 2288–2293 с полями limiter/dt/status задокументирована
- `invariant:` воспроизведение INV-12 FAIL на 20260520 и PASS на 20260612 для одного acn
- `manual-check:` root-cause hypothesis с evidence (sim log или SQL), согласована с человеком до patch

## Root cause (R1, research-graph-analyst + SQL, 2026-06-14)

**Статус:** подтверждена цепочка в sim_v2 (high-risk, fix — только после согласования).

1. **Demote 2→3 @ day=1364** при `ppr=269908` (остаток OH ≈ 92 мин) — планер «заморожен» в serviceable.
2. **Repromote 3→2 @ day=2288** — P1-квота выбирает кандидата по `rank=idx` **без** проверки `oh-ppr`; inline-limiter → `limiter=0`.
3. **`limiter=0` skip в reduction** (`rtc_limiter_optimized.py`) — агент не зажимает adaptive-шаг.
4. **Adaptive jump 2288→2293** — инкремент ~490 мин → `ppr=270398` (+398); **ops→unsvc** проверяется look-ahead **после** инкремента (`rtc_state_transitions_v8.py`).

На **2026-06-12** нет цикла 2→3→2 у порога: прямой выход 2→7 @ day=1318 при `ppr=269939 < oh`, с `limiter=3` за шагом раньше.

**Код (точки):**

| Файл | Суть |
|---|---|
| `code/sim_v2/messaging/rtc_quota_v8.py` | P1 promote по idx, без OH-запаса |
| `code/sim_v2/messaging/rtc_state_transitions_v7.py` | svc→ops + inline limiter |
| `code/sim_v2/messaging/rtc_compute_limiter_device.py` | `days_to_oh=0` у порога |
| `code/sim_v2/messaging/rtc_limiter_optimized.py` | skip `limiter==0` в min-reduction |
| `code/sim_v2/messaging/rtc_state_transitions_v8.py` | ops→unsvc после инкремента |
| `code/sim_v2/messaging/orchestrator_limiter_v8.py` | порядок фаз: ops → quota → post-quota |

**Кандидаты fix (human gate):** OH-aware promote; guard `limiter=0` → adaptive=1 или немедленный 2→7; пересмотр момента проверки INV-12.

## Evidence (локально, не в git)

- `output/dwh_sim_batch/sim_gate_2026-05-20.log`
- `output/dwh_sim_batch/sim_gate_2026-06-12.log` (из `output/sim_gate_2026-06-12_validation.log`)
- `output/dwh_sim_batch/summary.txt`

## Связанные документы

- `docs/dwh_sim_gate.md` — batch-regression
- `docs/changelog.md` — секция 2026-06-13
- `config/transitions/invariants.json` — INV-12
