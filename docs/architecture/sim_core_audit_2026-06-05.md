# Аудит симуляционного ядра (активный V8-чейн) — 2026-06-05

**Workflow**: `W_sim_core_audit_20260605T102043Z` · **Тип**: read-only архитектурный аудит, кросс-проверка двумя моделями (Claude Opus 4.8 + GPT-5.5). Правок кода нет.

**Область**: активный чейн `orchestrator_limiter_v8.build_model` и подключённые им модули в `code/sim_v2/messaging/`.

> **Важно про «v5/v7» в этом отчёте.** Аудировался **только v8**. Устаревшие оркестраторы `orchestrator_limiter_v2..v7.py` и `orchestrator_gpu_only.py` — **вне области** (не читались). Везде, где ниже упоминаются «v5/v7», речь идёт о **модулях, которые сам активный v8 импортирует и вызывает** в `build_model`: `rtc_state_transitions_v7.register_phase0_deterministic` (стр. 667), `rtc_state_transitions_v7.register_post_quota_v7` (стр. 703), `rtc_limiter_v5.register_v5` (стр. 841). То есть это **зависимости v8 от legacy-модулей**, а не отдельный аудит старых оркестраторов. Именно эта зависимость — часть избыточной сложности v8. `archive/**` — вне области.

> ⚠️ Приоритет проекта — **bit-identical** поведение. Все находки ниже — кандидаты на упрощение «с сохранением семантики». Любое удаление/рефактор в `code/sim_v2/**` = high-risk, требует governance + equivalence-прогон на реальных данных ClickHouse. Этот документ — только анализ.

---

## 0. Краткий вывод

1. **Разделение ядер L1/L2 уже существует в коде** — гипотеза подтверждена фактической архитектурой (см. §3).
2. **Значительный пласт dead/inert-кода** в активном чейне: compat-контур `register_v5`, неподключённая ветка `register_quota_p2_p3_v8` + `repair_line_slots_*`, exit_date-контур, ~8 мёртвых MacroProperty + ~6 write-only, legacy-буферы base_model. Потенциальное сокращение объявлений MacroProperty >30%.
3. **v8 тянет legacy-модули v5/v7** (вызывает их из `build_model`): отсюда три реализации adaptive-min, ≥3 копии binary-search limiter по `mp5_cumsum`, двойная инициализация MP. Это дублирование внутри активного v8-чейна, а не аудит старых оркестраторов.
4. **Унификация**: единое универсальное ядро — нецелесообразно. Рекомендация — **L1 (планеры) + L2 (агрегаты/двигатели)**, что уже реализовано; выигрыш — в выносе общего «движка адаптивного шага» и параметризации хардкода Mi-8/Mi-17 через `group_by`.

Обе модели сошлись по всем четырём пунктам. Расхождения — минорные (см. §5).

---

## 1. Карта «слой/функция → MacroProperty» (фактическая)

Порядок = execution order внутри шага. R=читает, W=пишет. EnvProp/PropertyArray помечены явно. (Backbone — карта Opus, сверена с `build_model`.)

### Init / служебные слои (HostFunctions)

| # | Слой / функция | R | W |
|---|---|---|---|
| 1 | `layer_init_mp5_cumsum` (HF_InitMP5Cumsum) | — | **mp5_cumsum** (загрузка), `mp_min_limiter[0]` |
| 2 | `layer_init_repair_lines` (HF_InitRepairLines) | — | repair_line_free_days_mp, _acn_mp, _gb_mp, _rt_mp, _last_acn_mp, _last_day_mp, _bank_count_mp, _bank_lock_mp, _bank_start_mp, _bank_end_mp |
| 3 | `layer_init_v8` (HF_InitV8) | — | current_day_mp, deterministic_dates_mp, *min_dynamic_mp*, adaptive_result_mp, mp_min_limiter, *min_exit_date_mp*; env num_deterministic_dates |
| 4 | `layer_step_controller` (HF_StepController) | mp_min_limiter(+reset), deterministic_dates_mp, env num_det/current/end | current_day_mp, adaptive_result_mp, mp2_day_for_step, mp2_num_steps; env current_day/prev_day/adaptive_days/step_days |

### Транзишены и квота (agent functions)

| # | Слой / функция | R | W |
|---|---|---|---|
| 5 | save_pre_status (v8) | agent.status_id | agent.pre_status_id (без MP) |
| 6 | layer_det_spawn (HF, cond) | env current_day | новые serviceable-агенты (без MP) |
| 7 | phase0_deterministic / repair_to_svc (v7) | env current_day | repair_line_free_days_mp/_acn_mp/_gb_mp (release); agent→serviceable |
| 8 | ops_increment_v8 | **mp5_cumsum**; env current/prev/end/frames_total | agent sne/ppr/daily_today/daily_next/limiter |
| 9 | unsvc_decrement_v8 | env adaptive | agent repair_days |
| 10-11 | ops_to_storage / ops_to_unsvc (limiter==0) | agent sne/ll/ppr/oh/br/limiter | agent status_id=6/7, repair_days, repair_line_id |
| 12 | repair_line_increment_v8 | repair_line_free_days_mp/_acn_mp/_rt_mp | repair_line_gb_mp (release); agent mirror |
| 13 | repair_line_write_v8 | agent mirror | repair_line_free_days_mp/_acn_mp |
| 14 | v8_reset_flags | — | agent flag-vars (needs_demote, promoted, commit_p*, …) |
| 15 | v8_reset_buffers | — | mi8/mi17 _*_count/_total/_status_day/commit_*/candidate ← 0 |
| 16-19 | count_ops/svc/unsvc/inactive | env repair_time; agent repair_days/line_id/status_change_day | mi*_*_count[idx], mi*_*_total, status_day-буферы |
| 20 | quota_manager_bucket | **mp4_ops_counter_mi8/mi17** (PropArray), env repair_time/quota, mi*_*_total, repair_line_free_days/_rt/_acn/_bank_count_mp | qm_ops_mp (см. dead), QuotaBucket message |
| 21 | demote_v8 | adaptive_result_mp[0], env days, mp4_ops_counter, mi*_ops_total/count | agent needs_demote; mi*_demote (write-only) |
| 22 | promote_svc_bucket (P1) | QuotaBucket, mi*_svc_count | agent promoted |
| 23 | promote_unsvc_bucket (P2 decide) | QuotaBucket, mi*_unsvc_ready_count/_status_day, env | agent repair_candidate/line_id/decision_p2, mi*_commit_p2_candidate |
| 24 | promote_inactive_bucket (P3 decide) | QuotaBucket, mi*_inactive_count/_status_day | agent repair_candidate/decision_p3, mi*_commit_p3_candidate |
| 25/27 | repair_line_snapshot_v8 (+_p3) | repair_line_free_days/_acn/_bank_count/_bank_end_mp | *_ro_mp (free_days/acn/bank_count/bank_head_end) |
| 26 | promote_unsvc_commit (P2) | *_ro_mp, candidate, env | repair_line_* (CAS); agent repair_claim_*, mi*_approve |
| 28 | promote_inactive_commit (P3) | *_ro_mp, candidate | repair_line_* (CAS); agent repair_claim_*, mi*_approve_s1 |
| 29 | post_quota_v7 ops_demote (cond) | agent needs_demote | agent→serviceable |
| 30 | post_quota_v7 svc_to_ops (P1) | **mp5_cumsum** | mi*_approve_s3, mi*_commit_p1; agent commit_p1/limiter/status→2 |
| 31 | post_quota_v7 unsvc_to_ops (P2) | **mp5_cumsum** | mi*_commit_p2; agent ppr=0/limiter/status→2 |
| 32 | post_quota_v7 inactive_to_ops (P3) | env mi17_br2_const; **mp5_cumsum** | mi*_commit_p3; agent ppr/limiter/status→2 |
| 33 | repair_line_export_v8 | repair_line_free_days/_acn/_rt/_gb/_bank_count/_bank_start/_bank_end_mp | rl_buf_* |
| 34 | post_quota_counts_v8 (reset+recount) | agent states | mi*_*_count/_total (пересчёт) |
| 35 | spawn_dynamic_mgr_v8 | **mp4_ops_counter**, mi*_ops_count, env dynamic_reserve | spawn_dynamic_need/base_idx/base_acn (+mi8); mgr-вары |
| 36 | spawn_dynamic_ticket_v8 (+mi8) | spawn_dynamic_*; **mp5_cumsum** | новые ops-агенты |
| 37 | L_limiter_min (rtc_compute_min_limiter) | agent.limiter | **mp_min_limiter[0]** (min) |
| 38 | layer_mp2_write | agent vars (25) | mp2_* (19 dynamic + 6 static) |

### Exit / init (вне слоёв шага)
- **HF_MP2_Drain** (ExitFunction): R mp2_*, mp2_num_steps, mp2_day_for_step.
- **HF_RepairLineDrain** (ExitFunction): R rl_buf_*, mp2_num_steps, mp2_day_for_step.
- **HF_InitV5** (addInitFunction, через `register_v5`): W current_day_mp, adaptive_result_mp, *limiter_buffer*, *program_changes_mp* (инертно — см. §2).
- **HF_ExitConditionV8**: R current_day_mp[0] vs end_day.

### «Горячие» MacroProperty (реально нужны)
- **mp5_cumsum** — единственный источник limiter (R в #8, #30–32, #36). RO после загрузки.
- **repair_line_*_mp** (live + *_ro_mp снапшоты) — полный RW-цикл RepairLine.
- **mp4_ops_counter_mi8/mi17** — цели ops (#20, #21, #35).
- **mp_min_limiter / current_day_mp / adaptive_result_mp / deterministic_dates_mp** — ядро адаптивного шага.

### Мёртвые MacroProperty (создаются, не читаются в активном чейне) — сверено grep'ом

| MacroProperty | Создаётся | Статус |
|---|---|---|
| **quota_left_mp** | `rtc_quota_v8.py:1468` | Единственное упоминание. Полностью мёртвый ✓ |
| min_dynamic_mp | setup_v8_macroproperties | Читатели — незарегистрированные RTC_COLLECT/COMPUTE_GLOBAL_MIN_V8 |
| min_exit_date_mp | HF_InitV8 | Читатель — `register_exit_date_copy` (не вызван, `:672`) |
| program_changes_mp | orchestrator + HF_InitV5 | Читатель — compute_global_min_v5 (не вызван) |
| limiter_buffer | setup_v8 + дубль `:~833` | Читатели — v5/adaptive (вне чейна) |
| mp_program_changes_v3, num_program_changes_v3 | setup_limiter_macroproperties | Читатель — HF_ComputeAdaptiveDays (не используется) |
| repair_line_slots_all/_days/_count_mp | build_model | Читаются только в `register_quota_p2_p3_v8` (`:1476`, не вызван) |
| mp2_quota_gap_mi8/mi17 | base_model_messaging | Не R/W в активном чейне |

### Write-only (пишутся, не читаются активными слоями — кандидаты, нужна точечная проверка телеметрии)
`qm_ops_mp` (spawn читает `mi*_ops_count`, не его), `mi*_demote`, MP-зеркала `mi*_approve/_approve_s1/_approve_s3/commit_p1/p2/p3` (экспортируется agent-переменная, не MP), большой пласт legacy base_model-буферов (qm_*_idx/count, quota_decision, repair_state_buffer, mp2_transition_*, …).

---

## 2. Аудит избыточной сложности логики

### Критическое (явный dead-code в активном чейне) — сверено grep'ом
1. **`register_v5` — инертный compat-шим** (`orchestrator_limiter_v8.py:~849`). В активном build_model даёт только `HF_InitV5`, которая инициализирует мёртвые MP (`program_changes_mp`, `limiter_buffer`) и повторно зануляет уже инициализированные HF_InitV8 `current_day_mp`/`adaptive_result_mp`. `register_v5_final_layers` намеренно не вызван (`:854`, сверено: вызывается только в v5/v6/v7-оркестраторах). Обоснование «нужно V7-модулям» — не подтверждается: активные v7-функции не читают `program_changes_mp`.
2. **Неподключённая ветка P2/P3 «slots»** — `register_quota_p2_p3_v8` (`rtc_quota_v8.py:1476`) определена, но не вызывается (заменена bucket-механизмом #23–28). Тянет мёртвые `repair_line_slots_*`.
3. **Мёртвые импорты** `rtc_quota_v8.py:50-51`: `register_publish_report`, `register_apply_decisions` импортированы, не вызваны.
4. **exit_date-контур** (`min_exit_date_mp` / `register_exit_date_copy`) — закомментирован в v8 (`:672`); MP инициализируется, не читается.

### v8 тянет legacy-модули v5/v7 → дублирование (DRY)

> Ниже — про модули v5/v7, которые **вызывает сам v8** (см. область вверху), а не про устаревшие оркестраторы.

5. **Три реализации «adaptive global min»**: v5, v8 (`RTC_COMPUTE_GLOBAL_MIN_V8`) и фактически активный `HF_StepController`. Активен только третий.
6. **≥3 копии binary-search `compute_limiter` по `mp5_cumsum`** (`rtc_state_transitions_v7`, `rtc_limiter_optimized`, spawn inline, + Python `precompute_events`). Кандидат на единый device-include.
   > **Уточнение (2026-06-05, проверено грепом):** заявленное здесь расхождение boundary-конвенций `>`/`>=` между *активными* RTC-копиями **не подтвердилось**. Две активные RTC-копии (`rtc_state_transitions_v7` для P1/P2/P3 и `rtc_spawn_dynamic_v7` для dynamic spawn) **байт-идентичны** (sha256 совпадают, обе `>=`). Граница `>` живёт только в мёртвом коде (`rtc_limiter_optimized.RTC_COMPUTE_LIMITER_ON_ENTRY`, регистрация закомментирована) и в Python day-0 init (`agent_population.compute_limiter_for_agent`). **Сделано:** активная device-функция вынесена в общий `rtc_compute_limiter_device.py` (DRY, byte-identical, плейсхолдер `__CUMSUM_SIZE__` сохранён). Вопрос Python `>` vs RTC `>=` на day-0 — отдельная тема (на текущих данных bit-identical к baseline).
7. **Двойная инициализация MP** (HF_InitV5 + HF_InitV8), двойное создание `limiter_buffer`/`program_changes_mp`, аллокации в `try/except` (нарушает правило «no try/except для угадывания API»).

### Пересечение ответственности (SOLID/KISS)
8. **HF_StepController сверхответственен**: adaptive_days + продвижение дня + запись MP2 day_for_step/num_steps + 4+ env-переменных (смешаны «шаг» и «экспорт»).
9. **post_quota разнесён v7↔v8**: решение «кого повышаем» — в v8-bucket-слоях (#22–28), сам переход — в v7-функциях (#30–32). Размазанная трассировка.
10. **Двойной подсчёт счётчиков** (#16–19 и #34) — функционально необходим (пересчёт после промоушенов), но стоит явно задокументировать инвариант.

---

## 3. Оценка унификации ядра

### Ключевой факт: разделение L1/L2 уже реализовано
- **L1 (планеры)** — аудируемый V8-чейн (`orchestrator_limiter_v8.py`), жёстко привязан к `group_by IN (1,2)` (Mi-8/Mi-17).
- **L2 (агрегаты/двигатели)** — отдельное ядро `code/sim_v2/units/` (27 файлов: `orchestrator_units.py`, `base_model_units.py`, `rtc_units_fifo*`, `rtc_units_state_*`, `rtc_units_spawn`, `mp2_drain_units.py`, `planer_dt_loader.py` и др.). Обрабатывает агрегаты и потребляет выход планеров (читает `sim_masterv2_v9`) как внешний сигнал.

Связь «внешние данные → планеры» и «планеры → двигатели» реализована как **data-feed между ядрами** (L1 пишет `sim_masterv2_v9` → L2-loader читает), а не как одно рекурсивное ядро.

### Что специфично для планеров (мешает универсальному ядру)
- Хардкод Mi-8/Mi-17: пары `mi8_*`/`mi17_*`, `mp4_ops_counter_mi8/mi17`, ветвление `group_by==1/2`, `mi17_br2_const`. Два типа «зашиты» в имена MP, а не параметризованы измерением `group_by`.
- Квоты по ops-таргетам (`mp4_ops_counter`) — планерная концепция «N бортов в operations/день».
- RepairLine (claim/commit/bank, P2/P3) — конечное число линий ремонта планеров (REPAIR_LINES_MAX=64).
- Dynamic spawn Mi-17/Mi-8 — рост парка планеров.

### Что специфично для агрегатов (L2)
FIFO-приоритет, reserve-переходы, assembly_trigger, синхронизация с планером-носителем — другая дисциплина обслуживания (агрегат «снимается/ставится», а не летает по ll/oh).

### Реально переиспользуемый каркас
- Адаптивный шаг (`current_day_mp`/`mp_min_limiter`/deterministic dates/StepController) — доменно-нейтрален.
- MP2-экспорт (паттерн общий; L2 имеет свой `mp2_drain_units`).
- save_pre_status, limiter по накопленному ресурсу (binary search по cumsum), counts/reset-инфраструктура.

### Вывод: **L1 + L2 (НЕ одно универсальное ядро)** — с движением к общему «движку шага»
1. Разные доменные сущности: планер расходует ресурс налётом + RepairLine; агрегат живёт в FIFO/reserve, синхронизируясь с носителем. Принудительная унификация = «универсальная» конфигурация под двух разных потребителей = anti-overengineering (запрещён правилами).
2. Каскад уровней L1→L2 (двигатель зависит от планера-носителя). Схема «прогон L1 → фид → прогон L2» естественно отражает причинность и уже работает.
3. Хардкод Mi-8/Mi-17 — это **внутрипланерная** проблема, а не «уровневая»: первый и самый безопасный кандидат на абстракцию (параметризовать `miX_*` → массивы по `group_by`), окупается независимо от вопроса уровней.

### Рекомендуемые абстракции (если двигаться к общему ядру)
- **Generic «AdaptiveStepEngine»**: вынести StepController + limiter-min + deterministic dates + MP2 в доменно-нейтральный модуль для L1 и L2 (низкий риск, высокая отдача).
- **Параметризация типов через `group_by`** вместо `mi8_*`/`mi17_*` пар.
- **Pluggable resource/maintenance policy**: L1 = (налёт ll/oh + RepairLine), L2 = (FIFO/reserve); общий каркас «count → quota-decision → commit → transition».

### Trade-offs
| Вариант | + | − |
|---|---|---|
| **L1+L2 (рекомендация)** | соответствие домену и причинности; меньше риск регрессий | дублирование step/limiter-инфраструктуры (снимается generic-движком) |
| Универсальное ядро | единая кодовая база | высокий риск; гигантский bit-identical-аудит; «универсальная» политика = скрытая сложность; ломает рабочую L1→фид→L2 |
| Не делить | — | неприменимо: код уже разделён, слияние = регресс |

---

## 4. Приоритизация (предложение, БЕЗ исполнения)

Все пункты — high-risk (`code/sim_v2/**`), требуют отдельного approval + equivalence-прогона. Порядок по соотношению выигрыш/риск:

1. **Удаление полностью мёртвых MP** (`quota_left_mp` и подтверждённо нечитаемые) — минимальный риск, но обязателен полный grep + прогон.
2. **Удаление `register_v5`-контура** (инертный шим) — средний риск (init-порядок), нужен bit-identical.
3. **Удаление неподключённой ветки `register_quota_p2_p3_v8` + slots-MP** и мёртвых импортов.
4. **Единый device-include `compute_limiter`** (устранить ≥3 копии binary-search) — снижает риск рассинхрона boundary-условий.
5. **Вынос generic AdaptiveStepEngine** — архитектурный, согласование.
6. **Параметризация Mi-8/Mi-17 по `group_by`** — крупный, отдельный workflow.

---

## 5. Кросс-проверка двух моделей

**Согласие (обе модели):** карта слой→MacroProperty; перечень мёртвых MP; инертность `register_v5`; дублирование из-за того, что v8 вызывает legacy-модули v5/v7 (adaptive-min, compute_limiter); вывод L1+L2 вместо универсального ядра; bit-identical как обязательный гейт.

**Расхождения (минорные):**
- **Risk-tier самого аудита**: GPT-5.5 — `high`, Opus — `low`. Разрешение: аудит read-only = **low**; предлагаемые правки = high (отдельный gate).
- **`register_v5`**: GPT — «подозрительно, без прогона не подтвердить инертность»; Opus — «инертен, безопасно удалить после equivalence». Обе требуют прогона; Opus конкретнее.
- **L2-ядро**: Opus явно нашёл существующий `code/sim_v2/units/` (с неточностью в именах: фактически `orchestrator_units.py` и `planer_dt_loader.py`, не `_v1`/`planer_l2_loader`); GPT обсуждал унификацию более абстрактно, без указания на готовое L2-ядро. Факт существования L2-ядра подтверждён (27 файлов).

**Источники:** независимые read-only прогоны Opus 4.8 и GPT-5.5; ключевые факты (`quota_left_mp`, `register_quota_p2_p3_v8`, `register_v5_final_layers`, мёртвые импорты, наличие `code/sim_v2/units/`) сверены оркестратором grep'ом.
