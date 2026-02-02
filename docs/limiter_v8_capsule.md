# Context Capsule — limiter_v8

## Scope
Трогаем:
- `code/sim_v2/messaging/orchestrator_limiter_v8.py` (основной оркестратор V8) — `README.md`
- `docs/architecture/limiter_architecture.md` (источник истины V8) — `docs/architecture/limiter_architecture.md`
- `docs/architecture/rtc_pipeline_architecture.md` (MP5/mp5_lin инварианты) — `docs/architecture/rtc_pipeline_architecture.md`
- `docs/architecture/validation_rules.md` (валидация/инварианты/SQL-first) — `docs/architecture/validation_rules.md`
- `config/transitions/transitions_rules.json` (матрица переходов state→state) — `docs/architecture/limiter_architecture.md`
- `config/transitions/quota_rules.json` (логика квот/RepairLine) — `docs/architecture/limiter_architecture.md`
- `tools/transitions_viewer/index.html` (визуализация переходов/квот) — `docs/architecture/limiter_architecture.md`

Не трогаем:
- `archive_vnv_cpu_project/` (архив, запрещено правилами) — `.cursorrules`
- Архивные версии оркестраторов V1–V7 (не использовать) — `README.md`

## Invariants (≤12)
- V8 — источник истины; противоречия в других документах считаются устаревшими — `docs/architecture/limiter_architecture.md`
- Для unsvc exit_date удалён; RepairLine — основной механизм ремонта — `docs/architecture/limiter_architecture.md`
- `repair_days`: для unsvc декрементируется до 0; для inactive всегда 0 — `docs/architecture/limiter_architecture.md`
- Ресурс: проверка `sne + dt >= ll` (next-day dt), а не post-increment — `docs/architecture/limiter_architecture.md`
- `limiter=0` обязан приводить к выходу; инициализация допускает 0 — `docs/architecture/limiter_architecture.md`
- `MAX_DETERMINISTIC_DATES=500`; лишние даты отбрасываются — `docs/architecture/limiter_architecture.md`
- MP5 длиной `(DAYS+1)*FRAMES`, индексация `row = day*FRAMES + idx` — `docs/architecture/validation_rules.md`
- `mp5_lin` read-only после инициализации — `docs/architecture/validation_rules.md`
- Warning'и NVRTC запрещены; при обнаружении — исправить и очистить кэш — `docs/architecture/validation_rules.md`
- Один агент делает максимум один переход за шаг — `docs/architecture/validation_rules.md`
- Порядок слоёв = порядок регистрации, критичен для логики — `docs/architecture/limiter_architecture.md`

## Decisions (≤7)
- V8 использует RepairLine вместо exit_date (пул линий из MP) — `docs/architecture/limiter_architecture.md`
- `repair_days` введён для unsvc, inactive не декрементируется — `docs/architecture/limiter_architecture.md`
- Правило ресурса — next-day dt (чтобы исключить переналёт) — `docs/architecture/limiter_architecture.md`
- V8 не эквивалентен V7 по переходам (осознанно) — `docs/architecture/limiter_architecture.md`
- Дет. даты ограничены 500 (с отбрасыванием лишних) — `docs/architecture/limiter_architecture.md`
- Основной код ветки — `orchestrator_limiter_v8.py` — `README.md`
- Квоты идут через MessageBucket/QuotaBucket, решения по rank — `README.md`

## Impact Paths
- `code/sim_v2/messaging/orchestrator_limiter_v8.py` → порядок слоёв V8 → поведение симуляции — `README.md`, `docs/architecture/limiter_architecture.md`
- `docs/architecture/limiter_architecture.md` → канон V8 → правила/слои — `docs/architecture/limiter_architecture.md`
- `config/transitions/transitions_rules.json` → матрица state→state → переходы — `docs/architecture/limiter_architecture.md`
- `config/transitions/quota_rules.json` → логика квот/RepairLine → P1/P2/P3 решения — `docs/architecture/limiter_architecture.md`
- `tools/transitions_viewer/index.html` → визуализация переходов/квот → отладка — `docs/architecture/limiter_architecture.md`
- `docs/architecture/rtc_pipeline_architecture.md` → MP5/mp5_lin инварианты → корректный dt/dn — `docs/architecture/rtc_pipeline_architecture.md`
- `docs/architecture/validation_rules.md` → SQL-first/инварианты → критерии корректности — `docs/architecture/validation_rules.md`
- `README.md` → статус V8 как основной архитектуры → зона ответственности — `README.md`

## Validation Proof
- `python code/analysis/validate_heli_pandas.py --analyze/--update/--all` — базовая валидация `heli_pandas` — `docs/architecture/validation_rules.md`
- Проверка отсутствия NVRTC warning'ов (log clean) — `docs/architecture/validation_rules.md`
- Инварианты MP5: длина `(DAYS+1)*FRAMES`, индексация `row = day*FRAMES + idx` — `docs/architecture/validation_rules.md`
- `mp5_lin` доступен только на чтение после инициализации — `docs/architecture/validation_rules.md`
- Линт капсулы: `python code/analysis/context_capsule_builder.py --lint docs/limiter_v8_capsule.md` — формат и лимиты — `docs/validation.md`

## Risks (≤7) + Mitigations
- Переполнение `MAX_DETERMINISTIC_DATES` → потеря событий → контролировать число дат и лимит — `docs/architecture/limiter_architecture.md`
- Нарушение порядка слоёв → логические сбои → использовать таблицу слоёв как источник истины — `docs/architecture/limiter_architecture.md`
- Ошибка размера/индексации MP5 → неверный dt → жёсткие проверки длины и формулы — `docs/architecture/validation_rules.md`
- Запись в `mp5_lin` после инициализации → порча данных → read-only режим — `docs/architecture/validation_rules.md`
- NVRTC warning'и → скрытые дефекты → фикс + очистка кэша — `docs/architecture/validation_rules.md`
- Неправильная обработка `limiter=0` → исключение → соблюдать правило выхода — `docs/architecture/limiter_architecture.md`

## Open Questions (≤7)
- Какие дополнительные SQL-инварианты для limiter_v8 обязательны кроме базовых `validation_rules`?
- Нужны ли дополнительные проверки по RepairLine (slots/commit consistency) в SQL?
- Какой k-hop радиус использовать в KG влияния для “Impact Paths” limiter_v8?

## Pointers (≤15)
- `code/sim_v2/messaging/orchestrator_limiter_v8.py`
- `docs/architecture/limiter_architecture.md`
- `docs/architecture/rtc_pipeline_architecture.md`
- `docs/architecture/validation_rules.md`
- `code/analysis/context_capsule_builder.py`
- `docs/validation.md`
- `config/transitions/transitions_rules.json`
- `config/transitions/quota_rules.json`
- `tools/transitions_viewer/index.html`
- `README.md`
