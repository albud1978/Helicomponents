# Context Capsule — flame_gpu

## Scope
Трогаем:
- `config/transitions/invariants.json` (GPU-1..GPU-6 — ограничения платформы)
- `code/sim_v2/messaging/` — все RTC-функции (паттерны и структура)
- `code/sim_v2/messaging/orchestrator_limiter_v8.py` (главный оркестратор модели)
- `code/sim_v2/messaging/base_model_messaging.py` (определение модели, MessageBucket, MacroProperty, init до simulate())
- `code/sim_v2/messaging/rtc_limiter_optimized.py` (inline limiter + publish/QM)
- `.cursor/rules/00_global_always.mdc` (глобальный запрет Float64)

Не трогаем:
- Конкретную бизнес-логику переходов → `docs/transitions_capsule.md`
- Квотирование → `docs/quota_capsule.md`

## Invariants (≤12)

SSoT: `config/transitions/invariants.json → gpu_constraints`

- GPU-1: запрет read+write одной переменной агента внутри одной RTC-функции (детерминизм)
- GPU-2: mp5_lin read-only после инициализации при сборке модели (до simulate())
- GPU-3: Float64 запрещён → UInt8/16/32, Float32 only
- GPU-4: NVRTC warnings = дефект → исправить → очистить .rtc_cache/* → перекомпилировать
- GPU-5: reset перед сбором (atomicMin/atomicAdd) → один reset mp_min_limiter в HF_StepController
- GPU-6: один агент — максимум один переход за шаг (FunctionCondition + setInitialState/setEndState)

## Decisions (≤7)

1. **Разделение read/write по слоям** (GPU-1) — агент читает переменные в слое N, пишет в слое N+1. Причина: FLAME GPU гарантирует детерминизм только при разделении.
2. **Atomic operations для MacroProperty** — exchange/min/max/add вместо прямого присваивания. Причина: конкурентный доступ тысяч агентов.
3. **Reset-before-collect паттерн** (GPU-5) — один reset mp_min_limiter в HF_StepController, без циклов reset+count.
4. **FunctionCondition для фильтрации** — вместо if/else внутри RTC, фильтрация на уровне модели (setInitialState). Причина: GPU не запускает ядро для неподходящих агентов.
5. **MessageBucket для broadcast** — key-based доступ, O(N/K) вместо O(N²). Отправка в слое N, приём в слое N+1 (message barrier).
6. **Фиксированные типы данных** (GPU-3) — UInt8 (флаги/маски), UInt16 (дни), UInt32 (SNE/PPR/LL/OH/indices), Float32 (расчёты). Float64 — только по явному разрешению.
7. **Shared device function для inline limiter** — `compute_limiter_inline` определён в RTC-строке и компилируется вместе.

## Impact Paths
- GPU-1 → нарушение → недетерминизм результатов → все RTC-функции зависят от этого
- GPU-3 → Float64 → несовместимость типов / потеря производительности → все переменные агентов
- GPU-5 → stale данные → каскад ошибок в квотировании и переходах → все фазы сбора
- GPU-6 → каскадный переход → непредсказуемое состояние агента → порядок RTC (51 слой)
- RTC не пишет Environment → пропуск HF_StepController → stale day/adaptive_days → неверные переходы и лимитер

## RTC Function Template

```cpp
FLAMEGPU_AGENT_FUNCTION(func_name, MessageInType, MessageOutType) {
    // 1. Read agent variables (getVariable)
    const unsigned int state = FLAMEGPU->getVariable<unsigned char>("state");
    const unsigned int idx   = FLAMEGPU->getVariable<unsigned int>("idx");

    // 2. Read environment (read-only)
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");

    // 3. Read MacroProperty (atomic)
    auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, SIZE>("mp_name");

    // 4. Compute (NO read+write same variable! GPU-1)

    // 5. Write agent variables (setVariable)
    FLAMEGPU->setVariable<unsigned int>("sne", new_sne);

    // 6. Write MacroProperty (atomic: exchange/min/max/add)
    mp[idx].exchange(value);

    // 7. Write messages (if MessageOutType != MessageNone)
    FLAMEGPU->message_out.setVariable<unsigned int>("field", value);

    return flamegpu::ALIVE;
}
```

## MacroProperty — ключевые буферы

Message-based архитектура: **count-буферы** (mi8_ops_count, commit_p1/p2/p3 и т.д.) удалены.
Вместо 3 циклов reset+count (12 слоёв) — 2 слоя: publish + QM.

| Имя | Тип | Размер | Назначение |
|-----|-----|--------|------------|
| mp5_lin | UInt32 | FRAMES × (DAYS+1) | Программа полётов (read-only после init) |
| mp5_cumsum | UInt32 | FRAMES × (DAYS+1) | Кумулятивная сумма полётов |
| repair_line_free_days_mp | UInt32 | REPAIR_LINES_MAX(64) | Свободные дни RepairLine |
| repair_line_acn_mp | UInt32 | REPAIR_LINES_MAX(64) | Aircraft number на линии (SSoT для занятости, обновляется P2/P3 CAS) |
| repair_line_rt_mp | UInt32 | REPAIR_LINES_MAX(64) | Repair time для текущего агента на линии |
| rl_buf_free_days | UInt32 | RL_BUF_SIZE(32000) | Export буфер: free_days per step per line |
| rl_buf_acn | UInt32 | RL_BUF_SIZE(32000) | Export буфер: aircraft_number per step per line |
| rl_buf_rt | UInt32 | RL_BUF_SIZE(32000) | Export буфер: repair_time per step per line |
| limiter_buffer | UInt32 | MAX_FRAMES | Значения лимитера |
| mp_min_limiter | UInt32 | 1 | Минимальный лимитер шага (reset в HF_StepController) |
| deterministic_dates_mp | UInt32 | MAX_DETERMINISTIC_DATES(500) | Фиксированные даты |
| spawn_dynamic_need | UInt32 | per-day | Потребность в spawn по дням |

## RepairLine Export Pipeline (2026-02-15)

**Цель:** ежедневная матрица состояния ремонтных линий для валидации INV-3 (ёмкость) и TEMP-1 (длительность).

**Важно: весь pipeline выполняется в одном запуске** `orchestrator_limiter_v8.py`:
1. GPU `simulate()` — RepairLine агенты записывают MacroProperty-снимки в буферы каждый адаптивный шаг
2. `HF_RepairLineDrain` — Python HostFunction читает GPU-буферы в numpy на финальном шаге
3. `interpolate_repairline_daily()` — Python постпроцессинг: разворачивает адаптивные шаги в ежедневную матрицу (3650 дней × repair_quota линий)
4. `export_repairline_to_ch()` — INSERT в ClickHouse `sim_repairline_v9`

Шаги 2-4 выполняются последовательно после `simulate()`, в том же процессе Python. Постпроцессинг (шаг 3) необходим, т.к. GPU работает в адаптивных шагах, а валидация требует ежедневных данных.

**GPU side:**
- `RTC_REPAIR_LINE_EXPORT` (rtc_repair_lines_v8.py) — каждый шаг записывает `repair_line_acn_mp`, `repair_line_free_days_mp`, `repair_line_rt_mp` в буферы `rl_buf_*`. Layout: `offset = step * REPAIR_LINES_MAX + line_id`.
- Читает из MacroProperty (SSoT), НЕ из agent variable (agent variable `aircraft_number` не синхронизирован — `LineAssignment` message не реализован, TODO).

**Python side (постпроцессинг в том же запуске):**
- `HF_RepairLineDrain` (rtc_repairline_export.py) — читает буферы на финальном шаге.
- `interpolate_repairline_daily()` — разворачивает адаптивные шаги в ежедневную матрицу: `free_days` +1/день, `acn` фиксирован, `release_day = d1 + (rt - fd)`.
- `export_repairline_to_ch()` — DDL + INSERT в `sim_repairline_v9`.

**ClickHouse:** `sim_repairline_v9 (version_date, version_id, day_u16, line_id, free_days, aircraft_number)`.

**Константы** (SSoT: model_build.py): `REPAIR_LINES_MAX=64`, `RL_BUF_SIZE=32000`.

## Environment Property — паттерн обновления

RTC не пишет Environment напрямую. Паттерн обновления:
`RTC → MacroProperty → HostFunction (HF_StepController) → Environment`

Обновляются: `current_day`, `prev_day`, `adaptive_days` + reset `mp_min_limiter`.

## HostFunction — HF_StepController

Назначение:
- `adaptive_days` (пересчёт шага)
- `current_day` / `prev_day` (сдвиг дня)
- reset `mp_min_limiter`

Заменяет: `HF_UpdateDayV8` и `HF_ComputeAdaptiveDays`.

## MessageArray — новые типы

| Тип | Поля | Назначение |
|-----|------|------------|
| RepairLineStatus | line_id → free_days | Статус RepairLine по дням |
| LineAssignment | line_id → aircraft_number | Привязка линии к борту |

## Инициализация при сборке модели

До `simulate()` в Python инициализируются:
- `mp5_cumsum`
- `deterministic_dates_mp`
- агенты RepairLine

## Risks (≤7) + Mitigations
- Нарушение GPU-1 (read+write) → недетерминизм → reviewer-flame обязан проверять при каждом ревью
- Забытый reset (GPU-5) → stale данные → паттерн: reset слой ВСЕГДА перед collect слоем
- NVRTC warning (GPU-4) → скрытый баг → CI: очистка .rtc_cache + проверка лога
- MessageBucket overflow → потеря данных → ограничение: 1 сообщение с key=0
- Пропуск HF_StepController → stale Environment → контроль порядка HostFunction в оркестраторе
- Отсутствие `compute_limiter_inline` в RTC-строке → ошибка компиляции/логики → ревью RTC-паттерна

## Open Questions (≤7)
- Оптимальный размер REPAIR_LINES_MAX (64) при масштабировании?
- Нужен ли double-buffering для MacroProperty при увеличении числа агентов?

## Pointers (≤15)
- `config/transitions/invariants.json` (gpu_constraints)
- `code/sim_v2/messaging/orchestrator_limiter_v8.py`
- `code/sim_v2/messaging/base_model_messaging.py`
- `code/sim_v2/messaging/rtc_limiter_v8.py`
- `code/sim_v2/messaging/rtc_limiter_optimized.py`
- `code/sim_v2/messaging/rtc_state_transitions_v8.py`
- `code/sim_v2/messaging/rtc_quota_v8.py`
- `code/sim_v2/messaging/rtc_repair_lines_v8.py`
- `code/sim_v2/messaging/rtc_repairline_export.py`
- `code/model_build.py` (REPAIR_LINES_MAX, RL_BUF_SIZE)
- `docs/architecture/rtc_pipeline_architecture.md`
- `docs/architecture/limiter_architecture.md`
- `.cursor/rules/00_global_always.mdc`
