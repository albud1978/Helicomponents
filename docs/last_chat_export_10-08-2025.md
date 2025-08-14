# Экспорт чата — 10-08-2025

Ключевые решения и изменения за сессию:

- Балансировка только на GPU:
  - Введены env свойства `trigger_program_mi8/mi17` (дневная квота миграций из MP4).
  - Host баланс использует квоту trigger_program_* напрямую (без вычисления current−target).
  - Готова архитектура под DeviceMacroProperty (ops_count/deficit/excess) и CAS‑фазы для полностью device‑баланса.

- RTC и триггеры:
  - Единый сброс `status_change` в rtc_change для любого ненулевого значения.
  - Формулы:
    - status_change=4: `partout = D + partout_time`, `assembly = D + (repair_time − assembly_time)`
    - status_change=5: `ppr=0`, `repair_days=0`
    - 1→2: `active = D − repair_time`, `assembly = D − assembly_time`
  - Инварианты: `ops_check` вход `status_change==0`; после `change` — `status_change==0`.

- Перенос времён в окружение:
  - `partout_time_arr`, `assembly_time_arr` как env массивы (индексация по agent.idx).

- MP2 расширен для диагностики:
  - Поля `ops_current_mi8/mi17` (фактическая укомплектованность в эксплуатации на D).
  - Поля дат: `partout_trigger`, `assembly_trigger`, `active_trigger`.

- Кодовые изменения (основные файлы):
  - `code/flame_gpu_helicopter_model.py`: env триггеры, массивы времени, инварианты, сайд‑эффекты, подготовка к macro‑балансу.
  - `code/flame_gpu_gpu_runner.py`: установка `trigger_program_*`, логирование MP2 (включая ops_current_* и даты триггеров).
  - `code/flame_macroproperty2_exporter.py`: схема MP2 расширена.
  - `code/flame_gpu_transform_runner.py`: CPU fallback соответствует тем же правилам (для оффлайн отладки).

- Документация:
  - `docs/transform.md` обновлён: статусы, формулы триггеров, инварианты, поля MP2, описание host_balance по дневной квоте.

Примечания:
- Следующий шаг: добавить DeviceMacroProperty `ops_*_by_group` и CAS‑фазы раздачи квот в агентные слои, чтобы исключить host баланс.