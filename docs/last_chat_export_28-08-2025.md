# Экспорт чата от 28-08-2025

## Основные темы чата
- Полный откат sim_master к env-only, стабилизация NVRTC
- Аккуратная уборка code/: перенос legacy GPU, чистка логов
- Фиксация правил FLAME GPU/pyflamegpu в .cursorrules
- Обновления docs: changelog, Tasktracker

## Решенные задачи
- Перенесены legacy GPU файлы в code/archive/legacy_gpu (без затрагивания ETL)
- Очищены логи старше 7 дней в code/logs/
- Обновлены правила в .cursorrules (NVRTC/JIT, MacroProperty ограничения, индексация MP5)
- Tasktracker: отмечен откат к env-only; зафиксирован план по инкрементальному возврату RTC

## Проблемы и их решения
- Повторные JIT ошибки при регистрации RTC в полной модели
  - Решение: откат к env-only, инкрементальное включение RTC по одному ядру
- Ограничение pyflamegpu по MacroPropertyArray
  - Решение: зафиксирован fallback (скалярные MacroProperty или host seed), запись в .cursorrules

## Изменения в коде
- Перемещены: `code/flame_gpu_helicopter_model.py`, `code/flame_gpu_gpu_runner.py`, `code/flame_gpu_transform_runner.py`, `code/sim_runner.py`, `code/utils/gpu_repair_probe_model.py` → `code/archive/legacy_gpu/`
- Обновлён `code/sim_master.py`: env-only режим, диагностический вывод Env

## Обновления документации
- `.cursorrules`: раздел «Специфика FLAME GPU / pyflamegpu (28-08-2025)»
- `docs/changelog.md`: запись за 28-08-2025 про уборку и опыт FLAME GPU
- `docs/Tasktracker.md`: пометка об env-only и поэтапном возврате RTC, дата обновления

## Следующие шаги
- Включить один RTC: `rtc_probe_mp5` (минимальная логика чтения dt/dn)
- Затем добавить квоту D+1 (скалярный fallback) и статусы по слоям
- Держать тайминги `t_host_fill_env / t_sim_step / t_export`, контролировать JIT логи
