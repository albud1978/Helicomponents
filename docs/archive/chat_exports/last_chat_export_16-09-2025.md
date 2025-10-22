# Экспорт чата от 16-09-2025

## Основные темы чата
- Восстановление зависимостей Extract (`aircraft_number_processor`, `inactive_planery_processor`)
- Диагностика отсутствия legacy `aircraft_number` в `heli_pandas` и пустого словаря планёров
- Бэкап/переключение скриптов `extract_master` между коммитами и из архивов
- Интеграция спавна (spawn) из Mode B в Mode A: Env/RTC/слои в конце суток
- NVRTC/Jitify ошибка при компиляции `rtc_log_day` и её локализация
- Коррекция FRAMES: объединение `MP3 ∪ MP5`; корректные размеры MP5/MP2
- Тест‑прогоны Mode A: 365 (без спавна), 180 (со спавном) и запуск 365 (со спавном)

## Решенные задачи
- Восстановлены отсутствующие процессоры: `code/aircraft_number_processor.py`, `code/inactive_planery_processor.py`.
- Выполнен бэкап действующих скриптов и восстановление версий из выбранных коммитов по пайплайну Extract.
- Интегрирован спавн в общий билдер модели (`code/model_build.py`): агенты `spawn_mgr`, `spawn_ticket`, Env/MacroProperty и слои в конце суток.
- Обновлен `code/sim_env_setup.py`: FRAMES формируется по объединению `MP3 ∪ MP5` с порядком `[MP3] + [будущие из MP5 ↑]`.
- Обновлен `code/sim_master.py` (ветка `--status12456-smoke-real`): уважение `--status12456-days`, нарезка массивов Env по DAYS, инициализация `frames_initial`, `mp4_new_counter_mi17_seed`, `month_first_u32`, популяции спавна.
- Устранена OutOfBounds‑ошибка Env: несоответствие длины массивов MP4 при меньшем DAYS.

## Проблемы и их решения
- NVRTC/Jitify падение на `rtc_log_day` в режиме 365 дней: идентифицировано место; запущен 180‑дн. прогон, подготовлены условия корректного 365 с отключённым постпроцессингом MP2.
- Спавн на 180 сутках не дал рождений — подтверждено отсутствие ненулевого `new_counter_mi17` в первые 180 суток.

## Изменения в коде
- Изменено: `code/model_build.py` — добавлены Env/MacroProperty для спавна; агенты и слои спавна после `rtc_log_day`.
- Изменено: `code/sim_env_setup.py` — FRAMES = |distinct `aircraft_number`| по объединению `MP3 ∪ MP5`; построение `mp5_daily_hours`.
- Изменено: `code/sim_master.py` — уважение количества дней из CLI, нарезка `MP4/seed/month_first` по DAYS, инициализация спавн‑популяций.
- Восстановлено: `code/aircraft_number_processor.py`, `code/inactive_planery_processor.py`.

## Обновления документации
- Добавлена запись в `docs/changelog.md` (16-09-2025) о правках Mode A/Spawn/FRAMES.
- Создан данный экспорт чата `docs/last_chat_export_16-09-2025.md`.

## Следующие шаги
- Дождаться завершения 365‑дневного прогона со спавном; проверить появления новых `aircraft_number ≥ 100000` с D+1 в `sim_results`.
- При необходимости: включить MP2‑постпроцессинг на 365 при стабильной конфигурации или зафиксировать размер MP2.
- Актуализировать `docs/GPUarc.md` и `docs/GPU.md` ссылкой на интеграцию спавна в Mode A и FRAMES из `MP3∪MP5`.
