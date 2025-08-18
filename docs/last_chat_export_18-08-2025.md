# Экспорт чата от 18-08-2025

## Основные темы чата
- Доведение `status_change` до MP3 и словаря
- Обновление типов MP2 и авто‑миграция
- Перезапуск загрузки MP (loader→exporter→validator)
- Автоматическая уборка по правилам

## Решенные задачи
- `heli_pandas.status_change` добавлен в словарь и MacroProperty3; валидатор подтвердил 7113/7113 без расхождений
- MP2: `ops_counter_mi8/mi17` переведены на `UInt16` с авто‑миграцией
- Оркестратор Extract: добавлен шаг `pre_simulation_status_change.py` (pre‑simulation разметка D0)

## Проблемы и их решения
- `status_change` отсутствовал в словаре → переработана логика словаря на устойчивый ключ `(primary_table, field_name)` и аддитивную дозагрузку
- ALTER UPDATE с экспериментальными SETTINGS в ClickHouse → реализован безопасный расчёт D0 на Python и пакетные UPDATE по `psn`

## Изменения в коде
- `code/digital_values_dictionary_creator.py` — устойчивые ключи и добавлен `status_change`
- `code/flame_macroproperty2_exporter.py` — UInt16 для `ops_counter_*`, авто‑миграция
- `code/dual_loader.py` — добавлен `status_change` в DDL/инициализацию/порядок колонок
- `code/extract_master.py` — добавлен шаг `pre_simulation_status_change.py`
- `code/pre_simulation_status_change.py` — расчёт D0 (ops_check) и пакетные UPDATE

## Обновления документации
- `docs/changelog.md` — запись от 18-08-2025 о статусе MP3/MP2, словаря и уборке

## Следующие шаги
- Суточный CPU‑прогон MP2 (1 день) и проверка схем/метрик завтра
- Согласование и доработка balance/RTC логики с последующими тестами
