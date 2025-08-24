# Экспорт чата от 24-08-2025

## Основные темы чата
- Консолидация Transform документации (удаление flame_gpu_architecture.md)
- Переход BR на раздельные br_mi8/br_mi17 в минутах
- Обновление загрузчиков и словаря полей; полный Extract 14/14
- Исправление pre_simulation_status_change под новые BR

## Решенные задачи
- Добавлены br_mi8/br_mi17 в md_components; расчёт массово в CH (минуты)
- md_components_loader выровнен; dict_digital_values обновлён
- pre_simulation_status_change использует br по ac_type_mask
- Полный Extract (TEST) успешно завершён

## Проблемы и их решения
- Некорректный ранний переход в Хранение из‑за BR в часах → перевели BR в минуты, округление до минут
- Ошибка выборки br в пресимуляции → заменено на br_mi8/br_mi17

## Изменения в коде
- code/md_components_loader.py — добавлены br_mi8/br_mi17, порядок колонок
- code/calculate_beyond_repair.py — массовый расчёт BR (минуты)
- code/pre_simulation_status_change.py — выбор BR по маске типов
- code/flame_macroproperty1_loader.py — br → br_mi8/br_mi17
- code/digital_values_dictionary_creator.py — описания новых полей

## Обновления документации
- docs/extract.md — BR по типам, минуты, формула/ограничения
- docs/transform.md — примечание про выбор BR и единицы
- docs/changelog.md — запись от 24-08-2025

## Следующие шаги
- Прогон Transform (GPU) с новыми BR, проверка MP2
- Валидации BR в CH и сводка квот D+1
