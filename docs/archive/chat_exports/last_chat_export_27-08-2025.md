# Экспорт чата от 27-08-2025

## Основные темы чата
- Удаление поля br из MP1; переход на br_mi8/br_mi17 (минуты)
- Исправление пайплайна: обогащение heli_pandas.group_by отдельным шагом с --apply
- Полный Transform: загрузка MP1/MP3/MP4/MP5 и Property
- Оценка VRAM и подтверждение батч‑экспорта MP2 по дням
- Безопасная уборка рабочего стола

## Решенные задачи
- br полностью исключён из загрузки/экспорта MacroProperty1; используются только br_mi8/br_mi17
- В Extract подтверждён порядок шага heli_pandas_group_by_enrичер.py (после program_ac_direct_loader.py, перед digital_values_dictionary_creator.py) и запуск с --apply
- Transform Master: успешно загружены MP1/MP3/MP4/MP5 и Property

## Проблемы и их решения
- Остаточные упоминания единого br → заменены/удалены; документация обновлена
- Экспорт чата: явное создание файла в docs/ и запись в changelog

## Изменения в коде
- code/flame_macroproperty1_loader.py — исключён br; добавлены br_mi8/br_mi17
- code/flame_macroproperty1_exporter.py — описания для br_mi8/br_mi17
- code/digital_values_dictionary_creator.py — удалён br из словаря
- code/extract_master.py — описание этапа BR обновлено (br_mi8/br_mi17)

## Обновления документации
- docs/extract.md — добавлен шаг group_by‑энричера с --apply
- docs/transform.md — MP1: br → br_mi8/br_mi17; br исключён
- docs/changelog.md — добавлена запись от 27-08-2025

## Следующие шаги
- Исправить маппинг field_id для heli_pandas.group_by в dict_digital_values_flat и перегенерировать словарь
- Повторный прогон симуляции на 7 суток

