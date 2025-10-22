# Экспорт чата от 29-08-2025

## Основные темы чата
- Уточнение FLAME GPU 2: MacroProperty‑массивы в Python объявляются как `newMacroProperty<Type>(name, dims...)` (без суффикса Array); в RTC доступ через `getMacroProperty<Type, DIMS>(name)[i]` с атомиками.
- План MP6: перейти на MacroProperty 1D массивы квот (UInt32) по дням с индексом `day+1`; разнести init (exchange из MP4) и потребление квоты по слоям `{6,4,2} → 3 → 5 → 1`.
- Смоук‑тесты: чтение MP5 на GPU и атомики над скалярным MP6 подтверждены.
- Документационные правки: уточнены MacroProperty/RTC и индексация MP5.

## Решенные задачи
- Зафиксированы корректные сигнатуры MacroProperty в Python/RTC и линейная индексация MP5.
- Обновлён план P1 под MP6 как MacroProperty UInt32 массивы.

## Проблемы и их решения
- Путаница API: ожидался `newMacroPropertyArray*`, фактически — `newMacroProperty<Type>(name, dims...)`. Решение: скорректировать документацию и план внедрения MP6.
- Несогласованность описания MP5: приведено к формуле `base = day * frames_total + idx` (DAYS+1 паддинг).

## Изменения в коде
- Код не менялся в этой итерации.

## Обновления документации
- Обновлён `docs/GPU.md`: MacroProperty массивы (Python/RTC), линейная индексация MP5, MP6 как UInt32.
- Обновлён `docs/Tasktracker.md` (P1): MP6 через MacroProperty UInt32 массивы и атомики в RTC.
- Обновлён `docs/migration.md`: примечание с ссылкой на официальную документацию FLAME GPU.
- Добавлена запись в `docs/changelog.md` с ссылкой на этот экспорт.

## Следующие шаги
- Реализовать MP6 как MacroProperty 1D массивы `uint32` по дням.
- Вернуть `rtc_probe_mp5` и подготовить `rtc_status_2` к MP6.
- Настроить сбор NVRTC‑логов в файл `code/logs/nvrtc.log`.
