# Экспорт чата от 20-08-2025

## Основные темы чата
- Завершение publish*/controller/apply на GPU, добавление таймингов
- CPU‑fallback прогон 7 дней и SQL‑сводки
- Отладка NVRTC/Jitify компиляции RTC на GPU, введение FLAMEGPU_PROBE
- Уборка рабочих артефактов (логи, __pycache__)

## Решенные задачи
- Реализованы RTC публикации: ops_persist, add_candidate_p1/p2/p3, cut_candidate (score в UInt32+UInt32)
- Реализован контроллер: ctrl_count_ops, ctrl_pick_add_p1/p2/p3, ctrl_pick_cut
- Добавлены тайминги step_ms и export_ms в `code/flame_gpu_gpu_runner.py`
- CPU‑прогон 7 дней выполнен, MP2 заполнен; получены сводки (ops vs target, статусы, ремонт/хранение)

## Проблемы и их решения
- NVRTC ошибка при компиляции RTC в полной модели: добавлен probe‑режим `FLAMEGPU_PROBE` и минимальные тесты
- Выявлено: `rtc_repair` компилируется и работает изолированно, падение связано с общей регистрацией/слоями; продолжается пошаговая локализация

## Изменения в коде
- `code/flame_gpu_helicopter_model.py`: добавлены RTC publish*/controller/apply, score_hi/score_lo, FLAMEGPU_PROBE, упрощена регистрация
- `code/flame_gpu_gpu_runner.py`: тайминги GPU шага и вставки в БД
- `code/flame_gpu_transform_runner.py`: тайминги вставки в БД (CPU)
- `code/utils/rtc_smoketest.py`, `code/utils/gpu_repair_minimal.py`, `code/utils/gpu_repair_probe_model.py`: вспомогательные тесты

## Обновления документации
- Обновлена формулировка в `docs/transform.md` (приоритет жёстких 4/6, формулировка слоёв)
- Готово к дополнению `docs/changelog.md` текущей записью

## Следующие шаги
- Пошагово включать RTC в модели через `FLAMEGPU_PROBE` (ops_check → main → change → publish_* → ctrl_* → apply)
- После прохождения всех RTC вернуть полный порядок слоёв и снять GPU тайминги за 1/7 дней
- При необходимости — детализировать NVRTC/Jitify лог и адаптировать регистрационный шаблон RTC
