# Экспорт чата от 18-09-2025

## Основные темы
- Интеграция spawn (Mode B) в Mode A; исправление индексации FRAMES и включение новорождённых в квоты.
- Переход к микросервисной (по‑шаговой) архитектуре v2: каждый RTC/этап подключается отдельным скриптом с изолированным smoke‑тестом.
- Корректная загрузка Env из ClickHouse (MP1/MP3/MP4/MP5), плотный `frames_index` как |MP3 ∪ MP5|, без «future» слотов; FRAMES=286.
- Отладка `rtc_probe_mp5` (NVRTC) на длинных горизонтах; фикса упрощением индексации и проверкой типов/размерностей.

## Принятые решения (архитектура v2)
- Создаём новые шаги в `code/sim_v2/`, не ломая старые скрипты.
- Управляем горизонтом/логгером через env (`HL_V2_*`), не через перепись кода.
- FRAMES в v2 берём из union MP3 ∪ MP5 (без future) — 286; MP5 уже содержит будущие номера, резерв повторно не добавляем.
- Логику подключаем по одному RTC: probe → statuses → quotas → spawn → export; каждый шаг — smoke и валидации.

Ссылки на документацию:
- `docs/rtc_pipeline_architecture.md` — раздел «V2: Пошаговый пайплайн и загрузка Env».
- `infra/README.md` — раздел «V2 Pipeline (RTC)».
- `.cursorrules` — правила типов, запреты на хардкод/удаления, процесс согласования.

## Что реализовано сегодня
- Шаг 01 — загрузка Env и валидации
  - Файл: `code/sim_v2/01_setup_env.py`
  - Результат: DAYS=4000, FRAMES=286, `reserved_slots_count=7`, `first_reserved_idx=279`, `first_future_idx=286`.
  - Валидированы длины: `len(mp4_ops_counter_mi8/mi17)=DAYS`, `len(mp5_daily_hours)=(DAYS+1)*FRAMES`.
  - Снапшот: `tmp/env_snapshot.json`.

- Шаг 02 — базовая модель без RTC
  - Файл: `code/sim_v2/02_build_model_base.py`
  - Итог: модель инициализируется с `FRAMES=286` (из снапшота) и `DAYS=4000`.

- Шаг 03 — MP5‑probe (минимальный стенд)
  - Файл: `code/sim_v2/03_add_probe_mp5.py`
  - Smoke 5 суток: `MP5Probe OK: DAYS=5, FRAMES=286, sample(dt,dn)=[...]`.
  - Для DAYS=90 выявлена ошибка NVRTC; применены упрощения:
    - Индексация `base_next = base + FRAMES` вместо `(d+1)*FRAMES + i`.
    - Жёсткая проверка согласования типов/длин: Env UInt32 / setter UInt32 / reader UInt32; `need=(DAYS+1)*FRAMES`.
    - Уникализация имени RTC по DAYS и статическая `DAYS` в ядре.
  - Ошибка при DAYS=90 всё ещё воспроизводится — локализована к `rtc_probe_mp5` в отрыве от других RTC.

- Шаг 04 — статус 2 + MP5‑probe (упрощённо)
  - Файл: `code/sim_v2/04_add_status_246.py`
  - Smoke 30 суток с трассировкой борта: рост `sne`, чтение `dt/dn` по дням, переходов 2→6 нет (ожидаемо при LL≫суммарный dt за 30 дней).
  - Пример:
    ```
    Status246 OK: DAYS=30, FRAMES=286, s6_count=0,
    sample(idx,status,sne0,sne,ll,dt_last,dn_last)=[(0,2,0,3400,360000,104,104), ...]
    trace_idx timeline (day, idx, dt, dn, sne, status):
    (1,0,153,0,153,2) ...
    ```

## Исправленные/подсвеченные проблемы
- Индексация новорождённых: устранён выход за FRAMES (теперь `first_reserved_idx=279`, `first_future_idx=286`); FRAMES берём как union MP3∪MP5 (=286), без «future» добавки.
- NVRTC/Jitify падения: при DAYS≥90 компиляция `rtc_probe_mp5` ломается даже в минимальном стенде; упрощения индексации и типизации применены, но требуется отдельная отладка compile‑log’а.

## Предложение по дальнейшим шагам
1) Отладка `rtc_probe_mp5` на DAYS=90+ (минимальный .cu для NVRTC, явный compile log); закрепить рабочий шаблон.
2) Расширить шаг 04 до {6,4,2} полностью и валидации переходов.
3) Квоты S2 по шагам: intent → approve → apply (шаги 05–07) с smoke и счётчиками.
4) Возврат spawn в самом конце суток (резерв слотов MP5‑only: 279..285).
5) Экспорт в ClickHouse (прямой, без MP2‑postprocess), сверки с валидаторами.

## Команды из сессии (основные)
- Запуск 01 (Env):
  ```bash
  python3 -u code/sim_v2/01_setup_env.py --dump-json tmp/env_snapshot.json
  ```
- Запуск 02 (Base):
  ```bash
  HL_V2_ENV_SNAPSHOT=tmp/env_snapshot.json python3 -u code/sim_v2/02_build_model_base.py
  ```
- Запуск 03 (Probe, 5/90):
  ```bash
  HL_V2_ENV_SNAPSHOT=tmp/env_snapshot.json HL_V2_STEPS=5  python3 -u code/sim_v2/03_add_probe_mp5.py
  HL_V2_ENV_SNAPSHOT=tmp/env_snapshot.json HL_V2_STEPS=90 python3 -u code/sim_v2/03_add_probe_mp5.py
  ```
- Запуск 04 (Status2 + MP5, 30d, трассировка idx=0):
  ```bash
  HL_V2_ENV_SNAPSHOT=tmp/env_snapshot.json HL_V2_STEPS=30 HL_V2_TRACE_IDX=0 \
  python3 -u code/sim_v2/04_add_status_246.py
  ```

## Итоги дня
- Стартована «микросервисная» v2: 01–04 шаги готовы, базовые smoke‑тесты проходят.
- FRAMES нормированы: 286 (без «future»), что устранило логическую избыточность.
- Найдена и локализована проблема NVRTC в `rtc_probe_mp5` на DAYS≥90 — выведена в отдельную ветку отладки.

## Ссылки на ключевые файлы
- `code/sim_v2/01_setup_env.py`
- `code/sim_v2/02_build_model_base.py`
- `code/sim_v2/03_add_probe_mp5.py`
- `code/sim_v2/04_add_status_246.py`
- `docs/rtc_pipeline_architecture.md`
- `infra/README.md`
- `.cursorrules`
