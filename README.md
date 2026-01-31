# Helicopter Component Lifecycle

🚁 **Система прогнозирования жизненного цикла компонентов вертолётов с использованием Agent-Based моделирования на FLAME GPU**

> ⚠️ **Все команды выполняются из корня проекта (Helicomponents/)**

---

## 🎯 Почему имитационная модель?

**Причина:** Невозможность аналитически предсказать все переходы состояний компонентов без тяжёлых расчётов из-за влияния множества взаимозависимых факторов:

| Фактор | Влияние |
|--------|---------|
| **Исходное состояние ресурсов** | SNE, PPR, repair_days у каждого агента |
| **Темпы утилизации** | Индивидуальный вектор налёта (MP5) |
| **Программа полётов** | Изменение target по Mi-8/Mi-17 (MP4) |
| **Сроки ремонтов** | repair_time, очередь на ремонт |
| **Ресурсные лимиты** | LL, OH, BR — пороги списания и ремонта |

Эти факторы создают **нелинейные зависимости**, где состояние одного агента влияет на решения по другим (квотирование, очередь ремонтов). Аналитическое решение системы уравнений невозможно — требуется **пошаговая симуляция**.

---

## 📂 Структура проекта

| Папка | Назначение |
|-------|------------|
| `code/` | Активная разработка (ETL, симуляция, утилиты) |
| `code/sim_v2/` | Оркестратор симуляции FLAME GPU |
| `docs/` | Документация проекта |
| `data_input/source_data/` | Входные датасеты (`v_YYYY-MM-DD/`) |
| `output/` | Выходные отчёты и результаты |
| `config/` | Конфигурационные файлы |
| `logs/` | Логи работы системы |

---

## 🚀 Быстрый старт

```bash
# 1. Активировать окружение
# Основной вариант: локальный venv
source .venv/bin/activate
# Альтернатива (если .venv отсутствует): conda cuda13
source ~/miniconda3/etc/profile.d/conda.sh
conda activate cuda13

# 2. Загрузить переменные окружения
source config/load_env.sh
export CUBE_CONFIG_PATH="$PWD/config"

# 3. Проверить подключение к БД
python3 code/utils/test_db_connection.py

# 4. Запустить ETL (интерактивный выбор датасета и режима)
python3 code/extract_master.py

# 5. Запустить симуляцию — см. полную команду в .cursor/rules/
```

> **Полные команды с параметрами:** см. `.cursor/rules/` (секции "Загрузка данных" и "Команда запуска симуляции")

---

## 📚 Документация

### Правила и методология
| Файл | Описание |
|------|----------|
| **`.cursor/rules/`** | Модульные правила разработки для Cursor AI |
| **`.cursor/agents/`** | Субагенты проекта (coder-flame, reviewer-flame, validator-judge) |
| `docs/migration.md` | Промт для новых разработчиков |

Методология отладки логики: см. `.cursor/rules/00_global_always.mdc`.

### Архитектура
| Файл | Описание |
|------|----------|
| `docs/architecture/rtc_pipeline_architecture.md` | **Baseline** — архитектура RTC пайплайна (intent-based, orchestrator_v2) |
| `docs/architecture/limiter_architecture.md` | **LIMITER V8** — RepairLine + repair_days в unsvc/inactive (readiness/спавн учитывают RepairLine) |
| `docs/architecture/validation_rules.md` | Инварианты и процедуры тестирования (включая `--version-id` для изоляции прогонов) |
| `docs/spawn_dynamic_architecture.md` | Архитектура динамического спавна |

### 🚀 LIMITER V8 — основная архитектура (ветка feature/flame-messaging)

> **⚠️ В этой ветке основной код — LIMITER V8 (`orchestrator_limiter_v8.py`)**
> V8 = RepairLine + adaptive steps с deterministic_dates; `min_dynamic` сбрасывается в `rtc_compute_global_min_v8` (без отдельного reset‑слоя), источник шага (limiter/repair_days) пишется в `adaptive_result_mp[1]`, а шаги по детерминированным датам помечаются как `deterministic_date:<day>`; P2/P3 ранжируются по idx и своему типу с раздельными квотами, выбор линии — минимальный `free_days >= repair_time` по MacroProperty (`repair_line_free_days_mp`/`repair_line_acn_mp`); spawn считает дефицит как `target − curr_ops − used(P1/P2/P3 commit)`, storage не участвует. Квоты распределяются через MessageBucket (`QuotaBucket`) по rank. Для диагностики RepairLine/квот используются `sim_repair_lines_v8` и `sim_quota_mgr_v8` (QM ops/target/quota_left).
> Дополнительно: тикеты spawn читают параметры по текущему дню (один день/один шаг), Mi‑8 использует `mi8_ll/oh/br` из `md_components` (через env).

| Файл | Статус | Описание |
|------|--------|----------|
| **`code/sim_v2/messaging/orchestrator_limiter_v8.py`** | ✅ **ОСНОВНОЙ** | RepairLine + adaptive steps (deterministic_dates) |
| `code/sim_v2/messaging/orchestrator_limiter_v5.py` | ⚡ Резервный | Двухфазная (intent-based), 100% GPU-only |
| `code/sim_v2/messaging/orchestrator_limiter_v3.py` | 📦 Архивный | `while step()` + HF |
| `code/sim_v2/messaging/rtc_quota_v8_base.py` | ✅ Актуальный | Локальные квоты V8 (без зависимости от V7) |
| `code/sim_v2/messaging/rtc_state_transitions_v7.py` | ✅ Актуальный | Однофазные переходы состояний |
| `code/sim_v2/messaging/rtc_limiter_optimized.py` | ✅ Актуальный | Бинарный поиск limiter через mp5_cumsum |
| `code/sim_v2/messaging/rtc_limiter_v5.py` | ✅ Актуальный | GPU-only модули (current_day, adaptive) |
| **`docs/architecture/limiter_architecture.md`** | 📄 Документ | **Архитектура LIMITER V8 с таблицей слоёв** |

**Архивные** (не использовать):
| Файл | Описание |
|------|----------|
| `code/sim_v2/messaging/orchestrator_limiter_v7.py` | Legacy V7 (однофазная архитектура) |
| `orchestrator_limiter.py` | Старая версия без оптимизации limiter |
| `orchestrator_limiter_v2.py` | Промежуточная версия (ежедневные шаги) |
| `orchestrator_limiter_v4.py` | Промежуточная GPU-only версия |
| `orchestrator_adaptive*.py` | Эксперименты с adaptive step |
| `orchestrator_gpu_only.py` | GPU-only эксперимент |

**Результаты LIMITER V7:**
- **219 адаптивных шагов** вместо 3650 ежедневных
- **1.59с** на 10 лет симуляции (**2296 дней/сек**)
- **100% GPU** — единый вызов `simulate()` без Python-цикла
- **Однофазная архитектура** — 26 RTC функций (было 45 в V5)
- **3 счётчика в 1 проход** — SNE, PPR, limiter в одной RTC
- **3 счётчика в 1 проход** — sne++, ppr++, limiter-- в одной RTC
- **Точный limiter** — бинарный поиск по mp5_cumsum (без аппроксимации)
- dt = программа ✅, Δsne = Σdt ✅

**Архитектура V7 vs V5:**
| Метрика | V7 | V5 |
|---------|-----|-----|
| Шаги | 219 | 332 |
| Время | **1.59с** | 3.71с |
| Ускорение | **2.3x** | — |
| GPU % | 100% | 100% |
| RTC функций | 26 | 45 |
| Архитектура | Single-phase | Intent-based |

### ETL процессы
| Файл | Описание |
|------|----------|
| `docs/extract.md` | Извлечение данных (включает проверку комплектности) |
| `docs/architecture/validation_rules.md` | Правила валидации данных |
| `docs/architecture/transform.md` | Трансформация данных |
| `docs/architecture/load.md` | Загрузка результатов |

### История
| Файл | Описание |
|------|----------|
| `docs/changelog.md` | История изменений проекта |
| `docs/Tasktracker.md` | Текущие и активные задачи |

---

## 🛠 Технологии

- **Python 3.12** — основной язык
- **ClickHouse** — хранилище данных
- **FLAME GPU 2.0.0rc4** — Agent-Based симуляция на GPU
- **CUDA 13.0** — GPU вычисления

### Версии библиотек (актуально на 03-01-2026)

| Библиотека | Версия |
|------------|--------|
| pandas | 2.3.3 |
| numpy | 2.2.6 |
| cudf-cu12 | 25.12.0 |
| pyflamegpu | 2.0.0rc4+cuda130 |
| clickhouse-driver | 0.2.10 |
| clickhouse-connect | 0.10.0 |

### Поддерживаемые платформы

| Платформа | GPU | CUDA | Статус |
|-----------|-----|------|--------|
| Linux native | RTX 30xx/40xx/50xx | 13.0 | ✅ Основная |
| Windows + WSL2 | RTX 30xx/40xx/50xx | 13.0 | ✅ Поддерживается |
| Windows native | — | — | ❌ Не поддерживается |

---

## ⚙️ Настройка окружения

### Первый запуск
```bash
# 1. Клонировать проект
git clone https://github.com/albud1978/Helicomponents.git
cd Helicomponents

# 2. Создать виртуальное окружение
python3 -m venv .venv
source .venv/bin/activate

# 3. Установить зависимости
pip install -r requirements.txt -c constraints.txt

# 4. Настроить .env
cp .env.example .env
# Добавить: CLICKHOUSE_PASSWORD=your_password

# 5. Первый запуск ETL
python3 code/extract_master.py  # → выбрать 1 (ТЕСТ)
```

> Для воспроизводимости используйте `constraints.txt`: он фиксирует проверенные версии ключевых библиотек,
> при этом `requirements.txt` остаётся “верхнеуровневым” списком зависимостей.

### Переменные окружения (.env)
```bash
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_PASSWORD=your_password
WORK_MODE=dev
LOG_LEVEL=INFO
```

### RTC кэширование (ускоряет повторные запуски симуляции)
```bash
mkdir -p .rtc_cache
export FLAMEGPU_RTC_EXPORT_CACHE_PATH="$(pwd)/.rtc_cache"
```

---

## 📊 Штатные процедуры

| Процедура | Команда | Результат |
|-----------|---------|-----------|
| Проверка комплектности | `python3 code/heli_pandas_ops_other_groups.py --version-date YYYY-MM-DD --version-id 1` | `output/` |
| Анализ лизинга | `python3 code/heli_pandas_lease_restricted.py` | `output/` |
| Диагностика БД | `python3 code/utils/test_db_connection.py` | — |
| Очистка словарей | `python3 code/utils/cleanup_dictionaries.py` | — |
| Полная очистка | `python3 code/utils/database_cleanup.py` | — |

> **Подробнее:** `.cursor/rules/` (секция "Штатные процедуры проверки данных")

---

## 🔧 Документированные хардкоды

> ⚠️ **Правило проекта**: Любой хардкод должен быть документирован. Недокументированный хардкод запрещён.

### Архитектурные константы (фиксированные)

| Константа | Значение | Файл | Назначение |
|-----------|----------|------|------------|
| `RTC_MAX_FRAMES` | 400 | `code/model_build.py` | Размер буферов MacroProperty для планеров (покрывает рост флота) |
| `MAX_DAYS` | 4000 | `code/model_build.py` | Максимум дней симуляции (~10.9 лет) |
| `MAX_SIZE` | 1,600,400 | `code/model_build.py` | MAX_FRAMES × (MAX_DAYS + 1) |

### Справочные константы (из md_components)

| Константа | Значение | Файл | Назначение |
|-----------|----------|------|------------|
| `SPAWN_PARTSEQNO_MI8` | 70387 | `code/sim_env_setup.py` | partseqno для планера Mi-8Т (group_by=1) |
| `SPAWN_PARTSEQNO_MI17` | 70386 | `code/sim_env_setup.py` | partseqno для планера Mi-8АМТ (group_by=2) |
| `mi8_*_const` | из md_components | `code/sim_env_setup.py` | ll, oh, br, repair_time, partout_time, assembly_time |
| `mi17_*_const` | из md_components | `code/sim_env_setup.py` | ll, oh, br, br2_mi17, repair_time, partout_time, assembly_time |

### Диапазоны нумерации

| Диапазон | Назначение | Файл |
|----------|------------|------|
| `1–99999` | Реальные борта из MP3 | — |
| `100000–999999` | Зарезервировано для spawn (новорожденные) | `code/sim_env_setup.py` |
| `1000000+` | Зарезервировано (будущее расширение) | — |

### Битовые маски типов ВС

| Тип ВС | Маска | Двоичное | Файл |
|--------|-------|----------|------|
| МИ-8Т, МИ-8МТВ, МИ-8П | 32 | `0b00100000` | `code/enrich_heli_pandas.py` |
| МИ-8АМТ, МИ-17 | 64 | `0b01000000` | `code/enrich_heli_pandas.py` |
| МИ-26 | 128 | `0b10000000` | `code/enrich_heli_pandas.py` |

### Статусы агентов

**Рабочие статусы (status_id 1-6)** — назначаются только при `error_flags = 0`:

| status_id | Название | FLAME GPU State | Описание |
|-----------|----------|-----------------|----------|
| 1 | Inactive | `inactive` | Неактивный (каннибализация, ожидание комплектации) |
| 2 | Operations | `operations` | В эксплуатации (летает) |
| 3 | Serviceable | `serviceable` | Исправен, на складе |
| 4 | Repair | `repair` | В ремонте |
| 5 | Reserve | `reserve` | Резерв (готов к вводу) |
| 6 | Storage | `storage` | Хранение (списан из активной эксплуатации) |

**Статусы ошибок (status_id 10-15)** — битовая маска `error_flags`:

| status_id | Флаг | Бит | Условие | Описание |
|-----------|------|-----|---------|----------|
| 10 | 1 | 0 | `ll = 0 OR NULL` | Недостаточные данные |
| 11 | 2 | 1 | `target_date < version_date` | Дата ремонта в прошлом |
| 12 | 4 | 2 | `condition ≠ ИСПРАВНЫЙ AND sne = 0` | Неисправен при нулевой наработке |
| 13 | 8 | 3 | `sne > ll OR ppr > oh` | Превышение ресурса |
| 14 | 16 | 4 | `condition NOT IN (...)` | Некорректное condition |
| 15 | 32 | 5 | `ДОНОР AND sne < br` | Донор при ремонтопригодном (warning) |

> **Подробнее:** `docs/architecture/validation_rules.md`

### Бизнес-логика (параметризуемые)

| Параметр | Значение | Источник | Назначение |
|----------|----------|----------|------------|
| `br2_mi17` | 3500 ч (210000 мин) | `MD_Сomponents.xlsx` | Порог ppr для подъёма Mi-17 из inactive: < порога → без ремонта, ≥ порога → ремонт |
| `br_mi8/br_mi17` | расчёт | `calculate_beyond_repair.py` | Beyond Repair — экономический порог списания |
| `oh_mi8/oh_mi17` | из Excel | `MD_Сomponents.xlsx` | Межремонтный ресурс (часы → минуты при загрузке) |
| `ll_mi8/ll_mi17` | из Excel | `MD_Сomponents.xlsx` | Назначенный ресурс (часы → минуты при загрузке) |

### История аудита хардкодов

| Дата | Действие | Статус |
|------|----------|--------|
| 02-10-2025 | Централизация partseqno (7 мест → 1) | ✅ Исправлено |
| 02-10-2025 | Константы времени из md_components | ✅ Исправлено |
| 02-10-2025 | Архивирование старых spawn модулей | ✅ Выполнено |
| 04-01-2026 | Добавлен br2_mi17 для логики inactive | ✅ Реализовано |

> **Подробнее:** `docs/archive/analysis/hardcode_audit_02-10-2025.md`

---

## 📄 Лицензия

MIT License — см. файл `LICENSE`.
