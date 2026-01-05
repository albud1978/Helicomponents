# Симуляция агрегатов (Units) V2

## Архитектура

Вторая модель симуляции для агрегатов (group_by >= 3) на базе FLAME GPU.

### Отличия от симуляции планеров:

| Планеры | Агрегаты |
|---------|----------|
| 6 состояний (включая inactive) | 5 состояний (без inactive) |
| aircraft_number = PRIMARY KEY | psn = PRIMARY KEY |
| Квотирование по программе | FIFO-очередь из пула |
| ~280-340 агентов | ~10,000-11,000 агентов |

## Быстрый старт

```bash
cd /media/albud/8C327EB0327E9F40/Projects/Heli/Helicomponents
source .venv/bin/activate
source config/load_env.sh
export CUDA_PATH=/home/albud/miniconda3/targets/x86_64-linux

cd code/sim_v2/units
python3 orchestrator_units.py --version-date 2025-07-04 --steps 3650
```

**Производительность:**
- 10 лет симуляции: ~5с (1.4мс/шаг)
- Агентов: 10634 (+ резервы)
- dt загружается из sim_masterv2 (результаты симуляции планеров)

## Единый оркестратор

Для запуска полной симуляции (планеры → агрегаты):

```bash
cd code/sim_v2
python3 orchestrator_full.py --version-date 2025-07-04 --steps 3650 --export

# Только агрегаты (если планеры уже симулированы):
python3 orchestrator_full.py --version-date 2025-07-04 --steps 3650 --units-only --export
```

## Интеграция с планерами

1. **dt загружается из sim_masterv2** — реальный налёт от симуляции планеров
2. **Блокировка при ремонте** — если планер не в operations, dt=0
3. **Экспорт в sim_units_v2** — результаты доступны для анализа

## Структура модуля

```
units/
├── __init__.py                    # Пакет
├── base_model_units.py            # Базовая модель FLAME GPU
├── agent_population_units.py      # Загрузка агентов из ClickHouse
├── orchestrator_units.py          # Оркестратор симуляции
├── rtc_units_count.py             # Подсчёт агентов по состояниям
├── rtc_units_fifo_assignment.py   # FIFO-назначение (TODO: read/write split)
├── rtc_units_request_replacement.py  # Запрос замены (TODO)
├── rtc_units_return_to_pool.py    # Возврат в пул из ремонта
├── rtc_units_state_manager.py     # Управление переходами
└── README.md                      # Эта документация
```

## Переменные агента (unit)

| Категория | Переменные | Описание |
|-----------|------------|----------|
| ID | idx, psn, aircraft_number, partseqno_i, group_by | psn - PRIMARY KEY |
| Наработка | sne, ppr | В минутах |
| Нормативы | ll, oh, br | Из md_components |
| Ремонт | repair_time, repair_days | В днях |
| FIFO | queue_position | Позиция в очереди пула |
| Управление | intent_state, mfg_date | Намерение и дата выпуска |

## Состояния агента

- **operations** (2): На планере, в полёте
- **serviceable** (3): Исправен, на складе (в очереди)
- **repair** (4): В ремонте
- **reserve** (5): Готов к установке (в очереди)
- **storage** (6): Хранение/списание (терминальное)

⚠️ **Нет состояния inactive** — новые агрегаты создаются сразу в reserve.

## Производительность

| Датасет | Агрегатов | 3650 дней | Ср. шаг |
|---------|-----------|-----------|---------|
| 2025-07-04 | 10,634 | 0.72с | 0.20мс |
| 2025-12-30 | 11,104 | 0.72с | 0.20мс |

## TODO

- [ ] Интеграция с assembly_trigger планера
- [ ] Разделение FIFO read/write на разные слои
- [ ] Запись результатов в ClickHouse
- [ ] MP2 интеграция с планерами

## Дата создания

05.01.2026

