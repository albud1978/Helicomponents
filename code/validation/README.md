# Валидационные скрипты

Скрипты для проверки корректности работы RTC модулей через анализ данных в ClickHouse.

## Принципы валидации

✅ **Основной контроль:** Итоговая выгрузка в СУБД (MP2)  
⚠️ **Логирование в RTC коде:** Только по явному согласованию  
🧹 **Очистка:** После устранения проблемы логирование удаляется  
✅ **Критерий успеха:** Результаты подтверждены в СУБД БЕЗ избыточного логирования  

## Структура

```
code/validation/
├── README.md                              # Этот файл
├── run_all.py                              # Ручной запуск SSoT-набора валидаторов
├── run_all_stream.py                       # Потоковый запуск валидаторов из invariants.json
├── inv*_*.py                               # Активные INV-* проверки
├── temp*_*.py                              # Активные TEMP-* проверки
└── ch_client.py                            # Подключение ClickHouse для валидаторов
```

## Активный SSoT-набор

`run_all.py` запускает текущие валидаторы:

- `inv1_sne_le_ll.py`
- `inv2_ops_vs_target.py`
- `inv3_repair_capacity.py`
- `inv4_unsvc_repair_time.py`
- `inv5_balance_increments.py`
- `inv6_dt_only_ops.py`
- `inv7_dt_eq_mp5.py`
- `inv8_storage_frozen.py`
- `inv9_limiter_exit.py`
- `inv10_turnover_balance.py`
- `inv11_spawn_limit_saturation.py`
- `inv12_ppr_le_oh.py`
- `temp1_repair_duration.py`
- `temp4_no_infinite_repair.py`
- `temp5_repair_hybrid_vector.py`

## Запуск всех проверок

```bash
python3 code/validation/run_all.py --version-id <version_id> --version-date <YYYYMMDD>
```

Для потокового запуска по датасетам из `config/transitions/invariants.json`:

```bash
python3 code/validation/run_all_stream.py --dataset <YYYYMMDD:version_id>
```

## Archived

Устаревшие или дублирующие скрипты перенесены в `code/archive/` и не входят в CI compile scope:

- `code/archive/validation/inv5_sne_balance.py` → SSoT: `inv5_balance_increments.py`
- `code/archive/validation/inv6_dt_outside_ops.py` → SSoT: `inv6_dt_only_ops.py`
- `code/archive/validation/inv3_repair_limit.py` → SSoT: `inv3_repair_capacity.py`
- `code/archive/validation/inv9_limiter_zero_exit.py` → SSoT: `inv9_limiter_exit.py`
- `code/archive/validation/temp4_liveness.py` → SSoT: `temp4_no_infinite_repair.py`
- `code/archive/validation/validate_state2ops_transitions.py` → SSoT: `inv10_turnover_balance.py`
- `code/archive/validation/validate_state2ops_increments.py` → SSoT: `inv5_balance_increments.py`
- `code/archive/validation/inv4_unsvc_min_repair.py` → SSoT: `inv4_unsvc_repair_time.py`
- `code/archive/analysis/sim_validation_ops_exits.py` → SSoT: `inv9_limiter_exit.py`
- `code/archive/analysis/repair_gantt_standalone.py` → SSoT: `inv3_repair_capacity.py`

Старый class-based фреймворк (P2, архив 2026-06-06) → SSoT: канонический `run_all.py` (INV-1..12 + TEMP):
- `code/archive/analysis/sim_validation_runner.py`
- `code/archive/analysis/sim_validation_runner_msg.py`
- `code/archive/analysis/sim_validation_quota.py`
- `code/archive/analysis/sim_validation_transitions.py`
- `code/archive/analysis/sim_validation_increments.py`
- `code/archive/analysis/sim_validation_units.py`

