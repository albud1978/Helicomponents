# LOAD: Загрузка результатов в СУБД (планируется 📋)

## 🎯 Цель этапа
Загрузка результатов Agent-Based моделирования обратно в ClickHouse для аналитики и принятия решений.

**Статус**: Планируется после реализации этапа TRANSFORM

## 📊 Планируемая схема данных результатов

### Планируемые таблицы результатов моделирования

#### prediction_results - прогнозы отказов (планируется)
```sql
-- Планируемая структура таблицы результатов
CREATE TABLE prediction_results (
    component_id String,
    partno String,
    ac_type UInt8,
    prediction_date Date,
    failure_probability Float64,
    remaining_resource_hours UInt32,
    confidence_level Float64,
    model_version String
) ENGINE = MergeTree()
ORDER BY (component_id, prediction_date)
```

#### maintenance_schedule - оптимальные графики ТО (планируется)
```sql
-- Планируемая структура графика ТО
CREATE TABLE maintenance_schedule (
    component_id String,
    partno String,
    scheduled_date Date,
    maintenance_type Enum8('TO-1'=1, 'TO-2'=2, 'Ремонт'=3, 'Замена'=4),
    priority_level UInt8,
    estimated_cost Float64,
    downtime_hours UInt16
) ENGINE = MergeTree()
ORDER BY (scheduled_date, priority_level)
```

#### supply_recommendations - рекомендации по закупкам (планируется)
```sql
-- Планируемая структура рекомендаций по закупкам
CREATE TABLE supply_recommendations (
    partno String,
    recommended_quantity UInt16,
    target_date Date,
    urgency_level UInt8,
    estimated_demand Float64,
    safety_stock UInt16,
    lead_time_days UInt16
) ENGINE = MergeTree()
ORDER BY (target_date, urgency_level)
```

#### simulation_metrics - метрики качества моделирования (планируется)
```sql
-- Планируемая структура метрик моделирования
CREATE TABLE simulation_metrics (
    model_run_id String,
    run_date DateTime,
    prediction_accuracy Float64,
    cost_optimization_percent Float64,
    fleet_readiness_percent Float64,
    total_components_modeled UInt32,
    simulation_horizon_months UInt16
) ENGINE = MergeTree()
ORDER BY run_date
```

## 🔄 Планируемый процесс загрузки

### 1. Подготовка данных GPU → CPU (планируется)
```python
# Планируемая выгрузка результатов с GPU
gpu_predictions = flame_gpu_model.get_predictions()
gpu_schedules = flame_gpu_model.get_maintenance_schedules()
gpu_supply = flame_gpu_model.get_supply_recommendations()

# Планируемая конвертация в pandas DataFrame
df_predictions = convert_gpu_to_pandas(gpu_predictions)
df_schedules = convert_gpu_to_pandas(gpu_schedules)
df_supply = convert_gpu_to_pandas(gpu_supply)
```

### 2. Планируемая валидация результатов
- Проверка корректности дат (не в прошлом)
- Валидация партномеров против md_components
- Контроль диапазонов значений (вероятности 0-1, затраты > 0)
- Проверка целостности связей component_id

### 3. Планируемая загрузка в ClickHouse
```python
# Использование существующей безопасной конфигурации
from utils.config_loader import get_clickhouse_client
client = get_clickhouse_client()

# Планируемая батчевая загрузка результатов
client.execute('INSERT INTO prediction_results VALUES', df_predictions.values.tolist())
client.execute('INSERT INTO maintenance_schedule VALUES', df_schedules.values.tolist())
client.execute('INSERT INTO supply_recommendations VALUES', df_supply.values.tolist())
```

## 📈 Планируемые аналитические представления

### materialized_views для быстрого доступа (планируется)
```sql
-- Планируемая сводка по критическим компонентам
CREATE MATERIALIZED VIEW critical_components_mv AS
SELECT 
    partno,
    COUNT(*) as components_count,
    AVG(failure_probability) as avg_failure_prob,
    MIN(remaining_resource_hours) as min_resource
FROM prediction_results
WHERE failure_probability > 0.7
GROUP BY partno;

-- Планируемая загрузка ТО по месяцам  
CREATE MATERIALIZED VIEW monthly_maintenance_load_mv AS
SELECT
    toStartOfMonth(scheduled_date) as month,
    maintenance_type,
    COUNT(*) as maintenance_count,
    SUM(estimated_cost) as total_cost
FROM maintenance_schedule
GROUP BY month, maintenance_type;
```

## 🎯 Планируемая интеграция с бизнес-процессами

### 1. Планируемые дашборды и отчеты
- **Прогнозы отказов**: топ-10 критических компонентов
- **График ТО**: планирование загрузки ремонтных служб
- **Закупки**: автоматические заявки на критичные запчасти

### 2. Планируемые алерты и уведомления
```sql
-- Планируемые критические ситуации требующие немедленного внимания
SELECT component_id, partno, failure_probability
FROM prediction_results 
WHERE failure_probability > 0.9 
AND remaining_resource_hours < 100;
```

### 3. Планируемое API для внешних систем
- REST API для получения прогнозов
- Integration с ERP системами предприятия
- Экспорт в Excel для планово-экономических служб

## 📊 Планируемый контроль качества

### Планируемые метрики успешности ETL цикла
- **Покрытие данных**: % компонентов с прогнозами
- **Актуальность**: время между моделированием и загрузкой
- **Точность**: сравнение прогнозов с фактическими отказами
- **Производительность**: время выполнения полного ETL цикла

### Планируемый мониторинг результатов
```sql
-- Планируемая проверка качества загрузки
SELECT 
    COUNT(*) as total_predictions,
    AVG(confidence_level) as avg_confidence,
    COUNT(CASE WHEN failure_probability > 0.8 THEN 1 END) as high_risk_count
FROM prediction_results 
WHERE prediction_date = today();
```

## 🔄 Планируемое завершение ETL цикла

**EXTRACT** 🔄 → **TRANSFORM** 📋 → **LOAD** 📋

Полный цикл от Excel файлов до бизнес-решений через Flame GPU моделирование планируется реализовать поэтапно. 