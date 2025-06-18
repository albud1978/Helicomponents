# Вертолетный проект: Реалистичная архитектура ABM

## 📊 Реальные объемы данных

### **Исходные данные (текущие):**
- **Status_Components.xlsx**: 108,623 записи × 20 полей = ~8.6 MB
- **После enrichment**: ~12 MB (добавляем числовые поля)
- **Прогноз роста**: до 500k-1M записей максимум

### **Результаты симуляций (600M ячеек):**
- **600M ячеек** = одна матрица расчета одного сценария
- **Множество повторяющихся значений** для удобства GPU расчетов
- **Потенциал оптимизации** выгрузки в ClickHouse после GPU
- **Объем после оптимизации**: ~15-25 GB (с дедупликацией)
- **Паттерн загрузки**: Однократно после симуляции, затем только чтение

---

## 🏗️ Оптимальная архитектура движков

### **Слой 1: RAW данные (существующий)**
```sql
-- Исходные данные всех типов компонентов - частые обновления
CREATE TABLE status_components_raw (
    partno String,
    serialno String,
    ac_typ String,                      -- Ми-8Т, Ми-17, Ми-26, и др.
    component_type String,              -- ВНВ, двигатель, редуктор, и др.
    location String,
    owner String,
    condition String,
    ll UInt32, oh UInt32, oh_threshold UInt32, sne UInt32, ppr UInt32,
    mfg_date Date,
    removal_date Nullable(Date),
    target_date Nullable(Date),
    version_date Date,
    load_timestamp DateTime
    
) ENGINE = MergeTree()                   -- ✅ MergeTree: обновления + версионность
PARTITION BY toYYYYMM(version_date)
ORDER BY (component_type, partno, serialno, version_date)
SETTINGS index_granularity = 8192;
```

### **Слой 2: ENRICHED данные**
```sql
-- Обогащенные данные всех компонентов - периодическое пересоздание
CREATE TABLE status_components_enriched (
    -- Исходные строковые поля
    partno String,
    serialno String,
    ac_typ String,
    component_type String,              -- Универсальность модели
    location String,
    owner String,
    condition String,
    
    -- Числовые ID для GPU
    partno_id UInt16,
    serialno_hash UInt32,
    ac_type_mask UInt8,                 -- Битовые операции для всех типов ВС
    component_type_id UInt8,            -- ID типа компонента
    location_id UInt16,
    owner_id UInt8,
    condition_mask UInt8,
    
    -- Ресурсы
    ll UInt32, oh UInt32, oh_threshold UInt32, sne UInt32, ppr UInt32,
    mfg_date Date,
    removal_date Nullable(Date),
    target_date Nullable(Date),
    
    -- Метаданные
    version_date Date,
    load_timestamp DateTime,
    enrichment_timestamp DateTime
    
) ENGINE = MergeTree()                   -- ✅ MergeTree: версионность важна
PARTITION BY (component_type, toYYYYMM(version_date))
ORDER BY (component_type_id, partno_id, ac_type_mask, version_date, serialno_hash)
SETTINGS index_granularity = 8192;
```

### **Слой 3: Результаты симуляций**

#### **3.1 Оптимизированные результаты (после дедупликации)**
```sql
CREATE TABLE abm_results_optimized (
    result_id UInt64,                    -- Уникальный ID результата
    
    -- Ключи для связи с исходными данными
    partno_id UInt16,                    
    component_type_id UInt8,             -- Универсальность модели
    ac_type_mask UInt8,                  
    scenario_params String,              -- JSON с параметрами сценария
    
    -- Результаты симуляции (готовые для аналитики)
    predicted_failure_days UInt16,
    maintenance_priority UInt8,
    replacement_recommended UInt8,
    remaining_resource_pct Float32,
    risk_score Float32,
    
    -- Денормализованные строковые поля для прямого использования
    partno String,
    component_name String,
    component_type String,
    ac_typ String,
    
    -- Метаданные
    simulation_id String,                -- UUID симуляции
    simulation_date Date,
    model_version String,
    input_version_date Date,
    processing_time_ms UInt32
    
) ENGINE = MergeTree()                   -- ✅ MergeTree: готовые данные для аналитики
PARTITION BY (component_type, toYYYYMM(simulation_date))
ORDER BY (simulation_date, component_type_id, partno_id, result_id)
SETTINGS index_granularity = 8192;
```

---

## 🚀 Оптимизации для больших объемов

### **1. Агрессивное сжатие повторяющихся данных**
```sql
-- Сжатие для часто повторяющихся полей
ALTER TABLE abm_results_optimized 
MODIFY COLUMN partno CODEC(ZSTD),
MODIFY COLUMN component_name CODEC(ZSTD),
MODIFY COLUMN component_type CODEC(ZSTD),
MODIFY COLUMN ac_typ CODEC(ZSTD),
MODIFY COLUMN scenario_params CODEC(ZSTD),
MODIFY COLUMN predicted_failure_days CODEC(DoubleDelta, ZSTD),
MODIFY COLUMN risk_score CODEC(Gorilla, ZSTD),
MODIFY COLUMN remaining_resource_pct CODEC(Gorilla, ZSTD);
```

### **2. Индексы для быстрого поиска**
```sql
-- Индексы под реальные запросы аналитики
ALTER TABLE abm_results_optimized 
ADD INDEX idx_risk_score risk_score TYPE minmax GRANULARITY 4,
ADD INDEX idx_failure_days predicted_failure_days TYPE minmax GRANULARITY 4,
ADD INDEX idx_component_type component_type TYPE set(0) GRANULARITY 4,
ADD INDEX idx_ac_typ ac_typ TYPE set(0) GRANULARITY 4;
```

### **3. TTL для управления жизненным циклом**
```sql
-- Автоматическая очистка старых результатов
ALTER TABLE abm_results_optimized 
MODIFY TTL simulation_date + INTERVAL 2 YEAR;
```

---

## 📋 Оптимизированная загрузка данных

### **GPU → ClickHouse с дедупликацией**
```python
def optimize_gpu_results_for_clickhouse(gpu_matrix_600m, input_version_date):
    """
    Оптимизация 600M ячеек матрицы перед загрузкой в ClickHouse
    """
    logger.info(f"Начинаем оптимизацию матрицы {gpu_matrix_600m.shape}")
    
    # 1. Дедупликация повторяющихся строк
    deduplicated = gpu_matrix_600m.drop_duplicates(
        subset=['partno_id', 'component_type_id', 'scenario_params'],
        keep='last'  # Берем последний результат для каждой уникальной комбинации
    )
    
    logger.info(f"После дедупликации: {len(deduplicated)} записей из {len(gpu_matrix_600m)}")
    
    # 2. Группировка по ключевым полям для агрегации
    optimized_results = deduplicated.groupby([
        'partno_id', 'component_type_id', 'ac_type_mask'
    ]).agg({
        'predicted_failure_days': 'mean',    # Среднее по сценариям
        'risk_score': 'max',                 # Максимальный риск
        'remaining_resource_pct': 'min',     # Минимальный остаток ресурса
        'maintenance_priority': 'max',       # Максимальный приоритет
        'replacement_recommended': 'max'     # Есть ли рекомендация замены
    }).reset_index()
    
    # 3. Обогащение строковыми значениями
    enriched_results = enrich_with_readable_fields(optimized_results, input_version_date)
    
    logger.info(f"Финальный размер для загрузки: {len(enriched_results)} записей")
    
    return enriched_results

def load_optimized_results(optimized_results, simulation_metadata):
    """Загрузка оптимизированных результатов"""
    
    # Подготовка финальной таблицы
    final_results = optimized_results.copy()
    final_results['result_id'] = range(len(final_results))
    final_results['simulation_id'] = simulation_metadata['simulation_id']
    final_results['simulation_date'] = simulation_metadata['simulation_date']
    final_results['model_version'] = simulation_metadata['model_version']
    final_results['input_version_date'] = simulation_metadata['input_version_date']
    final_results['processing_time_ms'] = simulation_metadata['processing_time_ms']
    
    # Batch загрузка с учетом размера
    BATCH_SIZE = 500_000  # Меньший batch для оптимизированных данных
    
    total_rows = len(final_results)
    batches = (total_rows + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"Загружаем {total_rows} оптимизированных записей в {batches} батчей")
    
    for i in range(batches):
        start_idx = i * BATCH_SIZE
        end_idx = min((i + 1) * BATCH_SIZE, total_rows)
        
        batch_data = final_results.iloc[start_idx:end_idx]
        
        client.insert_dataframe(
            "abm_results_optimized", 
            batch_data,
            settings={
                'async_insert': 1,
                'wait_for_async_insert': 0
            }
        )
        
        logger.info(f"Загружен батч {i+1}/{batches}")
    
    # Финальная оптимизация таблицы
    client.execute("OPTIMIZE TABLE abm_results_optimized FINAL")
    logger.info("Оптимизация таблицы завершена")
```

---

## 📊 Прямые аналитические запросы

### **Готовые данные без материализованных VIEW**
```sql
-- Топ компонентов по риску (прямой запрос)
SELECT 
    component_type,
    partno, 
    component_name, 
    ac_typ,
    risk_score,
    predicted_failure_days,
    remaining_resource_pct,
    CASE 
        WHEN replacement_recommended = 1 THEN 'Требует замены'
        WHEN maintenance_priority >= 8 THEN 'Высокий приоритет'
        ELSE 'Нормальное состояние'
    END as status
FROM abm_results_optimized
WHERE simulation_date = (SELECT max(simulation_date) FROM abm_results_optimized)
  AND model_version = 'HELICOPTER_ABM_v1.0'
  AND risk_score > 0.7
ORDER BY risk_score DESC, predicted_failure_days ASC
LIMIT 100;

-- Анализ по типам компонентов
SELECT 
    component_type,
    ac_typ,
    count() as component_count,
    avg(risk_score) as avg_risk,
    avg(predicted_failure_days) as avg_failure_days,
    countIf(replacement_recommended = 1) as need_replacement
FROM abm_results_optimized
WHERE simulation_date >= '2024-01-01'
GROUP BY component_type, ac_typ
ORDER BY avg_risk DESC;

-- Временные тренды
SELECT 
    simulation_date,
    component_type,
    avg(risk_score) as avg_risk,
    max(risk_score) as max_risk,
    countIf(risk_score > 0.8) as high_risk_count
FROM abm_results_optimized
WHERE simulation_date >= '2024-01-01'
GROUP BY simulation_date, component_type
ORDER BY simulation_date DESC, component_type;
```

---

## 🏆 Итоговая архитектура

### **Движки (все MergeTree):**
- ✅ **RAW**: MergeTree - версионность + обновления всех компонентов
- ✅ **ENRICHED**: MergeTree - универсальная модель с числовыми ID  
- ✅ **RESULTS**: MergeTree - оптимизированные готовые данные

### **Ключевые особенности:**
1. **Универсальность модели** - не только ВНВ, все типы компонентов
2. **Оптимизация 600M ячеек** - дедупликация + агрегация перед загрузкой
3. **Готовые данные** - денормализация для прямых запросов без VIEW
4. **Правильное партиционирование** по типу компонента + дате
5. **Агрессивное сжатие** повторяющихся значений

### **Реальные размеры:**
- **RAW + ENRICHED**: ~25 MB (все компоненты)
- **RESULTS (оптимизированные)**: ~2-5 GB вместо 600M ячеек
- **Экономия места**: 80-90% благодаря дедупликации

### **Преимущества подхода:**
- ✅ **Простая аналитика** - готовые денормализованные данные
- ✅ **Высокая производительность** - без сложных JOIN и VIEW
- ✅ **Универсальность** - модель для всех типов компонентов
- ✅ **Оптимальное использование ресурсов** - разумный размер данных 