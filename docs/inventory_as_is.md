# Инвентаризация AS-IS: источники данных для имитационной модели Helicomponents

> Дата: 2026-04-27
> Branch: feature/flame-messaging
> Скоуп: только AS-IS, без интерпретации/рефакторинга/предложений.

## 1. Project ClickHouse (10.95.19.132:9000 / default)
> Соединение: user **default** (пароль не публикуется).

### 1.1 `.inner_id.a69a5659-28b2-4705-8b9e-847f9dc56d1b`
- Engine: SummingMergeTree
- Partition key: (version_id, toYear(month_date))
- Sorting key: month_date, version_id
- Primary key: month_date, version_id
- Total rows: 136
- Total bytes: 39180
- `count()`: 136 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| month_date | Date |  | 1 | 1 | 1 |  |
| version_id | String |  | 1 | 1 | 1 |  |
| created_at | DateTime |  | 0 | 0 | 0 |  |
| description | String |  | 0 | 0 | 0 |  |
| avg_ops_count | Float64 |  | 0 | 0 | 0 |  |
| avg_hbs_count | Float64 |  | 0 | 0 | 0 |  |
| avg_repair_count | Float64 |  | 0 | 0 | 0 |  |
| avg_total_operable | Float64 |  | 0 | 0 | 0 |  |
| total_entry_count | Float64 |  | 0 | 0 | 0 |  |
| total_exit_count | Float64 |  | 0 | 0 | 0 |  |
| total_into_repair | Float64 |  | 0 | 0 | 0 |  |
| total_complete_repair | Float64 |  | 0 | 0 | 0 |  |
| avg_remain_repair | Float64 |  | 0 | 0 | 0 |  |
| avg_remain | Float64 |  | 0 | 0 | 0 |  |
| avg_midlife_repair | Float64 |  | 0 | 0 | 0 |  |
| avg_midlife | Float64 |  | 0 | 0 | 0 |  |
| total_hours | Float64 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.`.inner_id.a69a5659-28b2-4705-8b9e-847f9dc56d1b` (`month_date` Date, `version_id` String, `created_at` DateTime, `description` String, `avg_ops_count` Float64, `avg_hbs_count` Float64, `avg_repair_count` Float64, `avg_total_operable` Float64, `total_entry_count` Float64, `total_exit_count` Float64, `total_into_repair` Float64, `total_complete_repair` Float64, `avg_remain_repair` Float64, `avg_remain` Float64, `avg_midlife_repair` Float64, `avg_midlife` Float64, `total_hours` Float64) ENGINE = SummingMergeTree PARTITION BY (version_id, toYear(month_date)) ORDER BY (month_date, version_id) SETTINGS index_granularity = 8192
```
</details>

### 1.2 `Agregat`
- Engine: MergeTree
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: 1
- Total bytes: 603
- `count()`: 1 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ComponentNumber | Nullable(String) |  | 0 | 0 | 0 |  |
| UnloadDate | Nullable(Date) |  | 0 | 0 | 0 |  |
| NR_Mi8T | Nullable(Float64) |  | 0 | 0 | 0 |  |
| MRR_Mi8T | Nullable(Float64) |  | 0 | 0 | 0 |  |
| NR_Mi17 | Nullable(Float64) |  | 0 | 0 | 0 |  |
| MRR_Mi17 | Nullable(Float64) |  | 0 | 0 | 0 |  |
| MRR_2_Mi8T | Nullable(Float64) |  | 0 | 0 | 0 |  |
| MRR_2_Mi17 | Nullable(Float64) |  | 0 | 0 | 0 |  |
| RepairTime | Nullable(Float64) |  | 0 | 0 | 0 |  |
| PurchasePrice | Nullable(Float64) |  | 0 | 0 | 0 |  |
| RepairPrice | Nullable(Float64) |  | 0 | 0 | 0 |  |
| BR_Mi8T | Nullable(Float64) |  | 0 | 0 | 0 |  |
| BR_Mi17 | Nullable(Float64) |  | 0 | 0 | 0 |  |
| items_per_ac | Nullable(Float64) |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.Agregat (`ComponentNumber` Nullable(String), `UnloadDate` Nullable(Date), `NR_Mi8T` Nullable(Float64), `MRR_Mi8T` Nullable(Float64), `NR_Mi17` Nullable(Float64), `MRR_Mi17` Nullable(Float64), `MRR_2_Mi8T` Nullable(Float64), `MRR_2_Mi17` Nullable(Float64), `RepairTime` Nullable(Float64), `PurchasePrice` Nullable(Float64), `RepairPrice` Nullable(Float64), `BR_Mi8T` Nullable(Float64), `BR_Mi17` Nullable(Float64), `items_per_ac` Nullable(Float64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192
```
</details>

### 1.3 `FlameGPU_Agents`
- Engine: MergeTree
- Partition key: 
- Sorting key: agent_id
- Primary key: agent_id
- Total rows: 279
- Total bytes: 10595
- `count()`: 279 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| agent_id | UInt32 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| ac_type_mask | UInt8 |  | 0 | 0 | 0 |  |
| partseqno_i | UInt32 |  | 0 | 0 | 0 |  |
| mfg_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| status_id | UInt8 |  | 0 | 0 | 0 |  |
| sne | Float64 |  | 0 | 0 | 0 |  |
| ppr | Float64 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| ll | Float64 |  | 0 | 0 | 0 |  |
| oh | Float64 |  | 0 | 0 | 0 |  |
| br | Float64 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| partout_time | UInt16 |  | 0 | 0 | 0 |  |
| assembly_time | UInt16 |  | 0 | 0 | 0 |  |
| active_trigger | UInt32 |  | 0 | 0 | 0 |  |
| partout_trigger | UInt32 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt32 |  | 0 | 0 | 0 |  |
| lease_restricted | UInt8 |  | 0 | 0 | 0 |  |
| psn | String |  | 0 | 0 | 0 |  |
| address_i | String |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.FlameGPU_Agents (`agent_id` UInt32, `aircraft_number` UInt32, `ac_type_mask` UInt8, `partseqno_i` UInt32, `mfg_date` Nullable(Date), `status_id` UInt8, `sne` Float64, `ppr` Float64, `repair_days` UInt16, `ll` Float64, `oh` Float64, `br` Float64, `repair_time` UInt16, `partout_time` UInt16, `assembly_time` UInt16, `active_trigger` UInt32, `partout_trigger` UInt32, `assembly_trigger` UInt32, `lease_restricted` UInt8, `psn` String, `address_i` String) ENGINE = MergeTree ORDER BY agent_id SETTINGS index_granularity = 8192
```
</details>

### 1.4 `Heli_Components`
- Engine: MergeTree
- Partition key: (version_id, toYYYYMM(Dates))
- Sorting key: Dates, version_id, created_at
- Primary key: Dates, version_id, created_at
- Total rows: 700
- Total bytes: 102941
- `count()`: 700 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| Dates | Date |  | 1 | 1 | 1 |  |
| version_id | String |  | 1 | 1 | 1 |  |
| created_at | DateTime |  | 1 | 1 | 0 |  |
| description | String |  | 0 | 0 | 0 |  |
| ops_count | Float32 |  | 0 | 0 | 0 |  |
| hbs_count | Float32 |  | 0 | 0 | 0 |  |
| repair_count | Float32 |  | 0 | 0 | 0 |  |
| total_operable | Float32 |  | 0 | 0 | 0 |  |
| entry_count | Float32 |  | 0 | 0 | 0 |  |
| exit_count | Float32 |  | 0 | 0 | 0 |  |
| into_repair | Float32 |  | 0 | 0 | 0 |  |
| complete_repair | Float32 |  | 0 | 0 | 0 |  |
| remain_repair | Float32 |  | 0 | 0 | 0 |  |
| remain | Float32 |  | 0 | 0 | 0 |  |
| midlife_repair | Float32 |  | 0 | 0 | 0 |  |
| midlife | Float32 |  | 0 | 0 | 0 |  |
| hours | Float32 |  | 0 | 0 | 0 |  |
| year | UInt16 | toYear(Dates) | 0 | 0 | 0 |  |
| month | UInt8 | toMonth(Dates) | 0 | 0 | 0 |  |
| quarter | UInt8 | toQuarter(Dates) | 0 | 0 | 0 |  |
| week | UInt8 | toWeek(Dates) | 0 | 0 | 0 |  |
| day_of_week | UInt8 | toDayOfWeek(Dates) | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.Heli_Components (`Dates` Date, `version_id` String, `created_at` DateTime, `description` String, `ops_count` Float32, `hbs_count` Float32, `repair_count` Float32, `total_operable` Float32, `entry_count` Float32, `exit_count` Float32, `into_repair` Float32, `complete_repair` Float32, `remain_repair` Float32, `remain` Float32, `midlife_repair` Float32, `midlife` Float32, `hours` Float32, `year` UInt16 MATERIALIZED toYear(Dates), `month` UInt8 MATERIALIZED toMonth(Dates), `quarter` UInt8 MATERIALIZED toQuarter(Dates), `week` UInt8 MATERIALIZED toWeek(Dates), `day_of_week` UInt8 MATERIALIZED toDayOfWeek(Dates), INDEX idx_version_id version_id TYPE minmax GRANULARITY 1, INDEX idx_dates Dates TYPE minmax GRANULARITY 1, INDEX idx_year_month (year, month) TYPE set(100) GRANULARITY 1, INDEX idx_ops_count ops_count TYPE minmax GRANULARITY 4) ENGINE = MergeTree PARTITION BY (version_id, toYYYYMM(Dates)) ORDER BY (Dates, version_id, created_at) SETTINGS index_granularity = 8192
```
</details>

### 1.5 `Heli_Components_Monthly`
- Engine: MaterializedView
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: 136
- Total bytes: 39180
- `count()`: 136 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| month_date | Date |  | 0 | 0 | 0 |  |
| version_id | String |  | 0 | 0 | 0 |  |
| created_at | DateTime |  | 0 | 0 | 0 |  |
| description | String |  | 0 | 0 | 0 |  |
| avg_ops_count | Float64 |  | 0 | 0 | 0 |  |
| avg_hbs_count | Float64 |  | 0 | 0 | 0 |  |
| avg_repair_count | Float64 |  | 0 | 0 | 0 |  |
| avg_total_operable | Float64 |  | 0 | 0 | 0 |  |
| total_entry_count | Float64 |  | 0 | 0 | 0 |  |
| total_exit_count | Float64 |  | 0 | 0 | 0 |  |
| total_into_repair | Float64 |  | 0 | 0 | 0 |  |
| total_complete_repair | Float64 |  | 0 | 0 | 0 |  |
| avg_remain_repair | Float64 |  | 0 | 0 | 0 |  |
| avg_remain | Float64 |  | 0 | 0 | 0 |  |
| avg_midlife_repair | Float64 |  | 0 | 0 | 0 |  |
| avg_midlife | Float64 |  | 0 | 0 | 0 |  |
| total_hours | Float64 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE MATERIALIZED VIEW default.Heli_Components_Monthly (`month_date` Date, `version_id` String, `created_at` DateTime, `description` String, `avg_ops_count` Float64, `avg_hbs_count` Float64, `avg_repair_count` Float64, `avg_total_operable` Float64, `total_entry_count` Float64, `total_exit_count` Float64, `total_into_repair` Float64, `total_complete_repair` Float64, `avg_remain_repair` Float64, `avg_remain` Float64, `avg_midlife_repair` Float64, `avg_midlife` Float64, `total_hours` Float64) ENGINE = SummingMergeTree PARTITION BY (version_id, toYear(month_date)) ORDER BY (month_date, version_id) SETTINGS index_granularity = 8192 AS SELECT toStartOfMonth(Dates) AS month_date, version_id, max(created_at) AS created_at, any(description) AS description, avg(ops_count) AS avg_ops_count, avg(hbs_count) AS avg_hbs_count, avg(repair_count) AS avg_repair_count, avg(total_operable) AS avg_total_operable, sum(entry_count) AS total_entry_count, sum(exit_count) AS total_exit_count, sum(into_repair) AS total_into_repair, sum(complete_repair) AS total_complete_repair, avg(remain_repair) AS avg_remain_repair, avg(remain) AS avg_remain, avg(midlife_repair) AS avg_midlife_repair, avg(midlife) AS avg_midlife, sum(hours) AS total_hours FROM default.Heli_Components GROUP BY month_date, version_id
```
</details>

### 1.6 `LoggingLayer_Planes`
- Engine: MergeTree
- Partition key: 
- Sorting key: dates, aircraft_number
- Primary key: dates, aircraft_number
- Total rows: 11718
- Total bytes: 51448
- Distinct `version_date`: 1 (период 2025-07-04 .. 2025-07-04) | `count()`: 11718

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| dates | Date |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 1 | 1 | 0 |  |
| daily_hours | Float64 |  | 0 | 0 | 0 |  |
| status_id | UInt8 |  | 0 | 0 | 0 |  |
| partout_trigger | UInt8 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt8 |  | 0 | 0 | 0 |  |
| mfg_date | Date |  | 0 | 0 | 0 |  |
| active_trigger | UInt64 |  | 0 | 0 | 0 |  |
| ac_type_mask | UInt8 |  | 0 | 0 | 0 |  |
| sne | Float64 |  | 0 | 0 | 0 |  |
| ppr | Float64 |  | 0 | 0 | 0 |  |
| repair_days | UInt32 |  | 0 | 0 | 0 |  |
| version_date | Date |  | 0 | 0 | 0 |  |
| version_id | String |  | 0 | 0 | 0 |  |
| aircraft_age_years | Float64 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.LoggingLayer_Planes (`dates` Date, `aircraft_number` UInt32, `daily_hours` Float64, `status_id` UInt8, `partout_trigger` UInt8, `assembly_trigger` UInt8, `mfg_date` Date, `active_trigger` UInt64, `ac_type_mask` UInt8, `sne` Float64, `ppr` Float64, `repair_days` UInt32, `version_date` Date, `version_id` String, `aircraft_age_years` Float64) ENGINE = MergeTree ORDER BY (dates, aircraft_number) SETTINGS index_granularity = 8192
```
</details>

### 1.7 `OlapCube_Analytics`
- Engine: MergeTree
- Partition key: (version_id, toYYYYMM(Dates))
- Sorting key: Dates, version_id, created_at
- Primary key: Dates, version_id, created_at
- Total rows: 4000
- Total bytes: 474991
- `count()`: 4000 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| Dates | Date |  | 1 | 1 | 1 |  |
| version_id | String |  | 1 | 1 | 1 |  |
| created_at | DateTime |  | 1 | 1 | 0 |  |
| description | String |  | 0 | 0 | 0 |  |
| ops_count | Float32 |  | 0 | 0 | 0 |  |
| hbs_count | Float32 |  | 0 | 0 | 0 |  |
| repair_count | Float32 |  | 0 | 0 | 0 |  |
| total_operable | Float32 |  | 0 | 0 | 0 |  |
| entry_count | Float32 |  | 0 | 0 | 0 |  |
| exit_count | Float32 |  | 0 | 0 | 0 |  |
| into_repair | Float32 |  | 0 | 0 | 0 |  |
| complete_repair | Float32 |  | 0 | 0 | 0 |  |
| remain_repair | Float32 |  | 0 | 0 | 0 |  |
| remain | Float32 |  | 0 | 0 | 0 |  |
| midlife_repair | Float32 |  | 0 | 0 | 0 |  |
| midlife | Float32 |  | 0 | 0 | 0 |  |
| hours | Float32 |  | 0 | 0 | 0 |  |
| year | UInt16 | toYear(Dates) | 0 | 0 | 0 |  |
| month | UInt8 | toMonth(Dates) | 0 | 0 | 0 |  |
| quarter | UInt8 | toQuarter(Dates) | 0 | 0 | 0 |  |
| week | UInt8 | toWeek(Dates) | 0 | 0 | 0 |  |
| day_of_week | UInt8 | toDayOfWeek(Dates) | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.OlapCube_Analytics (`Dates` Date, `version_id` String, `created_at` DateTime, `description` String, `ops_count` Float32, `hbs_count` Float32, `repair_count` Float32, `total_operable` Float32, `entry_count` Float32, `exit_count` Float32, `into_repair` Float32, `complete_repair` Float32, `remain_repair` Float32, `remain` Float32, `midlife_repair` Float32, `midlife` Float32, `hours` Float32, `year` UInt16 MATERIALIZED toYear(Dates), `month` UInt8 MATERIALIZED toMonth(Dates), `quarter` UInt8 MATERIALIZED toQuarter(Dates), `week` UInt8 MATERIALIZED toWeek(Dates), `day_of_week` UInt8 MATERIALIZED toDayOfWeek(Dates), INDEX idx_version_id version_id TYPE minmax GRANULARITY 1, INDEX idx_dates Dates TYPE minmax GRANULARITY 1, INDEX idx_year_month (year, month) TYPE set(100) GRANULARITY 1, INDEX idx_ops_count ops_count TYPE minmax GRANULARITY 4) ENGINE = MergeTree PARTITION BY (version_id, toYYYYMM(Dates)) ORDER BY (Dates, version_id, created_at) SETTINGS index_granularity = 8192
```
</details>

### 1.8 `OlapCube_VNV`
- Engine: ReplacingMergeTree
- Partition key: 
- Sorting key: serialno, Dates
- Primary key: serialno, Dates
- Total rows: 1680420
- Total bytes: 11119690
- `count()`: 1680420 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| serialno | String |  | 1 | 1 | 0 |  |
| Dates | Date |  | 1 | 1 | 0 |  |
| Status_P | Nullable(String) |  | 0 | 0 | 0 |  |
| repair_days | Nullable(Float64) |  | 0 | 0 | 0 |  |
| sne | Nullable(Float64) |  | 0 | 0 | 0 |  |
| ppr | Nullable(Float64) |  | 0 | 0 | 0 |  |
| Status | Nullable(String) |  | 0 | 0 | 0 |  |
| daily_flight_hours | Nullable(Float64) |  | 0 | 0 | 0 |  |
| daily_flight_hours_f | Nullable(Float64) |  | 0 | 0 | 0 |  |
| BR | Nullable(Float64) |  | 0 | 0 | 0 |  |
| ll | Nullable(Float64) |  | 0 | 0 | 0 |  |
| oh | Nullable(Float64) |  | 0 | 0 | 0 |  |
| threshold | Nullable(Float64) |  | 0 | 0 | 0 |  |
| Effectivity | Nullable(String) |  | 0 | 0 | 0 |  |
| mi8t_count | Nullable(Float64) |  | 0 | 0 | 0 |  |
| mi17_count | Nullable(Float64) |  | 0 | 0 | 0 |  |
| balance_mi8t | Float64 |  | 0 | 0 | 0 |  |
| balance_mi17 | Float64 |  | 0 | 0 | 0 |  |
| balance_total | Float64 |  | 0 | 0 | 0 |  |
| stock_mi8t | Float64 |  | 0 | 0 | 0 |  |
| stock_mi17 | Float64 |  | 0 | 0 | 0 |  |
| stock_total | Float64 |  | 0 | 0 | 0 |  |
| balance_empty | Float64 |  | 0 | 0 | 0 |  |
| stock_empty | Float64 |  | 0 | 0 | 0 |  |
| location | String |  | 0 | 0 | 0 |  |
| ac_typ | String |  | 0 | 0 | 0 |  |
| RepairTime | Nullable(Float64) |  | 0 | 0 | 0 |  |
| trigger_type | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.OlapCube_VNV (`serialno` String, `Dates` Date, `Status_P` Nullable(String), `repair_days` Nullable(Float64), `sne` Nullable(Float64), `ppr` Nullable(Float64), `Status` Nullable(String), `daily_flight_hours` Nullable(Float64), `daily_flight_hours_f` Nullable(Float64), `BR` Nullable(Float64), `ll` Nullable(Float64), `oh` Nullable(Float64), `threshold` Nullable(Float64), `Effectivity` Nullable(String), `mi8t_count` Nullable(Float64), `mi17_count` Nullable(Float64), `balance_mi8t` Float64, `balance_mi17` Float64, `balance_total` Float64, `stock_mi8t` Float64, `stock_mi17` Float64, `stock_total` Float64, `balance_empty` Float64, `stock_empty` Float64, `location` String, `ac_typ` String, `RepairTime` Nullable(Float64), `trigger_type` UInt8) ENGINE = ReplacingMergeTree ORDER BY (serialno, Dates) SETTINGS index_granularity = 8192
```
</details>

### 1.9 `Vygruzka`
- Engine: MergeTree
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: 185
- Total bytes: 7267
- `count()`: 185 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_typ | Nullable(String) |  | 0 | 0 | 0 |  |
| location | Nullable(String) |  | 0 | 0 | 0 |  |
| ll | Nullable(Float64) |  | 0 | 0 | 0 |  |
| oh | Nullable(Float64) |  | 0 | 0 | 0 |  |
| sne | Nullable(Float64) |  | 0 | 0 | 0 |  |
| ppr | Nullable(Float64) |  | 0 | 0 | 0 |  |
| mfg_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| oh_at_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| shop_visit_counter | Nullable(Float64) |  | 0 | 0 | 0 |  |
| owner | Nullable(String) |  | 0 | 0 | 0 |  |
| condition | Nullable(String) |  | 0 | 0 | 0 |  |
| removal_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| repair_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| Effectivity | Nullable(String) |  | 0 | 0 | 0 |  |
| threshold | Nullable(String) |  | 0 | 0 | 0 |  |
| BR | Float64 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.Vygruzka (`partno` Nullable(String), `serialno` Nullable(String), `ac_typ` Nullable(String), `location` Nullable(String), `ll` Nullable(Float64), `oh` Nullable(Float64), `sne` Nullable(Float64), `ppr` Nullable(Float64), `mfg_date` Nullable(Date), `oh_at_date` Nullable(Date), `shop_visit_counter` Nullable(Float64), `owner` Nullable(String), `condition` Nullable(String), `removal_date` Nullable(Date), `repair_date` Nullable(Date), `Effectivity` Nullable(String), `threshold` Nullable(String), `BR` Float64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192
```
</details>

### 1.10 `ac_type_dict_flat`
- Engine: Dictionary
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None
- version/`count()`: **ошибка запроса** — `ServerException`: Code: 102. DB::Exception: Unexpected packet from server 10.95.19.132:8123 (expected Hello or Exception, got Unknown packet): (10.95.19.132:8123, local address: 172.18.0.2:37484): While executing Remot

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_type_mask | UInt64 |  | 0 | 0 | 0 |  |
| ac_typ | String |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE DICTIONARY default.ac_type_dict_flat (`ac_type_mask` UInt8, `ac_typ` String) PRIMARY KEY ac_type_mask SOURCE(CLICKHOUSE(HOST '10.95.19.132' PORT 8123 TABLE 'dict_ac_type_flat' DB 'default')) LIFETIME(MIN 0 MAX 3600) LAYOUT(FLAT(INITIAL_ARRAY_SIZE 256 MAX_ARRAY_SIZE 256))
```
</details>

### 1.11 `aircraft_number_dict_flat`
- Engine: Dictionary
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None
- version/`count()`: **ошибка запроса** — `ServerException`: Code: 102. DB::Exception: Unexpected packet from server 10.95.19.132:8123 (expected Hello or Exception, got Unknown packet): (10.95.19.132:8123, local address: 172.18.0.2:37488): While executing Remot

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_number | UInt64 |  | 0 | 0 | 0 |  |
| formatted_number | String |  | 0 | 0 | 0 |  |
| registration_code | String |  | 0 | 0 | 0 |  |
| is_leading_zero | UInt8 |  | 0 | 0 | 0 |  |
| ac_type_mask | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE DICTIONARY default.aircraft_number_dict_flat (`aircraft_number` UInt32, `formatted_number` String, `registration_code` String, `is_leading_zero` UInt8, `ac_type_mask` UInt8) PRIMARY KEY aircraft_number SOURCE(CLICKHOUSE(HOST '10.95.19.132' PORT 8123 TABLE 'dict_aircraft_number_flat' DB 'default')) LIFETIME(MIN 0 MAX 3600) LAYOUT(FLAT())
```
</details>

### 1.12 `ban_kk_analysis`
- Engine: ReplacingMergeTree
- Partition key: 
- Sorting key: kk_number
- Primary key: kk_number
- Total rows: 14
- Total bytes: 7031
- `count()`: 14 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| kk_number | String |  | 1 | 1 | 0 |  |
| control_date | Date |  | 0 | 0 | 0 |  |
| original_date | Date |  | 0 | 0 | 0 |  |
| extension_count | UInt16 |  | 0 | 0 | 0 |  |
| cluster | String |  | 0 | 0 | 0 |  |
| executor | String |  | 0 | 0 | 0 |  |
| manager | String |  | 0 | 0 | 0 |  |
| task_summary | String |  | 0 | 0 | 0 | Суть задачи — краткое описание |
| current_status | String |  | 0 | 0 | 0 | Текущий статус работ |
| last_action | String |  | 0 | 0 | 0 | Последнее действие / резолюция |
| next_action | String |  | 0 | 0 | 0 | Следующее необходимое действие |
| risk_level | String |  | 0 | 0 | 0 | Высокий / Средний / Низкий |
| risk_reason | String |  | 0 | 0 | 0 | Причина уровня риска |
| dependencies | String |  | 0 | 0 | 0 | Зависимости от других КК |
| sources_count | UInt16 |  | 0 | 0 | 0 |  |
| corpus_chars | UInt32 |  | 0 | 0 | 0 |  |
| loaded_at | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.ban_kk_analysis (`kk_number` String, `control_date` Date, `original_date` Date, `extension_count` UInt16, `cluster` String, `executor` String, `manager` String, `task_summary` String COMMENT 'Суть задачи — краткое описание', `current_status` String COMMENT 'Текущий статус работ', `last_action` String COMMENT 'Последнее действие / резолюция', `next_action` String COMMENT 'Следующее необходимое действие', `risk_level` String COMMENT 'Высокий / Средний / Низкий', `risk_reason` String COMMENT 'Причина уровня риска', `dependencies` String COMMENT 'Зависимости от других КК', `sources_count` UInt16, `corpus_chars` UInt32, `loaded_at` DateTime DEFAULT now()) ENGINE = ReplacingMergeTree(loaded_at) ORDER BY kk_number SETTINGS index_granularity = 8192
```
</details>

### 1.13 `ban_kk_corpus`
- Engine: MergeTree
- Partition key: 
- Sorting key: kk_number, source_type, source_name
- Primary key: kk_number, source_type, source_name
- Total rows: 73
- Total bytes: 150768
- `count()`: 73 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| kk_number | String |  | 1 | 1 | 0 |  |
| source_type | String |  | 1 | 1 | 0 |  |
| source_name | String |  | 1 | 1 | 0 |  |
| content | String |  | 0 | 0 | 0 |  |
| content_length | UInt32 |  | 0 | 0 | 0 |  |
| loaded_at | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.ban_kk_corpus (`kk_number` String, `source_type` String, `source_name` String, `content` String, `content_length` UInt32, `loaded_at` DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY (kk_number, source_type, source_name) SETTINGS index_granularity = 8192
```
</details>

### 1.14 `cbr_rates`
- Engine: ReplacingMergeTree
- Partition key: 
- Sorting key: rate_date, currency
- Primary key: rate_date, currency
- Total rows: 4071
- Total bytes: 32373
- `count()`: 4071 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| rate_date | Date |  | 1 | 1 | 0 | Дата курса |
| currency | String |  | 1 | 1 | 0 | ISO код валюты (USD, EUR, CNY...) |
| rate | Float64 |  | 0 | 0 | 0 | Курс к рублю (за 1 единицу валюты) |
| nominal | UInt32 |  | 0 | 0 | 0 | Номинал (1, 10, 100...) |
| name | String |  | 0 | 0 | 0 | Название валюты |
| load_time | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.cbr_rates (`rate_date` Date COMMENT 'Дата курса', `currency` String COMMENT 'ISO код валюты (USD, EUR, CNY...)', `rate` Float64 COMMENT 'Курс к рублю (за 1 единицу валюты)', `nominal` UInt32 COMMENT 'Номинал (1, 10, 100...)', `name` String COMMENT 'Название валюты', `load_time` DateTime DEFAULT now()) ENGINE = ReplacingMergeTree(load_time) ORDER BY (rate_date, currency) SETTINGS index_granularity = 8192
```
</details>

### 1.15 `dict_ac_type_flat`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: ac_type_mask, ac_typ, version_date, version_id, load_timestamp
- Primary key: ac_type_mask, ac_typ, version_date, version_id, load_timestamp
- Total rows: 4
- Total bytes: 710
- Distinct `version_date`: 1 (период 2026-04-08 .. 2026-04-08) | `count()`: 4

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_type_mask | UInt8 |  | 1 | 1 | 0 |  |
| ac_typ | String |  | 1 | 1 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| load_timestamp | DateTime | now() | 1 | 1 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.dict_ac_type_flat (`ac_type_mask` UInt8, `ac_typ` String, `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `load_timestamp` DateTime DEFAULT now()) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (ac_type_mask, ac_typ, version_date, version_id, load_timestamp) SETTINGS index_granularity = 8192
```
</details>

### 1.16 `dict_aircraft_number_flat`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: aircraft_number, version_date, version_id, load_timestamp
- Primary key: aircraft_number, version_date, version_id, load_timestamp
- Total rows: 501
- Total bytes: 7888
- Distinct `version_date`: 4 (период 2025-12-30 .. 2026-04-12) | `count()`: 501

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_number | UInt32 |  | 1 | 1 | 0 |  |
| formatted_number | String |  | 0 | 0 | 0 |  |
| registration_code | String |  | 0 | 0 | 0 |  |
| is_leading_zero | UInt8 | 0 | 0 | 0 | 0 |  |
| ac_type_mask | UInt8 | 0 | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| load_timestamp | DateTime | now() | 1 | 1 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.dict_aircraft_number_flat (`aircraft_number` UInt32, `formatted_number` String, `registration_code` String, `is_leading_zero` UInt8 DEFAULT 0, `ac_type_mask` UInt8 DEFAULT 0, `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `load_timestamp` DateTime DEFAULT now()) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (aircraft_number, version_date, version_id, load_timestamp) SETTINGS index_granularity = 8192
```
</details>

### 1.17 `dict_digital_values_flat`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: field_id, primary_table, field_name, version_date, version_id, load_timestamp
- Primary key: field_id, primary_table, field_name, version_date, version_id, load_timestamp
- Total rows: 1154
- Total bytes: 38808
- Distinct `version_date`: 5 (период 1970-01-01 .. 2026-04-08) | `count()`: 1012

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| field_id | UInt16 |  | 1 | 1 | 0 |  |
| primary_table | String |  | 1 | 1 | 0 |  |
| field_name | String |  | 1 | 1 | 0 |  |
| field_description | String |  | 0 | 0 | 0 |  |
| data_type | String |  | 0 | 0 | 0 |  |
| is_nullable | UInt8 |  | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| load_timestamp | DateTime | now() | 1 | 1 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.dict_digital_values_flat (`field_id` UInt16, `primary_table` String, `field_name` String, `field_description` String, `data_type` String, `is_nullable` UInt8, `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `load_timestamp` DateTime DEFAULT now()) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (field_id, primary_table, field_name, version_date, version_id, load_timestamp) SETTINGS index_granularity = 8192
```
</details>

### 1.18 `dict_owner_flat`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: address_i, owner, version_date, version_id, load_timestamp
- Primary key: address_i, owner, version_date, version_id, load_timestamp
- Total rows: 11
- Total bytes: 883
- Distinct `version_date`: 1 (период 2026-04-08 .. 2026-04-08) | `count()`: 11

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| address_i | UInt32 |  | 1 | 1 | 0 |  |
| owner | String |  | 1 | 1 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| load_timestamp | DateTime | now() | 1 | 1 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.dict_owner_flat (`address_i` UInt32, `owner` String, `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `load_timestamp` DateTime DEFAULT now()) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (address_i, owner, version_date, version_id, load_timestamp) SETTINGS index_granularity = 8192
```
</details>

### 1.19 `dict_partno_flat`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: partseqno_i, partno, version_date, version_id, load_timestamp
- Primary key: partseqno_i, partno, version_date, version_id, load_timestamp
- Total rows: 76
- Total bytes: 1873
- Distinct `version_date`: 1 (период 2026-04-08 .. 2026-04-08) | `count()`: 76

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partseqno_i | UInt32 |  | 1 | 1 | 0 |  |
| partno | String |  | 1 | 1 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| load_timestamp | DateTime | now() | 1 | 1 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.dict_partno_flat (`partseqno_i` UInt32, `partno` String, `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `load_timestamp` DateTime DEFAULT now()) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (partseqno_i, partno, version_date, version_id, load_timestamp) SETTINGS index_granularity = 8192
```
</details>

### 1.20 `dict_serialno_flat`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: psn, partno, serialno, version_date, version_id, load_timestamp
- Primary key: psn, partno, serialno, version_date, version_id, load_timestamp
- Total rows: 11627
- Total bytes: 155734
- Distinct `version_date`: 1 (период 2026-04-08 .. 2026-04-08) | `count()`: 11627

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| psn | UInt32 |  | 1 | 1 | 0 |  |
| partno | String |  | 1 | 1 | 0 |  |
| serialno | String |  | 1 | 1 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| load_timestamp | DateTime | now() | 1 | 1 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.dict_serialno_flat (`psn` UInt32, `partno` String, `serialno` String, `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `load_timestamp` DateTime DEFAULT now()) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (psn, partno, serialno, version_date, version_id, load_timestamp) SETTINGS index_granularity = 8192
```
</details>

### 1.21 `dict_status_flat`
- Engine: MergeTree
- Partition key: 
- Sorting key: status_id, load_timestamp
- Primary key: status_id, load_timestamp
- Total rows: 6
- Total bytes: 539
- `count()`: 6 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| status_id | UInt8 |  | 1 | 1 | 0 |  |
| status_name | String |  | 0 | 0 | 0 |  |
| load_timestamp | DateTime | now() | 1 | 1 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.dict_status_flat (`status_id` UInt8, `status_name` String, `load_timestamp` DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY (status_id, load_timestamp) SETTINGS index_granularity = 8192
```
</details>

### 1.22 `digital_values_dict_flat`
- Engine: Dictionary
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None
- version/`count()`: **ошибка запроса** — `ServerException`: Code: 102. DB::Exception: Unexpected packet from server 10.95.19.132:8123 (expected Hello or Exception, got Unknown packet): (10.95.19.132:8123, local address: 172.18.0.2:37502): While executing Remot

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| field_id | UInt64 |  | 0 | 0 | 0 |  |
| primary_table | String |  | 0 | 0 | 0 |  |
| field_name | String |  | 0 | 0 | 0 |  |
| field_description | String |  | 0 | 0 | 0 |  |
| data_type | String |  | 0 | 0 | 0 |  |
| is_nullable | UInt8 |  | 0 | 0 | 0 |  |
| version_date | Date |  | 0 | 0 | 0 |  |
| version_id | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE DICTIONARY default.digital_values_dict_flat (`field_id` UInt16, `primary_table` String, `field_name` String, `field_description` String, `data_type` String, `is_nullable` UInt8, `version_date` Date, `version_id` UInt8) PRIMARY KEY field_id SOURCE(CLICKHOUSE(HOST '10.95.19.132' PORT 8123 TABLE 'dict_digital_values_flat' DB 'default')) LIFETIME(MIN 0 MAX 3600) LAYOUT(FLAT())
```
</details>

### 1.23 `flame_macroproperty1_export`
- Engine: Memory
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: 0
- Total bytes: 0
- Distinct `version_date`: 0 (период 1970-01-01 .. 1970-01-01) | `count()`: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| record_id | UInt32 |  | 0 | 0 | 0 |  |
| ac_type_mask | Nullable(UInt8) |  | 0 | 0 | 0 | field_id: 67 |
| assembly_time | Nullable(UInt8) |  | 0 | 0 | 0 | field_id: 68 |
| br | String |  | 0 | 0 | 0 | field_id: 69 |
| common_restricted1 | Nullable(UInt8) |  | 0 | 0 | 0 | field_id: 70 |
| common_restricted2 | Nullable(UInt8) |  | 0 | 0 | 0 | field_id: 71 |
| comp_number | Nullable(UInt8) |  | 0 | 0 | 0 | field_id: 72 |
| group_by | Nullable(UInt8) |  | 0 | 0 | 0 | field_id: 73 |
| ll_mi17 | Nullable(UInt32) |  | 0 | 0 | 0 | field_id: 74 |
| ll_mi8 | Nullable(UInt32) |  | 0 | 0 | 0 | field_id: 75 |
| oh_mi17 | Nullable(UInt32) |  | 0 | 0 | 0 | field_id: 76 |
| oh_mi8 | Nullable(UInt32) |  | 0 | 0 | 0 | field_id: 77 |
| oh_threshold_mi8 | Nullable(UInt32) |  | 0 | 0 | 0 | field_id: 78 |
| partno | Nullable(String) |  | 0 | 0 | 0 | field_id: 79 |
| partno_comp | Nullable(UInt32) |  | 0 | 0 | 0 | field_id: 80 |
| partout_time | Nullable(UInt8) |  | 0 | 0 | 0 | field_id: 81 |
| ppr_new | Nullable(UInt32) |  | 0 | 0 | 0 | field_id: 82 |
| purchase_price | Nullable(Float32) |  | 0 | 0 | 0 | field_id: 83 |
| repair_price | Nullable(Float32) |  | 0 | 0 | 0 | field_id: 84 |
| repair_time | Nullable(UInt16) |  | 0 | 0 | 0 | field_id: 85 |
| restrictions_mask | UInt8 |  | 0 | 0 | 0 | field_id: 86 |
| sne_new | Nullable(UInt32) |  | 0 | 0 | 0 | field_id: 87 |
| trigger_interval | Nullable(UInt8) |  | 0 | 0 | 0 | field_id: 88 |
| type_restricted | Nullable(UInt8) |  | 0 | 0 | 0 | field_id: 89 |
| version_date | Date |  | 0 | 0 | 0 | field_id: 90 |
| version_id | UInt8 |  | 0 | 0 | 0 | field_id: 91 |
| br_mi17 | Nullable(UInt32) |  | 0 | 0 | 0 | field_id: 149 |
| br_mi8 | Nullable(UInt32) |  | 0 | 0 | 0 | field_id: 150 |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.flame_macroproperty1_export (`record_id` UInt32, `ac_type_mask` Nullable(UInt8) COMMENT 'field_id: 67', `assembly_time` Nullable(UInt8) COMMENT 'field_id: 68', `br` String COMMENT 'field_id: 69', `common_restricted1` Nullable(UInt8) COMMENT 'field_id: 70', `common_restricted2` Nullable(UInt8) COMMENT 'field_id: 71', `comp_number` Nullable(UInt8) COMMENT 'field_id: 72', `group_by` Nullable(UInt8) COMMENT 'field_id: 73', `ll_mi17` Nullable(UInt32) COMMENT 'field_id: 74', `ll_mi8` Nullable(UInt32) COMMENT 'field_id: 75', `oh_mi17` Nullable(UInt32) COMMENT 'field_id: 76', `oh_mi8` Nullable(UInt32) COMMENT 'field_id: 77', `oh_threshold_mi8` Nullable(UInt32) COMMENT 'field_id: 78', `partno` Nullable(String) COMMENT 'field_id: 79', `partno_comp` Nullable(UInt32) COMMENT 'field_id: 80', `partout_time` Nullable(UInt8) COMMENT 'field_id: 81', `ppr_new` Nullable(UInt32) COMMENT 'field_id: 82', `purchase_price` Nullable(Float32) COMMENT 'field_id: 83', `repair_price` Nullable(Float32) COMMENT 'field_id: 84', `repair_time` Nullable(UInt16) COMMENT 'field_id: 85', `restrictions_mask` UInt8 COMMENT 'field_id: 86', `sne_new` Nullable(UInt32) COMMENT 'field_id: 87', `trigger_interval` Nullable(UInt8) COMMENT 'field_id: 88', `type_restricted` Nullable(UInt8) COMMENT 'field_id: 89', `version_date` Date COMMENT 'field_id: 90', `version_id` UInt8 COMMENT 'field_id: 91', `br_mi17` Nullable(UInt32) COMMENT 'field_id: 149', `br_mi8` Nullable(UInt32) COMMENT 'field_id: 150') ENGINE = Memory COMMENT 'Экспорт MacroProperty1 из FLAME GPU (перезаписываемая таблица)'
```
</details>

### 1.24 `flame_macroproperty2_export`
- Engine: MergeTree
- Partition key: 
- Sorting key: dates, psn, aircraft_number
- Primary key: dates, psn, aircraft_number
- Total rows: 1953
- Total bytes: 17672
- `count()`: 1953 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| dates | Date |  | 1 | 1 | 0 |  |
| psn | UInt32 |  | 1 | 1 | 0 |  |
| partseqno_i | UInt32 |  | 0 | 0 | 0 |  |
| aircraft_number | UInt32 |  | 1 | 1 | 0 |  |
| ac_type_mask | UInt8 |  | 0 | 0 | 0 |  |
| status_id | UInt8 |  | 0 | 0 | 0 |  |
| daily_flight | UInt32 |  | 0 | 0 | 0 |  |
| ops_counter_mi8 | UInt16 |  | 0 | 0 | 0 |  |
| ops_counter_mi17 | UInt16 |  | 0 | 0 | 0 |  |
| ops_current_mi8 | UInt16 |  | 0 | 0 | 0 |  |
| ops_current_mi17 | UInt16 |  | 0 | 0 | 0 |  |
| partout_trigger | Nullable(Date) |  | 0 | 0 | 0 |  |
| assembly_trigger | Nullable(Date) |  | 0 | 0 | 0 |  |
| active_trigger | Nullable(Date) |  | 0 | 0 | 0 |  |
| aircraft_age_years | UInt8 |  | 0 | 0 | 0 |  |
| mfg_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| simulation_metadata | String |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.flame_macroproperty2_export (`dates` Date, `psn` UInt32, `partseqno_i` UInt32, `aircraft_number` UInt32, `ac_type_mask` UInt8, `status_id` UInt8, `daily_flight` UInt32, `ops_counter_mi8` UInt16, `ops_counter_mi17` UInt16, `ops_current_mi8` UInt16, `ops_current_mi17` UInt16, `partout_trigger` Nullable(Date), `assembly_trigger` Nullable(Date), `active_trigger` Nullable(Date), `aircraft_age_years` UInt8, `mfg_date` Nullable(Date), `sne` UInt32, `ppr` UInt32, `repair_days` UInt16, `simulation_metadata` String) ENGINE = MergeTree ORDER BY (dates, psn, aircraft_number) SETTINGS index_granularity = 8192 COMMENT 'LoggingLayer Planes (MP2) из FLAME GPU'
```
</details>

### 1.25 `flame_macroproperty3_export`
- Engine: Memory
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: 0
- Total bytes: 0
- Distinct `version_date`: 0 (период 1970-01-01 .. 1970-01-01) | `count()`: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| record_id | UInt32 |  | 0 | 0 | 0 | Порядковый номер записи |
| partseqno_i | UInt32 |  | 0 | 0 | 0 | field_id: 56 |
| psn | UInt32 |  | 0 | 0 | 0 | field_id: 58 |
| address_i | UInt16 |  | 0 | 0 | 0 | field_id: 44 |
| lease_restricted | UInt8 |  | 0 | 0 | 0 | field_id: 48 |
| group_by | UInt8 |  | 0 | 0 | 0 | field_id: 47 |
| status_id | UInt8 |  | 0 | 0 | 0 | field_id: 63 |
| aircraft_number | UInt32 |  | 0 | 0 | 0 | field_id: 45 |
| ac_type_mask | UInt8 |  | 0 | 0 | 0 | field_id: 43 |
| ll | UInt32 |  | 0 | 0 | 0 | field_id: 49 |
| oh | UInt32 |  | 0 | 0 | 0 | field_id: 52 |
| oh_threshold | UInt32 |  | 0 | 0 | 0 | field_id: 53 |
| sne | UInt32 |  | 0 | 0 | 0 | field_id: 62 |
| ppr | UInt32 |  | 0 | 0 | 0 | field_id: 57 |
| repair_days | UInt16 |  | 0 | 0 | 0 | field_id: 60 |
| mfg_date | Date |  | 0 | 0 | 0 | field_id: 51 |
| version_date | Date |  | 0 | 0 | 0 | field_id: 65 |
| version_id | UInt8 |  | 0 | 0 | 0 | field_id: 66 |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.flame_macroproperty3_export (`record_id` UInt32 COMMENT 'Порядковый номер записи', `partseqno_i` UInt32 COMMENT 'field_id: 56', `psn` UInt32 COMMENT 'field_id: 58', `address_i` UInt16 COMMENT 'field_id: 44', `lease_restricted` UInt8 COMMENT 'field_id: 48', `group_by` UInt8 COMMENT 'field_id: 47', `status_id` UInt8 COMMENT 'field_id: 63', `aircraft_number` UInt32 COMMENT 'field_id: 45', `ac_type_mask` UInt8 COMMENT 'field_id: 43', `ll` UInt32 COMMENT 'field_id: 49', `oh` UInt32 COMMENT 'field_id: 52', `oh_threshold` UInt32 COMMENT 'field_id: 53', `sne` UInt32 COMMENT 'field_id: 62', `ppr` UInt32 COMMENT 'field_id: 57', `repair_days` UInt16 COMMENT 'field_id: 60', `mfg_date` Date COMMENT 'field_id: 51', `version_date` Date COMMENT 'field_id: 65', `version_id` UInt8 COMMENT 'field_id: 66') ENGINE = Memory COMMENT 'Экспорт MacroProperty3 из FLAME GPU для визуального контроля'
```
</details>

### 1.26 `flame_macroproperty4_export`
- Engine: MergeTree
- Partition key: 
- Sorting key: record_index, export_timestamp
- Primary key: record_index, export_timestamp
- Total rows: 4000
- Total bytes: 27078
- `count()`: 4000 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| record_index | UInt32 |  | 1 | 1 | 0 |  |
| dates | Date |  | 0 | 0 | 0 | field_id: 25 |
| new_counter_mi17 | UInt8 |  | 0 | 0 | 0 | field_id: 26 |
| ops_counter_mi17 | UInt16 |  | 0 | 0 | 0 | field_id: 27 |
| ops_counter_mi8 | UInt16 |  | 0 | 0 | 0 | field_id: 28 |
| ops_counter_total | UInt16 |  | 0 | 0 | 0 | field_id: 29 |
| trigger_program | Int8 |  | 0 | 0 | 0 | field_id: 30 |
| trigger_program_mi17 | Int8 |  | 0 | 0 | 0 | field_id: 31 |
| trigger_program_mi8 | Int8 |  | 0 | 0 | 0 | field_id: 32 |
| export_timestamp | DateTime | now() | 1 | 1 | 0 |  |
| flame_gpu_version | String |  | 0 | 0 | 0 |  |
| data_version_date | Date |  | 0 | 0 | 0 |  |
| data_version_id | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.flame_macroproperty4_export (`record_index` UInt32, `dates` Date COMMENT 'field_id: 25', `new_counter_mi17` UInt8 COMMENT 'field_id: 26', `ops_counter_mi17` UInt16 COMMENT 'field_id: 27', `ops_counter_mi8` UInt16 COMMENT 'field_id: 28', `ops_counter_total` UInt16 COMMENT 'field_id: 29', `trigger_program` Int8 COMMENT 'field_id: 30', `trigger_program_mi17` Int8 COMMENT 'field_id: 31', `trigger_program_mi8` Int8 COMMENT 'field_id: 32', `export_timestamp` DateTime DEFAULT now(), `flame_gpu_version` String, `data_version_date` Date, `data_version_id` UInt8) ENGINE = MergeTree ORDER BY (record_index, export_timestamp) SETTINGS index_granularity = 8192 COMMENT 'Экспорт MacroProperty4 из FLAME GPU для визуального контроля'
```
</details>

### 1.27 `flame_macroproperty5_export`
- Engine: MergeTree
- Partition key: 
- Sorting key: record_index, export_timestamp
- Primary key: record_index, export_timestamp
- Total rows: 1116000
- Total bytes: 4803829
- `count()`: 1116000 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| record_index | UInt32 |  | 1 | 1 | 0 |  |
| ac_type_mask | UInt8 |  | 0 | 0 | 0 | field_id: 35 |
| aircraft_number | UInt16 |  | 0 | 0 | 0 | field_id: 36 |
| daily_hours | UInt32 |  | 0 | 0 | 0 | field_id: 37 |
| dates | Date |  | 0 | 0 | 0 | field_id: 38 |
| export_timestamp | DateTime | now() | 1 | 1 | 0 |  |
| flame_gpu_version | String |  | 0 | 0 | 0 |  |
| data_version_date | Date |  | 0 | 0 | 0 |  |
| data_version_id | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.flame_macroproperty5_export (`record_index` UInt32, `ac_type_mask` UInt8 COMMENT 'field_id: 35', `aircraft_number` UInt16 COMMENT 'field_id: 36', `daily_hours` UInt32 COMMENT 'field_id: 37', `dates` Date COMMENT 'field_id: 38', `export_timestamp` DateTime DEFAULT now(), `flame_gpu_version` String, `data_version_date` Date, `data_version_id` UInt8) ENGINE = MergeTree ORDER BY (record_index, export_timestamp) SETTINGS index_granularity = 8192 COMMENT 'Экспорт MacroProperty5 из FLAME GPU для визуального контроля'
```
</details>

### 1.28 `flame_property_export`
- Engine: MergeTree
- Partition key: 
- Sorting key: export_timestamp
- Primary key: export_timestamp
- Total rows: 1
- Total bytes: 769
- Distinct `version_date`: 1 (период 2025-07-04 .. 2025-07-04) | `count()`: 1

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | Date |  | 0 | 0 | 0 | field_id: 71 |
| version_id | UInt8 |  | 0 | 0 | 0 | field_id: 72 |
| export_timestamp | DateTime | now() | 1 | 1 | 0 |  |
| flame_gpu_version | String |  | 0 | 0 | 0 |  |
| data_version_date | Date |  | 0 | 0 | 0 |  |
| data_version_id | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.flame_property_export (`version_date` Date COMMENT 'field_id: 71', `version_id` UInt8 COMMENT 'field_id: 72', `export_timestamp` DateTime DEFAULT now(), `flame_gpu_version` String, `data_version_date` Date, `data_version_id` UInt8) ENGINE = MergeTree ORDER BY export_timestamp SETTINGS index_granularity = 8192 COMMENT 'Экспорт Property из FLAME GPU для визуального контроля'
```
</details>

### 1.29 `fleet`
- Engine: MergeTree
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: 24
- Total bytes: 470
- `count()`: 24 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| month | String |  | 0 | 0 | 0 |  |
| mi8t_count | Nullable(Float64) |  | 0 | 0 | 0 |  |
| mi17_count | Nullable(Float64) |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.fleet (`month` String, `mi8t_count` Nullable(Float64), `mi17_count` Nullable(Float64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192
```
</details>

### 1.30 `flight_hours`
- Engine: MergeTree
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: 2
- Total bytes: 144
- `count()`: 2 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| monthly_flight_hours | Nullable(Float64) |  | 0 | 0 | 0 |  |
| daily_flight_hours | Nullable(Float64) |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.flight_hours (`monthly_flight_hours` Nullable(Float64), `daily_flight_hours` Nullable(Float64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192
```
</details>

### 1.31 `flight_hours_mv`
- Engine: MaterializedView
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None
- `count()`: 2 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| monthly_flight_hours | Nullable(Float64) |  | 0 | 0 | 0 |  |
| daily_flight_hours | Nullable(Float64) |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE MATERIALIZED VIEW default.flight_hours_mv TO default.flight_hours (`monthly_flight_hours` Nullable(Float64), `daily_flight_hours` Nullable(Float64)) AS SELECT monthly_flight_hours, round((monthly_flight_hours * 12) / 365, 2) AS daily_flight_hours FROM default.monthly_flight_hours
```
</details>

### 1.32 `flight_program_ac`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_date, dates
- Primary key: version_date, dates
- Total rows: 16000
- Total bytes: 22638
- Distinct `version_date`: 4 (период 2025-07-04 .. 2026-04-08) | `count()`: 16000

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| dates | Date |  | 1 | 1 | 0 |  |
| ops_counter_mi8 | UInt16 |  | 0 | 0 | 0 |  |
| ops_counter_mi17 | UInt16 |  | 0 | 0 | 0 |  |
| ops_counter_total | UInt16 |  | 0 | 0 | 0 |  |
| new_counter_mi17 | UInt8 |  | 0 | 0 | 0 |  |
| trigger_program_mi8 | Int8 |  | 0 | 0 | 0 |  |
| trigger_program_mi17 | Int8 |  | 0 | 0 | 0 |  |
| trigger_program | Int8 |  | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 0 |  |
| version_id | UInt8 | 1 | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.flight_program_ac (`dates` Date, `ops_counter_mi8` UInt16, `ops_counter_mi17` UInt16, `ops_counter_total` UInt16, `new_counter_mi17` UInt8, `trigger_program_mi8` Int8, `trigger_program_mi17` Int8, `trigger_program` Int8, `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1) ENGINE = MergeTree ORDER BY (version_date, dates) SETTINGS index_granularity = 8192
```
</details>

### 1.33 `flight_program_fl`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_date, aircraft_number, dates
- Primary key: version_date, aircraft_number, dates
- Total rows: 7120000
- Total bytes: 2461695
- Distinct `version_date`: 4 (период 2025-07-04 .. 2026-04-08) | `count()`: 7120000

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_number | UInt32 |  | 1 | 1 | 0 |  |
| dates | Date |  | 1 | 1 | 0 |  |
| daily_hours | UInt32 |  | 0 | 0 | 0 |  |
| ac_type_mask | UInt8 |  | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 0 |  |
| version_id | UInt8 | 1 | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.flight_program_fl (`aircraft_number` UInt32, `dates` Date, `daily_hours` UInt32, `ac_type_mask` UInt8, `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1) ENGINE = MergeTree ORDER BY (version_date, aircraft_number, dates) SETTINGS index_granularity = 8192
```
</details>

### 1.34 `heli_pandas`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: version_date, version_id
- Primary key: version_date, version_id
- Total rows: 68199
- Total bytes: 2075179
- Distinct `version_date`: 4 (период 2025-07-04 .. 2026-04-08) | `count()`: 45277
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=652, 2=485, 3=2096, 4=1580, 5=1115, 6=6685, 7=1044, 8=1175, 9=1184, 10=1043

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_typ | Nullable(String) |  | 0 | 0 | 0 |  |
| location | Nullable(String) |  | 0 | 0 | 0 |  |
| mfg_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| removal_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| target_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| condition | Nullable(String) |  | 0 | 0 | 0 |  |
| owner | Nullable(String) |  | 0 | 0 | 0 |  |
| lease_restricted | UInt8 | 0 | 0 | 0 | 0 |  |
| oh | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| oh_threshold | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ll | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| sne | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ppr | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| partseqno_i | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| psn | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| address_i | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| ac_type_i | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| status_id | UInt8 | 0 | 0 | 0 | 0 |  |
| repair_days | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| aircraft_number | UInt32 | 0 | 0 | 0 | 0 |  |
| ac_type_mask | UInt8 | 0 | 0 | 0 | 0 |  |
| group_by | UInt8 | 0 | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.heli_pandas (`partno` Nullable(String), `serialno` Nullable(String), `ac_typ` Nullable(String), `location` Nullable(String), `mfg_date` Nullable(Date), `removal_date` Nullable(Date), `target_date` Nullable(Date), `condition` Nullable(String), `owner` Nullable(String), `lease_restricted` UInt8 DEFAULT 0, `oh` Nullable(UInt32), `oh_threshold` Nullable(UInt32), `ll` Nullable(UInt32), `sne` Nullable(UInt32), `ppr` Nullable(UInt32), `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `partseqno_i` Nullable(UInt32), `psn` Nullable(UInt32), `address_i` Nullable(UInt16), `ac_type_i` Nullable(UInt16), `status_id` UInt8 DEFAULT 0, `repair_days` Nullable(UInt16), `aircraft_number` UInt32 DEFAULT 0, `ac_type_mask` UInt8 DEFAULT 0, `group_by` UInt8 DEFAULT 0) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (version_date, version_id) SETTINGS index_granularity = 8192
```
</details>

### 1.35 `heli_pandas_backup_20260228_163254`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: version_date, version_id
- Primary key: version_date, version_id
- Total rows: 33797
- Total bytes: 1027323
- Distinct `version_date`: 3 (период 2025-07-04 .. 2026-02-21) | `count()`: 33797
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=489, 2=362, 3=1570, 4=1179, 5=832, 6=4982, 7=779, 8=877, 9=883, 10=779

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_typ | Nullable(String) |  | 0 | 0 | 0 |  |
| location | Nullable(String) |  | 0 | 0 | 0 |  |
| mfg_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| removal_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| target_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| condition | Nullable(String) |  | 0 | 0 | 0 |  |
| owner | Nullable(String) |  | 0 | 0 | 0 |  |
| lease_restricted | UInt8 | 0 | 0 | 0 | 0 |  |
| oh | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| oh_threshold | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ll | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| sne | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ppr | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| partseqno_i | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| psn | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| address_i | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| ac_type_i | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| status_id | UInt8 | 0 | 0 | 0 | 0 |  |
| repair_days | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| aircraft_number | UInt32 | 0 | 0 | 0 | 0 |  |
| ac_type_mask | UInt8 | 0 | 0 | 0 | 0 |  |
| group_by | UInt8 | 0 | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.heli_pandas_backup_20260228_163254 (`partno` Nullable(String), `serialno` Nullable(String), `ac_typ` Nullable(String), `location` Nullable(String), `mfg_date` Nullable(Date), `removal_date` Nullable(Date), `target_date` Nullable(Date), `condition` Nullable(String), `owner` Nullable(String), `lease_restricted` UInt8 DEFAULT 0, `oh` Nullable(UInt32), `oh_threshold` Nullable(UInt32), `ll` Nullable(UInt32), `sne` Nullable(UInt32), `ppr` Nullable(UInt32), `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `partseqno_i` Nullable(UInt32), `psn` Nullable(UInt32), `address_i` Nullable(UInt16), `ac_type_i` Nullable(UInt16), `status_id` UInt8 DEFAULT 0, `repair_days` Nullable(UInt16), `aircraft_number` UInt32 DEFAULT 0, `ac_type_mask` UInt8 DEFAULT 0, `group_by` UInt8 DEFAULT 0) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (version_date, version_id) SETTINGS index_granularity = 8192
```
</details>

### 1.36 `heli_pandas_dwh`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: version_date, version_id
- Primary key: version_date, version_id
- Total rows: 11480
- Total bytes: 319468
- Distinct `version_date`: 1 (период 2026-04-08 .. 2026-04-08) | `count()`: 11480
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=163, 2=123, 3=526, 4=401, 5=283, 6=1703, 7=265, 8=298, 9=301, 10=264

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_typ | Nullable(String) |  | 0 | 0 | 0 |  |
| location | Nullable(String) |  | 0 | 0 | 0 |  |
| mfg_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| removal_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| target_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| condition | Nullable(String) |  | 0 | 0 | 0 |  |
| owner | Nullable(String) |  | 0 | 0 | 0 |  |
| lease_restricted | UInt8 | 0 | 0 | 0 | 0 |  |
| oh | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| oh_threshold | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ll | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| sne | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ppr | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| partseqno_i | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| psn | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| address_i | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| ac_type_i | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| status_id | UInt8 | 0 | 0 | 0 | 0 |  |
| repair_days | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| aircraft_number | UInt32 | 0 | 0 | 0 | 0 |  |
| ac_type_mask | UInt8 | 0 | 0 | 0 | 0 |  |
| group_by | UInt8 | 0 | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.heli_pandas_dwh (`partno` Nullable(String), `serialno` Nullable(String), `ac_typ` Nullable(String), `location` Nullable(String), `mfg_date` Nullable(Date), `removal_date` Nullable(Date), `target_date` Nullable(Date), `condition` Nullable(String), `owner` Nullable(String), `lease_restricted` UInt8 DEFAULT 0, `oh` Nullable(UInt32), `oh_threshold` Nullable(UInt32), `ll` Nullable(UInt32), `sne` Nullable(UInt32), `ppr` Nullable(UInt32), `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `partseqno_i` Nullable(UInt32), `psn` Nullable(UInt32), `address_i` Nullable(UInt16), `ac_type_i` Nullable(UInt16), `status_id` UInt8 DEFAULT 0, `repair_days` Nullable(UInt16), `aircraft_number` UInt32 DEFAULT 0, `ac_type_mask` UInt8 DEFAULT 0, `group_by` UInt8 DEFAULT 0) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (version_date, version_id) SETTINGS index_granularity = 8192
```
</details>

### 1.37 `heli_pandas_src`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: version_date, version_id
- Primary key: version_date, version_id
- Total rows: 11485
- Total bytes: 469238
- Distinct `version_date`: 1 (период 2026-04-08 .. 2026-04-08) | `count()`: 11485
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=163, 2=123, 3=526, 4=403, 5=283, 6=1698, 7=265, 8=300, 9=301, 10=264

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_typ | Nullable(String) |  | 0 | 0 | 0 |  |
| location | Nullable(String) |  | 0 | 0 | 0 |  |
| mfg_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| removal_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| target_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| condition | Nullable(String) |  | 0 | 0 | 0 |  |
| owner | Nullable(String) |  | 0 | 0 | 0 |  |
| lease_restricted | UInt8 | 0 | 0 | 0 | 0 |  |
| oh | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| oh_threshold | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ll | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| sne | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ppr | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| partseqno_i | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| psn | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| address_i | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| ac_type_i | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| status_id | UInt8 | 0 | 0 | 0 | 0 |  |
| repair_days | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| aircraft_number | UInt32 | 0 | 0 | 0 | 0 |  |
| ac_type_mask | UInt8 | 0 | 0 | 0 | 0 |  |
| group_by | UInt8 | 0 | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.heli_pandas_src (`partno` Nullable(String), `serialno` Nullable(String), `ac_typ` Nullable(String), `location` Nullable(String), `mfg_date` Nullable(Date), `removal_date` Nullable(Date), `target_date` Nullable(Date), `condition` Nullable(String), `owner` Nullable(String), `lease_restricted` UInt8 DEFAULT 0, `oh` Nullable(UInt32), `oh_threshold` Nullable(UInt32), `ll` Nullable(UInt32), `sne` Nullable(UInt32), `ppr` Nullable(UInt32), `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `partseqno_i` Nullable(UInt32), `psn` Nullable(UInt32), `address_i` Nullable(UInt16), `ac_type_i` Nullable(UInt16), `status_id` UInt8 DEFAULT 0, `repair_days` Nullable(UInt16), `aircraft_number` UInt32 DEFAULT 0, `ac_type_mask` UInt8 DEFAULT 0, `group_by` UInt8 DEFAULT 0) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (version_date, version_id) SETTINGS index_granularity = 8192
```
</details>

### 1.38 `heli_raw`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: version_date, version_id
- Primary key: version_date, version_id
- Total rows: 974948
- Total bytes: 27560735
- Distinct `version_date`: 4 (период 2025-07-04 .. 2026-04-08) | `count()`: 772187

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_typ | Nullable(String) |  | 0 | 0 | 0 |  |
| location | Nullable(String) |  | 0 | 0 | 0 |  |
| mfg_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| removal_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| target_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| condition | Nullable(String) |  | 0 | 0 | 0 |  |
| owner | Nullable(String) |  | 0 | 0 | 0 |  |
| lease_restricted | UInt8 | 0 | 0 | 0 | 0 |  |
| oh | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| oh_threshold | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ll | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| sne | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ppr | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| partseqno_i | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| psn | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| address_i | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| ac_type_i | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| oh_at_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| shop_visit_counter | Nullable(UInt16) |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.heli_raw (`partno` Nullable(String), `serialno` Nullable(String), `ac_typ` Nullable(String), `location` Nullable(String), `mfg_date` Nullable(Date), `removal_date` Nullable(Date), `target_date` Nullable(Date), `condition` Nullable(String), `owner` Nullable(String), `lease_restricted` UInt8 DEFAULT 0, `oh` Nullable(UInt32), `oh_threshold` Nullable(UInt32), `ll` Nullable(UInt32), `sne` Nullable(UInt32), `ppr` Nullable(UInt32), `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `partseqno_i` Nullable(UInt32), `psn` Nullable(UInt32), `address_i` Nullable(UInt16), `ac_type_i` Nullable(UInt16), `oh_at_date` Nullable(Date), `shop_visit_counter` Nullable(UInt16)) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (version_date, version_id) SETTINGS index_granularity = 8192
```
</details>

### 1.39 `heli_raw_backup_20260228_163254`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: version_date, version_id
- Primary key: version_date, version_id
- Total rows: 569426
- Total bytes: 15975754
- Distinct `version_date`: 3 (период 2025-07-04 .. 2026-02-21) | `count()`: 569426

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_typ | Nullable(String) |  | 0 | 0 | 0 |  |
| location | Nullable(String) |  | 0 | 0 | 0 |  |
| mfg_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| removal_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| target_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| condition | Nullable(String) |  | 0 | 0 | 0 |  |
| owner | Nullable(String) |  | 0 | 0 | 0 |  |
| lease_restricted | UInt8 | 0 | 0 | 0 | 0 |  |
| oh | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| oh_threshold | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ll | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| sne | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ppr | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| partseqno_i | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| psn | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| address_i | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| ac_type_i | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| oh_at_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| shop_visit_counter | Nullable(UInt16) |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.heli_raw_backup_20260228_163254 (`partno` Nullable(String), `serialno` Nullable(String), `ac_typ` Nullable(String), `location` Nullable(String), `mfg_date` Nullable(Date), `removal_date` Nullable(Date), `target_date` Nullable(Date), `condition` Nullable(String), `owner` Nullable(String), `lease_restricted` UInt8 DEFAULT 0, `oh` Nullable(UInt32), `oh_threshold` Nullable(UInt32), `ll` Nullable(UInt32), `sne` Nullable(UInt32), `ppr` Nullable(UInt32), `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `partseqno_i` Nullable(UInt32), `psn` Nullable(UInt32), `address_i` Nullable(UInt16), `ac_type_i` Nullable(UInt16), `oh_at_date` Nullable(Date), `shop_visit_counter` Nullable(UInt16)) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (version_date, version_id) SETTINGS index_granularity = 8192
```
</details>

### 1.40 `md_components`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: version_date, version_id
- Primary key: version_date, version_id
- Total rows: 77
- Total bytes: 4606
- Distinct `version_date`: 1 (период 2025-07-04 .. 2025-07-04) | `count()`: 77
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=4, 2=2, 3=2, 4=2, 5=2, 6=2, 7=2, 8=2, 9=3, 10=2

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| comp_number | Nullable(UInt8) |  | 0 | 0 | 0 |  |
| group_by | Nullable(UInt8) |  | 0 | 0 | 0 |  |
| ac_type_mask | Nullable(UInt8) |  | 0 | 0 | 0 |  |
| type_restricted | Nullable(UInt8) |  | 0 | 0 | 0 |  |
| common_restricted1 | Nullable(UInt8) |  | 0 | 0 | 0 |  |
| common_restricted2 | Nullable(UInt8) |  | 0 | 0 | 0 |  |
| trigger_interval | Nullable(UInt8) |  | 0 | 0 | 0 |  |
| partout_time | Nullable(UInt8) |  | 0 | 0 | 0 |  |
| assembly_time | Nullable(UInt8) |  | 0 | 0 | 0 |  |
| repair_number | Nullable(UInt8) |  | 0 | 0 | 0 |  |
| repair_time | Nullable(UInt16) |  | 0 | 0 | 0 |  |
| ll_mi8 | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| oh_mi8 | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| oh_threshold_mi8 | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ll_mi17 | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| oh_mi17 | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| second_ll | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| repair_price | Nullable(Float32) |  | 0 | 0 | 0 |  |
| purchase_price | Nullable(Float32) |  | 0 | 0 | 0 |  |
| sne_new | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| ppr_new | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |
| br_mi8 | Nullable(UInt32) | NULL | 0 | 0 | 0 |  |
| br_mi17 | Nullable(UInt32) | NULL | 0 | 0 | 0 |  |
| br2_mi17 | Nullable(UInt32) | NULL | 0 | 0 | 0 |  |
| partno_comp | Nullable(UInt32) | NULL | 0 | 0 | 0 |  |
| restrictions_mask | UInt8 | 0 | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.md_components (`partno` Nullable(String), `comp_number` Nullable(UInt8), `group_by` Nullable(UInt8), `ac_type_mask` Nullable(UInt8), `type_restricted` Nullable(UInt8), `common_restricted1` Nullable(UInt8), `common_restricted2` Nullable(UInt8), `trigger_interval` Nullable(UInt8), `partout_time` Nullable(UInt8), `assembly_time` Nullable(UInt8), `repair_number` Nullable(UInt8), `repair_time` Nullable(UInt16), `ll_mi8` Nullable(UInt32), `oh_mi8` Nullable(UInt32), `oh_threshold_mi8` Nullable(UInt32), `ll_mi17` Nullable(UInt32), `oh_mi17` Nullable(UInt32), `second_ll` Nullable(UInt32), `repair_price` Nullable(Float32), `purchase_price` Nullable(Float32), `sne_new` Nullable(UInt32), `ppr_new` Nullable(UInt32), `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1, `br_mi8` Nullable(UInt32) DEFAULT NULL, `br_mi17` Nullable(UInt32) DEFAULT NULL, `br2_mi17` Nullable(UInt32) DEFAULT NULL, `partno_comp` Nullable(UInt32) DEFAULT NULL, `restrictions_mask` UInt8 DEFAULT 0) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (version_date, version_id) SETTINGS index_granularity = 8192
```
</details>

### 1.41 `mm_orders`
- Engine: MergeTree
- Partition key: 
- Sorting key: amos_order, amos_pos, sap_doc, sap_pos
- Primary key: amos_order, amos_pos, sap_doc, sap_pos
- Total rows: 2964
- Total bytes: 531476
- `count()`: 2964 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| key_sap | String |  | 0 | 0 | 0 | Составной ключ SAP: документ+позиция (450317838810) |
| key_amos | String |  | 0 | 0 | 0 | Составной ключ AMOS: заказ+позиция (P165744253) |
| sap_doc | String |  | 1 | 1 | 0 | Номер документа закупки SAP (4503178388) |
| sap_pos | UInt16 |  | 1 | 1 | 0 | Позиция в документе SAP (10,20,30) |
| amos_order | String |  | 1 | 1 | 0 | Номер заказа в AMOS (P16574425) |
| amos_pos | UInt16 |  | 1 | 1 | 0 | Позиция в заказе AMOS (1,2,3) |
| analysis | String |  | 0 | 0 | 0 | Результат анализа: 01 актуально, удален в AMOS, Возврат брака... |
| draft_flag | String |  | 0 | 0 | 0 | Признак драфта в SAP (C) |
| return_flag | String |  | 0 | 0 | 0 | Признак возврата брака (L) |
| final_delivery_nd | String |  | 0 | 0 | 0 | Конечная поставка |
| deletion_flag | String |  | 0 | 0 | 0 | Индикатор удаления |
| amos_pos_status | String |  | 0 | 0 | 0 | Статус позиции в AMOS (O=open, R=received, CA=cancelled) |
| amos_contract | String |  | 0 | 0 | 0 | Договор в AMOS (19/23ПА, 49/24ПА...) |
| purch_doc_type | String |  | 0 | 0 | 0 | Вид документа закупки (NB) |
| doc_type | String |  | 0 | 0 | 0 | Тип документа (F) |
| purch_group | String |  | 0 | 0 | 0 | Группа закупок (U17, U60) |
| acct_assign_type | String |  | 0 | 0 | 0 | Тип контировки (L) |
| po_history | String |  | 0 | 0 | 0 | История заказа / ДокумПоОтзыв |
| toro_order | String |  | 0 | 0 | 0 | Заказ ТОРО |
| doc_date | Nullable(Date) |  | 0 | 0 | 0 | Дата документа |
| target_date | Nullable(Date) |  | 0 | 0 | 0 | Целевая дата поставки (Targetdate) |
| confirmed_date | Nullable(Date) |  | 0 | 0 | 0 | Подтвержденная дата (confDate) |
| earliest_delivery | Nullable(Date) |  | 0 | 0 | 0 | Самая ранняя дата поставки |
| vendor_code | String |  | 0 | 0 | 0 | Код поставщика SAP (1009236) |
| vendor_name | String |  | 0 | 0 | 0 | Наименование поставщика (ООО ФТГ) |
| material | String |  | 0 | 0 | 0 | Номер материала SAP (29112) |
| short_text | String |  | 0 | 0 | 0 | Краткий текст: партномер_описание |
| plant | String |  | 0 | 0 | 0 | Завод (1101, 1109, 1110) |
| storage_loc | String |  | 0 | 0 | 0 | Склад |
| order_qty | Nullable(Float64) |  | 0 | 0 | 0 | Количество заказа |
| order_unit | String |  | 0 | 0 | 0 | Единица измерения (ШТ, КГ, М, М2) |
| net_price | Nullable(Float64) |  | 0 | 0 | 0 | Цена нетто |
| currency | String |  | 0 | 0 | 0 | Валюта (RUB, USDUE, CNY) |
| price_unit | Nullable(Float64) |  | 0 | 0 | 0 | Единица цены |
| target_qty | Nullable(Float64) |  | 0 | 0 | 0 | Целевое количество |
| open_contract_qty | Nullable(Float64) |  | 0 | 0 | 0 | Открытое договорное количество |
| still_deliver_qty | Nullable(Float64) |  | 0 | 0 | 0 | Ещё поставить (количество) |
| still_deliver_val | Nullable(Float64) |  | 0 | 0 | 0 | Ещё для поставки (стоимость) |
| to_invoice_qty | Nullable(Float64) |  | 0 | 0 | 0 | Для фактурирования (количество) |
| to_invoice_val | Nullable(Float64) |  | 0 | 0 | 0 | Для фактурирования (стоимость) |
| positions_count | UInt16 |  | 0 | 0 | 0 | Число позиций в заказе |
| amos_source | String |  | 0 | 0 | 0 | Источник: open / additional / none |
| amos_order_date | Nullable(Date) |  | 0 | 0 | 0 | Дата заказа в AMOS |
| amos_vendor | String |  | 0 | 0 | 0 | Краткое имя поставщика из AMOS (ЮНИТ, XIAMEN) |
| amos_partno | String |  | 0 | 0 | 0 | Номер детали из AMOS |
| amos_project | String |  | 0 | 0 | 0 | Проект/договор из AMOS (19/23ПА) |
| amos_ext_state | String |  | 0 | 0 | 0 | Внешний статус AMOS (O, PR, R, CA, B) |
| amos_state | String |  | 0 | 0 | 0 | Внутренний статус AMOS |
| amos_order_status | String |  | 0 | 0 | 0 | Числовой статус заказа AMOS (0) |
| amos_qty | Nullable(Float64) |  | 0 | 0 | 0 | Количество из AMOS |
| amos_amount | Nullable(Float64) |  | 0 | 0 | 0 | Сумма из AMOS (в рублях) |
| amos_cost_type | String |  | 0 | 0 | 0 | Тип НДС: БЕЗ НДС / НДС 20% / НДС 5% / НДС 0% |
| amos_sap_order | String |  | 0 | 0 | 0 | Номер заказа SAP из AMOS (основной) |
| amos_sap_order2 | String |  | 0 | 0 | 0 | Номер заказа SAP из AMOS (дополнительный) |
| amos_retry | String |  | 0 | 0 | 0 | Признак повтора (Истинно) |
| amos_comment | String |  | 0 | 0 | 0 | Комментарий из AMOS |
| sap_header_found | UInt8 |  | 0 | 0 | 0 | Найден ли заголовок в SAP (0/1) |
| sap_created_date | Nullable(Date) |  | 0 | 0 | 0 | Дата создания в SAP |
| sap_created_by | String |  | 0 | 0 | 0 | Кто создал (PO_PROXY, OHOTKIN_VI) |
| payment_terms | String |  | 0 | 0 | 0 | Условие платежа (A000, 0012, 0004) |
| purchasing_org | String |  | 0 | 0 | 0 | Закупочная организация (UT11) |
| sap_header_currency | String |  | 0 | 0 | 0 | Валюта из заголовка |
| incoterms | String |  | 0 | 0 | 0 | Инкотермс (DDP, EXW, FCA, DAP) |
| incoterms_loc | String |  | 0 | 0 | 0 | Инкотермс место (UTA VKO SO) |
| processing_status | String |  | 0 | 0 | 0 | Статус обработки (02) |
| contract_internal | String |  | 0 | 0 | 0 | Внутренний номер договора |
| external_order_no | String |  | 0 | 0 | 0 | Внешний номер заказа / комментарий |
| execution_place | String |  | 0 | 0 | 0 | Место исполнения контракта (01, 02) |
| sap_pos_found | UInt8 |  | 0 | 0 | 0 | Найдена ли позиция в SAP (0/1) |
| sap_last_change | Nullable(Date) |  | 0 | 0 | 0 | Дата последнего изменения |
| gross_value | Nullable(Float64) |  | 0 | 0 | 0 | Стоимость брутто |
| tax_code | String |  | 0 | 0 | 0 | Код налога (M3, M4, M0, MU) |
| actual_cost | Nullable(Float64) |  | 0 | 0 | 0 | Фактическая стоимость |
| goods_receipt | String |  | 0 | 0 | 0 | Поступление материала (X = да) |
| invoice_receipt | String |  | 0 | 0 | 0 | Поступление счета (X = да) |
| manufacturer_part | String |  | 0 | 0 | 0 | Номер детали производителя |
| sap_pos_doc_status | String |  | 0 | 0 | 0 | Статус позиции документа |
| full_delivery_flag | String |  | 0 | 0 | 0 | Полная поставка |
| aircraft_type | String |  | 0 | 0 | 0 | Тип воздушного судна |
| aircraft | String |  | 0 | 0 | 0 | Воздушное судно |
| amos_cost_center | String |  | 0 | 0 | 0 | AMOS Cost Center (RETURN, ПЕРЕСОРТ) |
| info_record | String |  | 0 | 0 | 0 | Инфо-запись закупки (из SAP v2) |
| reinvoice_contract | String |  | 0 | 0 | 0 | Договор перевыставки |
| log_found | UInt8 |  | 0 | 0 | 0 | Найдена ли запись в логе (0/1) |
| log_status | String |  | 0 | 0 | 0 | Статус в логе (@5B@=ок, @5C@=ошибка, @5D@=пересорт) |
| log_retry | String |  | 0 | 0 | 0 | Повтор в логе (Истинно/Ложно) |
| log_sap_order | String |  | 0 | 0 | 0 | Номер SAP заказа из лога |
| log_message | String |  | 0 | 0 | 0 | Текст сообщения из лога |
| log_error_date | Nullable(Date) |  | 0 | 0 | 0 | Дата ошибки |
| load_time | DateTime | now() | 0 | 0 | 0 | Время загрузки записи |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.mm_orders (`key_sap` String COMMENT 'Составной ключ SAP: документ+позиция (450317838810)', `key_amos` String COMMENT 'Составной ключ AMOS: заказ+позиция (P165744253)', `sap_doc` String COMMENT 'Номер документа закупки SAP (4503178388)', `sap_pos` UInt16 COMMENT 'Позиция в документе SAP (10,20,30)', `amos_order` String COMMENT 'Номер заказа в AMOS (P16574425)', `amos_pos` UInt16 COMMENT 'Позиция в заказе AMOS (1,2,3)', `analysis` String COMMENT 'Результат анализа: 01 актуально, удален в AMOS, Возврат брака...', `draft_flag` String COMMENT 'Признак драфта в SAP (C)', `return_flag` String COMMENT 'Признак возврата брака (L)', `final_delivery_nd` String COMMENT 'Конечная поставка', `deletion_flag` String COMMENT 'Индикатор удаления', `amos_pos_status` String COMMENT 'Статус позиции в AMOS (O=open, R=received, CA=cancelled)', `amos_contract` String COMMENT 'Договор в AMOS (19/23ПА, 49/24ПА...)', `purch_doc_type` String COMMENT 'Вид документа закупки (NB)', `doc_type` String COMMENT 'Тип документа (F)', `purch_group` String COMMENT 'Группа закупок (U17, U60)', `acct_assign_type` String COMMENT 'Тип контировки (L)', `po_history` String COMMENT 'История заказа / ДокумПоОтзыв', `toro_order` String COMMENT 'Заказ ТОРО', `doc_date` Nullable(Date) COMMENT 'Дата документа', `target_date` Nullable(Date) COMMENT 'Целевая дата поставки (Targetdate)', `confirmed_date` Nullable(Date) COMMENT 'Подтвержденная дата (confDate)', `earliest_delivery` Nullable(Date) COMMENT 'Самая ранняя дата поставки', `vendor_code` String COMMENT 'Код поставщика SAP (1009236)', `vendor_name` String COMMENT 'Наименование поставщика (ООО ФТГ)', `material` String COMMENT 'Номер материала SAP (29112)', `short_text` String COMMENT 'Краткий текст: партномер_описание', `plant` String COMMENT 'Завод (1101, 1109, 1110)', `storage_loc` String COMMENT 'Склад', `order_qty` Nullable(Float64) COMMENT 'Количество заказа', `order_unit` String COMMENT 'Единица измерения (ШТ, КГ, М, М2)', `net_price` Nullable(Float64) COMMENT 'Цена нетто', `currency` String COMMENT 'Валюта (RUB, USDUE, CNY)', `price_unit` Nullable(Float64) COMMENT 'Единица цены', `target_qty` Nullable(Float64) COMMENT 'Целевое количество', `open_contract_qty` Nullable(Float64) COMMENT 'Открытое договорное количество', `still_deliver_qty` Nullable(Float64) COMMENT 'Ещё поставить (количество)', `still_deliver_val` Nullable(Float64) COMMENT 'Ещё для поставки (стоимость)', `to_invoice_qty` Nullable(Float64) COMMENT 'Для фактурирования (количество)', `to_invoice_val` Nullable(Float64) COMMENT 'Для фактурирования (стоимость)', `positions_count` UInt16 COMMENT 'Число позиций в заказе', `amos_source` String COMMENT 'Источник: open / additional / none', `amos_order_date` Nullable(Date) COMMENT 'Дата заказа в AMOS', `amos_vendor` String COMMENT 'Краткое имя поставщика из AMOS (ЮНИТ, XIAMEN)', `amos_partno` String COMMENT 'Номер детали из AMOS', `amos_project` String COMMENT 'Проект/договор из AMOS (19/23ПА)', `amos_ext_state` String COMMENT 'Внешний статус AMOS (O, PR, R, CA, B)', `amos_state` String COMMENT 'Внутренний статус AMOS', `amos_order_status` String COMMENT 'Числовой статус заказа AMOS (0)', `amos_qty` Nullable(Float64) COMMENT 'Количество из AMOS', `amos_amount` Nullable(Float64) COMMENT 'Сумма из AMOS (в рублях)', `amos_cost_type` String COMMENT 'Тип НДС: БЕЗ НДС / НДС 20% / НДС 5% / НДС 0%', `amos_sap_order` String COMMENT 'Номер заказа SAP из AMOS (основной)', `amos_sap_order2` String COMMENT 'Номер заказа SAP из AMOS (дополнительный)', `amos_retry` String COMMENT 'Признак повтора (Истинно)', `amos_comment` String COMMENT 'Комментарий из AMOS', `sap_header_found` UInt8 COMMENT 'Найден ли заголовок в SAP (0/1)', `sap_created_date` Nullable(Date) COMMENT 'Дата создания в SAP', `sap_created_by` String COMMENT 'Кто создал (PO_PROXY, OHOTKIN_VI)', `payment_terms` String COMMENT 'Условие платежа (A000, 0012, 0004)', `purchasing_org` String COMMENT 'Закупочная организация (UT11)', `sap_header_currency` String COMMENT 'Валюта из заголовка', `incoterms` String COMMENT 'Инкотермс (DDP, EXW, FCA, DAP)', `incoterms_loc` String COMMENT 'Инкотермс место (UTA VKO SO)', `processing_status` String COMMENT 'Статус обработки (02)', `contract_internal` String COMMENT 'Внутренний номер договора', `external_order_no` String COMMENT 'Внешний номер заказа / комментарий', `execution_place` String COMMENT 'Место исполнения контракта (01, 02)', `sap_pos_found` UInt8 COMMENT 'Найдена ли позиция в SAP (0/1)', `sap_last_change` Nullable(Date) COMMENT 'Дата последнего изменения', `gross_value` Nullable(Float64) COMMENT 'Стоимость брутто', `tax_code` String COMMENT 'Код налога (M3, M4, M0, MU)', `actual_cost` Nullable(Float64) COMMENT 'Фактическая стоимость', `goods_receipt` String COMMENT 'Поступление материала (X = да)', `invoice_receipt` String COMMENT 'Поступление счета (X = да)', `manufacturer_part` String COMMENT 'Номер детали производителя', `sap_pos_doc_status` String COMMENT 'Статус позиции документа', `full_delivery_flag` String COMMENT 'Полная поставка', `aircraft_type` String COMMENT 'Тип воздушного судна', `aircraft` String COMMENT 'Воздушное судно', `amos_cost_center` String COMMENT 'AMOS Cost Center (RETURN, ПЕРЕСОРТ)', `info_record` String COMMENT 'Инфо-запись закупки (из SAP v2)', `reinvoice_contract` String COMMENT 'Договор перевыставки', `log_found` UInt8 COMMENT 'Найдена ли запись в логе (0/1)', `log_status` String COMMENT 'Статус в логе (@5B@=ок, @5C@=ошибка, @5D@=пересорт)', `log_retry` String COMMENT 'Повтор в логе (Истинно/Ложно)', `log_sap_order` String COMMENT 'Номер SAP заказа из лога', `log_message` String COMMENT 'Текст сообщения из лога', `log_error_date` Nullable(Date) COMMENT 'Дата ошибки', `load_time` DateTime DEFAULT now() COMMENT 'Время загрузки записи') ENGINE = MergeTree ORDER BY (amos_order, amos_pos, sap_doc, sap_pos) SETTINGS index_granularity = 256
```
</details>

### 1.42 `monthly_flight_hours`
- Engine: MergeTree
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: 2
- Total bytes: 89
- `count()`: 2 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| monthly_flight_hours | Nullable(Float64) |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.monthly_flight_hours (`monthly_flight_hours` Nullable(Float64)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192
```
</details>

### 1.43 `owner_dict_flat`
- Engine: Dictionary
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None
- version/`count()`: **ошибка запроса** — `ServerException`: Code: 102. DB::Exception: Unexpected packet from server 10.95.19.132:8123 (expected Hello or Exception, got Unknown packet): (10.95.19.132:8123, local address: 172.18.0.2:37510): While executing Remot

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| address_i | UInt64 |  | 0 | 0 | 0 |  |
| owner | String |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE DICTIONARY default.owner_dict_flat (`address_i` UInt32, `owner` String) PRIMARY KEY address_i SOURCE(CLICKHOUSE(HOST '10.95.19.132' PORT 8123 TABLE 'dict_owner_flat' DB 'default')) LIFETIME(MIN 0 MAX 3600) LAYOUT(FLAT())
```
</details>

### 1.44 `partno_dict_flat`
- Engine: Dictionary
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None
- version/`count()`: **ошибка запроса** — `ServerException`: Code: 102. DB::Exception: Unexpected packet from server 10.95.19.132:8123 (expected Hello or Exception, got Unknown packet): (10.95.19.132:8123, local address: 172.18.0.2:37514): While executing Remot

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partseqno_i | UInt64 |  | 0 | 0 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE DICTIONARY default.partno_dict_flat (`partseqno_i` UInt32, `partno` String) PRIMARY KEY partseqno_i SOURCE(CLICKHOUSE(HOST '10.95.19.132' PORT 8123 TABLE 'dict_partno_flat' DB 'default')) LIFETIME(MIN 0 MAX 3600) LAYOUT(FLAT())
```
</details>

### 1.45 `pn_check_tasks`
- Engine: MergeTree
- Partition key: 
- Sorting key: model, taskcardno
- Primary key: model, taskcardno
- Total rows: 4955
- Total bytes: 39682
- `count()`: 4955 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| model | String |  | 1 | 1 | 0 |  |
| check_name | String |  | 0 | 0 | 0 |  |
| taskcardno | String |  | 1 | 1 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.pn_check_tasks (`model` String, `check_name` String, `taskcardno` String) ENGINE = MergeTree ORDER BY (model, taskcardno) SETTINGS index_granularity = 8192
```
</details>

### 1.46 `pn_forecast_by_part`
- Engine: MergeTree
- Partition key: 
- Sorting key: ac_type, part_no
- Primary key: ac_type, part_no
- Total rows: 3706
- Total bytes: 53546
- `count()`: 3706 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_type | String |  | 1 | 1 | 0 |  |
| part_no | String |  | 1 | 1 | 0 |  |
| mu | String |  | 0 | 0 | 0 |  |
| forecast_qty_total | Float64 |  | 0 | 0 | 0 |  |
| work_count | UInt32 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.pn_forecast_by_part (`ac_type` String, `part_no` String, `mu` String, `forecast_qty_total` Float64, `work_count` UInt32) ENGINE = MergeTree ORDER BY (ac_type, part_no) SETTINGS index_granularity = 8192
```
</details>

### 1.47 `pn_forecast_result`
- Engine: MergeTree
- Partition key: 
- Sorting key: ac_type, tc_no, part_no
- Primary key: ac_type, tc_no, part_no
- Total rows: 8971
- Total bytes: 172032
- `count()`: 8971 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_type | String |  | 1 | 1 | 0 |  |
| tc_no | String |  | 1 | 1 | 0 |  |
| part_no | String |  | 1 | 1 | 0 |  |
| description | String |  | 0 | 0 | 0 |  |
| ata | String |  | 0 | 0 | 0 |  |
| weight | Float64 |  | 0 | 0 | 0 |  |
| weight_denominator | String |  | 0 | 0 | 0 |  |
| avg_qty_per_occurrence | Float64 |  | 0 | 0 | 0 |  |
| finding_wo_with_part | UInt32 |  | 0 | 0 | 0 |  |
| total_finding_wo | UInt32 |  | 0 | 0 | 0 |  |
| total_tasks_executed | Nullable(UInt32) |  | 0 | 0 | 0 |  |
| num_tasks_forecast | UInt32 |  | 0 | 0 | 0 |  |
| forecast_qty | Float64 |  | 0 | 0 | 0 |  |
| mu | String |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.pn_forecast_result (`ac_type` String, `tc_no` String, `part_no` String, `description` String, `ata` String, `weight` Float64, `weight_denominator` String, `avg_qty_per_occurrence` Float64, `finding_wo_with_part` UInt32, `total_finding_wo` UInt32, `total_tasks_executed` Nullable(UInt32), `num_tasks_forecast` UInt32, `forecast_qty` Float64, `mu` String) ENGINE = MergeTree ORDER BY (ac_type, tc_no, part_no) SETTINGS index_granularity = 8192
```
</details>

### 1.48 `pn_materials_by_check`
- Engine: MergeTree
- Partition key: 
- Sorting key: model, check_name, part_no
- Primary key: model, check_name, part_no
- Total rows: 107
- Total bytes: 4307
- `count()`: 107 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| model | String |  | 1 | 1 | 0 |  |
| check_name | String |  | 1 | 1 | 0 |  |
| taskcardno | String |  | 0 | 0 | 0 |  |
| part_no | String |  | 1 | 1 | 0 |  |
| part_description | String |  | 0 | 0 | 0 |  |
| forecast_qty | Float64 |  | 0 | 0 | 0 |  |
| mu | String |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.pn_materials_by_check (`model` String, `check_name` String, `taskcardno` String, `part_no` String, `part_description` String, `forecast_qty` Float64, `mu` String) ENGINE = MergeTree ORDER BY (model, check_name, part_no) SETTINGS index_granularity = 8192
```
</details>

### 1.49 `pn_nrc_recommended_stock`
- Engine: MergeTree
- Partition key: 
- Sorting key: partno, measure_unit
- Primary key: partno, measure_unit
- Total rows: 18634
- Total bytes: 293926
- `count()`: 18634 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | String |  | 1 | 1 | 0 |  |
| part_description | String |  | 0 | 0 | 0 |  |
| measure_unit | String |  | 1 | 1 | 0 |  |
| expected_demand_per_year | Float64 |  | 0 | 0 | 0 |  |
| recommended_stock | UInt32 |  | 0 | 0 | 0 |  |
| months_cover | UInt32 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.pn_nrc_recommended_stock (`partno` String, `part_description` String, `measure_unit` String, `expected_demand_per_year` Float64, `recommended_stock` UInt32, `months_cover` UInt32) ENGINE = MergeTree ORDER BY (partno, measure_unit) SETTINGS index_granularity = 8192
```
</details>

### 1.50 `program_ac`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: ac_registr, ac_typ, version_date, version_id
- Primary key: ac_registr, ac_typ, version_date, version_id
- Total rows: 1033
- Total bytes: 29011
- Distinct `version_date`: 4 (период 2025-07-04 .. 2026-04-08) | `count()`: 695

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_registr | UInt32 |  | 1 | 1 | 0 |  |
| ac_typ | String |  | 1 | 1 | 0 |  |
| object_type | String |  | 0 | 0 | 0 |  |
| description | String |  | 0 | 0 | 0 |  |
| owner | String |  | 0 | 0 | 0 |  |
| operator | String |  | 0 | 0 | 0 |  |
| homebase | String |  | 0 | 0 | 0 |  |
| homebase_name | String |  | 0 | 0 | 0 |  |
| directorate | String |  | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.program_ac (`ac_registr` UInt32, `ac_typ` String, `object_type` String, `description` String, `owner` String, `operator` String, `homebase` String, `homebase_name` String, `directorate` String, `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (ac_registr, ac_typ, version_date, version_id) SETTINGS index_granularity = 8192
```
</details>

### 1.51 `serialno_dict_flat`
- Engine: Dictionary
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None
- version/`count()`: **ошибка запроса** — `ServerException`: Code: 102. DB::Exception: Unexpected packet from server 10.95.19.132:8123 (expected Hello or Exception, got Unknown packet): (10.95.19.132:8123, local address: 172.18.0.2:37526): While executing Remot

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| psn | UInt64 |  | 0 | 0 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| serialno | String |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE DICTIONARY default.serialno_dict_flat (`psn` UInt32, `partno` String, `serialno` String) PRIMARY KEY psn SOURCE(CLICKHOUSE(HOST '10.95.19.132' PORT 8123 TABLE 'dict_serialno_flat' DB 'default')) LIFETIME(MIN 0 MAX 3600) LAYOUT(FLAT())
```
</details>

### 1.52 `sim_master_v3`
- Engine: MergeTree
- Partition key: toYYYYMM(day_date)
- Sorting key: version_date, day_u16, idx
- Primary key: version_date, day_u16, idx
- Total rows: 2161884
- Total bytes: 14337302
- Distinct `version_date`: 2 (период 20273 .. 20452) | `count()`: 2161884
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=1189900, 2=971984

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 0 | 0 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)) | 0 | 0 | 1 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| partseqno | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| intent_state | UInt8 |  | 0 | 0 | 0 |  |
| bi_counter | UInt8 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| cso | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| assembly_time | UInt16 |  | 0 | 0 | 0 |  |
| partout_time | UInt16 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| s4_days | UInt16 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt8 |  | 0 | 0 | 0 |  |
| active_trigger | UInt8 |  | 0 | 0 | 0 |  |
| partout_trigger | UInt8 |  | 0 | 0 | 0 |  |
| mfg_date_days | UInt32 |  | 0 | 0 | 0 |  |
| dt | UInt32 |  | 0 | 0 | 0 |  |
| dn | UInt32 |  | 0 | 0 | 0 |  |
| quota_target_mi8 | UInt16 |  | 0 | 0 | 0 |  |
| quota_target_mi17 | UInt16 |  | 0 | 0 | 0 |  |
| quota_gap_mi8 | Int16 |  | 0 | 0 | 0 |  |
| quota_gap_mi17 | Int16 |  | 0 | 0 | 0 |  |
| repair_quota_load | UInt16 |  | 0 | 0 | 0 |  |
| repair_quota_full | UInt8 |  | 0 | 0 | 0 |  |
| quota_demount | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p1 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p2 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_6 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_3_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_4_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_7_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_7_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| export_timestamp | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_master_v3 (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)), `idx` UInt16, `aircraft_number` UInt32, `partseqno` UInt32, `group_by` UInt8, `state` String, `intent_state` UInt8, `bi_counter` UInt8, `sne` UInt32, `ppr` UInt32, `cso` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `repair_time` UInt16, `assembly_time` UInt16, `partout_time` UInt16, `repair_days` UInt16, `s4_days` UInt16, `assembly_trigger` UInt8, `active_trigger` UInt8, `partout_trigger` UInt8, `mfg_date_days` UInt32, `dt` UInt32, `dn` UInt32, `quota_target_mi8` UInt16, `quota_target_mi17` UInt16, `quota_gap_mi8` Int16, `quota_gap_mi17` Int16, `repair_quota_load` UInt16, `repair_quota_full` UInt8, `quota_demount` UInt8, `quota_promote_p1` UInt8, `quota_promote_p2` UInt8, `quota_promote_p3` UInt8, `transition_0_to_2` UInt8, `transition_0_to_3` UInt8, `transition_2_to_4` UInt8, `transition_2_to_6` UInt8, `transition_2_to_3` UInt8, `transition_3_to_2` UInt8, `transition_1_to_4` UInt8, `transition_4_to_2` UInt8, `transition_7_to_4` UInt8, `transition_7_to_2` UInt8, `export_timestamp` DateTime DEFAULT now(), INDEX idx_version (version_date, version_id) TYPE minmax GRANULARITY 1, INDEX idx_day day_u16 TYPE minmax GRANULARITY 1, INDEX idx_state state TYPE bloom_filter GRANULARITY 1, INDEX idx_group_by group_by TYPE minmax GRANULARITY 1) ENGINE = MergeTree PARTITION BY toYYYYMM(day_date) ORDER BY (version_date, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.53 `sim_masterv2`
- Engine: MergeTree
- Partition key: toYYYYMM(day_date)
- Sorting key: version_date, day_u16, idx
- Primary key: version_date, day_u16, idx
- Total rows: 1194035
- Total bytes: 6830352
- Distinct `version_date`: 1 (период 20273 .. 20273) | `count()`: 1194035
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=594950, 2=599085

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 0 | 0 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)) | 0 | 0 | 1 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| partseqno | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| intent_state | UInt8 |  | 0 | 0 | 0 |  |
| bi_counter | UInt8 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| cso | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| assembly_time | UInt16 |  | 0 | 0 | 0 |  |
| partout_time | UInt16 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| s4_days | UInt16 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt8 |  | 0 | 0 | 0 |  |
| active_trigger | UInt8 |  | 0 | 0 | 0 |  |
| partout_trigger | UInt8 |  | 0 | 0 | 0 |  |
| mfg_date_days | UInt32 |  | 0 | 0 | 0 |  |
| dt | UInt32 |  | 0 | 0 | 0 |  |
| dn | UInt32 |  | 0 | 0 | 0 |  |
| quota_target_mi8 | UInt16 |  | 0 | 0 | 0 |  |
| quota_target_mi17 | UInt16 |  | 0 | 0 | 0 |  |
| quota_gap_mi8 | Int16 |  | 0 | 0 | 0 |  |
| quota_gap_mi17 | Int16 |  | 0 | 0 | 0 |  |
| repair_quota_load | UInt16 |  | 0 | 0 | 0 |  |
| repair_quota_full | UInt8 |  | 0 | 0 | 0 |  |
| quota_demount | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p1 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p2 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_6 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_7 | UInt8 |  | 0 | 0 | 0 |  |
| transition_3_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_4_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_7_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_7_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| export_timestamp | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2 (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)), `idx` UInt16, `aircraft_number` UInt32, `partseqno` UInt32, `group_by` UInt8, `state` String, `intent_state` UInt8, `bi_counter` UInt8, `sne` UInt32, `ppr` UInt32, `cso` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `repair_time` UInt16, `assembly_time` UInt16, `partout_time` UInt16, `repair_days` UInt16, `s4_days` UInt16, `assembly_trigger` UInt8, `active_trigger` UInt8, `partout_trigger` UInt8, `mfg_date_days` UInt32, `dt` UInt32, `dn` UInt32, `quota_target_mi8` UInt16, `quota_target_mi17` UInt16, `quota_gap_mi8` Int16, `quota_gap_mi17` Int16, `repair_quota_load` UInt16, `repair_quota_full` UInt8, `quota_demount` UInt8, `quota_promote_p1` UInt8, `quota_promote_p2` UInt8, `quota_promote_p3` UInt8, `transition_0_to_2` UInt8, `transition_0_to_3` UInt8, `transition_2_to_4` UInt8, `transition_2_to_6` UInt8, `transition_2_to_3` UInt8, `transition_2_to_7` UInt8, `transition_3_to_2` UInt8, `transition_1_to_4` UInt8, `transition_4_to_3` UInt8, `transition_7_to_4` UInt8, `transition_7_to_2` UInt8, `export_timestamp` DateTime DEFAULT now(), INDEX idx_version (version_date, version_id) TYPE minmax GRANULARITY 1, INDEX idx_day day_u16 TYPE minmax GRANULARITY 1, INDEX idx_state state TYPE bloom_filter GRANULARITY 1, INDEX idx_group_by group_by TYPE minmax GRANULARITY 1) ENGINE = MergeTree PARTITION BY toYYYYMM(day_date) ORDER BY (version_date, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.54 `sim_masterv2_adaptive`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_date, day_u16, idx
- Primary key: version_date, day_u16, idx
- Total rows: 1018629
- Total bytes: 773739
- Distinct `version_date`: 1 (период 20250704 .. 20250704) | `count()`: 1018629
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=595113, 2=423516

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt16 |  | 0 | 0 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| dt | UInt16 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| mfg_date | UInt16 |  | 0 | 0 | 0 |  |
| intent_state | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_2 | UInt8 | 0 | 0 | 0 | 0 |  |
| transition_2_to_3 | UInt8 | 0 | 0 | 0 | 0 |  |
| transition_2_to_4 | UInt8 | 0 | 0 | 0 | 0 |  |
| transition_3_to_2 | UInt8 | 0 | 0 | 0 | 0 |  |
| transition_4_to_5 | UInt8 | 0 | 0 | 0 | 0 |  |
| transition_5_to_4 | UInt8 | 0 | 0 | 0 | 0 |  |
| transition_1_to_5 | UInt8 | 0 | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_adaptive (`version_date` UInt32, `version_id` UInt16, `day_u16` UInt16, `idx` UInt16, `aircraft_number` UInt32, `group_by` UInt8, `state` String, `dt` UInt16, `sne` UInt32, `ppr` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `repair_days` UInt16, `repair_time` UInt16, `mfg_date` UInt16, `intent_state` UInt8, `transition_1_to_2` UInt8 DEFAULT 0, `transition_2_to_3` UInt8 DEFAULT 0, `transition_2_to_4` UInt8 DEFAULT 0, `transition_3_to_2` UInt8 DEFAULT 0, `transition_4_to_5` UInt8 DEFAULT 0, `transition_5_to_4` UInt8 DEFAULT 0, `transition_1_to_5` UInt8 DEFAULT 0) ENGINE = MergeTree ORDER BY (version_date, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.55 `sim_masterv2_adaptive20`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_date, day_u16, idx
- Primary key: version_date, day_u16, idx
- Total rows: 279
- Total bytes: 5172
- Distinct `version_date`: 1 (период 2025-07-04 .. 2025-07-04) | `count()`: 279
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=163, 2=116

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | Date |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| status_id | UInt8 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| timestamp | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_adaptive20 (`version_date` Date, `day_u16` UInt16, `idx` UInt16, `aircraft_number` UInt32, `sne` UInt32, `ppr` UInt32, `status_id` UInt8, `group_by` UInt8, `timestamp` DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY (version_date, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.56 `sim_masterv2_adaptive_v3`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_date, day_u16, idx
- Primary key: version_date, day_u16, idx
- Total rows: 89838
- Total bytes: 460956
- Distinct `version_date`: 1 (период 20273 .. 20273) | `count()`: 89838
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=52486, 2=37352

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 0 | 0 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| dt | UInt16 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| adaptive_days | UInt16 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_adaptive_v3 (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `idx` UInt16, `aircraft_number` UInt32, `group_by` UInt8, `state` String, `dt` UInt16, `sne` UInt32, `ppr` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `repair_days` UInt16, `repair_time` UInt16, `adaptive_days` UInt16) ENGINE = MergeTree ORDER BY (version_date, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.57 `sim_masterv2_baseline_sim`
- Engine: MergeTree
- Partition key: toYYYYMM(day_date)
- Sorting key: version_date, version_id, day_u16, idx
- Primary key: version_date, version_id, day_u16, idx
- Total rows: 0
- Total bytes: 0
- Distinct `version_date`: 0 (период 0 .. 0) | `count()`: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)) | 0 | 0 | 1 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| partseqno | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| intent_state | UInt8 |  | 0 | 0 | 0 |  |
| bi_counter | UInt8 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| cso | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| assembly_time | UInt16 |  | 0 | 0 | 0 |  |
| partout_time | UInt16 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| s4_days | UInt16 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt8 |  | 0 | 0 | 0 |  |
| active_trigger | UInt8 |  | 0 | 0 | 0 |  |
| partout_trigger | UInt8 |  | 0 | 0 | 0 |  |
| mfg_date_days | UInt32 |  | 0 | 0 | 0 |  |
| dt | UInt32 |  | 0 | 0 | 0 |  |
| dn | UInt32 |  | 0 | 0 | 0 |  |
| quota_target_mi8 | UInt16 |  | 0 | 0 | 0 |  |
| quota_target_mi17 | UInt16 |  | 0 | 0 | 0 |  |
| quota_gap_mi8 | Int16 |  | 0 | 0 | 0 |  |
| quota_gap_mi17 | Int16 |  | 0 | 0 | 0 |  |
| quota_demount | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p1 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p2 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_6 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_3_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_5_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_4_to_5 | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_4_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| export_timestamp | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_baseline_sim (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)), `idx` UInt16, `aircraft_number` UInt32, `partseqno` UInt32, `group_by` UInt8, `state` String, `intent_state` UInt8, `bi_counter` UInt8, `sne` UInt32, `ppr` UInt32, `cso` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `repair_time` UInt16, `assembly_time` UInt16, `partout_time` UInt16, `repair_days` UInt16, `s4_days` UInt16, `assembly_trigger` UInt8, `active_trigger` UInt8, `partout_trigger` UInt8, `mfg_date_days` UInt32, `dt` UInt32, `dn` UInt32, `quota_target_mi8` UInt16, `quota_target_mi17` UInt16, `quota_gap_mi8` Int16, `quota_gap_mi17` Int16, `quota_demount` UInt8, `quota_promote_p1` UInt8, `quota_promote_p2` UInt8, `quota_promote_p3` UInt8, `transition_0_to_2` UInt8, `transition_0_to_3` UInt8, `transition_2_to_4` UInt8, `transition_2_to_6` UInt8, `transition_2_to_3` UInt8, `transition_3_to_2` UInt8, `transition_5_to_2` UInt8, `transition_1_to_2` UInt8, `transition_4_to_5` UInt8, `transition_1_to_4` UInt8, `transition_4_to_2` UInt8, `export_timestamp` DateTime DEFAULT now(), INDEX idx_version (version_date, version_id) TYPE minmax GRANULARITY 1, INDEX idx_day day_u16 TYPE minmax GRANULARITY 1, INDEX idx_state state TYPE bloom_filter GRANULARITY 1, INDEX idx_group_by group_by TYPE minmax GRANULARITY 1) ENGINE = MergeTree PARTITION BY toYYYYMM(day_date) ORDER BY (version_date, version_id, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.58 `sim_masterv2_bench_short`
- Engine: MergeTree
- Partition key: toYYYYMM(day_date)
- Sorting key: version_date, day_u16, idx
- Primary key: version_date, day_u16, idx
- Total rows: 174090
- Total bytes: 2399746
- Distinct `version_date`: 1 (период 20273 .. 20273) | `count()`: 174090
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=94051, 2=80039

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 0 | 0 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)) | 0 | 0 | 1 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| partseqno | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| intent_state | UInt8 |  | 0 | 0 | 0 |  |
| bi_counter | UInt8 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| cso | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| assembly_time | UInt16 |  | 0 | 0 | 0 |  |
| partout_time | UInt16 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| s4_days | UInt16 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt8 |  | 0 | 0 | 0 |  |
| active_trigger | UInt8 |  | 0 | 0 | 0 |  |
| partout_trigger | UInt8 |  | 0 | 0 | 0 |  |
| mfg_date_days | UInt32 |  | 0 | 0 | 0 |  |
| dt | UInt32 |  | 0 | 0 | 0 |  |
| dn | UInt32 |  | 0 | 0 | 0 |  |
| quota_target_mi8 | UInt16 |  | 0 | 0 | 0 |  |
| quota_target_mi17 | UInt16 |  | 0 | 0 | 0 |  |
| quota_gap_mi8 | Int16 |  | 0 | 0 | 0 |  |
| quota_gap_mi17 | Int16 |  | 0 | 0 | 0 |  |
| repair_quota_load | UInt16 |  | 0 | 0 | 0 |  |
| repair_quota_full | UInt8 |  | 0 | 0 | 0 |  |
| quota_demount | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p1 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p2 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_6 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_3_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_5_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_4_to_5 | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_4_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| export_timestamp | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_bench_short (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)), `idx` UInt16, `aircraft_number` UInt32, `partseqno` UInt32, `group_by` UInt8, `state` String, `intent_state` UInt8, `bi_counter` UInt8, `sne` UInt32, `ppr` UInt32, `cso` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `repair_time` UInt16, `assembly_time` UInt16, `partout_time` UInt16, `repair_days` UInt16, `s4_days` UInt16, `assembly_trigger` UInt8, `active_trigger` UInt8, `partout_trigger` UInt8, `mfg_date_days` UInt32, `dt` UInt32, `dn` UInt32, `quota_target_mi8` UInt16, `quota_target_mi17` UInt16, `quota_gap_mi8` Int16, `quota_gap_mi17` Int16, `repair_quota_load` UInt16, `repair_quota_full` UInt8, `quota_demount` UInt8, `quota_promote_p1` UInt8, `quota_promote_p2` UInt8, `quota_promote_p3` UInt8, `transition_0_to_2` UInt8, `transition_0_to_3` UInt8, `transition_2_to_4` UInt8, `transition_2_to_6` UInt8, `transition_2_to_3` UInt8, `transition_3_to_2` UInt8, `transition_5_to_2` UInt8, `transition_1_to_2` UInt8, `transition_4_to_5` UInt8, `transition_1_to_4` UInt8, `transition_4_to_2` UInt8, `export_timestamp` DateTime DEFAULT now(), INDEX idx_version (version_date, version_id) TYPE minmax GRANULARITY 1, INDEX idx_day day_u16 TYPE minmax GRANULARITY 1, INDEX idx_state state TYPE bloom_filter GRANULARITY 1, INDEX idx_group_by group_by TYPE minmax GRANULARITY 1) ENGINE = MergeTree PARTITION BY toYYYYMM(day_date) ORDER BY (version_date, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.59 `sim_masterv2_daily`
- Engine: MergeTree
- Partition key: toYYYYMM(day_date)
- Sorting key: version_date, version_id, day_u16, idx
- Primary key: version_date, version_id, day_u16, idx
- Total rows: 0
- Total bytes: 0
- Distinct `version_date`: 0 (период 0 .. 0) | `count()`: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)) | 0 | 0 | 1 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| partseqno | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| intent_state | UInt8 |  | 0 | 0 | 0 |  |
| bi_counter | UInt8 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| cso | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| assembly_time | UInt16 |  | 0 | 0 | 0 |  |
| partout_time | UInt16 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| s4_days | UInt16 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt8 |  | 0 | 0 | 0 |  |
| active_trigger | UInt8 |  | 0 | 0 | 0 |  |
| partout_trigger | UInt8 |  | 0 | 0 | 0 |  |
| mfg_date_days | UInt32 |  | 0 | 0 | 0 |  |
| dt | UInt32 |  | 0 | 0 | 0 |  |
| dn | UInt32 |  | 0 | 0 | 0 |  |
| quota_target_mi8 | UInt16 |  | 0 | 0 | 0 |  |
| quota_target_mi17 | UInt16 |  | 0 | 0 | 0 |  |
| quota_gap_mi8 | Int16 |  | 0 | 0 | 0 |  |
| quota_gap_mi17 | Int16 |  | 0 | 0 | 0 |  |
| quota_demount | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p1 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p2 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_6 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_3_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_5_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_4_to_5 | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_4_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| export_timestamp | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_daily (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)), `idx` UInt16, `aircraft_number` UInt32, `partseqno` UInt32, `group_by` UInt8, `state` String, `intent_state` UInt8, `bi_counter` UInt8, `sne` UInt32, `ppr` UInt32, `cso` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `repair_time` UInt16, `assembly_time` UInt16, `partout_time` UInt16, `repair_days` UInt16, `s4_days` UInt16, `assembly_trigger` UInt8, `active_trigger` UInt8, `partout_trigger` UInt8, `mfg_date_days` UInt32, `dt` UInt32, `dn` UInt32, `quota_target_mi8` UInt16, `quota_target_mi17` UInt16, `quota_gap_mi8` Int16, `quota_gap_mi17` Int16, `quota_demount` UInt8, `quota_promote_p1` UInt8, `quota_promote_p2` UInt8, `quota_promote_p3` UInt8, `transition_0_to_2` UInt8, `transition_0_to_3` UInt8, `transition_2_to_4` UInt8, `transition_2_to_6` UInt8, `transition_2_to_3` UInt8, `transition_3_to_2` UInt8, `transition_5_to_2` UInt8, `transition_1_to_2` UInt8, `transition_4_to_5` UInt8, `transition_1_to_4` UInt8, `transition_4_to_2` UInt8, `export_timestamp` DateTime DEFAULT now(), INDEX idx_version (version_date, version_id) TYPE minmax GRANULARITY 1, INDEX idx_day day_u16 TYPE minmax GRANULARITY 1, INDEX idx_state state TYPE bloom_filter GRANULARITY 1, INDEX idx_group_by group_by TYPE minmax GRANULARITY 1) ENGINE = MergeTree PARTITION BY toYYYYMM(day_date) ORDER BY (version_date, version_id, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.60 `sim_masterv2_limiter`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_date, day_u16, idx
- Primary key: version_date, day_u16, idx
- Total rows: 0
- Total bytes: 0
- Distinct `version_date`: 0 (период 0 .. 0) | `count()`: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt8 |  | 0 | 0 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| dt | UInt32 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| mfg_date | UInt32 |  | 0 | 0 | 0 |  |
| intent_state | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_limiter (`version_date` UInt32, `version_id` UInt8, `day_u16` UInt16, `idx` UInt16, `aircraft_number` UInt32, `group_by` UInt8, `state` String, `dt` UInt32, `sne` UInt32, `ppr` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `repair_days` UInt16, `repair_time` UInt16, `mfg_date` UInt32, `intent_state` UInt8) ENGINE = MergeTree ORDER BY (version_date, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.61 `sim_masterv2_msg`
- Engine: MergeTree
- Partition key: toYYYYMM(day_date)
- Sorting key: version_date, day_u16, idx
- Primary key: version_date, day_u16, idx
- Total rows: 1039632
- Total bytes: 5924277
- Distinct `version_date`: 1 (период 20273 .. 20273) | `count()`: 1039632
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=594950, 2=444682

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 0 | 0 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)) | 0 | 0 | 1 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| partseqno | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| intent_state | UInt8 |  | 0 | 0 | 0 |  |
| bi_counter | UInt8 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| cso | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| assembly_time | UInt16 |  | 0 | 0 | 0 |  |
| partout_time | UInt16 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| s4_days | UInt16 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt8 |  | 0 | 0 | 0 |  |
| active_trigger | UInt8 |  | 0 | 0 | 0 |  |
| partout_trigger | UInt8 |  | 0 | 0 | 0 |  |
| mfg_date_days | UInt32 |  | 0 | 0 | 0 |  |
| dt | UInt32 |  | 0 | 0 | 0 |  |
| dn | UInt32 |  | 0 | 0 | 0 |  |
| quota_target_mi8 | UInt16 |  | 0 | 0 | 0 |  |
| quota_target_mi17 | UInt16 |  | 0 | 0 | 0 |  |
| quota_gap_mi8 | Int16 |  | 0 | 0 | 0 |  |
| quota_gap_mi17 | Int16 |  | 0 | 0 | 0 |  |
| quota_demount | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p1 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p2 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_6 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_3_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_5_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_4_to_5 | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_4_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| export_timestamp | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_msg (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)), `idx` UInt16, `aircraft_number` UInt32, `partseqno` UInt32, `group_by` UInt8, `state` String, `intent_state` UInt8, `bi_counter` UInt8, `sne` UInt32, `ppr` UInt32, `cso` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `repair_time` UInt16, `assembly_time` UInt16, `partout_time` UInt16, `repair_days` UInt16, `s4_days` UInt16, `assembly_trigger` UInt8, `active_trigger` UInt8, `partout_trigger` UInt8, `mfg_date_days` UInt32, `dt` UInt32, `dn` UInt32, `quota_target_mi8` UInt16, `quota_target_mi17` UInt16, `quota_gap_mi8` Int16, `quota_gap_mi17` Int16, `quota_demount` UInt8, `quota_promote_p1` UInt8, `quota_promote_p2` UInt8, `quota_promote_p3` UInt8, `transition_0_to_2` UInt8, `transition_0_to_3` UInt8, `transition_2_to_4` UInt8, `transition_2_to_6` UInt8, `transition_2_to_3` UInt8, `transition_3_to_2` UInt8, `transition_5_to_2` UInt8, `transition_1_to_2` UInt8, `transition_4_to_5` UInt8, `transition_1_to_4` UInt8, `transition_4_to_2` UInt8, `export_timestamp` DateTime DEFAULT now(), INDEX idx_version (version_date, version_id) TYPE minmax GRANULARITY 1, INDEX idx_day day_u16 TYPE minmax GRANULARITY 1, INDEX idx_state state TYPE bloom_filter GRANULARITY 1, INDEX idx_group_by group_by TYPE minmax GRANULARITY 1) ENGINE = MergeTree PARTITION BY toYYYYMM(day_date) ORDER BY (version_date, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.62 `sim_masterv2_repair_lines`
- Engine: MergeTree
- Partition key: toYYYYMM(day_date)
- Sorting key: version_date, day_u16, line_id
- Primary key: version_date, day_u16, line_id
- Total rows: 1543248
- Total bytes: 2582436
- Distinct `version_date`: 2 (период 20273 .. 20452) | `count()`: 1543248

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 0 | 0 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)) | 0 | 0 | 1 |  |
| line_id | UInt16 |  | 1 | 1 | 0 |  |
| free_days | UInt32 |  | 0 | 0 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| export_timestamp | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_repair_lines (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)), `line_id` UInt16, `free_days` UInt32, `aircraft_number` UInt32, `export_timestamp` DateTime DEFAULT now(), INDEX idx_version (version_date, version_id) TYPE minmax GRANULARITY 1, INDEX idx_day day_u16 TYPE minmax GRANULARITY 1) ENGINE = MergeTree PARTITION BY toYYYYMM(day_date) ORDER BY (version_date, day_u16, line_id) SETTINGS index_granularity = 8192
```
</details>

### 1.63 `sim_masterv2_short`
- Engine: MergeTree
- Partition key: toYYYYMM(day_date)
- Sorting key: version_date, day_u16, idx
- Primary key: version_date, day_u16, idx
- Total rows: 223458
- Total bytes: 3919595
- Distinct `version_date`: 2 (период 20273 .. 20452) | `count()`: 223458
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=123228, 2=100230

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 0 | 0 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)) | 0 | 0 | 1 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| partseqno | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| intent_state | UInt8 |  | 0 | 0 | 0 |  |
| bi_counter | UInt8 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| cso | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| assembly_time | UInt16 |  | 0 | 0 | 0 |  |
| partout_time | UInt16 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| s4_days | UInt16 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt8 |  | 0 | 0 | 0 |  |
| active_trigger | UInt8 |  | 0 | 0 | 0 |  |
| partout_trigger | UInt8 |  | 0 | 0 | 0 |  |
| mfg_date_days | UInt32 |  | 0 | 0 | 0 |  |
| dt | UInt32 |  | 0 | 0 | 0 |  |
| dn | UInt32 |  | 0 | 0 | 0 |  |
| quota_target_mi8 | UInt16 |  | 0 | 0 | 0 |  |
| quota_target_mi17 | UInt16 |  | 0 | 0 | 0 |  |
| quota_gap_mi8 | Int16 |  | 0 | 0 | 0 |  |
| quota_gap_mi17 | Int16 |  | 0 | 0 | 0 |  |
| repair_quota_load | UInt16 |  | 0 | 0 | 0 |  |
| repair_quota_full | UInt8 |  | 0 | 0 | 0 |  |
| quota_demount | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p1 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p2 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_6 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_3_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_4_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_7_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_7_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| export_timestamp | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_short (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)), `idx` UInt16, `aircraft_number` UInt32, `partseqno` UInt32, `group_by` UInt8, `state` String, `intent_state` UInt8, `bi_counter` UInt8, `sne` UInt32, `ppr` UInt32, `cso` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `repair_time` UInt16, `assembly_time` UInt16, `partout_time` UInt16, `repair_days` UInt16, `s4_days` UInt16, `assembly_trigger` UInt8, `active_trigger` UInt8, `partout_trigger` UInt8, `mfg_date_days` UInt32, `dt` UInt32, `dn` UInt32, `quota_target_mi8` UInt16, `quota_target_mi17` UInt16, `quota_gap_mi8` Int16, `quota_gap_mi17` Int16, `repair_quota_load` UInt16, `repair_quota_full` UInt8, `quota_demount` UInt8, `quota_promote_p1` UInt8, `quota_promote_p2` UInt8, `quota_promote_p3` UInt8, `transition_0_to_2` UInt8, `transition_0_to_3` UInt8, `transition_2_to_4` UInt8, `transition_2_to_6` UInt8, `transition_2_to_3` UInt8, `transition_3_to_2` UInt8, `transition_1_to_4` UInt8, `transition_4_to_2` UInt8, `transition_7_to_4` UInt8, `transition_7_to_2` UInt8, `export_timestamp` DateTime DEFAULT now(), INDEX idx_version (version_date, version_id) TYPE minmax GRANULARITY 1, INDEX idx_day day_u16 TYPE minmax GRANULARITY 1, INDEX idx_state state TYPE bloom_filter GRANULARITY 1, INDEX idx_group_by group_by TYPE minmax GRANULARITY 1) ENGINE = MergeTree PARTITION BY toYYYYMM(day_date) ORDER BY (version_date, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.64 `sim_masterv2_v7`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_date, version_id, day_u16, idx
- Primary key: version_date, version_id, day_u16, idx
- Total rows: 56777
- Total bytes: 328758
- Distinct `version_date`: 1 (период 20250704 .. 20250704) | `count()`: 56777
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=31459, 2=25318

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt8 |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_v7 (`version_date` UInt32, `version_id` UInt8, `day_u16` UInt16, `idx` UInt16, `aircraft_number` UInt32, `group_by` UInt8, `state` String, `sne` UInt32, `ppr` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `repair_days` UInt16, `repair_time` UInt16) ENGINE = MergeTree ORDER BY (version_date, version_id, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.65 `sim_masterv2_v8`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_date, version_id, day_u16, idx
- Primary key: version_date, version_id, day_u16, idx
- Total rows: 168741
- Total bytes: 1618291
- Distinct `version_date`: 2 (период 20250704 .. 20251230) | `count()`: 168741
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=91280, 2=77461

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt8 |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)) | 0 | 0 | 0 |  |
| debug_step | UInt32 |  | 0 | 0 | 0 |  |
| debug_prev_day | UInt32 |  | 0 | 0 | 0 |  |
| debug_adaptive_days | UInt32 |  | 0 | 0 | 0 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| daily_today_u32 | UInt32 |  | 0 | 0 | 0 |  |
| daily_next_u32 | UInt32 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| limiter | UInt16 |  | 0 | 0 | 0 |  |
| status_change_day | UInt32 |  | 0 | 0 | 0 |  |
| promoted | UInt8 |  | 0 | 0 | 0 |  |
| needs_demote | UInt8 |  | 0 | 0 | 0 |  |
| repair_candidate | UInt8 |  | 0 | 0 | 0 |  |
| repair_line_id | UInt32 |  | 0 | 0 | 0 |  |
| repair_line_day | UInt32 |  | 0 | 0 | 0 |  |
| debug_promoted | UInt8 |  | 0 | 0 | 0 |  |
| debug_needs_demote | UInt8 |  | 0 | 0 | 0 |  |
| debug_repair_candidate | UInt8 |  | 0 | 0 | 0 |  |
| debug_repair_line_id | UInt32 |  | 0 | 0 | 0 |  |
| debug_repair_line_day | UInt32 |  | 0 | 0 | 0 |  |
| debug_bucket_seen | UInt8 |  | 0 | 0 | 0 |  |
| commit_p1 | UInt32 |  | 0 | 0 | 0 |  |
| commit_p2 | UInt32 |  | 0 | 0 | 0 |  |
| commit_p3 | UInt32 |  | 0 | 0 | 0 |  |
| decision_p2 | UInt32 |  | 0 | 0 | 0 |  |
| decision_p3 | UInt32 |  | 0 | 0 | 0 |  |
| spawn_debug_curr_ops | UInt32 |  | 0 | 0 | 0 |  |
| spawn_debug_target | UInt32 |  | 0 | 0 | 0 |  |
| spawn_debug_need | UInt32 |  | 0 | 0 | 0 |  |
| spawn_debug_curr_ops_mi8 | UInt32 |  | 0 | 0 | 0 |  |
| spawn_debug_target_mi8 | UInt32 |  | 0 | 0 | 0 |  |
| spawn_debug_need_mi8 | UInt32 |  | 0 | 0 | 0 |  |
| debug_current_day | UInt32 |  | 0 | 0 | 0 |  |
| debug_rl_total | UInt32 |  | 0 | 0 | 0 |  |
| debug_rl_free | UInt32 |  | 0 | 0 | 0 |  |
| debug_rl_ready | UInt32 |  | 0 | 0 | 0 |  |
| debug_rl_min_free | UInt32 |  | 0 | 0 | 0 |  |
| debug_rl_max_free | UInt32 |  | 0 | 0 | 0 |  |
| debug_ops_mi17 | UInt32 |  | 0 | 0 | 0 |  |
| debug_svc_mi17 | UInt32 |  | 0 | 0 | 0 |  |
| debug_unsvc_ready_mi17 | UInt32 |  | 0 | 0 | 0 |  |
| debug_inactive_ready_mi17 | UInt32 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_v8 (`version_date` UInt32, `version_id` UInt8, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)), `debug_step` UInt32, `debug_prev_day` UInt32, `debug_adaptive_days` UInt32, `idx` UInt16, `aircraft_number` UInt32, `group_by` UInt8, `state` String, `sne` UInt32, `ppr` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `daily_today_u32` UInt32, `daily_next_u32` UInt32, `repair_days` UInt16, `repair_time` UInt16, `limiter` UInt16, `status_change_day` UInt32, `promoted` UInt8, `needs_demote` UInt8, `repair_candidate` UInt8, `repair_line_id` UInt32, `repair_line_day` UInt32, `debug_promoted` UInt8, `debug_needs_demote` UInt8, `debug_repair_candidate` UInt8, `debug_repair_line_id` UInt32, `debug_repair_line_day` UInt32, `debug_bucket_seen` UInt8, `commit_p1` UInt32, `commit_p2` UInt32, `commit_p3` UInt32, `decision_p2` UInt32, `decision_p3` UInt32, `spawn_debug_curr_ops` UInt32, `spawn_debug_target` UInt32, `spawn_debug_need` UInt32, `spawn_debug_curr_ops_mi8` UInt32, `spawn_debug_target_mi8` UInt32, `spawn_debug_need_mi8` UInt32, `debug_current_day` UInt32, `debug_rl_total` UInt32, `debug_rl_free` UInt32, `debug_rl_ready` UInt32, `debug_rl_min_free` UInt32, `debug_rl_max_free` UInt32, `debug_ops_mi17` UInt32, `debug_svc_mi17` UInt32, `debug_unsvc_ready_mi17` UInt32, `debug_inactive_ready_mi17` UInt32) ENGINE = MergeTree ORDER BY (version_date, version_id, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.66 `sim_masterv2_v9`
- Engine: MergeTree
- Partition key: (version_date, toYYYYMM(day_date))
- Sorting key: version_date, version_id, day_u16, idx
- Primary key: version_date, version_id, day_u16, idx
- Total rows: 346982
- Total bytes: 5777461
- Distinct `version_date`: 4 (период 20250704 .. 20260408) | `count()`: 346982
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=183538, 2=163444

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 1 |  |
| version_id | UInt32 |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate(toString(version_date)), toUInt16(day_u16)) | 0 | 0 | 1 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| status_id | UInt8 |  | 0 | 0 | 0 |  |
| pre_status_id | UInt8 |  | 0 | 0 | 0 |  |
| status_change_day | UInt16 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| limiter | UInt16 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| repair_claim_start_day | UInt16 |  | 0 | 0 | 0 |  |
| repair_claim_end_day | UInt16 |  | 0 | 0 | 0 |  |
| repair_claim_source | UInt8 |  | 0 | 0 | 0 |  |
| repair_claim_line_id | UInt16 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| assembly_time | UInt16 |  | 0 | 0 | 0 |  |
| active_trigger | UInt8 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt8 |  | 0 | 0 | 0 |  |
| daily_today_u32 | UInt32 |  | 0 | 0 | 0 |  |
| daily_next_u32 | UInt32 |  | 0 | 0 | 0 |  |
| commit_p2 | UInt32 |  | 0 | 0 | 0 |  |
| commit_p3 | UInt32 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_v9 (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate(toString(version_date)), toUInt16(day_u16)), `idx` UInt16, `aircraft_number` UInt32, `group_by` UInt8, `oh` UInt32, `br` UInt32, `ll` UInt32, `status_id` UInt8, `pre_status_id` UInt8, `status_change_day` UInt16, `sne` UInt32, `ppr` UInt32, `limiter` UInt16, `repair_days` UInt16, `repair_claim_start_day` UInt16, `repair_claim_end_day` UInt16, `repair_claim_source` UInt8, `repair_claim_line_id` UInt16, `repair_time` UInt16, `assembly_time` UInt16, `active_trigger` UInt8, `assembly_trigger` UInt8, `daily_today_u32` UInt32, `daily_next_u32` UInt32, `commit_p2` UInt32, `commit_p3` UInt32) ENGINE = MergeTree PARTITION BY (version_date, toYYYYMM(day_date)) ORDER BY (version_date, version_id, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.67 `sim_masterv2_v9_snap_20260218_181918`
- Engine: MergeTree
- Partition key: toYear(day_date)
- Sorting key: version_date, version_id, day_u16, idx
- Primary key: version_date, version_id, day_u16, idx
- Total rows: 174938
- Total bytes: 1413251
- Distinct `version_date`: 2 (период 20250704 .. 20251230) | `count()`: 174938
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=93904, 2=81034

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate(toString(version_date)), toUInt16(day_u16)) | 0 | 0 | 1 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| status_id | UInt8 |  | 0 | 0 | 0 |  |
| pre_status_id | UInt8 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| limiter | UInt16 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| assembly_time | UInt16 |  | 0 | 0 | 0 |  |
| active_trigger | UInt8 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt8 |  | 0 | 0 | 0 |  |
| daily_today_u32 | UInt32 |  | 0 | 0 | 0 |  |
| daily_next_u32 | UInt32 |  | 0 | 0 | 0 |  |
| commit_p2 | UInt32 |  | 0 | 0 | 0 |  |
| commit_p3 | UInt32 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv2_v9_snap_20260218_181918 (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate(toString(version_date)), toUInt16(day_u16)), `idx` UInt16, `aircraft_number` UInt32, `group_by` UInt8, `oh` UInt32, `br` UInt32, `ll` UInt32, `status_id` UInt8, `pre_status_id` UInt8, `sne` UInt32, `ppr` UInt32, `limiter` UInt16, `repair_days` UInt16, `repair_time` UInt16, `assembly_time` UInt16, `active_trigger` UInt8, `assembly_trigger` UInt8, `daily_today_u32` UInt32, `daily_next_u32` UInt32, `commit_p2` UInt32, `commit_p3` UInt32) ENGINE = MergeTree PARTITION BY toYear(day_date) ORDER BY (version_date, version_id, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.68 `sim_masterv3`
- Engine: MergeTree
- Partition key: toYYYYMM(day_date)
- Sorting key: version_date, day_u16, idx
- Primary key: version_date, day_u16, idx
- Total rows: 2161884
- Total bytes: 14320051
- Distinct `version_date`: 2 (период 20273 .. 20452) | `count()`: 2161884
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=1189900, 2=971984

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 0 | 0 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)) | 0 | 0 | 1 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| partseqno | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | String |  | 0 | 0 | 0 |  |
| intent_state | UInt8 |  | 0 | 0 | 0 |  |
| bi_counter | UInt8 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| cso | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| assembly_time | UInt16 |  | 0 | 0 | 0 |  |
| partout_time | UInt16 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| s4_days | UInt16 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt8 |  | 0 | 0 | 0 |  |
| active_trigger | UInt8 |  | 0 | 0 | 0 |  |
| partout_trigger | UInt8 |  | 0 | 0 | 0 |  |
| mfg_date_days | UInt32 |  | 0 | 0 | 0 |  |
| dt | UInt32 |  | 0 | 0 | 0 |  |
| dn | UInt32 |  | 0 | 0 | 0 |  |
| quota_target_mi8 | UInt16 |  | 0 | 0 | 0 |  |
| quota_target_mi17 | UInt16 |  | 0 | 0 | 0 |  |
| quota_gap_mi8 | Int16 |  | 0 | 0 | 0 |  |
| quota_gap_mi17 | Int16 |  | 0 | 0 | 0 |  |
| repair_quota_load | UInt16 |  | 0 | 0 | 0 |  |
| repair_quota_full | UInt8 |  | 0 | 0 | 0 |  |
| quota_demount | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p1 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p2 | UInt8 |  | 0 | 0 | 0 |  |
| quota_promote_p3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_0_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_6 | UInt8 |  | 0 | 0 | 0 |  |
| transition_2_to_3 | UInt8 |  | 0 | 0 | 0 |  |
| transition_3_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_1_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_4_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| transition_7_to_4 | UInt8 |  | 0 | 0 | 0 |  |
| transition_7_to_2 | UInt8 |  | 0 | 0 | 0 |  |
| export_timestamp | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_masterv3 (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)), `idx` UInt16, `aircraft_number` UInt32, `partseqno` UInt32, `group_by` UInt8, `state` String, `intent_state` UInt8, `bi_counter` UInt8, `sne` UInt32, `ppr` UInt32, `cso` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `repair_time` UInt16, `assembly_time` UInt16, `partout_time` UInt16, `repair_days` UInt16, `s4_days` UInt16, `assembly_trigger` UInt8, `active_trigger` UInt8, `partout_trigger` UInt8, `mfg_date_days` UInt32, `dt` UInt32, `dn` UInt32, `quota_target_mi8` UInt16, `quota_target_mi17` UInt16, `quota_gap_mi8` Int16, `quota_gap_mi17` Int16, `repair_quota_load` UInt16, `repair_quota_full` UInt8, `quota_demount` UInt8, `quota_promote_p1` UInt8, `quota_promote_p2` UInt8, `quota_promote_p3` UInt8, `transition_0_to_2` UInt8, `transition_0_to_3` UInt8, `transition_2_to_4` UInt8, `transition_2_to_6` UInt8, `transition_2_to_3` UInt8, `transition_3_to_2` UInt8, `transition_1_to_4` UInt8, `transition_4_to_2` UInt8, `transition_7_to_4` UInt8, `transition_7_to_2` UInt8, `export_timestamp` DateTime DEFAULT now(), INDEX idx_version (version_date, version_id) TYPE minmax GRANULARITY 1, INDEX idx_day day_u16 TYPE minmax GRANULARITY 1, INDEX idx_state state TYPE bloom_filter GRANULARITY 1, INDEX idx_group_by group_by TYPE minmax GRANULARITY 1) ENGINE = MergeTree PARTITION BY toYYYYMM(day_date) ORDER BY (version_date, day_u16, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.69 `sim_quota_mgr_v8`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_date, version_id, day_u16, group_by
- Primary key: version_date, version_id, day_u16, group_by
- Total rows: 24210
- Total bytes: 436534
- Distinct `version_date`: 2 (период 20250704 .. 20251230) | `count()`: 24210
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=12105, 2=12105

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt8 |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| debug_step | UInt32 |  | 0 | 0 | 0 |  |
| debug_prev_day | UInt32 |  | 0 | 0 | 0 |  |
| debug_adaptive_days | UInt32 |  | 0 | 0 | 0 |  |
| debug_current_day | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 1 | 1 | 0 |  |
| debug_slots_count_mi17 | UInt32 |  | 0 | 0 | 0 |  |
| debug_slot_mi17_0 | UInt32 |  | 0 | 0 | 0 |  |
| debug_slot_mi17_1 | UInt32 |  | 0 | 0 | 0 |  |
| debug_slot_mi17_2 | UInt32 |  | 0 | 0 | 0 |  |
| debug_slot_mi17_3 | UInt32 |  | 0 | 0 | 0 |  |
| debug_slot_mi17_4 | UInt32 |  | 0 | 0 | 0 |  |
| debug_slot_mi17_5 | UInt32 |  | 0 | 0 | 0 |  |
| debug_p2_ops | UInt32 |  | 0 | 0 | 0 |  |
| debug_p2_target | UInt32 |  | 0 | 0 | 0 |  |
| debug_p2_deficit | UInt32 |  | 0 | 0 | 0 |  |
| debug_p2_needed | UInt32 |  | 0 | 0 | 0 |  |
| debug_p2_slots | UInt32 |  | 0 | 0 | 0 |  |
| debug_p2_svc | UInt32 |  | 0 | 0 | 0 |  |
| debug_p2_unsvc | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_ops_mi8 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_ops_mi17 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_target_mi8 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_target_mi17 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_quota_left_mi8 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_quota_left_mi17 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_unsvc_cnt | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_inactive_cnt | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_p1_mi8 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_p1_mi17 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_p2_total | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_p3_total | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_balance_mi8 | Int32 |  | 0 | 0 | 0 |  |
| debug_qm_balance_mi17 | Int32 |  | 0 | 0 | 0 |  |
| debug_qm_target_day | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_ops_cnt_mi8 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_ops_cnt_mi17 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_svc_cnt_mi8 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_svc_cnt_mi17 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_unsvc_ready_mi8 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_unsvc_ready_mi17 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_inactive_mi8 | UInt32 |  | 0 | 0 | 0 |  |
| debug_qm_inactive_mi17 | UInt32 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_quota_mgr_v8 (`version_date` UInt32, `version_id` UInt8, `day_u16` UInt16, `debug_step` UInt32, `debug_prev_day` UInt32, `debug_adaptive_days` UInt32, `debug_current_day` UInt32, `group_by` UInt8, `debug_slots_count_mi17` UInt32, `debug_slot_mi17_0` UInt32, `debug_slot_mi17_1` UInt32, `debug_slot_mi17_2` UInt32, `debug_slot_mi17_3` UInt32, `debug_slot_mi17_4` UInt32, `debug_slot_mi17_5` UInt32, `debug_p2_ops` UInt32, `debug_p2_target` UInt32, `debug_p2_deficit` UInt32, `debug_p2_needed` UInt32, `debug_p2_slots` UInt32, `debug_p2_svc` UInt32, `debug_p2_unsvc` UInt32, `debug_qm_ops_mi8` UInt32, `debug_qm_ops_mi17` UInt32, `debug_qm_target_mi8` UInt32, `debug_qm_target_mi17` UInt32, `debug_qm_quota_left_mi8` UInt32, `debug_qm_quota_left_mi17` UInt32, `debug_qm_unsvc_cnt` UInt32, `debug_qm_inactive_cnt` UInt32, `debug_qm_p1_mi8` UInt32, `debug_qm_p1_mi17` UInt32, `debug_qm_p2_total` UInt32, `debug_qm_p3_total` UInt32, `debug_qm_balance_mi8` Int32, `debug_qm_balance_mi17` Int32, `debug_qm_target_day` UInt32, `debug_qm_ops_cnt_mi8` UInt32, `debug_qm_ops_cnt_mi17` UInt32, `debug_qm_svc_cnt_mi8` UInt32, `debug_qm_svc_cnt_mi17` UInt32, `debug_qm_unsvc_ready_mi8` UInt32, `debug_qm_unsvc_ready_mi17` UInt32, `debug_qm_inactive_mi8` UInt32, `debug_qm_inactive_mi17` UInt32) ENGINE = MergeTree ORDER BY (version_date, version_id, day_u16, group_by) SETTINGS index_granularity = 8192
```
</details>

### 1.70 `sim_repair_lines_v8`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_date, version_id, day_u16, line_id
- Primary key: version_date, version_id, day_u16, line_id
- Total rows: 217890
- Total bytes: 904227
- Distinct `version_date`: 2 (период 20250704 .. 20251230) | `count()`: 217890

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt8 |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| debug_step | UInt32 |  | 0 | 0 | 0 |  |
| debug_prev_day | UInt32 |  | 0 | 0 | 0 |  |
| debug_adaptive_days | UInt32 |  | 0 | 0 | 0 |  |
| debug_current_day | UInt32 |  | 0 | 0 | 0 |  |
| line_id | UInt32 |  | 1 | 1 | 0 |  |
| free_days | UInt32 |  | 0 | 0 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| last_acn | UInt32 |  | 0 | 0 | 0 |  |
| last_day | UInt32 |  | 0 | 0 | 0 |  |
| is_free | UInt8 |  | 0 | 0 | 0 |  |
| ready_mi8 | UInt8 |  | 0 | 0 | 0 |  |
| ready_mi17 | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_repair_lines_v8 (`version_date` UInt32, `version_id` UInt8, `day_u16` UInt16, `debug_step` UInt32, `debug_prev_day` UInt32, `debug_adaptive_days` UInt32, `debug_current_day` UInt32, `line_id` UInt32, `free_days` UInt32, `aircraft_number` UInt32, `last_acn` UInt32, `last_day` UInt32, `is_free` UInt8, `ready_mi8` UInt8, `ready_mi17` UInt8) ENGINE = MergeTree ORDER BY (version_date, version_id, day_u16, line_id) SETTINGS index_granularity = 8192
```
</details>

### 1.71 `sim_repair_slots_v8`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_date, version_id, day_u16, slot_idx
- Primary key: version_date, version_id, day_u16, slot_idx
- Total rows: 0
- Total bytes: 0
- Distinct `version_date`: 0 (период 0 .. 0) | `count()`: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt8 |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| debug_step | UInt32 |  | 0 | 0 | 0 |  |
| debug_prev_day | UInt32 |  | 0 | 0 | 0 |  |
| debug_adaptive_days | UInt32 |  | 0 | 0 | 0 |  |
| debug_current_day | UInt32 |  | 0 | 0 | 0 |  |
| slot_idx | UInt32 |  | 1 | 1 | 0 |  |
| line_id | UInt32 |  | 0 | 0 | 0 |  |
| slots_count | UInt32 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_repair_slots_v8 (`version_date` UInt32, `version_id` UInt8, `day_u16` UInt16, `debug_step` UInt32, `debug_prev_day` UInt32, `debug_adaptive_days` UInt32, `debug_current_day` UInt32, `slot_idx` UInt32, `line_id` UInt32, `slots_count` UInt32) ENGINE = MergeTree ORDER BY (version_date, version_id, day_u16, slot_idx) SETTINGS index_granularity = 8192
```
</details>

### 1.72 `sim_repairline_v9`
- Engine: MergeTree
- Partition key: (version_date, toYYYYMM(day_date))
- Sorting key: version_id, version_date, day_u16, line_id
- Primary key: version_id, version_date, day_u16, line_id
- Total rows: 262800
- Total bytes: 1552829
- Distinct `version_date`: 4 (период 20250704 .. 20260408) | `count()`: 262800
- `group_by` (до 10 групп, ORDER BY `group_by`): 0=143162, 1=15326, 2=104312

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 1 |  |
| version_id | UInt32 |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate(toString(version_date)), toUInt16(day_u16)) | 0 | 0 | 1 |  |
| line_id | UInt8 |  | 1 | 1 | 0 |  |
| free_days | UInt32 |  | 0 | 0 | 0 |  |
| repair_time | UInt32 |  | 0 | 0 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| bank_count | UInt32 |  | 0 | 0 | 0 |  |
| bank_head_start | UInt32 |  | 0 | 0 | 0 |  |
| bank_head_end | UInt32 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_repairline_v9 (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate(toString(version_date)), toUInt16(day_u16)), `line_id` UInt8, `free_days` UInt32, `repair_time` UInt32, `aircraft_number` UInt32, `group_by` UInt8, `bank_count` UInt32, `bank_head_start` UInt32, `bank_head_end` UInt32) ENGINE = MergeTree PARTITION BY (version_date, toYYYYMM(day_date)) ORDER BY (version_id, version_date, day_u16, line_id) SETTINGS index_granularity = 8192
```
</details>

### 1.73 `sim_repairline_v9_snap_20260218_181918`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_id, version_date, day_u16, line_id
- Primary key: version_id, version_date, day_u16, line_id
- Total rows: 131400
- Total bytes: 317582
- Distinct `version_date`: 2 (период 20250704 .. 20251230) | `count()`: 131400
- `group_by` (до 10 групп, ORDER BY `group_by`): 0=67031, 1=14279, 2=50090

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate(toString(version_date)), toUInt16(day_u16)) | 0 | 0 | 0 |  |
| line_id | UInt8 |  | 1 | 1 | 0 |  |
| free_days | UInt32 |  | 0 | 0 | 0 |  |
| repair_time | UInt32 |  | 0 | 0 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_repairline_v9_snap_20260218_181918 (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16, `day_date` Date MATERIALIZED addDays(toDate(toString(version_date)), toUInt16(day_u16)), `line_id` UInt8, `free_days` UInt32, `repair_time` UInt32, `aircraft_number` UInt32, `group_by` UInt8) ENGINE = MergeTree ORDER BY (version_id, version_date, day_u16, line_id) SETTINGS index_granularity = 8192
```
</details>

### 1.74 `sim_results`
- Engine: MergeTree
- Partition key: version_date
- Sorting key: version_date, day_u16, aircraft_number, idx
- Primary key: version_date, day_u16, aircraft_number, idx
- Total rows: 1584288
- Total bytes: 9472055
- Distinct `version_date`: 1 (период 20273 .. 20273) | `count()`: 1584288
- `group_by` (до 10 групп, ORDER BY `group_by`): 1=903835, 2=680453

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 1 |  |
| version_id | UInt32 |  | 0 | 0 | 0 |  |
| version_date_date | Date |  | 0 | 0 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_abs | UInt32 |  | 0 | 0 | 0 |  |
| day_date | Date |  | 0 | 0 | 0 |  |
| idx | UInt16 |  | 1 | 1 | 0 |  |
| aircraft_number | UInt32 |  | 1 | 1 | 0 |  |
| psn | UInt32 |  | 0 | 0 | 0 |  |
| mfg_date_date | Date |  | 0 | 0 | 0 |  |
| partseqno_i | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| status_id | UInt8 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| repair_time | UInt16 |  | 0 | 0 | 0 |  |
| assembly_time | UInt16 |  | 0 | 0 | 0 |  |
| partout_time | UInt16 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| daily_today_u32 | UInt32 |  | 0 | 0 | 0 |  |
| daily_next_u32 | UInt32 |  | 0 | 0 | 0 |  |
| ops_ticket | UInt8 |  | 0 | 0 | 0 |  |
| intent_flag | UInt8 |  | 0 | 0 | 0 |  |
| active_trigger | UInt16 |  | 0 | 0 | 0 |  |
| assembly_trigger | UInt16 |  | 0 | 0 | 0 |  |
| partout_trigger | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_results (`version_date` UInt32, `version_id` UInt32, `version_date_date` Date, `day_u16` UInt16, `day_abs` UInt32, `day_date` Date, `idx` UInt16, `aircraft_number` UInt32, `psn` UInt32, `mfg_date_date` Date, `partseqno_i` UInt32, `group_by` UInt8, `status_id` UInt8, `repair_days` UInt16, `repair_time` UInt16, `assembly_time` UInt16, `partout_time` UInt16, `sne` UInt32, `ppr` UInt32, `ll` UInt32, `oh` UInt32, `br` UInt32, `daily_today_u32` UInt32, `daily_next_u32` UInt32, `ops_ticket` UInt8, `intent_flag` UInt8, `active_trigger` UInt16, `assembly_trigger` UInt16, `partout_trigger` UInt8) ENGINE = MergeTree PARTITION BY version_date ORDER BY (version_date, day_u16, aircraft_number, idx) SETTINGS index_granularity = 8192
```
</details>

### 1.75 `sim_units_v2`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_date, version_id, day_u16, psn
- Primary key: version_date, version_id, day_u16, psn
- Total rows: 14754098
- Total bytes: 40387126
- Distinct `version_date`: 3 (период 20273 .. 20505) | `count()`: 14754098
- `group_by` (до 10 групп, ORDER BY `group_by`): 3=8412656, 4=6341442

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_date | UInt32 |  | 1 | 1 | 0 |  |
| version_id | UInt32 |  | 1 | 1 | 0 |  |
| day_u16 | UInt16 |  | 1 | 1 | 0 |  |
| day_date | Date | addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)) | 0 | 0 | 0 |  |
| idx | UInt32 |  | 0 | 0 | 0 |  |
| psn | UInt32 |  | 1 | 1 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| partseqno_i | UInt32 |  | 0 | 0 | 0 |  |
| aircraft_number | UInt32 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| state | UInt8 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| queue_position | UInt32 |  | 0 | 0 | 0 |  |
| active | UInt8 |  | 0 | 0 | 0 |  |
| export_timestamp | DateTime | now() | 0 | 0 | 0 |  |
| pre_state_id | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_units_v2 (`version_date` UInt32, `version_id` UInt32, `day_u16` UInt16 CODEC(Delta(2), ZSTD(1)), `day_date` Date MATERIALIZED addDays(toDate('1970-01-01'), toUInt32(version_date) + toUInt32(day_u16)), `idx` UInt32, `psn` UInt32, `group_by` UInt8 CODEC(ZSTD(1)), `partseqno_i` UInt32, `aircraft_number` UInt32, `sne` UInt32 CODEC(Delta(4), ZSTD(1)), `ppr` UInt32 CODEC(Delta(4), ZSTD(1)), `state` UInt8 CODEC(ZSTD(1)), `repair_days` UInt16 CODEC(Delta(2), ZSTD(1)), `queue_position` UInt32, `active` UInt8 CODEC(ZSTD(1)), `export_timestamp` DateTime DEFAULT now(), `pre_state_id` UInt8 CODEC(ZSTD(1))) ENGINE = MergeTree ORDER BY (version_date, version_id, day_u16, psn) SETTINGS index_granularity = 8192
```
</details>

### 1.76 `sim_units_v2_msg`
- Engine: MergeTree
- Partition key: 
- Sorting key: version_id, version_date, sim_day, unit_idx
- Primary key: version_id, version_date, sim_day, unit_idx
- Total rows: 0
- Total bytes: 0
- Distinct `version_date`: 0 (период 1970-01-01 .. 1970-01-01) | `count()`: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| version_id | UInt32 |  | 1 | 1 | 0 |  |
| version_date | Date |  | 1 | 1 | 0 |  |
| sim_day | UInt16 |  | 1 | 1 | 0 |  |
| unit_idx | UInt16 |  | 1 | 1 | 0 |  |
| psn | UInt32 |  | 0 | 0 | 0 |  |
| aircraft_id | UInt32 |  | 0 | 0 | 0 |  |
| group_by | UInt8 |  | 0 | 0 | 0 |  |
| state | UInt8 |  | 0 | 0 | 0 |  |
| sne | UInt32 |  | 0 | 0 | 0 |  |
| ppr | UInt32 |  | 0 | 0 | 0 |  |
| dt | UInt32 |  | 0 | 0 | 0 |  |
| repair_days | UInt16 |  | 0 | 0 | 0 |  |
| ll | UInt32 |  | 0 | 0 | 0 |  |
| oh | UInt32 |  | 0 | 0 | 0 |  |
| br | UInt32 |  | 0 | 0 | 0 |  |
| transition_2_to_3 | UInt8 | 0 | 0 | 0 | 0 |  |
| transition_3_to_2 | UInt8 | 0 | 0 | 0 | 0 |  |
| transition_4_to_5 | UInt8 | 0 | 0 | 0 | 0 |  |
| transition_5_to_2 | UInt8 | 0 | 0 | 0 | 0 |  |
| insert_time | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.sim_units_v2_msg (`version_id` UInt32, `version_date` Date, `sim_day` UInt16, `unit_idx` UInt16, `psn` UInt32, `aircraft_id` UInt32, `group_by` UInt8, `state` UInt8, `sne` UInt32, `ppr` UInt32, `dt` UInt32, `repair_days` UInt16, `ll` UInt32, `oh` UInt32, `br` UInt32, `transition_2_to_3` UInt8 DEFAULT 0, `transition_3_to_2` UInt8 DEFAULT 0, `transition_4_to_5` UInt8 DEFAULT 0, `transition_5_to_2` UInt8 DEFAULT 0, `insert_time` DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY (version_id, version_date, sim_day, unit_idx) SETTINGS index_granularity = 8192
```
</details>

### 1.77 `status_dict_flat`
- Engine: Dictionary
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None
- version/`count()`: **ошибка запроса** — `ServerException`: Code: 102. DB::Exception: Unexpected packet from server 10.95.19.132:8123 (expected Hello or Exception, got Unknown packet): (10.95.19.132:8123, local address: 172.18.0.2:37538): While executing Remot

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| status_id | UInt64 |  | 0 | 0 | 0 |  |
| status_name | String |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE DICTIONARY default.status_dict_flat (`status_id` UInt8, `status_name` String) PRIMARY KEY status_id SOURCE(CLICKHOUSE(HOST '10.95.19.132' PORT 8123 TABLE 'dict_status_flat' DB 'default')) LIFETIME(MIN 0 MAX 3600) LAYOUT(FLAT())
```
</details>

### 1.78 `status_overhaul`
- Engine: MergeTree
- Partition key: toYYYYMM(version_date)
- Sorting key: ac_registr, wpno, status, version_date, version_id
- Primary key: ac_registr, wpno, status, version_date, version_id
- Total rows: 315
- Total bytes: 18672
- Distinct `version_date`: 4 (период 2025-07-04 .. 2026-04-08) | `count()`: 206

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_registr | UInt32 |  | 1 | 1 | 0 |  |
| ac_typ | String |  | 0 | 0 | 0 |  |
| wpno | String |  | 1 | 1 | 0 |  |
| description | String |  | 0 | 0 | 0 |  |
| sched_start_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| sched_end_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| act_start_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| act_end_date | Nullable(Date) |  | 0 | 0 | 0 |  |
| status | String |  | 1 | 1 | 0 |  |
| owner | String |  | 0 | 0 | 0 |  |
| operator | String |  | 0 | 0 | 0 |  |
| version_date | Date | today() | 1 | 1 | 1 |  |
| version_id | UInt8 | 1 | 1 | 1 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.status_overhaul (`ac_registr` UInt32, `ac_typ` String, `wpno` String, `description` String, `sched_start_date` Nullable(Date), `sched_end_date` Nullable(Date), `act_start_date` Nullable(Date), `act_end_date` Nullable(Date), `status` String, `owner` String, `operator` String, `version_date` Date DEFAULT today(), `version_id` UInt8 DEFAULT 1) ENGINE = MergeTree PARTITION BY toYYYYMM(version_date) ORDER BY (ac_registr, wpno, status, version_date, version_id) SETTINGS index_granularity = 8192
```
</details>

### 1.79 `test_flame_macroproperty4_roundtrip`
- Engine: MergeTree
- Partition key: 
- Sorting key: record_index, validation_timestamp
- Primary key: record_index, validation_timestamp
- Total rows: 4000
- Total bytes: 27023
- `count()`: 4000 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| record_index | UInt32 |  | 1 | 1 | 0 |  |
| field_25 | Date |  | 0 | 0 | 0 |  |
| field_26 | UInt8 |  | 0 | 0 | 0 |  |
| field_27 | UInt16 |  | 0 | 0 | 0 |  |
| field_28 | UInt16 |  | 0 | 0 | 0 |  |
| field_29 | UInt16 |  | 0 | 0 | 0 |  |
| field_30 | Int8 |  | 0 | 0 | 0 |  |
| field_31 | Int8 |  | 0 | 0 | 0 |  |
| field_32 | Int8 |  | 0 | 0 | 0 |  |
| validation_timestamp | DateTime | now() | 1 | 1 | 0 |  |
| flame_gpu_version | String |  | 0 | 0 | 0 |  |
| data_version_date | Date |  | 0 | 0 | 0 |  |
| data_version_id | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.test_flame_macroproperty4_roundtrip (`record_index` UInt32, `field_25` Date, `field_26` UInt8, `field_27` UInt16, `field_28` UInt16, `field_29` UInt16, `field_30` Int8, `field_31` Int8, `field_32` Int8, `validation_timestamp` DateTime DEFAULT now(), `flame_gpu_version` String, `data_version_date` Date, `data_version_id` UInt8) ENGINE = MergeTree ORDER BY (record_index, validation_timestamp) SETTINGS index_granularity = 8192 COMMENT 'Roundtrip валидация MacroProperty4 FLAME GPU'
```
</details>

### 1.80 `test_flame_macroproperty5_roundtrip`
- Engine: MergeTree
- Partition key: 
- Sorting key: record_index, validation_timestamp
- Primary key: record_index, validation_timestamp
- Total rows: 1116000
- Total bytes: 4803821
- `count()`: 1116000 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| record_index | UInt32 |  | 1 | 1 | 0 |  |
| field_35 | UInt8 |  | 0 | 0 | 0 |  |
| field_36 | UInt16 |  | 0 | 0 | 0 |  |
| field_37 | UInt32 |  | 0 | 0 | 0 |  |
| field_38 | Date |  | 0 | 0 | 0 |  |
| validation_timestamp | DateTime | now() | 1 | 1 | 0 |  |
| flame_gpu_version | String |  | 0 | 0 | 0 |  |
| data_version_date | Date |  | 0 | 0 | 0 |  |
| data_version_id | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.test_flame_macroproperty5_roundtrip (`record_index` UInt32, `field_35` UInt8, `field_36` UInt16, `field_37` UInt32, `field_38` Date, `validation_timestamp` DateTime DEFAULT now(), `flame_gpu_version` String, `data_version_date` Date, `data_version_id` UInt8) ENGINE = MergeTree ORDER BY (record_index, validation_timestamp) SETTINGS index_granularity = 8192 COMMENT 'Roundtrip валидация MacroProperty5 FLAME GPU'
```
</details>

### 1.81 `test_flame_property_roundtrip`
- Engine: MergeTree
- Partition key: 
- Sorting key: validation_timestamp
- Primary key: validation_timestamp
- Total rows: 1
- Total bytes: 768
- `count()`: 1 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| field_71 | Date |  | 0 | 0 | 0 |  |
| field_72 | UInt8 |  | 0 | 0 | 0 |  |
| validation_timestamp | DateTime | now() | 1 | 1 | 0 |  |
| flame_gpu_version | String |  | 0 | 0 | 0 |  |
| data_version_date | Date |  | 0 | 0 | 0 |  |
| data_version_id | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.test_flame_property_roundtrip (`field_71` Date, `field_72` UInt8, `validation_timestamp` DateTime DEFAULT now(), `flame_gpu_version` String, `data_version_date` Date, `data_version_id` UInt8) ENGINE = MergeTree ORDER BY validation_timestamp SETTINGS index_granularity = 8192 COMMENT 'Roundtrip валидация Property FLAME GPU'
```
</details>

### 1.82 `test_table`
- Engine: Memory
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: 0
- Total bytes: 0
- `count()`: 0 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id | UInt32 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.test_table (`id` UInt32) ENGINE = Memory
```
</details>

### 1.83 `vks_events`
- Engine: MergeTree
- Partition key: toYYYYMM(event_date)
- Sorting key: event_time, conference_id, event_type
- Primary key: event_time, conference_id, event_type
- Total rows: 9530
- Total bytes: 127254
- `count()`: 9530 | **нет `version_date`** (колонка отсутствует)

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_date | Date | toDate(event_time) | 0 | 0 | 1 |  |
| event_time | DateTime |  | 1 | 1 | 0 |  |
| conference_id | String |  | 1 | 1 | 0 |  |
| conference_name | String |  | 0 | 0 | 0 |  |
| event_type | String |  | 1 | 1 | 0 |  |
| participant_id | String |  | 0 | 0 | 0 |  |
| participant_name | String |  | 0 | 0 | 0 |  |
| details | String |  | 0 | 0 | 0 |  |
| server_time | DateTime | now() | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>

```sql
CREATE TABLE default.vks_events (`event_date` Date DEFAULT toDate(event_time), `event_time` DateTime, `conference_id` String, `conference_name` String, `event_type` String, `participant_id` String, `participant_name` String, `details` String, `server_time` DateTime DEFAULT now()) ENGINE = MergeTree PARTITION BY toYYYYMM(event_date) ORDER BY (event_time, conference_id, event_type) TTL event_date + toIntervalYear(1) SETTINGS index_granularity = 8192
```
</details>

## 2. DWH ClickHouse (Yandex Managed: HTTPS+TLS)
> Хост/порт/user/db (без пароля): **rc1a-fhb99q2hquq89uhp.mdb.yandexcloud.net:8443**, user **budnik_an**, default database **default**
> TLS: verify=True, ca=yes

### 2.0 Схемы (количество таблиц, `system.tables`)

| database | tables_count |
|---|---:|
| analytics | 71 |
| integrated | 7 |
| reports | 7 |
| source | 53 |
| staging | 53 |

### `reports`.`amos_heli_rotables_components_status`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_dt)
- Sorting key: partno, processing_dt, report_date, mfg_date, partseqno_i, serialno, psn, ac_typ, owner
- Primary key: partno, processing_dt, report_date, mfg_date, partseqno_i, serialno, psn, ac_typ, owner
- Total rows: 10731831
- Total bytes: 92048116

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | String |  | 1 | 1 | 0 | Номер детали (Part Number) |
| partseqno_i | Int32 |  | 1 | 1 | 0 | Номер детали во внутренней нумерации системы |
| serialno | String |  | 1 | 1 | 0 | Серийный номер компонента |
| psn | Int64 |  | 1 | 1 | 0 | Идентификатор позиции |
| ac_typ | LowCardinality(String) |  | 1 | 1 | 0 | Тип воздушного судна |
| ac_type_i | Int32 |  | 0 | 0 | 0 | Внутренний код типа воздушного судна |
| location | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Текущее местоположение компонента |
| LL | Nullable(Int64) |  | 0 | 0 | 0 | Нижний предел (Lower Limit) ресурса |
| OH | Nullable(Int64) |  | 0 | 0 | 0 | Наработка с последнего капитального ремонта (Since Overhaul) |
| OH_threshold | Nullable(Int64) |  | 0 | 0 | 0 | Пороговое значение для капитального ремонта |
| sne | Nullable(Int32) |  | 0 | 0 | 0 | Количество циклов с последнего ремонта |
| ppr | Nullable(Int32) |  | 0 | 0 | 0 | Количество циклов с последнего профилактического ремонта |
| mfg_date | String |  | 1 | 1 | 0 | Дата изготовления |
| oh_at_date | Date32 |  | 0 | 0 | 0 | Дата последнего капитального ремонта |
| shop_visit_counter | Int32 |  | 0 | 0 | 0 | Счетчик визитов в мастерскую |
| owner | LowCardinality(String) |  | 1 | 1 | 0 | Владелец компонента |
| address_i | Int32 |  | 0 | 0 | 0 | Внутренний код адреса/местоположения |
| condition | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Состояние компонента |
| removal_date | Nullable(Date32) |  | 0 | 0 | 0 | Дата снятия с воздушного судна |
| target_date | Nullable(Date32) |  | 0 | 0 | 0 | Плановая дата |
| report_date | Date |  | 1 | 1 | 0 | Дата формирования отчета |
| processing_dt | DateTime |  | 1 | 1 | 1 | дата отработки дага (data_interval_start) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных  (DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE reports.amos_heli_rotables_components_status (`partno` String COMMENT 'Номер детали (Part Number)', `partseqno_i` Int32 COMMENT 'Номер детали во внутренней нумерации системы', `serialno` String COMMENT 'Серийный номер компонента', `psn` Int64 COMMENT 'Идентификатор позиции', `ac_typ` LowCardinality(String) COMMENT 'Тип воздушного судна', `ac_type_i` Int32 COMMENT 'Внутренний код типа воздушного судна', `location` LowCardinality(Nullable(String)) COMMENT 'Текущее местоположение компонента', `LL` Nullable(Int64) COMMENT 'Нижний предел (Lower Limit) ресурса', `OH` Nullable(Int64) COMMENT 'Наработка с последнего капитального ремонта (Since Overhaul)', `OH_threshold` Nullable(Int64) COMMENT 'Пороговое значение для капитального ремонта', `sne` Nullable(Int32) COMMENT 'Количество циклов с последнего ремонта', `ppr` Nullable(Int32) COMMENT 'Количество циклов с последнего профилактического ремонта', `mfg_date` String COMMENT 'Дата изготовления', `oh_at_date` Date32 COMMENT 'Дата последнего капитального ремонта', `shop_visit_counter` Int32 COMMENT 'Счетчик визитов в мастерскую', `owner` LowCardinality(String) COMMENT 'Владелец компонента', `address_i` Int32 COMMENT 'Внутренний код адреса/местоположения', `condition` LowCardinality(Nullable(String)) COMMENT 'Состояние компонента', `removal_date` Nullable(Date32) COMMENT 'Дата снятия с воздушного судна', `target_date` Nullable(Date32) COMMENT 'Плановая дата', `report_date` Date COMMENT 'Дата формирования отчета', `processing_dt` DateTime COMMENT 'дата отработки дага (data_interval_start)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных  (DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/reports/amos_heli_rotables_components_status', '{replica}') PARTITION BY toYYYYMM(processing_dt) ORDER BY (partno, processing_dt, report_date, mfg_date, partseqno_i, serialno, psn, ac_typ, owner) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения финальной витрины данных компонентов amos_heli'
```
</details>

### `reports`.`appareo_flights_detailed`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: processing_date, recorder_identifier, flight_id, event_at, file_name
- Primary key: processing_date, recorder_identifier, flight_id, event_at, file_name
- Total rows: 7568230
- Total bytes: 750137615

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_at | DateTime64(3) |  | 1 | 1 | 0 | Дата и время измерения в формате ISO 8601 (UTC) |
| relative_time_str | String |  | 0 | 0 | 0 | Относительное время от начала полёта (HH:MM:SS.ss) |
| relative_time_seconds | Float64 |  | 0 | 0 | 0 | Относительное время от начала полёта в секундах |
| processing_date | Date |  | 1 | 1 | 1 | Дата обработки файла (из data_interval_start DAG) |
| file_name | String |  | 1 | 1 | 0 | Имя исходного файла (например, VIS-FFK7-02898-Upload.7z) |
| recorder_identifier | LowCardinality(String) |  | 1 | 1 | 0 | ID регистратора (например, VIS-FFK7) |
| flight_id | String |  | 1 | 1 | 0 | ID полёта (например, FFK702898) |
| tail_number | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Бортовой номер          (из справочника, например, 7229) |
| aircraft_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Тип воздушного судна          (из справочника, например, AS 350B3) |
| trigger_statuses | Array(LowCardinality(Nullable(String))) |  | 0 | 0 | 0 | Массив статусов:          before_trigger, trigger_start, trigger_active, trigger_end, after_trigger, trigger_spike.          NULL если вне зоны влияния триггеров |
| trigger_count | UInt8 |  | 0 | 0 | 0 | Количество триггеров, влияющих на эту точку (включая before/after) |
| trigger_names | Array(String) |  | 0 | 0 | 0 | Названия триггеров (включая before/after) |
| trigger_identifiers | Array(String) |  | 0 | 0 | 0 | Идентификаторы триггеров с уровнями |
| trigger_event_ids | Array(String) |  | 0 | 0 | 0 | Порядковые номера событий триггеров в рамках полёта |
| trigger_starts | Array(DateTime) |  | 0 | 0 | 0 | Время начала каждого триггера |
| trigger_ends | Array(DateTime) |  | 0 | 0 | 0 | Время окончания каждого триггера |
| trigger_durations | Array(Float64) |  | 0 | 0 | 0 | Длительность каждого триггера в секундах |
| roll | Float32 |  | 0 | 0 | 0 | Крен самолёта (наклон вокруг продольной оси) в градусах |
| pitch | Float32 |  | 0 | 0 | 0 | Тангаж самолёта (наклон вокруг поперечной оси) в градусах |
| heading | Float32 |  | 0 | 0 | 0 | Магнитный курс самолёта (относительно магнитного севера) в градусах |
| course | Float32 |  | 0 | 0 | 0 | Путевой угол самолёта (относительно истинного севера) в градусах |
| ground_speed | Float32 |  | 0 | 0 | 0 | Путевая скорость относительно земли (узлы) |
| vertical_speed | Float32 |  | 0 | 0 | 0 | Вертикальная скорость (скорость набора или снижения) в 1000 ft/min |
| latitude | Float64 |  | 0 | 0 | 0 | Географическая широта в градусах |
| longitude | Float64 |  | 0 | 0 | 0 | Географическая долгота в градусах |
| altitude_msl | Float32 |  | 0 | 0 | 0 | Высота над уровнем моря (MSL) в метрах |
| altitude_cop | Nullable(Float32) |  | 0 | 0 | 0 | Высота рельефа Copernicus DEM в метрах |
| agl_cop | Nullable(Float32) |  | 0 | 0 | 0 | Высота над землёй (AGL) в метрах |
| x_rotation_rate | Float32 |  | 0 | 0 | 0 | Угловая скорость вокруг оси X (крен) в °/s |
| y_rotation_rate | Float32 |  | 0 | 0 | 0 | Угловая скорость вокруг оси Y (тангаж) в °/s |
| z_rotation_rate | Float32 |  | 0 | 0 | 0 | Угловая скорость вокруг оси Z (рыскание) в °/s |
| x_acceleration | Float32 |  | 0 | 0 | 0 | Линейное ускорение вдоль оси X в g |
| y_acceleration | Float32 |  | 0 | 0 | 0 | Линейное ускорение вдоль оси Y в g |
| z_acceleration | Float32 |  | 0 | 0 | 0 | Линейное ускорение вдоль оси Z в g |
| x_magnetic | Float32 |  | 0 | 0 | 0 | Магнитное поле вдоль оси X в G |
| y_magnetic | Float32 |  | 0 | 0 | 0 | Магнитное поле вдоль оси Y в G |
| z_magnetic | Float32 |  | 0 | 0 | 0 | Магнитное поле вдоль оси Z в G |
| itow | UInt64 |  | 0 | 0 | 0 | GPS-временная метка (ITOW) |
| solution_number | UInt32 |  | 0 | 0 | 0 | Номер GPS/INS решения |
| gps_fix_status | Enum8('NO_FIX' = 0, 'DEAD_RECKONING' = 1, 'FIX_2D' = 2, 'FIX_3D' = 3, 'GPS_PLUS_DEAD_RECKONING' = 4, 'TIME_FIX_ONLY' = 5) |  | 0 | 0 | 0 | Статус GPS-позиционирования |
| is_gps_fix_ok | UInt8 |  | 0 | 0 | 0 | Флаг: решение GPS валидно |
| is_gps_dif_sol | UInt8 |  | 0 | 0 | 0 | Флаг: используется дифференциальное решение |
| is_gps_valid_itow | UInt8 |  | 0 | 0 | 0 | Флаг: валидна временная метка ITOW |
| is_gps_valid_week | UInt8 |  | 0 | 0 | 0 | Флаг: валиден номер недели |
| is_gps_valid_utc | UInt8 |  | 0 | 0 | 0 | Флаг: валидно UTC-время |
| time_to_first_fix | UInt32 |  | 0 | 0 | 0 | Время до первого GPS-решения (мс) |
| ms_since_startup | UInt64 |  | 0 | 0 | 0 | Миллисекунды с момента запуска |
| is_heading_valid | UInt8 |  | 0 | 0 | 0 | Флаг: курс валиден |
| is_roll_pitch_valid | UInt8 |  | 0 | 0 | 0 | Флаг: крен/тангаж валидны |
| horizontal_accuracy | UInt32 |  | 0 | 0 | 0 | Точность горизонтального позиционирования (см) |
| vertical_accuracy | UInt32 |  | 0 | 0 | 0 | Точность вертикального позиционирования (см) |
| time_accuracy_estimate | Int32 |  | 0 | 0 | 0 | Точность времени GPS (нс) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник загрузки данных (служебное поле) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Дата загрузки данных (служебное поле) |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE reports.appareo_flights_detailed (`event_at` DateTime64(3) COMMENT 'Дата и время измерения в формате ISO 8601 (UTC)', `relative_time_str` String COMMENT 'Относительное время от начала полёта (HH:MM:SS.ss)', `relative_time_seconds` Float64 COMMENT 'Относительное время от начала полёта в секундах', `processing_date` Date COMMENT 'Дата обработки файла (из data_interval_start DAG)', `file_name` String COMMENT 'Имя исходного файла (например, VIS-FFK7-02898-Upload.7z)', `recorder_identifier` LowCardinality(String) COMMENT 'ID регистратора (например, VIS-FFK7)', `flight_id` String COMMENT 'ID полёта (например, FFK702898)', `tail_number` LowCardinality(Nullable(String)) COMMENT 'Бортовой номер\r\n        (из справочника, например, 7229)', `aircraft_type` LowCardinality(Nullable(String)) COMMENT 'Тип воздушного судна\r\n        (из справочника, например, AS 350B3)', `trigger_statuses` Array(LowCardinality(Nullable(String))) COMMENT 'Массив статусов:\r\n        before_trigger, trigger_start, trigger_active, trigger_end, after_trigger, trigger_spike.\r\n        NULL если вне зоны влияния триггеров', `trigger_count` UInt8 COMMENT 'Количество триггеров, влияющих на эту точку (включая before/after)', `trigger_names` Array(String) COMMENT 'Названия триггеров (включая before/after)', `trigger_identifiers` Array(String) COMMENT 'Идентификаторы триггеров с уровнями', `trigger_event_ids` Array(String) COMMENT 'Порядковые номера событий триггеров в рамках полёта', `trigger_starts` Array(DateTime) COMMENT 'Время начала каждого триггера', `trigger_ends` Array(DateTime) COMMENT 'Время окончания каждого триггера', `trigger_durations` Array(Float64) COMMENT 'Длительность каждого триггера в секундах', `roll` Float32 COMMENT 'Крен самолёта (наклон вокруг продольной оси) в градусах', `pitch` Float32 COMMENT 'Тангаж самолёта (наклон вокруг поперечной оси) в градусах', `heading` Float32 COMMENT 'Магнитный курс самолёта (относительно магнитного севера) в градусах', `course` Float32 COMMENT 'Путевой угол самолёта (относительно истинного севера) в градусах', `ground_speed` Float32 COMMENT 'Путевая скорость относительно земли (узлы)', `vertical_speed` Float32 COMMENT 'Вертикальная скорость (скорость набора или снижения) в 1000 ft/min', `latitude` Float64 COMMENT 'Географическая широта в градусах', `longitude` Float64 COMMENT 'Географическая долгота в градусах', `altitude_msl` Float32 COMMENT 'Высота над уровнем моря (MSL) в метрах', `altitude_cop` Nullable(Float32) COMMENT 'Высота рельефа Copernicus DEM в метрах', `agl_cop` Nullable(Float32) COMMENT 'Высота над землёй (AGL) в метрах', `x_rotation_rate` Float32 COMMENT 'Угловая скорость вокруг оси X (крен) в °/s', `y_rotation_rate` Float32 COMMENT 'Угловая скорость вокруг оси Y (тангаж) в °/s', `z_rotation_rate` Float32 COMMENT 'Угловая скорость вокруг оси Z (рыскание) в °/s', `x_acceleration` Float32 COMMENT 'Линейное ускорение вдоль оси X в g', `y_acceleration` Float32 COMMENT 'Линейное ускорение вдоль оси Y в g', `z_acceleration` Float32 COMMENT 'Линейное ускорение вдоль оси Z в g', `x_magnetic` Float32 COMMENT 'Магнитное поле вдоль оси X в G', `y_magnetic` Float32 COMMENT 'Магнитное поле вдоль оси Y в G', `z_magnetic` Float32 COMMENT 'Магнитное поле вдоль оси Z в G', `itow` UInt64 COMMENT 'GPS-временная метка (ITOW)', `solution_number` UInt32 COMMENT 'Номер GPS/INS решения', `gps_fix_status` Enum8('NO_FIX' = 0, 'DEAD_RECKONING' = 1, 'FIX_2D' = 2, 'FIX_3D' = 3, 'GPS_PLUS_DEAD_RECKONING' = 4, 'TIME_FIX_ONLY' = 5) COMMENT 'Статус GPS-позиционирования', `is_gps_fix_ok` UInt8 COMMENT 'Флаг: решение GPS валидно', `is_gps_dif_sol` UInt8 COMMENT 'Флаг: используется дифференциальное решение', `is_gps_valid_itow` UInt8 COMMENT 'Флаг: валидна временная метка ITOW', `is_gps_valid_week` UInt8 COMMENT 'Флаг: валиден номер недели', `is_gps_valid_utc` UInt8 COMMENT 'Флаг: валидно UTC-время', `time_to_first_fix` UInt32 COMMENT 'Время до первого GPS-решения (мс)', `ms_since_startup` UInt64 COMMENT 'Миллисекунды с момента запуска', `is_heading_valid` UInt8 COMMENT 'Флаг: курс валиден', `is_roll_pitch_valid` UInt8 COMMENT 'Флаг: крен/тангаж валидны', `horizontal_accuracy` UInt32 COMMENT 'Точность горизонтального позиционирования (см)', `vertical_accuracy` UInt32 COMMENT 'Точность вертикального позиционирования (см)', `time_accuracy_estimate` Int32 COMMENT 'Точность времени GPS (нс)', `meta_source` LowCardinality(String) COMMENT 'Источник загрузки данных (служебное поле)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Дата загрузки данных (служебное поле)') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/reports/appareo_flights_detailed', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (processing_date, recorder_identifier, flight_id, event_at, file_name) SETTINGS index_granularity = 8192 COMMENT 'Витрина: детальная телеметрия полётов Appareo с триггерами и справочными данными о бортах'
```
</details>

### `reports`.`appareo_flights_detailed_distributed`
- Engine: Distributed
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_at | DateTime64(3) |  | 0 | 0 | 0 | Дата и время измерения в формате ISO 8601 (UTC) |
| relative_time_str | String |  | 0 | 0 | 0 | Относительное время от начала полёта (HH:MM:SS.ss) |
| relative_time_seconds | Float64 |  | 0 | 0 | 0 | Относительное время от начала полёта в секундах |
| processing_date | Date |  | 0 | 0 | 0 | Дата обработки файла (из data_interval_start DAG) |
| file_name | String |  | 0 | 0 | 0 | Имя исходного файла (например, VIS-FFK7-02898-Upload.7z) |
| recorder_identifier | LowCardinality(String) |  | 0 | 0 | 0 | ID регистратора (например, VIS-FFK7) |
| flight_id | String |  | 0 | 0 | 0 | ID полёта (например, FFK702898) |
| tail_number | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Бортовой номер          (из справочника, например, 7229) |
| aircraft_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Тип воздушного судна          (из справочника, например, AS 350B3) |
| trigger_statuses | Array(LowCardinality(Nullable(String))) |  | 0 | 0 | 0 | Массив статусов:          before_trigger, trigger_start, trigger_active, trigger_end, after_trigger, trigger_spike.          NULL если вне зоны влияния триггеров |
| trigger_count | UInt8 |  | 0 | 0 | 0 | Количество триггеров, влияющих на эту точку (включая before/after) |
| trigger_names | Array(String) |  | 0 | 0 | 0 | Названия триггеров (включая before/after) |
| trigger_identifiers | Array(String) |  | 0 | 0 | 0 | Идентификаторы триггеров с уровнями |
| trigger_event_ids | Array(String) |  | 0 | 0 | 0 | Порядковые номера событий триггеров в рамках полёта |
| trigger_starts | Array(DateTime) |  | 0 | 0 | 0 | Время начала каждого триггера |
| trigger_ends | Array(DateTime) |  | 0 | 0 | 0 | Время окончания каждого триггера |
| trigger_durations | Array(Float64) |  | 0 | 0 | 0 | Длительность каждого триггера в секундах |
| roll | Float32 |  | 0 | 0 | 0 | Крен самолёта (наклон вокруг продольной оси) в градусах |
| pitch | Float32 |  | 0 | 0 | 0 | Тангаж самолёта (наклон вокруг поперечной оси) в градусах |
| heading | Float32 |  | 0 | 0 | 0 | Магнитный курс самолёта (относительно магнитного севера) в градусах |
| course | Float32 |  | 0 | 0 | 0 | Путевой угол самолёта (относительно истинного севера) в градусах |
| ground_speed | Float32 |  | 0 | 0 | 0 | Путевая скорость относительно земли (узлы) |
| vertical_speed | Float32 |  | 0 | 0 | 0 | Вертикальная скорость (скорость набора или снижения) в 1000 ft/min |
| latitude | Float64 |  | 0 | 0 | 0 | Географическая широта в градусах |
| longitude | Float64 |  | 0 | 0 | 0 | Географическая долгота в градусах |
| altitude_msl | Float32 |  | 0 | 0 | 0 | Высота над уровнем моря (MSL) в метрах |
| altitude_cop | Nullable(Float32) |  | 0 | 0 | 0 | Высота рельефа Copernicus DEM в метрах |
| agl_cop | Nullable(Float32) |  | 0 | 0 | 0 | Высота над землёй (AGL) в метрах |
| x_rotation_rate | Float32 |  | 0 | 0 | 0 | Угловая скорость вокруг оси X (крен) в °/s |
| y_rotation_rate | Float32 |  | 0 | 0 | 0 | Угловая скорость вокруг оси Y (тангаж) в °/s |
| z_rotation_rate | Float32 |  | 0 | 0 | 0 | Угловая скорость вокруг оси Z (рыскание) в °/s |
| x_acceleration | Float32 |  | 0 | 0 | 0 | Линейное ускорение вдоль оси X в g |
| y_acceleration | Float32 |  | 0 | 0 | 0 | Линейное ускорение вдоль оси Y в g |
| z_acceleration | Float32 |  | 0 | 0 | 0 | Линейное ускорение вдоль оси Z в g |
| x_magnetic | Float32 |  | 0 | 0 | 0 | Магнитное поле вдоль оси X в G |
| y_magnetic | Float32 |  | 0 | 0 | 0 | Магнитное поле вдоль оси Y в G |
| z_magnetic | Float32 |  | 0 | 0 | 0 | Магнитное поле вдоль оси Z в G |
| itow | UInt64 |  | 0 | 0 | 0 | GPS-временная метка (ITOW) |
| solution_number | UInt32 |  | 0 | 0 | 0 | Номер GPS/INS решения |
| gps_fix_status | Enum8('NO_FIX' = 0, 'DEAD_RECKONING' = 1, 'FIX_2D' = 2, 'FIX_3D' = 3, 'GPS_PLUS_DEAD_RECKONING' = 4, 'TIME_FIX_ONLY' = 5) |  | 0 | 0 | 0 | Статус GPS-позиционирования |
| is_gps_fix_ok | UInt8 |  | 0 | 0 | 0 | Флаг: решение GPS валидно |
| is_gps_dif_sol | UInt8 |  | 0 | 0 | 0 | Флаг: используется дифференциальное решение |
| is_gps_valid_itow | UInt8 |  | 0 | 0 | 0 | Флаг: валидна временная метка ITOW |
| is_gps_valid_week | UInt8 |  | 0 | 0 | 0 | Флаг: валиден номер недели |
| is_gps_valid_utc | UInt8 |  | 0 | 0 | 0 | Флаг: валидно UTC-время |
| time_to_first_fix | UInt32 |  | 0 | 0 | 0 | Время до первого GPS-решения (мс) |
| ms_since_startup | UInt64 |  | 0 | 0 | 0 | Миллисекунды с момента запуска |
| is_heading_valid | UInt8 |  | 0 | 0 | 0 | Флаг: курс валиден |
| is_roll_pitch_valid | UInt8 |  | 0 | 0 | 0 | Флаг: крен/тангаж валидны |
| horizontal_accuracy | UInt32 |  | 0 | 0 | 0 | Точность горизонтального позиционирования (см) |
| vertical_accuracy | UInt32 |  | 0 | 0 | 0 | Точность вертикального позиционирования (см) |
| time_accuracy_estimate | Int32 |  | 0 | 0 | 0 | Точность времени GPS (нс) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник загрузки данных (служебное поле) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Дата загрузки данных (служебное поле) |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE reports.appareo_flights_detailed_distributed (`event_at` DateTime64(3) COMMENT 'Дата и время измерения в формате ISO 8601 (UTC)', `relative_time_str` String COMMENT 'Относительное время от начала полёта (HH:MM:SS.ss)', `relative_time_seconds` Float64 COMMENT 'Относительное время от начала полёта в секундах', `processing_date` Date COMMENT 'Дата обработки файла (из data_interval_start DAG)', `file_name` String COMMENT 'Имя исходного файла (например, VIS-FFK7-02898-Upload.7z)', `recorder_identifier` LowCardinality(String) COMMENT 'ID регистратора (например, VIS-FFK7)', `flight_id` String COMMENT 'ID полёта (например, FFK702898)', `tail_number` LowCardinality(Nullable(String)) COMMENT 'Бортовой номер\r\n        (из справочника, например, 7229)', `aircraft_type` LowCardinality(Nullable(String)) COMMENT 'Тип воздушного судна\r\n        (из справочника, например, AS 350B3)', `trigger_statuses` Array(LowCardinality(Nullable(String))) COMMENT 'Массив статусов:\r\n        before_trigger, trigger_start, trigger_active, trigger_end, after_trigger, trigger_spike.\r\n        NULL если вне зоны влияния триггеров', `trigger_count` UInt8 COMMENT 'Количество триггеров, влияющих на эту точку (включая before/after)', `trigger_names` Array(String) COMMENT 'Названия триггеров (включая before/after)', `trigger_identifiers` Array(String) COMMENT 'Идентификаторы триггеров с уровнями', `trigger_event_ids` Array(String) COMMENT 'Порядковые номера событий триггеров в рамках полёта', `trigger_starts` Array(DateTime) COMMENT 'Время начала каждого триггера', `trigger_ends` Array(DateTime) COMMENT 'Время окончания каждого триггера', `trigger_durations` Array(Float64) COMMENT 'Длительность каждого триггера в секундах', `roll` Float32 COMMENT 'Крен самолёта (наклон вокруг продольной оси) в градусах', `pitch` Float32 COMMENT 'Тангаж самолёта (наклон вокруг поперечной оси) в градусах', `heading` Float32 COMMENT 'Магнитный курс самолёта (относительно магнитного севера) в градусах', `course` Float32 COMMENT 'Путевой угол самолёта (относительно истинного севера) в градусах', `ground_speed` Float32 COMMENT 'Путевая скорость относительно земли (узлы)', `vertical_speed` Float32 COMMENT 'Вертикальная скорость (скорость набора или снижения) в 1000 ft/min', `latitude` Float64 COMMENT 'Географическая широта в градусах', `longitude` Float64 COMMENT 'Географическая долгота в градусах', `altitude_msl` Float32 COMMENT 'Высота над уровнем моря (MSL) в метрах', `altitude_cop` Nullable(Float32) COMMENT 'Высота рельефа Copernicus DEM в метрах', `agl_cop` Nullable(Float32) COMMENT 'Высота над землёй (AGL) в метрах', `x_rotation_rate` Float32 COMMENT 'Угловая скорость вокруг оси X (крен) в °/s', `y_rotation_rate` Float32 COMMENT 'Угловая скорость вокруг оси Y (тангаж) в °/s', `z_rotation_rate` Float32 COMMENT 'Угловая скорость вокруг оси Z (рыскание) в °/s', `x_acceleration` Float32 COMMENT 'Линейное ускорение вдоль оси X в g', `y_acceleration` Float32 COMMENT 'Линейное ускорение вдоль оси Y в g', `z_acceleration` Float32 COMMENT 'Линейное ускорение вдоль оси Z в g', `x_magnetic` Float32 COMMENT 'Магнитное поле вдоль оси X в G', `y_magnetic` Float32 COMMENT 'Магнитное поле вдоль оси Y в G', `z_magnetic` Float32 COMMENT 'Магнитное поле вдоль оси Z в G', `itow` UInt64 COMMENT 'GPS-временная метка (ITOW)', `solution_number` UInt32 COMMENT 'Номер GPS/INS решения', `gps_fix_status` Enum8('NO_FIX' = 0, 'DEAD_RECKONING' = 1, 'FIX_2D' = 2, 'FIX_3D' = 3, 'GPS_PLUS_DEAD_RECKONING' = 4, 'TIME_FIX_ONLY' = 5) COMMENT 'Статус GPS-позиционирования', `is_gps_fix_ok` UInt8 COMMENT 'Флаг: решение GPS валидно', `is_gps_dif_sol` UInt8 COMMENT 'Флаг: используется дифференциальное решение', `is_gps_valid_itow` UInt8 COMMENT 'Флаг: валидна временная метка ITOW', `is_gps_valid_week` UInt8 COMMENT 'Флаг: валиден номер недели', `is_gps_valid_utc` UInt8 COMMENT 'Флаг: валидно UTC-время', `time_to_first_fix` UInt32 COMMENT 'Время до первого GPS-решения (мс)', `ms_since_startup` UInt64 COMMENT 'Миллисекунды с момента запуска', `is_heading_valid` UInt8 COMMENT 'Флаг: курс валиден', `is_roll_pitch_valid` UInt8 COMMENT 'Флаг: крен/тангаж валидны', `horizontal_accuracy` UInt32 COMMENT 'Точность горизонтального позиционирования (см)', `vertical_accuracy` UInt32 COMMENT 'Точность вертикального позиционирования (см)', `time_accuracy_estimate` Int32 COMMENT 'Точность времени GPS (нс)', `meta_source` LowCardinality(String) COMMENT 'Источник загрузки данных (служебное поле)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Дата загрузки данных (служебное поле)') ENGINE = Distributed('{cluster}', 'reports', 'appareo_flights_detailed', cityHash64(recorder_identifier, flight_id)) COMMENT 'Витрина: детальная телеметрия полётов Appareo с триггерами и справочными данными о бортах'
```
</details>

### `reports`.`availability_pricing_events`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(event_time)
- Sorting key: event_time, trace_id, method, duration
- Primary key: event_time, trace_id, method, duration
- Total rows: 66097590
- Total bytes: 2240778249

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| trace_id | String |  | 1 | 1 | 0 | Идентификатор трассировки запроса |
| event_time | DateTime64(3) |  | 1 | 1 | 1 | Время события (@timestamp), UTC, с миллисекундами |
| user_ip | Nullable(String) |  | 0 | 0 | 0 | IP адрес пользователя |
| duration | Float64 |  | 1 | 1 | 0 | Длительность выполнения запроса (секунды) |
| method | String |  | 1 | 1 | 0 | Название метода API |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в reports (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE reports.availability_pricing_events (`trace_id` String COMMENT 'Идентификатор трассировки запроса', `event_time` DateTime64(3) COMMENT 'Время события (@timestamp), UTC, с миллисекундами', `user_ip` Nullable(String) COMMENT 'IP адрес пользователя', `duration` Float64 COMMENT 'Длительность выполнения запроса (секунды)', `method` String COMMENT 'Название метода API', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в reports (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/reports/availability_pricing_events', '{replica}') PARTITION BY toYYYYMM(event_time) ORDER BY (event_time, trace_id, method, duration) TTL toDateTime(event_time) + toIntervalMonth(3) SETTINGS index_granularity = 8192 COMMENT 'Объединённые события availability_pricing из audit и logstash. Слой reports.'
```
</details>

### `reports`.`fuel_efficiency`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: processing_date, aircraft_type, departure_dt, department, bort, flight_number, kvs_fio, file_name
- Primary key: processing_date, aircraft_type, departure_dt, department, bort, flight_number, kvs_fio, file_name
- Total rows: 17978
- Total bytes: 2076880

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| file_name | String |  | 1 | 1 | 0 | Имя файла бортового самописца (FDR) — ключ связки телеметрии с планом CFP |
| aircraft_type | LowCardinality(String) |  | 1 | 1 | 0 | Тип воздушного судна (семейство ВС, например A320, B737) |
| bort | String |  | 1 | 1 | 0 | Бортовой номер ВС, с которым связан план полёта |
| flight_number | String |  | 1 | 1 | 0 | Номер рейса по расписанию |
| leg_number | Nullable(Int32) |  | 0 | 0 | 0 | Порядковый номер участка (этапа) составного рейса |
| departure_dt | DateTime |  | 1 | 1 | 0 | Плановые дата и время вылета (UTC) |
| arrival_dt | DateTime |  | 0 | 0 | 0 | Плановые дата и время прилёта (UTC) |
| kvs_fio | String |  | 1 | 1 | 0 | Фамилия, имя, отчество командира воздушного судна (КВС) |
| kvs_id | Nullable(Int32) |  | 0 | 0 | 0 | Табельный номер КВС в системе кадрового учёта |
| department | String |  | 1 | 1 | 0 | Лётный отряд (подразделение), к которому приписан КВС |
| departure_airport | Nullable(String) |  | 0 | 0 | 0 | Код аэропорта вылета (IATA) |
| arrival_airport | Nullable(String) |  | 0 | 0 | 0 | Код аэропорта прилёта (IATA) |
| meridian_flight_id | Nullable(Float64) |  | 0 | 0 | 0 | Уникальный идентификатор полёта в системе Meridian |
| cfp_estimated_tow | Nullable(Float64) |  | 0 | 0 | 0 | Расчётная взлётная масса ВС (кг) по плану CFP |
| cfp_fuel_on_trip | Nullable(Float64) |  | 0 | 0 | 0 | Плановый расход топлива на маршрут от взлёта до посадки (кг) по плану CFP |
| cfp_fuel_on_board | Nullable(Float64) |  | 0 | 0 | 0 | Плановое количество топлива на борту при вылете (кг) по плану CFP |
| cfp_fuel_extra | Nullable(Float64) |  | 0 | 0 | 0 | Дополнительное топливо сверх нормативного запаса (кг) по плану CFP |
| cfp_fuel_required | Nullable(Float64) |  | 0 | 0 | 0 | Минимально требуемое топливо для безопасного выполнения рейса (кг) по плану CFP |
| cfp_trip_duration_sec | Nullable(Float64) |  | 0 | 0 | 0 | Плановое время полёта от взлёта до посадки (секунды) по плану CFP |
| cfp_port_fueling | Nullable(Float64) |  | 0 | 0 | 0 | Плановая дозаправка в аэропорту назначения (кг) по плану CFP |
| efficiency_ratio | Float64 |  | 0 | 0 | 0 | Доля времени крейсерского полёта, в течение которого высота соответствовала плановому эшелону (0.0–1.0; 0.0 = нет данных) |
| flight_duration | Nullable(Float64) |  | 0 | 0 | 0 | Фактическая продолжительность полёта от взлёта до посадки (секунды), по данным FDR |
| fuel_on_start_engine | Nullable(Float64) |  | 0 | 0 | 0 | Количество топлива на момент запуска двигателей (кг), по данным FDR |
| fuel_on_start_flight | Nullable(Float64) |  | 0 | 0 | 0 | Количество топлива на момент начала разбега (взлёта) (кг), по данным FDR |
| fuel_on_end_flight | Nullable(Float64) |  | 0 | 0 | 0 | Количество топлива на момент касания ВПП (посадки) (кг), по данным FDR |
| fuel_on_end_engine | Nullable(Float64) |  | 0 | 0 | 0 | Количество топлива на момент остановки двигателей (кг), по данным FDR |
| fuel_on_trip | Nullable(Float64) |  | 0 | 0 | 0 | Фактический расход топлива на маршрут: разница между топливом на взлёте и посадке (кг) |
| diff_fuel_on_trip | Nullable(Float64) |  | 0 | 0 | 0 | Отклонение фактического расхода от планового (кг): cfp_fuel_on_trip − fuel_on_trip |
| tankering_extra_kg | Nullable(Float64) |  | 0 | 0 | 0 | Объём дополнительного танкеринга (кг) — топливо, взятое сверх нормы для экономии на следующем этапе |
| tankering_profit_rub | Nullable(Int32) |  | 0 | 0 | 0 | Экономический эффект от танкеринга (руб.) — разница стоимости топлива между аэропортами |
| error_code | Nullable(String) |  | 0 | 0 | 0 | Код ошибки при обработке полёта (null = успешно, no_flight_data = нет телеметрии FDR) |
| processing_date | Date |  | 1 | 1 | 1 | Дата, за которую выполнялась обработка |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор DAG, загрузившего строку. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE reports.fuel_efficiency (`file_name` String COMMENT 'Имя файла бортового самописца (FDR) — ключ связки телеметрии с планом CFP', `aircraft_type` LowCardinality(String) COMMENT 'Тип воздушного судна (семейство ВС, например A320, B737)', `bort` String COMMENT 'Бортовой номер ВС, с которым связан план полёта', `flight_number` String COMMENT 'Номер рейса по расписанию', `leg_number` Nullable(Int32) COMMENT 'Порядковый номер участка (этапа) составного рейса', `departure_dt` DateTime COMMENT 'Плановые дата и время вылета (UTC)', `arrival_dt` DateTime COMMENT 'Плановые дата и время прилёта (UTC)', `kvs_fio` String COMMENT 'Фамилия, имя, отчество командира воздушного судна (КВС)', `kvs_id` Nullable(Int32) COMMENT 'Табельный номер КВС в системе кадрового учёта', `department` String COMMENT 'Лётный отряд (подразделение), к которому приписан КВС', `departure_airport` Nullable(String) COMMENT 'Код аэропорта вылета (IATA)', `arrival_airport` Nullable(String) COMMENT 'Код аэропорта прилёта (IATA)', `meridian_flight_id` Nullable(Float64) COMMENT 'Уникальный идентификатор полёта в системе Meridian', `cfp_estimated_tow` Nullable(Float64) COMMENT 'Расчётная взлётная масса ВС (кг) по плану CFP', `cfp_fuel_on_trip` Nullable(Float64) COMMENT 'Плановый расход топлива на маршрут от взлёта до посадки (кг) по плану CFP', `cfp_fuel_on_board` Nullable(Float64) COMMENT 'Плановое количество топлива на борту при вылете (кг) по плану CFP', `cfp_fuel_extra` Nullable(Float64) COMMENT 'Дополнительное топливо сверх нормативного запаса (кг) по плану CFP', `cfp_fuel_required` Nullable(Float64) COMMENT 'Минимально требуемое топливо для безопасного выполнения рейса (кг) по плану CFP', `cfp_trip_duration_sec` Nullable(Float64) COMMENT 'Плановое время полёта от взлёта до посадки (секунды) по плану CFP', `cfp_port_fueling` Nullable(Float64) COMMENT 'Плановая дозаправка в аэропорту назначения (кг) по плану CFP', `efficiency_ratio` Float64 COMMENT 'Доля времени крейсерского полёта, в течение которого высота соответствовала плановому эшелону (0.0–1.0; 0.0 = нет данных)', `flight_duration` Nullable(Float64) COMMENT 'Фактическая продолжительность полёта от взлёта до посадки (секунды), по данным FDR', `fuel_on_start_engine` Nullable(Float64) COMMENT 'Количество топлива на момент запуска двигателей (кг), по данным FDR', `fuel_on_start_flight` Nullable(Float64) COMMENT 'Количество топлива на момент начала разбега (взлёта) (кг), по данным FDR', `fuel_on_end_flight` Nullable(Float64) COMMENT 'Количество топлива на момент касания ВПП (посадки) (кг), по данным FDR', `fuel_on_end_engine` Nullable(Float64) COMMENT 'Количество топлива на момент остановки двигателей (кг), по данным FDR', `fuel_on_trip` Nullable(Float64) COMMENT 'Фактический расход топлива на маршрут: разница между топливом на взлёте и посадке (кг)', `diff_fuel_on_trip` Nullable(Float64) COMMENT 'Отклонение фактического расхода от планового (кг): cfp_fuel_on_trip − fuel_on_trip', `tankering_extra_kg` Nullable(Float64) COMMENT 'Объём дополнительного танкеринга (кг) — топливо, взятое сверх нормы для экономии на следующем этапе', `tankering_profit_rub` Nullable(Int32) COMMENT 'Экономический эффект от танкеринга (руб.) — разница стоимости топлива между аэропортами', `error_code` Nullable(String) COMMENT 'Код ошибки при обработке полёта (null = успешно, no_flight_data = нет телеметрии FDR)', `processing_date` Date COMMENT 'Дата, за которую выполнялась обработка', `meta_source` LowCardinality(String) COMMENT 'Идентификатор DAG, загрузившего строку. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC). Служебное поле.', INDEX idx_efficiency_ratio efficiency_ratio TYPE minmax GRANULARITY 4) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/reports/fuel_efficiency_v2', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (processing_date, aircraft_type, departure_dt, department, bort, flight_number, kvs_fio, file_name) SETTINGS index_granularity = 8192 COMMENT 'Витрина отчёта по топливной эффективности полётов.'
```
</details>

### `reports`.`smsc_messages_with_organization`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: processing_date, send_date, reseller_login
- Primary key: processing_date, send_date, reseller_login
- Total rows: 1513940
- Total bytes: 98227458

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id | String |  | 0 | 0 | 0 | Уникальный идентификатор сообщения |
| int_id | String |  | 0 | 0 | 0 | Внешний идентификатор сообщения |
| last_date | DateTime |  | 0 | 0 | 0 | Дата и время последнего обновления |
| last_timestamp | UInt32 |  | 0 | 0 | 0 | Timestamp последнего обновления |
| send_date | DateTime |  | 1 | 1 | 0 | Дата и время отправки |
| send_timestamp | UInt32 |  | 0 | 0 | 0 | Timestamp отправки |
| processing_date | Date |  | 1 | 1 | 1 | Дата обработки сообщения |
| phone | String |  | 0 | 0 | 0 | Номер телефона получателя |
| sender_id | String |  | 0 | 0 | 0 | Идентификатор отправителя |
| reseller_login | String |  | 1 | 1 | 0 | Логин реселлера |
| mccmnc | String |  | 0 | 0 | 0 | MCC и MNC код оператора |
| country | String |  | 0 | 0 | 0 | Страна получателя |
| operator | String |  | 0 | 0 | 0 | Нормализованное название оператора |
| operator_orig | String |  | 0 | 0 | 0 | Оригинальное название оператора |
| region | String |  | 0 | 0 | 0 | Регион получателя |
| status | UInt16 |  | 0 | 0 | 0 | Статус доставки |
| status_name | String |  | 0 | 0 | 0 | Название статуса |
| flag | UInt16 |  | 0 | 0 | 0 | Флаг сообщения |
| type | UInt8 |  | 0 | 0 | 0 | Тип сообщения |
| format | UInt8 |  | 0 | 0 | 0 | Формат сообщения (0-текст, 1-unicode и т.д.) |
| err | UInt64 |  | 0 | 0 | 0 | Код ошибки (если есть) |
| message | String |  | 0 | 0 | 0 | Текст сообщения |
| sms_cnt | Nullable(UInt8) |  | 0 | 0 | 0 | Количество SMS частей |
| cost | Decimal(10, 3) |  | 0 | 0 | 0 | Стоимость сообщения |
| crc | UInt32 |  | 0 | 0 | 0 | Контрольная сумма |
| organization | String |  | 0 | 0 | 0 | Наименование ораганизации |
| organization_id | String |  | 0 | 0 | 0 | Внутренний индетификатор ораганизации |
| comment | String | '' | 0 | 0 | 0 | Комментарий к сообщению |
| send_retry | UInt8 | 0 | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE reports.smsc_messages_with_organization (`id` String COMMENT 'Уникальный идентификатор сообщения', `int_id` String COMMENT 'Внешний идентификатор сообщения', `last_date` DateTime COMMENT 'Дата и время последнего обновления', `last_timestamp` UInt32 COMMENT 'Timestamp последнего обновления', `send_date` DateTime COMMENT 'Дата и время отправки', `send_timestamp` UInt32 COMMENT 'Timestamp отправки', `processing_date` Date COMMENT 'Дата обработки сообщения', `phone` String COMMENT 'Номер телефона получателя', `sender_id` String COMMENT 'Идентификатор отправителя', `reseller_login` String COMMENT 'Логин реселлера', `mccmnc` String COMMENT 'MCC и MNC код оператора', `country` String COMMENT 'Страна получателя', `operator` String COMMENT 'Нормализованное название оператора', `operator_orig` String COMMENT 'Оригинальное название оператора', `region` String COMMENT 'Регион получателя', `status` UInt16 COMMENT 'Статус доставки', `status_name` String COMMENT 'Название статуса', `flag` UInt16 COMMENT 'Флаг сообщения', `type` UInt8 COMMENT 'Тип сообщения', `format` UInt8 COMMENT 'Формат сообщения (0-текст, 1-unicode и т.д.)', `err` UInt64 COMMENT 'Код ошибки (если есть)', `message` String COMMENT 'Текст сообщения', `sms_cnt` Nullable(UInt8) COMMENT 'Количество SMS частей', `cost` Decimal(10, 3) COMMENT 'Стоимость сообщения', `crc` UInt32 COMMENT 'Контрольная сумма', `organization` String COMMENT 'Наименование ораганизации', `organization_id` String COMMENT 'Внутренний индетификатор ораганизации', `comment` String DEFAULT '' COMMENT 'Комментарий к сообщению', `send_retry` UInt8 DEFAULT 0) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/reports/smsc_messages_with_organization', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (processing_date, send_date, reseller_login) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения SMS-сообщений из SMSC с добавлением организаций'
```
</details>

### `reports`.`smsc_messages_with_organization_mv`
- Engine: MaterializedView
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id | String |  | 0 | 0 | 0 |  |
| int_id | String |  | 0 | 0 | 0 |  |
| last_date | DateTime |  | 0 | 0 | 0 |  |
| last_timestamp | UInt32 |  | 0 | 0 | 0 |  |
| send_date | DateTime |  | 0 | 0 | 0 |  |
| send_timestamp | UInt32 |  | 0 | 0 | 0 |  |
| processing_date | Date |  | 0 | 0 | 0 |  |
| phone | String |  | 0 | 0 | 0 |  |
| sender_id | String |  | 0 | 0 | 0 |  |
| reseller_login | String |  | 0 | 0 | 0 |  |
| mccmnc | String |  | 0 | 0 | 0 |  |
| country | String |  | 0 | 0 | 0 |  |
| operator | String |  | 0 | 0 | 0 |  |
| operator_orig | String |  | 0 | 0 | 0 |  |
| region | String |  | 0 | 0 | 0 |  |
| status | UInt16 |  | 0 | 0 | 0 |  |
| status_name | String |  | 0 | 0 | 0 |  |
| flag | UInt16 |  | 0 | 0 | 0 |  |
| type | UInt8 |  | 0 | 0 | 0 |  |
| format | UInt8 |  | 0 | 0 | 0 |  |
| err | UInt64 |  | 0 | 0 | 0 |  |
| message | String |  | 0 | 0 | 0 |  |
| sms_cnt | Nullable(UInt8) |  | 0 | 0 | 0 |  |
| cost | Decimal(10, 3) |  | 0 | 0 | 0 |  |
| crc | UInt32 |  | 0 | 0 | 0 |  |
| organization | String |  | 0 | 0 | 0 |  |
| organization_id | String |  | 0 | 0 | 0 |  |
| comment | String |  | 0 | 0 | 0 |  |
| send_retry | UInt8 |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE MATERIALIZED VIEW reports.smsc_messages_with_organization_mv TO reports.smsc_messages_with_organization (`id` String, `int_id` String, `last_date` DateTime, `last_timestamp` UInt32, `send_date` DateTime, `send_timestamp` UInt32, `processing_date` Date, `phone` String, `sender_id` String, `reseller_login` String, `mccmnc` String, `country` String, `operator` String, `operator_orig` String, `region` String, `status` UInt16, `status_name` String, `flag` UInt16, `type` UInt8, `format` UInt8, `err` UInt64, `message` String, `sms_cnt` Nullable(UInt8), `cost` Decimal(10, 3), `crc` UInt32, `organization` String, `organization_id` String, `comment` String, `send_retry` UInt8) AS SELECT * FROM analytics.smsc_messages_with_organization
```
</details>

### `staging`.`amos_heli_ac_typ`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: ac_typ
- Primary key: ac_typ
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_type_i | Int32 |  | 0 | 0 | 0 |  |
| ac_typ | String |  | 1 | 1 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| fa_ac_typ | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_ac_typ (`ac_type_i` Int32, `ac_typ` String, `description` Nullable(String), `fa_ac_typ` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_ac_typ', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY ac_typ SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из ac_typ вертолетного амос'
```
</details>

### `staging`.`amos_heli_address`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: address_i
- Primary key: address_i
- Total rows: 2
- Total bytes: 1077

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| address_i | Int32 |  | 1 | 1 | 0 |  |
| vendor | LowCardinality(String) |  | 0 | 0 | 0 |  |
| name | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_address (`address_i` Int32, `vendor` LowCardinality(String), `name` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_address', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY address_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из address вертолетного амос'
```
</details>

### `staging`.`amos_heli_adr_properties`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: address_i
- Primary key: address_i
- Total rows: 16
- Total bytes: 2665

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| address_i | Int32 |  | 1 | 1 | 0 |  |
| prop_type_i | Int32 |  | 0 | 0 | 0 |  |
| remarks | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| value | LowCardinality(String) |  | 0 | 0 | 0 |  |
| status | Nullable(Int8) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных ( DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_adr_properties (`address_i` Int32, `prop_type_i` Int32, `remarks` LowCardinality(Nullable(String)), `value` LowCardinality(String), `status` Nullable(Int8), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных ( DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_adr_properties', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY address_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из adr_properties вертолетного амос'
```
</details>

### `staging`.`amos_heli_adr_special`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: special_i
- Primary key: special_i
- Total rows: 35
- Total bytes: 2946

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| special_i | Int32 |  | 1 | 1 | 0 |  |
| address_i | Int32 |  | 0 | 0 | 0 |  |
| special | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| remarks | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| amount | Nullable(Int8) |  | 0 | 0 | 0 |  |
| reference_no | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных ( DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_adr_special (`special_i` Int32, `address_i` Int32, `special` LowCardinality(Nullable(String)), `remarks` LowCardinality(Nullable(String)), `amount` Nullable(Int8), `reference_no` LowCardinality(Nullable(String)), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных ( DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_adr_special', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY special_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из adr_special вертолетного амос'
```
</details>

### `staging`.`amos_heli_aircraft`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: ac_registr
- Primary key: ac_registr
- Total rows: 28
- Total bytes: 10179

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_registr | String |  | 1 | 1 | 0 |  |
| ac_typ | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ac_registr_prefix | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| ac_model | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| owner | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| manual_owner | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| status | Int16 |  | 0 | 0 | 0 |  |
| non_managed | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| homebase | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| object_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| description | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_aircraft (`ac_registr` String, `ac_typ` LowCardinality(String), `ac_registr_prefix` LowCardinality(Nullable(String)), `ac_model` LowCardinality(Nullable(String)), `owner` LowCardinality(Nullable(String)), `manual_owner` LowCardinality(Nullable(String)), `status` Int16, `non_managed` LowCardinality(Nullable(String)), `homebase` LowCardinality(Nullable(String)), `object_type` LowCardinality(Nullable(String)), `description` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_aircraft', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY ac_registr SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из aircraft вертолетного амос'
```
</details>

### `staging`.`amos_heli_applicability`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: applicabilityno_i
- Primary key: applicabilityno_i
- Total rows: 86
- Total bytes: 13932

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| applicabilityno_i | Int64 |  | 1 | 1 | 0 |  |
| effectivityno_i | Int32 |  | 0 | 0 | 0 |  |
| applicable | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ref_key | Int64 |  | 0 | 0 | 0 |  |
| ref_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_applicability (`applicabilityno_i` Int64, `effectivityno_i` Int32, `applicable` LowCardinality(String), `ref_key` Int64, `ref_type` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_applicability', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY applicabilityno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из applicability вертолетного амос'
```
</details>

### `staging`.`amos_heli_condition`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: condition
- Primary key: condition
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| condition | LowCardinality(String) |  | 1 | 1 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_condition (`condition` LowCardinality(String), `description` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_condition', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY condition SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из condition вертолетного амос'
```
</details>

### `staging`.`amos_heli_counter`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: counterno_i
- Primary key: counterno_i
- Total rows: 1386
- Total bytes: 40309

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counterno_i | Int64 |  | 1 | 1 | 0 |  |
| counter_templateno_i | Int32 |  | 0 | 0 | 0 |  |
| ref_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ref_key | Int64 |  | 0 | 0 | 0 |  |
| life_value | Nullable(Float64) |  | 0 | 0 | 0 |  |
| is_unknown | LowCardinality(String) |  | 0 | 0 | 0 |  |
| status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| master_counterno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| readout_date | Date32 |  | 0 | 0 | 0 |  |
| readout_time | Int64 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_counter (`counterno_i` Int64, `counter_templateno_i` Int32, `ref_type` LowCardinality(String), `ref_key` Int64, `life_value` Nullable(Float64), `is_unknown` LowCardinality(String), `status` Nullable(Int16), `master_counterno_i` Nullable(Int64), `readout_date` Date32, `readout_time` Int64, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_counter', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY counterno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из counter вертолетного амос'
```
</details>

### `staging`.`amos_heli_counter_definition`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: counter_defno_i
- Primary key: counter_defno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counter_defno_i | UInt64 |  | 1 | 1 | 0 |  |
| code | LowCardinality(String) |  | 0 | 0 | 0 |  |
| name | Nullable(String) |  | 0 | 0 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| display_unit | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | Дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_counter_definition (`counter_defno_i` UInt64, `code` LowCardinality(String), `name` Nullable(String), `description` Nullable(String), `display_unit` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'Дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_counter_definition', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY counter_defno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из counter_definition вертолетного амос'
```
</details>

### `staging`.`amos_heli_counter_psn`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: psn
- Primary key: psn
- Total rows: 246303
- Total bytes: 1893102

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| psn | Int32 |  | 1 | 1 | 0 |  |
| counter_psn_overhaul | Nullable(Int32) |  | 0 | 0 | 0 |  |
| counter_psn_on_date | Nullable(Int32) |  | 0 | 0 | 0 |  |
| processing_dt | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_start) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_counter_psn (`psn` Int32, `counter_psn_overhaul` Nullable(Int32), `counter_psn_on_date` Nullable(Int32), `processing_dt` DateTime COMMENT 'дата отработки дага (data_interval_start)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_counter_psn', '{replica}') ORDER BY psn SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения временных данных вычислений функций БД Амос counter_psn_overhaul и counter_psn_on_date'
```
</details>

### `staging`.`amos_heli_counter_template`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: counter_templateno_i
- Primary key: counter_templateno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counter_templateno_i | Int32 |  | 1 | 1 | 0 |  |
| counter_defno_i | Int32 |  | 0 | 0 | 0 |  |
| counter_template_groupno_i | Int32 |  | 0 | 0 | 0 |  |
| type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| is_calculated | LowCardinality(String) |  | 0 | 0 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_counter_template (`counter_templateno_i` Int32, `counter_defno_i` Int32, `counter_template_groupno_i` Int32, `type` LowCardinality(String), `is_calculated` LowCardinality(String), `description` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_counter_template', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY counter_templateno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из counter_template вертолетного амос'
```
</details>

### `staging`.`amos_heli_counter_value`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: counter_valueno_i
- Primary key: counter_valueno_i
- Total rows: 910
- Total bytes: 17345

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counter_valueno_i | Int64 |  | 1 | 1 | 0 |  |
| counterno_i | Int64 |  | 0 | 0 | 0 |  |
| life_value | Nullable(Float64) |  | 0 | 0 | 0 |  |
| readout_ref_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| on_counter_valueno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| off_counter_valueno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| is_minor | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| readout_date | Date32 |  | 0 | 0 | 0 |  |
| readout_time | Int64 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_counter_value (`counter_valueno_i` Int64, `counterno_i` Int64, `life_value` Nullable(Float64), `readout_ref_type` LowCardinality(Nullable(String)), `on_counter_valueno_i` Nullable(Int64), `off_counter_valueno_i` Nullable(Int64), `is_minor` LowCardinality(Nullable(String)), `readout_date` Date32, `readout_time` Int64, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_counter_value', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY counter_valueno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из counter_value вертолетного амос'
```
</details>

### `staging`.`amos_heli_event_effectivity`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: effectivityno_i
- Primary key: effectivityno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| effectivityno_i | Int32 |  | 1 | 1 | 0 |  |
| effectivity_headerno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| title | String |  | 0 | 0 | 0 |  |
| aircraft_typ | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_event_effectivity (`effectivityno_i` Int32, `effectivity_headerno_i` Nullable(Int32), `title` String, `aircraft_typ` LowCardinality(Nullable(String)), `partno` Nullable(String), `status` Nullable(Int16), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_event_effectivity', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY effectivityno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из event_effectivity вертолетного амос'
```
</details>

### `staging`.`amos_heli_event_effectivity_link`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: effectivity_linkno_i
- Primary key: effectivity_linkno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| effectivityno_i | Int32 |  | 0 | 0 | 0 |  |
| effectivity_linkno_i | Int32 |  | 1 | 1 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_event_effectivity_link (`effectivityno_i` Int32, `effectivity_linkno_i` Int32, `event_type` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_event_effectivity_link', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY effectivity_linkno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из event_effectivity_link вертолетного амос'
```
</details>

### `staging`.`amos_heli_event_effectivity_rules`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: effectivityno_i
- Primary key: effectivityno_i
- Total rows: 6
- Total bytes: 1126

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| effectivityno_i | Int32 |  | 1 | 1 | 0 |  |
| aircraft_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| rotable_manuf_before | Nullable(Int32) |  | 0 | 0 | 0 |  |
| rotable_manuf_after | Nullable(Int32) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_event_effectivity_rules (`effectivityno_i` Int32, `aircraft_type` LowCardinality(Nullable(String)), `rotable_manuf_before` Nullable(Int32), `rotable_manuf_after` Nullable(Int32), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_event_effectivity_rules', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY effectivityno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из event_effectivity_rules вертолетного амос'
```
</details>

### `staging`.`amos_heli_event_effectivity_sns`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: effectivity_snno_i
- Primary key: effectivity_snno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| effectivityno_i | Int32 |  | 0 | 0 | 0 |  |
| effectivity_snno_i | Int32 |  | 1 | 1 | 0 |  |
| range_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_event_effectivity_sns (`effectivityno_i` Int32, `effectivity_snno_i` Int32, `range_type` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_event_effectivity_sns', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY effectivity_snno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из event_effectivity_sns вертолетного амос'
```
</details>

### `staging`.`amos_heli_forecast`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: event_perfno_i
- Primary key: event_perfno_i
- Total rows: 913811
- Total bytes: 15541891

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_perfno_i | Int64 |  | 1 | 1 | 0 |  |
| psn | Nullable(Int64) |  | 0 | 0 | 0 |  |
| requirement | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_registr | String |  | 0 | 0 | 0 |  |
| ac_typ | LowCardinality(String) |  | 0 | 0 | 0 |  |
| event | String |  | 0 | 0 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_forecast (`event_perfno_i` Int64, `psn` Nullable(Int64), `requirement` LowCardinality(Nullable(String)), `partno` Nullable(String), `serialno` Nullable(String), `ac_registr` String, `ac_typ` LowCardinality(String), `event` String, `event_type` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_forecast', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY event_perfno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из forecast вертолетного амос'
```
</details>

### `staging`.`amos_heli_forecast_dimension`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: event_perfno_i
- Primary key: event_perfno_i
- Total rows: 749029
- Total bytes: 4054681

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_perfno_i | Int64 |  | 1 | 1 | 0 |  |
| counter_defno_i | Int32 |  | 0 | 0 | 0 |  |
| dimension | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_forecast_dimension (`event_perfno_i` Int64, `counter_defno_i` Int32, `dimension` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_forecast_dimension', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY event_perfno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из forecast_dimension вертолетного амос'
```
</details>

### `staging`.`amos_heli_history`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: historyno_i
- Primary key: historyno_i
- Total rows: 123
- Total bytes: 4452

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| historyno_i | Int64 |  | 1 | 1 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| vm | LowCardinality(String) |  | 0 | 0 | 0 |  |
| od_detailno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| ac_registr | Nullable(String) |  | 0 | 0 | 0 |  |
| del_date | Date32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_history (`historyno_i` Int64, `partno` String, `serialno` Nullable(String), `vm` LowCardinality(String), `od_detailno_i` Nullable(Int32), `ac_registr` Nullable(String), `del_date` Date32, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_history', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY historyno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из history вертолетного амос'
```
</details>

### `staging`.`amos_heli_location`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: locationno_i
- Primary key: locationno_i
- Total rows: 14
- Total bytes: 7037

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| locationno_i | Int32 |  | 1 | 1 | 0 |  |
| description | LowCardinality(String) |  | 0 | 0 | 0 |  |
| store | LowCardinality(String) |  | 0 | 0 | 0 |  |
| station | LowCardinality(String) |  | 0 | 0 | 0 |  |
| location | LowCardinality(String) |  | 0 | 0 | 0 |  |
| status | Int16 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_location (`locationno_i` Int32, `description` LowCardinality(String), `store` LowCardinality(String), `station` LowCardinality(String), `location` LowCardinality(String), `status` Int16, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_location', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY locationno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из location вертолетного амос'
```
</details>

### `staging`.`amos_heli_mevt_effectivity`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: mevt_effectivityno_i
- Primary key: mevt_effectivityno_i
- Total rows: 92
- Total bytes: 18691

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| mevt_effectivityno_i | Int64 |  | 1 | 1 | 0 |  |
| mevt_headerno_i | Int64 |  | 0 | 0 | 0 |  |
| effectivity_linkno_i | Int64 |  | 0 | 0 | 0 |  |
| template_revisionno_i | Int64 |  | 0 | 0 | 0 |  |
| timerequirementno_i | Int64 |  | 0 | 0 | 0 |  |
| revision_key | Int32 |  | 0 | 0 | 0 |  |
| revision_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| applicable_status | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_mevt_effectivity (`mevt_effectivityno_i` Int64, `mevt_headerno_i` Int64, `effectivity_linkno_i` Int64, `template_revisionno_i` Int64, `timerequirementno_i` Int64, `revision_key` Int32, `revision_type` LowCardinality(String), `applicable_status` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_mevt_effectivity', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY mevt_effectivityno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из mevt_effectivity вертолетного амос'
```
</details>

### `staging`.`amos_heli_mevt_header`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: mevt_headerno_i
- Primary key: mevt_headerno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| mevt_headerno_i | Int64 |  | 1 | 1 | 0 |  |
| identifier | String |  | 0 | 0 | 0 |  |
| ref_key | Int64 |  | 0 | 0 | 0 |  |
| ref_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| mevt_key | Int32 |  | 0 | 0 | 0 |  |
| mevt_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_mevt_header (`mevt_headerno_i` Int64, `identifier` String, `ref_key` Int64, `ref_type` LowCardinality(String), `mevt_key` Int32, `mevt_type` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_mevt_header', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY mevt_headerno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из mevt_header вертолетного амос'
```
</details>

### `staging`.`amos_heli_od_detail`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: detailno_i
- Primary key: detailno_i
- Total rows: 133
- Total bytes: 7721

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| detailno_i | Int64 |  | 1 | 1 | 0 |  |
| orderno_i | Int64 |  | 0 | 0 | 0 |  |
| order_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ac_registr | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| state | LowCardinality(String) |  | 0 | 0 | 0 |  |
| vendor | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| condition | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| qty | Int32 |  | 0 | 0 | 0 |  |
| purch_price | Int64 |  | 0 | 0 | 0 |  |
| target_date | Date32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_od_detail (`detailno_i` Int64, `orderno_i` Int64, `order_type` LowCardinality(String), `ac_registr` LowCardinality(Nullable(String)), `state` LowCardinality(String), `vendor` LowCardinality(Nullable(String)), `partno` String, `serialno` Nullable(String), `condition` LowCardinality(Nullable(String)), `qty` Int32, `purch_price` Int64, `target_date` Date32, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_od_detail', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY detailno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из od_detail вертолетного амос'
```
</details>

### `staging`.`amos_heli_part`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: partno
- Primary key: partno
- Total rows: 6
- Total bytes: 1940

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | String |  | 1 | 1 | 0 |  |
| partmatch | String |  | 0 | 0 | 0 |  |
| partseqno_i | Int32 |  | 0 | 0 | 0 |  |
| ac_typ | LowCardinality(String) |  | 0 | 0 | 0 |  |
| mat_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| description | String |  | 0 | 0 | 0 |  |
| remarks | Nullable(String) |  | 0 | 0 | 0 |  |
| ata_chapter | LowCardinality(String) |  | 0 | 0 | 0 |  |
| vendor | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_part (`partno` String, `partmatch` String, `partseqno_i` Int32, `ac_typ` LowCardinality(String), `mat_type` LowCardinality(String), `description` String, `remarks` Nullable(String), `ata_chapter` LowCardinality(String), `vendor` Nullable(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_part', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY partno SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из part вертолетного амос'
```
</details>

### `staging`.`amos_heli_part_requirement`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: part_requirementno_i
- Primary key: part_requirementno_i
- Total rows: 11
- Total bytes: 4535

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| part_requirementno_i | Int32 |  | 1 | 1 | 0 |  |
| type | Int32 |  | 0 | 0 | 0 |  |
| status | Int16 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_part_requirement (`part_requirementno_i` Int32, `type` Int32, `status` Int16, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_part_requirement', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY part_requirementno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из part_requirement вертолетного амос'
```
</details>

### `staging`.`amos_heli_part_special`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: part_specialno_i
- Primary key: part_specialno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| part_specialno_i | Int32 |  | 1 | 1 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| special | LowCardinality(String) |  | 0 | 0 | 0 |  |
| remarks | LowCardinality(String) |  | 0 | 0 | 0 |  |
| amount | Int16 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_part_special (`part_specialno_i` Int32, `partno` String, `special` LowCardinality(String), `remarks` LowCardinality(String), `amount` Int16, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_part_special', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY part_specialno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из part_special вертолетного амос'
```
</details>

### `staging`.`amos_heli_requirement_header`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: requirement_headerno_i
- Primary key: requirement_headerno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_key | Int32 |  | 0 | 0 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| effectivity_headerno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| requirement_headerno_i | Int32 |  | 1 | 1 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_requirement_header (`event_key` Int32, `event_type` LowCardinality(String), `effectivity_headerno_i` Nullable(Int32), `requirement_headerno_i` Int32, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_requirement_header', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY requirement_headerno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из requirement_header вертолетного амос'
```
</details>

### `staging`.`amos_heli_requirement_type`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: requirement_typeno_i
- Primary key: requirement_typeno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| requirement_typeno_i | Int32 |  | 1 | 1 | 0 |  |
| requirement | LowCardinality(String) |  | 0 | 0 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| life_limit | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_requirement_type (`requirement_typeno_i` Int32, `requirement` LowCardinality(String), `description` Nullable(String), `life_limit` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_requirement_type', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY requirement_typeno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из requirement_type вертолетного амос'
```
</details>

### `staging`.`amos_heli_rotables`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: psn
- Primary key: psn
- Total rows: 216
- Total bytes: 24042

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_registr | Nullable(String) |  | 0 | 0 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| material_lifecycle_id | Int64 |  | 0 | 0 | 0 |  |
| serialno | String |  | 0 | 0 | 0 |  |
| locationno_i | Int32 |  | 0 | 0 | 0 |  |
| psn | Int64 |  | 1 | 1 | 0 |  |
| shop_visit_counter | Int32 |  | 0 | 0 | 0 |  |
| mfg_unknown | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| orderno | Nullable(String) |  | 0 | 0 | 0 |  |
| owner | LowCardinality(String) |  | 0 | 0 | 0 |  |
| condition | LowCardinality(String) |  | 0 | 0 | 0 |  |
| oh_at_date | Date32 |  | 0 | 0 | 0 |  |
| del_date | Date32 |  | 0 | 0 | 0 |  |
| mfg_date | Date32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_rotables (`ac_registr` Nullable(String), `partno` String, `material_lifecycle_id` Int64, `serialno` String, `locationno_i` Int32, `psn` Int64, `shop_visit_counter` Int32, `mfg_unknown` LowCardinality(Nullable(String)), `orderno` Nullable(String), `owner` LowCardinality(String), `condition` LowCardinality(String), `oh_at_date` Date32, `del_date` Date32, `mfg_date` Date32, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_rotables', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY psn SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из rotables вертолетного амос'
```
</details>

### `staging`.`amos_heli_treq_dimension_group`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: dimension_groupno_i
- Primary key: dimension_groupno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| interval_groupno_i | Int64 |  | 0 | 0 | 0 |  |
| dimension_groupno_i | Int64 |  | 1 | 1 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_treq_dimension_group (`interval_groupno_i` Int64, `dimension_groupno_i` Int64, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_treq_dimension_group', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY dimension_groupno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из treq_dimension_group вертолетного амос'
```
</details>

### `staging`.`amos_heli_treq_event_link`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: eventlinkno_i
- Primary key: eventlinkno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| eventlinkno_i | Int32 |  | 1 | 1 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| event_key | Int32 |  | 0 | 0 | 0 |  |
| ac_registr | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| psn | Int32 |  | 0 | 0 | 0 |  |
| status | Int16 |  | 0 | 0 | 0 |  |
| timerequirementno_i | Int32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_treq_event_link (`eventlinkno_i` Int32, `event_type` LowCardinality(String), `event_key` Int32, `ac_registr` LowCardinality(Nullable(String)), `psn` Int32, `status` Int16, `timerequirementno_i` Int32, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_treq_event_link', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY eventlinkno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из treq_event_link вертолетного амос'
```
</details>

### `staging`.`amos_heli_treq_interval`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: intervalno_i
- Primary key: intervalno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| intervalno_i | Int64 |  | 1 | 1 | 0 |  |
| interval_groupno_i | Int32 |  | 0 | 0 | 0 |  |
| dimension_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| counter_defno_i | Int32 |  | 0 | 0 | 0 |  |
| amount_interval | Int64 |  | 0 | 0 | 0 |  |
| due_at | Nullable(Int32) |  | 0 | 0 | 0 |  |
| dimension_groupno_i | Int64 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_treq_interval (`intervalno_i` Int64, `interval_groupno_i` Int32, `dimension_type` LowCardinality(String), `counter_defno_i` Int32, `amount_interval` Int64, `due_at` Nullable(Int32), `dimension_groupno_i` Int64, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_treq_interval', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY intervalno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из treq_interval вертолетного амос'
```
</details>

### `staging`.`amos_heli_treq_interval_group`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: interval_groupno_i
- Primary key: interval_groupno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| interval_groupno_i | Int64 |  | 1 | 1 | 0 |  |
| timerequirementno_i | Int64 |  | 0 | 0 | 0 |  |
| threshold | LowCardinality(String) |  | 0 | 0 | 0 |  |
| group_name | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_treq_interval_group (`interval_groupno_i` Int64, `timerequirementno_i` Int64, `threshold` LowCardinality(String), `group_name` LowCardinality(Nullable(String)), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_treq_interval_group', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY interval_groupno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из treq_interval_group вертолетного амос'
```
</details>

### `staging`.`amos_heli_treq_time_requirement`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: timerequirementno_i
- Primary key: timerequirementno_i
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| timerequirementno_i | Int64 |  | 1 | 1 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| event_key | Int64 |  | 0 | 0 | 0 |  |
| ac_group | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_treq_time_requirement (`timerequirementno_i` Int64, `event_type` LowCardinality(String), `event_key` Int64, `ac_group` LowCardinality(Nullable(String)), `type` LowCardinality(String), `status` Nullable(Int16), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_treq_time_requirement', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY timerequirementno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из treq_time_requirement вертолетного амос'
```
</details>

### `staging`.`amos_heli_wo_event_link`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: event_perfno_i
- Primary key: event_perfno_i
- Total rows: 1160
- Total bytes: 36056

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_perfno_i | Int64 |  | 1 | 1 | 0 |  |
| effectivity_linkno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| mevt_headerno_i | Int64 |  | 0 | 0 | 0 |  |
| pending_status | Int16 |  | 0 | 0 | 0 |  |
| event_name | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_wo_event_link (`event_perfno_i` Int64, `effectivity_linkno_i` Nullable(Int32), `mevt_headerno_i` Int64, `pending_status` Int16, `event_name` Nullable(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_wo_event_link', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY event_perfno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из wo_event_link вертолетного амос'
```
</details>

### `staging`.`amos_heli_wo_header`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: event_perfno_i
- Primary key: event_perfno_i
- Total rows: 839
- Total bytes: 52064

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_perfno_i | Int64 |  | 1 | 1 | 0 |  |
| psn | Nullable(Int64) |  | 0 | 0 | 0 |  |
| ata_chapter | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| state | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ac_registr | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_wo_header (`event_perfno_i` Int64, `psn` Nullable(Int64), `ata_chapter` LowCardinality(Nullable(String)), `state` LowCardinality(String), `ac_registr` Nullable(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_wo_header', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY event_perfno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из wo_header вертолетного амос'
```
</details>

### `staging`.`amos_heli_wo_transfer`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: event_transferno_i
- Primary key: event_transferno_i
- Total rows: 1055
- Total bytes: 17430

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_transferno_i | Int64 |  | 1 | 1 | 0 |  |
| event_perfno_i | Int64 |  | 0 | 0 | 0 |  |
| is_last_transfer | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_wo_transfer (`event_transferno_i` Int64, `event_perfno_i` Int64, `is_last_transfer` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_wo_transfer', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY event_transferno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из history вертолетного амос'
```
</details>

### `staging`.`amos_heli_wo_transfer_dimension`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: wo_transfer_dimensionno_i
- Primary key: wo_transfer_dimensionno_i
- Total rows: 1341
- Total bytes: 29713

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| wo_transfer_dimensionno_i | Int64 |  | 1 | 1 | 0 |  |
| event_transferno_i | Int64 |  | 0 | 0 | 0 |  |
| treq_intervalno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| counterno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| due_at | Nullable(Float64) |  | 0 | 0 | 0 |  |
| status | Int8 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_wo_transfer_dimension (`wo_transfer_dimensionno_i` Int64, `event_transferno_i` Int64, `treq_intervalno_i` Nullable(Int64), `counterno_i` Nullable(Int64), `due_at` Nullable(Float64), `status` Int8, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_wo_transfer_dimension', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY wo_transfer_dimensionno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из history вертолетного амос'
```
</details>

### `staging`.`amos_heli_wp_header`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: wpno_i
- Primary key: wpno_i
- Total rows: 598
- Total bytes: 80474

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| mpno | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| drop_locationno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| wpno_i | Int32 |  | 1 | 1 | 0 |  |
| wpno | String |  | 0 | 0 | 0 |  |
| ac_registr | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| ac_typ | LowCardinality(String) |  | 0 | 0 | 0 |  |
| projectno | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| est_groundtime | Int32 |  | 0 | 0 | 0 |  |
| station | LowCardinality(String) |  | 0 | 0 | 0 |  |
| start_date | Int32 |  | 0 | 0 | 0 |  |
| start_time | Int32 |  | 0 | 0 | 0 |  |
| end_date | Int32 |  | 0 | 0 | 0 |  |
| end_time | Int32 |  | 0 | 0 | 0 |  |
| description | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| owner | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| hidden | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| status | Int8 |  | 0 | 0 | 0 |  |
| act_start_date | Nullable(Int32) |  | 0 | 0 | 0 |  |
| act_start_time | Nullable(Int32) |  | 0 | 0 | 0 |  |
| act_end_date | Nullable(Int32) |  | 0 | 0 | 0 |  |
| act_end_time | Nullable(Int32) |  | 0 | 0 | 0 |  |
| responsible | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| delay | Nullable(Int32) |  | 0 | 0 | 0 |  |
| cust_wpno | Nullable(String) |  | 0 | 0 | 0 |  |
| priority_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| remarks | Nullable(String) |  | 0 | 0 | 0 |  |
| extension_time | Nullable(Int32) |  | 0 | 0 | 0 |  |
| extension_reason | Nullable(Int8) |  | 0 | 0 | 0 |  |
| mpno_i | Nullable(Int16) |  | 0 | 0 | 0 |  |
| mp_revision | Nullable(Int16) |  | 0 | 0 | 0 |  |
| wp_status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| events_collection_status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| uuid | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_operator | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| ac_model | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 0 | 0 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.amos_heli_wp_header (`mpno` LowCardinality(Nullable(String)), `drop_locationno_i` Nullable(Int32), `wpno_i` Int32, `wpno` String, `ac_registr` LowCardinality(Nullable(String)), `ac_typ` LowCardinality(String), `projectno` LowCardinality(Nullable(String)), `est_groundtime` Int32, `station` LowCardinality(String), `start_date` Int32, `start_time` Int32, `end_date` Int32, `end_time` Int32, `description` LowCardinality(Nullable(String)), `owner` LowCardinality(Nullable(String)), `hidden` LowCardinality(Nullable(String)), `status` Int8, `act_start_date` Nullable(Int32), `act_start_time` Nullable(Int32), `act_end_date` Nullable(Int32), `act_end_time` Nullable(Int32), `responsible` LowCardinality(Nullable(String)), `delay` Nullable(Int32), `cust_wpno` Nullable(String), `priority_code` LowCardinality(Nullable(String)), `remarks` Nullable(String) CODEC(ZSTD(3)), `extension_time` Nullable(Int32), `extension_reason` Nullable(Int8), `mpno_i` Nullable(Int16), `mp_revision` Nullable(Int16), `wp_status` Nullable(Int16), `events_collection_status` Nullable(Int16), `uuid` Nullable(String) CODEC(ZSTD(3)), `ac_operator` LowCardinality(Nullable(String)), `ac_model` LowCardinality(Nullable(String)), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/amos_heli_wp_header', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY wpno_i SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из wp_header вертолетного амос'
```
</details>

### `staging`.`appareo_flights_detailed`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: processing_date, recorder_identifier, flight_id, event_at
- Primary key: processing_date, recorder_identifier, flight_id, event_at
- Total rows: 25
- Total bytes: 8796

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_at | DateTime64(3) |  | 1 | 1 | 0 | Дата и время измерения в формате ISO 8601 (UTC) |
| relative_time_str | String |  | 0 | 0 | 0 | Относительное время от начала полета в строковом формате (HH:MM:SS.ss) |
| relative_time_seconds | Float64 |  | 0 | 0 | 0 | Относительное время от начала полета в секундах |
| processing_date | Date |  | 1 | 1 | 1 | Дата обработки файла (из data_interval_start DAG) |
| file_name | String |  | 0 | 0 | 0 | Имя исходного файла (например, VIS-FFK7-02898-Upload.7z) |
| recorder_identifier | LowCardinality(String) |  | 1 | 1 | 0 | ID регистратора (например, VIS-FFK7) |
| flight_id | String |  | 1 | 1 | 0 | ID полета (например, 02898) |
| tail_number | String |  | 0 | 0 | 0 | Бортовой номер (из справочника, например, 7229) |
| aircraft_type | String |  | 0 | 0 | 0 | Тип воздушного судна (из справочника, например, AS 350B3) |
| trigger_status | Enum8('no_trigger' = 0, 'trigger_start' = 1, 'trigger_active' = 2, 'trigger_end' = 3, 'multiple_triggers' = 4) |  | 0 | 0 | 0 | Статус триггера: нет, начало, активен, конец, множественные |
| trigger_count | UInt8 |  | 0 | 0 | 0 | Количество активных триггеров в этот момент времени |
| trigger_names | Array(String) |  | 0 | 0 | 0 | Названия всех активных триггеров (например, [350Roll, 350Pitch]) |
| trigger_identifiers | Array(String) |  | 0 | 0 | 0 | Идентификаторы всех активных триггеров с уровнями (например, [350ROLL-L, 350PITCH-H]) |
| trigger_starts | Array(DateTime) |  | 0 | 0 | 0 | Время начала каждого активного триггера |
| trigger_ends | Array(DateTime) |  | 0 | 0 | 0 | Время окончания каждого активного триггера |
| trigger_durations | Array(Float64) |  | 0 | 0 | 0 | Длительность каждого активного триггера в секундах |
| roll | Float32 |  | 0 | 0 | 0 | Крен самолета (наклон вокруг продольной оси) в градусах |
| pitch | Float32 |  | 0 | 0 | 0 | Тангаж самолета (наклон вокруг поперечной оси) в градусах |
| heading | Float32 |  | 0 | 0 | 0 | Магнитный курс самолета (относительно магнитного севера) в градусах |
| course | Float32 |  | 0 | 0 | 0 | Путевой угол самолета (относительно истинного севера) в градусах |
| ground_speed | Float32 |  | 0 | 0 | 0 | Путевая скорость относительно земли (узлы) |
| vertical_speed | Float32 |  | 0 | 0 | 0 | Вертикальная скорость (скорость набора или снижения) в 1000 ft/min |
| latitude | Float64 |  | 0 | 0 | 0 | Географическая широта в градусах |
| longitude | Float64 |  | 0 | 0 | 0 | Географическая долгота в градусах |
| altitude_msl | Float32 |  | 0 | 0 | 0 | Высота над уровнем моря (MSL) в метрах |
| altitude_cop | Nullable(Float32) |  | 0 | 0 | 0 | Высота рельефа Copernicus DEM в метрах |
| agl_cop | Nullable(Float32) |  | 0 | 0 | 0 | Высота над землей (AGL) в метрах |
| x_rotation_rate | Float32 |  | 0 | 0 | 0 | Угловая скорость вокруг оси X (крен) в °/s |
| y_rotation_rate | Float32 |  | 0 | 0 | 0 | Угловая скорость вокруг оси Y (тангаж) в °/s |
| z_rotation_rate | Float32 |  | 0 | 0 | 0 | Угловая скорость вокруг оси Z (рыскание) в °/s |
| x_acceleration | Float32 |  | 0 | 0 | 0 | Линейное ускорение вдоль оси X в g |
| y_acceleration | Float32 |  | 0 | 0 | 0 | Линейное ускорение вдоль оси Y в g |
| z_acceleration | Float32 |  | 0 | 0 | 0 | Линейное ускорение вдоль оси Z в g |
| x_magnetic | Float32 |  | 0 | 0 | 0 | Магнитное поле вдоль оси X в G |
| y_magnetic | Float32 |  | 0 | 0 | 0 | Магнитное поле вдоль оси Y в G |
| z_magnetic | Float32 |  | 0 | 0 | 0 | Магнитное поле вдоль оси Z в G |
| itow | UInt64 |  | 0 | 0 | 0 | GPS-временная метка (ITOW) |
| solution_number | UInt32 |  | 0 | 0 | 0 | Номер GPS/INS решения |
| gps_fix_status | Enum8('NO_FIX' = 0, 'DEAD_RECKONING' = 1, 'FIX_2D' = 2, 'FIX_3D' = 3, 'GPS_PLUS_DEAD_RECKONING' = 4, 'TIME_FIX_ONLY' = 5) |  | 0 | 0 | 0 | Статус GPS-позиционирования |
| gps_fix_ok | UInt8 |  | 0 | 0 | 0 | Флаг: решение GPS валидно |
| gps_dif_sol | UInt8 |  | 0 | 0 | 0 | Флаг: используется дифференциальное решение |
| gps_valid_itow | UInt8 |  | 0 | 0 | 0 | Флаг: валидна временная метка ITOW |
| gps_valid_week | UInt8 |  | 0 | 0 | 0 | Флаг: валиден номер недели |
| gps_valid_utc | UInt8 |  | 0 | 0 | 0 | Флаг: валидно UTC-время |
| time_to_first_fix | UInt32 |  | 0 | 0 | 0 | Время до первого GPS-решения (мс) |
| ms_since_startup | UInt64 |  | 0 | 0 | 0 | Миллисекунды с момента запуска |
| heading_valid | UInt8 |  | 0 | 0 | 0 | Флаг: курс валиден |
| roll_pitch_valid | UInt8 |  | 0 | 0 | 0 | Флаг: крен/тангаж валидны |
| horizontal_accuracy | UInt32 |  | 0 | 0 | 0 | Точность горизонтального позиционирования (см) |
| vertical_accuracy | UInt32 |  | 0 | 0 | 0 | Точность вертикального позиционирования (см) |
| time_accuracy_estimate | Int32 |  | 0 | 0 | 0 | Точность времени GPS (нс) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник загрузки данных (DAG_ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Дата загрузки данных (служебное поле) |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.appareo_flights_detailed (`event_at` DateTime64(3) COMMENT 'Дата и время измерения в формате ISO 8601 (UTC)', `relative_time_str` String COMMENT 'Относительное время от начала полета в строковом формате (HH:MM:SS.ss)', `relative_time_seconds` Float64 COMMENT 'Относительное время от начала полета в секундах', `processing_date` Date COMMENT 'Дата обработки файла (из data_interval_start DAG)', `file_name` String COMMENT 'Имя исходного файла (например, VIS-FFK7-02898-Upload.7z)', `recorder_identifier` LowCardinality(String) COMMENT 'ID регистратора (например, VIS-FFK7)', `flight_id` String COMMENT 'ID полета (например, 02898)', `tail_number` String COMMENT 'Бортовой номер (из справочника, например, 7229)', `aircraft_type` String COMMENT 'Тип воздушного судна (из справочника, например, AS 350B3)', `trigger_status` Enum8('no_trigger' = 0, 'trigger_start' = 1, 'trigger_active' = 2, 'trigger_end' = 3, 'multiple_triggers' = 4) COMMENT 'Статус триггера: нет, начало, активен, конец, множественные', `trigger_count` UInt8 COMMENT 'Количество активных триггеров в этот момент времени', `trigger_names` Array(String) COMMENT 'Названия всех активных триггеров (например, [350Roll, 350Pitch])', `trigger_identifiers` Array(String) COMMENT 'Идентификаторы всех активных триггеров с уровнями (например, [350ROLL-L, 350PITCH-H])', `trigger_starts` Array(DateTime) COMMENT 'Время начала каждого активного триггера', `trigger_ends` Array(DateTime) COMMENT 'Время окончания каждого активного триггера', `trigger_durations` Array(Float64) COMMENT 'Длительность каждого активного триггера в секундах', `roll` Float32 COMMENT 'Крен самолета (наклон вокруг продольной оси) в градусах', `pitch` Float32 COMMENT 'Тангаж самолета (наклон вокруг поперечной оси) в градусах', `heading` Float32 COMMENT 'Магнитный курс самолета (относительно магнитного севера) в градусах', `course` Float32 COMMENT 'Путевой угол самолета (относительно истинного севера) в градусах', `ground_speed` Float32 COMMENT 'Путевая скорость относительно земли (узлы)', `vertical_speed` Float32 COMMENT 'Вертикальная скорость (скорость набора или снижения) в 1000 ft/min', `latitude` Float64 COMMENT 'Географическая широта в градусах', `longitude` Float64 COMMENT 'Географическая долгота в градусах', `altitude_msl` Float32 COMMENT 'Высота над уровнем моря (MSL) в метрах', `altitude_cop` Nullable(Float32) COMMENT 'Высота рельефа Copernicus DEM в метрах', `agl_cop` Nullable(Float32) COMMENT 'Высота над землей (AGL) в метрах', `x_rotation_rate` Float32 COMMENT 'Угловая скорость вокруг оси X (крен) в °/s', `y_rotation_rate` Float32 COMMENT 'Угловая скорость вокруг оси Y (тангаж) в °/s', `z_rotation_rate` Float32 COMMENT 'Угловая скорость вокруг оси Z (рыскание) в °/s', `x_acceleration` Float32 COMMENT 'Линейное ускорение вдоль оси X в g', `y_acceleration` Float32 COMMENT 'Линейное ускорение вдоль оси Y в g', `z_acceleration` Float32 COMMENT 'Линейное ускорение вдоль оси Z в g', `x_magnetic` Float32 COMMENT 'Магнитное поле вдоль оси X в G', `y_magnetic` Float32 COMMENT 'Магнитное поле вдоль оси Y в G', `z_magnetic` Float32 COMMENT 'Магнитное поле вдоль оси Z в G', `itow` UInt64 COMMENT 'GPS-временная метка (ITOW)', `solution_number` UInt32 COMMENT 'Номер GPS/INS решения', `gps_fix_status` Enum8('NO_FIX' = 0, 'DEAD_RECKONING' = 1, 'FIX_2D' = 2, 'FIX_3D' = 3, 'GPS_PLUS_DEAD_RECKONING' = 4, 'TIME_FIX_ONLY' = 5) COMMENT 'Статус GPS-позиционирования', `gps_fix_ok` UInt8 COMMENT 'Флаг: решение GPS валидно', `gps_dif_sol` UInt8 COMMENT 'Флаг: используется дифференциальное решение', `gps_valid_itow` UInt8 COMMENT 'Флаг: валидна временная метка ITOW', `gps_valid_week` UInt8 COMMENT 'Флаг: валиден номер недели', `gps_valid_utc` UInt8 COMMENT 'Флаг: валидно UTC-время', `time_to_first_fix` UInt32 COMMENT 'Время до первого GPS-решения (мс)', `ms_since_startup` UInt64 COMMENT 'Миллисекунды с момента запуска', `heading_valid` UInt8 COMMENT 'Флаг: курс валиден', `roll_pitch_valid` UInt8 COMMENT 'Флаг: крен/тангаж валидны', `horizontal_accuracy` UInt32 COMMENT 'Точность горизонтального позиционирования (см)', `vertical_accuracy` UInt32 COMMENT 'Точность вертикального позиционирования (см)', `time_accuracy_estimate` Int32 COMMENT 'Точность времени GPS (нс)', `meta_source` LowCardinality(String) COMMENT 'Источник загрузки данных (DAG_ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Дата загрузки данных (служебное поле)') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/appareo_flights_detailed', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (processing_date, recorder_identifier, flight_id, event_at) SETTINGS index_granularity = 8192 COMMENT 'ВРЕМЕННАЯ ТАБЛИЦА, пример витрины для отчетов: детальная телеметрия полетов Appareo с информацией о триггерах и справочными данными о бортах'
```
</details>

### `staging`.`appareo_raw_files`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: processing_date, file_name, file_signature
- Primary key: processing_date, file_name, file_signature
- Total rows: 5
- Total bytes: 2732

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| file_name | String |  | 1 | 1 | 0 | Имя файла (как в источнике) |
| file_created_at | DateTime |  | 0 | 0 | 0 | Дата появления файла (UTC) |
| file_modified_at | DateTime |  | 0 | 0 | 0 | Дата изменения файла (UTC) |
| file_size | UInt64 |  | 0 | 0 | 0 | Размер файла в байтах |
| file_signature | String |  | 1 | 1 | 0 | Подпись файла: hash(name + size + modified_at) |
| source_path | String |  | 0 | 0 | 0 | Полный путь к каталогу/файлу на сетевой шаре |
| processing_date | Date |  | 1 | 1 | 0 | Дата обработки файла (когда файл был забран из источника) |
| s3_bucket | LowCardinality(String) |  | 0 | 0 | 0 | Имя S3 бакета с файлом |
| s3_key | String |  | 0 | 0 | 0 | Ключ объекта в S3 |
| s3_etag | String |  | 0 | 0 | 0 | ETag объекта после загрузки в S3 |
| s3_endpoint_url | LowCardinality(String) |  | 0 | 0 | 0 | Endpoint URL S3-совместимого хранилища |
| s3_region | LowCardinality(String) |  | 0 | 0 | 0 | Регион S3 хранилища |
| s3_url | String |  | 0 | 0 | 0 | Полный URL файла в S3 (s3://bucket/key) |
| load_status | LowCardinality(String) | 'pending' | 0 | 0 | 0 | Текущий статус обработки файла |
| error_message | String |  | 0 | 0 | 0 | Описание ошибки (при наличии) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.appareo_raw_files (`file_name` String COMMENT 'Имя файла (как в источнике)', `file_created_at` DateTime COMMENT 'Дата появления файла (UTC)', `file_modified_at` DateTime COMMENT 'Дата изменения файла (UTC)', `file_size` UInt64 COMMENT 'Размер файла в байтах', `file_signature` String COMMENT 'Подпись файла: hash(name + size + modified_at)', `source_path` String COMMENT 'Полный путь к каталогу/файлу на сетевой шаре', `processing_date` Date COMMENT 'Дата обработки файла (когда файл был забран из источника)', `s3_bucket` LowCardinality(String) COMMENT 'Имя S3 бакета с файлом', `s3_key` String COMMENT 'Ключ объекта в S3', `s3_etag` String COMMENT 'ETag объекта после загрузки в S3', `s3_endpoint_url` LowCardinality(String) COMMENT 'Endpoint URL S3-совместимого хранилища', `s3_region` LowCardinality(String) COMMENT 'Регион S3 хранилища', `s3_url` String COMMENT 'Полный URL файла в S3 (s3://bucket/key)', `load_status` LowCardinality(String) DEFAULT 'pending' COMMENT 'Текущий статус обработки файла', `error_message` String COMMENT 'Описание ошибки (при наличии)', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/appareo_raw_files', '{replica}') ORDER BY (processing_date, file_name, file_signature) SETTINGS index_granularity = 8192 COMMENT 'Промежуточная таблица с манифестом файлов данных Appareo и статусами обработки.'
```
</details>

### `staging`.`availability_pricing_staging`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: dag_id, event_time, trace_id
- Primary key: dag_id, event_time, trace_id
- Total rows: 0
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| trace_id | String |  | 1 | 1 | 0 | Идентификатор трассировки запроса |
| event_time | DateTime64(3) |  | 1 | 1 | 0 | Время события (@timestamp), UTC |
| user_ip | Nullable(String) |  | 0 | 0 | 0 | IP адрес пользователя |
| duration | Float64 |  | 0 | 0 | 0 | Длительность выполнения запроса (секунды) |
| method | Nullable(String) |  | 0 | 0 | 0 | Название метода API |
| dag_id | LowCardinality(String) |  | 1 | 1 | 0 | Идентификатор процесса загрузки данных |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник данных (идентификатор процесса загрузки) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время записи в staging (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.availability_pricing_staging (`trace_id` String COMMENT 'Идентификатор трассировки запроса', `event_time` DateTime64(3) COMMENT 'Время события (@timestamp), UTC', `user_ip` Nullable(String) COMMENT 'IP адрес пользователя', `duration` Float64 COMMENT 'Длительность выполнения запроса (секунды)', `method` Nullable(String) COMMENT 'Название метода API', `dag_id` LowCardinality(String) COMMENT 'Идентификатор процесса загрузки данных', `meta_source` LowCardinality(String) COMMENT 'Источник данных (идентификатор процесса загрузки)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время записи в staging (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/availability_pricing_staging', '{replica}') ORDER BY (dag_id, event_time, trace_id) SETTINGS index_granularity = 8192 COMMENT 'Промежуточная таблица выгрузки ES→source для availability_pricing. Без партиционирования.'
```
</details>

### `staging`.`fdr_express_analysis_docx_transfer`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: target_date, file_name, file_signature
- Primary key: target_date, file_name, file_signature
- Total rows: 122
- Total bytes: 17226

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| file_name | String |  | 1 | 1 | 0 | Имя docx файла |
| smb_relative_path | String |  | 0 | 0 | 0 | Относительный путь внутри шары TsopiShare |
| file_size | UInt64 |  | 0 | 0 | 0 | Размер файла, байт |
| file_modified_at | DateTime |  | 0 | 0 | 0 | Время изменения файла в DFI (UTC) |
| file_signature | String |  | 1 | 1 | 0 | MD5 содержимого файла |
| s3_bucket | LowCardinality(String) |  | 0 | 0 | 0 | Бакет S3 |
| s3_key | String |  | 0 | 0 | 0 | Ключ объекта S3 |
| s3_url | String |  | 0 | 0 | 0 | Полный путь объекта в S3 (s3://bucket/key) |
| s3_etag | String |  | 0 | 0 | 0 | ETag после загрузки |
| s3_endpoint_url | LowCardinality(String) |  | 0 | 0 | 0 | Endpoint S3 |
| s3_region | LowCardinality(String) |  | 0 | 0 | 0 | Регион S3 |
| target_date | Date |  | 1 | 1 | 0 | Целевая дата обработки |
| load_status | LowCardinality(String) |  | 0 | 0 | 0 | Статус обработки: pending/uploaded/failed_sync |
| error_stage | LowCardinality(String) |  | 0 | 0 | 0 | Этап ошибки: sync, пусто при успехе |
| error_message | String |  | 0 | 0 | 0 | Текст ошибки на этапе синхронизации |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор DAG |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Момент вставки в staging (UTC) |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.fdr_express_analysis_docx_transfer (`file_name` String COMMENT 'Имя docx файла', `smb_relative_path` String COMMENT 'Относительный путь внутри шары TsopiShare', `file_size` UInt64 COMMENT 'Размер файла, байт', `file_modified_at` DateTime COMMENT 'Время изменения файла в DFI (UTC)', `file_signature` String COMMENT 'MD5 содержимого файла', `s3_bucket` LowCardinality(String) COMMENT 'Бакет S3', `s3_key` String COMMENT 'Ключ объекта S3', `s3_url` String COMMENT 'Полный путь объекта в S3 (s3://bucket/key)', `s3_etag` String COMMENT 'ETag после загрузки', `s3_endpoint_url` LowCardinality(String) COMMENT 'Endpoint S3', `s3_region` LowCardinality(String) COMMENT 'Регион S3', `target_date` Date COMMENT 'Целевая дата обработки', `load_status` LowCardinality(String) COMMENT 'Статус обработки: pending/uploaded/failed_sync', `error_stage` LowCardinality(String) COMMENT 'Этап ошибки: sync, пусто при успехе', `error_message` String COMMENT 'Текст ошибки на этапе синхронизации', `meta_source` LowCardinality(String) COMMENT 'Идентификатор DAG', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Момент вставки в staging (UTC)') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/fdr_express_analysis_docx_transfer', '{replica}') ORDER BY (target_date, file_name, file_signature) SETTINGS index_granularity = 8192 COMMENT 'Staging: файловые метаданные FDR Express Analysis, реквизиты S3 и статус синхронизации.'
```
</details>

### `staging`.`fdr_express_analysis_raw_data`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: target_date, file_name
- Primary key: target_date, file_name
- Total rows: 115
- Total bytes: 272344

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| file_name | String |  | 1 | 1 | 0 | Имя исходного DOCX-файла отчета Express Analysis |
| s3_bucket | LowCardinality(String) |  | 0 | 0 | 0 | Имя S3-бакета, где хранится файл отчета |
| s3_key | String |  | 0 | 0 | 0 | Полный ключ (путь) объекта отчета в S3 |
| target_date | Date |  | 1 | 1 | 0 | Дата обработки файла |
| aircraft_type | LowCardinality(String) |  | 0 | 0 | 0 | Тип ВС |
| board_number | LowCardinality(String) |  | 0 | 0 | 0 | Бортовой номер ВС |
| report_date | String |  | 0 | 0 | 0 | Дата отчета (на время завершения отчета) |
| flight_number | LowCardinality(String) |  | 0 | 0 | 0 | Номер рейса |
| pilot_number | String |  | 0 | 0 | 0 | Табельный номер пилота из отчета |
| report_time_interval | String |  | 0 | 0 | 0 | Интервал времени отчета в формате HH:MM-HH:MM |
| route_note | String |  | 0 | 0 | 0 | Примечание по маршруту |
| arm_source_file_path | String |  | 0 | 0 | 0 | Путь к исходному ARM-файлу |
| flight_parameters_json | String |  | 0 | 0 | 0 | JSON с параметрами полета |
| flight_messages_json | String |  | 0 | 0 | 0 | JSON с сообщениями полета |
| regular_parameters_json | String |  | 0 | 0 | 0 | JSON с регулярными параметрами |
| control_points_json | String |  | 0 | 0 | 0 | JSON с контрольными точками |
| load_status | LowCardinality(String) |  | 0 | 0 | 0 | Статус парсинга файла: parsed или failed_parse |
| error_message | String |  | 0 | 0 | 0 | Текст ошибки при failed_parse |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC).          Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.fdr_express_analysis_raw_data (`file_name` String COMMENT 'Имя исходного DOCX-файла отчета Express Analysis', `s3_bucket` LowCardinality(String) COMMENT 'Имя S3-бакета, где хранится файл отчета', `s3_key` String COMMENT 'Полный ключ (путь) объекта отчета в S3', `target_date` Date COMMENT 'Дата обработки файла', `aircraft_type` LowCardinality(String) COMMENT 'Тип ВС', `board_number` LowCardinality(String) COMMENT 'Бортовой номер ВС', `report_date` String COMMENT 'Дата отчета (на время завершения отчета)', `flight_number` LowCardinality(String) COMMENT 'Номер рейса', `pilot_number` String COMMENT 'Табельный номер пилота из отчета', `report_time_interval` String COMMENT 'Интервал времени отчета в формате HH:MM-HH:MM', `route_note` String COMMENT 'Примечание по маршруту', `arm_source_file_path` String COMMENT 'Путь к исходному ARM-файлу', `flight_parameters_json` String COMMENT 'JSON с параметрами полета', `flight_messages_json` String COMMENT 'JSON с сообщениями полета', `regular_parameters_json` String COMMENT 'JSON с регулярными параметрами', `control_points_json` String COMMENT 'JSON с контрольными точками', `load_status` LowCardinality(String) COMMENT 'Статус парсинга файла: parsed или failed_parse', `error_message` String COMMENT 'Текст ошибки при failed_parse', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC).\r\n        Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/fdr_express_analysis_raw_data', '{replica}') ORDER BY (target_date, file_name) SETTINGS index_granularity = 8192 COMMENT 'Промежуточная таблица для хранения результатов парсинга данных DOCX-файла из S3 для Express Analysis для analytics слоя'
```
</details>

### `staging`.`flight_data_recorders_raw_files`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: processing_date, file_name, file_signature
- Primary key: processing_date, file_name, file_signature
- Total rows: 921
- Total bytes: 73346

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| file_name | String |  | 1 | 1 | 0 | Имя файла (как в источнике) |
| file_created_at | DateTime |  | 0 | 0 | 0 | Дата появления файла (UTC) |
| file_modified_at | DateTime |  | 0 | 0 | 0 | Дата изменения файла (UTC) |
| file_size | UInt64 |  | 0 | 0 | 0 | Размер файла в байтах |
| file_signature | String |  | 1 | 1 | 0 | Подпись файла: hash(name + size + modified_at) |
| source_path | String |  | 0 | 0 | 0 | Полный путь к каталогу/файлу на сетевой шаре |
| processing_date | Date |  | 1 | 1 | 0 | Дата обработки файла (когда файл был забран из источника) |
| file_date | Date |  | 0 | 0 | 0 | Дата файла, извлеченная из имени файла для группировки в S3 |
| s3_bucket | LowCardinality(String) |  | 0 | 0 | 0 | Имя S3 бакета с файлом |
| s3_key | String |  | 0 | 0 | 0 | Ключ объекта в S3 |
| s3_etag | String |  | 0 | 0 | 0 | ETag объекта после загрузки в S3 |
| s3_endpoint_url | LowCardinality(String) |  | 0 | 0 | 0 | Endpoint URL S3-совместимого хранилища |
| s3_region | LowCardinality(String) |  | 0 | 0 | 0 | Регион S3 хранилища |
| s3_url | String |  | 0 | 0 | 0 | Полный URL файла в S3 (s3://bucket/key) |
| load_status | LowCardinality(String) | 'pending' | 0 | 0 | 0 | Текущий статус обработки файла |
| error_message | String |  | 0 | 0 | 0 | Описание ошибки (при наличии) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.flight_data_recorders_raw_files (`file_name` String COMMENT 'Имя файла (как в источнике)', `file_created_at` DateTime COMMENT 'Дата появления файла (UTC)', `file_modified_at` DateTime COMMENT 'Дата изменения файла (UTC)', `file_size` UInt64 COMMENT 'Размер файла в байтах', `file_signature` String COMMENT 'Подпись файла: hash(name + size + modified_at)', `source_path` String COMMENT 'Полный путь к каталогу/файлу на сетевой шаре', `processing_date` Date COMMENT 'Дата обработки файла (когда файл был забран из источника)', `file_date` Date COMMENT 'Дата файла, извлеченная из имени файла для группировки в S3', `s3_bucket` LowCardinality(String) COMMENT 'Имя S3 бакета с файлом', `s3_key` String COMMENT 'Ключ объекта в S3', `s3_etag` String COMMENT 'ETag объекта после загрузки в S3', `s3_endpoint_url` LowCardinality(String) COMMENT 'Endpoint URL S3-совместимого хранилища', `s3_region` LowCardinality(String) COMMENT 'Регион S3 хранилища', `s3_url` String COMMENT 'Полный URL файла в S3 (s3://bucket/key)', `load_status` LowCardinality(String) DEFAULT 'pending' COMMENT 'Текущий статус обработки файла', `error_message` String COMMENT 'Описание ошибки (при наличии)', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/flight_data_recorders_raw_files', '{replica}') ORDER BY (processing_date, file_name, file_signature) SETTINGS index_granularity = 8192 COMMENT 'Промежуточная таблица с манифестом файлов полетных данных и статусами обработки.'
```
</details>

### `staging`.`fuel_efficiency_cfp_data`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: processing_date, file_name
- Primary key: processing_date, file_name
- Total rows: 134
- Total bytes: 404297

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| file_name | String |  | 1 | 1 | 0 | Имя файла бортового самописца (FDR) — ключ связки с телеметрией |
| bort | String |  | 0 | 0 | 0 | Бортовой номер ВС, с которым связан план полёта |
| flight_number | String |  | 0 | 0 | 0 | Номер рейса по расписанию |
| leg_number | Nullable(Int32) |  | 0 | 0 | 0 | Порядковый номер участка (этапа) составного рейса |
| departure_dt | DateTime |  | 0 | 0 | 0 | Плановые дата и время вылета (UTC) |
| arrival_dt | DateTime |  | 0 | 0 | 0 | Плановые дата и время прилёта (UTC) |
| kvs_fio | String |  | 0 | 0 | 0 | Фамилия, имя, отчество командира воздушного судна (КВС) |
| kvs_id | Nullable(Int32) |  | 0 | 0 | 0 | Табельный номер КВС в системе кадрового учёта |
| department | String |  | 0 | 0 | 0 | Лётный отряд (подразделение), к которому приписан КВС |
| departure_airport | Nullable(String) |  | 0 | 0 | 0 | Код аэропорта вылета (IATA) |
| arrival_airport | Nullable(String) |  | 0 | 0 | 0 | Код аэропорта прилёта (IATA) |
| meridian_flight_id | Nullable(Float64) |  | 0 | 0 | 0 | Уникальный идентификатор полёта в системе Meridian |
| cfp_xml | Nullable(String) |  | 0 | 0 | 0 | Плановый полётный план в формате XML из системы CFP Meridian |
| cfp_estimated_tow | Nullable(Float64) |  | 0 | 0 | 0 | Расчётная взлётная масса ВС (кг) по плану CFP |
| cfp_fuel_on_trip | Nullable(Float64) |  | 0 | 0 | 0 | Плановый расход топлива на маршрут от взлёта до посадки (кг)      по плану CFP |
| cfp_fuel_on_board | Nullable(Float64) |  | 0 | 0 | 0 | Плановое количество топлива на борту при вылете (кг)      по плану CFP |
| cfp_fuel_extra | Nullable(Float64) |  | 0 | 0 | 0 | Дополнительное топливо сверх нормативного запаса (кг)      по плану CFP |
| cfp_fuel_required | Nullable(Float64) |  | 0 | 0 | 0 | Минимально требуемое топливо для безопасного выполнения      рейса (кг) по плану CFP |
| cfp_trip_duration_sec | Nullable(Float64) |  | 0 | 0 | 0 | Плановое время полёта от взлёта до посадки (секунды)      по плану CFP |
| cfp_port_fueling | Nullable(Float64) |  | 0 | 0 | 0 | Плановая дозаправка в аэропорту назначения (кг) по плану CFP |
| tankering_extra_kg | Nullable(Float64) |  | 0 | 0 | 0 | Объём дополнительного танкеринга (кг) — топливо, взятое сверх      нормы для экономии на следующем этапе |
| tankering_profit_rub | Nullable(Int32) |  | 0 | 0 | 0 | Экономический эффект от танкеринга (руб.) — разница стоимости      топлива между аэропортами |
| processing_date | Date |  | 1 | 1 | 0 | Дата, за которую выполнялась обработка |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор DAG, загрузившего строку. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.fuel_efficiency_cfp_data (`file_name` String COMMENT 'Имя файла бортового самописца (FDR) — ключ связки с телеметрией', `bort` String COMMENT 'Бортовой номер ВС, с которым связан план полёта', `flight_number` String COMMENT 'Номер рейса по расписанию', `leg_number` Nullable(Int32) COMMENT 'Порядковый номер участка (этапа) составного рейса', `departure_dt` DateTime COMMENT 'Плановые дата и время вылета (UTC)', `arrival_dt` DateTime COMMENT 'Плановые дата и время прилёта (UTC)', `kvs_fio` String COMMENT 'Фамилия, имя, отчество командира воздушного судна (КВС)', `kvs_id` Nullable(Int32) COMMENT 'Табельный номер КВС в системе кадрового учёта', `department` String COMMENT 'Лётный отряд (подразделение), к которому приписан КВС', `departure_airport` Nullable(String) COMMENT 'Код аэропорта вылета (IATA)', `arrival_airport` Nullable(String) COMMENT 'Код аэропорта прилёта (IATA)', `meridian_flight_id` Nullable(Float64) COMMENT 'Уникальный идентификатор полёта в системе Meridian', `cfp_xml` Nullable(String) COMMENT 'Плановый полётный план в формате XML из системы CFP Meridian', `cfp_estimated_tow` Nullable(Float64) COMMENT 'Расчётная взлётная масса ВС (кг) по плану CFP', `cfp_fuel_on_trip` Nullable(Float64) COMMENT 'Плановый расход топлива на маршрут от взлёта до посадки (кг)\r\n    по плану CFP', `cfp_fuel_on_board` Nullable(Float64) COMMENT 'Плановое количество топлива на борту при вылете (кг)\r\n    по плану CFP', `cfp_fuel_extra` Nullable(Float64) COMMENT 'Дополнительное топливо сверх нормативного запаса (кг)\r\n    по плану CFP', `cfp_fuel_required` Nullable(Float64) COMMENT 'Минимально требуемое топливо для безопасного выполнения\r\n    рейса (кг) по плану CFP', `cfp_trip_duration_sec` Nullable(Float64) COMMENT 'Плановое время полёта от взлёта до посадки (секунды)\r\n    по плану CFP', `cfp_port_fueling` Nullable(Float64) COMMENT 'Плановая дозаправка в аэропорту назначения (кг) по плану CFP', `tankering_extra_kg` Nullable(Float64) COMMENT 'Объём дополнительного танкеринга (кг) — топливо, взятое сверх\r\n    нормы для экономии на следующем этапе', `tankering_profit_rub` Nullable(Int32) COMMENT 'Экономический эффект от танкеринга (руб.) — разница стоимости\r\n    топлива между аэропортами', `processing_date` Date COMMENT 'Дата, за которую выполнялась обработка', `meta_source` LowCardinality(String) COMMENT 'Идентификатор DAG, загрузившего строку. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/fuel_efficiency_cfp_data', '{replica}') ORDER BY (processing_date, file_name) SETTINGS index_granularity = 8192 COMMENT 'Плановые данные CFP со связкой по файлу'
```
</details>

### `staging`.`fuel_efficiency_flight_data`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: processing_date, file_name, board_number, aircraft_type, flight_time_seconds
- Primary key: processing_date, file_name, board_number, aircraft_type, flight_time_seconds
- Total rows: 10480745
- Total bytes: 147289902

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| file_name | String |  | 1 | 1 | 0 | Имя файла бортового самописца (FDR) — ключ связки      телеметрии с планом CFP |
| aircraft_type | LowCardinality(String) |  | 1 | 1 | 0 | Тип воздушного судна (семейство ВС, например A320, B737) |
| board_number | LowCardinality(String) |  | 1 | 1 | 0 | Бортовой регистрационный номер воздушного судна |
| flight_time_seconds | Float64 |  | 1 | 1 | 0 | Секунды от начала суток — используется как ось времени      внутри файла |
| barometric_altitude | Float64 |  | 0 | 0 | 0 | Барометрическая высота полёта (футы) |
| is_gear_uncompressed | UInt8 |  | 0 | 0 | 0 | 1 — шасси в воздухе (нет обжатия стойки), 0 — ВС на земле |
| amount_of_fuel_in_tanks | Float64 |  | 0 | 0 | 0 | Суммарное количество топлива во всех баках (кг) |
| is_on_echelon | UInt8 |  | 0 | 0 | 0 | 1 — ВС находится на плановом эшелоне крейсерского полёта,      0 — вне эшелона |
| processing_date | Date |  | 1 | 1 | 0 | Дата, за которую выполнялась обработка файла |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор DAG, загрузившего строку. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.fuel_efficiency_flight_data (`file_name` String COMMENT 'Имя файла бортового самописца (FDR) — ключ связки\r\n    телеметрии с планом CFP', `aircraft_type` LowCardinality(String) COMMENT 'Тип воздушного судна (семейство ВС, например A320, B737)', `board_number` LowCardinality(String) COMMENT 'Бортовой регистрационный номер воздушного судна', `flight_time_seconds` Float64 COMMENT 'Секунды от начала суток — используется как ось времени\r\n    внутри файла', `barometric_altitude` Float64 COMMENT 'Барометрическая высота полёта (футы)', `is_gear_uncompressed` UInt8 COMMENT '1 — шасси в воздухе (нет обжатия стойки), 0 — ВС на земле', `amount_of_fuel_in_tanks` Float64 COMMENT 'Суммарное количество топлива во всех баках (кг)', `is_on_echelon` UInt8 COMMENT '1 — ВС находится на плановом эшелоне крейсерского полёта,\r\n    0 — вне эшелона', `processing_date` Date COMMENT 'Дата, за которую выполнялась обработка файла', `meta_source` LowCardinality(String) COMMENT 'Идентификатор DAG, загрузившего строку. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/fuel_efficiency_flight_data', '{replica}') ORDER BY (processing_date, file_name, board_number, aircraft_type, flight_time_seconds) SETTINGS index_granularity = 8192 COMMENT 'Параметры полёта в унифицированном виде'
```
</details>

### `staging`.`fuel_efficiency_flight_data_smoothed_fuel`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: processing_date, file_name, board_number, aircraft_type, flight_time_seconds
- Primary key: processing_date, file_name, board_number, aircraft_type, flight_time_seconds
- Total rows: 10480745
- Total bytes: 225230264

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| file_name | String |  | 1 | 1 | 0 | Имя файла бортового самописца (FDR) — ключ связки      телеметрии с планом CFP |
| aircraft_type | LowCardinality(String) |  | 1 | 1 | 0 | Тип воздушного судна (семейство ВС,      например A320, B737) |
| board_number | LowCardinality(String) |  | 1 | 1 | 0 | Бортовой регистрационный номер воздушного судна |
| flight_time_seconds | Float64 |  | 1 | 1 | 0 | Секунды от начала суток — используется как ось      времени внутри файла |
| barometric_altitude | Float64 |  | 0 | 0 | 0 | Барометрическая высота полёта (футы) |
| is_gear_uncompressed | UInt8 |  | 0 | 0 | 0 | 1 — шасси в воздухе (нет обжатия стойки), 0 —      ВС на земле |
| amount_of_fuel_in_tanks | Float64 |  | 0 | 0 | 0 | Суммарное количество топлива во всех баках (кг) |
| smoothed_amount_of_fuel_in_tanks | Float64 |  | 0 | 0 | 0 | Сглаженное количество топлива (кг) — применяется      LOWESS или изотонная регрессия для устранения шумов датчика |
| is_on_echelon | UInt8 |  | 0 | 0 | 0 | 1 — ВС находится на плановом эшелоне крейсерского      полёта, 0 — вне эшелона |
| processing_date | Date |  | 1 | 1 | 0 | Дата, за которую выполнялась обработка файла |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор DAG, загрузившего строку.      Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC).      Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.fuel_efficiency_flight_data_smoothed_fuel (`file_name` String COMMENT 'Имя файла бортового самописца (FDR) — ключ связки\r\n    телеметрии с планом CFP', `aircraft_type` LowCardinality(String) COMMENT 'Тип воздушного судна (семейство ВС,\r\n    например A320, B737)', `board_number` LowCardinality(String) COMMENT 'Бортовой регистрационный номер воздушного судна', `flight_time_seconds` Float64 COMMENT 'Секунды от начала суток — используется как ось\r\n    времени внутри файла', `barometric_altitude` Float64 COMMENT 'Барометрическая высота полёта (футы)', `is_gear_uncompressed` UInt8 COMMENT '1 — шасси в воздухе (нет обжатия стойки), 0 —\r\n    ВС на земле', `amount_of_fuel_in_tanks` Float64 COMMENT 'Суммарное количество топлива во всех баках (кг)', `smoothed_amount_of_fuel_in_tanks` Float64 COMMENT 'Сглаженное количество топлива (кг) — применяется\r\n    LOWESS или изотонная регрессия для устранения шумов датчика', `is_on_echelon` UInt8 COMMENT '1 — ВС находится на плановом эшелоне крейсерского\r\n    полёта, 0 — вне эшелона', `processing_date` Date COMMENT 'Дата, за которую выполнялась обработка файла', `meta_source` LowCardinality(String) COMMENT 'Идентификатор DAG, загрузившего строку.\r\n    Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC).\r\n    Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/fuel_efficiency_flight_data_smoothed_fuel', '{replica}') ORDER BY (processing_date, file_name, board_number, aircraft_type, flight_time_seconds) SETTINGS index_granularity = 8192 COMMENT 'Параметры полёта со сглаженным топливом'
```
</details>

### `staging`.`lime_survey_answers_after_flight`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: id
- Primary key: id
- Total rows: 846
- Total bytes: 49811

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id | UInt32 |  | 1 | 1 | 0 | Уникальный идентификатор ответа в LimeSurvey |
| start_dt | DateTime |  | 0 | 0 | 0 | Момент начала заполнения анкеты |
| submit_dt | Nullable(DateTime) |  | 0 | 0 | 0 | Момент окончания заполнения анкеты |
| datestamp | DateTime |  | 0 | 0 | 0 | Момент отправки анкеты |
| email | Nullable(String) |  | 0 | 0 | 0 | Адрес электронной почты респондента |
| flight_number | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Номер рейса, указанный респондентом (например: UT-123) |
| flight_date | Nullable(String) |  | 0 | 0 | 0 | Дата рейса в строковом формате источника (например: «12января2024») |
| seat | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Номер места на борту |
| ticket | Nullable(String) |  | 0 | 0 | 0 | Номер авиабилета пассажира |
| departure_airport | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Город аэропорта вылета |
| arrival_airport | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Город аэропорта прилета |
| gender | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Пол респондента |
| birthday | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Дата рождения в строковом формате источника |
| flight_rating | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Общая оценка перелёта (строка, 0–5) |
| website_rating | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Оценка веб-сайта Utair (строка, 0–5) |
| airport_rating | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Оценка сервиса в аэропорту (строка, 0–5) |
| board_rating | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Оценка сервиса на борту (строка, 0–5) |
| ticket_purchase | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Покупал билет через сайт: Y — да, NULL — нет |
| addon_services | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Оформлял дополнительные услуги через сайт: Y — да, NULL — нет |
| checkin | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Проходил регистрацию на рейс через сайт: Y — да, NULL — нет |
| other1 | Nullable(String) |  | 0 | 0 | 0 | Свободный комментарий о действиях на сайте и в приложении |
| with_ticket | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Доп. услуга? оформлена при покупке билета: Y — да, NULL — нет |
| after_purchase | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Доп. услуга? оформлена после покупки билета: Y — да, NULL — нет |
| online_checkin | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Доп. услуга? оформлена на онлайн-регистрации: Y — да, NULL — нет |
| at_airport | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Доп. услуга? оформлена в аэропорту: Y — да, NULL — нет |
| convenience_1 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Удобство использования 1: Y — отмечено, NULL — нет |
| info_completeness_1 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Полнота информации 1: Y — отмечено, NULL — нет |
| miles_promo_use_1 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Возможность применить мили/промокод 1: Y — отмечено, NULL — нет |
| convenience_2 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Удобство использования 2: Y — отмечено, NULL — нет |
| info_completeness_2 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Полнота информации 2: Y — отмечено, NULL — нет |
| miles_promo_use_2 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Возможность применить мили/промокод 2: Y — отмечено, NULL — нет |
| convenience_3 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Удобство использования 3: Y — отмечено, NULL — нет |
| info_completeness_3 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Полнота информации 3: Y — отмечено, NULL — нет |
| miles_promo_use_3 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Возможность применить мили/промокод 3: Y — отмечено, NULL — нет |
| self_checkin | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Прошёл самостоятельную онлайн-регистрацию: Y — да, NULL — нет |
| counter_checkin | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Прошёл регистрацию на стойке в аэропорту: Y — да, NULL — нет |
| boarding_area | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Воспользовался зоной выхода на посадку: Y — да, NULL — нет |
| other2 | Nullable(String) |  | 0 | 0 | 0 | Свободный комментарий о сервисе в аэропорте |
| print_bp_self | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Распечатал посадочный талон самостоятельно: Y — да, NULL — нет |
| mobile_bp_scan | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Прошёл досмотр с экрана мобильного устройства: Y — да, NULL — нет |
| no_options | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Не воспользовался ни одной из перечисленных возможностей: Y — да, NULL — нет |
| staff_communication | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил коммуникацию и доброжелательность персонала аэропорта: Y — да, NULL — нет |
| baggage_rules_clarity | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил понятность правил провоза багажа: Y — да, NULL — нет |
| addon_service_speed | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил скорость и удобство оформления дополнительных услуг: Y — да, NULL — нет |
| gate_info | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил информирование о номере выхода на посадку и времени посадки: Y — да, NULL — нет |
| display_info_quality | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил актуальность и наглядность информации на табло: Y — да, NULL — нет |
| cabin_condition | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил состояние салона самолёта: Y — да, NULL — нет |
| onboard_info | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил качество информирования пассажиров на борту: Y — да, NULL — нет |
| cabin_crew | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил работу бортпроводников: Y — да, NULL — нет |
| other3 | Nullable(String) |  | 0 | 0 | 0 | Свободный комментарий о качестве сервиса на борту |
| seat_equipment_condition | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил исправность кресел, подлокотников, столиков и освещения: Y — да, NULL — нет |
| cleanliness | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил чистоту салона: Y — да, NULL — нет |
| captain_announcements | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил объявления командира воздушного судна о полёте: Y — да, NULL — нет |
| pa_clarity | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил громкость и чёткость объявлений бортпроводников: Y — да, NULL — нет |
| info_clarity | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил понятность информации о правилах поведения на борту: Y — да, NULL — нет |
| service_info | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил информирование о доступных услугах (бесплатных и платных): Y — да, NULL — нет |
| responsiveness | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил отзывчивость бортпроводников: Y — да, NULL — нет |
| politeness | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил вежливость бортпроводников: Y — да, NULL — нет |
| issue_handling | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил реагирование бортпроводников на нестандартные ситуации: Y — да, NULL — нет |
| open_feedback | Nullable(String) |  | 0 | 0 | 0 | Развёрнутый отзыв пассажира о перелёте в свободной форме |
| processing_dt | DateTime |  | 0 | 0 | 0 | Дата и время обработки записи в DAG (UTC) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Момент загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.lime_survey_answers_after_flight (`id` UInt32 COMMENT 'Уникальный идентификатор ответа в LimeSurvey', `start_dt` DateTime COMMENT 'Момент начала заполнения анкеты', `submit_dt` Nullable(DateTime) COMMENT 'Момент окончания заполнения анкеты', `datestamp` DateTime COMMENT 'Момент отправки анкеты', `email` Nullable(String) COMMENT 'Адрес электронной почты респондента', `flight_number` LowCardinality(Nullable(String)) COMMENT 'Номер рейса, указанный респондентом (например: UT-123)', `flight_date` Nullable(String) COMMENT 'Дата рейса в строковом формате источника (например: «12января2024»)', `seat` LowCardinality(Nullable(String)) COMMENT 'Номер места на борту', `ticket` Nullable(String) COMMENT 'Номер авиабилета пассажира', `departure_airport` LowCardinality(Nullable(String)) COMMENT 'Город аэропорта вылета', `arrival_airport` LowCardinality(Nullable(String)) COMMENT 'Город аэропорта прилета', `gender` LowCardinality(Nullable(String)) COMMENT 'Пол респондента', `birthday` LowCardinality(Nullable(String)) COMMENT 'Дата рождения в строковом формате источника', `flight_rating` LowCardinality(Nullable(String)) COMMENT 'Общая оценка перелёта (строка, 0–5)', `website_rating` LowCardinality(Nullable(String)) COMMENT 'Оценка веб-сайта Utair (строка, 0–5)', `airport_rating` LowCardinality(Nullable(String)) COMMENT 'Оценка сервиса в аэропорту (строка, 0–5)', `board_rating` LowCardinality(Nullable(String)) COMMENT 'Оценка сервиса на борту (строка, 0–5)', `ticket_purchase` LowCardinality(Nullable(String)) COMMENT 'Покупал билет через сайт: Y — да, NULL — нет', `addon_services` LowCardinality(Nullable(String)) COMMENT 'Оформлял дополнительные услуги через сайт: Y — да, NULL — нет', `checkin` LowCardinality(Nullable(String)) COMMENT 'Проходил регистрацию на рейс через сайт: Y — да, NULL — нет', `other1` Nullable(String) COMMENT 'Свободный комментарий о действиях на сайте и в приложении' CODEC(ZSTD(3)), `with_ticket` LowCardinality(Nullable(String)) COMMENT 'Доп. услуга? оформлена при покупке билета: Y — да, NULL — нет', `after_purchase` LowCardinality(Nullable(String)) COMMENT 'Доп. услуга? оформлена после покупки билета: Y — да, NULL — нет', `online_checkin` LowCardinality(Nullable(String)) COMMENT 'Доп. услуга? оформлена на онлайн-регистрации: Y — да, NULL — нет', `at_airport` LowCardinality(Nullable(String)) COMMENT 'Доп. услуга? оформлена в аэропорту: Y — да, NULL — нет', `convenience_1` LowCardinality(Nullable(String)) COMMENT 'Удобство использования 1: Y — отмечено, NULL — нет', `info_completeness_1` LowCardinality(Nullable(String)) COMMENT 'Полнота информации 1: Y — отмечено, NULL — нет', `miles_promo_use_1` LowCardinality(Nullable(String)) COMMENT 'Возможность применить мили/промокод 1: Y — отмечено, NULL — нет', `convenience_2` LowCardinality(Nullable(String)) COMMENT 'Удобство использования 2: Y — отмечено, NULL — нет', `info_completeness_2` LowCardinality(Nullable(String)) COMMENT 'Полнота информации 2: Y — отмечено, NULL — нет', `miles_promo_use_2` LowCardinality(Nullable(String)) COMMENT 'Возможность применить мили/промокод 2: Y — отмечено, NULL — нет', `convenience_3` LowCardinality(Nullable(String)) COMMENT 'Удобство использования 3: Y — отмечено, NULL — нет', `info_completeness_3` LowCardinality(Nullable(String)) COMMENT 'Полнота информации 3: Y — отмечено, NULL — нет', `miles_promo_use_3` LowCardinality(Nullable(String)) COMMENT 'Возможность применить мили/промокод 3: Y — отмечено, NULL — нет', `self_checkin` LowCardinality(Nullable(String)) COMMENT 'Прошёл самостоятельную онлайн-регистрацию: Y — да, NULL — нет', `counter_checkin` LowCardinality(Nullable(String)) COMMENT 'Прошёл регистрацию на стойке в аэропорту: Y — да, NULL — нет', `boarding_area` LowCardinality(Nullable(String)) COMMENT 'Воспользовался зоной выхода на посадку: Y — да, NULL — нет', `other2` Nullable(String) COMMENT 'Свободный комментарий о сервисе в аэропорте' CODEC(ZSTD(3)), `print_bp_self` LowCardinality(Nullable(String)) COMMENT 'Распечатал посадочный талон самостоятельно: Y — да, NULL — нет', `mobile_bp_scan` LowCardinality(Nullable(String)) COMMENT 'Прошёл досмотр с экрана мобильного устройства: Y — да, NULL — нет', `no_options` LowCardinality(Nullable(String)) COMMENT 'Не воспользовался ни одной из перечисленных возможностей: Y — да, NULL — нет', `staff_communication` LowCardinality(Nullable(String)) COMMENT 'Отметил коммуникацию и доброжелательность персонала аэропорта: Y — да, NULL — нет', `baggage_rules_clarity` LowCardinality(Nullable(String)) COMMENT 'Отметил понятность правил провоза багажа: Y — да, NULL — нет', `addon_service_speed` LowCardinality(Nullable(String)) COMMENT 'Отметил скорость и удобство оформления дополнительных услуг: Y — да, NULL — нет', `gate_info` LowCardinality(Nullable(String)) COMMENT 'Отметил информирование о номере выхода на посадку и времени посадки: Y — да, NULL — нет', `display_info_quality` LowCardinality(Nullable(String)) COMMENT 'Отметил актуальность и наглядность информации на табло: Y — да, NULL — нет', `cabin_condition` LowCardinality(Nullable(String)) COMMENT 'Отметил состояние салона самолёта: Y — да, NULL — нет', `onboard_info` LowCardinality(Nullable(String)) COMMENT 'Отметил качество информирования пассажиров на борту: Y — да, NULL — нет', `cabin_crew` LowCardinality(Nullable(String)) COMMENT 'Отметил работу бортпроводников: Y — да, NULL — нет', `other3` Nullable(String) COMMENT 'Свободный комментарий о качестве сервиса на борту' CODEC(ZSTD(3)), `seat_equipment_condition` LowCardinality(Nullable(String)) COMMENT 'Отметил исправность кресел, подлокотников, столиков и освещения: Y — да, NULL — нет', `cleanliness` LowCardinality(Nullable(String)) COMMENT 'Отметил чистоту салона: Y — да, NULL — нет', `captain_announcements` LowCardinality(Nullable(String)) COMMENT 'Отметил объявления командира воздушного судна о полёте: Y — да, NULL — нет', `pa_clarity` LowCardinality(Nullable(String)) COMMENT 'Отметил громкость и чёткость объявлений бортпроводников: Y — да, NULL — нет', `info_clarity` LowCardinality(Nullable(String)) COMMENT 'Отметил понятность информации о правилах поведения на борту: Y — да, NULL — нет', `service_info` LowCardinality(Nullable(String)) COMMENT 'Отметил информирование о доступных услугах (бесплатных и платных): Y — да, NULL — нет', `responsiveness` LowCardinality(Nullable(String)) COMMENT 'Отметил отзывчивость бортпроводников: Y — да, NULL — нет', `politeness` LowCardinality(Nullable(String)) COMMENT 'Отметил вежливость бортпроводников: Y — да, NULL — нет', `issue_handling` LowCardinality(Nullable(String)) COMMENT 'Отметил реагирование бортпроводников на нестандартные ситуации: Y — да, NULL — нет', `open_feedback` Nullable(String) COMMENT 'Развёрнутый отзыв пассажира о перелёте в свободной форме' CODEC(ZSTD(3)), `processing_dt` DateTime COMMENT 'Дата и время обработки записи в DAG (UTC)', `meta_source` LowCardinality(String) COMMENT 'Источник. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Момент загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/lime_survey_answers_after_flight', '{replica}') ORDER BY id SETTINGS index_granularity = 8192 COMMENT 'Промежуточная таблица с ответами на опросы LimeSurvey.'
```
</details>

### `staging`.`mongo_monoapp_cart`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: updated_at, id_document
- Primary key: updated_at, id_document
- Total rows: 107578
- Total bytes: 12346455

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id_document | String |  | 1 | 1 | 0 | ID документа рейса (MongoDD _id) |
| document | String |  | 0 | 0 | 0 | Исходный mongo-документ в виде JSON-строки |
| updated_at | DateTime |  | 1 | 1 | 0 | Время последнего обновления документа (дата выполнения дага) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи.Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC).Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.mongo_monoapp_cart (`id_document` String COMMENT 'ID документа рейса (MongoDD _id)', `document` String COMMENT 'Исходный mongo-документ в виде JSON-строки', `updated_at` DateTime COMMENT 'Время последнего обновления документа (дата выполнения дага)', `meta_source` LowCardinality(String) COMMENT 'Источник записи.Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC).Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/mongo_monoapp_cart', '{replica}') ORDER BY (updated_at, id_document) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения промежуточных данных из cart перед вставкой в source слой'
```
</details>

### `staging`.`mongo_monoapp_sirena_flights`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: updated_at, id_document
- Primary key: updated_at, id_document
- Total rows: 365
- Total bytes: 3131959

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id_document | String |  | 1 | 1 | 0 | ID документа рейса (MongoDD _id) |
| document | String |  | 0 | 0 | 0 | Исходный mongo-документ в виде JSON-строки |
| updated_at | DateTime64(3, 'UTC') |  | 1 | 1 | 1 | Время последнего обновления документа (дата выполнения дага) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.mongo_monoapp_sirena_flights (`id_document` String COMMENT 'ID документа рейса (MongoDD _id)', `document` String COMMENT 'Исходный mongo-документ в виде JSON-строки', `updated_at` DateTime64(3, 'UTC') COMMENT 'Время последнего обновления документа (дата выполнения дага)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/mongo_monoapp_sirena_flights', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (updated_at, id_document) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения промежуточных данных из sirena_flights перед вставкой в source слой'
```
</details>

### `staging`.`sofi_bi_sales_ut`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(transaction_date)
- Sorting key: document_number
- Primary key: document_number
- Total rows: 20417
- Total bytes: 1315207

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| document_number | String |  | 1 | 1 | 0 | Номер документа (бланка) |
| coupon_number | UInt8 |  | 0 | 0 | 0 | Номер купона |
| document_kind | LowCardinality(String) |  | 0 | 0 | 0 | Вид операции (бланка) |
| carrier | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Перевозчик |
| flight_number | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Номер рейса |
| origin | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Пункт вылета |
| destination | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Пункт прилета |
| agency_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Код агента |
| agency_name | LowCardinality(String) |  | 0 | 0 | 0 | Наименование агента |
| agency_number_code | String |  | 0 | 0 | 0 | Пункт продажи (валидатор) |
| document_coupon_count | UInt8 |  | 0 | 0 | 0 | Количество купонов документа |
| sale_segment_count | Int8 |  | 0 | 0 | 0 | Количество проданных сегментов |
| service_class | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Класс бронирования |
| fare_basis | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Вид тарифа |
| tax_rub_cpfp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Тариф на купоне РУБ |
| tax_rub_yq_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Топливный сбор РУБ |
| tax_rub_zz_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Такса ТКП (ZZ) РУБ |
| tax_rub_cp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Штраф за возврат(обмен) (CP) РУБ |
| tax_rub_ru_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор RU РУБ |
| tax_rub_psyr_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор за бланк (PS,YR) РУБ |
| tax_rub_another_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Прочие сборы РУБ |
| currency | LowCardinality(String) |  | 0 | 0 | 0 | Валюта |
| tax_cpfp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Тариф на купоне |
| tax_yq_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Топливный сбор |
| tax_zz_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Такса ТКП (ZZ) |
| tax_cp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Штраф за возврат(обмен) (CP) |
| tax_ru_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор RU |
| tax_psyr_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор за бланк (PS,YR) |
| tax_another_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Прочие сборы |
| transaction_date | Date |  | 0 | 0 | 1 | Дата транзакции |
| flight_date | Nullable(Date) |  | 0 | 0 | 0 | Дата вылета |
| gds | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Система бронирования |
| sale_system | LowCardinality(String) |  | 0 | 0 | 0 | Система продажи |
| ff_card_passenger | Nullable(String) |  | 0 | 0 | 0 | Номер карты частолетающего пассажира |
| ff_amount | Int32 |  | 0 | 0 | 0 | Сумма бонусов на купоне (эквив. РУБ) |
| is_exchange | LowCardinality(String) |  | 0 | 0 | 0 | Участвует в обмене? |
| exchange_document_number | Nullable(String) |  | 0 | 0 | 0 | Номер документа (обмен) |
| supplement_currency | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Валюта доплаты |
| supplement_amount | Nullable(Int32) |  | 0 | 0 | 0 | Сумма доплаты |
| supplement_amount_rub | Nullable(Int32) |  | 0 | 0 | 0 | Сумма доплаты РУБ |
| fop_string | String |  | 0 | 0 | 0 | Примечание по ФОП |
| is_rt | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Перевозка туда-обратно |
| is_tr | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Трансферная перевозка |
| brand | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Бренд |
| category_code | LowCardinality(String) |  | 0 | 0 | 0 | Категория пассажира |
| reason_issuance_sub_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Причина выписки купона EMD |
| reason_issuance_sub_grp | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Группа доп.услуг |
| is_ff | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | FF mark |
| tariff_structure | Nullable(String) |  | 0 | 0 | 0 | Структура тарифа |
| pnr | Nullable(String) |  | 0 | 0 | 0 | Номер брони |
| voucher_num | Nullable(String) |  | 0 | 0 | 0 | Номер ваучера |
| return_exchange_voucher_num | Nullable(String) |  | 0 | 0 | 0 | Номера Возвращаемых В Обмен На Ваучер Документов |
| is_return_exchange_voucher | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Возврат в обмен на ваучер |
| vouncher | LowCardinality(String) |  | 0 | 0 | 0 | Ваучер |
| exchange_voucher_usl | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Обмен ваучера на билет/услугу |
| passenger_name | Nullable(String) |  | 0 | 0 | 0 | ФИО пассажира |
| passenger_passport | Nullable(String) |  | 0 | 0 | 0 | Паспорт пассажира |
| passenger_birth_date | Nullable(Date32) |  | 0 | 0 | 0 | День рождения пассажира |
| emd_related_ticket_number | Nullable(String) |  | 0 | 0 | 0 | EMD - связанный документ |
| emd_related_coupon_number | Nullable(UInt8) |  | 0 | 0 | 0 | EMD - номер купона связанного документа |
| source | LowCardinality(String) |  | 0 | 0 | 0 | Источник |
| updated | DateTime64(3, 'UTC') |  | 0 | 0 | 0 | Время последнего обновления документа |
| processing_dt | DateTime('UTC') |  | 0 | 0 | 0 | Дата обработки (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Имя дага |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.sofi_bi_sales_ut (`document_number` String COMMENT 'Номер документа (бланка)', `coupon_number` UInt8 COMMENT 'Номер купона', `document_kind` LowCardinality(String) COMMENT 'Вид операции (бланка)', `carrier` LowCardinality(Nullable(String)) COMMENT 'Перевозчик', `flight_number` LowCardinality(Nullable(String)) COMMENT 'Номер рейса', `origin` LowCardinality(Nullable(String)) COMMENT 'Пункт вылета', `destination` LowCardinality(Nullable(String)) COMMENT 'Пункт прилета', `agency_code` LowCardinality(Nullable(String)) COMMENT 'Код агента', `agency_name` LowCardinality(String) COMMENT 'Наименование агента', `agency_number_code` String COMMENT 'Пункт продажи (валидатор)', `document_coupon_count` UInt8 COMMENT 'Количество купонов документа', `sale_segment_count` Int8 COMMENT 'Количество проданных сегментов', `service_class` LowCardinality(Nullable(String)) COMMENT 'Класс бронирования', `fare_basis` LowCardinality(Nullable(String)) COMMENT 'Вид тарифа', `tax_rub_cpfp_amount` Decimal(18, 4) COMMENT 'Тариф на купоне РУБ', `tax_rub_yq_amount` Decimal(18, 4) COMMENT 'Топливный сбор РУБ', `tax_rub_zz_amount` Decimal(18, 4) COMMENT 'Такса ТКП (ZZ) РУБ', `tax_rub_cp_amount` Decimal(18, 4) COMMENT 'Штраф за возврат(обмен) (CP) РУБ', `tax_rub_ru_amount` Decimal(18, 4) COMMENT 'Сбор RU РУБ', `tax_rub_psyr_amount` Decimal(18, 4) COMMENT 'Сбор за бланк (PS,YR) РУБ', `tax_rub_another_amount` Decimal(18, 4) COMMENT 'Прочие сборы РУБ', `currency` LowCardinality(String) COMMENT 'Валюта', `tax_cpfp_amount` Decimal(18, 4) COMMENT 'Тариф на купоне', `tax_yq_amount` Decimal(18, 4) COMMENT 'Топливный сбор', `tax_zz_amount` Decimal(18, 4) COMMENT 'Такса ТКП (ZZ)', `tax_cp_amount` Decimal(18, 4) COMMENT 'Штраф за возврат(обмен) (CP)', `tax_ru_amount` Decimal(18, 4) COMMENT 'Сбор RU', `tax_psyr_amount` Decimal(18, 4) COMMENT 'Сбор за бланк (PS,YR)', `tax_another_amount` Decimal(18, 4) COMMENT 'Прочие сборы', `transaction_date` Date COMMENT 'Дата транзакции', `flight_date` Nullable(Date) COMMENT 'Дата вылета', `gds` LowCardinality(Nullable(String)) COMMENT 'Система бронирования', `sale_system` LowCardinality(String) COMMENT 'Система продажи', `ff_card_passenger` Nullable(String) COMMENT 'Номер карты частолетающего пассажира', `ff_amount` Int32 COMMENT 'Сумма бонусов на купоне (эквив. РУБ)', `is_exchange` LowCardinality(String) COMMENT 'Участвует в обмене?', `exchange_document_number` Nullable(String) COMMENT 'Номер документа (обмен)', `supplement_currency` LowCardinality(Nullable(String)) COMMENT 'Валюта доплаты', `supplement_amount` Nullable(Int32) COMMENT 'Сумма доплаты', `supplement_amount_rub` Nullable(Int32) COMMENT 'Сумма доплаты РУБ', `fop_string` String COMMENT 'Примечание по ФОП' CODEC(ZSTD(3)), `is_rt` LowCardinality(Nullable(String)) COMMENT 'Перевозка туда-обратно', `is_tr` LowCardinality(Nullable(String)) COMMENT 'Трансферная перевозка', `brand` LowCardinality(Nullable(String)) COMMENT 'Бренд', `category_code` LowCardinality(String) COMMENT 'Категория пассажира', `reason_issuance_sub_code` LowCardinality(Nullable(String)) COMMENT 'Причина выписки купона EMD', `reason_issuance_sub_grp` LowCardinality(Nullable(String)) COMMENT 'Группа доп.услуг', `is_ff` LowCardinality(Nullable(String)) COMMENT 'FF mark', `tariff_structure` Nullable(String) COMMENT 'Структура тарифа', `pnr` Nullable(String) COMMENT 'Номер брони', `voucher_num` Nullable(String) COMMENT 'Номер ваучера', `return_exchange_voucher_num` Nullable(String) COMMENT 'Номера Возвращаемых В Обмен На Ваучер Документов', `is_return_exchange_voucher` LowCardinality(Nullable(String)) COMMENT 'Возврат в обмен на ваучер', `vouncher` LowCardinality(String) COMMENT 'Ваучер', `exchange_voucher_usl` LowCardinality(Nullable(String)) COMMENT 'Обмен ваучера на билет/услугу', `passenger_name` Nullable(String) COMMENT 'ФИО пассажира' CODEC(ZSTD(3)), `passenger_passport` Nullable(String) COMMENT 'Паспорт пассажира' CODEC(ZSTD(3)), `passenger_birth_date` Nullable(Date32) COMMENT 'День рождения пассажира', `emd_related_ticket_number` Nullable(String) COMMENT 'EMD - связанный документ', `emd_related_coupon_number` Nullable(UInt8) COMMENT 'EMD - номер купона связанного документа', `source` LowCardinality(String) COMMENT 'Источник', `updated` DateTime64(3, 'UTC') COMMENT 'Время последнего обновления документа', `processing_dt` DateTime('UTC') COMMENT 'Дата обработки (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Имя дага', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC).') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/sofi_bi_sales_ut', '{replica}') PARTITION BY toYYYYMM(transaction_date) ORDER BY document_number SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из view SOFI_BI.SALES_UT_DATA2021VIEW БД Sofi2021'
```
</details>

### `staging`.`sofi_bi_sales_ut_for_analytics`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(transaction_date)
- Sorting key: document_number
- Primary key: document_number
- Total rows: 20417
- Total bytes: 1340860

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| document_number | String |  | 1 | 1 | 0 | Номер документа (бланка) |
| coupon_number | UInt8 |  | 0 | 0 | 0 | Номер купона |
| document_kind | LowCardinality(String) |  | 0 | 0 | 0 | Вид операции (бланка) |
| carrier | LowCardinality(String) |  | 0 | 0 | 0 | Перевозчик |
| flight_number | LowCardinality(String) |  | 0 | 0 | 0 | Номер рейса; "Отсутствует" если не задан в источнике |
| origin | LowCardinality(String) |  | 0 | 0 | 0 | Пункт вылета; "Отсутствует" если не задан в источнике |
| destination | LowCardinality(String) |  | 0 | 0 | 0 | Пункт прилета; "Отсутствует" если не задан в источнике |
| agency_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Код агента |
| agency_name | LowCardinality(String) |  | 0 | 0 | 0 | Наименование агента |
| agency_number_code | String |  | 0 | 0 | 0 | Пункт продажи (валидатор) |
| document_coupon_count | UInt8 |  | 0 | 0 | 0 | Количество купонов документа |
| sale_segment_count | Int8 |  | 0 | 0 | 0 | Количество проданных сегментов |
| service_class | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Класс бронирования |
| fare_basis | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Вид тарифа |
| tax_rub_cpfp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Тариф на купоне РУБ |
| tax_rub_yq_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Топливный сбор РУБ |
| tax_rub_zz_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Такса ТКП (ZZ) РУБ |
| tax_rub_cp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Штраф за возврат(обмен) (CP) РУБ |
| tax_rub_ru_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор RU РУБ |
| tax_rub_psyr_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор за бланк (PS,YR) РУБ |
| tax_rub_another_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Прочие сборы РУБ |
| currency | LowCardinality(String) |  | 0 | 0 | 0 | Валюта |
| tax_cpfp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Тариф на купоне |
| tax_yq_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Топливный сбор |
| tax_zz_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Такса ТКП (ZZ) |
| tax_cp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Штраф за возврат(обмен) (CP) |
| tax_ru_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор RU |
| tax_psyr_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор за бланк (PS,YR) |
| tax_another_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Прочие сборы |
| transaction_date | Date |  | 0 | 0 | 1 | Дата транзакции |
| flight_date | Date |  | 0 | 0 | 0 | Дата вылета; 1970-01-01 если не задана в источнике |
| gds | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Система бронирования |
| sale_system | LowCardinality(String) |  | 0 | 0 | 0 | Система продажи |
| ff_card_passenger | Nullable(String) |  | 0 | 0 | 0 | Номер карты частолетающего пассажира |
| ff_amount | Int32 |  | 0 | 0 | 0 | Сумма бонусов на купоне (эквив. РУБ) |
| is_exchange | Nullable(Bool) |  | 0 | 0 | 0 | Участвует в обмене? |
| exchange_document_number | Nullable(String) |  | 0 | 0 | 0 | Номер документа (обмен) |
| supplement_currency | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Валюта доплаты |
| supplement_amount | Nullable(Int32) |  | 0 | 0 | 0 | Сумма доплаты |
| supplement_amount_rub | Nullable(Int32) |  | 0 | 0 | 0 | Сумма доплаты РУБ |
| fop_string | String |  | 0 | 0 | 0 | Примечание по ФОП |
| is_rt | Nullable(Bool) |  | 0 | 0 | 0 | Перевозка туда-обратно |
| is_tr | Nullable(Bool) |  | 0 | 0 | 0 | Трансферная перевозка |
| brand | LowCardinality(String) |  | 0 | 0 | 0 | Бренд |
| category_code | LowCardinality(String) |  | 0 | 0 | 0 | Категория пассажира |
| reason_issuance_sub_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Причина выписки купона EMD |
| reason_issuance_sub_grp | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Группа доп.услуг |
| is_ff | Nullable(Bool) |  | 0 | 0 | 0 | Отметка использования бонусов |
| tariff_structure | Nullable(String) |  | 0 | 0 | 0 | Структура тарифа |
| pnr | String |  | 0 | 0 | 0 | Номер брони; "Отсутствует" если не задан в источнике |
| voucher_num | Nullable(String) |  | 0 | 0 | 0 | Номер ваучера |
| return_exchange_voucher_num | Nullable(String) |  | 0 | 0 | 0 | Номера возвращаемых в обмен на ваучер документов |
| is_return_exchange_voucher | Nullable(Bool) |  | 0 | 0 | 0 | Возврат в обмен на ваучер |
| vouncher | LowCardinality(String) |  | 0 | 0 | 0 | Ваучер |
| exchange_voucher_usl | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Обмен ваучера на билет/услугу |
| passenger_name | Nullable(String) |  | 0 | 0 | 0 | ФИО пассажира |
| passenger_passport | Nullable(String) |  | 0 | 0 | 0 | Паспорт пассажира |
| passenger_birth_date | Nullable(Date32) |  | 0 | 0 | 0 | День рождения пассажира |
| emd_related_ticket_number | Nullable(String) |  | 0 | 0 | 0 | EMD - связанный документ |
| emd_related_coupon_number | Nullable(UInt8) |  | 0 | 0 | 0 | EMD - номер купона связанного документа |
| source | LowCardinality(String) |  | 0 | 0 | 0 | Источник |
| updated_dt | DateTime64(3, 'UTC') |  | 0 | 0 | 0 | Время последнего обновления документа |
| processing_dt | DateTime64(3, 'UTC') |  | 0 | 0 | 0 | Дата и время обработки (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Имя дага |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE staging.sofi_bi_sales_ut_for_analytics (`document_number` String COMMENT 'Номер документа (бланка)', `coupon_number` UInt8 COMMENT 'Номер купона', `document_kind` LowCardinality(String) COMMENT 'Вид операции (бланка)', `carrier` LowCardinality(String) COMMENT 'Перевозчик', `flight_number` LowCardinality(String) COMMENT 'Номер рейса; "Отсутствует" если не задан в источнике', `origin` LowCardinality(String) COMMENT 'Пункт вылета; "Отсутствует" если не задан в источнике', `destination` LowCardinality(String) COMMENT 'Пункт прилета; "Отсутствует" если не задан в источнике', `agency_code` LowCardinality(Nullable(String)) COMMENT 'Код агента', `agency_name` LowCardinality(String) COMMENT 'Наименование агента', `agency_number_code` String COMMENT 'Пункт продажи (валидатор)', `document_coupon_count` UInt8 COMMENT 'Количество купонов документа', `sale_segment_count` Int8 COMMENT 'Количество проданных сегментов', `service_class` LowCardinality(Nullable(String)) COMMENT 'Класс бронирования', `fare_basis` LowCardinality(Nullable(String)) COMMENT 'Вид тарифа', `tax_rub_cpfp_amount` Decimal(18, 4) COMMENT 'Тариф на купоне РУБ', `tax_rub_yq_amount` Decimal(18, 4) COMMENT 'Топливный сбор РУБ', `tax_rub_zz_amount` Decimal(18, 4) COMMENT 'Такса ТКП (ZZ) РУБ', `tax_rub_cp_amount` Decimal(18, 4) COMMENT 'Штраф за возврат(обмен) (CP) РУБ', `tax_rub_ru_amount` Decimal(18, 4) COMMENT 'Сбор RU РУБ', `tax_rub_psyr_amount` Decimal(18, 4) COMMENT 'Сбор за бланк (PS,YR) РУБ', `tax_rub_another_amount` Decimal(18, 4) COMMENT 'Прочие сборы РУБ', `currency` LowCardinality(String) COMMENT 'Валюта', `tax_cpfp_amount` Decimal(18, 4) COMMENT 'Тариф на купоне', `tax_yq_amount` Decimal(18, 4) COMMENT 'Топливный сбор', `tax_zz_amount` Decimal(18, 4) COMMENT 'Такса ТКП (ZZ)', `tax_cp_amount` Decimal(18, 4) COMMENT 'Штраф за возврат(обмен) (CP)', `tax_ru_amount` Decimal(18, 4) COMMENT 'Сбор RU', `tax_psyr_amount` Decimal(18, 4) COMMENT 'Сбор за бланк (PS,YR)', `tax_another_amount` Decimal(18, 4) COMMENT 'Прочие сборы', `transaction_date` Date COMMENT 'Дата транзакции', `flight_date` Date COMMENT 'Дата вылета; 1970-01-01 если не задана в источнике', `gds` LowCardinality(Nullable(String)) COMMENT 'Система бронирования', `sale_system` LowCardinality(String) COMMENT 'Система продажи', `ff_card_passenger` Nullable(String) COMMENT 'Номер карты частолетающего пассажира', `ff_amount` Int32 COMMENT 'Сумма бонусов на купоне (эквив. РУБ)', `is_exchange` Nullable(Bool) COMMENT 'Участвует в обмене?', `exchange_document_number` Nullable(String) COMMENT 'Номер документа (обмен)', `supplement_currency` LowCardinality(Nullable(String)) COMMENT 'Валюта доплаты', `supplement_amount` Nullable(Int32) COMMENT 'Сумма доплаты', `supplement_amount_rub` Nullable(Int32) COMMENT 'Сумма доплаты РУБ', `fop_string` String COMMENT 'Примечание по ФОП' CODEC(ZSTD(3)), `is_rt` Nullable(Bool) COMMENT 'Перевозка туда-обратно', `is_tr` Nullable(Bool) COMMENT 'Трансферная перевозка', `brand` LowCardinality(String) COMMENT 'Бренд', `category_code` LowCardinality(String) COMMENT 'Категория пассажира', `reason_issuance_sub_code` LowCardinality(Nullable(String)) COMMENT 'Причина выписки купона EMD', `reason_issuance_sub_grp` LowCardinality(Nullable(String)) COMMENT 'Группа доп.услуг', `is_ff` Nullable(Bool) COMMENT 'Отметка использования бонусов', `tariff_structure` Nullable(String) COMMENT 'Структура тарифа', `pnr` String COMMENT 'Номер брони; "Отсутствует" если не задан в источнике', `voucher_num` Nullable(String) COMMENT 'Номер ваучера', `return_exchange_voucher_num` Nullable(String) COMMENT 'Номера возвращаемых в обмен на ваучер документов', `is_return_exchange_voucher` Nullable(Bool) COMMENT 'Возврат в обмен на ваучер', `vouncher` LowCardinality(String) COMMENT 'Ваучер', `exchange_voucher_usl` LowCardinality(Nullable(String)) COMMENT 'Обмен ваучера на билет/услугу', `passenger_name` Nullable(String) COMMENT 'ФИО пассажира' CODEC(ZSTD(3)), `passenger_passport` Nullable(String) COMMENT 'Паспорт пассажира' CODEC(ZSTD(3)), `passenger_birth_date` Nullable(Date32) COMMENT 'День рождения пассажира', `emd_related_ticket_number` Nullable(String) COMMENT 'EMD - связанный документ', `emd_related_coupon_number` Nullable(UInt8) COMMENT 'EMD - номер купона связанного документа', `source` LowCardinality(String) COMMENT 'Источник', `updated_dt` DateTime64(3, 'UTC') COMMENT 'Время последнего обновления документа', `processing_dt` DateTime64(3, 'UTC') COMMENT 'Дата и время обработки (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Имя дага', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC).', INDEX idx_carrier carrier TYPE set(300) GRANULARITY 1, INDEX idx_flight_number flight_number TYPE set(1000) GRANULARITY 1, INDEX idx_origin origin TYPE set(1000) GRANULARITY 1, INDEX idx_destination destination TYPE set(1000) GRANULARITY 1, INDEX idx_flight_date flight_date TYPE minmax GRANULARITY 4, INDEX idx_pnr pnr TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/staging/sofi_bi_sales_ut_for_analytics', '{replica}') PARTITION BY toYYYYMM(transaction_date) ORDER BY document_number SETTINGS index_granularity = 8192 COMMENT 'Промежуточная таблица для переноса данных продаж sofi_bi из source в analytics слой.'
```
</details>

### `source`.`amos_heli_ac_typ`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: ac_typ, valid_from
- Primary key: ac_typ, valid_from
- Total rows: 255
- Total bytes: 46842

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_type_i | Int32 |  | 0 | 0 | 0 |  |
| ac_typ | String |  | 1 | 1 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| fa_ac_typ | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 1 | 1 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_ac_typ (`ac_type_i` Int32, `ac_typ` String, `description` Nullable(String), `fa_ac_typ` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_ac_typ', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (ac_typ, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из ac_typ вертолетного амос'
```
</details>

### `source`.`amos_heli_address`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: address_i, valid_from
- Primary key: address_i, valid_from
- Total rows: 5646
- Total bytes: 261192

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| address_i | Int32 |  | 1 | 1 | 0 |  |
| vendor | LowCardinality(String) |  | 0 | 0 | 0 |  |
| name | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 1 | 1 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_address (`address_i` Int32, `vendor` LowCardinality(String), `name` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_address', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (address_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из address вертолетного амос'
```
</details>

### `source`.`amos_heli_adr_properties`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: address_i, prop_type_i, valid_from
- Primary key: address_i, prop_type_i, valid_from
- Total rows: 5635
- Total bytes: 130309

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| address_i | Int32 |  | 1 | 1 | 0 |  |
| prop_type_i | Int32 |  | 1 | 1 | 0 |  |
| remarks | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| value | LowCardinality(String) |  | 0 | 0 | 0 |  |
| status | Nullable(Int8) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных ( DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_adr_properties (`address_i` Int32, `prop_type_i` Int32, `remarks` LowCardinality(Nullable(String)), `value` LowCardinality(String), `status` Nullable(Int8), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных ( DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_adr_properties', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (address_i, prop_type_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из adr_properties вертолетного амос'
```
</details>

### `source`.`amos_heli_adr_special`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: special_i, valid_from
- Primary key: special_i, valid_from
- Total rows: 5299
- Total bytes: 223716

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| special_i | Int32 |  | 1 | 1 | 0 |  |
| address_i | Int32 |  | 0 | 0 | 0 |  |
| special | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| remarks | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| amount | Nullable(Int8) |  | 0 | 0 | 0 |  |
| reference_no | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных ( DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_adr_special (`special_i` Int32, `address_i` Int32, `special` LowCardinality(Nullable(String)), `remarks` LowCardinality(Nullable(String)), `amount` Nullable(Int8), `reference_no` LowCardinality(Nullable(String)), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных ( DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_adr_special', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (special_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из adr_special вертолетного амос'
```
</details>

### `source`.`amos_heli_aircraft`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: ac_registr, valid_from
- Primary key: ac_registr, valid_from
- Total rows: 2106
- Total bytes: 136735

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_registr | String |  | 1 | 1 | 0 |  |
| ac_typ | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ac_registr_prefix | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| ac_model | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| owner | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| manual_owner | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| status | Int16 |  | 0 | 0 | 0 |  |
| non_managed | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| homebase | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| object_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| description | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 1 | 1 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_aircraft (`ac_registr` String, `ac_typ` LowCardinality(String), `ac_registr_prefix` LowCardinality(Nullable(String)), `ac_model` LowCardinality(Nullable(String)), `owner` LowCardinality(Nullable(String)), `manual_owner` LowCardinality(Nullable(String)), `status` Int16, `non_managed` LowCardinality(Nullable(String)), `homebase` LowCardinality(Nullable(String)), `object_type` LowCardinality(Nullable(String)), `description` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_aircraft', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (ac_registr, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из aircraft вертолетного амос'
```
</details>

### `source`.`amos_heli_applicability`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: applicabilityno_i, valid_from
- Primary key: applicabilityno_i, valid_from
- Total rows: 2602238
- Total bytes: 18470412

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| applicabilityno_i | Int64 |  | 1 | 1 | 0 |  |
| effectivityno_i | Int32 |  | 0 | 0 | 0 |  |
| applicable | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ref_key | Int64 |  | 0 | 0 | 0 |  |
| ref_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_applicability (`applicabilityno_i` Int64, `effectivityno_i` Int32, `applicable` LowCardinality(String), `ref_key` Int64, `ref_type` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_applicability', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (applicabilityno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из applicability вертолетного амос'
```
</details>

### `source`.`amos_heli_condition`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: condition, valid_from
- Primary key: condition, valid_from
- Total rows: 32
- Total bytes: 7178

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| condition | LowCardinality(String) |  | 1 | 1 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 1 | 1 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_condition (`condition` LowCardinality(String), `description` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_condition', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (condition, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из condition вертолетного амос'
```
</details>

### `source`.`amos_heli_counter`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: counterno_i, valid_from
- Primary key: counterno_i, valid_from
- Total rows: 2978136
- Total bytes: 36872605

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counterno_i | Int64 |  | 1 | 1 | 0 |  |
| counter_templateno_i | Int32 |  | 0 | 0 | 0 |  |
| ref_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ref_key | Int64 |  | 0 | 0 | 0 |  |
| life_value | Nullable(Float64) |  | 0 | 0 | 0 |  |
| is_unknown | LowCardinality(String) |  | 0 | 0 | 0 |  |
| status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| master_counterno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| readout_date | Date32 |  | 0 | 0 | 0 |  |
| readout_time | Int64 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_counter (`counterno_i` Int64, `counter_templateno_i` Int32, `ref_type` LowCardinality(String), `ref_key` Int64, `life_value` Nullable(Float64), `is_unknown` LowCardinality(String), `status` Nullable(Int16), `master_counterno_i` Nullable(Int64), `readout_date` Date32, `readout_time` Int64, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_counter', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (counterno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из counter вертолетного амос'
```
</details>

### `source`.`amos_heli_counter_definition`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: counter_defno_i, valid_from
- Primary key: counter_defno_i, valid_from
- Total rows: 64
- Total bytes: 18268

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counter_defno_i | UInt64 |  | 1 | 1 | 0 |  |
| code | LowCardinality(String) |  | 0 | 0 | 0 |  |
| name | Nullable(String) |  | 0 | 0 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| display_unit | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 1 | 1 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | Дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_counter_definition (`counter_defno_i` UInt64, `code` LowCardinality(String), `name` Nullable(String), `description` Nullable(String), `display_unit` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'Дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_counter_definition', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (counter_defno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из counter_definition вертолетного амос'
```
</details>

### `source`.`amos_heli_counter_template`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: counter_templateno_i, valid_from
- Primary key: counter_templateno_i, valid_from
- Total rows: 558
- Total bytes: 64555

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counter_templateno_i | Int32 |  | 1 | 1 | 0 |  |
| counter_defno_i | Int32 |  | 0 | 0 | 0 |  |
| counter_template_groupno_i | Int32 |  | 0 | 0 | 0 |  |
| type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| is_calculated | LowCardinality(String) |  | 0 | 0 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 1 | 1 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_counter_template (`counter_templateno_i` Int32, `counter_defno_i` Int32, `counter_template_groupno_i` Int32, `type` LowCardinality(String), `is_calculated` LowCardinality(String), `description` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_counter_template', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (counter_templateno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из counter_template вертолетного амос'
```
</details>

### `source`.`amos_heli_counter_value`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: counter_valueno_i, valid_from
- Primary key: counter_valueno_i, valid_from
- Total rows: 9553949
- Total bytes: 133260669

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counter_valueno_i | Int64 |  | 1 | 1 | 0 |  |
| counterno_i | Int64 |  | 0 | 0 | 0 |  |
| life_value | Nullable(Float64) |  | 0 | 0 | 0 |  |
| readout_ref_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| on_counter_valueno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| off_counter_valueno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| is_minor | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| readout_date | Date32 |  | 0 | 0 | 0 |  |
| readout_time | Int64 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_counter_value (`counter_valueno_i` Int64, `counterno_i` Int64, `life_value` Nullable(Float64), `readout_ref_type` LowCardinality(Nullable(String)), `on_counter_valueno_i` Nullable(Int64), `off_counter_valueno_i` Nullable(Int64), `is_minor` LowCardinality(Nullable(String)), `readout_date` Date32, `readout_time` Int64, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_counter_value', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (counter_valueno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из counter_value вертолетного амос'
```
</details>

### `source`.`amos_heli_event_effectivity`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: effectivityno_i, valid_from
- Primary key: effectivityno_i, valid_from
- Total rows: 40651
- Total bytes: 932468

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| effectivityno_i | Int32 |  | 1 | 1 | 0 |  |
| effectivity_headerno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| title | String |  | 0 | 0 | 0 |  |
| aircraft_typ | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_event_effectivity (`effectivityno_i` Int32, `effectivity_headerno_i` Nullable(Int32), `title` String, `aircraft_typ` LowCardinality(Nullable(String)), `partno` Nullable(String), `status` Nullable(Int16), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_event_effectivity', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (effectivityno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из event_effectivity вертолетного амос'
```
</details>

### `source`.`amos_heli_event_effectivity_link`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: effectivity_linkno_i, valid_from
- Primary key: effectivity_linkno_i, valid_from
- Total rows: 34266
- Total bytes: 404214

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| effectivityno_i | Int32 |  | 0 | 0 | 0 |  |
| effectivity_linkno_i | Int32 |  | 1 | 1 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 1 | 1 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_event_effectivity_link (`effectivityno_i` Int32, `effectivity_linkno_i` Int32, `event_type` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_event_effectivity_link', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (effectivity_linkno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из event_effectivity_link вертолетного амос'
```
</details>

### `source`.`amos_heli_event_effectivity_rules`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: effectivityno_i, valid_from
- Primary key: effectivityno_i, valid_from
- Total rows: 4967
- Total bytes: 133731

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| effectivityno_i | Int32 |  | 1 | 1 | 0 |  |
| aircraft_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| rotable_manuf_before | Nullable(Int32) |  | 0 | 0 | 0 |  |
| rotable_manuf_after | Nullable(Int32) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 1 | 1 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_event_effectivity_rules (`effectivityno_i` Int32, `aircraft_type` LowCardinality(Nullable(String)), `rotable_manuf_before` Nullable(Int32), `rotable_manuf_after` Nullable(Int32), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_event_effectivity_rules', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (effectivityno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из event_effectivity_rules вертолетного амос'
```
</details>

### `source`.`amos_heli_event_effectivity_sns`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: effectivity_snno_i, valid_from
- Primary key: effectivity_snno_i, valid_from
- Total rows: 76791
- Total bytes: 575976

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| effectivityno_i | Int32 |  | 0 | 0 | 0 |  |
| effectivity_snno_i | Int32 |  | 1 | 1 | 0 |  |
| range_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_event_effectivity_sns (`effectivityno_i` Int32, `effectivity_snno_i` Int32, `range_type` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_event_effectivity_sns', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (effectivity_snno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из event_effectivity_sns вертолетного амос'
```
</details>

### `source`.`amos_heli_forecast`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: event_perfno_i, valid_from
- Primary key: event_perfno_i, valid_from
- Total rows: 27585986
- Total bytes: 230639425

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_perfno_i | Int64 |  | 1 | 1 | 0 |  |
| psn | Nullable(Int64) |  | 0 | 0 | 0 |  |
| requirement | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_registr | String |  | 0 | 0 | 0 |  |
| ac_typ | LowCardinality(String) |  | 0 | 0 | 0 |  |
| event | String |  | 0 | 0 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_forecast (`event_perfno_i` Int64, `psn` Nullable(Int64), `requirement` LowCardinality(Nullable(String)), `partno` Nullable(String), `serialno` Nullable(String), `ac_registr` String, `ac_typ` LowCardinality(String), `event` String, `event_type` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_forecast', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (event_perfno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из forecast вертолетного амос'
```
</details>

### `source`.`amos_heli_forecast_dimension`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: event_perfno_i, counter_defno_i, valid_from
- Primary key: event_perfno_i, counter_defno_i, valid_from
- Total rows: 28138991
- Total bytes: 76255239

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_perfno_i | Int64 |  | 1 | 1 | 0 |  |
| counter_defno_i | Int32 |  | 1 | 1 | 0 |  |
| dimension | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_forecast_dimension (`event_perfno_i` Int64, `counter_defno_i` Int32, `dimension` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_forecast_dimension', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (event_perfno_i, counter_defno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из forecast_dimension вертолетного амос'
```
</details>

### `source`.`amos_heli_history`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: historyno_i, valid_from
- Primary key: historyno_i, valid_from
- Total rows: 3098627
- Total bytes: 53929442

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| historyno_i | Int64 |  | 1 | 1 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| vm | LowCardinality(String) |  | 0 | 0 | 0 |  |
| od_detailno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| ac_registr | Nullable(String) |  | 0 | 0 | 0 |  |
| del_date | Date32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_history (`historyno_i` Int64, `partno` String, `serialno` Nullable(String), `vm` LowCardinality(String), `od_detailno_i` Nullable(Int32), `ac_registr` Nullable(String), `del_date` Date32, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_history', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (historyno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из history вертолетного амос'
```
</details>

### `source`.`amos_heli_location`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: locationno_i, valid_from
- Primary key: locationno_i, valid_from
- Total rows: 6728
- Total bytes: 234446

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| locationno_i | Int32 |  | 1 | 1 | 0 |  |
| description | LowCardinality(String) |  | 0 | 0 | 0 |  |
| store | LowCardinality(String) |  | 0 | 0 | 0 |  |
| station | LowCardinality(String) |  | 0 | 0 | 0 |  |
| location | LowCardinality(String) |  | 0 | 0 | 0 |  |
| status | Int16 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_location (`locationno_i` Int32, `description` LowCardinality(String), `store` LowCardinality(String), `station` LowCardinality(String), `location` LowCardinality(String), `status` Int16, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_location', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (locationno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из location вертолетного амос'
```
</details>

### `source`.`amos_heli_mevt_effectivity`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: mevt_effectivityno_i, valid_from
- Primary key: mevt_effectivityno_i, valid_from
- Total rows: 1626114
- Total bytes: 19166725

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| mevt_effectivityno_i | Int64 |  | 1 | 1 | 0 |  |
| mevt_headerno_i | Int64 |  | 0 | 0 | 0 |  |
| effectivity_linkno_i | Int64 |  | 0 | 0 | 0 |  |
| template_revisionno_i | Int64 |  | 0 | 0 | 0 |  |
| timerequirementno_i | Int64 |  | 0 | 0 | 0 |  |
| revision_key | Int32 |  | 0 | 0 | 0 |  |
| revision_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| applicable_status | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_mevt_effectivity (`mevt_effectivityno_i` Int64, `mevt_headerno_i` Int64, `effectivity_linkno_i` Int64, `template_revisionno_i` Int64, `timerequirementno_i` Int64, `revision_key` Int32, `revision_type` LowCardinality(String), `applicable_status` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_mevt_effectivity', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (mevt_effectivityno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из mevt_effectivity вертолетного амос'
```
</details>

### `source`.`amos_heli_mevt_header`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: mevt_headerno_i, valid_from
- Primary key: mevt_headerno_i, valid_from
- Total rows: 1793198
- Total bytes: 28570568

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| mevt_headerno_i | Int64 |  | 1 | 1 | 0 |  |
| identifier | String |  | 0 | 0 | 0 |  |
| ref_key | Int64 |  | 0 | 0 | 0 |  |
| ref_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| mevt_key | Int32 |  | 0 | 0 | 0 |  |
| mevt_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_mevt_header (`mevt_headerno_i` Int64, `identifier` String, `ref_key` Int64, `ref_type` LowCardinality(String), `mevt_key` Int32, `mevt_type` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_mevt_header', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (mevt_headerno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из mevt_header вертолетного амос'
```
</details>

### `source`.`amos_heli_od_detail`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: detailno_i, valid_from
- Primary key: detailno_i, valid_from
- Total rows: 834733
- Total bytes: 18339514

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| detailno_i | Int64 |  | 1 | 1 | 0 |  |
| orderno_i | Int64 |  | 0 | 0 | 0 |  |
| order_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ac_registr | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| state | LowCardinality(String) |  | 0 | 0 | 0 |  |
| vendor | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| condition | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| qty | Int32 |  | 0 | 0 | 0 |  |
| purch_price | Int64 |  | 0 | 0 | 0 |  |
| target_date | Date32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_od_detail (`detailno_i` Int64, `orderno_i` Int64, `order_type` LowCardinality(String), `ac_registr` LowCardinality(Nullable(String)), `state` LowCardinality(String), `vendor` LowCardinality(Nullable(String)), `partno` String, `serialno` Nullable(String), `condition` LowCardinality(Nullable(String)), `qty` Int32, `purch_price` Int64, `target_date` Date32, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_od_detail', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (detailno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из od_detail вертолетного амос'
```
</details>

### `source`.`amos_heli_part`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: partno, valid_from
- Primary key: partno, valid_from
- Total rows: 89348
- Total bytes: 3829481

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | String |  | 1 | 1 | 0 |  |
| partmatch | String |  | 0 | 0 | 0 |  |
| partseqno_i | Int32 |  | 0 | 0 | 0 |  |
| ac_typ | LowCardinality(String) |  | 0 | 0 | 0 |  |
| mat_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| description | String |  | 0 | 0 | 0 |  |
| remarks | Nullable(String) |  | 0 | 0 | 0 |  |
| ata_chapter | LowCardinality(String) |  | 0 | 0 | 0 |  |
| vendor | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_part (`partno` String, `partmatch` String, `partseqno_i` Int32, `ac_typ` LowCardinality(String), `mat_type` LowCardinality(String), `description` String, `remarks` Nullable(String), `ata_chapter` LowCardinality(String), `vendor` Nullable(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_part', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (partno, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из part вертолетного амос'
```
</details>

### `source`.`amos_heli_part_requirement`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: part_requirementno_i, valid_from
- Primary key: part_requirementno_i, valid_from
- Total rows: 16055
- Total bytes: 212277

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| part_requirementno_i | Int32 |  | 1 | 1 | 0 |  |
| type | Int32 |  | 0 | 0 | 0 |  |
| status | Int16 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 1 | 1 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_part_requirement (`part_requirementno_i` Int32, `type` Int32, `status` Int16, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_part_requirement', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (part_requirementno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из part_requirement вертолетного амос'
```
</details>

### `source`.`amos_heli_part_special`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: part_specialno_i, valid_from
- Primary key: part_specialno_i, valid_from
- Total rows: 65461
- Total bytes: 1001744

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| part_specialno_i | Int32 |  | 1 | 1 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| special | LowCardinality(String) |  | 0 | 0 | 0 |  |
| remarks | LowCardinality(String) |  | 0 | 0 | 0 |  |
| amount | Int16 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_part_special (`part_specialno_i` Int32, `partno` String, `special` LowCardinality(String), `remarks` LowCardinality(String), `amount` Int16, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_part_special', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (part_specialno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из part_special вертолетного амос'
```
</details>

### `source`.`amos_heli_requirement_header`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: requirement_headerno_i, valid_from
- Primary key: requirement_headerno_i, valid_from
- Total rows: 31383
- Total bytes: 440646

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_key | Int32 |  | 0 | 0 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| effectivity_headerno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| requirement_headerno_i | Int32 |  | 1 | 1 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 1 | 1 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_requirement_header (`event_key` Int32, `event_type` LowCardinality(String), `effectivity_headerno_i` Nullable(Int32), `requirement_headerno_i` Int32, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_requirement_header', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (requirement_headerno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из requirement_header вертолетного амос'
```
</details>

### `source`.`amos_heli_requirement_type`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: requirement_typeno_i, valid_from
- Primary key: requirement_typeno_i, valid_from
- Total rows: 31
- Total bytes: 9260

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| requirement_typeno_i | Int32 |  | 1 | 1 | 0 |  |
| requirement | LowCardinality(String) |  | 0 | 0 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| life_limit | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 1 |  |
| valid_from | DateTime |  | 1 | 1 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_requirement_type (`requirement_typeno_i` Int32, `requirement` LowCardinality(String), `description` Nullable(String), `life_limit` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_requirement_type', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (requirement_typeno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из requirement_type вертолетного амос'
```
</details>

### `source`.`amos_heli_rotables`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: psn, valid_from
- Primary key: psn, valid_from
- Total rows: 512745
- Total bytes: 13134689

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_registr | Nullable(String) |  | 0 | 0 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| material_lifecycle_id | Int64 |  | 0 | 0 | 0 |  |
| serialno | String |  | 0 | 0 | 0 |  |
| locationno_i | Int32 |  | 0 | 0 | 0 |  |
| psn | Int64 |  | 1 | 1 | 0 |  |
| shop_visit_counter | Int32 |  | 0 | 0 | 0 |  |
| mfg_unknown | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| orderno | Nullable(String) |  | 0 | 0 | 0 |  |
| owner | LowCardinality(String) |  | 0 | 0 | 0 |  |
| condition | LowCardinality(String) |  | 0 | 0 | 0 |  |
| oh_at_date | Date32 |  | 0 | 0 | 0 |  |
| del_date | Date32 |  | 0 | 0 | 0 |  |
| mfg_date | Date32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_rotables (`ac_registr` Nullable(String), `partno` String, `material_lifecycle_id` Int64, `serialno` String, `locationno_i` Int32, `psn` Int64, `shop_visit_counter` Int32, `mfg_unknown` LowCardinality(Nullable(String)), `orderno` Nullable(String), `owner` LowCardinality(String), `condition` LowCardinality(String), `oh_at_date` Date32, `del_date` Date32, `mfg_date` Date32, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_rotables', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (psn, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из rotables вертолетного амос'
```
</details>

### `source`.`amos_heli_treq_dimension_group`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: dimension_groupno_i, valid_from
- Primary key: dimension_groupno_i, valid_from
- Total rows: 231775
- Total bytes: 1971845

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| interval_groupno_i | Int64 |  | 0 | 0 | 0 |  |
| dimension_groupno_i | Int64 |  | 1 | 1 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_treq_dimension_group (`interval_groupno_i` Int64, `dimension_groupno_i` Int64, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_treq_dimension_group', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (dimension_groupno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из treq_dimension_group вертолетного амос'
```
</details>

### `source`.`amos_heli_treq_event_link`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: eventlinkno_i, valid_from
- Primary key: eventlinkno_i, valid_from
- Total rows: 113655
- Total bytes: 1814189

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| eventlinkno_i | Int32 |  | 1 | 1 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| event_key | Int32 |  | 0 | 0 | 0 |  |
| ac_registr | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| psn | Int32 |  | 0 | 0 | 0 |  |
| status | Int16 |  | 0 | 0 | 0 |  |
| timerequirementno_i | Int32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_treq_event_link (`eventlinkno_i` Int32, `event_type` LowCardinality(String), `event_key` Int32, `ac_registr` LowCardinality(Nullable(String)), `psn` Int32, `status` Int16, `timerequirementno_i` Int32, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_treq_event_link', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (eventlinkno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из treq_event_link вертолетного амос'
```
</details>

### `source`.`amos_heli_treq_interval`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: intervalno_i, valid_from
- Primary key: intervalno_i, valid_from
- Total rows: 454508
- Total bytes: 5620292

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| intervalno_i | Int64 |  | 1 | 1 | 0 |  |
| interval_groupno_i | Int32 |  | 0 | 0 | 0 |  |
| dimension_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| counter_defno_i | Int32 |  | 0 | 0 | 0 |  |
| amount_interval | Int64 |  | 0 | 0 | 0 |  |
| due_at | Nullable(Int32) |  | 0 | 0 | 0 |  |
| dimension_groupno_i | Int64 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_treq_interval (`intervalno_i` Int64, `interval_groupno_i` Int32, `dimension_type` LowCardinality(String), `counter_defno_i` Int32, `amount_interval` Int64, `due_at` Nullable(Int32), `dimension_groupno_i` Int64, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_treq_interval', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (intervalno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из treq_interval вертолетного амос'
```
</details>

### `source`.`amos_heli_treq_interval_group`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: interval_groupno_i, valid_from
- Primary key: interval_groupno_i, valid_from
- Total rows: 246800
- Total bytes: 2198871

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| interval_groupno_i | Int64 |  | 1 | 1 | 0 |  |
| timerequirementno_i | Int64 |  | 0 | 0 | 0 |  |
| threshold | LowCardinality(String) |  | 0 | 0 | 0 |  |
| group_name | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_treq_interval_group (`interval_groupno_i` Int64, `timerequirementno_i` Int64, `threshold` LowCardinality(String), `group_name` LowCardinality(Nullable(String)), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_treq_interval_group', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (interval_groupno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из treq_interval_group вертолетного амос'
```
</details>

### `source`.`amos_heli_treq_time_requirement`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: timerequirementno_i, valid_from
- Primary key: timerequirementno_i, valid_from
- Total rows: 210311
- Total bytes: 1994540

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| timerequirementno_i | Int64 |  | 1 | 1 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| event_key | Int64 |  | 0 | 0 | 0 |  |
| ac_group | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_treq_time_requirement (`timerequirementno_i` Int64, `event_type` LowCardinality(String), `event_key` Int64, `ac_group` LowCardinality(Nullable(String)), `type` LowCardinality(String), `status` Nullable(Int16), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_treq_time_requirement', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (timerequirementno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из treq_time_requirement вертолетного амос'
```
</details>

### `source`.`amos_heli_wo_event_link`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: event_perfno_i, valid_from
- Primary key: event_perfno_i, valid_from
- Total rows: 3193953
- Total bytes: 39873374

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_perfno_i | Int64 |  | 1 | 1 | 0 |  |
| effectivity_linkno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| mevt_headerno_i | Int64 |  | 0 | 0 | 0 |  |
| pending_status | Int16 |  | 0 | 0 | 0 |  |
| event_name | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_wo_event_link (`event_perfno_i` Int64, `effectivity_linkno_i` Nullable(Int32), `mevt_headerno_i` Int64, `pending_status` Int16, `event_name` Nullable(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_wo_event_link', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (event_perfno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из wo_event_link вертолетного амос'
```
</details>

### `source`.`amos_heli_wo_header`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: event_perfno_i, valid_from
- Primary key: event_perfno_i, valid_from
- Total rows: 3866226
- Total bytes: 34142607

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_perfno_i | Int64 |  | 1 | 1 | 0 |  |
| psn | Nullable(Int64) |  | 0 | 0 | 0 |  |
| ata_chapter | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| state | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ac_registr | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_wo_header (`event_perfno_i` Int64, `psn` Nullable(Int64), `ata_chapter` LowCardinality(Nullable(String)), `state` LowCardinality(String), `ac_registr` Nullable(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_wo_header', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (event_perfno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из wo_header вертолетного амос'
```
</details>

### `source`.`amos_heli_wo_transfer`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: event_transferno_i, valid_from
- Primary key: event_transferno_i, valid_from
- Total rows: 4248596
- Total bytes: 35497949

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_transferno_i | Int64 |  | 1 | 1 | 0 |  |
| event_perfno_i | Int64 |  | 0 | 0 | 0 |  |
| is_last_transfer | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_wo_transfer (`event_transferno_i` Int64, `event_perfno_i` Int64, `is_last_transfer` LowCardinality(String), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_wo_transfer', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (event_transferno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из history вертолетного амос'
```
</details>

### `source`.`amos_heli_wo_transfer_dimension`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: wo_transfer_dimensionno_i, valid_from
- Primary key: wo_transfer_dimensionno_i, valid_from
- Total rows: 9678764
- Total bytes: 108225093

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| wo_transfer_dimensionno_i | Int64 |  | 1 | 1 | 0 |  |
| event_transferno_i | Int64 |  | 0 | 0 | 0 |  |
| treq_intervalno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| counterno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| due_at | Nullable(Float64) |  | 0 | 0 | 0 |  |
| status | Int8 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_wo_transfer_dimension (`wo_transfer_dimensionno_i` Int64, `event_transferno_i` Int64, `treq_intervalno_i` Nullable(Int64), `counterno_i` Nullable(Int64), `due_at` Nullable(Float64), `status` Int8, `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_wo_transfer_dimension', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (wo_transfer_dimensionno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из history вертолетного амос'
```
</details>

### `source`.`amos_heli_wp_header`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: wpno_i, valid_from
- Primary key: wpno_i, valid_from
- Total rows: 24068
- Total bytes: 1778634

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| mpno | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| drop_locationno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| wpno_i | Int32 |  | 1 | 1 | 0 |  |
| wpno | String |  | 0 | 0 | 0 |  |
| ac_registr | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| ac_typ | LowCardinality(String) |  | 0 | 0 | 0 |  |
| projectno | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| est_groundtime | Int32 |  | 0 | 0 | 0 |  |
| station | LowCardinality(String) |  | 0 | 0 | 0 |  |
| start_date | Int32 |  | 0 | 0 | 0 |  |
| start_time | Int32 |  | 0 | 0 | 0 |  |
| end_date | Int32 |  | 0 | 0 | 0 |  |
| end_time | Int32 |  | 0 | 0 | 0 |  |
| description | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| owner | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| hidden | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| status | Int8 |  | 0 | 0 | 0 |  |
| act_start_date | Nullable(Int32) |  | 0 | 0 | 0 |  |
| act_start_time | Nullable(Int32) |  | 0 | 0 | 0 |  |
| act_end_date | Nullable(Int32) |  | 0 | 0 | 0 |  |
| act_end_time | Nullable(Int32) |  | 0 | 0 | 0 |  |
| responsible | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| delay | Nullable(Int32) |  | 0 | 0 | 0 |  |
| cust_wpno | Nullable(String) |  | 0 | 0 | 0 |  |
| priority_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| remarks | Nullable(String) |  | 0 | 0 | 0 |  |
| extension_time | Nullable(Int32) |  | 0 | 0 | 0 |  |
| extension_reason | Nullable(Int8) |  | 0 | 0 | 0 |  |
| mpno_i | Nullable(Int16) |  | 0 | 0 | 0 |  |
| mp_revision | Nullable(Int16) |  | 0 | 0 | 0 |  |
| wp_status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| events_collection_status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| uuid | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_operator | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| ac_model | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Date |  | 0 | 0 | 1 | Дата изменения записи |
| valid_from | DateTime |  | 1 | 1 | 0 | Дата внесения записи в бд |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 | Дата когда была внесена более свежая запись |
| processing_date_at | DateTime |  | 0 | 0 | 0 | дата отработки дага (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.amos_heli_wp_header (`mpno` LowCardinality(Nullable(String)), `drop_locationno_i` Nullable(Int32), `wpno_i` Int32, `wpno` String, `ac_registr` LowCardinality(Nullable(String)), `ac_typ` LowCardinality(String), `projectno` LowCardinality(Nullable(String)), `est_groundtime` Int32, `station` LowCardinality(String), `start_date` Int32, `start_time` Int32, `end_date` Int32, `end_time` Int32, `description` LowCardinality(Nullable(String)), `owner` LowCardinality(Nullable(String)), `hidden` LowCardinality(Nullable(String)), `status` Int8, `act_start_date` Nullable(Int32), `act_start_time` Nullable(Int32), `act_end_date` Nullable(Int32), `act_end_time` Nullable(Int32), `responsible` LowCardinality(Nullable(String)), `delay` Nullable(Int32), `cust_wpno` Nullable(String), `priority_code` LowCardinality(Nullable(String)), `remarks` Nullable(String) CODEC(ZSTD(3)), `extension_time` Nullable(Int32), `extension_reason` Nullable(Int8), `mpno_i` Nullable(Int16), `mp_revision` Nullable(Int16), `wp_status` Nullable(Int16), `events_collection_status` Nullable(Int16), `uuid` Nullable(String) CODEC(ZSTD(3)), `ac_operator` LowCardinality(Nullable(String)), `ac_model` LowCardinality(Nullable(String)), `created_date` Date COMMENT 'Дата создания записи', `updated_at` Date COMMENT 'Дата изменения записи', `valid_from` DateTime COMMENT 'Дата внесения записи в бд', `valid_to` Nullable(DateTime) COMMENT 'Дата когда была внесена более свежая запись', `processing_date_at` DateTime COMMENT 'дата отработки дага (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/amos_heli_wp_header', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (wpno_i, valid_from) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из wp_header вертолетного амос'
```
</details>

### `source`.`appareo_raw_files`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: processing_date, file_name, file_signature
- Primary key: processing_date, file_name, file_signature
- Total rows: 539
- Total bytes: 64842

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| file_name | String |  | 1 | 1 | 0 | Имя файла (как в источнике) |
| file_created_at | DateTime |  | 0 | 0 | 0 | Дата появления файла в папке (UTC, st_ctime/creation) |
| file_modified_at | DateTime |  | 0 | 0 | 0 | Дата изменения файла (UTC) |
| file_size | UInt64 |  | 0 | 0 | 0 | Размер файла в байтах |
| file_signature | String |  | 1 | 1 | 0 | Подпись файла: hash(name + size + modified_at) |
| source_path | String |  | 0 | 0 | 0 | Полный путь к каталогу/файлу на сетевой шаре |
| processing_date | Date |  | 1 | 1 | 1 | Дата обработки файла (когда файл был забран из источника) |
| s3_bucket | LowCardinality(String) |  | 0 | 0 | 0 | Имя S3 бакета с файлом |
| s3_key | String |  | 0 | 0 | 0 | Ключ объекта в S3 |
| s3_etag | String |  | 0 | 0 | 0 | ETag, зафиксированный после загрузки в S3 |
| s3_endpoint_url | LowCardinality(String) |  | 0 | 0 | 0 | Endpoint URL S3-совместимого хранилища |
| s3_region | LowCardinality(String) |  | 0 | 0 | 0 | Регион S3 хранилища |
| s3_url | String |  | 0 | 0 | 0 | Полный URL файла в S3 (s3://bucket/key) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.appareo_raw_files (`file_name` String COMMENT 'Имя файла (как в источнике)', `file_created_at` DateTime COMMENT 'Дата появления файла в папке (UTC, st_ctime/creation)', `file_modified_at` DateTime COMMENT 'Дата изменения файла (UTC)', `file_size` UInt64 COMMENT 'Размер файла в байтах', `file_signature` String COMMENT 'Подпись файла: hash(name + size + modified_at)', `source_path` String COMMENT 'Полный путь к каталогу/файлу на сетевой шаре', `processing_date` Date COMMENT 'Дата обработки файла (когда файл был забран из источника)', `s3_bucket` LowCardinality(String) COMMENT 'Имя S3 бакета с файлом', `s3_key` String COMMENT 'Ключ объекта в S3', `s3_etag` String COMMENT 'ETag, зафиксированный после загрузки в S3', `s3_endpoint_url` LowCardinality(String) COMMENT 'Endpoint URL S3-совместимого хранилища', `s3_region` LowCardinality(String) COMMENT 'Регион S3 хранилища', `s3_url` String COMMENT 'Полный URL файла в S3 (s3://bucket/key)', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/appareo_raw_files', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (processing_date, file_name, file_signature) SETTINGS index_granularity = 8192 COMMENT 'Информация о сырых файлах данных Appareo, сохраненных в S3.'
```
</details>

### `source`.`appmetrica_installations`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(install_datetime)
- Sorting key: log_date, installation_id, install_datetime
- Primary key: log_date, installation_id, install_datetime
- Total rows: 129507
- Total bytes: 51164341

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| app_id | UInt64 |  | 0 | 0 | 0 | ID приложения AppMetrica |
| log_date | Date |  | 1 | 1 | 0 | Дата логов (параметр date_since) |
| installation_id | String |  | 1 | 1 | 0 | Идентификатор установки |
| install_datetime | DateTime |  | 1 | 1 | 1 | Дата и время установки |
| click_url_parameters | Nullable(String) |  | 0 | 0 | 0 | URL параметры клика |
| publisher_name | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Название паблишера |
| tracker_name | LowCardinality(String) |  | 0 | 0 | 0 | Название трекера |
| payload | String |  | 0 | 0 | 0 | Сырые данные источника в JSON |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.appmetrica_installations (`app_id` UInt64 COMMENT 'ID приложения AppMetrica', `log_date` Date COMMENT 'Дата логов (параметр date_since)', `installation_id` String COMMENT 'Идентификатор установки', `install_datetime` DateTime COMMENT 'Дата и время установки', `click_url_parameters` Nullable(String) COMMENT 'URL параметры клика', `publisher_name` LowCardinality(Nullable(String)) COMMENT 'Название паблишера', `tracker_name` LowCardinality(String) COMMENT 'Название трекера', `payload` String COMMENT 'Сырые данные источника в JSON', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/appmetrica_installations', '{replica}') PARTITION BY toYYYYMM(install_datetime) ORDER BY (log_date, installation_id, install_datetime) SETTINGS index_granularity = 8192 COMMENT 'Логи установок мобильного приложения из AppMetrica.'
```
</details>

### `source`.`availability_pricing_audit`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(event_time)
- Sorting key: event_time, trace_id, duration
- Primary key: event_time, trace_id, duration
- Total rows: 32972276
- Total bytes: 1610948935

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| trace_id | String |  | 1 | 1 | 0 | Идентификатор трассировки запроса |
| event_time | DateTime64(3) |  | 1 | 1 | 1 | Время события (@timestamp), UTC, с миллисекундами |
| user_ip | String |  | 0 | 0 | 0 | IP адрес пользователя |
| duration | Float64 |  | 1 | 1 | 0 | Длительность выполнения запроса (секунды) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.availability_pricing_audit (`trace_id` String COMMENT 'Идентификатор трассировки запроса', `event_time` DateTime64(3) COMMENT 'Время события (@timestamp), UTC, с миллисекундами', `user_ip` String COMMENT 'IP адрес пользователя', `duration` Float64 COMMENT 'Длительность выполнения запроса (секунды)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/availability_pricing_audit', '{replica}') PARTITION BY toYYYYMM(event_time) ORDER BY (event_time, trace_id, duration) TTL toDateTime(event_time) + toIntervalMonth(3) SETTINGS index_granularity = 8192 COMMENT 'Сырые данные audit для availability_pricing. Слой source.'
```
</details>

### `source`.`availability_pricing_logstash`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(event_time)
- Sorting key: event_time, trace_id, duration
- Primary key: event_time, trace_id, duration
- Total rows: 33125314
- Total bytes: 1423431718

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| trace_id | String |  | 1 | 1 | 0 | Идентификатор трассировки запроса |
| event_time | DateTime64(3) |  | 1 | 1 | 1 | Время события (@timestamp), UTC, с миллисекундами |
| duration | Float64 |  | 1 | 1 | 0 | Длительность выполнения запроса (секунды) |
| method | String |  | 0 | 0 | 0 | Название метода API |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.availability_pricing_logstash (`trace_id` String COMMENT 'Идентификатор трассировки запроса', `event_time` DateTime64(3) COMMENT 'Время события (@timestamp), UTC, с миллисекундами', `duration` Float64 COMMENT 'Длительность выполнения запроса (секунды)', `method` String COMMENT 'Название метода API', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/availability_pricing_logstash', '{replica}') PARTITION BY toYYYYMM(event_time) ORDER BY (event_time, trace_id, duration) TTL toDateTime(event_time) + toIntervalMonth(3) SETTINGS index_granularity = 8192 COMMENT 'Сырые данные logstash для availability_pricing. Слой source.'
```
</details>

### `source`.`contacts_from_mango_office`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_at)
- Sorting key: processing_at
- Primary key: processing_at
- Total rows: 4364
- Total bytes: 9610062

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| payload | JSON |  | 0 | 0 | 0 | Сырой JSON целиком |
| processing_at | DateTime |  | 1 | 1 | 1 | Дата отработки дага |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.contacts_from_mango_office (`payload` JSON COMMENT 'Сырой JSON целиком', `processing_at` DateTime COMMENT 'Дата отработки дага', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/contacts_from_mango_office_new', '{replica}') PARTITION BY toYYYYMM(processing_at) ORDER BY processing_at SETTINGS index_granularity = 8192 COMMENT 'Таблица с сырыми данными контактов из Mango Office'
```
</details>

### `source`.`contacts_from_mango_office_old`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: processing_date
- Primary key: processing_date
- Total rows: 52
- Total bytes: 197258

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| payload | JSON |  | 0 | 0 | 0 | Сырой JSON целиком |
| processing_date | Date |  | 1 | 1 | 0 | Дата отработки дага |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.contacts_from_mango_office_old (`payload` JSON COMMENT 'Сырой JSON целиком', `processing_date` Date COMMENT 'Дата отработки дага', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/contacts_from_mango_office', '{replica}') ORDER BY processing_date SETTINGS index_granularity = 8192 COMMENT 'Таблица с сырыми данными контактов из Mango Office'
```
</details>

### `source`.`credinfo_subagents`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_at)
- Sorting key: processing_at, search_inn
- Primary key: processing_at, search_inn
- Total rows: 50
- Total bytes: 56639

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| search_inn | LowCardinality(String) |  | 1 | 1 | 0 | ИНН, по которому выполнялся поиск компании в Credinform |
| company_id | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор компании в Credinform (companyId), пустая строка если не найден |
| payload_searchcompany | JSON |  | 0 | 0 | 0 | Сырой JSON-ответ метода /Search/SearchCompany |
| payload_indexes | JSON |  | 0 | 0 | 0 | Индексы Credinform: надёжность, рейтинг, скоринг, прогноз банкротства |
| payload_requires_attention | JSON |  | 0 | 0 | 0 | Факторы «Требует внимания» — оценка безопасности сотрудничества |
| payload_notifications_bankruptcy | JSON |  | 0 | 0 | 0 | Уведомления о банкротстве по данным ЕФРСБ |
| payload_notifications_bankruptcy_statistics | JSON |  | 0 | 0 | 0 | Статистика уведомлений о банкротстве |
| payload_notifications_bankruptcy_kommersant | JSON |  | 0 | 0 | 0 | Уведомления о банкротстве по данным газеты «Коммерсантъ» |
| payload_management_by_credinform | JSON |  | 0 | 0 | 0 | Руководство компании по данным Credinform |
| payload_management_by_unified_state_register | JSON |  | 0 | 0 | 0 | Руководство компании по данным ЕГРЮЛ |
| payload_management_by_statistics_service | JSON |  | 0 | 0 | 0 | Руководство компании по данным Росстата |
| payload_shareholders_by_credinform | JSON |  | 0 | 0 | 0 | Учредители компании по данным Credinform |
| payload_shareholders_by_unified_state_register | JSON |  | 0 | 0 | 0 | Учредители компании по данным ЕГРЮЛ |
| payload_shareholders_by_statistics_service | JSON |  | 0 | 0 | 0 | Учредители компании по данным Росстата |
| processing_at | DateTime |  | 1 | 1 | 1 | Дата и время отработки DAG (UTC) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи (DAG_ID). Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Момент загрузки строки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.credinfo_subagents (`search_inn` LowCardinality(String) COMMENT 'ИНН, по которому выполнялся поиск компании в Credinform', `company_id` LowCardinality(String) COMMENT 'Идентификатор компании в Credinform (companyId), пустая строка если не найден', `payload_searchcompany` JSON COMMENT 'Сырой JSON-ответ метода /Search/SearchCompany', `payload_indexes` JSON COMMENT 'Индексы Credinform: надёжность, рейтинг, скоринг, прогноз банкротства', `payload_requires_attention` JSON COMMENT 'Факторы «Требует внимания» — оценка безопасности сотрудничества', `payload_notifications_bankruptcy` JSON COMMENT 'Уведомления о банкротстве по данным ЕФРСБ', `payload_notifications_bankruptcy_statistics` JSON COMMENT 'Статистика уведомлений о банкротстве', `payload_notifications_bankruptcy_kommersant` JSON COMMENT 'Уведомления о банкротстве по данным газеты «Коммерсантъ»', `payload_management_by_credinform` JSON COMMENT 'Руководство компании по данным Credinform', `payload_management_by_unified_state_register` JSON COMMENT 'Руководство компании по данным ЕГРЮЛ', `payload_management_by_statistics_service` JSON COMMENT 'Руководство компании по данным Росстата', `payload_shareholders_by_credinform` JSON COMMENT 'Учредители компании по данным Credinform', `payload_shareholders_by_unified_state_register` JSON COMMENT 'Учредители компании по данным ЕГРЮЛ', `payload_shareholders_by_statistics_service` JSON COMMENT 'Учредители компании по данным Росстата', `processing_at` DateTime COMMENT 'Дата и время отработки DAG (UTC)', `meta_source` LowCardinality(String) COMMENT 'Источник записи (DAG_ID). Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Момент загрузки строки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/credinfo_subagents', '{replica}') PARTITION BY toYYYYMM(processing_at) ORDER BY (processing_at, search_inn) SETTINGS index_granularity = 8192 COMMENT 'Сырые JSON-ответы API Credinform по компаниям-субагентам (поиск по ИНН).'
```
</details>

### `source`.`fdr_express_analysis_docx`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(target_date)
- Sorting key: target_date, file_name, file_signature
- Primary key: target_date, file_name, file_signature
- Total rows: 4767
- Total bytes: 538446

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| file_name | String |  | 1 | 1 | 0 | Имя docx файла |
| smb_relative_path | String |  | 0 | 0 | 0 | Относительный путь внутри шары TsopiShare |
| file_size | UInt64 |  | 0 | 0 | 0 | Размер файла, байт |
| file_modified_at | DateTime |  | 0 | 0 | 0 | Время изменения файла в DFI (UTC) |
| file_signature | String |  | 1 | 1 | 0 | MD5 содержимого файла |
| s3_bucket | LowCardinality(String) |  | 0 | 0 | 0 | Бакет S3 |
| s3_key | String |  | 0 | 0 | 0 | Ключ объекта S3 |
| s3_url | String |  | 0 | 0 | 0 | Полный путь объекта в S3 (s3://bucket/key) |
| s3_etag | String |  | 0 | 0 | 0 | ETag после загрузки |
| s3_endpoint_url | LowCardinality(String) |  | 0 | 0 | 0 | Endpoint S3 |
| s3_region | LowCardinality(String) |  | 0 | 0 | 0 | Регион S3 |
| target_date | Date |  | 1 | 1 | 1 | Целевая дата обработки |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор DAG |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Момент загрузки в source (UTC) |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.fdr_express_analysis_docx (`file_name` String COMMENT 'Имя docx файла', `smb_relative_path` String COMMENT 'Относительный путь внутри шары TsopiShare', `file_size` UInt64 COMMENT 'Размер файла, байт', `file_modified_at` DateTime COMMENT 'Время изменения файла в DFI (UTC)', `file_signature` String COMMENT 'MD5 содержимого файла', `s3_bucket` LowCardinality(String) COMMENT 'Бакет S3', `s3_key` String COMMENT 'Ключ объекта S3', `s3_url` String COMMENT 'Полный путь объекта в S3 (s3://bucket/key)', `s3_etag` String COMMENT 'ETag после загрузки', `s3_endpoint_url` LowCardinality(String) COMMENT 'Endpoint S3', `s3_region` LowCardinality(String) COMMENT 'Регион S3', `target_date` Date COMMENT 'Целевая дата обработки', `meta_source` LowCardinality(String) COMMENT 'Идентификатор DAG', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Момент загрузки в source (UTC)') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/fdr_express_analysis_docx', '{replica}') PARTITION BY toYYYYMM(target_date) ORDER BY (target_date, file_name, file_signature) SETTINGS index_granularity = 8192 COMMENT 'Source: отчеты FDR Express Analysis (файловые метаданные и реквизиты S3).'
```
</details>

### `source`.`flight_data_recorders_raw_files`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: processing_date, file_name, file_signature
- Primary key: processing_date, file_name, file_signature
- Total rows: 18994
- Total bytes: 2328325

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| file_name | String |  | 1 | 1 | 0 | Имя файла (как в источнике) |
| file_created_at | DateTime |  | 0 | 0 | 0 | Дата появления файла в папке (UTC, st_ctime/creation) |
| file_modified_at | DateTime |  | 0 | 0 | 0 | Дата изменения файла (UTC) |
| file_size | UInt64 |  | 0 | 0 | 0 | Размер файла в байтах |
| file_signature | String |  | 1 | 1 | 0 | Подпись файла: hash(name + size + modified_at) |
| source_path | String |  | 0 | 0 | 0 | Полный путь к каталогу/файлу на сетевой шаре |
| processing_date | Date |  | 1 | 1 | 1 | Дата обработки файла (когда файл был забран из источника) |
| file_date | Date |  | 0 | 0 | 0 | Дата файла, извлеченная из имени файла |
| s3_bucket | LowCardinality(String) |  | 0 | 0 | 0 | Имя S3 бакета с файлом |
| s3_key | String |  | 0 | 0 | 0 | Ключ объекта в S3 |
| s3_etag | String |  | 0 | 0 | 0 | ETag, зафиксированный после загрузки в S3 |
| s3_endpoint_url | LowCardinality(String) |  | 0 | 0 | 0 | Endpoint URL S3-совместимого хранилища |
| s3_region | LowCardinality(String) |  | 0 | 0 | 0 | Регион S3 хранилища |
| s3_url | String |  | 0 | 0 | 0 | Полный URL файла в S3 (s3://bucket/key) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.flight_data_recorders_raw_files (`file_name` String COMMENT 'Имя файла (как в источнике)', `file_created_at` DateTime COMMENT 'Дата появления файла в папке (UTC, st_ctime/creation)', `file_modified_at` DateTime COMMENT 'Дата изменения файла (UTC)', `file_size` UInt64 COMMENT 'Размер файла в байтах', `file_signature` String COMMENT 'Подпись файла: hash(name + size + modified_at)', `source_path` String COMMENT 'Полный путь к каталогу/файлу на сетевой шаре', `processing_date` Date COMMENT 'Дата обработки файла (когда файл был забран из источника)', `file_date` Date COMMENT 'Дата файла, извлеченная из имени файла', `s3_bucket` LowCardinality(String) COMMENT 'Имя S3 бакета с файлом', `s3_key` String COMMENT 'Ключ объекта в S3', `s3_etag` String COMMENT 'ETag, зафиксированный после загрузки в S3', `s3_endpoint_url` LowCardinality(String) COMMENT 'Endpoint URL S3-совместимого хранилища', `s3_region` LowCardinality(String) COMMENT 'Регион S3 хранилища', `s3_url` String COMMENT 'Полный URL файла в S3 (s3://bucket/key)', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/flight_data_recorders_raw_files', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (processing_date, file_name, file_signature) SETTINGS index_granularity = 8192 COMMENT 'Информация о сырых файлах полетных данных, сохраненных в S3.'
```
</details>

### `source`.`history_of_currency`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: processing_date
- Primary key: processing_date
- Total rows: 80150
- Total bytes: 1027924

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| response_data | String |  | 0 | 0 | 0 | Запись с курсами валют за день в виде JSON-строки |
| processing_date | Date |  | 1 | 1 | 0 | Дата обработки файла, на которую зафиксирован курс валюты (из data_interval_start DAG) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.history_of_currency (`response_data` String COMMENT 'Запись с курсами валют за день в виде JSON-строки', `processing_date` Date COMMENT 'Дата обработки файла, на которую зафиксирован курс валюты (из data_interval_start DAG)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/history_of_currency', '{replica}') ORDER BY processing_date SETTINGS index_granularity = 8192 COMMENT 'Данные истории обмена курсов валют по отношению к рублю'
```
</details>

### `source`.`lime_survey_answers_after_flight`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: toDate(datestamp), processing_dt, id
- Primary key: toDate(datestamp), processing_dt, id
- Total rows: 355849
- Total bytes: 17883283

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id | UInt32 |  | 1 | 1 | 0 | Уникальный идентификатор ответа в LimeSurvey |
| start_dt | DateTime |  | 0 | 0 | 0 | Момент начала заполнения анкеты |
| submit_dt | Nullable(DateTime) |  | 0 | 0 | 0 | Момент окончания заполнения анкеты |
| datestamp | DateTime |  | 1 | 1 | 0 | Момент отправки анкеты |
| email | Nullable(String) |  | 0 | 0 | 0 | Адрес электронной почты респондента |
| flight_number | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Номер рейса, указанный респондентом (например: UT-123) |
| flight_date | Nullable(String) |  | 0 | 0 | 0 | Дата рейса в строковом формате источника (например: «12января2024») |
| seat | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Номер места на борту |
| ticket | Nullable(String) |  | 0 | 0 | 0 | Номер авиабилета пассажира |
| departure_airport | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Город аэропорта вылета |
| arrival_airport | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Город аэропорта прилета |
| gender | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Пол респондента |
| birthday | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Дата рождения в строковом формате источника |
| flight_rating | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Общая оценка перелёта (строка, 0–5) |
| website_rating | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Оценка веб-сайта Utair (строка, 0–5) |
| airport_rating | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Оценка сервиса в аэропорту (строка, 0–5) |
| board_rating | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Оценка сервиса на борту (строка, 0–5) |
| ticket_purchase | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Покупал билет через сайт: Y — да, NULL — нет |
| addon_services | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Оформлял дополнительные услуги через сайт: Y — да, NULL — нет |
| checkin | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Проходил регистрацию на рейс через сайт: Y — да, NULL — нет |
| other1 | Nullable(String) |  | 0 | 0 | 0 | Свободный комментарий о действиях на сайте и в приложении |
| with_ticket | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Доп. услуга? оформлена при покупке билета: Y — да, NULL — нет |
| after_purchase | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Доп. услуга? оформлена после покупки билета: Y — да, NULL — нет |
| online_checkin | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Доп. услуга? оформлена на онлайн-регистрации: Y — да, NULL — нет |
| at_airport | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Доп. услуга? оформлена в аэропорту: Y — да, NULL — нет |
| convenience_1 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Удобство использования 1: Y — отмечено, NULL — нет |
| info_completeness_1 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Полнота информации 1: Y — отмечено, NULL — нет |
| miles_promo_use_1 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Возможность применить мили/промокод 1: Y — отмечено, NULL — нет |
| convenience_2 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Удобство использования 2: Y — отмечено, NULL — нет |
| info_completeness_2 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Полнота информации 2: Y — отмечено, NULL — нет |
| miles_promo_use_2 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Возможность применить мили/промокод 2: Y — отмечено, NULL — нет |
| convenience_3 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Удобство использования 3: Y — отмечено, NULL — нет |
| info_completeness_3 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Полнота информации 3: Y — отмечено, NULL — нет |
| miles_promo_use_3 | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Возможность применить мили/промокод 3: Y — отмечено, NULL — нет |
| self_checkin | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Прошёл самостоятельную онлайн-регистрацию: Y — да, NULL — нет |
| counter_checkin | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Прошёл регистрацию на стойке в аэропорту: Y — да, NULL — нет |
| boarding_area | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Воспользовался зоной выхода на посадку: Y — да, NULL — нет |
| other2 | Nullable(String) |  | 0 | 0 | 0 | Свободный комментарий о сервисе в аэропорте |
| print_bp_self | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Распечатал посадочный талон самостоятельно: Y — да, NULL — нет |
| mobile_bp_scan | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Прошёл досмотр с экрана мобильного устройства: Y — да, NULL — нет |
| no_options | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Не воспользовался ни одной из перечисленных возможностей: Y — да, NULL — нет |
| staff_communication | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил коммуникацию и доброжелательность персонала аэропорта: Y — да, NULL — нет |
| baggage_rules_clarity | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил понятность правил провоза багажа: Y — да, NULL — нет |
| addon_service_speed | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил скорость и удобство оформления дополнительных услуг: Y — да, NULL — нет |
| gate_info | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил информирование о номере выхода на посадку и времени посадки: Y — да, NULL — нет |
| display_info_quality | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил актуальность и наглядность информации на табло: Y — да, NULL — нет |
| cabin_condition | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил состояние салона самолёта: Y — да, NULL — нет |
| onboard_info | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил качество информирования пассажиров на борту: Y — да, NULL — нет |
| cabin_crew | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил работу бортпроводников: Y — да, NULL — нет |
| other3 | Nullable(String) |  | 0 | 0 | 0 | Свободный комментарий о качестве сервиса на борту |
| seat_equipment_condition | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил исправность кресел, подлокотников, столиков и освещения: Y — да, NULL — нет |
| cleanliness | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил чистоту салона: Y — да, NULL — нет |
| captain_announcements | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил объявления командира воздушного судна о полёте: Y — да, NULL — нет |
| pa_clarity | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил громкость и чёткость объявлений бортпроводников: Y — да, NULL — нет |
| info_clarity | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил понятность информации о правилах поведения на борту: Y — да, NULL — нет |
| service_info | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил информирование о доступных услугах (бесплатных и платных): Y — да, NULL — нет |
| responsiveness | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил отзывчивость бортпроводников: Y — да, NULL — нет |
| politeness | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил вежливость бортпроводников: Y — да, NULL — нет |
| issue_handling | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Отметил реагирование бортпроводников на нестандартные ситуации: Y — да, NULL — нет |
| open_feedback | Nullable(String) |  | 0 | 0 | 0 | Развёрнутый отзыв пассажира о перелёте в свободной форме |
| processing_dt | DateTime |  | 1 | 1 | 0 | Дата и время обработки записи в DAG (UTC) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Момент загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.lime_survey_answers_after_flight (`id` UInt32 COMMENT 'Уникальный идентификатор ответа в LimeSurvey', `start_dt` DateTime COMMENT 'Момент начала заполнения анкеты', `submit_dt` Nullable(DateTime) COMMENT 'Момент окончания заполнения анкеты', `datestamp` DateTime COMMENT 'Момент отправки анкеты', `email` Nullable(String) COMMENT 'Адрес электронной почты респондента', `flight_number` LowCardinality(Nullable(String)) COMMENT 'Номер рейса, указанный респондентом (например: UT-123)', `flight_date` Nullable(String) COMMENT 'Дата рейса в строковом формате источника (например: «12января2024»)', `seat` LowCardinality(Nullable(String)) COMMENT 'Номер места на борту', `ticket` Nullable(String) COMMENT 'Номер авиабилета пассажира', `departure_airport` LowCardinality(Nullable(String)) COMMENT 'Город аэропорта вылета', `arrival_airport` LowCardinality(Nullable(String)) COMMENT 'Город аэропорта прилета', `gender` LowCardinality(Nullable(String)) COMMENT 'Пол респондента', `birthday` LowCardinality(Nullable(String)) COMMENT 'Дата рождения в строковом формате источника', `flight_rating` LowCardinality(Nullable(String)) COMMENT 'Общая оценка перелёта (строка, 0–5)', `website_rating` LowCardinality(Nullable(String)) COMMENT 'Оценка веб-сайта Utair (строка, 0–5)', `airport_rating` LowCardinality(Nullable(String)) COMMENT 'Оценка сервиса в аэропорту (строка, 0–5)', `board_rating` LowCardinality(Nullable(String)) COMMENT 'Оценка сервиса на борту (строка, 0–5)', `ticket_purchase` LowCardinality(Nullable(String)) COMMENT 'Покупал билет через сайт: Y — да, NULL — нет', `addon_services` LowCardinality(Nullable(String)) COMMENT 'Оформлял дополнительные услуги через сайт: Y — да, NULL — нет', `checkin` LowCardinality(Nullable(String)) COMMENT 'Проходил регистрацию на рейс через сайт: Y — да, NULL — нет', `other1` Nullable(String) COMMENT 'Свободный комментарий о действиях на сайте и в приложении' CODEC(ZSTD(3)), `with_ticket` LowCardinality(Nullable(String)) COMMENT 'Доп. услуга? оформлена при покупке билета: Y — да, NULL — нет', `after_purchase` LowCardinality(Nullable(String)) COMMENT 'Доп. услуга? оформлена после покупки билета: Y — да, NULL — нет', `online_checkin` LowCardinality(Nullable(String)) COMMENT 'Доп. услуга? оформлена на онлайн-регистрации: Y — да, NULL — нет', `at_airport` LowCardinality(Nullable(String)) COMMENT 'Доп. услуга? оформлена в аэропорту: Y — да, NULL — нет', `convenience_1` LowCardinality(Nullable(String)) COMMENT 'Удобство использования 1: Y — отмечено, NULL — нет', `info_completeness_1` LowCardinality(Nullable(String)) COMMENT 'Полнота информации 1: Y — отмечено, NULL — нет', `miles_promo_use_1` LowCardinality(Nullable(String)) COMMENT 'Возможность применить мили/промокод 1: Y — отмечено, NULL — нет', `convenience_2` LowCardinality(Nullable(String)) COMMENT 'Удобство использования 2: Y — отмечено, NULL — нет', `info_completeness_2` LowCardinality(Nullable(String)) COMMENT 'Полнота информации 2: Y — отмечено, NULL — нет', `miles_promo_use_2` LowCardinality(Nullable(String)) COMMENT 'Возможность применить мили/промокод 2: Y — отмечено, NULL — нет', `convenience_3` LowCardinality(Nullable(String)) COMMENT 'Удобство использования 3: Y — отмечено, NULL — нет', `info_completeness_3` LowCardinality(Nullable(String)) COMMENT 'Полнота информации 3: Y — отмечено, NULL — нет', `miles_promo_use_3` LowCardinality(Nullable(String)) COMMENT 'Возможность применить мили/промокод 3: Y — отмечено, NULL — нет', `self_checkin` LowCardinality(Nullable(String)) COMMENT 'Прошёл самостоятельную онлайн-регистрацию: Y — да, NULL — нет', `counter_checkin` LowCardinality(Nullable(String)) COMMENT 'Прошёл регистрацию на стойке в аэропорту: Y — да, NULL — нет', `boarding_area` LowCardinality(Nullable(String)) COMMENT 'Воспользовался зоной выхода на посадку: Y — да, NULL — нет', `other2` Nullable(String) COMMENT 'Свободный комментарий о сервисе в аэропорте' CODEC(ZSTD(3)), `print_bp_self` LowCardinality(Nullable(String)) COMMENT 'Распечатал посадочный талон самостоятельно: Y — да, NULL — нет', `mobile_bp_scan` LowCardinality(Nullable(String)) COMMENT 'Прошёл досмотр с экрана мобильного устройства: Y — да, NULL — нет', `no_options` LowCardinality(Nullable(String)) COMMENT 'Не воспользовался ни одной из перечисленных возможностей: Y — да, NULL — нет', `staff_communication` LowCardinality(Nullable(String)) COMMENT 'Отметил коммуникацию и доброжелательность персонала аэропорта: Y — да, NULL — нет', `baggage_rules_clarity` LowCardinality(Nullable(String)) COMMENT 'Отметил понятность правил провоза багажа: Y — да, NULL — нет', `addon_service_speed` LowCardinality(Nullable(String)) COMMENT 'Отметил скорость и удобство оформления дополнительных услуг: Y — да, NULL — нет', `gate_info` LowCardinality(Nullable(String)) COMMENT 'Отметил информирование о номере выхода на посадку и времени посадки: Y — да, NULL — нет', `display_info_quality` LowCardinality(Nullable(String)) COMMENT 'Отметил актуальность и наглядность информации на табло: Y — да, NULL — нет', `cabin_condition` LowCardinality(Nullable(String)) COMMENT 'Отметил состояние салона самолёта: Y — да, NULL — нет', `onboard_info` LowCardinality(Nullable(String)) COMMENT 'Отметил качество информирования пассажиров на борту: Y — да, NULL — нет', `cabin_crew` LowCardinality(Nullable(String)) COMMENT 'Отметил работу бортпроводников: Y — да, NULL — нет', `other3` Nullable(String) COMMENT 'Свободный комментарий о качестве сервиса на борту' CODEC(ZSTD(3)), `seat_equipment_condition` LowCardinality(Nullable(String)) COMMENT 'Отметил исправность кресел, подлокотников, столиков и освещения: Y — да, NULL — нет', `cleanliness` LowCardinality(Nullable(String)) COMMENT 'Отметил чистоту салона: Y — да, NULL — нет', `captain_announcements` LowCardinality(Nullable(String)) COMMENT 'Отметил объявления командира воздушного судна о полёте: Y — да, NULL — нет', `pa_clarity` LowCardinality(Nullable(String)) COMMENT 'Отметил громкость и чёткость объявлений бортпроводников: Y — да, NULL — нет', `info_clarity` LowCardinality(Nullable(String)) COMMENT 'Отметил понятность информации о правилах поведения на борту: Y — да, NULL — нет', `service_info` LowCardinality(Nullable(String)) COMMENT 'Отметил информирование о доступных услугах (бесплатных и платных): Y — да, NULL — нет', `responsiveness` LowCardinality(Nullable(String)) COMMENT 'Отметил отзывчивость бортпроводников: Y — да, NULL — нет', `politeness` LowCardinality(Nullable(String)) COMMENT 'Отметил вежливость бортпроводников: Y — да, NULL — нет', `issue_handling` LowCardinality(Nullable(String)) COMMENT 'Отметил реагирование бортпроводников на нестандартные ситуации: Y — да, NULL — нет', `open_feedback` Nullable(String) COMMENT 'Развёрнутый отзыв пассажира о перелёте в свободной форме' CODEC(ZSTD(3)), `processing_dt` DateTime COMMENT 'Дата и время обработки записи в DAG (UTC)', `meta_source` LowCardinality(String) COMMENT 'Источник. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Момент загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/lime_survey_answers_after_flight', '{replica}') ORDER BY (toDate(datestamp), processing_dt, id) SETTINGS index_granularity = 8192 COMMENT 'Сырые ответы пассажиров на послеполётный опрос LimeSurvey №399614. Оценки — строки 0–5, флаги выбора — Y/NULL'
```
</details>

### `source`.`mongo_monoapp_cart`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: updated_at, id_document
- Primary key: updated_at, id_document
- Total rows: 9622955
- Total bytes: 1105680034

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id_document | String |  | 1 | 1 | 0 | ID документа рейса (MongoDD _id) |
| document | String |  | 0 | 0 | 0 | Исходный mongo-документ в виде JSON-строки |
| updated_at | DateTime |  | 1 | 1 | 1 | Время последнего обновления документа (дата выполнения дага) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи.Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC).Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.mongo_monoapp_cart (`id_document` String COMMENT 'ID документа рейса (MongoDD _id)', `document` String COMMENT 'Исходный mongo-документ в виде JSON-строки', `updated_at` DateTime COMMENT 'Время последнего обновления документа (дата выполнения дага)', `meta_source` LowCardinality(String) COMMENT 'Источник записи.Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC).Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/mongo_monoapp_cart', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (updated_at, id_document) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из cart'
```
</details>

### `source`.`mongo_monoapp_sirena_flights`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(updated_at)
- Sorting key: updated_at, id_document
- Primary key: updated_at, id_document
- Total rows: 129929
- Total bytes: 2027338477

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id_document | String |  | 1 | 1 | 0 | ID документа рейса (MongoDD _id) |
| document | String |  | 0 | 0 | 0 | Исходный mongo-документ в виде JSON-строки |
| updated_at | DateTime64(3, 'UTC') |  | 1 | 1 | 1 | Время последнего обновления документа (дата выполнения дага) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (например, DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.mongo_monoapp_sirena_flights (`id_document` String COMMENT 'ID документа рейса (MongoDD _id)', `document` String COMMENT 'Исходный mongo-документ в виде JSON-строки', `updated_at` DateTime64(3, 'UTC') COMMENT 'Время последнего обновления документа (дата выполнения дага)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (например, DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/mongo_monoapp_sirena_flights', '{replica}') PARTITION BY toYYYYMM(updated_at) ORDER BY (updated_at, id_document) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из sirena_flights'
```
</details>

### `source`.`smsc_messages`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: processing_date, send_date, reseller_login
- Primary key: processing_date, send_date, reseller_login
- Total rows: 2031940
- Total bytes: 105067678

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id | String |  | 0 | 0 | 0 | Уникальный идентификатор сообщения |
| int_id | String |  | 0 | 0 | 0 | Внешний идентификатор сообщения |
| last_date | DateTime |  | 0 | 0 | 0 | Дата и время последнего обновления |
| last_timestamp | UInt32 |  | 0 | 0 | 0 | Timestamp последнего обновления |
| send_date | DateTime |  | 1 | 1 | 0 | Дата и время отправки |
| send_timestamp | UInt32 |  | 0 | 0 | 0 | Timestamp отправки |
| processing_date | Date |  | 1 | 1 | 1 | Дата обработки сообщения |
| phone | String |  | 0 | 0 | 0 | Номер телефона получателя |
| sender_id | String |  | 0 | 0 | 0 | Идентификатор отправителя |
| reseller_login | String |  | 1 | 1 | 0 | Логин реселлера |
| mccmnc | String |  | 0 | 0 | 0 | MCC и MNC код оператора |
| country | String |  | 0 | 0 | 0 | Страна получателя |
| operator | String |  | 0 | 0 | 0 | Нормализованное название оператора |
| operator_orig | String |  | 0 | 0 | 0 | Оригинальное название оператора |
| region | String |  | 0 | 0 | 0 | Регион получателя |
| status | UInt16 |  | 0 | 0 | 0 | Статус доставки |
| status_name | String |  | 0 | 0 | 0 | Название статуса |
| flag | UInt16 |  | 0 | 0 | 0 | Флаг сообщения |
| type | UInt8 |  | 0 | 0 | 0 | Тип сообщения |
| format | UInt8 |  | 0 | 0 | 0 | Формат сообщения (0-текст, 1-unicode и т.д.) |
| err | UInt64 |  | 0 | 0 | 0 | Код ошибки (если есть) |
| message | String |  | 0 | 0 | 0 | Текст сообщения |
| sms_cnt | Nullable(UInt8) |  | 0 | 0 | 0 | Количество SMS частей |
| cost | Decimal(10, 3) |  | 0 | 0 | 0 | Стоимость сообщения |
| crc | UInt32 |  | 0 | 0 | 0 | Контрольная сумма |
| comment | String | '' | 0 | 0 | 0 | Комментарий к сообщению |
| send_retry | UInt8 | 0 | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.smsc_messages (`id` String COMMENT 'Уникальный идентификатор сообщения', `int_id` String COMMENT 'Внешний идентификатор сообщения', `last_date` DateTime COMMENT 'Дата и время последнего обновления', `last_timestamp` UInt32 COMMENT 'Timestamp последнего обновления', `send_date` DateTime COMMENT 'Дата и время отправки', `send_timestamp` UInt32 COMMENT 'Timestamp отправки', `processing_date` Date COMMENT 'Дата обработки сообщения', `phone` String COMMENT 'Номер телефона получателя', `sender_id` String COMMENT 'Идентификатор отправителя', `reseller_login` String COMMENT 'Логин реселлера', `mccmnc` String COMMENT 'MCC и MNC код оператора', `country` String COMMENT 'Страна получателя', `operator` String COMMENT 'Нормализованное название оператора', `operator_orig` String COMMENT 'Оригинальное название оператора', `region` String COMMENT 'Регион получателя', `status` UInt16 COMMENT 'Статус доставки', `status_name` String COMMENT 'Название статуса', `flag` UInt16 COMMENT 'Флаг сообщения', `type` UInt8 COMMENT 'Тип сообщения', `format` UInt8 COMMENT 'Формат сообщения (0-текст, 1-unicode и т.д.)', `err` UInt64 COMMENT 'Код ошибки (если есть)', `message` String COMMENT 'Текст сообщения', `sms_cnt` Nullable(UInt8) COMMENT 'Количество SMS частей', `cost` Decimal(10, 3) COMMENT 'Стоимость сообщения', `crc` UInt32 COMMENT 'Контрольная сумма', `comment` String DEFAULT '' COMMENT 'Комментарий к сообщению', `send_retry` UInt8 DEFAULT 0) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/smsc_messages', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (processing_date, send_date, reseller_login) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения SMS сообщений из SMSC'
```
</details>

### `source`.`sofi_bi_sales_ut`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(transaction_date)
- Sorting key: transaction_date, document_number, coupon_number
- Primary key: transaction_date, document_number, coupon_number
- Total rows: 23950245
- Total bytes: 1483731013

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| document_number | String |  | 1 | 1 | 0 | Номер документа (бланка) |
| coupon_number | UInt8 |  | 1 | 1 | 0 | Номер купона |
| document_kind | LowCardinality(String) |  | 0 | 0 | 0 | Вид операции (бланка) |
| carrier | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Перевозчик |
| flight_number | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Номер рейса |
| origin | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Пункт вылета |
| destination | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Пункт прилета |
| agency_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Код агента |
| agency_name | LowCardinality(String) |  | 0 | 0 | 0 | Наименование агента |
| agency_number_code | String |  | 0 | 0 | 0 | Пункт продажи (валидатор) |
| document_coupon_count | UInt8 |  | 0 | 0 | 0 | Количество купонов документа |
| sale_segment_count | Int8 |  | 0 | 0 | 0 | Количество проданных сегментов |
| service_class | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Класс бронирования |
| fare_basis | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Вид тарифа |
| tax_rub_cpfp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Тариф на купоне РУБ |
| tax_rub_yq_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Топливный сбор РУБ |
| tax_rub_zz_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Такса ТКП (ZZ) РУБ |
| tax_rub_cp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Штраф за возврат(обмен) (CP) РУБ |
| tax_rub_ru_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор RU РУБ |
| tax_rub_psyr_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор за бланк (PS,YR) РУБ |
| tax_rub_another_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Прочие сборы РУБ |
| currency | LowCardinality(String) |  | 0 | 0 | 0 | Валюта |
| tax_cpfp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Тариф на купоне |
| tax_yq_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Топливный сбор |
| tax_zz_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Такса ТКП (ZZ) |
| tax_cp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Штраф за возврат(обмен) (CP) |
| tax_ru_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор RU |
| tax_psyr_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор за бланк (PS,YR) |
| tax_another_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Прочие сборы |
| transaction_date | Date |  | 1 | 1 | 1 | Дата транзакции |
| flight_date | Nullable(Date) |  | 0 | 0 | 0 | Дата вылета |
| gds | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Система бронирования |
| sale_system | LowCardinality(String) |  | 0 | 0 | 0 | Система продажи |
| ff_card_passenger | Nullable(String) |  | 0 | 0 | 0 | Номер карты частолетающего пассажира |
| ff_amount | Int32 |  | 0 | 0 | 0 | Сумма бонусов на купоне (эквив. РУБ) |
| is_exchange | LowCardinality(String) |  | 0 | 0 | 0 | Участвует в обмене? |
| exchange_document_number | Nullable(String) |  | 0 | 0 | 0 | Номер документа (обмен) |
| supplement_currency | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Валюта доплаты |
| supplement_amount | Nullable(Int32) |  | 0 | 0 | 0 | Сумма доплаты |
| supplement_amount_rub | Nullable(Int32) |  | 0 | 0 | 0 | Сумма доплаты РУБ |
| fop_string | String |  | 0 | 0 | 0 | Примечание по ФОП |
| is_rt | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Перевозка туда-обратно |
| is_tr | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Трансферная перевозка |
| brand | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Бренд |
| category_code | LowCardinality(String) |  | 0 | 0 | 0 | Категория пассажира |
| reason_issuance_sub_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Причина выписки купона EMD |
| reason_issuance_sub_grp | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Группа доп.услуг |
| is_ff | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | FF mark |
| tariff_structure | Nullable(String) |  | 0 | 0 | 0 | Структура тарифа |
| pnr | Nullable(String) |  | 0 | 0 | 0 | Номер брони |
| voucher_num | Nullable(String) |  | 0 | 0 | 0 | Номер ваучера |
| return_exchange_voucher_num | Nullable(String) |  | 0 | 0 | 0 | Номера Возвращаемых В Обмен На Ваучер Документов |
| is_return_exchange_voucher | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Возврат в обмен на ваучер |
| vouncher | LowCardinality(String) |  | 0 | 0 | 0 | Ваучер |
| exchange_voucher_usl | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Обмен ваучера на билет/услугу |
| passenger_name | Nullable(String) |  | 0 | 0 | 0 | ФИО пассажира |
| passenger_passport | Nullable(String) |  | 0 | 0 | 0 | Паспорт пассажира |
| passenger_birth_date | Nullable(Date32) |  | 0 | 0 | 0 | День рождения пассажира |
| emd_related_ticket_number | Nullable(String) |  | 0 | 0 | 0 | EMD - связанный документ |
| emd_related_coupon_number | Nullable(UInt8) |  | 0 | 0 | 0 | EMD - номер купона связанного документа |
| source | LowCardinality(String) |  | 0 | 0 | 0 | Источник |
| updated | DateTime64(3, 'UTC') |  | 0 | 0 | 0 | Время последнего обновления документа |
| processing_dt | DateTime('UTC') |  | 0 | 0 | 0 | Дата обработки (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Имя дага |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE source.sofi_bi_sales_ut (`document_number` String COMMENT 'Номер документа (бланка)', `coupon_number` UInt8 COMMENT 'Номер купона', `document_kind` LowCardinality(String) COMMENT 'Вид операции (бланка)', `carrier` LowCardinality(Nullable(String)) COMMENT 'Перевозчик', `flight_number` LowCardinality(Nullable(String)) COMMENT 'Номер рейса', `origin` LowCardinality(Nullable(String)) COMMENT 'Пункт вылета', `destination` LowCardinality(Nullable(String)) COMMENT 'Пункт прилета', `agency_code` LowCardinality(Nullable(String)) COMMENT 'Код агента', `agency_name` LowCardinality(String) COMMENT 'Наименование агента', `agency_number_code` String COMMENT 'Пункт продажи (валидатор)', `document_coupon_count` UInt8 COMMENT 'Количество купонов документа', `sale_segment_count` Int8 COMMENT 'Количество проданных сегментов', `service_class` LowCardinality(Nullable(String)) COMMENT 'Класс бронирования', `fare_basis` LowCardinality(Nullable(String)) COMMENT 'Вид тарифа', `tax_rub_cpfp_amount` Decimal(18, 4) COMMENT 'Тариф на купоне РУБ', `tax_rub_yq_amount` Decimal(18, 4) COMMENT 'Топливный сбор РУБ', `tax_rub_zz_amount` Decimal(18, 4) COMMENT 'Такса ТКП (ZZ) РУБ', `tax_rub_cp_amount` Decimal(18, 4) COMMENT 'Штраф за возврат(обмен) (CP) РУБ', `tax_rub_ru_amount` Decimal(18, 4) COMMENT 'Сбор RU РУБ', `tax_rub_psyr_amount` Decimal(18, 4) COMMENT 'Сбор за бланк (PS,YR) РУБ', `tax_rub_another_amount` Decimal(18, 4) COMMENT 'Прочие сборы РУБ', `currency` LowCardinality(String) COMMENT 'Валюта', `tax_cpfp_amount` Decimal(18, 4) COMMENT 'Тариф на купоне', `tax_yq_amount` Decimal(18, 4) COMMENT 'Топливный сбор', `tax_zz_amount` Decimal(18, 4) COMMENT 'Такса ТКП (ZZ)', `tax_cp_amount` Decimal(18, 4) COMMENT 'Штраф за возврат(обмен) (CP)', `tax_ru_amount` Decimal(18, 4) COMMENT 'Сбор RU', `tax_psyr_amount` Decimal(18, 4) COMMENT 'Сбор за бланк (PS,YR)', `tax_another_amount` Decimal(18, 4) COMMENT 'Прочие сборы', `transaction_date` Date COMMENT 'Дата транзакции', `flight_date` Nullable(Date) COMMENT 'Дата вылета', `gds` LowCardinality(Nullable(String)) COMMENT 'Система бронирования', `sale_system` LowCardinality(String) COMMENT 'Система продажи', `ff_card_passenger` Nullable(String) COMMENT 'Номер карты частолетающего пассажира', `ff_amount` Int32 COMMENT 'Сумма бонусов на купоне (эквив. РУБ)', `is_exchange` LowCardinality(String) COMMENT 'Участвует в обмене?', `exchange_document_number` Nullable(String) COMMENT 'Номер документа (обмен)', `supplement_currency` LowCardinality(Nullable(String)) COMMENT 'Валюта доплаты', `supplement_amount` Nullable(Int32) COMMENT 'Сумма доплаты', `supplement_amount_rub` Nullable(Int32) COMMENT 'Сумма доплаты РУБ', `fop_string` String COMMENT 'Примечание по ФОП' CODEC(ZSTD(3)), `is_rt` LowCardinality(Nullable(String)) COMMENT 'Перевозка туда-обратно', `is_tr` LowCardinality(Nullable(String)) COMMENT 'Трансферная перевозка', `brand` LowCardinality(Nullable(String)) COMMENT 'Бренд', `category_code` LowCardinality(String) COMMENT 'Категория пассажира', `reason_issuance_sub_code` LowCardinality(Nullable(String)) COMMENT 'Причина выписки купона EMD', `reason_issuance_sub_grp` LowCardinality(Nullable(String)) COMMENT 'Группа доп.услуг', `is_ff` LowCardinality(Nullable(String)) COMMENT 'FF mark', `tariff_structure` Nullable(String) COMMENT 'Структура тарифа', `pnr` Nullable(String) COMMENT 'Номер брони', `voucher_num` Nullable(String) COMMENT 'Номер ваучера', `return_exchange_voucher_num` Nullable(String) COMMENT 'Номера Возвращаемых В Обмен На Ваучер Документов', `is_return_exchange_voucher` LowCardinality(Nullable(String)) COMMENT 'Возврат в обмен на ваучер', `vouncher` LowCardinality(String) COMMENT 'Ваучер', `exchange_voucher_usl` LowCardinality(Nullable(String)) COMMENT 'Обмен ваучера на билет/услугу', `passenger_name` Nullable(String) COMMENT 'ФИО пассажира' CODEC(ZSTD(3)), `passenger_passport` Nullable(String) COMMENT 'Паспорт пассажира' CODEC(ZSTD(3)), `passenger_birth_date` Nullable(Date32) COMMENT 'День рождения пассажира', `emd_related_ticket_number` Nullable(String) COMMENT 'EMD - связанный документ', `emd_related_coupon_number` Nullable(UInt8) COMMENT 'EMD - номер купона связанного документа', `source` LowCardinality(String) COMMENT 'Источник', `updated` DateTime64(3, 'UTC') COMMENT 'Время последнего обновления документа', `processing_dt` DateTime('UTC') COMMENT 'Дата обработки (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Имя дага', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC).') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/source/sofi_bi_sales_ut', '{replica}') PARTITION BY toYYYYMM(transaction_date) ORDER BY (transaction_date, document_number, coupon_number) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения сырых данных из view SOFI_BI.SALES_UT_DATA2021VIEW БД Sofi2021'
```
</details>

### `analytics`.`amos_heli_ac_typ_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_type_i | Int32 |  | 0 | 0 | 0 |  |
| ac_typ | String |  | 0 | 0 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| fa_ac_typ | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_ac_typ_view (`ac_type_i` Int32, `ac_typ` String, `description` Nullable(String), `fa_ac_typ` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_ac_typ
```
</details>

### `analytics`.`amos_heli_address_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| address_i | Int32 |  | 0 | 0 | 0 |  |
| vendor | LowCardinality(String) |  | 0 | 0 | 0 |  |
| name | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_address_view (`address_i` Int32, `vendor` LowCardinality(String), `name` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_address
```
</details>

### `analytics`.`amos_heli_adr_properties_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| address_i | Int32 |  | 0 | 0 | 0 |  |
| prop_type_i | Int32 |  | 0 | 0 | 0 |  |
| remarks | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| value | LowCardinality(String) |  | 0 | 0 | 0 |  |
| status | Nullable(Int8) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_adr_properties_view (`address_i` Int32, `prop_type_i` Int32, `remarks` LowCardinality(Nullable(String)), `value` LowCardinality(String), `status` Nullable(Int8), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_adr_properties
```
</details>

### `analytics`.`amos_heli_adr_special_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| special_i | Int32 |  | 0 | 0 | 0 |  |
| address_i | Int32 |  | 0 | 0 | 0 |  |
| special | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| remarks | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| amount | Nullable(Int8) |  | 0 | 0 | 0 |  |
| reference_no | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_adr_special_view (`special_i` Int32, `address_i` Int32, `special` LowCardinality(Nullable(String)), `remarks` LowCardinality(Nullable(String)), `amount` Nullable(Int8), `reference_no` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_adr_special
```
</details>

### `analytics`.`amos_heli_aircraft_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_registr | String |  | 0 | 0 | 0 |  |
| ac_typ | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ac_registr_prefix | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| ac_model | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| owner | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| manual_owner | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| status | Int16 |  | 0 | 0 | 0 |  |
| non_managed | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| homebase | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| object_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| description | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_aircraft_view (`ac_registr` String, `ac_typ` LowCardinality(String), `ac_registr_prefix` LowCardinality(Nullable(String)), `ac_model` LowCardinality(Nullable(String)), `owner` LowCardinality(Nullable(String)), `manual_owner` LowCardinality(Nullable(String)), `status` Int16, `non_managed` LowCardinality(Nullable(String)), `homebase` LowCardinality(Nullable(String)), `object_type` LowCardinality(Nullable(String)), `description` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_aircraft
```
</details>

### `analytics`.`amos_heli_applicability_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| applicabilityno_i | Int64 |  | 0 | 0 | 0 |  |
| effectivityno_i | Int32 |  | 0 | 0 | 0 |  |
| applicable | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ref_key | Int64 |  | 0 | 0 | 0 |  |
| ref_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_applicability_view (`applicabilityno_i` Int64, `effectivityno_i` Int32, `applicable` LowCardinality(String), `ref_key` Int64, `ref_type` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_applicability
```
</details>

### `analytics`.`amos_heli_condition_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| condition | LowCardinality(String) |  | 0 | 0 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_condition_view (`condition` LowCardinality(String), `description` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_condition
```
</details>

### `analytics`.`amos_heli_counter_definition_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counter_defno_i | UInt64 |  | 0 | 0 | 0 |  |
| code | LowCardinality(String) |  | 0 | 0 | 0 |  |
| name | Nullable(String) |  | 0 | 0 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| display_unit | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_counter_definition_view (`counter_defno_i` UInt64, `code` LowCardinality(String), `name` Nullable(String), `description` Nullable(String), `display_unit` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_counter_definition
```
</details>

### `analytics`.`amos_heli_counter_template_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counter_templateno_i | Int32 |  | 0 | 0 | 0 |  |
| counter_defno_i | Int32 |  | 0 | 0 | 0 |  |
| counter_template_groupno_i | Int32 |  | 0 | 0 | 0 |  |
| type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| is_calculated | LowCardinality(String) |  | 0 | 0 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_counter_template_view (`counter_templateno_i` Int32, `counter_defno_i` Int32, `counter_template_groupno_i` Int32, `type` LowCardinality(String), `is_calculated` LowCardinality(String), `description` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_counter_template
```
</details>

### `analytics`.`amos_heli_counter_value_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counter_valueno_i | Int64 |  | 0 | 0 | 0 |  |
| counterno_i | Int64 |  | 0 | 0 | 0 |  |
| life_value | Nullable(Float64) |  | 0 | 0 | 0 |  |
| readout_ref_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| on_counter_valueno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| off_counter_valueno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| is_minor | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| readout_date | Date32 |  | 0 | 0 | 0 |  |
| readout_time | Int64 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_counter_value_view (`counter_valueno_i` Int64, `counterno_i` Int64, `life_value` Nullable(Float64), `readout_ref_type` LowCardinality(Nullable(String)), `on_counter_valueno_i` Nullable(Int64), `off_counter_valueno_i` Nullable(Int64), `is_minor` LowCardinality(Nullable(String)), `readout_date` Date32, `readout_time` Int64, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_counter_value
```
</details>

### `analytics`.`amos_heli_counter_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counterno_i | Int64 |  | 0 | 0 | 0 |  |
| counter_templateno_i | Int32 |  | 0 | 0 | 0 |  |
| ref_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ref_key | Int64 |  | 0 | 0 | 0 |  |
| life_value | Nullable(Float64) |  | 0 | 0 | 0 |  |
| is_unknown | LowCardinality(String) |  | 0 | 0 | 0 |  |
| status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| master_counterno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| readout_date | Date32 |  | 0 | 0 | 0 |  |
| readout_time | Int64 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_counter_view (`counterno_i` Int64, `counter_templateno_i` Int32, `ref_type` LowCardinality(String), `ref_key` Int64, `life_value` Nullable(Float64), `is_unknown` LowCardinality(String), `status` Nullable(Int16), `master_counterno_i` Nullable(Int64), `readout_date` Date32, `readout_time` Int64, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_counter
```
</details>

### `analytics`.`amos_heli_event_effectivity_link_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| effectivityno_i | Int32 |  | 0 | 0 | 0 |  |
| effectivity_linkno_i | Int32 |  | 0 | 0 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_event_effectivity_link_view (`effectivityno_i` Int32, `effectivity_linkno_i` Int32, `event_type` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_event_effectivity_link
```
</details>

### `analytics`.`amos_heli_event_effectivity_rules_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| effectivityno_i | Int32 |  | 0 | 0 | 0 |  |
| aircraft_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| rotable_manuf_before | Nullable(Int32) |  | 0 | 0 | 0 |  |
| rotable_manuf_after | Nullable(Int32) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_event_effectivity_rules_view (`effectivityno_i` Int32, `aircraft_type` LowCardinality(Nullable(String)), `rotable_manuf_before` Nullable(Int32), `rotable_manuf_after` Nullable(Int32), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_event_effectivity_rules
```
</details>

### `analytics`.`amos_heli_event_effectivity_sns_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| effectivityno_i | Int32 |  | 0 | 0 | 0 |  |
| effectivity_snno_i | Int32 |  | 0 | 0 | 0 |  |
| range_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_event_effectivity_sns_view (`effectivityno_i` Int32, `effectivity_snno_i` Int32, `range_type` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_event_effectivity_sns
```
</details>

### `analytics`.`amos_heli_event_effectivity_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| effectivityno_i | Int32 |  | 0 | 0 | 0 |  |
| effectivity_headerno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| title | String |  | 0 | 0 | 0 |  |
| aircraft_typ | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_event_effectivity_view (`effectivityno_i` Int32, `effectivity_headerno_i` Nullable(Int32), `title` String, `aircraft_typ` LowCardinality(Nullable(String)), `partno` Nullable(String), `status` Nullable(Int16), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_event_effectivity
```
</details>

### `analytics`.`amos_heli_forecast_dimension_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_perfno_i | Int64 |  | 0 | 0 | 0 |  |
| counter_defno_i | Int32 |  | 0 | 0 | 0 |  |
| dimension | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_forecast_dimension_view (`event_perfno_i` Int64, `counter_defno_i` Int32, `dimension` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_forecast_dimension
```
</details>

### `analytics`.`amos_heli_forecast_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_perfno_i | Int64 |  | 0 | 0 | 0 |  |
| psn | Nullable(Int64) |  | 0 | 0 | 0 |  |
| requirement | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_registr | String |  | 0 | 0 | 0 |  |
| ac_typ | LowCardinality(String) |  | 0 | 0 | 0 |  |
| event | String |  | 0 | 0 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_forecast_view (`event_perfno_i` Int64, `psn` Nullable(Int64), `requirement` LowCardinality(Nullable(String)), `partno` Nullable(String), `serialno` Nullable(String), `ac_registr` String, `ac_typ` LowCardinality(String), `event` String, `event_type` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_forecast
```
</details>

### `analytics`.`amos_heli_history_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| historyno_i | Int64 |  | 0 | 0 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| vm | LowCardinality(String) |  | 0 | 0 | 0 |  |
| od_detailno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| ac_registr | Nullable(String) |  | 0 | 0 | 0 |  |
| del_date | Date32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_history_view (`historyno_i` Int64, `partno` String, `serialno` Nullable(String), `vm` LowCardinality(String), `od_detailno_i` Nullable(Int32), `ac_registr` Nullable(String), `del_date` Date32, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_history
```
</details>

### `analytics`.`amos_heli_location_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| locationno_i | Int32 |  | 0 | 0 | 0 |  |
| description | LowCardinality(String) |  | 0 | 0 | 0 |  |
| store | LowCardinality(String) |  | 0 | 0 | 0 |  |
| station | LowCardinality(String) |  | 0 | 0 | 0 |  |
| location | LowCardinality(String) |  | 0 | 0 | 0 |  |
| status | Int16 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_location_view (`locationno_i` Int32, `description` LowCardinality(String), `store` LowCardinality(String), `station` LowCardinality(String), `location` LowCardinality(String), `status` Int16, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_location
```
</details>

### `analytics`.`amos_heli_mevt_effectivity_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| mevt_effectivityno_i | Int64 |  | 0 | 0 | 0 |  |
| mevt_headerno_i | Int64 |  | 0 | 0 | 0 |  |
| effectivity_linkno_i | Int64 |  | 0 | 0 | 0 |  |
| template_revisionno_i | Int64 |  | 0 | 0 | 0 |  |
| timerequirementno_i | Int64 |  | 0 | 0 | 0 |  |
| revision_key | Int32 |  | 0 | 0 | 0 |  |
| revision_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| applicable_status | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_mevt_effectivity_view (`mevt_effectivityno_i` Int64, `mevt_headerno_i` Int64, `effectivity_linkno_i` Int64, `template_revisionno_i` Int64, `timerequirementno_i` Int64, `revision_key` Int32, `revision_type` LowCardinality(String), `applicable_status` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_mevt_effectivity
```
</details>

### `analytics`.`amos_heli_mevt_header_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| mevt_headerno_i | Int64 |  | 0 | 0 | 0 |  |
| identifier | String |  | 0 | 0 | 0 |  |
| ref_key | Int64 |  | 0 | 0 | 0 |  |
| ref_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| mevt_key | Int32 |  | 0 | 0 | 0 |  |
| mevt_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_mevt_header_view (`mevt_headerno_i` Int64, `identifier` String, `ref_key` Int64, `ref_type` LowCardinality(String), `mevt_key` Int32, `mevt_type` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_mevt_header
```
</details>

### `analytics`.`amos_heli_od_detail_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| detailno_i | Int64 |  | 0 | 0 | 0 |  |
| orderno_i | Int64 |  | 0 | 0 | 0 |  |
| order_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ac_registr | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| state | LowCardinality(String) |  | 0 | 0 | 0 |  |
| vendor | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| serialno | Nullable(String) |  | 0 | 0 | 0 |  |
| condition | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| qty | Int32 |  | 0 | 0 | 0 |  |
| purch_price | Int64 |  | 0 | 0 | 0 |  |
| target_date | Date32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_od_detail_view (`detailno_i` Int64, `orderno_i` Int64, `order_type` LowCardinality(String), `ac_registr` LowCardinality(Nullable(String)), `state` LowCardinality(String), `vendor` LowCardinality(Nullable(String)), `partno` String, `serialno` Nullable(String), `condition` LowCardinality(Nullable(String)), `qty` Int32, `purch_price` Int64, `target_date` Date32, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_od_detail
```
</details>

### `analytics`.`amos_heli_part_requirement_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| part_requirementno_i | Int32 |  | 0 | 0 | 0 |  |
| type | Int32 |  | 0 | 0 | 0 |  |
| status | Int16 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_part_requirement_view (`part_requirementno_i` Int32, `type` Int32, `status` Int16, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_part_requirement
```
</details>

### `analytics`.`amos_heli_part_special_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| part_specialno_i | Int32 |  | 0 | 0 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| special | LowCardinality(String) |  | 0 | 0 | 0 |  |
| remarks | LowCardinality(String) |  | 0 | 0 | 0 |  |
| amount | Int16 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_part_special_view (`part_specialno_i` Int32, `partno` String, `special` LowCardinality(String), `remarks` LowCardinality(String), `amount` Int16, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_part_special
```
</details>

### `analytics`.`amos_heli_part_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | String |  | 0 | 0 | 0 |  |
| partmatch | String |  | 0 | 0 | 0 |  |
| partseqno_i | Int32 |  | 0 | 0 | 0 |  |
| ac_typ | LowCardinality(String) |  | 0 | 0 | 0 |  |
| mat_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| description | String |  | 0 | 0 | 0 |  |
| remarks | Nullable(String) |  | 0 | 0 | 0 |  |
| ata_chapter | LowCardinality(String) |  | 0 | 0 | 0 |  |
| vendor | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_part_view (`partno` String, `partmatch` String, `partseqno_i` Int32, `ac_typ` LowCardinality(String), `mat_type` LowCardinality(String), `description` String, `remarks` Nullable(String), `ata_chapter` LowCardinality(String), `vendor` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_part
```
</details>

### `analytics`.`amos_heli_requirement_header_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_key | Int32 |  | 0 | 0 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| effectivity_headerno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| requirement_headerno_i | Int32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_requirement_header_view (`event_key` Int32, `event_type` LowCardinality(String), `effectivity_headerno_i` Nullable(Int32), `requirement_headerno_i` Int32, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_requirement_header
```
</details>

### `analytics`.`amos_heli_requirement_type_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| requirement_typeno_i | Int32 |  | 0 | 0 | 0 |  |
| requirement | LowCardinality(String) |  | 0 | 0 | 0 |  |
| description | Nullable(String) |  | 0 | 0 | 0 |  |
| life_limit | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_requirement_type_view (`requirement_typeno_i` Int32, `requirement` LowCardinality(String), `description` Nullable(String), `life_limit` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_requirement_type
```
</details>

### `analytics`.`amos_heli_rotables_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_registr | Nullable(String) |  | 0 | 0 | 0 |  |
| partno | String |  | 0 | 0 | 0 |  |
| material_lifecycle_id | Int64 |  | 0 | 0 | 0 |  |
| serialno | String |  | 0 | 0 | 0 |  |
| locationno_i | Int32 |  | 0 | 0 | 0 |  |
| psn | Int64 |  | 0 | 0 | 0 |  |
| shop_visit_counter | Int32 |  | 0 | 0 | 0 |  |
| mfg_unknown | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| orderno | Nullable(String) |  | 0 | 0 | 0 |  |
| owner | LowCardinality(String) |  | 0 | 0 | 0 |  |
| condition | LowCardinality(String) |  | 0 | 0 | 0 |  |
| oh_at_date | Date32 |  | 0 | 0 | 0 |  |
| del_date | Date32 |  | 0 | 0 | 0 |  |
| mfg_date | Date32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_rotables_view (`ac_registr` Nullable(String), `partno` String, `material_lifecycle_id` Int64, `serialno` String, `locationno_i` Int32, `psn` Int64, `shop_visit_counter` Int32, `mfg_unknown` LowCardinality(Nullable(String)), `orderno` Nullable(String), `owner` LowCardinality(String), `condition` LowCardinality(String), `oh_at_date` Date32, `del_date` Date32, `mfg_date` Date32, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_rotables
```
</details>

### `analytics`.`amos_heli_treq_dimension_group_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| interval_groupno_i | Int64 |  | 0 | 0 | 0 |  |
| dimension_groupno_i | Int64 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_treq_dimension_group_view (`interval_groupno_i` Int64, `dimension_groupno_i` Int64, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_treq_dimension_group
```
</details>

### `analytics`.`amos_heli_treq_event_link_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| eventlinkno_i | Int32 |  | 0 | 0 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| event_key | Int32 |  | 0 | 0 | 0 |  |
| ac_registr | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| psn | Int32 |  | 0 | 0 | 0 |  |
| status | Int16 |  | 0 | 0 | 0 |  |
| timerequirementno_i | Int32 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_treq_event_link_view (`eventlinkno_i` Int32, `event_type` LowCardinality(String), `event_key` Int32, `ac_registr` LowCardinality(Nullable(String)), `psn` Int32, `status` Int16, `timerequirementno_i` Int32, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_treq_event_link
```
</details>

### `analytics`.`amos_heli_treq_interval_group_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| interval_groupno_i | Int64 |  | 0 | 0 | 0 |  |
| timerequirementno_i | Int64 |  | 0 | 0 | 0 |  |
| threshold | LowCardinality(String) |  | 0 | 0 | 0 |  |
| group_name | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_treq_interval_group_view (`interval_groupno_i` Int64, `timerequirementno_i` Int64, `threshold` LowCardinality(String), `group_name` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_treq_interval_group
```
</details>

### `analytics`.`amos_heli_treq_interval_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| intervalno_i | Int64 |  | 0 | 0 | 0 |  |
| interval_groupno_i | Int32 |  | 0 | 0 | 0 |  |
| dimension_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| counter_defno_i | Int32 |  | 0 | 0 | 0 |  |
| amount_interval | Int64 |  | 0 | 0 | 0 |  |
| due_at | Nullable(Int32) |  | 0 | 0 | 0 |  |
| dimension_groupno_i | Int64 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_treq_interval_view (`intervalno_i` Int64, `interval_groupno_i` Int32, `dimension_type` LowCardinality(String), `counter_defno_i` Int32, `amount_interval` Int64, `due_at` Nullable(Int32), `dimension_groupno_i` Int64, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_treq_interval
```
</details>

### `analytics`.`amos_heli_treq_time_requirement_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| timerequirementno_i | Int64 |  | 0 | 0 | 0 |  |
| event_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| event_key | Int64 |  | 0 | 0 | 0 |  |
| ac_group | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_treq_time_requirement_view (`timerequirementno_i` Int64, `event_type` LowCardinality(String), `event_key` Int64, `ac_group` LowCardinality(Nullable(String)), `type` LowCardinality(String), `status` Nullable(Int16), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_treq_time_requirement
```
</details>

### `analytics`.`amos_heli_wo_event_link_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_perfno_i | Int64 |  | 0 | 0 | 0 |  |
| effectivity_linkno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| mevt_headerno_i | Int64 |  | 0 | 0 | 0 |  |
| pending_status | Int16 |  | 0 | 0 | 0 |  |
| event_name | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_wo_event_link_view (`event_perfno_i` Int64, `effectivity_linkno_i` Nullable(Int32), `mevt_headerno_i` Int64, `pending_status` Int16, `event_name` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_wo_event_link
```
</details>

### `analytics`.`amos_heli_wo_header_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_perfno_i | Int64 |  | 0 | 0 | 0 |  |
| psn | Nullable(Int64) |  | 0 | 0 | 0 |  |
| ata_chapter | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| state | LowCardinality(String) |  | 0 | 0 | 0 |  |
| ac_registr | Nullable(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_wo_header_view (`event_perfno_i` Int64, `psn` Nullable(Int64), `ata_chapter` LowCardinality(Nullable(String)), `state` LowCardinality(String), `ac_registr` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_wo_header
```
</details>

### `analytics`.`amos_heli_wo_transfer_dimension_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| wo_transfer_dimensionno_i | Int64 |  | 0 | 0 | 0 |  |
| event_transferno_i | Int64 |  | 0 | 0 | 0 |  |
| treq_intervalno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| counterno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| due_at | Nullable(Float64) |  | 0 | 0 | 0 |  |
| status | Int8 |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_wo_transfer_dimension_view (`wo_transfer_dimensionno_i` Int64, `event_transferno_i` Int64, `treq_intervalno_i` Nullable(Int64), `counterno_i` Nullable(Int64), `due_at` Nullable(Float64), `status` Int8, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_wo_transfer_dimension
```
</details>

### `analytics`.`amos_heli_wo_transfer_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_transferno_i | Int64 |  | 0 | 0 | 0 |  |
| event_perfno_i | Int64 |  | 0 | 0 | 0 |  |
| is_last_transfer | LowCardinality(String) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_wo_transfer_view (`event_transferno_i` Int64, `event_perfno_i` Int64, `is_last_transfer` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_wo_transfer
```
</details>

### `analytics`.`amos_heli_wp_header_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| mpno | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| drop_locationno_i | Nullable(Int32) |  | 0 | 0 | 0 |  |
| wpno_i | Int32 |  | 0 | 0 | 0 |  |
| wpno | String |  | 0 | 0 | 0 |  |
| ac_registr | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| ac_typ | LowCardinality(String) |  | 0 | 0 | 0 |  |
| projectno | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| est_groundtime | Int32 |  | 0 | 0 | 0 |  |
| station | LowCardinality(String) |  | 0 | 0 | 0 |  |
| start_date | Int32 |  | 0 | 0 | 0 |  |
| start_time | Int32 |  | 0 | 0 | 0 |  |
| end_date | Int32 |  | 0 | 0 | 0 |  |
| end_time | Int32 |  | 0 | 0 | 0 |  |
| description | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| owner | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| hidden | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| status | Int8 |  | 0 | 0 | 0 |  |
| act_start_date | Nullable(Int32) |  | 0 | 0 | 0 |  |
| act_start_time | Nullable(Int32) |  | 0 | 0 | 0 |  |
| act_end_date | Nullable(Int32) |  | 0 | 0 | 0 |  |
| act_end_time | Nullable(Int32) |  | 0 | 0 | 0 |  |
| responsible | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| delay | Nullable(Int32) |  | 0 | 0 | 0 |  |
| cust_wpno | Nullable(String) |  | 0 | 0 | 0 |  |
| priority_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| remarks | Nullable(String) |  | 0 | 0 | 0 |  |
| extension_time | Nullable(Int32) |  | 0 | 0 | 0 |  |
| extension_reason | Nullable(Int8) |  | 0 | 0 | 0 |  |
| mpno_i | Nullable(Int16) |  | 0 | 0 | 0 |  |
| mp_revision | Nullable(Int16) |  | 0 | 0 | 0 |  |
| wp_status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| events_collection_status | Nullable(Int16) |  | 0 | 0 | 0 |  |
| uuid | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_operator | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| ac_model | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| created_date | Date |  | 0 | 0 | 0 |  |
| updated_at | Date |  | 0 | 0 | 0 |  |
| valid_from | DateTime |  | 0 | 0 | 0 |  |
| valid_to | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| processing_date_at | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.amos_heli_wp_header_view (`mpno` LowCardinality(Nullable(String)), `drop_locationno_i` Nullable(Int32), `wpno_i` Int32, `wpno` String, `ac_registr` LowCardinality(Nullable(String)), `ac_typ` LowCardinality(String), `projectno` LowCardinality(Nullable(String)), `est_groundtime` Int32, `station` LowCardinality(String), `start_date` Int32, `start_time` Int32, `end_date` Int32, `end_time` Int32, `description` LowCardinality(Nullable(String)), `owner` LowCardinality(Nullable(String)), `hidden` LowCardinality(Nullable(String)), `status` Int8, `act_start_date` Nullable(Int32), `act_start_time` Nullable(Int32), `act_end_date` Nullable(Int32), `act_end_time` Nullable(Int32), `responsible` LowCardinality(Nullable(String)), `delay` Nullable(Int32), `cust_wpno` Nullable(String), `priority_code` LowCardinality(Nullable(String)), `remarks` Nullable(String), `extension_time` Nullable(Int32), `extension_reason` Nullable(Int8), `mpno_i` Nullable(Int16), `mp_revision` Nullable(Int16), `wp_status` Nullable(Int16), `events_collection_status` Nullable(Int16), `uuid` Nullable(String), `ac_operator` LowCardinality(Nullable(String)), `ac_model` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_wp_header
```
</details>

### `analytics`.`appareo_aircraft_recorders_dict`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: serial_number
- Primary key: serial_number
- Total rows: 21
- Total bytes: 1423

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| is_active | Nullable(UInt8) |  | 0 | 0 | 0 | Флаг активности записи: 1 = активно, 0 = неактивно, NULL = неизвестно |
| serial_number | LowCardinality(String) |  | 1 | 1 | 0 | Серийный номер / идентификатор устройства |
| tail_number | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Бортовой номер / хвостовой номер самолета |
| location | LowCardinality(String) |  | 0 | 0 | 0 | Локация записи |
| appareo_enabled | UInt8 |  | 0 | 0 | 0 | Флаг: Appareo активен (1) или неактивен (0) |
| last_recording_dt | Nullable(Date) |  | 0 | 0 | 0 | Дата последней записи |
| start_recording_dt | Nullable(Date) |  | 0 | 0 | 0 | Дата начала записи |
| record_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Тип регистратора |
| aircraft_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Тип борта |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник загрузки данных (служебное поле) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Дата загрузки данных (служебное поле) |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.appareo_aircraft_recorders_dict (`is_active` Nullable(UInt8) COMMENT 'Флаг активности записи: 1 = активно, 0 = неактивно, NULL = неизвестно', `serial_number` LowCardinality(String) COMMENT 'Серийный номер / идентификатор устройства', `tail_number` LowCardinality(Nullable(String)) COMMENT 'Бортовой номер / хвостовой номер самолета', `location` LowCardinality(String) COMMENT 'Локация записи', `appareo_enabled` UInt8 COMMENT 'Флаг: Appareo активен (1) или неактивен (0)', `last_recording_dt` Nullable(Date) COMMENT 'Дата последней записи', `start_recording_dt` Nullable(Date) COMMENT 'Дата начала записи', `record_type` LowCardinality(Nullable(String)) COMMENT 'Тип регистратора', `aircraft_type` LowCardinality(Nullable(String)) COMMENT 'Тип борта', `meta_source` LowCardinality(String) COMMENT 'Источник загрузки данных (служебное поле)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Дата загрузки данных (служебное поле)') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/appareo_aircraft_recorders_dict', '{replica}') ORDER BY serial_number SETTINGS index_granularity = 8192 COMMENT 'Справочник бортовых регистраторов и соответствующих бортов. Содержит актуальный перечень устройств с датами эксплуатации и статусом активности.'
```
</details>

### `analytics`.`appareo_flight_event_triggers`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: processing_date, recorder_identifier, flight_id, event_start_at
- Primary key: processing_date, recorder_identifier, flight_id, event_start_at
- Total rows: 51
- Total bytes: 8068

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| processing_date | Date |  | 1 | 1 | 1 | Дата обработки файла (из data_interval_start DAG) |
| file_name | String |  | 0 | 0 | 0 | Имя исходного файла (например, VIS-FFK7-02898-Upload) |
| recorder_identifier | LowCardinality(String) |  | 1 | 1 | 0 | ID регистратора (например, VIS-FFK7) |
| flight_id | String |  | 1 | 1 | 0 | ID полёта (например, FFK702898) |
| event_id | String |  | 0 | 0 | 0 | Порядковый номер события в рамках полёта          (сквозная нумерация по event_start_at) |
| event_level | LowCardinality(String) |  | 0 | 0 | 0 | Уровень триггера (Low, Medium, High) |
| event_trigger_name | LowCardinality(String) |  | 0 | 0 | 0 | Имя триггера, вызвавшего событие (350Pitch, 350Roll,          Yaw Rat, и т.д.) |
| event_identifier | String |  | 0 | 0 | 0 | Уникальный идентификатор события с уровнем (например, 350PITCH-H,          350ROLL-M) |
| event_start_at | DateTime64(3) |  | 1 | 1 | 0 | Время начала события (GMT, миллисекундная точность) |
| event_end_at | DateTime64(3) |  | 0 | 0 | 0 | Время окончания события (GMT, миллисекундная точность) |
| event_duration_sec | Float64 |  | 0 | 0 | 0 | Длительность события в секундах от начала полета |
| event_duration_str | String |  | 0 | 0 | 0 | Длительность события в формате HH:MM:SS.sss |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник загрузки данных (DAG_ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Дата загрузки данных (служебное поле) |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.appareo_flight_event_triggers (`processing_date` Date COMMENT 'Дата обработки файла (из data_interval_start DAG)', `file_name` String COMMENT 'Имя исходного файла (например, VIS-FFK7-02898-Upload)', `recorder_identifier` LowCardinality(String) COMMENT 'ID регистратора (например, VIS-FFK7)', `flight_id` String COMMENT 'ID полёта (например, FFK702898)', `event_id` String COMMENT 'Порядковый номер события в рамках полёта\r\n        (сквозная нумерация по event_start_at)', `event_level` LowCardinality(String) COMMENT 'Уровень триггера (Low, Medium, High)', `event_trigger_name` LowCardinality(String) COMMENT 'Имя триггера, вызвавшего событие (350Pitch, 350Roll,\r\n        Yaw Rat, и т.д.)', `event_identifier` String COMMENT 'Уникальный идентификатор события с уровнем (например, 350PITCH-H,\r\n        350ROLL-M)', `event_start_at` DateTime64(3) COMMENT 'Время начала события (GMT, миллисекундная точность)', `event_end_at` DateTime64(3) COMMENT 'Время окончания события (GMT, миллисекундная точность)', `event_duration_sec` Float64 COMMENT 'Длительность события в секундах от начала полета', `event_duration_str` String COMMENT 'Длительность события в формате HH:MM:SS.sss', `meta_source` LowCardinality(String) COMMENT 'Источник загрузки данных (DAG_ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Дата загрузки данных (служебное поле)') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/appareo_flight_event_triggers', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (processing_date, recorder_identifier, flight_id, event_start_at) SETTINGS index_granularity = 8192 COMMENT 'Результаты обработки полётных данных Appareo по триггерам: хранит события с указанием времени,\r\n    длительности, идентификатора и источника'
```
</details>

### `analytics`.`appareo_flight_params_dict`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: helicopter_type, flight_param_name, trigger_name
- Primary key: helicopter_type, flight_param_name, trigger_name
- Total rows: 6
- Total bytes: 3089

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| helicopter_type | LowCardinality(String) |  | 1 | 1 | 0 | Тип вертолета, для которого определено ограничение |
| flight_param_name | String |  | 1 | 1 | 0 | Название параметра полета |
| flight_param_desc | String |  | 0 | 0 | 0 | Описание параметра полета |
| limits_rpp | String |  | 0 | 0 | 0 | Ограничения в РПП |
| as_limit_low | Nullable(String) |  | 0 | 0 | 0 | Ограничение в AS Web Analysis - низкий уровень |
| as_limit_medium | Nullable(String) |  | 0 | 0 | 0 | Ограничение в AS Web Analysis - средний уровень |
| as_limit_high | Nullable(String) |  | 0 | 0 | 0 | Ограничение в AS Web Analysis - высокий уровень |
| trigger_name | String |  | 1 | 1 | 0 | Название соответствующего триггера из Apareo EnVision |
| trigger_description | String |  | 0 | 0 | 0 | Описание триггера |
| pre_event_duration_sec | Float64 |  | 0 | 0 | 0 | Длительность пре-события в секундах |
| post_event_duration_sec | Float64 |  | 0 | 0 | 0 | Длительность пост-события в секундах |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник загрузки данных (служебное поле) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Дата загрузки данных (служебное поле) |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.appareo_flight_params_dict (`helicopter_type` LowCardinality(String) COMMENT 'Тип вертолета, для которого определено ограничение', `flight_param_name` String COMMENT 'Название параметра полета', `flight_param_desc` String COMMENT 'Описание параметра полета', `limits_rpp` String COMMENT 'Ограничения в РПП', `as_limit_low` Nullable(String) COMMENT 'Ограничение в AS Web Analysis - низкий уровень', `as_limit_medium` Nullable(String) COMMENT 'Ограничение в AS Web Analysis - средний уровень', `as_limit_high` Nullable(String) COMMENT 'Ограничение в AS Web Analysis - высокий уровень', `trigger_name` String COMMENT 'Название соответствующего триггера из Apareo EnVision', `trigger_description` String COMMENT 'Описание триггера', `pre_event_duration_sec` Float64 COMMENT 'Длительность пре-события в секундах', `post_event_duration_sec` Float64 COMMENT 'Длительность пост-события в секундах', `meta_source` LowCardinality(String) COMMENT 'Источник загрузки данных (служебное поле)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Дата загрузки данных (служебное поле)') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/appareo_flight_params_dict', '{replica}') ORDER BY (helicopter_type, flight_param_name, trigger_name) SETTINGS index_granularity = 8192 COMMENT 'Справочник параметров полета, ограничений и связанных триггеров Apareo EnVision для различных типов вертолетов'
```
</details>

### `analytics`.`appareo_flight_trigger_conditions_dict`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: trigger_name, track_name
- Primary key: trigger_name, track_name
- Total rows: 8
- Total bytes: 1154

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| trigger_name | String |  | 1 | 1 | 0 | Название триггера (связка с основной таблицей) |
| track_name | String |  | 1 | 1 | 0 | Имя трека (например Ground Speed, Pitch) |
| absolute_value | UInt8 |  | 0 | 0 | 0 | Флаг: 1 = абсолютное значение, 0 = как есть |
| operator | String |  | 0 | 0 | 0 | Оператор сравнения (например >, <, =) |
| threshold_low | Nullable(Float64) |  | 0 | 0 | 0 | Порог низкого уровня |
| threshold_medium | Nullable(Float64) |  | 0 | 0 | 0 | Порог среднего уровня |
| threshold_high | Nullable(Float64) |  | 0 | 0 | 0 | Порог высокого уровня |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник загрузки данных (служебное поле) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Дата загрузки данных (служебное поле) |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.appareo_flight_trigger_conditions_dict (`trigger_name` String COMMENT 'Название триггера (связка с основной таблицей)', `track_name` String COMMENT 'Имя трека (например Ground Speed, Pitch)', `absolute_value` UInt8 COMMENT 'Флаг: 1 = абсолютное значение, 0 = как есть', `operator` String COMMENT 'Оператор сравнения (например >, <, =)', `threshold_low` Nullable(Float64) COMMENT 'Порог низкого уровня', `threshold_medium` Nullable(Float64) COMMENT 'Порог среднего уровня', `threshold_high` Nullable(Float64) COMMENT 'Порог высокого уровня', `meta_source` LowCardinality(String) COMMENT 'Источник загрузки данных (служебное поле)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Дата загрузки данных (служебное поле)') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/appareo_flight_trigger_conditions_dict', '{replica}') ORDER BY (trigger_name, track_name) SETTINGS index_granularity = 8192 COMMENT 'Условия срабатывания триггеров Apareo EnVision'
```
</details>

### `analytics`.`appareo_flights_telemetry`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: processing_date, recorder_identifier, flight_id, relative_time_seconds, event_at
- Primary key: processing_date, recorder_identifier, flight_id, relative_time_seconds, event_at
- Total rows: 2707714
- Total bytes: 266987096

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_at | DateTime64(3) |  | 1 | 1 | 0 | Дата и время измерения в формате ISO 8601 (UTC) |
| relative_time_str | String |  | 0 | 0 | 0 | Относительное время от начала полета в строковом формате (HH:MM:SS.ss) |
| relative_time_seconds | Float64 |  | 1 | 1 | 0 | Относительное время от начала полета в секундах |
| processing_date | Date |  | 1 | 1 | 1 | Дата обработки файла (из data_interval_start DAG) |
| file_name | String |  | 0 | 0 | 0 | Имя исходного файла (например, VIS-FFK7-02898-Upload) |
| recorder_identifier | LowCardinality(String) |  | 1 | 1 | 0 | ID регистратора (например, VIS-FFK7) |
| flight_id | String |  | 1 | 1 | 0 | ID полета (например, FFK702898) |
| roll | Float32 |  | 0 | 0 | 0 | Крен самолета (наклон вокруг продольной оси) в градусах |
| pitch | Float32 |  | 0 | 0 | 0 | Тангаж самолета (наклон вокруг поперечной оси) в градусах |
| heading | Float32 |  | 0 | 0 | 0 | Магнитный курс самолета (относительно магнитного севера) в градусах |
| course | Float32 |  | 0 | 0 | 0 | Путевой угол самолета (относительно истинного севера) в градусах |
| ground_speed | Float32 |  | 0 | 0 | 0 | Путевая скорость относительно земли (узлы) |
| vertical_speed | Float32 |  | 0 | 0 | 0 | Вертикальная скорость (скорость набора или снижения) в 1000 ft/min |
| latitude | Float64 |  | 0 | 0 | 0 | Географическая широта в градусах |
| longitude | Float64 |  | 0 | 0 | 0 | Географическая долгота в градусах |
| altitude_msl | Float32 |  | 0 | 0 | 0 | Высота над уровнем моря (MSL) в метрах |
| altitude_cop | Nullable(Float32) |  | 0 | 0 | 0 | Высота рельефа Copernicus DEM в метрах |
| agl_cop | Nullable(Float32) |  | 0 | 0 | 0 | Высота над землей (AGL) в метрах |
| x_rotation_rate | Nullable(Float32) |  | 0 | 0 | 0 | Угловая скорость вокруг оси X (крен) в °/s |
| y_rotation_rate | Nullable(Float32) |  | 0 | 0 | 0 | Угловая скорость вокруг оси Y (тангаж) в °/s |
| z_rotation_rate | Nullable(Float32) |  | 0 | 0 | 0 | Угловая скорость вокруг оси Z (рыскание) в °/s |
| x_acceleration | Nullable(Float32) |  | 0 | 0 | 0 | Линейное ускорение вдоль оси X в g |
| y_acceleration | Nullable(Float32) |  | 0 | 0 | 0 | Линейное ускорение вдоль оси Y в g |
| z_acceleration | Nullable(Float32) |  | 0 | 0 | 0 | Линейное ускорение вдоль оси Z в g |
| x_magnetic | Nullable(Float32) |  | 0 | 0 | 0 | Магнитное поле вдоль оси X в G |
| y_magnetic | Nullable(Float32) |  | 0 | 0 | 0 | Магнитное поле вдоль оси Y в G |
| z_magnetic | Nullable(Float32) |  | 0 | 0 | 0 | Магнитное поле вдоль оси Z в G |
| itow | UInt64 |  | 0 | 0 | 0 | GPS-временная метка (ITOW) |
| solution_number | UInt32 |  | 0 | 0 | 0 | Номер GPS/INS решения |
| gps_fix_status | Enum8('NO_FIX' = 0, 'DEAD_RECKONING' = 1, 'FIX_2D' = 2, 'FIX_3D' = 3, 'GPS_PLUS_DEAD_RECKONING' = 4, 'TIME_FIX_ONLY' = 5) |  | 0 | 0 | 0 | Статус GPS-позиционирования |
| is_gps_fix_ok | UInt8 |  | 0 | 0 | 0 | Флаг: решение GPS валидно |
| is_gps_dif_sol | UInt8 |  | 0 | 0 | 0 | Флаг: используется дифференциальное решение |
| is_gps_valid_itow | UInt8 |  | 0 | 0 | 0 | Флаг: валидна временная метка ITOW |
| is_gps_valid_week | UInt8 |  | 0 | 0 | 0 | Флаг: валиден номер недели |
| is_gps_valid_utc | UInt8 |  | 0 | 0 | 0 | Флаг: валидно UTC-время |
| time_to_first_fix | UInt32 |  | 0 | 0 | 0 | Время до первого GPS-решения (мс) |
| ms_since_startup | UInt64 |  | 0 | 0 | 0 | Миллисекунды с момента запуска |
| is_heading_valid | UInt8 |  | 0 | 0 | 0 | Флаг: курс валиден |
| is_roll_pitch_valid | UInt8 |  | 0 | 0 | 0 | Флаг: крен/тангаж валидны |
| horizontal_accuracy | UInt32 |  | 0 | 0 | 0 | Точность горизонтального позиционирования (см) |
| vertical_accuracy | UInt32 |  | 0 | 0 | 0 | Точность вертикального позиционирования (см) |
| time_accuracy_estimate | Int32 |  | 0 | 0 | 0 | Точность времени GPS (нс) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник загрузки данных (DAG_ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Дата загрузки данных (служебное поле) |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.appareo_flights_telemetry (`event_at` DateTime64(3) COMMENT 'Дата и время измерения в формате ISO 8601 (UTC)', `relative_time_str` String COMMENT 'Относительное время от начала полета в строковом формате (HH:MM:SS.ss)', `relative_time_seconds` Float64 COMMENT 'Относительное время от начала полета в секундах', `processing_date` Date COMMENT 'Дата обработки файла (из data_interval_start DAG)', `file_name` String COMMENT 'Имя исходного файла (например, VIS-FFK7-02898-Upload)', `recorder_identifier` LowCardinality(String) COMMENT 'ID регистратора (например, VIS-FFK7)', `flight_id` String COMMENT 'ID полета (например, FFK702898)', `roll` Float32 COMMENT 'Крен самолета (наклон вокруг продольной оси) в градусах', `pitch` Float32 COMMENT 'Тангаж самолета (наклон вокруг поперечной оси) в градусах', `heading` Float32 COMMENT 'Магнитный курс самолета (относительно магнитного севера) в градусах', `course` Float32 COMMENT 'Путевой угол самолета (относительно истинного севера) в градусах', `ground_speed` Float32 COMMENT 'Путевая скорость относительно земли (узлы)', `vertical_speed` Float32 COMMENT 'Вертикальная скорость (скорость набора или снижения) в 1000 ft/min', `latitude` Float64 COMMENT 'Географическая широта в градусах', `longitude` Float64 COMMENT 'Географическая долгота в градусах', `altitude_msl` Float32 COMMENT 'Высота над уровнем моря (MSL) в метрах', `altitude_cop` Nullable(Float32) COMMENT 'Высота рельефа Copernicus DEM в метрах', `agl_cop` Nullable(Float32) COMMENT 'Высота над землей (AGL) в метрах', `x_rotation_rate` Nullable(Float32) COMMENT 'Угловая скорость вокруг оси X (крен) в °/s', `y_rotation_rate` Nullable(Float32) COMMENT 'Угловая скорость вокруг оси Y (тангаж) в °/s', `z_rotation_rate` Nullable(Float32) COMMENT 'Угловая скорость вокруг оси Z (рыскание) в °/s', `x_acceleration` Nullable(Float32) COMMENT 'Линейное ускорение вдоль оси X в g', `y_acceleration` Nullable(Float32) COMMENT 'Линейное ускорение вдоль оси Y в g', `z_acceleration` Nullable(Float32) COMMENT 'Линейное ускорение вдоль оси Z в g', `x_magnetic` Nullable(Float32) COMMENT 'Магнитное поле вдоль оси X в G', `y_magnetic` Nullable(Float32) COMMENT 'Магнитное поле вдоль оси Y в G', `z_magnetic` Nullable(Float32) COMMENT 'Магнитное поле вдоль оси Z в G', `itow` UInt64 COMMENT 'GPS-временная метка (ITOW)', `solution_number` UInt32 COMMENT 'Номер GPS/INS решения', `gps_fix_status` Enum8('NO_FIX' = 0, 'DEAD_RECKONING' = 1, 'FIX_2D' = 2, 'FIX_3D' = 3, 'GPS_PLUS_DEAD_RECKONING' = 4, 'TIME_FIX_ONLY' = 5) COMMENT 'Статус GPS-позиционирования', `is_gps_fix_ok` UInt8 COMMENT 'Флаг: решение GPS валидно', `is_gps_dif_sol` UInt8 COMMENT 'Флаг: используется дифференциальное решение', `is_gps_valid_itow` UInt8 COMMENT 'Флаг: валидна временная метка ITOW', `is_gps_valid_week` UInt8 COMMENT 'Флаг: валиден номер недели', `is_gps_valid_utc` UInt8 COMMENT 'Флаг: валидно UTC-время', `time_to_first_fix` UInt32 COMMENT 'Время до первого GPS-решения (мс)', `ms_since_startup` UInt64 COMMENT 'Миллисекунды с момента запуска', `is_heading_valid` UInt8 COMMENT 'Флаг: курс валиден', `is_roll_pitch_valid` UInt8 COMMENT 'Флаг: крен/тангаж валидны', `horizontal_accuracy` UInt32 COMMENT 'Точность горизонтального позиционирования (см)', `vertical_accuracy` UInt32 COMMENT 'Точность вертикального позиционирования (см)', `time_accuracy_estimate` Int32 COMMENT 'Точность времени GPS (нс)', `meta_source` LowCardinality(String) COMMENT 'Источник загрузки данных (DAG_ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Дата загрузки данных (служебное поле)') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/appareo_flights_telemetry', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (processing_date, recorder_identifier, flight_id, relative_time_seconds, event_at) SETTINGS index_granularity = 8192 COMMENT 'Таблица с телеметрическими данными полетов Appareo. Содержит параметры, значения и временные метки для\r\n    конкретного борта и рейса.'
```
</details>

### `analytics`.`appareo_flights_telemetry_distributed`
- Engine: Distributed
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| event_at | DateTime64(3) |  | 0 | 0 | 0 | Дата и время измерения в формате ISO 8601 (UTC) |
| relative_time_str | String |  | 0 | 0 | 0 | Относительное время от начала полета в строковом формате (HH:MM:SS.ss) |
| relative_time_seconds | Float64 |  | 0 | 0 | 0 | Относительное время от начала полета в секундах |
| processing_date | Date |  | 0 | 0 | 0 | Дата обработки файла (из data_interval_start DAG) |
| file_name | String |  | 0 | 0 | 0 | Имя исходного файла (например, VIS-FFK7-02898-Upload) |
| recorder_identifier | LowCardinality(String) |  | 0 | 0 | 0 | ID регистратора (например, VIS-FFK7) |
| flight_id | String |  | 0 | 0 | 0 | ID полета (например, FFK702898) |
| roll | Float32 |  | 0 | 0 | 0 | Крен самолета (наклон вокруг продольной оси) в градусах |
| pitch | Float32 |  | 0 | 0 | 0 | Тангаж самолета (наклон вокруг поперечной оси) в градусах |
| heading | Float32 |  | 0 | 0 | 0 | Магнитный курс самолета (относительно магнитного севера) в градусах |
| course | Float32 |  | 0 | 0 | 0 | Путевой угол самолета (относительно истинного севера) в градусах |
| ground_speed | Float32 |  | 0 | 0 | 0 | Путевая скорость относительно земли (узлы) |
| vertical_speed | Float32 |  | 0 | 0 | 0 | Вертикальная скорость (скорость набора или снижения) в 1000 ft/min |
| latitude | Float64 |  | 0 | 0 | 0 | Географическая широта в градусах |
| longitude | Float64 |  | 0 | 0 | 0 | Географическая долгота в градусах |
| altitude_msl | Float32 |  | 0 | 0 | 0 | Высота над уровнем моря (MSL) в метрах |
| altitude_cop | Nullable(Float32) |  | 0 | 0 | 0 | Высота рельефа Copernicus DEM в метрах |
| agl_cop | Nullable(Float32) |  | 0 | 0 | 0 | Высота над землей (AGL) в метрах |
| x_rotation_rate | Nullable(Float32) |  | 0 | 0 | 0 | Угловая скорость вокруг оси X (крен) в °/s |
| y_rotation_rate | Nullable(Float32) |  | 0 | 0 | 0 | Угловая скорость вокруг оси Y (тангаж) в °/s |
| z_rotation_rate | Nullable(Float32) |  | 0 | 0 | 0 | Угловая скорость вокруг оси Z (рыскание) в °/s |
| x_acceleration | Nullable(Float32) |  | 0 | 0 | 0 | Линейное ускорение вдоль оси X в g |
| y_acceleration | Nullable(Float32) |  | 0 | 0 | 0 | Линейное ускорение вдоль оси Y в g |
| z_acceleration | Nullable(Float32) |  | 0 | 0 | 0 | Линейное ускорение вдоль оси Z в g |
| x_magnetic | Nullable(Float32) |  | 0 | 0 | 0 | Магнитное поле вдоль оси X в G |
| y_magnetic | Nullable(Float32) |  | 0 | 0 | 0 | Магнитное поле вдоль оси Y в G |
| z_magnetic | Nullable(Float32) |  | 0 | 0 | 0 | Магнитное поле вдоль оси Z в G |
| itow | UInt64 |  | 0 | 0 | 0 | GPS-временная метка (ITOW) |
| solution_number | UInt32 |  | 0 | 0 | 0 | Номер GPS/INS решения |
| gps_fix_status | Enum8('NO_FIX' = 0, 'DEAD_RECKONING' = 1, 'FIX_2D' = 2, 'FIX_3D' = 3, 'GPS_PLUS_DEAD_RECKONING' = 4, 'TIME_FIX_ONLY' = 5) |  | 0 | 0 | 0 | Статус GPS-позиционирования |
| is_gps_fix_ok | UInt8 |  | 0 | 0 | 0 | Флаг: решение GPS валидно |
| is_gps_dif_sol | UInt8 |  | 0 | 0 | 0 | Флаг: используется дифференциальное решение |
| is_gps_valid_itow | UInt8 |  | 0 | 0 | 0 | Флаг: валидна временная метка ITOW |
| is_gps_valid_week | UInt8 |  | 0 | 0 | 0 | Флаг: валиден номер недели |
| is_gps_valid_utc | UInt8 |  | 0 | 0 | 0 | Флаг: валидно UTC-время |
| time_to_first_fix | UInt32 |  | 0 | 0 | 0 | Время до первого GPS-решения (мс) |
| ms_since_startup | UInt64 |  | 0 | 0 | 0 | Миллисекунды с момента запуска |
| is_heading_valid | UInt8 |  | 0 | 0 | 0 | Флаг: курс валиден |
| is_roll_pitch_valid | UInt8 |  | 0 | 0 | 0 | Флаг: крен/тангаж валидны |
| horizontal_accuracy | UInt32 |  | 0 | 0 | 0 | Точность горизонтального позиционирования (см) |
| vertical_accuracy | UInt32 |  | 0 | 0 | 0 | Точность вертикального позиционирования (см) |
| time_accuracy_estimate | Int32 |  | 0 | 0 | 0 | Точность времени GPS (нс) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник загрузки данных (DAG_ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Дата загрузки данных (служебное поле) |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.appareo_flights_telemetry_distributed (`event_at` DateTime64(3) COMMENT 'Дата и время измерения в формате ISO 8601 (UTC)', `relative_time_str` String COMMENT 'Относительное время от начала полета в строковом формате (HH:MM:SS.ss)', `relative_time_seconds` Float64 COMMENT 'Относительное время от начала полета в секундах', `processing_date` Date COMMENT 'Дата обработки файла (из data_interval_start DAG)', `file_name` String COMMENT 'Имя исходного файла (например, VIS-FFK7-02898-Upload)', `recorder_identifier` LowCardinality(String) COMMENT 'ID регистратора (например, VIS-FFK7)', `flight_id` String COMMENT 'ID полета (например, FFK702898)', `roll` Float32 COMMENT 'Крен самолета (наклон вокруг продольной оси) в градусах', `pitch` Float32 COMMENT 'Тангаж самолета (наклон вокруг поперечной оси) в градусах', `heading` Float32 COMMENT 'Магнитный курс самолета (относительно магнитного севера) в градусах', `course` Float32 COMMENT 'Путевой угол самолета (относительно истинного севера) в градусах', `ground_speed` Float32 COMMENT 'Путевая скорость относительно земли (узлы)', `vertical_speed` Float32 COMMENT 'Вертикальная скорость (скорость набора или снижения) в 1000 ft/min', `latitude` Float64 COMMENT 'Географическая широта в градусах', `longitude` Float64 COMMENT 'Географическая долгота в градусах', `altitude_msl` Float32 COMMENT 'Высота над уровнем моря (MSL) в метрах', `altitude_cop` Nullable(Float32) COMMENT 'Высота рельефа Copernicus DEM в метрах', `agl_cop` Nullable(Float32) COMMENT 'Высота над землей (AGL) в метрах', `x_rotation_rate` Nullable(Float32) COMMENT 'Угловая скорость вокруг оси X (крен) в °/s', `y_rotation_rate` Nullable(Float32) COMMENT 'Угловая скорость вокруг оси Y (тангаж) в °/s', `z_rotation_rate` Nullable(Float32) COMMENT 'Угловая скорость вокруг оси Z (рыскание) в °/s', `x_acceleration` Nullable(Float32) COMMENT 'Линейное ускорение вдоль оси X в g', `y_acceleration` Nullable(Float32) COMMENT 'Линейное ускорение вдоль оси Y в g', `z_acceleration` Nullable(Float32) COMMENT 'Линейное ускорение вдоль оси Z в g', `x_magnetic` Nullable(Float32) COMMENT 'Магнитное поле вдоль оси X в G', `y_magnetic` Nullable(Float32) COMMENT 'Магнитное поле вдоль оси Y в G', `z_magnetic` Nullable(Float32) COMMENT 'Магнитное поле вдоль оси Z в G', `itow` UInt64 COMMENT 'GPS-временная метка (ITOW)', `solution_number` UInt32 COMMENT 'Номер GPS/INS решения', `gps_fix_status` Enum8('NO_FIX' = 0, 'DEAD_RECKONING' = 1, 'FIX_2D' = 2, 'FIX_3D' = 3, 'GPS_PLUS_DEAD_RECKONING' = 4, 'TIME_FIX_ONLY' = 5) COMMENT 'Статус GPS-позиционирования', `is_gps_fix_ok` UInt8 COMMENT 'Флаг: решение GPS валидно', `is_gps_dif_sol` UInt8 COMMENT 'Флаг: используется дифференциальное решение', `is_gps_valid_itow` UInt8 COMMENT 'Флаг: валидна временная метка ITOW', `is_gps_valid_week` UInt8 COMMENT 'Флаг: валиден номер недели', `is_gps_valid_utc` UInt8 COMMENT 'Флаг: валидно UTC-время', `time_to_first_fix` UInt32 COMMENT 'Время до первого GPS-решения (мс)', `ms_since_startup` UInt64 COMMENT 'Миллисекунды с момента запуска', `is_heading_valid` UInt8 COMMENT 'Флаг: курс валиден', `is_roll_pitch_valid` UInt8 COMMENT 'Флаг: крен/тангаж валидны', `horizontal_accuracy` UInt32 COMMENT 'Точность горизонтального позиционирования (см)', `vertical_accuracy` UInt32 COMMENT 'Точность вертикального позиционирования (см)', `time_accuracy_estimate` Int32 COMMENT 'Точность времени GPS (нс)', `meta_source` LowCardinality(String) COMMENT 'Источник загрузки данных (DAG_ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Дата загрузки данных (служебное поле)') ENGINE = Distributed('{cluster}', 'analytics', 'appareo_flights_telemetry', cityHash64(recorder_identifier, flight_id)) COMMENT 'Таблица с телеметрическими данными полетов Appareo. Содержит параметры, значения и временные метки для\r\n    конкретного борта и рейса.'
```
</details>

### `analytics`.`availability_pricing_audit`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(event_time)
- Sorting key: event_time, trace_id, duration
- Primary key: event_time, trace_id, duration
- Total rows: 32972276
- Total bytes: 1613215517

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| trace_id | String |  | 1 | 1 | 0 | Идентификатор трассировки запроса |
| event_time | DateTime64(3) |  | 1 | 1 | 1 | Время события (@timestamp), UTC |
| user_ip | String |  | 0 | 0 | 0 | IP адрес пользователя |
| duration | Float64 |  | 1 | 1 | 0 | Длительность выполнения запроса (секунды) |
| method | String |  | 0 | 0 | 0 | Название метода API |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время записи в таблицу (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.availability_pricing_audit (`trace_id` String COMMENT 'Идентификатор трассировки запроса', `event_time` DateTime64(3) COMMENT 'Время события (@timestamp), UTC', `user_ip` String COMMENT 'IP адрес пользователя', `duration` Float64 COMMENT 'Длительность выполнения запроса (секунды)', `method` String COMMENT 'Название метода API', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время записи в таблицу (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/availability_pricing_audit', '{replica}') PARTITION BY toYYYYMM(event_time) ORDER BY (event_time, trace_id, duration) TTL toDateTime(event_time) + toIntervalMonth(3) SETTINGS index_granularity = 8192 COMMENT 'Данные audit для availability_pricing. Слой analytics.'
```
</details>

### `analytics`.`availability_pricing_audit_mv`
- Engine: MaterializedView
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| trace_id | String |  | 0 | 0 | 0 |  |
| event_time | DateTime64(3) |  | 0 | 0 | 0 |  |
| user_ip | String |  | 0 | 0 | 0 |  |
| duration | Float64 |  | 0 | 0 | 0 |  |
| method | String |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE MATERIALIZED VIEW analytics.availability_pricing_audit_mv TO analytics.availability_pricing_audit (`trace_id` String, `event_time` DateTime64(3), `user_ip` String, `duration` Float64, `method` String, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT trace_id, event_time, user_ip, duration, 'getavailability' AS method, meta_source, now() AS meta_loading_at FROM source.availability_pricing_audit
```
</details>

### `analytics`.`availability_pricing_logstash`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(event_time)
- Sorting key: event_time, trace_id, duration
- Primary key: event_time, trace_id, duration
- Total rows: 33125314
- Total bytes: 1423731856

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| trace_id | String |  | 1 | 1 | 0 | Идентификатор трассировки запроса |
| event_time | DateTime64(3) |  | 1 | 1 | 1 | Время события (@timestamp), UTC |
| user_ip | Nullable(String) |  | 0 | 0 | 0 | IP адрес пользователя |
| duration | Float64 |  | 1 | 1 | 0 | Длительность выполнения запроса (секунды) |
| method | String |  | 0 | 0 | 0 | Название метода API |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время записи в таблицу (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.availability_pricing_logstash (`trace_id` String COMMENT 'Идентификатор трассировки запроса', `event_time` DateTime64(3) COMMENT 'Время события (@timestamp), UTC', `user_ip` Nullable(String) COMMENT 'IP адрес пользователя', `duration` Float64 COMMENT 'Длительность выполнения запроса (секунды)', `method` String COMMENT 'Название метода API', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время записи в таблицу (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/availability_pricing_logstash', '{replica}') PARTITION BY toYYYYMM(event_time) ORDER BY (event_time, trace_id, duration) TTL toDateTime(event_time) + toIntervalMonth(3) SETTINGS index_granularity = 8192 COMMENT 'Данные logstash для availability_pricing. Слой analytics.'
```
</details>

### `analytics`.`availability_pricing_logstash_mv`
- Engine: MaterializedView
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| trace_id | String |  | 0 | 0 | 0 |  |
| event_time | DateTime64(3) |  | 0 | 0 | 0 |  |
| user_ip | Nullable(String) |  | 0 | 0 | 0 |  |
| duration | Float64 |  | 0 | 0 | 0 |  |
| method | String |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE MATERIALIZED VIEW analytics.availability_pricing_logstash_mv TO analytics.availability_pricing_logstash (`trace_id` String, `event_time` DateTime64(3), `user_ip` Nullable(String), `duration` Float64, `method` String, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT trace_id, event_time, CAST(NULL, 'Nullable(String)') AS user_ip, duration, method, meta_source, now() AS meta_loading_at FROM source.availability_pricing_logstash
```
</details>

### `analytics`.`fdr_express_analysis_atr72_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| board_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| year | String |  | 0 | 0 | 0 |  |
| month | String |  | 0 | 0 | 0 |  |
| day | String |  | 0 | 0 | 0 |  |
| flight_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| pilot_number | String |  | 0 | 0 | 0 |  |
| T0 | String |  | 0 | 0 | 0 |  |
| TK | String |  | 0 | 0 | 0 |  |
| route_note | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoff_weight | Nullable(String) |  | 0 | 0 | 0 |  |
| landing_weight | Nullable(String) |  | 0 | 0 | 0 |  |
| approach_system | Nullable(String) |  | 0 | 0 | 0 |  |
| R03 | Nullable(String) |  | 0 | 0 | 0 |  |
| R22 | Nullable(String) |  | 0 | 0 | 0 |  |
| max_R41_R42 | Nullable(String) |  | 0 | 0 | 0 |  |
| R18 | Nullable(String) |  | 0 | 0 | 0 |  |
| max_R24_R26 | Nullable(String) |  | 0 | 0 | 0 |  |
| landing_conditions | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoffSeat | Nullable(String) |  | 0 | 0 | 0 |  |
| landingSeat | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoff_datetime | DateTime |  | 0 | 0 | 0 |  |
| target_date | Date |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.fdr_express_analysis_atr72_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `takeoff_weight` Nullable(String), `landing_weight` Nullable(String), `approach_system` Nullable(String), `R03` Nullable(String), `R22` Nullable(String), `max_R41_R42` Nullable(String), `R18` Nullable(String), `max_R24_R26` Nullable(String), `landing_conditions` Nullable(String), `takeoffSeat` Nullable(String), `landingSeat` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS WITH fp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key IN ('Заход на посадку', 'Система захода на посадку')) AS approach_system, maxIf(flight_parameter_value, flight_parameter_key = 'Взлетный вес, т') AS takeoff_weight, maxIf(flight_parameter_value, flight_parameter_key = 'Посадочный вес, т') AS landing_weight, maxIf(flight_parameter_value, flight_parameter_key = 'Признак условий посадки') AS landing_conditions, maxIf(flight_parameter_value, flight_parameter_key = 'takeoffSeat') AS takeoffSeat, maxIf(flight_parameter_value, flight_parameter_key = 'landingSeat') AS landingSeat FROM analytics.fdr_express_analysis_flight_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date), rp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(regular_parameter_value, regular_parameter_key = 'R03') AS R03, maxIf(regular_parameter_value, regular_parameter_key = 'R18') AS R18, maxIf(regular_parameter_value, regular_parameter_key = 'R22') AS R22, maxIf(regular_parameter_value, regular_parameter_key = 'R24') AS R24, maxIf(regular_parameter_value, regular_parameter_key = 'R26') AS R26, maxIf(regular_parameter_value, regular_parameter_key = 'R41') AS R41, maxIf(regular_parameter_value, regular_parameter_key = 'R42') AS R42 FROM analytics.fdr_express_analysis_regular_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date) SELECT fp.aircraft_type, fp.board_number, toString(toYear(fp.report_start_dt)) AS year, toString(toMonth(fp.report_start_dt)) AS month, toString(toDayOfMonth(fp.report_start_dt)) AS day, fp.flight_number, fp.pilot_number, formatDateTime(fp.report_start_dt, '%H:%i:%S') AS T0, formatDateTime(fp.report_end_dt, '%H:%i:%S') AS TK, fp.route_note, fp.takeoff_weight, fp.landing_weight, fp.approach_system, rp.R03, rp.R22, if(greatest(toFloat64OrNull(rp.R41), toFloat64OrNull(rp.R42)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R41), toFloat64OrNull(rp.R42)))) AS max_R41_R42, rp.R18, if(greatest(toFloat64OrNull(rp.R24), toFloat64OrNull(rp.R26)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R24), toFloat64OrNull(rp.R26)))) AS max_R24_R26, fp.landing_conditions, fp.takeoffSeat, fp.landingSeat, fp.takeoff_datetime, fp.target_date FROM fp LEFT JOIN rp ON (fp.aircraft_type = rp.aircraft_type) AND (fp.board_number = rp.board_number) AND (fp.flight_number = rp.flight_number) AND (fp.pilot_number = rp.pilot_number) AND (fp.report_start_dt = rp.report_start_dt) AND (fp.report_end_dt = rp.report_end_dt) AND (fp.arm_source_file_path = rp.arm_source_file_path) AND (fp.source_file_name = rp.source_file_name) AND (fp.takeoff_datetime = rp.takeoff_datetime) AND (fp.target_date = rp.target_date) WHERE fp.aircraft_type = 'ATR-72-212' ORDER BY fp.aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, fp.board_number ASC, fp.flight_number ASC
```
</details>

### `analytics`.`fdr_express_analysis_b737700_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| board_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| year | String |  | 0 | 0 | 0 |  |
| month | String |  | 0 | 0 | 0 |  |
| day | String |  | 0 | 0 | 0 |  |
| flight_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| pilot_number | String |  | 0 | 0 | 0 |  |
| T0 | String |  | 0 | 0 | 0 |  |
| TK | String |  | 0 | 0 | 0 |  |
| route_note | Nullable(String) |  | 0 | 0 | 0 |  |
| approach_system | Nullable(String) |  | 0 | 0 | 0 |  |
| R01 | Nullable(String) |  | 0 | 0 | 0 |  |
| R03 | Nullable(String) |  | 0 | 0 | 0 |  |
| R14 | Nullable(String) |  | 0 | 0 | 0 |  |
| max_R25_R26_R27 | Nullable(String) |  | 0 | 0 | 0 |  |
| R30 | Nullable(String) |  | 0 | 0 | 0 |  |
| R31 | Nullable(String) |  | 0 | 0 | 0 |  |
| R32 | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoffSeat | Nullable(String) |  | 0 | 0 | 0 |  |
| landingSeat | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoff_datetime | DateTime |  | 0 | 0 | 0 |  |
| target_date | Date |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.fdr_express_analysis_b737700_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `approach_system` Nullable(String), `R01` Nullable(String), `R03` Nullable(String), `R14` Nullable(String), `max_R25_R26_R27` Nullable(String), `R30` Nullable(String), `R31` Nullable(String), `R32` Nullable(String), `takeoffSeat` Nullable(String), `landingSeat` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS WITH fp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key IN ('Заход на посадку', 'Система захода на посадку')) AS approach_system, maxIf(flight_parameter_value, flight_parameter_key = 'takeoffSeat') AS takeoffSeat, maxIf(flight_parameter_value, flight_parameter_key = 'landingSeat') AS landingSeat FROM analytics.fdr_express_analysis_flight_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date), rp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(regular_parameter_value, regular_parameter_key = 'R001') AS R001, maxIf(regular_parameter_value, regular_parameter_key = 'R01') AS R01, maxIf(regular_parameter_value, regular_parameter_key = 'R003') AS R003, maxIf(regular_parameter_value, regular_parameter_key = 'R03') AS R03, maxIf(regular_parameter_value, regular_parameter_key = 'R014') AS R014, maxIf(regular_parameter_value, regular_parameter_key = 'R14') AS R14, maxIf(regular_parameter_value, regular_parameter_key = 'R25') AS R25, maxIf(regular_parameter_value, regular_parameter_key = 'R26') AS R26, maxIf(regular_parameter_value, regular_parameter_key = 'R27') AS R27, maxIf(regular_parameter_value, regular_parameter_key = 'R030') AS R030, maxIf(regular_parameter_value, regular_parameter_key = 'R30') AS R30, maxIf(regular_parameter_value, regular_parameter_key = 'R031') AS R031, maxIf(regular_parameter_value, regular_parameter_key = 'R31') AS R31, maxIf(regular_parameter_value, regular_parameter_key = 'R032') AS R032, maxIf(regular_parameter_value, regular_parameter_key = 'R32') AS R32 FROM analytics.fdr_express_analysis_regular_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date) SELECT fp.aircraft_type, fp.board_number, toString(toYear(fp.report_start_dt)) AS year, toString(toMonth(fp.report_start_dt)) AS month, toString(toDayOfMonth(fp.report_start_dt)) AS day, fp.flight_number, fp.pilot_number, formatDateTime(fp.report_start_dt, '%H:%i:%S') AS T0, formatDateTime(fp.report_end_dt, '%H:%i:%S') AS TK, fp.route_note, fp.approach_system, coalesce(rp.R001, rp.R01) AS R01, coalesce(rp.R003, rp.R03) AS R03, coalesce(rp.R014, rp.R14) AS R14, if(greatest(toFloat64OrNull(rp.R25), toFloat64OrNull(rp.R26), toFloat64OrNull(rp.R27)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R25), toFloat64OrNull(rp.R26), toFloat64OrNull(rp.R27)))) AS max_R25_R26_R27, coalesce(rp.R030, rp.R30) AS R30, coalesce(rp.R031, rp.R31) AS R31, coalesce(rp.R032, rp.R32) AS R32, fp.takeoffSeat, fp.landingSeat, fp.takeoff_datetime, fp.target_date FROM fp LEFT JOIN rp ON (fp.aircraft_type = rp.aircraft_type) AND (fp.board_number = rp.board_number) AND (fp.flight_number = rp.flight_number) AND (fp.pilot_number = rp.pilot_number) AND (fp.report_start_dt = rp.report_start_dt) AND (fp.report_end_dt = rp.report_end_dt) AND (fp.arm_source_file_path = rp.arm_source_file_path) AND (fp.source_file_name = rp.source_file_name) AND (fp.takeoff_datetime = rp.takeoff_datetime) AND (fp.target_date = rp.target_date) WHERE fp.aircraft_type = 'B-737-700' ORDER BY fp.aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, fp.board_number ASC, fp.flight_number ASC
```
</details>

### `analytics`.`fdr_express_analysis_b737800_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| board_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| year | String |  | 0 | 0 | 0 |  |
| month | String |  | 0 | 0 | 0 |  |
| day | String |  | 0 | 0 | 0 |  |
| flight_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| pilot_number | String |  | 0 | 0 | 0 |  |
| T0 | String |  | 0 | 0 | 0 |  |
| TK | String |  | 0 | 0 | 0 |  |
| route_note | Nullable(String) |  | 0 | 0 | 0 |  |
| approach_system | Nullable(String) |  | 0 | 0 | 0 |  |
| R01 | Nullable(String) |  | 0 | 0 | 0 |  |
| R03 | Nullable(String) |  | 0 | 0 | 0 |  |
| R14 | Nullable(String) |  | 0 | 0 | 0 |  |
| max_R25_R26_R27 | Nullable(String) |  | 0 | 0 | 0 |  |
| R30 | Nullable(String) |  | 0 | 0 | 0 |  |
| R31 | Nullable(String) |  | 0 | 0 | 0 |  |
| R32 | Nullable(String) |  | 0 | 0 | 0 |  |
| R37 | Nullable(String) |  | 0 | 0 | 0 |  |
| max_R16_R35 | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoffSeat | Nullable(String) |  | 0 | 0 | 0 |  |
| landingSeat | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoff_datetime | DateTime |  | 0 | 0 | 0 |  |
| target_date | Date |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.fdr_express_analysis_b737800_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `approach_system` Nullable(String), `R01` Nullable(String), `R03` Nullable(String), `R14` Nullable(String), `max_R25_R26_R27` Nullable(String), `R30` Nullable(String), `R31` Nullable(String), `R32` Nullable(String), `R37` Nullable(String), `max_R16_R35` Nullable(String), `takeoffSeat` Nullable(String), `landingSeat` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS WITH fp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key IN ('Заход на посадку', 'Система захода на посадку')) AS approach_system, maxIf(flight_parameter_value, flight_parameter_key = 'takeoffSeat') AS takeoffSeat, maxIf(flight_parameter_value, flight_parameter_key = 'landingSeat') AS landingSeat FROM analytics.fdr_express_analysis_flight_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date), rp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(regular_parameter_value, regular_parameter_key = 'R001') AS R001, maxIf(regular_parameter_value, regular_parameter_key = 'R01') AS R01, maxIf(regular_parameter_value, regular_parameter_key = 'R003') AS R003, maxIf(regular_parameter_value, regular_parameter_key = 'R03') AS R03, maxIf(regular_parameter_value, regular_parameter_key = 'R014') AS R014, maxIf(regular_parameter_value, regular_parameter_key = 'R14') AS R14, maxIf(regular_parameter_value, regular_parameter_key = 'R16') AS R16, maxIf(regular_parameter_value, regular_parameter_key = 'R25') AS R25, maxIf(regular_parameter_value, regular_parameter_key = 'R26') AS R26, maxIf(regular_parameter_value, regular_parameter_key = 'R27') AS R27, maxIf(regular_parameter_value, regular_parameter_key = 'R030') AS R030, maxIf(regular_parameter_value, regular_parameter_key = 'R30') AS R30, maxIf(regular_parameter_value, regular_parameter_key = 'R031') AS R031, maxIf(regular_parameter_value, regular_parameter_key = 'R31') AS R31, maxIf(regular_parameter_value, regular_parameter_key = 'R032') AS R032, maxIf(regular_parameter_value, regular_parameter_key = 'R32') AS R32, maxIf(regular_parameter_value, regular_parameter_key = 'R35') AS R35, maxIf(regular_parameter_value, regular_parameter_key = 'R37') AS R37 FROM analytics.fdr_express_analysis_regular_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date) SELECT fp.aircraft_type, fp.board_number, toString(toYear(fp.report_start_dt)) AS year, toString(toMonth(fp.report_start_dt)) AS month, toString(toDayOfMonth(fp.report_start_dt)) AS day, fp.flight_number, fp.pilot_number, formatDateTime(fp.report_start_dt, '%H:%i:%S') AS T0, formatDateTime(fp.report_end_dt, '%H:%i:%S') AS TK, fp.route_note, fp.approach_system, coalesce(rp.R001, rp.R01) AS R01, coalesce(rp.R003, rp.R03) AS R03, coalesce(rp.R014, rp.R14) AS R14, if(greatest(toFloat64OrNull(rp.R25), toFloat64OrNull(rp.R26), toFloat64OrNull(rp.R27)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R25), toFloat64OrNull(rp.R26), toFloat64OrNull(rp.R27)))) AS max_R25_R26_R27, coalesce(rp.R030, rp.R30) AS R30, coalesce(rp.R031, rp.R31) AS R31, coalesce(rp.R032, rp.R32) AS R32, rp.R37, if(greatest(toFloat64OrNull(rp.R16), toFloat64OrNull(rp.R35)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R16), toFloat64OrNull(rp.R35)))) AS max_R16_R35, fp.takeoffSeat, fp.landingSeat, fp.takeoff_datetime, fp.target_date FROM fp LEFT JOIN rp ON (fp.aircraft_type = rp.aircraft_type) AND (fp.board_number = rp.board_number) AND (fp.flight_number = rp.flight_number) AND (fp.pilot_number = rp.pilot_number) AND (fp.report_start_dt = rp.report_start_dt) AND (fp.report_end_dt = rp.report_end_dt) AND (fp.arm_source_file_path = rp.arm_source_file_path) AND (fp.source_file_name = rp.source_file_name) AND (fp.takeoff_datetime = rp.takeoff_datetime) AND (fp.target_date = rp.target_date) WHERE fp.aircraft_type = 'B-737-800' ORDER BY fp.aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, fp.board_number ASC, fp.flight_number ASC
```
</details>

### `analytics`.`fdr_express_analysis_b737CL_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| board_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| year | String |  | 0 | 0 | 0 |  |
| month | String |  | 0 | 0 | 0 |  |
| day | String |  | 0 | 0 | 0 |  |
| flight_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| pilot_number | String |  | 0 | 0 | 0 |  |
| T0 | String |  | 0 | 0 | 0 |  |
| TK | String |  | 0 | 0 | 0 |  |
| route_note | Nullable(String) |  | 0 | 0 | 0 |  |
| approach_system | Nullable(String) |  | 0 | 0 | 0 |  |
| R01 | Nullable(String) |  | 0 | 0 | 0 |  |
| R03 | Nullable(String) |  | 0 | 0 | 0 |  |
| R18 | Nullable(String) |  | 0 | 0 | 0 |  |
| max_R20_R21_R22 | Nullable(String) |  | 0 | 0 | 0 |  |
| R27 | Nullable(String) |  | 0 | 0 | 0 |  |
| R25 | Nullable(String) |  | 0 | 0 | 0 |  |
| R29 | Nullable(String) |  | 0 | 0 | 0 |  |
| max_R32_R33 | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoffSeat | Nullable(String) |  | 0 | 0 | 0 |  |
| landingSeat | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoff_datetime | DateTime |  | 0 | 0 | 0 |  |
| target_date | Date |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.fdr_express_analysis_b737CL_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `approach_system` Nullable(String), `R01` Nullable(String), `R03` Nullable(String), `R18` Nullable(String), `max_R20_R21_R22` Nullable(String), `R27` Nullable(String), `R25` Nullable(String), `R29` Nullable(String), `max_R32_R33` Nullable(String), `takeoffSeat` Nullable(String), `landingSeat` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS WITH fp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key IN ('Заход на посадку', 'Система захода на посадку')) AS approach_system, maxIf(flight_parameter_value, flight_parameter_key = 'takeoffSeat') AS takeoffSeat, maxIf(flight_parameter_value, flight_parameter_key = 'landingSeat') AS landingSeat FROM analytics.fdr_express_analysis_flight_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date), rp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(regular_parameter_value, regular_parameter_key = 'R001') AS R001, maxIf(regular_parameter_value, regular_parameter_key = 'R01') AS R01, maxIf(regular_parameter_value, regular_parameter_key = 'R003') AS R003, maxIf(regular_parameter_value, regular_parameter_key = 'R03') AS R03, maxIf(regular_parameter_value, regular_parameter_key = 'R18') AS R18, maxIf(regular_parameter_value, regular_parameter_key = 'R20') AS R20, maxIf(regular_parameter_value, regular_parameter_key = 'R21') AS R21, maxIf(regular_parameter_value, regular_parameter_key = 'R22') AS R22, maxIf(regular_parameter_value, regular_parameter_key = 'R25') AS R25, maxIf(regular_parameter_value, regular_parameter_key = 'R27') AS R27, maxIf(regular_parameter_value, regular_parameter_key = 'R29') AS R29, maxIf(regular_parameter_value, regular_parameter_key = 'R32') AS R32, maxIf(regular_parameter_value, regular_parameter_key = 'R33') AS R33 FROM analytics.fdr_express_analysis_regular_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date) SELECT fp.aircraft_type, fp.board_number, toString(toYear(fp.report_start_dt)) AS year, toString(toMonth(fp.report_start_dt)) AS month, toString(toDayOfMonth(fp.report_start_dt)) AS day, fp.flight_number, fp.pilot_number, formatDateTime(fp.report_start_dt, '%H:%i:%S') AS T0, formatDateTime(fp.report_end_dt, '%H:%i:%S') AS TK, fp.route_note, fp.approach_system, coalesce(rp.R001, rp.R01) AS R01, coalesce(rp.R003, rp.R03) AS R03, rp.R18, if(greatest(toFloat64OrNull(rp.R20), toFloat64OrNull(rp.R21), toFloat64OrNull(rp.R22)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R20), toFloat64OrNull(rp.R21), toFloat64OrNull(rp.R22)))) AS max_R20_R21_R22, rp.R27, rp.R25, rp.R29, if(greatest(toFloat64OrNull(rp.R32), toFloat64OrNull(rp.R33)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R32), toFloat64OrNull(rp.R33)))) AS max_R32_R33, fp.takeoffSeat, fp.landingSeat, fp.takeoff_datetime, fp.target_date FROM fp LEFT JOIN rp ON (fp.aircraft_type = rp.aircraft_type) AND (fp.board_number = rp.board_number) AND (fp.flight_number = rp.flight_number) AND (fp.pilot_number = rp.pilot_number) AND (fp.report_start_dt = rp.report_start_dt) AND (fp.report_end_dt = rp.report_end_dt) AND (fp.arm_source_file_path = rp.arm_source_file_path) AND (fp.source_file_name = rp.source_file_name) AND (fp.takeoff_datetime = rp.takeoff_datetime) AND (fp.target_date = rp.target_date) WHERE fp.aircraft_type IN ('B-737-400', 'B-737-500', 'B-737-500W') ORDER BY fp.aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, fp.board_number ASC, fp.flight_number ASC
```
</details>

### `analytics`.`fdr_express_analysis_b767_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| board_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| year | String |  | 0 | 0 | 0 |  |
| month | String |  | 0 | 0 | 0 |  |
| day | String |  | 0 | 0 | 0 |  |
| flight_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| pilot_number | String |  | 0 | 0 | 0 |  |
| T0 | String |  | 0 | 0 | 0 |  |
| TK | String |  | 0 | 0 | 0 |  |
| route_note | Nullable(String) |  | 0 | 0 | 0 |  |
| approach_system | Nullable(String) |  | 0 | 0 | 0 |  |
| R01 | Nullable(String) |  | 0 | 0 | 0 |  |
| R05 | Nullable(String) |  | 0 | 0 | 0 |  |
| R28 | Nullable(String) |  | 0 | 0 | 0 |  |
| max_R29_R30_R31 | Nullable(String) |  | 0 | 0 | 0 |  |
| R36 | Nullable(String) |  | 0 | 0 | 0 |  |
| R38 | Nullable(String) |  | 0 | 0 | 0 |  |
| R39 | Nullable(String) |  | 0 | 0 | 0 |  |
| max_R79_R81 | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoffSeat | Nullable(String) |  | 0 | 0 | 0 |  |
| landingSeat | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoff_datetime | DateTime |  | 0 | 0 | 0 |  |
| target_date | Date |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.fdr_express_analysis_b767_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `approach_system` Nullable(String), `R01` Nullable(String), `R05` Nullable(String), `R28` Nullable(String), `max_R29_R30_R31` Nullable(String), `R36` Nullable(String), `R38` Nullable(String), `R39` Nullable(String), `max_R79_R81` Nullable(String), `takeoffSeat` Nullable(String), `landingSeat` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS WITH fp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key IN ('Заход на посадку', 'Система захода на посадку')) AS approach_system, maxIf(flight_parameter_value, flight_parameter_key = 'takeoffSeat') AS takeoffSeat, maxIf(flight_parameter_value, flight_parameter_key = 'landingSeat') AS landingSeat FROM analytics.fdr_express_analysis_flight_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date), rp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(regular_parameter_value, regular_parameter_key = 'R001') AS R001, maxIf(regular_parameter_value, regular_parameter_key = 'R01') AS R01, maxIf(regular_parameter_value, regular_parameter_key = 'R05') AS R05, maxIf(regular_parameter_value, regular_parameter_key = 'R28') AS R28, maxIf(regular_parameter_value, regular_parameter_key = 'R29') AS R29, maxIf(regular_parameter_value, regular_parameter_key = 'R30') AS R30, maxIf(regular_parameter_value, regular_parameter_key = 'R31') AS R31, maxIf(regular_parameter_value, regular_parameter_key = 'R36') AS R36, maxIf(regular_parameter_value, regular_parameter_key = 'R38') AS R38, maxIf(regular_parameter_value, regular_parameter_key = 'R39') AS R39, maxIf(regular_parameter_value, regular_parameter_key = 'R79') AS R79, maxIf(regular_parameter_value, regular_parameter_key = 'R81') AS R81 FROM analytics.fdr_express_analysis_regular_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date) SELECT fp.aircraft_type, fp.board_number, toString(toYear(fp.report_start_dt)) AS year, toString(toMonth(fp.report_start_dt)) AS month, toString(toDayOfMonth(fp.report_start_dt)) AS day, fp.flight_number, fp.pilot_number, formatDateTime(fp.report_start_dt, '%H:%i:%S') AS T0, formatDateTime(fp.report_end_dt, '%H:%i:%S') AS TK, fp.route_note, fp.approach_system, coalesce(rp.R001, rp.R01) AS R01, rp.R05, rp.R28, if(greatest(toFloat64OrNull(rp.R29), toFloat64OrNull(rp.R30), toFloat64OrNull(rp.R31)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R29), toFloat64OrNull(rp.R30), toFloat64OrNull(rp.R31)))) AS max_R29_R30_R31, rp.R36, rp.R38, rp.R39, if(greatest(toFloat64OrNull(rp.R79), toFloat64OrNull(rp.R81)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R79), toFloat64OrNull(rp.R81)))) AS max_R79_R81, fp.takeoffSeat, fp.landingSeat, fp.takeoff_datetime, fp.target_date FROM fp LEFT JOIN rp ON (fp.aircraft_type = rp.aircraft_type) AND (fp.board_number = rp.board_number) AND (fp.flight_number = rp.flight_number) AND (fp.pilot_number = rp.pilot_number) AND (fp.report_start_dt = rp.report_start_dt) AND (fp.report_end_dt = rp.report_end_dt) AND (fp.arm_source_file_path = rp.arm_source_file_path) AND (fp.source_file_name = rp.source_file_name) AND (fp.takeoff_datetime = rp.takeoff_datetime) AND (fp.target_date = rp.target_date) WHERE fp.aircraft_type = 'B-767' ORDER BY fp.aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, fp.board_number ASC, fp.flight_number ASC
```
</details>

### `analytics`.`fdr_express_analysis_flight_messages`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(target_date)
- Sorting key: target_date, aircraft_type, board_number, flight_number
- Primary key: target_date, aircraft_type, board_number, flight_number
- Total rows: 10592
- Total bytes: 280911

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_type | LowCardinality(String) |  | 1 | 1 | 0 | Тип ВС |
| board_number | LowCardinality(String) |  | 1 | 1 | 0 | Бортовой номер ВС |
| flight_number | LowCardinality(String) |  | 1 | 1 | 0 | Номер рейса |
| pilot_number | String |  | 0 | 0 | 0 | Табельный номер пилота из отчета |
| report_start_dt | DateTime |  | 0 | 0 | 0 | Дата и время начала отчета |
| report_end_dt | DateTime |  | 0 | 0 | 0 | Дата и время завершения отчета |
| arm_source_file_path | String |  | 0 | 0 | 0 | Путь к исходному ARM-файлу |
| source_file_name | String |  | 0 | 0 | 0 | Имя исходного DOCX-файла |
| flight_message_section | String |  | 0 | 0 | 0 | Название секции сообщения |
| flight_message_event | Nullable(String) |  | 0 | 0 | 0 | Код или номер события |
| flight_message_start_time | Nullable(String) |  | 0 | 0 | 0 | Время начала события |
| flight_message_end_time | Nullable(String) |  | 0 | 0 | 0 | Время завершения события |
| flight_message_interval | Nullable(String) |  | 0 | 0 | 0 | Длительность события |
| flight_message_text | Nullable(String) |  | 0 | 0 | 0 | Текстовое описание события |
| takeoff_datetime | DateTime |  | 0 | 0 | 0 | Дата и время взлета |
| target_date | Date |  | 1 | 1 | 1 | Дата обработки файла |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC).          Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.fdr_express_analysis_flight_messages (`aircraft_type` LowCardinality(String) COMMENT 'Тип ВС', `board_number` LowCardinality(String) COMMENT 'Бортовой номер ВС', `flight_number` LowCardinality(String) COMMENT 'Номер рейса', `pilot_number` String COMMENT 'Табельный номер пилота из отчета', `report_start_dt` DateTime COMMENT 'Дата и время начала отчета', `report_end_dt` DateTime COMMENT 'Дата и время завершения отчета', `arm_source_file_path` String COMMENT 'Путь к исходному ARM-файлу', `source_file_name` String COMMENT 'Имя исходного DOCX-файла', `flight_message_section` String COMMENT 'Название секции сообщения', `flight_message_event` Nullable(String) COMMENT 'Код или номер события', `flight_message_start_time` Nullable(String) COMMENT 'Время начала события', `flight_message_end_time` Nullable(String) COMMENT 'Время завершения события', `flight_message_interval` Nullable(String) COMMENT 'Длительность события', `flight_message_text` Nullable(String) COMMENT 'Текстовое описание события', `takeoff_datetime` DateTime COMMENT 'Дата и время взлета', `target_date` Date COMMENT 'Дата обработки файла', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC).\r\n        Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/fdr_express_analysis_flight_messages', '{replica}') PARTITION BY toYYYYMM(target_date) ORDER BY (target_date, aircraft_type, board_number, flight_number) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения данных сообщений полета из документа Express Analysis (Т2: Сообщения)'
```
</details>

### `analytics`.`fdr_express_analysis_flight_parameters`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(target_date)
- Sorting key: target_date, aircraft_type, board_number, flight_number, flight_parameter_key
- Primary key: target_date, aircraft_type, board_number, flight_number, flight_parameter_key
- Total rows: 13755
- Total bytes: 123139

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_type | LowCardinality(String) |  | 1 | 1 | 0 | Тип ВС |
| board_number | LowCardinality(String) |  | 1 | 1 | 0 | Бортовой номер ВС |
| flight_number | LowCardinality(String) |  | 1 | 1 | 0 | Номер рейса |
| pilot_number | String |  | 0 | 0 | 0 | Табельный номер пилота из отчета |
| report_start_dt | DateTime |  | 0 | 0 | 0 | Дата и время начала отчета |
| report_end_dt | DateTime |  | 0 | 0 | 0 | Дата и время завершения отчета |
| arm_source_file_path | String |  | 0 | 0 | 0 | Путь к исходному ARM-файлу |
| source_file_name | String |  | 0 | 0 | 0 | Имя исходного DOCX-файла |
| flight_parameter_key | String |  | 1 | 1 | 0 | Ключ параметра полета |
| flight_parameter_value | Nullable(String) |  | 0 | 0 | 0 | Значение параметра полета |
| takeoff_datetime | DateTime |  | 0 | 0 | 0 | Дата и время взлета |
| target_date | Date |  | 1 | 1 | 1 | Дата обработки файла |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC).          Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.fdr_express_analysis_flight_parameters (`aircraft_type` LowCardinality(String) COMMENT 'Тип ВС', `board_number` LowCardinality(String) COMMENT 'Бортовой номер ВС', `flight_number` LowCardinality(String) COMMENT 'Номер рейса', `pilot_number` String COMMENT 'Табельный номер пилота из отчета', `report_start_dt` DateTime COMMENT 'Дата и время начала отчета', `report_end_dt` DateTime COMMENT 'Дата и время завершения отчета', `arm_source_file_path` String COMMENT 'Путь к исходному ARM-файлу', `source_file_name` String COMMENT 'Имя исходного DOCX-файла', `flight_parameter_key` String COMMENT 'Ключ параметра полета', `flight_parameter_value` Nullable(String) COMMENT 'Значение параметра полета', `takeoff_datetime` DateTime COMMENT 'Дата и время взлета', `target_date` Date COMMENT 'Дата обработки файла', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC).\r\n        Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/fdr_express_analysis_flight_parameters', '{replica}') PARTITION BY toYYYYMM(target_date) ORDER BY (target_date, aircraft_type, board_number, flight_number, flight_parameter_key) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения данных параметров полета из документа Express Analysis (Т1)'
```
</details>

### `analytics`.`fdr_express_analysis_gulfstream_g4_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| board_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| year | String |  | 0 | 0 | 0 |  |
| month | String |  | 0 | 0 | 0 |  |
| day | String |  | 0 | 0 | 0 |  |
| flight_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| pilot_number | String |  | 0 | 0 | 0 |  |
| T0 | String |  | 0 | 0 | 0 |  |
| TK | String |  | 0 | 0 | 0 |  |
| route_note | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoff_weight | Nullable(String) |  | 0 | 0 | 0 |  |
| landing_weight | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoff_datetime | DateTime |  | 0 | 0 | 0 |  |
| target_date | Date |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.fdr_express_analysis_gulfstream_g4_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `takeoff_weight` Nullable(String), `landing_weight` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS SELECT aircraft_type, board_number, toString(toYear(report_start_dt)) AS year, toString(toMonth(report_start_dt)) AS month, toString(toDayOfMonth(report_start_dt)) AS day, flight_number, pilot_number, formatDateTime(report_start_dt, '%H:%i:%S') AS T0, formatDateTime(report_end_dt, '%H:%i:%S') AS TK, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key = 'Взлетный вес, т') AS takeoff_weight, maxIf(flight_parameter_value, flight_parameter_key = 'Посадочный вес, т') AS landing_weight, takeoff_datetime, target_date FROM analytics.fdr_express_analysis_flight_parameters WHERE aircraft_type = 'Gulfstream IV-SP' GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date ORDER BY aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, board_number ASC, flight_number ASC
```
</details>

### `analytics`.`fdr_express_analysis_gulfstream_g6_view`
- Engine: View
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_type | LowCardinality(String) |  | 0 | 0 | 0 |  |
| board_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| year | String |  | 0 | 0 | 0 |  |
| month | String |  | 0 | 0 | 0 |  |
| day | String |  | 0 | 0 | 0 |  |
| flight_number | LowCardinality(String) |  | 0 | 0 | 0 |  |
| pilot_number | String |  | 0 | 0 | 0 |  |
| T0 | String |  | 0 | 0 | 0 |  |
| TK | String |  | 0 | 0 | 0 |  |
| route_note | Nullable(String) |  | 0 | 0 | 0 |  |
| weight_without_fuel | Nullable(String) |  | 0 | 0 | 0 |  |
| takeoff_datetime | DateTime |  | 0 | 0 | 0 |  |
| target_date | Date |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE VIEW analytics.fdr_express_analysis_gulfstream_g6_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `weight_without_fuel` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS SELECT aircraft_type, board_number, toString(toYear(report_start_dt)) AS year, toString(toMonth(report_start_dt)) AS month, toString(toDayOfMonth(report_start_dt)) AS day, flight_number, pilot_number, formatDateTime(report_start_dt, '%H:%i:%S') AS T0, formatDateTime(report_end_dt, '%H:%i:%S') AS TK, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key = 'Вес самолета без топлива, т') AS weight_without_fuel, takeoff_datetime, target_date FROM analytics.fdr_express_analysis_flight_parameters WHERE aircraft_type = 'Gulfstream 650' GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date ORDER BY aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, board_number ASC, flight_number ASC
```
</details>

### `analytics`.`fdr_express_analysis_regular_parameters`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(target_date)
- Sorting key: target_date, aircraft_type, board_number, flight_number, regular_parameter_key
- Primary key: target_date, aircraft_type, board_number, flight_number, regular_parameter_key
- Total rows: 58698
- Total bytes: 1034438

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| aircraft_type | LowCardinality(String) |  | 1 | 1 | 0 | Тип ВС |
| board_number | LowCardinality(String) |  | 1 | 1 | 0 | Бортовой номер ВС |
| flight_number | LowCardinality(String) |  | 1 | 1 | 0 | Номер рейса |
| pilot_number | String |  | 0 | 0 | 0 | Табельный номер пилота из отчета |
| report_start_dt | DateTime |  | 0 | 0 | 0 | Дата и время начала отчета |
| report_end_dt | DateTime |  | 0 | 0 | 0 | Дата и время завершения отчета |
| arm_source_file_path | String |  | 0 | 0 | 0 | Путь к исходному ARM-файлу |
| source_file_name | String |  | 0 | 0 | 0 | Имя исходного DOCX-файла |
| regular_parameter_time | Nullable(String) |  | 0 | 0 | 0 | Время параметра |
| regular_parameter_key | String |  | 1 | 1 | 0 | Ключ параметра |
| regular_parameter_value | Nullable(String) |  | 0 | 0 | 0 | Значение параметра |
| regular_parameter_designation | Nullable(String) |  | 0 | 0 | 0 | Обозначение параметра |
| regular_parameter_text | Nullable(String) |  | 0 | 0 | 0 | Текстовое описание параметра |
| takeoff_datetime | DateTime |  | 0 | 0 | 0 | Дата и время взлета |
| target_date | Date |  | 1 | 1 | 1 | Дата обработки файла |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC).          Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.fdr_express_analysis_regular_parameters (`aircraft_type` LowCardinality(String) COMMENT 'Тип ВС', `board_number` LowCardinality(String) COMMENT 'Бортовой номер ВС', `flight_number` LowCardinality(String) COMMENT 'Номер рейса', `pilot_number` String COMMENT 'Табельный номер пилота из отчета', `report_start_dt` DateTime COMMENT 'Дата и время начала отчета', `report_end_dt` DateTime COMMENT 'Дата и время завершения отчета', `arm_source_file_path` String COMMENT 'Путь к исходному ARM-файлу', `source_file_name` String COMMENT 'Имя исходного DOCX-файла', `regular_parameter_time` Nullable(String) COMMENT 'Время параметра', `regular_parameter_key` String COMMENT 'Ключ параметра', `regular_parameter_value` Nullable(String) COMMENT 'Значение параметра', `regular_parameter_designation` Nullable(String) COMMENT 'Обозначение параметра', `regular_parameter_text` Nullable(String) COMMENT 'Текстовое описание параметра', `takeoff_datetime` DateTime COMMENT 'Дата и время взлета', `target_date` Date COMMENT 'Дата обработки файла', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC).\r\n        Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/fdr_express_analysis_regular_parameters', '{replica}') PARTITION BY toYYYYMM(target_date) ORDER BY (target_date, aircraft_type, board_number, flight_number, regular_parameter_key) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения данных регулярных параметров и наработки АТ из документа Express Analysis (Т3: Регулярная информация, Т4: Наработка АТ)'
```
</details>

### `analytics`.`flight_data_recorders_telemetry`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMMDD(processing_date)
- Sorting key: processing_date, board_number, flight_date, flight_detail, parameter, flight_time_seconds
- Primary key: processing_date, board_number, flight_date, flight_detail, parameter, flight_time_seconds
- Total rows: 33569612605
- Total bytes: 238077563126

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| board_number | LowCardinality(String) |  | 1 | 1 | 0 | Номер борта воздушного судна |
| flight_detail | String |  | 1 | 1 | 0 | Идентификатор полёта из имени файла: время посадки HHMMSS (Boeing) или номер полёта (ATR). При разбивке файла на сегменты добавляется постфикс через подчёркивание (например: 1166_1) |
| flight_date | Date |  | 1 | 1 | 0 | Дата полета |
| flight_time_seconds | Float64 |  | 1 | 1 | 0 | Время в секундах от начала суток для сортировки |
| parameter | LowCardinality(String) |  | 1 | 1 | 0 | Название телеметрического параметра |
| value | Nullable(Decimal(18, 6)) |  | 0 | 0 | 0 | Значение параметра |
| file_name | String |  | 0 | 0 | 0 | Имя исходного файла с данными |
| file_signature | String |  | 0 | 0 | 0 | Подпись файла для связки с source таблицей |
| file_created_at | DateTime |  | 0 | 0 | 0 | Дата создания файла для связки с source таблицей |
| processing_date | Date |  | 1 | 1 | 1 | Дата обработки файла (когда файл был взят и обработан из S3) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.flight_data_recorders_telemetry (`board_number` LowCardinality(String) COMMENT 'Номер борта воздушного судна', `flight_detail` String COMMENT 'Идентификатор полёта из имени файла: время посадки HHMMSS (Boeing) или номер полёта (ATR). При разбивке файла на сегменты добавляется постфикс через подчёркивание (например: 1166_1)', `flight_date` Date COMMENT 'Дата полета', `flight_time_seconds` Float64 COMMENT 'Время в секундах от начала суток для сортировки', `parameter` LowCardinality(String) COMMENT 'Название телеметрического параметра', `value` Nullable(Decimal(18, 6)) COMMENT 'Значение параметра', `file_name` String COMMENT 'Имя исходного файла с данными', `file_signature` String COMMENT 'Подпись файла для связки с source таблицей', `file_created_at` DateTime COMMENT 'Дата создания файла для связки с source таблицей', `processing_date` Date COMMENT 'Дата обработки файла (когда файл был взят и обработан из S3)', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/flight_data_recorders_telemetry', '{replica}') PARTITION BY toYYYYMMDD(processing_date) ORDER BY (processing_date, board_number, flight_date, flight_detail, parameter, flight_time_seconds) SETTINGS index_granularity = 16384 COMMENT 'Таблица для хранения телеметрических данных полетов с оптимизацией по изменениям параметров.'
```
</details>

### `analytics`.`flight_data_recorders_telemetry_distributed`
- Engine: Distributed
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| board_number | LowCardinality(String) |  | 0 | 0 | 0 | Номер борта воздушного судна |
| flight_detail | String |  | 0 | 0 | 0 | Идентификатор полёта из имени файла: время посадки HHMMSS (Boeing) или номер полёта (ATR). При разбивке файла на сегменты добавляется постфикс через подчёркивание (например: 1166_1) |
| flight_date | Date |  | 0 | 0 | 0 | Дата полета |
| flight_time_seconds | Float64 |  | 0 | 0 | 0 | Время в секундах от начала суток для сортировки |
| parameter | LowCardinality(String) |  | 0 | 0 | 0 | Название телеметрического параметра |
| value | Nullable(Decimal(18, 6)) |  | 0 | 0 | 0 | Значение параметра |
| file_name | String |  | 0 | 0 | 0 | Имя исходного файла с данными |
| file_signature | String |  | 0 | 0 | 0 | Подпись файла для связки с source таблицей |
| file_created_at | DateTime |  | 0 | 0 | 0 | Дата создания файла для связки с source таблицей |
| processing_date | Date |  | 0 | 0 | 0 | Дата обработки файла (когда файл был взят и обработан из S3) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник записи. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки строки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.flight_data_recorders_telemetry_distributed (`board_number` LowCardinality(String) COMMENT 'Номер борта воздушного судна', `flight_detail` String COMMENT 'Идентификатор полёта из имени файла: время посадки HHMMSS (Boeing) или номер полёта (ATR). При разбивке файла на сегменты добавляется постфикс через подчёркивание (например: 1166_1)', `flight_date` Date COMMENT 'Дата полета', `flight_time_seconds` Float64 COMMENT 'Время в секундах от начала суток для сортировки', `parameter` LowCardinality(String) COMMENT 'Название телеметрического параметра', `value` Nullable(Decimal(18, 6)) COMMENT 'Значение параметра', `file_name` String COMMENT 'Имя исходного файла с данными', `file_signature` String COMMENT 'Подпись файла для связки с source таблицей', `file_created_at` DateTime COMMENT 'Дата создания файла для связки с source таблицей', `processing_date` Date COMMENT 'Дата обработки файла (когда файл был взят и обработан из S3)', `meta_source` LowCardinality(String) COMMENT 'Источник записи. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки строки в ClickHouse (UTC). Служебное поле.') ENGINE = Distributed('{cluster}', 'analytics', 'flight_data_recorders_telemetry', cityHash64(board_number, flight_date, flight_detail)) COMMENT 'Таблица для хранения телеметрических данных полетов с оптимизацией по изменениям параметров.'
```
</details>

### `analytics`.`history_of_currency`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: processing_date
- Primary key: processing_date
- Total rows: 80150
- Total bytes: 450603

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| currency | LowCardinality(String) |  | 0 | 0 | 0 | Буквенный код валюты |
| currency_rate | Decimal(18, 12) |  | 0 | 0 | 0 | Обменный курс к рублю |
| currency_name | LowCardinality(String) |  | 0 | 0 | 0 | Наименование валюты |
| processing_date | Date |  | 1 | 1 | 0 | Дата обработки файла, на которую зафиксирован курс валюты (из data_interval_start DAG) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.history_of_currency (`currency` LowCardinality(String) COMMENT 'Буквенный код валюты', `currency_rate` Decimal(18, 12) COMMENT 'Обменный курс к рублю', `currency_name` LowCardinality(String) COMMENT 'Наименование валюты', `processing_date` Date COMMENT 'Дата обработки файла, на которую зафиксирован курс валюты (из data_interval_start DAG)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/history_of_currency', '{replica}') ORDER BY processing_date SETTINGS index_granularity = 8192 COMMENT 'Данные истории обмена курсов валют по отношению к рублю'
```
</details>

### `analytics`.`history_of_currency_mv`
- Engine: MaterializedView
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| currency | String |  | 0 | 0 | 0 |  |
| currency_rate | Decimal(18, 6) |  | 0 | 0 | 0 |  |
| currency_name | String |  | 0 | 0 | 0 |  |
| processing_date | Date |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |
| meta_loading_at | DateTime |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE MATERIALIZED VIEW analytics.history_of_currency_mv TO analytics.history_of_currency (`currency` String, `currency_rate` Decimal(18, 6), `currency_name` String, `processing_date` Date, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT JSONExtractString(response_data, 'CharCode') AS currency, toDecimal64(JSONExtractString(response_data, 'Value'), 6) / toDecimal64(JSONExtractString(response_data, 'Nominal'), 0) AS currency_rate, JSONExtractString(response_data, 'Name') AS currency_name, processing_date, meta_source, now() AS meta_loading_at FROM source.history_of_currency
```
</details>

### `analytics`.`lime_survey_answers_after_flight`
- Engine: ReplicatedMergeTree
- Partition key: 
- Sorting key: flight_date, send_dt, id
- Primary key: flight_date, send_dt, id
- Total rows: 347902
- Total bytes: 16748822

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id | Int32 |  | 1 | 1 | 0 | Уникальный идентификатор ответа в LimeSurvey |
| start_dt | DateTime |  | 0 | 0 | 0 | Момент начала заполнения анкеты |
| submit_dt | Nullable(DateTime) |  | 0 | 0 | 0 | Момент окончания заполнения анкеты |
| send_dt | DateTime |  | 1 | 1 | 0 | Момент отправки заполненной анкеты |
| email | Nullable(String) |  | 0 | 0 | 0 | Адрес электронной почты респондента |
| flight_number | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Номер рейса, указанный респондентом (например: UT-123) |
| flight_date | Date |  | 1 | 1 | 0 | Дата рейса (преобразована из строкового формата источника), Null значения заменены на 1970-01-01 |
| seat | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Номер места на борту |
| ticket | Nullable(String) |  | 0 | 0 | 0 | Номер авиабилета пассажира |
| departure_airport | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Город аэропорта вылета |
| arrival_airport | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Город аэропорта прилета |
| gender | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Пол респондента |
| birthday | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Дата рождения в строковом формате источника |
| flight_rating | Nullable(Int8) |  | 0 | 0 | 0 | Общая оценка перелёта (0–5) |
| website_rating | Nullable(Int8) |  | 0 | 0 | 0 | Оценка веб-сайта Utair (0–5) |
| airport_rating | Nullable(Int8) |  | 0 | 0 | 0 | Оценка сервиса в аэропорту (0–5) |
| board_rating | Nullable(Int8) |  | 0 | 0 | 0 | Оценка сервиса на борту (0–5) |
| is_ticket_purchase | UInt8 |  | 0 | 0 | 0 | Покупал билет через сайт: 1 — да, 0 — нет |
| is_addon_services | UInt8 |  | 0 | 0 | 0 | Оформлял дополнительные услуги через сайт: 1 — да, 0 — нет |
| is_checkin | UInt8 |  | 0 | 0 | 0 | Проходил регистрацию на рейс через сайт: 1 — да, 0 — нет |
| other1 | Nullable(String) |  | 0 | 0 | 0 | Свободный комментарий о действиях на сайте и в приложении |
| is_with_ticket | UInt8 |  | 0 | 0 | 0 | Доп. услуга оформлена при покупке билета: 1 — да, 0 — нет |
| is_after_purchase | UInt8 |  | 0 | 0 | 0 | Доп. услуга оформлена после покупки билета: 1 — да, 0 — нет |
| is_online_checkin | UInt8 |  | 0 | 0 | 0 | Доп. услуга оформлена на онлайн-регистрации: 1 — да, 0 — нет |
| is_at_airport | UInt8 |  | 0 | 0 | 0 | Доп. услуга оформлена в аэропорту: 1 — да, 0 — нет |
| is_convenience_1 | UInt8 |  | 0 | 0 | 0 | Удобство использования 1: 1 — отмечено, 0 — нет |
| is_info_completeness_1 | UInt8 |  | 0 | 0 | 0 | Полнота информации 1: 1 — отмечено, 0 — нет |
| is_miles_promo_use_1 | UInt8 |  | 0 | 0 | 0 | Возможность применить мили/промокод 1: 1 — отмечено, 0 — нет |
| is_convenience_2 | UInt8 |  | 0 | 0 | 0 | Удобство использования 2: 1 — отмечено, 0 — нет |
| is_info_completeness_2 | UInt8 |  | 0 | 0 | 0 | Полнота информации 2: 1 — отмечено, 0 — нет |
| is_miles_promo_use_2 | UInt8 |  | 0 | 0 | 0 | Возможность применить мили/промокод 2: 1 — отмечено, 0 — нет |
| is_convenience_3 | UInt8 |  | 0 | 0 | 0 | Удобство использования 3: 1 — отмечено, 0 — нет |
| is_info_completeness_3 | UInt8 |  | 0 | 0 | 0 | Полнота информации 3: 1 — отмечено, 0 — нет |
| is_miles_promo_use_3 | UInt8 |  | 0 | 0 | 0 | Возможность применить мили/промокод 3: 1 — отмечено, 0 — нет |
| is_self_checkin | UInt8 |  | 0 | 0 | 0 | Прошёл самостоятельную онлайн-регистрацию: 1 — да, 0 — нет |
| is_counter_checkin | UInt8 |  | 0 | 0 | 0 | Прошёл регистрацию на стойке в аэропорту: 1 — да, 0 — нет |
| is_boarding_area | UInt8 |  | 0 | 0 | 0 | Воспользовался зоной выхода на посадку: 1 — да, 0 — нет |
| other2 | Nullable(String) |  | 0 | 0 | 0 | Свободный комментарий о сервисе в аэропорте |
| is_print_bp_self | UInt8 |  | 0 | 0 | 0 | Распечатал посадочный талон самостоятельно: 1 — да, 0 — нет |
| is_mobile_bp_scan | UInt8 |  | 0 | 0 | 0 | Прошёл досмотр с экрана мобильного устройства: 1 — да, 0 — нет |
| is_no_options | UInt8 |  | 0 | 0 | 0 | Не воспользовался ни одной из перечисленных возможностей: 1 — да, 0 — нет |
| is_staff_communication | UInt8 |  | 0 | 0 | 0 | Отметил коммуникацию и доброжелательность персонала аэропорта: 1 — да, 0 — нет |
| is_baggage_rules_clarity | UInt8 |  | 0 | 0 | 0 | Отметил понятность правил провоза багажа: 1 — да, 0 — нет |
| is_addon_service_speed | UInt8 |  | 0 | 0 | 0 | Отметил скорость и удобство оформления дополнительных услуг: 1 — да, 0 — нет |
| is_gate_info | UInt8 |  | 0 | 0 | 0 | Отметил информирование о номере выхода на посадку и времени посадки: 1 — да, 0 — нет |
| is_display_info_quality | UInt8 |  | 0 | 0 | 0 | Отметил актуальность и наглядность информации на табло: 1 — да, 0 — нет |
| is_cabin_condition | UInt8 |  | 0 | 0 | 0 | Отметил состояние салона самолёта: 1 — да, 0 — нет |
| is_onboard_info | UInt8 |  | 0 | 0 | 0 | Отметил качество информирования пассажиров на борту: 1 — да, 0 — нет |
| is_cabin_crew | UInt8 |  | 0 | 0 | 0 | Отметил работу бортпроводников: 1 — да, 0 — нет |
| other3 | Nullable(String) |  | 0 | 0 | 0 | Свободный комментарий о качестве сервиса на борту |
| is_seat_equipment_condition | UInt8 |  | 0 | 0 | 0 | Отметил исправность кресел, подлокотников, столиков и освещения: 1 — да, 0 — нет |
| is_cleanliness | UInt8 |  | 0 | 0 | 0 | Отметил чистоту салона: 1 — да, 0 — нет |
| is_captain_announcements | UInt8 |  | 0 | 0 | 0 | Отметил объявления командира воздушного судна о полёте: 1 — да, 0 — нет |
| is_pa_clarity | UInt8 |  | 0 | 0 | 0 | Отметил громкость и чёткость объявлений бортпроводников: 1 — да, 0 — нет |
| is_info_clarity | UInt8 |  | 0 | 0 | 0 | Отметил понятность информации о правилах поведения на борту: 1 — да, 0 — нет |
| is_service_info | UInt8 |  | 0 | 0 | 0 | Отметил информирование о доступных услугах (бесплатных и платных): 1 — да, 0 — нет |
| is_responsiveness | UInt8 |  | 0 | 0 | 0 | Отметил отзывчивость бортпроводников: 1 — да, 0 — нет |
| is_politeness | UInt8 |  | 0 | 0 | 0 | Отметил вежливость бортпроводников: 1 — да, 0 — нет |
| is_issue_handling | UInt8 |  | 0 | 0 | 0 | Отметил реагирование бортпроводников на нестандартные ситуации: 1 — да, 0 — нет |
| open_feedback | Nullable(String) |  | 0 | 0 | 0 | Развёрнутый отзыв пассажира о перелёте в свободной форме |
| processing_dt | DateTime |  | 0 | 0 | 0 | Дата и время обработки записи в DAG (UTC) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Источник. Служебное поле. |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Момент загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.lime_survey_answers_after_flight (`id` Int32 COMMENT 'Уникальный идентификатор ответа в LimeSurvey', `start_dt` DateTime COMMENT 'Момент начала заполнения анкеты', `submit_dt` Nullable(DateTime) COMMENT 'Момент окончания заполнения анкеты', `send_dt` DateTime COMMENT 'Момент отправки заполненной анкеты', `email` Nullable(String) COMMENT 'Адрес электронной почты респондента', `flight_number` LowCardinality(Nullable(String)) COMMENT 'Номер рейса, указанный респондентом (например: UT-123)', `flight_date` Date COMMENT 'Дата рейса (преобразована из строкового формата источника), Null значения заменены на 1970-01-01', `seat` LowCardinality(Nullable(String)) COMMENT 'Номер места на борту', `ticket` Nullable(String) COMMENT 'Номер авиабилета пассажира', `departure_airport` LowCardinality(Nullable(String)) COMMENT 'Город аэропорта вылета', `arrival_airport` LowCardinality(Nullable(String)) COMMENT 'Город аэропорта прилета', `gender` LowCardinality(Nullable(String)) COMMENT 'Пол респондента', `birthday` LowCardinality(Nullable(String)) COMMENT 'Дата рождения в строковом формате источника', `flight_rating` Nullable(Int8) COMMENT 'Общая оценка перелёта (0–5)', `website_rating` Nullable(Int8) COMMENT 'Оценка веб-сайта Utair (0–5)', `airport_rating` Nullable(Int8) COMMENT 'Оценка сервиса в аэропорту (0–5)', `board_rating` Nullable(Int8) COMMENT 'Оценка сервиса на борту (0–5)', `is_ticket_purchase` UInt8 COMMENT 'Покупал билет через сайт: 1 — да, 0 — нет', `is_addon_services` UInt8 COMMENT 'Оформлял дополнительные услуги через сайт: 1 — да, 0 — нет', `is_checkin` UInt8 COMMENT 'Проходил регистрацию на рейс через сайт: 1 — да, 0 — нет', `other1` Nullable(String) COMMENT 'Свободный комментарий о действиях на сайте и в приложении' CODEC(ZSTD(3)), `is_with_ticket` UInt8 COMMENT 'Доп. услуга оформлена при покупке билета: 1 — да, 0 — нет', `is_after_purchase` UInt8 COMMENT 'Доп. услуга оформлена после покупки билета: 1 — да, 0 — нет', `is_online_checkin` UInt8 COMMENT 'Доп. услуга оформлена на онлайн-регистрации: 1 — да, 0 — нет', `is_at_airport` UInt8 COMMENT 'Доп. услуга оформлена в аэропорту: 1 — да, 0 — нет', `is_convenience_1` UInt8 COMMENT 'Удобство использования 1: 1 — отмечено, 0 — нет', `is_info_completeness_1` UInt8 COMMENT 'Полнота информации 1: 1 — отмечено, 0 — нет', `is_miles_promo_use_1` UInt8 COMMENT 'Возможность применить мили/промокод 1: 1 — отмечено, 0 — нет', `is_convenience_2` UInt8 COMMENT 'Удобство использования 2: 1 — отмечено, 0 — нет', `is_info_completeness_2` UInt8 COMMENT 'Полнота информации 2: 1 — отмечено, 0 — нет', `is_miles_promo_use_2` UInt8 COMMENT 'Возможность применить мили/промокод 2: 1 — отмечено, 0 — нет', `is_convenience_3` UInt8 COMMENT 'Удобство использования 3: 1 — отмечено, 0 — нет', `is_info_completeness_3` UInt8 COMMENT 'Полнота информации 3: 1 — отмечено, 0 — нет', `is_miles_promo_use_3` UInt8 COMMENT 'Возможность применить мили/промокод 3: 1 — отмечено, 0 — нет', `is_self_checkin` UInt8 COMMENT 'Прошёл самостоятельную онлайн-регистрацию: 1 — да, 0 — нет', `is_counter_checkin` UInt8 COMMENT 'Прошёл регистрацию на стойке в аэропорту: 1 — да, 0 — нет', `is_boarding_area` UInt8 COMMENT 'Воспользовался зоной выхода на посадку: 1 — да, 0 — нет', `other2` Nullable(String) COMMENT 'Свободный комментарий о сервисе в аэропорте' CODEC(ZSTD(3)), `is_print_bp_self` UInt8 COMMENT 'Распечатал посадочный талон самостоятельно: 1 — да, 0 — нет', `is_mobile_bp_scan` UInt8 COMMENT 'Прошёл досмотр с экрана мобильного устройства: 1 — да, 0 — нет', `is_no_options` UInt8 COMMENT 'Не воспользовался ни одной из перечисленных возможностей: 1 — да, 0 — нет', `is_staff_communication` UInt8 COMMENT 'Отметил коммуникацию и доброжелательность персонала аэропорта: 1 — да, 0 — нет', `is_baggage_rules_clarity` UInt8 COMMENT 'Отметил понятность правил провоза багажа: 1 — да, 0 — нет', `is_addon_service_speed` UInt8 COMMENT 'Отметил скорость и удобство оформления дополнительных услуг: 1 — да, 0 — нет', `is_gate_info` UInt8 COMMENT 'Отметил информирование о номере выхода на посадку и времени посадки: 1 — да, 0 — нет', `is_display_info_quality` UInt8 COMMENT 'Отметил актуальность и наглядность информации на табло: 1 — да, 0 — нет', `is_cabin_condition` UInt8 COMMENT 'Отметил состояние салона самолёта: 1 — да, 0 — нет', `is_onboard_info` UInt8 COMMENT 'Отметил качество информирования пассажиров на борту: 1 — да, 0 — нет', `is_cabin_crew` UInt8 COMMENT 'Отметил работу бортпроводников: 1 — да, 0 — нет', `other3` Nullable(String) COMMENT 'Свободный комментарий о качестве сервиса на борту' CODEC(ZSTD(3)), `is_seat_equipment_condition` UInt8 COMMENT 'Отметил исправность кресел, подлокотников, столиков и освещения: 1 — да, 0 — нет', `is_cleanliness` UInt8 COMMENT 'Отметил чистоту салона: 1 — да, 0 — нет', `is_captain_announcements` UInt8 COMMENT 'Отметил объявления командира воздушного судна о полёте: 1 — да, 0 — нет', `is_pa_clarity` UInt8 COMMENT 'Отметил громкость и чёткость объявлений бортпроводников: 1 — да, 0 — нет', `is_info_clarity` UInt8 COMMENT 'Отметил понятность информации о правилах поведения на борту: 1 — да, 0 — нет', `is_service_info` UInt8 COMMENT 'Отметил информирование о доступных услугах (бесплатных и платных): 1 — да, 0 — нет', `is_responsiveness` UInt8 COMMENT 'Отметил отзывчивость бортпроводников: 1 — да, 0 — нет', `is_politeness` UInt8 COMMENT 'Отметил вежливость бортпроводников: 1 — да, 0 — нет', `is_issue_handling` UInt8 COMMENT 'Отметил реагирование бортпроводников на нестандартные ситуации: 1 — да, 0 — нет', `open_feedback` Nullable(String) COMMENT 'Развёрнутый отзыв пассажира о перелёте в свободной форме' CODEC(ZSTD(3)), `processing_dt` DateTime COMMENT 'Дата и время обработки записи в DAG (UTC)', `meta_source` LowCardinality(String) COMMENT 'Источник. Служебное поле.', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Момент загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/lime_survey_answers_after_flight', '{replica}') ORDER BY (flight_date, send_dt, id) SETTINGS index_granularity = 8192 COMMENT 'Аналитическая таблица с ответами на опросы LimeSurvey. Оценки — Int8 0–5, флаги — Int8 1/0. Наполняется через MV.'
```
</details>

### `analytics`.`lime_survey_answers_after_flight_mv`
- Engine: MaterializedView
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: None

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id | UInt32 |  | 0 | 0 | 0 |  |
| start_dt | DateTime |  | 0 | 0 | 0 |  |
| submit_dt | Nullable(DateTime) |  | 0 | 0 | 0 |  |
| send_dt | DateTime |  | 0 | 0 | 0 |  |
| email | Nullable(String) |  | 0 | 0 | 0 |  |
| flight_number | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| flight_date | Date |  | 0 | 0 | 0 |  |
| seat | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| ticket | Nullable(String) |  | 0 | 0 | 0 |  |
| departure_airport | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| arrival_airport | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| gender | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| birthday | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| flight_rating | LowCardinality(Nullable(Int8)) |  | 0 | 0 | 0 |  |
| website_rating | Nullable(Int16) |  | 0 | 0 | 0 |  |
| airport_rating | Nullable(Int16) |  | 0 | 0 | 0 |  |
| board_rating | Nullable(Int16) |  | 0 | 0 | 0 |  |
| is_ticket_purchase | UInt8 |  | 0 | 0 | 0 |  |
| is_addon_services | UInt8 |  | 0 | 0 | 0 |  |
| is_checkin | UInt8 |  | 0 | 0 | 0 |  |
| other1 | Nullable(String) |  | 0 | 0 | 0 |  |
| is_with_ticket | UInt8 |  | 0 | 0 | 0 |  |
| is_after_purchase | UInt8 |  | 0 | 0 | 0 |  |
| is_online_checkin | UInt8 |  | 0 | 0 | 0 |  |
| is_at_airport | UInt8 |  | 0 | 0 | 0 |  |
| is_convenience_1 | UInt8 |  | 0 | 0 | 0 |  |
| is_info_completeness_1 | UInt8 |  | 0 | 0 | 0 |  |
| is_miles_promo_use_1 | UInt8 |  | 0 | 0 | 0 |  |
| is_convenience_2 | UInt8 |  | 0 | 0 | 0 |  |
| is_info_completeness_2 | UInt8 |  | 0 | 0 | 0 |  |
| is_miles_promo_use_2 | UInt8 |  | 0 | 0 | 0 |  |
| is_convenience_3 | UInt8 |  | 0 | 0 | 0 |  |
| is_info_completeness_3 | UInt8 |  | 0 | 0 | 0 |  |
| is_miles_promo_use_3 | UInt8 |  | 0 | 0 | 0 |  |
| is_self_checkin | UInt8 |  | 0 | 0 | 0 |  |
| is_counter_checkin | UInt8 |  | 0 | 0 | 0 |  |
| is_boarding_area | UInt8 |  | 0 | 0 | 0 |  |
| other2 | Nullable(String) |  | 0 | 0 | 0 |  |
| is_print_bp_self | UInt8 |  | 0 | 0 | 0 |  |
| is_mobile_bp_scan | UInt8 |  | 0 | 0 | 0 |  |
| is_no_options | UInt8 |  | 0 | 0 | 0 |  |
| is_staff_communication | UInt8 |  | 0 | 0 | 0 |  |
| is_baggage_rules_clarity | UInt8 |  | 0 | 0 | 0 |  |
| is_addon_service_speed | UInt8 |  | 0 | 0 | 0 |  |
| is_gate_info | UInt8 |  | 0 | 0 | 0 |  |
| is_display_info_quality | UInt8 |  | 0 | 0 | 0 |  |
| is_cabin_condition | UInt8 |  | 0 | 0 | 0 |  |
| is_onboard_info | UInt8 |  | 0 | 0 | 0 |  |
| is_cabin_crew | UInt8 |  | 0 | 0 | 0 |  |
| other3 | Nullable(String) |  | 0 | 0 | 0 |  |
| is_seat_equipment_condition | UInt8 |  | 0 | 0 | 0 |  |
| is_cleanliness | UInt8 |  | 0 | 0 | 0 |  |
| is_captain_announcements | UInt8 |  | 0 | 0 | 0 |  |
| is_pa_clarity | UInt8 |  | 0 | 0 | 0 |  |
| is_info_clarity | UInt8 |  | 0 | 0 | 0 |  |
| is_service_info | UInt8 |  | 0 | 0 | 0 |  |
| is_responsiveness | UInt8 |  | 0 | 0 | 0 |  |
| is_politeness | UInt8 |  | 0 | 0 | 0 |  |
| is_issue_handling | UInt8 |  | 0 | 0 | 0 |  |
| open_feedback | Nullable(String) |  | 0 | 0 | 0 |  |
| processing_dt | DateTime |  | 0 | 0 | 0 |  |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE MATERIALIZED VIEW analytics.lime_survey_answers_after_flight_mv TO analytics.lime_survey_answers_after_flight (`id` UInt32, `start_dt` DateTime, `submit_dt` Nullable(DateTime), `send_dt` DateTime, `email` Nullable(String), `flight_number` LowCardinality(Nullable(String)), `flight_date` Date, `seat` LowCardinality(Nullable(String)), `ticket` Nullable(String), `departure_airport` LowCardinality(Nullable(String)), `arrival_airport` LowCardinality(Nullable(String)), `gender` LowCardinality(Nullable(String)), `birthday` LowCardinality(Nullable(String)), `flight_rating` LowCardinality(Nullable(Int8)), `website_rating` Nullable(Int16), `airport_rating` Nullable(Int16), `board_rating` Nullable(Int16), `is_ticket_purchase` UInt8, `is_addon_services` UInt8, `is_checkin` UInt8, `other1` Nullable(String), `is_with_ticket` UInt8, `is_after_purchase` UInt8, `is_online_checkin` UInt8, `is_at_airport` UInt8, `is_convenience_1` UInt8, `is_info_completeness_1` UInt8, `is_miles_promo_use_1` UInt8, `is_convenience_2` UInt8, `is_info_completeness_2` UInt8, `is_miles_promo_use_2` UInt8, `is_convenience_3` UInt8, `is_info_completeness_3` UInt8, `is_miles_promo_use_3` UInt8, `is_self_checkin` UInt8, `is_counter_checkin` UInt8, `is_boarding_area` UInt8, `other2` Nullable(String), `is_print_bp_self` UInt8, `is_mobile_bp_scan` UInt8, `is_no_options` UInt8, `is_staff_communication` UInt8, `is_baggage_rules_clarity` UInt8, `is_addon_service_speed` UInt8, `is_gate_info` UInt8, `is_display_info_quality` UInt8, `is_cabin_condition` UInt8, `is_onboard_info` UInt8, `is_cabin_crew` UInt8, `other3` Nullable(String), `is_seat_equipment_condition` UInt8, `is_cleanliness` UInt8, `is_captain_announcements` UInt8, `is_pa_clarity` UInt8, `is_info_clarity` UInt8, `is_service_info` UInt8, `is_responsiveness` UInt8, `is_politeness` UInt8, `is_issue_handling` UInt8, `open_feedback` Nullable(String), `processing_dt` DateTime, `meta_source` LowCardinality(String)) AS SELECT id, start_dt AS start_dt, submit_dt AS submit_dt, datestamp AS send_dt, email, flight_number, coalesce(toDateOrNull(concat(extract(replaceRegexpAll(flight_date, '([а-яё])(\\d)', '\\1 \\2'), '(\\d{4})$'), '-', leftPad(toString(indexOf(['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'], extract(replaceRegexpAll(replaceRegexpAll(flight_date, '(\\d)([а-яё])', '\\1 \\2'), '([а-яё])(\\d)', '\\1 \\2'), '\\d+ (\\S+) \\d{4}'))), 2, '0'), '-', leftPad(extract(replaceRegexpAll(flight_date, '(\\d)([а-яё])', '\\1 \\2'), '^(\\d+)'), 2, '0'))), toDate('1970-01-01')) AS flight_date, seat, ticket, departure_airport, arrival_airport, gender, birthday, toInt8OrNull(flight_rating) AS flight_rating, if((flight_rating >= '5') AND (website_rating IS NULL), 5, toInt8OrNull(website_rating)) AS website_rating, if((flight_rating >= '5') AND (airport_rating IS NULL), 5, toInt8OrNull(airport_rating)) AS airport_rating, if((flight_rating >= '5') AND (board_rating IS NULL), 5, toInt8OrNull(board_rating)) AS board_rating, if(ticket_purchase LIKE 'Y', 1, 0) AS is_ticket_purchase, if(addon_services LIKE 'Y', 1, 0) AS is_addon_services, if(checkin LIKE 'Y', 1, 0) AS is_checkin, other1, if(with_ticket LIKE 'Y', 1, 0) AS is_with_ticket, if(after_purchase LIKE 'Y', 1, 0) AS is_after_purchase, if(online_checkin LIKE 'Y', 1, 0) AS is_online_checkin, if(at_airport LIKE 'Y', 1, 0) AS is_at_airport, if(convenience_1 LIKE 'Y', 1, 0) AS is_convenience_1, if(info_completeness_1 LIKE 'Y', 1, 0) AS is_info_completeness_1, if(miles_promo_use_1 LIKE 'Y', 1, 0) AS is_miles_promo_use_1, if(convenience_2 LIKE 'Y', 1, 0) AS is_convenience_2, if(info_completeness_2 LIKE 'Y', 1, 0) AS is_info_completeness_2, if(miles_promo_use_2 LIKE 'Y', 1, 0) AS is_miles_promo_use_2, if(convenience_3 LIKE 'Y', 1, 0) AS is_convenience_3, if(info_completeness_3 LIKE 'Y', 1, 0) AS is_info_completeness_3, if(miles_promo_use_3 LIKE 'Y', 1, 0) AS is_miles_promo_use_3, if(self_checkin LIKE 'Y', 1, 0) AS is_self_checkin, if(counter_checkin LIKE 'Y', 1, 0) AS is_counter_checkin, if(boarding_area LIKE 'Y', 1, 0) AS is_boarding_area, other2, if(print_bp_self LIKE 'Y', 1, 0) AS is_print_bp_self, if(mobile_bp_scan LIKE 'Y', 1, 0) AS is_mobile_bp_scan, if(no_options LIKE 'Y', 1, 0) AS is_no_options, if(staff_communication LIKE 'Y', 1, 0) AS is_staff_communication, if(baggage_rules_clarity LIKE 'Y', 1, 0) AS is_baggage_rules_clarity, if(addon_service_speed LIKE 'Y', 1, 0) AS is_addon_service_speed, if(gate_info LIKE 'Y', 1, 0) AS is_gate_info, if(display_info_quality LIKE 'Y', 1, 0) AS is_display_info_quality, if(cabin_condition LIKE 'Y', 1, 0) AS is_cabin_condition, if(onboard_info LIKE 'Y', 1, 0) AS is_onboard_info, if(cabin_crew LIKE 'Y', 1, 0) AS is_cabin_crew, other3, if(seat_equipment_condition LIKE 'Y', 1, 0) AS is_seat_equipment_condition, if(cleanliness LIKE 'Y', 1, 0) AS is_cleanliness, if(captain_announcements LIKE 'Y', 1, 0) AS is_captain_announcements, if(pa_clarity LIKE 'Y', 1, 0) AS is_pa_clarity, if(info_clarity LIKE 'Y', 1, 0) AS is_info_clarity, if(service_info LIKE 'Y', 1, 0) AS is_service_info, if(responsiveness LIKE 'Y', 1, 0) AS is_responsiveness, if(politeness LIKE 'Y', 1, 0) AS is_politeness, if(issue_handling LIKE 'Y', 1, 0) AS is_issue_handling, open_feedback, processing_dt, meta_source FROM source.lime_survey_answers_after_flight WHERE email IS NOT NULL
```
</details>

### `analytics`.`mongo_monoapp_sirena_flights`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: sirena_flights_object_id, processing_date, flight_number, departure_utc_plan_at
- Primary key: sirena_flights_object_id, processing_date, flight_number, departure_utc_plan_at
- Total rows: 129708
- Total bytes: 25753365

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| sirena_flights_object_id | String |  | 1 | 1 | 0 | Уникальный идентификатор документа (MongoDB _id) |
| point_id | Nullable(String) |  | 0 | 0 | 0 | Идентификатор рейса, уникальный в Астре |
| ak | LowCardinality(String) |  | 0 | 0 | 0 | Авиакомпания |
| flight_number | String |  | 1 | 1 | 0 | Номер рейса |
| aircraft_type | LowCardinality(String) |  | 0 | 0 | 0 | Тип воздушного судна |
| aircraft_bort | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Бортовой номер воздушного судна |
| departure_airport_code | LowCardinality(String) |  | 0 | 0 | 0 | IATA-код аэропорта вылета |
| departure_airport_name | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Название аэропорта вылета |
| departure_city_code | LowCardinality(String) |  | 0 | 0 | 0 | Код города вылета |
| departure_city_name | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Название города вылета |
| departure_utc_plan_at | DateTime64(3, 'UTC') |  | 1 | 1 | 0 | Плановое время вылета (UTC) |
| departure_utc_actual_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Фактическое время вылета (UTC) |
| departure_utc_estimate_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Оценочное время вылета (UTC) |
| departure_local_plan_at | Nullable(DateTime) |  | 0 | 0 | 0 | Плановое время вылета (локальное) |
| departure_local_actual_at | Nullable(DateTime) |  | 0 | 0 | 0 | Фактическое время вылета (локальное) |
| departure_local_estimate_at | Nullable(DateTime) |  | 0 | 0 | 0 | Оценочное время вылета (локальное) |
| arrival_airport_code | LowCardinality(String) |  | 0 | 0 | 0 | IATA-код аэропорта прилёта |
| arrival_airport_name | Nullable(String) |  | 0 | 0 | 0 | Название аэропорта прилёта |
| arrival_city_code | LowCardinality(String) |  | 0 | 0 | 0 | Код города прилёта |
| arrival_city_name | Nullable(String) |  | 0 | 0 | 0 | Название города прилёта |
| arrival_utc_plan_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Плановое время прилёта (UTC) |
| arrival_utc_actual_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Фактическое время прилёта (UTC) |
| arrival_utc_estimate_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Оценочное время прилёта (UTC) |
| arrival_local_plan_at | Nullable(DateTime) |  | 0 | 0 | 0 | Плановое время прилёта (локальное) |
| arrival_local_actual_at | Nullable(DateTime) |  | 0 | 0 | 0 | Фактическое время прилёта (локальное) |
| arrival_local_estimate_at | Nullable(DateTime) |  | 0 | 0 | 0 | Оценочное время прилёта (локальное) |
| canceled | Bool |  | 0 | 0 | 0 | Признак отмены рейса |
| stages_check_in_prepare_plan_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Плановое время подготовки регистрации |
| stages_check_in_prepare_actual_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Фактическое время подготовки регистрации |
| stages_check_in_open_plan_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Плановое время открытия регистрации |
| stages_check_in_open_actual_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Фактическое время открытия регистрации |
| stages_check_in_close_plan_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Плановое время закрытия регистрации |
| stages_check_in_close_actual_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Фактическое время закрытия регистрации |
| stages_check_in_web_open_plan_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Плановое время открытия онлайн-регистрации |
| stages_check_in_web_open_actual_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Фактическое время открытия онлайн-регистрации |
| stages_check_in_web_cancel_plan_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Плановое время отмены онлайн-регистрации |
| stages_check_in_web_cancel_actual_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Фактическое время отмены онлайн-регистрации |
| stages_check_in_web_close_plan_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Плановое время закрытия онлайн-регистрации |
| stages_check_in_web_close_actual_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Фактическое время закрытия онлайн-регистрации |
| stages_check_in_kiosk_open_plan_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Плановое время открытия само-регистрации |
| stages_check_in_kiosk_open_actual_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Фактическое время открытия само-регистрации |
| stages_check_in_kiosk_close_plan_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Плановое время закрытия само-регистрации |
| stages_check_in_kiosk_close_actual_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Фактическое время закрытия само-регистрации |
| stages_boarding_open_plan_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Плановое время начала посадки |
| stages_boarding_open_actual_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Фактическое время начала посадки |
| stages_boarding_close_plan_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Плановое время окончания посадки |
| stages_boarding_close_actual_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Фактическое время окончания посадки |
| delays | Array(String) |  | 0 | 0 | 0 | Список задержек рейса в виде JSON-объектов |
| events | Array(String) |  | 0 | 0 | 0 | События рейса в виде JSON-объектов |
| pos | Array(String) |  | 0 | 0 | 0 | Позиции рейса (POS) в виде JSON-объектов |
| gates | Array(String) |  | 0 | 0 | 0 | Гейты рейса в виде JSON-объектов |
| stations | Array(String) |  | 0 | 0 | 0 | Станции обслуживания рейса в виде JSON-объектов |
| astra_origin_time | Nullable(DateTime) |  | 0 | 0 | 0 | Время формирования данных в системе Astra |
| created_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Дата создания записи |
| updated_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Дата последнего обновления записи |
| processing_date | DateTime |  | 1 | 1 | 1 | Дата выполнения дага |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Технические метаданные (например, DAG или источник загрузки) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.mongo_monoapp_sirena_flights (`sirena_flights_object_id` String COMMENT 'Уникальный идентификатор документа (MongoDB _id)', `point_id` Nullable(String) COMMENT 'Идентификатор рейса, уникальный в Астре', `ak` LowCardinality(String) COMMENT 'Авиакомпания', `flight_number` String COMMENT 'Номер рейса', `aircraft_type` LowCardinality(String) COMMENT 'Тип воздушного судна', `aircraft_bort` LowCardinality(Nullable(String)) COMMENT 'Бортовой номер воздушного судна', `departure_airport_code` LowCardinality(String) COMMENT 'IATA-код аэропорта вылета', `departure_airport_name` LowCardinality(Nullable(String)) COMMENT 'Название аэропорта вылета', `departure_city_code` LowCardinality(String) COMMENT 'Код города вылета', `departure_city_name` LowCardinality(Nullable(String)) COMMENT 'Название города вылета', `departure_utc_plan_at` DateTime64(3, 'UTC') COMMENT 'Плановое время вылета (UTC)', `departure_utc_actual_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Фактическое время вылета (UTC)', `departure_utc_estimate_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Оценочное время вылета (UTC)', `departure_local_plan_at` Nullable(DateTime) COMMENT 'Плановое время вылета (локальное)', `departure_local_actual_at` Nullable(DateTime) COMMENT 'Фактическое время вылета (локальное)', `departure_local_estimate_at` Nullable(DateTime) COMMENT 'Оценочное время вылета (локальное)', `arrival_airport_code` LowCardinality(String) COMMENT 'IATA-код аэропорта прилёта', `arrival_airport_name` Nullable(String) COMMENT 'Название аэропорта прилёта', `arrival_city_code` LowCardinality(String) COMMENT 'Код города прилёта', `arrival_city_name` Nullable(String) COMMENT 'Название города прилёта', `arrival_utc_plan_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Плановое время прилёта (UTC)', `arrival_utc_actual_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Фактическое время прилёта (UTC)', `arrival_utc_estimate_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Оценочное время прилёта (UTC)', `arrival_local_plan_at` Nullable(DateTime) COMMENT 'Плановое время прилёта (локальное)', `arrival_local_actual_at` Nullable(DateTime) COMMENT 'Фактическое время прилёта (локальное)', `arrival_local_estimate_at` Nullable(DateTime) COMMENT 'Оценочное время прилёта (локальное)', `canceled` Bool COMMENT 'Признак отмены рейса', `stages_check_in_prepare_plan_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Плановое время подготовки регистрации', `stages_check_in_prepare_actual_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Фактическое время подготовки регистрации', `stages_check_in_open_plan_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Плановое время открытия регистрации', `stages_check_in_open_actual_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Фактическое время открытия регистрации', `stages_check_in_close_plan_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Плановое время закрытия регистрации', `stages_check_in_close_actual_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Фактическое время закрытия регистрации', `stages_check_in_web_open_plan_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Плановое время открытия онлайн-регистрации', `stages_check_in_web_open_actual_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Фактическое время открытия онлайн-регистрации', `stages_check_in_web_cancel_plan_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Плановое время отмены онлайн-регистрации', `stages_check_in_web_cancel_actual_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Фактическое время отмены онлайн-регистрации', `stages_check_in_web_close_plan_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Плановое время закрытия онлайн-регистрации', `stages_check_in_web_close_actual_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Фактическое время закрытия онлайн-регистрации', `stages_check_in_kiosk_open_plan_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Плановое время открытия само-регистрации', `stages_check_in_kiosk_open_actual_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Фактическое время открытия само-регистрации', `stages_check_in_kiosk_close_plan_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Плановое время закрытия само-регистрации', `stages_check_in_kiosk_close_actual_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Фактическое время закрытия само-регистрации', `stages_boarding_open_plan_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Плановое время начала посадки', `stages_boarding_open_actual_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Фактическое время начала посадки', `stages_boarding_close_plan_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Плановое время окончания посадки', `stages_boarding_close_actual_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Фактическое время окончания посадки', `delays` Array(String) COMMENT 'Список задержек рейса в виде JSON-объектов', `events` Array(String) COMMENT 'События рейса в виде JSON-объектов', `pos` Array(String) COMMENT 'Позиции рейса (POS) в виде JSON-объектов', `gates` Array(String) COMMENT 'Гейты рейса в виде JSON-объектов', `stations` Array(String) COMMENT 'Станции обслуживания рейса в виде JSON-объектов', `astra_origin_time` Nullable(DateTime) COMMENT 'Время формирования данных в системе Astra', `created_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Дата создания записи', `updated_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Дата последнего обновления записи', `processing_date` DateTime COMMENT 'Дата выполнения дага', `meta_source` LowCardinality(String) COMMENT 'Технические метаданные (например, DAG или источник загрузки)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/mongo_monoapp_sirena_flights', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (sirena_flights_object_id, processing_date, flight_number, departure_utc_plan_at) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения данных о полетах полученных из Sirena_flights'
```
</details>

### `analytics`.`mongo_monoapp_sirena_flights_passenger_remarks`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: sirena_flights_object_id, passengers_id, processing_date
- Primary key: sirena_flights_object_id, passengers_id, processing_date
- Total rows: 50564336
- Total bytes: 796326450

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| sirena_flights_object_id | String |  | 1 | 1 | 0 | ID документа из JSON (_id) |
| passengers_id | String |  | 1 | 1 | 0 | ID пассажира (p.1) |
| passenger_status | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Статус пассажира в рейсе |
| remark_idx | UInt8 |  | 0 | 0 | 0 | id услуги пассажира |
| remarks_code | Nullable(String) |  | 0 | 0 | 0 | Код примечания (JSON поле code) |
| remarks_text | Nullable(String) |  | 0 | 0 | 0 | Текст примечания (JSON поле text) |
| passenger_created_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Дата регистрации пассажира |
| passenger_updated_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Дата обновления записи пассажира |
| updated_at | DateTime64(3, 'UTC') |  | 0 | 0 | 0 | Дата обновления рейса |
| processing_date | DateTime |  | 1 | 1 | 1 | Дата выполнения дага |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Технические метаданные (например, DAG или источник загрузки) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.mongo_monoapp_sirena_flights_passenger_remarks (`sirena_flights_object_id` String COMMENT 'ID документа из JSON (_id)', `passengers_id` String COMMENT 'ID пассажира (p.1)', `passenger_status` LowCardinality(Nullable(String)) COMMENT 'Статус пассажира в рейсе', `remark_idx` UInt8 COMMENT 'id услуги пассажира', `remarks_code` Nullable(String) COMMENT 'Код примечания (JSON поле code)', `remarks_text` Nullable(String) COMMENT 'Текст примечания (JSON поле text)', `passenger_created_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Дата регистрации пассажира', `passenger_updated_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Дата обновления записи пассажира', `updated_at` DateTime64(3, 'UTC') COMMENT 'Дата обновления рейса', `processing_date` DateTime COMMENT 'Дата выполнения дага', `meta_source` LowCardinality(String) COMMENT 'Технические метаданные (например, DAG или источник загрузки)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/mongo_monoapp_sirena_flights_passenger_remarks', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (sirena_flights_object_id, passengers_id, processing_date) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения данных о ремарках пассажиров полученных из Sirena_flights'
```
</details>

### `analytics`.`mongo_monoapp_sirena_flights_passenger_services`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: sirena_flights_object_id, passengers_id, processing_date
- Primary key: sirena_flights_object_id, passengers_id, processing_date
- Total rows: 15756311
- Total bytes: 331167636

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| sirena_flights_object_id | String |  | 1 | 1 | 0 | ID документа из JSON (_id) |
| passengers_id | String |  | 1 | 1 | 0 | ID пассажира (p.1) |
| passenger_status | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Статус пассажира в рейсе |
| service_idx | UInt16 |  | 0 | 0 | 0 | id услуги пассажира |
| description | Nullable(String) |  | 0 | 0 | 0 | Описание примечания (JSON поле desc) |
| number | Nullable(String) |  | 0 | 0 | 0 | Номер примечания (JSON поле num) |
| rfic | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | RFIC (JSON поле rfic) |
| rfisc | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | RFISC (JSON поле rfisc) |
| status | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Статус примечания (JSON поле status) |
| passenger_created_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Дата регистрации пассажира |
| passenger_updated_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Дата обновления записи пассажира |
| updated_at | DateTime64(3, 'UTC') |  | 0 | 0 | 0 | Дата обновления рейса |
| processing_date | DateTime |  | 1 | 1 | 1 | Дата выполнения дага |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Технические метаданные (например, DAG или источник загрузки) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.mongo_monoapp_sirena_flights_passenger_services (`sirena_flights_object_id` String COMMENT 'ID документа из JSON (_id)', `passengers_id` String COMMENT 'ID пассажира (p.1)', `passenger_status` LowCardinality(Nullable(String)) COMMENT 'Статус пассажира в рейсе', `service_idx` UInt16 COMMENT 'id услуги пассажира', `description` Nullable(String) COMMENT 'Описание примечания (JSON поле desc)', `number` Nullable(String) COMMENT 'Номер примечания (JSON поле num)', `rfic` LowCardinality(Nullable(String)) COMMENT 'RFIC (JSON поле rfic)', `rfisc` LowCardinality(Nullable(String)) COMMENT 'RFISC (JSON поле rfisc)', `status` LowCardinality(Nullable(String)) COMMENT 'Статус примечания (JSON поле status)', `passenger_created_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Дата регистрации пассажира', `passenger_updated_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Дата обновления записи пассажира', `updated_at` DateTime64(3, 'UTC') COMMENT 'Дата обновления рейса', `processing_date` DateTime COMMENT 'Дата выполнения дага', `meta_source` LowCardinality(String) COMMENT 'Технические метаданные (например, DAG или источник загрузки)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/mongo_monoapp_sirena_flights_passenger_services', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (sirena_flights_object_id, passengers_id, processing_date) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения данных о услугах пассажиров, полученных из Sirena_flights'
```
</details>

### `analytics`.`mongo_monoapp_sirena_flights_passengers`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: sirena_flights_object_id, passengers_id, processing_date
- Primary key: sirena_flights_object_id, passengers_id, processing_date
- Total rows: 16508541
- Total bytes: 934172403

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| sirena_flights_object_id | String |  | 1 | 1 | 0 | ID документа рейса (MongoDB _id) |
| passengers_id | String |  | 1 | 1 | 0 | Идентификатор пассажира |
| name | Nullable(String) |  | 0 | 0 | 0 | Имя пассажира |
| class | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Класс обслуживания |
| subclass | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Подкласс обслуживания |
| pass_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Тип пассажира (ADULT, CHILD, INFANT и т.д.) |
| gender | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Пол пассажира |
| seat | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Место пассажира |
| seats_count | Nullable(Int32) |  | 0 | 0 | 0 | Количество мест, занимаемых пассажиром |
| excess | Nullable(Int32) |  | 0 | 0 | 0 | Платный багаж |
| hand_baggage_amount | Nullable(Int32) |  | 0 | 0 | 0 | Количество мест ручной клади |
| hand_baggage_weight | Nullable(Float32) |  | 0 | 0 | 0 | Вес ручной клади |
| baggege_amount | Nullable(Int32) |  | 0 | 0 | 0 | Количество сдаваемого багажа |
| baggage_weight | Nullable(Float32) |  | 0 | 0 | 0 | Вес сдаваемого багажа |
| status | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Статус пассажира (delete, uncheckin, checkin, boarded) |
| check_in_type | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Тип регистрации (WEB, KIOSK, MOBIL, TERM) |
| ticket_number | Nullable(String) |  | 0 | 0 | 0 | Номер билета |
| ticket_coupon | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Купон билета |
| ticket_confirm | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Подтверждение (активный билет) |
| ticket_rem | Nullable(String) |  | 0 | 0 | 0 | Ремарка, означающая, что пассажир с электронным билетом |
| rloc | Array(String) |  | 0 | 0 | 0 | Номер брони [PNR GDS, PNR] |
| excess_wt | Nullable(Float32) |  | 0 | 0 | 0 | Вес превышения |
| excess_pc | Nullable(Float32) |  | 0 | 0 | 0 | Вес превышения |
| passengers_astra_origin_time | Nullable(String) |  | 0 | 0 | 0 | Время формирования данных в Astra для пассажира |
| brand | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Тариф билета |
| card_number | Nullable(String) |  | 0 | 0 | 0 | Номер бонусной карты пассажира |
| tier_level | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Уровень программы лояльности |
| jmp | Bool |  | 0 | 0 | 0 | Статус что Пассажир, зарегистрирован на джамп-сит |
| passenger_created_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Дата регистрации пассажира |
| passenger_updated_at | Nullable(DateTime64(3, 'UTC')) |  | 0 | 0 | 0 | Дата обновления записи пассажира |
| updated_at | DateTime64(3, 'UTC') |  | 0 | 0 | 0 | Дата обновления рейса |
| processing_date | DateTime |  | 1 | 1 | 1 | Дата выполнения дага |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Технические метаданные (DAG) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.mongo_monoapp_sirena_flights_passengers (`sirena_flights_object_id` String COMMENT 'ID документа рейса (MongoDB _id)', `passengers_id` String COMMENT 'Идентификатор пассажира', `name` Nullable(String) COMMENT 'Имя пассажира', `class` LowCardinality(Nullable(String)) COMMENT 'Класс обслуживания', `subclass` LowCardinality(Nullable(String)) COMMENT 'Подкласс обслуживания', `pass_type` LowCardinality(Nullable(String)) COMMENT 'Тип пассажира (ADULT, CHILD, INFANT и т.д.)', `gender` LowCardinality(Nullable(String)) COMMENT 'Пол пассажира', `seat` LowCardinality(Nullable(String)) COMMENT 'Место пассажира', `seats_count` Nullable(Int32) COMMENT 'Количество мест, занимаемых пассажиром', `excess` Nullable(Int32) COMMENT 'Платный багаж', `hand_baggage_amount` Nullable(Int32) COMMENT 'Количество мест ручной клади', `hand_baggage_weight` Nullable(Float32) COMMENT 'Вес ручной клади', `baggege_amount` Nullable(Int32) COMMENT 'Количество сдаваемого багажа', `baggage_weight` Nullable(Float32) COMMENT 'Вес сдаваемого багажа', `status` LowCardinality(Nullable(String)) COMMENT 'Статус пассажира (delete, uncheckin, checkin, boarded)', `check_in_type` LowCardinality(Nullable(String)) COMMENT 'Тип регистрации (WEB, KIOSK, MOBIL, TERM)', `ticket_number` Nullable(String) COMMENT 'Номер билета', `ticket_coupon` LowCardinality(Nullable(String)) COMMENT 'Купон билета', `ticket_confirm` LowCardinality(Nullable(String)) COMMENT 'Подтверждение (активный билет)', `ticket_rem` Nullable(String) COMMENT 'Ремарка, означающая, что пассажир с электронным билетом', `rloc` Array(String) COMMENT 'Номер брони [PNR GDS, PNR]', `excess_wt` Nullable(Float32) COMMENT 'Вес превышения', `excess_pc` Nullable(Float32) COMMENT 'Вес превышения', `passengers_astra_origin_time` Nullable(String) COMMENT 'Время формирования данных в Astra для пассажира', `brand` LowCardinality(Nullable(String)) COMMENT 'Тариф билета', `card_number` Nullable(String) COMMENT 'Номер бонусной карты пассажира', `tier_level` LowCardinality(Nullable(String)) COMMENT 'Уровень программы лояльности', `jmp` Bool COMMENT 'Статус что Пассажир, зарегистрирован на джамп-сит', `passenger_created_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Дата регистрации пассажира', `passenger_updated_at` Nullable(DateTime64(3, 'UTC')) COMMENT 'Дата обновления записи пассажира', `updated_at` DateTime64(3, 'UTC') COMMENT 'Дата обновления рейса', `processing_date` DateTime COMMENT 'Дата выполнения дага', `meta_source` LowCardinality(String) COMMENT 'Технические метаданные (DAG)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/mongo_monoapp_sirena_flights_passengers', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (sirena_flights_object_id, passengers_id, processing_date) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения данных о пассажирах, полученных из Sirena_flights'
```
</details>

### `analytics`.`smsc_messages_with_organization`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_date)
- Sorting key: processing_date, send_date, reseller_login
- Primary key: processing_date, send_date, reseller_login
- Total rows: 1971940
- Total bytes: 106280617

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| id | String |  | 0 | 0 | 0 | Уникальный идентификатор сообщения |
| int_id | String |  | 0 | 0 | 0 | Внешний идентификатор сообщения |
| last_date | DateTime |  | 0 | 0 | 0 | Дата и время последнего обновления |
| last_timestamp | UInt32 |  | 0 | 0 | 0 | Timestamp последнего обновления |
| send_date | DateTime |  | 1 | 1 | 0 | Дата и время отправки |
| send_timestamp | UInt32 |  | 0 | 0 | 0 | Timestamp отправки |
| processing_date | Date |  | 1 | 1 | 1 | Дата обработки сообщения |
| phone | String |  | 0 | 0 | 0 | Номер телефона получателя |
| sender_id | String |  | 0 | 0 | 0 | Идентификатор отправителя |
| reseller_login | String |  | 1 | 1 | 0 | Логин реселлера |
| mccmnc | String |  | 0 | 0 | 0 | MCC и MNC код оператора |
| country | String |  | 0 | 0 | 0 | Страна получателя |
| operator | String |  | 0 | 0 | 0 | Нормализованное название оператора |
| operator_orig | String |  | 0 | 0 | 0 | Оригинальное название оператора |
| region | String |  | 0 | 0 | 0 | Регион получателя |
| status | UInt16 |  | 0 | 0 | 0 | Статус доставки |
| status_name | String |  | 0 | 0 | 0 | Название статуса |
| flag | UInt16 |  | 0 | 0 | 0 | Флаг сообщения |
| type | UInt8 |  | 0 | 0 | 0 | Тип сообщения |
| format | UInt8 |  | 0 | 0 | 0 | Формат сообщения (0-текст, 1-unicode и т.д.) |
| err | UInt64 |  | 0 | 0 | 0 | Код ошибки (если есть) |
| message | String |  | 0 | 0 | 0 | Текст сообщения |
| sms_cnt | Nullable(UInt8) |  | 0 | 0 | 0 | Количество SMS частей |
| cost | Decimal(10, 3) |  | 0 | 0 | 0 | Стоимость сообщения |
| crc | UInt32 |  | 0 | 0 | 0 | Контрольная сумма |
| organization | String |  | 0 | 0 | 0 | Наименование ораганизации |
| organization_id | String |  | 0 | 0 | 0 | Внутренний индетификатор ораганизации |
| comment | String | '' | 0 | 0 | 0 | Комментарий к сообщению |
| send_retry | UInt8 | 0 | 0 | 0 | 0 |  |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.smsc_messages_with_organization (`id` String COMMENT 'Уникальный идентификатор сообщения', `int_id` String COMMENT 'Внешний идентификатор сообщения', `last_date` DateTime COMMENT 'Дата и время последнего обновления', `last_timestamp` UInt32 COMMENT 'Timestamp последнего обновления', `send_date` DateTime COMMENT 'Дата и время отправки', `send_timestamp` UInt32 COMMENT 'Timestamp отправки', `processing_date` Date COMMENT 'Дата обработки сообщения', `phone` String COMMENT 'Номер телефона получателя', `sender_id` String COMMENT 'Идентификатор отправителя', `reseller_login` String COMMENT 'Логин реселлера', `mccmnc` String COMMENT 'MCC и MNC код оператора', `country` String COMMENT 'Страна получателя', `operator` String COMMENT 'Нормализованное название оператора', `operator_orig` String COMMENT 'Оригинальное название оператора', `region` String COMMENT 'Регион получателя', `status` UInt16 COMMENT 'Статус доставки', `status_name` String COMMENT 'Название статуса', `flag` UInt16 COMMENT 'Флаг сообщения', `type` UInt8 COMMENT 'Тип сообщения', `format` UInt8 COMMENT 'Формат сообщения (0-текст, 1-unicode и т.д.)', `err` UInt64 COMMENT 'Код ошибки (если есть)', `message` String COMMENT 'Текст сообщения', `sms_cnt` Nullable(UInt8) COMMENT 'Количество SMS частей', `cost` Decimal(10, 3) COMMENT 'Стоимость сообщения', `crc` UInt32 COMMENT 'Контрольная сумма', `organization` String COMMENT 'Наименование ораганизации', `organization_id` String COMMENT 'Внутренний индетификатор ораганизации', `comment` String DEFAULT '' COMMENT 'Комментарий к сообщению', `send_retry` UInt8 DEFAULT 0) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/analytics/smsc_messages_with_organization', '{replica}') PARTITION BY toYYYYMM(processing_date) ORDER BY (processing_date, send_date, reseller_login) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения SMS-сообщений из SMSC с добавлением организаций'
```
</details>

### `analytics`.`sofi_bi_sales_ut`
- Engine: ReplicatedReplacingMergeTree
- Partition key: toYYYYMM(transaction_date)
- Sorting key: document_number, coupon_number, document_kind, transaction_date
- Primary key: document_number, coupon_number, document_kind, transaction_date
- Total rows: 7984628
- Total bytes: 582560090

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| document_number | String |  | 1 | 1 | 0 | Номер документа (бланка) |
| coupon_number | UInt8 |  | 1 | 1 | 0 | Номер купона |
| document_kind | LowCardinality(String) |  | 1 | 1 | 0 | Вид операции (бланка) |
| carrier | LowCardinality(String) |  | 0 | 0 | 0 | Перевозчик |
| flight_number | LowCardinality(String) |  | 0 | 0 | 0 | Номер рейса; "Отсутствует" если не задан в источнике |
| origin | LowCardinality(String) |  | 0 | 0 | 0 | Пункт вылета; "Отсутствует" если не задан в источнике |
| destination | LowCardinality(String) |  | 0 | 0 | 0 | Пункт прилета; "Отсутствует" если не задан в источнике |
| agency_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Код агента |
| agency_name | LowCardinality(String) |  | 0 | 0 | 0 | Наименование агента |
| agency_number_code | String |  | 0 | 0 | 0 | Пункт продажи (валидатор) |
| document_coupon_count | UInt8 |  | 0 | 0 | 0 | Количество купонов документа |
| sale_segment_count | Int8 |  | 0 | 0 | 0 | Количество проданных сегментов |
| service_class | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Класс бронирования |
| fare_basis | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Вид тарифа |
| tax_rub_cpfp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Тариф на купоне РУБ |
| tax_rub_yq_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Топливный сбор РУБ |
| tax_rub_zz_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Такса ТКП (ZZ) РУБ |
| tax_rub_cp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Штраф за возврат(обмен) (CP) РУБ |
| tax_rub_ru_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор RU РУБ |
| tax_rub_psyr_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор за бланк (PS,YR) РУБ |
| tax_rub_another_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Прочие сборы РУБ |
| currency | LowCardinality(String) |  | 0 | 0 | 0 | Валюта |
| tax_cpfp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Тариф на купоне |
| tax_yq_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Топливный сбор |
| tax_zz_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Такса ТКП (ZZ) |
| tax_cp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Штраф за возврат(обмен) (CP) |
| tax_ru_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор RU |
| tax_psyr_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор за бланк (PS,YR) |
| tax_another_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Прочие сборы |
| transaction_date | Date |  | 1 | 1 | 1 | Дата транзакции |
| flight_date | Date |  | 0 | 0 | 0 | Дата вылета; 1970-01-01 если не задана в источнике |
| gds | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Система бронирования |
| sale_system | LowCardinality(String) |  | 0 | 0 | 0 | Система продажи |
| ff_card_passenger | Nullable(String) |  | 0 | 0 | 0 | Номер карты частолетающего пассажира |
| ff_amount | Int32 |  | 0 | 0 | 0 | Сумма бонусов на купоне (эквив. РУБ) |
| is_exchange | Nullable(Bool) |  | 0 | 0 | 0 | Участвует в обмене? |
| exchange_document_number | Nullable(String) |  | 0 | 0 | 0 | Номер документа (обмен) |
| supplement_currency | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Валюта доплаты |
| supplement_amount | Nullable(Int32) |  | 0 | 0 | 0 | Сумма доплаты |
| supplement_amount_rub | Nullable(Int32) |  | 0 | 0 | 0 | Сумма доплаты РУБ |
| fop_string | String |  | 0 | 0 | 0 | Примечание по ФОП |
| is_rt | Nullable(Bool) |  | 0 | 0 | 0 | Перевозка туда-обратно |
| is_tr | Nullable(Bool) |  | 0 | 0 | 0 | Трансферная перевозка |
| brand | LowCardinality(String) |  | 0 | 0 | 0 | Бренд |
| category_code | LowCardinality(String) |  | 0 | 0 | 0 | Категория пассажира |
| reason_issuance_sub_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Причина выписки купона EMD |
| reason_issuance_sub_grp | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Группа доп.услуг |
| is_ff | Nullable(Bool) |  | 0 | 0 | 0 | Отметка использования бонусов |
| tariff_structure | Nullable(String) |  | 0 | 0 | 0 | Структура тарифа |
| pnr | String |  | 0 | 0 | 0 | Номер брони; "Отсутствует" если не задан в источнике |
| voucher_num | Nullable(String) |  | 0 | 0 | 0 | Номер ваучера |
| return_exchange_voucher_num | Nullable(String) |  | 0 | 0 | 0 | Номера возвращаемых в обмен на ваучер документов |
| is_return_exchange_voucher | Nullable(Bool) |  | 0 | 0 | 0 | Возврат в обмен на ваучер |
| vouncher | LowCardinality(String) |  | 0 | 0 | 0 | Ваучер |
| exchange_voucher_usl | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Обмен ваучера на билет/услугу |
| passenger_name | Nullable(String) |  | 0 | 0 | 0 | ФИО пассажира |
| passenger_passport | Nullable(String) |  | 0 | 0 | 0 | Паспорт пассажира |
| passenger_birth_date | Nullable(Date32) |  | 0 | 0 | 0 | День рождения пассажира |
| emd_related_ticket_number | Nullable(String) |  | 0 | 0 | 0 | EMD - связанный документ |
| emd_related_coupon_number | Nullable(UInt8) |  | 0 | 0 | 0 | EMD - номер купона связанного документа |
| source | LowCardinality(String) |  | 0 | 0 | 0 | Источник |
| updated_dt | DateTime64(3, 'UTC') |  | 0 | 0 | 0 | Время последнего обновления документа |
| processing_dt | DateTime64(3, 'UTC') |  | 0 | 0 | 0 | Дата и время обработки (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Имя дага |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.sofi_bi_sales_ut (`document_number` String COMMENT 'Номер документа (бланка)', `coupon_number` UInt8 COMMENT 'Номер купона', `document_kind` LowCardinality(String) COMMENT 'Вид операции (бланка)', `carrier` LowCardinality(String) COMMENT 'Перевозчик', `flight_number` LowCardinality(String) COMMENT 'Номер рейса; "Отсутствует" если не задан в источнике', `origin` LowCardinality(String) COMMENT 'Пункт вылета; "Отсутствует" если не задан в источнике', `destination` LowCardinality(String) COMMENT 'Пункт прилета; "Отсутствует" если не задан в источнике', `agency_code` LowCardinality(Nullable(String)) COMMENT 'Код агента', `agency_name` LowCardinality(String) COMMENT 'Наименование агента', `agency_number_code` String COMMENT 'Пункт продажи (валидатор)', `document_coupon_count` UInt8 COMMENT 'Количество купонов документа', `sale_segment_count` Int8 COMMENT 'Количество проданных сегментов', `service_class` LowCardinality(Nullable(String)) COMMENT 'Класс бронирования', `fare_basis` LowCardinality(Nullable(String)) COMMENT 'Вид тарифа', `tax_rub_cpfp_amount` Decimal(18, 4) COMMENT 'Тариф на купоне РУБ', `tax_rub_yq_amount` Decimal(18, 4) COMMENT 'Топливный сбор РУБ', `tax_rub_zz_amount` Decimal(18, 4) COMMENT 'Такса ТКП (ZZ) РУБ', `tax_rub_cp_amount` Decimal(18, 4) COMMENT 'Штраф за возврат(обмен) (CP) РУБ', `tax_rub_ru_amount` Decimal(18, 4) COMMENT 'Сбор RU РУБ', `tax_rub_psyr_amount` Decimal(18, 4) COMMENT 'Сбор за бланк (PS,YR) РУБ', `tax_rub_another_amount` Decimal(18, 4) COMMENT 'Прочие сборы РУБ', `currency` LowCardinality(String) COMMENT 'Валюта', `tax_cpfp_amount` Decimal(18, 4) COMMENT 'Тариф на купоне', `tax_yq_amount` Decimal(18, 4) COMMENT 'Топливный сбор', `tax_zz_amount` Decimal(18, 4) COMMENT 'Такса ТКП (ZZ)', `tax_cp_amount` Decimal(18, 4) COMMENT 'Штраф за возврат(обмен) (CP)', `tax_ru_amount` Decimal(18, 4) COMMENT 'Сбор RU', `tax_psyr_amount` Decimal(18, 4) COMMENT 'Сбор за бланк (PS,YR)', `tax_another_amount` Decimal(18, 4) COMMENT 'Прочие сборы', `transaction_date` Date COMMENT 'Дата транзакции', `flight_date` Date COMMENT 'Дата вылета; 1970-01-01 если не задана в источнике', `gds` LowCardinality(Nullable(String)) COMMENT 'Система бронирования', `sale_system` LowCardinality(String) COMMENT 'Система продажи', `ff_card_passenger` Nullable(String) COMMENT 'Номер карты частолетающего пассажира', `ff_amount` Int32 COMMENT 'Сумма бонусов на купоне (эквив. РУБ)', `is_exchange` Nullable(Bool) COMMENT 'Участвует в обмене?', `exchange_document_number` Nullable(String) COMMENT 'Номер документа (обмен)', `supplement_currency` LowCardinality(Nullable(String)) COMMENT 'Валюта доплаты', `supplement_amount` Nullable(Int32) COMMENT 'Сумма доплаты', `supplement_amount_rub` Nullable(Int32) COMMENT 'Сумма доплаты РУБ', `fop_string` String COMMENT 'Примечание по ФОП' CODEC(ZSTD(3)), `is_rt` Nullable(Bool) COMMENT 'Перевозка туда-обратно', `is_tr` Nullable(Bool) COMMENT 'Трансферная перевозка', `brand` LowCardinality(String) COMMENT 'Бренд', `category_code` LowCardinality(String) COMMENT 'Категория пассажира', `reason_issuance_sub_code` LowCardinality(Nullable(String)) COMMENT 'Причина выписки купона EMD', `reason_issuance_sub_grp` LowCardinality(Nullable(String)) COMMENT 'Группа доп.услуг', `is_ff` Nullable(Bool) COMMENT 'Отметка использования бонусов', `tariff_structure` Nullable(String) COMMENT 'Структура тарифа', `pnr` String COMMENT 'Номер брони; "Отсутствует" если не задан в источнике', `voucher_num` Nullable(String) COMMENT 'Номер ваучера', `return_exchange_voucher_num` Nullable(String) COMMENT 'Номера возвращаемых в обмен на ваучер документов', `is_return_exchange_voucher` Nullable(Bool) COMMENT 'Возврат в обмен на ваучер', `vouncher` LowCardinality(String) COMMENT 'Ваучер', `exchange_voucher_usl` LowCardinality(Nullable(String)) COMMENT 'Обмен ваучера на билет/услугу', `passenger_name` Nullable(String) COMMENT 'ФИО пассажира' CODEC(ZSTD(3)), `passenger_passport` Nullable(String) COMMENT 'Паспорт пассажира' CODEC(ZSTD(3)), `passenger_birth_date` Nullable(Date32) COMMENT 'День рождения пассажира', `emd_related_ticket_number` Nullable(String) COMMENT 'EMD - связанный документ', `emd_related_coupon_number` Nullable(UInt8) COMMENT 'EMD - номер купона связанного документа', `source` LowCardinality(String) COMMENT 'Источник', `updated_dt` DateTime64(3, 'UTC') COMMENT 'Время последнего обновления документа', `processing_dt` DateTime64(3, 'UTC') COMMENT 'Дата и время обработки (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Имя дага', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC).', INDEX idx_carrier carrier TYPE set(300) GRANULARITY 1, INDEX idx_flight_number flight_number TYPE set(1000) GRANULARITY 1, INDEX idx_origin origin TYPE set(1000) GRANULARITY 1, INDEX idx_destination destination TYPE set(1000) GRANULARITY 1, INDEX idx_flight_date flight_date TYPE minmax GRANULARITY 4, INDEX idx_pnr pnr TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/analytics/sofi_bi_sales_ut', '{replica}', updated_dt) PARTITION BY toYYYYMM(transaction_date) ORDER BY (document_number, coupon_number, document_kind, transaction_date) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения аналитических данных из sofi_bi_sales_ut. Запросы выполнять с FINAL'
```
</details>

### `analytics`.`sofi_bi_sales_ut_distributed`
- Engine: Distributed
- Partition key: 
- Sorting key: 
- Primary key: 
- Total rows: None
- Total bytes: 0

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| document_number | String |  | 0 | 0 | 0 | Номер документа (бланка) |
| coupon_number | UInt8 |  | 0 | 0 | 0 | Номер купона |
| document_kind | LowCardinality(String) |  | 0 | 0 | 0 | Вид операции (бланка) |
| carrier | LowCardinality(String) |  | 0 | 0 | 0 | Перевозчик |
| flight_number | LowCardinality(String) |  | 0 | 0 | 0 | Номер рейса; "Отсутствует" если не задан в источнике |
| origin | LowCardinality(String) |  | 0 | 0 | 0 | Пункт вылета; "Отсутствует" если не задан в источнике |
| destination | LowCardinality(String) |  | 0 | 0 | 0 | Пункт прилета; "Отсутствует" если не задан в источнике |
| agency_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Код агента |
| agency_name | LowCardinality(String) |  | 0 | 0 | 0 | Наименование агента |
| agency_number_code | String |  | 0 | 0 | 0 | Пункт продажи (валидатор) |
| document_coupon_count | UInt8 |  | 0 | 0 | 0 | Количество купонов документа |
| sale_segment_count | Int8 |  | 0 | 0 | 0 | Количество проданных сегментов |
| service_class | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Класс бронирования |
| fare_basis | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Вид тарифа |
| tax_rub_cpfp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Тариф на купоне РУБ |
| tax_rub_yq_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Топливный сбор РУБ |
| tax_rub_zz_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Такса ТКП (ZZ) РУБ |
| tax_rub_cp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Штраф за возврат(обмен) (CP) РУБ |
| tax_rub_ru_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор RU РУБ |
| tax_rub_psyr_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор за бланк (PS,YR) РУБ |
| tax_rub_another_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Прочие сборы РУБ |
| currency | LowCardinality(String) |  | 0 | 0 | 0 | Валюта |
| tax_cpfp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Тариф на купоне |
| tax_yq_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Топливный сбор |
| tax_zz_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Такса ТКП (ZZ) |
| tax_cp_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Штраф за возврат(обмен) (CP) |
| tax_ru_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор RU |
| tax_psyr_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Сбор за бланк (PS,YR) |
| tax_another_amount | Decimal(18, 4) |  | 0 | 0 | 0 | Прочие сборы |
| transaction_date | Date |  | 0 | 0 | 0 | Дата транзакции |
| flight_date | Date |  | 0 | 0 | 0 | Дата вылета; 1970-01-01 если не задана в источнике |
| gds | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Система бронирования |
| sale_system | LowCardinality(String) |  | 0 | 0 | 0 | Система продажи |
| ff_card_passenger | Nullable(String) |  | 0 | 0 | 0 | Номер карты частолетающего пассажира |
| ff_amount | Int32 |  | 0 | 0 | 0 | Сумма бонусов на купоне (эквив. РУБ) |
| is_exchange | Nullable(Bool) |  | 0 | 0 | 0 | Участвует в обмене? |
| exchange_document_number | Nullable(String) |  | 0 | 0 | 0 | Номер документа (обмен) |
| supplement_currency | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Валюта доплаты |
| supplement_amount | Nullable(Int32) |  | 0 | 0 | 0 | Сумма доплаты |
| supplement_amount_rub | Nullable(Int32) |  | 0 | 0 | 0 | Сумма доплаты РУБ |
| fop_string | String |  | 0 | 0 | 0 | Примечание по ФОП |
| is_rt | Nullable(Bool) |  | 0 | 0 | 0 | Перевозка туда-обратно |
| is_tr | Nullable(Bool) |  | 0 | 0 | 0 | Трансферная перевозка |
| brand | LowCardinality(String) |  | 0 | 0 | 0 | Бренд |
| category_code | LowCardinality(String) |  | 0 | 0 | 0 | Категория пассажира |
| reason_issuance_sub_code | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Причина выписки купона EMD |
| reason_issuance_sub_grp | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Группа доп.услуг |
| is_ff | Nullable(Bool) |  | 0 | 0 | 0 | Отметка использования бонусов |
| tariff_structure | Nullable(String) |  | 0 | 0 | 0 | Структура тарифа |
| pnr | String |  | 0 | 0 | 0 | Номер брони; "Отсутствует" если не задан в источнике |
| voucher_num | Nullable(String) |  | 0 | 0 | 0 | Номер ваучера |
| return_exchange_voucher_num | Nullable(String) |  | 0 | 0 | 0 | Номера возвращаемых в обмен на ваучер документов |
| is_return_exchange_voucher | Nullable(Bool) |  | 0 | 0 | 0 | Возврат в обмен на ваучер |
| vouncher | LowCardinality(String) |  | 0 | 0 | 0 | Ваучер |
| exchange_voucher_usl | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 | Обмен ваучера на билет/услугу |
| passenger_name | Nullable(String) |  | 0 | 0 | 0 | ФИО пассажира |
| passenger_passport | Nullable(String) |  | 0 | 0 | 0 | Паспорт пассажира |
| passenger_birth_date | Nullable(Date32) |  | 0 | 0 | 0 | День рождения пассажира |
| emd_related_ticket_number | Nullable(String) |  | 0 | 0 | 0 | EMD - связанный документ |
| emd_related_coupon_number | Nullable(UInt8) |  | 0 | 0 | 0 | EMD - номер купона связанного документа |
| source | LowCardinality(String) |  | 0 | 0 | 0 | Источник |
| updated_dt | DateTime64(3, 'UTC') |  | 0 | 0 | 0 | Время последнего обновления документа |
| processing_dt | DateTime64(3, 'UTC') |  | 0 | 0 | 0 | Дата и время обработки (data_interval_end) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Имя дага |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE analytics.sofi_bi_sales_ut_distributed (`document_number` String COMMENT 'Номер документа (бланка)', `coupon_number` UInt8 COMMENT 'Номер купона', `document_kind` LowCardinality(String) COMMENT 'Вид операции (бланка)', `carrier` LowCardinality(String) COMMENT 'Перевозчик', `flight_number` LowCardinality(String) COMMENT 'Номер рейса; "Отсутствует" если не задан в источнике', `origin` LowCardinality(String) COMMENT 'Пункт вылета; "Отсутствует" если не задан в источнике', `destination` LowCardinality(String) COMMENT 'Пункт прилета; "Отсутствует" если не задан в источнике', `agency_code` LowCardinality(Nullable(String)) COMMENT 'Код агента', `agency_name` LowCardinality(String) COMMENT 'Наименование агента', `agency_number_code` String COMMENT 'Пункт продажи (валидатор)', `document_coupon_count` UInt8 COMMENT 'Количество купонов документа', `sale_segment_count` Int8 COMMENT 'Количество проданных сегментов', `service_class` LowCardinality(Nullable(String)) COMMENT 'Класс бронирования', `fare_basis` LowCardinality(Nullable(String)) COMMENT 'Вид тарифа', `tax_rub_cpfp_amount` Decimal(18, 4) COMMENT 'Тариф на купоне РУБ', `tax_rub_yq_amount` Decimal(18, 4) COMMENT 'Топливный сбор РУБ', `tax_rub_zz_amount` Decimal(18, 4) COMMENT 'Такса ТКП (ZZ) РУБ', `tax_rub_cp_amount` Decimal(18, 4) COMMENT 'Штраф за возврат(обмен) (CP) РУБ', `tax_rub_ru_amount` Decimal(18, 4) COMMENT 'Сбор RU РУБ', `tax_rub_psyr_amount` Decimal(18, 4) COMMENT 'Сбор за бланк (PS,YR) РУБ', `tax_rub_another_amount` Decimal(18, 4) COMMENT 'Прочие сборы РУБ', `currency` LowCardinality(String) COMMENT 'Валюта', `tax_cpfp_amount` Decimal(18, 4) COMMENT 'Тариф на купоне', `tax_yq_amount` Decimal(18, 4) COMMENT 'Топливный сбор', `tax_zz_amount` Decimal(18, 4) COMMENT 'Такса ТКП (ZZ)', `tax_cp_amount` Decimal(18, 4) COMMENT 'Штраф за возврат(обмен) (CP)', `tax_ru_amount` Decimal(18, 4) COMMENT 'Сбор RU', `tax_psyr_amount` Decimal(18, 4) COMMENT 'Сбор за бланк (PS,YR)', `tax_another_amount` Decimal(18, 4) COMMENT 'Прочие сборы', `transaction_date` Date COMMENT 'Дата транзакции', `flight_date` Date COMMENT 'Дата вылета; 1970-01-01 если не задана в источнике', `gds` LowCardinality(Nullable(String)) COMMENT 'Система бронирования', `sale_system` LowCardinality(String) COMMENT 'Система продажи', `ff_card_passenger` Nullable(String) COMMENT 'Номер карты частолетающего пассажира', `ff_amount` Int32 COMMENT 'Сумма бонусов на купоне (эквив. РУБ)', `is_exchange` Nullable(Bool) COMMENT 'Участвует в обмене?', `exchange_document_number` Nullable(String) COMMENT 'Номер документа (обмен)', `supplement_currency` LowCardinality(Nullable(String)) COMMENT 'Валюта доплаты', `supplement_amount` Nullable(Int32) COMMENT 'Сумма доплаты', `supplement_amount_rub` Nullable(Int32) COMMENT 'Сумма доплаты РУБ', `fop_string` String COMMENT 'Примечание по ФОП' CODEC(ZSTD(3)), `is_rt` Nullable(Bool) COMMENT 'Перевозка туда-обратно', `is_tr` Nullable(Bool) COMMENT 'Трансферная перевозка', `brand` LowCardinality(String) COMMENT 'Бренд', `category_code` LowCardinality(String) COMMENT 'Категория пассажира', `reason_issuance_sub_code` LowCardinality(Nullable(String)) COMMENT 'Причина выписки купона EMD', `reason_issuance_sub_grp` LowCardinality(Nullable(String)) COMMENT 'Группа доп.услуг', `is_ff` Nullable(Bool) COMMENT 'Отметка использования бонусов', `tariff_structure` Nullable(String) COMMENT 'Структура тарифа', `pnr` String COMMENT 'Номер брони; "Отсутствует" если не задан в источнике', `voucher_num` Nullable(String) COMMENT 'Номер ваучера', `return_exchange_voucher_num` Nullable(String) COMMENT 'Номера возвращаемых в обмен на ваучер документов', `is_return_exchange_voucher` Nullable(Bool) COMMENT 'Возврат в обмен на ваучер', `vouncher` LowCardinality(String) COMMENT 'Ваучер', `exchange_voucher_usl` LowCardinality(Nullable(String)) COMMENT 'Обмен ваучера на билет/услугу', `passenger_name` Nullable(String) COMMENT 'ФИО пассажира' CODEC(ZSTD(3)), `passenger_passport` Nullable(String) COMMENT 'Паспорт пассажира' CODEC(ZSTD(3)), `passenger_birth_date` Nullable(Date32) COMMENT 'День рождения пассажира', `emd_related_ticket_number` Nullable(String) COMMENT 'EMD - связанный документ', `emd_related_coupon_number` Nullable(UInt8) COMMENT 'EMD - номер купона связанного документа', `source` LowCardinality(String) COMMENT 'Источник', `updated_dt` DateTime64(3, 'UTC') COMMENT 'Время последнего обновления документа', `processing_dt` DateTime64(3, 'UTC') COMMENT 'Дата и время обработки (data_interval_end)', `meta_source` LowCardinality(String) COMMENT 'Имя дага', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC).') ENGINE = Distributed('{cluster}', 'analytics', 'sofi_bi_sales_ut', cityHash64(document_number, transaction_date, document_kind, coupon_number)) COMMENT 'Распределённая таблица для аналитических данных продаж sofi_bi_sales_ut. Запросы выполнять с FINAL'
```
</details>

### `integrated`.`amos_heli_req_ll`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_dt)
- Sorting key: ac_type, processing_dt
- Primary key: ac_type, processing_dt
- Total rows: 5796781
- Total bytes: 27124906

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| title | LowCardinality(String) |  | 0 | 0 | 0 |  |
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_type | LowCardinality(String) |  | 1 | 1 | 0 |  |
| rotable_manuf_before | Nullable(Int32) |  | 0 | 0 | 0 |  |
| rotable_manuf_after | Nullable(Int32) |  | 0 | 0 | 0 |  |
| interval | Int64 |  | 0 | 0 | 0 |  |
| psn | Int64 |  | 0 | 0 | 0 |  |
| processing_dt | DateTime |  | 1 | 1 | 1 | дата отработки дага (data_interval_start) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE integrated.amos_heli_req_ll (`title` LowCardinality(String), `partno` Nullable(String), `ac_type` LowCardinality(String), `rotable_manuf_before` Nullable(Int32), `rotable_manuf_after` Nullable(Int32), `interval` Int64, `psn` Int64, `processing_dt` DateTime COMMENT 'дата отработки дага (data_interval_start)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/integrated/amos_heli_req_ll', '{replica}') PARTITION BY toYYYYMM(processing_dt) ORDER BY (ac_type, processing_dt) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения объединенных данных подзапроса req_ll'
```
</details>

### `integrated`.`amos_heli_req_ll2`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_dt)
- Sorting key: ac_type, processing_dt
- Primary key: ac_type, processing_dt
- Total rows: 7862
- Total bytes: 37593

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| title | LowCardinality(String) |  | 0 | 0 | 0 |  |
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_type | LowCardinality(String) |  | 1 | 1 | 0 |  |
| rotable_manuf_before | Nullable(Int32) |  | 0 | 0 | 0 |  |
| rotable_manuf_after | Nullable(Int32) |  | 0 | 0 | 0 |  |
| interval | Int64 |  | 0 | 0 | 0 |  |
| processing_dt | DateTime |  | 1 | 1 | 1 | дата отработки дага (data_interval_start) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE integrated.amos_heli_req_ll2 (`title` LowCardinality(String), `partno` Nullable(String), `ac_type` LowCardinality(String), `rotable_manuf_before` Nullable(Int32), `rotable_manuf_after` Nullable(Int32), `interval` Int64, `processing_dt` DateTime COMMENT 'дата отработки дага (data_interval_start)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/integrated/amos_heli_req_ll2', '{replica}') PARTITION BY toYYYYMM(processing_dt) ORDER BY (ac_type, processing_dt) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения объединенных данных подзапроса req_ll2'
```
</details>

### `integrated`.`amos_heli_req_oh`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_dt)
- Sorting key: ac_type, processing_dt
- Primary key: ac_type, processing_dt
- Total rows: 4017720
- Total bytes: 19107038

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| title | LowCardinality(String) |  | 0 | 0 | 0 |  |
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_type | LowCardinality(String) |  | 1 | 1 | 0 |  |
| rotable_manuf_before | Nullable(Int32) |  | 0 | 0 | 0 |  |
| rotable_manuf_after | Nullable(Int32) |  | 0 | 0 | 0 |  |
| threshold | Int64 |  | 0 | 0 | 0 |  |
| interval | Int64 |  | 0 | 0 | 0 |  |
| psn | Int64 |  | 0 | 0 | 0 |  |
| processing_dt | DateTime |  | 1 | 1 | 1 | дата отработки дага (data_interval_start) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE integrated.amos_heli_req_oh (`title` LowCardinality(String), `partno` Nullable(String), `ac_type` LowCardinality(String), `rotable_manuf_before` Nullable(Int32), `rotable_manuf_after` Nullable(Int32), `threshold` Int64, `interval` Int64, `psn` Int64, `processing_dt` DateTime COMMENT 'дата отработки дага (data_interval_start)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/integrated/amos_heli_req_oh', '{replica}') PARTITION BY toYYYYMM(processing_dt) ORDER BY (ac_type, processing_dt) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения объединенных данных подзапроса req_oh'
```
</details>

### `integrated`.`amos_heli_req_oh2`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_dt)
- Sorting key: ac_type, processing_dt
- Primary key: ac_type, processing_dt
- Total rows: 4968
- Total bytes: 31996

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| title | LowCardinality(String) |  | 0 | 0 | 0 |  |
| partno | Nullable(String) |  | 0 | 0 | 0 |  |
| ac_type | LowCardinality(String) |  | 1 | 1 | 0 |  |
| rotable_manuf_before | Nullable(Int32) |  | 0 | 0 | 0 |  |
| rotable_manuf_after | Nullable(Int32) |  | 0 | 0 | 0 |  |
| threshold | Int64 |  | 0 | 0 | 0 |  |
| interval | Int64 |  | 0 | 0 | 0 |  |
| processing_dt | DateTime |  | 1 | 1 | 1 | дата отработки дага (data_interval_start) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE integrated.amos_heli_req_oh2 (`title` LowCardinality(String), `partno` Nullable(String), `ac_type` LowCardinality(String), `rotable_manuf_before` Nullable(Int32), `rotable_manuf_after` Nullable(Int32), `threshold` Int64, `interval` Int64, `processing_dt` DateTime COMMENT 'дата отработки дага (data_interval_start)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/integrated/amos_heli_req_oh2', '{replica}') PARTITION BY toYYYYMM(processing_dt) ORDER BY (ac_type, processing_dt) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения объединенных данных подзапроса req_oh2'
```
</details>

### `integrated`.`amos_heli_rot_ll_oh`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_dt)
- Sorting key: requirement, processing_dt, unit
- Primary key: requirement, processing_dt, unit
- Total rows: 9258870
- Total bytes: 108994052

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| counterno_i | Nullable(Int64) |  | 0 | 0 | 0 |  |
| psn | Nullable(Int64) |  | 0 | 0 | 0 |  |
| requirement | LowCardinality(String) |  | 1 | 1 | 0 |  |
| unit | LowCardinality(String) |  | 1 | 1 | 0 |  |
| due | Nullable(Float64) |  | 0 | 0 | 0 |  |
| int | Nullable(Int64) |  | 0 | 0 | 0 |  |
| display_unit | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| individualise | UInt8 |  | 0 | 0 | 0 |  |
| processing_dt | DateTime |  | 1 | 1 | 1 | дата отработки дага (data_interval_start) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE integrated.amos_heli_rot_ll_oh (`counterno_i` Nullable(Int64), `psn` Nullable(Int64), `requirement` LowCardinality(String), `unit` LowCardinality(String), `due` Nullable(Float64), `int` Nullable(Int64), `display_unit` LowCardinality(Nullable(String)), `individualise` UInt8, `processing_dt` DateTime COMMENT 'дата отработки дага (data_interval_start)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/integrated/amos_heli_rot_ll_oh', '{replica}') PARTITION BY toYYYYMM(processing_dt) ORDER BY (requirement, processing_dt, unit) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения объединенных данных подзапроса rot_ll_oh'
```
</details>

### `integrated`.`amos_heli_rotables_enriched`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_dt)
- Sorting key: partno, processing_dt
- Primary key: partno, processing_dt
- Total rows: 14601971
- Total bytes: 222085846

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| ac_registr | LowCardinality(Nullable(String)) |  | 0 | 0 | 0 |  |
| partno | String |  | 1 | 1 | 0 |  |
| serialno | String |  | 0 | 0 | 0 |  |
| locationno_i | Int32 |  | 0 | 0 | 0 |  |
| LL | Nullable(Int64) |  | 0 | 0 | 0 |  |
| LL_ind | Nullable(String) |  | 0 | 0 | 0 |  |
| OH | Nullable(Int64) |  | 0 | 0 | 0 |  |
| OH_ind | Nullable(String) |  | 0 | 0 | 0 |  |
| sne | Nullable(Int32) |  | 0 | 0 | 0 |  |
| ppr | Nullable(Int32) |  | 0 | 0 | 0 |  |
| shop_visit_counter | Int32 |  | 0 | 0 | 0 |  |
| mfg_date_int | Int64 |  | 0 | 0 | 0 |  |
| mfg_date | String |  | 0 | 0 | 0 |  |
| oh_at_date | Date32 |  | 0 | 0 | 0 |  |
| owner | LowCardinality(String) |  | 0 | 0 | 0 |  |
| condition | Nullable(String) |  | 0 | 0 | 0 |  |
| psn | Int64 |  | 0 | 0 | 0 |  |
| processing_dt | DateTime |  | 1 | 1 | 1 | дата отработки дага (data_interval_start) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных (DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE integrated.amos_heli_rotables_enriched (`ac_registr` LowCardinality(Nullable(String)), `partno` String, `serialno` String, `locationno_i` Int32, `LL` Nullable(Int64), `LL_ind` Nullable(String), `OH` Nullable(Int64), `OH_ind` Nullable(String), `sne` Nullable(Int32), `ppr` Nullable(Int32), `shop_visit_counter` Int32, `mfg_date_int` Int64, `mfg_date` String, `oh_at_date` Date32, `owner` LowCardinality(String), `condition` Nullable(String), `psn` Int64, `processing_dt` DateTime COMMENT 'дата отработки дага (data_interval_start)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных (DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/integrated/amos_heli_rotables_enriched', '{replica}') PARTITION BY toYYYYMM(processing_dt) ORDER BY (partno, processing_dt) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения объединенных данных подзапроса rotables_enriched'
```
</details>

### `integrated`.`amos_heli_rotables_enriched_aggregated`
- Engine: ReplicatedMergeTree
- Partition key: toYYYYMM(processing_dt)
- Sorting key: partno, processing_dt, psn, mfg_date_int
- Primary key: partno, processing_dt, psn, mfg_date_int
- Total rows: 10903754
- Total bytes: 107086256

| Колонка | Тип | Default | PK | Sort | Partition | Comment |
|---|---|---|:---|:---|:---:|---|
| partno | String |  | 1 | 1 | 0 |  |
| partseqno_i | Int32 |  | 0 | 0 | 0 |  |
| serialno | String |  | 0 | 0 | 0 |  |
| ac_typ | String |  | 0 | 0 | 0 |  |
| ac_type_i | Int32 |  | 0 | 0 | 0 |  |
| ac_typ_for_req | Nullable(String) |  | 0 | 0 | 0 |  |
| location | Nullable(String) |  | 0 | 0 | 0 |  |
| LL | Nullable(Int64) |  | 0 | 0 | 0 |  |
| LL_ind | Nullable(String) |  | 0 | 0 | 0 |  |
| OH | Nullable(Int64) |  | 0 | 0 | 0 |  |
| OH_ind | Nullable(String) |  | 0 | 0 | 0 |  |
| sne | Nullable(Int32) |  | 0 | 0 | 0 |  |
| ppr | Nullable(Int32) |  | 0 | 0 | 0 |  |
| mfg_date | String |  | 0 | 0 | 0 |  |
| oh_at_date | Date32 |  | 0 | 0 | 0 |  |
| shop_visit_counter | Int32 |  | 0 | 0 | 0 |  |
| owner | LowCardinality(String) |  | 0 | 0 | 0 |  |
| address_i | Int32 |  | 0 | 0 | 0 |  |
| condition | Nullable(String) |  | 0 | 0 | 0 |  |
| removal_date | Nullable(Date32) |  | 0 | 0 | 0 |  |
| mfg_date_int | Int64 |  | 1 | 1 | 0 |  |
| psn | Int64 |  | 1 | 1 | 0 |  |
| target_date | Nullable(Date32) |  | 0 | 0 | 0 |  |
| processing_dt | DateTime |  | 1 | 1 | 1 | дата отработки дага (data_interval_start) |
| meta_source | LowCardinality(String) |  | 0 | 0 | 0 | Идентификатор источника данных  (DAG ID) |
| meta_loading_at | DateTime | now() | 0 | 0 | 0 | Время загрузки в ClickHouse (UTC). Служебное поле. |

<details><summary>create_table_query</summary>


```sql
CREATE TABLE integrated.amos_heli_rotables_enriched_aggregated (`partno` String, `partseqno_i` Int32, `serialno` String, `ac_typ` String, `ac_type_i` Int32, `ac_typ_for_req` Nullable(String), `location` Nullable(String), `LL` Nullable(Int64), `LL_ind` Nullable(String), `OH` Nullable(Int64), `OH_ind` Nullable(String), `sne` Nullable(Int32), `ppr` Nullable(Int32), `mfg_date` String, `oh_at_date` Date32, `shop_visit_counter` Int32, `owner` LowCardinality(String), `address_i` Int32, `condition` Nullable(String), `removal_date` Nullable(Date32), `mfg_date_int` Int64, `psn` Int64, `target_date` Nullable(Date32), `processing_dt` DateTime COMMENT 'дата отработки дага (data_interval_start)', `meta_source` LowCardinality(String) COMMENT 'Идентификатор источника данных  (DAG ID)', `meta_loading_at` DateTime DEFAULT now() COMMENT 'Время загрузки в ClickHouse (UTC). Служебное поле.') ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/integrated/amos_heli_rotables_enriched_aggregated', '{replica}') PARTITION BY toYYYYMM(processing_dt) ORDER BY (partno, processing_dt, psn, mfg_date_int) SETTINGS index_granularity = 8192 COMMENT 'Таблица для хранения объединенных данных подзапроса rotables_enriched_aggregated'
```
</details>

### 2.MV Материализованные представления (`engine` LIKE '%View%')
- **`analytics`.`amos_heli_ac_typ_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_ac_typ_view (`ac_type_i` Int32, `ac_typ` String, `description` Nullable(String), `fa_ac_typ` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_ac_typ
```
</details>

- **`analytics`.`amos_heli_address_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_address_view (`address_i` Int32, `vendor` LowCardinality(String), `name` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_address
```
</details>

- **`analytics`.`amos_heli_adr_properties_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_adr_properties_view (`address_i` Int32, `prop_type_i` Int32, `remarks` LowCardinality(Nullable(String)), `value` LowCardinality(String), `status` Nullable(Int8), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_adr_properties
```
</details>

- **`analytics`.`amos_heli_adr_special_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_adr_special_view (`special_i` Int32, `address_i` Int32, `special` LowCardinality(Nullable(String)), `remarks` LowCardinality(Nullable(String)), `amount` Nullable(Int8), `reference_no` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_adr_special
```
</details>

- **`analytics`.`amos_heli_aircraft_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_aircraft_view (`ac_registr` String, `ac_typ` LowCardinality(String), `ac_registr_prefix` LowCardinality(Nullable(String)), `ac_model` LowCardinality(Nullable(String)), `owner` LowCardinality(Nullable(String)), `manual_owner` LowCardinality(Nullable(String)), `status` Int16, `non_managed` LowCardinality(Nullable(String)), `homebase` LowCardinality(Nullable(String)), `object_type` LowCardinality(Nullable(String)), `description` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_aircraft
```
</details>

- **`analytics`.`amos_heli_applicability_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_applicability_view (`applicabilityno_i` Int64, `effectivityno_i` Int32, `applicable` LowCardinality(String), `ref_key` Int64, `ref_type` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_applicability
```
</details>

- **`analytics`.`amos_heli_condition_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_condition_view (`condition` LowCardinality(String), `description` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_condition
```
</details>

- **`analytics`.`amos_heli_counter_definition_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_counter_definition_view (`counter_defno_i` UInt64, `code` LowCardinality(String), `name` Nullable(String), `description` Nullable(String), `display_unit` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_counter_definition
```
</details>

- **`analytics`.`amos_heli_counter_template_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_counter_template_view (`counter_templateno_i` Int32, `counter_defno_i` Int32, `counter_template_groupno_i` Int32, `type` LowCardinality(String), `is_calculated` LowCardinality(String), `description` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_counter_template
```
</details>

- **`analytics`.`amos_heli_counter_value_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_counter_value_view (`counter_valueno_i` Int64, `counterno_i` Int64, `life_value` Nullable(Float64), `readout_ref_type` LowCardinality(Nullable(String)), `on_counter_valueno_i` Nullable(Int64), `off_counter_valueno_i` Nullable(Int64), `is_minor` LowCardinality(Nullable(String)), `readout_date` Date32, `readout_time` Int64, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_counter_value
```
</details>

- **`analytics`.`amos_heli_counter_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_counter_view (`counterno_i` Int64, `counter_templateno_i` Int32, `ref_type` LowCardinality(String), `ref_key` Int64, `life_value` Nullable(Float64), `is_unknown` LowCardinality(String), `status` Nullable(Int16), `master_counterno_i` Nullable(Int64), `readout_date` Date32, `readout_time` Int64, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_counter
```
</details>

- **`analytics`.`amos_heli_event_effectivity_link_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_event_effectivity_link_view (`effectivityno_i` Int32, `effectivity_linkno_i` Int32, `event_type` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_event_effectivity_link
```
</details>

- **`analytics`.`amos_heli_event_effectivity_rules_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_event_effectivity_rules_view (`effectivityno_i` Int32, `aircraft_type` LowCardinality(Nullable(String)), `rotable_manuf_before` Nullable(Int32), `rotable_manuf_after` Nullable(Int32), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_event_effectivity_rules
```
</details>

- **`analytics`.`amos_heli_event_effectivity_sns_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_event_effectivity_sns_view (`effectivityno_i` Int32, `effectivity_snno_i` Int32, `range_type` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_event_effectivity_sns
```
</details>

- **`analytics`.`amos_heli_event_effectivity_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_event_effectivity_view (`effectivityno_i` Int32, `effectivity_headerno_i` Nullable(Int32), `title` String, `aircraft_typ` LowCardinality(Nullable(String)), `partno` Nullable(String), `status` Nullable(Int16), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_event_effectivity
```
</details>

- **`analytics`.`amos_heli_forecast_dimension_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_forecast_dimension_view (`event_perfno_i` Int64, `counter_defno_i` Int32, `dimension` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_forecast_dimension
```
</details>

- **`analytics`.`amos_heli_forecast_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_forecast_view (`event_perfno_i` Int64, `psn` Nullable(Int64), `requirement` LowCardinality(Nullable(String)), `partno` Nullable(String), `serialno` Nullable(String), `ac_registr` String, `ac_typ` LowCardinality(String), `event` String, `event_type` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_forecast
```
</details>

- **`analytics`.`amos_heli_history_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_history_view (`historyno_i` Int64, `partno` String, `serialno` Nullable(String), `vm` LowCardinality(String), `od_detailno_i` Nullable(Int32), `ac_registr` Nullable(String), `del_date` Date32, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_history
```
</details>

- **`analytics`.`amos_heli_location_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_location_view (`locationno_i` Int32, `description` LowCardinality(String), `store` LowCardinality(String), `station` LowCardinality(String), `location` LowCardinality(String), `status` Int16, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_location
```
</details>

- **`analytics`.`amos_heli_mevt_effectivity_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_mevt_effectivity_view (`mevt_effectivityno_i` Int64, `mevt_headerno_i` Int64, `effectivity_linkno_i` Int64, `template_revisionno_i` Int64, `timerequirementno_i` Int64, `revision_key` Int32, `revision_type` LowCardinality(String), `applicable_status` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_mevt_effectivity
```
</details>

- **`analytics`.`amos_heli_mevt_header_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_mevt_header_view (`mevt_headerno_i` Int64, `identifier` String, `ref_key` Int64, `ref_type` LowCardinality(String), `mevt_key` Int32, `mevt_type` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_mevt_header
```
</details>

- **`analytics`.`amos_heli_od_detail_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_od_detail_view (`detailno_i` Int64, `orderno_i` Int64, `order_type` LowCardinality(String), `ac_registr` LowCardinality(Nullable(String)), `state` LowCardinality(String), `vendor` LowCardinality(Nullable(String)), `partno` String, `serialno` Nullable(String), `condition` LowCardinality(Nullable(String)), `qty` Int32, `purch_price` Int64, `target_date` Date32, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_od_detail
```
</details>

- **`analytics`.`amos_heli_part_requirement_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_part_requirement_view (`part_requirementno_i` Int32, `type` Int32, `status` Int16, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_part_requirement
```
</details>

- **`analytics`.`amos_heli_part_special_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_part_special_view (`part_specialno_i` Int32, `partno` String, `special` LowCardinality(String), `remarks` LowCardinality(String), `amount` Int16, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_part_special
```
</details>

- **`analytics`.`amos_heli_part_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_part_view (`partno` String, `partmatch` String, `partseqno_i` Int32, `ac_typ` LowCardinality(String), `mat_type` LowCardinality(String), `description` String, `remarks` Nullable(String), `ata_chapter` LowCardinality(String), `vendor` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_part
```
</details>

- **`analytics`.`amos_heli_requirement_header_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_requirement_header_view (`event_key` Int32, `event_type` LowCardinality(String), `effectivity_headerno_i` Nullable(Int32), `requirement_headerno_i` Int32, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_requirement_header
```
</details>

- **`analytics`.`amos_heli_requirement_type_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_requirement_type_view (`requirement_typeno_i` Int32, `requirement` LowCardinality(String), `description` Nullable(String), `life_limit` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_requirement_type
```
</details>

- **`analytics`.`amos_heli_rotables_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_rotables_view (`ac_registr` Nullable(String), `partno` String, `material_lifecycle_id` Int64, `serialno` String, `locationno_i` Int32, `psn` Int64, `shop_visit_counter` Int32, `mfg_unknown` LowCardinality(Nullable(String)), `orderno` Nullable(String), `owner` LowCardinality(String), `condition` LowCardinality(String), `oh_at_date` Date32, `del_date` Date32, `mfg_date` Date32, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_rotables
```
</details>

- **`analytics`.`amos_heli_treq_dimension_group_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_treq_dimension_group_view (`interval_groupno_i` Int64, `dimension_groupno_i` Int64, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_treq_dimension_group
```
</details>

- **`analytics`.`amos_heli_treq_event_link_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_treq_event_link_view (`eventlinkno_i` Int32, `event_type` LowCardinality(String), `event_key` Int32, `ac_registr` LowCardinality(Nullable(String)), `psn` Int32, `status` Int16, `timerequirementno_i` Int32, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_treq_event_link
```
</details>

- **`analytics`.`amos_heli_treq_interval_group_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_treq_interval_group_view (`interval_groupno_i` Int64, `timerequirementno_i` Int64, `threshold` LowCardinality(String), `group_name` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_treq_interval_group
```
</details>

- **`analytics`.`amos_heli_treq_interval_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_treq_interval_view (`intervalno_i` Int64, `interval_groupno_i` Int32, `dimension_type` LowCardinality(String), `counter_defno_i` Int32, `amount_interval` Int64, `due_at` Nullable(Int32), `dimension_groupno_i` Int64, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_treq_interval
```
</details>

- **`analytics`.`amos_heli_treq_time_requirement_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_treq_time_requirement_view (`timerequirementno_i` Int64, `event_type` LowCardinality(String), `event_key` Int64, `ac_group` LowCardinality(Nullable(String)), `type` LowCardinality(String), `status` Nullable(Int16), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_treq_time_requirement
```
</details>

- **`analytics`.`amos_heli_wo_event_link_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_wo_event_link_view (`event_perfno_i` Int64, `effectivity_linkno_i` Nullable(Int32), `mevt_headerno_i` Int64, `pending_status` Int16, `event_name` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_wo_event_link
```
</details>

- **`analytics`.`amos_heli_wo_header_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_wo_header_view (`event_perfno_i` Int64, `psn` Nullable(Int64), `ata_chapter` LowCardinality(Nullable(String)), `state` LowCardinality(String), `ac_registr` Nullable(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_wo_header
```
</details>

- **`analytics`.`amos_heli_wo_transfer_dimension_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_wo_transfer_dimension_view (`wo_transfer_dimensionno_i` Int64, `event_transferno_i` Int64, `treq_intervalno_i` Nullable(Int64), `counterno_i` Nullable(Int64), `due_at` Nullable(Float64), `status` Int8, `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_wo_transfer_dimension
```
</details>

- **`analytics`.`amos_heli_wo_transfer_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_wo_transfer_view (`event_transferno_i` Int64, `event_perfno_i` Int64, `is_last_transfer` LowCardinality(String), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_wo_transfer
```
</details>

- **`analytics`.`amos_heli_wp_header_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.amos_heli_wp_header_view (`mpno` LowCardinality(Nullable(String)), `drop_locationno_i` Nullable(Int32), `wpno_i` Int32, `wpno` String, `ac_registr` LowCardinality(Nullable(String)), `ac_typ` LowCardinality(String), `projectno` LowCardinality(Nullable(String)), `est_groundtime` Int32, `station` LowCardinality(String), `start_date` Int32, `start_time` Int32, `end_date` Int32, `end_time` Int32, `description` LowCardinality(Nullable(String)), `owner` LowCardinality(Nullable(String)), `hidden` LowCardinality(Nullable(String)), `status` Int8, `act_start_date` Nullable(Int32), `act_start_time` Nullable(Int32), `act_end_date` Nullable(Int32), `act_end_time` Nullable(Int32), `responsible` LowCardinality(Nullable(String)), `delay` Nullable(Int32), `cust_wpno` Nullable(String), `priority_code` LowCardinality(Nullable(String)), `remarks` Nullable(String), `extension_time` Nullable(Int32), `extension_reason` Nullable(Int8), `mpno_i` Nullable(Int16), `mp_revision` Nullable(Int16), `wp_status` Nullable(Int16), `events_collection_status` Nullable(Int16), `uuid` Nullable(String), `ac_operator` LowCardinality(Nullable(String)), `ac_model` LowCardinality(Nullable(String)), `created_date` Date, `updated_at` Date, `valid_from` DateTime, `valid_to` Nullable(DateTime), `processing_date_at` DateTime, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT * FROM source.amos_heli_wp_header
```
</details>

- **`analytics`.`availability_pricing_audit_mv`** — engine: `MaterializedView`
<details><summary>create_table_query</summary>

```sql
CREATE MATERIALIZED VIEW analytics.availability_pricing_audit_mv TO analytics.availability_pricing_audit (`trace_id` String, `event_time` DateTime64(3), `user_ip` String, `duration` Float64, `method` String, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT trace_id, event_time, user_ip, duration, 'getavailability' AS method, meta_source, now() AS meta_loading_at FROM source.availability_pricing_audit
```
</details>

- **`analytics`.`availability_pricing_logstash_mv`** — engine: `MaterializedView`
<details><summary>create_table_query</summary>

```sql
CREATE MATERIALIZED VIEW analytics.availability_pricing_logstash_mv TO analytics.availability_pricing_logstash (`trace_id` String, `event_time` DateTime64(3), `user_ip` Nullable(String), `duration` Float64, `method` String, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT trace_id, event_time, CAST(NULL, 'Nullable(String)') AS user_ip, duration, method, meta_source, now() AS meta_loading_at FROM source.availability_pricing_logstash
```
</details>

- **`analytics`.`fdr_express_analysis_atr72_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.fdr_express_analysis_atr72_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `takeoff_weight` Nullable(String), `landing_weight` Nullable(String), `approach_system` Nullable(String), `R03` Nullable(String), `R22` Nullable(String), `max_R41_R42` Nullable(String), `R18` Nullable(String), `max_R24_R26` Nullable(String), `landing_conditions` Nullable(String), `takeoffSeat` Nullable(String), `landingSeat` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS WITH fp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key IN ('Заход на посадку', 'Система захода на посадку')) AS approach_system, maxIf(flight_parameter_value, flight_parameter_key = 'Взлетный вес, т') AS takeoff_weight, maxIf(flight_parameter_value, flight_parameter_key = 'Посадочный вес, т') AS landing_weight, maxIf(flight_parameter_value, flight_parameter_key = 'Признак условий посадки') AS landing_conditions, maxIf(flight_parameter_value, flight_parameter_key = 'takeoffSeat') AS takeoffSeat, maxIf(flight_parameter_value, flight_parameter_key = 'landingSeat') AS landingSeat FROM analytics.fdr_express_analysis_flight_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date), rp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(regular_parameter_value, regular_parameter_key = 'R03') AS R03, maxIf(regular_parameter_value, regular_parameter_key = 'R18') AS R18, maxIf(regular_parameter_value, regular_parameter_key = 'R22') AS R22, maxIf(regular_parameter_value, regular_parameter_key = 'R24') AS R24, maxIf(regular_parameter_value, regular_parameter_key = 'R26') AS R26, maxIf(regular_parameter_value, regular_parameter_key = 'R41') AS R41, maxIf(regular_parameter_value, regular_parameter_key = 'R42') AS R42 FROM analytics.fdr_express_analysis_regular_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date) SELECT fp.aircraft_type, fp.board_number, toString(toYear(fp.report_start_dt)) AS year, toString(toMonth(fp.report_start_dt)) AS month, toString(toDayOfMonth(fp.report_start_dt)) AS day, fp.flight_number, fp.pilot_number, formatDateTime(fp.report_start_dt, '%H:%i:%S') AS T0, formatDateTime(fp.report_end_dt, '%H:%i:%S') AS TK, fp.route_note, fp.takeoff_weight, fp.landing_weight, fp.approach_system, rp.R03, rp.R22, if(greatest(toFloat64OrNull(rp.R41), toFloat64OrNull(rp.R42)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R41), toFloat64OrNull(rp.R42)))) AS max_R41_R42, rp.R18, if(greatest(toFloat64OrNull(rp.R24), toFloat64OrNull(rp.R26)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R24), toFloat64OrNull(rp.R26)))) AS max_R24_R26, fp.landing_conditions, fp.takeoffSeat, fp.landingSeat, fp.takeoff_datetime, fp.target_date FROM fp LEFT JOIN rp ON (fp.aircraft_type = rp.aircraft_type) AND (fp.board_number = rp.board_number) AND (fp.flight_number = rp.flight_number) AND (fp.pilot_number = rp.pilot_number) AND (fp.report_start_dt = rp.report_start_dt) AND (fp.report_end_dt = rp.report_end_dt) AND (fp.arm_source_file_path = rp.arm_source_file_path) AND (fp.source_file_name = rp.source_file_name) AND (fp.takeoff_datetime = rp.takeoff_datetime) AND (fp.target_date = rp.target_date) WHERE fp.aircraft_type = 'ATR-72-212' ORDER BY fp.aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, fp.board_number ASC, fp.flight_number ASC
```
</details>

- **`analytics`.`fdr_express_analysis_b737700_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.fdr_express_analysis_b737700_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `approach_system` Nullable(String), `R01` Nullable(String), `R03` Nullable(String), `R14` Nullable(String), `max_R25_R26_R27` Nullable(String), `R30` Nullable(String), `R31` Nullable(String), `R32` Nullable(String), `takeoffSeat` Nullable(String), `landingSeat` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS WITH fp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key IN ('Заход на посадку', 'Система захода на посадку')) AS approach_system, maxIf(flight_parameter_value, flight_parameter_key = 'takeoffSeat') AS takeoffSeat, maxIf(flight_parameter_value, flight_parameter_key = 'landingSeat') AS landingSeat FROM analytics.fdr_express_analysis_flight_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date), rp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(regular_parameter_value, regular_parameter_key = 'R001') AS R001, maxIf(regular_parameter_value, regular_parameter_key = 'R01') AS R01, maxIf(regular_parameter_value, regular_parameter_key = 'R003') AS R003, maxIf(regular_parameter_value, regular_parameter_key = 'R03') AS R03, maxIf(regular_parameter_value, regular_parameter_key = 'R014') AS R014, maxIf(regular_parameter_value, regular_parameter_key = 'R14') AS R14, maxIf(regular_parameter_value, regular_parameter_key = 'R25') AS R25, maxIf(regular_parameter_value, regular_parameter_key = 'R26') AS R26, maxIf(regular_parameter_value, regular_parameter_key = 'R27') AS R27, maxIf(regular_parameter_value, regular_parameter_key = 'R030') AS R030, maxIf(regular_parameter_value, regular_parameter_key = 'R30') AS R30, maxIf(regular_parameter_value, regular_parameter_key = 'R031') AS R031, maxIf(regular_parameter_value, regular_parameter_key = 'R31') AS R31, maxIf(regular_parameter_value, regular_parameter_key = 'R032') AS R032, maxIf(regular_parameter_value, regular_parameter_key = 'R32') AS R32 FROM analytics.fdr_express_analysis_regular_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date) SELECT fp.aircraft_type, fp.board_number, toString(toYear(fp.report_start_dt)) AS year, toString(toMonth(fp.report_start_dt)) AS month, toString(toDayOfMonth(fp.report_start_dt)) AS day, fp.flight_number, fp.pilot_number, formatDateTime(fp.report_start_dt, '%H:%i:%S') AS T0, formatDateTime(fp.report_end_dt, '%H:%i:%S') AS TK, fp.route_note, fp.approach_system, coalesce(rp.R001, rp.R01) AS R01, coalesce(rp.R003, rp.R03) AS R03, coalesce(rp.R014, rp.R14) AS R14, if(greatest(toFloat64OrNull(rp.R25), toFloat64OrNull(rp.R26), toFloat64OrNull(rp.R27)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R25), toFloat64OrNull(rp.R26), toFloat64OrNull(rp.R27)))) AS max_R25_R26_R27, coalesce(rp.R030, rp.R30) AS R30, coalesce(rp.R031, rp.R31) AS R31, coalesce(rp.R032, rp.R32) AS R32, fp.takeoffSeat, fp.landingSeat, fp.takeoff_datetime, fp.target_date FROM fp LEFT JOIN rp ON (fp.aircraft_type = rp.aircraft_type) AND (fp.board_number = rp.board_number) AND (fp.flight_number = rp.flight_number) AND (fp.pilot_number = rp.pilot_number) AND (fp.report_start_dt = rp.report_start_dt) AND (fp.report_end_dt = rp.report_end_dt) AND (fp.arm_source_file_path = rp.arm_source_file_path) AND (fp.source_file_name = rp.source_file_name) AND (fp.takeoff_datetime = rp.takeoff_datetime) AND (fp.target_date = rp.target_date) WHERE fp.aircraft_type = 'B-737-700' ORDER BY fp.aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, fp.board_number ASC, fp.flight_number ASC
```
</details>

- **`analytics`.`fdr_express_analysis_b737800_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.fdr_express_analysis_b737800_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `approach_system` Nullable(String), `R01` Nullable(String), `R03` Nullable(String), `R14` Nullable(String), `max_R25_R26_R27` Nullable(String), `R30` Nullable(String), `R31` Nullable(String), `R32` Nullable(String), `R37` Nullable(String), `max_R16_R35` Nullable(String), `takeoffSeat` Nullable(String), `landingSeat` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS WITH fp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key IN ('Заход на посадку', 'Система захода на посадку')) AS approach_system, maxIf(flight_parameter_value, flight_parameter_key = 'takeoffSeat') AS takeoffSeat, maxIf(flight_parameter_value, flight_parameter_key = 'landingSeat') AS landingSeat FROM analytics.fdr_express_analysis_flight_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date), rp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(regular_parameter_value, regular_parameter_key = 'R001') AS R001, maxIf(regular_parameter_value, regular_parameter_key = 'R01') AS R01, maxIf(regular_parameter_value, regular_parameter_key = 'R003') AS R003, maxIf(regular_parameter_value, regular_parameter_key = 'R03') AS R03, maxIf(regular_parameter_value, regular_parameter_key = 'R014') AS R014, maxIf(regular_parameter_value, regular_parameter_key = 'R14') AS R14, maxIf(regular_parameter_value, regular_parameter_key = 'R16') AS R16, maxIf(regular_parameter_value, regular_parameter_key = 'R25') AS R25, maxIf(regular_parameter_value, regular_parameter_key = 'R26') AS R26, maxIf(regular_parameter_value, regular_parameter_key = 'R27') AS R27, maxIf(regular_parameter_value, regular_parameter_key = 'R030') AS R030, maxIf(regular_parameter_value, regular_parameter_key = 'R30') AS R30, maxIf(regular_parameter_value, regular_parameter_key = 'R031') AS R031, maxIf(regular_parameter_value, regular_parameter_key = 'R31') AS R31, maxIf(regular_parameter_value, regular_parameter_key = 'R032') AS R032, maxIf(regular_parameter_value, regular_parameter_key = 'R32') AS R32, maxIf(regular_parameter_value, regular_parameter_key = 'R35') AS R35, maxIf(regular_parameter_value, regular_parameter_key = 'R37') AS R37 FROM analytics.fdr_express_analysis_regular_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date) SELECT fp.aircraft_type, fp.board_number, toString(toYear(fp.report_start_dt)) AS year, toString(toMonth(fp.report_start_dt)) AS month, toString(toDayOfMonth(fp.report_start_dt)) AS day, fp.flight_number, fp.pilot_number, formatDateTime(fp.report_start_dt, '%H:%i:%S') AS T0, formatDateTime(fp.report_end_dt, '%H:%i:%S') AS TK, fp.route_note, fp.approach_system, coalesce(rp.R001, rp.R01) AS R01, coalesce(rp.R003, rp.R03) AS R03, coalesce(rp.R014, rp.R14) AS R14, if(greatest(toFloat64OrNull(rp.R25), toFloat64OrNull(rp.R26), toFloat64OrNull(rp.R27)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R25), toFloat64OrNull(rp.R26), toFloat64OrNull(rp.R27)))) AS max_R25_R26_R27, coalesce(rp.R030, rp.R30) AS R30, coalesce(rp.R031, rp.R31) AS R31, coalesce(rp.R032, rp.R32) AS R32, rp.R37, if(greatest(toFloat64OrNull(rp.R16), toFloat64OrNull(rp.R35)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R16), toFloat64OrNull(rp.R35)))) AS max_R16_R35, fp.takeoffSeat, fp.landingSeat, fp.takeoff_datetime, fp.target_date FROM fp LEFT JOIN rp ON (fp.aircraft_type = rp.aircraft_type) AND (fp.board_number = rp.board_number) AND (fp.flight_number = rp.flight_number) AND (fp.pilot_number = rp.pilot_number) AND (fp.report_start_dt = rp.report_start_dt) AND (fp.report_end_dt = rp.report_end_dt) AND (fp.arm_source_file_path = rp.arm_source_file_path) AND (fp.source_file_name = rp.source_file_name) AND (fp.takeoff_datetime = rp.takeoff_datetime) AND (fp.target_date = rp.target_date) WHERE fp.aircraft_type = 'B-737-800' ORDER BY fp.aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, fp.board_number ASC, fp.flight_number ASC
```
</details>

- **`analytics`.`fdr_express_analysis_b737CL_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.fdr_express_analysis_b737CL_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `approach_system` Nullable(String), `R01` Nullable(String), `R03` Nullable(String), `R18` Nullable(String), `max_R20_R21_R22` Nullable(String), `R27` Nullable(String), `R25` Nullable(String), `R29` Nullable(String), `max_R32_R33` Nullable(String), `takeoffSeat` Nullable(String), `landingSeat` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS WITH fp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key IN ('Заход на посадку', 'Система захода на посадку')) AS approach_system, maxIf(flight_parameter_value, flight_parameter_key = 'takeoffSeat') AS takeoffSeat, maxIf(flight_parameter_value, flight_parameter_key = 'landingSeat') AS landingSeat FROM analytics.fdr_express_analysis_flight_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date), rp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(regular_parameter_value, regular_parameter_key = 'R001') AS R001, maxIf(regular_parameter_value, regular_parameter_key = 'R01') AS R01, maxIf(regular_parameter_value, regular_parameter_key = 'R003') AS R003, maxIf(regular_parameter_value, regular_parameter_key = 'R03') AS R03, maxIf(regular_parameter_value, regular_parameter_key = 'R18') AS R18, maxIf(regular_parameter_value, regular_parameter_key = 'R20') AS R20, maxIf(regular_parameter_value, regular_parameter_key = 'R21') AS R21, maxIf(regular_parameter_value, regular_parameter_key = 'R22') AS R22, maxIf(regular_parameter_value, regular_parameter_key = 'R25') AS R25, maxIf(regular_parameter_value, regular_parameter_key = 'R27') AS R27, maxIf(regular_parameter_value, regular_parameter_key = 'R29') AS R29, maxIf(regular_parameter_value, regular_parameter_key = 'R32') AS R32, maxIf(regular_parameter_value, regular_parameter_key = 'R33') AS R33 FROM analytics.fdr_express_analysis_regular_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date) SELECT fp.aircraft_type, fp.board_number, toString(toYear(fp.report_start_dt)) AS year, toString(toMonth(fp.report_start_dt)) AS month, toString(toDayOfMonth(fp.report_start_dt)) AS day, fp.flight_number, fp.pilot_number, formatDateTime(fp.report_start_dt, '%H:%i:%S') AS T0, formatDateTime(fp.report_end_dt, '%H:%i:%S') AS TK, fp.route_note, fp.approach_system, coalesce(rp.R001, rp.R01) AS R01, coalesce(rp.R003, rp.R03) AS R03, rp.R18, if(greatest(toFloat64OrNull(rp.R20), toFloat64OrNull(rp.R21), toFloat64OrNull(rp.R22)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R20), toFloat64OrNull(rp.R21), toFloat64OrNull(rp.R22)))) AS max_R20_R21_R22, rp.R27, rp.R25, rp.R29, if(greatest(toFloat64OrNull(rp.R32), toFloat64OrNull(rp.R33)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R32), toFloat64OrNull(rp.R33)))) AS max_R32_R33, fp.takeoffSeat, fp.landingSeat, fp.takeoff_datetime, fp.target_date FROM fp LEFT JOIN rp ON (fp.aircraft_type = rp.aircraft_type) AND (fp.board_number = rp.board_number) AND (fp.flight_number = rp.flight_number) AND (fp.pilot_number = rp.pilot_number) AND (fp.report_start_dt = rp.report_start_dt) AND (fp.report_end_dt = rp.report_end_dt) AND (fp.arm_source_file_path = rp.arm_source_file_path) AND (fp.source_file_name = rp.source_file_name) AND (fp.takeoff_datetime = rp.takeoff_datetime) AND (fp.target_date = rp.target_date) WHERE fp.aircraft_type IN ('B-737-400', 'B-737-500', 'B-737-500W') ORDER BY fp.aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, fp.board_number ASC, fp.flight_number ASC
```
</details>

- **`analytics`.`fdr_express_analysis_b767_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.fdr_express_analysis_b767_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `approach_system` Nullable(String), `R01` Nullable(String), `R05` Nullable(String), `R28` Nullable(String), `max_R29_R30_R31` Nullable(String), `R36` Nullable(String), `R38` Nullable(String), `R39` Nullable(String), `max_R79_R81` Nullable(String), `takeoffSeat` Nullable(String), `landingSeat` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS WITH fp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key IN ('Заход на посадку', 'Система захода на посадку')) AS approach_system, maxIf(flight_parameter_value, flight_parameter_key = 'takeoffSeat') AS takeoffSeat, maxIf(flight_parameter_value, flight_parameter_key = 'landingSeat') AS landingSeat FROM analytics.fdr_express_analysis_flight_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date), rp AS (SELECT aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date, maxIf(regular_parameter_value, regular_parameter_key = 'R001') AS R001, maxIf(regular_parameter_value, regular_parameter_key = 'R01') AS R01, maxIf(regular_parameter_value, regular_parameter_key = 'R05') AS R05, maxIf(regular_parameter_value, regular_parameter_key = 'R28') AS R28, maxIf(regular_parameter_value, regular_parameter_key = 'R29') AS R29, maxIf(regular_parameter_value, regular_parameter_key = 'R30') AS R30, maxIf(regular_parameter_value, regular_parameter_key = 'R31') AS R31, maxIf(regular_parameter_value, regular_parameter_key = 'R36') AS R36, maxIf(regular_parameter_value, regular_parameter_key = 'R38') AS R38, maxIf(regular_parameter_value, regular_parameter_key = 'R39') AS R39, maxIf(regular_parameter_value, regular_parameter_key = 'R79') AS R79, maxIf(regular_parameter_value, regular_parameter_key = 'R81') AS R81 FROM analytics.fdr_express_analysis_regular_parameters GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date) SELECT fp.aircraft_type, fp.board_number, toString(toYear(fp.report_start_dt)) AS year, toString(toMonth(fp.report_start_dt)) AS month, toString(toDayOfMonth(fp.report_start_dt)) AS day, fp.flight_number, fp.pilot_number, formatDateTime(fp.report_start_dt, '%H:%i:%S') AS T0, formatDateTime(fp.report_end_dt, '%H:%i:%S') AS TK, fp.route_note, fp.approach_system, coalesce(rp.R001, rp.R01) AS R01, rp.R05, rp.R28, if(greatest(toFloat64OrNull(rp.R29), toFloat64OrNull(rp.R30), toFloat64OrNull(rp.R31)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R29), toFloat64OrNull(rp.R30), toFloat64OrNull(rp.R31)))) AS max_R29_R30_R31, rp.R36, rp.R38, rp.R39, if(greatest(toFloat64OrNull(rp.R79), toFloat64OrNull(rp.R81)) IS NULL, NULL, toString(greatest(toFloat64OrNull(rp.R79), toFloat64OrNull(rp.R81)))) AS max_R79_R81, fp.takeoffSeat, fp.landingSeat, fp.takeoff_datetime, fp.target_date FROM fp LEFT JOIN rp ON (fp.aircraft_type = rp.aircraft_type) AND (fp.board_number = rp.board_number) AND (fp.flight_number = rp.flight_number) AND (fp.pilot_number = rp.pilot_number) AND (fp.report_start_dt = rp.report_start_dt) AND (fp.report_end_dt = rp.report_end_dt) AND (fp.arm_source_file_path = rp.arm_source_file_path) AND (fp.source_file_name = rp.source_file_name) AND (fp.takeoff_datetime = rp.takeoff_datetime) AND (fp.target_date = rp.target_date) WHERE fp.aircraft_type = 'B-767' ORDER BY fp.aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, fp.board_number ASC, fp.flight_number ASC
```
</details>

- **`analytics`.`fdr_express_analysis_gulfstream_g4_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.fdr_express_analysis_gulfstream_g4_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `takeoff_weight` Nullable(String), `landing_weight` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS SELECT aircraft_type, board_number, toString(toYear(report_start_dt)) AS year, toString(toMonth(report_start_dt)) AS month, toString(toDayOfMonth(report_start_dt)) AS day, flight_number, pilot_number, formatDateTime(report_start_dt, '%H:%i:%S') AS T0, formatDateTime(report_end_dt, '%H:%i:%S') AS TK, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key = 'Взлетный вес, т') AS takeoff_weight, maxIf(flight_parameter_value, flight_parameter_key = 'Посадочный вес, т') AS landing_weight, takeoff_datetime, target_date FROM analytics.fdr_express_analysis_flight_parameters WHERE aircraft_type = 'Gulfstream IV-SP' GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date ORDER BY aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, board_number ASC, flight_number ASC
```
</details>

- **`analytics`.`fdr_express_analysis_gulfstream_g6_view`** — engine: `View`
<details><summary>create_table_query</summary>

```sql
CREATE VIEW analytics.fdr_express_analysis_gulfstream_g6_view (`aircraft_type` LowCardinality(String), `board_number` LowCardinality(String), `year` String, `month` String, `day` String, `flight_number` LowCardinality(String), `pilot_number` String, `T0` String, `TK` String, `route_note` Nullable(String), `weight_without_fuel` Nullable(String), `takeoff_datetime` DateTime, `target_date` Date) AS SELECT aircraft_type, board_number, toString(toYear(report_start_dt)) AS year, toString(toMonth(report_start_dt)) AS month, toString(toDayOfMonth(report_start_dt)) AS day, flight_number, pilot_number, formatDateTime(report_start_dt, '%H:%i:%S') AS T0, formatDateTime(report_end_dt, '%H:%i:%S') AS TK, maxIf(flight_parameter_value, flight_parameter_key = 'route_note') AS route_note, maxIf(flight_parameter_value, flight_parameter_key = 'Вес самолета без топлива, т') AS weight_without_fuel, takeoff_datetime, target_date FROM analytics.fdr_express_analysis_flight_parameters WHERE aircraft_type = 'Gulfstream 650' GROUP BY aircraft_type, board_number, flight_number, pilot_number, report_start_dt, report_end_dt, arm_source_file_path, source_file_name, takeoff_datetime, target_date ORDER BY aircraft_type ASC, toUInt16(year) DESC, toUInt16(month) DESC, toUInt16(day) DESC, board_number ASC, flight_number ASC
```
</details>

- **`analytics`.`history_of_currency_mv`** — engine: `MaterializedView`
<details><summary>create_table_query</summary>

```sql
CREATE MATERIALIZED VIEW analytics.history_of_currency_mv TO analytics.history_of_currency (`currency` String, `currency_rate` Decimal(18, 6), `currency_name` String, `processing_date` Date, `meta_source` LowCardinality(String), `meta_loading_at` DateTime) AS SELECT JSONExtractString(response_data, 'CharCode') AS currency, toDecimal64(JSONExtractString(response_data, 'Value'), 6) / toDecimal64(JSONExtractString(response_data, 'Nominal'), 0) AS currency_rate, JSONExtractString(response_data, 'Name') AS currency_name, processing_date, meta_source, now() AS meta_loading_at FROM source.history_of_currency
```
</details>

- **`analytics`.`lime_survey_answers_after_flight_mv`** — engine: `MaterializedView`
<details><summary>create_table_query</summary>

```sql
CREATE MATERIALIZED VIEW analytics.lime_survey_answers_after_flight_mv TO analytics.lime_survey_answers_after_flight (`id` UInt32, `start_dt` DateTime, `submit_dt` Nullable(DateTime), `send_dt` DateTime, `email` Nullable(String), `flight_number` LowCardinality(Nullable(String)), `flight_date` Date, `seat` LowCardinality(Nullable(String)), `ticket` Nullable(String), `departure_airport` LowCardinality(Nullable(String)), `arrival_airport` LowCardinality(Nullable(String)), `gender` LowCardinality(Nullable(String)), `birthday` LowCardinality(Nullable(String)), `flight_rating` LowCardinality(Nullable(Int8)), `website_rating` Nullable(Int16), `airport_rating` Nullable(Int16), `board_rating` Nullable(Int16), `is_ticket_purchase` UInt8, `is_addon_services` UInt8, `is_checkin` UInt8, `other1` Nullable(String), `is_with_ticket` UInt8, `is_after_purchase` UInt8, `is_online_checkin` UInt8, `is_at_airport` UInt8, `is_convenience_1` UInt8, `is_info_completeness_1` UInt8, `is_miles_promo_use_1` UInt8, `is_convenience_2` UInt8, `is_info_completeness_2` UInt8, `is_miles_promo_use_2` UInt8, `is_convenience_3` UInt8, `is_info_completeness_3` UInt8, `is_miles_promo_use_3` UInt8, `is_self_checkin` UInt8, `is_counter_checkin` UInt8, `is_boarding_area` UInt8, `other2` Nullable(String), `is_print_bp_self` UInt8, `is_mobile_bp_scan` UInt8, `is_no_options` UInt8, `is_staff_communication` UInt8, `is_baggage_rules_clarity` UInt8, `is_addon_service_speed` UInt8, `is_gate_info` UInt8, `is_display_info_quality` UInt8, `is_cabin_condition` UInt8, `is_onboard_info` UInt8, `is_cabin_crew` UInt8, `other3` Nullable(String), `is_seat_equipment_condition` UInt8, `is_cleanliness` UInt8, `is_captain_announcements` UInt8, `is_pa_clarity` UInt8, `is_info_clarity` UInt8, `is_service_info` UInt8, `is_responsiveness` UInt8, `is_politeness` UInt8, `is_issue_handling` UInt8, `open_feedback` Nullable(String), `processing_dt` DateTime, `meta_source` LowCardinality(String)) AS SELECT id, start_dt AS start_dt, submit_dt AS submit_dt, datestamp AS send_dt, email, flight_number, coalesce(toDateOrNull(concat(extract(replaceRegexpAll(flight_date, '([а-яё])(\\d)', '\\1 \\2'), '(\\d{4})$'), '-', leftPad(toString(indexOf(['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря'], extract(replaceRegexpAll(replaceRegexpAll(flight_date, '(\\d)([а-яё])', '\\1 \\2'), '([а-яё])(\\d)', '\\1 \\2'), '\\d+ (\\S+) \\d{4}'))), 2, '0'), '-', leftPad(extract(replaceRegexpAll(flight_date, '(\\d)([а-яё])', '\\1 \\2'), '^(\\d+)'), 2, '0'))), toDate('1970-01-01')) AS flight_date, seat, ticket, departure_airport, arrival_airport, gender, birthday, toInt8OrNull(flight_rating) AS flight_rating, if((flight_rating >= '5') AND (website_rating IS NULL), 5, toInt8OrNull(website_rating)) AS website_rating, if((flight_rating >= '5') AND (airport_rating IS NULL), 5, toInt8OrNull(airport_rating)) AS airport_rating, if((flight_rating >= '5') AND (board_rating IS NULL), 5, toInt8OrNull(board_rating)) AS board_rating, if(ticket_purchase LIKE 'Y', 1, 0) AS is_ticket_purchase, if(addon_services LIKE 'Y', 1, 0) AS is_addon_services, if(checkin LIKE 'Y', 1, 0) AS is_checkin, other1, if(with_ticket LIKE 'Y', 1, 0) AS is_with_ticket, if(after_purchase LIKE 'Y', 1, 0) AS is_after_purchase, if(online_checkin LIKE 'Y', 1, 0) AS is_online_checkin, if(at_airport LIKE 'Y', 1, 0) AS is_at_airport, if(convenience_1 LIKE 'Y', 1, 0) AS is_convenience_1, if(info_completeness_1 LIKE 'Y', 1, 0) AS is_info_completeness_1, if(miles_promo_use_1 LIKE 'Y', 1, 0) AS is_miles_promo_use_1, if(convenience_2 LIKE 'Y', 1, 0) AS is_convenience_2, if(info_completeness_2 LIKE 'Y', 1, 0) AS is_info_completeness_2, if(miles_promo_use_2 LIKE 'Y', 1, 0) AS is_miles_promo_use_2, if(convenience_3 LIKE 'Y', 1, 0) AS is_convenience_3, if(info_completeness_3 LIKE 'Y', 1, 0) AS is_info_completeness_3, if(miles_promo_use_3 LIKE 'Y', 1, 0) AS is_miles_promo_use_3, if(self_checkin LIKE 'Y', 1, 0) AS is_self_checkin, if(counter_checkin LIKE 'Y', 1, 0) AS is_counter_checkin, if(boarding_area LIKE 'Y', 1, 0) AS is_boarding_area, other2, if(print_bp_self LIKE 'Y', 1, 0) AS is_print_bp_self, if(mobile_bp_scan LIKE 'Y', 1, 0) AS is_mobile_bp_scan, if(no_options LIKE 'Y', 1, 0) AS is_no_options, if(staff_communication LIKE 'Y', 1, 0) AS is_staff_communication, if(baggage_rules_clarity LIKE 'Y', 1, 0) AS is_baggage_rules_clarity, if(addon_service_speed LIKE 'Y', 1, 0) AS is_addon_service_speed, if(gate_info LIKE 'Y', 1, 0) AS is_gate_info, if(display_info_quality LIKE 'Y', 1, 0) AS is_display_info_quality, if(cabin_condition LIKE 'Y', 1, 0) AS is_cabin_condition, if(onboard_info LIKE 'Y', 1, 0) AS is_onboard_info, if(cabin_crew LIKE 'Y', 1, 0) AS is_cabin_crew, other3, if(seat_equipment_condition LIKE 'Y', 1, 0) AS is_seat_equipment_condition, if(cleanliness LIKE 'Y', 1, 0) AS is_cleanliness, if(captain_announcements LIKE 'Y', 1, 0) AS is_captain_announcements, if(pa_clarity LIKE 'Y', 1, 0) AS is_pa_clarity, if(info_clarity LIKE 'Y', 1, 0) AS is_info_clarity, if(service_info LIKE 'Y', 1, 0) AS is_service_info, if(responsiveness LIKE 'Y', 1, 0) AS is_responsiveness, if(politeness LIKE 'Y', 1, 0) AS is_politeness, if(issue_handling LIKE 'Y', 1, 0) AS is_issue_handling, open_feedback, processing_dt, meta_source FROM source.lime_survey_answers_after_flight WHERE email IS NOT NULL
```
</details>

- **`reports`.`smsc_messages_with_organization_mv`** — engine: `MaterializedView`
<details><summary>create_table_query</summary>

```sql
CREATE MATERIALIZED VIEW reports.smsc_messages_with_organization_mv TO reports.smsc_messages_with_organization (`id` String, `int_id` String, `last_date` DateTime, `last_timestamp` UInt32, `send_date` DateTime, `send_timestamp` UInt32, `processing_date` Date, `phone` String, `sender_id` String, `reseller_login` String, `mccmnc` String, `country` String, `operator` String, `operator_orig` String, `region` String, `status` UInt16, `status_name` String, `flag` UInt16, `type` UInt8, `format` UInt8, `err` UInt64, `message` String, `sms_cnt` Nullable(UInt8), `cost` Decimal(10, 3), `crc` UInt32, `organization` String, `organization_id` String, `comment` String, `send_retry` UInt8) AS SELECT * FROM analytics.smsc_messages_with_organization
```
</details>


### 2.4 `reports.amos_heli_rotables_components_status` — сопоставление с `Status_Components_DWH.xlsx`

| column | в DWH | в xlsx | совпадает |
|---|---|---|:---:|
| LL | True | False | False |
| OH | True | False | False |
| OH_threshold | True | False | False |
| ac_typ | True | True | True |
| ac_type_i | True | True | True |
| address_i | True | True | True |
| condition | True | True | True |
| ll | False | True | False |
| location | True | True | True |
| meta_loading_at | True | False | False |
| meta_source | True | False | False |
| mfg_date | True | True | True |
| oh | False | True | False |
| oh_at_date | True | True | True |
| oh_threshold | False | True | False |
| owner | True | True | True |
| partno | True | True | True |
| partseqno_i | True | True | True |
| ppr | True | True | True |
| processing_dt | True | False | False |
| psn | True | True | True |
| removal_date | True | True | True |
| report_date | True | False | False |
| serialno | True | True | True |
| shop_visit_counter | True | True | True |
| sne | True | True | True |
| target_date | True | True | True |

## 3. AMOS source-таблицы (из `data_input/analytics/Database-Description39331920032025_0.csv`)
### 3.1 `rotables`
> header data for rotables

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [I] | purch_recdetailno_i | INT | int | foreign key to od_rec_detail.recdetailno_i of the last purchase. |
| [I] | cloned_serialno | ALPHANUM | univarchar(20) | This column is only filled if the current rotable is a clone of another one. The content of this column is the serial number of the clone rotable. The serial number of the current rotable shall always start with "UNK-" |
| [U] | material_lifecycle_id | INT | int | Unique reference number checked when the part is received/booked on an order  so that the correct owner change/exchange can be performed at the end of the cycle |
| [I] | config_id | INT | int | Link to the aircraft configuration. |
| [I] | assembly_locid_pk | INT | int | Link to the assembly position (cf. aircraft configuration).  |
| [I] | linked_contract | CN | int | the id of the contract that defines part use restrictions |
| [U,I,I] | partno | PA | univarchar(32) | link to part |
| [U,I] | serialno | ALPHANUM | univarchar(20) | serialnumber of rotable |
| [P,I] | psn | RO | int | unique key of table rotables |
| [I] | owner | VD | univarchar(12) | owner of the rotable, see table address. |
|  | del_date | DATE | int | delivery date of the rotable. |
|  | mfg_date | DATE | int | manufacture date of the rotable |
| [I] | partnonew | PA | univarchar(32) | partnumber of the higher rotable |
| [I] | serialnonew | RO | univarchar(20) | serialnumber of higher rotable. |
| [I] | orderno | ORDER | univarchar(12) | last orderno for the rotable. |
|  | orderdate | DATE | int | last order date of the rotable. |
| [I] | ac_registr | AC | univarchar(6) | aircraft where the rotable is installed, if it is. |
| [I] | pool_ac_registr | POOLAIRC | univarchar(6) | pool aircraft registration |
|  | location | POS | univarchar(20) | It is either the position on aircraft if the rotable is installed, or the location in the store or the address of the part.  |
|  | locid_pk | INT | int | Link to the aircraft position (cf. aircraft configuration).  |
| [I] | locationno_i | INT | int | link to location where the rotable is at a time. |
| [I] | labelno | LB | int | labelno which the rotable was received. |
| [I] | releaseno | ALPHANUM | univarchar(40) | release number of the rotable. |
|  | tree_position | ALPHANUM | univarchar(20) | position in tree - foreign key to tree_position.code |
|  | tree_type | ALPHANUM | univarchar(8) | refers to tree_reference.tree_type |
|  | date_inst | DATE | int | installation date of the rotable. |
|  | mfg_unknown | BOOL | univarchar(2) | contains `Y` when manufacture date is unknown. |
|  | oh_at_msn | DOUBLE | float | Flight Hours in MINUTES at the last overhaul. |
|  | oh_at_tsn | INT | int | Flight Hours in HOURS at the last overhaul. |
|  | oh_at_tsn_unknown | BOOL | univarchar(2) | contains `Y` when OH TSN is unknown. |
|  | oh_at_csn | INT | int | cycles since new at the last overhaul. |
|  | oh_at_csn_unknown | BOOL | univarchar(2) | contains `Y` when OH CSN is unknown. |
|  | oh_at_date | DATE | int | date of the last overhaul. |
|  | oh_at_date_unknown | BOOL | univarchar(2) | contains `Y` when OH date is unknown. |
|  | condition | ALPHANUM | univarchar(2) | actual condition of the rotable. |
|  | mod_status | ALPHANUM | univarchar(120) | plate mod. status of rotable. |
|  | hardware_mod_status | ALPHANUM | univarchar(120) | hardware mod. status of rotable. |
|  | software_mod_status | ALPHANUM | univarchar(120) | software mod. status of rotable. |
|  | database_mod_status | ALPHANUM | univarchar(120) | database mod. status of rotable. |
|  | processor | VD | univarchar(12) | pool processor of rotable. |
|  | int_remarks | ALPHANUM | unitext(1073741823) | internal remarks of rotable (this will be printed no where) |
| [I] | shipdetailno_i | INT | int | link to shipment detail where the rotable is shipped. |
|  | rep_at_msn | DOUBLE | float | Flight Hours in MINUTES at the last repair. |
|  | rep_at_tsn | INT | int | Flight Hours in HOURS at the last repair. |
|  | rep_at_tsn_unknown | BOOL | univarchar(2) | contains `Y` when REP TSN is unknown. |
|  | rep_at_csn | INT | int | cycles since new at the last repair. |
|  | rep_at_csn_unknown | BOOL | univarchar(2) | contains `Y` when REP CSN is unknown. |
|  | rep_at_date | DATE | int | date of last repair. |
|  | rep_at_date_unknown | BOOL | univarchar(2) | contains `Y` when REP date is unknown. |
|  | mod_at_msn | DOUBLE | float | Flight Hours in MINUTES at the last modification. |
|  | mod_at_tsn | INT | int | Flight Hours in HOURS at the last modification. |
|  | mod_at_tsn_unknown | BOOL | univarchar(2) | contains `Y` when MOD TSN is unknown. |
|  | mod_at_csn | INT | int | cycles since new at the last modification. |
|  | mod_at_csn_unknown | BOOL | univarchar(2) | contains `Y` when MOD CSN is unknown. |
|  | mod_at_date | DATE | int | last modification date. |
|  | mod_at_date_unknown | BOOL | univarchar(2) | contains `Y` when MOD date is unknown. |
|  | res_type | ALPHANUM | univarchar(8) | facility reservation type |
|  | shop_visit_counter | INT | int | actual shop visit counter . |
|  | fa_case | INT | int | financial case |
|  | fa_qty | INT | int | financial qty |
|  | pma | ALPHANUM | univarchar(1) | this field is used to mark rotables as Part Manufacturer Approval. true when field contains `Y`. |
| [I] | projectno_i | PR | int | link to project |
| [I] | rec_detailno_i | INT | int | foreign key to od_rec_detail.recdetailno_i |
| [I] | outgoing_detailno_i | INT | int | foreign key to od_detail.detailno_i This column is filled with the detailno_i of an outgoing order (loan or pool) DEPRECATED: should never be filled or read anymore |
|  | plate_number | ALPHANUM | univarchar(50) | The exact part number located on the physical part. It can defer from the AMOS part number |
| [I,I] | entityno_i | FAEA | int | Foreign Key references to entity_header (entityno_i) |
|  | rotable_status | INT | int | Logistic status of the rotable. Depending on the status, actions on the rotable installation, sell, etc...) can be allowed/disallowed . |
| [I] | origin_entityno_i | FAEA | int | Stores the entityno_i that had the rotable before its booking on a transaction between entities (e.g. the entity of the loan provider during the loan process) |
|  | origin_fa_case | INT | int | financial case of the rotable in the entity referenced by origin_entityno_i |
|  | origin_fa_qty | INT | int | financial qty of the rotable in the entity referenced by origin_entityno_i |
|  | expiry_date | DATE | int | Shelf expiry date of this rotable. |
|  | shelf_inspection_type | ALPHANUM | univarchar(2) |  |
|  | shelf_inspection_date | DATE | int |  |
|  | is_managed | BOOL | univarchar(1) | Defines if a rotable is managed or non-managed.  |
|  | mutation | DATE | int | The mutation date of this record as AMOS date. |
| [I] | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE | int | The creation date of this record as AMOS date. |

### 3.2 `aircraft`
> Table of aircraft.

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
|  | init_status | INT_0 | int | status of the aircraft during MP activations: 0 = normal, 1 = blocked, 2 = in progress |
|  | first_flight_date | DATE | int | The date of the first flight of the aircraft, used for time requirement baselines. |
|  | airw_reg_number | ALPHANUM | univarchar(20) | Number of Certificate of Registration |
|  | airw_reg_exp_date | DATE | int | Expire date of Certificate of Registration |
|  | airw_reg_issue_date | DATE | int | Issue date of Certificate of Registration |
|  | airw_cert_number | ALPHANUM | univarchar(36) | Number of Airworthiness Certificate |
|  | is_mod_controlled | ALPHANUM | univarchar(1) | Define if the Aircraft is included into Modcontrol. |
| [I] | uuid | ALPHANUM | univarchar(36) | Globally unique identifier |
| [P,I] | ac_registr | AC | univarchar(6) | Aircraft registration |
|  | ac_registr_prefix | ALPHANUM | univarchar(6) | AC registration prefix |
|  | object_type | ALPHANUM | univarchar(1) | Object type. Eg.: H for helicopter => Used to define the Resource Type in Resource Management module |
| [U] | aircraftno_i | INT | int | The internal aircraft number |
| [I] | serialno | ALPHANUM | univarchar(20) | Aircraft serialnumber |
|  | cec | ALPHANUM | univarchar(20) | Customer Effectivity Code  |
|  | manual_owner | VD | univarchar(12) | Owner of the aircraft manual |
|  | last_station | STA | univarchar(4) | Last station of the aircraft |
|  | last_gate | ALPHANUM | univarchar(6) | Last gate / parking position of the aircraft |
|  | last_gate_override | ALPHANUM | univarchar(6) | The last_gate field is filled by the leg import. If you manually want to override the position (e.g. A/C is towed to the hangar) this field is used. |
|  | description | ALPHANUM | univarchar(36) | Description of the aircraft |
|  | special_name | ALPHANUM | univarchar(36) | Any kind of special name |
|  | line_number | ALPHANUM | univarchar(20) | Line number of the aircraft |
|  | prod_number | ALPHANUM | univarchar(10) | Production number |
|  | variable_number | ALPHANUM | univarchar(20) | varaible Serial Number |
|  | effectivity | ALPHANUM | univarchar(6) | Effectivity |
| [I] | ac_typ | AT | univarchar(6) | Aircraft type |
| [I] | ac_subtype | AT | univarchar(6) | Aircraft subtype |
| [I] | ac_model | AT | univarchar(6) | Aircraft model |
|  | doc_ac_type | ALPHANUM | univarchar(6) | Document Aircraft Type |
|  | ac_group | ALPHANUM | univarchar(12) | Aircraft group |
|  | mfg_date | DATE | int | Manufacturing date |
|  | del_date | DATE | int | Delivery date |
|  | airw_del_date | DATE | int | Airworthiness delivery date |
|  | airw_exp_date | DATE | int | Airworthiness expiry date |
|  | expire_date | DATE | int | Expire date of the aircraft |
|  | fms_date | DATE | int | Effectivity FMS Database Date |
|  | owner | VD | univarchar(12) | Technical Owner of the aircraft |
| [I] | operator | VD | univarchar(12) | Operator of the aircraft |
| [I] | maintenance_provider | VD | univarchar(12) | Maintenance provider of the aircraft |
|  | fa_costcenter | VD | univarchar(10) | Costcenter |
| [I] | claim_address_i | AD | int | Addressnumber |
| [I] | homebase | STA | univarchar(4) | Homebase of the aircraft |
|  | remarks | ALPHANUM | unitext(1073741823) | Any kind of remarks |
| [I] | wo_prefix_no | INT | int | Prefix number for workorders |
|  | non_managed | BOOL | univarchar(1) | Non-managed aircraft (Y/N). |
| [I] | investigation | BOOL | univarchar(1) | Set the aircraft to "under investigation" will filter the aircraft out in case the user has not the global license to have access to aircraft that are currently "under investigation". |
|  | financial_type | ALPHANUM | univarchar(1) | Financial type: O = Operator, A = Asset, C = Customer |
| [I] | manufacturer | VD | univarchar(12) | Manufacturer of the aircraft |
|  | max_taxi_weight | DOUBLE | float | Maximum taxi weight |
|  | max_zero_fuel_weight | DOUBLE | float | Maximum zero fuel weight |
|  | dry_operating_weight | DOUBLE | float | Dry operating weight |
|  | projectno_in_pickslip | ALPHANUM | univarchar(1) | set the projectnumber-field in program "Pickslip Request" as mandatory field for this aircraft (in combination with Parameter 882)  |
|  | sched_flights | BOOL | univarchar(1) | Scheduled flights |
|  | is_ops_imported | ALPHANUM | univarchar(1) | Define here whether legs of this A/C are imported or get entered manually. Currently this will only influence module Flight Log 2 (legs can not be edited if this flag is set). |
| [I] | ac_config_no_i | INT | int | Contains the primary key of the aircraft configuration that is used. If no aircraft configuration is used, contains NULL. |
|  | flightlog_number_prefix | ALPHANUM | univarchar(6) | The flightlog number prefix to use for this A/C when having parameter 1572 set to "Y" in APN 1573 |
|  | mutation | DATE | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
| [I] | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE | int | The creation date of this record as AMOS date. |
|  | asset_owner | VD | univarchar(12) | The Asset Owner/Lessor of the aircraft (as opposed to the technical owner). This value is INFORMATION ONLY. |
| [I] | auth_address_i | AD | int | foreign key to organization approval authority address |
|  | mode_s_code | ALPHANUM | univarchar(6) | Mode S Code (unique transponder address of the aircraft), in hexadecimal representation |

### 3.3 `part`
> Keys

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
|  | valid_until | DATE_INT | int | Date until which the part customs definition is valid. |
| [I] | end_use | ALPHANUM | univarchar(2) | End use that should always be used for this part - customs. |
| [P] | part_customno_i | INT | int | Primary Key |
| [I] | partno | PA | univarchar(32) | reference to the Table Part |
| [I] | custom_addressno_i | INT | int | FK Address : An addressno_i with the property `CUSTOM AUTHORITY` |
|  | type_value | ALPHANUM | univarchar(1) | define if the value if the value is a fixed value F or a chosen value C |
|  | value | DOUBLE | float | value of the part  |
| [I] | currency | ALPHANUM | univarchar(4) | currency of the custom |
|  | weight | DOUBLE | float | weight of the part |
| [I] | measure_unit | ALPHANUM | univarchar(4) | Measure Unit of the weight |
| [I] | commodity_code | ALPHANUM | univarchar(20) | The commodity code corresponds to a type of part  |
| [I] | classification_no_i | INT | int | Describes the classification of this part. |
|  | description | ALPHANUM | univarchar(64) | description of the part for the custom |
|  | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |

### 3.4 `part_requirement`
> Contains the requiremets defined for a part.

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [I] | ac_rating_categoryno_i | INT | int |  |
|  | pro_rata_calc | BOOL | univarchar(1) | True if is pro-rata calculation (valid only for rules), otherwise false. |
|  | on_shelf | BOOL | univarchar(1) | True if is performable on shelf, otherwise false. |
|  | on_wing | BOOL | univarchar(1) | True if is performable on wing, otherwise false. |
|  | description | ALPHANUM | univarchar(36) | Description of the part requirement. |
|  | title | ALPHANUM | univarchar(38) | Requirement title. |
| [I] | type | INT | int | Requirement type, foreign key of the requirement table. (e.g. BO: borescope inspection, CA: calibre...).  |
| [P,I] | part_requirementno_i | INT | int | First column of the primary key |
| [I,I] | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
| [I,I] | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |

### 3.5 `requirement_type`
> Requirement Type of a Part Requirement

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [P] | requirement_typeno_i | INT | int |  |
| [U] | requirement | ALPHANUM | univarchar(2) | The requirement code, e.g. "OH" for overhaul or "SC" for scrap. |
|  | description | ALPHANUM | univarchar(36) | The description e.g. "OVERHAUL". |
|  | life_limit | BOOL | univarchar(1) | Indicator whether the part is life limited `Y` or not `N`. In case requirement is marked as "life limit" it cannot be reported back. |
|  | mutation | DATE | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE | int | The creation date of this record as AMOS date. |
|  | uuid | ALPHANUM | univarchar(36) | Globally unique identifier |

### 3.6 `requirement_header`
> default table description

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [I] | interval_type | ALPHANUM | univarchar(1) | L : intervals defined with interval codes/letter checks. I : detailed interval definition |
| [U,I] | event_type | ALPHANUM | univarchar(8) |  |
| [U,I,I,I] | event_key | INT | int |  |
| [I] | effectivity_headerno_i | INT | int | foreign key of event_effectivity_header.effectivity_headerno_i |
| [P] | requirement_headerno_i | INT | int | First column of the primary key |
|  | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |

### 3.7 `event_effectivity`
> Table used to store all the maintenance event effectivities

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [I] | effectivity_headerno_i | INT | int |  |
|  | effectivity_type | ALPHANUM | int | Type of the Effectivity (0 = Aircraft, 1 = Component) |
| [I] | revision_effectivityno_i | EFF | int | Identifier of the next Effectivity Revision |
|  | description | ALPHANUM | univarchar(70) | short description for an effectivity |
| [P] | effectivityno_i | EFF | int | Unique identifier for any Effectivity |
| [I] | parent_effectivityno_i | EFF | int | Identifier for the Parent Effectivity |
| [I] | higher_effectivityno_i | EFF | int | Identifier for the Higher Effectivity |
|  | title | ALPHANUM | univarchar(70) | Title of the effectivity |
| [I,I] | aircraft_typ | AT | univarchar(6) | Applicable aircraft type |
| [I] | aircraft_subtyp | AT | univarchar(6) | Applicable aircraft subtype |
| [I] | aircraft_model | AT | univarchar(6) | Applicable aircraft model |
|  | document_aircraft_typ | ALPHANUM | univarchar(6) | Applicable document aircraft typ |
| [I] | partno | PA | univarchar(32) | identifier for the linked Part(s) |
|  | serialno_type | ALPHANUM | univarchar(2) | S: Serial Number P: Production Number L: Line Number E: Effectivity Number V: Variable Number |
|  | changeable | BOOL | univarchar(1) | effectivity changeable boolean |
|  | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |

### 3.8 `event_effectivity_rules`
> table used to store all the effectivity rules

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
|  | amendment_letter | ALPHANUM | univarchar(10) | Amendment Letter |
|  | aircraft_type | AT | univarchar(6) |  |
| [P] | effectivityno_i | EFF | int | unique identifier for the linked effectivity |
|  | aircraft_rating | ALPHANUM | univarchar(10) | aircraft rating |
| [I] | aircraft_owner | AD | int | aircraft owner |
| [I] | aircraft_operator | AD | int | aircraft operator |
|  | aircraft_position | ALPHANUM | univarchar(14) | aircraft position |
|  | country | ALPHANUM | univarchar(4) | country |
| [I] | aircraft_mission_config | INT | int | aircraft mission configuration |
|  | aircraft_manuf_before | DATE_INT | int | before aircraft manufacture date |
|  | aircraft_manuf_after | DATE_INT | int | after aircraft manufacture date |
| [I] | rotable_owner | AD | int | part owner |
| [I] | higher_part | PA_INT | int | higher part |
| [I] | highest_part | PA_INT | int | highest part |
|  | rotable_manuf_before | DATE_INT | int | before rotable manufacture date |
|  | rotable_manuf_after | DATE_INT | int | after rotable manufacture date |
|  | threshold | INT | int | Value of the threshold, this value is in the dimension of the contents of threshold_dim. |
|  | threshold_dim | ALPHANUM | univarchar(2) | Dimension of the treshold |
|  | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |

### 3.9 `event_effectivity_sns`
> serialnumbers applicable for the event effectivity

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [I] | effectivityno_i | EFF | int | Identifier for the linked effectivity |
| [P] | effectivity_snno_i | INT | int | Unique pk of the table |
|  | range_type | ALPHANUM | univarchar(1) | Range type of the entry A: ALL SERIALNUMBERS AFFECTED R: FULL QUALIFIED SERIALNUMBER-RANGE S: ONLY SINGLE(-RANGES) |
|  | serialno_from | ALPHANUM | univarchar(20) | From SN |
|  | serialno_to | ALPHANUM | univarchar(20) | To SN |
|  | serialno_shift | ALPHANUM | univarchar(1) | shift option to format the SN From and To to the right or to the left into the string of 20 characters. |
|  | include_exclude | ALPHANUM | univarchar(1) | I: Include E: Exclude |
|  | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |

### 3.10 `applicability`
> Applicability Table

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [I,I,I] | group_key | INT | int | Internal key for the applicability group TMP -> null MMP -> MMP key  OMP -> OMP key MOD -> null |
| [I,I] | group_type | ALPHANUM | univarchar(8) | Internal Code of the applicability group TMP(Template) MMP(Maintenance Program) OMP(Operator Maintenance Program) MOD(Modification) |
| [I] | planable | BOOL | univarchar(1) | boolean to indicate if this entry could be assigned or not. |
| [I] | applicable | BOOL | univarchar(1) | applicability of the reference (Aircraft / Rotable) for the specified Effectivity |
| [I,I,I,I,I] | ref_key | INT | int | Unique ID of linked Aircraft / Rotable |
| [I,I,I,I] | ref_type | MIME | univarchar(8) | Internal Code of linked Aircraft / Rotable |
| [I,I] | effectivityno_i | EFF | int | Identifier of the associated Effectivity |
| [P] | applicabilityno_i | INT | int | First column of the primary key |
|  | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |

### 3.11 `treq_interval`
> one interval per dimension/counter

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [I] | dimension_groupno_i | INT | int | Link to treq_dimension_group |
| [P] | intervalno_i | INT | int | primary key of this table |
| [I] | interval_groupno_i | INT | int | link to a time requirement interval group |
|  | dimension_type | ALPHANUM | univarchar(2) | I : Interval W : Threshold B : Before (deprecated, not used anymore, no logic for calculation) |
| [I] | counter_defno_i | INT | int | counter link:  replaces previous dimension value table: counter_definition.counter_defno_i |
|  | unit | ALPHANUM | univarchar(2) | unit represents the special unit for the defined counter (dimension).  e.g. Days -> unit MT (month), unit YR (years) |
|  | accuracy | ALPHANUM | univarchar(1) | The accuracy of calendar time intervals, such as minute, hours, day, week, month, year. |
|  | amount_interval | INT | int | value of time requirement interval |
|  | due_at | INT | int | due at is a fix amount when the event becomes due: this is used with the dimension_type `B`: Before |
|  | neg_tolerance | INT | int | allowed negative tolerance |
|  | pos_tolerance | INT | int | allowed positive tolerance |
|  | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |

### 3.12 `treq_time_requirement`
> header table of every time requirement linked to a specific event

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [P] | timerequirementno_i | INT | int | primary key of table |
| [I,I] | event_type | ALPHANUM | univarchar(8) | amos type of the linked event	 |
| [I,I] | event_key | INT | int | primary key of the linked event |
|  | ac_group | ALPHANUM | univarchar(12) | distinguish between time requirements for the event and time requirements assigned to a specific ac-group |
|  | type | ALPHANUM | univarchar(3) | classification of defined time requirements.  e.g.  -OP -MRB -Notes |
|  | auto_reportback | ALPHANUM | univarchar(1) | Y: defines the possibility to perform the reporting automatically after closing the related workorder N: perform reporting back manually. |
|  | title | ALPHANUM | univarchar(20) | time requirement name |
|  | description | ALPHANUM | univarchar(70) | short description for the time requirement |
|  | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
| [I] | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |

### 3.13 `treq_event_link`
> This table describes the links between time requirements and all the events where the time requirement is used.

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [P] | eventlinkno_i | INT | int | pk of this table |
| [U,I,I] | event_type | ALPHANUM | univarchar(8) | amos type of the linked event	 |
| [U,I,I,I] | event_key | INT | int | primary key of the linked event |
| [I] | ac_registr | AC | univarchar(6) | unique id for aircraft |
|  | psn | RO | int | unique id for rotables |
| [U,I] | timerequirementno_i | INT | int | linked time requirement definition |
|  | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |

### 3.14 `wo_event_link`
> Links the workorder with the maintenance event. Previously done in check_pending, doc_pending,...

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [I,I] | pr_effectivityno_i | INT | int | Prime key of the replacment events. |
| [U,I,I,I] | planable_status | INT | int | This column replaces the column `planable` which has been set as deprecated. The new planable_status = 0 is equivalent to the former planable = `Y` Any other planable_status value indicates that the event is not planable. It is equivalent to the former planable = `N` |
| [I,I] | effectivity_linkno_i | EFL | int | Effectivity linked to the Maintenance Event. This might differ from the active effectivity linked to the Maintenance Requirement. From 10.90, this is only filled for Documents entries identified with event_type = DOC_EFF |
| [U,I] | mevt_headerno_i | INT | int | FK to the maintenance event header |
|  | future_event_reinit_mode | ALPHANUM | univarchar(1) | Indicates if the future maintenance events of the current pending should be reinitialised and how. Following values are allowed: Y: Future Events should be reinitialised U: Future Events should be initialised for Unlimited Time Requirements E: Only existing Future Events should be reinitialised N: Future Events should not be reinitialised null: Future Events should not be reinitialised This column is introduced from 10.90 onwards. |
| [P,I,I,I] | event_perfno_i | INT | int | Link to wo_header table |
| [I,I,I,I,I,I,I,I,I] | event_type | MIME | univarchar(8) | Type related to each maintenance event. Check: MEC Document: MED Part Requirement: MEP Part Replacment: MER Taskcard: MET |
|  | event_key | INT | int | only used for Replacement events -> event_type = MEP  All other Maintenance Events are managed "ONLY" with the effectivity_linkno_i  |
| [I,I,I,I,I,I] | event_key_parent | INT | int | Is the key of the direct requirement linked to the event. TASKCARD: msc_requirement_tc_link.req_linkno_i CHECH: check_type_int.check_intno_i DOCUMENT: doc_header.docno_i PART REQUIREMENT: part_requirement.part_requirementno_i PART REPLACEMENT: part.partseqno_i |
| [I,I] | event_key_root | INT | int | Is the key of the root requirement linked to the event.   TASKARD: msc_req_link_header.req_link_headerno_i (msc_req_link_header is the root of msc_requirement_tc_link) CHECK: check_type.checkno_i (check_type is the root of check_type_int) ------------------------------------------------------------------------------------------------------------------------------- For the bellow requirement event_key_root = event_key_parent no hierarchy used for events -------------------------------------------------------------------------------------------------------------------------------- DOCUMENT: doc_header.docno_i PART REQUIREMENT: part_requirement.part_requirementno_i PART REPLACEMENT: part.partseqno_i |
| [U,I,I,I,I,I,I,I] | pending_status | INT | int | Flag for pending status. Value greater than zero: entry is a future event Value less than zero: entry is a historic event Value is equal to zero: entry is the current pending entry |
| [I] | event_status | ALPHANUM | univarchar(1) | The status of the event.  ON ATTRITION(A), POSTPONED OPEN (B), CLOSED (C), DELETE ASSIGNMENTS (D), EVENT TRIGGERED (E), EXFACTORY (F), TERMACTION OPEN (G), ROTABLE DRIVEN ACCOMPLISHED (H), INFORMATION ONLY (I), REJECTED BY OPERATOR ,(J), ROTABLE DRIVEN NOT ACCOMPLISHED (K), LOGBOOK CONTROLLED (L), TASKCARD CONTROLLED (M), NOT APPLICABLE (N), OPEN (O), PARTLY PERFORMED (P), PARTREQ CONTROLLED (Q), REPETITIVE (R), DOCUMENT IN PREPARATION (S), TERMINATED BY THIRD PARTY (T), CANCELLED (U), PERF BY PEVIOUS OPERATOR (V), SEE PREV DOC STATUS (W), EXTERNALLY CONTROLLED (X), REOPEN (Y), SUPERSEDED (Z), UNKNOWN (!), UNKNOWN EMPTY ( ), NOT ASSIGNED (-), UNKNOWN ACTYPE (?), NOT INSTALLED (/), NOT PERFORMED (0) AC GROUP CHANGED (1), AC DEACTIVATED (2) |
|  | interval_usage | PROGRESS | int | The interval usage after the event is performed. This is the min or max value, depending on first_later logic. |
|  | auto_report_back | BOOL | univarchar(1) | If set to "Y" the event will be reported back automatically when the workorder is closed. |
|  | status_remarks | ALPHANUM | unitext(1073741823) | The info text for a status change. |
| [I] | revision | ALPHANUM | univarchar(8) | Revision Number of a Document / Taskcard |
|  | revision_status | ALPHANUM | univarchar(2) | Revision Status of a Taskcard |
|  | next_revision | ALPHANUM | univarchar(8) | Next Revision number of a Taskcard |
|  | next_revision_status | ALPHANUM | univarchar(2) | Next Revision Status of a Taskcard |
|  | event_name | ALPHANUM | univarchar(100) | Name of the Maintenance Event. It will be used to globally improve performances of the Maintenance Event fwk without using the associated Maintenance Requirements |
|  | event_display | ALPHANUM | univarchar(300) | Display of the Maintenance Event. It will be used to globally improve performances of the Maintenance Event fwk without using the associated Maintenance Requirements |
|  | preinit_used | BOOL | univarchar(1) | Indicates if pre-initialisation was used to initialise this pending. |
|  | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |
| [I] | taskcard_type | ALPHANUM | univarchar(1) | Indicates the Type of Taskcard used |
|  | initialisation_date | DATE_INT | int | Initialisation Date of the pending (only filled for Part Requirements at Receiving) |
|  | initialisation_time | TIME | int | Initialisation Time of the pending (only filled for Part Requirements at Receiving) |

### 3.15 `wo_header`
> This table is used to save the following objects which are different in the characteristics and behaviour: -	Workorders  -	Work Templates since 9.50 -	Event Pendings since 9.70 - 	Jobcards/Customer Requirements since 11.40 - 	Shopcards/Non-Technical Tasks since 19.12 How can you evaluate the difference?  Following the criteria to do so per object: -	A Workorder is identified by: 	o	wo_header.type = `S` OR `M` OR `C` OR `P` 	o	wo_header.event_perfno_i > 0  	o	wo_header.workorderno_display is not null -	A Work Template is identified by: 	o	wo_header.type = `T` 	o	wo_header.event_perfno_i < 0 -	An Event Pending is identified by: 	o	wo_header.type = `PD` 	o	wo_header.event_perfno_i > 0 	o	wo_header.workorderno_display = null -	A Jobcard is identified by: 	o	wo_header.event_type = `JC` 	o	wo_header.type = `PD` 	o	wo_header.event_perfno_i > 0 	o	wo_header.workorderno_display = null -	A Customer Requirement is identified by: 	o	wo_header.event_type  IN(`CT`,  `CD`, `CC`, `CP`) 	o	wo_header.type = `PD` 	o	wo_header.event_perfno_i > 0 	o	wo_header.workorderno_display = null -	A Shopcard is identified by: 	o	wo_header.event_type  = `SH` 	o	wo_header.type = `PD` 	o	wo_header.event_perfno_i > 0 	o	wo_header.workorderno_display = null -	A Non-Technical Task is identified by: 	o	wo_header.type = `NT` 	o	wo_header.event_perfno_i > 0  	o	wo_header.workorderno_display = null 	o	wo_header.ac_registr = `NONTEC`

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
|  | mro_measurement_result | ALPHANUM | unitext(1073741823) | Measurement Results from MRO |
|  | workorderno_customer | ALPHANUM | univarchar(120) | As a production planner I want to see the customer workorder number related to my internal workorder number so that I can easily identify the relationship.   Business Requirements: An MRO must be able to identify the corresponding airline Workorder number against their own internal workorder number. At time of importing the airline workorder number, the MRO system will generate an own internal workorder number and these two must be easily identified as belonging to each other.  |
|  | executable | BOOL | univarchar(1) | In case of a customer requirement it marks the entry if it is an executable event (`Y`) or not. For non customer requirement entries the entry is not set. |
| [P,I,I,I] | event_perfno_i | WO | int | Unique identifier for any planning event this value is negative for templates |
| [I] | ata_chapter | ATA | univarchar(12) | ata chapter |
| [I,I,I] | state | ALPHANUM | univarchar(2) | state of a Workorder O = open Workorder C = closed Workorder |
| [I,I,I,I] | ac_registr | AC | univarchar(6) | this field contains the Aircraft registration related to the Workorder |
| [I] | issue_date | DATE | int | this field contains the Issue Date of the workorder |
|  | issue_time | INT | int | this field contains the Issue Time of the workorder |
|  | issue_sign | SG | univarchar(8) | this field contains the Issue Sign of the workorder |
|  | issue_station | STA | univarchar(4) | this field contains the Issue Station of the workorder |
|  | issue_leg | ALPHANUM | univarchar(9) | this field contains the Issue Leg of the workorder |
|  | issue_tah | INT | int | this field contains the Issue TAH of the workorder |
|  | issue_tac | INT | int | this field contains the Issue TAC of the workorder |
|  | issue_flt_from | STA | univarchar(4) | departure station of the flight |
|  | issue_flt_to | STA | univarchar(4) | arrival/issue station of the flight |
| [I] | hil | BOOL | univarchar(1) | Hold Item List, Y or not set |
| [I] | type | ALPHANUM | univarchar(2) | Workorder type: P = Pirep C = Cabin M = Maintenance S = Scheduled Special types are not handled as workorder!!! T = Template PD = Pending NT = Non-Technical Task |
| [I] | cannibalized | BOOL | univarchar(1) | Cannibalization, Y or not set |
|  | mech_sign | SG | univarchar(8) | this field is in the closing section: mechanic sign,  necessary to close a work order |
|  | release_sign | SG | univarchar(8) | this field is in the closing section: closing sign, necessary to release a work order |
|  | release_sign2 | SG | univarchar(8) | this field is in the closing section: closing sign 2, necessary if old double inspection is required |
|  | release_station | STA | univarchar(4) | this field is in the closing section: closing station, necessary to close a work order |
|  | release_tah | INT | int | this field is in the closing section: Total Aircraft Hours at closing, necessary to close a work order |
|  | release_tac | INT | int | this field is in the closing section: Total Aircraft Cycles at closing, necessary to close a work order |
|  | release_time | ALPHANUM | univarchar(4) | this field is in the closing section: Time at closing, necessary to close a work order Be aware that the time is saved in the old powerhouse time format e.g. 1445 equals 14:45 |
| [I] | closing_date | DATE | int | this field is in the closing section: Date at closing, necessary to close a work order |
|  | closing_leg | ALPHANUM | univarchar(9) | this field is in the closing section: Leg at closing, necessary to close a work order |
|  | est_groundtime | INT | int | Estimated Groundtime saved as time |
| [I] | projectno | PRNO | univarchar(14) | Projectnumber depending on the workorderno |
| [I,I] | event_type | ALPHANUM | univarchar(2) | Event type is saved after issuing a workorder out of Maintenance Forecast D = Document R = Rotable Requirement C = Check T = Block Taskcard Q = Single Running / On Demand Taskcard V = Replacement Event E = Change Order O = Event Tracking WO SV = Shop Visit WO SC = Sim. Complaint WO W = Finding WO I = Import WO P = Panel Workorder J = JIC Workorder In case of a Work Template the type is saved after the creation: TW = Standalone Work Template JC = Jobcard JP = Jobcard Panel CD = Customer Requirement - Document CC = Customer Requirement - Check CT = Customer Requirement - Task CP = Customer Requirement - Part SH = Shopcard CO = Customer Requirement Component |
|  | ac_position | POS | univarchar(14) | position |
| [I] | locid_pk | INT | int | foreign key to ac_conf_locid.locid_pk |
| [I,I] | psn | RO | int | component psn -> Unique identifier for a Rotable |
| [I] | comp_partno | PA | univarchar(32) | Part Number of the Component |
| [I] | comp_serialno | ALPHANUM | univarchar(20) | Serial Number of the component |
|  | trouble_time | TIME | int | its used to save the time of old Double inspection |
| [I] | mel_code | ALPHANUM | univarchar(2) | mel code A or B or C or D or L(CDL) |
|  | incident | BOOL | univarchar(1) | incident |
| [I] | printed_by | SG | univarchar(8) | printed by is filled after printing the original user sign of SYSTEM |
|  | mutation | DATE | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
| [I] | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE | int | The creation date of this record as AMOS date. |
|  | add_page_counter | INT | int | additional Page counter is used for additional Pages |
|  | printed | INT | int | a counter to check how much printouts are already printed, you can reduce the counter when you press `Unprint` |
|  | trouble_date | DATE | int | date for old double inspection |
| [I] | dailyrecno_i | INT | int | ID of the parent event (Event Tracking) |
| [I] | ext_workorderno | ALPHANUM | univarchar(36) | External Workorder number, used to have a reference to a workorder number from another system. |
| [I] | prio | ALPHANUM | univarchar(4) | workorder priority replaces the field in the table wo_transfer.prio -> See also Basic Data Administration -> Workorder -> Priority |
| [I] | workorderno_display | INT | int | The displayed number of the workorder |
| [I] | template_revisionno_i | INT | int | The link to the template from which this WO was generated |
| [I] | issue_legno_i | INT | int | Associated flight leg for the workorder issue |
| [I] | closing_legno_i | INT | int | Associated flight leg for the workorder closure |
| [I] | uuid | ALPHANUM | univarchar(36) | Globally unique identifier |
| [I] | approvalno_i | INT | int | Approval Number reference key foreign key on table org_approval.approvalno_i |

### 3.16 `wo_transfer`
> This table contains the transfer data of a work order. only the last transfer record is valid for the work. The sorting of those entries is done that way: - use first the column event_perfno_i - then use the column recno - then use the column event_transferno_i All transfers are managed in that table: due, extensions, close and reporting back transfers. In order to find out which entry is the last one, use the column is_last_transfer together with the column transfer_type

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [I,I] | treq_dimension_groupno_i | INT | int | References the time requirement dimension group used for this transfer. |
|  | defer | BOOL | univarchar(1) | Specifies if is a deferred transfer. If true the value is Y; in all other cases (incl. null) it is false |
| [I,I] | treq_interval_groupno_i | INT | int | References the time requirement interval group used for this transfer. |
| [I,I] | event_perfno_i | WO | int | Unique identifier for a Work order |
| [I,I] | recno | INT | int | - to sort the entries, use column event_perfno_i - then use the recno - then use the event_transferno_i - to find out which entry is the last one, us the column is_last_transfer together with the column transfer_type |
| [I] | actionno_i | INT | int | Unique primary key for workorder action text. Only used for the new W/O text logic (wo_text_action). This field only contains a value if a transfer is performed with an action text otherwise the field is filled with null. |
|  | first_later_logic | ALPHANUM | univarchar(1) | This field holds the information about the calculation logic for this event. |
|  | is_last_transfer | BOOL | univarchar(1) | Boolean flag whether the current row is the last entry per event_perfno_i. |
| [I] | transfer_type | ALPHANUM | univarchar(1) | Possible types are: T=TRANSFER, E=EXTENSION, C=CLOSE, R=REPROT_BACK Values in table wo_transfer_dimension are due values for T, E. For C and R the values are performed at values. |
|  | transfer_station | STA | univarchar(4) | station at transfer |
|  | mech_sign | SG | univarchar(8) | mechanic sign user or mech sign is mandatory |
|  | user_sign | SG | univarchar(8) | user sign user or mech sign is mandatory |
|  | remarks | ALPHANUM | univarchar(36) | remarks |
|  | reason_code | ALPHANUM | univarchar(8) | The reason code. |
|  | reason | ALPHANUM | unitext(1073741823) | The transfer reason. |
|  | authorised_by | ALPHANUM | univarchar(12) | The authorised by value. |
|  | transfer_time | TIME | int | time at transfer |
|  | rts_date | DATE | int | Date is save on a Release to Service with Transfer |
|  | rts_time | TIME | int | Time is save on a Release to Service with Transfer |
|  | rts_sign | ALPHANUM | univarchar(8) | User Sign is save on a Release to Service with Transfer |
|  | rts_approval_number | ALPHANUM | univarchar(36) | Approval Number is save on a Release to Service with Transfer |
| [I] | rts_approvalno_i | INT | int | Approval Number reference key foreign key on table org_approval.approvalno_i |
|  | rts_reference | ALPHANUM | univarchar(128) | reference (free text) when release to info was selected (wo_transfer.rts_type=`V`) |
|  | rts_type | ALPHANUM | univarchar(1) | J = `Standard CRS` or  V = `CRS with reference` or W = `CRS within workpackage` |
|  | date_transfer | DATE | int | date at transfer |
|  | rts_station | STA | univarchar(6) | station is save on a Release to Service with Transfer |
|  | transfer_hours | INT | int | Old way to save the transfer hours, not used anymore. -> save the information in wo_transfer_dimension -> togo_interval |
|  | transfer_cycles | INT | int | Old way to save the transfer cycles, not used anymore. -> save the information in wo_transfer_dimension -> togo_interval |
|  | transfer_days | INT | int | Old way to save the transfer days, not used anymore. -> save the information in wo_transfer_dimension -> togo_interval |
|  | doc_ref | ALPHANUM | univarchar(36) | if a mel transfer is extended, in some cases a document reference is needed for the authority |
|  | mutation | DATE | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE | int | The creation date of this record as AMOS date. |
| [I] | legno_i | INT | int | Associated flight leg no for w/o transfer |
| [P] | event_transferno_i | INT | int | Unique primary key of this table (internal counter -248) |
|  | limit_type | ALPHANUM | univarchar(2) | The limit type which has been chosen for the input (NE=New Limits; AD=Adjustment of Limits; NO=No change of Limits) |
| [I] | uuid | ALPHANUM | univarchar(36) | Globally unique identifier |
|  | dc_use_case | INT | int | Used to identify the Due Calculation Use Case |
|  | transfer_context | ALPHANUM | univarchar(1) | Used to identify in which context the transfer was created. Can be either of Unknown (EMPTY_STRING or NULL), Initialistaion (I), Reporting Back (R), Sync. Time Requirement (S),  MP Activation (A),  Event Next Due Changed (C), Event Extension (E), Component Receiving (R) |
|  | init_option | ALPHANUM | univarchar(1) | Used to describe how the due was calculated. Can be either Unknown/Auto Calculation (EMPTY_STRING or NULL), Last Performed (L), Next Due (N), Never Performed (P) |
|  | absolute_due_date | DATE | int | The absolute value of the wo_transfer_dimension.due_at value for dimension D. In case of close or report back transfer this contains the closing/reporting back date. |
|  | absolute_due_time | TIME | int | The absolute value of the wo_transfer_dimension.due_at value for dimension D. In case of close or report back transfer this contains the closing/reporting back time in minutes since the start of the day. |

### 3.17 `wo_transfer_dimension`
> table containing dimensions that are used for interval codes/letter definition

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
|  | togo_interval | DOUBLE | float | The Togo value, in case it was entered by the user. Otherwise it is not set. Together with the date of the wo_transfer entry and the counter information (counterno_i) in this table, the value of the column due_at was set. |
| [P] | wo_transfer_dimensionno_i | INT | int | Unique identifier of the transfer dimension |
| [I] | treq_intervalno_i | INT | int | Interval used to generate this transfer dimension. |
| [I] | event_transferno_i | INT | int | Unique identifier of a transfer |
|  | due_at | DOUBLE | float | The value for this dimension in counter definition unit. The value could be a due at, closed at, performed at depending on the transfer type. |
|  | mutation | DATE | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE | int | The creation date of this record as AMOS date. |
| [I] | counterno_i | CO | int | Foreign key to counter.counterno_i |

### 3.18 `history`
> history

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
|  | shelf_inspection_type | ALPHANUM | univarchar(2) |  |
|  | shelf_inspection_date | DATE | int | Periodic check date. Used to indicate when the next periodic check is due.  |
| [I] | purch_recdetailno_i | REC | int | reference to the original purchase receiving detail |
| [I] | od_detail_rec_detailno_i | RD | int | contains the reference to the receiving detail of the order detail (D booking only) filled with -1 in case it has been initialised via migration script. |
| [I] | depr_rotablesno_i | DEPR | int | Depreciation Fixed Asset ID (deprec_rotables)  Filled only when the FA is on an active depreciation (ie Active/Fully Depreciated) |
| [I] | recinvno_i | IVD | int | reference to the invoice detail for which this history booking has been written |
| [I] | arp_exclude | BOOL | univarchar(1) | When set to Y, ARP will exclude this entry from the reorder calculation.  |
| [I] | inspection_detailno_i | ID | int | Key to inspection detail |
|  | ref_transaction_code | ALPHANUM | univarchar(16) | The Reference of the Default Transaction Code that has matched the history booking |
| [I] | transaction_id | INT | int | Stores the transactionID, an internal parameter used to group the history bookings |
| [I] | declared_end_use | CUSTEUSE | univarchar(2) | Declared customs end use code. |
| [I] | actual_end_use | CUSTEUSE | univarchar(2) | The actual customs end use code. |
| [I,I,I,I,I,I,I,I] | partno | PA | univarchar(32) | Part number |
| [I,I] | serialno | ALPHANUM | univarchar(20) | Serial number |
| [I,I] | psn | RO | int | Key rotables |
| [I,I,I,I,I] | labelno | LB | int | Label number |
| [I] | event_perfno_i | WO | int | Key event_perfno_i |
| [P,I,I,I] | historyno_i | HI | int | Primary key |
| [I,I] | wpno_i | WP | int | Key workpackage |
| [I] | station | STA | univarchar(4) | Last station of the part. In some cases it holds the station where it goes. |
|  | store_loc | LO | univarchar(16) | Used to hold the future store of the part, i.e. pickslip transfer. |
|  | ac_typ | AT | univarchar(6) | Aircraft type |
| [I,I] | ac_registr | AC | univarchar(6) | Aircraft registration |
|  | location | LONO | univarchar(16) | Last location of the part. |
|  | ata_chapter | ATA | univarchar(12) | Ata chapter |
| [I] | receiver | RCVR | univarchar(12) | Receiver, can be an aircraft or an address. |
| [I] | voucherno | ALPHANUM | univarchar(20) | Holds some information about the transaction, like order number, pickslip number, etc. |
| [I,I,I,I,I,I,I,I,I,I] | vm | VM | univarchar(2) | Voucher mode of the transaction, like D, YA, YE, etc. |
| [I,I,I] | del_date | DATE_INT | int | Date of the transaction. |
|  | qty | QTY | float | Quantity |
|  | average_price | MONETARY | float | Part average price |
|  | purch_price | MONETARY | float | Purchase price of order detail. Can be 0 when the repairable qty is 0 for consumables. Used for some older interfaces. |
|  | purch_pricec | MONETARY | float | Should not be used. Different type of data is stored in here. |
| [I] | releaseno | ALPHANUM | univarchar(40) | Rotable release number. Written by receiving. |
|  | tsn | INT | int | Time since new |
|  | msn | DOUBLE | float | MINUTES since new.   |
|  | csn | INT | int | Cycles since new |
|  | dsn | DATE_INT | int | Manufacture date, NOT days since new |
|  | tam | DOUBLE | float | Total aircraft MINUTES. |
|  | tah | INT | int | Total aircraft hours |
|  | tac | INT | int | Total aircraft cycles |
|  | mbi | DOUBLE | float | MINUTES between installation. |
| [I] | tbi | INT | int | Time between installation |
| [I] | cbi | INT | int | Cycles between installation |
| [I] | condition | COND | univarchar(2) | Rotable condition |
|  | reason | ALPHANUM | univarchar(6) | Holds information about scheduled or unscheduled removal US SD. |
|  | trouble_shooting | BOOL | univarchar(1) | Removal via trouble shooting. Is set via Label Booking. |
| [I] | please | ALPHANUM | univarchar(6) | Written by Tree Label Booking. Data taken from basic table reason. |
|  | confirmed | ALPHANUM | univarchar(2) | Written by Failure Confirmation if failure is confirmed. Following codes are valid: ``=failure not yet reported, Y=failure confirmed, N=failure unconfirmed but other failure found, WT=Only wear and tear ET=EPS Triage (Trail installation) NO=no failure found |
| [I] | costcenter | CCT | univarchar(8) | Cost center |
| [I] | mat_class | MATCL | univarchar(4) | Material class of the part |
| [I,I] | orderno | ORDER | univarchar(12) | Order number as key to order. |
|  | orderdate | DATE_INT | int | Date of the order |
| [I] | leg_no | ALPHANUM | univarchar(16) | Leg number, used sometimes to store the pos number of the order. Issued for wo, wp, label, etc data. |
| [I] | higher_partno | PA | univarchar(32) | Highest part number of an assy. |
| [I] | higher_serialno | ALPHANUM | univarchar(20) | Highest serial number of an assy |
| [I] | label_start | BOOL | univarchar(1) | Marker for highest part on assy (only written by Label Booking). |
| [I] | recdetailno_i | RD | int | Key to receiving goods |
|  | repairable | BOOL | univarchar(1) | Show if part is repairable (only consumables). |
|  | tool | BOOL | univarchar(1) | Shows if part is a tool. |
|  | special_contract | SP_CO | univarchar(2) | Special contract from part. |
|  | fa_qty | QTY | float | Financial part qty (calculated). Formerly known as Nominal Qty. |
|  | mutation | DATE | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. Status value 8 is reserved for Part Reliability for data inconsistency.   |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
| [I] | created_by | SG | univarchar(8) | The AMOS user who created this record. |
| [I,I,I,I] | created_date | DATE | int | The creation date of this record as AMOS date. |
| [I] | owner | VD | univarchar(12) | Owner of the part |
| [I] | action_performer | SG | univarchar(8) | Mechanic who performed the work in Label Booking. |
|  | booked_by | SG | univarchar(8) | User who booked the label in Label Booking. |
| [I] | repair_station | VD | univarchar(12) | Station where the label was booked. Component Tree Labelbooking. |
| [I] | projectno_i | PR | int | Key to the project. |
|  | store | STO | univarchar(10) | Last store of the part. |
| [I] | batchno | ALPHANUM | univarchar(20) | Batch number of consumable. |
|  | expire_date | DATE | int | Expire date. |
|  | trf_date | DATE | int | Used by older interfaces. |
|  | certificate_date | DATE | int | Date of certification. |
| [I] | certificate_company | VD | univarchar(12) | Address of vendor who certificated the part. |
| [I] | bill_detailno_i | INT | int | Link to bill_details is now defined in table billed_items. |
| [I] | od_detailno_i | ORDER_DE | int | Key order detail |
|  | tsn_unknown | BOOL | univarchar(1) | When tsn is unknown. |
|  | csn_unknown | BOOL | univarchar(1) | When csn is unknown. |
|  | mfg_unknown | BOOL | univarchar(1) | When mfg is unknown |
|  | fixed_asset | BOOL | univarchar(1) | Fixed asset flag |
|  | fa_relevant | BOOL | univarchar(1) | Indicates if the row is financially relevant or not. |
|  | pma | BOOL | univarchar(1) | indicate if the Part is PMA (Part MAnufacturer Approval). This is true when this field contains `Y`. |
|  | available_system_qty | QTY | float | Available System Quantity over all station |
|  | available_station_qty | QTY | float | Available Station Quantity (only available if booking is station relevant) |
| [I,I] | entityno_i | FAEA | int | Foreign Key references to entity_header (entityno_i) |
| [I] | pickslipseqno_i | ALPHANUM | int | unique key for pickslip_booked |
|  | mat_type | ALPHANUM | univarchar(4) | Part Material Type |
| [I] | storeno_i | ST | int | primary key of table store (consumables, stock) |
|  | average_price_func | MONETARY | float | Functional Average Price (in functional currency) |
|  | purch_price_func | MONETARY | float | Functional Order Price (in functional currency) |
|  | faqty_relevant | BOOL | univarchar(2) | Flag that the repairable consumable is new (has never been repaired) and therefore has to count in the financial qty. This flag will be used to replace the repairable counter for the repairable qty logic. |
|  | fsv | MONETARY | numeric | The financial stock value of the concerned part before the history booking |
|  | fsv_func | MONETARY | numeric | The financial stock value (functional) of the concerned part before the history booking |
|  | fsv_amount_delta | MONETARY | numeric | The amount impact on the FSV of the history booking considering the rounding precision defined on the local currency of the booking`s entity |
|  | fsv_amount_delta_func | MONETARY | numeric | The amount impact on the FSV of the history booking (functional) considering the rounding precision defined on the functional currency of the booking`s entity |

### 3.19 `od_detail`
> Holds all information about the details on an order.

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [I] | payment_condition | ALPHANUM | univarchar(4) | Payment conditions for this order. Defined in Basic Administration. |
| [I] | shipment_condition | ALPHANUM | univarchar(4) | Shipment conditions for this order detail. Defined in Basic Administration. |
| [I] | incoterm_incoming | ALPHANUM | univarchar(4) | Incoming Incoterm. International commercial terms; are used to divide transaction costs and responsibilities between buyer and seller. Defined in Basic Administration. |
| [I] | incoterm_outgoing | ALPHANUM | univarchar(4) | Outgoing Incoterm. International commercial terms; are used to divide transaction costs and responsibilities between buyer and seller. Defined in Basic Administration. Before it was stored in od_header.deliverycond. |
| [I] | address_from | VD | univarchar(12) | Ship from address. |
| [I] | address_via | VD | univarchar(12) | Drop to or via address. |
| [I] | address_return | VD | univarchar(12) | Return to address (formally on header as goods_destination) |
| [I] | address_ship | VD | univarchar(12) | Ship to address |
| [I] | receive_at_locationno_i | LO | int | Material should be received at this location for the replenishment process. |
| [I] | external_demandno_i | INT | int |  |
|  | direct_booking | BOOL | univarchar(1) | Defines if the linked pickslip on the order detail has to be booked automatically when received |
|  | serialno_setby | INT | int | an integer value that indicate if the serialno has been set by the system or by the user 0 or null -> created by system 1 -> created by the user |
| [I] | pricebookno_i | PB1 | int | Holds the foreign key to the selected price book entry. |
| [I] | material_lifecycle_id | INT | int | Reference number to the booked part so that the correct owner change/exchange  can be performed at the end of thecycle |
|  | rxr | BOOL | univarchar(1) | indicate if this order has been created for RXR or not. if the value is null or contains N -> means false, not an RXR order if the value = Y -> means true, an RXR order |
| [I] | account_no | ACC | univarchar(10) | Account number to be used for the detail |
|  | ext_req_qty | DOUBLE | float | The requested quantity expressed in the defined external mesure unit if exists. |
|  | req_qty | DOUBLE | float | The requested quantity on the order. |
| [I] | requested_serialno | RO | univarchar(20) | The serial number requested |
| [I] | end_use | ALPHANUM | univarchar(2) | Intended customs end use of the ordered part.  |
| [I] | transit_target_date | DATE | int | Date on which the detail is expected at the transit point. |
| [P] | detailno_i | INT | int | Primary key for every order detail. |
| [I,I,I] | orderno_i | INT | int | Referenz to od_order_header |
| [I] | posno | INT | int | Item position on order. |
| [I] | order_type | ALPHANUM | univarchar(2) | Order type, will be filled and should be the same as in corresponding order header. |
| [I] | ac_registr | AC | univarchar(6) | Holds the A/C registration for some orders, not all. |
| [I,I] | state | ALPHANUM | univarchar(2) | O: Open C: Closed T: Temporary N: Vendor open This will be filled but do not use it, use backorder instead to determine the detail state. |
|  | entry_date | DATE | int | similar to created_date (deprecated) |
|  | entry_sign | SG | univarchar(8) | Similar to created_by (deprecated) |
| [I] | vendor | VD | univarchar(12) | Holds the vendor for this detail. Not used anymore. |
| [I,I,I] | partno | PA | univarchar(32) | Partno which is on this detail. |
| [I] | serialno | RO | univarchar(20) | Serialno which is on this detail. |
|  | specification | ALPHANUM | univarchar(50) | Free text to enter specifications for the part or order. |
| [I] | quotationno_i | ORDER | int | Referenz to a Q-Order. Used in P-Orders. |
|  | condition | ALPHANUM | univarchar(2) | Condition of the part. |
|  | qty | DOUBLE | float | Qty on the order. |
| [I,I,I] | backorder | DOUBLE | float | Tells you the state of the order. qty = backorder -> Order is open, nothing delivered yet. qty != backorder, backorder > 0 -> Open, partly delivered. backorder = 0, Order is closed. Tells how much qty is left to be delivered. Negative qty means it was more  delivered than ordered. |
| [I] | currency | CUR | univarchar(4) | Currency of the order detail. |
|  | rate | DOUBLE | float | Exchange rate for the foreign currency at the time when the order was created. |
|  | purch_price | MONETARY | float | Price for an item in local currency. |
|  | purch_pricec | MONETARY | float | Price for an item in foreign currency. |
|  | amount | MONETARY | float | Total amount for all units on one detail entry in local currency.			 |
|  | amountc | MONETARY | float | Total amount for all units on one detail entry in foreign currency. |
|  | discount | DOUBLE | float | Discount given for an detail. |
|  | orig_target_date | DATE | int | Used as return target date for IR-Orders. |
|  | target_date | DATE | int | Target date of the detail. |
|  | confirmed_date | DATE | int | Confirmed date for the detail. |
|  | del_date | DATE | int | Delivery date of the detail. |
|  | reason | ALPHANUM | univarchar(6) | Reason for ordering this detai. |
| [I] | priority | ALPHANUM | univarchar(10) | Priority of the this detail. |
|  | req_condition | ALPHANUM | univarchar(6) | Required condition for this detail. |
|  | req_work | ALPHANUM | univarchar(6) | Used for transfer. |
| [I] | pickslipno | PSL | univarchar(10) | Pickslipno from where the detail came. |
| [I] | pickslipseqno_i | INT | int | Pickslipseqno_i from Pickslipno, used internally. |
|  | reminder_date | DATE | int | Date of last reminder. |
|  | reminder_count | INT | int | Counts how many reminders were already send. |
| [I] | projectno | PRNO | univarchar(14) | Project number (e.g. financial projects) |
| [I] | budgetno | ALPHANUM | univarchar(14) | Not used anymore |
| [I] | costcenter | CCT | univarchar(8) | Will hold the corresponding costcenter for a detail. |
|  | cost_type | CSTP | univarchar(10) | Will hold the cost type of the detail. |
|  | free_of_charge | BOOL | univarchar(1) | Shows if the detail is free of charge or not. |
|  | direct_consumption | BOOL | univarchar(1) | Will tell if the part should be consumed directly after receiving. Only consumables. |
|  | taxable | BOOL | univarchar(1) | Shows if the detail is taxable. Just for information not logic behind it. |
|  | package_code | ALPHANUM | univarchar(2) | Not used anymore |
| [I] | awbno | AWB | univarchar(24) | Holds the incoming awbno. |
|  | awb_date | DATE | int | AWB-Date |
| [I] | poolno_i | POOLAGRE | int | Referenz to a pool. |
| [I] | requested_partno | PA | univarchar(32) | Used for pool orders |
|  | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | Status of order. 0: Open order 1: Is booked but stays open 2: Is in Repair Administration 3: Is sent to LHT 4: Is in Electronic Pending Shelf 5: Is a stock access order 					 |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |
| [I] | shipdetailno_i | INT | int | This link to the shipment detail is deprecated. Correct is the link from shipment detail to the order detail (sh_detail.od_detailno_i). |
| [I] | locationno_i | LO | int | Referenz to location of the detail. Not used yet. |
| [I] | costcenter2 | CCT | univarchar(8) | Used for IRIS interface only. |
|  | ac_receiver | ALPHANUM | univarchar(6) | Holds information for ASTRE, made for RAE. |
| [I] | labelno | INT | int | Holds the labelno for consumables. Labelno for rotables will be taken from rotable table. |
|  | req_checked | BOOL | univarchar(1) | Holds information if the requirements on that detail are already checked. |
| [I] | measure_unit | UOM | univarchar(4) | Holds the internal measure unit for the time when the order was created. |
|  | agreed_price | ALPHANUM | univarchar(1) | Indicates if an order price is agreed with the vendor. If value is `Y` the price is agreed, if value is `N` or empty the price is not agreed. |
|  | detail_status | ALPHANUM | univarchar(2) | Status of detail |
| [I] | ext_state | ALPHANUM | univarchar(2) | Extended state of detail. O=open, C=closed, CA=cancelled, B=booked, BO=booked/open, R=received, PR=partly received, etc. |
|  | target_time | INT | int | Time for target date |
|  | detail_text | ALPHANUM | unitext(1073741823) | Detail text |
|  | detail_remarks | ALPHANUM | unitext(1073741823) | Detail remarks |
|  | warranty | ALPHANUM | univarchar(2) | Warranty flag |
| [I] | contract_id | INT | int | id of the pooling contract |
| [I] | psn | RO | int | Link to rotable |
| [I] | storeno_i | INT | int | Link to store entry |
|  | ext_measure_unit | UOM | univarchar(4) | Holds the external measure unit |
|  | ext_purch_price | MONETARY | float | Price for an item in local currency and external measure unit. |
|  | ext_purch_pricec | MONETARY | float | Price for an item in foreign currency and external measure unit. |
|  | ext_qty | DOUBLE | float | External qty on the order. |
|  | ext_backorder | DOUBLE | float | Tells you the state of the order. qty = backorder -> Order is open, nothing delivered yet. qty != backorder, backorder > 0 -> Open, partly delivered. backorder = 0, Order is closed. |
| [I] | recdetailno_i | INT | int | link to the original receiving detail |
|  | acknowledgement_date | DATE | int | date of acknowledgement |
|  | acknowledgement_time | TIME | int | time of acknowledgement |
|  | ack_source_type | INT | int | source type of acknowledgement, e.g. manual entry, Spec2000, LHT interface |
|  | acknowledgement_number | ALPHANUM | univarchar(40) | Acknowledgement number provided by the supplier |
|  | purch_price_func | MONETARY | float | Price for an item in functional currency. |
|  | ext_purch_price_func | MONETARY | float | Price for an item in functional currency and external measure unit. |
|  | amount_func | MONETARY | float | Total amount for all units on one detail entry in functional currency.		 |
|  | rate_func | DOUBLE | float | Exchange rate for the functional currency at the time when the order was created. |
|  | single_source_contract_id | INT | int | Single source contract when available in price book. |
|  | single_source_remarks | ALPHANUM | unitext(1073741823) | Single source remarks |
|  | created_time | TIME | int | The creation time of this record in milliseconds since the start of the day. |

### 3.20 `location`
> location (shelf) for parts

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [P] | locationno_i | LO | int | primary key |
|  | description | ALPHANUM | univarchar(36) | description of the location |
| [U] | station | STA | univarchar(4) | station where this location is (key together with store and location) |
| [U] | store | STO | univarchar(8) | store where this location is (key together with station and location) |
| [U] | location | LONO | univarchar(16) | location where this location is (key together with store and station) |
|  | owner | VD | univarchar(12) | location owner |
| [I,I] | location_type | INT | int | type of location: 0: standard 1: receiving 2: temporary -1: aircraft -2: transfer -3: inventory -4: U/S -5: hangar -6: shop -7: services -8: scrap -9: tree -10: enroute -11: not used yet (exhouse) -20: pool -100: tools -101: c-check -102: ship -103: consignment -104: customer |
| [I] | location_restriction | INT | int | restriction of location: 0: standard (not restricted) 1: restricted 2: inaccessible |
| [I,I] | location_storage_type | ALPHANUM | univarchar(2) | storage-type of location (automatic store): ST: standard EF: Effi-Store KA: Kardex |
|  | text | ALPHANUM | unitext(1073741823) | text or description for this location |
| [I] | area | ALPHANUM | univarchar(6) | area of locations |
|  | max_pn_qty | INT | int | maximum qty of partnumbers for this location |
|  | mutation | DATE | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE | int | The creation date of this record as AMOS date. |
| [I,I] | linked_type | ALPHANUM | univarchar(6) | amos type that is a location |
| [I,I,I,I,I] | linked_key | INT | int | number of the amos type that is a location psn required |

### 3.21 `address`
> contains all addresses (vendors, suppliers, stations, stores, departments...)

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
|  | incomingincoterm | ALPHANUM | univarchar(4) | contains the incoming incoterm of the address (see table deliverycond) |
|  | discount | DOUBLE | float | Holds the general vendor discount. |
| [P,I] | address_i | AD | int | Primary key of address. (internal parameter 407) |
| [I] | parent | AD | int | contains the address_i of the parent address |
| [U] | vendor | VD | univarchar(12) | contains the vendor code o the address |
| [I] | type_i | INT | int | contains the type of an address. (see table adr_type) |
|  | name | ALPHANUM | univarchar(36) | contains the name (first) of the address |
|  | name_1 | ALPHANUM | univarchar(36) | contains the name (second) of the address. |
|  | name_2 | ALPHANUM | univarchar(36) | contains the name (third) of the address. |
|  | contact_name | ALPHANUM | univarchar(36) | contains the contact name of the address. |
| [I] | currency | CUR | univarchar(4) | contains the currency of the address. (see table currency) |
|  | linked_adr | BOOL | univarchar(2) | contains `Y` when the address (address, city, country...) of the address is depending from a main address.  |
| [I] | linked_detail | INT | int | contains the address_i where the adr_detail (address,city,country...) is stored. |
|  | remarks | ALPHANUM | unitext(1073741823) | contains a specific remarks for an address |
| [I] | default_tax_code | ALPHANUM | univarchar(6) | Contains the default tax code defined for this address. |
|  | shipment | ALPHANUM | univarchar(4) | Method of Transportation. Link to the table shipment which is a Basic-Data table. It is used to prefill order data. |
|  | ship_via | INT | int | Ship Via address_i. It is used to prefill order data. |
|  | paymentcondition | ALPHANUM | univarchar(4) | contains the payment condition of the address. (see table paymentcond) |
|  | deliverycondition | ALPHANUM | univarchar(4) | contains the delivery condition of the address (see table deliverycond) |
|  | black_list | BOOL | univarchar(1) | contains "Y" when the address is marked as black list. |
|  | black_list_since | INT | int | contains the date since the address is black listed. |
|  | black_list_text | ALPHANUM | unitext(1073741823) | contains the reason (free text) why the address is black listed. |
|  | status | INT | int | contains the status of the address: 0 - ACTIVE 9 - INACTIVE 2 - CREATED BY SPEC2000 |
|  | homebase | STA | univarchar(4) | contains the homebase of the address. |
|  | costcenter | CCT | univarchar(8) | contains the cost center of the address. (see table costcenter) |
| [I] | resource_type_id | ALPHANUM | univarchar(20) | Contains the Resource Type associated with this Address. If the Resource Type is filled, then the Address is considered as a Resource. => Used to define the Resource Type |
|  | bill_text | ALPHANUM | unitext(1073741823) | contains a specific bill text for an address which will be printed on the bill (Billing). |
|  | bill_payment_cond | ALPHANUM | univarchar(4) | contains a specific payment condition for an address which will be copied to the bill when a bill is generated for this address. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
| [I,I] | mutation | DATE | int | The mutation date of this record as AMOS date. |
| [I,I] | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE | int | The creation date of this record as AMOS date. |

### 3.22 `ac_typ`
> contains all a/c types used throughout AMOS. A/C types that are not real operational A/C types do contain a `Y` in column non_operational.

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
|  | uuid | ALPHANUM | univarchar(36) | Globally unique identifier |
|  | use_blockhours | BOOL | univarchar(1) | Use Block Hours on MEL Transfer, if not enabled Flight Hours are used. Possible values: NULL: Undefined (default) Y: Block Hours are used |
|  | costcenter | CCT | univarchar(8) | Cost centres |
| [U] | ac_type_i | INT | int | aircraft type number |
| [P] | ac_typ | AT | univarchar(6) | Aircraft type |
|  | description | ALPHANUM | univarchar(36) | Description of the aircraft type |
|  | main_typ | AT | univarchar(6) | Main type of the aircraft used as official IATA ac type code, e.g. interfaces. |
|  | fa_ac_typ | FAT | univarchar(6) | financial A/C type. |
|  | tcds | ALPHANUM | univarchar(20) | type certificate data sheet number given by the A/C manufacturer. |
|  | non_operational | ALPHANUM | univarchar(2) | `Y` if the A/C type is a non-operational one, i.e. one that is required by certain AMOS application but does not represent a real A/C type. These A/C type are ignored in the books of certain applications. |
|  | single_multi_pilot | BOOL | univarchar(1) | Single or Multi pilot. Definition taken from A/C Type Possible values: NULL: Undefined (default) S: Single M: Multi |
|  | ops_rating | INT | int | reference to a ops_rating |
| [I] | counter_template_groupno_i | COTG | int | Reference to the counter_template_group.counter_template_groupno_i |
|  | cd_def_transfer_days | INT | int | Default transfer days for workorder created out of crew defects via an interface. The number of days is used as default in case no mel reference is defined. |
|  | cd_def_transfer_cycles | INT | int | Default transfer cycles for workorder created out of crew defects via an interface. The number of cycles is used as default in case no mel reference is defined. |
|  | cd_def_transfer_flight_hours | TIME_AMO | univarchar(10) | Default transfer flight hours for workorder created out of crew defects via an interface. The number of flight hours is used as default in case no mel reference is defined. |
|  | is_subtype | BOOL | univarchar(1) | Marker if it is an aircraft subtype which is the third highest level in the hierarchy. |
|  | is_model | BOOL | univarchar(1) | Marker if it is an aircraft model which is the second highest level in the hierarchy. |
|  | is_type | BOOL | univarchar(1) | Marker if it is an aircraft type which is the highest level in the hierarchy. |
|  | mutation | DATE | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE | int | The creation date of this record as AMOS date. |

### 3.23 `condition`
> Holds values for the part/rotable conditions. The table could be administrated in Basic Data Administration.

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
|  | aex_condition | ALPHANUM | univarchar(1) | This column contains mapping of AMOS condition towards AeroRepair Repair Process Code Possible values: C - Calibration D - Advance Exchange E - Rebuild F - Advance Loan L - Shelf Life Renewal M - Modification O - Overhaul/Heavy Repair R - Repair T - Test/Bench Check X - Exchange Unit Z - Other |
|  | spec2k_condition | ALPHANUM | univarchar(2) | Mapping to predefined SPEC2000 conditions. Possible values are:  EX - "EXCHANGE/LEASE" NU - "NEW" OH - "OVERHAULED" ST - "STOLEN/MISSING" US - "USED/UNSERVICEABLE" |
|  | spec2k_partstatuscode | ALPHANUM | univarchar(60) | Mapping to predefined SPEC2000 Part Status Code. Possible values are:  INSPECTED MANUFACTURED MODIFIED NEW OVERHAULED PROTOTYPE REPAIRED TESTED |
| [P] | condition | COND | univarchar(2) | Shortcut of condition. |
|  | description | ALPHANUM | univarchar(36) | Detailed description of condition. |
|  | jaa_condition | COND | univarchar(2) | JAA condition. This is used to map a internal condition to a official JAA Condition. |
|  | sh_condition | BOOL | univarchar(1) | set if the condition can be used in the shipment |
|  | percentage | DOUBLE | float | the percentage to apply on the part price according to its condition |
|  | mutation | DATE | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | 0 = Active 1 = New 9 = Inactive |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE | int | The creation date of this record as AMOS date. |
|  | uuid | ALPHANUM | univarchar(36) | Globally unique identifier |

### 3.24 `part_special`
> contains all special marking for a part

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [P] | part_specialno_i | INT | int | The primary key of the part_special table.  |
| [I] | partno | PA | univarchar(32) | link to part |
| [I] | special | ALPHANUM | univarchar(6) | special code, see table special |
| [I] | unique_key | ALPHANUM | univarchar(40) | primary of part_special code. |
|  | remarks | ALPHANUM | univarchar(36) | additional remark to part special |
|  | amount | DOUBLE | float | contains a numeric value of a part special |
| [I] | referenceno | ALPHANUM | univarchar(20) | contains a string value for a part_special |
|  | mutation | DATE | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE | int | The creation date of this record as AMOS date. |

### 3.25 `counter_definition`
> Basic table to define a counter, such as flight hours, cycles, apu hours,...

| Keys | Name | Mime-Type | Type | Description |
|---|---|---|---|---|
| [P] | counter_defno_i | INT | int | Internal unique ID, parameter -241 |
| [U] | code | ALPHANUM | univarchar(10) | Short description of the counter such as C,H,APU,... |
|  | name | ALPHANUM | univarchar(30) | Short description of the counter such as Flight Cycles, Flight Hours,... |
|  | description | ALPHANUM | univarchar(255) | Detailed description of the counter. |
|  | display_unit | MIME | univarchar(8) | The display unit of the counter, such as: double, integer, time,...  |
|  | sort_order | INT | int | Defines the sort order of the dimension. This is used to display the counters. |
|  | calc_order | INT | int | Use to define the calculation order. Only needed in case of dependencies between counters, can be null if not needed. |
|  | daily_average | DOUBLE | float | Default daily average used in case not defined on A/C or not installed. |
|  | mutation | DATE_INT | int | The mutation date of this record as AMOS date. |
|  | mutator | SG | univarchar(8) | The AMOS user who changed this record. |
|  | status | INT | int | The status of the record. This column has a different meaning for every table. |
|  | mutation_time | TIME | int | The time when this record has been added or changed in milliseconds since the start of the day. |
|  | created_by | SG | univarchar(8) | The AMOS user who created this record. |
|  | created_date | DATE_INT | int | The creation date of this record as AMOS date. |

## 4. Excel-файлы (data_input/source_data/v_2026-04-08 + data_input/master_data)
### 4.1 Status_Components.xlsx
- Path: `data_input/source_data/v_2026-04-08/Status_Components.xlsx`
- Sheet (default): Sheet1
- Rows (data): 202761
- Columns (count): 21

| Колонка | dtype |
|---|---|
| partno | int64 |
| partseqno_i | int64 |
| serialno | int64 |
| psn | int64 |
| ac_typ | object |
| ac_type_i | int64 |
| location | object |
| ll | int64 |
| oh | float64 |
| oh_threshold | float64 |
| sne | float64 |
| ppr | float64 |
| mfg_date | object |
| oh_at_date | float64 |
| shop_visit_counter | int64 |
| owner | object |
| address_i | int64 |
| condition | object |
| removal_date | object |
| target_date | float64 |
| lease_restricted | float64 |

### 4.2 Status_Components_DWH.xlsx
- Path: `data_input/source_data/v_2026-04-08/Status_Components_DWH.xlsx`
- Sheet (default): Sheet1
- Rows (data): 202720
- Columns (count): 20

| Колонка | dtype |
|---|---|
| partno | int64 |
| partseqno_i | int64 |
| serialno | int64 |
| psn | int64 |
| ac_typ | object |
| ac_type_i | int64 |
| location | object |
| ll | int64 |
| oh | float64 |
| oh_threshold | float64 |
| sne | float64 |
| ppr | float64 |
| mfg_date | object |
| oh_at_date | object |
| shop_visit_counter | int64 |
| owner | object |
| address_i | int64 |
| condition | object |
| removal_date | object |
| target_date | float64 |

### 4.3 Status_Overhaul.xlsx
- Path: `data_input/source_data/v_2026-04-08/Status_Overhaul.xlsx`
- Sheet (default): Sheet1
- Rows (data): 56
- Columns (count): 11

| Колонка | dtype |
|---|---|
| ac_registr | int64 |
| ac_typ | object |
| wpno | object |
| description | object |
| sched_start_date | datetime64[ns] |
| sched_end_date | datetime64[ns] |
| act_start_date | datetime64[ns] |
| act_end_date | float64 |
| status | object |
| owner | object |
| operator | object |

### 4.4 Program_AC.xlsx
- Path: `data_input/source_data/v_2026-04-08/Program_AC.xlsx`
- Sheet (default): Sheet1
- Rows (data): 170
- Columns (count): 9

| Колонка | dtype |
|---|---|
| ac_registr | int64 |
| ac_typ | object |
| object_type | object |
| description | object |
| owner | object |
| operator | object |
| homebase | object |
| homebase_name | object |
| directorate | object |

### 4.5 Program.xlsx
- Path: `data_input/source_data/v_2026-04-08/Program.xlsx`
- Sheet (default): 2025
- Rows (data): 23
- Columns (count): 99

| Колонка | dtype |
|---|---|
| ac_type_mask | float64 |
| serialno | float64 |
| Месяц | object |
| 1 | float64 |
| 2 | float64 |
| 3 | float64 |
| 4 | float64 |
| 5 | float64 |
| 6 | float64 |
| 7 | float64 |
| 8 | float64 |
| 9 | float64 |
| 10 | float64 |
| 11 | float64 |
| 12 | float64 |
| 1.1 | float64 |
| 2.1 | float64 |
| 3.1 | float64 |
| 4.1 | float64 |
| 5.1 | float64 |
| 6.1 | float64 |
| 7.1 | float64 |
| 8.1 | float64 |
| 9.1 | float64 |
| 10.1 | float64 |
| 11.1 | float64 |
| 12.1 | float64 |
| 1.2 | float64 |
| 2.2 | float64 |
| 3.2 | float64 |
| 4.2 | float64 |
| 5.2 | float64 |
| 6.2 | float64 |
| 7.2 | float64 |
| 8.2 | float64 |
| 9.2 | float64 |
| 10.2 | float64 |
| 11.2 | float64 |
| 12.2 | float64 |
| 1.3 | float64 |
| 2.3 | float64 |
| 3.3 | float64 |
| 4.3 | float64 |
| 5.3 | float64 |
| 6.3 | float64 |
| 7.3 | float64 |
| 8.3 | float64 |
| 9.3 | float64 |
| 10.3 | float64 |
| 11.3 | float64 |
| 12.3 | float64 |
| 1.4 | float64 |
| 2.4 | float64 |
| 3.4 | float64 |
| 4.4 | float64 |
| 5.4 | float64 |
| 6.4 | float64 |
| 7.4 | float64 |
| 8.4 | float64 |
| 9.4 | float64 |
| 10.4 | float64 |
| 11.4 | float64 |
| 12.4 | float64 |
| 1.5 | float64 |
| 2.5 | float64 |
| 3.5 | float64 |
| 4.5 | float64 |
| 5.5 | float64 |
| 6.5 | float64 |
| 7.5 | float64 |
| 8.5 | float64 |
| 9.5 | float64 |
| 10.5 | float64 |
| 11.5 | float64 |
| 12.5 | float64 |
| 1.6 | float64 |
| 2.6 | float64 |
| 3.6 | float64 |
| 4.6 | float64 |
| 5.6 | float64 |
| 6.6 | float64 |
| 7.6 | float64 |
| 8.6 | float64 |
| 9.6 | float64 |
| 10.6 | float64 |
| 11.6 | float64 |
| 12.6 | float64 |
| 1.7 | float64 |
| 2.7 | float64 |
| 3.7 | float64 |
| 4.7 | float64 |
| 5.7 | float64 |
| 6.7 | float64 |
| 7.7 | float64 |
| 8.7 | float64 |
| 9.7 | float64 |
| 10.7 | float64 |
| 11.7 | float64 |
| 12.7 | float64 |

### 4.6 Program_heli.xlsx
- Path: `data_input/source_data/v_2026-04-08/Program_heli.xlsx`
- Sheet (default): 2025
- Rows (data): 4
- Columns (count): 110

| Колонка | dtype |
|---|---|
| ac_type_mask | float64 |
| Месяц | object |
| 1 | float64 |
| 2 | float64 |
| 3 | float64 |
| 4 | float64 |
| 5 | float64 |
| 6 | float64 |
| 7 | float64 |
| 8 | float64 |
| 9 | float64 |
| 10 | int64 |
| 11 | float64 |
| 12 | float64 |
| 1.1 | float64 |
| 2.1 | float64 |
| 3.1 | float64 |
| 4.1 | float64 |
| 5.1 | float64 |
| 6.1 | float64 |
| 7.1 | float64 |
| 8.1 | float64 |
| 9.1 | float64 |
| 10.1 | float64 |
| 11.1 | float64 |
| 12.1 | float64 |
| 1.2 | int64 |
| 2.2 | float64 |
| 3.2 | float64 |
| 4.2 | float64 |
| 5.2 | float64 |
| 6.2 | float64 |
| 7.2 | float64 |
| 8.2 | float64 |
| 9.2 | float64 |
| 10.2 | float64 |
| 11.2 | float64 |
| 12.2 | float64 |
| 1.3 | int64 |
| 2.3 | float64 |
| 3.3 | float64 |
| 4.3 | float64 |
| 5.3 | float64 |
| 6.3 | float64 |
| 7.3 | float64 |
| 8.3 | float64 |
| 9.3 | float64 |
| 10.3 | float64 |
| 11.3 | float64 |
| 12.3 | float64 |
| 1.4 | int64 |
| 2.4 | float64 |
| 3.4 | float64 |
| 4.4 | float64 |
| 5.4 | float64 |
| 6.4 | float64 |
| 7.4 | float64 |
| 8.4 | float64 |
| 9.4 | float64 |
| 10.4 | float64 |
| 11.4 | float64 |
| 12.4 | float64 |
| 1.5 | int64 |
| 2.5 | float64 |
| 3.5 | float64 |
| 4.5 | float64 |
| 5.5 | float64 |
| 6.5 | float64 |
| 7.5 | float64 |
| 8.5 | float64 |
| 9.5 | float64 |
| 10.5 | float64 |
| 11.5 | float64 |
| 12.5 | float64 |
| 1.6 | float64 |
| 2.6 | float64 |
| 3.6 | float64 |
| 4.6 | float64 |
| 5.6 | float64 |
| 6.6 | float64 |
| 7.6 | float64 |
| 8.6 | float64 |
| 9.6 | float64 |
| 10.6 | float64 |
| 11.6 | float64 |
| 12.6 | float64 |
| 1.7 | float64 |
| 2.7 | float64 |
| 3.7 | float64 |
| 4.7 | float64 |
| 5.7 | float64 |
| 6.7 | float64 |
| 7.7 | float64 |
| 8.7 | float64 |
| 9.7 | float64 |
| 10.7 | float64 |
| 11.7 | float64 |
| 12.7 | float64 |
| 1.8 | float64 |
| 2.8 | float64 |
| 3.8 | float64 |
| 4.8 | float64 |
| 5.8 | float64 |
| 6.8 | float64 |
| 7.8 | float64 |
| 8.8 | float64 |
| 9.8 | float64 |
| 10.8 | float64 |
| 11.8 | float64 |
| 12.8 | float64 |

### 4.7 MD_Сomponents.xlsx
- Path: `data_input/master_data/MD_Сomponents.xlsx`
- Sheet (default): Sheet1
- Rows (data): 78
- Columns (count): 24

| Колонка | dtype |
|---|---|
| Чертежный номер | object |
| Количество установленных на ВС | object |
| Группировка оборота | object |
| Тип ВС | object |
| Запрет на смену типа ВС | object |
| Запрет совмещения ХВ | object |
| Unnamed: 6 | object |
| Учет МРР 1 | object |
| Два лимита | object |
| Срок разборки ВС  | object |
| Срок сборки ВС | object |
| Объем ремонта | object |
| Срок ремонта | object |
| НР Ми-8 | object |
| МРР Ми-8 | object |
| МРР 1 Ми-8 | object |
| НР Ми-17 | object |
| МРР Ми-17 | object |
| БР2 Ми-17 | object |
| Цена ремонта без НДС | object |
| Цена покупки без НДС | object |
| Правила пополнения фонда | object |
| Unnamed: 22 | object |
| Наработка на отказ | object |

## 5. Сводная статистика

- Project ClickHouse (10.95.19.132:9000/default): **83** таблиц, суммарный размер (total_bytes) ≈ **178.22 MB**
- DWH ClickHouse (rc1a-fhb99q2hquq89uhp.mdb.yandexcloud.net/default): **191** таблиц в проинвентаризированных схемах (reports, staging, source, analytics, integrated)
- Таблиц с колонкой `version_date` (в `default` Project CH): **47**
- Таблиц с колонкой `group_by` (в `default` Project CH): **29**
- AMOS-таблиц из списка ТЗ, найдено в CSV: **25** из **25**
- Excel-файлов по путям: **7** (факт чтения — см. секцию 4; отсутствующие отмечены)
- Дата инвентаризации: **2026-04-27**
- Источники: `.env`, `config/database_config.yaml`, `data_input/analytics/Database-Description39331920032025_0.csv`, `data_input/source_data/v_2026-04-08/`, `data_input/master_data/`

