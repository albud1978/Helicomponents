#!/usr/bin/env python3
"""
Проверка baseline симуляции БЕЗ quota_repair
Дата: 19-11-2025
Цель: Зафиксировать поведение ДО внедрения квотирования
"""
import sys
sys.path.insert(0, '/home/budnik_an/cube linux/cube/code')

from config_loader import get_clickhouse_client

client = get_clickhouse_client()

print("=" * 80)
print("BASELINE: Симуляция БЕЗ quota_repair (3650 дней)")
print("=" * 80)

# 1. Количество агентов в ремонте по дням
print("\n[1] Максимальное количество агентов в ремонте одновременно")
print("-" * 80)

query1 = """
SELECT day_u16, COUNT(*) as in_repair 
FROM sim_masterv2 
WHERE state = 'repair' AND group_by IN (1, 2)
GROUP BY day_u16
ORDER BY in_repair DESC
LIMIT 20
"""

result = client.execute(query1)
if result:
    print(f"Топ-20 дней по количеству агентов в ремонте (планеры Mi-8/Mi-17):")
    for row in result:
        day, count = row
        print(f"   День {day:4d}: {count:2d} агентов в ремонте")
else:
    print("⚠️ Нет данных")

# 2. Статистика входов в ремонт (transition 2→4)
print("\n[2] Переходы operations→repair (2→4)")
print("-" * 80)

query2 = """
SELECT COUNT(*) as total_entries, 
       COUNT(DISTINCT aircraft_number) as unique_aircraft
FROM sim_masterv2 
WHERE transition_2_to_4 = 1 AND group_by IN (1, 2)
"""

result = client.execute(query2)
if result:
    total, unique = result[0]
    print(f"Всего входов в ремонт: {total}")
    print(f"Уникальных агентов: {unique}")
else:
    print("⚠️ Нет данных")

# 3. Распределение по дням
query3 = """
SELECT day_u16, COUNT(*) as entries 
FROM sim_masterv2 
WHERE transition_2_to_4 = 1 AND group_by IN (1, 2)
GROUP BY day_u16
ORDER BY day_u16
LIMIT 50
"""

result = client.execute(query3)
if result:
    print(f"\nПервые 50 дней с входами в ремонт:")
    for row in result[:20]:
        day, entries = row
        print(f"   День {day:4d}: {entries} вход(ов)")
    if len(result) > 20:
        print(f"   ... (всего {len(result)} дней с входами)")
else:
    print("⚠️ Нет данных")

# 4. Проверка агентов которые НИКОГДА не попали в ремонт
print("\n[3] Агенты, которые НИКОГДА не попали в ремонт за 3650 дней")
print("-" * 80)

query4 = """
SELECT aircraft_number, group_by, MAX(state) as max_state
FROM sim_masterv2
WHERE group_by IN (1, 2)
GROUP BY aircraft_number, group_by
HAVING SUM(transition_2_to_4) = 0
ORDER BY aircraft_number
LIMIT 30
"""

result = client.execute(query4)
if result:
    print(f"Найдено {len(result)} планеров без ремонта:")
    for row in result[:20]:
        ac, gb, max_state = row
        type_str = "Mi-8" if gb == 1 else "Mi-17"
        print(f"   AC {ac} ({type_str})")
    if len(result) > 20:
        print(f"   ... и ещё {len(result) - 20}")
else:
    print("✅ Все планеры хотя бы раз попали в ремонт")

# 5. Средняя загрузка ремонтной базы
print("\n[4] Средняя загрузка ремонтной базы (планеры)")
print("-" * 80)

query5 = """
SELECT 
    AVG(in_repair) as avg_in_repair,
    MIN(in_repair) as min_in_repair,
    MAX(in_repair) as max_in_repair,
    STDDEV_POP(in_repair) as stddev_in_repair
FROM (
    SELECT day_u16, COUNT(*) as in_repair 
    FROM sim_masterv2 
    WHERE state = 'repair' AND group_by IN (1, 2)
    GROUP BY day_u16
)
"""

result = client.execute(query5)
if result:
    avg, min_val, max_val, stddev = result[0]
    print(f"Среднее количество в ремонте: {avg:.2f}")
    print(f"Минимум: {min_val}")
    print(f"Максимум: {max_val}")
    print(f"Стандартное отклонение: {stddev:.2f}")
    print(f"\n➡️ Если квота = 18, то превышение: {max_val - 18} агентов")
else:
    print("⚠️ Нет данных")

# 6. Динамика по периодам
print("\n[5] Динамика загрузки по годам")
print("-" * 80)

query6 = """
SELECT 
    FLOOR(day_u16 / 365) + 1 as year,
    AVG(in_repair) as avg_in_repair,
    MAX(in_repair) as max_in_repair
FROM (
    SELECT day_u16, COUNT(*) as in_repair 
    FROM sim_masterv2 
    WHERE state = 'repair' AND group_by IN (1, 2)
    GROUP BY day_u16
)
GROUP BY year
ORDER BY year
"""

result = client.execute(query6)
if result:
    for row in result:
        year, avg, max_val = row
        print(f"   Год {int(year)}: avg={avg:.1f}, max={max_val}")
else:
    print("⚠️ Нет данных")

print("\n" + "=" * 80)
print("BASELINE ЗАФИКСИРОВАН")
print("Лог симуляции: /tmp/sim_baseline_no_quota_repair_3650.log")
print("=" * 80)


