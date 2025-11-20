#!/usr/bin/env python3
"""
Детальный анализ baseline: переходы 2→4 и превышения квоты
Дата: 19-11-2025
"""
import sys
sys.path.insert(0, '/home/budnik_an/cube linux/cube/code')

from config_loader import get_clickhouse_client

client = get_clickhouse_client()

print("=" * 80)
print("ДЕТАЛЬНЫЙ АНАЛИЗ BASELINE")
print("Лог симуляции: /tmp/sim_baseline_no_quota_repair_3650.log")
print("=" * 80)

# 1. Проверка: есть ли вообще transition_2_to_4 в данных?
print("\n[1] Проверка наличия переходов 2→4 в СУБД")
print("-" * 80)

query_check = """
SELECT COUNT(*) as total_rows, 
       SUM(transition_2_to_4) as total_transitions,
       COUNT(DISTINCT day_u16) as days_with_data
FROM sim_masterv2 
WHERE group_by IN (1, 2)
"""

result = client.execute(query_check)
if result:
    total_rows, total_trans, days = result[0]
    print(f"Всего строк (планеры): {total_rows}")
    print(f"Всего переходов 2→4 в СУБД: {total_trans}")
    print(f"Дней с данными: {days}")
    
    if total_trans == 0:
        print("\n⚠️ ПРОБЛЕМА: transition_2_to_4 = 0 во всех строках!")
        print("   Возможные причины:")
        print("   - Поле compute_transitions не работает в baseline")
        print("   - Или модуль не включён в orchestrator")
        print("\n   Будем использовать ЛОГИ для анализа переходов 2→4")

# 2. Все дни с превышением квоты
print("\n[2] ВСЕ дни с превышением квоты (≥ 18)")
print("-" * 80)

query_violations = """
SELECT day_u16, COUNT(*) as in_repair 
FROM sim_masterv2 
WHERE state = 'repair' AND group_by IN (1, 2)
GROUP BY day_u16
HAVING in_repair >= 18
ORDER BY day_u16
"""

result = client.execute(query_violations)
if result:
    violations_18 = [row for row in result if row[1] == 18]
    violations_19 = [row for row in result if row[1] == 19]
    violations_20 = [row for row in result if row[1] >= 20]
    
    print(f"Дней с 18 агентами (квота): {len(violations_18)}")
    print(f"Дней с 19 агентами (+1): {len(violations_19)}")
    print(f"Дней с 20+ агентами (+2): {len(violations_20)}")
    print(f"\nВСЕГО дней с загрузкой ≥18: {len(result)}")
    
    if violations_20:
        print(f"\n⚠️ ПРЕВЫШЕНИЯ КВОТЫ (20+ агентов):")
        for day, count in violations_20:
            print(f"   День {day}: {count} агентов (превышение +{count-18})")
    
    if violations_19:
        print(f"\n⚠️ Близко к превышению (19 агентов):")
        # Группируем последовательные дни
        groups = []
        current_group = [violations_19[0][0]]
        
        for i in range(1, len(violations_19)):
            if violations_19[i][0] == current_group[-1] + 1:
                current_group.append(violations_19[i][0])
            else:
                groups.append(current_group)
                current_group = [violations_19[i][0]]
        groups.append(current_group)
        
        for group in groups:
            if len(group) > 1:
                print(f"   Дни {group[0]}-{group[-1]} ({len(group)} дней подряд)")
            else:
                print(f"   День {group[0]}")
else:
    print("✅ Квота никогда не превышена!")

# 3. Периоды с высокой загрузкой (17+ агентов)
print("\n[3] Периоды высокой загрузки (≥17 агентов)")
print("-" * 80)

query_high = """
SELECT day_u16, COUNT(*) as in_repair 
FROM sim_masterv2 
WHERE state = 'repair' AND group_by IN (1, 2)
GROUP BY day_u16
HAVING in_repair >= 17
ORDER BY day_u16
"""

result = client.execute(query_high)
if result:
    print(f"Всего дней с загрузкой ≥17: {len(result)}")
    
    # Группируем последовательные дни
    if result:
        groups = []
        current_group = [(result[0][0], result[0][1])]
        
        for i in range(1, len(result)):
            if result[i][0] == current_group[-1][0] + 1:
                current_group.append((result[i][0], result[i][1]))
            else:
                if len(current_group) > 5:  # Показываем периоды >5 дней
                    groups.append(current_group)
                current_group = [(result[i][0], result[i][1])]
        
        if len(current_group) > 5:
            groups.append(current_group)
        
        print(f"\nПериоды ≥17 агентов (длиной >5 дней):")
        for group in groups:
            start_day, start_count = group[0]
            end_day, end_count = group[-1]
            max_count = max(c for d, c in group)
            print(f"   Дни {start_day:4d}-{end_day:4d} ({len(group):3d} дней): max={max_count}")

# 4. Анализ из логов: переходы 2→4
print("\n[4] Анализ логов: первые 50 переходов 2→4")
print("-" * 80)

import re

log_file = "/tmp/sim_baseline_no_quota_repair_3650.log"
try:
    with open(log_file, 'r') as f:
        transitions_2_to_4 = []
        for line in f:
            if '[TRANSITION 2→4' in line:
                # Парсим: [TRANSITION 2→4 Day XXXX] AC YYYYY
                match = re.search(r'Day (\d+)\] AC (\d+)', line)
                if match:
                    day = int(match.group(1))
                    ac = int(match.group(2))
                    transitions_2_to_4.append((day, ac))
    
    if transitions_2_to_4:
        print(f"Найдено {len(transitions_2_to_4)} переходов 2→4 в логах")
        
        # Первые переходы
        print(f"\nПервые 20 переходов:")
        for day, ac in transitions_2_to_4[:20]:
            print(f"   День {day:4d}: AC {ac}")
        
        # Статистика по дням
        from collections import Counter
        day_counts = Counter(day for day, ac in transitions_2_to_4)
        
        print(f"\nДни с наибольшим количеством входов в ремонт:")
        for day, count in day_counts.most_common(20):
            print(f"   День {day:4d}: {count} входов")
        
        # Распределение по годам
        print(f"\nРаспределение входов в ремонт по годам:")
        for year in range(1, 11):
            start_day = (year - 1) * 365
            end_day = year * 365
            year_transitions = [d for d, ac in transitions_2_to_4 if start_day <= d < end_day]
            print(f"   Год {year}: {len(year_transitions)} входов")
    else:
        print("⚠️ Не найдено переходов 2→4 в логах")
        
except FileNotFoundError:
    print(f"⚠️ Файл логов не найден: {log_file}")
except Exception as e:
    print(f"⚠️ Ошибка чтения логов: {e}")

print("\n" + "=" * 80)
print("ДЕТАЛЬНЫЙ АНАЛИЗ ЗАВЕРШЁН")
print("=" * 80)

