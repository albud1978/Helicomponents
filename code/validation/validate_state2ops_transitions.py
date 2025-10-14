#!/usr/bin/env python3
"""
Валидация ЭТАП 1: state_2_operations
Метод 1: Проверка количества переходов intent_state

Проверяет:
1. Количество агентов в operations на начало
2. Количество агентов с intent=2 на конец
3. Количество переходов 2→4 и 2→6
4. Корректность логики intent для каждого типа планеров
"""

import sys
from pathlib import Path

# Добавляем путь к utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config_loader import get_clickhouse_client


def main():
    client = get_clickhouse_client()
    
    print("=" * 80)
    print("ВАЛИДАЦИЯ ЭТАП 1: state_2_operations")
    print("Метод 1: Проверка количества переходов intent_state")
    print("=" * 80)
    
    # 1. Начальное состояние (день 0)
    print("\n1. НАЧАЛЬНОЕ СОСТОЯНИЕ (день 0)")
    print("-" * 80)
    
    result = client.execute("""
    SELECT 
        state,
        COUNT(*) AS cnt
    FROM sim_masterv2
    WHERE day_u16 = 0
    GROUP BY state
    ORDER BY state
    """)
    
    print(f"{'State':>12} | {'Count':>8}")
    print("-" * 25)
    total_agents = 0
    operations_start = 0
    for row in result:
        print(f"{row[0]:>12} | {row[1]:>8}")
        total_agents += row[1]
        if row[0] == 'operations':
            operations_start = row[1]
    
    print(f"{'ВСЕГО':>12} | {total_agents:>8}")
    print(f"\n✅ Агентов в operations на начало: {operations_start}")
    
    # 2. Конечное состояние (день 3649)
    print("\n2. КОНЕЧНОЕ СОСТОЯНИЕ (день 3649)")
    print("-" * 80)
    
    result = client.execute("""
    SELECT 
        group_by,
        CASE WHEN group_by = 1 THEN 'Mi-8' ELSE 'Mi-17' END AS type,
        state,
        intent_state,
        COUNT(*) AS cnt
    FROM sim_masterv2
    WHERE day_u16 = 3649
    GROUP BY group_by, state, intent_state
    ORDER BY group_by, state, intent_state
    """)
    
    print(f"{'Type':>10} | {'State':>12} | {'Intent':>6} | {'Count':>6}")
    print("-" * 45)
    ops_intent2_end = 0
    for row in result:
        print(f"{row[1]:>10} | {row[2]:>12} | {row[3]:>6} | {row[4]:>6}")
        if row[2] == 'operations' and row[3] == 2:
            ops_intent2_end += row[4]
    
    print(f"\n✅ Агентов в operations с intent=2 на конец: {ops_intent2_end}")
    
    # 3. Количество переходов
    print("\n3. КОЛИЧЕСТВО ПЕРЕХОДОВ 2→4 и 2→6")
    print("-" * 80)
    
    result = client.execute("""
    WITH transitions AS (
        SELECT 
            idx,
            group_by,
            day_u16,
            intent_state,
            lagInFrame(intent_state) OVER (PARTITION BY idx ORDER BY day_u16) AS prev_intent
        FROM sim_masterv2
        WHERE state = 'operations'
    )
    SELECT 
        group_by,
        CASE WHEN group_by = 1 THEN 'Mi-8' ELSE 'Mi-17' END AS type,
        COUNT(CASE WHEN prev_intent = 2 AND intent_state = 4 THEN 1 END) AS transitions_to_4,
        COUNT(CASE WHEN prev_intent = 2 AND intent_state = 6 THEN 1 END) AS transitions_to_6,
        COUNT(DISTINCT CASE WHEN prev_intent = 2 AND intent_state = 4 THEN idx END) AS agents_to_4,
        COUNT(DISTINCT CASE WHEN prev_intent = 2 AND intent_state = 6 THEN idx END) AS agents_to_6
    FROM transitions
    GROUP BY group_by
    ORDER BY group_by
    """)
    
    print(f"{'Type':>10} | {'→4':>6} | {'→6':>6} | {'Agents→4':>10} | {'Agents→6':>10}")
    print("-" * 55)
    total_to_4 = 0
    total_to_6 = 0
    for row in result:
        print(f"{row[1]:>10} | {row[2]:>6} | {row[3]:>6} | {row[4]:>10} | {row[5]:>10}")
        total_to_4 += row[2]
        total_to_6 += row[3]
    
    total_transitions = total_to_4 + total_to_6
    expected_transitions = operations_start - ops_intent2_end
    
    print(f"\n{'ИТОГО':>10} | {total_to_4:>6} | {total_to_6:>6} | {total_transitions:>10}")
    
    # 4. Проверка баланса
    print("\n4. ПРОВЕРКА БАЛАНСА")
    print("-" * 80)
    print(f"Агентов в operations на начало:     {operations_start}")
    print(f"Агентов с intent=2 на конец:        {ops_intent2_end}")
    print(f"Ожидается переходов:                {expected_transitions}")
    print(f"Фактических переходов:              {total_transitions}")
    
    if total_transitions == expected_transitions:
        print(f"\n✅ БАЛАНС СХОДИТСЯ: {total_transitions} = {expected_transitions}")
    else:
        print(f"\n❌ ОШИБКА БАЛАНСА: {total_transitions} ≠ {expected_transitions}")
        return 1
    
    # 5. Проверка логики intent для operations
    print("\n5. ПРОВЕРКА ЛОГИКИ INTENT (state=operations)")
    print("-" * 80)
    
    result = client.execute("""
    SELECT 
        group_by,
        CASE WHEN group_by = 1 THEN 'Mi-8' ELSE 'Mi-17' END AS type,
        COUNT(*) AS total_intent_4,
        SUM(CASE WHEN (ppr + dn) >= oh AND (sne + dn) < br THEN 0 ELSE 1 END) AS errors_4
    FROM sim_masterv2
    WHERE intent_state = 4 AND state = 'operations'
    GROUP BY group_by
    ORDER BY group_by
    """)
    
    print("Intent=4 (repair):")
    print(f"{'Type':>10} | {'Total':>10} | {'Errors':>10}")
    print("-" * 35)
    errors_4 = 0
    for row in result:
        status = "✅ OK" if row[3] == 0 else f"❌ {row[3]}"
        print(f"{row[1]:>10} | {row[2]:>10} | {status:>10}")
        errors_4 += row[3]
    
    result = client.execute("""
    SELECT 
        group_by,
        CASE WHEN group_by = 1 THEN 'Mi-8' ELSE 'Mi-17' END AS type,
        COUNT(*) AS total_intent_6,
        SUM(CASE WHEN (sne + dn) >= ll OR ((ppr + dn) >= oh AND (sne + dn) >= br) THEN 0 ELSE 1 END) AS errors_6
    FROM sim_masterv2
    WHERE intent_state = 6 AND state = 'operations'
    GROUP BY group_by
    ORDER BY group_by
    """)
    
    print("\nIntent=6 (storage):")
    print(f"{'Type':>10} | {'Total':>10} | {'Errors':>10}")
    print("-" * 35)
    errors_6 = 0
    for row in result:
        status = "✅ OK" if row[3] == 0 else f"❌ {row[3]}"
        print(f"{row[1]:>10} | {row[2]:>10} | {status:>10}")
        errors_6 += row[3]
    
    # Итоговый результат
    print("\n" + "=" * 80)
    if errors_4 == 0 and errors_6 == 0 and total_transitions == expected_transitions:
        print("✅ ВАЛИДАЦИЯ УСПЕШНА: Все проверки пройдены!")
        print("=" * 80)
        return 0
    else:
        print("❌ ВАЛИДАЦИЯ ПРОВАЛЕНА: Обнаружены ошибки!")
        print("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(main())



