#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (P1 cleanup): устарел/дубликат, SSoT-замена: inv10_turnover_balance.py
"""
Валидация ЭТАП 1: state_2_operations
Метод 1: Проверка переходов (intent_state или state для V8)

Проверяет:
1. Количество агентов в operations на начало
2. Для V7: количество агентов с intent=2 на конец
3. Для V7: количество переходов 2→4 и 2→6
4. Для V7: корректность логики intent
5. Для V8: корректность переходов ops→storage/unsvc по state
"""

import argparse
import sys
from pathlib import Path

# Добавляем путь к utils
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config_loader import get_clickhouse_client


def split_table_name(table: str):
    if "." in table:
        db_name, table_name = table.split(".", 1)
        return db_name, table_name
    return None, table


def has_column(client, table: str, column_name: str) -> bool:
    db_name, table_name = split_table_name(table)
    if db_name:
        query = (
            "SELECT count() FROM system.columns "
            f"WHERE database = '{db_name}' AND table = '{table_name}' "
            f"AND name = '{column_name}'"
        )
    else:
        query = (
            "SELECT count() FROM system.columns "
            "WHERE database = currentDatabase() "
            f"AND table = '{table_name}' AND name = '{column_name}'"
        )
    return client.execute(query)[0][0] > 0


def main():
    parser = argparse.ArgumentParser(
        description="Валидация: state_2_operations (переходы intent/state)"
    )
    parser.add_argument(
        "--table",
        default="sim_masterv2_v8",
        help="Таблица ClickHouse (по умолчанию: sim_masterv2_v8)",
    )
    args = parser.parse_args()
    table = args.table
    client = get_clickhouse_client()
    table_base = table.split(".")[-1]
    has_intent_state = has_column(client, table, "intent_state")
    use_state_transitions = (table_base == "sim_masterv2_v8") or (not has_intent_state)
    has_debug_step = has_column(client, table, "debug_step")
    order_column = "debug_step" if has_debug_step else "day_u16"
    debug_step_select = (
        "debug_step" if has_debug_step else "CAST(NULL AS UInt32) AS debug_step"
    )
    
    print("=" * 80)
    print("ВАЛИДАЦИЯ ЭТАП 1: state_2_operations")
    if use_state_transitions:
        print("Метод 1: Проверка переходов по state (V8, без intent_state)")
    else:
        print("Метод 1: Проверка количества переходов intent_state")
    print("=" * 80)
    
    # 1. Начальное состояние (день 0)
    print("\n1. НАЧАЛЬНОЕ СОСТОЯНИЕ (день 0)")
    print("-" * 80)
    
    result = client.execute(f"""
    SELECT 
        state,
        COUNT(*) AS cnt
    FROM {table}
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

    if use_state_transitions:
        print("\n2. ПРОВЕРКА ПЕРЕХОДОВ OPS→STORAGE/UNSERVICEABLE (V8)")
        print("-" * 80)
        print(f"Порядок lagInFrame: {order_column}")

        result = client.execute(f"""
        WITH transitions AS (
            SELECT 
                idx,
                group_by,
                day_u16,
                {debug_step_select},
                state,
                sne,
                ppr,
                daily_next_u32,
                ll,
                oh,
                br,
                lagInFrame(state) OVER (PARTITION BY idx ORDER BY {order_column}) AS prev_state
            FROM {table}
        )
        SELECT 
            group_by,
            CASE WHEN group_by = 1 THEN 'Mi-8' ELSE 'Mi-17' END AS type,
            countIf(prev_state = 'operations' AND state = 'storage') AS transitions_storage,
            sum(
                prev_state = 'operations'
                AND state = 'storage'
                AND NOT (
                    sne + daily_next_u32 >= ll
                    OR (
                        ppr + daily_next_u32 >= oh
                        AND sne + daily_next_u32 >= br
                    )
                )
            ) AS errors_storage,
            countIf(prev_state = 'operations' AND state = 'unserviceable') AS transitions_unsvc,
            sum(
                prev_state = 'operations'
                AND state = 'unserviceable'
                AND NOT (
                    ppr + daily_next_u32 >= oh
                    AND sne + daily_next_u32 < br
                )
            ) AS errors_unsvc
        FROM transitions
        GROUP BY group_by
        ORDER BY group_by
        """)

        print(f"{'Type':>10} | {'→storage':>10} | {'Errors':>6} | {'→unsvc':>8} | {'Errors':>6}")
        print("-" * 60)
        total_errors_storage = 0
        total_errors_unsvc = 0
        for row in result:
            type_name = row[1]
            transitions_storage = row[2]
            errors_storage = row[3]
            transitions_unsvc = row[4]
            errors_unsvc = row[5]
            print(
                f"{type_name:>10} | {transitions_storage:>10} | {errors_storage:>6} | "
                f"{transitions_unsvc:>8} | {errors_unsvc:>6}"
            )
            total_errors_storage += errors_storage
            total_errors_unsvc += errors_unsvc

        total_errors = total_errors_storage + total_errors_unsvc
        print(f"\n✅ Ошибок storage: {total_errors_storage}")
        print(f"✅ Ошибок unsvc:   {total_errors_unsvc}")

        print("\nПРИМЕРЫ ПЕРЕХОДОВ OPS→STORAGE/UNSVC (первые 10)")
        result = client.execute(f"""
        WITH transitions AS (
            SELECT 
                idx,
                group_by,
                day_u16,
                {debug_step_select},
                state,
                lagInFrame(state) OVER (PARTITION BY idx ORDER BY {order_column}) AS prev_state
            FROM {table}
        )
        SELECT 
            idx,
            group_by,
            day_u16,
            debug_step,
            prev_state,
            state
        FROM transitions
        WHERE prev_state = 'operations' AND state IN ('storage', 'unserviceable')
        ORDER BY {order_column}
        LIMIT 10
        """)

        print(f"{'idx':>6} | {'Type':>10} | {'day':>6} | {'step':>10} | {'prev':>12} | {'state':>12}")
        print("-" * 70)
        for row in result:
            idx, group_by, day_u16, debug_step, prev_state, state = row
            type_name = "Mi-8" if group_by == 1 else "Mi-17"
            step_value = debug_step if debug_step is not None else "-"
            print(
                f"{idx:>6} | {type_name:>10} | {day_u16:>6} | {step_value:>10} | "
                f"{prev_state:>12} | {state:>12}"
            )

        print("\n" + "=" * 80)
        if total_errors == 0:
            print("✅ ВАЛИДАЦИЯ УСПЕШНА: Все переходы корректны!")
            print("=" * 80)
            return 0
        print(f"❌ ВАЛИДАЦИЯ ПРОВАЛЕНА: Обнаружено {total_errors} ошибок!")
        print("=" * 80)
        return 1
    
    # 2. Конечное состояние (день 3649)
    print("\n2. КОНЕЧНОЕ СОСТОЯНИЕ (день 3649)")
    print("-" * 80)
    
    result = client.execute(f"""
    SELECT 
        group_by,
        CASE WHEN group_by = 1 THEN 'Mi-8' ELSE 'Mi-17' END AS type,
        state,
        intent_state,
        COUNT(*) AS cnt
    FROM {table}
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
    
    result = client.execute(f"""
    WITH transitions AS (
        SELECT 
            idx,
            group_by,
            day_u16,
            intent_state,
            lagInFrame(intent_state) OVER (PARTITION BY idx ORDER BY day_u16) AS prev_intent
        FROM {table}
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
    
    result = client.execute(f"""
    SELECT 
        group_by,
        CASE WHEN group_by = 1 THEN 'Mi-8' ELSE 'Mi-17' END AS type,
        COUNT(*) AS total_intent_4,
        SUM(CASE WHEN (ppr + dn) >= oh AND (sne + dn) < br THEN 0 ELSE 1 END) AS errors_4
    FROM {table}
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
    
    result = client.execute(f"""
    SELECT 
        group_by,
        CASE WHEN group_by = 1 THEN 'Mi-8' ELSE 'Mi-17' END AS type,
        COUNT(*) AS total_intent_6,
        SUM(CASE WHEN (sne + dn) >= ll OR ((ppr + dn) >= oh AND (sne + dn) >= br) THEN 0 ELSE 1 END) AS errors_6
    FROM {table}
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



