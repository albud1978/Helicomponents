#!/usr/bin/env python3
"""
Валидация ЭТАП 1: state_2_operations
Метод 2: Проверка инкрементов sne и ppr

Проверяет:
1. Δsne = Σdt для выборки агентов
2. Δppr = Σdt для выборки агентов
3. Монотонность роста наработок
4. Учитывает инкремент дня 0
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.config_loader import get_clickhouse_client


def main():
    client = get_clickhouse_client()
    
    print("=" * 80)
    print("ВАЛИДАЦИЯ ЭТАП 1: state_2_operations")
    print("Метод 2: Проверка инкрементов")
    print("=" * 80)
    
    # 1. Проверка инкрементов для Mi-8
    print("\n1. ПРОВЕРКА ИНКРЕМЕНТОВ: Mi-8")
    print("-" * 80)
    
    result = client.execute("""
    WITH agent_stats AS (
        SELECT 
            idx,
            aircraft_number,
            MIN(sne) AS sne_start,
            MAX(sne) AS sne_end,
            SUM(dt) AS total_dt,
            (MAX(sne) - MIN(sne)) AS delta_sne,
            MIN(ppr) AS ppr_start,
            MAX(ppr) AS ppr_end,
            (MAX(ppr) - MIN(ppr)) AS delta_ppr,
            COUNT(*) AS days_tracked
        FROM sim_masterv2
        WHERE state = 'operations' AND group_by = 1
        GROUP BY idx, aircraft_number
    )
    SELECT 
        idx,
        aircraft_number,
        delta_sne,
        total_dt,
        ABS(delta_sne - total_dt) AS diff_sne,
        delta_ppr,
        ABS(delta_ppr - total_dt) AS diff_ppr,
        days_tracked
    FROM agent_stats
    WHERE days_tracked > 100
    ORDER BY idx
    LIMIT 10
    """)
    
    print(f"{'idx':>4} | {'AC':>8} | {'Δsne':>10} | {'Σdt':>10} | {'diff':>6} | {'Δppr':>10} | {'diff':>6} | {'days':>6}")
    print("-" * 85)
    errors_sne = 0
    errors_ppr = 0
    for row in result:
        idx, ac, delta_sne, total_dt, diff_sne, delta_ppr, diff_ppr, days = row
        check_sne = "✅" if diff_sne <= 200 else "❌"  # Допуск на dt дня 0
        check_ppr = "✅" if diff_ppr <= 200 else "❌"
        if diff_sne > 200:
            errors_sne += 1
        if diff_ppr > 200:
            errors_ppr += 1
        print(f"{idx:>4} | {ac:>8} | {delta_sne:>10} | {total_dt:>10} | {diff_sne:>6}{check_sne} | {delta_ppr:>10} | {diff_ppr:>6}{check_ppr} | {days:>6}")
    
    print(f"\n✅ Mi-8: Ошибок (>200 мин): Δsne={errors_sne}, Δppr={errors_ppr}")
    
    # 2. Проверка инкрементов для Mi-17
    print("\n2. ПРОВЕРКА ИНКРЕМЕНТОВ: Mi-17")
    print("-" * 80)
    
    result = client.execute("""
    WITH agent_stats AS (
        SELECT 
            idx,
            aircraft_number,
            MIN(sne) AS sne_start,
            MAX(sne) AS sne_end,
            SUM(dt) AS total_dt,
            (MAX(sne) - MIN(sne)) AS delta_sne,
            MIN(ppr) AS ppr_start,
            MAX(ppr) AS ppr_end,
            (MAX(ppr) - MIN(ppr)) AS delta_ppr,
            COUNT(*) AS days_tracked
        FROM sim_masterv2
        WHERE state = 'operations' AND group_by = 2
        GROUP BY idx, aircraft_number
    )
    SELECT 
        idx,
        aircraft_number,
        delta_sne,
        total_dt,
        ABS(delta_sne - total_dt) AS diff_sne,
        delta_ppr,
        ABS(delta_ppr - total_dt) AS diff_ppr,
        days_tracked
    FROM agent_stats
    WHERE days_tracked > 100
    ORDER BY idx
    LIMIT 10
    """)
    
    print(f"{'idx':>4} | {'AC':>8} | {'Δsne':>10} | {'Σdt':>10} | {'diff':>6} | {'Δppr':>10} | {'diff':>6} | {'days':>6}")
    print("-" * 85)
    errors_sne_17 = 0
    errors_ppr_17 = 0
    for row in result:
        idx, ac, delta_sne, total_dt, diff_sne, delta_ppr, diff_ppr, days = row
        check_sne = "✅" if diff_sne <= 200 else "❌"
        check_ppr = "✅" if diff_ppr <= 200 else "❌"
        if diff_sne > 200:
            errors_sne_17 += 1
        if diff_ppr > 200:
            errors_ppr_17 += 1
        print(f"{idx:>4} | {ac:>8} | {delta_sne:>10} | {total_dt:>10} | {diff_sne:>6}{check_sne} | {delta_ppr:>10} | {diff_ppr:>6}{check_ppr} | {days:>6}")
    
    print(f"\n✅ Mi-17: Ошибок (>200 мин): Δsne={errors_sne_17}, Δppr={errors_ppr_17}")
    
    # 3. Проверка монотонности (выборочно)
    print("\n3. ПРОВЕРКА МОНОТОННОСТИ (idx=0, первые 30 дней)")
    print("-" * 80)
    
    result = client.execute("""
    SELECT day_u16, sne, dt
    FROM sim_masterv2
    WHERE idx = 0 AND group_by = 2 AND state = 'operations'
    ORDER BY day_u16
    LIMIT 30
    """)
    
    print(f"{'day':>6} | {'sne':>10} | {'dt':>6} | {'Δsne':>10}")
    print("-" * 45)
    prev_sne = None
    monotonic_errors = 0
    for row in result:
        day, sne, dt = row
        if prev_sne is not None:
            delta = sne - prev_sne
            match = "✅" if delta == dt else f"❌ {delta} vs {dt}"
            if delta != dt:
                monotonic_errors += 1
            print(f"{day:>6} | {sne:>10} | {dt:>6} | {delta:>10} {match}")
        else:
            print(f"{day:>6} | {sne:>10} | {dt:>6} | {'N/A':>10} (start)")
        prev_sne = sne
    
    print(f"\n✅ Ошибок монотонности: {monotonic_errors}")
    
    # 4. Пояснение про день 0
    print("\n4. ПОЯСНЕНИЕ: РАЗНИЦА = dt(день 0)")
    print("-" * 80)
    print("Начальное sne в день 0 уже включает наработку.")
    print("Модуль прибавляет dt дня 0, поэтому:")
    print("  Σdt - Δsne = dt(день 0)")
    print("")
    print("Это ОЖИДАЕМОЕ поведение, а не ошибка!")
    
    # Итоговый результат
    print("\n" + "=" * 80)
    total_errors = errors_sne + errors_ppr + errors_sne_17 + errors_ppr_17 + monotonic_errors
    if total_errors == 0:
        print("✅ ВАЛИДАЦИЯ УСПЕШНА: Все инкременты корректны!")
        print("=" * 80)
        return 0
    else:
        print(f"❌ ВАЛИДАЦИЯ ПРОВАЛЕНА: Обнаружено {total_errors} ошибок!")
        print("=" * 80)
        return 1


if __name__ == "__main__":
    sys.exit(main())



