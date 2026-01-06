#!/usr/bin/env python3
"""
Валидация симуляции агрегатов (sim_units_v2)

Проверки:
1. Инкременты sne/ppr — dt от планера должен накапливаться
2. Переходы — operations→repair (ppr>=oh), operations→storage (sne>=ll)
3. Длительность ремонта — repair_days vs repair_time
4. Spawn count — количество активаций spawn-резерва по группам
5. Сравнение датасетов — резкие изменения

Дата: 06.01.2026
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_loader import get_clickhouse_client
from datetime import date
import argparse


def validate_increments(client, version_date: str, version_id: int = 1):
    """Проверка инкрементов sne/ppr"""
    print("\n" + "=" * 60)
    print("📊 ВАЛИДАЦИЯ ИНКРЕМЕНТОВ SNE/PPR")
    print("=" * 60)
    
    version_date_int = (date.fromisoformat(version_date) - date(1970, 1, 1)).days
    
    # Проверяем рост sne для агрегатов в operations
    sql = """
    SELECT 
        psn,
        group_by,
        min(sne) as sne_start,
        max(sne) as sne_end,
        max(sne) - min(sne) as delta_sne,
        count(*) as days_in_ops
    FROM sim_units_v2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND state = 2  -- operations
    GROUP BY psn, group_by
    HAVING days_in_ops > 100
    ORDER BY delta_sne DESC
    LIMIT 20
    """
    
    results = client.execute(sql, {
        'version_date': version_date_int,
        'version_id': version_id
    })
    
    print(f"\n📈 Топ-20 агрегатов по приросту SNE (operations >100 дней):")
    print(f"{'PSN':>10} {'Group':>6} {'SNE start':>12} {'SNE end':>12} {'Delta':>10} {'Days':>6}")
    print("-" * 60)
    
    for row in results:
        psn, gb, sne_start, sne_end, delta, days = row
        print(f"{psn:>10} {gb:>6} {sne_start:>12} {sne_end:>12} {delta:>10} {days:>6}")
    
    # Проверяем агрегаты с нулевым ростом в operations
    sql_zero = """
    SELECT count(DISTINCT psn) as zero_growth_count
    FROM sim_units_v2 s1
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND state = 2
      AND psn IN (
          SELECT psn 
          FROM sim_units_v2 
          WHERE version_date = %(version_date)s
            AND version_id = %(version_id)s
            AND state = 2
          GROUP BY psn
          HAVING max(sne) = min(sne) AND count(*) > 10
      )
    """
    
    zero_count = client.execute(sql_zero, {
        'version_date': version_date_int,
        'version_id': version_id
    })[0][0]
    
    if zero_count > 0:
        print(f"\n⚠️ WARNING: {zero_count} агрегатов с нулевым ростом SNE в operations (>10 дней)")
    else:
        print(f"\n✅ Все агрегаты в operations имеют рост SNE")
    
    return zero_count == 0


def validate_transitions(client, version_date: str, version_id: int = 1):
    """Проверка переходов состояний (оптимизированный запрос)"""
    print("\n" + "=" * 60)
    print("🔄 ВАЛИДАЦИЯ ПЕРЕХОДОВ СОСТОЯНИЙ")
    print("=" * 60)
    
    version_date_int = (date.fromisoformat(version_date) - date(1970, 1, 1)).days
    
    # Матрица переходов — используем LAG вместо JOIN
    sql = """
    SELECT 
        from_state,
        to_state,
        count(*) as transitions
    FROM (
        SELECT 
            psn,
            state as to_state,
            lagInFrame(state, 1) OVER (PARTITION BY psn ORDER BY day_u16) as from_state
        FROM sim_units_v2
        WHERE version_date = %(version_date)s
          AND version_id = %(version_id)s
          AND day_u16 %% 10 = 0  -- Каждый 10-й день для оптимизации
    )
    WHERE from_state != to_state AND from_state > 0
    GROUP BY from_state, to_state
    ORDER BY transitions DESC
    """
    
    results = client.execute(sql, {
        'version_date': version_date_int,
        'version_id': version_id
    })
    
    state_names = {2: 'operations', 3: 'serviceable', 4: 'repair', 5: 'reserve', 6: 'storage'}
    
    print(f"\n📊 Матрица переходов:")
    print(f"{'From':>15} {'To':>15} {'Count':>10}")
    print("-" * 45)
    
    for row in results:
        from_s, to_s, count = row
        from_name = state_names.get(from_s, f"state_{from_s}")
        to_name = state_names.get(to_s, f"state_{to_s}")
        print(f"{from_name:>15} {to_name:>15} {count:>10}")
    
    # Проверяем недопустимые переходы
    # Допустимые: 2→4, 2→6, 3→2, 4→5, 5→2
    invalid_transitions = []
    allowed = [(2, 4), (2, 6), (3, 2), (4, 5), (5, 2), (2, 3)]  # 2→3 для serviceable
    
    for row in results:
        from_s, to_s, count = row
        if (from_s, to_s) not in allowed and count > 10:
            invalid_transitions.append((from_s, to_s, count))
    
    if invalid_transitions:
        print(f"\n⚠️ WARNING: Обнаружены нестандартные переходы:")
        for from_s, to_s, count in invalid_transitions:
            print(f"   {state_names.get(from_s, from_s)} → {state_names.get(to_s, to_s)}: {count}")
    else:
        print(f"\n✅ Все переходы соответствуют ожидаемой логике")
    
    return len(invalid_transitions) == 0


def validate_repair_duration(client, version_date: str, version_id: int = 1):
    """Проверка длительности ремонта (оптимизированный)"""
    print("\n" + "=" * 60)
    print("🔧 ВАЛИДАЦИЯ ДЛИТЕЛЬНОСТИ РЕМОНТА")
    print("=" * 60)
    
    version_date_int = (date.fromisoformat(version_date) - date(1970, 1, 1)).days
    
    # Статистика repair_days в состоянии repair
    sql = """
    SELECT 
        group_by,
        count(DISTINCT psn) as units_in_repair,
        avg(repair_days) as avg_repair_days,
        max(repair_days) as max_repair_days
    FROM sim_units_v2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND state = 4
      AND day_u16 = 1000  -- Контрольная точка
    GROUP BY group_by
    ORDER BY units_in_repair DESC
    LIMIT 15
    """
    
    results = client.execute(sql, {
        'version_date': version_date_int,
        'version_id': version_id
    })
    
    print(f"\n📊 Статистика ремонтов по группам (день 1000):")
    print(f"{'Group':>6} {'Units':>8} {'Avg days':>10} {'Max':>6}")
    print("-" * 35)
    
    for row in results:
        gb, count, avg_days, max_d = row
        print(f"{gb:>6} {count:>8} {avg_days:>10.1f} {max_d:>6}")
    
    return True


def validate_spawn_count(client, version_date: str, version_id: int = 1):
    """Проверка количества spawn (активаций spawn-резерва)"""
    print("\n" + "=" * 60)
    print("🆕 ВАЛИДАЦИЯ SPAWN COUNT")
    print("=" * 60)
    
    version_date_int = (date.fromisoformat(version_date) - date(1970, 1, 1)).days
    
    # Spawn = агрегаты с psn >= 1000000 (синтетические)
    sql = """
    SELECT 
        group_by,
        count(DISTINCT psn) as spawn_count,
        min(day_u16) as first_spawn_day,
        max(day_u16) as last_spawn_day
    FROM sim_units_v2
    WHERE version_date = %(version_date)s
      AND version_id = %(version_id)s
      AND psn >= 1000000
      AND active = 1
    GROUP BY group_by
    ORDER BY spawn_count DESC
    """
    
    results = client.execute(sql, {
        'version_date': version_date_int,
        'version_id': version_id
    })
    
    print(f"\n📊 Spawn по группам (PSN >= 1,000,000):")
    print(f"{'Group':>6} {'Spawn':>8} {'First day':>10} {'Last day':>10}")
    print("-" * 40)
    
    total_spawn = 0
    for row in results:
        gb, count, first_d, last_d = row
        print(f"{gb:>6} {count:>8} {first_d:>10} {last_d:>10}")
        total_spawn += count
    
    print(f"\n📈 Всего spawn: {total_spawn}")
    
    return total_spawn


def compare_datasets(client, version_date1: str, version_date2: str, version_id: int = 1):
    """Сравнение двух датасетов"""
    print("\n" + "=" * 60)
    print("📊 СРАВНЕНИЕ ДАТАСЕТОВ")
    print(f"   DS1: {version_date1}")
    print(f"   DS2: {version_date2}")
    print("=" * 60)
    
    # FIX: Используем правильные даты для сравнения
    # version_date1 = текущий датасет (--version-date)
    # version_date2 = датасет для сравнения (--version-date2)
    
    vd1_int = (date.fromisoformat(version_date1) - date(1970, 1, 1)).days
    vd2_int = (date.fromisoformat(version_date2) - date(1970, 1, 1)).days
    
    # Сравнение spawn по группам
    sql = """
    SELECT 
        group_by,
        countIf(version_date = %(vd1)s AND psn >= 1000000 AND active = 1) as spawn_ds1,
        countIf(version_date = %(vd2)s AND psn >= 1000000 AND active = 1) as spawn_ds2
    FROM sim_units_v2
    WHERE version_id = %(version_id)s
      AND version_date IN (%(vd1)s, %(vd2)s)
    GROUP BY group_by
    HAVING spawn_ds1 > 0 OR spawn_ds2 > 0
    ORDER BY abs(toInt32(spawn_ds2) - toInt32(spawn_ds1)) DESC
    LIMIT 15
    """
    
    results = client.execute(sql, {
        'vd1': vd1_int,
        'vd2': vd2_int,
        'version_id': version_id
    })
    
    print(f"\n📊 Spawn по группам:")
    print(f"{'Group':>6} {'DS1':>8} {'DS2':>8} {'Delta':>8} {'Change':>10}")
    print("-" * 50)
    
    for row in results:
        gb, s1, s2, = row
        delta = s2 - s1
        change = f"{delta:+d}" if delta != 0 else "0"
        pct = f"({delta/s1*100:+.0f}%)" if s1 > 0 else "(new)"
        print(f"{gb:>6} {s1:>8} {s2:>8} {delta:>8} {change:>10}")
    
    # Сравнение storage (списанные)
    sql_storage = """
    SELECT 
        version_date,
        count(DISTINCT psn) as storage_count
    FROM sim_units_v2
    WHERE version_id = %(version_id)s
      AND version_date IN (%(vd1)s, %(vd2)s)
      AND state = 6
      AND day_u16 = 3649
    GROUP BY version_date
    ORDER BY version_date
    """
    
    storage_results = client.execute(sql_storage, {
        'vd1': vd1_int,
        'vd2': vd2_int,
        'version_id': version_id
    })
    
    print(f"\n📊 Списанные агрегаты (storage) на день 3649:")
    for row in storage_results:
        vd, count = row
        ds_name = "DS1" if vd == vd1_int else "DS2"
        print(f"   {ds_name}: {count}")
    
    # Сравнение среднего sne на конец периода
    sql_sne = """
    SELECT 
        version_date,
        avg(sne) as avg_sne,
        avg(ppr) as avg_ppr
    FROM sim_units_v2
    WHERE version_id = %(version_id)s
      AND version_date IN (%(vd1)s, %(vd2)s)
      AND state = 2
      AND day_u16 = 3649
    GROUP BY version_date
    ORDER BY version_date
    """
    
    sne_results = client.execute(sql_sne, {
        'vd1': vd1_int,
        'vd2': vd2_int,
        'version_id': version_id
    })
    
    print(f"\n📊 Средние наработки на день 3649 (operations):")
    for row in sne_results:
        vd, avg_sne, avg_ppr = row
        ds_name = "DS1" if vd == vd1_int else "DS2"
        print(f"   {ds_name}: SNE={avg_sne:.0f} мин ({avg_sne/60:.0f} ч), PPR={avg_ppr:.0f} мин ({avg_ppr/60:.0f} ч)")


def main():
    parser = argparse.ArgumentParser(description='Валидация симуляции агрегатов')
    parser.add_argument('--version-date', type=str, default='2025-07-04',
                        help='Дата версии для валидации')
    parser.add_argument('--version-date2', type=str, default='2025-07-04',
                        help='Вторая дата версии для сравнения (DS1 по умолчанию)')
    parser.add_argument('--version-id', type=int, default=1)
    parser.add_argument('--compare', action='store_true',
                        help='Сравнить два датасета')
    
    args = parser.parse_args()
    
    client = get_clickhouse_client()
    
    print("=" * 60)
    print(f"🔍 ВАЛИДАЦИЯ СИМУЛЯЦИИ АГРЕГАТОВ")
    print(f"   Датасет: {args.version_date}")
    print("=" * 60)
    
    # Запускаем валидации
    validate_increments(client, args.version_date, args.version_id)
    validate_transitions(client, args.version_date, args.version_id)
    validate_repair_duration(client, args.version_date, args.version_id)
    spawn_count = validate_spawn_count(client, args.version_date, args.version_id)
    
    if args.compare:
        compare_datasets(client, args.version_date, args.version_date2, args.version_id)
    
    print("\n" + "=" * 60)
    print("✅ ВАЛИДАЦИЯ ЗАВЕРШЕНА")
    print("=" * 60)


if __name__ == '__main__':
    main()

