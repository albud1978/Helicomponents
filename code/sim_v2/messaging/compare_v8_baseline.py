#!/usr/bin/env python3
"""
Сравнение V8 с baseline (sim_masterv2)

Метрики:
1. ops vs target по дням
2. Финальная структура агентов
3. Количество переходов
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'utils'))
from config_loader import get_clickhouse_client

def compare_v8_baseline(version_date: str = '2025-07-04'):
    """Сравнение V8 с baseline"""
    client = get_clickhouse_client()
    
    # Конвертируем дату в days since epoch
    from datetime import datetime
    vd = datetime.strptime(version_date, '%Y-%m-%d')
    version_date_days = (vd - datetime(1970, 1, 1)).days
    
    print(f"\n{'='*70}")
    print(f"📊 СРАВНЕНИЕ V8 vs BASELINE")
    print(f"   version_date: {version_date} (days={version_date_days})")
    print(f"{'='*70}\n")
    
    # ═══════════════════════════════════════════════════════════════════
    # 1. Структура агентов по дням (baseline)
    # ═══════════════════════════════════════════════════════════════════
    print("📊 1. Структура агентов по дням (baseline из СУБД):\n")
    
    query = f"""
    SELECT 
        day_u16,
        state,
        count(*) as cnt
    FROM sim_masterv2
    WHERE version_date = {version_date_days}
    GROUP BY day_u16, state
    ORDER BY day_u16, state
    """
    
    rows = client.execute(query)
    
    # Группируем по дням
    day_stats = {}
    for day, state, cnt in rows:
        if day not in day_stats:
            day_stats[day] = {}
        day_stats[day][state] = cnt
    
    # Выводим ключевые дни
    key_days = [0, 90, 180, 365, 730, 1095, 1825, 2555, 3285, 3650]
    print(f"{'День':>6} | {'ops':>5} | {'svc':>5} | {'unsvc':>5} | {'inact':>5} | {'repr':>5} | {'stor':>5} | {'ВСЕГО':>6}")
    print("-" * 70)
    
    for day in key_days:
        if day in day_stats:
            stats = day_stats[day]
            ops = stats.get('operations', 0)
            svc = stats.get('serviceable', 0)
            unsvc = stats.get('unserviceable', 0)
            inact = stats.get('inactive', 0)
            repair = stats.get('repair', 0)
            stor = stats.get('storage', 0)
            total = ops + svc + unsvc + inact + repair + stor
            print(f"{day:>6} | {ops:>5} | {svc:>5} | {unsvc:>5} | {inact:>5} | {repair:>5} | {stor:>5} | {total:>6}")
    
    # ═══════════════════════════════════════════════════════════════════
    # 2. ops vs target по Mi-8 и Mi-17
    # ═══════════════════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print("📊 2. OPS vs TARGET (baseline):\n")
    
    # Получаем target из flight_program_ac
    # Структура: dates (Date), ops_counter_mi8/mi17 (UInt16), version_date (Date)
    query_target = f"""
    SELECT 
        dateDiff('day', toDate('{version_date}'), dates) as day_offset,
        ops_counter_mi8,
        ops_counter_mi17
    FROM flight_program_ac
    WHERE version_date = toDate('{version_date}')
    AND dateDiff('day', toDate('{version_date}'), dates) IN (0, 90, 180, 365, 730, 1095, 1825, 2555, 3285, 3650)
    ORDER BY day_offset
    """
    
    target_rows = client.execute(query_target)
    target_by_day = {}
    for day, mi8_ops, mi17_ops in target_rows:
        target_by_day[day] = {'mi8': mi8_ops, 'mi17': mi17_ops}
    
    # Получаем actual ops по группам
    query_ops = f"""
    SELECT 
        day_u16,
        group_by,
        countIf(state = 'operations') as ops
    FROM sim_masterv2
    WHERE version_date = {version_date_days}
    AND day_u16 IN (0, 90, 180, 365, 730, 1095, 1825, 2555, 3285, 3650)
    GROUP BY day_u16, group_by
    ORDER BY day_u16, group_by
    """
    
    ops_rows = client.execute(query_ops)
    ops_by_day = {}
    for day, group_by, ops in ops_rows:
        if day not in ops_by_day:
            ops_by_day[day] = {}
        key = 'mi8' if group_by == 1 else 'mi17'
        ops_by_day[day][key] = ops
    
    print(f"{'День':>6} | {'Mi-8 ops':>9} | {'Mi-8 tgt':>9} | {'Mi-8 Δ':>7} | {'Mi-17 ops':>10} | {'Mi-17 tgt':>10} | {'Mi-17 Δ':>8}")
    print("-" * 85)
    
    for day in key_days:
        mi8_ops = ops_by_day.get(day, {}).get('mi8', 0)
        mi17_ops = ops_by_day.get(day, {}).get('mi17', 0)
        mi8_tgt = target_by_day.get(day, {}).get('mi8', 0)
        mi17_tgt = target_by_day.get(day, {}).get('mi17', 0)
        
        mi8_delta = mi8_ops - mi8_tgt
        mi17_delta = mi17_ops - mi17_tgt
        
        print(f"{day:>6} | {mi8_ops:>9} | {mi8_tgt:>9} | {mi8_delta:>+7} | {mi17_ops:>10} | {mi17_tgt:>10} | {mi17_delta:>+8}")
    
    # ═══════════════════════════════════════════════════════════════════
    # 3. Переходы в baseline (только существующие колонки)
    # ═══════════════════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print("📊 3. Переходы (baseline):\n")
    
    # Получаем список колонок transition_*
    query_cols = f"""
    SELECT name FROM system.columns 
    WHERE table = 'sim_masterv2' AND name LIKE 'transition_%'
    """
    cols = client.execute(query_cols)
    transition_cols = [c[0] for c in cols]
    
    for col in transition_cols:
        query = f"""
        SELECT sum({col}) as cnt
        FROM sim_masterv2
        WHERE version_date = {version_date_days}
        """
        result = client.execute(query)
        cnt = result[0][0] if result else 0
        if cnt > 0:
            print(f"  {col}: {cnt}")
    
    # ═══════════════════════════════════════════════════════════════════
    # 4. Финальная статистика
    # ═══════════════════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print("📊 4. Финальная статистика (день 3650):\n")
    
    query = f"""
    SELECT 
        state,
        count(*) as cnt,
        uniq(aircraft_number) as uniq_ac
    FROM sim_masterv2
    WHERE version_date = {version_date_days}
    AND day_u16 = 3650
    GROUP BY state
    ORDER BY cnt DESC
    """
    
    rows = client.execute(query)
    total = 0
    for state, cnt, uniq_ac in rows:
        print(f"  {state:15} : {cnt:>5} (uniq AC: {uniq_ac})")
        total += cnt
    print(f"  {'ВСЕГО':15} : {total:>5}")
    
    # ═══════════════════════════════════════════════════════════════════
    # 5. Spawn агенты в baseline
    # ═══════════════════════════════════════════════════════════════════
    print(f"\n{'='*70}")
    print("📊 5. Spawn агенты (baseline):\n")
    
    query = f"""
    SELECT 
        day_u16,
        count(*) as spawned
    FROM sim_masterv2
    WHERE version_date = {version_date_days}
    AND aircraft_number >= 100000
    GROUP BY day_u16
    ORDER BY day_u16
    LIMIT 20
    """
    
    rows = client.execute(query)
    if rows:
        print(f"  Первые spawn дни: {[f'd{d}={c}' for d, c in rows[:10]]}")
        
        # Всего spawn
        query_total = f"""
        SELECT uniq(aircraft_number)
        FROM sim_masterv2
        WHERE version_date = {version_date_days}
        AND aircraft_number >= 100000
        """
        total_spawn = client.execute(query_total)[0][0]
        print(f"  Всего spawn агентов: {total_spawn}")
    else:
        print("  Нет spawn агентов")
    
    print(f"\n{'='*70}")
    print("✅ Сравнение завершено")
    print(f"{'='*70}\n")
    
    return day_stats


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--version-date', default='2025-07-04')
    args = parser.parse_args()
    
    compare_v8_baseline(args.version_date)

