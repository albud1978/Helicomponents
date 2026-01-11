#!/usr/bin/env python3
"""
Валидация LIMITER V3 симуляции

Проверяет:
1. Σsne(last_day) - Σsne(day_0) = Σdt (до минуты)
2. dt = программа для каждого периода в ops
3. Статистика по состояниям

Запуск:
    python3 validate_limiter_v3.py [--version-date 2025-07-04] [--table sim_masterv2_limiter]
"""
import os
import sys
import argparse
from datetime import date

# Добавляем путь к utils
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_CODE_DIR = os.path.join(_SCRIPT_DIR, '..', '..')
sys.path.insert(0, _CODE_DIR)

from utils.config_loader import get_clickhouse_client


def validate_sne_dt_match(client, table: str, version_date: str) -> bool:
    """Проверка: Δsne = Σdt (до минуты)"""
    print("\n" + "=" * 60)
    print("1. ПРОВЕРКА: Δsne = Σdt")
    print("=" * 60)
    
    # Формат version_date в СУБД: YYYYMMDD
    vd_int = int(version_date.replace('-', ''))
    
    # Σsne день 0
    sne_0 = client.execute(f'SELECT sum(sne) FROM {table} WHERE day_u16 = 0 AND version_date = {vd_int}')[0][0] or 0
    
    # Последний день
    last_day = client.execute(f'SELECT max(day_u16) FROM {table} WHERE version_date = {vd_int}')[0][0] or 0
    sne_last = client.execute(f'SELECT sum(sne) FROM {table} WHERE day_u16 = {last_day} AND version_date = {vd_int}')[0][0] or 0
    
    # Δsne
    delta_sne = sne_last - sne_0
    
    # Σdt
    total_dt = client.execute(f'SELECT sum(dt) FROM {table} WHERE version_date = {vd_int}')[0][0] or 0
    
    print(f"Σsne день 0:      {sne_0:>15,} мин")
    print(f"Σsne день {last_day}: {sne_last:>15,} мин")
    print(f"Δsne:             {delta_sne:>15,} мин = {delta_sne/60:,.0f} ч")
    print("-" * 60)
    print(f"Σdt:              {total_dt:>15,} мин = {total_dt/60:,.0f} ч")
    print(f"Δ(Δsne - Σdt):    {delta_sne - total_dt:>+15,} мин")
    
    if delta_sne == total_dt:
        print("\n✅ СОШЛОСЬ ДО МИНУТЫ!")
        return True
    else:
        print(f"\n❌ ОШИБКА: Разница {delta_sne - total_dt:,} мин")
        return False


def validate_dt_vs_program(client, table: str, version_date: str, sample_ac: int = None) -> bool:
    """Проверка: dt = программа для ПОСЛЕДОВАТЕЛЬНЫХ дней в ops"""
    print("\n" + "=" * 60)
    print("2. ПРОВЕРКА: dt = программа (для последовательных ops периодов)")
    print("=" * 60)
    
    # Формат version_date в СУБД: YYYYMMDD
    vd_int = int(version_date.replace('-', ''))
    
    # Берём случайный агент с большим налётом если не указан
    if sample_ac is None:
        sample_ac = client.execute(f'''
            SELECT aircraft_number 
            FROM {table} 
            WHERE state = 'operations' AND version_date = {vd_int}
            GROUP BY aircraft_number 
            ORDER BY sum(dt) DESC 
            LIMIT 1
        ''')[0][0]
    
    print(f"Проверяемый агент: AC {sample_ac}")
    
    # Все записи ops для агента
    ops_recs = client.execute(f'''
        SELECT day_u16, dt, sne
        FROM {table}
        WHERE aircraft_number = {sample_ac} AND state = 'operations' AND version_date = {vd_int}
        ORDER BY day_u16
    ''')
    
    if not ops_recs:
        print(f"❌ Нет записей ops для AC {sample_ac}")
        return False
    
    errors = 0
    total_dt = 0
    total_prog_valid = 0
    valid_periods = 0
    skipped = 0
    
    print(f"\n{'Период':>12} | {'dt':>10} | {'prog':>10} | {'Статус':>8}")
    print("-" * 50)
    
    for i, (day, dt, sne) in enumerate(ops_recs):
        prev_day = ops_recs[i-1][0] if i > 0 else 0
        
        # Проверяем только ПОСЛЕДОВАТЕЛЬНЫЕ дни ops (без перерывов)
        # Если prev_day != day - 1 (или небольшой adaptive step), агент был в другом состоянии
        # В этом случае dt=0 на первом шаге после возврата - это ПРАВИЛЬНО
        
        # Проверяем был ли агент в ops ВСЕ дни между prev_day и day
        gap = day - prev_day
        
        # Если gap > 100 и dt=0, это возврат в ops после перерыва - пропускаем
        if gap > 100 and dt == 0:
            skipped += 1
            if skipped <= 3:
                print(f"  {prev_day:>4}-{day:<4} | {dt:>10,} | {'(пропуск)':>10} | ⏭️ возврат")
            continue
        
        # Программа за период
        prog = client.execute(f'''
            SELECT sum(daily_hours)
            FROM flight_program_fl
            WHERE version_date = '{version_date}'
              AND aircraft_number = {sample_ac}
              AND toRelativeDayNum(dates) - toRelativeDayNum(toDate('{version_date}')) >= {prev_day}
              AND toRelativeDayNum(dates) - toRelativeDayNum(toDate('{version_date}')) < {day}
        ''')[0][0] or 0
        
        total_dt += dt
        total_prog_valid += prog
        valid_periods += 1
        
        if dt != prog:
            errors += 1
            if errors <= 5:  # Показать первые 5 ошибок
                print(f"  {prev_day:>4}-{day:<4} | {dt:>10,} | {prog:>10,} | ❌")
        elif valid_periods <= 10:  # Показать первые 10 успешных
            print(f"  {prev_day:>4}-{day:<4} | {dt:>10,} | {prog:>10,} | ✓")
    
    print("-" * 50)
    print(f"Σdt = {total_dt:,} | Σprog = {total_prog_valid:,}")
    print(f"Проверено: {valid_periods} периодов, пропущено (возвраты): {skipped}")
    
    if errors == 0:
        print(f"\n✅ Все {valid_periods} последовательных периодов совпадают с программой!")
        return True
    else:
        print(f"\n❌ ОШИБКИ: {errors} из {valid_periods} периодов не совпадают")
        return False


def validate_ops_vs_target(client, table: str, version_date: str) -> bool:
    """Проверка: количество в operations vs target по программе"""
    print("\n" + "=" * 60)
    print("3. ПРОВЕРКА: ops_count vs target (квотирование)")
    print("=" * 60)
    
    # Формат version_date в СУБД: YYYYMMDD
    vd_int = int(version_date.replace('-', ''))
    
    # Получаем количество в ops на каждый день из симуляции (раздельно Mi-8 и Mi-17)
    ops_by_day = client.execute(f'''
        SELECT 
            day_u16,
            countIf(state = 'operations' AND group_by = 1) as mi8_ops,
            countIf(state = 'operations' AND group_by = 2) as mi17_ops
        FROM {table}
        WHERE version_date = {vd_int}
        GROUP BY day_u16
        ORDER BY day_u16
    ''')
    
    # Строим словарь: day -> (mi8_ops, mi17_ops)
    ops_dict = {}
    for day, mi8, mi17 in ops_by_day:
        ops_dict[day] = (mi8, mi17)
    
    # Получаем target из программы (flight_program_ac)
    targets = client.execute(f'''
        SELECT 
            toRelativeDayNum(dates) - toRelativeDayNum(toDate('{version_date}')) as day_num,
            ops_counter_mi8,
            ops_counter_mi17
        FROM flight_program_ac
        WHERE version_date = '{version_date}'
        ORDER BY day_num
    ''')
    
    # Строим словарь: day -> (mi8_tgt, mi17_tgt)
    target_dict = {}
    for day, mi8_tgt, mi17_tgt in targets:
        target_dict[day] = (mi8_tgt, mi17_tgt)
    
    # Уникальные дни из симуляции
    sim_days = sorted(ops_dict.keys())
    
    # Считаем отклонения
    mi8_diffs = []
    mi17_diffs = []
    
    print(f"\n{'День':>6} | {'Mi-8 ops':>8} | {'Mi-8 tgt':>8} | {'Δ':>5} | {'Mi-17 ops':>9} | {'Mi-17 tgt':>9} | {'Δ':>5}")
    print("-" * 75)
    
    for i, day in enumerate(sim_days[:20]):  # Показать первые 20 дней
        mi8_ops, mi17_ops = ops_dict.get(day, (0, 0))
        
        # target берётся на day+1 (прогноз на следующий день)
        mi8_tgt, mi17_tgt = target_dict.get(day + 1, (0, 0))
        
        d8 = mi8_ops - mi8_tgt
        d17 = mi17_ops - mi17_tgt
        
        mi8_diffs.append(d8)
        mi17_diffs.append(d17)
        
        print(f"{day:>6} | {mi8_ops:>8} | {mi8_tgt:>8} | {d8:>+5} | {mi17_ops:>9} | {mi17_tgt:>9} | {d17:>+5}")
    
    # Собираем все отклонения
    for day in sim_days[20:]:
        mi8_ops, mi17_ops = ops_dict.get(day, (0, 0))
        mi8_tgt, mi17_tgt = target_dict.get(day + 1, (0, 0))
        mi8_diffs.append(mi8_ops - mi8_tgt)
        mi17_diffs.append(mi17_ops - mi17_tgt)
    
    if len(sim_days) > 20:
        print(f"  ... ещё {len(sim_days) - 20} дней ...")
    
    # Последние 5 дней
    print("-" * 75)
    for day in sim_days[-5:]:
        mi8_ops, mi17_ops = ops_dict.get(day, (0, 0))
        mi8_tgt, mi17_tgt = target_dict.get(day + 1, (0, 0))
        d8 = mi8_ops - mi8_tgt
        d17 = mi17_ops - mi17_tgt
        print(f"{day:>6} | {mi8_ops:>8} | {mi8_tgt:>8} | {d8:>+5} | {mi17_ops:>9} | {mi17_tgt:>9} | {d17:>+5}")
    
    print("-" * 75)
    
    # Статистика отклонений
    mi8_max = max(mi8_diffs) if mi8_diffs else 0
    mi8_min = min(mi8_diffs) if mi8_diffs else 0
    mi17_max = max(mi17_diffs) if mi17_diffs else 0
    mi17_min = min(mi17_diffs) if mi17_diffs else 0
    
    # Среднее отклонение
    mi8_avg = sum(mi8_diffs) / len(mi8_diffs) if mi8_diffs else 0
    mi17_avg = sum(mi17_diffs) / len(mi17_diffs) if mi17_diffs else 0
    
    print(f"\nСтатистика отклонений (ops - target):")
    print(f"  Mi-8:  min={mi8_min:+}, max={mi8_max:+}, avg={mi8_avg:+.1f}")
    print(f"  Mi-17: min={mi17_min:+}, max={mi17_max:+}, avg={mi17_avg:+.1f}")
    
    # Считаем дни с дефицитом (ops < target)
    mi8_deficit_days = sum(1 for d in mi8_diffs if d < 0)
    mi17_deficit_days = sum(1 for d in mi17_diffs if d < 0)
    
    print(f"\nДни с дефицитом (ops < target):")
    print(f"  Mi-8:  {mi8_deficit_days} из {len(mi8_diffs)} дней ({mi8_deficit_days/len(mi8_diffs)*100:.1f}%)")
    print(f"  Mi-17: {mi17_deficit_days} из {len(mi17_diffs)} дней ({mi17_deficit_days/len(mi17_diffs)*100:.1f}%)")
    
    # Проверяем причину дефицита: есть ли inactive которые можно поднять?
    # Дефицит из-за unserviceable (в ремонте) — НЕ баг квотирования, а физическое ограничение
    last_day = max(sim_days)
    inactive_check = client.execute(f'''
        SELECT 
            countIf(group_by = 1 AND state = 'inactive') as mi8_inactive,
            countIf(group_by = 2 AND state = 'inactive') as mi17_inactive,
            countIf(group_by = 1 AND state = 'unserviceable') as mi8_unsvc,
            countIf(group_by = 2 AND state = 'unserviceable') as mi17_unsvc
        FROM {table}
        WHERE day_u16 = {last_day}
    ''')[0]
    mi8_inactive, mi17_inactive, mi8_unsvc, mi17_unsvc = inactive_check
    
    print(f"\nНа последний день ({last_day}):")
    print(f"  Mi-8:  inactive={mi8_inactive}, unserviceable={mi8_unsvc}")
    print(f"  Mi-17: inactive={mi17_inactive}, unserviceable={mi17_unsvc}")
    
    # Критерий успеха:
    # - Если inactive=0 и дефицит только из-за unserviceable — квотирование работает
    # - Если есть inactive при дефиците — квотирование НЕ работает
    mi8_quota_ok = (mi8_inactive == 0) or (mi8_deficit_days == 0)
    mi17_quota_ok = (mi17_inactive == 0) or (mi17_deficit_days == 0)
    
    if mi8_quota_ok and mi17_quota_ok:
        print(f"\n✅ Квотирование работает корректно!")
        if mi17_unsvc > 0 and mi17_deficit_days > 0:
            print(f"   (дефицит Mi-17 из-за {mi17_unsvc} агентов в ремонте — физическое ограничение)")
        return True
    else:
        print(f"\n❌ ОШИБКА квотирования:")
        if not mi8_quota_ok:
            print(f"  Mi-8: {mi8_inactive} inactive НЕ подняты при дефиците!")
        if not mi17_quota_ok:
            print(f"  Mi-17: {mi17_inactive} inactive НЕ подняты при дефиците!")
        return False


def show_state_statistics(client, table: str, version_date: str):
    """Статистика по состояниям"""
    print("\n" + "=" * 60)
    print("4. СТАТИСТИКА ПО СОСТОЯНИЯМ")
    print("=" * 60)
    
    # Формат version_date в СУБД: YYYYMMDD
    vd_int = int(version_date.replace('-', ''))
    
    # Общая статистика
    total_stats = client.execute(f'''
        SELECT 
            count() as total_rows,
            uniqExact(aircraft_number) as unique_ac,
            min(day_u16) as min_day,
            max(day_u16) as max_day,
            sum(dt) as total_dt
        FROM {table}
        WHERE version_date = {vd_int}
    ''')[0]
    
    print(f"Всего записей:    {total_stats[0]:,}")
    print(f"Уникальных AC:    {total_stats[1]}")
    print(f"Дни:              {total_stats[2]} - {total_stats[3]}")
    print(f"Σdt:              {total_stats[4]:,} мин = {total_stats[4]/60:,.0f} ч")
    
    # По состояниям (последний день)
    last_day = total_stats[3]
    state_stats = client.execute(f'''
        SELECT state, count(), sum(dt)
        FROM {table}
        WHERE day_u16 = {last_day} AND version_date = {vd_int}
        GROUP BY state
        ORDER BY count() DESC
    ''')
    
    print(f"\n{'Состояние':>15} | {'Агентов':>8} | {'Σdt':>12}")
    print("-" * 45)
    total_agents = 0
    for state, cnt, dt in state_stats:
        print(f"{state:>15} | {cnt:>8} | {dt:>12,}")
        total_agents += cnt
    print("-" * 45)
    print(f"{'ИТОГО':>15} | {total_agents:>8}")


def main():
    parser = argparse.ArgumentParser(description='Валидация LIMITER V3')
    parser.add_argument('--version-date', default='2025-07-04', help='Дата версии')
    parser.add_argument('--table', default='sim_masterv2_limiter', help='Таблица для валидации')
    parser.add_argument('--sample-ac', type=int, help='AC для детальной проверки dt')
    args = parser.parse_args()
    
    print("=" * 60)
    print("ВАЛИДАЦИЯ LIMITER V3")
    print(f"Таблица: {args.table}")
    print(f"Версия: {args.version_date}")
    print("=" * 60)
    
    client = get_clickhouse_client()
    
    # Проверки
    check1 = validate_sne_dt_match(client, args.table, args.version_date)
    check2 = validate_dt_vs_program(client, args.table, args.version_date, args.sample_ac)
    check3 = validate_ops_vs_target(client, args.table, args.version_date)
    show_state_statistics(client, args.table, args.version_date)
    
    # Итог
    print("\n" + "=" * 60)
    print("ИТОГ ВАЛИДАЦИИ")
    print("=" * 60)
    
    all_passed = check1 and check2 and check3
    if all_passed:
        print("✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ!")
    else:
        print("❌ ЕСТЬ ОШИБКИ:")
        if not check1:
            print("  - Δsne != Σdt")
        if not check2:
            print("  - dt != программа для некоторых периодов")
        if not check3:
            print("  - ops_count != target (квотирование)")
    
    return 0 if all_passed else 1


if __name__ == '__main__':
    sys.exit(main())

