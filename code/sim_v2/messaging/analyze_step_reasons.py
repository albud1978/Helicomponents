#!/usr/bin/env python3
"""
Анализ причин адаптивных шагов в LIMITER V5.

Определяет, что определило каждый шаг:
- Limiter агента (SNE→LL или PPR→OH)
- Изменение программы (program_change)
"""

import sys
from pathlib import Path

# Добавляем пути
sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import numpy as np
from precompute_events import find_program_change_days
from sim_env_setup import prepare_env_arrays


def analyze_step_reasons(version_date: str = "2025-07-04", end_day: int = 3650):
    """Анализирует причины адаптивных шагов"""
    
    print(f"📊 Анализ причин адаптивных шагов для {version_date}")
    print(f"   Горизонт: {end_day} дней\n")
    
    # Загружаем данные
    from utils.config_loader import get_clickhouse_client
    client = get_clickhouse_client()
    env_data = prepare_env_arrays(client, version_date)
    frames = env_data['frames_total_u16']
    days = env_data['days_total_u16']
    
    # Program changes
    mp4_mi8 = env_data['mp4_ops_counter_mi8']
    mp4_mi17 = env_data['mp4_ops_counter_mi17']
    program_changes_raw = find_program_change_days(mp4_mi8, mp4_mi17)
    program_changes = [pc[0] for pc in program_changes_raw]  # Только дни
    program_changes_set = set(program_changes)
    print(f"📅 Program changes: {len(program_changes)} событий")
    print(f"   Первые 10: {program_changes[:10]}")
    
    # MP5 cumsum для расчёта limiter
    mp5_lin = np.array(env_data.get('mp5_daily_hours_linear', [0] * (frames * (days + 1))), dtype=np.uint16)
    mp5_cumsum = np.zeros(frames * (days + 1), dtype=np.uint32)
    
    # Вычисляем cumsum для каждого фрейма
    for f in range(frames):
        cumsum = 0
        for d in range(days + 1):
            cumsum += mp5_lin[d * frames + f]
            mp5_cumsum[d * frames + f] = cumsum
    
    # Загружаем агентов из heli_pandas
    
    agents_query = f"""
    SELECT 
        aircraft_number, status_id, sne, ppr, ll, oh,
        CASE WHEN bitAnd(ac_type_mask, 32) > 0 THEN 1 ELSE 2 END as group_by
    FROM heli_pandas
    WHERE version_date = '{version_date}'
      AND bitAnd(ac_type_mask, 96) > 0
    ORDER BY aircraft_number
    """
    agents = client.execute(agents_query)
    print(f"✈️  Агентов: {len(agents)}")
    
    # Строим frames_index
    frames_index = env_data.get('frames_index', {})
    
    # Инициализируем агентов
    agent_state = {}  # ac -> {'sne', 'ppr', 'll', 'oh', 'status', 'idx', 'limiter'}
    
    for ac, status, sne, ppr, ll, oh, group_by in agents:
        idx = frames_index.get(ac, -1)
        if idx < 0:
            continue
        
        # Рассчитываем начальный limiter для агентов в operations
        limiter = end_day
        if status == 2:  # operations
            remaining_ll = max(0, ll - sne)
            remaining_oh = max(0, oh - ppr)
            
            if idx < frames:
                # Бинарный поиск дня исчерпания ресурса
                base_cumsum = mp5_cumsum[0 * frames + idx]
                
                # Дни до LL
                days_to_ll = end_day
                lo, hi = 1, end_day
                while lo < hi:
                    mid = (lo + hi) // 2
                    acc = mp5_cumsum[mid * frames + idx] - base_cumsum
                    if acc >= remaining_ll:
                        hi = mid
                    else:
                        lo = mid + 1
                days_to_ll = lo
                
                # Дни до OH
                days_to_oh = end_day
                lo, hi = 1, end_day
                while lo < hi:
                    mid = (lo + hi) // 2
                    acc = mp5_cumsum[mid * frames + idx] - base_cumsum
                    if acc >= remaining_oh:
                        hi = mid
                    else:
                        lo = mid + 1
                days_to_oh = lo
                
                limiter = min(days_to_ll, days_to_oh)
        
        agent_state[ac] = {
            'sne': sne, 'ppr': ppr, 'll': ll, 'oh': oh,
            'status': status, 'idx': idx, 'limiter': limiter,
            'group_by': group_by
        }
    
    # Симуляция шагов
    current_day = 0
    steps = []
    
    reasons = {
        'limiter_oh': 0,   # Исчерпание OH (PPR)
        'limiter_ll': 0,   # Исчерпание LL (SNE)
        'program': 0,      # Изменение программы
        'end': 0           # Конец симуляции
    }
    
    while current_day < end_day:
        # Находим минимальный limiter среди агентов в operations
        min_limiter = end_day - current_day
        min_limiter_ac = None
        min_limiter_type = None
        
        for ac, state in agent_state.items():
            if state['status'] == 2:  # operations
                remaining = state['limiter'] - current_day
                if remaining > 0 and remaining < min_limiter:
                    min_limiter = remaining
                    min_limiter_ac = ac
                    # Определяем причину: OH или LL
                    remaining_ll = max(0, state['ll'] - state['sne'])
                    remaining_oh = max(0, state['oh'] - state['ppr'])
                    if remaining_oh <= remaining_ll:
                        min_limiter_type = 'oh'
                    else:
                        min_limiter_type = 'll'
        
        # Находим следующее изменение программы
        next_pc = end_day
        for pc in program_changes:
            if pc > current_day:
                next_pc = pc
                break
        
        days_to_pc = next_pc - current_day
        
        # Определяем adaptive_days
        if min_limiter <= days_to_pc:
            adaptive_days = min_limiter
            if min_limiter_type == 'oh':
                reason = 'limiter_oh'
            else:
                reason = 'limiter_ll'
        else:
            adaptive_days = days_to_pc
            reason = 'program'
        
        # Ограничение end_day
        if current_day + adaptive_days >= end_day:
            adaptive_days = end_day - current_day
            reason = 'end'
        
        steps.append({
            'step': len(steps),
            'current_day': current_day,
            'adaptive_days': adaptive_days,
            'reason': reason,
            'min_limiter': min_limiter,
            'next_pc': next_pc,
            'limiter_ac': min_limiter_ac
        })
        
        reasons[reason] += 1
        
        # Обновляем current_day
        current_day += adaptive_days
        
        # Упрощённое обновление состояний (без полной логики квотирования)
        # Просто декрементируем limiter для агентов в operations
        for ac, state in agent_state.items():
            if state['status'] == 2:
                # Обновляем SNE/PPR (упрощённо - без cumsum)
                if state['idx'] >= 0 and state['idx'] < frames:
                    idx = state['idx']
                    # dt за период
                    if current_day <= days:
                        dt = mp5_cumsum[current_day * frames + idx] - mp5_cumsum[(current_day - adaptive_days) * frames + idx]
                        state['sne'] += dt
                        state['ppr'] += dt
    
    # Результаты
    print(f"\n{'='*60}")
    print(f"📊 РЕЗУЛЬТАТЫ АНАЛИЗА ({len(steps)} шагов)")
    print(f"{'='*60}")
    
    print(f"\n🔢 Распределение причин:")
    print(f"   • Limiter OH (PPR→OH): {reasons['limiter_oh']} ({100*reasons['limiter_oh']/len(steps):.1f}%)")
    print(f"   • Limiter LL (SNE→LL): {reasons['limiter_ll']} ({100*reasons['limiter_ll']/len(steps):.1f}%)")
    print(f"   • Program change:      {reasons['program']} ({100*reasons['program']/len(steps):.1f}%)")
    print(f"   • End of simulation:   {reasons['end']} ({100*reasons['end']/len(steps):.1f}%)")
    
    # Статистика по adaptive_days
    adaptive_days_list = [s['adaptive_days'] for s in steps]
    print(f"\n📈 Статистика adaptive_days:")
    print(f"   • Min: {min(adaptive_days_list)}")
    print(f"   • Max: {max(adaptive_days_list)}")
    print(f"   • Avg: {np.mean(adaptive_days_list):.1f}")
    print(f"   • Median: {np.median(adaptive_days_list):.1f}")
    
    # Шаги с adaptive=1 (минимальные)
    single_day_steps = [s for s in steps if s['adaptive_days'] == 1]
    print(f"\n⚡ Шаги с adaptive_days=1: {len(single_day_steps)}")
    if single_day_steps[:5]:
        print(f"   Первые 5:")
        for s in single_day_steps[:5]:
            print(f"     Step {s['step']}: day={s['current_day']}, reason={s['reason']}, ac={s['limiter_ac']}")
    
    # Шаги по program_change
    pc_steps = [s for s in steps if s['reason'] == 'program']
    print(f"\n📅 Шаги по program_change: {len(pc_steps)}")
    if pc_steps[:5]:
        print(f"   Первые 5:")
        for s in pc_steps[:5]:
            print(f"     Step {s['step']}: day={s['current_day']} → {s['current_day'] + s['adaptive_days']} (pc={s['next_pc']})")
    
    return steps, reasons


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--version-date", default="2025-07-04")
    parser.add_argument("--end-day", type=int, default=3650)
    args = parser.parse_args()
    
    analyze_step_reasons(args.version_date, args.end_day)

