#!/usr/bin/env python3
"""
Adaptive 2.0: Предварительное вычисление ProgramEvent

Создаёт агентов-событий из mp4_ops_counter.
Каждое изменение target → отдельный ProgramEvent агент.

Дата: 10.01.2026
"""
import numpy as np
from typing import List, Tuple, Dict
from clickhouse_driver import Client


def extract_program_events(client: Client, version_date: str, max_days: int = 3650) -> List[Dict]:
    """
    Извлекает события изменения программы из mp4_ops_counter.
    
    Returns:
        List[Dict]: Список событий с полями:
            - event_day: int
            - target_mi8: int
            - target_mi17: int
    """
    # Запрос данных
    query = f"""
    SELECT 
        day_u16,
        ops_counter_mi8,
        ops_counter_mi17
    FROM flight_program_ac
    WHERE version_date = toDate('{version_date}')
    ORDER BY day_u16
    LIMIT {max_days + 1}
    """
    
    rows = client.execute(query)
    
    if not rows:
        print(f"  ⚠️ Нет данных flight_program_ac для {version_date}")
        return []
    
    events = []
    prev_mi8 = None
    prev_mi17 = None
    
    for day, mi8, mi17 in rows:
        # Первый день — всегда событие
        if prev_mi8 is None:
            events.append({
                'event_day': int(day),
                'target_mi8': int(mi8),
                'target_mi17': int(mi17)
            })
            prev_mi8 = mi8
            prev_mi17 = mi17
            continue
        
        # Событие при изменении любого target
        if mi8 != prev_mi8 or mi17 != prev_mi17:
            events.append({
                'event_day': int(day),
                'target_mi8': int(mi8),
                'target_mi17': int(mi17)
            })
            prev_mi8 = mi8
            prev_mi17 = mi17
    
    print(f"  ✅ Найдено {len(events)} ProgramEvent за {max_days} дней")
    return events


def create_program_event_array(events: List[Dict], max_events: int = 500) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Создаёт массивы для MacroProperty из списка событий.
    
    Returns:
        Tuple[np.ndarray, np.ndarray, np.ndarray]:
            - event_days: дни событий
            - target_mi8: targets Mi-8
            - target_mi17: targets Mi-17
    """
    n = min(len(events), max_events)
    
    event_days = np.zeros(max_events, dtype=np.uint16)
    target_mi8 = np.zeros(max_events, dtype=np.uint16)
    target_mi17 = np.zeros(max_events, dtype=np.uint16)
    
    for i in range(n):
        event_days[i] = events[i]['event_day']
        target_mi8[i] = events[i]['target_mi8']
        target_mi17[i] = events[i]['target_mi17']
    
    # Заполнить остаток MAX значениями (не участвуют в min)
    for i in range(n, max_events):
        event_days[i] = 0xFFFF
        target_mi8[i] = 0
        target_mi17[i] = 0
    
    return event_days, target_mi8, target_mi17


def compute_limiter_date_ops(
    idx: int,
    current_day: int,
    sne: int,
    ppr: int,
    ll: int,
    oh: int,
    mp5_cumsum: np.ndarray,
    max_days: int = 4000
) -> int:
    """
    Вычисляет limiter_date для агента в operations.
    
    Использует бинарный поиск по mp5_cumsum для точного определения дня.
    
    Args:
        idx: Индекс агента
        current_day: Текущий день
        sne, ppr: Текущие значения ресурсов
        ll, oh: Лимиты
        mp5_cumsum: Кумулятивная сумма [idx * (max_days+1) + day]
        
    Returns:
        limiter_date: День когда агент достигнет лимита
    """
    MAX_DAYS_PLUS_1 = max_days + 1
    base_idx = idx * MAX_DAYS_PLUS_1
    
    # Текущий cumsum
    base_cumsum = mp5_cumsum[base_idx + current_day]
    
    # Горизонт по SNE (ll)
    horizon_sne = 3650
    remaining_sne = ll - sne if ll > sne else 0
    if remaining_sne > 0:
        target = base_cumsum + remaining_sne
        # Бинарный поиск
        lo, hi = current_day, min(current_day + 3650, max_days)
        while lo < hi:
            mid = (lo + hi) // 2
            if mp5_cumsum[base_idx + mid] < target:
                lo = mid + 1
            else:
                hi = mid
        horizon_sne = lo - current_day if lo > current_day else 1
    
    # Горизонт по PPR (oh) — аналогично, ppr += dt как sne
    horizon_ppr = 3650
    remaining_ppr = oh - ppr if oh > ppr else 0
    if remaining_ppr > 0:
        target = base_cumsum + remaining_ppr
        lo, hi = current_day, min(current_day + 3650, max_days)
        while lo < hi:
            mid = (lo + hi) // 2
            if mp5_cumsum[base_idx + mid] < target:
                lo = mid + 1
            else:
                hi = mid
        horizon_ppr = lo - current_day if lo > current_day else 1
    
    # Минимальный горизонт
    horizon = min(horizon_sne, horizon_ppr)
    if horizon < 1:
        horizon = 1
    
    return current_day + horizon


def compute_limiter_date_repair(
    current_day: int,
    repair_days: int,
    repair_time: int
) -> int:
    """
    Вычисляет limiter_date для агента в repair.
    
    Returns:
        limiter_date: День завершения ремонта
    """
    remaining = repair_time - repair_days if repair_time > repair_days else 0
    if remaining < 1:
        remaining = 1
    return current_day + remaining


# ═══════════════════════════════════════════════════════════════════════════
# Тестирование
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    import os
    
    client = Client(
        host=os.environ.get('CLICKHOUSE_HOST', 'localhost'),
        port=int(os.environ.get('CLICKHOUSE_PORT', 9000)),
        user=os.environ.get('CLICKHOUSE_USER', 'default'),
        password=os.environ.get('CLICKHOUSE_PASSWORD', '')
    )
    
    print("=== Тест ProgramEvent ===")
    events = extract_program_events(client, '2025-07-04', max_days=3650)
    
    print(f"\nПервые 10 событий:")
    for e in events[:10]:
        print(f"  Day {e['event_day']}: Mi-8={e['target_mi8']}, Mi-17={e['target_mi17']}")
    
    print(f"\nПоследние 5 событий:")
    for e in events[-5:]:
        print(f"  Day {e['event_day']}: Mi-8={e['target_mi8']}, Mi-17={e['target_mi17']}")
    
    # Создание массивов
    days, mi8, mi17 = create_program_event_array(events)
    print(f"\nМассивы: days[0:10]={days[:10]}, mi8[0:10]={mi8[:10]}")

