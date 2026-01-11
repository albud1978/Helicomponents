#!/usr/bin/env python3
"""
Предрасчёт событий для адаптивного шага симуляции

Вычисляет:
1. program_change_days — дни изменения target (mp4_ops_counter)
2. mp5_cumsum — кумулятивные суммы dt для быстрого расчёта sum(dt[a:b])
"""
import numpy as np
from typing import Dict, List, Tuple, Optional


def find_program_change_days(mp4_mi8: List[int], mp4_mi17: List[int]) -> List[Tuple[int, int, int]]:
    """
    Найти все дни, когда target меняется
    
    Returns:
        List[(day, target_mi8, target_mi17)] — ~12 изменений/год = ~120 за 10 лет
    """
    changes = []
    
    if not mp4_mi8 or not mp4_mi17:
        return changes
    
    prev_mi8, prev_mi17 = int(mp4_mi8[0]), int(mp4_mi17[0])
    
    # День 0 — первое значение
    changes.append((0, prev_mi8, prev_mi17))
    
    for day in range(1, min(len(mp4_mi8), len(mp4_mi17))):
        curr_mi8, curr_mi17 = int(mp4_mi8[day]), int(mp4_mi17[day])
        
        if curr_mi8 != prev_mi8 or curr_mi17 != prev_mi17:
            changes.append((day, curr_mi8, curr_mi17))
            prev_mi8, prev_mi17 = curr_mi8, curr_mi17
    
    return changes


def compute_mp5_cumsum(mp5_lin: np.ndarray, frames: int, days: int) -> np.ndarray:
    """
    Кумулятивная сумма dt для быстрого расчёта sum(dt[a:b])
    
    cumsum[f, d] = sum(mp5[f, 0:d])
    sum(mp5[f, a:b]) = cumsum[f, b] - cumsum[f, a]
    
    Args:
        mp5_lin: Линейный массив [days * frames] дневных наработок (формат FLAME GPU)
        frames: Количество агентов
        days: Количество дней
        
    Returns:
        np.ndarray: Кумулятивные суммы [frames * (days + 1)]
    """
    if len(mp5_lin) == 0:
        return np.zeros(frames * (days + 1), dtype=np.uint32)
    
    # mp5_lin имеет формат [day * frames + frame] (day-major)
    # Кумулятивная сумма в том же формате: cumsum[d * frames + f] = sum(mp5[0:d, f])
    
    # Reshape в [days, frames] чтобы cumsum по axis=0 (дням)
    mp5_2d = np.zeros((days, frames), dtype=np.uint32)
    for d in range(days):
        for f in range(frames):
            src_idx = d * frames + f
            if src_idx < len(mp5_lin):
                mp5_2d[d, f] = mp5_lin[src_idx]
    
    # Кумулятивная сумма по дням (axis=0), добавляем начальную строку нулей
    cumsum_2d = np.zeros((days + 1, frames), dtype=np.uint32)
    cumsum_2d[1:, :] = np.cumsum(mp5_2d, axis=0)
    
    # Flatten в day-major: [day0_frame0, day0_frame1, ..., day1_frame0, ...]
    return cumsum_2d.flatten()


def find_next_program_change(program_changes: List[Tuple[int, int, int]], 
                              current_day: int) -> Tuple[int, int, int]:
    """
    Найти ближайшее изменение программы после current_day
    
    Returns:
        (days_until_change, target_mi8, target_mi17)
    """
    for day, mi8, mi17 in program_changes:
        if day > current_day:
            return (day - current_day, mi8, mi17)
    
    # Если изменений больше нет — возвращаем большое число
    return (999999, 0, 0)


def compute_days_to_resource_limit(
    sne: int, ppr: int, ll: int, oh: int,
    mp5_cumsum: np.ndarray, idx: int, days: int, current_day: int
) -> int:
    """
    Вычислить количество дней до достижения ресурсного лимита
    
    Args:
        sne, ppr: Текущая наработка
        ll, oh: Лимиты
        mp5_cumsum: Кумулятивные суммы dt
        idx: Индекс агента
        days: Общее количество дней
        current_day: Текущий день
        
    Returns:
        Количество дней до лимита (или 999999 если не достигнет)
    """
    remaining_sne = max(0, ll - sne)
    remaining_ppr = max(0, oh - ppr)
    min_remaining = min(remaining_sne, remaining_ppr)
    
    if min_remaining == 0:
        return 0  # Уже на лимите
    
    # Бинарный поиск дня, когда cumsum >= min_remaining
    base = idx * (days + 1)
    start_cumsum = mp5_cumsum[base + current_day]
    
    for d in range(current_day + 1, days + 1):
        delta_dt = mp5_cumsum[base + d] - start_cumsum
        if delta_dt >= min_remaining:
            return d - current_day
    
    return 999999  # Не достигнет лимита в рамках симуляции


class EventPrecomputer:
    """Класс для предрасчёта событий адаптивного шага"""
    
    def __init__(self, env_data: Dict):
        self.env_data = env_data
        self.frames = int(env_data.get('frames_total_u16', 0))
        self.days = int(env_data.get('days_total_u16', 3650))
        
        # Кэшированные данные
        self._program_changes: Optional[List[Tuple[int, int, int]]] = None
        self._mp5_cumsum: Optional[np.ndarray] = None
    
    @property
    def program_changes(self) -> List[Tuple[int, int, int]]:
        """Дни изменения программы (lazy computation)"""
        if self._program_changes is None:
            mp4_mi8 = self.env_data.get('mp4_ops_counter_mi8', [])
            mp4_mi17 = self.env_data.get('mp4_ops_counter_mi17', [])
            self._program_changes = find_program_change_days(mp4_mi8, mp4_mi17)
            print(f"  📅 Найдено {len(self._program_changes)} изменений программы за {self.days} дней")
        return self._program_changes
    
    @property
    def mp5_cumsum(self) -> np.ndarray:
        """Кумулятивные суммы dt (lazy computation)"""
        if self._mp5_cumsum is None:
            mp5_lin = self.env_data.get('mp5_lin', np.zeros(self.frames * self.days))
            self._mp5_cumsum = compute_mp5_cumsum(mp5_lin, self.frames, self.days)
            print(f"  📊 Вычислены cumsum для {self.frames} агентов × {self.days} дней")
        return self._mp5_cumsum
    
    def get_next_event_day(self, current_day: int, agents_data: List[Dict]) -> int:
        """
        Найти минимальный день следующего события
        
        Args:
            current_day: Текущий день
            agents_data: Список данных агентов [{idx, sne, ppr, ll, oh, state}, ...]
            
        Returns:
            Количество дней до следующего события
        """
        # 1. Программный лимитер
        program_days, _, _ = find_next_program_change(self.program_changes, current_day)
        min_days = program_days
        
        # 2. Ресурсный лимитер (только для агентов в operations)
        for agent in agents_data:
            if agent.get('state') != 'operations':
                continue
            
            days_to_limit = compute_days_to_resource_limit(
                sne=agent['sne'],
                ppr=agent['ppr'],
                ll=agent['ll'],
                oh=agent['oh'],
                mp5_cumsum=self.mp5_cumsum,
                idx=agent['idx'],
                days=self.days,
                current_day=current_day
            )
            min_days = min(min_days, days_to_limit)
        
        # 3. Ремонтный лимитер (для агентов в repair)
        for agent in agents_data:
            if agent.get('state') != 'repair':
                continue
            
            repair_time = agent.get('repair_time', 180)
            repair_days = agent.get('repair_days', 0)
            days_to_repair = max(0, repair_time - repair_days)
            min_days = min(min_days, days_to_repair)
        
        # Ограничение: не более 365 дней за шаг
        min_days = min(min_days, 365)
        min_days = max(min_days, 1)  # Минимум 1 день
        
        return min_days


if __name__ == "__main__":
    # Тест
    print("=== Тест precompute_events ===")
    
    # Имитация данных
    mp4_mi8 = [68] * 28 + [64] * 28 + [68] * 34  # 90 дней
    mp4_mi17 = [88] * 56 + [90] * 34  # 90 дней
    
    changes = find_program_change_days(mp4_mi8, mp4_mi17)
    print(f"Program changes: {changes}")
    
    # Тест cumsum
    frames, days = 3, 10
    mp5 = np.array([
        [10, 10, 10, 10, 10, 10, 10, 10, 10, 10],  # Агент 0
        [5, 5, 5, 5, 5, 5, 5, 5, 5, 5],            # Агент 1
        [20, 20, 20, 20, 20, 20, 20, 20, 20, 20],  # Агент 2
    ], dtype=np.uint32).flatten()
    
    cumsum = compute_mp5_cumsum(mp5, frames, days)
    print(f"Cumsum shape: {cumsum.shape}")
    
    # Проверка: sum(agent0, days 2-5) = cumsum[0,5] - cumsum[0,2] = 50 - 20 = 30
    # cumsum[0, 2] = 20, cumsum[0, 5] = 50
    agent0_sum_2_5 = cumsum[5] - cumsum[2]  # base=0, days+1=11
    print(f"Agent 0, sum(days 2-5): {agent0_sum_2_5} (expected 30)")

