"""
AgentPopulation - модуль для инициализации агентов

Ответственность:
- Создание популяций агентов из MP3 данных
- Распределение по States (inactive/operations/serviceable/repair/reserve/storage)
- Инициализация переменных агентов (idx, aircraft_number, status_id, sne, ppr, ll, oh, br, etc)
- Вычисление нормативов LL/OH/BR по кадрам из MP1
- Инициализация limiter для агентов в operations (точный расчёт через mp5_cumsum)

Архитектурный принцип:
- Изолированный модуль, не зависит от orchestrator
- Работает только с данными env_data (MP1/MP3)
- Не выполняет RTC операций (только Python)
"""

import pyflamegpu as fg
import numpy as np
from typing import Dict, List, Tuple, Union, Optional
from .data_adapters import EnvDataAdapter

SECOND_LL_SENTINEL = 0xFFFFFFFF


def compute_limiter_for_agent(
    sne: int, ppr: int, ll: int, oh: int,
    mp5_cumsum: np.ndarray, idx: int, frames: int, end_day: int
) -> int:
    """
    Точный расчёт limiter через бинарный поиск по mp5_cumsum.
    
    Args:
        sne, ppr: Текущая наработка (минуты)
        ll, oh: Лимиты ресурса (минуты)
        mp5_cumsum: Кумулятивные суммы dt (day-major)
        idx: Индекс агента
        frames: Общее количество агентов
        end_day: Последний день симуляции
        
    Returns:
        limiter в днях (max 65535)
    """
    remaining_ll = max(0, ll - sne)
    remaining_oh = max(0, oh - ppr)
    
    # Ресурс исчерпан
    if remaining_ll == 0 or remaining_oh == 0:
        return 1  # Минимум 1 день для корректной работы
    
    # Day-major индексация: cumsum[day * frames + idx]
    current_day = 0
    base_idx = current_day * frames + idx
    base_cumsum = mp5_cumsum[base_idx] if base_idx < len(mp5_cumsum) else 0
    
    def binary_search_day(remaining: int) -> int:
        lo, hi = 1, end_day
        while lo < hi:
            mid = (lo + hi) // 2
            cumsum_mid_idx = mid * frames + idx
            if cumsum_mid_idx >= len(mp5_cumsum):
                hi = mid
                continue
            accumulated = int(mp5_cumsum[cumsum_mid_idx]) - int(base_cumsum)
            if accumulated >= remaining:
                hi = mid
            else:
                lo = mid + 1
        
        if lo <= end_day:
            final_idx = lo * frames + idx
            if final_idx < len(mp5_cumsum):
                final_accumulated = int(mp5_cumsum[final_idx]) - int(base_cumsum)
                if final_accumulated >= remaining:
                    return lo
        return end_day
    
    days_to_oh = binary_search_day(remaining_oh)
    days_to_ll = binary_search_day(remaining_ll)
    
    limiter = min(days_to_oh, days_to_ll)
    
    # Ограничиваем UInt16 и минимум 1
    return max(1, min(65535, limiter))


class AgentPopulationBuilder:
    """Строитель популяций агентов"""
    
    def __init__(self, env_data: Union[Dict[str, object], EnvDataAdapter],
                 mp5_cumsum: Optional[np.ndarray] = None, end_day: int = 3650):
        """
        Инициализация
        
        Args:
            env_data: словарь с данными окружения или EnvDataAdapter
            mp5_cumsum: Кумулятивные суммы dt для точного расчёта limiter (day-major)
            end_day: Последний день симуляции
        """
        # Поддержка обратной совместимости: принимаем как raw dict, так и адаптер
        if isinstance(env_data, EnvDataAdapter):
            self.adapter = env_data
            self.env_data = env_data._raw_data  # Для обратной совместимости
        else:
            self.adapter = EnvDataAdapter(env_data)
            self.env_data = env_data
        
        self.frames = self.adapter.dimensions.frames_total
        self.mp5_cumsum = mp5_cumsum
        self.end_day = end_day
        
    def populate_agents(self, simulation: fg.CUDASimulation, agent_def: fg.AgentDescription):
        """
        Загружает агентов из MP3 данных с поддержкой States
        
        Args:
            simulation: объект симуляции FLAME GPU
            agent_def: определение агента из BaseModel
        """
        print("Инициализация популяций агентов...")
        
        # Извлекаем массивы MP3
        mp3 = self.env_data.get('mp3_arrays', {})
        ac_list = mp3.get('mp3_aircraft_number', [])
        status_list = mp3.get('mp3_status_id', [])
        sne_list = mp3.get('mp3_sne', [])
        ppr_list = mp3.get('mp3_ppr', [])
        repair_days_list = mp3.get('mp3_repair_days', [])
        gb_list = mp3.get('mp3_group_by', [])
        pseq_list = mp3.get('mp3_partseqno_i', [])
        
        # Получаем MP1 данные для OH
        mp1_arrays = self.env_data.get('mp1_arrays', {})
        mp1_partseqno = mp1_arrays.get('partseqno_i', [])
        # OH берём из верхнего уровня env_data как в sim_master
        mp1_oh_mi8 = self.env_data.get('mp1_oh_mi8', [])
        mp1_oh_mi17 = self.env_data.get('mp1_oh_mi17', [])
        mp1_second_ll = self.env_data.get('mp1_second_ll', [])
        second_ll_sentinel = int(self.env_data.get('second_ll_sentinel', SECOND_LL_SENTINEL))
        
        # Индекс кадров
        frames_index = self.env_data.get('frames_index', {})
        
        # Предварительно вычисляем LL/OH/BR по кадрам
        ll_by_frame, oh_by_frame, br_by_frame = self._build_norms_by_frame()
        
        # Создаем популяции для каждого состояния (V6 архитектура)
        populations = {
            'inactive': fg.AgentVector(agent_def),      # state_1
            'operations': fg.AgentVector(agent_def),    # state_2
            'serviceable': fg.AgentVector(agent_def),   # state_3
            'repair': fg.AgentVector(agent_def),        # state_4 (V6: детерминированный выход)
            'reserve': fg.AgentVector(agent_def),       # state_5 (spawn tickets)
            'storage': fg.AgentVector(agent_def),       # state_6
            'unserviceable': fg.AgentVector(agent_def)  # state_7 (V6: после OH)
        }
        
        # Маппинг status_id -> state name (V6 архитектура)
        status_to_state = {
            0: 'inactive',      # Статус 0 → неактивные
            1: 'inactive',      # Статус 1 → неактивные  
            2: 'operations',    # Статус 2 → в эксплуатации
            3: 'serviceable',   # Статус 3 → исправные в холдинге
            4: 'repair',        # Статус 4 → V6: детерминированный ремонт
            5: 'reserve',       # Статус 5 → spawn tickets
            6: 'storage',       # Статус 6 → списанные
            7: 'unserviceable'  # Статус 7 → V6: после OH (новый)
        }
        
        # Сначала фильтруем записи с group_by in [1,2]
        plane_records = []
        for j in range(len(ac_list)):
            gb = int(gb_list[j] or 0) if j < len(gb_list) else 0
            if gb in [1, 2]:
                ac = int(ac_list[j] or 0)
                if ac > 0 and ac in frames_index:
                    mfg_list = mp3.get('mp3_mfg_date_days', [])
                    mfg_val = int(mfg_list[j] or 0) if j < len(mfg_list) else 0
                    plane_records.append({
                        'idx': j,
                        'aircraft_number': ac,
                        'frame_idx': frames_index[ac],
                        'status_id': int(status_list[j] or 1) if j < len(status_list) else 1,
                        'sne': int(sne_list[j] or 0) if j < len(sne_list) else 0,
                        # PPR из heli_pandas как есть (первый цикл корректируется в heli_pandas позже)
                        'ppr': int(ppr_list[j] or 0) if j < len(ppr_list) else 0,
                        'repair_days': int(repair_days_list[j] or 0) if j < len(repair_days_list) else 0,
                        'group_by': gb,
                        'partseqno_i': int(pseq_list[j] or 0) if j < len(pseq_list) else 0,
                        'mfg_date': mfg_val
                    })
        
        # Группируем по frame_idx и берем первую запись для каждого
        records_by_frame = {}
        for rec in plane_records:
            frame_idx = rec['frame_idx']
            if frame_idx not in records_by_frame:
                records_by_frame[frame_idx] = rec
        
        # ════════════════════════════════════════════════════════════════════
        # Сортировка по frame_idx УЖЕ СДЕЛАНА в build_frames_index (по mfg_date)
        # Просто берём записи в порядке frame_idx
        # ════════════════════════════════════════════════════════════════════
        sorted_records = sorted(records_by_frame.items(), key=lambda x: x[0])  # по frame_idx
        
        # Подсчитываем Mi-8 и Mi-17 для статистики
        n_mi8 = sum(1 for _, rec in sorted_records if rec['group_by'] == 1)
        n_mi17 = sum(1 for _, rec in sorted_records if rec['group_by'] == 2)
        
        self.env_data['n_mi8'] = n_mi8
        self.env_data['n_mi17'] = n_mi17
        
        print(f"  Агенты по типам: Mi-8={n_mi8}, Mi-17={n_mi17}")
        
        # Получаем информацию о зарезервированных слотах
        first_reserved_idx = self.env_data.get('first_reserved_idx', self.frames)
        
        # Заполняем агентов и распределяем по состояниям
        # Используем frame_idx как idx агента (сортировка уже сделана в ETL)
        for frame_idx, agent_data in sorted_records:
            # Пропускаем зарезервированные слоты для будущего спавна
            if frame_idx >= first_reserved_idx:
                continue
                
            # Определяем состояние и добавляем агента
            status_id = agent_data['status_id']
            state_name = status_to_state.get(status_id, 'inactive')
            pop = populations[state_name]
            pop.push_back()
            agent = pop[len(pop) - 1]
            
            # Базовые переменные (используем frame_idx из build_frames_index)
            agent.setVariableUInt("idx", frame_idx)
            agent.setVariableUInt("aircraft_number", agent_data['aircraft_number'])
            gb = agent_data.get('group_by', 0)
            partseqno = agent_data.get('partseqno_i', 0)
            agent.setVariableUInt("group_by", gb)
            agent.setVariableUInt("partseqno_i", partseqno)
            agent.setVariableUInt("repair_days", agent_data['repair_days'])
            agent.setVariableUInt("status_change_day", 0)
            agent.setVariableUInt("repair_candidate", 0)
            agent.setVariableUInt("repair_line_id", 0xFFFFFFFF)
            agent.setVariableUInt("repair_line_day", 0xFFFFFFFF)
            
            # OH вычисляем СНАЧАЛА (нужен для правила первого цикла ppr)
            oh_value = oh_by_frame[frame_idx]  # Значение по умолчанию
            mp1_index = self.env_data.get('mp1_index', {})
            pidx = mp1_index.get(partseqno, -1)
            
            if pidx >= 0:
                if gb == 1:  # Mi-8
                    if pidx < len(mp1_oh_mi8):
                        oh_value = int(mp1_oh_mi8[pidx] or 0)
                elif gb == 2:  # Mi-17
                    if pidx < len(mp1_oh_mi17):
                        oh_value = int(mp1_oh_mi17[pidx] or 0)
            
            # SNE и PPR — берём из heli_pandas как есть
            # PPR ≠ SNE означает что был ремонт (не первый цикл)
            sne_value = agent_data['sne']
            ppr_value = agent_data['ppr']
            
            agent.setVariableUInt("sne", sne_value)
            agent.setVariableUInt("ppr", ppr_value)
            
            # Нормативы
            # LL берём из MP3 (heli_pandas)
            ll_list = mp3.get('mp3_ll', [])
            # Используем индекс из agent_data, который указывает на позицию в MP3
            mp3_idx = agent_data.get('idx', -1)
            if mp3_idx >= 0 and mp3_idx < len(ll_list):
                ll_value = int(ll_list[mp3_idx] or 0)
                if ll_value == 0:  # Если 0, используем значение по умолчанию
                    ll_value = ll_by_frame[frame_idx]
            else:
                ll_value = ll_by_frame[frame_idx]  # значение по умолчанию
            
            agent.setVariableUInt("ll", ll_value)
            if 0 <= pidx < len(mp1_second_ll):
                second_ll_value = int(mp1_second_ll[pidx])
            else:
                second_ll_value = second_ll_sentinel
            agent.setVariableUInt("second_ll", second_ll_value)
            agent.setVariableUInt("oh", oh_value)
            agent.setVariableUInt("br", br_by_frame[frame_idx])

            # mfg_date для приоритизации квот (ord days от 1970-01-01)
            mfg_val = agent_data.get('mfg_date', 0)
            agent.setVariableUInt("mfg_date", mfg_val)
            
            # Времена ремонта из констант БЕЗ FALLBACK
            # FIX 3: Чтение из env_data, НЕ simulation (триггерит NVRTC!)
            if gb == 1:
                if 'mi8_repair_time_const' not in self.env_data:
                    raise KeyError(f"❌ 'mi8_repair_time_const' отсутствует в env_data для агента idx={new_idx}, group_by=1")
                if 'mi8_assembly_time_const' not in self.env_data:
                    raise KeyError(f"❌ 'mi8_assembly_time_const' отсутствует в env_data для агента idx={new_idx}, group_by=1")
                if 'mi8_partout_time_const' not in self.env_data:
                    raise KeyError(f"❌ 'mi8_partout_time_const' отсутствует в env_data для агента idx={new_idx}, group_by=1")
                
                agent.setVariableUInt("repair_time", int(self.env_data['mi8_repair_time_const']))
                agent.setVariableUInt("assembly_time", int(self.env_data['mi8_assembly_time_const']))
                agent.setVariableUInt("partout_time", int(self.env_data['mi8_partout_time_const']))
            elif gb == 2:
                if 'mi17_repair_time_const' not in self.env_data:
                    raise KeyError(f"❌ 'mi17_repair_time_const' отсутствует в env_data для агента idx={new_idx}, group_by=2")
                if 'mi17_assembly_time_const' not in self.env_data:
                    raise KeyError(f"❌ 'mi17_assembly_time_const' отсутствует в env_data для агента idx={new_idx}, group_by=2")
                if 'mi17_partout_time_const' not in self.env_data:
                    raise KeyError(f"❌ 'mi17_partout_time_const' отсутствует в env_data для агента idx={new_idx}, group_by=2")
                
                agent.setVariableUInt("repair_time", int(self.env_data['mi17_repair_time_const']))
                agent.setVariableUInt("assembly_time", int(self.env_data['mi17_assembly_time_const']))
                agent.setVariableUInt("partout_time", int(self.env_data['mi17_partout_time_const']))
            
            # Для агентов в статусе 6 устанавливаем s6_started
            if status_id == 6:
                agent.setVariableUInt("s6_started", 0)  # Изначально в статусе 6
            
            # V6: Для агентов в repair (status_id=4) устанавливаем exit_date
            if status_id == 4:
                repair_time = agent.getVariableUInt("repair_time")
                repair_days = agent.getVariableUInt("repair_days")
                assembly_time = agent.getVariableUInt("assembly_time")
                
                # Если осталось до конца ремонта МЕНЬШЕ assembly_time - агент в фазе сборки
                if repair_time - repair_days < assembly_time:
                    agent.setVariableUInt("assembly_trigger", 1)
                
                # V6: exit_date = день когда repair завершится (переход repair→serviceable)
                remaining_repair = max(0, repair_time - repair_days)
                agent.setVariableUInt("exit_date", remaining_repair)
            
            # intent_state зависит от status_id (state):
            # - inactive (1) → 1 (замороженные, ждут repair_time)
            # - operations (2) → 2 (в эксплуатации)
            # - serviceable (3) → 3 (холдинг)
            # - repair (4) → 4 (в ремонте)
            # - reserve (5) → 5 (в резерве)
            # - storage (6) → 6 (утилизирован)
            if status_id == 1:
                agent.setVariableUInt("intent_state", 1)  # ✅ inactive = замороженные
            elif status_id == 4:
                agent.setVariableUInt("intent_state", 4)
            elif status_id == 6:
                agent.setVariableUInt("intent_state", 6)
            else:  # 2, 3, 5
                agent.setVariableUInt("intent_state", status_id)  # соответствует state
            
            # ═══════════════════════════════════════════════════════════════════
            # LIMITER: Точный расчёт для агентов в operations (status_id=2)
            # Вычисляется ОДИН РАЗ при загрузке через бинарный поиск по mp5_cumsum
            # ═══════════════════════════════════════════════════════════════════
            if status_id == 2 and self.mp5_cumsum is not None:
                limiter = compute_limiter_for_agent(
                    sne=sne_value,
                    ppr=ppr_value,
                    ll=ll_value,
                    oh=oh_value,
                    mp5_cumsum=self.mp5_cumsum,
                    idx=frame_idx,
                    frames=self.frames,
                    end_day=self.end_day
                )
                agent.setVariableUInt16("limiter", limiter)
        
        # Загружаем популяции в симуляцию по состояниям (V6: 7 states)
        # ВАЖНО: Нужно инициализировать ВСЕ states, даже пустые
        # V7: reserve пропускаем — он заполняется отдельно spawn агентами в оркестраторе
        all_states = ['inactive', 'operations', 'serviceable', 'repair', 'storage', 'unserviceable']  # БЕЗ reserve!
        
        # FIX 4: Используем agent_def, НЕ simulation.getAgentDescription (нет такого метода!)
        for state_name in all_states:
            pop = populations.get(state_name, fg.AgentVector(agent_def))
            simulation.setPopulationData(pop, state_name)
            if len(pop) > 0:
                print(f"  Загружено {len(pop)} агентов в состояние '{state_name}'")
    
    def _build_norms_by_frame(self) -> Tuple[List[int], List[int], List[int]]:
        """
        Вычисляет нормативы LL/OH/BR по кадрам
        
        Returns:
            (ll_by_frame, oh_by_frame, br_by_frame) — списки размера frames
        
        Raises:
            RuntimeError: если отсутствуют критичные данные MP1
        """
        ll_by_frame = [0] * self.frames
        oh_by_frame = [0] * self.frames
        br_by_frame = [0] * self.frames
        
        # Получаем данные из env_data
        mp3_arrays = self.env_data.get('mp3_arrays', {})
        mp1_arrays = self.env_data.get('mp1_arrays', {})
        
        # MP3 данные
        ac_list = mp3_arrays.get('mp3_aircraft_number', [])
        gb_list = mp3_arrays.get('mp3_group_by', [])
        pseq_list = mp3_arrays.get('mp3_partseqno_i', [])
        
        # MP1 данные (нормативы из md_components)
        mp1_partseqno = mp1_arrays.get('partseqno_i', [])
        mp1_ll_mi8 = mp1_arrays.get('ll_mi8', [])
        mp1_ll_mi17 = mp1_arrays.get('ll_mi17', [])
        mp1_second_ll = mp1_arrays.get('second_ll', [])
        # OH берём из верхнего уровня env_data как в sim_master
        mp1_oh_mi8 = self.env_data.get('mp1_oh_mi8', [])
        mp1_oh_mi17 = self.env_data.get('mp1_oh_mi17', [])
        mp1_br_mi8 = mp1_arrays.get('br_mi8', [])
        mp1_br_mi17 = mp1_arrays.get('br_mi17', [])
        
        # Создаем карту partseqno -> нормативы
        norms_map = {}
        for i, partseq in enumerate(mp1_partseqno):
            if i < len(mp1_ll_mi8):
                norms_map[partseq] = {
                    'll_mi8': mp1_ll_mi8[i] if i < len(mp1_ll_mi8) else 0,
                    'll_mi17': mp1_ll_mi17[i] if i < len(mp1_ll_mi17) else 0,
                    'oh_mi8': mp1_oh_mi8[i] if i < len(mp1_oh_mi8) else 0,
                    'oh_mi17': mp1_oh_mi17[i] if i < len(mp1_oh_mi17) else 0,
                    'br_mi8': mp1_br_mi8[i] if i < len(mp1_br_mi8) else 0,
                    'br_mi17': mp1_br_mi17[i] if i < len(mp1_br_mi17) else 0,
                    'second_ll': mp1_second_ll[i] if i < len(mp1_second_ll) else SECOND_LL_SENTINEL,
                }
        
        # Индекс кадров
        frames_index = self.env_data.get('frames_index', {})
        # Количество РЕАЛЬНЫХ агентов (без резерва для spawn)
        frames_total_base = int(self.env_data.get('frames_total_base', self.frames))
        
        # Заполняем нормативы только для реальных кадров (без зарезервированных)
        for i in range(frames_total_base):
            # Находим aircraft_number для этого frame
            ac = 0
            gb = 0
            partseq = 0
            
            # Ищем в frames_index
            for aircraft_number, frame_idx in frames_index.items():
                if frame_idx == i:
                    ac = aircraft_number
                    # Находим group_by и partseqno для этого AC в MP3
                    for j, mp3_ac in enumerate(ac_list):
                        if int(mp3_ac or 0) == ac and j < len(gb_list):
                            gb_val = int(gb_list[j] or 0)
                            if gb_val in [1, 2]:  # Только планеры
                                gb = gb_val
                                partseq = int(pseq_list[j] or 0) if j < len(pseq_list) else 0
                                break
                    break
            
            # Проверяем принадлежность к планёрам и наличие ключевых полей
            if gb not in (1, 2) or partseq == 0:
                raise RuntimeError(
                    f"Отсутствуют MP1-данные для кадра i={i} (ac={ac}, group_by={gb}, partseqno_i={partseq}). "
                    "Пайплайн остановлен: запрещены дефолтные нормативы."
                )

            # Получаем нормативы из карты (строго без дефолтов)
            if partseq not in norms_map:
                raise RuntimeError(
                    f"Не найден partseqno_i={partseq} в MP1 для кадра i={i} (ac={ac}, group_by={gb}). "
                    "Проверьте md_components.* источники."
                )

            norms = norms_map[partseq]
            if gb == 1:  # Mi-8
                ll_by_frame[i] = int(norms.get('ll_mi8', 0) or 0)
                oh_by_frame[i] = int(norms.get('oh_mi8', 0) or 0)
                br_by_frame[i] = int(norms.get('br_mi8', 0) or 0)
                if oh_by_frame[i] == 0 or br_by_frame[i] == 0:
                    raise RuntimeError(
                        f"Нормативы OH/BR отсутствуют (0) для Mi-8: i={i}, ac={ac}, partseq={partseq}. "
                        "Запрещены дефолтные значения."
                    )
            else:  # gb == 2, Mi-17
                ll_by_frame[i] = int(norms.get('ll_mi17', 0) or 0)
                oh_by_frame[i] = int(norms.get('oh_mi17', 0) or 0)
                br_by_frame[i] = int(norms.get('br_mi17', 0) or 0)
                if oh_by_frame[i] == 0 or br_by_frame[i] == 0:
                    raise RuntimeError(
                        f"Нормативы OH/BR отсутствуют (0) для Mi-17: i={i}, ac={ac}, partseq={partseq}. "
                        "Запрещены дефолтные значения."
                    )
        
        return ll_by_frame, oh_by_frame, br_by_frame
    
    def get_initial_ops_count(self) -> Dict[int, int]:
        """
        Возвращает начальное количество агентов в состоянии operations по типам.
        
        Returns:
            Dict[int, int]: {group_by: count}, например {1: 67, 2: 93}
        """
        mp3 = self.env_data.get('mp3_arrays', {})
        ac_list = mp3.get('mp3_aircraft_number', [])
        status_list = mp3.get('mp3_status_id', [])
        gb_list = mp3.get('mp3_group_by', [])
        frames_index = self.env_data.get('frames_index', {})
        first_reserved_idx = self.env_data.get('first_reserved_idx', self.frames)
        
        # Считаем агентов в operations (status_id=2) по group_by
        ops_count = {1: 0, 2: 0}  # Mi-8, Mi-17
        
        seen_frames = set()
        for j in range(len(ac_list)):
            gb = int(gb_list[j] or 0) if j < len(gb_list) else 0
            if gb not in [1, 2]:
                continue
            
            ac = int(ac_list[j] or 0)
            if ac <= 0 or ac not in frames_index:
                continue
            
            frame_idx = frames_index[ac]
            if frame_idx >= first_reserved_idx:
                continue
            
            if frame_idx in seen_frames:
                continue
            seen_frames.add(frame_idx)
            
            status_id = int(status_list[j] or 1) if j < len(status_list) else 1
            if status_id == 2:  # operations
                ops_count[gb] += 1
        
        return ops_count

