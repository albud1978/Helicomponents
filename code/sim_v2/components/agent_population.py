"""
AgentPopulation - модуль для инициализации агентов

Ответственность:
- Создание популяций агентов из MP3 данных
- Распределение по States (inactive/operations/serviceable/repair/reserve/storage)
- Инициализация переменных агентов (idx, aircraft_number, status_id, sne, ppr, ll, oh, br, etc)
- Вычисление нормативов LL/OH/BR по кадрам из MP1

Архитектурный принцип:
- Изолированный модуль, не зависит от orchestrator
- Работает только с данными env_data (MP1/MP3)
- Не выполняет RTC операций (только Python)
"""

import pyflamegpu as fg
from typing import Dict, List, Tuple, Union
from .data_adapters import EnvDataAdapter


class AgentPopulationBuilder:
    """Строитель популяций агентов"""
    
    def __init__(self, env_data: Union[Dict[str, object], EnvDataAdapter]):
        """
        Инициализация
        
        Args:
            env_data: словарь с данными окружения или EnvDataAdapter
        """
        # Поддержка обратной совместимости: принимаем как raw dict, так и адаптер
        if isinstance(env_data, EnvDataAdapter):
            self.adapter = env_data
            self.env_data = env_data._raw_data  # Для обратной совместимости
        else:
            self.adapter = EnvDataAdapter(env_data)
            self.env_data = env_data
        
        self.frames = self.adapter.dimensions.frames_total
        
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
        
        # Индекс кадров
        frames_index = self.env_data.get('frames_index', {})
        
        # Предварительно вычисляем LL/OH/BR по кадрам
        ll_by_frame, oh_by_frame, br_by_frame = self._build_norms_by_frame()
        
        # Создаем популяции для каждого состояния
        populations = {
            'inactive': fg.AgentVector(agent_def),      # state_1
            'operations': fg.AgentVector(agent_def),    # state_2
            'serviceable': fg.AgentVector(agent_def),   # state_3
            'repair': fg.AgentVector(agent_def),        # state_4
            'reserve': fg.AgentVector(agent_def),       # state_5
            'storage': fg.AgentVector(agent_def)        # state_6
        }
        
        # Маппинг status_id -> state name
        status_to_state = {
            0: 'inactive',      # Статус 0 тоже считаем неактивным
            1: 'inactive',
            2: 'operations',
            3: 'serviceable',
            4: 'repair',
            5: 'reserve',
            6: 'storage'
        }
        
        # Сначала фильтруем записи с group_by in [1,2]
        plane_records = []
        for j in range(len(ac_list)):
            gb = int(gb_list[j] or 0) if j < len(gb_list) else 0
            if gb in [1, 2]:
                ac = int(ac_list[j] or 0)
                if ac > 0 and ac in frames_index:
                    plane_records.append({
                        'idx': j,
                        'aircraft_number': ac,
                        'frame_idx': frames_index[ac],
                        'status_id': int(status_list[j] or 1) if j < len(status_list) else 1,
                        'sne': int(sne_list[j] or 0) if j < len(sne_list) else 0,
                        'ppr': int(ppr_list[j] or 0) if j < len(ppr_list) else 0,
                        'repair_days': int(repair_days_list[j] or 0) if j < len(repair_days_list) else 0,
                        'group_by': gb,
                        'partseqno_i': int(pseq_list[j] or 0) if j < len(pseq_list) else 0
                    })
        
        # Группируем по frame_idx и берем первую запись для каждого
        records_by_frame = {}
        for rec in plane_records:
            frame_idx = rec['frame_idx']
            if frame_idx not in records_by_frame:
                records_by_frame[frame_idx] = rec
        
        # Получаем информацию о зарезервированных слотах
        first_reserved_idx = self.env_data.get('first_reserved_idx', self.frames)
        
        # Заполняем агентов и распределяем по состояниям
        # Создаем только реальных агентов, пропускаем зарезервированные слоты
        for i in range(self.frames):
            # Пропускаем зарезервированные слоты для будущего спавна
            if i >= first_reserved_idx:
                continue
                
            # Пропускаем индексы без реальных агентов
            if i not in records_by_frame:
                continue
                
            # Берем данные для этого frame_idx
            agent_data = records_by_frame[i]
            
            # Определяем состояние и добавляем агента
            status_id = agent_data['status_id']
            state_name = status_to_state.get(status_id, 'inactive')
            pop = populations[state_name]
            pop.push_back()
            agent = pop[len(pop) - 1]
            
            # Базовые переменные
            agent.setVariableUInt("idx", i)
            agent.setVariableUInt("aircraft_number", agent_data['aircraft_number'])
            # status_id НЕ используется - переведено на States
            agent.setVariableUInt("sne", agent_data['sne'])
            agent.setVariableUInt("ppr", agent_data['ppr'])
            agent.setVariableUInt("repair_days", agent_data['repair_days'])
            gb = agent_data.get('group_by', 0)
            partseqno = agent_data.get('partseqno_i', 0)
            agent.setVariableUInt("group_by", gb)
            agent.setVariableUInt("partseqno_i", partseqno)
            
            # OH берём из MP1 по типу вертолёта
            oh_value = oh_by_frame[i]  # Значение по умолчанию
            
            # Используем mp1_index как в sim_master
            mp1_index = self.env_data.get('mp1_index', {})
            pidx = mp1_index.get(partseqno, -1)
            
            if pidx >= 0:
                # В sim_master используется ac_type_mask, но у нас есть group_by
                # group_by = 1 это Mi-8 (mask & 32)
                # group_by = 2 это Mi-17 (mask & 64)
                if gb == 1:  # Mi-8
                    if pidx < len(mp1_oh_mi8):
                        oh_value = int(mp1_oh_mi8[pidx] or 0)
                elif gb == 2:  # Mi-17
                    if pidx < len(mp1_oh_mi17):
                        oh_value = int(mp1_oh_mi17[pidx] or 0)
            
            # Нормативы
            # LL берём из MP3 (heli_pandas)
            ll_list = mp3.get('mp3_ll', [])
            # Используем индекс из agent_data, который указывает на позицию в MP3
            mp3_idx = agent_data.get('idx', -1)
            if mp3_idx >= 0 and mp3_idx < len(ll_list):
                ll_value = int(ll_list[mp3_idx] or 0)
                if ll_value == 0:  # Если 0, используем значение по умолчанию
                    ll_value = ll_by_frame[i]
            else:
                ll_value = ll_by_frame[i]  # значение по умолчанию
            
            agent.setVariableUInt("ll", ll_value)
            agent.setVariableUInt("oh", oh_value)
            agent.setVariableUInt("br", br_by_frame[i])

            # mfg_date для приоритизации квот (ord days от 1970-01-01)
            mfg_list = mp3.get('mp3_mfg_date_days', [])
            if mp3_idx >= 0 and mp3_idx < len(mfg_list):
                mfg_val = int(mfg_list[mp3_idx] or 0)
            else:
                mfg_val = 0
            agent.setVariableUInt("mfg_date", mfg_val)
            
            # Времена ремонта из env_data (НЕ simulation - триггерит NVRTC!)
            if gb == 1:
                agent.setVariableUInt("repair_time", int(self.env_data.get('mi8_repair_time_const', 180)))
                agent.setVariableUInt("assembly_time", int(self.env_data.get('mi8_assembly_time_const', 180)))
                agent.setVariableUInt("partout_time", 180)  # Дефолт для Mi-8
            elif gb == 2:
                agent.setVariableUInt("repair_time", int(self.env_data.get('mi17_repair_time_const', 180)))
                agent.setVariableUInt("assembly_time", int(self.env_data.get('mi17_assembly_time_const', 180)))
                agent.setVariableUInt("partout_time", int(self.env_data.get('mi17_partout_time_const', 180)))
            
            # Для агентов в статусе 6 устанавливаем s6_started
            if status_id == 6:
                agent.setVariableUInt("s6_started", 0)  # Изначально в статусе 6
            
            # Для агентов в статусе 4 проверяем assembly_trigger
            if status_id == 4:
                repair_time = agent.getVariableUInt("repair_time")
                repair_days = agent.getVariableUInt("repair_days")
                assembly_time = agent.getVariableUInt("assembly_time")
                
                if repair_time - repair_days > assembly_time:
                    agent.setVariableUInt("assembly_trigger", 1)
            
            # intent_state = 0 для всех (нет начальных намерений)
            agent.setVariableUInt("intent_state", 0)
        
        # Загружаем популяции в симуляцию по состояниям
        # ВАЖНО: Нужно инициализировать ВСЕ states, даже пустые (для spawn)
        all_states = ['inactive', 'operations', 'serviceable', 'repair', 'reserve', 'storage']
        
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
                }
        
        # Индекс кадров
        frames_index = self.env_data.get('frames_index', {})
        # Начало зарезервированной области под будущий спавн — эти кадры пропускаем
        first_reserved_idx = int(self.env_data.get('first_reserved_idx', self.frames))
        
        # Заполняем нормативы только для реальных кадров (без зарезервированных)
        for i in range(first_reserved_idx):
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

