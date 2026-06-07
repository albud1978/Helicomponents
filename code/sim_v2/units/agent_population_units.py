#!/usr/bin/env python3
"""
AgentPopulationUnits - модуль для инициализации агрегатов

Ответственность:
- Создание популяций агрегатов из heli_pandas
- Распределение по States (operations/serviceable/repair/reserve/storage)
- Инициализация FIFO-очереди (queue_position сортировка по mfg_date)
- Вычисление нормативов LL/OH/BR из md_components

Архитектурные особенности:
- Нет state "inactive" для агрегатов
- queue_position определяет порядок выдачи со склада (FIFO)
- psn — PRIMARY KEY агрегата
- aircraft_number — связь с планером (0 = в пуле)

Дата: 05.01.2026
"""

import pyflamegpu as fg
from typing import Dict, List, Tuple, Union
from datetime import date

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

from utils.config_loader import get_clickhouse_client


class AgentPopulationUnitsBuilder:
    """Строитель популяций агрегатов"""
    
    def __init__(self, version_date: date, version_id: int = 1):
        """
        Инициализация
        
        Args:
            version_date: Дата версии данных
            version_id: ID версии данных
        """
        self.version_date = version_date
        self.version_id = version_id
        self.client = get_clickhouse_client()
        
        # Данные будут загружены в load_data()
        self.units_data: List[Dict] = []
        self.mp1_norms: Dict[int, Dict] = {}  # partseqno_i -> {ll_mi8, oh_mi8, br_mi8, ...}
        
    def load_data(self) -> Dict[str, object]:
        """
        Загружает данные агрегатов из ClickHouse
        
        Returns:
            env_data для симуляции
        """
        print("📊 Загрузка данных агрегатов из ClickHouse...")
        
        # 1. Загрузка агрегатов (group_by >= 3)
        self._load_units()
        
        # 2. Загрузка нормативов из md_components
        self._load_mp1_norms()
        
        # 3. Формирование env_data
        env_data = self._build_env_data()
        
        print(f"✅ Загружено {len(self.units_data)} агрегатов")
        return env_data
    
    def _load_units(self):
        """Загружает агрегаты из heli_pandas"""
        sql = f"""
        SELECT 
            psn,
            aircraft_number,
            partseqno_i,
            group_by,
            status_id,
            sne,
            ppr,
            ll,
            repair_days,
            mfg_date,
            ac_type_mask
        FROM heli_pandas
        WHERE version_date = toDate('{self.version_date}')
          AND version_id = {self.version_id}
          AND group_by >= 3
        ORDER BY group_by, mfg_date
        """
        
        rows = self.client.execute(sql)
        
        self.units_data = []
        for row in rows:
            self.units_data.append({
                'psn': int(row[0] or 0),
                'aircraft_number': int(row[1] or 0),
                'partseqno_i': int(row[2] or 0),
                'group_by': int(row[3] or 0),
                'status_id': int(row[4] or 0),
                'sne': int(row[5] or 0),
                'ppr': int(row[6] or 0),
                'll': int(row[7] or 0),
                'repair_days': int(row[8] or 0),
                'mfg_date': row[9] if row[9] else None,
                'ac_type_mask': int(row[10] or 0)
            })
        
        print(f"   Загружено {len(self.units_data)} агрегатов (group_by >= 3)")
    
    def _load_mp1_norms(self):
        """Загружает нормативы из md_components"""
        sql = """
        SELECT 
            partseqno_i,
            ll_mi8, ll_mi17,
            oh_mi8, oh_mi17,
            br_mi8, br_mi17,
            repair_time
        FROM md_components
        WHERE partseqno_i IS NOT NULL AND partseqno_i > 0
        """
        
        rows = self.client.execute(sql)
        
        self.mp1_norms = {}
        for row in rows:
            partseqno = int(row[0] or 0)
            if partseqno > 0:
                self.mp1_norms[partseqno] = {
                    'll_mi8': int(row[1] or 0),
                    'll_mi17': int(row[2] or 0),
                    'oh_mi8': int(row[3] or 0),
                    'oh_mi17': int(row[4] or 0),
                    'br_mi8': int(row[5] or 0),
                    'br_mi17': int(row[6] or 0),
                    'repair_time': int(row[7] or 30)
                }
        
        print(f"   Загружено {len(self.mp1_norms)} записей нормативов")
    
    def _build_env_data(self) -> Dict[str, object]:
        """Формирует env_data для симуляции"""
        # Подсчёт по группам
        group_counts = {}
        for unit in self.units_data:
            gb = unit['group_by']
            group_counts[gb] = group_counts.get(gb, 0) + 1
        
        # Получаем версию
        version_date_int = (self.version_date - date(1970, 1, 1)).days
        
        # === Расчёт резервных слотов по формуле ===
        # Формула: reserve_slots = max(100, aggregates_needed - existing_count)
        # aggregates_needed = (total_flight_hours_10y / ll_aggregate) × 1.2
        reserve_slots = self._calculate_spawn_reserve(group_counts)
        total_frames = len(self.units_data) + reserve_slots
        
        print(f"   📊 Резервирование spawn: {len(self.units_data)} существующих + {reserve_slots} резервных = {total_frames} слотов")
        
        env_data = {
            'version_date_u16': version_date_int,
            'version_id_u32': self.version_id,
            'units_frames_total': total_frames,
            'days_total_u16': 3650,
            'units_data': self.units_data,
            'mp1_norms': self.mp1_norms,
            'group_counts': group_counts,
            'reserve_slots': reserve_slots,
        }
        
        return env_data
    
    def _calculate_spawn_reserve(self, group_counts: Dict[int, int]) -> int:
        """
        Расчёт количества резервных слотов для spawn агрегатов
        
        Формула из docs/architecture/rtc_components.md (оборот определяет восполнение):
        1. aggregates_consumed = flight_by_type / ll_aggregate
        2. aggregates_needed = aggregates_consumed × 1.2 (+20% запас)
        3. reserve_slots = max(10, aggregates_needed - existing_count)
        
        Важно: используем ac_type_mask для выбора правильного налёта:
        - 32 (0x20) → только Mi-8
        - 64 (0x40) → только Mi-17
        - 96 (0x60) → оба типа
        """
        # Константы
        DAYS_10_YEARS = 3650
        AVG_DAILY_FLIGHT_MIN = 90  # средний налёт в минутах/день
        SAFETY_MARGIN = 1.2  # +20% запас
        
        # Получаем количество планеров из базы
        planers_sql = """
        SELECT 
            SUM(CASE WHEN group_by = 1 THEN 1 ELSE 0 END) as mi8_count,
            SUM(CASE WHEN group_by = 2 THEN 1 ELSE 0 END) as mi17_count
        FROM heli_pandas
        WHERE version_date = toDate(%(version_date)s)
          AND version_id = %(version_id)s
          AND group_by IN (1, 2)
        """
        result = self.client.execute(planers_sql, {
            'version_date': self.version_date,
            'version_id': self.version_id
        })
        
        n_mi8 = int(result[0][0] or 163) if result else 163
        n_mi17 = int(result[0][1] or 116) if result else 116
        
        # Налёт за 10 лет по типам (минуты)
        flight_mi8_10y = n_mi8 * DAYS_10_YEARS * AVG_DAILY_FLIGHT_MIN
        flight_mi17_10y = n_mi17 * DAYS_10_YEARS * AVG_DAILY_FLIGHT_MIN
        
        # Расчёт резерва по группам
        total_reserve = 0
        top_consumers = []  # Для диагностики
        
        for group_by, existing_count in group_counts.items():
            # Получаем средний LL и ac_type_mask для группы
            ll_group, ac_mask = self._get_ll_and_mask_for_group(group_by)
            
            if ll_group > 0:
                # Выбираем правильный налёт по ac_type_mask
                if ac_mask == 32:  # Только Mi-8
                    flight_10y = flight_mi8_10y
                elif ac_mask == 64:  # Только Mi-17
                    flight_10y = flight_mi17_10y
                else:  # Универсальный (96) — средний
                    flight_10y = (flight_mi8_10y + flight_mi17_10y) / 2
                
                # Оборот: сколько "жизней агрегатов" понадобится за 10 лет
                aggregates_consumed = flight_10y / ll_group
                
                # +20% запас
                aggregates_needed = aggregates_consumed * SAFETY_MARGIN
                
                # Резерв = потребность - существующие (минимум 10)
                # Агрегаты разные — нужен полный резерв по формуле оборота
                group_reserve = max(10, int(aggregates_needed - existing_count))
                
                # Собираем топ для диагностики
                if group_reserve > 100:
                    top_consumers.append((group_by, existing_count, int(aggregates_needed), group_reserve, ll_group))
            else:
                # Если LL неизвестен — минимальный резерв 20% от существующих
                group_reserve = max(10, int(existing_count * 0.20))
            
            total_reserve += group_reserve
        
        # Диагностика топ-5 групп с большим резервом
        if top_consumers:
            top_consumers.sort(key=lambda x: x[3], reverse=True)
            print("   📈 Топ-5 групп по резерву:")
            for gb, exist, needed, reserve, ll in top_consumers[:5]:
                print(f"      group_by={gb}: exist={exist}, needed={needed}, reserve={reserve}, ll={ll//60}ч")
        
        # Минимум 500 резервных слотов
        return max(500, total_reserve)
    
    def _get_ll_and_mask_for_group(self, group_by: int) -> Tuple[int, int]:
        """Получает средний LL и преобладающий ac_type_mask для группы"""
        lls = []
        masks = []
        
        for unit in self.units_data:
            if unit['group_by'] == group_by:
                partseqno = unit.get('partseqno_i', 0)
                norms = self.mp1_norms.get(partseqno, {})
                ac_mask = unit.get('ac_type_mask', 96)
                
                # Берём LL по типу ВС
                if ac_mask == 32:  # Mi-8
                    ll = norms.get('ll_mi8', 0)
                elif ac_mask == 64:  # Mi-17
                    ll = norms.get('ll_mi17', 0)
                else:  # Универсальный — берём максимум
                    ll = max(norms.get('ll_mi8', 0), norms.get('ll_mi17', 0))
                
                if ll > 0:
                    lls.append(ll)
                    masks.append(ac_mask)
        
        avg_ll = sum(lls) // len(lls) if lls else 0
        # Берём наиболее частую маску
        avg_mask = max(set(masks), key=masks.count) if masks else 96
        
        return avg_ll, avg_mask
    
    
    def populate_agents(self, simulation: fg.CUDASimulation, agent_def: fg.AgentDescription,
                       env_data: Dict[str, object]):
        """
        Загружает агентов-агрегатов в симуляцию
        
        Args:
            simulation: объект симуляции FLAME GPU
            agent_def: определение агента из BaseModel
            env_data: данные окружения
        """
        print("Инициализация популяций агрегатов...")
        
        units_data = env_data.get('units_data', [])
        mp1_norms = env_data.get('mp1_norms', {})
        
        # Создаем популяции для каждого состояния
        populations = {
            'operations': fg.AgentVector(agent_def),    # state_2
            'serviceable': fg.AgentVector(agent_def),   # state_3
            'repair': fg.AgentVector(agent_def),        # state_4
            'reserve': fg.AgentVector(agent_def),       # state_5
            'storage': fg.AgentVector(agent_def)        # state_6
        }
        
        # Маппинг status_id -> state name
        status_to_state = {
            2: 'operations',
            3: 'serviceable',
            4: 'repair',
            5: 'reserve',
            6: 'storage'
        }
        
        # === FIFO-очередь: группировка по group_by ===
        # Сортируем по group_by и mfg_date
        units_by_group = {}
        for unit in units_data:
            gb = unit['group_by']
            if gb not in units_by_group:
                units_by_group[gb] = []
            units_by_group[gb].append(unit)
        
        # Сортируем внутри группы по mfg_date
        for gb in units_by_group:
            units_by_group[gb].sort(key=lambda u: u.get('mfg_date') or date(1970, 1, 1))
        
        # === Инициализация FIFO-очередей (трёхуровневая система) ===
        # Приоритет 1: Serviceable (svc) — готовые агрегаты на складе
        # Приоритет 2: Reserve (rsv) — после ремонта
        # Приоритет 3: Spawn — active=0, не в очереди
        
        svc_positions = {}  # group_by -> текущая позиция для serviceable
        rsv_positions = {}  # group_by -> текущая позиция для reserve
        
        # Счётчики для инициализации MacroProperty
        svc_tails = {}  # group_by -> svc_tail
        rsv_tails = {}  # group_by -> rsv_tail
        
        for gb in units_by_group:
            svc_positions[gb] = 0
            rsv_positions[gb] = 0
            svc_tails[gb] = 0
            rsv_tails[gb] = 0
        
        # === Заполнение агентов ===
        idx = 0
        for gb in sorted(units_by_group.keys()):
            for unit in units_by_group[gb]:
                # Определяем состояние
                status_id = unit['status_id']
                
                # Дефолт: если status_id не 2-6, ставим в repair (для status_id=4 с repair_days=0)
                if status_id not in status_to_state:
                    status_id = 4  # repair
                
                # FIX: Агрегаты без aircraft_number не могут быть в operations
                # Они должны быть в serviceable (готовы к установке)
                if status_id == 2 and unit['aircraft_number'] == 0:
                    status_id = 3  # serviceable
                
                state_name = status_to_state[status_id]
                pop = populations[state_name]
                pop.push_back()
                agent = pop[len(pop) - 1]
                
                # === Базовые переменные ===
                agent.setVariableUInt("idx", idx)
                agent.setVariableUInt("psn", unit['psn'])
                agent.setVariableUInt("active", 1)  # Реальный агрегат (не spawn-резерв)
                agent.setVariableUInt("aircraft_number", unit['aircraft_number'])
                agent.setVariableUInt("partseqno_i", unit['partseqno_i'])
                agent.setVariableUInt("group_by", unit['group_by'])
                
                # === Наработки ===
                agent.setVariableUInt("sne", unit['sne'])
                agent.setVariableUInt("ppr", unit['ppr'])
                agent.setVariableUInt("repair_days", unit['repair_days'])
                
                # === Нормативы из MP1 ===
                partseqno = unit['partseqno_i']
                norms = mp1_norms.get(partseqno, {})
                ac_mask = unit['ac_type_mask']
                
                # Выбираем нормативы по типу ВС
                if ac_mask & 64:  # Mi-17
                    ll_val = norms.get('ll_mi17', 0)
                    oh_val = norms.get('oh_mi17', 0)
                    br_val = norms.get('br_mi17', 0)
                elif ac_mask & 32:  # Mi-8
                    ll_val = norms.get('ll_mi8', 0)
                    oh_val = norms.get('oh_mi8', 0)
                    br_val = norms.get('br_mi8', 0)
                else:
                    # Универсальный агрегат — берём Mi-17
                    ll_val = norms.get('ll_mi17', norms.get('ll_mi8', 0))
                    oh_val = norms.get('oh_mi17', norms.get('oh_mi8', 0))
                    br_val = norms.get('br_mi17', norms.get('br_mi8', 0))
                
                # Если LL из heli_pandas > 0, используем его
                if unit['ll'] > 0:
                    ll_val = unit['ll']
                
                agent.setVariableUInt("ll", ll_val)
                agent.setVariableUInt("oh", oh_val)
                agent.setVariableUInt("br", br_val)
                agent.setVariableUInt("repair_time", norms.get('repair_time', 30))
                
                # === FIFO queue_position (трёхуровневая система) ===
                # Все реальные агрегаты (active=1) получают позицию в очереди
                # Spawn-резерв (active=0) создаётся отдельно ниже
                
                if state_name == 'serviceable':
                    # Приоритет 1: svc-очередь
                    agent.setVariableUInt("queue_position", svc_positions[gb])
                    svc_positions[gb] += 1
                    svc_tails[gb] = svc_positions[gb]
                elif state_name == 'reserve':
                    # Приоритет 2: rsv-очередь (реальные агрегаты после ремонта)
                    agent.setVariableUInt("queue_position", rsv_positions[gb])
                    rsv_positions[gb] += 1
                    rsv_tails[gb] = rsv_positions[gb]
                else:
                    # Для агентов в других состояниях — позиция 0
                    agent.setVariableUInt("queue_position", 0)
                
                # === mfg_date ===
                mfg = unit.get('mfg_date')
                if mfg:
                    mfg_days = (mfg - date(1970, 1, 1)).days
                else:
                    mfg_days = 0
                agent.setVariableUInt("mfg_date", mfg_days)
                
                # === intent_state ===
                if status_id == 4:
                    agent.setVariableUInt("intent_state", 4)
                elif status_id == 6:
                    agent.setVariableUInt("intent_state", 6)
                else:
                    agent.setVariableUInt("intent_state", 2)
                
                idx += 1
        
        # === Создание spawn-резерва (active=0) ===
        reserve_slots = env_data.get('reserve_slots', 0)
        spawn_group_counts = env_data.get('group_counts', {})  # Из get_env_data
        
        if reserve_slots > 0 and spawn_group_counts:
            print(f"   🔄 Создаём {reserve_slots} spawn-слотов...")
            spawn_pop = populations.get('reserve', fg.AgentVector(agent_def))
            
            total_units = sum(spawn_group_counts.values())
            # Распределяем по группам пропорционально
            for gb, count in spawn_group_counts.items():
                # Резерв пропорционален размеру группы
                slots_for_group = max(10, int(reserve_slots * count / total_units)) if total_units > 0 else 10
                
                for _ in range(slots_for_group):
                    spawn_pop.push_back()
                    spawn_agent = spawn_pop.back()
                    
                    spawn_agent.setVariableUInt("idx", idx)
                    spawn_agent.setVariableUInt("psn", 1000000 + idx)  # Синтетический PSN
                    spawn_agent.setVariableUInt("active", 0)  # Spawn-резерв
                    spawn_agent.setVariableUInt("aircraft_number", 0)
                    spawn_agent.setVariableUInt("partseqno_i", 0)
                    spawn_agent.setVariableUInt("group_by", gb)
                    spawn_agent.setVariableUInt("sne", 0)
                    spawn_agent.setVariableUInt("ppr", 0)
                    spawn_agent.setVariableUInt("repair_days", 0)
                    spawn_agent.setVariableUInt("queue_position", 0)  # Не в очереди
                    spawn_agent.setVariableUInt("intent_state", 5)  # reserve
                    spawn_agent.setVariableUInt("mfg_date", 0)
                    
                    # Нормативы из первого агрегата группы (если есть)
                    if gb in units_by_group and len(units_by_group[gb]) > 0:
                        sample_unit = units_by_group[gb][0]
                        partseqno = sample_unit['partseqno_i']
                        norms = mp1_norms.get(partseqno, {})
                        ac_mask = sample_unit.get('ac_type_mask', 96)
                        
                        if ac_mask & 64:
                            spawn_agent.setVariableUInt("ll", norms.get('ll_mi17', 0))
                            spawn_agent.setVariableUInt("oh", norms.get('oh_mi17', 0))
                            spawn_agent.setVariableUInt("br", norms.get('br_mi17', 0))
                        else:
                            spawn_agent.setVariableUInt("ll", norms.get('ll_mi8', 0))
                            spawn_agent.setVariableUInt("oh", norms.get('oh_mi8', 0))
                            spawn_agent.setVariableUInt("br", norms.get('br_mi8', 0))
                        spawn_agent.setVariableUInt("repair_time", norms.get('repair_time', 30))
                    else:
                        spawn_agent.setVariableUInt("ll", 0)
                        spawn_agent.setVariableUInt("oh", 0)
                        spawn_agent.setVariableUInt("br", 0)
                        spawn_agent.setVariableUInt("repair_time", 30)
                    
                    idx += 1
            
            populations['reserve'] = spawn_pop
            print(f"   ✅ Создано {idx - len(self.units_data)} spawn-слотов")
        
        # === Загружаем популяции в симуляцию ===
        all_states = ['operations', 'serviceable', 'repair', 'reserve', 'storage']
        for state_name in all_states:
            pop = populations.get(state_name, fg.AgentVector(agent_def))
            simulation.setPopulationData(pop, state_name)
            if len(pop) > 0:
                print(f"   Загружено {len(pop)} агентов в состояние '{state_name}'")
        
        # === Сохраняем tails для инициализации MacroProperty ===
        self.svc_tails = svc_tails
        self.rsv_tails = rsv_tails
        
        # === Статистика ===
        print(f"   Всего загружено: {idx} агрегатов")
        print(f"   FIFO-очереди (трёхуровневая система):")
        for gb in sorted(set(svc_tails.keys()) | set(rsv_tails.keys())):
            svc_t = svc_tails.get(gb, 0)
            rsv_t = rsv_tails.get(gb, 0)
            if svc_t > 0 or rsv_t > 0:
                print(f"      group_by={gb}: svc_tail={svc_t}, rsv_tail={rsv_t}")
        
        # Сохраняем tails в env_data для инициализации MacroProperty
        env_data['svc_tails'] = svc_tails
        env_data['rsv_tails'] = rsv_tails

