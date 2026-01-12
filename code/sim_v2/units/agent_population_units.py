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
          AND group_by IN (3, 4)  -- ТВ2-117 (Mi-8) и ТВ3-117 (Mi-17)
        ORDER BY group_by, mfg_date
        """
        
        rows = self.client.execute(sql)
        
        # Загружаем список планеров в operations (status_id=2)
        planers_in_ops_sql = f"""
        SELECT DISTINCT toUInt32(serialno) as ac
        FROM heli_pandas
        WHERE version_date = toDate('{self.version_date}')
          AND version_id = {self.version_id}
          AND group_by IN (1, 2)
          AND status_id = 2
        """
        planers_in_ops = set(r[0] for r in self.client.execute(planers_in_ops_sql))
        print(f"   Планеров в operations: {len(planers_in_ops)}")
        
        self.units_data = []
        fixed_ac_count = 0
        for row in rows:
            ac_num = int(row[1] or 0)
            status_id = int(row[4] or 0)
            
            # Если агрегат привязан к планеру, но планер НЕ в operations
            # → агрегат должен быть serviceable с aircraft_number=0
            if ac_num > 0 and ac_num not in planers_in_ops:
                if status_id == 2:  # operations → serviceable
                    status_id = 3
                ac_num = 0
                fixed_ac_count += 1
            
            self.units_data.append({
                'psn': int(row[0] or 0),
                'aircraft_number': ac_num,
                'partseqno_i': int(row[2] or 0),
                'group_by': int(row[3] or 0),
                'status_id': status_id,
                'sne': int(row[5] or 0),
                'ppr': int(row[6] or 0),
                'll': int(row[7] or 0),
                'repair_days': int(row[8] or 0),
                'mfg_date': row[9] if row[9] else None,
                'ac_type_mask': int(row[10] or 0)
            })
        
        if fixed_ac_count > 0:
            print(f"   ⚠️ Исправлено {fixed_ac_count} агрегатов с aircraft_number на планере не в operations")
        
        print(f"   Загружено {len(self.units_data)} агрегатов (group_by=4 — ТВ3-117 для Mi-17)")
    
    def _load_mp1_norms(self):
        """Загружает нормативы из md_components"""
        sql = """
        SELECT 
            partno_comp,
            ll_mi8, ll_mi17,
            oh_mi8, oh_mi17,
            br_mi8, br_mi17,
            repair_time
        FROM md_components
        WHERE partno_comp IS NOT NULL AND partno_comp > 0
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
        Расчёт количества резервных слотов для spawn агрегатов.
        
        Универсальная формула (согласована с sim_env_setup.py):
        1. Если OH=0 или OH>=LL: simple = total_flight / LL × 1.2
        2. Если OH>0:
           - total_exits = total_flight / OH (всего выходов из эксплуатации)
           - permanent_exits = total_exits × (OH/LL) (списания)
           - repairs_count = total_exits × (1 - OH/LL) (ремонты)
           - repair_buffer = (repairs_count/10) × (repair_time/365) × 1.2
           - reserve = (permanent_exits + repair_buffer) × 1.2
        
        МУЛЬТИНОМЕНКЛАТУРА: если в группе несколько номенклатур с разными
        ресурсами — берём МАКСИМАЛЬНЫЙ резерв среди всех номенклатур группы.
        
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
        multi_nomen_groups = []  # Группы с мультиноменклатурой
        
        for group_by, existing_count in group_counts.items():
            # === МУЛЬТИНОМЕНКЛАТУРА: получаем ВСЕ номенклатуры группы ===
            nomenclatures = self._get_nomenclatures_for_group(group_by)
            
            if not nomenclatures:
                # Если нет номенклатур — минимальный резерв
                group_reserve = max(10, int(existing_count * 0.20))
                total_reserve += group_reserve
                continue
            
            # Определяем преобладающую маску для выбора налёта
            masks = [n['ac_mask'] for n in nomenclatures]
            ac_mask = max(set(masks), key=masks.count) if masks else 96
            
            # Выбираем правильный налёт по ac_type_mask
            if ac_mask == 32:  # Только Mi-8
                flight_10y = flight_mi8_10y
            elif ac_mask == 64:  # Только Mi-17
                flight_10y = flight_mi17_10y
            else:  # Универсальный (96) — средний
                flight_10y = (flight_mi8_10y + flight_mi17_10y) / 2
            
            # === Считаем резерв для КАЖДОЙ номенклатуры и берём МАКСИМУМ ===
            # ВАЖНО: учитываем сколько существующих агрегатов выбудет за 10 лет!
            existing_will_retire = self._count_existing_will_retire(
                group_by, 
                DAYS_10_YEARS, 
                AVG_DAILY_FLIGHT_MIN
            )
            
            reserves_by_nomen = []
            for nomen in nomenclatures:
                ll = nomen['ll']
                oh = nomen['oh']
                rt = nomen['repair_time']
                
                if ll > 0:
                    # Базовый резерв (на новый налёт)
                    base_reserve = self._calc_reserve_universal(
                        total_flight=flight_10y,
                        ll_minutes=ll,
                        oh_minutes=oh,
                        repair_time_days=rt,
                        safety_margin=SAFETY_MARGIN
                    )
                    # Добавляем выбывающие существующие агрегаты
                    nomen_reserve = base_reserve + existing_will_retire
                else:
                    nomen_reserve = 10
                
                reserves_by_nomen.append({
                    'partseqno': nomen['partseqno'],
                    'reserve': nomen_reserve,
                    'll': ll,
                    'oh': oh,
                    'rt': rt,
                    'existing_retire': existing_will_retire
                })
            
            # Берём МАКСИМАЛЬНЫЙ резерв среди всех номенклатур группы
            group_reserve = max(r['reserve'] for r in reserves_by_nomen)
            
            # Для лопастей (group_by=6): минимум 2000
            if group_by == 6:
                min_reserve = max(2000, int(existing_count * 0.5))
                group_reserve = max(min_reserve, group_reserve)
            else:
                group_reserve = max(10, group_reserve)
            
            # Диагностика мультиноменклатуры
            if len(nomenclatures) > 1:
                best_nomen = max(reserves_by_nomen, key=lambda x: x['reserve'])
                multi_nomen_groups.append((
                    group_by, len(nomenclatures), group_reserve,
                    best_nomen['partseqno'], best_nomen['ll'], best_nomen['oh'], best_nomen['rt']
                ))
            
            # Собираем топ для диагностики
            if group_reserve > 50:
                best = max(reserves_by_nomen, key=lambda x: x['reserve'])
                top_consumers.append((
                    group_by, existing_count, group_reserve,
                    best['ll'], best['oh'], best['rt'], len(nomenclatures),
                    best.get('existing_retire', 0)
                ))
            
            total_reserve += group_reserve
        
        # Диагностика мультиноменклатуры
        if multi_nomen_groups:
            print(f"   🔀 Мультиноменклатура ({len(multi_nomen_groups)} групп):")
            for gb, n_nom, reserve, psn, ll, oh, rt in multi_nomen_groups[:5]:
                print(f"      group_by={gb}: {n_nom} номенклатур → reserve={reserve} "
                      f"(best: PSN={psn}, LL={ll//60}ч, OH={oh//60}ч, RT={rt}дн)")
        
        # Диагностика топ-5 групп с большим резервом
        if top_consumers:
            top_consumers.sort(key=lambda x: x[2], reverse=True)
            print("   📈 Топ-5 групп по резерву (с учётом выбытия существующих):")
            for item in top_consumers[:5]:
                gb, exist, reserve, ll, oh, rt, n_nom = item[:7]
                existing_retire = item[7] if len(item) > 7 else 0
                nomen_info = f" ({n_nom} номенкл.)" if n_nom > 1 else ""
                # Расшифровка формулы
                if oh > 0 and oh < ll:
                    total_exits = flight_mi17_10y / oh  # Примерно
                    perm = total_exits * (oh / ll)
                    repairs = total_exits * (1 - oh / ll)
                    repair_buf = (repairs / 10) * (rt / 365) * 1.2
                    base = int((perm + repair_buf) * 1.2)
                    print(f"      group_by={gb}{nomen_info}: exist={exist}, reserve={reserve}")
                    print(f"         LL={ll//60}ч, OH={oh//60}ч, RT={rt}дн")
                    print(f"         base={base}, existing_retire={existing_retire}")
                else:
                    consumed = flight_mi17_10y / ll if ll > 0 else 0
                    print(f"      group_by={gb}{nomen_info}: exist={exist}, reserve={reserve}")
                    print(f"         LL={ll//60}ч, consumed={int(consumed)}, existing_retire={existing_retire}")
        
        # Минимум 500 резервных слотов
        return max(500, total_reserve)
    
    def _get_nomenclatures_for_group(self, group_by: int) -> List[Dict]:
        """
        Получает все уникальные номенклатуры (partseqno) группы с их ресурсами.
        
        Returns:
            Список словарей: [{partseqno, ll, oh, repair_time, ac_mask}, ...]
        """
        nomenclatures = {}  # partseqno -> {ll, oh, repair_time, ac_mask}
        
        for unit in self.units_data:
            if unit['group_by'] == group_by:
                partseqno = unit.get('partseqno_i', 0)
                if partseqno in nomenclatures:
                    continue  # Уже обработана эта номенклатура
                
                norms = self.mp1_norms.get(partseqno, {})
                ac_mask = unit.get('ac_type_mask', 96)
                
                # Берём LL и OH по типу ВС
                if ac_mask == 32:  # Mi-8
                    ll = norms.get('ll_mi8', 0)
                    oh = norms.get('oh_mi8', 0)
                elif ac_mask == 64:  # Mi-17
                    ll = norms.get('ll_mi17', 0)
                    oh = norms.get('oh_mi17', 0)
                else:  # Универсальный — берём максимум
                    ll = max(norms.get('ll_mi8', 0), norms.get('ll_mi17', 0))
                    oh = max(norms.get('oh_mi8', 0), norms.get('oh_mi17', 0))
                
                repair_time = norms.get('repair_time', 30)
                
                nomenclatures[partseqno] = {
                    'partseqno': partseqno,
                    'll': ll,
                    'oh': oh,
                    'repair_time': repair_time,
                    'ac_mask': ac_mask
                }
        
        return list(nomenclatures.values())
    
    def _calc_reserve_universal(
        self,
        total_flight: float,
        ll_minutes: int,
        oh_minutes: int = 0,
        repair_time_days: int = 0,
        safety_margin: float = 1.2
    ) -> int:
        """
        Универсальная формула расчёта резерва (дублирует sim_env_setup.py).
        
        Если OH=0 или OH>=LL: простая формула total_flight / LL × 1.2
        Если OH>0: учитываем ремонты.
        """
        if ll_minutes <= 0 or total_flight <= 0:
            return 10  # Минимум
        
        # Если нет OH или OH >= LL — только списания
        if oh_minutes <= 0 or oh_minutes >= ll_minutes:
            consumed = total_flight / ll_minutes
            return max(10, int(round(consumed * safety_margin)))
        
        # === Формула с учётом ремонтов ===
        # 1. Всего выходов из эксплуатации (по OH)
        total_exits = total_flight / oh_minutes
        
        # 2. Из них списания (конец жизни): доля OH/LL
        permanent_exits = total_exits * (oh_minutes / ll_minutes)
        
        # 3. Ремонты (временное выбытие): (1 - OH/LL)
        repairs_count = total_exits * (1 - oh_minutes / ll_minutes)
        
        # 4. Буфер на время ремонта (сколько одновременно в ремонте за год)
        repair_buffer = 0
        if repair_time_days > 0:
            annual_repairs = repairs_count / 10.0
            repair_buffer = annual_repairs * (repair_time_days / 365.0) * safety_margin
        
        # 5. Итого: (списания + ремонтный буфер) × safety_margin
        average_exits = permanent_exits + repair_buffer
        
        return max(10, int(round(average_exits * safety_margin)))
    
    def _count_existing_will_retire(
        self, 
        group_by: int, 
        sim_days: int, 
        avg_daily_flight_min: int
    ) -> int:
        """
        Подсчитывает сколько существующих агрегатов выбудет по LL за период симуляции.
        
        Учитывает текущую наработку (SNE) каждого агрегата:
        remaining = LL - SNE
        days_to_retire = remaining / avg_daily_flight
        Если days_to_retire < sim_days → агрегат выбудет
        """
        will_retire = 0
        
        for unit in self.units_data:
            if unit['group_by'] != group_by:
                continue
            
            sne = unit.get('sne', 0)
            ll = unit.get('ll', 0)
            
            if ll <= 0:
                continue
            
            remaining = ll - sne
            if remaining <= 0:
                # Уже выработан — точно выбудет
                will_retire += 1
                continue
            
            # Сколько дней до выбытия
            days_to_retire = remaining / avg_daily_flight_min if avg_daily_flight_min > 0 else 999999
            
            if days_to_retire < sim_days:
                will_retire += 1
        
        return will_retire
    
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
    
    def _get_ll_oh_and_mask_for_group(self, group_by: int) -> Tuple[int, int, int]:
        """
        Получает средний LL, OH и преобладающий ac_type_mask для группы.
        
        Returns:
            (avg_ll, avg_oh, avg_mask): Средний LL (мин), OH (мин), ac_type_mask
        """
        lls = []
        ohs = []
        masks = []
        
        for unit in self.units_data:
            if unit['group_by'] == group_by:
                partseqno = unit.get('partseqno_i', 0)
                norms = self.mp1_norms.get(partseqno, {})
                ac_mask = unit.get('ac_type_mask', 96)
                
                # Берём LL и OH по типу ВС
                if ac_mask == 32:  # Mi-8
                    ll = norms.get('ll_mi8', 0)
                    oh = norms.get('oh_mi8', 0)
                elif ac_mask == 64:  # Mi-17
                    ll = norms.get('ll_mi17', 0)
                    oh = norms.get('oh_mi17', 0)
                else:  # Универсальный — берём максимум
                    ll = max(norms.get('ll_mi8', 0), norms.get('ll_mi17', 0))
                    oh = max(norms.get('oh_mi8', 0), norms.get('oh_mi17', 0))
                
                if ll > 0:
                    lls.append(ll)
                    masks.append(ac_mask)
                if oh > 0:
                    ohs.append(oh)
        
        avg_ll = sum(lls) // len(lls) if lls else 0
        avg_oh = sum(ohs) // len(ohs) if ohs else 0
        # Берём наиболее частую маску
        avg_mask = max(set(masks), key=masks.count) if masks else 96
        
        return avg_ll, avg_oh, avg_mask
    
    def _get_repair_time_for_group(self, group_by: int) -> int:
        """Получает средний repair_time для группы (в днях)"""
        repair_times = []
        
        for unit in self.units_data:
            if unit['group_by'] == group_by:
                partseqno = unit.get('partseqno_i', 0)
                norms = self.mp1_norms.get(partseqno, {})
                rt = norms.get('repair_time', 30)
                if rt > 0:
                    repair_times.append(rt)
        
        if repair_times:
            return sum(repair_times) // len(repair_times)
        return 30  # Дефолт 30 дней
    
    
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
        
        # Счётчик initial_slots для mp_planer_slots
        # Формат: (group_by, planer_idx) -> count
        initial_slots = {}
        
        # Маппинг aircraft_number -> planer_idx (из env_data)
        ac_to_idx = env_data.get('ac_to_idx', {})
        MAX_PLANERS = 400
        
        for gb in units_by_group:
            svc_positions[gb] = 1  # Начинаем с 1 (head=1 в init_fifo_queues)
            rsv_positions[gb] = 1  # Начинаем с 1
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
                ac_num = unit['aircraft_number']
                if status_id == 2 and (ac_num == 0 or ac_num is None):
                    status_id = 3  # serviceable
                
                state_name = status_to_state[status_id]
                pop = populations[state_name]
                pop.push_back()
                agent = pop[len(pop) - 1]
                
                # === Базовые переменные ===
                agent.setVariableUInt("idx", idx)
                agent.setVariableUInt("psn", unit['psn'])
                agent.setVariableUInt("active", 1)  # Реальный агрегат (не spawn-резерв)
                
                # FIX: Для serviceable/repair/reserve/storage — сбрасываем aircraft_number
                # В heli_pandas serviceable двигатели могут иметь AC (привязка для учёта),
                # но в симуляции serviceable = "готов к установке" = AC должен быть 0
                # чтобы assembly мог их назначить на планеры
                if state_name in ('serviceable', 'repair', 'reserve', 'storage'):
                    agent.setVariableUInt("aircraft_number", 0)
                else:
                    agent.setVariableUInt("aircraft_number", ac_num if ac_num else 0)
                agent.setVariableUInt("partseqno_i", unit['partseqno_i'])
                agent.setVariableUInt("group_by", unit['group_by'])
                
                # === Наработки ===
                sne_val = unit['sne']
                ppr_val = unit['ppr']
                ll_raw = unit['ll']  # LL из heli_pandas (может быть 0)
                
                # FIX: PPR = SNE для "новых" агрегатов без ремонта
                # Условие: sne > 0 AND ppr = 0 AND sne < ll
                # Логика: агрегат до первого ремонта не имеет PPR в исходных данных
                if ppr_val == 0 and sne_val > 0:
                    # LL может быть 0 в heli_pandas — берём из mp1_norms
                    partseqno_check = unit['partseqno_i']
                    norms_check = mp1_norms.get(partseqno_check, {})
                    ac_mask_check = unit['ac_type_mask']
                    
                    if ac_mask_check & 64:  # Mi-17
                        ll_check = norms_check.get('ll_mi17', ll_raw)
                    elif ac_mask_check & 32:  # Mi-8
                        ll_check = norms_check.get('ll_mi8', ll_raw)
                    else:
                        ll_check = max(norms_check.get('ll_mi17', 0), norms_check.get('ll_mi8', ll_raw))
                    
                    if ll_check == 0:
                        ll_check = ll_raw
                    
                    if ll_check > 0 and sne_val < ll_check:
                        ppr_val = sne_val  # PPR = SNE для "нового" агрегата
                
                agent.setVariableUInt("sne", sne_val)
                agent.setVariableUInt("ppr", ppr_val)
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
                
                # === FIX: Агрегаты в operations с превышением лимитов должны остаться в operations ===
                # В heli_pandas агрегаты могут летать с превышением если есть разрешение/продление.
                # Увеличиваем лимиты чтобы check_limits не отправил их сразу в ремонт/списание.
                if state_name == 'operations':
                    # PPR >= OH → увеличиваем OH
                    if oh_val > 0 and ppr_val >= oh_val:
                        oh_val = ppr_val + 30000  # +500 часов
                    # SNE >= BR → увеличиваем BR
                    if br_val > 0 and sne_val >= br_val:
                        br_val = sne_val + 30000  # +500 часов
                    # SNE >= LL → увеличиваем LL
                    if ll_val > 0 and sne_val >= ll_val:
                        ll_val = sne_val + 30000  # +500 часов
                
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
                elif state_name == 'operations' and ac_num and ac_num > 0:
                    # === Инициализация mp_planer_slots для агрегатов в operations ===
                    planer_idx = ac_to_idx.get(ac_num, 0)
                    if planer_idx > 0 and planer_idx < MAX_PLANERS:
                        slot_key = (gb, planer_idx)
                        initial_slots[slot_key] = initial_slots.get(slot_key, 0) + 1
                    agent.setVariableUInt("queue_position", 0)
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
        
        # === Сохраняем tails и initial_slots для инициализации MacroProperty ===
        self.svc_tails = svc_tails
        self.rsv_tails = rsv_tails
        self.initial_slots = initial_slots
        
        # === Статистика ===
        print(f"   Всего загружено: {idx} агрегатов")
        print(f"   FIFO-очереди (трёхуровневая система):")
        for gb in sorted(set(svc_tails.keys()) | set(rsv_tails.keys())):
            svc_t = svc_tails.get(gb, 0)
            rsv_t = rsv_tails.get(gb, 0)
            if svc_t > 0 or rsv_t > 0:
                print(f"      group_by={gb}: svc_tail={svc_t}, rsv_tail={rsv_t}")
        
        # initial_slots статистика
        total_slots = sum(initial_slots.values())
        print(f"   mp_planer_slots: {total_slots} начальных агрегатов на планерах")
        
        # Сохраняем tails в env_data для инициализации MacroProperty
        env_data['svc_tails'] = svc_tails
        env_data['rsv_tails'] = rsv_tails
        env_data['initial_slots'] = initial_slots

