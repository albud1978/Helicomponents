#!/usr/bin/env python3
"""
Подготовка Environment для симуляции
Упрощенная версия sim_env_setup.py с четкой структурой
Дата: 2025-09-12
"""

from typing import Dict, List, Tuple
from datetime import date
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))
from config_loader import get_clickhouse_client
from .days_limiter import DaysLimiter


class EnvironmentSetup:
    """Подготовка данных Environment для FLAME GPU"""
    
    def __init__(self):
        self.client = get_clickhouse_client()
        self.days_limiter = DaysLimiter()
    
    def prepare_full_environment(self) -> Dict[str, any]:
        """Подготавливает полное окружение для симуляции"""
        
        # 1. Базовые версии и данные
        vdate, vid = self._fetch_versions()
        mp1_map = self._fetch_mp1_data()
        mp3_rows, mp3_fields = self._fetch_mp3_data(vdate, vid)
        mp4_by_day = self._fetch_mp4_data()
        mp5_by_day = self._fetch_mp5_data()
        
        # 2. Построение индексов и массивов
        days_sorted = sorted(mp4_by_day.keys())
        frames_index, frames_total = self._build_frames_index(mp3_rows, mp3_fields, mp5_by_day)
        
        # 3. Линейные массивы для GPU
        mp5_linear = self._build_mp5_linear(mp5_by_day, days_sorted, frames_index, frames_total)
        mp4_ops8, mp4_ops17 = self._build_mp4_arrays(mp4_by_day, days_sorted)
        mp4_spawn_seed = self._build_spawn_seed(mp4_by_day, days_sorted)
        month_first = self._build_month_first(days_sorted)
        
        # 4. MP1 и MP3 массивы
        mp1_oh_map = self._fetch_mp1_oh_data()
        mp1_arrays = self._build_mp1_arrays(mp1_map, mp1_oh_map)
        mp3_arrays = self._build_mp3_arrays(mp3_rows, mp3_fields)
        
        # 5. Сборка результата
        env_data = {
            # Скаляры
            'version_date_u16': self._date_to_ordinal(vdate),
            'version_id': vid,
            'frames_total': frames_total,
            'frames_initial': len(mp3_rows),
            'days_total': len(days_sorted),
            
            # Индексы и метаданные
            'frames_index': frames_index,
            'days_sorted': days_sorted,
            
            # MP4 квоты
            'mp4_ops_counter_mi8': mp4_ops8,
            'mp4_ops_counter_mi17': mp4_ops17,
            'mp4_new_counter_mi17_seed': mp4_spawn_seed,
            'month_first_u32': month_first,
            
            # MP5 налеты
            'mp5_daily_hours_linear': mp5_linear,
            
            # MP1 справочники
            **mp1_arrays,
            
            # MP3 агентные данные
            'mp3_arrays': mp3_arrays,
            'mp3_count': len(mp3_rows),
            'mp3_rows': mp3_rows,
            'mp3_fields': mp3_fields,
        }
        
        # 6. Валидация
        self._validate_environment(env_data)
        
        return env_data
    
    def prepare_environment_for_period(self, test_type: str = "smoke", custom_days: int = None) -> Dict[str, any]:
        """Подготавливает окружение для указанного периода тестирования"""
        
        # Получаем полные данные
        full_env = self.prepare_full_environment()
        
        # Определяем период
        target_days, desc = self.days_limiter.get_test_period(test_type, custom_days)
        
        print(f"🎯 {desc}")
        
        # Обрезаем данные если нужно
        if target_days < full_env['days_total']:
            limited_env = self.days_limiter.slice_data_arrays(full_env, target_days)
            return limited_env
        else:
            print(f"📊 Используем полные данные: {full_env['days_total']} дней")
            return full_env
    
    def _fetch_versions(self) -> Tuple[date, int]:
        """Получает последнюю версию данных"""
        rows = self.client.execute(
            "SELECT version_date, version_id FROM heli_pandas "
            "ORDER BY version_date DESC, version_id DESC LIMIT 1"
        )
        vd, vid = rows[0]
        return vd, int(vid)
    
    def _fetch_mp1_data(self) -> Dict[int, Tuple[int, int, int, int, int]]:
        """Загружает MP1 данные (br, repair_time, partout_time, assembly_time)"""
        rows = self.client.execute(
            "SELECT partno_comp, br_mi8, br_mi17, repair_time, partout_time, assembly_time "
            "FROM md_components"
        )
        return {
            int(p): (int(b8 or 0), int(b17 or 0), int(rt or 0), int(pt or 0), int(at or 0))
            for p, b8, b17, rt, pt, at in rows
        }
    
    def _fetch_mp3_data(self, vdate: date, vid: int) -> Tuple[List, List[str]]:
        """Загружает MP3 данные (только планеры для симуляции)"""
        fields = [
            'partseqno_i', 'psn', 'aircraft_number', 'ac_type_mask', 'group_by', 'status_id',
            'll', 'oh', 'oh_threshold', 'sne', 'ppr', 'repair_days', 'mfg_date'
        ]
        sql = f"""
        SELECT {', '.join(fields)}
        FROM heli_pandas
        WHERE version_date = '{vdate}' AND version_id = {vid}
        ORDER BY psn
        """
        rows = self.client.execute(sql)
        return rows, fields
    
    def _fetch_mp4_data(self) -> Dict[date, Dict[str, int]]:
        """Загружает MP4 данные (квоты и операции)"""
        rows = self.client.execute(
            "SELECT dates, ops_counter_mi8, ops_counter_mi17, trigger_program_mi8, "
            "trigger_program_mi17, new_counter_mi17 FROM flight_program_ac ORDER BY dates"
        )
        result = {}
        for d, mi8, mi17, t8, t17, n17 in rows:
            result[d] = {
                "ops_counter_mi8": int(mi8 or 0),
                "ops_counter_mi17": int(mi17 or 0),
                "trigger_program_mi8": int(t8 or 0),
                "trigger_program_mi17": int(t17 or 0),
                "new_counter_mi17": int(n17 or 0),
            }
        return result
    
    def _fetch_mp5_data(self) -> Dict[date, Dict[int, int]]:
        """Загружает MP5 данные (налеты по дням)"""
        rows = self.client.execute(
            "SELECT dates, aircraft_number, daily_hours FROM flight_program_fl "
            "ORDER BY dates, aircraft_number"
        )
        result = {}
        for d, ac, h in rows:
            m = result.setdefault(d, {})
            m[int(ac)] = int(h or 0)
        return result
    
    def _build_frames_index(self, mp3_rows, mp3_fields: List[str], 
                           mp5_by_day: Dict[date, Dict[int, int]] = None) -> Tuple[Dict[int, int], int]:
        """Строит индекс кадров по aircraft_number"""
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        ac_set = set()
        
        # Добавляем aircraft_number из MP3 (только планеры)
        for r in mp3_rows:
            ac = int(r[idx_map['aircraft_number']] or 0)
            if ac <= 0:
                continue
            
            # Проверяем что это планер (group_by in 1,2 или ac_type_mask & 32|64)
            is_plane = False
            if 'group_by' in idx_map:
                gb = int(r[idx_map['group_by']] or 0)
                is_plane = gb in (1, 2)
            elif 'ac_type_mask' in idx_map:
                mask = int(r[idx_map['ac_type_mask']] or 0)
                is_plane = (mask & (32 | 64)) != 0
            
            if is_plane:
                ac_set.add(ac)
        
        # Добавляем aircraft_number из MP5 (будущие вертолеты для спавна)
        if mp5_by_day:
            for day_data in mp5_by_day.values():
                for ac in day_data.keys():
                    if ac > 0:
                        ac_set.add(ac)
        
        # Сортируем и создаем индекс
        ac_list = sorted([ac for ac in ac_set if ac > 0])
        frames_index = {ac: i for i, ac in enumerate(ac_list)}
        
        return frames_index, len(ac_list)
    
    def _build_mp5_linear(self, mp5_by_day: Dict[date, Dict[int, int]], 
                         days_sorted: List[date], frames_index: Dict[int, int], 
                         frames_total: int) -> List[int]:
        """Строит линейный массив MP5 с паддингом D+1"""
        days_total = len(days_sorted)
        size = (days_total + 1) * frames_total  # Паддинг D+1
        arr = [0] * size
        
        for d_idx, D in enumerate(days_sorted):
            by_ac = mp5_by_day.get(D, {})
            base = d_idx * frames_total
            for ac, hours in by_ac.items():
                fi = frames_index.get(int(ac), -1)
                if fi >= 0:
                    arr[base + fi] = int(hours or 0)
        
        # Последний день (паддинг) остается нулями
        return arr
    
    def _build_mp4_arrays(self, mp4_by_day: Dict[date, Dict[str, int]], 
                         days_sorted: List[date]) -> Tuple[List[int], List[int]]:
        """Строит массивы MP4 квот"""
        ops8 = []
        ops17 = []
        for D in days_sorted:
            m = mp4_by_day.get(D, {})
            ops8.append(int(m.get('ops_counter_mi8', 0)))
            ops17.append(int(m.get('ops_counter_mi17', 0)))
        return ops8, ops17
    
    def _build_spawn_seed(self, mp4_by_day: Dict[date, Dict[str, int]], 
                         days_sorted: List[date]) -> List[int]:
        """Строит массив планов спавна MI-17"""
        spawn_seed = []
        for D in days_sorted:
            m = mp4_by_day.get(D, {})
            v = int(m.get('new_counter_mi17', 0))
            spawn_seed.append(max(0, v))
        return spawn_seed
    
    def _build_month_first(self, days_sorted: List[date]) -> List[int]:
        """Строит массив первых дней месяцев"""
        month_first = []
        for D in days_sorted:
            first = date(D.year, D.month, 1)
            month_first.append(self._date_to_ordinal(first))
        return month_first
    
    def _build_mp1_arrays(self, mp1_map: Dict[int, Tuple[int, int, int, int, int]], 
                         mp1_oh_map: Dict[int, Tuple[int, int]] = None) -> Dict[str, List[int]]:
        """Строит SoA массивы MP1"""
        keys = sorted(mp1_map.keys())
        idx_map = {k: i for i, k in enumerate(keys)}
        
        arrays = {
            'mp1_br_mi8': [],
            'mp1_br_mi17': [],
            'mp1_repair_time': [],
            'mp1_partout_time': [],
            'mp1_assembly_time': [],
            'mp1_oh_mi8': [],
            'mp1_oh_mi17': [],
            'mp1_index': idx_map
        }
        
        for k in keys:
            b8, b17, rt, pt, at = mp1_map.get(k, (0, 0, 0, 0, 0))
            arrays['mp1_br_mi8'].append(int(b8 or 0))
            arrays['mp1_br_mi17'].append(int(b17 or 0))
            arrays['mp1_repair_time'].append(int(rt or 0))
            arrays['mp1_partout_time'].append(int(pt or 0))
            arrays['mp1_assembly_time'].append(int(at or 0))
            
            # OH данные
            if mp1_oh_map and k in mp1_oh_map:
                oh8, oh17 = mp1_oh_map[k]
                arrays['mp1_oh_mi8'].append(int(oh8 or 0))
                arrays['mp1_oh_mi17'].append(int(oh17 or 0))
            else:
                arrays['mp1_oh_mi8'].append(0)
                arrays['mp1_oh_mi17'].append(0)
        
        return arrays
    
    def _build_mp3_arrays(self, mp3_rows, mp3_fields: List[str]) -> Dict[str, List[int]]:
        """Строит SoA массивы MP3"""
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        arrays = {
            'mp3_psn': [],
            'mp3_aircraft_number': [],
            'mp3_ac_type_mask': [],
            'mp3_group_by': [],
            'mp3_status_id': [],
            'mp3_sne': [],
            'mp3_ppr': [],
            'mp3_repair_days': [],
            'mp3_ll': [],
            'mp3_oh': [],
            'mp3_mfg_date_days': [],
        }
        
        epoch = date(1970, 1, 1)
        for r in mp3_rows:
            arrays['mp3_psn'].append(int(r[idx_map['psn']] or 0))
            arrays['mp3_aircraft_number'].append(int(r[idx_map['aircraft_number']] or 0))
            arrays['mp3_ac_type_mask'].append(int(r[idx_map['ac_type_mask']] or 0))
            arrays['mp3_group_by'].append(int(r[idx_map.get('group_by', -1)] or 0))
            arrays['mp3_status_id'].append(int(r[idx_map['status_id']] or 0))
            arrays['mp3_sne'].append(int(r[idx_map['sne']] or 0))
            arrays['mp3_ppr'].append(int(r[idx_map['ppr']] or 0))
            arrays['mp3_repair_days'].append(int(r[idx_map['repair_days']] or 0))
            arrays['mp3_ll'].append(int(r[idx_map['ll']] or 0))
            arrays['mp3_oh'].append(int(r[idx_map['oh']] or 0))
            
            # mfg_date -> ordinal days
            md = r[idx_map.get('mfg_date', -1)] if 'mfg_date' in idx_map else None
            ord_days = 0
            if md:
                try:
                    ord_days = max(0, int((md - epoch).days))
                except:
                    ord_days = 0
            arrays['mp3_mfg_date_days'].append(ord_days)
        
        return arrays
    
    def _fetch_mp1_oh_data(self) -> Dict[int, Tuple[int, int]]:
        """Загружает OH данные из MP1"""
        rows = self.client.execute(
            "SELECT partno_comp, oh_mi8, oh_mi17 FROM md_components"
        )
        return {
            int(p): (int(oh8 or 0), int(oh17 or 0))
            for p, oh8, oh17 in rows
        }
    
    def _validate_environment(self, env_data: Dict[str, any]):
        """Валидация подготовленных данных"""
        dt = env_data['days_total']
        ft = env_data['frames_total']
        
        # Проверки размеров массивов
        assert len(env_data['mp4_ops_counter_mi8']) == dt, "MP4_mi8 размер != days_total"
        assert len(env_data['mp4_ops_counter_mi17']) == dt, "MP4_mi17 размер != days_total"
        assert len(env_data['mp5_daily_hours_linear']) == (dt + 1) * ft, "MP5 размер != (days_total+1)*frames_total"
        assert len(env_data['month_first_u32']) == dt, "month_first размер != days_total"
        
        # Проверки MP3 массивов
        mp3_count = env_data['mp3_count']
        for key in env_data['mp3_arrays']:
            assert len(env_data['mp3_arrays'][key]) == mp3_count, f"MP3 {key} размер != mp3_count"
        
        print(f"✅ Environment валидация пройдена: {dt} дней, {ft} кадров, {mp3_count} агентов")
    
    def apply_to_simulation(self, sim, env_data: Dict[str, any]):
        """Применяет подготовленные данные к симуляции"""
        
        # Скаляры
        sim.setEnvironmentPropertyUInt("version_date", env_data['version_date_u16'])
        sim.setEnvironmentPropertyUInt("frames_total", env_data['frames_total'])
        sim.setEnvironmentPropertyUInt("days_total", env_data['days_total'])
        sim.setEnvironmentPropertyUInt("frames_initial", env_data['frames_initial'])
        
        # MP4 массивы
        sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", env_data['mp4_ops_counter_mi8'])
        sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", env_data['mp4_ops_counter_mi17'])
        sim.setEnvironmentPropertyArrayUInt32("mp4_new_counter_mi17_seed", env_data['mp4_new_counter_mi17_seed'])
        sim.setEnvironmentPropertyArrayUInt32("month_first_u32", env_data['month_first_u32'])
        
        # MP5 массив
        sim.setEnvironmentPropertyArrayUInt16("mp5_daily_hours", env_data['mp5_daily_hours_linear'])
        
        # MP1 массивы
        sim.setEnvironmentPropertyArrayUInt32("mp1_br_mi8", env_data['mp1_br_mi8'])
        sim.setEnvironmentPropertyArrayUInt32("mp1_br_mi17", env_data['mp1_br_mi17'])
        sim.setEnvironmentPropertyArrayUInt32("mp1_repair_time", env_data['mp1_repair_time'])
        sim.setEnvironmentPropertyArrayUInt32("mp1_partout_time", env_data['mp1_partout_time'])
        sim.setEnvironmentPropertyArrayUInt32("mp1_assembly_time", env_data['mp1_assembly_time'])
        
        if 'mp1_oh_mi8' in env_data:
            sim.setEnvironmentPropertyArrayUInt32("mp1_oh_mi8", env_data['mp1_oh_mi8'])
        if 'mp1_oh_mi17' in env_data:
            sim.setEnvironmentPropertyArrayUInt32("mp1_oh_mi17", env_data['mp1_oh_mi17'])
        
        # MP3 массивы
        mp3_arrays = env_data['mp3_arrays']
        for key, values in mp3_arrays.items():
            sim.setEnvironmentPropertyArrayUInt32(key, values)
        
        print(f"✅ Environment применен к симуляции")
    
    @staticmethod
    def _date_to_ordinal(d: date) -> int:
        """Конвертирует дату в ordinal дни от 1970-01-01"""
        epoch = date(1970, 1, 1)
        return max(0, (d - epoch).days)
