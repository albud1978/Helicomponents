#!/usr/bin/env python3
"""
LIMITER V8 Orchestrator — Архитектура с RepairLine

Архитектура V8 (отличия от V7):
1. RepairLine — общий пул линий ремонта (repair_number из MP)
2. unsvc НЕ участвует в min_dynamic
3. RepairLine участвует в квотировании, а не в adaptive-шагах
4. Правило ресурса: next-day dt (SNE + dt >= LL)
5. limiter=0 — обязательный выход (EXCEPTION если нет)
6. Протокол сообщений RepairLine → QuotaManager

Порядок слоёв V8:
1. Детерминированные переходы (repair→svc, spawn→ops)
2. Сброс + сбор exit_date (ПОСЛЕ переходов)
3. Operations инкременты
4. Operations переходы с next-day dt проверкой
5. Квотирование через RepairLine
6. Limiter + adaptive steps

См. docs/architecture/adaptive_steps_logic.md для полной архитектуры.

Дата: 16.01.2026
"""

import os
import sys
import time
import argparse

# Пути
script_dir = os.path.dirname(os.path.abspath(__file__))
sim_v2_dir = os.path.dirname(script_dir)
code_dir = os.path.dirname(sim_v2_dir)
project_root = os.path.dirname(code_dir)

sys.path.insert(0, script_dir)
sys.path.insert(0, sim_v2_dir)
sys.path.insert(0, code_dir)
sys.path.insert(0, os.path.join(code_dir, 'utils'))

# Загрузка окружения
from config_loader import auto_load_env_file
auto_load_env_file()

# RTC kernel cache — устанавливаем ДО импорта pyflamegpu
# Без этого ~73 RTC ядра перекомпилируются при каждом запуске (~70с overhead)
_rtc_cache_dir = os.path.join(project_root, '.rtc_cache')
os.makedirs(_rtc_cache_dir, exist_ok=True)
os.environ.setdefault('FLAMEGPU_RTC_EXPORT_CACHE_PATH', _rtc_cache_dir)

import pyflamegpu as fg
import model_build

import rtc_spawn_dynamic_v7


def collect_agents_state(simulation, agent_desc, current_day, version_date_int, version_id,
                         step_id=0, prev_day=0, adaptive_days=0,
                         spawn_mgr_desc=None, repair_line_desc=None):
    """Собирает состояние всех агентов в текущий момент"""
    states = ['inactive', 'operations', 'serviceable', 'repair', 'storage', 'unserviceable']  # DISABLED (state5-unused): reserve removed
    rows = []
    
    def _get_env_u32(name: str, default: int = 0) -> int:
        if hasattr(simulation, "getEnvironmentPropertyUInt"):
            return int(simulation.getEnvironmentPropertyUInt(name))
        if hasattr(simulation, "getEnvironmentProperty"):
            return int(simulation.getEnvironmentProperty(name))
        return default
    
    env_current_day = _get_env_u32("current_day", current_day)
    spawn_debug_curr_ops = 0
    spawn_debug_target = 0
    spawn_debug_need = 0
    spawn_debug_curr_ops_mi8 = 0
    spawn_debug_target_mi8 = 0
    spawn_debug_need_mi8 = 0
    if spawn_mgr_desc is not None:
        pop_mgr = fg.AgentVector(spawn_mgr_desc)
        simulation.getPopulationData(pop_mgr, "default")
        if pop_mgr.size() > 0:
            mgr = pop_mgr.at(0)
            spawn_debug_curr_ops = mgr.getVariableUInt("debug_curr_ops")
            spawn_debug_target = mgr.getVariableUInt("debug_target")
            spawn_debug_need = mgr.getVariableUInt("debug_need")
            spawn_debug_curr_ops_mi8 = mgr.getVariableUInt("debug_curr_ops_mi8")
            spawn_debug_target_mi8 = mgr.getVariableUInt("debug_target_mi8")
            spawn_debug_need_mi8 = mgr.getVariableUInt("debug_need_mi8")
    
    # RepairLine stats (временное логирование)
    repair_time = _get_env_u32("mi17_repair_time_const", 0)
    rl_total = 0
    rl_free = 0
    rl_ready = 0
    rl_min_free = 0
    rl_max_free = 0
    if repair_line_desc is not None:
        pop_rl = fg.AgentVector(repair_line_desc)
        simulation.getPopulationData(pop_rl, "default")
        rl_total = pop_rl.size()
        if rl_total > 0:
            min_free = None
            max_free = None
            for i in range(rl_total):
                line = pop_rl.at(i)
                free_days = line.getVariableUInt("free_days")
                aircraft_number = line.getVariableUInt("aircraft_number")
                if min_free is None or free_days < min_free:
                    min_free = free_days
                if max_free is None or free_days > max_free:
                    max_free = free_days
                if aircraft_number == 0:
                    rl_free += 1
                    if repair_time > 0 and free_days >= repair_time:
                        rl_ready += 1
            rl_min_free = min_free if min_free is not None else 0
            rl_max_free = max_free if max_free is not None else 0
    
    # Сбор состояния агентов
    for state_name in states:
        pop = fg.AgentVector(agent_desc)
        simulation.getPopulationData(pop, state_name)
        
        for i in range(pop.size()):
            agent = pop.at(i)
            group_by = agent.getVariableUInt('group_by')
            repair_days = agent.getVariableUInt('repair_days')
            rows.append({
                'version_date': version_date_int,
                'version_id': version_id,
                'debug_step': step_id,
                'debug_prev_day': prev_day,
                'debug_adaptive_days': adaptive_days,
                'day_u16': current_day,
                'idx': agent.getVariableUInt('idx'),
                'aircraft_number': agent.getVariableUInt('aircraft_number'),
                'group_by': group_by,
                'state': state_name,
                'sne': agent.getVariableUInt('sne'),
                'ppr': agent.getVariableUInt('ppr'),
                'll': agent.getVariableUInt('ll'),
                'oh': agent.getVariableUInt('oh'),
                'br': agent.getVariableUInt('br'),
                'daily_today_u32': agent.getVariableUInt('daily_today_u32'),
                'daily_next_u32': agent.getVariableUInt('daily_next_u32'),
                'repair_days': repair_days,
                'repair_time': agent.getVariableUInt('repair_time'),
                'limiter': agent.getVariableUInt16('limiter'),
                'status_change_day': agent.getVariableUInt('status_change_day'),
                'promoted': agent.getVariableUInt('promoted'),
                'needs_demote': agent.getVariableUInt('needs_demote'),
                'repair_candidate': agent.getVariableUInt('repair_candidate'),
                'repair_line_id': agent.getVariableUInt('repair_line_id'),
                'repair_line_day': agent.getVariableUInt('repair_line_day'),
                'debug_promoted': agent.getVariableUInt('debug_promoted'),
                'debug_needs_demote': agent.getVariableUInt('debug_needs_demote'),
                'debug_repair_candidate': agent.getVariableUInt('debug_repair_candidate'),
                'debug_repair_line_id': agent.getVariableUInt('debug_repair_line_id'),
                'debug_repair_line_day': agent.getVariableUInt('debug_repair_line_day'),
                'debug_bucket_seen': agent.getVariableUInt('debug_bucket_seen'),
                'commit_p1': agent.getVariableUInt('commit_p1'),
                'commit_p2': agent.getVariableUInt('commit_p2'),
                'commit_p3': agent.getVariableUInt('commit_p3'),
                'decision_p2': agent.getVariableUInt('decision_p2'),
                'decision_p3': agent.getVariableUInt('decision_p3'),
                'spawn_debug_curr_ops': spawn_debug_curr_ops,
                'spawn_debug_target': spawn_debug_target,
                'spawn_debug_need': spawn_debug_need,
                'spawn_debug_curr_ops_mi8': spawn_debug_curr_ops_mi8,
                'spawn_debug_target_mi8': spawn_debug_target_mi8,
                'spawn_debug_need_mi8': spawn_debug_need_mi8,
                'debug_current_day': env_current_day,
                'debug_rl_total': rl_total,
                'debug_rl_free': rl_free,
                'debug_rl_ready': rl_ready,
                'debug_rl_min_free': rl_min_free,
                'debug_rl_max_free': rl_max_free,
                'debug_ops_mi17': 0,
                'debug_svc_mi17': 0,
                'debug_unsvc_ready_mi17': 0,
                'debug_inactive_ready_mi17': 0,
            })
    
    # Статусные счётчики для Mi-17 (временное логирование)
    mi17_ops = 0
    mi17_svc = 0
    mi17_unsvc_ready = 0
    mi17_inactive_ready = 0
    for row in rows:
        if row['group_by'] != 2:
            continue
        state_name = row['state']
        repair_days = row['repair_days']
        if state_name == "operations":
            mi17_ops += 1
        elif state_name == "serviceable":
            mi17_svc += 1
        elif state_name == "unserviceable":
            if repair_days == 0 and (repair_time == 0 or current_day >= repair_time):
                mi17_unsvc_ready += 1
        elif state_name == "inactive":
            if repair_days == 0 and (repair_time == 0 or current_day >= repair_time):
                mi17_inactive_ready += 1
    
    for row in rows:
        row['debug_ops_mi17'] = mi17_ops
        row['debug_svc_mi17'] = mi17_svc
        row['debug_unsvc_ready_mi17'] = mi17_unsvc_ready
        row['debug_inactive_ready_mi17'] = mi17_inactive_ready
    return rows


def collect_repair_lines_state(simulation, repair_line_desc, day_u16, version_date_int, version_id,
                               step_id=0, prev_day=0, adaptive_days=0):
    """Собирает состояние RepairLine агентов (временное логирование)."""
    if repair_line_desc is None:
        return []
    
    def _get_env_u32(name: str, default: int = 0) -> int:
        if hasattr(simulation, "getEnvironmentPropertyUInt"):
            return int(simulation.getEnvironmentPropertyUInt(name))
        if hasattr(simulation, "getEnvironmentProperty"):
            return int(simulation.getEnvironmentProperty(name))
        return default
    
    env_current_day = _get_env_u32("current_day", day_u16)
    mi8_rt = _get_env_u32("mi8_repair_time_const", 0)
    mi17_rt = _get_env_u32("mi17_repair_time_const", 0)
    
    pop_rl = fg.AgentVector(repair_line_desc)
    simulation.getPopulationData(pop_rl, "default")
    rows = []
    for i in range(pop_rl.size()):
        line = pop_rl.at(i)
        line_id = line.getVariableUInt("line_id")
        free_days = line.getVariableUInt("free_days")
        acn = line.getVariableUInt("aircraft_number")
        is_free = 1 if acn == 0 else 0
        ready_mi8 = 1 if (acn == 0 and mi8_rt > 0 and free_days >= mi8_rt) else 0
        ready_mi17 = 1 if (acn == 0 and mi17_rt > 0 and free_days >= mi17_rt) else 0
        rows.append({
            'version_date': version_date_int,
            'version_id': version_id,
            'day_u16': day_u16,
            'debug_step': step_id,
            'debug_prev_day': prev_day,
            'debug_adaptive_days': adaptive_days,
            'debug_current_day': env_current_day,
            'line_id': line_id,
            'free_days': free_days,
            'aircraft_number': acn,
            'last_acn': line.getVariableUInt("last_acn"),
            'last_day': line.getVariableUInt("last_day"),
            'is_free': is_free,
            'ready_mi8': ready_mi8,
            'ready_mi17': ready_mi17,
        })
    return rows


def collect_quota_manager_state(simulation, quota_desc, day_u16, version_date_int, version_id,
                                step_id=0, prev_day=0, adaptive_days=0):
    """Собирает debug-состояние QuotaManager (временное логирование слотов)."""
    def _get_env_u32(name: str, default: int = 0) -> int:
        if hasattr(simulation, "getEnvironmentPropertyUInt"):
            return int(simulation.getEnvironmentPropertyUInt(name))
        if hasattr(simulation, "getEnvironmentProperty"):
            return int(simulation.getEnvironmentProperty(name))
        return default
    
    env_current_day = _get_env_u32("current_day", day_u16)
    
    pop = fg.AgentVector(quota_desc)
    simulation.getPopulationData(pop)
    rows = []
    for agent in pop:
        rows.append({
            'version_date': version_date_int,
            'version_id': version_id,
            'day_u16': day_u16,
            'debug_step': step_id,
            'debug_prev_day': prev_day,
            'debug_adaptive_days': adaptive_days,
            'debug_current_day': env_current_day,
            'group_by': int(agent.getVariableUInt8("group_by")),
            'debug_slots_count_mi17': int(agent.getVariableUInt("debug_slots_count_mi17")),
            'debug_slot_mi17_0': int(agent.getVariableUInt("debug_slot_mi17_0")),
            'debug_slot_mi17_1': int(agent.getVariableUInt("debug_slot_mi17_1")),
            'debug_slot_mi17_2': int(agent.getVariableUInt("debug_slot_mi17_2")),
            'debug_slot_mi17_3': int(agent.getVariableUInt("debug_slot_mi17_3")),
            'debug_slot_mi17_4': int(agent.getVariableUInt("debug_slot_mi17_4")),
            'debug_slot_mi17_5': int(agent.getVariableUInt("debug_slot_mi17_5")),
            'debug_p2_ops': int(agent.getVariableUInt("debug_p2_ops")),
            'debug_p2_target': int(agent.getVariableUInt("debug_p2_target")),
            'debug_p2_deficit': int(agent.getVariableUInt("debug_p2_deficit")),
            'debug_p2_needed': int(agent.getVariableUInt("debug_p2_needed")),
            'debug_p2_slots': int(agent.getVariableUInt("debug_p2_slots")),
            'debug_p2_svc': int(agent.getVariableUInt("debug_p2_svc")),
            'debug_p2_unsvc': int(agent.getVariableUInt("debug_p2_unsvc")),
            'debug_qm_ops_mi8': int(agent.getVariableUInt("debug_qm_ops_mi8")),
            'debug_qm_ops_mi17': int(agent.getVariableUInt("debug_qm_ops_mi17")),
            'debug_qm_target_mi8': int(agent.getVariableUInt("debug_qm_target_mi8")),
            'debug_qm_target_mi17': int(agent.getVariableUInt("debug_qm_target_mi17")),
            'debug_qm_quota_left_mi8': int(agent.getVariableUInt("debug_qm_quota_left_mi8")),
            'debug_qm_quota_left_mi17': int(agent.getVariableUInt("debug_qm_quota_left_mi17")),
            'debug_qm_unsvc_cnt': int(agent.getVariableUInt("debug_qm_unsvc_cnt")),
            'debug_qm_inactive_cnt': int(agent.getVariableUInt("debug_qm_inactive_cnt")),
            'debug_qm_p1_mi8': int(agent.getVariableUInt("debug_qm_p1_mi8")),
            'debug_qm_p1_mi17': int(agent.getVariableUInt("debug_qm_p1_mi17")),
            'debug_qm_p2_total': int(agent.getVariableUInt("debug_qm_p2_total")),
            'debug_qm_p3_total': int(agent.getVariableUInt("debug_qm_p3_total")),
            'debug_qm_balance_mi8': int(agent.getVariableInt("debug_qm_balance_mi8")),
            'debug_qm_balance_mi17': int(agent.getVariableInt("debug_qm_balance_mi17")),
            'debug_qm_target_day': int(agent.getVariableUInt("debug_qm_target_day")),
            'debug_qm_ops_cnt_mi8': int(agent.getVariableUInt("debug_qm_ops_cnt_mi8")),
            'debug_qm_ops_cnt_mi17': int(agent.getVariableUInt("debug_qm_ops_cnt_mi17")),
            'debug_qm_svc_cnt_mi8': int(agent.getVariableUInt("debug_qm_svc_cnt_mi8")),
            'debug_qm_svc_cnt_mi17': int(agent.getVariableUInt("debug_qm_svc_cnt_mi17")),
            'debug_qm_unsvc_ready_mi8': int(agent.getVariableUInt("debug_qm_unsvc_ready_mi8")),
            'debug_qm_unsvc_ready_mi17': int(agent.getVariableUInt("debug_qm_unsvc_ready_mi17")),
            'debug_qm_inactive_mi8': int(agent.getVariableUInt("debug_qm_inactive_mi8")),
            'debug_qm_inactive_mi17': int(agent.getVariableUInt("debug_qm_inactive_mi17")),
        })
    return rows


from sim_env_setup import get_client, prepare_env_arrays
from base_model_messaging import V2BaseModelMessaging
from precompute_events import (
    INFLATION_LOG_SCALE,
    compute_inflation_log_cumsum,
    compute_mp5_cumsum,
    find_program_change_days,
)
from datetime import date

# V8 модули
import rtc_state_transitions_v7  # Детерминированные переходы (repair→svc, spawn→ops)
import rtc_state_transitions_v8  # V8: next-day dt проверка!
import rtc_quota_v8              # V8: квотирование через RepairLine
import rtc_repair_lines_v8       # V8: RepairLine sync
import rtc_limiter_optimized
import rtc_limiter_v8            # V8: deterministic_dates!
import rtc_mp2_export
import rtc_repairline_export
import sim_daily_materializer
from components.agent_population import AgentPopulationBuilder
from model_build import REPAIR_LINES_MAX

# Максимум bank-окон на линию (должно совпадать с rtc_quota_v8.py)
REPAIR_BANK_MAX = 64


class LimiterV8Orchestrator:
    """
    Оркестратор LIMITER V8 — архитектура с RepairLine
    
    Ключевые отличия от V7:
    - RepairLine вместо RepairAgent.capacity для квотирования
    - next-day dt проверка ресурсов
    - limiter=0 = обязательный выход
    """
    
    def __init__(self, version_date: str, end_day: int = 3650,
                 enable_mp2: bool = False, clickhouse_client=None):
        self.version_date = version_date
        self.end_day = end_day
        self.enable_mp2 = enable_mp2
        self.clickhouse_client = clickhouse_client
        
        self.model = None
        self.simulation = None
        self.base_model = None
        self.env_data = None
        
        # DEBUG: дни для детального лога состояния (локальная отладка)
        self.debug_days = {830}
        
        self.frames = 0
        self.days = 0
        self.mp5_cumsum = None
        self.inflation_log_cumsum = None
        self.program_change_days = []
        
        # V8: детерминированные даты (один массив)
        self.deterministic_dates = []
        
    def prepare_data(self):
        """Подготовка данных"""
        print("\n" + "=" * 60)
        print(f"📊 LIMITER V8: Подготовка данных")
        print(f"   version_date: {self.version_date}")
        print(f"   end_day: {self.end_day}")
        print("=" * 60)
        
        client = get_client()
        self._client = client
        vd = date.fromisoformat(self.version_date)
        self.env_data = prepare_env_arrays(client, vd)
        self.frames = int(self.env_data['frames_total_u16'])
        self.days = min(int(self.env_data['days_total_u16']), self.end_day + 1)
        
        print(f"\n✅ Данные: frames={self.frames}, days={self.days}")
        
        # Подсчёт детерминированного spawn для смещения динамического
        spawn_seed = self.env_data.get('mp4_new_counter_mi17_seed', [])
        self._deterministic_spawn_count = sum(c for c in spawn_seed if c > 0)
        
        # MP5 cumsum
        print("\n📊 Вычисление mp5_cumsum...")
        t0 = time.perf_counter()
        import numpy as np
        mp5_lin = np.array(self.env_data.get('mp5_daily_hours_linear', []), dtype=np.uint32)
        self.mp5_cumsum = compute_mp5_cumsum(mp5_lin, self.frames, self.days)
        print(f"   mp5_cumsum: shape={self.mp5_cumsum.shape}, time={time.perf_counter()-t0:.2f}s")

        print("\n📊 Вычисление inflation_log_cumsum...")
        t0 = time.perf_counter()
        self.inflation_log_cumsum = compute_inflation_log_cumsum(vd, model_build.MAX_DAYS)
        print(
            "   inflation_log_cumsum: "
            f"len={len(self.inflation_log_cumsum)}, scale={int(INFLATION_LOG_SCALE)}, "
            f"time={time.perf_counter()-t0:.2f}s"
        )
        
        # Program changes
        print("\n📊 Поиск дней изменения программы...")
        mp4_mi8 = self.env_data.get('mp4_ops_counter_mi8', [])
        mp4_mi17 = self.env_data.get('mp4_ops_counter_mi17', [])
        program_changes = find_program_change_days(mp4_mi8, mp4_mi17)
        # Извлек только дни из tuples (day, target_mi8, target_mi17)
        self.program_change_days = [pc[0] for pc in program_changes if pc[0] <= self.end_day]
        
        # Добавляем end_day
        if self.end_day not in self.program_change_days:
            self.program_change_days.append(self.end_day)
        self.program_change_days = sorted(set(self.program_change_days))
        
        print(f"   program_changes: {len(self.program_change_days)} дней")
        
        # V8: Собираем ВСЕ детерминированные даты в один массив
        self._collect_deterministic_dates()
        
        # Population builder
        self.population_builder = AgentPopulationBuilder(
            self.env_data,
            mp5_cumsum=self.mp5_cumsum,
            end_day=self.end_day
        )
        
    def _collect_deterministic_dates(self):
        """V8: Собирает все детерминированные даты в один массив"""
        dates = set()
        
        # День 0
        dates.add(0)
        dates.add(1)  # День 1: первый обсчитываемый день (день 0 — инициализация)
        
        # Program changes
        dates.update(self.program_change_days)
        
        # end_day
        dates.add(self.end_day)
        
        # День repair_time (обязательный шаг)
        mi8_rt = int(self.env_data.get('mi8_repair_time_const', 0))
        mi17_rt = int(self.env_data.get('mi17_repair_time_const', 0))
        if mi8_rt > 0 and mi8_rt <= self.end_day:
            dates.add(mi8_rt)
        if mi17_rt > 0 and mi17_rt <= self.end_day:
            dates.add(mi17_rt)
        
        # Repair exits (repair_time - repair_days для агентов в repair)
        # Будет добавлено при populate_agents, пока placeholder
        
        # Spawn dates (детерминированный spawn)
        spawn_seed = self.env_data.get('mp4_new_counter_mi17_seed', [])
        for day, count in enumerate(spawn_seed):
            if count > 0 and day <= self.end_day:
                dates.add(day)
        
        self.deterministic_dates = sorted(dates)
        print(f"   V8 deterministic_dates: {len(self.deterministic_dates)} дат")
    
    def _compute_repair_quota(self) -> int:
        """Определяет квоту ремонтных линий из mp1_repair_number (без хардкода)."""
        repair_numbers = self.env_data.get('mp1_repair_number', [])
        mp1_index = self.env_data.get('mp1_index', {})
        mp3 = self.env_data.get('mp3_arrays', {})
        pseq_list = mp3.get('mp3_partseqno_i', [])
        gb_list = mp3.get('mp3_group_by', [])
        
        valid = []
        for i, partseqno in enumerate(pseq_list):
            gb = int(gb_list[i] or 0) if i < len(gb_list) else 0
            if gb not in (1, 2):
                continue
            pidx = mp1_index.get(int(partseqno or 0), -1)
            if pidx < 0 or pidx >= len(repair_numbers):
                continue
            rn = int(repair_numbers[pidx])
            if rn not in (0, 255):
                valid.append(rn)
        
        if not valid:
            raise RuntimeError("Не удалось определить repair_quota по планерам из mp1_repair_number")
        
        return max(valid)
        
    def build_model(self):
        """Построение модели V8"""
        print("\n" + "=" * 60)
        print("🔧 LIMITER V8: Построение модели")
        print("=" * 60)
        
        # Base model
        self.base_model = V2BaseModelMessaging()
        self.model = self.base_model.create_model(self.env_data)
        
        # Repair lines quota (без хардкода 18)
        self.repair_quota = self._compute_repair_quota()
        if self.repair_quota > REPAIR_LINES_MAX:
            raise RuntimeError(f"repair_quota={self.repair_quota} > REPAIR_LINES_MAX={REPAIR_LINES_MAX}")
        self.base_model.env.newPropertyUInt("repair_quota", self.repair_quota)
        self.base_model.env.newMacroPropertyUInt("repair_line_free_days_mp", REPAIR_LINES_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_acn_mp", REPAIR_LINES_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_gb_mp", REPAIR_LINES_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_rt_mp", REPAIR_LINES_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_last_acn_mp", REPAIR_LINES_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_last_day_mp", REPAIR_LINES_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_bank_count_mp", REPAIR_LINES_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_bank_lock_mp", REPAIR_LINES_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_bank_start_mp", REPAIR_LINES_MAX * REPAIR_BANK_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_bank_end_mp", REPAIR_LINES_MAX * REPAIR_BANK_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_free_days_ro_mp", REPAIR_LINES_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_acn_ro_mp", REPAIR_LINES_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_bank_count_ro_mp", REPAIR_LINES_MAX)
        self.base_model.env.newMacroPropertyUInt("repair_line_bank_head_end_ro_mp", REPAIR_LINES_MAX)
        
        # Environment properties
        self.base_model.env.newPropertyUInt("end_day", self.end_day)
        self.base_model.env.newPropertyUInt("prev_day", 0)
        self.base_model.env.newPropertyUInt("adaptive_days", 1)
        
        heli_agent = self.base_model.agent
        
        # ═══════════════════════════════════════════════════════════════
        # mp5_cumsum MacroProperty
        # ═══════════════════════════════════════════════════════════════
        cumsum_size = model_build.RTC_MAX_FRAMES * (model_build.MAX_DAYS + 1)
        self.base_model.env.newMacroPropertyUInt32("mp5_cumsum", cumsum_size)
        self.base_model.env.newMacroPropertyUInt32(
            "inflation_log_cumsum", model_build.MAX_DAYS + 1
        )
        # mp4_ops_counter_mi8/mi17 уже созданы в base_model как PropertyArray
        
        # MP2 Export: буферы для per-agent per-step экспорта
        rtc_mp2_export.setup_mp2_buffers(self.base_model.env)
        
        # RepairLine Export: буферы для per-line per-step экспорта
        rtc_repair_lines_v8.setup_rl_export_buffers(self.base_model.env)
        
        # HF для инициализации mp5_cumsum
        hf_init_cumsum = HF_InitMP5Cumsum(self.mp5_cumsum, self.frames, self.days)
        layer_init = self.model.newLayer("layer_init_mp5_cumsum")
        layer_init.addHostFunction(hf_init_cumsum)

        hf_init_inflation = HF_InitInflationLogCumsum(self.inflation_log_cumsum)
        layer_init_inflation = self.model.newLayer("layer_init_inflation_log_cumsum")
        layer_init_inflation.addHostFunction(hf_init_inflation)
        
        # HF для инициализации repair_line_*_mp
        # day0_map заполняется позже в _init_repair_lines_at_build
        mi8_rt = int(self.env_data.get('mi8_repair_time_const', 180))
        mi17_rt = int(self.env_data.get('mi17_repair_time_const', 180))
        self._hf_init_lines = HF_InitRepairLines(
            self.repair_quota, day0_map={}, mi8_rt=mi8_rt, mi17_rt=mi17_rt
        )
        layer_lines = self.model.newLayer("layer_init_repair_lines")
        layer_lines.addHostFunction(self._hf_init_lines)

        # V8: HF_InitV8 как layer (addInitFunction не вызывается в step() режиме)
        self._hf_init_v8 = rtc_limiter_v8.HF_InitV8(self.deterministic_dates, self.end_day)
        layer_init_v8 = self.model.newLayer("layer_init_v8")
        layer_init_v8.addHostFunction(self._hf_init_v8)
        print("  ✅ V8 HF_InitV8 зарегистрирован (layer host function)")

        # V8: StepController как layer ПЕРЕД обработкой (QM видит новый current_day)
        # Читает mp_min_limiter из ПРЕДЫДУЩЕГО шага, продвигает current_day
        self.hf_step_controller = rtc_limiter_optimized.HF_StepController()
        layer_step = self.model.newLayer("layer_step_controller")
        layer_step.addHostFunction(self.hf_step_controller)
        print("  ✅ V8 HF_StepController зарегистрирован (layer, перед QM)")

        # Pre-status snapshot (перед любыми переходами)
        rtc_state_transitions_v8.register_save_pre_status(self.model, heli_agent)
        
        # Детерминированный spawn (HostFunction, mid-simulation)
        spawn_seed = self.env_data.get('mp4_new_counter_mi17_seed', [])
        spawn_schedule = [
            (day_idx, count)
            for day_idx, count in enumerate(spawn_seed)
            if count > 0
        ]
        if spawn_schedule:
            det_spawn = getattr(self, '_deterministic_spawn_count', 0)
            env_consts = {
                'll': int(self.env_data.get('mi17_ll_const', 270000)),
                'oh': int(self.env_data.get('mi17_oh_const', 270000)),
                'br': int(self.env_data.get('mi17_br_const', 210000)),
                'repair_time': int(self.env_data.get('mi17_repair_time_const', 180)),
                'assembly_time': int(self.env_data.get('mi17_assembly_time_const', 30)),
                'partout_time': int(self.env_data.get('mi17_partout_time_const', 20)),
                'second_ll_sentinel': int(self.env_data.get('second_ll_sentinel', 0xFFFFFFFF)),
            }
            base_idx = int(self.env_data.get('first_reserved_idx', self.frames))
            if base_idx < 0:
                base_idx = self.frames
            self._hf_det_spawn = HF_DeterministicSpawn(
                spawn_schedule, base_idx, 100000, env_consts
            )
            layer_det_spawn = self.model.newLayer("layer_det_spawn")
            layer_det_spawn.addHostFunction(self._hf_det_spawn)
            print(f"  ✅ Детерм. spawn: {det_spawn} агентов, schedule={spawn_schedule}")
        
        # ═══════════════════════════════════════════════════════════════
        # V8: RepairAgent ОТКЛЮЧЁН — используем V8 квотирование через RepairLine
        # ═══════════════════════════════════════════════════════════════
        # repair_quota = int(self.env_data.get('mi17_repair_quota', 8))
        # repair_time = int(self.env_data.get('mi17_repair_time_const', 180))
        self.repair_agent = None  # Отключено для baseline совместимости
        
        # count_repair: подсчитывается динамически или через MacroProperty
        # Начальное значение 0, будет обновлено RTC функцией подсчёта
        self.base_model.env.newPropertyUInt("count_repair", 0)
        
        # ═══════════════════════════════════════════════════════════════
        # V8: Переходы состояний с next-day dt проверкой
        # ═══════════════════════════════════════════════════════════════
        
        # Фаза 0: Детерминированные переходы (repair→svc, spawn→ops) — из V7
        # REMOVED (W_sim_remove_d2_noop_20260601T200344Z):
        # v8_repair_line_assign_repair был no-op: обе ветки возвращали ALIVE.
        rtc_state_transitions_v7.register_phase0_deterministic(self.model, heli_agent)
        
        # Фаза 0.5: Копирование exit_date (repair, spawn, unsvc) — V8 compute_global_min игнорирует unsvc
        # REMOVED (remove-phase05): Фаза 0.5 — мёртвый код в V8.
        # register_exit_date_copy(self.model, heli_agent, self.base_model.quota_agent)
        
        # Фаза 1: V8 Operations (next-day dt проверка!)
        rtc_state_transitions_v8.register_ops_transitions_v8(self.model, heli_agent)
        
        # RepairLine: increment -> publish (до квотирования)
        rtc_repair_lines_v8.register_repair_line_pre_quota_layers(
            self.model, self.base_model.repair_line_agent
        )
        
        # Фаза 2: V8 Квотирование с RepairLine
        rtc_quota_v8.setup_quota_v8_macroproperties(self.base_model.env)
        rtc_quota_v8.register_quota_v8_messages(self.model, heli_agent, self.base_model.quota_agent)
        
        # Фаза 3: Переходы после квотирования
        rtc_state_transitions_v7.register_post_quota_v7(self.model, heli_agent)
        
        # RepairLine Export: запись состояния в буферы каждый шаг
        rtc_repair_lines_v8.register_repair_line_export_layer(
            self.model, self.base_model.repair_line_agent
        )
        
        # Синхронизация RepairLine после квотирования (deprecated)
        # rtc_repair_lines_v8.register_repair_line_sync_post_quota(
        #     self.model, self.base_model.repair_line_agent
        # )
        
        # Пересчёт буферов ПОСЛЕ пост-квотных переходов (нужно для корректного спавна)
        rtc_quota_v8.register_post_quota_counts_v8(self.model, heli_agent)
        
        # ═══════════════════════════════════════════════════════════════
        # Динамический спавн (после P3): anti-starvation reserve для Mi-8
        # ═══════════════════════════════════════════════════════════════
        det_spawn = getattr(self, '_deterministic_spawn_count', 0)
        env_first_dynamic_idx = self.env_data.get('first_dynamic_idx')
        env_dynamic_reserve_mi17 = self.env_data.get('dynamic_reserve_mi17')
        use_env_spawn = False
        first_dynamic_idx = None
        total_dynamic_slots = None
        if env_first_dynamic_idx is not None and env_dynamic_reserve_mi17 is not None:
            try:
                first_dynamic_idx = int(env_first_dynamic_idx)
                env_dynamic_reserve_mi17 = int(env_dynamic_reserve_mi17)
                if 0 <= first_dynamic_idx <= self.frames and env_dynamic_reserve_mi17 >= 0:
                    total_dynamic_slots = max(0, self.frames - first_dynamic_idx)
                    dynamic_reserve_mi17 = min(env_dynamic_reserve_mi17, total_dynamic_slots)
                    remaining_slots = max(0, total_dynamic_slots - dynamic_reserve_mi17)
                    dynamic_reserve_mi8 = min(8, remaining_slots)
                    use_env_spawn = True
            except (TypeError, ValueError):
                use_env_spawn = False
        if not use_env_spawn:
            total_dynamic_slots = max(
                0,
                model_build.RTC_MAX_FRAMES - self.frames - det_spawn
            )
            dynamic_reserve_mi8 = 8 if total_dynamic_slots >= 8 else total_dynamic_slots
            dynamic_reserve_mi17 = total_dynamic_slots - dynamic_reserve_mi8
            if dynamic_reserve_mi17 > 50:
                dynamic_reserve_mi17 = 50
            first_dynamic_idx = self.frames + det_spawn
        self._dynamic_reserve_mi17 = dynamic_reserve_mi17
        self._dynamic_reserve_mi8 = dynamic_reserve_mi8
        base_acn_spawn = 100000 + det_spawn
        spawn_env_data = {
            'first_dynamic_idx': first_dynamic_idx,
            'first_dynamic_idx_mi17': first_dynamic_idx,
            'first_dynamic_idx_mi8': first_dynamic_idx + dynamic_reserve_mi17,
            'dynamic_reserve_mi17': dynamic_reserve_mi17,
            'dynamic_reserve_mi8': dynamic_reserve_mi8,
            'base_acn_spawn_mi17': base_acn_spawn,
            'base_acn_spawn_mi8': base_acn_spawn + dynamic_reserve_mi17
        }
        self.spawn_data = rtc_spawn_dynamic_v7.register_spawn_dynamic_v8(
            self.model, heli_agent, spawn_env_data
        )

        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 4: Limiter (бинарный поиск)
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение limiter...")
        rtc_limiter_optimized.setup_limiter_macroproperties(
            self.base_model.env,
            self.program_change_days
        )
        # REMOVED (inline-limiter-entry): бинарный поиск встроен в X→ops функции.
        # rtc_limiter_optimized.register_limiter_optimized(
        #     self.model,
        #     heli_agent,
        #     skip_decrement=True
        # )
        fn_min = heli_agent.newRTCFunction(
            "rtc_compute_min_limiter",
            rtc_limiter_optimized.RTC_COMPUTE_ADAPTIVE_DAYS
        )
        fn_min.setInitialState("operations")
        fn_min.setEndState("operations")
        layer_min = self.model.newLayer("L_limiter_min")
        layer_min.addAgentFunction(fn_min)
        
        # ═══════════════════════════════════════════════════════════════
        # MP2 Export: agent write layer + drain layer
        # ═══════════════════════════════════════════════════════════════
        rtc_mp2_export.register_mp2_write_layer(self.model, heli_agent)
        # Включаем spawn слоты в MP2 drain
        total_agents_with_spawn = (
            self.frames if use_env_spawn
            else self.frames + det_spawn + dynamic_reserve_mi17 + dynamic_reserve_mi8
        )
        self._total_agents_with_spawn = total_agents_with_spawn
        self.hf_mp2_drain = rtc_mp2_export.register_mp2_drain(
            self.model, self.end_day, total_agents_with_spawn
        )
        
        # RepairLine Drain: чтение буферов на финальном шаге
        self.hf_rl_drain = rtc_repairline_export.register_repairline_drain(
            self.model, self.end_day, self.repair_quota
        )
        
        # V8 StepController перенесён в layer_step_controller (перед QM)
        # mp_min_limiter записывается здесь limiter, читается StepController на СЛЕДУЮЩЕМ шаге
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 5: V8 adaptive с deterministic_dates
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение V8 adaptive (deterministic_dates)...")
        
        # V8 MacroProperty (включает current_day_mp, adaptive_result_mp и др.)
        # Заполнение значений выполняется после populate_agents (move-hf-init-v8).
        rtc_limiter_v8.setup_v8_macroproperties(self.base_model.env, self.deterministic_dates)
        
        self.base_model.quota_agent.newVariableUInt("computed_adaptive_days", 1)
        self.base_model.quota_agent.newVariableUInt("current_day_cache", 0)
        
        # V8 StepController перенесён в начало (layer_step_controller, перед QM)
        
        # V8 Exit condition
        self.hf_exit = rtc_limiter_v8.HF_ExitConditionV8(self.end_day)
        self.model.addExitCondition(self.hf_exit)
        
        print("\n✅ Модель LIMITER V8 построена")
        print(f"   deterministic_dates: {len(self.deterministic_dates)} дат")
        print("=" * 60)
        
        return self.model
    
    def run(self, max_steps: int = 10000):
        """Запуск симуляции (V9: simulate() + MP2 drain)"""
        print("\n" + "=" * 60)
        print("🚀 LIMITER V8/V9: Запуск симуляции (simulate + MP2)")
        print(f"   max_steps: {max_steps}")
        print(f"   end_day: {self.end_day}")
        print("=" * 60)
        
        t_start = time.perf_counter()
        
        # Создание симуляции
        self.simulation = fg.CUDASimulation(self.model)
        self.simulation.SimulationConfig().steps = max_steps
        
        # Инициализация агентов
        self._populate_agents()
        
        # Запуск simulate() — все шаги выполняются на GPU без возврата в Python
        print("\n🏃 Запуск simulate()...")
        t_gpu_start = time.perf_counter()
        self.simulation.simulate()
        gpu_time = time.perf_counter() - t_gpu_start
        
        final_steps = self.simulation.getStepCounter()
        print(f"   ✅ simulate() завершён: {final_steps} шагов за {gpu_time:.2f}с")
        
        # Чтение MP2 данных
        vd = date.fromisoformat(self.version_date)
        version_date_int = vd.year * 10000 + vd.month * 100 + vd.day
        run_id_env = os.getenv("V8_RUN_ID")
        if run_id_env is not None and run_id_env.isdigit():
            version_id = int(run_id_env)
        else:
            version_id = int(self.env_data.get('version_id_u32', 1))
        
        mp2_data = self.hf_mp2_drain.data
        if mp2_data is None:
            print("⚠️ MP2 Drain не прочитал данные (возможно end_day не достигнут)")
            t_end = time.perf_counter()
            print(f"\n✅ Симуляция завершена (без экспорта): {t_end - t_start:.2f}с")
            self._print_final_stats()
            return self.end_day
        
        num_steps = mp2_data['num_steps']
        days = mp2_data['days']
        fields = mp2_data['fields']
        
        dyn17 = getattr(self, '_dynamic_reserve_mi17', 0)
        dyn8 = getattr(self, '_dynamic_reserve_mi8', 0)
        det_spawn = getattr(self, '_deterministic_spawn_count', 0)
        total_export_agents = getattr(
            self,
            '_total_agents_with_spawn',
            self.frames + det_spawn + dyn17 + dyn8
        )
        
        # Все поля (включая "статические") читаются из MP2 буферов
        # Fallback не используется
        
        print(
            f"\n📤 MP2 → ClickHouse: {num_steps} шагов × {total_export_agents} агентов "
            f"(base={self.frames} + spawn={total_export_agents - self.frames})"
        )
        
        # [2026-02-17] Постпроцессинг P2/P3 включён (Вариант B).
        # Ремонты также отслеживаются в sim_repairline_v9.
        # Вариант A ломал INV-10 из-за отсутствия обновления pre_status_id.
        # Вариант B: корректное обновление pre_status_id + status_id.
        t_pp = time.perf_counter()
        pp_count = self._postprocess_promotions(fields, days, num_steps, total_export_agents)
        pp_time = time.perf_counter() - t_pp
        if pp_count > 0:
            print(f"   📦 Постпроцессинг: {pp_count} записей модифицировано ({pp_time:.2f}с)")
        
        # Построение колонок для INSERT
        t_build = time.perf_counter()
        columns = [
            'version_date', 'version_id', 'day_u16',
            'idx', 'aircraft_number', 'group_by', 'oh', 'br', 'll',
            'status_id', 'pre_status_id', 'status_change_day', 'sne', 'ppr', 'limiter', 'repair_days',
            'repair_claim_start_day', 'repair_claim_end_day', 'repair_claim_source',
            'repair_claim_line_id',
            'repair_time', 'assembly_time', 'active_trigger', 'assembly_trigger',
            'daily_today_u32', 'daily_next_u32', 'commit_p2', 'commit_p3'
        ]
        columns_data = [[] for _ in columns]
        master_projection = []
        def _u16(val) -> int:
            return int(val) & 0xFFFF
        def _u8(val) -> int:
            return int(val) & 0xFF
        for s in range(num_steps):
            day = days[s]
            for a in range(total_export_agents):
                status = int(fields['mp2_status_id'][s, a])
                if status == 0:
                    continue  # Пустой spawn slot — пропускаем
                pre_status = int(fields['mp2_pre_status_id'][s, a])
                commit_p2 = int(fields['mp2_commit_p2'][s, a])
                commit_p3 = int(fields['mp2_commit_p3'][s, a])
                claim_start = int(fields['mp2_repair_claim_start_day'][s, a])
                claim_end = int(fields['mp2_repair_claim_end_day'][s, a])
                claim_source = int(fields['mp2_repair_claim_source'][s, a])
                claim_line_id = int(fields['mp2_repair_claim_line_id'][s, a])
                if pre_status == 0 and status in (2, 3):
                    commit_p2 = 0
                    commit_p3 = 0
                    claim_source = 0
                    claim_start = 0xFFFF
                    claim_end = 0xFFFF
                    claim_line_id = 0xFFFF
                day_u16 = _u16(day)
                aircraft_number = int(fields['mp2_aircraft_number'][a])
                group_by = _u8(fields['mp2_group_by'][a])
                status_id = _u8(status)
                pre_status_id = _u8(pre_status)
                claim_start_u16 = _u16(claim_start)
                claim_end_u16 = _u16(claim_end)
                claim_source_u8 = _u8(claim_source)
                claim_line_id_u16 = _u16(claim_line_id)
                row_values = (
                    version_date_int,
                    version_id,
                    day_u16,
                    _u16(fields['mp2_idx'][a]),
                    aircraft_number,
                    group_by,
                    int(fields['mp2_oh'][a]),
                    int(fields['mp2_br'][a]),
                    int(fields['mp2_ll'][a]),
                    status_id,
                    pre_status_id,
                    _u16(fields['mp2_status_change_day'][s, a]),
                    int(fields['mp2_sne'][s, a]),
                    int(fields['mp2_ppr'][s, a]),
                    _u16(fields['mp2_limiter'][s, a]),
                    _u16(fields['mp2_repair_days'][s, a]),
                    claim_start_u16,
                    claim_end_u16,
                    claim_source_u8,
                    claim_line_id_u16,
                    _u16(fields['mp2_repair_time'][s, a]),
                    _u16(fields['mp2_assembly_time'][s, a]),
                    _u8(fields['mp2_active_trigger'][s, a]),
                    _u8(fields['mp2_assembly_trigger'][s, a]),
                    int(fields['mp2_daily_today'][s, a]),
                    int(fields['mp2_daily_next'][s, a]),
                    commit_p2,
                    commit_p3,
                )
                for column_data, value in zip(columns_data, row_values):
                    column_data.append(value)
                master_projection.append((
                    aircraft_number,
                    group_by,
                    day_u16,
                    status_id,
                    pre_status_id,
                    int(commit_p2),
                    int(commit_p3),
                    claim_line_id_u16,
                    claim_start_u16,
                    claim_end_u16,
                    claim_source_u8,
                ))
        build_time = time.perf_counter() - t_build
        row_count = len(columns_data[0])
        print(f"   Строк: {row_count} ({build_time:.2f}с)")
        
        # Batch INSERT
        if self.clickhouse_client and row_count:
            t_insert = time.perf_counter()
            col_str = ', '.join(columns)
            self.clickhouse_client.execute(
                f"INSERT INTO sim_masterv2_v9 ({col_str}) VALUES",
                columns_data,
                columnar=True,
                settings={'max_partitions_per_insert_block': 300}
            )
            insert_time = time.perf_counter() - t_insert
            print(f"   ✅ INSERT: {row_count} строк ({insert_time:.2f}с)")
        
        # ═══════════════════════════════════════════════════════════════
        # RepairLine Export → ClickHouse
        # ═══════════════════════════════════════════════════════════════
        rl_data = self.hf_rl_drain.data
        if rl_data is not None and self.clickhouse_client:
            t_rl = time.perf_counter()
            rl_rows = rtc_repairline_export.interpolate_repairline_daily(
                rl_data, self.repair_quota
            )
            rl_interp_time = time.perf_counter() - t_rl
            print(f"\n📤 RepairLine → ClickHouse: {len(rl_rows)} строк (интерполяция {rl_interp_time:.2f}с)")
            
            rtc_repairline_export.export_repairline_to_ch(
                self.clickhouse_client, rl_rows,
                version_date_int, version_id,
                master_projection=master_projection
            )
            daily_rows = sim_daily_materializer.materialize_daily(
                self.clickhouse_client,
                version_date_int,
                version_id,
            )
            print(
                f"📊 Витрина sim_masterv2_v9_daily: {daily_rows} строк "
                f"({version_date_int}, {version_id})"
            )
        elif rl_data is None:
            print("⚠️ RepairLine Drain не прочитал данные")
        
        t_end = time.perf_counter()
        total_time = t_end - t_start
        
        print(f"\n✅ Симуляция завершена:")
        print(f"   Шагов: {final_steps}")
        print(f"   end_day: {self.end_day}")
        print(f"   Время общее: {total_time:.2f}с")
        print(f"   Время GPU: {gpu_time:.2f}с")
        if gpu_time > 0:
            print(f"   Скорость: {self.end_day / gpu_time:.0f} дней/сек (GPU)")
        if total_time > 0:
            print(f"   Скорость: {self.end_day / total_time:.0f} дней/сек (общая)")
        
        self._print_final_stats()
        
        return self.end_day
    
    def _populate_agents(self):
        """Заполнение агентов из heli_pandas + spawn"""
        print("\n📦 Заполнение агентов...")
        
        # Планеры из heli_pandas
        self.population_builder.populate_agents(self.simulation, self.base_model.agent)
        
        # inactive: repair_days всегда 0
        inactive_pop = fg.AgentVector(self.base_model.agent)
        self.simulation.getPopulationData(inactive_pop, "inactive")
        for i in range(inactive_pop.size()):
            inactive_pop[i].setVariableUInt("repair_days", 0)
        self.simulation.setPopulationData(inactive_pop, "inactive")
        
        # Детерминированный spawn реализован через HF_DeterministicSpawn (mid-simulation)
        spawn_count = getattr(self, '_deterministic_spawn_count', 0)
        
        # V8: Добавляем repair_exits в deterministic_dates
        self._add_repair_exits_to_deterministic()
        
        # QuotaManager агенты
        initial_ops = self.population_builder.get_initial_ops_count()
        mi8_ops = initial_ops.get(1, 0)
        mi17_ops = initial_ops.get(2, 0)
        
        qm_pop = fg.AgentVector(self.base_model.quota_agent, 2)
        qm_pop[0].setVariableUInt8("group_by", 1)  # Mi-8
        qm_pop[1].setVariableUInt8("group_by", 2)  # Mi-17
        self.simulation.setPopulationData(qm_pop)
        
        # RepairLine агенты (ремонтные линии)
        # if self.repair_quota > 0:
        #     rl_pop = fg.AgentVector(self.base_model.repair_line_agent, self.repair_quota)
        #     for i in range(self.repair_quota):
        #         rl_pop[i].setVariableUInt("line_id", i)
        #         rl_pop[i].setVariableUInt("free_days", 1)
        #         rl_pop[i].setVariableUInt("aircraft_number", 0)
        #     self.simulation.setPopulationData(rl_pop)
        self._init_repair_lines_at_build()
        
        # V8: RepairAgent ОТКЛЮЧЁН — не инициализируем популяцию
        # count_repair = self._count_agents_in_state("repair")
        
        # Динамический спавн (менеджер + тикеты)
        if hasattr(self, 'spawn_data') and self.spawn_data:
            rtc_spawn_dynamic_v7.init_spawn_dynamic_population_v8(
                self.simulation,
                self.model,
                self.spawn_data['first_dynamic_idx_mi17'],
                self.spawn_data['dynamic_reserve_mi17'],
                self.spawn_data['base_acn_mi17'],
                self.spawn_data['first_dynamic_idx_mi8'],
                self.spawn_data['dynamic_reserve_mi8'],
                self.spawn_data['base_acn_mi8']
            )
        
        print(f"   ✅ Агенты загружены: Mi-8 ops={mi8_ops}, Mi-17 ops={mi17_ops}, spawn={spawn_count}")

    def _init_repair_lines_at_build(self):
        """V8: Инициализация RepairLine агентов (day-0 repair -> line)."""
        if self.repair_quota <= 0:
            return
        
        rl_pop = fg.AgentVector(self.base_model.repair_line_agent, self.repair_quota)
        for i in range(self.repair_quota):
            rl_pop[i].setVariableUInt("line_id", i)
            rl_pop[i].setVariableUInt("free_days", 1)
            rl_pop[i].setVariableUInt("aircraft_number", 0)
            rl_pop[i].setVariableUInt("last_acn", 0)
            rl_pop[i].setVariableUInt("last_day", 0)
        
        # Day-0 repair: привязываем к первым линиям
        repair_pop = fg.AgentVector(self.base_model.agent)
        self.simulation.getPopulationData(repair_pop, "repair")
        assign_count = min(repair_pop.size(), self.repair_quota)
        day0_map = {}
        for i in range(assign_count):
            agent = repair_pop.at(i)
            acn = agent.getVariableUInt("aircraft_number")
            gb = agent.getVariableUInt("group_by")
            repair_time = agent.getVariableUInt("repair_time")
            exit_date = agent.getVariableUInt("exit_date")
            rl_pop[i].setVariableUInt("aircraft_number", acn)
            rl_pop[i].setVariableUInt("free_days", 0)
            rl_pop[i].setVariableUInt("last_acn", acn)
            rl_pop[i].setVariableUInt("last_day", 0)
            day0_map[i] = (acn, int(gb), int(repair_time), int(exit_date))
        
        # Передаём mapping занятых линий в HF_InitRepairLines
        # чтобы repair_line_rt_mp[i] был корректно инициализирован
        self._hf_init_lines.day0_map = day0_map
        
        self.simulation.setPopulationData(rl_pop)
        print(f"   ✅ RepairLine init: quota={self.repair_quota}, day0_assigned={assign_count}, day0_map={len(day0_map)}")

    # REMOVED: _init_v8_macroproperties_after_population — использовал несуществующий
    # CUDASimulation.getEnvironmentMacroPropertyUInt(). MacroProperty доступны только
    # из HostFunction.environment.getMacroPropertyUInt(). Заменён на HF_InitV8 (addInitFunction).

    def _debug_day_snapshot(self, day: int):
        """Локальный отладочный срез по Mi-17 на выбранный день."""
        print(f"\n🔎 DEBUG day={day}: Mi-17 snapshot")
        counts = {}
        for state_name in ["operations", "serviceable", "unserviceable", "inactive", "reserve", "storage", "repair"]:
            pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(pop, state_name)
            cnt = 0
            for i in range(pop.size()):
                if pop.at(i).getVariableUInt("group_by") == 2:
                    cnt += 1
            counts[state_name] = cnt
        print(f"  counts: {counts}")
        
        unsvc = []
        pop = fg.AgentVector(self.base_model.agent)
        self.simulation.getPopulationData(pop, "unserviceable")
        for i in range(pop.size()):
            agent = pop.at(i)
            if agent.getVariableUInt("group_by") != 2:
                continue
            unsvc.append({
                "acn": agent.getVariableUInt("aircraft_number"),
                "idx": agent.getVariableUInt("idx"),
                "exit_date": agent.getVariableUInt("exit_date"),
                "repair_time": agent.getVariableUInt("repair_time"),
                "ppr": agent.getVariableUInt("ppr"),
                "sne": agent.getVariableUInt("sne"),
            })
        if unsvc:
            unsvc_sorted = sorted(unsvc, key=lambda x: x["exit_date"])
            print("  unsvc (mi17) by exit_date:")
            for row in unsvc_sorted[:20]:
                print(f"    acn={row['acn']}, idx={row['idx']}, exit={row['exit_date']}, "
                      f"repair_time={row['repair_time']}, ppr={row['ppr']}, sne={row['sne']}")
        else:
            print("  unsvc (mi17): none")
    
    def _count_agents_in_state(self, state_name: str) -> int:
        """Подсчёт агентов в указанном состоянии"""
        pop = fg.AgentVector(self.base_model.agent)
        self.simulation.getPopulationData(pop, state_name)
        return pop.size()
    
    def _add_repair_exits_to_deterministic(self):
        """V8: Добавляет даты выхода из repair в deterministic_dates"""
        # Получаем агентов в repair
        repair_pop = fg.AgentVector(self.base_model.agent)
        self.simulation.getPopulationData(repair_pop, "repair")
        
        for i in range(repair_pop.size()):
            agent = repair_pop.at(i)
            repair_days = agent.getVariableUInt('repair_days')
            repair_time = agent.getVariableUInt('repair_time')
            # exit_day = repair_time - repair_days (абсолютный день)
            exit_day = repair_time - repair_days
            if exit_day > 0 and exit_day <= self.end_day:
                self.deterministic_dates.append(exit_day)
        
        self.deterministic_dates = sorted(set(self.deterministic_dates))
        print(f"   V8 deterministic_dates (с repair): {len(self.deterministic_dates)} дат")
    
    def _print_final_stats(self):
        """Вывод финальной статистики + ВАЛИДАЦИЯ"""
        print("\n📊 Финальная статистика V8:")
        
        # Подсчёт по состояниям и типам
        states = ["inactive", "operations", "serviceable", "repair", "storage", "unserviceable"]  # DISABLED (state5-unused): reserve removed
        state_counts = {}
        mi8_by_state = {}
        mi17_by_state = {}
        total = 0
        
        for state in states:
            heli_pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(heli_pop, state)
            count = heli_pop.size()
            state_counts[state] = count
            total += count
            
            # Подсчёт по типам
            mi8 = 0
            mi17 = 0
            for i in range(count):
                agent = heli_pop.at(i)
                group_by = agent.getVariableUInt("group_by")  # UInt32 в HELI agent
                if group_by == 1:
                    mi8 += 1
                elif group_by == 2:
                    mi17 += 1
            mi8_by_state[state] = mi8
            mi17_by_state[state] = mi17
            
            print(f"   {state}: {count} (Mi-8: {mi8}, Mi-17: {mi17})")
        print(f"   -----------")
        print(f"   ВСЕГО: {total}")
        
        # ═══════════════════════════════════════════════════════════════
        # ВАЛИДАЦИЯ: ops = target (на последний день)
        # ═══════════════════════════════════════════════════════════════
        print("\n🔍 ВАЛИДАЦИЯ ops = target:")
        
        mi8_ops = mi8_by_state.get("operations", 0)
        mi17_ops = mi17_by_state.get("operations", 0)
        
        # Target на СЛЕДУЮЩИЙ день после end_day (или end_day если это последний)
        mp4_mi8 = self.env_data.get('mp4_ops_counter_mi8', [])
        mp4_mi17 = self.env_data.get('mp4_ops_counter_mi17', [])
        
        # Используем target на end_day (так как это финальное состояние)
        target_day = min(self.end_day, len(mp4_mi8) - 1)
        mi8_target = mp4_mi8[target_day] if target_day < len(mp4_mi8) else 0
        mi17_target = mp4_mi17[target_day] if target_day < len(mp4_mi17) else 0
        
        mi8_delta = mi8_ops - mi8_target
        mi17_delta = mi17_ops - mi17_target
        
        mi8_status = "✅" if mi8_delta == 0 else "❌"
        mi17_status = "✅" if mi17_delta == 0 else "❌"
        
        print(f"   Mi-8:  ops={mi8_ops}, target={mi8_target}, Δ={mi8_delta:+} {mi8_status}")
        print(f"   Mi-17: ops={mi17_ops}, target={mi17_target}, Δ={mi17_delta:+} {mi17_status}")
        
        # Контекст для диагностики дефицита (не считаем это доказательством причины)
        mi8_unsvc = mi8_by_state.get("unserviceable", 0)
        mi8_repair = mi8_by_state.get("repair", 0)
        mi8_serviceable = mi8_by_state.get("serviceable", 0)
        mi8_inactive = mi8_by_state.get("inactive", 0)
        # DISABLED (state5-unused): mi8_reserve = mi8_by_state.get("reserve", 0)
        mi17_unsvc = mi17_by_state.get("unserviceable", 0)
        mi17_repair = mi17_by_state.get("repair", 0)
        mi17_serviceable = mi17_by_state.get("serviceable", 0)
        mi17_inactive = mi17_by_state.get("inactive", 0)
        # DISABLED (state5-unused): mi17_reserve = mi17_by_state.get("reserve", 0)
        
        if mi8_delta < 0:
            print(
                f"   ⚠️ Mi-8 дефицит: unsvc={mi8_unsvc}, repair={mi8_repair}, "
                f"svc={mi8_serviceable}, inactive={mi8_inactive}"
            )
        if mi17_delta < 0:
            print(
                f"   ⚠️ Mi-17 дефицит: unsvc={mi17_unsvc}, repair={mi17_repair}, "
                f"svc={mi17_serviceable}, inactive={mi17_inactive}"
            )
        
        # Итоговый статус валидации
        if mi8_delta == 0 and mi17_delta == 0:
            print("\n✅ ВАЛИДАЦИЯ ПРОЙДЕНА: ops = target")
        else:
            print("\n❌ ВАЛИДАЦИЯ ПРОВАЛЕНА: ops ≠ target")
    
    def _postprocess_promotions(self, fields, days, num_steps, total_agents):
        """
        Постпроцессинг P2/P3 промоутов: окно ремонта по runtime claim metadata + дневной cap.
        
        P2 (commit_p2=1): 7→2 превращается в 7→4→2 (unsvc→repair→ops)
        P3 (commit_p3=1): 1→2 превращается в 1→4→2 (inactive→repair→ops)
        
        Для каждого события промоута:
        1. Находим step S где commit_p2=1 или commit_p3=1
        2. Берём claim metadata (start/end/source) из MP2
        3. Ищем шаги в диапазоне [claim_start, claim_end) и заменяем их status на 4
        4. Устанавливаем repair_days (накопительный) и assembly_trigger
        """
        import numpy as np
        
        modified = 0
        apply_daily_cap = self.repair_quota > 0
        
        # Текущая дневная занятость repair по планерам (group_by 1/2) на основе status_id=4
        day_to_agents_set = {}
        for s in range(num_steps):
            day = int(days[s])
            day_set = day_to_agents_set.get(day)
            if day_set is None:
                day_set = set()
                day_to_agents_set[day] = day_set
            for a in range(total_agents):
                if int(fields['mp2_status_id'][s, a]) != 4:
                    continue
                group_by = int(fields['mp2_group_by'][a])
                if group_by not in (1, 2):
                    continue
                agent_key = int(fields['mp2_idx'][a])
                day_set.add(agent_key)
        
        for a in range(total_agents):
            assembly_time_val = 0
            
            for s in range(num_steps):
                p2 = int(fields['mp2_commit_p2'][s, a])
                p3 = int(fields['mp2_commit_p3'][s, a])
                
                if p2 == 1 or p3 == 1:
                    claim_source = int(fields['mp2_repair_claim_source'][s, a])
                    if claim_source not in (1, 2):
                        continue
                    claim_start = int(fields['mp2_repair_claim_start_day'][s, a])
                    claim_end = int(fields['mp2_repair_claim_end_day'][s, a])
                    if claim_start == 0xFFFFFFFF or claim_end == 0xFFFFFFFF or claim_end <= claim_start:
                        continue
                    assembly_time_val = int(fields['mp2_assembly_time'][s, a])
                    group_by_event = int(fields['mp2_group_by'][a])
                    agent_key = int(fields['mp2_idx'][a])
                    apply_daily_cap_for_agent = apply_daily_cap and group_by_event in (1, 2)
                    
                    # Собираем кандидатов окна ремонта с guard-условиями
                    candidate_steps = []
                    for s_back in range(num_steps):
                        d_back = int(days[s_back])
                        if claim_start <= d_back < claim_end:
                            current_status = int(fields['mp2_status_id'][s_back, a])
                            # Guard: перезаписываем только unsvc(7) или inactive(1)
                            if current_status not in (7, 1):
                                continue
                            # Guard: пропускаем дни перехода (pre_status != status) —
                            # сохраняем оригинальный GPU-переход (напр. 2→7)
                            current_pre = int(fields['mp2_pre_status_id'][s_back, a])
                            if current_pre != current_status:
                                continue
                            candidate_steps.append((s_back, d_back))
                    
                    if not candidate_steps:
                        continue
                    
                    # Pre-check: дневной cap (если превышен на любой день — отклоняем событие)
                    if apply_daily_cap_for_agent:
                        reject_event = False
                        for s_back, d_back in candidate_steps:
                            day_set = day_to_agents_set.get(d_back)
                            if day_set is None:
                                day_len = 0
                                has_agent = False
                            else:
                                day_len = len(day_set)
                                has_agent = agent_key in day_set
                            if not has_agent and day_len >= self.repair_quota:
                                reject_event = True
                                break
                        if reject_event:
                            continue
                    
                    # Устанавливаем active_trigger=1 на шаге промоута
                    fields['mp2_active_trigger'][s, a] = 1
                    
                    # Заполняем окно ремонта без пропусков
                    repair_day_counter = 0
                    first_repair_set = False
                    for s_back, d_back in candidate_steps:
                        # Этот шаг попадает в окно ремонта
                        fields['mp2_status_id'][s_back, a] = 4  # repair
                        if first_repair_set:
                            fields['mp2_pre_status_id'][s_back, a] = 4
                        else:
                            first_repair_set = True
                        if apply_daily_cap_for_agent:
                            day_set = day_to_agents_set.get(d_back)
                            if day_set is None:
                                day_set = set()
                                day_to_agents_set[d_back] = day_set
                            day_set.add(agent_key)
                        repair_day_counter += 1
                        fields['mp2_repair_days'][s_back, a] = repair_day_counter
                        
                        # assembly_trigger=1 если осталось <= assembly_time до конца ремонта
                        days_to_end = claim_end - d_back
                        if days_to_end <= assembly_time_val:
                            fields['mp2_assembly_trigger'][s_back, a] = 1
                        
                        modified += 1
                    
                    # Шаг промоута: pre_status_id = 4 только если окно было окрашено
                    if first_repair_set:
                        fields['mp2_pre_status_id'][s, a] = 4
        
        # Нормализация assembly_trigger по фактическому хвосту ремонта
        for s in range(num_steps):
            for a in range(total_agents):
                status = int(fields['mp2_status_id'][s, a])
                if status != 4:
                    fields['mp2_assembly_trigger'][s, a] = 0
                    continue
                assembly_time = int(fields['mp2_assembly_time'][s, a])
                if assembly_time <= 0:
                    fields['mp2_assembly_trigger'][s, a] = 0
                    continue
                repair_time = int(fields['mp2_repair_time'][s, a])
                repair_days = int(fields['mp2_repair_days'][s, a])
                remaining_repair = repair_time - repair_days
                if remaining_repair < 0:
                    remaining_repair = 0
                fields['mp2_assembly_trigger'][s, a] = 1 if remaining_repair < assembly_time else 0
        
        return modified


class HF_DeterministicSpawn(fg.HostFunction):
    """
    HostFunction: детерминированный spawn агентов в serviceable.
    Создаёт агентов mid-simulation когда current_day >= spawn_day.
    pre_status_id = 0 (маркер spawn) сохраняется корректно.
    """
    
    def __init__(self, spawn_schedule, base_idx, base_acn, env_consts):
        """
        Args:
            spawn_schedule: list of (day, count) — расписание spawn
            base_idx: стартовый idx для det spawn агентов
            base_acn: стартовый aircraft_number (100000)
            env_consts: dict с ll, oh, br, repair_time, assembly_time, partout_time, second_ll_sentinel
        """
        super().__init__()
        self.spawn_schedule = list(spawn_schedule or [])
        self.base_idx = int(base_idx)
        self.base_acn = int(base_acn)
        self.env_consts = env_consts or {}
        self.spawned_days = set()
        self.total_spawned = 0
    
    def run(self, FLAMEGPU):
        if not self.spawn_schedule:
            return
        
        env = FLAMEGPU.environment
        current_day = int(env.getPropertyUInt("current_day"))
        svc_api = None
        
        for spawn_day, count in self.spawn_schedule:
            spawn_day_i = int(spawn_day)
            if spawn_day_i in self.spawned_days:
                continue
            if current_day < spawn_day_i:
                continue
            
            self.spawned_days.add(spawn_day_i)
            count_i = int(count)
            if count_i <= 0:
                continue
            
            if svc_api is None:
                svc_api = FLAMEGPU.agent("HELI", "serviceable")
            
            start_acn = self.base_acn + self.total_spawned
            for _ in range(count_i):
                idx = self.base_idx + self.total_spawned
                acn = self.base_acn + self.total_spawned
                
                agent = svc_api.newAgent()
                agent.setVariableUInt("idx", idx)
                agent.setVariableUInt("aircraft_number", acn)
                agent.setVariableUInt("partseqno_i", 0)
                agent.setVariableUInt("group_by", 2)  # Mi-17
                agent.setVariableUInt("status_id", 3)
                agent.setVariableUInt("pre_status_id", 0)  # spawn marker
                agent.setVariableUInt("intent_state", 3)
                agent.setVariableUInt("prev_intent", 0)
                agent.setVariableUInt("bi_counter", 1)
                
                agent.setVariableUInt("transition_0_to_2", 0)
                agent.setVariableUInt("transition_2_to_3", 0)
                agent.setVariableUInt("transition_2_to_6", 0)
                agent.setVariableUInt("transition_2_to_7", 0)
                agent.setVariableUInt("transition_3_to_2", 0)
                agent.setVariableUInt("transition_7_to_2", 0)
                agent.setVariableUInt("transition_1_to_2", 0)
                agent.setVariableUInt("transition_4_to_3", 0)
                
                agent.setVariableUInt("exit_date", 0)
                agent.setVariableUInt("sne", 0)
                agent.setVariableUInt("ppr", 0)
                agent.setVariableUInt("cso", 0)
                agent.setVariableUInt("daily_today_u32", 0)
                agent.setVariableUInt("daily_next_u32", 0)
                
                agent.setVariableUInt("ll", int(self.env_consts.get('ll', 0)))
                agent.setVariableUInt(
                    "second_ll", int(self.env_consts.get('second_ll_sentinel', 0xFFFFFFFF))
                )
                agent.setVariableUInt("oh", int(self.env_consts.get('oh', 0)))
                agent.setVariableUInt("br", int(self.env_consts.get('br', 0)))
                
                agent.setVariableUInt("repair_time", int(self.env_consts.get('repair_time', 0)))
                agent.setVariableUInt("assembly_time", int(self.env_consts.get('assembly_time', 0)))
                agent.setVariableUInt("partout_time", int(self.env_consts.get('partout_time', 0)))
                agent.setVariableUInt("repair_days", 0)
                agent.setVariableUInt("assembly_trigger", 0)
                agent.setVariableUInt("active_trigger", 0)
                agent.setVariableUInt("partout_trigger", 0)
                
                agent.setVariableUInt("mfg_date", current_day)
                agent.setVariableUInt("s4_days", 0)
                
                agent.setVariableUInt("limiter_date", 0xFFFFFFFF)
                agent.setVariableUInt16("limiter", 0)
                agent.setVariableUInt("computed_adaptive_days", 1)
                
                agent.setVariableUInt("status_change_day", current_day)
                agent.setVariableUInt("demote_day", 0)
                agent.setVariableUInt("repair_candidate", 0)
                agent.setVariableUInt("repair_line_id", 0xFFFFFFFF)
                agent.setVariableUInt("repair_line_day", 0xFFFFFFFF)
                
                agent.setVariableUInt("promoted", 0)
                agent.setVariableUInt("needs_demote", 0)
                agent.setVariableUInt("commit_p1", 0)
                agent.setVariableUInt("commit_p2", 0)
                agent.setVariableUInt("commit_p3", 0)
                agent.setVariableUInt("decision_p2", 0)
                agent.setVariableUInt("decision_p3", 0)
                
                agent.setVariableUInt("debug_promoted", 0)
                agent.setVariableUInt("debug_needs_demote", 0)
                agent.setVariableUInt("debug_repair_candidate", 0)
                agent.setVariableUInt("debug_repair_line_id", 0xFFFFFFFF)
                agent.setVariableUInt("debug_repair_line_day", 0xFFFFFFFF)
                agent.setVariableUInt("debug_bucket_seen", 0)
                
                self.total_spawned += 1
            
            end_acn = self.base_acn + self.total_spawned - 1
            print(
                f"   📦 Det spawn: {count_i} агентов в serviceable "
                f"(day={current_day}, acn={start_acn}..{end_acn})"
            )


class HF_InitMP5Cumsum(fg.HostFunction):
    """HostFunction для инициализации mp5_cumsum"""
    
    def __init__(self, mp5_cumsum, frames: int, days: int):
        super().__init__()
        self.mp5_cumsum = mp5_cumsum
        self.frames = frames
        self.days = days
        self.initialized = False
    
    def run(self, FLAMEGPU):
        if self.initialized:
            return
        
        print(f"  [HF_InitMP5Cumsum] Загрузка mp5_cumsum: {self.mp5_cumsum.shape}")
        
        mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp5_cumsum")
        
        for i in range(min(len(self.mp5_cumsum), len(mp))):
            mp[i] = int(self.mp5_cumsum[i])
        
        mp_min = FLAMEGPU.environment.getMacroPropertyUInt32("mp_min_limiter")
        mp_min[0] = 0xFFFFFFFF
        
        self.initialized = True
        print(f"  [HF_InitMP5Cumsum] ✅ Загружено")


class HF_InitInflationLogCumsum(fg.HostFunction):
    """HostFunction для инициализации scaled UInt32 inflation_log_cumsum."""

    def __init__(self, inflation_log_cumsum):
        super().__init__()
        self.inflation_log_cumsum = inflation_log_cumsum
        self.initialized = False

    def run(self, FLAMEGPU):
        if self.initialized:
            return

        print(
            "  [HF_InitInflationLogCumsum] Загрузка inflation_log_cumsum: "
            f"{len(self.inflation_log_cumsum)}"
        )

        mp = FLAMEGPU.environment.getMacroPropertyUInt32("inflation_log_cumsum")
        if len(self.inflation_log_cumsum) > len(mp):
            raise RuntimeError(
                "inflation_log_cumsum length exceeds MacroProperty size: "
                f"{len(self.inflation_log_cumsum)} > {len(mp)}"
            )

        for i in range(len(self.inflation_log_cumsum)):
            mp[i] = int(self.inflation_log_cumsum[i])

        self.initialized = True
        print("  [HF_InitInflationLogCumsum] ✅ Загружено")


class HF_InitRepairLines(fg.HostFunction):
    """HostFunction для инициализации RepairLine MacroProperty.
    
    day0_map: {line_id: (acn, group_by, repair_time, exit_date)} — линии, занятые day-0 repair агентами.
    Для таких линий вычисляем стартовые free_days и mp_rt так, чтобы автоосвобождение
    (rt > 0 && free_days >= rt) совпадало с индивидуальным exit_date.
    """
    
    def __init__(self, repair_quota: int, day0_map: dict = None,
                 mi8_rt: int = 180, mi17_rt: int = 180):
        super().__init__()
        self.repair_quota = int(repair_quota)
        self.day0_map = day0_map or {}
        self.mi8_rt = int(mi8_rt)
        self.mi17_rt = int(mi17_rt)
        self.initialized = False
    
    def run(self, FLAMEGPU):
        if self.initialized:
            return
        
        mp_days = FLAMEGPU.environment.getMacroPropertyUInt("repair_line_free_days_mp")
        mp_acn = FLAMEGPU.environment.getMacroPropertyUInt("repair_line_acn_mp")
        mp_gb = FLAMEGPU.environment.getMacroPropertyUInt("repair_line_gb_mp")
        mp_rt = FLAMEGPU.environment.getMacroPropertyUInt("repair_line_rt_mp")
        mp_last_acn = FLAMEGPU.environment.getMacroPropertyUInt("repair_line_last_acn_mp")
        mp_last_day = FLAMEGPU.environment.getMacroPropertyUInt("repair_line_last_day_mp")
        mp_bank_count = FLAMEGPU.environment.getMacroPropertyUInt("repair_line_bank_count_mp")
        mp_bank_lock = FLAMEGPU.environment.getMacroPropertyUInt("repair_line_bank_lock_mp")
        mp_bank_start = FLAMEGPU.environment.getMacroPropertyUInt("repair_line_bank_start_mp")
        mp_bank_end = FLAMEGPU.environment.getMacroPropertyUInt("repair_line_bank_end_mp")
        
        occupied_count = 0
        for i in range(len(mp_days)):
            mp_bank_count[i] = 0
            mp_bank_lock[i] = 0
            base = i * REPAIR_BANK_MAX
            for j in range(REPAIR_BANK_MAX):
                mp_bank_start[base + j] = 0xFFFFFFFF
                mp_bank_end[base + j] = 0xFFFFFFFF
            if i >= self.repair_quota:
                mp_days[i] = 0xFFFFFFFF  # не используется
                mp_acn[i] = 0
                mp_gb[i] = 0
                mp_rt[i] = 0
                mp_last_acn[i] = 0
                mp_last_day[i] = 0
            elif i in self.day0_map:
                # Линия занята day-0 repair агентом
                entry = self.day0_map[i]
                if len(entry) >= 4:
                    acn, gb, rt_agent, exit_date = entry
                else:
                    acn, gb = entry
                    rt_agent = 0
                    exit_date = 0
                gb = int(gb)
                rt = int(rt_agent) if int(rt_agent) > 0 else (self.mi8_rt if gb == 1 else self.mi17_rt)
                remaining_days = int(exit_date) if int(exit_date) > 0 else 0
                rt_eff = remaining_days if remaining_days > rt else rt
                initial_free_days = rt_eff - remaining_days
                # При таком старте free_days + remaining_days == rt_eff, значит release (free_days>=rt_eff)
                # срабатывает именно в exit_date для каждого борта.
                mp_days[i] = int(initial_free_days)
                mp_acn[i] = acn
                mp_gb[i] = gb
                mp_rt[i] = int(rt_eff)
                mp_last_acn[i] = acn
                mp_last_day[i] = 0
                occupied_count += 1
            else:
                mp_days[i] = 1       # свободна с 1
                mp_acn[i] = 0        # свободна
                mp_gb[i] = 0
                min_rt = min(self.mi8_rt, self.mi17_rt)
                if min_rt <= 0:
                    min_rt = max(self.mi8_rt, self.mi17_rt)
                mp_rt[i] = min_rt if min_rt > 0 else 0  # baseline readiness threshold для свободной линии, иначе QM не увидит слот
                mp_last_acn[i] = 0
                mp_last_day[i] = 0
        
        self.initialized = True
        print(f"  [HF_InitRepairLines] ✅ quota={self.repair_quota}, "
              f"day0_occupied={occupied_count}, free={self.repair_quota - occupied_count}")


def main():
    parser = argparse.ArgumentParser(description="LIMITER V8 Orchestrator")
    parser.add_argument("--version-date", required=True, help="Дата датасета (YYYY-MM-DD)")
    parser.add_argument("--end-day", type=int, default=3650, help="Последний день симуляции")
    parser.add_argument("--max-steps", type=int, default=10000, help="Максимум шагов")
    parser.add_argument("--drop-table", action="store_true", help="Пересоздать таблицу")
    
    args = parser.parse_args()

    def _kg_guard():
        import json
        from datetime import datetime, timezone
        from pathlib import Path

        active_window_sec = 3600

        if os.environ.get("KG_GUARD_BYPASS") == "1":
            print("⚠️  KG_GUARD_BYPASS=1 — workflow check skipped")
            return

        env_workflow_id = os.environ.get("AGENT_KG_WORKFLOW_ID")
        if env_workflow_id:
            print(f"✓ KG workflow: {env_workflow_id} (from env)")
            return

        repo_root = Path(__file__).resolve().parents[3]
        kg_path = repo_root / "config" / "agent_kg.json"
        if not kg_path.is_file():
            print(f"❌ KG-guard: Agent KG file is not available: {kg_path}")
            sys.exit(2)

        try:
            data = json.loads(kg_path.read_text(encoding="utf-8"))
        except OSError as exc:
            print(f"❌ KG-guard: cannot read Agent KG file {kg_path}: {exc}")
            sys.exit(2)
        except json.JSONDecodeError as exc:
            print(f"❌ KG-guard: invalid Agent KG JSON in {kg_path}: {exc}")
            sys.exit(2)

        now = datetime.now(timezone.utc)
        for workflow in data.get("workflows", []):
            if workflow.get("status") != "active":
                continue

            timestamp = workflow.get("updated_at") or workflow.get("created_at")
            if not timestamp:
                continue

            workflow_time = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            if workflow_time.tzinfo is None:
                workflow_time = workflow_time.replace(tzinfo=timezone.utc)
            if (now - workflow_time).total_seconds() <= active_window_sec:
                print(f"✓ KG workflow: {workflow.get('workflow_id')} (from agent_kg.json)")
                return

        print("""❌ KG-guard: no active workflow found in Agent KG (updated_at < 60 min) and AGENT_KG_WORKFLOW_ID env not set.

Run simulation requires Agent KG traceability. Options:
  1) Init workflow:
     python3 code/utils/agent_kg.py --init-workflow --workflow-id W_sim_<descr>_<UTC> \\
       --user-goal "<...>" --owner orchestrator --phase validation --profile low

  2) Reuse existing active workflow:
     export AGENT_KG_WORKFLOW_ID=<existing_wf>

  3) Emergency bypass (NOT for production runs):
     export KG_GUARD_BYPASS=1""")
        sys.exit(2)

    _kg_guard()
    
    print("\n" + "=" * 70)
    print("🚀 LIMITER V8 — Архитектура с RepairLine")
    print("=" * 70)
    
    # Подключение к ClickHouse (MP2 всегда включён)
    from sim_env_setup import get_client
    client = get_client()
    
    if args.drop_table:
        print("🗑️ DROP TABLE sim_masterv2_v9...")
        client.execute("DROP TABLE IF EXISTS sim_masterv2_v9")
        print("🗑️ DROP TABLE sim_repairline_v9...")
        client.execute("DROP TABLE IF EXISTS sim_repairline_v9")
    
    # Создание таблицы (упрощённая схема V9/MP2)
    client.execute("""
        CREATE TABLE IF NOT EXISTS sim_masterv2_v9 (
            version_date UInt32,
            version_id UInt32,
            day_u16 UInt16,
            day_date Date MATERIALIZED addDays(toDate(toString(version_date)), toUInt16(day_u16)),
            idx UInt16,
            aircraft_number UInt32,
            group_by UInt8,
            oh UInt32,
            br UInt32,
            ll UInt32,
            status_id UInt8,
            pre_status_id UInt8,
            status_change_day UInt16,
            sne UInt32,
            ppr UInt32,
            limiter UInt16,
            repair_days UInt16,
            repair_claim_start_day UInt16,
            repair_claim_end_day UInt16,
            repair_claim_source UInt8,
            repair_claim_line_id UInt16,
            repair_time UInt16,
            assembly_time UInt16,
            active_trigger UInt8,
            assembly_trigger UInt8,
            daily_today_u32 UInt32,
            daily_next_u32 UInt32,
            commit_p2 UInt32,
            commit_p3 UInt32
        ) ENGINE = MergeTree()
        PARTITION BY version_date
        ORDER BY (version_date, version_id, day_u16, idx)
    """)
    client.execute("ALTER TABLE sim_masterv2_v9 ADD COLUMN IF NOT EXISTS status_change_day UInt16")
    client.execute("ALTER TABLE sim_masterv2_v9 ADD COLUMN IF NOT EXISTS repair_claim_start_day UInt16")
    client.execute("ALTER TABLE sim_masterv2_v9 ADD COLUMN IF NOT EXISTS repair_claim_end_day UInt16")
    client.execute("ALTER TABLE sim_masterv2_v9 ADD COLUMN IF NOT EXISTS repair_claim_source UInt8")
    client.execute("ALTER TABLE sim_masterv2_v9 ADD COLUMN IF NOT EXISTS repair_claim_line_id UInt16")
    print("✅ Таблица sim_masterv2_v9 готова")
    
    # RepairLine таблица
    client.execute(rtc_repairline_export.DDL_REPAIRLINE)
    print("✅ Таблица sim_repairline_v9 готова")
    
    orchestrator = LimiterV8Orchestrator(
        args.version_date, 
        args.end_day,
        enable_mp2=True,
        clickhouse_client=client
    )
    orchestrator.prepare_data()
    orchestrator.build_model()
    orchestrator.run(args.max_steps)


if __name__ == "__main__":
    main()
