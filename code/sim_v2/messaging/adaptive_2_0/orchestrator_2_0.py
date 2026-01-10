#!/usr/bin/env python3
"""
Adaptive 2.0: Главный оркестратор

Чистая архитектура с limiter_date и ProgramEvent.
Всего 5 модулей, минимум сложности.

Запуск:
    python3 orchestrator_2_0.py --version-date 2025-07-04 --end-day 3650

Дата: 10.01.2026
"""
import os
import sys
import argparse
import time
import numpy as np
from typing import Dict, List, Tuple

# Пути
_THIS_DIR = os.path.dirname(__file__)
_MESSAGING_DIR = os.path.join(_THIS_DIR, '..')
_SIM_V2_DIR = os.path.join(_MESSAGING_DIR, '..')
_CODE_DIR = os.path.join(_SIM_V2_DIR, '..')

sys.path.insert(0, _THIS_DIR)
sys.path.insert(0, _MESSAGING_DIR)
sys.path.insert(0, _SIM_V2_DIR)
sys.path.insert(0, _CODE_DIR)

from sim_env_setup import get_client, prepare_env_arrays
from precompute_program_events import extract_program_events, create_program_event_array, compute_limiter_date_ops
from agents_2_0 import create_planer_agent, create_quota_manager_agent, setup_environment_2_0
from rtc_modules_2_0 import register_all_modules

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


class HF_InitCumsum(fg.HostFunction):
    """HostFunction для инициализации mp5_cumsum."""
    
    def __init__(self, cumsum_data: np.ndarray, frames: int, days: int):
        super().__init__()
        self.data = cumsum_data
        self.frames = frames
        self.days = days
        self.done = False
    
    def run(self, FLAMEGPU):
        if self.done:
            return
        
        mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp5_cumsum")
        MAX_FRAMES = 400
        MAX_DAYS_PLUS_1 = 4001
        
        count = 0
        for f in range(min(self.frames, MAX_FRAMES)):
            for d in range(min(self.days + 1, MAX_DAYS_PLUS_1)):
                src = f * (self.days + 1) + d
                dst = f * MAX_DAYS_PLUS_1 + d
                if src < len(self.data):
                    mp[dst] = int(self.data[src])
                    count += 1
        
        print(f"  ✅ mp5_cumsum: {count} элементов")
        self.done = True


class HF_InitProgramEvents(fg.HostFunction):
    """HostFunction для инициализации ProgramEvent данных."""
    
    def __init__(self, event_days: np.ndarray, target_mi8: np.ndarray, target_mi17: np.ndarray):
        super().__init__()
        self.event_days = event_days
        self.target_mi8 = target_mi8
        self.target_mi17 = target_mi17
        self.done = False
    
    def run(self, FLAMEGPU):
        if self.done:
            return
        
        days_mp = FLAMEGPU.environment.getMacroPropertyUInt16("program_event_days")
        mi8_mp = FLAMEGPU.environment.getMacroPropertyUInt16("program_target_mi8")
        mi17_mp = FLAMEGPU.environment.getMacroPropertyUInt16("program_target_mi17")
        
        count = 0
        for i in range(len(self.event_days)):
            if self.event_days[i] < 0xFFFF:
                days_mp[i] = int(self.event_days[i])
                mi8_mp[i] = int(self.target_mi8[i])
                mi17_mp[i] = int(self.target_mi17[i])
                count += 1
            else:
                days_mp[i] = 0xFFFF
        
        FLAMEGPU.environment.setPropertyUInt("events_total", count)
        print(f"  ✅ ProgramEvents: {count} событий")
        self.done = True


class HF_InitCurrentDay(fg.HostFunction):
    """HostFunction для инициализации current_day_mp и mp2_write_idx_mp."""
    
    def __init__(self, start_day: int = 0):
        super().__init__()
        self.start_day = start_day
        self.done = False
    
    def run(self, FLAMEGPU):
        if self.done:
            return
        
        # Инициализация current_day_mp
        mp_day = FLAMEGPU.environment.getMacroPropertyUInt32("current_day_mp")
        mp_day[0] = self.start_day
        
        # Инициализация mp2_write_idx_mp
        mp_idx = FLAMEGPU.environment.getMacroPropertyUInt32("mp2_write_idx_mp")
        mp_idx[0] = 0
        
        print(f"  ✅ current_day_mp = {self.start_day}, mp2_write_idx_mp = 0")
        self.done = True


# Удалён HF_ExitCondition — теперь истинный GPU-only!
# Early return в RTC функциях останавливает вычисления когда current_day >= end_day


class Orchestrator2_0:
    """Оркестратор Adaptive 2.0."""
    
    def __init__(self, version_date: str, end_day: int = 3650):
        self.version_date = version_date
        self.end_day = end_day
        
        self.client = get_client()
        self.env_data: Dict = {}
        self.model: fg.ModelDescription = None
        self.simulation: fg.CUDASimulation = None
        
        self.planer_agent = None
        self.quota_agent = None
        
        # Данные
        self.mp5_cumsum = None
        self.program_events = []
    
    def prepare_data(self):
        """Подготовка данных."""
        print("\n📊 Подготовка данных Adaptive 2.0")
        print("=" * 60)
        
        # Загрузка env_data
        self.env_data = prepare_env_arrays(self.client, self.version_date)
        
        frames = self.env_data.get('frames_total_u16', 279)
        
        # MP5 cumsum
        mp5_lin = self.env_data.get('mp5_daily_hours_linear', np.array([]))
        if len(mp5_lin) > 0:
            print(f"  Вычисление mp5_cumsum для {frames} агентов × {self.end_day} дней...")
            from precompute_events import compute_mp5_cumsum
            self.mp5_cumsum = compute_mp5_cumsum(mp5_lin, frames, self.end_day)
            print(f"  ✅ mp5_cumsum: {len(self.mp5_cumsum)} элементов")
        else:
            self.mp5_cumsum = np.zeros(frames * (self.end_day + 1), dtype=np.uint32)
        
        # Program events
        self.program_events = extract_program_events(self.client, self.version_date, self.end_day)
    
    def build_model(self):
        """Построение модели."""
        print("\n🔧 Построение модели Adaptive 2.0")
        print("=" * 60)
        
        self.model = fg.ModelDescription("Adaptive2_0")
        
        # Агенты
        self.planer_agent = create_planer_agent(self.model)
        self.quota_agent = create_quota_manager_agent(self.model)
        
        # Environment
        env = self.model.Environment()
        setup_environment_2_0(env)
        
        # Свойства
        env.newPropertyUInt("current_day", 0)
        env.newPropertyUInt("end_day", self.end_day)
        env.newPropertyUInt("frames_total", self.env_data.get('frames_total_u16', 279))
        
        # Init функции
        self._register_init_functions()
        
        # RTC модули
        register_all_modules(self.model, self.planer_agent, self.quota_agent)
        
        # Нет exit condition — истинный GPU-only!
        # Early return в RTC остановит вычисления
        
        print("  ✅ Модель построена (истинный GPU-only, без host callbacks)")
    
    def _register_init_functions(self):
        """Регистрация init функций."""
        frames = self.env_data.get('frames_total_u16', 279)
        
        # Init current_day_mp (ПЕРВЫМ!)
        hf_day = HF_InitCurrentDay(start_day=0)
        layer_day = self.model.newLayer("init_00_current_day")
        layer_day.addHostFunction(hf_day)
        
        # Init cumsum
        hf_cumsum = HF_InitCumsum(self.mp5_cumsum, frames, self.end_day)
        layer_cumsum = self.model.newLayer("init_01_cumsum")
        layer_cumsum.addHostFunction(hf_cumsum)
        
        # Init program events
        event_days, target_mi8, target_mi17 = create_program_event_array(self.program_events)
        hf_events = HF_InitProgramEvents(event_days, target_mi8, target_mi17)
        layer_events = self.model.newLayer("init_02_events")
        layer_events.addHostFunction(hf_events)
    
    def create_simulation(self):
        """Создание симуляции."""
        print("\n🖥️  Создание симуляции")
        print("=" * 60)
        
        self.simulation = fg.CUDASimulation(self.model)
        
        # Популяция Planer
        self._populate_planers()
        
        # Популяция QuotaManager (1 агент)
        quota_pop = fg.AgentVector(self.quota_agent, 1)
        quota_pop[0].setVariableUInt8("id", 0)
        self.simulation.setPopulationData(quota_pop)
        
        print("  ✅ Симуляция создана")
    
    def _populate_planers(self):
        """Заполняет популяцию Planer из heli_pandas."""
        # Получаем данные из env_data
        hp_data = self.env_data.get('heli_pandas_agents', [])
        
        if not hp_data:
            print("  ⚠️ Нет данных heli_pandas, загружаем напрямую...")
            hp_data = self._load_heli_pandas()
        
        # Группируем по состояниям
        by_state = {'inactive': [], 'operations': [], 'repair': [], 'reserve': [], 'storage': []}
        state_map = {1: 'inactive', 2: 'operations', 3: 'serviceable', 4: 'repair', 5: 'reserve', 6: 'storage'}
        
        for agent in hp_data:
            state = state_map.get(agent.get('status_id', 1), 'inactive')
            if state == 'serviceable':
                state = 'operations'  # Simplify for 2.0
            by_state[state].append(agent)
        
        # Создаём агентов
        for state, agents in by_state.items():
            if not agents:
                continue
            
            pop = fg.AgentVector(self.planer_agent, len(agents), state)
            
            for i, agent in enumerate(agents):
                pop[i].setVariableUInt16("idx", agent['idx'])
                pop[i].setVariableUInt32("aircraft_number", agent.get('aircraft_number', 0))
                pop[i].setVariableUInt8("group_by", agent.get('group_by', 1))
                pop[i].setVariableUInt32("sne", agent.get('sne', 0))
                pop[i].setVariableUInt32("ppr", agent.get('ppr', 0))
                pop[i].setVariableUInt32("ll", agent.get('ll', 0))
                pop[i].setVariableUInt32("oh", agent.get('oh', 0))
                pop[i].setVariableUInt32("br", agent.get('br', 0))
                pop[i].setVariableUInt16("repair_days", agent.get('repair_days', 0))
                pop[i].setVariableUInt16("repair_time", agent.get('repair_time', 180))
                pop[i].setVariableUInt32("mfg_date", agent.get('mfg_date', 0))
                
                # Вычисляем limiter_date при создании
                if state == 'operations':
                    limiter = compute_limiter_date_ops(
                        agent['idx'], 0, agent.get('sne', 0), agent.get('ppr', 0),
                        agent.get('ll', 0), agent.get('oh', 0),
                        self.mp5_cumsum, max_days=4000
                    )
                    pop[i].setVariableUInt16("limiter_date", limiter)
                elif state == 'repair':
                    repair_time = agent.get('repair_time', 180)
                    repair_days = agent.get('repair_days', 0)
                    limiter = repair_time - repair_days if repair_time > repair_days else 1
                    pop[i].setVariableUInt16("limiter_date", limiter)
                else:
                    pop[i].setVariableUInt16("limiter_date", 0xFFFF)
            
            self.simulation.setPopulationData(pop, state)
            print(f"  Загружено {len(agents)} агентов в '{state}'")
    
    def _load_heli_pandas(self) -> List[Dict]:
        """Загружает данные из heli_pandas."""
        query = f"""
        SELECT 
            idx, aircraft_number, group_by, status_id,
            sne, ppr, ll, oh, br,
            repair_days, repair_time, mfg_date
        FROM heli_pandas
        WHERE version_date = toDate('{self.version_date}')
          AND day_u16 = 0
          AND group_by IN (1, 2)
        ORDER BY idx
        """
        rows = self.client.execute(query)
        
        result = []
        for row in rows:
            result.append({
                'idx': row[0],
                'aircraft_number': row[1],
                'group_by': row[2],
                'status_id': row[3],
                'sne': row[4],
                'ppr': row[5],
                'll': row[6],
                'oh': row[7],
                'br': row[8],
                'repair_days': row[9],
                'repair_time': row[10],
                'mfg_date': row[11]
            })
        
        return result
    
    def run(self):
        """Запуск симуляции — ОДИН вызов simulate(), истинный GPU-only!"""
        print(f"\n▶️  Запуск Adaptive 2.0 (истинный GPU-only)")
        print("=" * 60)
        print(f"  end_day = {self.end_day}")
        print("  Host: загрузка → simulate(N) → drain")
        print("  GPU: все RTC с early return когда current_day >= end_day")
        print()
        
        t_start = time.perf_counter()
        
        # ═══════════════════════════════════════════════════════════════════
        # Расчёт N шагов
        # ~100 шагов/год (изменения программы + выработка ресурса + ремонты)
        # С запасом ×1.5 для безопасности
        # ═══════════════════════════════════════════════════════════════════
        years = self.end_day / 365
        estimated_steps = int(years * 100 * 1.5) + 100  # ~150 шагов/год + запас
        
        print(f"  Запуск simulate({estimated_steps})...")
        
        # ═══════════════════════════════════════════════════════════════════
        # ИСТИННЫЙ GPU-ONLY: ОДИН вызов, НОЛЬ host callbacks!
        # RTC функции делают early return когда current_day >= end_day
        # Пустые шаги после end_day выполняются мгновенно
        # ═══════════════════════════════════════════════════════════════════
        self.simulation.simulate(estimated_steps)
        
        t_gpu = time.perf_counter()
        
        # Читаем финальное состояние (ОДИН раз в конце)
        mp_day = self.simulation.environment.getMacroPropertyUInt32("current_day_mp")
        final_day = int(mp_day[0])
        
        mp_idx = self.simulation.environment.getMacroPropertyUInt32("mp2_write_idx_mp")
        actual_steps = int(mp_idx[0])
        
        t_end = time.perf_counter()
        elapsed = t_end - t_start
        gpu_time = t_gpu - t_start
        
        print(f"\n✅ Adaptive 2.0 GPU-only завершена:")
        print(f"  • Выполнено шагов: {estimated_steps} (из них {actual_steps} рабочих)")
        print(f"  • Финальный день: {final_day}/{self.end_day}")
        print(f"  • Время GPU: {gpu_time:.2f}с")
        print(f"  • Время общее: {elapsed:.2f}с")
        print(f"  • Рабочих шагов/год: {actual_steps / years:.1f}")
        
        return {
            'steps': actual_steps,
            'total_steps': estimated_steps,
            'days': final_day,
            'elapsed': elapsed,
            'gpu_time': gpu_time
        }


def main():
    parser = argparse.ArgumentParser(description='Adaptive 2.0 Orchestrator')
    parser.add_argument('--version-date', required=True, help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--end-day', type=int, default=3650, help='Конечный день')
    
    args = parser.parse_args()
    
    orch = Orchestrator2_0(
        version_date=args.version_date,
        end_day=args.end_day
    )
    
    orch.prepare_data()
    orch.build_model()
    orch.create_simulation()
    result = orch.run()
    
    print("\n✅ Готово!")
    return result


if __name__ == '__main__':
    main()

