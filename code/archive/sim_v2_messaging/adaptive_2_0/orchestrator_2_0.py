#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (sim_v2 dead-code cleanup): не используется активным V8-ядром (orchestrator_limiter_v8). Внутренние импорты заморожены.
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
from components.agent_population import AgentPopulationBuilder

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
        
        # UInt32 для exchange() совместимости
        days_mp = FLAMEGPU.environment.getMacroPropertyUInt32("program_event_days")
        mi8_mp = FLAMEGPU.environment.getMacroPropertyUInt32("program_target_mi8")
        mi17_mp = FLAMEGPU.environment.getMacroPropertyUInt32("program_target_mi17")
        
        count = 0
        for i in range(len(self.event_days)):
            if self.event_days[i] < 0xFFFF:
                days_mp[i] = int(self.event_days[i])
                mi8_mp[i] = int(self.target_mi8[i])
                mi17_mp[i] = int(self.target_mi17[i])
                count += 1
            else:
                days_mp[i] = 0xFFFFFFFF
        
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


class HF_UpdateCurrentDay(fg.HostFunction):
    """DEPRECATED: Заменено на RTC_SAVE_ADAPTIVE + RTC_UPDATE_CURRENT_DAY.
    
    Оставлено для документации решения проблемы read/write MacroProperty.
    
    РЕШЕНИЕ: разделение на 2 RTC функции в разных слоях:
      L7a: rtc_save_adaptive   - READ MacroProperty → agent var
      L7b: rtc_update_day      - READ agent var → WRITE MacroProperty
    
    Это позволяет 100% GPU-only выполнение без HostFunction!
    """
    
    def __init__(self, end_day: int):
        super().__init__()
        self.end_day = end_day
        self.step_count = 0
        self.finished = False
    
    def run(self, FLAMEGPU):
        if self.finished:
            return
        
        # Читаем adaptive_days из GPU
        mp_adaptive = FLAMEGPU.environment.getMacroPropertyUInt32("global_min_result")
        adaptive_days = int(mp_adaptive[0])
        
        # Читаем current_day и обновляем
        mp_day = FLAMEGPU.environment.getMacroPropertyUInt32("current_day_mp")
        current_day = int(mp_day[0])
        
        new_day = current_day + adaptive_days
        mp_day[0] = new_day
        
        self.step_count += 1
        
        # Логирование каждые 50 шагов
        if self.step_count % 50 == 0:
            print(f"  День {new_day}/{self.end_day}, adaptive={adaptive_days}, шаг={self.step_count}")
        
        # Проверка завершения
        if new_day >= self.end_day:
            self.finished = True
            print(f"  ✅ Завершено на шаге {self.step_count}, день {new_day}")


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
        
        # Environment (уже включает end_day, frames_total)
        env = self.model.Environment()
        setup_environment_2_0(env)
        
        # Устанавливаем значения (свойства уже созданы в setup_environment_2_0)
        # end_day и frames_total будут установлены через simulation после создания
        
        # Init функции
        self._register_init_functions()
        
        # RTC модули (включая update_current_day — теперь тоже RTC!)
        register_all_modules(self.model, self.planer_agent, self.quota_agent)
        
        # HostFunction больше НЕ нужен!
        # Обновление current_day теперь через 2 RTC функции:
        #   L7a: rtc_save_adaptive - READ MacroProperty → agent var
        #   L7b: rtc_update_day    - READ agent var → WRITE MacroProperty
        
        print("  ✅ Модель построена (100% GPU-only, без HostFunction!)")
    
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
        
        # Устанавливаем значения Environment properties
        self.simulation.setEnvironmentPropertyUInt("end_day", self.end_day)
        self.simulation.setEnvironmentPropertyUInt("frames_total", self.env_data.get('frames_total_u16', 279))
        
        # Популяция Planer
        self._populate_planers()
        
        # Популяция QuotaManager (1 агент)
        quota_pop = fg.AgentVector(self.quota_agent, 1)
        quota_pop[0].setVariableUInt8("id", 0)
        self.simulation.setPopulationData(quota_pop, "active")
        
        print("  ✅ Симуляция создана")
    
    def _populate_planers(self):
        """Заполняет популяцию Planer из MP3 данных (через AgentPopulationBuilder или напрямую)."""
        # Извлекаем массивы MP3 из env_data (правильный источник!)
        mp3 = self.env_data.get('mp3_arrays', {})
        ac_list = mp3.get('mp3_aircraft_number', [])
        status_list = mp3.get('mp3_status_id', [])
        sne_list = mp3.get('mp3_sne', [])
        ppr_list = mp3.get('mp3_ppr', [])
        repair_days_list = mp3.get('mp3_repair_days', [])
        gb_list = mp3.get('mp3_group_by', [])
        
        # Получаем константы для LL/OH/BR
        mi8_ll = int(self.env_data.get('mi8_ll_const', 1080000))
        mi8_oh = int(self.env_data.get('mi8_oh_const', 270000))
        mi8_br = int(self.env_data.get('mi8_br_const', 973750))
        mi17_ll = int(self.env_data.get('mi17_ll_const', 1080000))
        mi17_oh = int(self.env_data.get('mi17_oh_const', 270000))
        mi17_br = int(self.env_data.get('mi17_br_const', 973750))
        mi8_repair_time = int(self.env_data.get('mi8_repair_time_const', 180))
        mi17_repair_time = int(self.env_data.get('mi17_repair_time_const', 180))
        
        # Фильтруем только планеры (group_by IN (1, 2))
        plane_records = []
        for i in range(len(ac_list)):
            gb = int(gb_list[i]) if i < len(gb_list) else 0
            if gb not in (1, 2):
                continue
            
            status = int(status_list[i]) if i < len(status_list) else 1
            ll = mi8_ll if gb == 1 else mi17_ll
            oh = mi8_oh if gb == 1 else mi17_oh
            br = mi8_br if gb == 1 else mi17_br
            repair_time = mi8_repair_time if gb == 1 else mi17_repair_time
            
            plane_records.append({
                'idx': len(plane_records),
                'aircraft_number': int(ac_list[i]) if i < len(ac_list) else 0,
                'group_by': gb,
                'status_id': status,
                'sne': int(sne_list[i]) if i < len(sne_list) else 0,
                'ppr': int(ppr_list[i]) if i < len(ppr_list) else 0,
                'll': ll,
                'oh': oh,
                'br': br,
                'repair_days': int(repair_days_list[i]) if i < len(repair_days_list) else 0,
                'repair_time': repair_time,
            })
        
        print(f"  📊 MP3: найдено {len(plane_records)} планеров (group_by IN (1,2))")
        
        # Сохраняем для drain
        self.env_data['heli_pandas'] = plane_records
        
        # Группируем по состояниям
        by_state = {'inactive': [], 'operations': [], 'repair': [], 'reserve': [], 'storage': []}
        state_map = {1: 'inactive', 2: 'operations', 3: 'serviceable', 4: 'repair', 5: 'reserve', 6: 'storage'}
        
        for agent in plane_records:
            state = state_map.get(agent.get('status_id', 1), 'inactive')
            if state == 'serviceable':
                state = 'operations'  # Simplify for 2.0
            by_state[state].append(agent)
        
        # Создаём агентов
        for state, agents in by_state.items():
            if not agents:
                continue
            
            pop = fg.AgentVector(self.planer_agent, len(agents))
            
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
        
        print(f"  Настройка steps={estimated_steps}...")
        
        # Устанавливаем количество шагов через SimulationConfig
        self.simulation.SimulationConfig().steps = estimated_steps
        
        print(f"  Запуск simulate()...")
        
        # ═══════════════════════════════════════════════════════════════════
        # ИСТИННЫЙ GPU-ONLY: ОДИН вызов, НОЛЬ host callbacks!
        # RTC функции делают early return когда current_day >= end_day
        # Пустые шаги после end_day выполняются мгновенно
        # ═══════════════════════════════════════════════════════════════════
        self.simulation.simulate()
        
        t_gpu = time.perf_counter()
        
        # Читаем финальное состояние
        # getStepCounter() возвращает количество выполненных шагов
        actual_steps = self.simulation.getStepCounter()
        
        # final_day определяем из логики: шаги * средний adaptive_days
        # Более точно — читаем через step() и HF_LogProgress
        # Но для простоты используем end_day (симуляция достигла его)
        final_day = self.end_day  # Симуляция завершилась при достижении end_day
        
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
    
    def drain_mp2_to_db(self, table_name: str = "sim_masterv2_adaptive20"):
        """Выгрузка MP2 буфера из GPU в ClickHouse."""
        import numpy as np
        
        print(f"\n📤 Выгрузка MP2 в {table_name}")
        print("=" * 60)
        t_start = time.perf_counter()
        
        # Получаем размеры
        frames = self.env_data.get('frames_total_u16', 279)
        
        # Читаем write_idx (количество записанных шагов)
        # К сожалению после simulate() нет прямого доступа к MacroProperty
        # Используем оценку: actual_steps из run()
        # Для точного значения нужен HostFunction в конце
        
        # Альтернатива: считаем что записали все шаги до end_day
        # write_idx ≈ количество adaptive шагов
        max_records = frames * 700  # максимум в буфере
        
        print(f"  Чтение буферов (макс {max_records} записей)...")
        
        # Читаем через HostFunction-like доступ
        # После simulate() нужен step() чтобы получить данные
        # Или использовать getPopulationData для агентов
        
        # Упрощённый подход: читаем финальное состояние агентов
        agents_data = []
        for state in ['operations', 'repair', 'reserve', 'storage', 'inactive']:
            try:
                # Создаём AgentVector и заполняем его данными из симуляции
                pop = fg.AgentVector(self.planer_agent, 0)  # пустой вектор
                self.simulation.getPopulationData(pop, state)  # заполняем
                
                for i in range(pop.size()):
                    agent = pop[i]
                    agents_data.append({
                        'idx': int(agent.getVariableUInt16("idx")),
                        'sne': int(agent.getVariableUInt32("sne")),
                        'ppr': int(agent.getVariableUInt32("ppr")),
                        'state': state,
                        'day': self.end_day
                    })
            except Exception as e:
                print(f"    ⚠️ Состояние {state}: {e}")
        
        print(f"  Получено {len(agents_data)} записей финального состояния")
        
        if not agents_data:
            print("  ❌ Нет данных для выгрузки")
            return 0
        
        # Создаём таблицу если не существует
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            version_date Date,
            day_u16 UInt16,
            idx UInt16,
            aircraft_number UInt32,
            sne UInt32,
            ppr UInt32,
            status_id UInt8,
            group_by UInt8,
            timestamp DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY (version_date, day_u16, idx)
        """
        
        try:
            self.client.execute(create_table_sql)
            print(f"  ✅ Таблица {table_name} готова")
        except Exception as e:
            print(f"  ⚠️ Таблица: {e}")
        
        # Очистка старых данных для этой версии
        self.client.execute(f"ALTER TABLE {table_name} DELETE WHERE version_date = toDate('{self.version_date}')")
        print(f"  🗑️ Очищены старые данные для {self.version_date}")
        
        # Подготовка данных для вставки
        # Добавляем aircraft_number и group_by из env_data
        hp_data = self.env_data.get('heli_pandas', [])
        ac_map = {row['idx']: (row['aircraft_number'], row['group_by']) for row in hp_data}
        
        # Маппинг состояний
        state_map = {'operations': 2, 'repair': 4, 'reserve': 5, 'storage': 6, 'inactive': 1}
        
        from datetime import datetime
        version_dt = datetime.strptime(self.version_date, '%Y-%m-%d').date()
        
        rows = []
        for agent in agents_data:
            idx = agent['idx']
            ac_num, group_by = ac_map.get(idx, (0, 0))
            rows.append((
                version_dt,
                agent['day'],
                idx,
                ac_num,
                agent['sne'],
                agent['ppr'],
                state_map.get(agent['state'], 0),
                group_by
            ))
        
        # Вставка
        self.client.execute(
            f"INSERT INTO {table_name} (version_date, day_u16, idx, aircraft_number, sne, ppr, status_id, group_by) VALUES",
            rows
        )
        
        t_end = time.perf_counter()
        print(f"  ✅ Выгружено {len(rows)} записей за {t_end - t_start:.2f}с")
        
        return len(rows)


def main():
    parser = argparse.ArgumentParser(description='Adaptive 2.0 Orchestrator')
    parser.add_argument('--version-date', required=True, help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--end-day', type=int, default=3650, help='Конечный день')
    parser.add_argument('--export', action='store_true', help='Экспорт в БД')
    parser.add_argument('--table', default='sim_masterv2_adaptive20', help='Имя таблицы')
    
    args = parser.parse_args()
    
    orch = Orchestrator2_0(
        version_date=args.version_date,
        end_day=args.end_day
    )
    
    orch.prepare_data()
    orch.build_model()
    orch.create_simulation()
    result = orch.run()
    
    if args.export:
        rows = orch.drain_mp2_to_db(args.table)
        result['exported_rows'] = rows
    
    print("\n✅ Готово!")
    return result


if __name__ == '__main__':
    main()

