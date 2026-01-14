#!/usr/bin/env python3
"""
LIMITER V7 Orchestrator — Однофазная архитектура

Архитектура V7:
- БЕЗ промежуточной переменной intent_state
- Переходы состояний через setInitialState/setEndState с FunctionCondition
- Флаги promoted/needs_demote для квотирования
- 100% GPU-only (как V5)

Порядок слоёв:
1. Сброс флагов (все агенты)
2. Детерминированные переходы (repair, spawn)
3. Operations инкременты
4. Operations переходы (2→6, 2→7)
5. Сброс буферов квотирования
6. Подсчёт агентов
7. Демоут
8. P1 промоут (serviceable)
9. P2 промоут (unserviceable)
10. P3 промоут (inactive)
11. Переходы после квотирования (demote, promote)
12. Limiter
13. GPU-only adaptive

Дата: 12.01.2026
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

import pyflamegpu as fg
import model_build

import rtc_spawn_dynamic_v7


def collect_agents_state(simulation, agent_desc, current_day, version_date_int, version_id):
    """Собирает состояние всех агентов в текущий момент"""
    states = ['inactive', 'operations', 'serviceable', 'repair', 'reserve', 'storage', 'unserviceable']
    rows = []
    
    for state_name in states:
        pop = fg.AgentVector(agent_desc)
        simulation.getPopulationData(pop, state_name)
        
        for i in range(pop.size()):
            agent = pop.at(i)
            rows.append({
                'version_date': version_date_int,
                'version_id': version_id,
                'day_u16': current_day,
                'idx': agent.getVariableUInt('idx'),
                'aircraft_number': agent.getVariableUInt('aircraft_number'),
                'group_by': agent.getVariableUInt('group_by'),
                'state': state_name,
                'sne': agent.getVariableUInt('sne'),
                'ppr': agent.getVariableUInt('ppr'),
                'll': agent.getVariableUInt('ll'),
                'oh': agent.getVariableUInt('oh'),
                'br': agent.getVariableUInt('br'),
                'repair_days': agent.getVariableUInt('repair_days'),
                'repair_time': agent.getVariableUInt('repair_time'),
            })
    return rows
from sim_env_setup import get_client, prepare_env_arrays
from base_model_messaging import V2BaseModelMessaging
from precompute_events import compute_mp5_cumsum, find_program_change_days
from datetime import date

# V7 модули
import rtc_state_transitions_v7
import rtc_quota_v7
import rtc_limiter_optimized
import rtc_limiter_v5
from components.agent_population import AgentPopulationBuilder


class LimiterV7Orchestrator:
    """Оркестратор LIMITER V7 — однофазная архитектура"""
    
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
        
        self.frames = 0
        self.days = 0
        self.mp5_cumsum = None
        self.program_change_days = []
        
    def prepare_data(self):
        """Подготовка данных"""
        print("\n" + "=" * 60)
        print(f"📊 LIMITER V7: Подготовка данных")
        print(f"   version_date: {self.version_date}")
        print(f"   end_day: {self.end_day}")
        print("=" * 60)
        
        client = get_client()
        self._client = client  # Сохраняем для использования в build_model
        vd = date.fromisoformat(self.version_date)
        self.env_data = prepare_env_arrays(client, vd)
        self.frames = int(self.env_data['frames_total_u16'])
        self.days = min(int(self.env_data['days_total_u16']), self.end_day + 1)
        
        print(f"\n✅ Данные: frames={self.frames}, days={self.days}")
        
        # MP5 cumsum
        print("\n📊 Вычисление mp5_cumsum...")
        t0 = time.perf_counter()
        import numpy as np
        mp5_lin = np.array(self.env_data.get('mp5_daily_hours_linear', []), dtype=np.uint32)
        self.mp5_cumsum = compute_mp5_cumsum(mp5_lin, self.frames, self.days)
        print(f"   mp5_cumsum: shape={self.mp5_cumsum.shape}, time={time.perf_counter()-t0:.2f}s")
        
        # Program changes
        print("\n📊 Поиск дней изменения программы...")
        mp4_mi8 = self.env_data.get('mp4_ops_counter_mi8', [])
        mp4_mi17 = self.env_data.get('mp4_ops_counter_mi17', [])
        program_changes = find_program_change_days(mp4_mi8, mp4_mi17)
        # Извлекаем только дни из tuples (day, target_mi8, target_mi17)
        self.program_change_days = [pc[0] for pc in program_changes if pc[0] <= self.end_day]
        
        # Добавляем end_day
        if self.end_day not in self.program_change_days:
            self.program_change_days.append(self.end_day)
        self.program_change_days = sorted(set(self.program_change_days))
        
        print(f"   program_changes: {len(self.program_change_days)} дней")
        
        # Population builder
        self.population_builder = AgentPopulationBuilder(
            self.env_data,
            mp5_cumsum=self.mp5_cumsum,
            end_day=self.end_day
        )
        
    def build_model(self):
        """Построение модели V7"""
        print("\n" + "=" * 60)
        print("🔧 LIMITER V7: Построение модели")
        print("=" * 60)
        
        # Base model
        self.base_model = V2BaseModelMessaging()
        self.model = self.base_model.create_model(self.env_data)
        
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
        
        # HF для инициализации mp5_cumsum
        hf_init_cumsum = HF_InitMP5Cumsum(self.mp5_cumsum, self.frames, self.days)
        layer_init = self.model.newLayer("layer_init_mp5_cumsum")
        layer_init.addHostFunction(hf_init_cumsum)
        
        # ═══════════════════════════════════════════════════════════════
        # V7: Однофазные переходы состояний
        # ═══════════════════════════════════════════════════════════════
        
        # Фаза -1 до 1: exit_date copy + Детерминированные переходы + Operations
        rtc_state_transitions_v7.register_all_v7(self.model, heli_agent, self.base_model.quota_agent)
        
        # Фаза 2: Квотирование (сброс, подсчёт, демоут, P1, P2, P3)
        rtc_quota_v7.register_quota_v7(self.model, heli_agent)
        
        # Фаза 3: Переходы после квотирования (demote, promote)
        rtc_state_transitions_v7.register_post_quota_v7(self.model, heli_agent)
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 3.5: Динамический спавн Mi-17 (после P3)
        # ═══════════════════════════════════════════════════════════════
        spawn_env_data = {
            'first_dynamic_idx': self.frames,  # После всех существующих агентов
            'dynamic_reserve_mi17': 50,
            'base_acn_spawn': 200000
        }
        self.spawn_data = rtc_spawn_dynamic_v7.register_spawn_dynamic_v7(
            self.model, heli_agent, spawn_env_data
        )
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 4: Limiter V3 (бинарный поиск)
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение V3 limiter...")
        rtc_limiter_optimized.setup_limiter_macroproperties(
            self.base_model.env,
            self.program_change_days  # список дней изменения программы
        )
        # V7: skip_decrement=True — декремент уже в rtc_ops_increment_v7 (один проход)
        rtc_limiter_optimized.register_limiter_optimized(
            self.model,
            heli_agent,
            skip_decrement=True
        )
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 5: V5 GPU-only adaptive
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение V5 100% GPU...")
        
        rtc_limiter_v5.setup_v5_macroproperties(self.base_model.env, self.program_change_days)
        
        self.base_model.quota_agent.newVariableUInt("computed_adaptive_days", 1)
        self.base_model.quota_agent.newVariableUInt("current_day_cache", 0)
        
        self.hf_init_v5, self.hf_sync_v5 = rtc_limiter_v5.register_v5(
            self.model,
            self.base_model.agent,
            self.base_model.quota_agent,
            self.program_change_days,
            self.end_day,
            verbose_logging=self.enable_mp2  # Логирование только при MP2
        )
        
        rtc_limiter_v5.register_v5_final_layers(
            self.model,
            self.base_model.agent,
            self.base_model.quota_agent
        )
        
        self.hf_exit = rtc_limiter_v5.HF_ExitCondition(self.end_day)
        self.model.addExitCondition(self.hf_exit)
        
        print("\n✅ Модель LIMITER V7 построена")
        print("=" * 60)
        
        return self.model
    
    def run(self, max_steps: int = 10000):
        """Запуск симуляции"""
        print("\n" + "=" * 60)
        print("🚀 LIMITER V7: Запуск симуляции")
        print(f"   max_steps: {max_steps}")
        print(f"   MP2 экспорт: {'✅' if self.enable_mp2 else '❌'}")
        print("=" * 60)
        
        t_start = time.perf_counter()
        
        # Создание симуляции
        self.simulation = fg.CUDASimulation(self.model)
        self.simulation.SimulationConfig().steps = max_steps
        
        # Инициализация агентов
        self._populate_agents()
        
        # Подготовка MP2
        mp2_rows = []
        vd = date.fromisoformat(self.version_date)
        version_date_int = vd.year * 10000 + vd.month * 100 + vd.day
        version_id = int(self.env_data.get('version_id_u32', 1))
        
        # Запуск — если MP2, то step() цикл, иначе simulate()
        if self.enable_mp2:
            print("\n🏃 Запуск step() цикл (для MP2 + логирование)...")
            print(f"   Датасет: {self.version_date}")
            print(f"   program_changes: {len(self.program_change_days)} дат")
            t_gpu_start = time.perf_counter()
            
            step_count = 0
            recorded_days = set()
            
            # 1. День 0 — начальное состояние (ДО первого step)
            rows = collect_agents_state(
                self.simulation, self.base_model.agent,
                0, version_date_int, version_id
            )
            mp2_rows.extend(rows)
            recorded_days.add(0)
            print(f"  [Step 0] day=0 (начальное состояние)")
            
            while self.simulation.step():
                step_count += 1
                
                # Читаем current_day из step_log (обновлён HF_SyncDayV5 в конце шага)
                step_log = self.hf_sync_v5.get_step_log()
                if step_log:
                    current_day = step_log[-1]['day']
                else:
                    current_day = 0
                
                # Записываем состояние КАЖДОГО шага (пропускаем если уже записан)
                if current_day not in recorded_days:
                    rows = collect_agents_state(
                        self.simulation, self.base_model.agent,
                        current_day, version_date_int, version_id
                    )
                    mp2_rows.extend(rows)
                    recorded_days.add(current_day)
                
                if step_count >= max_steps:
                    break
            
            # 3. Последний день — если не записан
            if self.end_day not in recorded_days:
                print(f"  [Final] day={self.end_day} (последний день)")
                rows = collect_agents_state(
                    self.simulation, self.base_model.agent,
                    self.end_day, version_date_int, version_id
                )
                mp2_rows.extend(rows)
                recorded_days.add(self.end_day)
            
            gpu_time = time.perf_counter() - t_gpu_start
            
            # Вывод лога шагов
            step_log = self.hf_sync_v5.get_step_log()
            print(f"\n📋 Лог шагов ({len(step_log)} записей):")
            
            # Статистика причин
            reason_counts = {}
            for entry in step_log:
                for r in entry['reasons']:
                    key = r.split(':')[0]  # limiter:N -> limiter
                    reason_counts[key] = reason_counts.get(key, 0) + 1
            
            print(f"   Причины шагов:")
            for reason, count in sorted(reason_counts.items()):
                print(f"     {reason}: {count}")
            
            # Шаги с несколькими причинами
            multi = [e for e in step_log if len(e['reasons']) > 1]
            if multi:
                print(f"\n   Шаги с несколькими причинами ({len(multi)}):")
                for e in multi[:10]:
                    print(f"     Step {e['step']}: day={e['day']}, reasons={e['reasons']}")
        else:
            print("\n🏃 Запуск simulate()...")
            t_gpu_start = time.perf_counter()
            self.simulation.simulate()
            gpu_time = time.perf_counter() - t_gpu_start
        
        # Результаты
        final_steps = self.simulation.getStepCounter()
        
        # MP2 export
        drain_time = 0.0
        
        if self.enable_mp2 and mp2_rows:
            unique_days = len(set(r['day_u16'] for r in mp2_rows))
            print(f"\n📤 Экспорт в СУБД: {len(mp2_rows)} строк, {unique_days} дней...")
            
            t_insert = time.perf_counter()
            columns = list(mp2_rows[0].keys())
            values = [[row[col] for col in columns] for row in mp2_rows]
            col_str = ', '.join(columns)
            self.clickhouse_client.execute(
                f"INSERT INTO sim_masterv2_v7 ({col_str}) VALUES",
                values
            )
            drain_time = time.perf_counter() - t_insert
            print(f"   ✅ INSERT: {len(mp2_rows)} строк ({drain_time:.2f}с)")
        
        t_end = time.perf_counter()
        total_time = t_end - t_start
        
        print(f"\n✅ Симуляция завершена:")
        print(f"   Шагов: {final_steps}")
        print(f"   end_day: {self.end_day}")
        print(f"   Время общее: {total_time:.2f}с")
        print(f"   Время GPU: {gpu_time:.2f}с")
        if self.enable_mp2:
            print(f"   Время drain: {drain_time:.2f}с")
        if gpu_time > 0:
            print(f"   Скорость: {self.end_day / gpu_time:.0f} дней/сек (GPU)")
            print(f"   Скорость: {self.end_day / total_time:.0f} дней/сек (общая)")
        
        self._print_final_stats()
        
        return self.end_day
    
    def _collect_mp2_final(self, rows: list):
        """Собирает финальное состояние агентов для экспорта"""
        states = ['inactive', 'operations', 'serviceable', 'repair', 'reserve', 'storage', 'unserviceable']
        
        vd = date.fromisoformat(self.version_date)
        version_date_int = vd.year * 10000 + vd.month * 100 + vd.day
        version_id = int(self.env_data.get('version_id_u32', 1))
        
        for state_name in states:
            heli_pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(heli_pop, state_name)
            
            for i in range(heli_pop.size()):
                agent = heli_pop.at(i)
                rows.append({
                    'version_date': version_date_int,
                    'version_id': version_id,
                    'day_u16': self.end_day,
                    'idx': agent.getVariableUInt('idx'),
                    'aircraft_number': agent.getVariableUInt('aircraft_number'),
                    'group_by': agent.getVariableUInt('group_by'),
                    'state': state_name,
                    'sne': agent.getVariableUInt('sne'),
                    'ppr': agent.getVariableUInt('ppr'),
                    'll': agent.getVariableUInt('ll'),
                    'oh': agent.getVariableUInt('oh'),
                    'br': agent.getVariableUInt('br'),
                    'repair_days': agent.getVariableUInt('repair_days'),
                    'repair_time': agent.getVariableUInt('repair_time'),
                })
    
    def _populate_agents(self):
        """Заполнение агентов из heli_pandas + spawn"""
        print("\n📦 Заполнение агентов...")
        
        # Планеры из heli_pandas
        self.population_builder.populate_agents(self.simulation, self.base_model.agent)
        
        # V7: Детерминированный spawn — создаём агентов в reserve с exit_date
        spawn_count = self._populate_spawn_agents()
        
        # QuotaManager агенты
        initial_ops = self.population_builder.get_initial_ops_count()
        mi8_ops = initial_ops.get(1, 0)
        mi17_ops = initial_ops.get(2, 0)
        
        qm_pop = fg.AgentVector(self.base_model.quota_agent, 2)
        qm_pop[0].setVariableUInt8("group_by", 1)  # Mi-8
        qm_pop[1].setVariableUInt8("group_by", 2)  # Mi-17
        self.simulation.setPopulationData(qm_pop)
        
        # Динамический спавн (менеджер + тикеты)
        if hasattr(self, 'spawn_data') and self.spawn_data:
            rtc_spawn_dynamic_v7.init_spawn_dynamic_population_v7(
                self.simulation,
                self.model,
                self.spawn_data['first_dynamic_idx'],
                self.spawn_data['dynamic_reserve'],
                self.spawn_data['base_acn']
            )
        
        print(f"   ✅ Агенты загружены: Mi-8 ops={mi8_ops}, Mi-17 ops={mi17_ops}, spawn={spawn_count}")
    
    def _populate_spawn_agents(self) -> int:
        """V7: Создаём агентов для детерминированного спавна в reserve"""
        spawn_seed = self.env_data.get('mp4_new_counter_mi17_seed', [])
        
        # Находим дни со спавном
        spawn_events = []  # [(day, count), ...]
        for day, count in enumerate(spawn_seed):
            if count > 0:
                spawn_events.append((day, count))
        
        if not spawn_events:
            return 0
        
        # Получаем параметры для Mi-17 (spawn только Mi-17)
        mi17_ll = int(self.env_data.get('mi17_ll_const', 270000))
        mi17_oh = int(self.env_data.get('mi17_oh_const', 270000))
        mi17_br = int(self.env_data.get('mi17_br_const', 210000))
        mi17_repair_time = int(self.env_data.get('mi17_repair_time_const', 180))
        mi17_assembly_time = int(self.env_data.get('mi17_assembly_time_const', 30))
        mi17_partout_time = int(self.env_data.get('mi17_partout_time_const', 20))
        
        # Стартовый idx для spawn (после всех существующих агентов)
        first_reserved_idx = int(self.env_data.get('first_reserved_idx', 279))
        next_idx = first_reserved_idx
        base_acn = 100000  # Начинаем aircraft_number с 100000
        
        total_spawn = 0
        spawn_agents = []
        
        for spawn_day, count in spawn_events:
            for i in range(count):
                agent_data = {
                    'idx': next_idx,
                    'aircraft_number': base_acn,
                    'group_by': 2,  # Mi-17
                    'sne': 0,
                    'ppr': 0,
                    'll': mi17_ll,
                    'oh': mi17_oh,
                    'br': mi17_br,
                    'repair_time': mi17_repair_time,
                    'assembly_time': mi17_assembly_time,
                    'partout_time': mi17_partout_time,
                    'exit_date': spawn_day,  # V7: день активации!
                    'limiter': 0,
                }
                spawn_agents.append(agent_data)
                next_idx += 1
                base_acn += 1
                total_spawn += 1
        
        if spawn_agents:
            # Создаём популяцию в reserve
            pop = fg.AgentVector(self.base_model.agent, len(spawn_agents))
            
            for i, data in enumerate(spawn_agents):
                agent = pop[i]
                agent.setVariableUInt("idx", data['idx'])
                agent.setVariableUInt("aircraft_number", data['aircraft_number'])
                agent.setVariableUInt("group_by", data['group_by'])
                agent.setVariableUInt("sne", data['sne'])
                agent.setVariableUInt("ppr", data['ppr'])
                agent.setVariableUInt("ll", data['ll'])
                agent.setVariableUInt("oh", data['oh'])
                agent.setVariableUInt("br", data['br'])
                agent.setVariableUInt("repair_time", data['repair_time'])
                agent.setVariableUInt("assembly_time", data['assembly_time'])
                agent.setVariableUInt("partout_time", data['partout_time'])
                agent.setVariableUInt("exit_date", data['exit_date'])
                agent.setVariableUInt16("limiter", 0)
                agent.setVariableUInt("repair_days", 0)
                agent.setVariableUInt("daily_today_u32", 0)
                agent.setVariableUInt("daily_next_u32", 0)
                # Transition flags
                agent.setVariableUInt("transition_5_to_2", 0)
                # V7 flags
                agent.setVariableUInt("promoted", 0)
                agent.setVariableUInt("needs_demote", 0)
            
            self.simulation.setPopulationData(pop, "reserve")
            
            spawn_days = sorted(set(d for d, _ in spawn_events))
            print(f"   📦 Spawn: {total_spawn} агентов в reserve, exit_dates={spawn_days}")
        
        return total_spawn
    
    def _print_final_stats(self):
        """Вывод финальной статистики"""
        print("\n📊 Финальная статистика:")
        
        states = ["inactive", "operations", "serviceable", "repair", "reserve", "storage", "unserviceable"]
        total = 0
        for state in states:
            heli_pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(heli_pop, state)
            count = heli_pop.size()
            total += count
            print(f"   {state}: {count}")
        print(f"   -----------")
        print(f"   ВСЕГО: {total}")


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
        
        # Копируем данные (flat array → flat MacroProperty)
        # mp5_cumsum уже в формате day * frames + frame
        for i in range(min(len(self.mp5_cumsum), len(mp))):
            mp[i] = int(self.mp5_cumsum[i])
        
        # Инициализация mp_min_limiter
        mp_min = FLAMEGPU.environment.getMacroPropertyUInt32("mp_min_limiter")
        mp_min[0] = 0xFFFFFFFF
        
        self.initialized = True
        print(f"  [HF_InitMP5Cumsum] ✅ Загружено")


def main():
    parser = argparse.ArgumentParser(description="LIMITER V7 Orchestrator")
    parser.add_argument("--version-date", required=True, help="Дата датасета (YYYY-MM-DD)")
    parser.add_argument("--end-day", type=int, default=3650, help="Последний день симуляции")
    parser.add_argument("--max-steps", type=int, default=10000, help="Максимум шагов")
    parser.add_argument("--enable-mp2", action="store_true", help="Экспорт в СУБД")
    parser.add_argument("--drop-table", action="store_true", help="Пересоздать таблицу")
    
    args = parser.parse_args()
    
    print("\n" + "=" * 70)
    print("🚀 LIMITER V7 — Однофазная архитектура")
    print("=" * 70)
    
    # Подключение к ClickHouse если нужен MP2
    client = None
    if args.enable_mp2:
        from sim_env_setup import get_client
        client = get_client()
        
        if args.drop_table:
            print("🗑️ DROP TABLE sim_masterv2_v7...")
            client.execute("DROP TABLE IF EXISTS sim_masterv2_v7")
        
        # Создание таблицы
        client.execute("""
            CREATE TABLE IF NOT EXISTS sim_masterv2_v7 (
                version_date UInt32,
                version_id UInt8,
                day_u16 UInt16,
                idx UInt16,
                aircraft_number UInt32,
                group_by UInt8,
                state String,
                sne UInt32,
                ppr UInt32,
                ll UInt32,
                oh UInt32,
                br UInt32,
                repair_days UInt16,
                repair_time UInt16
            ) ENGINE = MergeTree()
            ORDER BY (version_date, version_id, day_u16, idx)
        """)
        print("✅ Таблица sim_masterv2_v7 готова")
    
    orchestrator = LimiterV7Orchestrator(
        args.version_date, 
        args.end_day,
        enable_mp2=args.enable_mp2,
        clickhouse_client=client
    )
    orchestrator.prepare_data()
    orchestrator.build_model()
    orchestrator.run(args.max_steps)


if __name__ == "__main__":
    main()

