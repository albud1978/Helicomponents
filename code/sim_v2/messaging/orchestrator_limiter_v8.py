#!/usr/bin/env python3
"""
LIMITER V8 Orchestrator — Упрощённая архитектура adaptive steps

Архитектура V8:
- ОДИН MacroProperty с детерминированными датами (program_changes + repair_exits + spawns)
- Декремент limiter для ops/repair/unserviceable
- Пересчёт limiter ТОЛЬКО при входе в operations
- repair_days для unserviceable как счётчик до права на вход

Валидация:
  Количество динамических шагов ≈ 183 (baseline ops→storage/repair)

Дата: 15.01.2026
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

# V8 модули
import rtc_limiter_v8
import rtc_limiter_optimized  # Для бинарного поиска limiter
import rtc_state_transitions_v7  # V7 переходы состояний
import rtc_quota_v7  # V7 квотирование
from sim_env_setup import get_client, prepare_env_arrays
from base_model_messaging import V2BaseModelMessaging
from precompute_events import compute_mp5_cumsum, find_program_change_days
from datetime import date
from components.agent_population import AgentPopulationBuilder


class LimiterV8Orchestrator:
    """Оркестратор LIMITER V8"""
    
    def __init__(self, version_date: str, end_day: int = 3650, verbose: bool = False):
        self.version_date = version_date
        self.end_day = end_day
        self.verbose = verbose
        
        self.model = None
        self.simulation = None
        self.base_model = None
        self.env_data = None
        
        self.frames = 0
        self.days = 0
        self.mp5_cumsum = None
        self.deterministic_dates = []
        self.program_change_days = []
        
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
        self.program_change_days = [pc[0] for pc in program_changes if pc[0] <= self.end_day]
        print(f"   program_changes: {len(self.program_change_days)} дней")
        
        # Repair exits — вычисляем из агентов в repair на загрузке
        repair_exits = self._compute_repair_exits()
        print(f"   repair_exits: {len(repair_exits)} дней")
        
        # Spawn exits — из mp4_new_counter_mi17_seed
        spawn_exits = self._compute_spawn_exits()
        print(f"   spawn_exits: {len(spawn_exits)} дней")
        
        # Объединяем все детерминированные даты
        self.deterministic_dates = sorted(set(
            [0] +  # День 0
            self.program_change_days +
            repair_exits +
            spawn_exits +
            [self.end_day]  # Последний день
        ))
        print(f"\n✅ Детерминированные даты: {len(self.deterministic_dates)}")
        print(f"   Первые: {self.deterministic_dates[:10]}")
        print(f"   Последние: {self.deterministic_dates[-5:]}")
        
        # Population builder
        self.population_builder = AgentPopulationBuilder(
            self.env_data,
            mp5_cumsum=self.mp5_cumsum,
            end_day=self.end_day
        )
    
    def _compute_repair_exits(self) -> list:
        """Вычисляет даты выхода из ремонта для агентов в repair на загрузке"""
        # Получаем repair_time и repair_days из heli_pandas
        hp_repair_time = self.env_data.get('hp_repair_time', [])
        hp_repair_days = self.env_data.get('hp_repair_days', [])
        hp_status = self.env_data.get('hp_status_id', [])
        
        repair_exits = []
        for i, status in enumerate(hp_status):
            if status == 4:  # repair
                repair_time = hp_repair_time[i] if i < len(hp_repair_time) else 180
                repair_days = hp_repair_days[i] if i < len(hp_repair_days) else 0
                exit_day = repair_time - repair_days
                if exit_day > 0 and exit_day <= self.end_day:
                    repair_exits.append(exit_day)
        
        return sorted(set(repair_exits))
    
    def _compute_spawn_exits(self) -> list:
        """Вычисляет даты spawn из mp4_new_counter_mi17_seed"""
        spawn_seed = self.env_data.get('mp4_new_counter_mi17_seed', [])
        spawn_exits = []
        for day, count in enumerate(spawn_seed):
            if count > 0 and day <= self.end_day:
                spawn_exits.append(day)
        return sorted(set(spawn_exits))
    
    def build_model(self):
        """Построение модели V8"""
        print("\n" + "=" * 60)
        print("🔧 LIMITER V8: Построение модели")
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
        # mp5_cumsum MacroProperty (для бинарного поиска limiter)
        # ═══════════════════════════════════════════════════════════════
        cumsum_size = model_build.RTC_MAX_FRAMES * (model_build.MAX_DAYS + 1)
        self.base_model.env.newMacroPropertyUInt32("mp5_cumsum", cumsum_size)
        
        hf_init_cumsum = HF_InitMP5Cumsum(self.mp5_cumsum, self.frames, self.days)
        layer_init = self.model.newLayer("layer_init_mp5_cumsum")
        layer_init.addHostFunction(hf_init_cumsum)
        
        # ═══════════════════════════════════════════════════════════════
        # V8 MacroProperty (детерминированные даты)
        # ═══════════════════════════════════════════════════════════════
        rtc_limiter_v8.setup_v8_macroproperties(
            self.base_model.env,
            self.deterministic_dates,
            self.end_day
        )
        
        # ═══════════════════════════════════════════════════════════════
        # QuotaManager агентные переменные для избежания race condition
        # ═══════════════════════════════════════════════════════════════
        self.base_model.quota_agent.newVariableUInt("computed_adaptive_days", 1)
        self.base_model.quota_agent.newVariableUInt("current_day_cache", 0)
        
        # ═══════════════════════════════════════════════════════════════
        # V7 переходы состояний (ПЕРЕД адаптивными шагами)
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 V7 State Transitions...")
        
        # Фаза 0: Детерминированные переходы (repair→svc, spawn→ops)
        rtc_state_transitions_v7.register_phase0_deterministic(self.model, heli_agent)
        
        # Фаза 0.5: Копирование exit_date
        rtc_state_transitions_v7.register_exit_date_copy(
            self.model, heli_agent, self.base_model.quota_agent
        )
        
        # Фаза 1: Operations — инкременты и переходы (ops→storage, ops→unsvc)
        rtc_state_transitions_v7.register_phase1_operations(self.model, heli_agent)
        
        # Квотирование V7
        rtc_quota_v7.register_quota_v7(self.model, heli_agent)
        
        # Фазы 2-3: Демоут и промоуты
        rtc_state_transitions_v7.register_post_quota_v7(self.model, heli_agent)
        
        # ═══════════════════════════════════════════════════════════════
        # Limiter вычисление при входе в ops (бинарный поиск)
        # ═══════════════════════════════════════════════════════════════
        rtc_limiter_optimized.setup_limiter_macroproperties(
            self.base_model.env,
            self.program_change_days
        )
        
        fn_entry = heli_agent.newRTCFunction(
            "rtc_compute_limiter_on_entry", 
            rtc_limiter_optimized.RTC_COMPUTE_LIMITER_ON_ENTRY
        )
        fn_entry.setInitialState("operations")
        fn_entry.setEndState("operations")
        layer_entry = self.model.newLayer("L_limiter_entry_v8")
        layer_entry.addAgentFunction(fn_entry)
        
        # ═══════════════════════════════════════════════════════════════
        # V8 слои adaptive steps
        # ═══════════════════════════════════════════════════════════════
        self.hf_init_v8, self.hf_sync_v8, self.hf_exit_v8 = rtc_limiter_v8.register_v8_layers(
            self.model,
            heli_agent,
            self.base_model.quota_agent,
            self.deterministic_dates,
            self.end_day,
            verbose=self.verbose
        )
        
        print("\n✅ Модель LIMITER V8 построена")
        print("=" * 60)
        
        return self.model
    
    def run(self, max_steps: int = 10000):
        """Запуск симуляции"""
        print("\n" + "=" * 60)
        print("🚀 LIMITER V8: Запуск симуляции")
        print(f"   max_steps: {max_steps}")
        print(f"   verbose: {self.verbose}")
        print("=" * 60)
        
        t_start = time.perf_counter()
        
        # Создание симуляции
        self.simulation = fg.CUDASimulation(self.model)
        self.simulation.SimulationConfig().steps = max_steps
        
        # Инициализация агентов
        self._populate_agents()
        
        # Запуск
        print("\n🏃 Запуск simulate()...")
        t_gpu_start = time.perf_counter()
        self.simulation.simulate()
        gpu_time = time.perf_counter() - t_gpu_start
        
        # Результаты
        final_steps = self.simulation.getStepCounter()
        
        t_end = time.perf_counter()
        total_time = t_end - t_start
        
        print(f"\n✅ Симуляция завершена:")
        print(f"   Шагов: {final_steps}")
        print(f"   end_day: {self.end_day}")
        print(f"   Время GPU: {gpu_time:.2f}с")
        print(f"   Время общее: {total_time:.2f}с")
        if gpu_time > 0:
            print(f"   Скорость: {self.end_day / gpu_time:.0f} дней/сек (GPU)")
        
        # Статистика адаптивных шагов
        step_log = self.hf_sync_v8.get_step_log()
        dynamic_steps = self.hf_sync_v8.get_dynamic_steps_count()
        
        print(f"\n📊 Статистика адаптивных шагов:")
        print(f"   Всего шагов: {len(step_log)}")
        print(f"   Динамических (limiter=0): {dynamic_steps}")
        print(f"   Ожидаемо (baseline): ~183")
        
        # Причины шагов
        reason_counts = {}
        for entry in step_log:
            for r in entry['reasons']:
                key = r.split(':')[0]
                reason_counts[key] = reason_counts.get(key, 0) + 1
        
        print(f"   Причины шагов:")
        for reason, count in sorted(reason_counts.items()):
            print(f"     {reason}: {count}")
        
        self._print_final_stats()
        
        return final_steps, dynamic_steps
    
    def _populate_agents(self):
        """Заполнение агентов"""
        print("\n📦 Заполнение агентов...")
        
        # Планеры из heli_pandas
        self.population_builder.populate_agents(self.simulation, self.base_model.agent)
        
        # QuotaManager агенты
        qm_pop = fg.AgentVector(self.base_model.quota_agent, 2)
        qm_pop[0].setVariableUInt8("group_by", 1)  # Mi-8
        qm_pop[1].setVariableUInt8("group_by", 2)  # Mi-17
        self.simulation.setPopulationData(qm_pop)
        
        initial_ops = self.population_builder.get_initial_ops_count()
        mi8_ops = initial_ops.get(1, 0)
        mi17_ops = initial_ops.get(2, 0)
        
        print(f"   ✅ Агенты загружены: Mi-8 ops={mi8_ops}, Mi-17 ops={mi17_ops}")
    
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
        
        for i in range(min(len(self.mp5_cumsum), len(mp))):
            mp[i] = int(self.mp5_cumsum[i])
        
        self.initialized = True
        print(f"  [HF_InitMP5Cumsum] ✅ Загружено")


def main():
    parser = argparse.ArgumentParser(description="LIMITER V8 Orchestrator")
    parser.add_argument("--version-date", required=True, help="Дата датасета (YYYY-MM-DD)")
    parser.add_argument("--end-day", type=int, default=3650, help="Последний день симуляции")
    parser.add_argument("--max-steps", type=int, default=10000, help="Максимум шагов")
    parser.add_argument("--verbose", action="store_true", help="Подробное логирование")
    
    args = parser.parse_args()
    
    print("\n" + "=" * 70)
    print("🚀 LIMITER V8 — Упрощённая архитектура adaptive steps")
    print("=" * 70)
    
    orchestrator = LimiterV8Orchestrator(
        args.version_date,
        args.end_day,
        verbose=args.verbose
    )
    orchestrator.prepare_data()
    orchestrator.build_model()
    
    final_steps, dynamic_steps = orchestrator.run(args.max_steps)
    
    print("\n" + "=" * 70)
    print("📋 РЕЗУЛЬТАТ ВАЛИДАЦИИ:")
    print(f"   Динамических шагов (limiter=0): {dynamic_steps}")
    print(f"   Ожидаемо (baseline):            ~183")
    print(f"   Разница:                        {abs(dynamic_steps - 183)}")
    print("=" * 70)


if __name__ == "__main__":
    main()

