#!/usr/bin/env python3
"""
LIMITER V5 Orchestrator — 100% GPU-ONLY (как Adaptive 2.0)

V5 АРХИТЕКТУРА:
- ✅ current_day в MacroProperty (не Environment)
- ✅ adaptive_days вычисляется на GPU (rtc_compute_global_min_v5)
- ✅ current_day обновляется на GPU (rtc_update_day_v5)
- ✅ 1 sync callback (HF_SyncDayV5) для совместимости с существующими RTC
- ✅ simulate(N) без exit condition (early return в RTC)

ПРОИЗВОДИТЕЛЬНОСТЬ:
  Adaptive 2.0: 1.44с (6 модулей, 100% GPU)
  V5: ~2с цель (20 модулей, 100% GPU с sync)

Запуск:
    python3 orchestrator_limiter_v5.py --version-date 2025-07-04 --end-day 3650

База: V5 + GPU-only current_day
"""
import os
import sys
import argparse
import time
from typing import Dict, List
from datetime import date as dt_date

# Добавляем пути для импорта
_MESSAGING_DIR = os.path.dirname(__file__)
_SIM_V2_DIR = os.path.join(_MESSAGING_DIR, '..')
_CODE_DIR = os.path.join(_SIM_V2_DIR, '..')

sys.path.insert(0, _MESSAGING_DIR)
sys.path.insert(0, _SIM_V2_DIR)
sys.path.insert(0, _CODE_DIR)

from sim_env_setup import get_client, prepare_env_arrays
from base_model_messaging import V2BaseModelMessaging
from precompute_events import compute_mp5_cumsum
import rtc_limiter_date
import rtc_limiter_optimized
import messaging.rtc_limiter_v5 as rtc_v5  # V5: 100% GPU
import model_build

from components.agent_population import AgentPopulationBuilder

try:
    import pyflamegpu as fg
    import numpy as np
except ImportError as e:
    raise RuntimeError(f"Зависимости не установлены: {e}")


class HF_InitMP5Cumsum(fg.HostFunction):
    """HostFunction для инициализации mp5_cumsum MacroProperty"""
    
    def __init__(self, cumsum_data: np.ndarray, frames: int, days: int):
        super().__init__()
        self.data = cumsum_data
        self.frames = frames
        self.days = days
        self.initialized = False
    
    def run(self, FLAMEGPU):
        if self.initialized:
            return
        
        step = FLAMEGPU.getStepCounter()
        if step > 0:
            return
        
        # Инициализируем mp_min_limiter = MAX
        try:
            mp_min = FLAMEGPU.environment.getMacroPropertyUInt("mp_min_limiter")
            mp_min[0] = 0xFFFFFFFF
        except:
            pass
        
        mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp5_cumsum")
        
        MAX_FRAMES = 400
        MAX_DAYS_PLUS_1 = 4001
        
        print(f"HF_InitMP5Cumsum: Инициализация для FRAMES={self.frames}, DAYS={self.days}")
        
        count = 0
        for f in range(self.frames):
            for d in range(self.days + 1):
                src_idx = f * (self.days + 1) + d
                dst_idx = f * MAX_DAYS_PLUS_1 + d
                if src_idx < len(self.data):
                    mp[dst_idx] = int(self.data[src_idx])
                    count += 1
        
        print(f"HF_InitMP5Cumsum: Инициализировано {count} элементов")
        self.initialized = True


class LimiterV5Orchestrator:
    """V5 Оркестратор с GPU-only адаптивными шагами"""
    
    def __init__(self, env_data: Dict, end_day: int = 3650,
                 enable_mp2: bool = False, clickhouse_client=None,
                 version_date_str: str = "2025-07-04"):
        self.env_data = env_data
        self.end_day = end_day
        self.enable_mp2 = enable_mp2
        self.clickhouse_client = clickhouse_client
        self.version_date_str = version_date_str
        
        self.base_model = V2BaseModelMessaging()
        self.model = None
        self.simulation = None
        
        self.frames = int(env_data['frames_total_u16'])
        self.days = int(env_data.get('days_total_u16', 3650))
        
        # Предрасчёт program_change_days
        print("\n📊 Предрасчёт program_change_days...")
        self.program_change_days = rtc_limiter_date.precompute_program_changes(
            clickhouse_client, version_date_str
        )
        if self.end_day not in self.program_change_days:
            self.program_change_days.append(self.end_day)
            self.program_change_days.sort()
        
        # MP5 cumsum
        print("📊 Загрузка MP5 данных...")
        mp5_raw = np.array(env_data.get('mp5_daily_hours_linear', []), dtype=np.uint32)
        expected_lin_size = self.frames * self.days
        if len(mp5_raw) >= expected_lin_size:
            mp5_for_cumsum = mp5_raw[:expected_lin_size]
            self.mp5_cumsum = compute_mp5_cumsum(mp5_for_cumsum, self.frames, self.days)
            print(f"  ✅ mp5_cumsum: {len(self.mp5_cumsum)} элементов")
        else:
            self.mp5_cumsum = np.zeros(self.frames * (self.days + 1), dtype=np.uint32)
        
        self.population_builder = AgentPopulationBuilder(
            env_data,
            mp5_cumsum=self.mp5_cumsum,
            end_day=self.end_day
        )
        
        from components.mp5_strategy import HostOnlyMP5Strategy
        self.mp5_strategy = HostOnlyMP5Strategy(env_data, self.frames, self.days)
    
    def build_model(self):
        """Строит модель с V5 GPU-only архитектурой"""
        
        print("\n" + "=" * 60)
        print("🔧 Построение модели LIMITER V5 (GPU-only)")
        print("=" * 60)
        
        self.model = self.base_model.create_model(self.env_data)
        
        # Environment properties
        self.base_model.env.newPropertyUInt("end_day", self.end_day)
        
        # V3 MacroProperties (limiter) — используем как базу для V4
        rtc_limiter_date.setup_limiter_macroproperties(
            self.base_model.env, 
            self.program_change_days
        )
        
        # mp5_cumsum
        cumsum_size = model_build.RTC_MAX_FRAMES * (model_build.MAX_DAYS + 1)
        self.base_model.env.newMacroPropertyUInt32("mp5_cumsum", cumsum_size)
        
        # HostFunction для инициализации
        hf_init_cumsum = HF_InitMP5Cumsum(self.mp5_cumsum, self.frames, self.days)
        layer_init = self.model.newLayer("layer_init_mp5_cumsum")
        layer_init.addHostFunction(hf_init_cumsum)
        
        # mp5_lin
        self.mp5_strategy.register(self.model)
        
        heli_agent = self.base_model.agent
        
        # V5: Здесь НЕ регистрируем HF — он будет в конце (после limiter)
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 1: Ежедневные инкременты
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение state_2_operations + states_stub_v2...")
        import rtc_state_2_operations
        import rtc_states_stub_v2
        rtc_state_2_operations.register_rtc(self.model, heli_agent)
        rtc_states_stub_v2.register_rtc(self.model, heli_agent)
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 2: Квотирование
        # ═══════════════════════════════════════════════════════════════
        print("\n📊 Подключение квотирования...")
        self._register_quota_modules()
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 3: State managers
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение state managers...")
        self._register_state_managers()
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 4: Spawn
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение spawn...")
        self._register_spawn()
        self._register_spawn_dynamic()
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 5: Limiter (V3 оптимизированный)
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение V3 limiter (без HF_ComputeAdaptiveDays!)...")
        self._register_limiter_v3_rtc_only()
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 6: V5 100% GPU-only (замена HF на RTC)
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение V5 100% GPU...")
        
        # V5 MacroProperty и RTC
        rtc_v5.setup_v5_macroproperties(self.base_model.env, self.program_change_days)
        
        # computed_adaptive_days и current_day_cache для QuotaManager
        self.base_model.quota_agent.newVariableUInt("computed_adaptive_days", 1)
        self.base_model.quota_agent.newVariableUInt("current_day_cache", 0)
        
        # Регистрация V5 слоёв (copy_limiter + compute_min + sync)
        self.hf_init_v5, self.hf_sync_v5 = rtc_v5.register_v5(
            self.model,
            self.base_model.agent,
            self.base_model.quota_agent,
            self.program_change_days,
            self.end_day
        )
        
        # V5: Финальные слои (save_adaptive + update_day)
        rtc_v5.register_v5_final_layers(
            self.model,
            self.base_model.agent,
            self.base_model.quota_agent
        )
        
        # V5: Exit condition для остановки simulate()
        self.hf_exit = rtc_v5.HF_ExitCondition(self.end_day)
        self.model.addExitCondition(self.hf_exit)
        
        print("\n✅ Модель LIMITER V5 (GPU-only) построена")
        print("=" * 60)
        
        return self.model
    
    def _register_quota_modules(self):
        """Регистрирует модули квотирования"""
        import rtc_quota_count_ops
        import rtc_quota_ops_excess
        import rtc_quota_promote_serviceable
        import rtc_quota_promote_reserve
        import rtc_quota_promote_inactive
        
        rtc_quota_count_ops.register_rtc(self.model, self.base_model.agent)
        rtc_quota_ops_excess.register_rtc(self.model, self.base_model.agent)
        rtc_quota_promote_serviceable.register_rtc(self.model, self.base_model.agent)
        rtc_quota_promote_reserve.register_rtc(self.model, self.base_model.agent)
        rtc_quota_promote_inactive.register_rtc(self.model, self.base_model.agent)
        
        print("  ✅ Квотирование подключено")
    
    def _register_state_managers(self):
        """Регистрирует state managers"""
        import rtc_state_manager_serviceable
        import rtc_state_manager_operations
        import rtc_state_manager_unserviceable
        import rtc_state_manager_storage
        import rtc_state_manager_reserve
        import rtc_state_manager_inactive
        
        rtc_state_manager_serviceable.register_rtc(self.model, self.base_model.agent)
        rtc_state_manager_operations.register_state_manager_operations(self.model, self.base_model.agent)
        rtc_state_manager_unserviceable.register_state_manager_unserviceable(self.model, self.base_model.agent)
        rtc_state_manager_storage.register_state_manager_storage(self.model, self.base_model.agent)
        rtc_state_manager_reserve.register_state_manager_reserve(self.model, self.base_model.agent)
        rtc_state_manager_inactive.register_state_manager_inactive(self.model, self.base_model.agent)
        
        print("  ✅ State managers подключены")
    
    def _register_spawn(self):
        """Регистрирует spawn_v2"""
        from rtc_modules import rtc_spawn_v2
        rtc_spawn_v2.register_rtc(self.model, self.base_model.agent, self.env_data)
        print("  ✅ Spawn_v2 зарегистрирован")
    
    def _register_spawn_dynamic(self):
        """Регистрирует spawn_dynamic"""
        from rtc_modules import rtc_spawn_dynamic
        rtc_spawn_dynamic.register_rtc(self.model, self.base_model.agent, self.env_data)
        print("  ✅ Spawn_dynamic зарегистрирован")
    
    def _register_limiter_v3_rtc_only(self):
        """V5: Регистрирует V3 limiter RTC БЕЗ HF_ComputeAdaptiveDays"""
        
        # Setup MacroProperty
        rtc_limiter_optimized.setup_limiter_macroproperties(
            self.base_model.env,
            self.program_change_days
        )
        
        # InitFunction для mp_min_limiter
        self.hf_init_min_limiter = rtc_limiter_optimized.HF_InitMinLimiter()
        self.model.addInitFunction(self.hf_init_min_limiter)
        
        # RTC функции (БЕЗ HF_ComputeAdaptiveDays!)
        rtc_limiter_optimized.register_limiter_optimized(self.model, self.base_model.agent)
        
        # ❌ V5: НЕ регистрируем HF_ComputeAdaptiveDays
        # Вместо него используем rtc_compute_adaptive_gpu + rtc_update_day_gpu
        
        print("  ✅ V3 Limiter RTC (без HF) зарегистрирован")
    
    def create_simulation(self):
        """Создаёт симуляцию"""
        print("\n🚀 Создание симуляции...")
        
        self.simulation = fg.CUDASimulation(self.model)
        
        self.population_builder.populate_agents(self.simulation, self.base_model.agent)
        self._populate_quota_managers()
        self._init_spawn()
        self._init_spawn_dynamic()
        
        print("  ✅ Симуляция создана")
        return self.simulation
    
    def _init_spawn(self):
        """Инициализирует spawn_v2"""
        from rtc_modules import rtc_spawn_v2
        rtc_spawn_v2.initialize_spawn_population(
            self.simulation, self.model, self.env_data
        )
        print("  ✅ Spawn_v2 инициализирован")
    
    def _init_spawn_dynamic(self):
        """Инициализирует spawn_dynamic"""
        from rtc_modules import rtc_spawn_dynamic
        rtc_spawn_dynamic.init_population(
            self.simulation, self.model, self.env_data
        )
        print("  ✅ Spawn_dynamic инициализирован")
    
    def _populate_quota_managers(self):
        """Создаёт QuotaManager агентов"""
        initial_ops = self.population_builder.get_initial_ops_count()
        mi8_ops = initial_ops.get(1, 0)
        mi17_ops = initial_ops.get(2, 0)
        
        quota_pop = fg.AgentVector(self.base_model.quota_agent)
        
        quota_pop.push_back()
        mi8_mgr = quota_pop[len(quota_pop) - 1]
        mi8_mgr.setVariableUInt8("group_by", 1)
        mi8_mgr.setVariableUInt("target", 0)
        mi8_mgr.setVariableUInt("current", mi8_ops)
        mi8_mgr.setVariableInt("balance", 0)
        mi8_mgr.setVariableUInt("remaining_deficit", 0)
        
        quota_pop.push_back()
        mi17_mgr = quota_pop[len(quota_pop) - 1]
        mi17_mgr.setVariableUInt8("group_by", 2)
        mi17_mgr.setVariableUInt("target", 0)
        mi17_mgr.setVariableUInt("current", mi17_ops)
        mi17_mgr.setVariableInt("balance", 0)
        mi17_mgr.setVariableUInt("remaining_deficit", 0)
        
        self.simulation.setPopulationData(quota_pop)
        print(f"  ✅ QuotaManager: Mi-8 (curr={mi8_ops}), Mi-17 (curr={mi17_ops})")
    
    def run(self, max_steps: int = 10000):
        """V5: Запускает симуляцию с GPU-only адаптивными шагами"""
        
        print(f"\n▶️  Запуск LIMITER V5 симуляции (end_day={self.end_day})")
        print(f"  MP2 экспорт: {'✅' if self.enable_mp2 else '❌'}")
        print(f"  Режим: GPU-ONLY адаптивные шаги (simulate())")
        
        t_start = time.perf_counter()
        
        # Инициализируем Environment
        self.simulation.setEnvironmentPropertyUInt("current_day", 0)
        self.simulation.setEnvironmentPropertyUInt("prev_day", 0)
        self.simulation.setEnvironmentPropertyUInt("adaptive_days", 1)
        self.simulation.setEnvironmentPropertyUInt("step_days", 1)
        self.simulation.setEnvironmentPropertyUInt("quota_enabled", 1)
        
        # ═══════════════════════════════════════════════════════════════
        # V5: ОДИН ВЫЗОВ simulate() — минимум Python overhead
        # ═══════════════════════════════════════════════════════════════
        
        # ═══════════════════════════════════════════════════════════════
        # V5 GPU-ONLY: simulate() — архитектурно чище
        # ═══════════════════════════════════════════════════════════════
        
        # Оценка шагов: ~200/год × 10 лет + запас (с учётом ежедневных шагов в начале)
        estimated_steps = int(self.end_day / 365 * 200) + 100
        self.simulation.SimulationConfig().steps = estimated_steps
        
        t_gpu = time.perf_counter()
        self.simulation.simulate()
        total_gpu_time = time.perf_counter() - t_gpu
        
        # Получаем результаты
        current_day = self.simulation.getEnvironmentPropertyUInt("current_day")
        step_count = self.simulation.getStepCounter()
        
        # MP2 export (финальное состояние)
        total_collect_time = 0.0
        all_mp2_rows = []
        
        if self.enable_mp2:
            t_collect = time.perf_counter()
            self._collect_mp2_day(all_mp2_rows, current_day)
            total_collect_time = time.perf_counter() - t_collect
        
        # INSERT в СУБД
        total_drain_time = total_collect_time
        if self.enable_mp2 and all_mp2_rows:
            t_insert = time.perf_counter()
            columns = list(all_mp2_rows[0].keys())
            values = [[row[col] for col in columns] for row in all_mp2_rows]
            col_str = ', '.join(columns)
            self.clickhouse_client.execute(
                f"INSERT INTO sim_masterv2_limiter ({col_str}) VALUES",
                values
            )
            total_drain_time += time.perf_counter() - t_insert
            print(f"  ✅ Финальный INSERT: {len(all_mp2_rows)} строк")
        
        t_end = time.perf_counter()
        elapsed = t_end - t_start
        
        print(f"\n✅ LIMITER V5 симуляция завершена:")
        print(f"  • Шагов: {step_count}")
        print(f"  • Дней: {current_day} / {self.end_day}")
        print(f"  • Время общее: {elapsed:.2f}с")
        print(f"  • Время GPU: {total_gpu_time:.2f}с ({100*total_gpu_time/elapsed:.1f}%)")
        print(f"  • Время drain: {total_drain_time:.2f}с")
        print(f"  • Дней/сек: {current_day / elapsed:.1f}")
        if self.enable_mp2:
            print(f"  • Строк выгружено: {len(all_mp2_rows)}")
    
    def _collect_mp2_day(self, all_rows: list, day: int) -> int:
        """Собирает MP2 данные за день"""
        states = ['inactive', 'operations', 'serviceable', 'unserviceable', 'reserve', 'storage']
        
        vd = dt_date.fromisoformat(self.version_date_str)
        version_date = vd.year * 10000 + vd.month * 100 + vd.day
        version_id = int(self.env_data.get('version_id_u32', 1))
        
        count = 0
        for state_name in states:
            heli_pop = fg.AgentVector(self.base_model.agent)
            self.simulation.getPopulationData(heli_pop, state_name)
            
            for i in range(heli_pop.size()):
                agent = heli_pop.at(i)
                all_rows.append({
                    'version_date': version_date,
                    'version_id': version_id,
                    'day_u16': day,
                    'idx': agent.getVariableUInt('idx'),
                    'aircraft_number': agent.getVariableUInt('aircraft_number'),
                    'group_by': agent.getVariableUInt('group_by'),
                    'state': state_name,
                    'dt': agent.getVariableUInt('daily_today_u32'),
                    'sne': agent.getVariableUInt('sne'),
                    'ppr': agent.getVariableUInt('ppr'),
                    'll': agent.getVariableUInt('ll'),
                    'oh': agent.getVariableUInt('oh'),
                    'br': agent.getVariableUInt('br'),
                    'repair_days': agent.getVariableUInt('repair_days'),
                    'repair_time': agent.getVariableUInt('repair_time'),
                    'mfg_date': agent.getVariableUInt('mfg_date'),
                    'intent_state': agent.getVariableUInt('intent_state'),
                })
                count += 1
        return count


def main():
    parser = argparse.ArgumentParser(description='LIMITER V5 Orchestrator')
    parser.add_argument('--version-date', type=str, default='2025-07-04')
    parser.add_argument('--end-day', type=int, default=3650)
    parser.add_argument('--enable-mp2', action='store_true')
    parser.add_argument('--drop-table', action='store_true')
    args = parser.parse_args()
    
    print("=" * 60)
    print("🚀 LIMITER V5 — GPU-ONLY АДАПТИВНЫЕ ШАГИ")
    print("=" * 60)
    
    # Подключение к ClickHouse
    client = get_client()
    
    # Загрузка данных
    print(f"\n📊 Загрузка данных для {args.version_date}...")
    from datetime import date
    vd = date.fromisoformat(args.version_date)
    env_data = prepare_env_arrays(client, vd)
    
    # DROP TABLE если нужно
    if args.drop_table and args.enable_mp2:
        print("🗑️ DROP TABLE sim_masterv2_limiter...")
        client.execute("DROP TABLE IF EXISTS sim_masterv2_limiter")
        
        # Создаём таблицу
        client.execute("""
            CREATE TABLE IF NOT EXISTS sim_masterv2_limiter (
                version_date UInt32,
                version_id UInt8,
                day_u16 UInt16,
                idx UInt16,
                aircraft_number UInt32,
                group_by UInt8,
                state String,
                dt UInt32,
                sne UInt32,
                ppr UInt32,
                ll UInt32,
                oh UInt32,
                br UInt32,
                repair_days UInt16,
                repair_time UInt16,
                mfg_date UInt32,
                intent_state UInt8
            ) ENGINE = MergeTree()
            ORDER BY (version_date, day_u16, idx)
        """)
        print("  ✅ Таблица создана")
    
    # Создаём оркестратор
    orchestrator = LimiterV5Orchestrator(
        env_data=env_data,
        end_day=args.end_day,
        enable_mp2=args.enable_mp2,
        clickhouse_client=client,
        version_date_str=args.version_date
    )
    
    # Строим модель
    orchestrator.build_model()
    
    # Создаём симуляцию
    orchestrator.create_simulation()
    
    # Запускаем
    orchestrator.run()
    
    print("\n✅ LIMITER V5 завершён")


if __name__ == "__main__":
    main()

