#!/usr/bin/env python3
"""
LIMITER V6 Orchestrator — Архитектура с детерминированными переходами

V6 АРХИТЕКТУРА (12.01.2026):
- 7 состояний: inactive(1), operations(2), serviceable(3), repair(4), reserve(5), storage(6), unserviceable(7)
- repair(4): детерминированный выход по exit_date → serviceable(3)
- reserve(5): детерминированный spawn по exit_date → operations(2)  
- unserviceable(7): НОВЫЙ! после PPR>=OH, ждёт промоут P2
- Квотирование: P1(serviceable), P2(unserviceable), P3(inactive)

Запуск:
    python3 orchestrator_limiter_v6.py --version-date 2025-07-04 --end-day 3650

База: V5 + V6 states + rtc_deterministic_exit
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
import messaging.rtc_limiter_v5 as rtc_v5
import rtc_deterministic_exit
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


class LimiterV6Orchestrator:
    """V6 Оркестратор с детерминированными переходами и state 7"""
    
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
        """Строит модель с V6 архитектурой"""
        
        print("\n" + "=" * 60)
        print("🔧 Построение модели LIMITER V6 (детерминированные переходы)")
        print("=" * 60)
        
        self.model = self.base_model.create_model(self.env_data)
        
        # Environment properties
        self.base_model.env.newPropertyUInt("end_day", self.end_day)
        
        # V3 MacroProperties (limiter)
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
        
        # ═══════════════════════════════════════════════════════════════
        # V6 ФАЗА 0: Детерминированные переходы (repair→serviceable, spawn)
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 V6: Подключение детерминированных переходов...")
        rtc_deterministic_exit.register_all_deterministic(self.model, heli_agent)
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 1: Ежедневные инкременты
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение state_2_operations + states_stub_v2 (V6)...")
        import rtc_state_2_operations
        import rtc_states_stub_v2
        rtc_state_2_operations.register_rtc(self.model, heli_agent)
        rtc_states_stub_v2.register_rtc(self.model, heli_agent)
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 2: Квотирование (V6: P1, P2, P3)
        # ═══════════════════════════════════════════════════════════════
        print("\n📊 Подключение квотирования V6...")
        self._register_quota_modules_v6()
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 3: State managers (V6: включая state 7)
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение state managers V6...")
        self._register_state_managers_v6()
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 4: Limiter (V3 оптимизированный)
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение V3 limiter...")
        self._register_limiter_v3_rtc_only()
        
        # ═══════════════════════════════════════════════════════════════
        # ФАЗА 5: V5 100% GPU-only
        # ═══════════════════════════════════════════════════════════════
        print("\n📦 Подключение V5 100% GPU...")
        
        rtc_v5.setup_v5_macroproperties(self.base_model.env, self.program_change_days)
        
        self.base_model.quota_agent.newVariableUInt("computed_adaptive_days", 1)
        self.base_model.quota_agent.newVariableUInt("current_day_cache", 0)
        
        self.hf_init_v5, self.hf_sync_v5 = rtc_v5.register_v5(
            self.model,
            self.base_model.agent,
            self.base_model.quota_agent,
            self.program_change_days,
            self.end_day
        )
        
        rtc_v5.register_v5_final_layers(
            self.model,
            self.base_model.agent,
            self.base_model.quota_agent
        )
        
        self.hf_exit = rtc_v5.HF_ExitCondition(self.end_day)
        self.model.addExitCondition(self.hf_exit)
        
        print("\n✅ Модель LIMITER V6 построена")
        print("=" * 60)
        
        return self.model
    
    def _register_quota_modules_v6(self):
        """V6: Регистрирует модули квотирования с P2 (unserviceable)"""
        import rtc_quota_count_ops
        import rtc_quota_ops_excess
        import rtc_quota_promote_serviceable
        import rtc_quota_promote_unserviceable  # V6: P2
        import rtc_quota_promote_inactive
        
        rtc_quota_count_ops.register_rtc(self.model, self.base_model.agent)
        rtc_quota_ops_excess.register_rtc(self.model, self.base_model.agent)
        rtc_quota_promote_serviceable.register_rtc(self.model, self.base_model.agent)  # P1
        rtc_quota_promote_unserviceable.register_rtc(self.model, self.base_model.agent)  # P2 V6
        rtc_quota_promote_inactive.register_rtc(self.model, self.base_model.agent)  # P3
        
        print("  ✅ Квотирование V6 подключено (P1, P2, P3)")
    
    def _register_state_managers_v6(self):
        """V6: Регистрирует state managers включая state 7"""
        import rtc_state_manager_serviceable
        import rtc_state_manager_operations
        import rtc_state_manager_storage
        import rtc_state_manager_inactive
        import rtc_state_manager_state7  # V6: unserviceable (state 7)
        
        rtc_state_manager_serviceable.register_rtc(self.model, self.base_model.agent)
        rtc_state_manager_operations.register_state_manager_operations(self.model, self.base_model.agent)
        rtc_state_manager_storage.register_state_manager_storage(self.model, self.base_model.agent)
        rtc_state_manager_inactive.register_state_manager_inactive(self.model, self.base_model.agent)
        rtc_state_manager_state7.register_state_manager_state7(self.model, self.base_model.agent)  # V6
        
        # NOTE: state 4 (repair) и state 5 (reserve) обрабатываются через rtc_deterministic_exit
        
        print("  ✅ State managers V6 подключены (включая state 7)")
    
    def _register_limiter_v3_rtc_only(self):
        """V5: Регистрирует V3 limiter RTC"""
        
        rtc_limiter_optimized.setup_limiter_macroproperties(
            self.base_model.env,
            self.program_change_days
        )
        
        self.hf_init_min_limiter = rtc_limiter_optimized.HF_InitMinLimiter()
        self.model.addInitFunction(self.hf_init_min_limiter)
        
        rtc_limiter_optimized.register_limiter_optimized(self.model, self.base_model.agent)
        
        print("  ✅ V3 Limiter RTC зарегистрирован")
    
    def create_simulation(self):
        """Создаёт симуляцию"""
        print("\n🚀 Создание симуляции V6...")
        
        self.simulation = fg.CUDASimulation(self.model)
        
        self.population_builder.populate_agents(self.simulation, self.base_model.agent)
        self._populate_quota_managers()
        
        print("  ✅ Симуляция V6 создана")
        return self.simulation
    
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
        """V6: Запускает симуляцию"""
        
        print(f"\n▶️  Запуск LIMITER V6 симуляции (end_day={self.end_day})")
        print(f"  Max steps: {max_steps}")
        
        t_start = time.time()
        
        # Конфигурация
        self.simulation.SimulationConfig().steps = max_steps
        
        # Запуск
        self.simulation.simulate()
        
        t_gpu = time.time() - t_start
        steps = self.simulation.getStepCounter()
        
        print(f"\n📊 V6 Результаты:")
        print(f"  Время GPU: {t_gpu:.2f}с")
        print(f"  Шагов: {steps}")
        print(f"  Дней/сек: {self.end_day / t_gpu:.0f}")
        
        return steps


def main():
    parser = argparse.ArgumentParser(description='LIMITER V6 Orchestrator')
    parser.add_argument('--version-date', type=str, default='2025-07-04',
                        help='Дата датасета (YYYY-MM-DD)')
    parser.add_argument('--end-day', type=int, default=3650,
                        help='Последний день симуляции')
    parser.add_argument('--max-steps', type=int, default=10000,
                        help='Максимальное количество шагов')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🚀 LIMITER V6 Orchestrator")
    print(f"  Датасет: {args.version_date}")
    print(f"  Дней: {args.end_day}")
    print("=" * 60)
    
    # Загрузка данных
    client = get_client()
    env_data = prepare_env_arrays(client, args.version_date)
    
    # Создание оркестратора
    orchestrator = LimiterV6Orchestrator(
        env_data=env_data,
        end_day=args.end_day,
        clickhouse_client=client,
        version_date_str=args.version_date
    )
    
    # Построение модели
    orchestrator.build_model()
    
    # Создание симуляции
    orchestrator.create_simulation()
    
    # Запуск
    steps = orchestrator.run(max_steps=args.max_steps)
    
    print(f"\n✅ V6 симуляция завершена: {steps} шагов")


if __name__ == "__main__":
    main()

