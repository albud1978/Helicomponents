#!/usr/bin/env python3
"""
Baseline с simulation.simulate() вместо цикла step()

Цель: измерить Python overhead от цикла step() vs одиночный simulate()

Запуск:
    python3 orchestrator_baseline_simulate.py --version-date 2025-07-04 --steps 365 --enable-mp2 --drop-table
"""
import os
import sys
import argparse
import time
from typing import Dict
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
from components.agent_population import AgentPopulationBuilder
from components.mp5_strategy import HostOnlyMP5Strategy
import model_build

try:
    import pyflamegpu as fg
    import numpy as np
except ImportError as e:
    raise RuntimeError(f"Зависимости не установлены: {e}")


class BaselineSimulateOrchestrator:
    """Оркестратор с одиночным simulate() вызовом"""
    
    def __init__(self, env_data: Dict, steps: int = 365,
                 enable_mp2: bool = False, clickhouse_client=None,
                 version_date_str: str = "2025-07-04"):
        self.env_data = env_data
        self.steps = steps
        self.enable_mp2 = enable_mp2
        self.clickhouse_client = clickhouse_client
        self.version_date_str = version_date_str
        
        self.base_model = V2BaseModelMessaging()
        self.model = None
        self.simulation = None
        
        # Параметры
        self.frames = int(env_data['frames_total_u16'])
        
        # MP5 Strategy для инициализации наработок
        self.mp5_strategy = HostOnlyMP5Strategy(env_data, model_build, steps)
        
        print(f"  frames={self.frames}, steps={self.steps}")
    
    def build_model(self):
        """Строит модель с baseline модулями"""
        
        print("\n" + "=" * 60)
        print("🔧 Построение модели: BASELINE + simulate()")
        print("=" * 60)
        
        # Создаём модель
        self.model = self.base_model.create_model(self.env_data)
        
        heli_agent = self.base_model.agent
        
        # ═══════════════════════════════════════════════════════════════════════
        # Регистрация RTC модулей — ТОЧНО как в baseline orchestrator_v2.py
        # ═══════════════════════════════════════════════════════════════════════
        
        print("\n📦 Подключение baseline модулей...")
        
        # 1. State 2 operations (ежедневные инкременты)
        import rtc_state_2_operations
        rtc_state_2_operations.register_rtc(self.model, heli_agent)
        print("  ✅ rtc_state_2_operations")
        
        # 2. States stub
        import rtc_states_stub
        rtc_states_stub.register_rtc(self.model, heli_agent)
        print("  ✅ rtc_states_stub")
        
        # 3. Quota modules
        import rtc_quota_count_ops
        import rtc_quota_ops_excess
        import rtc_quota_promote_serviceable
        import rtc_quota_promote_reserve
        import rtc_quota_promote_inactive
        
        rtc_quota_count_ops.register_rtc(self.model, heli_agent)
        rtc_quota_ops_excess.register_rtc(self.model, heli_agent)
        rtc_quota_promote_serviceable.register_rtc(self.model, heli_agent)
        rtc_quota_promote_reserve.register_rtc(self.model, heli_agent)
        rtc_quota_promote_inactive.register_rtc(self.model, heli_agent)
        print("  ✅ quota modules")
        
        # 4. State managers
        import rtc_state_manager_serviceable
        import rtc_state_manager_operations
        import rtc_state_manager_repair
        import rtc_state_manager_storage
        import rtc_state_manager_reserve
        import rtc_state_manager_inactive
        
        rtc_state_manager_serviceable.register_rtc(self.model, heli_agent)
        rtc_state_manager_operations.register_rtc(self.model, heli_agent)
        rtc_state_manager_repair.register_rtc(self.model, heli_agent)
        rtc_state_manager_storage.register_rtc(self.model, heli_agent)
        rtc_state_manager_reserve.register_rtc(self.model, heli_agent)
        rtc_state_manager_inactive.register_rtc(self.model, heli_agent)
        print("  ✅ state managers")
        
        # 6. Spawn v2
        import rtc_spawn_v2
        rtc_spawn_v2.register_rtc(self.model, heli_agent)
        print("  ✅ rtc_spawn_v2")
        
        # 7. MP2 writer + drain
        if self.enable_mp2:
            import rtc_mp2_writer
            rtc_mp2_writer.register_mp2_writer(self.model, heli_agent, self.clickhouse_client)
            
            from mp2_drain_host import MP2DrainHostFunction
            self.mp2_drain_func = MP2DrainHostFunction(
                self.clickhouse_client,
                table_name='sim_masterv2_baseline_sim',
                batch_size=500000,
                simulation_steps=self.steps
            )
            
            final_layer = self.model.newLayer("layer_mp2_drain_final")
            final_layer.addHostFunction(self.mp2_drain_func)
            print("  ✅ MP2 writer + drain")
        
        print("\n✅ Модель BASELINE построена")
        print("=" * 60)
        
        return self.model
    
    def create_simulation(self):
        """Создаёт симуляцию и инициализирует популяции"""
        
        print("\n🚀 Создание симуляции...")
        
        self.simulation = fg.CUDASimulation(self.model)
        self.simulation.SimulationConfig().steps = self.steps
        
        # Инициализация популяций
        print("Инициализация популяций агентов...")
        population_builder = AgentPopulationBuilder(self.env_data)
        population_builder.populate_agents(self.simulation, self.base_model.agent)
        
        # Инициализация MP5
        self.mp5_strategy.init_host(self.simulation)
        
        # Spawn initialization
        import rtc_spawn_v2
        rtc_spawn_v2.initialize_spawn_population(
            self.simulation, self.base_model.agent, self.env_data
        )
        
        print("  ✅ Симуляция создана")
    
    def run(self):
        """
        Запускает симуляцию ОДНИМ вызовом simulate()
        """
        
        print(f"\n▶️  Запуск BASELINE симуляции через simulate()")
        print(f"  Steps: {self.steps}")
        print(f"  MP2: {'✅' if self.enable_mp2 else '❌'}")
        
        t_start = time.perf_counter()
        
        # ═══════════════════════════════════════════════════════════════════════
        # ОДИН ВЫЗОВ! Без Python overhead между шагами.
        # ═══════════════════════════════════════════════════════════════════════
        self.simulation.simulate()
        
        t_end = time.perf_counter()
        elapsed = t_end - t_start
        
        # Статистика MP2
        total_rows = 0
        drain_time = 0.0
        if self.enable_mp2 and hasattr(self, 'mp2_drain_func'):
            total_rows = self.mp2_drain_func.total_rows_written
            drain_time = self.mp2_drain_func.total_drain_time
        
        print(f"\n✅ BASELINE (simulate) завершена:")
        print(f"  • Дней: {self.steps}")
        print(f"  • Общее время: {elapsed:.2f}с")
        print(f"  • Дней/сек: {self.steps / elapsed:.1f}")
        if self.enable_mp2:
            print(f"  • Время drain: {drain_time:.2f}с")
            print(f"  • Строк выгружено: {total_rows:,}")


def main():
    """Точка входа"""
    
    parser = argparse.ArgumentParser(description="Baseline с simulate()")
    parser.add_argument("--version-date", required=True, help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--steps", type=int, default=365, help="Количество шагов (дней)")
    parser.add_argument("--enable-mp2", action="store_true", help="MP2 экспорт")
    parser.add_argument("--drop-table", action="store_true", help="Очистить таблицу")
    args = parser.parse_args()
    
    print("=" * 70)
    print("🚀 BASELINE + simulate() — тест Python overhead")
    print("=" * 70)
    print(f"  Version date: {args.version_date}")
    print(f"  Steps: {args.steps}")
    print("=" * 70)
    
    # Подключение к СУБД
    client = get_client()
    
    if args.drop_table:
        client.execute("DROP TABLE IF EXISTS sim_masterv2_baseline_sim")
        client.execute("""
            CREATE TABLE sim_masterv2_baseline_sim AS sim_masterv2
            ENGINE = MergeTree()
            ORDER BY (version_date, version_id, day_u16, idx)
        """)
        print("✅ Таблица sim_masterv2_baseline_sim создана")
    
    # Загрузка данных
    print("\n📥 Загрузка данных...")
    from datetime import date as dt_date
    vd = dt_date.fromisoformat(args.version_date)
    env_data = prepare_env_arrays(client, vd)
    
    # Оркестратор
    orchestrator = BaselineSimulateOrchestrator(
        env_data,
        steps=args.steps,
        enable_mp2=args.enable_mp2,
        clickhouse_client=client,
        version_date_str=args.version_date
    )
    
    orchestrator.build_model()
    orchestrator.create_simulation()
    orchestrator.run()
    
    print("\n✅ Готово!")


if __name__ == "__main__":
    main()

