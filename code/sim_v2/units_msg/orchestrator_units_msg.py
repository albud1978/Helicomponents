#!/usr/bin/env python3
"""
Оркестратор симуляции агрегатов на односторонних сообщениях (groups 3/4).

Дата: 15.01.2026
"""
import os
import sys
import time
import argparse
from datetime import date
from typing import Optional, Dict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
sys.path.insert(0, os.path.join(SCRIPT_DIR, '..'))
sys.path.insert(0, os.path.join(SCRIPT_DIR, '..', '..'))


def setup_environment():
    project_root = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..', '..'))
    rtc_cache = os.environ.get('FLAMEGPU2_RTC_CACHE', os.path.join(project_root, '.rtc_cache'))
    os.environ['FLAMEGPU2_RTC_CACHE'] = rtc_cache
    if not os.path.exists(rtc_cache):
        os.makedirs(rtc_cache)

    if 'CUDA_PATH' not in os.environ:
        os.environ['CUDA_PATH'] = '/home/albud/miniconda3/targets/x86_64-linux'

    print(f"⚡ RTC кэш: {rtc_cache}")
    print(f"🚀 CUDA_PATH: {os.environ.get('CUDA_PATH')}")


setup_environment()

try:
    import pyflamegpu as fg
except ImportError as e:
    print(f"❌ Ошибка импорта pyflamegpu: {e}")
    sys.exit(1)

from base_model_units_msg import V2BaseModelUnitsMsg
from agent_population_units_msg import AgentPopulationUnitsMsgBuilder
from sim_v2.units.init_planer_dt import register_init_planer_dt

# RTC модули
import rtc_planer_messages
import rtc_units_detach_msg
import rtc_units_slots_reset_msg
import rtc_units_slots_count_msg
import rtc_units_planer_need_msg
import rtc_units_counts_msg
import rtc_units_spawn_budget_msg
import rtc_units_states_stub_msg
import rtc_units_assign_msg
import rtc_units_ops_msg
import rtc_units_cap_reset_msg
import rtc_units_cap_ops_msg
import rtc_units_repair_msg
import rtc_units_transition_ops_msg
import rtc_units_transition_serviceable_msg
import rtc_units_transition_reserve_msg
import rtc_units_transition_repair_msg
import rtc_units_transition_storage_msg

# Экспорт MP2 (опционально)
from sim_v2.units import rtc_units_mp2_writer
from sim_v2.units.mp2_drain_units import register_mp2_drain_units
from utils.config_loader import get_clickhouse_client


class UnitsMsgOrchestrator:
    def __init__(self, version_date: date, version_id: int = 1):
        self.version_date = version_date
        self.version_id = version_id
        self.base_model: Optional[V2BaseModelUnitsMsg] = None
        self.simulation: Optional[fg.CUDASimulation] = None
        self.env_data: Dict = {}
        self.population_builder: Optional[AgentPopulationUnitsMsgBuilder] = None
        self.init_planer_dt_fn = None
        self.mp2_drain_fn = None

    def load_data(self):
        print("=" * 60)
        print("📊 ЗАГРУЗКА ДАННЫХ (units_msg)")
        print(f"   Дата версии: {self.version_date}")
        print("=" * 60)

        self.population_builder = AgentPopulationUnitsMsgBuilder(self.version_date, self.version_id)
        self.env_data = self.population_builder.load_data()

    def build_model(self):
        print("\n" + "=" * 60)
        print("🔧 ПОСТРОЕНИЕ МОДЕЛИ (units_msg)")
        print("=" * 60)

        self.base_model = V2BaseModelUnitsMsg()
        model = self.base_model.create_model(self.env_data)

        # InitPlanerDt — должен быть первым
        dt_array = self.env_data.get('planer_dt_array')
        ac_to_idx = self.env_data.get('ac_to_idx', {})
        assembly_array = self.env_data.get('planer_assembly_array')
        planer_in_ops = self.env_data.get('planer_in_ops', {})
        planer_type = self.env_data.get('planer_type', {})
        planer_in_ops_history = self.env_data.get('planer_in_ops_history')

        if dt_array is not None and len(ac_to_idx) > 0:
            self.init_planer_dt_fn = register_init_planer_dt(
                model, dt_array, ac_to_idx, self.env_data.get('days_total_u16', 3650),
                assembly_array, planer_in_ops, planer_type, planer_in_ops_history
            )
            print("  ✅ init_planer_dt зарегистрирован")
        else:
            print("  ⚠️ init_planer_dt не зарегистрирован (нет dt или ac_to_idx)")

        # Порядок слоёв
        rtc_planer_messages.register_rtc(model, self.base_model.agent_planer)
        rtc_units_detach_msg.register_rtc(model, self.base_model.agent_units)
        rtc_units_slots_reset_msg.register_rtc(model)
        rtc_units_slots_count_msg.register_rtc(model, self.base_model.agent_units)
        rtc_units_planer_need_msg.register_rtc(model)
        rtc_units_counts_msg.register_rtc(model, self.base_model.agent_units)
        rtc_units_spawn_budget_msg.register_rtc(model)
        rtc_units_states_stub_msg.register_rtc(model, self.base_model.agent_units)
        rtc_units_assign_msg.register_rtc(model, self.base_model.agent_units)
        rtc_units_ops_msg.register_rtc(model, self.base_model.agent_units)
        rtc_units_cap_reset_msg.register_rtc(model)
        rtc_units_cap_ops_msg.register_rtc(model, self.base_model.agent_units)
        rtc_units_repair_msg.register_rtc(model, self.base_model.agent_units)
        rtc_units_transition_ops_msg.register_rtc(model, self.base_model.agent_units)
        rtc_units_transition_repair_msg.register_rtc(model, self.base_model.agent_units)
        rtc_units_transition_serviceable_msg.register_rtc(model, self.base_model.agent_units)
        rtc_units_transition_reserve_msg.register_rtc(model, self.base_model.agent_units)
        rtc_units_transition_storage_msg.register_rtc(model, self.base_model.agent_units)

        # MP2 writer + drain (размеры должны совпадать с mp2 буфером)
        max_frames = int(self.env_data.get('units_frames_total', 10000))
        days_total = int(self.env_data.get('days_total_u16', 3650))
        rtc_units_mp2_writer.register_rtc(model, self.base_model.agent_units, max_frames=max_frames, max_days=days_total, drain_interval=10)
        client = get_clickhouse_client()
        self.mp2_drain_fn = register_mp2_drain_units(model, self.env_data, client, self.version_date, self.version_id)

        self.simulation = fg.CUDASimulation(model)

    def populate_agents(self):
        assert self.simulation is not None
        self.population_builder.populate_agents(
            self.simulation,
            self.base_model.agent_units,
            self.base_model.agent_planer,
            self.env_data
        )

        # Передаём initial_slots в InitPlanerDt
        if self.init_planer_dt_fn is not None:
            initial_slots = self.population_builder.initial_slots
            self.init_planer_dt_fn.set_initial_slots(initial_slots)
            total_slots = sum(initial_slots.values()) if initial_slots else 0
            print(f"  ✅ initial_slots переданы в InitPlanerDt ({total_slots})")

    def run(self, steps: int):
        print("\n" + "=" * 60)
        print(f"🚀 ЗАПУСК СИМУЛЯЦИИ (units_msg) на {steps} дней")
        print("=" * 60)
        self.simulation.SimulationConfig().steps = steps
        self.simulation.simulate()
        # Финальный drain пока не вызывается вручную (TODO синхронизировать с mp2_drain_units)


def main():
    parser = argparse.ArgumentParser(description="Units message-based simulation")
    parser.add_argument('--version-date', required=True, help='Дата версии (YYYY-MM-DD)')
    parser.add_argument('--version-id', type=int, default=1)
    parser.add_argument('--steps', type=int, default=3650)
    args = parser.parse_args()

    vd = date.fromisoformat(args.version_date)

    orch = UnitsMsgOrchestrator(vd, args.version_id)
    orch.load_data()
    orch.build_model()
    orch.populate_agents()
    orch.run(args.steps)


if __name__ == '__main__':
    main()
