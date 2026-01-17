#!/usr/bin/env python3
"""
Инициализация популяций для модели units_msg.

- Агрегаты (group_by 3/4) через AgentPopulationUnitsBuilder
- Планеры для сообщений через sim_masterv2

Дата: 15.01.2026
"""
from datetime import date
from typing import Dict, Optional

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

import pyflamegpu as fg

from sim_v2.units.agent_population_units import AgentPopulationUnitsBuilder
from sim_v2.units.planer_dt_loader import load_planer_data


class AgentPopulationUnitsMsgBuilder:
    """Комбинированный builder для units_msg"""

    def __init__(self, version_date: date, version_id: int = 1):
        self.version_date = version_date
        self.version_id = version_id
        self.units_builder = AgentPopulationUnitsBuilder(version_date, version_id)
        self.planer_data = {}
        self.env_data: Dict[str, object] = {}

    def load_data(self) -> Dict[str, object]:
        """Загружает агрегаты и планерные данные"""
        env_data = self.units_builder.load_data()

        # Загрузка данных планеров из sim_masterv2
        dt_array, assembly_array, ac_to_idx, planer_in_ops, planer_type, planer_in_ops_history = load_planer_data(
            str(self.version_date), self.version_id
        )

        env_data['planer_dt_array'] = dt_array
        env_data['planer_assembly_array'] = assembly_array
        env_data['ac_to_idx'] = ac_to_idx
        env_data['planer_in_ops'] = planer_in_ops
        env_data['planer_type'] = planer_type
        env_data['planer_in_ops_history'] = planer_in_ops_history
        # Предрасчёт списков планеров в ops по типам (по дням)
        if planer_in_ops_history is not None and len(ac_to_idx) > 0:
            max_planers = 400
            max_days = 4000
            size = max_planers * (max_days + 1)
            ops_list_g3 = np.zeros(size, dtype=np.uint32)
            ops_list_g4 = np.zeros(size, dtype=np.uint32)
            ops_count_g3 = np.zeros(max_days + 1, dtype=np.uint32)
            ops_count_g4 = np.zeros(max_days + 1, dtype=np.uint32)

            for day in range(max_days + 1):
                base = day * max_planers
                c3 = 0
                c4 = 0
                for idx in range(max_planers):
                    if planer_in_ops_history[base + idx] == 0:
                        continue
                    ptype = planer_type.get(idx, 0)
                    if ptype == 1:
                        ops_list_g3[base + c3] = idx
                        c3 += 1
                    elif ptype == 2:
                        ops_list_g4[base + c4] = idx
                        c4 += 1
                ops_count_g3[day] = c3
                ops_count_g4[day] = c4

            env_data['ops_list_g3'] = ops_list_g3
            env_data['ops_list_g4'] = ops_list_g4
            env_data['ops_count_g3'] = ops_count_g3
            env_data['ops_count_g4'] = ops_count_g4
        env_data['planers_total'] = len(ac_to_idx)

        self.planer_data = {
            'ac_to_idx': ac_to_idx,
            'planer_type': planer_type,
        }
        self.env_data = env_data

        return env_data

    def populate_agents(self, simulation: fg.CUDASimulation,
                        units_agent: fg.AgentDescription,
                        planer_agent: fg.AgentDescription,
                        env_data: Dict[str, object]):
        """Создаёт популяции агрегатов и планеров"""
        # Агрегаты (group_by 3/4)
        self.units_builder.populate_agents(simulation, units_agent, env_data)

        # Проставляем planer_idx для агрегатов по aircraft_number
        ac_to_idx = env_data.get('ac_to_idx', {})
        if ac_to_idx:
            for state_name in ("operations", "serviceable", "repair", "reserve", "storage"):
                units_pop = fg.AgentVector(units_agent)
                simulation.getPopulationData(units_pop, state_name)
                for agent in units_pop:
                    ac = agent.getVariableUInt("aircraft_number")
                    agent.setVariableUInt("planer_idx", int(ac_to_idx.get(ac, 0)))
                simulation.setPopulationData(units_pop, state_name)

        # Планеры для сообщений
        planer_type = env_data.get('planer_type', {})

        planer_pop = fg.AgentVector(planer_agent)
        for ac, idx in ac_to_idx.items():
            planer_pop.push_back()
            agent = planer_pop.back()
            agent.setVariableUInt("aircraft_number", int(ac))
            agent.setVariableUInt("planer_idx", int(idx))
            agent.setVariableUInt("planer_type", int(planer_type.get(idx, 0)))
            agent.setVariableUInt("in_ops", 0)
            agent.setVariableUInt("dt", 0)

        simulation.setPopulationData(planer_pop, "planer")
        print(f"   Планеры для сообщений: {len(planer_pop)}")

    @property
    def initial_slots(self) -> Dict:
        return getattr(self.units_builder, 'initial_slots', {})
