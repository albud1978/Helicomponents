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

        # Планеры для сообщений
        ac_to_idx = env_data.get('ac_to_idx', {})
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
