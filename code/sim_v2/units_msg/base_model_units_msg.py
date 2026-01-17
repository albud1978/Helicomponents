#!/usr/bin/env python3
"""
V2 Base Model для агрегатов на односторонних сообщениях (groups 3 и 4).

Отличия от текущей units модели:
- Добавлен агент Planer (источник сообщений)
- Назначение агрегатов через MessageBruteForce
- Минимальная FIFO-логика: приоритет serviceable → reserve

Дата: 15.01.2026
"""
import os
import sys
from typing import Dict, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")

# Константы
MAX_GROUPS = 50
MAX_PLANERS = 400
PLANER_MAX_DAYS = 4000
MAX_AC_NUMBER = 2000000
UNITS_MAX_FRAMES = 40000
UNITS_MAX_DAYS = 4000


class V2BaseModelUnitsMsg:
    """Базовая модель агрегатов с сообщениями от планеров"""

    def __init__(self):
        self.model: Optional[fg.ModelDescription] = None
        self.env: Optional[fg.EnvironmentDescription] = None
        self.agent_units: Optional[fg.AgentDescription] = None
        self.agent_planer: Optional[fg.AgentDescription] = None
        self.message_planer = None

    def create_model(self, env_data: Dict[str, object]) -> fg.ModelDescription:
        self.model = fg.ModelDescription("HeliUnitsMsgV1")
        self.env = self.model.Environment()

        self._setup_scalar_properties(env_data)
        self._setup_macro_properties(env_data)
        self._setup_property_arrays(env_data)

        self.agent_units = self._setup_units_agent()
        self.agent_planer = self._setup_planer_agent()
        self._setup_messages()

        return self.model

    def _setup_scalar_properties(self, env_data: Dict[str, object]):
        self.env.newPropertyUInt("version_date", int(env_data['version_date_u16']))
        self.env.newPropertyUInt("version_id", int(env_data.get('version_id_u32', 0)))
        self.env.newPropertyUInt("units_frames_total", int(env_data.get('units_frames_total', 400)))
        self.env.newPropertyUInt("days_total", int(env_data.get('days_total_u16', 3650)))
        self.env.newPropertyUInt("planers_total", int(env_data.get('planers_total', 0)))

    def _setup_macro_properties(self, env_data: Dict[str, object]):
        max_frames = int(env_data.get('units_frames_total', UNITS_MAX_FRAMES))
        max_days = int(env_data.get('days_total_u16', UNITS_MAX_DAYS))

        # Счётчики для приоритетов
        self.env.newMacroPropertyUInt32("mp_svc_count", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_rsv_count", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_spawn_budget", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_assign_hits", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_assign_attempts", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_assign_called", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_reserve_seen", MAX_GROUPS)

        # Планерные данные (из sim_masterv2)
        # Фиксированный размер для RTC (по правилам проекта)
        planer_dt_size = MAX_PLANERS * (PLANER_MAX_DAYS + 1)
        self.env.newMacroPropertyUInt("mp_planer_dt", planer_dt_size)
        self.env.newMacroPropertyUInt("mp_ac_to_idx", MAX_AC_NUMBER)
        self.env.newMacroPropertyUInt("mp_idx_to_ac", MAX_PLANERS)
        self.env.newMacroPropertyUInt8("mp_planer_in_ops_history", planer_dt_size)
        self.env.newMacroPropertyUInt8("mp_planer_type", MAX_PLANERS)
        # Для совместимости с init_planer_dt
        self.env.newMacroPropertyUInt8("mp_planer_assembly", planer_dt_size)
        self.env.newMacroPropertyUInt8("mp_planer_in_ops", MAX_PLANERS)

        # Слоты агрегатов на планерах
        self.env.newMacroPropertyUInt32("mp_planer_slots", MAX_GROUPS * MAX_PLANERS)
        self.env.newMacroPropertyUInt32("mp_planer_need", MAX_GROUPS * MAX_PLANERS)
        self.env.newMacroPropertyUInt32("mp_planer_cap", MAX_GROUPS * MAX_PLANERS)
        # Списки планеров в ops по типам (для назначения) — по дням
        self.env.newMacroPropertyUInt32("mp_ops_list_g3", planer_dt_size)
        self.env.newMacroPropertyUInt32("mp_ops_list_g4", planer_dt_size)
        self.env.newMacroPropertyUInt32("mp_ops_count_g3", PLANER_MAX_DAYS + 1)
        self.env.newMacroPropertyUInt32("mp_ops_count_g4", PLANER_MAX_DAYS + 1)

        # MP2 буфер (как в units) — для будущего экспорта
        DRAIN_INTERVAL = 10
        mp2_buffer_size = max_frames * (DRAIN_INTERVAL + 1)
        self.env.newPropertyUInt("mp2_drain_interval", DRAIN_INTERVAL)
        self.env.newMacroPropertyUInt("mp2_units_psn", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_group_by", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_sne", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_ppr", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_state", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_ac", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_repair_days", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_queue_pos", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_partseqno", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_active", mp2_buffer_size)

    def _setup_property_arrays(self, env_data: Dict[str, object]):
        comp_numbers = list(env_data.get('comp_numbers', [0] * MAX_GROUPS))
        comp_numbers = (comp_numbers + [0] * MAX_GROUPS)[:MAX_GROUPS]
        self.env.newPropertyArrayUInt32("comp_numbers", comp_numbers)

        repair_times = list(env_data.get('units_repair_times', [30] * MAX_GROUPS))
        repair_times = (repair_times + [30] * MAX_GROUPS)[:MAX_GROUPS]
        self.env.newPropertyArrayUInt32("units_repair_times", repair_times)

    def _setup_units_agent(self) -> fg.AgentDescription:
        agent = self.model.newAgent("unit")

        agent.newState("operations")
        agent.newState("serviceable")
        agent.newState("repair")
        agent.newState("reserve")
        agent.newState("storage")

        agent.newVariableUInt("idx", 0)
        agent.newVariableUInt("psn", 0)
        agent.newVariableUInt("aircraft_number", 0)
        agent.newVariableUInt("planer_idx", 0)
        agent.newVariableUInt("partseqno_i", 0)
        agent.newVariableUInt("group_by", 0)
        agent.newVariableUInt("queue_position", 0)
        agent.newVariableUInt("sne", 0)
        agent.newVariableUInt("ppr", 0)
        agent.newVariableUInt("ll", 0)
        agent.newVariableUInt("oh", 0)
        agent.newVariableUInt("br", 0)
        agent.newVariableUInt("repair_time", 30)
        agent.newVariableUInt("repair_days", 0)
        agent.newVariableUInt("intent_state", 0)
        agent.newVariableUInt("mfg_date", 0)
        agent.newVariableUInt("active", 1)
        agent.newVariableUInt("want_assign", 0)
        agent.newVariableUInt("transition_2_to_3", 0)
        agent.newVariableUInt("transition_2_to_4", 0)
        agent.newVariableUInt("transition_2_to_6", 0)
        agent.newVariableUInt("transition_3_to_2", 0)
        agent.newVariableUInt("transition_4_to_5", 0)
        agent.newVariableUInt("transition_5_to_2", 0)
        agent.newVariableUInt("transition_planer_exit", 0)

        return agent

    def _setup_planer_agent(self) -> fg.AgentDescription:
        agent = self.model.newAgent("planer")
        agent.newState("planer")

        agent.newVariableUInt("aircraft_number", 0)
        agent.newVariableUInt("planer_idx", 0)
        agent.newVariableUInt("planer_type", 0)
        agent.newVariableUInt("in_ops", 0)
        agent.newVariableUInt("dt", 0)

        return agent

    def _setup_messages(self):
        msg = self.model.newMessageBruteForce("planer_message")
        msg.newVariableUInt("aircraft_number")
        msg.newVariableUInt("planer_idx")
        msg.newVariableUInt("dt")
        msg.newVariableUInt("planer_type")
        msg.newVariableUInt("in_ops")
        self.message_planer = msg
