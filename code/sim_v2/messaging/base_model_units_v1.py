#!/usr/bin/env python3
"""
V1 Base Model для агрегатов (L2 engines): базовая модель и окружение для RTC симуляции

Отличия от base_model_units.py:
- Добавлены MacroProperty mp_planer_status / mp_planer_assembly_trigger
- Сохранено правило "без inactive" для агрегатов

Дата: 26.02.2026
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


MAX_GROUPS = 50
UNITS_MAX_FRAMES = 40000
UNITS_MAX_DAYS = 4000


class V1BaseModelUnits:
    """Базовая модель для симуляции агрегатов (group_by >= 3)"""

    def __init__(self):
        self.model: Optional[fg.ModelDescription] = None
        self.agent: Optional[fg.AgentDescription] = None
        self.env: Optional[fg.EnvironmentDescription] = None
        self.env_data: Optional[Dict[str, object]] = None

    def create_model(self, env_data: Dict[str, object]) -> fg.ModelDescription:
        """Создает базовую модель для агрегатов"""
        self.env_data = env_data

        frames_from_data = int(env_data.get('units_frames_total', UNITS_MAX_FRAMES))
        if frames_from_data > UNITS_MAX_FRAMES:
            print(f"⚠️ units_frames_total={frames_from_data} > UNITS_MAX_FRAMES={UNITS_MAX_FRAMES}")
            frames_from_data = UNITS_MAX_FRAMES

        self.model = fg.ModelDescription("HeliUnitsSimV2_L2")
        self.env = self.model.Environment()

        self._setup_scalar_properties(env_data)
        self._setup_macro_properties(env_data)
        self._setup_property_arrays(env_data)

        self.agent = self._setup_agent()

        return self.model

    def _setup_scalar_properties(self, env_data: Dict[str, object]):
        self.env.newPropertyUInt("version_date", int(env_data['version_date_u16']))
        self.env.newPropertyUInt("version_id", int(env_data.get('version_id_u32', 0)))
        self.env.newPropertyUInt("units_frames_total", int(env_data.get('units_frames_total', 400)))
        self.env.newPropertyUInt("days_total", int(env_data['days_total_u16']))
        self.env.newPropertyUInt("export_phase", 0)

        self.env.newPropertyUInt("planers_mi8_count", int(env_data.get('n_mi8', 163)))
        self.env.newPropertyUInt("planers_mi17_count", int(env_data.get('n_mi17', 116)))

    def _setup_macro_properties(self, env_data: Dict[str, object]):
        max_frames = int(env_data.get('units_frames_total', UNITS_MAX_FRAMES))
        max_days = int(env_data.get('days_total_u16', UNITS_MAX_DAYS))

        self._mp2_max_frames = max_frames
        self._mp2_max_days = max_days

        # === FIFO очереди ===
        self.env.newMacroPropertyUInt32("mp_svc_head", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_svc_tail", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_rsv_head", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_rsv_tail", MAX_GROUPS)

        self.env.newMacroPropertyUInt32("mp_queue_head", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_queue_tail", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_replacement_request", max_frames)
        self.env.newMacroPropertyUInt32("mp_replacement_group", max_frames)

        self.env.newMacroPropertyUInt32("units_pool_count", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("units_ops_count", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("units_repair_count", MAX_GROUPS)
        self.env.newMacroPropertyUInt32("mp_request_count", MAX_GROUPS)

        # === MP планеров (dt + status + assembly_trigger) ===
        MAX_PLANERS = 400
        planer_size = MAX_PLANERS * (max_days + 1)
        self.env.newMacroPropertyUInt("mp_planer_dt", planer_size)
        self.env.newMacroPropertyUInt("mp_planer_status", planer_size)
        self.env.newMacroPropertyUInt("mp_planer_assembly_trigger", planer_size)
        self.env.newMacroPropertyUInt("mp_ac_to_idx", 2000000)

        # === MP2 (циклический буфер) ===
        DRAIN_INTERVAL = 10
        mp2_buffer_size = max_frames * (DRAIN_INTERVAL + 1)
        self._mp2_drain_interval = DRAIN_INTERVAL
        print(f"  MP2 Units: max_frames={max_frames}, buffer_days={DRAIN_INTERVAL}, "
              f"buffer_size={mp2_buffer_size:,}")

        self.env.newMacroPropertyUInt("mp2_units_psn", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_group_by", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_sne", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_ppr", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_state", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_pre_state", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_ac", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_repair_days", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_queue_pos", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_partseqno", mp2_buffer_size)
        self.env.newMacroPropertyUInt("mp2_units_active", mp2_buffer_size)

        self.env.newPropertyUInt("mp2_drain_interval", DRAIN_INTERVAL)

    def _setup_property_arrays(self, env_data: Dict[str, object]):
        comp_numbers = list(env_data.get('comp_numbers', [0] * MAX_GROUPS))
        comp_numbers = (comp_numbers + [0] * MAX_GROUPS)[:MAX_GROUPS]
        self.env.newPropertyArrayUInt32("comp_numbers", comp_numbers)

        repair_times = list(env_data.get('units_repair_times', [30] * MAX_GROUPS))
        repair_times = (repair_times + [30] * MAX_GROUPS)[:MAX_GROUPS]
        self.env.newPropertyArrayUInt32("units_repair_times", repair_times)

    def _setup_agent(self) -> fg.AgentDescription:
        agent = self.model.newAgent("unit")

        # Состояния (без inactive)
        agent.newState("operations")
        agent.newState("serviceable")
        agent.newState("repair")
        agent.newState("reserve")
        agent.newState("storage")

        agent.newVariableUInt("idx", 0)
        agent.newVariableUInt("psn", 0)
        agent.newVariableUInt("aircraft_number", 0)
        agent.newVariableUInt("partseqno_i", 0)
        agent.newVariableUInt("group_by", 0)

        agent.newVariableUInt("queue_position", 0)
        agent.newVariableUInt("pre_state_id", 0)

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

        agent.newVariableUInt("bi_counter", 1)

        agent.newVariableUInt("transition_2_to_3", 0)
        agent.newVariableUInt("transition_2_to_4", 0)
        agent.newVariableUInt("transition_2_to_6", 0)
        agent.newVariableUInt("transition_3_to_2", 0)
        agent.newVariableUInt("transition_4_to_5", 0)
        agent.newVariableUInt("transition_5_to_2", 0)

        return agent

    def add_rtc_module(self, module_name: str):
        """Динамически подключает RTC модуль по имени"""
        try:
            module = __import__(f'rtc_units_{module_name}', fromlist=['register_rtc'])
            if hasattr(module, 'register_rtc'):
                module.register_rtc(self.model, self.agent)
            else:
                raise ValueError(f"Модуль rtc_units_{module_name} не содержит функцию register_rtc")
        except ImportError as e:
            raise RuntimeError(f"Не удалось загрузить RTC модуль '{module_name}': {e}")
