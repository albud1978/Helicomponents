#!/usr/bin/env python3
"""
Repair-only FLAME GPU модель:
- Загружает MP1/MP3 в память, создаёт агентов
- Определяет только один RTC слой: rtc_repair
Дата: 2025-08-21
"""

from __future__ import annotations
from typing import Optional, List, Tuple, Dict
import os

try:
    import pyflamegpu
except Exception:
    pyflamegpu = None


class RepairOnlyModel:
    def __init__(self) -> None:
        self.model: Optional["pyflamegpu.ModelDescription"] = None
        self.sim: Optional["pyflamegpu.CUDASimulation"] = None
        self.agent = None
        self.num_agents: int = 0

    def build_model(self, num_agents: int) -> Optional["pyflamegpu.ModelDescription"]:
        if pyflamegpu is None:
            print("⚠️ pyflamegpu не установлен. Пропуск сборки модели.")
            return None
        self.num_agents = int(num_agents)
        model = pyflamegpu.ModelDescription("RepairOnly_ABM")

        # Агент и переменные
        agent = model.newAgent("component")
        self.agent = agent
        agent.newVariableUInt("idx", 0)
        agent.newVariableUInt("psn", 0)
        agent.newVariableUInt("partseqno_i", 0)
        agent.newVariableUInt("group_by", 0)
        agent.newVariableUInt("aircraft_number", 0)
        agent.newVariableUInt("ac_type_mask", 0)
        agent.newVariableUInt("mfg_date", 0)
        agent.newVariableUInt("status_id", 0)
        agent.newVariableUInt("repair_days", 0)
        agent.newVariableUInt("repair_time", 0)
        agent.newVariableUInt("ppr", 0)

        # RTC: ремонт
        rtc_repair_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
            unsigned int status_id = FLAMEGPU->getVariable<unsigned int>("status_id");
            if (status_id == 4u) {
                unsigned int rd = FLAMEGPU->getVariable<unsigned int>("repair_days") + 1u;
                FLAMEGPU->setVariable<unsigned int>("repair_days", rd);
                unsigned int rt = FLAMEGPU->getVariable<unsigned int>("repair_time");
                if (rd >= rt) {
                    FLAMEGPU->setVariable<unsigned int>("status_id", 5u);
                    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
                    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
                }
            }
            return flamegpu::ALIVE;
        }
        """
        agent.newRTCFunction("rtc_repair", rtc_repair_src)
        lyr = model.newLayer()
        lyr.addAgentFunction(agent.getFunction("rtc_repair"))

        self.model = model
        return model

    def build_simulation(self) -> Optional["pyflamegpu.CUDASimulation"]:
        if pyflamegpu is None:
            return None
        if self.model is None:
            raise RuntimeError("Model is not built. Call build_model(num_agents) first.")
        sim = pyflamegpu.CUDASimulation(self.model)
        self.sim = sim
        return sim


