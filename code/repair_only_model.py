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

        # Окружение для ops_check
        env = model.Environment()
        env.newPropertyArrayUInt32("daily_today", [0] * self.num_agents)
        env.newPropertyArrayUInt32("daily_next", [0] * self.num_agents)
        # Квота на D+1 по группам (инициализируется перед шагом через RTC quota_init)
        env.newMacroPropertyUInt32("remaining_ops_next_mi8", 1)
        env.newMacroPropertyUInt32("remaining_ops_next_mi17", 1)
        # Хост пишет эти значения перед каждым шагом суток
        env.newPropertyUInt("quota_next_mi8", 0)
        env.newPropertyUInt("quota_next_mi17", 0)

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
        # Для ops_check
        agent.newVariableUInt("sne", 0)
        agent.newVariableUInt("ll", 0)
        agent.newVariableUInt("oh", 0)
        agent.newVariableUInt("br", 0)
        # Для подстановки суточных часов из раннера
        agent.newVariableUInt("daily_today_u32", 0)
        agent.newVariableUInt("daily_next_u32", 0)
        # Токен допуска (будет использоваться при квотировании)
        agent.newVariableUInt("ops_ticket", 0)

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

        # RTC: ops_check
        rtc_ops_check_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_ops_check, flamegpu::MessageNone, flamegpu::MessageNone) {
            if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
            unsigned int dt = FLAMEGPU->getVariable<unsigned int>("daily_today_u32");
            unsigned int dn = FLAMEGPU->getVariable<unsigned int>("daily_next_u32");
            unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
            unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
            unsigned int ll  = FLAMEGPU->getVariable<unsigned int>("ll");
            unsigned int oh  = FLAMEGPU->getVariable<unsigned int>("oh");
            unsigned int br  = FLAMEGPU->getVariable<unsigned int>("br");
            // main уже выполнил инкремент на dt, поэтому используем текущие значения
            unsigned int sne_p = sne;
            unsigned int ppr_p = ppr;
            unsigned int rem_ll = (ll >= sne_p ? (ll - sne_p) : 0u);
            if (rem_ll < dn) {
                FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
                return flamegpu::ALIVE;
            }
            unsigned int rem_oh = (oh >= ppr_p ? (oh - ppr_p) : 0u);
            if (rem_oh < dn) {
                if (sne_p + dn < br) FLAMEGPU->setVariable<unsigned int>("status_id", 4u);
                else FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
            }
            // Квотирование D+1: если есть налёт сегодня, пробуем занять слот на завтра
            if (dt > 0u) {
                unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
                if (gb == 1u) {
                    auto q = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi8");
                    unsigned int old = q--; // старое значение до декремента
                    if (old > 0u) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
                } else if (gb == 2u) {
                    auto q = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi17");
                    unsigned int old = q--;
                    if (old > 0u) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
                }
            }
            return flamegpu::ALIVE;
        }
        """
        agent.newRTCFunction("rtc_ops_check", rtc_ops_check_src)

        # RTC: main — начисление налёта/порогов (только при полученном ops_ticket)
        rtc_main_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_main, flamegpu::MessageNone, flamegpu::MessageNone) {
            if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
            unsigned int dt = FLAMEGPU->getVariable<unsigned int>("daily_today_u32");
            unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
            unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
            // Инкремент всегда на D, т.к. используем статус начала дня
            FLAMEGPU->setVariable<unsigned int>("sne", sne + dt);
            FLAMEGPU->setVariable<unsigned int>("ppr", ppr + dt);
            return flamegpu::ALIVE;
        }
        """
        agent.newRTCFunction("rtc_main", rtc_main_src)

        # RTC: quota_init — инициализация квоты из host env и сброс ops_ticket
        rtc_quota_init_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_quota_init, flamegpu::MessageNone, flamegpu::MessageNone) {
            // Сбрасываем билет допуска на сутки
            FLAMEGPU->setVariable<unsigned int>("ops_ticket", 0u);
            // Один агент (idx==0) инициализирует квотные macro properties из скалярных env
            if (FLAMEGPU->getVariable<unsigned int>("idx") == 0u) {
                unsigned int seed8 = FLAMEGPU->environment.getProperty<unsigned int>("quota_next_mi8");
                auto q8 = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi8");
                q8.exchange(seed8);
                unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("quota_next_mi17");
                auto q17 = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi17");
                q17.exchange(seed17);
            }
            return flamegpu::ALIVE;
        }
        """
        agent.newRTCFunction("rtc_quota_init", rtc_quota_init_src)

        # Слои: repair → quota_init → main → ops_check (решение на D+1 после инкремента)
        lyr1 = model.newLayer()
        lyr1.addAgentFunction(agent.getFunction("rtc_repair"))
        lyr2 = model.newLayer()
        lyr2.addAgentFunction(agent.getFunction("rtc_quota_init"))
        lyr3 = model.newLayer()
        lyr3.addAgentFunction(agent.getFunction("rtc_main"))
        lyr4 = model.newLayer()
        lyr4.addAgentFunction(agent.getFunction("rtc_ops_check"))

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


