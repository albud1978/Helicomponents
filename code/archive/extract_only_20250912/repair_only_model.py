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

    def build_model(self, num_agents: int, env_sizes: Optional[Dict[str, int]] = None) -> Optional["pyflamegpu.ModelDescription"]:
        if pyflamegpu is None:
            print("⚠️ pyflamegpu не установлен. Пропуск сборки модели.")
            return None
        self.num_agents = int(num_agents)
        model = pyflamegpu.ModelDescription("HeliSim")

        # Окружение (Env / MacroProperty)
        env = model.Environment()
        days_total = int((env_sizes or {}).get('days_total', 1))
        frames_total = int((env_sizes or {}).get('frames_total', 1))
        mp1_len = int((env_sizes or {}).get('mp1_len', 1))
        mp3_count = int((env_sizes or {}).get('mp3_count', self.num_agents))
        # Старые массивы (совместимость раннера; будут удалены после миграции на MP5):
        env.newPropertyArrayUInt32("daily_today", [0] * self.num_agents)
        env.newPropertyArrayUInt32("daily_next", [0] * self.num_agents)
        # Квота на D+1 по группам (инициализируется перед шагом через RTC quota_init)
        env.newMacroPropertyUInt32("remaining_ops_next_mi8", 1)
        env.newMacroPropertyUInt32("remaining_ops_next_mi17", 1)
        # Хост пишет эти значения перед каждым шагом суток
        env.newPropertyUInt("quota_next_mi8", 0)
        env.newPropertyUInt("quota_next_mi17", 0)
        # Новые скаляры окружения
        env.newPropertyUInt("version_date", 0)
        env.newPropertyUInt("frames_total", 0)
        env.newPropertyUInt("days_total", 0)
        # MP4 как RO Property Arrays (заполняются один раз на старте)
        env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * max(1, days_total))
        env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * max(1, days_total))
        # MP5 как RO Property Array (линейная раскладка (DAYS+1)*frames_total)
        env.newPropertyArrayUInt32("mp5_daily_hours", [0] * max(1, (days_total + 1) * frames_total))
        # MP1 как RO Property Arrays (SoA по partseqno_i индексу)
        env.newPropertyArrayUInt32("mp1_br_mi8", [0] * max(1, mp1_len))
        env.newPropertyArrayUInt32("mp1_br_mi17", [0] * max(1, mp1_len))
        env.newPropertyArrayUInt32("mp1_repair_time", [0] * max(1, mp1_len))
        env.newPropertyArrayUInt32("mp1_partout_time", [0] * max(1, mp1_len))
        env.newPropertyArrayUInt32("mp1_assembly_time", [0] * max(1, mp1_len))
        # MP3 как RO Property Arrays (SoA по строкам heli_pandas)
        env.newPropertyArrayUInt32("mp3_psn", [0] * max(1, mp3_count))
        env.newPropertyArrayUInt32("mp3_aircraft_number", [0] * max(1, mp3_count))
        env.newPropertyArrayUInt32("mp3_ac_type_mask", [0] * max(1, mp3_count))
        env.newPropertyArrayUInt32("mp3_status_id", [0] * max(1, mp3_count))
        env.newPropertyArrayUInt32("mp3_sne", [0] * max(1, mp3_count))
        env.newPropertyArrayUInt32("mp3_ppr", [0] * max(1, mp3_count))
        env.newPropertyArrayUInt32("mp3_repair_days", [0] * max(1, mp3_count))
        env.newPropertyArrayUInt32("mp3_ll", [0] * max(1, mp3_count))
        env.newPropertyArrayUInt32("mp3_oh", [0] * max(1, mp3_count))
        env.newPropertyArrayUInt32("mp3_mfg_date_days", [0] * max(1, mp3_count))
        # MP6 как MacroProperty Arrays (атомики квот по дням, заполняются из MP4)
        try:
            env.newMacroPropertyArrayUInt32("mp6_quota_mi8", 1)
            env.newMacroPropertyArrayUInt32("mp6_quota_mi17", 1)
        except Exception:
            # В старых версиях pyflamegpu MacroProperty Array может отсутствовать; оставляем для будущей миграции
            pass

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

        # RTC: ремонт (временно отключён в smoke)
        # rtc_repair_src = r"""
        # FLAMEGPU_AGENT_FUNCTION(rtc_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
        #     unsigned int status_id = FLAMEGPU->getVariable<unsigned int>("status_id");
        #     if (status_id == 4u) {
        #         unsigned int rd = FLAMEGPU->getVariable<unsigned int>("repair_days") + 1u;
        #         FLAMEGPU->setVariable<unsigned int>("repair_days", rd);
        #         unsigned int rt = FLAMEGPU->getVariable<unsigned int>("repair_time");
        #         if (rd >= rt) {
        #             FLAMEGPU->setVariable<unsigned int>("status_id", 5u);
        #             FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
        #             FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
        #         }
        #     }
        #     return flamegpu::ALIVE;
        # }
        # """
        # agent.newRTCFunction("rtc_repair", rtc_repair_src)

        # RTC: ops_check (минимальная заглушка для smoke, без Env и атомиков)
        rtc_ops_check_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_ops_check, flamegpu::MessageNone, flamegpu::MessageNone) {
            if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
            // Временно не используем Env/атомики, только сброс ops_ticket
            FLAMEGPU->setVariable<unsigned int>("ops_ticket", 0u);
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

        # RTC: quota_init — сброс ops_ticket и инициализация скалярных макро‑квот на D+1 из MP4
        rtc_quota_init_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_quota_init, flamegpu::MessageNone, flamegpu::MessageNone) {
            // Сбрасываем билет допуска на сутки
            FLAMEGPU->setVariable<unsigned int>("ops_ticket", 0u);
            // Один агент (idx==0) инициализирует квоты MP6[D+1] из MP4
            if (FLAMEGPU->getVariable<unsigned int>("idx") == 0u) {
                unsigned int day = FLAMEGPU->getStepCounter();
                unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
                unsigned int dayp1 = (day + 1u < days_total ? day + 1u : day);
                unsigned int seed8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
                unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
                // Инициализируем одиночные макро‑квоты для текущих суток (совместимый путь)
                {
                    auto q8s  = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi8");
                    auto q17s = FLAMEGPU->environment.getMacroProperty<unsigned int>("remaining_ops_next_mi17");
                    q8s.exchange(seed8);
                    q17s.exchange(seed17);
                }
            }
            return flamegpu::ALIVE;
        }
        """
        agent.newRTCFunction("rtc_quota_init", rtc_quota_init_src)

        # RTC: probe чтение MP5 -> при необходимости заполнить daily_* (smoke-test; не меняет значения, если уже заданы)
        rtc_probe_mp5_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_probe_mp5, flamegpu::MessageNone, flamegpu::MessageNone) {
            unsigned int day = FLAMEGPU->getStepCounter();
            unsigned int N   = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
            unsigned int i   = FLAMEGPU->getVariable<unsigned int>("idx");
            unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
            unsigned int dayp1 = (day + 1u < days_total ? day + 1u : day);
            unsigned int linT = day * N + i;
            unsigned int linN = dayp1 * N + i;
            unsigned int dt = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", linT);
            unsigned int dn = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", linN);
            // Не затирать значения, если они уже установлены хостом
            if (FLAMEGPU->getVariable<unsigned int>("daily_today_u32") == 0u)
                FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
            if (FLAMEGPU->getVariable<unsigned int>("daily_next_u32") == 0u)
                FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
            return flamegpu::ALIVE;
        }
        """
        agent.newRTCFunction("rtc_probe_mp5", rtc_probe_mp5_src)

        # Слои: repair → quota_init → main → ops_check (решение на D+1 после инкремента)
        # Слои (без rtc_repair)
        lyr2 = model.newLayer()
        lyr2.addAgentFunction(agent.getFunction("rtc_quota_init"))
        lyr3 = model.newLayer()
        lyr3.addAgentFunction(agent.getFunction("rtc_probe_mp5"))
        lyr4 = model.newLayer()
        lyr4.addAgentFunction(agent.getFunction("rtc_main"))
        lyr5 = model.newLayer()
        lyr5.addAgentFunction(agent.getFunction("rtc_ops_check"))

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


