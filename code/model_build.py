#!/usr/bin/env python3
"""
HeliSim: минимальная сборка модели по GPU.md для Этапа 0
- Env: скаляры + MP1/MP3/MP4/MP5
- RTC: rtc_quota_init, rtc_probe_mp5
- Слои: quota_init → probe_mp5
Дата: 2025-08-28
"""

from __future__ import annotations
from typing import Optional, Dict

try:
    import pyflamegpu
except Exception:
    pyflamegpu = None


class HeliSimModel:
    def __init__(self) -> None:
        self.model: Optional["pyflamegpu.ModelDescription"] = None
        self.sim: Optional["pyflamegpu.CUDASimulation"] = None
        self.agent = None
        self.num_agents: int = 0

    def build_model(self, num_agents: int, env_sizes: Optional[Dict[str, int]] = None) -> Optional["pyflamegpu.ModelDescription"]:
        if pyflamegpu is None:
            return None
        self.num_agents = int(num_agents)
        model = pyflamegpu.ModelDescription("HeliSim")

        env = model.Environment()
        days_total = int((env_sizes or {}).get('days_total', 1))
        frames_total = int((env_sizes or {}).get('frames_total', 1))
        mp1_len = int((env_sizes or {}).get('mp1_len', 1))
        mp3_count = int((env_sizes or {}).get('mp3_count', self.num_agents))

        # Scalars
        env.newPropertyUInt("version_date", 0)
        env.newPropertyUInt("frames_total", 0)
        env.newPropertyUInt("days_total", 0)
        # Host fallback quotas (D+1)
        env.newPropertyUInt("quota_next_mi8", 0)
        env.newPropertyUInt("quota_next_mi17", 0)
        # Scalar MacroProperty (fallback for quotas)
        env.newMacroPropertyUInt32("remaining_ops_next_mi8", 1)
        env.newMacroPropertyUInt32("remaining_ops_next_mi17", 1)
        # MP6 MacroProperty Arrays (квоты по дням; атомики в RTC)
        try:
            env.newMacroPropertyArrayUInt32("mp6_quota_mi8", 1)
            env.newMacroPropertyArrayUInt32("mp6_quota_mi17", 1)
        except Exception:
            # В старых сборках pyflamegpu MacroPropertyArray может отсутствовать
            pass

        # MP4 arrays
        env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * max(1, days_total))
        env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * max(1, days_total))
        # MP5 linear array with padding
        env.newPropertyArrayUInt32("mp5_daily_hours", [0] * max(1, (days_total + 1) * frames_total))
        # MP1 SoA
        env.newPropertyArrayUInt32("mp1_br_mi8", [0] * max(1, mp1_len))
        env.newPropertyArrayUInt32("mp1_br_mi17", [0] * max(1, mp1_len))
        env.newPropertyArrayUInt32("mp1_repair_time", [0] * max(1, mp1_len))
        env.newPropertyArrayUInt32("mp1_partout_time", [0] * max(1, mp1_len))
        env.newPropertyArrayUInt32("mp1_assembly_time", [0] * max(1, mp1_len))
        # MP3 SoA
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

        # Если агентов не требуется (Env-only smoke), пропускаем агент и RTC
        if self.num_agents <= 0:
            self.model = model
            return model

        # Agent
        agent = model.newAgent("component")
        self.agent = agent
        for name in [
            "idx","psn","partseqno_i","group_by","aircraft_number","ac_type_mask",
            "mfg_date","status_id","repair_days","repair_time","ppr","sne",
            "ll","oh","br","daily_today_u32","daily_next_u32","ops_ticket"
        ]:
            agent.newVariableUInt(name, 0)

        # RTC: probe_mp5 — чтение dt/dn из MP5 (Property Array) в агентные переменные
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
            if (FLAMEGPU->getVariable<unsigned int>("daily_today_u32") == 0u)
                FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
            if (FLAMEGPU->getVariable<unsigned int>("daily_next_u32") == 0u)
                FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
            return flamegpu::ALIVE;
        }
        """
        agent.newRTCFunction("rtc_probe_mp5", rtc_probe_mp5_src)

        # RTC: smoke no-op
        rtc_smoke_src = r"""
        FLAMEGPU_AGENT_FUNCTION(rtc_smoke, flamegpu::MessageNone, flamegpu::MessageNone) {
            return flamegpu::ALIVE;
        }
        """
        agent.newRTCFunction("rtc_smoke", rtc_smoke_src)

        # Layers: только rtc_probe_mp5 (smoke)
        lyr3 = model.newLayer(); lyr3.addAgentFunction(agent.getFunction("rtc_probe_mp5"))

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

