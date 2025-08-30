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
import os

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
        self.rtc_sources: Dict[str, str] = {}

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
        # MacroProperty 1D квоты по дням (инициализируются из MP4)
        env.newMacroPropertyUInt("mp4_quota_mi8", max(1, days_total))
        env.newMacroPropertyUInt("mp4_quota_mi17", max(1, days_total))
        # MacroProperty (FRAMES) для детерминированного менеджера квот (UInt32)
        env.newMacroPropertyUInt32("mi8_intent", max(1, frames_total))
        env.newMacroPropertyUInt32("mi17_intent", max(1, frames_total))
        env.newMacroPropertyUInt32("mi8_approve", max(1, frames_total))
        env.newMacroPropertyUInt32("mi17_approve", max(1, frames_total))

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
            "ll","oh","br","daily_today_u32","daily_next_u32","ops_ticket","quota_left"
        ]:
            agent.newVariableUInt(name, 0)

        # RTC: инициализация квот: копирование MP4[день] → MacroProperty массивы квот
        from string import Template
        def _safe_add_rtc_function(agent_local, name: str, src: str) -> None:
            try:
                agent_local.newRTCFunction(name, src)
            except Exception as e:
                try:
                    print(f"\n===== NVRTC ERROR in {name} =====\n{e}\n----- SOURCE BEGIN -----\n{src}\n----- SOURCE END -----\n")
                except Exception:
                    pass
                raise
        _seed_tpl = Template(
            """
        FLAMEGPU_AGENT_FUNCTION(rtc_seed_mp4_quota, flamegpu::MessageNone, flamegpu::MessageNone) {
            // Выполняет только один агент (idx==0) для избежания гонок при инициализации
            if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
            static const unsigned int DAYS = ${DAYS}u;
            auto q8  = FLAMEGPU->environment.getMacroProperty<unsigned int, DAYS>("mp4_quota_mi8");
            auto q17 = FLAMEGPU->environment.getMacroProperty<unsigned int, DAYS>("mp4_quota_mi17");
            // Для smoke: инициализируем только D+1 (при day=0 → индекс 1, иначе 0)
            const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
            const unsigned int d1 = (days_total > 1u ? 1u : 0u);
            const unsigned int s8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", d1);
            const unsigned int s17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", d1);
            q8[d1].exchange(s8);
            q17[d1].exchange(s17);
            return flamegpu::ALIVE;
        }
            """
        )
        rtc_seed_mp4_quota_src = _seed_tpl.substitute(DAYS=str(max(1, days_total)))
        # Примечание: seed-функция не регистрируется, т.к. квоты читаются из MP4 напрямую в менеджере

        # RTC: probe_mp5 — временно отключено в smoke (починим отдельно с NVRTC-логом)

        # RTC: smoke no-op — временно отключено

        # RTC: Lq1 — intent (агенты помечают желание квоты)
        _intent_tpl = Template(
            """
        FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent, flamegpu::MessageNone, flamegpu::MessageNone) {
            static const unsigned int FRAMES = ${FRAMES}u;
            const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
            const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
            if (idx >= FRAMES) return flamegpu::ALIVE;
            if (gb == 1u) { auto i8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent"); i8[idx].exchange(1u); }
            else if (gb == 2u) { auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent"); i17[idx].exchange(1u); }
            return flamegpu::ALIVE;
        }
            """
        )
        rtc_quota_intent_src = _intent_tpl.substitute(FRAMES=str(max(1, frames_total)))
        self.rtc_sources["rtc_quota_intent"] = rtc_quota_intent_src
        rtc_mode = os.environ.get("HL_RTC_MODE", "full").lower()
        enable_intent = True
        enable_approve = rtc_mode in ("full", "approve_only")
        enable_apply = rtc_mode in ("full",)

        _safe_add_rtc_function(agent, "rtc_quota_intent", rtc_quota_intent_src)

        # RTC: Lq2 — approve (один менеджер idx==0 распределяет квоты по intent)
        _approve_tpl = Template(
            """
        FLAMEGPU_AGENT_FUNCTION(rtc_quota_approve_manager, flamegpu::MessageNone, flamegpu::MessageNone) {
            static const unsigned int FRAMES = ${FRAMES}u;
            if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
            const unsigned int last = (days_total > 0u ? days_total - 1u : 0u);
            const unsigned int dayp1 = (day < last ? day + 1u : last);
            auto i8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
            auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent");
            auto a8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve");
            auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve");
            for (unsigned int k=0u;k<FRAMES;++k) { a8[k].exchange(0u); a17[k].exchange(0u); }
            unsigned int left8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
            unsigned int left17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
            for (unsigned int k=0u;k<FRAMES && left8>0u;++k) { if (i8[k]) { a8[k].exchange(1u); --left8; } }
            for (unsigned int k=0u;k<FRAMES && left17>0u;++k) { if (i17[k]) { a17[k].exchange(1u); --left17; } }
            return flamegpu::ALIVE;
        }
            """
        )
        rtc_quota_approve_manager_src = _approve_tpl.substitute(FRAMES=str(max(1, frames_total)))
        self.rtc_sources["rtc_quota_approve_manager"] = rtc_quota_approve_manager_src
        if enable_approve:
            _safe_add_rtc_function(agent, "rtc_quota_approve_manager", rtc_quota_approve_manager_src)

        # RTC: Lq3 — apply (агенты принимают решение по approve)
        _apply_tpl = Template(
            """
        FLAMEGPU_AGENT_FUNCTION(rtc_quota_apply, flamegpu::MessageNone, flamegpu::MessageNone) {
            static const unsigned int FRAMES = ${FRAMES}u;
            const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
            const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
            if (idx >= FRAMES) return flamegpu::ALIVE;
            if (gb == 1u) { auto a8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve"); if (a8[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u); }
            else if (gb == 2u) { auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve"); if (a17[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u); }
            return flamegpu::ALIVE;
        }
            """
        )
        rtc_quota_apply_src = _apply_tpl.substitute(FRAMES=str(max(1, frames_total)))
        self.rtc_sources["rtc_quota_apply"] = rtc_quota_apply_src
        if enable_apply:
            _safe_add_rtc_function(agent, "rtc_quota_apply", rtc_quota_apply_src)

        # RTC: простой smoke — выдача квоты по правилу idx < seed (без intent/approve)
        _apply_simple_tpl = Template(
            """
        FLAMEGPU_AGENT_FUNCTION(rtc_quota_apply_simple, flamegpu::MessageNone, flamegpu::MessageNone) {
            static const unsigned int DAYS = ${DAYS}u;
            const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
            if (gb != 1u && gb != 2u) return flamegpu::ALIVE;
            const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int dayp1 = (day + 1u < DAYS ? day + 1u : day);
            unsigned int seed8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
            unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
            if ((gb == 1u && idx < seed8) || (gb == 2u && idx < seed17)) {
                FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
            }
            return flamegpu::ALIVE;
        }
            """
        )
        rtc_quota_apply_simple_src = _apply_simple_tpl.substitute(DAYS=str(max(1, days_total)))

        # RTC: пост-слой чтения остатка квоты (для валидации)
        _readleft_tpl = Template(
            """
        FLAMEGPU_AGENT_FUNCTION(rtc_read_quota_left, flamegpu::MessageNone, flamegpu::MessageNone) {
            if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
            static const unsigned int DAYS = ${DAYS}u;
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int dayp1 = (day + 1u < DAYS ? day + 1u : day);
            auto q8  = FLAMEGPU->environment.getMacroProperty<unsigned int, DAYS>("mp4_quota_mi8");
            unsigned int left = q8[dayp1];
            FLAMEGPU->setVariable<unsigned int>("quota_left", left);
            return flamegpu::ALIVE;
        }
            """
        )
        rtc_read_quota_left_src = _readleft_tpl.substitute(DAYS=str(max(1, days_total)))
        # Отключено: диагностика остатка квоты не используется в слоях и тянет DAYS
        # agent.newRTCFunction("rtc_read_quota_left", rtc_read_quota_left_src)

        # Layers: менеджер квот — intent → approve → apply
        lyr1 = model.newLayer(); lyr1.addAgentFunction(agent.getFunction("rtc_quota_intent"))
        if enable_approve:
            lyr2 = model.newLayer(); lyr2.addAgentFunction(agent.getFunction("rtc_quota_approve_manager"))
        if enable_apply:
            lyr3 = model.newLayer(); lyr3.addAgentFunction(agent.getFunction("rtc_quota_apply"))

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


# === Фабрики сборки моделей (центр, используемый оркестратором) ===

def build_model_for_quota_smoke(frames_total: int, days_total: int):
    """Минимальная модель для smoke intent→approve→apply без лишних зависимостей.
    Возвращает кортеж (model, agent_desc).
    """
    try:
        import pyflamegpu as fg
    except Exception as e:
        raise RuntimeError(f"pyflamegpu не установлен: {e}")

    FRAMES = max(1, int(frames_total))
    DAYS = max(1, int(days_total))

    model = fg.ModelDescription("GPUQuotaFactory")
    env = model.Environment()
    # Скаляры и PropertyArrays (источник квот — MP4[D+1])
    env.newPropertyUInt("version_date", 0)
    env.newPropertyUInt("frames_total", 0)
    env.newPropertyUInt("days_total", 0)
    env.newPropertyUInt("approve_policy", 0)  # 0 = по idx (детерминизм)
    env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * DAYS)
    env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * DAYS)
    # Буферы менеджера квот (по кадрам)
    env.newMacroPropertyUInt32("mi8_intent", FRAMES)
    env.newMacroPropertyUInt32("mi17_intent", FRAMES)
    env.newMacroPropertyUInt32("mi8_approve", FRAMES)
    env.newMacroPropertyUInt32("mi17_approve", FRAMES)

    # Агент и минимальные переменные
    agent = model.newAgent("component")
    agent.newVariableUInt("idx", 0)
    agent.newVariableUInt("group_by", 0)
    agent.newVariableUInt("ops_ticket", 0)

    # RTC: intent
    rtc_intent = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FRAMES = {FRAMES}u;
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= FRAMES) return flamegpu::ALIVE;
        if (gb == 1u) {{ auto i8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent"); i8[idx].exchange(1u); }}
        else if (gb == 2u) {{ auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent"); i17[idx].exchange(1u); }}
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_intent", rtc_intent)

    # RTC: approve (менеджер idx==0). Политика выбирается по env.approve_policy (0=по idx)
    rtc_approve = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_approve_manager, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int DAYS = {DAYS}u;
        static const unsigned int FRAMES = {FRAMES}u;
        if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int dayp1 = (day + 1u < DAYS ? day + 1u : day);
        const unsigned int policy = FLAMEGPU->environment.getProperty<unsigned int>("approve_policy");
        auto i8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
        auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent");
        auto a8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve");
        auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve");
        for (unsigned int k=0u;k<FRAMES;++k) {{ a8[k].exchange(0u); a17[k].exchange(0u); }}
        unsigned int left8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
        unsigned int left17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
        // policy 0: скан по idx (детерминированно)
        if (policy == 0u) {{
            for (unsigned int k=0u;k<FRAMES && left8>0u;++k) {{ if (i8[k]) {{ a8[k].exchange(1u); --left8; }} }}
            for (unsigned int k=0u;k<FRAMES && left17>0u;++k) {{ if (i17[k]) {{ a17[k].exchange(1u); --left17; }} }}
        }} else {{
            // Резерв под будущие политики (sne/ppr/seed)
            for (unsigned int k=0u;k<FRAMES && left8>0u;++k) {{ if (i8[k]) {{ a8[k].exchange(1u); --left8; }} }}
            for (unsigned int k=0u;k<FRAMES && left17>0u;++k) {{ if (i17[k]) {{ a17[k].exchange(1u); --left17; }} }}
        }}
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_approve_manager", rtc_approve)

    # RTC: apply
    rtc_apply = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_apply, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FRAMES = {FRAMES}u;
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= FRAMES) return flamegpu::ALIVE;
        if (gb == 1u) {{ auto a8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve"); if (a8[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u); }}
        else if (gb == 2u) {{ auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve"); if (a17[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u); }}
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_apply", rtc_apply)

    # Слои: intent → approve → apply
    l1 = model.newLayer(); l1.addAgentFunction(agent.getFunction("rtc_quota_intent"))
    l2 = model.newLayer(); l2.addAgentFunction(agent.getFunction("rtc_quota_approve_manager"))
    l3 = model.newLayer(); l3.addAgentFunction(agent.getFunction("rtc_quota_apply"))

    return model, agent


def build_model_full(frames_total: int, days_total: int, with_logging_layer: bool = True):
    """Обёртка над HeliSimModel для полного пайпа; возвращает (model, agent, sim_builder).
    sim_builder = функция, которая создаёт CUDASimulation поверх собранной модели.
    """
    try:
        import pyflamegpu  # noqa:F401
    except Exception as e:
        raise RuntimeError(f"pyflamegpu не установлен: {e}")

    hm = HeliSimModel()
    hm.build_model(num_agents=max(1, int(frames_total)), env_sizes={
        'days_total': int(days_total),
        'frames_total': int(frames_total),
        'mp1_len': 1,
        'mp3_count': max(1, int(frames_total)),
    })

    def _mk_sim():
        return hm.build_simulation()

    return hm.model, hm.agent, _mk_sim
