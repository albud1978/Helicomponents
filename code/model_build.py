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

# Константы для MacroProperty размеров
# RTC_MAX_FRAMES — ФИКСИРОВАННАЯ константа для компиляции RTC ядер
# Все RTC ядра компилируются с этим размером для кэширования
# Реальное количество агентов (frames_total) передаётся в Environment как runtime-переменная
RTC_MAX_FRAMES = 400  # Фиксированный размер для RTC (покрывает рост флота до 400 планеров)
MAX_FRAMES = 400      # Текущее значение (совпадает с RTC_MAX_FRAMES для единообразия кэша)
MAX_DAYS = 4000       # Максимальное количество дней симуляции (фиксированный размер буфера)
MAX_SIZE = MAX_FRAMES * (MAX_DAYS + 1)  # 400 * 4001 = 1,600,400

def set_max_frames_from_data(frames_count: int):
    """
    Логирует реальное количество агентов, но НЕ изменяет MAX_FRAMES.
    MAX_FRAMES фиксирован для единообразной компиляции RTC ядер.
    Реальное количество агентов (frames_count) передаётся в Environment.
    """
    global MAX_FRAMES, MAX_SIZE
    # НЕ меняем MAX_FRAMES — он фиксирован для RTC
    if frames_count > MAX_FRAMES:
        raise ValueError(
            f"❌ frames_count={frames_count} превышает RTC_MAX_FRAMES={MAX_FRAMES}! "
            "Увеличьте RTC_MAX_FRAMES в model_build.py"
        )
    print(f"📊 Реальных агентов: {frames_count}, RTC буфер: MAX_FRAMES={MAX_FRAMES}")
    print(f"Размеры MacroProperty: MAX_FRAMES={MAX_FRAMES}, MAX_DAYS={MAX_DAYS}, MAX_SIZE={MAX_SIZE}")


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
        minimal_env = os.environ.get("HL_ENV_MINIMAL", "0") == "1"
        enable_mp5 = os.environ.get("HL_ENV_ENABLE_MP5", "0") == "1"
        enable_mp1 = os.environ.get("HL_ENV_ENABLE_MP1", "0") == "1"
        enable_mp3 = os.environ.get("HL_ENV_ENABLE_MP3", "0") == "1"

        # Scalars
        env.newPropertyUInt("version_date", 0)
        env.newPropertyUInt("frames_total", 0)
        env.newPropertyUInt("days_total", 0)
        # Константы нормативов для MI-17 (используются в rtc_spawn_mi17_atomic)
        env.newPropertyUInt("mi17_repair_time_const", 0)
        env.newPropertyUInt("mi17_partout_time_const", 0)
        env.newPropertyUInt("mi17_assembly_time_const", 0)
        # Пороговые/нормативные значения MI-17 из MP1: BR/LL/OH
        env.newPropertyUInt("mi17_br_const", 0)
        env.newPropertyUInt("mi17_ll_const", 0)
        env.newPropertyUInt("mi17_oh_const", 0)
        # Число стартовых агентов (для next_idx_spawn)
        env.newPropertyUInt("frames_initial", 0)
        # MacroProperty 1D квоты по дням (инициализируются из MP4) — не обязательны для smoke
        if not minimal_env:
            env.newMacroPropertyUInt("mp4_quota_mi8", MAX_DAYS)
            env.newMacroPropertyUInt("mp4_quota_mi17", MAX_DAYS)
        # MacroProperty (FRAMES) для детерминированного менеджера квот (UInt32)
        env.newMacroPropertyUInt32("mi8_intent", MAX_FRAMES)
        env.newMacroPropertyUInt32("mi17_intent", MAX_FRAMES)
        env.newMacroPropertyUInt32("mi8_approve", MAX_FRAMES)
        env.newMacroPropertyUInt32("mi17_approve", MAX_FRAMES)

        # MP4 arrays всегда нужны для квот
        env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * MAX_DAYS)
        env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * MAX_DAYS)
        # План поставок (seed) и служебные массивы спавна
        env.newPropertyArrayUInt32("mp4_new_counter_mi17_seed", [0] * MAX_DAYS)
        # Вспомогательные MP для билетов спавна: по дням (размер MAX_DAYS)
        env.newMacroPropertyUInt("spawn_need_u32", MAX_DAYS)
        env.newMacroPropertyUInt("spawn_base_idx_u32", MAX_DAYS)
        env.newMacroPropertyUInt("spawn_base_acn_u32", MAX_DAYS)
        env.newMacroPropertyUInt("spawn_base_psn_u32", MAX_DAYS)
        # MacroProperty-счётчики для генерации идентификаторов спавна (как 1D массивы длиной 1)
        env.newMacroPropertyUInt("next_idx_spawn", 1)
        env.newMacroPropertyUInt("next_aircraft_no_mi17", 1)
        env.newMacroPropertyUInt("next_psn_mi17", 1)
        # Первый день месяца для mfg_date (ord days)
        env.newPropertyArrayUInt32("month_first_u32", [0] * MAX_DAYS)
        if not minimal_env or enable_mp5:
            # MP5 как MacroProperty с фиксированным размером
            env.newMacroPropertyUInt32("mp5_lin", MAX_SIZE)
        if not minimal_env or enable_mp1:
            # MP1 SoA
            env.newPropertyArrayUInt32("mp1_br_mi8", [0] * max(1, mp1_len))
            env.newPropertyArrayUInt32("mp1_br_mi17", [0] * max(1, mp1_len))
            env.newPropertyArrayUInt32("mp1_repair_time", [0] * max(1, mp1_len))
            env.newPropertyArrayUInt32("mp1_partout_time", [0] * max(1, mp1_len))
            env.newPropertyArrayUInt32("mp1_assembly_time", [0] * max(1, mp1_len))
            # Новые массивы OH по типам (минуты)
            env.newPropertyArrayUInt32("mp1_oh_mi8", [0] * max(1, mp1_len))
            env.newPropertyArrayUInt32("mp1_oh_mi17", [0] * max(1, mp1_len))
            env.newPropertyArrayUInt32("mp1_second_ll", [0] * max(1, mp1_len))
        if not minimal_env or enable_mp3:
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
            "mfg_date","status_id","repair_days","repair_time","assembly_time","partout_time","ppr","sne",
            "ll","oh","br","daily_today_u32","daily_next_u32","ops_ticket","quota_left","intent_flag",
            "active_trigger","assembly_trigger","partout_trigger","s6_days","s6_started"
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
            auto q8  = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("mp4_quota_mi8");
            auto q17 = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("mp4_quota_mi17");
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
        rtc_seed_mp4_quota_src = _seed_tpl.substitute(MAX_DAYS=str(MAX_DAYS))
        # Примечание: seed-функция не регистрируется, т.к. квоты читаются из MP4 напрямую в менеджере

        # RTC: probe_mp5 — временно отключено в smoke (починим отдельно с NVRTC-логом)

        # RTC: smoke no-op — временно отключено

        # RTC: Lq1 — intent (агенты помечают желание квоты)
        _intent_tpl = Template(
            """
        FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent, flamegpu::MessageNone, flamegpu::MessageNone) {
            const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
            if (phase != 0u) return flamegpu::ALIVE;
            static const unsigned int FRAMES = ${FRAMES}u;
            const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
            const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
            if (idx >= {MAX_FRAMES}u) return flamegpu::ALIVE;
            if (gb == 1u) { auto i8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_intent"); i8[idx].exchange(1u); }
            else if (gb == 2u) { auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_intent"); i17[idx].exchange(1u); }
            return flamegpu::ALIVE;
        }
            """
        )
        trivial_intent = os.environ.get("HL_TRIVIAL_INTENT", "0") == "1"
        if trivial_intent:
            rtc_quota_intent_src = """
        FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent, flamegpu::MessageNone, flamegpu::MessageNone) {
            return flamegpu::ALIVE;
        }
            """
        else:
            rtc_quota_intent_src = _intent_tpl.substitute(FRAMES=str(max(1, frames_total)))
        self.rtc_sources["rtc_quota_intent"] = rtc_quota_intent_src
        rtc_mode = os.environ.get("HL_RTC_MODE", "full").lower()
        enable_intent = rtc_mode in ("full", "intent_only")
        enable_approve = rtc_mode in ("full", "approve_only")
        enable_apply = rtc_mode in ("full",)

        if enable_intent:
            _safe_add_rtc_function(agent, "rtc_quota_intent", rtc_quota_intent_src)

        # RTC: Lq2 — approve (один менеджер idx==0 распределяет квоты по intent)
        _approve_tpl = Template(
            """
        FLAMEGPU_AGENT_FUNCTION(rtc_quota_approve_manager, flamegpu::MessageNone, flamegpu::MessageNone) {
            const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
            if (phase != 0u) return flamegpu::ALIVE;
            static const unsigned int FRAMES = ${FRAMES}u;
            if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
            const unsigned int last = (days_total > 0u ? days_total - 1u : 0u);
            const unsigned int dayp1 = (day < last ? day + 1u : last);
            auto i8  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_intent");
            auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_intent");
            auto a8  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
            auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
            for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) { a8[k].exchange(0u); a17[k].exchange(0u); }
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
            const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
            if (phase != 0u) return flamegpu::ALIVE;
            static const unsigned int FRAMES = ${FRAMES}u;
            const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
            const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
            if (idx >= {MAX_FRAMES}u) return flamegpu::ALIVE;
            if (gb == 1u) { auto a8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve"); if (a8[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u); }
            else if (gb == 2u) { auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve"); if (a17[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u); }
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
            const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
            if (gb != 1u && gb != 2u) return flamegpu::ALIVE;
            const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
            const unsigned int dayp1 = (day + 1u < days_total ? day + 1u : day);
            unsigned int seed8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
            unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
            if ((gb == 1u && idx < seed8) || (gb == 2u && idx < seed17)) {
                FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
            }
            return flamegpu::ALIVE;
        }
        """
        )
        rtc_quota_apply_simple_src = _apply_simple_tpl.substitute()

        # RTC: пост-слой чтения остатка квоты (для валидации)
        _readleft_tpl = Template(
            """
        FLAMEGPU_AGENT_FUNCTION(rtc_read_quota_left, flamegpu::MessageNone, flamegpu::MessageNone) {
            if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
            const unsigned int dayp1 = (day + 1u < days_total ? day + 1u : day);
            auto q8  = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("mp4_quota_mi8");
            unsigned int left = q8[dayp1];
            FLAMEGPU->setVariable<unsigned int>("quota_left", left);
            return flamegpu::ALIVE;
        }
        """
        )
        rtc_read_quota_left_src = _readleft_tpl.substitute(MAX_DAYS=str(MAX_DAYS))
        # Отключено: диагностика остатка квоты не используется в слоях и тянет DAYS
        # agent.newRTCFunction("rtc_read_quota_left", rtc_read_quota_left_src)

        # Layers: менеджер квот — intent → approve → apply
        if enable_intent:
            lyr1 = model.newLayer(); lyr1.addAgentFunction(agent.getFunction("rtc_quota_intent"))
        if enable_approve:
            lyr2 = model.newLayer(); lyr2.addAgentFunction(agent.getFunction("rtc_quota_approve_manager"))
        if enable_apply:
            lyr3 = model.newLayer(); lyr3.addAgentFunction(agent.getFunction("rtc_quota_apply"))

        # === Агент-тикет спавна (каждый порождает не более одного HELI) ===
        spawn_ticket = model.newAgent("spawn_ticket")
        spawn_ticket.newVariableUInt("ticket", 0)  # порядок внутри дня 0..K-1
        from string import Template as _T
        _spawn_tpl = _T(
            """
        FLAMEGPU_AGENT_FUNCTION(rtc_spawn_mi17_atomic, flamegpu::MessageNone, flamegpu::MessageNone) {
            if (FLAMEGPU->getVariable<unsigned int>("ticket") >= 0xFFFFFFFFu) return flamegpu::ALIVE; // safety
            const unsigned int k = FLAMEGPU->getVariable<unsigned int>("ticket");
            // Чтение подготовленных менеджером значений (по dню)
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
            const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
            auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_need_u32");
            auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_idx_u32");
            auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_acn_u32");
            auto bpsn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_psn_u32");
            const unsigned int need = need_mp[safe_day];
            if (k >= need) return flamegpu::ALIVE;
            const unsigned int idx = bidx_mp[safe_day] + k;
            const unsigned int acn = bacn_mp[safe_day] + k;
            const unsigned int psn = bpsn_mp[safe_day] + k;
            const unsigned int mfg = FLAMEGPU->environment.getProperty<unsigned int>("month_first_u32", safe_day);
            auto out = FLAMEGPU->agent_out;
            out.setVariable<unsigned int>("idx", idx);
            out.setVariable<unsigned int>("psn", psn);
            out.setVariable<unsigned int>("aircraft_number", acn);
            out.setVariable<unsigned int>("ac_type_mask", 64u);
            out.setVariable<unsigned int>("group_by", 2u);
            out.setVariable<unsigned int>("partseqno_i", 70482u);
            // Нормативы времени из env-констант для MI-17
            out.setVariable<unsigned int>("repair_time", FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const"));
            out.setVariable<unsigned int>("assembly_time", FLAMEGPU->environment.getProperty<unsigned int>("mi17_assembly_time_const"));
            out.setVariable<unsigned int>("partout_time", FLAMEGPU->environment.getProperty<unsigned int>("mi17_partout_time_const"));
            out.setVariable<unsigned int>("mfg_date", mfg);
            out.setVariable<unsigned int>("status_id", 3u);
            out.setVariable<unsigned int>("sne", 0u);
            out.setVariable<unsigned int>("ppr", 0u);
            out.setVariable<unsigned int>("repair_days", 0u);
            out.setVariable<unsigned int>("ops_ticket", 0u);
            out.setVariable<unsigned int>("intent_flag", 0u);
            // BR/LL/OH для MI-17 из ENV-констант
            out.setVariable<unsigned int>("ll", FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const"));
            out.setVariable<unsigned int>("oh", FLAMEGPU->environment.getProperty<unsigned int>("mi17_oh_const"));
            out.setVariable<unsigned int>("br", FLAMEGPU->environment.getProperty<unsigned int>("mi17_br_const"));
            out.setVariable<unsigned int>("daily_today_u32", 0u);
            out.setVariable<unsigned int>("daily_next_u32", 0u);
            out.setVariable<unsigned int>("active_trigger", 0u);
            out.setVariable<unsigned int>("assembly_trigger", 0u);
            out.setVariable<unsigned int>("partout_trigger", 0u);
            out.setVariable<unsigned int>("s6_days", 0u);
            out.setVariable<unsigned int>("s6_started", 0u);
            return flamegpu::ALIVE;
        }
            """
        )
        rtc_spawn_src = _spawn_tpl.substitute(DAYS=str(max(1, days_total)))
        self.rtc_sources["rtc_spawn_mi17_atomic"] = rtc_spawn_src
        if os.environ.get("HL_JIT_LOG", "0") == "1":
            try:
                print("\n===== RTC SOURCE: rtc_spawn_mi17_atomic =====\n" + rtc_spawn_src + "\n===== END SOURCE =====\n")
            except Exception:
                pass
        # Менеджер спавна: готовит базовые индексы и need (одиночный агент)
        spawn_mgr = model.newAgent("spawn_mgr")
        spawn_mgr.newVariableUInt("next_idx", 0)
        spawn_mgr.newVariableUInt("next_acn", 0)
        spawn_mgr.newVariableUInt("next_psn", 0)
        from string import Template as _TM
        _mgr_tpl = _TM(
            """
        FLAMEGPU_AGENT_FUNCTION(rtc_spawn_mgr, flamegpu::MessageNone, flamegpu::MessageNone) {
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
            const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
            const unsigned int need = FLAMEGPU->environment.getProperty<unsigned int>("mp4_new_counter_mi17_seed", safe_day);
            unsigned int nx = FLAMEGPU->getVariable<unsigned int>("next_idx");
            unsigned int na = FLAMEGPU->getVariable<unsigned int>("next_acn");
            unsigned int np = FLAMEGPU->getVariable<unsigned int>("next_psn");
            if (day == 0u) {
                const unsigned int frames_initial = FLAMEGPU->environment.getProperty<unsigned int>("frames_initial");
                if (nx < frames_initial) nx = frames_initial;
                if (na < 100000u) na = 100000u;
                if (np < 2000000u) np = 2000000u;
            }
            auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_need_u32");
            auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_idx_u32");
            auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_acn_u32");
            auto bpsn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_psn_u32");
            need_mp[safe_day].exchange(need);
            bidx_mp[safe_day].exchange(nx);
            bacn_mp[safe_day].exchange(na);
            bpsn_mp[safe_day].exchange(np);
            nx += need; na += need; np += need;
            FLAMEGPU->setVariable<unsigned int>("next_idx", nx);
            FLAMEGPU->setVariable<unsigned int>("next_acn", na);
            FLAMEGPU->setVariable<unsigned int>("next_psn", np);
            return flamegpu::ALIVE;
        }
            """
        )
        rtc_spawn_mgr_src = _mgr_tpl.substitute(DAYS=str(max(1, days_total)))
        self.rtc_sources["rtc_spawn_mgr"] = rtc_spawn_mgr_src
        spawn_mgr_fn = spawn_mgr.newRTCFunction("rtc_spawn_mgr", rtc_spawn_mgr_src)

        # Регистрируем тикеты спавна
        try:
            spawn_fn = spawn_ticket.newRTCFunction("rtc_spawn_mi17_atomic", rtc_spawn_src)
            spawn_fn.setAgentOutput(self.agent)
        except Exception as e:
            try:
                print(f"\n===== NVRTC ERROR in rtc_spawn_mi17_atomic (register) =====\n{e}\n----- SOURCE BEGIN -----\n{rtc_spawn_src}\n----- SOURCE END -----\n")
            except Exception:
                pass
            raise

        # Слои для спавна будут добавлены в самом конце шага (после логгера)

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
    env.newPropertyUInt("export_phase", 0)  # 0=sim, 1=copyout MP2, 2=postprocess MP2
    env.newPropertyUInt("export_day", 0)
    # Константы нормативов для MI-17 (для присвоения новорождённым при спавне)
    env.newPropertyUInt("mi17_repair_time_const", 0)
    env.newPropertyUInt("mi17_partout_time_const", 0)
    env.newPropertyUInt("mi17_assembly_time_const", 0)
    # Пороговые/нормативные значения MI-17 из MP1: BR/LL/OH
    env.newPropertyUInt("mi17_br_const", 0)
    env.newPropertyUInt("mi17_ll_const", 0)
    env.newPropertyUInt("mi17_oh_const", 0)
    enable_mp2 = os.environ.get("HL_ENABLE_MP2", "0") == "1"
    enable_mp2_post = os.environ.get("HL_ENABLE_MP2_POST", "0") == "1"
    env.newPropertyUInt("approve_policy", 0)  # 0 = по idx (детерминизм)
    # Буферы остатка квоты не требуются: остаток вычисляется во второй фазе как seed - used
    env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * MAX_DAYS)
    env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * MAX_DAYS)
    # Спавн: входные Property/скаляры и MacroProperty
    env.newPropertyUInt("frames_initial", 0)
    env.newPropertyArrayUInt32("mp4_new_counter_mi17_seed", [0] * MAX_DAYS)
    env.newPropertyArrayUInt32("month_first_u32", [0] * MAX_DAYS)
    env.newMacroPropertyUInt("spawn_need_u32", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_base_idx_u32", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_base_acn_u32", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_base_psn_u32", MAX_DAYS)
    env.newMacroPropertyUInt("next_idx_spawn", 1)
    env.newMacroPropertyUInt("next_aircraft_no_mi17", 1)
    env.newMacroPropertyUInt("next_psn_mi17", 1)
    # MP3 производственные даты (ord days) по кадрам для приоритизации 1→2
    env.newPropertyArrayUInt32("mp3_mfg_date_days", [0] * MAX_FRAMES)
    # Буферы менеджера квот (по кадрам)
    env.newMacroPropertyUInt32("mi8_intent", MAX_FRAMES)
    env.newMacroPropertyUInt32("mi17_intent", MAX_FRAMES)
    env.newMacroPropertyUInt32("mi8_approve", MAX_FRAMES)
    env.newMacroPropertyUInt32("mi17_approve", MAX_FRAMES)
    # Вторая фаза approve для статуса 3
    env.newMacroPropertyUInt32("mi8_approve_s3", MAX_FRAMES)
    env.newMacroPropertyUInt32("mi17_approve_s3", MAX_FRAMES)
    # Третья фаза approve для статуса 5
    env.newMacroPropertyUInt32("mi8_approve_s5", MAX_FRAMES)
    env.newMacroPropertyUInt32("mi17_approve_s5", MAX_FRAMES)
    # Четвёртая фаза approve для статуса 1
    env.newMacroPropertyUInt32("mi8_approve_s1", MAX_FRAMES)
    env.newMacroPropertyUInt32("mi17_approve_s1", MAX_FRAMES)
    
    # MP5 если включен probe
    if os.environ.get("HL_MP5_PROBE", "0") == "1":
        env.newMacroPropertyUInt32("mp5_lin", MAX_SIZE)

    # Агент и минимальные переменные
    agent = model.newAgent("component")
    agent.newVariableUInt("idx", 0)
    # Для спавна и идентификации
    agent.newVariableUInt("psn", 0)
    agent.newVariableUInt("partseqno_i", 0)
    agent.newVariableUInt("ac_type_mask", 0)
    agent.newVariableUInt("group_by", 0)
    agent.newVariableUInt("ops_ticket", 0)
    agent.newVariableUInt("daily_today_u32", 0)
    agent.newVariableUInt("daily_next_u32", 0)
    # Дата производства в ord-днях (UInt, трактуем как UInt16 по диапазону)
    agent.newVariableUInt("mfg_date", 0)
    # Для status_4 smoke
    agent.newVariableUInt("status_id", 0)
    agent.newVariableUInt("repair_days", 0)
    agent.newVariableUInt("repair_time", 0)
    agent.newVariableUInt("assembly_time", 0)
    agent.newVariableUInt("partout_time", 0)
    agent.newVariableUInt("s6_days", 0)
    agent.newVariableUInt("s6_started", 0)
    agent.newVariableUInt("sne", 0)
    agent.newVariableUInt("ppr", 0)
    # Для status_2 логики (LL/OH/BR пороги)
    agent.newVariableUInt("ll", 0)
    agent.newVariableUInt("oh", 0)
    agent.newVariableUInt("br", 0)
    # Диагностика переходов: aircraft_number
    agent.newVariableUInt("aircraft_number", 0)
    agent.newVariableUInt("intent_flag", 0)
    agent.newVariableUInt("active_trigger", 0)
    agent.newVariableUInt("assembly_trigger", 0)
    agent.newVariableUInt("partout_trigger", 0)
    # Однодневные маркеры событий
    agent.newVariableUInt("active_trigger_mark", 0)
    agent.newVariableUInt("assembly_trigger_mark", 0)
    agent.newVariableUInt("partout_trigger_mark", 0)

    # MP2 SoA (1D линейные массивы длиной MAX_FRAMES*MAX_DAYS)
    if enable_mp2:
        env.newMacroPropertyUInt32("mp2_status", MAX_FRAMES * MAX_DAYS)
        env.newMacroPropertyUInt32("mp2_repair_days", MAX_FRAMES * MAX_DAYS)
        env.newMacroPropertyUInt32("mp2_active_trigger", MAX_FRAMES * MAX_DAYS)
        env.newMacroPropertyUInt32("mp2_assembly_trigger", MAX_FRAMES * MAX_DAYS)

    # RTC: intent
    rtc_intent = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent, flamegpu::MessageNone, flamegpu::MessageNone) {{
        if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= {MAX_FRAMES}u) return flamegpu::ALIVE;
        if (gb == 1u) {{ auto i8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_intent"); i8[idx].exchange(1u); }}
        else if (gb == 2u) {{ auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_intent"); i17[idx].exchange(1u); }}
        FLAMEGPU->setVariable<unsigned int>("intent_flag", 1u);
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_intent", rtc_intent)

    # RTC: approve (менеджер idx==0). Политика выбирается по env.approve_policy (0=по idx)
    rtc_approve = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_approve_manager, flamegpu::MessageNone, flamegpu::MessageNone) {{
        if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int last = (days_total > 0u ? days_total - 1u : 0u);
        const unsigned int dayp1 = (day < last ? day + 1u : last);
        const unsigned int policy = FLAMEGPU->environment.getProperty<unsigned int>("approve_policy");
        auto i8  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_intent");
        auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_intent");
        auto a8  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
        auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
        for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) {{ a8[k].exchange(0u); a17[k].exchange(0u); }}
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

    # RTC: apply — учитывает approve из четырёх фаз (status 2, 3, 5 и 1)
    rtc_apply = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_apply, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= {MAX_FRAMES}u) return flamegpu::ALIVE;
        if (gb == 1u) {{
            auto a8  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
            auto a8b = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s3");
            auto a8c = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s5");
            auto a8d = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s1");
            if (a8[idx] || a8b[idx] || a8c[idx] || a8d[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
        }} else if (gb == 2u) {{
            auto a17  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
            auto a17b = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s3");
            auto a17c = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s5");
            auto a17d = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s1");
            if (a17[idx] || a17b[idx] || a17c[idx] || a17d[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
        }}
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_apply", rtc_apply)

    # RTC: intent для статуса 3 (используется во второй фазе квотирования)
    rtc_intent_s3 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent_s3, flamegpu::MessageNone, flamegpu::MessageNone) {{
        if (FLAMEGPU->getVariable<unsigned int>("status_id") != 3u) return flamegpu::ALIVE;
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= {MAX_FRAMES}u) return flamegpu::ALIVE;
        if (gb == 1u) {{ auto i8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_intent"); i8[idx].exchange(1u); }}
        else if (gb == 2u) {{ auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_intent"); i17[idx].exchange(1u); }}
        FLAMEGPU->setVariable<unsigned int>("intent_flag", 1u);
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_intent_s3", rtc_intent_s3)

    # RTC: approve для статуса 3 - использует остаток квоты от первой фазы
    rtc_approve_s3 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_approve_manager_s3, flamegpu::MessageNone, flamegpu::MessageNone) {{
        if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
        auto i8   = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_intent");
        auto i17  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_intent");
        auto a8   = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
        auto a17  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
        auto a8b  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s3");
        auto a17b = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s3");
        // Считаем сколько уже было одобрено в фазе статуса 2
        unsigned int used8 = 0u; unsigned int used17 = 0u;
        for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) {{ used8 += (a8[k] ? 1u : 0u); used17 += (a17[k] ? 1u : 0u); }}
        // Инициализируем семена на D+1 и вычисляем остаток
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int last = (days_total > 0u ? days_total - 1u : 0u);
        const unsigned int dayp1 = (day < last ? day + 1u : last);
        unsigned int seed8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
        unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
        unsigned int left8 = (seed8 > used8 ? (seed8 - used8) : 0u);
        unsigned int left17 = (seed17 > used17 ? (seed17 - used17) : 0u);
        // Очистим approve-буферы второй фазы и распределим остаток по intent статуса 3
        for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) {{ a8b[k].exchange(0u); a17b[k].exchange(0u); }}
        for (unsigned int k=0u;k<FRAMES && left8>0u;++k) {{ if (i8[k]) {{ a8b[k].exchange(1u); --left8; }} }}
        for (unsigned int k=0u;k<FRAMES && left17>0u;++k) {{ if (i17[k]) {{ a17b[k].exchange(1u); --left17; }} }}
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_approve_manager_s3", rtc_approve_s3)

    # RTC: intent для статуса 5 (третья фаза квотирования)
    rtc_intent_s5 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent_s5, flamegpu::MessageNone, flamegpu::MessageNone) {{
        if (FLAMEGPU->getVariable<unsigned int>("status_id") != 5u) return flamegpu::ALIVE;
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= {MAX_FRAMES}u) return flamegpu::ALIVE;
        if (gb == 1u) {{ auto i8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_intent"); i8[idx].exchange(1u); }}
        else if (gb == 2u) {{ auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_intent"); i17[idx].exchange(1u); }}
        FLAMEGPU->setVariable<unsigned int>("intent_flag", 1u);
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_intent_s5", rtc_intent_s5)

    # RTC: approve для статуса 5 — остаток после фаз 2 и 3
    rtc_approve_s5 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_approve_manager_s5, flamegpu::MessageNone, flamegpu::MessageNone) {{
        if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
        auto i8   = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_intent");
        auto i17  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_intent");
        auto a8   = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
        auto a17  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
        auto a8b  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s3");
        auto a17b = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s3");
        auto a8c  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s5");
        auto a17c = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s5");
        // Считаем сколько уже было одобрено в фазах 2 и 3
        unsigned int used8 = 0u; unsigned int used17 = 0u;
        for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) {{ used8 += (a8[k] ? 1u : 0u); used17 += (a17[k] ? 1u : 0u); }}
        for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) {{ used8 += (a8b[k] ? 1u : 0u); used17 += (a17b[k] ? 1u : 0u); }}
        // Остаток на D+1
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int last = (days_total > 0u ? days_total - 1u : 0u);
        const unsigned int dayp1 = (day < last ? day + 1u : last);
        unsigned int seed8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
        unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
        unsigned int left8 = (seed8 > used8 ? (seed8 - used8) : 0u);
        unsigned int left17 = (seed17 > used17 ? (seed17 - used17) : 0u);
        // Очистим approve-буферы третьей фазы и распределим остаток по intent статуса 5
        for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) {{ a8c[k].exchange(0u); a17c[k].exchange(0u); }}
        for (unsigned int k=0u;k<FRAMES && left8>0u;++k) {{ if (i8[k]) {{ a8c[k].exchange(1u); --left8; }} }}
        for (unsigned int k=0u;k<FRAMES && left17>0u;++k) {{ if (i17[k]) {{ a17c[k].exchange(1u); --left17; }} }}
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_approve_manager_s5", rtc_approve_s5)

    # RTC: probe MP5 (опционально) — пишет dt/dn в агентные переменные
    rtc_probe_mp5 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_probe_mp5, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= {MAX_FRAMES}u) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
        const unsigned int base = safe_day * {MAX_FRAMES}u + idx;
        const unsigned int base_next = base + {MAX_FRAMES}u;
        // Чтение из MacroProperty mp5_lin
        auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
        const unsigned int dt = mp[base];
        const unsigned int dn = (safe_day < days_total - 1u ? mp[base_next] : 0u);
        FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
        FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_probe_mp5", rtc_probe_mp5)

    # Слои: intent → approve → apply (с опциональным прологом mp5_probe)
    import os as _os
    if _os.environ.get("HL_MP5_PROBE", "0") == "1":
        l0 = model.newLayer(); l0.addAgentFunction(agent.getFunction("rtc_probe_mp5"))
    # Сброс в начале суток — определить функцию и гарантировать её первым слоем
    rtc_begin_day = """
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_begin_day, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
        if (phase != 0u) return flamegpu::ALIVE;
        // Сбрасываем билет допуска на новый цикл и флаг intent диагностики
        FLAMEGPU->setVariable<unsigned int>("ops_ticket", 0u);
        FLAMEGPU->setVariable<unsigned int>("intent_flag", 0u);
        // Однодневные значения событий — обнуляем на начало суток
        // Не трогаем active_trigger на начало суток, он живёт до конца суток перехода
        FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 0u);
        FLAMEGPU->setVariable<unsigned int>("partout_trigger", 0u);
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_quota_begin_day", rtc_begin_day)
    l0b = model.newLayer(); l0b.addAgentFunction(agent.getFunction("rtc_quota_begin_day"))
    # Опциональный слой status_4 smoke
    rtc_status4_src = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_status_4, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
        if (phase != 0u) return flamegpu::ALIVE;
        // Быстрый предикат
        if (FLAMEGPU->getVariable<unsigned int>("status_id") != 4u) return flamegpu::ALIVE;
        unsigned int d = FLAMEGPU->getVariable<unsigned int>("repair_days") + 1u;
        FLAMEGPU->setVariable<unsigned int>("repair_days", d);
        const unsigned int rt = FLAMEGPU->getVariable<unsigned int>("repair_time");
        const unsigned int pt = FLAMEGPU->getVariable<unsigned int>("partout_time");
        const unsigned int at = FLAMEGPU->getVariable<unsigned int>("assembly_time");
        // Абсолютная дата дня больше не требуется — значения триггеров однодневные (0/1)
        // Однократные события в оригинальные поля (без mark)
        if (d == pt) {{
            if (FLAMEGPU->getVariable<unsigned int>("partout_trigger") == 0u) {{
                FLAMEGPU->setVariable<unsigned int>("partout_trigger", 1u);
            }}
        }}
        // Assembly: единица за assembly_time дней до конца ремонта (флаг 0/1 на один день)
        if ((rt > d ? (rt - d) : 0u) == at) {{
            if (FLAMEGPU->getVariable<unsigned int>("assembly_trigger") == 0u) {{
                FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 1u);
            }}
        }}
        // Завершение ремонта: 4 -> 5
        if (d >= rt) {{
            FLAMEGPU->setVariable<unsigned int>("status_id", 5u);
            FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
            FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
        }}
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_status_4", rtc_status4_src)
    # Опциональный слой status_6: счётчик дней и флаг partout
    rtc_status6_src = """
    FLAMEGPU_AGENT_FUNCTION(rtc_status_6, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
        if (phase != 0u) return flamegpu::ALIVE;
        if (FLAMEGPU->getVariable<unsigned int>("status_id") != 6u) return flamegpu::ALIVE;
        // Не начинать счётчик и триггеры, если агент стартовал в 6 (нет факта перехода 2→6)
        if (FLAMEGPU->getVariable<unsigned int>("s6_started") == 0u) return flamegpu::ALIVE;
        unsigned int d6 = FLAMEGPU->getVariable<unsigned int>("s6_days");
        const unsigned int pt = FLAMEGPU->getVariable<unsigned int>("partout_time");
        // Инкремент только пока не достигли pt и при pt>0
        if (pt > 0u && d6 < pt) {
            unsigned int nd6 = (d6 < 65535u ? d6 + 1u : d6);
            FLAMEGPU->setVariable<unsigned int>("s6_days", nd6);
            if (nd6 == pt) {
                if (FLAMEGPU->getVariable<unsigned int>("partout_trigger") == 0u) {
                    FLAMEGPU->setVariable<unsigned int>("partout_trigger", 1u);
                }
            }
        }
        // После достижения pt счётчик не растёт, повторной простановки не будет
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_status_6", rtc_status6_src)
    # Опциональный слой status_2: начисление dt и проверки LL/OH с BR-веткой (без квот)
    rtc_status2_src = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_status_2, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
        if (phase != 0u) return flamegpu::ALIVE;
        if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
        // 1) Начисление dt
        const unsigned int dt = FLAMEGPU->getVariable<unsigned int>("daily_today_u32");
        const unsigned int dn = FLAMEGPU->getVariable<unsigned int>("daily_next_u32");
        unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
        unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
        if (dt) {{
            sne = sne + dt;
            ppr = ppr + dt;
            FLAMEGPU->setVariable<unsigned int>("sne", sne);
            FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
        }}
        // 2) Прогноз на завтра
        const unsigned int s_next = sne + dn;
        const unsigned int p_next = ppr + dn;
        const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
        const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
        const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
        // 3) LL-порог: если sne+dn >= ll -> немедленно 2->6
        if (s_next >= ll) {{
            FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
            FLAMEGPU->setVariable<unsigned int>("s6_days", 0u);
            FLAMEGPU->setVariable<unsigned int>("s6_started", 1u);
            return flamegpu::ALIVE;
        }}
        // 4) OH-порог: если ppr+dn >= oh, уточняем по BR
        if (p_next >= oh) {{
            if (s_next < br) {{
                FLAMEGPU->setVariable<unsigned int>("status_id", 4u);
                FLAMEGPU->setVariable<unsigned int>("repair_days", 1u);
            }} else {{
                FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
                FLAMEGPU->setVariable<unsigned int>("s6_days", 0u);
                FLAMEGPU->setVariable<unsigned int>("s6_started", 1u);
            }}
            return flamegpu::ALIVE;
        }}
        // 5) Квота: intent выставляется отдельным слоем rtc_quota_intent только для status_id=2
        // Иначе остаёмся в 2
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_status_2", rtc_status2_src)

    # RTC: логгер суток в MP2 (в конце шага)
    rtc_log_day_src = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_log_day, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
        if (phase != 0u) return flamegpu::ALIVE;
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= {MAX_FRAMES}u) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        if (day >= days_total) return flamegpu::ALIVE;
        const unsigned int row = day * {MAX_FRAMES}u + idx;
        auto a_stat = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * MAX_DAYS}u>("mp2_status");
        auto a_rd   = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * MAX_DAYS}u>("mp2_repair_days");
        auto a_act  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * MAX_DAYS}u>("mp2_active_trigger");
        auto a_asm  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * MAX_DAYS}u>("mp2_assembly_trigger");
        a_stat[row].exchange(FLAMEGPU->getVariable<unsigned int>("status_id"));
        a_rd[row].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
        a_act[row].exchange(FLAMEGPU->getVariable<unsigned int>("active_trigger"));
        a_asm[row].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_trigger"));
        return flamegpu::ALIVE;
    }}
    """
    if enable_mp2:
        agent.newRTCFunction("rtc_log_day", rtc_log_day_src)

    # RTC: постпроцессинг MP2 в фазе export_phase=2 (per-agent, один проход по дням)
    rtc_mp2_post_src = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_mp2_postprocess, flamegpu::MessageNone, flamegpu::MessageNone) {{
        // Выполнять только в фазе export_phase=2
        const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
        if (phase != 2u) return flamegpu::ALIVE;
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        if (i >= {MAX_FRAMES}u) return flamegpu::ALIVE;
        const unsigned int vdate = FLAMEGPU->environment.getProperty<unsigned int>("version_date");
        const unsigned int R = FLAMEGPU->getVariable<unsigned int>("repair_time");
        const unsigned int A = FLAMEGPU->getVariable<unsigned int>("assembly_time");
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        auto a_stat = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * MAX_DAYS}u>("mp2_status");
        auto a_rd   = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * MAX_DAYS}u>("mp2_repair_days");
        auto a_act  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * MAX_DAYS}u>("mp2_active_trigger");
        auto a_asm  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * MAX_DAYS}u>("mp2_assembly_trigger");

        bool processed = false;
        for (unsigned int d_set = 0u; d_set < days_total && !processed; ++d_set) {{
            const unsigned int row_set = d_set * {MAX_FRAMES}u + i;
            // Учитываем любое событие active_trigger>0 в день d_set; день d_set не модифицируем
            const unsigned int act_abs = a_act[row_set];
            if (act_abs > 0u) {{
                if (d_set == 0u) continue;   // e = d_set-1 невозможен
                // s = value(active_trigger) как абсолютный день → относительный индекс суток
                // day_abs = vdate + (day_idx + 1) ⇒ s_rel = act_abs - vdate - 1
                unsigned int s_rel = 0u;
                if (act_abs > vdate) {{
                    unsigned int tmp = act_abs - vdate; // >=1 при act_abs>vdate
                    s_rel = (tmp > 0u ? tmp - 1u : 0u);
                }}
                if (s_rel >= days_total) continue; // вне горизонта
                unsigned int e = d_set - 1u; // правый конец окна (день до d_set)
                if (s_rel > e) continue;     // пустое/некорректное окно
                // Проставляем окно [s..e]: status_id=4, repair_days=1..R (обрезка по горизонту)
                for (unsigned int d = s_rel; d <= e && d < days_total; ++d) {{
                    const unsigned int row = d * {MAX_FRAMES}u + i;
                    a_stat[row].exchange(4u);
                    const unsigned int rdv = (d - s_rel + 1u);
                    a_rd[row].exchange(rdv);
                }}
                // assembly_trigger = 1 в день (d_set - A)
                if (d_set >= A) {{
                    const unsigned int asm_day = d_set - A;
                    if (asm_day < days_total) {{
                        const unsigned int row_asm = asm_day * {MAX_FRAMES}u + i;
                        a_asm[row_asm].exchange(1u);
                    }}
                }}
                processed = true;
            }}
        }}
        return flamegpu::ALIVE;
    }}
    """
    if enable_mp2_post:
        agent.newRTCFunction("rtc_mp2_postprocess", rtc_mp2_post_src)

    # RTC: copyout из MP2 в агентные переменные для конкретного дня (export_phase=1)
    rtc_mp2_copyout_src = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_mp2_copyout, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
        if (phase != 1u) return flamegpu::ALIVE;
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        if (i >= {MAX_FRAMES}u) return flamegpu::ALIVE;
        unsigned int d = FLAMEGPU->environment.getProperty<unsigned int>("export_day");
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        if (d >= days_total) return flamegpu::ALIVE;
        const unsigned int row = d * {MAX_FRAMES}u + i;
        auto a_stat = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * MAX_DAYS}u>("mp2_status");
        auto a_rd   = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * MAX_DAYS}u>("mp2_repair_days");
        auto a_act  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * MAX_DAYS}u>("mp2_active_trigger");
        auto a_asm  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * MAX_DAYS}u>("mp2_assembly_trigger");
        FLAMEGPU->setVariable<unsigned int>("status_id", a_stat[row]);
        FLAMEGPU->setVariable<unsigned int>("repair_days", a_rd[row]);
        FLAMEGPU->setVariable<unsigned int>("active_trigger", a_act[row]);
        FLAMEGPU->setVariable<unsigned int>("assembly_trigger", a_asm[row]);
        return flamegpu::ALIVE;
    }}
    """
    if enable_mp2_post:
        agent.newRTCFunction("rtc_mp2_copyout", rtc_mp2_copyout_src)

    # Комбинированный порядок для 2/4/6 (как единый блок за один step)
    combined_246 = _os.environ.get("HL_STATUS246_SMOKE", "1") == "1"
    if combined_246:
        # FLAME GPU не позволяет несколько функций с одним и тем же state в одном Layer,
        # поэтому оформляем как три последовательных слоя в одном блоке шага.
        l_246a = model.newLayer(); l_246a.addAgentFunction(agent.getFunction("rtc_status_6"))
        l_246b = model.newLayer(); l_246b.addAgentFunction(agent.getFunction("rtc_status_4"))
        l_246c = model.newLayer(); l_246c.addAgentFunction(agent.getFunction("rtc_status_2"))
    else:
        if _os.environ.get("HL_STATUS4_SMOKE", "0") == "1":
            ls4 = model.newLayer(); ls4.addAgentFunction(agent.getFunction("rtc_status_4"))
        if _os.environ.get("HL_STATUS6_SMOKE", "0") == "1":
            ls6 = model.newLayer(); ls6.addAgentFunction(agent.getFunction("rtc_status_6"))
        if _os.environ.get("HL_STATUS2_SMOKE", "0") == "1":
            ls2 = model.newLayer(); ls2.addAgentFunction(agent.getFunction("rtc_status_2"))
    # begin_day уже добавлен как первый слой выше
    l1 = model.newLayer(); l1.addAgentFunction(agent.getFunction("rtc_quota_intent"))
    l2 = model.newLayer(); l2.addAgentFunction(agent.getFunction("rtc_quota_approve_manager"))
    l3 = model.newLayer(); l3.addAgentFunction(agent.getFunction("rtc_quota_apply"))
    # Очистка intent: отдельный слой после apply (без чтения intent в этом слое)
    rtc_intent_clear = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent_clear, flamegpu::MessageNone, flamegpu::MessageNone) {{
        if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
        auto i8  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_intent");
        auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_intent");
        for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) {{ i8[k].exchange(0u); }}
        for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) {{ i17[k].exchange(0u); }}
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_intent_clear", rtc_intent_clear)
    l3b = model.newLayer(); l3b.addAgentFunction(agent.getFunction("rtc_quota_intent_clear"))

    # Вторая фаза квотирования: статус 3 на остатке квоты после фазы статуса 2
    l3c = model.newLayer(); l3c.addAgentFunction(agent.getFunction("rtc_quota_intent_s3"))
    l3d = model.newLayer(); l3d.addAgentFunction(agent.getFunction("rtc_quota_approve_manager_s3"))
    l3e = model.newLayer(); l3e.addAgentFunction(agent.getFunction("rtc_quota_apply"))
    l3f = model.newLayer(); l3f.addAgentFunction(agent.getFunction("rtc_quota_intent_clear"))

    # Post-quota: 3→2 если получен билет
    rtc_status3_post = """
    FLAMEGPU_AGENT_FUNCTION(rtc_status_3_post_quota, flamegpu::MessageNone, flamegpu::MessageNone) {
        if (FLAMEGPU->getVariable<unsigned int>("status_id") != 3u) return flamegpu::ALIVE;
        if (FLAMEGPU->getVariable<unsigned int>("ops_ticket") == 1u) {
            FLAMEGPU->setVariable<unsigned int>("status_id", 2u);
        }
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_status_3_post_quota", rtc_status3_post);
    l3g = model.newLayer(); l3g.addAgentFunction(agent.getFunction("rtc_status_3_post_quota"))

    # Третья фаза квотирования: статус 5 на остатке после фаз 2 и 3
    l5a = model.newLayer(); l5a.addAgentFunction(agent.getFunction("rtc_quota_intent_s5"))
    l5b = model.newLayer(); l5b.addAgentFunction(agent.getFunction("rtc_quota_approve_manager_s5"))
    l5c = model.newLayer(); l5c.addAgentFunction(agent.getFunction("rtc_quota_apply"))
    l5d = model.newLayer(); l5d.addAgentFunction(agent.getFunction("rtc_quota_intent_clear"))

    # Четвёртая фаза квотирования: статус 1 на остатке после фаз 2,3 и 5
    rtc_intent_s1 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent_s1, flamegpu::MessageNone, flamegpu::MessageNone) {{
        if (FLAMEGPU->getVariable<unsigned int>("status_id") != 1u) return flamegpu::ALIVE;
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= {MAX_FRAMES}u) return flamegpu::ALIVE;
        // Гейт по сроку ремонта: квотируем только если (D+1) - version_date >= repair_time
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int vdate = FLAMEGPU->environment.getProperty<unsigned int>("version_date");
        const unsigned int dayp1_abs = vdate + (day + 1u);
        const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
        if ((dayp1_abs - vdate) < repair_time) return flamegpu::ALIVE;
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        if (gb == 1u) {{ auto i8 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_intent"); i8[idx].exchange(1u); }}
        else if (gb == 2u) {{ auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_intent"); i17[idx].exchange(1u); }}
        FLAMEGPU->setVariable<unsigned int>("intent_flag", 1u);
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_intent_s1", rtc_intent_s1)

    rtc_approve_s1 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_approve_manager_s1, flamegpu::MessageNone, flamegpu::MessageNone) {{
        if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int last = (days_total > 0u ? days_total - 1u : 0u);
        const unsigned int dayp1 = (day < last ? day + 1u : last);
        auto i8  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_intent");
        auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_intent");
        auto a8   = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
        auto a17  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
        auto a8b  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s3");
        auto a17b = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s3");
        auto a8c  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s5");
        auto a17c = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s5");
        auto a8d  = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s1");
        auto a17d = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s1");
        // Очистим буферы фазу S1
        for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) {{ a8d[k].exchange(0u); a17d[k].exchange(0u); }}
        // Остаток от семени после фаз 2,3,5
        unsigned int seed8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
        unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
        unsigned int used8 = 0u, used17 = 0u;
        for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) {{ if (a8[k] || a8b[k] || a8c[k]) ++used8; if (a17[k] || a17b[k] || a17c[k]) ++used17; }}
        unsigned int left8 = (seed8 > used8 ? (seed8 - used8) : 0u);
        unsigned int left17 = (seed17 > used17 ? (seed17 - used17) : 0u);
        // Приоритет: самые молодые по mfg_date (больший ordinal день)
        if (left8 > 0u) {{
            // Локальная отметка выбранных индексов, чтобы не читать/писать approve повторно
            bool picked8[FRAMES];
            for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) picked8[k] = false;
            while (left8 > 0u) {{
                unsigned int best_idx = FRAMES;
                unsigned int best_mfg = 0u;
                for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) {{
                    if (picked8[k]) continue;
                    if (i8[k]) {{
                        const unsigned int mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", k);
                        if (mfg >= best_mfg) {{ best_mfg = mfg; best_idx = k; }}
                    }}
                }}
                if (best_idx < FRAMES) {{ a8d[best_idx].exchange(1u); picked8[best_idx] = true; --left8; }}
                else break;
            }}
        }}
        if (left17 > 0u) {{
            bool picked17[FRAMES];
            for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) picked17[k] = false;
            while (left17 > 0u) {{
                unsigned int best_idx = FRAMES;
                unsigned int best_mfg = 0u;
                for (unsigned int k=0u;k<{MAX_FRAMES}u;++k) {{
                    if (picked17[k]) continue;
                    if (i17[k]) {{
                        const unsigned int mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", k);
                        if (mfg >= best_mfg) {{ best_mfg = mfg; best_idx = k; }}
                    }}
                }}
                if (best_idx < FRAMES) {{ a17d[best_idx].exchange(1u); picked17[best_idx] = true; --left17; }}
                else break;
            }}
        }}
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_approve_manager_s1", rtc_approve_s1)

    # Слои для статуса 1
    l1a = model.newLayer(); l1a.addAgentFunction(agent.getFunction("rtc_quota_intent_s1"))
    l1b = model.newLayer(); l1b.addAgentFunction(agent.getFunction("rtc_quota_approve_manager_s1"))
    l1c = model.newLayer(); l1c.addAgentFunction(agent.getFunction("rtc_quota_apply"))
    l1d = model.newLayer(); l1d.addAgentFunction(agent.getFunction("rtc_quota_intent_clear"))

    rtc_status1_post = """
    FLAMEGPU_AGENT_FUNCTION(rtc_status_1_post_quota, flamegpu::MessageNone, flamegpu::MessageNone) {
        if (FLAMEGPU->getVariable<unsigned int>("status_id") != 1u) return flamegpu::ALIVE;
        if (FLAMEGPU->getVariable<unsigned int>("ops_ticket") == 1u) {
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int vdate = FLAMEGPU->environment.getProperty<unsigned int>("version_date");
            const unsigned int dayp1_abs = vdate + (day + 1u);
            const unsigned int rt = FLAMEGPU->getVariable<unsigned int>("repair_time");
            unsigned int act = (dayp1_abs > rt ? (dayp1_abs - rt) : 0u);
            if (act > 65535u) act = 65535u;
            if (FLAMEGPU->getVariable<unsigned int>("active_trigger") == 0u && act > 0u) {
                FLAMEGPU->setVariable<unsigned int>("active_trigger", act);
            }
            // Сброс PPR при переходе 1 -> 2
            FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
            FLAMEGPU->setVariable<unsigned int>("status_id", 2u);
        }
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_status_1_post_quota", rtc_status1_post)
    l1e = model.newLayer(); l1e.addAgentFunction(agent.getFunction("rtc_status_1_post_quota"))

    # Post-quota: 5→2 если получен билет
    rtc_status5_post = """
    FLAMEGPU_AGENT_FUNCTION(rtc_status_5_post_quota, flamegpu::MessageNone, flamegpu::MessageNone) {
        if (FLAMEGPU->getVariable<unsigned int>("status_id") != 5u) return flamegpu::ALIVE;
        if (FLAMEGPU->getVariable<unsigned int>("ops_ticket") == 1u) {
            FLAMEGPU->setVariable<unsigned int>("status_id", 2u);
        }
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_status_5_post_quota", rtc_status5_post);
    l5e = model.newLayer(); l5e.addAgentFunction(agent.getFunction("rtc_status_5_post_quota"))

    # Post-quota: 2→3 если билет не получен (ставим после цикла статуса 3, чтобы не было двух переходов в сутки)
    rtc_status2_post = """
    FLAMEGPU_AGENT_FUNCTION(rtc_status_2_post_quota, flamegpu::MessageNone, flamegpu::MessageNone) {
        if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
        if (FLAMEGPU->getVariable<unsigned int>("ops_ticket") == 0u) {
            FLAMEGPU->setVariable<unsigned int>("status_id", 3u);
        }
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_status_2_post_quota", rtc_status2_post);
    l4 = model.newLayer(); l4.addAgentFunction(agent.getFunction("rtc_status_2_post_quota"))

    # Логгер суток MP2 в конце шага, но до спавна
    if enable_mp2:
        l_log = model.newLayer(); l_log.addAgentFunction(agent.getFunction("rtc_log_day"))

    # Добавим спавн в самый конец суток (после логгера и до постпроцессинга/копиаута)
    # Агенты спавна
    spawn_ticket = model.newAgent("spawn_ticket")
    spawn_ticket.newVariableUInt("ticket", 0)
    spawn_mgr = model.newAgent("spawn_mgr")
    spawn_mgr.newVariableUInt("next_idx", 0)
    spawn_mgr.newVariableUInt("next_acn", 0)
    spawn_mgr.newVariableUInt("next_psn", 0)

    from string import Template as _TM2
    _mgr_tpl2 = _TM2(
        """
    FLAMEGPU_AGENT_FUNCTION(rtc_spawn_mgr, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
        const unsigned int need = FLAMEGPU->environment.getProperty<unsigned int>("mp4_new_counter_mi17_seed", safe_day);
        unsigned int nx = FLAMEGPU->getVariable<unsigned int>("next_idx");
        unsigned int na = FLAMEGPU->getVariable<unsigned int>("next_acn");
        unsigned int np = FLAMEGPU->getVariable<unsigned int>("next_psn");
        if (day == 0u) {
            const unsigned int frames_initial = FLAMEGPU->environment.getProperty<unsigned int>("frames_initial");
            if (nx < frames_initial) nx = frames_initial;
            if (na < 100000u) na = 100000u;
            if (np < 2000000u) np = 2000000u;
        }
        auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_need_u32");
        auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_idx_u32");
        auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_acn_u32");
        auto bpsn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_psn_u32");
        need_mp[safe_day].exchange(need);
        bidx_mp[safe_day].exchange(nx);
        bacn_mp[safe_day].exchange(na);
        bpsn_mp[safe_day].exchange(np);
        nx += need; na += need; np += need;
        FLAMEGPU->setVariable<unsigned int>("next_idx", nx);
        FLAMEGPU->setVariable<unsigned int>("next_acn", na);
        FLAMEGPU->setVariable<unsigned int>("next_psn", np);
        return flamegpu::ALIVE;
    }
        """
    )
    spawn_mgr_fn = spawn_mgr.newRTCFunction("rtc_spawn_mgr", _mgr_tpl2.substitute(MAX_DAYS=str(MAX_DAYS)))

    _spawn_tpl2 = _TM2(
        """
    FLAMEGPU_AGENT_FUNCTION(rtc_spawn_mi17_atomic, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int k = FLAMEGPU->getVariable<unsigned int>("ticket");
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
        auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_need_u32");
        const unsigned int need = need_mp[safe_day];
        if (k >= need) return flamegpu::ALIVE;
        auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_idx_u32");
        auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_acn_u32");
        auto bpsn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_base_psn_u32");
        const unsigned int idx = bidx_mp[safe_day] + k;
        const unsigned int acn = bacn_mp[safe_day] + k;
        const unsigned int psn = bpsn_mp[safe_day] + k;
        const unsigned int mfg = FLAMEGPU->environment.getProperty<unsigned int>("month_first_u32", safe_day);
        auto out = FLAMEGPU->agent_out;
        out.setVariable<unsigned int>("idx", idx);
        out.setVariable<unsigned int>("psn", psn);
        out.setVariable<unsigned int>("aircraft_number", acn);
        out.setVariable<unsigned int>("ac_type_mask", 64u);
        out.setVariable<unsigned int>("group_by", 2u);
        out.setVariable<unsigned int>("partseqno_i", 70482u);
        // Нормативы времени из env-констант для MI-17
        out.setVariable<unsigned int>("repair_time", FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const"));
        out.setVariable<unsigned int>("assembly_time", FLAMEGPU->environment.getProperty<unsigned int>("mi17_assembly_time_const"));
        out.setVariable<unsigned int>("partout_time", FLAMEGPU->environment.getProperty<unsigned int>("mi17_partout_time_const"));
        out.setVariable<unsigned int>("mfg_date", mfg);
        out.setVariable<unsigned int>("status_id", 3u);
        out.setVariable<unsigned int>("sne", 0u);
        out.setVariable<unsigned int>("ppr", 0u);
        out.setVariable<unsigned int>("repair_days", 0u);
        out.setVariable<unsigned int>("ops_ticket", 0u);
        out.setVariable<unsigned int>("intent_flag", 0u);
        // BR/LL/OH для MI-17 из ENV-констант
        out.setVariable<unsigned int>("ll", FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const"));
        out.setVariable<unsigned int>("oh", FLAMEGPU->environment.getProperty<unsigned int>("mi17_oh_const"));
        out.setVariable<unsigned int>("br", FLAMEGPU->environment.getProperty<unsigned int>("mi17_br_const"));
        out.setVariable<unsigned int>("daily_today_u32", 0u);
        out.setVariable<unsigned int>("daily_next_u32", 0u);
        out.setVariable<unsigned int>("active_trigger", 0u);
        out.setVariable<unsigned int>("assembly_trigger", 0u);
        out.setVariable<unsigned int>("partout_trigger", 0u);
        out.setVariable<unsigned int>("s6_days", 0u);
        out.setVariable<unsigned int>("s6_started", 0u);
        return flamegpu::ALIVE;
    }
        """
    )
    spawn_fn2 = spawn_ticket.newRTCFunction("rtc_spawn_mi17_atomic", _spawn_tpl2.substitute(MAX_DAYS=str(MAX_DAYS)))
    spawn_fn2.setAgentOutput(agent)
    l_spawn_mgr = model.newLayer(); l_spawn_mgr.addAgentFunction(spawn_mgr_fn)
    l_spawn = model.newLayer(); l_spawn.addAgentFunction(spawn_fn2)

    # Слой постпроцессинга MP2 (выполнится только при export_phase=2)
    if enable_mp2_post:
        l_pp = model.newLayer(); l_pp.addAgentFunction(agent.getFunction("rtc_mp2_postprocess"))

    # Слой copyout MP2 (выполнится только при export_phase=1)
    if enable_mp2_post:
        l_co = model.newLayer(); l_co.addAgentFunction(agent.getFunction("rtc_mp2_copyout"))

    

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
