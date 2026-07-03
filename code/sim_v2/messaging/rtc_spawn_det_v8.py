#!/usr/bin/env python3
"""
RTC Deterministic Spawn V8 — device-birth детерминированного spawn (шаг P2)

Заменяет host-функцию HF_DeterministicSpawn: рождение агентов в состоянии
**serviceable** (status_id=3, pre_status_id=0) на детерминированные даты из
расписания mp4_new_counter_mi17_seed переносится на GPU по паттерну
manager+ticket (зеркало rtc_spawn_dynamic_v7.py, но birth в serviceable, а не
в operations).

Bit-identity контракт (сверено с HF_DeterministicSpawn.run):
- mfg_date и status_change_day = current_day (день срабатывания), НЕ spawn_day;
  поэтому все агенты, рождённые в один шаг, идентичны кроме idx/aircraft_number,
  и порядок внутри шага влияет только на contiguous idx.
- Catch-up: срабатывание при current_day >= spawn_day (адаптивный day-loop может
  перепрыгнуть точный день). Несколько spawn-дней могут сработать в одном шаге —
  их count суммируется, idx назначаются contiguous от общего курсора total_spawned.
- done_days gating: каждый spawn-день ровно один раз (MacroProperty det_spawn_done_days).
- Курсор total_spawned хранится как agent-variable DetSpawnMgr (RTC не может писать
  Environment scalar).
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import MAX_DAYS

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")

from string import Template


# ═══════════════════════════════════════════════════════════════════════════════
# RTC ФУНКЦИИ ДЕТЕРМИНИРОВАННОГО СПАВНА
# ═══════════════════════════════════════════════════════════════════════════════

RTC_DET_SPAWN_MGR_V8 = Template("""
FLAMEGPU_AGENT_FUNCTION(rtc_det_spawn_mgr_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    auto current_day_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int day = current_day_mp[0];
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int write_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));

    auto done_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("det_spawn_done_days");

    // Catch-up агрегация: суммируем count всех due (current_day >= spawn_day) ещё
    // не отработавших дней расписания. Восходящий порядок d зеркалит host
    // enumerate(spawn_seed); поскольку все агенты шага идентичны кроме idx/acn,
    // порядок влияет только на contiguous idx → bit-identical результат.
    unsigned int need = 0u;
    const unsigned int scan_days = (days_total <= ${MAX_DAYS}u ? days_total : ${MAX_DAYS}u);
    auto mp4_new_seed = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("mp4_new_counter_mi17_seed");
    for (unsigned int d = 0u; d < scan_days; ++d) {
        if (d > day) break;  // spawn_day в будущем: ещё не due (day-loop мог перепрыгнуть)
        const unsigned int cnt = mp4_new_seed[d];
        if (cnt == 0u) continue;
        // exchange возвращает старое значение done: считаем только при первой активации.
        const unsigned int old = done_mp[d].exchange(1u);
        if (old == 0u) {
            need += cnt;
        }
    }

    if (need == 0u) {
        return flamegpu::ALIVE;
    }

    const unsigned int total_spawned = FLAMEGPU->getVariable<unsigned int>("total_spawned");
    const unsigned int first_idx = FLAMEGPU->environment.getProperty<unsigned int>("det_spawn_first_idx_const");
    const unsigned int base_acn_const = FLAMEGPU->environment.getProperty<unsigned int>("det_spawn_base_acn_const");
    const unsigned int base_idx = first_idx + total_spawned;
    const unsigned int base_acn = base_acn_const + total_spawned;

    auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("det_spawn_need");
    auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("det_spawn_base_idx");
    auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("det_spawn_base_acn");
    need_mp[write_day].exchange(need);
    bidx_mp[write_day].exchange(base_idx);
    bacn_mp[write_day].exchange(base_acn);

    // Сдвигаем курсор (agent-variable, единственный агент → без гонки).
    FLAMEGPU->setVariable<unsigned int>("total_spawned", total_spawned + need);
    return flamegpu::ALIVE;
}
""").substitute(MAX_DAYS=MAX_DAYS)


RTC_DET_SPAWN_TICKET_V8 = Template("""
FLAMEGPU_AGENT_FUNCTION(rtc_det_spawn_ticket_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    auto current_day_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int day = current_day_mp[0];
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    const unsigned int ticket = FLAMEGPU->getVariable<unsigned int>("ticket");

    auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("det_spawn_need");
    const unsigned int need = need_mp[safe_day];
    if (ticket >= need) {
        return flamegpu::ALIVE;
    }

    auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("det_spawn_base_idx");
    auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("det_spawn_base_acn");
    const unsigned int new_idx = bidx_mp[safe_day] + ticket;
    const unsigned int new_acn = bacn_mp[safe_day] + ticket;

    // Нормативы Mi-17 (env-константы, читаются как в HF_DeterministicSpawn.env_consts).
    const unsigned int ll = FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const");
    const unsigned int oh = FLAMEGPU->environment.getProperty<unsigned int>("mi17_oh_const");
    const unsigned int br = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br_const");
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    const unsigned int assembly_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_assembly_time_const");
    const unsigned int partout_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_partout_time_const");

    // Рождение в serviceable (status_id=3). Набор и значения полей — дословно как в
    // HF_DeterministicSpawn.run (host-версия), кроме idx/aircraft_number.
    FLAMEGPU->agent_out.setVariable<unsigned int>("idx", new_idx);
    FLAMEGPU->agent_out.setVariable<unsigned int>("aircraft_number", new_acn);
    FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("group_by", 2u);  // Mi-17
    FLAMEGPU->agent_out.setVariable<unsigned int>("status_id", 3u);  // serviceable
    FLAMEGPU->agent_out.setVariable<unsigned int>("pre_status_id", 0u);  // spawn marker
    FLAMEGPU->agent_out.setVariable<unsigned int>("intent_state", 3u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("prev_intent", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("bi_counter", 1u);

    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_0_to_2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_2_to_3", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_2_to_6", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_2_to_7", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_3_to_2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_7_to_2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_1_to_2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_4_to_3", 0u);

    FLAMEGPU->agent_out.setVariable<unsigned int>("exit_date", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("sne", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("ppr", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("cso", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("daily_today_u32", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("daily_next_u32", 0u);

    FLAMEGPU->agent_out.setVariable<unsigned int>("ll", ll);
    FLAMEGPU->agent_out.setVariable<unsigned int>("second_ll", 0xFFFFFFFFu);  // sentinel
    FLAMEGPU->agent_out.setVariable<unsigned int>("oh", oh);
    FLAMEGPU->agent_out.setVariable<unsigned int>("br", br);

    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_time", repair_time);
    FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_time", assembly_time);
    FLAMEGPU->agent_out.setVariable<unsigned int>("partout_time", partout_time);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_trigger", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("active_trigger", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("partout_trigger", 0u);

    FLAMEGPU->agent_out.setVariable<unsigned int>("mfg_date", day);  // current_day, не spawn_day
    FLAMEGPU->agent_out.setVariable<unsigned int>("s4_days", 0u);

    FLAMEGPU->agent_out.setVariable<unsigned int>("limiter_date", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned short>("limiter", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("computed_adaptive_days", 1u);

    FLAMEGPU->agent_out.setVariable<unsigned int>("status_change_day", day);  // current_day
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_candidate", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_line_day", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_claim_start_day", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_claim_end_day", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_claim_source", 0u);

    FLAMEGPU->agent_out.setVariable<unsigned int>("promoted", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("needs_demote", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("commit_p1", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("commit_p2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("commit_p3", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("decision_p2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("decision_p3", 0u);

    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_promoted", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_needs_demote", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_repair_candidate", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_repair_line_id", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_repair_line_day", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("debug_bucket_seen", 0u);

    return flamegpu::ALIVE;
}
""").substitute(MAX_DAYS=MAX_DAYS)


def register_det_spawn_v8(model: fg.ModelDescription, heli_agent: fg.AgentDescription,
                          env: fg.EnvironmentDescription, det_spawn_count: int,
                          first_idx: int, base_acn: int) -> dict:
    """Регистрация device-birth детерминированного spawn (manager + ticket).

    Args:
        model: описание модели FLAME GPU
        heli_agent: агент HELI (планеры) — цель agent_out (serviceable)
        env: EnvironmentDescription для регистрации свойств/MacroProperty
        det_spawn_count: суммарное число детерминированных spawn (= число тикетов)
        first_idx: стартовый idx (first_reserved_idx) — как в host base_idx
        base_acn: стартовый aircraft_number (100000) — как в host base_acn
    """
    print("\n📦 V8: Детерминированный spawn (device, mgr+ticket)...")

    # Day-indexed MacroProperty (det_spawn_done_days уже создан в orchestrator).
    env.newMacroPropertyUInt("det_spawn_need", MAX_DAYS)
    env.newMacroPropertyUInt("det_spawn_base_idx", MAX_DAYS)
    env.newMacroPropertyUInt("det_spawn_base_acn", MAX_DAYS)

    # Константы курсора (читаются mgr на device).
    env.newPropertyUInt("det_spawn_first_idx_const", int(first_idx))
    env.newPropertyUInt("det_spawn_base_acn_const", int(base_acn))

    # Агент-менеджер (1 агент): курсор total_spawned.
    det_mgr = model.newAgent("DetSpawnMgr")
    det_mgr.newState("default")
    det_mgr.newVariableUInt("total_spawned", 0)

    # Агенты-тикеты (по одному на каждый потенциальный spawn).
    det_ticket = model.newAgent("DetSpawnTicket")
    det_ticket.newState("default")
    det_ticket.newVariableUInt("ticket", 0)

    # RTC функции
    mgr_fn = det_mgr.newRTCFunction("rtc_det_spawn_mgr_v8", RTC_DET_SPAWN_MGR_V8)
    mgr_fn.setInitialState("default")
    mgr_fn.setEndState("default")

    ticket_fn = det_ticket.newRTCFunction("rtc_det_spawn_ticket_v8", RTC_DET_SPAWN_TICKET_V8)
    ticket_fn.setAgentOutput(heli_agent, "serviceable")  # рождение в serviceable
    ticket_fn.setInitialState("default")
    ticket_fn.setEndState("default")

    # Слои строго на позиции старого layer_det_spawn (после save_pre_status, до квот).
    layer_mgr = model.newLayer("layer_det_spawn_mgr")
    layer_mgr.addAgentFunction(mgr_fn)
    layer_ticket = model.newLayer("layer_det_spawn_ticket")
    layer_ticket.addAgentFunction(ticket_fn)

    print(f"  ✅ Det spawn device: тикетов={det_spawn_count}, first_idx={first_idx}, base_acn={base_acn}")
    print(f"  ✅ Слои: layer_det_spawn_mgr, layer_det_spawn_ticket")

    return {
        'mgr_agent': det_mgr,
        'ticket_agent': det_ticket,
        'det_spawn_count': int(det_spawn_count),
        'first_idx': int(first_idx),
        'base_acn': int(base_acn),
    }


def init_det_spawn_population_v8(simulation: fg.CUDASimulation, model: fg.ModelDescription,
                                 det_spawn_count: int):
    """Инициализация популяции det-spawn агентов (менеджер + тикеты)."""
    mgr_pop = fg.AgentVector(model.getAgent("DetSpawnMgr"))
    mgr_pop.push_back()
    mgr_pop[0].setVariableUInt("total_spawned", 0)
    simulation.setPopulationData(mgr_pop, "default")

    ticket_pop = fg.AgentVector(model.getAgent("DetSpawnTicket"))
    for i in range(int(det_spawn_count)):
        ticket_pop.push_back()
        ticket_pop[i].setVariableUInt("ticket", i)
    simulation.setPopulationData(ticket_pop, "default")

    print(f"  ✅ Det spawn популяция: mgr=1, тикетов={det_spawn_count}")
