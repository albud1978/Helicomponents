#!/usr/bin/env python3
"""
RTC Spawn Dynamic V7 — Динамический спавн для V7 архитектуры

Логика:
- После P3 промоута проверяем остаточный дефицит Mi-17
- Если дефицит > 0 и день >= repair_time → создаём новых агентов
- Агенты создаются СРАЗУ в operations (немедленное покрытие)

Отличия от baseline rtc_spawn_dynamic.py:
- Не использует intent_state
- Использует V7 count буферы (mi17_ops_count, mi17_svc_count, etc.)
- Учитывает промоутнутых через promoted флаг
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")

from string import Template


# ═══════════════════════════════════════════════════════════════════════════════
# RTC ФУНКЦИИ ДИНАМИЧЕСКОГО СПАВНА
# ═══════════════════════════════════════════════════════════════════════════════

RTC_SPAWN_DYNAMIC_MGR_V7 = Template("""
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_dynamic_mgr_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int target_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    const unsigned int write_day = target_day;
    
    // Условие активации: day >= repair_time
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    if (day < repair_time) {
        return flamegpu::ALIVE;
    }
    
    // Считаем текущее количество Mi-17 в operations
    // КРИТИЧНО: используем MAX_FRAMES чтобы учесть динамически спавненных агентов!
    auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_ops_count");
    unsigned int curr_ops = 0u;
    for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
        if (ops_count[i] == 1u) ++curr_ops;
    }
    // NOTE: выходы из ops обрабатываются общим квотированием
    
    // Считаем промоутнутых P1 (serviceable)
    auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_svc_count");
    unsigned int svc_available = 0u;
    for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
        if (svc_count[i] == 1u) ++svc_available;
    }
    
    // Считаем промоутнутых P2 (unserviceable)
    auto unsvc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_unsvc_ready_count");
    unsigned int unsvc_available = 0u;
    for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
        if (unsvc_count[i] == 1u) ++unsvc_available;
    }
    
    // Считаем промоутнутых P3 (inactive)
    auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_inactive_count");
    unsigned int inactive_available = 0u;
    for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
        if (inactive_count[i] == 1u) ++inactive_available;
    }
    
    // Целевое значение из MP4
    const unsigned int target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", target_day);
    
    // Каскадный расчёт: сколько промоутит P1, P2, P3
    unsigned int deficit_p1 = (target > curr_ops) ? (target - curr_ops) : 0u;
    unsigned int p1_will = (deficit_p1 < svc_available) ? deficit_p1 : svc_available;
    unsigned int after_p1 = curr_ops + p1_will;
    
    unsigned int deficit_p2 = (target > after_p1) ? (target - after_p1) : 0u;
    unsigned int p2_will = (deficit_p2 < unsvc_available) ? deficit_p2 : unsvc_available;
    unsigned int after_p2 = after_p1 + p2_will;
    
    unsigned int deficit_p3 = (target > after_p2) ? (target - after_p2) : 0u;
    unsigned int p3_will = (deficit_p3 < inactive_available) ? deficit_p3 : inactive_available;
    unsigned int after_p3 = after_p2 + p3_will;
    
    // Остаточный дефицит после всех промоутов
    if (after_p3 >= target) {
        return flamegpu::ALIVE;
    }
    
    unsigned int deficit = target - after_p3;
    
    // Курсоры
    unsigned int next_idx = FLAMEGPU->getVariable<unsigned int>("next_idx");
    unsigned int next_acn = FLAMEGPU->getVariable<unsigned int>("next_acn");
    unsigned int total_spawned = FLAMEGPU->getVariable<unsigned int>("total_spawned");
    const unsigned int dynamic_reserve = FLAMEGPU->environment.getProperty<unsigned int>("dynamic_reserve_mi17");
    
    // Доступный резерв
    unsigned int available = (total_spawned < dynamic_reserve) ? (dynamic_reserve - total_spawned) : 0u;
    unsigned int need = (deficit < available) ? deficit : available;
    
    if (need == 0u) {
        return flamegpu::ALIVE;
    }
    
    // Публикуем параметры в MacroProperty
    auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_need");
    auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_idx");
    auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_acn");
    
    need_mp[write_day].exchange(need);
    bidx_mp[write_day].exchange(next_idx);
    bacn_mp[write_day].exchange(next_acn);
    
    
    // Сдвигаем курсоры
    FLAMEGPU->setVariable<unsigned int>("next_idx", next_idx + need);
    FLAMEGPU->setVariable<unsigned int>("next_acn", next_acn + need);
    FLAMEGPU->setVariable<unsigned int>("total_spawned", total_spawned + need);
    
    return flamegpu::ALIVE;
}
""").substitute(MAX_FRAMES=RTC_MAX_FRAMES, MAX_DAYS=MAX_DAYS)

RTC_SPAWN_DYNAMIC_TICKET_V8 = Template("""
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_dynamic_ticket_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    const unsigned int ticket = FLAMEGPU->getVariable<unsigned int>("ticket");
    
    // Читаем параметры
    auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_need");
    auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_idx");
    auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_acn");
    
    const unsigned int need = need_mp[safe_day];
    const unsigned int base_idx = bidx_mp[safe_day];
    const unsigned int base_acn = bacn_mp[safe_day];
    
    if (ticket >= need) {
        return flamegpu::ALIVE;
    }
    
    // Создаём нового агента
    const unsigned int new_idx = base_idx + ticket;
    const unsigned int new_acn = base_acn + ticket;
    
    // Нормативы Mi-17
    const unsigned int ll = FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const");
    const unsigned int oh = FLAMEGPU->environment.getProperty<unsigned int>("mi17_oh_const");
    const unsigned int br = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br_const");
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    
    // Начальная наработка (новый вертолёт)
    const unsigned int sne_new = 0u;
    const unsigned int ppr_new = 0u;
    
    // Устанавливаем переменные агента
    FLAMEGPU->agent_out.setVariable<unsigned int>("idx", new_idx);
    FLAMEGPU->agent_out.setVariable<unsigned int>("aircraft_number", new_acn);
    FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("group_by", 2u);  // Mi-17
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("sne", sne_new);
    FLAMEGPU->agent_out.setVariable<unsigned int>("ppr", ppr_new);
    FLAMEGPU->agent_out.setVariable<unsigned int>("cso", 0u);
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("ll", ll);
    FLAMEGPU->agent_out.setVariable<unsigned int>("oh", oh);
    FLAMEGPU->agent_out.setVariable<unsigned int>("br", br);
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_time", repair_time);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("status_change_day", day);
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_5_to_2", 1u);
    FLAMEGPU->agent_out.setVariable<unsigned short>("limiter", 0u);
    
    return flamegpu::ALIVE;
}
""").substitute(MAX_DAYS=MAX_DAYS)

RTC_SPAWN_DYNAMIC_MGR_V8 = Template("""
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_dynamic_mgr_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int target_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    const unsigned int write_day = target_day;
    
    // Условие активации: day >= repair_time
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    if (day < repair_time) {
        return flamegpu::ALIVE;
    }
    
    // Текущее количество Mi-17 в operations
    auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_FRAMES}u>("mi17_ops_count");
    unsigned int curr_ops = 0u;
    for (unsigned int i = 0u; i < ${MAX_FRAMES}u; ++i) {
        if (ops_count[i] == 1u) ++curr_ops;
    }
    
    // Целевое значение из MP4 (текущий день)
    const unsigned int target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", target_day);
    FLAMEGPU->setVariable<unsigned int>("debug_curr_ops", curr_ops);
    FLAMEGPU->setVariable<unsigned int>("debug_target", target);
    if (curr_ops >= target) {
        FLAMEGPU->setVariable<unsigned int>("debug_need", 0u);
        return flamegpu::ALIVE;
    }
    
    unsigned int deficit = target - curr_ops;
    FLAMEGPU->setVariable<unsigned int>("debug_need", deficit);
    
    // Курсоры
    unsigned int next_idx = FLAMEGPU->getVariable<unsigned int>("next_idx");
    unsigned int next_acn = FLAMEGPU->getVariable<unsigned int>("next_acn");
    unsigned int total_spawned = FLAMEGPU->getVariable<unsigned int>("total_spawned");
    const unsigned int dynamic_reserve = FLAMEGPU->environment.getProperty<unsigned int>("dynamic_reserve_mi17");
    
    unsigned int available = (total_spawned < dynamic_reserve) ? (dynamic_reserve - total_spawned) : 0u;
    unsigned int need = (deficit < available) ? deficit : available;
    
    if (need == 0u) {
        return flamegpu::ALIVE;
    }
    
    auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_need");
    auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_idx");
    auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_acn");
    
    need_mp[write_day].exchange(need);
    bidx_mp[write_day].exchange(next_idx);
    bacn_mp[write_day].exchange(next_acn);
    
    FLAMEGPU->setVariable<unsigned int>("next_idx", next_idx + need);
    FLAMEGPU->setVariable<unsigned int>("next_acn", next_acn + need);
    FLAMEGPU->setVariable<unsigned int>("total_spawned", total_spawned + need);
    
    return flamegpu::ALIVE;
}
""").substitute(MAX_FRAMES=RTC_MAX_FRAMES, MAX_DAYS=MAX_DAYS)

RTC_SPAWN_DYNAMIC_TICKET_V7 = Template("""
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_dynamic_ticket_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    unsigned int step_days = mp_result[0];
    if (step_days == 0u) step_days = 1u;
    const unsigned int safe_day = ((day + step_days) < days_total ? (day + step_days) : (days_total > 0u ? days_total - 1u : 0u));
    const unsigned int ticket = FLAMEGPU->getVariable<unsigned int>("ticket");
    
    // Читаем параметры
    auto need_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_need");
    auto bidx_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_idx");
    auto bacn_mp = FLAMEGPU->environment.getMacroProperty<unsigned int, ${MAX_DAYS}u>("spawn_dynamic_base_acn");
    
    const unsigned int need = need_mp[safe_day];
    const unsigned int base_idx = bidx_mp[safe_day];
    const unsigned int base_acn = bacn_mp[safe_day];
    
    if (ticket >= need) {
        return flamegpu::ALIVE;
    }
    
    // Создаём нового агента
    const unsigned int new_idx = base_idx + ticket;
    const unsigned int new_acn = base_acn + ticket;
    
    // Нормативы Mi-17
    const unsigned int ll = FLAMEGPU->environment.getProperty<unsigned int>("mi17_ll_const");
    const unsigned int oh = FLAMEGPU->environment.getProperty<unsigned int>("mi17_oh_const");
    const unsigned int br = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br_const");
    const unsigned int repair_time = FLAMEGPU->environment.getProperty<unsigned int>("mi17_repair_time_const");
    
    // Начальная наработка (новый вертолёт)
    const unsigned int sne_new = 0u;
    const unsigned int ppr_new = 0u;
    
    // Устанавливаем переменные агента
    FLAMEGPU->agent_out.setVariable<unsigned int>("idx", new_idx);
    FLAMEGPU->agent_out.setVariable<unsigned int>("aircraft_number", new_acn);
    FLAMEGPU->agent_out.setVariable<unsigned int>("partseqno_i", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("group_by", 2u);  // Mi-17
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("sne", sne_new);
    FLAMEGPU->agent_out.setVariable<unsigned int>("ppr", ppr_new);
    FLAMEGPU->agent_out.setVariable<unsigned int>("cso", 0u);
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("ll", ll);
    FLAMEGPU->agent_out.setVariable<unsigned int>("oh", oh);
    FLAMEGPU->agent_out.setVariable<unsigned int>("br", br);
    
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_time", repair_time);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("exit_date", 0u);
    
    // V7 флаги
    FLAMEGPU->agent_out.setVariable<unsigned int>("promoted", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("needs_demote", 0u);
    
    // Limiter (будет вычислен в limiter_on_entry)
    FLAMEGPU->agent_out.setVariable<unsigned short>("limiter", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("computed_adaptive_days", 1u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("daily_today_u32", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("daily_next_u32", 0u);
    
    // Дополнительные переменные
    FLAMEGPU->agent_out.setVariable<unsigned int>("status_id", 2u);  // operations
    FLAMEGPU->agent_out.setVariable<unsigned int>("intent_state", 2u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("bi_counter", 1u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("mfg_date", day);
    FLAMEGPU->agent_out.setVariable<unsigned int>("second_ll", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_time", 30u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("partout_time", 20u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("assembly_trigger", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("active_trigger", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("partout_trigger", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("s4_days", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("limiter_date", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("prev_intent", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("status_change_day", day);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_candidate", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
    FLAMEGPU->agent_out.setVariable<unsigned int>("repair_line_day", 0xFFFFFFFFu);
    
    // Transitions (все 0) - только те что есть в модели
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_0_to_2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_2_to_3", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_2_to_6", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_2_to_7", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_3_to_2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_4_to_3", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_5_to_2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_7_to_2", 0u);
    FLAMEGPU->agent_out.setVariable<unsigned int>("transition_1_to_2", 0u);
    
    return flamegpu::ALIVE;
}
""").substitute(MAX_DAYS=MAX_DAYS)


def register_spawn_dynamic_v7(model: fg.ModelDescription, heli_agent: fg.AgentDescription, env_data: dict):
    """Регистрация динамического спавна V7"""
    print("\n📦 V7: Динамический спавн...")
    
    env = model.Environment()
    
    # Параметры
    first_dynamic_idx = env_data.get('first_dynamic_idx', 340)  # После всех существующих агентов
    dynamic_reserve_mi17 = env_data.get('dynamic_reserve_mi17', 50)
    base_acn_spawn = env_data.get('base_acn_spawn', 100000)
    
    env.newPropertyUInt("first_dynamic_idx", first_dynamic_idx)
    env.newPropertyUInt("dynamic_reserve_mi17", dynamic_reserve_mi17)
    
    # MacroProperty для параметров спавна
    env.newMacroPropertyUInt("spawn_dynamic_need", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_idx", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_acn", MAX_DAYS)
    
    # Агент-менеджер
    spawn_mgr = model.newAgent("SpawnDynamicMgr")
    spawn_mgr.newState("default")
    spawn_mgr.newVariableUInt("next_idx", first_dynamic_idx)
    spawn_mgr.newVariableUInt("next_acn", base_acn_spawn)
    spawn_mgr.newVariableUInt("total_spawned", 0)
    spawn_mgr.newVariableUInt("debug_curr_ops", 0)
    spawn_mgr.newVariableUInt("debug_target", 0)
    spawn_mgr.newVariableUInt("debug_need", 0)
    
    # Агенты-тикеты
    spawn_ticket = model.newAgent("SpawnDynamicTicket")
    spawn_ticket.newState("default")
    spawn_ticket.newVariableUInt("ticket", 0)
    
    # RTC функции
    mgr_fn = spawn_mgr.newRTCFunction("rtc_spawn_dynamic_mgr_v7", RTC_SPAWN_DYNAMIC_MGR_V7)
    mgr_fn.setInitialState("default")
    mgr_fn.setEndState("default")
    
    ticket_fn = spawn_ticket.newRTCFunction("rtc_spawn_dynamic_ticket_v7", RTC_SPAWN_DYNAMIC_TICKET_V7)
    ticket_fn.setAgentOutput(heli_agent, "operations")  # Создаём сразу в operations
    ticket_fn.setInitialState("default")
    ticket_fn.setEndState("default")
    
    # Слои (после P3 промоута)
    layer_mgr = model.newLayer("v7_spawn_dynamic_mgr")
    layer_mgr.addAgentFunction(mgr_fn)
    
    layer_ticket = model.newLayer("v7_spawn_dynamic_ticket")
    layer_ticket.addAgentFunction(ticket_fn)
    
    print(f"  ✅ Менеджер: first_idx={first_dynamic_idx}, reserve={dynamic_reserve_mi17}")
    print(f"  ✅ Слои: v7_spawn_dynamic_mgr, v7_spawn_dynamic_ticket")
    
    return {
        'mgr_agent': spawn_mgr,
        'ticket_agent': spawn_ticket,
        'first_dynamic_idx': first_dynamic_idx,
        'dynamic_reserve': dynamic_reserve_mi17,
        'base_acn': base_acn_spawn
    }


def register_spawn_dynamic_v8(model: fg.ModelDescription, heli_agent: fg.AgentDescription, env_data: dict):
    """Регистрация динамического спавна V8 (с учётом RepairLine слотов)"""
    print("\n📦 V8: Динамический спавн...")
    
    env = model.Environment()
    
    first_dynamic_idx = env_data.get('first_dynamic_idx', 340)
    dynamic_reserve_mi17 = env_data.get('dynamic_reserve_mi17', 50)
    base_acn_spawn = env_data.get('base_acn_spawn', 100000)
    
    env.newPropertyUInt("first_dynamic_idx", first_dynamic_idx)
    env.newPropertyUInt("dynamic_reserve_mi17", dynamic_reserve_mi17)
    
    env.newMacroPropertyUInt("spawn_dynamic_need", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_idx", MAX_DAYS)
    env.newMacroPropertyUInt("spawn_dynamic_base_acn", MAX_DAYS)
    
    spawn_mgr = model.newAgent("SpawnDynamicMgr")
    spawn_mgr.newState("default")
    spawn_mgr.newVariableUInt("next_idx", first_dynamic_idx)
    spawn_mgr.newVariableUInt("next_acn", base_acn_spawn)
    spawn_mgr.newVariableUInt("total_spawned", 0)
    spawn_mgr.newVariableUInt("debug_curr_ops", 0)
    spawn_mgr.newVariableUInt("debug_target", 0)
    spawn_mgr.newVariableUInt("debug_need", 0)
    
    spawn_ticket = model.newAgent("SpawnDynamicTicket")
    spawn_ticket.newState("default")
    spawn_ticket.newVariableUInt("ticket", 0)
    
    mgr_fn = spawn_mgr.newRTCFunction("rtc_spawn_dynamic_mgr_v8", RTC_SPAWN_DYNAMIC_MGR_V8)
    mgr_fn.setInitialState("default")
    mgr_fn.setEndState("default")
    
    ticket_fn = spawn_ticket.newRTCFunction("rtc_spawn_dynamic_ticket_v8", RTC_SPAWN_DYNAMIC_TICKET_V8)
    ticket_fn.setAgentOutput(heli_agent, "operations")
    ticket_fn.setInitialState("default")
    ticket_fn.setEndState("default")
    
    layer_mgr = model.newLayer("v8_spawn_dynamic_mgr")
    layer_mgr.addAgentFunction(mgr_fn)
    
    layer_ticket = model.newLayer("v8_spawn_dynamic_ticket")
    layer_ticket.addAgentFunction(ticket_fn)
    
    print(f"  ✅ Менеджер: first_idx={first_dynamic_idx}, reserve={dynamic_reserve_mi17}")
    print(f"  ✅ Слои: v8_spawn_dynamic_mgr, v8_spawn_dynamic_ticket")
    
    return {
        'mgr_agent': spawn_mgr,
        'ticket_agent': spawn_ticket,
        'first_dynamic_idx': first_dynamic_idx,
        'dynamic_reserve': dynamic_reserve_mi17,
        'base_acn': base_acn_spawn
    }


def init_spawn_dynamic_population_v7(simulation: fg.CUDASimulation, model: fg.ModelDescription, 
                                      first_dynamic_idx: int, dynamic_reserve: int, base_acn: int):
    """Инициализация популяции агентов спавна"""
    
    # Менеджер (1 агент)
    mgr_pop = fg.AgentVector(model.getAgent("SpawnDynamicMgr"))
    mgr_pop.push_back()
    mgr_pop[0].setVariableUInt("next_idx", first_dynamic_idx)
    mgr_pop[0].setVariableUInt("next_acn", base_acn)
    mgr_pop[0].setVariableUInt("total_spawned", 0)
    simulation.setPopulationData(mgr_pop, "default")
    
    # Тикеты (по количеству резерва)
    ticket_pop = fg.AgentVector(model.getAgent("SpawnDynamicTicket"))
    for i in range(dynamic_reserve):
        ticket_pop.push_back()
        ticket_pop[i].setVariableUInt("ticket", i)
    simulation.setPopulationData(ticket_pop, "default")
    
    print(f"  ✅ Spawn популяция: mgr next_idx={first_dynamic_idx}, тикетов={dynamic_reserve}")

