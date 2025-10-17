#!/usr/bin/env python3
"""
RTC модуль для подсчёта агентов в operations и serviceable
Должен выполняться ПОСЛЕ state_2_operations (когда intent уже установлен)

ВАРИАНТ B: Считаем агентов по intent=2, а не по state
- operations агенты с intent=2 → хотят остаться в operations
- serviceable без проверки intent (для ранжирования в промоуте)

ВАЖНО: Первый агент (idx=0) сбрасывает ВСЕ буферы перед подсчётом!
"""

import pyflamegpu as fg

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Подсчёт агентов в operations для каждого group_by"""
    
    # =========================================================================
    # Слой 1: Обнуление буферов (выполняет только первый агент)
    # =========================================================================
    max_frames = model.Environment().getPropertyUInt("frames_total")
    max_days = model.Environment().getPropertyUInt("days_total")
    MP2_SIZE = max_frames * (max_days + 1)
    
    RTC_RESET_BUFFERS = """
FLAMEGPU_AGENT_FUNCTION(rtc_reset_quota_buffers, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Только первый агент (idx=0) обнуляет буферы
    if (idx == 0u) {
        auto mi8_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi8_ops_count");
        auto mi17_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_ops_count");
        auto mi8_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi8_svc_count");
        auto mi17_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_svc_count");
        auto mi8_res = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi8_reserve_count");
        auto mi17_res = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_reserve_count");
        auto mi8_ina = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi8_inactive_count");
        auto mi17_ina = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_inactive_count");
        for (unsigned int i = 0u; i < 286u; ++i) {
            mi8_ops[i].exchange(0u);
            mi17_ops[i].exchange(0u);
            mi8_svc[i].exchange(0u);
            mi17_svc[i].exchange(0u);
        }
    }
    
    return flamegpu::ALIVE;
}
"""
    
    rtc_reset = agent.newRTCFunction("rtc_reset_quota_buffers", RTC_RESET_BUFFERS)
    # ✅ ИСПРАВЛЕНИЕ: Убираем фильтр по state, чтобы reset срабатывал для ВСЕХ агентов
    # Только первый агент (idx=0) сбросит буферы, остальные просто пройдут
    # rtc_reset.setInitialState("operations")  ← УДАЛЕНО
    # rtc_reset.setEndState("operations")      ← УДАЛЕНО
    
    layer_reset = model.newLayer("reset_quota_buffers")
    layer_reset.addAgentFunction(rtc_reset)
    
    # =========================================================================
    # Слой 2: Подсчёт агентов в operations с intent=2
    # =========================================================================
    RTC_COUNT_OPS = """
FLAMEGPU_AGENT_FUNCTION(rtc_count_ops, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // ✅ ВАРИАНТ B: Считаем только агентов с intent=2 (хотят быть в operations)
    if (intent == 2u) {
        if (group_by == 1u) {
            auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi8_ops_count");
            ops_count[idx].exchange(1u);
        } else if (group_by == 2u) {
            auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_ops_count");
            ops_count[idx].exchange(1u);
        }
    }
    
    return flamegpu::ALIVE;
}
"""
    
    rtc_func = agent.newRTCFunction("rtc_count_ops", RTC_COUNT_OPS)
    rtc_func.setInitialState("operations")
    rtc_func.setEndState("operations")
    
    layer_count = model.newLayer("count_ops")
    layer_count.addAgentFunction(rtc_func)
    
    # =========================================================================
    # Слой 3: Подсчёт агентов в serviceable
    # =========================================================================
    RTC_COUNT_SVC = """
FLAMEGPU_AGENT_FUNCTION(rtc_count_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (group_by == 1u) {
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi8_svc_count");
        svc_count[idx].exchange(1u);
    } else if (group_by == 2u) {
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_svc_count");
        svc_count[idx].exchange(1u);
    }
    
    return flamegpu::ALIVE;
}
"""
    
    rtc_func_svc = agent.newRTCFunction("rtc_count_serviceable", RTC_COUNT_SVC)
    rtc_func_svc.setInitialState("serviceable")
    rtc_func_svc.setEndState("serviceable")
    
    layer_count_svc = model.newLayer("count_serviceable")
    layer_count_svc.addAgentFunction(rtc_func_svc)
    
    # =========================================================================
    # Слой 4: Подсчёт агентов в reserve
    # =========================================================================
    RTC_COUNT_RESERVE = """
FLAMEGPU_AGENT_FUNCTION(rtc_count_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Записываем флаг что этот агент в reserve
    if (group_by == 1u) {
        auto reserve_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi8_reserve_count");
        reserve_count[idx].exchange(1u);
    } else if (group_by == 2u) {
        auto reserve_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_reserve_count");
        reserve_count[idx].exchange(1u);
    }
    
    return flamegpu::ALIVE;
}
"""
    
    rtc_func_res = agent.newRTCFunction("rtc_count_reserve", RTC_COUNT_RESERVE)
    rtc_func_res.setInitialState("reserve")
    rtc_func_res.setEndState("reserve")
    
    layer_count_res = model.newLayer("count_reserve")
    layer_count_res.addAgentFunction(rtc_func_res)
    
    # =========================================================================
    # Слой 5: Подсчёт агентов в inactive
    # =========================================================================
    RTC_COUNT_INACTIVE = """
FLAMEGPU_AGENT_FUNCTION(rtc_count_inactive, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Записываем флаг что этот агент в inactive
    if (group_by == 1u) {
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi8_inactive_count");
        inactive_count[idx].exchange(1u);
    } else if (group_by == 2u) {
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, 286u>("mi17_inactive_count");
        inactive_count[idx].exchange(1u);
    }
    
    return flamegpu::ALIVE;
}
"""
    
    rtc_func_ina = agent.newRTCFunction("rtc_count_inactive", RTC_COUNT_INACTIVE)
    rtc_func_ina.setInitialState("inactive")
    rtc_func_ina.setEndState("inactive")
    
    layer_count_ina = model.newLayer("count_inactive")
    layer_count_ina.addAgentFunction(rtc_func_ina)
    
    # =========================================================================
    # Слой 6: Логирование квот в MP2 (для агентов в operations)
    # =========================================================================
    RTC_LOG_QUOTAS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_log_quotas_to_mp2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Логируем только агентов в operations (где есть интерес квот)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;  // Пропускаем пусто
    }}
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));
    
    const unsigned int pos = day * {max_frames}u + idx;
    
    auto mp2_quota_curr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_curr_ops");
    auto mp2_quota_target = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_target_ops");
    auto mp2_quota_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_svc_count");
    auto mp2_quota_deficit = FLAMEGPU->environment.getMacroProperty<int, {MP2_SIZE}u>("mp2_quota_deficit");
    
    unsigned int curr = 0u, svc = 0u;
    unsigned int target = 0u;
    
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_svc_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ops_count[i] == 1u) ++curr;
            if (svc_count[i] == 1u) ++svc;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_svc_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ops_count[i] == 1u) ++curr;
            if (svc_count[i] == 1u) ++svc;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    }}
    
    const int deficit = (int)target - (int)curr;
    
    mp2_quota_curr[pos].exchange(curr);
    mp2_quota_target[pos].exchange(target);
    mp2_quota_svc[pos].exchange(svc);
    mp2_quota_deficit[pos].exchange(deficit);
    
    return flamegpu::ALIVE;
}}
"""
    
    rtc_log_quotas = agent.newRTCFunction("rtc_log_quotas_to_mp2", RTC_LOG_QUOTAS)
    rtc_log_quotas.setInitialState("operations")
    rtc_log_quotas.setEndState("operations")
    
    layer_log_quotas = model.newLayer("log_quotas_to_mp2")
    layer_log_quotas.addAgentFunction(rtc_log_quotas)
    
    print("  RTC модуль count_ops зарегистрирован (обнуление + подсчёт operations/serviceable/reserve/inactive + логирование квот)")


