"""
Debug модуль для логирования квотирования на каждый день
Выводит информацию о дефиците, целях и выборе агентов
"""

def create_quota_debug_layer(model, agent, max_frames):
    """Создает слой для логирования квотирования"""
    
    RTC_QUOTA_DEBUG = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_debug_log, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // Только первый агент каждого типа логирует
    if (idx != 0u) return flamegpu::ALIVE;
    
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));
    
    unsigned int curr = 0u, target = 0u, svc = 0u;
    
    if (group_by == 1u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_svc_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ops_count[i] == 1u) ++curr;
            if (svc_count[i] == 1u) ++svc;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
        
        const int deficit = (int)target - (int)curr;
        if (day % 50u == 0u || day < 100u) {{
            printf("[Day %u] Mi-8: curr=%u, svc=%u, target=%u, deficit=%d\\n", day, curr, svc, target, deficit);
        }}
    }} else if (group_by == 2u) {{
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_ops_count");
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_svc_count");
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ops_count[i] == 1u) ++curr;
            if (svc_count[i] == 1u) ++svc;
        }}
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
        
        const int deficit = (int)target - (int)curr;
        if (day % 50u == 0u || day < 100u) {{
            printf("[Day %u] Mi-17: curr=%u, svc=%u, target=%u, deficit=%d\\n", day, curr, svc, target, deficit);
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    layer = model.newLayer("quota_debug_log")
    rtc_func = agent.newRTCFunction("rtc_quota_debug_log", RTC_QUOTA_DEBUG)
    rtc_func.setInitialState("operations")
    rtc_func.setEndState("operations")
    layer.addAgentFunction(rtc_func)
    
    return layer

