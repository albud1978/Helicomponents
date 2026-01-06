#!/usr/bin/env python3
"""
RTC модуль инкремента наработки агрегатов

Читает dt от планера через MacroProperty mp_planer_dt
и инкрементирует sne/ppr агрегата

Логика:
1. Агрегат в operations читает aircraft_number
2. По aircraft_number находит planer_idx в mp_ac_to_idx
3. Читает dt из mp_planer_dt[day * MAX_PLANERS + planer_idx]
4. Инкрементирует sne += dt, ppr += dt

Дата: 05.01.2026
"""

import pyflamegpu as fg

MAX_PLANERS = 400
MAX_AC_NUMBER = 2000000


def get_rtc_code_increment(max_days: int) -> str:
    """
    CUDA код для инкремента sne/ppr по dt от планера
    """
    planer_dt_size = MAX_PLANERS * (max_days + 1)
    
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_increment_sne, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к operations
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    // Только для агрегатов на планере
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Получаем planer_idx по aircraft_number
    auto mp_ac_to_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_AC_NUMBER}u>("mp_ac_to_idx");
    
    unsigned int planer_idx = 0u;
    if (aircraft_number < {MAX_AC_NUMBER}u) {{
        planer_idx = mp_ac_to_idx[aircraft_number];
    }}
    
    // Если маппинг не найден (planer_idx == 0 и aircraft_number != первый планер)
    // используем fallback — средний dt
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    
    // Читаем dt из mp_planer_dt
    auto mp_planer_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {planer_dt_size}u>("mp_planer_dt");
    
    const unsigned int dt_pos = step_day * {MAX_PLANERS}u + planer_idx;
    unsigned int dt = 0u;
    
    if (dt_pos < {planer_dt_size}u) {{
        dt = mp_planer_dt[dt_pos];
    }}
    
    // Если dt == 0, агрегат не летает (планер не в operations)
    // НЕ используем fallback - sne не должен расти без полётов
    
    // Инкрементируем наработку
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    sne += dt;
    ppr += dt;
    
    FLAMEGPU->setVariable<unsigned int>("sne", sne);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
    // dt сохраняется в bi_counter временно (для возможного логирования)
    // daily_today_u32 не используется в агрегатах
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_check_limits() -> str:
    """
    CUDA код для проверки ppr >= oh и sne >= ll
    Устанавливает intent_state если ресурс исчерпан
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_check_limits, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к operations
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    
    // Условия перехода:
    // 1. ppr >= oh → ремонт (intent = 4)
    // 2. sne >= ll → списание (intent = 6)
    // 3. sne >= br → списание (дешевле купить новый)
    
    bool needs_repair = (oh > 0u && ppr >= oh);
    bool needs_storage = (ll > 0u && sne >= ll) || (br > 0u && sne >= br);
    
    if (needs_storage) {{
        // Списание в хранение
        FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
        // Debug: логируем все переходы в storage
        const unsigned int psn = FLAMEGPU->getVariable<unsigned int>("psn");
        const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
        const unsigned int step_day = FLAMEGPU->getStepCounter();
        if (psn < 100000u || group_by == 6u) {{
            printf("  [CHECK_LIMITS Day %u] PSN %u (group %u): sne=%u >= ll=%u -> STORAGE\\n", 
                   step_day, psn, group_by, sne, ll);
        }}
    }} else if (needs_repair) {{
        // В ремонт
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
    }} else {{
        // Остаёмся в operations
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription, 
                 max_days: int = 3650):
    """Регистрирует RTC функции инкремента и проверки лимитов"""
    
    # === Инкремент sne/ppr ===
    rtc_increment = get_rtc_code_increment(max_days)
    fn_increment = agent.newRTCFunction("rtc_units_increment_sne", rtc_increment)
    fn_increment.setInitialState("operations")
    fn_increment.setEndState("operations")
    
    layer_increment = model.newLayer("layer_units_increment")
    layer_increment.addAgentFunction(fn_increment)
    
    # === Проверка лимитов ===
    rtc_check = get_rtc_code_check_limits()
    fn_check = agent.newRTCFunction("rtc_units_check_limits", rtc_check)
    fn_check.setInitialState("operations")
    fn_check.setEndState("operations")
    
    layer_check = model.newLayer("layer_units_check_limits")
    layer_check.addAgentFunction(fn_check)
    
    print("  RTC модуль units_increment зарегистрирован (2 слоя: increment, check_limits)")

