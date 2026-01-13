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


MAX_GROUPS = 50


def get_rtc_code_increment(max_days: int) -> str:
    """
    CUDA код для инкремента sne/ppr по dt от планера
    
    КРИТИЧЕСКАЯ ЛОГИКА:
    - Проверяем mp_planer_slots НАПРЯМУЮ: если < 2 агрегатов, планер НЕ ЛЕТИТ
    - Пилот не полетит без двигателя!
    - Записываем дефицит-дни для аналитики
    """
    planer_dt_size = MAX_PLANERS * (max_days + 1)
    slots_size = MAX_GROUPS * MAX_PLANERS
    
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_increment_sne, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к operations
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
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
    
    if (planer_idx == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    
    // === ПРОВЕРКА КОМПЛЕКТНОСТИ НАПРЯМУЮ ===
    // Пилот не полетит без двигателя! Проверяем mp_planer_slots
    auto mp_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, {slots_size}u>("mp_planer_slots");
    auto mp_deficit_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {slots_size}u>("mp_deficit_days");
    
    const unsigned int slots_pos = group_by * {MAX_PLANERS}u + planer_idx;
    unsigned int current_slots = 0u;
    
    // comp_numbers — это PropertyArray, используем getProperty
    // Для двигателей (group_by=3,4) всегда нужно 2
    const unsigned int required_slots = 2u;
    
    if (slots_pos < {slots_size}u) {{
        current_slots = mp_slots[slots_pos];
    }}
    
    // КРИТИЧНО: Если не хватает агрегатов - НЕ ЛЕТАЕМ!
    if (current_slots < required_slots) {{
        // Записываем дефицит для аналитики
        if (slots_pos < {slots_size}u) {{
            mp_deficit_days[slots_pos] += 1u;
        }}
        // НЕ инкрементируем SNE - планер не летает
        return flamegpu::ALIVE;
    }}
    
    // Читаем dt из mp_planer_dt
    auto mp_planer_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {planer_dt_size}u>("mp_planer_dt");
    
    const unsigned int dt_pos = step_day * {MAX_PLANERS}u + planer_idx;
    unsigned int dt = 0u;
    
    if (dt_pos < {planer_dt_size}u) {{
        dt = mp_planer_dt[dt_pos];
    }}
    
    // Если dt == 0, агрегат не летает (планер не в operations)
    if (dt == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Инкрементируем наработку
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    sne += dt;
    ppr += dt;
    
    FLAMEGPU->setVariable<unsigned int>("sne", sne);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_code_check_limits(max_days: int) -> str:
    """
    CUDA код для проверки ppr >= oh и sne >= ll
    Устанавливает intent_state если ресурс исчерпан
    
    ВАЖНО: НЕ отправляем агрегаты в serviceable при dt=0!
    Планеры с dt=0 — зимующие на удалённых точках, разбирать их НЕЛЬЗЯ.
    Агрегаты остаются на планере независимо от dt.
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_check_limits, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Функция привязана к operations
    // Проверяем только исчерпание ресурса (ppr >= oh, sne >= ll)
    // НЕ проверяем dt — агрегаты остаются на планере даже если он не летает
    
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    
    // Условия перехода:
    // 1. ppr >= oh → ремонт/продление (intent = 4)
    // 2. sne >= ll → списание (intent = 6)
    // 3. sne >= br → списание (дешевле купить новый)
    
    // ppr >= oh работает одинаково для всех агрегатов включая лопасти
    // После ремонта/продления ppr обнуляется (подтверждено данными: PPR=NULL после shop_visit)
    bool needs_repair = (oh > 0u && ppr >= oh);
    // FIX: storage только если LL строго больше OH (иначе repair имеет приоритет)
    // Это важно для лопастей где ll=120000=oh — они должны идти в repair, не в storage
    bool needs_storage = (ll > 0u && sne >= ll && ll > oh) || (br > 0u && sne >= br);
    
    // FIX: Приоритет: repair > storage (можно ещё раз продлить ресурс)
    if (needs_repair) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
    }} else if (needs_storage) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
    }} else {{
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
    rtc_check = get_rtc_code_check_limits(max_days)
    fn_check = agent.newRTCFunction("rtc_units_check_limits", rtc_check)
    fn_check.setInitialState("operations")
    fn_check.setEndState("operations")
    
    layer_check = model.newLayer("layer_units_check_limits")
    layer_check.addAgentFunction(fn_check)
    
    print("  RTC модуль units_increment зарегистрирован (2 слоя: increment, check_limits)")

