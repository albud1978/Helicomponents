"""
RTC модуль для агентов в состоянии repair (агрегаты)
Аналог rtc_state_manager_repair планеров

Функционал:
- Инкремент repair_days
- Вычисление intent: 4 (остаёмся) или 5 (переход в reserve)
- Обнуление ppr при переходе (кроме лопастей)
- queue_position = rsv_tail++ при переходе в reserve

Дата: 05.01.2026, обновлено 08.01.2026
"""

import pyflamegpu as fg


MAX_GROUPS = 50


def get_rtc_code() -> str:
    """Возвращает CUDA код для модуля repair"""
    return """
// Вычисление intent в repair
FLAMEGPU_AGENT_FUNCTION(rtc_units_state_repair_intent, flamegpu::MessageNone, flamegpu::MessageNone) {
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    
    // Инкремент дней в ремонте
    repair_days += 1u;
    FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
    
    // Проверка завершения ремонта
    if (repair_days >= repair_time) {
        FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);  // reserve
        FLAMEGPU->setVariable<unsigned int>("transition_4_to_5", 1u);
    } else {
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);  // остаёмся
    }
    
    return flamegpu::ALIVE;
}

// Условие: intent = 4 (остаёмся в repair)
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_repair_stay) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}

// Условие: intent = 5 (переход в reserve)
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_repair_to_reserve) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 5u;
}

// Функция 4→4 (остаёмся в repair)
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_4_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Ничего не делаем, агент остаётся в repair
    return flamegpu::ALIVE;
}

// Функция 4→5 (repair → reserve) — INTENT ONLY
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_4_to_5, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Этот переход обрабатывается в отдельной RTC функции с rsv_tail++
    return flamegpu::ALIVE;
}
"""


RTC_COND_REPAIR_STAY = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_repair_stay) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""

RTC_COND_REPAIR_TO_RESERVE = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_units_repair_to_reserve) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 5u;
}
"""


def get_rtc_code_4_to_5() -> str:
    """RTC код для 4→5 (repair → reserve) с rsv_tail++"""
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_units_apply_4_to_5_with_queue, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int psn = FLAMEGPU->getVariable<unsigned int>("psn");
    
    // Обнуляем ppr для ВСЕХ агрегатов после ремонта/продления
    // Для лопастей это означает "продление ресурса" — после repair они получают ещё oh часов
    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    
    // Сброс счётчиков
    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_4_to_5", 0u);
    FLAMEGPU->setVariable<unsigned int>("active", 1u);  // Реальный агрегат
    
    // Получаем позицию в очереди reserve: queue_position = rsv_tail++
    if (group_by < {MAX_GROUPS}u) {{
        auto mp_rsv_tail = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_GROUPS}u>("mp_rsv_tail");
        unsigned int my_pos = mp_rsv_tail[group_by]++;  // atomicAdd, возвращает старое значение
        FLAMEGPU->setVariable<unsigned int>("queue_position", my_pos);
    }} else {{
        FLAMEGPU->setVariable<unsigned int>("queue_position", 0u);
    }}
    
    // Логирование
    if (psn < 100000u) {{
        printf("  [UNIT 4→5 Day %u] PSN %u (group %u): repair -> reserve, queue_pos=%u\\n", 
               step_day, psn, group_by, FLAMEGPU->getVariable<unsigned int>("queue_position"));
    }}
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для repair"""
    rtc_code = get_rtc_code()
    rtc_code_4_to_5 = get_rtc_code_4_to_5()
    
    # Функция вычисления intent
    fn_intent = agent.newRTCFunction("rtc_units_state_repair_intent", rtc_code)
    fn_intent.setInitialState("repair")
    fn_intent.setEndState("repair")
    
    # Функция 4→4 (с условием)
    fn_4_to_4 = agent.newRTCFunction("rtc_units_apply_4_to_4", rtc_code)
    fn_4_to_4.setRTCFunctionCondition(RTC_COND_REPAIR_STAY)
    fn_4_to_4.setInitialState("repair")
    fn_4_to_4.setEndState("repair")
    
    # Функция 4→5 (с условием и сменой состояния!) — с rsv_tail++
    fn_4_to_5 = agent.newRTCFunction("rtc_units_apply_4_to_5_with_queue", rtc_code_4_to_5)
    fn_4_to_5.setRTCFunctionCondition(RTC_COND_REPAIR_TO_RESERVE)
    fn_4_to_5.setInitialState("repair")
    fn_4_to_5.setEndState("reserve")  # СМЕНА СОСТОЯНИЯ!
    
    # Слои
    layer_intent = model.newLayer("layer_units_repair_intent")
    layer_intent.addAgentFunction(fn_intent)
    
    layer_4_to_4 = model.newLayer("layer_units_repair_4_to_4")
    layer_4_to_4.addAgentFunction(fn_4_to_4)
    
    layer_4_to_5 = model.newLayer("layer_units_repair_4_to_5")
    layer_4_to_5.addAgentFunction(fn_4_to_5)
    
    print("  RTC модуль units_state_repair зарегистрирован (3 слоя, 4→5 с rsv_tail++)")

