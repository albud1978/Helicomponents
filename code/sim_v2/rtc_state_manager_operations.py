"""
RTC модуль state manager для переходов из состояния operations
Обрабатывает четыре типа переходов:
- 2→2 (operations → operations) при intent=2
- 2→3 (operations → serviceable) при intent=3 (квотный демоут)
- 2→4 (operations → repair) при intent=4
- 2→6 (operations → storage) при intent=6
"""

import pyflamegpu as fg

# Условия для фильтрации по intent
RTC_COND_INTENT_2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_2) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""

RTC_COND_INTENT_4 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_4) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""

RTC_COND_INTENT_6 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_6) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 6u;
}
"""

# Фильтр intent=3
RTC_COND_INTENT_3 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_3) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 3u;
}
"""

# Фильтр intent=2 из serviceable (проверяет ТОЛЬКО intent, state проверяется через setInitialState)
RTC_COND_SERVICEABLE_INTENT_2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_serviceable_intent_2) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""

RTC_COND_RESERVE_INTENT_2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_reserve_intent_2) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""

RTC_COND_INACTIVE_INTENT_2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_inactive_intent_2) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
}
"""

# Функция для перехода 2→2 (остаемся в operations)
RTC_APPLY_2_TO_2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_2_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Агент остается в operations
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Логирование перехода 2→2 (для новых агентов ACN >= 100000 или дни спавна)
    if (aircraft_number >= 100000u || step_day == 226u || step_day == 227u || step_day == 228u) {
        const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
        const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
        printf("  [TRANSITION 2→2 Day %u] AC %u (idx %u): staying in operations, sne=%u, ppr=%u\\n", 
               step_day, aircraft_number, idx, sne, ppr);
    }
    
    return flamegpu::ALIVE;
}
"""

# Функция для перехода 2→4 (operations → repair)
RTC_APPLY_2_TO_4 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_2_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    
    // Логирование перехода (2→4)
    printf("  [TRANSITION 2→4 Day %u] AC %u (idx %u): operations -> repair, sne=%u, ppr=%u, oh=%u, br=%u\\n", 
           step_day, aircraft_number, idx, sne, ppr, oh, br);
    
    return flamegpu::ALIVE;
}
"""

# Функция для перехода 2→6 (operations → storage)
RTC_APPLY_2_TO_6 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_2_to_6, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    // При переходе в хранение фиксируем день начала
    FLAMEGPU->setVariable<unsigned int>("s6_started", step_day);
    
    // Логирование перехода с указанием причины
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    printf("  [TRANSITION 2→6 Day %u] AC %u (idx %u): operations -> storage, sne=%u, ppr=%u, ll=%u, oh=%u, br=%u\\n", 
           step_day, aircraft_number, idx, sne, ppr, ll, oh, br);
    
    return flamegpu::ALIVE;
}
"""

def register_state_manager_operations(model: fg.ModelDescription, agent: fg.AgentDescription):
    """
    Регистрирует state manager для обработки переходов из operations
    
    Args:
        model: FLAME GPU ModelDescription
        agent: FLAME GPU AgentDescription
    """
    print("  Регистрация state manager для operations (2→2, 2→3, 2→4, 2→6)")
    
    # Слой 1: Переход 2→2 (остаемся в operations)
    layer_2_to_2 = model.newLayer("transition_2_to_2")
    rtc_func_2_to_2 = agent.newRTCFunction("rtc_apply_2_to_2", RTC_APPLY_2_TO_2)
    rtc_func_2_to_2.setRTCFunctionCondition(RTC_COND_INTENT_2)
    rtc_func_2_to_2.setInitialState("operations")
    rtc_func_2_to_2.setEndState("operations")
    layer_2_to_2.addAgentFunction(rtc_func_2_to_2)
    
    # Слой 1b: Переход 2→3 (operations → serviceable) для квотного демоута
    RTC_APPLY_2_TO_3 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_2_to_3, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    printf("  [TRANSITION 2→3 Day %u] AC %u (idx %u): operations -> serviceable (DEMOUNT), sne=%u, ppr=%u\\n", 
           step_day, aircraft_number, idx, sne, ppr);
    
    // ✅ Оставляем intent=3 (холдинг в serviceable, будет обработан quota_promote или state_manager_serviceable)
    // НЕ меняем intent! Это важно для холдинга!
    return flamegpu::ALIVE;
}
"""
    layer_2_to_3 = model.newLayer("transition_2_to_3")
    rtc_func_2_to_3 = agent.newRTCFunction("rtc_apply_2_to_3", RTC_APPLY_2_TO_3)
    rtc_func_2_to_3.setRTCFunctionCondition(RTC_COND_INTENT_3)
    rtc_func_2_to_3.setInitialState("operations")
    rtc_func_2_to_3.setEndState("serviceable")
    layer_2_to_3.addAgentFunction(rtc_func_2_to_3)

    # ✅ Слой 1c: Переход 3→2 (serviceable&intent=2 → operations) для промутов
    # Это обработка результата quota_promote_serviceable/reserve/inactive
    RTC_APPLY_3_TO_2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_3_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // ✅ SAFETY CHECK: Проверяем что мы действительно в serviceable (не нужно, setInitialState уже фильтрует)
    // но добавляем для отладки в случае если что-то пошло не так
    
    // Логирование переходов promocode→operations
    if (aircraft_number >= 100000u || step_day == 226u || step_day == 227u || step_day == 228u) {
        printf("  [TRANSITION 3→2 Day %u] AC %u (idx %u): serviceable -> operations (PROMOTE)\\n", 
               step_day, aircraft_number, idx);
    }
    
    return flamegpu::ALIVE;
}
"""
    
    # Фильтр: только intent=2 из serviceable
    # RTC_COND_SERVICEABLE_INTENT_2 = """
    # FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_serviceable_intent_2) {
    #     return FLAMEGPU->getVariable<unsigned int>("intent_state") == 2u;
    # }
    # """
    
    layer_3_to_2 = model.newLayer("transition_3_to_2")
    rtc_func_3_to_2 = agent.newRTCFunction("rtc_apply_3_to_2", RTC_APPLY_3_TO_2)
    rtc_func_3_to_2.setRTCFunctionCondition(RTC_COND_SERVICEABLE_INTENT_2)
    rtc_func_3_to_2.setInitialState("serviceable")
    rtc_func_3_to_2.setEndState("operations")
    layer_3_to_2.addAgentFunction(rtc_func_3_to_2)

    # ✅ Слой 1d: Переход 5→2 (reserve&intent=2 → operations) для промутов P2
    RTC_APPLY_5_TO_2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_5_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (aircraft_number >= 100000u || step_day == 226u || step_day == 227u || step_day == 228u) {
        printf("  [TRANSITION 5→2 Day %u] AC %u (idx %u): reserve -> operations (PROMOTE P2)\\n", 
               step_day, aircraft_number, idx);
    }
    
    return flamegpu::ALIVE;
}
"""
    
    layer_5_to_2 = model.newLayer("transition_5_to_2")
    rtc_func_5_to_2 = agent.newRTCFunction("rtc_apply_5_to_2", RTC_APPLY_5_TO_2)
    rtc_func_5_to_2.setRTCFunctionCondition(RTC_COND_RESERVE_INTENT_2)  # Тот же фильтр (intent=2)
    rtc_func_5_to_2.setInitialState("reserve")
    rtc_func_5_to_2.setEndState("operations")
    layer_5_to_2.addAgentFunction(rtc_func_5_to_2)

    # ✅ Слой 1e: Переход 1→2 (inactive&intent=2 → operations) для промутов P3
    RTC_APPLY_1_TO_2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_1_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (aircraft_number >= 100000u || step_day == 226u || step_day == 227u || step_day == 228u) {
        printf("  [TRANSITION 1→2 Day %u] AC %u (idx %u): inactive -> operations (PROMOTE P3)\\n", 
               step_day, aircraft_number, idx);
    }
    
    return flamegpu::ALIVE;
}
"""
    
    layer_1_to_2 = model.newLayer("transition_1_to_2")
    rtc_func_1_to_2 = agent.newRTCFunction("rtc_apply_1_to_2", RTC_APPLY_1_TO_2)
    rtc_func_1_to_2.setRTCFunctionCondition(RTC_COND_INACTIVE_INTENT_2)  # Тот же фильтр (intent=2)
    rtc_func_1_to_2.setInitialState("inactive")
    rtc_func_1_to_2.setEndState("operations")
    layer_1_to_2.addAgentFunction(rtc_func_1_to_2)

    # Слой 2: Переход 2→4 (operations → repair)
    layer_2_to_4 = model.newLayer("transition_2_to_4")
    rtc_func_2_to_4 = agent.newRTCFunction("rtc_apply_2_to_4", RTC_APPLY_2_TO_4)
    rtc_func_2_to_4.setRTCFunctionCondition(RTC_COND_INTENT_4)
    rtc_func_2_to_4.setInitialState("operations")
    rtc_func_2_to_4.setEndState("repair")
    layer_2_to_4.addAgentFunction(rtc_func_2_to_4)
    
    # Слой 3: Переход 2→6 (operations → storage)
    layer_2_to_6 = model.newLayer("transition_2_to_6")
    rtc_func_2_to_6 = agent.newRTCFunction("rtc_apply_2_to_6", RTC_APPLY_2_TO_6)
    rtc_func_2_to_6.setRTCFunctionCondition(RTC_COND_INTENT_6)
    rtc_func_2_to_6.setInitialState("operations")
    rtc_func_2_to_6.setEndState("storage")
    layer_2_to_6.addAgentFunction(rtc_func_2_to_6)
    
    print("  RTC модуль state_manager_operations зарегистрирован")
