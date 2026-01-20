"""
RTC модуль state manager для переходов из состояния operations
Обрабатывает шесть типов переходов:
- 2→2 (operations → operations) при intent=2
- 2→3 (operations → serviceable) при intent=3 (квотный демоут)
- 2→4 (operations → repair) при intent=4
- 2→7 (operations → unserviceable) при intent=7 (очередь на ремонт)
- 2→6 (operations → storage) при intent=6

А также переходы В operations с записью dt:
- 3→2 (serviceable → operations)
- 5→2 (reserve → operations)
- 1→2 (inactive → operations)

ВАЖНО: При переходе В operations dt читается из MP5 и записывается в daily_today_u32
для полной атрибуции налёта в аналитике.
"""

import pyflamegpu as fg
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import model_build

# Константы для RTC
RTC_MAX_FRAMES = model_build.RTC_MAX_FRAMES
MAX_DAYS = model_build.MAX_DAYS
RTC_MAX_SIZE = RTC_MAX_FRAMES * (MAX_DAYS + 1)

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

RTC_COND_INTENT_7 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_7) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 7u;
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
    // Агент остается в operations (без логирования — не является переходом)
    
    // ✅ КРИТИЧНО: Сброс active_trigger после первого дня в operations
    // Это должно происходить ЗДЕСЬ, а не в state_2_operations, т.к. агент может
    // сразу перейти в repair (2→4) и пропустить сброс
    unsigned int active_trigger = FLAMEGPU->getVariable<unsigned int>("active_trigger");
    if (active_trigger == 1u) {
        FLAMEGPU->setVariable<unsigned int>("active_trigger", 0u);
    }
    
    // ✅ Сброс s4_days (агент в операциях, счётчик repair+reserve обнуляется)
    FLAMEGPU->setVariable<unsigned int>("s4_days", 0u);
    
    return flamegpu::ALIVE;
}
"""

# Функция для перехода 2→4 (operations → repair)
RTC_APPLY_2_TO_4 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_2_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    
    // ✅ КРИТИЧНО: Сброс active_trigger при переходе из operations
    unsigned int active_trigger = FLAMEGPU->getVariable<unsigned int>("active_trigger");
    if (active_trigger == 1u) {
        FLAMEGPU->setVariable<unsigned int>("active_trigger", 0u);
    }
    
    // ✅ Начало отсчёта s4_days (repair+reserve)
    FLAMEGPU->setVariable<unsigned int>("s4_days", 1u);
    
    // Логирование перехода (2→4) с типом вертолёта
    const char* type = (group_by == 1u) ? "Mi-8" : (group_by == 2u) ? "Mi-17" : "Unknown";
    printf("  [TRANSITION 2→4 Day %u] AC %u (idx %u, %s): operations -> repair, sne=%u, ppr=%u, oh=%u, br=%u\\n", 
           step_day, aircraft_number, idx, type, sne, ppr, oh, br);
    
    return flamegpu::ALIVE;
}
"""

# Функция для перехода 2→7 (operations → unserviceable)
RTC_APPLY_2_TO_7 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_2_to_7, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Сброс active_trigger при переходе из operations
    unsigned int active_trigger = FLAMEGPU->getVariable<unsigned int>("active_trigger");
    if (active_trigger == 1u) {
        FLAMEGPU->setVariable<unsigned int>("active_trigger", 0u);
    }
    
    // Очередь на ремонт
    FLAMEGPU->setVariable<unsigned int>("intent_state", 0u);
    
    // Начало отсчёта s4_days (repair+queue)
    FLAMEGPU->setVariable<unsigned int>("s4_days", 1u);
    
    printf("  [TRANSITION 2→7 Day %u] AC %u (idx %u): operations -> unserviceable (REPAIR QUEUE)\\n", 
           step_day, aircraft_number, idx);
    
    return flamegpu::ALIVE;
}
"""

# Функция для перехода 2→6 (operations → storage)
RTC_APPLY_2_TO_6 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_2_to_6, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    // ✅ КРИТИЧНО: Сброс active_trigger при переходе из operations
    unsigned int active_trigger = FLAMEGPU->getVariable<unsigned int>("active_trigger");
    if (active_trigger == 1u) {
        FLAMEGPU->setVariable<unsigned int>("active_trigger", 0u);
    }
    
    // Логирование перехода с указанием причины
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // Логирование перехода (2→6) с типом вертолёта
    const char* type = (group_by == 1u) ? "Mi-8" : (group_by == 2u) ? "Mi-17" : "Unknown";
    printf("  [TRANSITION 2→6 Day %u] AC %u (idx %u, %s): operations -> storage, sne=%u, ppr=%u, ll=%u, oh=%u, br=%u\\n", 
           step_day, aircraft_number, idx, type, sne, ppr, ll, oh, br);
    
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
    print("  Регистрация state manager для operations (2→2, 2→3, 2→4, 2→5, 2→6)")
    
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
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    // ✅ КРИТИЧНО: Сброс active_trigger при переходе из operations
    unsigned int active_trigger = FLAMEGPU->getVariable<unsigned int>("active_trigger");
    if (active_trigger == 1u) {
        FLAMEGPU->setVariable<unsigned int>("active_trigger", 0u);
    }
    
    // Логирование перехода (2→3) с типом вертолёта
    const char* type = (group_by == 1u) ? "Mi-8" : (group_by == 2u) ? "Mi-17" : "Unknown";
    printf("  [TRANSITION 2→3 Day %u] AC %u (idx %u, %s): operations -> serviceable (DEMOUNT), sne=%u, ppr=%u\\n", 
           step_day, aircraft_number, idx, type, sne, ppr);
    
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
    # ВАЖНО: Читаем dt из MP5 для полной атрибуции налёта!
    RTC_APPLY_3_TO_2 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_apply_3_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // ✅ КРИТИЧНО: Читаем dt из MP5 при переходе В operations
    // Это обеспечивает полную атрибуцию налёта даже в день входа
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_SIZE}u>("mp5_lin");
    const unsigned int base = step_day * frames + idx;
    const unsigned int base_next = base + frames;
    const unsigned int dt = mp5[base];
    const unsigned int dn = (step_day < {MAX_DAYS}u - 1u) ? mp5[base_next] : 0u;
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // ✅ Начисляем налёт (агент вошёл в operations, он летает сегодня!)
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    FLAMEGPU->setVariable<unsigned int>("sne", sne + dt);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr + dt);
    
    // Логирование переходов promocode→operations
    if (aircraft_number >= 100000u || step_day == 226u || step_day == 227u || step_day == 228u) {{
        printf("  [TRANSITION 3→2 Day %u] AC %u (idx %u): serviceable -> operations (PROMOTE), dt=%u\\n", 
               step_day, aircraft_number, idx, dt);
    }}
    
    return flamegpu::ALIVE;
}}
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
    # ВАЖНО: Читаем dt из MP5 для полной атрибуции налёта!
    RTC_APPLY_5_TO_2 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_apply_5_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // ✅ КРИТИЧНО: Читаем dt из MP5 при переходе В operations
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_SIZE}u>("mp5_lin");
    const unsigned int base = step_day * frames + idx;
    const unsigned int base_next = base + frames;
    const unsigned int dt = mp5[base];
    const unsigned int dn = (step_day < {MAX_DAYS}u - 1u) ? mp5[base_next] : 0u;
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // ✅ Начисляем налёт
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    FLAMEGPU->setVariable<unsigned int>("sne", sne + dt);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr + dt);
    
    if (aircraft_number >= 100000u || step_day == 226u || step_day == 227u || step_day == 228u) {{
        printf("  [TRANSITION 5→2 Day %u] AC %u (idx %u): reserve -> operations (PROMOTE P2), dt=%u\\n", 
               step_day, aircraft_number, idx, dt);
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    layer_5_to_2 = model.newLayer("transition_5_to_2")
    rtc_func_5_to_2 = agent.newRTCFunction("rtc_apply_5_to_2", RTC_APPLY_5_TO_2)
    rtc_func_5_to_2.setRTCFunctionCondition(RTC_COND_RESERVE_INTENT_2)  # Тот же фильтр (intent=2)
    rtc_func_5_to_2.setInitialState("reserve")
    rtc_func_5_to_2.setEndState("operations")
    layer_5_to_2.addAgentFunction(rtc_func_5_to_2)

    # ✅ Слой 1e: Переход 1→2 (inactive&intent=2 → operations) для промутов P3
    # ВАЖНО: Читаем dt из MP5 для полной атрибуции налёта!
    RTC_APPLY_1_TO_2 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_apply_1_to_2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    // ✅ КРИТИЧНО: Устанавливаем active_trigger=1 при переходе inactive → operations
    FLAMEGPU->setVariable<unsigned int>("active_trigger", 1u);
    
    // br2_mi17 — порог межремонтного для подъёма из inactive
    // Если ppr >= br2_mi17 → обнуляем ppr (ремонт)
    // Если ppr < br2_mi17 → сохраняем ppr (комплектация без ремонта)
    const unsigned int br2_mi17 = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br2_const");
    
    // Логика обнуления PPR:
    // Mi-8: всегда обнуляем (реальный ремонт планера)
    // Mi-17: обнуляем только если ppr >= br2_mi17 (иначе комплектация без ремонта)
    if (group_by == 1u) {{
        // Mi-8: реальный ремонт → PPR = 0
        ppr = 0u;
        FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    }} else if (group_by == 2u && ppr >= br2_mi17) {{
        // Mi-17 с ppr >= порога (3500ч): обнуляем PPR (ремонт)
        ppr = 0u;
        FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    }}
    // Mi-17 с ppr < порога: PPR сохраняется (комплектация без ремонта)
    
    // ✅ КРИТИЧНО: Читаем dt из MP5 при переходе В operations
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_SIZE}u>("mp5_lin");
    const unsigned int base = step_day * frames + idx;
    const unsigned int base_next = base + frames;
    const unsigned int dt = mp5[base];
    const unsigned int dn = (step_day < {MAX_DAYS}u - 1u) ? mp5[base_next] : 0u;
    
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // ✅ Начисляем налёт (к ppr после возможного обнуления!)
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    FLAMEGPU->setVariable<unsigned int>("sne", sne + dt);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr + dt);
    
    if (aircraft_number >= 100000u || step_day == 226u || step_day == 227u || step_day == 228u) {{
        const unsigned int ppr_final = FLAMEGPU->getVariable<unsigned int>("ppr");
        const char* action = (ppr == 0u) ? "ppr=0 (ремонт)" : "ppr сохранён (комплектация)";
        printf("  [TRANSITION 1→2 Day %u] AC %u (group_by=%u): %s, dt=%u, ppr_final=%u\\n", 
               step_day, aircraft_number, group_by, action, dt, ppr_final);
    }}
    
    return flamegpu::ALIVE;
}}
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
    
    # Слой 2b: Переход 2→7 (operations → unserviceable) для очереди на ремонт
    layer_2_to_7 = model.newLayer("transition_2_to_7")
    rtc_func_2_to_7 = agent.newRTCFunction("rtc_apply_2_to_7", RTC_APPLY_2_TO_7)
    rtc_func_2_to_7.setRTCFunctionCondition(RTC_COND_INTENT_7)
    rtc_func_2_to_7.setInitialState("operations")
    rtc_func_2_to_7.setEndState("unserviceable")
    layer_2_to_7.addAgentFunction(rtc_func_2_to_7)
    
    # Слой 3: Переход 2→6 (operations → storage)
    layer_2_to_6 = model.newLayer("transition_2_to_6")
    rtc_func_2_to_6 = agent.newRTCFunction("rtc_apply_2_to_6", RTC_APPLY_2_TO_6)
    rtc_func_2_to_6.setRTCFunctionCondition(RTC_COND_INTENT_6)
    rtc_func_2_to_6.setInitialState("operations")
    rtc_func_2_to_6.setEndState("storage")
    layer_2_to_6.addAgentFunction(rtc_func_2_to_6)
    
    print("  RTC модуль state_manager_operations зарегистрирован")
