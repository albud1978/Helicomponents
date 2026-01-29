"""
State Manager для переходов из состояния repair
Обрабатывает: 4->4 (остаться в ремонте), 4->3 (переход в serviceable)
"""

import pyflamegpu as fg
import model_build

RTC_MAX_FRAMES = model_build.RTC_MAX_FRAMES

# Условие для intent_state == 4 (остаться в repair)
RTC_COND_INTENT_4 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_4) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 4u;
}
"""

# Условие для intent_state == 3 (переход в serviceable)
RTC_COND_INTENT_3 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_intent_3) {
    return FLAMEGPU->getVariable<unsigned int>("intent_state") == 3u;
}
"""

# Функция для агентов, остающихся в repair (4->4)
RTC_APPLY_4_TO_4 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_4_to_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Агенты остаются в repair
    return flamegpu::ALIVE;
}
"""

# Функция для перехода repair -> serviceable (4->3)
RTC_APPLY_4_TO_3 = """
FLAMEGPU_AGENT_FUNCTION(rtc_apply_4_to_3, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int debug_enabled = FLAMEGPU->environment.getProperty<unsigned int>("debug_enabled");
    
    // Логирование перехода (4→3) с типом вертолёта
    const char* type = (group_by == 1u) ? "Mi-8" : (group_by == 2u) ? "Mi-17" : "Unknown";
    if (debug_enabled) {
        printf("  [TRANSITION 4→3 Day %u] AC %u (idx %u, %s): repair -> serviceable, repair_days=%u/%u\\n", 
               step_day, aircraft_number, idx, type, repair_days, repair_time);
    }
    
    // Сбрасываем счетчики при переходе в serviceable
    FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
    FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 0u);
    
    // Освобождаем ремонтную линию (если была закреплена)
    const unsigned int line_id = FLAMEGPU->getVariable<unsigned int>("repair_line_id");
    if (line_id != 0xFFFFFFFFu) {
        auto line_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, RTC_MAX_FRAMES_PLACEHOLDER>("repair_line_aircraft_number");
        line_acn[line_id].exchange(0u);
        FLAMEGPU->setVariable<unsigned int>("repair_line_id", 0xFFFFFFFFu);
    }
    
    // ✅ Инкремент s4_days (продолжаем счёт repair+reserve)
    unsigned int s4_days = FLAMEGPU->getVariable<unsigned int>("s4_days");
    s4_days++;
    FLAMEGPU->setVariable<unsigned int>("s4_days", s4_days);
    
    return flamegpu::ALIVE;
}
"""
RTC_APPLY_4_TO_3 = RTC_APPLY_4_TO_3.replace(
    "RTC_MAX_FRAMES_PLACEHOLDER",
    f"{RTC_MAX_FRAMES}u"
)

def register_state_manager_repair(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует state manager для переходов из repair"""
    print("  Регистрация state manager для repair (4→4, 4→3)")
    
    # Layer 1: Transition 4->4 (stay in repair)
    layer_4_to_4 = model.newLayer("transition_4_to_4")
    rtc_func_4_to_4 = agent.newRTCFunction("rtc_apply_4_to_4", RTC_APPLY_4_TO_4)
    rtc_func_4_to_4.setRTCFunctionCondition(RTC_COND_INTENT_4)
    rtc_func_4_to_4.setInitialState("repair")
    rtc_func_4_to_4.setEndState("repair")
    layer_4_to_4.addAgentFunction(rtc_func_4_to_4)
    
    # Layer 2: Transition 4->3 (repair -> serviceable)
    layer_4_to_3 = model.newLayer("transition_4_to_3")
    rtc_func_4_to_3 = agent.newRTCFunction("rtc_apply_4_to_3", RTC_APPLY_4_TO_3)
    rtc_func_4_to_3.setRTCFunctionCondition(RTC_COND_INTENT_3)
    rtc_func_4_to_3.setInitialState("repair")
    rtc_func_4_to_3.setEndState("serviceable")
    layer_4_to_3.addAgentFunction(rtc_func_4_to_3)
    
    print("  RTC модуль state_manager_repair зарегистрирован")
