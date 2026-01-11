#!/usr/bin/env python3
"""
RTC модуль V2/V3 для состояний

ИЗМЕНЕНИЯ V3 (11.01.2026):
- ✅ Восстановлен инкремент repair_days
- ✅ Восстановлен переход repair→reserve при repair_days >= repair_time
- ✅ Используется step_days из Environment для адаптивных шагов
- ✅ State 5 (reserve) только для spawn
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS, MAX_SIZE

MAX_FRAMES = RTC_MAX_FRAMES

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


# RTC функция для state_3 (serviceable) — без изменений
RTC_STATE_3_SERVICEABLE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_3_serviceable_v2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Все агенты в serviceable по умолчанию хотят вернуться в operations (intent=3)
    FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_4 (repair/unserviceable) — V3: С repair логикой!
RTC_STATE_4_REPAIR = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_4_repair_v2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V3: Repair/Unserviceable — агенты ожидают завершения ремонта
    // ✅ Инкремент repair_days
    // ✅ Переход НАПРЯМУЮ в operations (intent=2) при завершении ремонта
    // ✅ PPR обнуляется в rtc_apply_4_to_2
    
    // Читаем step_days из Environment для адаптивных шагов
    const unsigned int step_days = FLAMEGPU->environment.getProperty<unsigned int>("step_days");
    
    // Инкремент repair_days
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    repair_days += step_days;
    FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
    
    // Проверка завершения ремонта
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    if (repair_days >= repair_time) {{
        // V3: Ремонт завершён → переход НАПРЯМУЮ в operations (не через reserve!)
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    }} else {{
        // Продолжаем ремонт
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
    }}
    
    // Обнуляем daily_today_u32 (нет налёта в ремонте)
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_5 (reserve) — V2: ТОЛЬКО для spawn!
RTC_STATE_5_RESERVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_5_reserve_v2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V2: Reserve ТОЛЬКО для spawn агентов
    // ❌ НЕТ очереди на ремонт (intent=0)
    // ✅ Все агенты просто ожидают активации (intent=5)
    
    FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_6 (storage) — без изменений
RTC_STATE_6_STORAGE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_6_storage_v2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Агенты в хранении остаются там навсегда (S6 immutable)
    FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции V2 для всех состояний"""
    
    funcs = [
        ("rtc_state_3_serviceable_v2", RTC_STATE_3_SERVICEABLE, "serviceable", "serviceable"),
        ("rtc_state_4_repair_v2", RTC_STATE_4_REPAIR, "repair", "repair"),  # V3: с repair логикой
        ("rtc_state_5_reserve_v2", RTC_STATE_5_RESERVE, "reserve", "reserve"),
        ("rtc_state_6_storage_v2", RTC_STATE_6_STORAGE, "storage", "storage")
    ]
    
    for func_name, func_code, state_name, end_state in funcs:
        try:
            rtc_func = agent.newRTCFunction(func_name, func_code)
            rtc_func.setInitialState(state_name)
            rtc_func.setEndState(end_state)
            
            layer = model.newLayer()
            layer.addAgentFunction(rtc_func)
            
            print(f"  V2: RTC функция {func_name} зарегистрирована для {state_name}")
            
        except Exception as e:
            print(f"  Ошибка регистрации {func_name}: {e}")
    
    print("  ✅ RTC модуль states_stub_v2 зарегистрирован (V3: с repair логикой)")

