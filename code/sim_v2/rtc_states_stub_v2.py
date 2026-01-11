#!/usr/bin/env python3
"""
RTC модуль V2 для состояний БЕЗ repair в обороте

ИЗМЕНЕНИЯ V2:
- ❌ Убраны инкременты repair_days
- ❌ Убран переход repair→reserve
- ✅ State 4 теперь "unserviceable" — просто ожидание promote P2
- ✅ State 5 (reserve) только для spawn

Ремонт будет добавлен постпроцессингом на GPU.
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

# RTC функция для state_4 (unserviceable) — V2: БЕЗ repair логики!
RTC_STATE_4_UNSERVICEABLE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_4_unserviceable_v2, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V2: Unserviceable — агенты ожидают promote P2
    // ❌ НЕТ инкрементов repair_days
    // ❌ НЕТ перехода в reserve при завершении ремонта
    // ✅ Ремонт будет добавлен постпроцессингом на GPU
    
    // По умолчанию остаёмся в состоянии 4 (unserviceable)
    // Квотирование (promote P2) установит intent=2 для готовых агентов
    FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
    
    // Обнуляем daily_today_u32 (нет налёта)
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
        ("rtc_state_4_unserviceable_v2", RTC_STATE_4_UNSERVICEABLE, "repair", "repair"),  # state ID=4, семантика=unserviceable
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
    
    print("  ✅ RTC модуль states_stub_v2 зарегистрирован (БЕЗ repair!)")

