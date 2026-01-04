#!/usr/bin/env python3
"""
RTC модуль-заглушка для остальных состояний (временный для тестирования)
Обрабатывает state_1, state_3, state_4, state_5, state_6
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS, MAX_SIZE

# Алиас для совместимости
MAX_FRAMES = RTC_MAX_FRAMES

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


# RTC функция для state_1 (inactive)
RTC_STATE_1_INACTIVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_1_inactive, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // ✅ Все неактивные агенты замороженные (intent=1)
    // Логика проверки step_day >= repair_time находится в quota_promote_inactive
    FLAMEGPU->setVariable<unsigned int>("intent_state", 1u);
    
    // Обнуляем daily_today_u32 (нет налёта в inactive)
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_3 (serviceable)
RTC_STATE_3_SERVICEABLE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_3_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // ✅ Все агенты в serviceable по умолчанию хотят вернуться в operations (intent=3)
    // Только избранные (выбранные quota_promote_serviceable) получат intent=2 и перейдут
    FLAMEGPU->setVariable<unsigned int>("intent_state", 3u);
    
    // Обнуляем daily_today_u32 (нет налёта в serviceable)
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_4 (repair)
RTC_STATE_4_REPAIR = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_4_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Агенты в ремонте
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    
    // Обнуляем daily_today_u32 (нет налёта в repair)
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    
    // На шаге 0 не выполняем переходы, но intent должен быть задан явно
    if (step_day == 0u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
        return flamegpu::ALIVE;
    }}
    
    // Увеличиваем счетчик дней ремонта
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    repair_days++;
    FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
    
    // Увеличиваем счётчик дней в repair+reserve (s4_days)
    unsigned int s4_days = FLAMEGPU->getVariable<unsigned int>("s4_days");
    s4_days++;
    FLAMEGPU->setVariable<unsigned int>("s4_days", s4_days);
    
    // Обработка assembly_trigger: срабатывает ОДИН РАЗ, затем автоматически сбрасывается
    unsigned int assembly_trigger = FLAMEGPU->getVariable<unsigned int>("assembly_trigger");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int assembly_time = FLAMEGPU->getVariable<unsigned int>("assembly_time");
    
    // Если был установлен в прошлом шаге, сбрасываем
    if (assembly_trigger == 1u) {{
        FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 0u);
    }}
    
    // Проверяем порог (после сброса)
    if (repair_days == repair_time - assembly_time) {{
        FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 1u);
    }}
    
    // Проверяем завершение ремонта
    if (repair_days == repair_time) {{
        // Переход в резерв
        FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
        
        // Логирование перехода
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
        const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
        const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
        printf("  [Step %u] AC %u: intent=5 (reserve), repair complete rd=%u/%u, ll=%u, oh=%u, br=%u\\n", 
               step_day, aircraft_number, repair_days, repair_time, ll, oh, br);

        // Сбрасываем ppr и repair_days согласно требованию
        FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
        FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
    }} else {{
        // Остаемся в ремонте
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
    }}
    
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_5 (reserve)
RTC_STATE_5_RESERVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_5_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // ✅ КРИТИЧНО: НЕ трогаем intent=0 (очередь на ремонт)!
    // Только агенты с другими intent переходят в intent=5 (общий резерв)
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    if (intent != 0u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
    }}
    // intent=0 остаётся без изменений (агент в очереди на ремонт)
    
    // Обнуляем daily_today_u32 (нет налёта в reserve)
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    
    // Увеличиваем счётчик дней в repair+reserve (s4_days)
    unsigned int s4_days = FLAMEGPU->getVariable<unsigned int>("s4_days");
    s4_days++;
    FLAMEGPU->setVariable<unsigned int>("s4_days", s4_days);
    
    return flamegpu::ALIVE;
}}
"""

# RTC функция для state_6 (storage)
RTC_STATE_6_STORAGE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_6_storage, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Агенты в хранении остаются там навсегда (S6 immutable)
    FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
    
    // Обнуляем daily_today_u32 (нет налёта в storage)
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", 0u);
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для всех состояний с setEndState"""
    
    # Все функции теперь с setEndState для корректной работы
    funcs = [
        ("rtc_state_1_inactive", RTC_STATE_1_INACTIVE, "inactive", "inactive"),  # Остаётся
        ("rtc_state_3_serviceable", RTC_STATE_3_SERVICEABLE, "serviceable", "serviceable"),  # Остаётся  
        ("rtc_state_4_repair", RTC_STATE_4_REPAIR, "repair", None),  # Условные переходы внутри
        ("rtc_state_5_reserve", RTC_STATE_5_RESERVE, "reserve", "reserve"),  # Остаётся
        ("rtc_state_6_storage", RTC_STATE_6_STORAGE, "storage", "storage")  # Остаётся
    ]
    
    for func_name, func_code, state_name, end_state in funcs:
        try:
            # Создаем RTC функцию
            rtc_func = agent.newRTCFunction(func_name, func_code)
            
            # Устанавливаем состояние для функции
            rtc_func.setInitialState(state_name)
            # ВАЖНО: всегда устанавливаем setEndState, иначе агенты уйдут в inactive
            if not end_state:
                end_state = state_name  # Остаются в том же состоянии
            rtc_func.setEndState(end_state)
            
            # Создаем слой для функции
            layer = model.newLayer()
            
            # Добавляем функцию в слой
            layer.addAgentFunction(rtc_func)
            
            print(f"  RTC функция {func_name} зарегистрирована для состояния {state_name}")
            
        except Exception as e:
            print(f"  Ошибка регистрации {func_name}: {e}")
    
    print("  RTC модуль states_stub зарегистрирован")
