#!/usr/bin/env python3
"""
RTC модуль для state_2 (operations) - эксплуатация
Обрабатывает агентов в эксплуатации, обновляет наработки, проверяет переходы
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from model_build import MAX_FRAMES, MAX_DAYS, MAX_SIZE

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


# RTC функция для state_2
RTC_STATE_2_OPERATIONS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_2_operations, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Эта функция вызывается только для агентов в состоянии "operations"
    // благодаря setInitialState("operations")
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    
    // Получаем данные MP5
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (step_day < days_total ? step_day : (days_total > 0u ? days_total - 1u : 0u));
    
    const unsigned int base = safe_day * {MAX_FRAMES}u + idx;
    const unsigned int base_next = base + {MAX_FRAMES}u;
    
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
    const unsigned int dt = mp5[base];
    const unsigned int dn = (safe_day < days_total - 1u ? mp5[base_next] : 0u);
    
    // Сохраняем MP5 данные
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // Обновляем наработки
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    sne += dt;
    ppr += dt;
    
    FLAMEGPU->setVariable<unsigned int>("sne", sne);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
    
    // Отладка: для агента idx=0 установим специальное значение
    if (idx == 0u) {{
        FLAMEGPU->setVariable<unsigned int>("sne", 999999u);
    }}
    
    // Получаем пороги
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    
    // Проверяем детерминированные переходы
    
    // Переход 2->6 (списание по LL)
    if (sne + dn >= ll) {{
        // Устанавливаем намерение перейти в state_6
        FLAMEGPU->setVariable<unsigned int>("s6_started", 1u);
        FLAMEGPU->setVariable<unsigned int>("s6_days", 0u);
        FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);  // Намерение перейти в хранение
        return flamegpu::ALIVE;
    }}
    
    // Переход 2->4 или 2->6 (ремонт или списание по BR)
    if (ppr + dn >= oh) {{
        if (sne + dn >= br) {{
            // Намерение перейти в state_6 (списание по BR)
            FLAMEGPU->setVariable<unsigned int>("s6_started", 1u);
            FLAMEGPU->setVariable<unsigned int>("s6_days", 0u);
            FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
        }} else {{
            // Намерение перейти в state_4 (в ремонт)
            FLAMEGPU->setVariable<unsigned int>("repair_days", 1u);
            FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
        }}
        return flamegpu::ALIVE;
    }}
    
    // Если остался в state_2 - намерение продолжить эксплуатацию (требует квоту)
    // Временно ставим 1 для проверки работы RTC
    FLAMEGPU->setVariable<unsigned int>("intent_state", 1u);
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции и слои для state_2"""
    
    # Добавляем RTC функцию
    rtc_func = agent.newRTCFunction("rtc_state_2_operations", RTC_STATE_2_OPERATIONS)
    
    # Устанавливаем начальное состояние для функции
    rtc_func.setInitialState("operations")
    # Функция не меняет состояние сама (это делает менеджер состояний)
    rtc_func.setEndState("operations")
    
    try:
        # Создаем новый слой для этой функции
        state_layer = model.newLayer()
        
        # Добавляем функцию в слой
        state_layer.addAgentFunction(rtc_func)
        
        print("  RTC модуль state_2_operations зарегистрирован")
    except Exception as e:
        print(f"  Ошибка регистрации state_2_operations: {e}")
