"""
RTC функция для state_2 (operations) с установкой intent_state
"""

# Константы из sim_env_setup
MAX_DAYS = 4000
MAX_FRAMES = 286  # Будет переопределено динамически
MAX_SIZE = MAX_FRAMES * (MAX_DAYS + 1)

# Проверка импорта
try:
    import pyflamegpu as fg
    print("  pyflamegpu доступен для rtc_state_2_operations")
except ImportError:
    print("  Внимание: pyflamegpu недоступен, используем заглушку")
    class fg:
        class ModelDescription: pass
        class AgentDescription: pass
else:
    import pyflamegpu as fg


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функцию для state_2 с установкой intent_state"""
    
    # Одна функция для всех агентов в operations
    rtc_func = agent.newRTCFunction("rtc_state_2_operations", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_2_operations, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    
    // Не выполнять на шаге 0
    if (step_day == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Достаём значения из агента
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    
    // Получаем суточный налёт из MP5
    const unsigned int offset = idx * {MAX_DAYS + 1} + step_day;
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}>("mp5_lin");
    const unsigned int dt = mp5[offset];
    const unsigned int dn = (step_day < {MAX_DAYS} - 1u) ? mp5[offset + 1] : 0u;
    
    // Обновляем MP5 в агенте
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // Начисляем налёт
    const unsigned int sne_new = sne + dt;
    const unsigned int ppr_new = ppr + dt;
    FLAMEGPU->setVariable<unsigned int>("sne", sne_new);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr_new);
    
    // Отладка для первых агентов
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 22171u || aircraft_number == 22172u) {{
        printf("  [DEBUG Step %u] AC %u: sne=%u, ppr=%u, oh=%u, br=%u, ll=%u, dt=%u, dn=%u\\n", 
               step_day, aircraft_number, sne, ppr, oh, br, ll, dt, dn);
    }}
    
    // Прогноз на завтра для условий переходов (как в sim_master)
    const unsigned int s_next = sne_new + dn;
    const unsigned int p_next = ppr_new + dn;
    
    // ПРИОРИТЕТ проверок:
    // 1. Сначала проверяем LL
    if (s_next >= ll) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
        const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [Step %u] AC %u: intent=6 (storage LL), sne_next=%u >= ll=%u\\n", 
               step_day, aircraft_number, s_next, ll);
        return flamegpu::ALIVE;
    }}
    
    // 2. Потом проверяем OH с учётом BR
    if (p_next >= oh) {{
        if (s_next < br) {{
            // Переход в ремонт
            FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
            const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
            printf("  [Step %u] AC %u: intent=4 (repair), ppr_next=%u >= oh=%u, sne_next=%u < br=%u\\n", 
                   step_day, aircraft_number, p_next, oh, s_next, br);
        }} else {{
            // Переход в хранение (BR достигнут)
            FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);
            const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
            printf("  [Step %u] AC %u: intent=6 (storage BR), ppr_next=%u >= oh=%u, sne_next=%u >= br=%u\\n", 
                   step_day, aircraft_number, p_next, oh, s_next, br);
        }}
        return flamegpu::ALIVE;
    }}
    
    // 3. Остаёмся в operations
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    
    // Побочные эффекты
    unsigned int active_trigger = FLAMEGPU->getVariable<unsigned int>("active_trigger");
    if (active_trigger > 0u) {{
        FLAMEGPU->setVariable<unsigned int>("active_trigger", 0u);
    }}
    
    return flamegpu::ALIVE;
}}
""")
    
    # Устанавливаем для агентов в operations
    rtc_func.setInitialState("operations")
    rtc_func.setEndState("operations")  # ВАЖНО: иначе агенты уйдут в inactive
    
    try:
        # Создаем слой
        layer = model.newLayer("state_2_operations")
        layer.addAgentFunction(rtc_func)
        
        print("  RTC модуль state_2_operations зарегистрирован")
    except Exception as e:
        print(f"  Ошибка регистрации state_2_operations: {e}")