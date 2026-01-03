"""
RTC функция для state_2 (operations) с установкой intent_state
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
import model_build

# Константы из model_build (фиксированные для RTC кэширования)
MAX_DAYS = model_build.MAX_DAYS
RTC_MAX_FRAMES = model_build.RTC_MAX_FRAMES  # Фиксированный размер для компиляции RTC

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
    
    # ФИКСИРОВАННЫЕ размеры для RTC (для кэширования ядер)
    rtc_max_frames = RTC_MAX_FRAMES
    rtc_max_size = rtc_max_frames * (MAX_DAYS + 1)
    
    # Одна функция для всех агентов в operations
    # Используем ФИКСИРОВАННЫЕ размеры для RTC кэширования
    rtc_func = agent.newRTCFunction("rtc_state_2_operations", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_state_2_operations, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    // Runtime frames_total из Environment (для индексации)
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // DEBUG для агента 100006 (idx=285) в день 824
    if (step_day == 824u && idx == 285u) {{
        const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
        printf("[DEBUG Day %u STATE_2] Agent idx=%u (ACN=%u): intent=%u (BEFORE setting)\\n", 
               step_day, idx, acn, intent);
    }}
    
    // Получаем суточный налёт из MP5 (всегда, даже на шаге 0)
    // ВАЖНО: base использует runtime frames, но mp5 объявлен с фиксированным RTC размером
    const unsigned int base = step_day * frames + idx;
    const unsigned int base_next = base + frames;
    auto mp5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {rtc_max_size}u>("mp5_lin");
    const unsigned int dt = mp5[base];
    const unsigned int dn = (step_day < {MAX_DAYS}u - 1u) ? mp5[base_next] : 0u;
    
    // Обновляем MP5 в агенте
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    // На шаге 0 не выполняем переходы, но intent должен быть задан явно
    if (step_day == 0u) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
        return flamegpu::ALIVE;
    }}
    
    // Достаём значения из агента
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    
    // Начисляем налёт
    const unsigned int sne_new = sne + dt;
    const unsigned int ppr_new = ppr + dt;
    FLAMEGPU->setVariable<unsigned int>("sne", sne_new);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr_new);
    
    // Отладка для первых агентов
    // DEBUG печать отключена
    
    // Прогноз на завтра для условий переходов: учитываем сегодняшний dt
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
    // intent=2 означает "хочу в operations" (для демоута/промоута)
    // Агент работает И хочет продолжать работать → участвует в квотировании
    FLAMEGPU->setVariable<unsigned int>("intent_state", 2u);
    
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