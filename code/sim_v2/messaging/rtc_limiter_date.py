#!/usr/bin/env python3
"""
RTC модуль для адаптивного шага с limiter_date

Архитектура:
1. При входе агента в operations — вычисляется limiter_date
2. QuotaManager делает min() по всем limiter_date + program_change_dates
3. step_days = min_limiter_date - current_day

Преимущества:
- 300 простых сравнений вместо 400 итераций cumsum
- Расчёт лимита только при входе в ops, не каждый шаг

Дата: 08.01.2026
"""

import pyflamegpu as fg

# Константы
MAX_FRAMES = 400
MAX_PROGRAM_CHANGES = 150  # Максимум изменений программы за 10 лет
AVG_DT_PER_DAY = 180  # ~3 часа = 180 минут в день (среднее)


def get_rtc_compute_limiter() -> str:
    """
    RTC код для вычисления limiter_date при входе в operations.
    Вызывается в state_manager_operations при переходе в ops.
    """
    return f"""
// Вычисляет limiter_date для агента в operations
// Вызывается при входе в operations (transition 3->2, 5->2, 1->2)
FLAMEGPU_DEVICE_FUNCTION void compute_limiter_date(flamegpu::DeviceAPI<flamegpu::MessageNone, flamegpu::MessageNone>* FLAMEGPU) {{
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    
    // Читаем ресурсы агента
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    
    // Читаем avg_dt для этого агента из mp5 (упрощение: используем константу)
    const unsigned int avg_dt = {AVG_DT_PER_DAY}u;
    
    // Вычисляем дни до исчерпания ресурса
    unsigned int days_sne = 0xFFFFFFFFu;  // MAX
    unsigned int days_ppr = 0xFFFFFFFFu;
    
    if (ll > sne && avg_dt > 0u) {{
        days_sne = (ll - sne) / avg_dt;
    }}
    if (oh > ppr && avg_dt > 0u) {{
        // PPR обычно меньше (моторесурс), используем коэффициент
        days_ppr = (oh - ppr) / (avg_dt / 6u + 1u);  // ~30 мин/день
    }}
    
    // Минимум из двух
    unsigned int days_until_limit = (days_sne < days_ppr) ? days_sne : days_ppr;
    
    // Защита от слишком больших значений
    if (days_until_limit > 3650u) days_until_limit = 3650u;
    
    // limiter_date = current_day + days_until_limit
    const unsigned int limiter_date = current_day + days_until_limit;
    
    FLAMEGPU->setVariable<unsigned int>("limiter_date", limiter_date);
}}
"""


def get_rtc_set_limiter_ops() -> str:
    """
    RTC функция для агентов в operations — устанавливает limiter_date
    если он ещё не установлен (при первом входе).
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_set_limiter_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Проверяем, нужно ли вычислить limiter_date
    unsigned int limiter_date = FLAMEGPU->getVariable<unsigned int>("limiter_date");
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    
    // Если limiter_date < current_day, значит устарел и нужно пересчитать
    if (limiter_date < current_day || limiter_date == 0xFFFFFFFFu) {{
        // Читаем ресурсы агента
        const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
        const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
        const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
        const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
        
        const unsigned int avg_dt = {AVG_DT_PER_DAY}u;
        
        // Вычисляем дни до исчерпания ресурса
        unsigned int days_sne = 3650u;
        unsigned int days_ppr = 3650u;
        
        if (ll > sne && avg_dt > 0u) {{
            days_sne = (ll - sne) / avg_dt;
        }}
        if (oh > ppr) {{
            days_ppr = (oh - ppr) / 30u;  // ~30 мин/день для ppr
        }}
        
        unsigned int days_until_limit = (days_sne < days_ppr) ? days_sne : days_ppr;
        if (days_until_limit > 3650u) days_until_limit = 3650u;
        if (days_until_limit == 0u) days_until_limit = 1u;
        
        limiter_date = current_day + days_until_limit;
        FLAMEGPU->setVariable<unsigned int>("limiter_date", limiter_date);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_compute_step_days() -> str:
    """
    RTC функция для QuotaManager — вычисляет step_days через min reduction.
    Читает limiter_date всех агентов и program_change_dates.
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_step_days, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Только первый QuotaManager (Mi-8) вычисляет step_days
    const unsigned char group_by = FLAMEGPU->getVariable<unsigned char>("group_by");
    if (group_by != 1u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    const unsigned int frames_total = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    if (current_day >= end_day) {{
        return flamegpu::ALIVE;
    }}
    
    // ═══════════════════════════════════════════════════════════════════════
    // 1. Минимум по limiter_date агентов
    // ═══════════════════════════════════════════════════════════════════════
    auto mp_limiters = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_limiter_dates");
    
    unsigned int min_limiter = 0xFFFFFFFFu;
    for (unsigned int i = 0u; i < frames_total && i < {MAX_FRAMES}u; ++i) {{
        const unsigned int lim = mp_limiters[i];
        if (lim > current_day && lim < min_limiter) {{
            min_limiter = lim;
        }}
    }}
    
    // ═══════════════════════════════════════════════════════════════════════
    // 2. Следующее изменение программы
    // ═══════════════════════════════════════════════════════════════════════
    auto mp_program = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_PROGRAM_CHANGES}u>("mp_program_changes");
    const unsigned int num_changes = FLAMEGPU->environment.getProperty<unsigned int>("num_program_changes");
    
    unsigned int next_program = 0xFFFFFFFFu;
    for (unsigned int i = 0u; i < num_changes && i < {MAX_PROGRAM_CHANGES}u; ++i) {{
        const unsigned int pday = mp_program[i];
        if (pday > current_day) {{
            next_program = pday;
            break;  // Массив отсортирован, берём первый
        }}
    }}
    
    // ═══════════════════════════════════════════════════════════════════════
    // 3. step_days = min(limiters) - current_day
    // ═══════════════════════════════════════════════════════════════════════
    unsigned int next_event = min_limiter;
    if (next_program < next_event) next_event = next_program;
    
    unsigned int step_days = 1u;  // Default: 1 день если нет событий
    if (next_event != 0xFFFFFFFFu && next_event > current_day) {{
        step_days = next_event - current_day;
    }}
    
    // Ограничения
    // НЕТ ограничения на 30 дней — step_days определяется только событиями!
    if (step_days == 0u) step_days = 1u;
    
    const unsigned int remaining = end_day - current_day;
    if (step_days > remaining) step_days = remaining;
    
    // Записываем в MacroProperty для чтения другими (размер 4 для совместимости)
    auto mp_step = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("mp_step_days_result");
    mp_step[0].exchange(step_days);
    
    // Debug
    if (current_day % 365u == 0u || step_days != 30u) {{
        printf("[Day %u] step_days=%u (limiter=%u, program=%u)\\n", 
               current_day, step_days, min_limiter, next_program);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_copy_limiter_to_macro() -> str:
    """
    RTC функция для копирования limiter_date агентов в MacroProperty.
    Выполняется агентами в operations.
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_copy_limiter_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int limiter_date = FLAMEGPU->getVariable<unsigned int>("limiter_date");
    
    if (idx < {MAX_FRAMES}u) {{
        auto mp_limiters = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_limiter_dates");
        mp_limiters[idx].exchange(limiter_date);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_clear_limiter(state_name: str) -> str:
    """
    Генерирует RTC код для сброса лимитера агентов в конкретном состоянии.
    """
    return f"""
// Агенты в {state_name} сбрасывают свой лимитер в MAX
FLAMEGPU_AGENT_FUNCTION(rtc_clear_limiter_{state_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (idx < {MAX_FRAMES}u) {{
        auto mp_limiters = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_limiter_dates");
        mp_limiters[idx].exchange(0xFFFFFFFFu);  // MAX = не участвует в min
    }}
    
    // Сбрасываем limiter_date агента (пересчитается при входе в ops)
    FLAMEGPU->setVariable<unsigned int>("limiter_date", 0xFFFFFFFFu);
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_repair_limiter() -> str:
    """
    RTC функция для агентов в repair — устанавливает limiter_date = current_day + (repair_time - repair_days).
    Когда repair_days достигает repair_time, агент должен перейти в reserve.
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_set_limiter_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    
    // Читаем время ремонта
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    
    // Вычисляем limiter_date — когда ремонт завершится
    unsigned int days_remaining = 0u;
    if (repair_time > repair_days) {{
        days_remaining = repair_time - repair_days;
    }}
    
    const unsigned int limiter_date = current_day + days_remaining;
    
    // Записываем в переменную агента
    FLAMEGPU->setVariable<unsigned int>("limiter_date", limiter_date);
    
    // Копируем в MacroProperty для глобального min
    if (idx < {MAX_FRAMES}u) {{
        auto mp_limiters = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_limiter_dates");
        mp_limiters[idx].exchange(limiter_date);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def register_rtc(model: fg.ModelDescription, heli_agent: fg.AgentDescription, 
                 quota_agent: fg.AgentDescription):
    """
    Регистрирует RTC функции для адаптивного шага с limiter_date.
    ВАЖНО: Каждая функция компилируется отдельно!
    
    Лимитеры:
    - operations: limiter_date по ресурсам (LL-SNE, OH-PPR)
    - repair: limiter_date по repair_time - repair_days
    - остальные: MAX (не участвуют в min)
    """
    print("  🚀 Регистрация модуля: rtc_limiter_date (адаптивный шаг)")
    
    # Отдельные RTC коды
    rtc_set_code = get_rtc_set_limiter_ops()
    rtc_copy_code = get_rtc_copy_limiter_to_macro()
    rtc_quota_code = get_rtc_compute_step_days()
    rtc_repair_code = get_rtc_repair_limiter()
    
    # ═══════════════════════════════════════════════════════════════════════
    # 1. rtc_set_limiter_ops — агенты в operations устанавливают limiter_date
    # ═══════════════════════════════════════════════════════════════════════
    fn_set_limiter = heli_agent.newRTCFunction("rtc_set_limiter_ops", rtc_set_code)
    fn_set_limiter.setInitialState("operations")
    fn_set_limiter.setEndState("operations")
    
    layer_set_limiter = model.newLayer("layer_set_limiter")
    layer_set_limiter.addAgentFunction(fn_set_limiter)
    
    # ═══════════════════════════════════════════════════════════════════════
    # 2. rtc_set_limiter_repair — агенты в repair устанавливают limiter_date
    # ═══════════════════════════════════════════════════════════════════════
    fn_set_repair = heli_agent.newRTCFunction("rtc_set_limiter_repair", rtc_repair_code)
    fn_set_repair.setInitialState("repair")
    fn_set_repair.setEndState("repair")
    
    layer_set_limiter.addAgentFunction(fn_set_repair)
    
    # ═══════════════════════════════════════════════════════════════════════
    # 3. rtc_copy_limiter_ops — агенты в ops копируют limiter в MacroProperty
    # ═══════════════════════════════════════════════════════════════════════
    fn_copy_ops = heli_agent.newRTCFunction("rtc_copy_limiter_ops", rtc_copy_code)
    fn_copy_ops.setInitialState("operations")
    fn_copy_ops.setEndState("operations")
    
    layer_copy = model.newLayer("layer_copy_limiter")
    layer_copy.addAgentFunction(fn_copy_ops)
    
    # ═══════════════════════════════════════════════════════════════════════
    # 4. rtc_clear_limiter_* — агенты в inactive/serviceable/reserve/storage сбрасывают лимитер
    # (repair обрабатывается отдельно через rtc_set_limiter_repair)
    # ═══════════════════════════════════════════════════════════════════════
    states_other = ['inactive', 'serviceable', 'reserve', 'storage']  # repair исключен!
    layer_clear = model.newLayer("layer_clear_limiter")
    
    for state in states_other:
        fn_name = f"rtc_clear_limiter_{state}"
        rtc_clear_code = get_rtc_clear_limiter(state)
        fn = heli_agent.newRTCFunction(fn_name, rtc_clear_code)
        fn.setInitialState(state)
        fn.setEndState(state)
        layer_clear.addAgentFunction(fn)
    
    # ═══════════════════════════════════════════════════════════════════════
    # 5. rtc_compute_step_days — QuotaManager вычисляет step_days
    # ═══════════════════════════════════════════════════════════════════════
    fn_step = quota_agent.newRTCFunction("rtc_compute_step_days", rtc_quota_code)
    
    layer_step = model.newLayer("layer_compute_step_days")
    layer_step.addAgentFunction(fn_step)
    
    print("    ✅ Зарегистрировано: set_limiter_ops, set_limiter_repair, copy_limiter, clear_limiter, compute_step_days")


def setup_limiter_macroproperties(env: fg.EnvironmentDescription, 
                                   program_change_days: list):
    """
    Создаёт MacroProperty для limiter_date системы.
    
    Args:
        env: EnvironmentDescription
        program_change_days: список дней с изменениями программы (отсортирован)
    """
    # MacroProperty для limiter_date каждого агента
    env.newMacroPropertyUInt32("mp_limiter_dates", MAX_FRAMES)
    
    # MacroProperty для дат изменения программы
    env.newMacroPropertyUInt32("mp_program_changes", MAX_PROGRAM_CHANGES)
    
    # Результат вычисления step_days (размер 4 для совместимости с RTC)
    env.newMacroPropertyUInt32("mp_step_days_result", 4)
    
    # Количество изменений программы
    num_changes = min(len(program_change_days), MAX_PROGRAM_CHANGES)
    env.newPropertyUInt("num_program_changes", num_changes)
    
    print(f"  ✅ MacroProperty для limiter_date: mp_limiter_dates[{MAX_FRAMES}], "
          f"mp_program_changes[{MAX_PROGRAM_CHANGES}], num_changes={num_changes}")


def precompute_program_changes(client, version_date_str: str) -> list:
    """
    Предрасчёт дней изменения программы из flight_program_ac.
    
    Returns:
        Список дней (ordinal от version_date) с изменениями программы.
    """
    from datetime import date
    
    query = f"""
        SELECT toRelativeDayNum(dates) - toRelativeDayNum(toDate('{version_date_str}')) as day_offset
        FROM flight_program_ac 
        WHERE version_date = toDate('{version_date_str}')
          AND (trigger_program_mi8 != 0 OR trigger_program_mi17 != 0)
        ORDER BY dates
    """
    
    result = client.execute(query)
    days = [int(row[0]) for row in result if row[0] >= 0]
    
    print(f"  📊 Предрасчёт program_changes: {len(days)} событий")
    if days[:5]:
        print(f"     Первые 5: {days[:5]}")
    
    return days

