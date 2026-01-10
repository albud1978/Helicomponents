#!/usr/bin/env python3
"""
RTC модули для Adaptive Step v3

Архитектура:
1. Вычисление горизонтов: бинарный поиск по mp5_cumsum
2. Global min reduction → adaptive_days
3. Батчевые инкременты по cumsum (точные!)
4. Условные переходы (только при horizon == adaptive_days)

Дата: 10.01.2026
"""

import pyflamegpu as fg

# Константы
MAX_FRAMES = 400
MAX_DAYS = 4000
MAX_SIZE = MAX_FRAMES * (MAX_DAYS + 1)


def get_rtc_compute_horizon_ops() -> str:
    """
    Вычисляет горизонт для агентов в operations.
    Использует бинарный поиск по mp5_cumsum для точного определения дня исчерпания ресурса.
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_horizon_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    
    // Читаем ресурсы
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    
    // Остатки ресурсов
    const unsigned int remaining_sne = (ll > sne) ? (ll - sne) : 0u;
    const unsigned int remaining_ppr = (oh > ppr) ? (oh - ppr) : 0u;
    
    // Читаем cumsum для этого агента
    auto mp_cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_cumsum");
    
    const unsigned int MAX_DAYS_PLUS_1 = {MAX_DAYS + 1}u;
    const unsigned int base_idx = idx * MAX_DAYS_PLUS_1;
    
    // Текущее значение cumsum
    const unsigned int safe_day = (current_day < {MAX_DAYS}u) ? current_day : ({MAX_DAYS}u - 1u);
    const unsigned int base_cumsum = mp_cumsum[base_idx + safe_day];
    
    // ═══════════════════════════════════════════════════════════════════════
    // Бинарный поиск для SNE (назначенный ресурс)
    // ═══════════════════════════════════════════════════════════════════════
    unsigned int horizon_sne = 3650u;
    if (remaining_sne > 0u) {{
        const unsigned int target_sne = base_cumsum + remaining_sne;
        
        unsigned int lo = current_day;
        unsigned int hi = end_day;
        if (hi > {MAX_DAYS}u) hi = {MAX_DAYS}u;
        
        while (lo < hi) {{
            unsigned int mid = (lo + hi) / 2u;
            unsigned int mid_cumsum = mp_cumsum[base_idx + mid];
            
            if (mid_cumsum < target_sne) {{
                lo = mid + 1u;
            }} else {{
                hi = mid;
            }}
        }}
        
        // Округляем ВНИЗ (lo - 1 если не точное совпадение, но минимум current_day)
        if (lo > current_day) {{
            horizon_sne = lo - current_day;
            // Проверяем, не превысили ли мы на этом дне
            unsigned int check_cumsum = mp_cumsum[base_idx + lo];
            if (check_cumsum > target_sne && lo > current_day + 1u) {{
                horizon_sne = lo - current_day - 1u;
            }}
        }} else {{
            horizon_sne = 1u;
        }}
    }}
    
    // ═══════════════════════════════════════════════════════════════════════
    // Бинарный поиск для PPR (межремонтный ресурс)
    // PPR накапливается так же как SNE (ppr += dt)
    // ═══════════════════════════════════════════════════════════════════════
    unsigned int horizon_ppr = 3650u;
    if (remaining_ppr > 0u) {{
        // PPR = SNE, поэтому target_ppr = base_cumsum + remaining_ppr
        const unsigned int target_ppr = base_cumsum + remaining_ppr;
        
        unsigned int lo = current_day;
        unsigned int hi = end_day;
        if (hi > {MAX_DAYS}u) hi = {MAX_DAYS}u;
        
        while (lo < hi) {{
            unsigned int mid = (lo + hi) / 2u;
            unsigned int mid_cumsum = mp_cumsum[base_idx + mid];
            
            if (mid_cumsum < target_ppr) {{
                lo = mid + 1u;
            }} else {{
                hi = mid;
            }}
        }}
        
        if (lo > current_day) {{
            horizon_ppr = lo - current_day;
        }} else {{
            horizon_ppr = 1u;
        }}
    }}
    
    // ═══════════════════════════════════════════════════════════════════════
    // Итоговый горизонт = min(horizon_sne, horizon_ppr)
    // ═══════════════════════════════════════════════════════════════════════
    unsigned int horizon = (horizon_sne < horizon_ppr) ? horizon_sne : horizon_ppr;
    
    // Защита от 0
    if (horizon == 0u) horizon = 1u;
    
    // Сохраняем горизонт
    FLAMEGPU->setVariable<unsigned int>("horizon", horizon);
    
    // Копируем в MacroProperty для global min
    if (idx < {MAX_FRAMES}u) {{
        auto mp_horizons = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_horizons");
        mp_horizons[idx].exchange(horizon);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_compute_horizon_repair() -> str:
    """
    Вычисляет горизонт для агентов в repair.
    Простой расчёт: repair_time - repair_days
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_horizon_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    
    // Горизонт = дней до завершения ремонта
    unsigned int horizon = 1u;
    if (repair_time > repair_days) {{
        horizon = repair_time - repair_days;
    }}
    
    // Сохраняем
    FLAMEGPU->setVariable<unsigned int>("horizon", horizon);
    
    // Копируем в MacroProperty
    if (idx < {MAX_FRAMES}u) {{
        auto mp_horizons = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_horizons");
        mp_horizons[idx].exchange(horizon);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_clear_horizon(state_name: str) -> str:
    """
    Сбрасывает горизонт для агентов в состояниях, не участвующих в adaptive step.
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_clear_horizon_{state_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Устанавливаем MAX — не участвует в min
    const unsigned int MAX_HORIZON = 0xFFFFFFFFu;
    FLAMEGPU->setVariable<unsigned int>("horizon", MAX_HORIZON);
    
    if (idx < {MAX_FRAMES}u) {{
        auto mp_horizons = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_horizons");
        mp_horizons[idx].exchange(MAX_HORIZON);
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_compute_adaptive_days() -> str:
    """
    QuotaManager вычисляет global min по всем горизонтам.
    Результат = adaptive_days для текущего шага.
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_adaptive_days, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Только первый QuotaManager (Mi-8) вычисляет
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
    // Global min по всем горизонтам
    // ═══════════════════════════════════════════════════════════════════════
    auto mp_horizons = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mp_horizons");
    
    unsigned int min_horizon = 0xFFFFFFFFu;
    for (unsigned int i = 0u; i < frames_total && i < {MAX_FRAMES}u; ++i) {{
        const unsigned int h = mp_horizons[i];
        if (h > 0u && h < min_horizon) {{
            min_horizon = h;
        }}
    }}
    
    // ═══════════════════════════════════════════════════════════════════════
    // adaptive_days = min_horizon (или 1 если нет событий)
    // ═══════════════════════════════════════════════════════════════════════
    unsigned int adaptive_days = 1u;
    if (min_horizon != 0xFFFFFFFFu && min_horizon > 0u) {{
        adaptive_days = min_horizon;
    }}
    
    // Ограничение: не выходим за end_day
    const unsigned int remaining = end_day - current_day;
    if (adaptive_days > remaining) {{
        adaptive_days = remaining;
    }}
    
    // Записываем результат
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("mp_adaptive_result");
    mp_result[0].exchange(adaptive_days);
    
    // Логирование
    printf("[Day %u] adaptive_days=%u (min_horizon=%u, remaining=%u)\\n", 
           current_day, adaptive_days, min_horizon, remaining);
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_batch_increment_ops() -> str:
    """
    Батчевый инкремент для operations.
    Использует cumsum для точного расчёта delta.
    ВАЖНО: Устанавливает intent_state при достижении лимита!
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_batch_increment_ops_v3, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    
    // Читаем adaptive_days из MacroProperty
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("mp_adaptive_result");
    const unsigned int adaptive_days = mp_result[0];
    
    if (adaptive_days == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // ═══════════════════════════════════════════════════════════════════════
    // Читаем delta из cumsum: cumsum[day + adaptive] - cumsum[day]
    // ═══════════════════════════════════════════════════════════════════════
    auto mp_cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_cumsum");
    
    const unsigned int MAX_DAYS_PLUS_1 = {MAX_DAYS + 1}u;
    const unsigned int base_idx = idx * MAX_DAYS_PLUS_1;
    
    const unsigned int safe_day_start = (current_day < {MAX_DAYS}u) ? current_day : ({MAX_DAYS}u - 1u);
    const unsigned int end_day_raw = current_day + adaptive_days;
    const unsigned int safe_day_end = (end_day_raw < {MAX_DAYS}u) ? end_day_raw : ({MAX_DAYS}u - 1u);
    
    const unsigned int cumsum_start = mp_cumsum[base_idx + safe_day_start];
    const unsigned int cumsum_end = mp_cumsum[base_idx + safe_day_end];
    
    // Delta SNE
    const unsigned int delta_sne = (cumsum_end > cumsum_start) ? (cumsum_end - cumsum_start) : 0u;
    
    // Delta PPR = Delta SNE (как в baseline)
    const unsigned int delta_ppr = delta_sne;
    
    // ═══════════════════════════════════════════════════════════════════════
    // Применяем инкременты
    // ═══════════════════════════════════════════════════════════════════════
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    sne += delta_sne;
    ppr += delta_ppr;
    
    FLAMEGPU->setVariable<unsigned int>("sne", sne);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
    
    // ═══════════════════════════════════════════════════════════════════════
    // Проверка достижения лимита → установка intent_state
    // ═══════════════════════════════════════════════════════════════════════
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    
    // sne >= ll → storage (назначенный ресурс исчерпан)
    if (ll > 0u && sne >= ll) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 6u);  // → storage
        return flamegpu::ALIVE;
    }}
    
    // ppr >= oh → repair (межремонтный ресурс исчерпан)
    if (oh > 0u && ppr >= oh) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);  // → repair
        return flamegpu::ALIVE;
    }}
    
    // sne >= br → repair (межремонтный контроль по SNE)
    if (br > 0u && sne >= br) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);  // → repair
        return flamegpu::ALIVE;
    }}
    
    return flamegpu::ALIVE;
}}
"""


def get_rtc_batch_increment_repair() -> str:
    """
    Батчевый инкремент для repair: repair_days += adaptive_days
    """
    return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_batch_increment_repair_v3, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Читаем adaptive_days
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("mp_adaptive_result");
    const unsigned int adaptive_days = mp_result[0];
    
    if (adaptive_days == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Инкремент repair_days
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    repair_days += adaptive_days;
    FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
    
    // Проверка завершения ремонта
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    if (repair_days >= repair_time) {{
        FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);  // → reserve
    }}
    
    return flamegpu::ALIVE;
}}
"""


def register_adaptive_v3(model: fg.ModelDescription, 
                         heli_agent: fg.AgentDescription,
                         quota_agent: fg.AgentDescription):
    """
    Регистрирует RTC модули для Adaptive Step v3.
    """
    print("  🚀 Регистрация модуля: Adaptive Step v3")
    
    # ═══════════════════════════════════════════════════════════════════════
    # 1. Вычисление горизонтов
    # ═══════════════════════════════════════════════════════════════════════
    
    # Operations: бинарный поиск по cumsum
    fn_horizon_ops = heli_agent.newRTCFunction("rtc_compute_horizon_ops", get_rtc_compute_horizon_ops())
    fn_horizon_ops.setInitialState("operations")
    fn_horizon_ops.setEndState("operations")
    
    # Repair: repair_time - repair_days
    fn_horizon_repair = heli_agent.newRTCFunction("rtc_compute_horizon_repair", get_rtc_compute_horizon_repair())
    fn_horizon_repair.setInitialState("repair")
    fn_horizon_repair.setEndState("repair")
    
    layer_horizon = model.newLayer("layer_compute_horizons")
    layer_horizon.addAgentFunction(fn_horizon_ops)
    layer_horizon.addAgentFunction(fn_horizon_repair)
    
    # Остальные состояния: MAX (не участвуют)
    layer_clear = model.newLayer("layer_clear_horizons")
    for state in ['inactive', 'serviceable', 'reserve', 'storage']:
        fn_name = f"rtc_clear_horizon_{state}"
        fn = heli_agent.newRTCFunction(fn_name, get_rtc_clear_horizon(state))
        fn.setInitialState(state)
        fn.setEndState(state)
        layer_clear.addAgentFunction(fn)
    
    # ═══════════════════════════════════════════════════════════════════════
    # 2. Global min → adaptive_days
    # ═══════════════════════════════════════════════════════════════════════
    fn_adaptive = quota_agent.newRTCFunction("rtc_compute_adaptive_days", get_rtc_compute_adaptive_days())
    
    layer_adaptive = model.newLayer("layer_compute_adaptive")
    layer_adaptive.addAgentFunction(fn_adaptive)
    
    # ═══════════════════════════════════════════════════════════════════════
    # 3. Батчевые инкременты
    # ═══════════════════════════════════════════════════════════════════════
    fn_inc_ops = heli_agent.newRTCFunction("rtc_batch_increment_ops_v3", get_rtc_batch_increment_ops())
    fn_inc_ops.setInitialState("operations")
    fn_inc_ops.setEndState("operations")
    
    fn_inc_repair = heli_agent.newRTCFunction("rtc_batch_increment_repair_v3", get_rtc_batch_increment_repair())
    fn_inc_repair.setInitialState("repair")
    fn_inc_repair.setEndState("repair")
    
    layer_increment = model.newLayer("layer_batch_increment")
    layer_increment.addAgentFunction(fn_inc_ops)
    layer_increment.addAgentFunction(fn_inc_repair)
    
    print("    ✅ Зарегистрировано: horizons, adaptive_days, batch_increments")


def setup_adaptive_v3_macroproperties(env: fg.EnvironmentDescription):
    """
    Создаёт MacroProperty для Adaptive Step v3.
    """
    # Горизонты всех агентов
    env.newMacroPropertyUInt32("mp_horizons", MAX_FRAMES)
    
    # Результат adaptive_days
    env.newMacroPropertyUInt32("mp_adaptive_result", 4)
    
    # Кумулятивные суммы MP5 для бинарного поиска
    env.newMacroPropertyUInt32("mp5_cumsum", MAX_SIZE)
    
    print(f"  ✅ MacroProperty для Adaptive v3: mp_horizons[{MAX_FRAMES}], mp_adaptive_result[4], mp5_cumsum[{MAX_SIZE}]")

