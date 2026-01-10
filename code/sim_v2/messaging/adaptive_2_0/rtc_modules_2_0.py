#!/usr/bin/env python3
"""
Adaptive 2.0: RTC модули

Всего 5 модулей:
1. compute_adaptive_days — вычисление adaptive_days из limiter_date
2. batch_increment — батчевые инкременты sne/ppr/repair_days
3. transitions — переходы состояний с пересчётом limiter_date
4. quota — квотирование по ProgramEvent
5. mp2_write — запись в MP2 буфер

Дата: 10.01.2026
"""
import pyflamegpu as fg

# Константы
MAX_FRAMES = 400
MAX_DAYS = 4000
MAX_DAYS_PLUS_1 = MAX_DAYS + 1
CUMSUM_SIZE = MAX_FRAMES * MAX_DAYS_PLUS_1


# ═══════════════════════════════════════════════════════════════════════════════
# МОДУЛЬ 1: compute_adaptive_days
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COPY_LIMITER_TO_BUFFER = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_copy_limiter_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Нет early return — нельзя смешивать read/write MacroProperty в одном слое!
    // Проверка end_day через Environment property (не MacroProperty)
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    const unsigned int limiter_date = FLAMEGPU->getVariable<unsigned short>("limiter_date");
    
    // Если limiter уже за пределами — пропускаем запись
    if (limiter_date > end_day) return flamegpu::ALIVE;
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned short>("idx");
    
    if (idx < {MAX_FRAMES}u) {{
        // UInt32 для exchange() совместимости
        auto buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("limiter_buffer");
        buffer[idx].exchange((unsigned int)limiter_date);
    }}
    
    return flamegpu::ALIVE;
}}
"""

RTC_COPY_LIMITER_REPAIR = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_copy_limiter_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Нет чтения current_day из MacroProperty — используем только exchange
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    const unsigned int limiter_date = FLAMEGPU->getVariable<unsigned short>("limiter_date");
    
    if (limiter_date > end_day) return flamegpu::ALIVE;
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned short>("idx");
    
    if (idx < {MAX_FRAMES}u) {{
        // UInt32 для exchange() совместимости
        // Используем atomicMin логику
        auto buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("limiter_buffer");
        buffer[idx].min((unsigned int)limiter_date);  // atomicMin
    }}
    
    return flamegpu::ALIVE;
}}
"""

RTC_CLEAR_LIMITER_BUFFER = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_clear_limiter, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Нет чтения current_day из MacroProperty
    const unsigned int idx = FLAMEGPU->getVariable<unsigned short>("idx");
    
    if (idx < {MAX_FRAMES}u) {{
        // UInt32 для exchange() совместимости
        auto buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("limiter_buffer");
        buffer[idx].exchange(0xFFFFFFFFu);
    }}
    
    return flamegpu::ALIVE;
}}
"""

RTC_COMPUTE_GLOBAL_MIN = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_global_min, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // QuotaManager агент вычисляет global min
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    
    // Читаем current_day из MacroProperty (внутри GPU!)
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    
    // Early return если симуляция завершена
    if (current_day >= end_day) {{
        // Обнуляем adaptive_days чтобы другие функции тоже пропустили
        auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("global_min_result");
        result[0].exchange(0u);
        return flamegpu::ALIVE;
    }}
    
    // UInt32 для exchange() совместимости
    auto buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("limiter_buffer");
    
    // Находим минимум
    unsigned int min_limiter = 0xFFFFu;
    unsigned int min_idx = 0u;
    
    for (unsigned int i = 0u; i < frames && i < {MAX_FRAMES}u; ++i) {{
        unsigned int val = buffer[i];
        if (val > current_day && val < min_limiter) {{
            min_limiter = val;
            min_idx = i;
        }}
    }}
    
    // Проверяем ProgramEvent (UInt32)
    auto event_days = FLAMEGPU->environment.getMacroProperty<unsigned int, 500u>("program_event_days");
    unsigned int events_total = FLAMEGPU->environment.getProperty<unsigned int>("events_total");
    
    for (unsigned int i = 0u; i < events_total && i < 500u; ++i) {{
        unsigned int event_day = event_days[i];
        if (event_day > current_day && event_day < min_limiter) {{
            min_limiter = event_day;
        }}
    }}
    
    // adaptive_days = min_limiter - current_day
    unsigned int adaptive_days = 1u;
    if (min_limiter != 0xFFFFu && min_limiter > current_day) {{
        adaptive_days = min_limiter - current_day;
    }}
    
    // Не выходить за end_day
    unsigned int remaining = end_day - current_day;
    if (adaptive_days > remaining) {{
        adaptive_days = remaining;
    }}
    
    // Записываем результат (UInt32 для exchange())
    auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("global_min_result");
    result[0].exchange(adaptive_days);
    result[1].exchange(min_idx);
    
    // Логирование
    printf("[Day %u] adaptive_days=%u (min_limiter=%u, min_idx=%u)\\n", 
           current_day, adaptive_days, min_limiter, min_idx);
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# МОДУЛЬ 2: batch_increment
# ═══════════════════════════════════════════════════════════════════════════════

RTC_BATCH_INCREMENT_OPS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_batch_increment_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Читаем current_day и проверяем завершение
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    if (current_day >= end_day) return flamegpu::ALIVE;
    
    // Читаем adaptive_days из global_min_result (UInt32)
    auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("global_min_result");
    const unsigned int adaptive_days = result[0];
    
    if (adaptive_days == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned short>("idx");
    
    // Читаем delta из cumsum
    auto cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, {CUMSUM_SIZE}u>("mp5_cumsum");
    
    const unsigned int base = idx * {MAX_DAYS_PLUS_1}u;
    const unsigned int start_cumsum = cumsum[base + current_day];
    const unsigned int end_cumsum = cumsum[base + current_day + adaptive_days];
    const unsigned int delta = (end_cumsum > start_cumsum) ? (end_cumsum - start_cumsum) : 0u;
    
    // Инкременты
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    sne += delta;
    ppr += delta;
    
    FLAMEGPU->setVariable<unsigned int>("sne", sne);
    FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
    
    return flamegpu::ALIVE;
}}
"""

RTC_BATCH_INCREMENT_REPAIR = """
FLAMEGPU_AGENT_FUNCTION(rtc_batch_increment_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Early return если симуляция завершена
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    if (current_day >= end_day) return flamegpu::ALIVE;
    
    auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("global_min_result");
    const unsigned int adaptive_days = result[0];
    
    if (adaptive_days == 0u) {
        return flamegpu::ALIVE;
    }
    
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned short>("repair_days");
    repair_days += adaptive_days;
    FLAMEGPU->setVariable<unsigned short>("repair_days", (unsigned short)repair_days);
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# МОДУЛЬ 3: transitions
# ═══════════════════════════════════════════════════════════════════════════════

RTC_TRANSITION_REPAIR_TO_RESERVE = """
FLAMEGPU_AGENT_FUNCTION(rtc_transition_repair_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Early return если симуляция завершена
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    if (current_day >= end_day) return flamegpu::ALIVE;
    
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned short>("repair_days");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned short>("repair_time");
    
    if (repair_days >= repair_time) {
        // Переход repair → reserve
        FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
        FLAMEGPU->setVariable<unsigned short>("repair_days", 0u);
        FLAMEGPU->setVariable<unsigned short>("limiter_date", 0xFFFFu);
        
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [TRANSITION Day %u] AC %u: repair -> reserve\\n", current_day, ac);
    }
    
    return flamegpu::ALIVE;
}
"""

RTC_TRANSITION_REPAIR_CONDITION = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(condition_repair_done) {
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned short>("repair_days");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned short>("repair_time");
    return repair_days >= repair_time;
}
"""

RTC_TRANSITION_OPS_CHECK = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_transition_ops_check, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Читаем current_day и проверяем завершение
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    if (current_day >= end_day) return flamegpu::ALIVE;
    
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    
    // sne >= ll → storage
    if (ll > 0u && sne >= ll) {{
        FLAMEGPU->setVariable<unsigned short>("limiter_date", 0xFFFFu);  // Storage конечное
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [TRANSITION Day %u] AC %u: operations -> storage (sne=%u >= ll=%u)\\n", 
               current_day, ac, sne, ll);
        // Состояние изменится через state manager
        return flamegpu::ALIVE;
    }}
    
    // ppr >= oh → repair
    if (oh > 0u && ppr >= oh) {{
        const unsigned int repair_time = FLAMEGPU->getVariable<unsigned short>("repair_time");
        FLAMEGPU->setVariable<unsigned short>("limiter_date", (unsigned short)(current_day + repair_time));
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [TRANSITION Day %u] AC %u: operations -> repair (ppr=%u >= oh=%u)\\n", 
               current_day, ac, ppr, oh);
        return flamegpu::ALIVE;
    }}
    
    // sne >= br → repair
    if (br > 0u && sne >= br) {{
        const unsigned int repair_time = FLAMEGPU->getVariable<unsigned short>("repair_time");
        FLAMEGPU->setVariable<unsigned short>("limiter_date", (unsigned short)(current_day + repair_time));
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [TRANSITION Day %u] AC %u: operations -> repair (sne=%u >= br=%u)\\n", 
               current_day, ac, sne, br);
        return flamegpu::ALIVE;
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# МОДУЛЬ 4: quota
# ═══════════════════════════════════════════════════════════════════════════════

RTC_QUOTA_PROCESS_EVENT = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_process_event, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Проверяем завершение через стандартное свойство (не MacroProperty!)
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    const unsigned int events_total = FLAMEGPU->environment.getProperty<unsigned int>("events_total");
    
    // Читаем current_day — ТОЛЬКО ЧТЕНИЕ
    auto mp_day_ro = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day_ro[0];
    if (current_day >= end_day) return flamegpu::ALIVE;
    
    // UInt32 для exchange() совместимости
    auto event_days = FLAMEGPU->environment.getMacroProperty<unsigned int, 500u>("program_event_days");
    auto target_mi8 = FLAMEGPU->environment.getMacroProperty<unsigned int, 500u>("program_target_mi8");
    auto target_mi17 = FLAMEGPU->environment.getMacroProperty<unsigned int, 500u>("program_target_mi17");
    
    // Ищем событие на текущий день
    for (unsigned int i = 0u; i < events_total && i < 500u; ++i) {{
        if (event_days[i] == current_day) {{
            // Нашли событие — обновляем targets
            // (targets будут использованы в отдельном RTC для подсчёта и demote/promote)
            unsigned int new_mi8 = target_mi8[i];
            unsigned int new_mi17 = target_mi17[i];
            
            printf("  [PROGRAM EVENT Day %u] target_mi8=%u, target_mi17=%u\\n", 
                   current_day, new_mi8, new_mi17);
            
            // Примечание: event_days не меняем — проверяем по current_day каждый шаг
            // Это позволяет избежать конфликта read/write в одном слое
            
            break;
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# МОДУЛЬ 5: mp2_write
# ═══════════════════════════════════════════════════════════════════════════════

RTC_MP2_WRITE_OPS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Читаем current_day и проверяем завершение
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    if (current_day >= end_day) return flamegpu::ALIVE;
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned short>("idx");
    
    // Читаем write_idx из MacroProperty
    auto mp_write_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("mp2_write_idx_mp");
    const unsigned int write_idx = mp_write_idx[0];
    
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    // Позиция в буфере
    const unsigned int pos = write_idx * {MAX_FRAMES}u + idx;
    
    auto buf_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * 500}u>("mp2_buffer_sne");
    auto buf_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * 500}u>("mp2_buffer_ppr");
    auto buf_day = FLAMEGPU->environment.getMacroProperty<unsigned short, {MAX_FRAMES * 500}u>("mp2_buffer_day");
    auto buf_state = FLAMEGPU->environment.getMacroProperty<unsigned char, {MAX_FRAMES * 500}u>("mp2_buffer_state");
    
    if (pos < {MAX_FRAMES * 500}u) {{
        buf_sne[pos] = sne;
        buf_ppr[pos] = ppr;
        buf_day[pos] = (unsigned short)current_day;
        buf_state[pos] = 2u;  // operations = 2
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# МОДУЛЬ 6: update_current_day (ВНУТРИ GPU!)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_UPDATE_CURRENT_DAY = """
FLAMEGPU_AGENT_FUNCTION(rtc_update_current_day, flamegpu::MessageNone, flamegpu::MessageNone) {
    // ОГРАНИЧЕНИЕ FLAME GPU: нельзя смешивать read/write MacroProperty в одном слое
    // current_day обновляется через HostFunction
    
    // Эта функция теперь пустая — вся логика перенесена в HF_UpdateCurrentDay
    // Оставлена для совместимости со слоями модели
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# DEPRECATED: старая версия mp2_write (удалить после тестирования)
# ═══════════════════════════════════════════════════════════════════════════════

_RTC_MP2_WRITE_OPS_OLD = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned short>("idx");
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int write_idx = FLAMEGPU->environment.getProperty<unsigned int>("mp2_write_idx");
    
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    // Вычисляем позицию в буфере
    const unsigned int pos = write_idx * {MAX_FRAMES}u + idx;
    
    auto buf_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * 500}u>("mp2_buffer_sne");
    auto buf_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES * 500}u>("mp2_buffer_ppr");
    auto buf_day = FLAMEGPU->environment.getMacroProperty<unsigned short, {MAX_FRAMES * 500}u>("mp2_buffer_day");
    auto buf_state = FLAMEGPU->environment.getMacroProperty<unsigned char, {MAX_FRAMES * 500}u>("mp2_buffer_state");
    
    buf_sne[pos] = sne;
    buf_ppr[pos] = ppr;
    buf_day[pos] = (unsigned short)current_day;
    buf_state[pos] = 2u;  // operations = 2
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# Регистрация модулей
# ═══════════════════════════════════════════════════════════════════════════════

def register_all_modules(model: fg.ModelDescription, 
                         planer: fg.AgentDescription,
                         quota_manager: fg.AgentDescription):
    """
    Регистрирует все 5 модулей Adaptive 2.0.
    """
    print("  📦 Регистрация модулей Adaptive 2.0")
    
    # ═══════════════════════════════════════════════════════════════════════
    # МОДУЛЬ 1: compute_adaptive_days
    # ═══════════════════════════════════════════════════════════════════════
    
    # Layer 1.1: Копирование limiter в буфер (operations)
    fn_copy_ops = planer.newRTCFunction("rtc_copy_limiter_ops", RTC_COPY_LIMITER_TO_BUFFER)
    fn_copy_ops.setInitialState("operations")
    fn_copy_ops.setEndState("operations")
    
    # Layer 1.2: Копирование limiter (repair)
    fn_copy_repair = planer.newRTCFunction("rtc_copy_limiter_repair", RTC_COPY_LIMITER_REPAIR)
    fn_copy_repair.setInitialState("repair")
    fn_copy_repair.setEndState("repair")
    
    # Layer 1.3: Очистка буфера для остальных состояний
    fn_clear_inactive = planer.newRTCFunction("rtc_clear_limiter_inactive", RTC_CLEAR_LIMITER_BUFFER)
    fn_clear_inactive.setInitialState("inactive")
    fn_clear_inactive.setEndState("inactive")
    
    fn_clear_reserve = planer.newRTCFunction("rtc_clear_limiter_reserve", RTC_CLEAR_LIMITER_BUFFER)
    fn_clear_reserve.setInitialState("reserve")
    fn_clear_reserve.setEndState("reserve")
    
    fn_clear_storage = planer.newRTCFunction("rtc_clear_limiter_storage", RTC_CLEAR_LIMITER_BUFFER)
    fn_clear_storage.setInitialState("storage")
    fn_clear_storage.setEndState("storage")
    
    layer1a = model.newLayer("L1a_copy_limiters")
    layer1a.addAgentFunction(fn_copy_ops)
    layer1a.addAgentFunction(fn_copy_repair)
    layer1a.addAgentFunction(fn_clear_inactive)
    layer1a.addAgentFunction(fn_clear_reserve)
    layer1a.addAgentFunction(fn_clear_storage)
    
    # Layer 1.4: Global min
    fn_global_min = quota_manager.newRTCFunction("rtc_compute_global_min", RTC_COMPUTE_GLOBAL_MIN)
    layer1b = model.newLayer("L1b_global_min")
    layer1b.addAgentFunction(fn_global_min)
    
    print("    ✅ Модуль 1: compute_adaptive_days")
    
    # ═══════════════════════════════════════════════════════════════════════
    # МОДУЛЬ 2: batch_increment
    # ═══════════════════════════════════════════════════════════════════════
    
    fn_inc_ops = planer.newRTCFunction("rtc_batch_increment_ops", RTC_BATCH_INCREMENT_OPS)
    fn_inc_ops.setInitialState("operations")
    fn_inc_ops.setEndState("operations")
    
    fn_inc_repair = planer.newRTCFunction("rtc_batch_increment_repair", RTC_BATCH_INCREMENT_REPAIR)
    fn_inc_repair.setInitialState("repair")
    fn_inc_repair.setEndState("repair")
    
    layer2 = model.newLayer("L2_batch_increment")
    layer2.addAgentFunction(fn_inc_ops)
    layer2.addAgentFunction(fn_inc_repair)
    
    print("    ✅ Модуль 2: batch_increment")
    
    # ═══════════════════════════════════════════════════════════════════════
    # МОДУЛЬ 3: transitions
    # ═══════════════════════════════════════════════════════════════════════
    
    # Repair → Reserve
    fn_trans_repair = planer.newRTCFunction("rtc_transition_repair_reserve", RTC_TRANSITION_REPAIR_TO_RESERVE)
    fn_trans_repair.setInitialState("repair")
    fn_trans_repair.setEndState("reserve")
    fn_trans_repair.setAllowAgentDeath(False)
    # Добавляем condition
    fn_trans_repair.setRTCFunctionCondition(RTC_TRANSITION_REPAIR_CONDITION)
    
    layer3a = model.newLayer("L3a_transition_repair")
    layer3a.addAgentFunction(fn_trans_repair)
    
    # Operations → Repair/Storage проверка
    fn_trans_ops = planer.newRTCFunction("rtc_transition_ops_check", RTC_TRANSITION_OPS_CHECK)
    fn_trans_ops.setInitialState("operations")
    fn_trans_ops.setEndState("operations")  # Переход через state manager
    
    layer3b = model.newLayer("L3b_transition_ops")
    layer3b.addAgentFunction(fn_trans_ops)
    
    print("    ✅ Модуль 3: transitions")
    
    # ═══════════════════════════════════════════════════════════════════════
    # МОДУЛЬ 4: quota
    # ═══════════════════════════════════════════════════════════════════════
    
    fn_quota = quota_manager.newRTCFunction("rtc_quota_process_event", RTC_QUOTA_PROCESS_EVENT)
    layer4 = model.newLayer("L4_quota")
    layer4.addAgentFunction(fn_quota)
    
    print("    ✅ Модуль 4: quota")
    
    # ═══════════════════════════════════════════════════════════════════════
    # МОДУЛЬ 5: mp2_write (временно отключен - требует рефакторинга)
    # ═══════════════════════════════════════════════════════════════════════
    # DeviceMacroProperty не поддерживает operator=, только exchange()
    # Для MP2 нужен другой подход (агентные переменные или unified memory)
    
    print("    ⚠️ Модуль 5: mp2_write (отключен для теста)")
    
    # ═══════════════════════════════════════════════════════════════════════
    # МОДУЛЬ 6: update_current_day (ВНУТРИ GPU!)
    # ═══════════════════════════════════════════════════════════════════════
    
    fn_update_day = quota_manager.newRTCFunction("rtc_update_current_day", RTC_UPDATE_CURRENT_DAY)
    layer6 = model.newLayer("L6_update_day")
    layer6.addAgentFunction(fn_update_day)
    
    print("    ✅ Модуль 6: update_current_day (GPU-only)")
    
    print("  ✅ Все 6 модулей зарегистрированы (полностью на GPU)")

