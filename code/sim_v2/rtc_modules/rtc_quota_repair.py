"""
RTC модуль для квотирования ремонтов (repairs quota)
Каскадная архитектура: сначала очередь (reserve&intent=0), затем новые запросы (operations&intent=4)
Приоритизация: youngest first (больший idx = моложе)
"""

import pyflamegpu as fg
from string import Template

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции для квотирования ремонтов"""
    print("  Регистрация модуля квотирования: ремонты (repair quota)")
    
    # Получаем MAX_FRAMES из модели
    max_frames = model.Environment().getPropertyUInt("frames_total")
    
    print(f"  📊 MAX_FRAMES = {max_frames} (для буферов quota_repair)")
    
    # RTC код с использованием Template для подстановки
    RTC_QUOTA_REPAIR_TEMPLATE = Template("""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_repair, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Фильтр: только кандидаты (reserve&0 или operations&4)
    if (intent != 0u && intent != 4u) {
        return flamegpu::ALIVE;
    }
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // Только планеры Mi-8/Mi-17
    if (group_by != 1u && group_by != 2u) {
        return flamegpu::ALIVE;
    }
    
    // ✅ Читаем repair_number из MacroProperty по idx
    auto repair_number_by_idx = FLAMEGPU->environment.getMacroProperty<unsigned char, $MAX_FRAMES>("repair_number_by_idx");
    const unsigned char repair_number = repair_number_by_idx[idx];
    
    // 0 означает отсутствие квоты
    if (repair_number == 0u) {
        return flamegpu::ALIVE;
    }
    
    // ═══════════════════════════════════════════════════════════════
    // ШАГ 2: Подсчитать curr_in_repair (агенты с тем же repair_number)
    // ═══════════════════════════════════════════════════════════════
    unsigned int curr_in_repair = 0u;
    auto repair_state_buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, $MAX_FRAMES>("repair_state_buffer");
    
    // ✅ ТОЧНЫЙ подсчёт: только агенты с ТЕМ ЖЕ repair_number
    for (unsigned int i = 0u; i < frames; ++i) {
        if (repair_state_buffer[i] != 1u) continue;  // Только агенты в repair
        
        // Проверяем repair_number агента i
        const unsigned char rn_i = repair_number_by_idx[i];
        if (rn_i == repair_number) {
            ++curr_in_repair;
        }
    }
    
    // Рассчитать available
    const unsigned int quota = static_cast<unsigned int>(repair_number);
    const int available = static_cast<int>(quota) - static_cast<int>(curr_in_repair);
    
    // Early exit
    if (available <= 0) {
        if (intent == 4u) {
            FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
        }
        return flamegpu::ALIVE;
    }
    
    // ═══════════════════════════════════════════════════════════════
    // ШАГ 3: Ранжирование youngest first среди кандидатов
    // ═══════════════════════════════════════════════════════════════
    unsigned int rank = 0u;
    auto reserve_queue_buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, $MAX_FRAMES>("reserve_queue_buffer");
    auto ops_repair_buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, $MAX_FRAMES>("ops_repair_buffer");
    
    for (unsigned int i = 0u; i < frames; ++i) {
        if (i == idx) continue;
        
        const bool is_candidate = (reserve_queue_buffer[i] == 1u) || (ops_repair_buffer[i] == 1u);
        if (!is_candidate) continue;
        
        // ✅ ТОЧНАЯ ФИЛЬТРАЦИЯ: проверяем что repair_number совпадает
        const unsigned char rn_i = repair_number_by_idx[i];
        if (rn_i != repair_number) continue;
        
        // Youngest first: rank растёт если other (i) МОЛОЖЕ меня (больший idx)
        if (i > idx) {
            ++rank;
        }
    }
    
    // ═══════════════════════════════════════════════════════════════
    // ШАГ 4: Каскадное одобрение
    // ═══════════════════════════════════════════════════════════════
    const unsigned int K = static_cast<unsigned int>(available);
    
    if (rank < K) {
        // ✅ ОДОБРЕН
        if (intent == 0u) {
            // reserve&0 → intent=4 (из очереди в ремонт)
            FLAMEGPU->setVariable<unsigned int>("intent_state", 4u);
            const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
            printf("  [REPAIR APPROVE QUEUE Day %u] AC %u (idx %u): rank=%u/%u reserve->repair (quota=%u, in_repair=%u)\\n", 
                   step_day, aircraft_number, idx, rank, K, quota, curr_in_repair);
        }
        // operations&4 остаётся intent=4 (без изменений)
        if (intent == 4u) {
            const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
            printf("  [REPAIR APPROVE NEW Day %u] AC %u (idx %u): rank=%u/%u operations->repair (quota=%u, in_repair=%u)\\n", 
                   step_day, aircraft_number, idx, rank, K, quota, curr_in_repair);
        }
    } else {
        // ❌ НЕ ОДОБРЕН
        if (intent == 4u) {
            // operations&4 → intent=5 (отклонён)
            FLAMEGPU->setVariable<unsigned int>("intent_state", 5u);
            if (step_day <= 10u || step_day == 180u || step_day == 181u || step_day == 182u) {
                const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
                printf("  [REPAIR REJECT Day %u] AC %u (idx %u): rank=%u, available=%d (quota=%u, in_repair=%u)\\n", 
                       step_day, aircraft_number, idx, rank, available, quota, curr_in_repair);
            }
        }
        // reserve&0 остаётся intent=0 (без изменений)
    }
    
    return flamegpu::ALIVE;
}
""")
    
    # Подставляем значения
    rtc_code = RTC_QUOTA_REPAIR_TEMPLATE.substitute(
        MAX_FRAMES=f"{max_frames}u"
    )
    
    # Регистрируем слой
    layer_repair = model.newLayer("quota_repair")
    
    # RTC функция для reserve
    rtc_func_reserve = agent.newRTCFunction("rtc_quota_repair_reserve", rtc_code)
    rtc_func_reserve.setAllowAgentDeath(False)
    rtc_func_reserve.setInitialState("reserve")
    rtc_func_reserve.setEndState("reserve")
    layer_repair.addAgentFunction(rtc_func_reserve)
    
    # RTC функция для operations
    rtc_func_ops = agent.newRTCFunction("rtc_quota_repair_ops", rtc_code)
    rtc_func_ops.setAllowAgentDeath(False)
    rtc_func_ops.setInitialState("operations")
    rtc_func_ops.setEndState("operations")
    layer_repair.addAgentFunction(rtc_func_ops)
    
    print("  RTC модуль quota_repair зарегистрирован (1 слой, каскадное квотирование, youngest first)")
