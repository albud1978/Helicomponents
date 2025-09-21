#!/usr/bin/env python3
"""
Концепт: Использование States для оптимизации переходов статусов
"""

def register_rtc_with_states(model, agent):
    """Регистрирует RTC с использованием состояний агентов"""
    
    # Определяем состояния
    agent.newState("flying")        # status_id = 2
    agent.newState("awaiting_oh")   # status_id = 3  
    agent.newState("in_repair")     # status_id = 4
    agent.newState("ready")         # status_id = 5
    agent.newState("reserve")       # status_id = 1
    agent.newState("written_off")   # status_id = 6
    agent.newState("inactive")      # status_id = 0
    
    # Переменные для квотирования
    agent.newVariableUInt("intent_flag", 0)    # 0=нет, 1=хочу квоту
    agent.newVariableUInt("quota_ticket", 0)   # 0=нет, 1=получил квоту
    
    # RTC функции привязаны к состояниям
    
    # 1. ФАЗА INTENT (параллельно для каждого состояния)
    rtc_flying_intent = """
    FLAMEGPU_AGENT_FUNCTION(rtc_flying_intent, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
        const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
        
        // Если приближается капремонт - хочу квоту
        if (oh > 0u && ppr >= oh - 300u) {
            FLAMEGPU->setVariable<unsigned int>("intent_flag", 1u);
        }
        return flamegpu::ALIVE;
    }
    """
    agent.newRTCFunction("rtc_flying_intent", rtc_flying_intent)
    agent.newRTCFunctionCondition("cond_flying").setFunctionCondition("rtc_flying_intent", "FLAMEGPU->getVariable<unsigned int>('status_id') == 2")
    
    # 2. ФАЗА APPROVE (менеджер квот - выполняется одним агентом)
    rtc_quota_manager = """
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_manager, flamegpu::MessageNone, flamegpu::MessageNone) {
        if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
        
        // Читаем ops_counter
        unsigned int quota_mi8 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", day);
        
        // Массивы intent и approve
        auto intent_mi8 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_FRAMES>("mi8_intent");
        auto approve_mi8 = FLAMEGPU->environment.getMacroProperty<unsigned int, MAX_FRAMES>("mi8_approve");
        
        // Распределяем квоты по приоритетам
        // Приоритет 1: flying (status=2)
        for (unsigned int i = 0u; i < MAX_FRAMES; ++i) {
            if (quota_mi8 > 0u && intent_mi8[i] == 2u) {  // 2 = intent from flying
                approve_mi8[i] = 1u;
                quota_mi8--;
            }
        }
        
        // Приоритет 2: awaiting_oh (status=3)
        for (unsigned int i = 0u; i < MAX_FRAMES; ++i) {
            if (quota_mi8 > 0u && intent_mi8[i] == 3u) {
                approve_mi8[i] = 1u;
                quota_mi8--;
            }
        }
        
        // И так далее для других статусов...
        
        return flamegpu::ALIVE;
    }
    """
    
    # 3. ФАЗА TRANSITION (единая функция смены статусов)
    rtc_apply_transitions = """
    FLAMEGPU_AGENT_FUNCTION(rtc_apply_transitions, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int current_state = FLAMEGPU->getVariable<unsigned int>("status_id");
        const unsigned int has_ticket = FLAMEGPU->getVariable<unsigned int>("quota_ticket");
        const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
        const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
        const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
        const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
        
        unsigned int new_state = current_state;
        
        // Логика переходов централизована здесь
        switch(current_state) {
            case 2: // flying
                if (br > 0u && sne >= br) {
                    new_state = 6; // → written_off
                } else if (has_ticket && oh > 0u && ppr >= oh) {
                    new_state = 4; // → in_repair
                    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
                }
                break;
                
            case 3: // awaiting_oh
                if (has_ticket) {
                    new_state = 2; // → flying
                }
                break;
                
            case 4: // in_repair
                const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
                const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
                if (repair_days >= repair_time) {
                    new_state = 5; // → ready
                }
                break;
                
            // ... другие переходы
        }
        
        // Применяем новый статус
        if (new_state != current_state) {
            FLAMEGPU->setVariable<unsigned int>("status_id", new_state);
            // Здесь же можно изменить State для следующего шага
        }
        
        // Сбрасываем флаги
        FLAMEGPU->setVariable<unsigned int>("intent_flag", 0u);
        FLAMEGPU->setVariable<unsigned int>("quota_ticket", 0u);
        
        return flamegpu::ALIVE;
    }
    """
    
    # Создаем слои
    layer_intent = model.newLayer("intent_phase")
    layer_intent.addAgentFunction("rtc_flying_intent", "flying")
    layer_intent.addAgentFunction("rtc_awaiting_intent", "awaiting_oh")
    layer_intent.addAgentFunction("rtc_ready_intent", "ready")
    
    layer_approve = model.newLayer("approve_phase")
    layer_approve.addAgentFunction("rtc_quota_manager")
    
    layer_transition = model.newLayer("transition_phase")
    layer_transition.addAgentFunction("rtc_apply_transitions")  # Для всех состояний
    
    return [layer_intent, layer_approve, layer_transition]
