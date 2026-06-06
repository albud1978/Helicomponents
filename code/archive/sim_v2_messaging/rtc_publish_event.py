#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (sim_v2 dead-code cleanup): не используется активным V8-ядром (orchestrator_limiter_v8). Внутренние импорты заморожены.
"""
RTC модуль: Event-driven публикация событий (вместо polling)

Агенты публикуют события ТОЛЬКО когда что-то изменилось:
- DEMOUNT_EVENT (1): агент выходит из operations (2→4, 2→6, 2→3)
- READY_EVENT (2): агент готов к промоуту (repair завершён, inactive ready)
- LIMIT_EVENT (3): агент достиг лимита (sne >= ll ИЛИ ppr >= oh)

Это сокращает количество сообщений с ~570/шаг до ~1-5/шаг в среднем.
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
import model_build

import pyflamegpu as fg


# Типы событий
EVENT_NONE = 0
EVENT_DEMOUNT = 1      # Агент выходит из operations
EVENT_READY = 2        # Агент готов к промоуту
EVENT_LIMIT = 3        # Агент достиг лимита (нужен переход)


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции event-driven публикации"""
    
    print("  📤 Регистрация модуля: publish_event (EVENT-DRIVEN)")
    
    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция для OPERATIONS: ГИБРИД
    # - Если intent != 2 → публикует DEMOUNT (event_type=1)
    # - Если intent == 2 → публикует OPS_REPORT (event_type=3) для учёта и ранжирования
    # Это позволяет QuotaManager знать текущий состав operations для демоута
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_OPERATIONS_EVENT = """
FLAMEGPU_AGENT_FUNCTION(rtc_event_operations, flamegpu::MessageNone, flamegpu::MessageBruteForce) {
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int mfg_date = FLAMEGPU->getVariable<unsigned int>("mfg_date");
    
    // Публикуем сообщение ВСЕГДА (для HYBRID подхода)
    FLAMEGPU->message_out.setVariable<unsigned short>("idx", (unsigned short)idx);
    FLAMEGPU->message_out.setVariable<unsigned char>("group_by", (unsigned char)group_by);
    FLAMEGPU->message_out.setVariable<unsigned char>("state", 2u);  // operations
    FLAMEGPU->message_out.setVariable<unsigned char>("intent", (unsigned char)intent);
    FLAMEGPU->message_out.setVariable<unsigned short>("mfg_date", (unsigned short)mfg_date);
    FLAMEGPU->message_out.setVariable<unsigned char>("repair_ready", 0u);
    FLAMEGPU->message_out.setVariable<unsigned char>("skip_repair", 0u);
    
    if (intent != 2u) {
        // DEMOUNT: агент выходит из operations
        FLAMEGPU->message_out.setVariable<unsigned char>("event_type", 1u);
    } else {
        // OPS_REPORT: агент остаётся в operations (для учёта и ранжирования при демоуте)
        FLAMEGPU->message_out.setVariable<unsigned char>("event_type", 3u);
    }
    
    return flamegpu::ALIVE;
}
"""

    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция для SERVICEABLE: ГИБРИД (как operations)
    # ВСЕГДА публикует своё состояние для учёта при промоуте
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_SERVICEABLE_EVENT = """
FLAMEGPU_AGENT_FUNCTION(rtc_event_serviceable, flamegpu::MessageNone, flamegpu::MessageBruteForce) {
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int mfg_date = FLAMEGPU->getVariable<unsigned int>("mfg_date");
    
    // Serviceable агенты ВСЕГДА публикуют READY для промоута (если intent=2 или =3)
    // intent=2 = хочу в operations, intent=3 = хочу остаться (холдинг)
    // Для простоты публикуем всегда — QuotaManager сам решит кого промоутить
    
    FLAMEGPU->message_out.setVariable<unsigned short>("idx", (unsigned short)idx);
    FLAMEGPU->message_out.setVariable<unsigned char>("group_by", (unsigned char)group_by);
    FLAMEGPU->message_out.setVariable<unsigned char>("state", 3u);  // serviceable
    FLAMEGPU->message_out.setVariable<unsigned char>("intent", (unsigned char)intent);
    FLAMEGPU->message_out.setVariable<unsigned short>("mfg_date", (unsigned short)mfg_date);
    FLAMEGPU->message_out.setVariable<unsigned char>("event_type", 2u);  // READY
    FLAMEGPU->message_out.setVariable<unsigned char>("repair_ready", 1u);
    FLAMEGPU->message_out.setVariable<unsigned char>("skip_repair", 0u);
    
    return flamegpu::ALIVE;
}
"""

    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция для RESERVE: проверяет готовность к промоуту
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_RESERVE_EVENT = """
FLAMEGPU_AGENT_FUNCTION(rtc_event_reserve, flamegpu::MessageNone, flamegpu::MessageBruteForce) {
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int mfg_date = FLAMEGPU->getVariable<unsigned int>("mfg_date");
    const unsigned int prev_intent = FLAMEGPU->getVariable<unsigned int>("prev_intent");
    
    const bool intent_changed = (prev_intent != intent);
    const bool wants_promote = (intent == 2u);
    
    if (intent_changed && wants_promote) {
        FLAMEGPU->message_out.setVariable<unsigned short>("idx", (unsigned short)idx);
        FLAMEGPU->message_out.setVariable<unsigned char>("group_by", (unsigned char)group_by);
        FLAMEGPU->message_out.setVariable<unsigned char>("state", 5u);  // reserve
        FLAMEGPU->message_out.setVariable<unsigned char>("intent", (unsigned char)intent);
        FLAMEGPU->message_out.setVariable<unsigned short>("mfg_date", (unsigned short)mfg_date);
        FLAMEGPU->message_out.setVariable<unsigned char>("event_type", 2u);  // READY
        FLAMEGPU->message_out.setVariable<unsigned char>("repair_ready", 1u);
        FLAMEGPU->message_out.setVariable<unsigned char>("skip_repair", 0u);
    }
    
    FLAMEGPU->setVariable<unsigned int>("prev_intent", intent);
    
    return flamegpu::ALIVE;
}
"""

    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция для INACTIVE: проверяет готовность к активации
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_INACTIVE_EVENT = """
FLAMEGPU_AGENT_FUNCTION(rtc_event_inactive, flamegpu::MessageNone, flamegpu::MessageBruteForce) {
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int mfg_date = FLAMEGPU->getVariable<unsigned int>("mfg_date");
    const unsigned int prev_intent = FLAMEGPU->getVariable<unsigned int>("prev_intent");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    // Вычисляем repair_ready и skip_repair
    const unsigned char repair_ready = (repair_days >= repair_time) ? 1u : 0u;
    const unsigned int br2_mi17 = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br2_const");
    const unsigned char skip_repair = (group_by == 2u && ppr < br2_mi17) ? 1u : 0u;
    
    const bool intent_changed = (prev_intent != intent);
    // intent == 2 (promote to ops) или intent == 4 (repair)
    const bool wants_action = (intent == 2u || intent == 4u);
    
    if (intent_changed && wants_action && (repair_ready || skip_repair)) {
        FLAMEGPU->message_out.setVariable<unsigned short>("idx", (unsigned short)idx);
        FLAMEGPU->message_out.setVariable<unsigned char>("group_by", (unsigned char)group_by);
        FLAMEGPU->message_out.setVariable<unsigned char>("state", 1u);  // inactive
        FLAMEGPU->message_out.setVariable<unsigned char>("intent", (unsigned char)intent);
        FLAMEGPU->message_out.setVariable<unsigned short>("mfg_date", (unsigned short)mfg_date);
        FLAMEGPU->message_out.setVariable<unsigned char>("event_type", 2u);  // READY
        FLAMEGPU->message_out.setVariable<unsigned char>("repair_ready", repair_ready);
        FLAMEGPU->message_out.setVariable<unsigned char>("skip_repair", skip_repair);
    }
    
    FLAMEGPU->setVariable<unsigned int>("prev_intent", intent);
    
    return flamegpu::ALIVE;
}
"""

    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция для REPAIR: проверяет завершение ремонта
    # Публикует EVENT_READY когда repair_days >= repair_time
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_REPAIR_EVENT = """
FLAMEGPU_AGENT_FUNCTION(rtc_event_repair, flamegpu::MessageNone, flamegpu::MessageBruteForce) {
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int mfg_date = FLAMEGPU->getVariable<unsigned int>("mfg_date");
    const unsigned int prev_intent = FLAMEGPU->getVariable<unsigned int>("prev_intent");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    
    // Repair завершён когда repair_days >= repair_time
    const unsigned char repair_complete = (repair_days >= repair_time) ? 1u : 0u;
    
    const bool intent_changed = (prev_intent != intent);
    const bool wants_out = (intent == 5u);  // хочет в reserve
    
    // Публикуем событие когда ремонт завершён И intent изменился
    if (intent_changed && wants_out && repair_complete) {
        FLAMEGPU->message_out.setVariable<unsigned short>("idx", (unsigned short)idx);
        FLAMEGPU->message_out.setVariable<unsigned char>("group_by", (unsigned char)group_by);
        FLAMEGPU->message_out.setVariable<unsigned char>("state", 4u);  // repair
        FLAMEGPU->message_out.setVariable<unsigned char>("intent", (unsigned char)intent);
        FLAMEGPU->message_out.setVariable<unsigned short>("mfg_date", (unsigned short)mfg_date);
        FLAMEGPU->message_out.setVariable<unsigned char>("event_type", 2u);  // READY
        FLAMEGPU->message_out.setVariable<unsigned char>("repair_ready", 1u);
        FLAMEGPU->message_out.setVariable<unsigned char>("skip_repair", 0u);
    }
    
    FLAMEGPU->setVariable<unsigned int>("prev_intent", intent);
    
    return flamegpu::ALIVE;
}
"""

    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция для STORAGE: обычно не публикует события (конечное состояние)
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_STORAGE_EVENT = """
FLAMEGPU_AGENT_FUNCTION(rtc_event_storage, flamegpu::MessageNone, flamegpu::MessageBruteForce) {
    // Storage - конечное состояние, обычно нет событий
    // Но обновляем prev_intent для консистентности
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    FLAMEGPU->setVariable<unsigned int>("prev_intent", intent);
    return flamegpu::ALIVE;
}
"""
    
    # Регистрируем функции для каждого состояния
    state_rtc = {
        "inactive": RTC_INACTIVE_EVENT,
        "operations": RTC_OPERATIONS_EVENT,
        "serviceable": RTC_SERVICEABLE_EVENT,
        "repair": RTC_REPAIR_EVENT,
        "reserve": RTC_RESERVE_EVENT,
        "storage": RTC_STORAGE_EVENT
    }
    
    # Состояния с обязательным выводом сообщений (для подсчёта и ранжирования)
    required_output_states = ["operations", "serviceable"]
    
    for state_name, rtc_code in state_rtc.items():
        layer = model.newLayer(f"event_{state_name}")
        func_name = f"rtc_event_{state_name}"
        rtc_func = agent.newRTCFunction(func_name, rtc_code)
        rtc_func.setInitialState(state_name)
        rtc_func.setEndState(state_name)
        rtc_func.setMessageOutput("PlanerEvent")
        # operations и serviceable ВСЕГДА публикуют, остальные — опционально
        if state_name not in required_output_states:
            rtc_func.setMessageOutputOptional(True)
        layer.addAgentFunction(rtc_func)
    
    print(f"    ✅ Зарегистрировано {len(state_rtc)} event-функций (ops+svc=required, rest=optional)")

