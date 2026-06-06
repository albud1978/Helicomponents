#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (sim_v2 dead-code cleanup): не используется активным V8-ядром (orchestrator_limiter_v8). Внутренние импорты заморожены.
"""
RTC модуль: Однофазное квотирование V7

Архитектура (5 слоёв вместо 8+):
1. reset_counters — QM обнуляет счётчики и слоты
2. count — все агенты инкрементируют счётчики своего состояния
3. compute_quotas — QM вычисляет p1/p2/p3_quota и demote_quota
4. demote — ops агенты: atomicAdd → если slot < demote_quota → 2→3
5. promote — svc/unsvc/ina агенты: atomicAdd → если slot < quota → переход в ops

Преимущества:
- Нет флагов promoted/needs_demote
- Нет отдельных apply_* слоёв
- Меньше RTC функций

Дата: 13.01.2026
"""

import pyflamegpu as fg


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 1: СБРОС БУФЕРОВ (QuotaManager)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_RESET_COUNTERS = """
FLAMEGPU_AGENT_FUNCTION(rtc_reset_counters_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int gb = (unsigned int)FLAMEGPU->getVariable<unsigned char>("group_by");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // Сброс буферов подсчёта (каждый QM сбрасывает свою группу)
    if (gb == 1u) {
        auto ops = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi8_ops_buf");
        auto svc = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi8_svc_buf");
        auto unsvc = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi8_unsvc_buf");
        auto ina = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi8_ina_buf");
        for (unsigned int i = 0u; i < frames && i < 400u; ++i) {
            ops[i].exchange(0u);
            svc[i].exchange(0u);
            unsvc[i].exchange(0u);
            ina[i].exchange(0u);
        }
    } else if (gb == 2u) {
        auto ops = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi17_ops_buf");
        auto svc = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi17_svc_buf");
        auto unsvc = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi17_unsvc_buf");
        auto ina = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi17_ina_buf");
        for (unsigned int i = 0u; i < frames && i < 400u; ++i) {
            ops[i].exchange(0u);
            svc[i].exchange(0u);
            unsvc[i].exchange(0u);
            ina[i].exchange(0u);
        }
    }
    
    // Сброс слотов
    auto demote_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_demote_slots");
    auto p1_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p1_slots");
    auto p2_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p2_slots");
    auto p3_slots = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p3_slots");
    
    demote_slots[gb].exchange(0u);
    p1_slots[gb].exchange(0u);
    p2_slots[gb].exchange(0u);
    p3_slots[gb].exchange(0u);
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 2: ПОДСЧЁТ АГЕНТОВ (каждый пишет 1 в свою ячейку по idx)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COUNT_OPS = """
FLAMEGPU_AGENT_FUNCTION(rtc_count_ops_onephase, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    if (gb == 1u) {
        auto buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi8_ops_buf");
        if (idx < 400u) buf[idx].exchange(1u);
    } else if (gb == 2u) {
        auto buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi17_ops_buf");
        if (idx < 400u) buf[idx].exchange(1u);
    }
    return flamegpu::ALIVE;
}
"""

RTC_COUNT_SVC = """
FLAMEGPU_AGENT_FUNCTION(rtc_count_svc_onephase, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    if (gb == 1u) {
        auto buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi8_svc_buf");
        if (idx < 400u) buf[idx].exchange(1u);
    } else if (gb == 2u) {
        auto buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi17_svc_buf");
        if (idx < 400u) buf[idx].exchange(1u);
    }
    return flamegpu::ALIVE;
}
"""

RTC_COUNT_UNSVC = """
FLAMEGPU_AGENT_FUNCTION(rtc_count_unsvc_onephase, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    if (gb == 1u) {
        auto buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi8_unsvc_buf");
        if (idx < 400u) buf[idx].exchange(1u);
    } else if (gb == 2u) {
        auto buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi17_unsvc_buf");
        if (idx < 400u) buf[idx].exchange(1u);
    }
    return flamegpu::ALIVE;
}
"""

RTC_COUNT_INA = """
FLAMEGPU_AGENT_FUNCTION(rtc_count_ina_onephase, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    if (gb == 1u) {
        auto buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi8_ina_buf");
        if (idx < 400u) buf[idx].exchange(1u);
    } else if (gb == 2u) {
        auto buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi17_ina_buf");
        if (idx < 400u) buf[idx].exchange(1u);
    }
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 3: ВЫЧИСЛЕНИЕ КВОТ (QuotaManager суммирует буферы)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COMPUTE_QUOTAS = """
FLAMEGPU_AGENT_FUNCTION(rtc_compute_quotas_v7, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int gb = (unsigned int)FLAMEGPU->getVariable<unsigned char>("group_by");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // Текущий день
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    const unsigned int safe_day = (current_day < end_day) ? current_day : (end_day - 1u);
    
    // Целевое количество в operations из mp4_ops_counter[day]
    unsigned int target = 0u;
    if (gb == 1u) {
        target = FLAMEGPU->environment.getProperty<unsigned int, 4000u>("mp4_ops_counter_mi8", safe_day);
    } else if (gb == 2u) {
        target = FLAMEGPU->environment.getProperty<unsigned int, 4000u>("mp4_ops_counter_mi17", safe_day);
    }
    
    // Суммируем буферы
    unsigned int ops = 0u, svc = 0u, unsvc = 0u, ina = 0u;
    
    if (gb == 1u) {
        auto ops_buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi8_ops_buf");
        auto svc_buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi8_svc_buf");
        auto unsvc_buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi8_unsvc_buf");
        auto ina_buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi8_ina_buf");
        for (unsigned int i = 0u; i < frames && i < 400u; ++i) {
            ops += ops_buf[i];
            svc += svc_buf[i];
            unsvc += unsvc_buf[i];
            ina += ina_buf[i];
        }
    } else if (gb == 2u) {
        auto ops_buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi17_ops_buf");
        auto svc_buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi17_svc_buf");
        auto unsvc_buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi17_unsvc_buf");
        auto ina_buf = FLAMEGPU->environment.getMacroProperty<unsigned int, 400u>("mi17_ina_buf");
        for (unsigned int i = 0u; i < frames && i < 400u; ++i) {
            ops += ops_buf[i];
            svc += svc_buf[i];
            unsvc += unsvc_buf[i];
            ina += ina_buf[i];
        }
    }
    
    // Квоты
    auto demote_quota = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_demote");
    auto p1_quota = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p1");
    auto p2_quota = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p2");
    auto p3_quota = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p3");
    
    // Вычисление
    if (ops > target) {
        // Избыток — демоут
        demote_quota[gb].exchange(ops - target);
        p1_quota[gb].exchange(0u);
        p2_quota[gb].exchange(0u);
        p3_quota[gb].exchange(0u);
    } else if (ops < target) {
        // Дефицит — промоут
        demote_quota[gb].exchange(0u);
        
        unsigned int deficit = target - ops;
        
        // P1: serviceable
        unsigned int p1 = (deficit < svc) ? deficit : svc;
        p1_quota[gb].exchange(p1);
        deficit -= p1;
        
        // P2: unserviceable
        unsigned int p2 = (deficit < unsvc) ? deficit : unsvc;
        p2_quota[gb].exchange(p2);
        deficit -= p2;
        
        // P3: inactive
        unsigned int p3 = (deficit < ina) ? deficit : ina;
        p3_quota[gb].exchange(p3);
    } else {
        // Баланс
        demote_quota[gb].exchange(0u);
        p1_quota[gb].exchange(0u);
        p2_quota[gb].exchange(0u);
        p3_quota[gb].exchange(0u);
    }
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 4: ДЕМОУТ (operations → serviceable)
# Используем CAS через exchange() т.к. MacroProperty не поддерживает atomicAdd
# ═══════════════════════════════════════════════════════════════════════════════

COND_DEMOTE = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_demote_onephase) {
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    auto demote_quota = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_demote");
    return demote_quota[gb] > 0u;
}
"""

RTC_DEMOTE = """
FLAMEGPU_AGENT_FUNCTION(rtc_demote_onephase, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    auto demote_quota = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_demote");
    auto slots = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_demote_slots");
    
    const unsigned int quota = demote_quota[gb];
    if (quota == 0u) return flamegpu::ALIVE;
    
    // CAS: занимаем слот через exchange()
    unsigned int old_val;
    unsigned int my_slot;
    do {
        old_val = slots[gb];
        if (old_val >= quota) {
            // Все слоты заняты
            return flamegpu::ALIVE;
        }
        my_slot = old_val;
    } while (slots[gb].exchange(old_val + 1u) != old_val);
    
    // Успешно заняли слот — переход operations → serviceable
    FLAMEGPU->setVariable<unsigned int>("transition_2_to_3", 1u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 5: ПРОМОУТЫ (P1, P2, P3)
# ═══════════════════════════════════════════════════════════════════════════════

# P1: serviceable → operations
COND_P1 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_p1_onephase) {
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    auto p1_quota = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p1");
    return p1_quota[gb] > 0u;
}
"""

RTC_P1 = """
FLAMEGPU_AGENT_FUNCTION(rtc_p1_onephase, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    auto p1_quota = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p1");
    auto slots = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p1_slots");
    
    const unsigned int quota = p1_quota[gb];
    if (quota == 0u) return flamegpu::ALIVE;
    
    // CAS: занимаем слот
    unsigned int old_val;
    do {
        old_val = slots[gb];
        if (old_val >= quota) return flamegpu::ALIVE;
    } while (slots[gb].exchange(old_val + 1u) != old_val);
    
    // P1: serviceable → operations (PPR сохраняется)
    FLAMEGPU->setVariable<unsigned int>("transition_3_to_2", 1u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);
    return flamegpu::ALIVE;
}
"""

# P2: unserviceable → operations
COND_P2 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_p2_onephase) {
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    auto p2_quota = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p2");
    return p2_quota[gb] > 0u;
}
"""

RTC_P2 = """
FLAMEGPU_AGENT_FUNCTION(rtc_p2_onephase, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    auto p2_quota = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p2");
    auto slots = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p2_slots");
    
    const unsigned int quota = p2_quota[gb];
    if (quota == 0u) return flamegpu::ALIVE;
    
    // CAS: занимаем слот
    unsigned int old_val;
    do {
        old_val = slots[gb];
        if (old_val >= quota) return flamegpu::ALIVE;
    } while (slots[gb].exchange(old_val + 1u) != old_val);
    
    // P2: unserviceable → operations (PPR обнуляется!)
    FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    FLAMEGPU->setVariable<unsigned int>("transition_7_to_2", 1u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);
    return flamegpu::ALIVE;
}
"""

# P3: inactive → operations
COND_P3 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_p3_onephase) {
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    auto p3_quota = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p3");
    return p3_quota[gb] > 0u;
}
"""

RTC_P3 = """
FLAMEGPU_AGENT_FUNCTION(rtc_p3_onephase, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    auto p3_quota = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p3");
    auto slots = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("quota_p3_slots");
    
    const unsigned int quota = p3_quota[gb];
    if (quota == 0u) return flamegpu::ALIVE;
    
    // CAS: занимаем слот
    unsigned int old_val;
    do {
        old_val = slots[gb];
        if (old_val >= quota) return flamegpu::ALIVE;
    } while (slots[gb].exchange(old_val + 1u) != old_val);
    
    // P3: inactive → operations
    // PPR по правилам group_by (Mi-17: если PPR < br2 — сохраняем)
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int br2_mi17 = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br2_const");
    
    if (gb == 2u && ppr < br2_mi17) {
        // Комплектация без ремонта — PPR сохраняется
    } else {
        // Ремонт — PPR обнуляется
        FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
    }
    
    FLAMEGPU->setVariable<unsigned int>("transition_1_to_2", 1u);
    FLAMEGPU->setVariable<unsigned short>("limiter", 0u);
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# РЕГИСТРАЦИЯ
# ═══════════════════════════════════════════════════════════════════════════════

def setup_quota_macroproperties(model: fg.ModelDescription):
    """Создаёт MacroProperty для однофазного квотирования"""
    env = model.Environment()
    
    # Буферы подсчёта по idx (размер 400 = RTC_MAX_FRAMES)
    env.newMacroPropertyUInt("mi8_ops_buf", 400)
    env.newMacroPropertyUInt("mi8_svc_buf", 400)
    env.newMacroPropertyUInt("mi8_unsvc_buf", 400)
    env.newMacroPropertyUInt("mi8_ina_buf", 400)
    
    env.newMacroPropertyUInt("mi17_ops_buf", 400)
    env.newMacroPropertyUInt("mi17_svc_buf", 400)
    env.newMacroPropertyUInt("mi17_unsvc_buf", 400)
    env.newMacroPropertyUInt("mi17_ina_buf", 400)
    
    # Квоты [0, Mi-8, Mi-17, reserved]
    env.newMacroPropertyUInt("quota_demote", 4)
    env.newMacroPropertyUInt("quota_p1", 4)
    env.newMacroPropertyUInt("quota_p2", 4)
    env.newMacroPropertyUInt("quota_p3", 4)
    
    # Слоты для CAS
    env.newMacroPropertyUInt("quota_demote_slots", 4)
    env.newMacroPropertyUInt("quota_p1_slots", 4)
    env.newMacroPropertyUInt("quota_p2_slots", 4)
    env.newMacroPropertyUInt("quota_p3_slots", 4)
    
    print("  ✅ Quota MacroProperty: буферы[400] + квоты + слоты (однофазная)")


def register_quota_onephase(model: fg.ModelDescription, 
                            agent: fg.AgentDescription,
                            quota_agent: fg.AgentDescription):
    """Регистрирует однофазное квотирование (5 слоёв)"""
    
    print("\n📦 V7 Однофазное квотирование...")
    
    # Слой 1: reset_counters (QM)
    layer_reset = model.newLayer("v7_quota_reset")
    fn = quota_agent.newRTCFunction("rtc_reset_counters_v7", RTC_RESET_COUNTERS)
    layer_reset.addAgentFunction(fn)
    
    # Слой 2: count (все агенты в одном слое — разные состояния)
    layer_count = model.newLayer("v7_quota_count")
    
    fn = agent.newRTCFunction("rtc_count_ops_onephase", RTC_COUNT_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_svc_onephase", RTC_COUNT_SVC)
    fn.setInitialState("serviceable")
    fn.setEndState("serviceable")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_unsvc_onephase", RTC_COUNT_UNSVC)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_count.addAgentFunction(fn)
    
    fn = agent.newRTCFunction("rtc_count_ina_onephase", RTC_COUNT_INA)
    fn.setInitialState("inactive")
    fn.setEndState("inactive")
    layer_count.addAgentFunction(fn)
    
    # Слой 3: compute_quotas (QM)
    layer_compute = model.newLayer("v7_quota_compute")
    fn = quota_agent.newRTCFunction("rtc_compute_quotas_v7", RTC_COMPUTE_QUOTAS)
    layer_compute.addAgentFunction(fn)
    
    # Слой 4: demote (operations → serviceable)
    layer_demote = model.newLayer("v7_quota_demote")
    fn = agent.newRTCFunction("rtc_demote_onephase", RTC_DEMOTE)
    fn.setRTCFunctionCondition(COND_DEMOTE)
    fn.setInitialState("operations")
    fn.setEndState("serviceable")
    layer_demote.addAgentFunction(fn)
    
    # Слои 5-7: promote (FLAME GPU не позволяет одинаковый output state в одном слое)
    
    # Слой 5: P1 (serviceable → operations)
    layer_p1 = model.newLayer("v7_quota_p1")
    fn = agent.newRTCFunction("rtc_p1_onephase", RTC_P1)
    fn.setRTCFunctionCondition(COND_P1)
    fn.setInitialState("serviceable")
    fn.setEndState("operations")
    layer_p1.addAgentFunction(fn)
    
    # Слой 6: P2 (unserviceable → operations)
    layer_p2 = model.newLayer("v7_quota_p2")
    fn = agent.newRTCFunction("rtc_p2_onephase", RTC_P2)
    fn.setRTCFunctionCondition(COND_P2)
    fn.setInitialState("unserviceable")
    fn.setEndState("operations")
    layer_p2.addAgentFunction(fn)
    
    # Слой 7: P3 (inactive → operations)
    layer_p3 = model.newLayer("v7_quota_p3")
    fn = agent.newRTCFunction("rtc_p3_onephase", RTC_P3)
    fn.setRTCFunctionCondition(COND_P3)
    fn.setInitialState("inactive")
    fn.setEndState("operations")
    layer_p3.addAgentFunction(fn)
    
    print("    ✅ Слой 1: reset_counters (QM)")
    print("    ✅ Слой 2: count (ops/svc/unsvc/ina)")
    print("    ✅ Слой 3: compute_quotas (QM)")
    print("    ✅ Слой 4: demote (ops→svc)")
    print("    ✅ Слой 5: P1 (svc→ops)")
    print("    ✅ Слой 6: P2 (unsvc→ops)")
    print("    ✅ Слой 7: P3 (ina→ops)")
    print("  ✅ Однофазное квотирование готово (7 слоёв)")

