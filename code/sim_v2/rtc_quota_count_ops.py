#!/usr/bin/env python3
"""
RTC модуль для подсчёта агентов в operations и serviceable
Должен выполняться ПОСЛЕ state_2_operations (когда intent уже установлен)

ВАРИАНТ B: Считаем агентов по intent=2, а не по state
- operations агенты с intent=2 → хотят остаться в operations
- serviceable без проверки intent (для ранжирования в промоуте)

ВАЖНО: Первый агент (idx=0) сбрасывает ВСЕ буферы перед подсчётом!
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import model_build

import pyflamegpu as fg

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Подсчёт агентов в operations для каждого group_by"""
    
    # =========================================================================
    # Слой 1: Обнуление буферов (выполняет только первый агент)
    # =========================================================================
    # ✅ ФИКСИРОВАННЫЙ MAX_FRAMES для RTC кэширования
    # Runtime frames_total передаётся через Environment для индексации
    max_frames = model_build.RTC_MAX_FRAMES
    max_days = model_build.MAX_DAYS
    MP2_SIZE = max_frames * (max_days + 1)
    print(f"  🔍 DEBUG count_ops: max_frames={max_frames}, max_days={max_days}")
    
    RTC_RESET_BUFFERS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_reset_quota_buffers, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Только первый агент (idx=0) обнуляет буферы
    if (idx == 0u) {{
        // Буферы подсчёта по состояниям
        auto mi8_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_ops_count");
        auto mi17_ops = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_ops_count");
        auto mi8_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_svc_count");
        auto mi17_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_svc_count");
        auto mi8_res = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_reserve_count");
        auto mi17_res = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_reserve_count");
        auto mi8_ina = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_inactive_count");
        auto mi17_ina = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_inactive_count");
        
        // Буферы approve для квотирования (демоут + промоуты P1/P2/P3)
        auto mi8_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve");
        auto mi17_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve");
        auto mi8_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s3");
        auto mi17_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s3");
        auto mi8_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s5");
        auto mi17_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s5");
        auto mi8_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_approve_s1");
        auto mi17_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_approve_s1");
        
        // Spawn pending флаги
        auto mi8_spawn_pending = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_spawn_pending");
        auto mi17_spawn_pending = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_spawn_pending");
        
        // Буферы для квотирования ремонтов
        auto repair_state_buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("repair_state_buffer");
        auto reserve_queue_buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("reserve_queue_buffer");
        auto ops_repair_buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("ops_repair_buffer");
        
        // Сброс ВСЕХ буферов
        for (unsigned int i = 0u; i < {max_frames}u; ++i) {{
            // Подсчёт по состояниям
            mi8_ops[i].exchange(0u);
            mi17_ops[i].exchange(0u);
            mi8_svc[i].exchange(0u);
            mi17_svc[i].exchange(0u);
            mi8_res[i].exchange(0u);
            mi17_res[i].exchange(0u);
            mi8_ina[i].exchange(0u);
            mi17_ina[i].exchange(0u);
            
            // Approve флаги (демоут + промоуты)
            mi8_approve[i].exchange(0u);
            mi17_approve[i].exchange(0u);
            mi8_approve_s3[i].exchange(0u);
            mi17_approve_s3[i].exchange(0u);
            mi8_approve_s5[i].exchange(0u);
            mi17_approve_s5[i].exchange(0u);
            mi8_approve_s1[i].exchange(0u);
            mi17_approve_s1[i].exchange(0u);
            
            // Spawn pending флаги
            mi8_spawn_pending[i].exchange(0u);
            mi17_spawn_pending[i].exchange(0u);
            
            // Буферы квотирования ремонтов
            repair_state_buffer[i].exchange(0u);
            reserve_queue_buffer[i].exchange(0u);
            ops_repair_buffer[i].exchange(0u);
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    rtc_reset = agent.newRTCFunction("rtc_reset_quota_buffers", RTC_RESET_BUFFERS)
    # ✅ ИСПРАВЛЕНИЕ: Убираем фильтр по state, чтобы reset срабатывал для ВСЕХ агентов
    # Только первый агент (idx=0) сбросит буферы, остальные просто пройдут
    # rtc_reset.setInitialState("operations")  ← УДАЛЕНО
    # rtc_reset.setEndState("operations")      ← УДАЛЕНО
    
    layer_reset = model.newLayer("reset_quota_buffers")
    layer_reset.addAgentFunction(rtc_reset)
    
    # =========================================================================
    # Слой 2: Подсчёт агентов в operations с intent=2
    # =========================================================================
    RTC_COUNT_OPS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_ops, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    
    // DEBUG для агента 100006 (idx=285) в дни 824-826
    if ((step_day >= 824u && step_day <= 826u) && idx == 285u && group_by == 2u) {{
        const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("[DEBUG Day %u COUNT_OPS] Agent idx=%u (ACN=%u): intent=%u, group_by=%u\\n", 
               step_day, idx, acn, intent, group_by);
    }}
    
    // ✅ ВАРИАНТ B: Считаем только агентов с intent=2 (хотят быть в operations)
    if (intent == 2u) {{
        if (group_by == 1u) {{
            auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_ops_count");
            ops_count[idx].exchange(1u);
        }} else if (group_by == 2u) {{
            auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_ops_count");
            ops_count[idx].exchange(1u);
            
            // DEBUG для агента 100006
            if (step_day == 824u && idx == 285u) {{
                printf("[DEBUG Day %u COUNT_OPS] Agent idx=%u SET in ops_count!\\n", step_day, idx);
            }}
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    rtc_func = agent.newRTCFunction("rtc_count_ops", RTC_COUNT_OPS)
    rtc_func.setInitialState("operations")
    rtc_func.setEndState("operations")
    
    layer_count = model.newLayer("count_ops")
    layer_count.addAgentFunction(rtc_func)
    
    # =========================================================================
    # Слой 3: Подсчёт агентов в serviceable
    # =========================================================================
    RTC_COUNT_SVC = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (group_by == 1u) {{
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_svc_count");
        svc_count[idx].exchange(1u);
    }} else if (group_by == 2u) {{
        auto svc_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_svc_count");
        svc_count[idx].exchange(1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    rtc_func_svc = agent.newRTCFunction("rtc_count_serviceable", RTC_COUNT_SVC)
    rtc_func_svc.setInitialState("serviceable")
    rtc_func_svc.setEndState("serviceable")
    
    layer_count_svc = model.newLayer("count_serviceable")
    layer_count_svc.addAgentFunction(rtc_func_svc)
    
    # =========================================================================
    # Слой 4: Подсчёт агентов в reserve
    # =========================================================================
    RTC_COUNT_RESERVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Записываем флаг что этот агент в reserve
    if (group_by == 1u) {{
        auto reserve_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_reserve_count");
        reserve_count[idx].exchange(1u);
    }} else if (group_by == 2u) {{
        auto reserve_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_reserve_count");
        reserve_count[idx].exchange(1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    rtc_func_res = agent.newRTCFunction("rtc_count_reserve", RTC_COUNT_RESERVE)
    rtc_func_res.setInitialState("reserve")
    rtc_func_res.setEndState("reserve")
    
    layer_count_res = model.newLayer("count_reserve")
    layer_count_res.addAgentFunction(rtc_func_res)
    
    # =========================================================================
    # Слой 5: Подсчёт агентов в inactive
    # =========================================================================
    RTC_COUNT_INACTIVE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_inactive, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Записываем флаг что этот агент в inactive
    if (group_by == 1u) {{
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_inactive_count");
        inactive_count[idx].exchange(1u);
    }} else if (group_by == 2u) {{
        auto inactive_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_inactive_count");
        inactive_count[idx].exchange(1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    rtc_func_ina = agent.newRTCFunction("rtc_count_inactive", RTC_COUNT_INACTIVE)
    rtc_func_ina.setInitialState("inactive")
    rtc_func_ina.setEndState("inactive")
    
    layer_count_ina = model.newLayer("count_inactive")
    layer_count_ina.addAgentFunction(rtc_func_ina)
    
    # =========================================================================
    # Слой 6: Подсчёт агентов в repair (для квотирования ремонтов)
    # =========================================================================
    RTC_COUNT_REPAIR = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Записываем флаг что этот агент в repair
    auto repair_state_buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("repair_state_buffer");
    repair_state_buffer[idx].exchange(1u);
    
    return flamegpu::ALIVE;
}}
"""
    
    rtc_func_rep = agent.newRTCFunction("rtc_count_repair", RTC_COUNT_REPAIR)
    rtc_func_rep.setInitialState("repair")
    rtc_func_rep.setEndState("repair")
    
    layer_count_rep = model.newLayer("count_repair")
    layer_count_rep.addAgentFunction(rtc_func_rep)
    
    # =========================================================================
    # Слой 7: Подсчёт кандидатов в очереди на ремонт (reserve & intent=0)
    # =========================================================================
    RTC_COUNT_RESERVE_QUEUE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_reserve_queue, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Только агенты с intent=0 (в очереди на ремонт)
    if (intent == 0u) {{
        auto reserve_queue_buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("reserve_queue_buffer");
        reserve_queue_buffer[idx].exchange(1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    rtc_func_rq = agent.newRTCFunction("rtc_count_reserve_queue", RTC_COUNT_RESERVE_QUEUE)
    rtc_func_rq.setInitialState("reserve")
    rtc_func_rq.setEndState("reserve")
    
    layer_count_rq = model.newLayer("count_reserve_queue")
    layer_count_rq.addAgentFunction(rtc_func_rq)
    
    # =========================================================================
    # Слой 8: Подсчёт запросов на ремонт (operations & intent=4)
    # =========================================================================
    RTC_COUNT_OPS_REPAIR = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_count_ops_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Только агенты с intent=4 (запрос на ремонт)
    if (intent == 4u) {{
        auto ops_repair_buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("ops_repair_buffer");
        ops_repair_buffer[idx].exchange(1u);
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    rtc_func_or = agent.newRTCFunction("rtc_count_ops_repair", RTC_COUNT_OPS_REPAIR)
    rtc_func_or.setInitialState("operations")
    rtc_func_or.setEndState("operations")
    
    layer_count_or = model.newLayer("count_ops_repair")
    layer_count_or.addAgentFunction(rtc_func_or)
    
    # =========================================================================
    # Слой 9: Логирование MP4 целевых значений в MacroProperty для экспорта
    # =========================================================================
    # Создаем буферы для хранения целевых значений по дням
    model.Environment().newMacroPropertyUInt32("mp2_mp4_target_mi8", max_days + 1)
    model.Environment().newMacroPropertyUInt32("mp2_mp4_target_mi17", max_days + 1)
    
    # =========================================================================
    # Логирование баланса (gap) по типам (per-day агрегированный показатель)
    # =========================================================================
    model.Environment().newMacroPropertyInt32("mp2_quota_gap_mi8", max_days + 1)
    model.Environment().newMacroPropertyInt32("mp2_quota_gap_mi17", max_days + 1)

    # ✅ УСТАРЕЛО: RTC функция для логирования MP4 целей больше не используется
    # Целевые значения теперь читаются напрямую из mp4_ops_counter на стороне Python
    # при дренаже в базу (см. mp2_drain_host.py::_get_mp4_target)
    # 
    # Причина: RTC функция вызывается только для существующих агентов, что приводило
    # к пропускам дней когда все агенты определённого типа меняли состояние.
    RTC_LOG_MP4_TARGETS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_log_mp4_targets, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // ⚠️ DEPRECATED: Эта функция больше не используется для записи mp2_mp4_target_*
    // Целевые значения читаются напрямую из mp4_ops_counter при дренаже
    return flamegpu::ALIVE;
}}
"""
    
    # =========================================================================
    # Слой 7: Логирование gap (баланс = curr - target) по типам
    # =========================================================================
    RTC_LOG_GAP = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_log_quota_gap, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = ((day + 1u) < days_total ? (day + 1u) : (days_total > 0u ? days_total - 1u : 0u));
    
    if (group_by == 1u) {{
        // Подсчитываем curr (агентов в operations)
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi8_ops_count");
        unsigned int curr = 0u;
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ops_count[i] == 1u) ++curr;
        }}
        
        // Читаем target
        unsigned int target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
        
        // Расчитываем gap = curr - target
        int gap = (int)curr - (int)target;
        
        // Логируем gap в MacroProperty (будет затерт на следующей итерации, но это нормально)
        auto mp2_gap = FLAMEGPU->environment.getMacroProperty<int, {max_days + 1}u>("mp2_quota_gap_mi8");
        mp2_gap[day].exchange(gap);
        
    }} else if (group_by == 2u) {{
        // То же для Mi-17
        auto ops_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {max_frames}u>("mi17_ops_count");
        unsigned int curr = 0u;
        for (unsigned int i = 0u; i < frames; ++i) {{
            if (ops_count[i] == 1u) ++curr;
        }}
        
        unsigned int target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
        int gap = (int)curr - (int)target;
        
        auto mp2_gap = FLAMEGPU->environment.getMacroProperty<int, {max_days + 1}u>("mp2_quota_gap_mi17");
        mp2_gap[day].exchange(gap);
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    rtc_log_mp4 = agent.newRTCFunction("rtc_log_mp4_targets", RTC_LOG_MP4_TARGETS)
    # Запускаем для ВСЕХ агентов (exchange атомарна, поэтому безопасна многократная запись)
    
    layer_log_mp4 = model.newLayer("log_mp4_targets")
    layer_log_mp4.addAgentFunction(rtc_log_mp4)
    
    # =========================================================================
    # Регистрация слоя для логирования gap
    # =========================================================================
    rtc_log_gap = agent.newRTCFunction("rtc_log_quota_gap", RTC_LOG_GAP)
    
    layer_log_gap = model.newLayer("log_quota_gap")
    layer_log_gap.addAgentFunction(rtc_log_gap)
    
    print("  RTC модуль count_ops зарегистрирован (обнуление + подсчёт ops/svc/reserve/inactive/repair/queue + логирование MP4 целей + gap)")


