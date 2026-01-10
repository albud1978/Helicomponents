#!/usr/bin/env python3
"""
RTC модуль: QuotaManager — централизованное квотирование

QuotaManager (2 агента: Mi-8, Mi-17):
1. Читает все PlanerReport сообщения своей группы
2. Подсчитывает curr (operations+intent=2), ready_serviceable, ready_reserve, ready_inactive
3. Вычисляет balance = curr - target
4. Выполняет демоут (если balance > 0) или каскадный промоут (если balance < 0)
5. Публикует QuotaDecision сообщения

Ранжирование:
- Демоут: oldest first (меньший idx = старше)
- Промоут: youngest first (больший idx = моложе)

ВАЖНО: Используем локальные массивы вместо MacroProperty т.к. FLAME GPU
запрещает смешивать read и atomic write в одном слое.
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
import model_build

import pyflamegpu as fg


def register_rtc(model: fg.ModelDescription, quota_agent: fg.AgentDescription):
    """Регистрирует RTC функцию QuotaManager"""
    
    print("  🎯 Регистрация модуля: quota_manager (централизованное квотирование)")
    
    max_frames = model_build.RTC_MAX_FRAMES
    
    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция QuotaManager
    # Используем ЛОКАЛЬНЫЕ массивы вместо MacroProperty
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_QUOTA_MANAGER = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_manager, flamegpu::MessageBruteForce, flamegpu::MessageBruteForce) {{
    const unsigned char my_group = (unsigned char)FLAMEGPU->getVariable<unsigned char>("group_by");
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = ((step_day + 1u) < days_total) ? (step_day + 1u) : (days_total > 0u ? days_total - 1u : 0u);
    
    // Получаем target из MP4 (на D+1)
    unsigned int target = 0u;
    if (my_group == 1u) {{
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", safe_day);
    }} else if (my_group == 2u) {{
        target = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", safe_day);
    }}
    
    // ═══════════════════════════════════════════════════════════════════
    // ШАГ 1: Сбор данных из PlanerReport сообщений
    // ═══════════════════════════════════════════════════════════════════
    // ЛОКАЛЬНЫЕ массивы (до 400 агентов — помещается в локальную память GPU)
    unsigned short ops_idx[{max_frames}u];
    unsigned short svc_idx[{max_frames}u];
    unsigned short rsv_idx[{max_frames}u];
    unsigned short ina_idx[{max_frames}u];
    
    // Счётчики
    unsigned int ops_count = 0u;
    unsigned int svc_count = 0u;
    unsigned int rsv_count = 0u;
    unsigned int ina_count = 0u;
    
    // Читаем все сообщения
    for (const auto& msg : FLAMEGPU->message_in) {{
        const unsigned char msg_group = msg.getVariable<unsigned char>("group_by");
        
        // Фильтруем по своей группе
        if (msg_group != my_group) continue;
        
        const unsigned short msg_idx = msg.getVariable<unsigned short>("idx");
        const unsigned char msg_state = msg.getVariable<unsigned char>("state");
        const unsigned char msg_intent = msg.getVariable<unsigned char>("intent");
        const unsigned char msg_repair_ready = msg.getVariable<unsigned char>("repair_ready");
        const unsigned char msg_skip_repair = msg.getVariable<unsigned char>("skip_repair");
        
        // Классификация агентов
        if (msg_state == 2u && msg_intent == 2u) {{
            // operations + intent=2 → кандидат на демоут
            if (ops_count < {max_frames}u) {{
                ops_idx[ops_count] = msg_idx;
                ops_count++;
            }}
        }}
        else if (msg_state == 3u && msg_intent == 3u) {{
            // serviceable + intent=3 → кандидат на промоут P1
            if (svc_count < {max_frames}u) {{
                svc_idx[svc_count] = msg_idx;
                svc_count++;
            }}
        }}
        else if (msg_state == 5u && msg_intent == 5u) {{
            // reserve + intent=5 → кандидат на промоут P2
            if (rsv_count < {max_frames}u) {{
                rsv_idx[rsv_count] = msg_idx;
                rsv_count++;
            }}
        }}
        else if (msg_state == 1u && msg_intent == 1u) {{
            // inactive + intent=1 + ready → кандидат на промоут P3
            if (msg_repair_ready == 1u || msg_skip_repair == 1u) {{
                if (ina_count < {max_frames}u) {{
                    ina_idx[ina_count] = msg_idx;
                    ina_count++;
                }}
            }}
        }}
    }}
    
    // Сохраняем current в переменную агента
    FLAMEGPU->setVariable<unsigned short>("current", (unsigned short)ops_count);
    FLAMEGPU->setVariable<unsigned short>("target", (unsigned short)target);
    
    // ═══════════════════════════════════════════════════════════════════
    // ШАГ 2: Вычисление баланса
    // ═══════════════════════════════════════════════════════════════════
    int balance = (int)ops_count - (int)target;
    FLAMEGPU->setVariable<short>("balance", (short)balance);
    
    // ═══════════════════════════════════════════════════════════════════
    // ШАГ 3: ДЕМОУТ (если balance > 0)
    // idx уже отсортированы по mfg_date (старые первые при создании агентов)
    // Oldest first = первые K в ops_idx
    // ═══════════════════════════════════════════════════════════════════
    unsigned int decisions_count = 0u;
    
    if (balance > 0) {{
        unsigned int K = (unsigned int)balance;
        if (K > ops_count) K = ops_count;
        
        // Сортируем ops_idx по возрастанию (oldest first = меньший idx)
        // Bubble sort для небольших K
        for (unsigned int i = 0u; i < ops_count - 1u; ++i) {{
            for (unsigned int j = 0u; j < ops_count - i - 1u; ++j) {{
                if (ops_idx[j] > ops_idx[j + 1u]) {{
                    unsigned short tmp = ops_idx[j];
                    ops_idx[j] = ops_idx[j + 1u];
                    ops_idx[j + 1u] = tmp;
                }}
            }}
        }}
        
        for (unsigned int i = 0u; i < K; ++i) {{
            unsigned short dem_idx = ops_idx[i];
            
            // Публикуем решение DEMOTE
            FLAMEGPU->message_out.setVariable<unsigned short>("idx", dem_idx);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 1u);  // DEMOTE
            
            // Логирование
            if (step_day < 10u || step_day == 180u || step_day == 181u) {{
                printf("  [QM Day %u] DEMOTE idx=%u (group=%u, K=%u)\\n", 
                       step_day, (unsigned int)dem_idx, (unsigned int)my_group, K);
            }}
            decisions_count++;
        }}
        
        balance = 0;  // Демоут закрыл избыток
    }}
    
    // ═══════════════════════════════════════════════════════════════════
    // ШАГ 4: КАСКАДНЫЙ ПРОМОУТ (если balance < 0 → deficit)
    // ═══════════════════════════════════════════════════════════════════
    int deficit = (balance < 0) ? -balance : 0;
    
    // P1: serviceable (youngest first = последние K в svc_idx)
    if (deficit > 0 && svc_count > 0u) {{
        unsigned int K = (deficit < (int)svc_count) ? (unsigned int)deficit : svc_count;
        
        // Сортируем по убыванию для youngest first
        for (unsigned int i = 0u; i < svc_count - 1u; ++i) {{
            for (unsigned int j = 0u; j < svc_count - i - 1u; ++j) {{
                if (svc_idx[j] < svc_idx[j + 1u]) {{
                    unsigned short tmp = svc_idx[j];
                    svc_idx[j] = svc_idx[j + 1u];
                    svc_idx[j + 1u] = tmp;
                }}
            }}
        }}
        
        for (unsigned int i = 0u; i < K; ++i) {{
            unsigned short promo_idx = svc_idx[i];
            
            FLAMEGPU->message_out.setVariable<unsigned short>("idx", promo_idx);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 2u);  // PROMOTE
            
            if (step_day < 10u || step_day == 180u) {{
                printf("  [QM Day %u] PROMOTE P1 idx=%u (group=%u, deficit=%d)\\n", 
                       step_day, (unsigned int)promo_idx, (unsigned int)my_group, deficit);
            }}
            decisions_count++;
        }}
        
        deficit -= K;
    }}
    
    // P2: reserve (youngest first)
    if (deficit > 0 && rsv_count > 0u) {{
        unsigned int K = (deficit < (int)rsv_count) ? (unsigned int)deficit : rsv_count;
        
        // Сортируем по убыванию
        for (unsigned int i = 0u; i < rsv_count - 1u; ++i) {{
            for (unsigned int j = 0u; j < rsv_count - i - 1u; ++j) {{
                if (rsv_idx[j] < rsv_idx[j + 1u]) {{
                    unsigned short tmp = rsv_idx[j];
                    rsv_idx[j] = rsv_idx[j + 1u];
                    rsv_idx[j + 1u] = tmp;
                }}
            }}
        }}
        
        for (unsigned int i = 0u; i < K; ++i) {{
            unsigned short promo_idx = rsv_idx[i];
            
            FLAMEGPU->message_out.setVariable<unsigned short>("idx", promo_idx);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 2u);
            
            if (step_day < 10u || step_day == 180u) {{
                printf("  [QM Day %u] PROMOTE P2 idx=%u (group=%u, deficit=%d)\\n", 
                       step_day, (unsigned int)promo_idx, (unsigned int)my_group, deficit);
            }}
            decisions_count++;
        }}
        
        deficit -= K;
    }}
    
    // P3: inactive (youngest first)
    if (deficit > 0 && ina_count > 0u) {{
        unsigned int K = (deficit < (int)ina_count) ? (unsigned int)deficit : ina_count;
        
        // Сортируем по убыванию
        for (unsigned int i = 0u; i < ina_count - 1u; ++i) {{
            for (unsigned int j = 0u; j < ina_count - i - 1u; ++j) {{
                if (ina_idx[j] < ina_idx[j + 1u]) {{
                    unsigned short tmp = ina_idx[j];
                    ina_idx[j] = ina_idx[j + 1u];
                    ina_idx[j + 1u] = tmp;
                }}
            }}
        }}
        
        for (unsigned int i = 0u; i < K; ++i) {{
            unsigned short promo_idx = ina_idx[i];
            
            FLAMEGPU->message_out.setVariable<unsigned short>("idx", promo_idx);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 2u);
            
            if (step_day < 10u || step_day == 180u) {{
                printf("  [QM Day %u] PROMOTE P3 idx=%u (group=%u, deficit=%d)\\n", 
                       step_day, (unsigned int)promo_idx, (unsigned int)my_group, deficit);
            }}
            decisions_count++;
        }}
        
        deficit -= K;
    }}
    
    // Сохраняем остаток deficit (для spawn_dynamic)
    FLAMEGPU->setVariable<unsigned short>("remaining_deficit", (unsigned short)((deficit > 0) ? deficit : 0));
    
    return flamegpu::ALIVE;
}}
"""
    
    # Регистрация слоя
    layer = model.newLayer("quota_manager")
    rtc_func = quota_agent.newRTCFunction("rtc_quota_manager", RTC_QUOTA_MANAGER)
    rtc_func.setMessageInput("PlanerReport")
    rtc_func.setMessageOutput("QuotaDecision")
    layer.addAgentFunction(rtc_func)
    
    print(f"    ✅ QuotaManager зарегистрирован (локальные массивы {max_frames})")
