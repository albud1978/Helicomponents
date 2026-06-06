#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (sim_v2 dead-code cleanup): не используется активным V8-ядром (orchestrator_limiter_v8). Внутренние импорты заморожены.
"""
RTC модуль: Event-driven QuotaManager (ГИБРИДНЫЙ подход)

Архитектура:
1. QuotaManager хранит curr_ops и обновляет при событиях
2. Читает target из mp4_ops_counter[day]
3. При DEMOUNT → curr_ops--, deficit++ → промоут из ready пулов
4. При balance < 0 (curr > target) → демоут oldest из operations
5. Агенты в serviceable/reserve/inactive шлют READY при изменении intent
6. Агенты в operations шлют:
   - DEMOUNT при выходе (intent != 2)
   - OPS_REPORT для учёта (mfg_date для ранжирования)

Типы событий:
- EVENT_DEMOUNT (1): агент выходит из operations
- EVENT_READY (2): агент готов к промоуту
- EVENT_OPS_REPORT (3): агент остаётся в operations (для подсчёта и ранжирования)
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
import model_build

import pyflamegpu as fg


MAX_FRAMES = model_build.RTC_MAX_FRAMES
MAX_DAYS = model_build.MAX_DAYS


def register_rtc(model: fg.ModelDescription, quota_agent: fg.AgentDescription):
    """Регистрирует event-driven QuotaManager (гибридный)"""
    
    print("  🎯 Регистрация модуля: quota_manager_event (HYBRID EVENT-DRIVEN)")
    
    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция QuotaManager: гибридный event-driven
    # ═══════════════════════════════════════════════════════════════════════
    
    RTC_QUOTA_MANAGER_EVENT = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_manager_event, flamegpu::MessageBruteForce, flamegpu::MessageBruteForce) {{
    const unsigned char my_group = FLAMEGPU->getVariable<unsigned char>("group_by");
    const unsigned int step = FLAMEGPU->getStepCounter();
    
    // Читаем текущее curr_ops из переменной агента
    unsigned int curr_ops = FLAMEGPU->getVariable<unsigned int>("current");
    
    // Читаем target из mp4_ops_counter_mi8 или mp4_ops_counter_mi17
    unsigned int target;
    if (my_group == 1u) {{
        target = FLAMEGPU->environment.getProperty<unsigned int, {MAX_DAYS}u>("mp4_ops_counter_mi8", step);
    }} else {{
        target = FLAMEGPU->environment.getProperty<unsigned int, {MAX_DAYS}u>("mp4_ops_counter_mi17", step);
    }}
    
    // ═══════════════════════════════════════════════════════════════════
    // Шаг 1: Читаем события и собираем данные
    // ═══════════════════════════════════════════════════════════════════
    
    unsigned int demount_count = 0;  // Сколько агентов вышло из operations
    unsigned int ops_count = 0;      // Текущее количество в operations (из сообщений)
    unsigned int ready_serviceable = 0;
    unsigned int ready_reserve = 0;
    unsigned int ready_inactive = 0;
    
    // Буферы для данных агентов (для ранжирования)
    // MAX_EVENTS должен покрывать максимальное количество агентов в одной группе (~163 Mi-8, ~116 Mi-17)
    const unsigned int MAX_EVENTS = 200;
    unsigned short ops_idx[MAX_EVENTS];   // idx агентов в operations (для демоута)
    unsigned short ops_mfg[MAX_EVENTS];   // mfg_date (для сортировки)
    unsigned short svc_idx[MAX_EVENTS];
    unsigned short svc_mfg[MAX_EVENTS];
    unsigned short rsv_idx[MAX_EVENTS];
    unsigned short rsv_mfg[MAX_EVENTS];
    unsigned short ina_idx[MAX_EVENTS];
    unsigned short ina_mfg[MAX_EVENTS];
    
    // Читаем все события
    for (const auto& msg : FLAMEGPU->message_in) {{
        const unsigned char msg_group = msg.getVariable<unsigned char>("group_by");
        if (msg_group != my_group) continue;  // Фильтр по типу ВС
        
        const unsigned char event_type = msg.getVariable<unsigned char>("event_type");
        const unsigned char state = msg.getVariable<unsigned char>("state");
        const unsigned short idx = msg.getVariable<unsigned short>("idx");
        const unsigned short mfg = msg.getVariable<unsigned short>("mfg_date");
        
        if (event_type == 1u) {{
            // DEMOUNT: агент выходит из operations
            demount_count++;
        }}
        else if (event_type == 2u) {{
            // READY: агент готов к промоуту
            if (state == 3u && ready_serviceable < MAX_EVENTS) {{
                svc_idx[ready_serviceable] = idx;
                svc_mfg[ready_serviceable] = mfg;
                ready_serviceable++;
            }}
            else if (state == 5u && ready_reserve < MAX_EVENTS) {{
                rsv_idx[ready_reserve] = idx;
                rsv_mfg[ready_reserve] = mfg;
                ready_reserve++;
            }}
            else if (state == 1u && ready_inactive < MAX_EVENTS) {{
                ina_idx[ready_inactive] = idx;
                ina_mfg[ready_inactive] = mfg;
                ready_inactive++;
            }}
        }}
        else if (event_type == 3u) {{
            // OPS_REPORT: агент остаётся в operations
            if (ops_count < MAX_EVENTS) {{
                ops_idx[ops_count] = idx;
                ops_mfg[ops_count] = mfg;
                ops_count++;
            }}
        }}
    }}
    
    // ═══════════════════════════════════════════════════════════════════
    // Шаг 2: Обновляем curr_ops
    // ═══════════════════════════════════════════════════════════════════
    
    // Если есть OPS_REPORT → используем точное количество
    if (ops_count > 0) {{
        curr_ops = ops_count;
    }}
    // Иначе корректируем на основе событий
    else if (demount_count > 0) {{
        if (curr_ops >= demount_count) {{
            curr_ops -= demount_count;
        }} else {{
            curr_ops = 0;
        }}
    }}
    
    // ═══════════════════════════════════════════════════════════════════
    // Шаг 3: Расчёт баланса и действий
    // ═══════════════════════════════════════════════════════════════════
    
    int balance = (int)curr_ops - (int)target;
    unsigned int promoted = 0;
    unsigned int demoted = 0;
    
    // Если balance > 0 (curr > target) → нужен ДЕМОУТ
    if (balance > 0 && ops_count > 0) {{
        // Сортируем operations по mfg_date (descending = oldest first для демоута)
        for (unsigned int i = 0; i < ops_count && i < MAX_EVENTS - 1; i++) {{
            for (unsigned int j = i + 1; j < ops_count && j < MAX_EVENTS; j++) {{
                if (ops_mfg[j] > ops_mfg[i]) {{  // Older first (larger mfg = newer, so swap if j > i)
                    unsigned short tmp_idx = ops_idx[i]; ops_idx[i] = ops_idx[j]; ops_idx[j] = tmp_idx;
                    unsigned short tmp_mfg = ops_mfg[i]; ops_mfg[i] = ops_mfg[j]; ops_mfg[j] = tmp_mfg;
                }}
            }}
        }}
        
        // Демоутим K oldest
        unsigned int K = (unsigned int)balance;
        for (unsigned int i = 0; i < K && i < ops_count; i++) {{
            FLAMEGPU->message_out.setVariable<unsigned short>("idx", ops_idx[i]);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 1u);  // DEMOTE
            FLAMEGPU->message_out.setVariable<unsigned char>("group_by", my_group);
            demoted++;
            curr_ops--;  // Обновляем счётчик
        }}
    }}
    
    // Если balance < 0 (curr < target) → нужен ПРОМОУТ
    if (balance < 0) {{
        unsigned int deficit = (unsigned int)(-balance);
        
        // Добавляем demount_count к deficit (замена выбывших)
        deficit += demount_count;
        
        // P1: Промоут из serviceable (youngest first = smallest mfg_date)
        for (unsigned int i = 0; i < ready_serviceable && i < MAX_EVENTS - 1; i++) {{
            for (unsigned int j = i + 1; j < ready_serviceable && j < MAX_EVENTS; j++) {{
                if (svc_mfg[j] < svc_mfg[i]) {{
                    unsigned short tmp_idx = svc_idx[i]; svc_idx[i] = svc_idx[j]; svc_idx[j] = tmp_idx;
                    unsigned short tmp_mfg = svc_mfg[i]; svc_mfg[i] = svc_mfg[j]; svc_mfg[j] = tmp_mfg;
                }}
            }}
        }}
        
        for (unsigned int i = 0; i < ready_serviceable && deficit > 0; i++) {{
            FLAMEGPU->message_out.setVariable<unsigned short>("idx", svc_idx[i]);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 2u);  // PROMOTE
            FLAMEGPU->message_out.setVariable<unsigned char>("group_by", my_group);
            deficit--;
            promoted++;
            curr_ops++;
        }}
        
        // P2: Промоут из reserve
        for (unsigned int i = 0; i < ready_reserve && i < MAX_EVENTS - 1; i++) {{
            for (unsigned int j = i + 1; j < ready_reserve && j < MAX_EVENTS; j++) {{
                if (rsv_mfg[j] < rsv_mfg[i]) {{
                    unsigned short tmp_idx = rsv_idx[i]; rsv_idx[i] = rsv_idx[j]; rsv_idx[j] = tmp_idx;
                    unsigned short tmp_mfg = rsv_mfg[i]; rsv_mfg[i] = rsv_mfg[j]; rsv_mfg[j] = tmp_mfg;
                }}
            }}
        }}
        
        for (unsigned int i = 0; i < ready_reserve && deficit > 0; i++) {{
            FLAMEGPU->message_out.setVariable<unsigned short>("idx", rsv_idx[i]);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 2u);  // PROMOTE
            FLAMEGPU->message_out.setVariable<unsigned char>("group_by", my_group);
            deficit--;
            promoted++;
            curr_ops++;
        }}
        
        // P3: Промоут из inactive
        for (unsigned int i = 0; i < ready_inactive && i < MAX_EVENTS - 1; i++) {{
            for (unsigned int j = i + 1; j < ready_inactive && j < MAX_EVENTS; j++) {{
                if (ina_mfg[j] < ina_mfg[i]) {{
                    unsigned short tmp_idx = ina_idx[i]; ina_idx[i] = ina_idx[j]; ina_idx[j] = tmp_idx;
                    unsigned short tmp_mfg = ina_mfg[i]; ina_mfg[i] = ina_mfg[j]; ina_mfg[j] = tmp_mfg;
                }}
            }}
        }}
        
        for (unsigned int i = 0; i < ready_inactive && deficit > 0; i++) {{
            FLAMEGPU->message_out.setVariable<unsigned short>("idx", ina_idx[i]);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 2u);  // PROMOTE
            FLAMEGPU->message_out.setVariable<unsigned char>("group_by", my_group);
            deficit--;
            promoted++;
            curr_ops++;
        }}
    }}
    // Если balance == 0 но есть demount → просто замена (промоут)
    else if (balance == 0 && demount_count > 0) {{
        unsigned int deficit = demount_count;
        
        // P1: serviceable
        for (unsigned int i = 0; i < ready_serviceable && deficit > 0; i++) {{
            FLAMEGPU->message_out.setVariable<unsigned short>("idx", svc_idx[i]);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 2u);
            FLAMEGPU->message_out.setVariable<unsigned char>("group_by", my_group);
            deficit--;
            promoted++;
        }}
        
        // P2: reserve
        for (unsigned int i = 0; i < ready_reserve && deficit > 0; i++) {{
            FLAMEGPU->message_out.setVariable<unsigned short>("idx", rsv_idx[i]);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 2u);
            FLAMEGPU->message_out.setVariable<unsigned char>("group_by", my_group);
            deficit--;
            promoted++;
        }}
        
        // P3: inactive
        for (unsigned int i = 0; i < ready_inactive && deficit > 0; i++) {{
            FLAMEGPU->message_out.setVariable<unsigned short>("idx", ina_idx[i]);
            FLAMEGPU->message_out.setVariable<unsigned char>("action", 2u);
            FLAMEGPU->message_out.setVariable<unsigned char>("group_by", my_group);
            deficit--;
            promoted++;
        }}
    }}
    
    // ═══════════════════════════════════════════════════════════════════
    // Шаг 4: Сохраняем обновлённый curr_ops
    // ═══════════════════════════════════════════════════════════════════
    
    FLAMEGPU->setVariable<unsigned int>("current", curr_ops);
    FLAMEGPU->setVariable<int>("balance", balance);
    
    // Логирование (каждые 10 шагов или при действиях)
    if (step % 10 == 0 || demoted > 0 || promoted > 0) {{
        printf("[QM Day %u] group=%u: target=%u, curr=%u, ops_count=%u, balance=%d, demount=%u, ready(S/R/I)=%u/%u/%u, demoted=%u, promoted=%u\\n",
               step, my_group, target, curr_ops, ops_count, balance, demount_count, 
               ready_serviceable, ready_reserve, ready_inactive, demoted, promoted);
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    # Регистрируем функцию
    layer = model.newLayer("quota_manager_event")
    rtc_func = quota_agent.newRTCFunction("rtc_quota_manager_event", RTC_QUOTA_MANAGER_EVENT)
    rtc_func.setMessageInput("PlanerEvent")
    rtc_func.setMessageOutput("QuotaDecision")
    rtc_func.setMessageOutputOptional(True)
    layer.addAgentFunction(rtc_func)
    
    print("    ✅ QuotaManager HYBRID event-driven зарегистрирован")
