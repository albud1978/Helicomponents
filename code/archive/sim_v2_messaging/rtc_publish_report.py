#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (sim_v2 dead-code cleanup): не используется активным V8-ядром (orchestrator_limiter_v8). Внутренние импорты заморожены.
"""
RTC модуль: Публикация PlanerReport сообщений

Каждый планер публикует своё состояние для QuotaManager:
- idx, group_by, state, intent, mfg_date
- repair_ready: 1 если step_day >= repair_time
- skip_repair: 1 если Mi-17 && ppr < br2
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
import model_build

import pyflamegpu as fg


def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC функции публикации PlanerReport для всех состояний"""
    
    print("  📤 Регистрация модуля: publish_report (PlanerReport сообщения)")
    
    # ═══════════════════════════════════════════════════════════════════════
    # RTC функция публикации — одна для всех состояний
    # Привязка к конкретному state через setInitialState/setEndState
    # ═══════════════════════════════════════════════════════════════════════
    
    # Создаём отдельную RTC функцию для каждого состояния с хардкодом state
    # Это нужно потому что status_id переменная не синхронизирована с FLAME GPU state
    def get_rtc_publish_report(state_code: int) -> str:
        return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_publish_report, flamegpu::MessageNone, flamegpu::MessageBruteForce) {{
    // Получаем данные агента
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned int mfg_date = FLAMEGPU->getVariable<unsigned int>("mfg_date");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    const unsigned int repair_line_id = FLAMEGPU->getVariable<unsigned int>("repair_line_id");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    
    // State хардкодится для каждой функции (определён из FLAME GPU state machine)
    const unsigned char state = {state_code}u;
    
    // Вычисляем repair_ready
    const unsigned char repair_ready = (day >= repair_time) ? 1u : 0u;
    
    // Вычисляем skip_repair (для Mi-17 с ppr < br2)
    const unsigned int br2_mi17 = FLAMEGPU->environment.getProperty<unsigned int>("mi17_br2_const");
    const unsigned char skip_repair = (group_by == 2u && ppr < br2_mi17) ? 1u : 0u;
    
    // Публикуем сообщение
    FLAMEGPU->message_out.setVariable<unsigned short>("idx", (unsigned short)idx);
    FLAMEGPU->message_out.setVariable<unsigned char>("group_by", (unsigned char)group_by);
    FLAMEGPU->message_out.setVariable<unsigned char>("state", state);
    FLAMEGPU->message_out.setVariable<unsigned char>("intent", (unsigned char)intent);
    FLAMEGPU->message_out.setVariable<unsigned short>("mfg_date", (unsigned short)mfg_date);
    FLAMEGPU->message_out.setVariable<unsigned char>("repair_ready", repair_ready);
    FLAMEGPU->message_out.setVariable<unsigned char>("skip_repair", skip_repair);
    FLAMEGPU->message_out.setVariable<unsigned int>("repair_days", repair_days);
    FLAMEGPU->message_out.setVariable<unsigned int>("repair_line_id", repair_line_id);
    
    return flamegpu::ALIVE;
}}
"""
    
    # Регистрируем для каждого состояния (чтобы охватить всех агентов)
    # ВАЖНО: FLAME GPU не позволяет нескольким функциям в одном слое
    # писать в один и тот же MessageList. Используем отдельные слои.
    # Маппинг состояний на числовые коды
    state_codes = {
        "inactive": 1,
        "operations": 2,
        "serviceable": 3,
        "repair": 4,
        "reserve": 5,
        "unserviceable": 7
    }
    
    for state_name, state_code in state_codes.items():
        layer = model.newLayer(f"publish_report_{state_name}")
        func_name = f"rtc_publish_report_{state_name}"
        # Генерируем RTC код с хардкодом state для этого конкретного состояния
        rtc_code = get_rtc_publish_report(state_code)
        rtc_func = agent.newRTCFunction(func_name, rtc_code)
        rtc_func.setInitialState(state_name)
        rtc_func.setEndState(state_name)
        rtc_func.setMessageOutput("PlanerReport")
        layer.addAgentFunction(rtc_func)
    
    print(f"    ✅ Зарегистрировано {len(state_codes)} функций публикации (каждая в своём слое)")

