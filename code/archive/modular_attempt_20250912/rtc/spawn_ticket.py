#!/usr/bin/env python3
"""
RTC функция: rtc_spawn_ticket
Создание новых агентов MI-17 через тикеты
Основано на архитектуре из GPUarc.md
Дата: 2025-09-12
"""

from rtc import BaseRTC


class SpawnTicketRTC(BaseRTC):
    """RTC функция для создания новых агентов через тикеты"""
    
    NAME = "rtc_spawn_ticket"
    DEPENDENCIES = ["rtc_spawn_mgr"]
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код rtc_spawn_ticket"""
        
        return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_ticket, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    
    // Каждый тикет может создать максимум одного агента
    const unsigned int ticket = FLAMEGPU->getVariable<unsigned int>("ticket");
    
    // Читаем параметры спавна от менеджера
    auto spawn_need = FLAMEGPU->environment.getMacroProperty<unsigned int>("spawn_need_u32");
    const unsigned int need = spawn_need[0];
    
    // Проверяем, нужно ли создавать агента для этого тикета
    if (ticket >= need) return flamegpu::ALIVE;
    
    // Читаем базовые параметры
    auto spawn_base_idx = FLAMEGPU->environment.getMacroProperty<unsigned int>("spawn_base_idx_u32");
    auto spawn_base_acn = FLAMEGPU->environment.getMacroProperty<unsigned int>("spawn_base_acn_u32");
    auto spawn_base_psn = FLAMEGPU->environment.getMacroProperty<unsigned int>("spawn_base_psn_u32");
    
    const unsigned int base_idx = spawn_base_idx[0];
    const unsigned int base_acn = spawn_base_acn[0];
    const unsigned int base_psn = spawn_base_psn[0];
    
    // Параметры нового агента
    const unsigned int new_idx = base_idx + ticket;
    const unsigned int new_acn = base_acn + ticket;
    const unsigned int new_psn = base_psn + ticket;
    
    // Дата производства = начало текущего месяца
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int mfg_date = FLAMEGPU->environment.getProperty<unsigned int>("month_first_u32", day);
    
    // Создаем нового агента через agent_out
    auto new_agent = FLAMEGPU->agent_out.newAgent();
    
    // Инициализация полей нового агента
    new_agent.setVariableUInt("idx", new_idx);
    new_agent.setVariableUInt("psn", new_psn);
    new_agent.setVariableUInt("aircraft_number", new_acn);
    new_agent.setVariableUInt("partseqno_i", 70482u);  // MI-17 константа
    new_agent.setVariableUInt("ac_type_mask", 64u);    // MI-17 маска
    new_agent.setVariableUInt("group_by", 2u);         // MI-17 группа
    new_agent.setVariableUInt("mfg_date", mfg_date);
    new_agent.setVariableUInt("status_id", 3u);        // Начинаем в статусе "Исправен"
    
    // Начальные ресурсы из MP1 для partseqno_i=70482
    // TODO: Загрузить из MP1 по mp1_idx_mi17_spawn
    new_agent.setVariableUInt("sne", 0u);              // MP1.sne_new
    new_agent.setVariableUInt("ppr", 0u);              // MP1.ppr_new
    new_agent.setVariableUInt("ll", 0u);               // MP1.ll_mi17
    new_agent.setVariableUInt("oh", 0u);               // MP1.oh_mi17
    new_agent.setVariableUInt("br", 0u);               // MP1.br_mi17
    new_agent.setVariableUInt("repair_time", 0u);      // MP1.repair_time
    new_agent.setVariableUInt("assembly_time", 0u);    // MP1.assembly_time
    new_agent.setVariableUInt("partout_time", 0u);     // MP1.partout_time
    
    // Начальные состояния
    new_agent.setVariableUInt("repair_days", 0u);
    new_agent.setVariableUInt("ops_ticket", 0u);
    new_agent.setVariableUInt("intent_flag", 0u);
    new_agent.setVariableUInt("daily_today_u32", 0u);
    new_agent.setVariableUInt("daily_next_u32", 0u);
    new_agent.setVariableUInt("active_trigger", 0u);
    new_agent.setVariableUInt("assembly_trigger", 0u);
    new_agent.setVariableUInt("partout_trigger", 0u);
    new_agent.setVariableUInt("active_trigger_mark", 0u);
    new_agent.setVariableUInt("assembly_trigger_mark", 0u);
    new_agent.setVariableUInt("partout_trigger_mark", 0u);
    
    return flamegpu::ALIVE;
}}
        """


