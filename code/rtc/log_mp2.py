#!/usr/bin/env python3
"""
RTC функции: rtc_log_day, rtc_log_d0, rtc_mp2_copyout
Логирование состояния агентов в MP2 массивы
Основано на коде из бэкапов
Дата: 2025-09-12
"""

from rtc import BaseRTC


class LogMP2RTC(BaseRTC):
    """RTC функции для логирования в MP2"""
    
    NAME = "rtc_log_day"
    DEPENDENCIES = []
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код rtc_log_day"""
        
        func_type = kwargs.get('func_type', 'log_day')
        
        if func_type == 'log_day':
            return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_log_day, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Записываем дневной лог ТОЛЬКО в обычной фазе симуляции (export_phase==0)
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    
    static const unsigned int FRAMES = {frames}u;
    static const unsigned int DAYS = {days}u;
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    if (idx >= FRAMES) return flamegpu::ALIVE;
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int dayp1 = (day + 1u <= DAYS ? day + 1u : DAYS);
    const unsigned int row = dayp1 * FRAMES + idx;
    
    // Получаем все MP2 массивы
    auto a_status  = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_status");
    auto a_rdays   = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_repair_days");
    auto a_sne     = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_sne");
    auto a_ppr     = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_ppr");
    auto a_act     = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_active_trigger");
    auto a_asm     = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_assembly_trigger");
    auto a_part    = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_partout_trigger");
    auto a_ticket  = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_ops_ticket");
    auto a_intent  = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_intent_flag");
    auto a_dt      = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_daily_today");
    auto a_dn      = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_daily_next");
    
    // Записываем состояние агента
    a_status[row].exchange(FLAMEGPU->getVariable<unsigned int>("status_id"));
    a_rdays[row].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    a_sne[row].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    a_ppr[row].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    a_act[row].exchange(FLAMEGPU->getVariable<unsigned int>("active_trigger"));
    a_asm[row].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_trigger"));
    a_part[row].exchange(FLAMEGPU->getVariable<unsigned int>("partout_trigger"));
    a_ticket[row].exchange(FLAMEGPU->getVariable<unsigned int>("ops_ticket"));
    a_intent[row].exchange(FLAMEGPU->getVariable<unsigned int>("intent_flag"));
    a_dt[row].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    a_dn[row].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    return flamegpu::ALIVE;
}}
            """
        
        elif func_type == 'log_d0':
            return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_log_d0, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
    const unsigned int day = FLAMEGPU->getStepCounter();
    
    // Записываем D0 либо в первый день (day==0), либо при форс-логировании (phase==2)
    if (!(day == 0u || phase == 2u)) return flamegpu::ALIVE;
    
    static const unsigned int FRAMES = {frames}u;
    static const unsigned int DAYS = {days}u;
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    if (idx >= FRAMES) return flamegpu::ALIVE;
    
    const unsigned int row = 0u * FRAMES + idx; // D0 всегда в начале MP2
    
    // Получаем все MP2 массивы
    auto a_status  = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_status");
    auto a_rdays   = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_repair_days");
    auto a_sne     = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_sne");
    auto a_ppr     = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_ppr");
    auto a_act     = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_active_trigger");
    auto a_asm     = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_assembly_trigger");
    auto a_part    = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_partout_trigger");
    auto a_ticket  = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_ops_ticket");
    auto a_intent  = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_intent_flag");
    auto a_dt      = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_daily_today");
    auto a_dn      = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_daily_next");
    
    // Записываем начальное состояние (D0)
    a_status[row].exchange(FLAMEGPU->getVariable<unsigned int>("status_id"));
    a_rdays[row].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    a_sne[row].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    a_ppr[row].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    a_act[row].exchange(FLAMEGPU->getVariable<unsigned int>("active_trigger"));
    a_asm[row].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_trigger"));
    a_part[row].exchange(FLAMEGPU->getVariable<unsigned int>("partout_trigger"));
    a_ticket[row].exchange(FLAMEGPU->getVariable<unsigned int>("ops_ticket"));
    a_intent[row].exchange(FLAMEGPU->getVariable<unsigned int>("intent_flag"));
    a_dt[row].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    a_dn[row].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    return flamegpu::ALIVE;
}}
            """
        
        elif func_type == 'mp2_copyout':
            return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_copyout, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Копирование из MP2 в агентные переменные для экспорта (export_phase==1)
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 1u) return flamegpu::ALIVE;
    
    static const unsigned int FRAMES = {frames}u;
    static const unsigned int DAYS = {days}u;
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    if (idx >= FRAMES) return flamegpu::ALIVE;
    
    const unsigned int d = FLAMEGPU->environment.getProperty<unsigned int>("export_day");
    if (d > DAYS) return flamegpu::ALIVE;
    
    const unsigned int row = d * FRAMES + idx;
    
    // Получаем все MP2 массивы
    auto a_status  = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_status");
    auto a_rdays   = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_repair_days");
    auto a_sne     = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_sne");
    auto a_ppr     = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_ppr");
    auto a_act     = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_active_trigger");
    auto a_asm     = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_assembly_trigger");
    auto a_part    = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_partout_trigger");
    auto a_ticket  = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_ops_ticket");
    auto a_intent  = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*(DAYS+1))>("mp2_intent_flag");
    
    // Копируем данные из MP2 в агентные переменные
    FLAMEGPU->setVariable<unsigned int>("status_id",        a_status[row]);
    FLAMEGPU->setVariable<unsigned int>("repair_days",      a_rdays[row]);
    FLAMEGPU->setVariable<unsigned int>("sne",              a_sne[row]);
    FLAMEGPU->setVariable<unsigned int>("ppr",              a_ppr[row]);
    FLAMEGPU->setVariable<unsigned int>("active_trigger",   a_act[row]);
    FLAMEGPU->setVariable<unsigned int>("assembly_trigger", a_asm[row]);
    FLAMEGPU->setVariable<unsigned int>("partout_trigger",  a_part[row]);
    FLAMEGPU->setVariable<unsigned int>("ops_ticket",       a_ticket[row]);
    FLAMEGPU->setVariable<unsigned int>("intent_flag",      a_intent[row]);
    
    return flamegpu::ALIVE;
}}
            """
        
        else:
            return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_status_{status_id}_post, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Заглушка для статуса {status_id}
    return flamegpu::ALIVE;
}}
            """
    
    @staticmethod
    def get_log_d0_source(frames: int, days: int) -> str:
        """Специальный метод для rtc_log_d0"""
        return LogMP2RTC.get_source(frames, days, func_type='log_d0')
    
    @staticmethod
    def get_mp2_copyout_source(frames: int, days: int) -> str:
        """Специальный метод для rtc_mp2_copyout"""
        return LogMP2RTC.get_source(frames, days, func_type='mp2_copyout')


# Дополнительные классы для других log функций  
class LogD0RTC(BaseRTC):
    NAME = "rtc_log_d0"
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        return LogMP2RTC.get_source(frames, days, func_type='log_d0')

class MP2CopyoutRTC(BaseRTC):
    NAME = "rtc_mp2_copyout"
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        return LogMP2RTC.get_source(frames, days, func_type='mp2_copyout')
