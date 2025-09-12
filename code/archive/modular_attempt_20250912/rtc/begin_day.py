#!/usr/bin/env python3
"""
RTC функция: rtc_prepare_day (объединенная)
Подготовка данных на начало суток: сброс флагов + чтение MP5
Объединяет rtc_quota_begin_day + rtc_probe_mp5
Основано на коде из бэкапов
Дата: 2025-09-12
"""

from rtc import BaseRTC


class PrepareDayRTC(BaseRTC):
    """RTC функция для подготовки данных на начало суток"""
    
    NAME = "rtc_prepare_day"
    DEPENDENCIES = []
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код rtc_quota_begin_day"""
        
        return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_prepare_day, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
    if (phase != 0u) return flamegpu::ALIVE;
    
    static const unsigned int FRAMES = {frames}u;
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    if (idx >= FRAMES) return flamegpu::ALIVE;
    
    // === ЧАСТЬ 1: Сброс операционных флагов (rtc_quota_begin_day) ===
    
    // Сбрасываем билет допуска на новый цикл и флаг intent диагностики
    FLAMEGPU->setVariable<unsigned int>("ops_ticket", 0u);
    FLAMEGPU->setVariable<unsigned int>("intent_flag", 0u);
    
    // Однодневные значения событий — обнуляем на начало суток
    // НЕ трогаем active_trigger: он должен дожить до логгера и постпроцессинга MP2
    FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 0u);
    FLAMEGPU->setVariable<unsigned int>("partout_trigger", 0u);
    
    // === ЧАСТЬ 2: Чтение налетов из MP5 (rtc_probe_mp5) ===
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
    
    // Индексация MP5: base = day * FRAMES + idx (с паддингом D+1)
    const unsigned int base = safe_day * FRAMES + idx;
    const unsigned int base_next = (safe_day + 1u) * FRAMES + idx;
    
    // Чтение налетов (тип unsigned short в MP5)
    const unsigned int dt = FLAMEGPU->environment.getProperty<unsigned short>("mp5_daily_hours", base);
    const unsigned int dn = FLAMEGPU->environment.getProperty<unsigned short>("mp5_daily_hours", base_next);
    
    // Записываем в агентные переменные
    FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
    FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
    
    return flamegpu::ALIVE;
}}
        """
