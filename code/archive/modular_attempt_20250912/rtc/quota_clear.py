#!/usr/bin/env python3
"""
RTC функция: rtc_quota_clear
Очистка intent буферов после распределения квоты
Универсальная для всех статусов
Основано на коде из бэкапов
Дата: 2025-09-12
"""

from rtc import BaseRTC


class QuotaClearRTC(BaseRTC):
    """RTC функция для очистки intent буферов"""
    
    NAME = "rtc_quota_clear"
    DEPENDENCIES = ["rtc_quota_apply"]
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код rtc_quota_clear (универсальный)"""
        
        status_id = kwargs.get('status_id', 2)
        func_name = f"rtc_quota_clear_s{status_id}"
        
        return f"""
FLAMEGPU_AGENT_FUNCTION({func_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    
    static const unsigned int FRAMES = {frames}u;
    
    // Только менеджер (idx==0) выполняет очистку
    if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
    
    // Очищаем intent буферы
    auto i8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
    auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent");
    
    for (unsigned int k=0u; k<FRAMES; ++k) {{
        i8[k].exchange(0u);
        i17[k].exchange(0u);
    }}
    
    return flamegpu::ALIVE;
}}
        """
    
    @staticmethod
    def get_source_for_status(status_id: int, frames: int, days: int) -> str:
        """Удобный метод для генерации кода под конкретный статус"""
        return QuotaClearRTC.get_source(frames, days, status_id=status_id)


