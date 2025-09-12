#!/usr/bin/env python3
"""
RTC функция: rtc_quota_apply
Получение операционного билета при одобрении
Универсальная для всех статусов
Основано на коде из бэкапов
Дата: 2025-09-12
"""

from rtc import BaseRTC


class QuotaApplyRTC(BaseRTC):
    """RTC функция для получения операционного билета"""
    
    NAME = "rtc_quota_apply"
    DEPENDENCIES = ["rtc_quota_approve"]
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код rtc_quota_apply (универсальный)"""
        
        return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_quota_apply, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    
    static const unsigned int FRAMES = {frames}u;
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    if (idx >= FRAMES) return flamegpu::ALIVE;
    
    // Проверяем все approve буферы (статусы 2,3,5,1)
    if (gb == 1u) {{
        auto a8   = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve");
        auto a8b  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve_s3");
        auto a8c  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve_s5");
        auto a8d  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve_s1");
        
        if (a8[idx] || a8b[idx] || a8c[idx] || a8d[idx]) {{
            FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
        }}
    }} else if (gb == 2u) {{
        auto a17  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve");
        auto a17b = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve_s3");
        auto a17c = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve_s5");
        auto a17d = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve_s1");
        
        if (a17[idx] || a17b[idx] || a17c[idx] || a17d[idx]) {{
            FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
        }}
    }}
    
    return flamegpu::ALIVE;
}}
        """


