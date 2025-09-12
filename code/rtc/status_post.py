#!/usr/bin/env python3
"""
RTC функции: rtc_status_*_post
Пост-обработка статусов после квотирования
Основано на коде из бэкапов
Дата: 2025-09-12
"""

from rtc import BaseRTC


class StatusPostRTC(BaseRTC):
    """RTC функции для пост-обработки статусов"""
    
    NAME = "rtc_status_post"
    DEPENDENCIES = ["rtc_quota_apply"]
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код для пост-обработки статуса"""
        
        status_id = kwargs.get('status_id', 3)
        func_name = f"rtc_status_{status_id}_post"
        
        if status_id == 3:
            # Переход 3->2 при получении билета
            return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_status_3_post, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != 3u) return flamegpu::ALIVE;
    
    if (FLAMEGPU->getVariable<unsigned int>("ops_ticket") == 1u) {{
        FLAMEGPU->setVariable<unsigned int>("status_id", 2u);
    }}
    
    return flamegpu::ALIVE;
}}
            """
        
        elif status_id == 1:
            # Переход 1->2 при получении билета + установка active_trigger
            return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_status_1_post, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != 1u) return flamegpu::ALIVE;
    
    if (FLAMEGPU->getVariable<unsigned int>("ops_ticket") == 1u) {{
        // Установка active_trigger: день активации = (D+1) - repair_time
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int vdate = FLAMEGPU->environment.getProperty<unsigned int>("version_date");
        const unsigned int dayp1_abs = vdate + (day + 1u);
        const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
        
        unsigned int act = (dayp1_abs > repair_time ? (dayp1_abs - repair_time) : 0u);
        if (act > 65535u) act = 65535u;
        
        if (FLAMEGPU->getVariable<unsigned int>("active_trigger") == 0u && act > 0u) {{
            FLAMEGPU->setVariable<unsigned int>("active_trigger", act);
            FLAMEGPU->setVariable<unsigned int>("active_trigger_mark", 1u);
        }}
        
        FLAMEGPU->setVariable<unsigned int>("status_id", 2u);
    }}
    
    return flamegpu::ALIVE;
}}
            """
        
        elif status_id == 5:
            # Переход 5->2 при получении билета
            return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_status_5_post, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != 5u) return flamegpu::ALIVE;
    
    if (FLAMEGPU->getVariable<unsigned int>("ops_ticket") == 1u) {{
        FLAMEGPU->setVariable<unsigned int>("status_id", 2u);
    }}
    
    return flamegpu::ALIVE;
}}
            """
        
        elif status_id == 2:
            # Переход 2->3 при отсутствии билета
            return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_status_2_post, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
    
    if (FLAMEGPU->getVariable<unsigned int>("ops_ticket") == 0u) {{
        FLAMEGPU->setVariable<unsigned int>("status_id", 3u);
    }}
    
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
    def get_source_for_status(status_id: int, frames: int, days: int) -> str:
        """Удобный метод для генерации кода под конкретный статус"""
        return StatusPostRTC.get_source(frames, days, status_id=status_id)