#!/usr/bin/env python3
"""
RTC функция: rtc_quota_intent
Подача заявки на квоту (параметризованно для статусов 1,2,3,5)
Основано на коде из бэкапов
Дата: 2025-09-12
"""

from rtc import BaseRTC


class QuotaIntentRTC(BaseRTC):
    """RTC функция для подачи заявки на квоту"""
    
    NAME = "rtc_quota_intent"
    DEPENDENCIES = []
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код rtc_quota_intent для конкретного статуса"""
        
        status_id = kwargs.get('status_id', 2)
        func_name = f"rtc_quota_intent_s{status_id}"
        
        # Специальная логика для статуса 1 (проверка repair_time)
        status1_check = ""
        if status_id == 1:
            status1_check = """
    // Гейт по сроку ремонта: квотируем только если (D+1) - version_date >= repair_time
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int vdate = FLAMEGPU->environment.getProperty<unsigned int>("version_date");
    const unsigned int dayp1_abs = vdate + (day + 1u);
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    if ((dayp1_abs - vdate) < repair_time) return flamegpu::ALIVE;
            """
        
        return f"""
FLAMEGPU_AGENT_FUNCTION({func_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    
    static const unsigned int FRAMES = {frames}u;
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != {status_id}u) return flamegpu::ALIVE;
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    if (idx >= FRAMES) return flamegpu::ALIVE;
    
    {status1_check}
    
    const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
    
    // Подача заявки в соответствующий intent буфер
    if (gb == 1u) {{
        auto i8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
        i8[idx].exchange(1u);
    }} else if (gb == 2u) {{
        auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent");
        i17[idx].exchange(1u);
    }}
    
    // Устанавливаем флаг для диагностики
    FLAMEGPU->setVariable<unsigned int>("intent_flag", 1u);
    
    return flamegpu::ALIVE;
}}
        """
    
    @staticmethod
    def get_source_for_status(status_id: int, frames: int, days: int) -> str:
        """Удобный метод для генерации кода под конкретный статус"""
        return QuotaIntentRTC.get_source(frames, days, status_id=status_id)


