#!/usr/bin/env python3
"""
RTC функция: rtc_status_6
Обработка статуса хранения (пасс-тру, без изменений)
Основано на коде из бэкапов
Дата: 2025-09-12
"""

from rtc import BaseRTC


class Status6RTC(BaseRTC):
    """RTC функция для обработки статуса 6 (хранение)"""
    
    NAME = "rtc_status_6"
    DEPENDENCIES = ["rtc_quota_begin_day"]
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код rtc_status_6"""
        
        return """
FLAMEGPU_AGENT_FUNCTION(rtc_status_6, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
    if (phase != 0u) return flamegpu::ALIVE;
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != 6u) return flamegpu::ALIVE;
    
    // Не начинать счётчик и триггеры, если агент стартовал в 6 (нет факта перехода 2→6)
    if (FLAMEGPU->getVariable<unsigned int>("s6_started") == 0u) return flamegpu::ALIVE;
    
    unsigned int d6 = FLAMEGPU->getVariable<unsigned int>("s6_days");
    const unsigned int pt = FLAMEGPU->getVariable<unsigned int>("partout_time");
    
    // Инкремент только пока не достигли pt и при pt>0
    if (pt > 0u && d6 < pt) {
        unsigned int nd6 = (d6 < 65535u ? d6 + 1u : d6);
        FLAMEGPU->setVariable<unsigned int>("s6_days", nd6);
        if (nd6 == pt) {
            if (FLAMEGPU->getVariable<unsigned int>("partout_trigger") == 0u) {
                FLAMEGPU->setVariable<unsigned int>("partout_trigger", 1u);
            }
        }
    }
    
    // После достижения pt счётчик не растёт, повторной простановки не будет
    return flamegpu::ALIVE;
}
        """
