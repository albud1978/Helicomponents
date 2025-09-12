#!/usr/bin/env python3
"""
RTC функция: rtc_status_4
Обработка статуса ремонта (счетчик дней, триггеры завершения)
Основано на коде из бэкапов
Дата: 2025-09-12
"""

from rtc import BaseRTC


class Status4RTC(BaseRTC):
    """RTC функция для обработки статуса 4 (ремонт)"""
    
    NAME = "rtc_status_4"
    DEPENDENCIES = ["rtc_quota_begin_day"]
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код rtc_status_4"""
        
        return """
FLAMEGPU_AGENT_FUNCTION(rtc_status_4, flamegpu::MessageNone, flamegpu::MessageNone) {
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != 4u) return flamegpu::ALIVE;
    
    // Инкремент дней ремонта
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days") + 1u;
    FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
    
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int partout_time = FLAMEGPU->getVariable<unsigned int>("partout_time");
    const unsigned int assembly_time = FLAMEGPU->getVariable<unsigned int>("assembly_time");
    
    // Триггеры событий ремонта (однодневные флаги 0/1)
    
    // Partout trigger: срабатывает в день partout_time
    if (repair_days == partout_time && partout_time > 0u) {
        if (FLAMEGPU->getVariable<unsigned int>("partout_trigger") == 0u) {
            FLAMEGPU->setVariable<unsigned int>("partout_trigger", 1u);
            FLAMEGPU->setVariable<unsigned int>("partout_trigger_mark", 1u);
        }
    }
    
    // Assembly trigger: срабатывает за assembly_time дней до окончания ремонта
    if (repair_time > repair_days && assembly_time > 0u) {
        unsigned int days_to_end = repair_time - repair_days;
        if (days_to_end == assembly_time) {
            if (FLAMEGPU->getVariable<unsigned int>("assembly_trigger") == 0u) {
                FLAMEGPU->setVariable<unsigned int>("assembly_trigger", 1u);
                FLAMEGPU->setVariable<unsigned int>("assembly_trigger_mark", 1u);
            }
        }
    }
    
    // Завершение ремонта: переход 4->5
    if (repair_days >= repair_time) {
        FLAMEGPU->setVariable<unsigned int>("status_id", 5u);
        FLAMEGPU->setVariable<unsigned int>("ppr", 0u);           // Сброс PPR
        FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);   // Сброс счетчика
    }
    
    return flamegpu::ALIVE;
}
        """


