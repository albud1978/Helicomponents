#!/usr/bin/env python3
"""
RTC функция: rtc_status_2
Обработка статуса эксплуатации (начисление наработки, проверка лимитов)
Основано на ops_check.cu и коде из бэкапов
Дата: 2025-09-12
"""

from rtc import BaseRTC


class Status2RTC(BaseRTC):
    """RTC функция для обработки статуса 2 (эксплуатация)"""
    
    NAME = "rtc_status_2"
    DEPENDENCIES = ["rtc_probe_mp5", "rtc_quota_begin_day"]
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код rtc_status_2"""
        
        return """
FLAMEGPU_AGENT_FUNCTION(rtc_status_2, flamegpu::MessageNone, flamegpu::MessageNone) {
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
    
    // 1) Начисление наработки за сегодня (dt)
    const unsigned int dt = FLAMEGPU->getVariable<unsigned int>("daily_today_u32");
    const unsigned int dn = FLAMEGPU->getVariable<unsigned int>("daily_next_u32");
    
    unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    
    if (dt > 0u) {
        sne = sne + dt;
        ppr = ppr + dt;
        FLAMEGPU->setVariable<unsigned int>("sne", sne);
        FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
    }
    
    // 2) Прогноз на завтра (проверка окон ресурсов)
    const unsigned int sne_next = sne + dn;
    const unsigned int ppr_next = ppr + dn;
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
    
    // 3) LL-порог: если sne+dn >= ll -> переход 2->6 (хранение)
    if (sne_next >= ll) {
        FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
        return flamegpu::ALIVE;
    }
    
    // 4) OH-порог: если ppr+dn >= oh -> проверка BR
    if (ppr_next >= oh) {
        if (sne_next < br) {
            // Beyond Repair не достигнут -> ремонт
            FLAMEGPU->setVariable<unsigned int>("status_id", 4u);
            FLAMEGPU->setVariable<unsigned int>("repair_days", 1u);
            
            // Установка триггеров ремонта
            const unsigned int day = FLAMEGPU->getStepCounter();
            const unsigned int vdate = FLAMEGPU->environment.getProperty<unsigned int>("version_date");
            const unsigned int day_abs = vdate + day + 1u; // завтра
            const unsigned int pt = FLAMEGPU->getVariable<unsigned int>("partout_time");
            const unsigned int rt = FLAMEGPU->getVariable<unsigned int>("repair_time");
            const unsigned int at = FLAMEGPU->getVariable<unsigned int>("assembly_time");
            
            if (pt > 0u) {
                unsigned int partout_day = day_abs + pt - 1u;
                if (partout_day > 65535u) partout_day = 65535u;
                FLAMEGPU->setVariable<unsigned int>("partout_trigger", partout_day);
            }
            
            if (rt > at && at > 0u) {
                unsigned int assembly_day = day_abs + rt - at - 1u;
                if (assembly_day > 65535u) assembly_day = 65535u;
                FLAMEGPU->setVariable<unsigned int>("assembly_trigger", assembly_day);
            }
        } else {
            // Beyond Repair достигнут -> хранение
            FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
        }
        return flamegpu::ALIVE;
    }
    
    // 5) Если лимиты не нарушены, остаемся в статусе 2
    // Квотирование будет обрабатываться отдельными слоями quota
    
    return flamegpu::ALIVE;
}
        """


