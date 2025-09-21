#!/usr/bin/env python3
"""
RTC модуль: статусы 2/4/6 - логика переходов и расчетов
"""
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from model_build import MAX_FRAMES, MAX_SIZE


def register_rtc(model, agent):
    """Регистрирует RTC функции и слои для статусов 2/4/6"""
    
    # Статус 6: неизменяемый
    rtc_status_6 = """
    FLAMEGPU_AGENT_FUNCTION(rtc_status_6, flamegpu::MessageNone, flamegpu::MessageNone) {
        const unsigned int st = FLAMEGPU->getVariable<unsigned int>("status_id");
        if (st != 6u) return flamegpu::ALIVE;
        
        // Статус 6 неизменяем в симуляции
        return flamegpu::ALIVE;
    }
    """
    
    # Статус 4: ремонт
    rtc_status_4 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_status_4, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int st = FLAMEGPU->getVariable<unsigned int>("status_id");
        if (st != 4u) return flamegpu::ALIVE;
        
        const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
        unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
        
        // Инкремент счетчика дней в ремонте
        repair_days++;
        FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
        
        // Проверка выхода из ремонта
        if (repair_days >= repair_time) {{
            FLAMEGPU->setVariable<unsigned int>("status_id", 5u);
            FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
            FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
        }}
        
        return flamegpu::ALIVE;
    }}
    """
    
    # Статус 2: летный с проверками переходов
    rtc_status_2 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_status_2, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int st = FLAMEGPU->getVariable<unsigned int>("status_id");
        if (st != 2u) return flamegpu::ALIVE;
        
        // Наработки и пороги
        unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
        unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
        const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
        const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
        const unsigned int br = FLAMEGPU->getVariable<unsigned int>("br");
        
        // Суточный налет
        const unsigned int dt = FLAMEGPU->getVariable<unsigned int>("daily_today_u32");
        const unsigned int dn = FLAMEGPU->getVariable<unsigned int>("daily_next_u32");
        
        // Увеличиваем наработки
        sne += dt;
        ppr += dt;
        FLAMEGPU->setVariable<unsigned int>("sne", sne);
        FLAMEGPU->setVariable<unsigned int>("ppr", ppr);
        
        // Проверка переходов
        if (br > 0u && sne >= br) {{
            // Переход 2->6 (списание)
            FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
            FLAMEGPU->setVariable<unsigned int>("s6_started", 1u);
        }} else if (oh > 0u && ppr >= oh) {{
            // Переход 2->4 (капремонт)
            FLAMEGPU->setVariable<unsigned int>("status_id", 4u);
        }}
        
        return flamegpu::ALIVE;
    }}
    """
    
    # Регистрируем функции
    agent.newRTCFunction("rtc_status_6", rtc_status_6)
    agent.newRTCFunction("rtc_status_4", rtc_status_4)
    agent.newRTCFunction("rtc_status_2", rtc_status_2)
    
    # Создаем слои в правильном порядке
    layer_s6 = model.newLayer()
    layer_s6.addAgentFunction(agent.getFunction("rtc_status_6"))
    
    layer_s4 = model.newLayer()
    layer_s4.addAgentFunction(agent.getFunction("rtc_status_4"))
    
    layer_s2 = model.newLayer()
    layer_s2.addAgentFunction(agent.getFunction("rtc_status_2"))
    
    return [layer_s6, layer_s4, layer_s2]
