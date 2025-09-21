#!/usr/bin/env python3
"""
RTC модуль: MP5 probe - чтение данных MP5 для каждого агента
"""
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from model_build import MAX_FRAMES, MAX_SIZE


def register_rtc(model, agent):
    """Регистрирует RTC функции и слои для MP5 probe"""
    
    # RTC функция чтения MP5
    rtc_probe_mp5 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_probe_mp5, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= {MAX_FRAMES}u) return flamegpu::ALIVE;
        
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int safe_day = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
        
        // Читаем из MacroProperty mp5_lin
        const unsigned int base = safe_day * {MAX_FRAMES}u + idx;
        const unsigned int base_next = base + {MAX_FRAMES}u;
        
        auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
        const unsigned int dt = mp[base];
        const unsigned int dn = (safe_day < days_total - 1u ? mp[base_next] : 0u);
        
        FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
        FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
        
        return flamegpu::ALIVE;
    }}
    """
    
    # Регистрируем функцию
    agent.newRTCFunction("rtc_probe_mp5", rtc_probe_mp5)
    
    # Создаем слой для выполнения
    layer_mp5 = model.newLayer()
    layer_mp5.addAgentFunction(agent.getFunction("rtc_probe_mp5"))
    
    return layer_mp5


def create_host_function_mp5_init(mp5_data, frames, days):
    """Создает HostFunction для инициализации MP5 данных"""
    try:
        import pyflamegpu as fg
    except ImportError:
        raise RuntimeError("pyflamegpu не установлен")
    
    class HF_InitMP5(fg.HostFunction):
        def __init__(self, data, frames, days):
            super().__init__()
            self.data = data
            self.frames = frames
            self.days = days
        
        def run(self, FLAMEGPU):
            if FLAMEGPU.getStepCounter() > 0:
                return
            
            mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp5_lin")
            print(f"HF_InitMP5: Инициализация mp5_lin для FRAMES={self.frames}, DAYS={self.days}")
            
            for d in range(self.days + 1):
                for f in range(self.frames):
                    src_idx = d * self.frames + f
                    dst_idx = d * MAX_FRAMES + f
                    if src_idx < len(self.data):
                        mp[dst_idx] = int(self.data[src_idx])
            
            print(f"HF_InitMP5: Инициализировано {min(len(self.data), (self.days+1)*self.frames)} элементов")
    
    return HF_InitMP5(mp5_data, frames, days)
