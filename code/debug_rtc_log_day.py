#!/usr/bin/env python3
"""
Отладка компиляции rtc_log_day
"""
import os
import sys

# Настройка окружения для Mode A
os.environ['HL_ENABLE_MP2'] = '1'
os.environ['HL_STATUS246_SMOKE'] = '1'
os.environ['PYTHONUNBUFFERED'] = '1'

try:
    import pyflamegpu
except ImportError:
    print("pyflamegpu не установлен")
    sys.exit(1)

def test_rtc_log_day():
    """Тестируем только rtc_log_day"""
    FRAMES = 286
    DAYS = 7
    
    model = pyflamegpu.ModelDescription("TestRTCLogDay")
    env = model.Environment()
    
    # Минимальные environment свойства
    env.newPropertyUInt("export_phase", 0)
    
    # MP2 MacroProperty 
    env.newMacroPropertyUInt32("mp2_status", FRAMES * DAYS)
    env.newMacroPropertyUInt32("mp2_repair_days", FRAMES * DAYS)
    env.newMacroPropertyUInt32("mp2_active_trigger", FRAMES * DAYS)
    env.newMacroPropertyUInt32("mp2_assembly_trigger", FRAMES * DAYS)
    
    # Агент
    agent = model.newAgent("component")
    agent.newVariableUInt("idx", 0)
    agent.newVariableUInt("status_id", 0)
    agent.newVariableUInt("repair_days", 0)
    agent.newVariableUInt("active_trigger", 0)
    agent.newVariableUInt("assembly_trigger", 0)
    
    # RTC функция rtc_log_day
    rtc_log_day_src = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_log_day, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
        if (phase != 0u) return flamegpu::ALIVE;
        static const unsigned int FRAMES = {FRAMES}u;
        static const unsigned int DAYS   = {DAYS}u;
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= FRAMES) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        if (day >= DAYS) return flamegpu::ALIVE;
        const unsigned int row = day * FRAMES + idx;
        auto a_stat = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*DAYS)>("mp2_status");
        auto a_rd   = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*DAYS)>("mp2_repair_days");
        auto a_act  = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*DAYS)>("mp2_active_trigger");
        auto a_asm  = FLAMEGPU->environment.getMacroProperty<unsigned int, (FRAMES*DAYS)>("mp2_assembly_trigger");
        a_stat[row].exchange(FLAMEGPU->getVariable<unsigned int>("status_id"));
        a_rd[row].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
        a_act[row].exchange(FLAMEGPU->getVariable<unsigned int>("active_trigger"));
        a_asm[row].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_trigger"));
        return flamegpu::ALIVE;
    }}
    """
    
    print(f"Компилируем rtc_log_day с FRAMES={FRAMES}, DAYS={DAYS}")
    print(f"Размер MP2 массивов: {FRAMES * DAYS}")
    
    try:
        agent.newRTCFunction("rtc_log_day", rtc_log_day_src)
        print("✅ RTC функция зарегистрирована")
        
        # Добавляем в слой
        layer = model.newLayer()
        layer.addAgentFunction(agent.getFunction("rtc_log_day"))
        print("✅ Слой создан")
        
        # Создаем симуляцию для компиляции
        print("Создаем симуляцию...")
        sim = pyflamegpu.CUDASimulation(model)
        print("✅ Симуляция создана - компиляция успешна!")
        
    except Exception as e:
        print(f"\n❌ ОШИБКА: {type(e).__name__}")
        print(f"Сообщение: {e}")
        print("\n----- ИСХОДНЫЙ КОД rtc_log_day -----")
        print(rtc_log_day_src)
        print("----- КОНЕЦ КОДА -----\n")
        
        # Попробуем найти детали в сообщении об ошибке
        error_str = str(e)
        if "compile" in error_str:
            print("💡 Подсказка: Проблема компиляции NVRTC")
            
            # Проверим размерность
            total_size = FRAMES * DAYS
            print(f"\nПроверка размерности:")
            print(f"  FRAMES = {FRAMES}")
            print(f"  DAYS = {DAYS}")
            print(f"  FRAMES*DAYS = {total_size}")
            print(f"  Выражение в коде: (FRAMES*DAYS) = ({FRAMES}u*{DAYS}u)")

if __name__ == "__main__":
    test_rtc_log_day()
