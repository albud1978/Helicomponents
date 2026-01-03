#!/usr/bin/env python3
"""
RTC модуль для GPU постпроцессинга MP2: заполнение истории ремонта по active_trigger

Назначение:
- Обрабатывает переход inactive → operations (1→2), который происходит МГНОВЕННО в симуляции
- На самом деле агент прошёл через ремонт (1→4→2), но это скрыто
- active_trigger=1 помечает день перехода 1→2
- Постпроцессинг заполняет историю ремонта ЗАДНИМ ЧИСЛОМ

Логика:
1. Поиск дня d_event где active_trigger=1
2. Вычисление окна ремонта: [d_event - repair_time .. d_event - 1]
3. Заполнение state=4, repair_days=1..R для окна
4. Установка assembly_trigger=1 в день (d_event - assembly_time)
5. Исправление transition флагов:
   - transition_1_to_4=1 в день начала ремонта (s)
   - transition_4_to_2=1 в день выхода из ремонта (d_event)

Выполнение:
- ПОСЛЕ завершения основной симуляции
- При export_phase=2 (один фиктивный шаг)
- ПЕРЕД MP2 drain
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import model_build

import pyflamegpu as fg


def register_mp2_postprocess_active(model, agent):
    """Регистрирует RTC модуль для постпроцессинга active_trigger"""
    
    print("  Подключение модуля: mp2_postprocess_active")
    
    # ФИКСИРОВАННЫЕ размеры для RTC кэширования
    MAX_FRAMES = model_build.RTC_MAX_FRAMES
    MAX_DAYS = model_build.MAX_DAYS
    MP2_SIZE = MAX_FRAMES * (MAX_DAYS + 1)
    
    rtc_code = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_postprocess_active, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Работает ТОЛЬКО при export_phase=2 (постпроцессинг после симуляции)
    const unsigned int phase = FLAMEGPU->environment.getProperty<unsigned int>("export_phase");
    if (phase != 2u) return flamegpu::ALIVE;
    
    // Пропускаем агентов с aircraft_number=0 (резерв под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) return flamegpu::ALIVE;
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
    const unsigned int R = FLAMEGPU->getVariable<unsigned int>("repair_time");
    const unsigned int A = FLAMEGPU->getVariable<unsigned int>("assembly_time");
    
    
    // Получаем MP2 MacroProperty для чтения и модификации
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_transition_1_to_4 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_transition_1_to_4");
    auto mp2_transition_4_to_2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_transition_4_to_2");
    
    // ═══════════════════════════════════════════════════════════════════════════
    // Поиск события active_trigger=1 (переход inactive → operations)
    // ═══════════════════════════════════════════════════════════════════════════
    bool processed = false;
    for (unsigned int d_event = 0u; d_event < days_total && !processed; ++d_event) {{
        const unsigned int pos_event = d_event * {MAX_FRAMES}u + idx;
        const unsigned int act_flag = mp2_active_trigger[pos_event];
        
        if (act_flag == 1u) {{
            // Нашли день перехода 1→2!
            // Реально агент прошёл через ремонт: 1→4 (день s) → 4→2 (день d_event)
            
            // ═══════════════════════════════════════════════════════════════════
            // Вычисление окна ремонта [s..e]
            // ═══════════════════════════════════════════════════════════════════
            int s_signed = (int)d_event - (int)R;
            unsigned int s = (s_signed < 0) ? 0u : (unsigned int)s_signed;
            unsigned int e = (d_event > 0u) ? (d_event - 1u) : 0u;
            
            // Валидация окна
            if (s > e || s >= days_total) continue;
            
            // ═══════════════════════════════════════════════════════════════════
            // ЗАПОЛНЕНИЕ ОКНА РЕМОНТА [s..e]
            // ═══════════════════════════════════════════════════════════════════
            for (unsigned int d = s; d <= e && d < days_total; ++d) {{
                const unsigned int pos = d * {MAX_FRAMES}u + idx;
                
                // 1. Устанавливаем state = 4 (repair)
                mp2_state[pos].exchange(4u);
                
                // 2. Устанавливаем repair_days от 1 с инкрементом
                const unsigned int rdv = (d - s + 1u);
                mp2_repair_days[pos].exchange(rdv);
            }}
            
            // ═══════════════════════════════════════════════════════════════════
            // УСТАНОВКА assembly_trigger
            // ═══════════════════════════════════════════════════════════════════
            // 3. assembly_trigger=1 в день (d_event - A)
            if (d_event >= A) {{
                const unsigned int asm_day = d_event - A;
                if (asm_day < days_total) {{
                    const unsigned int pos_asm = asm_day * {MAX_FRAMES}u + idx;
                    mp2_assembly_trigger[pos_asm].exchange(1u);
                }}
            }}
            
            // ═══════════════════════════════════════════════════════════════════
            // ИСПРАВЛЕНИЕ TRANSITION ФЛАГОВ
            // ═══════════════════════════════════════════════════════════════════
            // 4. Устанавливаем transition_1_to_4=1 в день начала ремонта (s)
            if (s < days_total) {{
                const unsigned int pos_s = s * {MAX_FRAMES}u + idx;
                mp2_transition_1_to_4[pos_s].exchange(1u);
            }}
            
            // 5. Устанавливаем transition_4_to_2=1 в день выхода из ремонта (d_event)
            mp2_transition_4_to_2[pos_event].exchange(1u);
            
            // Обрабатываем только первое событие active_trigger для агента
            processed = true;
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""
    
    try:
        layer = model.newLayer("mp2_postprocess_active")
        
        # ✅ КРИТИЧНО: Создаём функции для КАЖДОГО состояния
        # FLAME GPU не вызывает агентные функции без привязки к состояниям!
        states = ['inactive', 'operations', 'serviceable', 'repair', 'reserve', 'storage']
        
        for state_name in states:
            fn = agent.newRTCFunction(f"rtc_mp2_postprocess_active_{state_name}", rtc_code)
            fn.setInitialState(state_name)
            fn.setEndState(state_name)  # Остаются в том же состоянии
            layer.addAgentFunction(fn)
        
        print(f"  RTC модуль mp2_postprocess_active зарегистрирован (6 функций для всех состояний)")
        return layer
        
    except Exception as e:
        print(f"  ❌ Ошибка при регистрации mp2_postprocess_active: {e}")
        raise


def register_rtc(model, agent):
    """Совместимость с orchestrator_v2 (alias для register_mp2_postprocess_active)"""
    return register_mp2_postprocess_active(model, agent)

