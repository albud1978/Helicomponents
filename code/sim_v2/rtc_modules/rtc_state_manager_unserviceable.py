#!/usr/bin/env python3
"""
RTC State Manager: Unserviceable (V2)

НОВЫЙ СТАТУС в LIMITER V2:
- Заменяет repair в обороте модели
- Агенты попадают сюда после выработки OH (PPR >= OH)
- Ожидают promote P2 в очередь operations
- Ремонт будет добавлен постпроцессингом (на GPU)

Переходы:
  operations → unserviceable (когда PPR >= OH)
  unserviceable → operations (promote P2)

TODO:
  - Постпроцессинг добавит repair_time дней как статус repair
  - Квота ремонта (ограничение одновременных)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import model_build

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


# RTC код для unserviceable state manager
# Просто держит агентов — переходы управляются через intent_state и квотирование
RTC_STATE_MANAGER_UNSERVICEABLE = """
FLAMEGPU_AGENT_FUNCTION(rtc_state_manager_unserviceable, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V2: Unserviceable — агенты ожидают promote P2
    // Ремонт добавится постпроцессингом (на GPU)
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");  // UInt!
    
    // Если intent=2 (operations), значит получили approve от promote P2
    if (intent == 2u) {
        // Переход будет выполнен через state change в rtc_state_manager_reserve
        // который обрабатывает состояние repair (тот же ID в терминах baseline)
        
        // TODO: пометить для постпроцессинга (добавить repair период)
        // Для этого нужен флаг или запись в MacroProperty
        
        // Логирование
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        const unsigned int idx = FLAMEGPU->getVariable<unsigned short>("idx");
        printf("[UNSERVICEABLE→OPS Day %u] AC %u (idx %u): promote P2 (TODO: postprocess repair)\\n",
               day, ac, idx);
    }
    
    return flamegpu::ALIVE;
}
"""


def register_state_manager_unserviceable(model: 'fg.ModelDescription', agent: 'fg.AgentDescription'):
    """Регистрирует state manager для unserviceable (V2)."""
    
    # RTC функция
    fn = agent.newRTCFunction("rtc_state_manager_unserviceable", RTC_STATE_MANAGER_UNSERVICEABLE)
    fn.setInitialState("repair")  # Используем тот же state ID что и repair (для совместимости)
    fn.setEndState("repair")      # Остаётся в том же состоянии (переход через intent)
    
    # Убираем condition — агенты просто остаются в состоянии
    # Переход будет через state change в другом месте
    # fn.setRTCFunctionCondition(...)
    
    # Альтернативный end state при переходе
    fn.setAllowAgentDeath(False)
    
    # Слой
    layer = model.newLayer("L_state_manager_unserviceable")
    layer.addAgentFunction(fn)
    
    print("    ✅ rtc_state_manager_unserviceable зарегистрирован (V2)")

