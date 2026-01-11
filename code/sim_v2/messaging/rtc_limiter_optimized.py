#!/usr/bin/env python3
"""
RTC модуль: Оптимизированный расчёт LIMITER V2

Оптимизация:
- limiter вычисляется ОДИН РАЗ при входе в operations
- На каждом шаге: limiter -= adaptive_days (вместе с инкрементами)
- adaptive_days = min(min_agent_limiter, next_program_change - current_day)

Формула limiter при входе в ops:
  limiter = min(
    (ll - sne) / avg_dt,  // дней до LL
    (oh - ppr) / avg_dt   // дней до OH
  )
  
  где avg_dt ≈ 100 мин/день (среднее из mp5)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


# ═══════════════════════════════════════════════════════════════════
# RTC: Вычисление limiter при входе в operations
# ═══════════════════════════════════════════════════════════════════
RTC_COMPUTE_LIMITER_ON_ENTRY = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_limiter_on_entry, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Вычисляем limiter для агентов в operations:
    // 1. На шаге 0: для ВСЕХ агентов в ops (начальная инициализация)
    // 2. На остальных шагах: для агентов с intent=2 (входящих в ops)
    
    const unsigned int step = FLAMEGPU->getStepCounter();
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    const unsigned short current_limiter = FLAMEGPU->getVariable<unsigned short>("limiter");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // Debug: логируем условия на шаге 0
    if (step == 0u && idx < 3u) {{
        printf("  [LIMITER CHECK Step 0] idx=%u: intent=%u, current_limiter=%u\\n", idx, intent, current_limiter);
    }}
    
    // На шаге 0: вычисляем для всех с limiter=0
    // На остальных шагах: только для входящих (intent=2)
    bool need_compute = (step == 0u && current_limiter == 0u) || (step > 0u && intent == 2u);
    
    if (!need_compute) {{
        return flamegpu::ALIVE;
    }}
    
    // Текущие значения
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    
    // Средний налёт (минут/день) — берём консервативную оценку
    // В реальности ~100-150 мин/день, используем 100 для безопасности
    const unsigned int avg_dt = 100u;  // TODO: можно брать из Environment
    
    // Остаток ресурса (в минутах)
    unsigned int remaining_ll = (sne < ll) ? (ll - sne) : 0u;
    unsigned int remaining_oh = (ppr < oh) ? (oh - ppr) : 0u;
    
    // Дней до исчерпания (делим на avg_dt)
    unsigned int days_to_ll = remaining_ll / avg_dt;
    unsigned int days_to_oh = remaining_oh / avg_dt;
    
    // Limiter = минимум из двух
    unsigned int limiter = (days_to_ll < days_to_oh) ? days_to_ll : days_to_oh;
    
    // Ограничиваем UInt16 (65535 дней ≈ 179 лет)
    if (limiter > 65535u) limiter = 65535u;
    
    // Устанавливаем limiter
    FLAMEGPU->setVariable<unsigned short>("limiter", (unsigned short)limiter);
    
    // Debug (первые несколько агентов или шаг 0) — idx уже объявлен выше!
    if (step == 0u || idx < 3u) {{
        const unsigned int ac = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
        printf("  [LIMITER ENTRY Step %u] AC %u (idx %u): ll=%u, sne=%u, days_ll=%u; oh=%u, ppr=%u, days_oh=%u → limiter=%u\\n",
               step, ac, idx, ll, sne, days_to_ll, oh, ppr, days_to_oh, limiter);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════
# RTC: Декремент limiter на каждом шаге (вместе с инкрементами)
# ═══════════════════════════════════════════════════════════════════
RTC_DECREMENT_LIMITER = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_decrement_limiter, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Декрементируем limiter для агентов в operations
    // Вызывается ПОСЛЕ инкрементов SNE/PPR
    
    const unsigned short limiter = FLAMEGPU->getVariable<unsigned short>("limiter");
    
    // Если limiter == 0, агент уже на выходе (или не в ops)
    if (limiter == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Получаем adaptive_days из Environment
    const unsigned int adaptive_days = FLAMEGPU->environment.getProperty<unsigned int>("adaptive_days");
    
    // Декремент
    unsigned short new_limiter;
    if (limiter <= adaptive_days) {{
        new_limiter = 0u;  // Достигли лимита
    }} else {{
        new_limiter = limiter - (unsigned short)adaptive_days;
    }}
    
    FLAMEGPU->setVariable<unsigned short>("limiter", new_limiter);
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════
# RTC: Обнуление limiter при выходе из operations
# ═══════════════════════════════════════════════════════════════════
RTC_CLEAR_LIMITER_ON_EXIT = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_clear_limiter_on_exit, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Обнуляем limiter при выходе из operations
    // intent_state != 2 означает, что агент покидает ops
    
    const unsigned int intent = FLAMEGPU->getVariable<unsigned int>("intent_state");
    
    // Если intent != 2, агент уходит из operations
    if (intent != 2u && intent != 0u) {{  // 0 = нет intent
        FLAMEGPU->setVariable<unsigned short>("limiter", 0u);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════
# RTC: Вычисление adaptive_days (min из всех лимитеров)
# ═══════════════════════════════════════════════════════════════════
RTC_COMPUTE_ADAPTIVE_DAYS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_min_limiter, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Каждый агент в ops записывает свой limiter в MacroProperty для reduction
    // ВАЖНО: Только атомарная запись, без чтения!
    
    const unsigned short limiter = FLAMEGPU->getVariable<unsigned short>("limiter");
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int step = FLAMEGPU->getStepCounter();
    
    // Debug: логируем на шаге 0
    if (step == 0u && idx < 3u) {{
        printf("  [MIN_LIMITER Step %u] idx=%u: limiter=%u\\n", step, idx, limiter);
    }}
    
    // Пропускаем агентов с limiter == 0 (не в ops или уже на выходе)
    if (limiter == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    // Атомарный min в MacroProperty (без предварительного чтения!)
    auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("mp_min_limiter");
    
    // atomicMin: записываем если наш limiter меньше текущего
    mp_min[0].min((unsigned int)limiter);
    
    // Debug: первый агент с небольшим limiter
    if (limiter < 50u && step == 0u) {{
        printf("  [MIN_LIMITER] AC idx=%u wrote limiter=%u to mp_min\\n", idx, limiter);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════
# HostFunction: Вычисление adaptive_days
# ═══════════════════════════════════════════════════════════════════
class HF_InitMinLimiter(fg.HostFunction):
    """Инициализирует mp_min_limiter = MAX перед первым RTC (init function)"""
    
    def run(self, FLAMEGPU):
        mp_min = FLAMEGPU.environment.getMacroPropertyUInt("mp_min_limiter")
        mp_min[0] = 0xFFFFFFFF
        print(f"  [HF_InitMinLimiter] mp_min_limiter[0] = {mp_min[0]}")


class HF_ComputeAdaptiveDays(fg.HostFunction):
    """Вычисляет adaptive_days = min(min_limiter, next_program_change - current_day)"""
    
    def __init__(self, program_changes: list, end_day: int):
        super().__init__()
        self.program_changes = sorted(program_changes)
        self.end_day = end_day
        self.current_pc_idx = 0  # Индекс следующего изменения программы
    
    def run(self, FLAMEGPU):
        current_day = FLAMEGPU.environment.getPropertyUInt("current_day")
        
        # 1. Получаем min_limiter из MacroProperty
        try:
            mp_min = FLAMEGPU.environment.getMacroPropertyUInt("mp_min_limiter")
            min_limiter = int(mp_min[0])
            # print(f"  [HF] mp_min[0] raw = {mp_min[0]}, min_limiter = {min_limiter}")
        except Exception as e:
            print(f"  [HF] ERROR reading mp_min_limiter: {e}")
            min_limiter = 0xFFFFFFFF
        
        # Сбрасываем MacroProperty для следующего шага
        try:
            mp_min = FLAMEGPU.environment.getMacroPropertyUInt("mp_min_limiter")
            mp_min[0] = 0xFFFFFFFF
        except Exception as e:
            print(f"  [HF] ERROR resetting mp_min_limiter: {e}")
        
        # 2. Находим следующее изменение программы
        next_pc_day = self.end_day
        for i, pc_day in enumerate(self.program_changes):
            if pc_day > current_day:
                next_pc_day = pc_day
                break
        
        days_to_pc = next_pc_day - current_day if next_pc_day > current_day else self.end_day
        
        # 3. adaptive_days = min(min_limiter, days_to_pc, MAX_ADAPTIVE_DAYS)
        # MAX_ADAPTIVE_DAYS = 7 гарантирует регулярную проверку динамического spawn
        MAX_ADAPTIVE_DAYS = 7
        
        if min_limiter == 0xFFFFFFFF:
            min_limiter = days_to_pc  # Нет агентов в ops
        
        adaptive_days = min(min_limiter, days_to_pc, MAX_ADAPTIVE_DAYS)
        
        # Не выходим за end_day
        if current_day + adaptive_days > self.end_day:
            adaptive_days = self.end_day - current_day
        
        # Минимум 1 день
        if adaptive_days < 1:
            adaptive_days = 1
        
        # 4. Устанавливаем adaptive_days в Environment
        FLAMEGPU.environment.setPropertyUInt("adaptive_days", adaptive_days)
        
        # 5. Обновляем current_day
        new_day = current_day + adaptive_days
        FLAMEGPU.environment.setPropertyUInt("current_day", new_day)
        
        step = FLAMEGPU.getStepCounter()
        if step % 50 == 0 or adaptive_days > 30:
            print(f"  [Step {step}] Day {current_day} → {new_day}, adaptive={adaptive_days} (limiter={min_limiter}, pc={days_to_pc})")
        
        return fg.CONTINUE


def setup_limiter_macroproperties(env, program_changes: list):
    """Настраивает MacroProperty для оптимизированного лимитера"""
    
    # MacroProperty для min reduction
    env.newMacroPropertyUInt("mp_min_limiter", 4)  # 4 элемента для atomic
    
    # Сохраняем program_changes как PropertyArray
    # API: newPropertyArrayUInt(name, values) без размера
    max_changes = 150
    pc_array = [0] * max_changes
    for i, pc_day in enumerate(program_changes[:max_changes]):
        pc_array[i] = pc_day
    env.newPropertyArrayUInt("mp_program_changes_v3", pc_array)
    
    # Эти свойства могут уже существовать из rtc_limiter_date
    try:
        env.newPropertyUInt("num_program_changes_v3", len(program_changes))
    except:
        pass
    try:
        env.newPropertyUInt("adaptive_days", 1)  # По умолчанию 1 день
    except:
        pass
    
    print(f"  ✅ Limiter MacroProperty: mp_min_limiter, mp_program_changes_v3[{len(program_changes)}]")


def register_limiter_optimized(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует оптимизированные RTC функции для limiter"""
    
    # 1. Вычисление limiter при входе в ops (после state managers)
    fn_entry = agent.newRTCFunction("rtc_compute_limiter_on_entry", RTC_COMPUTE_LIMITER_ON_ENTRY)
    fn_entry.setInitialState("operations")
    fn_entry.setEndState("operations")
    layer_entry = model.newLayer("L_limiter_entry")
    layer_entry.addAgentFunction(fn_entry)
    
    # 2. Декремент limiter на каждом шаге
    fn_decr = agent.newRTCFunction("rtc_decrement_limiter", RTC_DECREMENT_LIMITER)
    fn_decr.setInitialState("operations")
    fn_decr.setEndState("operations")
    layer_decr = model.newLayer("L_limiter_decrement")
    layer_decr.addAgentFunction(fn_decr)
    
    # 3. Обнуление при выходе
    fn_exit = agent.newRTCFunction("rtc_clear_limiter_on_exit", RTC_CLEAR_LIMITER_ON_EXIT)
    fn_exit.setInitialState("operations")
    fn_exit.setEndState("operations")
    layer_exit = model.newLayer("L_limiter_exit")
    layer_exit.addAgentFunction(fn_exit)
    
    # 4. Reduction для min
    fn_min = agent.newRTCFunction("rtc_compute_min_limiter", RTC_COMPUTE_ADAPTIVE_DAYS)
    fn_min.setInitialState("operations")
    fn_min.setEndState("operations")
    layer_min = model.newLayer("L_limiter_min")
    layer_min.addAgentFunction(fn_min)
    
    print("  ✅ Limiter optimized RTC зарегистрирован")

