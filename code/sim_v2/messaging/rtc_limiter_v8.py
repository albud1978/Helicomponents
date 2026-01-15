#!/usr/bin/env python3
"""
RTC модуль: LIMITER V8 — Упрощённая архитектура adaptive steps

АРХИТЕКТУРА V8:
- ОДИН MacroProperty `deterministic_dates[]` со всеми детерминированными датами
- Декремент limiter для ops/repair/unserviceable
- Пересчёт limiter ТОЛЬКО при входе в operations
- repair_days для unserviceable как счётчик до права на вход

Формула adaptive_days:
  adaptive_days = MIN(
      min_dynamic_limiter,  // MIN(ops.limiter, repair.repair_days, unsvc.repair_days)
      next_deterministic - current_day  // Ближайшая дата из массива
  )

Валидация:
  Количество динамических шагов по limiter=0 ≈ 183 (baseline)

Дата: 15.01.2026
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


CUMSUM_SIZE = RTC_MAX_FRAMES * (MAX_DAYS + 1)
MAX_DETERMINISTIC_DATES = 500  # Макс. количество детерминированных дат


# ═══════════════════════════════════════════════════════════════════════════════
# MacroProperty для V8
# ═══════════════════════════════════════════════════════════════════════════════

def setup_v8_macroproperties(env, deterministic_dates: list, end_day: int):
    """
    Настраивает MacroProperty для V8.
    
    Args:
        env: EnvironmentDescription
        deterministic_dates: Отсортированный список всех детерминированных дат:
            - День 0
            - Даты выхода из ремонта (repair_time - repair_days для агентов в repair)
            - Даты детерминированного spawn
            - Program changes
            - end_day
        end_day: Последний день симуляции
    """
    
    # Убираем дубликаты и сортируем
    dates = sorted(set(deterministic_dates))
    
    # Убеждаемся что 0 и end_day присутствуют
    if 0 not in dates:
        dates = [0] + dates
    if end_day not in dates:
        dates.append(end_day)
    dates = sorted(dates)
    
    # deterministic_dates_mp — отсортированный массив дат
    env.newMacroPropertyUInt("deterministic_dates_mp", MAX_DETERMINISTIC_DATES)
    
    # current_day в MacroProperty
    env.newMacroPropertyUInt("current_day_mp", 4)  # [0]=current_day, [1]=prev_day
    
    # min_dynamic_limiter — минимум по ops/repair/unsvc
    env.newMacroPropertyUInt("min_dynamic_mp", 4)  # [0]=min_limiter
    
    # adaptive_result
    env.newMacroPropertyUInt("adaptive_result_mp", 4)  # [0]=adaptive_days
    
    # min_exit_date_mp — для совместимости с V7 state transitions
    env.newMacroPropertyUInt("min_exit_date_mp", 4)  # [0]=min_exit_date
    
    # Environment properties
    try:
        env.newPropertyUInt("num_deterministic_dates", len(dates))
    except:
        env.setPropertyUInt("num_deterministic_dates", len(dates))
    
    try:
        env.newPropertyUInt("end_day", end_day)
    except:
        pass  # Уже существует
    
    print(f"  ✅ V8 MacroProperty: deterministic_dates[{len(dates)}], min_dynamic_mp")
    print(f"     Первые даты: {dates[:10]}...")
    print(f"     Последние даты: ...{dates[-5:]}")
    
    return dates


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 1: Сброс min_dynamic_mp перед сбором
# ═══════════════════════════════════════════════════════════════════════════════

RTC_RESET_MIN_DYNAMIC = """
FLAMEGPU_AGENT_FUNCTION(rtc_reset_min_dynamic_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Сброс min_dynamic_mp в начале шага (ТОЛЬКО QuotaManager group_by=1)
    const unsigned char group_by = FLAMEGPU->getVariable<unsigned char>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;
    
    auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_dynamic_mp");
    mp_min[0].exchange(0xFFFFFFFFu);  // MAX = нет лимитера
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 2: Сбор min limiter от operations
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COLLECT_MIN_OPS = """
FLAMEGPU_AGENT_FUNCTION(rtc_collect_min_ops_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Агенты в operations записывают свой limiter в min_dynamic_mp
    const unsigned short limiter = FLAMEGPU->getVariable<unsigned short>("limiter");
    
    // Пропускаем агентов с limiter == 0 (уже на выходе)
    if (limiter == 0u) return flamegpu::ALIVE;
    
    auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_dynamic_mp");
    mp_min[0].min((unsigned int)limiter);  // atomicMin
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 3: Сбор min repair_days от repair агентов
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COLLECT_MIN_REPAIR = """
FLAMEGPU_AGENT_FUNCTION(rtc_collect_min_repair_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Агенты в repair записывают свой repair_days в min_dynamic_mp
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    
    // Пропускаем если repair_days == 0 (уже на выходе)
    if (repair_days == 0u) return flamegpu::ALIVE;
    
    auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_dynamic_mp");
    mp_min[0].min(repair_days);  // atomicMin
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 4: Сбор min repair_days от unserviceable агентов
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COLLECT_MIN_UNSVC = """
FLAMEGPU_AGENT_FUNCTION(rtc_collect_min_unsvc_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Агенты в unserviceable записывают свой repair_days в min_dynamic_mp
    // repair_days = дней до права на вход в operations
    const unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    
    // Пропускаем если repair_days == 0 (уже имеет право на промоут)
    if (repair_days == 0u) return flamegpu::ALIVE;
    
    auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_dynamic_mp");
    mp_min[0].min(repair_days);  // atomicMin
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 5: Вычисление adaptive_days
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COMPUTE_ADAPTIVE_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_adaptive_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V8: QuotaManager вычисляет adaptive_days
    // adaptive_days = MIN(min_dynamic, days_to_next_deterministic)
    
    const uint8_t group_by = FLAMEGPU->getVariable<uint8_t>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;  // Только один агент
    
    // Читаем current_day
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    
    // Early return если завершено
    if (current_day >= end_day) {{
        auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
        result[0].exchange(0u);
        return flamegpu::ALIVE;
    }}
    
    // 1. Читаем min_dynamic (ops.limiter, repair.repair_days, unsvc.repair_days)
    auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_dynamic_mp");
    unsigned int min_dynamic = mp_min[0];
    
    // 2. Находим next deterministic date
    auto dates = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DETERMINISTIC_DATES}u>("deterministic_dates_mp");
    const unsigned int num_dates = FLAMEGPU->environment.getProperty<unsigned int>("num_deterministic_dates");
    
    unsigned int next_det = end_day;
    for (unsigned int i = 0u; i < num_dates && i < {MAX_DETERMINISTIC_DATES}u; ++i) {{
        unsigned int d = dates[i];
        if (d > current_day) {{
            next_det = d;
            break;
        }}
    }}
    
    unsigned int days_to_det = (next_det > current_day) ? (next_det - current_day) : 1u;
    
    // 3. adaptive_days = MIN(min_dynamic, days_to_det)
    unsigned int adaptive_days = days_to_det;
    
    if (min_dynamic < 0xFFFFFFFFu && min_dynamic > 0u && min_dynamic < adaptive_days) {{
        adaptive_days = min_dynamic;
    }}
    
    // Ограничения
    unsigned int remaining = end_day - current_day;
    if (adaptive_days > remaining) adaptive_days = remaining;
    if (adaptive_days < 1u) adaptive_days = 1u;
    
    // DEBUG: каждые 50 шагов
    unsigned int step = FLAMEGPU->getStepCounter();
    if (step % 50u == 0u || step < 10u) {{
        printf("[V8] step=%u, day=%u, min_dyn=%u, next_det=%u -> adaptive=%u\\n",
               step, current_day, min_dynamic, next_det, adaptive_days);
    }}
    
    // 4. Записываем результат
    auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    result[0].exchange(adaptive_days);
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 6: Декремент limiter для operations
# ═══════════════════════════════════════════════════════════════════════════════

RTC_DECREMENT_OPS_V8 = """
FLAMEGPU_AGENT_FUNCTION(rtc_decrement_ops_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Декремент limiter для агентов в operations
    
    auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    const unsigned int adaptive_days = result[0];
    
    if (adaptive_days == 0u) return flamegpu::ALIVE;
    
    unsigned short limiter = FLAMEGPU->getVariable<unsigned short>("limiter");
    
    if (limiter > 0u) {
        if (limiter <= (unsigned short)adaptive_days) {
            limiter = 0u;  // Достигли лимита!
        } else {
            limiter -= (unsigned short)adaptive_days;
        }
        FLAMEGPU->setVariable<unsigned short>("limiter", limiter);
    }
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 7: Декремент repair_days для repair агентов
# ═══════════════════════════════════════════════════════════════════════════════

RTC_DECREMENT_REPAIR_V8 = """
FLAMEGPU_AGENT_FUNCTION(rtc_decrement_repair_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Декремент repair_days для агентов в repair
    
    auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    const unsigned int adaptive_days = result[0];
    
    if (adaptive_days == 0u) return flamegpu::ALIVE;
    
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    
    if (repair_days > 0u) {
        if (repair_days <= adaptive_days) {
            repair_days = 0u;  // Ремонт завершён!
        } else {
            repair_days -= adaptive_days;
        }
        FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
    }
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 8: Декремент repair_days для unserviceable агентов
# ═══════════════════════════════════════════════════════════════════════════════

RTC_DECREMENT_UNSVC_V8 = """
FLAMEGPU_AGENT_FUNCTION(rtc_decrement_unsvc_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // Декремент repair_days для агентов в unserviceable
    // repair_days = дней до права на вход в operations
    
    auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    const unsigned int adaptive_days = result[0];
    
    if (adaptive_days == 0u) return flamegpu::ALIVE;
    
    unsigned int repair_days = FLAMEGPU->getVariable<unsigned int>("repair_days");
    
    if (repair_days > 0u) {
        if (repair_days <= adaptive_days) {
            repair_days = 0u;  // Получил право на промоут!
        } else {
            repair_days -= adaptive_days;
        }
        FLAMEGPU->setVariable<unsigned int>("repair_days", repair_days);
    }
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 9a: Save adaptive и current_day в агентные переменные (избежание race condition)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_SAVE_ADAPTIVE_V8 = """
FLAMEGPU_AGENT_FUNCTION(rtc_save_adaptive_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: READ adaptive_days и current_day → agent variables
    // Это нужно чтобы избежать read/write конфликта в rtc_update_day
    
    auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    const unsigned int adaptive_days = result[0];
    
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    
    FLAMEGPU->setVariable<unsigned int>("computed_adaptive_days", adaptive_days);
    FLAMEGPU->setVariable<unsigned int>("current_day_cache", current_day);
    
    return flamegpu::ALIVE;
}
"""

# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 9b: Update current_day (ТОЛЬКО WRITE!)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_UPDATE_DAY_V8 = """
FLAMEGPU_AGENT_FUNCTION(rtc_update_day_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V8: READ agent var → WRITE current_day_mp (ТОЛЬКО WRITE!)
    const uint8_t group_by = FLAMEGPU->getVariable<uint8_t>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;
    
    // Читаем из агентных переменных (записаны в rtc_save_adaptive)
    const unsigned int adaptive_days = FLAMEGPU->getVariable<unsigned int>("computed_adaptive_days");
    const unsigned int current_day = FLAMEGPU->getVariable<unsigned int>("current_day_cache");
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    
    if (adaptive_days == 0u) return flamegpu::ALIVE;
    if (current_day >= end_day) return flamegpu::ALIVE;
    
    unsigned int new_day = current_day + adaptive_days;
    if (new_day > end_day) new_day = end_day;
    
    // ТОЛЬКО WRITE в MacroProperty
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    mp_day[1].exchange(current_day);  // prev_day
    mp_day[0].exchange(new_day);      // current_day
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# HostFunction для инициализации
# ═══════════════════════════════════════════════════════════════════════════════

class HF_InitV8(fg.HostFunction):
    """Инициализация MacroProperty для V8"""
    
    def __init__(self, deterministic_dates: list, end_day: int):
        super().__init__()
        self.dates = sorted(set(deterministic_dates))
        self.end_day = end_day
        self.done = False
    
    def run(self, FLAMEGPU):
        if self.done:
            return
        
        # current_day_mp = 0
        mp_day = FLAMEGPU.environment.getMacroPropertyUInt("current_day_mp")
        mp_day[0] = 0
        mp_day[1] = 0
        
        # adaptive_result_mp = 1
        mp_result = FLAMEGPU.environment.getMacroPropertyUInt("adaptive_result_mp")
        mp_result[0] = 1
        
        # min_dynamic_mp = MAX
        mp_min = FLAMEGPU.environment.getMacroPropertyUInt("min_dynamic_mp")
        mp_min[0] = 0xFFFFFFFF
        
        # deterministic_dates_mp
        mp_dates = FLAMEGPU.environment.getMacroPropertyUInt("deterministic_dates_mp")
        for i, d in enumerate(self.dates):
            if i < MAX_DETERMINISTIC_DATES:
                mp_dates[i] = d
        
        # Environment sync
        FLAMEGPU.environment.setPropertyUInt("current_day", 0)
        FLAMEGPU.environment.setPropertyUInt("prev_day", 0)
        FLAMEGPU.environment.setPropertyUInt("adaptive_days", 1)
        
        print(f"  ✅ V8 Init: deterministic_dates={len(self.dates)}, end_day={self.end_day}")
        self.done = True


class HF_SyncDayV8(fg.HostFunction):
    """Синхронизация MacroProperty → Environment + логирование"""
    
    def __init__(self, end_day: int, deterministic_dates: list = None, verbose: bool = False):
        super().__init__()
        self.end_day = end_day
        self.deterministic_dates = set(deterministic_dates) if deterministic_dates else set()
        self.verbose = verbose
        self.step_log = []
        self.dynamic_steps = 0  # Счётчик динамических шагов (limiter=0)
    
    def run(self, FLAMEGPU):
        step = FLAMEGPU.getStepCounter()
        
        # Читаем из MacroProperty
        mp_day = FLAMEGPU.environment.getMacroPropertyUInt("current_day_mp")
        current_day = int(mp_day[0])
        prev_day = int(mp_day[1])
        
        mp_result = FLAMEGPU.environment.getMacroPropertyUInt("adaptive_result_mp")
        adaptive_days = int(mp_result[0])
        
        mp_min = FLAMEGPU.environment.getMacroPropertyUInt("min_dynamic_mp")
        min_dynamic = int(mp_min[0])
        
        # Определяем причины шага
        reasons = []
        
        if current_day == 0:
            reasons.append("day_0")
        
        if current_day >= self.end_day - 1:
            reasons.append("end_day")
        
        # Динамический limiter (ресурс)
        if min_dynamic < 0xFFFFFFFF and min_dynamic == adaptive_days:
            reasons.append(f"limiter:{min_dynamic}")
            self.dynamic_steps += 1
        
        # Детерминированная дата
        if current_day in self.deterministic_dates:
            reasons.append("deterministic")
        
        if not reasons:
            reasons.append(f"dynamic:{adaptive_days}")
        
        self.step_log.append({
            'step': step,
            'day': current_day,
            'prev_day': prev_day,
            'adaptive': adaptive_days,
            'min_dynamic': min_dynamic if min_dynamic < 0xFFFFFFFF else None,
            'reasons': reasons
        })
        
        if self.verbose or step % 50 == 0:
            reason_str = ', '.join(reasons)
            print(f"  [Step {step}] day={current_day}, +{adaptive_days}, причина: {reason_str}")
        
        # Sync Environment
        FLAMEGPU.environment.setPropertyUInt("current_day", current_day)
        FLAMEGPU.environment.setPropertyUInt("prev_day", prev_day)
        FLAMEGPU.environment.setPropertyUInt("adaptive_days", adaptive_days)
    
    def get_step_log(self):
        return self.step_log
    
    def get_dynamic_steps_count(self):
        return self.dynamic_steps


class HF_ExitConditionV8(fg.HostCondition):
    """Exit condition для V8"""
    
    def __init__(self, end_day: int):
        super().__init__()
        self.end_day = end_day
    
    def run(self, FLAMEGPU):
        current_day = FLAMEGPU.environment.getPropertyUInt("current_day")
        if current_day >= self.end_day:
            return fg.EXIT
        return fg.CONTINUE


# ═══════════════════════════════════════════════════════════════════════════════
# Регистрация V8 модулей
# ═══════════════════════════════════════════════════════════════════════════════

def register_v8_layers(model: fg.ModelDescription, heli_agent: fg.AgentDescription,
                       quota_agent: fg.AgentDescription, deterministic_dates: list,
                       end_day: int, verbose: bool = False):
    """Регистрирует V8 слои для adaptive steps"""
    
    print("\n📦 V8: Регистрация слоёв adaptive steps...")
    
    # Init + Sync HostFunctions
    hf_init = HF_InitV8(deterministic_dates, end_day)
    model.addInitFunction(hf_init)
    
    hf_sync = HF_SyncDayV8(end_day, deterministic_dates, verbose=verbose)
    model.addStepFunction(hf_sync)
    
    hf_exit = HF_ExitConditionV8(end_day)
    model.addExitCondition(hf_exit)
    
    # Слой 1: Reset min_dynamic (QuotaManager)
    layer_reset = model.newLayer("v8_reset_min")
    fn = quota_agent.newRTCFunction("rtc_reset_min_dynamic_v8", RTC_RESET_MIN_DYNAMIC)
    layer_reset.addAgentFunction(fn)
    
    # Слой 2: Collect min от operations
    layer_min_ops = model.newLayer("v8_collect_min_ops")
    fn = heli_agent.newRTCFunction("rtc_collect_min_ops_v8", RTC_COLLECT_MIN_OPS)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_min_ops.addAgentFunction(fn)
    
    # Слой 3: Collect min от repair
    layer_min_repair = model.newLayer("v8_collect_min_repair")
    fn = heli_agent.newRTCFunction("rtc_collect_min_repair_v8", RTC_COLLECT_MIN_REPAIR)
    fn.setInitialState("repair")
    fn.setEndState("repair")
    layer_min_repair.addAgentFunction(fn)
    
    # Слой 4: Collect min от unserviceable
    layer_min_unsvc = model.newLayer("v8_collect_min_unsvc")
    fn = heli_agent.newRTCFunction("rtc_collect_min_unsvc_v8", RTC_COLLECT_MIN_UNSVC)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_min_unsvc.addAgentFunction(fn)
    
    # Слой 5: Compute adaptive_days (QuotaManager)
    layer_compute = model.newLayer("v8_compute_adaptive")
    fn = quota_agent.newRTCFunction("rtc_compute_adaptive_v8", RTC_COMPUTE_ADAPTIVE_V8)
    layer_compute.addAgentFunction(fn)
    
    # Слой 6: Decrement ops.limiter
    layer_decr_ops = model.newLayer("v8_decr_ops")
    fn = heli_agent.newRTCFunction("rtc_decrement_ops_v8", RTC_DECREMENT_OPS_V8)
    fn.setInitialState("operations")
    fn.setEndState("operations")
    layer_decr_ops.addAgentFunction(fn)
    
    # Слой 7: Decrement repair.repair_days
    layer_decr_repair = model.newLayer("v8_decr_repair")
    fn = heli_agent.newRTCFunction("rtc_decrement_repair_v8", RTC_DECREMENT_REPAIR_V8)
    fn.setInitialState("repair")
    fn.setEndState("repair")
    layer_decr_repair.addAgentFunction(fn)
    
    # Слой 8: Decrement unsvc.repair_days
    layer_decr_unsvc = model.newLayer("v8_decr_unsvc")
    fn = heli_agent.newRTCFunction("rtc_decrement_unsvc_v8", RTC_DECREMENT_UNSVC_V8)
    fn.setInitialState("unserviceable")
    fn.setEndState("unserviceable")
    layer_decr_unsvc.addAgentFunction(fn)
    
    # Слой 9a: Save adaptive в агентные переменные (QuotaManager)
    layer_save = model.newLayer("v8_save_adaptive")
    fn = quota_agent.newRTCFunction("rtc_save_adaptive_v8", RTC_SAVE_ADAPTIVE_V8)
    layer_save.addAgentFunction(fn)
    
    # Слой 9b: Update day (QuotaManager)
    layer_update = model.newLayer("v8_update_day")
    fn = quota_agent.newRTCFunction("rtc_update_day_v8", RTC_UPDATE_DAY_V8)
    layer_update.addAgentFunction(fn)
    
    print("  ✅ V8 слои зарегистрированы (10 слоёв)")
    
    return hf_init, hf_sync, hf_exit

