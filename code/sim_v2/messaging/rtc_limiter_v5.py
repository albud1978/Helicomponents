#!/usr/bin/env python3
"""
RTC модуль: LIMITER V5 — 100% GPU-only (как Adaptive 2.0)

АРХИТЕКТУРА V5:
- ✅ current_day в MacroProperty (не Environment)
- ✅ Нет Python HostFunction для adaptive_days
- ✅ Все вычисления на GPU через RTC
- ✅ simulate(N) без exit condition
- ✅ Early return в RTC когда current_day >= end_day

Слои V5:
1. rtc_copy_limiter_to_buffer — operations копируют limiter в буфер
2. rtc_compute_global_min — QuotaManager вычисляет min + adaptive_days
3. [существующие модули V4: quotas, state_managers, spawn]
4. rtc_save_adaptive — READ adaptive_days из MacroProperty
5. rtc_update_current_day — WRITE current_day_mp

Дата: 12.01.2026
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# MacroProperty для V5
# ═══════════════════════════════════════════════════════════════════════════════

def setup_v5_macroproperties(env, program_changes: list):
    """Настраивает MacroProperty для V5 (100% GPU)"""
    
    # current_day в MacroProperty (не Environment!)
    env.newMacroPropertyUInt("current_day_mp", 4)  # [0]=current_day, [1]=prev_day
    
    # adaptive_days результат (вычисляется на GPU)
    env.newMacroPropertyUInt("adaptive_result_mp", 4)  # [0]=adaptive_days, [1]=min_idx
    
    # limiter_buffer для atomicMin
    env.newMacroPropertyUInt("limiter_buffer", RTC_MAX_FRAMES)
    
    # V7: min_exit_date для детерминированных переходов (repair, spawn)
    env.newMacroPropertyUInt("min_exit_date_mp", 4)  # [0]=min_exit_date
    
    # program_changes массив (фиксированный размер для RTC)
    env.newMacroPropertyUInt("program_changes_mp", 150)
    
    # Environment properties (read-only для RTC)
    # end_day уже создан в base_model, устанавливаем num_program_changes
    try:
        env.newPropertyUInt("num_program_changes", len(program_changes))
    except:
        # Property уже существует — обновляем значение (важно для повторных запусков!)
        env.setPropertyUInt("num_program_changes", len(program_changes))
    
    print(f"  ✅ V5 MacroProperty: current_day_mp, adaptive_result_mp, limiter_buffer[{RTC_MAX_FRAMES}], min_exit_date_mp, program_changes_mp[150], num_pc={len(program_changes)}")


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 1: Copy limiter to buffer (operations)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COPY_LIMITER = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_copy_limiter_v5, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V5: Копируем limiter агента в буфер для global min
    
    // Early return если симуляция завершена
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    if (current_day >= end_day) return flamegpu::ALIVE;
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int limiter = FLAMEGPU->getVariable<unsigned short>("limiter");
    
    if (idx < {RTC_MAX_FRAMES}u && limiter > 0u) {{
        auto buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("limiter_buffer");
        buffer[idx].min(limiter);  // atomicMin
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 2: Compute global min (QuotaManager, один агент)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_COMPUTE_GLOBAL_MIN = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_global_min_v5, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V5/V7: QuotaManager вычисляет adaptive_days из min(limiter, exit_date, program_change)
    // ВАЖНО: только агент group_by=1 (Mi-8) выполняет вычисления (избегаем race condition)
    
    const uint8_t group_by = FLAMEGPU->getVariable<uint8_t>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;  // Только один агент вычисляет
    
    // Читаем current_day из MacroProperty
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    
    // Early return если завершено
    if (current_day >= end_day) {{
        auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
        result[0].exchange(0u);
        return flamegpu::ALIVE;
    }}
    
    // 1. Читаем min_limiter из V3 MacroProperty (заполняется rtc_compute_min_limiter)
    auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("mp_min_limiter");
    unsigned int min_limiter = mp_min[0];
    
    // 2. V7: Читаем min_exit_date для детерминированных переходов (repair, spawn)
    auto mp_exit = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("min_exit_date_mp");
    unsigned int min_exit_date = mp_exit[0];
    
    // 3. Находим next program change
    auto pc = FLAMEGPU->environment.getMacroProperty<unsigned int, 150u>("program_changes_mp");
    const unsigned int num_pc = FLAMEGPU->environment.getProperty<unsigned int>("num_program_changes");
    
    // DEBUG: проверка program_changes_mp
    unsigned int dbg_step2 = FLAMEGPU->getStepCounter();
    if (dbg_step2 < 3u) {{
        printf("[RTC PC] step=%u, num_pc=%u, pc[0]=%u, pc[1]=%u, pc[2]=%u\\n",
               dbg_step2, num_pc, (unsigned int)pc[0], (unsigned int)pc[1], (unsigned int)pc[2]);
    }}
    
    unsigned int next_pc = end_day;
    for (unsigned int i = 0u; i < num_pc && i < 150u; ++i) {{
        unsigned int pc_day = pc[i];
        if (pc_day > current_day && pc_day < next_pc) {{
            next_pc = pc_day;
            break;  // Массив отсортирован
        }}
    }}
    
    // 4. adaptive_days = min(min_limiter, days_to_exit, days_to_pc)
    unsigned int days_to_pc = (next_pc > current_day) ? (next_pc - current_day) : 1u;
    
    // V7: days_to_exit_date (если есть агенты в repair/reserve с exit_date)
    unsigned int days_to_exit = 0xFFFFFFFFu;
    if (min_exit_date < 0xFFFFFFFFu && min_exit_date > current_day) {{
        days_to_exit = min_exit_date - current_day;
    }}
    
    // Вычисляем adaptive_days
    unsigned int adaptive_days = days_to_pc;
    
    if (min_limiter < 0xFFFFFFFFu && min_limiter > 0u && min_limiter < adaptive_days) {{
        adaptive_days = min_limiter;
    }}
    
    if (days_to_exit < adaptive_days) {{
        adaptive_days = days_to_exit;
    }}
    
    // Не выходить за end_day
    unsigned int remaining = end_day - current_day;
    if (adaptive_days > remaining) adaptive_days = remaining;
    
    // V8 FIX: Ограничение max adaptive_days до repair_time (180)
    // Это гарантирует что exit_date unsvc агентов не будет перепрыгнут
    const unsigned int max_step = 180u;  // repair_time
    if (adaptive_days > max_step) adaptive_days = max_step;
    
    if (adaptive_days < 1u) adaptive_days = 1u;
    
    // DEBUG: каждые 50 шагов
    unsigned int step = FLAMEGPU->getStepCounter();
    if (step % 50u == 0u || step < 10u) {{
        printf("[V7] step=%u, day=%u, lim=%u, exit=%u, pc=%u -> adaptive=%u\\n",
               step, current_day, min_limiter, min_exit_date, next_pc, adaptive_days);
    }}
    
    // 5. Записываем результат
    auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    result[0].exchange(adaptive_days);
    
    return flamegpu::ALIVE;
}}
"""

# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ 3: Clear limiter buffer (QuotaManager)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_RESET_MIN_LIMITER = """
FLAMEGPU_AGENT_FUNCTION(rtc_reset_min_limiter_v5, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V5: Сброс mp_min_limiter для следующего шага (ТОЛЬКО WRITE)
    // Только один агент (group_by=1) выполняет сброс
    const uint8_t group_by = FLAMEGPU->getVariable<uint8_t>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;
    
    auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("mp_min_limiter");
    mp_min[0].exchange(0xFFFFFFFFu);
    return flamegpu::ALIVE;
}
"""

RTC_CLEAR_LIMITER = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_clear_limiter_v5, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // V5: Сброс limiter_buffer для следующего шага (ТОЛЬКО WRITE)
    // Только один агент (group_by=1) выполняет сброс
    const uint8_t group_by = FLAMEGPU->getVariable<uint8_t>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;
    
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    if (current_day >= end_day) return flamegpu::ALIVE;
    
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    auto buffer = FLAMEGPU->environment.getMacroProperty<unsigned int, {RTC_MAX_FRAMES}u>("limiter_buffer");
    
    for (unsigned int i = 0u; i < frames && i < {RTC_MAX_FRAMES}u; ++i) {{
        buffer[i].exchange(0xFFFFFFFFu);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ N-1: Save adaptive (все агенты)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_SAVE_ADAPTIVE = """
FLAMEGPU_AGENT_FUNCTION(rtc_save_adaptive_v5, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V5: READ adaptive_days из MacroProperty → agent variable (для HELI)
    
    auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    const unsigned int adaptive_days = result[0];
    
    FLAMEGPU->setVariable<unsigned int>("computed_adaptive_days", adaptive_days);
    
    return flamegpu::ALIVE;
}
"""

RTC_SAVE_ADAPTIVE_QM = """
FLAMEGPU_AGENT_FUNCTION(rtc_save_adaptive_v5_qm, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V5: READ adaptive_days и current_day из MacroProperty → agent variables (для QuotaManager)
    // Это нужно чтобы избежать read/write конфликта в rtc_update_day
    
    auto result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    const unsigned int adaptive_days = result[0];
    
    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    const unsigned int current_day = mp_day[0];
    
    // Сохраняем в агентные переменные (для rtc_update_day)
    FLAMEGPU->setVariable<unsigned int>("computed_adaptive_days", adaptive_days);
    FLAMEGPU->setVariable<unsigned int>("current_day_cache", current_day);
    
    return flamegpu::ALIVE;
}
"""


# ═══════════════════════════════════════════════════════════════════════════════
# СЛОЙ N: Update current_day (QuotaManager, один агент)
# ═══════════════════════════════════════════════════════════════════════════════

RTC_UPDATE_DAY = """
FLAMEGPU_AGENT_FUNCTION(rtc_update_day_v5, flamegpu::MessageNone, flamegpu::MessageNone) {
    // V5: READ agent var → WRITE current_day_mp (ТОЛЬКО WRITE!)
    // Только один агент (group_by=1) обновляет день
    const uint8_t group_by = FLAMEGPU->getVariable<uint8_t>("group_by");
    if (group_by != 1u) return flamegpu::ALIVE;
    
    const unsigned int adaptive_days = FLAMEGPU->getVariable<unsigned int>("computed_adaptive_days");
    // current_day из агентной переменной (записана в rtc_save_adaptive)
    const unsigned int current_day = FLAMEGPU->getVariable<unsigned int>("current_day_cache");
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    
    if (adaptive_days == 0u) return flamegpu::ALIVE;
    if (current_day >= end_day) return flamegpu::ALIVE;
    
    // Вычисляем новый день
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
# HostFunction для инициализации (только один раз!)
# ═══════════════════════════════════════════════════════════════════════════════

class HF_ExitCondition(fg.HostCondition):
    """Exit condition: остановить симуляцию когда current_day >= end_day"""
    
    def __init__(self, end_day: int):
        super().__init__()
        self.end_day = end_day
    
    def run(self, FLAMEGPU):
        current_day = FLAMEGPU.environment.getPropertyUInt("current_day")
        if current_day >= self.end_day:
            return fg.EXIT
        return fg.CONTINUE


class HF_InitV5(fg.HostFunction):
    """Инициализация MacroProperty для V5 (один раз)"""
    
    def __init__(self, program_changes: list, end_day: int):
        super().__init__()
        self.program_changes = sorted(program_changes)
        self.end_day = end_day
        self.done = False
    
    def run(self, FLAMEGPU):
        if self.done:
            return
        
        # Инициализация current_day_mp = 0
        mp_day = FLAMEGPU.environment.getMacroPropertyUInt("current_day_mp")
        mp_day[0] = 0
        mp_day[1] = 0
        
        # Инициализация adaptive_result_mp
        mp_result = FLAMEGPU.environment.getMacroPropertyUInt("adaptive_result_mp")
        mp_result[0] = 1
        
        # Инициализация limiter_buffer = MAX
        buffer = FLAMEGPU.environment.getMacroPropertyUInt("limiter_buffer")
        for i in range(RTC_MAX_FRAMES):
            buffer[i] = 0xFFFFFFFF
        
        # Инициализация program_changes_mp
        pc_mp = FLAMEGPU.environment.getMacroPropertyUInt("program_changes_mp")
        for i, day in enumerate(self.program_changes):
            if i < 150:
                pc_mp[i] = day
        
        # Синхронизация в Environment для первого шага
        FLAMEGPU.environment.setPropertyUInt("current_day", 0)
        FLAMEGPU.environment.setPropertyUInt("prev_day", 0)
        FLAMEGPU.environment.setPropertyUInt("adaptive_days", 1)
        FLAMEGPU.environment.setPropertyUInt("step_days", 1)
        
        print(f"  ✅ V5 Init: current_day=0, program_changes={len(self.program_changes)}")
        self.done = True


class HF_SyncDayV5(fg.HostFunction):
    """
    Лёгкий sync: MacroProperty → Environment + логирование причин шагов
    
    Запускается в НАЧАЛЕ каждого шага чтобы существующие RTC модули
    могли читать current_day/prev_day из Environment.
    """
    
    def __init__(self, end_day: int, program_changes: list = None, verbose: bool = False):
        super().__init__()
        self.end_day = end_day
        # program_changes может быть списком tuples (day, mi8, mi17) или просто дней
        if program_changes:
            if isinstance(program_changes[0], tuple):
                self.program_changes = set(d for d, m8, m17 in program_changes)
            else:
                self.program_changes = set(program_changes)
        else:
            self.program_changes = set()
        self.verbose = verbose
        self.step_log = []  # Лог шагов: [(step, day, adaptive, причины)]
    
    def run(self, FLAMEGPU):
        step = FLAMEGPU.getStepCounter()
        
        # Читаем из MacroProperty
        mp_day = FLAMEGPU.environment.getMacroPropertyUInt("current_day_mp")
        current_day = int(mp_day[0])
        prev_day = int(mp_day[1])
        
        
        mp_result = FLAMEGPU.environment.getMacroPropertyUInt("adaptive_result_mp")
        adaptive_days = int(mp_result[0])
        
        # Читаем источники для определения причины шага
        mp_min = FLAMEGPU.environment.getMacroPropertyUInt("mp_min_limiter")
        min_limiter = int(mp_min[0])
        
        mp_exit = FLAMEGPU.environment.getMacroPropertyUInt("min_exit_date_mp")
        min_exit_date = int(mp_exit[0])
        
        # Определяем ВСЕ причины текущего шага
        reasons = []
        
        # День 0
        if current_day == 0:
            reasons.append("day_0")
        
        # Последний день (end_day - 1, т.к. индексация с 0)
        if current_day >= self.end_day - 1:
            reasons.append("end_day")
        
        # Limiter (ресурс LL/OH) — если limiter == adaptive_days
        if min_limiter < 0xFFFFFFFF and min_limiter == adaptive_days:
            reasons.append(f"limiter:{min_limiter}")
        
        # Exit date (repair/spawn/unserviceable)
        if min_exit_date < 0xFFFFFFFF:
            days_to_exit = min_exit_date - prev_day if min_exit_date > prev_day else 0
            if days_to_exit == adaptive_days:
                reasons.append(f"exit_date:{min_exit_date}")
        
        # Program change — если текущий день в списке
        if current_day in self.program_changes:
            reasons.append("program_change")
        
        # Если причина не определена — это limiter (ресурс агента)
        if not reasons:
            reasons.append(f"resource:{adaptive_days}")
        
        # Записываем в лог
        self.step_log.append({
            'step': step,
            'day': current_day,
            'prev_day': prev_day,
            'adaptive': adaptive_days,
            'limiter': min_limiter if min_limiter < 0xFFFFFFFF else None,
            'exit_date': min_exit_date if min_exit_date < 0xFFFFFFFF else None,
            'reasons': reasons
        })
        
        if self.verbose or step % 50 == 0:
            reason_str = ', '.join(reasons)
            print(f"  [Step {step}] day={current_day}, +{adaptive_days}, причина: {reason_str}")
        
        # Синхронизируем в Environment (для существующих RTC модулей)
        FLAMEGPU.environment.setPropertyUInt("current_day", current_day)
        FLAMEGPU.environment.setPropertyUInt("prev_day", prev_day)
        FLAMEGPU.environment.setPropertyUInt("adaptive_days", adaptive_days)
        FLAMEGPU.environment.setPropertyUInt("step_days", adaptive_days)
    
    def get_step_log(self):
        return self.step_log


# ═══════════════════════════════════════════════════════════════════════════════
# Регистрация V5 модулей
# ═══════════════════════════════════════════════════════════════════════════════

def register_v5(model: fg.ModelDescription, heli_agent: fg.AgentDescription, 
                quota_agent: fg.AgentDescription, program_changes: list, end_day: int,
                verbose_logging: bool = False):
    """Регистрирует V5 RTC модули"""
    
    # Init HostFunction (один раз при старте)
    hf_init = HF_InitV5(program_changes, end_day)
    model.addInitFunction(hf_init)
    
    # Step sync — единственный Python callback в step loop
    # Синхронизирует MacroProperty → Environment + логирование причин шагов
    hf_sync = HF_SyncDayV5(end_day, program_changes, verbose=verbose_logging)
    model.addStepFunction(hf_sync)
    
    # V5 использует mp_min_limiter от V3 RTC (rtc_compute_min_limiter)
    # Не регистрируем copy/clear — V3 уже делает это
    
    # Только compute global min (после V3 limiter слоёв)
    # Эта функция регистрируется в register_v5_final_layers
    
    print(f"  ✅ V5 Init + Sync зарегистрированы")
    
    return hf_init, hf_sync


def register_v5_final_layers(model: fg.ModelDescription, heli_agent: fg.AgentDescription,
                             quota_agent: fg.AgentDescription):
    """Регистрирует финальные слои V5 (после V3 limiter модулей)"""
    
    # Слой N-3: Compute adaptive_days (ТОЛЬКО READ mp_min_limiter, WRITE adaptive_result_mp)
    fn_compute = quota_agent.newRTCFunction("rtc_compute_global_min_v5", RTC_COMPUTE_GLOBAL_MIN)
    layer_compute = model.newLayer("L_v5_compute_adaptive")
    layer_compute.addAgentFunction(fn_compute)
    
    # Слой N-2: Reset mp_min_limiter (ТОЛЬКО WRITE)
    fn_reset = quota_agent.newRTCFunction("rtc_reset_min_limiter_v5", RTC_RESET_MIN_LIMITER)
    layer_reset = model.newLayer("L_v5_reset_limiter")
    layer_reset.addAgentFunction(fn_reset)
    
    # Слой N-1: Save adaptive (все состояния HELI + QuotaManager)
    states = ["inactive", "operations", "serviceable", "unserviceable", "reserve", "storage"]
    
    layer_save = model.newLayer("L_v5_save_adaptive")
    
    # HELI агенты
    for state in states:
        fn_save = heli_agent.newRTCFunction(f"rtc_save_adaptive_v5_{state}", RTC_SAVE_ADAPTIVE)
        fn_save.setInitialState(state)
        fn_save.setEndState(state)
        layer_save.addAgentFunction(fn_save)
    
    # QuotaManager тоже нужен для rtc_update_day
    fn_save_qm = quota_agent.newRTCFunction("rtc_save_adaptive_v5_qm", RTC_SAVE_ADAPTIVE_QM)
    layer_save.addAgentFunction(fn_save_qm)
    
    # Слой N: Update day (QuotaManager, без states)
    fn_update = quota_agent.newRTCFunction("rtc_update_day_v5", RTC_UPDATE_DAY)
    # QuotaManager не использует state machine
    
    layer_update = model.newLayer("L_v5_update_day")
    layer_update.addAgentFunction(fn_update)
    
    print(f"  ✅ V5 финальные слои: save_adaptive (6 состояний) + update_day")

