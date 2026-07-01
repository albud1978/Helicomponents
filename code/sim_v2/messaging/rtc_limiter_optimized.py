#!/usr/bin/env python3
"""
RTC модуль: Оптимизированный расчёт LIMITER V3

Архитектура ТОЧНОГО расчёта limiter:
- limiter вычисляется ОДИН РАЗ при входе в operations (бинарный поиск по mp5_cumsum)
- На каждом шаге: limiter -= adaptive_days (декремент)
- adaptive_days = min(min_agent_limiter, next_program_change - current_day)

Формула limiter через mp5_cumsum (ТОЧНАЯ):
  limiter = min(days_to_ll, days_to_oh)
  
  где days_to_X = минимальный day такой что:
    mp5_cumsum[day, idx] - mp5_cumsum[current_day, idx] >= remaining_X
    
  Это бинарный поиск: O(log N) где N = end_day - current_day
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import RTC_MAX_FRAMES, MAX_DAYS, MAX_EXPORT_STEPS
from rtc_limiter_v8 import MAX_DETERMINISTIC_DATES

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu не установлен: {e}")


# Константы для RTC
CUMSUM_SIZE = RTC_MAX_FRAMES * (MAX_DAYS + 1)


# ═══════════════════════════════════════════════════════════════════
# RTC: ТОЧНЫЙ расчёт limiter через бинарный поиск по mp5_cumsum
# ═══════════════════════════════════════════════════════════════════
RTC_COMPUTE_LIMITER_ON_ENTRY = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_compute_limiter_on_entry, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // ТОЧНЫЙ расчёт limiter через бинарный поиск по mp5_cumsum
    // 
    // V7 оптимизация: вычисляем ТОЛЬКО для агентов с limiter=0
    // (новые в ops — при переходе X→2 limiter ставится в 0)
    
    const unsigned short current_limiter = FLAMEGPU->getVariable<unsigned short>("limiter");
    
    // Ранний выход: если limiter уже вычислен — не трогаем
    if (current_limiter > 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    
    // ═══════════════════════════════════════════════════════════════════
    // Получаем параметры агента и окружения
    // ═══════════════════════════════════════════════════════════════════
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
    const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
    const unsigned int ll = FLAMEGPU->getVariable<unsigned int>("ll");
    const unsigned int oh = FLAMEGPU->getVariable<unsigned int>("oh");
    
    // ВАЖНО: limiter рассчитываем на день входа в operations (до update_day)
    // current_day в env уже обновлён, поэтому используем prev_day
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("prev_day");
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // ═══════════════════════════════════════════════════════════════════
    // Остаток ресурса (в минутах)
    // ═══════════════════════════════════════════════════════════════════
    unsigned int remaining_ll = (sne < ll) ? (ll - sne) : 0u;
    unsigned int remaining_oh = (ppr < oh) ? (oh - ppr) : 0u;
    
    // Если ресурс уже исчерпан — limiter = 0
    if (remaining_ll == 0u || remaining_oh == 0u) {{
        FLAMEGPU->setVariable<unsigned short>("limiter", 0u);
        return flamegpu::ALIVE;
    }}
    
    // ═══════════════════════════════════════════════════════════════════
    // Доступ к mp5_cumsum MacroProperty
    // ВАЖНО: Используем ту же индексацию что в rtc_state_2_operations:
    //   cumsum[day * frames + idx] — day-major
    // ═══════════════════════════════════════════════════════════════════
    auto cumsum = FLAMEGPU->environment.getMacroProperty<unsigned int, {CUMSUM_SIZE}u>("mp5_cumsum");
    
    // Базовое значение cumsum на текущий день (day-major)
    const unsigned int base_cumsum = cumsum[current_day * frames + idx];
    
    // ═══════════════════════════════════════════════════════════════════
    // Бинарный поиск: найти день когда накопленный налёт >= remaining_oh
    // Логика: найти минимальный day такой что cumsum[day] - cumsum[current] >= remaining
    // ═══════════════════════════════════════════════════════════════════
    unsigned int days_to_oh = end_day - current_day;
    bool found_oh = false;
    {{
        unsigned int lo = current_day + 1u;
        unsigned int hi = end_day;
        while (lo < hi) {{
            unsigned int mid = (lo + hi) / 2u;
            unsigned int accumulated = cumsum[mid * frames + idx] - base_cumsum;
            if (accumulated > remaining_oh) {{
                hi = mid;
            }} else {{
                lo = mid + 1u;
            }}
        }}
        if (lo <= end_day) {{
            unsigned int final_accumulated = cumsum[lo * frames + idx] - base_cumsum;
            if (final_accumulated > remaining_oh) {{
                days_to_oh = (lo - 1u) - current_day;
                found_oh = true;
            }}
        }}
    }}
    if (!found_oh) {{
        days_to_oh = end_day - current_day;
    }}
    
    // ═══════════════════════════════════════════════════════════════════
    // Бинарный поиск: найти день когда накопленный налёт >= remaining_ll
    // ═══════════════════════════════════════════════════════════════════
    unsigned int days_to_ll = end_day - current_day;
    bool found_ll = false;
    {{
        unsigned int lo = current_day + 1u;
        unsigned int hi = end_day;
        while (lo < hi) {{
            unsigned int mid = (lo + hi) / 2u;
            unsigned int accumulated = cumsum[mid * frames + idx] - base_cumsum;
            if (accumulated > remaining_ll) {{
                hi = mid;
            }} else {{
                lo = mid + 1u;
            }}
        }}
        if (lo <= end_day) {{
            unsigned int final_accumulated = cumsum[lo * frames + idx] - base_cumsum;
            if (final_accumulated > remaining_ll) {{
                days_to_ll = (lo - 1u) - current_day;
                found_ll = true;
            }}
        }}
    }}
    if (!found_ll) {{
        days_to_ll = end_day - current_day;
    }}
    
    // ═══════════════════════════════════════════════════════════════════
    // Limiter = min(days_to_oh, days_to_ll)
    // ═══════════════════════════════════════════════════════════════════
    unsigned int limiter = (days_to_oh < days_to_ll) ? days_to_oh : days_to_ll;
    
    // Ограничиваем UInt16 (65535 дней ≈ 179 лет)
    if (limiter > 65535u) limiter = 65535u;
    
    FLAMEGPU->setVariable<unsigned short>("limiter", (unsigned short)limiter);
    
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
        // PERF OFF: printf("  [MIN_LIMITER Step %u] idx=%u: limiter=%u\\n", step, idx, limiter);
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
        // PERF OFF: printf("  [MIN_LIMITER] AC idx=%u wrote limiter=%u to mp_min\\n", idx, limiter);
    }}
    
    return flamegpu::ALIVE;
}}
"""


# ══════════════════════════════════════════════════════════════════
# RTC: V8 StepController on QuotaManager singleton
# ══════════════════════════════════════════════════════════════════
COND_STEP_CONTROLLER_QM_MI8 = """
FLAMEGPU_AGENT_FUNCTION_CONDITION(cond_step_controller_qm_mi8) {
    return FLAMEGPU->getVariable<unsigned char>("group_by") == 1u;
}
"""


RTC_STEP_CONTROLLER_PREPARE_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_step_controller_prepare_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Single-exec guard: only QuotaManager(group_by=1) computes global day advance.
    if (FLAMEGPU->getVariable<unsigned char>("group_by") != 1u) {{
        return flamegpu::ALIVE;
    }}

    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("mp_min_limiter");

    const unsigned int current_day = mp_day[0];
    const unsigned int end_day = FLAMEGPU->environment.getProperty<unsigned int>("end_day");
    const unsigned int min_limiter = mp_min[0];

    if (current_day >= end_day) {{
        FLAMEGPU->setVariable<unsigned int>("step_prev_day", current_day);
        FLAMEGPU->setVariable<unsigned int>("step_new_day", current_day);
        FLAMEGPU->setVariable<unsigned int>("step_adaptive_days", 0u);
        FLAMEGPU->setVariable<unsigned int>("step_min_limiter", 0xFFFFFFFFu);
        return flamegpu::ALIVE;
    }}

    auto mp_dates = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_DETERMINISTIC_DATES}u>("deterministic_dates_mp");
    unsigned int num_dates = FLAMEGPU->environment.getProperty<unsigned int>("num_deterministic_dates");
    if (num_dates > {MAX_DETERMINISTIC_DATES}u) {{
        num_dates = {MAX_DETERMINISTIC_DATES}u;
    }}

    unsigned int days_to_det = {MAX_DAYS}u;
    for (unsigned int i = 0u; i < num_dates; ++i) {{
        const unsigned int det_day = mp_dates[i];
        if (det_day > current_day) {{
            const unsigned int delta = det_day - current_day;
            if (delta < days_to_det) {{
                days_to_det = delta;
            }}
        }}
    }}
    if (days_to_det == {MAX_DAYS}u) {{
        days_to_det = end_day - current_day;
    }}

    unsigned int adaptive_days = (min_limiter == 0xFFFFFFFFu)
        ? days_to_det
        : ((min_limiter < days_to_det) ? min_limiter : days_to_det);

    if (current_day + adaptive_days > end_day) {{
        adaptive_days = end_day - current_day;
    }}
    if (adaptive_days < 1u) {{
        adaptive_days = 1u;
    }}

    const unsigned int prev_day = current_day;
    const unsigned int new_day = current_day + adaptive_days;

    FLAMEGPU->setVariable<unsigned int>("step_prev_day", prev_day);
    FLAMEGPU->setVariable<unsigned int>("step_new_day", new_day);
    FLAMEGPU->setVariable<unsigned int>("step_adaptive_days", adaptive_days);
    FLAMEGPU->setVariable<unsigned int>("step_min_limiter", min_limiter);

    return flamegpu::ALIVE;
}}
"""


RTC_STEP_CONTROLLER_COMMIT_V8 = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_step_controller_commit_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Single-exec guard: only QuotaManager(group_by=1) writes global MacroProperties.
    if (FLAMEGPU->getVariable<unsigned char>("group_by") != 1u) {{
        return flamegpu::ALIVE;
    }}

    const unsigned int prev_day = FLAMEGPU->getVariable<unsigned int>("step_prev_day");
    const unsigned int new_day = FLAMEGPU->getVariable<unsigned int>("step_new_day");
    const unsigned int adaptive_days = FLAMEGPU->getVariable<unsigned int>("step_adaptive_days");
    const unsigned int min_limiter = FLAMEGPU->getVariable<unsigned int>("step_min_limiter");

    auto mp_day = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("current_day_mp");
    auto mp_result = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("adaptive_result_mp");
    auto mp_min = FLAMEGPU->environment.getMacroProperty<unsigned int, 4u>("mp_min_limiter");

    mp_min[0].exchange(0xFFFFFFFFu);
    mp_day[1].exchange(prev_day);
    mp_day[0].exchange(new_day);
    mp_result[0].exchange(adaptive_days);
    mp_result[1].exchange(
        (min_limiter == 0xFFFFFFFFu) ? 0xFFFFFFFFu : (min_limiter << 1u)
    );

    const unsigned int step = FLAMEGPU->getStepCounter();
    if (adaptive_days > 0u && step < {MAX_EXPORT_STEPS}u) {{
        auto mp2_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_EXPORT_STEPS}u>("mp2_day_for_step");
        auto mp2_num = FLAMEGPU->environment.getMacroProperty<unsigned int, 2u>("mp2_num_steps");
        mp2_days[step].exchange(new_day);
        mp2_num[0].exchange(step + 1u);
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
        
        # 3. adaptive_days = min(min_limiter, days_to_pc)
        if min_limiter == 0xFFFFFFFF:
            min_limiter = days_to_pc  # Нет агентов в ops
        
        adaptive_days = min(min_limiter, days_to_pc)
        
        # Не выходим за end_day
        if current_day + adaptive_days > self.end_day:
            adaptive_days = self.end_day - current_day
        
        # Минимум 1 день
        if adaptive_days < 1:
            adaptive_days = 1
        
        # 4. Устанавливаем adaptive_days в Environment
        FLAMEGPU.environment.setPropertyUInt("adaptive_days", adaptive_days)
        
        # 5. Сохраняем prev_day ПЕРЕД обновлением current_day (для cumsum в RTC)
        FLAMEGPU.environment.setPropertyUInt("prev_day", current_day)
        
        # 6. Обновляем current_day
        new_day = current_day + adaptive_days
        FLAMEGPU.environment.setPropertyUInt("current_day", new_day)
        
        step = FLAMEGPU.getStepCounter()
        if step % 50 == 0 or adaptive_days > 30:
            print(f"  [Step {step}] Day {current_day} → {new_day}, adaptive={adaptive_days} (limiter={min_limiter}, pc={days_to_pc})")
        
        return fg.CONTINUE


class HF_StepController(fg.HostFunction):
    """
    V8 StepController:
    adaptive_days = min(mp_min_limiter, days_to_next_deterministic_date)
    current_day += adaptive_days, reset mp_min_limiter
    """
    
    def run(self, FLAMEGPU):
        env = FLAMEGPU.environment
        current_day = int(env.getPropertyUInt("current_day"))
        end_day = int(env.getPropertyUInt("end_day"))
        
        # 1. Читаем min_limiter и сразу сбрасываем MacroProperty
        mp_min = env.getMacroPropertyUInt("mp_min_limiter")
        min_limiter = int(mp_min[0]) & 0xFFFFFFFF  # Ensure unsigned (pyflamegpu может вернуть signed)
        mp_min[0] = 0xFFFFFFFF
        
        # 2. Если уже на end_day — фиксируем и выходим
        if current_day >= end_day:
            mp_day = env.getMacroPropertyUInt("current_day_mp")
            mp_day[1] = current_day
            mp_day[0] = current_day
            mp_result = env.getMacroPropertyUInt("adaptive_result_mp")
            mp_result[0] = 0
            mp_result[1] = 0xFFFFFFFF
            env.setPropertyUInt("prev_day", current_day)
            env.setPropertyUInt("current_day", current_day)
            env.setPropertyUInt("adaptive_days", 0)
            env.setPropertyUInt("step_days", 0)
            return fg.CONTINUE
        
        # 3. Находим ближайшую детерминированную дату
        mp_dates = env.getMacroPropertyUInt("deterministic_dates_mp")
        num_dates = int(env.getPropertyUInt("num_deterministic_dates"))
        if num_dates > len(mp_dates):
            num_dates = len(mp_dates)
        
        days_to_det = MAX_DAYS
        for i in range(num_dates):
            det_day = int(mp_dates[i])
            if det_day > current_day:
                delta = det_day - current_day
                if delta < days_to_det:
                    days_to_det = delta
        
        if days_to_det == MAX_DAYS:
            days_to_det = end_day - current_day
        
        # 4. adaptive_days = min(min_limiter, days_to_det)
        if min_limiter == 0xFFFFFFFF:
            adaptive_days = days_to_det
        else:
            adaptive_days = min(min_limiter, days_to_det)
        
        # Не выходить за end_day
        if current_day + adaptive_days > end_day:
            adaptive_days = end_day - current_day
        
        if adaptive_days < 1:
            adaptive_days = 1
        
        # 5. Обновляем current_day/prev_day
        prev_day = current_day
        new_day = current_day + adaptive_days
        
        mp_day = env.getMacroPropertyUInt("current_day_mp")
        mp_day[1] = prev_day
        mp_day[0] = new_day
        
        mp_result = env.getMacroPropertyUInt("adaptive_result_mp")
        mp_result[0] = adaptive_days
        if min_limiter == 0xFFFFFFFF:
            mp_result[1] = 0xFFFFFFFF
        else:
            mp_result[1] = (min_limiter << 1)
        
        # 6. Синхронизируем в Environment
        env.setPropertyUInt("prev_day", prev_day)
        env.setPropertyUInt("current_day", new_day)
        env.setPropertyUInt("adaptive_days", adaptive_days)
        env.setPropertyUInt("step_days", adaptive_days)
        
        # 7. MP2 Export: записываем маппинг step -> day и счётчик шагов
        step = FLAMEGPU.getStepCounter()
        if step < MAX_EXPORT_STEPS:
            try:
                mp2_days = env.getMacroPropertyUInt("mp2_day_for_step")
                mp2_days[step] = new_day
                mp2_num = env.getMacroPropertyUInt("mp2_num_steps")
                mp2_num[0] = step + 1
            except Exception:
                pass  # MP2 буферы не объявлены (старая конфигурация)
        
        return fg.CONTINUE


def setup_limiter_macroproperties(env, program_changes: list):
    """Настраивает MacroProperty для оптимизированного лимитера"""
    
    # MacroProperty для min reduction
    env.newMacroPropertyUInt("mp_min_limiter", 4)  # 4 элемента для atomic
    
    try:
        env.newPropertyUInt("adaptive_days", 1)  # По умолчанию 1 день
    except:
        pass
    try:
        env.newPropertyUInt("prev_day", 0)  # Предыдущий день для cumsum
    except:
        pass
    
    print("  ✅ Limiter MacroProperty: mp_min_limiter")


def register_device_step_controller(model: fg.ModelDescription, quota_agent: fg.AgentDescription):
    """Register V8 StepController as single-exec prepare/commit device layers."""
    fn_prepare = quota_agent.newRTCFunction(
        "rtc_step_controller_prepare_v8", RTC_STEP_CONTROLLER_PREPARE_V8
    )
    fn_prepare.setRTCFunctionCondition(COND_STEP_CONTROLLER_QM_MI8)
    fn_prepare.setInitialState("default")
    fn_prepare.setEndState("default")

    layer_prepare = model.newLayer("layer_step_controller_prepare")
    layer_prepare.addAgentFunction(fn_prepare)

    fn_commit = quota_agent.newRTCFunction(
        "rtc_step_controller_commit_v8", RTC_STEP_CONTROLLER_COMMIT_V8
    )
    fn_commit.setRTCFunctionCondition(COND_STEP_CONTROLLER_QM_MI8)
    fn_commit.setInitialState("default")
    fn_commit.setEndState("default")

    layer_commit = model.newLayer("layer_step_controller")
    layer_commit.addAgentFunction(fn_commit)
    print("  ✅ V8 RTC StepController зарегистрирован (QuotaManager group_by=1, first layers)")


def register_limiter_optimized(model: fg.ModelDescription, agent: fg.AgentDescription,
                               skip_decrement: bool = False):
    """Регистрирует оптимизированные RTC функции для limiter
    
    Args:
        model: Модель FLAME GPU
        agent: Агент HELI
        skip_decrement: True для V7 (декремент уже в rtc_ops_increment_v7)
    """
    
    # 1. Вычисление limiter при входе в ops (после state managers)
    fn_entry = agent.newRTCFunction("rtc_compute_limiter_on_entry", RTC_COMPUTE_LIMITER_ON_ENTRY)
    fn_entry.setInitialState("operations")
    fn_entry.setEndState("operations")
    layer_entry = model.newLayer("L_limiter_entry")
    layer_entry.addAgentFunction(fn_entry)
    
    # 2. Декремент limiter на каждом шаге
    # V7: пропускаем — декремент уже в rtc_ops_increment_v7 (один проход: sne++, ppr++, limiter--)
    if not skip_decrement:
        fn_decr = agent.newRTCFunction("rtc_decrement_limiter", RTC_DECREMENT_LIMITER)
        fn_decr.setInitialState("operations")
        fn_decr.setEndState("operations")
        layer_decr = model.newLayer("L_limiter_decrement")
        layer_decr.addAgentFunction(fn_decr)
    
    # 3. V7: clear_limiter_on_exit УБРАН — обнуление уже в функциях 2→3, 2→6, 2→7
    
    # 4. Reduction для min
    fn_min = agent.newRTCFunction("rtc_compute_min_limiter", RTC_COMPUTE_ADAPTIVE_DAYS)
    fn_min.setInitialState("operations")
    fn_min.setEndState("operations")
    layer_min = model.newLayer("L_limiter_min")
    layer_min.addAgentFunction(fn_min)
    
    decr_status = "пропущен (V7)" if skip_decrement else "включён"
    print(f"  ✅ Limiter optimized RTC зарегистрирован (decrement: {decr_status})")

