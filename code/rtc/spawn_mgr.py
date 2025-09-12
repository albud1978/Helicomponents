#!/usr/bin/env python3
"""
RTC функция: rtc_spawn_mgr
Менеджер спавна новых агентов MI-17
Основано на архитектуре из GPUarc.md
Дата: 2025-09-12
"""

from rtc import BaseRTC


class SpawnMgrRTC(BaseRTC):
    """RTC функция для управления спавном агентов"""
    
    NAME = "rtc_spawn_mgr"
    DEPENDENCIES = ["rtc_log_day"]
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код rtc_spawn_mgr"""
        
        return f"""
FLAMEGPU_AGENT_FUNCTION(rtc_spawn_mgr, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    
    // Только один менеджер спавна выполняет подготовку
    if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    static const unsigned int DAYS = {days}u;
    
    if (day >= DAYS) return flamegpu::ALIVE;
    
    // Читаем план рождения на сегодня
    const unsigned int need = FLAMEGPU->environment.getProperty<unsigned int>("mp4_new_counter_mi17_seed", day);
    const unsigned int frames_total = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // Ограничиваем по доступной мощности кадров
    auto next_idx_spawn = FLAMEGPU->environment.getMacroProperty<unsigned int>("next_idx_spawn");
    const unsigned int current_idx = next_idx_spawn[0];
    const unsigned int available = (frames_total > current_idx ? (frames_total - current_idx) : 0u);
    const unsigned int actual_need = (need < available ? need : available);
    
    // Публикуем параметры спавна в MacroProperty
    auto spawn_need = FLAMEGPU->environment.getMacroProperty<unsigned int>("spawn_need_u32");
    auto spawn_base_idx = FLAMEGPU->environment.getMacroProperty<unsigned int>("spawn_base_idx_u32");
    auto spawn_base_acn = FLAMEGPU->environment.getMacroProperty<unsigned int>("spawn_base_acn_u32");
    auto spawn_base_psn = FLAMEGPU->environment.getMacroProperty<unsigned int>("spawn_base_psn_u32");
    
    spawn_need[0].exchange(actual_need);
    spawn_base_idx[0].exchange(current_idx);
    
    // Генерируем базовые ID для новорожденных
    auto next_acn = FLAMEGPU->environment.getMacroProperty<unsigned int>("next_aircraft_no_mi17");
    auto next_psn = FLAMEGPU->environment.getMacroProperty<unsigned int>("next_psn_mi17");
    
    spawn_base_acn[0].exchange(next_acn[0]);
    spawn_base_psn[0].exchange(next_psn[0]);
    
    // Обновляем счетчики для следующего дня
    next_idx_spawn[0].exchange(current_idx + actual_need);
    next_acn[0].exchange(next_acn[0] + actual_need);
    next_psn[0].exchange(next_psn[0] + actual_need);
    
    return flamegpu::ALIVE;
}}
        """


