#!/usr/bin/env python3
"""
RTC функция: rtc_quota_approve
Одобрение квот менеджером (параметризованно для статусов 1,2,3,5)
Основано на коде из бэкапов
Дата: 2025-09-12
"""

from rtc import BaseRTC


class QuotaApproveRTC(BaseRTC):
    """RTC функция для одобрения квот менеджером"""
    
    NAME = "rtc_quota_approve"
    DEPENDENCIES = ["rtc_quota_intent"]
    
    @staticmethod
    def get_source(frames: int, days: int, **kwargs) -> str:
        """Возвращает исходный код rtc_quota_approve для конкретного статуса"""
        
        status_id = kwargs.get('status_id', 2)
        func_name = f"rtc_quota_approve_manager_s{status_id}"
        
        if status_id == 2:
            # Первая фаза: используем полную квоту
            return f"""
FLAMEGPU_AGENT_FUNCTION({func_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    
    static const unsigned int FRAMES = {frames}u;
    static const unsigned int DAYS = {days}u;
    
    // Только менеджер (idx==0) выполняет распределение
    if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int last = (DAYS > 0u ? DAYS - 1u : 0u);
    const unsigned int dayp1 = (day < last ? day + 1u : last);
    
    // Получаем семена квоты на D+1
    unsigned int seed8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
    unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
    
    // Получаем буферы
    auto i8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
    auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent");
    auto a8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve");
    auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve");
    
    // Очищаем approve буферы
    for (unsigned int k=0u; k<FRAMES; ++k) {{
        a8[k].exchange(0u);
        a17[k].exchange(0u);
    }}
    
    // Распределяем квоту по порядку idx
    unsigned int left8 = seed8;
    unsigned int left17 = seed17;
    
    for (unsigned int k=0u; k<FRAMES && left8>0u; ++k) {{
        if (i8[k]) {{
            a8[k].exchange(1u);
            --left8;
        }}
    }}
    for (unsigned int k=0u; k<FRAMES && left17>0u; ++k) {{
        if (i17[k]) {{
            a17[k].exchange(1u);
            --left17;
        }}
    }}
    
    return flamegpu::ALIVE;
}}
            """
        
        elif status_id == 3:
            # Вторая фаза: остаток после статуса 2
            return f"""
FLAMEGPU_AGENT_FUNCTION({func_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    
    static const unsigned int FRAMES = {frames}u;
    static const unsigned int DAYS = {days}u;
    
    if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int last = (DAYS > 0u ? DAYS - 1u : 0u);
    const unsigned int dayp1 = (day < last ? day + 1u : last);
    
    unsigned int seed8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
    unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
    
    auto i8   = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
    auto i17  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent");
    auto a8   = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve");
    auto a17  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve");
    auto a8b  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve_s3");
    auto a17b = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve_s3");
    
    // Подсчет использованной квоты в фазе статуса 2
    unsigned int used8 = 0u, used17 = 0u;
    for (unsigned int k=0u; k<FRAMES; ++k) {{
        if (a8[k]) ++used8;
        if (a17[k]) ++used17;
    }}
    
    unsigned int left8 = (seed8 > used8 ? (seed8 - used8) : 0u);
    unsigned int left17 = (seed17 > used17 ? (seed17 - used17) : 0u);
    
    // Очищаем буферы статуса 3
    for (unsigned int k=0u; k<FRAMES; ++k) {{
        a8b[k].exchange(0u);
        a17b[k].exchange(0u);
    }}
    
    // Распределяем остаток
    for (unsigned int k=0u; k<FRAMES && left8>0u; ++k) {{
        if (i8[k]) {{
            a8b[k].exchange(1u);
            --left8;
        }}
    }}
    for (unsigned int k=0u; k<FRAMES && left17>0u; ++k) {{
        if (i17[k]) {{
            a17b[k].exchange(1u);
            --left17;
        }}
    }}
    
    return flamegpu::ALIVE;
}}
            """
        
        elif status_id == 5:
            # Третья фаза: остаток после статусов 2 и 3
            return f"""
FLAMEGPU_AGENT_FUNCTION({func_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    
    static const unsigned int FRAMES = {frames}u;
    static const unsigned int DAYS = {days}u;
    
    if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int last = (DAYS > 0u ? DAYS - 1u : 0u);
    const unsigned int dayp1 = (day < last ? day + 1u : last);
    
    unsigned int seed8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
    unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
    
    auto i8   = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
    auto i17  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent");
    auto a8   = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve");
    auto a17  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve");
    auto a8b  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve_s3");
    auto a17b = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve_s3");
    auto a8c  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve_s5");
    auto a17c = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve_s5");
    
    // Подсчет использованной квоты в фазах 2 и 3
    unsigned int used8 = 0u, used17 = 0u;
    for (unsigned int k=0u; k<FRAMES; ++k) {{
        if (a8[k] || a8b[k]) ++used8;
        if (a17[k] || a17b[k]) ++used17;
    }}
    
    unsigned int left8 = (seed8 > used8 ? (seed8 - used8) : 0u);
    unsigned int left17 = (seed17 > used17 ? (seed17 - used17) : 0u);
    
    // Очищаем буферы статуса 5
    for (unsigned int k=0u; k<FRAMES; ++k) {{
        a8c[k].exchange(0u);
        a17c[k].exchange(0u);
    }}
    
    // Распределяем остаток
    for (unsigned int k=0u; k<FRAMES && left8>0u; ++k) {{
        if (i8[k]) {{
            a8c[k].exchange(1u);
            --left8;
        }}
    }}
    for (unsigned int k=0u; k<FRAMES && left17>0u; ++k) {{
        if (i17[k]) {{
            a17c[k].exchange(1u);
            --left17;
        }}
    }}
    
    return flamegpu::ALIVE;
}}
            """
        
        elif status_id == 1:
            # Четвертая фаза: остаток после статусов 2,3,5 с приоритизацией по mfg_date
            return f"""
FLAMEGPU_AGENT_FUNCTION({func_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    
    static const unsigned int FRAMES = {frames}u;
    static const unsigned int DAYS = {days}u;
    
    if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int last = (DAYS > 0u ? DAYS - 1u : 0u);
    const unsigned int dayp1 = (day < last ? day + 1u : last);
    
    unsigned int seed8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
    unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
    
    auto i8   = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
    auto i17  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent");
    auto a8   = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve");
    auto a17  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve");
    auto a8b  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve_s3");
    auto a17b = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve_s3");
    auto a8c  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve_s5");
    auto a17c = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve_s5");
    auto a8d  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve_s1");
    auto a17d = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve_s1");
    
    // Подсчет использованной квоты в фазах 2, 3 и 5
    unsigned int used8 = 0u, used17 = 0u;
    for (unsigned int k=0u; k<FRAMES; ++k) {{
        if (a8[k] || a8b[k] || a8c[k]) ++used8;
        if (a17[k] || a17b[k] || a17c[k]) ++used17;
    }}
    
    unsigned int left8 = (seed8 > used8 ? (seed8 - used8) : 0u);
    unsigned int left17 = (seed17 > used17 ? (seed17 - used17) : 0u);
    
    // Очищаем буферы статуса 1
    for (unsigned int k=0u; k<FRAMES; ++k) {{
        a8d[k].exchange(0u);
        a17d[k].exchange(0u);
    }}
    
    // Приоритизация по mfg_date (самые молодые первыми)
    if (left8 > 0u) {{
        bool picked8[FRAMES];
        for (unsigned int k=0u; k<FRAMES; ++k) picked8[k] = false;
        while (left8 > 0u) {{
            unsigned int best_idx = FRAMES;
            unsigned int best_mfg = 0u;
            for (unsigned int k=0u; k<FRAMES; ++k) {{
                if (picked8[k]) continue;
                if (i8[k]) {{
                    const unsigned int mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", k);
                    if (mfg >= best_mfg) {{ best_mfg = mfg; best_idx = k; }}
                }}
            }}
            if (best_idx < FRAMES) {{
                a8d[best_idx].exchange(1u);
                picked8[best_idx] = true;
                --left8;
            }} else break;
        }}
    }}
    
    if (left17 > 0u) {{
        bool picked17[FRAMES];
        for (unsigned int k=0u; k<FRAMES; ++k) picked17[k] = false;
        while (left17 > 0u) {{
            unsigned int best_idx = FRAMES;
            unsigned int best_mfg = 0u;
            for (unsigned int k=0u; k<FRAMES; ++k) {{
                if (picked17[k]) continue;
                if (i17[k]) {{
                    const unsigned int mfg = FLAMEGPU->environment.getProperty<unsigned int>("mp3_mfg_date_days", k);
                    if (mfg >= best_mfg) {{ best_mfg = mfg; best_idx = k; }}
                }}
            }}
            if (best_idx < FRAMES) {{
                a17d[best_idx].exchange(1u);
                picked17[best_idx] = true;
                --left17;
            }} else break;
        }}
    }}
    
    return flamegpu::ALIVE;
}}
            """
        
        else:
            # Для статусов 3 и 5: простое распределение остатка
            approve_buffer = f"mi{8 if status_id == 3 else (8 if status_id == 5 else 8)}_approve_s{status_id}"
            approve17_buffer = f"mi17_approve_s{status_id}"
            
            return f"""
FLAMEGPU_AGENT_FUNCTION({func_name}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    if (FLAMEGPU->environment.getProperty<unsigned int>("export_phase") != 0u) return flamegpu::ALIVE;
    
    static const unsigned int FRAMES = {frames}u;
    static const unsigned int DAYS = {days}u;
    
    if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
    
    const unsigned int day = FLAMEGPU->getStepCounter();
    const unsigned int last = (DAYS > 0u ? DAYS - 1u : 0u);
    const unsigned int dayp1 = (day < last ? day + 1u : last);
    
    unsigned int seed8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
    unsigned int seed17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
    
    auto i8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
    auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent");
    auto a8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve");
    auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve");
    auto a8b = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve_s3");
    auto a17b = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve_s3");
    auto a8c = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("{approve_buffer}");
    auto a17c = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("{approve17_buffer}");
    
    // Подсчет уже использованной квоты
    unsigned int used8 = 0u, used17 = 0u;
    for (unsigned int k=0u; k<FRAMES; ++k) {{
        if (a8[k]) ++used8;
        if (a17[k]) ++used17;
        if ({status_id} > 3 && (a8b[k])) ++used8;
        if ({status_id} > 3 && (a17b[k])) ++used17;
    }}
    
    unsigned int left8 = (seed8 > used8 ? (seed8 - used8) : 0u);
    unsigned int left17 = (seed17 > used17 ? (seed17 - used17) : 0u);
    
    // Очищаем текущие буферы
    for (unsigned int k=0u; k<FRAMES; ++k) {{
        a8c[k].exchange(0u);
        a17c[k].exchange(0u);
    }}
    
    // Распределяем остаток
    for (unsigned int k=0u; k<FRAMES && left8>0u; ++k) {{
        if (i8[k]) {{
            a8c[k].exchange(1u);
            --left8;
        }}
    }}
    for (unsigned int k=0u; k<FRAMES && left17>0u; ++k) {{
        if (i17[k]) {{
            a17c[k].exchange(1u);
            --left17;
        }}
    }}
    
    return flamegpu::ALIVE;
}}
            """
    
    @staticmethod
    def get_source_for_status(status_id: int, frames: int, days: int) -> str:
        """Удобный метод для генерации кода под конкретный статус"""
        return QuotaApproveRTC.get_source(frames, days, status_id=status_id)