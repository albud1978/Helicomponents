"""
RTC модуль для записи в MP2 (device-side export) - ПОЛНАЯ ВЕРСИЯ
Все 27 агентных переменных логируются в СУБД
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import model_build

import pyflamegpu as fg

def register_rtc(model: fg.ModelDescription, agent: fg.AgentDescription):
    """Регистрирует RTC модуль mp2_writer (совместимо с orchestrator_v2)"""
    register_mp2_writer(model, agent)

def register_mp2_writer(model: fg.ModelDescription, agent: fg.AgentDescription, clickhouse_client=None):
    """Регистрирует RTC функции для записи в MP2 и host функцию для дренажа"""
    
    # ФИКСИРОВАННЫЕ размеры для RTC кэширования
    MAX_FRAMES = model_build.RTC_MAX_FRAMES
    MAX_DAYS = model_build.MAX_DAYS
    MP2_SIZE = MAX_FRAMES * (MAX_DAYS + 1)  # Плотная матрица с D+1 паддингом
    
    # Проверяем, был ли уже вызван register_mp2_writer (например, если mp2_writer в списке модулей)
    # Если mp2_day_u16 уже существует, то выходим
    try:
        model.Environment().getMacroPropertyDescription("mp2_day_u16")
        print("  ⚠️  MP2 MacroProperties уже зарегистрированы, пропускаем")
        return None
    except:
        pass  # MacroProperty не существует, продолжаем создание
    
    # Обёртываем создание MacroProperties в try-except для безопасности
    try:
        # Основные колонки MP2 (ВСЕ агентные переменные)
        model.Environment().newMacroPropertyUInt("mp2_day_u16", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_idx", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_aircraft_number", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_partseqno", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_group_by", MP2_SIZE)
        
        model.Environment().newMacroPropertyUInt("mp2_state", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_intent_state", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_bi_counter", MP2_SIZE)  # Служебное поле для BI (всегда 1)
        
        model.Environment().newMacroPropertyUInt("mp2_sne", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_ppr", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_cso", MP2_SIZE)
        
        model.Environment().newMacroPropertyUInt("mp2_ll", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_oh", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_br", MP2_SIZE)
        
        model.Environment().newMacroPropertyUInt("mp2_repair_time", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_assembly_time", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_partout_time", MP2_SIZE)
        
        model.Environment().newMacroPropertyUInt("mp2_repair_days", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_s4_days", MP2_SIZE)  # Счётчик дней в repair+reserve
        model.Environment().newMacroPropertyUInt("mp2_assembly_trigger", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_active_trigger", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_partout_trigger", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_mfg_date_days", MP2_SIZE)
        
        model.Environment().newMacroPropertyUInt("mp2_dt", MP2_SIZE)
        model.Environment().newMacroPropertyUInt("mp2_dn", MP2_SIZE)
        
        # ops_ticket удален (никогда не устанавливается)
        # model.Environment().newMacroPropertyUInt("mp2_ops_ticket", MP2_SIZE)
        
        # Буфер событий
        MP2_EVENT_SIZE = 10000
        model.Environment().newMacroPropertyUInt("mp2_event_counter")  # Атомарный счетчик (скаляр)
        model.Environment().newMacroPropertyUInt("event_day", MP2_EVENT_SIZE)
        model.Environment().newMacroPropertyUInt("event_idx", MP2_EVENT_SIZE)
        model.Environment().newMacroPropertyUInt("event_type", MP2_EVENT_SIZE)
        model.Environment().newMacroPropertyUInt("event_from_state", MP2_EVENT_SIZE)
        model.Environment().newMacroPropertyUInt("event_to_state", MP2_EVENT_SIZE)
        model.Environment().newMacroPropertyUInt("event_value1", MP2_EVENT_SIZE)
        model.Environment().newMacroPropertyUInt("event_value2", MP2_EVENT_SIZE)
        
        # Квотирование (отладка)
        model.Environment().newMacroPropertyUInt32("mp2_quota_curr_ops", MP2_SIZE)    # Текущее кол-во в operations
        model.Environment().newMacroPropertyUInt32("mp2_quota_target_ops", MP2_SIZE)  # Целевое кол-во
        model.Environment().newMacroPropertyUInt32("mp2_quota_svc_count", MP2_SIZE)   # Кол-во в serviceable
        model.Environment().newMacroPropertyInt("mp2_quota_deficit", MP2_SIZE)      # Дефицит (может быть <0)
        
        # Флаги квотирования (per-agent per-day)
        model.Environment().newMacroPropertyUInt("mp2_quota_demount", MP2_SIZE)      # Флаг демоута (1 бит)
        model.Environment().newMacroPropertyUInt("mp2_quota_promote_p1", MP2_SIZE)   # Флаг промоута P1 (serviceable)
        model.Environment().newMacroPropertyUInt("mp2_quota_promote_p2", MP2_SIZE)   # Флаг промоута P2 (reserve)
        model.Environment().newMacroPropertyUInt("mp2_quota_promote_p3", MP2_SIZE)   # Флаг промоута P3 (inactive)
        
        # MP4 целевые значения (для верификации логики квот)
        # Note: эти буферы создаются в rtc_quota_count_ops.py как mp2_mp4_target_mi8/mi17

        # Флаги переходов между состояниями (вычисляются GPU post-processing слоем)
        model.Environment().newMacroPropertyUInt("mp2_transition_0_to_2", MP2_SIZE)   # spawn → operations (динамический)
        model.Environment().newMacroPropertyUInt("mp2_transition_0_to_3", MP2_SIZE)   # spawn → serviceable (детерминированный)
        model.Environment().newMacroPropertyUInt("mp2_transition_2_to_4", MP2_SIZE)   # operations → repair
        model.Environment().newMacroPropertyUInt("mp2_transition_2_to_6", MP2_SIZE)   # operations → storage
        model.Environment().newMacroPropertyUInt("mp2_transition_2_to_3", MP2_SIZE)   # operations → serviceable
        model.Environment().newMacroPropertyUInt("mp2_transition_3_to_2", MP2_SIZE)   # serviceable → operations
        model.Environment().newMacroPropertyUInt("mp2_transition_5_to_2", MP2_SIZE)   # reserve → operations
        model.Environment().newMacroPropertyUInt("mp2_transition_1_to_2", MP2_SIZE)   # inactive → operations
        model.Environment().newMacroPropertyUInt("mp2_transition_4_to_5", MP2_SIZE)   # repair → reserve
        model.Environment().newMacroPropertyUInt("mp2_transition_1_to_4", MP2_SIZE)   # inactive → repair
        model.Environment().newMacroPropertyUInt("mp2_transition_4_to_2", MP2_SIZE)   # repair → operations
        print("  ✅ Transition MacroProperty созданы для MP2")

        # inactive (state=1)
        rtc_write_inactive = agent.newRTCFunction("rtc_mp2_write_inactive", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_inactive, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_bi_counter = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_bi_counter");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_s4_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s4_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange(1u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_bi_counter[pos].exchange(1u);  // Служебное поле для BI (всегда 1)
    
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_cso[pos].exchange(FLAMEGPU->getVariable<unsigned int>("cso"));
    
    mp2_ll[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ll"));
    mp2_oh[pos].exchange(FLAMEGPU->getVariable<unsigned int>("oh"));
    mp2_br[pos].exchange(FLAMEGPU->getVariable<unsigned int>("br"));
    
    mp2_repair_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_time"));
    mp2_assembly_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_time"));
    mp2_partout_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_time"));
    
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_s4_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s4_days"));
    mp2_assembly_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_trigger"));
    mp2_active_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("active_trigger"));
    mp2_partout_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_trigger"));
    mp2_mfg_date[pos].exchange(FLAMEGPU->getVariable<unsigned int>("mfg_date"));
    
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    
    // Квотирование - читаем значения заполненные в слое count_ops
    auto mp2_quota_curr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_curr_ops");
    auto mp2_quota_target = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_target_ops");
    auto mp2_quota_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_svc_count");
    auto mp2_quota_deficit = FLAMEGPU->environment.getMacroProperty<int, {MP2_SIZE}u>("mp2_quota_deficit");
    
    // Просто копируем значения (или 0 если не заполнено)
    unsigned int quota_curr = mp2_quota_curr[pos];
    unsigned int quota_target = mp2_quota_target[pos];
    unsigned int quota_svc = mp2_quota_svc[pos];
    int quota_deficit = mp2_quota_deficit[pos];
    
    // ═══════════════════════════════════════════════════════════════════
    // Логирование флагов квотирования
    // ═══════════════════════════════════════════════════════════════════
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // Читаем флаги из MacroProperty буферов
    unsigned int demount_flag = 0u;
    unsigned int promote_p1_flag = 0u;
    unsigned int promote_p2_flag = 0u;
    unsigned int promote_p3_flag = 0u;
    
    if (group_by == 1u) {{
        auto mi8_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
        auto mi8_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s3");
        auto mi8_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s5");
        auto mi8_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s1");
        
        demount_flag = mi8_approve[idx];
        promote_p1_flag = mi8_approve_s3[idx];
        promote_p2_flag = mi8_approve_s5[idx];
        promote_p3_flag = mi8_approve_s1[idx];
    }} else if (group_by == 2u) {{
        auto mi17_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
        auto mi17_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s3");
        auto mi17_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s5");
        auto mi17_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s1");
        
        demount_flag = mi17_approve[idx];
        promote_p1_flag = mi17_approve_s3[idx];
        promote_p2_flag = mi17_approve_s5[idx];
        promote_p3_flag = mi17_approve_s1[idx];
    }}
    
    // Логируем флаги в MP2
    auto mp2_demount = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_demount");
    auto mp2_p1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p1");
    auto mp2_p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p2");
    auto mp2_p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p3");
    
    mp2_demount[pos].exchange(demount_flag);
    mp2_p1[pos].exchange(promote_p1_flag);
    mp2_p2[pos].exchange(promote_p2_flag);
    mp2_p3[pos].exchange(promote_p3_flag);
    
    return flamegpu::ALIVE;
}}
""")
        rtc_write_inactive.setInitialState("inactive")
        rtc_write_inactive.setEndState("inactive")

        # operations (state=2)
        rtc_write_operations = agent.newRTCFunction("rtc_mp2_write_operations", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_operations, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // ОТЛАДКА: Логирование динамических агентов (ACN >= 100006)
    if (aircraft_number >= 100006u && step_day <= 850u) {{
        const unsigned int partseqno_i = FLAMEGPU->getVariable<unsigned int>("partseqno_i");
        const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
        const unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
        printf("  [MP2 WRITE operations Day %u] ACN=%u, idx=%u, pos=%u, psn_i=%u, sne=%u, ppr=%u\\n",
               step_day, aircraft_number, idx, pos, partseqno_i, sne, ppr);
    }}
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_bi_counter = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_bi_counter");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_s4_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s4_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange(2u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_bi_counter[pos].exchange(1u);  // Служебное поле для BI (всегда 1)
    
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_cso[pos].exchange(FLAMEGPU->getVariable<unsigned int>("cso"));
    
    mp2_ll[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ll"));
    mp2_oh[pos].exchange(FLAMEGPU->getVariable<unsigned int>("oh"));
    mp2_br[pos].exchange(FLAMEGPU->getVariable<unsigned int>("br"));
    
    mp2_repair_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_time"));
    mp2_assembly_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_time"));
    mp2_partout_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_time"));
    
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_s4_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s4_days"));
    mp2_assembly_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_trigger"));
    mp2_active_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("active_trigger"));
    mp2_partout_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_trigger"));
    mp2_mfg_date[pos].exchange(FLAMEGPU->getVariable<unsigned int>("mfg_date"));
    
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    
    // Квотирование - читаем значения заполненные в слое count_ops
    auto mp2_quota_curr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_curr_ops");
    auto mp2_quota_target = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_target_ops");
    auto mp2_quota_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_svc_count");
    auto mp2_quota_deficit = FLAMEGPU->environment.getMacroProperty<int, {MP2_SIZE}u>("mp2_quota_deficit");
    
    // Просто копируем значения (или 0 если не заполнено)
    unsigned int quota_curr = mp2_quota_curr[pos];
    unsigned int quota_target = mp2_quota_target[pos];
    unsigned int quota_svc = mp2_quota_svc[pos];
    int quota_deficit = mp2_quota_deficit[pos];
    
    // ═══════════════════════════════════════════════════════════════════
    // Логирование флагов квотирования
    // ═══════════════════════════════════════════════════════════════════
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // Читаем флаги из MacroProperty буферов
    unsigned int demount_flag = 0u;
    unsigned int promote_p1_flag = 0u;
    unsigned int promote_p2_flag = 0u;
    unsigned int promote_p3_flag = 0u;
    
    if (group_by == 1u) {{
        auto mi8_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
        auto mi8_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s3");
        auto mi8_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s5");
        auto mi8_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s1");
        
        demount_flag = mi8_approve[idx];
        promote_p1_flag = mi8_approve_s3[idx];
        promote_p2_flag = mi8_approve_s5[idx];
        promote_p3_flag = mi8_approve_s1[idx];
    }} else if (group_by == 2u) {{
        auto mi17_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
        auto mi17_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s3");
        auto mi17_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s5");
        auto mi17_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s1");
        
        demount_flag = mi17_approve[idx];
        promote_p1_flag = mi17_approve_s3[idx];
        promote_p2_flag = mi17_approve_s5[idx];
        promote_p3_flag = mi17_approve_s1[idx];
    }}
    
    // Логируем флаги в MP2
    auto mp2_demount = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_demount");
    auto mp2_p1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p1");
    auto mp2_p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p2");
    auto mp2_p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p3");
    
    mp2_demount[pos].exchange(demount_flag);
    mp2_p1[pos].exchange(promote_p1_flag);
    mp2_p2[pos].exchange(promote_p2_flag);
    mp2_p3[pos].exchange(promote_p3_flag);

    return flamegpu::ALIVE;
}}
""")
        rtc_write_operations.setInitialState("operations")
        rtc_write_operations.setEndState("operations")

        # serviceable (state=3)
        rtc_write_serviceable = agent.newRTCFunction("rtc_mp2_write_serviceable", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_serviceable, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_bi_counter = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_bi_counter");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_s4_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s4_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange(3u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_bi_counter[pos].exchange(1u);  // Служебное поле для BI (всегда 1)
    
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_cso[pos].exchange(FLAMEGPU->getVariable<unsigned int>("cso"));
    
    mp2_ll[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ll"));
    mp2_oh[pos].exchange(FLAMEGPU->getVariable<unsigned int>("oh"));
    mp2_br[pos].exchange(FLAMEGPU->getVariable<unsigned int>("br"));
    
    mp2_repair_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_time"));
    mp2_assembly_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_time"));
    mp2_partout_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_time"));
    
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_s4_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s4_days"));
    mp2_assembly_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_trigger"));
    mp2_active_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("active_trigger"));
    mp2_partout_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_trigger"));
    mp2_mfg_date[pos].exchange(FLAMEGPU->getVariable<unsigned int>("mfg_date"));
    
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    
    // Квотирование - читаем значения заполненные в слое count_ops
    auto mp2_quota_curr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_curr_ops");
    auto mp2_quota_target = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_target_ops");
    auto mp2_quota_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_svc_count");
    auto mp2_quota_deficit = FLAMEGPU->environment.getMacroProperty<int, {MP2_SIZE}u>("mp2_quota_deficit");
    
    // Просто копируем значения (или 0 если не заполнено)
    unsigned int quota_curr = mp2_quota_curr[pos];
    unsigned int quota_target = mp2_quota_target[pos];
    unsigned int quota_svc = mp2_quota_svc[pos];
    int quota_deficit = mp2_quota_deficit[pos];
    
    // ═══════════════════════════════════════════════════════════════════
    // Логирование флагов квотирования
    // ═══════════════════════════════════════════════════════════════════
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // Читаем флаги из MacroProperty буферов
    unsigned int demount_flag = 0u;
    unsigned int promote_p1_flag = 0u;
    unsigned int promote_p2_flag = 0u;
    unsigned int promote_p3_flag = 0u;
    
    if (group_by == 1u) {{
        auto mi8_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
        auto mi8_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s3");
        auto mi8_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s5");
        auto mi8_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s1");
        
        demount_flag = mi8_approve[idx];
        promote_p1_flag = mi8_approve_s3[idx];
        promote_p2_flag = mi8_approve_s5[idx];
        promote_p3_flag = mi8_approve_s1[idx];
    }} else if (group_by == 2u) {{
        auto mi17_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
        auto mi17_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s3");
        auto mi17_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s5");
        auto mi17_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s1");
        
        demount_flag = mi17_approve[idx];
        promote_p1_flag = mi17_approve_s3[idx];
        promote_p2_flag = mi17_approve_s5[idx];
        promote_p3_flag = mi17_approve_s1[idx];
    }}
    
    // Логируем флаги в MP2
    auto mp2_demount = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_demount");
    auto mp2_p1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p1");
    auto mp2_p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p2");
    auto mp2_p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p3");
    
    mp2_demount[pos].exchange(demount_flag);
    mp2_p1[pos].exchange(promote_p1_flag);
    mp2_p2[pos].exchange(promote_p2_flag);
    mp2_p3[pos].exchange(promote_p3_flag);

    return flamegpu::ALIVE;
}}
""")
        rtc_write_serviceable.setInitialState("serviceable")
        rtc_write_serviceable.setEndState("serviceable")

        # repair (state=4)
        rtc_write_repair = agent.newRTCFunction("rtc_mp2_write_repair", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_repair, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_bi_counter = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_bi_counter");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_s4_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s4_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange(4u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_bi_counter[pos].exchange(1u);  // Служебное поле для BI (всегда 1)
    
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_cso[pos].exchange(FLAMEGPU->getVariable<unsigned int>("cso"));
    
    mp2_ll[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ll"));
    mp2_oh[pos].exchange(FLAMEGPU->getVariable<unsigned int>("oh"));
    mp2_br[pos].exchange(FLAMEGPU->getVariable<unsigned int>("br"));
    
    mp2_repair_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_time"));
    mp2_assembly_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_time"));
    mp2_partout_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_time"));
    
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_s4_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s4_days"));
    mp2_assembly_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_trigger"));
    mp2_active_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("active_trigger"));
    mp2_partout_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_trigger"));
    mp2_mfg_date[pos].exchange(FLAMEGPU->getVariable<unsigned int>("mfg_date"));
    
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    
    // Квотирование - читаем значения заполненные в слое count_ops
    auto mp2_quota_curr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_curr_ops");
    auto mp2_quota_target = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_target_ops");
    auto mp2_quota_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_svc_count");
    auto mp2_quota_deficit = FLAMEGPU->environment.getMacroProperty<int, {MP2_SIZE}u>("mp2_quota_deficit");
    
    // Просто копируем значения (или 0 если не заполнено)
    unsigned int quota_curr = mp2_quota_curr[pos];
    unsigned int quota_target = mp2_quota_target[pos];
    unsigned int quota_svc = mp2_quota_svc[pos];
    int quota_deficit = mp2_quota_deficit[pos];
    
    // ═══════════════════════════════════════════════════════════════════
    // Логирование флагов квотирования
    // ═══════════════════════════════════════════════════════════════════
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // Читаем флаги из MacroProperty буферов
    unsigned int demount_flag = 0u;
    unsigned int promote_p1_flag = 0u;
    unsigned int promote_p2_flag = 0u;
    unsigned int promote_p3_flag = 0u;
    
    if (group_by == 1u) {{
        auto mi8_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
        auto mi8_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s3");
        auto mi8_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s5");
        auto mi8_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s1");
        
        demount_flag = mi8_approve[idx];
        promote_p1_flag = mi8_approve_s3[idx];
        promote_p2_flag = mi8_approve_s5[idx];
        promote_p3_flag = mi8_approve_s1[idx];
    }} else if (group_by == 2u) {{
        auto mi17_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
        auto mi17_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s3");
        auto mi17_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s5");
        auto mi17_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s1");
        
        demount_flag = mi17_approve[idx];
        promote_p1_flag = mi17_approve_s3[idx];
        promote_p2_flag = mi17_approve_s5[idx];
        promote_p3_flag = mi17_approve_s1[idx];
    }}
    
    // Логируем флаги в MP2
    auto mp2_demount = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_demount");
    auto mp2_p1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p1");
    auto mp2_p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p2");
    auto mp2_p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p3");
    
    mp2_demount[pos].exchange(demount_flag);
    mp2_p1[pos].exchange(promote_p1_flag);
    mp2_p2[pos].exchange(promote_p2_flag);
    mp2_p3[pos].exchange(promote_p3_flag);

    return flamegpu::ALIVE;
}}
""")
        rtc_write_repair.setInitialState("repair")
        rtc_write_repair.setEndState("repair")

        # reserve (state=5)
        rtc_write_reserve = agent.newRTCFunction("rtc_mp2_write_reserve", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_reserve, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_bi_counter = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_bi_counter");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_s4_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s4_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange(5u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_bi_counter[pos].exchange(1u);  // Служебное поле для BI (всегда 1)
    
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_cso[pos].exchange(FLAMEGPU->getVariable<unsigned int>("cso"));
    
    mp2_ll[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ll"));
    mp2_oh[pos].exchange(FLAMEGPU->getVariable<unsigned int>("oh"));
    mp2_br[pos].exchange(FLAMEGPU->getVariable<unsigned int>("br"));
    
    mp2_repair_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_time"));
    mp2_assembly_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_time"));
    mp2_partout_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_time"));
    
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_s4_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s4_days"));
    mp2_assembly_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_trigger"));
    mp2_active_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("active_trigger"));
    mp2_partout_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_trigger"));
    mp2_mfg_date[pos].exchange(FLAMEGPU->getVariable<unsigned int>("mfg_date"));
    
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    
    // Квотирование - читаем значения заполненные в слое count_ops
    auto mp2_quota_curr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_curr_ops");
    auto mp2_quota_target = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_target_ops");
    auto mp2_quota_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_svc_count");
    auto mp2_quota_deficit = FLAMEGPU->environment.getMacroProperty<int, {MP2_SIZE}u>("mp2_quota_deficit");
    
    // Просто копируем значения (или 0 если не заполнено)
    unsigned int quota_curr = mp2_quota_curr[pos];
    unsigned int quota_target = mp2_quota_target[pos];
    unsigned int quota_svc = mp2_quota_svc[pos];
    int quota_deficit = mp2_quota_deficit[pos];
    
    // ═══════════════════════════════════════════════════════════════════
    // Логирование флагов квотирования
    // ═══════════════════════════════════════════════════════════════════
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // Читаем флаги из MacroProperty буферов
    unsigned int demount_flag = 0u;
    unsigned int promote_p1_flag = 0u;
    unsigned int promote_p2_flag = 0u;
    unsigned int promote_p3_flag = 0u;
    
    if (group_by == 1u) {{
        auto mi8_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
        auto mi8_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s3");
        auto mi8_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s5");
        auto mi8_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s1");
        
        demount_flag = mi8_approve[idx];
        promote_p1_flag = mi8_approve_s3[idx];
        promote_p2_flag = mi8_approve_s5[idx];
        promote_p3_flag = mi8_approve_s1[idx];
    }} else if (group_by == 2u) {{
        auto mi17_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
        auto mi17_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s3");
        auto mi17_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s5");
        auto mi17_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s1");
        
        demount_flag = mi17_approve[idx];
        promote_p1_flag = mi17_approve_s3[idx];
        promote_p2_flag = mi17_approve_s5[idx];
        promote_p3_flag = mi17_approve_s1[idx];
    }}
    
    // Логируем флаги в MP2
    auto mp2_demount = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_demount");
    auto mp2_p1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p1");
    auto mp2_p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p2");
    auto mp2_p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p3");
    
    mp2_demount[pos].exchange(demount_flag);
    mp2_p1[pos].exchange(promote_p1_flag);
    mp2_p2[pos].exchange(promote_p2_flag);
    mp2_p3[pos].exchange(promote_p3_flag);

    return flamegpu::ALIVE;
}}
""")
        rtc_write_reserve.setInitialState("reserve")
        rtc_write_reserve.setEndState("reserve")

        # storage (state=6)
        rtc_write_storage = agent.newRTCFunction("rtc_mp2_write_storage", f"""
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_storage, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Пропускаем агентов с aircraft_number=0 (запас под спавн)
    const unsigned int aircraft_number = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (aircraft_number == 0u) {{
        return flamegpu::ALIVE;
    }}
    
    const unsigned int step_day = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    const unsigned int pos = step_day * {MAX_FRAMES}u + idx;
    
    // Получаем все MacroProperty для записи (ВСЕ поля)
    auto mp2_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_day_u16");
    auto mp2_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_idx");
    auto mp2_aircraft = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_aircraft_number");
    auto mp2_partseqno = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partseqno");
    auto mp2_group_by = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_group_by");
    
    auto mp2_state = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_state");
    auto mp2_intent = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_intent_state");
    auto mp2_bi_counter = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_bi_counter");
    
    auto mp2_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_sne");
    auto mp2_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ppr");
    auto mp2_cso = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_cso");
    
    auto mp2_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_ll");
    auto mp2_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_oh");
    auto mp2_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_br");
    
    auto mp2_repair_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_time");
    auto mp2_assembly_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_time");
    auto mp2_partout_time = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_time");
    
    auto mp2_repair_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_repair_days");
    auto mp2_s4_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_s4_days");
    auto mp2_assembly_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_assembly_trigger");
    auto mp2_active_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_active_trigger");
    auto mp2_partout_trigger = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_partout_trigger");
    auto mp2_mfg_date = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_mfg_date_days");
    
    auto mp2_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dt");
    auto mp2_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_dn");
    
    
    // Записываем ВСЕ поля
    mp2_day[pos].exchange(step_day);
    mp2_idx[pos].exchange(idx);
    mp2_aircraft[pos].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    mp2_partseqno[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partseqno_i"));
    mp2_group_by[pos].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    mp2_state[pos].exchange(6u); // state_id
    mp2_intent[pos].exchange(FLAMEGPU->getVariable<unsigned int>("intent_state"));
    mp2_bi_counter[pos].exchange(1u);  // Служебное поле для BI (всегда 1)
    
    mp2_sne[pos].exchange(FLAMEGPU->getVariable<unsigned int>("sne"));
    mp2_ppr[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    mp2_cso[pos].exchange(FLAMEGPU->getVariable<unsigned int>("cso"));
    
    mp2_ll[pos].exchange(FLAMEGPU->getVariable<unsigned int>("ll"));
    mp2_oh[pos].exchange(FLAMEGPU->getVariable<unsigned int>("oh"));
    mp2_br[pos].exchange(FLAMEGPU->getVariable<unsigned int>("br"));
    
    mp2_repair_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_time"));
    mp2_assembly_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_time"));
    mp2_partout_time[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_time"));
    
    mp2_repair_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    mp2_s4_days[pos].exchange(FLAMEGPU->getVariable<unsigned int>("s4_days"));
    mp2_assembly_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_trigger"));
    mp2_active_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("active_trigger"));
    mp2_partout_trigger[pos].exchange(FLAMEGPU->getVariable<unsigned int>("partout_trigger"));
    mp2_mfg_date[pos].exchange(FLAMEGPU->getVariable<unsigned int>("mfg_date"));
    
    mp2_dt[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    mp2_dn[pos].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    
    // Квотирование - читаем значения заполненные в слое count_ops
    auto mp2_quota_curr = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_curr_ops");
    auto mp2_quota_target = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_target_ops");
    auto mp2_quota_svc = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_svc_count");
    auto mp2_quota_deficit = FLAMEGPU->environment.getMacroProperty<int, {MP2_SIZE}u>("mp2_quota_deficit");
    
    // Просто копируем значения (или 0 если не заполнено)
    unsigned int quota_curr = mp2_quota_curr[pos];
    unsigned int quota_target = mp2_quota_target[pos];
    unsigned int quota_svc = mp2_quota_svc[pos];
    int quota_deficit = mp2_quota_deficit[pos];
    
    // ═══════════════════════════════════════════════════════════════════
    // Логирование флагов квотирования
    // ═══════════════════════════════════════════════════════════════════
    const unsigned int group_by = FLAMEGPU->getVariable<unsigned int>("group_by");
    const unsigned int frames = FLAMEGPU->environment.getProperty<unsigned int>("frames_total");
    
    // Читаем флаги из MacroProperty буферов
    unsigned int demount_flag = 0u;
    unsigned int promote_p1_flag = 0u;
    unsigned int promote_p2_flag = 0u;
    unsigned int promote_p3_flag = 0u;
    
    if (group_by == 1u) {{
        auto mi8_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve");
        auto mi8_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s3");
        auto mi8_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s5");
        auto mi8_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi8_approve_s1");
        
        demount_flag = mi8_approve[idx];
        promote_p1_flag = mi8_approve_s3[idx];
        promote_p2_flag = mi8_approve_s5[idx];
        promote_p3_flag = mi8_approve_s1[idx];
    }} else if (group_by == 2u) {{
        auto mi17_approve = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve");
        auto mi17_approve_s3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s3");
        auto mi17_approve_s5 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s5");
        auto mi17_approve_s1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_FRAMES}u>("mi17_approve_s1");
        
        demount_flag = mi17_approve[idx];
        promote_p1_flag = mi17_approve_s3[idx];
        promote_p2_flag = mi17_approve_s5[idx];
        promote_p3_flag = mi17_approve_s1[idx];
    }}
    
    // Логируем флаги в MP2
    auto mp2_demount = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_demount");
    auto mp2_p1 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p1");
    auto mp2_p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p2");
    auto mp2_p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP2_SIZE}u>("mp2_quota_promote_p3");
    
    mp2_demount[pos].exchange(demount_flag);
    mp2_p1[pos].exchange(promote_p1_flag);
    mp2_p2[pos].exchange(promote_p2_flag);
    mp2_p3[pos].exchange(promote_p3_flag);

    return flamegpu::ALIVE;
}}
""")
        rtc_write_storage.setInitialState("storage")
        rtc_write_storage.setEndState("storage")

        # Создаем layer для записи снимков (все states)
        layer_snapshot = model.newLayer("mp2_write_snapshot")
        layer_snapshot.addAgentFunction(rtc_write_inactive)
        layer_snapshot.addAgentFunction(rtc_write_operations)
        layer_snapshot.addAgentFunction(rtc_write_serviceable)
        layer_snapshot.addAgentFunction(rtc_write_repair)
        layer_snapshot.addAgentFunction(rtc_write_reserve)
        layer_snapshot.addAgentFunction(rtc_write_storage)
        
        # Host функция для дренажа (если нужна)
        # ⚠️ ОТКЛЮЧЕНО: дренаж теперь происходит в конце симуляции одним батчем
        # if clickhouse_client:
        #     from mp2_drain_host import MP2DrainHostFunction
        #     drain_func = MP2DrainHostFunction(
        #         clickhouse_client,
        #         table_name='sim_masterv2',
        #         batch_size=250000,
        #         simulation_steps=MAX_DAYS
        #     )
        #     
        #     # Регистрируем host функцию в отдельном слое ПОСЛЕ всех RTC функций
        #     layer_drain = model.newLayer("mp2_drain_to_db")
        #     layer_drain.addHostFunction(drain_func)
        #     
        #     return drain_func
        
        return None
    except Exception as e:
        # MacroProperties уже созданы (вероятно, register_mp2_writer вызван дважды)
        print(f"  ⚠️  Ошибка при создании MP2 MacroProperties (вероятно, уже созданы): {str(e)[:80]}")
        return None
