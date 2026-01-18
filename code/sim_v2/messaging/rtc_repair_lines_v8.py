#!/usr/bin/env python3
"""
RTC модуль V8: RepairLine (free_days + aircraft_number) синхронизация.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import pyflamegpu as fg

# Максимум ремонтных линий (MacroProperty размер)
REPAIR_LINES_MAX = 64

RTC_REPAIR_LINE_SYNC = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_repair_line_sync_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int line_id = FLAMEGPU->getVariable<unsigned int>("line_id");
    auto mp_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    auto mp_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
    auto mp_last_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_acn_mp");
    auto mp_last_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_day_mp");
    FLAMEGPU->setVariable<unsigned int>("free_days", mp_days[line_id]);
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", mp_acn[line_id]);
    FLAMEGPU->setVariable<unsigned int>("last_acn", mp_last_acn[line_id]);
    FLAMEGPU->setVariable<unsigned int>("last_day", mp_last_day[line_id]);
    return flamegpu::ALIVE;
}}
"""

RTC_REPAIR_LINE_INCREMENT = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_repair_line_increment_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int prev_day = FLAMEGPU->environment.getProperty<unsigned int>("prev_day");
    const unsigned int adaptive_days = (current_day > prev_day) ? (current_day - prev_day) : 0u;
    
    const unsigned int line_id = FLAMEGPU->getVariable<unsigned int>("line_id");
    unsigned int free_days = FLAMEGPU->getVariable<unsigned int>("free_days");
    free_days += adaptive_days;
    FLAMEGPU->setVariable<unsigned int>("free_days", free_days);
    
    // Если линия отработала свой repair_time — освобождаем aircraft_number
    const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    if (acn != 0u) {{
        auto mp_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_rt_mp");
        const unsigned int rt = mp_rt[line_id];
        if (rt > 0u && free_days >= rt) {{
            FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
        }}
    }}
    
    return flamegpu::ALIVE;
}}
"""

RTC_REPAIR_LINE_WRITE = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_repair_line_write_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int line_id = FLAMEGPU->getVariable<unsigned int>("line_id");
    const unsigned int free_days = FLAMEGPU->getVariable<unsigned int>("free_days");
    const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    auto mp_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    auto mp_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
    mp_days[line_id].exchange(free_days);
    mp_acn[line_id].exchange(acn);
    return flamegpu::ALIVE;
}}
"""

RTC_REPAIR_LINE_ASSIGN_REPAIR = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_repair_line_assign_repair_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    // Назначение линии для repair->serviceable (без порога free_days)
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int exit_date = FLAMEGPU->getVariable<unsigned int>("exit_date");
    if (exit_date == 0xFFFFFFFFu || current_day < exit_date) {{
        return flamegpu::ALIVE;
    }}
    const unsigned int repair_quota = FLAMEGPU->environment.getProperty<unsigned int>("repair_quota");
    auto mp_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    auto mp_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
    auto mp_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_rt_mp");
    auto mp_last_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_acn_mp");
    auto mp_last_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_day_mp");
    
    const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    const unsigned int repair_time = FLAMEGPU->getVariable<unsigned int>("repair_time");
    for (unsigned int i = 0u; i < repair_quota; ++i) {{
        const unsigned int prev_last_acn = mp_last_acn[i].exchange(0u);
        const unsigned int prev_last_day = mp_last_day[i].exchange(0u);
        if (prev_last_acn == acn && prev_last_day == (current_day > 0u ? current_day - 1u : 0u)) {{
            mp_last_acn[i].exchange(prev_last_acn);
            mp_last_day[i].exchange(prev_last_day);
            continue;
        }}
        const unsigned int prev = mp_acn[i].exchange(acn);
        if (prev == 0u) {{
            mp_days[i].exchange(0u);
            mp_rt[i].exchange(repair_time);
            mp_last_acn[i].exchange(acn);
            mp_last_day[i].exchange(current_day);
            FLAMEGPU->setVariable<unsigned int>("repair_line_id", i);
            break;
        }} else {{
            mp_last_acn[i].exchange(prev_last_acn);
            mp_last_day[i].exchange(prev_last_day);
        }}
    }}
    return flamegpu::ALIVE;
}}
"""

RTC_REPAIR_LINE_PUBLISH_STATUS = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_repair_line_publish_status_v8, flamegpu::MessageNone, flamegpu::MessageArray) {{
    const unsigned int line_id = FLAMEGPU->getVariable<unsigned int>("line_id");
    const unsigned int free_days = FLAMEGPU->getVariable<unsigned int>("free_days");
    const unsigned int acn = FLAMEGPU->getVariable<unsigned int>("aircraft_number");
    
    FLAMEGPU->message_out.setIndex(line_id);
    FLAMEGPU->message_out.setVariable<unsigned int>("free_days", free_days);
    FLAMEGPU->message_out.setVariable<unsigned int>("aircraft_number", acn);
    return flamegpu::ALIVE;
}}
"""


def register_repair_line_pre_quota_layers(model: fg.ModelDescription, repair_line_agent: fg.AgentDescription):
    """Слои RepairLine до квотирования: sync -> increment -> write"""
    layer_sync = model.newLayer("v8_repair_line_sync_pre")
    fn = repair_line_agent.newRTCFunction("rtc_repair_line_sync_v8", RTC_REPAIR_LINE_SYNC)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_sync.addAgentFunction(fn)
    
    layer_inc = model.newLayer("v8_repair_line_increment")
    fn = repair_line_agent.newRTCFunction("rtc_repair_line_increment_v8", RTC_REPAIR_LINE_INCREMENT)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_inc.addAgentFunction(fn)
    
    layer_write = model.newLayer("v8_repair_line_write")
    fn = repair_line_agent.newRTCFunction("rtc_repair_line_write_v8", RTC_REPAIR_LINE_WRITE)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer_write.addAgentFunction(fn)

    layer_pub = model.newLayer("v8_repair_line_publish_status")
    fn = repair_line_agent.newRTCFunction("rtc_repair_line_publish_status_v8", RTC_REPAIR_LINE_PUBLISH_STATUS)
    fn.setInitialState("default")
    fn.setEndState("default")
    fn.setMessageOutput("RepairLineStatus")
    layer_pub.addAgentFunction(fn)
    
    print("  ✅ V8: RepairLine pre-quota слои зарегистрированы")


def register_repair_line_assign_for_repair_exit(model: fg.ModelDescription, heli_agent: fg.AgentDescription):
    """Слой назначения линии для repair→serviceable"""
    layer = model.newLayer("v8_repair_line_assign_repair")
    fn = heli_agent.newRTCFunction("rtc_repair_line_assign_repair_v8", RTC_REPAIR_LINE_ASSIGN_REPAIR)
    fn.setInitialState("repair")
    fn.setEndState("repair")
    layer.addAgentFunction(fn)
    print("  ✅ V8: RepairLine assign (repair) слой зарегистрирован")


def register_repair_line_sync_post_quota(model: fg.ModelDescription, repair_line_agent: fg.AgentDescription):
    """Слой синхронизации RepairLine после квотирования"""
    layer = model.newLayer("v8_repair_line_sync_post")
    fn = repair_line_agent.newRTCFunction("rtc_repair_line_sync_post_v8", RTC_REPAIR_LINE_SYNC)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer.addAgentFunction(fn)
    print("  ✅ V8: RepairLine post-quota sync слой зарегистрирован")

