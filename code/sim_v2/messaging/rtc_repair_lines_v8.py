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
    FLAMEGPU->setVariable<unsigned int>("free_days", mp_days[line_id]);
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", mp_acn[line_id]);
    return flamegpu::ALIVE;
}}
"""

RTC_REPAIR_LINE_INCREMENT = """
FLAMEGPU_AGENT_FUNCTION(rtc_repair_line_increment_v8, flamegpu::MessageNone, flamegpu::MessageNone) {
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int prev_day = FLAMEGPU->environment.getProperty<unsigned int>("prev_day");
    const unsigned int adaptive_days = (current_day > prev_day) ? (current_day - prev_day) : 0u;
    
    unsigned int free_days = FLAMEGPU->getVariable<unsigned int>("free_days");
    free_days += adaptive_days;
    FLAMEGPU->setVariable<unsigned int>("free_days", free_days);
    
    return flamegpu::ALIVE;
}
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
    const unsigned int repair_quota = FLAMEGPU->environment.getProperty<unsigned int>("repair_quota");
    auto mp_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    
    unsigned int best_line = 0xFFFFFFFFu;
    unsigned int best_days = 0xFFFFFFFFu;
    for (unsigned int i = 0u; i < repair_quota; ++i) {{
        const unsigned int fd = mp_days[i];
        if (fd < best_days) {{
            best_days = fd;
            best_line = i;
        }}
    }}
    
    if (best_line == 0xFFFFFFFFu) return flamegpu::ALIVE;
    
    FLAMEGPU->setVariable<unsigned int>("repair_candidate", 1u);
    FLAMEGPU->setVariable<unsigned int>("repair_line_id", best_line);
    FLAMEGPU->setVariable<unsigned int>("repair_line_day", best_days);
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

