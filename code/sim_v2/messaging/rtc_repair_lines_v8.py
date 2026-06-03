#!/usr/bin/env python3
"""
RTC модуль V8: RepairLine (free_days + aircraft_number) синхронизация.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import pyflamegpu as fg
from model_build import MAX_EXPORT_STEPS, RL_BUF_SIZE, REPAIR_LINES_MAX

REPAIR_BANK_MAX = 64
REPAIR_BANK_SIZE = REPAIR_LINES_MAX * REPAIR_BANK_MAX


def setup_rl_export_buffers(env):
    """Объявление буферов экспорта RepairLine."""
    env.newMacroPropertyUInt("rl_buf_free_days", RL_BUF_SIZE)
    env.newMacroPropertyUInt("rl_buf_acn", RL_BUF_SIZE)
    env.newMacroPropertyUInt("rl_buf_rt", RL_BUF_SIZE)
    env.newMacroPropertyUInt("rl_buf_gb", RL_BUF_SIZE)
    env.newMacroPropertyUInt("rl_buf_bank_count", RL_BUF_SIZE)
    env.newMacroPropertyUInt("rl_buf_bank_head_start", RL_BUF_SIZE)
    env.newMacroPropertyUInt("rl_buf_bank_head_end", RL_BUF_SIZE)
    mem_mb = 4 * RL_BUF_SIZE * 7 / (1024 * 1024)
    print(f"  ✅ RepairLine Export: 7 буферов × {RL_BUF_SIZE} = {mem_mb:.1f} МБ GPU")

# RTC_REPAIR_LINE_SYNC = f"""
# FLAMEGPU_AGENT_FUNCTION(rtc_repair_line_sync_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
#     const unsigned int line_id = FLAMEGPU->getVariable<unsigned int>("line_id");
#     auto mp_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
#     auto mp_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
#     auto mp_last_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_acn_mp");
#     auto mp_last_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_last_day_mp");
#     const unsigned int free_days = mp_days[line_id];
#     FLAMEGPU->setVariable<unsigned int>("free_days", free_days);
#     FLAMEGPU->setVariable<unsigned int>("aircraft_number", mp_acn[line_id]);
#     FLAMEGPU->setVariable<unsigned int>("last_acn", mp_last_acn[line_id]);
#     FLAMEGPU->setVariable<unsigned int>("last_day", mp_last_day[line_id]);
#     return flamegpu::ALIVE;
# }}
# """

RTC_REPAIR_LINE_INCREMENT = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_repair_line_increment_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int current_day = FLAMEGPU->environment.getProperty<unsigned int>("current_day");
    const unsigned int prev_day = FLAMEGPU->environment.getProperty<unsigned int>("prev_day");
    const unsigned int adaptive_days = (current_day > prev_day) ? (current_day - prev_day) : 0u;
    
    const unsigned int line_id = FLAMEGPU->getVariable<unsigned int>("line_id");
    auto mp_days = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    auto mp_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
    unsigned int free_days = mp_days[line_id];
    free_days += adaptive_days;
    FLAMEGPU->setVariable<unsigned int>("free_days", free_days);
    
    const unsigned int acn = mp_acn[line_id];
    FLAMEGPU->setVariable<unsigned int>("aircraft_number", acn);
    
    // Если линия отработала свой repair_time — освобождаем aircraft_number
    if (acn != 0u) {{
        auto mp_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_rt_mp");
        const unsigned int rt = mp_rt[line_id];
        if (rt > 0u && free_days >= rt) {{
            FLAMEGPU->setVariable<unsigned int>("aircraft_number", 0u);
            auto mp_gb = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_gb_mp");
            mp_gb[line_id].exchange(0u);
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

RTC_REPAIR_LINE_EXPORT = f"""
FLAMEGPU_AGENT_FUNCTION(rtc_repair_line_export_v8, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int step = FLAMEGPU->getStepCounter();
    if (step >= {MAX_EXPORT_STEPS}u) return flamegpu::ALIVE;
    
    const unsigned int line_id = FLAMEGPU->getVariable<unsigned int>("line_id");
    if (line_id >= {REPAIR_LINES_MAX}u) return flamegpu::ALIVE;
    
    const unsigned int offset = step * {REPAIR_LINES_MAX}u + line_id;
    
    auto buf_fd = FLAMEGPU->environment.getMacroProperty<unsigned int, {RL_BUF_SIZE}u>("rl_buf_free_days");
    auto buf_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {RL_BUF_SIZE}u>("rl_buf_acn");
    auto buf_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {RL_BUF_SIZE}u>("rl_buf_rt");
    auto buf_gb = FLAMEGPU->environment.getMacroProperty<unsigned int, {RL_BUF_SIZE}u>("rl_buf_gb");
    auto buf_bank_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {RL_BUF_SIZE}u>("rl_buf_bank_count");
    auto buf_bank_head_start = FLAMEGPU->environment.getMacroProperty<unsigned int, {RL_BUF_SIZE}u>("rl_buf_bank_head_start");
    auto buf_bank_head_end = FLAMEGPU->environment.getMacroProperty<unsigned int, {RL_BUF_SIZE}u>("rl_buf_bank_head_end");
    
    // free_days — из MacroProperty (SSoT, обновляется WRITE слоем)
    auto mp_fd = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_free_days_mp");
    buf_fd[offset].exchange(mp_fd[line_id]);
    
    // aircraft_number — из MacroProperty (SSoT: обновляется P2/P3 commit через CAS)
    // Agent variable не используется как источник для line-id телеметрии.
    auto mp_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_acn_mp");
    buf_acn[offset].exchange(mp_acn[line_id]);
    
    // repair_time из MacroProperty (per-line)
    auto mp_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_rt_mp");
    buf_rt[offset].exchange(mp_rt[line_id]);
    
    // group_by из MacroProperty (SSoT, устанавливается в P2/P3 commit)
    auto mp_gb = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_gb_mp");
    buf_gb[offset].exchange(mp_gb[line_id]);

    // bank telemetry: count + head_start/head_end (без нормализации)
    auto mp_bank_count = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_LINES_MAX}u>("repair_line_bank_count_mp");
    const unsigned int bank_base = line_id * {REPAIR_BANK_MAX}u;
    auto mp_bank_start = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_BANK_SIZE}u>("repair_line_bank_start_mp");
    auto mp_bank_end = FLAMEGPU->environment.getMacroProperty<unsigned int, {REPAIR_BANK_SIZE}u>("repair_line_bank_end_mp");
    buf_bank_count[offset].exchange(mp_bank_count[line_id]);
    buf_bank_head_start[offset].exchange(mp_bank_start[bank_base]);
    buf_bank_head_end[offset].exchange(mp_bank_end[bank_base]);
    
    return flamegpu::ALIVE;
}}
"""

# REMOVED (W_sim_remove_d2_noop_20260601T200344Z):
# repair-assign слой был no-op: обе ветки возвращали ALIVE.

def register_repair_line_pre_quota_layers(model: fg.ModelDescription, repair_line_agent: fg.AgentDescription):
    """Слои RepairLine до квотирования: increment -> write."""
    # layer_sync = model.newLayer("v8_repair_line_sync_pre")
    # fn = repair_line_agent.newRTCFunction("rtc_repair_line_sync_v8", RTC_REPAIR_LINE_SYNC)
    # fn.setInitialState("default")
    # fn.setEndState("default")
    # layer_sync.addAgentFunction(fn)

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


def register_repair_line_export_layer(model: fg.ModelDescription, repair_line_agent: fg.AgentDescription):
    """Слой экспорта RepairLine в MacroProperty буферы."""
    layer = model.newLayer("v8_repair_line_export")
    fn = repair_line_agent.newRTCFunction("rtc_repair_line_export_v8", RTC_REPAIR_LINE_EXPORT)
    fn.setInitialState("default")
    fn.setEndState("default")
    layer.addAgentFunction(fn)
    print("  ✅ V8: RepairLine export слой зарегистрирован")

def register_repair_line_sync_post_quota(model: fg.ModelDescription, repair_line_agent: fg.AgentDescription):
    """Слой синхронизации RepairLine после квотирования"""
    # layer = model.newLayer("v8_repair_line_sync_post")
    # fn = repair_line_agent.newRTCFunction("rtc_repair_line_sync_post_v8", RTC_REPAIR_LINE_SYNC)
    # fn.setInitialState("default")
    # fn.setEndState("default")
    # layer.addAgentFunction(fn)
    # print("  ✅ V8: RepairLine post-quota sync слой зарегистрирован")

