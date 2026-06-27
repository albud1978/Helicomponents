#!/usr/bin/env python3
"""
MP2 Export: GPU-side MacroProperty buffers for per-agent per-step state export.

Architecture:
- Agents write MP2 dynamic fields to MacroProperty buffers each step
- Dynamic layout: mp2_field[step * MAX_FRAMES + idx]
- Static layout: mp2_field[idx] (last-write-wins, same value for spawn slots after birth)
- After simulate(), HF_MP2_Drain reads buffers into numpy arrays
- Python reconstructs rows only from MP2 buffers (no fallback)

Buffers (25 total):
  mp2_status_id, mp2_pre_status_id, mp2_status_change_day, mp2_sne, mp2_ppr, mp2_limiter, mp2_repair_days,
  mp2_daily_today, mp2_daily_next, mp2_commit_p2, mp2_commit_p3,
  mp2_repair_time, mp2_assembly_time, mp2_active_trigger, mp2_assembly_trigger,
  mp2_repair_claim_start_day, mp2_repair_claim_end_day, mp2_repair_claim_source,
  mp2_repair_claim_line_id,
  mp2_idx, mp2_aircraft_number, mp2_group_by, mp2_ll, mp2_oh, mp2_br

Auxiliary:
  mp2_day_for_step[MAX_EXPORT_STEPS] — step->day mapping (written by StepController)
  mp2_num_steps — total number of simulated steps (written by StepController)
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from model_build import MAX_FRAMES, MAX_EXPORT_STEPS, MP2_BUF_SIZE

try:
    import pyflamegpu as fg
except ImportError as e:
    raise RuntimeError(f"pyflamegpu not installed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# MP2 field names (19 dynamic + 6 static fields)
# ═══════════════════════════════════════════════════════════════════════════════

MP2_DYNAMIC_FIELDS = [
    "mp2_status_id",
    "mp2_pre_status_id",
    "mp2_status_change_day",
    "mp2_sne",
    "mp2_ppr",
    "mp2_limiter",
    "mp2_repair_days",
    "mp2_daily_today",
    "mp2_daily_next",
    "mp2_commit_p2",
    "mp2_commit_p3",
    "mp2_repair_time",
    "mp2_assembly_time",
    "mp2_active_trigger",
    "mp2_assembly_trigger",
    "mp2_repair_claim_start_day",
    "mp2_repair_claim_end_day",
    "mp2_repair_claim_source",
    "mp2_repair_claim_line_id",
]

MP2_STATIC_FIELDS = [
    "mp2_idx",
    "mp2_aircraft_number",
    "mp2_group_by",
    "mp2_ll",
    "mp2_oh",
    "mp2_br",
]

MP2_FIELDS = MP2_DYNAMIC_FIELDS + MP2_STATIC_FIELDS

SPIKEA_MAX_STEPS = 512
SPIKEA_FIELDS = ("spikeA_status_id", "spikeA_sne")
SPIKEA_BUF_SIZE = SPIKEA_MAX_STEPS * MAX_FRAMES
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
SPIKEA_EXPORT_DIR = os.path.join(PROJECT_ROOT, "output", "ensemble_b1")

# Status ID mapping (FLAME GPU state name -> numeric ID)
STATUS_MAP = {
    "inactive": 1,
    "operations": 2,
    "serviceable": 3,
    "repair": 4,
    "reserve": 5,
    "storage": 6,
    "unserviceable": 7,
}


# ═══════════════════════════════════════════════════════════════════════════════
# Setup: declare MacroProperties
# ═══════════════════════════════════════════════════════════════════════════════

def setup_mp2_buffers(env):
    """Declare MP2 export buffers + day_for_step + num_steps."""
    for name in MP2_DYNAMIC_FIELDS:
        env.newMacroPropertyUInt(name, MP2_BUF_SIZE)
    for name in MP2_STATIC_FIELDS:
        env.newMacroPropertyUInt(name, MAX_FRAMES)
    
    # Step -> day mapping
    env.newMacroPropertyUInt("mp2_day_for_step", MAX_EXPORT_STEPS)
    
    # Total number of steps counter (размер 2 чтобы избежать проблемы с индексацией scalar в pyflamegpu)
    env.newMacroPropertyUInt("mp2_num_steps", 2)
    for name in SPIKEA_FIELDS:
        env.newMacroPropertyUInt32(name, SPIKEA_BUF_SIZE)
    
    mem_mb = (
        len(MP2_DYNAMIC_FIELDS) * MP2_BUF_SIZE
        + len(MP2_STATIC_FIELDS) * MAX_FRAMES
        + len(SPIKEA_FIELDS) * SPIKEA_BUF_SIZE
    ) * 4 / (1024 * 1024)
    print(
        f"  ✅ MP2 Export: {len(MP2_DYNAMIC_FIELDS)} dynamic × {MP2_BUF_SIZE} "
        f"+ {len(MP2_STATIC_FIELDS)} static × {MAX_FRAMES} "
        f"+ SpikeA {len(SPIKEA_FIELDS)} × {SPIKEA_BUF_SIZE} = {mem_mb:.1f} МБ GPU"
    )


# ═══════════════════════════════════════════════════════════════════════════════
# RTC template for writing agent state to MP2 buffers
# ═══════════════════════════════════════════════════════════════════════════════

RTC_MP2_WRITE_TEMPLATE = """
FLAMEGPU_AGENT_FUNCTION(rtc_mp2_write_{state}, flamegpu::MessageNone, flamegpu::MessageNone) {{
    const unsigned int step = FLAMEGPU->getStepCounter();
    const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
    if (idx >= {MAX_FRAMES}u) return flamegpu::ALIVE;
    
    const unsigned int status = FLAMEGPU->getVariable<unsigned int>("status_id");
    const unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");

    if (step < {SPIKEA_MAX_STEPS}u) {{
        const unsigned int spikeA_offset = step * {MAX_FRAMES}u + idx;
        auto spikeA_status = FLAMEGPU->environment.getMacroProperty<unsigned int, {SPIKEA_BUF_SIZE}u>("spikeA_status_id");
        spikeA_status[spikeA_offset].exchange(status);

        auto spikeA_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {SPIKEA_BUF_SIZE}u>("spikeA_sne");
        spikeA_sne[spikeA_offset].exchange(sne);
    }}

    if (step >= {MAX_EXPORT_STEPS}u) return flamegpu::ALIVE;
    
    const unsigned int offset = step * {MAX_FRAMES}u + idx;
    
    auto buf_status = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_status_id");
    buf_status[offset].exchange(status);
    
    auto buf_pre_status = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_pre_status_id");
    buf_pre_status[offset].exchange(FLAMEGPU->getVariable<unsigned int>("pre_status_id"));

    auto buf_status_change_day = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_status_change_day");
    buf_status_change_day[offset].exchange(FLAMEGPU->getVariable<unsigned int>("status_change_day"));
    
    auto buf_sne = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_sne");
    buf_sne[offset].exchange(sne);
    
    auto buf_ppr = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_ppr");
    buf_ppr[offset].exchange(FLAMEGPU->getVariable<unsigned int>("ppr"));
    
    auto buf_limiter = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_limiter");
    buf_limiter[offset].exchange((unsigned int)FLAMEGPU->getVariable<unsigned short>("limiter"));
    
    auto buf_repair = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_repair_days");
    buf_repair[offset].exchange(FLAMEGPU->getVariable<unsigned int>("repair_days"));
    
    auto buf_dt = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_daily_today");
    buf_dt[offset].exchange(FLAMEGPU->getVariable<unsigned int>("daily_today_u32"));
    
    auto buf_dn = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_daily_next");
    buf_dn[offset].exchange(FLAMEGPU->getVariable<unsigned int>("daily_next_u32"));
    
    auto buf_p2 = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_commit_p2");
    buf_p2[offset].exchange(FLAMEGPU->getVariable<unsigned int>("commit_p2"));
    
    auto buf_p3 = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_commit_p3");
    buf_p3[offset].exchange(FLAMEGPU->getVariable<unsigned int>("commit_p3"));
    
    auto buf_rt = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_repair_time");
    buf_rt[offset].exchange(FLAMEGPU->getVariable<unsigned int>("repair_time"));
    
    auto buf_at = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_assembly_time");
    buf_at[offset].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_time"));
    
    auto buf_act = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_active_trigger");
    buf_act[offset].exchange(FLAMEGPU->getVariable<unsigned int>("active_trigger"));
    
    auto buf_asm = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_assembly_trigger");
    buf_asm[offset].exchange(FLAMEGPU->getVariable<unsigned int>("assembly_trigger"));
    
    auto buf_claim_start = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_repair_claim_start_day");
    buf_claim_start[offset].exchange(FLAMEGPU->getVariable<unsigned int>("repair_claim_start_day"));
    
    auto buf_claim_end = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_repair_claim_end_day");
    buf_claim_end[offset].exchange(FLAMEGPU->getVariable<unsigned int>("repair_claim_end_day"));
    
    auto buf_claim_source = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_repair_claim_source");
    buf_claim_source[offset].exchange(FLAMEGPU->getVariable<unsigned int>("repair_claim_source"));

    auto buf_claim_line_id = FLAMEGPU->environment.getMacroProperty<unsigned int, {BUF_SIZE}u>("mp2_repair_claim_line_id");
    buf_claim_line_id[offset].exchange(FLAMEGPU->getVariable<unsigned int>("repair_line_id"));
    
    auto buf_idx = FLAMEGPU->environment.getMacroProperty<unsigned int, {STATIC_BUF_SIZE}u>("mp2_idx");
    buf_idx[idx].exchange(FLAMEGPU->getVariable<unsigned int>("idx"));
    
    auto buf_acn = FLAMEGPU->environment.getMacroProperty<unsigned int, {STATIC_BUF_SIZE}u>("mp2_aircraft_number");
    buf_acn[idx].exchange(FLAMEGPU->getVariable<unsigned int>("aircraft_number"));
    
    auto buf_gb = FLAMEGPU->environment.getMacroProperty<unsigned int, {STATIC_BUF_SIZE}u>("mp2_group_by");
    buf_gb[idx].exchange(FLAMEGPU->getVariable<unsigned int>("group_by"));
    
    auto buf_ll = FLAMEGPU->environment.getMacroProperty<unsigned int, {STATIC_BUF_SIZE}u>("mp2_ll");
    buf_ll[idx].exchange(FLAMEGPU->getVariable<unsigned int>("ll"));
    
    auto buf_oh = FLAMEGPU->environment.getMacroProperty<unsigned int, {STATIC_BUF_SIZE}u>("mp2_oh");
    buf_oh[idx].exchange(FLAMEGPU->getVariable<unsigned int>("oh"));
    
    auto buf_br = FLAMEGPU->environment.getMacroProperty<unsigned int, {STATIC_BUF_SIZE}u>("mp2_br");
    buf_br[idx].exchange(FLAMEGPU->getVariable<unsigned int>("br"));
    
    return flamegpu::ALIVE;
}}
"""


def register_mp2_write_layer(model, heli_agent):
    """Register MP2 write functions for all 6 active states on a single layer."""
    layer = model.newLayer("layer_mp2_write")
    
    active_states = ["inactive", "operations", "serviceable", "repair", "storage", "unserviceable"]
    
    for state_name in active_states:
        status_id = STATUS_MAP[state_name]
        func_name = f"rtc_mp2_write_{state_name}"
        
        rtc_src = RTC_MP2_WRITE_TEMPLATE.format(
            state=state_name,
            STATUS_ID=status_id,
            MAX_EXPORT_STEPS=MAX_EXPORT_STEPS,
            SPIKEA_MAX_STEPS=SPIKEA_MAX_STEPS,
            MAX_FRAMES=MAX_FRAMES,
            BUF_SIZE=MP2_BUF_SIZE,
            SPIKEA_BUF_SIZE=SPIKEA_BUF_SIZE,
            STATIC_BUF_SIZE=MAX_FRAMES,
        )
        
        fn = heli_agent.newRTCFunction(func_name, rtc_src)
        fn.setInitialState(state_name)
        fn.setEndState(state_name)
        layer.addAgentFunction(fn)
    
    print(f"  ✅ MP2 Write: {len(active_states)} состояний на слое layer_mp2_write")


# ═══════════════════════════════════════════════════════════════════════════════
# HF_MP2_Drain: reads MP2 buffers on the final step
# ═══════════════════════════════════════════════════════════════════════════════

class HF_MP2_Drain(fg.HostFunction):
    """
    Exit HostFunction that reads MP2 buffers into Python after simulate().
    
    self.data['fields'] stores dynamic fields as shape=(num_steps, num_agents)
    and static fields as shape=(num_agents,).
    """
    
    def __init__(self, end_day: int, num_agents: int):
        super().__init__()
        self.end_day = end_day
        self.num_agents = num_agents
        self.data = None  # Populated on final step
    
    def run(self, FLAMEGPU):
        env = FLAMEGPU.environment
        import numpy as np
        if int(env.getPropertyUInt("ensemble_mode")) != 0:
            print("  [MP2 Drain] ensemble_mode=1: skip shared Python drain")
            return
        
        # Read step count
        mp_num = env.getMacroPropertyUInt("mp2_num_steps")
        num_steps = int(mp_num[0])
        if num_steps == 0:
            num_steps = FLAMEGPU.getStepCounter() + 1
        
        if num_steps > MAX_EXPORT_STEPS:
            num_steps = MAX_EXPORT_STEPS
        
        print(f"  [MP2 Drain] Чтение {num_steps} шагов × {self.num_agents} агентов...")
        
        # Read day_for_step mapping
        mp_days = env.getMacroPropertyUInt("mp2_day_for_step")
        days = [int(mp_days[s]) for s in range(num_steps)]
        
        # Read MP2 field buffers — dynamic fields are per step, static fields per agent.
        result = {}
        for field_name in MP2_DYNAMIC_FIELDS:
            mp = env.getMacroPropertyUInt(field_name)
            arr = np.zeros((num_steps, self.num_agents), dtype=np.uint32)
            for s in range(num_steps):
                base = s * MAX_FRAMES
                for a in range(self.num_agents):
                    val = int(mp[base + a]) & 0xFFFFFFFF
                    arr[s, a] = val
            result[field_name] = arr
        for field_name in MP2_STATIC_FIELDS:
            mp = env.getMacroPropertyUInt(field_name)
            arr = np.zeros(self.num_agents, dtype=np.uint32)
            for a in range(self.num_agents):
                arr[a] = int(mp[a]) & 0xFFFFFFFF
            result[field_name] = arr
        
        self.data = {
            'num_steps': num_steps,
            'days': days,
            'fields': result,
        }
        
        total_values = (
            num_steps * self.num_agents * len(MP2_DYNAMIC_FIELDS)
            + self.num_agents * len(MP2_STATIC_FIELDS)
        )
        print(f"  [MP2 Drain] ✅ Прочитано {total_values:,} значений")


class HF_SpikeA_Export(fg.HostFunction):
    """Stateless exit export for Spike A raw MacroProperty buffers."""

    def run(self, FLAMEGPU):
        env = FLAMEGPU.environment
        version_id = int(env.getPropertyUInt("version_id"))
        os.makedirs(SPIKEA_EXPORT_DIR, exist_ok=True)
        for field_name in SPIKEA_FIELDS:
            path = os.path.join(SPIKEA_EXPORT_DIR, f"run_{version_id}_{field_name}.bin")
            if os.path.exists(path):
                os.remove(path)
            env.exportMacroProperty(field_name, path, False)
            print(f"  [SpikeA] exportMacroProperty {field_name} → {path}")


def register_mp2_drain(model, end_day: int, num_agents: int):
    """Register HF_MP2_Drain as an exit host function."""
    drain = HF_MP2_Drain(end_day, num_agents)
    model.addExitFunction(drain)
    print(f"  ✅ MP2 Drain зарегистрирован (ExitFunction)")
    model.addExitFunction(HF_SpikeA_Export())
    print("  ✅ SpikeA MacroProperty export зарегистрирован (ExitFunction)")
    return drain
