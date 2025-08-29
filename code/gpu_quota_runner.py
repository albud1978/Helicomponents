#!/usr/bin/env python3
"""
Минимальный GPU-раннер (без CH):
- Собирает простую модель с Env: days_total=3, frames_total=5
- MP4: квоты на D1 = 2 (mi8), 0 (mi17)
- RTC: один слой rtc_quota_apply_simple — выдаёт билеты по правилу idx < seed[D+1]
- Печатает CLAIMED и seed для проверки пайплайна NVRTC/RTC
"""
import os

try:
    import pyflamegpu as fg
except Exception as e:
    print("ERR_NO_PYFLAMEGPU:", e)
    raise SystemExit(1)


def ensure_cuda_path() -> None:
    if os.environ.get('CUDA_PATH'):
        return
    for p in [
        '/usr/local/cuda',
        '/usr/local/cuda-12.4',
        '/usr/local/cuda-12.3',
        '/usr/local/cuda-12.2',
        '/usr/local/cuda-12.1',
        '/usr/local/cuda-12.0',
    ]:
        if os.path.isdir(p) and os.path.isdir(os.path.join(p, 'include')):
            os.environ['CUDA_PATH'] = p
            break


def main():
    ensure_cuda_path()
    DAYS = 3
    FRAMES = 5

    model = fg.ModelDescription("GPUQuotaSimple")
    env = model.Environment()
    env.newPropertyUInt("version_date", 0)
    env.newPropertyUInt("frames_total", 0)
    env.newPropertyUInt("days_total", 0)
    env.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * DAYS)
    env.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * DAYS)

    agent = model.newAgent("component")
    agent.newVariableUInt("idx", 0)
    agent.newVariableUInt("group_by", 0)
    agent.newVariableUInt("ops_ticket", 0)

    # MacroProperty для intent/approve (UInt32, размер FRAMES)
    env.newMacroPropertyUInt32("mi8_intent", FRAMES)
    env.newMacroPropertyUInt32("mi8_approve", FRAMES)

    rtc_intent = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FRAMES = {FRAMES}u;
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        if (gb != 1u) return flamegpu::ALIVE; // только MI-8 в этом примере
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= FRAMES) return flamegpu::ALIVE;
        auto i8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
        i8[idx].exchange(1u);
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_intent", rtc_intent)

    rtc_approve = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_approve_manager, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int DAYS = {DAYS}u;
        static const unsigned int FRAMES = {FRAMES}u;
        if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int dayp1 = (day + 1u < DAYS ? day + 1u : day);
        auto i8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
        auto a8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve");
        for (unsigned int k=0u;k<FRAMES;++k) a8[k].exchange(0u);
        unsigned int left8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
        for (unsigned int k=0u;k<FRAMES && left8>0u;++k) {{ if (i8[k]) {{ a8[k].exchange(1u); --left8; }} }}
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_approve_manager", rtc_approve)

    rtc_apply = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_apply, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FRAMES = {FRAMES}u;
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        if (gb != 1u) return flamegpu::ALIVE;
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= FRAMES) return flamegpu::ALIVE;
        auto a8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve");
        if (a8[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u);
        return flamegpu::ALIVE;
    }}
    """
    agent.newRTCFunction("rtc_quota_apply", rtc_apply)

    l1 = model.newLayer(); l1.addAgentFunction(agent.getFunction("rtc_quota_intent"))
    l2 = model.newLayer(); l2.addAgentFunction(agent.getFunction("rtc_quota_approve_manager"))
    l3 = model.newLayer(); l3.addAgentFunction(agent.getFunction("rtc_quota_apply"))

    sim = fg.CUDASimulation(model)
    # Env
    sim.setEnvironmentPropertyUInt("version_date", 20000)
    sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
    sim.setEnvironmentPropertyUInt("days_total", DAYS)
    # Квота на D1: MI-8 = 2, MI-17 = 0
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", [0, 2, 0])
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", [0, 0, 0])

    # Агенты: idx=0..4, все gb=1 (MI-8)
    av = fg.AgentVector(agent, FRAMES)
    for i in range(FRAMES):
        a = av[i]
        a.setVariableUInt("idx", i)
        a.setVariableUInt("group_by", 1)
        a.setVariableUInt("ops_ticket", 0)
    sim.setPopulationData(av)

    sim.step()

    out = fg.AgentVector(agent)
    sim.getPopulationData(out)
    claimed = sum(int(a.getVariableUInt("ops_ticket")) for a in out)
    print(f"GPUQuotaSimple: claimed={claimed}, seed_mi8_D1=2")


if __name__ == "__main__":
    main()


