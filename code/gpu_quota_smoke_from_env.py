#!/usr/bin/env python3
"""
GPU quota smoke (from real Env):
- Загружает Env (MP4/MP3/MP5 размеры) через prepare_env_arrays (ClickHouse)
- Собирает минимальную модель с тремя RTC: intent → approve → apply (для MI‑8 и MI‑17)
- Создаёт агентов с корректным idx (по frames_index) и group_by из MP3
- Выполняет один шаг и печатает claimed vs seed по обоим типам
"""
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__)))
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from sim_env_setup import get_client, prepare_env_arrays

try:
    import pyflamegpu as fg
except Exception as e:
    print("ERR_NO_PYFLAMEGPU:", e)
    raise SystemExit(1)


def ensure_cuda_path():
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
    client = get_client()
    env = prepare_env_arrays(client)
    DAYS = int(env['days_total_u16'])
    FRAMES = int(env['frames_total_u16'])
    mp4_ops8 = list(env['mp4_ops_counter_mi8'])
    mp4_ops17 = list(env['mp4_ops_counter_mi17'])
    mp3 = env['mp3_arrays']
    n_agents = int(env.get('mp3_count', len(mp3.get('mp3_aircraft_number', []))))
    frames_index = env['frames_index']

    model = fg.ModelDescription("GPUQuotaFromEnv")
    e = model.Environment()
    e.newPropertyUInt("version_date", 0)
    e.newPropertyUInt("frames_total", 0)
    e.newPropertyUInt("days_total", 0)
    e.newPropertyArrayUInt32("mp4_ops_counter_mi8", [0] * DAYS)
    e.newPropertyArrayUInt32("mp4_ops_counter_mi17", [0] * DAYS)
    # intent/approve
    e.newMacroPropertyUInt32("mi8_intent", FRAMES)
    e.newMacroPropertyUInt32("mi17_intent", FRAMES)
    e.newMacroPropertyUInt32("mi8_approve", FRAMES)
    e.newMacroPropertyUInt32("mi17_approve", FRAMES)

    a = model.newAgent("component")
    a.newVariableUInt("idx", 0)
    a.newVariableUInt("group_by", 0)
    a.newVariableUInt("ops_ticket", 0)

    rtc_intent = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_intent, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FRAMES = {FRAMES}u;
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= FRAMES) return flamegpu::ALIVE;
        if (gb == 1u) {{ auto i8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent"); i8[idx].exchange(1u); }}
        else if (gb == 2u) {{ auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent"); i17[idx].exchange(1u); }}
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction("rtc_quota_intent", rtc_intent)

    rtc_approve = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_approve_manager, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int DAYS = {DAYS}u;
        static const unsigned int FRAMES = {FRAMES}u;
        if (FLAMEGPU->getVariable<unsigned int>("idx") != 0u) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int dayp1 = (day + 1u < DAYS ? day + 1u : day);
        auto i8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_intent");
        auto i17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_intent");
        auto a8  = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve");
        auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve");
        for (unsigned int k=0u;k<FRAMES;++k) {{ a8[k].exchange(0u); a17[k].exchange(0u); }}
        unsigned int left8  = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi8", dayp1);
        unsigned int left17 = FLAMEGPU->environment.getProperty<unsigned int>("mp4_ops_counter_mi17", dayp1);
        for (unsigned int k=0u;k<FRAMES && left8>0u;++k) {{ if (i8[k]) {{ a8[k].exchange(1u); --left8; }} }}
        for (unsigned int k=0u;k<FRAMES && left17>0u;++k) {{ if (i17[k]) {{ a17[k].exchange(1u); --left17; }} }}
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction("rtc_quota_approve_manager", rtc_approve)

    rtc_apply = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_quota_apply, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FRAMES = {FRAMES}u;
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        const unsigned int idx = FLAMEGPU->getVariable<unsigned int>("idx");
        if (idx >= FRAMES) return flamegpu::ALIVE;
        if (gb == 1u) {{ auto a8 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi8_approve"); if (a8[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u); }}
        else if (gb == 2u) {{ auto a17 = FLAMEGPU->environment.getMacroProperty<unsigned int, FRAMES>("mi17_approve"); if (a17[idx]) FLAMEGPU->setVariable<unsigned int>("ops_ticket", 1u); }}
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction("rtc_quota_apply", rtc_apply)

    l1 = model.newLayer(); l1.addAgentFunction(a.getFunction("rtc_quota_intent"))
    l2 = model.newLayer(); l2.addAgentFunction(a.getFunction("rtc_quota_approve_manager"))
    l3 = model.newLayer(); l3.addAgentFunction(a.getFunction("rtc_quota_apply"))

    sim = fg.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("version_date", int(env['version_date_u16']))
    sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
    sim.setEnvironmentPropertyUInt("days_total", DAYS)
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi8", mp4_ops8)
    sim.setEnvironmentPropertyArrayUInt32("mp4_ops_counter_mi17", mp4_ops17)

    # Построим карту group_by для каждого кадра (берём первое валидное значение из MP3)
    frame_gb = [0] * FRAMES
    ac_nums = mp3.get('mp3_aircraft_number', [])
    gbs = mp3.get('mp3_group_by', [1] * len(ac_nums))
    for i in range(min(len(ac_nums), len(gbs))):
        ac = int(ac_nums[i] or 0)
        gb = int(gbs[i] or 0)
        fi = int(frames_index.get(ac, -1)) if ac else -1
        if 0 <= fi < FRAMES and frame_gb[fi] == 0 and gb in (1, 2):
            frame_gb[fi] = gb
    # Заполним отсутствующие значения типом 1 по умолчанию
    frame_gb = [gb if gb in (1, 2) else 1 for gb in frame_gb]

    # Создаём ровно FRAMES агентов: по одному на каждый кадр
    av = fg.AgentVector(a, FRAMES)
    for i in range(FRAMES):
        av[i].setVariableUInt("idx", i)
        av[i].setVariableUInt("group_by", int(frame_gb[i]))
        av[i].setVariableUInt("ops_ticket", 0)
    sim.setPopulationData(av)

    sim.step()

    out = fg.AgentVector(a)
    sim.getPopulationData(out)
    claimed8 = claimed17 = 0
    for ag in out:
        if int(ag.getVariableUInt("ops_ticket")) == 1:
            gb = int(ag.getVariableUInt("group_by"))
            if gb == 1:
                claimed8 += 1
            elif gb == 2:
                claimed17 += 1
    d1 = 1 if DAYS > 1 else 0
    print(f"GPUQuotaFromEnv: seed8[D1]={mp4_ops8[d1]}, claimed8={claimed8}; seed17[D1]={mp4_ops17[d1]}, claimed17={claimed17}")


if __name__ == '__main__':
    main()


