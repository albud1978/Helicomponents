#!/usr/bin/env python3
"""
V2 Step 04: add_status_246 (упрощённо, без квот)
- Модель: probe MP5 (dt/dn) + status_2 начисление и LL-порог 2->6
- Env: FRAMES из снапшота (union без future), MP5 линейный (UInt32), mp3_ll/oh по кадрам
- Smoke: 5 шагов, печать sample статусов и sne
"""
from __future__ import annotations

import os
import sys
from typing import Dict, List

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sim_env_setup import get_client, prepare_env_arrays


def build_ll_oh_by_frame(env: Dict[str, object], frames: int) -> (List[int], List[int]):
    a = env.get('mp3_arrays', {})
    ac_list = a.get('mp3_aircraft_number', [])
    ll_list = a.get('mp3_ll', [])
    oh_list = a.get('mp3_oh', [])
    frames_index = env.get('frames_index', {})
    out_ll = [0] * frames
    out_oh = [0] * frames
    for i in range(min(len(ac_list), len(ll_list), len(oh_list))):
        ac = int(ac_list[i] or 0)
        fi = frames_index.get(ac, -1)
        if 0 <= fi < frames:
            out_ll[fi] = int(ll_list[i] or 0)
            out_oh[fi] = int(oh_list[i] or 0)
    return out_ll, out_oh


def main() -> int:
    try:
        import pyflamegpu as fg
    except Exception as e:
        print(f"pyflamegpu not installed: {e}")
        return 1

    client = get_client()
    env = prepare_env_arrays(client)
    DAYS_full = int(env['days_total_u16'])
    # FRAMES из снапшота (union)
    FRAMES = int(env['frames_total_u16'])
    import json
    with open(os.environ.get('HL_V2_ENV_SNAPSHOT', 'tmp/env_snapshot.json'), 'r', encoding='utf-8') as f:
        snap = json.load(f)
    FRAMES = int(snap.get('frames_union_no_future', FRAMES))
    # Smoke horizon
    STEPS = int(os.environ.get('HL_V2_STEPS', '90'))
    DAYS = min(STEPS, DAYS_full)

    ll_by_f, oh_by_f = build_ll_oh_by_frame(env, FRAMES)

    model = fg.ModelDescription("HeliSim_V2_Status246")
    e = model.Environment()
    e.newPropertyUInt("version_date", int(env['version_date_u16']))
    e.newPropertyUInt("frames_total", FRAMES)
    e.newPropertyUInt("days_total", DAYS)
    # Env arrays
    e.newPropertyArrayUInt32("mp5_daily_hours", [0] * ((DAYS + 1) * FRAMES))
    e.newPropertyArrayUInt32("mp3_ll_by_frame", ll_by_f)
    e.newPropertyArrayUInt32("mp3_oh_by_frame", oh_by_f)

    a = model.newAgent("component")
    for name in [
        "idx","status_id","sne","ppr","daily_today_u32","daily_next_u32","ll","oh","s6_days","s6_started"
    ]:
        a.newVariableUInt(name, 0)

    # probe mp5
    rtc_probe = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_probe_mp5, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FRAMES = {FRAMES}u;
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        if (i >= FRAMES) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int days_total = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int d = (day < days_total ? day : (days_total > 0u ? days_total - 1u : 0u));
        const unsigned int base = d * FRAMES + i;
        const unsigned int base_next = base + FRAMES; // упрощение индекса следующего дня
        const unsigned int dt = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", base);
        const unsigned int dn = FLAMEGPU->environment.getProperty<unsigned int>("mp5_daily_hours", base_next);
        FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
        FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction("rtc_probe_mp5", rtc_probe)

    # status_2 (упрощённо): начисление и LL-порог 2->6
    rtc_s2 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_status_2, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FRAMES = {FRAMES}u;
        if (FLAMEGPU->getVariable<unsigned int>("status_id") != 2u) return flamegpu::ALIVE;
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        if (i >= FRAMES) return flamegpu::ALIVE;
        // начисление
        unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
        unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
        const unsigned int dt = FLAMEGPU->getVariable<unsigned int>("daily_today_u32");
        const unsigned int dn = FLAMEGPU->getVariable<unsigned int>("daily_next_u32");
        if (dt) {{ sne += dt; ppr += dt; FLAMEGPU->setVariable<unsigned int>("sne", sne); FLAMEGPU->setVariable<unsigned int>("ppr", ppr); }}
        const unsigned int s_next = sne + dn;
        // LL порог из Env
        const unsigned int ll = FLAMEGPU->environment.getProperty<unsigned int>("mp3_ll_by_frame", i);
        if (ll > 0u && s_next >= ll) {{
            FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
            FLAMEGPU->setVariable<unsigned int>("s6_days", 0u);
            FLAMEGPU->setVariable<unsigned int>("s6_started", 1u);
        }}
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction("rtc_status_2", rtc_s2)

    # layers: probe -> s2
    l0 = model.newLayer(); l0.addAgentFunction(a.getFunction("rtc_probe_mp5"))
    l1 = model.newLayer(); l1.addAgentFunction(a.getFunction("rtc_status_2"))

    sim = fg.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("version_date", int(env['version_date_u16']))
    sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
    sim.setEnvironmentPropertyUInt("days_total", DAYS)
    # MP5 линейный
    mp5 = list(env['mp5_daily_hours_linear'])
    need = (DAYS + 1) * FRAMES
    mp5 = mp5[:need]
    assert len(mp5) == need, f"mp5_daily_hours length mismatch: {len(mp5)} != {need}"
    sim.setEnvironmentPropertyArrayUInt32("mp5_daily_hours", mp5)
    sim.setEnvironmentPropertyArrayUInt32("mp3_ll_by_frame", ll_by_f)
    sim.setEnvironmentPropertyArrayUInt32("mp3_oh_by_frame", oh_by_f)

    # Популяция: все во 2 статусе
    av = fg.AgentVector(a, FRAMES)
    for i in range(FRAMES):
        av[i].setVariableUInt("idx", i)
        av[i].setVariableUInt("status_id", 2)
        av[i].setVariableUInt("sne", 0)
        av[i].setVariableUInt("ppr", 0)
    sim.setPopulationData(av)

    # d0 срез (до шагов)
    out0 = fg.AgentVector(a)
    sim.getPopulationData(out0)

    # Шаги + опциональный трейс одного индекса
    trace_idx_env = os.environ.get('HL_V2_TRACE_IDX', '')
    trace_idx = int(trace_idx_env) if trace_idx_env.isdigit() else -1
    timeline = []  # (day, idx, dt, dn, sne, status)
    for _ in range(STEPS):
        sim.step()
        if 0 <= trace_idx < FRAMES:
            tmp = fg.AgentVector(a)
            sim.getPopulationData(tmp)
            dt = tmp[trace_idx].getVariableUInt("daily_today_u32")
            dn = tmp[trace_idx].getVariableUInt("daily_next_u32")
            sne_v = tmp[trace_idx].getVariableUInt("sne")
            st_v = tmp[trace_idx].getVariableUInt("status_id")
            day = len(timeline) + 1
            timeline.append((day, trace_idx, dt, dn, sne_v, st_v))

    out = fg.AgentVector(a)
    sim.getPopulationData(out)
    sample = min(5, FRAMES)
    rows: List[tuple] = []
    for i in range(sample):
        st = out[i].getVariableUInt("status_id")
        sne = out[i].getVariableUInt("sne")
        llv = ll_by_f[i]
        dt = out[i].getVariableUInt("daily_today_u32")
        dn = out[i].getVariableUInt("daily_next_u32")
        sne0 = out0[i].getVariableUInt("sne")
        rows.append((i, st, sne0, sne, llv, dt, dn))
    # Счётчик переходов в 6 за горизонт
    total_s6 = 0
    for i in range(FRAMES):
        if out[i].getVariableUInt("status_id") == 6:
            total_s6 += 1
    print(f"Status246 OK: DAYS={DAYS}, FRAMES={FRAMES}, s6_count={total_s6}, sample(idx,status,sne0,sne,ll,dt_last,dn_last)={rows}")
    if timeline:
        print("trace_idx timeline (day, idx, dt, dn, sne, status):")
        # печатаем до 15 строк чтобы не засорять лог
        for rec in timeline[:15]:
            print(rec)
        if len(timeline) > 15:
            print(f"... ({len(timeline)-15} more)")
    return 0


if __name__ == "__main__":
    sys.exit(main())


