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


def build_gb_by_frame(env: Dict[str, object], frames: int) -> List[int]:
    a = env.get('mp3_arrays', {})
    ac_list = a.get('mp3_aircraft_number', [])
    gb_list = a.get('mp3_group_by', [])
    frames_index = env.get('frames_index', {})
    out_gb = [0] * frames
    n = min(len(ac_list), len(gb_list))
    for i in range(n):
        ac = int(ac_list[i] or 0)
        gb = int(gb_list[i] or 0)
        fi = frames_index.get(ac, -1)
        if 0 <= fi < frames and gb in (1, 2):
            out_gb[fi] = gb
    return out_gb

def build_status_by_frame(env: Dict[str, object], frames: int) -> List[int]:
    a = env.get('mp3_arrays', {})
    ac_list = a.get('mp3_aircraft_number', [])
    st_list = a.get('mp3_status_id', [])
    gb_list = a.get('mp3_group_by', [])
    frames_index = env.get('frames_index', {})
    # По умолчанию: 0 (зарезервирован, ещё не в обороте)
    out_st = [0] * frames
    n = min(len(ac_list), len(st_list), len(gb_list) if gb_list else len(ac_list))
    for i in range(n):
        ac = int(ac_list[i] or 0)
        gb = int(gb_list[i] or 0) if gb_list else 0
        if gb not in (1, 2):
            continue
        fi = frames_index.get(ac, -1)
        if 0 <= fi < frames:
            sid = int(st_list[i] or 0)
            if sid in (1, 2, 3, 4, 5, 6):
                out_st[fi] = sid
    return out_st


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
    gb_by_f = build_gb_by_frame(env, FRAMES)
    st_by_f = build_status_by_frame(env, FRAMES)
    MP5_SIZE = FRAMES * (DAYS + 1)

    model = fg.ModelDescription("HeliSim_V2_Status246")
    e = model.Environment()
    e.newPropertyUInt("version_date", int(env['version_date_u16']))
    e.newPropertyUInt("frames_total", FRAMES)
    e.newPropertyUInt("days_total", DAYS)
    # Env arrays
    e.newMacroPropertyUInt32("mp5_lin", MP5_SIZE)
    e.newPropertyArrayUInt32("mp5_src", [0] * MP5_SIZE)
    e.newPropertyArrayUInt32("mp3_ll_by_frame", ll_by_f)
    e.newPropertyArrayUInt32("mp3_oh_by_frame", oh_by_f)

    a = model.newAgent("component")
    for name in [
        "idx","status_id","sne","ppr","daily_today_u32","daily_next_u32","ll","oh","s6_days","s6_started"
    ]:
        a.newVariableUInt(name, 0)

    # probe mp5
    func_copy = f"rtc_mp5_copy_columns_d{DAYS}"
    rtc_copy = f"""
    FLAMEGPU_AGENT_FUNCTION({func_copy}, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FRAMES = {FRAMES}u;
        static const unsigned int DAYS   = {DAYS}u;
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        if (i >= FRAMES) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int d = (day < DAYS ? day : (DAYS > 0u ? DAYS - 1u : 0u));
        const unsigned int base = d * FRAMES + i;
        const unsigned int base_next = base + FRAMES;
        auto dst = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP5_SIZE}>("mp5_lin");
        const unsigned int v0 = FLAMEGPU->environment.getProperty<unsigned int>("mp5_src", base);
        dst[base].exchange(v0);
        if (d < DAYS) {{
            const unsigned int v1 = FLAMEGPU->environment.getProperty<unsigned int>("mp5_src", base_next);
            dst[base_next].exchange(v1);
        }}
        return flamegpu::ALIVE;
    }}
    """
    # Debug: печать исходника RTC копирования
    print("--- RTC SRC (copy) ---\n" + rtc_copy + "\n--- END RTC SRC ---")
    a.newRTCFunction(func_copy, rtc_copy)

    rtc_probe = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_probe_mp5, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FRAMES = {FRAMES}u;
        static const unsigned int DAYS   = {DAYS}u;
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        if (i >= FRAMES) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int d = (day < DAYS ? day : (DAYS > 0u ? DAYS - 1u : 0u));
        const unsigned int base = d * FRAMES + i;
        const unsigned int base_next = base + FRAMES;
        auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {MP5_SIZE}>("mp5_lin");
        const unsigned int dt = mp[base];
        const unsigned int dn = mp[base_next];
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

    # layers: copy mp5 -> probe -> s2
    lcopy = model.newLayer(); lcopy.addAgentFunction(a.getFunction(func_copy))
    l0 = model.newLayer(); l0.addAgentFunction(a.getFunction("rtc_probe_mp5"))
    l1 = model.newLayer(); l1.addAgentFunction(a.getFunction("rtc_status_2"))

    sim = fg.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("version_date", int(env['version_date_u16']))
    sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
    sim.setEnvironmentPropertyUInt("days_total", DAYS)
    # MP5 линейный
    # Заполняем источник MP5 из ClickHouse
    mp5 = list(env['mp5_daily_hours_linear'])
    need = (DAYS + 1) * FRAMES
    mp5 = mp5[:need]
    assert len(mp5) == need, f"mp5_src length mismatch: {len(mp5)} != {need}"
    sim.setEnvironmentPropertyArrayUInt32("mp5_src", mp5)
    sim.setEnvironmentPropertyArrayUInt32("mp3_ll_by_frame", ll_by_f)
    sim.setEnvironmentPropertyArrayUInt32("mp3_oh_by_frame", oh_by_f)

    # Популяция: стартовые статусы из MP3 (по frames_index)
    av = fg.AgentVector(a, FRAMES)
    for i in range(FRAMES):
        av[i].setVariableUInt("idx", i)
        av[i].setVariableUInt("status_id", int(st_by_f[i] or 0))
        av[i].setVariableUInt("sne", 0)
        av[i].setVariableUInt("ppr", 0)
    sim.setPopulationData(av)

    # d0 срез (до шагов)
    out0 = fg.AgentVector(a)
    sim.getPopulationData(out0)
    # Распределение статусов на D0
    d0_counts = {k: 0 for k in (0,1,2,3,4,5,6)}
    d0_plane_counts = {k: 0 for k in (1,2,3,4,5,6)}
    for i in range(FRAMES):
        s = int(out0[i].getVariableUInt("status_id"))
        if s in d0_counts:
            d0_counts[s] += 1
        else:
            d0_counts[0] += 1
        if gb_by_f[i] in (1,2) and s in d0_plane_counts:
            d0_plane_counts[s] += 1
    print(f"D0_status_counts: s1={d0_counts.get(1,0)}, s2={d0_counts.get(2,0)}, s3={d0_counts.get(3,0)}, s4={d0_counts.get(4,0)}, s5={d0_counts.get(5,0)}, s6={d0_counts.get(6,0)}")
    print(f"D0_reserved: s0={d0_counts.get(0,0)}")
    print(f"D0_plane_status_counts(gb∈1,2): s1={d0_plane_counts.get(1,0)}, s2={d0_plane_counts.get(2,0)}, s3={d0_plane_counts.get(3,0)}, s4={d0_plane_counts.get(4,0)}, s5={d0_plane_counts.get(5,0)}, s6={d0_plane_counts.get(6,0)}")

    # Шаги + опциональный трейс одного индекса
    trace_idx_env = os.environ.get('HL_V2_TRACE_IDX', '')
    trace_idx = int(trace_idx_env) if trace_idx_env.isdigit() else -1
    timeline = []  # (day, idx, dt, dn, sne, ppr, status)
    # Счётчики переходов
    trans_2_to_4 = 0
    trans_2_to_6 = 0
    trans_4_to_5 = 0
    strict_validation = os.environ.get('HL_V2_VALIDATION_STRICT', '0') == '1'
    # Отслеживание предыдущего статуса по всем кадрам
    prev = fg.AgentVector(a)
    sim.getPopulationData(prev)
    prev_status = [int(prev[i].getVariableUInt("status_id")) for i in range(FRAMES)]
    for step_i in range(STEPS):
        sim.step()
        cur = fg.AgentVector(a)
        sim.getPopulationData(cur)
        # Validation: s6 неизменяемость, s2 инкременты
        s6_break = 0
        sum_dt_s2 = 0
        sum_sne_delta_s2 = 0
        for i in range(FRAMES):
            ps = prev_status[i]
            cs = int(cur[i].getVariableUInt("status_id"))
            if ps == 6 and cs != 6:
                s6_break += 1
            if ps == 2:
                dt_i = cur[i].getVariableUInt("daily_today_u32")
                sne_cur = cur[i].getVariableUInt("sne")
                sne_prev = prev[i].getVariableUInt("sne")
                if sne_cur >= sne_prev:
                    sum_sne_delta_s2 += (sne_cur - sne_prev)
                sum_dt_s2 += dt_i
        if s6_break > 0:
            msg = f"VALIDATION FAIL (day {step_i+1}): status6 changed for {s6_break} frames"
            print(msg)
            if strict_validation:
                print("Strict mode: aborting")
                return 2
        if sum_sne_delta_s2 != sum_dt_s2:
            msg = f"VALIDATION FAIL (day {step_i+1}): sne_delta_s2={sum_sne_delta_s2} != sum_dt_s2={sum_dt_s2}"
            print(msg)
            if strict_validation:
                print("Strict mode: aborting")
                return 2
        # Трейс одного индекса
        if 0 <= trace_idx < FRAMES:
            dt = cur[trace_idx].getVariableUInt("daily_today_u32")
            dn = cur[trace_idx].getVariableUInt("daily_next_u32")
            sne_v = cur[trace_idx].getVariableUInt("sne")
            ppr_v = cur[trace_idx].getVariableUInt("ppr")
            st_v = cur[trace_idx].getVariableUInt("status_id")
            day = len(timeline) + 1
            timeline.append((day, trace_idx, dt, dn, sne_v, ppr_v, st_v))
        # Счёт переходов
        for i in range(FRAMES):
            ps = prev_status[i]
            cs = int(cur[i].getVariableUInt("status_id"))
            if ps == 2 and cs == 4:
                trans_2_to_4 += 1
            elif ps == 2 and cs == 6:
                trans_2_to_6 += 1
            elif ps == 4 and cs == 5:
                trans_4_to_5 += 1
            prev_status[i] = cs
        # Обновляем prev для delta-метрик
        prev = cur

    out = fg.AgentVector(a)
    sim.getPopulationData(out)
    sample = min(5, FRAMES)
    rows: List[tuple] = []
    for i in range(sample):
        st = out[i].getVariableUInt("status_id")
        sne = out[i].getVariableUInt("sne")
        ppr = out[i].getVariableUInt("ppr")
        llv = ll_by_f[i]
        ohv = oh_by_f[i]
        dt = out[i].getVariableUInt("daily_today_u32")
        dn = out[i].getVariableUInt("daily_next_u32")
        rows.append((i, st, sne, ppr, llv, ohv, dt, dn))
    # Счётчик переходов в 6 за горизонт
    cnt2 = cnt4 = cnt6 = 0
    for i in range(FRAMES):
        stv = out[i].getVariableUInt("status_id")
        if stv == 2:
            cnt2 += 1
        elif stv == 4:
            cnt4 += 1
        elif stv == 6:
            cnt6 += 1
    print(f"Status246 OK: DAYS={DAYS}, FRAMES={FRAMES}, final_status_counts: s2={cnt2}, s4={cnt4}, s6={cnt6}, transitions: 2->4={trans_2_to_4}, 2->6={trans_2_to_6}, 4->5={trans_4_to_5}, sample(idx,status,sne,ppr,ll,oh,dt,dn)={rows}")
    if timeline:
        print("trace_idx timeline (day, idx, dt, dn, sne, ppr, status):")
        # печатаем до 15 строк чтобы не засорять лог
        for rec in timeline[:15]:
            print(rec)
        if len(timeline) > 15:
            print(f"... ({len(timeline)-15} more)")
    return 0


if __name__ == "__main__":
    sys.exit(main())


