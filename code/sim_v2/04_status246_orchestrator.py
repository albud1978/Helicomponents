#!/usr/bin/env python3
"""
V2 Orchestrator: материализация MP5 + статус‑ядро 2/4/6 в одном процессе
Назначение: устранить нули dt/dn без RTC‑копирования в шаге 04 за счёт единой модели.
"""
from __future__ import annotations

import os
import sys
from typing import Dict, List

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from sim_env_setup import get_client, prepare_env_arrays, fetch_versions


def build_ll_oh_br_by_frame(env: Dict[str, object], frames: int) -> tuple[list[int], list[int], list[int]]:
    a = env.get('mp3_arrays', {})
    ac_list = a.get('mp3_aircraft_number', [])
    ll_list = a.get('mp3_ll', [])
    gb_list = a.get('mp3_group_by', [])
    frames_index = env.get('frames_index', {})
    out_ll = [0] * frames
    for i in range(min(len(ac_list), len(ll_list))):
        ac = int(ac_list[i] or 0)
        fi = frames_index.get(ac, -1)
        if 0 <= fi < frames and (not gb_list or int(gb_list[i] or 0) in (1, 2)):
            out_ll[fi] = int(ll_list[i] or 0)

    # partseq по кадрам (по совпадающему group_by)
    pseq_by_f = [0] * frames
    partseq_list = a.get('mp3_partseqno_i', [])
    for i in range(min(len(ac_list), len(partseq_list), len(gb_list) if gb_list else len(ac_list))):
        ac = int(ac_list[i] or 0)
        fi = int(frames_index.get(ac, -1))
        if 0 <= fi < frames:
            pseq_by_f[fi] = int(partseq_list[i] or 0)

    # BR/OH из MP1 по partseq и group_by
    br_by_f = [0] * frames
    oh_by_f = [0] * frames
    mp1_idx_map = env.get('mp1_index', {})
    br8 = env.get('mp1_br_mi8', [])
    br17 = env.get('mp1_br_mi17', [])
    oh8 = env.get('mp1_oh_mi8', [])
    oh17 = env.get('mp1_oh_mi17', [])
    gb_by_f = [0] * frames
    if gb_list:
        for i in range(min(len(ac_list), len(gb_list))):
            ac = int(ac_list[i] or 0)
            fi = frames_index.get(ac, -1)
            if 0 <= fi < frames:
                gb_by_f[fi] = int(gb_list[i] or 0)
    for fi in range(frames):
        pseq = int(pseq_by_f[fi] or 0)
        pidx = int(mp1_idx_map.get(pseq, -1))
        gb = int(gb_by_f[fi] or 0)
        if pidx >= 0:
            if gb == 1 and pidx < len(br8):
                br_by_f[fi] = int(br8[pidx] or 0)
                if pidx < len(oh8):
                    oh_by_f[fi] = int(oh8[pidx] or 0)
            elif gb == 2 and pidx < len(br17):
                br_by_f[fi] = int(br17[pidx] or 0)
                if pidx < len(oh17):
                    oh_by_f[fi] = int(oh17[pidx] or 0)
    return out_ll, oh_by_f, br_by_f


def main() -> int:
    try:
        import pyflamegpu as fg
    except Exception as e:
        print(f"pyflamegpu not installed: {e}")
        return 1

    client = get_client()
    env = prepare_env_arrays(client)
    DAYS_full = int(env['days_total_u16'])
    FRAMES = int(env['frames_total_u16'])
    # FRAMES из снапшота (union)
    import json
    with open(os.environ.get('HL_V2_ENV_SNAPSHOT', 'tmp/env_snapshot.json'), 'r', encoding='utf-8') as f:
        snap = json.load(f)
    FRAMES = int(snap.get('frames_union_no_future', FRAMES))
    STEPS = int(os.environ.get('HL_V2_STEPS', '90'))
    DAYS = min(STEPS, DAYS_full)

    ll_by_f, oh_by_f, br_by_f = build_ll_oh_br_by_frame(env, FRAMES)

    model = fg.ModelDescription("HeliSim_V2_Status246_Orchestrated")
    e = model.Environment()
    e.newPropertyUInt("version_date", int(env['version_date_u16']))
    e.newPropertyUInt("frames_total", FRAMES)
    e.newPropertyUInt("days_total", DAYS)
    e.newPropertyArrayUInt32("mp3_ll_by_frame", ll_by_f)
    e.newPropertyArrayUInt32("mp1_oh_by_frame", oh_by_f)
    e.newPropertyArrayUInt32("mp1_br_by_frame", br_by_f)
    # MP5 MacroProperty не используется в этом оркестраторе (dt/dn задаются на host перед шагом)

    a = model.newAgent("component")
    for name in [
        "idx","aircraft_number","partseqno_i","group_by",
        "status_id","sne","ppr","repair_days",
        "daily_today_u32","daily_next_u32","ll","oh","s6_days","s6_started"
    ]:
        a.newVariableUInt(name, 0)

    import pyflamegpu as fg

    # Статус 2/4/6 + запись dt/dn из mp5_lin
    rtc_246 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_status_246_run, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FR = {FRAMES}u;
        static const unsigned int DY = {DAYS}u;
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        if (i >= FR) return flamegpu::ALIVE;
        // dt/dn передаются с host в агентные переменные перед каждым шагом
        const unsigned int dt = FLAMEGPU->getVariable<unsigned int>("daily_today_u32");
        const unsigned int dn = FLAMEGPU->getVariable<unsigned int>("daily_next_u32");
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        if (!(gb == 1u || gb == 2u)) return flamegpu::ALIVE;
        unsigned int sid = FLAMEGPU->getVariable<unsigned int>("status_id");
        if (sid == 2u) {{
            unsigned int sne = FLAMEGPU->getVariable<unsigned int>("sne");
            unsigned int ppr = FLAMEGPU->getVariable<unsigned int>("ppr");
            if (dt) {{ sne += dt; ppr += dt; FLAMEGPU->setVariable<unsigned int>("sne", sne); FLAMEGPU->setVariable<unsigned int>("ppr", ppr); }}
            const unsigned int s_next = sne + dn;
            const unsigned int p_next = ppr + dn;
            const unsigned int ll = FLAMEGPU->environment.getProperty<unsigned int>("mp3_ll_by_frame", i);
        const unsigned int oh = FLAMEGPU->environment.getProperty<unsigned int>("mp1_oh_by_frame", i);
        const unsigned int br = FLAMEGPU->environment.getProperty<unsigned int>("mp1_br_by_frame", i);
        // Переходы по порогам: сначала OH/BR (приоритет ремонта), затем LL (невозвратный)
        if (oh > 0u && p_next >= oh) {{
            if (br > 0u && s_next >= br) {{
                FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
                FLAMEGPU->setVariable<unsigned int>("s6_days", 0u);
                FLAMEGPU->setVariable<unsigned int>("s6_started", 1u);
            }} else {{
                FLAMEGPU->setVariable<unsigned int>("status_id", 4u);
            }}
        }} else if (ll > 0u && s_next >= ll) {{
            FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
            FLAMEGPU->setVariable<unsigned int>("s6_days", 0u);
            FLAMEGPU->setVariable<unsigned int>("s6_started", 1u);
        }}
        }} else if (sid == 4u) {{
            unsigned int rd = FLAMEGPU->getVariable<unsigned int>("repair_days");
            FLAMEGPU->setVariable<unsigned int>("repair_days", rd + 1u);
        }} else if (sid == 6u) {{
            unsigned int s6d = FLAMEGPU->getVariable<unsigned int>("s6_days");
            FLAMEGPU->setVariable<unsigned int>("s6_days", s6d + 1u);
        }}
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction("rtc_status_246_run", rtc_246)

    # Слои: только статус 246 (MP5 будет инициализирован HostFunction до запуска симуляции)
    l0 = model.newLayer(); l0.addAgentFunction(a.getFunction("rtc_status_246_run"))

    # Источник MP5 линейный массив (используется host-циклом перед шагами)
    mp5 = list(env['mp5_daily_hours_linear'])
    need = (DAYS + 1) * FRAMES
    mp5 = mp5[:need]
    assert len(mp5) == need, f"mp5 length mismatch: {len(mp5)} != {need}"

    # Данные окружения
    sim = fg.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("version_date", int(env['version_date_u16']))
    sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
    sim.setEnvironmentPropertyUInt("days_total", DAYS)
    sim.setEnvironmentPropertyArrayUInt32("mp3_ll_by_frame", ll_by_f)
    sim.setEnvironmentPropertyArrayUInt32("mp1_oh_by_frame", oh_by_f)
    sim.setEnvironmentPropertyArrayUInt32("mp1_br_by_frame", br_by_f)

    # Популяция из MP3 по кадрам
    mp3 = env.get('mp3_arrays', {})
    ac_list  = mp3.get('mp3_aircraft_number', [])
    st_list  = mp3.get('mp3_status_id', [])
    sne_list = mp3.get('mp3_sne', [])
    ppr_list = mp3.get('mp3_ppr', [])
    gb_list  = mp3.get('mp3_group_by', [])
    psn_list = mp3.get('mp3_partseqno_i', [])
    frames_index = env.get('frames_index', {})
    inv_index: Dict[int,int] = {fi: ac for ac, fi in frames_index.items()}

    # Собираем агрегат по AC (последнее значение в MP3) и дополняем anyLast из heli_pandas
    agg_by_ac: Dict[int, Dict[str,int]] = {}
    m = min(len(ac_list), len(st_list), len(sne_list), len(ppr_list))
    for row in range(m):
        ac = int(ac_list[row] or 0)
        agg_by_ac[ac] = {
            'group_by': int(gb_list[row] or 0) if row < len(gb_list) else 0,
            'status_id': int(st_list[row] or 0),
            'sne': int(sne_list[row] or 0),
            'ppr': int(ppr_list[row] or 0),
            'partseqno_i': int(psn_list[row] or 0) if row < len(psn_list) else 0,
        }
    try:
        vdate, vid = fetch_versions(client)
        sql = (
            "SELECT aircraft_number, anyLast(sne) sne, anyLast(ppr) ppr, anyLast(status_id) sid, anyLast(group_by) gb "
            f"FROM heli_pandas WHERE version_date = '{vdate}' AND version_id = {vid} AND group_by IN (1,2) GROUP BY aircraft_number"
        )
        rows = client.execute(sql)
        for ac, sne_v, ppr_v, sid_v, gb_v in rows:
            ac_i = int(ac or 0)
            base = agg_by_ac.get(ac_i, {})
            base.update({
                'group_by': int(gb_v or base.get('group_by', 0)),
                'status_id': int(sid_v or base.get('status_id', 0)),
                'sne': int(sne_v or base.get('sne', 0)),
                'ppr': int(ppr_v or base.get('ppr', 0)),
            })
            agg_by_ac[ac_i] = base
    except Exception:
        pass

    # Инициализация популяции по FRAMES с заполнением из agg_by_ac
    av = fg.AgentVector(a, FRAMES)
    for i in range(FRAMES):
        av[i].setVariableUInt("idx", i)
        ac = int(inv_index.get(i, 0))
        av[i].setVariableUInt("aircraft_number", ac)
        base = agg_by_ac.get(ac, None)
        if base is not None:
            av[i].setVariableUInt("group_by", int(base.get('group_by', 0)))
            av[i].setVariableUInt("status_id", int(base.get('status_id', 0)))
            av[i].setVariableUInt("sne", int(base.get('sne', 0)))
            av[i].setVariableUInt("ppr", int(base.get('ppr', 0)))
            av[i].setVariableUInt("partseqno_i", int(base.get('partseqno_i', 0)))
        else:
            av[i].setVariableUInt("group_by", 0)
            av[i].setVariableUInt("status_id", 0)
            av[i].setVariableUInt("sne", 0)
            av[i].setVariableUInt("ppr", 0)

    # Пересчёт oh/br по ппл: используем pseq по кадру из MP3 (последняя запись с совпадающим gb)
    mp1_idx_map = env.get('mp1_index', {})
    oh8 = env.get('mp1_oh_mi8', [])
    oh17 = env.get('mp1_oh_mi17', [])
    br8 = env.get('mp1_br_mi8', [])
    br17 = env.get('mp1_br_mi17', [])
    # Построим pseq_by_f по MP3 с фильтром по gb
    pseq_by_f = [0] * FRAMES
    for row in range(m):
        ac = int(ac_list[row] or 0)
        fi = int(frames_index.get(ac, -1))
        if 0 <= fi < FRAMES:
            gb_row = int(gb_list[row] or 0)
            if gb_row in (1, 2) and gb_row == int(agg_by_ac.get(ac, {}).get('group_by', 0)):
                pseq_by_f[fi] = int(psn_list[row] or 0)
    oh_by_f = [0] * FRAMES
    br_by_f = [0] * FRAMES
    for i in range(FRAMES):
        ac = int(inv_index.get(i, 0))
        gb = int(agg_by_ac.get(ac, {}).get('group_by', 0))
        pseq = int(pseq_by_f[i] or 0)
        pidx = int(mp1_idx_map.get(pseq, -1))
        if pidx >= 0:
            # OH уже в минутах, брать ненулевое из [oh_mi17, oh_mi8]
            _oh8 = int(oh8[pidx] if pidx < len(oh8) else 0)
            _oh17 = int(oh17[pidx] if pidx < len(oh17) else 0)
            oh_by_f[i] = _oh17 if _oh17 > 0 else (_oh8 if _oh8 > 0 else 0)
            # BR по типу борта
            if gb == 1:
                br_by_f[i] = int(br8[pidx] if pidx < len(br8) else 0)
            elif gb == 2:
                br_by_f[i] = int(br17[pidx] if pidx < len(br17) else 0)

    # Обновим Env после пересчёта порогов
    sim.setEnvironmentPropertyArrayUInt32("mp1_oh_by_frame", oh_by_f)
    sim.setEnvironmentPropertyArrayUInt32("mp1_br_by_frame", br_by_f)
    sim.setPopulationData(av)

    # D0 сверка с базой heli_pandas (anyLast) перед шагами
    try:
        vdate, vid = fetch_versions(client)
        sql = (
            "SELECT aircraft_number, anyLast(sne) sne, anyLast(ppr) ppr, anyLast(status_id) sid "
            f"FROM heli_pandas WHERE version_date = '{vdate}' AND version_id = {vid} AND group_by IN (1,2) GROUP BY aircraft_number"
        )
        rows = client.execute(sql)
        base_map = {int(ac): (int(sne or 0), int(ppr or 0), int(sid or 0)) for ac, sne, ppr, sid in rows}
        out0 = fg.AgentVector(a)
        sim.getPopulationData(out0)
        mism_sne = mism_ppr = mism_sid = 0
        zero_status_should_be_plane: List[int] = []
        for ac, fi in frames_index.items():
            if ac in base_map:
                sne_b, ppr_b, sid_b = base_map[ac]
                sne0 = int(out0[fi].getVariableUInt("sne"))
                ppr0 = int(out0[fi].getVariableUInt("ppr"))
                sid0 = int(out0[fi].getVariableUInt("status_id"))
                if sne0 != sne_b: mism_sne += 1
                if ppr0 != ppr_b: mism_ppr += 1
                if sid0 != sid_b: mism_sid += 1
                if sid0 == 0 and sid_b in (1,2,3,4,5,6):
                    zero_status_should_be_plane.append(int(ac))
        print(f"D0_check: mismatches sne={mism_sne}, ppr={mism_ppr}, status={mism_sid}")
        if zero_status_should_be_plane[:10]:
            print(f"D0_check status==0 but plane (sample ACN)={zero_status_should_be_plane[:10]}")
    except Exception as _e:
        print(f"D0_check skipped: {_e}")

    # Автотрассировка по HL_V2_CHECK_ACN (без лишнего лога)
    check_acn_env = os.environ.get('HL_V2_CHECK_ACN', '').strip()
    if check_acn_env.isdigit():
        acn_for_trace = int(check_acn_env)
        mapped_idx = int(frames_index.get(acn_for_trace, -1))
        if mapped_idx >= 0:
            os.environ['HL_V2_TRACE_IDX'] = str(mapped_idx)

    trace_idx_env = os.environ.get('HL_V2_TRACE_IDX', '')
    trace_idx = int(trace_idx_env) if trace_idx_env.isdigit() else -1
    timeline: List[tuple] = []

    # Суммарные счётчики и подробные логи переходов
    trans_24 = trans_26 = trans_45 = 0
    trans24_info: List[tuple] = []  # (day_str, acn, status_id, sne, ppr, ll, oh, br)
    trans26_info: List[tuple] = []
    trans45_info: List[tuple] = []
    # Предыдущее состояние статусов
    prev_vec = fg.AgentVector(a)
    sim.getPopulationData(prev_vec)
    prev_status = [int(prev_vec[i].getVariableUInt("status_id")) for i in range(FRAMES)]

    for d in range(DAYS):
        # Перед шагом: подготовить dt/dn для агентов в статусе 2
        pop_before = fg.AgentVector(a)
        sim.getPopulationData(pop_before)
        for i in range(FRAMES):
            if int(pop_before[i].getVariableUInt("status_id")) == 2:
                base = d * FRAMES + i
                dt = int(mp5[base]) if 0 <= base < len(mp5) else 0
                dn = int(mp5[base + FRAMES]) if 0 <= (base + FRAMES) < len(mp5) else 0
                pop_before[i].setVariableUInt("daily_today_u32", dt)
                pop_before[i].setVariableUInt("daily_next_u32", dn)
        sim.setPopulationData(pop_before)
        sim.step()
        # Подсчёт переходов за день
        cur_all = fg.AgentVector(a)
        sim.getPopulationData(cur_all)
        for i in range(FRAMES):
            ps = prev_status[i]
            cs = int(cur_all[i].getVariableUInt("status_id"))
            if ps == 2 and cs == 4:
                trans_24 += 1
                day_str = env['days_sorted'][d] if d < len(env.get('days_sorted', [])) else str(d+1)
                acn = int(cur_all[i].getVariableUInt("aircraft_number"))
                sne_v = int(cur_all[i].getVariableUInt("sne"))
                ppr_v = int(cur_all[i].getVariableUInt("ppr"))
                ll_v = int(ll_by_f[i] if 0 <= i < len(ll_by_f) else 0)
                oh_v = int(oh_by_f[i] if 0 <= i < len(oh_by_f) else 0)
                br_v = int(br_by_f[i] if 0 <= i < len(br_by_f) else 0)
                trans24_info.append((day_str, acn, cs, sne_v, ppr_v, ll_v, oh_v, br_v))
            elif ps == 2 and cs == 6:
                trans_26 += 1
                day_str = env['days_sorted'][d] if d < len(env.get('days_sorted', [])) else str(d+1)
                acn = int(cur_all[i].getVariableUInt("aircraft_number"))
                sne_v = int(cur_all[i].getVariableUInt("sne"))
                ppr_v = int(cur_all[i].getVariableUInt("ppr"))
                ll_v = int(ll_by_f[i] if 0 <= i < len(ll_by_f) else 0)
                oh_v = int(oh_by_f[i] if 0 <= i < len(oh_by_f) else 0)
                br_v = int(br_by_f[i] if 0 <= i < len(br_by_f) else 0)
                trans26_info.append((day_str, acn, cs, sne_v, ppr_v, ll_v, oh_v, br_v))
            elif ps == 4 and cs == 5:
                trans_45 += 1
                day_str = env['days_sorted'][d] if d < len(env.get('days_sorted', [])) else str(d+1)
                acn = int(cur_all[i].getVariableUInt("aircraft_number"))
                sne_v = int(cur_all[i].getVariableUInt("sne"))
                ppr_v = int(cur_all[i].getVariableUInt("ppr"))
                ll_v = int(ll_by_f[i] if 0 <= i < len(ll_by_f) else 0)
                oh_v = int(oh_by_f[i] if 0 <= i < len(oh_by_f) else 0)
                br_v = int(br_by_f[i] if 0 <= i < len(br_by_f) else 0)
                trans45_info.append((day_str, acn, cs, sne_v, ppr_v, ll_v, oh_v, br_v))
            prev_status[i] = cs

        if 0 <= trace_idx < FRAMES:
            cur = fg.AgentVector(a)
            sim.getPopulationData(cur)
            dt = int(cur[trace_idx].getVariableUInt("daily_today_u32"))
            dn = int(cur[trace_idx].getVariableUInt("daily_next_u32"))
            sne_v = int(cur[trace_idx].getVariableUInt("sne"))
            ppr_v = int(cur[trace_idx].getVariableUInt("ppr"))
            st_v = int(cur[trace_idx].getVariableUInt("status_id"))
            gb_v = int(cur[trace_idx].getVariableUInt("group_by"))
            ll_v = int(ll_by_f[trace_idx] if 0 <= trace_idx < len(ll_by_f) else 0)
            oh_v = int(oh_by_f[trace_idx] if 0 <= trace_idx < len(oh_by_f) else 0)
            br_v = int(br_by_f[trace_idx] if 0 <= trace_idx < len(br_by_f) else 0)
            timeline.append((len(timeline) + 1, trace_idx, dt, dn, sne_v, ppr_v, st_v, gb_v, ll_v, oh_v, br_v))

    # Итоговый сэмпл
    out = fg.AgentVector(a)
    sim.getPopulationData(out)
    sample = min(5, FRAMES)
    rows: List[tuple] = []
    for i in range(sample):
        st = out[i].getVariableUInt("status_id")
        sne = out[i].getVariableUInt("sne")
        ppr = out[i].getVariableUInt("ppr")
        dt = out[i].getVariableUInt("daily_today_u32")
        dn = out[i].getVariableUInt("daily_next_u32")
        gb = out[i].getVariableUInt("group_by")
        rows.append((i, st, sne, ppr, dt, dn, gb))
    print(f"Orchestrated Status246 OK: DAYS={DAYS}, FRAMES={FRAMES}, sample(idx,st,sne,ppr,dt,dn,gb)={rows}")
    print(f"transitions summary: 2->4={trans_24}, 2->6={trans_26}, 4->5={trans_45}")
    # Подробные логи (ограничим выводом первых 200 записей на тип перехода)
    if trans24_info:
        print("transitions_2to4 details (day, ac, status, sne, ppr, ll, oh, br):")
        for rec in trans24_info[:200]:
            print(f"  {rec[0]} ac={rec[1]} st={rec[2]} sne={rec[3]} ppr={rec[4]} ll={rec[5]} oh={rec[6]} br={rec[7]}")
        if len(trans24_info) > 200:
            print(f"  ... ({len(trans24_info)-200} more)")
    if trans26_info:
        print("transitions_2to6 details (day, ac, status, sne, ppr, ll, oh, br):")
        for rec in trans26_info[:200]:
            print(f"  {rec[0]} ac={rec[1]} st={rec[2]} sne={rec[3]} ppr={rec[4]} ll={rec[5]} oh={rec[6]} br={rec[7]}")
        if len(trans26_info) > 200:
            print(f"  ... ({len(trans26_info)-200} more)")
    if trans45_info:
        print("transitions_4to5 details (day, ac, status, sne, ppr, ll, oh, br):")
        for rec in trans45_info[:200]:
            print(f"  {rec[0]} ac={rec[1]} st={rec[2]} sne={rec[3]} ppr={rec[4]} ll={rec[5]} oh={rec[6]} br={rec[7]}")
        if len(trans45_info) > 200:
            print(f"  ... ({len(trans45_info)-200} more)")
    if timeline:
        print("trace_idx timeline (day, idx, dt, dn, sne, ppr, status, gb, ll, oh, br):")
        for rec in timeline[:15]:
            print(rec)
    return 0


if __name__ == "__main__":
    sys.exit(main())


