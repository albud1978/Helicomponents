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

    # Константы для MacroProperty
    MAX_FRAMES = 300  # Увеличено для покрытия 286 frames
    MAX_DAYS = 4000   # Фиксированный размер буфера данных
    MAX_SIZE = MAX_FRAMES * (MAX_DAYS + 1)
    
    if FRAMES > MAX_FRAMES:
        print(f"ERROR: FRAMES={FRAMES} превышает MAX_FRAMES={MAX_FRAMES}. Используйте больший MAX_FRAMES или уменьшите количество кадров.")
        return 1
    if DAYS > MAX_DAYS:
        print(f"ERROR: DAYS={DAYS} превышает MAX_DAYS={MAX_DAYS}")
        return 1

    ll_by_f, oh_by_f, br_by_f = build_ll_oh_br_by_frame(env, FRAMES)

    model = fg.ModelDescription("HeliSim_V2_Status246_Orchestrated")
    e = model.Environment()
    e.newPropertyUInt("version_date", int(env['version_date_u16']))
    e.newPropertyUInt("frames_total", FRAMES)
    e.newPropertyUInt("days_total", DAYS)
    e.newPropertyArrayUInt32("mp3_ll_by_frame", ll_by_f)
    e.newPropertyArrayUInt32("mp1_oh_by_frame", oh_by_f)
    e.newPropertyArrayUInt32("mp1_br_by_frame", br_by_f)
    # MP5 MacroProperty с фиксированными размерами (константы объявлены выше)
    e.newMacroPropertyUInt32("mp5_lin", MAX_SIZE)

    a = model.newAgent("component")
    for name in [
        "idx","aircraft_number","partseqno_i","group_by",
        "status_id","sne","ppr","repair_days","repair_time",
        "daily_today_u32","daily_next_u32","ll","oh","s6_days","s6_started"
    ]:
        a.newVariableUInt(name, 0)

    import pyflamegpu as fg

    # Статус 2/4/6 + чтение dt/dn из mp5_lin MacroProperty
    rtc_246 = f"""
    FLAMEGPU_AGENT_FUNCTION(rtc_status_246_run, flamegpu::MessageNone, flamegpu::MessageNone) {{
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        const unsigned int gb = FLAMEGPU->getVariable<unsigned int>("group_by");
        if (!(gb == 1u || gb == 2u)) return flamegpu::ALIVE;
        
        // Чтение MP5 из MacroProperty
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int DAYS = FLAMEGPU->environment.getProperty<unsigned int>("days_total");
        const unsigned int d = (day < DAYS ? day : (DAYS > 0u ? DAYS - 1u : 0u));
        const unsigned int base = d * {MAX_FRAMES}u + i;
        const unsigned int base_next = base + {MAX_FRAMES}u;
        auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, {MAX_SIZE}u>("mp5_lin");
        const unsigned int dt = mp[base];
        const unsigned int dn = (d < DAYS ? mp[base_next] : 0u);
        FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
        FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
        
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
            rd = rd + 1u;
            FLAMEGPU->setVariable<unsigned int>("repair_days", rd);
            const unsigned int rt = FLAMEGPU->getVariable<unsigned int>("repair_time");
            if (rt > 0u && rd >= rt) {{
                FLAMEGPU->setVariable<unsigned int>("status_id", 5u);
                // Сайд эффекты: обнуление PPR и REPAIR_DAYS при выходе из ремонта
                FLAMEGPU->setVariable<unsigned int>("ppr", 0u);
                FLAMEGPU->setVariable<unsigned int>("repair_days", 0u);
            }}
        }} else if (sid == 6u) {{
            unsigned int s6d = FLAMEGPU->getVariable<unsigned int>("s6_days");
            FLAMEGPU->setVariable<unsigned int>("s6_days", s6d + 1u);
        }}
        return flamegpu::ALIVE;
    }}
    """
    a.newRTCFunction("rtc_status_246_run", rtc_246)

    # HostFunction для инициализации mp5_lin
    class HF_InitMP5(fg.HostFunction):
        def __init__(self, data: list[int], frames: int, days: int):
            super().__init__()
            self.data = data
            self.frames = frames
            self.days = days

        def run(self, FLAMEGPU):
            # Выполняется только на шаге 0
            if FLAMEGPU.getStepCounter() > 0:
                return
            mp = FLAMEGPU.environment.getMacroPropertyUInt32("mp5_lin")
            print(f"HF_InitMP5: Инициализация mp5_lin для FRAMES={self.frames}, DAYS={self.days}")
            for d in range(self.days + 1):
                for f in range(self.frames):
                    src_idx = d * self.frames + f
                    dst_idx = d * MAX_FRAMES + f  # Используем MAX_FRAMES для индексации
                    if src_idx < len(self.data):
                        mp[dst_idx] = self.data[src_idx]
            print(f"HF_InitMP5: Инициализировано {(self.days+1)*self.frames} элементов")

    # Источник MP5 линейный массив
    mp5 = list(env['mp5_daily_hours_linear'])
    need = (DAYS + 1) * FRAMES
    mp5 = mp5[:need]
    assert len(mp5) == need, f"mp5 length mismatch: {len(mp5)} != {need}"

    # Слои: инициализация MP5 + статус 246
    l0 = model.newLayer()
    l0.addHostFunction(HF_InitMP5(mp5, FRAMES, DAYS))
    l1 = model.newLayer(); l1.addAgentFunction(a.getFunction("rtc_status_246_run"))

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
    rd_list  = mp3.get('mp3_repair_days', [])
    m = min(len(ac_list), len(st_list), len(sne_list), len(ppr_list))
    for row in range(m):
        ac = int(ac_list[row] or 0)
        agg_by_ac[ac] = {
            'group_by': int(gb_list[row] or 0) if row < len(gb_list) else 0,
            'status_id': int(st_list[row] or 0),
            'sne': int(sne_list[row] or 0),
            'ppr': int(ppr_list[row] or 0),
            'partseqno_i': int(psn_list[row] or 0) if row < len(psn_list) else 0,
            'repair_days': int(rd_list[row] or 0) if row < len(rd_list) else 0,
        }
    try:
        vdate, vid = fetch_versions(client)
        sql = (
            "SELECT aircraft_number, anyLast(sne) sne, anyLast(ppr) ppr, anyLast(status_id) sid, anyLast(group_by) gb, anyLast(repair_days) rd "
            f"FROM heli_pandas WHERE version_date = '{vdate}' AND version_id = {vid} AND group_by IN (1,2) GROUP BY aircraft_number"
        )
        rows = client.execute(sql)
        for ac, sne_v, ppr_v, sid_v, gb_v, rd_v in rows:
            ac_i = int(ac or 0)
            base = agg_by_ac.get(ac_i, {})
            base.update({
                'group_by': int(gb_v or base.get('group_by', 0)),
                'status_id': int(sid_v or base.get('status_id', 0)),
                'sne': int(sne_v or base.get('sne', 0)),
                'ppr': int(ppr_v or base.get('ppr', 0)),
                'repair_days': int(rd_v or base.get('repair_days', 0)),
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
            av[i].setVariableUInt("repair_days", int(base.get('repair_days', 0)))
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
    rt_arr = env.get('mp1_repair_time', [])
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
    rt_by_f = [0] * FRAMES
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
            # Норматив ремонта в днях
            rt_by_f[i] = int(rt_arr[pidx] if pidx < len(rt_arr) else 0)

    # Обновим Env после пересчёта порогов
    sim.setEnvironmentPropertyArrayUInt32("mp1_oh_by_frame", oh_by_f)
    sim.setEnvironmentPropertyArrayUInt32("mp1_br_by_frame", br_by_f)
    # Проставим repair_time агентам
    for i in range(FRAMES):
        av[i].setVariableUInt("repair_time", int(rt_by_f[i] if 0 <= i < len(rt_by_f) else 0))
    sim.setPopulationData(av)

    # D0 сверка с базой heli_pandas (anyLast) перед шагами
    try:
        vdate, vid = fetch_versions(client)
        sql = (
            "SELECT aircraft_number, anyLast(sne) sne, anyLast(ppr) ppr, anyLast(status_id) sid, anyLast(repair_days) rd "
            f"FROM heli_pandas WHERE version_date = '{vdate}' AND version_id = {vid} AND group_by IN (1,2) GROUP BY aircraft_number"
        )
        rows = client.execute(sql)
        base_map = {int(ac): (int(sne or 0), int(ppr or 0), int(sid or 0), int(rd or 0)) for ac, sne, ppr, sid, rd in rows}
        out0 = fg.AgentVector(a)
        sim.getPopulationData(out0)
        mism_sne = mism_ppr = mism_sid = 0
        zero_status_should_be_plane: List[int] = []
        for ac, fi in frames_index.items():
            if ac in base_map:
                sne_b, ppr_b, sid_b, rd_b = base_map[ac]
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
    prev_rd = [int(prev_vec[i].getVariableUInt("repair_days")) for i in range(FRAMES)]
    prev_s6d = [int(prev_vec[i].getVariableUInt("s6_days")) for i in range(FRAMES)]
    # День входа в статус 4 (1-based). Для стартовых S4 считаем вход на D1
    entered_s4_day = [1 if prev_status[i] == 4 else -1 for i in range(FRAMES)]
    entered_s4_rd_start = [int(prev_rd[i]) if prev_status[i] == 4 else -1 for i in range(FRAMES)]
    s6_break_total = 0
    s6_break_samples: List[tuple] = []  # (day, ac, prev->cur)
    s4_rd_viol_total = 0
    s4_rd_viol_samples: List[tuple] = []  # (day, ac, rd_prev, rd_cur)

    for d in range(DAYS):
        # Шаг симуляции (MP5 читается из MacroProperty внутри RTC)
        sim.step()
        # Подсчёт переходов за день
        cur_all = fg.AgentVector(a)
        sim.getPopulationData(cur_all)
        for i in range(FRAMES):
            ps = prev_status[i]
            cs = int(cur_all[i].getVariableUInt("status_id"))
            rd_prev = int(prev_rd[i])
            rd_cur = int(cur_all[i].getVariableUInt("repair_days"))
            s6d_prev = int(prev_s6d[i])
            s6d_cur = int(cur_all[i].getVariableUInt("s6_days"))
            # Инвариант S6: не должен выходить из 6
            if ps == 6 and cs != 6:
                s6_break_total += 1
                if len(s6_break_samples) < 10:
                    acn = int(cur_all[i].getVariableUInt("aircraft_number"))
                    s6_break_samples.append((d+1, acn, ps, cs))
            # Инкремент repair_days в статусе 4 при удержании 4
            if ps == 4 and cs == 4 and not (rd_cur == rd_prev + 1):
                s4_rd_viol_total += 1
                if len(s4_rd_viol_samples) < 10:
                    acn = int(cur_all[i].getVariableUInt("aircraft_number"))
                    s4_rd_viol_samples.append((d+1, acn, rd_prev, rd_cur))
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
                # Зафиксируем день входа в 4, если ранее не фиксировали
                if entered_s4_day[i] < 0:
                    entered_s4_day[i] = d + 1
                    entered_s4_rd_start[i] = int(cur_all[i].getVariableUInt("repair_days"))
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
                rt_v = int(cur_all[i].getVariableUInt("repair_time"))
                s4_day = entered_s4_day[i]
                rd_start = int(entered_s4_rd_start[i]) if entered_s4_rd_start[i] >= 0 else int(prev_rd[i])
                delta_elapsed = (d + 1 - s4_day) if s4_day > 0 else -1
                delta_needed = (rt_v - rd_start) if rt_v > 0 and rd_start >= 0 else -1
                trans45_info.append((day_str, acn, cs, sne_v, ppr_v, ll_v, oh_v, br_v, rt_v, delta_elapsed, delta_needed, rd_start))
            prev_status[i] = cs
            prev_rd[i] = rd_cur
            prev_s6d[i] = s6d_cur

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
    # Инварианты статусов 4/6
    print(f"status4_repair_days_increments_violations={s4_rd_viol_total}")
    if s4_rd_viol_samples:
        print("status4_rd_violations samples (day, ac, rd_prev, rd_cur):")
        for rec in s4_rd_viol_samples:
            print(f"  {rec[0]} ac={rec[1]} rd_prev={rec[2]} rd_cur={rec[3]}")
    print(f"status6_invariance_violations={s6_break_total}")
    if s6_break_samples:
        print("status6_break samples (day, ac, prev, cur):")
        for rec in s6_break_samples:
            print(f"  {rec[0]} ac={rec[1]} prev={rec[2]} cur={rec[3]}")
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
        print("transitions_4to5 details (day, ac, status, sne, ppr, ll, oh, br, rt, delta_elapsed, delta_needed, rd_start):")
        for rec in trans45_info[:200]:
            print(f"  {rec[0]} ac={rec[1]} st={rec[2]} sne={rec[3]} ppr={rec[4]} ll={rec[5]} oh={rec[6]} br={rec[7]} rt={rec[8]} d_elapsed={rec[9]} d_needed={rec[10]} rd0={rec[11]}")
        if len(trans45_info) > 200:
            print(f"  ... ({len(trans45_info)-200} more)")
    if timeline:
        print("trace_idx timeline (day, idx, dt, dn, sne, ppr, status, gb, ll, oh, br):")
        for rec in timeline[:15]:
            print(rec)
    return 0


if __name__ == "__main__":
    sys.exit(main())


