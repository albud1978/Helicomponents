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
from sim_env_setup import get_client, prepare_env_arrays, fetch_versions


def build_ll_oh_by_frame(env: Dict[str, object], frames: int) -> (List[int], List[int]):
    a = env.get('mp3_arrays', {})
    ac_list = a.get('mp3_aircraft_number', [])
    ll_list = a.get('mp3_ll', [])
    oh_list = a.get('mp3_oh', [])
    gb_list = a.get('mp3_group_by', [])
    frames_index = env.get('frames_index', {})
    # Автоподстановка trace_idx по HL_V2_CHECK_ACN (если задан acn)
    check_acn_env = os.environ.get('HL_V2_CHECK_ACN', '').strip()
    if check_acn_env.isdigit():
        acn_for_trace = int(check_acn_env)
        mapped_idx = int(frames_index.get(acn_for_trace, -1))
        print(f"[trace] ACN {acn_for_trace} → frame_idx {mapped_idx}")
        if mapped_idx >= 0:
            os.environ['HL_V2_TRACE_IDX'] = str(mapped_idx)
    out_ll = [0] * frames
    out_oh = [0] * frames
    for i in range(min(len(ac_list), len(ll_list), len(oh_list), len(gb_list) if gb_list else len(ac_list))):
        ac = int(ac_list[i] or 0)
        gb = int(gb_list[i] or 0) if gb_list else 0
        if gb not in (1, 2):
            continue
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
    # База адреса MP5 для текущего дня (обновляется на хосте перед шагом)
    e.newPropertyUInt("mp5_day_base", 0)
    # Env arrays
    # MP5 источник для RTC: PropertyArray (DAYS+1)*FRAMES
    e.newPropertyArrayUInt32("mp5_src", [0] * MP5_SIZE)
    # Для будущей полной MacroProperty и экспорта (опционально)
    e.newMacroPropertyUInt32("mp5_lin", FRAMES * (DAYS + 1))
    # База адресации MP5 на текущий день (без умножений в RTC)
    # Уже объявлена ранее — пропускаем повторное объявление
    e.newPropertyArrayUInt32("mp3_ll_by_frame", ll_by_f)
    mp3 = env.get('mp3_arrays', {})
    ac_list = mp3.get('mp3_aircraft_number', [])
    frames_index = env.get('frames_index', {})
    # Построим соответствие partseq по кадрам с учётом gb: берём row из MP3 с тем же gb
    pseq_by_f = [0] * FRAMES
    mp3_gb_list = mp3.get('mp3_group_by', [])
    partseq_list = mp3.get('mp3_partseqno_i', [])
    for row_idx in range(min(len(partseq_list), len(mp3_gb_list), len(ac_list))):
        ac = int(ac_list[row_idx] or 0)
        fi = int(frames_index.get(ac, -1))
        if 0 <= fi < FRAMES:
            gb_row = int(mp3_gb_list[row_idx] or 0)
            if gb_row in (1,2) and gb_row == int(gb_by_f[fi] or 0):
                pseq_by_f[fi] = int(partseq_list[row_idx] or 0)

    # BR по типу ВС из MP1: формируем по кадру (gb_by_frame + pseq_by_f)
    br_by_f = [0] * FRAMES
    mp1_idx_map = env.get('mp1_index', {})
    br8 = env.get('mp1_br_mi8', [])
    br17 = env.get('mp1_br_mi17', [])
    frames_index = env.get('frames_index', {})
    for fi in range(FRAMES):
        ac = None  # не требуется
        pseq = int(pseq_by_f[fi] or 0)
        pidx = int(mp1_idx_map.get(pseq, -1))
        brv = 0
        if pidx >= 0:
            gb_frame = int(gb_by_f[fi] or 0)
            if gb_frame == 1 and pidx < len(br8):
                brv = int(br8[pidx] or 0)
            elif gb_frame == 2 and pidx < len(br17):
                brv = int(br17[pidx] or 0)
        br_by_f[fi] = brv
    e.newPropertyArrayUInt32("mp1_br_by_frame", br_by_f)
    # OH из MP1 по partseq: значения берём как есть (минуты), выбор по типу ВС
    # gb=1 → oh_mi8; gb=2 → oh_mi17
    oh_by_f = [0] * FRAMES
    oh8 = env.get('mp1_oh_mi8', [])
    oh17 = env.get('mp1_oh_mi17', [])
    for fi in range(FRAMES):
        pseq = int(pseq_by_f[fi] or 0)
        pidx = int(mp1_idx_map.get(pseq, -1))
        oh_minutes = 0
        if pidx >= 0:
            gb_frame = int(gb_by_f[fi] or 0)
            if gb_frame == 1 and pidx < len(oh8):
                oh_minutes = int(oh8[pidx] or 0)
            elif gb_frame == 2 and pidx < len(oh17):
                oh_minutes = int(oh17[pidx] or 0)
        oh_by_f[fi] = oh_minutes
    e.newPropertyArrayUInt32("mp1_oh_by_frame", oh_by_f)

    a = model.newAgent("component")
    for name in [
        "idx","aircraft_number","psn","partseqno_i","group_by",
        "status_id","sne","ppr","repair_days",
        "daily_today_u32","daily_next_u32","ll","oh","s6_days","s6_started"
    ]:
        a.newVariableUInt(name, 0)

    # HostFunction init: заполнение mp5_lin из mp5_daily_hours_linear (без RTC)
    import pyflamegpu as fg
    class HF_Init_MP5(fg.HostFunction):
        def run(self, sim):
            envp = sim.getEnvironment()
            mp = envp.getMacroPropertyUInt32("mp5_lin")
            # Загружаем полный горизонт (DAYS_full из снапшота env, а не STEPS)
            for k, v in enumerate(self._payload):
                mp[k].exchange(int(v))
    
    hf = HF_Init_MP5()

    # Единое ядро: rtc_status_246 (probe MP5 из PropertyArray + логика статусов 2/4/6)
    rtc_246 = """
    FLAMEGPU_AGENT_FUNCTION(rtc_status_246_run, flamegpu::MessageNone, flamegpu::MessageNone) {{
        static const unsigned int FR = {FRAMES}u;
        static const unsigned int DY = {DAYS}u;
        const unsigned int i = FLAMEGPU->getVariable<unsigned int>("idx");
        if (i >= FR) return flamegpu::ALIVE;
        const unsigned int day = FLAMEGPU->getStepCounter();
        const unsigned int d = (day < DY ? day : (DY > 0u ? DY - 1u : 0u));
        const unsigned int base = d * FR + i;
        const unsigned int base_next = base + FR;
        // Чтение MP5 из MacroProperty
        auto mp = FLAMEGPU->environment.getMacroProperty<unsigned int, (FR*(DY+1))>("mp5_lin");
        const unsigned int dt = mp[base];
        const unsigned int dn = (d < DY) ? mp[base_next] : 0u;
        FLAMEGPU->setVariable<unsigned int>("daily_today_u32", dt);
        FLAMEGPU->setVariable<unsigned int>("daily_next_u32", dn);
        // Фильтр по планёрам: только group_by ∈ {{1,2}} влияют на логику статусов
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
            if (ll > 0u && s_next >= ll) {{
                // LL → неремонтопригоден
                FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
                FLAMEGPU->setVariable<unsigned int>("s6_days", 0u);
                FLAMEGPU->setVariable<unsigned int>("s6_started", 1u);
            }} else if (oh > 0u && p_next >= oh) {{
                // OH: если s_next >= br → 6, иначе 4
                if (br > 0u && s_next >= br) {{
                    FLAMEGPU->setVariable<unsigned int>("status_id", 6u);
                    FLAMEGPU->setVariable<unsigned int>("s6_days", 0u);
                    FLAMEGPU->setVariable<unsigned int>("s6_started", 1u);
                }} else {{
                    FLAMEGPU->setVariable<unsigned int>("status_id", 4u);
                }}
            }}
        }} else if (sid == 4u) {{
            // Минимальная логика ремонта: инкремент счётчика (без переходов)
            unsigned int rd = FLAMEGPU->getVariable<unsigned int>("repair_days");
            FLAMEGPU->setVariable<unsigned int>("repair_days", rd + 1u);
        }} else if (sid == 6u) {{
            // Хранение: инкремент дней хранения (без переходов)
            unsigned int s6d = FLAMEGPU->getVariable<unsigned int>("s6_days");
            FLAMEGPU->setVariable<unsigned int>("s6_days", s6d + 1u);
        }}
        return flamegpu::ALIVE;
    }}
    """.format(FRAMES=FRAMES, DAYS=DAYS)
    if os.environ.get('HL_V2_DEBUG_RTC', '0') == '1':
        try:
            print("--- rtc_status_246 source (truncated) ---")
            src_lines = rtc_246.splitlines()
            for ln in src_lines[:120]:
                print(ln)
            print("--- end source ---")
        except Exception:
            pass
    a.newRTCFunction("rtc_status_246_run", rtc_246)

    # Слои: единое ядро
    l0 = model.newLayer(); l0.addAgentFunction(a.getFunction("rtc_status_246_run"))

    sim = fg.CUDASimulation(model)
    sim.setEnvironmentPropertyUInt("version_date", int(env['version_date_u16']))
    sim.setEnvironmentPropertyUInt("frames_total", FRAMES)
    sim.setEnvironmentPropertyUInt("days_total", DAYS)
    # MP5: хост‑инициализация MacroProperty для текущего горизонта STEPS (DAYS+1)*FRAMES
    mp5 = list(env['mp5_daily_hours_linear'])
    need_steps = (DAYS + 1) * FRAMES
    mp5_steps = mp5[:need_steps]
    assert len(mp5_steps) == need_steps, f"mp5_linear length mismatch: {len(mp5_steps)} != {need_steps}"
    hf._payload = mp5_steps
    model.addInitFunction(hf)
    sim.setEnvironmentPropertyArrayUInt32("mp3_ll_by_frame", ll_by_f)
    sim.setEnvironmentPropertyArrayUInt32("mp1_oh_by_frame", oh_by_f)

    # Популяция: стартовые статусы и sne/ppr по ACN (агрегация по планёру)
    av = fg.AgentVector(a, FRAMES)
    mp3 = env.get('mp3_arrays', {})
    ac_list  = mp3.get('mp3_aircraft_number', [])
    st_list  = mp3.get('mp3_status_id', [])
    sne_list = mp3.get('mp3_sne', [])
    ppr_list = mp3.get('mp3_ppr', [])
    psn_list = mp3.get('mp3_psn', [])
    partseq_list = mp3.get('mp3_partseqno_i', [])
    ll_row   = mp3.get('mp3_ll', [])
    oh_row   = mp3.get('mp3_oh', [])
    gb_list  = mp3.get('mp3_group_by', [])
    rd_list  = mp3.get('mp3_repair_days', [])
    frames_index = env.get('frames_index', {})
    # Соберём агрегат по ACN из MP3 (последняя запись) и из БД (anyLast) для точного совпадения
    agg_by_ac: Dict[int, Dict[str,int]] = {}
    n = min(len(ac_list), len(st_list), len(sne_list), len(ppr_list))
    for row_idx in range(n):
        ac = int(ac_list[row_idx] or 0)
        agg_by_ac[ac] = {
            'status_id': int(st_list[row_idx] or 0),
            'sne': int(sne_list[row_idx] or 0),
            'ppr': int(ppr_list[row_idx] or 0),
            'psn': int(psn_list[row_idx] or 0) if row_idx < len(psn_list) else 0,
            'partseqno_i': int(partseq_list[row_idx] or 0) if row_idx < len(partseq_list) else 0,
            'group_by': int(gb_list[row_idx] or 0) if row_idx < len(gb_list) else 0,
            'repair_days': int(rd_list[row_idx] or 0) if row_idx < len(rd_list) else 0,
            'll': int(ll_row[row_idx] or 0) if row_idx < len(ll_row) else 0,
            'oh': int(oh_row[row_idx] or 0) if row_idx < len(oh_row) else 0,
        }
    # Перебьём базу по данным из ClickHouse агрегатом anyLast на версию (источник истины heli_pandas)
    try:
        client = get_client()
        vdate, vid = fetch_versions(client)
        sql = (
            "SELECT aircraft_number, anyLast(sne) sne, anyLast(ppr) ppr, anyLast(ll) ll, anyLast(oh) oh, anyLast(status_id) sid "
            f"FROM heli_pandas WHERE version_date = '{vdate}' AND version_id = {vid} AND group_by IN (1,2) GROUP BY aircraft_number"
        )
        rows = client.execute(sql)
        for ac, sne_v, ppr_v, ll_v, oh_v, sid_v in rows:
            ac_i = int(ac or 0)
            base = agg_by_ac.get(ac_i, {})
            base.update({
                'status_id': int(sid_v or base.get('status_id', 0)),
                'sne': int(sne_v or base.get('sne', 0)),
                'ppr': int(ppr_v or base.get('ppr', 0)),
                'll': int(ll_v or base.get('ll', 0)),
                'oh': int(oh_v or base.get('oh', 0)),
            })
            agg_by_ac[ac_i] = base
    except Exception as _e:
        # В случае проблем с запросом используем локальный агрегат MP3
        pass
    # Обратный индекс кадр->ACN
    inv_index: Dict[int,int] = {fi: ac for ac, fi in frames_index.items()}
    for i in range(FRAMES):
        av[i].setVariableUInt("idx", i)
        ac = int(inv_index.get(i, 0))
        base = agg_by_ac.get(ac, None)
        # По умолчанию — из распределений по кадрам
        sid = int(st_by_f[i] or 0)
        sne0 = 0
        ppr0 = 0
        ll0 = int(ll_by_f[i] or 0)
        oh0 = int(oh_by_f[i] or 0)
        psn0 = 0
        pseq0 = 0
        gb0 = int(gb_by_f[i] or 0)
        rd0 = 0
        if base is not None:
            sid = int(base['status_id'] or sid)
            sne0 = int(base['sne'] or 0)
            ppr0 = int(base['ppr'] or 0)
            ll0 = int(base['ll'] or ll0)
            oh0 = int(base['oh'] or oh0)
            psn0 = int(base['psn'] or 0)
            pseq0 = int(base['partseqno_i'] or 0)
            gb_from_base = int(base['group_by'] or 0)
            if gb_from_base in (1,2):
                gb0 = gb_from_base
            rd0 = int(base['repair_days'] or 0)
        av[i].setVariableUInt("aircraft_number", ac)
        av[i].setVariableUInt("psn", psn0)
        av[i].setVariableUInt("partseqno_i", pseq0)
        av[i].setVariableUInt("group_by", gb0)
        av[i].setVariableUInt("status_id", sid)
        av[i].setVariableUInt("sne", sne0)
        av[i].setVariableUInt("ppr", ppr0)
        av[i].setVariableUInt("repair_days", rd0)
        av[i].setVariableUInt("ll", ll0)
        av[i].setVariableUInt("oh", oh0)
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

    # D0 диагностика причин мгновенных 2→6/2→4
    # Используем исходные sne/ppr и пороги ll/oh/br; для проверки используем dt0, dn0 из mp5
    d0_2_total = 0
    d0_2_to6_ll = 0
    d0_2_to6_ohbr = 0
    d0_2_to4_oh = 0
    d0_2_to6_samples: List[int] = []
    d0_2_to4_samples: List[int] = []
    for i in range(FRAMES):
        if gb_by_f[i] not in (1,2):
            continue
        s0 = int(out0[i].getVariableUInt("status_id"))
        if s0 != 2:
            continue
        d0_2_total += 1
        sne0 = int(out0[i].getVariableUInt("sne"))
        ppr0 = int(out0[i].getVariableUInt("ppr"))
        llv = int(ll_by_f[i] or 0)
        ohv = int(oh_by_f[i] or 0)
        brv = int(br_by_f[i] or 0)
        dt0 = int(mp5[i] if i < len(mp5) else 0)
        dn0 = int(mp5[FRAMES + i] if (FRAMES + i) < len(mp5) else 0)
        s_next = sne0 + dn0
        # LL: если ll > 0 и уже на старте s_next >= ll → 2→6
        if llv > 0 and s_next >= llv:
            d0_2_to6_ll += 1
            if len(d0_2_to6_samples) < 10:
                d0_2_to6_samples.append(int(out0[i].getVariableUInt("aircraft_number")))
            continue
        # OH ветка: если ppr >= oh, то 2→6 при s_next>=br, иначе 2→4
        if ohv > 0 and ppr0 >= ohv:
            if brv > 0 and s_next >= brv:
                d0_2_to6_ohbr += 1
                if len(d0_2_to6_samples) < 10:
                    d0_2_to6_samples.append(int(out0[i].getVariableUInt("aircraft_number")))
            else:
                d0_2_to4_oh += 1
                if len(d0_2_to4_samples) < 10:
                    d0_2_to4_samples.append(int(out0[i].getVariableUInt("aircraft_number")))
        # BR проверяем только в связке с OH (см. ветку выше)

    print(f"D0_diag: S2_total={d0_2_total}, S2->6_by_LL={d0_2_to6_ll}, S2->6_by_OH_BR={d0_2_to6_ohbr}, S2->4_by_OH={d0_2_to4_oh}")
    if d0_2_to6_samples:
        print(f"D0_diag_samples_2to6(acn)={d0_2_to6_samples}")
    if d0_2_to4_samples:
        print(f"D0_diag_samples_2to4(acn)={d0_2_to4_samples}")

    # Запрос на проверку конкретных бортов: HL_V2_CHECK_ACN="22418,22211,..."
    check_env = os.environ.get('HL_V2_CHECK_ACN', '').strip()
    check_acns: List[int] = []
    if check_env:
        for tok in check_env.split(','):
            tok = tok.strip()
            if tok.isdigit():
                check_acns.append(int(tok))
    else:
        # Если не задано — проверим первые из выборок D0
        check_acns = d0_2_to6_samples[:10] + d0_2_to4_samples[:10]
    if check_acns:
        print("D0_check_acn (per-ACN LL/OH/BR и dt0/dn0, триггеры):")
        for acn in check_acns:
            fi = int(frames_index.get(int(acn), -1))
            if not (0 <= fi < FRAMES):
                print(f"  ac={acn}: not_in_frames")
                continue
            sid0 = int(out0[fi].getVariableUInt("status_id"))
            sne0 = int(out0[fi].getVariableUInt("sne"))
            ppr0 = int(out0[fi].getVariableUInt("ppr"))
            llv  = int(ll_by_f[fi] or 0)
            ohv  = int(oh_by_f[fi] or 0)
            brv  = int(br_by_f[fi] or 0)
            # Диагностика по pseq: (а) pseq агента из agg_by_ac, (б) pseq_by_f (по gb)
            pseq_agent = int(out0[fi].getVariableUInt("partseqno_i")) if "partseqno_i" else 0
            pidx_agent = int(env.get('mp1_index', {}).get(pseq_agent, -1))
            pseq_frame = int(pseq_by_f[fi] or 0)
            pidx_frame = int(env.get('mp1_index', {}).get(pseq_frame, -1))
            oh8_val = oh17_val = 0
            oh8_arr = env.get('mp1_oh_mi8', [])
            oh17_arr = env.get('mp1_oh_mi17', [])
            if 0 <= pidx_frame < len(oh8_arr):
                oh8_val = int(oh8_arr[pidx_frame] or 0)
            if 0 <= pidx_frame < len(oh17_arr):
                oh17_val = int(oh17_arr[pidx_frame] or 0)
            gbf = int(gb_by_f[fi] or 0)
            oh_expected = (oh8_val if gbf == 1 else oh17_val) if gbf in (1,2) else 0
            oh_agent = int(out0[fi].getVariableUInt("oh"))
            dt0  = int(mp5[fi] if fi < len(mp5) else 0)
            dn0  = int(mp5[FRAMES + fi] if (FRAMES + fi) < len(mp5) else 0)
            s_next = sne0 + dn0
            p_next = ppr0 + dn0
            trig_ll = (llv > 0 and s_next >= llv)
            trig_oh = (ohv > 0 and p_next >= ohv)
            trig_oh6 = (trig_oh and brv > 0 and s_next >= brv)
            result = "stay"
            if sid0 == 2:
                if trig_ll:
                    result = "2->6_LL"
                elif trig_oh6:
                    result = "2->6_OH_BR"
                elif trig_oh:
                    result = "2->4_OH"
            print(f"  ac={acn} idx={fi} sid0={sid0} sne0={sne0} ppr0={ppr0} ll={llv} oh={ohv} oh_agent={oh_agent} oh_expected={oh_expected} br={brv} pseq_agent={pseq_agent} pidx_agent={pidx_agent} pseq_frame={pseq_frame} pidx_frame={pidx_frame} oh_mi8={oh8_val} oh_mi17={oh17_val} dt0={dt0} dn0={dn0} s_next={s_next} p_next={p_next} trig_ll={int(trig_ll)} trig_oh={int(trig_oh)} trig_oh6={int(trig_oh6)} result={result}")

        # Сверка с источником heli_pandas и mp3_arrays
        try:
            client = get_client()
            vdate, vid = fetch_versions(client)
            acn_list = ",".join(str(x) for x in check_acns)
            sql = (
                "SELECT aircraft_number, anyLast(sne) sne, anyLast(ppr) ppr, anyLast(ll) ll, anyLast(oh) oh, anyLast(status_id) sid "
                f"FROM heli_pandas WHERE version_date = '{vdate}' AND version_id = {vid} "
                f"AND group_by IN (1,2) AND aircraft_number IN ({acn_list}) GROUP BY aircraft_number ORDER BY aircraft_number"
            )
            rows = client.execute(sql)
            print("heli_pandas anyLast by ACN:")
            for r in rows:
                print(f"  ac={int(r[0])} sne={int(r[1] or 0)} ppr={int(r[2] or 0)} ll={int(r[3] or 0)} oh={int(r[4] or 0)} sid={int(r[5] or 0)}")
        except Exception as _e:
            print(f"heli_pandas check failed: {_e}")
        # Отпечатка из локального агрегата agg_by_ac (собран ранее)
        print("mp3/agg_by_ac snapshot:")
        for acn in check_acns:
            base = agg_by_ac.get(int(acn), {})
            if base:
                print(f"  ac={acn} sne={base.get('sne',0)} ppr={base.get('ppr',0)} ll={base.get('ll',0)} oh={base.get('oh',0)} sid={base.get('status_id',0)} gb={base.get('group_by',0)}")
            else:
                print(f"  ac={acn} not found in agg_by_ac")

    # Шаги + опциональный трейс одного индекса
    trace_idx_env = os.environ.get('HL_V2_TRACE_IDX', '')
    trace_idx = int(trace_idx_env) if trace_idx_env.isdigit() else -1
    timeline = []  # (day, idx, dt, dn, sne, ppr, status)
    # Счётчики и логи переходов (для данного RTC оставляем только 2→4, 2→6, 4→5)
    trans_2_to_4 = 0
    trans_2_to_6 = 0
    trans_4_to_5 = 0

    trans24_log: List[tuple] = []
    trans26_log: List[tuple] = []
    trans45_log: List[tuple] = []
    strict_validation = os.environ.get('HL_V2_VALIDATION_STRICT', '0') == '1'
    # Отслеживание предыдущего статуса/счётчиков по всем кадрам (снимок списками)
    prev_vec = fg.AgentVector(a)
    sim.getPopulationData(prev_vec)
    prev_status = [int(prev_vec[i].getVariableUInt("status_id")) for i in range(FRAMES)]
    prev_sne = [int(prev_vec[i].getVariableUInt("sne")) for i in range(FRAMES)]
    for step_i in range(STEPS):
        # Перед шагом выставляем базу адреса для MP5 текущего дня (прямая адресация)
        sim.setEnvironmentPropertyUInt("mp5_day_base", int(step_i) * FRAMES)
        sim.step()
        cur = fg.AgentVector(a)
        sim.getPopulationData(cur)
        # Validation: s6 неизменяемость, s2 инкременты (используем текущий снимок без промежуточных host-подмен)
        s6_break = 0
        sum_dt_s2 = 0
        sum_sne_delta_s2 = 0
        for i in range(FRAMES):
            ps = prev_status[i]
            cs = int(cur[i].getVariableUInt("status_id"))
            if ps == 6 and cs != 6:
                s6_break += 1
            if ps == 2:
                dt_i = int(cur[i].getVariableUInt("daily_today_u32"))
                sne_cur = int(cur[i].getVariableUInt("sne"))
                sne_prev = int(prev_sne[i])
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
        # Счёт переходов (ограничено: 2→4, 2→6, 4→5)
        day_str = env['days_sorted'][step_i] if step_i < len(env.get('days_sorted', [])) else str(step_i)
        for i in range(FRAMES):
            ps = prev_status[i]
            cs = int(cur[i].getVariableUInt("status_id"))
            acn = int(cur[i].getVariableUInt("aircraft_number")) if "aircraft_number" else 0
            if ps == 2 and cs == 4:
                trans_2_to_4 += 1
                trans24_log.append((day_str, acn))
            elif ps == 2 and cs == 6:
                trans_2_to_6 += 1
                trans26_log.append((day_str, acn))
            elif ps == 4 and cs == 5:
                trans_4_to_5 += 1
                trans45_log.append((day_str, acn))
            prev_status[i] = cs
        # Обновляем prev-снимок для delta-метрик
        for i in range(FRAMES):
            prev_sne[i] = int(cur[i].getVariableUInt("sne"))

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

    # Печать логов переходов (если есть)
    def _print_log(title, log):
        if log:
            print(title)
            for dstr, acn in log[:200]:  # ограничим вывод
                print(f"  {dstr}: ac={acn}")

    _print_log("transitions_2to4:", trans24_log)
    _print_log("transitions_2to6:", trans26_log)
    _print_log("transitions_4to5:", trans45_log)
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


