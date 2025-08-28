#!/usr/bin/env python3
"""
FLAME GPU GPU Runner

Строит модель, инициализирует агентов из MP3 (+br/repair_time из MP1),
проходит по дням из MP4/MP5, заполняет окружение (daily_today/daily_next и триггеры),
шагает симуляцию и пишет LoggingLayer Planes (MP2) в ClickHouse.

Дата: 2025-08-10
"""

from __future__ import annotations
from typing import Dict, List, Tuple
from datetime import date, timedelta
import time

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client
from flame_macroproperty2_exporter import FlameMacroProperty2Exporter
try:
    import pyflamegpu
except Exception:
    pyflamegpu = None

from flame_gpu_helicopter_model import HelicopterFlameModel

try:
    import pyflamegpu
except Exception as e:
    pyflamegpu = None


def fetch_versions(client):
    rows = client.execute(
        "SELECT version_date, version_id FROM heli_pandas ORDER BY version_date DESC, version_id DESC LIMIT 1"
    )
    vd, vid = rows[0]
    return vd, int(vid)


def fetch_mp1_br_rt(client) -> Dict[int, Tuple[int, int, int, int, int]]:
    rows = client.execute("SELECT partno_comp, br_mi8, br_mi17, repair_time, partout_time, assembly_time FROM md_components")
    return {int(p): (int(b8 or 0), int(b17 or 0), int(rt or 0), int(pt or 0), int(at or 0)) for p, b8, b17, rt, pt, at in rows}


def fetch_mp3(client, vdate, vid):
    fields = [
        'partseqno_i','psn','aircraft_number','ac_type_mask','group_by','status_id','status_change',
        'll','oh','oh_threshold','sne','ppr','repair_days','mfg_date','version_date'
    ]
    sql = f"""
    SELECT {', '.join(fields)}
    FROM heli_pandas
    WHERE version_date = '{vdate}' AND version_id = {vid}
    ORDER BY psn
    """
    rows = client.execute(sql)
    return rows, fields


def fetch_dates(client) -> List[date]:
    return [r[0] for r in client.execute("SELECT dates FROM flight_program_ac ORDER BY dates")]


def fetch_daily_map(client, d: date) -> Dict[int,int]:
    rows = client.execute("SELECT aircraft_number, daily_hours FROM flight_program_fl WHERE dates = %(d)s", {"d": d})
    return {int(ac): int(h or 0) for ac, h in rows}


def preload_mp4_by_day(client) -> Dict[date, Dict[str,int]]:
    """Загружает все записи MP4 в память: на дату → ops_counter/trigger значения."""
    rows = client.execute(
        """
        SELECT dates, ops_counter_mi8, ops_counter_mi17, trigger_program_mi8, trigger_program_mi17
        FROM flight_program_ac
        ORDER BY dates
        """
    )
    result: Dict[date, Dict[str,int]] = {}
    for d, mi8, mi17, t8, t17 in rows:
        result[d] = {
            "ops_counter_mi8": int(mi8 or 0),
            "ops_counter_mi17": int(mi17 or 0),
            "trigger_program_mi8": int(t8 or 0),
            "trigger_program_mi17": int(t17 or 0),
        }
    return result


def preload_mp5_maps(client) -> Dict[date, Dict[int,int]]:
    """Загружает все суточные налёты в память: на дату → {aircraft_number: daily_hours}."""
    rows = client.execute(
        """
        SELECT dates, aircraft_number, daily_hours
        FROM flight_program_fl
        ORDER BY dates, aircraft_number
        """
    )
    result: Dict[date, Dict[int,int]] = {}
    for d, ac, h in rows:
        m = result.setdefault(d, {})
        m[int(ac)] = int(h or 0)
    return result


def fetch_ops_and_triggers(client, d: date) -> Dict[str,int]:
    rows = client.execute("""
        SELECT ops_counter_mi8, ops_counter_mi17, trigger_program_mi8, trigger_program_mi17
        FROM flight_program_ac WHERE dates = %(d)s
    """, {"d": d})
    if not rows:
        return {"ops_counter_mi8":0, "ops_counter_mi17":0, "trigger_program_mi8":0, "trigger_program_mi17":0}
    mi8, mi17, t8, t17 = rows[0]
    return {"ops_counter_mi8": int(mi8 or 0), "ops_counter_mi17": int(mi17 or 0), "trigger_program_mi8": int(t8 or 0), "trigger_program_mi17": int(t17 or 0)}


def init_agents(sim: "pyflamegpu.CUDASimulation", model: HelicopterFlameModel, mp3_rows, mp3_fields, br_rt_map):
    assert pyflamegpu is not None
    comp_desc = model.model.getAgent("component")
    av = pyflamegpu.AgentVector(comp_desc, len(mp3_rows))
    idx_map = {name: i for i, name in enumerate(mp3_fields)}
    for i, r in enumerate(mp3_rows):
        ai = av[i]
        ai.setVariableUInt("idx", i)
        ai.setVariableUInt("partseqno_i", int(r[idx_map['partseqno_i']] or 0))
        ai.setVariableUInt("psn", int(r[idx_map['psn']] or 0))
        ai.setVariableUInt("aircraft_number", int(r[idx_map['aircraft_number']] or 0))
        ai.setVariableUInt("group_by", int(r[idx_map['group_by']] or 0))
        ai.setVariableUInt("ac_type_mask", int(r[idx_map['ac_type_mask']] or 0))
        ai.setVariableUInt("status_id", int(r[idx_map['status_id']] or 0))
        ai.setVariableUInt("status_change", int(r[idx_map['status_change']] or 0))
        ai.setVariableUInt("ll", int(r[idx_map['ll']] or 0))
        ai.setVariableUInt("oh", int(r[idx_map['oh']] or 0))
        ai.setVariableUInt("oh_threshold", int(r[idx_map['oh_threshold']] or 0))
        ai.setVariableUInt("sne", int(r[idx_map['sne']] or 0))
        ai.setVariableUInt("ppr", int(r[idx_map['ppr']] or 0))
        ai.setVariableUInt("repair_days", int(r[idx_map['repair_days']] or 0))
        ai.setVariableUInt("mfg_date", int(getattr(r[idx_map['mfg_date']], 'toordinal', lambda: 0)()))
        ai.setVariableUInt("version_date", int(getattr(r[idx_map['version_date']], 'toordinal', lambda: 0)()))
        partseq = int(r[idx_map['partseqno_i']] or 0)
        br, rt, pt, at = br_rt_map.get(partseq, (0, 0, 0, 0))
        ai.setVariableUInt("br", int(br))
        ai.setVariableUInt("repair_time", int(rt))
        ai.setVariableUInt("partout_time", int(pt))
        ai.setVariableUInt("assembly_time", int(at))
    # Загрузка популяции в симуляцию
    sim.setPopulationData(av)


def fill_daily_arrays(sim: "pyflamegpu.CUDASimulation", daily_today: List[int], daily_next: List[int]):
    sim.setEnvironmentPropertyArrayUInt32("daily_today", daily_today)
    sim.setEnvironmentPropertyArrayUInt32("daily_next", daily_next)


def fill_time_arrays(sim: "pyflamegpu.CUDASimulation", partout: List[int], assembly: List[int]):
    sim.setEnvironmentPropertyArrayUInt32("partout_time_arr", partout)
    sim.setEnvironmentPropertyArrayUInt32("assembly_time_arr", assembly)


def run(days: int | None = None):
    if pyflamegpu is None:
        raise RuntimeError("pyflamegpu не установлен")

    client = get_clickhouse_client()
    vdate, vid = fetch_versions(client)
    br_rt_map = fetch_mp1_br_rt(client)
    mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
    n = len(mp3_rows)

    model = HelicopterFlameModel()
    model.build_model(num_agents=n)
    sim = model.build_simulation()

    # Создать популяцию агентов
    init_agents(sim, model, mp3_rows, mp3_fields, br_rt_map)

    # Создать singleton-контроллер (для будущей GPU-балансировки на сообщениях)
    try:
        ctrl_desc = model.model.getAgent("controller")
        cv = pyflamegpu.AgentVector(ctrl_desc, 1)
        cv[0].setVariableUInt("day_index", 0)
        sim.setPopulationData(cv)
    except Exception:
        pass

    # Экспортер MP2
    exporter = FlameMacroProperty2Exporter(client=client)
    exporter.ensure_table()

    # Даты симуляции
    days_list = fetch_dates(client)
    if days:
        days_list = days_list[:days]

    # Однократная загрузка MP4/MP5 в память
    mp4_by_day = preload_mp4_by_day(client)
    mp5_maps = preload_mp5_maps(client)

    # Сводные метрики таймингов
    total_step_s = 0.0
    total_export_s = 0.0
    total_rows = 0

    for i, d in enumerate(days_list):
        # Подготовка окружения на сутки D
        today_map = mp5_maps.get(d, {})
        next_map = mp5_maps.get(d + timedelta(days=1), {})
        # Сформировать массивы по индексу агента (по idx)
        # Для планеров берем daily по их aircraft_number; прочие агрегаты получают 0
        idx_map = {name: i for i, name in enumerate(mp3_fields)}
        daily_today = []
        daily_next = []
        part_arr = []
        asm_arr = []
        for r in mp3_rows:
            ac = int(r[idx_map['aircraft_number']] or 0)
            daily_today.append(int(today_map.get(ac, 0)))
            daily_next.append(int(next_map.get(ac, 0)))
            partseq = int(r[idx_map['partseqno_i']] or 0)
            _, _, pt, at = br_rt_map.get(partseq, (0,0,0,0))
            part_arr.append(int(pt))
            asm_arr.append(int(at))
        fill_daily_arrays(sim, daily_today, daily_next)
        fill_time_arrays(sim, part_arr, asm_arr)

        # Установить триггеры из MP4
        ops = mp4_by_day.get(d, {"ops_counter_mi8":0, "ops_counter_mi17":0, "trigger_program_mi8":0, "trigger_program_mi17":0})
        # Триггеры балансировки задаём как target (runner может предварительно балансировать, но здесь только запись)
        sim.setEnvironmentPropertyInt("trigger_pr_final_mi8", int(ops.get('ops_counter_mi8', 0)))
        sim.setEnvironmentPropertyInt("trigger_pr_final_mi17", int(ops.get('ops_counter_mi17', 0)))
        sim.setEnvironmentPropertyInt("trigger_program_mi8", int(ops.get('trigger_program_mi8', 0)))
        sim.setEnvironmentPropertyInt("trigger_program_mi17", int(ops.get('trigger_program_mi17', 0)))
        sim.setEnvironmentPropertyInt("current_day_index", i)
        # Передаём текущую дату как ordinal для расчёта триггеров (примем base 1970-01-01)
        base = date(1970,1,1)
        sim.setEnvironmentPropertyUInt("current_day_ordinal", (d - base).days)

        # Шаг симуляции (repair → ops_check → main → change → pass)
        t_step_start = time.time()
        sim.step()
        step_s = time.time() - t_step_start
        total_step_s += step_s

        # Логирование MP2 для планеров (group_by 1|2)
        a = sim.getAgents("component")
        ag_iter = a.getPopulationData()
        # Текущее число в эксплуатации по группам
        ops_cur = {1: 0, 2: 0}
        for ag in ag_iter:
            if ag.getVariableUInt("status_id") == 2 and ag.getVariableUInt("status_change") == 0:
                gb = ag.getVariableUInt("group_by")
                if gb in ops_cur:
                    ops_cur[gb] += 1
        # Повторно обойдём для записи строк
        ag_iter = a.getPopulationData()
        log_rows = []
        for ag in ag_iter:
            gb = ag.getVariableUInt("group_by")
            if gb not in (1, 2):
                continue
            ac = ag.getVariableUInt("aircraft_number")
            psn = ag.getVariableUInt("psn")
            partseqno_i = ag.getVariableUInt("partseqno_i")
            ac_type_mask = ag.getVariableUInt("ac_type_mask")
            status_id = ag.getVariableUInt("status_id")
            idx = ag.getVariableUInt("idx")
            daily_flight = int(daily_today[idx]) if idx < len(daily_today) else 0
            # mfg_date ord → Date
            md_ord = ag.getVariableUInt("mfg_date")
            md = date(1970,1,1) + timedelta(days=int(md_ord)) if md_ord else date(1970,1,1)
            # триггеры ord → Date
            part_ord = ag.getVariableUInt("partout_trigger_ord")
            asm_ord = ag.getVariableUInt("assembly_trigger_ord")
            act_ord = ag.getVariableUInt("active_trigger_ord")
            part_date = date(1970,1,1) + timedelta(days=int(part_ord)) if part_ord else date(1970,1,1)
            asm_date = date(1970,1,1) + timedelta(days=int(asm_ord)) if asm_ord else date(1970,1,1)
            act_date = date(1970,1,1) + timedelta(days=int(act_ord)) if act_ord else date(1970,1,1)
            log_rows.append({
                'dates': d,
                'psn': int(psn),
                'partseqno_i': int(partseqno_i),
                'aircraft_number': int(ac),
                'ac_type_mask': int(ac_type_mask),
                'status_id': int(status_id),
                'daily_flight': int(daily_flight),
                'ops_counter_mi8': int(ops.get('ops_counter_mi8', 0)),
                'ops_counter_mi17': int(ops.get('ops_counter_mi17', 0)),
                'ops_current_mi8': int(ops_cur.get(1, 0)),
                'ops_current_mi17': int(ops_cur.get(2, 0)),
                'partout_trigger': None,
                'assembly_trigger': None,
                'active_trigger': None,
                'aircraft_age_years': 0,
                'mfg_date': md,
                'sne': int(ag.getVariableUInt('sne')),
                'ppr': int(ag.getVariableUInt('ppr')),
                'repair_days': int(ag.getVariableUInt('repair_days')),
                'simulation_metadata': f"v={vdate}/id={vid};D={d}"
            })
        if log_rows:
            t_exp_start = time.time()
            exporter.insert_rows(log_rows)
            exp_s = time.time() - t_exp_start
            total_export_s += exp_s
            total_rows += len(log_rows)
            # Диагностический лог по дню
            try:
                print(f"GPU day={d} step_ms={step_s*1000:.1f} export_ms={exp_s*1000:.1f} rows={len(log_rows)} ops_cur={ops_cur.get(1,0)}/{ops_cur.get(2,0)} ops_tgt={ops.get('ops_counter_mi8',0)}/{ops.get('ops_counter_mi17',0)}")
            except Exception:
                pass


def main():
    import argparse
    parser = argparse.ArgumentParser(description='FLAME GPU GPU Runner')
    parser.add_argument('--days', type=int, default=None, help='Сколько дней симулировать')
    args = parser.parse_args()
    start_time = time.time()
    run(days=args.days)
    total_time = time.time() - start_time
    print(f"\n⏱️ Общее время выполнения GPU-runner: {total_time:.1f} секунд")
    # Печать сводной статистики, если она собиралась внутри run (через замыкание не доступна)


if __name__ == '__main__':
    main()