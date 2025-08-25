#!/usr/bin/env python3
"""
Repair-only runner:
- Загружает MP1 (repair_time) и MP3 (статус/ремонт/идентификаторы)
- Создаёт популяцию агентов
- Выполняет 1 шаг rtc_repair
- Экспортирует MP2 за D0 (daily_flight=0)
Дата: 2025-08-21
"""

from __future__ import annotations
from typing import Dict, List, Tuple
from datetime import date, timedelta
import time
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client
from flame_macroproperty2_exporter import FlameMacroProperty2Exporter
from sim_env_setup import (
    fetch_versions, fetch_mp1_br_rt, fetch_mp3, preload_mp4_by_day, preload_mp5_maps, build_daily_arrays
)

from repair_only_model import RepairOnlyModel

try:
    import pyflamegpu
except Exception:
    pyflamegpu = None


def main():
    if pyflamegpu is None:
        raise RuntimeError("pyflamegpu не установлен")
    # CLI
    import argparse
    p = argparse.ArgumentParser(description='Repair-only runner')
    p.add_argument('--days', type=int, default=1, help='Сколько суток прогонять (начиная с D0)')
    p.add_argument('--clean-mp2', action='store_true', help='TRUNCATE flame_macroproperty2_export перед вставкой')
    p.add_argument('--verify-agents', action='store_true', help='Проверить, что agent.mfg_date заполнен (ordinal)')
    a = p.parse_args()
    # CUDA_PATH fallback
    if not os.environ.get('CUDA_PATH'):
        for p in [
            "/usr/local/cuda",
            "/usr/local/cuda-12.4",
            "/usr/local/cuda-12.3",
            "/usr/local/cuda-12.2",
            "/usr/local/cuda-12.1",
            "/usr/local/cuda-12.0",
        ]:
            if os.path.isdir(p) and os.path.isdir(os.path.join(p, 'include')):
                os.environ['CUDA_PATH'] = p
                break

    client = get_clickhouse_client()
    vdate, vid = fetch_versions(client)
    mp1_map = fetch_mp1_br_rt(client)  # partseqno_i -> (br_mi8, br_mi17, repair_time, partout_time, assembly_time)
    mp3_rows, mp3_fields = fetch_mp3(client, vdate, vid)
    n = len(mp3_rows)

    model = RepairOnlyModel()
    mp4 = preload_mp4_by_day(client)
    model.build_model(num_agents=n)
    sim = model.build_simulation()
    agent_desc = model.model.getAgent("component")

    # Создаём популяцию
    idx = {name: i for i, name in enumerate(mp3_fields)}
    # Маппинг точной даты производства по PSN из MP3
    psn_to_mfg: Dict[int, date] = {}
    av = pyflamegpu.AgentVector(agent_desc, n)
    for i, r in enumerate(mp3_rows):
        ai = av[i]
        ai.setVariableUInt("idx", i)
        ai.setVariableUInt("psn", int(r[idx['psn']] or 0))
        ai.setVariableUInt("partseqno_i", int(r[idx['partseqno_i']] or 0))
        ai.setVariableUInt("group_by", int(r[idx.get('group_by', -1)] or 0))
        ai.setVariableUInt("aircraft_number", int(r[idx.get('aircraft_number', -1)] or 0))
        ai.setVariableUInt("ac_type_mask", int(r[idx.get('ac_type_mask', -1)] or 0))
        # Сохраняем исходную дату производства для экспорта и записываем epoch-days (UInt16) в агентную переменную
        ord_val = 0
        if 'mfg_date' in idx:
            mfg_val = r[idx['mfg_date']]
            if mfg_val:
                psn_to_mfg[int(r[idx['psn']] or 0)] = mfg_val
                try:
                    # ClickHouse Date = дни от 1970-01-01
                    epoch = date(1970, 1, 1)
                    ord_val = max(0, int((mfg_val - epoch).days))
                except Exception:
                    ord_val = 0
        ai.setVariableUInt("mfg_date", ord_val)
        ai.setVariableUInt("status_id", int(r[idx['status_id']] or 0))
        ai.setVariableUInt("repair_days", int(r[idx['repair_days']] or 0))
        partseq = int(r[idx['partseqno_i']] or 0)
        b8, b17, rt, pt, at = mp1_map.get(partseq, (0, 0, 0, 0, 0))
        # Выбор BR по маске типа планера (у планера всегда один бит). br=0 => неремонтопригоден.
        mask = int(r[idx.get('ac_type_mask', -1)] or 0)
        br_val = 0
        if mask & 32:
            br_val = int(b8)
        elif mask & 64:
            br_val = int(b17)
        else:
            # Если маска не задана или не планер — порог остаётся 0 (неремонтопригоден)
            br_val = 0
        ai.setVariableUInt("repair_time", int(rt))
        ai.setVariableUInt("ppr", int(r[idx.get('ppr', -1)] or 0))
        ai.setVariableUInt("sne", int(r[idx.get('sne', -1)] or 0))
        ai.setVariableUInt("ll", int(r[idx.get('ll', -1)] or 0))
        ai.setVariableUInt("oh", int(r[idx.get('oh', -1)] or 0))
        ai.setVariableUInt("br", int(br_val))
    sim.setPopulationData(av)

    # Опциональная верификация агентных переменных
    if a.verify_agents:
        pop0 = pyflamegpu.AgentVector(agent_desc)
        sim.getPopulationData(pop0)
        zero_cnt = 0
        min_ord = None
        max_ord = None
        for ag in pop0:
            v = int(ag.getVariableUInt('mfg_date'))
            if v == 0:
                zero_cnt += 1
            if min_ord is None or v < min_ord:
                min_ord = v
            if max_ord is None or v > max_ord:
                max_ord = v
        print(f"Agent mfg_date: zeros={zero_cnt}/{n}, min_ord={min_ord}, max_ord={max_ord}")

    # Выполняем 1 шаг (D0): rtc_repair
    sim.step()

    # Экспорт MP2 за D0
    exporter = FlameMacroProperty2Exporter(client=client)
    exporter.ensure_table()
    if a.clean_mp2:
        client.execute(f"TRUNCATE TABLE {exporter.table_name}")

    mp5_maps = preload_mp5_maps(client)
    all_days = sorted(mp4.keys())
    if not all_days:
        raise RuntimeError("Нет дат в MP4")
    days_list = all_days[: a.days] if a.days else all_days[:1]

    total_step_s = 0.0
    total_export_s = 0.0
    all_rows: List[Dict] = []
    for day_idx, D in enumerate(days_list):
        t0 = time.perf_counter()
        # Перед шагом заполняем массивы суточных часов для ops_check
        today_map = mp5_maps.get(D, {})
        next_map = mp5_maps.get(D + timedelta(days=1), {})
        daily_today, daily_next, _, _ = build_daily_arrays(mp3_rows, mp3_fields, mp1_map, today_map, next_map)
        # Запишем суточные часы прямо в агентные переменные для RTC совместимости
        pop_buf = pyflamegpu.AgentVector(agent_desc)
        sim.getPopulationData(pop_buf)
        for i, ag in enumerate(pop_buf):
            ag.setVariableUInt('daily_today_u32', int(daily_today[i] if i < len(daily_today) else 0))
            ag.setVariableUInt('daily_next_u32', int(daily_next[i] if i < len(daily_next) else 0))
        sim.setPopulationData(pop_buf)
        # Fallback: прокидываем квоты на D+1 с host (до фикса NVRTC)
        ops_targets = mp4.get(D + timedelta(days=1), {"ops_counter_mi8": 0, "ops_counter_mi17": 0})
        sim.setEnvironmentPropertyUInt32("quota_next_mi8", int(ops_targets.get('ops_counter_mi8', 0)))
        sim.setEnvironmentPropertyUInt32("quota_next_mi17", int(ops_targets.get('ops_counter_mi17', 0)))
        # Шаг суток
        sim.step()
        t1 = time.perf_counter()

        # Экспорт строк за день D
        pop = pyflamegpu.AgentVector(agent_desc)
        sim.getPopulationData(pop)
        ops_current = {1: 0, 2: 0}
        quota_claimed = {1: 0, 2: 0}
        for ag in pop:
            gb = int(ag.getVariableUInt('group_by'))
            if ag.getVariableUInt("status_id") == 2 and gb in (1,2):
                ops_current[gb] += 1
            if gb in (1,2) and int(ag.getVariableUInt('ops_ticket')) == 1:
                quota_claimed[gb] += 1
        epoch = date(1970,1,1)
        rows: List[Dict] = []
        for ag in pop:
            gb = int(ag.getVariableUInt('group_by'))
            if gb not in (1, 2):
                continue
            # Точная дата из MP3 по PSN; если нет — NULL
            mfg_date = psn_to_mfg.get(int(ag.getVariableUInt('psn')), None)
            sid = int(ag.getVariableUInt('status_id'))
            daily_f = int(ag.getVariableUInt('daily_today_u32')) if sid == 2 else 0
            # Возраст воздушного судна в полных годах (округление вниз)
            age_years = 0
            if mfg_date is not None:
                years = D.year - mfg_date.year
                if (D.month, D.day) < (mfg_date.month, mfg_date.day):
                    years -= 1
                age_years = max(0, min(255, years))
            rows.append({
                'dates': D,
                'psn': int(ag.getVariableUInt('psn')),
                'partseqno_i': int(ag.getVariableUInt('partseqno_i')),
                'aircraft_number': int(ag.getVariableUInt('aircraft_number')),
                'ac_type_mask': int(ag.getVariableUInt('ac_type_mask')),
                'status_id': sid,
                'daily_flight': daily_f,
                'ops_counter_mi8': int(ops_targets.get('ops_counter_mi8', 0)),
                'ops_counter_mi17': int(ops_targets.get('ops_counter_mi17', 0)),
                'ops_current_mi8': int(ops_current.get(1, 0)),
                'ops_current_mi17': int(ops_current.get(2, 0)),
                'partout_trigger': None,
                'assembly_trigger': None,
                'active_trigger': None,
                'aircraft_age_years': int(age_years),
                'mfg_date': mfg_date,
                'sne': int(ag.getVariableUInt('sne')),
                'ppr': int(ag.getVariableUInt('ppr')),
                'repair_days': int(ag.getVariableUInt('repair_days')),
                'simulation_metadata': (
                    f"v={vdate}/id={vid};D={D};mode=repair_only;"
                    f"quota_seed_mi8={int(ops_targets.get('ops_counter_mi8', 0))},"
                    f"quota_seed_mi17={int(ops_targets.get('ops_counter_mi17', 0))},"
                    f"quota_claimed_mi8={int(quota_claimed.get(1,0))},"
                    f"quota_claimed_mi17={int(quota_claimed.get(2,0))}"
                )
            })
        # Копим строки, вставим одним батчем после цикла
        all_rows.extend(rows)

        step_s = t1 - t0
        total_step_s += step_s
        print(f"D={D} step={step_s*1000:.2f} ms, rows={len(rows)}")

    # Один батч на весь период
    e0 = time.perf_counter()
    if all_rows:
        exporter.insert_rows(all_rows)
    e1 = time.perf_counter()
    total_export_s = e1 - e0
    print(f"Totals: step={total_step_s*1000:.2f} ms, export={total_export_s*1000:.2f} ms, days={len(days_list)}, rows={len(all_rows)}")


if __name__ == '__main__':
    main()


