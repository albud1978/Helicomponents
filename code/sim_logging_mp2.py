#!/usr/bin/env python3
"""
Финальное логирование MP2 за сутки D (единый батч), расчёт ops_current_mi8(D).
Дата: 2025-08-21
"""

from __future__ import annotations
from typing import List, Dict, Any
from datetime import date, timedelta

import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from config_loader import get_clickhouse_client
from flame_macroproperty2_exporter import FlameMacroProperty2Exporter

try:
    import pyflamegpu
except Exception:
    pyflamegpu = None


def export_day(sim, agent_desc, d: date, daily_today: List[int], ops_targets: Dict[str,int], vdate: date, vid: int):
    client = get_clickhouse_client()
    exporter = FlameMacroProperty2Exporter(client=client)
    exporter.ensure_table()

    # Считываем популяцию через AgentVector API
    pop = pyflamegpu.AgentVector(agent_desc)
    sim.getPopulationData(pop)
    ops_current = {1: 0, 2: 0}
    for ag in pop:
        if ag.getVariableUInt("status_id") == 2:
            gb = ag.getVariableUInt("group_by")
            if gb in ops_current:
                ops_current[gb] += 1

    # Повторно для формирования строк
    pop = pyflamegpu.AgentVector(agent_desc)
    sim.getPopulationData(pop)
    rows: List[Dict[str, Any]] = []
    epoch = date(1970,1,1)
    for ag in pop:
        gb = ag.getVariableUInt("group_by")
        if gb not in (1,2):
            continue
        idx = ag.getVariableUInt("idx")
        md_ord = ag.getVariableUInt("mfg_date")
        md = epoch + timedelta(days=int(md_ord)) if md_ord else None
        part_ord = ag.getVariableUInt("partout_trigger_ord")
        asm_ord = ag.getVariableUInt("assembly_trigger_ord")
        act_ord = ag.getVariableUInt("active_trigger_ord")
        part_date = epoch + timedelta(days=int(part_ord)) if part_ord else None
        asm_date = epoch + timedelta(days=int(asm_ord)) if asm_ord else None
        act_date = epoch + timedelta(days=int(act_ord)) if act_ord else None
        rows.append({
            'dates': d,
            'psn': int(ag.getVariableUInt('psn')),
            'partseqno_i': int(ag.getVariableUInt('partseqno_i')),
            'aircraft_number': int(ag.getVariableUInt('aircraft_number')),
            'ac_type_mask': int(ag.getVariableUInt('ac_type_mask')),
            'status_id': int(ag.getVariableUInt('status_id')),
            'daily_flight': int(daily_today[idx]) if idx < len(daily_today) else 0,
            'ops_counter_mi8': int(ops_targets.get('ops_counter_mi8', 0)),
            'ops_counter_mi17': int(ops_targets.get('ops_counter_mi17', 0)),
            'ops_current_mi8': int(ops_current.get(1, 0)),
            'ops_current_mi17': int(ops_current.get(2, 0)),
            'partout_trigger': part_date,
            'assembly_trigger': asm_date,
            'active_trigger': act_date,
            'aircraft_age_years': 0,
            'mfg_date': md,
            'sne': int(ag.getVariableUInt('sne')),
            'ppr': int(ag.getVariableUInt('ppr')),
            'repair_days': int(ag.getVariableUInt('repair_days')),
            'simulation_metadata': f"v={vdate}/id={vid};D={d}"
        })

    if rows:
        exporter.insert_rows(rows)


