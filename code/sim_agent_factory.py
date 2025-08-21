#!/usr/bin/env python3
"""
Создание популяции агентов из MP3 (инициализация agent variables из существующих MP).
Дата: 2025-08-21
"""

from __future__ import annotations
from typing import List, Tuple, Dict
from datetime import date

import os
import sys

try:
    import pyflamegpu
except Exception:
    pyflamegpu = None

sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))


def build_agents(sim, model, mp3_rows: List[Tuple], mp3_fields: List[str], mp1_br_rt_map: Dict[int, Tuple[int,int,int,int]]):
    assert pyflamegpu is not None
    comp_desc = model.model.getAgent("component")
    av = pyflamegpu.AgentVector(comp_desc, len(mp3_rows))
    idx = {name: i for i, name in enumerate(mp3_fields)}
    from datetime import date
    epoch = date(1970,1,1)
    # Минимальный probe-режим: если агент не содержит расширенные переменные, заполняем только базовые
    minimal_fields = set([
        'status_id','repair_days','repair_time','ppr',
    ])
    has_idx = comp_desc.hasVariable("idx") if hasattr(comp_desc, 'hasVariable') else True
    for i, r in enumerate(mp3_rows):
        ai = av[i]
        if has_idx:
            ai.setVariableUInt("idx", i)
        ai.setVariableUInt("partseqno_i", int(r[idx['partseqno_i']] or 0))
        ai.setVariableUInt("psn", int(r[idx['psn']] or 0))
        ai.setVariableUInt("aircraft_number", int(r[idx['aircraft_number']] or 0))
        ai.setVariableUInt("group_by", int(r[idx['group_by']] or 0))
        ai.setVariableUInt("ac_type_mask", int(r[idx['ac_type_mask']] or 0))
        ai.setVariableUInt("status_id", int(r[idx['status_id']] or 0))
        if comp_desc.hasVariable("status_change") if hasattr(comp_desc, 'hasVariable') else False:
            ai.setVariableUInt("status_change", int(r[idx.get('status_change','status_id')] or 0))
        ai.setVariableUInt("ll", int(r[idx['ll']] or 0))
        ai.setVariableUInt("oh", int(r[idx['oh']] or 0))
        ai.setVariableUInt("oh_threshold", int(r[idx['oh_threshold']] or 0))
        ai.setVariableUInt("sne", int(r[idx['sne']] or 0))
        ai.setVariableUInt("ppr", int(r[idx['ppr']] or 0))
        ai.setVariableUInt("repair_days", int(r[idx['repair_days']] or 0))
        # Даты → ordinal UInt16
        mfg = r[idx['mfg_date']]
        vdt = r[idx['version_date']]
        mfg_ord = (mfg.toordinal() - epoch.toordinal()) if hasattr(mfg, 'toordinal') else 0
        vdt_ord = (vdt.toordinal() - epoch.toordinal()) if hasattr(vdt, 'toordinal') else 0
        ai.setVariableUInt("mfg_date", int(max(0, mfg_ord)))
        ai.setVariableUInt("version_date", int(max(0, vdt_ord)))
        partseq = int(r[idx['partseqno_i']] or 0)
        br, rt, pt, at = mp1_br_rt_map.get(partseq, (0, 0, 0, 0))
        ai.setVariableUInt("br", int(br))
        ai.setVariableUInt("repair_time", int(rt))
        ai.setVariableUInt("partout_time", int(pt))
        ai.setVariableUInt("assembly_time", int(at))
    sim.setPopulationData(av)


