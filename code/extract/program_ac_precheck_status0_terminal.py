#!/usr/bin/env python3
"""
D1 precheck для status_id=0: переводит только в terminal (6) по ресурсной логике.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Dict, List

from extract.program_ac_status_processor import PLANER_PARTNOS


def _load_daily_map_for_d1(client) -> Dict[int, int]:
    d0 = client.execute("SELECT min(dates) FROM flight_program_fl")[0][0]
    d1 = d0 + timedelta(days=1)
    rows = client.execute(
        "SELECT aircraft_number, daily_hours FROM flight_program_fl WHERE dates = %(d)s",
        {"d": d1},
    )
    return {int(ac): int(h or 0) for ac, h in rows if ac is not None}


def _load_br_map(client) -> Dict[int, tuple]:
    rows = client.execute(
        "SELECT partno_comp, br_mi8, br_mi17 FROM md_components"
    )
    return {
        int(p): (int(b8 or 0), int(b17 or 0))
        for p, b8, b17 in rows
        if p is not None
    }


# Минимальный словарь масок по ac_typ (текст)
AC_TYPE_MASKS: Dict[str, int] = {
    'Ми-17': 64, 'МИ171': 64, '171А2': 64, 'МИ171Е': 64,
    'Ми-8Т': 32, 'МИ8МТВ': 32, 'МИ8': 32, 'МИ8АМТ': 32,
}


def _mask_from_ac_typ(ac_typ: str) -> int:
    if not ac_typ:
        return 0
    return AC_TYPE_MASKS.get(str(ac_typ).strip(), 0)


def process_program_ac_precheck_status0_terminal_d1(pandas_df, client):
    """Модифицирует pandas_df на месте: D1 precheck для status_id=0 → terminal(6)."""
    if pandas_df is None or len(pandas_df) == 0:
        return pandas_df

    # Нужные колонки с безопасными значениями по умолчанию
    for col in [
        'status_id', 'aircraft_number', 'll', 'oh', 'sne', 'ppr',
        'partseqno_i', 'ac_typ', 'partno',
    ]:
        if col not in pandas_df.columns:
            if col in ('ac_typ', 'partno'):
                pandas_df[col] = ''
            else:
                pandas_df[col] = 0

    daily_map = _load_daily_map_for_d1(client)
    br_map = _load_br_map(client)

    # Фильтр кандидатов (status_id == 0, только планеры по partno)
    mask_candidates = (
        (pandas_df['status_id'] == 0)
        & (pandas_df['partno'].isin(PLANER_PARTNOS))
    )
    idxs: List[int] = list(pandas_df[mask_candidates].index)

    for idx in idxs:
        row = pandas_df.loc[idx]
        ac = int(row.get('aircraft_number') or 0)
        dt = int(daily_map.get(ac, 0) or 0)
        if dt <= 0:
            continue

        ll = int(row.get('ll') or 0)
        oh = int(row.get('oh') or 0)
        sne = int(row.get('sne') or 0)
        ppr = int(row.get('ppr') or 0)
        rem_ll0 = ll - sne
        rem_oh0 = oh - ppr

        if rem_ll0 < dt:
            pandas_df.at[idx, 'status_id'] = 6
            continue

        if rem_oh0 < dt:
            partseq = int(row.get('partseqno_i') or 0)
            b8, b17 = br_map.get(partseq, (0, 0))
            mask = _mask_from_ac_typ(row.get('ac_typ'))
            br = b8 if (mask & 32) else (b17 if (mask & 64) else 0)
            if br == 0 or (sne + dt) >= br:
                pandas_df.at[idx, 'status_id'] = 6

    return pandas_df
