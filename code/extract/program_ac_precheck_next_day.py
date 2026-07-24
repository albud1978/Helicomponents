#!/usr/bin/env python3
"""
Precheck D1 для записей с status_id == 2 на этапе обогащения heli_pandas

Логика (перед первым днём симуляции):
- Для group_by ∈ {1,2} и status_id == 2 берём dt = налёт за первый
  полётный день программы: daily_hours(version_date) = mp5[0]
- Считаем rem_ll0 = ll - sne, rem_oh0 = oh - ppr (вечер D0)
- Если rem_ll0 < dt → status_id = 6 (хранение)
- Иначе если rem_oh0 < dt:
    * если выбранный BR (из br_mi8/br_mi17 по маске) == 0 или sne + dt ≥ BR → status_id = 6
    * иначе → status_id = 7 (ремонтопригодный)

Примечания:
- Проверка зеркалит первый инкремент симуляции: cumsum[1] - cumsum[0] = mp5[0].
- Историческое имя helper-а содержит D1, но загружается именно D0/version_date.
- BR выбирается по маске типов (из ac_typ → ac_type_mask): Ми‑8 → br_mi8, Ми‑17 → br_mi17 (ед.: минуты)
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Dict, List, Mapping, Optional

import pandas as pd

from extract.program_fl_direct_loader import round_half_up_nonneg


def _load_daily_map_for_d1(
    client, version_date: date, version_id: int
) -> Dict[int, int]:
    d0 = client.execute(
        """
        SELECT min(dates)
        FROM flight_program_fl
        WHERE version_date = %(vd)s AND version_id = %(vi)s
        """,
        {"vd": version_date, "vi": version_id},
    )[0][0]
    if d0 is None:
        raise ValueError(f"flight_program_fl пуст для version_date={version_date}")

    rows = client.execute(
        """
        SELECT aircraft_number, daily_hours
        FROM flight_program_fl
        WHERE dates = %(d)s
          AND version_date = %(vd)s
          AND version_id = %(vi)s
        """,
        {"d": d0, "vd": version_date, "vi": version_id},
    )
    return {int(ac): int(h or 0) for ac, h in rows if ac is not None}


def _load_day0_map_from_program(
    dataset_path: str,
    version_date: date,
    aircraft_types: Mapping[int, int],
) -> Dict[int, int]:
    """Build day0 daily_hours from Program.xlsx without aircraft dictionaries."""
    program_path = Path(dataset_path) / "Program.xlsx"
    if not program_path.is_file():
        raise FileNotFoundError(f"Program.xlsx не найден: {program_path}")

    frame = pd.read_excel(program_path, sheet_name="2025", header=0, engine="openpyxl")
    required = {"ac_type_mask", "serialno", "Месяц", version_date.month}
    missing = sorted(required.difference(frame.columns), key=str)
    if missing:
        raise ValueError(f"{program_path}: отсутствуют колонки {missing}")

    rows = frame.loc[frame["Месяц"] == "daily_flight"]
    if rows.empty:
        raise ValueError(f"{program_path}: нет строк daily_flight для day0={version_date}")

    month = version_date.month
    instance_hours = {
        int(row["serialno"]): round_half_up_nonneg(row[month])
        for _, row in rows.iterrows()
        if pd.notna(row["serialno"]) and pd.notna(row[month])
    }
    type_hours = {
        int(row["ac_type_mask"]): round_half_up_nonneg(row[month])
        for _, row in rows.iterrows()
        if pd.notna(row["ac_type_mask"]) and pd.notna(row[month])
    }
    day0_map = {
        int(aircraft_number): instance_hours.get(
            int(aircraft_number), type_hours.get(int(ac_type_mask), 0)
        )
        for aircraft_number, ac_type_mask in aircraft_types.items()
    }
    if not day0_map:
        raise ValueError(f"{program_path}: day0 map пуст для day0={version_date}")
    return day0_map


def _load_br_map(client, version_date: date) -> Dict[int, tuple]:
    rows = client.execute(
        """
        SELECT partseqno_i, br_mi8, br_mi17
        FROM md_components
        WHERE version_date = %(vd)s
        """,
        {"vd": version_date},
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


def process_program_ac_precheck_d1(
    pandas_df,
    client,
    version_date: date,
    version_id: int,
    *,
    dataset_path: Optional[str] = None,
    daily_map: Optional[Mapping[int, int]] = None,
):
    """Модифицирует pandas_df на месте: корректирует status_id для D1 precheck.

    Меняет только строки с status_id == 2 и group_by ∈ {1,2}.
    """
    if pandas_df is None or len(pandas_df) == 0:
        return pandas_df

    # Нужные колонки с безопасными значениями по умолчанию
    for col in ['group_by', 'status_id', 'aircraft_number', 'ac_type_mask', 'll', 'oh', 'sne', 'ppr', 'partseqno_i', 'ac_typ']:
        if col not in pandas_df.columns:
            pandas_df[col] = 0 if col != 'ac_typ' else ''

    if daily_map is not None:
        resolved_daily_map = {int(ac): int(hours or 0) for ac, hours in daily_map.items()}
        if not resolved_daily_map:
            raise ValueError(f"Переданный day0 map пуст для day0={version_date}")
        print(f"Precheck day0 dt source: provided map ({len(resolved_daily_map)} aircraft)")
    elif dataset_path:
        planner_rows = pandas_df.loc[
            pandas_df["group_by"].isin([1, 2]) & (pandas_df["aircraft_number"] > 0),
            ["aircraft_number", "ac_type_mask"],
        ].drop_duplicates()
        aircraft_types = {
            int(row["aircraft_number"]): int(row["ac_type_mask"] or 0)
            for _, row in planner_rows.iterrows()
        }
        resolved_daily_map = _load_day0_map_from_program(
            dataset_path, version_date, aircraft_types
        )
        print(
            f"Precheck day0 dt source: Excel {Path(dataset_path) / 'Program.xlsx'} "
            f"({len(resolved_daily_map)} aircraft)"
        )
    else:
        resolved_daily_map = _load_daily_map_for_d1(
            client, version_date, version_id
        )
        print(
            f"Precheck day0 dt source: SQL flight_program_fl "
            f"({len(resolved_daily_map)} aircraft)"
        )
    br_map = _load_br_map(client, version_date)

    # Фильтр кандидатов (status_id == 2)
    mask_candidates = (
        (pandas_df['status_id'] == 2)
        & (pandas_df['group_by'].isin([1, 2]))
    )
    idxs: List[int] = list(pandas_df[mask_candidates].index)

    for idx in idxs:
        row = pandas_df.loc[idx]
        ac = int(row.get('aircraft_number') or 0)
        dt = int(resolved_daily_map.get(ac, 0) or 0)
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
            else:
                pandas_df.at[idx, 'status_id'] = 7
            continue

    return pandas_df





