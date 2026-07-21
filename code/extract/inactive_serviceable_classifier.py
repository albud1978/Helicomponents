#!/usr/bin/env python3
"""
Day-0 классификатор OOR: inactive планеры + агрегаты.

Выполняется ПОСЛЕ process_inactive_planery_status, ДО component_status / serviceable_status.

Синхрон с OPS demote destination gates (`planer_calendar_remain.destination_for_remain`):
- сначала program_ac с 2025-07-04; без истории → планер 1, agg 7
- в программе + remain_calendar_days > 0 → планер 3, agg 3
- в программе + нет положительного календаря → планер 1, agg 3
Календарь в 3b — только treq OH(D); fallback +10y−1д здесь НЕ применяется (только demote).

Полная симметрия Mi-8 / Mi-17. OPS (status_id=2) не трогаем.
"""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime

import pandas as pd

from extract.planer_calendar_remain import (
    destination_for_remain,
    fetch_calendar_remain_by_psn,
    normalize_registr,
    open_dwh_client,
    program_history_serials,
)


def _as_u32(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="raise").fillna(0).astype("int64")


def _version_date_from_df(pandas_df: pd.DataFrame) -> date:
    if "version_date" not in pandas_df.columns or pandas_df.empty:
        raise ValueError("heli_pandas DataFrame: отсутствует version_date")
    unique = pandas_df["version_date"].dropna().unique()
    if len(unique) == 0:
        raise ValueError("heli_pandas DataFrame: version_date пуст")
    if len(unique) > 1:
        raise ValueError(
            f"heli_pandas DataFrame: ожидается один version_date, найдено {len(unique)}"
        )
    raw = unique[0]
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    if hasattr(raw, "date") and callable(raw.date):
        return raw.date()
    return date.fromisoformat(str(raw)[:10])


def process_inactive_serviceable_status(pandas_df: pd.DataFrame, client):
    """
    Классифицирует inactive-планеры и агрегаты теми же гейтами, что demote destination.
    """
    print("\n🚀 === INACTIVE/SERVICEABLE CLASSIFIER (calendar+program, synced with demote) ===")
    required = {
        "group_by",
        "status_id",
        "serialno",
        "aircraft_number",
        "version_date",
        "psn",
    }
    missing = required - set(pandas_df.columns)
    if missing:
        raise ValueError(
            f"inactive_serviceable_classifier: отсутствуют колонки {sorted(missing)}"
        )

    version_date = _version_date_from_df(pandas_df)
    group_by = _as_u32(pandas_df["group_by"])
    status_id = _as_u32(pandas_df["status_id"])
    aircraft_number = _as_u32(pandas_df["aircraft_number"])
    psn = _as_u32(pandas_df["psn"])
    serial_norm = pandas_df["serialno"].map(normalize_registr)

    inactive_planer = group_by.isin([1, 2]) & (status_id == 1)
    inactive_idx = pandas_df.index[inactive_planer]
    if len(inactive_idx) == 0:
        print("ℹ️ Inactive OOR планеров нет — классификатор пропущен")
        print("✅ inactive_serviceable_classifier завершён")
        return pandas_df

    # Один планер = одна строка group_by∈{1,2}; ключ — aircraft_number (fallback serial).
    planer_rows = []
    seen = set()
    for idx in inactive_idx:
        acn = int(aircraft_number.loc[idx])
        serial = str(serial_norm.loc[idx])
        key = acn if acn > 0 else f"s:{serial}"
        if key in seen:
            continue
        seen.add(key)
        planer_rows.append(
            {
                "idx": idx,
                "aircraft_number": acn,
                "serial": serial,
                "psn": int(psn.loc[idx]),
                "group_by": int(group_by.loc[idx]),
            }
        )

    history = program_history_serials(client)
    dwh = open_dwh_client()
    # Fallback +10y−1д только в demote; 3b считает календарь строго по treq OH(D).
    cal = fetch_calendar_remain_by_psn(
        dwh,
        version_date,
        [r["psn"] for r in planer_rows],
        fallback_10y_psns=None,
    )
    print(
        f"📋 program_ac history since 2025-07-04: {len(history)} serials; "
        f"inactive OOR planers: {len(planer_rows)}; fallback_10y=OFF (demote-only)"
    )

    reason_counts: Counter = Counter()
    planer_to_3 = 0
    planer_stay_1 = 0
    agg_to_3 = 0
    agg_to_7 = 0

    for row in planer_rows:
        rem = cal.get(row["psn"])
        remain_d = rem.remain_d if rem is not None else None
        in_hist = row["serial"] in history
        planer_st, agg_st, reason = destination_for_remain(remain_d, in_hist)
        reason_counts[reason] += 1

        pandas_df.loc[row["idx"], "status_id"] = planer_st
        if planer_st == 3:
            planer_to_3 += 1
        else:
            planer_stay_1 += 1

        acn = row["aircraft_number"]
        if acn <= 0:
            continue
        # на этапе 3b агрегаты обычно ещё status_id=0
        agg_mask = (group_by > 2) & (aircraft_number == acn)
        n_agg = int(agg_mask.sum())
        if n_agg:
            pandas_df.loc[agg_mask, "status_id"] = agg_st
            if agg_st == 3:
                agg_to_3 += n_agg
            else:
                agg_to_7 += n_agg

        print(
            f"  {row['serial']} gb={row['group_by']} remain_d={remain_d} "
            f"hist={int(in_hist)} → planer={planer_st} agg={agg_st} ({reason})"
        )

    print(f"✅ OOR planers → serviceable(3): {planer_to_3}")
    print(f"ℹ️ OOR planers остаются inactive(1): {planer_stay_1}")
    print(f"✅ Aggregates → serviceable(3): {agg_to_3}")
    print(f"✅ Aggregates → unserviceable(7): {agg_to_7}")
    print(f"📊 by_reason: {dict(reason_counts)}")
    print("✅ inactive_serviceable_classifier завершён")
    return pandas_df


if __name__ == "__main__":
    print("🧪 Модуль inactive_serviceable_classifier")
    print("💡 Вызывается из dual_loader.py / dwh_post_enrichment.py")
