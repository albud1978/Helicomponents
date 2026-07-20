#!/usr/bin/env python3
"""
Day-0 классификатор: inactive → serviceable (планеры) + наследование агрегатов.

Выполняется ПОСЛЕ process_inactive_planery_status, ДО component_status / serviceable_status.

Планеры со status_id=1 (вне ростера):
- Mi-17 (group_by=2): всегда → status_id=3 (serviceable)
- Mi-8 (group_by=1): → status_id=3 только при историческом присутствии в программе
  * SCD depth ≥365 дней: presence в source.amos_heli_aircraft за 365 дней
    (ac_typ IN МИ8/МИ8АМТ/МИ8МТВ, manual_owner='ЮТ-ВУ', owner IN ALLOWED_OWNERS,
     non_managed='N'; поле AMOS status НЕ используется)
  * иначе / DWH недоступен: presence в program_ac с version_date >= 2025-07-04

Агрегаты (до serviceable_status):
- планер status_id=1 → все агрегаты на борту (group_by>2) → status_id=7
- планер status_id=3 → агрегаты остаются 0 (ИСПРАВНЫЙ→3 позже)
- OPS (status_id=2) не трогаем
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional, Set

import pandas as pd

from extract.dual_loader import ALLOWED_OWNERS

PROGRAM_AC_HISTORY_START = date(2025, 7, 4)
SCD_DEPTH_DAYS = 365
MI8_AC_TYPS = ("МИ8", "МИ8АМТ", "МИ8МТВ")


def _as_u32(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="raise").fillna(0).astype("int64")


def _normalize_registr(value) -> str:
    return str(value).strip().zfill(5)


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


def _probe_scd_depth_days(dwh_client, version_date: date) -> Optional[int]:
    """Глубина SCD source.amos_heli_aircraft относительно version_date (дни)."""
    rows = dwh_client.query(
        """
        SELECT dateDiff(
            'day',
            min(toDate(valid_from)),
            toDate(%(vd)s)
        ) AS depth_days
        FROM source.amos_heli_aircraft
        WHERE valid_from <= toDateTime(%(vd)s) + INTERVAL 1 DAY - INTERVAL 1 SECOND
        """,
        parameters={"vd": version_date.isoformat()},
    ).result_rows
    if not rows or rows[0][0] is None:
        return None
    return int(rows[0][0])


def _mi8_serials_from_dwh_scd(dwh_client, version_date: date) -> Set[str]:
    """Mi-8 registr из DWH SCD за последние 365 дней (без поля status)."""
    owners_sql = ", ".join(f"'{o}'" for o in sorted(ALLOWED_OWNERS))
    ac_typs_sql = ", ".join(f"'{t}'" for t in MI8_AC_TYPS)
    window_start = (version_date - timedelta(days=SCD_DEPTH_DAYS)).isoformat()
    rows = dwh_client.query(
        f"""
        SELECT DISTINCT toInt64OrZero(trimBoth(ac_registr)) AS ac_registr
        FROM source.amos_heli_aircraft
        WHERE ac_typ IN ({ac_typs_sql})
          AND manual_owner = 'ЮТ-ВУ'
          AND coalesce(owner, '') IN ({owners_sql})
          AND non_managed = 'N'
          AND valid_from <= toDateTime(%(vd)s) + INTERVAL 1 DAY - INTERVAL 1 SECOND
          AND (valid_to IS NULL OR valid_to > toDateTime(%(ws)s))
        """,
        parameters={"vd": version_date.isoformat(), "ws": window_start},
    ).result_rows
    return {_normalize_registr(r[0]) for r in rows if r[0] is not None and int(r[0]) > 0}


def _mi8_serials_from_program_ac(client) -> Set[str]:
    """Registr из project program_ac с version_date >= 2025-07-04."""
    rows = client.execute(
        """
        SELECT DISTINCT ac_registr
        FROM program_ac
        WHERE version_date >= %(start)s
        """,
        {"start": PROGRAM_AC_HISTORY_START},
    )
    return {_normalize_registr(r[0]) for r in rows if r[0] is not None}


def _resolve_mi8_program_history(
    client, version_date: date
) -> tuple[Set[str], str]:
    """
    Возвращает (set serialnos, source_label).
    При недоступности DWH / недостаточной SCD-глубине — program_ac datasets.
    """
    dwh = None
    try:
        from utils.dwh_golden_replay_export import dwh_client

        dwh = dwh_client()
        depth = _probe_scd_depth_days(dwh, version_date)
    except Exception as exc:
        print(
            f"⚠️ DWH недоступен для SCD depth probe ({type(exc).__name__}: {exc}) "
            f"— fallback на program_ac datasets (version_date >= {PROGRAM_AC_HISTORY_START})"
        )
        return _mi8_serials_from_program_ac(client), "program_ac_datasets"

    if depth is None or depth < SCD_DEPTH_DAYS:
        print(
            f"ℹ️ SCD depth source.amos_heli_aircraft = {depth} дней "
            f"(нужно ≥{SCD_DEPTH_DAYS}) — fallback на program_ac datasets "
            f"(version_date >= {PROGRAM_AC_HISTORY_START})"
        )
        return _mi8_serials_from_program_ac(client), "program_ac_datasets"

    print(
        f"ℹ️ SCD depth = {depth} дней ≥ {SCD_DEPTH_DAYS} — "
        f"Mi-8 history из source.amos_heli_aircraft (без поля status)"
    )
    return _mi8_serials_from_dwh_scd(dwh, version_date), "dwh_scd_365d"


def process_inactive_serviceable_status(pandas_df: pd.DataFrame, client):
    """
    Классифицирует inactive-планеры как serviceable и проставляет агрегатам unserviceable.

    Args:
        pandas_df: DataFrame heli_pandas (после inactive_planery)
        client: ClickHouse client проекта (для program_ac history)

    Returns:
        DataFrame с обновлёнными status_id
    """
    print("\n🚀 === INACTIVE → SERVICEABLE CLASSIFIER ===")
    required = {"group_by", "status_id", "serialno", "aircraft_number", "version_date"}
    missing = required - set(pandas_df.columns)
    if missing:
        raise ValueError(
            f"inactive_serviceable_classifier: отсутствуют колонки {sorted(missing)}"
        )

    version_date = _version_date_from_df(pandas_df)
    group_by = _as_u32(pandas_df["group_by"])
    status_id = _as_u32(pandas_df["status_id"])
    aircraft_number = _as_u32(pandas_df["aircraft_number"])
    serial_norm = pandas_df["serialno"].map(_normalize_registr)

    inactive_planer = group_by.isin([1, 2]) & (status_id == 1)
    mi17_mask = inactive_planer & (group_by == 2)
    mi8_mask = inactive_planer & (group_by == 1)

    mi17_count = int(mi17_mask.sum())
    if mi17_count:
        pandas_df.loc[mi17_mask, "status_id"] = 3
    print(f"✅ Mi-17 OOR → serviceable (3): {mi17_count}")

    history_set, history_source = _resolve_mi8_program_history(client, version_date)
    print(f"📋 Mi-8 program history source={history_source}, size={len(history_set)}")

    mi8_promote = mi8_mask & serial_norm.isin(history_set)
    mi8_promote_count = int(mi8_promote.sum())
    mi8_stay = int(mi8_mask.sum()) - mi8_promote_count
    if mi8_promote_count:
        pandas_df.loc[mi8_promote, "status_id"] = 3
        promoted = sorted(serial_norm[mi8_promote].unique().tolist())
        print(f"✅ Mi-8 OOR → serviceable (3): {mi8_promote_count} ({', '.join(promoted)})")
    else:
        print("✅ Mi-8 OOR → serviceable (3): 0")
    print(f"ℹ️ Mi-8 OOR остаются inactive (1): {mi8_stay}")

    # Перечитываем статусы после промоушена планеров
    status_id = _as_u32(pandas_df["status_id"])
    inactive_acns = set(
        aircraft_number[
            group_by.isin([1, 2]) & (status_id == 1) & (aircraft_number > 0)
        ].tolist()
    )
    agg_unserv_mask = (
        (group_by > 2)
        & (aircraft_number > 0)
        & (aircraft_number.isin(inactive_acns))
    )
    agg_unserv_count = int(agg_unserv_mask.sum())
    if agg_unserv_count:
        pandas_df.loc[agg_unserv_mask, "status_id"] = 7
    print(
        f"✅ Aggregates on inactive planers → unserviceable (7): {agg_unserv_count} "
        f"(planers={len(inactive_acns)})"
    )

    serv_oor_acns = set(
        aircraft_number[
            group_by.isin([1, 2]) & (status_id == 3) & (aircraft_number > 0)
        ].tolist()
    )
    print(
        f"ℹ️ Serviceable OOR planers (status=3): {len(serv_oor_acns)} — "
        f"агрегаты оставлены для serviceable_status"
    )
    print("✅ inactive_serviceable_classifier завершён")
    return pandas_df


if __name__ == "__main__":
    print("🧪 Модуль inactive_serviceable_classifier")
    print("💡 Вызывается из dual_loader.py / dwh_post_enrichment.py")
