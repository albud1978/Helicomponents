#!/usr/bin/env python3
"""Прямая загрузка Status_Components из DWH в Project ClickHouse на любую дату.

Заменяет цепочку: AMOS → Excel → extract_master/dual_loader → heli_pandas.
Обогащает: aircraft_number, ac_type_mask, group_by, lease_restricted.
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

CODE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = CODE_ROOT.parent
sys.path.append(str(CODE_ROOT))
sys.path.append(str(CODE_ROOT / "utils"))

from config_loader import get_clickhouse_client
from dwh_golden_replay_export import DEFAULT_REPORT_DATE, _lease_col, _fmt_dmY_series, dwh_client
from extract.aircraft_number_processor import process_aircraft_numbers_in_memory
from extract.dual_loader import create_tables, get_md_partnos, insert_data, prepare_data

# Порядок колонок для heli_raw и heli_pandas (дублирует dual_loader)
RAW_COLUMN_ORDER = [
    "partno", "serialno", "ac_typ", "location", "mfg_date",
    "removal_date", "target_date", "condition", "owner", "lease_restricted",
    "oh", "oh_threshold", "ll", "sne", "ppr",
    "version_date", "version_id", "partseqno_i", "psn",
    "address_i", "ac_type_i", "oh_at_date", "shop_visit_counter",
]
PANDAS_COLUMN_ORDER = [
    "partno", "serialno", "ac_typ", "location", "mfg_date",
    "removal_date", "target_date", "condition", "owner", "lease_restricted",
    "oh", "oh_threshold", "ll", "sne", "ppr",
    "version_date", "version_id", "partseqno_i", "psn",
    "address_i", "ac_type_i",
    "status_id", "repair_days", "aircraft_number", "ac_type_mask", "group_by",
]


def _fail(message: str) -> None:
    print(f"ERROR: {message}", file=sys.stderr)
    raise SystemExit(1)


def _parse_report_date(raw: str) -> tuple[str, str]:
    """Возвращает (report_date, version_date) — оба в ISO формате."""
    try:
        parsed = datetime.strptime(raw.strip(), "%Y-%m-%d").date()
    except ValueError as exc:
        _fail(f"Некорректная дата --report-date={raw!r}: {exc}")
    iso = parsed.isoformat()
    return iso, iso


def _align_columns(df: pd.DataFrame, column_order: list[str]) -> pd.DataFrame:
    missing = [c for c in column_order if c not in df.columns]
    if missing:
        _fail(f"Отсутствуют ожидаемые колонки: {missing}")
    return df[list(column_order)].copy()


def _ac_type_mask(ac_typ_series: pd.Series) -> pd.Series:
    """Ми-8 → 0, Ми-17 → 1."""
    return ac_typ_series.map(lambda x: 1 if isinstance(x, str) and "17" in x else 0).fillna(0).astype("uint8")


def _md_group_by_map(client) -> dict[int, int]:
    rows = client.execute(
        """SELECT toUInt32(partseqno_i) AS partseqno_i,
                  toUInt8(max(`group_by`)) AS gb
           FROM md_components
           WHERE partseqno_i IS NOT NULL AND `group_by` IS NOT NULL
           GROUP BY partseqno_i HAVING gb != 0"""
    )
    return {int(r[0]): int(r[1]) for r in rows}


def _assert_version_absent(client, table: str, version_date: str, version_id: int) -> None:
    count = client.execute(
        f"SELECT COUNT(*) FROM {table} WHERE version_date = %(vd)s AND version_id = %(vi)s",
        {"vd": version_date, "vi": version_id},
    )[0][0]
    if count:
        _fail(f"В {table} уже есть {count} строк для version_date={version_date} version_id={version_id}")


def fetch_status_components_df(dwh, report_date: str) -> pd.DataFrame:
    """Достать Status_Components из DWH reports на указанную дату."""
    sql = f"""
    SELECT partno, partseqno_i, serialno, psn, ac_typ, ac_type_i, location,
           LL AS ll, OH AS oh, OH_threshold AS oh_threshold, sne, ppr,
           mfg_date, oh_at_date, shop_visit_counter, owner, address_i,
           condition, removal_date, target_date
    FROM reports.amos_heli_rotables_components_status
    WHERE report_date = toDate('{report_date}')
    """
    df = dwh.query_df(sql)
    df["lease_restricted"] = _lease_col(df["owner"])
    df["removal_date"] = _fmt_dmY_series(df["removal_date"])
    df["target_date"] = _fmt_dmY_series(df["target_date"])
    df["oh_at_date"] = pd.to_datetime(df["oh_at_date"], errors="coerce")
    return df


def enrich_for_heli_pandas(
    df: pd.DataFrame,
    version_date: str,
    version_id: int,
    local_ch,
    md_partnos: set,
) -> pd.DataFrame:
    """Обогатить DataFrame перед загрузкой в heli_pandas."""
    pdf = prepare_data(
        df.copy(), version_date, version_id=version_id,
        filter_partnos=md_partnos, table_name="heli_pandas",
    )

    # aircraft_number из location
    pdf, _, invalid_ra = process_aircraft_numbers_in_memory(pdf)
    if invalid_ra:
        _fail(f"Невалидные RA-форматы в location: {invalid_ra}")

    # ac_type_mask
    if "ac_type_mask" not in pdf.columns or pdf["ac_type_mask"].eq(0).all():
        pdf["ac_type_mask"] = _ac_type_mask(pdf["ac_typ"])

    # group_by из md_components
    gb_map = _md_group_by_map(local_ch)
    pdf["partseqno_i"] = pd.to_numeric(pdf["partseqno_i"], errors="coerce").fillna(0).astype("int64")
    pdf["group_by"] = pdf["partseqno_i"].map(gb_map).fillna(0).astype("int64")

    # status_id / repair_days — базовые значения (каскад из 9 этапов не переносим)
    if "status_id" not in pdf.columns:
        pdf["status_id"] = 0
    if "repair_days" not in pdf.columns:
        pdf["repair_days"] = None

    return _align_columns(pdf, PANDAS_COLUMN_ORDER)


def load_to_clickhouse(
    local_ch,
    raw_df: pd.DataFrame,
    pandas_df: pd.DataFrame,
    version_date: str,
    version_id: int,
    *,
    dry_run: bool = False,
) -> dict:
    """Загрузить в heli_raw и heli_pandas. Возвращает статистику."""
    stats = {
        "raw_rows": len(raw_df),
        "pandas_rows": len(pandas_df),
        "raw_inserted": 0,
        "pandas_inserted": 0,
        "md_partnos": 0,
    }

    if dry_run:
        return stats

    create_tables(local_ch)
    _assert_version_absent(local_ch, "heli_raw", version_date, version_id)
    _assert_version_absent(local_ch, "heli_pandas", version_date, version_id)

    stats["raw_inserted"] = insert_data(local_ch, raw_df, "heli_raw", "DWH analytics raw")
    stats["pandas_inserted"] = insert_data(local_ch, pandas_df, "heli_pandas", "DWH analytics filtered staging")

    if stats["raw_inserted"] != len(raw_df):
        _fail(f"heli_raw: вставлено {stats['raw_inserted']}, ожидалось {len(raw_df)}")
    if stats["pandas_inserted"] != len(pandas_df):
        _fail(f"heli_pandas: вставлено {stats['pandas_inserted']}, ожидалось {len(pandas_df)}")

    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Прямая загрузка Status_Components из DWH в Project ClickHouse")
    parser.add_argument("--report-date", default=DEFAULT_REPORT_DATE,
                        help=f"Дата снимка YYYY-MM-DD (по умолчанию {DEFAULT_REPORT_DATE})")
    parser.add_argument("--dry-run", action="store_true",
                        help="Построить payload и показать объёмы без записи")
    args = parser.parse_args()

    report_date, version_date = _parse_report_date(args.report_date)
    version_id = 1

    print(f"Дата снимка: report_date={report_date}, version_date={version_date}, version_id={version_id}")
    print()

    # 1. DWH: забрать данные
    print("1/4 Запрос DWH...")
    dwh = dwh_client()
    source_df = fetch_status_components_df(dwh, report_date)
    print(f"   DWH reports: {len(source_df)} строк")

    # 2. Локальный CH: справочники
    print("2/4 Справочники (md_components)...")
    local_ch = get_clickhouse_client()
    md_partnos = get_md_partnos(local_ch)
    print(f"   md_partnos: {len(md_partnos)} записей")

    # 3. Обогащение
    print("3/4 Обогащение...")
    raw_df = prepare_data(source_df.copy(), version_date, version_id=version_id, table_name="heli_raw")
    raw_df = _align_columns(raw_df, RAW_COLUMN_ORDER)

    pandas_df = enrich_for_heli_pandas(source_df, version_date, version_id, local_ch, md_partnos)

    print(f"   heli_raw:   {len(raw_df)} строк")
    print(f"   heli_pandas: {len(pandas_df)} строк")

    # 4. Загрузка
    print(f"4/4 {'[DRY-RUN] ' if args.dry_run else ''}Загрузка в Project ClickHouse...")
    stats = load_to_clickhouse(local_ch, raw_df, pandas_df, version_date, version_id, dry_run=args.dry_run)

    if not args.dry_run:
        print(f"   heli_raw:   {stats['raw_inserted']} вставлено")
        print(f"   heli_pandas: {stats['pandas_inserted']} вставлено")
        print(f"   Готово: version_date={version_date}")

    # Краткая сводка
    print()
    print("── Статистика ──")
    print(f"  source (DWH):     {len(source_df):>8,}")
    print(f"  heli_raw:         {len(raw_df):>8,}")
    print(f"  heli_pandas:      {len(pandas_df):>8,}  (filtered by md_partnos)")
    print(f"  отсев (не MD):    {len(source_df) - len(pandas_df):>8,}")

    if not args.dry_run:
        # Проверить дубли
        dup_check = local_ch.execute(
            "SELECT version_date, version_id, COUNT(*) FROM heli_pandas "
            "WHERE version_date = %(vd)s AND version_id = %(vi)s "
            "GROUP BY version_date, version_id",
            {"vd": version_date, "vi": version_id},
        )
        if dup_check:
            print(f"  Проверка: в heli_pandas {dup_check[0][2]} строк для {dup_check[0][0]} / v{dup_check[0][1]}")


if __name__ == "__main__":
    main()
