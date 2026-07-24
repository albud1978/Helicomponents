#!/usr/bin/env python3
"""
Program AC Precheck Runner

Минимально-инвазивный микросервис: запускает D1 precheck после формирования тензора FL,
используя уже существующую функцию process_program_ac_precheck_d1(). Работает только в явно
переданном скоупе version_date/version_id.
"""

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd


def _parse_version_date(raw: str) -> date:
    try:
        return date.fromisoformat(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("--version-date должен быть в формате YYYY-MM-DD") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Program AC D1 precheck для одной версии heli_pandas."
    )
    parser.add_argument("--version-date", required=True, type=_parse_version_date)
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--dataset-path")
    return parser.parse_args()


def apply_program_ac_precheck(
    df: pd.DataFrame,
    client,
    version_date: date,
    version_id: int,
    *,
    dataset_path: str | None = None,
    daily_map: dict[int, int] | None = None,
) -> pd.DataFrame:
    """Применяет D1 precheck к переданному heli_pandas DataFrame без записи в БД."""
    from extract.program_ac_precheck_next_day import process_program_ac_precheck_d1

    updated = df.copy()
    old = updated["status_id"].to_numpy(copy=True)
    updated = process_program_ac_precheck_d1(
        updated,
        client,
        version_date,
        version_id,
        dataset_path=dataset_path,
        daily_map=daily_map,
    )
    changed = int((old != updated["status_id"].to_numpy()).sum())
    print(f"🔄 Precheck in-memory status changes: {changed}")
    return updated


def main() -> int:
    print("🚀 === PROGRAM AC PRECHECK RUNNER ===")
    args = parse_args()
    code_root = Path(__file__).resolve().parents[1]
    sys.path.append(str(code_root / 'utils'))
    sys.path.append(str(code_root))
    from config_loader import get_clickhouse_client
    from utils.dwh_post_enrichment import (
        _load_heli_pandas_version,
        _replace_heli_pandas_version,
    )

    client = get_clickhouse_client()
    checks = {
        'heli_pandas': "EXISTS TABLE heli_pandas",
        'md_components': "EXISTS TABLE md_components",
    }
    if not args.dataset_path:
        checks['flight_program_fl'] = "EXISTS TABLE flight_program_fl"
    for name, sql in checks.items():
        if client.execute(sql)[0][0] == 0:
            print(f"❌ Таблица {name} отсутствует — precheck невозможен")
            return 1

    df = _load_heli_pandas_version(client, args.version_date, args.version_id)
    print(f"📦 heli_pandas в памяти: {len(df):,} записей")
    updated_df = apply_program_ac_precheck(
        df,
        client,
        args.version_date,
        args.version_id,
        dataset_path=args.dataset_path,
    )
    _replace_heli_pandas_version(client, updated_df, args.version_date, args.version_id)
    print("✅ Precheck применён")
    return 0


if __name__ == "__main__":
    sys.exit(main())
