#!/usr/bin/env python3
"""
Пометка исправных агрегатов статусом 3 (Исправен).

Условия:
- group_by > 2 (только агрегаты, не планеры)
- condition = 'ИСПРАВНЫЙ'
- status_id = 0 (ещё не обработаны предыдущими этапами)

Выполняется ПОСЛЕ heli_pandas_component_status.py (этап 12).
Агрегаты на ВС в эксплуатации уже получили status_id = 2.
Этот скрипт обрабатывает оставшиеся исправные агрегаты (на складе, в резерве и т.д.)

Идемпотентен, поддерживает dry-run.
"""

import argparse
import sys
from datetime import datetime, date
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / 'utils'))
sys.path.append(str(code_root))
from config_loader import get_clickhouse_client  # type: ignore


# Условие: агрегаты (group_by > 2), исправные (condition = 'ИСПРАВНЫЙ'), ещё не обработанные (status_id = 0)
BASE_CONDITION = """
    version_date = %(version_date)s
    AND version_id = %(version_id)s
    AND toUInt32(ifNull(group_by, 0)) > 2
    AND upperUTF8(replaceRegexpAll(ifNull(condition, ''), '^\\s+|\\s+$', '')) = 'ИСПРАВНЫЙ'
    AND toUInt8(ifNull(status_id, 0)) = 0
"""

STATS_SQL = f"""
SELECT
    count() AS total_candidates
FROM heli_pandas
WHERE {BASE_CONDITION}
"""

UPDATE_SQL = f"""
ALTER TABLE heli_pandas
UPDATE status_id = 3
WHERE {BASE_CONDITION}
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Помечает исправные агрегаты статусом 3 (Исправен)"
    )
    parser.add_argument(
        "--version-date",
        type=str,
        help="Дата версии данных (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--version-id",
        type=int,
        help="ID версии данных (UInt8)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только вывести статистику без выполнения ALTER UPDATE",
    )
    return parser.parse_args()


def resolve_version(
    client,
    version_date: Optional[str],
    version_id: Optional[int],
) -> Tuple[date, int]:
    if version_date:
        parsed_date = datetime.strptime(version_date, "%Y-%m-%d").date()
        vid = version_id if version_id is not None else 1
        return parsed_date, vid

    row = client.execute(
        """
        SELECT version_date, version_id
        FROM heli_pandas
        ORDER BY version_date DESC, version_id DESC
        LIMIT 1
        """
    )
    if not row:
        raise RuntimeError("Таблица heli_pandas пуста — нечего обрабатывать")
    v_date, v_id = row[0]
    return v_date, int(v_id)


def fetch_stats(client, version_date: date, version_id: int) -> int:
    params = {"version_date": version_date, "version_id": version_id}
    total = client.execute(STATS_SQL, params)[0][0]
    return int(total)


def run_update(client, version_date: date, version_id: int) -> None:
    params = {"version_date": version_date, "version_id": version_id}
    client.execute("SET mutations_sync = 1")
    client.execute(UPDATE_SQL, params)


def _as_u32(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="raise").fillna(0).astype("int64")


def _normalized_text(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().str.upper()


def apply_serviceable_status(
    df: pd.DataFrame, client, version_date: date, version_id: int
) -> pd.DataFrame:
    """Помечает оставшиеся исправные агрегаты status_id=3 в памяти."""
    del client, version_date, version_id
    updated = df.copy()
    required = {"group_by", "condition", "status_id"}
    missing = required - set(updated.columns)
    if missing:
        raise ValueError(f"heli_pandas DataFrame missing columns: {sorted(missing)}")

    mask = (
        (_as_u32(updated["group_by"]) > 2)
        & (_normalized_text(updated["condition"]) == "ИСПРАВНЫЙ")
        & (_as_u32(updated["status_id"]) == 0)
    )
    updated.loc[mask, "status_id"] = 3
    print(f"✅ serviceable_status in-memory: updated={int(mask.sum())}")
    return updated


def main() -> int:
    args = parse_args()
    client = get_clickhouse_client()

    version_date, version_id = resolve_version(
        client, args.version_date, args.version_id
    )
    print(
        f"📅 Версия {version_date} (version_id={version_id}), "
        f"dry-run={'ON' if args.dry_run else 'OFF'}"
    )

    candidates = fetch_stats(client, version_date, version_id)
    print(f"📊 Исправных агрегатов со status_id=0: {candidates}")

    if candidates == 0:
        print("✅ Все исправные агрегаты уже обработаны")
        return 0

    if args.dry_run:
        print("📝 DRY-RUN завершён без изменений")
        return 0

    from utils.dwh_post_enrichment import (
        _load_heli_pandas_version,
        _replace_heli_pandas_version,
    )

    df = _load_heli_pandas_version(client, version_date, version_id)
    updated_df = apply_serviceable_status(df, client, version_date, version_id)
    _replace_heli_pandas_version(client, updated_df, version_date, version_id)
    remaining = fetch_stats(client, version_date, version_id)
    updated = candidates - remaining
    print(f"✅ Обновлено: {updated} агрегатов → status_id=3 (Исправен)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


















