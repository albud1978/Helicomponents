#!/usr/bin/env python3
"""
HeliPandas GroupBy Enricher

Добавляет и заполняет колонку group_by в таблице heli_pandas на основе
md_components.group_by через ключевое соответствие:
  heli_pandas.partseqno_i = md_components.partseqno_i

Особенности:
- Безопасное добавление колонки (IF NOT EXISTS)
- Идемпотентное обновление: только там, где group_by = 0
- DRY-RUN по умолчанию (печатает SQL). Для применения используйте --apply

Дата: 2025-08-25
"""

import argparse
import sys
from datetime import date
from typing import List, Dict, Tuple

# Доступ к ClickHouse
sys.path.append(str(__file__).rsplit('/code/', 1)[0] + '/code/utils')
from config_loader import get_clickhouse_client  # type: ignore


ADD_COLUMN_SQL = (
    "ALTER TABLE heli_pandas "
    "ADD COLUMN IF NOT EXISTS group_by UInt8 DEFAULT 0"
)

def make_row_update_sql(
    partseqno_i: int,
    group_by: int,
    version_date: date,
    version_id: int,
) -> str:
    return (
        "ALTER TABLE heli_pandas "
        f"UPDATE group_by = toUInt8({group_by}) "
        f"WHERE version_date = toDate('{version_date.isoformat()}') "
        f"AND version_id = toUInt8({version_id}) "
        f"AND group_by = 0 AND partseqno_i = toUInt32({partseqno_i})"
    )

def build_update_sqls(
    client,
    version_date: date,
    version_id: int,
) -> List[str]:
    sqls: List[str] = [ADD_COLUMN_SQL]
    # Собираем соответствия partseqno_i -> group_by через JOIN, группируем
    rows: List[Tuple[int, int]] = client.execute(
        """
        SELECT toUInt32(hp.partseqno_i) AS partseqno_i,
               toUInt8(any(m.group_by)) AS gb
        FROM heli_pandas hp
        INNER JOIN md_components m
            ON m.partseqno_i = hp.partseqno_i
        WHERE hp.group_by = 0
          AND hp.version_date = %(vd)s
          AND hp.version_id = %(vi)s
          AND hp.partseqno_i IS NOT NULL
          AND m.group_by IS NOT NULL
        GROUP BY partseqno_i
        ORDER BY partseqno_i
        """,
        {"vd": version_date, "vi": version_id},
    )
    for partseq, gb in rows:
        if gb and partseq:
            sqls.append(
                make_row_update_sql(
                    int(partseq), int(gb), version_date, version_id
                )
            )
    return sqls


def print_plan(sqls: List[str]) -> None:
    print("\n=== DRY RUN: Планируемые SQL ===")
    for i, sql in enumerate(sqls, 1):
        print(f"\n-- SQL #{i}:\n{sql};")
    print("\nПодсказка: запустите с --apply для выполнения.")


def main() -> int:
    parser = argparse.ArgumentParser(description='Обогащение heli_pandas.group_by из md_components.partseqno_i')
    parser.add_argument('--apply', action='store_true', help='Выполнить SQL (по умолчанию DRY-RUN)')
    parser.add_argument('--version-date', required=True, help='Дата версии YYYY-MM-DD')
    parser.add_argument('--version-id', type=int, required=True, help='ID версии данных')
    args = parser.parse_args()
    version_date = date.fromisoformat(args.version_date)

    client = get_clickhouse_client()
    print(f"🗓️ Версия данных: {version_date} (version_id={args.version_id})")

    # Формируем план обновлений по группам partseqno_i
    sqls: List[str] = build_update_sqls(client, version_date, args.version_id)

    if not args.apply:
        print_plan(sqls)
        return 0

    # Выполнение плана
    for sql in sqls:
        client.execute(sql)

    print("✅ heli_pandas.group_by добавлен и заполнен (идемпотентно)")
    return 0


if __name__ == '__main__':
    sys.exit(main())


