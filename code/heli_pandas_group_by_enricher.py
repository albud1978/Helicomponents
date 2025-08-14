#!/usr/bin/env python3
"""
HeliPandas GroupBy Enricher

Добавляет и заполняет колонку group_by в таблице heli_pandas на основе
md_components.group_by через ключевое соответствие:
  heli_pandas.partseqno_i = md_components.partno_comp

Особенности:
- Безопасное добавление колонки (IF NOT EXISTS)
- Идемпотентное обновление: только там, где group_by = 0
- DRY-RUN по умолчанию (печатает SQL). Для применения используйте --apply

Дата: 2025-08-14
"""

import argparse
import sys
from typing import List, Dict

# Доступ к ClickHouse
sys.path.append(str(__file__).rsplit('/code/', 1)[0] + '/code/utils')
from config_loader import get_clickhouse_client  # type: ignore


ADD_COLUMN_SQL = (
    "ALTER TABLE heli_pandas "
    "ADD COLUMN IF NOT EXISTS group_by UInt8 DEFAULT 0"
)

# Создание временной таблицы в памяти для стабильного обновления без JOIN в UPDATE
CREATE_TMP_SQL = (
    "CREATE TABLE IF NOT EXISTS tmp_partseq_to_group_by "
    "(partseqno_i UInt32, group_by UInt8) ENGINE = Memory"
)

FILL_TMP_SQL = (
    "INSERT INTO tmp_partseq_to_group_by (partseqno_i, group_by) "
    "SELECT partno_comp AS partseqno_i, toUInt8(group_by) AS group_by "
    "FROM md_components WHERE partno_comp IS NOT NULL AND group_by IS NOT NULL"
)

DROP_TMP_SQL = "DROP TABLE IF EXISTS tmp_partseq_to_group_by"


def print_plan(sqls: List[str]) -> None:
    print("\n=== DRY RUN: Планируемые SQL ===")
    for i, sql in enumerate(sqls, 1):
        print(f"\n-- SQL #{i}:\n{sql};")
    print("\nПодсказка: запустите с --apply для выполнения.")


def main() -> int:
    parser = argparse.ArgumentParser(description='Обогащение heli_pandas.group_by из md_components.partno_comp')
    parser.add_argument('--dry-run', action='store_true', help='Только показать SQL без выполнения')
    # Параметры версионирования для совместимости с Extract Master (не используются напрямую)
    parser.add_argument('--version-date', type=str, default=None, help='Дата версии данных (совместимость)')
    parser.add_argument('--version-id', type=int, default=None, help='ID версии данных (совместимость)')
    args = parser.parse_args()

    client = get_clickhouse_client()
    if args.version_date is not None and args.version_id is not None:
        print(f"🗓️ Версия данных (совместимость): {args.version_date} (version_id={args.version_id})")

    # Узнаем набор distinct group_by из md_components (обычно малый кардиналитет)
    distinct_vals: List[int] = [row[0] for row in client.execute(
        "SELECT DISTINCT toUInt8(group_by) FROM md_components WHERE group_by IS NOT NULL ORDER BY group_by"
    )]

    # Формируем пошаговый план
    sqls: List[str] = [
        ADD_COLUMN_SQL,
        DROP_TMP_SQL,
        CREATE_TMP_SQL,
        FILL_TMP_SQL,
    ]
    # По одному UPDATE на каждое значение group_by
    for gb in distinct_vals:
        sqls.append(
            (
                f"ALTER TABLE heli_pandas "
                f"UPDATE group_by = toUInt8({gb}) "
                f"WHERE group_by = 0 AND partseqno_i IN ("
                f"SELECT partseqno_i FROM tmp_partseq_to_group_by WHERE group_by = toUInt8({gb})"
                f")"
            )
        )
    sqls.append(DROP_TMP_SQL)

    if args.dry_run:
        print_plan(sqls)
        return 0

    # Выполнение плана
    for sql in sqls:
        client.execute(sql)

    print("✅ heli_pandas.group_by добавлен и заполнен (идемпотентно)")
    return 0


if __name__ == '__main__':
    sys.exit(main())


