#!/usr/bin/env python3
"""
MP3 GroupBy Filler
Добавляет колонку group_by в heli_pandas и заполняет её из md_components.group_by
по связи: heli_pandas.partseqno_i = md_components.partno_comp.

Особенности:
- Безопасно добавляет колонку (IF NOT EXISTS)
- Идемпотентная запись: только там, где group_by = 0
- Поддержка --dry-run (по умолчанию): только печатает SQL
- Поддержка --apply: выполняет SQL против ClickHouse

Дата: 2025-08-10
"""

import argparse
import sys
from typing import List

# Доступ к конфигурации
sys.path.append(str(__file__).rsplit('/utils/', 1)[0] + '/utils')
from config_loader import get_clickhouse_client

ADD_COLUMN_SQL = """
ALTER TABLE heli_pandas 
ADD COLUMN IF NOT EXISTS group_by UInt8 DEFAULT 0
""".strip()

FILL_GROUP_BY_SQL = """
ALTER TABLE heli_pandas 
UPDATE group_by = m.group_by
WHERE group_by = 0
  AND partseqno_i IS NOT NULL
  AND partseqno_i IN (
      SELECT partno_comp FROM md_components WHERE partno_comp IS NOT NULL AND group_by IS NOT NULL
  )
SETTINGS allow_experimental_alter_update = 1
""".strip()

# Вариант с явным JOIN через временный подзапрос для явности цели
FILL_GROUP_BY_JOIN_SQL = """
ALTER TABLE heli_pandas 
UPDATE group_by = (
    SELECT any(group_by) FROM md_components m WHERE m.partno_comp = heli_pandas.partseqno_i
)
WHERE group_by = 0 AND partseqno_i IS NOT NULL
SETTINGS allow_experimental_alter_update = 1
""".strip()


def print_plan(sqls: List[str]):
    print("\n=== DRY RUN: Планируемые SQL ===")
    for i, sql in enumerate(sqls, 1):
        print(f"\n-- SQL #{i}:\n{sql};")
    print("\nПодсказка: запустите с --apply для выполнения.")


def main():
    parser = argparse.ArgumentParser(description='Заполнение MP3.group_by из MP1 (partseqno_i = partno_comp)')
    parser.add_argument('--apply', action='store_true', help='Выполнить SQL (по умолчанию только печать)')
    parser.add_argument('--use-join', action='store_true', help='Использовать вариант с подзапросом JOIN вместо IN')
    args = parser.parse_args()

    sqls = [ADD_COLUMN_SQL, (FILL_GROUP_BY_JOIN_SQL if args.use_join else FILL_GROUP_BY_SQL)]

    if not args.apply:
        print_plan(sqls)
        return 0

    client = get_clickhouse_client()
    for sql in sqls:
        client.execute(sql)
    print("✅ MP3.group_by добавлен и заполнен (идемпотентно)")
    return 0


if __name__ == '__main__':
    sys.exit(main())