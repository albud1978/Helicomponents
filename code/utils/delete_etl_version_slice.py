#!/usr/bin/env python3
"""
Удаление одного среза ETL в ClickHouse: строки с заданными version_date и version_id.

Обходит таблицы из известного списка (ETL + версионные dict_* + dict_digital_values_flat),
для каждой проверяет наличие колонок version_date и version_id и выполняет DELETE только
если обе есть. Таблицы без версионирования (например dict_status_flat) не трогает.

Использование:
  python code/utils/delete_etl_version_slice.py --version-date 2026-04-08 --version-id 1 --dry-run
  python code/utils/delete_etl_version_slice.py --version-date 2026-04-08 --version-id 1 --execute
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path

code_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(code_root / "utils"))
sys.path.insert(0, str(code_root))

from config_loader import get_clickhouse_client  # noqa: E402

# ETL + словари с версионированием (см. etl_version_manager, dictionary_creator, digital_values_dictionary_creator)
CANDIDATE_TABLES = [
    "heli_pandas",
    "heli_raw",
    "md_components",
    "status_overhaul",
    "program_ac",
    "flight_program_ac",
    "flight_program_fl",
    "dict_digital_values_flat",
    "dict_partno_flat",
    "dict_serialno_flat",
    "dict_owner_flat",
    "dict_ac_type_flat",
    "dict_aircraft_number_flat",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Удаление среза ETL по version_date + version_id")
    p.add_argument("--version-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--version-id", type=int, required=True)
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Только подсчёт строк, без DELETE",
    )
    p.add_argument(
        "--execute",
        action="store_true",
        help="Выполнить DELETE (требуется явно)",
    )
    return p.parse_args()


def table_has_version_columns(client, table: str) -> bool:
    q = """
    SELECT count()
    FROM system.columns
    WHERE database = currentDatabase()
      AND table = %(t)s
      AND name IN ('version_date', 'version_id')
    """
    n = int(client.execute(q, {"t": table})[0][0])
    return n == 2


def main() -> int:
    args = parse_args()
    if args.execute and args.dry_run:
        print("❌ Укажите только один режим: --dry-run или --execute")
        return 2
    if not args.execute and not args.dry_run:
        print("❌ Укажите --dry-run или --execute")
        return 2

    vd = datetime.strptime(args.version_date, "%Y-%m-%d").date()
    vid = args.version_id

    client = get_clickhouse_client()

    print(
        f"📅 Срез: version_date={vd}, version_id={vid} "
        f"({'DRY-RUN' if args.dry_run else 'DELETE'})"
    )

    total_before = 0
    total_deleted = 0

    for table in CANDIDATE_TABLES:
        exists = int(client.execute(f"EXISTS TABLE {table}")[0][0])
        if not exists:
            print(f"⏭️  {table}: таблицы нет")
            continue
        if not table_has_version_columns(client, table):
            print(f"⏭️  {table}: нет version_date+version_id — пропуск")
            continue

        count_sql = f"""
        SELECT count()
        FROM {table}
        WHERE version_date = %(vd)s AND version_id = %(vid)s
        """
        n = int(
            client.execute(
                count_sql,
                {"vd": vd, "vid": vid},
            )[0][0]
        )
        total_before += n
        if n == 0:
            print(f"   {table}: 0 строк")
            continue

        print(f"   {table}: {n} строк")
        if args.execute:
            delete_sql = f"""
            DELETE FROM {table}
            WHERE version_date = %(vd)s AND version_id = %(vid)s
            """
            client.execute(delete_sql, {"vd": vd, "vid": vid})
            total_deleted += n

    print(f"📊 Итого строк в срезе: {total_before}")
    if args.execute:
        print(f"✅ DELETE отправлен; удалено по учёту до вызова: {total_deleted} (проверьте финальные count в CH)")
    else:
        print("📝 DRY-RUN: DELETE не выполнялся")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
