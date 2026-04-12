#!/usr/bin/env python3
"""
Снимок таблиц ClickHouse (MergeTree*) в отдельную базу на том же сервере для сравнения/отката.

Режимы:
  --scope etl (по умолчанию) — только конвейер экстракта и словари (см. ETL_SNAPSHOT_TABLES).
  --scope all        — все MergeTree-таблицы в рабочей базе (может быть очень долго и тяжело).

Структура: CREATE TABLE backup_db.t AS source_db.t, затем INSERT SELECT.

Примеры:
  python code/utils/backup_clickhouse_etl_snapshot.py --dry-run
  python code/utils/backup_clickhouse_etl_snapshot.py --execute
  python code/utils/backup_clickhouse_etl_snapshot.py --execute --suffix 2026_04_12_pre_extract
  python code/utils/backup_clickhouse_etl_snapshot.py --execute --scope all
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import date
from pathlib import Path

code_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(code_root / "utils"))
sys.path.insert(0, str(code_root))

from config_loader import get_clickhouse_client, load_clickhouse_config  # noqa: E402

_SAFE_SUFFIX = re.compile(r"^[a-zA-Z0-9_]+$")

# Снимок для сравнения после экстракта (без sim_*, Olap*, тестовых бэкапов таблиц)
ETL_SNAPSHOT_TABLES = [
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
    "dict_status_flat",
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Снимок MergeTree-таблиц в БД hc_snapshot_<suffix>"
    )
    p.add_argument(
        "--suffix",
        type=str,
        default=None,
        help="Суффикс имени БД (по умолчанию: YYYY_MM_DD)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Только список таблиц и оценка строк",
    )
    p.add_argument(
        "--execute",
        action="store_true",
        help="Создать БД и скопировать данные",
    )
    p.add_argument(
        "--replace",
        action="store_true",
        help="Если БД-снимок уже есть — DROP DATABASE ... SYNC и создать заново",
    )
    p.add_argument(
        "--scope",
        choices=("etl", "all"),
        default="etl",
        help="etl: только таблицы конвейера экстракта; all: все MergeTree в базе",
    )
    return p.parse_args()


def list_merge_tree_tables(client, database: str) -> list[str]:
    q = """
    SELECT name
    FROM system.tables
    WHERE database = %(db)s
      AND positionCaseInsensitive(engine, 'MergeTree') > 0
      AND name NOT LIKE '.inner%%'
    ORDER BY name
    """
    rows = client.execute(q, {"db": database})
    return [r[0] for r in rows]


def table_row_count(client, database: str, table: str) -> int:
    r = client.execute(f"SELECT count() FROM `{database}`.`{table}`")
    return int(r[0][0])


def main() -> int:
    args = parse_args()
    if args.execute and args.dry_run:
        print("❌ Укажите только один режим: --dry-run или --execute")
        return 2
    if not args.execute and not args.dry_run:
        print("❌ Укажите --dry-run или --execute")
        return 2

    cfg = load_clickhouse_config()
    src_db: str = cfg["database"]
    suffix = args.suffix or date.today().strftime("%Y_%m_%d")
    if not _SAFE_SUFFIX.match(suffix):
        print("❌ --suffix: только буквы, цифры и подчёркивание")
        return 2

    backup_db = f"hc_snapshot_{suffix}"

    client = get_clickhouse_client()

    if args.scope == "all":
        tables = list_merge_tree_tables(client, src_db)
    else:
        tables = []
        missing: list[str] = []
        for name in ETL_SNAPSHOT_TABLES:
            ex = int(
                client.execute(
                    "SELECT count() FROM system.tables WHERE database = %(db)s AND name = %(n)s",
                    {"db": src_db, "n": name},
                )[0][0]
            )
            if ex:
                eng = client.execute(
                    """
                    SELECT engine FROM system.tables
                    WHERE database = %(db)s AND name = %(n)s
                    """,
                    {"db": src_db, "n": name},
                )[0][0]
                if "MergeTree" not in eng:
                    print(f"⚠️ {name}: движок {eng!r} — пропуск (не MergeTree)")
                    continue
                tables.append(name)
            else:
                missing.append(name)
        if missing:
            print(f"ℹ️ Нет в базе (пропуск): {', '.join(missing)}")
    if not tables:
        print(f"⚠️ Нечего копировать в режиме {args.scope!r}")
        return 1

    exists = int(
        client.execute(
            "SELECT count() FROM system.databases WHERE name = %(n)s",
            {"n": backup_db},
        )[0][0]
    )

    print(f"📂 Источник: {src_db}")
    print(f"📂 Снимок:   {backup_db}")
    print(f"📋 Таблиц:   {len(tables)}")

    total_rows = 0
    counts: list[tuple[str, int]] = []
    for t in tables:
        n = table_row_count(client, src_db, t)
        counts.append((t, n))
        total_rows += n

    for t, n in counts:
        print(f"   {t}: {n:,} строк")
    print(f"📊 Всего строк (оценка): {total_rows:,}")

    if args.dry_run:
        print("📝 DRY-RUN: копирование не выполнялось")
        return 0

    if exists and not args.replace:
        print(
            f"❌ База {backup_db!r} уже существует. "
            f"Удалите вручную или запустите с --replace"
        )
        return 3

    # Долгие INSERT SELECT
    client.execute("SET max_execution_time = 0")

    if exists and args.replace:
        client.execute(f"DROP DATABASE IF EXISTS `{backup_db}` SYNC")

    client.execute(f"CREATE DATABASE IF NOT EXISTS `{backup_db}`")

    for i, t in enumerate(tables, 1):
        n = dict(counts)[t]
        print(f"⏳ [{i}/{len(tables)}] {t} ({n:,} строк) …")
        client.execute(f"CREATE TABLE `{backup_db}`.`{t}` AS `{src_db}`.`{t}`")
        client.execute(
            f"INSERT INTO `{backup_db}`.`{t}` SELECT * FROM `{src_db}`.`{t}`"
        )
        got = table_row_count(client, backup_db, t)
        if got != n:
            print(f"❌ {t}: ожидалось {n}, в снимке {got}")
            return 4
        print(f"   ✅ {t}: {got:,}")

    print(f"✅ Снимок готов: `{backup_db}` (строк: {total_rows:,})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
