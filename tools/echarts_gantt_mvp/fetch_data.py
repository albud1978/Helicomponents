#!/usr/bin/env python3
"""
Генерация данных интервалов занятости линий для ECharts Gantt (вариант A).

Запуск из корня репозитория:
  PYTHONPATH="$PWD/code" python tools/echarts_gantt_mvp/fetch_data.py --version-date 20250704
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from utils.config_loader import get_clickhouse_client


def parse_int_list(raw: str) -> List[int]:
    values = [x.strip() for x in raw.split(",") if x.strip()]
    if not values:
        raise ValueError("Список не может быть пустым")
    return [int(v) for v in values]


def build_query(table: str, version_ids: List[int], groups: List[int], has_date_range: bool) -> str:
    version_ids_sql = ", ".join(str(v) for v in version_ids)
    groups_sql = ", ".join(str(v) for v in groups)
    date_clause = ""
    if has_date_range:
        date_clause = "AND day_date BETWEEN %(date_from)s AND %(date_to)s"

    # PREWHERE version_date гарантирует partition pruning для нового PARTITION BY.
    return f"""
WITH base AS (
    SELECT
        version_date,
        version_id,
        group_by,
        line_id,
        aircraft_number,
        day_u16,
        day_date
    FROM {table}
    PREWHERE version_date = %(version_date)s
    WHERE version_id IN ({version_ids_sql})
      AND group_by IN ({groups_sql})
      AND aircraft_number NOT IN (0, 4294967295)
      {date_clause}
),
islands AS (
    SELECT
        *,
        day_u16 - row_number() OVER (
            PARTITION BY version_date, version_id, group_by, line_id, aircraft_number
            ORDER BY day_u16
        ) AS island_id
    FROM base
)
SELECT
    version_date,
    version_id,
    group_by,
    line_id,
    aircraft_number,
    min(day_date) AS start_date,
    addDays(max(day_date), 1) AS end_date_exclusive,
    dateDiff('day', min(day_date), max(day_date)) + 1 AS duration_days
FROM islands
GROUP BY
    version_date, version_id, group_by,
    line_id, aircraft_number, island_id
ORDER BY line_id, start_date, aircraft_number
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Export repairline occupancy intervals to JSON")
    parser.add_argument("--table", default="sim_repairline_v9", help="Таблица ClickHouse")
    parser.add_argument("--version-date", type=int, required=True, help="version_date, например 20250704")
    parser.add_argument("--version-ids", default="1,2", help="Список version_id через запятую")
    parser.add_argument("--group-by", default="1,2", help="Список group_by через запятую")
    parser.add_argument("--date-from", default="", help="Начальная дата YYYY-MM-DD (опционально)")
    parser.add_argument("--date-to", default="", help="Конечная дата YYYY-MM-DD (опционально)")
    parser.add_argument(
        "--out",
        default="tools/echarts_gantt_mvp/data.json",
        help="Путь вывода JSON",
    )
    args = parser.parse_args()

    version_ids = parse_int_list(args.version_ids)
    groups = parse_int_list(args.group_by)
    has_date_range = bool(args.date_from and args.date_to)
    if bool(args.date_from) ^ bool(args.date_to):
        raise ValueError("Нужно указать одновременно --date-from и --date-to")

    query = build_query(args.table, version_ids, groups, has_date_range)
    params = {"version_date": args.version_date}
    if has_date_range:
        params["date_from"] = args.date_from
        params["date_to"] = args.date_to

    client = get_clickhouse_client()
    rows = client.execute(query, params)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_table": args.table,
        "filters": {
            "version_date": args.version_date,
            "version_ids": version_ids,
            "group_by": groups,
            "date_from": args.date_from or None,
            "date_to": args.date_to or None,
        },
        "rows": [
            {
                "version_date": int(r[0]),
                "version_id": int(r[1]),
                "group_by": int(r[2]),
                "line_id": int(r[3]),
                "aircraft_number": int(r[4]),
                "start_date": str(r[5]),
                "end_date_exclusive": str(r[6]),
                "duration_days": int(r[7]),
            }
            for r in rows
        ],
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload_json = json.dumps(payload, ensure_ascii=False, indent=2)
    out_path.write_text(payload_json, encoding="utf-8")

    js_path = out_path.with_suffix(".js")
    js_payload = f"window.__GANTT_PAYLOAD__ = {payload_json};\n"
    js_path.write_text(js_payload, encoding="utf-8")

    intervals_count = len(payload["rows"])
    print(f"OK: {intervals_count} intervals")
    print(f"JSON: {out_path}")
    print(f"JS: {js_path}")


if __name__ == "__main__":
    main()
