#!/usr/bin/env python3
"""
INV-12: ppr <= oh по всему жизненному циклу планеров.

Проверка не фильтрует status_id: перелёт по ремонтному ресурсу может
материализоваться на exit-day строке с терминальным статусом.

Классификация превышений (решение Алексея 2026-07-01):
  - "входной переналёт": борт уже во входе (heli_pandas) имеет ppr>oh —
    признаётся ВАЛИДНЫМ для симуляции, помечается WARNING (источник данных
    правится отдельно). Инвариант при этом НЕ падает.
  - "переналёт движка": борт зашёл под лимитом (ppr<=oh во входе), но в
    симуляции ppr превысил oh — это РЕАЛЬНОЕ нарушение (FAIL).
"""
import argparse
import re
import sys

from ch_client import get_client


def validate_table_name(table: str) -> str:
    if not re.match(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$", table):
        raise SystemExit(f"Некорректное имя таблицы: {table}")
    return table


def print_result(name: str, passed: bool, details) -> None:
    status = "PASS" if passed else "FAIL"
    print("=" * 80)
    print(f"{name}: {status}")
    for line in details:
        print(line)
    print("=" * 80)


def _yyyymmdd_to_date_str(version_date_int: int) -> str:
    vd = int(version_date_int)
    return f"{vd // 10000:04d}-{(vd // 100) % 100:02d}-{vd % 100:02d}"


def _input_exceed_keys(client, version_date_int: int, input_version_id: int):
    """(aircraft_number, group_by) планеров с ppr>oh во входном heli_pandas."""
    rows = client.execute(
        """
        SELECT DISTINCT aircraft_number, group_by
        FROM heli_pandas
        WHERE version_date = toDate(%(vdstr)s)
          AND version_id = %(ivid)s
          AND ppr > oh
          AND group_by IN (1, 2)
        """,
        {
            "vdstr": _yyyymmdd_to_date_str(version_date_int),
            "ivid": int(input_version_id),
        },
    )
    return {(int(acn), int(gb)) for acn, gb in rows}


def _format_sample(rows) -> list:
    out = []
    for row in rows[:10]:
        acn, group_by, status_id, day, ppr, oh, exceed, limiter, dt = row
        out.append(
            f"  acn={acn}, group_by={group_by}, status={status_id}, day={day}, "
            f"ppr={ppr}, oh={oh}, exceed={exceed}, limiter={limiter}, dt={dt}"
        )
    return out


def run(
    client,
    version_id: int,
    version_date=None,
    table: str = "sim_masterv2_v9",
    input_version_id=None,
) -> bool:
    table = validate_table_name(table)
    if input_version_id is None:
        input_version_id = version_id

    vd_filter = ""
    params = {"vid": version_id}
    if version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = version_date

    over_query = f"""
    SELECT
        aircraft_number,
        group_by,
        status_id,
        day_u16,
        ppr,
        oh,
        (ppr - oh) AS exceed,
        limiter,
        daily_today_u32
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter}
      AND ppr > oh
      AND group_by IN (1, 2)
    ORDER BY exceed DESC
    """
    all_rows = client.execute(over_query, params)
    total_violations = len(all_rows)

    # Разделение на входной переналёт (WARNING) и переналёт движка (FAIL)
    # возможно только при известном version_date (нужно для скоупа heli_pandas).
    can_classify = version_date is not None
    input_keys = set()
    if can_classify:
        input_keys = _input_exceed_keys(client, version_date, input_version_id)

    warn_rows = [r for r in all_rows if (int(r[0]), int(r[1])) in input_keys]
    real_rows = [r for r in all_rows if (int(r[0]), int(r[1])) not in input_keys]

    warn_keys = sorted({(int(r[0]), int(r[1])) for r in warn_rows})
    real_keys = sorted({(int(r[0]), int(r[1])) for r in real_rows})

    details = [
        f"total_ppr_gt_oh_rows={total_violations}",
        f"input_version_id={input_version_id}",
    ]
    if not can_classify:
        details.append(
            "classification_skipped=1 (нет version_date — все превышения строгие)"
        )

    if warn_rows:
        details.append(
            f"⚠️ WARNING input_originated: rows={len(warn_rows)} "
            f"aircraft={len(warn_keys)} — валидны (переналёт из источника), "
            "правка данных вне симуляции"
        )
        details.append(
            "  warn (acn,group_by): "
            + ", ".join(f"{acn}/{gb}" for acn, gb in warn_keys)
        )
        details.extend(_format_sample(warn_rows))

    details.append(
        f"sim_introduced_violations: rows={len(real_rows)} aircraft={len(real_keys)}"
    )
    if real_rows:
        details.append(
            "top10 sim_introduced (acn, group_by, status, day, "
            "ppr, oh, exceed, limiter, dt):"
        )
        details.extend(_format_sample(real_rows))

    passed = len(real_rows) == 0
    print_result("INV-12 ppr<=oh", passed, details)
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="INV-12: ppr <= oh по всему жизненному циклу планеров"
    )
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--version-date",
        type=int,
        default=None,
        help="version_date (YYYYMMDD) для фильтрации",
    )
    parser.add_argument(
        "--input-version-id",
        type=int,
        default=None,
        help="version_id входного heli_pandas для классификации входного "
        "переналёта (по умолчанию = --version-id)",
    )
    parser.add_argument(
        "--table",
        default="sim_masterv2_v9",
        help="Таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
    )
    args = parser.parse_args()
    client = get_client()
    passed = run(
        client,
        args.version_id,
        args.version_date,
        args.table,
        args.input_version_id,
    )
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
