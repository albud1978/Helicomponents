#!/usr/bin/env python3
"""
INV-5: Баланс наработок — пошаговая проверка.

Для каждой пары последовательных шагов одного агента:
  sne[N+1] - sne[N] == daily_today_u32[N+1]

Исключения:
  - Первый шаг агента (нет предыдущего)
  - Динамический spawn: pre_status_id=0 на текущем шаге (новый агент)

Известный артефакт:
  - На шаге выхода из ops (pre_status_id=2, status_id!=2) dt=0,
    но sne уже увеличен ops_increment-ом. Это артефакт порядка
    RTC слоёв (increment → demote → export). Выделяется отдельно.
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


def run(client, version_id: int, version_date=None, table: str = "sim_masterv2_v9") -> bool:
    table = validate_table_name(table)

    vd_filter = ""
    params = {"vid": version_id}
    if version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = version_date

    base_subquery = f"""
        SELECT
            aircraft_number, version_date, day_u16, status_id, pre_status_id,
            sne, daily_today_u32 AS dt,
            lagInFrame(sne, 1, 0) OVER w AS prev_sne,
            lagInFrame(day_u16, 1, 0) OVER w AS prev_day
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
        WINDOW w AS (PARTITION BY aircraft_number, group_by, version_date ORDER BY day_u16)
    """

    # Реальные нарушения (не артефакт transition-out)
    real_query = f"""
    SELECT count()
    FROM ({base_subquery})
    WHERE prev_day > 0
      AND pre_status_id > 0
      AND toInt64(sne) - toInt64(prev_sne) != toInt64(dt)
      AND NOT (pre_status_id = 2 AND status_id != 2 AND dt = 0)
    """
    real_violations = client.execute(real_query, params)[0][0]

    # Артефакт transition-out (известный)
    artifact_query = f"""
    SELECT count()
    FROM ({base_subquery})
    WHERE prev_day > 0
      AND pre_status_id > 0
      AND toInt64(sne) - toInt64(prev_sne) != toInt64(dt)
      AND pre_status_id = 2 AND status_id != 2 AND dt = 0
    """
    artifact_count = client.execute(artifact_query, params)[0][0]

    details = [
        f"real_violations={real_violations}",
        f"transition_out_artifacts={artifact_count} (pre_st=2→st≠2, dt=0, sne incremented — RTC layer ordering)",
    ]

    if real_violations > 0:
        sample_query = f"""
        SELECT
            aircraft_number, day_u16, status_id, pre_status_id,
            sne, prev_sne, dt,
            toInt64(sne) - toInt64(prev_sne) AS sne_delta,
            toInt64(sne) - toInt64(prev_sne) - toInt64(dt) AS diff
        FROM ({base_subquery})
        WHERE prev_day > 0
          AND pre_status_id > 0
          AND toInt64(sne) - toInt64(prev_sne) != toInt64(dt)
          AND NOT (pre_status_id = 2 AND status_id != 2 AND dt = 0)
        ORDER BY abs(toInt64(sne) - toInt64(prev_sne) - toInt64(dt)) DESC
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append("top5 real violations (acn, day, st, pre_st, sne, prev, dt, delta, diff):")
        for acn, d, st, pst, sne, psne, dt, sd, diff in rows:
            details.append(
                f"  acn={acn}, day={d}, st={st}, pre_st={pst}, "
                f"sne={sne}, prev={psne}, dt={dt}, delta={sd}, diff={diff}"
            )

    passed = real_violations == 0
    print_result("INV-5 balance increments", passed, details)
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="INV-5: баланс наработок (пошаговый sne_diff == dt)"
    )
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--version-date", type=int, default=None,
        help="version_date (YYYYMMDD) для фильтрации",
    )
    parser.add_argument(
        "--table", default="sim_masterv2_v9",
        help="Таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
    )
    args = parser.parse_args()
    client = get_client()
    passed = run(client, args.version_id, args.version_date, args.table)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
