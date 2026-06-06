#!/usr/bin/env python3
"""
INV-10: Баланс оборота по статусам.

Формула для каждого статуса s in {1,2,3,4,6,7}:
  initial(s) + entries(s) + spawn_entries(s) = exits(s) + final(s)

Где:
- initial(s): агенты в статусе s на первом шаге (min day_u16)
- entries(s): переходы INTO s (pre_status_id != status_id, pre_status_id > 0)
- spawn_entries(s): spawn (pre_status_id = 0)
- exits(s): переходы OUT OF s (pre_status_id = s, status_id != s, pre_status_id > 0)
- final(s): агенты в статусе s на последнем шаге (max day_u16)

Дополнительно выводится таблица фактических переходов с LEGAL/ILLEGAL.
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


def format_table(headers, rows):
    if not rows:
        widths = [len(str(h)) for h in headers]
        header_line = " | ".join(
            str(h).ljust(widths[idx]) for idx, h in enumerate(headers)
        )
        sep_line = "-+-".join("-" * w for w in widths)
        return [header_line, sep_line, "(empty)"]
    widths = [len(str(h)) for h in headers]
    for row in rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(str(cell)))
    header_line = " | ".join(
        str(h).ljust(widths[idx]) for idx, h in enumerate(headers)
    )
    sep_line = "-+-".join("-" * w for w in widths)
    body_lines = [
        " | ".join(str(cell).ljust(widths[idx]) for idx, cell in enumerate(row))
        for row in rows
    ]
    return [header_line, sep_line] + body_lines


def main() -> int:
    parser = argparse.ArgumentParser(description="INV-10: turnover balance")
    parser.add_argument("--version-id", required=True, type=int)
    parser.add_argument("--version-date", type=int, default=None)
    parser.add_argument("--table", default="sim_masterv2_v9")
    args = parser.parse_args()
    table = validate_table_name(args.table)
    client = get_client()

    vd_filter = ""
    vd_filter_m = ""
    params = {"vid": args.version_id}
    if args.version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        vd_filter_m = " AND m.version_date = %(vdate)s"
        params["vdate"] = args.version_date

    minmax_query = f"""
    SELECT version_date, min(day_u16) AS min_day, max(day_u16) AS max_day
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter}
      AND group_by IN (1, 2)
    GROUP BY version_date
    ORDER BY version_date
    """
    minmax_rows = client.execute(minmax_query, params)
    day_ranges = [
        (int(version_date), int(min_day), int(max_day))
        for version_date, min_day, max_day in minmax_rows
        if min_day is not None and max_day is not None
    ]
    has_data = bool(day_ranges)

    # Считаем входы и выходы для каждого статуса
    # Вход: pre_status_id != status_id (т.е. переход произошёл)
    entries_query = f"""
    SELECT version_date, status_id AS s, count() AS entries
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter}
      AND group_by IN (1, 2)
      AND pre_status_id != status_id
      AND pre_status_id > 0
    GROUP BY version_date, status_id
    ORDER BY version_date, status_id
    """
    entries_rows = client.execute(entries_query, params) if has_data else []
    entries = {(int(r[0]), int(r[1])): int(r[2]) for r in entries_rows}

    spawn_query = f"""
    SELECT version_date, status_id AS s, count() AS spawn_entries
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter}
      AND group_by IN (1, 2)
      AND pre_status_id = 0
    GROUP BY version_date, status_id
    ORDER BY version_date, status_id
    """
    spawn_rows = client.execute(spawn_query, params) if has_data else []
    spawn_entries = {(int(r[0]), int(r[1])): int(r[2]) for r in spawn_rows}

    # Выходы: pre_status_id = s, status_id != s
    exits_query = f"""
    SELECT version_date, pre_status_id AS s, count() AS exits
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter}
      AND group_by IN (1, 2)
      AND pre_status_id != status_id
      AND pre_status_id > 0
    GROUP BY version_date, pre_status_id
    ORDER BY version_date, pre_status_id
    """
    exits_rows = client.execute(exits_query, params) if has_data else []
    exits = {(int(r[0]), int(r[1])): int(r[2]) for r in exits_rows}

    # Начальные и финальные статусы (по первому/последнему дню)
    initial_counts = {}
    final_counts = {}
    if has_data:
        initial_query = f"""
        WITH minmax AS (
            SELECT version_date, min(day_u16) AS min_day
            FROM {table}
            WHERE version_id = %(vid)s{vd_filter}
              AND group_by IN (1, 2)
            GROUP BY version_date
        )
        SELECT m.version_date, m.pre_status_id AS s, count() AS cnt
        FROM {table} m
        INNER JOIN minmax mm
            ON m.version_date = mm.version_date
           AND m.day_u16 = mm.min_day
        WHERE m.version_id = %(vid)s{vd_filter_m}
          AND m.group_by IN (1, 2)
          AND m.pre_status_id > 0
        GROUP BY m.version_date, m.pre_status_id
        ORDER BY m.version_date, m.pre_status_id
        """
        initial_rows = client.execute(initial_query, params)
        initial_counts = {(int(r[0]), int(r[1])): int(r[2]) for r in initial_rows}

        final_query = f"""
        WITH minmax AS (
            SELECT version_date, max(day_u16) AS max_day
            FROM {table}
            WHERE version_id = %(vid)s{vd_filter}
              AND group_by IN (1, 2)
            GROUP BY version_date
        )
        SELECT m.version_date, m.status_id AS s, count() AS cnt
        FROM {table} m
        INNER JOIN minmax mm
            ON m.version_date = mm.version_date
           AND m.day_u16 = mm.max_day
        WHERE m.version_id = %(vid)s{vd_filter_m}
          AND m.group_by IN (1, 2)
        GROUP BY m.version_date, m.status_id
        ORDER BY m.version_date, m.status_id
        """
        final_rows = client.execute(final_query, params)
        final_counts = {(int(r[0]), int(r[1])): int(r[2]) for r in final_rows}

    # Баланс для каждого статуса
    check_states = [1, 2, 3, 4, 6, 7]
    violations = 0
    balance_rows = []

    for version_date, _min_day, _max_day in day_ranges:
        for s in check_states:
            key = (version_date, s)
            initial = initial_counts.get(key, 0)
            ent = entries.get(key, 0)
            spawn = spawn_entries.get(key, 0)
            ext = exits.get(key, 0)
            final = final_counts.get(key, 0)
            balance = (initial + ent + spawn) - (ext + final)
            ok = balance == 0
            balance_rows.append(
                [
                    version_date,
                    s,
                    initial,
                    ent,
                    spawn,
                    ext,
                    final,
                    balance,
                    "OK" if ok else "FAIL",
                ]
            )
            if not ok:
                violations += 1

    # Таблица переходов с LEGAL/ILLEGAL
    transitions_query = f"""
    SELECT version_date, pre_status_id AS pre, status_id AS st, count() AS cnt
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter}
      AND group_by IN (1, 2)
      AND pre_status_id != status_id
    GROUP BY version_date, pre_status_id, status_id
    ORDER BY version_date, pre_status_id, status_id
    """
    transition_rows = client.execute(transitions_query, params) if has_data else []
    allowed = {
        (0, 2),
        (0, 3),
        (1, 2),
        (1, 4),
        (2, 3),
        (2, 6),
        (2, 7),
        (3, 2),
        (4, 2),
        (4, 3),
        (7, 2),
        (7, 4),
    }
    illegal = 0
    transitions_rows = []
    for version_date, pre, st, cnt in transition_rows:
        pre_i = int(pre)
        st_i = int(st)
        cnt_i = int(cnt)
        legal = (pre_i, st_i) in allowed
        if not legal:
            illegal += 1
        transitions_rows.append(
            [int(version_date), pre_i, st_i, cnt_i, "LEGAL" if legal else "ILLEGAL"]
        )

    details = []
    if not has_data:
        details.append(
            "no rows for filters: version_id=%s, version_date=%s, group_by IN (1,2)"
            % (args.version_id, args.version_date if args.version_date else "ANY")
        )
    details.append("day ranges:")
    if day_ranges:
        for version_date, min_day, max_day in day_ranges:
            details.append(
                "  version_date=%s, min_day=%s, max_day=%s"
                % (version_date, min_day, max_day)
            )
    else:
        details.append("  none")
    details.append("balance table:")
    details.extend(
        format_table(
            [
                "version_date",
                "status",
                "initial",
                "entries",
                "spawn",
                "exits",
                "final",
                "balance",
                "ok",
            ],
            balance_rows,
        )
    )
    details.append("transitions table:")
    details.extend(
        format_table(["version_date", "pre", "status", "count", "legal"], transitions_rows)
    )
    details.append(f"illegal_transitions={illegal}")

    passed = violations == 0 and illegal == 0
    print_result("INV-10 turnover balance", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
