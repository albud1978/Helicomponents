#!/usr/bin/env python3
"""
TEMP-5: Claim metadata consistency (bank+claim semantics, sim_masterv2_v9).

Проверяет:
1) invalid_claim_rows: claim-события (commit_p2/commit_p3) имеют валидные поля claim.
2) transition_claim_mismatch: переход 4->2 сопровождается валидным claim в той же строке.
3) overlap_violations: для каждой линии интервалы [start, end) не пересекаются.
4) bank_underflow_suspicions: для claim_source=2 длина интервала равна repair_time.
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "TEMP-5: claim metadata consistency "
            "(bank+claim semantics, sim_masterv2_v9)"
        )
    )
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--version-date",
        type=int,
        default=None,
        help="version_date (YYYYMMDD) для фильтрации",
    )
    parser.add_argument("--table-main", default="sim_masterv2_v9")
    parser.add_argument("--table-repair", default="sim_repairline_v9")
    parser.add_argument("--repair-window", type=int, default=180)
    args = parser.parse_args()

    table_main = validate_table_name(args.table_main)
    table_repair = validate_table_name(args.table_repair)
    repair_window = int(args.repair_window)
    if repair_window <= 0:
        raise SystemExit("--repair-window должен быть > 0")
    _ = table_repair
    _ = repair_window

    client = get_client()

    vd_filter = ""
    params = {
        "vid": args.version_id,
    }
    if args.version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = args.version_date
    valid_claim_expr = (
        "repair_claim_source IN (1, 2)"
        " AND repair_claim_line_id != 65535"
        " AND repair_claim_start_day != 65535"
        " AND repair_claim_end_day != 65535"
        " AND repair_claim_end_day > repair_claim_start_day"
        " AND (toInt32(repair_claim_end_day) - toInt32(repair_claim_start_day))"
        " = toInt32(repair_time)"
    )
    length_match_expr = (
        "(toInt32(repair_claim_end_day) - toInt32(repair_claim_start_day))"
        " = toInt32(repair_time)"
    )

    base_cte = f"""
    WITH base AS (
        SELECT
            aircraft_number,
            group_by,
            version_date,
            day_u16,
            pre_status_id,
            status_id,
            commit_p2,
            commit_p3,
            repair_time,
            repair_claim_source,
            repair_claim_line_id,
            repair_claim_start_day,
            repair_claim_end_day
        FROM {table_main}
        WHERE version_id = %(vid)s{vd_filter}
          AND group_by IN (1, 2)
    ),
    claim_rows AS (
        SELECT
            *,
            ({valid_claim_expr}) AS is_valid_claim
        FROM base
        WHERE commit_p2 = 1 OR commit_p3 = 1
    ),
    transition_rows AS (
        SELECT
            *,
            ({valid_claim_expr}) AS is_valid_claim
        FROM base
        WHERE pre_status_id = 4 AND status_id = 2
          AND day_u16 >= repair_time
    ),
    valid_claims AS (
        SELECT
            aircraft_number,
            group_by,
            version_date,
            day_u16,
            repair_claim_line_id,
            repair_claim_start_day,
            repair_claim_end_day,
            row_number() OVER (
                PARTITION BY group_by, version_date, repair_claim_line_id
                ORDER BY repair_claim_start_day,
                         repair_claim_end_day,
                         day_u16,
                         aircraft_number
            ) AS claim_row_id
        FROM claim_rows
        WHERE is_valid_claim = 1
    ),
    valid_claims_with_prev AS (
        SELECT
            aircraft_number,
            group_by,
            version_date,
            day_u16,
            repair_claim_line_id,
            repair_claim_start_day,
            repair_claim_end_day,
            max(repair_claim_end_day) OVER (
                PARTITION BY group_by, version_date, repair_claim_line_id
                ORDER BY repair_claim_start_day,
                         repair_claim_end_day,
                         day_u16,
                         aircraft_number
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) AS prev_max_end
        FROM valid_claims
    )
    """

    counts_query = base_cte + f"""
    SELECT
        (SELECT count() FROM claim_rows) AS total_claim_events,
        (SELECT count() FROM claim_rows WHERE is_valid_claim = 1) AS total_valid_claim_events,
        (SELECT count() FROM claim_rows WHERE is_valid_claim = 0) AS invalid_claim_rows,
        (SELECT count() FROM transition_rows) AS total_transitions_4to2,
        (SELECT count() FROM transition_rows WHERE is_valid_claim = 0) AS transition_claim_mismatch,
        (
            SELECT count()
            FROM valid_claims_with_prev
            WHERE prev_max_end > repair_claim_start_day
        ) AS overlap_violations,
        (
            SELECT count()
            FROM base
            WHERE repair_claim_source = 2
              AND NOT ({length_match_expr})
        ) AS bank_underflow_suspicions
    """

    (
        total_claim_events,
        total_valid_claim_events,
        invalid_claim_rows,
        total_transitions_4to2,
        transition_claim_mismatch,
        overlap_violations,
        bank_underflow_suspicions,
    ) = client.execute(counts_query, params)[0]

    details = [
        f"table_main={table_main}",
        f"total_claim_events={total_claim_events}",
        f"total_valid_claim_events={total_valid_claim_events}",
        f"total_transitions_4to2={total_transitions_4to2}",
        f"invalid_claim_rows={invalid_claim_rows}",
        f"transition_claim_mismatch={transition_claim_mismatch}",
        f"overlap_violations={overlap_violations}",
        f"bank_underflow_suspicions={bank_underflow_suspicions}",
    ]

    if invalid_claim_rows:
        sample_query = base_cte + """
        SELECT
            group_by,
            day_u16,
            aircraft_number,
            commit_p2,
            commit_p3,
            repair_claim_source,
            repair_claim_line_id,
            repair_claim_start_day,
            repair_claim_end_day,
            repair_time,
            (toInt32(repair_claim_end_day) - toInt32(repair_claim_start_day)) AS len_calc
        FROM claim_rows
        WHERE is_valid_claim = 0
        ORDER BY day_u16, aircraft_number
        LIMIT 5
        """
        rows = client.execute(sample_query, params)
        details.append(
            "top5 invalid_claim_rows (gb, day, acn, c2, c3, src, line, start, end, rt, len):"
        )
        for row in rows:
            (
                gb,
                day,
                acn,
                c2,
                c3,
                src,
                line_id,
                start_day,
                end_day,
                repair_time,
                length_calc,
            ) = row
            details.append(
                "  gb={gb}, day={day}, acn={acn}, c2={c2}, c3={c3}, "
                "src={src}, line={line}, start={start}, end={end}, rt={rt}, "
                "len={ln}".format(
                    gb=gb,
                    day=day,
                    acn=acn,
                    c2=c2,
                    c3=c3,
                    src=src,
                    line=line_id,
                    start=start_day,
                    end=end_day,
                    rt=repair_time,
                    ln=length_calc,
                )
            )

    if transition_claim_mismatch:
        transition_sample_query = base_cte + """
        SELECT
            group_by,
            day_u16,
            aircraft_number,
            pre_status_id,
            status_id,
            commit_p2,
            commit_p3,
            repair_claim_source,
            repair_claim_line_id,
            repair_claim_start_day,
            repair_claim_end_day,
            repair_time
        FROM transition_rows
        WHERE is_valid_claim = 0
        ORDER BY day_u16, aircraft_number
        LIMIT 5
        """
        rows = client.execute(transition_sample_query, params)
        details.append(
            "top5 transition_mismatch (gb, day, acn, pre, status, c2, c3, src, line, start, end, rt):"
        )
        for row in rows:
            (
                gb,
                day,
                acn,
                pre_status,
                status,
                c2,
                c3,
                src,
                line_id,
                start_day,
                end_day,
                repair_time,
            ) = row
            details.append(
                "  gb={gb}, day={day}, acn={acn}, pre={pre}, status={status}, "
                "c2={c2}, c3={c3}, src={src}, line={line}, start={start}, "
                "end={end}, rt={rt}".format(
                    gb=gb,
                    day=day,
                    acn=acn,
                    pre=pre_status,
                    status=status,
                    c2=c2,
                    c3=c3,
                    src=src,
                    line=line_id,
                    start=start_day,
                    end=end_day,
                    rt=repair_time,
                )
            )

    if overlap_violations:
        overlap_sample_query = base_cte + """
        SELECT
            group_by,
            repair_claim_line_id,
            aircraft_number,
            day_u16,
            repair_claim_start_day,
            repair_claim_end_day,
            prev_max_end
        FROM valid_claims_with_prev
        WHERE prev_max_end > repair_claim_start_day
        ORDER BY group_by, repair_claim_line_id, repair_claim_start_day
        LIMIT 5
        """
        rows = client.execute(overlap_sample_query, params)
        details.append(
            "top5 overlap_rows (gb, line, acn, day, start, end, prev_max_end):"
        )
        for row in rows:
            (
                gb,
                line_id,
                acn,
                day,
                start_day,
                end_day,
                prev_max_end,
            ) = row
            details.append(
                "  gb={gb}, line={line}, acn={acn}, day={day}, "
                "start={start}, end={end}, prev_max_end={prev}".format(
                    gb=gb,
                    line=line_id,
                    acn=acn,
                    day=day,
                    start=start_day,
                    end=end_day,
                    prev=prev_max_end,
                )
            )

    if bank_underflow_suspicions:
        bank_sample_query = base_cte + f"""
        SELECT
            group_by,
            day_u16,
            aircraft_number,
            repair_claim_start_day,
            repair_claim_end_day,
            repair_time,
            (toInt32(repair_claim_end_day) - toInt32(repair_claim_start_day)) AS len_calc
        FROM base
        WHERE repair_claim_source = 2
          AND NOT ({length_match_expr})
        ORDER BY day_u16, aircraft_number
        LIMIT 5
        """
        rows = client.execute(bank_sample_query, params)
        details.append(
            "top5 bank_underflow (gb, day, acn, start, end, rt, len):"
        )
        for row in rows:
            gb, day, acn, start_day, end_day, repair_time, length_calc = row
            details.append(
                "  gb={gb}, day={day}, acn={acn}, start={start}, end={end}, "
                "rt={rt}, len={ln}".format(
                    gb=gb,
                    day=day,
                    acn=acn,
                    start=start_day,
                    end=end_day,
                    rt=repair_time,
                    ln=length_calc,
                )
            )

    passed = (
        invalid_claim_rows == 0
        and transition_claim_mismatch == 0
        and overlap_violations == 0
        and bank_underflow_suspicions == 0
    )
    print_result("TEMP-5 claim metadata", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
