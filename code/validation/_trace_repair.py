#!/usr/bin/env python3
"""Trace repair agents lifecycle for Dataset 2."""
import argparse
import datetime
import re

try:
    from ch_client import get_client
except ImportError:
    from .ch_client import get_client


def validate_table_name(table: str) -> str:
    if not re.match(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$", table):
        raise SystemExit(f"Некорректное имя таблицы: {table}")
    return table


def parse_int_list(value: str) -> list[int]:
    items = []
    for part in value.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            items.append(int(part))
        except ValueError as exc:
            raise SystemExit(f"Некорректное число в списке: {part}") from exc
    return items


def load_mp4_targets(client, version_date_int: int):
    y = version_date_int // 10000
    m = (version_date_int % 10000) // 100
    d = version_date_int % 100
    vdate = datetime.date(y, m, d)

    rows = client.execute(
        "SELECT dates, ops_counter_mi17 FROM flight_program_ac "
        "WHERE version_date = %(vd)s ORDER BY dates",
        {"vd": vdate},
    )
    if not rows:
        raise SystemExit(
            f"flight_program_ac пуст для version_date={vdate}. "
            "Невозможно получить таргеты MP4."
        )

    targets_by_day = {}
    sorted_dates = sorted(r[0] for r in rows)
    start_date = sorted_dates[0]
    for dt, t17 in rows:
        targets_by_day[(dt - start_date).days] = int(t17 or 0)
    sorted_keys = sorted(targets_by_day.keys())

    return targets_by_day, sorted_keys


def get_tgt(day_u16: int, targets_by_day: dict[int, int], sorted_keys: list[int]) -> int:
    prev = None
    for k in sorted_keys:
        if k <= day_u16:
            prev = k
        else:
            break
    return targets_by_day[prev] if prev is not None else targets_by_day[sorted_keys[0]]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Trace repair agents lifecycle.")
    parser.add_argument("--version-date", type=int, default=20251230)
    parser.add_argument("--group-by", type=int, default=2)
    parser.add_argument("--table", default="sim_masterv2_v9")
    parser.add_argument("--max-day", type=int, default=200)
    parser.add_argument(
        "--acns",
        default="22490,22484,22418,22216,22493,25413,22977,22419,22478,22497,22378",
    )
    parser.add_argument(
        "--key-days",
        default="0,1,2,19,33,61,92,93,102,116,122,153,167,180",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    table = validate_table_name(args.table)
    repair_acns = parse_int_list(args.acns)
    key_days = parse_int_list(args.key_days)

    client = get_client()

    print(f"Dataset {args.version_date} Mi-17 repair agents lifecycle (day 0..{args.max_day}):")
    for a in repair_acns:
        rows = client.execute(
            "SELECT day_u16, status_id, pre_status_id, repair_days "
            f"FROM {table} "
            "WHERE version_date = %(vd)s AND aircraft_number = %(acn)s AND group_by = %(gb)s "
            "  AND day_u16 <= %(max_day)s "
            "ORDER BY day_u16",
            {"acn": a, "vd": args.version_date, "gb": args.group_by, "max_day": args.max_day},
        )
        prev_st = None
        transitions = []
        for day, st, pre, rd in rows:
            st_i, pre_i, rd_i = int(st), int(pre), int(rd)
            if prev_st is not None and st_i != prev_st:
                transitions.append(f"day={int(day):3d}: {prev_st}->{st_i} (rd={rd_i})")
            prev_st = st_i
        day0_rd = int(rows[0][3]) if rows else "?"
        day0_st = int(rows[0][1]) if rows else "?"
        print(f"  acn={a}: day0 st={day0_st} rd={day0_rd}")
        for t in transitions:
            print(f"    {t}")
        if not transitions:
            print(f"    (no transitions in 0..{args.max_day})")

    # Also show status breakdown at key warmup days
    print(f"\nDataset {args.version_date} Mi-17 status breakdown at key warmup days:")
    print(
        f"{'day':>5s} {'ops':>4s} {'svc':>4s} {'rep':>4s} "
        f"{'unsvc':>5s} {'inact':>5s} {'stor':>5s} | target diff"
    )

    targets_by_day, sorted_keys = load_mp4_targets(client, args.version_date)

    for dd in key_days:
        row = client.execute(
            "SELECT "
            "  countIf(status_id=2) AS ops, "
            "  countIf(status_id=3) AS svc, "
            "  countIf(status_id=4) AS rep, "
            "  countIf(status_id=7) AS unsvc, "
            "  countIf(status_id=1) AS inact, "
            "  countIf(status_id=6) AS stor "
            f"FROM {table} "
            "WHERE version_date=%(vd)s AND group_by=%(gb)s AND day_u16=%(d)s",
            {"d": dd, "vd": args.version_date, "gb": args.group_by},
        )
        if row:
            ops, svc, rep, unsvc, inact, stor = [int(x) for x in row[0]]
            tgt = get_tgt(dd, targets_by_day, sorted_keys)
            diff = ops - tgt
            flag = " <<<" if diff != 0 else ""
            print(
                f"{dd:5d} {ops:4d} {svc:4d} {rep:4d} {unsvc:5d} "
                f"{inact:5d} {stor:5d} | {tgt:4d} {diff:+3d}{flag}"
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
