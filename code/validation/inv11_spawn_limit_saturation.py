#!/usr/bin/env python3
"""
INV-11: Спавн упёрся в лимит, а post-warmup дефицит ops остаётся.

FAIL только если:
  - dynamic_spawned_mi17 >= dynamic_reserve_mi17 (saturated)
  - post_warmup дефицит ops для Mi-17 > 0
"""
import argparse
import re
import sys
from datetime import date as _date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from ch_client import get_client
from sim_env_setup import prepare_env_arrays


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


def to_date(version_date_int: int) -> _date:
    y = version_date_int // 10000
    m = (version_date_int % 10000) // 100
    d = version_date_int % 100
    try:
        return _date(y, m, d)
    except ValueError as exc:
        raise SystemExit(f"Некорректный version_date: {version_date_int}") from exc


def load_mp4_targets(client, version_date_int: int):
    """Возвращает targets_mi8, targets_mi17, start_date (как в INV-2)."""
    vdate = to_date(version_date_int)
    rows = client.execute(
        "SELECT dates, ops_counter_mi8, ops_counter_mi17 "
        "FROM flight_program_ac "
        "WHERE version_date = %(vd)s "
        "ORDER BY dates",
        {"vd": vdate},
    )
    if not rows:
        raise SystemExit(
            f"flight_program_ac пуст для version_date={vdate}. "
            "Невозможно получить таргеты MP4."
        )

    date_targets = {}
    for dt, mi8, mi17 in rows:
        date_targets[dt] = (int(mi8 or 0), int(mi17 or 0))

    sorted_dates = sorted(date_targets.keys())
    start_date = sorted_dates[0]
    targets_mi8 = {}
    targets_mi17 = {}
    for dt in sorted_dates:
        day_idx = (dt - start_date).days
        t8, t17 = date_targets[dt]
        targets_mi8[day_idx] = t8
        targets_mi17[day_idx] = t17

    return targets_mi8, targets_mi17, start_date


def get_target_for_day(targets_dict, day_u16):
    if day_u16 in targets_dict:
        return targets_dict[day_u16]
    prev_day = None
    for d in sorted(targets_dict.keys()):
        if d <= day_u16:
            prev_day = d
        else:
            break
    if prev_day is not None:
        return targets_dict[prev_day]
    first = min(targets_dict.keys())
    return targets_dict[first]


def run(client, version_id: int, version_date=None, table: str = "sim_masterv2_v9") -> bool:
    table = validate_table_name(table)

    params = {"vid": version_id}
    if version_date is not None:
        version_dates = [version_date]
    else:
        version_dates_query = f"""
        SELECT version_date
        FROM {table}
        WHERE version_id = %(vid)s
          AND group_by IN (1, 2)
        GROUP BY version_date
        ORDER BY version_date
        """
        version_dates = [
            int(row[0]) for row in client.execute(version_dates_query, params)
        ]
    if not version_dates:
        raise SystemExit("Не удалось определить version_date из данных симуляции")

    details = [f"version_id={version_id}, table={table}"]
    violating_datasets = 0

    for version_date_int in version_dates:
        scoped_params = {"vid": version_id, "vdate": version_date_int}
        vd_filter = " AND version_date = %(vdate)s"

        vdate_date = to_date(version_date_int)
        env_data = prepare_env_arrays(client, vdate_date)
        deterministic_spawn_mi17 = int(env_data.get("deterministic_spawn_mi17", 0))
        dynamic_reserve_mi17 = int(env_data.get("dynamic_reserve_mi17", 0))

        spawn_query = f"""
        SELECT version_date, countDistinct(idx)
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter}
          AND group_by = 2
          AND pre_status_id = 0
        GROUP BY version_date
        """
        spawn_rows = client.execute(spawn_query, scoped_params)
        total_spawned_mi17 = int(spawn_rows[0][1]) if spawn_rows else 0
        dynamic_spawned_mi17 = max(0, total_spawned_mi17 - deterministic_spawn_mi17)
        saturated = False
        if dynamic_reserve_mi17 > 0:
            saturated = dynamic_spawned_mi17 >= dynamic_reserve_mi17

        warmup_query = f"""
        SELECT max(repair_time)
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter}
          AND group_by IN (1, 2)
        """
        warmup_value = client.execute(warmup_query, scoped_params)[0][0]
        warmup_days = int(warmup_value) if warmup_value is not None else 0

        _, targets_mi17, _ = load_mp4_targets(client, version_date_int)

        step_days_query = f"""
        SELECT DISTINCT day_u16
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter}
          AND group_by IN (1, 2)
        ORDER BY day_u16
        """
        step_days = sorted(
            [int(row[0]) for row in client.execute(step_days_query, scoped_params)]
        )
        if not step_days:
            raise SystemExit(
                f"В таблице нет day_u16 для version_date={version_date_int}"
            )

        ops_query = f"""
        SELECT version_date, day_u16, count() AS ops_count
        FROM {table}
        WHERE version_id = %(vid)s{vd_filter}
          AND status_id = 2
          AND group_by = 2
        GROUP BY version_date, day_u16
        """
        ops_counts = {}
        for _version_date, day_u16, ops_count in client.execute(
            ops_query, scoped_params
        ):
            ops_counts[int(day_u16)] = int(ops_count)

        post_deficits = []
        for day in step_days:
            if day <= warmup_days:
                continue
            target = get_target_for_day(targets_mi17, day)
            ops_count = ops_counts.get(day, 0)
            if ops_count < target:
                deficit = int(target - ops_count)
                post_deficits.append((day, ops_count, target, deficit))

        post_deficit_days = len(post_deficits)
        max_post_deficit = max((d[3] for d in post_deficits), default=0)
        dataset_fails = saturated and post_deficit_days > 0
        if dataset_fails:
            violating_datasets += 1

        details.extend(
            [
                f"version_date={version_date_int}",
                f"  deterministic_spawn_mi17={deterministic_spawn_mi17}",
                f"  dynamic_reserve_mi17={dynamic_reserve_mi17}",
                f"  total_spawned_mi17={total_spawned_mi17}",
                f"  dynamic_spawned_mi17={dynamic_spawned_mi17}",
                f"  saturated={'true' if saturated else 'false'}",
                f"  warmup_days={warmup_days}",
                f"  post_deficit_days={post_deficit_days}",
                f"  max_post_deficit={max_post_deficit}",
            ]
        )
        if post_deficits:
            details.append("  post_deficit_sample (day, ops, target, deficit):")
            for day, ops, target, deficit in post_deficits[:5]:
                details.append(
                    f"    day={day}: ops={ops}, target={target}, deficit={deficit}"
                )
            if len(post_deficits) > 5:
                details.append(f"    ... and {len(post_deficits) - 5} more")
        else:
            details.append("  post_deficit_sample: none")

    details.append(f"violating_datasets={violating_datasets}")
    passed = violating_datasets == 0
    print_result("INV-11 spawn limit saturation", passed, details)
    return passed


def main() -> int:
    parser = argparse.ArgumentParser(
        description="INV-11: spawn limit saturation + post-warmup ops deficit"
    )
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument(
        "--version-date",
        type=int,
        default=None,
        help="version_date (YYYYMMDD) для фильтрации",
    )
    parser.add_argument(
        "--table",
        default="sim_masterv2_v9",
        help="Таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
    )
    args = parser.parse_args()
    client = get_client()
    passed = run(client, args.version_id, args.version_date, args.table)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
