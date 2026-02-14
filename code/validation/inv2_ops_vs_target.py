#!/usr/bin/env python3
"""
INV-2: ops count = target (±tolerance) после warmup.

Таргеты берутся ДИНАМИЧЕСКИ из flight_program_ac (MP4) — те же данные,
что использует симуляция через mp4_ops_counter_mi8 / mp4_ops_counter_mi17.
"""
import argparse
import re
import sys
from datetime import date as _date, timedelta

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


def load_mp4_targets(client, version_date_int: int):
    """Загружает per-day таргеты из flight_program_ac.

    Возвращает два словаря: {day_index: target} для Mi-8 и Mi-17,
    где day_index — порядковый номер дня от start_date (0-based).
    """
    # version_date_int = YYYYMMDD, преобразуем в date
    y = version_date_int // 10000
    m = (version_date_int % 10000) // 100
    d = version_date_int % 100
    vdate = _date(y, m, d)

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

    # Строим маппинг date → (mi8_target, mi17_target)
    date_targets = {}
    for dt, mi8, mi17 in rows:
        date_targets[dt] = (int(mi8 or 0), int(mi17 or 0))

    # Симуляция использует days_sorted = sorted(union(mp4_dates, mp5_dates)).
    # Здесь используем только MP4 даты (порядок совпадает при union с MP5,
    # т.к. для пропущенных дней MP4 target остаётся прежним).
    # Строим day_index → target, используя start_date как day 0.
    sorted_dates = sorted(date_targets.keys())
    start_date = sorted_dates[0]

    targets_mi8 = {}  # day_u16 → target
    targets_mi17 = {}
    for dt in sorted_dates:
        day_idx = (dt - start_date).days
        t8, t17 = date_targets[dt]
        targets_mi8[day_idx] = t8
        targets_mi17[day_idx] = t17

    return targets_mi8, targets_mi17, start_date


def get_target_for_day(targets_dict, day_u16):
    """Возвращает таргет для конкретного day_u16.

    Если day_u16 точно есть в словаре — возвращаем его.
    Иначе — возвращаем таргет от ближайшего предыдущего дня (step-function).
    """
    if day_u16 in targets_dict:
        return targets_dict[day_u16]
    # Найти ближайший предыдущий
    prev_day = None
    for d in sorted(targets_dict.keys()):
        if d <= day_u16:
            prev_day = d
        else:
            break
    if prev_day is not None:
        return targets_dict[prev_day]
    # Если день раньше первого — вернуть первый
    first = min(targets_dict.keys())
    return targets_dict[first]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="INV-2: ops count = target (±tolerance) после warmup. "
                    "Таргеты из flight_program_ac (MP4)."
    )
    parser.add_argument("--version-id", required=True, type=int, help="version_id")
    parser.add_argument("--version-date", type=int, default=None,
                        help="version_date (YYYYMMDD) для фильтрации")
    parser.add_argument(
        "--table",
        default="sim_masterv2_v9",
        help="Таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
    )
    parser.add_argument(
        "--tolerance",
        type=int,
        default=0,
        help="Допуск по ops (по умолчанию: 0)",
    )
    args = parser.parse_args()
    table = validate_table_name(args.table)
    client = get_client()

    # Базовый фильтр
    vd_filter = ""
    params = {"vid": args.version_id}
    if args.version_date is not None:
        vd_filter = " AND version_date = %(vdate)s"
        params["vdate"] = args.version_date

    # Загружаем динамические таргеты из MP4
    # version_date для MP4 = start_date симуляции
    if args.version_date is not None:
        mp4_vd = args.version_date
    else:
        # Пытаемся определить из данных симуляции
        row = client.execute(
            f"SELECT min(version_date) FROM {table} WHERE version_id = %(vid)s",
            params,
        )
        mp4_vd = int(row[0][0]) if row and row[0][0] else None
        if mp4_vd is None:
            raise SystemExit("Не удалось определить version_date из данных симуляции")

    targets_mi8, targets_mi17, start_date = load_mp4_targets(client, mp4_vd)
    targets = {1: targets_mi8, 2: targets_mi17}

    print(f"MP4 targets loaded: Mi-8 unique={len(set(targets_mi8.values()))}, "
          f"Mi-17 unique={len(set(targets_mi17.values()))}")

    # Warmup = max(repair_time) из данных
    warmup_query = f"""
    SELECT max(repair_time)
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
    """
    warmup_value = client.execute(warmup_query, params)[0][0]
    warmup_days = int(warmup_value) if warmup_value is not None else 0

    # Все шаги симуляции
    step_days_query = f"""
    SELECT DISTINCT day_u16
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter} AND group_by IN (1, 2)
    ORDER BY day_u16
    """
    step_days = sorted(
        [int(row[0]) for row in client.execute(step_days_query, params)]
    )

    # Фактический ops count по дням и группам
    ops_query = f"""
    SELECT day_u16, group_by, count() AS ops_count
    FROM {table}
    WHERE version_id = %(vid)s{vd_filter} AND status_id = 2 AND group_by IN (1, 2)
    GROUP BY day_u16, group_by
    """
    ops_counts = {}
    for day_u16, group_by, ops_count in client.execute(ops_query, params):
        ops_counts[(int(group_by), int(day_u16))] = int(ops_count)

    warmup_violations = []
    post_violations = []
    group_names = {1: "Mi-8", 2: "Mi-17"}

    for group_by in (1, 2):
        target_dict = targets[group_by]
        for day in step_days:
            target = get_target_for_day(target_dict, day)
            ops_count = ops_counts.get((group_by, day), 0)
            diff = ops_count - target
            entry = (group_by, day, ops_count, target, diff)
            if day <= warmup_days:
                if abs(diff) > args.tolerance:
                    warmup_violations.append(entry)
            else:
                if abs(diff) > args.tolerance:
                    post_violations.append(entry)

    # --- Warmup report (информационный, не влияет на PASS/FAIL) ---
    warmup_steps = sum(1 for d in step_days if d <= warmup_days)
    post_steps = sum(1 for d in step_days if d > warmup_days)

    print("-" * 80)
    print(f"Warmup period (day <= {warmup_days}): {warmup_steps} steps, "
          f"{len(warmup_violations)} deviations (INFO, not counted)")
    if warmup_violations:
        for gb in (1, 2):
            wv = [v for v in warmup_violations if v[0] == gb]
            if wv:
                max_dev = max(abs(v[4]) for v in wv)
                print(f"  {group_names[gb]}: {len(wv)} deviations, max |diff|={max_dev}")
                for _, day, ops, tgt, diff in wv[:5]:
                    sign = "+" if diff > 0 else ""
                    print(f"    day={day}: ops={ops}, target={tgt}, diff={sign}{diff}")
                if len(wv) > 5:
                    print(f"    ... and {len(wv) - 5} more")
    print("-" * 80)

    # --- Post-warmup: главный результат ---
    passed = not post_violations
    details = [
        f"warmup_days={warmup_days}",
        f"tolerance={args.tolerance}",
        f"post_warmup_steps={post_steps}",
        f"Mi-8 target range: {min(targets_mi8.values())}..{max(targets_mi8.values())}",
        f"Mi-17 target range: {min(targets_mi17.values())}..{max(targets_mi17.values())}",
        f"warmup_deviations={len(warmup_violations)} (info only)",
    ]
    details.append(f"post_warmup_violations={len(post_violations)}")

    if post_violations:
        v_mi8 = [v for v in post_violations if v[0] == 1]
        v_mi17 = [v for v in post_violations if v[0] == 2]
        details.append(f"  Mi-8 violations: {len(v_mi8)}")
        details.append(f"  Mi-17 violations: {len(v_mi17)}")

        for gb, vlist in [(1, v_mi8), (2, v_mi17)]:
            if vlist:
                sample = vlist[:5]
                details.append(f"  {group_names[gb]} sample (day, ops, target, diff):")
                for _, day, ops, tgt, diff in sample:
                    sign = "+" if diff > 0 else ""
                    details.append(f"    day={day}: ops={ops}, target={tgt}, diff={sign}{diff}")

    print_result("INV-2 ops vs target (dynamic MP4)", passed, details)
    return 0 if passed else 1


if __name__ == "__main__":
    sys.exit(main())
