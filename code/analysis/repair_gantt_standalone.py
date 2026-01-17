#!/usr/bin/env python3
"""
Standalone prototype: Repair Gantt scheduler (GPU-agnostic logic).

Key idea:
- repair_gantt[day, slot] stores aircraft_number (0 = free).
- On each day we can compute available contiguous free windows
  of length repair_time that are fully in the past (<= current_day).
- A reservation picks the earliest available window (day, slot)
  and writes aircraft_number into all days of that window.

Notes:
- Uses integer types only (no Float64).
- Does not read external data or run tests.
"""

from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

import argparse
from datetime import date
from pathlib import Path
import sys


@dataclass(frozen=True)
class Window:
    start_day: int
    slot: int


class RepairGanttScheduler:
    """
    Gantt-like repair scheduler with a day x slot matrix.

    Matrix layout:
        index = day * capacity + slot
        value = 0 (free) or aircraft_number (>0)
    """

    def __init__(self, days_total: int, capacity: int, repair_time: int) -> None:
        if days_total <= 0:
            raise ValueError("days_total must be > 0")
        if capacity <= 0:
            raise ValueError("capacity must be > 0")
        if repair_time <= 0:
            raise ValueError("repair_time must be > 0")
        if repair_time > days_total:
            raise ValueError("repair_time cannot exceed days_total")

        self.days_total = int(days_total)
        self.capacity = int(capacity)
        self.repair_time = int(repair_time)
        self._matrix: List[int] = [0] * (self.days_total * self.capacity)

    def _idx(self, day: int, slot: int) -> int:
        return day * self.capacity + slot

    def _is_free_window(self, start_day: int, slot: int, repair_days: int) -> bool:
        end_day = start_day + repair_days
        for d in range(start_day, end_day):
            if self._matrix[self._idx(d, slot)] != 0:
                return False
        return True

    def count_free_slots(self, current_day: int, repair_days: int) -> int:
        """
        Count free slots which have at least one window of length repair_days
        fully in [0..current_day]. The result is capped by capacity.
        """
        if current_day < 0:
            raise ValueError("current_day must be >= 0")
        if repair_days <= 0:
            raise ValueError("repair_days must be > 0")
        current_day = min(current_day, self.days_total - 1)
        max_start = current_day - repair_days + 1
        if max_start < 0:
            return 0

        count = 0
        for slot in range(self.capacity):
            for start_day in range(0, max_start + 1):
                if self._is_free_window(start_day, slot, repair_days):
                    count += 1
                    break
        return count

    def find_earliest_window(self, current_day: int, repair_days: int) -> Optional[Window]:
        """
        Find the earliest available window (by start_day, then slot)
        fully in the past (<= current_day).
        """
        if current_day < 0:
            raise ValueError("current_day must be >= 0")
        if repair_days <= 0:
            raise ValueError("repair_days must be > 0")
        current_day = min(current_day, self.days_total - 1)
        max_start = current_day - repair_days + 1
        if max_start < 0:
            return None

        for start_day in range(0, max_start + 1):
            for slot in range(self.capacity):
                if self._is_free_window(start_day, slot, repair_days):
                    return Window(start_day=start_day, slot=slot)
        return None

    def reserve(self, aircraft_number: int, current_day: int, repair_days: int) -> Optional[Window]:
        """
        Reserve the earliest available window (repair_days) for aircraft_number.
        Returns the reserved window or None if not available.
        """
        if aircraft_number <= 0:
            raise ValueError("aircraft_number must be > 0")
        if repair_days <= 0:
            raise ValueError("repair_days must be > 0")

        window = self.find_earliest_window(current_day, repair_days)
        if window is None:
            return None

        for d in range(window.start_day, window.start_day + repair_days):
            self._matrix[self._idx(d, window.slot)] = int(aircraft_number)
        return window

    def reserve_many(
        self, aircraft_numbers: Iterable[int], current_day: int, repair_days: int
    ) -> List[Tuple[int, Optional[Window]]]:
        """
        Reserve windows for multiple aircraft numbers in ascending order.
        Returns list of (aircraft_number, window or None).
        """
        results: List[Tuple[int, Optional[Window]]] = []
        for acn in sorted(aircraft_numbers):
            results.append((acn, self.reserve(acn, current_day, repair_days)))
        return results

    def drain_matrix(self) -> List[int]:
        """
        Returns a copy of the full matrix for external export.
        """
        return list(self._matrix)


def _load_env_params(version_date: str) -> Tuple[int, int]:
    """
    Load days_total and repair_time from ClickHouse via env arrays.
    """
    repo_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(repo_root / "code"))
    sys.path.append(str(repo_root / "code" / "utils"))

    from config_loader import get_clickhouse_client
    from sim_env_setup import prepare_env_arrays

    client = get_clickhouse_client()
    env = prepare_env_arrays(client, date.fromisoformat(version_date))

    if "days_total_u16" not in env:
        raise RuntimeError("days_total_u16 not found in env_data")
    if "mi17_repair_time_const" not in env:
        raise RuntimeError("mi17_repair_time_const not found in env_data")

    days_total = int(env["days_total_u16"])
    repair_time = int(env["mi17_repair_time_const"])

    if days_total <= 0 or repair_time <= 0:
        raise RuntimeError("Invalid env params: days_total and repair_time must be > 0")

    return days_total, repair_time


def _load_unsvc_aircraft_numbers(version_date: str, table: str, day: int, group_by: int) -> List[int]:
    """
    Load unserviceable aircraft numbers from simulation table for given day.
    """
    repo_root = Path(__file__).resolve().parents[2]
    sys.path.append(str(repo_root / "code"))
    sys.path.append(str(repo_root / "code" / "utils"))

    from config_loader import get_clickhouse_client

    client = get_clickhouse_client()
    version_date_int = int(version_date.replace("-", ""))

    rows = client.execute(
        f"""
        SELECT aircraft_number
        FROM {table}
        WHERE version_date = {version_date_int}
          AND day_u16 = {int(day)}
          AND group_by = {int(group_by)}
          AND state = 'unserviceable'
        ORDER BY aircraft_number
        """
    )
    return [int(r[0]) for r in rows]


def main() -> None:
    parser = argparse.ArgumentParser(description="Repair Gantt standalone test (real data only)")
    parser.add_argument("--version-date", required=True, help="Dataset date (YYYY-MM-DD)")
    parser.add_argument("--day", type=int, required=True, help="Current day for capacity check")
    parser.add_argument("--table", default="sim_masterv2_v8", help="Source table for unserviceable list")
    parser.add_argument("--group-by", type=int, default=2, help="Aircraft group_by (default 2 = Mi-17)")
    parser.add_argument("--repair-quota", type=int, required=True, help="Daily repair capacity (slots)")
    args = parser.parse_args()

    if args.repair_quota <= 0:
        raise RuntimeError("repair_quota must be > 0")

    days_total, repair_time = _load_env_params(args.version_date)
    if args.day < 0 or args.day >= days_total:
        raise RuntimeError("day out of range for days_total")

    unsvc_aircraft = _load_unsvc_aircraft_numbers(
        args.version_date, args.table, args.day, args.group_by
    )

    scheduler = RepairGanttScheduler(days_total, args.repair_quota, repair_time)
    free_before = scheduler.count_free_slots(args.day, repair_time)
    reservations = scheduler.reserve_many(unsvc_aircraft, args.day, repair_time)
    reserved_count = sum(1 for _, w in reservations if w is not None)
    free_after = scheduler.count_free_slots(args.day, repair_time)

    print("\n=== Repair Gantt Standalone Test ===")
    print(f"version_date: {args.version_date}, day: {args.day}, group_by: {args.group_by}")
    print(f"days_total: {days_total}, repair_capacity: {args.repair_quota}, repair_time: {repair_time}")
    print(f"unserviceable aircraft: {len(unsvc_aircraft)}")
    print(f"free_windows_before: {free_before}")
    print(f"reserved: {reserved_count}")
    print(f"free_windows_after: {free_after}")


if __name__ == "__main__":
    main()

