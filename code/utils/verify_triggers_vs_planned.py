#!/usr/bin/env python3
from __future__ import annotations
import os
import re
import sys

sys.path.append(os.path.join(os.path.dirname(__file__)))
from config_loader import get_clickhouse_client  # type: ignore


def parse_planned(log_path: str) -> list[tuple[int, int, int]]:
    planned: list[tuple[int, int, int]] = []
    pat = re.compile(r"planned_triggers: ac=(\d+), part_day=(\d+), asm_flag_day=(\d+)")
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            m = pat.search(line)
            if m:
                ac = int(m.group(1))
                pd = int(m.group(2))
                ad = int(m.group(3))
                planned.append((ac, pd, ad))
    return planned


def main() -> None:
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    log_path = os.path.join(root, "logs", "status246_365_planned.log")
    if not os.path.isfile(log_path):
        print(f"лог не найден: {log_path}")
        sys.exit(1)

    planned = parse_planned(log_path)
    if not planned:
        print("planned_triggers не найдены в логе")
        sys.exit(2)

    acs = sorted({ac for ac, _, _ in planned})
    in_list = ",".join(str(a) for a in acs)

    client = get_clickhouse_client()
    rows = client.execute(
        f"""
        SELECT aircraft_number, day_abs, partout_trigger, assembly_trigger
        FROM default.sim_results
        WHERE aircraft_number IN ({in_list})
          AND (partout_trigger=1 OR assembly_trigger=1)
        """
    )
    trg = {(int(r[0]), int(r[1])): (int(r[2]), int(r[3])) for r in rows}

    part_ok = 0
    asm_ok = 0
    part_miss: list[tuple[int, int]] = []
    asm_miss: list[tuple[int, int]] = []

    for ac, pd, ad in planned:
        if trg.get((ac, pd), (0, 0))[0] == 1:
            part_ok += 1
        else:
            part_miss.append((ac, pd))
        if trg.get((ac, ad), (0, 0))[1] == 1:
            asm_ok += 1
        else:
            asm_miss.append((ac, ad))

    print(f"planned_count= {len(planned)}")
    print(f"partout_ok= {part_ok} miss= {len(part_miss)}")
    print(f"assembly_ok= {asm_ok} miss= {len(asm_miss)}")
    if part_miss:
        print("partout_miss_samples:")
        for ac, d in part_miss[:10]:
            print(f"  ac={ac} day_abs={d}")
    if asm_miss:
        print("assembly_miss_samples:")
        for ac, d in asm_miss[:10]:
            print(f"  ac={ac} day_abs={d}")


if __name__ == "__main__":
    main()


