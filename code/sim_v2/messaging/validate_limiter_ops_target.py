#!/usr/bin/env python3
"""
Валидация LIMITER: ops == target на каждом адаптивном шаге.

Проверяет:
1) Количество в operations строго соответствует программе (MP4) для Mi-8 и Mi-17
2) Проверка выполняется по ВСЕМ дням, которые присутствуют в таблице лимитера

Правило target: safe_day = day + 1 (как в baseline), с ограничением по максимальному дню программы.
"""
import os
import sys
import argparse
from datetime import date

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_CODE_DIR = os.path.join(_SCRIPT_DIR, '..', '..')
sys.path.insert(0, _CODE_DIR)

from utils.config_loader import get_clickhouse_client


def load_targets(client, version_date: str) -> dict:
    """Загружает target (ops_counter) по дням."""
    query = f"""
    SELECT
        dateDiff('day', toDate('{version_date}'), dates) AS day_u16,
        ops_counter_mi8,
        ops_counter_mi17
    FROM flight_program_ac
    WHERE version_date = toDate('{version_date}')
    ORDER BY day_u16
    """
    rows = client.execute(query)
    target = {}
    for day_u16, mi8, mi17 in rows:
        target[int(day_u16)] = (int(mi8), int(mi17))
    return target


def main() -> int:
    parser = argparse.ArgumentParser(description="LIMITER ops vs target (adaptive steps)")
    parser.add_argument("--version-date", default="2025-07-04", help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--table", default="sim_masterv2_v8", help="Таблица лимитера")
    args = parser.parse_args()

    client = get_clickhouse_client()
    vd_int = int(args.version_date.replace("-", ""))

    # Максимальный день из симуляции
    max_day = client.execute(
        f"SELECT max(day_u16) FROM {args.table} WHERE version_date = {vd_int}"
    )[0][0]
    if max_day is None:
        print("❌ Таблица пуста или версия не найдена.")
        return 1
    max_day = int(max_day)

    # Цели из программы
    target_dict = load_targets(client, args.version_date)
    if not target_dict:
        print("❌ Не найдено данных программы (flight_program_ac).")
        return 1
    max_target_day = max(target_dict.keys())

    # Количество в operations по дням
    ops_by_day = client.execute(f"""
        SELECT
            day_u16,
            countIf(state = 'operations' AND group_by = 1) AS mi8_ops,
            countIf(state = 'operations' AND group_by = 2) AS mi17_ops
        FROM {args.table}
        WHERE version_date = {vd_int}
        GROUP BY day_u16
        ORDER BY day_u16
    """)

    if not ops_by_day:
        print("❌ Нет данных ops в симуляции.")
        return 1

    print("=" * 80)
    print("VALIDATION: ops == target (adaptive steps)")
    print(f"Table: {args.table}")
    print(f"Version: {args.version_date}")
    print(f"Days: {ops_by_day[0][0]} .. {ops_by_day[-1][0]}")
    print("=" * 80)

    mismatches = 0
    total_days = 0
    mi8_mismatches = 0
    mi17_mismatches = 0
    sample_bad = []

    print(f"\n{'Day':>6} | {'Mi-8 ops':>8} | {'Mi-8 tgt':>8} | {'Δ':>5} | "
          f"{'Mi-17 ops':>9} | {'Mi-17 tgt':>9} | {'Δ':>5}")
    print("-" * 80)

    for idx, (day, mi8_ops, mi17_ops) in enumerate(ops_by_day):
        day = int(day)
        total_days += 1

        safe_day = day + 1
        if safe_day > max_target_day:
            safe_day = max_target_day

        mi8_tgt, mi17_tgt = target_dict.get(safe_day, (0, 0))
        d8 = int(mi8_ops) - mi8_tgt
        d17 = int(mi17_ops) - mi17_tgt

        if d8 != 0 or d17 != 0:
            mismatches += 1
            if d8 != 0:
                mi8_mismatches += 1
            if d17 != 0:
                mi17_mismatches += 1
            if len(sample_bad) < 50:
                sample_bad.append((day, int(mi8_ops), mi8_tgt, d8, int(mi17_ops), mi17_tgt, d17))

        if idx < 20 or idx >= len(ops_by_day) - 5:
            print(f"{day:>6} | {int(mi8_ops):>8} | {mi8_tgt:>8} | {d8:>+5} | "
                  f"{int(mi17_ops):>9} | {mi17_tgt:>9} | {d17:>+5}")
        elif idx == 20:
            print("  ...")

    print("-" * 80)
    print(f"Всего шагов: {total_days}")
    print(f"Несовпадений: {mismatches}")
    print(f"  Mi-8 mismatch days: {mi8_mismatches}")
    print(f"  Mi-17 mismatch days: {mi17_mismatches}")

    if sample_bad:
        print("\nПримеры нарушений (до 50):")
        print(f"{'Day':>6} | {'Mi-8 ops':>8} | {'Mi-8 tgt':>8} | {'Δ':>5} | "
              f"{'Mi-17 ops':>9} | {'Mi-17 tgt':>9} | {'Δ':>5}")
        print("-" * 80)
        for row in sample_bad:
            day, mi8_ops, mi8_tgt, d8, mi17_ops, mi17_tgt, d17 = row
            print(f"{day:>6} | {mi8_ops:>8} | {mi8_tgt:>8} | {d8:>+5} | "
                  f"{mi17_ops:>9} | {mi17_tgt:>9} | {d17:>+5}")

    if mismatches == 0:
        print("\n✅ OK: ops == target на всех адаптивных шагах")
        return 0

    print("\n❌ FAIL: ops != target на некоторых шагах")
    return 1


if __name__ == "__main__":
    sys.exit(main())

