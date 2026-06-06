#!/usr/bin/env python3
# ARCHIVED 2026-06-06 (P1 cleanup): устарел/дубликат, SSoT-замена: inv9_limiter_exit.py
"""
SQL-first валидация выходов из ops для limiter V8.

Проверяет инварианты:
1) transition 2→6 (storage): limiter == 0
2) transition 2→7 (unserviceable): limiter == 0
3) transition 2→6/2→7: ресурс исчерпан
4) (инфо) limiter=1 при фактическом исчерпании ресурса — возможная маскировка

Примечание: 2→3 (demote) исключён из проверок; limiter должен обнуляться принудительно.

Usage:
    python3 code/analysis/sim_validation_ops_exits.py --version-date 2025-07-04
"""

import argparse
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CODE_DIR = PROJECT_ROOT / "code"
if str(CODE_DIR) not in sys.path:
    sys.path.insert(0, str(CODE_DIR))

from utils.config_loader import get_clickhouse_client

TABLE_NAME = "sim_masterv2_v8"


def get_version_date_int(version_date_str: str) -> int:
    """Конвертирует YYYY-MM-DD в version_date (дни с 1970-01-01)"""
    dt = datetime.strptime(version_date_str, "%Y-%m-%d")
    return (dt - datetime(1970, 1, 1)).days


def run_scalar_check(client, title: str, query: str) -> Tuple[bool, int]:
    """Выполняет скалярный запрос и печатает PASS/FAIL."""
    rows = client.execute(query)
    if not rows or rows[0][0] is None:
        print(f"❌ {title}: нет результата запроса")
        return False, 0
    violations = int(rows[0][0])
    if violations == 0:
        print(f"✅ {title}: PASS (violations=0)")
        return True, violations
    print(f"❌ {title}: FAIL (violations={violations})")
    return False, violations


def print_masking_info(rows: List[Tuple[int, int]]) -> None:
    """Печатает возможные случаи маскировки limiter=1."""
    if not rows:
        print("ℹ️  Маскировка limiter=1: случаев не найдено")
        return
    print("ℹ️  Маскировка limiter=1 (potential):")
    for masked, day_u16 in rows:
        print(f"  day_u16={day_u16}: {masked}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Валидация выходов из ops для limiter V8")
    parser.add_argument("--version-date", required=True, help="Дата версии (YYYY-MM-DD)")
    args = parser.parse_args()

    version_date = get_version_date_int(args.version_date)
    client = get_clickhouse_client()

    print("\n" + "=" * 80)
    print("ВАЛИДАЦИЯ ВЫХОДОВ ИЗ OPS (limiter V8)")
    print(f"  Таблица: {TABLE_NAME}")
    print(f"  version_date: {args.version_date} (days={version_date})")
    print("=" * 80)

    failures = 0

    # Проверка 1: limiter == 0 при выходе 2→6 (storage)
    check_1 = f"""
        SELECT count() as violations
        FROM {TABLE_NAME}
        WHERE transition_2_to_6 = 1
          AND limiter > 0
          AND version_date = {version_date}
    """
    ok, _ = run_scalar_check(client, "1) limiter==0 при выходе 2→6 (storage)", check_1)
    if not ok:
        failures += 1

    # Проверка 2: limiter == 0 при выходе 2→7 (unserviceable)
    check_2 = f"""
        SELECT count() as violations
        FROM {TABLE_NAME}
        WHERE transition_2_to_7 = 1
          AND limiter > 0
          AND version_date = {version_date}
    """
    ok, _ = run_scalar_check(client, "2) limiter==0 при выходе 2→7 (unsvc)", check_2)
    if not ok:
        failures += 1

    # Проверка 3: ресурс исчерпан при выходе из ops (2→6 или 2→7)
    check_3 = f"""
        SELECT count() as violations
        FROM {TABLE_NAME}
        WHERE (transition_2_to_6 = 1 OR transition_2_to_7 = 1)
          AND NOT (sne + daily_next_u32 >= ll OR (ppr + daily_next_u32 >= oh))
          AND version_date = {version_date}
    """
    ok, _ = run_scalar_check(client, "3) ресурс исчерпан при выходе из ops", check_3)
    if not ok:
        failures += 1

    # Проверка 4 (инфо): потенциальная маскировка limiter=1
    check_4 = f"""
        SELECT count() as masked, day_u16
        FROM {TABLE_NAME}
        WHERE state = 'operations'
          AND limiter = 1
          AND (sne + daily_next_u32 >= ll OR ppr + daily_next_u32 >= oh)
          AND version_date = {version_date}
        GROUP BY day_u16
        ORDER BY day_u16
    """
    rows = client.execute(check_4)
    print_masking_info(rows)

    print("\n" + "-" * 80)
    if failures == 0:
        print("✅ ВАЛИДАЦИЯ ПРОЙДЕНА")
        return 0
    print(f"❌ ВАЛИДАЦИЯ НЕ ПРОЙДЕНА: {failures} проверок с нарушениями")
    return 1


if __name__ == "__main__":
    sys.exit(main())
