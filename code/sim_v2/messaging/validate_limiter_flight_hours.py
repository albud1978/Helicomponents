#!/usr/bin/env python3
"""
Валидация LIMITER: налёт и инварианты dt через delta_sne.

Проверяет:
1) Σ(delta_sne) по ops периодам == Δsne всего флота
2) Для prev_state != operations: delta_sne == 0 (нет налёта вне ops)
3) Для prev_state == operations:
   - delta_sne == sum(program) за интервал
   - если sum(program) == 0 (зимовка), то delta_sne == 0
"""
import os
import sys
import argparse

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_CODE_DIR = os.path.join(_SCRIPT_DIR, '..', '..')
sys.path.insert(0, _CODE_DIR)

from utils.config_loader import get_clickhouse_client


def main() -> int:
    parser = argparse.ArgumentParser(description="LIMITER flight hours validation (delta_sne)")
    parser.add_argument("--version-date", default="2025-07-04", help="Дата версии (YYYY-MM-DD)")
    parser.add_argument("--table", default="sim_masterv2_v8", help="Таблица лимитера")
    args = parser.parse_args()

    client = get_clickhouse_client()
    vd_int = int(args.version_date.replace("-", ""))

    # Глобальные дни
    min_max = client.execute(
        f"SELECT min(day_u16), max(day_u16) FROM {args.table} WHERE version_date = {vd_int}"
    )[0]
    min_day, max_day = min_max
    if min_day is None or max_day is None:
        print("❌ Таблица пуста или версия не найдена.")
        return 1
    min_day = int(min_day)
    max_day = int(max_day)

    # Δsne всего флота
    sne_start = client.execute(
        f"SELECT sum(sne) FROM {args.table} WHERE version_date = {vd_int} AND day_u16 = {min_day}"
    )[0][0] or 0
    sne_end = client.execute(
        f"SELECT sum(sne) FROM {args.table} WHERE version_date = {vd_int} AND day_u16 = {max_day}"
    )[0][0] or 0
    delta_sne_total = int(sne_end) - int(sne_start)

    # Общий CTE для вычисления program sum через arrayJoin (без range JOIN)
    prog_cte = f"""
    WITH sim AS (
        SELECT
            aircraft_number,
            day_u16,
            lagInFrame(day_u16) OVER (PARTITION BY aircraft_number ORDER BY day_u16) AS prev_day,
            lagInFrame(state) OVER (PARTITION BY aircraft_number ORDER BY day_u16) AS prev_state,
            lagInFrame(sne) OVER (PARTITION BY aircraft_number ORDER BY day_u16) AS prev_sne,
            sne
        FROM {args.table}
        WHERE version_date = {vd_int}
    ),
    intervals AS (
        SELECT
            aircraft_number,
            prev_day,
            day_u16,
            prev_state,
            toInt64(sne) - toInt64(prev_sne) AS delta_sne
        FROM sim
        WHERE prev_day IS NOT NULL
    ),
    expanded AS (
        SELECT
            aircraft_number,
            prev_day,
            day_u16,
            prev_state,
            delta_sne,
            arrayJoin(range(prev_day, day_u16)) AS day_i
        FROM intervals
    ),
    prog AS (
        SELECT
            e.aircraft_number,
            e.prev_day,
            e.day_u16,
            e.prev_state,
            e.delta_sne,
            sum(toUInt32(f.daily_hours)) AS prog_sum
        FROM expanded e
        LEFT JOIN flight_program_fl f
            ON f.aircraft_number = e.aircraft_number
           AND f.version_date = toDate('{args.version_date}')
           AND dateDiff('day', toDate('{args.version_date}'), f.dates) = e.day_i
        GROUP BY e.aircraft_number, e.prev_day, e.day_u16, e.prev_state, e.delta_sne
    )
    """

    summary = client.execute(f"""
    {prog_cte}
    SELECT
        sum(delta_sne) AS total_delta_sne,
        sumIf(delta_sne, prev_state = 'operations') AS ops_delta_sne,
        sumIf(delta_sne, prev_state != 'operations') AS non_ops_delta_sne,
        sumIf(prog_sum, prev_state = 'operations') AS ops_prog_sum,
        countIf(prev_state != 'operations' AND delta_sne != 0) AS non_ops_with_delta,
        countIf(prev_state = 'operations' AND delta_sne != prog_sum) AS ops_mismatch_prog,
        countIf(prev_state = 'operations' AND prog_sum = 0 AND delta_sne != 0) AS ops_zero_prog_with_delta,
        countIf(prev_state = 'operations' AND prog_sum > 0 AND delta_sne = 0) AS ops_prog_positive_delta_zero
    FROM prog
    """)[0]

    (total_delta_sne, ops_delta_sne, non_ops_delta_sne, ops_prog_sum,
     non_ops_with_delta, ops_mismatch_prog, ops_zero_prog_with_delta,
     ops_prog_positive_delta_zero) = summary

    print("=" * 80)
    print("VALIDATION: flight hours via delta_sne")
    print(f"Table: {args.table}")
    print(f"Version: {args.version_date}")
    print(f"Days: {min_day} .. {max_day}")
    print("=" * 80)

    print(f"Σsne(day {min_day}) = {int(sne_start):,}")
    print(f"Σsne(day {max_day}) = {int(sne_end):,}")
    print(f"Δsne total          = {int(delta_sne_total):,}")
    print("-" * 80)
    print(f"Σ delta_sne (all intervals)         = {int(total_delta_sne):,}")
    print(f"Σ delta_sne (ops intervals)         = {int(ops_delta_sne):,}")
    print(f"Σ delta_sne (non-ops intervals)     = {int(non_ops_delta_sne):,}")
    print(f"Σ program (ops intervals)           = {int(ops_prog_sum):,}")
    print("-" * 80)
    print(f"non-ops with delta_sne != 0         = {int(non_ops_with_delta)}")
    print(f"ops delta_sne != program            = {int(ops_mismatch_prog)}")
    print(f"ops program=0 but delta_sne>0        = {int(ops_zero_prog_with_delta)}")
    print(f"ops program>0 but delta_sne=0        = {int(ops_prog_positive_delta_zero)}")

    ok = True
    if int(total_delta_sne) != int(delta_sne_total):
        print("\n❌ Δsne total != Σ delta_sne (all intervals)")
        ok = False
    if int(ops_delta_sne) != int(delta_sne_total):
        print("❌ Δsne total != Σ delta_sne (ops intervals)")
        ok = False
    if int(non_ops_delta_sne) != 0:
        print("❌ Найден налёт вне операций (non-ops delta_sne != 0)")
        ok = False
    if int(ops_mismatch_prog) != 0:
        print("❌ Найдены интервалы ops, где delta_sne != программа")
        ok = False
    if int(ops_zero_prog_with_delta) != 0:
        print("❌ Найдены интервалы ops с program=0, но delta_sne>0")
        ok = False
    if int(ops_prog_positive_delta_zero) != 0:
        print("❌ Найдены интервалы ops с program>0, но delta_sne=0")
        ok = False

    # Детализация нарушений (до 50 строк на тип)
    if int(non_ops_with_delta) > 0:
        print("\n--- Нарушения: налёт вне операций (prev_state != operations) ---")
        rows = client.execute(f"""
        {prog_cte}
        SELECT aircraft_number, prev_day, day_u16, prev_state, delta_sne
        FROM prog
        WHERE prev_state != 'operations' AND delta_sne != 0
        ORDER BY day_u16, aircraft_number
        LIMIT 50
        """)
        for row in rows:
            print(row)

    if int(ops_mismatch_prog) > 0:
        print("\n--- Нарушения: ops delta_sne != программа ---")
        rows = client.execute(f"""
        {prog_cte}
        SELECT aircraft_number, prev_day, day_u16, delta_sne, prog_sum
        FROM prog
        WHERE prev_state = 'operations' AND delta_sne != prog_sum
        ORDER BY day_u16, aircraft_number
        LIMIT 50
        """)
        for row in rows:
            print(row)

    if int(ops_zero_prog_with_delta) > 0:
        print("\n--- Нарушения: program=0, но delta_sne>0 ---")
        rows = client.execute(f"""
        {prog_cte}
        SELECT aircraft_number, prev_day, day_u16, delta_sne, prog_sum
        FROM prog
        WHERE prev_state = 'operations' AND prog_sum = 0 AND delta_sne != 0
        ORDER BY day_u16, aircraft_number
        LIMIT 50
        """)
        for row in rows:
            print(row)

    if int(ops_prog_positive_delta_zero) > 0:
        print("\n--- Нарушения: program>0, но delta_sne=0 ---")
        rows = client.execute(f"""
        {prog_cte}
        SELECT aircraft_number, prev_day, day_u16, delta_sne, prog_sum
        FROM prog
        WHERE prev_state = 'operations' AND prog_sum > 0 AND delta_sne = 0
        ORDER BY day_u16, aircraft_number
        LIMIT 50
        """)
        for row in rows:
            print(row)

    if ok:
        print("\n✅ OK: Налёт и инварианты по delta_sne соблюдены")
        return 0

    print("\n❌ FAIL: Найдены нарушения инвариантов")
    return 1


if __name__ == "__main__":
    sys.exit(main())

