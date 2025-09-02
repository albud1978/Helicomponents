#!/usr/bin/env python3
"""
Валидация триггеров partout_trigger и assembly_trigger относительно переходов 2→4
Дата: 2025-09-01

Назначение:
- Сопоставить фактические срабатывания `partout_trigger` и `assembly_trigger` с ожиданиями
  от дня перехода 2→4 с учётом сдвигов по нормативам:
  - expected_partout_day = day_2to4 + partout_time
  - expected_assembly_day = day_2to4 + (repair_time - assembly_time)

Особенности:
- Период расчёта может закончиться раньше ожидаемой даты события → допустимы расхождения
  «ожидалось событие после горизонта» (такие случаи считаем обоснованными).

Запуск:
  python3 -u code/utils/validate_triggers_vs_2to4.py \
    --sim-table sim_results \
    --version-date 2025-07-04 \
    --version-id 1

Вывод:
- Итоговая сводка в JSON: количества переходов 2→4, ожидаемых и фактических событий, совпадений и объяснимых расхождений.
"""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, Tuple

from config_loader import get_clickhouse_client


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Валидация триггеров vs 2→4")
    parser.add_argument("--sim-table", default="sim_results", help="Таблица с дневными снимками симуляции")
    parser.add_argument("--version-date", required=True, help="version_date (YYYY-MM-DD)")
    parser.add_argument("--version-id", type=int, required=True, help="version_id")
    parser.add_argument("--ac-sample", type=int, default=0, help="Фильтр по aircraft_number (0=все)")
    return parser.parse_args()


def build_queries(sim_table: str, version_date: str, version_id: int, ac_sample: int) -> Tuple[str, str, str]:
    filter_ac = "" if ac_sample == 0 else f"AND s.aircraft_number = {ac_sample}"

    # 1) Переходы 2→4 (день D: 4, день D-1: 2)
    q_transitions = f"""
        WITH
          max_day AS (
            SELECT max(day_abs) AS md
            FROM {sim_table}
            WHERE version_date = toUInt32(toDate('{version_date}')) AND version_id = {version_id}
          )
        SELECT
          s.aircraft_number,
          s.day_abs            AS day_2to4,
          s.partout_time       AS partout_time,
          s.repair_time        AS repair_time,
          s.assembly_time      AS assembly_time,
          (SELECT md FROM max_day)  AS horizon_last_day
        FROM {sim_table} s
        INNER JOIN {sim_table} p
          ON s.version_date = p.version_date
         AND s.version_id   = p.version_id
         AND s.aircraft_number = p.aircraft_number
         AND s.day_abs = p.day_abs + 1
        WHERE s.version_date = toUInt32(toDate('{version_date}'))
          AND s.version_id   = {version_id}
          AND p.status_id = 2 AND s.status_id = 4
          {filter_ac}
    """

    # 2) Фактические срабатывания (однодневные): берём дни, где триггер > 0
    q_actual = f"""
        SELECT
          aircraft_number,
          groupUniqArrayIf(day_abs, partout_trigger > 0)  AS jours_partout,
          groupUniqArrayIf(day_abs, assembly_trigger > 0) AS jours_assembly
        FROM {sim_table}
        WHERE version_date = toUInt32(toDate('{version_date}')) AND version_id = {version_id}
          {filter_ac}
        GROUP BY aircraft_number
    """

    # 3) Объединённая проверка соответствий с учётом горизонта и стартов в статусе 4
    #   Правильные формулы (off-by-one):
    #     expected_partout_day = day_2to4 + (partout_time - 1)
    #     expected_assembly_day = day_2to4 + (repair_time - assembly_time - 1)
    #   Для бортов, начавших горизонт в статусе 4 (нет перехода 2→4 внутри горизонта):
    #     при repair_days=d0 в первый день горизонта:
    #       expected_partout_day = day_first + max(0, partout_time - d0)  if d0 <= partout_time
    #       expected_assembly_day = day_first + max(0, (repair_time - assembly_time) - d0) if d0 <= (repair_time - assembly_time)
    q_eval = f"""
        WITH
          max_day AS (
            SELECT max(day_abs) AS md
            FROM {sim_table}
            WHERE version_date = toUInt32(toDate('{version_date}')) AND version_id = {version_id}
          ),
          min_day AS (
            SELECT min(day_abs) AS fd
            FROM {sim_table}
            WHERE version_date = toUInt32(toDate('{version_date}')) AND version_id = {version_id}
          ),
          transitions AS (
            SELECT
              s.aircraft_number,
              s.day_abs            AS day_2to4,
              s.partout_time       AS partout_time,
              s.repair_time        AS repair_time,
              s.assembly_time      AS assembly_time,
              (SELECT md FROM max_day)  AS horizon_last_day
            FROM {sim_table} s
            INNER JOIN {sim_table} p
              ON s.version_date = p.version_date
             AND s.version_id   = p.version_id
             AND s.aircraft_number = p.aircraft_number
             AND s.day_abs = p.day_abs + 1
            WHERE s.version_date = toUInt32(toDate('{version_date}'))
              AND s.version_id   = {version_id}
              AND p.status_id = 2 AND s.status_id = 4
              {filter_ac}
          ),
          transitions_cnt AS (
            SELECT aircraft_number, count() AS transitions_2to4
            FROM transitions
            GROUP BY aircraft_number
          ),
          initial_s4 AS (
            SELECT
              s.aircraft_number,
              s.day_abs          AS day_first,
              s.repair_days      AS d0,
              s.partout_time     AS partout_time,
              s.repair_time      AS repair_time,
              s.assembly_time    AS assembly_time,
              (SELECT md FROM max_day) AS horizon_last_day
            FROM {sim_table} s
            WHERE s.version_date = toUInt32(toDate('{version_date}'))
              AND s.version_id   = {version_id}
              AND s.day_abs = (SELECT fd FROM min_day)
              AND s.status_id = 4
              {filter_ac}
          ),
          expected AS (
            -- Из переходов 2→4
            SELECT
              aircraft_number,
              if((day_2to4 + (partout_time - 1)) <= horizon_last_day, day_2to4 + (partout_time - 1), NULL) AS exp_partout_day,
              if((day_2to4 + (repair_time - assembly_time - 1)) <= horizon_last_day, day_2to4 + (repair_time - assembly_time - 1), NULL) AS exp_assembly_day
            FROM transitions
            UNION ALL
            -- Из стартов горизонта в статусе 4
            SELECT
              aircraft_number,
              if(partout_time >= d0 AND (day_first + (partout_time - d0)) <= horizon_last_day,
                 day_first + (partout_time - d0), NULL) AS exp_partout_day,
              if((repair_time - assembly_time) >= d0 AND (day_first + (repair_time - assembly_time - d0)) <= horizon_last_day,
                 day_first + (repair_time - assembly_time - d0), NULL) AS exp_assembly_day
            FROM initial_s4
          ),
          actual AS (
            {q_actual}
          )
        SELECT
          e.aircraft_number,
          ifNull(max(tc.transitions_2to4), 0) AS transitions_2to4,
          countIf(e.exp_partout_day IS NOT NULL) AS expected_partout_within,
          sumIf(arrayExists(x -> x = e.exp_partout_day, a.jours_partout), e.exp_partout_day IS NOT NULL) AS matched_partout,
          countIf(e.exp_assembly_day IS NOT NULL) AS expected_assembly_within,
          sumIf(arrayExists(x -> x = e.exp_assembly_day, a.jours_assembly), e.exp_assembly_day IS NOT NULL) AS matched_assembly
        FROM expected e
        LEFT JOIN transitions_cnt tc USING (aircraft_number)
        LEFT JOIN actual a USING (aircraft_number)
        GROUP BY e.aircraft_number
        ORDER BY e.aircraft_number
    """

    return q_transitions, q_actual, q_eval


def run_validation() -> Dict[str, Any]:
    args = parse_args()
    client = get_clickhouse_client()

    _, _, q_eval = build_queries(
        sim_table=args.sim_table,
        version_date=args.version_date,
        version_id=args.version_id,
        ac_sample=args.ac_sample,
    )

    rows = client.execute(q_eval)
    total = {
        "transitions_2to4": 0,
        "expected_partout_within": 0,
        "matched_partout": 0,
        "expected_assembly_within": 0,
        "matched_assembly": 0,
    }
    per_ac = []
    for ac, tr, exp_p, mat_p, exp_a, mat_a in rows:
        total["transitions_2to4"] += tr
        total["expected_partout_within"] += exp_p
        total["matched_partout"] += mat_p
        total["expected_assembly_within"] += exp_a
        total["matched_assembly"] += mat_a
        per_ac.append({
            "aircraft_number": ac,
            "transitions_2to4": tr,
            "expected_partout_within": exp_p,
            "matched_partout": mat_p,
            "expected_assembly_within": exp_a,
            "matched_assembly": mat_a,
        })

    summary = {
        "version_date": args.version_date,
        "version_id": args.version_id,
        "table": args.sim_table,
        "ac_sample": args.ac_sample,
        "totals": total,
        "per_aircraft": per_ac[:50],  # обрезаем для читаемости
        "notes": [
            "Сравнение ведётся только для ожидаемых дат внутри горизонта (expected_within)",
            "Несовпадения при expected_day > horizon считаются объяснимыми",
        ],
    }
    return summary


def main() -> None:
    result = run_validation()
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()


