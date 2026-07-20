#!/usr/bin/env python3
"""Материализация status_overhaul (DWH) в ClickHouse default.bi_status_overhaul_gantt для BI-Ганта.

Полоса Ганта = фактический интервал (act_start→act_end), где факта нет — плановый (sched_*).
overdue=1, если конечная дата интервала в прошлом относительно report_date и статус ≠ 'Закрыто'
(эти ремонты подсвечиваются красной рамкой плагином echarts6_gantt).

Идемпотентно по report_date (ALTER DELETE, не DROP/TRUNCATE). Только SELECT из DWH.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1] / "utils"))

from config_loader import auto_load_env_file, get_clickhouse_client  # type: ignore
from dwh_golden_replay_export import dwh_client, status_overhaul_dataframe  # type: ignore

TABLE = "bi_status_overhaul_gantt"


def _to_dt(v):
    if v is None or pd.isna(v):
        return None
    return pd.Timestamp(v).to_pydatetime()


def build(df: pd.DataFrame, report_date: str) -> pd.DataFrame:
    ref = pd.Timestamp(report_date)
    out = df.copy()
    out["ac_registr"] = out["ac_registr"].astype("int64")
    # тип → group_by: Ми-17 = 2 (цвет), иначе Ми-8 = 1
    out["group_by"] = out["ac_typ"].map(lambda t: 2 if str(t).strip() == "Ми-17" else 1).astype("uint8")
    out["line"] = "RA-" + out["ac_registr"].astype(str)
    out["aircraft_number"] = out["ac_registr"].astype("uint32")

    # эффективный интервал: факт, где есть; иначе план
    start = out["act_start_date"].where(out["act_start_date"].notna(), out["sched_start_date"])
    end = out["act_end_date"].where(out["act_end_date"].notna(), out["sched_end_date"])
    out["start_date"] = start
    out["end_date"] = end

    overdue = (end < ref) & (out["status"].astype(str).str.strip() != "Закрыто")
    out["overdue"] = overdue.fillna(False).astype("uint8")

    out["report_date"] = ref.date()
    out["ac_type"] = out["ac_typ"].astype(str)
    return out[[
        "report_date", "line", "aircraft_number", "group_by", "ac_type", "wpno",
        "description", "status", "start_date", "end_date", "sched_start_date",
        "sched_end_date", "act_start_date", "act_end_date", "overdue",
    ]]


def materialize(client, df: pd.DataFrame, report_date: str) -> None:
    client.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            report_date Date,
            line String,
            aircraft_number UInt32,
            group_by UInt8,
            ac_type String,
            wpno String,
            description String,
            status String,
            start_date DateTime,
            end_date DateTime,
            sched_start_date Nullable(DateTime),
            sched_end_date Nullable(DateTime),
            act_start_date Nullable(DateTime),
            act_end_date Nullable(DateTime),
            overdue UInt8
        ) ENGINE = MergeTree ORDER BY (report_date, line, start_date)
    """)
    client.execute(f"ALTER TABLE {TABLE} DELETE WHERE report_date = %(d)s", {"d": report_date})

    rows = []
    for r in df.itertuples(index=False):
        rows.append((
            r.report_date, r.line, int(r.aircraft_number), int(r.group_by), r.ac_type,
            str(r.wpno), str(r.description), str(r.status),
            _to_dt(r.start_date), _to_dt(r.end_date),
            _to_dt(r.sched_start_date), _to_dt(r.sched_end_date),
            _to_dt(r.act_start_date), _to_dt(r.act_end_date), int(r.overdue),
        ))
    client.execute(
        f"INSERT INTO {TABLE} (report_date, line, aircraft_number, group_by, ac_type, wpno, "
        f"description, status, start_date, end_date, sched_start_date, sched_end_date, "
        f"act_start_date, act_end_date, overdue) VALUES",
        rows,
    )
    print(f"✅ default.{TABLE}: {len(rows)} строк (report_date={report_date}), "
          f"overdue={int(df['overdue'].sum())}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--report-date", default="2026-07-14")
    args = p.parse_args()

    auto_load_env_file()
    report_date = args.report_date.strip()
    dwh = dwh_client()
    df = status_overhaul_dataframe(dwh, report_date=report_date)
    built = build(df, report_date)
    print(f"📅 status_overhaul report_date={report_date}: {len(built)} WP; "
          f"просроченных: {sorted(built.loc[built['overdue'] == 1, 'aircraft_number'].tolist())}")
    client = get_clickhouse_client()
    materialize(client, built, report_date)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
