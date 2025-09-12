#!/usr/bin/env python3
"""
Формирование журнала переходов статусов на базе sim_results и экспорт в Excel.

- Журнал: одна строка = переход между d-1 и d (prev_status != curr_status)
- Печатаются условия (cond_*) и сайд-эффекты (side_*) для валидации.

CLI:
  --table    источник (по умолчанию sim_results)
  --excel    путь к Excel (по умолчанию logs/transitions_log_YYYYMMDD_HHMMSS.xlsx)
  --vd/--vid версия (если не указаны — берём максимальные)

Дата: 2025-09-04
"""

from __future__ import annotations

import argparse
import datetime
import os
import sys
from typing import Tuple

import pandas as pd

# Подключаем проектные модули (избегаем конфликта со stdlib 'code')
_BASE = os.path.dirname(__file__)
_CODE_DIR = os.path.abspath(os.path.join(_BASE, ".."))
_UTILS_DIR = os.path.join(_CODE_DIR, "utils")
if _CODE_DIR not in sys.path:
    sys.path.append(_CODE_DIR)
if _UTILS_DIR not in sys.path:
    sys.path.append(_UTILS_DIR)

from utils.config_loader import get_clickhouse_client


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Экспорт журнала переходов из sim_results")
    p.add_argument("--table", default="sim_results", help="Имя таблицы в ClickHouse")
    p.add_argument("--excel", default="", help="Путь к Excel-файлу вывода")
    p.add_argument("--vd", type=int, default=None, help="version_date (UInt32)")
    p.add_argument("--vid", type=int, default=None, help="version_id (UInt32)")
    return p.parse_args()


def get_version(client, table: str, vd: int | None, vid: int | None) -> Tuple[int, int]:
    if vd is not None and vid is not None:
        return int(vd), int(vid)
    sql = f"SELECT toUInt32(max(version_date)), toUInt32(max(version_id)) FROM {table}"
    v = client.execute(sql)[0]
    return int(v[0]), int(v[1])


def build_sql(table: str, vd: int, vid: int) -> str:
    return f"""
WITH toUInt32({vd}) AS vd, toUInt32({vid}) AS vid
SELECT
  c.aircraft_number AS id,
  c.day_u16, c.day_date,
  p.day_u16 AS prev_day_u16, p.day_date AS prev_day_date,
  p.status_id AS prev_status, c.status_id AS curr_status,
  multiIf(p.status_id=1 AND c.status_id=2, '1to2',
          p.status_id=1 AND c.status_id=4, '1to4',
          p.status_id=4 AND c.status_id=2, '4to2',
          p.status_id=2 AND c.status_id=4, '2to4',
          p.status_id=2 AND c.status_id=6, '2to6',
          p.status_id=2 AND c.status_id=3, '2to3',
          p.status_id=3 AND c.status_id=2, '3to2',
          p.status_id=4 AND c.status_id=5, '4to5',
          p.status_id=5 AND c.status_id=2, '5to2',
          '') AS transition_code,

  p.sne AS sne_prev, c.sne AS sne_curr,
  p.ppr AS ppr_prev, c.ppr AS ppr_curr,
  p.repair_days AS repair_prev, c.repair_days AS repair_curr,
  p.daily_today_u32 AS dt_prev, c.daily_today_u32 AS dt_curr,
  p.ops_ticket AS ops_ticket_prev, p.intent_flag AS intent_flag_prev,
  p.active_trigger AS active_trigger_prev, c.assembly_trigger AS assembly_trigger_curr,
  c.group_by, c.oh, c.br,

  toInt32(p.active_trigger) - toInt32(vd) - 1 AS s_rel,
  NULL AS d_set,
  NULL AS asm_day,
  (s_rel >= 0 AND s_rel < c.day_u16) AS window_ok,

  /* условия */
  (p.status_id=1 AND p.ops_ticket=1 AND p.intent_flag=1) AS cond_ops_approved,
  (p.active_trigger > 0) AS cond_trigger_set,
  (p.ppr >= c.oh) AS cond_ppr_ge_oh,
  (p.sne >= c.br) AS cond_reach_br,
  (p.status_id=3) AS cond_leave_simple,
  (c.assembly_trigger=1) AS cond_assembly_day,

  /* сайд-эффекты */
  (p.status_id=1 AND c.status_id=2 AND c.ppr=0) AS side_ppr_reset,
  (p.status_id=2 AND c.status_id=2 AND c.sne - p.sne = p.daily_today_u32) AS side_sne_inc_by_dt,
  (p.status_id=2 AND c.status_id=2 AND c.ppr - p.ppr = p.daily_today_u32) AS side_ppr_inc_by_dt,
  (c.status_id=4 AND c.repair_days - p.repair_days = 1) AS side_repair_inc,
  (p.status_id=4 AND c.status_id=2 AND c.repair_days >= p.repair_days) AS side_repair_stop,
  (c.assembly_trigger=1) AS side_asm_trigger,
  (c.status_id=6 AND c.sne=p.sne AND c.ppr=p.ppr) AS side_fix_in_s6

FROM {table} c
INNER JOIN {table} p
  ON p.version_date = vd
 AND p.version_id   = vid
 AND c.version_date = vd
 AND c.version_id   = vid
 AND p.aircraft_number = c.aircraft_number
 AND p.day_u16 = c.day_u16 - 1
WHERE c.version_date = vd AND c.version_id = vid
  AND p.status_id != c.status_id
ORDER BY id, day_u16
"""


def build_sql_assembly_check(table: str, vd: int, vid: int) -> str:
    """Срез по дням сборки: ожидаемый 4→5 и факт на d+1."""
    return f"""
WITH toUInt32({vd}) AS vd, toUInt32({vid}) AS vid
SELECT
  c.aircraft_number AS id,
  c.day_u16 AS asm_day, c.day_date AS asm_date,
  p.status_id AS prev_status, c.status_id AS curr_status,
  p.repair_days AS repair_prev, c.repair_days AS repair_curr,
  c.assembly_trigger,
  n.status_id AS next_status,
  (p.status_id=4) AS cond_prev_s4,
  (c.assembly_trigger=1) AS cond_asm,
  (n.status_id=5) AS side_4to5_fact
FROM {table} c
INNER JOIN {table} p
  ON p.version_date=vd AND p.version_id=vid
 AND c.version_date=vd AND c.version_id=vid
 AND p.aircraft_number=c.aircraft_number
 AND p.day_u16=c.day_u16-1
LEFT JOIN {table} n
  ON n.version_date=vd AND n.version_id=vid
 AND n.aircraft_number=c.aircraft_number
 AND n.day_u16=c.day_u16+1
WHERE c.version_date=vd AND c.version_id=vid AND c.assembly_trigger=1
ORDER BY id, asm_day
"""


def main() -> None:
    args = parse_args()
    client = get_clickhouse_client()
    vd, vid = get_version(client, args.table, args.vd, args.vid)

    sql = build_sql(args.table, vd, vid)
    rows = client.execute(sql)
    cols = [
        "id","day_u16","day_date","prev_day_u16","prev_day_date","prev_status","curr_status","transition_code",
        "sne_prev","sne_curr","ppr_prev","ppr_curr","repair_prev","repair_curr","dt_prev","dt_curr",
        "ops_ticket_prev","intent_flag_prev","active_trigger_prev","assembly_trigger_curr","group_by","oh","br",
        "s_rel","d_set","asm_day","window_ok",
        "cond_ops_approved","cond_trigger_set","cond_ppr_ge_oh","cond_reach_br","cond_leave_simple","cond_assembly_day",
        "side_ppr_reset","side_sne_inc_by_dt","side_ppr_inc_by_dt","side_repair_inc","side_repair_stop","side_asm_trigger","side_fix_in_s6",
    ]
    df = pd.DataFrame(rows, columns=cols)

    os.makedirs("logs", exist_ok=True)
    out_path = args.excel or f"logs/transitions_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    # Свод по типам переходов и быстрые нарушения условий/сайд-эффектов
    # Свод по предопределённым меткам
    piv = (
        df.groupby("transition_code")["id"].count().rename("count").reset_index()
    )
    # Универсальная метка для всех комбинаций: prev→curr
    df["trans_any"] = df["prev_status"].astype(str) + "to" + df["curr_status"].astype(str)
    piv_all = (
        df.groupby("trans_any")["id"].count().rename("count").reset_index().sort_values("count", ascending=False)
    )
    # Матрица переходов prev_status × curr_status
    pivot_matrix = (
        df.pivot_table(index="prev_status", columns="curr_status", values="id", aggfunc="count", fill_value=0)
    )
    viol_cols = [c for c in df.columns if c.startswith("cond_") or c.startswith("side_")]
    viol = pd.DataFrame({col: (~df[col].astype(bool)).sum() for col in viol_cols}, index=[0]).T.reset_index()
    viol.columns = ["check", "violations"]

    with pd.ExcelWriter(out_path, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="transitions")
        piv.to_excel(w, index=False, sheet_name="summary")
        piv_all.to_excel(w, index=False, sheet_name="summary_all")
        pivot_matrix.to_excel(w, sheet_name="matrix")
        viol.to_excel(w, index=False, sheet_name="violations")

    print(out_path, len(df))


if __name__ == "__main__":
    main()


