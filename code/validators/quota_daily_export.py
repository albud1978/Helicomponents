#!/usr/bin/env python3
"""
Экспорт ежедневных квот и статусов в Excel на основе содержимого sim_results.

- Берёт последнюю версию (max(version_date), max(version_id)) если не указаны.
- Строит список дней по фактическим day_u16 из sim_results (как в раннем отчёте).
- Для каждого дня считает:
  seed8/seed17 (из MP4 через prepare_env_arrays, индекс d+1),
  approved8/approved17 (ops_ticket=1 & intent_flag=1 по группам 1/2),
  left8/left17 = seed - approved (не отриц.),
  cnt_s2_gb1/cnt_s2_gb2 — число агрегатов в статусе 2 по группам 1/2.

Параметры CLI:
  --table           имя таблицы (по умолчанию sim_results)
  --excel           путь к выходному Excel (по умолчанию logs/quota_daily_YYYYMMDD_HHMMSS.xlsx)
  --vd, --vid       зафиксировать версию; иначе берутся максимальные из таблицы

Дата: 2025-09-04
"""

from __future__ import annotations

import argparse
import datetime
import os
from typing import Dict, Tuple, List

import pandas as pd

import sys
import os as _os

# Добавляем пути к модулям проекта (избегаем конфликта со stdlib 'code')
_BASE = _os.path.dirname(__file__)
_CODE_DIR = _os.path.abspath(_os.path.join(_BASE, ".."))
_UTILS_DIR = _os.path.join(_CODE_DIR, "utils")
if _CODE_DIR not in sys.path:
    sys.path.append(_CODE_DIR)
if _UTILS_DIR not in sys.path:
    sys.path.append(_UTILS_DIR)

from utils.config_loader import get_clickhouse_client
from sim_env_setup import prepare_env_arrays


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Экспорт дневных квот/статусов из sim_results в Excel")
    parser.add_argument("--table", default="sim_results", help="Источник данных (ClickHouse) — таблица sim_results")
    parser.add_argument("--excel", default="", help="Путь к Excel. По умолчанию logs/quota_daily_YYYYMMDD_HHMMSS.xlsx")
    parser.add_argument("--vd", type=int, default=None, help="version_date (UInt32)")
    parser.add_argument("--vid", type=int, default=None, help="version_id (UInt32)")
    return parser.parse_args()


def get_version(client, table: str, vd: int | None, vid: int | None) -> Tuple[int, int]:
    if vd is not None and vid is not None:
        return int(vd), int(vid)
    q = f"SELECT toUInt32(max(version_date)), toUInt32(max(version_id)) FROM {table}"
    res = client.execute(q)[0]
    return int(res[0]), int(res[1])


def load_days(client, table: str, vd: int, vid: int) -> List[Tuple[int, str]]:
    sql = (
        f"SELECT day_u16, any(day_date) AS day_date "
        f"FROM {table} WHERE version_date=toUInt32({vd}) AND version_id=toUInt32({vid}) "
        f"GROUP BY day_u16 ORDER BY day_u16"
    )
    rows = client.execute(sql)
    return [(int(d), str(dt)) for d, dt in rows]


def load_approvals_and_status2(client, table: str, vd: int, vid: int) -> Dict[int, Tuple[int, int, int, int]]:
    sql = f"""
    WITH toUInt32({vd}) AS vd, toUInt32({vid}) AS vid
    SELECT day_u16,
           sum(if(group_by=1 AND ops_ticket=1 AND intent_flag=1, 1, 0)) AS appr8,
           sum(if(group_by=2 AND ops_ticket=1 AND intent_flag=1, 1, 0)) AS appr17,
           sum(if(group_by=1 AND status_id=2, 1, 0)) AS cnt_s2_gb1,
           sum(if(group_by=2 AND status_id=2, 1, 0)) AS cnt_s2_gb2
    FROM {table}
    WHERE version_date=vd AND version_id=vid
    GROUP BY day_u16
    ORDER BY day_u16
    """
    rows = client.execute(sql)
    return {int(d): (int(a8), int(a17), int(c1), int(c2)) for d, a8, a17, c1, c2 in rows}


def seed_value(arr: List[int], day: int) -> int:
    if not arr:
        return 0
    idx = day + 1
    if idx < len(arr):
        return int(arr[idx])
    return int(arr[-1])


def build_rows(client, table: str, vd: int, vid: int) -> pd.DataFrame:
    days = load_days(client, table, vd, vid)
    A = load_approvals_and_status2(client, table, vd, vid)

    env = prepare_env_arrays(client)
    arr8 = list(env.get("mp4_ops_counter_mi8", []))
    arr17 = list(env.get("mp4_ops_counter_mi17", []))

    out = []
    for d, d_date in days:
        s8 = seed_value(arr8, d)
        s17 = seed_value(arr17, d)
        a8, a17, c1, c2 = A.get(d, (0, 0, 0, 0))
        out.append({
            "day_u16": d,
            "day_date": d_date,
            "seed8": s8,
            "approved8": a8,
            "left8": max(0, s8 - a8),
            "seed17": s17,
            "approved17": a17,
            "left17": max(0, s17 - a17),
            "cnt_s2_gb1": c1,
            "cnt_s2_gb2": c2,
        })
    return pd.DataFrame(out)


def main() -> None:
    args = parse_args()
    client = get_clickhouse_client()

    vd, vid = get_version(client, args.table, args.vd, args.vid)
    df = build_rows(client, args.table, vd, vid)

    os.makedirs("logs", exist_ok=True)
    if args.excel:
        out_path = args.excel
    else:
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        out_path = f"logs/quota_daily_{ts}.xlsx"

    try:
        with pd.ExcelWriter(out_path, engine="openpyxl") as w:
            df.to_excel(w, index=False, sheet_name="quota_daily")
            # Добавим 3 контрольные формулы: chk8, chk17, s2_total
            ws = w.book["quota_daily"]
            # Заголовки
            ws.cell(row=1, column=11, value="chk8")      # K
            ws.cell(row=1, column=12, value="chk17")     # L
            ws.cell(row=1, column=13, value="s2_total")  # M
            n = len(df)
            for r in range(2, n + 2):
                ws.cell(row=r, column=11, value=f"=C{r}-D{r}-E{r}")
                ws.cell(row=r, column=12, value=f"=F{r}-G{r}-H{r}")
                ws.cell(row=r, column=13, value=f"=I{r}+J{r}")
        print(out_path, len(df))
    except Exception as e:
        # Fallback в CSV
        csv_path = os.path.splitext(out_path)[0] + ".csv"
        df.to_csv(csv_path, index=False)
        print(csv_path, len(df), f"(excel_failed={e})")


if __name__ == "__main__":
    main()


