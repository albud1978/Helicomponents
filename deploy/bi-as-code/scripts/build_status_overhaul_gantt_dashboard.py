#!/usr/bin/env python3
"""
Сборка отдельного дашборда «График ремонта (Status Overhaul)» в Superset (API-only).

Источник: ClickHouse default.bi_status_overhaul_gantt
(материализуется code/analysis/status_overhaul_gantt_bi.py).

Диаграмма — тот же кастомный плагин echarts6_gantt, что и в 10-летнем прогнозе.
Полоса = факт (act_*), где нет факта — план (sched_*). Просроченные ремонты
(overdue=1: конечная дата в прошлом и статус ≠ 'Закрыто') плагин подсвечивает
красной рамкой (см. superset-frontend/plugins/plugin-chart-echarts6-gantt).

Идемпотентно: датасет/чарт/дашборд ищутся по имени и обновляются.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.parse
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))
from build_ops_completeness_dashboard import SS  # type: ignore

sys.path.append(str(Path(__file__).resolve().parents[3] / "code" / "utils"))
from config_loader import auto_load_env_file  # type: ignore

REPORT_DATE = os.environ.get("BI_REPORT_DATE", "2026-07-14")
TABLE = "bi_status_overhaul_gantt"
CHART_NAME = "График ремонта (Status Overhaul)"
DASH_TITLE = "График ремонта (Status Overhaul)"

VERBOSE = {
    "report_date": "Дата отчёта", "line": "Линия", "aircraft_number": "Борт",
    "group_by": "Тип (код)", "ac_type": "Тип ВС", "wpno": "WP",
    "description": "Описание", "status": "Статус", "start_date": "Начало",
    "end_date": "Окончание", "sched_start_date": "План начало",
    "sched_end_date": "План окончание", "act_start_date": "Факт начало",
    "act_end_date": "Факт окончание", "overdue": "Просрочен",
}


def main() -> int:
    auto_load_env_file()
    ss = SS()
    ss.login()
    ds = ss.ensure_dataset(TABLE)
    print(f"dataset {TABLE} -> {ds}; verbose={ss.set_verbose(ds, VERBOSE)}")

    metric = {"expressionType": "SIMPLE",
              "column": {"column_name": "aircraft_number"},
              "aggregate": "MAX", "label": "MAX(aircraft_number)"}
    params = {
        "viz_type": "echarts6_gantt",
        "groupby": [
            "line", "start_date", "end_date", "aircraft_number",
            "overdue", "description", "status",
        ],
        "metrics": [metric],
        "adhoc_filters": [{"clause": "WHERE", "subject": "report_date", "operator": "==",
                           "comparator": REPORT_DATE, "expressionType": "SIMPLE"}],
        "row_limit": 70000,
        "color_scheme": "supersetColors",
    }
    cid = ss.upsert_chart(CHART_NAME, ds, "echarts6_gantt", params)
    print(f"chart {CHART_NAME} -> {cid}; smoke chart/data -> {ss.chart_data_smoke(cid)}")

    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": ["ROW-0"], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASH_TITLE}},
        "ROW-0": {"type": "ROW", "id": "ROW-0", "children": [f"CHART-{cid}"],
                  "meta": {"background": "BACKGROUND_TRANSPARENT"}, "parents": ["ROOT_ID", "GRID_ID"]},
        f"CHART-{cid}": {"type": "CHART", "id": f"CHART-{cid}", "children": [],
                         "parents": ["ROOT_ID", "GRID_ID", "ROW-0"],
                         "meta": {"chartId": cid, "width": 12, "height": 110, "uuid": None}},
    }

    q = urllib.parse.quote(json.dumps({"filters": [
        {"col": "dashboard_title", "opr": "eq", "value": DASH_TITLE}]}))
    st, r = ss._req("GET", f"/api/v1/dashboard/?q={q}")
    dash_id = None
    for d in r.get("result", []):
        if d["dashboard_title"] == DASH_TITLE:
            dash_id = d["id"]
    payload = {"dashboard_title": DASH_TITLE, "position_json": json.dumps(pos), "published": True}
    if dash_id:
        st, r = ss._req("PUT", f"/api/v1/dashboard/{dash_id}", payload)
    else:
        st, r = ss._req("POST", "/api/v1/dashboard/", payload)
        dash_id = r.get("id")
    ss._req("PUT", f"/api/v1/chart/{cid}", {"dashboards": [dash_id]})
    print(f"dashboard: id={dash_id} status={st}")
    print(f"URL: {ss.base}/superset/dashboard/{dash_id}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
