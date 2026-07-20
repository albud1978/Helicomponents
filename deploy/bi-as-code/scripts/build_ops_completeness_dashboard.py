#!/usr/bin/env python3
"""
Сборка дашборда «Комплектность агрегатов по программе» в Superset (API-only).

Источник: таблицы ClickHouse default.bi_ops_completeness_board / _detail
(материализуются code/analysis/ops_aggregate_completeness_dwh.py --materialize).

Идемпотентно: датасеты/чарты/дашборд ищутся по имени; при наличии — переиспользуются
и обновляются. Никаких admin-only endpoint'ов.
"""
from __future__ import annotations

import http.cookiejar
import json
import os
import sys
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[3] / "code" / "utils"))
from config_loader import auto_load_env_file  # type: ignore

REPORT_DATE = os.environ.get("BI_REPORT_DATE", "2026-07-14")
DB_ID = int(os.environ.get("BI_DB_ID", "2"))
SCHEMA = "default"
BOARD_TABLE = "bi_ops_completeness_board"
DETAIL_TABLE = "bi_ops_completeness_detail"
DASH_TITLE = "Комплектность агрегатов по программе"


class SS:
    def __init__(self) -> None:
        self.base = os.environ["SUPERSET_API_BASE_URL"].rstrip("/")
        self.cj = http.cookiejar.CookieJar()
        self.op = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(self.cj))
        self.token = None
        self.csrf = None

    def _req(self, method, path, data=None, headers=None):
        url = self.base + path
        body = json.dumps(data).encode() if data is not None else None
        h = {"Content-Type": "application/json"}
        if self.token:
            h["Authorization"] = f"Bearer {self.token}"
        if self.csrf:
            h["X-CSRFToken"] = self.csrf
            h["Referer"] = self.base
        h.update(headers or {})
        req = urllib.request.Request(url, data=body, headers=h, method=method)
        try:
            with self.op.open(req, timeout=40) as r:
                return r.status, json.load(r)
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.load(e)
            except Exception:
                return e.code, {"_raw": e.read().decode(errors="replace")}

    def login(self):
        st, r = self._req("POST", "/api/v1/security/login", {
            "username": os.environ["SUPERSET_API_USERNAME"],
            "password": os.environ["SUPERSET_API_PASSWORD"],
            "provider": os.environ.get("SUPERSET_API_PROVIDER", "db"),
            "refresh": True,
        })
        assert st == 200, (st, r)
        self.token = r["access_token"]
        st, r = self._req("GET", "/api/v1/security/csrf_token/")
        if st == 200:
            self.csrf = r["result"]

    # ---- datasets ----
    def find_dataset(self, table_name):
        q = urllib.parse.quote(json.dumps({"filters": [
            {"col": "table_name", "opr": "eq", "value": table_name}]}))
        st, r = self._req("GET", f"/api/v1/dataset/?q={q}")
        for d in r.get("result", []):
            if d["table_name"] == table_name:
                return d["id"]
        return None

    def ensure_dataset(self, table_name):
        did = self.find_dataset(table_name)
        if did:
            self._req("PUT", f"/api/v1/dataset/{did}/refresh", {})
            return did
        st, r = self._req("POST", "/api/v1/dataset/", {
            "database": DB_ID, "schema": SCHEMA, "table_name": table_name})
        assert st in (200, 201), (st, r)
        return r["id"]

    def set_verbose(self, ds_id, verbose_map):
        """Проставляет русские verbose_name на колонки датасета (по id, override_columns)."""
        st, r = self._req("GET", f"/api/v1/dataset/{ds_id}")
        out = []
        for c in r["result"]["columns"]:
            item = {"id": c["id"], "column_name": c["column_name"]}
            vn = verbose_map.get(c["column_name"], c.get("verbose_name"))
            if vn:
                item["verbose_name"] = vn
            out.append(item)
        st, r = self._req("PUT", f"/api/v1/dataset/{ds_id}?override_columns=true", {"columns": out})
        return st

    # ---- charts ----
    def find_chart(self, name):
        q = urllib.parse.quote(json.dumps({"filters": [
            {"col": "slice_name", "opr": "eq", "value": name}]}))
        st, r = self._req("GET", f"/api/v1/chart/?q={q}")
        for c in r.get("result", []):
            if c["slice_name"] == name:
                return c["id"]
        return None

    def upsert_chart(self, name, ds_id, viz_type, params):
        params = {**params, "datasource": f"{ds_id}__table", "viz_type": viz_type}
        payload = {
            "slice_name": name, "viz_type": viz_type,
            "datasource_id": ds_id, "datasource_type": "table",
            "params": json.dumps(params),
        }
        cid = self.find_chart(name)
        if cid:
            st, r = self._req("PUT", f"/api/v1/chart/{cid}", payload)
            assert st == 200, (st, r)
            return cid
        st, r = self._req("POST", "/api/v1/chart/", payload)
        assert st in (200, 201), (st, r)
        return r["id"]

    def chart_data_smoke(self, cid):
        st, r = self._req("GET", f"/api/v1/chart/{cid}")
        params = json.loads(r["result"]["params"])
        ds_id = int(params["datasource"].split("__")[0])
        mets = params.get("metrics") or ([params["metric"]] if params.get("metric") else [])
        cols = params.get("groupby") or params.get("all_columns") or []
        filt = [{"col": f["subject"], "op": f["operator"], "val": f["comparator"]}
                for f in params.get("adhoc_filters", []) if f.get("expressionType") == "SIMPLE"]
        q = {"filters": filt, "extras": {"where": ""}, "row_limit": 5, "orderby": []}
        if mets:
            q["metrics"] = mets
        if cols:
            q["columns"] = cols
        st2, r2 = self._req("POST", "/api/v1/chart/data", {
            "datasource": {"id": ds_id, "type": "table"},
            "queries": [q], "form_data": params,
            "result_format": "json", "result_type": "results",
        })
        return st2


def report_filter():
    return {"clause": "WHERE", "subject": "report_date", "operator": "==",
            "comparator": REPORT_DATE, "expressionType": "SIMPLE"}


def sql_metric(label, expr):
    return {"expressionType": "SQL", "sqlExpression": expr, "label": label}


BOARD_VERBOSE = {
    "report_date": "Дата отчёта", "acn": "Борт", "ac_type": "Тип",
    "variant": "Вариант", "status": "Статус", "mfg_date": "Дата выпуска",
    "required": "Норма", "installed_serviceable": "Исправных установлено",
    "deficit_positions": "Позиций с дефицитом", "deficit_units": "Дефицит (шт)",
    "has_deficit": "Есть дефицит", "missing_nomenclatures": "Недостающие номенклатуры",
}
DETAIL_VERBOSE = {
    "report_date": "Дата отчёта", "acn": "Борт", "ac_type": "Тип",
    "variant": "Вариант", "status": "Статус", "nomenclature": "Номенклатура",
    "installed": "Исправных", "required": "Норма", "deficit": "Дефицит (шт)",
}


def main():
    auto_load_env_file()
    ss = SS()
    ss.login()
    board = ss.ensure_dataset(BOARD_TABLE)
    detail = ss.ensure_dataset(DETAIL_TABLE)
    print(f"datasets: board={board} detail={detail}")
    print(f"verbose board={ss.set_verbose(board, BOARD_VERBOSE)} detail={ss.set_verbose(detail, DETAIL_VERBOSE)}")

    m_prog = sql_metric("По программе", "count(*)")
    m_ok = sql_metric("Укомплектованы", "sum(1 - has_deficit)")
    m_def = sql_metric("С дефицитом", "sum(has_deficit)")
    m_units = sql_metric("Дефицит (шт)", "sum(deficit)")

    charts = []
    # 0-2: big numbers (общие, дата — через нативный фильтр дашборда)
    charts.append(ss.upsert_chart("По программе — бортов", board, "big_number_total",
        {"metric": m_prog, "adhoc_filters": []}))
    charts.append(ss.upsert_chart("Укомплектованы (исправный набор)", board, "big_number_total",
        {"metric": m_ok, "adhoc_filters": []}))
    charts.append(ss.upsert_chart("С дефицитом исправных агрегатов", board, "big_number_total",
        {"metric": m_def, "adhoc_filters": []}))
    # 3: статистика по типам (столбики)
    charts.append(ss.upsert_chart("Статистика по типам ВС", board, "echarts_timeseries_bar",
        {"x_axis": "ac_type", "metrics": [m_prog, m_ok, m_def], "adhoc_filters": [],
         "row_limit": 10, "x_axis_title": "Тип ВС", "y_axis_title": "Бортов"}))
    # 4: статус (pie)
    charts.append(ss.upsert_chart("Статус бортов программы", board, "pie",
        {"groupby": ["status"], "metric": m_prog, "adhoc_filters": []}))
    # 5: недостающие номенклатуры — столбики раздельно по типам
    charts.append(ss.upsert_chart("Недостающие номенклатуры", detail, "echarts_timeseries_bar",
        {"x_axis": "nomenclature", "groupby": ["ac_type"], "metrics": [m_units],
         "adhoc_filters": [], "row_limit": 40, "x_axis_title": "Номенклатура",
         "y_axis_title": "Дефицит (шт)", "x_axis_sort_asc": False}))
    # 6: таблица бортов с дефицитом
    charts.append(ss.upsert_chart("Борты с дефицитом", board, "table",
        {"query_mode": "raw",
         "all_columns": ["acn", "ac_type", "variant", "status", "installed_serviceable",
                         "required", "deficit_units", "missing_nomenclatures"],
         "adhoc_filters": [{"clause": "WHERE", "subject": "has_deficit", "operator": "==",
                            "comparator": "1", "expressionType": "SIMPLE"}],
         "order_by_cols": ["[\"deficit_units\", false]"], "row_limit": 200}))
    print("charts:", charts)

    print("smoke:")
    for cid in charts:
        print(f"  chart {cid}: chart/data -> {ss.chart_data_smoke(cid)}")

    # ---- layout ----
    # (chart_index, width, height) по рядам
    plan = [
        [(0, 4, 40), (1, 4, 40), (2, 4, 40)],
        [(3, 6, 55), (4, 6, 55)],
        [(5, 12, 55)],
        [(6, 12, 60)],
    ]
    pos = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": DASH_TITLE}},
    }
    for ri, row in enumerate(plan):
        rid = f"ROW-{ri}"
        pos[rid] = {"type": "ROW", "id": rid, "children": [],
                    "meta": {"background": "BACKGROUND_TRANSPARENT"}, "parents": ["ROOT_ID", "GRID_ID"]}
        pos["GRID_ID"]["children"].append(rid)
        for idx, w, h in row:
            cid = charts[idx]
            nid = f"CHART-{cid}"
            pos[nid] = {"type": "CHART", "id": nid, "children": [],
                        "parents": ["ROOT_ID", "GRID_ID", rid],
                        "meta": {"chartId": cid, "width": w, "height": h, "uuid": None}}
            pos[rid]["children"].append(nid)

    # ---- native date filter ----
    json_meta = {
        "native_filter_configuration": [{
            "id": "NATIVE_FILTER-report_date",
            "name": "Дата отчёта",
            "filterType": "filter_select",
            "targets": [{"datasetId": board, "column": {"name": "report_date"}}],
            "controlValues": {"enableEmptyFilter": False, "multiSelect": False,
                              "defaultToFirstItem": True, "sortAscending": False,
                              "searchAllOptions": False, "inverseSelection": False},
            "scope": {"rootPath": ["ROOT_ID"], "excluded": []},
            "type": "NATIVE_FILTER",
        }],
    }

    q = urllib.parse.quote(json.dumps({"filters": [
        {"col": "dashboard_title", "opr": "eq", "value": DASH_TITLE}]}))
    st, r = ss._req("GET", f"/api/v1/dashboard/?q={q}")
    dash_id = None
    for d in r.get("result", []):
        if d["dashboard_title"] == DASH_TITLE:
            dash_id = d["id"]
    payload = {"dashboard_title": DASH_TITLE, "position_json": json.dumps(pos),
               "json_metadata": json.dumps(json_meta), "published": True}
    if dash_id:
        st, r = ss._req("PUT", f"/api/v1/dashboard/{dash_id}", payload)
    else:
        st, r = ss._req("POST", "/api/v1/dashboard/", payload)
        dash_id = r.get("id")
    for cid in charts:
        ss._req("PUT", f"/api/v1/chart/{cid}", {"dashboards": [dash_id]})
    print(f"dashboard: id={dash_id} status={st}")
    print(f"URL: {ss.base}/superset/dashboard/{dash_id}/")


if __name__ == "__main__":
    main()
