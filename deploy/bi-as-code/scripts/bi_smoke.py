#!/usr/bin/env python3
"""Smoke-check Superset chart data and Gantt repairline slot overlaps."""

from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any

import requests
import yaml
from clickhouse_driver import Client

from superset_git_sync import _build_base_url, _login, _require_non_empty


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CHART_IDS = [3, 8, 4, 5, 6, 7, 9, 10, 11, 54, 55]
REPAIR_TABLE = "sim_repairline_v9"
REQUIRED_REPAIR_COLUMNS = {
    "version_date",
    "version_id",
    "group_by",
    "day_u16",
    "line_id",
    "aircraft_number",
}


def _health_check(base_url: str, timeout_sec: int) -> None:
    response = requests.get(f"{base_url}/health", timeout=timeout_sec)
    if response.status_code != 200:
        raise RuntimeError(f"Superset health check failed: HTTP {response.status_code}")
    print(f"health OK: {base_url}/health")


def _parse_json_field(name: str, value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        parsed = json.loads(value)
        if isinstance(parsed, dict):
            return parsed
    raise RuntimeError(f"Chart field is not a JSON object: {name}")


def _get_chart_query_context(ss: Any, chart_id: int, timeout_sec: int) -> dict[str, Any]:
    response = ss.session.get(
        f"{ss.base_url}/api/v1/chart/{chart_id}",
        headers=ss.auth_headers,
        timeout=timeout_sec,
    )
    if response.status_code != 200:
        raise RuntimeError(f"chart {chart_id} metadata HTTP {response.status_code}: {response.text}")
    result = response.json().get("result")
    if not isinstance(result, dict):
        raise RuntimeError(f"chart {chart_id} metadata has no result object")

    query_context = result.get("query_context")
    if query_context:
        return _parse_json_field("query_context", query_context)

    params = result.get("params")
    if params:
        parsed_params = _parse_json_field("params", params)
        nested_query_context = parsed_params.get("query_context")
        if nested_query_context:
            return _parse_json_field("params.query_context", nested_query_context)

    raise RuntimeError(f"chart {chart_id} has no query_context in API metadata")


def _simple_filter(col: str, op: str, val: Any) -> dict[str, Any]:
    return {"col": col, "op": op, "val": val}


def _adhoc_filter(subject: str, operator: str, comparator: Any) -> dict[str, Any]:
    return {
        "clause": "WHERE",
        "subject": subject,
        "operator": operator,
        "comparator": comparator,
        "expressionType": "SIMPLE",
    }


def _pin_gantt_context(
    query_context: dict[str, Any],
    version_date: int,
    version_id: int | None,
) -> dict[str, Any]:
    queries = query_context.get("queries")
    if not isinstance(queries, list) or not queries:
        raise RuntimeError("Gantt query_context has no queries")

    for query in queries:
        filters = query.get("filters", [])
        if not isinstance(filters, list):
            raise RuntimeError("Gantt query filters are not a list")
        filters = [f for f in filters if f.get("col") not in {"version_date", "version_id"}]
        filters.append(_simple_filter("version_date", "==", version_date))
        if version_id is not None:
            filters.append(_simple_filter("version_id", "==", version_id))
        query["filters"] = filters

    form_data = query_context.setdefault("form_data", {})
    if not isinstance(form_data, dict):
        raise RuntimeError("Gantt form_data is not an object")
    adhoc_filters = form_data.get("adhoc_filters", [])
    if not isinstance(adhoc_filters, list):
        raise RuntimeError("Gantt adhoc_filters are not a list")
    adhoc_filters = [
        f for f in adhoc_filters if f.get("subject") not in {"version_date", "version_id"}
    ]
    adhoc_filters.append(_adhoc_filter("version_date", "==", version_date))
    if version_id is not None:
        adhoc_filters.append(_adhoc_filter("version_id", "==", version_id))
    form_data["adhoc_filters"] = adhoc_filters
    return query_context


def _extract_row_count(payload: dict[str, Any]) -> int:
    result = payload.get("result")
    if not isinstance(result, list):
        raise RuntimeError("chart/data response has no result list")
    rows = 0
    for item in result:
        if not isinstance(item, dict):
            continue
        rowcount = item.get("rowcount")
        if isinstance(rowcount, int):
            rows += rowcount
            continue
        data = item.get("data")
        if isinstance(data, list):
            rows += len(data)
        elif isinstance(data, dict) and isinstance(data.get("records"), list):
            rows += len(data["records"])
    return rows


def _post_chart_data(ss: Any, chart_id: int, query_context: dict[str, Any], timeout_sec: int) -> int:
    query_context["force"] = False
    query_context["result_format"] = "json"
    query_context["result_type"] = "full"
    response = ss.session.post(
        f"{ss.base_url}/api/v1/chart/data",
        headers=ss.write_headers,
        json=query_context,
        timeout=timeout_sec,
    )
    if response.status_code != 200:
        raise RuntimeError(f"chart {chart_id} data HTTP {response.status_code}: {response.text}")
    rows = _extract_row_count(response.json())
    print(f"chart {chart_id} data OK: rows={rows}")
    return rows


def _load_ch_client() -> tuple[Client, str]:
    config_path = REPO_ROOT / "config" / "database_config.yaml"
    config = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    database_config = config.get("database")
    if not isinstance(database_config, dict):
        raise RuntimeError("Invalid config/database_config.yaml: missing database section")
    env_config = database_config.get("env", {})
    if not isinstance(env_config, dict):
        raise RuntimeError("Invalid database_config.yaml: database.env is not an object")

    host = os.getenv(env_config.get("host_var", "CLICKHOUSE_HOST"), database_config.get("host"))
    port = int(os.getenv(env_config.get("port_var", "CLICKHOUSE_PORT"), database_config.get("port")))
    user = os.getenv(env_config.get("user_var", "CLICKHOUSE_USER"), database_config.get("user"))
    password_var = env_config.get("password_var", "CLICKHOUSE_PASSWORD")
    password = os.getenv(password_var, "")
    database = str(database_config.get("database", "default"))
    if not password:
        raise RuntimeError(f"Missing required ClickHouse password env var: {password_var}")
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        raise RuntimeError(f"Unsafe ClickHouse database identifier: {database}")

    client = Client(host=host, port=port, user=user, password=password, database=database)
    return client, database


def _check_repairline_overlap(version_date: int, version_id: int | None) -> str | None:
    client, database = _load_ch_client()
    table_ref = f"{database}.{REPAIR_TABLE}"
    describe_rows = client.execute(f"DESCRIBE TABLE {table_ref}")
    columns = {row[0] for row in describe_rows}
    missing = sorted(REQUIRED_REPAIR_COLUMNS - columns)
    if missing:
        raise RuntimeError(f"{table_ref} missing required columns: {missing}")

    filters = [
        "version_date = %(version_date)s",
        "group_by IN (1, 2)",
        "aircraft_number > 0",
    ]
    params: dict[str, int] = {"version_date": version_date}
    if version_id is not None:
        filters.append("version_id = %(version_id)s")
        params["version_id"] = version_id
    where_sql = " AND ".join(filters)

    # Superset может вернуть агрегаты Gantt; честный overlap проверяем напрямую по CH-источнику.
    total_rows = client.execute(
        f"SELECT count() FROM {table_ref} WHERE {where_sql}",
        params,
    )[0][0]
    offenders = client.execute(
        f"""
        SELECT
            line_id,
            day_u16,
            count() AS aircraft_records,
            uniqExact(aircraft_number) AS distinct_aircraft,
            uniqExact(version_id) AS distinct_versions,
            arraySlice(arraySort(groupArrayDistinct(version_id)), 1, 10) AS sample_version_ids,
            arraySlice(arraySort(groupArrayDistinct(aircraft_number)), 1, 10) AS sample_aircraft
        FROM {table_ref}
        WHERE {where_sql}
        GROUP BY line_id, day_u16
        HAVING aircraft_records > 1
        ORDER BY aircraft_records DESC, line_id, day_u16
        LIMIT 10
        """,
        params,
    )
    scope = f"version_date={version_date}"
    if version_id is not None:
        scope += f", version_id={version_id}"
    print(f"gantt source rows: {total_rows} ({scope}, group_by IN (1,2))")
    if total_rows == 0:
        raise RuntimeError(f"Gantt source slice is empty: {scope}")
    if offenders:
        top = offenders[0]
        return (
            "Gantt overlap FAIL: "
            f"max aircraft_records per (line_id, day_u16)={top[2]} at "
            f"line_id={top[0]}, day_u16={top[1]}, distinct_versions={top[4]}, "
            f"sample_version_ids={top[5]}, sample_aircraft={top[6]}"
        )
    print("gantt overlap OK: max aircraft_records per (line_id, day_u16)<=1")
    return None


def _validate_version_date(value: str | None) -> int | None:
    if value is None:
        return None
    if not re.fullmatch(r"\d{8}", value):
        raise RuntimeError("--version-date must use YYYYMMDD format")
    return int(value)


def build_parser() -> argparse.ArgumentParser:
    default_timeout = int(os.getenv("SUPERSET_API_TIMEOUT_SEC", "120"))
    parser = argparse.ArgumentParser(description="Superset chart/data smoke checks.")
    parser.add_argument("--chart-ids", nargs="+", type=int, default=DEFAULT_CHART_IDS)
    parser.add_argument("--version-date")
    parser.add_argument("--version-id", type=int)
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--base-url", default=os.getenv("SUPERSET_API_BASE_URL"))
    parser.add_argument("--username", default=os.getenv("SUPERSET_API_USERNAME"))
    parser.add_argument("--password", default=os.getenv("SUPERSET_API_PASSWORD"))
    parser.add_argument("--provider", default=os.getenv("SUPERSET_API_PROVIDER", "db"))
    parser.add_argument("--timeout-sec", type=int, default=default_timeout)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    base_url = _build_base_url(
        _require_non_empty("--base-url / SUPERSET_API_BASE_URL", args.base_url)
    )
    username = _require_non_empty("--username / SUPERSET_API_USERNAME", args.username)
    password = _require_non_empty("--password / SUPERSET_API_PASSWORD", args.password)
    version_date = _validate_version_date(args.version_date)
    if 3 in args.chart_ids and version_date is None:
        raise RuntimeError("--version-date is required when chart 3 is checked")

    failures: list[str] = []
    _health_check(base_url, args.timeout_sec)
    ss = _login(base_url, username, password, provider=args.provider, timeout_sec=args.timeout_sec)

    for chart_id in args.chart_ids:
        context = _get_chart_query_context(ss, chart_id, args.timeout_sec)
        if chart_id == 3:
            context = _pin_gantt_context(context, int(version_date), args.version_id)
        rows = _post_chart_data(ss, chart_id, context, args.timeout_sec)
        if rows <= 0:
            failures.append(f"chart {chart_id}: rows=0")

    if 3 in args.chart_ids:
        overlap_failure = _check_repairline_overlap(int(version_date), args.version_id)
        if overlap_failure:
            failures.append(overlap_failure)

    if failures:
        print("SMOKE FAIL:")
        for failure in failures:
            print(f"- {failure}")
        return 1 if args.strict else 0

    print("SMOKE PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
