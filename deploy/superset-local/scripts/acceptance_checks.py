#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from typing import List

import requests
from clickhouse_driver import Client


def parse_int_list(value: str) -> List[int]:
    values = [x.strip() for x in value.split(",") if x.strip()]
    if not values:
        raise ValueError("Empty list is not allowed")
    return [int(v) for v in values]


def check_superset_health(base_url: str) -> None:
    r = requests.get(f"{base_url}/health", timeout=20)
    r.raise_for_status()
    text = r.text.strip()
    if text.upper() == "OK":
        print("[check] Superset health OK")
        return
    payload = r.json()
    if payload.get("status") != "OK":
        raise RuntimeError(f"Superset health is not OK: {payload}")
    print("[check] Superset health OK")


def login_superset(base_url: str, username: str, password: str) -> str:
    payload = {
        "username": username,
        "password": password,
        "provider": "db",
        "refresh": True
    }
    r = requests.post(f"{base_url}/api/v1/security/login", json=payload, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def check_database_registration(base_url: str, token: str, database_name: str) -> None:
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(
        f"{base_url}/api/v1/database/",
        headers=headers,
        params={"q": "(page:0,page_size:200)"},
        timeout=30
    )
    r.raise_for_status()
    items = r.json().get("result", [])
    names = {item.get("database_name") for item in items}
    if database_name not in names:
        raise RuntimeError(f"Database '{database_name}' not found in Superset metadata")
    print(f"[check] Superset database '{database_name}' is registered")


def check_status4_granularity(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    table: str,
    version_ids: List[int],
    group_by: List[int]
) -> None:
    version_sql = ", ".join(str(v) for v in version_ids)
    group_sql = ", ".join(str(v) for v in group_by)
    where = (
        f"version_id IN ({version_sql}) "
        f"AND group_by IN ({group_sql}) "
        "AND status_id = 4"
    )

    client = Client(host=host, port=port, user=user, password=password, database=database)
    counts = {}

    queries = {
        "day": f"SELECT count() FROM (SELECT day_date FROM {table} WHERE {where} GROUP BY day_date)",
        "month": f"SELECT count() FROM (SELECT toStartOfMonth(day_date) AS m FROM {table} WHERE {where} GROUP BY m)",
        "year": f"SELECT count() FROM (SELECT toStartOfYear(day_date) AS y FROM {table} WHERE {where} GROUP BY y)"
    }

    for granularity, sql in queries.items():
        result = client.execute(sql)
        counts[granularity] = int(result[0][0]) if result else 0

    print("[check] status=4 buckets:", json.dumps(counts, ensure_ascii=True))
    if counts["day"] <= 0:
        raise RuntimeError("No day-level buckets for status=4")
    if counts["month"] <= 0 or counts["year"] <= 0:
        raise RuntimeError("No month/year-level buckets for status=4")


def main() -> int:
    parser = argparse.ArgumentParser(description="Local acceptance checks for Superset + ClickHouse")
    parser.add_argument("--superset-url", default=os.getenv("SUPERSET_URL", "http://localhost:8088"))
    parser.add_argument("--superset-user", default=os.getenv("SUPERSET_ADMIN_USERNAME", "admin"))
    parser.add_argument("--superset-password", default=os.getenv("SUPERSET_ADMIN_PASSWORD", "admin"))
    parser.add_argument("--superset-db-name", default=os.getenv("SUPERSET_CH_DATABASE_NAME", "clickhouse_default"))
    parser.add_argument("--ch-host", default=os.getenv("CLICKHOUSE_HOST", "10.95.19.132"))
    parser.add_argument("--ch-port", type=int, default=int(os.getenv("CLICKHOUSE_PORT", "9000")))
    parser.add_argument("--ch-user", default=os.getenv("CLICKHOUSE_USER", "default"))
    parser.add_argument("--ch-password", default=os.getenv("CLICKHOUSE_PASSWORD", ""))
    parser.add_argument("--ch-database", default=os.getenv("CLICKHOUSE_DATABASE", "default"))
    parser.add_argument("--table", default=os.getenv("CHECK_TABLE", "sim_masterv2_v9"))
    parser.add_argument("--version-ids", default=os.getenv("CHECK_VERSION_IDS", "1,2"))
    parser.add_argument("--group-by", default=os.getenv("CHECK_GROUP_BY", "1,2"))
    args = parser.parse_args()

    version_ids = parse_int_list(args.version_ids)
    group_by = parse_int_list(args.group_by)

    check_superset_health(args.superset_url)
    token = login_superset(args.superset_url, args.superset_user, args.superset_password)
    check_database_registration(args.superset_url, token, args.superset_db_name)
    check_status4_granularity(
        host=args.ch_host,
        port=args.ch_port,
        user=args.ch_user,
        password=args.ch_password,
        database=args.ch_database,
        table=args.table,
        version_ids=version_ids,
        group_by=group_by
    )
    print("[check] All acceptance checks passed")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[check] FAILED: {exc}", file=sys.stderr)
        raise
