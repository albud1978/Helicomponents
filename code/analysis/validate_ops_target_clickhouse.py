#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import yaml
from clickhouse_driver import Client


ROOT_DIR = Path(__file__).resolve().parents[2]
ENV_PATH = ROOT_DIR / ".env"
DB_CONFIG_PATH = ROOT_DIR / "config" / "database_config.yaml"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def load_db_config(path: Path) -> Dict[str, object]:
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or "database" not in data:
        raise ValueError("Invalid database_config.yaml structure")
    return data


def build_client(db_cfg: Dict[str, object]) -> Client:
    database_cfg = db_cfg.get("database", {})
    env_cfg = database_cfg.get("env", {}) if isinstance(database_cfg, dict) else {}

    def env_or_default(env_key: str, default: object) -> object:
        return os.getenv(env_key, default) if env_key else default

    host = env_or_default(env_cfg.get("host_var", "CLICKHOUSE_HOST"), database_cfg.get("host"))
    port = int(env_or_default(env_cfg.get("port_var", "CLICKHOUSE_PORT"), database_cfg.get("port", 9000)))
    user = env_or_default(env_cfg.get("user_var", "CLICKHOUSE_USER"), database_cfg.get("user", "default"))
    password = env_or_default(env_cfg.get("password_var", "CLICKHOUSE_PASSWORD"), "")
    database = os.getenv("CLICKHOUSE_DATABASE", database_cfg.get("database", "default"))
    settings = database_cfg.get("settings", {}) if isinstance(database_cfg, dict) else {}

    if not host:
        raise ValueError("Missing ClickHouse host")
    if not password:
        raise ValueError("Missing ClickHouse password (CLICKHOUSE_PASSWORD)")

    return Client(
        host=str(host),
        port=port,
        user=str(user),
        password=str(password),
        database=str(database),
        settings=settings,
    )


def fetch_columns(client: Client, database: str, table: str) -> List[str]:
    rows = client.execute(
        """
        SELECT name
        FROM system.columns
        WHERE database = %(db)s
          AND table = %(table)s
        ORDER BY name
        """,
        {"db": database, "table": table},
    )
    return [row[0] for row in rows]


def pick_column(
    columns: List[str],
    label: str,
    exact_candidates: List[str],
    prefix_candidates: List[str],
) -> str:
    for name in exact_candidates:
        if name in columns:
            return name
    for prefix in prefix_candidates:
        for name in columns:
            if name.startswith(prefix):
                return name
    raise ValueError(f"Missing column for {label}. Available columns: {columns}")


def has_version_date_column(columns: List[str]) -> bool:
    return "version_date" in columns


def fetch_ops_vs_target(
    client: Client,
    version_date: str,
    version_date_int: int,
    sim_cols: Dict[str, str],
    fp_cols: Dict[str, str],
    fp_has_version_date: bool,
) -> List[Tuple[object, int, int, Optional[int], Optional[int]]]:
    """V9: group_by: 1=Mi-8, 2=Mi-17. Offset дня: 0 (StepController в начале → QM видит new_day)"""
    fp_where = ""
    if fp_has_version_date:
        fp_where = f"WHERE `{fp_cols['version_date']}` = toDate(%(version_date)s)"
    query = f"""
        SELECT
            agg.day,
            agg.mi8_ops,
            agg.mi17_ops,
            fp.mi8_target,
            fp.mi17_target
        FROM (
            SELECT
                s.`{sim_cols['day']}` AS day,
                sumIf(s.`{sim_cols['state']}` = 2, s.`{sim_cols['group_by']}` = 1) AS mi8_ops,
                sumIf(s.`{sim_cols['state']}` = 2, s.`{sim_cols['group_by']}` = 2) AS mi17_ops
            FROM sim_masterv2_v9 s
            WHERE s.`{sim_cols['version_date']}` = %(version_date_int)s
            GROUP BY s.`{sim_cols['day']}`
        ) AS agg
        ANY LEFT JOIN (
            SELECT
                dateDiff('day', toDate(%(version_date)s), `{fp_cols['day']}`) AS day_u16,
                `{fp_cols['mi8_target']}` AS mi8_target,
                `{fp_cols['mi17_target']}` AS mi17_target
            FROM flight_program_ac
            {fp_where}
        ) fp ON agg.day = fp.day_u16
        ORDER BY agg.day
    """
    return client.execute(
        query,
        {"version_date": version_date, "version_date_int": version_date_int},
    )


def fetch_steps_count(client: Client, version_date_int: int) -> int:
    rows = client.execute(
        """
        SELECT countDistinct(day_u16)
        FROM sim_masterv2_v9
        WHERE version_date = %(vdi)s
        """,
        {"vdi": version_date_int},
    )
    return int(rows[0][0]) if rows else 0


def format_table(rows: Iterable[List[str]]) -> str:
    rows = list(rows)
    if not rows:
        return ""
    widths = [max(len(row[idx]) for row in rows) for idx in range(len(rows[0]))]
    lines = []
    for row in rows:
        padded = [row[idx].ljust(widths[idx]) for idx in range(len(row))]
        lines.append(" | ".join(padded))
    return "\n".join(lines)


def to_int(value: Optional[object]) -> Optional[int]:
    if value is None:
        return None
    return int(value)


def build_violation_rows(
    rows: List[Tuple[object, int, int, Optional[int], Optional[int]]]
) -> List[List[str]]:
    header = [
        "day",
        "mi8_ops",
        "mi8_target",
        "mi8_delta",
        "mi17_ops",
        "mi17_target",
        "mi17_delta",
    ]
    output_rows: List[List[str]] = [header]
    for day, mi8_ops, mi17_ops, mi8_target, mi17_target in rows:
        mi8_ops_i = to_int(mi8_ops)
        mi17_ops_i = to_int(mi17_ops)
        mi8_target_i = to_int(mi8_target)
        mi17_target_i = to_int(mi17_target)

        mi8_delta = mi8_ops_i - mi8_target_i if mi8_target_i is not None else None
        mi17_delta = mi17_ops_i - mi17_target_i if mi17_target_i is not None else None

        is_violation = (
            mi8_target_i is None
            or mi17_target_i is None
            or mi8_delta != 0
            or mi17_delta != 0
        )
        if not is_violation:
            continue

        output_rows.append(
            [
                str(day),
                str(mi8_ops_i),
                str(mi8_target_i) if mi8_target_i is not None else "NA",
                str(mi8_delta) if mi8_delta is not None else "NA",
                str(mi17_ops_i),
                str(mi17_target_i) if mi17_target_i is not None else "NA",
                str(mi17_delta) if mi17_delta is not None else "NA",
            ]
        )
    return output_rows


def resolve_columns(client: Client, database: str) -> Tuple[Dict[str, str], Dict[str, str], bool]:
    sim_columns = fetch_columns(client, database, "sim_masterv2_v9")
    fp_columns = fetch_columns(client, database, "flight_program_ac")

    sim_cols = {
        "day": pick_column(sim_columns, "sim.day", ["day_u16", "day"], ["day"]),
        "state": pick_column(sim_columns, "sim.state", ["status_id", "state"], ["stat"]),
        "group_by": pick_column(sim_columns, "sim.group_by", ["group_by"], ["group_by"]),
        "version_date": pick_column(sim_columns, "sim.version_date", ["version_date"], ["version_date"]),
    }

    fp_cols = {
        "day": pick_column(fp_columns, "fp.dates", ["dates", "date", "day"], ["date", "day"]),
        "mi8_target": pick_column(fp_columns, "fp.ops_counter_mi8", ["ops_counter_mi8"], ["ops_counter_mi8"]),
        "mi17_target": pick_column(fp_columns, "fp.ops_counter_mi17", ["ops_counter_mi17"], ["ops_counter_mi17"]),
    }

    fp_has_version_date = has_version_date_column(fp_columns)
    if fp_has_version_date:
        fp_cols["version_date"] = "version_date"

    return sim_cols, fp_cols, fp_has_version_date


def main() -> None:
    load_env_file(ENV_PATH)
    db_cfg = load_db_config(DB_CONFIG_PATH)
    client = build_client(db_cfg)
    database = os.getenv("CLICKHOUSE_DATABASE", db_cfg["database"].get("database", "default"))
    sim_cols, fp_cols, fp_has_version_date = resolve_columns(client, database)

    version_dates = ["2025-07-04", "2025-12-30"]
    for version_date in version_dates:
        vd_int = int(version_date.replace("-", ""))
        rows = fetch_ops_vs_target(
            client,
            version_date,
            vd_int,
            sim_cols,
            fp_cols,
            fp_has_version_date,
        )
        steps_count = fetch_steps_count(client, vd_int)

        print(f"version_date: {version_date}")
        print(f"total steps checked: {steps_count}")

        violations_table = build_violation_rows(rows)
        if len(violations_table) == 1:
            print("all steps pass")
        else:
            print(format_table(violations_table))
        print("")


if __name__ == "__main__":
    main()
