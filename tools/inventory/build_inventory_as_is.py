#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Сбор AS-IS инвентаризации источников данных в docs/inventory_as_is.md
Только SELECT / read-only к ClickHouse; без Float64 в собственных вычислениях.
"""
from __future__ import annotations

import csv
import os
import re
import subprocess
import sys
import urllib.request
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

# --- repo root + config_loader (Project CH) ---
REPO = Path(__file__).resolve().parents[2]
if str(REPO / "code") not in sys.path:
    sys.path.insert(0, str(REPO / "code"))
from utils.config_loader import auto_load_env_file, get_clickhouse_client  # noqa: E402

# --- constants ---
AMOS_TABLES: Tuple[str, ...] = (
    "rotables",
    "aircraft",
    "part",
    "part_requirement",
    "requirement_type",
    "requirement_header",
    "event_effectivity",
    "event_effectivity_rules",
    "event_effectivity_sns",
    "applicability",
    "treq_interval",
    "treq_time_requirement",
    "treq_event_link",
    "wo_event_link",
    "wo_header",
    "wo_transfer",
    "wo_transfer_dimension",
    "history",
    "od_detail",
    "location",
    "address",
    "ac_typ",
    "condition",
    "part_special",
    "counter_definition",
)
AMOS_CSV = REPO / "data_input/analytics/Database-Description39331920032025_0.csv"
EXCEL_PATHS: Tuple[Path, ...] = (
    REPO / "data_input/source_data/v_2026-04-08/Status_Components.xlsx",
    REPO / "data_input/source_data/v_2026-04-08/Status_Components_DWH.xlsx",
    REPO / "data_input/source_data/v_2026-04-08/Status_Overhaul.xlsx",
    REPO / "data_input/source_data/v_2026-04-08/Program_AC.xlsx",
    REPO / "data_input/source_data/v_2026-04-08/Program.xlsx",
    REPO / "data_input/source_data/v_2026-04-08/Program_heli.xlsx",
    REPO / "data_input/master_data/MD_Сomponents.xlsx",
)
DWH_SCHEMAS: Tuple[str, ...] = ("reports", "staging", "source", "analytics", "integrated", "business")
YC_CA_URL = "https://storage.yandexcloud.net/cloud-certs/CA.pem"


def md_escape_cell(s: str) -> str:
    if s is None:
        return ""
    s = s.replace("\r", " ").replace("\n", " ")
    s = s.replace("|", "\\|")
    return s


def safe_ident(name: str) -> str:
    if not re.match(r"^[A-Za-z0-9_]+$", name or ""):
        raise ValueError(f"unsafe identifier: {name!r}")
    return name


def sql_str_literal(s: str) -> str:
    return "'" + str(s).replace("\\", "\\\\").replace("'", "''") + "'"


def backtick_ident(n: str) -> str:
    return "`" + str(n).replace("`", r"\`") + "`"


def table_ref_qualified(database: str, table: str) -> str:
    return f"{backtick_ident(database)}.{backtick_ident(table)}"


def git_branch() -> str:
    try:
        p = subprocess.run(
            ["git", "-C", str(REPO), "rev-parse", "--abbrev-ref", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if p.returncode == 0 and p.stdout.strip():
            return p.stdout.strip()
    except OSError:
        pass
    return "unknown"


def ch_project_tables_meta(client) -> List[dict]:
    q = """
    SELECT name, engine, partition_key, sorting_key, primary_key, total_rows, total_bytes, create_table_query
    FROM system.tables
    WHERE database = 'default'
    ORDER BY name
    """
    rows = client.execute(q)
    cols = "name,engine,partition_key,sorting_key,primary_key,total_rows,total_bytes,create_table_query".split(
        ","
    )
    return [dict(zip(cols, r)) for r in rows]


def ch_project_columns(client, table: str) -> List[dict]:
    ts = sql_str_literal(table)
    q = f"""
    SELECT name, type, default_expression, is_in_primary_key, is_in_sorting_key, is_in_partition_key, comment
    FROM system.columns
    WHERE database = 'default' AND table = {ts}
    ORDER BY position
    """
    rows = client.execute(q)
    out = []
    for r in rows:
        out.append(
            {
                "name": r[0],
                "type": r[1],
                "default_expression": r[2] or "",
                "is_in_primary_key": r[3],
                "is_in_sorting_key": r[4],
                "is_in_partition_key": r[5],
                "comment": r[6] or "",
            }
        )
    return out


def table_has_column(client, table: str, col: str) -> bool:
    q = (
        "SELECT count() FROM system.columns WHERE database = 'default' AND table = "
        + sql_str_literal(table)
        + " AND name = "
        + sql_str_literal(col)
    )
    r = client.execute(q)
    return int(r[0][0]) > 0


def version_stats(
    client, table: str, has_vd: Optional[bool] = None,
) -> Tuple[Optional[dict], str]:
    """Returns (stats dict or None, note). No Float64: counts as int only."""
    if has_vd is None:
        has_vd = table_has_column(client, table, "version_date")
    if has_vd:
        tref = table_ref_qualified("default", table)
        q = f"""
        SELECT count() AS rows,
               uniq(version_date) AS distinct_versions,
               min(version_date) AS first_version,
               max(version_date) AS last_version
        FROM {tref}"""
        r = client.execute(q)[0]
        return (
            {
                "rows": int(r[0]),
                "distinct_versions": int(r[1]),
                "first_version": r[2],
                "last_version": r[3],
            },
            "version_date",
        )
    tref = table_ref_qualified("default", table)
    r = client.execute(f"SELECT count() FROM {tref}")[0]
    return ({"rows": int(r[0])}, "no version_date")


def group_by_stats(
    client, table: str, has_group_by: Optional[bool] = None,
) -> List[Tuple[Any, int]]:
    if has_group_by is None:
        has_group_by = table_has_column(client, table, "group_by")
    if not has_group_by:
        return []
    tref = table_ref_qualified("default", table)
    q = f"""
    SELECT group_by, count() AS cnt FROM {tref} GROUP BY group_by ORDER BY group_by LIMIT 10
    """
    rows = client.execute(q)
    return [(r[0], int(r[1])) for r in rows]


def ensure_yc_ca_pem() -> Optional[Path]:
    env_path = os.environ.get("DWH_CLICKHOUSE_CA_CERT", "").strip()
    candidates: List[Path] = []
    if env_path:
        candidates.append(Path(env_path).expanduser())
    candidates.append(Path("/tmp/yc_root_ca.pem"))
    for p in candidates:
        if p.is_file() and p.stat().st_size > 0:
            return p
    target = Path("/tmp/yc_root_ca.pem")
    try:
        urllib.request.urlretrieve(YC_CA_URL, str(target))  # noqa: S310 — fixed YC URL
    except OSError:
        return None
    if target.is_file() and target.stat().st_size > 0:
        return target
    return None


def get_dwh_client_impl(forced_verify: Optional[bool] = None) -> Tuple[Any, bool, Optional[Path]]:
    """Returns (client, verify_used, ca_path_or_none)."""
    import clickhouse_connect

    host = os.environ.get("DWH_CLICKHOUSE_HOST", "").strip()
    if not host:
        raise RuntimeError("DWH_CLICKHOUSE_HOST не задан")
    user = os.environ.get("DWH_CLICKHOUSE_USER", "").strip()
    password = os.environ.get("DWH_CLICKHOUSE_PASSWORD", "")
    dbase = os.environ.get("DWH_CLICKHOUSE_DATABASE", "default")
    port = int(os.environ.get("DWH_CLICKHOUSE_PORT", "8443"))
    if forced_verify is not None:
        verify = forced_verify
    else:
        verify = os.environ.get("DWH_CLICKHOUSE_VERIFY", "true").lower() in (
            "1",
            "true",
            "yes",
        )
    ca = ensure_yc_ca_pem()
    kwargs: Dict[str, Any] = {
        "host": host,
        "port": port,
        "username": user,
        "password": password,
        "database": dbase,
        "interface": "https",
        "secure": True,
    }
    if verify and ca is not None:
        kwargs["verify"] = True
        kwargs["ca_cert"] = str(ca)
    else:
        kwargs["verify"] = False
    return clickhouse_connect.get_client(**kwargs), verify, ca


def dwh_query(client, sql: str) -> List[Tuple]:
    r = client.query(sql)
    return list(r.result_rows)


def parse_amos_description_csv(
    path: Path, wanted: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    wanted_l = {w.lower() for w in wanted}
    found: Dict[str, Dict[str, Any]] = {}
    if not path.is_file():
        return found
    with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
        lines = list(csv.reader(f, delimiter=";"))
    i = 0
    while i < len(lines):
        row = lines[i]
        if not row or not row[0].strip().startswith("Table "):
            i += 1
            continue
        m = re.match(r"^Table\s+(\S+)", row[0].strip())
        if not m:
            i += 1
            continue
        tname = m.group(1).lower()
        i += 1
        if tname not in wanted_l:
            while i < len(lines) and (
                not lines[i]
                or not lines[i][0].strip().startswith("Table ")
            ):
                i += 1
            continue
        desc = ""
        if i < len(lines):
            desc = (lines[i][0] or "").strip()
        i += 1
        if i < len(lines) and lines[i] and (lines[i][0] or "").strip() == "Keys":
            header = lines[i]
            i += 1
        else:
            header = ["Keys", "Name", "Mime-Type", "Type", "Description"]
        col_rows: List[List[str]] = []
        while i < len(lines) and (
            not lines[i] or not lines[i][0].strip().startswith("Table ")
        ):
            if lines[i]:
                col_rows.append(lines[i])
            i += 1
        found[tname] = {
            "description": desc,
            "header": header,
            "data_rows": col_rows,
        }
    return found


def rel_to_repo(path: Path) -> str:
    try:
        return f"`{path.relative_to(REPO).as_posix()}`"
    except ValueError:
        return f"`{path.as_posix()}`"


def read_excel_section(path: Path, section_no: int) -> str:
    import pandas as pd  # type: ignore

    rel = rel_to_repo(path)
    if not path.is_file():
        return (
            f"### 4.{section_no} (отсутствует) {path.name}\n\n"
            f"- Path: {rel}\n"
            f"- **Файл не найден на момент инвентаризации.**\n\n"
        )
    xl = pd.ExcelFile(path, engine="openpyxl")
    name0 = xl.sheet_names[0]
    full = pd.read_excel(path, engine="openpyxl", sheet_name=name0)
    sample = pd.read_excel(path, engine="openpyxl", nrows=5, sheet_name=name0)
    nrows = int(len(full))
    lines = [f"### 4.{section_no} {path.name}"]
    lines.append(f"- Path: {rel}")
    lines.append(f"- Sheet (default): {name0}")
    lines.append(f"- Rows (data): {nrows}")
    lines.append(f"- Columns (count): {len(full.columns)}")
    lines.append("")
    lines.append("| Колонка | dtype |")
    lines.append("|---|---|")
    for c in full.columns:
        st = str(c)
        dt = str(sample[c].dtype) if c in sample.columns else str(full[c].dtype)
        lines.append(f"| {md_escape_cell(st)} | {md_escape_cell(dt)} |")
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    auto_load_env_file()
    out_path = REPO / "docs/inventory_as_is.md"
    inv_date = date.today().isoformat()
    branch = git_branch()
    ch_host = os.environ.get("CLICKHOUSE_HOST", "10.95.19.132")
    ch_port = int(os.environ.get("CLICKHOUSE_PORT", "9000"))
    ch_user = os.environ.get("CLICKHOUSE_USER", "default")
    ch_db = os.environ.get("CLICKHOUSE_DATABASE", "default")
    dwh_host = "rc1a-fhb99q2hquq89uhp.mdb.yandexcloud.net"
    dwh_port = 8443

    lines: List[str] = [
        "# Инвентаризация AS-IS: источники данных для имитационной модели Helicomponents",
        "",
        f"> Дата: {inv_date}",
        f"> Branch: {branch}",
        "> Скоуп: только AS-IS, без интерпретации/рефакторинга/предложений.",
        "",
        f"## 1. Project ClickHouse ({ch_host}:{ch_port} / {ch_db})",
        f"> Соединение: user **{ch_user}** (пароль не публикуется).",
        "",
    ]

    # ---- Project CH ----
    ch = get_clickhouse_client()
    tables = ch_project_tables_meta(ch)
    table_names = [r["name"] for r in tables]
    cols_index: Dict[str, List[dict]] = {}
    for r in tables:
        nm = r["name"]
        cols_index[nm] = ch_project_columns(ch, nm)

    colsets = {n: {c["name"] for c in cols_index[n]} for n in table_names}
    n_with_vd = sum(1 for n in table_names if "version_date" in colsets.get(n, set()))
    n_with_gb = sum(1 for n in table_names if "group_by" in colsets.get(n, set()))
    total_bytes = sum(int(r.get("total_bytes") or 0) for r in tables)

    for idx, r in enumerate(tables, start=1):
        name = r["name"]
        cols = cols_index[name]
        cset = colsets.get(name, set())
        st_extra: List[str] = []
        has_vd = "version_date" in cset
        has_g = "group_by" in cset
        try:
            vstat, st_note = version_stats(ch, name, has_vd=has_vd)
        except Exception as e:
            st_extra.append(
                f"- version/`count()`: **ошибка запроса** — "
                f"`{type(e).__name__}`: {md_escape_cell(str(e)[:200])}"
            )
            vstat, st_note = None, "error"
        if vstat and st_note == "version_date":
            first_d = vstat.get("first_version")
            last_d = vstat.get("last_version")
            st_extra.append(
                f"- Distinct `version_date`: {vstat.get('distinct_versions', 0)} "
                f"(период {first_d!s} .. {last_d!s}) | `count()`: {vstat.get('rows', 0)}"
            )
        elif vstat and st_note == "no version_date":
            st_extra.append(
                f"- `count()`: {vstat.get('rows', 0)} | **нет `version_date`** (колонка отсутствует)"
            )
        if has_g and st_note != "error":
            try:
                grows = group_by_stats(ch, name, has_group_by=True)
            except Exception as e:
                st_extra.append(
                    f"- `group_by` сводка: **ошибка** — "
                    f"`{type(e).__name__}`: {md_escape_cell(str(e)[:200])}"
                )
                grows = []
            if grows:
                gtxt = ", ".join(f"{a!s}={b}" for a, b in grows)
                st_extra.append(
                    f"- `group_by` (до 10 групп, ORDER BY `group_by`): {gtxt}"
                )
        cquery = (r.get("create_table_query") or "").rstrip()
        cquery_md = cquery.replace("```", "`\u200b``")
        lines.append(f"### 1.{idx} `{name}`")
        lines.append(f"- Engine: {md_escape_cell(str(r.get('engine') or ''))}")
        lines.append(f"- Partition key: {md_escape_cell(str(r.get('partition_key') or ''))}")
        lines.append(f"- Sorting key: {md_escape_cell(str(r.get('sorting_key') or ''))}")
        lines.append(f"- Primary key: {md_escape_cell(str(r.get('primary_key') or ''))}")
        lines.append(f"- Total rows: {r.get('total_rows')!s}")
        lines.append(f"- Total bytes: {r.get('total_bytes')!s}")
        for se in st_extra:
            lines.append(se)
        lines.append("")
        lines.append("| Колонка | Тип | Default | PK | Sort | Partition | Comment |")
        lines.append("|---|---|---|:---|:---|:---:|---|")
        for c in cols:
            lines.append(
                "| "
                + " | ".join(
                    [
                        md_escape_cell(str(c["name"])),
                        md_escape_cell(str(c["type"])),
                        md_escape_cell(str(c["default_expression"])),
                        str(c["is_in_primary_key"]),
                        str(c["is_in_sorting_key"]),
                        str(c["is_in_partition_key"]),
                        md_escape_cell(str(c["comment"])),
                    ]
                )
                + " |"
            )
        lines.append("")
        lines.append("<details><summary>create_table_query</summary>")
        lines.append("")
        lines.append("```sql")
        lines.append(cquery_md)
        lines.append("```")
        lines.append("</details>")
        lines.append("")

    # ---- DWH ----
    dwh_unavailable: Optional[str] = None
    dwh_schemas_active: List[str] = []
    dwh_k_tables = 0
    dwh_verify_state = "N/A"
    dwh_ch = None
    dwh_stats: Dict[str, Any] = {"db_counts": []}
    dwh_verify_insecure = False
    used_ca: Optional[Path] = None

    try:
        dwh_ch, used_verify, used_ca = get_dwh_client_impl()
        dwh_verify_state = f"verify={used_verify!s}, ca={'yes' if used_ca is not None else 'no'}"
    except Exception as e1:
        try:
            dwh_ch, used_verify, used_ca = get_dwh_client_impl(forced_verify=False)
            dwh_verify_insecure = True
            dwh_verify_state = f"verify=False (fallback, первичная ошибка: {type(e1).__name__}), ca=ignored"
        except Exception as e2:
            dwh_unavailable = f"{type(e1).__name__}: {e1}; retry verify=False: {type(e2).__name__}: {e2}"

    lines.append("## 2. DWH ClickHouse (Yandex Managed: HTTPS+TLS)")
    lines.append(
        f"> Хост/порт/user/db (без пароля): **{dwh_host}:{dwh_port}**, "
        f"user **{os.environ.get('DWH_CLICKHOUSE_USER', 'N/A')!s}**, "
        f"default database **{os.environ.get('DWH_CLICKHOUSE_DATABASE', 'default')!s}**"
    )
    if dwh_unavailable or dwh_ch is None:
        reason = dwh_unavailable or "клиент не инициализирован"
        lines.append("")
        lines.append(
            f"**DWH ClickHouse: недоступен на момент инвентаризации, причина: {md_escape_cell(reason)}**"
        )
        lines.append("")
    else:
        lines.append(f"> TLS: {dwh_verify_state}")
        if dwh_verify_insecure:
            lines.append(
                "> **Внимание:** для подключения использован `verify=False` (fallback, только метаданные)."
            )
        lines.append("")
        try:
            db_counts = dwh_query(
                dwh_ch,
                """
                SELECT database, count(*) AS tables_count
                FROM system.tables
                WHERE database NOT IN ('system','INFORMATION_SCHEMA','information_schema')
                GROUP BY database
                ORDER BY database
                """,
            )
            dwh_stats["db_counts"] = db_counts
            lines.append("### 2.0 Схемы (количество таблиц, `system.tables`)")
            lines.append("")
            lines.append("| database | tables_count |")
            lines.append("|---|---:|")
            for dbc, tc in db_counts:
                lines.append(
                    f"| {md_escape_cell(str(dbc))} | {int(tc)} |"
                )
            lines.append("")
            have = {r[0] for r in db_counts}
            dwh_schemas_active = [s for s in DWH_SCHEMAS if s in have]
            for sch in dwh_schemas_active:
                sch_safe = safe_ident(sch)
                trows = dwh_query(
                    dwh_ch,
                    f"""
                    SELECT name, engine, partition_key, sorting_key, primary_key, total_rows, total_bytes, create_table_query
                    FROM system.tables
                    WHERE database = '{sch_safe}'
                    ORDER BY name
                    """,
                )
                dwh_k_tables += len(trows)
                for tr in trows:
                    tname, eng, pkey, skey, pk, trows_c, tbytes, ctq = tr
                    lines.append(f"### `{sch_safe}`.`{tname}`")
                    lines.append(f"- Engine: {md_escape_cell(str(eng or ''))}")
                    lines.append(
                        f"- Partition key: {md_escape_cell(str(pkey or ''))}"
                    )
                    lines.append(
                        f"- Sorting key: {md_escape_cell(str(skey or ''))}"
                    )
                    lines.append(
                        f"- Primary key: {md_escape_cell(str(pk or ''))}"
                    )
                    lines.append(f"- Total rows: {trows_c!s}")
                    lines.append(f"- Total bytes: {tbytes!s}")
                    tlit = sql_str_literal(str(tname))
                    dblit = sql_str_literal(sch_safe)
                    cd = dwh_query(
                        dwh_ch,
                        f"""
                        SELECT name, type, default_expression, is_in_primary_key, is_in_sorting_key, is_in_partition_key, comment
                        FROM system.columns
                        WHERE database = {dblit} AND table = {tlit}
                        ORDER BY position
                        """,
                    )
                    lines.append("")
                    lines.append(
                        "| Колонка | Тип | Default | PK | Sort | Partition | Comment |"
                    )
                    lines.append("|---|---|---|:---|:---|:---:|---|")
                    for c in cd:
                        lines.append(
                            "| "
                            + " | ".join(
                                [
                                    md_escape_cell(str(c[0])),
                                    md_escape_cell(str(c[1])),
                                    md_escape_cell(
                                        str(c[2] if c[2] is not None else "")
                                    ),
                                    str(c[3]),
                                    str(c[4]),
                                    str(c[5]),
                                    md_escape_cell(
                                        str(c[6] if c[6] is not None else "")
                                    ),
                                ]
                            )
                            + " |"
                        )
                    lines.append("")
                    lines.append(
                        "<details><summary>create_table_query</summary>\n"
                    )
                    lines.append("")
                    lines.append("```sql")
                    lines.append(
                        (str(ctq or "").rstrip()).replace("```", "`\u200b``")
                    )
                    lines.append("```")
                    lines.append("</details>")
                    lines.append("")

            mviews: List[Tuple] = []
            if dwh_schemas_active:
                mviews = dwh_query(
                    dwh_ch,
                    f"""
                    SELECT database, name, engine, create_table_query
                    FROM system.tables
                    WHERE database IN ({", ".join("'" + safe_ident(s) + "'" for s in dwh_schemas_active)})
                      AND engine LIKE '%View%'
                    ORDER BY database, name
                    """,
                )
            lines.append(
                "### 2.MV Материализованные представления (`engine` LIKE '%View%')"
            )
            if not dwh_schemas_active:
                lines.append(
                    "*Ни одна схема из набора {reports, staging, source, analytics, integrated, business} не найдена на кластере — запрос к MV пропущен.*"
                )
            elif not mviews:
                lines.append("*(нет в проинвентаризированных схемах)*")
            else:
                for mv in mviews:
                    d_, n_, e_, q_ = mv[0], mv[1], mv[2], mv[3]
                    lines.append(
                        f"- **`{d_}`.`{n_}`** — engine: `{md_escape_cell(str(e_))}`"
                    )
                    lines.append(
                        f"<details><summary>create_table_query</summary>\n\n```sql"
                    )
                    lines.append(
                        (str(q_ or "").rstrip()).replace("```", "`\u200b``")
                    )
                    lines.append("```\n</details>\n")
            lines.append("")

            # 2.4 сравнение с xlsx
            pri_tbl = "amos_heli_rotables_components_status"
            xlp = REPO / "data_input/source_data/v_2026-04-08/Status_Components_DWH.xlsx"
            if "reports" in dwh_schemas_active:
                cnames = dwh_query(
                    dwh_ch,
                    f"""
                    SELECT name FROM system.columns
                    WHERE database = 'reports' AND table = {sql_str_literal(pri_tbl)} ORDER BY position
                    """,
                )
                dwh_cols = [r[0] for r in cnames]
                lines.append(
                    "### 2.4 `reports."
                    + pri_tbl
                    + "` — сопоставление с `Status_Components_DWH.xlsx`"
                )
                if not xlp.is_file():
                    lines.append(
                        f"- xlsx **не найден** ({rel_to_repo(xlp)}), сравнение не выполнено."
                    )
                    lines.append(
                        f"- Колонок в DWH: **{len(dwh_cols)}**."
                    )
                else:
                    import pandas as pd  # type: ignore
                    s = pd.read_excel(xlp, engine="openpyxl", nrows=0)
                    xcols = [str(c) for c in s.columns]
                    all_n = sorted(set(dwh_cols) | set(xcols))
                    lines.append("")
                    lines.append("| column | в DWH | в xlsx | совпадает |")
                    lines.append("|---|---|---|:---:|")
                    for c in all_n:
                        d = c in dwh_cols
                        x = c in xcols
                        ok = d and x
                        lines.append(
                            f"| {md_escape_cell(c)} | {d!s} | {x!s} | {ok!s} |"
                        )
                    lines.append("")
            else:
                lines.append(
                    "### 2.4 `reports."
                    + pri_tbl
                    + "` — сопоставление с `Status_Components_DWH.xlsx`"
                )
                lines.append(
                    "- Схема `reports` не обнаружена на кластере (не входит в набор схем **или** пуста) — сравнение с xlsx не выполнялось."
                )
                lines.append("")

        except Exception as e:
            lines.append(
                f"**DWH: ошибка при чтении метаданных: {type(e).__name__}: {md_escape_cell(str(e))}**"
            )
            lines.append("")

    # ---- AMOS CSV ----
    lines.append("## 3. AMOS source-таблицы (из `data_input/analytics/Database-Description39331920032025_0.csv`)")
    amos = parse_amos_description_csv(AMOS_CSV, AMOS_TABLES)
    for i, t in enumerate(AMOS_TABLES, start=1):
        lines.append(f"### 3.{i} `{t}`")
        if t not in amos:
            lines.append("**не найдено в Database-Description CSV** (в пределах парсера блоков `Table …`).")
            lines.append("")
            continue
        desc = amos[t].get("description", "")
        if desc:
            lines.append(f"> {md_escape_cell(desc)}")
        hdr = amos[t].get("header", [])
        drows = amos[t].get("data_rows", [])
        hline = " | ".join(md_escape_cell(str(h)) for h in hdr) if hdr else "Keys | Name | Mime-Type | Type | Description"
        lines.append("")
        lines.append(f"| {hline} |")
        lines.append("|" + "|".join(["---"] * max(5, len(hdr) if hdr else 5)) + "|")
        for dr in drows:
            if not dr:
                continue
            pad = list(dr) + [""] * 8
            cut = pad[:5]
            lines.append(
                "| " + " | ".join(md_escape_cell(str(x) if x is not None else "") for x in cut) + " |"
            )
        lines.append("")

    # ---- Excel ----
    lines.append("## 4. Excel-файлы (data_input/source_data/v_2026-04-08 + data_input/master_data)")
    for i, p in enumerate(EXCEL_PATHS, start=1):
        lines.append(read_excel_section(p, i))

    # ---- Summary ----
    m_found = len([t for t in AMOS_TABLES if t in amos])
    gb_ok = 1024 * 1024 * 1024
    if total_bytes and total_bytes >= gb_ok:
        approx_gb = f"{(total_bytes / float(gb_ok)):.2f} GB"
    else:
        approx_gb = f"{(total_bytes / 1024.0 / 1024.0):.2f} MB" if total_bytes else "0"
    n_proj = len(tables)
    lines.append("## 5. Сводная статистика")
    lines.append("")
    lines.append(
        f"- Project ClickHouse ({ch_host}:{ch_port}/default): **{n_proj}** таблиц, "
        f"суммарный размер (total_bytes) ≈ **{approx_gb}**"
    )
    if dwh_ch and not dwh_unavailable and dwh_k_tables >= 0:
        schem_txt = ", ".join(dwh_schemas_active) if dwh_schemas_active else "нет"
        lines.append(
            f"- DWH ClickHouse ({dwh_host}/default): **{dwh_k_tables}** таблиц "
            f"в проинвентаризированных схемах ({schem_txt})"
        )
    else:
        lines.append(
            f"- DWH ClickHouse ({dwh_host}/default): **н/д** (см. секцию 2; проинвентаризировано 0, если нет соединения)"
        )
    lines.append(
        f"- Таблиц с колонкой `version_date` (в `default` Project CH): **{n_with_vd}**"
    )
    lines.append(
        f"- Таблиц с колонкой `group_by` (в `default` Project CH): **{n_with_gb}**"
    )
    lines.append(
        f"- AMOS-таблиц из списка ТЗ, найдено в CSV: **{m_found}** из **{len(AMOS_TABLES)}**"
    )
    missing_amos = [t for t in AMOS_TABLES if t not in amos]
    if missing_amos:
        lines.append(
            f"  - Не найдены в CSV: {', '.join('`' + m + '`' for m in missing_amos)}"
        )
    lines.append(
        f"- Excel-файлов по путям: **7** (факт чтения — см. секцию 4; отсутствующие отмечены)"
    )
    lines.append(f"- Дата инвентаризации: **{inv_date}**")
    lines.append(
        "- Источники: `.env`, `config/database_config.yaml`, "
        "`data_input/analytics/Database-Description39331920032025_0.csv`, "
        "`data_input/source_data/v_2026-04-08/`, `data_input/master_data/`"
    )
    lines.append("")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {out_path} ({out_path.stat().st_size} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
