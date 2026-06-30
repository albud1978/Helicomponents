#!/usr/bin/env python3
"""Экспорт из DWH ClickHouse в Excel для сравнения с golden v_YYYY-MM-DD (replay / direct load)."""
from __future__ import annotations

import argparse
import os
import sys
import warnings
from pathlib import Path

import numpy as np
import pandas as pd

from prep_source_dataset import LEASE_RESTRICTED_OWNERS

DEFAULT_REPORT_DATE = "2026-04-08"
AMOS_DAY_ORIGIN = pd.Timestamp("1971-12-31")


def _as_of_end(report_date: str) -> str:
    return f"toDateTime('{report_date} 23:59:59')"


def _as_of_start(report_date: str) -> str:
    return f"toDateTime('{report_date} 00:00:00')"


def _allowed_owners_sql_in() -> str:
    """Whitelist owner для program_ac / status_overhaul — как в dual_loader.prepare_data()."""
    from extract.dual_loader import ALLOWED_OWNERS

    return ", ".join(f"'{owner}'" for owner in sorted(ALLOWED_OWNERS))


def default_out_subdir(report_date: str, *, prefix: str = "dwh_replay") -> str:
    """Каталог под output/, например dwh_replay_v2026-04-08."""
    return f"{prefix}_v{report_date}"

REQUIRED_ENV = (
    "DWH_CLICKHOUSE_HOST",
    "DWH_CLICKHOUSE_PORT",
    "DWH_CLICKHOUSE_DATABASE",
    "DWH_CLICKHOUSE_USER",
    "DWH_CLICKHOUSE_PASSWORD",
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _fail(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(1)


def _env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y")


def dwh_client():
    import clickhouse_connect

    for k in REQUIRED_ENV:
        if not os.getenv(k):
            _fail(f"Отсутствует переменная окружения {k}")
    kwargs = dict(
        host=os.environ["DWH_CLICKHOUSE_HOST"],
        port=int(os.environ["DWH_CLICKHOUSE_PORT"]),
        username=os.environ["DWH_CLICKHOUSE_USER"],
        password=os.environ["DWH_CLICKHOUSE_PASSWORD"],
        database=os.environ["DWH_CLICKHOUSE_DATABASE"],
        secure=_env_bool("DWH_CLICKHOUSE_SECURE", True),
        verify=_env_bool("DWH_CLICKHOUSE_VERIFY", True),
    )
    ca = os.getenv("DWH_CLICKHOUSE_CA_CERT")
    if ca:
        kwargs["ca_cert"] = ca
    return clickhouse_connect.get_client(**kwargs)


def _lease_col(owners: pd.Series) -> pd.Series:
    s = owners.map(lambda x: "" if pd.isna(x) else str(x).strip())
    return np.where(s.isin(LEASE_RESTRICTED_OWNERS), "Y", "")


def _fmt_dmY_series(s: pd.Series) -> pd.Series:
    def one(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return ""
        if pd.isna(v):
            return ""
        if hasattr(v, "strftime"):
            return v.strftime("%d.%m.%Y")
        ts = pd.to_datetime(v, errors="coerce")
        if pd.isna(ts):
            return ""
        return ts.strftime("%d.%m.%Y")

    return s.map(one)


def _amos_int_to_timestamp(s: pd.Series) -> pd.Series:
    def one(v):
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return pd.NaT
        try:
            iv = int(v)
        except (TypeError, ValueError):
            return pd.NaT
        if iv <= 0:
            return pd.NaT
        return AMOS_DAY_ORIGIN + pd.Timedelta(days=iv)

    return s.map(one)


def _ac_typ_case_sql(col: str) -> str:
    return f"""multiIf(
  {col} = 'МИ8', 'Ми-8Т',
  {col} IN ('МИ8АМТ', 'МИ8МТВ'), 'Ми-17',
  {col}
)"""


def _status_components_sql(report_date: str) -> str:
    return f"""
SELECT
  partno,
  partseqno_i,
  serialno,
  psn,
  ac_typ,
  ac_type_i,
  location,
  LL AS ll,
  OH AS oh,
  OH_threshold AS oh_threshold,
  sne,
  ppr,
  mfg_date,
  oh_at_date,
  shop_visit_counter,
  owner,
  address_i,
  condition,
  removal_date,
  target_date
FROM reports.amos_heli_rotables_components_status
WHERE report_date = toDate('{report_date}')
"""


def _program_ac_sql(
    report_date: str,
    *,
    strict_status: bool = True,
    ac_registrs: list[int] | None = None,
) -> str:
    ae, ast = _as_of_end(report_date), _as_of_start(report_date)
    status_filter = "  AND a.status = 0\n" if strict_status else ""
    regs_filter = ""
    if ac_registrs:
        in_list = ",".join(str(int(value)) for value in sorted(set(ac_registrs)))
        regs_filter = (
            "  AND toInt64OrZero(trimBoth(a.ac_registr)) "
            f"IN ({in_list})\n"
        )
    return f"""
WITH
snapshot_addr_raw AS (
  SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY address_i ORDER BY valid_from DESC) AS rn
    FROM source.amos_heli_address
    WHERE valid_from <= {ae} AND (valid_to IS NULL OR valid_to > {ast})
  ) WHERE rn = 1
),
snapshot_addr AS (
  SELECT vendor, any(address_i) AS address_i, any(name) AS name
  FROM snapshot_addr_raw
  GROUP BY vendor
),
snapshot_air AS (
  SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY ac_registr ORDER BY valid_from DESC) AS rn
    FROM source.amos_heli_aircraft
    WHERE valid_from <= {ae} AND (valid_to IS NULL OR valid_to > {ast})
  ) WHERE rn = 1
),
snapshot_spec AS (
  SELECT * FROM (
    SELECT *, row_number() OVER (
      PARTITION BY address_i, coalesce(special, '')
      ORDER BY valid_from DESC
    ) AS rn
    FROM source.amos_heli_adr_special
    WHERE valid_from <= {ae} AND (valid_to IS NULL OR valid_to > {ast})
  ) WHERE rn = 1
)
SELECT
  toInt64OrZero(trimBoth(a.ac_registr)) AS ac_registr,
  {_ac_typ_case_sql("a.ac_typ")} AS ac_typ,
  if(a.object_type = 'H', 'HELICOPTER', coalesce(a.object_type, '')) AS object_type,
  coalesce(a.description, '') AS description,
  coalesce(a.owner, '') AS owner,
  coalesce(a.manual_owner, '') AS operator,
  coalesce(a.homebase, '') AS homebase,
  coalesce(ad.name, '') AS homebase_name,
  coalesce(sp.remarks, '') AS directorate
FROM snapshot_air a
LEFT JOIN snapshot_addr ad ON ad.vendor = a.homebase
LEFT JOIN snapshot_spec sp
  ON sp.address_i = ad.address_i
  AND upperUTF8(replaceAll(trim(BOTH ' ' FROM coalesce(sp.special, '')), ' ', '')) = 'ДИРЕКЦ'
WHERE a.ac_typ IN ('МИ8', 'МИ8АМТ', 'МИ8МТВ')
  AND a.manual_owner = 'ЮТ-ВУ'
  AND coalesce(a.owner, '') IN ({_allowed_owners_sql_in()})
{status_filter}{regs_filter}  AND a.non_managed = 'N'
ORDER BY ac_registr
"""


def _status_overhaul_sql(report_date: str) -> str:
    ae, ast = _as_of_end(report_date), _as_of_start(report_date)
    return f"""
WITH
snapshot_air AS (
  SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY ac_registr ORDER BY valid_from DESC) AS rn
    FROM source.amos_heli_aircraft
    WHERE valid_from <= {ae} AND (valid_to IS NULL OR valid_to > {ast})
  ) WHERE rn = 1
),
wp_snap AS (
  SELECT * FROM (
    SELECT *, row_number() OVER (
      PARTITION BY toString(ac_registr), wpno
      ORDER BY valid_from DESC
    ) AS rn
    FROM source.amos_heli_wp_header
    WHERE valid_from <= {ae} AND (valid_to IS NULL OR valid_to > {ast})
  ) WHERE rn = 1
)
SELECT
  toInt64OrZero(trimBoth(wp.ac_registr)) AS ac_registr,
  {_ac_typ_case_sql("wp.ac_typ")} AS ac_typ,
  toString(wp.wpno) AS wpno,
  coalesce(wp.description, '') AS description,
  wp.start_date,
  wp.end_date,
  wp.act_start_date,
  wp.act_end_date,
  wp.wp_status AS status_code,
  coalesce(a.owner, '') AS owner,
  coalesce(a.manual_owner, '') AS operator
FROM wp_snap wp
INNER JOIN snapshot_air a ON toString(a.ac_registr) = toString(wp.ac_registr)
WHERE (
  /* КР: как в исходном WP_heli + golden v_2026-04-08 — точное совпадение после снятия пробелов. Широкие ILIKE '%КАПИТАЛЬНЫЙ%РЕМОНТ%' в CH дают лишние WP; хвост «(доп.работы)» — см. README. */
  (wp.hidden = 'H' AND upperUTF8(replaceAll(coalesce(wp.description, ''), ' ', '')) = 'КАПИТАЛЬНЫЙРЕМОНТ')
  OR upperUTF8(replaceAll(coalesce(wp.remarks, ''), ' ', '')) LIKE '%СБОРКАВС%'
)
AND wp.start_date > 18993
AND (wp.act_start_date > 18993 OR wp.act_start_date IS NULL)
AND a.manual_owner = 'ЮТ-ВУ'
AND coalesce(a.owner, '') IN ({_allowed_owners_sql_in()})
AND wp.ac_typ IN ('МИ8', 'МИ8АМТ', 'МИ8МТВ')
ORDER BY ac_registr, wpno
"""


def _map_wp_status(status_code: object) -> str:
    stmap = {-1: "Открыто", -2: "Закрыто", -3: "В процессе"}
    if pd.isna(status_code):
        return ""
    try:
        code = int(status_code)
    except (TypeError, ValueError):
        return str(status_code)
    return stmap.get(code, str(code))


def status_components_dataframe(
    client,
    report_date: str = DEFAULT_REPORT_DATE,
) -> pd.DataFrame:
    df = client.query_df(_status_components_sql(report_date))
    df["lease_restricted"] = _lease_col(df["owner"])
    df["removal_date"] = _fmt_dmY_series(df["removal_date"])
    df["target_date"] = _fmt_dmY_series(df["target_date"])
    df["oh_at_date"] = pd.to_datetime(df["oh_at_date"], errors="coerce")
    return df


def program_ac_dataframe(
    client,
    report_date: str = DEFAULT_REPORT_DATE,
    *,
    strict_status: bool = True,
    ac_registrs: list[int] | None = None,
) -> pd.DataFrame:
    return client.query_df(
        _program_ac_sql(
            report_date,
            strict_status=strict_status,
            ac_registrs=ac_registrs,
        )
    )


def status_overhaul_dataframe(
    client,
    report_date: str = DEFAULT_REPORT_DATE,
) -> pd.DataFrame:
    df = client.query_df(_status_overhaul_sql(report_date))
    df["status"] = df["status_code"].map(_map_wp_status)
    df = df.drop(columns=["status_code"])
    df["sched_start_date"] = _amos_int_to_timestamp(df["start_date"])
    df["sched_end_date"] = _amos_int_to_timestamp(df["end_date"])
    df["act_start_date"] = _amos_int_to_timestamp(df["act_start_date"])
    df["act_end_date"] = _amos_int_to_timestamp(df["act_end_date"])
    df = df.drop(columns=["start_date", "end_date"])
    df = df[
        [
            "ac_registr",
            "ac_typ",
            "wpno",
            "description",
            "sched_start_date",
            "sched_end_date",
            "act_start_date",
            "act_end_date",
            "status",
            "owner",
            "operator",
        ]
    ]
    return df


def export_status_components(client, out_dir: Path, report_date: str = DEFAULT_REPORT_DATE) -> Path:
    df = status_components_dataframe(client, report_date=report_date)
    path = out_dir / "Status_Components.xlsx"
    df.to_excel(path, index=False, engine="openpyxl")
    return path


def export_program_ac(client, out_dir: Path, report_date: str = DEFAULT_REPORT_DATE) -> Path:
    df = program_ac_dataframe(client, report_date=report_date)
    path = out_dir / "Program_AC.xlsx"
    df.to_excel(path, index=False, engine="openpyxl")
    return path


def export_status_overhaul(client, out_dir: Path, report_date: str = DEFAULT_REPORT_DATE) -> Path:
    df = status_overhaul_dataframe(client, report_date=report_date)
    path = out_dir / "Status_Overhaul.xlsx"
    df.to_excel(path, index=False, engine="openpyxl")
    return path


def export_program_ac_match_golden(
    client, out_dir: Path, report_date: str, golden_dir: Path
) -> Path:
    """Program_AC: только борта из golden; без фильтра status=0 (восстанавливает 22321 и др.). Порядок строк как в golden."""
    gpath = golden_dir / "Program_AC.xlsx"
    df_g = pd.read_excel(gpath, engine="openpyxl")
    regs = [int(x) for x in df_g["ac_registr"].dropna().unique()]
    df = program_ac_dataframe(
        client,
        report_date=report_date,
        strict_status=False,
        ac_registrs=regs,
    )
    kg = df_g[["ac_registr"]].copy()
    kg["_ord"] = np.arange(len(kg), dtype=np.int64)
    out = kg.merge(df, on="ac_registr", how="left").sort_values("_ord").drop(columns=["_ord"])
    for c in df_g.columns:
        if c not in out.columns:
            out[c] = np.nan
    out = out[list(df_g.columns)]
    path = out_dir / "Program_AC.xlsx"
    out.to_excel(path, index=False, engine="openpyxl")
    return path


def export_status_components_match_golden(
    client, out_dir: Path, report_date: str, golden_dir: Path
) -> Path:
    """Status_Components: строки и порядок как в golden; значения из витрины reports по (psn, partno)."""
    gpath = golden_dir / "Status_Components.xlsx"
    df_g = pd.read_excel(gpath, engine="openpyxl")
    df_q = status_components_dataframe(client, report_date=report_date)
    df_q["partno"] = df_q["partno"].astype(str).str.strip()
    df_q = df_q.drop_duplicates(subset=["psn", "partno"], keep="first")
    df_g = df_g.copy()
    df_g["partno"] = df_g["partno"].astype(str).str.strip()
    kg = df_g[["psn", "partno"]].copy()
    kg["_ord"] = np.arange(len(kg), dtype=np.int64)
    m = kg.merge(df_q, on=["psn", "partno"], how="left").sort_values("_ord")
    m = m.drop(columns=["_ord"])
    m["lease_restricted"] = _lease_col(m["owner"])
    m["removal_date"] = _fmt_dmY_series(m["removal_date"])
    m["target_date"] = _fmt_dmY_series(m["target_date"])
    m["oh_at_date"] = pd.to_datetime(m["oh_at_date"], errors="coerce")
    col_order = list(pd.read_excel(gpath, engine="openpyxl", nrows=0).columns)
    for c in col_order:
        if c not in m.columns:
            m[c] = np.nan
    m = m[col_order]
    path = out_dir / "Status_Components.xlsx"
    m.to_excel(path, index=False, engine="openpyxl")
    return path


def export_status_overhaul_match_golden(
    client, out_dir: Path, report_date: str, golden_dir: Path
) -> Path:
    """Status_Overhaul: порядок строк как в golden; ключи (ac_registr, wpno) к данным DWH."""
    gpath = golden_dir / "Status_Overhaul.xlsx"
    df_g = pd.read_excel(gpath, engine="openpyxl")
    df_r = status_overhaul_dataframe(client, report_date)
    df_r["ac_registr"] = df_r["ac_registr"].astype(np.int64)
    df_r["wpno"] = df_r["wpno"].astype(str).str.strip()
    kg = df_g[["ac_registr", "wpno"]].copy()
    kg["ac_registr"] = kg["ac_registr"].astype(np.int64)
    kg["wpno"] = kg["wpno"].astype(str).str.strip()
    kg["_ord"] = np.arange(len(kg), dtype=np.int64)
    out = kg.merge(df_r, on=["ac_registr", "wpno"], how="left").sort_values("_ord").drop(columns=["_ord"])
    for c in df_g.columns:
        if c not in out.columns:
            out[c] = np.nan
    out = out[list(df_g.columns)]
    path = out_dir / "Status_Overhaul.xlsx"
    out.to_excel(path, index=False, engine="openpyxl")
    return path


def _norm_cell(v):
    if v is None:
        return None
    if isinstance(v, float) and np.isnan(v):
        return None
    if isinstance(v, (np.floating, float)):
        if np.isnan(v):
            return None
        return round(float(v), 6)
    if isinstance(v, str):
        t = v.strip()
        return None if t == "" else t
    if pd.isna(v):
        return None
    if hasattr(v, "to_pydatetime"):
        return pd.Timestamp(v).normalize()
    if isinstance(v, pd.Timestamp):
        return v.normalize()
    return v


def _row_key_psn(row: pd.Series) -> str:
    psn = row.get("psn")
    if psn is not None and not (isinstance(psn, float) and np.isnan(psn)) and str(psn).strip() != "":
        return str(psn).strip()
    p = row.get("partno")
    s = row.get("serialno")
    return f"{p}|{s}"


def compare_status_components(gold: pd.DataFrame, replay: pd.DataFrame) -> None:
    kg = gold.apply(_row_key_psn, axis=1)
    kr = replay.apply(_row_key_psn, axis=1)
    sg, sr = set(kg), set(kr)
    print(f"  Status_Components keys: golden {len(sg)}, replay {len(sr)}", flush=True)
    print(f"  only in golden: {len(sg - sr)}  only in replay: {len(sr - sg)}", flush=True)
    cols_g = set(gold.columns)
    cols_r = set(replay.columns)
    print(f"  columns only golden: {sorted(cols_g - cols_r)}", flush=True)
    print(f"  columns only replay: {sorted(cols_r - cols_g)}", flush=True)
    shared = sorted(cols_g & cols_r)
    gi = gold.assign(_k=kg).drop_duplicates("_k", keep="first").set_index("_k")
    ri = replay.assign(_k=kr).drop_duplicates("_k", keep="first").set_index("_k")
    common_idx = gi.index.intersection(ri.index)
    subg = gi.loc[common_idx]
    subr = ri.loc[common_idx]
    total_cells = len(common_idx) * len(shared)
    match = 0
    for c in shared:
        a = subg[c].map(_norm_cell)
        b = subr[c].map(_norm_cell)
        match += int((a == b).sum())
    if total_cells:
        print(
            f"  cell match (normalized) approx: {match}/{total_cells} "
            f"({100.0 * match / total_cells:.2f}%)",
            flush=True,
        )


def compare_frames(name: str, gold: pd.DataFrame, replay: pd.DataFrame, key_cols: list[str]) -> None:
    print(f"  [{name}] rows golden={len(gold)} replay={len(replay)}", flush=True)
    print(f"  columns only golden: {sorted(set(gold.columns) - set(replay.columns))}", flush=True)
    print(f"  columns only replay: {sorted(set(replay.columns) - set(gold.columns))}", flush=True)
    kg = gold[key_cols].astype(str).agg("|".join, axis=1)
    kr = replay[key_cols].astype(str).agg("|".join, axis=1)
    sg, sr = set(kg), set(kr)
    print(f"  keys only golden: {len(sg - sr)}  only replay: {len(sr - sg)}", flush=True)


def run_compare(
    golden_dir: Path,
    out_dir: Path,
    *,
    steps: set[str] | None = None,
) -> None:
    """steps: подмножество {'status_components','program_ac','status_overhaul'}; None = все три."""
    warnings.filterwarnings(
        "ignore", message="Workbook contains no default style", category=UserWarning
    )
    all_steps = {"status_components", "program_ac", "status_overhaul"}
    active = all_steps if steps is None else (steps & all_steps)
    if not active:
        return

    print("\n=== Сравнение с golden ===", flush=True)
    if "status_components" in active:
        print("  чтение Status_Components (golden)...", flush=True)
        sc_g = pd.read_excel(golden_dir / "Status_Components.xlsx", engine="openpyxl")
        print("  чтение Status_Components (replay)...", flush=True)
        sc_r = pd.read_excel(out_dir / "Status_Components.xlsx", engine="openpyxl")
        compare_status_components(sc_g, sc_r)

    if "program_ac" in active:
        pa_g = pd.read_excel(golden_dir / "Program_AC.xlsx", engine="openpyxl")
        pa_r = pd.read_excel(out_dir / "Program_AC.xlsx", engine="openpyxl")
        compare_frames("Program_AC", pa_g, pa_r, ["ac_registr"])

    if "status_overhaul" in active:
        so_g = pd.read_excel(golden_dir / "Status_Overhaul.xlsx", engine="openpyxl")
        so_r = pd.read_excel(out_dir / "Status_Overhaul.xlsx", engine="openpyxl")
        compare_frames("Status_Overhaul", so_g, so_r, ["ac_registr", "wpno"])


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Прямая выгрузка из YC DWH в Excel + опционально сравнение с golden."
    )
    p.add_argument(
        "--report-date",
        default=DEFAULT_REPORT_DATE,
        help=f"Дата витрины / as-of (YYYY-MM-DD). По умолчанию {DEFAULT_REPORT_DATE}",
    )
    p.add_argument(
        "--out-subdir",
        default=None,
        help="Подкаталог в output/ (по умолчанию dwh_direct_v<date> или dwh_replay_v<date>)",
    )
    p.add_argument(
        "--prefix",
        choices=("dwh_direct", "dwh_replay"),
        default="dwh_direct",
        help="Префикс каталога выгрузки, если --out-subdir не задан",
    )
    p.add_argument(
        "--step",
        action="append",
        choices=("program_ac", "status_overhaul", "status_components", "all"),
        default=None,
        help="Какие файлы выгрузить (можно повторить). По умолчанию: all",
    )
    p.add_argument(
        "--no-compare",
        action="store_true",
        help="Не сравнивать с golden после выгрузки",
    )
    p.add_argument(
        "--golden-dir",
        default=None,
        help="Каталог golden (по умолчанию data_input/source_data/v_<report-date>)",
    )
    p.add_argument(
        "--match-golden",
        action="store_true",
        help="Выгрузка по ключам/порядку golden: Program_AC без status=0; SC/WP join к витрине по ключам из Excel.",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    report_date = args.report_date.strip()
    root = _repo_root()
    sub = args.out_subdir or default_out_subdir(report_date, prefix=args.prefix)
    out_dir = root / "output" / sub
    out_dir.mkdir(parents=True, exist_ok=True)

    steps_arg = args.step
    if not steps_arg or "all" in steps_arg:
        export_steps = {"program_ac", "status_overhaul", "status_components"}
    else:
        export_steps = set(steps_arg)

    client = dwh_client()
    golden = (
        Path(args.golden_dir).resolve()
        if args.golden_dir
        else root / "data_input" / "source_data" / f"v_{report_date}"
    )
    use_match = args.match_golden
    if use_match and not golden.is_dir():
        _fail(f"--match-golden: нет каталога {golden}")

    if "status_components" in export_steps:
        print("Экспорт Status_Components...", flush=True)
        if use_match:
            export_status_components_match_golden(client, out_dir, report_date, golden)
        else:
            export_status_components(client, out_dir, report_date=report_date)
    if "program_ac" in export_steps:
        print("Экспорт Program_AC...", flush=True)
        if use_match:
            export_program_ac_match_golden(client, out_dir, report_date, golden)
        else:
            export_program_ac(client, out_dir, report_date=report_date)
    if "status_overhaul" in export_steps:
        print("Экспорт Status_Overhaul...", flush=True)
        if use_match:
            export_status_overhaul_match_golden(client, out_dir, report_date, golden)
        else:
            export_status_overhaul(client, out_dir, report_date=report_date)

    print(f"Готово: {out_dir}", flush=True)

    if args.no_compare:
        return
    if not golden.is_dir():
        print(f"WARNING: нет каталога golden, сравнение пропущено: {golden}", file=sys.stderr)
        return
    compare_only = export_steps if export_steps else None
    run_compare(golden, out_dir, steps=compare_only)
    if use_match:
        print("\n=== Режим --match-golden: ключи и число строк совпадают с golden; проверьте cell diff выше ===", flush=True)


if __name__ == "__main__":
    main()
