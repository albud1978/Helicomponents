#!/usr/bin/env python3
"""Прямая загрузка Status_Components из DWH в Project ClickHouse на любую дату."""
from __future__ import annotations
import argparse, sys
from datetime import datetime
from pathlib import Path
import pandas as pd

CODE_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(CODE_ROOT)); sys.path.append(str(CODE_ROOT / "utils"))

from config_loader import get_clickhouse_client
from dwh_golden_replay_export import (
    DEFAULT_REPORT_DATE,
    _lease_col,
    dwh_client,
    program_ac_dataframe,
    status_overhaul_dataframe,
)
from extract.aircraft_number_processor import process_aircraft_numbers_in_memory
from extract.dual_loader import create_tables, get_md_partnos, prepare_data
from extract.program_ac_loader import (
    create_program_ac_table,
    insert_program_ac_data,
    prepare_program_ac_data,
)
from extract.status_overhaul_loader import (
    create_status_overhaul_table,
    insert_status_overhaul_data,
    prepare_status_overhaul_data,
)
from dwh_post_enrichment import run_post_enrichment

PANDAS_COLS = ["partno","serialno","ac_typ","location","mfg_date","removal_date","target_date","condition","owner","lease_restricted","oh","oh_threshold","ll","sne","ppr","version_date","version_id","partseqno_i","psn","address_i","ac_type_i","status_id","repair_days","repair_time","aircraft_number","ac_type_mask","group_by"]

def _fail(msg): print(f"ERROR: {msg}", file=sys.stderr); raise SystemExit(1)

def _parse_date(raw):
    try: dt = datetime.strptime(raw.strip(),"%Y-%m-%d").date()
    except ValueError as e: _fail(f"Bad date {raw!r}: {e}")
    return dt.isoformat(), dt


def _norm_dates(df):
    """Normalize date columns before ClickHouse insert."""
    _normalize_date_columns(
        df,
        ["mfg_date", "removal_date", "oh_at_date", "version_date"],
        fill_missing=True,
    )
    return _normalize_date_columns(
        df,
        ["target_date"],
        fill_missing=True,
        fill_value=pd.Timestamp("1970-01-01"),
    )

def _align(df, cols):
    m = [c for c in cols if c not in df.columns]
    if m: _fail(f"Missing cols: {m}")
    return df[list(cols)].copy()


def _count_rows(ch, table, vd, vi):
    return int(
        ch.execute(
            f"SELECT COUNT(*) FROM {table} WHERE version_date=%(vd)s AND version_id=%(vi)s",
            {"vd": vd, "vi": vi},
        )[0][0]
    )


def _normalize_date_columns(df, columns, *, fill_missing=False, fill_value=pd.Timestamp("1971-01-01")):
    for col in columns:
        if col not in df.columns:
            continue
        parsed = pd.to_datetime(df[col], errors="coerce")
        if fill_missing:
            parsed = parsed.fillna(fill_value)
            df[col] = parsed.dt.date
        else:
            df[col] = parsed.map(lambda value: value.date() if pd.notna(value) else None)
    return df

def _ac_mask(s):
    def one(x):
        if pd.isna(x):
            return 0
        text = str(x).upper().replace("-", "").replace(" ", "")
        if "17" in text or "АМТ" in text or "МТВ" in text:
            return 64
        if "8" in text:
            return 32
        return 0
    return s.map(one).fillna(0).astype("uint8")

def _gb_map(ch):
    rows = ch.execute("SELECT toUInt32(partseqno_i), toUInt8(max(`group_by`)) FROM md_components WHERE partseqno_i IS NOT NULL AND `group_by` IS NOT NULL GROUP BY partseqno_i HAVING max(`group_by`) != 0")
    return {int(r[0]): int(r[1]) for r in rows}

def fetch_df(dwh, report_date):
    df = dwh.query_df(f"""SELECT partno,partseqno_i,serialno,psn,ac_typ,ac_type_i,location,LL AS ll,OH AS oh,OH_threshold AS oh_threshold,sne,ppr,mfg_date,oh_at_date,shop_visit_counter,owner,address_i,condition,removal_date,target_date FROM reports.amos_heli_rotables_components_status WHERE report_date=toDate('{report_date}')""")
    df["lease_restricted"] = _lease_col(df["owner"])
    df["mfg_date"] = pd.to_datetime(df["mfg_date"], errors="coerce").fillna(pd.Timestamp("1971-01-01"))
    df["oh_at_date"] = pd.to_datetime(df["oh_at_date"], errors="coerce").fillna(pd.Timestamp("1971-01-01"))
    for c in ("sne","ppr"):
        if c in df.columns: df[c] = df[c].fillna(0).astype("int64")
    return df

def enrich(df, vd, vi, ch, md):
    pdf = prepare_data(df.copy(), vd, version_id=vi, filter_partnos=md, table_name="heli_pandas")
    pdf, _, inv = process_aircraft_numbers_in_memory(pdf)
    if inv: _fail(f"Invalid RA: {inv}")
    if "ac_type_mask" not in pdf.columns or pdf["ac_type_mask"].eq(0).all():
        pdf["ac_type_mask"] = _ac_mask(pdf["ac_typ"])
    gb = _gb_map(ch)
    pdf["partseqno_i"] = pd.to_numeric(pdf["partseqno_i"], errors="coerce").fillna(0).astype("int64")
    pdf["group_by"] = pdf["partseqno_i"].map(gb).fillna(0).astype("int64")
    if "status_id" not in pdf.columns: pdf["status_id"] = 0
    if "repair_days" not in pdf.columns: pdf["repair_days"] = None
    if "repair_time" not in pdf.columns: pdf["repair_time"] = 0
    pdf["repair_days"] = pdf["repair_days"].map(
        lambda value: None if pd.isna(value) else int(value)
    ).astype(object)
    pdf["repair_time"] = pd.to_numeric(pdf["repair_time"], errors="coerce").fillna(0).astype("int64")
    return _align(pdf, PANDAS_COLS)

def _batch_insert(ch, df, table, desc, batch=50000):
    cols = ", ".join(f"`{c}`" for c in df.columns)
    total = 0
    for start in range(0, len(df), batch):
        chunk = df.iloc[start:start+batch]
        data = [tuple(row) for row in chunk.values]
        ch.execute(f"INSERT INTO {table} ({cols}) VALUES", data)
        total += len(chunk)
    print(f"  {table}: {total:,} inserted ({desc})")
    return total

def _delete_heli_pandas_slice(ch, vd, vi):
    ch.execute(
        "DELETE FROM heli_pandas WHERE version_date = %(vd)s AND version_id = %(vi)s",
        {"vd": vd, "vi": vi},
    )


def load(ch, pandas, vd, vi, dry=False, skip_existing=False, replace_slice=False):
    """Insert filtered heli_pandas staging slice (DWH path does not write heli_raw)."""
    s = {"pandas": len(pandas), "pandas_inserted": 0}
    if dry:
        return s
    create_tables(ch)
    pandas_existing = _count_rows(ch, "heli_pandas", vd, vi)
    if replace_slice and pandas_existing > 0:
        print(f"  heli_pandas: replace slice ({pandas_existing:,} rows deleted)")
        _delete_heli_pandas_slice(ch, vd, vi)
        pandas_existing = 0
    if pandas_existing:
        if skip_existing:
            print(f"  heli_pandas: skip existing {pandas_existing:,}")
            return s
        _fail(
            "heli_pandas target version already exists "
            f"({pandas_existing:,} rows). Use --replace-slice to reload."
        )
    s["pandas_inserted"] = _batch_insert(ch, pandas, "heli_pandas", "DWH staging")
    if s["pandas_inserted"] != len(pandas):
        _fail(f"heli_pandas: inserted {s['pandas_inserted']}, expected {len(pandas)}")
    return s


def fetch_program_ac_df(dwh, report_date):
    """Program_AC через общий DWH exporter."""
    df = program_ac_dataframe(dwh, report_date=report_date)
    df["ac_registr"] = df["ac_registr"].fillna(0).astype("int64")
    return df


def fetch_status_overhaul_df(dwh, report_date):
    """Status_Overhaul через общий DWH exporter."""
    df = status_overhaul_dataframe(dwh, report_date=report_date)
    df["ac_registr"] = df["ac_registr"].fillna(0).astype("int64")
    _normalize_date_columns(
        df,
        ["sched_start_date", "sched_end_date", "act_start_date", "act_end_date"],
        fill_missing=False,
    )
    return df


def load_program_ac(ch, df, vd, vi):
    """Insert into program_ac for target version."""
    create_program_ac_table(ch)
    existing = _count_rows(ch, "program_ac", vd, vi)
    if existing:
        _fail(f"program_ac already has {existing} rows for {vd}")
    prepared = prepare_program_ac_data(df.copy(), vd, vi)
    if "version_date" in prepared.columns:
        prepared = _normalize_date_columns(prepared, ["version_date"], fill_missing=True)
    inserted = insert_program_ac_data(ch, prepared)
    if inserted != len(prepared):
        _fail(f"program_ac: inserted {inserted}, expected {len(prepared)}")
    return inserted


def load_status_overhaul(ch, df, vd, vi):
    """Insert into status_overhaul for target version."""
    create_status_overhaul_table(ch)
    existing = _count_rows(ch, "status_overhaul", vd, vi)
    if existing:
        _fail(f"status_overhaul already has {existing} rows for {vd}")
    prepared = prepare_status_overhaul_data(df.copy(), vd, vi)
    prepared = _normalize_date_columns(
        prepared,
        ["sched_start_date", "sched_end_date", "act_start_date", "act_end_date", "version_date"],
        fill_missing=False,
    )
    if "version_date" in prepared.columns:
        prepared["version_date"] = pd.to_datetime(
            prepared["version_date"], errors="coerce"
        ).fillna(pd.Timestamp("1971-01-01")).dt.date
    inserted = insert_status_overhaul_data(ch, prepared)
    if inserted != len(prepared):
        _fail(f"status_overhaul: inserted {inserted}, expected {len(prepared)}")
    return inserted


def _parse_steps(steps):
    if not steps or "all" in steps:
        return ["program_ac", "status_overhaul", "status_components", "enrich"]
    out = []
    for step in steps:
        if step not in out:
            out.append(step)
    if "status_components" in out and "enrich" not in out:
        out.append("enrich")
    return out


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--report-date", default=DEFAULT_REPORT_DATE)
    p.add_argument("--version-id", type=int, default=1)
    p.add_argument(
        "--step",
        action="append",
        choices=("program_ac", "status_overhaul", "status_components", "enrich", "all"),
        default=None,
    )
    p.add_argument("--skip-existing", action="store_true")
    p.add_argument(
        "--replace-slice",
        action="store_true",
        help="DELETE heli_pandas for version_date/version_id before status_components insert",
    )
    p.add_argument(
        "--no-enrich",
        action="store_true",
        help="Не запускать post-enrichment после загрузки status_components",
    )
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()

    rd, vd = _parse_date(a.report_date)
    vi = a.version_id
    steps = _parse_steps(a.step)
    if a.no_enrich and "enrich" in steps:
        steps = [s for s in steps if s != "enrich"]

    print(f"Report: {rd}  Version: {vd} v{vi}")
    print(f"Steps: {', '.join(steps)}")
    if a.skip_existing:
        print("Mode: skip existing")
    if a.replace_slice:
        print("Mode: replace heli_pandas slice")
    if a.dry_run:
        print("Mode: dry-run")

    dwh = dwh_client()
    ch = get_clickhouse_client()
    summary = {}

    if "program_ac" in steps:
        print("\n[1/4] DWH Program_AC...")
        pac_df = fetch_program_ac_df(dwh, rd)
        print(f"   DWH: {len(pac_df):,} rows")
        if a.skip_existing and _count_rows(ch, "program_ac", vd, vi) > 0:
            existing = _count_rows(ch, "program_ac", vd, vi)
            print(f"   skip existing program_ac: {existing:,}")
            summary["program_ac"] = {"source": len(pac_df), "inserted": 0, "existing": existing}
        elif not a.dry_run:
            n = load_program_ac(ch, pac_df, vd, vi)
            print(f"   program_ac: {n:,} inserted")
            summary["program_ac"] = {"source": len(pac_df), "inserted": n, "existing": 0}
        else:
            print(f"   [DRY] program_ac: {len(pac_df):,} rows")
            summary["program_ac"] = {"source": len(pac_df), "inserted": 0, "existing": 0}

    if "status_overhaul" in steps:
        print("\n[2/4] DWH Status_Overhaul...")
        so_df = fetch_status_overhaul_df(dwh, rd)
        print(f"   DWH: {len(so_df):,} rows")
        if a.skip_existing and _count_rows(ch, "status_overhaul", vd, vi) > 0:
            existing = _count_rows(ch, "status_overhaul", vd, vi)
            print(f"   skip existing status_overhaul: {existing:,}")
            summary["status_overhaul"] = {"source": len(so_df), "inserted": 0, "existing": existing}
        elif not a.dry_run:
            n = load_status_overhaul(ch, so_df, vd, vi)
            print(f"   status_overhaul: {n:,} inserted")
            summary["status_overhaul"] = {"source": len(so_df), "inserted": n, "existing": 0}
        else:
            print(f"   [DRY] status_overhaul: {len(so_df):,} rows")
            summary["status_overhaul"] = {"source": len(so_df), "inserted": 0, "existing": 0}

    if "status_components" in steps:
        print("\n[3/4] DWH Status_Components...")
        src = fetch_df(dwh, rd)
        print(f"   DWH: {len(src):,} rows")
        md = get_md_partnos(ch)
        print(f"   md_partnos: {len(md)}")
        pandas = enrich(src, vd, vi, ch, md)
        pandas = _norm_dates(pandas)
        print(f"   source: {len(src):,}  pandas: {len(pandas):,}  (heli_raw not written)")
        sc = load(
            ch,
            pandas,
            vd,
            vi,
            dry=a.dry_run,
            skip_existing=a.skip_existing,
            replace_slice=a.replace_slice,
        )
        summary["status_components"] = {
            "source": len(src),
            "pandas": len(pandas),
            "pandas_inserted": sc["pandas_inserted"],
        }

    if "enrich" in steps:
        print("\n[4/4] Post-enrichment (status_id cascade)...")
        if a.dry_run:
            est = run_post_enrichment(vd, vi, dry_run=True, client=ch)
            summary["enrich"] = est
        else:
            est = run_post_enrichment(vd, vi, dry_run=False, client=ch)
            summary["enrich"] = est

    print(f"\nSummary: {summary}")

if __name__ == "__main__":
    main()
