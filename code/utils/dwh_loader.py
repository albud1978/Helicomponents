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
from dwh_golden_replay_export import DEFAULT_REPORT_DATE, _lease_col, dwh_client
from extract.aircraft_number_processor import process_aircraft_numbers_in_memory
from extract.dual_loader import create_tables, get_md_partnos, insert_data, prepare_data

RAW_COLS = ["partno","serialno","ac_typ","location","mfg_date","removal_date","target_date","condition","owner","lease_restricted","oh","oh_threshold","ll","sne","ppr","version_date","version_id","partseqno_i","psn","address_i","ac_type_i","oh_at_date","shop_visit_counter"]
PANDAS_COLS = ["partno","serialno","ac_typ","location","mfg_date","removal_date","target_date","condition","owner","lease_restricted","oh","oh_threshold","ll","sne","ppr","version_date","version_id","partseqno_i","psn","address_i","ac_type_i","status_id","repair_days","aircraft_number","ac_type_mask","group_by"]

def _fail(msg): print(f"ERROR: {msg}", file=sys.stderr); raise SystemExit(1)

def _parse_date(raw):
    try: dt = datetime.strptime(raw.strip(),"%Y-%m-%d").date()
    except ValueError as e: _fail(f"Bad date {raw!r}: {e}")
    return dt.isoformat(), dt.isoformat()


def _norm_dates(df):
    """Normalize date columns before ClickHouse insert."""
    date_cols = ["mfg_date","removal_date","target_date","oh_at_date","version_date"]
    for col in date_cols:
        if col not in df.columns:
            continue
        # Convert strings to datetime
        if df[col].dtype == object:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        # Fill NaT
        df[col] = df[col].fillna(pd.Timestamp("1971-01-01"))
    # Ensure version_date is date
    if "version_date" in df.columns:
        df["version_date"] = pd.to_datetime(df["version_date"], errors="coerce").fillna(pd.Timestamp("1971-01-01"))
    return df

def _align(df, cols):
    m = [c for c in cols if c not in df.columns]
    if m: _fail(f"Missing cols: {m}")
    return df[list(cols)].copy()

def _ac_mask(s):
    return s.map(lambda x: 1 if isinstance(x,str) and "17" in x else 0).fillna(0).astype("uint8")

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
    return _align(pdf, PANDAS_COLS)

def _batch_insert(ch, df, table, desc, batch=50000):
    total = 0
    for start in range(0, len(df), batch):
        chunk = df.iloc[start:start+batch]
        data = [tuple(row) for row in chunk.values]
        ch.execute(f"INSERT INTO {table} VALUES", data)
        total += len(chunk)
    print(f"  {table}: {total:,} inserted ({desc})")
    return total

def load(ch, raw, pandas, vd, vi, dry=False):
    s = {"raw": len(raw), "pandas": len(pandas), "ri": 0, "pi": 0}
    if dry: return s
    create_tables(ch)
    c = ch.execute(f"SELECT COUNT(*) FROM heli_raw WHERE version_date=%(vd)s AND version_id=%(vi)s",{"vd":vd,"vi":vi})[0][0]
    if c: _fail(f"heli_raw already has {c} rows for {vd}")
    c = ch.execute(f"SELECT COUNT(*) FROM heli_pandas WHERE version_date=%(vd)s AND version_id=%(vi)s",{"vd":vd,"vi":vi})[0][0]
    if c: _fail(f"heli_pandas already has {c} rows for {vd}")
    s["ri"] = _batch_insert(ch, raw, "heli_raw", "DWH raw")
    s["pi"] = _batch_insert(ch, pandas, "heli_pandas", "DWH staging")
    if s["ri"] != len(raw): _fail(f"raw: inserted {s['ri']}, expected {len(raw)}")
    if s["pi"] != len(pandas): _fail(f"pandas: inserted {s['pi']}, expected {len(pandas)}")
    return s

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--report-date", default=DEFAULT_REPORT_DATE)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    rd, vd = _parse_date(a.report_date)
    vi = 1
    print(f"Report: {rd}  Version: {vd} v{vi}")
    print("1/4 DWH...")
    dwh = dwh_client()
    src = fetch_df(dwh, rd)
    print(f"   DWH: {len(src):,} rows")
    print("2/4 md_components...")
    ch = get_clickhouse_client()
    md = get_md_partnos(ch)
    print(f"   md_partnos: {len(md)}")
    print("3/4 Enrich...")
    raw = prepare_data(src.copy(), vd, version_id=vi, table_name="heli_raw")
    raw = _align(raw, RAW_COLS)
    raw = _norm_dates(raw)
    pandas = enrich(src, vd, vi, ch, md)
    pandas = _norm_dates(pandas)
    print(f"   raw: {len(raw):,}  pandas: {len(pandas):,}")
    print(f"4/4 {'[DRY] ' if a.dry_run else ''}Load...")
    s = load(ch, raw, pandas, vd, vi, dry=a.dry_run)
    if not a.dry_run:
        print(f"   raw inserted: {s['ri']:,}  pandas: {s['pi']:,}")
    print(f"\nSummary: DWH={len(src):,}  raw={len(raw):,}  pandas={len(pandas):,}  dropped={len(src)-len(pandas):,}")

if __name__ == "__main__":
    main()
