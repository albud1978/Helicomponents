#!/usr/bin/env python3
"""Post-load enrichment for heli_pandas after DWH ingest (version-scoped)."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

CODE_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(CODE_ROOT))
sys.path.append(str(CODE_ROOT / "utils"))

from config_loader import get_clickhouse_client
from extract.dual_loader import insert_data
from extract.inactive_serviceable_classifier import process_inactive_serviceable_status
from extract.overhaul_status_processor import process_status_field
from extract.program_ac_precheck_runner import apply_program_ac_precheck
from extract.program_ac_status_processor import process_program_ac_status_field
from extract.heli_pandas_component_status import apply_component_status
from extract.heli_pandas_serviceable_status import apply_serviceable_status
from extract.heli_pandas_repair_status import apply_repair_status
from extract.heli_pandas_storage_status import apply_storage_status
from extract.repair_days_calculator import apply_repair_days
from extract.heli_pandas_terminal_br_gate import apply_terminal_br_gate

PANDAS_COLS = [
    "partno", "serialno", "ac_typ", "location", "mfg_date", "removal_date", "target_date",
    "condition", "owner", "lease_restricted", "oh", "oh_threshold", "ll", "sne", "ppr",
    "version_date", "version_id", "partseqno_i", "psn", "address_i", "ac_type_i",
    "status_id", "repair_days", "repair_time", "aircraft_number", "ac_type_mask", "group_by",
]

POST_SCRIPTS = (
    ("program_ac_precheck_runner.py", apply_program_ac_precheck),
    ("heli_pandas_component_status.py", apply_component_status),
    ("heli_pandas_serviceable_status.py", apply_serviceable_status),
    ("heli_pandas_repair_status.py", apply_repair_status),
    # After repair force-exits planers (past target_date → OPS), re-sync
    # ИСПРАВНЫЙ aggregates that were painted 3 while the planer was still 4.
    ("heli_pandas_component_status.py", apply_component_status),
    ("heli_pandas_storage_status.py", apply_storage_status),
    ("repair_days_calculator.py", apply_repair_days),
    ("heli_pandas_terminal_br_gate.py", apply_terminal_br_gate),
)


def _fail(msg: str) -> None:
    print(f"ERROR: {msg}", file=sys.stderr)
    raise SystemExit(1)


def _count_rows(client, table: str, version_date: date, version_id: int) -> int:
    return int(
        client.execute(
            f"SELECT COUNT(*) FROM {table} WHERE version_date=%(vd)s AND version_id=%(vi)s",
            {"vd": version_date, "vi": version_id},
        )[0][0]
    )


def _ops_planers(client, version_date: date, version_id: int) -> int:
    return int(
        client.execute(
            """
            SELECT countDistinct(aircraft_number)
            FROM heli_pandas
            WHERE version_date = %(vd)s
              AND version_id = %(vi)s
              AND toUInt8(ifNull(status_id, 0)) = 2
              AND toUInt32(ifNull(group_by, 0)) IN (1, 2)
              AND toUInt32(ifNull(aircraft_number, 0)) > 0
            """,
            {"vd": version_date, "vi": version_id},
        )[0][0]
    )


def _planner_zero(client, version_date: date, version_id: int) -> int:
    return int(
        client.execute(
            """
            SELECT count()
            FROM heli_pandas
            WHERE version_date = %(vd)s
              AND version_id = %(vi)s
              AND toUInt32(ifNull(group_by, 0)) IN (1, 2)
              AND toUInt8(ifNull(status_id, 0)) = 0
            """,
            {"vd": version_date, "vi": version_id},
        )[0][0]
    )


def _ensure_heli_pandas_schema(client) -> None:
    client.execute("ALTER TABLE heli_pandas ADD COLUMN IF NOT EXISTS repair_time UInt16 DEFAULT 0")


def _load_heli_pandas_version(client, version_date: date, version_id: int) -> pd.DataFrame:
    cols = ", ".join(f"`{c}`" for c in PANDAS_COLS)
    rows = client.execute(
        f"""
        SELECT {cols}
        FROM heli_pandas
        WHERE version_date = %(vd)s AND version_id = %(vi)s
        """,
        {"vd": version_date, "vi": version_id},
    )
    if not rows:
        _fail(f"heli_pandas пуст для {version_date} v{version_id}")
    return pd.DataFrame(rows, columns=PANDAS_COLS)


def _reset_enrichment_outputs(df: pd.DataFrame) -> pd.DataFrame:
    """Clear prior cascade outputs so re-enrich is idempotent on classified data.

    Resets only enrichment-derived fields. Base identity/dims stay intact:
    aircraft_number, group_by, ac_type_mask, and raw DWH columns.
    """
    out = df.copy()
    n = len(out)
    out["status_id"] = 0
    out["repair_days"] = pd.Series([None] * n, index=out.index, dtype=object)
    print(
        f"Enrichment reset applied: status_id=0, repair_days=NA for {n} rows "
        "(base fields / aircraft_number / group_by / ac_type_mask unchanged)"
    )
    return out


def _replace_heli_pandas_version(
    client, df: pd.DataFrame, version_date: date, version_id: int
) -> int:
    if "repair_days" in df.columns:
        df = df.copy()
        df["repair_days"] = pd.Series(
            [None if pd.isna(value) else int(value) for value in df["repair_days"]],
            index=df.index,
            dtype=object,
        )
    if "repair_time" in df.columns:
        df = df.copy()
        df["repair_time"] = pd.Series(
            [0 if pd.isna(value) else int(value) for value in df["repair_time"]],
            index=df.index,
            dtype=object,
        )
    client.execute(
        "DELETE FROM heli_pandas WHERE version_date = %(vd)s AND version_id = %(vi)s",
        {"vd": version_date, "vi": version_id},
    )
    return insert_data(client, df, "heli_pandas", "DWH post-enrichment")


def _run_planner_cascade(client, df: pd.DataFrame) -> pd.DataFrame:
    df = process_status_field(df, client)
    df = process_program_ac_status_field(df, client)
    df = process_inactive_serviceable_status(df, client)
    return df


def _run_post_cascade(
    client,
    df: pd.DataFrame,
    version_date: date,
    version_id: int,
    dataset_path: str | None,
) -> pd.DataFrame:
    for script_name, apply_func in POST_SCRIPTS:
        print(f"  -> {script_name}")
        if apply_func is apply_program_ac_precheck:
            df = apply_func(
                df,
                client,
                version_date,
                version_id,
                dataset_path=dataset_path,
            )
        else:
            df = apply_func(df, client, version_date, version_id)
    return df


def run_post_enrichment(
    version_date: date,
    version_id: int = 1,
    *,
    phase: str = "all",
    dry_run: bool = False,
    dataset_path: str | None = None,
    client=None,
) -> dict:
    """Run the selected status-cascade phase for one heli_pandas version."""
    if phase not in {"planner", "post", "all"}:
        _fail(f"неизвестная phase={phase!r}; ожидается planner, post или all")

    ch = client or get_clickhouse_client()

    if _count_rows(ch, "program_ac", version_date, version_id) == 0:
        _fail("program_ac не загружен — enrichment невозможен")
    if _count_rows(ch, "status_overhaul", version_date, version_id) == 0:
        _fail("status_overhaul не загружен — enrichment невозможен")
    if _count_rows(ch, "heli_pandas", version_date, version_id) == 0:
        _fail("heli_pandas не загружен — enrichment невозможен")
    _ensure_heli_pandas_schema(ch)

    stats = {
        "ops_before": _ops_planers(ch, version_date, version_id),
        "planner_zero_before": _planner_zero(ch, version_date, version_id),
    }
    print(
        f"Enrichment {version_date} v{version_id}: "
        f"OPS={stats['ops_before']}, planner_status_0={stats['planner_zero_before']}"
    )

    df = _load_heli_pandas_version(ch, version_date, version_id)
    if phase in {"planner", "all"}:
        print("Planner cascade (reset -> overhaul -> program_ac -> 3b)...")
        df = _reset_enrichment_outputs(df)
        df = _run_planner_cascade(ch, df)

    if phase in {"post", "all"}:
        print("Post cascade (precheck -> component/serviceable/repair/storage/terminal)...")
        df = _run_post_cascade(ch, df, version_date, version_id, dataset_path)

    ops_after = int(
        df.loc[
            (df["group_by"].isin([1, 2]))
            & (df["status_id"] == 2)
            & (df["aircraft_number"].fillna(0).astype(int) > 0),
            "aircraft_number",
        ].nunique()
    )
    stats["ops_after"] = ops_after
    stats["planner_zero_after"] = int(
        ((df["group_by"].isin([1, 2])) & (df["status_id"] == 0)).sum()
    )
    if dry_run:
        print(
            f"[dry-run] phase={phase}: OPS={stats['ops_after']}, "
            f"planner_status_0={stats['planner_zero_after']}"
        )
        return stats

    inserted = _replace_heli_pandas_version(ch, df, version_date, version_id)
    if inserted != len(df):
        _fail(f"heli_pandas: inserted {inserted}, expected {len(df)}")

    print(
        f"Enrichment phase={phase} done: OPS={stats['ops_after']}, "
        f"planner_status_0={stats['planner_zero_after']}"
    )
    if phase in {"post", "all"}:
        source = (
            f"Excel {Path(dataset_path) / 'Program.xlsx'}"
            if dataset_path
            else "SQL flight_program_fl"
        )
        print(f"Precheck day0 dt source confirmed: {source}")
    return stats


def main() -> None:
    import argparse

    p = argparse.ArgumentParser(description="Post-enrichment heli_pandas after DWH load")
    p.add_argument("--version-date", required=True, help="YYYY-MM-DD")
    p.add_argument("--version-id", type=int, default=1)
    p.add_argument("--phase", choices=("planner", "post", "all"), default="all")
    p.add_argument("--dataset-path")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()
    vd = date.fromisoformat(args.version_date)
    run_post_enrichment(
        vd,
        args.version_id,
        phase=args.phase,
        dry_run=args.dry_run,
        dataset_path=args.dataset_path,
    )


if __name__ == "__main__":
    main()
