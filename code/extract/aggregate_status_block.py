#!/usr/bin/env python3
"""Mechanical day0 aggregate status block preserving the factual cascade.

The planner cascade (overhaul → program_ac → inactive/serviceable 3b) and the
planner-only post precheck run before this module.  This module then executes
A1 → A2 → A3 → A4 → A5 without changing the semantics of the leaf ``apply_*``
processors.  The day0 tail runs repair_days → terminal_br → demote afterwards.

``FACTUAL_DAY0_MATRIX`` is a documentation-friendly contract of that order.
It describes the working code; it is not an alternative rules engine.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

import pandas as pd

CODE_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(CODE_ROOT))
sys.path.append(str(CODE_ROOT / "utils"))

from config_loader import get_clickhouse_client
from extract.heli_pandas_component_status import apply_component_status
from extract.heli_pandas_repair_status import apply_repair_status
from extract.heli_pandas_serviceable_status import apply_serviceable_status
from extract.heli_pandas_storage_status import apply_storage_status


FACTUAL_DAY0_MATRIX = (
    {
        "axis_planer": "planer",
        "condition": "ignored",
        "target_date": "status_overhaul dates",
        "in_status": "0",
        "out_status": "4 (ongoing) or 2 (past end)",
        "step_id": "P1",
        "script": "overhaul_status_processor.py",
    },
    {
        "axis_planer": "planer",
        "condition": "ignored",
        "target_date": "n/a",
        "in_status": "0",
        "out_status": "2 when in program_ac as-of day0",
        "step_id": "P2",
        "script": "program_ac_status_processor.py",
    },
    {
        "axis_planer": "planer + attached aggregates",
        "condition": "ignored",
        "target_date": "not in program history",
        "in_status": "planer 0",
        "out_status": "planer 1; aggregates 7",
        "step_id": "P3b.1",
        "script": "inactive_serviceable_classifier.py",
    },
    {
        "axis_planer": "planer + attached aggregates",
        "condition": "ignored",
        "target_date": "history and remain_d > 0",
        "in_status": "planer 0",
        "out_status": "planer 3; aggregates 3",
        "step_id": "P3b.2",
        "script": "inactive_serviceable_classifier.py",
    },
    {
        "axis_planer": "planer + attached aggregates",
        "condition": "ignored",
        "target_date": "history without positive calendar remain",
        "in_status": "planer 0",
        "out_status": "planer 1; aggregates 3",
        "step_id": "P3b.3",
        "script": "inactive_serviceable_classifier.py",
    },
    {
        "axis_planer": "planer only",
        "condition": "resource precheck",
        "target_date": "n/a",
        "in_status": "2",
        "out_status": "6 or 7 when first-day resource is insufficient",
        "step_id": "P4",
        "script": "program_ac_precheck_runner.py",
    },
    {
        "axis_planer": "aggregate; carrier planer=2",
        "condition": "normalized ИСПРАВНЫЙ",
        "target_date": "any",
        "in_status": "!=2",
        "out_status": "2",
        "step_id": "A1",
        "script": "heli_pandas_component_status.py",
    },
    {
        "axis_planer": "aggregate",
        "condition": "normalized ИСПРАВНЫЙ",
        "target_date": "any",
        "in_status": "0",
        "out_status": "3",
        "step_id": "A2",
        "script": "heli_pandas_serviceable_status.py",
    },
    {
        "axis_planer": "aggregate",
        "condition": "any",
        "target_date": "valid and past",
        "in_status": "0",
        "out_status": "3",
        "step_id": "A3a",
        "script": "heli_pandas_repair_status.py",
    },
    {
        "axis_planer": "planer or aggregate",
        "condition": "planer or != ИСПРАВНЫЙ",
        "target_date": "valid and current/future",
        "in_status": "0",
        "out_status": "4",
        "step_id": "A3b",
        "script": "heli_pandas_repair_status.py",
    },
    {
        "axis_planer": "aggregate; carrier planer=2",
        "condition": "normalized ИСПРАВНЫЙ",
        "target_date": "any",
        "in_status": "!=2",
        "out_status": "2",
        "step_id": "A4",
        "script": "heli_pandas_component_status.py",
    },
    {
        "axis_planer": "aggregate",
        "condition": "non-serviceable BR reached, ДОНОР, or ВОЗМОЖНОЕ ПРОДЛЕНИЕ НР",
        "target_date": "any",
        "in_status": "0",
        "out_status": "6",
        "step_id": "A5a",
        "script": "heli_pandas_storage_status.py",
    },
    {
        "axis_planer": "aggregate",
        "condition": "normalized НЕИСПРАВНЫЙ",
        "target_date": "no repair branch selected",
        "in_status": "0",
        "out_status": "7",
        "step_id": "A5b",
        "script": "heli_pandas_storage_status.py",
    },
    {
        "axis_planer": "aggregate",
        "condition": "any leftover",
        "target_date": "any",
        "in_status": "0",
        "out_status": "7",
        "step_id": "A5c",
        "script": "heli_pandas_storage_status.py",
    },
    {
        "axis_planer": "planer or aggregate",
        "condition": "BR > 0 and sne >= BR",
        "target_date": "any",
        "in_status": "1 or 7",
        "out_status": "6",
        "step_id": "T1",
        "script": "heli_pandas_terminal_br_gate.py",
    },
    {
        "axis_planer": "selected excess OPS planer + attached status=2 aggregates",
        "condition": "destination_for_remain",
        "target_date": "program history then calendar; demote-only fallback",
        "in_status": "planer 2; aggregates 2",
        "out_status": "planer 1|3; aggregates 7|3",
        "step_id": "D1",
        "script": "day0_ops_deficit_demote_runner.py",
    },
)


AGGREGATE_STATUS_STEPS = (
    ("component", apply_component_status),
    ("serviceable", apply_serviceable_status),
    ("repair", apply_repair_status),
    ("component_resync", apply_component_status),
    ("storage", apply_storage_status),
)


def apply_aggregate_status_block(
    df: pd.DataFrame,
    client,
    version_date: date,
    version_id: int,
) -> pd.DataFrame:
    """Apply the existing aggregate status processors in their fixed order."""
    print("aggregate_status_block: factual matrix order A1→A5 (no semantic change)")
    for step_name, apply_func in AGGREGATE_STATUS_STEPS:
        print(f"aggregate_status_block/{step_name}")
        df = apply_func(df, client, version_date, version_id)
    return df


def main() -> int:
    parser = argparse.ArgumentParser(description="Apply aggregate status cascade")
    parser.add_argument("--version-date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--version-id", type=int, default=1)
    args = parser.parse_args()

    version_date = date.fromisoformat(args.version_date)
    client = get_clickhouse_client()

    from utils.dwh_post_enrichment import (
        _load_heli_pandas_version,
        _replace_heli_pandas_version,
    )

    df = _load_heli_pandas_version(client, version_date, args.version_id)
    updated = apply_aggregate_status_block(
        df,
        client,
        version_date,
        args.version_id,
    )
    _replace_heli_pandas_version(client, updated, version_date, args.version_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
