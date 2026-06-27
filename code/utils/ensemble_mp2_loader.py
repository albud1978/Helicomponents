#!/usr/bin/env python3
"""Load CUDAEnsemble MP2/RL .bin exports into ClickHouse V9 tables."""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from datetime import date
from pathlib import Path

import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CODE_DIR = PROJECT_ROOT / "code"
MESSAGING_DIR = CODE_DIR / "sim_v2" / "messaging"
sys.path.insert(0, str(CODE_DIR))
sys.path.insert(0, str(MESSAGING_DIR))

from model_build import MAX_EXPORT_STEPS, MAX_FRAMES, MP2_BUF_SIZE, REPAIR_LINES_MAX, RL_BUF_SIZE
from rtc_mp2_export import MP2_DYNAMIC_FIELDS, MP2_EXPORT_DIR, MP2_FIELDS, MP2_STATIC_FIELDS
from rtc_repairline_export import (
    RL_EXPORT_FIELDS,
    export_repairline_to_ch,
    interpolate_repairline_daily,
)
from sim_env_setup import get_client


MASTER_COLUMNS = [
    "version_date", "version_id", "day_u16",
    "idx", "aircraft_number", "group_by", "oh", "br", "ll",
    "status_id", "pre_status_id", "status_change_day", "sne", "ppr", "limiter", "repair_days",
    "repair_claim_start_day", "repair_claim_end_day", "repair_claim_source",
    "repair_claim_line_id",
    "repair_time", "assembly_time", "active_trigger", "assembly_trigger",
    "daily_today_u32", "daily_next_u32", "commit_p2", "commit_p3",
]

MASTER_COMPARE_COLUMNS = [column for column in MASTER_COLUMNS if column != "version_id"]

REPAIRLINE_COLUMNS = [
    "version_date", "version_id", "day_u16", "line_id", "free_days", "repair_time",
    "aircraft_number", "group_by", "bank_count", "bank_head_start", "bank_head_end",
]
REPAIRLINE_COMPARE_COLUMNS = [column for column in REPAIRLINE_COLUMNS if column != "version_id"]


def _parse_version_date(value: str) -> int:
    if "-" in value:
        vd = date.fromisoformat(value)
        return vd.year * 10000 + vd.month * 100 + vd.day
    version_date_int = int(value)
    if version_date_int < 19000101 or version_date_int > 29991231:
        raise RuntimeError(f"Invalid version_date: {value}")
    return version_date_int


def _expected_size(field: str) -> int:
    if field in MP2_DYNAMIC_FIELDS:
        return MP2_BUF_SIZE
    if field in MP2_STATIC_FIELDS:
        return MAX_FRAMES
    if field in RL_EXPORT_FIELDS:
        return RL_BUF_SIZE
    if field == "mp2_day_for_step":
        return MAX_EXPORT_STEPS
    if field == "mp2_num_steps":
        return 2
    raise RuntimeError(f"Unknown ensemble export field: {field}")


def _read_u32(path: Path, expected_values: int) -> np.ndarray:
    if not path.is_file():
        raise RuntimeError(f"Missing ensemble .bin file: {path}")
    data = np.fromfile(path, dtype=np.uint32)
    if data.size != expected_values:
        raise RuntimeError(
            f"Unexpected ensemble .bin size for {path}: values={data.size}, expected={expected_values}"
        )
    return data


def load_mp2_export(export_dir: Path, version_id: int, total_agents: int | None) -> dict:
    fields = {}
    for field in MP2_DYNAMIC_FIELDS:
        raw = _read_u32(export_dir / f"run_{version_id}_{field}.bin", _expected_size(field))
        fields[field] = raw.reshape(MAX_EXPORT_STEPS, MAX_FRAMES)
    for field in MP2_STATIC_FIELDS:
        fields[field] = _read_u32(export_dir / f"run_{version_id}_{field}.bin", _expected_size(field))

    days_raw = _read_u32(export_dir / f"run_{version_id}_mp2_day_for_step.bin", MAX_EXPORT_STEPS)
    num_steps_raw = _read_u32(export_dir / f"run_{version_id}_mp2_num_steps.bin", 2)
    num_steps = int(num_steps_raw[0])
    if num_steps <= 0 or num_steps > MAX_EXPORT_STEPS:
        raise RuntimeError(f"Invalid mp2_num_steps={num_steps}, max={MAX_EXPORT_STEPS}")

    status_slice = fields["mp2_status_id"][:num_steps, :]
    active_slots = np.nonzero(np.any(status_slice != 0, axis=0))[0]
    if total_agents is None:
        if active_slots.size == 0:
            raise RuntimeError("Cannot infer total_agents: mp2_status_id has no active slots")
        total_agents = int(active_slots[-1]) + 1
    if total_agents <= 0 or total_agents > MAX_FRAMES:
        raise RuntimeError(f"Invalid total_agents={total_agents}, max={MAX_FRAMES}")

    return {
        "num_steps": num_steps,
        "days": days_raw[:num_steps].astype(np.uint32, copy=True),
        "fields": fields,
        "total_agents": int(total_agents),
    }


def load_repairline_export(
    export_dir: Path,
    version_id: int,
    repair_quota: int,
    num_steps: int,
    days: np.ndarray,
) -> dict:
    if repair_quota <= 0 or repair_quota > REPAIR_LINES_MAX:
        raise RuntimeError(
            f"Invalid repair_quota={repair_quota}, expected 1..{REPAIR_LINES_MAX}"
        )

    field_to_key = {
        "rl_buf_free_days": "free_days",
        "rl_buf_acn": "acn",
        "rl_buf_rt": "rt",
        "rl_buf_gb": "gb",
        "rl_buf_bank_count": "bank_count",
        "rl_buf_bank_head_start": "bank_head_start",
        "rl_buf_bank_head_end": "bank_head_end",
    }
    result = {
        "num_steps": int(num_steps),
        "days": [int(day) for day in days[:num_steps]],
    }
    for field, key in field_to_key.items():
        raw = _read_u32(export_dir / f"run_{version_id}_{field}.bin", _expected_size(field))
        result[key] = raw.reshape(MAX_EXPORT_STEPS, REPAIR_LINES_MAX)[
            :num_steps, :repair_quota
        ].copy()
    return result


def postprocess_promotions(fields: dict, days: np.ndarray, num_steps: int,
                           total_agents: int, repair_quota: int) -> int:
    modified = 0
    status = fields["mp2_status_id"][:num_steps, :total_agents]
    pre_status = fields["mp2_pre_status_id"][:num_steps, :total_agents]
    group_by = fields["mp2_group_by"][:total_agents]
    idx = fields["mp2_idx"][:total_agents]
    apply_daily_cap = repair_quota > 0

    day_to_agents_set: dict[int, set[int]] = {}
    planner_mask = np.isin(group_by, np.array([1, 2], dtype=np.uint32))
    repair_mask = (status == 4) & planner_mask.reshape(1, total_agents)
    for s in np.nonzero(np.any(repair_mask, axis=1))[0]:
        day = int(days[s])
        day_set = day_to_agents_set.setdefault(day, set())
        day_set.update(int(value) for value in idx[repair_mask[s]])

    event_mask = (fields["mp2_commit_p2"][:num_steps, :total_agents] == 1) | (
        fields["mp2_commit_p3"][:num_steps, :total_agents] == 1
    )
    event_agents = np.nonzero(np.any(event_mask, axis=0))[0]
    for a in event_agents:
        event_steps = np.nonzero(event_mask[:, a])[0]
        for s in event_steps:
            claim_source = int(fields["mp2_repair_claim_source"][s, a])
            if claim_source not in (1, 2):
                continue
            claim_start = int(fields["mp2_repair_claim_start_day"][s, a])
            claim_end = int(fields["mp2_repair_claim_end_day"][s, a])
            if claim_start == 0xFFFFFFFF or claim_end == 0xFFFFFFFF or claim_end <= claim_start:
                continue

            current_status = status[:, a]
            candidate_mask = (
                (days >= claim_start)
                & (days < claim_end)
                & np.isin(current_status, np.array([7, 1], dtype=np.uint32))
                & (pre_status[:, a] == current_status)
            )
            candidate_steps = np.nonzero(candidate_mask)[0]
            if candidate_steps.size == 0:
                continue

            group_by_event = int(group_by[a])
            agent_key = int(idx[a])
            apply_daily_cap_for_agent = apply_daily_cap and group_by_event in (1, 2)
            if apply_daily_cap_for_agent:
                reject_event = False
                for s_back in candidate_steps:
                    day_back = int(days[s_back])
                    day_set = day_to_agents_set.get(day_back)
                    day_len = 0 if day_set is None else len(day_set)
                    has_agent = False if day_set is None else agent_key in day_set
                    if not has_agent and day_len >= repair_quota:
                        reject_event = True
                        break
                if reject_event:
                    continue

            fields["mp2_active_trigger"][s, a] = 1
            fields["mp2_status_id"][candidate_steps, a] = 4
            if candidate_steps.size > 1:
                fields["mp2_pre_status_id"][candidate_steps[1:], a] = 4

            repair_days = np.arange(1, candidate_steps.size + 1, dtype=np.uint32)
            fields["mp2_repair_days"][candidate_steps, a] = repair_days

            assembly_time = int(fields["mp2_assembly_time"][s, a])
            days_to_end = claim_end - days[candidate_steps]
            fields["mp2_assembly_trigger"][candidate_steps, a] = (
                days_to_end <= assembly_time
            ).astype(np.uint32)

            if apply_daily_cap_for_agent:
                for s_back in candidate_steps:
                    day_back = int(days[s_back])
                    day_to_agents_set.setdefault(day_back, set()).add(agent_key)

            fields["mp2_pre_status_id"][s, a] = 4
            modified += int(candidate_steps.size)

    repair_time = fields["mp2_repair_time"][:num_steps, :total_agents].astype(np.int32, copy=False)
    repair_days = fields["mp2_repair_days"][:num_steps, :total_agents].astype(np.int32, copy=False)
    assembly_time = fields["mp2_assembly_time"][:num_steps, :total_agents]
    remaining_repair = np.maximum(repair_time - repair_days, 0)
    fields["mp2_assembly_trigger"][:num_steps, :total_agents] = (
        (status == 4)
        & (assembly_time > 0)
        & (remaining_repair < assembly_time.astype(np.int32, copy=False))
    ).astype(np.uint32)

    return modified


def _u16(arr: np.ndarray) -> np.ndarray:
    return np.bitwise_and(arr, np.uint32(0xFFFF)).astype(np.uint32, copy=False)


def _u8(arr: np.ndarray) -> np.ndarray:
    return np.bitwise_and(arr, np.uint32(0xFF)).astype(np.uint32, copy=False)


def build_master_columns(mp2_data: dict, version_date_int: int, version_id: int) -> tuple[list[list[int]], str]:
    num_steps = mp2_data["num_steps"]
    days = mp2_data["days"]
    fields = mp2_data["fields"]
    total_agents = mp2_data["total_agents"]

    status = fields["mp2_status_id"][:num_steps, :total_agents]
    pre_status = fields["mp2_pre_status_id"][:num_steps, :total_agents]
    mask = status != 0
    row_count = int(np.count_nonzero(mask))
    if row_count <= 0:
        raise RuntimeError("MP2 reconstruction produced zero rows")

    commit_p2 = fields["mp2_commit_p2"][:num_steps, :total_agents].copy()
    commit_p3 = fields["mp2_commit_p3"][:num_steps, :total_agents].copy()
    claim_start = fields["mp2_repair_claim_start_day"][:num_steps, :total_agents].copy()
    claim_end = fields["mp2_repair_claim_end_day"][:num_steps, :total_agents].copy()
    claim_source = fields["mp2_repair_claim_source"][:num_steps, :total_agents].copy()
    claim_line_id = fields["mp2_repair_claim_line_id"][:num_steps, :total_agents].copy()

    spawn_mask = (pre_status == 0) & ((status == 2) | (status == 3))
    commit_p2[spawn_mask] = 0
    commit_p3[spawn_mask] = 0
    claim_source[spawn_mask] = 0
    claim_start[spawn_mask] = 0xFFFF
    claim_end[spawn_mask] = 0xFFFF
    claim_line_id[spawn_mask] = 0xFFFF

    day_2d = np.broadcast_to(days.reshape(num_steps, 1), (num_steps, total_agents))

    def static_2d(field: str) -> np.ndarray:
        return np.broadcast_to(fields[field][:total_agents].reshape(1, total_agents), (num_steps, total_agents))

    arrays = [
        [version_date_int] * row_count,
        [version_id] * row_count,
        _u16(day_2d)[mask].tolist(),
        _u16(static_2d("mp2_idx"))[mask].tolist(),
        static_2d("mp2_aircraft_number")[mask].tolist(),
        _u8(static_2d("mp2_group_by"))[mask].tolist(),
        static_2d("mp2_oh")[mask].tolist(),
        static_2d("mp2_br")[mask].tolist(),
        static_2d("mp2_ll")[mask].tolist(),
        _u8(status)[mask].tolist(),
        _u8(pre_status)[mask].tolist(),
        _u16(fields["mp2_status_change_day"][:num_steps, :total_agents])[mask].tolist(),
        fields["mp2_sne"][:num_steps, :total_agents][mask].tolist(),
        fields["mp2_ppr"][:num_steps, :total_agents][mask].tolist(),
        _u16(fields["mp2_limiter"][:num_steps, :total_agents])[mask].tolist(),
        _u16(fields["mp2_repair_days"][:num_steps, :total_agents])[mask].tolist(),
        _u16(claim_start)[mask].tolist(),
        _u16(claim_end)[mask].tolist(),
        _u8(claim_source)[mask].tolist(),
        _u16(claim_line_id)[mask].tolist(),
        _u16(fields["mp2_repair_time"][:num_steps, :total_agents])[mask].tolist(),
        _u16(fields["mp2_assembly_time"][:num_steps, :total_agents])[mask].tolist(),
        _u8(fields["mp2_active_trigger"][:num_steps, :total_agents])[mask].tolist(),
        _u8(fields["mp2_assembly_trigger"][:num_steps, :total_agents])[mask].tolist(),
        fields["mp2_daily_today"][:num_steps, :total_agents][mask].tolist(),
        fields["mp2_daily_next"][:num_steps, :total_agents][mask].tolist(),
        commit_p2[mask].tolist(),
        commit_p3[mask].tolist(),
    ]

    master_hash = hashlib.sha256()
    for row in zip(*arrays):
        master_hash.update(("|".join(str(value) for value in row) + "\n").encode("ascii"))

    return arrays, master_hash.hexdigest()


def ensure_master_table(client) -> None:
    client.execute("""
        CREATE TABLE IF NOT EXISTS sim_masterv2_v9 (
            version_date UInt32,
            version_id UInt32,
            day_u16 UInt16,
            day_date Date MATERIALIZED addDays(toDate(toString(version_date)), toUInt16(day_u16)),
            idx UInt16,
            aircraft_number UInt32,
            group_by UInt8,
            oh UInt32,
            br UInt32,
            ll UInt32,
            status_id UInt8,
            pre_status_id UInt8,
            status_change_day UInt16,
            sne UInt32,
            ppr UInt32,
            limiter UInt16,
            repair_days UInt16,
            repair_claim_start_day UInt16,
            repair_claim_end_day UInt16,
            repair_claim_source UInt8,
            repair_claim_line_id UInt16,
            repair_time UInt16,
            assembly_time UInt16,
            active_trigger UInt8,
            assembly_trigger UInt8,
            daily_today_u32 UInt32,
            daily_next_u32 UInt32,
            commit_p2 UInt32,
            commit_p3 UInt32
        ) ENGINE = MergeTree()
        PARTITION BY version_date
        ORDER BY (version_date, version_id, day_u16, idx)
    """)


def resolve_repair_quota(repair_quota: int | None) -> int:
    if repair_quota is not None:
        if repair_quota <= 0 or repair_quota > REPAIR_LINES_MAX:
            raise RuntimeError(
                f"repair_quota must be in 1..{REPAIR_LINES_MAX}, got {repair_quota}"
            )
        return int(repair_quota)

    raise RuntimeError(
        "repair_quota is required for ensemble load; pass --repair-quota from the harness"
    )


def insert_master(client, columns_data: list[list[int]], version_date_int: int, version_id: int) -> int:
    row_count = len(columns_data[0])
    if any(len(column) != row_count for column in columns_data):
        raise RuntimeError("Column length mismatch before INSERT")
    ensure_master_table(client)
    client.execute(
        "ALTER TABLE sim_masterv2_v9 DELETE "
        "WHERE version_date = %(vd)s AND version_id = %(vid)s",
        {"vd": version_date_int, "vid": version_id},
        settings={"mutations_sync": 2},
    )
    client.execute(
        f"INSERT INTO sim_masterv2_v9 ({', '.join(MASTER_COLUMNS)}) VALUES",
        columns_data,
        columnar=True,
        settings={"max_partitions_per_insert_block": 300},
    )
    return row_count


def load_master_projection(client, version_date_int: int, version_id: int) -> list[tuple]:
    return client.execute(
        "SELECT aircraft_number, group_by, day_u16, status_id, pre_status_id, "
        "commit_p2, commit_p3, repair_claim_line_id, repair_claim_start_day, "
        "repair_claim_end_day, repair_claim_source "
        "FROM sim_masterv2_v9 "
        "WHERE version_date=%(vd)s AND version_id=%(vid)s",
        {"vd": version_date_int, "vid": version_id},
    )


def _where(version_date_int: int, version_id: int, extra: str = "") -> str:
    suffix = f" AND {extra}" if extra else ""
    return f"version_date = {version_date_int} AND version_id = {version_id}{suffix}"


def _except_count(client, version_date_int: int, left_vid: int, right_vid: int,
                  extra: str = "") -> int:
    cols = ", ".join(MASTER_COMPARE_COLUMNS)
    query = (
        "SELECT count() FROM ("
        f"SELECT {cols} FROM sim_masterv2_v9 WHERE {_where(version_date_int, left_vid, extra)} "
        "EXCEPT "
        f"SELECT {cols} FROM sim_masterv2_v9 WHERE {_where(version_date_int, right_vid, extra)}"
        ")"
    )
    return int(client.execute(query)[0][0])


def _repairline_except_count(client, version_date_int: int, left_vid: int, right_vid: int,
                             extra: str = "") -> int:
    cols = ", ".join(REPAIRLINE_COMPARE_COLUMNS)
    query = (
        "SELECT count() FROM ("
        f"SELECT {cols} FROM sim_repairline_v9 WHERE {_where(version_date_int, left_vid, extra)} "
        "EXCEPT "
        f"SELECT {cols} FROM sim_repairline_v9 WHERE {_where(version_date_int, right_vid, extra)}"
        ")"
    )
    return int(client.execute(query)[0][0])


def compare_versions(client, version_date_int: int, ensemble_vid: int, baseline_vid: int) -> dict:
    result = {
        "row_count_ensemble": int(client.execute(
            "SELECT count() FROM sim_masterv2_v9 WHERE version_date=%(vd)s AND version_id=%(vid)s",
            {"vd": version_date_int, "vid": ensemble_vid},
        )[0][0]),
        "row_count_baseline": int(client.execute(
            "SELECT count() FROM sim_masterv2_v9 WHERE version_date=%(vd)s AND version_id=%(vid)s",
            {"vd": version_date_int, "vid": baseline_vid},
        )[0][0]),
        "except_ensemble_minus_baseline": _except_count(client, version_date_int, ensemble_vid, baseline_vid),
        "except_baseline_minus_ensemble": _except_count(client, version_date_int, baseline_vid, ensemble_vid),
        "gb1_ensemble_minus_baseline": _except_count(
            client, version_date_int, ensemble_vid, baseline_vid, "group_by = 1"
        ),
        "gb1_baseline_minus_ensemble": _except_count(
            client, version_date_int, baseline_vid, ensemble_vid, "group_by = 1"
        ),
        "gb2_idx286_346_ensemble_minus_baseline": _except_count(
            client, version_date_int, ensemble_vid, baseline_vid, "group_by = 2 AND idx BETWEEN 286 AND 346"
        ),
        "gb2_idx286_346_baseline_minus_ensemble": _except_count(
            client, version_date_int, baseline_vid, ensemble_vid, "group_by = 2 AND idx BETWEEN 286 AND 346"
        ),
    }
    return result


def compare_repairline_versions(
    client,
    version_date_int: int,
    ensemble_vid: int,
    baseline_vid: int,
) -> dict:
    return {
        "row_count_ensemble": int(client.execute(
            "SELECT count() FROM sim_repairline_v9 WHERE version_date=%(vd)s AND version_id=%(vid)s",
            {"vd": version_date_int, "vid": ensemble_vid},
        )[0][0]),
        "row_count_baseline": int(client.execute(
            "SELECT count() FROM sim_repairline_v9 WHERE version_date=%(vd)s AND version_id=%(vid)s",
            {"vd": version_date_int, "vid": baseline_vid},
        )[0][0]),
        "line_count_ensemble": int(client.execute(
            "SELECT uniqExact(line_id) FROM sim_repairline_v9 "
            "WHERE version_date=%(vd)s AND version_id=%(vid)s",
            {"vd": version_date_int, "vid": ensemble_vid},
        )[0][0]),
        "line_count_baseline": int(client.execute(
            "SELECT uniqExact(line_id) FROM sim_repairline_v9 "
            "WHERE version_date=%(vd)s AND version_id=%(vid)s",
            {"vd": version_date_int, "vid": baseline_vid},
        )[0][0]),
        "except_ensemble_minus_baseline": _repairline_except_count(
            client, version_date_int, ensemble_vid, baseline_vid
        ),
        "except_baseline_minus_ensemble": _repairline_except_count(
            client, version_date_int, baseline_vid, ensemble_vid
        ),
        "gb1_ensemble_minus_baseline": _repairline_except_count(
            client, version_date_int, ensemble_vid, baseline_vid, "group_by = 1"
        ),
        "gb1_baseline_minus_ensemble": _repairline_except_count(
            client, version_date_int, baseline_vid, ensemble_vid, "group_by = 1"
        ),
        "gb2_ensemble_minus_baseline": _repairline_except_count(
            client, version_date_int, ensemble_vid, baseline_vid, "group_by = 2"
        ),
        "gb2_baseline_minus_ensemble": _repairline_except_count(
            client, version_date_int, baseline_vid, ensemble_vid, "group_by = 2"
        ),
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load CUDAEnsemble MP2/RL .bin into V9 ClickHouse tables")
    parser.add_argument("--version-date", required=True, help="YYYY-MM-DD or YYYYMMDD")
    parser.add_argument("--version-id", type=int, required=True, help="Target disposable version_id")
    parser.add_argument("--baseline-version-id", type=int, default=3)
    parser.add_argument("--export-dir", default=str(MP2_EXPORT_DIR))
    parser.add_argument("--total-agents", type=int, default=None)
    parser.add_argument("--repair-quota", type=int, required=True)
    parser.add_argument("--skip-repairline", action="store_true")
    parser.add_argument("--compare", action="store_true")
    parser.add_argument("--require-bit-identical", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    version_date_int = _parse_version_date(args.version_date)
    export_dir = Path(args.export_dir)
    client = get_client()

    t0 = time.perf_counter()
    mp2_data = load_mp2_export(export_dir, args.version_id, args.total_agents)
    repair_quota = resolve_repair_quota(args.repair_quota)
    pp_count = postprocess_promotions(
        mp2_data["fields"], mp2_data["days"], mp2_data["num_steps"],
        mp2_data["total_agents"], repair_quota
    )
    columns_data, master_sha256 = build_master_columns(mp2_data, version_date_int, args.version_id)
    t_load = time.perf_counter()
    row_count = insert_master(client, columns_data, version_date_int, args.version_id)
    t_insert = time.perf_counter()
    repairline_rows = None

    if not args.skip_repairline:
        rl_data = load_repairline_export(
            export_dir,
            args.version_id,
            repair_quota,
            mp2_data["num_steps"],
            mp2_data["days"],
        )
        rl_rows = interpolate_repairline_daily(rl_data, repair_quota)
        master_projection = load_master_projection(client, version_date_int, args.version_id)
        export_repairline_to_ch(
            client,
            rl_rows,
            version_date_int,
            args.version_id,
            master_projection=master_projection,
        )
        repairline_rows = len(rl_rows)
    t_repairline = time.perf_counter()

    print(
        "ensemble_mp2_loader "
        f"version_date={version_date_int} version_id={args.version_id} "
        f"num_steps={mp2_data['num_steps']} total_agents={mp2_data['total_agents']} "
        f"repair_quota={repair_quota} pp_modified={pp_count} rows={row_count} "
        f"repairline_rows={repairline_rows} "
        f"master_rows_sha256={master_sha256} load_build_s={t_load - t0:.3f} "
        f"insert_s={t_insert - t_load:.3f} repairline_s={t_repairline - t_insert:.3f} "
        f"total_s={t_repairline - t0:.3f}"
    )

    if args.compare or args.require_bit_identical:
        compare = compare_versions(client, version_date_int, args.version_id, args.baseline_version_id)
        for key, value in compare.items():
            print(f"compare_master_{key}={value}")
        repairline_compare = {}
        if not args.skip_repairline:
            repairline_compare = compare_repairline_versions(
                client, version_date_int, args.version_id, args.baseline_version_id
            )
            for key, value in repairline_compare.items():
                print(f"compare_repairline_{key}={value}")
        if args.require_bit_identical:
            diff_failures = {
                f"master_{key}": value for key, value in compare.items()
                if not key.startswith("row_count_") and value != 0
            }
            diff_failures.update({
                f"repairline_{key}": value for key, value in repairline_compare.items()
                if not key.startswith(("row_count_", "line_count_")) and value != 0
            })
            row_mismatch = compare["row_count_ensemble"] != compare["row_count_baseline"]
            if repairline_compare:
                row_mismatch = (
                    row_mismatch
                    or repairline_compare["row_count_ensemble"] != repairline_compare["row_count_baseline"]
                    or repairline_compare["line_count_ensemble"] != repairline_compare["line_count_baseline"]
                )
            if diff_failures or row_mismatch:
                raise RuntimeError(
                    f"Bit-identical check failed: row_mismatch={row_mismatch}, failures={diff_failures}"
                )


if __name__ == "__main__":
    main()
