#!/usr/bin/env python3
"""
Spawn cap curve layer 1.

This module is a pure superstructure over the existing Limiter V8 orchestrator:
it derives the cap step shape from real input data, runs one cap by reading input
from a shared source version, and collects curve metrics from output versions.
WARNING: --run starts GPU simulation and writes simulation outputs. Import,
--dry-run, --validate, and --collect do not start GPU simulation; --collect only
writes a local CSV file.
"""
from __future__ import annotations

import argparse
import csv
import sys
from datetime import date
from pathlib import Path
from typing import Iterable

CODE_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = CODE_ROOT.parent
for path in (CODE_ROOT, CODE_ROOT / "utils", CODE_ROOT / "sim_v2" / "messaging"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from sim_env_setup import get_client, prepare_env_arrays  # noqa: E402

DEFAULT_VERSION_DATE = "2026-06-22"
DEFAULT_CSV_PATH = "output/spawn_cap_curve_20260622.csv"
CSV_COLUMNS = (
    "cap",
    "version_id",
    "births",
    "deficit_total",
    "deficit_post180",
    "unmet_hours_total",
    "unmet_hours_post180",
    "d_deficit_per_cap",
    "d_unmet_hours_per_cap",
)


def _parse_version_date(version_date: str | date) -> date:
    if isinstance(version_date, date):
        return version_date
    return date.fromisoformat(str(version_date))


def _version_date_int(version_date: str | date) -> int:
    return int(_parse_version_date(version_date).strftime("%Y%m%d"))


def _cap_range(cap_min: int, cap_max: int) -> list[int]:
    if cap_min < 0 or cap_max < 0:
        raise ValueError("cap-min/cap-max must be non-negative")
    if cap_min > cap_max:
        raise ValueError("cap-min must be <= cap-max")
    return list(range(cap_min, cap_max + 1))


def _load_reference_env(client, version_date: str | date, ref_vid: int) -> dict:
    return prepare_env_arrays(client, _parse_version_date(version_date), ref_vid)


def _first_nonzero(values: list[int]) -> int:
    for idx, value in enumerate(values):
        if int(value) > 0:
            return idx
    raise ValueError("reference spawn_limit_cumulative has no positive values")


def get_reference_shape(
    client,
    version_date: str | date,
    ref_vid: int = 9,
) -> tuple[int, int]:
    """Return (threshold, days_total) from the real reference cap version."""
    env = _load_reference_env(client, version_date, ref_vid)
    real_cum = [int(value) for value in env["spawn_limit_cumulative"]]
    return _first_nonzero(real_cum), len(real_cum)


def synth_cumulative(threshold: int, days_total: int, cap_N: int) -> list[int]:
    if threshold < 0 or days_total <= 0 or threshold >= days_total:
        raise ValueError("invalid threshold/days_total for cap synthesis")
    if cap_N < 0:
        raise ValueError("cap_N must be non-negative")
    return [0] * threshold + [int(cap_N)] * (days_total - threshold)


def synth_seed(threshold: int, days_total: int, cap_N: int) -> list[int]:
    if threshold < 0 or days_total <= 0 or threshold >= days_total:
        raise ValueError("invalid threshold/days_total for seed synthesis")
    if cap_N < 0:
        raise ValueError("cap_N must be non-negative")
    seed = [0] * days_total
    if cap_N > 0:
        seed[threshold] = int(cap_N)
    return seed


def validate_synthesis(
    client,
    version_date: str | date,
    ref_vid: int = 9,
    ref_cap: int = 60,
) -> dict[str, int]:
    """Read-only correctness gate: synthetic ref_cap must match real ref_vid."""
    env = _load_reference_env(client, version_date, ref_vid)
    real_cum = [int(value) for value in env["spawn_limit_cumulative"]]
    real_seed = [int(value) for value in env["mp4_spawn_limit_seed"]]
    threshold = _first_nonzero(real_cum)
    days_total = len(real_cum)

    expected_cum = synth_cumulative(threshold, days_total, ref_cap)
    expected_seed = synth_seed(threshold, days_total, ref_cap)
    if expected_cum != real_cum:
        raise AssertionError(
            f"spawn_limit_cumulative synthesis mismatch for ref_vid={ref_vid}, ref_cap={ref_cap}"
        )
    if expected_seed != real_seed:
        raise AssertionError(
            f"mp4_spawn_limit_seed synthesis mismatch for ref_vid={ref_vid}, ref_cap={ref_cap}"
        )
    if int(env.get("spawn_limit_active", 0)) != 1:
        raise AssertionError(f"spawn_limit_active is not 1 for ref_vid={ref_vid}")
    return {"threshold": threshold, "days_total": days_total, "ref_cap": ref_cap}


def run_one_cap(
    version_date: str,
    cap: int,
    out_base: int = 100,
    end_day: int = 3650,
    src_vid: int = 3,
) -> None:
    """Run one cap through Limiter V8. This starts GPU simulation and writes outputs."""
    if cap < 0:
        raise ValueError("cap must be non-negative")

    from orchestrator_limiter_v8 import LimiterV8Orchestrator  # noqa: PLC0415

    client = get_client()
    version_id = out_base + int(cap)
    orchestrator = LimiterV8Orchestrator(
        version_date,
        end_day=end_day,
        clickhouse_client=client,
        version_id=version_id,
        input_version_id=src_vid,
    )
    orchestrator.prepare_data()

    threshold, days_total = get_reference_shape(client, version_date, ref_vid=9)
    env = orchestrator.env_data
    env["spawn_limit_cumulative"] = synth_cumulative(threshold, days_total, cap)
    env["spawn_limit_active"] = 1
    env["mp4_spawn_limit_seed"] = synth_seed(threshold, days_total, cap)
    orchestrator._collect_deterministic_dates()

    orchestrator.build_model()
    orchestrator.run()


def _births(client, version_date: str | date, version_id: int) -> int:
    return int(
        client.execute(
            """
            SELECT uniqExact(aircraft_number)
            FROM sim_masterv2_v9
            WHERE version_date = %(version_date_int)s
              AND version_id = %(version_id)s
              AND group_by = 2
              AND aircraft_number >= 100000
            """,
            {"version_date_int": _version_date_int(version_date), "version_id": version_id},
        )[0][0]
        or 0
    )


def _deficit_by_day(client, version_date: str | date, version_id: int) -> list[tuple[date, int]]:
    rows = client.execute(
        """
        SELECT day_date, greatest(deficit, 0) AS positive_deficit
        FROM sim_deficit_v9_daily
        WHERE version_date = %(version_date_int)s
          AND version_id = %(version_id)s
          AND group_by = 2
        ORDER BY day_date
        """,
        {"version_date_int": _version_date_int(version_date), "version_id": version_id},
    )
    return [(row[0], int(row[1] or 0)) for row in rows]


def _mi17_minutes_by_day(client, version_date: str | date, src_vid: int = 3) -> dict[date, float]:
    rows = client.execute(
        """
        SELECT dates, sum(daily_hours) AS total_minutes, count() AS frame_count
        FROM flight_program_fl
        WHERE version_date = toDate(%(version_date)s)
          AND version_id = %(src_vid)s
          AND ac_type_mask = 64
        GROUP BY dates
        ORDER BY dates
        """,
        {"version_date": _parse_version_date(version_date).isoformat(), "src_vid": src_vid},
    )
    minutes_by_day: dict[date, float] = {}
    for day, total_minutes, frame_count in rows:
        count = int(frame_count or 0)
        minutes_by_day[day] = (float(total_minutes or 0) / count) if count else 0.0
    return minutes_by_day


def _metrics_for_version(
    client,
    version_date: str | date,
    version_id: int,
    src_vid: int = 3,
) -> dict[str, int | float]:
    deficits = _deficit_by_day(client, version_date, version_id)
    minutes_by_day = _mi17_minutes_by_day(client, version_date, src_vid=src_vid)
    post180 = _parse_version_date(version_date).toordinal() + 180

    deficit_total = 0
    deficit_post180 = 0
    unmet_minutes_total = 0.0
    unmet_minutes_post180 = 0.0
    for day, deficit in deficits:
        avg_minutes = minutes_by_day.get(day, 0.0)
        unmet_minutes = float(deficit) * avg_minutes
        deficit_total += deficit
        unmet_minutes_total += unmet_minutes
        if day.toordinal() >= post180:
            deficit_post180 += deficit
            unmet_minutes_post180 += unmet_minutes

    return {
        "births": _births(client, version_date, version_id),
        "deficit_total": deficit_total,
        "deficit_post180": deficit_post180,
        "unmet_hours_total": unmet_minutes_total / 60.0,
        "unmet_hours_post180": unmet_minutes_post180 / 60.0,
    }


def collect_curve(
    client,
    version_date: str | date,
    caps: Iterable[int],
    out_base: int = 100,
    baseline_vid: int = 3,
    src_vid: int = 3,
) -> list[dict[str, int | float | None]]:
    """Collect read-only curve metrics for cap versions and baseline cap=61."""
    rows: list[dict[str, int | float | None]] = []
    for cap in sorted(int(value) for value in caps):
        version_id = out_base + cap
        row: dict[str, int | float | None] = {"cap": cap, "version_id": version_id}
        row.update(_metrics_for_version(client, version_date, version_id, src_vid=src_vid))
        rows.append(row)

    baseline: dict[str, int | float | None] = {"cap": 61, "version_id": baseline_vid}
    baseline.update(_metrics_for_version(client, version_date, baseline_vid, src_vid=baseline_vid))
    rows.append(baseline)
    rows.sort(key=lambda item: int(item["cap"]))

    for idx, row in enumerate(rows):
        if idx + 1 >= len(rows):
            row["d_deficit_per_cap"] = None
            row["d_unmet_hours_per_cap"] = None
            continue
        next_row = rows[idx + 1]
        row["d_deficit_per_cap"] = int(next_row["deficit_total"]) - int(row["deficit_total"])
        row["d_unmet_hours_per_cap"] = float(next_row["unmet_hours_total"]) - float(
            row["unmet_hours_total"]
        )
    return rows


def write_curve(rows: list[dict], csv_path: str | Path) -> Path:
    path = Path(csv_path)
    if not path.is_absolute():
        path = REPO_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({col: "" if row.get(col) is None else row.get(col) for col in CSV_COLUMNS})
    return path


def _print_plan(args: argparse.Namespace, caps: list[int]) -> None:
    actions = []
    if args.run:
        actions.append("run simulations (GPU + ClickHouse writes)")
    if args.collect:
        actions.append(f"collect metrics -> {args.csv_path}")
    if args.validate:
        actions.append("validate synthetic cap shape (read-only)")
    if not actions:
        actions.append("dry-run only")

    print("DRY RUN: no ClickHouse writes, no GPU simulation")
    print(
        f"version_date={args.version_date} src_vid={args.src_vid} "
        f"out_base={args.out_base} end_day={args.end_day}"
    )
    print("actions=" + ", ".join(actions))
    for cap in caps:
        print(f"  cap={cap:02d}: input vid{args.src_vid} -> output vid{args.out_base + cap}")
    print("  baseline cap=61: input vid3 -> output vid3")
    print("  validate: synth cap=60 == ref vid9")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build and collect spawn cap curve runs.")
    parser.add_argument("--version-date", default=DEFAULT_VERSION_DATE)
    parser.add_argument("--cap-min", type=int, default=0)
    parser.add_argument("--cap-max", type=int, default=60)
    parser.add_argument("--out-base", type=int, default=100)
    parser.add_argument("--src-vid", type=int, default=3)
    parser.add_argument("--end-day", type=int, default=3650)
    parser.add_argument("--csv-path", default=DEFAULT_CSV_PATH)
    parser.add_argument("--run", action="store_true")
    parser.add_argument("--collect", action="store_true")
    parser.add_argument("--validate", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    caps = _cap_range(args.cap_min, args.cap_max)
    has_action = args.run or args.collect or args.validate

    if args.dry_run or not has_action:
        _print_plan(args, caps)
        return 0

    client = get_client()
    if args.validate:
        result = validate_synthesis(client, args.version_date)
        print(
            "validate: OK "
            f"threshold={result['threshold']} days_total={result['days_total']} ref_cap={result['ref_cap']}"
        )
    if args.run:
        for cap in caps:
            print(f"run: cap={cap} input_vid={args.src_vid} output_vid={args.out_base + cap}")
            run_one_cap(
                args.version_date,
                cap,
                out_base=args.out_base,
                end_day=args.end_day,
                src_vid=args.src_vid,
            )
    if args.collect:
        rows = collect_curve(client, args.version_date, caps, out_base=args.out_base, src_vid=args.src_vid)
        path = write_curve(rows, args.csv_path)
        print(f"collect: wrote {len(rows)} rows -> {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
