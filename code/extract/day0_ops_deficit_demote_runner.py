#!/usr/bin/env python3
"""Day-0 OPS deficit demote runner."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any

code_root = Path(__file__).resolve().parents[1]
repo_root = code_root.parent
sys.path.append(str(code_root))
sys.path.append(str(code_root / "utils"))

from config_loader import get_clickhouse_client  # type: ignore
from extract.deficit_demoter import apply_demotions, select_demotions  # type: ignore
from extract.deficit_rank_calculator import calculate_deficit_ranking  # type: ignore
from extract.ops_target_comparator import compare_ops_to_target  # type: ignore


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Day-0 demote OPS planers by aggregate deficit until OPS matches MP4 target"
    )
    parser.add_argument("--version-date", required=True, help="Slice date YYYY-MM-DD")
    parser.add_argument("--version-id", type=int, default=1, help="Slice version_id")
    parser.add_argument("--dry-run", action="store_true", help="Do not mutate heli_pandas")
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Audit output directory; default output/day0_ops_deficit_demote_<vd>_v<vid>",
    )
    return parser.parse_args()


def _json_default(value: Any) -> str:
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _records(frame) -> list[dict[str, Any]]:
    return json.loads(frame.to_json(orient="records", force_ascii=False, date_format="iso"))


def _write_outputs(output_dir: Path, name: str, frame) -> None:
    frame.to_csv(output_dir / f"{name}.csv", index=False)
    frame.to_excel(output_dir / f"{name}.xlsx", index=False)


def main() -> int:
    args = parse_args()
    version_date = date.fromisoformat(args.version_date)
    version_id = int(args.version_id)
    output_dir = (
        Path(args.output_dir)
        if args.output_dir
        else repo_root / "output" / f"day0_ops_deficit_demote_{version_date}_v{version_id}"
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    client = get_clickhouse_client()
    print(
        f"Day0 OPS deficit demote: version_date={version_date}, "
        f"version_id={version_id}, dry_run={'ON' if args.dry_run else 'OFF'}"
    )
    print(f"Output: {output_dir}")

    ranking_path = output_dir / "ranking.xlsx"
    ranking = calculate_deficit_ranking(
        client,
        version_date,
        version_id,
        output_path=ranking_path,
    )
    before = compare_ops_to_target(client, version_date, version_id)
    before.to_csv(output_dir / "ops_target_before.csv", index=False)
    before.to_excel(output_dir / "ops_target_before.xlsx", index=False)

    demoted = select_demotions(ranking, before)
    _write_outputs(output_dir, "demoted", demoted)
    mutation_stats = apply_demotions(
        client,
        version_date,
        version_id,
        demoted,
        dry_run=args.dry_run,
    )

    after = compare_ops_to_target(client, version_date, version_id)
    after.to_csv(output_dir / "ops_target_after.csv", index=False)
    after.to_excel(output_dir / "ops_target_after.xlsx", index=False)

    demoted_acn = [int(value) for value in demoted.get("aircraft_number", []).tolist()]
    summary = {
        "version_date": version_date,
        "version_id": version_id,
        "dry_run": bool(args.dry_run),
        "ranking_path": str(ranking_path),
        "demoted_count": len(demoted_acn),
        "demoted_acn": demoted_acn,
        "before": _records(before),
        "after": _records(after),
        "mutation_stats": mutation_stats,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=_json_default),
        encoding="utf-8",
    )

    before_line = ", ".join(
        f"gb{int(row.group_by)} ops={int(row.ops_count)} target={int(row.target)} "
        f"excess={int(row.excess)}"
        for row in before.itertuples(index=False)
    )
    after_line = ", ".join(
        f"gb{int(row.group_by)} ops={int(row.ops_count)} target={int(row.target)} "
        f"excess={int(row.excess)}"
        for row in after.itertuples(index=False)
    )
    print(f"Before: {before_line}")
    print(f"Selected demotions: {len(demoted_acn)} aircraft")
    if demoted_acn:
        print("Demoted aircraft_number: " + ", ".join(str(value) for value in demoted_acn))
    print(f"Mutation stats: {mutation_stats}")
    print(f"After: {after_line}")
    print(f"Summary: {output_dir / 'summary.json'}")
    if args.dry_run:
        print("DRY-RUN completed without mutations")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
