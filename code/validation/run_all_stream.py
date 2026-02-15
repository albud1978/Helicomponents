#!/usr/bin/env python3
"""
Потоковый раннер всех валидаторов из invariants.json.
"""
import argparse
import json
import os
import re
import subprocess
import sys
from typing import Dict, List, Tuple

from ch_client import get_client


DATASET_RE = re.compile(r"^(?P<date>\d{8}):(?P<id>\d+)$")
TABLE_RE = re.compile(r"^[A-Za-z0-9_]+(\.[A-Za-z0-9_]+)?$")


def validate_table_name(table: str) -> str:
    if not TABLE_RE.match(table):
        raise SystemExit(f"Некорректное имя таблицы: {table}")
    return table


def format_dataset(version_date: int, version_id: int) -> str:
    return f"{int(version_date):08d}:{int(version_id)}"


def parse_dataset_arg(value: str) -> Tuple[int, int]:
    match = DATASET_RE.match(value.strip())
    if not match:
        raise SystemExit(
            f"Некорректный формат --dataset: {value}. Ожидается YYYYMMDD:ID"
        )
    return int(match.group("date")), int(match.group("id"))


def load_validators(
    invariants_path: str, project_root: str
) -> Tuple[List[Tuple[str, List[str]]], List[str]]:
    with open(invariants_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    ordered_paths: List[str] = []
    path_to_ids: Dict[str, List[str]] = {}
    inv_id_order: List[str] = []

    for section in ("global_invariants", "temporal_invariants"):
        for inv in data.get(section, []):
            validator = inv.get("validator")
            if not validator:
                continue
            inv_id = inv.get("id", "UNKNOWN")
            if inv_id not in inv_id_order:
                inv_id_order.append(inv_id)
            if validator not in path_to_ids:
                path_to_ids[validator] = []
                ordered_paths.append(validator)
            path_to_ids[validator].append(inv_id)

    validators: List[Tuple[str, List[str]]] = []
    missing: List[str] = []
    for validator in ordered_paths:
        if os.path.isabs(validator):
            script_path = validator
        else:
            script_path = os.path.join(project_root, validator)
        script_path = os.path.abspath(script_path)
        if not os.path.exists(script_path):
            missing.append(validator)
        validators.append((script_path, path_to_ids[validator]))

    if missing:
        print("Не найдены файлы валидаторов:")
        for item in missing:
            print(f"  - {item}")
        raise SystemExit(2)

    return validators, inv_id_order


def detect_datasets(table: str) -> List[Tuple[int, int]]:
    client = get_client()
    query = f"""
    SELECT version_date, version_id
    FROM {table}
    WHERE group_by IN (1, 2)
    GROUP BY version_date, version_id
    ORDER BY version_date, version_id
    """
    rows = client.execute(query)
    datasets: List[Tuple[int, int]] = []
    for version_date, version_id in rows:
        if version_date is None or version_id is None:
            continue
        datasets.append((int(version_date), int(version_id)))
    return datasets


def print_datasets(datasets: List[Tuple[int, int]]) -> None:
    if not datasets:
        print("Датасеты не найдены.")
        return
    print(f"Найдено датасетов: {len(datasets)}")
    for version_date, version_id in datasets:
        print(f"  {format_dataset(version_date, version_id)}")


def stream_process(cmd: List[str]) -> int:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        env=env,
    )
    if process.stdout is not None:
        for line in process.stdout:
            print(line, end="")
    return process.wait()


def run_validator(
    script_path: str,
    inv_ids: List[str],
    dataset: Tuple[int, int],
    table_main: str,
    table_repair: str,
) -> int:
    version_date, version_id = dataset
    dataset_key = format_dataset(version_date, version_id)
    script_name = os.path.basename(script_path)
    inv_label = "/".join(inv_ids)
    table = table_repair if script_name == "inv3_repair_capacity.py" else table_main

    print("\n" + "=" * 80)
    print(f"DATASET {dataset_key} | {inv_label} -> {script_name}")
    print("=" * 80)

    cmd = [
        sys.executable,
        script_path,
        "--version-id",
        str(version_id),
        "--version-date",
        str(version_date),
        "--table",
        table,
    ]
    return stream_process(cmd)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Потоковый запуск валидаций из invariants.json"
    )
    parser.add_argument(
        "--list-datasets",
        action="store_true",
        help="Вывести доступные датасеты и выйти",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Формат YYYYMMDD:ID (можно несколько раз)",
    )
    parser.add_argument(
        "--all-datasets",
        action="store_true",
        help="Запуск по всем обнаруженным датасетам",
    )
    parser.add_argument(
        "--table-main",
        default="sim_masterv2_v9",
        help="Основная таблица ClickHouse (по умолчанию: sim_masterv2_v9)",
    )
    parser.add_argument(
        "--table-repair",
        default="sim_repairline_v9",
        help="Таблица RepairLine (по умолчанию: sim_repairline_v9)",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Остановиться при первом FAIL",
    )
    args = parser.parse_args()

    if args.dataset and args.all_datasets:
        print("Нельзя одновременно указывать --dataset и --all-datasets.")
        return 2

    base_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(base_dir, "..", ".."))
    invariants_path = os.path.join(
        project_root, "config", "transitions", "invariants.json"
    )
    if not os.path.exists(invariants_path):
        print(f"Не найден файл invariants.json: {invariants_path}")
        return 2

    table_main = validate_table_name(args.table_main)
    table_repair = validate_table_name(args.table_repair)

    validators, inv_id_order = load_validators(invariants_path, project_root)
    if not validators:
        print("В invariants.json нет валидаторов для запуска.")
        return 2

    datasets = detect_datasets(table_main)

    if args.list_datasets:
        print_datasets(datasets)
        return 0

    if not datasets:
        print("Датасеты не найдены в таблице.")
        return 2

    dataset_index = {
        format_dataset(version_date, version_id): (version_date, version_id)
        for version_date, version_id in datasets
    }

    selected: List[Tuple[int, int]] = []
    selected_keys: List[str] = []
    if args.all_datasets:
        selected = list(datasets)
        selected_keys = [format_dataset(vd, vid) for vd, vid in selected]
    else:
        for value in args.dataset:
            version_date, version_id = parse_dataset_arg(value)
            key = format_dataset(version_date, version_id)
            if key not in dataset_index:
                print(f"Датасет не найден: {key}")
                return 2
            if key not in selected_keys:
                selected_keys.append(key)
                selected.append(dataset_index[key])

    if not selected:
        print("Не выбраны датасеты для запуска. Используйте --dataset или --all-datasets.")
        print_datasets(datasets)
        return 2

    results: Dict[str, Dict[str, Tuple[str, int]]] = {}
    total = 0
    passed = 0
    failed = 0
    stopped_early = False

    for dataset in selected:
        dataset_key = format_dataset(*dataset)
        results[dataset_key] = {}
        for script_path, inv_ids in validators:
            rc = run_validator(
                script_path,
                inv_ids,
                dataset,
                table_main=table_main,
                table_repair=table_repair,
            )
            status = "PASS" if rc == 0 else "FAIL"
            for inv_id in inv_ids:
                results[dataset_key][inv_id] = (status, rc)
                total += 1
                if rc == 0:
                    passed += 1
                else:
                    failed += 1
            if rc != 0 and args.fail_fast:
                stopped_early = True
                break
        if stopped_early:
            break

    print("\n" + "=" * 80)
    print("SUMMARY")
    for dataset_key in selected_keys:
        print(f"\nDataset {dataset_key}")
        dataset_results = results.get(dataset_key, {})
        for inv_id in inv_id_order:
            if inv_id in dataset_results:
                status, rc = dataset_results[inv_id]
                print(f"  {inv_id}: {status} (rc={rc})")
            else:
                print("  {0}: SKIPPED (rc=NA)".format(inv_id))
    if stopped_early:
        print("\nStopped early due to --fail-fast.")
    print(f"\nTOTAL={total} PASSED={passed} FAILED={failed}")
    print("=" * 80)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
