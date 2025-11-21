#!/usr/bin/env python3
"""
Подсчёт количества агрегатов по partno в таблице heli_pandas.

Особенности:
- Использует стандартное подключение через utils.config_loader.get_clickhouse_client
- По умолчанию берёт самую свежую версию данных (version_date, version_id)
- Позволяет ограничить выборку и задать минимальное количество экземпляров на partno
"""

from __future__ import annotations

import argparse
from datetime import datetime
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple

# Подключение конфигурации (единый порядок доступа к БД)
sys.path.append(str(Path(__file__).resolve().parent / "utils"))
from config_loader import get_clickhouse_client  # type: ignore


@dataclass(frozen=True)
class VersionInfo:
    version_date: str
    version_id: int


def resolve_version(
    client,
    version_date: Optional[str],
    version_id: Optional[int],
) -> VersionInfo:
    """Определяет версию данных. Если не передана, берём максимальную."""
    if version_date and version_id is not None:
        return VersionInfo(version_date=version_date, version_id=version_id)

    rows: Sequence[Tuple[str, int]] = client.execute(
        """
        SELECT toString(version_date) AS version_date,
               toUInt8(version_id) AS version_id
        FROM heli_pandas
        ORDER BY version_date DESC, version_id DESC
        LIMIT 1
        """
    )
    if not rows:
        raise RuntimeError("Таблица heli_pandas пуста, нет версий для анализа")
    latest_date, latest_id = rows[0]
    return VersionInfo(version_date=latest_date, version_id=int(latest_id))


def fetch_stats(
    client,
    version: VersionInfo,
    min_count: int,
    limit: Optional[int],
) -> List[Tuple[str, int, int]]:
    """Возвращает список (partno, количество экземпляров, количество ВС)."""
    params = {
        "version_date": version.version_date,
        "version_id": version.version_id,
        "min_count": max(min_count, 1),
    }

    limit_clause = ""
    if limit is not None and limit > 0:
        limit_clause = "LIMIT %(limit)s"
        params["limit"] = limit

    query = f"""
        SELECT partno,
               count() AS components_total,
               uniqExact(aircraft_number) AS aircrafts_total
        FROM heli_pandas
        WHERE partno IS NOT NULL
          AND partno != ''
          AND version_date = %(version_date)s
          AND version_id = %(version_id)s
        GROUP BY partno
        HAVING components_total >= %(min_count)s
        ORDER BY components_total DESC, partno ASC
        {limit_clause}
    """
    return [
        (partno, int(total), int(aircrafts))
        for partno, total, aircrafts in client.execute(query, params)
    ]


def print_stats(version: VersionInfo, rows: Sequence[Tuple[str, int, int]]) -> None:
    """Печатает агрегированную статистику."""
    if not rows:
        print("⚠️ Под условия выборки не попало ни одного агрегата.")
        return

    total_records = sum(total for _, total, _ in rows)
    header = f"\n📊 Агрегация heli_pandas по partno (версия {version.version_date} v{version.version_id})"
    print(header)
    print("-" * len(header))
    print(f"{'partno':<30} {'components':>10} {'aircrafts':>10}")
    print("-" * 54)
    for partno, total, aircrafts in rows:
        print(f"{partno:<30} {total:>10} {aircrafts:>10}")
    print("-" * 54)
    print(f"Всего строк в отчёте: {len(rows)}, суммарно агрегатов: {total_records}")


def build_markdown(version: VersionInfo, rows: Sequence[Tuple[str, int, int]]) -> str:
    """Формирует markdown-отчёт."""
    total_rows = len(rows)
    total_records = sum(total for _, total, _ in rows)
    lines = [
        "# Агрегация `heli_pandas` по `partno`",
        "",
        "- Источник: `heli_pandas` после `extract_master.py`",
        f"- Версия данных: `{version.version_date} v{version.version_id}`",
        f"- Сгенерировано: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "",
        "| partno | components | aircrafts |",
        "| --- | ---: | ---: |",
    ]
    for partno, total, aircrafts in rows:
        lines.append(f"| {partno} | {total} | {aircrafts} |")
    lines.append("")
    lines.append(f"Всего строк: **{total_rows}**, суммарно агрегатов: **{total_records}**.")
    lines.append("")
    return "\n".join(lines)


def write_markdown(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Подсчёт количества агрегатов по partno в heli_pandas"
    )
    parser.add_argument(
        "--version-date",
        type=str,
        help="Дата версии (YYYY-MM-DD). По умолчанию берётся последняя в таблице",
    )
    parser.add_argument(
        "--version-id",
        type=int,
        help="ID версии данных. По умолчанию берётся последний",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Ограничить количество partno в отчёте (например 20). По умолчанию выводятся все",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=1,
        help="Минимальное количество экземпляров на partno",
    )
    parser.add_argument(
        "--md-path",
        type=str,
        help="Путь для сохранения Markdown-отчёта. По умолчанию docs/heli_pandas_partno_stats_<version>.md",
    )
    parser.add_argument(
        "--skip-md",
        action="store_true",
        help="Не сохранять Markdown-отчёт на диск",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    client = get_clickhouse_client()

    version = resolve_version(client, args.version_date, args.version_id)
    rows = fetch_stats(client, version, args.min_count, args.limit)
    print_stats(version, rows)

    if not args.skip_md:
        if rows:
            default_path = Path(
                f"docs/heli_pandas_partno_stats_{version.version_date}.md"
            )
            target_path = Path(args.md_path) if args.md_path else default_path
            markdown = build_markdown(version, rows)
            write_markdown(target_path, markdown)
            print(f"📝 Отчёт сохранён в {target_path}")
        else:
            print("ℹ️ Отчёт не сохранён: таблица пуста.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

