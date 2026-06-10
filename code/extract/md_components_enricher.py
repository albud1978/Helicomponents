#!/usr/bin/env python3
"""Валидатор Excel SSoT поля md_components.partseqno_i.

`partseqno_i` больше не вычисляется этим шагом. Значение приходит из
`data_input/master_data/MD_Сomponents.xlsx`, а валидатор сверяет его с
`dict_partno_flat.partseqno_i` и fail-fast останавливает ETL при расхождении.

Место в ETL Pipeline:
- ПОСЛЕ: dictionary_creator.py (dict_partno_flat уже создан/актуализирован)
- ПЕРЕД: calculate_beyond_repair.py и md_components_psn_reserve.py
"""

import argparse
import csv
import logging
import sys
from pathlib import Path
from typing import Any

code_root = Path(__file__).resolve().parents[1]
sys.path.append(str(code_root / "utils"))
sys.path.append(str(code_root))
from config_loader import get_clickhouse_client


REPORT_PATH = Path("output/psn_reservation/md_components_enricher_validation_report.csv")


class MDComponentsEnricher:
    """Совместимое имя класса: теперь это валидатор `partseqno_i` из Excel SSoT."""

    def __init__(self):
        self.logger = self._setup_logging()
        self.client = get_clickhouse_client()

    def _setup_logging(self) -> logging.Logger:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        return logging.getLogger(__name__)

    def _require_table(self, table: str) -> None:
        exists = self.client.execute(
            """
            SELECT count()
            FROM system.tables
            WHERE database = currentDatabase() AND name = %(table)s
            """,
            {"table": table},
        )[0][0]
        if not exists:
            raise RuntimeError(f"Отсутствует обязательная таблица {table}")

    def _require_columns(self, table: str, required: set[str]) -> None:
        rows = self.client.execute(
            """
            SELECT name
            FROM system.columns
            WHERE database = currentDatabase() AND table = %(table)s
            """,
            {"table": table},
        )
        columns = {row[0] for row in rows}
        missing = sorted(required - columns)
        if missing:
            raise RuntimeError(f"В таблице {table} отсутствуют обязательные колонки: {missing}")

    @staticmethod
    def _clean_partno(value: Any) -> str:
        if value is None:
            return ""
        return str(value).replace("\n", "")

    def _load_md_components(self) -> dict[str, int | None]:
        rows = self.client.execute(
            """
            SELECT partno, partseqno_i
            FROM md_components
            WHERE partno IS NOT NULL AND partno != ''
            ORDER BY partno
            """
        )
        result: dict[str, int | None] = {}
        duplicates: list[str] = []
        for partno_raw, partseqno_i in rows:
            partno = self._clean_partno(partno_raw)
            if partno in result:
                duplicates.append(partno)
            result[partno] = None if partseqno_i is None else int(partseqno_i)
        if duplicates:
            raise RuntimeError(f"Дубли очищенного partno в md_components: {sorted(set(duplicates))}")
        if not result:
            raise RuntimeError("md_components пустая или не содержит partno")
        return result

    def _load_partno_dict(self) -> dict[str, int]:
        rows = self.client.execute(
            """
            SELECT partno, partseqno_i
            FROM dict_partno_flat
            WHERE partno IS NOT NULL AND partno != ''
            ORDER BY partno
            """
        )
        result: dict[str, int] = {}
        duplicates: list[str] = []
        null_ids: list[str] = []
        for partno_raw, partseqno_i in rows:
            partno = self._clean_partno(partno_raw)
            if partno in result:
                duplicates.append(partno)
            if partseqno_i is None:
                null_ids.append(partno)
            else:
                result[partno] = int(partseqno_i)
        if duplicates:
            raise RuntimeError(f"Дубли очищенного partno в dict_partno_flat: {sorted(set(duplicates))}")
        if null_ids:
            raise RuntimeError(f"NULL partseqno_i в dict_partno_flat: {null_ids[:20]}")
        if not result:
            raise RuntimeError("dict_partno_flat пустая или не содержит partno")
        return result

    def validate_enrichment(self) -> bool:
        """Сверяет Excel-загруженный `partseqno_i` с AMOS-ID из словаря."""
        self._require_table("md_components")
        self._require_table("dict_partno_flat")
        self._require_columns("md_components", {"partno", "partseqno_i"})
        self._require_columns("dict_partno_flat", {"partno", "partseqno_i"})

        md_by_partno = self._load_md_components()
        dict_by_partno = self._load_partno_dict()
        report_rows: list[dict[str, Any]] = []
        violations: list[str] = []
        missing_from_dict: list[str] = []
        matched = 0

        for partno, excel_partseqno_i in sorted(md_by_partno.items()):
            dict_partseqno_i = dict_by_partno.get(partno)
            if dict_partseqno_i is None:
                status = "missing_dict_expected_null" if excel_partseqno_i is None else "missing_dict_but_excel_filled"
                missing_from_dict.append(partno)
                if excel_partseqno_i is not None:
                    violations.append(
                        f"{partno}: отсутствует в dict_partno_flat, но Excel partseqno_i={excel_partseqno_i}"
                    )
            elif excel_partseqno_i is None:
                status = "excel_null_but_dict_filled"
                violations.append(f"{partno}: Excel partseqno_i=NULL, dict_partno_flat={dict_partseqno_i}")
            elif excel_partseqno_i != dict_partseqno_i:
                status = "mismatch"
                violations.append(
                    f"{partno}: Excel partseqno_i={excel_partseqno_i}, dict_partno_flat={dict_partseqno_i}"
                )
            else:
                status = "matched"
                matched += 1

            report_rows.append(
                {
                    "partno": partno,
                    "excel_partseqno_i": excel_partseqno_i,
                    "dict_partseqno_i": dict_partseqno_i,
                    "status": status,
                }
            )

        if missing_from_dict:
            violations.append(
                "Все номенклатуры md_components должны быть в dict_partno_flat "
                f"(heli_pandas + md_components SSoT supplement), отсутствуют: {missing_from_dict}"
            )

        REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
        with REPORT_PATH.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=["partno", "excel_partseqno_i", "dict_partseqno_i", "status"],
            )
            writer.writeheader()
            writer.writerows(report_rows)

        total = len(md_by_partno)
        self.logger.info("📊 partseqno_i validation: matched=%s total=%s report=%s", matched, total, REPORT_PATH)
        self.logger.info("ℹ️ Номенклатура без AMOS-ID: %s", missing_from_dict)

        if violations:
            preview = "\n".join(violations[:20])
            raise RuntimeError(f"partseqno_i validation failed ({len(violations)} violations):\n{preview}")

        self.logger.info("✅ partseqno_i validation passed")
        return True

    def run_enrichment(self) -> bool:
        """Совместимый интерфейс прежнего ETL-шагa."""
        self.logger.info("🚀 Запуск валидатора md_components.partseqno_i из Excel SSoT")
        return self.validate_enrichment()


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate md_components.partseqno_i from Excel SSoT")
    parser.add_argument("--version-date", type=str, help="Совместимость с extract_master; не используется")
    parser.add_argument("--version-id", type=int, help="Совместимость с extract_master; не используется")
    parser.parse_args()

    print("🚀 === ВАЛИДАТОР MD_COMPONENTS.partseqno_i ===")
    enricher = MDComponentsEnricher()
    enricher.run_enrichment()
    print("🎯 partseqno_i: Excel SSoT согласован с dict_partno_flat")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())