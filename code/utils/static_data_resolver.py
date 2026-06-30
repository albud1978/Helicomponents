"""
Пути и срезы ClickHouse для статических Excel (без версионирования по DWH).

Program_heli/Program — из последнего data_input/source_data/v_* (метка = дата прогона в CH).
MD_Сomponents, Economics — только data_input/master_data (как md_components: без привязки к version_date heli).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence, Tuple

from dataset_manager import DatasetManager, DatasetInfo

REPO_ROOT = Path(__file__).resolve().parents[2]
SOURCE_DATA_ROOT = REPO_ROOT / "data_input" / "source_data"
MASTER_DATA_ROOT = REPO_ROOT / "data_input" / "master_data"


@dataclass(frozen=True)
class StaticSlice:
    version_date: str
    version_id: int
    source: str


def get_latest_source_dataset() -> Optional[DatasetInfo]:
    manager = DatasetManager(str(SOURCE_DATA_ROOT))
    return manager.get_latest_dataset()


def latest_source_dataset_label() -> str:
    dataset = get_latest_source_dataset()
    if dataset is None:
        return "нет датасетов в source_data"
    return f"{dataset.name} ({dataset.version_date.isoformat()})"


def resolve_static_excel(*candidates: str) -> Path:
    """
    Ищет Excel: сначала в последнем source_data/v_*, затем в master_data.
    """
    if not candidates:
        raise ValueError("resolve_static_excel: нужен хотя бы один candidate")

    search_dirs: list[Path] = []
    dataset = get_latest_source_dataset()
    if dataset is not None:
        search_dirs.append(dataset.path)
    search_dirs.append(MASTER_DATA_ROOT)

    for directory in search_dirs:
        for name in candidates:
            path = directory / name
            if path.exists():
                return path

    dirs = ", ".join(str(d) for d in search_dirs)
    names = ", ".join(candidates)
    raise FileNotFoundError(f"Статический Excel не найден ({names}) в: {dirs}")


def resolve_master_data_excel(*candidates: str) -> Path:
    """SSoT master_data: MD, Economics и др. (не ищем в source_data/v_*)."""
    if not candidates:
        raise ValueError("resolve_master_data_excel: нужен хотя бы один candidate")
    for name in candidates:
        path = MASTER_DATA_ROOT / name
        if path.exists():
            return path
    names = ", ".join(candidates)
    raise FileNotFoundError(
        f"Master-data Excel не найден ({names}) в {MASTER_DATA_ROOT}"
    )


def resolve_economics_workbook() -> Path:
    return resolve_master_data_excel("Economics.xlsx")


def resolve_md_components_workbook() -> Path:
    return resolve_master_data_excel("MD_Сomponents.xlsx", "MD_Components.xlsx")


def resolve_latest_md_slice(client) -> StaticSlice:
    rows: Sequence[Tuple[str, int]] = client.execute(
        """
        SELECT toString(version_date) AS version_date,
               toUInt8(version_id) AS version_id
        FROM md_components
        ORDER BY version_date DESC, version_id DESC
        LIMIT 1
        """
    )
    if not rows:
        raise RuntimeError(
            "Таблица md_components пуста — загрузите MD_Сomponents.xlsx "
            f"({resolve_md_components_workbook()})"
        )
    version_date, version_id = rows[0]
    return StaticSlice(
        version_date=version_date,
        version_id=int(version_id),
        source="clickhouse",
    )
