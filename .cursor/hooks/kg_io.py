"""Shared Agent KG read helper for hooks (DRY for orchestrator/ssot/pre_gate/pre_close).

Без cache: каждый hook — отдельный процесс с собственным STDIN; общий
disk-кеш создаёт race и stale-reads. Цель модуля — единое поведение
загрузки и единый формат `state`-кодов при недоступности файла.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Tuple


REPO_ROOT = Path(__file__).resolve().parents[2]
AGENT_KG_PATH = REPO_ROOT / "config" / "agent_kg.json"


def load_agent_kg(path: Path | None = None) -> Tuple[Dict[str, Any], str]:
    """Returns (data, state). state in {ok, unavailable:<exc>, invalid_root}.

    state == "ok" гарантирует, что data — top-level dict.
    Caller обязан проверять state перед использованием data.
    """
    kg_path = path or AGENT_KG_PATH
    try:
        with kg_path.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        return {}, f"unavailable:{type(exc).__name__}"
    if not isinstance(data, dict):
        return {}, "invalid_root"
    return data, "ok"
