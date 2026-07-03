#!/usr/bin/env python3
"""preToolUse hook: тотальный запрет DROP/TRUNCATE таблиц ClickHouse без явного согласия Алексея.

Политика (решение Алексея 2026-07-03, после инцидента с --drop-table,
уничтожившим все version_id в sim_masterv2_v9/sim_repairline_v9):
- Любой DROP TABLE / DROP DATABASE / TRUNCATE — только с явного согласия Алексея в текущем чате.
- Флаг --drop-table у orchestrator_limiter_v8.py пересоздаёт ВСЮ таблицу (не срез) — запрещён.
- drop_extract_objects.py — массовое удаление extract-объектов — запрещён.

Bypass (только после явного согласия Алексея в текущем чате):
добавить в команду переменную DROP_APPROVED_BY_ALEXEY=1 — токен виден в чате и аудируем.
"""

from __future__ import annotations

import json
import re
import sys
from typing import Any, Dict

DROP_PATTERN = re.compile(
    r"\bdrop\s+table\b|\bdrop\s+database\b|\btruncate\s+table\b|\btruncate\s+\w+\.|"
    r"--drop-table\b|drop_extract_objects\.py",
    re.IGNORECASE,
)
BYPASS_TOKEN = "DROP_APPROVED_BY_ALEXEY=1"


def _allow() -> None:
    sys.stdout.write(json.dumps({"decision": "allow"}))


def _deny(reason: str) -> None:
    sys.stdout.write(json.dumps({"decision": "deny", "reason": reason}))


def _extract_tool_name(payload: Dict[str, Any]) -> str:
    for key in ("tool_name", "toolName", "name"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _extract_command(payload: Dict[str, Any]) -> str:
    tool_input = payload.get("tool_input")
    if isinstance(tool_input, dict):
        for key in ("command", "cmd", "text"):
            value = tool_input.get(key)
            if isinstance(value, str) and value:
                return value
    top_level = payload.get("command")
    if isinstance(top_level, str):
        return top_level
    return ""


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    if _extract_tool_name(payload).lower() != "shell":
        _allow()
        return

    command = _extract_command(payload)
    if not command or not DROP_PATTERN.search(command):
        _allow()
        return

    if BYPASS_TOKEN in command:
        _allow()
        return

    _deny(
        "ЗАПРЕЩЕНО: DROP/TRUNCATE таблиц и флаг --drop-table — тотальный запрет "
        "(решение Алексея 2026-07-03). Удаление таблиц только с явного согласия "
        "Алексея в текущем чате; после согласия добавь в команду "
        f"{BYPASS_TOKEN}. Для очистки среза используй идемпотентные загрузчики "
        "(они чистят только свой version_date/version_id). "
        "См. docs/runbook_sim_launch.md."
    )


if __name__ == "__main__":
    main()
