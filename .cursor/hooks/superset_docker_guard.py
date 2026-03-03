#!/usr/bin/env python3
"""preToolUse hook: запрещает Docker/Superset runtime операции в этом репозитории.

Политика:
- Superset BI управляется внешним контуром (другой проект).
- В этом репозитории разрешена только API-работа с удалённым Superset.
"""

from __future__ import annotations

import json
import re
import sys
from typing import Any, Dict


DOCKER_PATTERN = re.compile(r"(^|\s)docker(\s|$)")
BLOCK_MARKERS = (
    "deploy/superset-local/",
    "start_local_plugin.sh",
    "build_superset_with_plugin.sh",
    "superset-local",
)


def _allow() -> None:
    sys.stdout.write(json.dumps({"decision": "allow"}))


def _deny(reason: str) -> None:
    sys.stdout.write(json.dumps({"decision": "deny", "reason": reason}))


def _extract_command(payload: Dict[str, Any]) -> str:
    tool_input = payload.get("tool_input")
    if isinstance(tool_input, dict):
        cmd = tool_input.get("command")
        if isinstance(cmd, str):
            return cmd
    cmd = payload.get("command")
    if isinstance(cmd, str):
        return cmd
    return ""


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    if payload.get("tool_name") != "Shell":
        _allow()
        return

    command = _extract_command(payload).strip()
    if not command:
        _allow()
        return

    lower = command.lower()
    has_docker = bool(DOCKER_PATTERN.search(lower))
    has_block_marker = any(marker in lower for marker in BLOCK_MARKERS)
    if has_docker or has_block_marker:
        _deny(
            "Запрещено политикой проекта: Docker/Superset runtime не управляется из этого репозитория. "
            "Используйте только Superset API на http://10.96.96.47:8088/."
        )
        return

    _allow()


if __name__ == "__main__":
    main()

