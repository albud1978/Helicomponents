#!/usr/bin/env python3
"""preToolUse hook: жестко блокирует runtime-операции в "ядре" Superset.

Политика:
- Этот агент не управляет локальным Superset runtime (ядро).
- Запрещены любые shell-операции в deploy/superset-local.
- Отдельно запрещены контейнерные/оркестрационные команды (docker/compose/systemd и т.д.).
"""

from __future__ import annotations

import json
import re
import sys
from typing import Any, Dict


RUNTIME_CMD_PATTERN = re.compile(
    r"(^|[\s;&|])(?:docker|docker-compose|podman|nerdctl|kubectl|systemctl|service)(?=($|[\s;&|]))"
)
CORE_MARKERS = (
    "deploy/superset-local",
    "start_local.sh",
    "start_local_plugin.sh",
    "stop_local.sh",
    "build_superset_with_plugin.sh",
    "init-superset.sh",
    "superset-local",
    "superset-db-local",
    "superset-redis-local",
    "superset-gateway-local",
)
BLOCK_EDIT_PATH_MARKERS = (
    "deploy/superset-local/.env",
    "deploy/superset-local/.env.example",
    "deploy/superset-local/docker-compose.yml",
    "deploy/superset-local/docker-compose.plugin.yml",
    "deploy/superset-local/init-superset.sh",
    "deploy/superset-local/start_local.sh",
    "deploy/superset-local/start_local_plugin.sh",
    "deploy/superset-local/stop_local.sh",
)
ADMIN_ENV_MARKERS = (
    "superset_admin_username",
    "superset_admin_firstname",
    "superset_admin_lastname",
    "superset_admin_email",
    "superset_admin_password",
)


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


def _extract_tool_input(payload: Dict[str, Any]) -> Dict[str, Any]:
    tool_input = payload.get("tool_input")
    if isinstance(tool_input, dict):
        return tool_input
    return {}


def _extract_command(payload: Dict[str, Any]) -> str:
    tool_input = _extract_tool_input(payload)
    for key in ("command", "cmd", "text"):
        value = tool_input.get(key)
        if isinstance(value, str) and value:
            return value
    top_level = payload.get("command")
    if isinstance(top_level, str):
        return top_level
    return ""


def _extract_working_directory(payload: Dict[str, Any]) -> str:
    tool_input = _extract_tool_input(payload)
    for key in ("working_directory", "workingDirectory", "cwd"):
        value = tool_input.get(key)
        if isinstance(value, str) and value:
            return value
    for key in ("working_directory", "workingDirectory", "cwd"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _extract_applypatch_text(payload: Dict[str, Any]) -> str:
    tool_input = payload.get("tool_input")
    if isinstance(tool_input, str):
        return tool_input
    if isinstance(tool_input, dict):
        for key in ("patch", "content", "text"):
            value = tool_input.get(key)
            if isinstance(value, str):
                return value
    return ""


def _extract_paths_from_patch(patch_text: str) -> list[str]:
    paths: list[str] = []
    for line in patch_text.splitlines():
        if line.startswith("*** Update File: "):
            paths.append(line.replace("*** Update File: ", "", 1).strip().lower())
        elif line.startswith("*** Add File: "):
            paths.append(line.replace("*** Add File: ", "", 1).strip().lower())
    return [p for p in paths if p]


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    tool_name = _extract_tool_name(payload)
    tool_name_l = tool_name.lower()

    if tool_name_l == "applypatch":
        patch_text = _extract_applypatch_text(payload)
        patch_l = patch_text.lower()
        touched_paths = _extract_paths_from_patch(patch_text)
        touches_core_runtime_files = any(
            any(marker in path for marker in BLOCK_EDIT_PATH_MARKERS) for path in touched_paths
        )
        touches_admin_env = any(marker in patch_l for marker in ADMIN_ENV_MARKERS)
        if touches_core_runtime_files or touches_admin_env:
            _deny(
                "Запрещено: правки bootstrap/runtime-контура Superset (deploy/superset-local/**, "
                "docker compose, init/start scripts, SUPERSET_ADMIN_*) недоступны для этого агента. "
                "Передайте задачу агенту ядра."
            )
            return
        _allow()
        return

    if tool_name_l != "shell":
        _allow()
        return

    command = _extract_command(payload).strip()
    working_dir = _extract_working_directory(payload).strip()
    command_l = command.lower()
    wd_l = working_dir.lower()

    # 1) Любые контейнерные/оркестрационные runtime команды запрещены
    if command and (
        RUNTIME_CMD_PATTERN.search(command_l) or "superset fab create-admin" in command_l
    ):
        _deny(
            "Запрещено: runtime-операции ядра (docker/compose/systemctl/service и аналоги) "
            "недоступны для этого агента. Передайте задачу агенту, который ведет ядро."
        )
        return

    # 2) Любые shell-операции в контуре superset-local также запрещены
    if any(marker in command_l for marker in CORE_MARKERS) or any(marker in wd_l for marker in CORE_MARKERS):
        _deny(
            "Запрещено: любые shell-операции в ядре Superset (deploy/superset-local и связанные скрипты/контейнеры) "
            "для этого агента недоступны. Передайте задачу агенту ядра."
        )
        return

    _allow()


if __name__ == "__main__":
    main()

