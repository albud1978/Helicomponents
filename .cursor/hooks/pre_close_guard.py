#!/usr/bin/env python3
"""preToolUse hook: блокирует --close-workflow без governance/docs handoff.

Проверка применяется только к Shell-вызовам `code/utils/agent_kg.py --close-workflow`.
Для закрытия workflow должны существовать handoff от:
- governance-compliance
- docs-curator
"""

import json
import re
import shlex
import sys
from pathlib import Path
from typing import Dict, List


REQUIRED_AGENTS = ("governance-compliance", "docs-curator")
KG_RELATIVE_PATH = Path("config/agent_kg.json")


def _extract_shell_command(payload: Dict[str, object]) -> str:
    tool_input = payload.get("tool_input")
    if isinstance(tool_input, dict):
        command = tool_input.get("command")
        if isinstance(command, str):
            return command
    command = payload.get("command")
    if isinstance(command, str):
        return command
    return ""


def _is_close_workflow_command(command: str) -> bool:
    if "agent_kg.py" not in command:
        return False
    return "--close-workflow" in command


def _extract_workflow_id(command: str) -> str:
    try:
        tokens = shlex.split(command)
    except ValueError:
        tokens = command.split()

    for idx, token in enumerate(tokens):
        if token == "--workflow-id" and idx + 1 < len(tokens):
            return tokens[idx + 1].strip()
        if token.startswith("--workflow-id="):
            return token.split("=", 1)[1].strip()

    fallback = re.search(r"--workflow-id(?:=|\s+)([A-Za-z0-9_:-]+)", command)
    if fallback:
        return fallback.group(1).strip()
    return ""


def _load_agent_kg(repo_root: Path) -> Dict[str, object]:
    kg_path = repo_root / KG_RELATIVE_PATH
    with kg_path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _missing_required_handoffs(kg_data: Dict[str, object], workflow_id: str) -> List[str]:
    found = set()
    handoffs = kg_data.get("handoffs", [])
    if not isinstance(handoffs, list):
        return list(REQUIRED_AGENTS)

    for item in handoffs:
        if not isinstance(item, dict):
            continue
        if item.get("workflow_id") != workflow_id:
            continue
        agent = item.get("agent")
        if agent in REQUIRED_AGENTS:
            found.add(agent)

    return [agent for agent in REQUIRED_AGENTS if agent not in found]


def _deny(reason: str) -> None:
    sys.stdout.write(json.dumps({"decision": "deny", "reason": reason}))


def _allow() -> None:
    sys.stdout.write(json.dumps({"decision": "allow"}))


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    if payload.get("tool_name") != "Shell":
        _allow()
        return

    command = _extract_shell_command(payload)
    if not _is_close_workflow_command(command):
        _allow()
        return

    workflow_id = _extract_workflow_id(command)
    if not workflow_id:
        _deny(
            "Закрытие workflow заблокировано: в команде отсутствует --workflow-id. "
            "Укажите workflow-id и повторите."
        )
        return

    repo_root = Path(__file__).resolve().parents[2]
    kg_path = repo_root / KG_RELATIVE_PATH
    if not kg_path.exists():
        _deny(
            "Закрытие workflow заблокировано: не найден config/agent_kg.json. "
            "Невозможно проверить governance/docs handoff."
        )
        return

    try:
        kg_data = _load_agent_kg(repo_root)
    except (OSError, json.JSONDecodeError):
        _deny(
            "Закрытие workflow заблокировано: не удалось прочитать config/agent_kg.json."
        )
        return

    missing = _missing_required_handoffs(kg_data, workflow_id)
    if missing:
        missing_text = ", ".join(missing)
        _deny(
            "Закрытие workflow заблокировано: отсутствуют обязательные handoff "
            f"для {workflow_id}: {missing_text}. "
            "Сначала получите и запишите handoff, затем повторите --close-workflow."
        )
        return

    _allow()


if __name__ == "__main__":
    main()
