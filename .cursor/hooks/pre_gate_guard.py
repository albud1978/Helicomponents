#!/usr/bin/env python3
"""preToolUse hook: dispatch pre-gate для dispatch subagent-ов.

Operational rules:
- любой dispatch subagent должен быть привязан к workflow_id
- workflow_id должен существовать в Agent KG и быть active
- prompt должен явно требовать возврат Handoff оркестратору
- сам hook не заменяет `governance-compliance`; governance вызывается оркестратором отдельно по risk/policy
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict


KG_RELATIVE_PATH = Path("config/agent_kg.json")
WORKFLOW_RE = re.compile(r"\bW_[A-Za-z0-9_:-]+\b")


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
    for key in ("tool_input", "toolInput", "arguments", "params", "input"):
        value = payload.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _extract_text(tool_input: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = tool_input.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return ""


def _extract_workflow_id(text: str, payload: Dict[str, Any], tool_input: Dict[str, Any]) -> str:
    for key in ("workflow_id", "workflowId"):
        for source in (tool_input, payload):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

    match = WORKFLOW_RE.search(text)
    if match:
        return match.group(0)
    return ""


def _workflow_is_active(repo_root: Path, workflow_id: str) -> bool:
    kg_path = repo_root / KG_RELATIVE_PATH
    if not kg_path.exists():
        return False
    try:
        data = json.loads(kg_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return False

    workflows = data.get("workflows", [])
    if not isinstance(workflows, list):
        return False
    for item in workflows:
        if not isinstance(item, dict):
            continue
        if str(item.get("workflow_id")) != workflow_id:
            continue
        return str(item.get("status") or "").lower() == "active"
    return False


def _has_handoff_to_orchestrator(text: str) -> bool:
    lowered = text.lower()
    return "handoff" in lowered and ("orchestrator" in lowered or "оркестратор" in lowered)


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    tool_name = _extract_tool_name(payload).lower()
    if tool_name not in {"subagent", "task"}:
        _allow()
        return

    tool_input = _extract_tool_input(payload)
    prompt = _extract_text(tool_input, "prompt")
    description = _extract_text(tool_input, "description")
    combined = f"{description}\n{prompt}".strip()

    if not combined:
        _deny(
            "Pre-gate: dispatch subagent заблокирован. Пустой prompt/description недопустим."
        )
        return

    workflow_id = _extract_workflow_id(combined, payload, tool_input)
    if not workflow_id:
        _deny(
            "Pre-gate: dispatch subagent заблокирован. Укажите `workflow_id`/`W_<id>` в prompt или arguments."
        )
        return

    repo_root = Path(__file__).resolve().parents[2]
    if not _workflow_is_active(repo_root, workflow_id):
        _deny(
            f"Pre-gate: dispatch subagent заблокирован. Workflow `{workflow_id}` отсутствует в Agent KG "
            "или не находится в статусе active."
        )
        return

    if not _has_handoff_to_orchestrator(combined):
        _deny(
            "Pre-gate: dispatch subagent заблокирован. Prompt должен явно требовать возврат Handoff оркестратору."
        )
        return

    _allow()


if __name__ == "__main__":
    main()
