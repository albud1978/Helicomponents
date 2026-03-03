#!/usr/bin/env python3
"""preToolUse hook: блокирует SSOT-операции без явного human approval.

Защищает:
- правки JSON SSOT в config/transitions/*.json
- команды sync-domain-graph через Shell

Требование допуска:
- в последней записи user_comm_audit.log должен быть approval_hint=yes
- и workflow_id должен быть заполнен (не N/A)
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Dict, List


REPO_ROOT = Path(__file__).resolve().parents[2]
AUDIT_LOG = Path(__file__).resolve().parent / "user_comm_audit.log"

SENSITIVE_SSOT_FILES = {
    "config/transitions/transitions_rules.json",
    "config/transitions/quota_rules.json",
    "config/transitions/invariants.json",
    "config/transitions/transitions_rules_l2_engines.json",
    "config/transitions/quota_rules_l2_engines.json",
}

SYNC_COMMAND_MARKERS = (
    "make sync-domain-graph",
    "make sync-domain-graph-clear",
)


def _allow() -> None:
    sys.stdout.write(json.dumps({"decision": "allow"}))


def _deny(reason: str) -> None:
    sys.stdout.write(json.dumps({"decision": "deny", "reason": reason}))


def _extract_shell_command(payload: Dict[str, object]) -> str:
    tool_input = payload.get("tool_input")
    if isinstance(tool_input, dict):
        cmd = tool_input.get("command")
        if isinstance(cmd, str):
            return cmd
    cmd = payload.get("command")
    if isinstance(cmd, str):
        return cmd
    return ""


def _extract_applypatch_text(payload: Dict[str, object]) -> str:
    tool_input = payload.get("tool_input")
    if isinstance(tool_input, str):
        return tool_input
    if isinstance(tool_input, dict):
        # На случай изменения формата payload.
        for key in ("patch", "content", "text"):
            value = tool_input.get(key)
            if isinstance(value, str):
                return value
    return ""


def _normalize_path(path_str: str) -> str:
    path_str = path_str.strip()
    if not path_str:
        return ""
    p = Path(path_str)
    if p.is_absolute():
        try:
            return p.resolve().relative_to(REPO_ROOT).as_posix()
        except Exception:
            return p.as_posix()
    return p.as_posix()


def _extract_paths_from_patch(patch_text: str) -> List[str]:
    paths: List[str] = []
    for line in patch_text.splitlines():
        if line.startswith("*** Update File: "):
            raw = line.replace("*** Update File: ", "", 1)
            paths.append(_normalize_path(raw))
        elif line.startswith("*** Add File: "):
            raw = line.replace("*** Add File: ", "", 1)
            paths.append(_normalize_path(raw))
    return [p for p in paths if p]


def _is_sensitive_ssot_path(path_rel: str) -> bool:
    if path_rel in SENSITIVE_SSOT_FILES:
        return True
    return path_rel.startswith("config/transitions/") and path_rel.endswith(".json")


def _read_last_audit_flags() -> Dict[str, str]:
    if not AUDIT_LOG.exists():
        return {"workflow_id": "N/A", "approval_hint": "no"}

    try:
        with AUDIT_LOG.open("r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
    except OSError:
        return {"workflow_id": "N/A", "approval_hint": "no"}

    if not lines:
        return {"workflow_id": "N/A", "approval_hint": "no"}

    last = lines[-1]
    wf_match = re.search(r"workflow_id=([^\s]+)", last)
    ap_match = re.search(r"approval_hint=([^\s]+)", last)
    return {
        "workflow_id": wf_match.group(1) if wf_match else "N/A",
        "approval_hint": ap_match.group(1).lower() if ap_match else "no",
    }


def _has_human_approval() -> bool:
    flags = _read_last_audit_flags()
    workflow_ok = flags.get("workflow_id", "N/A") not in {"", "N/A", "n/a"}
    approval_ok = flags.get("approval_hint", "no") == "yes"
    return workflow_ok and approval_ok


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    tool_name = str(payload.get("tool_name") or "")

    # 1) Блок sync-domain-graph без human approval
    if tool_name == "Shell":
        command = _extract_shell_command(payload)
        if any(marker in command for marker in SYNC_COMMAND_MARKERS):
            if not _has_human_approval():
                _deny(
                    "SSOT-gate: sync-domain-graph заблокирован без явного human approval. "
                    "Добавьте подтверждение пользователя (approval_hint=yes) и workflow_id (например W_<id>) в текущем чате."
                )
                return

    # 2) Блок правок config/transitions/*.json без human approval
    if tool_name == "ApplyPatch":
        patch_text = _extract_applypatch_text(payload)
        touched_paths = _extract_paths_from_patch(patch_text)
        sensitive_paths = [p for p in touched_paths if _is_sensitive_ssot_path(p)]
        if sensitive_paths and not _has_human_approval():
            _deny(
                "SSOT-gate: правка JSON SSOT заблокирована без явного human approval. "
                f"Затронуты: {', '.join(sensitive_paths)}. "
                "Нужно подтверждение пользователя и workflow_id в текущем чате."
            )
            return

    _allow()


if __name__ == "__main__":
    main()

