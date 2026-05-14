#!/usr/bin/env python3
"""preToolUse hook: блокирует SSOT-операции без явного workflow-scoped human approval.

Защищает (без изменений):
- правки JSON SSOT в config/transitions/*.json
- команды sync-domain-graph через Shell

Требование допуска (workflow-scoped, Variant B с явным selection):
- при ровно одном active workflow — он выбирается автоматически;
- при нескольких active — payload (Shell command / ApplyPatch text /
  tool_input) должен содержать литерал W_<workflow_id> для одного из
  active workflows; иначе deny с ambiguity reason;
- для выбранного workflow зарегистрирован approval_request|approval_gate context;
- в user_comm_audit.log есть строка с workflow_id=<выбранного> и approval_hint=yes;
- KG unavailable → fail-safe deny.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
from kg_io import load_agent_kg

REPO_ROOT = Path(__file__).resolve().parents[2]
AUDIT_LOG = Path(__file__).resolve().parent / "user_comm_audit.log"
APPROVAL_CONTEXT_TYPES = {"approval_request", "approval_gate", "pending_approval"}
AUDIT_SCAN_LINES = 50

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


def _load_agent_kg() -> Tuple[Dict[str, Any], str]:
    return load_agent_kg()


def _active_workflows(data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Returns {workflow_id: workflow_dict} только для status=active."""
    workflows = data.get("workflows", [])
    if not isinstance(workflows, list):
        return {}

    active: Dict[str, Dict[str, Any]] = {}
    for item in workflows:
        if not isinstance(item, dict):
            continue
        workflow_id = str(item.get("workflow_id") or "")
        if workflow_id and str(item.get("status") or "").lower() == "active":
            active[workflow_id] = item
    return active


def _has_approval_request_context(data: Dict[str, Any], workflow_id: str) -> bool:
    """True если в data.contexts есть запись workflow_id+context_type ∈ APPROVAL_CONTEXT_TYPES."""
    contexts = data.get("contexts", [])
    if not isinstance(contexts, list):
        return False

    for item in contexts:
        if not isinstance(item, dict):
            continue
        if str(item.get("workflow_id") or "") != workflow_id:
            continue
        if str(item.get("context_type") or "") in APPROVAL_CONTEXT_TYPES:
            return True
    return False


def _audit_flags(line: str) -> Dict[str, str]:
    wf_match = re.search(r"workflow_id=([^\s]+)", line)
    ap_match = re.search(r"approval_hint=([^\s]+)", line)
    return {
        "workflow_id": wf_match.group(1) if wf_match else "N/A",
        "approval_hint": ap_match.group(1).lower() if ap_match else "no",
    }


def _has_approval_for_workflow(workflow_id: str) -> bool:
    """True если последняя approval_hint=yes строка в AUDIT_LOG относится к workflow_id."""
    if not AUDIT_LOG.exists():
        return False

    try:
        with AUDIT_LOG.open("r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
    except OSError:
        return False

    for line in reversed(lines[-AUDIT_SCAN_LINES:]):
        flags = _audit_flags(line)
        if flags.get("approval_hint") != "yes":
            continue
        return flags.get("workflow_id") == workflow_id
    return False


def _workflow_ids_in_text(text: str) -> List[str]:
    return re.findall(r"\bW_[A-Za-z0-9_:-]+\b", text or "")


def _select_workflow(active: Dict[str, Dict[str, Any]], payload_text: str) -> Tuple[str, str]:
    """Returns (workflow_id, reason). workflow_id="" если selection невозможен."""
    if len(active) == 1:
        return next(iter(active)), ""
    if len(active) == 0:
        return "", "нет active workflow в Agent KG; SSoT-операции запрещены."
    referenced = sorted(set(_workflow_ids_in_text(payload_text)) & set(active))
    if len(referenced) == 1:
        return referenced[0], ""
    if len(referenced) > 1:
        return "", (
            f"ambiguous: payload ссылается на несколько active workflow ({', '.join(referenced)}); "
            "оставь явное упоминание ровно одного W_<id>."
        )
    return "", (
        f"ambiguous active workflows ({', '.join(sorted(active))}); "
        "укажи явно W_<id> в payload (Shell command / ApplyPatch description) "
        "или закрой лишние."
    )


def _check_workflow_scoped_approval(payload_text: str = "") -> Tuple[bool, str]:
    data, state = _load_agent_kg()
    if state != "ok":
        return False, f"SSOT-gate: Agent KG unavailable ({state}); fail-safe deny."
    active = _active_workflows(data)
    wid, ambig_reason = _select_workflow(active, payload_text)
    if not wid:
        return False, f"SSOT-gate: {ambig_reason}"
    if not _has_approval_request_context(data, wid):
        return False, (
            f"SSOT-gate: для active workflow {wid} нет approval_request context в Agent KG. "
            f"Зарегистрируй: python3 code/utils/agent_kg.py --register-approval-request --workflow-id {wid} --content '...'"
        )
    if not _has_approval_for_workflow(wid):
        return False, (
            f"SSOT-gate: context зарегистрирован для {wid}, но нет user confirmation (approval_hint=yes) "
            f"в user_comm_audit.log для этого workflow. Получи явное подтверждение пользователя."
        )
    return True, ""


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    tool_name = str(payload.get("tool_name") or "")

    # Sync-domain-graph guard
    if tool_name == "Shell":
        command = _extract_shell_command(payload)
        if any(marker in command for marker in SYNC_COMMAND_MARKERS):
            ok, reason = _check_workflow_scoped_approval(command)
            if not ok:
                _deny(reason)
                return

    # SSoT JSON ApplyPatch guard
    if tool_name == "ApplyPatch":
        patch_text = _extract_applypatch_text(payload)
        touched_paths = _extract_paths_from_patch(patch_text)
        sensitive_paths = [p for p in touched_paths if _is_sensitive_ssot_path(p)]
        if sensitive_paths:
            ok, reason = _check_workflow_scoped_approval(patch_text)
            if not ok:
                _deny(f"{reason} Затронуты: {', '.join(sensitive_paths)}")
                return

    _allow()


if __name__ == "__main__":
    main()

