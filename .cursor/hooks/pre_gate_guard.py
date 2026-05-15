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
from typing import Any, Dict, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent))
from kg_io import load_agent_kg

KG_RELATIVE_PATH = Path("config/agent_kg.json")
WORKFLOW_RE = re.compile(r"\bW_[A-Za-z0-9_:-]+\b")
RISK_TIER_RE = re.compile(r"\brisk[_\s-]?tier\s*[:=]\s*(low|medium|high)\b", re.IGNORECASE)
SUCCESS_CRITERIA_LABEL_RE = re.compile(
    r"success[_\s-]?criteria\s*[:=]",
    re.IGNORECASE,
)
# Verifiable markers: SQL/script/numeric/invariant/INV-N/TEMP-N/GPU-N/acceptance/manual-check (last only OK for low-risk).
SUCCESS_CRITERIA_VERIFIABLE_RE = re.compile(
    r"\b(?:SQL\s*:|script\s*:|numeric\s*:|invariant\s*:|INV-\d|TEMP-\d|GPU-\d|"
    r"acceptance\s*:|manual-check\s*:)",
    re.IGNORECASE,
)
HANDOFF_ARG_KEYS = ("handoff_to", "handoffTo", "next_owner", "nextOwner")
ORCHESTRATOR_TARGETS = {"orchestrator", "оркестратор"}


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
    data, state = load_agent_kg()
    if state != "ok":
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


def _workflow_caps_status(workflow_id: str) -> Tuple[bool, str]:
    """Hard-block dispatch когда workflow caps превышены (C11, Tier-2b).

    Legacy workflows без caps/usage пропускаются (backward-compat).
    Если KG не читается — не блокируем (other guards словят).
    """
    data, state = load_agent_kg()
    if state != "ok":
        return True, ""

    workflows = data.get("workflows", [])
    if not isinstance(workflows, list):
        return True, ""

    for item in workflows:
        if not isinstance(item, dict) or str(item.get("workflow_id")) != workflow_id:
            continue
        caps = item.get("caps")
        usage = item.get("usage")
        if not isinstance(caps, dict) or not isinstance(usage, dict):
            return True, ""

        max_steps = caps.get("max_steps")
        cum_steps = usage.get("cumulative_steps", 0) or 0
        if isinstance(max_steps, int) and isinstance(cum_steps, int) and cum_steps >= max_steps:
            return False, (
                f"caps_exceeded: cumulative_steps={cum_steps} >= max_steps={max_steps}. "
                f"Повысь caps `python3 code/utils/agent_kg.py --set-caps --workflow-id {workflow_id} --max-steps <N>` "
                "или открой новый workflow."
            )

        max_tokens = caps.get("max_tokens")
        cum_tokens = usage.get("cumulative_tokens", 0) or 0
        if isinstance(max_tokens, int) and isinstance(cum_tokens, int) and cum_tokens >= max_tokens:
            return False, (
                f"caps_exceeded: cumulative_tokens={cum_tokens} >= max_tokens={max_tokens}. "
                f"Повысь caps `python3 code/utils/agent_kg.py --set-caps --workflow-id {workflow_id} --max-tokens <N>` "
                "или открой новый workflow."
            )

        return True, ""

    return True, ""


def _has_handoff_to_orchestrator(text: str, tool_input: Dict[str, Any]) -> bool:
    for key in HANDOFF_ARG_KEYS:
        value = tool_input.get(key)
        if isinstance(value, str) and value.strip().lower() in ORCHESTRATOR_TARGETS:
            return True
    lowered = text.lower()
    return "handoff" in lowered and ("orchestrator" in lowered or "оркестратор" in lowered)


def _extract_risk_tier(text: str, tool_input: Dict[str, Any]) -> str:
    for key in ("risk_tier", "riskTier"):
        value = tool_input.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().lower()
    match = RISK_TIER_RE.search(text)
    if match:
        return match.group(1).lower()
    return ""


def _verifiable_success_criteria(text: str, tool_input: Dict[str, Any], risk_tier: str) -> Tuple[bool, str]:
    """Returns (ok, reason). ok=True если SuccessCriteria есть и verifiable для данного риска."""
    candidate_value = ""
    for key in ("success_criteria", "successCriteria"):
        value = tool_input.get(key)
        if isinstance(value, str) and value.strip():
            candidate_value = value
            break
    if not candidate_value and SUCCESS_CRITERIA_LABEL_RE.search(text):
        candidate_value = text
    if not candidate_value:
        return False, "отсутствует"

    match = SUCCESS_CRITERIA_VERIFIABLE_RE.search(candidate_value)
    if not match:
        return False, "значение не содержит verifiable marker (SQL:/script:/numeric:/invariant:/INV-N/TEMP-N/GPU-N/acceptance:/manual-check:)"
    marker = match.group(0).rstrip(":").strip().lower()
    if marker == "manual-check" and risk_tier in {"medium", "high"}:
        return False, "manual-check разрешён только для low-risk; для medium/high требуется SQL/script/numeric/invariant/INV-/TEMP-/GPU-/acceptance"
    return True, ""


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

    caps_ok, caps_reason = _workflow_caps_status(workflow_id)
    if not caps_ok:
        _deny(f"Pre-gate: dispatch subagent заблокирован. {caps_reason}")
        return

    if not _has_handoff_to_orchestrator(combined, tool_input):
        _deny(
            "Pre-gate: dispatch subagent заблокирован. Укажите явный возврат Handoff оркестратору: "
            "либо аргумент `handoff_to=orchestrator` (или `next_owner=orchestrator`), либо явный текст "
            "в prompt с упоминанием `handoff ... orchestrator`."
        )
        return

    risk_tier = _extract_risk_tier(combined, tool_input)
    if risk_tier in {"medium", "high"}:
        ok, reason = _verifiable_success_criteria(combined, tool_input, risk_tier)
        if not ok:
            _deny(
                f"Pre-gate: dispatch subagent заблокирован. SuccessCriteria для {risk_tier}-risk {reason}. "
                "Допустимые формы: `SuccessCriteria: SQL: <query>`, `script: <path>`, `numeric: A == B`, "
                "`invariant: INV-N`, `INV-N`/`TEMP-N`/`GPU-N`, `acceptance: ...`."
            )
            return

    _allow()


if __name__ == "__main__":
    main()
