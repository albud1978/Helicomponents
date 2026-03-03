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
from typing import Dict, List, Tuple


REQUIRED_AGENTS = ("governance-compliance", "docs-curator")
KG_RELATIVE_PATH = Path("config/agent_kg.json")
RISK_TIERS_WITH_REQUIRED_ARTIFACTS = {"medium", "high"}


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


def _latest_required_handoffs(
    kg_data: Dict[str, object], workflow_id: str
) -> Tuple[Dict[str, Dict[str, object]], List[str]]:
    """Возвращает последние handoff по обязательным агентам и список отсутствующих."""
    latest: Dict[str, Dict[str, object]] = {}
    handoffs = kg_data.get("handoffs", [])
    if not isinstance(handoffs, list):
        return latest, list(REQUIRED_AGENTS)

    for item in handoffs:
        if not isinstance(item, dict):
            continue
        if item.get("workflow_id") != workflow_id:
            continue
        agent = item.get("agent")
        if agent not in REQUIRED_AGENTS:
            continue
        current = latest.get(agent)
        if current is None:
            latest[agent] = item
            continue
        if (item.get("created_at") or "") > (current.get("created_at") or ""):
            latest[agent] = item

    missing = [agent for agent in REQUIRED_AGENTS if agent not in latest]
    return latest, missing


def _invalid_trace_fields(handoffs_by_agent: Dict[str, Dict[str, object]]) -> Dict[str, List[str]]:
    """Проверяет обязательные поля трассировки в handoff."""
    invalid: Dict[str, List[str]] = {}
    for agent, handoff in handoffs_by_agent.items():
        missing_fields: List[str] = []
        if not str(handoff.get("trace_id") or "").strip():
            missing_fields.append("trace_id")
        if not str(handoff.get("plan_step_id") or "").strip():
            missing_fields.append("plan_step_id")
        if missing_fields:
            invalid[agent] = missing_fields
    return invalid


def _normalize_text(value: object) -> str:
    return str(value or "").strip()


def _is_true_like(value: object) -> bool:
    normalized = _normalize_text(value).lower()
    return normalized in {"yes", "true", "1", "да"}


def _is_na_like(value: object) -> bool:
    normalized = _normalize_text(value).lower()
    return normalized in {"n/a", "n/a (low-risk)", "na"}


def _policy_violations(kg_data: Dict[str, object], workflow_id: str) -> List[str]:
    """Проверяет policy-поля risk/artifacts/approval для handoff workflow."""
    violations: List[str] = []
    handoffs = kg_data.get("handoffs", [])
    if not isinstance(handoffs, list):
        return violations

    for item in handoffs:
        if not isinstance(item, dict):
            continue
        if item.get("workflow_id") != workflow_id:
            continue

        handoff_id = _normalize_text(item.get("handoff_id")) or "<unknown_handoff>"
        risk_tier = _normalize_text(item.get("risk_tier")).lower()
        if risk_tier in RISK_TIERS_WITH_REQUIRED_ARTIFACTS:
            required = {
                "plan_card": item.get("plan_card"),
                "evidence_pack": item.get("evidence_pack"),
                "compliance_checklist": item.get("compliance_checklist"),
            }
            missing = [
                key
                for key, value in required.items()
                if not _normalize_text(value) or _is_na_like(value)
            ]
            if missing:
                violations.append(
                    f"{handoff_id}: risk_tier={risk_tier}, отсутствуют обязательные артефакты: "
                    + ", ".join(missing)
                )

        graph_update = _normalize_text(item.get("graph_update"))
        if _is_true_like(graph_update):
            approval_fields = {
                "approval_gate_id": item.get("approval_gate_id"),
                "approval_status": item.get("approval_status"),
                "approval_source": item.get("approval_source"),
            }
            missing_approval = [
                key for key, value in approval_fields.items() if not _normalize_text(value)
            ]
            if missing_approval:
                violations.append(
                    f"{handoff_id}: graph_update={graph_update}, не заполнены approval-поля: "
                    + ", ".join(missing_approval)
                )

    return violations


def _workflow_handoffs(kg_data: Dict[str, object], workflow_id: str) -> List[Dict[str, object]]:
    handoffs = kg_data.get("handoffs", [])
    if not isinstance(handoffs, list):
        return []
    result: List[Dict[str, object]] = []
    for item in handoffs:
        if not isinstance(item, dict):
            continue
        if item.get("workflow_id") == workflow_id:
            result.append(item)
    return result


def _latest_orchestrator_handoff(
    handoffs: List[Dict[str, object]]
) -> Dict[str, object]:
    orchestrator = [h for h in handoffs if str(h.get("agent")) == "orchestrator"]
    if not orchestrator:
        return {}
    orchestrator.sort(key=lambda h: str(h.get("created_at") or ""))
    return orchestrator[-1]


def _has_explicit_graph_decision(handoff: Dict[str, object]) -> bool:
    value = _normalize_text(handoff.get("graph_update")).lower()
    return value in {"yes", "true", "1", "да", "no", "false", "0", "нет"}


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

    handoffs_by_agent, missing = _latest_required_handoffs(kg_data, workflow_id)
    if missing:
        missing_text = ", ".join(missing)
        _deny(
            "Закрытие workflow заблокировано: отсутствуют обязательные handoff "
            f"для {workflow_id}: {missing_text}. "
            "Сначала получите и запишите handoff, затем повторите --close-workflow."
        )
        return

    invalid = _invalid_trace_fields(handoffs_by_agent)
    if invalid:
        details = "; ".join(
            f"{agent}: {', '.join(fields)}" for agent, fields in invalid.items()
        )
        _deny(
            "Закрытие workflow заблокировано: в обязательных handoff не заполнены "
            f"поля трассировки для {workflow_id}: {details}. "
            "Сначала переоформите handoff с trace_id и plan_step_id."
        )
        return

    policy_issues = _policy_violations(kg_data, workflow_id)
    if policy_issues:
        details = "; ".join(policy_issues[:3])
        if len(policy_issues) > 3:
            details = f"{details}; ... +{len(policy_issues) - 3} ещё"
        _deny(
            "Закрытие workflow заблокировано: обнаружены policy-нарушения в handoff "
            f"для {workflow_id}: {details}. "
            "Исправьте handoff (risk/artifacts/approval) и повторите --close-workflow."
        )
        return

    wf_handoffs = _workflow_handoffs(kg_data, workflow_id)
    orch = _latest_orchestrator_handoff(wf_handoffs)
    if not orch:
        _deny(
            "Закрытие workflow заблокировано: отсутствует handoff orchestrator. "
            "Требуется явное решение по GraphImpactProposal (graph_update=yes|no)."
        )
        return
    if not _has_explicit_graph_decision(orch):
        _deny(
            "Закрытие workflow заблокировано: в handoff orchestrator не зафиксировано "
            "явное решение GraphImpactProposal (graph_update=yes|no). "
            "Запишите handoff orchestrator и повторите --close-workflow."
        )
        return
    if not _normalize_text(orch.get("drift_check")):
        _deny(
            "Закрытие workflow заблокировано: в handoff orchestrator отсутствует DriftCheck. "
            "Нужно кратко зафиксировать обоснование по scope/GraphImpactProposal и повторить --close-workflow."
        )
        return

    _allow()


if __name__ == "__main__":
    main()
