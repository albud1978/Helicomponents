#!/usr/bin/env python3
"""preToolUse hook: блокирует --close-workflow без operational pre-close условий.

Проверка применяется только к Shell-вызовам `code/utils/agent_kg.py --close-workflow`.
Проверка риск-ориентированная:
- low-risk: обязателен orchestrator handoff с `graph_update=yes|no` и `drift_check`
- medium-risk: дополнительно обязателен handoff `governance-compliance` + подтверждение
  `SuccessCriteria` в orchestrator handoff (машинное стоп-условие, Tier-L)
- high-risk: дополнительно обязательны handoff `governance-compliance` и `docs-curator`
  + подтверждение `SuccessCriteria`
"""

import json
import re
import shlex
import sys
from pathlib import Path
from typing import Dict, List, Tuple


MEDIUM_RISK_REQUIRED_AGENTS = ("governance-compliance",)
HIGH_RISK_REQUIRED_AGENTS = ("governance-compliance", "docs-curator")
KG_RELATIVE_PATH = Path("config/agent_kg.json")
SUCCESS_CRITERIA_EVIDENCE_RE = re.compile(
    r"success[_\s-]?criteria|validation_sql|\bINV-\d+\b|\bTEMP-\d+\b|\bGPU-\d+\b|acceptance|manual-check",
    re.IGNORECASE,
)


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
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from kg_io import load_agent_kg as _kg_load

    data, state = _kg_load()
    if state != "ok":
        raise RuntimeError(f"agent_kg unavailable: {state}")
    return data


def _latest_required_handoffs(
    kg_data: Dict[str, object], workflow_id: str, required_agents: Tuple[str, ...]
) -> Tuple[Dict[str, Dict[str, object]], List[str]]:
    """Возвращает последние handoff по обязательным агентам и список отсутствующих."""
    latest: Dict[str, Dict[str, object]] = {}
    handoffs = kg_data.get("handoffs", [])
    if not isinstance(handoffs, list):
        return latest, list(required_agents)

    for item in handoffs:
        if not isinstance(item, dict):
            continue
        if item.get("workflow_id") != workflow_id:
            continue
        agent = item.get("agent")
        if agent not in required_agents:
            continue
        current = latest.get(agent)
        if current is None:
            latest[agent] = item
            continue
        if (item.get("created_at") or "") > (current.get("created_at") or ""):
            latest[agent] = item

    missing = [agent for agent in required_agents if agent not in latest]
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


def _normalize_governance_decision(value: object) -> str:
    normalized = _normalize_text(value).lower()
    mapping = {
        "allow": "allow",
        "approve": "allow",
        "needs_human_gate": "needs_human_gate",
        "escalate": "needs_human_gate",
        "reject": "reject",
    }
    return mapping.get(normalized, "")


def _normalize_risk_tier(value: object) -> str:
    """Returns 'low'|'medium'|'high' для валидных значений; '' для missing/invalid.

    Caller обязан обрабатывать пустое значение явным deny, чтобы избежать
    silent fallback на low (риск пропуска governance/docs close-gates для
    medium/high workflows с malformed orchestrator handoff).
    """
    normalized = _normalize_text(value).lower()
    if normalized in {"low", "medium", "high"}:
        return normalized
    return ""


def _extract_checklist_value(text: str, key: str) -> str:
    match = re.search(rf"\b{re.escape(key)}\s*=\s*([A-Za-z_]+)\b", text, flags=re.IGNORECASE)
    if match:
        return match.group(1).strip().lower()
    return ""


def _required_agents_for_risk(risk_tier: str) -> Tuple[str, ...]:
    if risk_tier == "high":
        return HIGH_RISK_REQUIRED_AGENTS
    if risk_tier == "medium":
        return MEDIUM_RISK_REQUIRED_AGENTS
    return ()


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


def _derive_governance_decision(
    governance: Dict[str, object], orchestrator_handoff: Dict[str, object]
) -> str:
    explicit = _normalize_governance_decision(governance.get("decision"))
    if explicit:
        return explicit

    checklist = _normalize_text(governance.get("compliance_checklist"))
    explicit = _normalize_governance_decision(_extract_checklist_value(checklist, "decision"))
    if explicit:
        return explicit

    policy_status = _extract_checklist_value(checklist, "policy_status")
    scope_match = _extract_checklist_value(checklist, "scope_match")
    traceability_status = _extract_checklist_value(checklist, "traceability_status")
    if not traceability_status:
        traceability_status = _extract_checklist_value(checklist, "traceability")
    human_gate_status = _extract_checklist_value(checklist, "human_gate_status")

    if policy_status == "fail" or scope_match == "no" or traceability_status == "fail":
        return "reject"

    approval_status = _normalize_text(orchestrator_handoff.get("approval_status")).lower()
    gate_required = _is_true_like(governance.get("human_gate_required"))
    if human_gate_status == "missing" or (gate_required and approval_status != "approved"):
        return "needs_human_gate"

    if policy_status == "pass" and scope_match == "yes" and traceability_status == "pass":
        if human_gate_status in {"ok", "not_required", ""}:
            return "allow"

    return ""


def _handoff_has_usage(handoff: Dict[str, object]) -> bool:
    usage = handoff.get("usage")
    if not isinstance(usage, dict) or not usage:
        return False
    return usage.get("est_tokens") is not None


def _handoffs_without_usage(handoffs: List[Dict[str, object]]) -> List[str]:
    """Возвращает 'agent:handoff_id' для handoff без непустого usage."""
    missing: List[str] = []
    for handoff in handoffs:
        if not isinstance(handoff, dict):
            continue
        if _handoff_has_usage(handoff):
            continue
        agent = str(handoff.get("agent") or "unknown")
        handoff_id = str(handoff.get("handoff_id") or "unknown")
        missing.append(f"{agent}:{handoff_id}")
    return missing


def _deny(reason: str) -> None:
    sys.stdout.write(json.dumps({"decision": "deny", "reason": reason}))


def _allow(warning: str = "") -> None:
    payload: Dict[str, object] = {"decision": "allow"}
    if warning:
        payload["agentMessage"] = warning
    sys.stdout.write(json.dumps(payload, ensure_ascii=False))


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

    wf_handoffs = _workflow_handoffs(kg_data, workflow_id)
    orch = _latest_orchestrator_handoff(wf_handoffs)
    if not orch:
        _deny(
            "Закрытие workflow заблокировано: отсутствует handoff orchestrator. "
            "Требуется handoff orchestrator с risk_tier, drift_check и graph_update."
        )
        return

    risk_tier = _normalize_risk_tier(orch.get("risk_tier"))
    if not risk_tier:
        _deny(
            "Закрытие workflow заблокировано: orchestrator handoff не содержит валидный risk_tier "
            "(low|medium|high). Перепиши handoff с явным --risk-tier и повтори --close-workflow."
        )
        return
    required_agents = _required_agents_for_risk(risk_tier)

    handoffs_by_agent, missing = _latest_required_handoffs(kg_data, workflow_id, required_agents)
    if missing:
        missing_text = ", ".join(missing)
        _deny(
            "Закрытие workflow заблокировано: отсутствуют обязательные handoff "
            f"для {workflow_id} (risk_tier={risk_tier}): {missing_text}. "
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

    if "governance-compliance" in handoffs_by_agent:
        governance = handoffs_by_agent.get("governance-compliance", {})
        decision = _derive_governance_decision(governance, orch)
        if not decision:
            _deny(
                "Закрытие workflow заблокировано: не удалось вывести governance verdict "
                "из handoff governance-compliance. Запишите `decision=...` в "
                "`ComplianceChecklist` и повторите --close-workflow."
            )
            return
        if decision == "reject":
            _deny(
                "Закрытие workflow заблокировано: governance-compliance вернул `reject`. "
                "Исправьте замечания и обновите handoff перед `--close-workflow`."
            )
            return
        if decision == "needs_human_gate":
            _deny(
                "Закрытие workflow заблокировано: governance-compliance требует human gate "
                "(`needs_human_gate`). Получите подтверждение человека и обновите verdict."
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

    if risk_tier in ("high", "medium"):
        facts_blob = _normalize_text(orch.get("facts"))
        success_blob = _normalize_text(orch.get("success_criteria"))
        combined_blob = f"{facts_blob}\n{success_blob}"
        if not SUCCESS_CRITERIA_EVIDENCE_RE.search(combined_blob):
            _deny(
                f"Закрытие workflow заблокировано: для {risk_tier}-risk workflow в handoff orchestrator "
                "не зафиксировано подтверждение `SuccessCriteria` (машинное стоп-условие, Tier-L). "
                "Нужно ссылаться на validation_sql, инвариант (INV-N/TEMP-N/GPU-N), acceptance-проверку "
                "или `manual-check: ...` в Facts/SuccessCriteria."
            )
            return

    missing_usage = _handoffs_without_usage(wf_handoffs)
    if missing_usage:
        warning = (
            f"WARNING token-coverage: у workflow {workflow_id} "
            f"{len(missing_usage)}/{len(wf_handoffs)} handoff'ов без непустого usage "
            f"({', '.join(missing_usage)}). Закрытие разрешено, но orchestrator должен "
            "заполнять usage (model+est_tokens+source, char_estimate допустим) для каждого "
            "--write-handoff — см. .cursor/rules/91_handoff_template.mdc."
        )
        _allow(warning)
        return

    _allow()


if __name__ == "__main__":
    main()
