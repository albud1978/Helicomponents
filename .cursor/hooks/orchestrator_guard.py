#!/usr/bin/env python3
"""beforeSubmitPrompt hook: инжектирует напоминание о запрете кодинга для оркестратора.

Возвращает agentMessage с reinforcement-напоминанием при каждом промпте.
Это не блокировка, а policy reinforcement — модель получает напоминание
в начале каждого цикла обработки промпта.

Если Cursor beta не поддерживает agentMessage — хук просто игнорируется (fail-open).
"""
import json
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[2]
AGENT_KG_PATH = REPO_ROOT / "config" / "agent_kg.json"
HYGIENE_STATE_PATH = Path(__file__).resolve().parent / ".hygiene_last_check.txt"
APPROVAL_CONTEXT_TYPES = {"approval_request", "approval_gate", "pending_approval"}
APPROVAL_KEYWORDS_RE = re.compile(
    r"\b(approve|approval|approved|gate)\b|"
    r"согласовать|согласован|согласуй|подтверди(?:те|шь|)|подтверждаю",
    re.IGNORECASE,
)

AGENT_MESSAGE = (
    "НАПОМИНАНИЕ GOVERNANCE: "
    "Оркестратор не пишет исходники и скрипты напрямую. "
    "Разрешенный allowlist: .cursor/agents/**, .cursor/hooks/**, .cursor/rules/**, docs/**, README.md и plan-артефакты. "
    "Для любой реализации вне allowlist используй Task tool и профильного subagent. "
    "Shell у оркестратора только readonly, кроме operational-команд python code/utils/agent_kg.py ... "
    "Agent KG ведется write-through: dispatch -> phase_start -> --write-handoff -> --close-workflow. "
    "Перед dispatch выполняй pre_gate как проверку workflow/handoff discipline; governance-compliance вызывай отдельно для medium/high-risk или policy-sensitive задач. "
    "Перед запросом high-risk approval сначала запиши approval_request context в Agent KG и укажи W_<workflow_id> в сообщении человеку. "
    "Перед закрытием workflow пройди pre_close и проверь, что обязательные по риску handoff содержат trace_id и plan_step_id. "
    "Перед make sync-domain-graph всегда запроси ApprovalGate у человека (с W_<workflow_id>) "
    "и получи governance-compliance verdict. "
    "Все handoff subagents возвращаются оркестратору."
)


def _item_ts(item: Dict[str, Any]) -> str:
    return str(item.get("updated_at") or item.get("created_at") or "")


def _load_agent_kg() -> Tuple[Dict[str, Any], str]:
    try:
        with open(AGENT_KG_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        return {}, f"unavailable:{type(exc).__name__}"
    if not isinstance(data, dict):
        return {}, "invalid_root"
    return data, "ok"


def _active_workflows(data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
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


def _workflow_ids_from_payload(payload: Dict[str, Any]) -> Iterable[str]:
    text = json.dumps(payload, ensure_ascii=False)
    return re.findall(r"\bW_[A-Za-z0-9_:-]+\b", text)


def _select_workflow(
    payload: Dict[str, Any], active: Dict[str, Dict[str, Any]]
) -> Tuple[Optional[Dict[str, Any]], str]:
    prompt_matches = [workflow_id for workflow_id in _workflow_ids_from_payload(payload) if workflow_id in active]
    unique_matches = sorted(set(prompt_matches))
    if len(unique_matches) == 1:
        return active[unique_matches[0]], "prompt"
    if len(active) == 1:
        return next(iter(active.values())), "single_active"
    return None, f"ambiguous_active:{len(active)}"


def _latest_risk_tier(data: Dict[str, Any], workflow_id: str) -> str:
    handoffs = data.get("handoffs", [])
    if not isinstance(handoffs, list):
        return "N/A"

    latest: Optional[Dict[str, Any]] = None
    for item in handoffs:
        if not isinstance(item, dict) or str(item.get("workflow_id") or "") != workflow_id:
            continue
        if not str(item.get("risk_tier") or ""):
            continue
        if latest is None or _item_ts(item) > _item_ts(latest):
            latest = item
    if latest is None:
        return "N/A"
    return str(latest.get("risk_tier") or "N/A")


def _pending_approval_count(data: Dict[str, Any], active_ids: Iterable[str]) -> str:
    active_set = set(active_ids)
    pending: set[str] = set()

    contexts = data.get("contexts", [])
    if isinstance(contexts, list):
        for item in contexts:
            if not isinstance(item, dict):
                continue
            workflow_id = str(item.get("workflow_id") or "")
            if workflow_id in active_set and str(item.get("context_type") or "") in APPROVAL_CONTEXT_TYPES:
                pending.add(workflow_id)

    handoffs = data.get("handoffs", [])
    if isinstance(handoffs, list):
        for item in handoffs:
            if not isinstance(item, dict):
                continue
            workflow_id = str(item.get("workflow_id") or "")
            if workflow_id not in active_set:
                continue
            if str(item.get("human_gate_required") or "").lower() not in {"yes", "true", "1", "да"}:
                continue
            if str(item.get("approval_status") or "").lower() in {"", "pending"}:
                pending.add(workflow_id)

    return str(len(pending))


def _approval_warning(data: Dict[str, Any], payload: Dict[str, Any]) -> str:
    prompt_text = json.dumps(payload, ensure_ascii=False)
    if not APPROVAL_KEYWORDS_RE.search(prompt_text):
        return ""

    contexts = data.get("contexts", [])
    if not isinstance(contexts, list):
        contexts = []

    missing = []
    for workflow_id in sorted(_active_workflows(data)):
        risk_tier = _latest_risk_tier(data, workflow_id).lower()
        if risk_tier not in {"medium", "high"}:
            continue
        has_approval_request = any(
            isinstance(item, dict)
            and str(item.get("workflow_id") or "") == workflow_id
            and str(item.get("context_type") or "") in APPROVAL_CONTEXT_TYPES
            for item in contexts
        )
        if not has_approval_request:
            missing.append(workflow_id)

    if not missing:
        return ""
    return (
        "WARNING approval_request: medium/high-risk workflow(s) без "
        f"approval_request context: {', '.join(missing)}. Зарегистрируй: "
        "python3 code/utils/agent_kg.py --register-approval-request "
        "--workflow-id W_<id> --content \"...\" перед отправкой "
        "approval-запроса человеку."
    )


def _reviewer_flame_warning(data: Dict[str, Any]) -> str:
    """WARNING for coder-flame medium/high handoffs without later reviewer-flame."""
    handoffs = data.get("handoffs", [])
    if not isinstance(handoffs, list):
        return ""

    active = _active_workflows(data)
    missing: list[str] = []

    for workflow_id in sorted(active):
        workflow_handoffs = [
            item
            for item in handoffs
            if isinstance(item, dict) and str(item.get("workflow_id") or "") == workflow_id
        ]
        workflow_handoffs.sort(key=_item_ts)

        last_coder_flame = None
        for item in workflow_handoffs:
            if str(item.get("agent") or "") == "coder-flame":
                last_coder_flame = item

        if last_coder_flame is None:
            continue

        risk_tier = str(last_coder_flame.get("risk_tier") or "").lower()
        if risk_tier not in {"medium", "high"}:
            continue

        coder_flame_ts = _item_ts(last_coder_flame)
        has_review_after = any(
            str(item.get("agent") or "") == "reviewer-flame"
            and _item_ts(item) >= coder_flame_ts
            for item in workflow_handoffs
        )
        if not has_review_after:
            missing.append(workflow_id)

    if not missing:
        return ""
    return (
        "WARNING reviewer-flame: active workflow(s) с coder-flame medium/high handoff "
        f"без последующего reviewer-flame review: {', '.join(missing)}. "
        "Запусти reviewer-flame через Task tool до закрытия workflow."
    )


def _hygiene_reminder() -> str:
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    try:
        saved_date = HYGIENE_STATE_PATH.read_text(encoding="utf-8").strip()
    except OSError:
        saved_date = ""
    if saved_date == today_utc:
        return ""

    try:
        result = subprocess.run(
            ["python3", "tools/hygiene_check.py", "--summary-only"],
            cwd=str(REPO_ROOT),
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""

    summary = result.stdout.strip().splitlines()[0] if result.stdout.strip() else ""
    if result.returncode not in {0, 1} or not summary:
        return ""
    clean_summary = (
        "0 stale workflows, 0 stale capsules, 0 phantom invariants, "
        "0 incomplete handoffs, 0 dangling"
    )
    if clean_summary in summary:
        return ""

    try:
        HYGIENE_STATE_PATH.write_text(f"{today_utc}\n", encoding="utf-8")
    except OSError:
        return ""
    return (
        f"HYGIENE REMINDER (daily): {summary}. "
        "Запусти `python3 tools/hygiene_check.py` для деталей."
    )


def _agent_kg_status(payload: Dict[str, Any]) -> str:
    data, state = _load_agent_kg()
    if state != "ok":
        return (
            "AGENT_KG_STATUS: workflow_id=N/A phase=N/A risk_tier=N/A "
            f"pending_approvals=N/A source={state}."
        )

    active = _active_workflows(data)
    pending_approvals = _pending_approval_count(data, active.keys())
    selected, source = _select_workflow(payload, active)
    if selected is None:
        return (
            "AGENT_KG_STATUS: workflow_id=N/A phase=N/A risk_tier=N/A "
            f"pending_approvals={pending_approvals} source={source}."
        )

    workflow_id = str(selected.get("workflow_id") or "N/A")
    phase = str(selected.get("phase") or "N/A")
    risk_tier = _latest_risk_tier(data, workflow_id)
    return (
        f"AGENT_KG_STATUS: workflow_id={workflow_id} phase={phase} "
        f"risk_tier={risk_tier} pending_approvals={pending_approvals} source={source}."
    )


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    data, state = _load_agent_kg()
    warning = _approval_warning(data, payload) if state == "ok" else ""
    reviewer_warning = _reviewer_flame_warning(data) if state == "ok" else ""
    status = _agent_kg_status(payload)
    agent_message = AGENT_MESSAGE
    if warning:
        agent_message = f"{agent_message} {warning}"
    if reviewer_warning:
        agent_message = f"{agent_message} {reviewer_warning}"
    hygiene = _hygiene_reminder()
    if hygiene:
        agent_message = f"{agent_message} {hygiene}"
    agent_message = f"{agent_message} {status}"

    # Возвращаем continue=true + agentMessage для reinforcement
    result = {
        "continue": True,
        "agentMessage": agent_message,
    }

    sys.stdout.write(json.dumps(result))


if __name__ == "__main__":
    main()
