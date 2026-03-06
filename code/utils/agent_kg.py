#!/usr/bin/env python3
"""
Agent KG — JSON-шина координации мультиагентного workflow.

SSoT: config/agent_kg.json
Типы записей: WorkflowState, HandoffNode, ContextNode

Использование:
    python code/utils/agent_kg.py --init-workflow --workflow-id W1 --goal "цель"
    python code/utils/agent_kg.py --write-handoff --workflow-id W1 --agent coder-flame --user-goal "цель" --changes "что сделано" --facts "что проверено" --trace-id "wf:123" --plan-step-id "P1" --risk-tier low --risk-reasons "..." --plan-card "N/A (low-risk)" --evidence-pack "N/A (low-risk)" --compliance-checklist "N/A (low-risk)"
    python code/utils/agent_kg.py --read-state --workflow-id W1
    python code/utils/agent_kg.py --write-context --workflow-id W1 --context-type research --content "контент"
    python code/utils/agent_kg.py --read-context --workflow-id W1
    python code/utils/agent_kg.py --close-workflow --workflow-id W1 --close-reason "задача завершена" --agent orchestrator
"""

import argparse
import json
import os
import sys
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


DEFAULT_KG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "config", "agent_kg.json"
)


def _resolve_path(path: Optional[str]) -> str:
    """Определяет путь к JSON-файлу шины."""
    if path:
        return os.path.abspath(path)
    return os.path.abspath(DEFAULT_KG_PATH)


def _load(path: str) -> Dict[str, Any]:
    """Загружает JSON-шину."""
    if not os.path.exists(path):
        return {
            "metadata": {
                "version": 1,
                "updated_at": _now(),
                "description": "Agent KG — JSON-шина координации мультиагентного workflow",
            },
            "workflows": [],
            "handoffs": [],
            "contexts": [],
        }
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _save(path: str, data: Dict[str, Any]) -> None:
    """Сохраняет JSON-шину."""
    data["metadata"]["updated_at"] = _now()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _short(text: Optional[str], limit: int = 120) -> str:
    if text is None:
        return ""
    text = str(text)
    if len(text) <= limit:
        return text
    return text[:limit] + "..."


def _find_workflow(
    workflows: List[Dict[str, Any]], workflow_id: str
) -> Optional[Dict[str, Any]]:
    for w in workflows:
        if w.get("workflow_id") == workflow_id:
            return w
    return None


# ─── Commands ───────────────────────────────────────────────────────


def init_workflow(args: argparse.Namespace) -> None:
    """Инициализирует новый workflow."""
    if not args.workflow_id:
        raise ValueError("--workflow-id обязателен")
    if not args.goal:
        raise ValueError("--goal обязателен")

    path = _resolve_path(args.kg_path)
    data = _load(path)

    existing = _find_workflow(data["workflows"], args.workflow_id)
    if existing:
        existing["goal"] = args.goal
        existing["phase"] = args.phase or existing.get("phase", "analysis")
        existing["owner"] = args.owner or existing.get("owner", "orchestrator")
        existing["status"] = "active"
        existing["updated_at"] = _now()
        print(f"Workflow updated: {args.workflow_id}")
    else:
        data["workflows"].append(
            {
                "workflow_id": args.workflow_id,
                "goal": args.goal,
                "phase": args.phase or "analysis",
                "owner": args.owner or "orchestrator",
                "status": "active",
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
        print(f"Workflow initialized: {args.workflow_id}")

    _save(path, data)


def write_handoff(args: argparse.Namespace) -> None:
    """Записывает handoff от агента."""
    if not args.workflow_id:
        raise ValueError("--workflow-id обязателен")
    if not args.agent:
        raise ValueError("--agent обязателен")
    user_goal = args.user_goal or args.goal
    if not user_goal:
        raise ValueError("--user-goal или --goal обязателен")
    if args.approval_status:
        allowed = {"pending", "approved", "rejected"}
        if args.approval_status not in allowed:
            raise ValueError("--approval-status должен быть pending|approved|rejected")

    path = _resolve_path(args.kg_path)
    data = _load(path)

    workflow = _find_workflow(data["workflows"], args.workflow_id)
    if not workflow:
        print(f"Workflow не найден: {args.workflow_id}", file=sys.stderr)
        raise SystemExit(1)

    handoff_id = f"handoff_{args.workflow_id}_{args.agent}_{uuid.uuid4().hex[:8]}"
    evidence_arg = args.evidence
    evidence = evidence_arg if evidence_arg is not None else "не запускалось"
    facts = args.facts or ""
    if not facts and evidence_arg:
        facts = f"legacy evidence: {evidence_arg}"
    risk_tier_arg = args.risk_tier.strip() if args.risk_tier else ""
    risk_tier_provided = bool(risk_tier_arg)
    if risk_tier_provided:
        risk_tier = risk_tier_arg.lower()
        allowed_risk_tiers = {"low", "medium", "high"}
        if risk_tier not in allowed_risk_tiers:
            raise ValueError("--risk-tier должен быть low|medium|high")
    else:
        risk_tier = "low"
    risk_reasons = (args.risk_reasons or "").strip()
    if not risk_tier_provided:
        if not risk_reasons:
            risk_reasons = "legacy default (risk tier not provided)"
    else:
        if not risk_reasons:
            raise ValueError("--risk-reasons обязателен при переданном --risk-tier")
    risk_owner = (args.risk_owner or "").strip() or "orchestrator"
    risk_validated_by = (args.risk_validated_by or "").strip()
    if not risk_validated_by:
        risk_validated_by = "N/A" if risk_tier == "low" else "pending"
    human_gate_required = (args.human_gate_required or "").strip()
    if not human_gate_required:
        if risk_tier == "low":
            human_gate_required = "no"
        elif risk_tier == "high":
            human_gate_required = "yes"
        else:
            human_gate_required = "conditional"
    plan_card = (args.plan_card or "").strip()
    evidence_pack = (args.evidence_pack or "").strip()
    compliance_checklist = (args.compliance_checklist or "").strip()
    if risk_tier in {"medium", "high"}:
        missing = []
        if not plan_card:
            missing.append("--plan-card")
        if not evidence_pack:
            missing.append("--evidence-pack")
        if not compliance_checklist:
            missing.append("--compliance-checklist")
        if missing:
            missing_str = ", ".join(missing)
            raise ValueError(
                f"Для risk-tier {risk_tier} обязательны: {missing_str}"
            )
    else:
        if not plan_card:
            plan_card = "N/A (low-risk)"
        if not evidence_pack:
            evidence_pack = "N/A (low-risk)"
        if not compliance_checklist:
            compliance_checklist = "N/A (low-risk)"
    handoff = {
        "handoff_id": handoff_id,
        "workflow_id": args.workflow_id,
        "agent": args.agent,
        "user_goal": user_goal,
        "goal": user_goal,
        "changes": args.changes or "",
        "facts": facts,
        "assumptions": args.assumptions or "",
        "evidence": evidence,
        "drift_check": args.drift_check or "",
        "process_insights": args.process_insights or "",
        "trace_id": args.trace_id or "",
        "plan_step_id": args.plan_step_id or "",
        "approval_gate_id": args.approval_gate_id or "",
        "approval_status": args.approval_status or "",
        "approval_source": args.approval_source or "",
        "risk_tier": risk_tier,
        "risk_reasons": risk_reasons,
        "risk_owner": risk_owner,
        "risk_validated_by": risk_validated_by,
        "human_gate_required": human_gate_required,
        "plan_card": plan_card,
        "evidence_pack": evidence_pack,
        "compliance_checklist": compliance_checklist,
        "risks": args.risks or "нет",
        "next_owner": args.next_owner or "orchestrator",
        "open_questions": args.open_questions or "",
        "graph_update": args.graph_update or "нет",
        "created_at": _now(),
    }
    data["handoffs"].append(handoff)

    # Обновляем workflow
    workflow["phase"] = args.phase or workflow.get("phase") or "implementation"
    workflow["owner"] = args.next_owner or "orchestrator"
    workflow["updated_at"] = _now()

    _save(path, data)
    print(f"Handoff written: {handoff_id}")


def read_state(args: argparse.Namespace) -> None:
    """Читает текущее состояние workflow и последние handoff'ы."""
    if not args.workflow_id:
        raise ValueError("--workflow-id обязателен")

    path = _resolve_path(args.kg_path)
    data = _load(path)

    workflow = _find_workflow(data["workflows"], args.workflow_id)
    if not workflow:
        print(f"Workflow не найден: {args.workflow_id}")
        return

    print(f"=== Workflow State: {workflow['workflow_id']} ===")
    print(f"Goal: {workflow['goal']}")
    print(f"Phase: {workflow['phase']}")
    print(f"Owner: {workflow['owner']}")
    print(f"Status: {workflow['status']}")
    print(f"Created: {workflow.get('created_at', '?')}")
    print(f"Updated: {workflow.get('updated_at', '?')}")
    print()

    # Последние 5 handoff'ов для этого workflow
    wf_handoffs = [
        h for h in data["handoffs"] if h.get("workflow_id") == args.workflow_id
    ]
    wf_handoffs.sort(key=lambda h: h.get("created_at", ""), reverse=True)
    recent = wf_handoffs[:5]

    if recent:
        print(f"=== Recent Handoffs ({len(recent)}) ===")
        for h in recent:
            print(f"\n--- {h['handoff_id']} ---")
            print(f"Agent: {h['agent']}")
            print(f"UserGoal: {h.get('user_goal') or h.get('goal', '')}")
            if h.get("trace_id"):
                print(f"TraceID: {h.get('trace_id')}")
            if h.get("plan_step_id"):
                print(f"PlanStepID: {h.get('plan_step_id')}")
            if h.get("risk_tier"):
                print(f"RiskTier: {h.get('risk_tier')}")
            if h.get("risk_reasons"):
                print(f"RiskReasons: {_short(h.get('risk_reasons'))}")
            if h.get("risk_owner"):
                print(f"RiskOwner: {h.get('risk_owner')}")
            if h.get("risk_validated_by"):
                print(f"RiskValidatedBy: {h.get('risk_validated_by')}")
            if h.get("human_gate_required"):
                print(f"HumanGateRequired: {h.get('human_gate_required')}")
            print(f"Changes: {h['changes']}")
            if h.get("facts"):
                print(f"Facts: {h.get('facts')}")
            if h.get("assumptions"):
                print(f"Assumptions: {h.get('assumptions')}")
            print(f"Evidence: {h['evidence']}")
            if h.get("plan_card"):
                print(f"PlanCard: {_short(h.get('plan_card'))}")
            if h.get("evidence_pack"):
                print(f"EvidencePack: {_short(h.get('evidence_pack'))}")
            if h.get("compliance_checklist"):
                print(f"ComplianceChecklist: {_short(h.get('compliance_checklist'))}")
            if h.get("drift_check"):
                print(f"DriftCheck: {h.get('drift_check')}")
            if h.get("process_insights"):
                print(f"ProcessInsights: {h.get('process_insights')}")
            approval_bits = []
            if h.get("approval_gate_id"):
                approval_bits.append(f"id={h.get('approval_gate_id')}")
            if h.get("approval_status"):
                approval_bits.append(f"status={h.get('approval_status')}")
            if h.get("approval_source"):
                approval_bits.append(f"source={h.get('approval_source')}")
            if approval_bits:
                print(f"ApprovalGate: {', '.join(approval_bits)}")
            print(f"Risks: {h['risks']}")
            print(f"Next Owner: {h['next_owner']}")
            print(f"Created: {h.get('created_at', '?')}")
    else:
        print("No handoffs yet.")


def write_context(args: argparse.Namespace) -> None:
    """Записывает контекст для обмена между чатами/агентами."""
    if not args.workflow_id:
        raise ValueError("--workflow-id обязателен")
    if not args.context_type:
        raise ValueError("--context-type обязателен")
    if not args.content:
        raise ValueError("--content обязателен")

    path = _resolve_path(args.kg_path)
    data = _load(path)

    workflow = _find_workflow(data["workflows"], args.workflow_id)
    if not workflow:
        print(f"Workflow не найден: {args.workflow_id}", file=sys.stderr)
        raise SystemExit(1)

    context_id = (
        f"ctx_{args.workflow_id}_{args.context_type}_{uuid.uuid4().hex[:8]}"
    )

    # MERGE-семантика: обновляем существующий контекст того же типа или создаём новый
    existing = None
    for ctx in data["contexts"]:
        if (
            ctx.get("workflow_id") == args.workflow_id
            and ctx.get("context_type") == args.context_type
        ):
            existing = ctx
            break

    if existing:
        existing["content"] = args.content
        existing["agent"] = args.agent or "unknown"
        existing["updated_at"] = _now()
        print(f"Context updated: {existing['context_id']}")
    else:
        data["contexts"].append(
            {
                "context_id": context_id,
                "workflow_id": args.workflow_id,
                "context_type": args.context_type,
                "content": args.content,
                "agent": args.agent or "unknown",
                "created_at": _now(),
                "updated_at": _now(),
            }
        )
        print(f"Context written: {context_id}")

    _save(path, data)


def read_context(args: argparse.Namespace) -> None:
    """Читает контексты для workflow."""
    if not args.workflow_id:
        raise ValueError("--workflow-id обязателен")

    path = _resolve_path(args.kg_path)
    data = _load(path)

    wf_contexts = [
        c for c in data["contexts"] if c.get("workflow_id") == args.workflow_id
    ]
    wf_contexts.sort(key=lambda c: c.get("updated_at", ""), reverse=True)

    if not wf_contexts:
        print(f"No context found for workflow: {args.workflow_id}")
        return

    print(f"=== Context Nodes for {args.workflow_id} ({len(wf_contexts)}) ===")
    for ctx in wf_contexts:
        print(f"\n--- {ctx['context_type']} ---")
        print(f"ID: {ctx['context_id']}")
        print(f"Agent: {ctx.get('agent', '?')}")
        print(f"Updated: {ctx.get('updated_at', '?')}")
        content = ctx.get("content", "")
        if len(content) > 500:
            print(f"Content:\n{content[:500]}...")
        else:
            print(f"Content:\n{content}")


def close_workflow(args: argparse.Namespace) -> None:
    """Закрывает workflow и опционально записывает причину."""
    if not args.workflow_id:
        raise ValueError("--workflow-id обязателен")

    path = _resolve_path(args.kg_path)
    data = _load(path)

    workflow = _find_workflow(data["workflows"], args.workflow_id)
    if not workflow:
        print(f"Workflow не найден: {args.workflow_id}", file=sys.stderr)
        raise SystemExit(1)

    workflow["status"] = "closed"
    if args.phase:
        workflow["phase"] = args.phase
    if args.owner:
        workflow["owner"] = args.owner
    workflow["updated_at"] = _now()

    if args.close_reason:
        context_id = f"ctx_{args.workflow_id}_closure_{uuid.uuid4().hex[:8]}"
        data["contexts"].append(
            {
                "context_id": context_id,
                "workflow_id": args.workflow_id,
                "context_type": "closure",
                "content": args.close_reason,
                "agent": args.agent or "orchestrator",
                "created_at": _now(),
                "updated_at": _now(),
            }
        )

    _save(path, data)
    print(f"Workflow closed: {args.workflow_id}")


# ─── CLI ────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Agent KG — JSON-шина координации мультиагентного workflow "
            "(init-workflow, write-handoff, read-state, write-context, read-context, close-workflow)"
        )
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--init-workflow", action="store_true", help="Инициализировать workflow"
    )
    mode.add_argument(
        "--write-handoff", action="store_true", help="Записать handoff от агента"
    )
    mode.add_argument(
        "--read-state", action="store_true", help="Прочитать состояние workflow"
    )
    mode.add_argument(
        "--write-context", action="store_true", help="Записать контекст"
    )
    mode.add_argument(
        "--read-context", action="store_true", help="Прочитать контексты"
    )
    mode.add_argument(
        "--close-workflow", action="store_true", help="Закрыть workflow"
    )

    parser.add_argument("--kg-path", type=str, help="Путь к agent_kg.json (по умолчанию config/agent_kg.json)")
    parser.add_argument("--workflow-id", type=str, help="Идентификатор workflow")
    parser.add_argument("--goal", type=str, help="Цель workflow/handoff")
    parser.add_argument("--user-goal", type=str, help="UserGoal для handoff (новый формат)")
    parser.add_argument("--phase", type=str, help="Фаза (analysis/research/implementation/review/validation)")
    parser.add_argument("--owner", type=str, help="Текущий владелец")
    parser.add_argument("--agent", type=str, help="Агент (для handoff/context)")
    parser.add_argument("--changes", type=str, help="Описание изменений (handoff)")
    parser.add_argument("--evidence", type=str, help="Доказательства (handoff)")
    parser.add_argument("--facts", type=str, help="Факты/проверки (handoff)")
    parser.add_argument("--assumptions", type=str, help="Предположения (handoff)")
    parser.add_argument("--drift-check", type=str, help="Drift check (handoff)")
    parser.add_argument("--process-insights", type=str, help="Process insights (handoff)")
    parser.add_argument("--trace-id", type=str, help="Trace ID (handoff)")
    parser.add_argument("--plan-step-id", type=str, help="Plan step ID (handoff)")
    parser.add_argument("--approval-gate-id", type=str, help="Approval gate ID (handoff)")
    parser.add_argument(
        "--approval-status",
        type=str,
        help="Approval status (pending|approved|rejected)",
    )
    parser.add_argument("--approval-source", type=str, help="Approval source (handoff)")
    parser.add_argument("--risk-tier", type=str, help="Risk tier (low|medium|high)")
    parser.add_argument("--risk-reasons", type=str, help="Причины риска (handoff)")
    parser.add_argument("--risk-owner", type=str, help="Владелец риска (handoff)")
    parser.add_argument(
        "--risk-validated-by", type=str, help="Кем валидирован риск (handoff)"
    )
    parser.add_argument(
        "--human-gate-required", type=str, help="Human gate required (handoff)"
    )
    parser.add_argument("--plan-card", type=str, help="Plan card (handoff)")
    parser.add_argument("--evidence-pack", type=str, help="Evidence pack (handoff)")
    parser.add_argument(
        "--compliance-checklist", type=str, help="Compliance checklist (handoff)"
    )
    parser.add_argument("--risks", type=str, help="Риски (handoff)")
    parser.add_argument("--next-owner", type=str, help="Следующий владелец (handoff)")
    parser.add_argument("--open-questions", type=str, help="Открытые вопросы (handoff)")
    parser.add_argument("--graph-update", type=str, help="Требуется ли обновление графа (да/нет)")
    parser.add_argument("--context-type", type=str, help="Тип контекста (research/specification/decision)")
    parser.add_argument("--content", type=str, help="Содержимое контекста")
    parser.add_argument("--close-reason", type=str, help="Причина закрытия workflow")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.init_workflow:
        init_workflow(args)
    elif args.write_handoff:
        write_handoff(args)
    elif args.read_state:
        read_state(args)
    elif args.write_context:
        write_context(args)
    elif args.read_context:
        read_context(args)
    elif args.close_workflow:
        close_workflow(args)


if __name__ == "__main__":
    main()
