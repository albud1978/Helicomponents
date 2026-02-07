#!/usr/bin/env python3
"""
Agent KG — JSON-шина координации мультиагентного workflow.

SSoT: config/agent_kg.json
Типы записей: WorkflowState, HandoffNode, ContextNode

Использование:
    python code/utils/agent_kg.py --init-workflow --workflow-id W1 --goal "цель"
    python code/utils/agent_kg.py --write-handoff --workflow-id W1 --agent coder-flame --goal "цель" --changes "что сделано"
    python code/utils/agent_kg.py --read-state --workflow-id W1
    python code/utils/agent_kg.py --write-context --workflow-id W1 --context-type research --content "контент"
    python code/utils/agent_kg.py --read-context --workflow-id W1
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
    if not args.goal:
        raise ValueError("--goal обязателен")

    path = _resolve_path(args.kg_path)
    data = _load(path)

    workflow = _find_workflow(data["workflows"], args.workflow_id)
    if not workflow:
        print(f"Workflow не найден: {args.workflow_id}", file=sys.stderr)
        raise SystemExit(1)

    handoff_id = f"handoff_{args.workflow_id}_{args.agent}_{uuid.uuid4().hex[:8]}"
    handoff = {
        "handoff_id": handoff_id,
        "workflow_id": args.workflow_id,
        "agent": args.agent,
        "goal": args.goal,
        "changes": args.changes or "",
        "evidence": args.evidence or "не запускалось",
        "risks": args.risks or "нет",
        "next_owner": args.next_owner or "orchestrator",
        "open_questions": args.open_questions or "",
        "graph_update": args.graph_update or "нет",
        "created_at": _now(),
    }
    data["handoffs"].append(handoff)

    # Обновляем workflow
    workflow["phase"] = args.phase or "implementation"
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
            print(f"Goal: {h['goal']}")
            print(f"Changes: {h['changes']}")
            print(f"Evidence: {h['evidence']}")
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


# ─── CLI ────────────────────────────────────────────────────────────


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Agent KG — JSON-шина координации мультиагентного workflow"
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

    parser.add_argument("--kg-path", type=str, help="Путь к agent_kg.json (по умолчанию config/agent_kg.json)")
    parser.add_argument("--workflow-id", type=str, help="Идентификатор workflow")
    parser.add_argument("--goal", type=str, help="Цель workflow/handoff")
    parser.add_argument("--phase", type=str, help="Фаза (analysis/research/implementation/review/validation)")
    parser.add_argument("--owner", type=str, help="Текущий владелец")
    parser.add_argument("--agent", type=str, help="Агент (для handoff/context)")
    parser.add_argument("--changes", type=str, help="Описание изменений (handoff)")
    parser.add_argument("--evidence", type=str, help="Доказательства (handoff)")
    parser.add_argument("--risks", type=str, help="Риски (handoff)")
    parser.add_argument("--next-owner", type=str, help="Следующий владелец (handoff)")
    parser.add_argument("--open-questions", type=str, help="Открытые вопросы (handoff)")
    parser.add_argument("--graph-update", type=str, help="Требуется ли обновление графа (да/нет)")
    parser.add_argument("--context-type", type=str, help="Тип контекста (research/specification/decision)")
    parser.add_argument("--content", type=str, help="Содержимое контекста")

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


if __name__ == "__main__":
    main()
