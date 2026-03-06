#!/usr/bin/env python3
"""beforeSubmitPrompt hook: инжектирует напоминание о запрете кодинга для оркестратора.

Возвращает agentMessage с reinforcement-напоминанием при каждом промпте.
Это не блокировка, а policy reinforcement — модель получает напоминание
в начале каждого цикла обработки промпта.

Если Cursor beta не поддерживает agentMessage — хук просто игнорируется (fail-open).
"""
import json
import sys

AGENT_MESSAGE = (
    "НАПОМИНАНИЕ GOVERNANCE: "
    "Оркестратор не пишет исходники и скрипты напрямую. "
    "Разрешенный allowlist: .cursor/agents/**, .cursor/hooks/**, .cursor/rules/**, docs/**, README.md и plan-артефакты. "
    "Для любой реализации вне allowlist используй Task tool и профильного subagent. "
    "Shell у оркестратора только readonly. "
    "Agent KG ведется write-through: dispatch -> phase_start -> --write-handoff -> --close-workflow. "
    "Перед dispatch выполняй pre_gate, перед закрытием — pre_close; при нетривиальном policy-check вызывай governance-compliance. "
    "Перед закрытием workflow проверь, что handoff governance/docs содержат trace_id и plan_step_id. "
    "Перед make sync-domain-graph всегда запроси ApprovalGate у человека (с W_<workflow_id>) "
    "и получи governance-compliance verdict. "
    "Все handoff subagents возвращаются оркестратору."
)


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    # Возвращаем continue=true + agentMessage для reinforcement
    result = {
        "continue": True,
        "agentMessage": AGENT_MESSAGE,
    }

    sys.stdout.write(json.dumps(result))


if __name__ == "__main__":
    main()
