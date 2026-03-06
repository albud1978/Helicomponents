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
    "Shell у оркестратора только readonly, кроме operational-команд python code/utils/agent_kg.py ... "
    "Agent KG ведется write-through: dispatch -> phase_start -> --write-handoff -> --close-workflow. "
    "Перед dispatch выполняй pre_gate как проверку workflow/handoff discipline; governance-compliance вызывай отдельно для medium/high-risk или policy-sensitive задач. "
    "Перед запросом high-risk approval сначала запиши approval_request context в Agent KG и укажи W_<workflow_id> в сообщении человеку. "
    "Перед закрытием workflow пройди pre_close и проверь, что обязательные по риску handoff содержат trace_id и plan_step_id. "
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
