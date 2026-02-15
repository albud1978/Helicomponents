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
    "Оркестратор НЕ пишет код в code/** и tools/**. "
    "Для изменений в code/ и tools/ — используй Task tool "
    "для делегирования coder-flame или coder-general. "
    "Перед каждым Write/StrReplace/Shell проверяй путь. "
    "Agent KG ведется write-through: dispatch -> phase_start -> --write-handoff -> --close-workflow. "
    "Перед закрытием workflow проверь, что handoff governance/docs содержат trace_id и plan_step_id. "
    "Перед make sync-domain-graph всегда запроси ApprovalGate у человека (с W_<workflow_id>) "
    "и получи governance-compliance verdict. "
    "High-risk закрывается только после governance-compliance и docs-curator."
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
