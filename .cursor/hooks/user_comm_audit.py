#!/usr/bin/env python3
"""beforeSubmitPrompt hook: логирует метаданные коммуникации с пользователем.

Логирует только технические метаданные (без полного текста промпта):
- timestamp
- conversation_id / generation_id (сокращённо)
- prompt_hash (SHA256 от нормализованного текста, первые 16 символов)
- prompt_length
- workflow_id (если встречен шаблон W_...)
- approval_hint (эвристика наличия слов подтверждения)
"""

import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


LOG_PATH = Path(__file__).resolve().parent / "user_comm_audit.log"
APPROVAL_WORDS = (
    "одобряю",
    "подтверждаю",
    "разрешаю",
    "внедряй",
    "делай",
    "согласен",
    "approved",
    "approve",
)


def _normalize_text(text: str) -> str:
    return " ".join(text.split())


def _has_approval_hint(text: str) -> bool:
    lower = text.lower()
    return any(word in lower for word in APPROVAL_WORDS)


def _extract_workflow_id(text: str) -> str:
    match = re.search(r"\bW_[A-Za-z0-9_]+\b", text)
    if not match:
        return "N/A"
    return match.group(0)


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    prompt = payload.get("prompt") or ""
    norm_prompt = _normalize_text(prompt)
    prompt_hash = hashlib.sha256(norm_prompt.encode("utf-8")).hexdigest()[:16]
    prompt_len = len(prompt)
    workflow_id = _extract_workflow_id(prompt)
    approval_hint = "yes" if _has_approval_hint(prompt) else "no"

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    conv_id = (payload.get("conversation_id") or "?")[:8]
    gen_id = (payload.get("generation_id") or "?")[:8]

    line = (
        f"[{ts}] conv={conv_id} gen={gen_id} "
        f"prompt_hash={prompt_hash} prompt_len={prompt_len} "
        f"workflow_id={workflow_id} approval_hint={approval_hint}"
    )

    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except OSError:
        # fail-open: не блокируем работу при ошибке записи лога
        pass

    sys.stdout.write(json.dumps({"continue": True}))


if __name__ == "__main__":
    main()
