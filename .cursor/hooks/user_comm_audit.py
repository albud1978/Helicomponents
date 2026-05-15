#!/usr/bin/env python3
"""beforeSubmitPrompt hook: логирует метаданные коммуникации с пользователем.

Логирует только технические метаданные (без полного текста промпта):
- timestamp
- conversation_id / generation_id (сокращённо)
- prompt_hash (SHA256 от нормализованного текста, первые 16 символов)
- prompt_length
- workflow_id
- workflow_id_source
- approval_hint (эвристика наличия слов подтверждения)
"""

import hashlib
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


LOG_PATH = Path(__file__).resolve().parent / "user_comm_audit.log"
AGENT_KG_PATH = Path(__file__).resolve().parents[2] / "config" / "agent_kg.json"
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
CONFIRMATION_WORDS = (
    "одобряю",
    "подтверждаю",
    "разрешаю",
    "согласен",
    "approved",
    "approve",
)
APPROVAL_CONTEXT_TYPES = {"approval_request", "approval_gate", "pending_approval"}


def _normalize_text(text: str) -> str:
    return " ".join(text.split())


def _has_approval_hint(text: str) -> bool:
    lower = text.lower()
    return any(word in lower for word in APPROVAL_WORDS)


def _is_confirmation_like(text: str) -> bool:
    lower = text.lower()
    return any(word in lower for word in CONFIRMATION_WORDS)


def _load_agent_kg() -> dict:
    try:
        with open(AGENT_KG_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (OSError, json.JSONDecodeError):
        return {}


def _previous_hash() -> str | None:
    if not LOG_PATH.exists():
        return None
    try:
        lines = LOG_PATH.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        print(f"user_comm_audit: failed to read previous hash: {exc}", file=sys.stderr)
        return None
    for line in reversed(lines):
        if not line.strip():
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            return None
        current_hash = entry.get("current_hash") if isinstance(entry, dict) else None
        return str(current_hash) if current_hash else None
    return None


def _compute_hash(entry: dict, prev_hash: str | None) -> str:
    content = json.dumps(
        {
            key: value
            for key, value in entry.items()
            if key not in ("prev_hash", "current_hash")
        },
        sort_keys=True,
        ensure_ascii=False,
    )
    return hashlib.sha256(((prev_hash or "") + content).encode("utf-8")).hexdigest()


def _append_audit_entry(entry: dict) -> None:
    prev_hash = _previous_hash()
    entry["prev_hash"] = prev_hash
    entry["current_hash"] = _compute_hash(entry, prev_hash)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
    except OSError as exc:
        print(f"user_comm_audit: failed to write audit log: {exc}", file=sys.stderr)


def _item_ts(item: dict) -> str:
    return str(item.get("updated_at") or item.get("created_at") or "")


def _infer_workflow_id_for_approval() -> tuple[str, str]:
    data = _load_agent_kg()
    workflows = data.get("workflows", [])
    if not isinstance(workflows, list):
        return "N/A", "none"

    active_workflow_ids = {
        str(item.get("workflow_id"))
        for item in workflows
        if isinstance(item, dict) and str(item.get("status") or "").lower() == "active"
    }
    if not active_workflow_ids:
        return "N/A", "none"

    contexts = data.get("contexts", [])
    if isinstance(contexts, list):
        approval_contexts: dict[str, dict] = {}
        for item in contexts:
            if not isinstance(item, dict):
                continue
            workflow_id = str(item.get("workflow_id") or "")
            if workflow_id not in active_workflow_ids:
                continue
            if str(item.get("context_type") or "") not in APPROVAL_CONTEXT_TYPES:
                continue
            current = approval_contexts.get(workflow_id)
            if current is None or _item_ts(item) > _item_ts(current):
                approval_contexts[workflow_id] = item
        if len(approval_contexts) == 1:
            return next(iter(approval_contexts)), "inferred_approval_context"

    handoffs = data.get("handoffs", [])
    if not isinstance(handoffs, list):
        return "N/A", "none"

    pending_high_risk: dict[str, dict] = {}
    for item in handoffs:
        if not isinstance(item, dict):
            continue
        workflow_id = str(item.get("workflow_id") or "")
        if workflow_id not in active_workflow_ids:
            continue
        if str(item.get("agent") or "") != "orchestrator":
            continue
        if str(item.get("risk_tier") or "").lower() != "high":
            continue
        if str(item.get("human_gate_required") or "").lower() not in {"yes", "true", "1", "да"}:
            continue
        if str(item.get("approval_status") or "").lower() not in {"", "pending"}:
            continue
        current = pending_high_risk.get(workflow_id)
        if current is None or _item_ts(item) > _item_ts(current):
            pending_high_risk[workflow_id] = item

    if len(pending_high_risk) == 1:
        return next(iter(pending_high_risk)), "inferred_pending_high_risk"

    return "N/A", "none"


def _extract_workflow_id(text: str, payload: dict, approval_hint: bool) -> tuple[str, str]:
    payload_wf = payload.get("workflow_id")
    if isinstance(payload_wf, str) and payload_wf.strip():
        return payload_wf.strip(), "payload"

    canonical = re.search(r"\bW_[A-Za-z0-9_]+\b", text)
    if canonical:
        return canonical.group(0), "prompt"

    by_label = re.search(r"\bworkflow_id\s*[:=]\s*([A-Za-z0-9_:-]+)\b", text, flags=re.IGNORECASE)
    if by_label:
        return by_label.group(1), "prompt_label"

    by_trace = re.search(r"\bwf:([A-Za-z0-9_:-]+)\b", text)
    if by_trace:
        return by_trace.group(1), "trace"

    if approval_hint and _is_confirmation_like(text):
        return _infer_workflow_id_for_approval()

    return "N/A", "none"


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
    approval_hint = "yes" if _has_approval_hint(prompt) else "no"
    workflow_id, workflow_id_source = _extract_workflow_id(
        prompt, payload, approval_hint == "yes"
    )

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conv_id = (payload.get("conversation_id") or "?")[:8]
    gen_id = (payload.get("generation_id") or "?")[:8]

    entry = {
        "timestamp": ts,
        "action": "beforeSubmitPrompt",
        "conversation_id": conv_id,
        "generation_id": gen_id,
        "prompt_hash": prompt_hash,
        "prompt_length": prompt_len,
        "workflow_id": workflow_id,
        "workflow_id_source": workflow_id_source,
        "approval_hint": approval_hint,
    }
    _append_audit_entry(entry)

    sys.stdout.write(json.dumps({"continue": True}))


if __name__ == "__main__":
    main()
