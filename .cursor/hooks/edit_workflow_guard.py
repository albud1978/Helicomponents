#!/usr/bin/env python3
"""preToolUse hook: гарантирует наличие active workflow перед мутацией.

Срабатывает на Write/StrReplace/EditNotebook (matcher в hooks.json).

Политика:
- Файл в CRITICAL_PREFIXES (`code/`, `config/`, `deploy/`, `tools/`) и нет
  active workflow в Agent KG (updated_at < ACTIVE_WINDOW_SEC) и нет env
  `AGENT_KG_WORKFLOW_ID` → DENY с hint-сообщением.
- Файл в HOUSEKEEPING_PREFIXES (`docs/`, `.cursor/rules/`, README.md) и нет
  active workflow → автосоздание `W_housekeeping_<UTC>` (profile=low), ALLOW.
- Иные пути (.cursor/hooks/**, .cursor/agents/**, output/**, локальные временные) → ALLOW без действий.

Bypass: env `KG_GUARD_BYPASS=1` → ALLOW с обязательной строкой WARN в audit-log.

Контракт ответа (Cursor preToolUse): JSON c полем `decision` ∈ {allow, deny}
+ опционально `reason`. Стиль совпадает с orchestrator_write_guard.py.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict


REPO_ROOT = Path(__file__).resolve().parents[2]
KG_PATH = REPO_ROOT / "config" / "agent_kg.json"
AGENT_KG_CLI = REPO_ROOT / "code" / "utils" / "agent_kg.py"
AUDIT_LOG_PATH = Path(__file__).resolve().parent / "code_edit_audit.log"

CRITICAL_PREFIXES = ("code/", "config/", "deploy/", "tools/")
HOUSEKEEPING_PREFIXES = ("docs/", ".cursor/rules/")
HOUSEKEEPING_EXACT = {"README.md"}
ACTIVE_WINDOW_SEC = 60 * 60  # active workflow считается валидным < 1 часа


def _allow() -> None:
    sys.stdout.write(json.dumps({"decision": "allow"}))


def _deny(reason: str) -> None:
    sys.stdout.write(json.dumps({"decision": "deny", "reason": reason}))


def _extract_tool_name(payload: Dict[str, Any]) -> str:
    for key in ("tool_name", "toolName", "name"):
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _extract_tool_input(payload: Dict[str, Any]) -> Dict[str, Any]:
    for key in ("tool_input", "toolInput", "arguments", "params", "input"):
        tool_input = payload.get(key)
        if isinstance(tool_input, dict):
            return tool_input
    return {}


def _extract_target_path(tool_name: str, payload: Dict[str, Any]) -> str:
    tool_input = _extract_tool_input(payload)
    tname = tool_name.lower()
    if tname == "editnotebook":
        candidates = ("target_notebook", "notebook_path", "path")
    else:
        candidates = ("file_path", "path", "target_file")
    for key in candidates:
        value = tool_input.get(key)
        if isinstance(value, str) and value:
            return value
        value = payload.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def _normalize_path(raw_path: str) -> str:
    if not raw_path:
        return ""
    normalized = raw_path.replace("\\", "/").strip()
    path_obj = Path(normalized)
    if path_obj.is_absolute():
        try:
            return path_obj.resolve().relative_to(REPO_ROOT).as_posix()
        except (ValueError, OSError):
            return path_obj.as_posix()
    return normalized


def _has_active_workflow() -> str | None:
    """Возвращает workflow_id первого подходящего active WF (updated_at < 1ч) или None."""
    try:
        with KG_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return None
    now = datetime.now(timezone.utc)
    for wf in data.get("workflows", []):
        if wf.get("status") != "active":
            continue
        ts = wf.get("updated_at") or wf.get("created_at") or ""
        try:
            updated = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if (now - updated).total_seconds() < ACTIVE_WINDOW_SEC:
            return wf.get("workflow_id")
    return None


def _bootstrap_housekeeping(file_path: str) -> str | None:
    if not AGENT_KG_CLI.is_file():
        return None
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    wf_id = f"W_housekeeping_{ts}"
    try:
        subprocess.run(
            [
                "python3", str(AGENT_KG_CLI),
                "--init-workflow",
                "--workflow-id", wf_id,
                "--user-goal", f"housekeeping auto-bootstrap (edit_workflow_guard on {file_path})",
                "--goal", f"low-risk housekeeping edit of {file_path}",
                "--owner", "orchestrator",
                "--phase", "implement",
                "--profile", "low",
            ],
            cwd=str(REPO_ROOT),
            capture_output=True, text=True, timeout=10, check=True,
        )
        return wf_id
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return None


def _audit(level: str, message: str, file_path: str, payload: Dict[str, Any]) -> None:
    """Минимальная JSONL-строка в общий audit-log (тот же формат что audit_code_edit)."""
    try:
        entry = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "action": "preToolUse:edit_workflow_guard",
            "level": level,
            "message": message,
            "file_path": file_path,
            "conversation_id": (payload.get("conversation_id") or "?")[:8],
            "generation_id": (payload.get("generation_id") or "?")[:8],
            "agent": payload.get("agent") or payload.get("model") or "unknown",
        }
        with AUDIT_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
    except OSError:
        pass  # не блокируем основной flow ошибкой логирования


def _classify(file_path: str) -> str:
    """Возвращает 'critical' | 'housekeeping' | 'neutral'."""
    if not file_path:
        return "neutral"
    if any(file_path.startswith(p) for p in CRITICAL_PREFIXES):
        return "critical"
    if file_path in HOUSEKEEPING_EXACT:
        return "housekeeping"
    if any(file_path.startswith(p) for p in HOUSEKEEPING_PREFIXES):
        return "housekeeping"
    return "neutral"


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    tool_name = _extract_tool_name(payload)
    if tool_name.lower() not in ("write", "strreplace", "editnotebook"):
        _allow()
        return

    raw_path = _extract_target_path(tool_name, payload)
    file_path = _normalize_path(raw_path)
    category = _classify(file_path)

    if category == "neutral":
        _allow()
        return

    if os.environ.get("KG_GUARD_BYPASS") == "1":
        _audit(
            "WARN",
            f"WARN KG_GUARD_BYPASS=1 — edit allowed without active workflow (category={category})",
            file_path, payload,
        )
        _allow()
        return

    env_wf = os.environ.get("AGENT_KG_WORKFLOW_ID")
    active_wf = _has_active_workflow()
    if env_wf or active_wf:
        _allow()
        return

    if category == "housekeeping":
        bootstrap_wf = _bootstrap_housekeeping(file_path)
        if bootstrap_wf:
            _audit(
                "INFO",
                f"INFO auto-bootstrap {bootstrap_wf} for housekeeping edit",
                file_path, payload,
            )
        else:
            _audit(
                "WARN",
                "WARN failed to auto-bootstrap housekeeping workflow",
                file_path, payload,
            )
        _allow()
        return

    reason = (
        f"edit blocked: no active workflow in Agent KG (updated < 1h) and "
        f"AGENT_KG_WORKFLOW_ID env not set. File '{file_path}' is in "
        f"protected scope {CRITICAL_PREFIXES}. "
        f"Запусти: python3 code/utils/agent_kg.py --init-workflow "
        f"--workflow-id W_<descr>_<UTC> --user-goal <...> --owner orchestrator "
        f"--phase implement --profile low|medium-policy|high-strict. "
        f"Или экспортируй AGENT_KG_WORKFLOW_ID. Bypass (НЕ для production-правок): "
        f"export KG_GUARD_BYPASS=1."
    )
    _audit("BLOCK", f"BLOCK {file_path}: {reason}", file_path, payload)
    _deny(reason)


if __name__ == "__main__":
    main()
