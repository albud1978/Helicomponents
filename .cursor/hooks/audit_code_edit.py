#!/usr/bin/env python3
"""afterFileEdit hook: логирует правки в code/ и tools/ для audit trail.

Дополнительно (W_kg_enforcement_pack, 2026-05-21): если на момент edit'а
в Agent KG нет ни одного active workflow (updated_at < BOOTSTRAP_WINDOW_SEC),
автоматически создаём implicit `W_session_<UTC>` (profile=low, owner=orchestrator).
Цель — гарантировать, что любая мутация имеет workflow-привязку даже если
оркестратор пропустил init-фазу. Отключается env `KG_AUTO_BOOTSTRAP=0`.
"""
import hashlib
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


LOG_PATH = Path(__file__).resolve().parent / "code_edit_audit.log"
WATCHED_PREFIXES = ("code/", "tools/")
KG_AUDIT_PATH = "config/agent_kg.json"
KG_AUDIT_PREFIX = "config/agent_kg_archive/"
AUDIT_LOG_PATHS = (
    ".cursor/hooks/code_edit_audit.log",
    ".cursor/hooks/user_comm_audit.log",
)
MAX_SNIPPET = 120  # максимум символов old/new в логе
REPO_ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP_WINDOW_SEC = 30 * 60  # workflow считается активным если updated_at < 30 мин
KG_PATH = REPO_ROOT / "config" / "agent_kg.json"
AGENT_KG_CLI = REPO_ROOT / "code" / "utils" / "agent_kg.py"


def _normalize_path(raw_path: str) -> str:
    if not raw_path:
        return ""

    path_obj = Path(raw_path)
    if path_obj.is_absolute():
        try:
            return path_obj.resolve().relative_to(REPO_ROOT).as_posix()
        except ValueError:
            return path_obj.as_posix()

    return raw_path.replace("\\", "/")


def _previous_hash() -> str | None:
    if not LOG_PATH.exists():
        return None
    try:
        lines = LOG_PATH.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        print(f"audit_code_edit: failed to read previous hash: {exc}", file=sys.stderr)
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


def _classify_edit(file_path: str) -> tuple[str, str] | None:
    if file_path == KG_AUDIT_PATH or file_path.startswith(KG_AUDIT_PREFIX):
        return (
            "WARN",
            f"WARN {file_path} edited via Cursor tool (Write/StrReplace) - "
            "bypass of code/utils/agent_kg.py CLI",
        )
    if file_path in AUDIT_LOG_PATHS:
        return (
            "WARN",
            f"WARN {file_path} edited via Cursor tool (Write/StrReplace) - "
            "audit log integrity event",
        )
    if any(file_path.startswith(prefix) for prefix in WATCHED_PREFIXES):
        return (
            "INFO",
            f"INFO {file_path} edited via Cursor tool (Write/StrReplace)",
        )
    return None


def _has_active_workflow() -> bool:
    """Есть ли в KG workflow со status='active' и updated_at < BOOTSTRAP_WINDOW_SEC?"""
    try:
        with KG_PATH.open("r", encoding="utf-8") as f:
            data = json.load(f)
    except (OSError, json.JSONDecodeError):
        return True  # KG недоступен -> не bootstrap'им, чтобы не плодить мусор
    now = datetime.now(timezone.utc)
    for wf in data.get("workflows", []):
        if wf.get("status") != "active":
            continue
        ts = wf.get("updated_at") or wf.get("created_at") or ""
        try:
            updated = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            continue
        if (now - updated).total_seconds() < BOOTSTRAP_WINDOW_SEC:
            return True
    return False


def _bootstrap_session_workflow(file_path: str, conv_id: str) -> str | None:
    """Создаёт implicit W_session_<UTC> через CLI agent_kg.py. Возвращает workflow_id или None."""
    if os.environ.get("KG_AUTO_BOOTSTRAP", "1") == "0":
        return None
    if not AGENT_KG_CLI.is_file():
        return None
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    wf_id = f"W_session_{ts}"
    goal = f"implicit session workflow (auto-bootstrap on edit of {file_path})"
    user_goal = f"auto-bootstrap via audit_code_edit hook (conv_id={conv_id})"
    try:
        subprocess.run(
            [
                "python3",
                str(AGENT_KG_CLI),
                "--init-workflow",
                "--workflow-id", wf_id,
                "--user-goal", user_goal,
                "--goal", goal,
                "--owner", "orchestrator",
                "--phase", "implement",
                "--profile", "low",
            ],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )
        return wf_id
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError):
        return None


def _append_audit_entry(entry: dict) -> None:
    prev_hash = _previous_hash()
    entry["prev_hash"] = prev_hash
    entry["current_hash"] = _compute_hash(entry, prev_hash)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
    except OSError as exc:
        print(f"audit_code_edit: failed to write audit log: {exc}", file=sys.stderr)


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    file_path = _normalize_path(payload.get("file_path", ""))
    classification = _classify_edit(file_path)
    if classification is None:
        sys.stdout.write("{}")
        return
    level, message = classification

    edits = payload.get("edits", [])
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conv_id = payload.get("conversation_id", "?")[:8]
    gen_id = payload.get("generation_id", "?")[:8]

    bootstrap_wf: str | None = None
    if not _has_active_workflow():
        bootstrap_wf = _bootstrap_session_workflow(file_path, conv_id)

    entry = {
        "timestamp": ts,
        "action": "afterFileEdit",
        "level": level,
        "message": message,
        "conversation_id": conv_id,
        "generation_id": gen_id,
        "agent": payload.get("agent") or payload.get("model") or "unknown",
        "file_path": file_path,
        "edit_count": len(edits) if isinstance(edits, list) else 0,
        "edits": [
            {
                "index": i,
                "old": (edit.get("old_string") or "")[:MAX_SNIPPET].replace("\n", "\\n"),
                "new": (edit.get("new_string") or "")[:MAX_SNIPPET].replace("\n", "\\n"),
            }
            for i, edit in enumerate(edits)
            if isinstance(edit, dict)
        ],
    }
    if bootstrap_wf:
        entry["auto_bootstrap_workflow"] = bootstrap_wf
    _append_audit_entry(entry)

    sys.stdout.write("{}")


if __name__ == "__main__":
    main()
