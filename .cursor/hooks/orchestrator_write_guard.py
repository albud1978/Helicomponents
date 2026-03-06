#!/usr/bin/env python3
"""preToolUse hook: hard-block для оркестратора.

Политика:
- Оркестратор не редактирует исходники и скрипты напрямую.
- Разрешены только `.cursor/agents/**`, `.cursor/hooks/**`, `.cursor/rules/**`, `docs/**`, `README.md` и plan-артефакты.
- Shell допускается только для readonly-команд.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List


REPO_ROOT = Path(__file__).resolve().parents[2]
ALLOWED_RELATIVE_PREFIXES = (
    ".cursor/agents/",
    ".cursor/hooks/",
    ".cursor/rules/",
    "docs/",
)
ALLOWED_EXACT_PATHS = {
    ".cursor/hooks.json",
    "README.md",
}
ALLOWED_EXTERNAL_SUBSTRINGS = (
    "/.cursor/plans/",
    "\\.cursor\\plans\\",
)
READONLY_SHELL_PATTERNS = (
    re.compile(r"^\s*git\s+status(?:\s|$)"),
    re.compile(r"^\s*git\s+diff(?:\s|$)"),
    re.compile(r"^\s*git\s+log(?:\s|$)"),
    re.compile(r"^\s*git\s+show(?:\s|$)"),
    re.compile(r"^\s*git\s+branch(?:\s+--show-current|\s+--list|\s*$)"),
    re.compile(r"^\s*git\s+rev-parse(?:\s|$)"),
    re.compile(r"^\s*git\s+ls-files(?:\s|$)"),
    re.compile(r"^\s*ls(?:\s|$)"),
    re.compile(r"^\s*pwd(?:\s|$)"),
    re.compile(r"^\s*python3?\s+-m\s+py_compile(?:\s|$)"),
    re.compile(r"^\s*python3?\s+--version(?:\s|$)"),
    re.compile(r"^\s*python3?\s+-V(?:\s|$)"),
)
SHELL_SPLIT_RE = re.compile(r"\s*(?:&&|\|\||;)\s*")


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


def _extract_shell_command(payload: Dict[str, Any]) -> str:
    tool_input = _extract_tool_input(payload)
    for key in ("command", "cmd", "text"):
        value = tool_input.get(key)
        if isinstance(value, str) and value:
            return value
    top_level = payload.get("command")
    if isinstance(top_level, str):
        return top_level
    return ""


def _extract_delete_path(payload: Dict[str, Any]) -> str:
    tool_input = _extract_tool_input(payload)
    value = tool_input.get("path")
    if isinstance(value, str):
        return value
    value = payload.get("path")
    if isinstance(value, str):
        return value
    return ""


def _extract_edit_notebook_path(payload: Dict[str, Any]) -> str:
    tool_input = _extract_tool_input(payload)
    value = tool_input.get("target_notebook")
    if isinstance(value, str):
        return value
    value = payload.get("target_notebook")
    if isinstance(value, str):
        return value
    return ""


def _extract_applypatch_text(payload: Dict[str, Any]) -> str:
    for key in ("tool_input", "toolInput", "arguments", "params", "input"):
        tool_input = payload.get(key)
        if isinstance(tool_input, str):
            return tool_input
        if isinstance(tool_input, dict):
            for nested_key in ("patch", "content", "text"):
                value = tool_input.get(nested_key)
                if isinstance(value, str):
                    return value
    return ""


def _normalize_path(raw_path: str) -> str:
    raw_path = raw_path.strip()
    if not raw_path:
        return ""

    normalized = raw_path.replace("\\", "/")
    if any(marker in normalized for marker in ALLOWED_EXTERNAL_SUBSTRINGS):
        return normalized

    path_obj = Path(raw_path)
    if path_obj.is_absolute():
        try:
            return path_obj.resolve().relative_to(REPO_ROOT).as_posix()
        except Exception:
            return path_obj.resolve().as_posix()
    return path_obj.as_posix()


def _is_allowed_path(path_str: str) -> bool:
    if not path_str:
        return False
    normalized = _normalize_path(path_str)
    if not normalized:
        return False
    if any(marker in normalized for marker in ALLOWED_EXTERNAL_SUBSTRINGS):
        return True
    if normalized in ALLOWED_EXACT_PATHS:
        return True
    return any(normalized.startswith(prefix) for prefix in ALLOWED_RELATIVE_PREFIXES)


def _extract_paths_from_patch(patch_text: str) -> List[str]:
    paths: List[str] = []
    for line in patch_text.splitlines():
        if line.startswith("*** Update File: "):
            raw = line.replace("*** Update File: ", "", 1)
            paths.append(raw.strip())
        elif line.startswith("*** Add File: "):
            raw = line.replace("*** Add File: ", "", 1)
            paths.append(raw.strip())
    return paths


def _deny_if_any_path_forbidden(paths: Iterable[str], reason: str) -> bool:
    touched = [_normalize_path(path) for path in paths if path]
    forbidden = [path for path in touched if not _is_allowed_path(path)]
    if forbidden:
        _deny(f"{reason} Запрещенные пути: {', '.join(forbidden)}.")
        return True
    return False


def _is_readonly_shell(command: str) -> bool:
    if not command.strip():
        return True
    if any(token in command for token in (">", "<<", "|", "&")):
        return False
    parts = [part.strip() for part in SHELL_SPLIT_RE.split(command) if part.strip()]
    if not parts:
        return True
    return all(any(pattern.match(part) for pattern in READONLY_SHELL_PATTERNS) for part in parts)


def main() -> None:
    raw = sys.stdin.read()
    try:
        payload = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        payload = {}

    tool_name = _extract_tool_name(payload).lower()

    if tool_name == "applypatch":
        patch_text = _extract_applypatch_text(payload)
        paths = _extract_paths_from_patch(patch_text)
        if _deny_if_any_path_forbidden(
            paths,
            "Оркестратору запрещено править исходники и скрипты напрямую.",
        ):
            return
        _allow()
        return

    if tool_name == "delete":
        target = _extract_delete_path(payload)
        if _deny_if_any_path_forbidden(
            [target],
            "Оркестратору запрещено удалять исходники и скрипты напрямую.",
        ):
            return
        _allow()
        return

    if tool_name == "editnotebook":
        target = _extract_edit_notebook_path(payload)
        if _deny_if_any_path_forbidden(
            [target],
            "Оркестратору запрещено редактировать notebook/source-артефакты напрямую.",
        ):
            return
        _allow()
        return

    if tool_name == "shell":
        command = _extract_shell_command(payload)
        if not _is_readonly_shell(command):
            _deny(
                "Оркестратору разрешен только readonly shell. "
                "Используйте Subagent для любых mutating shell-действий."
            )
            return
        _allow()
        return

    _allow()


if __name__ == "__main__":
    main()
