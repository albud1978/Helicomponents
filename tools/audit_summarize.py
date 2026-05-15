#!/usr/bin/env python3
"""Generate sanitized markdown summaries from local audit logs."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from audit_verify import verify_log


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "docs" / "audit_summaries"
CODE_EDIT_LOG = REPO_ROOT / ".cursor" / "hooks" / "code_edit_audit.log"
USER_COMM_LOG = REPO_ROOT / ".cursor" / "hooks" / "user_comm_audit.log"
SECRET_RE = re.compile(
    r"(?i)(token|secret|password|api[_-]?key)\s*[:=]\s*[\w-]+"
)
TOP_N = 10


def sanitize_text(value: Any) -> str:
    """Remove local absolute paths, secret-looking values, and time precision."""
    text = str(value)
    text = text.replace(str(REPO_ROOT), "<repo>")
    text = SECRET_RE.sub(lambda match: f"{match.group(1)}=<redacted>", text)
    text = re.sub(r"\d{4}-\d{2}-\d{2}[T ][0-9:.+-]+Z?", lambda m: m.group(0)[:10], text)
    return text


def sanitize_path(raw_path: Any) -> str:
    """Return a repo-relative, sanitized path label."""
    text = sanitize_text(raw_path)
    if text.startswith("<repo>/"):
        return text.removeprefix("<repo>/")
    path = Path(text)
    if path.is_absolute():
        try:
            return path.resolve().relative_to(REPO_ROOT).as_posix()
        except (OSError, ValueError):
            return "<external-path>"
    return text.replace("\\", "/")


def parse_date(value: str | None) -> date | None:
    """Parse a date from ISO or legacy audit timestamp strings."""
    if not value:
        return None
    match = re.search(r"\d{4}-\d{2}-\d{2}", value)
    if not match:
        return None
    return date.fromisoformat(match.group(0))


def _legacy_code_event(line: str) -> dict[str, Any] | None:
    match = re.match(r"\[(?P<ts>\d{4}-\d{2}-\d{2})[^\]]*\].*file=(?P<file>\S+)", line)
    if not match:
        return None
    return {
        "timestamp": match.group("ts"),
        "action": "afterFileEdit",
        "agent": "legacy",
        "file_path": match.group("file"),
    }


def _legacy_user_event(line: str) -> dict[str, Any] | None:
    match = re.match(r"\[(?P<ts>\d{4}-\d{2}-\d{2})[^\]]*\].*", line)
    if not match or "prompt_hash=" not in line:
        return None
    approval = re.search(r"approval_hint=(?P<approval>\w+)", line)
    return {
        "timestamp": match.group("ts"),
        "action": "beforeSubmitPrompt",
        "approval_hint": approval.group("approval") if approval else "unknown",
    }


def load_events(log_path: Path, log_type: str) -> list[dict[str, Any]]:
    """Load JSONL and legacy audit events from a log file."""
    if not log_path.exists():
        return []

    events: list[dict[str, Any]] = []
    with log_path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError:
                item = (
                    _legacy_code_event(line)
                    if log_type == "code"
                    else _legacy_user_event(line)
                )
            if isinstance(item, dict):
                item["log_type"] = log_type
                events.append(item)
    return events


def filter_events(
    events: list[dict[str, Any]], from_date: date, to_date: date
) -> list[dict[str, Any]]:
    """Filter events by inclusive date range."""
    filtered: list[dict[str, Any]] = []
    for event in events:
        event_date = parse_date(str(event.get("timestamp") or ""))
        if event_date and from_date <= event_date <= to_date:
            filtered.append(event)
    return filtered


def _counter_lines(title: str, counter: Counter[str]) -> list[str]:
    lines = [f"### {title}"]
    if not counter:
        return lines + ["- none"]
    return lines + [f"- `{key}`: {count}" for key, count in counter.most_common(TOP_N)]


def hash_status(log_path: Path) -> str:
    """Return a one-line hash-chain status for the report."""
    if not log_path.exists():
        return "MISSING"
    ok, message = verify_log(log_path)
    return f"OK ({message})" if ok else f"BROKEN ({message})"


def anomaly_hints(code_events: list[dict[str, Any]], all_events: list[dict[str, Any]]) -> list[str]:
    """Build lightweight anomaly hints for compliance review."""
    hints: list[str] = []
    unusual = sorted(
        {
            sanitize_path(event.get("file_path", ""))
            for event in code_events
            if re.search(r"(^|/)\.env|credential|secret|password", str(event.get("file_path", "")), re.I)
        }
    )
    hints.extend(f"unusual path: `{path}`" for path in unusual)

    by_date = Counter(
        parse_date(str(event.get("timestamp") or "")).isoformat()
        for event in all_events
        if parse_date(str(event.get("timestamp") or "")) is not None
    )
    hints.extend(
        f"high-density burst: `{day}` has {count} events"
        for day, count in sorted(by_date.items())
        if count > 100
    )
    return hints


def render_report(from_date: date, to_date: date) -> str:
    """Render a sanitized markdown report for the requested date range."""
    code_events = filter_events(load_events(CODE_EDIT_LOG, "code"), from_date, to_date)
    user_events = filter_events(load_events(USER_COMM_LOG, "user"), from_date, to_date)
    all_events = code_events + user_events

    code_agents = Counter(sanitize_text(event.get("agent", "unknown")) for event in code_events)
    code_paths = Counter(sanitize_path(event.get("file_path", "")) for event in code_events)
    code_actions = Counter(sanitize_text(event.get("action", "unknown")) for event in code_events)
    user_actions = Counter(sanitize_text(event.get("action", "unknown")) for event in user_events)
    approval_count = sum(1 for event in user_events if event.get("approval_hint") == "yes")
    hints = anomaly_hints(code_events, all_events)

    lines = [
        "# Audit Summary",
        "",
        f"- scope: `{from_date.isoformat()}..{to_date.isoformat()}`",
        f"- generated_at: `{datetime.now(timezone.utc).date().isoformat()}`",
        "",
        "## Code Edit Summary",
        f"- total_events: {len(code_events)}",
    ]
    lines.extend(_counter_lines("Top Agents", code_agents))
    lines.extend(_counter_lines("Top File Paths", code_paths))
    lines.extend(_counter_lines("Top Action Types", code_actions))
    lines.extend(
        [
            "",
            "## User Comm Summary",
            f"- total_events: {len(user_events)}",
            f"- approval_events: {approval_count}",
        ]
    )
    lines.extend(_counter_lines("Top Actions", user_actions))
    lines.extend(["", "## Anomaly Hints"])
    lines.extend([f"- {hint}" for hint in hints] if hints else ["- none"])
    lines.extend(
        [
            "",
            "## Hash Chain Status",
            f"- code_edit_audit.log: {hash_status(CODE_EDIT_LOG)}",
            f"- user_comm_audit.log: {hash_status(USER_COMM_LOG)}",
            "",
            "⚠️ this file generated automatically, do not edit manually",
            "",
        ]
    )
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate sanitized audit summary.")
    default_to = datetime.now(timezone.utc).date()
    default_from = default_to - timedelta(days=7)
    parser.add_argument("--from", dest="from_date", type=date.fromisoformat, default=default_from)
    parser.add_argument("--to", dest="to_date", type=date.fromisoformat, default=default_to)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    if args.from_date > args.to_date:
        raise SystemExit("--from must be <= --to")
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{args.to_date.isoformat()}.md"
    output_path.write_text(render_report(args.from_date, args.to_date), encoding="utf-8")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    main()
