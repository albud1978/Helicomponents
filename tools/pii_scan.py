#!/usr/bin/env python3
"""Lite regex PII scan for Agent KG handoffs and contexts."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_KG_PATH = REPO_ROOT / "config" / "agent_kg.json"
DEFAULT_WHITELIST = "@helicomponents.ru,@utair.io,@cursor.com,@anthropic.com,@anysphere.inc"
LOG_PATHS = (
    REPO_ROOT / ".cursor" / "hooks" / "user_comm_audit.log",
    REPO_ROOT / ".cursor" / "hooks" / "code_edit_audit.log",
)

PII_PATTERNS = {
    "phone_ru_int": r"\+7[\s\-]?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}",
    "iin_kz": r"\b\d{12}\b",
    "inn_ru": r"\b\d{10}\b|\b\d{12}\b",
    "passport_ru": r"\b\d{4}\s?\d{6}\b",
    "credit_card": r"\b(?:\d[ -]*?){13,16}\b",
    "secret_token": r"(?i)(?:password|api[_-]?key|token|secret)\s*[:=]\s*['\"]?[a-zA-Z0-9_\-]{8,}['\"]?",
    "aws_key": r"AKIA[0-9A-Z]{16}",
    "private_key": r"-----BEGIN (?:RSA |EC |OPENSSH |PGP )?PRIVATE KEY-----",
}
LOOSE_PII_PATTERNS = {
    "phone_general": r"\b\+?[1-9]\d{1,14}\b",
}
EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+-]+@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})")


def _load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object in {path}")
    return data


def _empty_projection() -> dict[str, list[dict[str, Any]]]:
    return {"workflows": [], "handoffs": [], "contexts": []}


def _load_active(path: Path) -> dict[str, list[dict[str, Any]]]:
    raw = _load_json(path)
    data = _empty_projection()
    for key in data:
        values = raw.get(key, [])
        if isinstance(values, list):
            data[key].extend(item for item in values if isinstance(item, dict))
    return data


def _archive_paths(kg_path: Path) -> list[Path]:
    config_dir = kg_path.resolve().parent
    paths = list(config_dir.glob("agent_kg_archive*.jsonl"))
    archive_dir = config_dir / "agent_kg_archive"
    if archive_dir.exists():
        paths.extend(archive_dir.glob("**/*.jsonl"))
    return sorted(set(paths))


def _load_archive(kg_path: Path) -> dict[str, list[dict[str, Any]]]:
    data = _empty_projection()
    for jsonl_path in _archive_paths(kg_path):
        with jsonl_path.open("r", encoding="utf-8") as handle:
            for raw_line in handle:
                if not raw_line.strip():
                    continue
                record = json.loads(raw_line)
                if not isinstance(record, dict):
                    continue
                record_type = record.get("type")
                item = {key: value for key, value in record.items() if key != "type"}
                if record_type == "workflow":
                    data["workflows"].append(item)
                elif record_type == "handoff":
                    data["handoffs"].append(item)
                elif record_type == "context":
                    data["contexts"].append(item)
    return data


def _merge(base: dict[str, list[dict[str, Any]]], extra: dict[str, list[dict[str, Any]]]) -> dict[str, list[dict[str, Any]]]:
    merged = _empty_projection()
    keys = {"workflows": "workflow_id", "handoffs": "handoff_id", "contexts": "context_id"}
    for bucket, id_key in keys.items():
        seen: set[str] = set()
        for item in base[bucket] + extra[bucket]:
            item_id = str(item.get(id_key) or json.dumps(item, ensure_ascii=False, sort_keys=True))
            if item_id in seen:
                continue
            seen.add(item_id)
            merged[bucket].append(item)
    return merged


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _trunc(value: str, limit: int) -> str:
    return value if len(value) <= limit else value[:limit]


def _surrounding(text: str, start: int, end: int, radius: int = 60) -> str:
    left = max(0, start - radius)
    right = min(len(text), end + radius)
    prefix = "..." if left > 0 else ""
    suffix = "..." if right < len(text) else ""
    return f"{prefix}{text[left:right]}{suffix}"


def _whitelist_domains(raw: str) -> set[str]:
    domains = set()
    for item in raw.split(","):
        domain = item.strip().lower().lstrip("@")
        if domain:
            domains.add(domain)
    return domains


def _email_findings(text: str, whitelist: set[str]) -> Iterable[tuple[str, int, int]]:
    for match in EMAIL_RE.finditer(text):
        domain = match.group(1).lower()
        if domain in whitelist:
            continue
        yield match.group(0), match.start(), match.end()


def _pattern_findings(text: str) -> Iterable[tuple[str, str, int, int]]:
    for pattern_name, pattern in PII_PATTERNS.items():
        for match in re.finditer(pattern, text):
            yield pattern_name, match.group(0), match.start(), match.end()


def _scan_field(
    findings: list[dict[str, str]],
    *,
    pattern_whitelist: set[str],
    workflow_id: str,
    item_id: str,
    where: str,
    field: str,
    value: Any,
) -> None:
    text = _to_text(value)
    if not text:
        return
    for matched, start, end in _email_findings(text, pattern_whitelist):
        findings.append(
            {
                "pattern": "email_external",
                "where": where,
                "workflow": workflow_id or "N/A",
                "item": item_id or "N/A",
                "field": field,
                "match": _trunc(matched, 30),
                "context": _trunc(_surrounding(text, start, end), 160),
            }
        )
    for pattern_name, matched, start, end in _pattern_findings(text):
        findings.append(
            {
                "pattern": pattern_name,
                "where": where,
                "workflow": workflow_id or "N/A",
                "item": item_id or "N/A",
                "field": field,
                "match": _trunc(matched, 30),
                "context": _trunc(_surrounding(text, start, end), 160),
            }
        )


def _scan_kg(data: dict[str, list[dict[str, Any]]], whitelist: set[str], handoffs_only: bool) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    if not handoffs_only:
        for workflow in data["workflows"]:
            _scan_field(
                findings,
                pattern_whitelist=whitelist,
                workflow_id=str(workflow.get("workflow_id") or ""),
                item_id="N/A",
                where="workflow",
                field="goal",
                value=workflow.get("goal"),
            )

    handoff_fields = ("user_goal", "changes", "facts", "evidence_pack", "process_insights", "open_questions")
    for handoff in data["handoffs"]:
        for field in handoff_fields:
            _scan_field(
                findings,
                pattern_whitelist=whitelist,
                workflow_id=str(handoff.get("workflow_id") or ""),
                item_id=str(handoff.get("handoff_id") or "N/A"),
                where="handoff",
                field=field,
                value=handoff.get(field),
            )

    if not handoffs_only:
        for context in data["contexts"]:
            _scan_field(
                findings,
                pattern_whitelist=whitelist,
                workflow_id=str(context.get("workflow_id") or ""),
                item_id=str(context.get("context_id") or "N/A"),
                where="context",
                field="content",
                value=context.get("content"),
            )
    return findings


def _scan_logs(whitelist: set[str]) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for path in LOG_PATHS:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        _scan_field(
            findings,
            pattern_whitelist=whitelist,
            workflow_id="N/A",
            item_id=path.relative_to(REPO_ROOT).as_posix(),
            where="log",
            field=path.name,
            value=text,
        )
    return findings


def _print_finding(finding: dict[str, str]) -> None:
    print(f"FINDING [{finding['pattern']}] in {finding['where']}:")
    print(f"  workflow: {finding['workflow']}")
    print(f"  handoff/context: {finding['item']}")
    print(f"  field: {finding['field']}")
    print(f"  match: {finding['match']}")
    print(f"  context: {finding['context']}")


def _summary(findings: list[dict[str, str]]) -> str:
    counts = Counter(finding["pattern"] for finding in findings)
    if not counts:
        return "Total findings: 0"
    by_pattern = ", ".join(f"{name}={count}" for name, count in sorted(counts.items()))
    return f"Total findings: {len(findings)} (by pattern: {by_pattern})"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, default=DEFAULT_KG_PATH)
    parser.add_argument("--include-archive", action="store_true")
    parser.add_argument("--scan-logs", action="store_true")
    parser.add_argument("--scan-handoffs-only", action="store_true")
    parser.add_argument("--summary-only", action="store_true")
    parser.add_argument("--exit-on-finding", action="store_true")
    parser.add_argument("--include-loose-patterns", action="store_true")
    parser.add_argument("--whitelist-email", default=DEFAULT_WHITELIST)
    return parser


def main() -> None:
    args = build_parser().parse_args()
    data = _load_active(args.input)
    if args.include_archive:
        data = _merge(data, _load_archive(args.input))
    if args.include_loose_patterns:
        PII_PATTERNS.update(LOOSE_PII_PATTERNS)

    whitelist = _whitelist_domains(args.whitelist_email)
    findings = _scan_kg(data, whitelist, args.scan_handoffs_only)
    if args.scan_logs and not args.scan_handoffs_only:
        findings.extend(_scan_logs(whitelist))

    if not args.summary_only:
        for finding in findings:
            _print_finding(finding)
    print(_summary(findings))
    raise SystemExit(1 if findings and args.exit_on_finding else 0)


if __name__ == "__main__":
    main()
