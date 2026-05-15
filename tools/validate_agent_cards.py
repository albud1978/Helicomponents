#!/usr/bin/env python3
"""Validate Cursor agent profile frontmatter against agent_card schema."""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

import jsonschema
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
AGENTS_DIR = REPO_ROOT / ".cursor" / "agents"
DEFAULT_SCHEMA = REPO_ROOT / "config" / "schemas" / "agent_card.schema.json"
DEPRECATED_RE = re.compile(r"\bDEPRECATED\b")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate .cursor/agents/*.md agent_card frontmatter."
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=DEFAULT_SCHEMA,
        help="Path to agent_card JSON schema.",
    )
    parser.add_argument(
        "--profile",
        type=str,
        help="Validate one profile by name, for example: coder-general.",
    )
    return parser


def load_schema(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"schema not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        schema = json.load(f)
    if not isinstance(schema, dict):
        raise ValueError(f"schema must be a JSON object: {path}")
    jsonschema.Draft202012Validator.check_schema(schema)
    return schema


def resolve_profiles(profile: str | None) -> list[Path]:
    if profile:
        profile_name = profile[:-3] if profile.endswith(".md") else profile
        path = AGENTS_DIR / f"{profile_name}.md"
        if not path.exists():
            raise FileNotFoundError(f"profile not found: {path}")
        return [path]
    return sorted(AGENTS_DIR.glob("*.md"))


def parse_frontmatter(path: Path) -> dict[str, Any]:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0].strip() != "---":
        raise ValueError("missing opening frontmatter delimiter")

    end_index = None
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            end_index = index
            break

    if end_index is None:
        raise ValueError("missing closing frontmatter delimiter")

    frontmatter = "\n".join(lines[1:end_index])
    data = yaml.safe_load(frontmatter)
    if not isinstance(data, dict):
        raise ValueError("frontmatter must be a YAML mapping")
    return data


def format_validation_error(error: jsonschema.ValidationError) -> str:
    location = ".".join(str(part) for part in error.absolute_path)
    if not location:
        location = "<root>"
    return f"{location}: {error.message}"


def validate_profile(path: Path, schema: dict[str, Any]) -> tuple[bool, str]:
    data = parse_frontmatter(path)
    description = str(data.get("description", ""))

    if "agent_card" not in data:
        if DEPRECATED_RE.search(description):
            return True, "SKIP - missing agent_card; description contains DEPRECATED"
        return False, "FAIL - missing agent_card and description is not DEPRECATED"

    try:
        jsonschema.validate(instance=data, schema=schema)
    except jsonschema.ValidationError as exc:
        return False, f"FAIL - {format_validation_error(exc)}"

    return True, "PASS"


def main() -> int:
    args = build_parser().parse_args()
    schema_path = args.schema if args.schema.is_absolute() else Path.cwd() / args.schema

    schema = load_schema(schema_path)
    profiles = resolve_profiles(args.profile)
    failed = False

    for path in profiles:
        profile_name = path.stem
        try:
            ok, status = validate_profile(path, schema)
        except (OSError, ValueError, yaml.YAMLError, jsonschema.SchemaError) as exc:
            ok = False
            status = f"FAIL - {exc}"

        if not ok:
            failed = True
        print(f"{profile_name}: {status}")

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
