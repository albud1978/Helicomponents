#!/usr/bin/env python3
"""Extract a cloneable L1 multi-agent framework template from this project."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import yaml


ASSET_GROUPS = ("hooks", "rules", "code", "tools", "docs", "config", "workflows")
L3_PLACEHOLDERS = {
    "model": "# TODO: select model slug",
    "agent_card.model_fallback": "# TODO: select fallback model slug",
    "agent_card.scope.allowed_paths": ["# TODO: project-specific paths"],
    "agent_card.scope.denied_paths": ["# TODO: project-specific denied paths"],
    "agent_card.scope.read_only_paths": ["# TODO: project-specific read-only paths"],
    "agent_card.tools.mcp_servers": ["# TODO: project-specific MCP servers"],
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract L1-only multi-agent framework template."
    )
    parser.add_argument(
        "--manifest",
        default="framework/manifest.yaml",
        help="Path to framework inventory manifest.",
    )
    parser.add_argument(
        "--output",
        default="framework/template_out/",
        help="Output directory for extracted template.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print extraction plan without creating files.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite output directory if it already exists.",
    )
    return parser.parse_args()


def load_manifest(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise SystemExit(f"Manifest not found: {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise SystemExit(f"Manifest must be a YAML mapping: {path}")
    return data


def prepare_output(output: Path, dry_run: bool, force: bool) -> None:
    if dry_run:
        return
    if output.exists():
        if not force:
            raise SystemExit(
                f"Output already exists: {output}. Re-run with --force to overwrite."
            )
        shutil.rmtree(output)
    output.mkdir(parents=True)


def get_nested(data: Dict[str, Any], dotted_path: str) -> Any:
    current: Any = data
    for part in dotted_path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def set_nested(data: Dict[str, Any], dotted_path: str, value: Any) -> None:
    current = data
    parts = dotted_path.split(".")
    for part in parts[:-1]:
        next_value = current.get(part)
        if not isinstance(next_value, dict):
            next_value = {}
            current[part] = next_value
        current = next_value
    current[parts[-1]] = value


def parse_frontmatter(text: str, path: Path) -> Tuple[Dict[str, Any], str]:
    lines = text.splitlines()
    if not lines or lines[0].strip() != "---":
        raise SystemExit(f"Agent profile has no YAML frontmatter: {path}")

    closing_index = None
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            closing_index = index
            break
    if closing_index is None:
        raise SystemExit(f"Agent profile frontmatter is not closed: {path}")

    frontmatter_text = "\n".join(lines[1:closing_index])
    body = "\n".join(lines[closing_index + 1 :])
    frontmatter = yaml.safe_load(frontmatter_text) or {}
    if not isinstance(frontmatter, dict):
        raise SystemExit(f"Agent frontmatter must be a YAML mapping: {path}")
    return frontmatter, body


def extract_frontmatter(source: Dict[str, Any], spec: Dict[str, Any]) -> Dict[str, Any]:
    extracted: Dict[str, Any] = {}
    for field in spec.get("l1_fields", []):
        value = get_nested(source, field)
        if value is not None:
            set_nested(extracted, field, value)

    for field in spec.get("l3_fields", []):
        placeholder = L3_PLACEHOLDERS.get(field, "# TODO: project-specific value")
        set_nested(extracted, field, placeholder)

    return extracted


def extract_body(body: str, spec: Dict[str, Any]) -> str:
    body_spec = spec.get("body") or {}
    if not body_spec.get("l1_default", False):
        return "<!-- TODO L1: add reusable agent instructions -->\n"

    patterns = [re.compile(pattern) for pattern in body_spec.get("l3_patterns", [])]
    output_lines: List[str] = []
    for line in body.splitlines():
        if any(pattern.search(line) for pattern in patterns):
            output_lines.append(f"<!-- TODO L3: {line} -->")
        else:
            output_lines.append(line)
    return "\n".join(output_lines).rstrip() + "\n"


def render_agent_profile(source_path: Path, spec: Dict[str, Any]) -> str:
    frontmatter, body = parse_frontmatter(source_path.read_text(encoding="utf-8"), source_path)
    extracted = extract_frontmatter(frontmatter, spec)
    rendered_frontmatter = yaml.safe_dump(
        extracted,
        allow_unicode=True,
        sort_keys=False,
        default_flow_style=False,
    ).rstrip()
    return f"---\n{rendered_frontmatter}\n---\n\n{extract_body(body, spec)}"


def comment_prefix(path: Path) -> str:
    if path.suffix in {".md", ".mdc"}:
        return "<!-- {text} -->"
    if path.suffix == ".json":
        return ""
    return "# {text}"


def l3_header(path: Path, patterns: Iterable[str]) -> str:
    patterns_text = ", ".join(patterns) if patterns else "unspecified"
    message = (
        "TODO L3: следующие patterns в этом файле проектно-специфичны: "
        f"{patterns_text}"
    )
    template = comment_prefix(path)
    if not template:
        return ""
    return template.format(text=message) + "\n"


def add_l3_header(content: str, path: Path, patterns: Iterable[str]) -> str:
    header = l3_header(path, patterns)
    if not header:
        return content
    if content.startswith("#!"):
        first_line, _, rest = content.partition("\n")
        return f"{first_line}\n{header}{rest}"
    return header + content


def render_stub(original_path: str, classification: str) -> str:
    return (
        "# L3-only project artifact\n"
        f"original_path: {original_path}\n"
        f"classification: {classification}\n"
        "template_action: provide a project-specific replacement in the target repo\n"
    )


def render_empty_template(path: str) -> str:
    if path == "config/agent_kg.json":
        return json.dumps(
            {
                "metadata": {
                    "description": "Agent KG initial empty state for framework template"
                },
                "workflows": [],
                "handoffs": [],
                "contexts": [],
            },
            ensure_ascii=False,
            indent=2,
        ) + "\n"
    return ""


def write_output(path: Path, content: str, dry_run: bool) -> int:
    encoded_size = len(content.encode("utf-8"))
    if not dry_run:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    return encoded_size


def render_classified_asset(repo: Path, spec: Dict[str, Any]) -> Tuple[str, str]:
    rel_path = spec["path"]
    classification = spec.get("classification", "l1")
    source_path = repo / rel_path

    if classification == "empty_template":
        return rel_path, render_empty_template(rel_path)

    if classification == "l3":
        return f"{rel_path}.l3_stub", render_stub(rel_path, classification)

    if not source_path.exists():
        raise SystemExit(f"Manifest asset not found: {rel_path}")

    content = source_path.read_text(encoding="utf-8")
    if str(classification).startswith("l1_with_l3"):
        content = add_l3_header(content, Path(rel_path), spec.get("l3_patterns", []))
    return rel_path, content


def extract_agents(
    repo: Path,
    output: Path,
    manifest: Dict[str, Any],
    dry_run: bool,
) -> Tuple[int, int]:
    count = 0
    total_size = 0
    for spec in manifest.get("framework_assets", {}).get("agent_profiles", []):
        rel_path = spec["path"]
        source_path = repo / rel_path
        if not source_path.exists():
            raise SystemExit(f"Agent profile not found: {rel_path}")
        rendered = render_agent_profile(source_path, spec)
        total_size += write_output(output / rel_path, rendered, dry_run)
        count += 1
        print(f"{'Would extract' if dry_run else 'Extracted'} agent: {rel_path}")
    return count, total_size


def extract_assets(
    repo: Path,
    output: Path,
    manifest: Dict[str, Any],
    dry_run: bool,
) -> Tuple[int, int, int]:
    extracted_count = 0
    stubbed_count = 0
    total_size = 0
    assets = manifest.get("framework_assets", {})

    for group_name in ASSET_GROUPS:
        for spec in assets.get(group_name, []):
            target_rel_path, content = render_classified_asset(repo, spec)
            total_size += write_output(output / target_rel_path, content, dry_run)
            if target_rel_path.endswith(".l3_stub"):
                stubbed_count += 1
                action = "Would stub" if dry_run else "Stubbed"
            else:
                extracted_count += 1
                action = "Would extract" if dry_run else "Extracted"
            print(f"{action} {group_name}: {target_rel_path}")

    return extracted_count, stubbed_count, total_size


def main() -> int:
    args = parse_args()
    root = repo_root()
    manifest_path = (root / args.manifest).resolve()
    output_path = (root / args.output).resolve()

    manifest = load_manifest(manifest_path)
    prepare_output(output_path, args.dry_run, args.force)

    agent_count, agent_size = extract_agents(root, output_path, manifest, args.dry_run)
    asset_count, stubbed_count, asset_size = extract_assets(
        root, output_path, manifest, args.dry_run
    )
    skipped_count = len(manifest.get("not_in_template", []))
    total_size = agent_size + asset_size

    print()
    print(f"Extracted: {agent_count + asset_count} L1 files")
    print(f"Stubbed L3-only: {stubbed_count} files")
    print(f"Skipped (not_in_template): {skipped_count} paths")
    print(f"Output: {args.output}")
    print(f"Total size: {total_size} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
