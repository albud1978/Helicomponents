#!/usr/bin/env python3
"""Build local Agent KG dashboard (template-default paths)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[2]
DEFAULT_KG_PATH = ROOT_DIR / "config" / "agent_kg.json"
DEFAULT_CAPSULES_PATH = ROOT_DIR / "config" / "capsules_manifest.json"
OUTPUT_PATH = ROOT_DIR / "tools" / "agent_kg_viewer" / "index.html"


def _load_json(path: Path, fallback: dict[str, Any]) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback


def _as_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _short(value: Any, max_len: int = 140) -> str:
    raw = " ".join(_text(value).split())
    if not raw:
        return "N/A"
    if len(raw) <= max_len:
        return raw
    return raw[: max_len - 1] + "..."


def _latest_event_for_workflow(
    workflow_id: str, handoffs: list[dict[str, Any]], contexts: list[dict[str, Any]]
) -> dict[str, str]:
    wf_handoffs = sorted(
        [h for h in handoffs if _text(h.get("workflow_id")) == workflow_id],
        key=lambda h: _text(h.get("created_at")),
        reverse=True,
    )
    wf_contexts = sorted(
        [c for c in contexts if _text(c.get("workflow_id")) == workflow_id],
        key=lambda c: _text(c.get("updated_at") or c.get("created_at")),
        reverse=True,
    )

    last_handoff = wf_handoffs[0] if wf_handoffs else None
    last_context = wf_contexts[0] if wf_contexts else None

    handoff_ts = _text(last_handoff.get("created_at")) if last_handoff else ""
    context_ts = _text(last_context.get("updated_at") or last_context.get("created_at")) if last_context else ""

    if handoff_ts and (not context_ts or handoff_ts >= context_ts):
        return {
            "type": "handoff",
            "at": handoff_ts,
            "actor": _text(last_handoff.get("agent")) or "N/A",
            "stage": _text(last_handoff.get("plan_step_id")) or "N/A",
            "summary": _short(
                _text(last_handoff.get("changes"))
                or _text(last_handoff.get("facts"))
                or _text(last_handoff.get("goal"))
            ),
        }

    if context_ts:
        return {
            "type": "context",
            "at": context_ts,
            "actor": _text(last_context.get("agent")) or "N/A",
            "stage": _text(last_context.get("context_type")) or "N/A",
            "summary": _short(_text(last_context.get("content")), 120),
        }

    return {"type": "N/A", "at": "N/A", "actor": "N/A", "stage": "N/A", "summary": "N/A"}


def _render_table(headers: list[str], rows: list[list[str]]) -> str:
    if not rows:
        return '<div class="empty">Нет данных</div>'
    header_html = "".join(f"<th>{escape(h)}</th>" for h in headers)
    body_rows = []
    for row in rows:
        body_rows.append("<tr>" + "".join(f"<td>{escape(col)}</td>" for col in row) + "</tr>")
    return (
        '<div class="table-wrap"><table>'
        f"<thead><tr>{header_html}</tr></thead>"
        f"<tbody>{''.join(body_rows)}</tbody>"
        "</table></div>"
    )


def _render_execution_status(
    workflows: list[dict[str, Any]], handoffs: list[dict[str, Any]], contexts: list[dict[str, Any]]
) -> str:
    active = [w for w in workflows if _text(w.get("status")).lower() == "active"]
    active.sort(key=lambda w: _text(w.get("updated_at")), reverse=True)

    rows: list[list[str]] = []
    for workflow in active:
        workflow_id = _text(workflow.get("workflow_id")) or "N/A"
        latest = _latest_event_for_workflow(workflow_id, handoffs, contexts)
        rows.append(
            [
                workflow_id,
                _text(workflow.get("phase")) or "N/A",
                _text(workflow.get("owner")) or "N/A",
                f"{latest['type']} @ {latest['at']}",
                latest["actor"],
                latest["stage"],
                latest["summary"],
            ]
        )

    return _render_table(
        ["Workflow", "Phase", "Owner", "LastEvent", "Actor", "Stage", "Summary"],
        rows,
    )


def _render_latest_interactions(
    handoffs: list[dict[str, Any]], contexts: list[dict[str, Any]], limit: int = 25
) -> str:
    events: list[dict[str, str]] = []
    for handoff in handoffs:
        events.append(
            {
                "ts": _text(handoff.get("created_at")),
                "workflow": _text(handoff.get("workflow_id")) or "N/A",
                "type": "handoff",
                "actor": _text(handoff.get("agent")) or "N/A",
                "stage": _text(handoff.get("plan_step_id")) or "N/A",
                "summary": _short(
                    _text(handoff.get("changes"))
                    or _text(handoff.get("facts"))
                    or _text(handoff.get("goal"))
                ),
            }
        )
    for context in contexts:
        events.append(
            {
                "ts": _text(context.get("updated_at") or context.get("created_at")),
                "workflow": _text(context.get("workflow_id")) or "N/A",
                "type": "context",
                "actor": _text(context.get("agent")) or "N/A",
                "stage": _text(context.get("context_type")) or "N/A",
                "summary": _short(_text(context.get("content")), 120),
            }
        )

    events = [event for event in events if event["ts"]]
    events.sort(key=lambda event: event["ts"], reverse=True)
    top_events = events[:limit]

    rows = [
        [event["ts"], event["workflow"], event["type"], event["actor"], event["stage"], event["summary"]]
        for event in top_events
    ]
    return _render_table(["Time", "Workflow", "Type", "Actor", "Stage", "Summary"], rows)


def build_html(kg_data: dict[str, Any], capsules_data: dict[str, Any]) -> str:
    workflows = _as_list(kg_data.get("workflows"))
    handoffs = _as_list(kg_data.get("handoffs"))
    contexts = _as_list(kg_data.get("contexts"))
    capsules = _as_list(capsules_data.get("capsules"))
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    html: list[str] = []
    html.append("<!doctype html>")
    html.append('<html lang="ru">')
    html.append("<head>")
    html.append('  <meta charset="utf-8" />')
    html.append('  <meta name="viewport" content="width=device-width, initial-scale=1" />')
    html.append("  <title>Agent KG Viewer</title>")
    html.append("  <style>")
    html.append("    body { font-family: Arial, Helvetica, sans-serif; margin: 24px; max-width: 1300px; }")
    html.append("    h1, h2 { margin: 0 0 8px; }")
    html.append("    .meta { color: #666; font-size: 12px; margin-bottom: 8px; }")
    html.append("    .section { margin-top: 18px; }")
    html.append("    .section-note { font-size: 12px; color: #666; margin: 2px 0 8px; }")
    html.append("    .empty { color: #888; font-style: italic; }")
    html.append("    .table-wrap { overflow-x: auto; border: 1px solid #ddd; border-radius: 8px; }")
    html.append("    table { border-collapse: collapse; width: 100%; font-size: 12px; }")
    html.append("    th, td { border-bottom: 1px solid #eee; padding: 8px; text-align: left; vertical-align: top; }")
    html.append("    th { font-size: 11px; text-transform: uppercase; color: #555; background: #f7f7f7; }")
    html.append("  </style>")
    html.append("</head>")
    html.append("<body>")
    html.append("  <h1>Agent KG Viewer</h1>")
    html.append(f'  <div class="meta">Generated at {generated_at}</div>')
    html.append("  <div class=\"meta\">Default sources: config/agent_kg.json, config/capsules_manifest.json</div>")
    html.append("  <div class=\"meta\">Run: python3 tools/agent_kg_viewer/build_agent_kg_viewer.py</div>")

    html.append('  <div class="section">')
    html.append("    <h2>Текущий статус исполнения</h2>")
    html.append('    <div class="section-note">Активные workflow и их последнее событие.</div>')
    html.append(_render_execution_status(workflows, handoffs, contexts))
    html.append("  </div>")

    html.append('  <div class="section">')
    html.append("    <h2>Последние взаимодействия агентов</h2>")
    html.append('    <div class="section-note">Последние handoff/context события (до 25 записей).</div>')
    html.append(_render_latest_interactions(handoffs, contexts, limit=25))
    html.append("  </div>")

    html.append('  <div class="section">')
    html.append("    <h2>Краткая сводка</h2>")
    html.append(
        _render_table(
            ["Metric", "Value"],
            [
                ["workflows", str(len(workflows))],
                ["handoffs", str(len(handoffs))],
                ["contexts", str(len(contexts))],
                ["capsules", str(len(capsules))],
            ],
        )
    )
    html.append("  </div>")

    html.append("</body>")
    html.append("</html>")
    return "\n".join(html)


def main() -> None:
    kg_data = _load_json(DEFAULT_KG_PATH, {"workflows": [], "handoffs": [], "contexts": []})
    capsules_data = _load_json(DEFAULT_CAPSULES_PATH, {"capsules": []})
    html = build_html(kg_data, capsules_data)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(html, encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
